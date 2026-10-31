//! Storage-node daemon core: control channel + bounded registered DRAM pool + file store.
//! One daemon owns one storage root. The data plane is NIXL/UCX; the file store is authoritative.
use crate::error::{KvError, Result};
use crate::file_store::{FileStore, ObjectReader, PutOutcome, PutTxn, StoreConfig};
use crate::nixl_transport::{NixlAgent, RegisteredBuf};
use crate::protocol::*;
use std::collections::HashMap;
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub listen: String,
    pub storage_root: PathBuf,
    /// Total registered staging pool (split into `chunk_bytes` slots). Keep <= 1 GiB.
    pub ram_pool_bytes: u64,
    pub chunk_bytes: u64,
    pub max_object_bytes: u64,
    pub file_quota_bytes: u64,
    pub fsync: bool,
    pub token: String,
    pub deadline: Duration,
    pub idle: Duration,
    pub quarantine: Duration,
    pub allowed_namespaces: Vec<String>,
    pub allow_delete: bool,
    pub max_connections: usize,
    pub ucx_net_devices: String,
    pub ucx_tls: String,
    pub nixl_prefix: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            listen: "10.10.10.2:7471".into(),
            storage_root: PathBuf::from("/home/hpcnc/llm-kv-cache"),
            ram_pool_bytes: 512 << 20,
            chunk_bytes: 64 << 20,
            max_object_bytes: 1 << 30,
            file_quota_bytes: 2 << 30,
            fsync: true,
            token: String::new(),
            deadline: Duration::from_secs(30),
            idle: Duration::from_secs(120),
            quarantine: Duration::from_secs(60),
            allowed_namespaces: vec![],
            allow_delete: false,
            max_connections: 16,
            ucx_net_devices: "mlx5_0:1".into(),
            ucx_tls: "^tcp".into(),
            nixl_prefix: "/home/hpcnc/rdma-kv-experiments/build/nixl-install".into(),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum SlotState {
    Free,
    Held(u64),
    Quarantined(Instant),
}

struct SlotInner {
    st: Vec<SlotState>,
    gen: Vec<u64>,
}

struct SlotTable {
    m: Mutex<SlotInner>,
    cv: Condvar,
}

impl SlotTable {
    fn new(n: usize) -> SlotTable {
        SlotTable { m: Mutex::new(SlotInner { st: vec![SlotState::Free; n], gen: vec![0; n] }), cv: Condvar::new() }
    }
    fn alloc(&self, conn: u64, wait: Duration) -> Option<(u32, u64)> {
        let end = Instant::now() + wait;
        let mut g = self.m.lock().unwrap();
        loop {
            let now = Instant::now();
            for i in 0..g.st.len() {
                if let SlotState::Quarantined(u) = g.st[i] {
                    if u <= now {
                        g.st[i] = SlotState::Free;
                    }
                }
            }
            if let Some(i) = g.st.iter().position(|s| *s == SlotState::Free) {
                g.st[i] = SlotState::Held(conn);
                g.gen[i] += 1;
                return Some((i as u32, g.gen[i]));
            }
            if now >= end {
                return None;
            }
            let (ng, _) = self.cv.wait_timeout(g, std::cmp::min(end - now, Duration::from_millis(200))).unwrap();
            g = ng;
        }
    }
    fn release(&self, slot: u32, gen: u64, conn: u64) -> bool {
        let mut g = self.m.lock().unwrap();
        let i = slot as usize;
        if i < g.st.len() && g.st[i] == SlotState::Held(conn) && g.gen[i] == gen {
            g.st[i] = SlotState::Free;
            self.cv.notify_all();
            true
        } else {
            false
        }
    }
    fn quarantine_conn(&self, conn: u64, until: Instant) -> usize {
        let mut g = self.m.lock().unwrap();
        let mut n = 0;
        for s in g.st.iter_mut() {
            if *s == SlotState::Held(conn) {
                *s = SlotState::Quarantined(until);
                n += 1;
            }
        }
        n
    }
    fn counts(&self) -> (u32, u32, u32) {
        let g = self.m.lock().unwrap();
        let now = Instant::now();
        let free = g.st.iter().filter(|s| matches!(s, SlotState::Free) || matches!(s, SlotState::Quarantined(u) if *u <= now)).count();
        let q = g.st.iter().filter(|s| matches!(s, SlotState::Quarantined(u) if *u > now)).count();
        (g.st.len() as u32, free as u32, q as u32)
    }
}

struct Session {
    agent: String,
    refs: usize,
}

struct Shared {
    cfg: ServerConfig,
    store: Arc<FileStore>,
    agent: Arc<NixlAgent>,
    pool: RegisteredBuf,
    slots: SlotTable,
    sessions: Mutex<HashMap<u64, Session>>,
    next_op: AtomicU64,
    next_conn: AtomicU64,
    active: AtomicUsize,
    stop: AtomicBool,
}

struct PutOp {
    txn: PutTxn,
    next_off: u64,
    size: u64,
}
struct GetOp {
    reader: ObjectReader,
}
struct Pending {
    slot: u32,
    gen: u64,
    op: u64,
    offset: u64,
    len: u64,
    put: bool,
}

pub fn log(level: &str, msg: &str) {
    let t = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    eprintln!("[{t}] {level} {msg}");
}

static STOP: AtomicBool = AtomicBool::new(false);
extern "C" fn on_signal(_: libc::c_int) {
    STOP.store(true, Ordering::SeqCst);
}

fn random_u64() -> u64 {
    use std::io::Read;
    let mut b = [0u8; 8];
    if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
        let _ = f.read_exact(&mut b);
    }
    u64::from_le_bytes(b)
}

pub fn run(cfg: ServerConfig) -> Result<()> {
    if cfg.token.len() < 16 {
        return Err(KvError::BadRequest("token must be at least 16 characters (use --token-file)".into()));
    }
    if cfg.ram_pool_bytes < cfg.chunk_bytes || cfg.ram_pool_bytes > (1 << 30) {
        return Err(KvError::BadRequest("ram_pool_bytes must be between chunk_bytes and 1 GiB".into()));
    }
    // Explicit transport selection: RDMA only, no silent TCP fallback.
    std::env::set_var("UCX_NET_DEVICES", &cfg.ucx_net_devices);
    std::env::set_var("UCX_TLS", &cfg.ucx_tls);
    if std::env::var("NIXL_PLUGIN_DIR").is_err() {
        std::env::set_var("NIXL_PLUGIN_DIR", format!("{}/lib/x86_64-linux-gnu/plugins", cfg.nixl_prefix));
    }
    let store = FileStore::open(
        &cfg.storage_root,
        StoreConfig { quota_bytes: cfg.file_quota_bytes, max_object_bytes: cfg.max_object_bytes, fsync: cfg.fsync, ..StoreConfig::default() },
    )?;
    let agent = Arc::new(NixlAgent::new(&format!("kvstored-{}-{:08x}", std::process::id(), random_u64() as u32))?);
    let nslots = (cfg.ram_pool_bytes / cfg.chunk_bytes) as usize;
    let pool = RegisteredBuf::new(&agent, nslots * cfg.chunk_bytes as usize)?;
    log(
        "INFO",
        &format!(
            "agent={} pool={} MiB in {} slots of {} MiB, storage_root={}",
            agent.name,
            pool.len() >> 20,
            nslots,
            cfg.chunk_bytes >> 20,
            store.root().display()
        ),
    );
    let shared = Arc::new(Shared {
        cfg: cfg.clone(),
        store,
        agent,
        pool,
        slots: SlotTable::new(nslots),
        sessions: Mutex::new(HashMap::new()),
        next_op: AtomicU64::new(1),
        next_conn: AtomicU64::new(1),
        active: AtomicUsize::new(0),
        stop: AtomicBool::new(false),
    });
    let listener = TcpListener::bind(&cfg.listen).map_err(|e| KvError::Unavailable(format!("bind {}: {e}", cfg.listen)))?;
    listener.set_nonblocking(true)?;
    // SAFETY: async-signal-safe handler that only sets an atomic.
    unsafe {
        libc::signal(libc::SIGTERM, on_signal as *const () as libc::sighandler_t);
        libc::signal(libc::SIGINT, on_signal as *const () as libc::sighandler_t);
    }
    log("INFO", &format!("listening on {} (control plane); data plane NIXL/UCX UCX_TLS={} UCX_NET_DEVICES={}", cfg.listen, cfg.ucx_tls, cfg.ucx_net_devices));
    while !STOP.load(Ordering::SeqCst) {
        match listener.accept() {
            Ok((s, peer)) => {
                if shared.active.load(Ordering::SeqCst) >= cfg.max_connections {
                    log("WARN", &format!("rejecting {peer}: too many connections"));
                    continue;
                }
                let sh = shared.clone();
                shared.active.fetch_add(1, Ordering::SeqCst);
                std::thread::spawn(move || {
                    let id = sh.next_conn.fetch_add(1, Ordering::SeqCst);
                    handle(&sh, s, id);
                    sh.active.fetch_sub(1, Ordering::SeqCst);
                });
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => std::thread::sleep(Duration::from_millis(100)),
            Err(e) => {
                log("ERROR", &format!("accept: {e}"));
                std::thread::sleep(Duration::from_millis(200));
            }
        }
    }
    // Graceful shutdown: stop accepting, drain bounded in-flight work, then deregister memory (drop).
    log("INFO", "shutdown requested: draining");
    shared.stop.store(true, Ordering::SeqCst);
    drop(listener);
    let t0 = Instant::now();
    while shared.active.load(Ordering::SeqCst) > 0 && t0.elapsed() < Duration::from_secs(10) {
        std::thread::sleep(Duration::from_millis(100));
    }
    let left = shared.active.load(Ordering::SeqCst);
    if left > 0 {
        log("WARN", &format!("{left} connections still active after drain; exiting without deregistering to avoid racing in-flight RDMA"));
        std::process::exit(0);
    }
    log("INFO", "clean shutdown");
    Ok(())
}

fn ns_allowed(sh: &Shared, ns: &str) -> Result<()> {
    if sh.cfg.allowed_namespaces.is_empty() || sh.cfg.allowed_namespaces.iter().any(|a| a == ns) {
        Ok(())
    } else {
        Err(KvError::Unauthorized(format!("namespace {ns:?} not allowed")))
    }
}

fn handle(sh: &Arc<Shared>, mut s: TcpStream, conn: u64) {
    let _ = s.set_nodelay(true);
    let _ = s.set_read_timeout(Some(sh.cfg.idle));
    let _ = s.set_write_timeout(Some(sh.cfg.deadline));
    let mut session: Option<u64> = None;
    let mut puts: HashMap<u64, PutOp> = HashMap::new();
    let mut gets: HashMap<u64, GetOp> = HashMap::new();
    let mut pending: Option<Pending> = None;
    loop {
        if sh.stop.load(Ordering::SeqCst) {
            break;
        }
        let req: Req = match read_frame(&mut s) {
            Ok(r) => r,
            Err(KvError::Timeout(_)) if !sh.stop.load(Ordering::SeqCst) && pending.is_none() && puts.is_empty() && gets.is_empty() => break, // idle
            Err(_) => break,
        };
        let resp = match dispatch(sh, conn, &mut session, &mut puts, &mut gets, &mut pending, req) {
            Ok(r) => r,
            Err(e) => {
                if !matches!(e, KvError::Miss) {
                    log("DEBUG", &format!("conn {conn}: {}", e.code()));
                }
                Resp::err(&e)
            }
        };
        if write_frame(&mut s, &resp).is_err() {
            break;
        }
    }
    // Connection teardown: anything still in flight is abandoned safely.
    let n = sh.slots.quarantine_conn(conn, Instant::now() + sh.cfg.quarantine);
    if n > 0 {
        log("WARN", &format!("conn {conn}: {n} slot(s) quarantined for {:?} (transfer completion unknown)", sh.cfg.quarantine));
    }
    drop(puts); // PutTxn::drop removes unpublished temp files and returns reservations
    drop(gets);
    if let Some(sid) = session {
        let mut m = sh.sessions.lock().unwrap();
        if let Some(se) = m.get_mut(&sid) {
            se.refs -= 1;
            if se.refs == 0 {
                let name = se.agent.clone();
                m.remove(&sid);
                sh.agent.invalidate_remote(&name);
            }
        }
    }
}

fn dispatch(
    sh: &Arc<Shared>,
    conn: u64,
    session: &mut Option<u64>,
    puts: &mut HashMap<u64, PutOp>,
    gets: &mut HashMap<u64, GetOp>,
    pending: &mut Option<Pending>,
    req: Req,
) -> Result<Resp> {
    let cs = sh.cfg.chunk_bytes;
    match req {
        Req::Health { token } => {
            if !token_eq(&token, &sh.cfg.token) {
                return Err(KvError::Unauthorized("bad token".into()));
            }
            let st = sh.store.stats();
            let (t, f, q) = sh.slots.counts();
            let writable = sh.store.check_writable().is_ok();
            Ok(Resp::Health {
                ok: writable,
                writable,
                objects: st.objects,
                used_bytes: st.used_bytes,
                reserved_bytes: st.reserved_bytes,
                quota_bytes: st.quota_bytes,
                slots_total: t,
                slots_free: f,
                slots_quarantined: q,
                version: env!("CARGO_PKG_VERSION").to_string(),
            })
        }
        Req::Hello { v, token, agent, md } => {
            if !token_eq(&token, &sh.cfg.token) {
                return Err(KvError::Unauthorized("bad token".into()));
            }
            if v != PROTOCOL_VERSION {
                return Err(KvError::Incompatible(format!("protocol {v} != {PROTOCOL_VERSION}")));
            }
            if session.is_some() {
                return Err(KvError::BadRequest("already attached".into()));
            }
            let mdb = hex_decode(&md)?;
            // A reconnecting client must never reuse stale remote descriptors: drop any old state for this name.
            {
                let m = sh.sessions.lock().unwrap();
                if m.values().any(|s| s.agent == agent) {
                    return Err(KvError::BadRequest("agent name already has an active session".into()));
                }
            }
            sh.agent.invalidate_remote(&agent);
            let name = sh.agent.load_remote(&mdb)?;
            if name != agent {
                sh.agent.invalidate_remote(&name);
                return Err(KvError::BadRequest("metadata agent name mismatch".into()));
            }
            let sid = random_u64();
            sh.sessions.lock().unwrap().insert(sid, Session { agent: name, refs: 1 });
            *session = Some(sid);
            let my_md = sh.agent.local_metadata()?;
            Ok(Resp::HelloOk {
                session: sid,
                agent: sh.agent.name.clone(),
                md: crate::object::hex(&my_md),
                slot_bytes: cs,
                slots: sh.slots.counts().0,
                max_object_bytes: sh.cfg.max_object_bytes,
                version: env!("CARGO_PKG_VERSION").to_string(),
            })
        }
        Req::Attach { session: sid, token } => {
            if !token_eq(&token, &sh.cfg.token) {
                return Err(KvError::Unauthorized("bad token".into()));
            }
            let mut m = sh.sessions.lock().unwrap();
            match m.get_mut(&sid) {
                Some(se) if session.is_none() => {
                    se.refs += 1;
                    *session = Some(sid);
                    Ok(Resp::Ok)
                }
                _ => Err(KvError::Unauthorized("unknown session".into())),
            }
        }
        other => {
            let sid = session.ok_or_else(|| KvError::Unauthorized("no session".into()))?;
            let client_agent = sh.sessions.lock().unwrap().get(&sid).map(|s| s.agent.clone()).ok_or_else(|| KvError::Unauthorized("session ended".into()))?;
            let _ = client_agent;
            match other {
                Req::PutBegin { ns, key, meta } => {
                    ns_allowed(sh, &ns)?;
                    match sh.store.begin_put(&ns, &key, meta.clone())? {
                        PutOutcome::Exists => Ok(Resp::PutExists),
                        PutOutcome::Txn(txn) => {
                            let op = sh.next_op.fetch_add(1, Ordering::SeqCst);
                            puts.insert(op, PutOp { txn, next_off: 0, size: meta.size });
                            Ok(Resp::PutReady { op, chunk_bytes: cs })
                        }
                    }
                }
                Req::PutChunk { op, offset, len } => {
                    let p = puts.get(&op).ok_or_else(|| KvError::BadRequest("unknown put op".into()))?;
                    if pending.is_some() {
                        return Err(KvError::BadRequest("a slot is already outstanding on this connection".into()));
                    }
                    if len == 0 || len > cs || offset != p.next_off || offset + len > p.size {
                        return Err(KvError::BadRequest("bad chunk offset/len".into()));
                    }
                    let (slot, gen) = sh.slots.alloc(conn, sh.cfg.deadline).ok_or_else(|| KvError::Unavailable("no free staging slot".into()))?;
                    *pending = Some(Pending { slot, gen, op, offset, len, put: true });
                    Ok(Resp::Slot { slot, gen, addr: (sh.pool.addr() + slot as usize * cs as usize) as u64, len })
                }
                Req::PutChunkDone { op, slot, gen, offset, len } => {
                    let pd = pending.take().ok_or_else(|| KvError::BadRequest("no outstanding slot".into()))?;
                    if !(pd.put && pd.op == op && pd.slot == slot && pd.gen == gen && pd.offset == offset && pd.len == len) {
                        sh.slots.release(pd.slot, pd.gen, conn);
                        return Err(KvError::BadRequest("chunk completion does not match the granted slot".into()));
                    }
                    std::sync::atomic::fence(Ordering::Acquire);
                    let p = puts.get_mut(&op).ok_or_else(|| KvError::BadRequest("unknown put op".into()))?;
                    // SAFETY: slot is Held by this connection and the client reported its RDMA WRITE complete.
                    let data = unsafe { sh.pool.slice(slot as usize * cs as usize, len as usize) };
                    let r = p.txn.write(data);
                    sh.slots.release(slot, gen, conn);
                    match r {
                        Ok(()) => {
                            p.next_off += len;
                            Ok(Resp::Ok)
                        }
                        Err(e) => {
                            puts.remove(&op);
                            Err(e)
                        }
                    }
                }
                Req::PutCommit { op } => {
                    let p = puts.remove(&op).ok_or_else(|| KvError::BadRequest("unknown put op".into()))?;
                    if p.next_off != p.size {
                        return Err(KvError::ChecksumMismatch(format!("commit with {} of {} bytes", p.next_off, p.size)));
                    }
                    let stored = p.txn.commit()?;
                    Ok(Resp::PutDone { stored })
                }
                Req::PutAbort { op } => {
                    puts.remove(&op);
                    Ok(Resp::Ok)
                }
                Req::GetBegin { ns, key } => {
                    ns_allowed(sh, &ns)?;
                    match sh.store.open_reader(&ns, &key)? {
                        None => Ok(Resp::Miss),
                        Some(reader) => {
                            let op = sh.next_op.fetch_add(1, Ordering::SeqCst);
                            let meta = reader.meta.clone();
                            gets.insert(op, GetOp { reader });
                            Ok(Resp::GetReady { op, meta, chunk_bytes: cs })
                        }
                    }
                }
                Req::GetChunk { op, offset, len } => {
                    let g = gets.get(&op).ok_or_else(|| KvError::BadRequest("unknown get op".into()))?;
                    if pending.is_some() {
                        return Err(KvError::BadRequest("a slot is already outstanding on this connection".into()));
                    }
                    if len == 0 || len > cs || offset + len > g.reader.size() {
                        return Err(KvError::BadRequest("bad chunk offset/len".into()));
                    }
                    let (slot, gen) = sh.slots.alloc(conn, sh.cfg.deadline).ok_or_else(|| KvError::Unavailable("no free staging slot".into()))?;
                    // SAFETY: slot is Held by this connection; nobody else touches it until released.
                    let buf = unsafe { sh.pool.slice_mut(slot as usize * cs as usize, len as usize) };
                    if let Err(e) = g.reader.read_at(offset, buf) {
                        sh.slots.release(slot, gen, conn);
                        return Err(e);
                    }
                    std::sync::atomic::fence(Ordering::Release);
                    *pending = Some(Pending { slot, gen, op, offset, len, put: false });
                    Ok(Resp::Slot { slot, gen, addr: (sh.pool.addr() + slot as usize * cs as usize) as u64, len })
                }
                Req::GetChunkDone { op, slot, gen } => {
                    let pd = pending.take().ok_or_else(|| KvError::BadRequest("no outstanding slot".into()))?;
                    let ok = !pd.put && pd.op == op && pd.slot == slot && pd.gen == gen;
                    sh.slots.release(pd.slot, pd.gen, conn);
                    if ok {
                        Ok(Resp::Ok)
                    } else {
                        Err(KvError::BadRequest("chunk completion does not match the granted slot".into()))
                    }
                }
                Req::GetEnd { op } => {
                    gets.remove(&op);
                    Ok(Resp::Ok)
                }
                Req::Exists { ns, key } => {
                    ns_allowed(sh, &ns)?;
                    Ok(Resp::Bool { value: sh.store.exists(&ns, &key)? })
                }
                Req::Delete { ns, key } => {
                    if !sh.cfg.allow_delete {
                        return Err(KvError::Unauthorized("delete disabled on this daemon".into()));
                    }
                    ns_allowed(sh, &ns)?;
                    Ok(Resp::Bool { value: sh.store.delete(&ns, &key)? })
                }
                _ => Err(KvError::BadRequest("unexpected message".into())),
            }
        }
    }
}
