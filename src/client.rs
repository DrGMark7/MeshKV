//! GPU-node client: bounded registered staging buffer + NIXL/UCX transfers + control connections.
//! Payload is copied between caller memory and a registered CPU buffer (CPU staging is mandatory: no GPU memory
//! is ever registered), then moved by RDMA WRITE (PUT) / RDMA READ (GET).
use crate::error::{KvError, Result};
use crate::nixl_transport::{NixlAgent, Op, RegisteredBuf};
use crate::object::{hex, ObjectMeta, SCHEMA_VERSION};
use crate::protocol::*;
use sha2::{Digest, Sha256};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct ClientConfig {
    pub server: String,
    pub token: String,
    /// number of parallel operations (control connections + staging slots); the plan caps this at 4
    pub inflight: usize,
    pub chunk_bytes: u64,
    pub deadline: Duration,
    pub ucx_net_devices: String,
    pub ucx_tls: String,
    pub nixl_prefix: String,
}

impl Default for ClientConfig {
    fn default() -> Self {
        ClientConfig {
            server: "10.10.10.2:7471".into(),
            token: String::new(),
            inflight: 1,
            chunk_bytes: 64 << 20,
            deadline: Duration::from_secs(30),
            ucx_net_devices: "mlx5_0:1".into(),
            ucx_tls: "^tcp".into(),
            nixl_prefix: "/home/hpcnc/rdma-kv-experiments/build/nixl-install".into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Timing {
    pub total: Duration,
    pub hash: Duration,
    pub control: Duration,
    pub transfer: Duration,
    pub copy: Duration,
    pub bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutResult {
    /// false when an identical object already existed (idempotent success)
    pub stored: bool,
    pub bytes: u64,
    /// sha256 (hex) of the payload as declared to the server
    pub sha256: String,
}

struct Conn {
    stream: TcpStream,
    slot: usize, // staging slot owned by this connection
}

pub struct Client {
    cfg: ClientConfig,
    agent: Arc<NixlAgent>,
    staging: RegisteredBuf,
    server_agent: String,
    session: u64,
    chunk: u64,
    free: Mutex<Vec<Conn>>,
    cv: Condvar,
    poisoned: AtomicBool,
}

fn connect(cfg: &ClientConfig) -> Result<TcpStream> {
    let addr = cfg
        .server
        .to_socket_addrs()
        .map_err(|e| KvError::Unavailable(format!("resolve {}: {e}", cfg.server)))?
        .next()
        .ok_or_else(|| KvError::Unavailable("no address".into()))?;
    let s = TcpStream::connect_timeout(&addr, cfg.deadline).map_err(io_err)?;
    s.set_nodelay(true).ok();
    s.set_read_timeout(Some(cfg.deadline)).ok();
    s.set_write_timeout(Some(cfg.deadline)).ok();
    Ok(s)
}

fn expect_ok(r: Resp) -> Result<Resp> {
    match r {
        Resp::Err { code, msg } => Err(KvError::from_code(&code, msg)),
        other => Ok(other),
    }
}

fn rpc(s: &mut TcpStream, req: &Req) -> Result<Resp> {
    write_frame(s, req)?;
    expect_ok(read_frame(s)?)
}

impl Client {
    pub fn connect(cfg: ClientConfig) -> Result<Client> {
        if cfg.inflight == 0 || cfg.inflight > 4 {
            return Err(KvError::BadRequest("inflight must be 1..=4".into()));
        }
        if cfg.chunk_bytes as usize * cfg.inflight > (1 << 30) {
            return Err(KvError::BadRequest("staging must stay <= 1 GiB".into()));
        }
        std::env::set_var("UCX_NET_DEVICES", &cfg.ucx_net_devices);
        std::env::set_var("UCX_TLS", &cfg.ucx_tls);
        if std::env::var("NIXL_PLUGIN_DIR").is_err() {
            std::env::set_var("NIXL_PLUGIN_DIR", format!("{}/lib/x86_64-linux-gnu/plugins", cfg.nixl_prefix));
        }
        // unique agent name per process start: a restarted client never reuses a stale remote identity
        let name = format!(
            "kvclient-{}-{:x}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0)
        );
        let agent = Arc::new(NixlAgent::new(&name)?);
        let staging = RegisteredBuf::new(&agent, cfg.chunk_bytes as usize * cfg.inflight)?;
        let md = hex(&agent.local_metadata()?);
        let mut first = connect(&cfg)?;
        let (session, server_agent, slot_bytes) = match rpc(&mut first, &Req::Hello { v: PROTOCOL_VERSION, token: cfg.token.clone(), agent: name.clone(), md })?
        {
            Resp::HelloOk { session, agent: sa, md, slot_bytes, .. } => {
                let loaded = agent.load_remote(&hex_decode(&md)?)?;
                if loaded != sa {
                    return Err(KvError::Unavailable("server metadata name mismatch".into()));
                }
                (session, sa, slot_bytes)
            }
            _ => return Err(KvError::Unavailable("unexpected reply to HELLO".into())),
        };
        let chunk = std::cmp::min(cfg.chunk_bytes, slot_bytes);
        let mut conns = vec![Conn { stream: first, slot: 0 }];
        for i in 1..cfg.inflight {
            let mut s = connect(&cfg)?;
            rpc(&mut s, &Req::Attach { session, token: cfg.token.clone() })?;
            conns.push(Conn { stream: s, slot: i });
        }
        Ok(Client { cfg, agent, staging, server_agent, session, chunk, free: Mutex::new(conns), cv: Condvar::new(), poisoned: AtomicBool::new(false) })
    }

    pub fn session(&self) -> u64 {
        self.session
    }

    fn acquire(&self) -> Result<Conn> {
        if self.poisoned.load(Ordering::SeqCst) {
            return Err(KvError::Unavailable("client poisoned after an uncertain transfer; create a new Client".into()));
        }
        let mut g = self.free.lock().unwrap();
        let end = Instant::now() + self.cfg.deadline;
        loop {
            if let Some(c) = g.pop() {
                return Ok(c);
            }
            let now = Instant::now();
            if now >= end {
                return Err(KvError::Timeout("no free client connection".into()));
            }
            let (ng, _) = self.cv.wait_timeout(g, end - now).unwrap();
            g = ng;
        }
    }

    fn release(&self, c: Conn, ok: bool) {
        let c = if ok {
            Some(c)
        } else {
            // a failed connection is replaced by a fresh one (new stream, same staging slot)
            let slot = c.slot;
            drop(c);
            connect(&self.cfg)
                .ok()
                .and_then(|mut s| rpc(&mut s, &Req::Attach { session: self.session, token: self.cfg.token.clone() }).ok().map(|_| Conn { stream: s, slot }))
        };
        if let Some(c) = c {
            self.free.lock().unwrap().push(c);
            self.cv.notify_one();
        }
    }

    fn with_conn<T>(&self, f: &dyn Fn(&mut Conn) -> Result<T>) -> Result<T> {
        self.with_conn_opts(true, f)
    }

    fn with_conn_opts<T>(&self, retry: bool, f: &dyn Fn(&mut Conn) -> Result<T>) -> Result<T> {
        let mut attempt = if retry { 0 } else { 1 };
        loop {
            let mut c = self.acquire()?;
            let r = f(&mut c);
            match &r {
                Ok(_) => self.release(c, true),
                Err(e) => {
                    if matches!(e, KvError::Timeout(m) if m.starts_with("nixl transfer")) {
                        // completion unknown: never reuse buffers of this client
                        self.poisoned.store(true, Ordering::SeqCst);
                    }
                    let transport_level = is_transport_level(e);
                    self.release(c, !transport_level);
                    if transport_level && attempt == 0 && !self.poisoned.load(Ordering::SeqCst) {
                        attempt += 1;
                        continue; // one safe retry: all operations are idempotent
                    }
                }
            }
            return r;
        }
    }

    pub fn health(&self) -> Result<Resp> {
        self.with_conn(&|c| rpc(&mut c.stream, &Req::Health { token: self.cfg.token.clone() }))
    }

    pub fn exists(&self, ns: &str, key: &str) -> Result<bool> {
        match self.with_conn(&|c| rpc(&mut c.stream, &Req::Exists { ns: ns.to_string(), key: key.to_string() }))? {
            Resp::Bool { value } => Ok(value),
            _ => Err(KvError::Io("unexpected reply".into())),
        }
    }

    pub fn delete(&self, ns: &str, key: &str) -> Result<bool> {
        match self.with_conn(&|c| rpc(&mut c.stream, &Req::Delete { ns: ns.to_string(), key: key.to_string() }))? {
            Resp::Bool { value } => Ok(value),
            _ => Err(KvError::Io("unexpected reply".into())),
        }
    }

    pub fn put(&self, ns: &str, key: &str, fingerprint: &str, format: &str, payload: &[u8]) -> Result<PutResult> {
        self.put_timed(ns, key, fingerprint, format, payload).map(|(r, _)| r)
    }

    pub fn put_timed(&self, ns: &str, key: &str, fingerprint: &str, format: &str, payload: &[u8]) -> Result<(PutResult, Timing)> {
        let t0 = Instant::now();
        let th = Instant::now();
        let meta = ObjectMeta::for_payload(fingerprint, format, payload);
        let hash = th.elapsed();
        let (r, mut t) = self.put_with(ns, key, meta, &|off, buf| {
            buf.copy_from_slice(&payload[off as usize..off as usize + buf.len()]);
            Ok(())
        })?;
        t.hash = hash;
        t.total = t0.elapsed();
        Ok((r, t))
    }

    /// Core PUT. `fill(offset, buf)` supplies payload bytes; `meta` must already carry size + sha256.
    pub fn put_with(&self, ns: &str, key: &str, meta: ObjectMeta, fill: &dyn Fn(u64, &mut [u8]) -> Result<()>) -> Result<(PutResult, Timing)> {
        if meta.schema_version != SCHEMA_VERSION {
            return Err(KvError::Incompatible("schema_version".into()));
        }
        self.with_conn(&|c| self.put_once(c, ns, key, &meta, fill, None))
    }

    /// Test hook for the interrupted-transfer check: drop the control connection after `n` chunks, without commit.
    pub fn put_interrupted(&self, ns: &str, key: &str, fingerprint: &str, format: &str, payload: &[u8], after_chunks: u32) -> Result<()> {
        let meta = ObjectMeta::for_payload(fingerprint, format, payload);
        let mut c = self.acquire()?;
        let r = self.put_once(
            &mut c,
            ns,
            key,
            &meta,
            &|off, buf| {
                buf.copy_from_slice(&payload[off as usize..off as usize + buf.len()]);
                Ok(())
            },
            Some(after_chunks),
        );
        self.release(c, false); // connection is closed on purpose; server must discard the partial object
        match r {
            Err(KvError::Unavailable(m)) if m == "fault injected" => Ok(()),
            Err(e) => Err(e),
            Ok(_) => Err(KvError::Io("transfer completed before fault point".into())),
        }
    }

    fn put_once(
        &self,
        c: &mut Conn,
        ns: &str,
        key: &str,
        meta: &ObjectMeta,
        fill: &dyn Fn(u64, &mut [u8]) -> Result<()>,
        fault_after: Option<u32>,
    ) -> Result<(PutResult, Timing)> {
        let mut t = Timing { bytes: meta.size, ..Default::default() };
        let t0 = Instant::now();
        let mut ctl = Duration::ZERO;
        let tc = Instant::now();
        let (op, srv_chunk) = match rpc(&mut c.stream, &Req::PutBegin { ns: ns.to_string(), key: key.to_string(), meta: meta.clone() })? {
            Resp::PutExists => {
                t.control = tc.elapsed();
                t.total = t0.elapsed();
                return Ok((PutResult { stored: false, bytes: meta.size, sha256: meta.sha256.clone() }, t));
            }
            Resp::PutReady { op, chunk_bytes } => (op, chunk_bytes),
            _ => return Err(KvError::Io("unexpected reply to PUT_BEGIN".into())),
        };
        ctl += tc.elapsed();
        let chunk = std::cmp::min(self.chunk, srv_chunk);
        let local_base = c.slot * self.cfg.chunk_bytes as usize;
        let mut off = 0u64;
        let mut n_chunks = 0u32;
        while off < meta.size {
            let len = std::cmp::min(chunk, meta.size - off);
            let tcp = Instant::now();
            // SAFETY: this connection exclusively owns its staging slot; no transfer on it is in flight here.
            let buf = unsafe { self.staging.slice_mut(local_base, len as usize) };
            fill(off, buf)?;
            t.copy += tcp.elapsed();
            let tc = Instant::now();
            let (slot, gen, addr) = match rpc(&mut c.stream, &Req::PutChunk { op, offset: off, len })? {
                Resp::Slot { slot, gen, addr, .. } => (slot, gen, addr),
                _ => return Err(KvError::Io("unexpected reply to PUT_CHUNK".into())),
            };
            ctl += tc.elapsed();
            let tx = Instant::now();
            self.agent.transfer(Op::Write, &self.server_agent, self.staging.addr() + local_base, addr as usize, len as usize, self.cfg.deadline)?;
            t.transfer += tx.elapsed();
            n_chunks += 1;
            if let Some(n) = fault_after {
                if n_chunks >= n {
                    return Err(KvError::Unavailable("fault injected".into()));
                }
            }
            let tc = Instant::now();
            expect_done(rpc(&mut c.stream, &Req::PutChunkDone { op, slot, gen, offset: off, len })?)?;
            ctl += tc.elapsed();
            off += len;
        }
        let tc = Instant::now();
        let stored = match rpc(&mut c.stream, &Req::PutCommit { op })? {
            Resp::PutDone { stored } => stored,
            _ => return Err(KvError::Io("unexpected reply to PUT_COMMIT".into())),
        };
        ctl += tc.elapsed();
        t.control = ctl;
        t.total = t0.elapsed();
        Ok((PutResult { stored, bytes: meta.size, sha256: meta.sha256.clone() }, t))
    }

    pub fn get(&self, ns: &str, key: &str) -> Result<Option<(ObjectMeta, Vec<u8>)>> {
        self.get_timed(ns, key).map(|o| o.map(|(m, v, _)| (m, v)))
    }

    pub fn get_timed(&self, ns: &str, key: &str) -> Result<Option<(ObjectMeta, Vec<u8>, Timing)>> {
        let mut attempt = 0;
        loop {
            let mut out: Vec<u8> = Vec::new();
            let r = self.get_to_opts(false, ns, key, &mut |chunk| {
                out.extend_from_slice(chunk);
                Ok(())
            });
            match r {
                Ok(o) => return Ok(o.map(|(m, t)| (m, out, t))),
                Err(e) if attempt == 0 && is_transport_level(&e) && !self.poisoned.load(Ordering::SeqCst) => {
                    attempt += 1; // one safe retry of an idempotent read, with a fresh buffer
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Streaming GET: `sink` receives chunks in order; the checksum of the full object is verified before success.
    /// No automatic retry here (the sink may already have consumed data); use `get`/`get_timed` for retrying reads.
    pub fn get_to(&self, ns: &str, key: &str, sink: &mut dyn FnMut(&[u8]) -> Result<()>) -> Result<Option<(ObjectMeta, Timing)>> {
        self.get_to_opts(false, ns, key, sink)
    }

    fn get_to_opts(&self, retry: bool, ns: &str, key: &str, sink: &mut dyn FnMut(&[u8]) -> Result<()>) -> Result<Option<(ObjectMeta, Timing)>> {
        let cell = std::cell::RefCell::new(sink);
        self.with_conn_opts(retry, &|c| {
            let mut s = cell.borrow_mut();
            self.get_once(c, ns, key, &mut **s)
        })
    }

    fn get_once(&self, c: &mut Conn, ns: &str, key: &str, sink: &mut dyn FnMut(&[u8]) -> Result<()>) -> Result<Option<(ObjectMeta, Timing)>> {
        let t0 = Instant::now();
        let mut t = Timing::default();
        let mut ctl = Duration::ZERO;
        let tc = Instant::now();
        let (op, meta, srv_chunk) = match rpc(&mut c.stream, &Req::GetBegin { ns: ns.to_string(), key: key.to_string() }) {
            Ok(Resp::Miss) => return Ok(None),
            Ok(Resp::GetReady { op, meta, chunk_bytes }) => (op, meta, chunk_bytes),
            Ok(_) => return Err(KvError::Io("unexpected reply to GET_BEGIN".into())),
            Err(e) => return Err(e),
        };
        ctl += tc.elapsed();
        t.bytes = meta.size;
        let chunk = std::cmp::min(self.chunk, srv_chunk);
        let local_base = c.slot * self.cfg.chunk_bytes as usize;
        let mut hasher = Sha256::new();
        let mut off = 0u64;
        while off < meta.size {
            let len = std::cmp::min(chunk, meta.size - off);
            let tc = Instant::now();
            let (slot, gen, addr) = match rpc(&mut c.stream, &Req::GetChunk { op, offset: off, len })? {
                Resp::Slot { slot, gen, addr, .. } => (slot, gen, addr),
                _ => return Err(KvError::Io("unexpected reply to GET_CHUNK".into())),
            };
            ctl += tc.elapsed();
            let tx = Instant::now();
            self.agent.transfer(Op::Read, &self.server_agent, self.staging.addr() + local_base, addr as usize, len as usize, self.cfg.deadline)?;
            t.transfer += tx.elapsed();
            let tcp = Instant::now();
            // SAFETY: slot owned by this connection; the READ above completed.
            let data = unsafe { self.staging.slice(local_base, len as usize) };
            hasher.update(data);
            sink(data)?;
            t.copy += tcp.elapsed();
            let tc = Instant::now();
            expect_done(rpc(&mut c.stream, &Req::GetChunkDone { op, slot, gen })?)?;
            ctl += tc.elapsed();
            off += len;
        }
        let th = Instant::now();
        let got = hex(&hasher.finalize());
        t.hash = th.elapsed();
        let tc = Instant::now();
        let _ = rpc(&mut c.stream, &Req::GetEnd { op });
        ctl += tc.elapsed();
        if got != meta.sha256 {
            return Err(KvError::ChecksumMismatch("received payload differs from stored checksum".into()));
        }
        t.control = ctl;
        t.total = t0.elapsed();
        Ok(Some((meta, t)))
    }
}

fn is_transport_level(e: &KvError) -> bool {
    matches!(e, KvError::Unavailable(_) | KvError::Io(_) | KvError::Timeout(_))
}

fn expect_done(r: Resp) -> Result<()> {
    match r {
        Resp::Ok => Ok(()),
        _ => Err(KvError::Io("unexpected reply".into())),
    }
}
