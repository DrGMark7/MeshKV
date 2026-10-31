//! POSIX file persistence for immutable KV objects (runs on the storage node).
//!
//! Layout: `<root>/<namespace>/<dd>/<digest>.obj`, one file per object:
//!   magic "RKVOBJ01" | u32 LE header_len | header JSON (namespace, key, meta) | payload
//! Publication is atomic and never overwrites: write `.tmp-*`, fsync, `link()` (fails if the name exists),
//! unlink tmp, fsync directory. A PUT is acknowledged only after that sequence.
use crate::error::{KvError, Result};
use crate::object::{hex, key_digest, validate_key, validate_namespace, ObjectMeta};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, FileExt, OpenOptionsExt};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const MAGIC: &[u8; 8] = b"RKVOBJ01";
const MAX_HEADER: u32 = 64 * 1024;
const HASH_BUF: usize = 1 << 20;
static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug)]
pub struct StoreConfig {
    /// Hard cap on bytes of published objects + in-flight reservations.
    pub quota_bytes: u64,
    pub max_object_bytes: u64,
    /// fsync the object file and its directory before acknowledging (durable PUT).
    pub fsync: bool,
    /// Refuse to open a root located under any of these prefixes (model weights must never be touched).
    pub deny_prefixes: Vec<PathBuf>,
}

impl Default for StoreConfig {
    fn default() -> Self {
        StoreConfig {
            quota_bytes: 2 << 30,
            max_object_bytes: 1 << 30,
            fsync: true,
            deny_prefixes: vec![PathBuf::from("/srv/llm-models"), PathBuf::from("/mnt/node2-models")],
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Header {
    namespace: String,
    key: String,
    meta: ObjectMeta,
    created_unix: u64,
}

#[derive(Default)]
struct Usage {
    used: u64,     // bytes of published object files
    reserved: u64, // bytes reserved by in-flight PutTxn
    objects: u64,
}

pub struct FileStore {
    root: PathBuf,
    cfg: StoreConfig,
    usage: Mutex<Usage>,
    _lock: File,
}

pub enum PutOutcome {
    /// Identical object already stored: idempotent success.
    Exists,
    Txn(PutTxn),
}

pub struct PutTxn {
    store: Arc<FileStore>,
    ns: String,
    key: String,
    meta: ObjectMeta,
    tmp_path: PathBuf,
    final_path: PathBuf,
    file: Option<File>,
    hasher: Sha256,
    written: u64,
    reserved: u64,
    done: bool,
}

pub struct ObjectReader {
    file: File,
    pub meta: ObjectMeta,
    payload_off: u64,
}

pub struct StoreStats {
    pub objects: u64,
    pub used_bytes: u64,
    pub reserved_bytes: u64,
    pub quota_bytes: u64,
}

fn now_unix() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

fn mkdir_private(p: &Path) -> Result<()> {
    if let Ok(md) = fs::symlink_metadata(p) {
        if md.file_type().is_symlink() || !md.is_dir() {
            return Err(KvError::Io(format!("{} is not a plain directory", p.display())));
        }
        return Ok(());
    }
    fs::DirBuilder::new().mode(0o700).create(p)?;
    Ok(())
}

fn fsync_dir(p: &Path) -> Result<()> {
    File::open(p)?.sync_all()?;
    Ok(())
}

impl FileStore {
    pub fn open(root: &Path, cfg: StoreConfig) -> Result<Arc<FileStore>> {
        for d in &cfg.deny_prefixes {
            if root.starts_with(d) {
                return Err(KvError::BadRequest(format!("storage root {} is under protected prefix {}", root.display(), d.display())));
            }
        }
        if !root.exists() {
            fs::DirBuilder::new().mode(0o700).recursive(true).create(root)?;
        }
        let root = fs::canonicalize(root)?;
        for d in &cfg.deny_prefixes {
            if root.starts_with(d) {
                return Err(KvError::BadRequest(format!("storage root {} resolves under protected prefix {}", root.display(), d.display())));
            }
        }
        let lock = OpenOptions::new().create(true).write(true).truncate(false).mode(0o600).open(root.join(".kvstored.lock"))?;
        // SAFETY: plain flock on an owned fd.
        if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(KvError::Unavailable(format!("another process already owns {}", root.display())));
        }
        let store = FileStore { root, cfg, usage: Mutex::new(Usage::default()), _lock: lock };
        store.recover()?;
        Ok(Arc::new(store))
    }

    /// Startup recovery (single daemon owns the root): remove our own orphaned `.tmp-*` files, rebuild usage.
    fn recover(&self) -> Result<()> {
        let mut used = 0u64;
        let mut objects = 0u64;
        for ns in fs::read_dir(&self.root)? {
            let ns = ns?;
            if !ns.file_type()?.is_dir() {
                continue;
            }
            for sub in fs::read_dir(ns.path())? {
                let sub = sub?;
                if !sub.file_type()?.is_dir() {
                    continue;
                }
                for f in fs::read_dir(sub.path())? {
                    let f = f?;
                    let name = f.file_name().to_string_lossy().to_string();
                    if name.starts_with(".tmp-") {
                        let _ = fs::remove_file(f.path());
                    } else if name.ends_with(".obj") {
                        used += f.metadata()?.len();
                        objects += 1;
                    }
                }
            }
        }
        let mut u = self.usage.lock().unwrap();
        u.used = used;
        u.objects = objects;
        Ok(())
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn stats(&self) -> StoreStats {
        let u = self.usage.lock().unwrap();
        StoreStats { objects: u.objects, used_bytes: u.used, reserved_bytes: u.reserved, quota_bytes: self.cfg.quota_bytes }
    }

    /// Cheap writability check for health endpoints: creates and removes a probe file.
    pub fn check_writable(&self) -> Result<()> {
        let p = self.root.join(format!(".probe-{}", std::process::id()));
        File::create(&p)?.write_all(b"ok")?;
        fs::remove_file(&p)?;
        Ok(())
    }

    fn paths(&self, ns: &str, key: &str) -> Result<(PathBuf, PathBuf)> {
        validate_namespace(ns)?;
        validate_key(key)?;
        let d = key_digest(ns, key);
        let dir = self.root.join(ns).join(&d[..2]);
        Ok((dir.clone(), dir.join(format!("{}.obj", d))))
    }

    fn read_header(file: &File, expect_ns: Option<(&str, &str)>) -> Result<(Header, u64)> {
        let mut fixed = [0u8; 12];
        file.read_exact_at(&mut fixed, 0).map_err(|_| KvError::ChecksumMismatch("object file shorter than header".into()))?;
        if &fixed[..8] != MAGIC {
            return Err(KvError::ChecksumMismatch("bad object magic".into()));
        }
        let hl = u32::from_le_bytes([fixed[8], fixed[9], fixed[10], fixed[11]]);
        if hl == 0 || hl > MAX_HEADER {
            return Err(KvError::ChecksumMismatch("bad header length".into()));
        }
        let mut hb = vec![0u8; hl as usize];
        file.read_exact_at(&mut hb, 12).map_err(|_| KvError::ChecksumMismatch("truncated header".into()))?;
        let h: Header = serde_json::from_slice(&hb).map_err(|e| KvError::ChecksumMismatch(format!("bad header json: {e}")))?;
        if let Some((ns, key)) = expect_ns {
            if h.namespace != ns || h.key != key {
                return Err(KvError::ChecksumMismatch("object file belongs to a different key".into()));
            }
        }
        Ok((h, 12 + hl as u64))
    }

    /// Returns the stored meta without reading the payload (None = miss). Header-level check only.
    pub fn stat(&self, ns: &str, key: &str) -> Result<Option<ObjectMeta>> {
        let (_, path) = self.paths(ns, key)?;
        match File::open(&path) {
            Ok(f) => {
                let (h, off) = Self::read_header(&f, Some((ns, key)))?;
                let len = f.metadata()?.len();
                if len != off + h.meta.size {
                    return Err(KvError::ChecksumMismatch("object file length does not match header".into()));
                }
                Ok(Some(h.meta))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn exists(&self, ns: &str, key: &str) -> Result<bool> {
        Ok(self.stat(ns, key)?.is_some())
    }

    /// Open an object for reading after verifying its full payload checksum (never serve corrupted bytes).
    pub fn open_reader(&self, ns: &str, key: &str) -> Result<Option<ObjectReader>> {
        let (_, path) = self.paths(ns, key)?;
        let file = match File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let (h, off) = Self::read_header(&file, Some((ns, key)))?;
        let len = file.metadata()?.len();
        if len != off + h.meta.size {
            return Err(KvError::ChecksumMismatch(format!("length {} != header {}+{}", len, off, h.meta.size)));
        }
        let mut hasher = Sha256::new();
        let mut buf = vec![0u8; HASH_BUF];
        let mut pos = 0u64;
        while pos < h.meta.size {
            let n = std::cmp::min(HASH_BUF as u64, h.meta.size - pos) as usize;
            file.read_exact_at(&mut buf[..n], off + pos).map_err(|e| KvError::Io(format!("read payload: {e}")))?;
            hasher.update(&buf[..n]);
            pos += n as u64;
        }
        if hex(&hasher.finalize()) != h.meta.sha256 {
            return Err(KvError::ChecksumMismatch("payload sha256 differs from stored header".into()));
        }
        Ok(Some(ObjectReader { file, meta: h.meta, payload_off: off }))
    }

    /// Convenience for small objects/tests: whole payload, verified.
    pub fn get(&self, ns: &str, key: &str) -> Result<Option<(ObjectMeta, Vec<u8>)>> {
        match self.open_reader(ns, key)? {
            None => Ok(None),
            Some(r) => {
                let mut v = vec![0u8; r.meta.size as usize];
                r.read_at(0, &mut v)?;
                Ok(Some((r.meta.clone(), v)))
            }
        }
    }

    /// Start a PUT. Same key + identical meta already stored -> `Exists`; different -> `Conflict`.
    pub fn begin_put(self: &Arc<Self>, ns: &str, key: &str, meta: ObjectMeta) -> Result<PutOutcome> {
        meta.validate()?;
        let (dir, final_path) = self.paths(ns, key)?;
        if meta.size > self.cfg.max_object_bytes {
            return Err(KvError::TooLarge(format!("{} > max_object_bytes {}", meta.size, self.cfg.max_object_bytes)));
        }
        if let Some(existing) = self.stat(ns, key)? {
            return if existing == meta {
                Ok(PutOutcome::Exists)
            } else {
                Err(KvError::Conflict("key exists with different checksum/metadata (objects are immutable)".into()))
            };
        }
        let header = Header { namespace: ns.to_string(), key: key.to_string(), meta: meta.clone(), created_unix: now_unix() };
        let hb = serde_json::to_vec(&header).map_err(|e| KvError::Io(e.to_string()))?;
        if hb.len() as u32 > MAX_HEADER {
            return Err(KvError::BadRequest("header too large".into()));
        }
        let total = 12 + hb.len() as u64 + meta.size;
        {
            let mut u = self.usage.lock().unwrap();
            if u.used + u.reserved + total > self.cfg.quota_bytes {
                return Err(KvError::QuotaExceeded(format!("{} + {} + {} > quota {}", u.used, u.reserved, total, self.cfg.quota_bytes)));
            }
            u.reserved += total;
        }
        let release = |s: &FileStore| {
            let mut u = s.usage.lock().unwrap();
            u.reserved -= total;
        };
        let r = (|| -> Result<(PathBuf, File)> {
            mkdir_private(&self.root.join(ns))?;
            mkdir_private(&dir)?;
            let tmp = dir.join(format!(".tmp-{}-{}-{}", std::process::id(), TMP_COUNTER.fetch_add(1, Ordering::Relaxed), &key_digest(ns, key)[..16]));
            let mut f = OpenOptions::new().write(true).create_new(true).mode(0o600).open(&tmp)?;
            f.write_all(MAGIC)?;
            f.write_all(&(hb.len() as u32).to_le_bytes())?;
            f.write_all(&hb)?;
            Ok((tmp, f))
        })();
        match r {
            Ok((tmp_path, file)) => Ok(PutOutcome::Txn(PutTxn {
                store: Arc::clone(self),
                ns: ns.to_string(),
                key: key.to_string(),
                meta,
                tmp_path,
                final_path,
                file: Some(file),
                hasher: Sha256::new(),
                written: 0,
                reserved: total,
                done: false,
            })),
            Err(e) => {
                release(self);
                Err(e)
            }
        }
    }

    /// One-shot PUT of an in-memory payload (tests, small objects).
    pub fn put(self: &Arc<Self>, ns: &str, key: &str, fingerprint: &str, format: &str, payload: &[u8]) -> Result<bool> {
        let meta = ObjectMeta::for_payload(fingerprint, format, payload);
        match self.begin_put(ns, key, meta)? {
            PutOutcome::Exists => Ok(false),
            PutOutcome::Txn(mut t) => {
                t.write(payload)?;
                t.commit()
            }
        }
    }

    /// Delete an object (only used to clean up explicitly owned experimental objects).
    pub fn delete(&self, ns: &str, key: &str) -> Result<bool> {
        let (dir, path) = self.paths(ns, key)?;
        let len = match fs::metadata(&path) {
            Ok(m) => m.len(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(e.into()),
        };
        fs::remove_file(&path)?;
        if self.cfg.fsync {
            fsync_dir(&dir)?;
        }
        let mut u = self.usage.lock().unwrap();
        u.used = u.used.saturating_sub(len);
        u.objects = u.objects.saturating_sub(1);
        Ok(true)
    }
}

impl PutTxn {
    pub fn meta(&self) -> &ObjectMeta {
        &self.meta
    }
    pub fn written(&self) -> u64 {
        self.written
    }

    pub fn write(&mut self, chunk: &[u8]) -> Result<()> {
        if self.written + chunk.len() as u64 > self.meta.size {
            return Err(KvError::BadRequest("more payload bytes than declared size".into()));
        }
        self.hasher.update(chunk);
        self.file.as_mut().ok_or_else(|| KvError::Io("txn closed".into()))?.write_all(chunk)?;
        self.written += chunk.len() as u64;
        Ok(())
    }

    /// Verify length + checksum, publish atomically. Returns Ok(true) if newly stored, Ok(false) if an identical
    /// object won a race (idempotent).
    pub fn commit(mut self) -> Result<bool> {
        let r = self.commit_inner();
        self.done = true;
        let _ = fs::remove_file(&self.tmp_path);
        let mut u = self.store.usage.lock().unwrap();
        u.reserved -= self.reserved;
        if let Ok(true) = r {
            u.used += self.reserved;
            u.objects += 1;
        }
        r
    }

    fn commit_inner(&mut self) -> Result<bool> {
        if self.written != self.meta.size {
            return Err(KvError::ChecksumMismatch(format!("received {} of {} bytes", self.written, self.meta.size)));
        }
        let got = hex(&self.hasher.clone().finalize());
        if got != self.meta.sha256 {
            return Err(KvError::ChecksumMismatch("payload sha256 differs from declared checksum".into()));
        }
        let f = self.file.take().ok_or_else(|| KvError::Io("txn closed".into()))?;
        if self.store.cfg.fsync {
            f.sync_all()?;
        }
        drop(f);
        let dir = self.final_path.parent().unwrap().to_path_buf();
        match fs::hard_link(&self.tmp_path, &self.final_path) {
            Ok(()) => {
                if self.store.cfg.fsync {
                    fsync_dir(&dir)?;
                }
                Ok(true)
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => match self.store.stat(&self.ns, &self.key)? {
                Some(m) if m == self.meta => Ok(false),
                _ => Err(KvError::Conflict("key was published concurrently with different content".into())),
            },
            Err(e) => Err(e.into()),
        }
    }

    pub fn abort(mut self) {
        self.cleanup();
    }

    fn cleanup(&mut self) {
        if self.done {
            return;
        }
        self.done = true;
        self.file.take();
        let _ = fs::remove_file(&self.tmp_path);
        let mut u = self.store.usage.lock().unwrap();
        u.reserved -= self.reserved;
    }
}

impl Drop for PutTxn {
    fn drop(&mut self) {
        // An interrupted PUT must never publish anything and must give the reservation back.
        self.cleanup();
    }
}

impl ObjectReader {
    /// Read `buf.len()` payload bytes starting at payload offset `offset`.
    pub fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        if offset + buf.len() as u64 > self.meta.size {
            return Err(KvError::BadRequest("read beyond object end".into()));
        }
        self.file.read_exact_at(buf, self.payload_off + offset).map_err(|e| KvError::Io(format!("read: {e}")))
    }
    pub fn size(&self) -> u64 {
        self.meta.size
    }
}

#[allow(dead_code)]
fn _assert_send_sync() {
    fn is<T: Send + Sync>() {}
    is::<FileStore>();
}
