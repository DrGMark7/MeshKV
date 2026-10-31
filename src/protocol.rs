//! Control-plane protocol: small length-prefixed JSON frames over TCP on the private network.
//! No payload ever travels here; payload moves by NIXL/UCX RDMA between registered DRAM buffers.
use crate::error::{KvError, Result};
use crate::object::ObjectMeta;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::io::{Read, Write};
use std::net::TcpStream;

pub const PROTOCOL_VERSION: u32 = 1;
pub const MAX_FRAME: u32 = 4 << 20; // NIXL metadata blobs are a few KB

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "t")]
pub enum Req {
    /// Unauthenticated-by-session liveness + storage-writability probe (still needs the token).
    Health {
        token: String,
    },
    Hello {
        v: u32,
        token: String,
        agent: String,
        md: String,
    },
    Attach {
        session: u64,
        token: String,
    },
    PutBegin {
        ns: String,
        key: String,
        meta: ObjectMeta,
    },
    PutChunk {
        op: u64,
        offset: u64,
        len: u64,
    },
    PutChunkDone {
        op: u64,
        slot: u32,
        gen: u64,
        offset: u64,
        len: u64,
    },
    PutCommit {
        op: u64,
    },
    PutAbort {
        op: u64,
    },
    GetBegin {
        ns: String,
        key: String,
    },
    GetChunk {
        op: u64,
        offset: u64,
        len: u64,
    },
    GetChunkDone {
        op: u64,
        slot: u32,
        gen: u64,
    },
    GetEnd {
        op: u64,
    },
    Exists {
        ns: String,
        key: String,
    },
    Delete {
        ns: String,
        key: String,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "t")]
pub enum Resp {
    Health {
        ok: bool,
        writable: bool,
        objects: u64,
        used_bytes: u64,
        reserved_bytes: u64,
        quota_bytes: u64,
        slots_total: u32,
        slots_free: u32,
        slots_quarantined: u32,
        version: String,
    },
    HelloOk {
        session: u64,
        agent: String,
        md: String,
        slot_bytes: u64,
        slots: u32,
        max_object_bytes: u64,
        version: String,
    },
    Ok,
    PutExists,
    PutReady {
        op: u64,
        chunk_bytes: u64,
    },
    Slot {
        slot: u32,
        gen: u64,
        addr: u64,
        len: u64,
    },
    PutDone {
        stored: bool,
    },
    Miss,
    GetReady {
        op: u64,
        meta: ObjectMeta,
        chunk_bytes: u64,
    },
    Bool {
        value: bool,
    },
    Err {
        code: String,
        msg: String,
    },
}

impl Resp {
    pub fn err(e: &KvError) -> Resp {
        Resp::Err { code: e.code().to_string(), msg: e.message() }
    }
}

pub fn write_frame<T: Serialize>(s: &mut TcpStream, v: &T) -> Result<()> {
    let b = serde_json::to_vec(v).map_err(|e| KvError::Io(e.to_string()))?;
    if b.len() as u32 > MAX_FRAME {
        return Err(KvError::BadRequest("frame too large".into()));
    }
    s.write_all(&(b.len() as u32).to_be_bytes()).and_then(|_| s.write_all(&b)).and_then(|_| s.flush()).map_err(io_err)
}

pub fn read_frame<T: DeserializeOwned>(s: &mut TcpStream) -> Result<T> {
    let mut l = [0u8; 4];
    s.read_exact(&mut l).map_err(io_err)?;
    let n = u32::from_be_bytes(l);
    if n == 0 || n > MAX_FRAME {
        return Err(KvError::BadRequest(format!("bad frame length {n}")));
    }
    let mut b = vec![0u8; n as usize];
    s.read_exact(&mut b).map_err(io_err)?;
    serde_json::from_slice(&b).map_err(|e| KvError::BadRequest(format!("bad frame json: {e}")))
}

/// Timeouts and resets are reported as TIMEOUT / UNAVAILABLE, never as hangs.
pub fn io_err(e: std::io::Error) -> KvError {
    use std::io::ErrorKind::*;
    match e.kind() {
        WouldBlock | TimedOut => KvError::Timeout(e.to_string()),
        UnexpectedEof | ConnectionReset | ConnectionAborted | BrokenPipe | ConnectionRefused | NotConnected => KvError::Unavailable(e.to_string()),
        _ => KvError::Io(e.to_string()),
    }
}

pub fn hex_decode(s: &str) -> Result<Vec<u8>> {
    if s.len() % 2 != 0 {
        return Err(KvError::BadRequest("odd hex length".into()));
    }
    (0..s.len() / 2).map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).map_err(|_| KvError::BadRequest("bad hex".into()))).collect()
}

/// Constant-time token comparison.
pub fn token_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    let mut d = (a.len() ^ b.len()) as u8;
    for i in 0..a.len().min(b.len()) {
        d |= a[i] ^ b[i];
    }
    d == 0
}
