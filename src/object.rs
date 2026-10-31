//! Immutable object identity and metadata. The payload is opaque bytes.
use crate::error::{KvError, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const SCHEMA_VERSION: u32 = 1;
pub const MAX_KEY_BYTES: usize = 512;
pub const MAX_NAMESPACE_BYTES: usize = 64;

/// Metadata carried with every object. `fingerprint` is an opaque producer/model compatibility string:
/// the caller decides what goes in it (model revision, quantisation, kv dtype, ...); the store only compares it.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectMeta {
    pub schema_version: u32,
    pub fingerprint: String,
    pub format: String,
    pub size: u64,
    pub sha256: String,
}

impl ObjectMeta {
    pub fn for_payload(fingerprint: &str, format: &str, payload: &[u8]) -> ObjectMeta {
        ObjectMeta {
            schema_version: SCHEMA_VERSION,
            fingerprint: fingerprint.to_string(),
            format: format.to_string(),
            size: payload.len() as u64,
            sha256: hex(&Sha256::digest(payload)),
        }
    }
    pub fn validate(&self) -> Result<()> {
        if self.schema_version != SCHEMA_VERSION {
            return Err(KvError::Incompatible(format!("schema_version {} != {}", self.schema_version, SCHEMA_VERSION)));
        }
        if self.fingerprint.len() > 1024 || self.format.len() > 128 {
            return Err(KvError::BadRequest("fingerprint/format too long".into()));
        }
        if self.sha256.len() != 64 || !self.sha256.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(KvError::BadRequest("sha256 must be 64 hex chars".into()));
        }
        Ok(())
    }
}

pub fn hex(bytes: &[u8]) -> String {
    const T: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(T[(b >> 4) as usize] as char);
        s.push(T[(b & 15) as usize] as char);
    }
    s
}

/// Namespace becomes a directory name: restrict it hard.
pub fn validate_namespace(ns: &str) -> Result<()> {
    let ok = !ns.is_empty()
        && ns.len() <= MAX_NAMESPACE_BYTES
        && ns != "."
        && ns != ".."
        && !ns.starts_with('.')
        && ns.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.');
    if ok {
        Ok(())
    } else {
        Err(KvError::BadRequest(format!("invalid namespace {:?} (use [A-Za-z0-9._-], max {} chars, no leading dot)", ns, MAX_NAMESPACE_BYTES)))
    }
}

/// Keys are opaque logical identities, never used as paths.
pub fn validate_key(key: &str) -> Result<()> {
    if key.is_empty() || key.len() > MAX_KEY_BYTES || key.contains('\0') {
        return Err(KvError::BadRequest(format!("invalid key (1..={} bytes, no NUL)", MAX_KEY_BYTES)));
    }
    Ok(())
}

/// File name stem for a (namespace, key): a digest, so a key can never escape the store directory.
pub fn key_digest(ns: &str, key: &str) -> String {
    let mut h = Sha256::new();
    h.update(b"rdma-kv-store/v1\0");
    h.update(ns.as_bytes());
    h.update(b"\0");
    h.update(key.as_bytes());
    hex(&h.finalize())
}
