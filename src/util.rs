//! Small shared helpers for the binaries: flag/config parsing and deterministic test payloads.
use crate::error::{KvError, Result};
use std::collections::HashMap;

/// Parse `--key value` flags and optional `--config file` (lines `key=value`, `#` comments). Flags win over file.
pub fn parse_args(args: &[String]) -> Result<(Vec<String>, HashMap<String, String>)> {
    let mut pos = vec![];
    let mut m: HashMap<String, String> = HashMap::new();
    let mut i = 0;
    while i < args.len() {
        let a = &args[i];
        if let Some(k) = a.strip_prefix("--") {
            let k = k.replace('-', "_");
            if i + 1 < args.len() && !args[i + 1].starts_with("--") {
                m.insert(k, args[i + 1].clone());
                i += 2;
            } else {
                m.insert(k, "true".into());
                i += 1;
            }
        } else {
            pos.push(a.clone());
            i += 1;
        }
    }
    if let Some(path) = m.get("config").cloned() {
        let txt = std::fs::read_to_string(&path).map_err(|e| KvError::BadRequest(format!("config {path}: {e}")))?;
        for line in txt.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some((k, v)) = line.split_once('=') {
                m.entry(k.trim().replace('-', "_")).or_insert_with(|| v.trim().to_string());
            }
        }
    }
    Ok((pos, m))
}

pub fn get_u64(m: &HashMap<String, String>, k: &str, default: u64) -> Result<u64> {
    match m.get(k) {
        None => Ok(default),
        Some(v) => v.parse::<u64>().map_err(|_| KvError::BadRequest(format!("--{k} must be an integer"))),
    }
}

pub fn get_bool(m: &HashMap<String, String>, k: &str, default: bool) -> bool {
    match m.get(k).map(|s| s.as_str()) {
        None => default,
        Some("true") | Some("1") | Some("yes") | Some("on") => true,
        Some(_) => false,
    }
}

/// Read a token from a file (the secret never appears on a command line or in logs).
pub fn read_token(m: &HashMap<String, String>) -> Result<String> {
    let p = m.get("token_file").ok_or_else(|| KvError::BadRequest("--token-file is required".into()))?;
    let md = std::fs::metadata(p).map_err(|e| KvError::BadRequest(format!("token file {p}: {e}")))?;
    {
        use std::os::unix::fs::PermissionsExt;
        if md.permissions().mode() & 0o077 != 0 {
            return Err(KvError::BadRequest(format!("token file {p} must not be group/world accessible (chmod 600)")));
        }
    }
    Ok(std::fs::read_to_string(p)?.trim().to_string())
}

/// Deterministic, practically incompressible bytes (xorshift64*), so any corruption is detectable by regeneration.
pub fn gen_payload(seed: u64, len: usize) -> Vec<u8> {
    let mut x = seed.wrapping_mul(0x9E3779B97F4A7C15) | 1;
    let mut v = vec![0u8; len];
    let mut i = 0;
    while i < len {
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        let r = x.wrapping_mul(0x2545F4914F6CDD1D).to_le_bytes();
        let n = std::cmp::min(8, len - i);
        v[i..i + n].copy_from_slice(&r[..n]);
        i += n;
    }
    v
}
