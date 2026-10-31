//! Structured errors. The string codes are part of the wire protocol.
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KvError {
    Miss,
    Conflict(String),
    ChecksumMismatch(String),
    Timeout(String),
    Unavailable(String),
    Incompatible(String),
    QuotaExceeded(String),
    Io(String),
    BadRequest(String),
    TooLarge(String),
    Unauthorized(String),
}

impl KvError {
    pub fn code(&self) -> &'static str {
        match self {
            KvError::Miss => "MISS",
            KvError::Conflict(_) => "CONFLICT",
            KvError::ChecksumMismatch(_) => "CHECKSUM_MISMATCH",
            KvError::Timeout(_) => "TIMEOUT",
            KvError::Unavailable(_) => "UNAVAILABLE",
            KvError::Incompatible(_) => "INCOMPATIBLE",
            KvError::QuotaExceeded(_) => "QUOTA_EXCEEDED",
            KvError::Io(_) => "IO_ERROR",
            KvError::BadRequest(_) => "BAD_REQUEST",
            KvError::TooLarge(_) => "TOO_LARGE",
            KvError::Unauthorized(_) => "UNAUTHORIZED",
        }
    }
    pub fn message(&self) -> String {
        match self {
            KvError::Miss => String::new(),
            KvError::Conflict(m)
            | KvError::ChecksumMismatch(m)
            | KvError::Timeout(m)
            | KvError::Unavailable(m)
            | KvError::Incompatible(m)
            | KvError::QuotaExceeded(m)
            | KvError::Io(m)
            | KvError::BadRequest(m)
            | KvError::TooLarge(m)
            | KvError::Unauthorized(m) => m.clone(),
        }
    }
    pub fn from_code(code: &str, msg: String) -> KvError {
        match code {
            "MISS" => KvError::Miss,
            "CONFLICT" => KvError::Conflict(msg),
            "CHECKSUM_MISMATCH" => KvError::ChecksumMismatch(msg),
            "TIMEOUT" => KvError::Timeout(msg),
            "UNAVAILABLE" => KvError::Unavailable(msg),
            "INCOMPATIBLE" => KvError::Incompatible(msg),
            "QUOTA_EXCEEDED" => KvError::QuotaExceeded(msg),
            "BAD_REQUEST" => KvError::BadRequest(msg),
            "TOO_LARGE" => KvError::TooLarge(msg),
            "UNAUTHORIZED" => KvError::Unauthorized(msg),
            _ => KvError::Io(msg),
        }
    }
}

impl fmt::Display for KvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code(), self.message())
    }
}
impl std::error::Error for KvError {}
impl From<std::io::Error> for KvError {
    fn from(e: std::io::Error) -> Self {
        KvError::Io(e.to_string())
    }
}
pub type Result<T> = std::result::Result<T, KvError>;
