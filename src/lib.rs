//! rdma-kv-store: an independent store for opaque, immutable KV-cache objects.
//! The library never interprets payload bytes (no tensors, tokenizers, attention): callers own their layout.
pub mod client;
pub mod error;
pub mod file_store;
pub mod nixl_transport;
pub mod object;
pub mod protocol;
pub mod server;
pub mod util;

pub use client::{Client, ClientConfig, PutResult, Timing};
pub use error::{KvError, Result};
pub use file_store::{FileStore, ObjectReader, PutOutcome, PutTxn, StoreConfig, StoreStats};
pub use object::{ObjectMeta, SCHEMA_VERSION};
pub use server::ServerConfig;
