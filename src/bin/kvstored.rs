//! kvstored: storage-node daemon. Flags or `--config file` (key=value). See README.md.
use rdma_kv_store::util::*;
use rdma_kv_store::{server, KvError, ServerConfig};
use std::path::PathBuf;
use std::time::Duration;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(&args) {
        Ok(()) => {}
        Err(e) => {
            eprintln!("kvstored: {e}");
            std::process::exit(if matches!(e, KvError::BadRequest(_)) { 2 } else { 1 });
        }
    }
}

fn run(args: &[String]) -> Result<(), KvError> {
    let (_, m) = parse_args(args)?;
    if m.contains_key("help") {
        println!("usage: kvstored --config FILE | --listen IP:PORT --root DIR --token-file F [--ram-pool-mib 512 --chunk-mib 64 --max-object-mib 1024 --quota-mib 2048 --fsync true --deadline-ms 30000 --quarantine-ms 60000 --namespaces a,b --allow-delete --ucx-net-devices mlx5_0:1 --ucx-tls ^tcp --ucx-log info --nixl-prefix DIR --pidfile F --log-level info]");
        return Ok(());
    }
    let d = ServerConfig::default();
    let cfg = ServerConfig {
        listen: m.get("listen").cloned().unwrap_or(d.listen),
        storage_root: m.get("root").map(PathBuf::from).unwrap_or(d.storage_root),
        ram_pool_bytes: get_u64(&m, "ram_pool_mib", d.ram_pool_bytes >> 20)? << 20,
        chunk_bytes: get_u64(&m, "chunk_mib", d.chunk_bytes >> 20)? << 20,
        max_object_bytes: get_u64(&m, "max_object_mib", d.max_object_bytes >> 20)? << 20,
        file_quota_bytes: get_u64(&m, "quota_mib", d.file_quota_bytes >> 20)? << 20,
        fsync: get_bool(&m, "fsync", d.fsync),
        token: read_token(&m)?,
        deadline: Duration::from_millis(get_u64(&m, "deadline_ms", d.deadline.as_millis() as u64)?),
        idle: Duration::from_millis(get_u64(&m, "idle_ms", d.idle.as_millis() as u64)?),
        quarantine: Duration::from_millis(get_u64(&m, "quarantine_ms", d.quarantine.as_millis() as u64)?),
        allowed_namespaces: m.get("namespaces").map(|s| s.split(',').map(|x| x.trim().to_string()).filter(|x| !x.is_empty()).collect()).unwrap_or_default(),
        allow_delete: get_bool(&m, "allow_delete", false),
        max_connections: get_u64(&m, "max_connections", d.max_connections as u64)? as usize,
        ucx_net_devices: m.get("ucx_net_devices").cloned().unwrap_or(d.ucx_net_devices),
        ucx_tls: m.get("ucx_tls").cloned().unwrap_or(d.ucx_tls),
        nixl_prefix: m.get("nixl_prefix").cloned().unwrap_or(d.nixl_prefix),
    };
    if let Some(l) = m.get("ucx_log") {
        std::env::set_var("UCX_LOG_LEVEL", l);
    }
    if let Some(p) = m.get("pidfile") {
        std::fs::write(p, format!("{}\n", std::process::id()))?;
    }
    let r = server::run(cfg);
    if let Some(p) = m.get("pidfile") {
        let _ = std::fs::remove_file(p);
    }
    r
}
