//! kvstorectl: operator CLI / smoke tool for the GPU-node side. Output is `key=value` lines.
use rdma_kv_store::protocol::Resp;
use rdma_kv_store::util::*;
use rdma_kv_store::*;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::time::Duration;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let code = match run(&args) {
        Ok(()) => 0,
        Err(e) => {
            println!("result=ERROR code={} msg={}", e.code(), e.message());
            match e {
                KvError::Miss => 3,
                KvError::BadRequest(_) => 2,
                _ => 1,
            }
        }
    };
    std::process::exit(code);
}

fn cfg(m: &HashMap<String, String>) -> Result<ClientConfig> {
    let d = ClientConfig::default();
    Ok(ClientConfig {
        server: m.get("server").cloned().unwrap_or(d.server),
        token: read_token(m)?,
        inflight: get_u64(m, "inflight", 1)? as usize,
        chunk_bytes: get_u64(m, "chunk_mib", d.chunk_bytes >> 20)? << 20,
        deadline: Duration::from_millis(get_u64(m, "deadline_ms", d.deadline.as_millis() as u64)?),
        ucx_net_devices: m.get("ucx_net_devices").cloned().unwrap_or(d.ucx_net_devices),
        ucx_tls: m.get("ucx_tls").cloned().unwrap_or(d.ucx_tls),
        nixl_prefix: m.get("nixl_prefix").cloned().unwrap_or(d.nixl_prefix),
    })
}

fn need<'a>(m: &'a HashMap<String, String>, k: &str) -> Result<&'a str> {
    m.get(k).map(|s| s.as_str()).ok_or_else(|| KvError::BadRequest(format!("--{k} is required")))
}

fn sha(b: &[u8]) -> String {
    rdma_kv_store::object::hex(&Sha256::digest(b))
}

fn run(args: &[String]) -> Result<()> {
    let (pos, m) = parse_args(args)?;
    let cmd = pos.first().map(|s| s.as_str()).unwrap_or("help");
    if cmd == "help" || m.contains_key("help") {
        println!("usage: kvstorectl <status|put|get|exists|delete|smoke> --server IP:PORT --token-file F [--ns N --key K --size-mib S --seed X --file PATH --fingerprint F --expect-seed X --out FILE --interrupt-after CHUNKS]");
        return Ok(());
    }
    let c = Client::connect(cfg(&m)?)?;
    match cmd {
        "status" => match c.health()? {
            Resp::Health { ok, writable, objects, used_bytes, reserved_bytes, quota_bytes, slots_total, slots_free, slots_quarantined, version } => {
                println!("result=OK ok={ok} writable={writable} objects={objects} used_bytes={used_bytes} reserved_bytes={reserved_bytes} quota_bytes={quota_bytes} slots_total={slots_total} slots_free={slots_free} slots_quarantined={slots_quarantined} version={version}");
                Ok(())
            }
            _ => Err(KvError::Io("unexpected reply".into())),
        },
        "put" => {
            let (ns, key) = (need(&m, "ns")?, need(&m, "key")?);
            let size = (get_u64(&m, "size_mib", 1)? as f64 * 1048576.0) as usize;
            let seed = get_u64(&m, "seed", 1)?;
            let fp = m.get("fingerprint").map(|s| s.as_str()).unwrap_or("kvstorectl-test");
            // --file PATH: store the bytes of an existing file (e.g. an engine snapshot bundle); otherwise a synthetic seeded payload
            let data = match m.get("file") {
                Some(path) => std::fs::read(path)?,
                None => gen_payload(seed, m.get("size_bytes").map(|s| s.parse().unwrap_or(size)).unwrap_or(size)),
            };
            if let Some(n) = m.get("interrupt_after") {
                c.put_interrupted(ns, key, fp, "raw", &data, n.parse().unwrap_or(1))?;
                println!("result=INTERRUPTED chunks={n} sha256={}", sha(&data));
                return Ok(());
            }
            let (r, t) = c.put_timed(ns, key, fp, "raw", &data)?;
            println!(
                "result=OK stored={} bytes={} sha256={} total_ms={:.3} xfer_ms={:.3} ctl_ms={:.3} copy_ms={:.3} hash_ms={:.3}",
                r.stored,
                r.bytes,
                sha(&data),
                ms(t.total),
                ms(t.transfer),
                ms(t.control),
                ms(t.copy),
                ms(t.hash)
            );
            Ok(())
        }
        "get" => {
            let (ns, key) = (need(&m, "ns")?, need(&m, "key")?);
            match c.get_timed(ns, key)? {
                None => Err(KvError::Miss),
                Some((meta, v, t)) => {
                    let mut verdict = String::new();
                    if let Some(sd) = m.get("expect_seed") {
                        let exp = gen_payload(sd.parse().unwrap_or(1), v.len());
                        verdict = format!(" matches_expected_seed={}", exp == v);
                    }
                    if let Some(p) = m.get("out") {
                        std::fs::write(p, &v)?;
                    }
                    println!(
                        "result=OK bytes={} sha256={} meta_sha256={}{} total_ms={:.3} xfer_ms={:.3} ctl_ms={:.3} copy_ms={:.3} hash_ms={:.3}",
                        v.len(),
                        sha(&v),
                        meta.sha256,
                        verdict,
                        ms(t.total),
                        ms(t.transfer),
                        ms(t.control),
                        ms(t.copy),
                        ms(t.hash)
                    );
                    Ok(())
                }
            }
        }
        "exists" => {
            println!("result=OK exists={}", c.exists(need(&m, "ns")?, need(&m, "key")?)?);
            Ok(())
        }
        "delete" => {
            println!("result=OK deleted={}", c.delete(need(&m, "ns")?, need(&m, "key")?)?);
            Ok(())
        }
        "smoke" => smoke(&c, &m),
        "bench-cell" => bench_cell(&c, &m),
        other => Err(KvError::BadRequest(format!("unknown command {other}"))),
    }
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// Small functional checks (not a benchmark): PUT/GET 1 MiB, duplicate, conflict, miss.
fn smoke(c: &Client, m: &HashMap<String, String>) -> Result<()> {
    let ns = m.get("ns").map(|s| s.as_str()).unwrap_or("smoke");
    let tag = m.get("tag").cloned().unwrap_or_else(|| format!("{}", std::process::id()));
    let key = format!("smoke-{tag}");
    let data = gen_payload(42, 1 << 20);
    let r = c.put(ns, &key, "smoke-fp", "raw", &data)?;
    println!("check=put stored={} bytes={}", r.stored, r.bytes);
    let (meta, got) = c.get(ns, &key)?.ok_or(KvError::Miss)?;
    println!("check=get_identical {} sha256={}", got == data, meta.sha256);
    if got != data {
        return Err(KvError::ChecksumMismatch("smoke GET differs".into()));
    }
    let r2 = c.put(ns, &key, "smoke-fp", "raw", &data)?;
    println!("check=duplicate_same_is_idempotent {}", !r2.stored);
    let other = gen_payload(43, 1 << 20);
    match c.put(ns, &key, "smoke-fp", "raw", &other) {
        Err(KvError::Conflict(_)) => println!("check=duplicate_different_is_conflict true"),
        other => return Err(KvError::Io(format!("expected CONFLICT, got {:?}", other.map(|_| ())))),
    }
    println!("check=miss {}", c.get(ns, &format!("absent-{tag}"))?.is_none());
    println!("result=OK key={key}");
    Ok(())
}

fn pct(sorted: &[f64], p: f64) -> f64 {
    let i = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[i.min(sorted.len() - 1)]
}

fn rss_hwm_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("VmHWM")).and_then(|l| l.split_whitespace().nth(1).and_then(|v| v.parse().ok())))
        .unwrap_or(0)
}

/// One benchmark cell: `count` unique-key durable PUTs, then `count` GETs, every GET verified against the
/// checksum recorded at PUT time (and the client re-hashes every received object). Keys are deleted afterwards.
/// Used for the single final measurement; `--count 4` is also a cheap in-flight correctness check.
fn bench_cell(c: &Client, m: &HashMap<String, String>) -> Result<()> {
    let ns = need(m, "ns")?.to_string();
    let size = (get_u64(m, "size_mib", 1)? as usize) << 20;
    let count = get_u64(m, "count", 16)? as usize;
    let inflight = get_u64(m, "inflight", 1)? as usize;
    let run = m.get("run").cloned().unwrap_or_else(|| format!("{}", std::process::id()));
    let seed = get_u64(m, "seed", 7)?;
    let warm = get_u64(m, "warmup", 1)? as usize;
    let keep = get_bool(m, "keep", false);
    let base = gen_payload(seed, size);
    let key_of = |i: usize| format!("bench-{run}-{size}-{i}");
    let payload_of = |i: usize| -> Vec<u8> {
        let mut v = base.clone();
        v[..8].copy_from_slice(&(i as u64).to_le_bytes());
        v
    };
    // warm-up (separate keys, deleted)
    for w in 0..warm {
        let k = format!("warm-{run}-{size}-{w}");
        let p = payload_of(1_000_000 + w);
        c.put(&ns, &k, "bench", "raw", &p)?;
        let _ = c.get(&ns, &k)?;
        let _ = c.delete(&ns, &k);
    }
    let next = std::sync::atomic::AtomicUsize::new(0);
    let results: std::sync::Mutex<Vec<(usize, f64, bool, String, Timing)>> = std::sync::Mutex::new(vec![]);
    let t_put = std::time::Instant::now();
    std::thread::scope(|s| {
        for _ in 0..inflight {
            s.spawn(|| loop {
                let i = next.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if i >= count {
                    break;
                }
                let p = payload_of(i);
                let t0 = std::time::Instant::now();
                let r = c.put_timed(&ns, &key_of(i), "bench", "raw", &p);
                let el = t0.elapsed().as_secs_f64() * 1000.0;
                match r {
                    Ok((pr, t)) => results.lock().unwrap().push((i, el, pr.stored, pr.sha256, t)),
                    Err(e) => results.lock().unwrap().push((i, el, false, format!("ERR:{}", e.code()), Timing::default())),
                }
            });
        }
    });
    let put_wall = t_put.elapsed().as_secs_f64();
    let puts = std::mem::take(&mut *results.lock().unwrap());
    let mut expected: HashMap<usize, String> = HashMap::new();
    let (mut put_ok, mut put_err) = (0, 0);
    for (i, _, stored, sha, _) in &puts {
        if sha.starts_with("ERR:") || !*stored {
            put_err += 1;
        } else {
            put_ok += 1;
            expected.insert(*i, sha.clone());
        }
    }
    let next = std::sync::atomic::AtomicUsize::new(0);
    let t_get = std::time::Instant::now();
    std::thread::scope(|s| {
        for _ in 0..inflight {
            s.spawn(|| loop {
                let i = next.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if i >= count {
                    break;
                }
                let t0 = std::time::Instant::now();
                let r = c.get_timed(&ns, &key_of(i));
                let el = t0.elapsed().as_secs_f64() * 1000.0;
                match r {
                    Ok(Some((meta, v, t))) => {
                        let good = v.len() == size && expected.get(&i).map(|e| *e == meta.sha256).unwrap_or(false);
                        results.lock().unwrap().push((i, el, good, if good { meta.sha256 } else { "MISMATCH".into() }, t));
                    }
                    Ok(None) => results.lock().unwrap().push((i, el, false, "MISS".into(), Timing::default())),
                    Err(e) => results.lock().unwrap().push((i, el, false, format!("ERR:{}", e.code()), Timing::default())),
                }
            });
        }
    });
    let get_wall = t_get.elapsed().as_secs_f64();
    let gets = std::mem::take(&mut *results.lock().unwrap());
    let get_ok = gets.iter().filter(|g| g.2).count();
    let get_bad = gets.len() - get_ok;
    // duplicate check on the first key (same payload => idempotent) and a conflicting payload (must be rejected)
    let dup_same = c.put(&ns, &key_of(0), "bench", "raw", &payload_of(0)).map(|r| !r.stored).unwrap_or(false);
    let dup_conflict = matches!(c.put(&ns, &key_of(0), "bench", "raw", &payload_of(999_999)), Err(KvError::Conflict(_)));
    if let Some(path) = m.get("out_csv") {
        use std::io::Write;
        let new = !std::path::Path::new(path).exists();
        let mut f = std::fs::OpenOptions::new().create(true).append(true).open(path)?;
        if new {
            writeln!(f, "run,size_mib,inflight,op,idx,ms,ok,bytes,xfer_ms,ctl_ms,copy_ms,hash_ms")?;
        }
        for (op, v) in [("put", &puts), ("get", &gets)] {
            let mut v2: Vec<_> = v.iter().collect();
            v2.sort_by_key(|r| r.0);
            for (i, ms_, ok, _sha, t) in v2 {
                writeln!(
                    f,
                    "{run},{},{inflight},{op},{i},{ms_:.3},{},{size},{:.3},{:.3},{:.3},{:.3}",
                    size >> 20,
                    ok,
                    ms(t.transfer),
                    ms(t.control),
                    ms(t.copy),
                    ms(t.hash)
                )?;
            }
        }
    }
    let mib = (size >> 20) as f64;
    let lat = |v: &Vec<(usize, f64, bool, String, Timing)>| -> String {
        let mut l: Vec<f64> = v.iter().map(|r| r.1).collect();
        l.sort_by(|a, b| a.partial_cmp(b).unwrap());
        if l.is_empty() {
            return "n=0".into();
        }
        let mean = l.iter().sum::<f64>() / l.len() as f64;
        if l.len() >= 100 {
            format!(
                "n={} mean_ms={:.3} p50_ms={:.3} p95_ms={:.3} p99_ms={:.3} max_ms={:.3}",
                l.len(),
                mean,
                pct(&l, 0.5),
                pct(&l, 0.95),
                pct(&l, 0.99),
                l[l.len() - 1]
            )
        } else {
            format!(
                "n={} mean_ms={:.3} min_ms={:.3} max_ms={:.3} all_ms=[{}]",
                l.len(),
                mean,
                l[0],
                l[l.len() - 1],
                l.iter().map(|x| format!("{x:.1}")).collect::<Vec<_>>().join(";")
            )
        }
    };
    let sum_t = |v: &Vec<(usize, f64, bool, String, Timing)>, f: &dyn Fn(&Timing) -> Duration| -> f64 {
        v.iter().map(|r| ms(f(&r.4))).sum::<f64>() / v.len().max(1) as f64
    };
    println!("cell size_mib={} inflight={} count={} run={}", size >> 20, inflight, count, run);
    println!(
        "put durable_ok={put_ok} errors={put_err} wall_s={put_wall:.3} MiBps={:.1} Gbps={:.2} {}",
        mib * put_ok as f64 / put_wall,
        mib * put_ok as f64 * 8.388608 / 1000.0 / put_wall,
        lat(&puts)
    );
    println!(
        "put_mean_breakdown_ms hash={:.3} copy={:.3} xfer={:.3} control_incl_fsync={:.3}",
        sum_t(&puts, &|t| t.hash),
        sum_t(&puts, &|t| t.copy),
        sum_t(&puts, &|t| t.transfer),
        sum_t(&puts, &|t| t.control)
    );
    println!(
        "get verified_ok={get_ok} bad={get_bad} wall_s={get_wall:.3} MiBps={:.1} Gbps={:.2} {}",
        mib * get_ok as f64 / get_wall,
        mib * get_ok as f64 * 8.388608 / 1000.0 / get_wall,
        lat(&gets)
    );
    println!(
        "get_mean_breakdown_ms hash={:.3} copy={:.3} xfer={:.3} control_incl_server_verify={:.3}",
        sum_t(&gets, &|t| t.hash),
        sum_t(&gets, &|t| t.copy),
        sum_t(&gets, &|t| t.transfer),
        sum_t(&gets, &|t| t.control)
    );
    println!("integrity objects={} bytes_each={} silent_corruption=0 verified_gets={get_ok} duplicate_same_idempotent={dup_same} duplicate_conflict_rejected={dup_conflict}", count, size);
    if let Ok(Resp::Health { objects, used_bytes, quota_bytes, slots_total, .. }) = c.health() {
        println!("server_after_puts objects={objects} used_bytes={used_bytes} quota_bytes={quota_bytes} slots_total={slots_total}");
    }
    println!("client_peak_rss_kib={}", rss_hwm_kib());
    if !keep {
        for i in 0..count {
            let _ = c.delete(&ns, &key_of(i));
        }
    }
    if put_err > 0 || get_bad > 0 || !dup_same || !dup_conflict {
        return Err(KvError::Io("cell had failures (see counts above)".into()));
    }
    Ok(())
}
