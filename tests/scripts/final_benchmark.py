#!/usr/bin/env python3
"""Single final measurement of the file-backed RDMA KV store (plan section 9.1). Run on the GPU node (node1).

Cells (one timed run per row after one warm-up put/get inside bench-cell; unique immutable keys, deleted after each cell):
  A 1 MiB x1000, inflight 1 | B 64 MiB x20, inflight 1 | C 64 MiB x20, inflight 4
Collects app throughput (durable PUT = fsync before ack, file-backed GET incl. server whole-file verification),
NIC byte counters on both nodes (RDMA wire bytes vs application bytes), peak RSS and CPU of client and daemon,
UCX transport evidence, integrity counts. Usage: final_benchmark.py <outdir>
"""
import csv, json, os, re, shlex, subprocess, sys, time

ROOT = os.path.expanduser("~/proj/local-llm")
RUN = os.path.expanduser("~/kv-run")
OUT = sys.argv[1] if len(sys.argv) > 1 else f"{ROOT}/results/rdma-rust-kv/final"
CNT = "/sys/class/infiniband/mlx5_0/ports/1/counters"
CTL = f"{ROOT}/scripts/kvstore-ctl.sh"
HOST = "hpcnc-gpu2"
SERVER = "10.10.10.2:7471"
CELLS = [("A", 1, 1000, 1), ("B", 64, 20, 1), ("C", 64, 20, 4)]
if os.environ.get("KVB_DRYRUN"):  # script validation only: tiny counts, output must go to /tmp and be discarded
    CELLS = [("A", 1, 20, 1), ("B", 64, 2, 1), ("C", 64, 4, 4)]


def sh(cmd, timeout=600):
    return subprocess.run(["bash", "-c", cmd], capture_output=True, text=True, timeout=timeout)


def n2(cmd):
    return sh(f"ssh -o ConnectTimeout=8 {HOST} {shlex.quote(cmd)}")


def nic(node):
    c = (lambda x: int(sh(f"cat {CNT}/{x}").stdout)) if node == 1 else (lambda x: int(n2(f"cat {CNT}/{x}").stdout))
    return c("port_xmit_data") * 4, c("port_rcv_data") * 4


def daemon_pid():
    r = n2(f"cat {RUN}/kvstored.pid").stdout.strip()
    return int(r)


def dstat(pid):
    r = n2(f"awk '{{print $14+$15}}' /proc/{pid}/stat; grep VmHWM /proc/{pid}/status | awk '{{print $2}}'; getconf CLK_TCK").stdout.split()
    return int(r[0]), int(r[1]), int(r[2])


def parse(out):
    d = {}
    for line in out.splitlines():
        for key in ("put", "get"):
            if line.startswith(key + " "):
                d[key] = dict(re.findall(r"(\w+)=([^\s\[]+)", line))
        if line.startswith("integrity"):
            d["integrity"] = dict(re.findall(r"(\w+)=([^\s]+)", line))
        if line.startswith("server_after_puts"):
            d["server"] = dict(re.findall(r"(\w+)=([^\s]+)", line))
        if line.startswith("client_peak_rss_kib"):
            d["client_rss_kib"] = int(line.split("=")[1])
        if line.startswith("put_mean_breakdown_ms"):
            d["put_bd"] = dict(re.findall(r"(\w+)=([\d.]+)", line))
        if line.startswith("get_mean_breakdown_ms"):
            d["get_bd"] = dict(re.findall(r"(\w+)=([\d.]+)", line))
    return d


def main():
    os.makedirs(OUT, exist_ok=True)
    pre = {}
    pre["loadavg_node1"] = open("/proc/loadavg").read().strip()
    pre["loadavg_node2"] = n2("cat /proc/loadavg").stdout.strip()
    pre["top_cpu_node1"] = sh("ps -eo pcpu,comm --sort=-pcpu | head -4 | tr '\\n' ';'").stdout
    pre["stray_before"] = sh(f"ps -eo comm | grep -c '^kvstor'").stdout.strip() + "/" + n2("ps -eo comm | grep -c '^kvstor'").stdout.strip()
    pre["hyperqwen"] = sh("docker inspect -f '{{.State.Health.Status}}' local-llm-hyperqwen-1").stdout.strip()
    pre["files_before"] = n2("find /home/hpcnc/llm-kv-cache -name '*.obj' | wc -l").stdout.strip()
    pre["source_sha256"] = sh(f"cd {ROOT}/components/rdma-kv-store && find src Cargo.toml Cargo.lock build.rs -type f | sort | xargs sha256sum | sha256sum | cut -d' ' -f1").stdout.strip()
    pre["nixl_sha"] = "2653767b80f080da4c375d663a308d444e63a8f1"
    pre["conf"] = open(f"{ROOT}/config/rdma-kv-store.conf").read()
    pre["date"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    r = sh(f"{CTL} start --force")
    pre["start"] = r.stdout.strip()
    pid = daemon_pid()
    pre["daemon_pid"] = pid
    pre["daemon_cmdline"] = n2(f"tr '\\0' ' ' < /proc/{pid}/cmdline").stdout.strip()
    json.dump(pre, open(f"{OUT}/preflight.json", "w"), indent=1)
    rows = []
    run_id = time.strftime("%H%M%S")
    for cell, size, count, inflight in CELLS:
        time.sleep(5)
        n2(f"echo 5 > /proc/{pid}/clear_refs")  # reset the daemon's peak RSS (VmHWM) for this cell
        b1, b2 = nic(1), nic(2)
        d0 = dstat(pid)
        t0 = time.time()
        cmd = (f". {RUN}/env.sh; cd {RUN}; UCX_LOG_LEVEL=info /usr/bin/time -v ./bin/kvstorectl bench-cell --server {SERVER} --token-file {RUN}/token "
               f"--ns final-{run_id} --size-mib {size} --count {count} --inflight {inflight} --run {cell}{run_id} --warmup 1 --out-csv {OUT}/per_op.csv")
        p = sh(cmd, timeout=900)
        wall = time.time() - t0
        a1, a2 = nic(1), nic(2)
        d1 = dstat(pid)
        log = p.stdout + "\n" + p.stderr
        open(f"{OUT}/cell_{cell}.log", "w").write(log)
        res = parse(p.stdout)
        cpu = re.search(r"Percent of CPU this job got: (\d+)%", p.stderr)
        rss = re.search(r"Maximum resident set size \(kbytes\): (\d+)", p.stderr)
        rma = len([l for l in log.splitlines() if "inter-node" in l and "rma(rc_mlx5" in l])
        tcp = len([l for l in log.splitlines() if "tcp" in l.lower() and "cfg#" in l])
        dcpu = 100.0 * (d1[0] - d0[0]) / d0[2] / wall
        meta = dict(cell=cell, rc=p.returncode, wall_total_s=round(wall, 2), nic_n1_xmit_bytes=a1[0] - b1[0], nic_n1_rcv_bytes=a1[1] - b1[1],
                    nic_n2_xmit_bytes=a2[0] - b2[0], nic_n2_rcv_bytes=a2[1] - b2[1], client_peak_rss_kib=int(rss.group(1)) if rss else None,
                    client_cpu_pct=int(cpu.group(1)) if cpu else None, daemon_peak_rss_kib=d1[1], daemon_cpu_pct_avg_over_cell=round(dcpu, 1),
                    ucx_rma_rc_mlx5_lines=rma, ucx_tcp_cfg_lines=tcp)
        for op in ("put", "get"):
            o = res.get(op, {})
            ok = o.get("durable_ok") or o.get("verified_ok")
            rows.append(dict(cell=cell, op="PUT(durable)" if op == "put" else "GET(file-backed,verified)", size_mib=size, inflight=inflight, count=count, ok=ok,
                             errors=o.get("errors") or o.get("bad"), wall_s=o.get("wall_s"), MiBps=o.get("MiBps"), Gbps=o.get("Gbps"), mean_ms=o.get("mean_ms"),
                             p50_ms=o.get("p50_ms"), p95_ms=o.get("p95_ms"), p99_ms=o.get("p99_ms"), min_ms=o.get("min_ms"), max_ms=o.get("max_ms"),
                             breakdown_ms=json.dumps(res.get(op + "_bd", {})), **{k: v for k, v in meta.items() if k != "cell"},
                             server_objects_after_puts=res.get("server", {}).get("objects"), server_used_bytes_after_puts=res.get("server", {}).get("used_bytes"),
                             silent_corruption=res.get("integrity", {}).get("silent_corruption"), dup_same_idempotent=res.get("integrity", {}).get("duplicate_same_idempotent"),
                             dup_conflict_rejected=res.get("integrity", {}).get("duplicate_conflict_rejected")))
        print(json.dumps(dict(meta, put=res.get("put"), get=res.get("get"), integrity=res.get("integrity"))), flush=True)
    with open(f"{OUT}/summary.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    post = {}
    post["stop"] = sh(f"{CTL} stop").stdout.strip()
    post["stray_after"] = sh("ps -eo comm | grep -c '^kvstor'").stdout.strip() + "/" + n2("ps -eo comm | grep -c '^kvstor'").stdout.strip()
    post["footprint"] = sh(f"{CTL} footprint").stdout.strip()
    post["loadavg_node1_after"] = open("/proc/loadavg").read().strip()
    json.dump(post, open(f"{OUT}/postflight.json", "w"), indent=1)
    print(json.dumps(post))


if __name__ == "__main__":
    main()
