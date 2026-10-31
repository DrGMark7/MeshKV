#!/usr/bin/env python3
"""Real LLM API -> engine KV snapshot -> rdma-kv-store (NIXL/RDMA) -> storage-node tmpfs -> GET -> byte-exact check.

Correctness experiment, not a benchmark and not a restore test (remote KV *reuse* is NOT TESTED here).
Operator-side test driver: runs on the GPU node, talks to the normal authenticated gateway, and drives the existing Rust client
(`kvstorectl`) for transport. It never exposes snapshots through the public API.

  preflight            discover routing/engine/node2 tmpfs and write preflight.json (read-only)
  run --n 1|2|4        send N concurrent chat requests, capture N engine snapshots, PUT x N, GET x N, verify
  selftest             adapter=dry-run only: validates this script's own mechanics (NOT a result, output refused under results/)

Adapters (engine-specific part, kept small and separate):
  llamacpp-slot   llama.cpp server slot save (`POST /slots/{id}?action=save`), reached through `docker exec` so the backend key never leaves
                  the container. Needs the engine started with `--slot-save-path /tmp/kv-staging` (e.g. Q5_EXTRA_ARGS=... switch-model.sh q5).
  dry-run         synthetic bytes + loopback copy. Labelled DRYRUN; can never produce a PASS.

A synthetic blob, token list, prompt or hash is never accepted as a snapshot by the llamacpp-slot adapter. Secrets are read from a
0600 file or the environment and are never printed or written.
"""
import argparse, concurrent.futures as cf, csv, hashlib, json, os, random, re, struct, subprocess, sys, tempfile, threading, time, urllib.request, urllib.error

MAGIC = b"KVBNDL01"
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
DEFAULT_OUT = os.path.join(ROOT, "results", "rdma-rust-kv", "llm-api-ram-node2")
WORDS = ("amber basalt cobalt delta ember fjord garnet harbor indigo juniper krypton lantern marble nebula obsidian prairie quartz "
         "ripple saffron timber umber velvet willow xenon yonder zephyr alpine bramble cinder dune estuary fathom glacier").split()
NS_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


def sha(b): return hashlib.sha256(b).hexdigest()
def now(): return time.time()
def sh(cmd, timeout=120, inp=None):
    return subprocess.run(["bash", "-c", cmd], capture_output=True, timeout=timeout, input=inp)
def sht(cmd, timeout=120): return sh(cmd, timeout).stdout.decode(errors="replace")


def make_prompt(run_id, idx, target_tokens):
    """Deterministic, distinct per request, carries a nonce that identifies the request inside the snapshot's own token stream."""
    nonce = f"NONCE-{sha(f'{run_id}:{idx}'.encode())[:12]}"
    rng = random.Random(f"{run_id}:{idx}")
    words = max(20, int(target_tokens * 0.75))
    body = " ".join(rng.choice(WORDS) for _ in range(words))
    return nonce, f"Reference tag {nonce}. Read the following list and answer with the single word OK.\n{body}\nTag again: {nonce}."


# ---------------------------------------------------------------- adapters
class DryRunAdapter:
    name = "dry-run"
    label = "DRYRUN_NOT_A_RESULT"
    def describe(self): return {"adapter": self.name, "engine": "none (synthetic)", "label": self.label}
    def start_monitor(self): pass
    def stop_monitor(self): return {"max_busy_slots": None, "note": "dry-run"}
    def capture_all(self, requests, run_id):
        out = {}
        for r in requests:
            rng = random.Random(r["nonce"]); data = rng.randbytes(2 * 1024 * 1024 + 17 * r["idx"])
            out[r["request_id"]] = {"state": data, "tokens": None, "group_inventory": [{"id": "synthetic", "type": "dry-run", "bytes": len(data)}],
                                    "boundary": "synthetic", "capture_ms": 0.0}
        return out
    def post_chat(self, req): return {"status": 200, "t_send": now(), "t_first": now(), "t_done": now(), "usage": {}, "text_len": 0}
    fingerprint = lambda self: sha(b"dry-run")


class LlamaSlotAdapter:
    """llama.cpp server. Binding of a saved slot to its request: the slot file contains the token ids; they are detokenised via the
    server and the request's unique nonce must appear. No id_slot is sent (the gateway strips it by policy)."""
    name = "llamacpp-slot"
    label = "REAL_ENGINE"
    def __init__(self, container, gateway, key, model, staging="/tmp/kv-staging"):
        self.c, self.gateway, self.key, self.model, self.staging = container, gateway, key, model, staging
        self._mon = None; self._stop = threading.Event(); self._busy = []
    def _curl(self, method, path, body=None, timeout=60):
        data = f"-d '{json.dumps(body)}' -H 'Content-Type: application/json'" if body is not None else ""
        cmd = (f"docker exec {self.c} sh -c 'curl -sf -m {timeout} -X {method} {data} -H \"Authorization: Bearer $(cat /run/secrets/llamacpp_api_key)\" "
               f"http://127.0.0.1:8080{path}'")
        r = sh(cmd, timeout=timeout + 15)
        if r.returncode != 0: raise RuntimeError(f"engine {method} {path} failed rc={r.returncode}: {r.stderr.decode()[:200]}")
        return json.loads(r.stdout or b"null")
    def describe(self):
        props = self._curl("GET", "/props"); slots = self._curl("GET", "/slots")
        img = sht(f"docker inspect -f '{{{{.Image}}}} {{{{.Config.Image}}}}' {self.c}").strip()
        return {"adapter": self.name, "container": self.c, "image": img, "build_info": props.get("build_info"), "model_alias": props.get("model_alias"),
                "model_path_basename": os.path.basename(str(props.get("model_path", ""))), "n_slots": len(slots),
                "slot_n_ctx": [s.get("n_ctx") for s in slots], "chat_template_sha256": sha(str(props.get("chat_template", "")).encode()),
                "cmdline": sht(f"docker inspect -f '{{{{.Args}}}}' {self.c}")[:600]}
    def fingerprint(self):
        d = self.describe()
        keep = {k: d.get(k) for k in ("image", "build_info", "model_alias", "model_path_basename", "chat_template_sha256", "cmdline")}
        return sha(json.dumps(keep, sort_keys=True).encode())
    def start_monitor(self):
        def loop():
            while not self._stop.is_set():
                try: self._busy.append((now(), sum(1 for s in self._curl("GET", "/slots", timeout=5) if s.get("is_processing"))))
                except Exception: pass
                time.sleep(0.2)
        self._stop.clear(); self._busy = []; self._mon = threading.Thread(target=loop, daemon=True); self._mon.start()
    def stop_monitor(self):
        self._stop.set(); self._mon and self._mon.join(5)
        return {"max_busy_slots": max((b for _, b in self._busy), default=None), "samples": len(self._busy)}
    def post_chat(self, req):
        body = {"model": self.model, "messages": [{"role": "user", "content": req["prompt"]}], "temperature": 0, "max_tokens": req["max_tokens"], "stream": False}
        h = {"Content-Type": "application/json", "Authorization": f"Bearer {self.key}", "X-Request-Id": req["request_id"]}
        t_send = now()
        try:
            rq = urllib.request.Request(self.gateway.rstrip("/") + "/v1/chat/completions", json.dumps(body).encode(), h)
            with urllib.request.urlopen(rq, timeout=600) as resp:
                t_first = now(); raw = resp.read(); status = resp.status
            j = json.loads(raw); return {"status": status, "t_send": t_send, "t_first": t_first, "t_done": now(), "usage": j.get("usage", {}),
                                         "text_len": len(j["choices"][0]["message"].get("content") or "")}
        except urllib.error.HTTPError as e:
            return {"status": e.code, "t_send": t_send, "t_first": now(), "t_done": now(), "usage": {}, "text_len": 0, "error": f"HTTP {e.code}"}
        except Exception as e:
            return {"status": 0, "t_send": t_send, "t_first": now(), "t_done": now(), "usage": {}, "text_len": 0, "error": type(e).__name__}
    @staticmethod
    def parse_tokens(blob):
        """llama.cpp seq state file: u32 magic 'ggsq', u32 version, u32 n_tokens, n_tokens x i32 token ids, then the state. Verified at run time."""
        if len(blob) < 12 or struct.unpack_from("<I", blob, 0)[0] != 0x67677371: return None
        n = struct.unpack_from("<I", blob, 8)[0]
        return list(struct.unpack_from(f"<{n}i", blob, 12)) if 0 < n < 10_000_000 and 12 + 4 * n <= len(blob) else None
    def capture_all(self, requests, run_id):
        """Save every non-empty slot after the batch, bind each file to a request through its nonce. Unbound requests stay absent (=> INCOMPLETE)."""
        out, slots = {}, self._curl("GET", "/slots")
        for s in slots:
            sid = s["id"]; fn = f"{run_id}-slot{sid}.bin"; t0 = now()
            try: self._curl("POST", f"/slots/{sid}?action=save", {"filename": fn}, timeout=120)
            except RuntimeError: continue
            blob = sh(f"docker exec {self.c} cat {self.staging}/{fn}", timeout=300).stdout
            sh(f"docker exec {self.c} rm -f {self.staging}/{fn}")
            toks = self.parse_tokens(blob)
            if not toks: continue
            text = (self._curl("POST", "/detokenize", {"tokens": toks}, timeout=60) or {}).get("content", "")
            for r in requests:
                if r["nonce"] in text and r["request_id"] not in out:
                    out[r["request_id"]] = {"state": blob, "tokens": len(toks), "slot": sid, "capture_ms": (now() - t0) * 1000,
                                            "group_inventory": [{"id": "llama_state_seq", "type": "llama.cpp seq state (all layers + recurrent/KV as saved by the engine)", "bytes": len(blob)}],
                                            "boundary": "end of request (prompt + generated tokens); KV was not touched after completion"}
        return out


# ---------------------------------------------------------------- transport
class KvstorectlTransport:
    """Existing Rust client, one process per in-flight operation. Needs the `put --file` option."""
    def __init__(self, ctl, server, token_file, nixl_env):
        self.base = f"{nixl_env}; UCX_TLS=^tcp UCX_LOG_LEVEL=info {ctl} {{cmd}} --server {server} --token-file {token_file}"
    def run(self, cmd): r = sh(self.base.format(cmd=cmd), timeout=600); return r.returncode, r.stdout.decode(), r.stderr.decode()
    def put(self, ns, key, fp, path):
        t0 = now(); rc, so, se = self.run(f"put --ns {ns} --key {key} --file {path} --fingerprint {fp}"); return rc, so, se, t0, now()
    def get(self, ns, key, out):
        t0 = now(); rc, so, se = self.run(f"get --ns {ns} --key {key} --out {out}"); return rc, so, se, t0, now()


class LoopbackTransport:  # dry-run stand-in for the Rust client; plain file copy into a local dir
    def __init__(self, d): self.d = d; os.makedirs(d, exist_ok=True)
    def put(self, ns, key, fp, path):
        t0 = now(); data = open(path, "rb").read(); open(os.path.join(self.d, f"{ns}__{key}"), "wb").write(data)
        return 0, f"result=OK stored=true bytes={len(data)} sha256={sha(data)}", "", t0, now()
    def get(self, ns, key, out):
        t0 = now(); p = os.path.join(self.d, f"{ns}__{key}")
        if not os.path.exists(p): return 1, "result=MISS", "", t0, now()
        data = open(p, "rb").read(); open(out, "wb").write(data)
        return 0, f"result=OK bytes={len(data)} sha256={sha(data)}", "", t0, now()


def parse_kv(line, key):
    m = re.search(rf"\b{key}=(\S+)", line); return m.group(1) if m else None
def nic_bytes(node_cmd_prefix=""):
    base = "/sys/class/infiniband/mlx5_0/ports/1/counters"
    try: return int(sht(f"{node_cmd_prefix}cat {base}/port_xmit_data")) * 4, int(sht(f"{node_cmd_prefix}cat {base}/port_rcv_data")) * 4
    except Exception: return None, None
def overlap(spans):
    """largest number of operations simultaneously in flight, from (t_start, t_end) spans"""
    ev = sorted([(a, 1) for a, b in spans] + [(b, -1) for a, b in spans]); cur = best = 0
    for _, d in ev: cur += d; best = max(best, cur)
    return best


def build_bundle(manifest, state):
    m = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    return MAGIC + struct.pack("<I", len(m)) + m + state
def split_bundle(b):
    if b[:8] != MAGIC: raise ValueError("bad magic")
    n = struct.unpack_from("<I", b, 8)[0]; return json.loads(b[12:12 + n]), b[12 + n:]


# ---------------------------------------------------------------- run
def run_batch(a, adapter, transport, n, run_id, out_dir, staging_dir, fingerprint, tenant, dryrun):
    os.makedirs(out_dir, exist_ok=True); os.makedirs(staging_dir, exist_ok=True)
    ns = f"{a.ns_prefix}-{run_id}"; assert NS_RE.match(ns), "namespace must match [A-Za-z0-9._-]{1,64}"
    reqs = []
    for i in range(n):
        nonce, prompt = make_prompt(run_id, i, a.prompt_tokens)
        reqs.append({"idx": i, "request_id": f"{run_id}-r{i}", "nonce": nonce, "prompt": prompt, "max_tokens": a.max_tokens})
    res = {"run_id": run_id, "n": n, "label": adapter.label, "problems": []}
    adapter.start_monitor()
    with cf.ThreadPoolExecutor(n) as ex: api = list(ex.map(adapter.post_chat, reqs))
    mon = adapter.stop_monitor()
    for r, x in zip(reqs, api): r["api"] = x
    res["api_ok"] = sum(1 for x in api if x["status"] == 200)
    res["max_busy_slots"] = mon.get("max_busy_slots")
    res["api_overlap"] = overlap([(x["t_send"], x["t_done"]) for x in api])
    snaps = adapter.capture_all(reqs, run_id)
    res["snapshots"] = len(snaps)
    for r in reqs:
        r["_ok"] = r["api"]["status"] == 200 and r["request_id"] in snaps
        if not r["_ok"]: res["problems"].append(f"{r['request_id']}: api={r['api']['status']} snapshot={'yes' if r['request_id'] in snaps else 'NO'}")
    work = [r for r in reqs if r["_ok"]]
    bundles = {}
    for r in work:
        s = snaps[r["request_id"]]
        man = {"run_id": run_id, "request_id": r["request_id"], "tenant_id": tenant, "requested_model": a.model, "adapter": adapter.name, "label": adapter.label,
               "compat_fingerprint": fingerprint, "token_count": s.get("tokens"), "boundary": s["boundary"], "groups": s["group_inventory"],
               "group_count": len(s["group_inventory"]), "state_bytes": len(s["state"]), "state_sha256": sha(s["state"]),
               "prompt_tokens_reported": r["api"]["usage"].get("prompt_tokens"), "completion_tokens_reported": r["api"]["usage"].get("completion_tokens")}
        b = build_bundle(man, s["state"]); path = os.path.join(staging_dir, f"{r['request_id']}.bundle")
        open(path, "wb").write(b); bundles[r["request_id"]] = {"path": path, "bytes": len(b), "sha256": sha(b), "manifest": man, "key": f"{r['request_id']}-{man['state_sha256'][:16]}"}
    res["bundle_bytes"] = {k: v["bytes"] for k, v in bundles.items()}
    res["keys_distinct"] = len({v["key"] for v in bundles.values()}) == len(bundles)
    # PUT x n in flight
    n1_before = nic_bytes(); put_spans = {}; t0 = now()
    with cf.ThreadPoolExecutor(max(1, len(bundles))) as ex:
        futs = {rid: ex.submit(transport.put, ns, v["key"], fingerprint, v["path"]) for rid, v in bundles.items()}
        for rid, f in futs.items():
            rc, so, se, ts, te = f.result(); put_spans[rid] = (ts, te)
            ok = rc == 0 and parse_kv(so, "sha256") == bundles[rid]["sha256"]
            bundles[rid]["put"] = {"rc": rc, "ok": ok, "ms": (te - ts) * 1000, "stdout": so.strip()[:300]}
            if not ok: res["problems"].append(f"PUT {rid} failed rc={rc}")
    put_nic = nic_bytes(); res["put_inflight_observed"] = overlap(list(put_spans.values())); res["put_wall_ms"] = (now() - t0) * 1000
    # drop local copies of the stored objects (the original bytes stay in memory only for comparison); GET into FRESH files
    orig = {rid: open(v["path"], "rb").read() for rid, v in bundles.items()}
    for v in bundles.values(): os.remove(v["path"])
    get_spans = {}; t0 = now(); get_nic0 = nic_bytes()
    with cf.ThreadPoolExecutor(max(1, len(bundles))) as ex:
        futs = {rid: ex.submit(transport.get, ns, v["key"], os.path.join(staging_dir, f"{rid}.get")) for rid, v in bundles.items()}
        for rid, f in futs.items():
            rc, so, se, ts, te = f.result(); get_spans[rid] = (ts, te); v = bundles[rid]
            got = open(os.path.join(staging_dir, f"{rid}.get"), "rb").read() if rc == 0 and os.path.exists(os.path.join(staging_dir, f"{rid}.get")) else b""
            exact = got == orig[rid]; man_ok = False; state_ok = False
            try:
                gm, gstate = split_bundle(got); man_ok = gm == v["manifest"] and gm["request_id"] == rid; state_ok = sha(gstate) == v["manifest"]["state_sha256"]
            except Exception: pass
            v["get"] = {"rc": rc, "ms": (te - ts) * 1000, "bytes": len(got), "sha256": sha(got) if got else None, "exact_bytes": exact, "manifest_ok": man_ok,
                        "state_sha_ok": state_ok, "length_ok": len(got) == v["bytes"], "stdout_meta_sha_matches": parse_kv(so, "meta_sha256") == v["sha256"]}
            if not (exact and man_ok and state_ok): res["problems"].append(f"GET verify failed for {rid}")
    get_nic1 = nic_bytes(); res["get_inflight_observed"] = overlap(list(get_spans.values())); res["get_wall_ms"] = (now() - t0) * 1000
    res["nic_node1_delta_bytes"] = {"put_xmit": (put_nic[0] - n1_before[0]) if None not in (put_nic[0], n1_before[0]) else None,
                                    "get_rcv": (get_nic1[1] - get_nic0[1]) if None not in (get_nic1[1], get_nic0[1]) else None}
    # one tiny negative check: a wrong namespace must MISS
    if bundles:
        rid = next(iter(bundles)); rc, so, se, _, _ = transport.get(f"{ns}-wrongns", bundles[rid]["key"], os.path.join(staging_dir, "neg.get"))
        res["negative_wrong_namespace"] = "MISS" if "MISS" in (so + se).upper() or rc != 0 else "UNEXPECTED_HIT"
    for f in os.listdir(staging_dir):
        if f.endswith(".get"): os.remove(os.path.join(staging_dir, f))
    res["all_exact"] = bool(bundles) and all(v.get("get", {}).get("exact_bytes") and v.get("get", {}).get("manifest_ok") and v["get"].get("state_sha_ok") for v in bundles.values())
    res["verdict"] = ("PASS" if (res["all_exact"] and len(bundles) == n and res["api_ok"] == n and res["keys_distinct"] and not res["problems"]) else "FAIL_OR_INCOMPLETE")
    if adapter.label != "REAL_ENGINE": res["verdict"] = "DRYRUN_NOT_A_RESULT"
    res["requests"] = [{k: v for k, v in r.items() if k not in ("prompt",)} for r in reqs]   # prompts are not logged
    res["objects"] = {rid: {k: v[k] for k in ("bytes", "sha256", "key", "manifest", "put", "get")} for rid, v in bundles.items()}
    json.dump(res, open(os.path.join(out_dir, f"batch-n{n}.json"), "w"), indent=1, default=str)
    return res


def read_key(a):
    if a.key_file: return open(a.key_file).read().strip()
    return os.environ.get("LLM_TEST_API_KEY", "")


def main():
    p = argparse.ArgumentParser(); p.add_argument("cmd", choices=["preflight", "run", "selftest"])
    p.add_argument("--n", type=int, choices=[1, 2, 4], default=1); p.add_argument("--run-id", default=time.strftime("%Y%m%d-%H%M%S"))
    p.add_argument("--gateway", default=os.environ.get("LLM_GATEWAY_URL", "http://127.0.0.1:4000")); p.add_argument("--model", default="qwen3.8-27b-q5km")
    p.add_argument("--key-file"); p.add_argument("--engine-container", default="local-llm-llamacpp-1")
    p.add_argument("--server", default="10.10.10.2:7472"); p.add_argument("--token-file", default=os.path.expanduser("~/kv-run/token"))
    p.add_argument("--ctl", default=os.path.expanduser("~/kv-run/bin/kvstorectl")); p.add_argument("--nixl-env", default=". ~/kv-run/env.sh")
    p.add_argument("--ns-prefix", default="real-kv"); p.add_argument("--tenant", default="test-tenant-1")
    p.add_argument("--prompt-tokens", type=int, default=600); p.add_argument("--max-tokens", type=int, default=8)
    p.add_argument("--out-root", default=DEFAULT_OUT); p.add_argument("--staging", default=os.path.expanduser("~/kv-run/real-kv-staging"))
    a = p.parse_args()
    if a.cmd == "selftest":
        out = os.path.join(tempfile.mkdtemp(prefix="kvdryrun-"), a.run_id); assert not os.path.abspath(out).startswith(os.path.join(ROOT, "results"))
        ok_all = True
        for n in (1, 2, 4):
            ad = DryRunAdapter(); tr = LoopbackTransport(os.path.join(out, "loop"))
            r = run_batch(a, ad, tr, n, f"{a.run_id}-n{n}", os.path.join(out, "out"), os.path.join(out, "stage"), ad.fingerprint(), a.tenant, True)
            print(f"[selftest n={n}] verdict={r['verdict']} all_exact={r['all_exact']} keys_distinct={r['keys_distinct']} put_inflight={r['put_inflight_observed']} get_inflight={r['get_inflight_observed']} problems={r['problems']}")
            ok_all &= r["all_exact"] and r["keys_distinct"] and not r["problems"]
        # corruption must be detected: flip one byte in a stored object and GET again
        ad = DryRunAdapter(); loop = os.path.join(out, "loop2"); tr = LoopbackTransport(loop)
        r = run_batch(a, ad, tr, 1, f"{a.run_id}-corrupt", os.path.join(out, "out2"), os.path.join(out, "stage2"), ad.fingerprint(), a.tenant, True)
        f = os.path.join(loop, os.listdir(loop)[0]); b = bytearray(open(f, "rb").read()); b[-1] ^= 1; open(f, "wb").write(bytes(b))
        got = os.path.join(out, "x.get"); tr.get(*f.split(os.sep)[-1].split("__"), got)
        detected = sha(open(got, "rb").read()) != next(iter(r["objects"].values()))["sha256"]
        print(f"[selftest corruption] byte flip detected by hash compare: {detected}"); ok_all &= detected
        print("SELFTEST", "OK (mechanics only, DRYRUN_NOT_A_RESULT)" if ok_all else "FAILED"); print("outputs in", out); sys.exit(0 if ok_all else 1)
    key = read_key(a)
    ad = LlamaSlotAdapter(a.engine_container, a.gateway, key, a.model)
    out_dir = os.path.join(a.out_root, a.run_id)
    if a.cmd == "preflight":
        os.makedirs(out_dir, exist_ok=True)
        pf = {"run_id": a.run_id, "date": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "requested_model": a.model,
              "active_model_file": sht(f"cat {ROOT}/data/run/active-model").strip(), "active_models_yaml_head": sht(f"grep -E 'model_name|blocked' {ROOT}/data/run/active_models.yaml | head -20"),
              "autoswap": sht(f"cd {ROOT} && scripts/swapd-ctl.sh status 2>&1 | head -3"), "gpu": sht("nvidia-smi --query-gpu=memory.used,memory.total --format=csv,noheader"),
              "node1_mem": sht("free -m | sed -n 2p"), "gateway_models_http": None}
        try:
            rq = urllib.request.Request(a.gateway.rstrip("/") + "/v1/models", headers={"Authorization": f"Bearer {key}"})
            pf["gateway_models_http"] = json.loads(urllib.request.urlopen(rq, timeout=10).read()).get("data", [])
        except Exception as e: pf["gateway_models_http"] = f"unavailable: {type(e).__name__}"
        try: pf["engine"] = ad.describe()
        except Exception as e: pf["engine"] = f"unavailable: {e}"
        pf["storage_node_tmpfs"] = sht(f"{ROOT}/scripts/kvstore-tmpfs-ctl.sh preflight 2>&1 | tail -15")
        json.dump(pf, open(os.path.join(out_dir, "preflight.json"), "w"), indent=1, default=str); print("wrote", os.path.join(out_dir, "preflight.json")); return
    if not key: sys.exit("no API key: set LLM_TEST_API_KEY or --key-file (a scoped key from scripts/create-user-key.sh); never the master key")
    tr = KvstorectlTransport(a.ctl, a.server, a.token_file, a.nixl_env)
    r = run_batch(a, ad, tr, a.n, f"{a.run_id}-n{a.n}", out_dir, os.path.join(a.staging, a.run_id), ad.fingerprint(), a.tenant, False)
    print(json.dumps({k: r[k] for k in ("run_id", "n", "verdict", "api_ok", "snapshots", "keys_distinct", "all_exact", "max_busy_slots", "api_overlap",
                                       "put_inflight_observed", "get_inflight_observed", "negative_wrong_namespace", "problems") if k in r}, indent=1))


if __name__ == "__main__":
    main()
