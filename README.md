# MeshKV (crate: rdma-kv-store)

Small, independent Rust package that stores **opaque, immutable, checksummed objects** as real files on a storage node and moves
them between two CPU hosts with **NVIDIA NIXL + UCX over RoCE (CPU DRAM <-> CPU DRAM)**. It never interprets payload bytes
(no tensors, tokenizers, attention, vLLM types); an inference adapter owns serialisation of KV/recurrent state.

* No GPUDirect RDMA / GDS / CUDA registration anywhere. GPU data must be staged in CPU RAM by the caller.
* NIXL is transport only; this package adds the missing parts: key -> file mapping, immutability, atomic durable publish,
  checksums, quota, bounded buffers, restart behaviour, timeouts, error codes.
* Tested against the pinned NIXL revision `2653767b80f080da4c375d663a308d444e63a8f1` (C API `libnixl_capi.so`, hand-written FFI in
  `src/nixl_transport.rs`, the only `unsafe` module besides the registered-buffer slices).

```
node1 (GPU host)                                   node2 (storage host)
caller -> Client (staging buf, registered) --RDMA WRITE/READ (NIXL/UCX, RoCE)--> kvstored (registered DRAM pool, 8 x 64 MiB slots)
              \--- control: length-prefixed JSON over TCP (10.10.10.x) ---/        |  POSIX file I/O
                                                                                   v
                                                         <storage_root>/<namespace>/<xx>/<sha256(ns,key)>.obj
```

## Object file format
`"RKVOBJ01" | u32 LE header_len | header JSON {namespace,key,meta{schema_version,fingerprint,format,size,sha256},created_unix} | payload`.
Publication: write `.tmp-*` -> fsync -> `link()` (never overwrites) -> unlink tmp -> fsync dir. PUT is acknowledged only after that.

## Semantics
* PUT: same key + identical metadata/checksum = idempotent success (`stored=false`); same key + different content = `CONFLICT`.
* GET: the daemon re-hashes the whole file before serving; the client re-hashes what it received. Corruption = `CHECKSUM_MISMATCH`, never bytes.
* Absent key = `MISS` (not an error). Other codes: `TIMEOUT UNAVAILABLE INCOMPATIBLE QUOTA_EXCEEDED IO_ERROR BAD_REQUEST TOO_LARGE UNAUTHORIZED`.
* `fingerprint` is an opaque compatibility string the caller compares (model revision, quantisation, KV dtype, ...). The store keeps it with the object.
* Restart: objects survive; orphan `.tmp-*` files are removed at startup; clients must create a new `Client` (new NIXL agent, fresh metadata) after a daemon restart.
* A dropped connection / interrupted transfer never publishes a partial object; slots held by the dead connection are quarantined (default 60 s) before reuse.
* The storage root is configured only on the daemon; clients cannot choose paths. A root under `/srv/llm-models` or `/mnt/node2-models` is refused. Namespaces are `[A-Za-z0-9._-]{1,64}`; keys are hashed.

## Build (node2 has the Rust toolchain; binaries are copied to node1, both are Ubuntu 22.04)
```bash
# from the local-llm checkout on node1
scripts/kvstore-ctl.sh deploy         # rsync -> node2 ~/kv-build, cargo build --release, install to ~/kv-run/bin on both hosts
# or manually on node2:
cd ~/kv-build/rdma-kv-store && export PATH=$HOME/.cargo/bin:$PATH
NIXL_PREFIX=$HOME/rdma-kv-experiments/build/nixl-install cargo build --release && cargo test --release
cargo fmt --check && cargo check --release
```
Runtime needs `~/kv-run/env.sh` (see `env.sh.example`: LD_LIBRARY_PATH of the NIXL install) and a 0600 token file on both hosts.
Rust 1.99.1 (rustup, user-level, node2). Dependencies are pinned in `Cargo.lock` (sha2, serde, serde_json, libc).

## Run (optional subsystem, disabled by default: `config/rdma-kv-store.conf` -> `enabled=false`)
```bash
scripts/kvstore-ctl.sh enable        # or: start --force for a diagnostic start while disabled
scripts/kvstore-ctl.sh start         # starts kvstored on node2, waits until healthy (storage writable)
scripts/kvstore-ctl.sh status
scripts/kvstore-ctl.sh smoke         # PUT/GET 1 MiB, duplicate, conflict, miss; removes its own object
scripts/kvstore-ctl.sh footprint     # files, du, df, orphan tmp files
scripts/kvstore-ctl.sh stop          # SIGTERM to the owned pid, drains, SIGKILL only that pid after 15 s
scripts/kvstore-ctl.sh disable       # sets enabled=false and stops the daemon
```
Direct CLI (GPU node): `kvstorectl status|put|get|exists|delete|smoke|bench-cell --server 10.10.10.2:7471 --token-file ~/kv-run/token ...`.
Daemon flags: `kvstored --help` (or `--config file` with `key=value` lines).

## Functional checks
`cargo test --release` (6 file-store tests: 1 MiB PUT/GET + real file, duplicate idempotent/conflict, corrupt/truncated file, restart + orphan cleanup,
names/quota/size/single-owner, bad declared checksum). Two-host checks: `tests/scripts/gate2_*.sh` (multi-chunk, interrupted transfer, server-side corruption,
daemon restart, hung/stopped daemon -> bounded TIMEOUT, 4 in flight). Correctness checks only; the single benchmark is `kvstorectl bench-cell`.

## Crash / restore / cleanup / rollback
* Daemon killed: restart it; objects remain; clients reconnect with a new `Client`.
* Remove only owned experimental objects: `kvstorectl delete --ns <ns> --key <key>` (daemon needs `--allow-delete`) or stop the daemon and remove the
  test namespace directory under `storage_root` (never touch anything outside it).
* Rollback of the whole subsystem: `scripts/kvstore-ctl.sh disable`; delete `~/kv-run` and `~/llm-kv-cache` on node2 and `~/kv-build`; remove
  `components/rdma-kv-store`, `scripts/kvstore-ctl.sh`, `config/rdma-kv-store.conf`. Nothing in compose files, LiteLLM, swapd or the GPU engines references it.

## Security boundary (be honest)
Single shared token (constant-time compared) on a private link; no per-tenant identity. Namespace allow-list is optional (`--namespaces`). This is **not**
per-user isolation: a caller-supplied namespace is not authentication. Multi-user use needs identity propagated by the trusted gateway (not implemented).

## Limitations
One storage daemon, one root, one NIXL agent per process. NIXL calls are serialised by a process-wide lock. Whole-file checksum verification on every GET costs
one extra sequential read + SHA-256 on the storage node. No compression, replication, eviction or tiering.
