. ~/kv-run/env.sh
scp -q hpcnc-gpu2:kv-build/rdma-kv-store/target/release/kvstorectl ~/kv-run/bin/kvstorectl
ssh hpcnc-gpu2 'cp ~/kv-build/rdma-kv-store/target/release/kvstorectl ~/kv-run/bin/'
cd ~/kv-run
CTL="./bin/kvstorectl --server 10.10.10.2:7471 --token-file $HOME/kv-run/token"
echo "== tiny in-flight correctness check (NOT a benchmark): 16 x 1 MiB, inflight 4"
$CTL bench-cell --ns gate2par --size-mib 1 --count 16 --inflight 4 --warmup 1 2>&1 | grep -a -E "^(cell|put|get|integrity|client)" | cut -c1-230
echo "== tiny: 8 x 64 MiB, inflight 4"
$CTL bench-cell --ns gate2par --size-mib 64 --count 8 --inflight 4 --warmup 0 2>&1 | grep -a -E "^(cell|put|get|integrity|client)" | cut -c1-230
$CTL status 2>&1 | grep -a "result=" | cut -c1-200
