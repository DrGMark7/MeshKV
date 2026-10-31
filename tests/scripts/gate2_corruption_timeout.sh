. ~/kv-run/env.sh
cd ~/kv-run
CTL="./bin/kvstorectl --server 10.10.10.2:7471 --token-file $HOME/kv-run/token"
N2() { ssh hpcnc-gpu2 ". ~/kv-run/env.sh; $1"; }
echo "== precise server-side corruption of key proof-1 (ns gate2)"
D=$(printf 'rdma-kv-store/v1\0gate2\0proof-1' | sha256sum | cut -d' ' -f1)
F="/home/hpcnc/llm-kv-cache/gate2/${D:0:2}/$D.obj"
N2 "ls -la $F | cut -c1-60"
$CTL get --ns gate2 --key proof-1 --expect-seed 5 2>&1 | grep -a "result=" | cut -c1-120
N2 "python3 - <<PY
f='$F'
b=bytearray(open(f,'rb').read()); b[len(b)//2]^=0xff; open(f,'wb').write(b)
PY"
$CTL get --ns gate2 --key proof-1 --expect-seed 5 2>&1 | grep -a "result="
echo "== hung server (SIGSTOP) -> bounded TIMEOUT, then resume"
PID=$(N2 'cat ~/kv-run/kvstored.pid'); N2 "tr '\0' ' ' < /proc/$PID/cmdline | cut -c1-40; kill -STOP $PID"
S=$(date +%s.%N); $CTL get --ns gate2 --key smoke-g2a --deadline-ms 3000 2>&1 | grep -a "result="; echo "elapsed $(echo "$(date +%s.%N) - $S" | bc)"
N2 "kill -CONT $PID"; sleep 1
$CTL get --ns gate2 --key smoke-g2a --expect-seed 42 2>&1 | grep -a "result=" | cut -c1-110
echo "== cleanup owned test objects of this namespace"
for k in smoke-g2a proof-1; do $CTL delete --ns gate2 --key $k 2>&1 | grep -a "result="; done
N2 'ls ~/llm-kv-cache/gate2/*/*.obj 2>/dev/null | wc -l | sed "s/^/objects left in gate2: /"'
