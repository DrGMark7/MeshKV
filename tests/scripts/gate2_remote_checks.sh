. ~/kv-run/env.sh
cd ~/kv-run
CTL="./bin/kvstorectl --server 10.10.10.2:7471 --token-file $HOME/kv-run/token"
N2() { ssh hpcnc-gpu2 ". ~/kv-run/env.sh; $1"; }
RID=$(date +%H%M%S)
echo "== multi-chunk 100 MiB PUT + GET (2 chunks of 64 MiB)"
$CTL put --ns gate2 --key big-$RID --size-mib 100 --seed 11 2>&1 | grep -a "result="
$CTL get --ns gate2 --key big-$RID --expect-seed 11 2>&1 | grep -a "result="
echo "== interrupted transfer: connection dropped after 1 of 2 chunks, no commit"
$CTL put --ns gate2 --key intr-$RID --size-mib 100 --seed 12 --interrupt-after 1 2>&1 | grep -a "result="
sleep 1
$CTL exists --ns gate2 --key intr-$RID 2>&1 | grep -a "result="
N2 'find ~/llm-kv-cache -name ".tmp-*" | wc -l | sed "s/^/tmp files left: /"'
$CTL status 2>&1 | grep -a "result="
echo "== retry the same key afterwards (must succeed: nothing was published)"
$CTL put --ns gate2 --key intr-$RID --size-mib 100 --seed 12 2>&1 | grep -a "result="
echo "== server-side corruption of a stored file -> CHECKSUM_MISMATCH, never bytes"
F=$(N2 'ls -S ~/llm-kv-cache/gate2/*/*.obj | head -1')
N2 "python3 - <<PY
f='$F'
b=bytearray(open(f,'rb').read()); b[len(b)//2]^=0xff; open(f,'wb').write(b)
PY"
$CTL get --ns gate2 --key $( [ -n "$F" ] && echo big-$RID ) 2>&1 | grep -a "result="
echo "== daemon restart, reconnect, GET again"
PID=$(N2 'cat ~/kv-run/kvstored.pid'); N2 "tr '\0' ' ' < /proc/$PID/cmdline | cut -c1-60"
N2 "kill $PID; sleep 3; kill -0 $PID 2>/dev/null && echo STILL_RUNNING || echo daemon_stopped; tail -3 ~/kv-run/logs/kvstored.log | cut -c1-120"
echo "-- GET while daemon is down (must fail fast with a bounded error)"
S=$(date +%s.%N); $CTL get --ns gate2 --key smoke-g2a --deadline-ms 3000 2>&1 | grep -a "result="; echo "elapsed $(echo "$(date +%s.%N) - $S" | bc)"
N2 "cd ~/kv-run; nohup ./bin/kvstored --listen 10.10.10.2:7471 --root ~/llm-kv-cache --token-file ~/kv-run/token --allow-delete --pidfile ~/kv-run/kvstored.pid >> ~/kv-run/logs/kvstored.log 2>&1 & sleep 6; cat ~/kv-run/kvstored.pid"
$CTL get --ns gate2 --key smoke-g2a --expect-seed 42 2>&1 | grep -a "result="
$CTL status 2>&1 | grep -a "result="
