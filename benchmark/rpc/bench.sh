#!/bin/bash
# Compares Config.CoalesceWrites on fib/arpc and fib/grpc.
#
#   benchmark/rpc/bench.sh [rounds]
#
# Each round runs, for arpc and then grpc, the server and the client with it on
# and then off, and prints calls a second and the server's CPU. Environment:
#
#   DUR           time of each run (default 12s)
#   CONNS         client connections (default 16)
#   CALLERS       callers per connection (default 64)
#   SERVER_CPUS   taskset CPU list for the server, e.g. 0-7 (Linux; default unpinned)
#   CLIENT_CPUS   taskset CPU list for the client, e.g. 8-15
#   SPLIT=1       also run server on / client off and server off / client on,
#                 which tells which side the setting matters on
set -euo pipefail
cd "$(dirname "$0")/../.."
BIN=${BIN:-$(mktemp -d)/rpc}
go build -o "$BIN" ./benchmark/rpc
ROUNDS=${1:-3}
ADDR=127.0.0.1:9100

# pin runs a command, on the CPUs of the list if there is one. It execs, so that
# the pid of a background pin is the command's own, which the server is stopped by.
pin() { # pin <cpus> <command...>
    local cpus=$1; shift
    if [ -n "$cpus" ] && command -v taskset >/dev/null; then exec taskset -c "$cpus" "$@"; else exec "$@"; fi
}

run() { # run <proto> <server coalesce> <client coalesce>
    local proto=$1 sc=$2 cc=$3
    pin "${SERVER_CPUS:-}" "$BIN" -proto="$proto" -role=server -coalesce="$sc" -addr=$ADDR >/dev/null 2>&1 &
    local pid=$!
    sleep 1.5
    local c0 c1 t0 t1
    cpu() { [ -r /proc/$pid/stat ] && awk '{print $14+$15}' /proc/$pid/stat || echo 0; }
    c0=$(cpu); t0=$(date +%s.%N)
    local out
    out=$(pin "${CLIENT_CPUS:-}" "$BIN" -proto="$proto" -role=client -coalesce="$sc" -client-coalesce="$cc" \
        -addr=$ADDR -dur="${DUR:-12s}" -conns="${CONNS:-16}" -callers="${CALLERS:-64}" 2>&1 | tail -1)
    c1=$(cpu); t1=$(date +%s.%N)
    kill -TERM $pid 2>/dev/null || true
    for _ in 1 2 3 4 5 6 7 8 9 10; do kill -0 $pid 2>/dev/null || break; sleep 0.3; done
    kill -KILL $pid 2>/dev/null || true
    wait $pid 2>/dev/null || true
    local pct="n/a"
    if [ -r /proc/$$/stat ]; then
        pct=$(awk -v a="$c0" -v b="$c1" -v x="$t0" -v y="$t1" 'BEGIN{printf "%.0f%%", (b-a)/100/(y-x)*100}')
    fi
    echo "$proto server=$sc client=$cc $out servercpu=$pct"
}

for r in $(seq "$ROUNDS"); do
    echo "== round $r"
    for proto in arpc grpc; do
        run "$proto" true true
        run "$proto" false false
        if [ "${SPLIT:-}" = 1 ]; then
            run "$proto" true false
            run "$proto" false true
        fi
    done
done
