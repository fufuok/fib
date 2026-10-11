#!/bin/bash
# The half of run.sh that runs in the container: /fib is the repository,
# /workspace the benchmark's own tree with the quiche binaries.
set -u
fail=0
step() { echo; echo "=============== $*"; }
check() { "$@" || { echo "FAILED: $*"; fail=1; }; }

# Copy the module: it is mounted read-only, and go wants to write beside it.
cp -r /fib /tmp/fib
cd /tmp/fib/http3/quicheinterop
go build -buildvcs=false -o /tmp/h3s ./h3s || exit 1
go build -buildvcs=false -o /tmp/h3c ./h3c || exit 1

# The Rust client is built against the benchmark's locked quiche, offline,
# into the benchmark's target directory, so quiche itself is not rebuilt.
mkdir -p /tmp/qc && cp -r quicheclient/. /tmp/qc/ && cp /workspace/Cargo.lock /tmp/qc/
(cd /tmp/qc && CARGO_TARGET_DIR=/workspace/target cargo build --release --offline -q) || exit 1
QC=/workspace/target/release/quicheclient

step "quiche client -> fib server: 300 connections, 30000 echoes, multiplexed streams"
/tmp/h3s > /tmp/fib.log 2>&1 &
FIB=$!
sleep 2
check /workspace/target/release/benchcli-rust -f=fib -ip=127.0.0.1 -c=300 -dc=100 -ec=300 -en=30000 -b=1024 \
  -rate=true -rd=3 -rr=100 -check 2>&1 | grep -v '^$\|control request'
test "${PIPESTATUS[0]}" = 0 || fail=1

step "quiche client -> fib server: session resumption (a ticket from each connection offered to the next)"
for size in 0 1024 200000; do
  check $QC 127.0.0.1:11001 4 0 $size
done

# rounds runs quicheclient and checks that at least n of its rounds say what.
rounds() {
  local n=$1 what=$2; shift 2
  local out
  out=$("$@") || { echo "$out"; echo "FAILED: $*"; fail=1; return; }
  echo "$out"
  if [ "$(echo "$out" | grep -c -- "$what")" -lt "$n" ]; then
    echo "FAILED: fewer than $n rounds with $what: $*"; fail=1
  fi
}

step "quiche client -> fib server, early data not enabled on the server: no 0-RTT is offered"
rounds 4 "sent-as-0rtt=false" $QC 127.0.0.1:11001 4 1 get
kill $FIB; wait $FIB 2>/dev/null

step "quiche client -> fib server with 0-RTT: a GET goes in the first flight, and the server knows"
/tmp/h3s -early > /tmp/fib3.log 2>&1 &
FIB=$!
sleep 2
rounds 3 "sent-as-0rtt=true.*server-saw-early=true" $QC 127.0.0.1:11001 4 1 get
rounds 4 "server-saw-early=false" $QC 127.0.0.1:11001 4 0 get

step "quiche client -> fib server with 0-RTT: a POST in early data is answered 425 and sent again after the handshake"
rounds 3 "sent-as-0rtt=true sent-again-after-425=true" $QC 127.0.0.1:11001 4 1 1024
rounds 3 "sent-as-0rtt=true sent-again-after-425=true" $QC 127.0.0.1:11001 4 1 200000
kill $FIB; wait $FIB 2>/dev/null

step "quiche client -> fib server with 0-RTT for every method: the POST is served from early data"
/tmp/h3s -early-unsafe > /tmp/fib4.log 2>&1 &
FIB=$!
sleep 2
for size in 1024 200000; do
  rounds 3 "sent-as-0rtt=true sent-again-after-425=false" $QC 127.0.0.1:11001 4 1 $size
done
kill $FIB; wait $FIB 2>/dev/null

step "quiche client -> fib server, 0-RTT on crypto/tls's QUICConn (-blocking)"
/tmp/h3s -early -blocking > /tmp/fib5.log 2>&1 &
FIB=$!
sleep 2
rounds 3 "sent-as-0rtt=true.*server-saw-early=true" $QC 127.0.0.1:11001 4 1 get
kill $FIB; wait $FIB 2>/dev/null
tail -3 /tmp/fib3.log

# quiche-server does not offer early data. One that does is the same source
# with a line added.
cp /workspace/target/release/quiche-server /tmp/quiche-server-plain
rm -rf /tmp/ws && mkdir /tmp/ws
(cd /workspace && tar --exclude=./target -cf - .) | tar -xf - -C /tmp/ws
sed -i 's|c.grease(false);|c.grease(false);\n    c.enable_early_data();|' /tmp/ws/frameworks/quiche/src/server.rs
(cd /tmp/ws && CARGO_TARGET_DIR=/workspace/target cargo build --release --offline -q -p quiche-server) || exit 1
cp /workspace/target/release/quiche-server /tmp/quiche-server-early
cp /tmp/quiche-server-plain /workspace/target/release/quiche-server

step "fib client -> quiche server: load, then certificate verification"
/workspace/target/release/quiche-server -b=1024 -streams=100 -idle=120s -t 2 > /tmp/quiche.log 2>&1 &
Q=$!
sleep 2
check /tmp/h3c -addr=127.0.0.1:13001 -n=300
check /tmp/h3c -addr=127.0.0.1:13001 -n=300 -blocking
check /tmp/h3c -addr=127.0.0.1:13001 -verify

step "fib client -> quiche server: session resumption (quiche offers no early data)"
check /tmp/h3c -addr=127.0.0.1:13001 -resume 4
check /tmp/h3c -addr=127.0.0.1:13001 -resume 4 -blocking
kill $Q; wait $Q 2>/dev/null

step "fib client -> quiche server that accepts early data: 0-RTT"
/tmp/quiche-server-early -b=1024 -streams=100 -idle=120s -t 2 > /tmp/quiche2.log 2>&1 &
Q=$!
sleep 2
check /tmp/h3c -addr=127.0.0.1:13001 -resume 4 -want-early
kill $Q; wait $Q 2>/dev/null
tail -3 /tmp/quiche.log

echo
if [ $fail = 0 ]; then echo "ALL PASSED"; else echo "SOME FAILED"; fi
exit $fail
