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

# fib's tickets do not advertise early data, so quiche, which sends 0-RTT
# only against a ticket that does, never tries it: sent-as-0rtt stays false
# and the requests go out after the handshake. What this checks is that
# enabling early data on the client changes nothing for such a server.
step "quiche client -> fib server: early data enabled on the client (fib does not offer 0-RTT)"
for size in 1024 200000; do
  check $QC 127.0.0.1:11001 4 1 $size
done
kill $FIB; wait $FIB 2>/dev/null
tail -3 /tmp/fib.log

step "quiche client -> fib server, handshaking on crypto/tls's QUICConn (-blocking)"
/tmp/h3s -blocking > /tmp/fib2.log 2>&1 &
FIB=$!
sleep 2
check $QC 127.0.0.1:11001 4 1 1024
kill $FIB; wait $FIB 2>/dev/null

step "fib client -> quiche server: load, then certificate verification"
/workspace/target/release/quiche-server -b=1024 -streams=100 -idle=120s -t 2 > /tmp/quiche.log 2>&1 &
Q=$!
sleep 2
check /tmp/h3c -addr=127.0.0.1:13001 -n=300
check /tmp/h3c -addr=127.0.0.1:13001 -n=300 -blocking
check /tmp/h3c -addr=127.0.0.1:13001 -verify
kill $Q; wait $Q 2>/dev/null
tail -3 /tmp/quiche.log

echo
if [ $fail = 0 ]; then echo "ALL PASSED"; else echo "SOME FAILED"; fi
exit $fail
