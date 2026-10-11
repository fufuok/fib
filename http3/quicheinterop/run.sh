#!/bin/bash
# Runs fib's HTTP/3 against cloudflare/quiche, both ways round, inside the
# go-http3-benchmark image (see README.md). Usage: ./run.sh [image]
set -eu
here="$(cd "$(dirname "$0")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
image="${1:-go-http3-benchmark:local}"
exec docker run --rm --network none \
  -v "$repo":/fib:ro \
  --entrypoint bash "$image" /fib/http3/quicheinterop/inside.sh
