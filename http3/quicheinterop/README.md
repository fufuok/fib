# fib HTTP/3 ⇄ cloudflare/quiche interop

Runs fib's HTTP/3 client and server against [cloudflare/quiche](https://github.com/cloudflare/quiche),
an independent QUIC and HTTP/3 implementation, with both of fib's QUIC handshakes
(the default state machine and `BlockingTLSHandshake`).

```sh
./run.sh            # needs Docker and the go-http3-benchmark:local image
```

The image is the one built from go-http3-benchmark: it carries the quiche
server (`quiche-server`), the benchmark's quiche client (`benchcli-rust`) and
Go. The container has no network; the repository is mounted read-only.

| Direction | What it checks |
| --- | --- |
| quiche client → fib server (`h3s`) | 300 connections, 30 000 echoes with body comparison, 87 000 multiplexed streams (`benchcli-rust -check`) |
| quiche client → fib server | session resumption: `quicheclient` hands each connection's ticket to the next; every round after the first must resume (`is_resumed`) and still echo the body (0, 1 KiB, 200 KB) |
| quiche client → fib server | early data enabled on the client: fib's tickets do not offer 0-RTT, so quiche does not send any, and the request is served after the handshake |
| fib client (`h3c`) → quiche server | 300 connections × 2 requests of 0 B … 300 KB, echoed correctly |
| fib client → quiche server | certificate verification (`h3c -verify`): the server's own certificate as `RootCAs` connects; another root, a name the certificate lacks, and the system roots are refused |

quiche's server makes a fresh self-signed certificate every start, so `-verify`
learns it through `VerifyPeerCertificate` on a first connection and then uses it
as the only root.

## Layout

- `run.sh` – starts the container; `inside.sh` – what runs in it
- `h3s/` – fib HTTP/3 echo server on the benchmark's ports (UDP 11001–11050)
- `h3c/` – fib HTTP/3 client for quiche's server (UDP 13001–13050)
- `quicheclient/` – a small quiche client (Rust) that resumes sessions and can enable 0-RTT; built offline against the image's locked quiche
- `go.mod` – a module of its own with `replace ../..`, so it is not part of fib's

See also `../interop`, the quic-go peer the conformance tests use.
