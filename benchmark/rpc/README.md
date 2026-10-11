# benchmark/rpc

Measures [`arpc`](../../arpc) and [`grpc`](../../grpc) under load, mainly to compare
`Config.CoalesceWrites` (and `Connection.SetCoalesceWrites`) on protocols whose
handlers run on the engine's pool rather than on the connection's read round. It needs
nothing outside this module.

The server and the client are one program, so each can be pinned to CPUs of its own:

```sh
go run ./benchmark/rpc -role=server -proto=arpc &
go run ./benchmark/rpc -role=client -proto=arpc -conns=16 -callers=64 -dur=12s
```

`bench.sh` runs both protocols with the setting on and off, a few rounds in turn:

```sh
SERVER_CPUS=0-7 CLIENT_CPUS=8-15 SPLIT=1 benchmark/rpc/bench.sh 3
```

`SPLIT=1` also runs server on / client off and server off / client on, which shows
which side the setting matters on. CPU pinning needs `taskset`, and the server's CPU
reading `/proc`, so both are Linux only; elsewhere the numbers are calls a second.

## Result

On a Ryzen 7 7800X3D (server on 8 CPUs, client on the other 8, 16 connections of 64
callers, 64B echoes, async handlers on the pool, 3 rounds):

| | CoalesceWrites on | off |
| --- | ---: | ---: |
| arpc | 1.49M calls/s | 1.42M calls/s |
| grpc | 427k calls/s | 403k calls/s |

Splitting the two sides, the gain was all on the client's engine: with the server's
setting changed alone, nothing moved. The client was the busier side, so this says
little about a server with a stronger client. The setting stays on for these protocols.
