# benchmark/rpc

在负载下测 [`arpc`](../../arpc) 和 [`grpc`](../../grpc),主要用来比较
`Config.CoalesceWrites`(以及 `Connection.SetCoalesceWrites`)对那些 handler 在引擎池里
执行、而不在连接读 round 里执行的协议的影响。不依赖本模块以外的任何东西。

服务端和客户端是同一个程序,所以可以各自绑到自己的 CPU 上:

```sh
go run ./benchmark/rpc -role=server -proto=arpc &
go run ./benchmark/rpc -role=client -proto=arpc -conns=16 -callers=64 -dur=12s
```

`bench.sh` 对两个协议分别在该设置开和关时各跑几轮:

```sh
SERVER_CPUS=0-7 CLIENT_CPUS=8-15 SPLIT=1 benchmark/rpc/bench.sh 3
```

`SPLIT=1` 还会跑"服务端开/客户端关"和"服务端关/客户端开",用来看设置在哪一侧起作用。
绑定 CPU 需要 `taskset`,服务端 CPU 读自 `/proc`,所以这两项只在 Linux 上可用;其他系统只输出
每秒调用次数。

## 结果

Ryzen 7 7800X3D 上(服务端 8 个 CPU,客户端另外 8 个,16 个连接、每连接 64 个调用者,64B
回显,池里异步 handler,3 轮):

| | CoalesceWrites 开 | 关 |
| --- | ---: | ---: |
| arpc | 1.49M 次/秒 | 1.42M 次/秒 |
| grpc | 427k 次/秒 | 403k 次/秒 |

把两侧拆开后,收益全部来自客户端引擎的设置:只改服务端的设置,没有任何变化。这次客户端
是更忙的一方,所以对"客户端更强时的服务端"说明不了什么。这两个协议保持开启。
