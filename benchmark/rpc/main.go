//go:build linux || darwin || windows

// Command rpc measures fib/arpc and fib/grpc under load, to compare settings
// such as Config.CoalesceWrites: handlers run on the engine's pool, and N
// connections of M callers each make echo calls for a fixed time. The server
// and the client are the same program, run with -role=server and
// -role=client, so that each can be pinned to CPUs of its own; see bench.sh.
//
//	go run ./benchmark/rpc -role=server &
//	go run ./benchmark/rpc -role=client
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/arpc"
	"github.com/lesismal/fib/examples/grpc/greeter"
	"github.com/lesismal/fib/grpc"
)

type gsrv struct{}

func (gsrv) SayHello(ctx context.Context, in *greeter.HelloRequest) (*greeter.HelloReply, error) {
	return &greeter.HelloReply{Message: in.Name}, nil
}
func (gsrv) SayHellos(*greeter.HelloRequest, grpc.ServerStreamingServer[greeter.HelloReply]) error {
	return nil
}

func fatal(err error) { fmt.Fprintln(os.Stderr, "error:", err); os.Exit(1) }

func main() {
	proto := flag.String("proto", "arpc", "arpc or grpc")
	role := flag.String("role", "server", "server or client")
	addr := flag.String("addr", "127.0.0.1:9100", "address")
	coalesce := flag.Bool("coalesce", true, "Config.CoalesceWrites of the engine (the server's, and the client's unless -client-coalesce says otherwise)")
	clientCoalesce := flag.String("client-coalesce", "", `Config.CoalesceWrites of the client's engine, "true" or "false"; empty takes -coalesce`)
	conns := flag.Int("conns", 16, "client connections")
	callers := flag.Int("callers", 64, "callers per connection")
	dur := flag.Duration("dur", 15*time.Second, "duration")
	size := flag.Int("size", 64, "payload bytes")
	flag.Parse()

	config := fib.DefaultConfig()
	config.CoalesceWrites = *coalesce
	if *role == "server" {
		config.Addr = *addr
		var h fib.Handler
		if *proto == "arpc" {
			s := arpc.NewServer()
			s.Handler.Handle("/echo", func(ctx *arpc.Context) {
				var v string
				if err := ctx.Bind(&v); err != nil {
					ctx.Error(err)
					return
				}
				ctx.Write(v)
			}, true)
			h = s
		} else {
			s := grpc.NewServer()
			greeter.RegisterGreeterServer(s, gsrv{})
			h = s
		}
		engine, err := fib.Bind(config, h)
		if err != nil {
			fatal(err)
		}
		go func() {
			c := make(chan os.Signal, 1)
			signal.Notify(c, os.Interrupt, syscall.SIGTERM)
			<-c
			engine.Stop()
		}()
		fmt.Println("listening", *addr)
		if err := engine.Run(); err != nil {
			fatal(err)
		}
		return
	}

	switch *clientCoalesce {
	case "":
	case "true", "false":
		config.CoalesceWrites = *clientCoalesce == "true"
	default:
		fatal(fmt.Errorf("-client-coalesce must be true or false, not %q", *clientCoalesce))
	}
	engine, err := fib.NewEngine(config, nil)
	if err != nil {
		fatal(err)
	}
	go engine.Run()
	payload := strings.Repeat("x", *size)
	var calls, fails atomic.Int64
	var stop atomic.Bool
	var wg sync.WaitGroup
	call := make([]func(), 0, *conns)
	var closers []func()
	for i := 0; i < *conns; i++ {
		if *proto == "arpc" {
			c, err := arpc.Dial(engine, "tcp", *addr, 5*time.Second, nil)
			if err != nil {
				fatal(err)
			}
			closers = append(closers, c.Stop)
			call = append(call, func() {
				var rsp string
				if err := c.Call("/echo", payload, &rsp, 10*time.Second); err != nil {
					fails.Add(1)
				}
			})
		} else {
			cc, err := grpc.NewClient(*addr, grpc.WithEngine(engine))
			if err != nil {
				fatal(err)
			}
			closers = append(closers, func() { cc.Close() })
			cl := greeter.NewGreeterClient(cc)
			call = append(call, func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if _, err := cl.SayHello(ctx, &greeter.HelloRequest{Name: payload}); err != nil {
					fails.Add(1)
				}
			})
		}
	}
	// warm up
	for _, f := range call {
		f()
	}
	start := time.Now()
	for _, f := range call {
		for j := 0; j < *callers; j++ {
			wg.Add(1)
			go func(f func()) {
				defer wg.Done()
				for !stop.Load() {
					f()
					calls.Add(1)
				}
			}(f)
		}
	}
	time.Sleep(*dur)
	stop.Store(true)
	n := calls.Load()
	el := time.Since(start)
	wg.Wait()
	for _, c := range closers {
		c()
	}
	f := fails.Load()
	fmt.Printf("calls=%d fails=%d rate=%.0f/s\n", n-f, f, float64(n-f)/el.Seconds())
	engine.Stop()
}
