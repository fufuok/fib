//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

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
	"io"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/arpc"
	"github.com/lesismal/fib/examples/example"
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

// fatal reports an error and exits; a test replaces it.
var fatal = example.Fatal

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, nil); err != nil {
		fatal(err)
	}
}

// run is the server, which serves until ctx ends and tells ready, if not nil,
// its address once it is listening, or the client, which prints its result.
func run(ctx context.Context, args []string, out io.Writer, ready func(addr string)) error {
	flags := flag.NewFlagSet("rpc", flag.ContinueOnError)
	flags.SetOutput(out)
	proto := flags.String("proto", "arpc", "arpc or grpc")
	role := flags.String("role", "server", "server or client")
	addr := flags.String("addr", "127.0.0.1:9100", "address")
	coalesce := flags.Bool("coalesce", true, "Config.CoalesceWrites of the engine (the server's, and the client's unless -client-coalesce says otherwise)")
	clientCoalesce := flags.String("client-coalesce", "", `Config.CoalesceWrites of the client's engine, "true" or "false"; empty takes -coalesce`)
	conns := flags.Int("conns", 16, "client connections")
	callers := flags.Int("callers", 64, "callers per connection")
	dur := flags.Duration("dur", 15*time.Second, "duration")
	size := flags.Int("size", 64, "payload bytes")
	if err := flags.Parse(args); err != nil {
		return err
	}

	config := fib.DefaultConfig()
	config.CoalesceWrites = *coalesce
	if *role == "server" {
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
		engine, err := fib.New(config, fib.Binding{Addr: *addr, Handler: h})
		if err != nil {
			return err
		}
		return example.Serve(ctx, out, ready, "listening %s", engine)
	}

	switch *clientCoalesce {
	case "":
	case "true", "false":
		config.CoalesceWrites = *clientCoalesce == "true"
	default:
		return fmt.Errorf("-client-coalesce must be true or false, not %q", *clientCoalesce)
	}
	engine, err := fib.New(config)
	if err != nil {
		return err
	}
	stopEngine := example.Start(engine)
	defer stopEngine()
	payload := strings.Repeat("x", *size)
	// calls counts the calls that succeeded, fails those that did not.
	var calls, fails atomic.Int64
	var stop atomic.Bool
	var wg sync.WaitGroup
	call := make([]func() error, 0, *conns)
	var closers []func()
	// The connections close before the engine stops.
	defer func() {
		for _, c := range closers {
			c()
		}
	}()
	for i := 0; i < *conns; i++ {
		if *proto == "arpc" {
			c, err := arpc.Dial(engine, "tcp", *addr, 5*time.Second, nil)
			if err != nil {
				return err
			}
			closers = append(closers, c.Stop)
			call = append(call, func() error {
				var rsp string
				return c.Call("/echo", payload, &rsp, 10*time.Second)
			})
		} else {
			cc, err := grpc.NewClient(*addr, grpc.WithEngine(engine))
			if err != nil {
				return err
			}
			closers = append(closers, func() { cc.Close() })
			cl := greeter.NewGreeterClient(cc)
			call = append(call, func() error {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_, err := cl.SayHello(ctx, &greeter.HelloRequest{Name: payload})
				return err
			})
		}
	}
	// warm up
	for _, f := range call {
		_ = f()
	}
	start := time.Now()
	for _, f := range call {
		for j := 0; j < *callers; j++ {
			wg.Add(1)
			go func(f func() error) {
				defer wg.Done()
				for !stop.Load() {
					if f() != nil {
						fails.Add(1)
					} else {
						calls.Add(1)
					}
				}
			}(f)
		}
	}
	time.Sleep(*dur)
	stop.Store(true)
	n, f := calls.Load(), fails.Load()
	el := time.Since(start)
	wg.Wait()
	fmt.Fprintf(out, "calls=%d fails=%d rate=%.0f/s\n", n, f, float64(n)/el.Seconds())
	return nil
}
