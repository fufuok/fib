//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

// Command server is an arpc server: an echo method, a typed service, and a
// stream that answers each message in upper case.
//
//	go run ./examples/arpc/server
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/arpc"
	"github.com/lesismal/fib/examples/example"
)

type SumReq struct{ A, B int }
type SumRsp struct{ Sum int }

// Math is registered as "Math.Sum".
type Math struct{}

func (Math) Sum(ctx context.Context, req *SumReq, rsp *SumRsp) { rsp.Sum = req.A + req.B }

// fatal reports a startup error and exits; a test replaces it.
var fatal = example.Fatal

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, nil); err != nil {
		fatal(err)
	}
}

// run serves until ctx ends. ready, if not nil, is told the address once the
// server is listening.
func run(ctx context.Context, args []string, out io.Writer, ready func(addr string)) error {
	flags := flag.NewFlagSet("server", flag.ContinueOnError)
	flags.SetOutput(out)
	addr := flags.String("addr", "127.0.0.1:8888", "listen address")
	if err := flags.Parse(args); err != nil {
		return err
	}

	server := arpc.NewServer()
	// Handlers run on the engine's handler pool unless told otherwise; an
	// echo is quick enough to answer where the connection is read.
	server.Handler.Handle("/echo", func(ctx *arpc.Context) {
		var s string
		if err := ctx.Bind(&s); err != nil {
			ctx.Error(err)
			return
		}
		ctx.Write(s)
	}, false)
	if err := server.Handler.Register("Math", Math{}); err != nil {
		return err
	}
	server.Handler.HandleStream("/upper", func(s *arpc.Stream) {
		defer s.CloseSend()
		for {
			var msg string
			if err := s.Recv(&msg); err != nil {
				if err != io.EOF {
					fmt.Fprintln(out, "stream:", err)
				}
				return
			}
			if err := s.Send(strings.ToUpper(msg)); err != nil {
				return
			}
		}
	})
	server.Handler.HandleConnected(func(c *arpc.Client) {
		fmt.Fprintf(out, "connected: %s\n", c.Conn().RemoteAddr())
	})

	config := fib.DefaultConfig()
	engine, err := fib.New(config, fib.Binding{Addr: *addr, Handler: server})
	if err != nil {
		return err
	}
	return example.Serve(ctx, out, ready, "arpc server listening on %s", engine)
}
