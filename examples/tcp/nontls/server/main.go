//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

// Command server is a TCP echo server.
//
//	go run ./examples/tcp/nontls/server
package main

import (
	"context"
	"flag"
	"io"
	"os"
	"os/signal"
	"syscall"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/examples/example"
)

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
	addr := flags.String("addr", "127.0.0.1:9000", "listen address")
	writev := flags.Bool("writev", true, "batch queued output with writev")
	if err := flags.Parse(args); err != nil {
		return err
	}

	config := fib.DefaultConfig()
	config.UseWritev = *writev
	engine, err := fib.New(config, fib.Binding{Addr: *addr, Handler: fib.HandlerFuncs{
		// TCP is a byte stream: data is whatever arrived in one read, not
		// necessarily one of the client's writes. An echo does not care.
		Data: func(c *fib.Connection, data []byte) {
			if c.Send(data) != nil {
				c.Close()
			}
		},
	}})
	if err != nil {
		return err
	}
	return example.Serve(ctx, out, ready, "TCP echo server listening on %s", engine)
}
