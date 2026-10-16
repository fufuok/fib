//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

// Command server is a UDP echo server.
//
//	go run ./examples/udp/nontls/server
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

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
	addr := flags.String("addr", "127.0.0.1:9001", "listen address")
	idle := flags.Duration("idle", time.Minute, "close a peer after this long without a datagram")
	if err := flags.Parse(args); err != nil {
		return err
	}

	config := fib.DefaultConfig()
	config.Network = "udp"
	config.UDPIdleTimeout = *idle
	engine, err := fib.New(config, fib.Binding{Addr: *addr, Handler: fib.HandlerFuncs{
		// Every peer address is a connection of its own: OnOpen runs on its
		// first datagram and OnClose once it has been idle too long.
		Open: func(c *fib.Connection) { fmt.Fprintf(out, "peer %s: open\n", c.RemoteAddr()) },
		// data is exactly one datagram, and Send sends exactly one.
		Data: func(c *fib.Connection, data []byte) {
			if err := c.Send(data); err != nil {
				fmt.Fprintf(out, "peer %s: echo dropped: %v\n", c.RemoteAddr(), err)
			}
		},
		Close: func(c *fib.Connection, err error) { fmt.Fprintf(out, "peer %s: closed: %v\n", c.RemoteAddr(), err) },
	}})
	if err != nil {
		return err
	}
	return example.Serve(ctx, out, ready, "UDP echo server listening on %s", engine)
}
