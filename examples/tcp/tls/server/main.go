//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

// Command server is a TCP echo server over TLS.
//
//	go run ./examples/tcp/tls/server
//
// Without -cert and -key it issues itself a certificate for localhost and
// writes it where the client looks for it.
package main

import (
	"context"
	"flag"
	"io"
	"os"
	"os/signal"
	"syscall"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/examples/certs"
	"github.com/lesismal/fib/examples/example"
	fibtls "github.com/lesismal/fib/tls"
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
	addr := flags.String("addr", "127.0.0.1:9443", "listen address")
	certFlags := certs.ServerFlagsOn(flags)
	if err := flags.Parse(args); err != nil {
		return err
	}

	tlsConfig, err := certFlags.Config()
	if err != nil {
		return err
	}
	echo := fib.HandlerFuncs{
		// The handler is the same as without TLS: fibtls.NewServer hands it
		// plaintext, and its Send encrypts.
		Data: func(c *fib.Connection, data []byte) {
			if c.Send(data) != nil {
				c.Close()
			}
		},
	}
	config := fib.DefaultConfig()
	engine, err := fib.New(config, fib.Binding{Addr: *addr, Handler: fibtls.NewServer(tlsConfig, echo)})
	if err != nil {
		return err
	}
	return example.Serve(ctx, out, ready, "TCP+TLS echo server listening on %s", engine)
}
