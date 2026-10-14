//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

// Command upstream is the backend the gateway example forwards to. It speaks
// HTTP/1.1 on a cleartext port, and HTTP/2 and HTTP/3 on a TLS one, so that
// the gateway can be shown reaching an upstream over each protocol.

// 	go run ./examples/gateway/upstream

// It serves:

// 	/echo       the request's protocol, method, path, query, forwarding headers and body
// 	/download   ?size=N bytes, with a Content-Length
// 	/stream     ?n=5&delay=200ms lines, one at a time, then a trailer
// 	/upload     counts the request body
// 	/status     ?code=N answers with that status
// 	/ws         a WebSocket echo server, which closes with code 4002 when sent "close"

// Without -cert and -key it issues itself a certificate for localhost and
// writes it where the gateway looks for it.
package main

import (
	"context"
	"flag"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/lesismal/fib/examples/certs"
	"github.com/lesismal/fib/examples/example"
	"github.com/lesismal/fib/examples/gateway/upstream/backend"
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

// run serves until ctx ends. ready, if not nil, is told the TLS address once
// the server is listening.
func run(ctx context.Context, args []string, out io.Writer, ready func(addr string)) error {
	flags := flag.NewFlagSet("upstream", flag.ContinueOnError)
	flags.SetOutput(out)
	addr := flags.String("addr", "127.0.0.1:9443", "TLS listen address: TCP for HTTP/2 and HTTP/1.1, UDP for HTTP/3")
	plainAddr := flags.String("plain-addr", "127.0.0.1:9000", "cleartext HTTP/1.1 and ws:// listen address; empty to disable")
	certFlags := certs.ServerFlagsOn(flags)
	if err := flags.Parse(args); err != nil {
		return err
	}

	tlsConfig, err := certFlags.Config()
	if err != nil {
		return err
	}
	engines, err := backend.NewEngines(tlsConfig, *addr, *plainAddr)
	if err != nil {
		return err
	}
	banner := "upstream listening on https://%s (HTTP/2, HTTP/1.1; HTTP/3 on UDP)"
	if len(engines) > 2 {
		banner += " and http://" + strings.ReplaceAll(example.Addr(engines[2]), "%", "%%")
	}
	return example.Serve(ctx, out, ready, banner, engines...)
}
