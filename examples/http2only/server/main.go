//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

// Command server is an HTTP/2-only echo server: it answers each request with
// its protocol, method, path and body, like examples/http/nontls and
// examples/http/tls, but never falls back to HTTP/1 — a connection that does
// not open with the HTTP/2 preface is ended with GOAWAY. That suits a peer
// that only ever speaks HTTP/2, including conformance tools such as h2spec,
// which expect an HTTP/2-only server to behave this way.
//
//	go run ./examples/http2only/server                  # h2c, cleartext
//	go run ./examples/http2only/server -tls -addr :8443  # h2, over TLS
//
// Without -cert and -key, -tls issues a self-signed certificate for
// localhost and writes it where the TLS example client looks for it.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	stdhttp "net/http"
	"os"
	"os/signal"
	"syscall"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/examples/certs"
	"github.com/lesismal/fib/examples/example"
	fibhttp "github.com/lesismal/fib/http"
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
	addr := flags.String("addr", "127.0.0.1:8080", "listen address")
	useTLS := flags.Bool("tls", false, "serve HTTP/2 over TLS (h2) instead of cleartext (h2c)")
	certFlags := certs.ServerFlagsOn(flags)
	if err := flags.Parse(args); err != nil {
		return err
	}

	httpConfig := fibhttp.DefaultConfig()
	httpConfig.HTTP2Only = true
	handler := fibhttp.NewHandlerWithConfig(httpConfig, echo())

	config := fib.DefaultConfig()
	config.Addr = *addr

	scheme, engine, err := "http", (*fib.Engine)(nil), error(nil)
	if *useTLS {
		scheme = "https"
		tlsConfig, cfgErr := certFlags.Config()
		if cfgErr != nil {
			return cfgErr
		}
		engine, err = fib.Bind(config, fibtls.NewServer(fibhttp.ConfigureTLS(tlsConfig), handler))
	} else {
		engine, err = fib.Bind(config, handler)
	}
	if err != nil {
		return err
	}
	return example.Serve(ctx, out, ready, "HTTP/2-only echo server listening on "+scheme+"://%s", engine)
}

func echo() fibhttp.HandlerFunc {
	return func(c *fibhttp.Context) {
		body, _ := io.ReadAll(c.Request.Body)
		reply := fmt.Sprintf("%s %s %s %s", c.Request.Proto, c.Request.Method, c.Request.URL.Path, body)
		if err := c.Respond(stdhttp.StatusOK, "text/plain; charset=utf-8", []byte(reply)); err != nil {
			// The request went away before its answer did — an HTTP/2 client
			// resetting one stream, or a peer that hung up. The connection
			// carries the others, so it is left alone; one the engine cannot
			// use any more it closes itself.
			log.Printf("respond: %v", err)
		}
	}
}
