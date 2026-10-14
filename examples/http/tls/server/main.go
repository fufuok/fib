//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

// Command server is an HTTPS echo server: it answers each request with its
// protocol, method, path and body. It speaks HTTP/2 to clients that choose it
// through ALPN and HTTP/1.1 to the rest.
//
//	go run ./examples/http/tls/server
//
// Without -cert and -key it issues itself a certificate for localhost and
// writes it where the client looks for it.
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
	addr := flags.String("addr", "127.0.0.1:8443", "listen address")
	certFlags := certs.ServerFlagsOn(flags)
	if err := flags.Parse(args); err != nil {
		return err
	}

	tlsConfig, err := certFlags.Config()
	if err != nil {
		return err
	}
	config := fib.DefaultConfig()
	config.Addr = *addr
	// The HTTP handler is the same one the plain server uses: fibtls.NewServer
	// decrypts in front of it and encrypts what it sends. ConfigureTLS offers
	// h2 through ALPN; the handler serves HTTP/2 and HTTP/1.1 alike.
	engine, err := fib.Bind(config, fibtls.NewServer(fibhttp.ConfigureTLS(tlsConfig), fibhttp.NewHandler(echo())))
	if err != nil {
		return err
	}
	return example.Serve(ctx, out, ready, "HTTPS echo server listening on https://%s", engine)
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
