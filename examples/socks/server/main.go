//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

// Command server is a proxy: SOCKS5, with UDP ASSOCIATE on the UDP port of
// the same number, SOCKS4 and SOCKS4a, and on the same TCP port an HTTP
// proxy for CONNECT, which package mux tells apart by the first bytes a
// client sends.
//
//	go run ./examples/socks/server
//	curl --socks5-hostname 127.0.0.1:1080 https://example.com/
//	curl --proxy http://127.0.0.1:1080 https://example.com/
//
// With -user and -pass it asks SOCKS5 and HTTP clients to log in, and turns
// SOCKS4 clients, which have no password to give, away:
//
//	go run ./examples/socks/server -user me -pass secret
//	curl --proxy socks5h://me:secret@127.0.0.1:1080 https://example.com/
//	curl --proxy http://me:secret@127.0.0.1:1080 https://example.com/
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	stdhttp "net/http"
	"os"
	"os/signal"
	"syscall"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/examples/example"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/mux"
	"github.com/lesismal/fib/socks"
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

// run serves until ctx ends. ready, if not nil, is told the address once
// the proxy is listening.
func run(ctx context.Context, args []string, out io.Writer, ready func(addr string)) error {
	flags := flag.NewFlagSet("server", flag.ContinueOnError)
	flags.SetOutput(out)
	addr := flags.String("addr", "127.0.0.1:1080", "TCP address, and UDP address for UDP ASSOCIATE")
	user := flags.String("user", "", "username SOCKS5 clients must log in with")
	pass := flags.String("pass", "", "password SOCKS5 clients must log in with")
	if err := flags.Parse(args); err != nil {
		return err
	}

	var config socks.Config
	var connect fibhttp.ConnectProxyConfig
	if *user != "" || *pass != "" {
		config.Authenticate = func(u, p string) bool { return u == *user && p == *pass }
		connect.Authenticate = config.Authenticate
	}
	config.Allow = func(r *socks.Request) error {
		fmt.Fprintf(out, "%s %s %s from %s\n", r.Version, r.Command, r.Addr, r.Conn.RemoteAddr())
		return nil
	}
	connect.Allow = func(r *stdhttp.Request) error {
		fmt.Fprintf(out, "%s CONNECT %s from %s\n", r.Proto, r.Host, r.RemoteAddr)
		return nil
	}
	server := socks.NewServer(config)
	engine, err := fib.New(fib.DefaultConfig(),
		fib.Binding{Addr: *addr, Handler: mux.New(
			mux.Route{Match: mux.SOCKS(), Handler: server},
			mux.Route{Match: mux.HTTP(), Handler: fibhttp.NewHandler(fibhttp.NewConnectProxy(connect))},
		)},
		fib.Binding{Network: "udp", Addr: *addr, Handler: server},
	)
	if err != nil {
		return err
	}
	return example.Serve(ctx, out, ready, "SOCKS and HTTP proxy listening on %s", engine)
}
