//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

// Command server is a SOCKS proxy: SOCKS5, with UDP ASSOCIATE on the UDP
// port of the same number, and SOCKS4 and SOCKS4a.
//
//	go run ./examples/socks/server
//	curl --socks5-hostname 127.0.0.1:1080 https://example.com/
//
// With -user and -pass it asks SOCKS5 clients to log in, and turns SOCKS4
// clients, which have no password to give, away:
//
//	go run ./examples/socks/server -user me -pass secret
//	curl --proxy socks5h://me:secret@127.0.0.1:1080 https://example.com/
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/examples/example"
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
	if *user != "" || *pass != "" {
		config.Authenticate = func(u, p string) bool { return u == *user && p == *pass }
	}
	config.Allow = func(r *socks.Request) error {
		fmt.Fprintf(out, "%s %s %s from %s\n", r.Version, r.Command, r.Addr, r.Conn.RemoteAddr())
		return nil
	}
	server := socks.NewServer(config)
	engineConfig := fib.DefaultConfig()
	engineConfig.Bindings = []fib.Binding{
		{Addr: *addr, Handler: server},
		{Network: "udp", Addr: *addr, Handler: server},
	}
	engine, err := fib.Bind(engineConfig, nil)
	if err != nil {
		return err
	}
	return example.Serve(ctx, out, ready, "SOCKS proxy listening on %s", engine)
}
