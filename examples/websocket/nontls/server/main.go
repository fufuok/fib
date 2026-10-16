//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

// Command server is a WebSocket echo server. CI runs the Autobahn test suite
// against it.
//
//	go run ./examples/websocket/nontls/server
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
	"github.com/lesismal/fib/websocket"
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
	addr := flags.String("addr", "127.0.0.1:8081", "listen address")
	compress := flags.Bool("compress", false, "accept permessage-deflate compression")
	if err := flags.Parse(args); err != nil {
		return err
	}

	config := fib.DefaultConfig()
	engine, err := fib.New(config, fib.Binding{Addr: *addr, Handler: echo(out, *compress)})
	if err != nil {
		return err
	}
	return example.Serve(ctx, out, ready, "WebSocket echo server listening on ws://%s", engine)
}

func echo(out io.Writer, compress bool) fib.Handler {
	config := websocket.DefaultConfig()
	config.EnableCompression = compress
	return websocket.NewHandlerWithConfig(config, websocket.HandlerFuncs{
		Open: func(_ *websocket.Connection, r *stdhttp.Request) {
			fmt.Fprintf(out, "%s: open %s\n", r.RemoteAddr, r.URL.Path)
		},
		// A message arrives whole, reassembled from its frames.
		Message: func(c *websocket.Connection, opcode websocket.Opcode, data []byte) {
			if err := c.WriteMessage(opcode, data); err != nil {
				_ = c.Close(websocket.CloseInternalError, "write failed")
			}
		},
		Close: func(_ *websocket.Connection, code uint16, reason string, err error) {
			fmt.Fprintf(out, "closed: code=%d reason=%q error=%v\n", code, reason, err)
		},
	})
}
