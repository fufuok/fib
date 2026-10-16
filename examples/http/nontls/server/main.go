//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

// Command server is an HTTP echo server: it answers each request with its
// method, path and body. With -dir it also serves that directory's files
// under /files/, through net/http's FileServer, which gets Range and
// conditional requests from net/http and sends each file by sendfile(2).
//
//	go run ./examples/http/nontls/server -dir .
//	curl -O http://127.0.0.1:8080/files/go.mod
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
	"strings"
	"syscall"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/examples/example"
	fibhttp "github.com/lesismal/fib/http"
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
	dir := flags.String("dir", "", "directory to serve under /files/")
	if err := flags.Parse(args); err != nil {
		return err
	}

	handler := echo()
	if *dir != "" {
		handler = withFiles(*dir, handler)
	}
	config := fib.DefaultConfig()
	engine, err := fib.New(config, fib.Binding{Addr: *addr, Handler: fibhttp.NewHandler(handler)})
	if err != nil {
		return err
	}
	return example.Serve(ctx, out, ready, "HTTP echo server listening on http://%s", engine)
}

func echo() fibhttp.HandlerFunc {
	return func(c *fibhttp.Context) {
		// The body has already been read off the connection whole, so
		// reading it here never waits on the network.
		body, _ := io.ReadAll(c.Request.Body)
		reply := fmt.Sprintf("%s %s %s", c.Request.Method, c.Request.URL.Path, body)
		if err := c.Respond(stdhttp.StatusOK, "text/plain; charset=utf-8", []byte(reply)); err != nil {
			// The request went away before its answer did — an HTTP/2 client
			// resetting one stream, or a peer that hung up. The connection
			// carries the others, so it is left alone; one the engine cannot
			// use any more it closes itself.
			log.Printf("respond: %v", err)
		}
	}
}

// withFiles serves dir under /files/ and leaves every other path to next.
// Context is an http.ResponseWriter, so net/http's handlers answer through it.
func withFiles(dir string, next fibhttp.HandlerFunc) fibhttp.HandlerFunc {
	files := stdhttp.StripPrefix("/files/", stdhttp.FileServer(stdhttp.Dir(dir)))
	return func(c *fibhttp.Context) {
		if strings.HasPrefix(c.Request.URL.Path, "/files/") {
			files.ServeHTTP(c, c.Request)
			return
		}
		next(c)
	}
}
