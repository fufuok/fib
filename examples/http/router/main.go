//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

// Command router serves a small REST API through fibhttp.Router: routes with
// parameters, a Route with middleware of its own, and a mounted router.
//
//	go run ./examples/http/router
//	curl http://127.0.0.1:8080/users/42
//	curl -X POST http://127.0.0.1:8080/users/42   # 405, Allow: GET, HEAD, DELETE
//	curl http://127.0.0.1:8080/api/v1/files/a/b.txt
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
	"github.com/lesismal/fib/middleware/logger"
	"github.com/lesismal/fib/middleware/recover"
	"github.com/lesismal/fib/middleware/requestid"
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
	if err := flags.Parse(args); err != nil {
		return err
	}

	r := fibhttp.NewRouter()
	// Middleware on the root router sees every request, 404s included.
	r.Use(recover.New(), logger.New(logger.Config{Output: out}))

	r.Get("/", func(c *fibhttp.Context) {
		reply(c, "index")
	})
	r.Route("/users", func(r *fibhttp.Router) {
		// Only the requests under /users get a request ID.
		r.Use(requestid.New())
		r.Get("/", func(c *fibhttp.Context) {
			reply(c, "all users")
		})
		r.Get("/{id:[0-9]+}", func(c *fibhttp.Context) {
			reply(c, "user "+c.Param("id"))
		})
		r.Delete("/{id:[0-9]+}", func(c *fibhttp.Context) {
			reply(c, "deleted user "+c.Param("id"))
		})
		r.Get("/{name}", func(c *fibhttp.Context) {
			reply(c, "user named "+c.Param("name"))
		})
	})
	r.Mount("/api/{version}", api())

	config := fib.DefaultConfig()
	config.Addr = *addr
	engine, err := fib.Bind(config, fibhttp.NewHandler(r))
	if err != nil {
		return err
	}
	return example.Serve(ctx, out, ready, "HTTP router listening on http://%s", engine)
}

// api is a router of its own, which routes what is left of the path under
// the pattern it is mounted at.
func api() *fibhttp.Router {
	r := fibhttp.NewRouter()
	r.Get("/files/*", func(c *fibhttp.Context) {
		reply(c, fmt.Sprintf("file %q of API %s, routed by %s", c.Param("*"), c.Param("version"), c.RoutePattern()))
	})
	return r
}

func reply(c *fibhttp.Context, text string) {
	_ = c.Respond(stdhttp.StatusOK, "text/plain; charset=utf-8", []byte(text+"\n"))
}
