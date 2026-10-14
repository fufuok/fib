//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

// Command client posts messages to the HTTP echo server and prints the
// replies.
//
//	go run ./examples/http/nontls/client
package main

import (
	"flag"
	"fmt"
	"io"
	stdhttp "net/http"
	"os"
	"strings"

	"github.com/lesismal/fib/examples/example"
	fibhttp "github.com/lesismal/fib/http"
)

// fatal reports an error and exits; a test replaces it.
var fatal = example.Fatal

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fatal(err)
	}
}

func run(args []string, out io.Writer) error {
	flags := flag.NewFlagSet("client", flag.ContinueOnError)
	flags.SetOutput(out)
	url := flags.String("url", "http://127.0.0.1:8080/echo", "server URL")
	count := flags.Int("n", 5, "requests to send")
	if err := flags.Parse(args); err != nil {
		return err
	}

	engine, stop := example.ClientEngine()
	defer stop()
	client := fibhttp.NewClient(engine, fibhttp.DefaultClientConfig())
	defer client.Close()

	for i := 1; i <= *count; i++ {
		req, err := stdhttp.NewRequest(stdhttp.MethodPost, *url, strings.NewReader(example.Message(i)))
		if err != nil {
			return err
		}
		// Go returns at once; Wait blocks this goroutine, not the engine.
		// Client.Do takes a callback instead, for callers that must not wait.
		// The connection is kept alive and reused by the next request.
		resp, err := client.Go(req).Wait()
		if err != nil {
			return err
		}
		body, _ := io.ReadAll(resp.Body)
		fmt.Fprintf(out, "%s: %s\n", resp.Status, body)
	}
	return nil
}
