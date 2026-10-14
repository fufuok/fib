//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

// Command client posts messages to the HTTP/3 echo server and prints the
// replies.
//
//	go run ./examples/http3/tls/client
package main

import (
	"flag"
	"fmt"
	"io"
	stdhttp "net/http"
	"os"
	"strings"

	"github.com/lesismal/fib/examples/certs"
	"github.com/lesismal/fib/examples/example"
	"github.com/lesismal/fib/http3"
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
	url := flags.String("url", "https://127.0.0.1:8445/echo", "server URL")
	count := flags.Int("n", 5, "requests to send")
	certFlags := certs.ClientFlagsOn(flags)
	if err := flags.Parse(args); err != nil {
		return err
	}

	tlsConfig, err := certFlags.Config()
	if err != nil {
		return err
	}
	// The client dials UDP through an engine of any kind; this one has no
	// listener of its own.
	engine, stop := example.ClientEngine()
	defer stop()
	config := http3.DefaultClientConfig()
	// The server name comes from the URL's host.
	config.TLSConfig = tlsConfig
	client := http3.NewClient(engine, config)
	defer client.Close()

	for i := 1; i <= *count; i++ {
		req, err := stdhttp.NewRequest(stdhttp.MethodPost, *url, strings.NewReader(example.Message(i)))
		if err != nil {
			return err
		}
		resp, err := client.Go(req).Wait()
		if err != nil {
			return err
		}
		body, _ := io.ReadAll(resp.Body)
		fmt.Fprintf(out, "%s %s: %s\n", resp.Proto, resp.Status, body)
	}
	return nil
}
