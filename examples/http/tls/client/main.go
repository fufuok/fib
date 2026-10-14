//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

// Command client posts messages to the HTTPS echo server and prints the
// replies. It speaks HTTP/2 when the server offers it; -http1 keeps it on
// HTTP/1.1.
//
//	go run ./examples/http/tls/client
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
	url := flags.String("url", "https://127.0.0.1:8443/echo", "server URL")
	count := flags.Int("n", 5, "requests to send")
	http1 := flags.Bool("http1", false, "use HTTP/1.1 even if the server offers HTTP/2")
	certFlags := certs.ClientFlagsOn(flags)
	if err := flags.Parse(args); err != nil {
		return err
	}

	tlsConfig, err := certFlags.Config()
	if err != nil {
		return err
	}
	engine, stop := example.ClientEngine()
	defer stop()
	config := fibhttp.DefaultClientConfig()
	// Used for https:// URLs; the server name comes from the URL's host.
	config.TLSConfig = tlsConfig
	config.DisableHTTP2 = *http1
	client := fibhttp.NewClient(engine, config)
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
