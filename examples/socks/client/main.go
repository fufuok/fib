//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

// Command client fetches a URL through a SOCKS proxy with package http's
// client, whose connections the proxy's Dialer opens.
//
//	go run ./examples/socks/server &
//	go run ./examples/socks/client -url https://example.com/
package main

import (
	"flag"
	"fmt"
	"io"
	stdhttp "net/http"
	"os"
	"time"

	"github.com/lesismal/fib/examples/example"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/socks"
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
	proxyURL := flags.String("proxy", "socks5h://127.0.0.1:1080",
		"proxy: socks5:// or socks4:// resolve names here, socks5h:// and socks4a:// at the proxy")
	target := flags.String("url", "http://example.com/", "URL to fetch")
	if err := flags.Parse(args); err != nil {
		return err
	}
	proxy, err := socks.ParseURL(*proxyURL)
	if err != nil {
		return err
	}
	req, err := stdhttp.NewRequest(stdhttp.MethodGet, *target, nil)
	if err != nil {
		return err
	}

	engine, stop := example.ClientEngine()
	defer stop()
	config := fibhttp.DefaultClientConfig()
	config.Timeout = 15 * time.Second
	config.Dial = proxy.Dial
	client := fibhttp.NewClient(engine, config)
	defer client.Close()

	resp, err := client.Go(req).Wait()
	if err != nil {
		return err
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "%s %s\n%s", resp.Proto, resp.Status, body)
	return nil
}
