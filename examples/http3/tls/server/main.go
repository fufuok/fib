//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

// Command server is an HTTP/3 echo server: it answers each request with its
// protocol, method, path and body.
//
//	go run ./examples/http3/tls/server
//
// HTTP/3 runs over UDP. The same handler also serves HTTPS on the TCP port of
// the same number, whose responses carry Alt-Svc, which is how a browser
// learns that it can switch to HTTP/3.
//
// Without -cert and -key it issues itself a certificate for localhost and
// writes it where the client looks for it.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	stdhttp "net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/examples/certs"
	"github.com/lesismal/fib/examples/example"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3"
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
	addr := flags.String("addr", "127.0.0.1:8445", "listen address, UDP for HTTP/3 and TCP for HTTPS")
	certFlags := certs.ServerFlagsOn(flags)
	if err := flags.Parse(args); err != nil {
		return err
	}

	tlsConfig, err := certFlags.Config()
	if err != nil {
		return err
	}
	_, port, err := net.SplitHostPort(*addr)
	if err != nil {
		return err
	}
	portNum, _ := strconv.Atoi(port)
	handler := echo(http3.AltSvc(portNum))

	// HTTP/3: a UDP engine whose handler is http3's. The http.Handler is the
	// same kind the HTTP/1 and HTTP/2 servers take.
	udpConfig := fib.DefaultConfig()
	udpConfig.Network = "udp"
	udpConfig.Addr = *addr
	udp, err := fib.Bind(udpConfig, http3.NewHandler(tlsConfig, handler))
	if err != nil {
		return err
	}

	// HTTPS over TCP, for clients that do not know yet that HTTP/3 is here.
	tcpConfig := fib.DefaultConfig()
	tcpConfig.Addr = *addr
	tcp, err := fib.Bind(tcpConfig, fibtls.NewServer(fibhttp.ConfigureTLS(tlsConfig), fibhttp.NewHandler(handler)))
	if err != nil {
		_ = udp.Close()
		return err
	}
	return example.Serve(ctx, out, ready, "HTTP/3 echo server listening on https://%s (UDP), HTTPS on the same TCP port", udp, tcp)
}

func echo(altSvc string) fibhttp.HandlerFunc {
	return func(c *fibhttp.Context) {
		body, _ := io.ReadAll(c.Request.Body)
		reply := fmt.Sprintf("%s %s %s %s", c.Request.Proto, c.Request.Method, c.Request.URL.Path, body)
		header := stdhttp.Header{"Content-Type": {"text/plain; charset=utf-8"}}
		if c.Request.ProtoMajor < 3 {
			header.Set("Alt-Svc", altSvc)
		}
		if err := c.WriteResponse(fibhttp.Response{StatusCode: stdhttp.StatusOK, Header: header, Body: []byte(reply)}); err != nil {
			c.Conn.Close()
		}
	}
}
