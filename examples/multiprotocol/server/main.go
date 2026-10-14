//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

// Command server serves five protocols from one engine, each on addresses of
// its own: plain HTTP, HTTPS with HTTP/1.1 and HTTP/2, HTTP/3 on the UDP port
// of the HTTPS port's number, WebSocket, and a raw TCP echo on a group of
// ports. One more port serves several of them at once: package mux looks at
// what each client sends first, and hands the connection to HTTPS, to plain
// HTTP or h2c, or to the raw echo.
//
//	go run ./examples/multiprotocol/server
//
// Every binding shares the engine's event loops, its descriptor table, its
// task pool and its buffer pool, where an engine per protocol would give each
// its own copy of all four. Without -cert and -key it issues itself a
// certificate for localhost and writes it where the TLS clients look for it.
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
	"strings"
	"syscall"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/examples/certs"
	"github.com/lesismal/fib/examples/example"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3"
	"github.com/lesismal/fib/mux"
	fibtls "github.com/lesismal/fib/tls"
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

// run serves until ctx ends. ready, if not nil, is told the plain HTTP
// address once the server is listening.
func run(ctx context.Context, args []string, out io.Writer, ready func(addr string)) error {
	flags := flag.NewFlagSet("server", flag.ContinueOnError)
	flags.SetOutput(out)
	httpAddr := flags.String("http", "127.0.0.1:8090", "plain HTTP address")
	httpsAddr := flags.String("https", "127.0.0.1:8453", "HTTPS address over TCP, and HTTP/3 over UDP")
	wsAddr := flags.String("ws", "127.0.0.1:8091", "WebSocket address")
	tcpAddrs := flags.String("tcp", "127.0.0.1:9010,127.0.0.1:9011", "comma-separated raw TCP echo addresses")
	sharedAddr := flags.String("shared", "127.0.0.1:9012", "one port for HTTPS, HTTP, h2c and the TCP echo, told apart by their first bytes")
	certFlags := certs.ServerFlagsOn(flags)
	if err := flags.Parse(args); err != nil {
		return err
	}

	tlsConfig, err := certFlags.Config()
	if err != nil {
		return err
	}
	_, port, err := net.SplitHostPort(*httpsAddr)
	if err != nil {
		return err
	}
	portNum, _ := strconv.Atoi(port)
	web := describe(http3.AltSvc(portNum))

	plain := fibhttp.NewHandler(web)
	secure := fibtls.NewServer(fibhttp.ConfigureTLS(tlsConfig), fibhttp.NewHandler(web))
	config := fib.DefaultConfig()
	config.Bindings = []fib.Binding{
		// The handler Bind is given serves the bindings that name none:
		// here, plain HTTP.
		{Addr: *httpAddr},
		{Addr: *httpsAddr, Handler: secure},
		{Network: "udp", Addr: *httpsAddr, Handler: http3.NewHandler(tlsConfig, web)},
		{Addr: *wsAddr, Handler: wsEcho()},
		{Addrs: strings.Split(*tcpAddrs, ","), Handler: tcpEcho()},
		// Every protocol whose client speaks first can share a port. The echo
		// takes whatever is neither TLS nor HTTP.
		{Addr: *sharedAddr, Handler: mux.NewWithConfig(mux.Config{Fallback: tcpEcho()},
			mux.Route{Match: mux.TLS(), Handler: secure},
			mux.Route{Match: mux.HTTP(), Handler: plain},
		)},
	}
	engine, err := fib.Bind(config, plain)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "HTTP on http://%s, HTTPS on https://%s, HTTP/3 on https://%s (UDP), WebSocket on ws://%s/ws, TCP echo on %s, all but WebSocket and HTTP/3 on %s\n",
		*httpAddr, *httpsAddr, *httpsAddr, *wsAddr, *tcpAddrs, *sharedAddr)
	return example.Serve(ctx, out, ready, "multi-protocol server listening on http://%s", engine)
}

// describe answers each request with its protocol, method, path and body,
// and points the clients of HTTP/1 and HTTP/2 to HTTP/3 with Alt-Svc.
func describe(altSvc string) fibhttp.HandlerFunc {
	return func(c *fibhttp.Context) {
		body, _ := io.ReadAll(c.Request.Body)
		reply := fmt.Sprintf("%s %s %s %s", c.Request.Proto, c.Request.Method, c.Request.URL.Path, body)
		header := stdhttp.Header{"Content-Type": {"text/plain; charset=utf-8"}}
		if c.Request.TLS != nil && c.Request.ProtoMajor < 3 {
			header.Set("Alt-Svc", altSvc)
		}
		if err := c.WriteResponse(fibhttp.Response{StatusCode: stdhttp.StatusOK, Header: header, Body: []byte(reply)}); err != nil {
			c.Conn.Close()
		}
	}
}

// wsEcho echoes every WebSocket message.
func wsEcho() fib.Handler {
	return websocket.NewHandler(websocket.HandlerFuncs{
		Message: func(c *websocket.Connection, opcode websocket.Opcode, data []byte) {
			if err := c.WriteMessage(opcode, data); err != nil {
				_ = c.Close(websocket.CloseInternalError, "write failed")
			}
		},
	})
}

// tcpEcho echoes the bytes of a raw TCP connection.
func tcpEcho() fib.Handler {
	return fib.HandlerFuncs{
		Data: func(c *fib.Connection, data []byte) {
			if err := c.Send(data); err != nil {
				c.Close()
			}
		},
	}
}
