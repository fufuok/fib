//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	stdhttp "net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/arpc"
	"github.com/lesismal/fib/examples/certs"
	"github.com/lesismal/fib/examples/grpc/greeter"
	"github.com/lesismal/fib/grpc"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/multiproto"
	"github.com/lesismal/fib/socks"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/websocket"
)

type greeterServer struct{}

func (greeterServer) SayHello(_ context.Context, in *greeter.HelloRequest) (*greeter.HelloReply, error) {
	return &greeter.HelloReply{Message: "hello, " + in.Name}, nil
}

func (greeterServer) SayHellos(*greeter.HelloRequest, grpc.ServerStreamingServer[greeter.HelloReply]) error {
	return nil
}

// serve runs what examples/multiproto/server does on a port of its own, and
// returns where it listens and the certificate file its clients trust.
func serve(t *testing.T) (addr, certFile string) {
	t.Helper()
	certFile = filepath.Join(t.TempDir(), "cert.pem")
	flags := flag.NewFlagSet("server", flag.ContinueOnError)
	certFlags := certs.ServerFlagsWithOut(flags, certFile)
	if err := flags.Parse(nil); err != nil {
		t.Fatal(err)
	}
	flags.SetOutput(io.Discard)
	tlsConfig, err := certFlags.Config()
	if err != nil {
		t.Fatal(err)
	}
	web := fibhttp.NewHandler(fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		scheme := "http"
		if c.Request.TLS != nil {
			scheme = "https"
		}
		reply := fmt.Sprintf("%s %s %s %s\n", c.Request.Proto, scheme, c.Request.Method, c.Request.URL.Path)
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(reply))
	}))
	grpcServer := grpc.NewServer()
	greeter.RegisterGreeterServer(grpcServer, greeterServer{})
	arpcServer := arpc.NewServer()
	arpcServer.Handler.Handle("/echo", func(ctx *arpc.Context) {
		var s string
		_ = ctx.Bind(&s)
		ctx.Write(s)
	})

	config := fib.DefaultConfig()
	engine, err := fib.New(config, fib.Binding{Addr: "127.0.0.1:0", Handler: multiproto.New(multiproto.Config{
		TLS:   fibtls.NewServer(fibhttp.ConfigureTLS(tlsConfig), web),
		HTTP1: web,
		HTTP2: web,
		WebSocket: websocket.NewHandler(websocket.HandlerFuncs{
			Message: func(c *websocket.Connection, opcode websocket.Opcode, data []byte) {
				_ = c.WriteMessage(opcode, data)
			},
		}),
		GRPC:      grpcServer,
		SOCKS:     socks.NewServer(socks.Config{}),
		HTTPProxy: fibhttp.NewHandler(fibhttp.NewConnectProxy(fibhttp.ConnectProxyConfig{})),
		ARPC:      arpcServer,
		Fallback:  fib.HandlerFuncs{Data: func(c *fib.Connection, data []byte) { _ = c.Send(data) }},
	})})
	if err != nil {
		t.Fatal(err)
	}
	local, err := engine.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- engine.Run() }()
	t.Cleanup(func() {
		engine.Stop()
		<-done
		_ = engine.Close()
	})
	return local.String(), certFile
}

func TestRunSpeaksEveryProtocol(t *testing.T) {
	addr, certFile := serve(t)
	var out bytes.Buffer
	if err := run([]string{"-addr", addr, "-ca", certFile}, &out); err != nil {
		t.Fatalf("%v: %s", err, out.String())
	}
	want := `HTTP/1.1      HTTP/1.1 http GET /http1
h2c           HTTP/2.0 http GET /h2c
HTTPS         HTTP/2.0 https GET /https
WebSocket     hello from fib #1
gRPC          hello, fib
SOCKS         HTTP/1.1 http GET /via-socks
HTTP CONNECT  HTTP/2.0 https GET /via-connect
arpc          hello from fib #1
TCP echo      hello from fib #1
`
	if out.String() != want {
		t.Fatalf("output\n%s\nwant\n%s", out.String(), want)
	}
}

func TestRunFailures(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closed := l.Addr().String()
	_ = l.Close()
	for _, args := range [][]string{
		{"-nope"},
		{"-ca", filepath.Join(t.TempDir(), "missing.pem")},
		{"-addr", closed, "-insecure"},
	} {
		if err := run(args, io.Discard); err == nil {
			t.Errorf("%q ran", args)
		}
	}
}

func TestMainReportsAnError(t *testing.T) {
	var mu sync.Mutex
	var got error
	oldFatal, oldArgs := fatal, os.Args
	defer func() { fatal, os.Args = oldFatal, oldArgs }()
	fatal = func(err error) { mu.Lock(); got = err; mu.Unlock() }
	os.Args = []string{"client", "-nope"}
	main()
	mu.Lock()
	defer mu.Unlock()
	if got == nil {
		t.Fatal("fatal was not called")
	}
}
