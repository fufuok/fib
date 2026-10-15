//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

// Command server serves eight protocols and a raw TCP echo on one port with
// package multiproto, which reads the first bytes each client sends and
// hands the connection to the handler of the protocol it found: HTTPS (with
// HTTP/1.1 and HTTP/2 by ALPN), HTTP/1.1, h2c, WebSocket, gRPC, SOCKS5/4/4a,
// an HTTP CONNECT proxy and arpc. What is none of them is echoed back.
//
//	go run ./examples/multiproto/server
//
// Every protocol answers on 127.0.0.1:9000:
//
//	curl http://127.0.0.1:9000/hello
//	curl --http2-prior-knowledge http://127.0.0.1:9000/hello
//	curl --cacert "${TMPDIR:-/tmp}/fib-example-cert.pem" https://localhost:9000/hello
//	curl --socks5-hostname 127.0.0.1:9000 http://127.0.0.1:9000/via-socks
//	curl --proxy http://127.0.0.1:9000 --cacert "${TMPDIR:-/tmp}/fib-example-cert.pem" https://localhost:9000/via-connect
//	go run ./examples/websocket/nontls/client -url ws://127.0.0.1:9000/ws
//	go run ./examples/grpc/client -addr 127.0.0.1:9000
//	go run ./examples/arpc/client -addr 127.0.0.1:9000
//	go run ./examples/tcp/nontls/client -addr 127.0.0.1:9000
//
// Without -cert and -key it issues itself a certificate for localhost and
// writes it where the TLS clients look for it, which -cert-out changes.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	stdhttp "net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/arpc"
	"github.com/lesismal/fib/examples/certs"
	"github.com/lesismal/fib/examples/example"
	"github.com/lesismal/fib/examples/grpc/greeter"
	"github.com/lesismal/fib/grpc"
	"github.com/lesismal/fib/grpc/codes"
	"github.com/lesismal/fib/grpc/status"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/multiproto"
	"github.com/lesismal/fib/socks"
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

// run serves until ctx ends. ready, if not nil, is told the address once the
// server is listening.
func run(ctx context.Context, args []string, out io.Writer, ready func(addr string)) error {
	flags := flag.NewFlagSet("server", flag.ContinueOnError)
	flags.SetOutput(out)
	addr := flags.String("addr", "127.0.0.1:9000", "the one address every protocol is served on")
	certFlags := certs.ServerFlagsOn(flags)
	if err := flags.Parse(args); err != nil {
		return err
	}

	tlsConfig, err := certFlags.Config()
	if err != nil {
		return err
	}
	arpcServer, err := arpcEcho()
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()
	greeter.RegisterGreeterServer(grpcServer, greeterServer{})

	// One handler serves HTTP/1.1, h2c and, behind TLS, HTTPS.
	web := fibhttp.NewHandler(describe())
	config := fib.DefaultConfig()
	config.Addr = *addr
	engine, err := fib.Bind(config, multiproto.New(multiproto.Config{
		TLS:       fibtls.NewServer(fibhttp.ConfigureTLS(tlsConfig), web),
		HTTP1:     web,
		HTTP2:     web,
		WebSocket: wsEcho(),
		GRPC:      grpcServer,
		SOCKS:     socks.NewServer(socks.Config{}),
		HTTPProxy: fibhttp.NewHandler(fibhttp.NewConnectProxy(fibhttp.ConnectProxyConfig{})),
		ARPC:      arpcServer,
		// The echo takes whatever none of the protocols above recognises.
		Fallback: tcpEcho(),
	}))
	if err != nil {
		return err
	}
	return example.Serve(ctx, out, ready,
		"HTTPS, HTTP/1.1, h2c, WebSocket, gRPC, SOCKS, HTTP CONNECT, arpc and a TCP echo listening on %s", engine)
}

// describe answers each request with its protocol, whether it came over TLS,
// its method and its path.
func describe() fibhttp.HandlerFunc {
	return func(c *fibhttp.Context) {
		scheme := "http"
		if c.Request.TLS != nil {
			scheme = "https"
		}
		reply := fmt.Sprintf("%s %s %s %s\n", c.Request.Proto, scheme, c.Request.Method, c.Request.URL.Path)
		header := stdhttp.Header{"Content-Type": {"text/plain; charset=utf-8"}}
		if err := c.WriteResponse(fibhttp.Response{StatusCode: stdhttp.StatusOK, Header: header, Body: []byte(reply)}); err != nil {
			c.Conn.Close()
		}
	}
}

// greeterServer is the service examples/grpc/client calls.
type greeterServer struct{}

func (greeterServer) SayHello(_ context.Context, in *greeter.HelloRequest) (*greeter.HelloReply, error) {
	if in.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	return &greeter.HelloReply{Message: "hello, " + in.Name}, nil
}

func (greeterServer) SayHellos(in *greeter.HelloRequest, stream grpc.ServerStreamingServer[greeter.HelloReply]) error {
	for i := 1; i <= 3; i++ {
		if err := stream.Send(&greeter.HelloReply{Message: fmt.Sprintf("hello #%d, %s", i, in.Name)}); err != nil {
			return err
		}
	}
	return nil
}

type SumReq struct{ A, B int }
type SumRsp struct{ Sum int }

// Math is registered as "Math.Sum", which examples/arpc/client calls.
type Math struct{}

func (Math) Sum(_ context.Context, req *SumReq, rsp *SumRsp) { rsp.Sum = req.A + req.B }

// arpcEcho serves what examples/arpc/client calls: an echo, Math.Sum, and a
// stream that answers each message in upper case.
func arpcEcho() (*arpc.Server, error) {
	server := arpc.NewServer()
	server.Handler.Handle("/echo", func(ctx *arpc.Context) {
		var s string
		if err := ctx.Bind(&s); err != nil {
			ctx.Error(err)
			return
		}
		ctx.Write(s)
	}, false)
	if err := server.Handler.Register("Math", Math{}); err != nil {
		return nil, err
	}
	server.Handler.HandleStream("/upper", func(s *arpc.Stream) {
		defer s.CloseSend()
		for {
			var msg string
			// io.EOF ends the stream once the client has sent its last.
			if err := s.Recv(&msg); err != nil {
				return
			}
			if err := s.Send(strings.ToUpper(msg)); err != nil {
				return
			}
		}
	})
	return server, nil
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
