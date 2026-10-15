//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib/arpc"
	"github.com/lesismal/fib/examples/example"
	"github.com/lesismal/fib/examples/grpc/greeter"
	"github.com/lesismal/fib/grpc"
	"github.com/lesismal/fib/grpc/codes"
	"github.com/lesismal/fib/grpc/status"
	"github.com/lesismal/fib/websocket"
)

// get fetches rawURL with transport and returns the body.
func get(t *testing.T, transport *http.Transport, rawURL string) string {
	t.Helper()
	defer transport.CloseIdleConnections()
	resp, err := (&http.Client{Transport: transport, Timeout: 10 * time.Second}).Get(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

// Every protocol is answered on the one port, each by a real client of its
// own.
func TestRunServesEveryProtocolOnOnePort(t *testing.T) {
	certOut := filepath.Join(t.TempDir(), "cert.pem")
	ctx, cancel := context.WithCancel(context.Background())
	var out bytes.Buffer
	started := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, []string{"-addr", "127.0.0.1:0", "-cert-out", certOut}, &out, func(addr string) { started <- addr })
	}()
	var addr string
	select {
	case addr = <-started:
	case err := <-done:
		t.Fatalf("did not start: %v", err)
	}
	pem, err := os.ReadFile(certOut)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(pem)
	engine, stopEngine := example.ClientEngine()
	defer stopEngine()

	t.Run("HTTP1", func(t *testing.T) {
		if got := get(t, &http.Transport{}, "http://"+addr+"/plain"); got != "HTTP/1.1 http GET /plain\n" {
			t.Fatalf("HTTP/1.1 answered %q", got)
		}
	})

	t.Run("HTTP2", func(t *testing.T) {
		transport := &http.Transport{Protocols: new(http.Protocols)}
		transport.Protocols.SetUnencryptedHTTP2(true)
		if got := get(t, transport, "http://"+addr+"/h2c"); got != "HTTP/2.0 http GET /h2c\n" {
			t.Fatalf("h2c answered %q", got)
		}
	})

	t.Run("TLS", func(t *testing.T) {
		transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots}, ForceAttemptHTTP2: true}
		if got := get(t, transport, "https://"+addr+"/secure"); got != "HTTP/2.0 https GET /secure\n" {
			t.Fatalf("HTTPS answered %q", got)
		}
		transport = &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, NextProtos: []string{"http/1.1"}}}
		if got := get(t, transport, "https://"+addr+"/secure"); got != "HTTP/1.1 https GET /secure\n" {
			t.Fatalf("HTTPS with HTTP/1.1 answered %q", got)
		}
	})

	t.Run("WebSocket", func(t *testing.T) {
		messages := make(chan string, 1)
		conn, _, err := websocket.NewDialer(engine, websocket.DefaultDialerConfig()).Go("ws://"+addr+"/ws", nil,
			websocket.HandlerFuncs{Message: func(_ *websocket.Connection, _ websocket.Opcode, data []byte) {
				messages <- string(data)
			}}).Wait()
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close(websocket.CloseNormal, "done")
		if err := conn.WriteText("hi"); err != nil {
			t.Fatal(err)
		}
		select {
		case got := <-messages:
			if got != "hi" {
				t.Fatalf("WebSocket echoed %q", got)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("no WebSocket echo")
		}
	})

	t.Run("GRPC", func(t *testing.T) {
		cc, err := grpc.NewClient(addr, grpc.WithEngine(engine))
		if err != nil {
			t.Fatal(err)
		}
		defer cc.Close()
		client := greeter.NewGreeterClient(cc)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		reply, err := client.SayHello(ctx, &greeter.HelloRequest{Name: "fib"})
		if err != nil || reply.Message != "hello, fib" {
			t.Fatalf("SayHello answered %+v (%v)", reply, err)
		}
		if _, err := client.SayHello(ctx, &greeter.HelloRequest{}); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("SayHello without a name: %v", err)
		}
		stream, err := client.SayHellos(ctx, &greeter.HelloRequest{Name: "fib"})
		if err != nil {
			t.Fatal(err)
		}
		var replies []string
		for {
			reply, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			replies = append(replies, reply.Message)
		}
		if len(replies) != 3 || replies[2] != "hello #3, fib" {
			t.Fatalf("SayHellos answered %q", replies)
		}
	})

	// A SOCKS5 CONNECT back to the same port, where the tunnel's HTTP/1.1
	// is served in turn.
	t.Run("SOCKS", func(t *testing.T) {
		transport := &http.Transport{Proxy: http.ProxyURL(&url.URL{Scheme: "socks5", Host: addr})}
		if got := get(t, transport, "http://"+addr+"/via-socks"); got != "HTTP/1.1 http GET /via-socks\n" {
			t.Fatalf("through SOCKS: %q", got)
		}
	})

	// An HTTP CONNECT back to the same port, where the tunnel's TLS is
	// served in turn.
	t.Run("HTTPProxy", func(t *testing.T) {
		transport := &http.Transport{Proxy: http.ProxyURL(&url.URL{Scheme: "http", Host: addr}),
			TLSClientConfig: &tls.Config{RootCAs: roots}, ForceAttemptHTTP2: true}
		if got := get(t, transport, "https://"+addr+"/via-connect"); got != "HTTP/2.0 https GET /via-connect\n" {
			t.Fatalf("through CONNECT: %q", got)
		}
	})

	t.Run("ARPC", func(t *testing.T) {
		client, err := arpc.Dial(engine, "tcp", addr, 3*time.Second, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Stop()
		var echo string
		if err := client.Call("/echo", "hello", &echo, 5*time.Second); err != nil || echo != "hello" {
			t.Fatalf("/echo answered %q (%v)", echo, err)
		}
		var sum SumRsp
		if err := client.Call("Math.Sum", &SumReq{A: 2, B: 3}, &sum, 5*time.Second); err != nil || sum.Sum != 5 {
			t.Fatalf("Math.Sum answered %d (%v)", sum.Sum, err)
		}
		stream := client.NewStream("/upper")
		if err := stream.Send("fib"); err != nil {
			t.Fatal(err)
		}
		stream.CloseSend()
		var word string
		if err := stream.Recv(&word); err != nil || word != "FIB" {
			t.Fatalf("/upper answered %q (%v)", word, err)
		}
	})

	t.Run("Fallback", func(t *testing.T) {
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
		if _, err := conn.Write([]byte("hello from fib")); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, len("hello from fib"))
		if _, err := io.ReadFull(conn, buf); err != nil || string(buf) != "hello from fib" {
			t.Fatalf("TCP echo: %q %v", buf, err)
		}
	})

	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "listening on "+addr) {
		t.Fatalf("output %q", out.String())
	}
}

func TestRunErrors(t *testing.T) {
	ctx := context.Background()
	certOut := filepath.Join(t.TempDir(), "cert.pem")
	if err := run(ctx, []string{"-nope"}, io.Discard, nil); err == nil {
		t.Error("accepted an unknown flag")
	}
	if err := run(ctx, []string{"-cert", "missing.pem", "-key", "missing.key"}, io.Discard, nil); err == nil {
		t.Error("started with a certificate that is not there")
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err := run(ctx, []string{"-addr", l.Addr().String(), "-cert-out", certOut}, io.Discard, nil); err == nil {
		t.Error("started on a port that is taken")
	}
}

func TestMainReportsAStartupError(t *testing.T) {
	var mu sync.Mutex
	var got error
	oldFatal, oldArgs := fatal, os.Args
	defer func() { fatal, os.Args = oldFatal, oldArgs }()
	fatal = func(err error) { mu.Lock(); got = err; mu.Unlock() }
	os.Args = []string{"server", "-nope"}
	main()
	mu.Lock()
	defer mu.Unlock()
	if got == nil {
		t.Fatal("fatal was not called")
	}
}
