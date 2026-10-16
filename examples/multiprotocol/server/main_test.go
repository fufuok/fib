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
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/http3"
	"github.com/lesismal/fib/websocket"
)

// freePort finds a port that is free for both TCP and UDP, since HTTPS and
// HTTP/3 share one number.
func freePort(t *testing.T) string {
	t.Helper()
	for i := 0; i < 100; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := l.Addr().String()
		pc, err := net.ListenPacket("udp", addr)
		_ = l.Close()
		if err == nil {
			_ = pc.Close()
			return addr
		}
	}
	t.Fatal("no port free for both TCP and UDP")
	return ""
}

// One engine answers plain HTTP, HTTPS, HTTP/3, WebSocket and raw TCP, each
// on its own binding.
func TestRunServesEveryBinding(t *testing.T) {
	certOut := filepath.Join(t.TempDir(), "cert.pem")
	httpAddr, httpsAddr, wsAddr := freePort(t), freePort(t), freePort(t)
	tcpAddrs := []string{freePort(t), freePort(t)}
	sharedAddr := freePort(t)
	ctx, cancel := context.WithCancel(context.Background())
	var out bytes.Buffer
	started := make(chan string, 1)
	done := make(chan error, 1)
	args := []string{"-http", httpAddr, "-https", httpsAddr, "-ws", wsAddr, "-tcp", strings.Join(tcpAddrs, ","),
		"-shared", sharedAddr, "-cert-out", certOut}
	go func() { done <- run(ctx, args, &out, func(addr string) { started <- addr }) }()
	select {
	case addr := <-started:
		if addr != httpAddr {
			t.Fatalf("ready with %s, want the HTTP address %s", addr, httpAddr)
		}
	case err := <-done:
		t.Fatalf("did not start: %v", err)
	}
	pem, err := os.ReadFile(certOut)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(pem)

	// Plain HTTP, on the first binding.
	resp, err := http.Post("http://"+httpAddr+"/plain", "text/plain", strings.NewReader("hi"))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "HTTP/1.1 POST /plain hi" || resp.Header.Get("Alt-Svc") != "" {
		t.Fatalf("HTTP: %q %v", body, resp.Header)
	}

	// HTTPS, negotiating HTTP/2, which points to HTTP/3.
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots}, ForceAttemptHTTP2: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 30 * time.Second}
	resp, err = client.Post("https://"+httpsAddr+"/secure", "text/plain", strings.NewReader("hi"))
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	_, port, _ := net.SplitHostPort(httpsAddr)
	if string(body) != "HTTP/2.0 POST /secure hi" || !strings.Contains(resp.Header.Get("Alt-Svc"), port) {
		t.Fatalf("HTTPS: %q %v", body, resp.Header)
	}

	engine, err := fib.New(fib.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	engineDone := make(chan error, 1)
	go func() { engineDone <- engine.Run() }()
	defer func() {
		engine.Stop()
		<-engineDone
		_ = engine.Close()
	}()

	// HTTP/3, on the UDP port of the HTTPS port's number.
	config := http3.DefaultClientConfig()
	config.TLSConfig = &tls.Config{RootCAs: roots}
	h3 := http3.NewClient(engine, config)
	defer h3.Close()
	req, _ := http.NewRequest("POST", "https://"+httpsAddr+"/quic", strings.NewReader("hi"))
	resp, err = h3.Go(req).Wait()
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	if string(body) != "HTTP/3.0 POST /quic hi" {
		t.Fatalf("HTTP/3: %q", body)
	}

	// WebSocket.
	messages := make(chan string, 1)
	conn, _, err := websocket.NewDialer(engine, websocket.DefaultDialerConfig()).Go("ws://"+wsAddr+"/ws", nil,
		websocket.HandlerFuncs{Message: func(_ *websocket.Connection, _ websocket.Opcode, data []byte) {
			messages <- string(data)
		}}).Wait()
	if err != nil {
		t.Fatal(err)
	}
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
	_ = conn.Close(websocket.CloseNormal, "done")

	// The shared port tells HTTPS, plain HTTP and the echo apart.
	resp, err = client.Get("https://" + sharedAddr + "/shared")
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "HTTP/2.0 GET /shared " {
		t.Fatalf("HTTPS on the shared port: %q", body)
	}
	resp, err = http.Get("http://" + sharedAddr + "/shared")
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "HTTP/1.1 GET /shared " {
		t.Fatalf("HTTP on the shared port: %q", body)
	}

	// Raw TCP, on every address of its group and on the shared port.
	for _, addr := range append(tcpAddrs, sharedAddr) {
		c, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		_ = c.SetDeadline(time.Now().Add(5 * time.Second))
		if _, err := c.Write([]byte("hi")); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 2)
		if _, err := io.ReadFull(c, buf); err != nil || string(buf) != "hi" {
			t.Fatalf("TCP echo at %s: %q %v", addr, buf, err)
		}
		_ = c.Close()
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "multi-protocol server listening on http://"+httpAddr) {
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
	if err := run(ctx, []string{"-https", "not an address", "-cert-out", certOut}, io.Discard, nil); err == nil {
		t.Error("started on a bad address")
	}
	// Every binding is opened or none is: one address in use fails them all.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	args := []string{"-http", freePort(t), "-https", freePort(t), "-ws", freePort(t), "-tcp", l.Addr().String(),
		"-shared", freePort(t), "-cert-out", certOut}
	if err := run(ctx, args, io.Discard, nil); err == nil {
		t.Error("started with a TCP echo port taken")
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
