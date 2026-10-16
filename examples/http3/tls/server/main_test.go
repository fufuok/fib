//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
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
)

// freePort finds a port that is free for both TCP and UDP, since HTTPS and
// HTTP/3 share one number.
func freePort(t *testing.T) int {
	t.Helper()
	for i := 0; i < 100; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := l.Addr().(*net.TCPAddr).Port
		pc, err := net.ListenPacket("udp", fmt.Sprintf("127.0.0.1:%d", port))
		_ = l.Close()
		if err == nil {
			_ = pc.Close()
			return port
		}
	}
	t.Fatal("no port free for both TCP and UDP")
	return 0
}

// The server answers HTTP/3 on UDP and HTTPS on the TCP port of the same
// number, where its responses point to HTTP/3 with Alt-Svc.
func TestRunServesHTTP3AndHTTPS(t *testing.T) {
	certOut := filepath.Join(t.TempDir(), "cert.pem")
	ctx, cancel := context.WithCancel(context.Background())
	var out bytes.Buffer
	addrs := make(chan string, 1)
	done := make(chan error, 1)
	port := freePort(t)
	args := []string{"-addr", fmt.Sprintf("127.0.0.1:%d", port), "-cert-out", certOut}
	go func() { done <- run(ctx, args, &out, func(addr string) { addrs <- addr }) }()
	var addr string
	select {
	case addr = <-addrs:
	case err := <-done:
		t.Fatalf("did not start: %v", err)
	}
	pem, err := os.ReadFile(certOut)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(pem)

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
	config := http3.DefaultClientConfig()
	config.TLSConfig = &tls.Config{RootCAs: roots}
	h3 := http3.NewClient(engine, config)
	defer h3.Close()
	req, _ := http.NewRequest("POST", "https://"+addr+"/echo", strings.NewReader("hi"))
	resp, err := h3.Go(req).Wait()
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || string(body) != "HTTP/3.0 POST /echo hi" || resp.Header.Get("Alt-Svc") != "" {
		t.Fatalf("HTTP/3: %d %q %v", resp.StatusCode, body, resp.Header)
	}

	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 30 * time.Second}
	resp, err = client.Post("https://"+addr+"/echo", "text/plain", strings.NewReader("hi"))
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !strings.HasSuffix(string(body), " POST /echo hi") || !strings.Contains(resp.Header.Get("Alt-Svc"), fmt.Sprint(port)) {
		t.Fatalf("HTTPS: %d %q %v", resp.StatusCode, body, resp.Header)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "HTTP/3 echo server listening on https://"+addr) {
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
	if err := run(ctx, []string{"-addr", "not an address", "-cert-out", certOut}, io.Discard, nil); err == nil {
		t.Error("started on a bad address")
	}
	if err := run(ctx, []string{"-addr", "127.0.0.1:99999", "-cert-out", certOut}, io.Discard, nil); err == nil {
		t.Error("started on a port that does not exist")
	}
	// The UDP port is free and the TCP one is not.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err := run(ctx, []string{"-addr", l.Addr().String(), "-cert-out", certOut}, io.Discard, nil); err == nil {
		t.Error("started with its TCP port taken")
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
