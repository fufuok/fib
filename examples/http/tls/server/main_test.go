//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// The server speaks HTTP/2 to a client that offers it and HTTP/1.1 to one
// that does not, verified by the certificate it issued itself.
func TestRunEchoesOverHTTP2AndHTTP1(t *testing.T) {
	certOut := filepath.Join(t.TempDir(), "cert.pem")
	ctx, cancel := context.WithCancel(context.Background())
	var out bytes.Buffer
	addrs := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, []string{"-addr", "127.0.0.1:0", "-cert-out", certOut}, &out, func(addr string) { addrs <- addr })
	}()
	addr := <-addrs
	pem, err := os.ReadFile(certOut)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(pem)
	for _, h2 := range []bool{true, false} {
		transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots}, ForceAttemptHTTP2: h2}
		if !h2 {
			transport.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
		}
		client := &http.Client{Transport: transport, Timeout: 30 * time.Second}
		resp, err := client.Post("https://"+addr+"/echo", "text/plain", strings.NewReader("hi"))
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		want := "HTTP/1.1 POST /echo hi"
		if h2 {
			want = "HTTP/2.0 POST /echo hi"
		}
		if resp.StatusCode != 200 || string(body) != want {
			t.Fatalf("%d %q, want %q", resp.StatusCode, body, want)
		}
		transport.CloseIdleConnections()
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "HTTPS echo server listening on https://"+addr) {
		t.Fatalf("output %q", out.String())
	}
}

func TestRunErrors(t *testing.T) {
	ctx := context.Background()
	if err := run(ctx, []string{"-nope"}, io.Discard, nil); err == nil {
		t.Error("accepted an unknown flag")
	}
	if err := run(ctx, []string{"-cert", "missing.pem", "-key", "missing.key"}, io.Discard, nil); err == nil {
		t.Error("started with a certificate that is not there")
	}
	if err := run(ctx, []string{"-addr", "not an address", "-cert-out", filepath.Join(t.TempDir(), "cert.pem")}, io.Discard, nil); err == nil {
		t.Error("started on a bad address")
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
