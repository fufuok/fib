//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// The server issues itself a certificate, writes it where -cert-out says, and
// echoes over TLS to a client that trusts that certificate.
func TestRunEchoesOverTLS(t *testing.T) {
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
	conn, err := tls.Dial("tcp", addr, &tls.Config{RootCAs: roots, ServerName: "localhost"})
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	msg := []byte("hello tls echo")
	if _, err := conn.Write(msg); err != nil {
		t.Fatal(err)
	}
	echo := make([]byte, len(msg))
	if _, err := io.ReadFull(conn, echo); err != nil || !bytes.Equal(echo, msg) {
		t.Fatalf("echo %q, %v", echo, err)
	}
	_ = conn.Close()
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "TCP+TLS echo server listening on "+addr) {
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
