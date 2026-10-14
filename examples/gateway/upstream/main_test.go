//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	stdhttp "net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// The upstream serves HTTPS on the address it is given and cleartext on the
// other, and stops when its context ends.
func TestRunServesUntilCancelled(t *testing.T) {
	certOut := filepath.Join(t.TempDir(), "cert.pem")
	ctx, cancel := context.WithCancel(context.Background())
	var out bytes.Buffer
	addrs := make(chan string, 1)
	done := make(chan error, 1)
	args := []string{"-addr", "127.0.0.1:0", "-plain-addr", "127.0.0.1:0", "-cert-out", certOut}
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
	transport := &stdhttp.Transport{TLSClientConfig: &tls.Config{RootCAs: roots}}
	defer transport.CloseIdleConnections()
	resp, err := (&stdhttp.Client{Transport: transport, Timeout: 30 * time.Second}).Get("https://" + addr + "/status?code=418")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 418 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if text := out.String(); !strings.Contains(text, "upstream listening on https://"+addr) || !strings.Contains(text, " and http://127.0.0.1:") {
		t.Fatalf("output %q", text)
	}

	// Without a cleartext listener, and nothing to serve.
	ctx2, cancel2 := context.WithCancel(context.Background())
	cancel2()
	out.Reset()
	if err := run(ctx2, []string{"-addr", "127.0.0.1:0", "-plain-addr", "", "-cert-out", certOut}, &out, nil); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "http://") {
		t.Fatalf("a cleartext listener announced: %q", out.String())
	}
}

func TestRunStartupErrors(t *testing.T) {
	ctx := context.Background()
	certOut := filepath.Join(t.TempDir(), "cert.pem")
	for _, args := range [][]string{
		{"-nope"},
		{"-cert", "missing.pem", "-key", "missing.key"},
		{"-cert-out", certOut, "-addr", "not an address"},
	} {
		if err := run(ctx, args, io.Discard, nil); err == nil {
			t.Errorf("%q: started", args)
		}
	}
}

func TestMainReportsAStartupError(t *testing.T) {
	var mu sync.Mutex
	var got error
	oldFatal, oldArgs := fatal, os.Args
	defer func() { fatal, os.Args = oldFatal, oldArgs }()
	fatal = func(err error) { mu.Lock(); got = err; mu.Unlock() }
	os.Args = []string{"upstream", "-nope"}
	main()
	mu.Lock()
	defer mu.Unlock()
	if got == nil {
		t.Fatal("fatal was not called")
	}
}
