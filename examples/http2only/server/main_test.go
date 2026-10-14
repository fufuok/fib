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

// The server answers HTTP/2 in the clear (h2c, with prior knowledge) and,
// with -tls, over TLS, which is how CI's h2spec job runs it.
func TestRunServesHTTP2(t *testing.T) {
	for _, useTLS := range []bool{false, true} {
		name := map[bool]string{false: "h2c", true: "h2"}[useTLS]
		t.Run(name, func(t *testing.T) {
			certOut := filepath.Join(t.TempDir(), "cert.pem")
			args := []string{"-addr", "127.0.0.1:0"}
			if useTLS {
				args = append(args, "-tls", "-cert-out", certOut)
			}
			ctx, cancel := context.WithCancel(context.Background())
			var out bytes.Buffer
			addrs := make(chan string, 1)
			done := make(chan error, 1)
			go func() { done <- run(ctx, args, &out, func(addr string) { addrs <- addr }) }()
			addr := <-addrs

			transport := &http.Transport{Protocols: new(http.Protocols)}
			scheme := "http"
			if useTLS {
				scheme = "https"
				pem, err := os.ReadFile(certOut)
				if err != nil {
					t.Fatal(err)
				}
				roots := x509.NewCertPool()
				roots.AppendCertsFromPEM(pem)
				transport.TLSClientConfig = &tls.Config{RootCAs: roots}
				transport.Protocols.SetHTTP2(true)
			} else {
				transport.Protocols.SetUnencryptedHTTP2(true)
			}
			defer transport.CloseIdleConnections()
			client := &http.Client{Transport: transport, Timeout: 30 * time.Second}
			resp, err := client.Post(scheme+"://"+addr+"/echo", "text/plain", strings.NewReader("hi"))
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 200 || string(body) != "HTTP/2.0 POST /echo hi" {
				t.Fatalf("%d %q", resp.StatusCode, body)
			}
			cancel()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(out.String(), "HTTP/2-only echo server listening on "+scheme+"://"+addr) {
				t.Fatalf("output %q", out.String())
			}
		})
	}
}

func TestRunErrors(t *testing.T) {
	ctx := context.Background()
	if err := run(ctx, []string{"-nope"}, io.Discard, nil); err == nil {
		t.Error("accepted an unknown flag")
	}
	if err := run(ctx, []string{"-tls", "-cert", "missing.pem", "-key", "missing.key"}, io.Discard, nil); err == nil {
		t.Error("started with a certificate that is not there")
	}
	if err := run(ctx, []string{"-addr", "not an address"}, io.Discard, nil); err == nil {
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
