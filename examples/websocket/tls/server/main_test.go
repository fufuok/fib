//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/websocket"
)

// syncBuffer is an output the engine's goroutines and the test share.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// Over TLS, the server echoes text and binary messages, compressed when both ends
// offer it, and reports each connection's opening and closing.
func TestRunEchoes(t *testing.T) {
	for _, compress := range []bool{false, true} {
		t.Run(fmt.Sprint("compress=", compress), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			out := &syncBuffer{}
			addrs := make(chan string, 1)
			done := make(chan error, 1)
			certOut := filepath.Join(t.TempDir(), "cert.pem")
			args := []string{"-addr", "127.0.0.1:0", fmt.Sprint("-compress=", compress), "-cert-out", certOut}
			go func() { done <- run(ctx, args, out, func(addr string) { addrs <- addr }) }()
			addr := <-addrs

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
			config := websocket.DefaultDialerConfig()
			config.EnableCompression = compress
			// The client trusts the certificate the server issued itself.
			pem, err := os.ReadFile(certOut)
			if err != nil {
				t.Fatal(err)
			}
			roots := x509.NewCertPool()
			roots.AppendCertsFromPEM(pem)
			config.TLSConfig = &tls.Config{RootCAs: roots}
			messages := make(chan string, 4)
			closed := make(chan uint16, 1)
			conn, _, err := websocket.NewDialer(engine, config).Go("wss://"+addr+"/ws", nil, websocket.HandlerFuncs{
				Message: func(_ *websocket.Connection, _ websocket.Opcode, data []byte) { messages <- string(data) },
				Close:   func(_ *websocket.Connection, code uint16, _ string, _ error) { closed <- code },
			}).Wait()
			if err != nil {
				t.Fatal(err)
			}
			text := strings.Repeat("echo me ", 100)
			if err := conn.WriteText(text); err != nil {
				t.Fatal(err)
			}
			if err := conn.WriteBinary([]byte{1, 2, 3}); err != nil {
				t.Fatal(err)
			}
			for _, want := range []string{text, "\x01\x02\x03"} {
				select {
				case got := <-messages:
					if got != want {
						t.Fatalf("echo %q, want %q", got, want)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("no echo")
				}
			}
			_ = conn.Close(websocket.CloseNormal, "done")
			select {
			case <-closed:
			case <-time.After(10 * time.Second):
				t.Fatal("not closed")
			}
			deadline := time.Now().Add(10 * time.Second)
			for !strings.Contains(out.String(), "closed: code=1000") {
				if time.Now().After(deadline) {
					t.Fatalf("output %q", out.String())
				}
				time.Sleep(10 * time.Millisecond)
			}
			cancel()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if text := out.String(); !strings.Contains(text, "listening on wss://"+addr) || !strings.Contains(text, ": open /ws") {
				t.Fatalf("output %q", text)
			}
		})
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
