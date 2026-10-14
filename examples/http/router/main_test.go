//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
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

// Each of the routes the doc comment lists answers as it says, and the logger
// writes a line for each request.
func TestRunRoutes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	out := &syncBuffer{}
	addrs := make(chan string, 1)
	done := make(chan error, 1)
	go func() { done <- run(ctx, []string{"-addr", "127.0.0.1:0"}, out, func(addr string) { addrs <- addr }) }()
	addr := <-addrs
	client := &http.Client{Timeout: 30 * time.Second}
	for _, c := range []struct {
		method, path string
		code         int
		body         string
		header       string
	}{
		{"GET", "/", 200, "index\n", ""},
		{"GET", "/users/", 200, "all users\n", "X-Request-Id"},
		{"GET", "/users/42", 200, "user 42\n", "X-Request-Id"},
		{"DELETE", "/users/42", 200, "deleted user 42\n", ""},
		{"GET", "/users/bob", 200, "user named bob\n", ""},
		{"POST", "/users/42", 405, "", "Allow"},
		{"GET", "/api/v1/files/a/b.txt", 200, "file \"a/b.txt\" of API v1, routed by /api/{version}/files/*\n", ""},
		{"GET", "/nowhere", 404, "", ""},
	} {
		req, _ := http.NewRequest(c.method, "http://"+addr+c.path, nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != c.code || c.body != "" && string(body) != c.body || c.header != "" && resp.Header.Get(c.header) == "" {
			t.Errorf("%s %s: %d %q %v", c.method, c.path, resp.StatusCode, body, resp.Header)
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if text := out.String(); !strings.Contains(text, "HTTP router listening on http://"+addr) || !strings.Contains(text, "/users/42") {
		t.Fatalf("output %q", text)
	}
}

func TestRunErrors(t *testing.T) {
	ctx := context.Background()
	if err := run(ctx, []string{"-nope"}, io.Discard, nil); err == nil {
		t.Error("accepted an unknown flag")
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
