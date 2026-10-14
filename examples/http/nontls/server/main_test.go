//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// startRun runs the server with args on a free port until the test ends, and
// returns its address and what it printed.
func startRun(t *testing.T, args ...string) (string, *bytes.Buffer) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	out := &bytes.Buffer{}
	addrs := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, append([]string{"-addr", "127.0.0.1:0"}, args...), out, func(addr string) { addrs <- addr })
	}()
	addr := <-addrs
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	return addr, out
}

func get(t *testing.T, method, url, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	text, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(text)
}

// The server answers with the request's method, path and body.
func TestRunEchoes(t *testing.T) {
	addr, out := startRun(t)
	if code, text := get(t, "POST", "http://"+addr+"/echo", "hi"); code != 200 || text != "POST /echo hi" {
		t.Fatalf("%d %q", code, text)
	}
	if !strings.Contains(out.String(), "HTTP echo server listening on http://"+addr) {
		t.Fatalf("output %q", out.String())
	}
}

// With -dir, /files/ serves that directory, and every other path is still
// echoed.
func TestRunServesFiles(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("file contents"), 0o644); err != nil {
		t.Fatal(err)
	}
	addr, _ := startRun(t, "-dir", dir)
	if code, text := get(t, "GET", "http://"+addr+"/files/a.txt", ""); code != 200 || text != "file contents" {
		t.Fatalf("file: %d %q", code, text)
	}
	if code, _ := get(t, "GET", "http://"+addr+"/files/missing.txt", ""); code != 404 {
		t.Fatalf("missing file: %d", code)
	}
	if code, text := get(t, "GET", "http://"+addr+"/other", ""); code != 200 || text != "GET /other " {
		t.Fatalf("echo: %d %q", code, text)
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
