//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"io"
	stdhttp "net/http"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
)

// TestCloseLetsGoOfAnIdleHTTP2Connection checks that an engine closed while
// an HTTP/2 connection sits idle on it can be collected. The connection's
// idle timer, set for IdleTimeout once its last stream ended, holds the
// connection, and through it the engine, its pools and their queues; Close
// used to cut the connection without its OnClose, which stops the timer, so
// a closed engine stayed in memory until the timer fired, two minutes by
// default. A process that starts and closes servers, as tests do, piled up a
// megabyte or more for each until it ran short of memory.
func TestCloseLetsGoOfAnIdleHTTP2Connection(t *testing.T) {
	var collected atomic.Bool
	func() {
		config := fib.DefaultConfig()
		engine, err := fib.New(config, fib.Binding{Addr: "127.0.0.1:0", Handler: NewHandler(HandlerFunc(func(c *Context) {
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("ok"))
		}))})
		if err != nil {
			t.Fatal(err)
		}
		addr, err := engine.LocalAddr()
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- engine.Run() }()

		var protocols stdhttp.Protocols
		protocols.SetUnencryptedHTTP2(true)
		transport := &stdhttp.Transport{Protocols: &protocols}
		client := &stdhttp.Client{Timeout: 5 * time.Second, Transport: transport}
		resp, err := client.Get("http://" + addr.String() + "/")
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.ProtoMajor != 2 {
			t.Fatalf("served over %s, want HTTP/2", resp.Proto)
		}

		// The connection is idle, and stays open: the client keeps it.
		engine.Stop()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if err := engine.Close(); err != nil {
			t.Fatal(err)
		}
		transport.CloseIdleConnections()
		runtime.AddCleanup(engine, func(b *atomic.Bool) { b.Store(true) }, &collected)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for !collected.Load() && time.Now().Before(deadline) {
		runtime.GC()
		time.Sleep(20 * time.Millisecond)
	}
	if !collected.Load() {
		t.Fatal("the closed engine is still reachable: something it served holds on to it")
	}
}
