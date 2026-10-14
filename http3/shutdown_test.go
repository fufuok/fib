//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"context"
	"encoding/binary"
	"fmt"
	stdhttp "net/http"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3/qpack"
	"github.com/lesismal/fib/tlstest"
	"github.com/lesismal/fib/websocket"
)

// shutdownServer serves handler over HTTP/3 on an engine the test shuts
// down, and returns it, its base URL, and what its Run returns.
func shutdownServer(t *testing.T, handler fibhttp.HandlerFunc) (*fib.Engine, string, chan error) {
	t.Helper()
	serverTLS, _, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	config := DefaultConfig()
	config.TLSConfig = serverTLS
	fc := fib.DefaultConfig()
	fc.Network, fc.Addr = "udp", "127.0.0.1:0"
	engine, err := fib.Bind(fc, NewHandlerWithConfig(config, handler))
	if err != nil {
		t.Fatal(err)
	}
	addr, err := engine.LocalUDPAddr()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- engine.Run() }()
	t.Cleanup(func() {
		engine.Stop()
		<-done
		_ = engine.Close()
	})
	return engine, fmt.Sprintf("https://localhost:%d", addr.Port), done
}

func startShutdown(engine *fib.Engine) <-chan error {
	shut := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		shut <- engine.Shutdown(ctx)
	}()
	return shut
}

func awaitShutdown(t *testing.T, done chan error, shut <-chan error) {
	t.Helper()
	select {
	case err := <-shut:
		if err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Shutdown did not return")
	}
	if err := <-done; err != nil {
		t.Errorf("Run: %v", err)
	}
	done <- nil
}

// TestShutdownHTTP3 checks that a request in flight when the engine shuts
// down is answered, and that Shutdown then returns, its connection, and the
// client address it came from, having closed rather than waited out their
// idle timeouts.
func TestShutdownHTTP3(t *testing.T) {
	entered, release := make(chan struct{}, 1), make(chan struct{})
	engine, base, done := shutdownServer(t, func(c *fibhttp.Context) {
		if c.Request.URL.Path == "/slow" {
			entered <- struct{}{}
			<-release
		}
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("ok"))
	})
	client := newClient(t, nil)
	resp, err := client.Go(mustRequest(t, stdhttp.MethodGet, base+"/fast", nil)).Wait()
	if err != nil || readBody(t, resp) != "ok" {
		t.Fatalf("before Shutdown: %v", err)
	}
	slow := make(chan error, 1)
	go func() {
		resp, err := client.Go(mustRequest(t, stdhttp.MethodGet, base+"/slow", nil)).Wait()
		if err == nil && (resp.StatusCode != stdhttp.StatusOK || readBody(t, resp) != "ok") {
			err = fmt.Errorf("answered %s", resp.Status)
		}
		slow <- err
	}()
	<-entered

	shut := startShutdown(engine)
	for !engine.ShuttingDown() {
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-shut:
		t.Fatalf("Shutdown returned %v with a request in flight", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-slow; err != nil {
		t.Fatalf("request in flight: %v", err)
	}
	awaitShutdown(t, done, shut)
}

// TestShutdownHTTP3WebSocket checks that a WebSocket on an HTTP/3 stream is
// sent a close frame with 1001 as the engine shuts down, and that once the
// client ends its side, the connection closes and Shutdown returns.
func TestShutdownHTTP3WebSocket(t *testing.T) {
	closed := make(chan uint16, 1)
	ws := websocket.NewHandler(websocket.HandlerFuncs{
		Close: func(_ *websocket.Connection, code uint16, _ string, _ error) { closed <- code },
	})
	engine, base, done := shutdownServer(t, func(c *fibhttp.Context) {
		if _, err := ws.Upgrade(c, nil); err != nil {
			t.Logf("upgrade: %v", err)
		}
	})
	r := dialRaw(t, base)
	s, err := r.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	sr := newStreamReader(t, r, s)
	request := headersFrame(
		qpack.HeaderField{Name: ":method", Value: "CONNECT"},
		qpack.HeaderField{Name: ":protocol", Value: "websocket"},
		qpack.HeaderField{Name: ":scheme", Value: "https"},
		qpack.HeaderField{Name: ":authority", Value: "localhost"},
		qpack.HeaderField{Name: ":path", Value: "/ws"},
		qpack.HeaderField{Name: "sec-websocket-version", Value: "13"},
	)
	if err := s.Write(request, false); err != nil {
		t.Fatal(err)
	}
	if typ, _, ok := sr.frame(); !ok || typ != frameHeaders {
		t.Fatalf("frame %d, want HEADERS", typ)
	}

	shut := startShutdown(engine)
	typ, payload, ok := sr.frame()
	if !ok || typ != frameData || len(payload) < 4 || payload[0] != 0x88 ||
		binary.BigEndian.Uint16(payload[2:]) != websocket.CloseGoingAway {
		t.Fatalf("frame %d %x, want a close frame with %d", typ, payload, websocket.CloseGoingAway)
	}
	if _, _, ok := sr.frame(); ok {
		t.Fatal("stream not ended after the close frame")
	}
	_ = s.Close()
	select {
	case code := <-closed:
		if code != websocket.CloseGoingAway {
			t.Errorf("close code %d, want %d", code, websocket.CloseGoingAway)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnClose not called")
	}
	select {
	case <-r.closed:
	case <-time.After(10 * time.Second):
		t.Error("the connection did not close")
	}
	awaitShutdown(t, done, shut)
}
