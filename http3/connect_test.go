//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"bytes"
	"errors"
	"io"
	"net"
	stdhttp "net/http"
	"sync/atomic"
	"testing"
	"time"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3/qpack"
)

// connectFrame is the HEADERS frame of a CONNECT to authority (RFC 9114
// section 4.4).
func connectFrame(authority string) []byte {
	return headersFrame(
		qpack.HeaderField{Name: ":method", Value: "CONNECT"},
		qpack.HeaderField{Name: ":authority", Value: authority},
	)
}

// A CONNECT over HTTP/3 switches its stream to a tunnel, which Upgrade does
// not take and Connect does, and which the handler paces: what it sends
// waits for the client, and holding its reads keeps the stream's window
// from coming back until it lets go.
func TestConnectOverHTTP3(t *testing.T) {
	results := make(ccvResults, 16)
	var received atomic.Int64
	opened := make(chan *fibhttp.Tunnel, 1)
	base := startServer(t, Config{}, func(c *fibhttp.Context) {
		if _, ok := c.Request.Header[":protocol"]; ok {
			_, err := c.Connect(nil, &ccvTunnel{})
			results.report("extended", err)
			_ = c.Respond(stdhttp.StatusTeapot, "text/plain", nil)
			return
		}
		_, err := c.Upgrade("ccv", nil, &ccvTunnel{})
		results.report("upgrade", err)
		h := &ccvTunnel{
			open: func(tn *fibhttp.Tunnel) { opened <- tn },
			data: func(_ *fibhttp.Tunnel, data []byte) { received.Add(int64(len(data))) },
		}
		_, err = c.Connect(stdhttp.Header{"X-Proxy": {"fib"}}, h)
		results.report("connect", err)
	})
	w := dialH3Wire(t, nil, base)

	extended, err := w.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := extended.Write(ccvConnect("/"), false); err != nil {
		t.Fatal(err)
	}
	if err := results.expect(t, "extended"); !errors.Is(err, fibhttp.ErrNotUpgradable) {
		t.Fatalf("Connect on an extended CONNECT: %v", err)
	}

	s, err := w.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write(connectFrame("example.com:443"), false); err != nil {
		t.Fatal(err)
	}
	if err := results.expect(t, "upgrade"); !errors.Is(err, fibhttp.ErrNotUpgradable) {
		t.Fatalf("Upgrade on a CONNECT: %v", err)
	}
	if err := results.expect(t, "connect"); err != nil {
		t.Fatal(err)
	}
	tunnel := <-opened
	w.await(s.ID(), "the 200", func(st *h3WireStream) bool { return st.status == stdhttp.StatusOK })

	// What the tunnel sends goes to the client, which WaitSendable waits for.
	payload := bytes.Repeat([]byte("h3"), 1<<20)
	if err := tunnel.Send(payload); err != nil {
		t.Fatal(err)
	}
	if tunnel.Unsent() < 0 {
		t.Fatal("negative unsent")
	}
	notified := make(chan error, 1)
	if tunnel.NotifySendable(0, func(err error) { notified <- err }) {
		select {
		case err := <-notified:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("NotifySendable never ran")
		}
	}
	if err := tunnel.WaitSendable(0); err != nil {
		t.Fatal(err)
	}
	w.await(s.ID(), "the tunnel's data", func(st *h3WireStream) bool { return len(st.body) == len(payload) })

	// Held, the tunnel takes no more than the stream's window; released, it
	// takes the rest.
	tunnel.HoldReads(true)
	sent := 3 << 20
	if err := s.Write(dataFrame(make([]byte, sent)), false); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if n := received.Load(); n >= int64(sent) {
		t.Fatalf("the tunnel took %d bytes under the hold", n)
	}
	tunnel.HoldReads(false)
	deadline := time.Now().Add(10 * time.Second)
	for received.Load() < int64(sent) {
		if time.Now().After(deadline) {
			t.Fatalf("the tunnel took %d of %d bytes after the hold", received.Load(), sent)
		}
		time.Sleep(5 * time.Millisecond)
	}

	// A reset stream ends a wait.
	s.Reset(uint64(ErrCodeRequestCancelled))
	time.Sleep(50 * time.Millisecond)
	_ = tunnel.Send(make([]byte, 1<<20))
	if err := tunnel.WaitSendable(0); err == nil {
		t.Fatal("WaitSendable on a reset stream returned nil")
	}
	if !tunnel.NotifySendable(0, func(err error) { notified <- err }) {
		t.Fatal("NotifySendable on a reset stream reported false")
	}
	select {
	case err := <-notified:
		if err == nil {
			t.Fatal("NotifySendable on a reset stream ran with nil")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("NotifySendable on a reset stream never ran")
	}
}

// fibhttp.ConnectProxy over HTTP/3 reaches a TCP server.
func TestConnectProxyOverHTTP3(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()
	proxy := fibhttp.NewConnectProxy(fibhttp.ConnectProxyConfig{MaxQueuedBytes: 4 << 10})
	base := startServer(t, Config{}, proxy.ServeHTTP)
	w := dialH3Wire(t, nil, base)
	s, err := w.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write(connectFrame(ln.Addr().String()), false); err != nil {
		t.Fatal(err)
	}
	w.await(s.ID(), "the 200", func(st *h3WireStream) bool { return st.status == stdhttp.StatusOK })
	payload := bytes.Repeat([]byte("0123456789"), 400<<10)
	if err := s.Write(dataFrame(payload), false); err != nil {
		t.Fatal(err)
	}
	st := w.await(s.ID(), "the echo", func(st *h3WireStream) bool { return len(st.body) >= len(payload) })
	if !bytes.Equal(st.body, payload) {
		t.Fatal("the echo differs from what was sent")
	}
}
