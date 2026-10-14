//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package websocket

import (
	"errors"
	"io"
	"net"
	stdhttp "net/http"
	"strings"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
)

// idleHandler serves a connection by doing nothing with it.
type idleHandler struct{}

func (idleHandler) OnOpen(*fib.Connection)                 {}
func (idleHandler) OnData(*fib.Connection, []byte)         {}
func (idleHandler) OnPriorityData(*fib.Connection, []byte) {}
func (idleHandler) OnClose(*fib.Connection, error)         {}

// liveConnection dials a plain listener through engine and returns the
// dialing side's connection, which idleHandler serves, so that a test can
// hand it to callbacks of this package's own, and the listener's side of it,
// to see what they send.
func liveConnection(t *testing.T, engine *fib.Engine) (*fib.Connection, net.Conn) {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	accepted := make(chan net.Conn, 1)
	go func() {
		if conn, err := listener.Accept(); err == nil {
			accepted <- conn
		}
	}()
	dialed := make(chan *fib.Connection, 1)
	if err := engine.DialWithHandler("tcp", listener.Addr().String(), 5*time.Second, idleHandler{},
		func(c *fib.Connection, err error) {
			if err != nil {
				t.Error(err)
			}
			dialed <- c
		}); err != nil {
		t.Fatal(err)
	}
	conn := <-dialed
	if conn == nil {
		t.FailNow()
	}
	peer := <-accepted
	t.Cleanup(func() { peer.Close() })
	_ = peer.SetDeadline(time.Now().Add(5 * time.Second))
	return conn, peer
}

func TestNewDialerDefaults(t *testing.T) {
	d := NewDialer(nil, DialerConfig{})
	defaults := DefaultDialerConfig()
	if d.config.MaxMessageBytes != defaults.MaxMessageBytes || d.config.MaxHandshakeBytes != defaults.MaxHandshakeBytes {
		t.Fatalf("limits %d/%d", d.config.MaxMessageBytes, d.config.MaxHandshakeBytes)
	}
	if d.config.HandshakeTimeout != 0 {
		t.Fatalf("a zero timeout became %v", d.config.HandshakeTimeout)
	}
}

func TestHandshakeRequestURLs(t *testing.T) {
	d := NewDialer(nil, DefaultDialerConfig())
	for _, tc := range []struct {
		url, addr string
		secure    bool
	}{
		{"ws://example.com/chat", "example.com:80", false},
		{"wss://example.com/chat", "example.com:443", true},
		{"wss://example.com:8443/chat", "example.com:8443", true},
	} {
		req, addr, secure, key, err := d.handshakeRequest(tc.url, nil)
		if err != nil || addr != tc.addr || secure != tc.secure || len(key) != 24 || req.URL.Scheme != "http" {
			t.Errorf("%s: addr %q secure %v key %q err %v", tc.url, addr, secure, key, err)
		}
	}
	for _, tc := range []struct{ name, url string }{
		{"unparsable", "ws://%zz/"},
		{"scheme", "ftp://example.com/"},
		{"no host", "ws:///chat"},
	} {
		if _, _, _, _, err := d.handshakeRequest(tc.url, nil); err == nil {
			t.Errorf("%s: accepted %q", tc.name, tc.url)
		}
	}
	if _, _, _, _, err := d.handshakeRequest("ws://example.com/", stdhttp.Header{"Sec-Websocket-Key": {"k"}}); err == nil {
		t.Error("accepted a header the dialer sets itself")
	}
}

func TestDialFailsBeforeConnecting(t *testing.T) {
	d := NewDialer(startClientEngine(t), DefaultDialerConfig())
	// No done and no handler: nothing to tell, and nothing panics.
	d.Dial("ftp://example.com/", nil, nil, nil)

	f := d.Go("ftp://example.com/", nil, nil)
	select {
	case <-f.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Done not closed")
	}
	if _, _, err := f.Wait(); !errors.Is(err, ErrUnsupportedScheme) {
		t.Fatalf("scheme: %v", err)
	}

	// A host url.Parse accepts but a request may not carry.
	if _, _, err := d.Go("ws://�/", nil, nil).Wait(); err == nil {
		t.Fatal("a host the request cannot carry was dialed")
	}
	// An address the engine refuses at once.
	if _, _, err := d.Go("ws://127.0.0.1:99999/", nil, nil).Wait(); err == nil {
		t.Fatal("a port out of range was dialed")
	}
}

func TestDialRefused(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	listener.Close()
	d := NewDialer(startClientEngine(t), DefaultDialerConfig())
	conn, resp, err := d.Go("ws://"+addr+"/", nil, nil).Wait()
	if err == nil || conn != nil || resp != nil {
		t.Fatalf("conn %v resp %v err %v", conn, resp, err)
	}
}

func TestClientConnBadResponses(t *testing.T) {
	config := DefaultDialerConfig()
	config.MaxHandshakeBytes = 16
	for _, tc := range []struct{ name, data string }{
		{"too large", "HTTP/1.1 101 Switching Protocols"},
		{"unparsable", "garbage\r\n\r\n"},
	} {
		var got error
		cc := &clientConn{dialer: NewDialer(nil, config), req: &stdhttp.Request{Method: stdhttp.MethodGet},
			done: func(_ *Connection, _ *stdhttp.Response, err error) { got = err }}
		cc.OnData(nil, []byte(tc.data))
		if !errors.Is(got, ErrBadHandshake) {
			t.Errorf("%s: %v", tc.name, got)
		}
		// The dial has failed; what follows is ignored.
		cc.OnData(nil, []byte("more"))
		if len(cc.response) != len(tc.data) {
			t.Errorf("%s: kept %d bytes after failing", tc.name, len(cc.response))
		}
	}
}

func TestClientConnCheckResponse(t *testing.T) {
	const key = "MDEyMzQ1Njc4OWFiY2RlZg=="
	accept := acceptFor(key)
	response := func(header ...string) *stdhttp.Response {
		h := stdhttp.Header{}
		for i := 0; i < len(header); i += 2 {
			h.Add(header[i], header[i+1])
		}
		return &stdhttp.Response{StatusCode: stdhttp.StatusSwitchingProtocols, Header: h}
	}
	for _, tc := range []struct {
		name     string
		compress bool
		resp     *stdhttp.Response
		want     string
	}{
		{"no upgrade", false, response("Connection", "Upgrade", "Sec-WebSocket-Accept", accept), "Upgrade"},
		{"no connection", false, response("Upgrade", "websocket", "Sec-WebSocket-Accept", accept), "Connection"},
		{"extension not offered", false, response("Upgrade", "websocket", "Connection", "Upgrade",
			"Sec-WebSocket-Accept", accept, "Sec-WebSocket-Extensions", "permessage-deflate"), "not offered"},
		{"bad deflate answer", true, response("Upgrade", "websocket", "Connection", "Upgrade",
			"Sec-WebSocket-Accept", accept, "Sec-WebSocket-Extensions", "x-other"), "answered permessage-deflate"},
	} {
		cc := &clientConn{dialer: NewDialer(nil, DialerConfig{EnableCompression: tc.compress})}
		websocketAccept(cc.accept[:], []byte(key))
		if _, _, _, err := cc.checkResponse(tc.resp); !errors.Is(err, ErrBadHandshake) ||
			!strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v", tc.name, err)
		}
	}
}

// A connection that arrives after its dial has already failed, as one that
// timed out while connecting does, is closed on arrival.
func TestClientConnClosesLateConnection(t *testing.T) {
	conn, peer := liveConnection(t, startClientEngine(t))
	cc := &clientConn{}
	cc.settled.Store(true)
	cc.OnPriorityData(conn, nil)
	cc.OnOpen(conn)
	if _, err := peer.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("read %v, want EOF", err)
	}
}
