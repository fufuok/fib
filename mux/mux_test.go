//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package mux

import (
	"context"
	"errors"
	"io"
	"net"
	stdhttp "net/http"
	"os"
	"sync/atomic"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
)

// web answers every request with its protocol, whether it came over TLS, and
// its path.
func web(c *fibhttp.Context) {
	reply := c.Request.Proto + " tls=" + map[bool]string{false: "no", true: "yes"}[c.Request.TLS != nil] +
		" " + c.Request.URL.Path
	if err := c.WriteResponse(fibhttp.Response{StatusCode: 200, Body: []byte(reply)}); err != nil {
		c.Conn.Close()
	}
}

// replying answers every read with reply followed by what was read, so a
// client can tell which handler served it and that nothing it sent was lost.
func replying(reply string) fib.Handler {
	return fib.HandlerFuncs{Data: func(c *fib.Connection, b []byte) {
		if c.Send(append([]byte(reply), b...)) != nil {
			c.Close()
		}
	}}
}

// serve binds h on a port of its own and runs it until the test ends.
func serve(t *testing.T, h fib.Handler) (*fib.Engine, string) {
	t.Helper()
	config := fib.DefaultConfig()
	config.Name = "mux-" + t.Name()
	engine, err := fib.New(config, fib.Binding{Addr: "127.0.0.1:0", Handler: h})
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
	addr, err := engine.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	return engine, addr.String()
}

// dial connects to addr with a deadline that bounds the whole test step.
func dial(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// readUntil reads from conn until it has len(want) bytes, and compares.
func readUntil(t *testing.T, conn net.Conn, want string) {
	t.Helper()
	got := make([]byte, len(want))
	if _, err := io.ReadFull(conn, got); err != nil || string(got) != want {
		t.Fatalf("read %q (%v), want %q", got, err, want)
	}
}

// TLS, HTTP/1.1, h2c and a raw protocol all share one port, and anything
// else falls back to an echo.
func TestOnePortServesEveryProtocol(t *testing.T) {
	serverTLS, clientTLS, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	h := NewWithConfig(Config{Fallback: replying("echo:")},
		Route{Match: TLS(), Handler: fibtls.NewServer(fibhttp.ConfigureTLS(serverTLS), fibhttp.NewHandler(fibhttp.HandlerFunc(web)))},
		Route{Match: HTTP(), Handler: fibhttp.NewHandler(fibhttp.HandlerFunc(web))},
		Route{Match: Prefix("PING"), Handler: replying("pong:")},
	)
	_, addr := serve(t, h)

	// HTTP/1.1, keeping the connection for several requests, which all go to
	// the HTTP handler straight from the engine once the first has handed
	// the connection over.
	client := &stdhttp.Client{Timeout: 10 * time.Second}
	for i := 0; i < 3; i++ {
		resp, err := client.Get("http://" + addr + "/plain")
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if string(body) != "HTTP/1.1 tls=no /plain" {
			t.Fatalf("HTTP/1.1 answered %q", body)
		}
	}

	// HTTPS, over HTTP/2 by ALPN.
	transport := &stdhttp.Transport{TLSClientConfig: clientTLS, ForceAttemptHTTP2: true}
	defer transport.CloseIdleConnections()
	resp, err := (&stdhttp.Client{Transport: transport, Timeout: 10 * time.Second}).Get("https://" + addr + "/secure")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "HTTP/2.0 tls=yes /secure" {
		t.Fatalf("HTTPS answered %q", body)
	}

	// h2c with prior knowledge: the preface and an empty SETTINGS frame,
	// which an HTTP/2 server answers with a SETTINGS frame of its own.
	conn := dial(t, addr)
	if _, err := conn.Write([]byte(http2Preface + "\x00\x00\x00\x04\x00\x00\x00\x00\x00")); err != nil {
		t.Fatal(err)
	}
	header := make([]byte, 9)
	if _, err := io.ReadFull(conn, header); err != nil || header[3] != 0x4 {
		t.Fatalf("h2c answered % x (%v), want a SETTINGS frame", header, err)
	}

	// A raw protocol, recognised by its first word.
	conn = dial(t, addr)
	if _, err := conn.Write([]byte("PING 1")); err != nil {
		t.Fatal(err)
	}
	readUntil(t, conn, "pong:PING 1")
	// The mux has stepped aside: what follows reaches the handler as is.
	if _, err := conn.Write([]byte("again")); err != nil {
		t.Fatal(err)
	}
	readUntil(t, conn, "pong:again")

	// Anything else, to the fallback.
	conn = dial(t, addr)
	if _, err := conn.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	readUntil(t, conn, "echo:hello")
}

// A first read too short to decide on is kept, and handed over whole with
// the rest once the route is known.
func TestShortFirstReadsAreKept(t *testing.T) {
	_, addr := serve(t, New(
		Route{Match: Prefix("PING"), Handler: replying("pong:")},
		Route{Match: Any(), Handler: replying("any:")},
	))
	conn := dial(t, addr)
	for _, part := range []string{"P", "I", "NG!"} {
		if _, err := conn.Write([]byte(part)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	readUntil(t, conn, "pong:PING!")

	conn = dial(t, addr)
	for _, part := range []string{"PI", "X"} {
		if _, err := conn.Write([]byte(part)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	readUntil(t, conn, "any:PIX")
}

// Without a fallback, a connection no route recognises is closed, and so is
// one that never decides within MaxPeek bytes.
func TestUnmatchedConnectionsClose(t *testing.T) {
	closed := make(chan error, 2)
	h := NewWithConfig(Config{MaxPeek: 8}, Route{Match: Prefix("0123456789"), Handler: replying("digits:")})
	_, addr := serve(t, &closeWatcher{Handler: h, closed: closed})
	for _, send := range []string{"nope", "01234567"} {
		conn := dial(t, addr)
		if _, err := conn.Write([]byte(send)); err != nil {
			t.Fatal(err)
		}
		if n, err := conn.Read(make([]byte, 1)); err == nil {
			t.Fatalf("%q: read %d bytes from a connection no route took", send, n)
		}
		select {
		case err := <-closed:
			if !errors.Is(err, ErrNoMatch) {
				t.Fatalf("%q: closed with %v, want ErrNoMatch", send, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%q: not closed", send)
		}
	}
}

// closeWatcher reports the errors the mux closes connections with.
type closeWatcher struct {
	*Handler
	closed chan error
}

func (w *closeWatcher) OnClose(c *fib.Connection, err error) {
	w.Handler.OnClose(c, err)
	w.closed <- err
}

// A route listed first keeps its priority while it waits for more, over a
// later route that already recognises the bytes.
func TestEarlierRoutesWin(t *testing.T) {
	h := New(
		Route{Match: Prefix("GETX"), Handler: replying("first:")},
		Route{Match: Prefix("GE"), Handler: replying("second:")},
	)
	if got, _, ok := h.decide([]byte("GE")); ok {
		t.Fatalf("decided on %v before the first route ruled itself out", got)
	}
	_, addr := serve(t, h)
	for send, want := range map[string]string{"GETX": "first:GETX", "GETY": "second:GETY"} {
		conn := dial(t, addr)
		if _, err := conn.Write([]byte(send)); err != nil {
			t.Fatal(err)
		}
		readUntil(t, conn, want)
	}
}

// A peer that says nothing is closed once the timeout passes; the handler
// it is handed to afterwards starts with no deadline.
func TestSilentPeersTimeOut(t *testing.T) {
	closed := make(chan error, 1)
	h := NewWithConfig(Config{Timeout: 100 * time.Millisecond}, Route{Match: Any(), Handler: replying("ok:")})
	_, addr := serve(t, &closeWatcher{Handler: h, closed: closed})
	conn := dial(t, addr)
	select {
	case err := <-closed:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("closed with %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a silent peer was kept")
	}
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("the silent connection is still open")
	}

	conn = dial(t, addr)
	if _, err := conn.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	readUntil(t, conn, "ok:hi")
	time.Sleep(300 * time.Millisecond)
	if _, err := conn.Write([]byte("still")); err != nil {
		t.Fatal(err)
	}
	readUntil(t, conn, "ok:still")
}

// Shutdown closes the connections still undecided, which have nothing in
// flight, and leaves the ones handed off to their handlers.
func TestShutdownClosesUndecided(t *testing.T) {
	engine, addr := serve(t, New(Route{Match: Prefix("PING"), Handler: replying("pong:")}))
	undecided := dial(t, addr)
	if _, err := undecided.Write([]byte("PI")); err != nil {
		t.Fatal(err)
	}
	served := dial(t, addr)
	if _, err := served.Write([]byte("PING")); err != nil {
		t.Fatal(err)
	}
	readUntil(t, served, "pong:PING")
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	// The served connection's handler takes no OnShutdown, so Shutdown runs
	// out of time waiting for it, while the undecided one is closed at once.
	_ = engine.Shutdown(ctx)
	// The read would wait out the connection's deadline were it still open.
	if _, err := undecided.Read(make([]byte, 1)); err == nil || errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("an undecided connection outlived Shutdown's first round: %v", err)
	}
}

func TestMatchers(t *testing.T) {
	cases := []struct {
		name  string
		m     Matcher
		input string
		want  Result
	}{
		{"tls hello", TLS(), "\x16\x03\x01\x02\x00", Match},
		{"tls short", TLS(), "\x16\x03", NeedMore},
		{"tls ssl2", TLS(), "\x16\x02\x00", NoMatch},
		{"tls other", TLS(), "G", NoMatch},
		{"http get", HTTP(), "GET / HTTP/1.1\r\n", Match},
		{"http partial", HTTP(), "GE", NeedMore},
		{"http webdav", HTTP1(), "PROPFIND /", Match},
		{"http no space", HTTP1(), "GETX", NoMatch},
		{"h2 preface", HTTP(), http2Preface, Match},
		{"h2 partial", HTTP2(), "PRI * HTTP/2", NeedMore},
		{"h2 not http1", HTTP1(), "PRI ", NoMatch},
		{"prefix many", Prefix("ab", "abc"), "abc", Match},
		{"prefix empty", Prefix(""), "x", Match},
		{"prefix none", Prefix(), "x", NoMatch},
		{"anyof", AnyOf(Prefix("x"), Prefix("yz")), "y", NeedMore},
		{"any", Any(), "?", Match},
	}
	for _, tc := range cases {
		if got := tc.m([]byte(tc.input)); got != tc.want {
			t.Errorf("%s: %q gave %d, want %d", tc.name, tc.input, got, tc.want)
		}
	}
}

func TestRouteNeedsMatcherAndHandler(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("a route without a handler was accepted")
		}
	}()
	New(Route{Match: Any()})
}

// The routes are copied: changing the caller's slice afterwards changes
// nothing.
func TestRoutesAreCopied(t *testing.T) {
	routes := []Route{{Match: Prefix("a"), Handler: replying("a:")}}
	h := New(routes...)
	routes[0].Match = Prefix("b")
	if got, _, ok := h.decide([]byte("a")); !ok || got == nil {
		t.Fatal("the route changed with the caller's slice")
	}
}

// A route waiting for more can speak first, once, to a client that waits for
// the server before it says enough.
func TestNudgeSpeaksOnce(t *testing.T) {
	var nudges atomic.Int32
	h := New(Route{
		Match:   Prefix("HELLO"),
		Handler: replying("hello:"),
		Nudge: func(c *fib.Connection, input []byte) bool {
			nudges.Add(1)
			_ = c.Send([]byte("?"))
			return true
		},
	})
	_, addr := serve(t, h)
	conn := dial(t, addr)
	if _, err := conn.Write([]byte("HE")); err != nil {
		t.Fatal(err)
	}
	readUntil(t, conn, "?")
	if _, err := conn.Write([]byte("L")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if _, err := conn.Write([]byte("LO")); err != nil {
		t.Fatal(err)
	}
	// No second "?" comes before the reply.
	readUntil(t, conn, "hello:HELLO")
	if n := nudges.Load(); n != 1 {
		t.Fatalf("Nudge ran %d times, want 1", n)
	}
}

func TestSOCKSMatchers(t *testing.T) {
	for _, c := range []struct {
		name  string
		m     Matcher
		input string
		want  Result
	}{
		{"SOCKS5", SOCKS5(), "\x05", NeedMore},
		{"SOCKS5", SOCKS5(), "\x05\x00", NoMatch},
		{"SOCKS5", SOCKS5(), "\x05\x02\x00\x02", Match},
		{"SOCKS5", SOCKS5(), "\x04\x01", NoMatch},
		{"SOCKS4", SOCKS4(), "\x04", NeedMore},
		{"SOCKS4", SOCKS4(), "\x04\x01", Match},
		{"SOCKS4", SOCKS4(), "\x04\x02", Match},
		{"SOCKS4", SOCKS4(), "\x04\x03", NoMatch},
		{"SOCKS4", SOCKS4(), "\x05\x01", NoMatch},
		{"SOCKS", SOCKS(), "\x05\x01\x00", Match},
		{"SOCKS", SOCKS(), "\x04\x01", Match},
		{"SOCKS", SOCKS(), "\x05", NeedMore},
		{"SOCKS", SOCKS(), "GET ", NoMatch},
	} {
		if got := c.m([]byte(c.input)); got != c.want {
			t.Errorf("%s(% x) = %v, want %v", c.name, c.input, got, c.want)
		}
	}
}

// The guards for what races the decision: data on a connection the mux did
// not open, and a shutdown that closed the connection before a no-match or a
// handoff could. None of them touches the connection beyond its attachment.
func TestDecisionGuards(t *testing.T) {
	h := NewWithConfig(Config{Timeout: -1}, Route{Match: Prefix("a"), Handler: replying("a:")})
	c := &fib.Connection{}
	h.OnData(c, []byte("a"))
	h.OnPriorityData(c, []byte("a"))

	s := &sniff{}
	s.decided.Store(true)
	c.SetAttachment(s)
	h.OnData(c, []byte("b")) // no route, but already decided
	h.OnData(c, []byte("a")) // a route, but already decided
	if c.Attachment() != s {
		t.Fatal("a decided connection was handed off")
	}
}
