//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package websocket

import (
	"bufio"
	"errors"
	"io"
	"net"
	stdhttp "net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	epollhttp "github.com/lesismal/fib/http"
)

// rawWebSocketConn dials the server behind a ws:// URL from
// startWebSocketServer without speaking to it yet.
func rawWebSocketConn(t *testing.T, url string) net.Conn {
	t.Helper()
	addr := strings.TrimSuffix(strings.TrimPrefix(url, "ws://"), "/ws")
	conn, err := net.DialTimeout("tcp4", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	return conn
}

func writeString(t *testing.T, conn net.Conn, data string) {
	t.Helper()
	if _, err := io.WriteString(conn, data); err != nil {
		t.Fatal(err)
	}
}

func readStatus(t *testing.T, reader *bufio.Reader) *stdhttp.Response {
	t.Helper()
	resp, err := stdhttp.ReadResponse(reader, &stdhttp.Request{Method: stdhttp.MethodGet})
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// A handshake the minimal path, which serves handlers that take no request,
// cannot accept is answered with 400 and the connection closed.
func TestServerRejectsMalformedMinimalHandshake(t *testing.T) {
	url := startWebSocketServer(t, DefaultConfig(), HandlerFuncs{})
	conn := rawWebSocketConn(t, url)
	writeString(t, conn, "GET chat HTTP/1.1\r\nHost: test\r\n\r\n")
	reader := bufio.NewReader(conn)
	if resp := readStatus(t, reader); resp.StatusCode != stdhttp.StatusBadRequest || !resp.Close {
		t.Fatalf("status %d close %v", resp.StatusCode, resp.Close)
	}
}

// A handler that takes the request has the handshake parsed in full, from as
// many reads as it arrives in.
func TestServerFullHandshakePaths(t *testing.T) {
	config := DefaultConfig()
	config.Subprotocols = []string{"chat"}
	opened := make(chan string, 4)
	url := startWebSocketServer(t, config, HandlerFuncs{
		Open: func(_ *Connection, r *stdhttp.Request) { opened <- r.RemoteAddr },
		Message: func(c *Connection, opcode Opcode, payload []byte) {
			_ = c.WriteMessage(opcode, payload)
		},
	})
	request := strings.Replace(minimalHandshake, "Host: test\r\n", "Host: test\r\nSec-WebSocket-Protocol: chat\r\n", 1)

	t.Run("fragmented", func(t *testing.T) {
		conn := rawWebSocketConn(t, url)
		split := strings.Index(request, "Upgrade")
		writeString(t, conn, request[:split])
		time.Sleep(50 * time.Millisecond)
		writeString(t, conn, request[split:]+"\r\n"+string(clientFrame(Text, true, []byte("hi"))))
		reader := bufio.NewReader(conn)
		resp := readStatus(t, reader)
		if resp.StatusCode != stdhttp.StatusSwitchingProtocols || resp.Header.Get("Sec-WebSocket-Protocol") != "chat" {
			t.Fatalf("status %d header %v", resp.StatusCode, resp.Header)
		}
		if remote := <-opened; remote == "" {
			t.Fatal("OnOpen got no remote address")
		}
		if opcode, payload, err := readServerFrame(reader); err != nil || opcode != Text || string(payload) != "hi" {
			t.Fatalf("echo %v %q %v", opcode, payload, err)
		}
	})

	t.Run("coalesced frame", func(t *testing.T) {
		conn := rawWebSocketConn(t, url)
		writeString(t, conn, request+"\r\n"+string(clientFrame(Binary, true, []byte("first"))))
		reader := bufio.NewReader(conn)
		if resp := readStatus(t, reader); resp.StatusCode != stdhttp.StatusSwitchingProtocols {
			t.Fatalf("status %d", resp.StatusCode)
		}
		<-opened
		if opcode, payload, err := readServerFrame(reader); err != nil || opcode != Binary || string(payload) != "first" {
			t.Fatalf("echo %v %q %v", opcode, payload, err)
		}
	})

	for _, tc := range []struct{ name, data string }{
		{"not a handshake", "GET / HTTP/1.0\r\nHost: test\r\n\r\n"},
		{"wrong method", strings.Replace(minimalHandshake, "GET", "PUT", 1) + "\r\n"},
		{"fragmented and malformed", "GET / HTTP/1.1\r\nHost: test\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := rawWebSocketConn(t, url)
			writeString(t, conn, tc.data)
			if strings.HasPrefix(tc.name, "fragmented") {
				time.Sleep(50 * time.Millisecond)
				writeString(t, conn, " folded: line\r\n\r\n")
			}
			if resp := readStatus(t, bufio.NewReader(conn)); resp.StatusCode != stdhttp.StatusBadRequest {
				t.Fatalf("status %d", resp.StatusCode)
			}
		})
	}
}

// Over a Unix socket the peer has no address and port, and OnOpen's request
// says what the socket does instead.
func TestServerOverUnixSocket(t *testing.T) {
	dir, err := os.MkdirTemp("", "ws")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "s.sock")
	opened := make(chan *stdhttp.Request, 1)
	engineConfig := fib.DefaultConfig()
	engineConfig.Network = "unix"
	engineConfig.Addr = path
	server, err := fib.Bind(engineConfig, NewHandler(HandlerFuncs{
		Open: func(_ *Connection, r *stdhttp.Request) { opened <- r },
	}))
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	t.Cleanup(func() {
		server.Stop()
		<-runDone
		_ = server.Close()
	})
	conn, err := net.DialTimeout("unix", path, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	writeString(t, conn, minimalHandshake+"\r\n")
	if resp := readStatus(t, bufio.NewReader(conn)); resp.StatusCode != stdhttp.StatusSwitchingProtocols {
		t.Fatalf("status %d", resp.StatusCode)
	}
	select {
	case r := <-opened:
		t.Logf("remote address %q", r.RemoteAddr)
	case <-time.After(5 * time.Second):
		t.Fatal("OnOpen not called")
	}
}

// ServerHandler serves a connection it was not told the opening of, and a
// close it hears twice is reported once.
func TestServerHandlerDirectCallbacks(t *testing.T) {
	engine := startClientEngine(t)
	closed := make(chan uint16, 2)
	h := NewHandler(HandlerFuncs{Close: func(_ *Connection, code uint16, _ string, _ error) { closed <- code }})
	h.OnPriorityData(nil, nil)

	conn, peer := liveConnection(t, engine)
	h.OnData(conn, []byte(minimalHandshake+"\r\n"))
	if resp := readStatus(t, bufio.NewReader(peer)); resp.StatusCode != stdhttp.StatusSwitchingProtocols {
		t.Fatalf("status %d", resp.StatusCode)
	}
	h.OnClose(conn, nil)
	h.OnClose(conn, nil)
	if code := <-closed; code != 1006 || len(closed) != 0 {
		t.Fatalf("closed with %d, %d more", code, len(closed))
	}

	// A connection that cannot take the handshake response is closed.
	conn, _ = liveConnection(t, engine)
	_ = conn.Close()
	h.OnData(conn, []byte(minimalHandshake+"\r\n"))
	if state := stateOf(conn); state != nil && state.upgraded() {
		t.Fatal("a connection that could not be answered was upgraded")
	}
}

// Once the handler pool has stopped taking work, work meant for it runs on a
// goroutine of its own.
func TestElsewhereAfterPoolStopped(t *testing.T) {
	// The pool is shared by the engines of one name, so this one has a name
	// of its own.
	config := fib.DefaultConfig()
	config.Name = "ws-elsewhere-test"
	engine, err := fib.NewEngine(config, nil)
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- engine.Run() }()
	t.Cleanup(func() {
		engine.Stop()
		<-runDone
		_ = engine.Close()
	})
	conn, _ := liveConnection(t, engine)
	engine.HandlerPool().Stop()
	ran := make(chan struct{})
	elsewhere(conn, func() { close(ran) })
	select {
	case <-ran:
	case <-time.After(5 * time.Second):
		t.Fatal("fn did not run")
	}
}

// serveHTTP runs a server of package http with handler.
func serveHTTP(t *testing.T, handler epollhttp.HandlerFunc) string {
	t.Helper()
	engineConfig := fib.DefaultConfig()
	engineConfig.Addr = "127.0.0.1:0"
	server, err := fib.Bind(engineConfig, epollhttp.NewHandler(handler))
	if err != nil {
		t.Fatal(err)
	}
	addr, err := server.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	t.Cleanup(func() {
		server.Stop()
		<-runDone
		_ = server.Close()
	})
	return addr.String()
}

// Upgrade answers with the subprotocol it chose, and a second Upgrade of the
// same request fails, since the response has gone.
func TestUpgradeSubprotocolAndSecondUpgrade(t *testing.T) {
	config := DefaultConfig()
	config.Subprotocols = []string{"chat"}
	ws := NewHandlerWithConfig(config, nil)
	second := make(chan error, 1)
	addr := serveHTTP(t, func(c *epollhttp.Context) {
		if _, err := ws.Upgrade(c, nil); err != nil {
			t.Error(err)
			return
		}
		_, err := ws.Upgrade(c, nil)
		second <- err
	})
	conn, err := net.DialTimeout("tcp4", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	writeString(t, conn, strings.Replace(minimalHandshake, "Host: test\r\n",
		"Host: test\r\nSec-WebSocket-Protocol: other, chat\r\n", 1)+"\r\n")
	resp := readStatus(t, bufio.NewReader(conn))
	if resp.StatusCode != stdhttp.StatusSwitchingProtocols || resp.Header.Get("Sec-WebSocket-Protocol") != "chat" {
		t.Fatalf("status %d header %v", resp.StatusCode, resp.Header)
	}
	if err := <-second; !errors.Is(err, epollhttp.ErrResponseWritten) {
		t.Fatalf("second upgrade: %v", err)
	}
}

func TestUpgradeOverHTTP2RefusesOriginAndSubprotocol(t *testing.T) {
	config := DefaultConfig()
	config.CheckOrigin = func(r *stdhttp.Request) bool { return r.Header.Get("Origin") != "http://evil" }
	addr, _ := upgradeServer(t, config)
	c, _ := dialH2(t, addr)
	for i, tc := range []struct {
		field, value, status string
	}{
		{"origin", "http://evil", "403"},
		{"sec-websocket-protocol", "b@d", "400"},
	} {
		stream := uint32(2*i + 1)
		c.headers(stream, false, ":method", "CONNECT", ":protocol", "websocket", ":scheme", "http",
			":path", "/ws", ":authority", "test", "sec-websocket-version", "13", tc.field, tc.value)
		// Skip the connection's frames and the rest of the previous refusal.
		f := c.read()
		for f.stream != stream || f.typ != 0x1 {
			f = c.read()
		}
		if fields := c.decode(f.payload); f.typ != 0x1 || fields[":status"] != tc.status {
			t.Fatalf("%s: response %v", tc.field, fields)
		}
	}
}
