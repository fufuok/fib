//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package websocket

import (
	"fmt"
	stdhttp "net/http"
	"sync"
	"testing"
	"time"

	epollhttp "github.com/lesismal/fib/http"
)

// receiveString waits for what a server handler reports on ch.
func receiveString(t *testing.T, ch <-chan string) string {
	t.Helper()
	select {
	case s := <-ch:
		return s
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the server")
		return ""
	}
}

// The slot holds any value, nil and values of differing types included.
func TestSessionStoresAnyValue(t *testing.T) {
	var c Connection
	if v := c.Session(); v != nil {
		t.Fatalf("Session before SetSession = %v, want nil", v)
	}
	c.SetSession(42)
	if v, ok := c.Session().(int); !ok || v != 42 {
		t.Fatalf("Session = %v, want 42", c.Session())
	}
	c.SetSession("replaced")
	if v, ok := c.Session().(string); !ok || v != "replaced" {
		t.Fatalf("Session = %v, want \"replaced\"", c.Session())
	}
	// A typed nil is a value like any other, kept with its type.
	var p *int
	c.SetSession(p)
	if v, ok := c.Session().(*int); !ok || v != nil {
		t.Fatalf("Session = %#v, want (*int)(nil)", c.Session())
	}
	c.SetSession(nil)
	if v := c.Session(); v != nil {
		t.Fatalf("Session after SetSession(nil) = %v, want nil", v)
	}
}

// SetSession and Session may race; run with -race.
func TestSessionConcurrentAccess(t *testing.T) {
	var c Connection
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				if i%2 == 0 {
					c.SetSession(g*1000 + i)
				} else {
					c.SetSession(fmt.Sprint(g, i))
				}
				switch v := c.Session().(type) {
				case int, string:
				default:
					t.Errorf("Session = %#v", v)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}

// On a server of this package and on a Dialer's connection, Conn is the
// connection itself, and the session set in OnOpen is there for messages.
func TestSessionAndConnOnServerAndDialer(t *testing.T) {
	remote := make(chan string, 1)
	url := startWebSocketServer(t, DefaultConfig(), HandlerFuncs{
		Open: func(c *Connection, _ *stdhttp.Request) {
			c.SetSession("server-session")
			if conn := c.Conn(); conn != nil {
				remote <- conn.RemoteAddr().String()
			} else {
				remote <- "nil"
			}
		},
		Message: func(c *Connection, opcode Opcode, payload []byte) {
			s, _ := c.Session().(string)
			_ = c.WriteMessage(opcode, append([]byte(s+":"), payload...))
		},
	})
	dialer := NewDialer(startClientEngine(t), DefaultDialerConfig())
	recorder := newClientRecorder()
	conn, _, err := dialer.Go(url, nil, recorder.handler()).Wait()
	if err != nil {
		t.Fatal(err)
	}
	client := conn.Conn()
	if client == nil {
		t.Fatal("Conn of a Dialer's connection is nil")
	}
	if got, want := receiveString(t, remote), client.LocalAddr().String(); got != want {
		t.Fatalf("server Conn remote %s, want the client's local %s", got, want)
	}
	if err := conn.WriteText("hi"); err != nil {
		t.Fatal(err)
	}
	if event := recorder.next(t); string(event.Payload) != "server-session:hi" {
		t.Fatalf("echo %q", event.Payload)
	}
	conn.SetSession(7)
	if v, ok := conn.Session().(int); !ok || v != 7 {
		t.Fatalf("client Session = %v, want 7", conn.Session())
	}
	if err := conn.Close(CloseNormal, ""); err != nil {
		t.Fatal(err)
	}
	// The slot and Conn outlive the close.
	if v, ok := conn.Session().(int); !ok || v != 7 || conn.Conn() != client {
		t.Fatalf("after close: Session %v, Conn %p, want 7 and %p", conn.Session(), conn.Conn(), client)
	}
}

// After an HTTP/1.1 upgrade, Conn is the connection the request came on,
// already in OnOpen.
func TestConnAfterHTTP1Upgrade(t *testing.T) {
	remote := make(chan string, 2)
	ws := NewHandler(HandlerFuncs{
		Open: func(c *Connection, _ *stdhttp.Request) {
			c.SetSession("upgraded")
			if conn := c.Conn(); conn != nil {
				remote <- conn.RemoteAddr().String()
			} else {
				remote <- "nil"
			}
		},
		Message: func(c *Connection, opcode Opcode, payload []byte) {
			s, _ := c.Session().(string)
			_ = c.WriteMessage(opcode, append([]byte(s+":"), payload...))
		},
	})
	addr := serveHTTP(t, func(c *epollhttp.Context) {
		conn, err := ws.Upgrade(c, nil)
		if err != nil {
			t.Error(err)
			return
		}
		// Upgrade reports the same connection it handed OnOpen.
		if c.Conn != conn.Conn() {
			t.Errorf("Conn %p, want the request's connection %p", conn.Conn(), c.Conn)
		}
	})
	dialer := NewDialer(startClientEngine(t), DefaultDialerConfig())
	recorder := newClientRecorder()
	conn, _, err := dialer.Go("ws://"+addr+"/ws", nil, recorder.handler()).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := receiveString(t, remote), conn.Conn().LocalAddr().String(); got != want {
		t.Fatalf("upgraded Conn remote %s, want the client's local %s", got, want)
	}
	if err := conn.WriteText("hi"); err != nil {
		t.Fatal(err)
	}
	if event := recorder.next(t); string(event.Payload) != "upgraded:hi" {
		t.Fatalf("echo %q", event.Payload)
	}
	if err := conn.Close(CloseNormal, ""); err != nil {
		t.Fatal(err)
	}
}

// Over HTTP/2 the WebSocket is one stream of a shared connection, so Conn
// is nil, while the session works as anywhere else.
func TestConnNilOverHTTP2ExtendedConnect(t *testing.T) {
	seen := make(chan string, 1)
	ws := NewHandler(HandlerFuncs{
		Open: func(c *Connection, _ *stdhttp.Request) {
			c.SetSession("h2")
			if c.Conn() != nil {
				seen <- "not nil"
			} else {
				seen <- "nil"
			}
		},
		Message: func(c *Connection, opcode Opcode, payload []byte) {
			s, _ := c.Session().(string)
			_ = c.WriteMessage(opcode, append([]byte(s+":"), payload...))
		},
	})
	addr := serveHTTP(t, func(c *epollhttp.Context) {
		if _, err := ws.Upgrade(c, nil); err != nil {
			t.Logf("upgrade: %v", err)
		}
	})
	c, _ := dialH2(t, addr)
	c.headers(1, false, ":method", "CONNECT", ":protocol", "websocket", ":scheme", "http",
		":path", "/ws", ":authority", "test", "sec-websocket-version", "13")
	if f := c.next(1); f.typ != 0x1 {
		t.Fatalf("frame type %d, want HEADERS", f.typ)
	}
	if got := receiveString(t, seen); got != "nil" {
		t.Fatalf("Conn over HTTP/2 is %s, want nil", got)
	}
	c.write(h2TestFrameBytes(0x0, 0, 1, clientFrame(Text, true, []byte("hi"))))
	if _, payload, _ := c.readWebSocket(1); string(payload) != "h2:hi" {
		t.Fatalf("echo %q", payload)
	}
}
