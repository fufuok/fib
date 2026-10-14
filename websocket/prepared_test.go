//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package websocket

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	stdhttp "net/http"
	"strings"
	"testing"
	"time"
)

func preparedEcho(c *Connection, opcode Opcode, payload []byte) error {
	m, err := NewPreparedMessage(opcode, payload)
	if err != nil {
		return err
	}
	return c.WritePreparedMessage(m)
}

func TestNewPreparedMessageRejects(t *testing.T) {
	for _, opcode := range []Opcode{Continuation, Close, Ping, Pong} {
		if _, err := NewPreparedMessage(opcode, nil); err == nil {
			t.Errorf("opcode %v accepted", opcode)
		}
	}
	if _, err := NewPreparedMessage(Text, []byte{0xff}); err != ErrInvalidPayload {
		t.Errorf("invalid UTF-8: err = %v, want ErrInvalidPayload", err)
	}
	payload := []byte("copied")
	m, err := NewPreparedMessage(Binary, payload)
	if err != nil {
		t.Fatal(err)
	}
	payload[0] = 'X'
	if string(m.payload()) != "copied" {
		t.Fatalf("payload = %q: NewPreparedMessage kept the caller's array", m.payload())
	}
}

// A prepared message has to arrive as WriteMessage's would, from a server
// and from a client, each with and without permessage-deflate.
func TestPreparedMessageEchoThroughDialer(t *testing.T) {
	for _, serverCompress := range []bool{false, true} {
		for _, clientCompress := range []bool{false, true} {
			t.Run(fmt.Sprintf("server=%v/client=%v", serverCompress, clientCompress), func(t *testing.T) {
				config := DefaultConfig()
				config.EnableCompression = serverCompress
				handler := HandlerFuncs{Message: func(c *Connection, opcode Opcode, payload []byte) {
					if err := preparedEcho(c, opcode, payload); err != nil {
						t.Error(err)
					}
				}}
				url := startWebSocketServer(t, config, handler)
				dialerConfig := DefaultDialerConfig()
				dialerConfig.EnableCompression = clientCompress
				recorder := newClientRecorder()
				conn, _, err := NewDialer(startClientEngine(t), dialerConfig).Go(url, nil, recorder.handler()).Wait()
				if err != nil {
					t.Fatal(err)
				}
				if conn.compress != (serverCompress && clientCompress) {
					t.Fatalf("compress = %v", conn.compress)
				}
				for _, size := range []int{0, 1, 125, 4096, 70000} {
					payload := compressiblePayload(size)
					if err := preparedEcho(conn, Text, payload); err != nil {
						t.Fatal(err)
					}
					if event := recorder.next(t); event.Opcode != Text || !bytes.Equal(event.Payload, payload) {
						t.Fatalf("size %d: echo opcode=%v len=%d", size, event.Opcode, len(event.Payload))
					}
				}
			})
		}
	}
}

// dialRaw completes a handshake offering extensions, if any, and returns the
// connection and the extensions the server accepted.
func dialRaw(tb testing.TB, addr, extensions string) (net.Conn, *bufio.Reader, string) {
	tb.Helper()
	conn, err := net.DialTimeout("tcp4", addr, 5*time.Second)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { conn.Close() })
	key := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef"))
	request := "GET /ws HTTP/1.1\r\nHost: test\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: " + key + "\r\n"
	if extensions != "" {
		request += "Sec-WebSocket-Extensions: " + extensions + "\r\n"
	}
	if _, err = io.WriteString(conn, request+"\r\n"); err != nil {
		tb.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	response, err := stdhttp.ReadResponse(reader, &stdhttp.Request{Method: stdhttp.MethodGet})
	if err != nil {
		tb.Fatal(err)
	}
	if response.StatusCode != stdhttp.StatusSwitchingProtocols {
		tb.Fatalf("handshake status = %d", response.StatusCode)
	}
	return conn, reader, response.Header.Get("Sec-WebSocket-Extensions")
}

// One message broadcast to connections that agreed different things: no
// compression, compression with the full window, and with a 512-byte window.
// Each has to get the message it agreed to, and the message compresses once
// for each window in use and no more.
func TestPreparedMessageBroadcast(t *testing.T) {
	opened := make(chan *Connection, 4)
	url := startWebSocketServer(t, compressionConfig(), HandlerFuncs{
		Open: func(c *Connection, _ *stdhttp.Request) { opened <- c },
	})
	addr := strings.TrimSuffix(strings.TrimPrefix(url, "ws://"), "/ws")

	type peer struct {
		name       string
		reader     *bufio.Reader
		compressed bool
	}
	var peers []peer
	for _, offer := range []struct{ name, extensions string }{
		{"plain", ""},
		{"deflate", "permessage-deflate"},
		{"deflate window 9", "permessage-deflate; server_max_window_bits=9"},
		{"deflate again", "permessage-deflate; client_max_window_bits"},
	} {
		conn, reader, accepted := dialRaw(t, addr, offer.extensions)
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
		peers = append(peers, peer{offer.name, reader, accepted != ""})
	}
	var conns []*Connection
	for range peers {
		select {
		case c := <-opened:
			conns = append(conns, c)
		case <-time.After(5 * time.Second):
			t.Fatal("OnOpen not called")
		}
	}

	for _, size := range []int{0, 3000, 70000} {
		payload := compressiblePayload(size)
		m, err := NewPreparedMessage(Binary, payload)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range conns {
			if err := c.WritePreparedMessage(m); err != nil {
				t.Fatal(err)
			}
		}
		for _, p := range peers {
			first, err := p.reader.Peek(1)
			if err != nil {
				t.Fatalf("%s: %v", p.name, err)
			}
			if compressed := first[0]&0x40 != 0; compressed != p.compressed {
				t.Fatalf("%s: RSV1 = %v, want %v", p.name, compressed, p.compressed)
			}
			opcode, got, err := readServerFrame(p.reader)
			if err != nil || opcode != Binary {
				t.Fatalf("%s: opcode=%v err=%v", p.name, opcode, err)
			}
			if p.compressed {
				if got, err = decompressMessage(nil, got, nil, 1<<20); err != nil {
					t.Fatalf("%s: %v", p.name, err)
				}
			}
			if !bytes.Equal(got, payload) {
				t.Fatalf("%s: size %d arrived as %d bytes that differ", p.name, size, len(got))
			}
		}
		for bits := minWindowBits; bits <= maxWindowBits; bits++ {
			made := m.deflated[bits-minWindowBits].frame != nil
			if want := bits == 9 || bits == maxWindowBits; made != want {
				t.Fatalf("size %d: window %d compressed = %v, want %v", size, bits, made, want)
			}
		}
	}
}

// Through Upgrade the connection is a tunnel: on HTTP/1.1 the connection
// itself, and on HTTP/2 a stream, whose writer frames what it is given.
func TestPreparedMessageThroughUpgrade(t *testing.T) {
	t.Run("HTTP/1.1", func(t *testing.T) {
		for _, compress := range []bool{false, true} {
			config := DefaultConfig()
			config.EnableCompression = compress
			addr, _ := upgradeServerWith(t, config, preparedEcho)
			dialerConfig := DefaultDialerConfig()
			dialerConfig.EnableCompression = compress
			recorder := newClientRecorder()
			conn, _, err := NewDialer(startClientEngine(t), dialerConfig).Go("ws://"+addr+"/ws", nil, recorder.handler()).Wait()
			if err != nil {
				t.Fatal(err)
			}
			for _, size := range []int{5, 70000} {
				payload := compressiblePayload(size)
				if err := conn.WriteBinary(payload); err != nil {
					t.Fatal(err)
				}
				if event := recorder.next(t); event.Opcode != Binary || !bytes.Equal(event.Payload, payload) {
					t.Fatalf("compress=%v size %d: echo opcode=%v len=%d", compress, size, event.Opcode, len(event.Payload))
				}
			}
		}
	})
	t.Run("HTTP/2", func(t *testing.T) {
		addr, _ := upgradeServerWith(t, DefaultConfig(), preparedEcho)
		c, _ := dialH2(t, addr)
		c.headers(1, false, ":method", "CONNECT", ":protocol", "websocket", ":scheme", "http",
			":path", "/ws", ":authority", "test", "sec-websocket-version", "13")
		if f := c.next(1); f.typ != 0x1 {
			t.Fatalf("frame type %d, want HEADERS", f.typ)
		}
		c.write(h2TestFrameBytes(0x0, 0, 1, clientFrame(Text, true, []byte("over h2"))))
		if f := c.next(1); f.typ != 0x0 ||
			!bytes.Equal(f.payload, append([]byte{0x81, byte(len("over h2"))}, "over h2"...)) {
			t.Fatalf("frame type %d payload %q", f.typ, f.payload)
		}
	})
}

// BenchmarkBroadcast sends one message to many connections, whose peers in
// this process read and discard it, with WriteMessage for each connection and
// with one PreparedMessage for them all. ns/op is one broadcast.
func BenchmarkBroadcast(b *testing.B) {
	const connections = 64
	for _, compress := range []bool{false, true} {
		for _, size := range []int{1 << 10, 16 << 10} {
			for _, prepared := range []bool{false, true} {
				name := fmt.Sprintf("deflate=%v/%dKiB/prepared=%v", compress, size>>10, prepared)
				b.Run(name, func(b *testing.B) {
					benchmarkBroadcast(b, connections, compress, size, prepared)
				})
			}
		}
	}
}

func benchmarkBroadcast(b *testing.B, connections int, compress bool, size int, prepared bool) {
	opened := make(chan *Connection, connections)
	config := DefaultConfig()
	config.EnableCompression = compress
	url := startWebSocketServer(b, config, HandlerFuncs{
		Open: func(c *Connection, _ *stdhttp.Request) { opened <- c },
	})
	addr := strings.TrimSuffix(strings.TrimPrefix(url, "ws://"), "/ws")
	extensions := ""
	if compress {
		extensions = "permessage-deflate"
	}
	conns := make([]*Connection, 0, connections)
	for i := 0; i < connections; i++ {
		_, reader, _ := dialRaw(b, addr, extensions)
		go func() { _, _ = io.Copy(io.Discard, reader) }()
		conns = append(conns, <-opened)
	}
	payload := compressiblePayload(size)
	b.SetBytes(int64(size * connections))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if prepared {
			m, err := NewPreparedMessage(Binary, payload)
			if err != nil {
				b.Fatal(err)
			}
			for _, c := range conns {
				if err := c.WritePreparedMessage(m); err != nil {
					b.Fatal(err)
				}
			}
			continue
		}
		for _, c := range conns {
			if err := c.WriteMessage(Binary, payload); err != nil {
				b.Fatal(err)
			}
		}
	}
}
