//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package multiproto

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	stdhttp "net/http"
	"strings"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/arpc"
	"github.com/lesismal/fib/grpc"
	"github.com/lesismal/fib/hpack"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/socks"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
	"github.com/lesismal/fib/websocket"
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

// startEngine runs an engine of config and h until the test ends.
func startEngine(t *testing.T, config fib.Config, h fib.Handler) *fib.Engine {
	t.Helper()
	config.Name = "multiproto-" + t.Name()
	var engine *fib.Engine
	var err error
	if h == nil {
		engine, err = fib.NewEngine(config, nil)
	} else {
		engine, err = fib.Bind(config, h)
	}
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
	return engine
}

// serve binds h on a port of its own and returns where it listens.
func serve(t *testing.T, h fib.Handler) string {
	t.Helper()
	config := fib.DefaultConfig()
	config.Addr = "127.0.0.1:0"
	addr, err := startEngine(t, config, h).LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	return addr.String()
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

func readFull(t *testing.T, conn net.Conn, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := io.ReadFull(conn, b); err != nil {
		t.Fatalf("read %d bytes: %v", n, err)
	}
	return b
}

// readH2Frame reads one HTTP/2 frame.
func readH2Frame(t *testing.T, conn net.Conn) (typ, flags byte, stream uint32, payload []byte) {
	t.Helper()
	h := readFull(t, conn, h2FrameHeaderLen)
	length := int(h[0])<<16 | int(h[1])<<8 | int(h[2])
	return h[3], h[4], binary.BigEndian.Uint32(h[5:9]) & 0x7fffffff, readFull(t, conn, length)
}

// h2Response reads frames until stream 1's response headers and returns
// them, and its body if the stream ends with one.
func h2Response(t *testing.T, conn net.Conn) (map[string]string, string) {
	t.Helper()
	fields := map[string]string{}
	var body []byte
	for {
		typ, flags, stream, payload := readH2Frame(t, conn)
		if stream != 1 {
			continue
		}
		switch typ {
		case h2FrameHeaders:
			if len(fields) == 0 {
				fragment, _ := headersFragment(payload, flags)
				err := hpack.NewDecoder(h2HeaderTableSize).Decode(fragment, func(f hpack.HeaderField) error {
					fields[f.Name] = f.Value
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
			}
		case 0:
			body = append(body, payload...)
		}
		if flags&0x1 != 0 || (typ == h2FrameHeaders && fields["content-type"] == "application/grpc") {
			return fields, string(body)
		}
	}
}

// grpcEcho answers every call with its method and the message it was sent.
func grpcEcho() *grpc.Server {
	type msg struct{ Method, Text string }
	return grpc.NewServer(grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		var in msg
		if err := stream.RecvMsg(&in); err != nil {
			return err
		}
		method, _ := grpc.Method(stream.Context())
		return stream.SendMsg(&msg{Method: method, Text: in.Text})
	}))
}

// wsEcho echoes every WebSocket message.
func wsEcho() fib.Handler {
	return websocket.NewHandler(websocket.HandlerFuncs{
		Message: func(c *websocket.Connection, opcode websocket.Opcode, data []byte) {
			_ = c.WriteMessage(opcode, data)
		},
	})
}

// replying answers every read with reply followed by what was read.
func replying(reply string) fib.Handler {
	return fib.HandlerFuncs{Data: func(c *fib.Connection, b []byte) {
		if c.Send(append([]byte(reply), b...)) != nil {
			c.Close()
		}
	}}
}

// echoTarget is a plain TCP echo server for the tunnels to reach.
func echoTarget(t *testing.T) string {
	t.Helper()
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
	return ln.Addr().String()
}

// splitAddr is addr's IPv4 address and port.
func splitAddr(t *testing.T, addr string) ([]byte, uint16) {
	t.Helper()
	tcp, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	return tcp.IP.To4(), uint16(tcp.Port)
}

// roundTrip sends a megabyte through a tunnel to the echo target, reading
// the echo from r as it goes, and checks it comes back whole.
func roundTrip(t *testing.T, conn net.Conn, r io.Reader) {
	t.Helper()
	want := make([]byte, 1<<20)
	for i := range want {
		want[i] = byte(i * 7)
	}
	sent := make(chan error, 1)
	go func() {
		_, err := conn.Write(want)
		sent <- err
	}()
	got := make([]byte, len(want))
	if _, err := io.ReadFull(r, got); err != nil {
		t.Fatalf("read the echo: %v", err)
	}
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("the echo differs from what was sent")
	}
}

// Every protocol the package knows shares one port, each served by a
// handler of its own, and what none of them is falls back.
func TestOnePortServesEveryProtocol(t *testing.T) {
	serverTLS, clientTLS, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	httpHandler := fibhttp.NewHandler(fibhttp.HandlerFunc(web))
	arpcServer := arpc.NewServer()
	arpcServer.Handler.Handle("/echo", func(ctx *arpc.Context) { ctx.Write(ctx.Body()) })
	addr := serve(t, New(Config{
		TLS:       fibtls.NewServer(fibhttp.ConfigureTLS(serverTLS), httpHandler),
		HTTP1:     httpHandler,
		HTTP2:     httpHandler,
		WebSocket: wsEcho(),
		GRPC:      grpcEcho(),
		SOCKS:     socks.NewServer(socks.Config{}),
		HTTPProxy: fibhttp.NewHandler(fibhttp.NewConnectProxy(fibhttp.ConnectProxyConfig{})),
		ARPC:      arpcServer,
		Fallback:  replying("echo:"),
	}))
	target := echoTarget(t)

	t.Run("HTTP1", func(t *testing.T) {
		client := &stdhttp.Client{Timeout: 10 * time.Second}
		for range 3 {
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
	})

	t.Run("TLS", func(t *testing.T) {
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
	})

	t.Run("WebSocket", func(t *testing.T) {
		conn := dial(t, addr)
		_, err := conn.Write([]byte("GET /ws HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
			"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"))
		if err != nil {
			t.Fatal(err)
		}
		r := bufio.NewReader(conn)
		status, err := r.ReadString('\n')
		if err != nil || !strings.HasPrefix(status, "HTTP/1.1 101") {
			t.Fatalf("handshake answered %q (%v)", status, err)
		}
		for line := ""; line != "\r\n"; {
			if line, err = r.ReadString('\n'); err != nil {
				t.Fatal(err)
			}
		}
		// A masked text frame, which the server echoes unmasked.
		payload, mask := []byte("hello"), []byte{1, 2, 3, 4}
		frame := append([]byte{0x81, 0x80 | byte(len(payload))}, mask...)
		for i, b := range payload {
			frame = append(frame, b^mask[i%4])
		}
		if _, err := conn.Write(frame); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, 2+len(payload))
		if _, err := io.ReadFull(r, got); err != nil || string(got[2:]) != "hello" || got[0] != 0x81 {
			t.Fatalf("echo % x (%v)", got, err)
		}
	})

	t.Run("HTTP2", func(t *testing.T) {
		conn := dial(t, addr)
		block := headerBlock(":method", "GET", ":scheme", "http", ":path", "/h2c", ":authority", "localhost")
		_, err := conn.Write([]byte(h2Preface + string(h2Frame(h2FrameSettings, 0, 0, nil)) +
			string(h2Frame(h2FrameHeaders, h2FlagEndHeaders|0x1, 1, block))))
		if err != nil {
			t.Fatal(err)
		}
		fields, body := h2Response(t, conn)
		if fields[":status"] != "200" || body != "HTTP/2.0 tls=no /h2c" {
			t.Fatalf("h2c answered %v %q", fields, body)
		}
	})

	t.Run("GRPC", func(t *testing.T) {
		cc, err := grpc.NewClient(addr, grpc.WithEngine(startEngine(t, fib.DefaultConfig(), nil)),
			grpc.WithDefaultCallOptions(grpc.CallContentSubtype("json")))
		if err != nil {
			t.Fatal(err)
		}
		defer cc.Close()
		type msg struct{ Method, Text string }
		var out msg
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := cc.Invoke(ctx, "/test.Echo/Say", &msg{Text: "hi"}, &out); err != nil {
			t.Fatal(err)
		}
		if out.Method != "/test.Echo/Say" || out.Text != "hi" {
			t.Fatalf("gRPC answered %+v", out)
		}
	})

	// A client that waits for the server's SETTINGS before its first
	// request, as grpc-go's does, is sent them while the handler decides.
	t.Run("GRPCWaitsForSettings", func(t *testing.T) {
		conn := dial(t, addr)
		if _, err := conn.Write([]byte(h2Preface + string(h2Frame(h2FrameSettings, 0, 0, nil)))); err != nil {
			t.Fatal(err)
		}
		if typ, flags, _, payload := readH2Frame(t, conn); typ != h2FrameSettings || flags != 0 || len(payload) != 0 {
			t.Fatalf("got frame type %d flags %d, want an empty SETTINGS", typ, flags)
		}
		block := headerBlock(":method", "POST", ":scheme", "http", ":path", "/test.Echo/Say", ":authority", "localhost",
			"content-type", "application/grpc+json", "te", "trailers")
		message := []byte(`{"Text":"waited"}`)
		data := append([]byte{0, 0, 0, 0, byte(len(message))}, message...)
		_, err := conn.Write(append(h2Frame(h2FrameSettings, 0x1, 0, nil),
			append(h2Frame(h2FrameHeaders, h2FlagEndHeaders, 1, block), h2Frame(0, 0x1, 1, data)...)...))
		if err != nil {
			t.Fatal(err)
		}
		fields, _ := h2Response(t, conn)
		if fields[":status"] != "200" || fields["content-type"] != "application/grpc+json" {
			t.Fatalf("gRPC answered %v", fields)
		}
	})

	t.Run("SOCKS", func(t *testing.T) {
		conn := dial(t, addr)
		if _, err := conn.Write([]byte{5, 1, 0}); err != nil {
			t.Fatal(err)
		}
		if got := readFull(t, conn, 2); got[0] != 5 || got[1] != 0 {
			t.Fatalf("SOCKS5 greeting answered % x", got)
		}
	})

	// A SOCKS5 CONNECT relays to its destination, both ways.
	t.Run("SOCKSTunnel", func(t *testing.T) {
		conn := dial(t, addr)
		host, port := splitAddr(t, target)
		request := append([]byte{5, 1, 0, 5, 1, 0, 1}, host...)
		if _, err := conn.Write(binary.BigEndian.AppendUint16(request, port)); err != nil {
			t.Fatal(err)
		}
		if got := readFull(t, conn, 2); got[0] != 5 || got[1] != 0 {
			t.Fatalf("SOCKS5 greeting answered % x", got)
		}
		if got := readFull(t, conn, 10); got[1] != 0 {
			t.Fatalf("SOCKS5 CONNECT answered % x", got)
		}
		roundTrip(t, conn, conn)
	})

	// An HTTP CONNECT, as curl -x sends, relays the same way.
	t.Run("HTTPProxyTunnel", func(t *testing.T) {
		conn := dial(t, addr)
		if _, err := conn.Write([]byte("CONNECT " + target + " HTTP/1.1\r\nHost: " + target + "\r\n\r\n")); err != nil {
			t.Fatal(err)
		}
		r := bufio.NewReader(conn)
		status, err := r.ReadString('\n')
		if err != nil || !strings.HasPrefix(status, "HTTP/1.1 200") {
			t.Fatalf("CONNECT answered %q (%v)", status, err)
		}
		for line := ""; line != "\r\n"; {
			if line, err = r.ReadString('\n'); err != nil {
				t.Fatal(err)
			}
		}
		roundTrip(t, conn, r)
	})

	t.Run("ARPC", func(t *testing.T) {
		client, err := arpc.NewClient(arpc.TCPDialer(startEngine(t, fib.DefaultConfig(), nil), "tcp", addr, 3*time.Second), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Stop()
		var rsp string
		if err := client.Call("/echo", "hello", &rsp, 5*time.Second); err != nil || rsp != "hello" {
			t.Fatalf("arpc answered %q (%v)", rsp, err)
		}
	})

	t.Run("Fallback", func(t *testing.T) {
		conn := dial(t, addr)
		if _, err := conn.Write([]byte("hello")); err != nil {
			t.Fatal(err)
		}
		if got := readFull(t, conn, len("echo:hello")); string(got) != "echo:hello" {
			t.Fatalf("fallback answered %q", got)
		}
	})
}

// A protocol that is not served is not taken: without a fallback, its
// connections are closed.
func TestUnservedProtocolsClose(t *testing.T) {
	addr := serve(t, New(Config{WebSocket: wsEcho(), HTTP1: fibhttp.NewHandler(fibhttp.HandlerFunc(web))}))
	for _, first := range []string{h2Preface, "\x05\x01\x00", "\x16\x03\x01\x00\x10"} {
		conn := dial(t, addr)
		if _, err := conn.Write([]byte(first)); err != nil {
			t.Fatal(err)
		}
		if n, err := conn.Read(make([]byte, 1)); err == nil {
			t.Fatalf("% x was answered with %d bytes", first, n)
		}
	}
}
