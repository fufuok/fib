//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package socks

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"net"
	"net/netip"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
)

const testTimeout = 10 * time.Second

// run binds handler with config, or makes an engine that only dials when
// config listens nowhere, and runs it until the test ends.
func run(t *testing.T, config fib.Config, handler fib.Handler, listen bool) *fib.Engine {
	t.Helper()
	config.Name = "socks-" + t.Name()
	var engine *fib.Engine
	var err error
	if listen {
		engine, err = fib.Bind(config, handler)
	} else {
		engine, err = fib.NewEngine(config, handler)
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

// proxy runs a Server with config on 127.0.0.1, with a UDP binding beside
// its TCP one, and returns its address.
func proxy(t *testing.T, config Config) (string, *Server) {
	t.Helper()
	server := NewServer(config)
	engineConfig := fib.DefaultConfig()
	engineConfig.Bindings = []fib.Binding{
		{Addr: "127.0.0.1:0", Handler: server},
		{Network: "udp", Addr: "127.0.0.1:0", Handler: server},
	}
	engine := run(t, engineConfig, nil, true)
	addrs, err := engine.BindingAddrs(0)
	if err != nil {
		t.Fatal(err)
	}
	return addrs[0].String(), server
}

func clientEngine(t *testing.T) *fib.Engine {
	return run(t, fib.DefaultConfig(), nil, false)
}

// echo is a TCP echo server of the net package's, so that what the proxy
// reaches is not fib.
func echo(t *testing.T) string {
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

// udpEcho answers every datagram with "echo:" and the datagram.
func udpEcho(t *testing.T) string {
	t.Helper()
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	go func() {
		buf := make([]byte, 64<<10)
		for {
			n, from, err := conn.ReadFrom(buf)
			if err != nil {
				return
			}
			_, _ = conn.WriteTo(append([]byte("echo:"), buf[:n]...), from)
		}
	}()
	return conn.LocalAddr().String()
}

func dialRaw(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, testTimeout)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetDeadline(time.Now().Add(testTimeout))
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func write(t *testing.T, conn net.Conn, b []byte) {
	t.Helper()
	if _, err := conn.Write(b); err != nil {
		t.Fatal(err)
	}
}

func expect(t *testing.T, conn net.Conn, want []byte) {
	t.Helper()
	got := make([]byte, len(want))
	if _, err := io.ReadFull(conn, got); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("read %x (%v), want %x", got, err, want)
	}
}

func readN(t *testing.T, conn net.Conn, n int) []byte {
	t.Helper()
	got := make([]byte, n)
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("read %x: %v", got, err)
	}
	return got
}

// addr5 is addr as a SOCKS5 address.
func addr5(t *testing.T, addr string) []byte {
	t.Helper()
	host, port, err := splitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	b, err := appendAddr(nil, host, port)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// sink collects what a connection a test dials receives.
type sink struct {
	mu     sync.Mutex
	data   []byte
	got    chan struct{}
	closed chan error
	opened chan *fib.Connection
}

func newSink() *sink {
	return &sink{got: make(chan struct{}, 1), closed: make(chan error, 1), opened: make(chan *fib.Connection, 1)}
}

func (s *sink) OnOpen(c *fib.Connection) { s.opened <- c }
func (s *sink) OnData(_ *fib.Connection, b []byte) {
	s.mu.Lock()
	s.data = append(s.data, b...)
	s.mu.Unlock()
	select {
	case s.got <- struct{}{}:
	default:
	}
}
func (s *sink) OnPriorityData(*fib.Connection, []byte) {}
func (s *sink) OnClose(_ *fib.Connection, err error)   { s.closed <- err }

// wait waits until what has arrived is want.
func (s *sink) wait(t *testing.T, want []byte) {
	t.Helper()
	deadline := time.After(testTimeout)
	for {
		s.mu.Lock()
		got := append([]byte(nil), s.data...)
		s.mu.Unlock()
		if bytes.Equal(got, want) {
			return
		}
		if len(got) >= len(want) {
			t.Fatalf("got %q, want %q", got, want)
		}
		select {
		case <-s.got:
		case <-deadline:
			t.Fatalf("got %q, still waiting for %q", got, want)
		}
	}
}

type dialResult struct {
	c   *fib.Connection
	err error
}

func dialed() (chan dialResult, func(*fib.Connection, error)) {
	ch := make(chan dialResult, 1)
	return ch, func(c *fib.Connection, err error) { ch <- dialResult{c, err} }
}

func wait[T any](t *testing.T, ch chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(testTimeout):
		t.Fatal("timed out")
		panic("unreachable")
	}
}

// A SOCKS5 client of its own reaches an echo server through the proxy, with
// what it sends pipelined behind its request.
func TestServerConnect5(t *testing.T) {
	proxyAddr, _ := proxy(t, Config{})
	target := echo(t)
	conn := dialRaw(t, proxyAddr)
	req := append([]byte{5, 1, 0, 5, 1, 0}, addr5(t, target)...)
	write(t, conn, append(req, "early"...))
	expect(t, conn, []byte{5, 0})
	reply := readN(t, conn, 10)
	if reply[0] != 5 || reply[1] != 0 || reply[3] != atypIPv4 {
		t.Fatalf("reply %x", reply)
	}
	expect(t, conn, []byte("early"))
	write(t, conn, []byte("later"))
	expect(t, conn, []byte("later"))
}

func TestServerConnectByName(t *testing.T) {
	proxyAddr, _ := proxy(t, Config{})
	_, port, _ := net.SplitHostPort(echo(t))
	conn := dialRaw(t, proxyAddr)
	write(t, conn, []byte{5, 1, 0})
	expect(t, conn, []byte{5, 0})
	write(t, conn, append([]byte{5, 1, 0}, addr5(t, "localhost:"+port)...))
	reply := readN(t, conn, 4)
	if reply[1] != 0 {
		t.Fatalf("reply %x", reply)
	}
	if reply[3] == atypIPv4 {
		readN(t, conn, 6)
	} else {
		readN(t, conn, 18)
	}
	write(t, conn, []byte("hi"))
	expect(t, conn, []byte("hi"))
}

func TestServerLogin(t *testing.T) {
	proxyAddr, _ := proxy(t, Config{Authenticate: func(u, p string) bool { return u == "user" && p == "pass" }})
	target := echo(t)

	conn := dialRaw(t, proxyAddr)
	write(t, conn, []byte{5, 2, 0, 2})
	expect(t, conn, []byte{5, 2})
	write(t, conn, []byte{1, 4, 'u', 's', 'e', 'r', 4, 'p', 'a', 's', 's'})
	expect(t, conn, []byte{1, 0})
	write(t, conn, append([]byte{5, 1, 0}, addr5(t, target)...))
	readN(t, conn, 10)
	write(t, conn, []byte("x"))
	expect(t, conn, []byte("x"))

	bad := dialRaw(t, proxyAddr)
	write(t, bad, []byte{5, 1, 2, 1, 4, 'u', 's', 'e', 'r', 4, 'n', 'o', 'p', 'e'})
	expect(t, bad, []byte{5, 2, 1, 1})
	if _, err := bad.Read(make([]byte, 1)); err == nil {
		t.Fatal("connection open after a failed login")
	}

	none := dialRaw(t, proxyAddr)
	write(t, none, []byte{5, 1, 0})
	expect(t, none, []byte{5, 0xff})

	// SOCKS4 has no password to check.
	v4 := dialRaw(t, proxyAddr)
	host, port, _ := splitHostPort(target)
	ip := netip.MustParseAddr(host).As4()
	write(t, v4, append([]byte{4, 1, byte(port >> 8), byte(port), ip[0], ip[1], ip[2], ip[3]}, "user\x00"...))
	if reply := readN(t, v4, 8); reply[1] != reply4Rejected {
		t.Fatalf("SOCKS4 reply %x with Authenticate set", reply)
	}
}

func TestServerConnect4(t *testing.T) {
	proxyAddr, _ := proxy(t, Config{})
	target := echo(t)
	host, port, _ := splitHostPort(target)
	ip := netip.MustParseAddr(host).As4()

	conn := dialRaw(t, proxyAddr)
	write(t, conn, append([]byte{4, 1, byte(port >> 8), byte(port), ip[0], ip[1], ip[2], ip[3]}, "me\x00early"...))
	if reply := readN(t, conn, 8); reply[0] != 0 || reply[1] != reply4Granted {
		t.Fatalf("reply %x", reply)
	}
	expect(t, conn, []byte("early"))

	// 4a: 0.0.0.1 and the name after the user ID.
	conn = dialRaw(t, proxyAddr)
	write(t, conn, append([]byte{4, 1, byte(port >> 8), byte(port), 0, 0, 0, 1}, "\x00localhost\x00"...))
	if reply := readN(t, conn, 8); reply[1] != reply4Granted {
		t.Fatalf("4a reply %x", reply)
	}
	write(t, conn, []byte("4a"))
	expect(t, conn, []byte("4a"))
}

func TestServerRefuses(t *testing.T) {
	refused := errors.New("no")
	proxyAddr, _ := proxy(t, Config{Allow: func(r *Request) error {
		switch r.Addr {
		case "10.0.0.1:1":
			return refused
		case "10.0.0.2:2":
			return &ReplyError{Code: ReplyNetworkUnreachable}
		}
		return nil
	}})
	// A port nothing listens on.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closedAddr := ln.Addr().String()
	_ = ln.Close()

	for _, tc := range []struct {
		addr string
		cmd  byte
		code byte
	}{
		{"10.0.0.1:1", 1, ReplyNotAllowed},
		{"10.0.0.2:2", 1, ReplyNetworkUnreachable},
		{closedAddr, 1, ReplyConnectionRefused},
		{closedAddr, 9, ReplyCommandNotSupported},
	} {
		conn := dialRaw(t, proxyAddr)
		write(t, conn, append([]byte{5, 1, 0, 5, tc.cmd, 0}, addr5(t, tc.addr)...))
		expect(t, conn, []byte{5, 0})
		if reply := readN(t, conn, 2); reply[1] != tc.code {
			t.Errorf("%s cmd %d: reply %x, want code %d", tc.addr, tc.cmd, reply, tc.code)
		}
	}

	conn := dialRaw(t, proxyAddr)
	write(t, conn, []byte{5, 1, 0, 5, 1, 0, 9})
	expect(t, conn, []byte{5, 0})
	if reply := readN(t, conn, 2); reply[1] != ReplyAddressNotSupported {
		t.Errorf("unknown address type: reply %x", reply)
	}

	conn = dialRaw(t, proxyAddr)
	write(t, conn, []byte("GET / HTTP/1.1\r\n\r\n"))
	if n, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatalf("read %d bytes from a proxy spoken HTTP to", n)
	}
}

func TestServerHandshakeTimeout(t *testing.T) {
	proxyAddr, _ := proxy(t, Config{HandshakeTimeout: 50 * time.Millisecond})
	conn := dialRaw(t, proxyAddr)
	write(t, conn, []byte{5})
	start := time.Now()
	if _, err := conn.Read(make([]byte, 1)); err == nil || time.Since(start) > testTimeout/2 {
		t.Fatalf("read %v after %v", err, time.Since(start))
	}
}

// fakeProxy accepts one connection, checks that the client sends each of
// script's exchanges' want in turn, and answers each with its reply.
func fakeProxy(t *testing.T, script ...[2][]byte) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(testTimeout))
		for _, step := range script {
			got := make([]byte, len(step[0]))
			if _, err := io.ReadFull(conn, got); err != nil || !bytes.Equal(got, step[0]) {
				t.Errorf("proxy read %x (%v), want %x", got, err, step[0])
				return
			}
			if _, err := conn.Write(step[1]); err != nil {
				return
			}
		}
		_, _ = io.Copy(conn, conn)
	}()
	return ln.Addr().String()
}

// The dialer sends exactly what RFC 1928, RFC 1929 and SOCKS4a call for,
// and what the destination sends with the reply reaches the handler.
func TestDialerBytes(t *testing.T) {
	engine := clientEngine(t)
	for _, tc := range []struct {
		name   string
		dialer Dialer
		addr   string
		script [][2][]byte
	}{
		{"socks5h", Dialer{}, "example.com:80", [][2][]byte{
			{{5, 1, 0}, {5, 0}},
			{append([]byte{5, 1, 0, 3, 11}, "example.com\x00\x50"...), {5, 0, 0, 1, 1, 2, 3, 4, 0, 1, 'h', 'i'}},
		}},
		{"login", Dialer{Username: "u", Password: "pw"}, "[2001:db8::1]:443", [][2][]byte{
			{{5, 2, 0, 2}, {5, 2}},
			{{1, 1, 'u', 2, 'p', 'w'}, {1, 0}},
			{append([]byte{5, 1, 0, 4, 0x20, 1, 0xd, 0xb8, 19: 1}, 1, 187), append([]byte{5, 0, 0, 3, 1, 'x', 0, 1}, "hi"...)},
		}},
		{"socks4", Dialer{Version: Version4, Username: "id"}, "1.2.3.4:80", [][2][]byte{
			{{4, 1, 0, 80, 1, 2, 3, 4, 'i', 'd', 0}, {0, 0x5a, 0, 0, 0, 0, 0, 0, 'h', 'i'}},
		}},
		{"socks4a", Dialer{Version: Version4A}, "example.com:80", [][2][]byte{
			{append([]byte{4, 1, 0, 80, 0, 0, 0, 1, 0}, "example.com\x00"...), {0, 0x5a, 0, 0, 0, 0, 0, 0, 'h', 'i'}},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := tc.dialer
			d.Addr = fakeProxy(t, tc.script...)
			s := newSink()
			ch, done := dialed()
			if err := d.Dial(engine, "tcp", tc.addr, testTimeout, s, done); err != nil {
				t.Fatal(err)
			}
			r := wait(t, ch)
			if r.err != nil {
				t.Fatal(r.err)
			}
			s.wait(t, []byte("hi"))
			_ = r.c.Send([]byte("!"))
			s.wait(t, []byte("hi!"))
			r.c.Close()
		})
	}
}

func TestDialerFailures(t *testing.T) {
	engine := clientEngine(t)
	for _, tc := range []struct {
		name   string
		dialer Dialer
		script [][2][]byte
		want   error
	}{
		{"no method", Dialer{}, [][2][]byte{{{5, 1, 0}, {5, 0xff}}}, ErrNoAcceptableMethod},
		{"login", Dialer{Username: "u"}, [][2][]byte{{{5, 2, 0, 2}, {5, 2}}, {{1, 1, 'u', 0}, {1, 1}}}, ErrAuthFailed},
		{"refused", Dialer{}, [][2][]byte{{{5, 1, 0}, {5, 0}},
			{{5, 1, 0, 1, 1, 2, 3, 4, 0, 80}, {5, 5, 0, 1, 0, 0, 0, 0, 0, 0}}},
			&ReplyError{Code: ReplyConnectionRefused}},
		{"socks4 rejected", Dialer{Version: Version4}, [][2][]byte{
			{{4, 1, 0, 80, 1, 2, 3, 4, 0}, {0, 0x5b, 0, 0, 0, 0, 0, 0}}}, &ReplyError{Version: Version4, Code: 0x5b}},
		{"not socks", Dialer{}, [][2][]byte{{{5, 1, 0}, []byte("HTTP/1.1 400 Bad\r\n")}}, ErrMalformed},
		{"closed", Dialer{}, [][2][]byte{{{5, 1, 0}, nil}}, io.ErrUnexpectedEOF},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := tc.dialer
			script := tc.script
			if tc.want == io.ErrUnexpectedEOF {
				d.Addr = closingProxy(t)
			} else {
				d.Addr = fakeProxy(t, script...)
			}
			ch, done := dialed()
			if err := d.Dial(engine, "tcp", "1.2.3.4:80", testTimeout, newSink(), done); err != nil {
				t.Fatal(err)
			}
			r := wait(t, ch)
			var replyErr *ReplyError
			if want, ok := tc.want.(*ReplyError); ok {
				if !errors.As(r.err, &replyErr) || *replyErr != *want {
					t.Fatalf("error %v, want %v", r.err, want)
				}
			} else if !errors.Is(r.err, tc.want) {
				t.Fatalf("error %v, want %v", r.err, tc.want)
			}
			var opErr *net.OpError
			if !errors.As(r.err, &opErr) || opErr.Addr.String() != "1.2.3.4:80" {
				t.Fatalf("error %#v is not a *net.OpError naming the destination", r.err)
			}
		})
	}

	d := Dialer{Addr: "127.0.0.1:1"}
	if err := d.Dial(engine, "tcp", "no port", 0, nil, nil); err == nil {
		t.Error("an address without a port dialed")
	}
	if err := (&Dialer{Version: Version4}).Dial(engine, "udp", "1.2.3.4:53", 0, nil, nil); err == nil {
		t.Error("SOCKS4 dialed UDP")
	}
	if err := (&Dialer{Version: Version4A}).Dial(engine, "tcp", "[::1]:80", 0, nil, nil); err == nil {
		t.Error("SOCKS4a dialed IPv6")
	}
	if err := d.Dial(engine, "unixgram", "x:1", 0, nil, nil); err == nil {
		t.Error("an unknown network dialed")
	}
}

// closingProxy reads a greeting and hangs up.
func closingProxy(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err == nil {
			_, _ = conn.Read(make([]byte, 3))
			_ = conn.Close()
		}
	}()
	return ln.Addr().String()
}

func TestDialerTimeout(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	// A proxy that never answers.
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { _ = conn.Close() })
		}
	}()
	d := Dialer{Addr: ln.Addr().String()}
	ch, done := dialed()
	start := time.Now()
	if err := d.Dial(clientEngine(t), "tcp", "1.2.3.4:80", 100*time.Millisecond, newSink(), done); err != nil {
		t.Fatal(err)
	}
	if r := wait(t, ch); r.err == nil || time.Since(start) > testTimeout/2 {
		t.Fatalf("dial: %v after %v", r.err, time.Since(start))
	}
}

func TestParseURL(t *testing.T) {
	for _, tc := range []struct {
		url  string
		want Dialer
	}{
		{"socks5://h", Dialer{Addr: "h:1080", ResolveLocally: true}},
		{"socks5h://u:p@h:9", Dialer{Addr: "h:9", Username: "u", Password: "p"}},
		{"socks4://id@[::1]:9", Dialer{Addr: "[::1]:9", Version: Version4, Username: "id"}},
		{"socks4a://h:9", Dialer{Addr: "h:9", Version: Version4A}},
	} {
		d, err := ParseURL(tc.url)
		if err != nil || !reflect.DeepEqual(*d, tc.want) {
			t.Errorf("ParseURL(%q) = %+v, %v, want %+v", tc.url, d, err, tc.want)
		}
	}
	for _, bad := range []string{"http://h:1", "socks5://", "::"} {
		if _, err := ParseURL(bad); err == nil {
			t.Errorf("ParseURL(%q) succeeded", bad)
		}
	}
}

// The dialer and the server, against each other, in every version.
func TestDialThroughServer(t *testing.T) {
	proxyAddr, _ := proxy(t, Config{Authenticate: func(u, p string) bool { return u == "u" && p == "p" }})
	open, _ := proxy(t, Config{})
	engine := clientEngine(t)
	target := echo(t)
	_, port, _ := net.SplitHostPort(target)
	for _, tc := range []struct {
		name string
		url  string
		addr string
	}{
		{"socks5", "socks5://u:p@" + proxyAddr, "localhost:" + port},
		{"socks5h", "socks5h://u:p@" + proxyAddr, "localhost:" + port},
		{"socks4", "socks4://" + open, "localhost:" + port},
		{"socks4a", "socks4a://" + open, "localhost:" + port},
		{"ip", "socks5h://" + open, target},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, err := ParseURL(tc.url)
			if err != nil {
				t.Fatal(err)
			}
			s := newSink()
			ch, done := dialed()
			if err := d.Dial(engine, "tcp", tc.addr, testTimeout, s, done); err != nil {
				t.Fatal(err)
			}
			r := wait(t, ch)
			if r.err != nil {
				t.Fatal(r.err)
			}
			if c := wait(t, s.opened); c != r.c {
				t.Fatal("OnOpen and done saw different connections")
			}
			_ = r.c.Send([]byte("ping"))
			s.wait(t, []byte("ping"))
			r.c.Close()
			wait(t, s.closed)
		})
	}
	d := Dialer{Addr: proxyAddr, Username: "u", Password: "wrong"}
	ch, done := dialed()
	_ = d.Dial(engine, "tcp", target, testTimeout, newSink(), done)
	if r := wait(t, ch); !errors.Is(r.err, ErrAuthFailed) {
		t.Fatalf("dial with a wrong password: %v", r.err)
	}
}

// A transfer far larger than the relay may queue arrives whole, both ways.
func TestRelayBackpressure(t *testing.T) {
	proxyAddr, _ := proxy(t, Config{MaxQueuedBytes: 4 << 10})
	target := echo(t)
	conn := dialRaw(t, proxyAddr)
	write(t, conn, append([]byte{5, 1, 0, 5, 1, 0}, addr5(t, target)...))
	readN(t, conn, 12)
	payload := make([]byte, 16<<20)
	_, _ = rand.Read(payload)
	errs := make(chan error, 1)
	go func() {
		_, err := conn.Write(payload)
		errs <- err
	}()
	got := make([]byte, len(payload))
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatal(err)
	}
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("payload corrupted on the way")
	}
}

// When the destination closes, the client is closed once what the
// destination sent has reached it.
func TestRelayCloses(t *testing.T) {
	proxyAddr, _ := proxy(t, Config{})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err == nil {
			_, _ = conn.Write([]byte("bye"))
			_ = conn.Close()
		}
	}()
	conn := dialRaw(t, proxyAddr)
	write(t, conn, append([]byte{5, 1, 0, 5, 1, 0}, addr5(t, ln.Addr().String())...))
	readN(t, conn, 12)
	expect(t, conn, []byte("bye"))
	if _, err := conn.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("read after the destination closed: %v", err)
	}
}

func TestBindThroughServer(t *testing.T) {
	proxyAddr, _ := proxy(t, Config{})
	engine := clientEngine(t)
	for _, version := range []Version{Version5, Version4} {
		t.Run(version.String(), func(t *testing.T) {
			d := Dialer{Addr: proxyAddr, Version: version}
			s := newSink()
			listening := make(chan net.Addr, 1)
			ch, done := dialed()
			err := d.Bind(engine, "127.0.0.1:0", testTimeout, s, func(a net.Addr) { listening <- a }, done)
			if err != nil {
				t.Fatal(err)
			}
			addr := wait(t, listening)
			peer := dialRaw(t, addr.String())
			r := wait(t, ch)
			if r.err != nil {
				t.Fatal(r.err)
			}
			write(t, peer, []byte("from peer"))
			s.wait(t, []byte("from peer"))
			_ = r.c.Send([]byte("to peer"))
			expect(t, peer, []byte("to peer"))
			_ = peer.Close()
			wait(t, s.closed)
		})
	}
}

func TestBindRefusesOtherPeers(t *testing.T) {
	proxyAddr, _ := proxy(t, Config{BindTimeout: 300 * time.Millisecond})
	conn := dialRaw(t, proxyAddr)
	write(t, conn, append([]byte{5, 1, 0, 5, 2, 0}, addr5(t, "10.9.9.9:0")...))
	readN(t, conn, 2)
	reply := readN(t, conn, 10)
	port := int(reply[8])<<8 | int(reply[9])
	stranger := dialRaw(t, net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if _, err := stranger.Read(make([]byte, 1)); err == nil {
		t.Fatal("a peer the client did not name was relayed")
	}
	if reply := readN(t, conn, 2); reply[1] != ReplyHostUnreachable {
		t.Fatalf("second reply %x after the timeout", reply)
	}
}

func TestUDPThroughServer(t *testing.T) {
	proxyAddr, _ := proxy(t, Config{Allow: func(r *Request) error {
		if r.Command == CommandUDPAssociate && r.Addr == "127.0.0.1:9" {
			return errors.New("not there")
		}
		return nil
	}})
	target := udpEcho(t)
	engine := clientEngine(t)
	d := Dialer{Addr: proxyAddr}
	s := newSink()
	ch, done := dialed()
	if err := d.Dial(engine, "udp", target, testTimeout, s, done); err != nil {
		t.Fatal(err)
	}
	r := wait(t, ch)
	if r.err != nil {
		t.Fatal(r.err)
	}
	if !r.c.IsUDP() {
		t.Fatal("not a UDP connection")
	}
	// Datagrams can be lost, even on loopback, so the first is sent until
	// it is answered.
	deadline := time.Now().Add(testTimeout)
	for {
		_ = r.c.Send([]byte("one"))
		select {
		case <-s.got:
		case <-time.After(100 * time.Millisecond):
			if time.Now().After(deadline) {
				t.Fatal("no answer through the association")
			}
			continue
		}
		break
	}
	s.mu.Lock()
	got := string(s.data)
	s.mu.Unlock()
	if !strings.HasPrefix(got, "echo:one") {
		t.Fatalf("got %q", got)
	}

	// The same association, spoken by hand, to a destination Allow refuses
	// and one it does not.
	conn := dialRaw(t, proxyAddr)
	write(t, conn, append([]byte{5, 1, 0, 5, 3, 0}, addr5(t, "0.0.0.0:0")...))
	readN(t, conn, 2)
	reply := readN(t, conn, 10)
	if reply[1] != 0 {
		t.Fatalf("reply %x", reply)
	}
	relay := net.UDPAddr{IP: net.IP(reply[4:8]), Port: int(reply[8])<<8 | int(reply[9])}
	udp, err := net.DialUDP("udp", nil, &relay)
	if err != nil {
		t.Fatal(err)
	}
	defer udp.Close()
	refused := append(append([]byte{0, 0, 0}, addr5(t, "127.0.0.1:9")...), "lost"...)
	header := append([]byte{0, 0, 0}, addr5(t, target)...)
	buf := make([]byte, 1500)
	for {
		_, _ = udp.Write(refused)
		_, _ = udp.Write(append(header, "two"...))
		_ = udp.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		n, err := udp.Read(buf)
		if err == nil {
			if want := append(header, "echo:two"...); !bytes.Equal(buf[:n], want) {
				t.Fatalf("got %x, want %x", buf[:n], want)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no answer through the association")
		}
	}

	// The association ends with its control connection.
	r.c.Close()
	wait(t, s.closed)
}

// relayAddr names the address the client reached the proxy at, and turns
// UDP associations away on an engine without UDP.
func TestUDPWithoutUDPBinding(t *testing.T) {
	server := NewServer(Config{})
	config := fib.DefaultConfig()
	config.Addr = "127.0.0.1:0"
	engine := run(t, config, server, true)
	addr, err := engine.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	conn := dialRaw(t, addr.String())
	write(t, conn, append([]byte{5, 1, 0, 5, 3, 0}, addr5(t, "0.0.0.0:0")...))
	readN(t, conn, 2)
	if reply := readN(t, conn, 2); reply[1] != ReplyCommandNotSupported {
		t.Fatalf("reply %x", reply)
	}
}

// A server chained to another through Config.Dial.
func TestChainedServers(t *testing.T) {
	upstream, _ := proxy(t, Config{})
	d := &Dialer{Addr: upstream}
	front, _ := proxy(t, Config{Dial: d.Dial})
	target := echo(t)
	conn := dialRaw(t, front)
	write(t, conn, append([]byte{5, 1, 0, 5, 1, 0}, addr5(t, target)...))
	readN(t, conn, 2)
	if reply := readN(t, conn, 10); reply[1] != 0 {
		t.Fatalf("reply %x", reply)
	}
	write(t, conn, []byte("chained"))
	expect(t, conn, []byte("chained"))
}
