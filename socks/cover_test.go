//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package socks

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
)

// The paths the tests of the protocol do not reach: edges of the wire
// format, a peer that sends a message a piece at a time or not as the
// protocol has it, and the races between a request's callbacks, which a
// gated DialFunc puts in a known order.

// scripted accepts one connection, and runs fn on it.
func scripted(t *testing.T, fn func(conn net.Conn)) string {
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
		fn(conn)
	}()
	return ln.Addr().String()
}

// trickle writes b a byte at a time, so that the reader sees it arrive in
// pieces.
func trickle(conn net.Conn, b []byte) {
	for i := range b {
		if _, err := conn.Write(b[i : i+1]); err != nil {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func skip(conn net.Conn, n int) bool {
	_, err := io.ReadFull(conn, make([]byte, n))
	return err == nil
}

// dialVia dials addr through d and waits for the outcome.
func dialVia(t *testing.T, engine *fib.Engine, d *Dialer, network, addr string, h fib.Handler) dialResult {
	t.Helper()
	ch, done := dialed()
	if err := d.Dial(engine, network, addr, testTimeout, h, done); err != nil {
		return dialResult{err: err}
	}
	return wait(t, ch)
}

// fibConn dials addr on engine, without a proxy.
func fibConn(t *testing.T, engine *fib.Engine, network, addr string, h fib.Handler) *fib.Connection {
	t.Helper()
	ch, done := dialed()
	if err := engine.DialWithHandler(network, addr, testTimeout, h, done); err != nil {
		t.Fatal(err)
	}
	r := wait(t, ch)
	if r.err != nil {
		t.Fatal(r.err)
	}
	t.Cleanup(func() { r.c.Close() })
	return r.c
}

// gate is a DialFunc that waits to be opened before it dials, and dials
// with DialDirect then, or fails as fail says.
type gate struct {
	open   chan struct{}
	dialed chan string
	fail   func(network string) (sync, async error)
}

func newGate() *gate {
	return &gate{open: make(chan struct{}), dialed: make(chan string, 64)}
}

func (g *gate) dial(engine *fib.Engine, network, addr string, timeout time.Duration,
	handler fib.Handler, done func(*fib.Connection, error)) error {
	g.dialed <- addr
	if g.fail != nil {
		syncErr, asyncErr := g.fail(network)
		if syncErr != nil {
			return syncErr
		}
		if asyncErr != nil {
			go done(nil, asyncErr)
			return nil
		}
	}
	go func() {
		<-g.open
		if err := DialDirect(engine, network, addr, timeout, handler, done); err != nil {
			done(nil, err)
		}
	}()
	return nil
}

// proxyWith is proxy on an engine configured by configure.
func proxyWith(t *testing.T, configure func(*fib.Config), config Config) (string, *Server) {
	t.Helper()
	server := NewServer(config)
	engineConfig := fib.DefaultConfig()
	configure(&engineConfig)
	engine := run(t, engineConfig,
		fib.Binding{Addr: "127.0.0.1:0", Handler: server},
		fib.Binding{Network: "udp", Addr: "127.0.0.1:0", Handler: server},
	)
	addrs, err := engine.BindingAddrs(0)
	if err != nil {
		t.Fatal(err)
	}
	return addrs[0].String(), server
}

// connect5 sends a SOCKS5 CONNECT to addr without logging in, and reads
// the method the server picked.
func connect5(t *testing.T, conn net.Conn, cmd byte, addr string) {
	t.Helper()
	write(t, conn, append([]byte{5, 1, 0, 5, cmd, 0}, addr5(t, addr)...))
	expect(t, conn, []byte{5, 0})
}

func eof(t *testing.T, conn net.Conn) {
	t.Helper()
	if n, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatalf("read %d bytes from a connection that should have closed", n)
	}
}

func TestWireFormatEdges(t *testing.T) {
	for v, want := range map[Version]string{Version5: "SOCKS5", Version4: "SOCKS4", Version4A: "SOCKS4a", 9: "SOCKS(9)"} {
		if v.String() != want {
			t.Errorf("Version %d is %q", v, v.String())
		}
	}
	for c, want := range map[Command]string{CommandConnect: "connect", CommandBind: "bind",
		CommandUDPAssociate: "udp associate", 9: "command(9)"} {
		if c.String() != want {
			t.Errorf("Command %d is %q", c, c.String())
		}
	}
	for code := byte(1); code <= 9; code++ {
		if text := (&ReplyError{Code: code}).Error(); !strings.HasPrefix(text, "socks: ") {
			t.Errorf("code %d: %q", code, text)
		}
	}
	for _, code := range []byte{0x5b, 0x5c, 0x5d, 0x99} {
		if text := (&ReplyError{Version: Version4, Code: code}).Error(); !strings.HasPrefix(text, "socks: ") {
			t.Errorf("SOCKS4 code %#x: %q", code, text)
		}
	}

	for _, host := range []string{"", strings.Repeat("h", 256)} {
		if _, err := appendAddr(nil, host, 1); err == nil {
			t.Errorf("host of %d bytes encoded", len(host))
		}
	}
	whole := map[byte][]byte{
		atypIPv4:   {atypIPv4, 1, 2, 3, 4, 0, 80},
		atypIPv6:   append(append([]byte{atypIPv6}, make([]byte, 16)...), 0, 80),
		atypDomain: {atypDomain, 1, 'h', 0, 80},
	}
	for atyp, b := range whole {
		for i := 0; i < len(b); i++ {
			if _, _, n, err := parseAddr(b[:i]); n != 0 || err != nil {
				t.Errorf("atyp %d: %d of %d bytes parsed as %d, %v", atyp, i, len(b), n, err)
			}
		}
		if _, port, n, err := parseAddr(b); n != len(b) || port != 80 || err != nil {
			t.Errorf("atyp %d: parsed %d bytes, port %d, %v", atyp, n, port, err)
		}
	}
	if _, _, _, err := parseAddr([]byte{atypDomain, 0, 0, 80}); err != ErrMalformed {
		t.Errorf("empty host name: %v", err)
	}

	if _, _, err := splitHostPort("h:65536"); err == nil {
		t.Error("port 65536 split")
	}
	if host, port, err := splitHostPort("[fe80::1%lo0]:80"); host != "fe80::1" || port != 80 || err != nil {
		t.Errorf("zoned address split as %q %d %v", host, port, err)
	}

	for err, want := range map[error]byte{
		&ReplyError{Code: ReplyTTLExpired}:            ReplyTTLExpired,
		syscall.ECONNREFUSED:                          ReplyConnectionRefused,
		syscall.ENETUNREACH:                           ReplyNetworkUnreachable,
		syscall.EHOSTUNREACH:                          ReplyHostUnreachable,
		&net.DNSError{Err: "no such host", Name: "x"}: ReplyHostUnreachable,
		os.ErrDeadlineExceeded:                        ReplyHostUnreachable,
		timeoutError{}:                                ReplyHostUnreachable,
		errors.New("other"):                           ReplyGeneralFailure,
	} {
		if got := replyCode(err); got != want {
			t.Errorf("replyCode(%v) = %d, want %d", err, got, want)
		}
	}
	if (socksAddr{"udp", "x:1"}).Network() != "udp" {
		t.Error("socksAddr names the wrong network")
	}
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

// The callbacks no connection reaches in the order the protocol runs: out
// of band bytes, and the OnOpen of a handler a connection is handed to
// already open.
func TestCallbacksThatDoNothing(t *testing.T) {
	(&clientConn{}).OnPriorityData(nil, nil)
	(&udpAssociation{handler: fib.HandlerFuncs{}}).OnPriorityData(nil, nil)
	associationControl{}.OnOpen(nil)
	associationControl{}.OnData(nil, nil)
	associationControl{}.OnPriorityData(nil, nil)
	(&tunnelClient{}).OnOpen(nil)
	(&tunnelClient{}).OnPriorityData(nil, nil)
	(&tunnelTarget{}).OnPriorityData(nil, nil)
	(&Server{}).OnPriorityData(nil, nil)
	(&streamPipe{}).OnOpen(nil)
	(&streamPipe{}).OnPriorityData(nil, nil)
	(&udpTargetHandler{}).OnPriorityData(nil, nil)
}

func TestDialerRefusesAtOnce(t *testing.T) {
	engine := clientEngine(t)
	long := strings.Repeat("x", 256)
	refused := errors.New("refused")
	for name, tc := range map[string]struct {
		d    Dialer
		addr string
	}{
		"no host":       {Dialer{}, ":80"},
		"long username": {Dialer{Username: long}, "h:80"},
		"long host":     {Dialer{}, long + ":80"},
		"socks4 ipv6":   {Dialer{Version: Version4}, "[::1]:80"},
		"forward":       {Dialer{Forward: failing(refused, nil)}, "1.2.3.4:80"},
	} {
		if err := tc.d.Dial(engine, "", tc.addr, 0, nil, nil); err == nil {
			t.Errorf("%s: dialed", name)
		}
	}
}

// failing is a DialFunc that fails at once with syncErr, or else later with
// asyncErr.
func failing(syncErr, asyncErr error) DialFunc {
	return func(_ *fib.Engine, _, _ string, _ time.Duration, _ fib.Handler, done func(*fib.Connection, error)) error {
		if syncErr != nil {
			return syncErr
		}
		go done(nil, asyncErr)
		return nil
	}
}

func TestDialerFailsLater(t *testing.T) {
	engine := clientEngine(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closed := ln.Addr().String()
	_ = ln.Close()
	refused := errors.New("refused")
	for name, tc := range map[string]struct {
		d    Dialer
		addr string
		want error
	}{
		// The proxy is not there.
		"connect": {Dialer{Addr: closed}, "1.2.3.4:80", syscall.ECONNREFUSED},
		// Resolving locally fails, and so does dialing the proxy after it.
		"lookup":  {Dialer{Addr: closed, ResolveLocally: true}, "no-such-host.invalid:80", nil},
		"forward": {Dialer{Version: Version4, Forward: failing(refused, nil)}, "localhost:80", refused},
	} {
		r := dialVia(t, engine, &tc.d, "tcp", tc.addr, newSink())
		if r.err == nil || tc.want != nil && !errors.Is(r.err, tc.want) {
			t.Errorf("%s: %v, want %v", name, r.err, tc.want)
		}
	}
}

// The client takes every answer of the proxy in pieces as well as whole.
func TestDialerReadsAnswersInPieces(t *testing.T) {
	engine := clientEngine(t)
	for name, tc := range map[string]struct {
		d     Dialer
		proxy func(net.Conn)
	}{
		"socks5": {Dialer{Username: "u", Password: "p"}, func(conn net.Conn) {
			skip(conn, 4)
			trickle(conn, []byte{5, 2})
			skip(conn, 5)
			trickle(conn, []byte{1, 0})
			skip(conn, 10)
			trickle(conn, []byte{5, 0, 0, atypDomain, 1, 'h', 0, 1, 'o', 'k'})
		}},
		"socks4": {Dialer{Version: Version4}, func(conn net.Conn) {
			skip(conn, 9)
			trickle(conn, []byte{4, 0x5a, 0, 0, 0, 0, 0, 0, 'o', 'k'})
		}},
	} {
		t.Run(name, func(t *testing.T) {
			d := tc.d
			d.Addr = scripted(t, func(conn net.Conn) {
				tc.proxy(conn)
				_, _ = io.Copy(io.Discard, conn)
			})
			s := newSink()
			r := dialVia(t, engine, &d, "tcp", "1.2.3.4:80", s)
			if r.err != nil {
				t.Fatal(r.err)
			}
			s.wait(t, []byte("ok"))
		})
	}
}

func TestDialerRefusesBadAnswers(t *testing.T) {
	engine := clientEngine(t)
	for name, tc := range map[string]struct {
		d      Dialer
		answer []byte
		want   error
	}{
		"socks4 version":    {Dialer{Version: Version4}, []byte{9, 0x5a, 0, 0, 0, 0, 0, 0}, ErrMalformed},
		"method version":    {Dialer{}, []byte{4, 0}, ErrMalformed},
		"unoffered login":   {Dialer{}, []byte{5, 2}, ErrMalformed},
		"unknown method":    {Dialer{}, []byte{5, 0x80}, ErrMalformed},
		"reply version":     {Dialer{}, []byte{5, 0, 4, 0, 0, 1}, ErrMalformed},
		"reply address":     {Dialer{}, []byte{5, 0, 5, 0, 0, 9}, &ReplyError{Code: ReplyAddressNotSupported}},
		"udp relay by name": {Dialer{}, []byte{5, 0, 5, 0, 0, atypDomain, 1, 'h', 0, 1}, &ReplyError{Code: ReplyAddressNotSupported}},
	} {
		t.Run(name, func(t *testing.T) {
			d := tc.d
			d.Addr = scripted(t, func(conn net.Conn) {
				_, _ = conn.Write(tc.answer)
				_, _ = io.Copy(io.Discard, conn)
			})
			network := "tcp"
			if name == "udp relay by name" {
				network = "udp"
			}
			r := dialVia(t, engine, &d, network, "1.2.3.4:80", newSink())
			var want *ReplyError
			if errors.As(tc.want, &want) {
				var got *ReplyError
				if !errors.As(r.err, &got) || *got != *want {
					t.Fatalf("%v, want %v", r.err, tc.want)
				}
			} else if !errors.Is(r.err, tc.want) {
				t.Fatalf("%v, want %v", r.err, tc.want)
			}
		})
	}
}

// udpProxy is a SOCKS5 proxy of a test's own for one UDP association: it
// grants it, naming its relay by the unspecified address, and hands the
// relay's socket and the client's control connection to fn.
func udpProxy(t *testing.T, fn func(relay net.PacketConn, control net.Conn)) string {
	t.Helper()
	relay, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = relay.Close() })
	port := relay.LocalAddr().(*net.UDPAddr).Port
	return scripted(t, func(conn net.Conn) {
		skip(conn, 3)
		_, _ = conn.Write([]byte{5, 0})
		skip(conn, 10)
		_, _ = conn.Write([]byte{5, 0, 0, atypIPv4, 0, 0, 0, 0, byte(port >> 8), byte(port)})
		fn(relay, conn)
	})
}

// The client drops datagrams from the relay that are not whole ones with a
// header, and CloseAfterSend ends the association.
func TestDialerUDPRelay(t *testing.T) {
	engine := clientEngine(t)
	controlClosed := make(chan struct{})
	d := Dialer{Addr: udpProxy(t, func(relay net.PacketConn, control net.Conn) {
		buf := make([]byte, 1500)
		n, from, err := relay.ReadFrom(buf)
		if err != nil {
			return
		}
		header := []byte{0, 0, 0, atypIPv4, 1, 2, 3, 4, 0, 80}
		if string(buf[:n]) != string(append(header, "out"...)) {
			t.Errorf("relay read %x", buf[:n])
		}
		for _, d := range [][]byte{{0, 0}, {0, 0, 1, atypIPv4}, {0, 0, 0, 9, 0}, append(header, "back"...)} {
			_, _ = relay.WriteTo(d, from)
		}
		_, _ = io.Copy(io.Discard, control)
		close(controlClosed)
	})}
	s := newSink()
	r := dialVia(t, engine, &d, "udp", "1.2.3.4:80", s)
	if r.err != nil {
		t.Fatal(r.err)
	}
	_ = r.c.Send([]byte("out"))
	s.wait(t, []byte("back"))
	r.c.CloseAfterSend()
	wait(t, s.closed)
	wait(t, controlClosed)
}

// The relay of a UDP association that cannot be dialed fails the dial, at
// once or later, and closes the control connection.
func TestDialerUDPRelayFails(t *testing.T) {
	engine := clientEngine(t)
	refused := errors.New("refused")
	for name, fail := range map[string]func() (error, error){
		"at once": func() (error, error) { return refused, nil },
		"later":   func() (error, error) { return nil, refused },
	} {
		t.Run(name, func(t *testing.T) {
			controlClosed := make(chan struct{})
			d := Dialer{Addr: udpProxy(t, func(_ net.PacketConn, control net.Conn) {
				_, _ = io.Copy(io.Discard, control)
				close(controlClosed)
			})}
			d.Forward = func(engine *fib.Engine, network, addr string, timeout time.Duration,
				h fib.Handler, done func(*fib.Connection, error)) error {
				if network == "udp" {
					syncErr, asyncErr := fail()
					return failing(syncErr, asyncErr)(engine, network, addr, timeout, h, done)
				}
				return DialDirect(engine, network, addr, timeout, h, done)
			}
			if r := dialVia(t, engine, &d, "udp", "1.2.3.4:80", newSink()); !errors.Is(r.err, refused) {
				t.Fatalf("%v, want %v", r.err, refused)
			}
			wait(t, controlClosed)
		})
	}
}

// A relay dialed after its association has ended is closed as soon as the
// dial is reported.
func TestDialerUDPRelayAfterTheAssociationEnded(t *testing.T) {
	engine := clientEngine(t)
	s := newSink()
	reported := make(chan dialResult, 1)
	a := &udpAssociation{handler: s, stopped: true,
		h: &clientConn{done: func(c *fib.Connection, err error) { reported <- dialResult{c, err} }}}
	a.control = fibConn(t, engine, "tcp", echo(t), fib.HandlerFuncs{})
	conn := fibConn(t, engine, "udp", udpEcho(t), a)
	a.dialed(conn, nil)
	if r := wait(t, reported); r.c != conn {
		t.Fatal("the dial was not reported")
	}
	if err := wait(t, s.closed); err != errAssociationEnded {
		t.Fatalf("closed with %v", err)
	}
}

func TestNewServerDefaults(t *testing.T) {
	if s := NewServer(Config{DialTimeout: -1}); s.config.DialTimeout != 0 {
		t.Fatalf("DialTimeout %v", s.config.DialTimeout)
	}
}

// The server takes every message of the client in pieces as well as whole.
func TestServerReadsMessagesInPieces(t *testing.T) {
	proxyAddr, _ := proxy(t, Config{Authenticate: func(u, p string) bool { return u == "u" && p == "p" }})
	open, _ := proxy(t, Config{})
	target := echo(t)
	conn := dialRaw(t, proxyAddr)
	trickle(conn, []byte{5, 1, 2})
	expect(t, conn, []byte{5, 2})
	trickle(conn, []byte{1, 1, 'u', 1, 'p'})
	expect(t, conn, []byte{1, 0})
	trickle(conn, append([]byte{5, 1, 0}, addr5(t, target)...))
	readN(t, conn, 10)
	write(t, conn, []byte("5"))
	expect(t, conn, []byte("5"))

	host, port, _ := splitHostPort(target)
	ip := []byte(net.ParseIP(host).To4())
	for _, req := range [][]byte{
		append(append([]byte{4, 1, byte(port >> 8), byte(port)}, ip...), "id\x00"...),
		append([]byte{4, 1, byte(port >> 8), byte(port), 0, 0, 0, 1}, "id\x00localhost\x00"...),
	} {
		conn := dialRaw(t, open)
		trickle(conn, req)
		if reply := readN(t, conn, 8); reply[1] != reply4Granted {
			t.Fatalf("reply %x", reply)
		}
		write(t, conn, []byte("4"))
		expect(t, conn, []byte("4"))
	}
}

func TestServerRefusesBadMessages(t *testing.T) {
	proxyAddr, _ := proxy(t, Config{Authenticate: func(string, string) bool { return true }})
	open, _ := proxy(t, Config{})
	long := strings.Repeat("x", 256)
	for name, tc := range map[string]struct {
		addr string
		msg  []byte
	}{
		"login version":   {proxyAddr, []byte{5, 1, 2, 9, 1, 'u', 1, 'p'}},
		"request version": {open, []byte{5, 1, 0, 4, 1, 0, 1, 1, 2, 3, 4, 0, 80}},
		"socks4 user id":  {open, append([]byte{4, 1, 0, 80, 1, 2, 3, 4}, long...)},
		"socks4a host":    {open, append([]byte{4, 1, 0, 80, 0, 0, 0, 1, 0}, long...)},
		"socks4a no host": {open, []byte{4, 1, 0, 80, 0, 0, 0, 1, 0, 0}},
	} {
		t.Run(name, func(t *testing.T) {
			conn := dialRaw(t, tc.addr)
			write(t, conn, tc.msg)
			_, _ = io.ReadAll(conn)
			eof(t, conn)
		})
	}
}

// What the client sends while its destination is dialed waits, holding
// its reads back once there is more than the relay queues, and then goes
// on first; what it sends on a UDP association's control is dropped.
func TestServerEarlyData(t *testing.T) {
	g := newGate()
	proxyAddr, _ := proxy(t, Config{Dial: g.dial, MaxQueuedBytes: 1 << 10})
	target := echo(t)
	conn := dialRaw(t, proxyAddr)
	connect5(t, conn, 1, target)
	<-g.dialed
	early := []byte(strings.Repeat("e", 64<<10))
	write(t, conn, early)
	time.Sleep(50 * time.Millisecond)
	close(g.open)
	readN(t, conn, 10)
	expect(t, conn, early)

	control := dialRaw(t, proxyAddr)
	connect5(t, control, 3, "0.0.0.0:0")
	readN(t, control, 10)
	write(t, control, []byte("ignored"))
	_ = control.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if n, err := control.Read(make([]byte, 1)); n != 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("control read %d bytes, %v", n, err)
	}
}

func TestServerDialFails(t *testing.T) {
	g := newGate()
	g.fail = func(string) (error, error) { return errors.New("no"), nil }
	proxyAddr, _ := proxy(t, Config{Dial: g.dial})
	conn := dialRaw(t, proxyAddr)
	connect5(t, conn, 1, "1.2.3.4:80")
	if reply := readN(t, conn, 2); reply[1] != ReplyGeneralFailure {
		t.Fatalf("reply %x", reply)
	}
}

// A destination that opens after its client has gone is closed.
func TestServerDestinationAfterTheClientWent(t *testing.T) {
	g := newGate()
	proxyAddr, _ := proxy(t, Config{Dial: g.dial})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	conn := dialRaw(t, proxyAddr)
	connect5(t, conn, 1, ln.Addr().String())
	<-g.dialed
	_ = conn.Close()
	time.Sleep(50 * time.Millisecond)
	close(g.open)
	target, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	_ = target.SetDeadline(time.Now().Add(testTimeout))
	eof(t, target)
}

// A BIND whose client goes, or whose port cannot be listened on, ends.
func TestServerBindEnds(t *testing.T) {
	proxyAddr, _ := proxy(t, Config{})
	conn := dialRaw(t, proxyAddr)
	// A peer the probes below are not, so that none of them is relayed.
	connect5(t, conn, 2, "10.9.9.9:0")
	reply := readN(t, conn, 10)
	port := int(reply[8])<<8 | int(reply[9])
	_ = conn.Close()
	deadline := time.Now().Add(testTimeout)
	for {
		peer, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", itoa(port)))
		if err != nil {
			break
		}
		_ = peer.Close()
		if time.Now().After(deadline) {
			t.Fatal("still listening after the client went")
		}
		time.Sleep(10 * time.Millisecond)
	}

	listenTCP = func(string, *net.TCPAddr) (*net.TCPListener, error) { return nil, syscall.EADDRINUSE }
	t.Cleanup(func() { listenTCP = net.ListenTCP })
	conn = dialRaw(t, proxyAddr)
	connect5(t, conn, 2, "127.0.0.1:0")
	if reply := readN(t, conn, 2); reply[1] != ReplyGeneralFailure {
		t.Fatalf("reply %x", reply)
	}
}

func itoa(n int) string { return (&net.TCPAddr{Port: n}).String()[1:] }

// A peer that connects once its BIND's client has gone is closed.
func TestServerBoundAfterTheClientWent(t *testing.T) {
	sc := &serverConn{s: NewServer(Config{}), state: stateClosed}
	ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() { _, _ = net.Dial("tcp", ln.Addr().String()) }()
	conn, err := ln.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	sc.bound(conn)
	if _, err := conn.Write([]byte("x")); err == nil {
		t.Fatal("the peer's connection is open")
	}
}

// What the client sends a BIND's peer that is slow to read holds the
// client's reads back, and arrives whole.
func TestServerBindBackpressure(t *testing.T) {
	proxyAddr, _ := proxy(t, Config{MaxQueuedBytes: 1 << 10})
	conn := dialRaw(t, proxyAddr)
	connect5(t, conn, 2, "127.0.0.1:0")
	reply := readN(t, conn, 10)
	payload := []byte(strings.Repeat("b", 8<<20))
	// Early data for the peer, more than the queue, before it connects.
	write(t, conn, payload[:4<<10])
	peer := dialRaw(t, net.JoinHostPort("127.0.0.1", itoa(int(reply[8])<<8|int(reply[9]))))
	readN(t, conn, 10)
	go func() { _, _ = conn.Write(payload[4<<10:]) }()
	time.Sleep(100 * time.Millisecond)
	_ = peer.SetDeadline(time.Now().Add(30 * time.Second))
	got := make([]byte, len(payload))
	if n, err := io.ReadFull(peer, got); err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("peer read %d of %d bytes: %v", n, len(payload), err)
	}
}

// streamPipe ends the relay when the peer's connection fails either way.
func TestStreamPipeFailures(t *testing.T) {
	engine := clientEngine(t)
	s := newSink()
	c := fibConn(t, engine, "tcp", echo(t), s)
	p := &streamPipe{c: c, conn: failingConn{}, limit: 1}
	p.cond.L = &p.mu
	p.enqueue([]byte("x"))
	p.write()
	wait(t, s.closed)

	// Read sends to a client that has closed.
	p = &streamPipe{c: c, conn: &readingConn{chunk: []byte("x")}, limit: 1}
	p.read()

	// The client closes while read waits for it to drain.
	sinkhole := scripted(t, func(conn net.Conn) { time.Sleep(testTimeout) })
	s = newSink()
	c = fibConn(t, engine, "tcp", sinkhole, s)
	p = &streamPipe{c: c, conn: &readingConn{chunk: make([]byte, 1<<20)}, limit: 1}
	go func() {
		time.Sleep(100 * time.Millisecond)
		c.Close()
	}()
	p.read()
}

type failingConn struct{ net.Conn }

func (failingConn) Write([]byte) (int, error) { return 0, syscall.EPIPE }
func (failingConn) Close() error              { return nil }

// readingConn reads chunk forever.
type readingConn struct {
	net.Conn
	chunk []byte
}

func (r *readingConn) Read(b []byte) (int, error) { return copy(b, r.chunk), nil }

// Forwarding to a destination that has closed leaves the client to the
// destination's OnClose.
func TestForwardToAClosedConnection(t *testing.T) {
	engine := clientEngine(t)
	from := fibConn(t, engine, "tcp", echo(t), fib.HandlerFuncs{})
	to := fibConn(t, engine, "tcp", echo(t), fib.HandlerFuncs{})
	to.Close()
	if (&tunnel{limit: 1}).forward(from, to, []byte("x")) {
		t.Fatal("forward held reads for a closed destination")
	}
}

// Engine.Shutdown closes the clients still negotiating, and leaves UDP
// peers and relayed clients alone.
func TestServerShutdown(t *testing.T) {
	server := NewServer(Config{})
	config := fib.DefaultConfig()
	config.Name = "socks-" + t.Name()
	engine, err := fib.New(config,
		fib.Binding{Addr: "127.0.0.1:0", Handler: server},
		fib.Binding{Network: "udp", Addr: "127.0.0.1:0", Handler: server},
	)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- engine.Run() }()
	defer func() {
		engine.Stop()
		<-done
		_ = engine.Close()
	}()
	addrs, _ := engine.BindingAddrs(0)
	proxyAddr := addrs[0].String()

	negotiating := dialRaw(t, proxyAddr)
	write(t, negotiating, []byte{5})
	relayed := dialRaw(t, proxyAddr)
	connect5(t, relayed, 1, echo(t))
	readN(t, relayed, 10)
	control := dialRaw(t, proxyAddr)
	connect5(t, control, 3, "0.0.0.0:0")
	reply := readN(t, control, 10)
	udp, err := net.DialUDP("udp", nil, &net.UDPAddr{IP: net.IP(reply[4:8]), Port: int(reply[8])<<8 | int(reply[9])})
	if err != nil {
		t.Fatal(err)
	}
	defer udp.Close()
	_, _ = udp.Write(append(append([]byte{0, 0, 0}, addr5(t, udpEcho(t))...), "x"...))
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = engine.Shutdown(ctx)
	eof(t, negotiating)
}

// A server bound to a Unix socket has no IP to bind or relay on.
func TestServerOnAUnixSocket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s")
	run(t, fib.DefaultConfig(), fib.Binding{Network: "unix", Addr: path, Handler: NewServer(Config{})})
	for _, cmd := range []byte{2, 3} {
		conn, err := net.Dial("unix", path)
		if err != nil {
			t.Skip(err)
		}
		_ = conn.SetDeadline(time.Now().Add(testTimeout))
		connect5(t, conn, cmd, "127.0.0.1:0")
		if reply := readN(t, conn, 2); reply[1] != ReplyCommandNotSupported {
			t.Fatalf("command %d: reply %x", cmd, reply)
		}
		_ = conn.Close()
	}
}

// relayAddr names the address the client reached the server at when the
// UDP socket is bound to every address, and finds none of the client's
// family when there is none.
func TestRelayAddrFamilies(t *testing.T) {
	for _, tc := range []struct {
		tcp, udp string
		want     bool
	}{
		{"127.0.0.1:0", "0.0.0.0:0", true},
		{"127.0.0.1:0", "[::1]:0", false},
		{"[::1]:0", "0.0.0.0:0", false},
		{"[::1]:0", "[::]:0", true},
		{"[::1]:0", "[::1]:0", true},
	} {
		server := NewServer(Config{})
		config := fib.DefaultConfig()
		config.Name = "socks-" + t.Name()
		engine, err := fib.New(config,
			fib.Binding{Addr: tc.tcp, Handler: server},
			fib.Binding{Network: "udp", Addr: tc.udp, Handler: server},
		)
		if err != nil {
			t.Logf("%s and %s: %v", tc.tcp, tc.udp, err)
			continue
		}
		stop := func() { _ = engine.Close() }
		done := make(chan error, 1)
		go func() { done <- engine.Run() }()
		addrs, _ := engine.BindingAddrs(0)
		conn := dialRaw(t, addrs[0].String())
		connect5(t, conn, 3, "0.0.0.0:0")
		reply := readN(t, conn, 2)
		if granted := reply[1] == ReplySucceeded; granted != tc.want {
			t.Errorf("%s and %s: reply %x", tc.tcp, tc.udp, reply)
		}
		_ = conn.Close()
		engine.Stop()
		<-done
		stop()
	}
}

// udpClient is a client of a UDP association, spoken by hand.
type udpClient struct {
	t       *testing.T
	control net.Conn
	sock    *net.UDPConn
	relay   *net.UDPAddr
}

// associate opens a UDP association on proxyAddr, naming the port its
// datagrams come from when namePort is set.
func associate(t *testing.T, proxyAddr string, namePort bool) *udpClient {
	t.Helper()
	sock, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sock.Close() })
	named := "0.0.0.0:0"
	if namePort {
		named = sock.LocalAddr().String()
	}
	control := dialRaw(t, proxyAddr)
	connect5(t, control, 3, named)
	reply := readN(t, control, 10)
	if reply[1] != 0 {
		t.Fatalf("reply %x", reply)
	}
	relay := &net.UDPAddr{IP: net.IP(reply[4:8]), Port: int(reply[8])<<8 | int(reply[9])}
	return &udpClient{t: t, control: control, sock: sock, relay: relay}
}

func (u *udpClient) sendRaw(b []byte) {
	if _, err := u.sock.WriteTo(b, u.relay); err != nil {
		u.t.Fatal(err)
	}
}

func (u *udpClient) send(addr, payload string) {
	u.sendRaw(append(append([]byte{0, 0, 0}, addr5(u.t, addr)...), payload...))
}

// recv waits a while for a datagram, and reports its payload.
func (u *udpClient) recv(d time.Duration) (string, bool) {
	buf := make([]byte, 1500)
	_ = u.sock.SetReadDeadline(time.Now().Add(d))
	n, err := u.sock.Read(buf)
	if err != nil {
		return "", false
	}
	_, _, an, _ := parseAddr(buf[3:n])
	return string(buf[3+an : n]), true
}

// until sends to addr until an answer comes, and returns it.
func (u *udpClient) until(addr, payload string) string {
	deadline := time.Now().Add(testTimeout)
	for time.Now().Before(deadline) {
		u.send(addr, payload)
		if got, ok := u.recv(100 * time.Millisecond); ok {
			return got
		}
	}
	u.t.Fatalf("no answer from %s", addr)
	return ""
}

// Datagrams that are not whole ones with a header, or that come from
// where no association expects them, go nowhere; the association a client
// named its port for is its; and a client silent for the engine's UDP idle
// timeout is known again by its address.
func TestServerUDPPeers(t *testing.T) {
	proxyAddr, server := proxyWith(t, func(c *fib.Config) { c.UDPIdleTimeout = 100 * time.Millisecond }, Config{})
	target := udpEcho(t)
	named := associate(t, proxyAddr, true)
	other := associate(t, proxyAddr, false)
	// The unnamed association was opened last, but the datagram from the
	// named port claims the named one.
	if got := named.until(target, "named"); got != "echo:named" {
		t.Fatalf("got %q", got)
	}
	server.mu.Lock()
	claimed := server.associations[netip.MustParseAddr("127.0.0.1")][0].src.IsValid()
	server.mu.Unlock()
	if !claimed {
		t.Fatal("the named association was not claimed")
	}

	named.sendRaw([]byte{0, 0})
	named.sendRaw([]byte{0, 0, 1, atypIPv4, 1, 2, 3, 4, 0, 80})
	named.sendRaw([]byte{0, 0, 0, 9, 1})
	if got, ok := named.recv(50 * time.Millisecond); ok {
		t.Fatalf("answered %q", got)
	}

	time.Sleep(400 * time.Millisecond)
	if got := named.until(target, "again"); got != "echo:again" {
		t.Fatalf("got %q after the idle timeout", got)
	}
	if got := other.until(target, "other"); got != "echo:other" {
		t.Fatalf("got %q", got)
	}

	// A third socket from the same IP, with both associations claimed.
	stranger := &udpClient{t: t, relay: named.relay}
	sock, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer sock.Close()
	stranger.sock = sock
	stranger.send(target, "nobody's")
	if got, ok := stranger.recv(100 * time.Millisecond); ok {
		t.Fatalf("a stranger was answered %q", got)
	}
}

// One association's destinations: the oldest makes room for a new one
// beyond MaxUDPTargets, a refused one stays refused, and the datagrams for
// one being dialed wait for it, up to a limit.
func TestServerUDPTargets(t *testing.T) {
	g := newGate()
	proxyAddr, _ := proxy(t, Config{Dial: g.dial, MaxUDPTargets: 2, Allow: func(r *Request) error {
		if strings.HasSuffix(r.Addr, ":9") {
			return errors.New("refused")
		}
		return nil
	}})
	close(g.open)
	first, second := udpEcho(t), udpEcho(t)
	u := associate(t, proxyAddr, false)
	if got := u.until(first, "1"); got != "echo:1" {
		t.Fatalf("got %q", got)
	}
	u.send("127.0.0.1:9", "refused")
	u.send("127.0.0.1:9", "refused again")
	// The third destination drops the first, which then dials again.
	if got := u.until(second, "2"); got != "echo:2" {
		t.Fatalf("got %q", got)
	}
	if got := u.until(first, "1"); got != "echo:1" {
		t.Fatalf("got %q", got)
	}

	// The dial waits until the proxy has taken in every datagram: one sent
	// to another destination after them, which the proxy reads after them
	// from the same peer, has had its own dial started. A fixed pause was not
	// enough on a loaded Windows runner, where datagrams that arrived once
	// the dial was open went straight through: 22 answers to 32.
	g2 := newGate()
	proxyAddr, _ = proxy(t, Config{Dial: g2.dial})
	u = associate(t, proxyAddr, false)
	for i := 0; i < 2*maxWaitingDatagrams; i++ {
		u.send(first, "w")
	}
	if addr := <-g2.dialed; addr != first {
		t.Fatalf("dialed %s first, want %s", addr, first)
	}
	deadline := time.After(testTimeout)
	for marked := false; !marked; {
		u.send(second, "marker")
		select {
		case addr := <-g2.dialed:
			if addr != second {
				t.Fatalf("dialed %s, want %s", addr, second)
			}
			marked = true
		case <-time.After(100 * time.Millisecond):
		case <-deadline:
			t.Fatal("the datagram after the waiting ones never reached the proxy")
		}
	}
	close(g2.open)
	answers := 0
	for {
		got, ok := u.recv(200 * time.Millisecond)
		if !ok {
			break
		}
		if got == "echo:w" {
			answers++
		}
	}
	if answers == 0 || answers > maxWaitingDatagrams {
		t.Fatalf("%d answers to %d datagrams that waited", answers, 2*maxWaitingDatagrams)
	}
}

// A destination that cannot be dialed, at once or later, is dialed again
// for the next datagram to it.
func TestServerUDPDialFails(t *testing.T) {
	for _, later := range []bool{false, true} {
		g := newGate()
		var mu sync.Mutex
		failures := 1
		g.fail = func(string) (error, error) {
			mu.Lock()
			defer mu.Unlock()
			if failures == 0 {
				return nil, nil
			}
			failures--
			if later {
				return nil, errors.New("no")
			}
			return errors.New("no"), nil
		}
		close(g.open)
		proxyAddr, _ := proxy(t, Config{Dial: g.dial})
		u := associate(t, proxyAddr, false)
		if got := u.until(udpEcho(t), "x"); got != "echo:x" {
			t.Fatalf("got %q", got)
		}
	}
}

// A destination that opens after its association has ended is closed, and
// so is one whose association ends while Allow decides it.
func TestServerUDPAssociationEnds(t *testing.T) {
	g := newGate()
	proxyAddr, server := proxy(t, Config{Dial: g.dial})
	u := associate(t, proxyAddr, false)
	u.send(udpEcho(t), "x")
	<-g.dialed
	_ = u.control.Close()
	waitAssociations(t, server)
	close(g.open)
	if got, ok := u.recv(100 * time.Millisecond); ok {
		t.Fatalf("answered %q after the association ended", got)
	}

	var srv *Server
	proxyAddr, srv = proxy(t, Config{Allow: func(r *Request) error {
		// The request itself names no destination.
		if r.Command == CommandUDPAssociate && r.Addr != "0.0.0.0:0" {
			r.Conn.Close()
			waitAssociations(t, srv)
		}
		return nil
	}})
	u = associate(t, proxyAddr, false)
	u.send(udpEcho(t), "x")
	if got, ok := u.recv(200 * time.Millisecond); ok {
		t.Fatalf("answered %q after the association ended", got)
	}
}

// waitAssociations waits until server has no association left.
func waitAssociations(t *testing.T, server *Server) {
	deadline := time.Now().Add(testTimeout)
	for time.Now().Before(deadline) {
		server.mu.Lock()
		n := len(server.associations)
		server.mu.Unlock()
		if n == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Error("the association did not end")
}

// A nil handler is the engine's, and a nil done is allowed.
func TestDialerDefaults(t *testing.T) {
	engine := clientEngine(t)
	granted := make(chan struct{})
	d := Dialer{Addr: scripted(t, func(conn net.Conn) {
		skip(conn, 3)
		_, _ = conn.Write([]byte{5, 0})
		skip(conn, 10)
		_, _ = conn.Write([]byte{5, 0, 0, atypIPv4, 0, 0, 0, 0, 0, 0})
		close(granted)
		_, _ = io.Copy(io.Discard, conn)
	})}
	if err := d.Dial(engine, "", "1.2.3.4:80", testTimeout, nil, nil); err != nil {
		t.Fatal(err)
	}
	wait(t, granted)
}
