package multiproto

import (
	"encoding/binary"
	"strings"
	"testing"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/hpack"
	"github.com/lesismal/fib/mux"
)

// h2Frame is one HTTP/2 frame.
func h2Frame(typ, flags byte, stream uint32, payload []byte) []byte {
	b := []byte{byte(len(payload) >> 16), byte(len(payload) >> 8), byte(len(payload)), typ, flags, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(b[5:], stream)
	return append(b, payload...)
}

// headerBlock encodes fields, name and value in turn, as a fresh client
// would.
func headerBlock(fields ...string) []byte {
	e := hpack.NewEncoder()
	b := e.Begin(nil)
	for i := 0; i+1 < len(fields); i += 2 {
		b = e.AppendField(b, fields[i], fields[i+1], false)
	}
	return b
}

// requestBlock is the header block of a request on stream 1 whose
// content-type is contentType.
func requestBlock(contentType string) []byte {
	return headerBlock(":method", "POST", ":scheme", "http", ":path", "/svc/M", ":authority", "localhost",
		"content-type", contentType, "te", "trailers")
}

func check(t *testing.T, name string, m mux.Matcher, input string, want mux.Result) {
	t.Helper()
	if got := m([]byte(input)); got != want {
		t.Errorf("%s(%q) = %v, want %v", name, input, got, want)
	}
}

func TestWebSocketMatcher(t *testing.T) {
	m := WebSocket()
	handshake := "GET /ws HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: WebSocket\r\nSec-WebSocket-Version: 13\r\n\r\n"
	for _, c := range []struct {
		input string
		want  mux.Result
	}{
		{"G", mux.NeedMore},
		{"POST / HTTP/1.1\r\n", mux.NoMatch},
		{"PRI * HTTP/2.0", mux.NoMatch},
		{handshake[:30], mux.NeedMore},
		{handshake, mux.Match},
		{"GET / HTTP/1.1\nupgrade:  h2c, websocket \n\n", mux.Match},
		{"GET / HTTP/1.1\r\nHost: x\r\n\r\n", mux.NoMatch},
		{"GET / HTTP/1.1\r\nUpgrade: h2c\r\n\r\n", mux.NoMatch},
		// An Upgrade after the head is the body's, not a header.
		{"GET / HTTP/1.1\r\n\r\nUpgrade: websocket\r\n", mux.NoMatch},
	} {
		check(t, "WebSocket", m, c.input, c.want)
	}
}

func TestGRPCMatcher(t *testing.T) {
	m := GRPC()
	settings := string(h2Frame(h2FrameSettings, 0, 0, []byte{0, 3, 0, 0, 0, 100}))
	window := string(h2Frame(h2FrameWindowUpdate, 0, 0, []byte{0, 0, 1, 0}))
	grpcBlock := requestBlock("application/grpc+proto")
	grpc := h2Preface + settings + window + string(h2Frame(h2FrameHeaders, h2FlagEndHeaders, 1, grpcBlock))
	check(t, "GRPC", m, "PRI * HTTP", mux.NeedMore)
	check(t, "GRPC", m, "GET / HTTP/1.1\r\n", mux.NoMatch)
	check(t, "GRPC", m, h2Preface, mux.NeedMore)
	check(t, "GRPC", m, h2Preface+settings, mux.NeedMore)
	check(t, "GRPC", m, grpc[:len(grpc)-3], mux.NeedMore)
	check(t, "GRPC", m, grpc, mux.Match)
	check(t, "GRPC", m, h2Preface+settings+string(h2Frame(h2FrameHeaders, h2FlagEndHeaders, 1,
		requestBlock("application/GRPC"))), mux.Match)
	check(t, "GRPC", m, h2Preface+settings+string(h2Frame(h2FrameHeaders, h2FlagEndHeaders, 1,
		requestBlock("application/grpc-web"))), mux.NoMatch)
	check(t, "GRPC", m, h2Preface+settings+string(h2Frame(h2FrameHeaders, h2FlagEndHeaders, 1,
		headerBlock(":method", "GET", ":path", "/"))), mux.NoMatch)
	// A DATA frame before any request is not a gRPC client's.
	check(t, "GRPC", m, h2Preface+settings+string(h2Frame(0, 0, 1, []byte("x"))), mux.NoMatch)

	// Padding and priority around the fragment, and a block split across
	// CONTINUATION frames.
	padded := append([]byte{3, 0, 0, 0, 0, 16}, grpcBlock...)
	padded = append(padded, 0, 0, 0)
	check(t, "GRPC", m, h2Preface+string(h2Frame(h2FrameHeaders, h2FlagEndHeaders|h2FlagPadded|h2FlagPriority, 1, padded)), mux.Match)
	split := h2Preface + string(h2Frame(h2FrameHeaders, 0, 1, grpcBlock[:5]))
	check(t, "GRPC", m, split, mux.NeedMore)
	check(t, "GRPC", m, split+string(h2Frame(h2FrameContinuation, h2FlagEndHeaders, 1, grpcBlock[5:])), mux.Match)
	check(t, "GRPC", m, split+string(h2Frame(h2FrameContinuation, h2FlagEndHeaders, 3, grpcBlock[5:])), mux.NoMatch)
	check(t, "GRPC", m, split+settings, mux.NoMatch)
	// A padding longer than the frame.
	check(t, "GRPC", m, h2Preface+string(h2Frame(h2FrameHeaders, h2FlagEndHeaders|h2FlagPadded, 1, []byte{9, 1})), mux.NoMatch)
}

// arpcMessage is an arpc message's header and method name, with a body of
// bodyLen bytes announced.
func arpcMessage(cmd byte, method string, bodyLen int) string {
	h := make([]byte, arpcHeadLen)
	binary.LittleEndian.PutUint32(h, uint32(bodyLen))
	h[arpcIndexCmd], h[arpcIndexMethod] = cmd, byte(len(method))
	return string(h) + method
}

func TestARPCMatcher(t *testing.T) {
	m := ARPC(0)
	req := arpcMessage(arpcCmdRequest, "/echo", 12)
	check(t, "ARPC", m, req[:3], mux.NeedMore)
	check(t, "ARPC", m, req[:18], mux.NeedMore)
	check(t, "ARPC", m, req, mux.Match)
	check(t, "ARPC", m, arpcMessage(arpcCmdPing, "", 0), mux.Match)
	check(t, "ARPC", m, arpcMessage(arpcCmdNotify, "n", 1), mux.Match)
	check(t, "ARPC", m, arpcMessage(2, "/echo", 12), mux.NoMatch) // a response
	check(t, "ARPC", m, arpcMessage(arpcCmdPing, "", 4), mux.NoMatch)
	check(t, "ARPC", m, arpcMessage(arpcCmdRequest, "", 4), mux.NoMatch)
	check(t, "ARPC", m, arpcMessage(arpcCmdRequest, "/echo", 2), mux.NoMatch)
	check(t, "ARPC", m, arpcMessage(arpcCmdRequest, "/e\x01ho", 12), mux.NoMatch)
	check(t, "ARPC", m, arpcMessage(arpcCmdRequest, "/echo", 1<<27), mux.NoMatch)
	check(t, "ARPC", ARPC(1<<28), arpcMessage(arpcCmdRequest, "/echo", 1<<27), mux.Match)
	check(t, "ARPC", m, "GET / HTTP/1.1\r\nHost: x\r\n\r\n", mux.NoMatch)
	check(t, "ARPC", m, h2Preface, mux.NoMatch)
}

// pick is what a mux.Handler decides for input with routes: the index of
// the route that takes it, -1 for none, or -2 while it needs more.
func pick(routes []mux.Route, input []byte) int {
	for i, r := range routes {
		switch r.Match(input) {
		case mux.Match:
			return i
		case mux.NeedMore:
			return -2
		}
	}
	return -1
}

// named is a handler that a test tells apart by its name.
type named struct {
	fib.HandlerFuncs
	name string
}

func routeName(routes []mux.Route, i int) string {
	switch i {
	case -1:
		return "none"
	case -2:
		return "more"
	}
	return routes[i].Handler.(*named).name
}

// With arpc served beside them, TLS and SOCKS leave arpc's messages to it,
// even those whose length begins the way their clients' first bytes do.
func TestARPCBesideTLSAndSOCKS(t *testing.T) {
	h := func(name string) fib.Handler { return &named{name: name} }
	routes := Routes(Config{TLS: h("tls"), SOCKS: h("socks"), ARPC: h("arpc"), HTTP1: h("http1")})
	tlsLike := arpcMessage(arpcCmdRequest, "/echo", 0x00040316) // 16 03 04 00
	socksLike := arpcMessage(arpcCmdRequest, "/echo", 0x0105)   // 05 01 00 00
	for _, c := range []struct{ input, want string }{
		{tlsLike, "arpc"},
		{socksLike, "arpc"},
		{socksLike[:3], "socks"}, // a whole SOCKS5 greeting, after which its client waits
		{"\x05\x02\x00\x02", "socks"},
		{"\x05\x02\x00", "more"},
		{"\x04\x01\x00\x50\x0a\x00\x00\x01user\x00", "socks"},
		{"\x04\x01\x00\x50\x00\x00\x00\x01\x00example.com\x00", "socks"},
		// Not yet whole, but no arpc header either: its cmd would be 0.
		{"\x04\x01\x00\x50\x00\x00\x00\x01\x00example", "socks"},
		// A SOCKS5 greeting that has not ended could still be arpc's.
		{"\x05\x03\x00\x00\x00\x01", "more"},
		{"\x16\x03\x01\x02\x00\x01\x00\x01\xfc\x03\x03" + strings.Repeat("\x00", 32), "tls"},
		{"GET / HTTP/1.1\r\n", "http1"},
		{"hello", "none"},
	} {
		if got := routeName(routes, pick(routes, []byte(c.input))); got != c.want {
			t.Errorf("% x went to %s, want %s", c.input, got, c.want)
		}
	}
	// Without arpc, SOCKS does not wait for a greeting's end.
	routes = Routes(Config{SOCKS: h("socks")})
	if got := routeName(routes, pick(routes, []byte{5, 2})); got != "socks" {
		t.Errorf("a SOCKS5 greeting's start went to %s", got)
	}
}

// Only what is served is routed, and the deeper sniffs run only where there
// is a choice to make.
func TestRoutesFollowConfig(t *testing.T) {
	h := func(name string) fib.Handler { return &named{name: name} }
	ws := "GET /ws HTTP/1.1\r\nUpgrade: websocket\r\n\r\n"
	plain := "GET / HTTP/1.1\r\n\r\n"
	grpc := h2Preface + string(h2Frame(h2FrameHeaders, h2FlagEndHeaders, 1, requestBlock("application/grpc")))
	h2 := h2Preface + string(h2Frame(h2FrameHeaders, h2FlagEndHeaders, 1, headerBlock(":method", "GET")))
	for _, c := range []struct {
		config Config
		want   map[string]string
	}{
		{Config{HTTP1: h("http1"), WebSocket: h("ws"), HTTP2: h("h2"), GRPC: h("grpc")},
			map[string]string{ws: "ws", plain: "http1", grpc: "grpc", h2: "h2", "GET": "more", h2Preface: "more"}},
		{Config{WebSocket: h("ws"), GRPC: h("grpc")},
			map[string]string{ws: "ws", plain: "ws", "GET ": "ws", grpc: "grpc", h2: "grpc", h2Preface: "grpc"}},
		{Config{HTTP1: h("http1"), HTTP2: h("h2")},
			map[string]string{ws: "http1", plain: "http1", grpc: "h2", h2: "h2"}},
		{Config{GRPC: h("grpc"), HTTP2: h("h2")},
			map[string]string{ws: "none", grpc: "grpc", h2: "h2"}},
		{Config{}, map[string]string{ws: "none", grpc: "none"}},
	} {
		routes := Routes(c.config)
		for input, want := range c.want {
			if got := routeName(routes, pick(routes, []byte(input))); got != want {
				t.Errorf("%+v: %q went to %s, want %s", c.config, input, got, want)
			}
		}
	}
	// The caller's routes come last.
	routes := Routes(Config{HTTP1: h("http1"), Routes: []mux.Route{{Match: mux.Any(), Handler: h("mine")}}})
	if got := routeName(routes, pick(routes, []byte("hello"))); got != "mine" {
		t.Errorf("the caller's route was not tried: %s", got)
	}
	// Only the gRPC sniff, with HTTP/2 beside it, nudges.
	for _, r := range Routes(Config{GRPC: h("grpc")}) {
		if r.Nudge != nil {
			t.Error("gRPC alone nudges")
		}
	}
}

func TestHTTPProxyMatcher(t *testing.T) {
	m := HTTPProxy()
	for _, c := range []struct {
		input string
		want  mux.Result
	}{
		{"C", mux.NeedMore},
		{"CONNECT example.com:443 HTTP/1.1\r\n", mux.Match},
		{"GET http://example.com/ HTTP/1.1\r\n", mux.Match},
		{"POST HTTPS://example.com/ HTTP/1.1\r\n", mux.Match},
		{"GET h", mux.NeedMore},
		{"GET https:/", mux.NeedMore},
		{"GET ", mux.NeedMore},
		{"GET / HTTP/1.1\r\n", mux.NoMatch},
		{"OPTIONS * HTTP/1.1\r\n", mux.NoMatch},
		{"GET ftp://example.com/ HTTP/1.1\r\n", mux.NoMatch},
		{"\x05\x01\x00", mux.NoMatch},
	} {
		check(t, "HTTPProxy", m, c.input, c.want)
	}
}

func TestSOCKSSettled(t *testing.T) {
	for _, c := range []struct {
		input string
		want  bool
	}{
		{"\x05", false},
		{"\x05\x01", false},
		{"\x05\x01\x00", true},
		{"\x05\x01\x00\x00", false},
		{"\x04\x01\x00\x50\x0a\x00", false},
		{"\x04\x01\x00\x50\x0a\x00\x00\x01user", false},
		{"\x04\x01\x00\x50\x0a\x00\x00\x01user\x00", true},
		{"\x04\x01\x00\x50\x0a\x00\x00\x01\x00", true},
		{"\x04\x01\x00\x50\x0a\x00\x00\x01\x00x", false},
		// SOCKS4a: 0.0.0.x, then a host after the user id.
		{"\x04\x01\x00\x50\x00\x00\x00\x01\x00", false},
		{"\x04\x01\x00\x50\x00\x00\x00\x01\x00example.com", false},
		{"\x04\x01\x00\x50\x00\x00\x00\x01\x00example.com\x00", true},
		{"\x04\x01\x00\x50\x00\x00\x00\x01\x00example.com\x00x", false},
		// 0.0.0.0 is not SOCKS4a.
		{"\x04\x01\x00\x50\x00\x00\x00\x00\x00", true},
		{"\x06\x01\x00", false},
		{"", false},
	} {
		if got := socksSettled([]byte(c.input)); got != c.want {
			t.Errorf("socksSettled(% x) = %v, want %v", c.input, got, c.want)
		}
	}
}

// The malformed frames and blocks GRPC rules out, and a block that takes
// more than one CONTINUATION.
func TestGRPCMatcherEdges(t *testing.T) {
	m := GRPC()
	block := requestBlock("application/grpc")
	cont := h2Preface + string(h2Frame(h2FrameHeaders, 0, 1, block[:3])) +
		string(h2Frame(h2FrameContinuation, 0, 1, block[3:6]))
	check(t, "GRPC", m, cont, mux.NeedMore)
	check(t, "GRPC", m, cont+string(h2Frame(h2FrameContinuation, h2FlagEndHeaders, 1, block[6:])), mux.Match)
	// A priority that does not fit the frame.
	check(t, "GRPC", m, h2Preface+string(h2Frame(h2FrameHeaders, h2FlagEndHeaders|h2FlagPriority, 1, []byte{0, 0})), mux.NoMatch)
	// A padded frame with no room for its pad length.
	check(t, "GRPC", m, h2Preface+string(h2Frame(h2FrameHeaders, h2FlagEndHeaders|h2FlagPadded, 1, nil)), mux.NoMatch)
	// A block HPACK cannot decode: an index beyond both tables.
	check(t, "GRPC", m, h2Preface+string(h2Frame(h2FrameHeaders, h2FlagEndHeaders, 1, []byte{0xff, 0x7f})), mux.NoMatch)
	// A block without a content-type.
	check(t, "GRPC", m, h2Preface+string(h2Frame(h2FrameHeaders, h2FlagEndHeaders, 1, nil)), mux.NoMatch)
}

func TestARPCMatcherEdges(t *testing.T) {
	m := ARPC(0)
	req := []byte(arpcMessage(arpcCmdRequest, "/echo", 12))
	flagged := append([]byte(nil), req...)
	flagged[arpcIndexFlag] = 0x04
	check(t, "ARPC", m, string(flagged[:arpcIndexFlag+1]), mux.NoMatch)
	flagged[arpcIndexFlag] = arpcFlagMask
	check(t, "ARPC", m, string(flagged), mux.Match)
	long := append([]byte(nil), req...)
	long[arpcIndexMethod] = arpcMaxMethodLen + 1
	check(t, "ARPC", m, string(long[:arpcIndexMethod+1]), mux.NoMatch)
	// The stream bits around a cmd do not hide it.
	stream := append([]byte(nil), req...)
	stream[arpcIndexCmd] = arpcCmdStream | 0xc0
	check(t, "ARPC", m, string(stream), mux.Match)
}

// NudgeHTTP2 waits for the whole preface before it speaks.
func TestNudgeHTTP2WaitsForPreface(t *testing.T) {
	if NudgeHTTP2(nil, []byte(h2Preface[:10])) {
		t.Fatal("nudged before the preface arrived")
	}
}

// Of HTTPProxy, WebSocket and HTTP1, each served one but the last reads as
// far as it needs, and the last takes the rest of HTTP/1.x.
func TestHTTP1Family(t *testing.T) {
	h := func(name string) fib.Handler { return &named{name: name} }
	connect := "CONNECT example.com:443 HTTP/1.1\r\n"
	absolute := "GET http://example.com/ HTTP/1.1\r\n"
	ws := "GET /ws HTTP/1.1\r\nUpgrade: websocket\r\n\r\n"
	plain := "GET / HTTP/1.1\r\n\r\n"
	for _, c := range []struct {
		config Config
		want   map[string]string
	}{
		{Config{HTTPProxy: h("proxy"), WebSocket: h("ws"), HTTP1: h("http1")},
			map[string]string{connect: "proxy", absolute: "proxy", ws: "ws", plain: "http1"}},
		{Config{HTTPProxy: h("proxy"), HTTP1: h("http1")},
			map[string]string{connect: "proxy", absolute: "proxy", ws: "http1", plain: "http1"}},
		{Config{HTTPProxy: h("proxy"), WebSocket: h("ws")},
			map[string]string{connect: "proxy", ws: "ws", plain: "ws"}},
		{Config{HTTPProxy: h("proxy"), SOCKS: h("socks")},
			map[string]string{connect: "proxy", plain: "proxy", "\x05\x01\x00": "socks"}},
	} {
		routes := Routes(c.config)
		for input, want := range c.want {
			if got := routeName(routes, pick(routes, []byte(input))); got != want {
				t.Errorf("%+v: %q went to %s, want %s", c.config, input, got, want)
			}
		}
	}
}
