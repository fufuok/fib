package multiproto

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/hpack"
	"github.com/lesismal/fib/mux"
)

// WebSocket matches an HTTP/1.1 request that opens a WebSocket: a GET whose
// head carries an Upgrade header naming websocket. It waits for the whole
// request head, so it needs a MaxPeek as long as the longest head a client
// sends; a head that does not fit counts as not matching, and goes to the
// route for other HTTP/1.x.
func WebSocket() mux.Matcher {
	return func(input []byte) mux.Result {
		const get = "GET "
		if n := min(len(input), len(get)); string(input[:n]) != get[:n] {
			return mux.NoMatch
		}
		// The request line is not a header, and ends at the first newline.
		line := bytes.IndexByte(input, '\n')
		if line < 0 {
			return mux.NeedMore
		}
		rest := input[line+1:]
		for {
			end := bytes.IndexByte(rest, '\n')
			if end < 0 {
				return mux.NeedMore
			}
			field := bytes.TrimSuffix(rest[:end], []byte{'\r'})
			rest = rest[end+1:]
			if len(field) == 0 {
				// The empty line that ends the head, with no Upgrade before it.
				return mux.NoMatch
			}
			name, value, ok := bytes.Cut(field, []byte{':'})
			if ok && strings.EqualFold(string(bytes.TrimSpace(name)), "upgrade") && hasToken(value, "websocket") {
				return mux.Match
			}
		}
	}
}

// HTTPProxy matches an HTTP/1.x request meant for a proxy: a CONNECT, or a
// request whose target is an absolute http:// or https:// URL, which a
// client sends a proxy rather than the server the URL names. It needs only
// the request line's method and the start of its target.
func HTTPProxy() mux.Matcher {
	http1 := mux.HTTP1()
	return func(input []byte) mux.Result {
		if r := http1(input); r != mux.Match {
			return r
		}
		method, target, _ := bytes.Cut(input, []byte{' '})
		if string(method) == "CONNECT" {
			return mux.Match
		}
		for _, scheme := range [...]string{"http://", "https://"} {
			n := min(len(target), len(scheme))
			if !strings.EqualFold(string(target[:n]), scheme[:n]) {
				continue
			}
			if n == len(scheme) {
				return mux.Match
			}
			return mux.NeedMore
		}
		return mux.NoMatch
	}
}

// hasToken reports whether the comma-separated list value names token, in
// any case.
func hasToken(value []byte, token string) bool {
	for len(value) > 0 {
		var item []byte
		item, value, _ = bytes.Cut(value, []byte{','})
		if strings.EqualFold(string(bytes.TrimSpace(item)), token) {
			return true
		}
	}
	return false
}

// The HTTP/2 frames GRPC reads, and the flags it looks at (RFC 9113).
const (
	h2Preface           = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
	h2FrameHeaderLen    = 9
	h2FrameHeaders      = 0x1
	h2FramePriority     = 0x2
	h2FrameSettings     = 0x4
	h2FramePing         = 0x6
	h2FrameWindowUpdate = 0x8
	h2FrameContinuation = 0x9
	h2FlagEndHeaders    = 0x4
	h2FlagPadded        = 0x8
	h2FlagPriority      = 0x20
	// h2HeaderTableSize is the dynamic table a client may use before it has
	// seen the server's SETTINGS, which is all the first request can rely on.
	h2HeaderTableSize = 4096
)

// h2EmptySettings is a SETTINGS frame that changes nothing, which a server
// may send as its connection preface.
var h2EmptySettings = []byte{0, 0, 0, h2FrameSettings, 0, 0, 0, 0, 0}

// GRPC matches an HTTP/2 connection with prior knowledge (h2c) whose first
// request is a gRPC call: one whose content-type is application/grpc, alone
// or with a subtype. It reads past the preface to the first request's
// headers, over the frames a client may send before them, and needs a
// MaxPeek long enough for those; a connection whose first headers do not fit
// counts as not matching. Pair it with NudgeHTTP2, which sends the client the
// SETTINGS that some clients, grpc-go's among them, wait for before they send
// a request.
func GRPC() mux.Matcher {
	return func(input []byte) mux.Result {
		if n := min(len(input), len(h2Preface)); string(input[:n]) != h2Preface[:n] {
			return mux.NoMatch
		}
		if len(input) < len(h2Preface) {
			return mux.NeedMore
		}
		rest := input[len(h2Preface):]
		var block []byte
		var stream uint32
		for {
			if len(rest) < h2FrameHeaderLen {
				return mux.NeedMore
			}
			length := int(rest[0])<<16 | int(rest[1])<<8 | int(rest[2])
			typ, flags := rest[3], rest[4]
			id := binary.BigEndian.Uint32(rest[5:9]) & 0x7fffffff
			if len(rest) < h2FrameHeaderLen+length {
				return mux.NeedMore
			}
			payload := rest[h2FrameHeaderLen : h2FrameHeaderLen+length]
			rest = rest[h2FrameHeaderLen+length:]
			if block != nil {
				// A header block that did not end goes on in CONTINUATION
				// frames of its stream, and nothing comes between them.
				if typ != h2FrameContinuation || id != stream {
					return mux.NoMatch
				}
				block = append(block, payload...)
				if flags&h2FlagEndHeaders != 0 {
					return grpcHeaders(block)
				}
				continue
			}
			switch typ {
			case h2FrameSettings, h2FrameWindowUpdate, h2FramePing, h2FramePriority:
				// What a client may send before its first request.
			case h2FrameHeaders:
				fragment, ok := headersFragment(payload, flags)
				if !ok {
					return mux.NoMatch
				}
				if flags&h2FlagEndHeaders != 0 {
					return grpcHeaders(fragment)
				}
				block, stream = append([]byte(nil), fragment...), id
			default:
				return mux.NoMatch
			}
		}
	}
}

// headersFragment is the header block fragment of a HEADERS frame's payload,
// without its padding and priority.
func headersFragment(payload []byte, flags byte) ([]byte, bool) {
	if flags&h2FlagPadded != 0 {
		if len(payload) == 0 || int(payload[0]) > len(payload)-1 {
			return nil, false
		}
		payload = payload[1 : len(payload)-int(payload[0])]
	}
	if flags&h2FlagPriority != 0 {
		if len(payload) < 5 {
			return nil, false
		}
		payload = payload[5:]
	}
	return payload, true
}

// errFound stops decoding at the content-type.
var errFound = errors.New("found")

// grpcHeaders matches a header block whose content-type is gRPC's.
func grpcHeaders(block []byte) mux.Result {
	result := mux.NoMatch
	err := hpack.NewDecoder(h2HeaderTableSize).Decode(block, func(f hpack.HeaderField) error {
		if f.Name != "content-type" {
			return nil
		}
		if v := strings.ToLower(f.Value); v == "application/grpc" ||
			strings.HasPrefix(v, "application/grpc+") || strings.HasPrefix(v, "application/grpc;") {
			result = mux.Match
		}
		return errFound
	})
	if err != nil && err != errFound {
		return mux.NoMatch
	}
	return result
}

// NudgeHTTP2 is a mux.Route's Nudge for GRPC: once the client's preface has
// arrived it sends an empty SETTINGS frame, the server's preface, which a
// client that waits for it before its first request needs to go on. The
// handler the connection goes to sends its own SETTINGS after it, which
// HTTP/2 allows, and ignores the client's acknowledgement of the first.
func NudgeHTTP2(c *fib.Connection, input []byte) bool {
	if len(input) < len(h2Preface) {
		return false
	}
	_ = c.Send(h2EmptySettings)
	return true
}

// The arpc header, little endian, ARPC checks (see package arpc).
const (
	arpcHeadLen      = 16
	arpcIndexCmd     = 5
	arpcIndexFlag    = 6
	arpcIndexMethod  = 7
	arpcCmdMask      = 0x3f
	arpcFlagMask     = 0x03
	arpcMaxMethodLen = 127
	arpcCmdRequest   = 1
	arpcCmdNotify    = 3
	arpcCmdPing      = 4
	arpcCmdStream    = 6
	// DefaultARPCMaxBodyLen is the longest body ARPC takes for an arpc
	// message's, package arpc's DefaultMaxBodyLen.
	DefaultARPCMaxBodyLen = 1024*1024*64 - 16
)

// ARPC matches the first message an arpc client sends: a request, a notify,
// a stream message or a ping, whose header is consistent and whose method
// name, if it has one, holds no control characters. arpc has no magic number,
// so this is a heuristic, strict enough not to take TLS, SOCKS or HTTP, and
// best listed after the routes that recognise those. maxBodyLen bounds the
// body a header may announce; zero or less means DefaultARPCMaxBodyLen.
//
// Only an arpc client that speaks first is recognised; one that connects and
// waits for the server to send is closed when the mux's Timeout passes.
func ARPC(maxBodyLen int) mux.Matcher {
	if maxBodyLen <= 0 {
		maxBodyLen = DefaultARPCMaxBodyLen
	}
	return func(input []byte) mux.Result {
		if len(input) >= 4 && uint64(binary.LittleEndian.Uint32(input)) > uint64(maxBodyLen) {
			return mux.NoMatch
		}
		if len(input) > arpcIndexCmd {
			switch input[arpcIndexCmd] & arpcCmdMask {
			case arpcCmdRequest, arpcCmdNotify, arpcCmdPing, arpcCmdStream:
			default:
				// None, or what only answers the other side: a response or
				// a pong.
				return mux.NoMatch
			}
		}
		if len(input) > arpcIndexFlag && input[arpcIndexFlag]&^arpcFlagMask != 0 {
			return mux.NoMatch
		}
		if len(input) > arpcIndexMethod && input[arpcIndexMethod] > arpcMaxMethodLen {
			return mux.NoMatch
		}
		if len(input) < arpcHeadLen {
			return mux.NeedMore
		}
		bodyLen := int(binary.LittleEndian.Uint32(input))
		methodLen := int(input[arpcIndexMethod])
		cmd := input[arpcIndexCmd] & arpcCmdMask
		switch {
		case methodLen > bodyLen:
			return mux.NoMatch
		case cmd == arpcCmdPing && bodyLen != 0:
			return mux.NoMatch
		case (cmd == arpcCmdRequest || cmd == arpcCmdNotify) && methodLen == 0:
			return mux.NoMatch
		}
		method := input[arpcHeadLen:min(len(input), arpcHeadLen+methodLen)]
		for _, b := range method {
			if b < 0x20 || b == 0x7f {
				return mux.NoMatch
			}
		}
		if len(method) < methodLen {
			return mux.NeedMore
		}
		return mux.Match
	}
}

// except matches what m does, unless other matches it too, which then takes
// it. While other needs more, so does the result, unless settled says the
// input is all m's client sends before it waits for an answer.
func except(m, other mux.Matcher, settled func([]byte) bool) mux.Matcher {
	return func(input []byte) mux.Result {
		if r := m(input); r != mux.Match {
			return r
		}
		switch other(input) {
		case mux.Match:
			return mux.NoMatch
		case mux.NoMatch:
			return mux.Match
		}
		if settled != nil && settled(input) {
			return mux.Match
		}
		return mux.NeedMore
	}
}

// socksSettled reports whether input is a whole SOCKS5 greeting or a whole
// SOCKS4 or SOCKS4a request, after which a SOCKS client waits for the server.
func socksSettled(input []byte) bool {
	switch {
	case len(input) >= 2 && input[0] == 5:
		return len(input) == 2+int(input[1])
	case len(input) >= 8 && input[0] == 4:
		// The user id after the address ends with a NUL, and so does the
		// host of SOCKS4a, whose address is 0.0.0.x with x not zero.
		nul := bytes.IndexByte(input[8:], 0)
		if nul < 0 {
			return false
		}
		end := 8 + nul + 1
		if input[4] == 0 && input[5] == 0 && input[6] == 0 && input[7] != 0 {
			host := bytes.IndexByte(input[end:], 0)
			if host < 0 {
				return false
			}
			end += host + 1
		}
		return len(input) == end
	}
	return false
}
