package mux

// Result is a Matcher's verdict on the bytes a connection has sent so far.
type Result uint8

const (
	// NoMatch says the bytes are not the protocol's, whatever follows them.
	NoMatch Result = iota
	// NeedMore says the bytes so far could begin the protocol, and more of
	// them would settle it.
	NeedMore
	// Match says the bytes are the protocol's.
	Match
)

// Matcher recognises a protocol from the first bytes a connection sends.
// input is everything sent so far, at least one byte of it; a Matcher may be
// asked again with more, each time it answers NeedMore, until the mux has
// read Config.MaxPeek bytes. It must not keep input, and it runs on the
// connection's worker, so it must not block.
type Matcher func(input []byte) Result

// tlsRecordHandshake is the content type of a TLS handshake record, which a
// ClientHello travels in.
const tlsRecordHandshake = 0x16

// TLS matches a TLS ClientHello: a handshake record of TLS 1.0 or later,
// which is what every TLS version, 1.3 included, opens with. Inside the TLS
// stream ALPN picks the protocol, as fibhttp.ConfigureTLS has HTTP/1.1 and
// HTTP/2 share one TLS handler.
func TLS() Matcher {
	return func(input []byte) Result {
		if input[0] != tlsRecordHandshake {
			return NoMatch
		}
		if len(input) < 3 {
			return NeedMore
		}
		// The record's legacy version is 3.1 (TLS 1.0) for every hello since,
		// and 3.0 for SSL 3.0, which stays refused by the handler anyway.
		if input[1] != 3 || input[2] > 4 {
			return NoMatch
		}
		return Match
	}
}

// http1Methods are the request lines HTTP1 recognises, each a method and the
// space that ends it: the methods of RFC 9110 and PATCH, and those of
// WebDAV, which ride on the same handlers.
var http1Methods = []string{
	"GET ", "HEAD ", "POST ", "PUT ", "DELETE ", "CONNECT ", "OPTIONS ", "TRACE ", "PATCH ",
	"PROPFIND ", "PROPPATCH ", "MKCOL ", "COPY ", "MOVE ", "LOCK ", "UNLOCK ", "REPORT ", "SEARCH ",
}

// HTTP1 matches an HTTP/1.x request: a request line that starts with one of
// the methods of HTTP or WebDAV. A WebSocket handshake is one too, so a
// WebSocket handler takes this route as an HTTP one does.
func HTTP1() Matcher { return Prefix(http1Methods...) }

// http2Preface is what an HTTP/2 client sends first on a connection without
// TLS, which it knows to speak HTTP/2 to (h2c with prior knowledge).
const http2Preface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"

// HTTP2 matches the HTTP/2 connection preface a client with prior knowledge
// sends: h2c, as gRPC without TLS speaks.
func HTTP2() Matcher { return Prefix(http2Preface) }

// HTTP matches HTTP/1.x and HTTP/2 with prior knowledge, which
// fibhttp.NewHandler serves both of on one connection.
func HTTP() Matcher { return AnyOf(HTTP1(), HTTP2()) }

// Prefix matches a connection whose first bytes are one of prefixes, as an
// RPC protocol's magic number or a text protocol's first command would be.
// An empty prefix matches everything.
func Prefix(prefixes ...string) Matcher {
	return func(input []byte) Result {
		result := NoMatch
		for _, p := range prefixes {
			n := min(len(input), len(p))
			if string(input[:n]) != p[:n] {
				continue
			}
			if n == len(p) {
				return Match
			}
			result = NeedMore
		}
		return result
	}
}

// SOCKS5 matches a SOCKS5 client's greeting: version 5 and a count of the
// authentication methods it offers, which is never zero.
func SOCKS5() Matcher {
	return func(input []byte) Result {
		if input[0] != 5 {
			return NoMatch
		}
		if len(input) < 2 {
			return NeedMore
		}
		if input[1] == 0 {
			return NoMatch
		}
		return Match
	}
}

// SOCKS4 matches a SOCKS4 or SOCKS4a request: version 4 and a CONNECT (1)
// or a BIND (2).
func SOCKS4() Matcher {
	return func(input []byte) Result {
		if input[0] != 4 {
			return NoMatch
		}
		if len(input) < 2 {
			return NeedMore
		}
		if input[1] != 1 && input[1] != 2 {
			return NoMatch
		}
		return Match
	}
}

// SOCKS matches SOCKS5, SOCKS4 and SOCKS4a, which a socks.Server serves
// all of.
func SOCKS() Matcher { return AnyOf(SOCKS5(), SOCKS4()) }

// AnyOf matches what any of matchers does. It needs more as long as one of
// them does and none has matched.
func AnyOf(matchers ...Matcher) Matcher {
	return func(input []byte) Result {
		result := NoMatch
		for _, m := range matchers {
			switch m(input) {
			case Match:
				return Match
			case NeedMore:
				result = NeedMore
			}
		}
		return result
	}
}

// Any matches every connection, for a last route that takes whatever the
// routes before it did not; Config.Fallback does the same.
func Any() Matcher { return func([]byte) Result { return Match } }
