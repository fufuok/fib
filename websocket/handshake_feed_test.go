package websocket

import (
	"errors"
	"strings"
	"testing"
)

func TestHandshakeParserFeedLimits(t *testing.T) {
	// A complete handshake past the limit, and an incomplete one, in one read.
	p := &handshakeParser{maxHeaderBytes: 32}
	if _, _, err := p.Feed([]byte(minimalHandshake + "\r\n")); !errors.Is(err, errHandshakeHeaderTooLarge) {
		t.Fatalf("complete: %v", err)
	}
	p = &handshakeParser{maxHeaderBytes: 32}
	if _, _, err := p.Feed([]byte(strings.Repeat("x", 33))); !errors.Is(err, errHandshakeHeaderTooLarge) {
		t.Fatalf("incomplete: %v", err)
	}

	// The same, buffered across reads.
	p = &handshakeParser{maxHeaderBytes: 32}
	if _, complete, err := p.Feed([]byte("GET / HTTP/1.1\r\n")); complete || err != nil {
		t.Fatalf("first part: %v %v", complete, err)
	}
	if _, _, err := p.Feed([]byte("Host: a-host-name-that-is-long\r\n\r\n")); !errors.Is(err, errHandshakeHeaderTooLarge) {
		t.Fatalf("buffered complete: %v", err)
	}
	p = &handshakeParser{maxHeaderBytes: 32}
	_, _, _ = p.Feed([]byte("GET / HTTP/1.1\r\n"))
	if _, _, err := p.Feed([]byte(strings.Repeat("x", 20))); !errors.Is(err, errHandshakeHeaderTooLarge) {
		t.Fatalf("buffered incomplete: %v", err)
	}
}

func TestHandshakeParserFeedShortPieces(t *testing.T) {
	p := &handshakeParser{maxHeaderBytes: 1024}
	if _, complete, err := p.Feed([]byte("GE")); complete || err != nil || p.headerScan != 0 {
		t.Fatalf("complete %v err %v scan %d", complete, err, p.headerScan)
	}
	if _, _, err := p.Feed([]byte("T\r\n\r\n")); !errors.Is(err, errMalformedHandshake) {
		t.Fatalf("request line without target: %v", err)
	}

	p = &handshakeParser{maxHeaderBytes: 1024}
	_, _, _ = p.Feed([]byte("GET /chat HTTP/1.1\r\nHost: test\r\n"))
	request, complete, err := p.Feed([]byte("\r\nframe"))
	if err != nil || !complete || request.Host != "test" {
		t.Fatalf("request %v complete %v err %v", request, complete, err)
	}
	if rest := p.TakeBuffered(); string(rest) != "frame" {
		t.Fatalf("buffered %q", rest)
	}
	_, _, _ = p.Feed([]byte("GET"))
	p.Reset()
	if p.buffer != nil || p.headerScan != 0 {
		t.Fatal("Reset kept state")
	}
}

func TestParseHandshakeRequestRejects(t *testing.T) {
	for _, tc := range []struct{ name, data string }{
		{"empty request line", "\r\n\r\n"},
		{"no target", "GET /\r\n\r\n"},
		{"bad method", "G@T / HTTP/1.1\r\nHost: a\r\n\r\n"},
		{"old protocol", "GET / HTTP/1.0\r\nHost: a\r\n\r\n"},
		{"bad target", "GET chat HTTP/1.1\r\nHost: a\r\n\r\n"},
		{"empty line inside", "GET / HTTP/1.1\r\nHost: a\r\n\r\nX: y\r\n\r\n"},
		{"folded line", "GET / HTTP/1.1\r\n Host: a\r\n\r\n"},
		{"bad name", "GET / HTTP/1.1\r\nB@d: x\r\nHost: a\r\n\r\n"},
		{"control in value", "GET / HTTP/1.1\r\nHost: a\x01\r\n\r\n"},
		{"second host", "GET / HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n"},
		{"no host", "GET / HTTP/1.1\r\nUpgrade: websocket\r\n\r\n"},
		{"body", "GET / HTTP/1.1\r\nHost: a\r\nContent-Length: 1\r\n\r\n"},
	} {
		if _, err := parseHandshakeRequest([]byte(tc.data)); !errors.Is(err, errMalformedHandshake) {
			t.Errorf("%s: %v", tc.name, err)
		}
	}

	request, err := parseHandshakeRequest([]byte("GET /a%20b?q=1 HTTP/1.1\r\nHost: a\r\nX-Custom: v\r\n\r\n"))
	if err != nil || request.URL.Path != "/a b" || request.URL.RawQuery != "q=1" || request.Header.Get("X-Custom") != "v" {
		t.Fatalf("request %+v, %v", request, err)
	}
}
