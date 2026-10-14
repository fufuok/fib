package http

import (
	"errors"
	"io"
	"strings"
	"testing"
)

// TestParserDefaultsAndReset checks that a parser made from an empty Config
// takes DefaultConfig's limits rather than refusing every header and body,
// and that Reset readies it for another connection mid-request.
func TestParserDefaultsAndReset(t *testing.T) {
	parser := NewParser(Config{})
	defaults := DefaultConfig()
	if parser.config.MaxHeaderBytes != defaults.MaxHeaderBytes || parser.config.MaxBodyBytes != defaults.MaxBodyBytes {
		t.Fatalf("limits %d/%d, want the defaults %d/%d", parser.config.MaxHeaderBytes, parser.config.MaxBodyBytes,
			defaults.MaxHeaderBytes, defaults.MaxBodyBytes)
	}
	if requests, err := parser.Feed([]byte("POST /half HTTP/1.1\r\nHost: test\r\nContent-Length: 10\r\n\r\nabc")); err != nil || len(requests) != 0 {
		t.Fatalf("a partial request gave %d requests, %v", len(requests), err)
	}
	parser.Reset()
	// Nothing of the request the connection was sending is left: the next
	// connection starts with a request of its own.
	requests, err := parser.Feed([]byte("POST /whole HTTP/1.1\r\nHost: test\r\nContent-Length: 2\r\n\r\nok"))
	if err != nil || len(requests) != 1 {
		t.Fatalf("after Reset: %d requests, %v", len(requests), err)
	}
	body, _ := io.ReadAll(requests[0].Body)
	if requests[0].URL.Path != "/whole" || string(body) != "ok" {
		t.Fatalf("after Reset: %s %q", requests[0].URL.Path, body)
	}
}

// TestParserRefusals feeds requests whose framing the parser refuses, each
// in one piece as a client that sends it all at once would, and checks the
// error each is refused with, which is what decides the status the server
// answers with.
func TestParserRefusals(t *testing.T) {
	small := DefaultConfig()
	small.MaxHeaderBytes = 256
	small.MaxBodyBytes = 16
	longExtension := ";ext=" + strings.Repeat("e", 300)
	cases := []struct {
		name    string
		config  Config
		request string
		want    error
	}{{
		// The end of the header is in the buffer, past the limit.
		name:    "header past the limit",
		config:  small,
		request: "GET / HTTP/1.1\r\nHost: test\r\nX-Long: " + strings.Repeat("h", 300) + "\r\n\r\n",
		want:    ErrHeaderTooLarge,
	}, {
		// Every chunk is within the body's limit, but their framing is
		// longer than any header the server takes.
		name:   "chunk framing past the limits",
		config: small,
		request: "POST / HTTP/1.1\r\nHost: test\r\nTransfer-Encoding: chunked\r\n\r\n" +
			"1" + longExtension + "\r\na\r\n0\r\n\r\n",
		want: ErrBodyTooLarge,
	}, {
		name:   "trailer past the limit",
		config: small,
		request: "POST / HTTP/1.1\r\nHost: test\r\nTransfer-Encoding: chunked\r\n\r\n" +
			"1\r\na\r\n0\r\nX-Trailer: " + strings.Repeat("t", 300) + "\r\n\r\n",
		want: ErrHeaderTooLarge,
	}, {
		name:   "unterminated trailer past the limit",
		config: small,
		request: "POST / HTTP/1.1\r\nHost: test\r\nTransfer-Encoding: chunked\r\n\r\n" +
			"1\r\na\r\n0\r\nX-Trailer: " + strings.Repeat("t", 300),
		want: ErrHeaderTooLarge,
	}, {
		name:   "chunk without its CRLF",
		config: DefaultConfig(),
		request: "POST / HTTP/1.1\r\nHost: test\r\nTransfer-Encoding: chunked\r\n\r\n" +
			"1\r\naXX0\r\n\r\n",
		want: ErrMalformed,
	}, {
		// The framing is sound, so it is net/http's decoder that reads the
		// body with its trailer, and finds the trailer malformed.
		name:   "malformed trailer",
		config: DefaultConfig(),
		request: "POST / HTTP/1.1\r\nHost: test\r\nTransfer-Encoding: chunked\r\n\r\n" +
			"1\r\na\r\n0\r\nno colon here\r\n\r\n",
		want: ErrMalformed,
	}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			requests, err := NewParser(tc.config).Feed([]byte(tc.request))
			if !errors.Is(err, tc.want) {
				t.Fatalf("Feed = %d requests, %v; want %v", len(requests), err, tc.want)
			}
		})
	}
}

// TestParserUnterminatedTrailerWaits checks that a trailer still within the
// limit and not yet ended is waited for rather than refused.
func TestParserUnterminatedTrailerWaits(t *testing.T) {
	parser := NewParser(DefaultConfig())
	head := "POST / HTTP/1.1\r\nHost: test\r\nTransfer-Encoding: chunked\r\n\r\n1\r\na\r\n0\r\nX-Sum: 1"
	if requests, err := parser.Feed([]byte(head)); err != nil || len(requests) != 0 {
		t.Fatalf("a partial trailer gave %d requests, %v", len(requests), err)
	}
	requests, err := parser.Feed([]byte("\r\n\r\n"))
	if err != nil || len(requests) != 1 {
		t.Fatalf("the rest of the trailer gave %d requests, %v", len(requests), err)
	}
	body, _ := io.ReadAll(requests[0].Body)
	if string(body) != "a" || requests[0].Trailer.Get("X-Sum") != "1" {
		t.Fatalf("body %q, trailer %v", body, requests[0].Trailer)
	}
}

// TestPlainChunkSizeRejects checks the lines plainChunkSize leaves to
// net/http's decoder: a size larger than what follows it, and a line that
// runs out before its CRLF.
func TestPlainChunkSizeRejects(t *testing.T) {
	for _, line := range []string{"ff\r\nabc", "12", "1\r", ""} {
		if size, n := plainChunkSize([]byte(line)); n != 0 {
			t.Fatalf("plainChunkSize(%q) = %d, %d; want it refused", line, size, n)
		}
	}
}
