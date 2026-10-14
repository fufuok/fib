package websocket

import (
	"errors"
	"io"
	"testing"
)

// TestParseExtensionsSyntax checks the corners of the Sec-WebSocket-Extensions
// grammar (RFC 6455 section 9.1) that the offer and response tests do not:
// a quoted value with an escape, and the malformed lists a peer may send.
func TestParseExtensionsSyntax(t *testing.T) {
	extensions, ok := parseExtensions([]string{`permessage-deflate; server_max_window_bits="1\0"`})
	if !ok || len(extensions) != 1 || extensions[0][1].value != "10" {
		t.Fatalf("escaped quoted value: %+v, ok %v", extensions, ok)
	}
	for _, value := range []string{
		"permessage-deflate=1",                             // a value on the extension's name
		"permessage-deflate x",                             // a parameter without ';'
		`permessage-deflate; client_max_window_bits="1\`,   // an escape at the end
		`permessage-deflate; client_max_window_bits="a b"`, // a quoted value that is no token
	} {
		if extensions, ok := parseExtensions([]string{value}); ok {
			t.Errorf("%q parsed as %+v", value, extensions)
		}
	}
}

// TestAcceptDeflateResponseRejectsBadParameters checks that a client refuses
// a server's answer that repeats a parameter, or gives a value to one that
// takes none, as RFC 7692 section 7 has it fail the connection.
func TestAcceptDeflateResponseRejectsBadParameters(t *testing.T) {
	for _, value := range []string{
		"permessage-deflate; server_no_context_takeover; server_no_context_takeover",
		"permessage-deflate; server_no_context_takeover=1",
		"permessage-deflate; client_no_context_takeover=yes",
		"permessage-deflate=x",
	} {
		if params, enabled, ok := acceptDeflateResponse([]string{value}); ok || enabled {
			t.Errorf("%q accepted: %+v, enabled %v", value, params, enabled)
		}
	}
}

// TestSliceReaderEOF checks that the reader a decompressor is fed reports the
// end of what it holds through both of the ways flate may read it.
func TestSliceReaderEOF(t *testing.T) {
	r := &sliceReader{parts: [2][]byte{[]byte("ab"), []byte("c")}}
	data, err := io.ReadAll(r)
	if err != nil || string(data) != "abc" {
		t.Fatalf("ReadAll = %q, %v", data, err)
	}
	if _, err := r.ReadByte(); err != io.EOF {
		t.Fatalf("ReadByte at the end: %v, want io.EOF", err)
	}
}

// TestParserRejectsBadFragmentedCompressedMessages checks a compressed message
// in several frames that does not inflate, or inflates to text that is not
// UTF-8: either is found once the last frame arrives, since only the whole
// message can be inflated.
func TestParserRejectsBadFragmentedCompressedMessages(t *testing.T) {
	split := func(opcode Opcode, payload []byte) []byte {
		half := len(payload) / 2
		return append(deflateFrame(opcode, false, true, payload[:half]), deflateFrame(Continuation, true, false, payload[half:])...)
	}
	for _, tc := range []struct {
		name   string
		stream []byte
		want   error
	}{
		{"corrupt", split(Binary, []byte{0xff, 0xff, 0xff, 0xff}), ErrInvalidPayload},
		{"invalid utf8", split(Text, compressed(t, []byte("abc\xff\xfedef"))), ErrInvalidPayload},
	} {
		for _, perFrame := range []bool{false, true} {
			parser := deflateParser(1 << 10)
			parser.SetPerFrame(perFrame)
			if _, err := parser.Feed(tc.stream); !errors.Is(err, tc.want) {
				t.Errorf("%s (per frame %v): error %v, want %v", tc.name, perFrame, err, tc.want)
			}
		}
	}
}
