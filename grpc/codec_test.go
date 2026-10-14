//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package grpc

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/lesismal/fib/grpc/codes"
	"github.com/lesismal/fib/grpc/metadata"
	"github.com/lesismal/fib/hpack"
)

// vtMessage encodes itself as vtprotobuf's messages do, gogoMessage as
// gogo/protobuf's do; plainMessage does neither.
type vtMessage struct{ b []byte }

func (m *vtMessage) MarshalVT() ([]byte, error)    { return append([]byte("vt:"), m.b...), nil }
func (m *vtMessage) UnmarshalVT(data []byte) error { m.b = append(m.b[:0], data...); return nil }

type gogoMessage struct{ b []byte }

func (m *gogoMessage) Marshal() ([]byte, error)    { return append([]byte("gogo:"), m.b...), nil }
func (m *gogoMessage) Unmarshal(data []byte) error { m.b = append(m.b[:0], data...); return nil }

type plainMessage struct{}

// TestProtoCodec pins down what the built-in proto codec encodes: messages
// with vtprotobuf's methods or gogo/protobuf's, and nothing else, which it
// refuses with an error naming the fix rather than encoding wrongly.
func TestProtoCodec(t *testing.T) {
	c := GetCodec("PROTO")
	if c == nil || c.Name() != "proto" {
		t.Fatalf("GetCodec(PROTO) = %v", c)
	}
	if b, err := c.Marshal(&vtMessage{b: []byte("x")}); err != nil || string(b) != "vt:x" {
		t.Fatalf("vt Marshal = %q, %v", b, err)
	}
	if b, err := c.Marshal(&gogoMessage{b: []byte("x")}); err != nil || string(b) != "gogo:x" {
		t.Fatalf("gogo Marshal = %q, %v", b, err)
	}
	vt, gogo := &vtMessage{}, &gogoMessage{}
	if err := c.Unmarshal([]byte("a"), vt); err != nil || string(vt.b) != "a" {
		t.Fatalf("vt Unmarshal = %q, %v", vt.b, err)
	}
	if err := c.Unmarshal([]byte("b"), gogo); err != nil || string(gogo.b) != "b" {
		t.Fatalf("gogo Unmarshal = %q, %v", gogo.b, err)
	}
	if _, err := c.Marshal(&plainMessage{}); err == nil || !strings.Contains(err.Error(), "RegisterCodec") {
		t.Fatalf("Marshal of a plain message = %v", err)
	}
	if err := c.Unmarshal(nil, &plainMessage{}); err == nil || !strings.Contains(err.Error(), "RegisterCodec") {
		t.Fatalf("Unmarshal into a plain message = %v", err)
	}
}

// TestRegisterNilPanics checks that registering a codec or compressor that
// could never be found by name panics at once.
func TestRegisterNilPanics(t *testing.T) {
	for name, register := range map[string]func(){
		"nil codec":      func() { RegisterCodec(nil) },
		"nameless codec": func() { RegisterCodec(namedCodec("")) },
		"nil compressor": func() { RegisterCompressor(nil) },
		"nameless comp":  func() { RegisterCompressor(&failingCompressor{}) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s did not panic", name)
				}
			}()
			register()
		}()
	}
}

// namedCodec is the JSON codec under another name.
type namedCodec string

func (c namedCodec) Name() string                     { return string(c) }
func (namedCodec) Marshal(v any) ([]byte, error)      { return jsonCodec{}.Marshal(v) }
func (namedCodec) Unmarshal(data []byte, v any) error { return jsonCodec{}.Unmarshal(data, v) }

// failingCompressor fails at whichever step its fields name.
type failingCompressor struct {
	name                                           string
	failCompress, failWrite, failClose, failDecomp bool
}

var errFailingCompressor = errors.New("failing compressor")

func (c *failingCompressor) Name() string { return c.name }

func (c *failingCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	if c.failCompress {
		return nil, errFailingCompressor
	}
	return &failingWriter{w: w, c: c}, nil
}

func (c *failingCompressor) Decompress(r io.Reader) (io.Reader, error) {
	if c.failDecomp {
		return nil, errFailingCompressor
	}
	return r, nil
}

type failingWriter struct {
	w io.Writer
	c *failingCompressor
}

func (w *failingWriter) Write(p []byte) (int, error) {
	if w.c.failWrite {
		return 0, errFailingCompressor
	}
	return w.w.Write(p)
}

func (w *failingWriter) Close() error {
	if w.c.failClose {
		return errFailingCompressor
	}
	return nil
}

// TestCompressErrors checks that compress and decompress hand back a
// compressor's failure at each step, and that decompress refuses output
// over its limit.
func TestCompressErrors(t *testing.T) {
	for _, c := range []*failingCompressor{{failCompress: true}, {failWrite: true}, {failClose: true}} {
		if _, err := compress(c, []byte("data")); err != errFailingCompressor {
			t.Errorf("compress with %+v = %v", *c, err)
		}
	}
	if _, err := decompress(&failingCompressor{failDecomp: true}, []byte("data"), 10); err != errFailingCompressor {
		t.Errorf("decompress = %v", err)
	}
	if _, err := decompress(&failingCompressor{}, []byte("data"), 3); err != errMessageTooLarge {
		t.Errorf("decompress over the limit = %v", err)
	}
	gz := GetCompressor("gzip")
	if _, err := decompress(gz, []byte("not gzip"), 100); err == nil {
		t.Error("decompress of what is not gzip succeeded")
	}
	// Messages compressed one after another reuse the pool's writers.
	for i := range 8 {
		msg := strings.Repeat("round trip ", i+1)
		data, err := compress(gz, []byte(msg))
		if err != nil {
			t.Fatal(err)
		}
		if out, err := decompress(gz, data, 1000); err != nil || string(out) != msg {
			t.Fatalf("round trip %d = %q, %v", i, out, err)
		}
	}
	// A valid gzip header in front of a corrupt stream fails in the read.
	good, err := compress(gz, bytes.Repeat([]byte("abc"), 100))
	if err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte(nil), good[:12]...)
	corrupt = append(corrupt, bytes.Repeat([]byte{0xff}, 20)...)
	if _, err := decompress(gz, corrupt, 1000); err == nil {
		t.Error("decompress of a corrupt stream succeeded")
	}
}

// TestGzipReaderPool checks that the gzip compressor reuses its readers:
// one drained goes back to the pool, and a later message is read with it.
// Before the fix only a reader taken from the pool was ever put back, so
// the pool stayed empty and every message made a new reader.
func TestGzipReaderPool(t *testing.T) {
	c := &gzipCompressor{}
	data, err := compress(c, []byte(strings.Repeat("pooled ", 50)))
	if err != nil {
		t.Fatal(err)
	}
	seen := map[*gzip.Reader]bool{}
	// sync.Pool may drop what it is given, under the race detector a
	// quarter of the time: a reuse among many tries is what is certain.
	for range 32 {
		r, err := c.Decompress(bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		pr, ok := r.(*pooledGzipReader)
		if !ok {
			t.Fatalf("Decompress returned %T, which never goes back to the pool", r)
		}
		if seen[pr.Reader] {
			return
		}
		seen[pr.Reader] = true
		if out, err := io.ReadAll(r); err != nil || string(out) != strings.Repeat("pooled ", 50) {
			t.Fatalf("read %q, %v", out, err)
		}
	}
	t.Fatal("no gzip reader was ever reused")
}

// TestGzipReaderPoolBadHeader checks that a pooled reader which fails to
// reset on a bad stream goes back to the pool rather than being lost.
func TestGzipReaderPoolBadHeader(t *testing.T) {
	c := &gzipCompressor{}
	z, err := gzip.NewReader(bytes.NewReader(mustGzip(t, "x")))
	if err != nil {
		t.Fatal(err)
	}
	c.readers.Put(z)
	if _, err := c.Decompress(strings.NewReader("not gzip")); err == nil {
		t.Fatal("Decompress of what is not gzip succeeded")
	}
}

func mustGzip(t *testing.T, s string) []byte {
	t.Helper()
	b, err := compress(&gzipCompressor{}, []byte(s))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestHeaderHelpers covers the edges of the header encodings: timeouts at
// and past their bounds, malformed values, binary metadata, header names
// that cannot be sent, and the HTTP statuses a response without a gRPC
// status maps to.
func TestHeaderHelpers(t *testing.T) {
	if got := encodeTimeout(0); got != "1n" {
		t.Errorf("encodeTimeout(0) = %q", got)
	}
	for in, want := range map[string]time.Duration{
		"5u": 5 * time.Microsecond, "7m": 7 * time.Millisecond, "2M": 2 * time.Minute, "3H": 3 * time.Hour,
		"99999999H": 1<<63 - 1,
	} {
		if got, ok := decodeTimeout(in); !ok || got != want {
			t.Errorf("decodeTimeout(%q) = %v, %v", in, got, ok)
		}
	}
	for _, bad := range []string{"", "5", "1234567890n", "xxS", "-1S", "5x"} {
		if _, ok := decodeTimeout(bad); ok {
			t.Errorf("decodeTimeout(%q) succeeded", bad)
		}
	}
	if got := decodeGRPCMessage("100%zz%4"); got != "100%zz%4" {
		t.Errorf("a malformed escape came back as %q", got)
	}

	md := metadata.MD{}
	if err := addMetadata(md, "k-bin", "!!!"); err == nil {
		t.Error("addMetadata took malformed base64")
	}
	if err := addMetadata(md, "k-bin", "AQI="); err != nil || md["k-bin"][0] != "\x01\x02" {
		t.Errorf("padded base64 = %q, %v", md["k-bin"], err)
	}

	// Reserved names and names a header cannot carry are left out.
	enc := hpack.NewEncoder()
	block := appendMetadata(enc, enc.Begin(nil), metadata.MD{"grpc-status": {"0"}, "bad name": {"x"}, "": {"x"}, "Ok_.1": {"v"}})
	var names []string
	if err := hpack.NewDecoder(defaultHeaderTable).Decode(block, func(f hpack.HeaderField) error {
		names = append(names, f.Name)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if strings.Join(names, ",") != "ok_.1" {
		t.Errorf("appendMetadata sent %v", names)
	}

	if sub, ok := contentSubtype("application/grpc;charset=utf-8"); !ok || sub != "proto" {
		t.Errorf("parameters = %q, %v", sub, ok)
	}
	if _, ok := contentSubtype("application/grpcx"); ok {
		t.Error("application/grpcx taken for gRPC")
	}
	if contentType("") != "application/grpc" {
		t.Error("contentType of no subtype")
	}
	for status, want := range map[int]codes.Code{
		400: codes.Internal, 401: codes.Unauthenticated, 403: codes.PermissionDenied,
		404: codes.Unimplemented, 429: codes.Unavailable, 502: codes.Unavailable,
		503: codes.Unavailable, 504: codes.Unavailable, 500: codes.Unknown,
	} {
		if got := httpStatusCode(status); got != want {
			t.Errorf("httpStatusCode(%d) = %v, want %v", status, got, want)
		}
	}
}

// TestFrameHelpers covers the framing edges: a header block over the peer's
// frame size goes out as HEADERS and CONTINUATIONs, padding is stripped or
// refused, and the errors describe themselves.
func TestFrameHelpers(t *testing.T) {
	block := bytes.Repeat([]byte{'h'}, 25)
	out := appendHeaderBlock(nil, 1, block, true, 10)
	var types []frameType
	var got []byte
	for len(out) > 0 {
		f := parseFrameHeader(out)
		types = append(types, f.typ)
		end := frameHeaderLen + int(f.length)
		got = append(got, out[frameHeaderLen:end]...)
		if f.typ == frameContinuation && f.has(flagEndStream) {
			t.Error("a CONTINUATION carries END_STREAM")
		}
		if (len(out) == end) != f.has(flagEndHeaders) {
			t.Error("END_HEADERS is not on the last frame alone")
		}
		out = out[end:]
	}
	if len(types) != 3 || types[0] != frameHeaders || types[1] != frameContinuation || types[2] != frameContinuation || !bytes.Equal(got, block) {
		t.Fatalf("frames %v carried %q", types, got)
	}

	f := &frame{flags: flagPadded, payload: []byte{2, 'a', 'b', 0, 0}}
	if err := stripPadding(f); err != nil || string(f.payload) != "ab" {
		t.Errorf("stripPadding = %q, %v", f.payload, err)
	}
	for _, payload := range [][]byte{{}, {3, 'a', 0}} {
		if err := stripPadding(&frame{flags: flagPadded, payload: payload}); err == nil {
			t.Errorf("stripPadding of %v succeeded", payload)
		}
	}
	if s := (&connError{code: errProtocol, msg: "m"}).Error(); !strings.Contains(s, "error 1: m") {
		t.Errorf("connError = %q", s)
	}
	if s := (&streamError{id: 3, code: errCancel}).Error(); !strings.Contains(s, "stream 3 error 8") {
		t.Errorf("streamError = %q", s)
	}
}
