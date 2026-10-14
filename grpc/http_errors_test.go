//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package grpc

import (
	"bytes"
	"context"
	"errors"
	"io"
	stdhttp "net/http"
	"strings"
	"testing"
	"time"

	"github.com/lesismal/fib/grpc/codes"
	"github.com/lesismal/fib/grpc/internal/ctxkeys"
	"github.com/lesismal/fib/grpc/metadata"
	"github.com/lesismal/fib/grpc/status"
	fibhttp "github.com/lesismal/fib/http"
)

// startHTTPServer routes the echo service and every /api/ method to a
// Server of opts, through package http with config.
func startHTTPServer(t *testing.T, config fibhttp.Config, opts ...ServerOption) string {
	t.Helper()
	s := NewServer(opts...)
	s.RegisterService(&echoDesc, &echoImpl{})
	r := fibhttp.NewRouter()
	r.Handle("/test.Echo/*", s)
	r.Handle("/api/*", s)
	return startHandler(t, fibhttp.NewHandlerWithConfig(config, r))
}

// TestServeHTTPHandlerAPI runs TestServerHandlerAPI's calls through package
// http, whose handlers see the call through ServeHTTP's stream.
func TestServeHTTPHandlerAPI(t *testing.T) {
	testHandlerAPI(t, func(opts ...ServerOption) string {
		return startHTTPServer(t, streamingConfig(), opts...)
	})
}

// h2cPost sends body to path over HTTP/2 without TLS, as a gRPC client
// would but without one's checks, and returns the call's grpc-status and
// grpc-message, from the trailer or, for a response that is only that, the
// header.
func h2cPost(t *testing.T, addr, path string, body []byte, header ...string) (string, string) {
	t.Helper()
	var h2 stdhttp.Protocols
	h2.SetUnencryptedHTTP2(true)
	transport := &stdhttp.Transport{Protocols: &h2}
	defer transport.CloseIdleConnections()
	req, err := stdhttp.NewRequest("POST", "http://"+addr+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/grpc+json")
	for i := 0; i < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	resp, err := (&stdhttp.Client{Transport: transport}).Do(req.WithContext(ctx))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if _, err := io.ReadAll(resp.Body); err != nil {
		t.Fatal(err)
	}
	if code := resp.Trailer.Get("Grpc-Status"); code != "" {
		return code, resp.Trailer.Get("Grpc-Message")
	}
	return resp.Header.Get("Grpc-Status"), resp.Header.Get("Grpc-Message")
}

// TestServeHTTPMalformedRequests sends ServeHTTP requests no gRPC client
// sends, with the request read whole before the handler runs and as it
// arrives, checking the status each call ends with.
func TestServeHTTPMalformedRequests(t *testing.T) {
	msg := grpcMessage(t, Req{Msg: "x"})
	for _, tc := range []struct {
		name   string
		body   []byte
		header []string
		code   string
	}{
		{"binary metadata that is not base64", msg, []string{"X-Bin", "!!!"}, "13"},
		// A call without a codec used to make ServeHTTP's answer
		// dereference the nil codec, and the handler panic.
		{"an unknown content subtype", msg, []string{"Content-Type", "application/grpc+nope"}, "13"},
		{"an unknown grpc-encoding", msg, []string{"Grpc-Encoding", "bogus"}, "12"},
		{"a compressed message without grpc-encoding", lengthPrefixed(true, []byte("{}")), nil, "13"},
		{"a message that does not decompress", lengthPrefixed(true, []byte("not gzip")), []string{"Grpc-Encoding", "gzip"}, "13"},
		{"a message that does not decode", lengthPrefixed(false, []byte(`"str"`)), nil, "13"},
		{"no message", nil, nil, "2"},
		{"a message header cut short", []byte{0, 0, 0}, nil, "13"},
		{"a message over the limit", []byte{0, 0x10, 0, 0, 0, 'x'}, nil, "8"},
		{"a message cut short", []byte{0, 0, 0, 0, 10, 'a', 'b', 'c'}, nil, "13"},
	} {
		for _, streaming := range []bool{false, true} {
			config := fibhttp.DefaultConfig()
			if streaming {
				config = streamingConfig()
			}
			addr := startHTTPServer(t, config)
			code, message := h2cPost(t, addr, "/test.Echo/Unary", tc.body, tc.header...)
			if code != tc.code {
				t.Errorf("%s (streaming %v): grpc-status %s %q, want %s", tc.name, streaming, code, message, tc.code)
			}
		}
	}
}

// TestServeHTTPCanceledStream checks that a streaming call through package
// http hears its client go while it waits for a message, and ends with the
// status of its canceled context.
func TestServeHTTPCanceledStream(t *testing.T) {
	_, c := dial(t, startHTTPServer(t, streamingConfig()))
	// No deadline: the handler's context is the stream itself, canceled
	// as the client goes.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sum, err := c.Sum(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := sum.Send(&Req{Msg: "x", N: 1}); err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, err := sum.CloseAndRecv(); err == nil {
		t.Fatal("a canceled call succeeded")
	}
}

// TestHTTPStreamWhiteBox covers what ServeHTTP's stream does in the states
// no request puts it in on cue: metadata that cannot be decoded asked for
// as a context value, metadata asked for once the call has ended, a stream
// used after it has finished, a body that fails rather than ends, and the
// pipe the body crosses closed from either end.
func TestHTTPStreamWhiteBox(t *testing.T) {
	r, err := stdhttp.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("K-Bin", "!!!")
	hs := &httpStream{server: NewServer(), r: r}
	if md, ok := hs.Value(ctxkeys.Incoming{}).(metadata.MD); !ok || len(md) != 0 {
		t.Fatalf("the metadata of a request with a malformed binary value = %v", md)
	}
	if _, err := hs.incoming(); err == nil || !strings.Contains(err.Error(), "k-bin") {
		t.Fatalf("incoming = %v", err)
	}
	ended := &httpStream{r: r}
	ended.end()
	if md, err := ended.incoming(); err != nil || md == nil || len(md) != 0 {
		t.Fatalf("metadata asked for after the call ended = %v, %v", md, err)
	}

	// Done asked for only once the call was canceled is closed already.
	hs.cancel(errors.New("gone"))
	select {
	case <-hs.Done():
	default:
		t.Fatal("Done of a canceled call is open")
	}
	hs.done = true
	if err := hs.SendMsg(&Rsp{}); err == nil {
		t.Fatal("SendMsg on a finished stream succeeded")
	}
	hs.finish(nil)

	for key, want := range map[string]string{"User-Agent": "user-agent", "Te": "te", "Content-Type": "content-type",
		"Grpc-Timeout": "grpc-timeout", "Grpc-Encoding": "grpc-encoding", "Grpc-Accept-Encoding": "grpc-accept-encoding", "X-Other": "x-other"} {
		if got := lowerKey(key); got != want {
			t.Errorf("lowerKey(%q) = %q", key, got)
		}
	}
	h := stdhttp.Header{}
	setMetadata(h, "", metadata.MD{"grpc-status": {"0"}, "bad name": {"x"}, "k": {"v"}})
	if len(h) != 1 || h.Get("K") != "v" {
		t.Errorf("setMetadata = %v", h)
	}

	// A body that fails in the middle of a message, with the call's
	// context still live.
	broken := &httpStream{server: NewServer(), in: newBodyPipe(), ctx: context.Background()}
	go broken.in.write([]byte{0, 0}, false, errors.New("the body broke"))
	if _, _, err := broken.readMsg(); status.Code(err) != codes.Unavailable {
		t.Fatalf("a body that broke = %v", err)
	}

	// A read the call's cancellation ends gives the context's status.
	canceled := &httpStream{server: NewServer(), in: newBodyPipe()}
	canceled.ctx = canceled
	canceled.cancel(errors.New("reset"))
	if _, _, err := canceled.readMsg(); status.Code(err) != codes.Canceled {
		t.Fatalf("a read of a canceled call = %v", err)
	}

	// Closed by the reader: what arrives is dropped, and a read finds the
	// pipe closed.
	p := newBodyPipe()
	p.close(nil)
	p.write([]byte("dropped"), false, nil)
	if n, err := p.Read(make([]byte, 4)); n != 0 || err != io.ErrClosedPipe {
		t.Fatalf("Read of a closed pipe = %d, %v", n, err)
	}
	// Closed after the body ended: the end is what a read finds.
	p = newBodyPipe()
	p.write([]byte("ab"), true, nil)
	p.close(io.ErrClosedPipe)
	buf := make([]byte, 4)
	if n, err := p.Read(buf); n != 2 || err != nil {
		t.Fatalf("Read = %d, %v", n, err)
	}
	if _, err := p.Read(buf); err != io.EOF {
		t.Fatalf("Read after the end = %v", err)
	}
	// Taking part of what arrived and then more arriving moves the rest to
	// the front.
	p = newBodyPipe()
	p.write([]byte("abcd"), false, nil)
	if n, _ := p.Read(buf[:3]); n != 3 {
		t.Fatalf("Read = %d", n)
	}
	p.write([]byte("ef"), true, nil)
	if n, err := p.Read(buf); n != 3 || err != nil || string(buf[:n]) != "def" {
		t.Fatalf("Read after compaction = %q, %v", buf[:n], err)
	}
}
