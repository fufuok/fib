//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	stdhttp "net/http"
	"testing"
	"time"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3/qpack"
)

// TestStreamRequestBodyAtHeader checks that a request picked by
// StreamRequestBodyAtHeader reaches its handler with its first small DATA
// frame, with or without a content-length, and that one not picked still
// waits for the threshold or the end.
func TestStreamRequestBodyAtHeader(t *testing.T) {
	started := make(chan string, 4)
	base := startServer(t, Config{StreamRequestBody: true, StreamRequestBodyThreshold: 64 << 10,
		StreamRequestBodyAtHeader: func(r *stdhttp.Request) bool { return r.Header.Get("Content-Type") == "application/grpc" }},
		func(c *fibhttp.Context) {
			if c.BodyComplete() {
				started <- "whole " + string(c.Body())
				_ = c.Respond(stdhttp.StatusOK, "text/plain", c.Body())
				return
			}
			var got []byte
			c.OnBody(func(data []byte, fin bool, err error) {
				if len(got) == 0 && len(data) > 0 {
					started <- "streamed " + string(data)
				}
				got = append(got, data...)
				if fin || err != nil {
					_ = c.Respond(stdhttp.StatusOK, "text/plain", got)
				}
			})
		})
	w := dialH3Wire(t, nil, base)
	wait := func() string {
		t.Helper()
		select {
		case got := <-started:
			return got
		case <-time.After(5 * time.Second):
			t.Fatal("the handler did not run")
			return ""
		}
	}
	data := func(s string) []byte { return append(appendFrameHeader(nil, frameData, len(s)), s...) }
	head := func(contentType string, extra ...qpack.HeaderField) []byte {
		fields := []qpack.HeaderField{{Name: ":method", Value: "POST"}, {Name: ":scheme", Value: "https"},
			{Name: ":authority", Value: "localhost"}, {Name: ":path", Value: "/"}, {Name: "content-type", Value: contentType}}
		return headersFrame(append(fields, extra...)...)
	}
	for _, tc := range []struct {
		name        string
		contentType string
		extra       []qpack.HeaderField
		first, rest string
		want        string
	}{
		{"picked", "application/grpc", nil, "first", " second", "streamed first"},
		{"picked with a length", "application/grpc", []qpack.HeaderField{{Name: "content-length", Value: "8"}}, "abc", "defgh", "streamed abc"},
		{"not picked", "text/plain", nil, "held", " whole", ""},
	} {
		s, err := w.qc.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Write(append(head(tc.contentType, tc.extra...), data(tc.first)...), false); err != nil {
			t.Fatal(err)
		}
		if tc.want != "" {
			if got := wait(); got != tc.want {
				t.Fatalf("%s: the handler saw %q", tc.name, got)
			}
		} else {
			select {
			case got := <-started:
				t.Fatalf("%s: the handler ran before the body ended: %s", tc.name, got)
			case <-time.After(300 * time.Millisecond):
			}
		}
		if err := s.Write(data(tc.rest), true); err != nil {
			t.Fatal(err)
		}
		if tc.want == "" {
			if got := wait(); got != "whole "+tc.first+tc.rest {
				t.Fatalf("%s: the handler saw %q", tc.name, got)
			}
		}
		if status, body := w.response(s.ID()); status != stdhttp.StatusOK || string(body) != tc.first+tc.rest {
			t.Fatalf("%s: answered %d %q", tc.name, status, body)
		}
	}
}
