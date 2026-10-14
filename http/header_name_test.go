//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"io"
	"net"
	stdhttp "net/http"
	"strings"
	"testing"
	"time"
)

// A field name has to be a token (RFC 9110 section 5.1). Canonicalising
// leaves one that is not, such as "Bad Name", as it is, so HTTP/1 has to
// check the name itself: written out, "Bad Name: x" is a line the client
// cannot parse, or reads as something else. HTTP/2 and HTTP/3 already refuse
// it, and so must HTTP/1, whether the response is written whole or streamed.
func TestHTTP1RefusesAnInvalidHeaderName(t *testing.T) {
	errs := make(chan error, 1)
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		switch c.Request.URL.Path {
		case "/whole":
			errs <- c.WriteResponse(Response{Header: stdhttp.Header{"Bad Name": {"x"}}, Body: []byte("bad")})
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("ok"))
		case "/streamed":
			c.Header()["Bad Name"] = []string{"x"}
			_, err := c.Write([]byte("streamed"))
			if err == nil {
				err = c.FlushError()
			}
			errs <- err
		}
	})))

	conn := dialRaw(t, addr)
	conn.send("GET /whole HTTP/1.1\r\nHost: x\r\n\r\n")
	if err := <-errs; err == nil || !strings.Contains(err.Error(), "invalid response header name") {
		t.Fatalf("whole response: %v", err)
	}
	if resp, body := conn.response(stdhttp.MethodGet); resp.StatusCode != stdhttp.StatusOK || body != "ok" {
		t.Fatalf("whole response: %d %q", resp.StatusCode, body)
	}

	raw, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	_ = raw.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := io.WriteString(raw, "GET /streamed HTTP/1.1\r\nHost: x\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	if err := <-errs; err == nil || !strings.Contains(err.Error(), "invalid response header name") {
		t.Fatalf("streamed response: %v", err)
	}
	// The connection may answer with an error or just close; what it must
	// not do is put the field on the wire.
	got, _ := io.ReadAll(raw)
	if strings.Contains(string(got), "Bad Name") {
		t.Fatalf("the invalid field went out: %q", got)
	}
}
