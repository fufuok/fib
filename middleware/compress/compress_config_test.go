//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package compress_test

import (
	"crypto/rand"
	stdhttp "net/http"
	"strconv"
	"testing"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/middleware/compress"
	"github.com/lesismal/fib/middleware/mwtest"
)

// TestCompressSkipsWhatItMustNotTouch checks the responses compress leaves
// as they are: those its Next skips, partial ones, those whose Cache-Control
// forbids transforming them, those that compress to no fewer bytes, and the
// response to HEAD, which still says in Vary that a GET's would differ.
func TestCompressSkipsWhatItMustNotTouch(t *testing.T) {
	random := make([]byte, 4096)
	_, _ = rand.Read(random)
	handler := compress.New(compress.Config{
		Next: func(c *fibhttp.Context) bool { return c.Request.URL.Path == "/skip" },
	})(fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		header := stdhttp.Header{"Content-Type": {"text/plain"}}
		body := []byte(text)
		switch c.Request.URL.Path {
		case "/range":
			header.Set("Content-Range", "bytes 0-"+strconv.Itoa(len(text)-1)+"/"+strconv.Itoa(len(text)))
		case "/no-transform":
			header.Set("Cache-Control", "public, No-Transform")
		case "/random":
			body = random
		case "/declared":
			// A HEAD response that declares its length without a body.
			header.Set("Content-Length", strconv.Itoa(len(text)))
			body = nil
		}
		_ = c.WriteResponse(fibhttp.Response{Header: header, Body: body})
	}))
	url := mwtest.Serve(t, handler)
	gz := stdhttp.Header{"Accept-Encoding": {"gzip"}}
	mwtest.Run(t, func(t *testing.T, c mwtest.Client) {
		for _, path := range []string{"/skip", "/range", "/no-transform", "/random"} {
			resp, body := c.Do(t, "GET", url+path, gz, "")
			if enc := resp.Header.Get("Content-Encoding"); enc != "" {
				t.Errorf("%s: Content-Encoding %q, want none", path, enc)
			}
			want := text
			if path == "/random" {
				want = string(random)
			}
			if body != want {
				t.Errorf("%s: body of %d bytes, want %d", path, len(body), len(want))
			}
			// Only the incompressible body was eligible, and so varies.
			if vary := resp.Header.Get("Vary"); (path == "/random") != (vary == "Accept-Encoding") {
				t.Errorf("%s: Vary %q", path, vary)
			}
		}
		for _, path := range []string{"/", "/declared"} {
			resp, _ := c.Do(t, "HEAD", url+path, gz, "")
			if enc := resp.Header.Get("Content-Encoding"); enc != "" {
				t.Errorf("HEAD %s: Content-Encoding %q, want none", path, enc)
			}
			if vary := resp.Header.Get("Vary"); vary != "Accept-Encoding" {
				t.Errorf("HEAD %s: Vary %q, want Accept-Encoding", path, vary)
			}
		}
	})
}

// TestCompressLevels checks that each level compresses a body that decodes
// back to the original, and that MinLength set lower compresses a body the
// default would leave alone.
func TestCompressLevels(t *testing.T) {
	for _, level := range []compress.Level{compress.LevelBestSpeed, compress.LevelBestCompression} {
		short := text[:200]
		handler := compress.New(compress.Config{Level: level, MinLength: 100})(fibhttp.HandlerFunc(func(c *fibhttp.Context) {
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(short))
		}))
		url := mwtest.Serve(t, handler)
		c := mwtest.Clients(t)[0]
		for _, encoding := range []string{"gzip", "deflate"} {
			resp, body := c.Do(t, "GET", url, stdhttp.Header{"Accept-Encoding": {encoding}}, "")
			if got := resp.Header.Get("Content-Encoding"); got != encoding {
				t.Fatalf("level %d: Content-Encoding %q, want %q", level, got, encoding)
			}
			if decoded := decode(t, encoding, body); decoded != short {
				t.Errorf("level %d %s: decodes to %d bytes, want %d", level, encoding, len(decoded), len(short))
			}
		}
	}
}
