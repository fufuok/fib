//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// gzipFilter compresses a body as it is written, as a compression
// middleware's filter does.
type gzipFilter struct {
	buf bytes.Buffer
	w   *gzip.Writer
}

func newGzipFilter() *gzipFilter {
	f := &gzipFilter{}
	f.w = gzip.NewWriter(&f.buf)
	return f
}

func (f *gzipFilter) Filter(p []byte) ([]byte, error) {
	f.buf.Reset()
	_, err := f.w.Write(p)
	return f.buf.Bytes(), err
}

func (f *gzipFilter) Flush() ([]byte, error) {
	f.buf.Reset()
	err := f.w.Flush()
	return f.buf.Bytes(), err
}

func (f *gzipFilter) Close() ([]byte, error) {
	f.buf.Reset()
	err := f.w.Close()
	return f.buf.Bytes(), err
}

// upperFilter upper-cases a body, in a buffer of its own it reuses.
type upperFilter struct{ buf []byte }

func (f *upperFilter) Filter(p []byte) ([]byte, error) {
	f.buf = append(f.buf[:0], bytes.ToUpper(p)...)
	return f.buf, nil
}

func (f *upperFilter) Close() ([]byte, error) { return []byte("!"), nil }

// failFilter fails the body.
type failFilter struct{}

var errFilter = errors.New("filter failed")

func (failFilter) Filter([]byte) ([]byte, error) { return nil, errFilter }
func (failFilter) Close() ([]byte, error)        { return nil, nil }

// TestBodyFilter writes a response each way a handler writes one through a
// gzip filter, and checks that what arrives, over HTTP/1.1 and HTTP/2,
// decompresses to the body written, without a Content-Length that would not
// match it.
func TestBodyFilter(t *testing.T) {
	long := strings.Repeat("a long body that compresses well. ", 6000) // ~200KB
	file := filepath.Join(t.TempDir(), "f.txt")
	if err := os.WriteFile(file, []byte(long), 0o644); err != nil {
		t.Fatal(err)
	}
	write := map[string]func(c *Context){
		"respond": func(c *Context) { _ = c.Respond(200, "text/plain", []byte(long)) },
		"small":   func(c *Context) { _, _ = io.WriteString(c, "short") },
		"declared": func(c *Context) {
			c.Header().Set("Content-Length", fmt.Sprint(len(long)))
			_, _ = io.WriteString(c, long)
		},
		"streamed": func(c *Context) {
			for i := 0; i < len(long); i += 7000 {
				_, _ = io.WriteString(c, long[i:min(i+7000, len(long))])
				c.Flush()
			}
		},
		"nowait": func(c *Context) {
			for i := 0; i < len(long); i += 9000 {
				if _, err := c.WriteNoWait([]byte(long[i:min(i+9000, len(long))])); err != nil {
					return
				}
			}
		},
		"readfrom": func(c *Context) {
			f, err := os.Open(file)
			if err != nil {
				return
			}
			defer f.Close()
			_, _ = c.ReadFrom(f)
		},
	}
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		c.OnBodyFilter(func(status int, h stdhttp.Header) BodyFilter {
			h.Set("Content-Encoding", "gzip")
			return newGzipFilter()
		})
		write[strings.TrimPrefix(c.Request.URL.Path, "/")](c)
	})))
	for _, client := range hookClients(t) {
		client.Transport.(*stdhttp.Transport).DisableCompression = true
		for name := range write {
			t.Run(client.proto+"/"+name, func(t *testing.T) {
				resp, err := client.Get("http://" + addr + "/" + name)
				if err != nil {
					t.Fatal(err)
				}
				raw, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil {
					t.Fatal(err)
				}
				if resp.Header.Get("Content-Encoding") != "gzip" {
					t.Fatalf("Content-Encoding %q", resp.Header.Get("Content-Encoding"))
				}
				if resp.ContentLength >= 0 && resp.ContentLength != int64(len(raw)) {
					t.Errorf("Content-Length %d for %d bytes", resp.ContentLength, len(raw))
				}
				zr, err := gzip.NewReader(bytes.NewReader(raw))
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(zr)
				want := long
				if name == "small" {
					want = "short"
				}
				if err != nil || string(body) != want {
					t.Errorf("%v: %d bytes, want %d", err, len(body), len(want))
				}
				if name != "small" && len(raw) >= len(long)/10 {
					t.Errorf("%d bytes on the wire for %d", len(raw), len(long))
				}
			})
		}
	}
}

// TestBodyFilterFlush checks that what a handler flushes reaches the client
// through the filter before the body ends, as server-sent events need.
func TestBodyFilterFlush(t *testing.T) {
	got := make(chan struct{})
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		c.OnBodyFilter(func(int, stdhttp.Header) BodyFilter { return newGzipFilter() })
		_, _ = io.WriteString(c, "event one\n")
		c.Flush()
		select {
		case <-got:
		case <-time.After(5 * time.Second):
		}
		_, _ = io.WriteString(c, "event two\n")
	})))
	for _, client := range hookClients(t) {
		client.Transport.(*stdhttp.Transport).DisableCompression = true
		resp, err := client.Get("http://" + addr + "/")
		if err != nil {
			t.Fatal(err)
		}
		zr, err := gzip.NewReader(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		r := bufio.NewReader(zr)
		start := time.Now()
		line, err := r.ReadString('\n')
		if err != nil || line != "event one\n" || time.Since(start) > 2*time.Second {
			t.Fatalf("%s: %q %v after %v", client.proto, line, err, time.Since(start))
		}
		got <- struct{}{}
		rest, _ := io.ReadAll(r)
		resp.Body.Close()
		if string(rest) != "event two\n" {
			t.Errorf("%s: then %q", client.proto, rest)
		}
	}
}

// TestBodyFilterChainAndSkips checks filters in a chain, a hook that
// returns none, HEAD, a status without a body, and a filter that fails.
func TestBodyFilterChainAndSkips(t *testing.T) {
	failed := make(chan error, 2)
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		path := c.Request.URL.Path
		// Registered first, so run last: on what upper made.
		c.OnBodyFilter(func(_ int, h stdhttp.Header) BodyFilter {
			if path == "/none" {
				return nil
			}
			h.Set("Content-Encoding", "gzip")
			return newGzipFilter()
		})
		c.OnBodyFilter(func(int, stdhttp.Header) BodyFilter {
			if path == "/fail" {
				return failFilter{}
			}
			return &upperFilter{}
		})
		switch path {
		case "/204":
			c.WriteHeader(stdhttp.StatusNoContent)
		case "/whole":
			_ = c.Respond(200, "text/plain", []byte("whole body"))
		case "/fail":
			c.WriteHeader(200)
			_, err := io.WriteString(c, "x")
			failed <- err
		default:
			c.Header().Set("Content-Length", "9")
			_, _ = io.WriteString(c, "some body")
		}
	})))
	for _, client := range hookClients(t) {
		client.Transport.(*stdhttp.Transport).DisableCompression = true
		get := func(method, path string) (*stdhttp.Response, string) {
			req, _ := stdhttp.NewRequest(method, "http://"+addr+path, nil)
			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("%s %s %s: %v", client.proto, method, path, err)
			}
			raw, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.Header.Get("Content-Encoding") == "gzip" && len(raw) > 0 {
				zr, err := gzip.NewReader(bytes.NewReader(raw))
				if err != nil {
					t.Fatalf("%s %s: %v", client.proto, path, err)
				}
				raw, _ = io.ReadAll(zr)
			}
			return resp, string(raw)
		}
		if _, body := get("GET", "/chain"); body != "SOME BODY!" {
			t.Errorf("%s chain: %q", client.proto, body)
		}
		if _, body := get("GET", "/whole"); body != "WHOLE BODY!" {
			t.Errorf("%s whole: %q", client.proto, body)
		}
		if resp, body := get("GET", "/none"); body != "SOME BODY!" || resp.Header.Get("Content-Encoding") != "" {
			t.Errorf("%s none: %q %v", client.proto, body, resp.Header)
		}
		if resp, body := get("HEAD", "/chain"); body != "" || resp.ContentLength != 9 {
			t.Errorf("%s HEAD: %q, Content-Length %d", client.proto, body, resp.ContentLength)
		}
		if resp, _ := get("GET", "/204"); resp.StatusCode != 204 || resp.Header.Get("Content-Encoding") != "" {
			t.Errorf("%s 204: %d %v", client.proto, resp.StatusCode, resp.Header)
		}
		// The body cannot be carried on: the client gets an error or a
		// response cut short, and Write the filter's error.
		req, _ := stdhttp.NewRequest("GET", "http://"+addr+"/fail", nil)
		if resp, err := client.Do(req); err == nil {
			_, _ = io.ReadAll(resp.Body)
			resp.Body.Close()
		}
		select {
		case err := <-failed:
			if !errors.Is(err, errFilter) {
				t.Errorf("%s fail: Write reported %v", client.proto, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s fail: the handler did not write", client.proto)
		}
	}
}
