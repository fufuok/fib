//go:build linux || darwin || windows

package http

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	stdhttp "net/http"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib/internal/reqbody"
)

// produce writes size bytes of genBlock to body in pieces of piece bytes,
// waiting whenever OnDrain says the body is full, as a producer paced by its
// consumer does, then closes it with the X-Crc trailer when trailer is set. It
// returns the CRC of what it wrote and the first error Write reported.
func produce(body *BodyWriter, req *stdhttp.Request, size, piece int, trailer bool) (uint32, error) {
	h := crc32.NewIEEE()
	for off := 0; off < size; off += piece {
		n := min(piece, size-off)
		p := genBlock[off%len(genBlock):]
		if len(p) < n {
			p = append(append([]byte(nil), p...), genBlock[:n-len(p)]...)
		}
		h.Write(p[:n])
		if _, err := body.Write(p[:n]); err != nil {
			return h.Sum32(), err
		}
		drained := make(chan struct{})
		if body.OnDrain(func() { close(drained) }) {
			<-drained
		}
	}
	if trailer {
		req.Trailer.Set("X-Crc", strconv.FormatUint(uint64(h.Sum32()), 10))
	}
	return h.Sum32(), body.Close()
}

// pushRequest is a POST to url whose body is a BodyWriter.
func pushRequest(t *testing.T, url string, declared int64, trailer bool) (*stdhttp.Request, *BodyWriter) {
	t.Helper()
	body := NewBodyWriter(64 << 10)
	req := mustRequest(t, stdhttp.MethodPost, url, nil)
	req.Body = body
	if declared >= 0 {
		req.ContentLength = declared
	}
	if trailer {
		req.Trailer = stdhttp.Header{"X-Crc": nil}
	}
	return req, body
}

// heldSink is fibUploadSink with every piece held for a moment before the
// next is taken, which keeps the client's side backed up, and its pushed
// body waiting for room, most of the time.
func heldSink(c *Context) {
	h := crc32.NewIEEE()
	var n int64
	c.OnBody(func(data []byte, fin bool, err error) {
		h.Write(data)
		n += int64(len(data))
		if err != nil {
			_ = c.Respond(stdhttp.StatusBadRequest, "text/plain", []byte(err.Error()))
			return
		}
		if fin {
			_ = c.Respond(stdhttp.StatusOK, "text/plain",
				fmt.Appendf(nil, "%d %d %d %s", c.Request.ProtoMajor, n, h.Sum32(), c.Request.Trailer.Get("X-Crc")))
			return
		}
		c.HoldBody(true)
		time.AfterFunc(200*time.Microsecond, func() { c.HoldBody(false) })
	})
}

// A pushed body arrives whole, over HTTP/1 and h2c, declared or chunked with
// a trailer, to a server that takes it as fast as it comes and to one that
// holds it back, which has the client wait for room on the socket, and on
// HTTP/2 for the stream's window, without a goroutine waiting for either.
func TestBodyWriterUploads(t *testing.T) {
	config := DefaultConfig()
	config.StreamRequestBody = true
	config.StreamRequestBodyBuffer = 32 << 10
	fast := "http://" + serve(t, NewHandlerWithConfig(config, HandlerFunc(fibUploadSink)))
	slow := "http://" + serve(t, NewHandlerWithConfig(config, HandlerFunc(heldSink)))
	h1 := newTestClient(t, uploadClientConfig())
	h2config := uploadClientConfig()
	h2config.UnencryptedHTTP2 = true
	h2c := newTestClient(t, h2config)
	for _, server := range []struct {
		name, url string
		size      int
	}{{"fast", fast, int(uploadSize() / 4)}, {"held", slow, 4 << 20}} {
		for _, tc := range []struct {
			name   string
			client *Client
			proto  int
		}{{"http1", h1, 1}, {"h2c", h2c, 2}} {
			for _, declared := range []bool{true, false} {
				t.Run(fmt.Sprintf("%s/%s/declared=%v", server.name, tc.name, declared), func(t *testing.T) {
					length := int64(-1)
					if declared {
						length = int64(server.size)
					}
					req, body := pushRequest(t, server.url, length, !declared)
					future := tc.client.Go(req)
					crc, err := produce(body, req, server.size, 16<<10, !declared)
					if err != nil {
						t.Fatal(err)
					}
					resp, err := future.Wait()
					if err != nil {
						t.Fatal(err)
					}
					want := fmt.Sprintf("%d %d %d ", tc.proto, server.size, crc)
					if !declared {
						want += strconv.FormatUint(uint64(crc), 10)
					}
					if got := readBody(t, resp); got != want {
						t.Fatalf("server saw %q, want %q", got, want)
					}
				})
			}
		}
	}
}

// Uploads whose producers have stopped for a while hold no goroutine each, as
// a body the client reads would: the count stays where it was however many
// are open.
func TestBodyWriterHoldsNoGoroutine(t *testing.T) {
	config := DefaultConfig()
	config.StreamRequestBody = true
	url := "http://" + serve(t, NewHandlerWithConfig(config, HandlerFunc(fibUploadSink)))
	h2config := uploadClientConfig()
	h2config.UnencryptedHTTP2 = true
	for _, tc := range []struct {
		name   string
		config ClientConfig
	}{{"http1", uploadClientConfig()}, {"h2c", h2config}} {
		t.Run(tc.name, func(t *testing.T) {
			client := newTestClient(t, tc.config)
			// Warm up: connections, pools, the server's workers.
			warm, warmBody := pushRequest(t, url, -1, false)
			warmFuture := client.Go(warm)
			_, _ = warmBody.Write([]byte("warm"))
			_ = warmBody.Close()
			if _, err := warmFuture.Wait(); err != nil {
				t.Fatal(err)
			}
			// open starts n uploads that write a piece each and then stop.
			type upload struct {
				body   *BodyWriter
				future *Future
			}
			open := func(n int) []upload {
				ups := make([]upload, n)
				for i := range ups {
					var req *stdhttp.Request
					req, ups[i].body = pushRequest(t, url, -1, false)
					ups[i].future = client.Go(req)
					_, _ = ups[i].body.Write(bytes.Repeat([]byte("p"), 8<<10))
				}
				return ups
			}
			// A first batch grows the client's and the server's pools to what
			// a burst of uploads takes; the second, measured, finds them
			// grown, so what it adds is what open uploads hold themselves.
			const uploads = 40
			first := open(uploads)
			time.Sleep(300 * time.Millisecond)
			before := runtime.NumGoroutine()
			second := open(uploads)
			time.Sleep(300 * time.Millisecond)
			during := runtime.NumGoroutine()
			var stacks bytes.Buffer
			_ = pprof.Lookup("goroutine").WriteTo(&stacks, 2)
			for _, up := range append(first, second...) {
				_ = up.body.Close()
			}
			for _, up := range append(first, second...) {
				resp, err := up.future.Wait()
				if err != nil {
					t.Fatal(err)
				}
				if got := readBody(t, resp); !strings.HasPrefix(got, fmt.Sprintf("%d %d ", map[string]int{"http1": 1, "h2c": 2}[tc.name], 8<<10)) {
					t.Fatalf("server saw %q", got)
				}
			}
			// No goroutine waits in an upload, as one that reads a body does
			// (sendBody, or a Read on the body), and the count does not grow by
			// one an upload. The count is of the whole process, where other
			// tests' goroutines come and go, hence the margin.
			for _, waiting := range []string{".sendBody(", "reqbody.(*Pusher).Read("} {
				if n := strings.Count(stacks.String(), waiting); n > 0 {
					t.Fatalf("%d goroutines wait in %s", n, waiting)
				}
			}
			if grew := during - before; grew >= uploads {
				t.Fatalf("%d more open uploads added %d goroutines (%d -> %d)", uploads, grew, before, during)
			}
		})
	}
}

// A producer that fails, and a body not as long as it declared, fail the
// request; a request that ends before the body has all gone stops it, which
// the producer hears from Write, and its OnDrain callback runs.
func TestBodyWriterFailures(t *testing.T) {
	config := DefaultConfig()
	config.StreamRequestBody = true
	sink := "http://" + serve(t, NewHandlerWithConfig(config, HandlerFunc(fibUploadSink)))
	early := "http://" + serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
		_ = c.Respond(stdhttp.StatusRequestEntityTooLarge, "text/plain", []byte("too large"))
	})))
	h1 := newTestClient(t, uploadClientConfig())
	h2config := uploadClientConfig()
	h2config.UnencryptedHTTP2 = true
	h2c := newTestClient(t, h2config)
	for _, tc := range []struct {
		name   string
		client *Client
	}{{"http1", h1}, {"h2c", h2c}} {
		t.Run(tc.name+"/producer", func(t *testing.T) {
			req, body := pushRequest(t, sink, -1, false)
			future := tc.client.Go(req)
			_, _ = body.Write([]byte("part"))
			body.CloseWithError(errors.New("producer gave up"))
			if _, err := future.Wait(); err == nil || err.Error() != "producer gave up" {
				t.Fatalf("err %v", err)
			}
		})
		for _, length := range []struct {
			name     string
			declared int64
			want     string
		}{{"short", 10, "with Body length 4"}, {"long", 2, "with Body length at least 4"}} {
			t.Run(tc.name+"/"+length.name, func(t *testing.T) {
				req, body := pushRequest(t, sink, length.declared, false)
				future := tc.client.Go(req)
				_, _ = body.Write([]byte("four"))
				_ = body.Close()
				if _, err := future.Wait(); err == nil || !strings.Contains(err.Error(), length.want) {
					t.Fatalf("err %v, want %q", err, length.want)
				}
			})
		}
		t.Run(tc.name+"/early", func(t *testing.T) {
			req, body := pushRequest(t, early, -1, false)
			future := tc.client.Go(req)
			// A chunked body reaches the handler once some of it has.
			_, _ = body.Write(make([]byte, 8<<10))
			resp, err := future.Wait()
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != stdhttp.StatusRequestEntityTooLarge || readBody(t, resp) != "too large" {
				t.Fatalf("status %d", resp.StatusCode)
			}
			deadline := time.Now().Add(5 * time.Second)
			var werr error
			for werr == nil && time.Now().Before(deadline) {
				_, werr = body.Write(make([]byte, 64<<10))
				time.Sleep(time.Millisecond)
			}
			if !errors.Is(werr, reqbody.ErrPushStopped) {
				t.Fatalf("write after the answer: %v", werr)
			}
		})
	}
	t.Run("rejected", func(t *testing.T) {
		req, body := pushRequest(t, "ftp://example.com/", -1, false)
		if _, err := h1.Go(req).Wait(); !errors.Is(err, ErrUnsupportedScheme) {
			t.Fatalf("err %v", err)
		}
		if _, err := body.Write([]byte("x")); !errors.Is(err, reqbody.ErrPushStopped) {
			t.Fatalf("write to a rejected request's body: %v", err)
		}
	})
}

// HoldBody stops what OnBody delivers, holds an HTTP/1 connection's reads once
// the buffer fills, and lets everything through once it is let go, from
// another goroutine; a failure is still reported while the body is held.
func TestHoldBody(t *testing.T) {
	const size = 1 << 20
	config := streamingConfig()
	config.StreamRequestBodyBuffer = 16 << 10
	t.Run("resume", func(t *testing.T) {
		held := make(chan *Context, 1)
		var mu sync.Mutex
		calls, total := 0, 0
		addr := serveStreamingServer(t, config, func(c *Context) {
			c.OnBody(func(data []byte, fin bool, err error) {
				mu.Lock()
				calls++
				total += len(data)
				first := calls == 1
				mu.Unlock()
				if first {
					c.HoldBody(true)
					held <- c
				}
				if fin || err != nil {
					_ = c.Respond(stdhttp.StatusOK, "text/plain", fmt.Appendf(nil, "%d %v", total, err))
				}
			})
		})
		conn := dialRaw(t, addr)
		conn.send(fmt.Sprintf("POST /x HTTP/1.1\r\nHost: test\r\nContent-Length: %d\r\n\r\n", size))
		go func() { _, _ = conn.Write(bytes.Repeat([]byte("h"), size)) }()
		c := <-held
		deadline := time.Now().Add(5 * time.Second)
		for !c.Conn.ReadsHeld() && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if !c.Conn.ReadsHeld() {
			t.Fatal("the connection never held its reads while the body was held")
		}
		mu.Lock()
		before := calls
		mu.Unlock()
		time.Sleep(50 * time.Millisecond)
		mu.Lock()
		after := calls
		mu.Unlock()
		if after != before {
			t.Fatalf("OnBody called %d times while held", after-before)
		}
		c.HoldBody(false)
		if _, body := conn.response(stdhttp.MethodPost); body != fmt.Sprintf("%d <nil>", size) {
			t.Fatalf("body = %q", body)
		}
	})
	t.Run("failure", func(t *testing.T) {
		failed := make(chan error, 1)
		// Handed over at the header, so that the few bytes sent reach the
		// callback before the connection goes.
		immediate := immediateStreamingConfig()
		addr := serveStreamingServer(t, immediate, func(c *Context) {
			c.OnBody(func(data []byte, fin bool, err error) {
				if err != nil {
					failed <- err
					return
				}
				c.HoldBody(true)
			})
		})
		conn := dialRaw(t, addr)
		conn.send(fmt.Sprintf("POST /x HTTP/1.1\r\nHost: test\r\nContent-Length: %d\r\n\r\npartial", size))
		time.Sleep(50 * time.Millisecond)
		conn.Close()
		select {
		case err := <-failed:
			if err == nil {
				t.Fatal("no error")
			}
		case <-time.After(10 * time.Second):
			t.Fatal("a failure was not reported while the body was held")
		}
	})
	t.Run("whole", func(t *testing.T) {
		got := make(chan string, 1)
		addr := serveStreamingServer(t, streamingConfig(), func(c *Context) {
			c.HoldBody(true)
			c.OnBody(func(data []byte, fin bool, err error) {
				got <- string(data)
				_ = c.Respond(stdhttp.StatusOK, "text/plain", nil)
			})
		})
		conn := dialRaw(t, addr)
		conn.send("POST /x HTTP/1.1\r\nHost: test\r\nContent-Length: 5\r\n\r\nsmall")
		if s := <-got; s != "small" {
			t.Fatalf("got %q", s)
		}
		conn.response(stdhttp.MethodPost)
	})
}

// A stream that is gone takes no more of a pushed body, nor its end, and a
// body some of which went on a connection that failed is not sent again: its
// request fails instead.
func TestBodyWriterSinkEdges(t *testing.T) {
	sink := &h2Sink{hc: &h2ClientConn{}, st: &h2ClientStream{id: 1}}
	if err := sink.Send([]byte("x")); !errors.Is(err, errStreamGone) {
		t.Fatalf("send on a gone stream: %v", err)
	}
	if err := sink.End(); !errors.Is(err, errStreamGone) {
		t.Fatalf("end on a gone stream: %v", err)
	}

	push := NewBodyWriter(0)
	sent := &h1Sink{}
	_ = push.Attach(roomySink{sent}, -1)
	_, _ = push.Write([]byte("gone"))
	failed := make(chan error, 1)
	req := mustRequest(t, stdhttp.MethodPost, "http://example.com/", nil)
	r := &clientRequest{req: req, callback: func(_ *ClientResponse, err error) { failed <- err }}
	attachPush(r, push, roomySink{sent})
	if err := <-failed; !errors.Is(err, reqbody.ErrNotReplayable) {
		t.Fatalf("second attempt: %v", err)
	}
}

// roomySink takes everything and drops it.
type roomySink struct{ *h1Sink }

func (roomySink) Room() bool        { return true }
func (roomySink) Send([]byte) error { return nil }
func (roomySink) End() error        { return nil }
func (roomySink) Fail(error)        {}
