//go:build linux || darwin || windows

package http3

import (
	"errors"
	"fmt"
	"hash/crc32"
	stdhttp "net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/internal/reqbody"
)

// heldSink is uploadSink with every piece held for a moment before the next
// is taken, which keeps the stream's flow control, and the client's pushed
// body, waiting for room most of the time.
func heldSink(c *fibhttp.Context) {
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
				fmt.Appendf(nil, "%d %d %s", n, h.Sum32(), c.Request.Trailer.Get("X-Crc")))
			return
		}
		c.HoldBody(true)
		time.AfterFunc(200*time.Microsecond, func() { c.HoldBody(false) })
	})
}

// pushBody writes size bytes to body, waiting whenever OnDrain says it is
// full, and closes it, with the X-Crc trailer when trailer is set.
func pushBody(body *fibhttp.BodyWriter, req *stdhttp.Request, size int, trailer bool) (uint32, error) {
	h := crc32.NewIEEE()
	piece := make([]byte, 16<<10)
	for i := range piece {
		piece[i] = byte(i * 7)
	}
	for off := 0; off < size; off += len(piece) {
		p := piece[:min(len(piece), size-off)]
		h.Write(p)
		if _, err := body.Write(p); err != nil {
			return 0, err
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

// A pushed body arrives whole over HTTP/3, declared or with a trailer, to a
// server that takes it as it comes and to one that holds it back, which has
// the client wait for the stream's flow control without a goroutine waiting.
func TestBodyWriterUploads(t *testing.T) {
	client := uploadClient(t)
	for _, server := range []struct {
		name    string
		handler func(*fibhttp.Context)
		size    int
	}{{"fast", uploadSink, int(uploadSize() / 4)}, {"held", heldSink, 4 << 20}} {
		url := startServer(t, Config{StreamRequestBody: true}, server.handler)
		for _, declared := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/declared=%v", server.name, declared), func(t *testing.T) {
				body := fibhttp.NewBodyWriter(64 << 10)
				req := mustRequest(t, stdhttp.MethodPost, url+"/upload", nil)
				req.Body = body
				if declared {
					req.ContentLength = int64(server.size)
				} else {
					req.Trailer = stdhttp.Header{"X-Crc": nil}
				}
				future := client.Go(req)
				crc, err := pushBody(body, req, server.size, !declared)
				if err != nil {
					t.Fatal(err)
				}
				resp, err := future.Wait()
				if err != nil {
					t.Fatal(err)
				}
				want := fmt.Sprintf("%d %d ", server.size, crc)
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

// A producer that fails fails the request, and an answer that comes before the
// body has all gone stops it, which the producer hears from Write.
func TestBodyWriterFailures(t *testing.T) {
	client := uploadClient(t)
	sink := startServer(t, Config{StreamRequestBody: true}, uploadSink)
	early := startServer(t, Config{StreamRequestBody: true}, func(c *fibhttp.Context) {
		_ = c.Respond(stdhttp.StatusRequestEntityTooLarge, "text/plain", []byte("too large"))
	})
	t.Run("producer", func(t *testing.T) {
		body := fibhttp.NewBodyWriter(0)
		req := mustRequest(t, stdhttp.MethodPost, sink+"/upload", nil)
		req.Body = body
		future := client.Go(req)
		_, _ = body.Write([]byte("part"))
		body.CloseWithError(errors.New("producer gave up"))
		if _, err := future.Wait(); err == nil || err.Error() != "producer gave up" {
			t.Fatalf("err %v", err)
		}
	})
	t.Run("early", func(t *testing.T) {
		body := fibhttp.NewBodyWriter(0)
		req := mustRequest(t, stdhttp.MethodPost, early+"/upload", nil)
		req.Body = body
		future := client.Go(req)
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
	t.Run("rejected", func(t *testing.T) {
		body := fibhttp.NewBodyWriter(0)
		req := mustRequest(t, stdhttp.MethodPost, strings.Replace(sink, "https", "http", 1), nil)
		req.Body = body
		if _, err := client.Go(req).Wait(); !errors.Is(err, ErrUnsupportedScheme) {
			t.Fatalf("err %v", err)
		}
		if _, err := body.Write([]byte("x")); !errors.Is(err, reqbody.ErrPushStopped) {
			t.Fatalf("write to a rejected request's body: %v", err)
		}
	})
}

// A pushed body closed with CloseWithTrailer sends the trailer it was given
// in place of the request's Trailer, which only names the field.
func TestBodyWriterCloseWithTrailer(t *testing.T) {
	client := uploadClient(t)
	url := startServer(t, Config{StreamRequestBody: true}, uploadSink)
	body := fibhttp.NewBodyWriter(0)
	req := mustRequest(t, stdhttp.MethodPost, url+"/upload", nil)
	req.Body = body
	req.Trailer = stdhttp.Header{"X-Crc": nil}
	future := client.Go(req)
	payload := []byte(strings.Repeat("trailer ", 8<<10))
	if _, err := body.Write(payload); err != nil {
		t.Fatal(err)
	}
	crc := strconv.FormatUint(uint64(crc32.ChecksumIEEE(payload)), 10)
	if err := body.CloseWithTrailer(stdhttp.Header{"X-Crc": {crc}}); err != nil {
		t.Fatal(err)
	}
	resp, err := future.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := readBody(t, resp), fmt.Sprintf("%d %s %s", len(payload), crc, crc); got != want {
		t.Fatalf("server saw %q, want %q", got, want)
	}
}
