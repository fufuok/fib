//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"errors"
	"fmt"
	stdhttp "net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3/quic"
)

// http.ResponseController reaches a request stream's deadlines: lifting
// either succeeds, and a handler that lifts its write deadline streams for as
// long as it likes under WriteTimeout. The timeout runs from the request's
// arrival, so it leaves a loaded machine a second to start the handler, which
// then streams for longer than that.
func TestSetWriteDeadlineLiftsWriteTimeout(t *testing.T) {
	errs := make(chan error, 1)
	base := startServer(t, Config{WriteTimeout: time.Second}, func(c *fibhttp.Context) {
		rc := stdhttp.NewResponseController(c)
		if err := errors.Join(rc.SetWriteDeadline(time.Time{}), rc.SetReadDeadline(time.Time{}),
			rc.SetReadDeadline(time.Now().Add(time.Minute))); err != nil {
			errs <- err
			return
		}
		for i := 0; i < 4; i++ {
			if i > 0 {
				time.Sleep(400 * time.Millisecond)
			}
			_, _ = c.WriteString("tick\n")
			if err := rc.Flush(); err != nil {
				errs <- err
				return
			}
		}
		errs <- nil
	})
	client := newClient(t, nil)
	resp, err := client.Go(mustRequest(t, stdhttp.MethodGet, base+"/lifted", nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if body := readBody(t, resp); body != strings.Repeat("tick\n", 4) {
		t.Fatalf("body = %q", body)
	}
	select {
	case err := <-errs:
		if err != nil {
			t.Fatalf("the handler failed: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the handler never finished")
	}
}

// A handler can bound its response without WriteTimeout: once the deadline
// has passed, a stream whose client stopped taking it is reset and the
// handler waiting for room is let go.
func TestSetWriteDeadlineResetsStalledStream(t *testing.T) {
	errs := make(chan error, 1)
	base := startServer(t, Config{}, func(c *fibhttp.Context) {
		if err := c.SetWriteDeadline(time.Now().Add(300 * time.Millisecond)); err != nil {
			errs <- err
			return
		}
		chunk := make([]byte, 16<<10)
		for {
			if _, err := c.Write(chunk); err != nil {
				errs <- err
				return
			}
		}
	})
	w := dialH3WireWith(t, nil, base, quic.Config{StreamReceiveWindow: 16 << 10})
	s, err := w.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	s.HoldCredit()
	head, _ := h3Request{path: "/stall"}.frames()
	if err := s.Write(head, true); err != nil {
		t.Fatal(err)
	}
	st := w.await(s.ID(), "reset", func(st *h3WireStream) bool { return st.done })
	w.mu.Lock()
	reset := st.reset
	w.mu.Unlock()
	if !reset {
		t.Fatal("the stalled stream ended without a reset")
	}
	select {
	case err := <-errs:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("the waiting Write reported %v, want %v", err, os.ErrDeadlineExceeded)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the handler still waits past its deadline")
	}
}

// A read deadline on HTTP/3 bounds the stream's body, not the connection:
// once it has passed, a body still arriving fails with
// os.ErrDeadlineExceeded, the handler still answers, and the connection goes
// on serving. Lifting a deadline before it passes leaves the body unbounded,
// and a body that has all arrived has nothing to bound.
func TestSetReadDeadline(t *testing.T) {
	base := startServer(t, Config{StreamRequestBody: true}, func(c *fibhttp.Context) {
		var err error
		switch c.Request.URL.Path {
		case "/whole":
			zero := c.SetReadDeadline(time.Time{})
			later := c.SetReadDeadline(time.Now().Add(time.Minute))
			_ = c.Respond(stdhttp.StatusOK, "text/plain", fmt.Appendf(nil, "zero=%v later=%v", zero, later))
			return
		case "/bounded":
			err = c.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
		case "/lifted":
			err = errors.Join(c.SetReadDeadline(time.Now().Add(200*time.Millisecond)), c.SetReadDeadline(time.Time{}))
		}
		if err != nil {
			_ = c.Respond(stdhttp.StatusInternalServerError, "text/plain", []byte(err.Error()))
			return
		}
		total := 0
		c.OnBody(func(data []byte, fin bool, err error) {
			total += len(data)
			switch {
			case err != nil:
				_ = c.Respond(stdhttp.StatusRequestTimeout, "text/plain", []byte(err.Error()))
			case fin:
				_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(strconv.Itoa(total)))
			}
		})
	})
	w := dialH3Wire(t, nil, base)
	// post opens a request whose header declares 100 bytes of body, of
	// which it sends 10.
	post := func(path string) *quic.Stream {
		t.Helper()
		s, err := w.qc.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		head, _ := h3Request{path: path, framing: h3Length, body: make([]byte, 100)}.frames()
		head = appendFrameHeader(head, frameData, 10)
		if err := s.Write(append(head, make([]byte, 10)...), false); err != nil {
			t.Fatal(err)
		}
		return s
	}
	whole := func() {
		t.Helper()
		if status, body := w.response(w.send(t, h3Request{path: "/whole"}, false)); status != 200 ||
			string(body) != "zero=<nil> later=<nil>" {
			t.Fatalf("a body that has arrived: %d %q", status, body)
		}
	}

	whole()
	start := time.Now()
	s := post("/bounded")
	if status, body := w.response(s.ID()); status != 408 || string(body) != os.ErrDeadlineExceeded.Error() {
		t.Fatalf("a body past its deadline: %d %q", status, body)
	}
	if waited := time.Since(start); waited < 250*time.Millisecond {
		t.Fatalf("the body failed after %v, before its deadline", waited)
	}

	s = post("/lifted")
	time.Sleep(500 * time.Millisecond)
	if err := s.Write(append(appendFrameHeader(nil, frameData, 90), make([]byte, 90)...), true); err != nil {
		t.Fatal(err)
	}
	if status, body := w.response(s.ID()); status != 200 || string(body) != "100" {
		t.Fatalf("a body whose deadline was lifted: %d %q", status, body)
	}
	whole()
}

// A Flush made once WriteTimeout has reset the stream reports it, even with
// nothing written in between, and so does every Write and Flush after it.
func TestFlushReportsAResetStream(t *testing.T) {
	results := make(chan [3]error, 1)
	base := startServer(t, Config{WriteTimeout: 200 * time.Millisecond}, func(c *fibhttp.Context) {
		_, _ = c.WriteString("first")
		if err := c.FlushError(); err != nil {
			// A handler that started late finds the stream reset already;
			// what follows has to report it all the same.
			_, write := c.WriteString(strings.Repeat("x", 32<<10))
			results <- [3]error{err, write, c.FlushError()}
			return
		}
		time.Sleep(500 * time.Millisecond)
		flush := c.FlushError()
		_, write := c.WriteString(strings.Repeat("x", 32<<10))
		results <- [3]error{flush, write, c.FlushError()}
	})
	w := dialH3Wire(t, nil, base)
	id := w.send(t, h3Request{path: "/events"}, false)
	w.await(id, "reset", func(st *h3WireStream) bool { return st.done })
	var errs [3]error
	select {
	case errs = <-results:
	case <-time.After(10 * time.Second):
		t.Fatal("the handler never finished")
	}
	for i, err := range errs {
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("call %d after the reset reported %v, want %v", i, err, os.ErrDeadlineExceeded)
		}
	}
}
