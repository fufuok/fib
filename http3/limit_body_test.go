//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"errors"
	"fmt"
	stdhttp "net/http"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	fibhttp "github.com/lesismal/fib/http"
)

// limitBodyN is the limit limitBodyHandler sets, well below the server's.
const limitBodyN = 1000

// limitBodyRefused counts the requests LimitBody refused, and limitBodyLet
// is told of each it let through.
var (
	limitBodyRefused atomic.Int64
	limitBodyLet     = make(chan struct{}, 64)
)

// limitBodyHandler is package http's: it bounds the body to limitBodyN and
// takes it through OnBody, answering with its length, and leaves a body ended
// for its size unanswered, for the server to answer.
func limitBodyHandler(c *fibhttp.Context) {
	if err := c.LimitBody(limitBodyN); err != nil {
		if !errors.Is(err, fibhttp.ErrBodyTooLarge) {
			_ = c.Respond(stdhttp.StatusInternalServerError, "text/plain", []byte(err.Error()))
			return
		}
		limitBodyRefused.Add(1)
		if c.Respond(stdhttp.StatusOK, "text/plain", nil) == nil {
			h3Violation("%s: answered again after LimitBody refused the body", c.Request.URL.Path)
		}
		return
	}
	select {
	case limitBodyLet <- struct{}{}:
	default:
	}
	n := 0
	c.OnBody(func(data []byte, fin bool, err error) {
		n += len(data)
		switch {
		case errors.Is(err, fibhttp.ErrBodyTooLarge):
			if n > limitBodyN {
				h3Violation("%s: %d bytes handed over past a limit of %d", c.Request.URL.Path, n, limitBodyN)
			}
		case err != nil:
			_ = c.Respond(stdhttp.StatusInternalServerError, "text/plain", []byte(err.Error()))
		case fin:
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(strconv.Itoa(n)))
		}
	})
}

// TestLimitBody is package http's TestLimitBody over HTTP/3: a body within
// the limit is served, one that declares a length past it or is here past it
// when the handler runs is refused at once with 413, and one of no declared
// length that grows past it is ended, with the server answering 413; such a
// body sent to a server that streams it from the start goes in two parts,
// the second once LimitBody has let the first through.
func TestLimitBody(t *testing.T) {
	configs := []h3Config{{"buffered", false, 0}, {"stream", true, 0}, {"stream-2k", true, 2 << 10}}
	for _, m := range configs {
		base := startServer(t, m.config(), limitBodyHandler)
		for _, framing := range []h3Framing{h3Length, h3Unsized} {
			for _, size := range []int{limitBodyN, limitBodyN + 1, 4 << 10} {
				t.Run(fmt.Sprintf("%s/%s/%d", m.name, framing, size), func(t *testing.T) {
					w := dialH3Wire(t, nil, base)
					refusedBefore := limitBodyRefused.Load()
					for len(limitBodyLet) > 0 {
						<-limitBodyLet
					}
					req := newH3Request("limit", framing, size, 0)
					late := m.stream && m.threshold == 0 && framing == h3Unsized
					var id uint64
					if late {
						s, err := w.qc.OpenStream()
						if err != nil {
							t.Fatal(err)
						}
						id = s.ID()
						head, _ := req.frames()
						data := func(b []byte) []byte { return append(appendFrameHeader(nil, frameData, len(b)), b...) }
						if err := s.Write(append(head, data(req.body[:500])...), false); err != nil {
							t.Fatal(err)
						}
						select {
						case <-limitBodyLet:
						case <-time.After(5 * time.Second):
							t.Fatal("the handler never let the body through")
						}
						if err := s.Write(data(req.body[500:]), true); err != nil {
							t.Fatal(err)
						}
					} else {
						id = w.send(t, req, false)
					}
					status, got := w.response(id)
					refused := limitBodyRefused.Load() != refusedBefore
					if size <= limitBodyN {
						if status != stdhttp.StatusOK || string(got) != strconv.Itoa(size) {
							t.Fatalf("status %d %q, want 200 %q", status, got, strconv.Itoa(size))
						}
						return
					}
					if status != stdhttp.StatusRequestEntityTooLarge {
						t.Fatalf("status %d %q, want 413", status, got)
					}
					mustRefuse := framing == h3Length || !m.stream || m.threshold >= limitBodyN
					if mustRefuse && !refused {
						t.Fatal("LimitBody let a body through that it could have refused")
					}
					if late && refused {
						t.Fatal("LimitBody refused a body still within the limit")
					}
				})
			}
		}
	}
	checkH3Violations(t)
}
