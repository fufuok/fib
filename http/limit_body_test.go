//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"errors"
	"fmt"
	stdhttp "net/http"
	"strconv"
	"testing"
	"time"

	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
)

// limitBodyN is the limit limitBodyHandler sets, well below the server's.
const limitBodyN = 1000

// limitBodyOutcome is told, for each request, whether LimitBody refused
// it: once LimitBody has returned, which for a refusal is after the 413 has
// gone, so the test waits for it rather than read it as the response
// arrives.
var limitBodyOutcome = make(chan bool, 64)

// limitBodyHandler bounds the body to limitBodyN and takes it through OnBody,
// answering with its length. A body ended for its size is left unanswered,
// for the server to answer.
func limitBodyHandler(c *Context) {
	if err := c.LimitBody(limitBodyN); err != nil {
		if !errors.Is(err, ErrBodyTooLarge) {
			_ = c.Respond(stdhttp.StatusInternalServerError, "text/plain", []byte(err.Error()))
			return
		}
		limitBodyOutcome <- true
		// Answered already: a response of the handler's own fails.
		if c.Respond(stdhttp.StatusOK, "text/plain", nil) == nil {
			matrixViolation("%s: answered again after LimitBody refused the body", c.Request.URL.Path)
		}
		return
	}
	limitBodyOutcome <- false
	n := 0
	c.OnBody(func(data []byte, fin bool, err error) {
		n += len(data)
		switch {
		case errors.Is(err, ErrBodyTooLarge):
			if n > limitBodyN {
				matrixViolation("%s: %d bytes handed over past a limit of %d", c.Request.URL.Path, n, limitBodyN)
			}
		case err != nil:
			_ = c.Respond(stdhttp.StatusInternalServerError, "text/plain", []byte(err.Error()))
		case fin:
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(strconv.Itoa(n)))
		}
	})
}

// TestLimitBody bounds a body below the server's limits on every protocol
// but HTTP/3 (see package http3's TestLimitBody), for a server that reads
// bodies whole, one that streams them, and one that streams them past a
// threshold the limit is below: a body within the limit is served; one that
// declares a length past it, or that arrived whole or in part past it before
// the handler ran, is refused at once with 413; and one of no declared length
// that grows past it is ended with ErrBodyTooLarge, which leaves the handler
// with nothing to answer and the server answering 413. A body of no declared
// length sent to a server that streams it from the start is sent in two
// parts, the second once LimitBody has let the first through, so that it is
// ended past the limit rather than refused.
func TestLimitBody(t *testing.T) {
	configs := []matrixConfig{{"buffered", false, 0}, {"stream", true, 0}, {"stream-2k", true, 2 << 10}}
	serverTLS, _, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	servers := matrixServers{}
	for _, m := range configs {
		plain := serve(t, NewHandlerWithConfig(m.config(), HandlerFunc(limitBodyHandler)))
		secure := serve(t, fibtls.NewServer(ConfigureTLS(serverTLS), NewHandlerWithConfig(m.config(), HandlerFunc(limitBodyHandler))))
		servers[m.name] = [2]string{plain, secure}
	}
	for _, proto := range matrixProtos {
		for _, m := range configs {
			for _, framing := range []matrixFraming{mframeLength, mframeChunked} {
				for _, size := range []int{limitBodyN, limitBodyN + 1, 4 << 10} {
					name := fmt.Sprintf("%s/%s/%s/%d", proto.name, m.name, framing, size)
					t.Run(name, func(t *testing.T) {
						w := dialMatrix(t, servers.addr(m.name, proto.secure), proto)
						defer w.close()
						req := newMatrixRequest("limit", framing, size, 0)
						var head, body []byte
						if proto.h2 {
							head, body = req.h2(w.(*h2Wire).tc, matrixStreamID(0), proto.secure)
						} else {
							head, body = req.h1()
						}
						late := m.stream && m.threshold == 0 && framing == mframeChunked
						for len(limitBodyOutcome) > 0 {
							<-limitBodyOutcome
						}
						var refused bool
						if late {
							first, rest := limitBodyParts(req.body[:500], req.body[500:], proto.h2)
							w.write(append(head, first...))
							select {
							case refused = <-limitBodyOutcome:
							case <-time.After(5 * time.Second):
								t.Fatal("the handler never let the body through")
							}
							w.write(rest)
						} else {
							w.write(append(head, body...))
						}
						status, got := w.response(0)
						if !late {
							select {
							case refused = <-limitBodyOutcome:
							case <-time.After(5 * time.Second):
								t.Fatal("the handler never ran")
							}
						}
						if size <= limitBodyN {
							if status != stdhttp.StatusOK || string(got) != strconv.Itoa(size) {
								t.Fatalf("status %d %q, want 200 %q", status, got, strconv.Itoa(size))
							}
							return
						}
						if status != stdhttp.StatusRequestEntityTooLarge {
							t.Fatalf("status %d %q, want 413", status, got)
						}
						// A body that declares its length, or that is all or
						// partly here past the limit when the handler runs, is
						// refused by LimitBody. One of no declared length that
						// streams from the start may be let through, if the
						// handler runs before the limit's worth of it has
						// arrived, and ended later.
						mustRefuse := framing == mframeLength || !m.stream || m.threshold >= limitBodyN
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
	}
	checkMatrixViolations(t)
}

// limitBodyParts frames the body of a request of no declared length in two
// parts, chunks on HTTP/1 and DATA frames on stream 1 on HTTP/2, the second
// ending the body.
func limitBodyParts(first, rest []byte, h2 bool) (a, b []byte) {
	if h2 {
		id := matrixStreamID(0)
		a = append(h2AppendFrameHeader(nil, h2FrameData, 0, id, len(first)), first...)
		b = append(h2AppendFrameHeader(nil, h2FrameData, h2FlagEndStream, id, len(rest)), rest...)
		return a, b
	}
	a = append(fmt.Appendf(nil, "%x\r\n", len(first)), first...)
	a = append(a, "\r\n"...)
	b = append(fmt.Appendf(nil, "%x\r\n", len(rest)), rest...)
	b = append(b, "\r\n0\r\n\r\n"...)
	return a, b
}

// TestLimitBodyLowerOfTheTwo checks that a limit above the server's own does
// not lift it: the lower of the two applies.
func TestLimitBodyLowerOfTheTwo(t *testing.T) {
	config := DefaultConfig()
	config.StreamRequestBody = true
	config.MaxStreamedBodyBytes = 2000
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
		if err := c.LimitBody(1 << 20); err != nil {
			_ = c.Respond(stdhttp.StatusInternalServerError, "text/plain", []byte(err.Error()))
			return
		}
		n := 0
		c.OnBody(func(data []byte, fin bool, err error) {
			n += len(data)
			if err == nil && fin {
				_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(strconv.Itoa(n)))
			}
		})
	})))
	for _, tt := range []struct{ size, status int }{{2000, 200}, {3000, 413}} {
		w := dialMatrix(t, addr, matrixProtos[0])
		head, body := newMatrixRequest("lower", mframeChunked, tt.size, 0).h1()
		w.write(append(head, body...))
		if status, got := w.response(0); status != tt.status {
			t.Fatalf("%d bytes: status %d %q, want %d", tt.size, status, got, tt.status)
		}
		w.close()
	}
}

// TestLimitBodyToNothing bounds a body to no bytes at all, which a limit of
// zero in the server's configuration would leave unbounded.
func TestLimitBodyToNothing(t *testing.T) {
	config := DefaultConfig()
	config.StreamRequestBody = true
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
		if c.LimitBody(0) != nil {
			return
		}
		c.OnBody(func(data []byte, fin bool, err error) {
			if err == nil && fin {
				_ = c.Respond(stdhttp.StatusOK, "text/plain", nil)
			}
		})
	})))
	for _, tt := range []struct {
		framing matrixFraming
		size    int
		status  int
	}{{mframeNone, 0, 200}, {mframeLength, 0, 200}, {mframeLength, 1, 413}, {mframeChunked, 1, 413}} {
		w := dialMatrix(t, addr, matrixProtos[0])
		head, body := newMatrixRequest("nothing", tt.framing, tt.size, 0).h1()
		w.write(append(head, body...))
		if status, got := w.response(0); status != tt.status {
			t.Fatalf("%s of %d bytes: status %d %q, want %d", tt.framing, tt.size, status, got, tt.status)
		}
		w.close()
	}
}
