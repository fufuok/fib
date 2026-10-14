//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package pprof_test

import (
	"context"
	stdhttp "net/http"
	"strings"
	"testing"
	"time"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/middleware/mwtest"
	"github.com/lesismal/fib/middleware/pprof"
)

// TestPprofSymbolTraceAndRedirect checks the symbol and trace endpoints, a
// redirect to the index that keeps the query, and a request Next skips.
func TestPprofSymbolTraceAndRedirect(t *testing.T) {
	next := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("app"))
	})
	url := mwtest.Serve(t, pprof.New(pprof.Config{
		Next: func(c *fibhttp.Context) bool { return c.Request.Header.Get("X-Skip") != "" },
	})(next))
	base := url + pprof.DefaultPrefix
	c := mwtest.Clients(t)[0]
	resp, body := c.Do(t, "GET", base+"/symbol", nil, "")
	if resp.StatusCode != stdhttp.StatusOK || !strings.HasPrefix(body, "num_symbols:") {
		t.Errorf("symbol: %d %.80q", resp.StatusCode, body)
	}
	resp, body = c.Do(t, "GET", base+"/trace?seconds=0.05", nil, "")
	if resp.StatusCode != stdhttp.StatusOK || len(body) == 0 {
		t.Errorf("trace: %d, %d bytes", resp.StatusCode, len(body))
	}
	resp, _ = c.Do(t, "GET", base+"?debug=1", nil, "")
	if loc := resp.Header.Get("Location"); resp.StatusCode != stdhttp.StatusMovedPermanently || loc != pprof.DefaultPrefix+"/?debug=1" {
		t.Errorf("redirect: %d to %q", resp.StatusCode, loc)
	}
	if _, body := c.Do(t, "GET", base+"/", stdhttp.Header{"X-Skip": {"1"}}, ""); body != "app" {
		t.Errorf("a skipped request did not reach the handler: %.40q", body)
	}
}

// TestPprofClientGoneStopsProfile checks that a profile whose client goes
// stops being gathered: CPU profiles run one at a time, so a second one is
// refused until the first has stopped, which with the first asked to run for
// a minute it does only because its request was cancelled.
func TestPprofClientGoneStopsProfile(t *testing.T) {
	url := mwtest.Serve(t, pprof.New()(fibhttp.HandlerFunc(func(c *fibhttp.Context) {})))
	profile := url + pprof.DefaultPrefix + "/profile"
	c := mwtest.Clients(t)[0]
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	req, err := stdhttp.NewRequestWithContext(ctx, "GET", profile+"?seconds=60", nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp, err := c.Client.Do(req); err == nil {
		resp.Body.Close()
		t.Fatalf("a minute's profile answered %d at once", resp.StatusCode)
	}
	for deadline := time.Now().Add(20 * time.Second); ; {
		resp, _ := c.Do(t, "GET", profile+"?seconds=1", nil, "")
		if resp.StatusCode == stdhttp.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the cancelled profile kept running: a new one answered %d", resp.StatusCode)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
