//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"fmt"
	"io"
	stdhttp "net/http"
	"strings"
	"testing"
	"time"

	fibhttp "github.com/lesismal/fib/http"
	fibtls "github.com/lesismal/fib/tls"
)

// earlyHandler says whether the request arrived as early data.
func earlyHandler(c *fibhttp.Context) {
	if c.Request.URL.Path == "/toearly" && c.Request.Header.Get("Early-Data") == "1" {
		// An application that would rather not answer early data.
		_ = c.Respond(425, "text/plain", []byte("too early"))
		return
	}
	_ = c.Respond(200, "text/plain", []byte(fmt.Sprintf("%s early=%q", c.Request.Method, c.Request.Header.Get("Early-Data"))))
}

// settle starts a new connection for the peer's next request, once the
// ticket the server sends when the handshake is over has arrived: a response
// to early data can come before that, and a client that goes before it has
// only the ticket it has used, whose early data the server turns away, since
// a ticket carries early data once.
func settle(t *testing.T, p *peer) {
	t.Helper()
	if r := p.do(t, interopSpec{Command: "waitticket"}); r.Error != "" {
		t.Fatalf("waiting for the session ticket: %s", r.Error)
	}
	p.do(t, interopSpec{Command: "newconn"})
}

// TestEarlyDataQUICGo resumes a session with quic-go's client, which sends
// its GET in the first flight when the ticket allows it.
func TestEarlyDataQUICGo(t *testing.T) {
	certFile, _ := writePEM(t)
	for _, tc := range []struct {
		name      string
		config    Config
		wantEarly string
	}{
		{"accepted", Config{EarlyData: true}, "1"},
		{"not enabled", Config{}, ""},
		{"blocking handshake", Config{EarlyData: true, BlockingTLSHandshake: true}, "1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			url := startServer(t, tc.config, earlyHandler)
			p := startPeer(t, "-mode", "client", "-url", url, "-ca", certFile, "-early")

			first := p.ok(t, interopSpec{Path: "/first"})
			if first.Resumed || first.Body != `GET early=""` {
				t.Fatalf("first connection: resumed %v, %q", first.Resumed, first.Body)
			}
			// Whether quic-go puts the GET in its first flight depends on its
			// own timing: one that has finished its handshake by the time it
			// writes the request, as a loaded machine may, sends it under the
			// 1-RTT keys, which the peer reports. That is tried again, up to
			// three connections; a request that did go out early and was not
			// taken as early is the server's failing, and fails at once.
			want := fmt.Sprintf("GET early=%q", tc.wantEarly)
			for attempt := 1; ; attempt++ {
				settle(t, p)
				got := p.ok(t, interopSpec{Method: "GET_0RTT", Path: "/second"})
				if !got.Resumed {
					t.Fatal("the second connection did not resume the session")
				}
				if early := got.Body == `GET early="1"`; early && !got.Sent0RTT {
					t.Fatalf("the server took the request as early data, but quic-go sent it under the 1-RTT keys")
				}
				if got.Body == want {
					break
				}
				if tc.wantEarly == "" || got.Sent0RTT || attempt == 3 {
					t.Fatalf("the server saw %q, want %q (quic-go sent the request in 0-RTT: %v)", got.Body, want, got.Sent0RTT)
				}
				t.Logf("attempt %d: quic-go sent the request under the 1-RTT keys", attempt)
			}

			// And again: every connection gets a ticket of its own.
			settle(t, p)
			got := p.ok(t, interopSpec{Method: "HEAD_0RTT", Path: "/third"})
			if !got.Resumed {
				t.Fatal("the third connection did not resume")
			}

			// A request that is not early goes the usual way.
			settle(t, p)
			got = p.ok(t, interopSpec{Method: "POST", Path: "/fourth", Body: "x"})
			if !got.Resumed || got.Body != `POST early=""` {
				t.Fatalf("POST: resumed %v, %q", got.Resumed, got.Body)
			}
		})
	}
}

// earlyClients are the clients of a test that resumes sessions: each is a new
// client, so a new connection, and all share the sessions.
type earlyClients struct {
	t     *testing.T
	cache fibtls.QUICSessionCache
}

func newEarlyClients(t *testing.T) *earlyClients {
	return &earlyClients{t: t, cache: fibtls.NewQUICSessionCache(4)}
}

func (e *earlyClients) new() *Client {
	return newClient(e.t, func(c *ClientConfig) {
		c.EarlyData = true
		c.SessionCache = e.cache
	})
}

// waitSession waits for the ticket of the connection before.
func (e *earlyClients) waitSession() {
	e.t.Helper()
	for deadline := time.Now().Add(5 * time.Second); e.cache.Get("localhost") == nil; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			e.t.Fatal("the server sent no ticket")
		}
	}
}

// do sends a request and returns the status and body of the answer.
func do(t *testing.T, client *Client, method, url string, body string) (*stdhttp.Response, string) {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	resp, err := client.Go(mustRequest(t, method, url, reader)).Wait()
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	data, _ := io.ReadAll(resp.Body)
	return resp, string(data)
}

// earlyAccepted says whether the client's connection had its 0-RTT data taken.
func earlyAccepted(client *Client) bool {
	client.mu.Lock()
	defer client.mu.Unlock()
	for _, cc := range client.conns {
		cc.mu.Lock()
		qc := cc.qc
		cc.mu.Unlock()
		if qc != nil && qc.EarlyDataAccepted() {
			return true
		}
	}
	return false
}

func TestEarlyDataClient(t *testing.T) {
	url := startServer(t, Config{EarlyData: true}, earlyHandler)
	e := newEarlyClients(t)

	first := e.new()
	if _, body := do(t, first, "GET", url+"/first", ""); body != `GET early=""` {
		t.Fatalf("first: %q", body)
	}
	e.waitSession()

	second := e.new()
	resp, body := do(t, second, "GET", url+"/second", "")
	if body != `GET early="1"` {
		t.Fatalf("a GET on a resumed session: %q", body)
	}
	if !earlyAccepted(second) {
		t.Fatal("the connection did not send early data")
	}
	if st := second.Stats(); st.Resumed != 1 || st.EarlySent != 1 || st.EarlyAccepted != 1 || st.EarlyRejected != 0 {
		t.Fatalf("stats %+v", st)
	}
	if resp.TLS == nil || !resp.TLS.DidResume {
		t.Fatalf("the response says the session was not resumed: %+v", resp.TLS)
	}
	// And the same connection goes on to carry requests of any method.
	if _, body := do(t, second, "POST", url+"/post", "data"); body != `POST early=""` {
		t.Fatalf("POST: %q", body)
	}
	e.waitSession()

	third := e.new()
	if _, body := do(t, third, "POST", url+"/post", "data"); body != `POST early=""` {
		t.Fatalf("a POST on a resumed session: %q", body)
	}
	e.waitSession()

	fourth := e.new()
	if _, body := do(t, fourth, "HEAD", url+"/head", ""); body != "" {
		t.Fatalf("HEAD: %q", body)
	}
	e.waitSession()

	// A server that answers early data 425 gets the request again.
	fifth := e.new()
	resp, body = do(t, fifth, "GET", url+"/toearly", "")
	if resp.StatusCode != 200 || body != `GET early=""` {
		t.Fatalf("after a 425: %d %q", resp.StatusCode, body)
	}
	if st := fifth.Stats(); st.EarlySent != 1 || st.TooEarly != 1 {
		t.Fatalf("stats after a 425: %+v", st)
	}
}

func TestEarlyDataClientNotAccepted(t *testing.T) {
	url := startServer(t, Config{}, earlyHandler)
	e := newEarlyClients(t)
	if _, body := do(t, e.new(), "GET", url+"/first", ""); body != `GET early=""` {
		t.Fatalf("first: %q", body)
	}
	e.waitSession()
	second := e.new()
	resp, body := do(t, second, "GET", url+"/second", "")
	if st := second.Stats(); st.EarlySent != 0 || st.Resumed != 1 {
		t.Fatalf("stats %+v", st)
	}
	if body != `GET early=""` || earlyAccepted(second) {
		t.Fatalf("%q, accepted %v", body, earlyAccepted(second))
	}
	if resp.TLS == nil || !resp.TLS.DidResume {
		t.Fatal("not resumed")
	}
}

// TestEarlyDataClientQUICGo sends early data to quic-go's server.
func TestEarlyDataClientQUICGo(t *testing.T) {
	_, url := startPeerServer(t, "-early-server")
	e := newEarlyClients(t)
	resp, _ := do(t, e.new(), "GET", url+"/first", "")
	if got := resp.Header.Get("X-Used-0RTT"); got != "false" {
		t.Fatalf("first connection: X-Used-0RTT %q", got)
	}
	e.waitSession()
	second := e.new()
	resp, _ = do(t, second, "GET", url+"/second", "")
	if got := resp.Header.Get("X-Used-0RTT"); got != "true" {
		t.Fatalf("X-Used-0RTT %q: quic-go did not take the early data", got)
	}
	if !earlyAccepted(second) {
		t.Fatal("not accepted")
	}
}

// A quic-go server that does not take early data gets the request after the
// handshake, and the session still resumes.
func TestEarlyDataClientQUICGoNotAccepted(t *testing.T) {
	_, url := startPeerServer(t)
	e := newEarlyClients(t)
	do(t, e.new(), "GET", url+"/first", "")
	e.waitSession()
	second := e.new()
	resp, _ := do(t, second, "GET", url+"/second", "")
	if got := resp.Header.Get("X-Used-0RTT"); got != "false" || earlyAccepted(second) {
		t.Fatalf("X-Used-0RTT %q, accepted %v", got, earlyAccepted(second))
	}
	if resp.TLS == nil || !resp.TLS.DidResume {
		t.Fatal("the session did not resume")
	}
}
