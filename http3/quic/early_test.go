package quic

import (
	"bytes"
	"crypto/rand"
	"crypto/tls"
	"sync"
	"testing"
	"time"

	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
)

// earlyEnv is what two connections of a test share for the second to resume
// the first: the server's ticket keys, and the client's sessions.
type earlyEnv struct {
	server   *fibtls.QUICServer
	sessions fibtls.QUICSessionCache
	// serverEarly and clientEarly say whether the sides use early data.
	serverEarly, clientEarly bool
}

func newEarlyEnv() *earlyEnv {
	return &earlyEnv{sessions: fibtls.NewQUICSessionCache(4), serverEarly: true, clientEarly: true}
}

// configure is the configure of newTestPair for a pair that handshakes with
// this package's state machines, sharing what makes resumption work.
func (e *earlyEnv) configure(client, server *Config) {
	client.EarlyData, server.EarlyData = e.clientEarly, e.serverEarly
	if e.server == nil {
		e.server = fibtls.NewQUICServer(server.TLSConfig)
	}
	client.NewTLS = func(_ bool, config *tls.Config) TLSConn {
		if c := fibtls.NewQUICClientWithSessions(config, e.sessions); c != nil {
			return c
		}
		return nil
	}
	server.NewTLS = func(bool, *tls.Config) TLSConn { return e.server.NewConn() }
}

// first runs a connection to get a session, and waits for the ticket.
func (e *earlyEnv) first(t *testing.T) {
	t.Helper()
	newTestPair(t, 0, e.configure)
	for deadline := time.Now().Add(5 * time.Second); e.sessions.Get("localhost") == nil; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the server sent no ticket")
		}
	}
}

// earlyServer is a server handler that takes early data as it comes, and
// records what it heard of it.
type earlyServer struct {
	*testHandler
	echo  bool
	mu    sync.Mutex
	order []string
	early map[uint64]bool
}

func newEarlyServer(h *testHandler, echo bool) *earlyServer {
	return &earlyServer{testHandler: h, echo: echo, early: map[uint64]bool{}}
}

func (h *earlyServer) note(what string) {
	h.mu.Lock()
	if len(h.order) == 0 || h.order[len(h.order)-1] != what {
		h.order = append(h.order, what)
	}
	h.mu.Unlock()
}

func (h *earlyServer) OnEarlyData(*Conn) { h.note("early") }

func (h *earlyServer) OnHandshake(c *Conn) {
	h.note("handshake")
	h.testHandler.OnHandshake(c)
}

func (h *earlyServer) OnStreamData(s *Stream, data []byte, fin bool) {
	h.note("data")
	h.mu.Lock()
	h.early[s.ID()] = h.early[s.ID()] || s.EarlyData()
	h.mu.Unlock()
	h.testHandler.OnStreamData(s, data, fin)
	if h.echo {
		_ = s.Write(data, fin)
	}
}

func (h *earlyServer) sawEarly(id uint64) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.early[id]
}

func (h *earlyServer) calls() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.order...)
}

// earlyRequest is what a test's client writes as soon as it is dialed.
type earlyRequest struct {
	body []byte
	id   uint64
	// early is what CanSendEarly said.
	early bool
}

func (r *earlyRequest) send(t *testing.T) func(*Conn) {
	return func(c *Conn) {
		r.early = c.CanSendEarly()
		if !r.early {
			return
		}
		s, err := c.OpenStream()
		if err != nil {
			t.Errorf("OpenStream: %v", err)
			return
		}
		r.id = s.ID()
		if err := s.Write(r.body, true); err != nil {
			t.Errorf("Write: %v", err)
		}
	}
}

func randomBody(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

func (c *Conn) rejectedEarly() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.earlyRejected
}

func TestEarlyDataAccepted(t *testing.T) {
	for _, size := range []int{100, 20000, 300000} {
		t.Run("", func(t *testing.T) {
			env := newEarlyEnv()
			env.first(t)
			req := &earlyRequest{body: randomBody(size)}
			var server *earlyServer
			p := newTestPairOpts(t, 0, env.configure, pairOpts{
				afterDial:  req.send(t),
				wrapServer: func(h *testHandler) Handler { server = newEarlyServer(h, true); return server },
			})
			if !req.early {
				t.Fatal("the client could not send early data")
			}
			waitFinished(t, p.clientH, req.id)
			if got := p.clientH.received(req.id); !bytes.Equal(got, req.body) {
				t.Fatalf("echoed %d bytes, want %d, and the same", len(got), len(req.body))
			}
			if !server.sawEarly(req.id) {
				t.Fatal("the server did not see the stream as early data")
			}
			if calls := server.calls(); len(calls) < 2 || calls[0] != "early" || calls[1] != "data" {
				t.Fatalf("the server's calls: %v", calls)
			}
			if p.client.rejectedEarly() {
				t.Fatal("the early data was rejected")
			}
		})
	}
}

// A handler that is not an EarlyDataHandler is told of early data once the
// handshake is over.
func TestEarlyDataHeldForHandlersWithoutIt(t *testing.T) {
	env := newEarlyEnv()
	env.first(t)
	req := &earlyRequest{body: []byte("a request")}
	var handshaken chan struct{}
	p := newTestPairOpts(t, 0, env.configure, pairOpts{
		afterDial: req.send(t),
		wrapServer: func(h *testHandler) Handler {
			handshaken = h.handshake
			h.onData = func(s *Stream, data []byte, fin bool) {
				select {
				case <-handshaken:
				default:
					t.Error("early data delivered before the handshake was over")
				}
				if !s.EarlyData() {
					t.Error("the stream is not marked early")
				}
				_ = s.Write(data, fin)
			}
			return h
		},
	})
	waitFinished(t, p.clientH, req.id)
	if got := p.clientH.received(req.id); !bytes.Equal(got, req.body) {
		t.Fatalf("echoed %q", got)
	}
}

func TestEarlyDataNotOffered(t *testing.T) {
	t.Run("the server's ticket does not allow it", func(t *testing.T) {
		env := newEarlyEnv()
		env.serverEarly = false
		env.first(t)
		req := &earlyRequest{body: []byte("late")}
		p := newTestPairOpts(t, 0, env.configure, pairOpts{afterDial: req.send(t)})
		_ = p
		if req.early {
			t.Fatal("early data offered on a ticket that does not allow it")
		}
	})
	t.Run("the client does not use it", func(t *testing.T) {
		env := newEarlyEnv()
		env.first(t)
		env.clientEarly = false
		req := &earlyRequest{body: []byte("late")}
		p := newTestPairOpts(t, 0, env.configure, pairOpts{afterDial: req.send(t)})
		_ = p
		if req.early {
			t.Fatal("early data offered by a client that was not to")
		}
	})
}

func serverTLSForTest(t *testing.T) *tls.Config {
	t.Helper()
	c, _, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	c.NextProtos = []string{"test"}
	return c
}

func TestEarlyDataRejected(t *testing.T) {
	for _, size := range []int{100, 200000} {
		t.Run("", func(t *testing.T) {
			env := newEarlyEnv()
			env.first(t)
			// A server that cannot open the ticket turns the early data away.
			config := serverTLSForTest(t)
			config.SessionTicketKey = [32]byte{5}
			other := fibtls.NewQUICServer(config)
			configure := func(c, s *Config) {
				env.configure(c, s)
				s.NewTLS = func(bool, *tls.Config) TLSConn { return other.NewConn() }
			}
			req := &earlyRequest{body: randomBody(size)}
			var server *earlyServer
			p := newTestPairOpts(t, 0, configure, pairOpts{
				afterDial:  req.send(t),
				wrapServer: func(h *testHandler) Handler { server = newEarlyServer(h, true); return server },
			})
			if !req.early {
				t.Fatal("the client could not send early data")
			}
			waitFinished(t, p.clientH, req.id)
			if got := p.clientH.received(req.id); !bytes.Equal(got, req.body) {
				t.Fatalf("echoed %d bytes, want %d, and the same", len(got), len(req.body))
			}
			if server.sawEarly(req.id) {
				t.Fatal("the server saw early data it could not have")
			}
			if !p.client.rejectedEarly() {
				t.Fatal("the client was not told its early data was rejected")
			}
		})
	}
}

func TestEarlyDataLost(t *testing.T) {
	env := newEarlyEnv()
	env.first(t)
	req := &earlyRequest{body: randomBody(5000)}
	dropped := 0
	var server *earlyServer
	p := newTestPairOpts(t, 0, env.configure, pairOpts{
		afterDial:  req.send(t),
		wrapServer: func(h *testHandler) Handler { server = newEarlyServer(h, true); return server },
		pipes: func(client, _ *pipeEnd) {
			client.drop = func(d []byte) bool {
				// The first datagram that has early data in it is lost.
				if d[0]&0xf0 == 0xd0 && dropped < 1 {
					dropped++
					return true
				}
				return false
			}
		},
	})
	waitFinished(t, p.clientH, req.id)
	if got := p.clientH.received(req.id); !bytes.Equal(got, req.body) {
		t.Fatalf("echoed %d bytes, want %d", len(got), len(req.body))
	}
	if dropped == 0 {
		t.Fatal("no datagram carried early data")
	}
}

// TestEarlyDataReplay sends a server what a client sent another before, to see
// that it does not hear the early data twice.
func TestEarlyDataReplay(t *testing.T) {
	env := newEarlyEnv()
	env.first(t)
	var mu sync.Mutex
	var recorded [][]byte
	req := &earlyRequest{body: []byte("pay")}
	var server *earlyServer
	p := newTestPairOpts(t, 0, env.configure, pairOpts{
		afterDial:  req.send(t),
		wrapServer: func(h *testHandler) Handler { server = newEarlyServer(h, false); return server },
		pipes: func(client, _ *pipeEnd) {
			client.drop = func(d []byte) bool {
				mu.Lock()
				recorded = append(recorded, bytes.Clone(d))
				mu.Unlock()
				return false
			}
		},
	})
	waitFinished(t, p.serverH, req.id)
	if !server.sawEarly(req.id) {
		t.Fatal("the original did not arrive as early data")
	}

	mu.Lock()
	flight := append([][]byte(nil), recorded...)
	mu.Unlock()
	replayed := newEarlyServer(newTestHandler(), false)
	end, _ := newPipe()
	var conn *Conn
	for _, d := range flight {
		if conn == nil {
			if !IsInitial(d) {
				continue
			}
			sc := Config{TLSConfig: serverTLSForTest(t), EarlyData: true}
			sc.NewTLS = func(bool, *tls.Config) TLSConn { return env.server.NewConn() }
			conn = Accept(end, nil, sc, replayed, d)
			continue
		}
		conn.HandleDatagram(d)
	}
	if conn == nil {
		t.Fatal("the first flight has no Initial packet")
	}
	time.Sleep(200 * time.Millisecond)
	replayed.mu.Lock()
	heard := len(replayed.early)
	replayed.mu.Unlock()
	if heard != 0 || len(replayed.calls()) != 0 {
		t.Fatalf("the replayed flight delivered early data: %v", replayed.calls())
	}
	conn.Abort(errTestOver)
}
