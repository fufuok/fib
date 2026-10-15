package socks

import (
	"net"
	"net/netip"
	"sync"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/internal/outputwait"
)

// A BIND listens on a port of its own, for one connection, and the engine
// cannot take up a listener or a socket after it has started: the listener
// and the connection it accepts are the net package's, served by two
// goroutines of their own for as long as the connection lasts. BIND is
// rare, FTP's active mode being about its only use, and the goroutines are
// its alone.

// listenTCP is net.ListenTCP, which a test replaces to make it fail.
var listenTCP = net.ListenTCP

// binding is a BIND waiting for its peer.
type binding struct {
	sc     *serverConn
	ln     *net.TCPListener
	expect netip.Addr
}

// bind listens for a BIND's peer, on the address the client reached the
// server at, and tells the client where.
func (s *Server) bind(sc *serverConn, req *Request) {
	local, _ := sc.c.LocalAddr().(*net.TCPAddr)
	if local == nil {
		sc.refuse(ReplyCommandNotSupported)
		return
	}
	ln, err := listenTCP("tcp", &net.TCPAddr{IP: local.IP, Zone: local.Zone})
	if err != nil {
		sc.refuse(replyCode(err))
		return
	}
	b := &binding{sc: sc, ln: ln}
	// Only the peer the client named may connect, when it named one by IP.
	if host, _, err := splitHostPort(req.Addr); err == nil {
		if ip, err := netip.ParseAddr(host); err == nil && !ip.IsUnspecified() {
			b.expect = ip.Unmap()
		}
	}
	sc.mu.Lock()
	sc.binding = b
	sc.mu.Unlock()
	sc.reply(ReplySucceeded, ln.Addr())
	go b.accept(s.config.BindTimeout)
}

// accept waits for the peer, and relays it.
func (b *binding) accept(timeout time.Duration) {
	_ = b.ln.SetDeadline(time.Now().Add(timeout))
	for {
		conn, err := b.ln.AcceptTCP()
		if err != nil {
			_ = b.ln.Close()
			b.sc.bindFailed(err)
			return
		}
		if b.expect.IsValid() && conn.RemoteAddr().(*net.TCPAddr).AddrPort().Addr().Unmap() != b.expect {
			_ = conn.Close()
			continue
		}
		_ = b.ln.Close()
		b.sc.bound(conn)
		return
	}
}

// cancel stops waiting, when the client has gone.
func (b *binding) cancel() { _ = b.ln.Close() }

// bindFailed answers a BIND whose peer never came.
func (sc *serverConn) bindFailed(err error) {
	sc.mu.Lock()
	closed := sc.state == stateClosed
	sc.state = stateClosed
	sc.mu.Unlock()
	if !closed {
		sc.refuse(replyCode(err))
	}
}

// bound answers a BIND whose peer has connected, and relays the client to
// it.
func (sc *serverConn) bound(conn *net.TCPConn) {
	p := &streamPipe{c: sc.c, conn: conn, limit: sc.s.config.MaxQueuedBytes}
	p.cond.L = &p.mu
	sc.mu.Lock()
	if sc.state == stateClosed {
		sc.mu.Unlock()
		_ = conn.Close()
		return
	}
	sc.state = stateRelaying
	sc.relay = p
	// The client is paced by the queue to the peer, not its own backlog; see
	// tunnel.
	sc.c.SetWriteBackpressure(false)
	early := sc.early
	sc.early = nil
	sc.reply(ReplySucceeded, conn.RemoteAddr())
	// Reads held for early data stay held: as much of it is in the pipe's
	// queue as was waiting here, so enqueue holds them again, and the writer
	// lets them go once it takes it.
	if len(early) > 0 {
		p.enqueue(early)
	}
	sc.mu.Unlock()
	go p.write()
	go p.read()
}

// streamPipe relays a client to a connection of the net package's: the
// client's handler queues what it receives for a goroutine that writes it,
// and another goroutine reads the connection and sends what it reads to the
// client.
type streamPipe struct {
	c     *fib.Connection
	conn  net.Conn
	limit int

	mu   sync.Mutex
	cond sync.Cond
	// queue is what waits for the writer, and held says the client's reads
	// are held because it has grown to the limit.
	queue  []byte
	held   bool
	closed bool
}

func (p *streamPipe) OnOpen(*fib.Connection)                 {}
func (p *streamPipe) OnData(_ *fib.Connection, data []byte)  { p.enqueue(data) }
func (p *streamPipe) OnPriorityData(*fib.Connection, []byte) {}

func (p *streamPipe) OnClose(*fib.Connection, error) {
	p.mu.Lock()
	p.closed = true
	p.cond.Signal()
	p.mu.Unlock()
}

// enqueue queues data for the writer, and holds the client's reads once the
// queue reaches the limit, reporting that it did.
//
// HoldReads is called under the lock, here and in write, so that the hold
// it leaves is the one held says: the last call wins, and the writer's
// release could otherwise overtake the hold it releases.
func (p *streamPipe) enqueue(data []byte) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.queue = append(p.queue, data...)
	hold := !p.held && len(p.queue) >= p.limit
	if hold {
		p.held = true
		p.c.HoldReads(true)
	}
	p.cond.Signal()
	return hold
}

// write writes what the client sends to the connection, until the client
// has closed and all of it has been written, and then closes it.
func (p *streamPipe) write() {
	var out []byte
	for {
		p.mu.Lock()
		for len(p.queue) == 0 && !p.closed {
			p.cond.Wait()
		}
		if len(p.queue) == 0 {
			p.mu.Unlock()
			break
		}
		out, p.queue = p.queue, out[:0]
		if p.held {
			p.held = false
			p.c.HoldReads(false)
		}
		p.mu.Unlock()
		if _, err := p.conn.Write(out); err != nil {
			p.c.Close()
			break
		}
	}
	_ = p.conn.Close()
}

// read sends what the connection carries to the client, waiting while more
// than the limit of it is queued there, and ends the client once the
// connection has ended.
func (p *streamPipe) read() {
	buf := make([]byte, 32<<10)
	for {
		n, err := p.conn.Read(buf)
		if n > 0 {
			if p.c.Send(buf[:n]) != nil {
				break
			}
			if outputwait.Wait != nil && outputwait.Wait(p.c, int64(p.limit)) != nil {
				break
			}
		}
		if err != nil {
			break
		}
	}
	p.c.CloseAfterSend()
}
