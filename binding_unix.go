//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd

package fib

import (
	"net"
	"net/netip"
	"syscall"

	"github.com/lesismal/fib/internal/listener"
	"github.com/lesismal/fib/internal/netaddr"
)

// placement is a socket AddBinding opened and the loop that is to wait on
// it: a TCP or Unix listener, or a UDP socket when udp is set.
type placement struct {
	loop   *Engine
	stream streamListener
	udp    *udpListener
}

// close closes the socket, unless it is the engine's own, shared with the
// poller it was placed on, which the engine closes itself.
func (p placement) close() {
	switch {
	case p.udp != nil:
		syscall.Close(p.udp.fd)
	case !p.stream.shared || p.loop.parent == nil:
		syscall.Close(p.stream.fd)
	}
}

func newUDPListener(fd int, b Binding, i int) *udpListener {
	return &udpListener{udpListenerPlatform: udpListenerPlatform{fd: fd}, network: b.Network, handler: b.Handler,
		binding: i, peers: make(map[netip.AddrPort]*Connection)}
}

// openBinding opens the sockets of b, the i-th binding, and has the loops
// wait on them, each where open would have placed it had New been given
// b: a TCP or Unix listener on the engine's loop, or with pollers that
// listen, on every poller, which waits on the engine's socket or, with
// SO_REUSEPORT spreading connections, on one of its own beside it; a UDP
// socket on the loop that reads the engine's UDP sockets, and beside it a
// socket for each other poller where they each read one. It opens every
// socket before any loop waits on one, and on failure closes whatever it
// opened.
func (e *Engine) openBinding(b Binding, i int) error {
	opts := listenOptions(e.bindConfig, b.Network)
	// held are the engine's own sockets where it has no loop to place them
	// on: those its pollers share, or that hold an address for them.
	var held []streamListener
	var placed []placement
	fail := func(err error) error {
		for _, l := range held {
			syscall.Close(l.fd)
		}
		for _, p := range placed {
			p.close()
		}
		e.removeUnixPathsOf(i)
		return err
	}
	for _, addr := range b.Addrs {
		spread := pollersListen(e.bindConfig, b.Network)
		if netaddr.IsUDP(b.Network) {
			fd, err := listener.CreateUDP(opts, addr)
			if err != nil {
				return fail(err)
			}
			owner := e
			if !e.udpOwn() {
				owner = e.pollers[0]
			}
			placed = append(placed, placement{loop: owner, udp: newUDPListener(fd, b, i)})
			if spread {
				for _, p := range e.pollers[1:] {
					like, err := listener.CreateUDPLike(opts, fd)
					if err != nil {
						return fail(err)
					}
					placed = append(placed, placement{loop: p, udp: newUDPListener(like, b, i)})
				}
			}
			continue
		}
		fd, err := listener.Create(opts, addr, !spread, spread)
		if err != nil {
			return fail(err)
		}
		l := streamListener{fd: fd, network: b.Network, unix: netaddr.IsUnix(b.Network), spread: spread,
			shared: e.noLoop && !spread, handler: b.Handler, binding: i}
		e.noteUnixPath(b.Network, addr, i)
		if !e.noLoop {
			placed = append(placed, placement{loop: e, stream: l})
			continue
		}
		held = append(held, l)
		for _, p := range e.pollers {
			own := l
			if spread {
				like, err := listener.CreateLike(opts, fd)
				if err != nil {
					return fail(err)
				}
				own.fd, own.spread = like, false
			}
			placed = append(placed, placement{loop: p, stream: own})
		}
	}
	if len(held) > 0 {
		e.listenMu.Lock()
		stopped := e.listenStopped
		if !stopped {
			e.listeners = append(e.listeners, held...)
		}
		e.listenMu.Unlock()
		if stopped {
			return fail(net.ErrClosed)
		}
	}
	loops := e.loops()
	for n, loop := range loops {
		var mine []placement
		for _, p := range placed {
			if p.loop == loop {
				mine = append(mine, p)
			}
		}
		if len(mine) == 0 {
			continue
		}
		var attachErr error
		err := loop.onLoop(func() { attachErr = loop.attachSockets(mine) })
		if err == nil {
			err = attachErr
		}
		if err == nil {
			continue
		}
		// The loops that never took theirs leave them here to close. The
		// others hold theirs, or will once their queue drains, and give
		// them up as the binding is closed, or as the engine is.
		if err == errLoopClosed {
			n--
		}
		for _, later := range loops[n+1:] {
			for _, p := range placed {
				if p.loop == later {
					p.close()
				}
			}
		}
		_, _ = e.closeBinding(i, false)
		return err
	}
	return nil
}

// attachSockets puts sockets AddBinding opened on the loop's lists, and has
// the loop wait on them. A loop whose listeners have stopped takes none, and
// closes them. A socket the loop failed to wait on stays on its list, for
// closeBinding to take off. Callers run on the event loop.
func (e *Engine) attachSockets(sockets []placement) error {
	e.listenMu.Lock()
	if e.listenStopped {
		e.listenMu.Unlock()
		for _, p := range sockets {
			p.close()
		}
		return net.ErrClosed
	}
	for _, p := range sockets {
		if p.udp != nil {
			e.udpListeners = append(e.udpListeners, p.udp)
		} else {
			e.listeners = append(e.listeners, p.stream)
		}
	}
	e.listenMu.Unlock()
	for _, p := range sockets {
		var err error
		if p.udp != nil {
			err = e.watchUDP(p.udp.fd)
		} else {
			err = e.watchListener(p.stream)
		}
		if err != nil {
			return err
		}
	}
	e.startUDPSweeper()
	return nil
}

// closeBinding takes the i-th binding's sockets off every loop, the pollers
// before the engine's own sockets they share, and closes them. With keepUDP,
// its UDP sockets stay, open no connection for a peer they do not know, and
// go on serving the ones they do; without it they close, and so do those
// peers. It reports whether the engine had any socket of the binding left,
// and net.ErrClosed if the engine stopped before every loop let its sockets
// go, which leaves those to Close.
func (e *Engine) closeBinding(i int, keepUDP bool) (bool, error) {
	found := false
	var err error
	for _, loop := range e.loops() {
		var had bool
		if runErr := loop.onLoop(func() { had = loop.detachBinding(i, keepUDP) }); runErr != nil {
			err = runErr
			continue
		}
		found = found || had
	}
	if e.noLoop {
		e.listenMu.Lock()
		var held []streamListener
		if err == nil {
			held = e.dropListenersLocked(i)
		} else {
			// A poller may still wait on them, so they stay for Close.
			for _, l := range e.listeners {
				found = found || l.binding == i
			}
		}
		e.listenMu.Unlock()
		for _, l := range held {
			syscall.Close(l.fd)
		}
		found = found || len(held) > 0
	}
	if err == nil {
		e.removeUnixPathsOf(i)
	}
	return found, err
}

// detachBinding takes the loop's sockets of the i-th binding off its lists
// and stops waiting on them, as closeBinding describes, and reports whether
// it had any. Callers run on the event loop.
func (e *Engine) detachBinding(i int, keepUDP bool) bool {
	e.listenMu.Lock()
	streams := e.dropListenersLocked(i)
	found := len(streams) > 0
	var udps []*udpListener
	kept := e.udpListeners[:0]
	for _, l := range e.udpListeners {
		if l.binding == i {
			found = true
			if !keepUDP {
				udps = append(udps, l)
				continue
			}
			l.draining = true
		}
		kept = append(kept, l)
	}
	clear(e.udpListeners[len(kept):])
	e.udpListeners = kept
	e.listenMu.Unlock()
	for _, l := range streams {
		// The loop waits on what is on its lists, which a poller's socket
		// that only holds an address never is.
		e.unwatch(l.fd)
		if !l.shared || e.parent == nil {
			syscall.Close(l.fd)
		}
	}
	for _, l := range udps {
		e.unwatch(l.fd)
		// The peers send from the socket, so none outlives it.
		for _, c := range l.peers {
			e.closeConnection(c, net.ErrClosed, true)
		}
		syscall.Close(l.fd)
	}
	return found
}

// dropListenersLocked takes the i-th binding's TCP and Unix listeners off
// the engine's list, with the addresses stopListening kept for them, and
// returns them. Callers hold listenMu.
func (e *Engine) dropListenersLocked(i int) []streamListener {
	var dropped []streamListener
	tracked := e.listenStopped && len(e.listenAddrs) == len(e.listeners)
	kept := e.listeners[:0]
	keptAddrs := e.listenAddrs[:0]
	for k, l := range e.listeners {
		if l.binding == i {
			dropped = append(dropped, l)
			continue
		}
		kept = append(kept, l)
		if tracked {
			keptAddrs = append(keptAddrs, e.listenAddrs[k])
		}
	}
	clear(e.listeners[len(kept):])
	e.listeners = kept
	if tracked {
		clear(e.listenAddrs[len(keptAddrs):])
		e.listenAddrs = keptAddrs
	}
	return dropped
}

// closeBindingConnections closes what is left of the i-th binding's
// connections, and its UDP sockets' peers, as ShutdownBinding does once its
// ctx is done.
func (e *Engine) closeBindingConnections(i int) {
	for _, loop := range e.loops() {
		_ = loop.onLoop(func() { loop.closeConnectionsOf(i) })
	}
}

// closeConnectionsOf closes the loop's connections of the i-th binding, and
// its peers on that binding's UDP sockets. Callers run on the event loop.
func (e *Engine) closeConnectionsOf(i int) {
	for _, entries := range e.connections {
		for _, c := range entries {
			if c != nil && !c.dialed && c.inBinding(i) {
				e.closeConnection(c, net.ErrClosed, true)
			}
		}
	}
	for _, l := range e.udpListeners {
		if l.binding != i {
			continue
		}
		for _, c := range l.peers {
			e.closeConnection(c, net.ErrClosed, true)
		}
	}
}
