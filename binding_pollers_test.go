//go:build linux

package fib

import (
	"testing"
)

// Under pollers each binding's addresses are served as one address on its
// own network would be: the pollers accept TCP connections themselves, the
// engine's loop accepts the Unix ones and hands them on, and with ReusePort
// the pollers read UDP datagrams themselves, each with the handler of the
// binding the socket belongs to.
func TestBindingsUnderPollers(t *testing.T) {
	for _, reusePort := range []bool{false, true} {
		config := pollerConfig("bindings-pollers", 3)
		config.ReusePort = reusePort
		config.Bindings = []Binding{
			{Addr: "127.0.0.1:0", Handler: prefixEcho("t:")},
			{Network: "unix", Addr: unixSocketPath(t), Handler: prefixEcho("x:")},
			{Network: "udp", Addr: "127.0.0.1:0", Handler: prefixEcho("u:")},
		}
		server, err := Bind(config, nil)
		if err != nil {
			t.Fatal(err)
		}
		runEngine(t, server)
		if !server.pollersListen || server.udpSpread != reusePort {
			t.Fatalf("ReusePort=%v: pollersListen=%v udpSpread=%v", reusePort, server.pollersListen, server.udpSpread)
		}
		if len(server.listeners) != 2 || !server.listeners[0].spread || server.listeners[1].spread {
			t.Fatalf("ReusePort=%v: the engine's listeners are %+v", reusePort, server.listeners)
		}
		for i, p := range server.pollers {
			if len(p.listeners) != 1 || p.listeners[0].binding != 0 {
				t.Fatalf("poller %d listens on %+v, want the TCP binding alone", i, p.listeners)
			}
			wantUDP := 0
			if reusePort {
				wantUDP = 1
			}
			if len(p.udpListeners) != wantUDP {
				t.Fatalf("poller %d reads %d UDP sockets, want %d", i, len(p.udpListeners), wantUDP)
			}
			for _, l := range p.udpListeners {
				if l.binding != 2 {
					t.Fatalf("poller %d's UDP socket serves binding %d", i, l.binding)
				}
			}
		}
		for i, prefix := range []string{"t:", "x:", "u:"} {
			addrs, err := server.BindingAddrs(i)
			if err != nil || len(addrs) != 1 {
				t.Fatalf("BindingAddrs(%d) = %v, %v", i, addrs, err)
			}
			for round := 0; round < 6; round++ {
				if got := exchange(t, addrs[0], "ping"); got != prefix+"ping" {
					t.Fatalf("binding %d answered %q", i, got)
				}
			}
		}
	}
}
