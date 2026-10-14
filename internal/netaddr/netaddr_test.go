//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package netaddr

import (
	"net"
	"syscall"
	"testing"
)

// An empty listen address means what it means to net.Listen: every interface,
// on a port the kernel picks.
func TestResolveListenEmptyAddressIsAnyPort(t *testing.T) {
	family, sa, err := ResolveListen("tcp", "")
	if err != nil {
		t.Fatal(err)
	}
	addr, err := ToTCPAddr(sa)
	if err != nil {
		t.Fatal(err)
	}
	if addr.Port != 0 || !addr.IP.IsUnspecified() {
		t.Errorf("ResolveListen(tcp, \"\") = family %d, %v; want the unspecified address, port 0", family, addr)
	}
	if _, _, err := ResolveListen("unix", ""); err == nil {
		t.Error("ResolveListen accepted a unix socket with no path")
	}
}

// A dial address with no host, or an unspecified one, goes to the local system
// on the loopback of the family the network or address names, as net.Dial
// sends it; an empty network means "tcp".
func TestResolveDialUnspecifiedHostIsLoopback(t *testing.T) {
	cases := []struct {
		network, addr string
		want          string
	}{
		{"", ":80", "127.0.0.1:80"},
		{"tcp", "0.0.0.0:81", "127.0.0.1:81"},
		{"tcp6", ":82", "[::1]:82"},
		{"udp6", ":83", "[::1]:83"},
		{"tcp", "[::]:84", "[::1]:84"},
	}
	for _, tc := range cases {
		_, _, raddr, err := ResolveDial(tc.network, tc.addr)
		if err != nil {
			t.Errorf("ResolveDial(%q, %q): %v", tc.network, tc.addr, err)
			continue
		}
		if raddr.String() != tc.want {
			t.Errorf("ResolveDial(%q, %q) = %v, want %s", tc.network, tc.addr, raddr, tc.want)
		}
	}
	if _, _, _, err := ResolveDial("tcp4", "[2001:db8::1]:80"); err == nil {
		t.Error("ResolveDial(tcp4) accepted an IPv6 address")
	}
	if _, _, err := tcpAddrToSockaddr("udp4", "[2001:db8::1]:80", &net.TCPAddr{IP: net.ParseIP("2001:db8::1")}); err == nil {
		t.Error("an IPv6 address became an IPv4 socket address")
	}
}

// ToAddr has nothing to say about an address that is neither IP nor Unix.
func TestToAddrRejectsOtherFamilies(t *testing.T) {
	if addr := ToAddr(nil); addr != nil {
		t.Errorf("ToAddr(nil) = %v", addr)
	}
	if addr := ToAddr(&syscall.SockaddrUnix{Name: "/tmp/s"}); addr.String() != "/tmp/s" || addr.Network() != "unix" {
		t.Errorf("ToAddr(unix) = %v", addr)
	}
}

// A link-local address carries its zone to the socket as the interface's
// index, and back as its name; a zone naming no interface is refused.
func TestZonedIPv6AddressRoundTrips(t *testing.T) {
	_, _, err := tcpAddrToSockaddr("tcp6", "[fe80::1%nosuch]:80",
		&net.TCPAddr{IP: net.ParseIP("fe80::1"), Port: 80, Zone: "fib-no-such-interface"})
	if err == nil {
		t.Error("a zone naming no interface was accepted")
	}
	ifaces, err := net.Interfaces()
	if err != nil || len(ifaces) == 0 {
		t.Skip("no network interfaces to name a zone after")
	}
	iface := ifaces[0]
	family, sa, err := tcpAddrToSockaddr("tcp6", "",
		&net.TCPAddr{IP: net.ParseIP("fe80::1"), Port: 80, Zone: iface.Name})
	if err != nil {
		t.Fatal(err)
	}
	if v6, ok := sa.(*syscall.SockaddrInet6); family != syscall.AF_INET6 || !ok || v6.ZoneId != uint32(iface.Index) {
		t.Fatalf("family %d, %#v; want AF_INET6 with zone %d", family, sa, iface.Index)
	}
	addr, err := ToTCPAddr(sa)
	if err != nil {
		t.Fatal(err)
	}
	if addr.Zone != iface.Name {
		t.Errorf("zone came back as %q, want %q", addr.Zone, iface.Name)
	}
}
