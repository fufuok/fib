package sys

import (
	"net/netip"
	"syscall"
	"testing"
)

// SockaddrAddrPort decodes the two IP families, gives an IPv4-mapped IPv6
// address back as the IPv4 address it is, so that a dual-stack listener
// reports the same peer as an IPv4 one, and answers the zero AddrPort for a
// zoned IPv6 address, which an AddrPort cannot carry by index, and for any
// other kind of address.
func TestSockaddrAddrPortDecodesEachFamily(t *testing.T) {
	mapped := &syscall.SockaddrInet6{Port: 8080}
	copy(mapped.Addr[:], netip.MustParseAddr("::ffff:10.1.2.3").AsSlice())
	v6 := &syscall.SockaddrInet6{Port: 443}
	copy(v6.Addr[:], netip.MustParseAddr("2001:db8::1").AsSlice())
	zoned := &syscall.SockaddrInet6{Port: 443, ZoneId: 1}
	cases := []struct {
		name string
		sa   syscall.Sockaddr
		want netip.AddrPort
	}{
		{"ipv4", &syscall.SockaddrInet4{Port: 80, Addr: [4]byte{127, 0, 0, 1}}, netip.MustParseAddrPort("127.0.0.1:80")},
		{"mapped", mapped, netip.MustParseAddrPort("10.1.2.3:8080")},
		{"ipv6", v6, netip.MustParseAddrPort("[2001:db8::1]:443")},
		{"zoned", zoned, netip.AddrPort{}},
		{"other", nil, netip.AddrPort{}},
	}
	for _, tc := range cases {
		if got := SockaddrAddrPort(tc.sa); got != tc.want {
			t.Errorf("%s: SockaddrAddrPort = %v, want %v", tc.name, got, tc.want)
		}
	}
}
