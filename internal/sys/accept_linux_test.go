//go:build linux && !386

package sys

import (
	"net/netip"
	"syscall"
	"testing"
	"unsafe"
)

// An accepted peer's address decodes as SockaddrAddrPort would decode it:
// a zoned IPv6 peer, a link-local one, has no AddrPort, and a family other
// than IP has none either.
func TestRawAddrPortDecodesKernelAddresses(t *testing.T) {
	var raw syscall.RawSockaddrAny
	v6 := (*syscall.RawSockaddrInet6)(unsafe.Pointer(&raw))
	v6.Family = syscall.AF_INET6
	port := (*[2]byte)(unsafe.Pointer(&v6.Port))
	port[0], port[1] = 0x1f, 0x90
	copy(v6.Addr[:], netip.MustParseAddr("::ffff:10.0.0.7").AsSlice())
	if got, want := rawAddrPort(&raw), netip.MustParseAddrPort("10.0.0.7:8080"); got != want {
		t.Errorf("mapped IPv6: %v, want %v", got, want)
	}
	v6.Scope_id = 2
	if got := rawAddrPort(&raw); got.IsValid() {
		t.Errorf("zoned IPv6: %v, want the zero AddrPort", got)
	}
	raw.Addr.Family = syscall.AF_UNIX
	if got := rawAddrPort(&raw); got.IsValid() {
		t.Errorf("unix: %v, want the zero AddrPort", got)
	}
}
