//go:build linux

package udpbatch

import (
	"syscall"
	"testing"
	"unsafe"
)

// An IPv6 destination reaches the kernel in network byte order with its scope,
// as sendmmsg reads a sockaddr_in6.
func TestRawSockaddrIPv6(t *testing.T) {
	var rsa syscall.RawSockaddrAny
	to := &syscall.SockaddrInet6{Port: 0x1234, ZoneId: 7, Addr: [16]byte{15: 1}}
	if n := rawSockaddr(to, &rsa); n != syscall.SizeofSockaddrInet6 {
		t.Fatalf("length %d, want %d", n, syscall.SizeofSockaddrInet6)
	}
	p := (*syscall.RawSockaddrInet6)(unsafe.Pointer(&rsa))
	port := (*[2]byte)(unsafe.Pointer(&p.Port))
	if p.Family != syscall.AF_INET6 || port[0] != 0x12 || port[1] != 0x34 || p.Scope_id != 7 || p.Addr != to.Addr {
		t.Fatalf("raw sockaddr %+v", *p)
	}
	if n := rawSockaddr(nil, &rsa); n != 0 {
		t.Fatalf("nil sockaddr has length %d", n)
	}
}

// A batched send that fails for any reason but the kernel refusing the call
// reports the errno and that nothing was sent.
func TestSendBatchReportsErrno(t *testing.T) {
	n, err := SendSys(-1, nil, [][]byte{[]byte("a")})
	if err != syscall.EBADF || n != 0 {
		t.Fatalf("SendSys(-1) = %d, %v; want 0, EBADF", n, err)
	}
}
