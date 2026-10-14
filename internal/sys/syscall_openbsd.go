package sys

import (
	"syscall"
	"unsafe"
)

// OpenBSD only lets system calls through libc, so these go through the
// syscall package rather than trapping directly.

// As on Darwin, go:noescape keeps what iovecs points at off the heap: the
// body is out of the compiler's sight here, but syscall.writev keeps nothing.
//
//go:noescape
//go:linkname writev syscall.writev
func writev(fd int, iovecs []syscall.Iovec) (cnt uintptr, err error)

func WritevRaw(fd int, iov []syscall.Iovec) (int, error) {
	n, err := writev(fd, iov)
	return int(n), err
}

// Recvfrom reads one datagram into p and the sender's address into from.
// The libc recvfrom wrapper is not open to linkname, so this pays the
// allocation for the address that recvfrom_unix.go avoids elsewhere.
func Recvfrom(fd int, p []byte, from *syscall.RawSockaddrAny) (int, error) {
	n, sa, err := syscall.Recvfrom(fd, p, 0)
	if err != nil {
		return 0, err
	}
	*from = syscall.RawSockaddrAny{}
	switch a := sa.(type) {
	case *syscall.SockaddrInet4:
		r := (*syscall.RawSockaddrInet4)(unsafe.Pointer(from))
		r.Len, r.Family = syscall.SizeofSockaddrInet4, syscall.AF_INET
		port := (*[2]byte)(unsafe.Pointer(&r.Port))
		port[0], port[1] = byte(a.Port>>8), byte(a.Port)
		r.Addr = a.Addr
	case *syscall.SockaddrInet6:
		r := (*syscall.RawSockaddrInet6)(unsafe.Pointer(from))
		r.Len, r.Family = syscall.SizeofSockaddrInet6, syscall.AF_INET6
		port := (*[2]byte)(unsafe.Pointer(&r.Port))
		port[0], port[1] = byte(a.Port>>8), byte(a.Port)
		r.Scope_id = a.ZoneId
		r.Addr = a.Addr
	}
	return n, nil
}
