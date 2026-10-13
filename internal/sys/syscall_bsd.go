//go:build dragonfly || freebsd || netbsd || openbsd

package sys

import (
	"net/netip"
	"sync"
	"syscall"
)

// The BSDs have accept4 and SOCK_NONBLOCK|SOCK_CLOEXEC, so a new descriptor
// is born with its flags, as on Linux, and needs no ForkLock. A write to a
// socket the peer has reset fails with EPIPE without stopping the program,
// since the runtime only lets SIGPIPE kill it for standard output and error.
func AcceptSocket(listenFD int) (int, netip.AddrPort, error) {
	fd, sa, err := syscall.Accept4(listenFD, syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC)
	if err != nil {
		return -1, netip.AddrPort{}, err
	}
	return fd, SockaddrAddrPort(sa), nil
}

func NewSocket(family int) (int, error) {
	return syscall.Socket(family, syscall.SOCK_STREAM|syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC, 0)
}

func NewDatagramSocket(family int) (int, error) {
	return syscall.Socket(family, syscall.SOCK_DGRAM|syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC, 0)
}

var (
	backlogOnce  sync.Once
	backlogValue int
)

// DefaultBacklog reports the accept queue depth the kernel is willing to
// honour, which is what net.Listen asks for. See the Linux version for why the
// historical SOMAXCONN of 128 is too shallow for a connection burst. FreeBSD
// and DragonFly name the limit kern.ipc.somaxconn, NetBSD and OpenBSD
// kern.somaxconn.
func DefaultBacklog() int {
	backlogOnce.Do(func() {
		backlogValue = syscall.SOMAXCONN
		for _, name := range [...]string{"kern.ipc.somaxconn", "kern.somaxconn"} {
			if limit, err := syscall.SysctlUint32(name); err == nil && limit > 0 {
				backlogValue = int(limit)
				break
			}
		}
	})
	return backlogValue
}
