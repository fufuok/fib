//go:build dragonfly || freebsd || netbsd

package sys

import (
	"syscall"
	"unsafe"
)

func WritevRaw(fd int, iov []syscall.Iovec) (int, error) {
	n, _, errno := syscall.Syscall(syscall.SYS_WRITEV, uintptr(fd),
		uintptr(unsafe.Pointer(unsafe.SliceData(iov))), uintptr(len(iov)))
	if errno != 0 {
		return -1, errno
	}
	return int(n), nil
}
