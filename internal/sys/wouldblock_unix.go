//go:build unix

package sys

import "syscall"

// IsWouldBlock reports whether err says a non-blocking call had nothing to do
// yet.
func IsWouldBlock(err error) bool { return err == syscall.EAGAIN || err == syscall.EWOULDBLOCK }

// PortableErrno is errno as a failed connect reports it: on Unix, errno
// itself; see the Windows version.
func PortableErrno(errno syscall.Errno) error { return errno }
