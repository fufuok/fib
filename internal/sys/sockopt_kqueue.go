//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package sys

import "syscall"

const SoReusePort = syscall.SO_REUSEPORT

// ReusePortSpreads is false: Darwin and the BSDs let sockets share an address
// through SO_REUSEPORT, but, FreeBSD's SO_REUSEPORT_LB and DragonFly aside,
// hand a TCP connection to one of them rather than spreading connections over
// all of them, so the pollers cannot each accept their own; see
// Config.ReusePort.
const ReusePortSpreads = false

// AcceptedInheritNoDelay is false: each accepted connection is given
// TCP_NODELAY itself.
const AcceptedInheritNoDelay = false
