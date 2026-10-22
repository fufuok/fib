//go:build !linux

package prefork

import "syscall"

// supported is false: elsewhere SO_REUSEPORT does not spread connections
// between processes, or there is no fork, and Run serves in this process.
const supported = false

func sysProcAttr() *syscall.SysProcAttr { return nil }

// setupBPFFS and removeBPFFS do nothing where there is no prefork: Run serves
// in this process, so there are no children to share a selector.
func setupBPFFS() string   { return "" }
func removeBPFFS(_ string) {}
