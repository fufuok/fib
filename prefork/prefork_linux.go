package prefork

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// supported is whether this platform's SO_REUSEPORT spreads connections
// between processes, which Linux's does.
const supported = true

// sysProcAttr has a child sent SIGTERM if the master exits, so that it stops
// rather than outlive the process that supervises it.
func sysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Pdeathsig: syscall.SIGTERM}
}

// bpffsMount is where the kernel's bpf filesystem is mounted by convention,
// under which pinned bpf objects can be shared between processes.
const bpffsMount = "/sys/fs/bpf"

// setupBPFFS makes a directory under bpffs for this run's children to pin
// their shared reuseport selectors in, and returns its path, or "" when
// bpffs is not mounted or the directory cannot be made (no CAP_BPF, say), in
// which case the children keep the kernel's hash. The name carries the
// master's pid and the time so that runs do not collide.
func setupBPFFS() string {
	if fi, err := os.Stat(bpffsMount); err != nil || !fi.IsDir() {
		return ""
	}
	dir := filepath.Join(bpffsMount, fmt.Sprintf("fib-prefork-%d-%d", os.Getpid(), time.Now().UnixNano()))
	if err := os.Mkdir(dir, 0o700); err != nil {
		return ""
	}
	return dir
}

// removeBPFFS removes the run's bpffs directory and the selectors pinned
// under it, so nothing is left behind once the children have exited.
func removeBPFFS(dir string) { _ = os.RemoveAll(dir) }
