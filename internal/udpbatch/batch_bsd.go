//go:build dragonfly || freebsd || netbsd || openbsd

package udpbatch

import "syscall"

// batchHeader is empty: these kernels are not given batched datagram calls
// here, so a Batch reads, and Send sends, one datagram at a time.
type batchHeader struct{}

func (b *Batch) recvBatch(int, int) (int, error) { return 0, syscall.ENOSYS }

func sendBatchSys(int, syscall.Sockaddr, [][]byte) (int, error) { return 0, syscall.ENOSYS }
