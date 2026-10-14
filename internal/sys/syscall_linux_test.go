//go:build linux

package sys

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"unsafe"
)

// The accept backlog follows somaxconn where it can be read, is capped where
// the kernel's 16-bit backlog field would truncate it, and falls back to
// SOMAXCONN where the file is missing or says nothing usable.
func TestReadBacklogParsesSomaxconn(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		return path
	}
	cases := []struct {
		name string
		path string
		want int
	}{
		{"plain", write("plain", "4096\n"), 4096},
		{"capped", write("capped", "1000000\n"), 1<<16 - 1},
		{"garbage", write("garbage", "lots\n"), syscall.SOMAXCONN},
		{"zero", write("zero", "0\n"), syscall.SOMAXCONN},
		{"missing", filepath.Join(dir, "missing"), syscall.SOMAXCONN},
	}
	for _, tc := range cases {
		if got := readBacklog(tc.path); got != tc.want {
			t.Errorf("%s: readBacklog = %d, want %d", tc.name, got, tc.want)
		}
	}
	if got := DefaultBacklog(); got <= 0 || got > 1<<16-1 {
		t.Errorf("DefaultBacklog = %d", got)
	}
}

// WritevRaw, which the race and 386 builds write with, gathers its buffers
// into one write and reports the errno of a failed one.
func TestWritevRawGathersAndReportsErrors(t *testing.T) {
	var p [2]int
	if err := syscall.Pipe(p[:]); err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(p[0])
	defer syscall.Close(p[1])
	a, b := []byte("hello, "), []byte("world")
	iov := []syscall.Iovec{{Base: unsafe.SliceData(a)}, {Base: unsafe.SliceData(b)}}
	iov[0].SetLen(len(a))
	iov[1].SetLen(len(b))
	n, err := WritevRaw(p[1], iov)
	if err != nil || n != len(a)+len(b) {
		t.Fatalf("WritevRaw = %d, %v", n, err)
	}
	got := make([]byte, 32)
	if n, _ = syscall.Read(p[0], got); string(got[:n]) != "hello, world" {
		t.Fatalf("read %q", got[:n])
	}
	if _, err = WritevRaw(-1, iov); err != syscall.EBADF {
		t.Fatalf("WritevRaw on a bad descriptor = %v, want EBADF", err)
	}
}
