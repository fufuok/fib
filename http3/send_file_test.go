//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	fibhttp "github.com/lesismal/fib/http"
)

// TestSendFile checks that over HTTP/3 SendFile sends a file whole, read a
// piece at a time and written without waiting, and that done hears of the
// end.
func TestSendFile(t *testing.T) {
	data := make([]byte, 4<<20)
	rand.New(rand.NewSource(1)).Read(data)
	path := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 1)
	base := startServer(t, Config{}, func(c *fibhttp.Context) {
		f, err := os.Open(path)
		if err != nil {
			t.Error(err)
			return
		}
		c.SendFile(f, 0, int64(len(data)), func(err error) {
			f.Close()
			results <- err
		})
	})
	client := newClient(t, nil)
	resp, err := client.Go(mustRequest(t, "GET", base+"/", nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got := readBody(t, resp); got != string(data) || resp.ContentLength != int64(len(data)) {
		t.Fatalf("got %d bytes, Content-Length %d, want %d", len(got), resp.ContentLength, len(data))
	}
	select {
	case err := <-results:
		if err != nil {
			t.Fatalf("done heard %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("done never ran")
	}
}
