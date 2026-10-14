//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	stdhttp "net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestFileCacheRootErrors checks that a cache is refused a root that does
// not exist, or that is a file rather than a directory.
func TestFileCacheRootErrors(t *testing.T) {
	dir := t.TempDir()
	if _, err := NewFileCache(FileCacheConfig{Root: filepath.Join(dir, "missing")}); err == nil {
		t.Fatal("a missing root was accepted")
	}
	file := filepath.Join(dir, "file")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := NewFileCache(FileCacheConfig{Root: file}); err == nil || !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("a file as the root: %v", err)
	}
}

// TestFileCacheNamesThatAreNoFile checks names that reach no file: the root
// itself, a directory, a path through a file, and one through a directory
// that does not exist, which a watch cannot watch. Each is a 404.
func TestFileCacheNamesThatAreNoFile(t *testing.T) {
	_, dir, addr := newTestFileCache(t, true)
	if err := os.Mkdir(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"", "sub", "plain.txt/x", "nosuchdir/x"} {
		if resp, _ := getStatic(t, addr, name); resp.StatusCode != stdhttp.StatusNotFound {
			t.Errorf("%q: %d, want 404", name, resp.StatusCode)
		}
	}
}

// TestFileCacheUnknownExtension checks the Content-Type of a file whose
// extension names none: sniffed from its bytes, as net/http.ServeContent
// does, while its compressed twin, whose bytes say nothing of the original,
// is application/octet-stream. It also serves the file a second time, which
// a cache without a watch answers from memory once the file's stamp shows
// it unchanged.
func TestFileCacheUnknownExtension(t *testing.T) {
	_, dir, addr := newTestFileCache(t, false)
	for name, body := range map[string]string{"page.zzunknown": "<html><body>hi</body></html>", "page.zzunknown.gz": "GZ"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		resp, body := getStatic(t, addr, "page.zzunknown")
		if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") || !strings.HasPrefix(body, "<html>") {
			t.Fatalf("plain: %q %q", ct, body)
		}
	}
	resp, body := getStatic(t, addr, "page.zzunknown", "Accept-Encoding", "gzip")
	if ct := resp.Header.Get("Content-Type"); ct != "application/octet-stream" || body != "GZ" {
		t.Fatalf("twin: %q %q", ct, body)
	}
}

// TestFileCacheLargeTwinFromDisk serves a compressed twin too large to keep,
// from the disk: net/http.ServeContent leaves out the length of a body with
// a Content-Encoding, and the cache puts it back for the whole file.
func TestFileCacheLargeTwinFromDisk(t *testing.T) {
	dir := t.TempDir()
	twin := strings.Repeat("z", 64)
	for name, body := range map[string]string{"big.css": "body{}", "big.css.gz": twin} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	fc, err := NewFileCache(FileCacheConfig{Root: dir, Precompressed: true, MaxFileBytes: 16})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fc.Close() })
	addr := serveHTTP1(t, func(c *Context) {
		fc.ServeFile(c, strings.TrimPrefix(c.Request.URL.Path, "/static/"))
	})
	resp, body := getStatic(t, addr, "big.css", "Accept-Encoding", "gzip")
	if body != twin || resp.Header.Get("Content-Encoding") != "gzip" || resp.ContentLength != int64(len(twin)) {
		t.Fatalf("%d bytes, encoding %q, length %d", len(body), resp.Header.Get("Content-Encoding"), resp.ContentLength)
	}
}

// TestFileCacheServeFromDiskGone checks a file too large to keep that is
// gone by the time it is opened to be served, as one deleted between the
// lookup and the open is: a 404.
func TestFileCacheServeFromDiskGone(t *testing.T) {
	fc, _, _ := newTestFileCache(t, false)
	stream := new(extStream)
	_, c := newExtRequest(stdhttp.MethodGet, "/gone.bin", stream)
	fc.serveFromDisk(c, "gone.bin", &cachedFile{name: "gone.bin"})
	if got := stream.response(t); got.StatusCode != stdhttp.StatusNotFound {
		t.Fatalf("status %d, want 404", got.StatusCode)
	}
}

// TestFileCacheChangedTwins checks what a reported change drops: a change to
// a twin drops what is cached of it as its original's twin, and leaves the
// original, and a change to the original drops it and its twins.
func TestFileCacheChangedTwins(t *testing.T) {
	fc, _, _ := newTestFileCache(t, false)
	keys := []fileKey{{"a.css", noTwin}, {"a.css", brTwin}, {"a.css", gzTwin}}
	load := func() {
		for _, key := range keys {
			if fc.lookup(key) == nil {
				t.Fatalf("%v did not load", key)
			}
		}
	}
	cached := func(key fileKey) bool {
		fc.mu.RLock()
		defer fc.mu.RUnlock()
		return fc.entries[key] != nil
	}
	load()
	fc.changed("a.css.br")
	if cached(keys[1]) || !cached(keys[0]) || !cached(keys[2]) {
		t.Fatal("a change to the .br twin dropped the wrong entries")
	}
	fc.changed("a.css.gz")
	if cached(keys[2]) || !cached(keys[0]) {
		t.Fatal("a change to the .gz twin dropped the wrong entries")
	}
	load()
	fc.changed("a.css")
	for _, key := range keys {
		if cached(key) {
			t.Fatalf("a change to the original left %v", key)
		}
	}
	fc.mu.RLock()
	defer fc.mu.RUnlock()
	if fc.bytes != 0 {
		t.Fatalf("%d bytes counted with nothing cached", fc.bytes)
	}
}

// TestFileCacheDirectoryRemoved removes a directory whose file the cache
// holds. A watch that loses the directory can trust nothing it cached, and
// the cache without one finds the file gone when it checks it: either way
// the file is a 404 from then on.
func TestFileCacheDirectoryRemoved(t *testing.T) {
	_, dir, addr := newTestFileCache(t, true)
	sub := filepath.Join(dir, "sub")
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "f.txt"), []byte("in sub"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, body := getStatic(t, addr, "sub/f.txt"); body != "in sub" {
		t.Fatalf("before: %q", body)
	}
	if err := os.RemoveAll(sub); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, _ := getStatic(t, addr, "sub/f.txt")
		if resp.StatusCode == stdhttp.StatusNotFound {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the removed file is still served: %d", resp.StatusCode)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
