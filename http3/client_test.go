package http3

import (
	"io"
	stdhttp "net/http"
	"testing"
	"time"
)

// A streamed request that fails before it is sent, on a header that cannot be
// written or a scheme other than https, still has its body closed, as a
// transport closes it whatever becomes of the request.
func TestClientUploadBadHeaderClosesBody(t *testing.T) {
	client := newClient(t, nil)
	for _, url := range []string{"https://localhost:1", "http://localhost:1"} {
		t.Run(url, func(t *testing.T) { badRequestClosesBody(t, client, url) })
	}
}

// badRequestClosesBody sends a request with a bad header to url, which Do
// turns away before sending it for that or, on http, for its scheme.
func badRequestClosesBody(t *testing.T, client *Client, url string) {
	pr, pw := io.Pipe()
	defer pw.Close()
	closed := make(chan struct{})
	req := mustRequest(t, stdhttp.MethodPost, url, nil)
	req.Body = closeNotifier{pr, closed}
	req.Header.Set("X-Bad", "a\r\nb")
	if _, err := client.Go(req).Wait(); err == nil {
		t.Fatal("a request with a bad header was sent")
	}
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("the request body was never closed")
	}
}

type closeNotifier struct {
	io.Reader
	closed chan struct{}
}

func (c closeNotifier) Close() error { close(c.closed); return nil }
