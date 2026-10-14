package reqbody

import (
	"errors"
	"io"
	stdhttp "net/http"
	"strings"
	"testing"
)

// closeCounter is a body that counts how often it is closed.
type closeCounter struct {
	io.Reader
	closes int
}

func (c *closeCounter) Close() error { c.closes++; return nil }

// A second attempt at a streamed request abandons the first, closing what it
// was reading, and reads the body afresh from GetBody; without GetBody, or
// once the request is over, there is nothing to send again.
func TestSourceTakeReplaysThroughGetBody(t *testing.T) {
	first := &closeCounter{Reader: strings.NewReader("one")}
	again := &closeCounter{Reader: strings.NewReader("two")}
	req := &stdhttp.Request{Body: first, GetBody: func() (io.ReadCloser, error) { return again, nil }}
	s := NewSource(req)
	if _, err := s.Take(); err != nil {
		t.Fatal(err)
	}
	rc, err := s.Take()
	if err != nil {
		t.Fatal(err)
	}
	if first.closes != 1 {
		t.Errorf("the abandoned attempt's body was closed %d times, want 1", first.closes)
	}
	if b, _ := io.ReadAll(rc); string(b) != "two" {
		t.Errorf("the second attempt read %q, want the body GetBody returned", b)
	}
	s.Close()
	s.Close()
	if again.closes != 1 {
		t.Errorf("the second attempt's body was closed %d times, want 1", again.closes)
	}
	if _, err := s.Take(); err != ErrNotReplayable {
		t.Errorf("Take after Close = %v, want ErrNotReplayable", err)
	}

	broken := errors.New("cannot reopen")
	s = NewSource(&stdhttp.Request{Body: io.NopCloser(strings.NewReader("x")),
		GetBody: func() (io.ReadCloser, error) { return nil, broken }})
	_, _ = s.Take()
	if _, err := s.Take(); err != broken {
		t.Errorf("Take with a failing GetBody = %v, want its error", err)
	}

	s = NewSource(&stdhttp.Request{Body: io.NopCloser(strings.NewReader("x"))})
	_, _ = s.Take()
	if _, err := s.Take(); err != ErrNotReplayable {
		t.Errorf("Take again without GetBody = %v, want ErrNotReplayable", err)
	}
}

// A body longer than the length it declared fails as the body's fault, and
// the error says what it was.
func TestPumpRefusesABodyLongerThanDeclared(t *testing.T) {
	err := Pump(io.NopCloser(strings.NewReader("too long")), 3, func([]byte) error { return nil })
	var bodyErr *BodyError
	if !errors.As(err, &bodyErr) || !strings.Contains(err.Error(), "ContentLength=3") || errors.Unwrap(err) == nil {
		t.Fatalf("Pump = %v, want a BodyError naming the declared length", err)
	}
}
