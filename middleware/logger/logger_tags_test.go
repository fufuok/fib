//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package logger_test

import (
	stdhttp "net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/middleware/logger"
	"github.com/lesismal/fib/middleware/mwtest"
)

// waitLines waits for out to hold n lines, the last of them written once its
// response was handed to the connection, which can be after the client has
// it.
func waitLines(t *testing.T, out *syncBuffer, n int) []string {
	t.Helper()
	var got []string
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		if got = out.lines(); len(got) == n && got[0] != "" {
			break
		}
	}
	return got
}

// TestLoggerAllTags checks every tag the Format documents that TestLogger
// does not, a header the request lacks standing for nothing, and a "${" with
// no closing brace being written as it is.
func TestLoggerAllTags(t *testing.T) {
	out := &syncBuffer{}
	handler := logger.New(logger.Config{
		Format:     "${time}|${latency}|${query}|${host}|${referer}|${ua}|${bytesReceived}|${pid}|${reqHeader:X-Absent}|${path}|${unclosed\n",
		TimeFormat: "2006",
		Output:     out,
		Next:       func(c *fibhttp.Context) bool { return c.Request.URL.Path == "/skip" },
	})(fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("ok"))
	}))
	url := mwtest.Serve(t, handler)
	c := mwtest.Clients(t)[0]
	// The skipped request writes no line; the one after it does.
	c.Do(t, "GET", url+"/skip", nil, "")
	c.Do(t, "POST", url+"/p?a=1&b=2", stdhttp.Header{"Referer": {"https://ref.example/"}, "User-Agent": {"tester"}}, "body!")
	got := waitLines(t, out, 1)
	if len(got) != 1 {
		t.Fatalf("logged %q, want one line", got)
	}
	fields := strings.Split(got[0], "|")
	if len(fields) != 11 {
		t.Fatalf("logged %q", got[0])
	}
	host := strings.TrimPrefix(url, "http://")
	// ${time} and ${latency} vary, and are checked by their form.
	want := []string{"", "", "a=1&b=2", host, "https://ref.example/", "tester", "5",
		strconv.Itoa(os.Getpid()), "", "/p", "${unclosed"}
	for i, w := range want {
		switch i {
		case 0:
			if !regexp.MustCompile(`^\d{4}$`).MatchString(fields[i]) {
				t.Errorf("${time} = %q, want a year", fields[i])
			}
			continue
		case 1:
			if _, err := time.ParseDuration(fields[i]); err != nil {
				t.Errorf("${latency} = %q: %v", fields[i], err)
			}
			continue
		}
		if fields[i] != w {
			t.Errorf("field %d = %q, want %q", i, fields[i], w)
		}
	}
}

// TestLoggerDefaultFormat checks the line DefaultFormat lays out.
func TestLoggerDefaultFormat(t *testing.T) {
	out := &syncBuffer{}
	handler := logger.New(logger.Config{Output: out})(fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		_ = c.Respond(stdhttp.StatusAccepted, "text/plain", []byte("ok"))
	}))
	url := mwtest.Serve(t, handler)
	mwtest.Clients(t)[0].Do(t, "DELETE", url+"/thing", nil, "")
	got := waitLines(t, out, 1)
	re := regexp.MustCompile(`^\d\d:\d\d:\d\d \| 202 \| \S+ \| 127\.0\.0\.1 \| DELETE \| /thing$`)
	if len(got) != 1 || !re.MatchString(got[0]) {
		t.Errorf("logged %q", got)
	}
}
