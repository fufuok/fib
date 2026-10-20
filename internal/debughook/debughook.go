// Package debughook lets tests in this module read a fib connection's
// internal I/O state. Temporary: it is here to diagnose a Windows CI hang.
package debughook

// ConnState describes the state of c, a *fib.Connection, for a test to
// report. Package fib sets it.
var ConnState func(c any) string

// SetTrace turns on, or off, the recording of each connection's last I/O
// steps, and ConnTrace returns those of c, a *fib.Connection. Package fib
// sets them.
var (
	SetTrace  func(on bool)
	ConnTrace func(c any) string
)
