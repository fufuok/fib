// Package debughook lets tests in this module read a fib connection's
// internal I/O state. Temporary: it is here to diagnose a Windows CI hang.
package debughook

// ConnState describes the state of c, a *fib.Connection, for a test to
// report. Package fib sets it.
var ConnState func(c any) string
