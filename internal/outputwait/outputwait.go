// Package outputwait lets the packages of this module wait for a fib
// connection's queued output to drain, which package fib does not offer
// outside the module.
package outputwait

// Wait waits while more than limit bytes of what c, a *fib.Connection, was
// sent are still queued for its socket, until no more than half of limit
// are, and reports an error instead once the connection has closed or a
// write to it has failed. Package fib sets it when it is initialized; it is
// nil on a platform fib serves through the portable backend.
var Wait func(c any, limit int64) error

// Pending reports how many bytes of what c, a *fib.Connection, was sent are
// still queued for its socket, without waiting. It is set and nil where Wait
// is.
var Pending func(c any) int64
