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

// Unsent is Pending read under the connection's lock, for a caller that
// wants the exact figure rather than one to pace by: it never counts bytes a
// write in progress has already handed the socket, which the peer may have
// read, and counts those a read round has queued without counting them yet.
// It is set and nil where Wait is.
var Unsent func(c any) int64
