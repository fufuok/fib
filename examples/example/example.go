// Package example holds the plumbing every example shares, so that each one
// shows only its own protocol.
package example

import (
	"context"
	"fmt"
	"io"
	"os"

	fib "github.com/lesismal/fib"
)

// Start runs engine on a goroutine of its own and returns a function that
// stops and closes it, and reports what Run returned.
func Start(engine *fib.Engine) (stop func() error) {
	running := make(chan error, 1)
	go func() { running <- engine.Run() }()
	return func() error {
		engine.Stop()
		err := <-running
		_ = engine.Close()
		return err
	}
}

// Serve runs engines until ctx ends, then stops and closes them, the last
// first. Once they are running it prints banner to out, with the address the
// first one listens on for its %s, and tells ready, if not nil, that address.
func Serve(ctx context.Context, out io.Writer, ready func(addr string), banner string, engines ...*fib.Engine) error {
	stops := make([]func() error, len(engines))
	for i, engine := range engines {
		stops[i] = Start(engine)
	}
	addr := Addr(engines[0])
	fmt.Fprintf(out, banner+"\n", addr)
	if ready != nil {
		ready(addr)
	}
	<-ctx.Done()
	var err error
	for i := len(stops) - 1; i >= 0; i-- {
		if stopErr := stops[i](); err == nil {
			err = stopErr
		}
	}
	return err
}

// Addr is the address engine listens on: its first TCP listener's, or else
// its first UDP one's.
func Addr(engine *fib.Engine) string {
	if addr, err := engine.LocalAddr(); err == nil {
		return addr.String()
	}
	addr, _ := engine.LocalUDPAddr()
	return addr.String()
}

// ClientEngine starts an engine with no listener, for dialing, and returns a
// function that stops and closes it.
func ClientEngine() (*fib.Engine, func()) {
	engine, err := fib.New(fib.DefaultConfig())
	if err != nil {
		Fatal(err)
	}
	stop := Start(engine)
	return engine, func() { _ = stop() }
}

// Message is the n-th message a client sends.
func Message(n int) string { return fmt.Sprintf("hello from fib #%d", n) }

// Fatal reports err and exits.
func Fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
