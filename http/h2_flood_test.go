//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"testing"
	"time"
)

// h2floodWrite writes b, ignoring an error: the server may end the
// connection before it has all of it.
func h2floodWrite(tc *h2TestConn, b []byte) {
	_, _ = tc.c.Write(b)
}

// h2floodResets is n streams opened, each waiting for its body, and reset
// at once.
func h2floodResets(tc *h2TestConn, first uint32, n int) []byte {
	var out []byte
	for i := 0; i < n; i++ {
		id := first + uint32(2*i)
		block := tc.enc.Begin(nil)
		for _, f := range [][2]string{{":method", "POST"}, {":scheme", "http"}, {":authority", "test"}, {":path", "/"}} {
			block = tc.enc.AppendField(block, f[0], f[1], false)
		}
		out = h2AppendHeaderBlock(out, id, block, false, 16384)
		out = append(h2AppendFrameHeader(out, h2FrameRSTStream, 0, id, 4), 0, 0, 0, byte(H2Cancel))
	}
	return out
}

// TestH2ServerEndsRapidReset checks that a client opening streams and
// resetting them at once, past the burst the server allows, has its
// connection ended with ENHANCE_YOUR_CALM (CVE-2023-44487).
func TestH2ServerEndsRapidReset(t *testing.T) {
	addr := serve(t, NewHandlerWithConfig(DefaultConfig(), echoHandler()))
	tc := dialH2(t, addr)
	h2floodWrite(tc, h2floodResets(tc, 1, h2ResetBurst+100))
	if code := h2covGoAway(t, tc); code != H2EnhanceYourCalm {
		t.Fatalf("GOAWAY %v, want ENHANCE_YOUR_CALM", code)
	}
}

// TestH2ServerAllowsResetsUnderTheBurst checks that resets within the burst
// leave the connection serving.
func TestH2ServerAllowsResetsUnderTheBurst(t *testing.T) {
	addr := serve(t, NewHandlerWithConfig(DefaultConfig(), echoHandler()))
	tc := dialH2(t, addr)
	n := h2ResetBurst / 2
	tc.write(h2floodResets(tc, 1, n))
	id := uint32(1 + 2*n)
	tc.headers(id, true, get("/after")...)
	if header, _ := tc.response(id); header[":status"] != "200" {
		t.Fatalf("status %q after %d resets", header[":status"], n)
	}
}

// TestH2ServerEndsControlFrameFloods checks that PING, SETTINGS, empty DATA
// and empty CONTINUATION frames past the burst end the connection with
// ENHANCE_YOUR_CALM.
func TestH2ServerEndsControlFrameFloods(t *testing.T) {
	addr := serve(t, NewHandlerWithConfig(DefaultConfig(), echoHandler()))
	for _, tt := range []struct {
		name  string
		flood func(tc *h2TestConn) []byte
	}{
		{"PING", func(tc *h2TestConn) []byte {
			var out []byte
			for i := 0; i < h2GlitchBurst+100; i++ {
				out = append(h2AppendFrameHeader(out, h2FramePing, 0, 0, 8), "flooding"...)
			}
			return out
		}},
		{"SETTINGS", func(tc *h2TestConn) []byte {
			var out []byte
			for i := 0; i < h2GlitchBurst+100; i++ {
				out = h2AppendSettings(out)
			}
			return out
		}},
		{"empty DATA", func(tc *h2TestConn) []byte {
			block := tc.enc.Begin(nil)
			for _, f := range [][2]string{{":method", "POST"}, {":scheme", "http"}, {":authority", "test"}, {":path", "/"}} {
				block = tc.enc.AppendField(block, f[0], f[1], false)
			}
			out := h2AppendHeaderBlock(nil, 1, block, false, 16384)
			for i := 0; i < h2GlitchBurst+100; i++ {
				out = h2AppendFrameHeader(out, h2FrameData, 0, 1, 0)
			}
			return out
		}},
		{"empty CONTINUATION", func(tc *h2TestConn) []byte {
			out := h2AppendFrameHeader(nil, h2FrameHeaders, 0, 1, 0)
			for i := 0; i < h2GlitchBurst+100; i++ {
				out = h2AppendFrameHeader(out, h2FrameContinuation, 0, 1, 0)
			}
			return out
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tc := dialH2(t, addr)
			h2floodWrite(tc, tt.flood(tc))
			if code := h2covGoAway(t, tc); code != H2EnhanceYourCalm {
				t.Fatalf("GOAWAY %v, want ENHANCE_YOUR_CALM", code)
			}
		})
	}
}

// TestH2ServerBoundsContinuationFrames checks that a header block arriving
// in more CONTINUATION frames than the server allows, each carrying a byte,
// ends the connection though it stays under MaxHeaderBytes.
func TestH2ServerBoundsContinuationFrames(t *testing.T) {
	config := DefaultConfig()
	config.MaxHeaderBytes = 1 << 20
	addr := serve(t, NewHandlerWithConfig(config, echoHandler()))
	tc := dialH2(t, addr)
	out := append(h2AppendFrameHeader(nil, h2FrameHeaders, 0, 1, 1), 0)
	for i := 0; i < h2MaxContinuations+10; i++ {
		out = append(h2AppendFrameHeader(out, h2FrameContinuation, 0, 1, 1), 0)
	}
	h2floodWrite(tc, out)
	if code := h2covGoAway(t, tc); code != H2EnhanceYourCalm {
		t.Fatalf("GOAWAY %v, want ENHANCE_YOUR_CALM", code)
	}
}

// TestH2ServerBoundsASlowHeaderBlock checks that a header block whose
// CONTINUATION frames do not finish it within ReadHeaderTimeout ends the
// connection, though frames keep arriving.
func TestH2ServerBoundsASlowHeaderBlock(t *testing.T) {
	config := DefaultConfig()
	config.ReadHeaderTimeout = 200 * time.Millisecond
	addr := serve(t, NewHandlerWithConfig(config, echoHandler()))
	tc := dialH2(t, addr)
	tc.write(append(h2AppendFrameHeader(nil, h2FrameHeaders, 0, 1, 1), 0))
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(50 * time.Millisecond):
				h2floodWrite(tc, append(h2AppendFrameHeader(nil, h2FrameContinuation, 0, 1, 1), 0))
			}
		}
	}()
	start := time.Now()
	if code := h2covGoAway(t, tc); code != H2EnhanceYourCalm {
		t.Fatalf("GOAWAY %v, want ENHANCE_YOUR_CALM", code)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("GOAWAY after %v", elapsed)
	}
}
