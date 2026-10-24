package http

import "time"

// What an HTTP/2 client may make the server do for it without asking for
// anything, before the connection ends with GOAWAY and ENHANCE_YOUR_CALM.
// Each is a bucket of burst tokens, given back at rate a second.
//
// Resets count the streams a client resets while they are still open, and
// those the server resets for a stream error the client made: opening a
// stream and resetting it at once costs the server the stream and its
// handler but costs the client nothing, and the stream no longer counts
// against MaxConcurrentStreams (Rapid Reset, CVE-2023-44487). Glitches count
// the frames that ask for work and carry no request: PING and SETTINGS,
// each of which the server acknowledges, and DATA and CONTINUATION frames
// that are empty without ending anything (CVE-2019-9512, CVE-2019-9515,
// CVE-2019-9518, CVE-2024-27316). Frames that cost no more than their bytes
// to read, such as PRIORITY, or a WINDOW_UPDATE or RST_STREAM for a stream
// that has ended, which a client may send in flight, are not counted.
const (
	h2ResetBurst  = 1000
	h2ResetRate   = 100
	h2GlitchBurst = 1000
	h2GlitchRate  = 100
	// h2MaxContinuations bounds the CONTINUATION frames one header block may
	// arrive in, whatever their size.
	h2MaxContinuations = 1000
)

// h2Budget is one of a connection's flood buckets. Only its reader touches
// it.
type h2Budget struct {
	tokens float64
	last   time.Time
}

// take spends a token, refilling the bucket for the time since the last
// one, and reports false once there is none left.
func (b *h2Budget) take(burst, rate float64) bool {
	now := time.Now()
	if b.last.IsZero() {
		b.tokens = burst
	} else {
		b.tokens = min(burst, b.tokens+now.Sub(b.last).Seconds()*rate)
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// glitch counts a frame that asked for work and carried no request, and
// reports the connection error to end it with once there have been too many.
func (sc *h2ServerConn) glitch() error {
	if sc.glitches.take(h2GlitchBurst, h2GlitchRate) {
		return nil
	}
	return h2ConnErr(H2EnhanceYourCalm, "too many control frames")
}

// resetCounted counts a stream reset while it was open, and reports the
// connection error to end it with once there have been too many.
func (sc *h2ServerConn) resetCounted() error {
	if sc.resets.take(h2ResetBurst, h2ResetRate) {
		return nil
	}
	return h2ConnErr(H2EnhanceYourCalm, "too many streams reset")
}

// armHeaderTimer bounds how long a header block may take to arrive in
// CONTINUATION frames by the header timeout, as an HTTP/1 header is: a
// block that has not ended by then ends the connection.
func (sc *h2ServerConn) armHeaderTimer() {
	d := sc.handler.headerTimeout()
	if d <= 0 {
		return
	}
	gen := sc.contGen.Load()
	sc.contTimer = time.AfterFunc(d, func() {
		if sc.contGen.Load() == gen {
			sc.fail(h2ConnErr(H2EnhanceYourCalm, "header block took too long"))
		}
	})
}

// stopHeaderTimer is a header block's end: the timer armHeaderTimer set for
// it no longer applies, even when it has fired already.
func (sc *h2ServerConn) stopHeaderTimer() {
	sc.contGen.Add(1)
	if sc.contTimer != nil {
		sc.contTimer.Stop()
		sc.contTimer = nil
	}
}
