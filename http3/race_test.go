//go:build race

package http3

// raceEnabled says the race detector is on, which slows large transfers
// several times over.
const raceEnabled = true
