// Package testkit contains deterministic fixtures shared by package tests.
package testkit

// RampPCM returns byteCount bytes of deterministic PCM16LE data. byteCount may
// be odd so callers can exercise sample-boundary handling.
func RampPCM(byteCount int) []byte {
	pcm := make([]byte, byteCount)
	for i := range pcm {
		pcm[i] = byte((i*29 + 7) % 251)
	}
	return pcm
}
