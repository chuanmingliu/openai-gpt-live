// Package secretbuf provides narrow ownership helpers for temporary secret buffers.
package secretbuf

import "encoding/base64"

// WithDecodedBase64 decodes encoded, gives the temporary buffer to use, and
// clears the buffer on every return. Decode errors, including errors that
// return a non-empty decoded prefix, are returned without calling use.
func WithDecodedBase64(encoded string, use func([]byte) error) error {
	return withDecodedBase64(encoded, base64.StdEncoding.DecodeString, use)
}

func withDecodedBase64(encoded string, decode func(string) ([]byte, error), use func([]byte) error) error {
	decoded, err := decode(encoded)
	defer clear(decoded)
	if err != nil {
		return err
	}
	return use(decoded)
}
