package secretbuf

import (
	"encoding/base64"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithDecodedBase64ClearsEveryDecoderOutput(t *testing.T) {
	tests := []struct {
		name       string
		decodedLen int
		decodeErr  error
		use        func([]byte) error
		wantErr    error
	}{
		{name: "valid", decodedLen: 32, use: func([]byte) error { return nil }},
		{name: "invalid length", decodedLen: 31, use: func(value []byte) error {
			if len(value) != 32 {
				return errInvalidLength
			}
			return nil
		}, wantErr: errInvalidLength},
		{name: "corrupt base64 partial output", decodedLen: 7, decodeErr: base64.CorruptInputError(9), use: func([]byte) error {
			t.Fatal("consumer called after corrupt base64 decode")
			return nil
		}, wantErr: base64.CorruptInputError(9)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			owned := make([]byte, test.decodedLen)
			for index := range owned {
				owned[index] = byte(index + 1)
			}
			err := withDecodedBase64("content-is-never-reported", func(string) ([]byte, error) {
				return owned, test.decodeErr
			}, test.use)
			if test.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Equal(t, test.wantErr.Error(), err.Error())
			}
			assert.Equal(t, make([]byte, test.decodedLen), owned)
		})
	}
}

var errInvalidLength = errors.New("invalid decoded length")
