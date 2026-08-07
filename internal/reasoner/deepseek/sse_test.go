package deepseek

import (
	"bufio"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadSSEHandlesCommentsCRLFBlankAndMultilineData(t *testing.T) {
	input := ": comment\r\n\r\ndata: {\"a\":\r\ndata: 1}\r\n\r\n\r\ndata: [DONE]\r\n\r\n"
	reader := newSSEReader(bufio.NewReaderSize(strings.NewReader(input), 3), 1<<20)
	event, err := reader.Next()
	require.NoError(t, err)
	assert.Equal(t, []byte("{\"a\":\n1}"), event)
	event, err = reader.Next()
	require.NoError(t, err)
	assert.Equal(t, []byte("[DONE]"), event)
}

func TestReadSSEEnforcesExactEventBoundary(t *testing.T) {
	for _, test := range []struct {
		name    string
		payload string
		wantErr bool
	}{
		{name: "exact", payload: strings.Repeat("x", maxSSEEventBytes)},
		{name: "first byte over", payload: strings.Repeat("x", maxSSEEventBytes+1), wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			reader := newSSEReader(bufio.NewReaderSize(strings.NewReader("data:"+test.payload+"\n\n"), 1), maxSSEEventBytes)
			_, err := reader.Next()
			if test.wantErr {
				assert.ErrorIs(t, err, errEventTooLarge)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestReadSSERejectsContentInNonDataFieldsAndTruncation(t *testing.T) {
	reader := newSSEReader(bufio.NewReader(strings.NewReader("event: secret\n\n")), 100)
	_, err := reader.Next()
	assert.ErrorIs(t, err, errUnsupportedSSEField)

	reader = newSSEReader(bufio.NewReader(strings.NewReader("data: partial")), 100)
	_, err = reader.Next()
	assert.True(t, errors.Is(err, io.ErrUnexpectedEOF))
}
