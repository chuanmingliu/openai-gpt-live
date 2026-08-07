package deepseek

import (
	"bufio"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/reasoner"
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

func TestReadSSERawAggregateBoundsChargeSkippedAndEmptySyntax(t *testing.T) {
	tests := map[string]struct {
		input   string
		records int
		wantEOF bool
	}{
		"comment only":  {input: ": comment\n", records: 1, wantEOF: true},
		"blank only":    {input: "\n", records: 1, wantEOF: true},
		"ignored field": {input: "event:\n", records: 1, wantEOF: true},
		"empty data":    {input: "data:\n\n", records: 2},
		"CRLF":          {input: ": comment\r\n\r\n", records: 2, wantEOF: true},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			exact := newSSEReaderWithBounds(bufio.NewReaderSize(strings.NewReader(test.input), 3), 1<<20, len(test.input), test.records)
			event, err := exact.Next()
			if test.wantEOF {
				assert.ErrorIs(t, err, io.EOF)
			} else {
				require.NoError(t, err)
				assert.Empty(t, event)
			}

			byteShort := newSSEReaderWithBounds(bufio.NewReaderSize(strings.NewReader(test.input), 3), 1<<20, len(test.input)-1, test.records)
			_, err = byteShort.Next()
			assert.ErrorIs(t, err, errStreamLimit)

			recordShort := newSSEReaderWithBounds(bufio.NewReaderSize(strings.NewReader(test.input), 3), 1<<20, len(test.input), test.records-1)
			_, err = recordShort.Next()
			assert.ErrorIs(t, err, errStreamLimit)
		})
	}
}

func TestReadSSERawBoundsStopOverlongIgnoredLineWithoutProportionalRetention(t *testing.T) {
	input := strings.Repeat("x", 1<<20) + ":\n"
	reader := newSSEReaderWithBounds(bufio.NewReaderSize(strings.NewReader(input), 16), 64, 64, 4)
	_, err := reader.Next()
	assert.ErrorIs(t, err, errStreamLimit)

	reader = newSSEReaderWithBounds(bufio.NewReaderSize(strings.NewReader(input), 16), 64, len(input), 4)
	_, err = reader.Next()
	assert.ErrorIs(t, err, errEventTooLarge)
}

func TestReadStreamRawRecordBoundsAndTerminalPrecedence(t *testing.T) {
	const overflow = "data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"xx\"}}]}\n\n"
	const blocked = "data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"content_filter\"}]}\n\n"
	const safe = "data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n"
	const providerFailure = "data: {\"error\":{\"code\":\"provider_failure\",\"message\":\"ignored\"}}\n\n"
	const done = "data: [DONE]\n\n"

	t.Run("normal exact raw bounds", func(t *testing.T) {
		raw := ": comment\r\n\r\nevent:\r\n\r\n" + safe + done
		client := boundedStreamTestClient(len(raw), 8)
		_, err := client.readStream(t.Context(), strings.NewReader(raw), func(reasoner.Chunk) error { return nil })
		require.NoError(t, err)
	})

	t.Run("late filter just before bounds", func(t *testing.T) {
		raw := overflow + ": comment\r\n\r\n" + blocked + done
		client := boundedStreamTestClient(len(raw), 8)
		result, err := client.readStream(t.Context(), strings.NewReader(raw), func(reasoner.Chunk) error { return nil })
		require.NoError(t, err)
		assert.True(t, result.ProviderBlocked)
	})

	t.Run("late filter after record bound", func(t *testing.T) {
		raw := overflow + ": comment\n\n" + blocked + done
		client := boundedStreamTestClient(len(raw), 4)
		_, err := client.readStream(t.Context(), strings.NewReader(raw), func(reasoner.Chunk) error { return nil })
		assertProviderCode(t, err, "stream_limit")
	})

	t.Run("late filter after byte bound", func(t *testing.T) {
		prefix := overflow + ": comment\n\n"
		raw := prefix + blocked + done
		client := boundedStreamTestClient(len(prefix), 16)
		_, err := client.readStream(t.Context(), strings.NewReader(raw), func(reasoner.Chunk) error { return nil })
		assertProviderCode(t, err, "stream_limit")
	})

	t.Run("provider error just before bounds", func(t *testing.T) {
		raw := ": comment\n\n" + providerFailure
		client := boundedStreamTestClient(len(raw), 4)
		_, err := client.readStream(t.Context(), strings.NewReader(raw), func(reasoner.Chunk) error { return nil })
		assertProviderCode(t, err, "provider_failure")
	})

	t.Run("provider error after bound", func(t *testing.T) {
		prefix := ": comment\n\n"
		raw := prefix + providerFailure
		client := boundedStreamTestClient(len(prefix), 4)
		_, err := client.readStream(t.Context(), strings.NewReader(raw), func(reasoner.Chunk) error { return nil })
		assertProviderCode(t, err, "stream_limit")
	})

	t.Run("safe overflow", func(t *testing.T) {
		raw := overflow + "\n\nevent:\n\n" + safe + done
		client := boundedStreamTestClient(len(raw), 10)
		_, err := client.readStream(t.Context(), strings.NewReader(raw), func(reasoner.Chunk) error { return nil })
		assertProviderCode(t, err, "output_limit")
	})
}

func TestReadStreamSkippedRecordExhaustionAndCancellation(t *testing.T) {
	const overflow = "data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"xx\"}}]}\n\n"
	for name, skipped := range map[string]string{
		"comments":       ": comment\n\n: second\n\n",
		"blanks":         "\n\n\n\n",
		"ignored fields": "event:\n\nid:\n\n",
	} {
		t.Run(name, func(t *testing.T) {
			raw := overflow + skipped
			client := boundedStreamTestClient(len(raw), 5)
			_, err := client.readStream(t.Context(), strings.NewReader(raw), func(reasoner.Chunk) error { return nil })
			assertProviderCode(t, err, "stream_limit")
		})
	}

	t.Run("empty data is charged then rejected", func(t *testing.T) {
		raw := overflow + "data:\n\n"
		client := boundedStreamTestClient(len(raw), 4)
		_, err := client.readStream(t.Context(), strings.NewReader(raw), func(reasoner.Chunk) error { return nil })
		assertProviderCode(t, err, "malformed_chunk")
	})

	t.Run("slow cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		reader := &cancelTailReader{ctx: ctx, data: []byte(overflow + ": comment\n\n"), delivered: make(chan struct{})}
		client := boundedStreamTestClient(1<<20, 100)
		done := make(chan error, 1)
		go func() {
			_, err := client.readStream(ctx, reader, func(reasoner.Chunk) error { return nil })
			done <- err
		}()
		<-reader.delivered
		cancel()
		assert.ErrorIs(t, <-done, context.Canceled)
	})

	t.Run("slow deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		reader := &cancelTailReader{ctx: ctx, data: []byte(overflow + ": comment\n\n"), delivered: make(chan struct{})}
		client := boundedStreamTestClient(1<<20, 100)
		done := make(chan error, 1)
		go func() {
			_, err := client.readStream(ctx, reader, func(reasoner.Chunk) error { return nil })
			done <- err
		}()
		<-reader.delivered
		assert.ErrorIs(t, <-done, context.DeadlineExceeded)
	})
}

func boundedStreamTestClient(rawBytes, records int) *Client {
	return &Client{config: Config{MaxOutputBytes: 1, MaxToolCalls: 4, MaxToolArgumentBytes: 64}, streamByteLimit: rawBytes, streamRecordLimit: records}
}

func assertProviderCode(t *testing.T, err error, code string) {
	t.Helper()
	var providerErr *reasoner.ProviderError
	require.ErrorAs(t, err, &providerErr)
	assert.Equal(t, code, providerErr.Code)
}

type cancelTailReader struct {
	ctx       context.Context
	data      []byte
	delivered chan struct{}
	once      sync.Once
}

func (r *cancelTailReader) Read(buffer []byte) (int, error) {
	if len(r.data) != 0 {
		count := copy(buffer, r.data)
		r.data = r.data[count:]
		if len(r.data) == 0 {
			r.once.Do(func() { close(r.delivered) })
		}
		return count, nil
	}
	<-r.ctx.Done()
	return 0, r.ctx.Err()
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
