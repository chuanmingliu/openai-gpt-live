package s2sws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAudioWaitsForMatchingProviderSessionUpdated(t *testing.T) {
	var writes atomic.Int32
	session := newCommitAckTestSession(t, time.Second, func(context.Context, websocket.MessageType, []byte) error {
		writes.Add(1)
		return nil
	})
	resetInputTurnModeForTest(session)

	require.NoError(t, session.Send(t.Context(), realtime.Command{
		Kind:    realtime.CommandSessionUpdate,
		Session: realtime.SessionConfig{TurnDetection: json.RawMessage(`null`)},
	}))
	require.Equal(t, int32(1), writes.Load())

	err := session.Send(t.Context(), realtime.Command{Kind: realtime.CommandAppendAudio, Audio: []byte{1, 2}})
	require.Error(t, err, "audio must not reach the wire before provider confirmation")
	assert.Equal(t, int32(1), writes.Load())

	sendCommitAckTestRead(t, session, `{"type":"session.updated","event_id":"mode-confirmed","session":{"type":"realtime","audio":{"input":{"turn_detection":null}}}}`)
	event, receiveErr := session.Receive(t.Context())
	require.NoError(t, receiveErr)
	assert.Equal(t, realtime.EventSessionUpdated, event.Kind)

	require.NoError(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandAppendAudio, Audio: []byte{1, 2}}))
	assert.Equal(t, int32(2), writes.Load())
}

func TestSessionUpdatedCarriesOutstandingLocalSequence(t *testing.T) {
	session := newCommitAckTestSession(t, time.Second, func(context.Context, websocket.MessageType, []byte) error { return nil })
	resetInputTurnModeForTest(session)

	require.NoError(t, session.Send(t.Context(), realtime.Command{
		Kind: realtime.CommandSessionUpdate, Sequence: 73,
		Session: realtime.SessionConfig{TurnDetection: json.RawMessage(`null`)},
	}))
	sendCommitAckTestRead(t, session, `{"type":"session.updated","event_id":"mode-confirmed","session":{"type":"realtime","audio":{"input":{"turn_detection":null}}}}`)
	event, err := session.Receive(t.Context())
	require.NoError(t, err)
	require.Equal(t, realtime.EventSessionUpdated, event.Kind)
	require.True(t, event.SessionUpdateAcknowledgement)
	require.Equal(t, uint64(73), event.SessionUpdateSequence)
}

func TestUnsolicitedSessionUpdatedIsNotMarkedAsAcknowledgement(t *testing.T) {
	session := newCommitAckTestSession(t, time.Second, func(context.Context, websocket.MessageType, []byte) error { return nil })
	resetInputTurnModeForTest(session)

	sendCommitAckTestRead(t, session, `{"type":"session.updated","event_id":"unsolicited","session":{"type":"realtime","audio":{"input":{"turn_detection":null}}}}`)
	event, err := session.Receive(t.Context())
	require.NoError(t, err)
	require.Equal(t, realtime.EventSessionUpdated, event.Kind)
	require.False(t, event.SessionUpdateAcknowledgement)
	require.Zero(t, event.SessionUpdateSequence)
}

func TestInputTurnModeConfirmationFailsClosed(t *testing.T) {
	tests := []struct {
		name      string
		provider  string
		wantError string
		immutable bool
	}{
		{
			name:      "missing turn detection",
			provider:  `{"type":"session.updated","event_id":"missing","session":{"type":"realtime","audio":{"input":{}}}}`,
			wantError: "omitted input turn mode",
		},
		{
			name:      "mismatched turn detection",
			provider:  `{"type":"session.updated","event_id":"mismatch","session":{"type":"realtime","audio":{"input":{"turn_detection":{"type":"server_vad"}}}}}`,
			wantError: realtime.ErrInputTurnModeImmutable.Error(),
			immutable: true,
		},
		{
			name:      "unknown turn detection",
			provider:  `{"type":"session.updated","event_id":"unknown","session":{"type":"realtime","audio":{"input":{"turn_detection":{"type":"future_vad"}}}}}`,
			wantError: `unsupported turn detection type "future_vad"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			session := newCommitAckTestSession(t, time.Second, func(context.Context, websocket.MessageType, []byte) error { return nil })
			resetInputTurnModeForTest(session)
			require.NoError(t, session.Send(t.Context(), realtime.Command{
				Kind:    realtime.CommandSessionUpdate,
				Session: realtime.SessionConfig{TurnDetection: json.RawMessage(`null`)},
			}))
			sendCommitAckTestRead(t, session, test.provider)
			_, err := session.Receive(t.Context())
			require.ErrorContains(t, err, test.wantError)
			if test.immutable {
				require.True(t, errors.Is(err, realtime.ErrInputTurnModeImmutable))
			}
		})
	}
}

func TestSessionUpdatesSerializeBeforeInitialModeNegotiation(t *testing.T) {
	var writes atomic.Int32
	session := newCommitAckTestSession(t, time.Second, func(context.Context, websocket.MessageType, []byte) error {
		writes.Add(1)
		return nil
	})
	resetInputTurnModeForTest(session)

	require.NoError(t, session.Send(t.Context(), realtime.Command{
		Kind:    realtime.CommandSessionUpdate,
		Session: realtime.SessionConfig{Instructions: "reserved tools only"},
	}))
	require.Equal(t, int32(1), writes.Load())

	second := make(chan error, 1)
	go func() {
		second <- session.Send(t.Context(), realtime.Command{
			Kind:     realtime.CommandSessionUpdate,
			Sequence: 41,
			Session:  realtime.SessionConfig{TurnDetection: json.RawMessage(`null`)},
		})
	}()
	select {
	case err := <-second:
		t.Fatalf("second update bypassed the uncorrelated provider acknowledgement: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	require.Equal(t, int32(1), writes.Load())

	sendCommitAckTestRead(t, session, `{"type":"session.updated","event_id":"first-ack","session":{"type":"realtime"}}`)
	firstAck, err := session.Receive(t.Context())
	require.NoError(t, err)
	require.Equal(t, realtime.EventSessionUpdated, firstAck.Kind)
	require.Zero(t, firstAck.SessionUpdateSequence)
	require.NoError(t, <-second)
	require.Equal(t, int32(2), writes.Load())

	sendCommitAckTestRead(t, session, `{"type":"session.updated","event_id":"mode-ack","session":{"type":"realtime","audio":{"input":{"turn_detection":null}}}}`)
	modeAck, err := session.Receive(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint64(41), modeAck.SessionUpdateSequence)
	require.NoError(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandAppendAudio, Audio: []byte{1, 2}}))
	require.Equal(t, int32(3), writes.Load())
}

func TestProviderVADNeverWritesManualCommit(t *testing.T) {
	for _, vadType := range []string{"server_vad", "semantic_vad"} {
		t.Run(vadType, func(t *testing.T) {
			var writes atomic.Int32
			session := newCommitAckTestSession(t, time.Second, func(context.Context, websocket.MessageType, []byte) error {
				writes.Add(1)
				return nil
			})
			resetInputTurnModeForTest(session)
			turnDetection := json.RawMessage(`{"type":"` + vadType + `"}`)
			require.NoError(t, session.Send(t.Context(), realtime.Command{
				Kind:    realtime.CommandSessionUpdate,
				Session: realtime.SessionConfig{TurnDetection: turnDetection},
			}))
			require.Equal(t, int32(1), writes.Load())
			sendCommitAckTestRead(t, session, `{"type":"session.updated","event_id":"mode-confirmed","session":{"type":"realtime","audio":{"input":{"turn_detection":{"type":"`+vadType+`"}}}}}`)
			event, receiveErr := session.Receive(t.Context())
			require.NoError(t, receiveErr)
			assert.Equal(t, realtime.EventSessionUpdated, event.Kind)

			err := session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCommitAudio, Sequence: 1})
			require.Error(t, err)
			assert.Equal(t, int32(1), writes.Load())
		})
	}
}

func TestProviderVADAcknowledgementsDoNotConsumeManualHistory(t *testing.T) {
	session := newCommitAckTestSession(t, time.Second, func(context.Context, websocket.MessageType, []byte) error { return nil })
	session.inputTurnMu.Lock()
	session.inputTurnConfirmed = realtime.InputTurnModeProviderVAD
	session.inputTurnMu.Unlock()

	for index := 0; index <= audioCommitAckHistoryItems; index++ {
		itemID := fmt.Sprintf("provider-vad-%04d", index)
		sendCommitAckTestRead(t, session, `{"type":"input_audio_buffer.committed","event_id":"ack-`+itemID+`","item_id":"`+itemID+`","previous_item_id":null}`)
		event, err := session.Receive(t.Context())
		require.NoError(t, err)
		require.Equal(t, realtime.EventInputAudioCommitted, event.Kind)
		require.Equal(t, itemID, event.ItemID)
		require.Zero(t, event.InputAudioCommitSequence)
	}
}
