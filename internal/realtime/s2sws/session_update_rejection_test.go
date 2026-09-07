package s2sws_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Exact removal must work for the head, middle, and initialization slot;
// malformed or duplicate identities must never consume a different update.
func TestSessionUpdateRejectionPreservesOtherPendingUpdates(t *testing.T) {
	for _, rejected := range []uint64{0, 41, 42} {
		t.Run(fmt.Sprint(rejected), func(t *testing.T) {
			server := newTestVoiceServer(t)
			voice := connectTestSession(t, server.URL, nil, realtime.OpenRequest{Model: "model"})
			for _, seq := range []uint64{0, 41, 42} {
				require.NoError(t, voice.Send(t.Context(), realtime.Command{Kind: realtime.CommandSessionUpdate, Sequence: seq}))
				var frame struct {
					EventID string `json:"event_id"`
				}
				require.NoError(t, json.Unmarshal(server.nextClientFrame(t), &frame))
				assert.Equal(t, fmt.Sprintf("live-session-update-%d", seq), frame.EventID)
			}
			for _, eventID := range []string{"", "unknown", "live-session-update-99", "live-session-update-041", "live-session-update-+41", "live-session-update--1", "live-session-update-18446744073709551616", "live-response-create-41", "live-response-cancel-42"} {
				server.sendEvent(t, map[string]any{"type": "error", "error": map[string]any{"event_id": eventID, "message": "private provider detail"}})
				event := receiveEvent(t, voice)
				assert.Equal(t, realtime.EventError, event.Kind)
				assert.False(t, event.SessionUpdateRejected)
				assert.Zero(t, event.SessionUpdateSequence)
				assert.False(t, event.SessionUpdateAcknowledgement)
			}
			errorWire := map[string]any{"type": "error", "error": map[string]any{"event_id": fmt.Sprintf("live-session-update-%d", rejected), "message": "private provider detail"}}
			server.sendEvent(t, errorWire)
			event := receiveEvent(t, voice)
			assert.True(t, event.SessionUpdateRejected)
			assert.Equal(t, rejected, event.SessionUpdateSequence)
			assert.Equal(t, "Realtime provider request failed.", event.Reason)
			assert.Empty(t, event.Payload)
			server.sendEvent(t, errorWire)
			duplicate := receiveEvent(t, voice)
			assert.False(t, duplicate.SessionUpdateRejected)
			assert.Zero(t, duplicate.SessionUpdateSequence)
			for _, seq := range []uint64{0, 41, 42} {
				if seq == rejected {
					continue
				}
				ack := sendAndReceiveEvent(t, server, voice, map[string]any{"type": "session.updated"})
				assert.True(t, ack.SessionUpdateAcknowledgement)
				assert.Equal(t, seq, ack.SessionUpdateSequence)
			}
			unsolicited := sendAndReceiveEvent(t, server, voice, map[string]any{"type": "session.updated"})
			assert.False(t, unsolicited.SessionUpdateAcknowledgement)
		})
	}
}
