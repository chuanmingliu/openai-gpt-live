package s2sws

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A response is mapped before writeMessage returns, so registration after
// writing would deterministically lose correlation even when Send succeeds.
func TestSessionUpdateImmediateResponseBeforeWriteReturns(t *testing.T) {
	for _, reject := range []bool{false, true} {
		for _, sequence := range []uint64{0, ^uint64(0)} {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			var session *voiceSession
			var observed realtime.Event
			var eventID string
			var writerDone <-chan struct{}
			session, writerDone = newUnitWriteSession(t, func(ctx context.Context, _ websocket.MessageType, raw []byte) error {
				var command struct {
					EventID string `json:"event_id"`
				}
				if err := json.Unmarshal(raw, &command); err != nil {
					return err
				}
				eventID = command.EventID
				response := []byte(`{"type":"session.updated"}`)
				if reject {
					response = []byte(`{"type":"error","error":{"event_id":"` + eventID + `","message":"private detail"}}`)
				}
				select {
				case session.reads <- socketRead{messageType: websocket.MessageText, raw: response}:
				case <-ctx.Done():
					return ctx.Err()
				}
				var err error
				observed, err = session.Receive(ctx)
				return err
			}, func(websocket.StatusCode, string) error { return nil })
			session.correlations = make(chan correlationRequest)
			session.reads = make(chan socketRead)
			session.events = make(chan realtime.Event, 1)
			mapperDone := make(chan struct{})
			go func() { defer close(mapperDone); session.mappingLoop() }()
			err := session.Send(ctx, realtime.Command{Kind: realtime.CommandSessionUpdate, Sequence: sequence})
			assert.NoError(t, session.Close(context.Background()))
			<-writerDone
			<-mapperDone
			cancel()
			require.NoError(t, err)
			assert.NotEmpty(t, eventID)
			assert.LessOrEqual(t, len(eventID), 40)
			assert.Equal(t, sequence, observed.SessionUpdateSequence)
			assert.Equal(t, reject, observed.SessionUpdateRejected)
			assert.Equal(t, !reject, observed.SessionUpdateAcknowledgement)
		}
	}
}
