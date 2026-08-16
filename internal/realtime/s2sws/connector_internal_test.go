package s2sws

import (
	"context"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/stretchr/testify/require"
)

func TestBindImplicitZeroesAndNilsDrainedOwnedBuffers(t *testing.T) {
	raw := []byte("private-raw-payload")
	decoded := []byte("decoded-owned-bytes")
	session := &voiceSession{
		ctx: context.Background(), events: make(chan realtime.Event, 1), done: make(chan struct{}),
		responses: map[string]*responseCorrelation{
			"response": {
				unbound: true,
				buffered: []bufferedResponseEvent{{
					event:    realtime.Event{Kind: realtime.EventAudioDelta, ResponseID: "response", Payload: raw, Audio: decoded},
					rawBytes: len(raw), decodedBytes: len(decoded), ownedBytes: len(raw) + len(decoded),
				}},
				bufferedBytes: len(raw),
			},
		},
		implicitUnbound: 1, implicitBufferedEvents: 1, implicitBufferedBytes: len(raw) + len(decoded),
	}
	require.NoError(t, session.bindImplicit(correlationRequest{ctx: t.Context(), responseID: "response", epoch: identity.ResponseEpoch(7)}))
	correlation := session.responses["response"]
	require.Nil(t, correlation.buffered)
	require.Zero(t, correlation.bufferedBytes)
	require.Zero(t, session.implicitUnbound)
	require.Zero(t, session.implicitBufferedEvents)
	require.Zero(t, session.implicitBufferedBytes)
	for _, value := range raw {
		require.Zero(t, value)
	}
	for _, value := range decoded {
		require.Zero(t, value)
	}
}

func TestOrphanMediaBeforeBindIsCountedAndDropped(t *testing.T) {
	session := &voiceSession{
		ctx: context.Background(), events: make(chan realtime.Event, 1), done: make(chan struct{}),
		responses: map[string]*responseCorrelation{},
	}
	require.NoError(t, session.handleMapped(mappedServerEvent{
		responseSpecific: true,
		event: realtime.Event{
			Kind: realtime.EventAudioDelta, ResponseID: "unbound", ItemID: "item",
			Audio: []byte{0, 1},
		},
	}))
	require.Equal(t, 1, session.orphanMediaDropped)
	select {
	case <-session.events:
		t.Fatal("orphan media was delivered")
	default:
	}
}

func TestDeliverFailsClosedWhenReceiveQueueIsFull(t *testing.T) {
	session := &voiceSession{
		ctx: context.Background(), events: make(chan realtime.Event, 1), done: make(chan struct{}),
	}
	require.NoError(t, session.deliver(context.Background(), realtime.Event{Kind: realtime.EventSpeechStarted}))
	err := session.deliver(context.Background(), realtime.Event{Kind: realtime.EventSpeechStopped})
	require.ErrorIs(t, err, ErrReceiveQueueFull)
}

func TestHandleMappedDoesNotBlockWhenReceiveQueueIsFull(t *testing.T) {
	session := &voiceSession{
		ctx: context.Background(), events: make(chan realtime.Event, 1), done: make(chan struct{}),
	}
	require.NoError(t, session.deliver(context.Background(), realtime.Event{Kind: realtime.EventSpeechStarted}))
	done := make(chan error, 1)
	go func() {
		done <- session.handleMapped(mappedServerEvent{event: realtime.Event{Kind: realtime.EventSpeechStopped}})
	}()
	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrReceiveQueueFull)
	case <-time.After(time.Second):
		t.Fatal("mapping deliver blocked on a full receive queue")
	}
}
