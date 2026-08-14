package s2sws

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/coder/websocket"
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

// TestMappingLoopServicesCorrelationWhileEventDeliveryIsBackpressured catches
// a direct blocking send to events from the sole correlation owner.
func TestMappingLoopServicesCorrelationWhileEventDeliveryIsBackpressured(t *testing.T) {
	const count = 4
	ctx, cancel := context.WithCancel(context.Background())
	session := &voiceSession{
		ctx: ctx, cancel: cancel,
		reads: make(chan socketRead, count), correlations: make(chan correlationRequest),
		events: make(chan realtime.Event, 1), closing: make(chan struct{}), done: make(chan struct{}),
		responses: make(map[string]*responseCorrelation), pending: make(map[identity.ResponseEpoch]struct{}),
		maxFrameBytes: 1 << 20, closeNow: func() error { return nil },
	}
	go session.mappingLoop()
	t.Cleanup(cancel)

	for index := 1; index <= count; index++ {
		raw := []byte(fmt.Sprintf(`{"type":"input_audio_buffer.speech_started","event_id":"event-%d","item_id":"item-%d","audio_start_ms":%d}`, index, index, index))
		select {
		case session.reads <- socketRead{messageType: websocket.MessageText, raw: raw}:
		case <-time.After(time.Second):
			t.Fatalf("mapped event %d was not accepted", index)
		}
		if index == 1 {
			require.Eventually(t, func() bool { return len(session.events) == 1 }, time.Second, time.Millisecond)
		}
	}
	// Give the current implementation time to enter its second direct event
	// send. The bounded implementation instead owns the accepted tail locally.
	time.Sleep(20 * time.Millisecond)

	correlated := make(chan error, 1)
	go func() {
		correlated <- session.correlate(context.Background(), correlationRequest{kind: correlationRegister, epoch: 7})
	}()
	select {
	case err := <-correlated:
		require.NoError(t, err)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("correlation did not progress while public event delivery was backpressured")
	}

	for index := 1; index <= count; index++ {
		select {
		case event := <-session.events:
			require.Equal(t, fmt.Sprintf("event-%d", index), event.ProviderEventID)
		case <-time.After(time.Second):
			t.Fatalf("mapped event %d was silently dropped", index)
		}
	}
}
