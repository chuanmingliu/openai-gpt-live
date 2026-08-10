package s2sws

import (
	"context"
	"testing"

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
