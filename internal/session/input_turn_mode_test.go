package session

import (
	"encoding/json"
	"testing"

	"github.com/chuanmingliu/openai-gpt-live/internal/protocol"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func inputTurnSessionUpdate(turnDetection json.RawMessage) protocol.SessionUpdateEvent {
	return protocol.SessionUpdateEvent{
		Type: "session.update",
		Session: protocol.SessionUpdate{
			Type: "realtime",
			Audio: &protocol.SessionAudio{Input: &protocol.SessionAudioInput{
				TurnDetection: turnDetection,
			}},
		},
	}
}

func providerSessionUpdated(turnDetection string, sequence ...uint64) realtime.Event {
	event := realtime.Event{
		Kind:            realtime.EventSessionUpdated,
		ProviderEventID: "provider-session-updated",
		Payload:         json.RawMessage(`{"type":"session.updated","event_id":"provider-session-updated","session":{"type":"realtime","audio":{"input":{"turn_detection":` + turnDetection + `}}}}`),
	}
	if len(sequence) != 0 {
		event.SessionUpdateAcknowledgement = true
		event.SessionUpdateSequence = sequence[0]
	}
	return event
}

func TestInputTurnModeRequiresActualProviderSessionUpdated(t *testing.T) {
	reactor, _ := unconfiguredTestReactor(1)
	effects := applyOK(t, reactor, ClientInput{Event: inputTurnSessionUpdate(json.RawMessage(`null`))})
	require.Len(t, effects, 1)
	command := effects[0].VoiceCommand
	require.NotNil(t, command)

	beforeCompletion := reactor.Snapshot()
	effects = applyOK(t, reactor, VoiceCommandCompletedInput{
		Sequence: command.Sequence,
		Kind:     realtime.CommandSessionUpdate,
	})
	assert.Empty(t, effects, "wire completion is not provider acceptance")
	assert.Equal(t, beforeCompletion, reactor.Snapshot())

	effects, err := reactor.Apply(ClientInput{Event: protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}})
	require.Error(t, err, "audio is gated until the provider confirms the mode")
	assert.Empty(t, effects)

	effects = applyOK(t, reactor, currentVoice(reactor, providerSessionUpdated(`null`, command.Sequence)))
	require.Len(t, effects, 1)
	assert.IsType(t, protocol.SessionUpdatedEvent{}, effects[0].ClientEvent)
	assert.Equal(t, realtime.InputTurnModeManual, effects[0].InputTurnModeBoundary)

	effects = applyOK(t, reactor, ClientInput{Event: protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}})
	require.Len(t, effects, 1)
	assert.Equal(t, realtime.CommandCommitAudio, effects[0].VoiceCommand.Kind)
}

func TestSequenceZeroInitialAckCannotConsumePendingClientMode(t *testing.T) {
	reactor, _ := unconfiguredTestReactor(1)
	effects := applyOK(t, reactor, ClientInput{Event: inputTurnSessionUpdate(json.RawMessage(`null`))})
	require.Len(t, effects, 1)
	command := effects[0].VoiceCommand
	require.NotNil(t, command)
	require.NotZero(t, command.Sequence)

	for _, turnDetection := range []string{`null`, `{"type":"server_vad"}`} {
		initialAck := providerSessionUpdated(turnDetection)
		initialAck.SessionUpdateAcknowledgement = true
		initialAck.SessionUpdateSequence = 0
		assert.Empty(t, applyOK(t, reactor, currentVoice(reactor, initialAck)))
		assert.Equal(t, realtime.InputTurnModeUnspecified, reactor.Snapshot().InputTurnMode)
		assert.Len(t, reactor.pendingSessionUpdates, 1)
	}

	clientAck := providerSessionUpdated(`null`, command.Sequence)
	effects = applyOK(t, reactor, currentVoice(reactor, clientAck))
	require.Len(t, effects, 1)
	assert.IsType(t, protocol.SessionUpdatedEvent{}, effects[0].ClientEvent)
	assert.Equal(t, realtime.InputTurnModeManual, reactor.Snapshot().InputTurnMode)
	assert.Empty(t, reactor.pendingSessionUpdates)
}

func TestUnsolicitedSequenceZeroCannotConfirmExpectedInitialMode(t *testing.T) {
	reactor, _ := unconfiguredTestReactor(1)
	require.NoError(t, reactor.expectInitialInputTurnMode(json.RawMessage(`null`)))

	unsolicited := providerSessionUpdated(`null`)
	unsolicited.SessionUpdateSequence = 0
	unsolicited.SessionUpdateAcknowledgement = false
	assert.Empty(t, applyOK(t, reactor, currentVoice(reactor, unsolicited)))
	assert.Equal(t, realtime.InputTurnModeUnspecified, reactor.Snapshot().InputTurnMode)
	assert.Equal(t, realtime.InputTurnModeManual, reactor.initialInputTurnMode)

	acknowledged := providerSessionUpdated(`null`)
	acknowledged.SessionUpdateSequence = 0
	acknowledged.SessionUpdateAcknowledgement = true
	assert.Empty(t, applyOK(t, reactor, currentVoice(reactor, acknowledged)))
	assert.Equal(t, realtime.InputTurnModeManual, reactor.Snapshot().InputTurnMode)
}

func TestMatchingBrowserModeWaitsBehindInitialProviderAcknowledgement(t *testing.T) {
	reactor, _ := unconfiguredTestReactor(1)
	turnDetection := json.RawMessage(`{"type":"server_vad"}`)
	require.NoError(t, reactor.expectInitialInputTurnMode(turnDetection))

	effects, err := reactor.Apply(ClientInput{Event: inputTurnSessionUpdate(turnDetection)})
	require.NoError(t, err, "the browser repeats the configured mode immediately after session.created")
	require.Len(t, effects, 1)
	command := effects[0].VoiceCommand
	require.NotNil(t, command)
	require.NotZero(t, command.Sequence)

	initialAck := providerSessionUpdated(string(turnDetection))
	initialAck.SessionUpdateAcknowledgement = true
	applyOK(t, reactor, currentVoice(reactor, initialAck))
	assert.Equal(t, realtime.InputTurnModeProviderVAD, reactor.Snapshot().InputTurnMode)
	assert.Len(t, reactor.pendingSessionUpdates, 1, "the browser acknowledgement is still independently pending")

	browserAck := providerSessionUpdated(string(turnDetection), command.Sequence)
	effects = applyOK(t, reactor, currentVoice(reactor, browserAck))
	require.Len(t, effects, 1)
	assert.IsType(t, protocol.SessionUpdatedEvent{}, effects[0].ClientEvent)
	assert.Empty(t, reactor.pendingSessionUpdates)
}

func TestWrongNonzeroSessionUpdateSequenceCannotConsumePendingMode(t *testing.T) {
	reactor, _ := unconfiguredTestReactor(1)
	effects := applyOK(t, reactor, ClientInput{Event: inputTurnSessionUpdate(json.RawMessage(`null`))})
	command := effects[0].VoiceCommand
	require.NotNil(t, command)

	wrong := providerSessionUpdated(`null`, command.Sequence+1)
	before := mutationView(reactor)
	assert.Empty(t, applyOK(t, reactor, currentVoice(reactor, wrong)))
	assert.Equal(t, before, mutationView(reactor))
	assert.Len(t, reactor.pendingSessionUpdates, 1)

	matching := providerSessionUpdated(`null`, command.Sequence)
	require.Len(t, applyOK(t, reactor, currentVoice(reactor, matching)), 1)
}

func TestProviderVADRejectsManualCommitAfterProviderConfirmation(t *testing.T) {
	for _, turnDetection := range []string{
		`{"type":"server_vad"}`,
		`{"type":"semantic_vad"}`,
	} {
		t.Run(turnDetection, func(t *testing.T) {
			reactor, _ := unconfiguredTestReactor(1)
			update := applyOK(t, reactor, ClientInput{Event: inputTurnSessionUpdate(json.RawMessage(turnDetection))})
			applyOK(t, reactor, currentVoice(reactor, providerSessionUpdated(turnDetection, update[0].VoiceCommand.Sequence)))
			before := reactor.Snapshot()

			effects, err := reactor.Apply(ClientInput{Event: protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}})
			require.Error(t, err)
			assert.Empty(t, effects)
			assert.Equal(t, before, reactor.Snapshot())
		})
	}
}

func TestManualModeRejectsProviderOwnedSpeechAndUnsolicitedCommit(t *testing.T) {
	tests := []realtime.Event{
		{Kind: realtime.EventSpeechStarted, ProviderEventID: "speech", ItemID: "auto-item"},
		{Kind: realtime.EventInputAudioCommitted, ProviderEventID: "committed", ItemID: "auto-item"},
	}
	for _, event := range tests {
		t.Run(event.ProviderEventID, func(t *testing.T) {
			reactor, _ := unconfiguredTestReactor(1)
			update := applyOK(t, reactor, ClientInput{Event: inputTurnSessionUpdate(json.RawMessage(`null`))})
			applyOK(t, reactor, currentVoice(reactor, providerSessionUpdated(`null`, update[0].VoiceCommand.Sequence)))
			before := mutationView(reactor)

			effects, err := reactor.Apply(currentVoice(reactor, event))
			require.Error(t, err)
			assert.Empty(t, effects)
			assert.Equal(t, before, mutationView(reactor))
		})
	}
}

func TestConfirmedInputTurnModeIsImmutable(t *testing.T) {
	reactor, _ := unconfiguredTestReactor(1)
	update := applyOK(t, reactor, ClientInput{Event: inputTurnSessionUpdate(json.RawMessage(`null`))})
	applyOK(t, reactor, currentVoice(reactor, providerSessionUpdated(`null`, update[0].VoiceCommand.Sequence)))
	before := mutationView(reactor)

	effects, err := reactor.Apply(ClientInput{Event: inputTurnSessionUpdate(json.RawMessage(`{"type":"server_vad"}`))})
	require.Error(t, err)
	assert.Empty(t, effects)
	assert.Equal(t, before, mutationView(reactor))
}
