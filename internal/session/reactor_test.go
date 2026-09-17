package session

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/protocol"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testIDs() IDs {
	return IDs{
		SessionID:      identity.SessionID("session-1"),
		ConversationID: identity.ConversationID("conversation-1"),
		Incarnation:    identity.Incarnation(7),
	}
}

func TestReactorPresentsDurableApprovalRequiredWithoutArguments(t *testing.T) {
	reactor := NewReactor(IDs{SessionID: "session", ConversationID: "conversation", Incarnation: 1}, NewEpochFence(0))
	expires := time.Now().UTC().Add(time.Minute)
	payload, err := json.Marshal(map[string]any{"invocation_id": "inv-1", "handoff_id": "handoff-1", "action_summary": "Approve side-effecting tool action: notify", "expires_at": expires, "risk_class": "side_effecting"})
	require.NoError(t, err)
	effects, err := reactor.Apply(HandoffEventInput{Scope: handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "conversation"}, Event: handoff.Event{Seq: 4, EventID: "event-4", HandoffID: "handoff-1", TenantID: "tenant", ConversationID: "conversation", Kind: "tool.approval_required", State: handoff.StateNeedsInput, Payload: payload}})
	require.NoError(t, err)
	require.Len(t, effects, 1)
	required, ok := effects[0].ClientEvent.(protocol.LiveToolApprovalRequiredEvent)
	require.True(t, ok)
	assert.Equal(t, "inv-1", required.InvocationID)
	encoded, err := protocol.Encode(required)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), "arguments")
}

func TestReactorPresentsApprovalResolutionWithoutSessionDegradation(t *testing.T) {
	tests := []struct {
		kind, outcome string
		state         handoff.State
		want          any
	}{
		{"tool.approval_approved", "approved", handoff.StatePending, protocol.LiveHandoffProgressEvent{}},
		{"tool.approval_denied", "denied", handoff.StateFailed, protocol.LiveHandoffFailedEvent{}},
		{"tool.approval_expired", "expired", handoff.StateFailed, protocol.LiveHandoffFailedEvent{}},
	}
	for _, test := range tests {
		t.Run(test.kind, func(t *testing.T) {
			reactor := presentationReactor(&presentationSafety{})
			payload := json.RawMessage(`{"invocation_id":"inv-1","outcome":"` + test.outcome + `"}`)
			effects, err := reactor.Apply(HandoffEventInput{Scope: handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "conversation"}, Event: handoff.Event{
				Seq: 5, EventID: "approval-outcome", HandoffID: "handoff-1", TenantID: "tenant", ConversationID: "conversation",
				Kind: test.kind, State: test.state, Payload: payload,
			}})
			require.NoError(t, err)
			var visual protocol.ServerEvent
			for _, effect := range effects {
				if effect.ClientEvent != nil {
					require.NotEqual(t, "live.session.degraded", effect.ClientEvent.EventType())
					visual = effect.ClientEvent
				}
			}
			require.NotNil(t, visual)
			assert.IsType(t, test.want, visual)
		})
	}
}

func testReactor(initial identity.ResponseEpoch) (*Reactor, *EpochFence) {
	fence := NewEpochFence(initial)
	reactor := NewReactor(testIDs(), fence)
	reactor.inputTurnMode = realtime.InputTurnModeManual
	return reactor, fence
}

func unconfiguredTestReactor(initial identity.ResponseEpoch) (*Reactor, *EpochFence) {
	fence := NewEpochFence(initial)
	return NewReactor(testIDs(), fence), fence
}

func providerVADTestReactor(initial identity.ResponseEpoch) (*Reactor, *EpochFence) {
	fence := NewEpochFence(initial)
	reactor := NewReactor(testIDs(), fence)
	reactor.inputTurnMode = realtime.InputTurnModeProviderVAD
	return reactor, fence
}

func applyOK(t *testing.T, reactor *Reactor, input Input) []Effect {
	t.Helper()
	effects, err := reactor.Apply(input)
	require.NoError(t, err)
	return effects
}

func currentVoice(reactor *Reactor, event realtime.Event) VoiceInput {
	return VoiceInput{Incarnation: reactor.Snapshot().IDs.Incarnation, Event: event}
}

func acknowledgeAudio(t *testing.T, reactor *Reactor, itemID string, previousItemID *string) []Effect {
	t.Helper()
	pending := reactor.providerInput.awaitingAcknowledgment()
	require.GreaterOrEqual(t, pending, 0)
	sequence := reactor.providerInput.generations[pending].commandSequence
	return applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventInputAudioCommitted, ProviderEventID: "committed-" + itemID,
		ItemID: itemID, PreviousItemID: previousItemID,
		InputAudioCommitSequence: sequence,
	}))
}

func openManualInputTranscription(t *testing.T, reactor *Reactor, itemID string) {
	t.Helper()
	applyOK(t, reactor, ClientInput{Event: protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}})
	acknowledgeAudio(t, reactor, itemID, nil)
}

func exactProviderInputItemID(index int) string {
	prefix := fmt.Sprintf("item-%04d-", index)
	return prefix + strings.Repeat("i", DurableIdentifierBytes-len(prefix))
}

func exactSizeSessionUpdate(t *testing.T, size int) protocol.SessionUpdateEvent {
	t.Helper()
	const prefix = `{"type":"realtime","instructions":"`
	const suffix = `"}`
	require.GreaterOrEqual(t, size, len(prefix)+len(suffix)+1)
	event := protocol.SessionUpdateEvent{
		Type: "session.update",
		Session: protocol.SessionUpdate{
			Type:         "realtime",
			Instructions: strings.Repeat("x", size-len(prefix)-len(suffix)),
		},
	}
	encoded, err := json.Marshal(event.Session)
	require.NoError(t, err)
	require.Len(t, encoded, size)
	return event
}

func createResponse(t *testing.T, reactor *Reactor) (identity.ResponseEpoch, realtime.Command) {
	t.Helper()
	effects := applyOK(t, reactor, ClientInput{Event: protocol.ResponseCreateEvent{Type: "response.create"}})
	require.Len(t, effects, 1)
	require.NotNil(t, effects[0].VoiceCommand)
	return effects[0].VoiceCommand.Epoch, *effects[0].VoiceCommand
}

func startStreaming(t *testing.T, reactor *Reactor, responseID string) identity.ResponseEpoch {
	t.Helper()
	epoch, _ := createResponse(t, reactor)
	effects := applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind:            realtime.EventResponseCreated,
		Epoch:           epoch,
		ProviderEventID: "provider-created-" + responseID,
		ResponseID:      responseID,
		Status:          "in_progress",
	}))
	require.Len(t, effects, 1)
	_, ok := effects[0].ClientEvent.(protocol.ResponseCreatedEvent)
	require.True(t, ok)
	return epoch
}

func responseTerminalCount(effects []Effect) int {
	count := 0
	for _, effect := range effects {
		if _, ok := effect.ClientEvent.(protocol.ResponseDoneEvent); ok {
			count++
		}
	}
	return count
}

func effectOrder(effects []Effect) []string {
	order := make([]string, 0, len(effects))
	for _, effect := range effects {
		switch {
		case effect.FlushBefore != 0:
			order = append(order, "flush")
		case effect.VoiceCommand != nil:
			order = append(order, fmt.Sprintf("command:%d", effect.VoiceCommand.Kind))
		case effect.ClientEvent != nil:
			order = append(order, "client:"+effect.ClientEvent.EventType())
		case effect.Async != nil:
			order = append(order, "async:"+effect.Async.Kind)
		case effect.Close != nil:
			order = append(order, "close")
		default:
			order = append(order, "empty")
		}
	}
	return order
}

func assertSnapshotAndEpochUnchanged(t *testing.T, before Snapshot, beforeEpoch identity.ResponseEpoch, reactor *Reactor) {
	t.Helper()
	assert.Equal(t, before, reactor.Snapshot())
	assert.Equal(t, beforeEpoch, reactor.Snapshot().ResponseEpoch)
}

type reactorMutationView struct {
	Snapshot                  Snapshot
	NextSequence              uint64
	InitializationError       string
	ActiveResponseID          string
	ActiveResponseEpoch       identity.ResponseEpoch
	Terminal                  *terminalMarker
	PendingSessionUpdates     map[uint64]string
	PendingSessionUpdateBytes int
	PendingToolCount          int
	PendingToolBytes          int
	ActiveTurn                *activeInputTurn
	ActiveSpeechItemID        string
	InputTurnRevision         uint32
	PendingCancelSequence     uint64
	SynthesizedEventSequence  uint64
	TranscriptSequence        uint64
	ProviderInput             providerInputAuthorityLedger
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func mutationView(reactor *Reactor) reactorMutationView {
	pendingUpdates := make(map[uint64]string, len(reactor.pendingSessionUpdates))
	for sequence, pending := range reactor.pendingSessionUpdates {
		pendingUpdates[sequence] = string(pending.session)
	}
	var terminal *terminalMarker
	if reactor.terminal != nil {
		copy := *reactor.terminal
		terminal = &copy
	}
	var activeTurn *activeInputTurn
	if reactor.activeTurn != nil {
		copy := *reactor.activeTurn
		activeTurn = &copy
	}
	providerInput := reactor.providerInput
	providerInput.history = make(map[string]string, len(reactor.providerInput.history))
	for itemID, canonical := range reactor.providerInput.history {
		providerInput.history[itemID] = canonical
	}
	providerInput.generations = append([]providerInputGeneration(nil), reactor.providerInput.generations...)
	return reactorMutationView{
		Snapshot:                  reactor.Snapshot(),
		NextSequence:              reactor.nextSeq,
		InitializationError:       errorString(reactor.initErr),
		ActiveResponseID:          reactor.activeResponseID,
		ActiveResponseEpoch:       reactor.activeResponseEpoch,
		Terminal:                  terminal,
		PendingSessionUpdates:     pendingUpdates,
		PendingSessionUpdateBytes: reactor.pendingSessionUpdateBytes,
		PendingToolCount:          len(reactor.pendingTools),
		PendingToolBytes:          reactor.pendingToolBytes,
		ActiveTurn:                activeTurn,
		ActiveSpeechItemID:        reactor.activeSpeechItemID,
		InputTurnRevision:         reactor.inputTurnRevision,
		PendingCancelSequence:     reactor.pendingCancelSequence,
		SynthesizedEventSequence:  reactor.synthEventSequence,
		TranscriptSequence:        reactor.transcriptSequence,
		ProviderInput:             providerInput,
	}
}

func TestEpochFenceMonotonicallyAdvancesAndAcceptsOnlyCurrent(t *testing.T) {
	fence := NewEpochFence(identity.ResponseEpoch(41))
	assert.Equal(t, identity.ResponseEpoch(41), fence.Current())
	assert.True(t, fence.Accept(identity.ResponseEpoch(41)))
	assert.False(t, fence.Accept(identity.ResponseEpoch(40)))

	assert.Equal(t, identity.ResponseEpoch(42), fence.Advance())
	assert.Equal(t, identity.ResponseEpoch(43), fence.Advance())
	assert.True(t, fence.Accept(identity.ResponseEpoch(43)))
	assert.False(t, fence.Accept(identity.ResponseEpoch(42)))
}

func TestInvalidConstructorIDsRejectEveryApplyWithoutMutation(t *testing.T) {
	tests := []struct {
		name string
		ids  IDs
	}{
		{name: "session ID", ids: IDs{ConversationID: "conversation-1", Incarnation: 1}},
		{name: "conversation ID", ids: IDs{SessionID: "session-1", Incarnation: 1}},
		{name: "incarnation", ids: IDs{SessionID: "session-1", ConversationID: "conversation-1"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reactor := NewReactor(test.ids, NewEpochFence(4))
			before := mutationView(reactor)
			effects, err := reactor.Apply(ClientInput{Event: protocol.InputAudioBufferCommitEvent{
				Type: "input_audio_buffer.commit",
			}})
			require.ErrorContains(t, err, "invalid session IDs")
			assert.Empty(t, effects)
			assert.Equal(t, before, mutationView(reactor))
		})
	}
}

func TestResponseStateTransitionsAndEpochRules(t *testing.T) {
	reactor, fence := testReactor(10)
	assert.Equal(t, ResponseIdle, reactor.Snapshot().Response)

	firstEpoch, firstCommand := createResponse(t, reactor)
	assert.Equal(t, identity.ResponseEpoch(11), firstEpoch)
	assert.Equal(t, realtime.CommandCreateResponse, firstCommand.Kind)
	assert.Equal(t, uint64(1), firstCommand.Sequence)
	assert.Equal(t, ResponsePending, reactor.Snapshot().Response)

	effects := applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind:            realtime.EventResponseCreated,
		Epoch:           firstEpoch,
		ProviderEventID: "created-1",
		ResponseID:      "response-1",
		Status:          "in_progress",
	}))
	require.Len(t, effects, 1)
	created := effects[0].ClientEvent.(protocol.ResponseCreatedEvent)
	assert.Equal(t, "response-1", created.Response.ID)
	assert.Equal(t, "created-1", created.EventID)
	assert.Equal(t, ResponseStreaming, reactor.Snapshot().Response)
	assert.Equal(t, identity.ResponseEpoch(11), fence.Current(), "explicit acknowledgement must not advance")

	effects = applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind:            realtime.EventResponseDone,
		Epoch:           firstEpoch,
		ProviderEventID: "done-1",
		ResponseID:      "response-1",
		Status:          "completed",
	}))
	require.Equal(t, 1, responseTerminalCount(effects))
	assert.Equal(t, ResponseTerminal, reactor.Snapshot().Response)
	assert.Equal(t, identity.ResponseEpoch(11), fence.Current(), "terminal events must not advance")

	secondEpoch, secondCommand := createResponse(t, reactor)
	assert.Equal(t, identity.ResponseEpoch(12), secondEpoch)
	assert.Equal(t, uint64(2), secondCommand.Sequence)
	assert.Equal(t, ResponsePending, reactor.Snapshot().Response, "a valid start resets observable terminal state")
}

func TestRejectedTransitionsDoNotPartiallyMutate(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testing.T, *Reactor)
		input func(*Reactor) Input
	}{
		{
			name:  "create while pending",
			setup: func(t *testing.T, r *Reactor) { createResponse(t, r) },
			input: func(*Reactor) Input { return ClientInput{Event: protocol.ResponseCreateEvent{Type: "response.create"}} },
		},
		{
			name:  "create while streaming",
			setup: func(t *testing.T, r *Reactor) { startStreaming(t, r, "active") },
			input: func(*Reactor) Input { return ClientInput{Event: protocol.ResponseCreateEvent{Type: "response.create"}} },
		},
		{
			name: "create while cancellation requested",
			setup: func(t *testing.T, r *Reactor) {
				startStreaming(t, r, "active")
				applyOK(t, r, ClientInput{Event: protocol.ResponseCancelEvent{Type: "response.cancel", ResponseID: "active"}})
			},
			input: func(*Reactor) Input { return ClientInput{Event: protocol.ResponseCreateEvent{Type: "response.create"}} },
		},
		{
			name:  "explicit response created while idle",
			setup: func(*testing.T, *Reactor) {},
			input: func(r *Reactor) Input {
				return currentVoice(r, realtime.Event{Kind: realtime.EventResponseCreated, Epoch: r.Snapshot().ResponseEpoch, ResponseID: "unexpected"})
			},
		},
		{
			name:  "explicit response created twice",
			setup: func(t *testing.T, r *Reactor) { startStreaming(t, r, "active") },
			input: func(r *Reactor) Input {
				return currentVoice(r, realtime.Event{Kind: realtime.EventResponseCreated, Epoch: r.Snapshot().ResponseEpoch, ResponseID: "active"})
			},
		},
		{
			name:  "implicit response created while explicit pending",
			setup: func(t *testing.T, r *Reactor) { createResponse(t, r) },
			input: func(r *Reactor) Input {
				return currentVoice(r, realtime.Event{Kind: realtime.EventResponseCreated, ResponseID: "implicit", NeedsEpochBinding: true})
			},
		},
		{
			name:  "cancel names a different active response",
			setup: func(t *testing.T, r *Reactor) { startStreaming(t, r, "active") },
			input: func(*Reactor) Input {
				return ClientInput{Event: protocol.ResponseCancelEvent{Type: "response.cancel", ResponseID: "different"}}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reactor, _ := testReactor(1)
			test.setup(t, reactor)
			before := reactor.Snapshot()
			beforeEpoch := before.ResponseEpoch
			effects, err := reactor.Apply(test.input(reactor))
			require.Error(t, err)
			assert.Empty(t, effects)
			assertSnapshotAndEpochUnchanged(t, before, beforeEpoch, reactor)
		})
	}
}

func TestResponseCreateCopiesControlValues(t *testing.T) {
	reactor, _ := testReactor(3)
	toolChoice := json.RawMessage(`{"type":"function","name":"lookup"}`)
	metadata := json.RawMessage(`{"caller":"mobile"}`)
	input := json.RawMessage(`[{"type":"message","role":"user"}]`)
	event := protocol.ResponseCreateEvent{
		Type: "response.create",
		Response: &protocol.ResponseConfig{
			Instructions: "answer briefly",
			ToolChoice:   toolChoice,
			Metadata:     metadata,
			Input:        input,
		},
	}

	effects := applyOK(t, reactor, ClientInput{Event: event})
	command := effects[0].VoiceCommand
	require.NotNil(t, command)
	toolChoice[1], metadata[2], input[2] = 'X', 'X', 'X'
	event.Response.Instructions = "mutated"

	assert.Equal(t, "answer briefly", command.Response.Instructions)
	assert.JSONEq(t, `{"type":"function","name":"lookup"}`, string(command.Response.ToolChoice))
	assert.JSONEq(t, `"mobile"`, string(command.Response.Metadata["caller"]))
	assert.JSONEq(t, `[{"type":"message","role":"user"}]`, string(command.Response.Input))
}

func TestResponseMetadataRequiresOnlyJSONStringsAtomically(t *testing.T) {
	tests := []struct {
		name     string
		metadata string
	}{
		{name: "number", metadata: `{"bad":1}`},
		{name: "boolean", metadata: `{"bad":true}`},
		{name: "null", metadata: `{"bad":null}`},
		{name: "array", metadata: `{"bad":[]}`},
		{name: "object", metadata: `{"bad":{}}`},
		{name: "mixed valid and invalid values", metadata: `{"ok":"value","bad":1}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reactor, _ := testReactor(3)
			before := mutationView(reactor)
			effects, err := reactor.Apply(ClientInput{Event: protocol.ResponseCreateEvent{
				Type: "response.create",
				Response: &protocol.ResponseConfig{
					Metadata: json.RawMessage(test.metadata),
				},
			}})

			require.Error(t, err)
			assert.Empty(t, effects)
			assert.Equal(t, before, mutationView(reactor))
		})
	}
}

func TestImplicitCreatedAdvancesBindsThenPublishes(t *testing.T) {
	reactor, fence := testReactor(20)
	effects := applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind:              realtime.EventResponseCreated,
		ProviderEventID:   "implicit-created",
		ResponseID:        "implicit-1",
		Status:            "in_progress",
		NeedsEpochBinding: true,
	}))

	assert.Equal(t, identity.ResponseEpoch(21), fence.Current())
	assert.Equal(t, ResponseStreaming, reactor.Snapshot().Response)
	require.Len(t, effects, 2)
	assert.Equal(t, []string{
		fmt.Sprintf("command:%d", realtime.CommandBindResponseEpoch),
		"client:response.created",
	}, effectOrder(effects))
	bind := effects[0].VoiceCommand
	require.NotNil(t, bind)
	assert.Equal(t, "implicit-1", bind.ResponseID)
	assert.Equal(t, identity.ResponseEpoch(21), bind.Epoch)
	assert.Equal(t, uint64(1), bind.Sequence)
	created := effects[1].ClientEvent.(protocol.ResponseCreatedEvent)
	assert.Equal(t, "implicit-1", created.Response.ID)
	assert.Equal(t, "implicit-created", created.EventID)

	effects = applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind:       realtime.EventTranscriptDelta,
		Epoch:      21,
		ResponseID: "implicit-1",
		ItemID:     "assistant-1",
		Text:       "hello",
	}))
	require.Len(t, effects, 1)
	assert.IsType(t, protocol.ResponseOutputAudioTranscriptDeltaEvent{}, effects[0].ClientEvent)
}

func TestImplicitCreatedWhileSpeakingBindsCancelsAndNeverPublishes(t *testing.T) {
	reactor, fence := providerVADTestReactor(20)
	applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventSpeechStarted, ItemID: "user-speaking",
	}))
	newerStarted := applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventSpeechStarted, ItemID: "newer-user-speaking",
	}))
	require.Len(t, newerStarted, 1)
	assert.Equal(t, "newer-user-speaking", newerStarted[0].ClientEvent.(protocol.InputAudioBufferSpeechStartedEvent).ItemID)
	assert.Empty(t, applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventSpeechStopped, ItemID: "user-speaking",
	})))
	require.True(t, reactor.Snapshot().UserSpeaking, "a delayed stop must not open the implicit-response fence")
	beforeSequence := reactor.nextSeq

	effects := applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind:              realtime.EventResponseCreated,
		ProviderEventID:   "implicit-created",
		ResponseID:        "implicit-over-user",
		Status:            "in_progress",
		NeedsEpochBinding: true,
	}))

	assert.Equal(t, []string{
		fmt.Sprintf("command:%d", realtime.CommandBindResponseEpoch),
		"flush",
		fmt.Sprintf("command:%d", realtime.CommandCancelResponse),
	}, effectOrder(effects))
	require.Len(t, effects, 3)
	bind := effects[0].VoiceCommand
	require.NotNil(t, bind)
	assert.Equal(t, "implicit-over-user", bind.ResponseID)
	assert.Equal(t, identity.ResponseEpoch(22), bind.Epoch)
	assert.Equal(t, identity.ResponseEpoch(23), effects[1].FlushBefore)
	cancel := effects[2].VoiceCommand
	require.NotNil(t, cancel)
	assert.Equal(t, "implicit-over-user", cancel.ResponseID)
	assert.Equal(t, identity.ResponseEpoch(23), cancel.Epoch)
	assert.Equal(t, beforeSequence+2, reactor.nextSeq)
	assert.Equal(t, identity.ResponseEpoch(23), fence.Current())
	assert.Equal(t, ResponseTerminal, reactor.Snapshot().Response)

	assert.Empty(t, applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventTranscriptDelta, Epoch: bind.Epoch, ResponseID: "implicit-over-user",
		ItemID: "assistant-over-user", Text: "must not stream",
	})))
	assert.Empty(t, applyOK(t, reactor, VoiceCommandCompletedInput{
		Sequence: cancel.Sequence, Kind: realtime.CommandCancelResponse,
	}))
	assert.Empty(t, applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventResponseDone, Epoch: cancel.Epoch, ResponseID: "implicit-over-user", Status: "cancelled",
	})))
	assert.Empty(t, applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventError, Epoch: cancel.Epoch, ProviderEventID: "implicit-cancel-error", Reason: "cancel rejected",
	})))
	assert.Equal(t, ResponseTerminal, reactor.Snapshot().Response)
}

func TestSpeechEdgesAndExplicitCancelFenceInOrder(t *testing.T) {
	t.Run("idle speech edge flushes once", func(t *testing.T) {
		reactor, fence := providerVADTestReactor(1)
		effects := applyOK(t, reactor, currentVoice(reactor, realtime.Event{
			Kind:   realtime.EventSpeechStarted,
			ItemID: "utterance-1",
		}))
		assert.Equal(t, identity.ResponseEpoch(2), fence.Current())
		assert.Equal(t, []string{"flush", "client:input_audio_buffer.speech_started"}, effectOrder(effects))
		assert.Equal(t, identity.ResponseEpoch(2), effects[0].FlushBefore)

		before := reactor.Snapshot()
		assert.Empty(t, applyOK(t, reactor, currentVoice(reactor, realtime.Event{Kind: realtime.EventSpeechStarted, ItemID: "utterance-1"})))
		assert.Equal(t, before, reactor.Snapshot())
		assert.Equal(t, identity.ResponseEpoch(2), fence.Current())

		effects = applyOK(t, reactor, currentVoice(reactor, realtime.Event{Kind: realtime.EventSpeechStarted, ItemID: "utterance-2"}))
		require.Len(t, effects, 1)
		started := effects[0].ClientEvent.(protocol.InputAudioBufferSpeechStartedEvent)
		assert.Equal(t, "utterance-2", started.ItemID)
		assert.Equal(t, identity.ResponseEpoch(2), fence.Current(), "a superseding speech start must not re-fence")
		beforeDelayedStop := mutationView(reactor)
		assert.Empty(t, applyOK(t, reactor, currentVoice(reactor, realtime.Event{Kind: realtime.EventSpeechStopped, ItemID: "utterance-1"})))
		assert.Equal(t, beforeDelayedStop, mutationView(reactor), "a delayed stop for the superseded item must not clear the live speech fence")
		assert.True(t, reactor.Snapshot().UserSpeaking)

		effects = applyOK(t, reactor, currentVoice(reactor, realtime.Event{Kind: realtime.EventSpeechStopped, ItemID: "utterance-2"}))
		require.Len(t, effects, 1)
		stopped := effects[0].ClientEvent.(protocol.InputAudioBufferSpeechStoppedEvent)
		assert.Equal(t, "utterance-2", stopped.ItemID)
		assert.Equal(t, identity.ResponseEpoch(2), fence.Current(), "speech stop must not advance")
		assert.Empty(t, applyOK(t, reactor, currentVoice(reactor, realtime.Event{Kind: realtime.EventSpeechStopped, ItemID: "utterance-2"})))
	})

	for _, test := range []struct {
		name   string
		cancel func(*Reactor) Input
	}{
		{
			name: "explicit cancel",
			cancel: func(*Reactor) Input {
				return ClientInput{Event: protocol.ResponseCancelEvent{Type: "response.cancel", ResponseID: "active"}}
			},
		},
		{
			name: "barge in",
			cancel: func(r *Reactor) Input {
				return currentVoice(r, realtime.Event{Kind: realtime.EventSpeechStarted, ItemID: "utterance-2"})
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			reactor, fence := providerVADTestReactor(5)
			startStreaming(t, reactor, "active")
			assert.Equal(t, identity.ResponseEpoch(6), fence.Current())
			effects := applyOK(t, reactor, test.cancel(reactor))
			assert.Equal(t, identity.ResponseEpoch(7), fence.Current())
			assert.Equal(t, ResponseCancelRequested, reactor.Snapshot().Response)
			require.GreaterOrEqual(t, len(effects), 2)
			assert.Equal(t, "flush", effectOrder(effects)[0])
			assert.Equal(t, fmt.Sprintf("command:%d", realtime.CommandCancelResponse), effectOrder(effects)[1])
			assert.Equal(t, identity.ResponseEpoch(7), effects[0].FlushBefore)
			assert.Equal(t, identity.ResponseEpoch(7), effects[1].VoiceCommand.Epoch)
			assert.Equal(t, "active", effects[1].VoiceCommand.ResponseID)

			before := reactor.Snapshot()
			beforeEpoch := fence.Current()
			assert.Empty(t, applyOK(t, reactor, ClientInput{Event: protocol.ResponseCancelEvent{Type: "response.cancel", ResponseID: "active"}}))
			assertSnapshotAndEpochUnchanged(t, before, beforeEpoch, reactor)
		})
	}

	t.Run("spurious cancel does nothing", func(t *testing.T) {
		reactor, fence := testReactor(9)
		before := reactor.Snapshot()
		assert.Empty(t, applyOK(t, reactor, ClientInput{Event: protocol.ResponseCancelEvent{Type: "response.cancel"}}))
		assertSnapshotAndEpochUnchanged(t, before, 9, reactor)
		assert.Equal(t, identity.ResponseEpoch(9), fence.Current())
	})
}

func TestDifferentSpeechItemWhileSpeakingSupersedesCommittedEligibility(t *testing.T) {
	reactor, _ := providerVADTestReactor(1)
	applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventSpeechStarted, ItemID: "turn-a",
	}))
	applyOK(t, reactor, DurableInput{
		Sequence: 1,
		Kind:     DurableKindTurnCommitted,
		Payload:  committedTurnPayload(identity.TurnKey{ID: "turn-a", Revision: 1}),
	})
	epoch := startStreaming(t, reactor, "response-1")

	supersedingStart := applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventSpeechStarted, ItemID: "turn-b",
	}))
	require.Len(t, supersedingStart, 1)
	assert.Equal(t, "turn-b", supersedingStart[0].ClientEvent.(protocol.InputAudioBufferSpeechStartedEvent).ItemID)
	require.NotNil(t, reactor.activeTurn)
	assert.Equal(t, identity.TurnKey{ID: "turn-b", Revision: 2}, reactor.activeTurn.key)
	assert.False(t, reactor.activeTurn.committed)

	effects := applyOK(t, reactor, currentVoice(reactor, backgroundToolEvent(
		epoch, "response-1", "call-after-new-speech", json.RawMessage(`{"objective":"new"}`),
	)))
	require.Len(t, effects, 1)
	assert.Nil(t, effects[0].Async)
}

func TestNewSpeechItemRejectsLateOlderInputTranscriptsWithoutMutation(t *testing.T) {
	for _, test := range []struct {
		name string
		kind realtime.EventKind
		text string
	}{
		{name: "delta", kind: realtime.EventTranscriptionDelta, text: "stale partial"},
		{name: "completion", kind: realtime.EventTranscriptionCompleted, text: "stale final"},
	} {
		t.Run(test.name, func(t *testing.T) {
			reactor, fence := providerVADTestReactor(1)
			applyOK(t, reactor, currentVoice(reactor, realtime.Event{
				Kind: realtime.EventSpeechStarted, ItemID: "turn-a",
			}))
			applyOK(t, reactor, currentVoice(reactor, realtime.Event{
				Kind: realtime.EventSpeechStarted, ItemID: "turn-b",
			}))
			require.NotNil(t, reactor.activeTurn)
			beforeTurn := *reactor.activeTurn
			beforeNextSequence := reactor.nextSeq
			beforeTranscriptSequence := reactor.transcriptSequence
			beforeSnapshot := reactor.Snapshot()
			beforeEpoch := fence.Current()

			effects := applyOK(t, reactor, currentVoice(reactor, realtime.Event{
				Kind: test.kind, ItemID: "turn-a", Text: test.text,
			}))

			assert.Empty(t, effects, "stale input must not publish or request persistence")
			require.NotNil(t, reactor.activeTurn)
			assert.Equal(t, beforeTurn, *reactor.activeTurn, "stale input must not replace the active turn")
			assert.Equal(t, beforeNextSequence, reactor.nextSeq, "stale input must not allocate an effect sequence")
			assert.Equal(t, beforeTranscriptSequence, reactor.transcriptSequence, "stale input must not allocate a durable transcript sequence")
			assert.Equal(t, beforeSnapshot, reactor.Snapshot())
			assert.Equal(t, beforeEpoch, fence.Current())
		})
	}
}

func TestClientCreatedUserTurnRejectsLateOlderProviderTranscriptsWithoutMutation(t *testing.T) {
	for _, test := range []struct {
		name string
		kind realtime.EventKind
		text string
	}{
		{name: "delta", kind: realtime.EventTranscriptionDelta, text: "late partial"},
		{name: "completion", kind: realtime.EventTranscriptionCompleted, text: "late final"},
	} {
		t.Run(test.name, func(t *testing.T) {
			reactor, fence := providerVADTestReactor(1)
			applyOK(t, reactor, currentVoice(reactor, realtime.Event{
				Kind: realtime.EventSpeechStarted, ItemID: "provider-turn-a",
			}))
			applyOK(t, reactor, currentVoice(reactor, realtime.Event{
				Kind: realtime.EventSpeechStopped, ItemID: "provider-turn-a",
			}))
			applyOK(t, reactor, ClientInput{Event: protocol.ConversationItemCreateEvent{
				Type: "conversation.item.create",
				Item: protocol.ConversationItem{
					ID: "client-turn-b", Type: "message", Role: "user",
					Content: json.RawMessage(`[{"type":"input_text","text":"newer user turn"}]`),
				},
			}})
			require.NotNil(t, reactor.activeTurn)
			beforeTurn := *reactor.activeTurn
			beforeNextSequence := reactor.nextSeq
			beforeTranscriptSequence := reactor.transcriptSequence
			beforeSnapshot := reactor.Snapshot()
			beforeEpoch := fence.Current()

			effects := applyOK(t, reactor, currentVoice(reactor, realtime.Event{
				Kind: test.kind, ItemID: "provider-turn-a", Text: test.text,
			}))

			assert.Empty(t, effects)
			require.NotNil(t, reactor.activeTurn)
			assert.Equal(t, beforeTurn, *reactor.activeTurn)
			assert.Equal(t, beforeNextSequence, reactor.nextSeq)
			assert.Equal(t, beforeTranscriptSequence, reactor.transcriptSequence)
			assert.Equal(t, beforeSnapshot, reactor.Snapshot())
			assert.Equal(t, beforeEpoch, fence.Current())
		})
	}
}

func TestClientTurnSupersedesPendingManualCommitWithoutLateAckStealingAuthority(t *testing.T) {
	reactor, fence := testReactor(1)
	commit := applyOK(t, reactor, ClientInput{Event: protocol.InputAudioBufferCommitEvent{
		Type: "input_audio_buffer.commit",
	}})
	require.Len(t, commit, 1)
	commitSequence := commit[0].VoiceCommand.Sequence
	applyOK(t, reactor, ClientInput{Event: protocol.ConversationItemCreateEvent{
		Type: "conversation.item.create",
		Item: protocol.ConversationItem{
			ID: "client-turn-b", Type: "message", Role: "user",
			Content: json.RawMessage(`[{"type":"input_text","text":"newer user turn"}]`),
		},
	}})
	require.NotNil(t, reactor.activeTurn)
	assert.Equal(t, identity.TurnKey{ID: "client-turn-b", Revision: 1}, reactor.activeTurn.key)

	lateAck := applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventInputAudioCommitted, ProviderEventID: "late-provider-turn-a",
		ItemID: "provider-turn-a", InputAudioCommitSequence: commitSequence,
	}))
	require.Len(t, lateAck, 2, "the causal ack still releases the sole manual commit slot")
	assert.Equal(t, realtime.CommandAcknowledgeAudioCommit, lateAck[0].VoiceCommand.Kind)
	require.NotNil(t, reactor.activeTurn)
	assert.Equal(t, identity.TurnKey{ID: "client-turn-b", Revision: 1}, reactor.activeTurn.key,
		"a superseded commit ack must not replace newer explicit client authority")
	beforeLateTranscript := mutationView(reactor)
	for _, kind := range []realtime.EventKind{realtime.EventTranscriptionDelta, realtime.EventTranscriptionCompleted} {
		assert.Empty(t, applyOK(t, reactor, currentVoice(reactor, realtime.Event{
			Kind: kind, ItemID: "provider-turn-a", Text: "late transcript",
		})))
	}
	assert.Equal(t, beforeLateTranscript, mutationView(reactor))

	applyOK(t, reactor, ClientInput{Event: protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}})
	acknowledgeAudio(t, reactor, "provider-turn-c", nil)

	effects := applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventTranscriptionDelta, ItemID: "provider-turn-c", Text: "new partial",
	}))
	require.Len(t, effects, 1)
	assert.Equal(t, "conversation.item.input_audio_transcription.delta", effects[0].ClientEvent.EventType())
	require.NotNil(t, reactor.activeTurn)
	assert.Equal(t, identity.TurnKey{ID: "provider-turn-c", Revision: 2}, reactor.activeTurn.key)

	effects = applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventTranscriptionCompleted, ItemID: "provider-turn-c", Text: "new final",
	}))
	require.Len(t, effects, 1)
	require.NotNil(t, effects[0].Transcript)
	assert.Equal(t, identity.TurnKey{ID: "provider-turn-c", Revision: 2}, effects[0].Transcript.Turn)
	assert.Equal(t, "new final", effects[0].Transcript.FinalText)

	for _, itemID := range []string{"provider-turn-a", "client-turn-b"} {
		before := mutationView(reactor)
		beforeEpoch := fence.Current()
		for _, event := range []realtime.Event{
			{Kind: realtime.EventTranscriptionDelta, ItemID: itemID, Text: "late partial"},
			{Kind: realtime.EventTranscriptionCompleted, ItemID: itemID, Text: "late final"},
		} {
			assert.Empty(t, applyOK(t, reactor, currentVoice(reactor, event)))
		}
		assert.Equal(t, before, mutationView(reactor))
		assert.Equal(t, beforeEpoch, fence.Current())
	}
}

func TestManualTranscriptCannotOpenBeforeCommittedAck(t *testing.T) {
	reactor, _ := testReactor(1)
	applyOK(t, reactor, ClientInput{Event: protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}})
	before := mutationView(reactor)
	assert.Empty(t, applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventTranscriptionCompleted, ItemID: "turn-a", Text: "early turn a",
	})))
	assert.Equal(t, before, mutationView(reactor))
	acknowledgeAudio(t, reactor, "turn-a", nil)

	effects := applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventTranscriptionCompleted, ItemID: "turn-a", Text: "turn a",
	}))
	require.Len(t, effects, 1)
	require.NotNil(t, effects[0].Transcript)
	assert.Equal(t, identity.TurnKey{ID: "turn-a", Revision: 1}, effects[0].Transcript.Turn)
}

func TestClientUserItemCannotReuseHistoricalProviderItemWithoutSpeechEdge(t *testing.T) {
	reactor, _ := providerVADTestReactor(1)
	applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventSpeechStarted, ItemID: "turn-a",
	}))
	applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventSpeechStopped, ItemID: "turn-a",
	}))
	applyOK(t, reactor, ClientInput{Event: protocol.ConversationItemCreateEvent{
		Type: "conversation.item.create",
		Item: protocol.ConversationItem{
			ID: "turn-b", Type: "message", Role: "user",
			Content: json.RawMessage(`[{"type":"input_text","text":"turn b"}]`),
		},
	}})
	before := mutationView(reactor)

	effects, err := reactor.Apply(ClientInput{Event: protocol.ConversationItemCreateEvent{
		Type: "conversation.item.create",
		Item: protocol.ConversationItem{
			ID: "turn-a", Type: "message", Role: "user",
			Content: json.RawMessage(`[{"type":"input_text","text":"reused turn a"}]`),
		},
	}})
	require.ErrorContains(t, err, "historical provider input item")
	assert.Empty(t, effects)
	assert.Equal(t, before, mutationView(reactor))

	for _, event := range []realtime.Event{
		{Kind: realtime.EventTranscriptionDelta, ItemID: "turn-a", Text: "late partial"},
		{Kind: realtime.EventTranscriptionCompleted, ItemID: "turn-a", Text: "late final"},
	} {
		assert.Empty(t, applyOK(t, reactor, currentVoice(reactor, event)))
	}
	assert.Equal(t, before, mutationView(reactor))
}

func TestAudioCommitRequiresCommittedAckBeforeNextCommit(t *testing.T) {
	reactor, _ := testReactor(1)

	first := applyOK(t, reactor, ClientInput{Event: protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}})
	require.Len(t, first, 1)
	require.NotNil(t, first[0].VoiceCommand)
	assert.Equal(t, realtime.CommandCommitAudio, first[0].VoiceCommand.Kind)

	before := mutationView(reactor)
	second, err := reactor.Apply(ClientInput{Event: protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}})
	require.ErrorIs(t, err, ErrAudioCommitInFlight)
	assert.Empty(t, second)
	assert.Equal(t, before, mutationView(reactor), "rejected commit must not allocate a sequence or mutate correlation state")

	previous := "assistant-or-tool-item"
	ack := applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventInputAudioCommitted, ProviderEventID: "ack-a",
		ItemID: "input-a", PreviousItemID: &previous,
		InputAudioCommitSequence: first[0].VoiceCommand.Sequence,
	}))
	require.Len(t, ack, 2)
	require.NotNil(t, ack[0].VoiceCommand)
	assert.Equal(t, realtime.CommandAcknowledgeAudioCommit, ack[0].VoiceCommand.Kind)
	assert.Equal(t, "input-a", ack[0].VoiceCommand.InputItemID)
	committed, ok := ack[1].ClientEvent.(protocol.InputAudioBufferCommittedEvent)
	require.True(t, ok)
	assert.Equal(t, "input-a", committed.ItemID)
	require.NotNil(t, committed.PreviousItemID)
	assert.Equal(t, previous, *committed.PreviousItemID)

	next := applyOK(t, reactor, ClientInput{Event: protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}})
	require.Len(t, next, 1, "the exact committed ack releases the sole commit slot")
	assert.Equal(t, realtime.CommandCommitAudio, next[0].VoiceCommand.Kind)
}

func TestManualCommittedWithoutRegisteredSequenceCannotStealPendingCommit(t *testing.T) {
	reactor, _ := testReactor(1)
	applyOK(t, reactor, ClientInput{Event: protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}})
	before := mutationView(reactor)

	effects, err := reactor.Apply(currentVoice(reactor, realtime.Event{
		Kind: realtime.EventInputAudioCommitted, ProviderEventID: "pre-registration",
		ItemID: "stale-auto-item",
	}))
	require.ErrorIs(t, err, ErrUnexpectedAudioCommitAck)
	assert.Empty(t, effects)
	assert.Equal(t, before, mutationView(reactor))
	require.True(t, reactor.Snapshot().InputAudioCommitPending)

	effects, err = reactor.Apply(ClientInput{Event: protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}})
	require.ErrorIs(t, err, ErrAudioCommitInFlight)
	assert.Empty(t, effects)
	assert.Equal(t, before, mutationView(reactor))

	ack := acknowledgeAudio(t, reactor, "actual-manual-item", nil)
	require.Len(t, ack, 2)
	assert.Equal(t, realtime.CommandAcknowledgeAudioCommit, ack[0].VoiceCommand.Kind)
}

func TestPriorManualSequenceCannotStealNextCommit(t *testing.T) {
	reactor, _ := testReactor(1)
	first := applyOK(t, reactor, ClientInput{Event: protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}})
	firstSequence := first[0].VoiceCommand.Sequence

	ackA := realtime.Event{
		Kind: realtime.EventInputAudioCommitted, ProviderEventID: "slot-one-a", ItemID: "slot-one-a",
		InputAudioCommitSequence: firstSequence,
	}
	bufferedB := realtime.Event{
		Kind: realtime.EventInputAudioCommitted, ProviderEventID: "slot-one-b", ItemID: "slot-one-b",
		InputAudioCommitSequence: firstSequence,
	}
	release := applyOK(t, reactor, currentVoice(reactor, ackA))
	require.Len(t, release, 2)
	assert.Equal(t, firstSequence, release[0].VoiceCommand.InputAudioCommitSequence)

	second := applyOK(t, reactor, ClientInput{Event: protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}})
	secondSequence := second[0].VoiceCommand.Sequence
	require.NotEqual(t, firstSequence, secondSequence)

	before := mutationView(reactor)
	stale, err := reactor.Apply(currentVoice(reactor, bufferedB))
	require.ErrorIs(t, err, ErrUnexpectedAudioCommitAck)
	assert.Empty(t, stale)
	assert.Equal(t, before, mutationView(reactor))
	require.True(t, reactor.Snapshot().InputAudioCommitPending)

	actual := applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventInputAudioCommitted, ProviderEventID: "slot-two", ItemID: "slot-two",
		InputAudioCommitSequence: secondSequence,
	}))
	require.Len(t, actual, 2)
	assert.Equal(t, secondSequence, actual[0].VoiceCommand.InputAudioCommitSequence)
}

func TestUnknownTranscriptAndDuplicateAckCannotReleaseLaterCommit(t *testing.T) {
	reactor, _ := testReactor(1)
	applyOK(t, reactor, ClientInput{Event: protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}})
	beforeUnknown := mutationView(reactor)
	assert.Empty(t, applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventTranscriptionCompleted, ItemID: "unacknowledged-x", Text: "must not claim",
	})))
	assert.Equal(t, beforeUnknown, mutationView(reactor))

	acknowledgeAudio(t, reactor, "turn-a", nil)
	applyOK(t, reactor, ClientInput{Event: protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}})
	beforeDuplicate := mutationView(reactor)
	assert.Empty(t, acknowledgeAudio(t, reactor, "turn-a", nil), "historical duplicate is consumed without binding the new slot")
	assert.Equal(t, beforeDuplicate, mutationView(reactor))

	effects, err := reactor.Apply(ClientInput{Event: protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}})
	require.ErrorIs(t, err, ErrAudioCommitInFlight)
	assert.Empty(t, effects)
	acknowledgeAudio(t, reactor, "turn-b", nil)
	require.Len(t, applyOK(t, reactor, ClientInput{Event: protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}}), 1)
}

func TestProviderVADTranscriptsRequireCommittedForEachGeneration(t *testing.T) {
	reactor, _ := providerVADTestReactor(1)
	applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventSpeechStarted, ItemID: "turn-a",
	}))
	applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventSpeechStopped, ItemID: "turn-a",
	}))
	beforeCommit := mutationView(reactor)
	assert.Empty(t, applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventTranscriptionCompleted, ItemID: "turn-a", Text: "turn a",
	})))
	assert.Equal(t, beforeCommit, mutationView(reactor))
	applyOK(t, reactor, currentVoice(reactor, realtime.Event{Kind: realtime.EventInputAudioCommitted, ItemID: "turn-a"}))
	first := applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventTranscriptionCompleted, ItemID: "turn-a", Text: "turn a",
	}))
	require.Len(t, first, 1)
	require.NotNil(t, first[0].Transcript)

	applyOK(t, reactor, currentVoice(reactor, realtime.Event{Kind: realtime.EventSpeechStarted, ItemID: "turn-b"}))
	applyOK(t, reactor, currentVoice(reactor, realtime.Event{Kind: realtime.EventSpeechStopped, ItemID: "turn-b"}))
	applyOK(t, reactor, currentVoice(reactor, realtime.Event{Kind: realtime.EventInputAudioCommitted, ItemID: "turn-b"}))

	effects := applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventTranscriptionCompleted, ItemID: "turn-b", Text: "turn b",
	}))
	require.Len(t, effects, 1)
	require.NotNil(t, effects[0].Transcript)
	assert.Equal(t, identity.TurnKey{ID: "turn-b", Revision: 2}, effects[0].Transcript.Turn)
}

func TestUnknownTranscriptsNeverEstablishAuthorityInConfiguredModes(t *testing.T) {
	for _, test := range []struct {
		name string
		new  func(identity.ResponseEpoch) (*Reactor, *EpochFence)
	}{
		{name: "manual", new: testReactor},
		{name: "provider VAD", new: providerVADTestReactor},
	} {
		t.Run(test.name, func(t *testing.T) {
			reactor, fence := test.new(1)
			before := mutationView(reactor)
			beforeEpoch := fence.Current()
			for _, event := range []realtime.Event{
				{Kind: realtime.EventTranscriptionDelta, ItemID: "unknown", Text: "partial"},
				{Kind: realtime.EventTranscriptionCompleted, ItemID: "unknown", Text: "final"},
			} {
				assert.Empty(t, applyOK(t, reactor, currentVoice(reactor, event)))
			}
			assert.Equal(t, before, mutationView(reactor))
			assert.Equal(t, beforeEpoch, fence.Current())
		})
	}
}

func TestAudioCommitGenerationLedgerFailsClosedAtCapacity(t *testing.T) {
	reactor, _ := testReactor(1)
	for index := range providerInputGenerationLimit {
		applyOK(t, reactor, ClientInput{Event: protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}})
		itemID := fmt.Sprintf("open-%02d", index)
		acknowledgeAudio(t, reactor, itemID, nil)
		applyOK(t, reactor, currentVoice(reactor, realtime.Event{
			Kind: realtime.EventTranscriptionDelta, ItemID: itemID, Text: "open",
		}))
	}
	before := mutationView(reactor)

	effects, err := reactor.Apply(ClientInput{Event: protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}})
	require.ErrorContains(t, err, "provider input generation limit")
	assert.Empty(t, effects)
	assert.Equal(t, before, mutationView(reactor))
}

func TestProviderInputHistoryUsesExactNoEvictionBounds(t *testing.T) {
	reactor, _ := providerVADTestReactor(1)
	for index := range providerInputItemHistoryLimit {
		itemID := exactProviderInputItemID(index)
		applyOK(t, reactor, currentVoice(reactor, realtime.Event{Kind: realtime.EventSpeechStarted, ItemID: itemID}))
		applyOK(t, reactor, currentVoice(reactor, realtime.Event{Kind: realtime.EventSpeechStopped, ItemID: itemID}))
		applyOK(t, reactor, currentVoice(reactor, realtime.Event{Kind: realtime.EventInputAudioCommitted, ItemID: itemID}))
		effects := applyOK(t, reactor, currentVoice(reactor, realtime.Event{
			Kind: realtime.EventTranscriptionCompleted, ItemID: itemID, Text: "bounded",
		}))
		require.Len(t, effects, 1)
	}
	require.Len(t, reactor.providerInput.history, providerInputItemHistoryLimit)
	assert.Equal(t, providerInputItemHistoryBytes, reactor.providerInput.historyBytes)

	first := exactProviderInputItemID(0)
	beforeReuse := mutationView(reactor)
	effects, err := reactor.Apply(currentVoice(reactor, realtime.Event{
		Kind: realtime.EventSpeechStarted, ItemID: first,
	}))
	require.ErrorIs(t, err, ErrHistoricalProviderInputItem)
	assert.Empty(t, effects)
	assert.Equal(t, beforeReuse, mutationView(reactor))

	beforeClientReuse := mutationView(reactor)
	effects, err = reactor.Apply(ClientInput{Event: protocol.ConversationItemCreateEvent{
		Type: "conversation.item.create",
		Item: protocol.ConversationItem{ID: first, Type: "message", Role: "user", Content: json.RawMessage(`[{"type":"input_text","text":"reuse"}]`)},
	}})
	require.ErrorIs(t, err, ErrHistoricalProviderInputItem)
	assert.Empty(t, effects)
	assert.Equal(t, beforeClientReuse, mutationView(reactor))

	beforeOverflow := mutationView(reactor)
	effects, err = reactor.Apply(currentVoice(reactor, realtime.Event{
		Kind: realtime.EventSpeechStarted, ItemID: exactProviderInputItemID(providerInputItemHistoryLimit),
	}))
	require.ErrorIs(t, err, ErrProviderInputItemLimit)
	assert.Empty(t, effects)
	assert.Equal(t, beforeOverflow, mutationView(reactor), "overflow must not evict history")
}

func TestProviderInputIdentifiersRejectInvalidValuesWithoutMutation(t *testing.T) {
	invalidUTF8 := string([]byte{0xff})
	tests := []struct {
		name string
		id   string
	}{
		{name: "257 bytes", id: strings.Repeat("i", DurableIdentifierBytes+1)},
		{name: "one mebibyte", id: strings.Repeat("i", 1<<20)},
		{name: "whitespace only", id: " \t"},
		{name: "interior ASCII space", id: "item bad"},
		{name: "unicode space", id: "item\u00a0bad"},
		{name: "control", id: "item\x00bad"},
		{name: "invalid UTF-8", id: invalidUTF8},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			inputs := []func(*Reactor) Input{
				func(*Reactor) Input {
					return ClientInput{Event: protocol.ConversationItemCreateEvent{
						Type: "conversation.item.create",
						Item: protocol.ConversationItem{ID: test.id, Type: "message", Role: "user", Content: json.RawMessage(`[{"type":"input_text","text":"x"}]`)},
					}}
				},
				func(r *Reactor) Input {
					r.inputTurnMode = realtime.InputTurnModeProviderVAD
					return currentVoice(r, realtime.Event{Kind: realtime.EventSpeechStarted, ItemID: test.id})
				},
				func(r *Reactor) Input {
					r.inputTurnMode = realtime.InputTurnModeProviderVAD
					applyOK(t, r, currentVoice(r, realtime.Event{Kind: realtime.EventSpeechStarted, ItemID: "valid-speech-item"}))
					return currentVoice(r, realtime.Event{Kind: realtime.EventSpeechStopped, ItemID: test.id})
				},
				func(r *Reactor) Input {
					applyOK(t, r, ClientInput{Event: protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}})
					return currentVoice(r, realtime.Event{Kind: realtime.EventInputAudioCommitted, ItemID: test.id})
				},
				func(r *Reactor) Input {
					return currentVoice(r, realtime.Event{Kind: realtime.EventTranscriptionDelta, ItemID: test.id, Text: "x"})
				},
				func(r *Reactor) Input {
					return currentVoice(r, realtime.Event{Kind: realtime.EventTranscriptionCompleted, ItemID: test.id, Text: "x"})
				},
			}
			for index, build := range inputs {
				reactor, _ := testReactor(1)
				input := build(reactor)
				before := mutationView(reactor)
				effects, err := reactor.Apply(input)
				require.Error(t, err, "input variant %d", index)
				assert.Empty(t, effects)
				assert.Equal(t, before, mutationView(reactor), "input variant %d", index)
			}
		})
	}

	t.Run("invalid nullable predecessor", func(t *testing.T) {
		reactor, _ := testReactor(1)
		applyOK(t, reactor, ClientInput{Event: protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}})
		invalid := "assistant item"
		before := mutationView(reactor)
		effects, err := reactor.Apply(currentVoice(reactor, realtime.Event{
			Kind: realtime.EventInputAudioCommitted, ItemID: "valid-item", PreviousItemID: &invalid,
		}))
		require.Error(t, err)
		assert.Empty(t, effects)
		assert.Equal(t, before, mutationView(reactor))
	})

	t.Run("exact 256 bytes", func(t *testing.T) {
		reactor, _ := testReactor(1)
		applyOK(t, reactor, ClientInput{Event: protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}})
		itemID := strings.Repeat("i", DurableIdentifierBytes)
		require.Len(t, acknowledgeAudio(t, reactor, itemID, nil), 2)
		assert.Equal(t, DurableIdentifierBytes, reactor.providerInput.historyBytes)
	})
}

func TestClientSupersessionRetainsOnlyUnknownAckTombstone(t *testing.T) {
	reactor, _ := testReactor(1)
	commit := applyOK(t, reactor, ClientInput{Event: protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}})
	commitSequence := commit[0].VoiceCommand.Sequence
	applyOK(t, reactor, ClientInput{Event: protocol.ConversationItemCreateEvent{
		Type: "conversation.item.create",
		Item: protocol.ConversationItem{
			ID: "client-b", Type: "message", Role: "user",
			Content: json.RawMessage(`[{"type":"input_text","text":"newer"}]`),
		},
	}})
	beforeLate := mutationView(reactor)
	assert.Empty(t, applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventTranscriptionCompleted, ItemID: "input-a", Text: "late pre-ack transcript",
	})))
	assert.Equal(t, beforeLate, mutationView(reactor))

	release := acknowledgeAudio(t, reactor, "input-a", nil)
	require.Len(t, release, 2)
	assert.Equal(t, commitSequence, release[0].VoiceCommand.InputAudioCommitSequence)
	require.NotNil(t, reactor.activeTurn)
	assert.Equal(t, "client-b", reactor.activeTurn.key.ID, "late ack releases the slot but cannot replace the client turn")
	applyOK(t, reactor, ClientInput{Event: protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}})
	previous := "tool-result-predecessor"
	acknowledgeAudio(t, reactor, "input-c", &previous)
	effects := applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventTranscriptionCompleted, ItemID: "input-c", Text: "turn c",
	}))
	require.Len(t, effects, 1)
	require.NotNil(t, effects[0].Transcript)
	assert.Equal(t, "input-c", effects[0].Transcript.Turn.ID)

	beforeStale := mutationView(reactor)
	assert.Empty(t, applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventTranscriptionCompleted, ItemID: "input-a", Text: "late a",
	})))
	assert.Equal(t, beforeStale, mutationView(reactor))
}

func TestAcknowledgedOpenGenerationsKeepImmutableTurnsWhenInterleaved(t *testing.T) {
	reactor, _ := testReactor(1)
	applyOK(t, reactor, ClientInput{Event: protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}})
	acknowledgeAudio(t, reactor, "turn-b", nil)
	applyOK(t, reactor, currentVoice(reactor, realtime.Event{Kind: realtime.EventTranscriptionDelta, ItemID: "turn-b", Text: "b"}))
	turnB := reactor.providerInput.generations[0].turn

	applyOK(t, reactor, ClientInput{Event: protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}})
	acknowledgeAudio(t, reactor, "turn-c", nil)
	applyOK(t, reactor, currentVoice(reactor, realtime.Event{Kind: realtime.EventTranscriptionDelta, ItemID: "turn-c", Text: "c"}))
	turnC := reactor.providerInput.generations[1].turn

	completedB := applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventTranscriptionCompleted, ItemID: "turn-b", Text: "turn b",
	}))
	require.NotNil(t, completedB[0].Transcript)
	assert.Equal(t, turnB, completedB[0].Transcript.Turn)
	require.NotNil(t, reactor.activeTurn)
	assert.Equal(t, turnC, reactor.activeTurn.key, "older completion must not replace the newer active turn")

	completedC := applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventTranscriptionCompleted, ItemID: "turn-c", Text: "turn c",
	}))
	require.NotNil(t, completedC[0].Transcript)
	assert.Equal(t, turnC, completedC[0].Transcript.Turn)
	require.Len(t, applyOK(t, reactor, ClientInput{Event: protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}}), 1,
		"matching completions release aggregate generation capacity")
}

func TestPendingCancelNeverSynthesizesEmptyResponseDone(t *testing.T) {
	reactor, fence := testReactor(1)
	createResponse(t, reactor)
	effects := applyOK(t, reactor, ClientInput{Event: protocol.ResponseCancelEvent{Type: "response.cancel"}})
	assert.Equal(t, identity.ResponseEpoch(3), fence.Current())
	assert.Equal(t, ResponseTerminal, reactor.Snapshot().Response)
	assert.Equal(t, []string{"flush", fmt.Sprintf("command:%d", realtime.CommandCancelResponse)}, effectOrder(effects))
	require.Empty(t, effects[1].VoiceCommand.ResponseID)

	completion := VoiceCommandCompletedInput{
		Sequence: effects[1].VoiceCommand.Sequence,
		Kind:     realtime.CommandCancelResponse,
	}
	assert.Empty(t, applyOK(t, reactor, completion))
	assert.Equal(t, ResponseTerminal, reactor.Snapshot().Response)
	assert.Zero(t, responseTerminalCount(effects))
}

func TestResponseOutputRequiresCurrentEpochAndIncarnation(t *testing.T) {
	reactor, _ := testReactor(4)
	epoch := startStreaming(t, reactor, "response-5")
	before := reactor.Snapshot()

	tests := []struct {
		name  string
		input VoiceInput
	}{
		{
			name: "old epoch",
			input: VoiceInput{Incarnation: testIDs().Incarnation, Event: realtime.Event{
				Kind: realtime.EventTranscriptDelta, Epoch: epoch - 1, ResponseID: "response-5", ItemID: "assistant", Text: "stale",
			}},
		},
		{
			name: "old incarnation",
			input: VoiceInput{Incarnation: testIDs().Incarnation - 1, Event: realtime.Event{
				Kind: realtime.EventTranscriptDelta, Epoch: epoch, ResponseID: "response-5", ItemID: "assistant", Text: "stale",
			}},
		},
		{
			name: "different response",
			input: VoiceInput{Incarnation: testIDs().Incarnation, Event: realtime.Event{
				Kind: realtime.EventTranscriptDelta, Epoch: epoch, ResponseID: "different", ItemID: "assistant", Text: "stale",
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Empty(t, applyOK(t, reactor, test.input))
			assert.Equal(t, before, reactor.Snapshot())
		})
	}

	effects := applyOK(t, reactor, VoiceInput{Incarnation: testIDs().Incarnation, Event: realtime.Event{
		Kind: realtime.EventTranscriptDelta, Epoch: epoch, ResponseID: "response-5", ItemID: "assistant", Text: "current",
	}})
	require.Len(t, effects, 1)
	delta := effects[0].ClientEvent.(protocol.ResponseOutputAudioTranscriptDeltaEvent)
	assert.Equal(t, "current", delta.Delta)
	assert.NotEmpty(t, delta.EventID)

	// Decoded media is never placed in a generic client control effect.
	assert.Empty(t, applyOK(t, reactor, VoiceInput{Incarnation: testIDs().Incarnation, Event: realtime.Event{
		Kind: realtime.EventAudioDelta, Epoch: epoch, ResponseID: "response-5", ItemID: "assistant", Audio: []byte{1, 2, 3},
	}}))
}

func TestZeroAndWrongIncarnationVoiceInputsAreInert(t *testing.T) {
	type fixture struct {
		reactor *Reactor
		event   realtime.Event
	}
	tests := []struct {
		name  string
		setup func(*testing.T) fixture
	}{
		{
			name: "speech",
			setup: func(t *testing.T) fixture {
				reactor, _ := testReactor(1)
				return fixture{reactor: reactor, event: realtime.Event{Kind: realtime.EventSpeechStarted, ItemID: "turn-1"}}
			},
		},
		{
			name: "response created",
			setup: func(t *testing.T) fixture {
				reactor, _ := testReactor(1)
				epoch, _ := createResponse(t, reactor)
				return fixture{reactor: reactor, event: realtime.Event{Kind: realtime.EventResponseCreated, Epoch: epoch, ResponseID: "response-1"}}
			},
		},
		{
			name: "response output",
			setup: func(t *testing.T) fixture {
				reactor, _ := testReactor(1)
				epoch := startStreaming(t, reactor, "response-1")
				return fixture{reactor: reactor, event: realtime.Event{Kind: realtime.EventTranscriptDelta, Epoch: epoch, ResponseID: "response-1", ItemID: "assistant-1", Text: "late"}}
			},
		},
		{
			name: "response done",
			setup: func(t *testing.T) fixture {
				reactor, _ := testReactor(1)
				epoch := startStreaming(t, reactor, "response-1")
				return fixture{reactor: reactor, event: realtime.Event{Kind: realtime.EventResponseDone, Epoch: epoch, ResponseID: "response-1", Status: "completed"}}
			},
		},
		{
			name: "transcription",
			setup: func(t *testing.T) fixture {
				reactor, _ := testReactor(1)
				return fixture{reactor: reactor, event: realtime.Event{Kind: realtime.EventTranscriptionCompleted, ItemID: "turn-1", Text: "hello"}}
			},
		},
		{
			name: "tool",
			setup: func(t *testing.T) fixture {
				reactor, _ := testReactor(1)
				epoch := startStreaming(t, reactor, "response-1")
				return fixture{reactor: reactor, event: backgroundToolEvent(epoch, "response-1", "call-1", json.RawMessage(`{"objective":"research"}`))}
			},
		},
	}

	incarnationCases := []struct {
		name               string
		currentIncarnation identity.Incarnation
		inputIncarnation   identity.Incarnation
	}{
		{name: "zero", currentIncarnation: testIDs().Incarnation, inputIncarnation: 0},
		{name: "wrong", currentIncarnation: testIDs().Incarnation, inputIncarnation: testIDs().Incarnation + 1},
		{name: "zero even when current is zero", currentIncarnation: 0, inputIncarnation: 0},
	}
	for _, test := range tests {
		for _, incarnationCase := range incarnationCases {
			name := fmt.Sprintf("%s/%s", test.name, incarnationCase.name)
			t.Run(name, func(t *testing.T) {
				fixture := test.setup(t)
				fixture.reactor.ids.Incarnation = incarnationCase.currentIncarnation
				before := mutationView(fixture.reactor)
				effects := applyOK(t, fixture.reactor, VoiceInput{Incarnation: incarnationCase.inputIncarnation, Event: fixture.event})
				assert.Empty(t, effects)
				assert.Equal(t, before, mutationView(fixture.reactor))
			})
		}
	}
}

func TestOldEpochProviderErrorCannotTerminateCurrentResponse(t *testing.T) {
	reactor, _ := testReactor(6)
	epoch := startStreaming(t, reactor, "current-response")
	before := reactor.Snapshot()

	effects := applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind:   realtime.EventError,
		Epoch:  epoch - 1,
		Reason: "late failure from prior response",
	}))
	assert.Empty(t, effects)
	assert.Equal(t, before, reactor.Snapshot())
}

func TestResponseTerminalRacesEmitExactlyOneDone(t *testing.T) {
	tests := []struct {
		name  string
		race  []string
		first string
	}{
		{name: "normal done before other signals", race: []string{"done", "completion", "error"}, first: "done"},
		{name: "cancel completion wins", race: []string{"completion", "done", "error"}, first: "completion"},
		{name: "provider error wins", race: []string{"error", "completion", "done"}, first: "error"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reactor, _ := testReactor(1)
			responseEpoch := startStreaming(t, reactor, "race-response")
			cancelEffects := applyOK(t, reactor, ClientInput{Event: protocol.ResponseCancelEvent{Type: "response.cancel", ResponseID: "race-response"}})
			cancel := cancelEffects[1].VoiceCommand
			require.NotNil(t, cancel)
			currentEpoch := reactor.Snapshot().ResponseEpoch

			totalDone := 0
			for _, signal := range test.race {
				var effects []Effect
				switch signal {
				case "done":
					// A racing provider may stamp either the pre-cancel response
					// epoch (stale) or the current cancel epoch. The current one
					// is eligible to win terminalization.
					epoch := currentEpoch
					if test.first != "done" {
						epoch = responseEpoch
					}
					effects = applyOK(t, reactor, currentVoice(reactor, realtime.Event{
						Kind: realtime.EventResponseDone, Epoch: epoch, ResponseID: "race-response", Status: "cancelled",
					}))
				case "completion":
					effects = applyOK(t, reactor, VoiceCommandCompletedInput{Sequence: cancel.Sequence, Kind: realtime.CommandCancelResponse})
				case "error":
					effects = applyOK(t, reactor, currentVoice(reactor, realtime.Event{Kind: realtime.EventError, Reason: "provider failed"}))
				}
				totalDone += responseTerminalCount(effects)
				for _, effect := range effects {
					if done, ok := effect.ClientEvent.(protocol.ResponseDoneEvent); ok {
						assert.Equal(t, "race-response", done.Response.ID)
						assert.NotEmpty(t, done.EventID)
					}
				}
			}
			assert.Equal(t, 1, totalDone)
			assert.Equal(t, ResponseTerminal, reactor.Snapshot().Response)
		})
	}
}

func TestCancelCompletionRequiresExactCommandCorrelation(t *testing.T) {
	reactor, _ := testReactor(1)
	startStreaming(t, reactor, "active")
	cancelEffects := applyOK(t, reactor, ClientInput{Event: protocol.ResponseCancelEvent{Type: "response.cancel", ResponseID: "active"}})
	command := cancelEffects[1].VoiceCommand

	assert.Empty(t, applyOK(t, reactor, VoiceCommandCompletedInput{Sequence: command.Sequence + 1, Kind: realtime.CommandCancelResponse}))
	assert.Equal(t, ResponseCancelRequested, reactor.Snapshot().Response)
	assert.Empty(t, applyOK(t, reactor, VoiceCommandCompletedInput{Sequence: command.Sequence, Kind: realtime.CommandCreateResponse}))
	assert.Equal(t, ResponseCancelRequested, reactor.Snapshot().Response)

	effects := applyOK(t, reactor, VoiceCommandCompletedInput{Sequence: command.Sequence, Kind: realtime.CommandCancelResponse})
	assert.Equal(t, 1, responseTerminalCount(effects))
	assert.Empty(t, applyOK(t, reactor, VoiceCommandCompletedInput{Sequence: command.Sequence, Kind: realtime.CommandCancelResponse}))
}

func TestConnectedInputSynthesizesSessionCreatedAndOnlyReconnectAdvances(t *testing.T) {
	reactor, fence := testReactor(30)
	providerSession := json.RawMessage(`{"id":"provider-session-1","model":"voice"}`)
	effects := applyOK(t, reactor, ConnectedInput{ProviderSession: providerSession})
	require.Len(t, effects, 1)
	created := effects[0].ClientEvent.(protocol.SessionCreatedEvent)
	assert.Equal(t, identity.ResponseEpoch(30), fence.Current())
	assert.NotEmpty(t, created.EventID)
	assert.JSONEq(t, string(providerSession), string(created.Session))
	providerSession[2] = 'X'
	assert.JSONEq(t, `{"id":"provider-session-1","model":"voice"}`, string(created.Session))

	second := applyOK(t, reactor, ConnectedInput{ProviderSession: json.RawMessage(`{"id":"provider-session-2"}`)})
	assert.Equal(t, identity.ResponseEpoch(31), fence.Current())
	assert.Equal(t, []string{"flush", "client:session.created"}, effectOrder(second))
	assert.NotEqual(t, created.EventID, second[1].ClientEvent.(protocol.SessionCreatedEvent).EventID)

	assert.Empty(t, applyOK(t, reactor, currentVoice(reactor, realtime.Event{Kind: realtime.EventSessionCreated, ProviderEventID: "upstream-created"})))
}

func TestConnectedInputEmitsSafeProfileSelectionAfterSessionCreated(t *testing.T) {
	reactor, _ := testReactor(1)
	selected := protocol.SelectedProfile{ID: "insurance", Version: 3, SnapshotHash: strings.Repeat("a", 64)}
	effects := applyOK(t, reactor, ConnectedInput{ProviderSession: json.RawMessage(`{"id":"provider"}`), Profile: &selected})
	require.Len(t, effects, 2)
	assert.IsType(t, protocol.SessionCreatedEvent{}, effects[0].ClientEvent)
	profileEvent, ok := effects[1].ClientEvent.(protocol.LiveProfileSelectedEvent)
	require.True(t, ok)
	assert.Equal(t, selected, profileEvent.Profile)
	assert.NotEmpty(t, profileEvent.EventID)
	restaged, _ := testReactor(1)
	restagedEffects := applyOK(t, restaged, ConnectedInput{ProviderSession: json.RawMessage(`{"id":"provider-retry"}`), Profile: &selected})
	restagedEvent := restagedEffects[1].ClientEvent.(protocol.LiveProfileSelectedEvent)
	assert.Equal(t, profileEvent.EventID, restagedEvent.EventID, "the same incarnation and attempt order must replay a stable identity")

	selected.ID = "mutated"
	assert.Equal(t, "insurance", profileEvent.Profile.ID)
}

func TestSessionUpdatedRequiresActualProviderAcknowledgement(t *testing.T) {
	reactor, fence := unconfiguredTestReactor(1)
	toolParams := json.RawMessage(`{"type":"object","properties":{"q":{"type":"string"}}}`)
	tools := json.RawMessage(`[{"type":"function","name":"lookup","description":"find it","parameters":` + string(toolParams) + `}]`)
	turnDetection := json.RawMessage(`{"type":"server_vad","threshold":0.5}`)
	event := protocol.SessionUpdateEvent{
		Type:    "session.update",
		EventID: "client-update-1",
		Session: protocol.SessionUpdate{
			Type:         "realtime",
			Instructions: "be concise",
			Audio: &protocol.SessionAudio{
				Input: &protocol.SessionAudioInput{
					Format:        &protocol.AudioFormat{Type: "audio/pcm", Rate: 24_000},
					TurnDetection: turnDetection,
				},
				Output: &protocol.SessionAudioOutput{
					Format: &protocol.AudioFormat{Type: "audio/pcm", Rate: 24_000},
					Voice:  "alloy",
				},
			},
			Tools:      tools,
			ToolChoice: json.RawMessage(`"auto"`),
		},
	}
	effects := applyOK(t, reactor, ClientInput{Event: event})
	require.Len(t, effects, 1)
	command := effects[0].VoiceCommand
	require.NotNil(t, command)
	assert.Equal(t, realtime.CommandSessionUpdate, command.Kind)
	assert.Equal(t, uint64(1), command.Sequence)
	assert.Equal(t, "be concise", command.Session.Instructions)
	assert.Equal(t, "pcm16le", command.Session.InputFormat.Encoding)
	assert.Equal(t, 24_000, command.Session.InputFormat.SampleRate)
	assert.Equal(t, 1, command.Session.InputFormat.Channels)
	assert.Equal(t, "pcm16le", command.Session.OutputFormat.Encoding)
	assert.Equal(t, 24_000, command.Session.OutputFormat.SampleRate)
	assert.Equal(t, 1, command.Session.OutputFormat.Channels)
	assert.Equal(t, "alloy", command.Session.Voice)
	require.Len(t, command.Session.Tools, 4)
	assert.Equal(t, "function", command.Session.Tools[0].Type)
	assert.Equal(t, "lookup", command.Session.Tools[0].Name)
	assert.Equal(t, "find it", command.Session.Tools[0].Description)
	assert.JSONEq(t, string(toolParams), string(command.Session.Tools[0].Parameters))
	assert.JSONEq(t, `"auto"`, string(command.Session.ToolChoice))

	tools[2], turnDetection[2], toolParams[2] = 'X', 'X', 'X'
	assert.Equal(t, "lookup", command.Session.Tools[0].Name)
	assert.JSONEq(t, `{"type":"server_vad","threshold":0.5}`, string(command.Session.TurnDetection))
	assert.JSONEq(t, `{"type":"object","properties":{"q":{"type":"string"}}}`, string(command.Session.Tools[0].Parameters))

	assert.Empty(t, applyOK(t, reactor, VoiceCommandCompletedInput{Sequence: command.Sequence + 1, Kind: realtime.CommandSessionUpdate}))
	assert.Empty(t, applyOK(t, reactor, VoiceCommandCompletedInput{Sequence: command.Sequence, Kind: realtime.CommandCommitAudio}))
	assert.Empty(t, applyOK(t, reactor, VoiceCommandCompletedInput{Sequence: command.Sequence, Kind: realtime.CommandSessionUpdate}))

	effects = applyOK(t, reactor, currentVoice(reactor, providerSessionUpdated(`{"type":"server_vad","threshold":0.5}`, command.Sequence)))
	require.Len(t, effects, 1)
	updated := effects[0].ClientEvent.(protocol.SessionUpdatedEvent)
	assert.NotEmpty(t, updated.EventID)
	assert.JSONEq(t, `{"type":"realtime","instructions":"be concise","audio":{"input":{"format":{"type":"audio/pcm","rate":24000},"turn_detection":{"type":"server_vad","threshold":0.5}},"output":{"format":{"type":"audio/pcm","rate":24000},"voice":"alloy"}},"tools":[{"type":"function","name":"lookup","description":"find it","parameters":{"type":"object","properties":{"q":{"type":"string"}}}}],"tool_choice":"auto"}`, string(updated.Session))
	assert.Empty(t, applyOK(t, reactor, VoiceCommandCompletedInput{Sequence: command.Sequence, Kind: realtime.CommandSessionUpdate}))
	assert.Equal(t, identity.ResponseEpoch(1), fence.Current(), "session updates and completions never advance")
}

func TestInvalidSessionUpdateDoesNotAllocateSequenceOrPendingAck(t *testing.T) {
	reactor, _ := testReactor(1)
	before := reactor.Snapshot()
	effects, err := reactor.Apply(ClientInput{Event: protocol.SessionUpdateEvent{
		Type: "session.update",
		Session: protocol.SessionUpdate{
			Type:  "realtime",
			Tools: json.RawMessage(`[{"type":"function","name":"broken","parameters":]`),
		},
	}})
	require.Error(t, err)
	assert.Empty(t, effects)
	assert.Equal(t, before, reactor.Snapshot())

	effects = applyOK(t, reactor, ClientInput{Event: protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}})
	require.Len(t, effects, 1)
	assert.Equal(t, uint64(1), effects[0].VoiceCommand.Sequence)
	assert.Empty(t, applyOK(t, reactor, VoiceCommandCompletedInput{Sequence: 1, Kind: realtime.CommandSessionUpdate}))
}

func TestUnsupportedNonzeroClientFieldsRejectWithoutMutation(t *testing.T) {
	tests := []struct {
		name  string
		input Input
	}{
		{
			name: "response conversation",
			input: ClientInput{Event: protocol.ResponseCreateEvent{Type: "response.create", Response: &protocol.ResponseConfig{
				Conversation: "conversation-1",
			}}},
		},
		{
			name: "response output modalities",
			input: ClientInput{Event: protocol.ResponseCreateEvent{Type: "response.create", Response: &protocol.ResponseConfig{
				OutputModalities: []string{"audio"},
			}}},
		},
		{
			name: "response tools",
			input: ClientInput{Event: protocol.ResponseCreateEvent{Type: "response.create", Response: &protocol.ResponseConfig{
				Tools: json.RawMessage(`[]`),
			}}},
		},
		{
			name: "session model",
			input: ClientInput{Event: protocol.SessionUpdateEvent{Type: "session.update", Session: protocol.SessionUpdate{
				Type: "realtime", Model: "gpt-realtime",
			}}},
		},
		{
			name: "session output modalities",
			input: ClientInput{Event: protocol.SessionUpdateEvent{Type: "session.update", Session: protocol.SessionUpdate{
				Type: "realtime", OutputModalities: []string{"audio"},
			}}},
		},
		{
			name: "session metadata",
			input: ClientInput{Event: protocol.SessionUpdateEvent{Type: "session.update", Session: protocol.SessionUpdate{
				Type: "realtime", Metadata: json.RawMessage(`{}`),
			}}},
		},
		{
			name: "input transcription",
			input: ClientInput{Event: protocol.SessionUpdateEvent{Type: "session.update", Session: protocol.SessionUpdate{
				Type: "realtime", Audio: &protocol.SessionAudio{Input: &protocol.SessionAudioInput{
					Transcription: json.RawMessage(`{}`),
				}},
			}}},
		},
		{
			name: "input noise reduction",
			input: ClientInput{Event: protocol.SessionUpdateEvent{Type: "session.update", Session: protocol.SessionUpdate{
				Type: "realtime", Audio: &protocol.SessionAudio{Input: &protocol.SessionAudioInput{
					NoiseReduction: json.RawMessage(`{}`),
				}},
			}}},
		},
		{
			name: "output speed",
			input: ClientInput{Event: protocol.SessionUpdateEvent{Type: "session.update", Session: protocol.SessionUpdate{
				Type: "realtime", Audio: &protocol.SessionAudio{Output: &protocol.SessionAudioOutput{Speed: 1}},
			}}},
		},
		{
			name: "conversation previous item ID",
			input: ClientInput{Event: protocol.ConversationItemCreateEvent{
				Type:           "conversation.item.create",
				PreviousItemID: "previous-1",
				Item:           protocol.ConversationItem{ID: "item-1", Type: "message"},
			}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reactor, _ := testReactor(5)
			before := mutationView(reactor)
			effects, err := reactor.Apply(test.input)

			require.Error(t, err)
			assert.Empty(t, effects)
			assert.Equal(t, before, mutationView(reactor))
		})
	}
}

func TestSessionToolsRequireSupportedFunctionShapeAtomically(t *testing.T) {
	tests := []struct {
		name  string
		tools string
	}{
		{name: "tools must be array", tools: `{}`},
		{name: "tools cannot be null", tools: `null`},
		{name: "type must be function", tools: `[{"type":"custom","name":"lookup","parameters":{}}]`},
		{name: "name is required", tools: `[{"type":"function","parameters":{}}]`},
		{name: "parameters are required", tools: `[{"type":"function","name":"lookup"}]`},
		{name: "parameters cannot be null", tools: `[{"type":"function","name":"lookup","parameters":null}]`},
		{name: "parameters cannot be array", tools: `[{"type":"function","name":"lookup","parameters":[]}]`},
		{name: "parameters cannot be scalar", tools: `[{"type":"function","name":"lookup","parameters":"schema"}]`},
		{name: "parameters must be valid JSON", tools: `[{"type":"function","name":"lookup","parameters":]`},
		{name: "unknown tool field cannot be dropped", tools: `[{"type":"function","name":"lookup","parameters":{},"strict":true}]`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reactor, _ := testReactor(5)
			before := mutationView(reactor)
			effects, err := reactor.Apply(ClientInput{Event: protocol.SessionUpdateEvent{
				Type: "session.update",
				Session: protocol.SessionUpdate{
					Type:  "realtime",
					Tools: json.RawMessage(test.tools),
				},
			}})

			require.Error(t, err)
			assert.Empty(t, effects)
			assert.Equal(t, before, mutationView(reactor))
		})
	}

	reactor, _ := testReactor(5)
	effects := applyOK(t, reactor, ClientInput{Event: protocol.SessionUpdateEvent{
		Type: "session.update",
		Session: protocol.SessionUpdate{
			Type:  "realtime",
			Tools: json.RawMessage(`[{"type":"function","name":"lookup","description":"find it","parameters":{}}]`),
		},
	}})
	require.Len(t, effects, 1)
	require.Len(t, effects[0].VoiceCommand.Session.Tools, 4)
	tool := effects[0].VoiceCommand.Session.Tools[0]
	assert.Equal(t, "function", tool.Type)
	assert.Equal(t, "lookup", tool.Name)
	assert.Equal(t, "find it", tool.Description)
	assert.JSONEq(t, `{}`, string(tool.Parameters))
}

func TestSessionUpdatePreservesOrdinaryToolsAndInjectsReservedSchemas(t *testing.T) {
	reactor, _ := testReactor(5)
	effects := applyOK(t, reactor, ClientInput{Event: protocol.SessionUpdateEvent{
		Type: "session.update",
		Session: protocol.SessionUpdate{
			Type:       "realtime",
			Tools:      json.RawMessage(`[{"type":"function","name":"lookup","parameters":{"type":"object"}}]`),
			ToolChoice: json.RawMessage(`{"type":"function","name":"lookup"}`),
		},
	}})
	require.Len(t, effects, 1)
	command := effects[0].VoiceCommand
	require.NotNil(t, command)
	require.Len(t, command.Session.Tools, 4)
	assert.Equal(t, "lookup", command.Session.Tools[0].Name)
	assert.Equal(t, []string{
		handoff.ToolStartBackgroundTask,
		handoff.ToolSteerBackgroundTask,
		handoff.ToolCancelBackgroundTask,
	}, []string{command.Session.Tools[1].Name, command.Session.Tools[2].Name, command.Session.Tools[3].Name})
	assert.JSONEq(t, `{"type":"function","name":"lookup"}`, string(command.Session.ToolChoice))
}

func TestSessionUpdateDoesNotInjectReservedToolsWhenHandoffCapabilityIsDisabled(t *testing.T) {
	freeze := NewEpochFence(5)
	reactor := newReactorWithHandoffTools(testIDs(), freeze, false)
	reactor.inputTurnMode = realtime.InputTurnModeManual
	effects := applyOK(t, reactor, ClientInput{Event: protocol.SessionUpdateEvent{
		Type: "session.update",
		Session: protocol.SessionUpdate{
			Type:       "realtime",
			Tools:      json.RawMessage(`[{"type":"function","name":"lookup","parameters":{"type":"object"}}]`),
			ToolChoice: json.RawMessage(`{"type":"function","name":"lookup"}`),
		},
	}})
	require.Len(t, effects, 1)
	command := effects[0].VoiceCommand
	require.NotNil(t, command)
	require.Len(t, command.Session.Tools, 1)
	assert.Equal(t, "lookup", command.Session.Tools[0].Name)
}

func TestDisabledHandoffCapabilityStillRejectsClientReservedToolCollision(t *testing.T) {
	reactor := newReactorWithHandoffTools(testIDs(), NewEpochFence(5), false)
	effects, err := reactor.Apply(ClientInput{Event: protocol.SessionUpdateEvent{
		Type: "session.update",
		Session: protocol.SessionUpdate{
			Type:  "realtime",
			Tools: json.RawMessage(`[{"type":"function","name":"start_background_task","parameters":{}}]`),
		},
	}})
	require.ErrorIs(t, err, ErrInvalidSessionTool)
	assert.Empty(t, effects)
}

func TestReservedProviderCallRemainsOrdinaryWhenHandoffCapabilityIsDisabled(t *testing.T) {
	reactor := newReactorWithHandoffTools(testIDs(), NewEpochFence(1), false)
	epoch := startStreaming(t, reactor, "response-disabled-handoff")
	effects := applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventFunctionCallDone, Epoch: epoch, ResponseID: "response-disabled-handoff",
		ItemID: "tool-disabled-handoff", CallID: "call-disabled-handoff", Name: StartBackgroundTaskTool,
		Arguments: json.RawMessage(`{"objective":"ordinary provider-owned function"}`),
	}))
	require.Len(t, effects, 1)
	assert.IsType(t, protocol.ResponseFunctionCallArgumentsDoneEvent{}, effects[0].ClientEvent)
	assert.Nil(t, effects[0].Handoff)
	assert.Nil(t, effects[0].Async)
	assert.Equal(t, ResponseStreaming, reactor.Snapshot().Response)
}

func TestClientCannotShadowOrForceReservedTools(t *testing.T) {
	tests := []protocol.SessionUpdate{
		{Type: "realtime", Tools: json.RawMessage(`[{"type":"function","name":"start_background_task","parameters":{}}]`)},
		{Type: "realtime", Tools: json.RawMessage(`[{"type":"function","name":"steer_background_task","parameters":{}}]`)},
		{Type: "realtime", Tools: json.RawMessage(`[{"type":"function","name":"cancel_background_task","parameters":{}}]`)},
		{Type: "realtime", ToolChoice: json.RawMessage(`{"type":"function","name":"start_background_task"}`)},
		{Type: "realtime", ToolChoice: json.RawMessage(`{"type":"function","function":{"name":"cancel_background_task"}}`)},
	}
	for _, update := range tests {
		reactor, _ := testReactor(5)
		before := mutationView(reactor)
		effects, err := reactor.Apply(ClientInput{Event: protocol.SessionUpdateEvent{Type: "session.update", Session: update}})
		require.Error(t, err)
		assert.Empty(t, effects)
		assert.Equal(t, before, mutationView(reactor))
	}

	reactor, _ := testReactor(5)
	before := mutationView(reactor)
	effects, err := reactor.Apply(ClientInput{Event: protocol.ResponseCreateEvent{Type: "response.create", Response: &protocol.ResponseConfig{
		ToolChoice: json.RawMessage(`{"type":"function","name":"steer_background_task"}`),
	}}})
	require.Error(t, err)
	assert.Empty(t, effects)
	assert.Equal(t, before, mutationView(reactor))
}

func TestPendingSessionUpdateCountLimitPrecedesNormalizationAndRecovers(t *testing.T) {
	const countLimit = 1
	const wantLimitError = "pending session update limit exceeded"
	assert.Equal(t, countLimit, PendingSessionUpdateLimit)
	reactor, _ := testReactor(5)
	event := protocol.SessionUpdateEvent{
		Type:    "session.update",
		Session: protocol.SessionUpdate{Type: "realtime"},
	}

	effects := applyOK(t, reactor, ClientInput{Event: event})
	require.Len(t, effects, 1)
	firstSequence := effects[0].VoiceCommand.Sequence
	assert.Equal(t, uint64(1), firstSequence)
	require.Len(t, reactor.pendingSessionUpdates, countLimit)
	retainedCapacity := 0
	for _, pending := range reactor.pendingSessionUpdates {
		retainedCapacity += cap(pending.session)
	}
	assert.Equal(t, retainedCapacity, reactor.pendingSessionUpdateBytes)

	largeTools := json.RawMessage(`[{"type":"function","name":"lookup","description":"` + strings.Repeat("x", 256<<10) + `","parameters":{}}]`)
	require.True(t, json.Valid(largeTools))
	expensiveInvalidEvent := protocol.SessionUpdateEvent{
		Type: "session.update",
		Session: protocol.SessionUpdate{
			Type:  "realtime",
			Tools: largeTools,
			Audio: &protocol.SessionAudio{Input: &protocol.SessionAudioInput{
				TurnDetection: json.RawMessage(`{`),
			}},
		},
	}
	beforeOverflow := mutationView(reactor)
	effects, err := reactor.Apply(ClientInput{Event: expensiveInvalidEvent})
	require.EqualError(t, err, wantLimitError)
	require.ErrorIs(t, err, ErrPendingSessionUpdateLimit)
	assert.Empty(t, effects)
	assert.Equal(t, beforeOverflow, mutationView(reactor))

	assert.Empty(t, applyOK(t, reactor, VoiceCommandCompletedInput{
		Sequence: firstSequence,
		Kind:     realtime.CommandSessionUpdate,
	}))
	effects = applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventSessionUpdated, ProviderEventID: "provider-ack",
		SessionUpdateAcknowledgement: true, SessionUpdateSequence: firstSequence,
	}))
	require.Len(t, effects, 1)
	assert.IsType(t, protocol.SessionUpdatedEvent{}, effects[0].ClientEvent)

	effects = applyOK(t, reactor, ClientInput{Event: event})
	require.Len(t, effects, 1)
	assert.Equal(t, uint64(2), effects[0].VoiceCommand.Sequence)
	require.Len(t, reactor.pendingSessionUpdates, countLimit)

	beforeDuplicate := mutationView(reactor)
	assert.Empty(t, applyOK(t, reactor, VoiceCommandCompletedInput{
		Sequence: firstSequence,
		Kind:     realtime.CommandSessionUpdate,
	}))
	assert.Equal(t, beforeDuplicate, mutationView(reactor))
}

func TestPendingSessionUpdateByteBoundaryNoMutationAndRecovery(t *testing.T) {
	const byteLimit = 1 << 20
	const wantLimitError = "pending session update limit exceeded"
	assert.Equal(t, byteLimit, PendingSessionUpdateBytes)
	reactor, _ := testReactor(5)
	exactBoundary := exactSizeSessionUpdate(t, byteLimit)
	effects := applyOK(t, reactor, ClientInput{Event: exactBoundary})
	require.Len(t, effects, 1)
	firstSequence := effects[0].VoiceCommand.Sequence
	assert.Equal(t, uint64(1), firstSequence)
	assert.Equal(t, byteLimit, reactor.pendingSessionUpdateBytes)

	beforeOverflow := mutationView(reactor)
	effects, err := reactor.Apply(ClientInput{Event: protocol.SessionUpdateEvent{
		Type:    "session.update",
		Session: protocol.SessionUpdate{Type: "realtime"},
	}})
	require.EqualError(t, err, wantLimitError)
	require.ErrorIs(t, err, ErrPendingSessionUpdateLimit)
	assert.Empty(t, effects)
	assert.Equal(t, beforeOverflow, mutationView(reactor))

	assert.Empty(t, applyOK(t, reactor, VoiceCommandCompletedInput{
		Sequence: firstSequence,
		Kind:     realtime.CommandSessionUpdate,
	}))
	effects = applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventSessionUpdated, ProviderEventID: "provider-ack",
		SessionUpdateAcknowledgement: true, SessionUpdateSequence: firstSequence,
	}))
	require.Len(t, effects, 1)
	updated := effects[0].ClientEvent.(protocol.SessionUpdatedEvent)
	assert.Len(t, updated.Session, byteLimit)

	effects = applyOK(t, reactor, ClientInput{Event: protocol.SessionUpdateEvent{
		Type:    "session.update",
		Session: protocol.SessionUpdate{Type: "realtime"},
	}})
	require.Len(t, effects, 1)
	assert.Equal(t, uint64(2), effects[0].VoiceCommand.Sequence)
}

func TestReconnectClearsPendingSessionUpdateSnapshots(t *testing.T) {
	const countLimit = 1
	const byteLimit = 1 << 20
	const wantLimitError = "pending session update limit exceeded"
	reactor, _ := testReactor(5)
	applyOK(t, reactor, ConnectedInput{ProviderSession: json.RawMessage(`{"id":"first"}`)})

	minimal := protocol.SessionUpdateEvent{
		Type:    "session.update",
		Session: protocol.SessionUpdate{Type: "realtime"},
	}
	effects := applyOK(t, reactor, ClientInput{Event: minimal})
	require.Len(t, effects, 1)
	assert.Equal(t, uint64(1), effects[0].VoiceCommand.Sequence)
	require.Len(t, reactor.pendingSessionUpdates, countLimit)

	reconnectEffects := applyOK(t, reactor, ConnectedInput{ProviderSession: json.RawMessage(`{"id":"second"}`)})
	require.Len(t, reconnectEffects, 2)
	assert.Empty(t, reactor.pendingSessionUpdates)

	beforeLateCompletion := mutationView(reactor)
	assert.Empty(t, applyOK(t, reactor, VoiceCommandCompletedInput{Sequence: 1, Kind: realtime.CommandSessionUpdate}))
	assert.Equal(t, beforeLateCompletion, mutationView(reactor))

	exactBoundary := exactSizeSessionUpdate(t, byteLimit)
	effects = applyOK(t, reactor, ClientInput{Event: exactBoundary})
	require.Len(t, effects, 1)
	boundarySequence := effects[0].VoiceCommand.Sequence
	assert.Equal(t, uint64(2), boundarySequence)
	assert.Empty(t, applyOK(t, reactor, VoiceCommandCompletedInput{
		Sequence: boundarySequence,
		Kind:     realtime.CommandSessionUpdate,
	}))
	applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventSessionUpdated, ProviderEventID: "provider-ack",
		SessionUpdateAcknowledgement: true, SessionUpdateSequence: boundarySequence,
	}))

	applyOK(t, reactor, ClientInput{Event: minimal})
	beforeOverflow := mutationView(reactor)
	effects, err := reactor.Apply(ClientInput{Event: minimal})
	require.EqualError(t, err, wantLimitError)
	require.ErrorIs(t, err, ErrPendingSessionUpdateLimit)
	assert.Empty(t, effects)
	assert.Equal(t, beforeOverflow, mutationView(reactor))
}

func TestControlAndTranscriptMappingsUseTypedDTOsAndNonemptyEventIDs(t *testing.T) {
	reactor, _ := testReactor(1)
	epoch := startStreaming(t, reactor, "response-1")
	openManualInputTranscription(t, reactor, "user-item")

	tests := []struct {
		name      string
		event     realtime.Event
		wantType  string
		checkType func(*testing.T, protocol.ServerEvent)
	}{
		{
			name:     "input transcript delta",
			event:    realtime.Event{Kind: realtime.EventTranscriptionDelta, ItemID: "user-item", Text: "hel"},
			wantType: "conversation.item.input_audio_transcription.delta",
			checkType: func(t *testing.T, event protocol.ServerEvent) {
				mapped := event.(protocol.ConversationItemInputAudioTranscriptionDeltaEvent)
				assert.Equal(t, "hel", mapped.Delta)
			},
		},
		{
			name:     "input transcript completed",
			event:    realtime.Event{Kind: realtime.EventTranscriptionCompleted, ItemID: "user-item", Text: "hello"},
			wantType: "conversation.item.input_audio_transcription.completed",
			checkType: func(t *testing.T, event protocol.ServerEvent) {
				mapped := event.(protocol.ConversationItemInputAudioTranscriptionCompletedEvent)
				assert.Equal(t, "hello", mapped.Transcript)
			},
		},
		{
			name:     "audio done is not response terminal",
			event:    realtime.Event{Kind: realtime.EventAudioDone, Epoch: epoch, ResponseID: "response-1", ItemID: "assistant"},
			wantType: "response.output_audio.done",
			checkType: func(t *testing.T, event protocol.ServerEvent) {
				assert.IsType(t, protocol.ResponseOutputAudioDoneEvent{}, event)
			},
		},
		{
			name:     "transcript done is not response terminal",
			event:    realtime.Event{Kind: realtime.EventTranscriptDone, Epoch: epoch, ResponseID: "response-1", ItemID: "assistant", Text: "answer"},
			wantType: "response.output_audio_transcript.done",
			checkType: func(t *testing.T, event protocol.ServerEvent) {
				mapped := event.(protocol.ResponseOutputAudioTranscriptDoneEvent)
				assert.Equal(t, "answer", mapped.Transcript)
			},
		},
		{
			name: "ordinary tool call",
			event: realtime.Event{
				Kind: realtime.EventFunctionCallDone, Epoch: epoch, ResponseID: "response-1", ItemID: "tool-item",
				CallID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"q":"value"}`),
			},
			wantType: "response.function_call_arguments.done",
			checkType: func(t *testing.T, event protocol.ServerEvent) {
				mapped := event.(protocol.ResponseFunctionCallArgumentsDoneEvent)
				assert.Equal(t, `{"q":"value"}`, mapped.Arguments)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			effects := applyOK(t, reactor, currentVoice(reactor, test.event))
			require.Len(t, effects, 1)
			require.NotNil(t, effects[0].ClientEvent)
			assert.Equal(t, test.wantType, effects[0].ClientEvent.EventType())
			test.checkType(t, effects[0].ClientEvent)
			encoded, err := protocol.Encode(effects[0].ClientEvent)
			require.NoError(t, err)
			var envelope struct {
				EventID string `json:"event_id"`
			}
			require.NoError(t, json.Unmarshal(encoded, &envelope))
			assert.NotEmpty(t, envelope.EventID)
			assert.Equal(t, ResponseStreaming, reactor.Snapshot().Response)
		})
	}
}

func TestMalformedMappedVoiceEventsFailWithoutMutation(t *testing.T) {
	type fixture struct {
		reactor *Reactor
		event   realtime.Event
	}
	idle := func(event realtime.Event) func(*testing.T) fixture {
		return func(*testing.T) fixture {
			reactor, _ := testReactor(1)
			return fixture{reactor: reactor, event: event}
		}
	}
	providerIdle := func(event realtime.Event) func(*testing.T) fixture {
		return func(*testing.T) fixture {
			reactor, _ := providerVADTestReactor(1)
			return fixture{reactor: reactor, event: event}
		}
	}
	pending := func(event realtime.Event) func(*testing.T) fixture {
		return func(t *testing.T) fixture {
			reactor, _ := testReactor(1)
			epoch, _ := createResponse(t, reactor)
			event.Epoch = epoch
			return fixture{reactor: reactor, event: event}
		}
	}
	streaming := func(event realtime.Event) func(*testing.T) fixture {
		return func(t *testing.T) fixture {
			reactor, _ := testReactor(1)
			epoch := startStreaming(t, reactor, "response-1")
			event.Epoch = epoch
			if event.ResponseID == "current" {
				event.ResponseID = "response-1"
			}
			return fixture{reactor: reactor, event: event}
		}
	}
	tests := []struct {
		name  string
		setup func(*testing.T) fixture
	}{
		{name: "speech started item ID", setup: providerIdle(realtime.Event{Kind: realtime.EventSpeechStarted})},
		{
			name: "speech stopped item ID",
			setup: func(t *testing.T) fixture {
				reactor, _ := providerVADTestReactor(1)
				applyOK(t, reactor, currentVoice(reactor, realtime.Event{Kind: realtime.EventSpeechStarted, ItemID: "turn-1"}))
				return fixture{reactor: reactor, event: realtime.Event{Kind: realtime.EventSpeechStopped}}
			},
		},
		{name: "transcription delta item ID", setup: idle(realtime.Event{Kind: realtime.EventTranscriptionDelta, Text: "delta"})},
		{name: "transcription completed item ID", setup: idle(realtime.Event{Kind: realtime.EventTranscriptionCompleted, Text: "done"})},
		{name: "explicit created response ID", setup: pending(realtime.Event{Kind: realtime.EventResponseCreated, Status: "in_progress"})},
		{name: "explicit created status", setup: pending(realtime.Event{Kind: realtime.EventResponseCreated, ResponseID: "response-1"})},
		{name: "implicit created response ID", setup: idle(realtime.Event{Kind: realtime.EventResponseCreated, Status: "in_progress", NeedsEpochBinding: true})},
		{name: "implicit created status", setup: idle(realtime.Event{Kind: realtime.EventResponseCreated, ResponseID: "implicit", NeedsEpochBinding: true})},
		{name: "audio delta response ID", setup: streaming(realtime.Event{Kind: realtime.EventAudioDelta, ItemID: "assistant-1", Audio: []byte{1}})},
		{name: "audio delta item ID", setup: streaming(realtime.Event{Kind: realtime.EventAudioDelta, ResponseID: "current", Audio: []byte{1}})},
		{name: "audio delta payload", setup: streaming(realtime.Event{Kind: realtime.EventAudioDelta, ResponseID: "current", ItemID: "assistant-1"})},
		{name: "transcript delta response ID", setup: streaming(realtime.Event{Kind: realtime.EventTranscriptDelta, ItemID: "assistant-1", Text: "delta"})},
		{name: "transcript delta item ID", setup: streaming(realtime.Event{Kind: realtime.EventTranscriptDelta, ResponseID: "current", Text: "delta"})},
		{name: "audio done response ID", setup: streaming(realtime.Event{Kind: realtime.EventAudioDone, ItemID: "assistant-1"})},
		{name: "audio done item ID", setup: streaming(realtime.Event{Kind: realtime.EventAudioDone, ResponseID: "current"})},
		{name: "transcript done response ID", setup: streaming(realtime.Event{Kind: realtime.EventTranscriptDone, ItemID: "assistant-1"})},
		{name: "transcript done item ID", setup: streaming(realtime.Event{Kind: realtime.EventTranscriptDone, ResponseID: "current"})},
		{name: "function response ID", setup: streaming(realtime.Event{Kind: realtime.EventFunctionCallDone, ItemID: "tool-1", CallID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{}`)})},
		{name: "function item ID", setup: streaming(realtime.Event{Kind: realtime.EventFunctionCallDone, ResponseID: "current", CallID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{}`)})},
		{name: "function call ID", setup: streaming(realtime.Event{Kind: realtime.EventFunctionCallDone, ResponseID: "current", ItemID: "tool-1", Name: "lookup", Arguments: json.RawMessage(`{}`)})},
		{name: "function name", setup: streaming(realtime.Event{Kind: realtime.EventFunctionCallDone, ResponseID: "current", ItemID: "tool-1", CallID: "call-1", Arguments: json.RawMessage(`{}`)})},
		{name: "function arguments", setup: streaming(realtime.Event{Kind: realtime.EventFunctionCallDone, ResponseID: "current", ItemID: "tool-1", CallID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"broken":`)})},
		{name: "response done response ID", setup: streaming(realtime.Event{Kind: realtime.EventResponseDone, Status: "completed"})},
		{name: "response done status", setup: streaming(realtime.Event{Kind: realtime.EventResponseDone, ResponseID: "current"})},
		{name: "provider error reason", setup: streaming(realtime.Event{Kind: realtime.EventError, ResponseID: "current"})},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := test.setup(t)
			before := mutationView(fixture.reactor)
			effects, err := fixture.reactor.Apply(currentVoice(fixture.reactor, fixture.event))
			require.Error(t, err)
			assert.Empty(t, effects)
			assert.Equal(t, before, mutationView(fixture.reactor))
		})
	}
}

func TestDurableSequenceHighWaterRejectsGapsWithoutMutation(t *testing.T) {
	reactor, fence := testReactor(8)
	assert.Empty(t, applyOK(t, reactor, DurableInput{Sequence: 1, Kind: "opaque", Payload: json.RawMessage(`{"value":1}`)}))
	assert.Equal(t, uint64(1), reactor.Snapshot().LastDurableSeq)

	assert.Empty(t, applyOK(t, reactor, DurableInput{Sequence: 1, Kind: "different-replay", Payload: json.RawMessage(`not-json`)}))
	assert.Equal(t, uint64(1), reactor.Snapshot().LastDurableSeq)

	before := reactor.Snapshot()
	effects, err := reactor.Apply(DurableInput{Sequence: 3, Kind: "opaque", Payload: json.RawMessage(`{"value":3}`)})
	require.ErrorIs(t, err, ErrDurableSequenceGap)
	assert.Empty(t, effects)
	assertSnapshotAndEpochUnchanged(t, before, 8, reactor)

	assert.Empty(t, applyOK(t, reactor, DurableInput{Sequence: 2, Kind: "opaque", Payload: json.RawMessage(`{"value":2}`)}))
	assert.Equal(t, uint64(2), reactor.Snapshot().LastDurableSeq)
	assert.Equal(t, identity.ResponseEpoch(8), fence.Current())
}

func TestFinalTranscriptsEmitTypedPersistenceOnlyAtCompletion(t *testing.T) {
	reactor, _ := testReactor(1)
	openManualInputTranscription(t, reactor, "user-turn")
	applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventTranscriptionDelta, ItemID: "user-turn", Text: "par",
	}))

	effects := applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventTranscriptionCompleted, ItemID: "user-turn", Text: "final user text",
	}))
	require.Len(t, effects, 1)
	require.NotNil(t, effects[0].Transcript)
	assert.Equal(t, TranscriptRequestedEffect{
		Turn: identity.TurnKey{ID: "user-turn", Revision: 1}, Sequence: 1,
		Role: "user", FinalText: "final user text",
	}, *effects[0].Transcript)

	epoch := startStreaming(t, reactor, "response-final")
	effects = applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventTranscriptDone, Epoch: epoch, ResponseID: "response-final",
		ItemID: "assistant-item", Text: "final assistant text",
	}))
	require.Len(t, effects, 1)
	require.NotNil(t, effects[0].Transcript)
	assert.Equal(t, "assistant", effects[0].Transcript.Role)
	assert.Equal(t, "final assistant text", effects[0].Transcript.FinalText)
	assert.Equal(t, uint64(2), effects[0].Transcript.Sequence)
	assert.Equal(t, identity.TurnKey{ID: "user-turn", Revision: 1}, effects[0].Transcript.Turn)
}

func TestIdenticalFinalTranscriptReplayDoesNotAllocateRevisionOrDurableWork(t *testing.T) {
	reactor, _ := testReactor(1)
	openManualInputTranscription(t, reactor, "user-replay")
	event := realtime.Event{Kind: realtime.EventTranscriptionCompleted, ItemID: "user-replay", Text: "same final"}
	first := applyOK(t, reactor, currentVoice(reactor, event))
	require.NotNil(t, first[0].Transcript)
	turn := first[0].Transcript.Turn

	second := applyOK(t, reactor, currentVoice(reactor, event))
	assert.Empty(t, second)
	assert.Equal(t, turn, reactor.activeTurn.key)
}

func TestTypedTurnCommitAttachesStartToExactTranscript(t *testing.T) {
	reactor, _ := testReactor(1)
	epoch := startStreaming(t, reactor, "response-start")
	openManualInputTranscription(t, reactor, "turn-exact")
	effects := applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventTranscriptionCompleted, ItemID: "turn-exact", Text: "trusted transcript",
	}))
	require.NotNil(t, effects[0].Transcript)
	turn := effects[0].Transcript.Turn

	effects = applyOK(t, reactor, currentVoice(reactor, backgroundToolEvent(
		epoch, "response-start", "call-start", json.RawMessage(`{"objective":"investigate"}`),
	)))
	require.Len(t, effects, 1, "start waits for committed transcript")

	effects = applyOK(t, reactor, TurnCommittedInput{
		Sequence: 1, Turn: turn, Transcript: "trusted transcript",
	})
	require.Len(t, effects, 1)
	require.NotNil(t, effects[0].Handoff)
	assert.Equal(t, HandoffRequestedEffect{
		OriginEpoch: epoch, ResponseID: "response-start", CallID: "call-start", ToolName: StartBackgroundTaskTool,
		Arguments: json.RawMessage(`{"objective":"investigate"}`), Turn: turn,
		Transcript: "trusted transcript",
	}, *effects[0].Handoff)

	assert.Empty(t, applyOK(t, reactor, TurnCommittedInput{
		Sequence: 1, Turn: turn, Transcript: "mutated replay",
	}), "durable replay is ignored before payload inspection")
}

func TestReservedHandoffToolsExcludeOrdinaryCalls(t *testing.T) {
	reactor, _ := testReactor(1)
	epoch := startStreaming(t, reactor, "response-tools")

	tests := []struct {
		name string
		args json.RawMessage
		want bool
	}{
		{name: "lookup", args: json.RawMessage(`{"q":"x"}`), want: false},
		{name: SteerBackgroundTaskTool, args: json.RawMessage(`{"handoff_id":"h-1","objective":"new"}`), want: true},
		{name: CancelBackgroundTaskTool, args: json.RawMessage(`{"handoff_id":"h-1"}`), want: true},
	}
	for index, test := range tests {
		effects := applyOK(t, reactor, currentVoice(reactor, realtime.Event{
			Kind: realtime.EventFunctionCallDone, Epoch: epoch, ResponseID: "response-tools",
			ItemID: fmt.Sprintf("tool-%d", index), CallID: fmt.Sprintf("call-%d", index),
			Name: test.name, Arguments: test.args,
		}))
		require.Len(t, effects, 1)
		if test.want {
			require.NotNil(t, effects[0].Handoff)
			assert.Equal(t, test.name, effects[0].Handoff.ToolName)
		} else {
			assert.Nil(t, effects[0].Handoff)
		}
	}
}

func TestSteerAndCancelCarryExactCommittedTurnAndRejectUnfencedCalls(t *testing.T) {
	reactor, _ := testReactor(1)
	epoch := startStreaming(t, reactor, "response-tools-fenced")
	unfenced := applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventFunctionCallDone, Epoch: epoch, ResponseID: "response-tools-fenced",
		ItemID: "tool-unfenced", CallID: "call-unfenced", Name: SteerBackgroundTaskTool,
		Arguments: json.RawMessage(`{"handoff_id":"h-1","objective":"new"}`),
	}))
	require.NotNil(t, unfenced[0].Handoff)
	assert.Equal(t, "missing_committed_turn", unfenced[0].Handoff.RejectReason)

	turn := identity.TurnKey{ID: "turn-tools", Revision: 1}
	applyOK(t, reactor, TurnCommittedInput{Sequence: 1, Turn: turn, Transcript: "trusted request"})
	for index, tool := range []struct {
		name string
		args json.RawMessage
	}{
		{name: SteerBackgroundTaskTool, args: json.RawMessage(`{"handoff_id":"h-1","objective":"revised"}`)},
		{name: CancelBackgroundTaskTool, args: json.RawMessage(`{"handoff_id":"h-1","reason":"stop"}`)},
	} {
		effects := applyOK(t, reactor, currentVoice(reactor, realtime.Event{
			Kind: realtime.EventFunctionCallDone, Epoch: epoch, ResponseID: "response-tools-fenced",
			ItemID: fmt.Sprintf("tool-fenced-%d", index), CallID: fmt.Sprintf("call-fenced-%d", index),
			Name: tool.name, Arguments: tool.args,
		}))
		require.NotNil(t, effects[0].Handoff)
		assert.Equal(t, turn, effects[0].Handoff.Turn)
		assert.Equal(t, "trusted request", effects[0].Handoff.Transcript)
		assert.Equal(t, epoch, effects[0].Handoff.OriginEpoch)
	}
}

func TestLateCompletionCannotRebindAfterResponseAndCallIDReuse(t *testing.T) {
	reactor, _ := testReactor(1)
	turn := identity.TurnKey{ID: "turn-reuse", Revision: 1}
	applyOK(t, reactor, TurnCommittedInput{Sequence: 1, Turn: turn, Transcript: "request"})
	firstEpoch := startStreaming(t, reactor, "response-reused")
	applyOK(t, reactor, currentVoice(reactor, backgroundToolEvent(firstEpoch, "response-reused", "call-reused", json.RawMessage(`{"objective":"first"}`))))
	applyOK(t, reactor, currentVoice(reactor, realtime.Event{Kind: realtime.EventResponseDone, Epoch: firstEpoch, ResponseID: "response-reused", Status: "completed"}))
	first := applyOK(t, reactor, HandoffAcceptedInput{
		OriginEpoch: firstEpoch, Turn: turn, ResponseID: "response-reused", CallID: "call-reused", Accepted: true, HandoffID: "h-first",
	})
	require.Len(t, first, 2)
	acceptanceEpoch := first[1].VoiceCommand.Epoch
	applyOK(t, reactor, currentVoice(reactor, realtime.Event{Kind: realtime.EventResponseCreated, Epoch: acceptanceEpoch, ResponseID: "acceptance-1", Status: "in_progress"}))
	applyOK(t, reactor, currentVoice(reactor, realtime.Event{Kind: realtime.EventResponseDone, Epoch: acceptanceEpoch, ResponseID: "acceptance-1", Status: "completed"}))

	secondEpoch := startStreaming(t, reactor, "response-reused")
	applyOK(t, reactor, currentVoice(reactor, backgroundToolEvent(secondEpoch, "response-reused", "call-reused", json.RawMessage(`{"objective":"second"}`))))
	applyOK(t, reactor, currentVoice(reactor, realtime.Event{Kind: realtime.EventResponseDone, Epoch: secondEpoch, ResponseID: "response-reused", Status: "completed"}))
	assert.Empty(t, applyOK(t, reactor, HandoffAcceptedInput{
		OriginEpoch: firstEpoch, Turn: turn, ResponseID: "response-reused", CallID: "call-reused", Accepted: true, HandoffID: "h-first",
	}))
	second := applyOK(t, reactor, HandoffAcceptedInput{
		OriginEpoch: secondEpoch, Turn: turn, ResponseID: "response-reused", CallID: "call-reused", Accepted: true, HandoffID: "h-second",
	})
	require.Len(t, second, 2)
	var item struct {
		Output string `json:"output"`
	}
	require.NoError(t, json.Unmarshal(second[0].VoiceCommand.Item, &item))
	assert.Contains(t, item.Output, "h-second")
	assert.NotContains(t, item.Output, "h-first")
}

func TestAcceptedHandoffWaitsForOriginThenEmitsOrderedCommandsOnce(t *testing.T) {
	reactor, fence := testReactor(4)
	epoch := startStreaming(t, reactor, "response-origin")
	turn := identity.TurnKey{ID: "turn-accepted", Revision: 1}
	applyOK(t, reactor, TurnCommittedInput{Sequence: 1, Turn: turn, Transcript: "do it"})
	applyOK(t, reactor, currentVoice(reactor, backgroundToolEvent(
		epoch, "response-origin", "call-accepted", json.RawMessage(`{"objective":"do it"}`),
	)))

	assert.Empty(t, applyOK(t, reactor, HandoffAcceptedInput{
		OriginEpoch: epoch, Turn: turn, ResponseID: "response-origin", CallID: "call-accepted", Accepted: true, HandoffID: "handoff-1",
	}), "persistence-first completion waits for the originating response")

	effects := applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventResponseDone, Epoch: epoch, ResponseID: "response-origin", Status: "completed",
	}))
	require.Len(t, effects, 3)
	require.NotNil(t, effects[1].VoiceCommand)
	require.NotNil(t, effects[2].VoiceCommand)
	assert.Equal(t, realtime.CommandCreateItem, effects[1].VoiceCommand.Kind)
	assert.Equal(t, realtime.CommandCreateResponse, effects[2].VoiceCommand.Kind)
	assert.Equal(t, identity.ResponseEpoch(6), effects[2].VoiceCommand.Epoch)
	assert.Equal(t, effects[2].VoiceCommand.Epoch, fence.Current())
	var item struct {
		Type   string `json:"type"`
		CallID string `json:"call_id"`
		Output string `json:"output"`
	}
	require.NoError(t, json.Unmarshal(effects[1].VoiceCommand.Item, &item))
	assert.Equal(t, "function_call_output", item.Type)
	assert.Equal(t, "call-accepted", item.CallID)
	assert.JSONEq(t, `{"accepted":true,"handoff_id":"handoff-1"}`, item.Output)

	assert.Empty(t, applyOK(t, reactor, HandoffAcceptedInput{
		OriginEpoch: epoch, Turn: turn, ResponseID: "response-origin", CallID: "call-accepted", Accepted: true, HandoffID: "handoff-1",
	}), "duplicate completion cannot allocate another epoch")
	assert.Equal(t, identity.ResponseEpoch(6), fence.Current())
}

func TestTerminalFirstHandoffFailureEmitsNoDurableIDAndIgnoresWrongCorrelation(t *testing.T) {
	reactor, fence := testReactor(2)
	epoch := startStreaming(t, reactor, "response-terminal-first")
	turn := identity.TurnKey{ID: "turn-failure", Revision: 1}
	applyOK(t, reactor, TurnCommittedInput{Sequence: 1, Turn: turn, Transcript: "request"})
	applyOK(t, reactor, currentVoice(reactor, backgroundToolEvent(
		epoch, "response-terminal-first", "call-failure", json.RawMessage(`{"objective":"request"}`),
	)))
	applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventResponseDone, Epoch: epoch, ResponseID: "response-terminal-first", Status: "completed",
	}))

	assert.Empty(t, applyOK(t, reactor, HandoffAcceptedInput{
		OriginEpoch: epoch, Turn: turn, ResponseID: "response-terminal-first", CallID: "unknown", Accepted: false, Reason: "rejected",
	}))
	assert.Equal(t, identity.ResponseEpoch(3), fence.Current())

	effects := applyOK(t, reactor, HandoffAcceptedInput{
		OriginEpoch: epoch, Turn: turn, ResponseID: "response-terminal-first", CallID: "call-failure", Accepted: false, Reason: "rejected",
	})
	require.Len(t, effects, 2)
	assert.Equal(t, realtime.CommandCreateItem, effects[0].VoiceCommand.Kind)
	assert.Equal(t, realtime.CommandCreateResponse, effects[1].VoiceCommand.Kind)
	var item struct {
		Output string `json:"output"`
	}
	require.NoError(t, json.Unmarshal(effects[0].VoiceCommand.Item, &item))
	assert.JSONEq(t, `{"accepted":false,"reason":"rejected"}`, item.Output)
	assert.NotContains(t, item.Output, "handoff_id")
	assert.Equal(t, identity.ResponseEpoch(4), effects[1].VoiceCommand.Epoch)
}

func TestDisabledHandoffAcceptanceEmitsContentFreeDegradedWithoutClosingVoice(t *testing.T) {
	reactor, _ := testReactor(2)
	epoch := startStreaming(t, reactor, "response-disabled")
	turn := identity.TurnKey{ID: "turn-disabled", Revision: 1}
	applyOK(t, reactor, TurnCommittedInput{Sequence: 1, Turn: turn, Transcript: "request"})
	applyOK(t, reactor, currentVoice(reactor, backgroundToolEvent(epoch, "response-disabled", "call-disabled", json.RawMessage(`{"objective":"request"}`))))
	applyOK(t, reactor, currentVoice(reactor, realtime.Event{Kind: realtime.EventResponseDone, Epoch: epoch, ResponseID: "response-disabled", Status: "completed"}))
	effects := applyOK(t, reactor, HandoffAcceptedInput{
		OriginEpoch: epoch, Turn: turn, ResponseID: "response-disabled", CallID: "call-disabled", Accepted: false, Reason: "handoffs_disabled",
	})
	require.Len(t, effects, 3)
	require.NotNil(t, effects[0].VoiceCommand)
	require.NotNil(t, effects[1].VoiceCommand)
	degraded, ok := effects[2].ClientEvent.(protocol.LiveSessionDegradedEvent)
	require.True(t, ok)
	assert.Equal(t, "live.session.degraded", degraded.Type)
	assert.JSONEq(t, `{"code":"handoffs_disabled"}`, string(degraded.Details))
	assert.Nil(t, effects[2].Close)
}

func TestSuppressedHandoffAcceptanceCreateResponseClearsPendingAndAllowsNormalVoice(t *testing.T) {
	reactor, _ := testReactor(2)
	epoch := startStreaming(t, reactor, "response-shadow-acceptance")
	turn := identity.TurnKey{ID: "turn-shadow-acceptance", Revision: 1}
	applyOK(t, reactor, TurnCommittedInput{Sequence: 1, Turn: turn, Transcript: "request"})
	applyOK(t, reactor, currentVoice(reactor, backgroundToolEvent(epoch, "response-shadow-acceptance", "call-shadow-acceptance", json.RawMessage(`{"objective":"request"}`))))
	applyOK(t, reactor, currentVoice(reactor, realtime.Event{Kind: realtime.EventResponseDone, Epoch: epoch, ResponseID: "response-shadow-acceptance", Status: "completed"}))
	accepted := HandoffAcceptedInput{
		OriginEpoch: epoch, Turn: turn, ResponseID: "response-shadow-acceptance", CallID: "call-shadow-acceptance", Accepted: true, HandoffID: "handoff-shadow-acceptance",
	}
	effects := applyOK(t, reactor, accepted)
	require.Len(t, effects, 2)
	response := effects[1].VoiceCommand
	require.NotNil(t, response)
	assert.Equal(t, ResponsePending, reactor.Snapshot().Response)
	assert.Empty(t, applyOK(t, reactor, VoiceCommandCompletedInput{Sequence: response.Sequence, Kind: response.Kind, Suppressed: true}))
	assert.Equal(t, ResponseTerminal, reactor.Snapshot().Response)
	assert.Empty(t, applyOK(t, reactor, accepted), "durable acceptance remains resolved after presentation suppression")
	normal := applyOK(t, reactor, ClientInput{Event: protocol.ResponseCreateEvent{Type: "response.create"}})
	require.Len(t, normal, 1)
	assert.Equal(t, realtime.CommandCreateResponse, normal[0].VoiceCommand.Kind)
}

func TestPendingAcceptanceTerminalPathsDrainNextReadyCompletion(t *testing.T) {
	setup := func(t *testing.T, connect bool) (*Reactor, identity.ResponseEpoch) {
		t.Helper()
		reactor, _ := testReactor(1)
		if connect {
			applyOK(t, reactor, ConnectedInput{ProviderSession: json.RawMessage(`{}`)})
		}
		turn := identity.TurnKey{ID: "turn-drain", Revision: 1}
		applyOK(t, reactor, TurnCommittedInput{Sequence: 1, Turn: turn, Transcript: "request"})
		epoch := startStreaming(t, reactor, "response-drain-origin")
		for _, callID := range []string{"call-drain-a", "call-drain-b"} {
			applyOK(t, reactor, currentVoice(reactor, backgroundToolEvent(epoch, "response-drain-origin", callID, json.RawMessage(`{"objective":"work"}`))))
			applyOK(t, reactor, HandoffAcceptedInput{
				OriginEpoch: epoch, Turn: turn, ResponseID: "response-drain-origin", CallID: callID, Accepted: true, HandoffID: identity.HandoffID("h-" + callID),
			})
		}
		effects := applyOK(t, reactor, currentVoice(reactor, realtime.Event{
			Kind: realtime.EventResponseDone, Epoch: epoch, ResponseID: "response-drain-origin", Status: "completed",
		}))
		require.Len(t, effects, 3, "first acceptance starts its response")
		return reactor, effects[2].VoiceCommand.Epoch
	}

	tests := []struct {
		name    string
		connect bool
		apply   func(*testing.T, *Reactor, identity.ResponseEpoch) []Effect
	}{
		{
			name: "provider error before response created",
			apply: func(t *testing.T, reactor *Reactor, epoch identity.ResponseEpoch) []Effect {
				return applyOK(t, reactor, currentVoice(reactor, realtime.Event{Kind: realtime.EventError, Epoch: epoch, Reason: "failed"}))
			},
		},
		{
			name: "empty response id cancel",
			apply: func(t *testing.T, reactor *Reactor, _ identity.ResponseEpoch) []Effect {
				return applyOK(t, reactor, ClientInput{Event: protocol.ResponseCancelEvent{Type: "response.cancel"}})
			},
		},
		{
			name:    "reconnect terminalization",
			connect: true,
			apply: func(t *testing.T, reactor *Reactor, _ identity.ResponseEpoch) []Effect {
				return applyOK(t, reactor, ConnectedInput{ProviderSession: json.RawMessage(`{}`)})
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reactor, acceptanceEpoch := setup(t, test.connect)
			effects := test.apply(t, reactor, acceptanceEpoch)
			commands := make([]realtime.CommandKind, 0, 2)
			for _, effect := range effects {
				if effect.VoiceCommand != nil {
					commands = append(commands, effect.VoiceCommand.Kind)
				}
			}
			require.GreaterOrEqual(t, len(commands), 2)
			assert.Equal(t, []realtime.CommandKind{realtime.CommandCreateItem, realtime.CommandCreateResponse}, commands[len(commands)-2:])
		})
	}
}

func TestDurableWorkByteBoundariesCoverImmediateReservedToolsTranscriptsAndIDs(t *testing.T) {
	reservedArgs := func(name string, size int) json.RawMessage {
		var prefix, suffix string
		switch name {
		case StartBackgroundTaskTool:
			prefix, suffix = `{"objective":"`, `"}`
		case SteerBackgroundTaskTool:
			prefix, suffix = `{"handoff_id":"h","objective":"`, `"}`
		case CancelBackgroundTaskTool:
			prefix, suffix = `{"handoff_id":"h","reason":"`, `"}`
		}
		return json.RawMessage(prefix + strings.Repeat("x", size-len(prefix)-len(suffix)) + suffix)
	}
	for _, name := range []string{StartBackgroundTaskTool, SteerBackgroundTaskTool, CancelBackgroundTaskTool} {
		t.Run(name, func(t *testing.T) {
			for _, size := range []int{DurableArgumentsBytes, DurableArgumentsBytes + 1} {
				reactor, _ := testReactor(1)
				turn := identity.TurnKey{ID: "turn-bounds", Revision: 1}
				applyOK(t, reactor, TurnCommittedInput{Sequence: 1, Turn: turn, Transcript: "request"})
				epoch := startStreaming(t, reactor, "response-bounds")
				effects, err := reactor.Apply(currentVoice(reactor, realtime.Event{
					Kind: realtime.EventFunctionCallDone, Epoch: epoch, ResponseID: "response-bounds",
					ItemID: "tool-bounds", CallID: "call-bounds", Name: name, Arguments: reservedArgs(name, size),
				}))
				if size == DurableArgumentsBytes {
					require.NoError(t, err)
					require.NotEmpty(t, effects)
				} else {
					require.ErrorIs(t, err, ErrPendingToolLimit)
					assert.Empty(t, effects)
				}
			}
		})
	}

	for _, size := range []int{DurableTranscriptBytes, DurableTranscriptBytes + 1} {
		reactor, _ := testReactor(1)
		openManualInputTranscription(t, reactor, "turn-transcript-bound")
		effects, err := reactor.Apply(currentVoice(reactor, realtime.Event{
			Kind: realtime.EventTranscriptionCompleted, ItemID: "turn-transcript-bound", Text: strings.Repeat("t", size),
		}))
		if size == DurableTranscriptBytes {
			require.NoError(t, err)
			require.NotNil(t, effects[0].Transcript)
		} else {
			require.Error(t, err)
			assert.Empty(t, effects)
		}
	}

	reactor, _ := testReactor(1)
	turn := identity.TurnKey{ID: "turn-id-bound", Revision: 1}
	applyOK(t, reactor, TurnCommittedInput{Sequence: 1, Turn: turn, Transcript: "request"})
	epoch := startStreaming(t, reactor, "response-id-bound")
	_, err := reactor.Apply(currentVoice(reactor, realtime.Event{
		Kind: realtime.EventFunctionCallDone, Epoch: epoch, ResponseID: "response-id-bound", ItemID: "tool",
		CallID: strings.Repeat("c", DurableIdentifierBytes+1), Name: CancelBackgroundTaskTool,
		Arguments: json.RawMessage(`{"handoff_id":"h"}`),
	}))
	require.Error(t, err)
}

func TestFinalAssistantReplacementRemainsByteAccountedAfterEviction(t *testing.T) {
	reactor, _ := testReactor(1)
	turn := identity.TurnKey{ID: "turn-assistant-bounds", Revision: 1}
	applyOK(t, reactor, TurnCommittedInput{Sequence: 1, Turn: turn, Transcript: "request"})
	epoch := startStreaming(t, reactor, "response-assistant-bounds")
	applyDone := func(itemID, text string) {
		applyOK(t, reactor, currentVoice(reactor, realtime.Event{
			Kind: realtime.EventTranscriptDone, Epoch: epoch, ResponseID: "response-assistant-bounds",
			ItemID: itemID, Text: text,
		}))
	}
	applyDone("item-00", "x")
	for index := 1; index < PendingBackgroundToolLimit; index++ {
		applyDone(fmt.Sprintf("item-%02d", index), strings.Repeat("x", DurableTranscriptBytes))
	}
	applyDone("item-00", strings.Repeat("y", DurableTranscriptBytes))

	assert.LessOrEqual(t, reactor.finalizedAssistantBytes, PendingBackgroundToolBytes)
	assert.Len(t, reactor.finalizedAssistantOrder, len(reactor.finalizedAssistantTexts))
	key := "response-assistant-bounds\x00item-00"
	assert.Contains(t, reactor.finalizedAssistantOrder, key)
	calculated := 0
	for storedKey, finalized := range reactor.finalizedAssistantTexts {
		calculated += len(storedKey) + len(finalized.text) + len(finalized.turn.ID)
	}
	assert.Equal(t, calculated, reactor.finalizedAssistantBytes)
}

func TestProviderResponseIDDurableBoundariesArePanicFreeAndAtomic(t *testing.T) {
	for _, implicit := range []bool{false, true} {
		mode := "explicit"
		if implicit {
			mode = "implicit"
		}
		for _, size := range []int{DurableIdentifierBytes, DurableIdentifierBytes + 1} {
			t.Run(fmt.Sprintf("%s/%d", mode, size), func(t *testing.T) {
				reactor, _ := testReactor(1)
				responseID := strings.Repeat("r", size)
				var input VoiceInput
				if implicit {
					input = currentVoice(reactor, realtime.Event{
						Kind: realtime.EventResponseCreated, ResponseID: responseID,
						Status: "in_progress", NeedsEpochBinding: true,
					})
				} else {
					epoch, _ := createResponse(t, reactor)
					input = currentVoice(reactor, realtime.Event{
						Kind: realtime.EventResponseCreated, Epoch: epoch, ResponseID: responseID, Status: "in_progress",
					})
				}
				before := reactor.Snapshot()
				effects, err := reactor.Apply(input)
				if size == DurableIdentifierBytes {
					require.NoError(t, err)
					require.NotEmpty(t, effects)
					return
				}
				require.Error(t, err)
				assert.Empty(t, effects)
				assert.Equal(t, before, reactor.Snapshot())
			})
		}
	}

	for _, size := range []int{DurableIdentifierBytes, DurableIdentifierBytes + 1, PendingBackgroundToolBytes + 1} {
		t.Run(fmt.Sprintf("assistant-transcript/%d", size), func(t *testing.T) {
			reactor, _ := testReactor(1)
			turn := identity.TurnKey{ID: "turn-response-bound", Revision: 1}
			applyOK(t, reactor, TurnCommittedInput{Sequence: 1, Turn: turn, Transcript: "request"})
			responseID := strings.Repeat("r", size)
			reactor.fence.Advance()
			reactor.state.Response = ResponseStreaming
			reactor.activeResponseEpoch = 2
			reactor.activeResponseID = responseID
			before := reactor.Snapshot()
			effects, err := reactor.Apply(currentVoice(reactor, realtime.Event{
				Kind: realtime.EventTranscriptDone, Epoch: 2, ResponseID: responseID,
				ItemID: "assistant-item", Text: "answer",
			}))
			if size == DurableIdentifierBytes {
				require.NoError(t, err)
				require.NotEmpty(t, effects)
				return
			}
			require.Error(t, err)
			assert.Empty(t, effects)
			assert.Equal(t, before, reactor.Snapshot())
		})
	}
}

func committedTurnPayload(turn identity.TurnKey) json.RawMessage {
	raw, err := json.Marshal(struct {
		Turn identity.TurnKey `json:"turn"`
	}{Turn: turn})
	if err != nil {
		panic(err)
	}
	return raw
}

type asyncToolPayload struct {
	ResponseID string           `json:"response_id"`
	CallID     string           `json:"call_id"`
	Name       string           `json:"name"`
	Arguments  json.RawMessage  `json:"arguments"`
	Turn       identity.TurnKey `json:"turn"`
	Reason     string           `json:"reason,omitempty"`
}

func backgroundToolEvent(epoch identity.ResponseEpoch, responseID, callID string, arguments json.RawMessage) realtime.Event {
	return realtime.Event{
		Kind:       realtime.EventFunctionCallDone,
		Epoch:      epoch,
		ResponseID: responseID,
		ItemID:     "tool-" + callID,
		CallID:     callID,
		Name:       StartBackgroundTaskTool,
		Arguments:  arguments,
	}
}

func TestPendingBackgroundToolCountBoundary(t *testing.T) {
	reactor, _ := testReactor(1)
	epoch := startStreaming(t, reactor, "response-1")
	applyOK(t, reactor, DurableInput{
		Sequence: 1, Kind: DurableKindTurnProvisional,
		Payload: committedTurnPayload(identity.TurnKey{ID: "turn-1", Revision: 1}),
	})

	for index := 0; index < PendingBackgroundToolLimit; index++ {
		effects := applyOK(t, reactor, currentVoice(reactor, backgroundToolEvent(
			epoch,
			"response-1",
			fmt.Sprintf("call-%02d", index),
			json.RawMessage(`{"objective":"research"}`),
		)))
		require.Len(t, effects, 1, "pending calls still publish the provider tool event")
		assert.NotNil(t, effects[0].ClientEvent)
		assert.Nil(t, effects[0].Async)
	}

	before := reactor.Snapshot()
	effects, err := reactor.Apply(currentVoice(reactor, backgroundToolEvent(
		epoch,
		"response-1",
		"call-over-limit",
		json.RawMessage(`{"objective":"one too many"}`),
	)))
	require.ErrorIs(t, err, ErrPendingToolLimit)
	assert.Empty(t, effects)
	assert.Equal(t, before, reactor.Snapshot())
}

func exactJSONString(size int) json.RawMessage {
	if size < 2 {
		panic("JSON string size must include two quotes")
	}
	return json.RawMessage(`"` + strings.Repeat("x", size-2) + `"`)
}

func TestPendingBackgroundToolByteBoundaryAndCopyIsolation(t *testing.T) {
	reactor, _ := testReactor(1)
	epoch := startStreaming(t, reactor, "response-1")
	turn := identity.TurnKey{ID: "turn-1", Revision: 9}
	applyOK(t, reactor, DurableInput{Sequence: 1, Kind: DurableKindTurnProvisional, Payload: committedTurnPayload(turn)})
	arguments := exactJSONString(DurableArgumentsBytes)
	effects := applyOK(t, reactor, currentVoice(reactor, backgroundToolEvent(epoch, "response-1", "call-boundary", arguments)))
	require.Len(t, effects, 1)
	arguments[1] = 'Z'

	before := reactor.Snapshot()
	effects, err := reactor.Apply(currentVoice(reactor, backgroundToolEvent(
		epoch, "response-1", "call-over", exactJSONString(DurableArgumentsBytes+1),
	)))
	require.ErrorIs(t, err, ErrPendingToolLimit)
	assert.Empty(t, effects)
	assert.Equal(t, before, reactor.Snapshot())

	effects = applyOK(t, reactor, DurableInput{
		Sequence: 2,
		Kind:     DurableKindTurnCommitted,
		Payload:  committedTurnPayload(turn),
	})
	require.Len(t, effects, 1)
	require.NotNil(t, effects[0].Async)
	assert.Equal(t, AsyncKindStartBackgroundTask, effects[0].Async.Kind)
	var payload asyncToolPayload
	require.NoError(t, json.Unmarshal(effects[0].Async.Payload, &payload))
	assert.Equal(t, "call-boundary", payload.CallID)
	assert.Equal(t, turn, payload.Turn)
	assert.Equal(t, byte('x'), payload.Arguments[1], "queued arguments must not alias provider memory")
}

func TestPendingBackgroundToolsFlushInOrderOnCommit(t *testing.T) {
	reactor, _ := testReactor(1)
	epoch := startStreaming(t, reactor, "response-1")
	turn := identity.TurnKey{ID: "turn-1", Revision: 1}
	applyOK(t, reactor, DurableInput{Sequence: 1, Kind: DurableKindTurnProvisional, Payload: committedTurnPayload(turn)})
	for _, callID := range []string{"call-a", "call-b"} {
		applyOK(t, reactor, currentVoice(reactor, backgroundToolEvent(
			epoch, "response-1", callID, json.RawMessage(`{"value":"`+callID+`"}`),
		)))
	}

	effects := applyOK(t, reactor, DurableInput{
		Sequence: 2,
		Kind:     DurableKindTurnCommitted,
		Payload:  committedTurnPayload(turn),
	})
	require.Len(t, effects, 2)
	for index, callID := range []string{"call-a", "call-b"} {
		require.NotNil(t, effects[index].Async)
		assert.Equal(t, AsyncKindStartBackgroundTask, effects[index].Async.Kind)
		assert.Equal(t, uint64(index+2), effects[index].Async.Sequence, "create response consumed sequence one")
		var payload asyncToolPayload
		require.NoError(t, json.Unmarshal(effects[index].Async.Payload, &payload))
		assert.Equal(t, callID, payload.CallID)
		assert.Equal(t, identity.TurnKey{ID: "turn-1", Revision: 1}, payload.Turn)
	}

	assert.Equal(t, identity.TurnKey{ID: "turn-1", Revision: 1}, *reactor.Snapshot().CommittedTurn)

	// Once committed, a start call no longer occupies the pending queue.
	effects = applyOK(t, reactor, currentVoice(reactor, backgroundToolEvent(
		epoch, "response-1", "call-c", json.RawMessage(`{"value":"c"}`),
	)))
	assert.Equal(t, []string{"client:response.function_call_arguments.done", "async:" + AsyncKindStartBackgroundTask}, effectOrder(effects))
}

func TestPriorCommittedTurnDoesNotAuthorizeNewInputTurnTool(t *testing.T) {
	tests := []struct {
		name        string
		activeTurn  identity.TurnKey
		providerVAD bool
		boundary    func(*testing.T, *Reactor)
	}{
		{
			name:        "speech boundary",
			activeTurn:  identity.TurnKey{ID: "speech-new", Revision: 6},
			providerVAD: true,
			boundary: func(t *testing.T, reactor *Reactor) {
				applyOK(t, reactor, currentVoice(reactor, realtime.Event{Kind: realtime.EventSpeechStarted, ItemID: "speech-new"}))
				applyOK(t, reactor, currentVoice(reactor, realtime.Event{Kind: realtime.EventSpeechStopped, ItemID: "speech-new"}))
			},
		},
		{
			name:       "text conversation item boundary",
			activeTurn: identity.TurnKey{ID: "text-new", Revision: 6},
			boundary: func(t *testing.T, reactor *Reactor) {
				applyOK(t, reactor, ClientInput{Event: protocol.ConversationItemCreateEvent{
					Type: "conversation.item.create",
					Item: protocol.ConversationItem{
						ID:      "text-new",
						Type:    "message",
						Role:    "user",
						Content: json.RawMessage(`[{"type":"input_text","text":"new request"}]`),
					},
				}})
			},
		},
		{
			name:       "transcription boundary",
			activeTurn: identity.TurnKey{ID: "transcript-new", Revision: 6},
			boundary: func(t *testing.T, reactor *Reactor) {
				openManualInputTranscription(t, reactor, "transcript-new")
				applyOK(t, reactor, currentVoice(reactor, realtime.Event{
					Kind: realtime.EventTranscriptionCompleted, ItemID: "transcript-new", Text: "new request",
				}))
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reactor, _ := testReactor(1)
			if test.providerVAD {
				reactor, _ = providerVADTestReactor(1)
			}
			oldTurn := identity.TurnKey{ID: "turn-old", Revision: 5}
			applyOK(t, reactor, DurableInput{Sequence: 1, Kind: DurableKindTurnCommitted, Payload: committedTurnPayload(oldTurn)})
			test.boundary(t, reactor)
			epoch := startStreaming(t, reactor, "response-new")

			effects := applyOK(t, reactor, currentVoice(reactor, backgroundToolEvent(
				epoch, "response-new", "call-new", json.RawMessage(`{"objective":"new"}`),
			)))
			require.Len(t, effects, 1)
			assert.NotNil(t, effects[0].ClientEvent)
			assert.Nil(t, effects[0].Async, "the prior durable turn must not authorize the new turn")
			assert.Equal(t, oldTurn, *reactor.Snapshot().CommittedTurn, "historical committed turn remains observable")

			effects = applyOK(t, reactor, DurableInput{
				Sequence: 2,
				Kind:     DurableKindTurnCommitted,
				Payload:  committedTurnPayload(test.activeTurn),
			})
			require.Len(t, effects, 1)
			require.NotNil(t, effects[0].Async)
			assert.Equal(t, AsyncKindStartBackgroundTask, effects[0].Async.Kind)
			var payload asyncToolPayload
			require.NoError(t, json.Unmarshal(effects[0].Async.Payload, &payload))
			assert.Equal(t, test.activeTurn, payload.Turn)
		})
	}
}

func TestRepeatedTranscriptionCompletionDoesNotAdvanceCapturedRevision(t *testing.T) {
	reactor, _ := testReactor(1)
	epoch := startStreaming(t, reactor, "response-1")
	openManualInputTranscription(t, reactor, "turn-same")
	event := realtime.Event{Kind: realtime.EventTranscriptionCompleted, ItemID: "turn-same", Text: "accepted final"}
	first := applyOK(t, reactor, currentVoice(reactor, event))
	require.Len(t, first, 1)
	require.NotNil(t, first[0].Transcript)
	turn := first[0].Transcript.Turn
	beforeReplay := mutationView(reactor)
	assert.Empty(t, applyOK(t, reactor, currentVoice(reactor, event)))
	assert.Equal(t, beforeReplay, mutationView(reactor))
	applyOK(t, reactor, currentVoice(reactor, backgroundToolEvent(
		epoch, "response-1", "call-revision", json.RawMessage(`{"objective":"latest"}`),
	)))

	effects := applyOK(t, reactor, DurableInput{
		Sequence: 1,
		Kind:     DurableKindTurnCommitted,
		Payload:  committedTurnPayload(turn),
	})
	require.Len(t, effects, 1)
	require.NotNil(t, effects[0].Async)
	assert.Equal(t, AsyncKindStartBackgroundTask, effects[0].Async.Kind)
	var payload asyncToolPayload
	require.NoError(t, json.Unmarshal(effects[0].Async.Payload, &payload))
	assert.Equal(t, identity.TurnKey{ID: "turn-same", Revision: 1}, payload.Turn)
	assert.Empty(t, payload.Reason)
}

func TestSupersededTurnKeyCannotResurrectAfterABAItemSequence(t *testing.T) {
	reactor, _ := testReactor(1)
	epoch := startStreaming(t, reactor, "response-1")
	for index, turn := range []identity.TurnKey{
		{ID: "turn-a", Revision: 1},
		{ID: "turn-b", Revision: 2},
		{ID: "turn-a", Revision: 3},
	} {
		applyOK(t, reactor, DurableInput{
			Sequence: uint64(index + 1), Kind: DurableKindTurnProvisional, Payload: committedTurnPayload(turn),
		})
		if turn.Revision == 1 {
			applyOK(t, reactor, currentVoice(reactor, backgroundToolEvent(
				epoch, "response-1", "call-before-supersession", json.RawMessage(`{"objective":"first"}`),
			)))
		}
	}
	require.NotNil(t, reactor.activeTurn)
	assert.Equal(t, identity.TurnKey{ID: "turn-a", Revision: 3}, reactor.activeTurn.key)

	effects := applyOK(t, reactor, DurableInput{
		Sequence: 4,
		Kind:     DurableKindTurnCommitted,
		Payload:  committedTurnPayload(identity.TurnKey{ID: "turn-a", Revision: 1}),
	})
	require.Len(t, effects, 1)
	require.NotNil(t, effects[0].Async)
	assert.Equal(t, AsyncKindStartBackgroundTaskRejected, effects[0].Async.Kind)
	var rejected toolAsyncPayload
	require.NoError(t, json.Unmarshal(effects[0].Async.Payload, &rejected))
	assert.Equal(t, identity.TurnKey{ID: "turn-a", Revision: 1}, rejected.Turn)
	assert.Equal(t, "superseded_turn_revision", rejected.Reason)

	effects = applyOK(t, reactor, currentVoice(reactor, backgroundToolEvent(
		epoch, "response-1", "call-after-stale-commit", json.RawMessage(`{"objective":"second"}`),
	)))
	require.Len(t, effects, 1)
	assert.Nil(t, effects[0].Async, "an old A:1 commit must not authorize the new A:2 turn")

	effects = applyOK(t, reactor, DurableInput{
		Sequence: 5,
		Kind:     DurableKindTurnCommitted,
		Payload:  committedTurnPayload(identity.TurnKey{ID: "turn-a", Revision: 3}),
	})
	require.Len(t, effects, 1)
	require.NotNil(t, effects[0].Async)
	assert.Equal(t, AsyncKindStartBackgroundTask, effects[0].Async.Kind)
}

func TestStaleDurableProvisionalCannotResurrectSupersededTurn(t *testing.T) {
	reactor, _ := testReactor(1)
	applyOK(t, reactor, DurableInput{Sequence: 1, Kind: DurableKindTurnProvisional,
		Payload: committedTurnPayload(identity.TurnKey{ID: "turn-a", Revision: 1})})
	applyOK(t, reactor, DurableInput{Sequence: 2, Kind: DurableKindTurnProvisional,
		Payload: committedTurnPayload(identity.TurnKey{ID: "turn-b", Revision: 2})})
	require.NotNil(t, reactor.activeTurn)
	assert.Equal(t, identity.TurnKey{ID: "turn-b", Revision: 2}, reactor.activeTurn.key)

	before := mutationView(reactor)
	effects, err := reactor.Apply(DurableInput{
		Sequence: 3,
		Kind:     DurableKindTurnProvisional,
		Payload:  committedTurnPayload(identity.TurnKey{ID: "turn-a", Revision: 1}),
	})
	require.ErrorIs(t, err, ErrTurnRevisionRegression)
	assert.Empty(t, effects)
	assert.Equal(t, before, mutationView(reactor))
}

func TestNewerAndStaleDurableCommitsCannotResurrectOldEligibility(t *testing.T) {
	reactor, _ := testReactor(1)
	turnA := identity.TurnKey{ID: "turn-a", Revision: 1}
	turnB := identity.TurnKey{ID: "turn-b", Revision: 2}
	applyOK(t, reactor, DurableInput{Sequence: 1, Kind: DurableKindTurnCommitted, Payload: committedTurnPayload(turnA)})
	applyOK(t, reactor, DurableInput{Sequence: 2, Kind: DurableKindTurnCommitted, Payload: committedTurnPayload(turnB)})
	require.NotNil(t, reactor.activeTurn)
	assert.Equal(t, turnB, reactor.activeTurn.key)
	assert.True(t, reactor.activeTurn.committed)

	applyOK(t, reactor, DurableInput{Sequence: 3, Kind: DurableKindTurnCommitted, Payload: committedTurnPayload(turnA)})
	require.NotNil(t, reactor.activeTurn)
	assert.Equal(t, turnB, reactor.activeTurn.key, "a stale commit must not restore the older active key")
	currentCommitted := reactor.committedTurnForCurrentRevision()
	require.NotNil(t, currentCommitted)
	assert.Equal(t, turnB, *currentCommitted)

	epoch := startStreaming(t, reactor, "response-1")
	effects := applyOK(t, reactor, currentVoice(reactor, backgroundToolEvent(
		epoch, "response-1", "call-after-stale-commit", json.RawMessage(`{"objective":"current"}`),
	)))
	require.Len(t, effects, 2)
	require.NotNil(t, effects[1].Async)
	assert.Equal(t, AsyncKindStartBackgroundTask, effects[1].Async.Kind)
	var payload toolAsyncPayload
	require.NoError(t, json.Unmarshal(effects[1].Async.Payload, &payload))
	assert.Equal(t, turnB, payload.Turn)
}

func TestStaleCommitPreservesPendingToolForCurrentHighWaterTurn(t *testing.T) {
	reactor, _ := testReactor(1)
	turnA := identity.TurnKey{ID: "turn-a", Revision: 1}
	turnB := identity.TurnKey{ID: "turn-b", Revision: 2}
	applyOK(t, reactor, DurableInput{Sequence: 1, Kind: DurableKindTurnCommitted, Payload: committedTurnPayload(turnA)})
	applyOK(t, reactor, DurableInput{Sequence: 2, Kind: DurableKindTurnProvisional, Payload: committedTurnPayload(turnB)})
	epoch := startStreaming(t, reactor, "response-1")
	applyOK(t, reactor, currentVoice(reactor, backgroundToolEvent(
		epoch, "response-1", "call-current", json.RawMessage(`{"objective":"current"}`),
	)))
	require.Len(t, reactor.pendingTools, 1)

	assert.Empty(t, applyOK(t, reactor, DurableInput{
		Sequence: 3,
		Kind:     DurableKindTurnCommitted,
		Payload:  committedTurnPayload(turnA),
	}))
	require.Len(t, reactor.pendingTools, 1)
	assert.Equal(t, turnB, *reactor.pendingTools[0].turn)

	effects := applyOK(t, reactor, DurableInput{
		Sequence: 4,
		Kind:     DurableKindTurnCommitted,
		Payload:  committedTurnPayload(turnB),
	})
	require.Len(t, effects, 1)
	require.NotNil(t, effects[0].Async)
	assert.Equal(t, AsyncKindStartBackgroundTask, effects[0].Async.Kind)
}

func TestPendingToolRetainsCapturedTurnAndArgumentsAcrossNewItem(t *testing.T) {
	reactor, _ := testReactor(1)
	epoch := startStreaming(t, reactor, "response-1")
	capturedTurn := identity.TurnKey{ID: "turn-a", Revision: 4}
	applyOK(t, reactor, DurableInput{Sequence: 1, Kind: DurableKindTurnProvisional, Payload: committedTurnPayload(capturedTurn)})
	arguments := json.RawMessage(`{"objective":"original"}`)
	applyOK(t, reactor, currentVoice(reactor, backgroundToolEvent(epoch, "response-1", "call-copy", arguments)))
	arguments[14] = 'X'

	newTurn := identity.TurnKey{ID: "turn-b", Revision: 5}
	applyOK(t, reactor, DurableInput{Sequence: 2, Kind: DurableKindTurnProvisional, Payload: committedTurnPayload(newTurn)})
	effects := applyOK(t, reactor, DurableInput{Sequence: 3, Kind: DurableKindTurnCommitted, Payload: committedTurnPayload(newTurn)})
	require.Len(t, effects, 1)
	require.NotNil(t, effects[0].Async)
	assert.Equal(t, AsyncKindStartBackgroundTaskRejected, effects[0].Async.Kind)
	var payload asyncToolPayload
	require.NoError(t, json.Unmarshal(effects[0].Async.Payload, &payload))
	assert.Equal(t, capturedTurn, payload.Turn)
	assert.JSONEq(t, `{"objective":"original"}`, string(payload.Arguments))
	assert.Equal(t, "superseded_turn", payload.Reason)
}

func TestTurnRevisionOverflowRejectsInputBoundariesWithoutMutation(t *testing.T) {
	tests := []struct {
		name        string
		providerVAD bool
		input       func(*testing.T, *Reactor) Input
	}{
		{
			name:        "speech",
			providerVAD: true,
			input: func(_ *testing.T, reactor *Reactor) Input {
				return currentVoice(reactor, realtime.Event{Kind: realtime.EventSpeechStarted, ItemID: "next"})
			},
		},
		{
			name: "manual committed acknowledgement",
			input: func(t *testing.T, reactor *Reactor) Input {
				commit := applyOK(t, reactor, ClientInput{Event: protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}})
				return currentVoice(reactor, realtime.Event{
					Kind: realtime.EventInputAudioCommitted, ItemID: "next",
					InputAudioCommitSequence: commit[0].VoiceCommand.Sequence,
				})
			},
		},
		{
			name: "user conversation item",
			input: func(*testing.T, *Reactor) Input {
				return ClientInput{Event: protocol.ConversationItemCreateEvent{
					Type: "conversation.item.create",
					Item: protocol.ConversationItem{
						ID:      "next",
						Type:    "message",
						Role:    "user",
						Content: json.RawMessage(`[{"type":"input_text","text":"next"}]`),
					},
				}}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reactor, _ := testReactor(1)
			if test.providerVAD {
				reactor, _ = providerVADTestReactor(1)
			}
			applyOK(t, reactor, DurableInput{
				Sequence: 1,
				Kind:     DurableKindTurnProvisional,
				Payload:  committedTurnPayload(identity.TurnKey{ID: "last", Revision: ^uint32(0)}),
			})
			input := test.input(t, reactor)
			before := mutationView(reactor)
			effects, err := reactor.Apply(input)
			require.EqualError(t, err, "session turn revision exhausted")
			assert.Empty(t, effects)
			assert.Equal(t, before, mutationView(reactor))
		})
	}
}

func TestSupersededProvisionalTurnRejectsPendingStartDeterministically(t *testing.T) {
	reactor, _ := testReactor(1)
	epoch := startStreaming(t, reactor, "response-1")
	applyOK(t, reactor, DurableInput{
		Sequence: 1,
		Kind:     DurableKindTurnProvisional,
		Payload:  committedTurnPayload(identity.TurnKey{ID: "turn-1", Revision: 1}),
	})
	applyOK(t, reactor, currentVoice(reactor, backgroundToolEvent(
		epoch, "response-1", "call-superseded", json.RawMessage(`{"objective":"old"}`),
	)))

	effects := applyOK(t, reactor, DurableInput{
		Sequence: 2,
		Kind:     DurableKindTurnCommitted,
		Payload:  committedTurnPayload(identity.TurnKey{ID: "turn-1", Revision: 2}),
	})
	require.Len(t, effects, 1)
	require.NotNil(t, effects[0].Async)
	assert.Equal(t, AsyncKindStartBackgroundTaskRejected, effects[0].Async.Kind)
	var payload asyncToolPayload
	require.NoError(t, json.Unmarshal(effects[0].Async.Payload, &payload))
	assert.Equal(t, "call-superseded", payload.CallID)
	assert.Equal(t, "superseded_turn_revision", payload.Reason)
	assert.Equal(t, identity.TurnKey{ID: "turn-1", Revision: 2}, *reactor.Snapshot().CommittedTurn)
}

func TestOtherToolCallsNeverEnterReservedPendingQueue(t *testing.T) {
	reactor, _ := testReactor(1)
	epoch := startStreaming(t, reactor, "response-1")
	for index := 0; index < PendingBackgroundToolLimit+2; index++ {
		effects := applyOK(t, reactor, currentVoice(reactor, realtime.Event{
			Kind: realtime.EventFunctionCallDone, Epoch: epoch, ResponseID: "response-1", ItemID: "item",
			CallID: fmt.Sprintf("ordinary-%d", index), Name: "lookup", Arguments: json.RawMessage(`{"q":"x"}`),
		}))
		require.Len(t, effects, 1)
		assert.NotNil(t, effects[0].ClientEvent)
		assert.Nil(t, effects[0].Async)
	}

	assert.Empty(t, applyOK(t, reactor, DurableInput{
		Sequence: 1,
		Kind:     DurableKindTurnCommitted,
		Payload:  committedTurnPayload(identity.TurnKey{ID: "turn-1", Revision: 1}),
	}))
}

func TestSnapshotDeepCopiesCommittedTurn(t *testing.T) {
	reactor, _ := testReactor(3)
	turn := identity.TurnKey{ID: "turn-1", Revision: 4}
	applyOK(t, reactor, DurableInput{Sequence: 1, Kind: DurableKindTurnCommitted, Payload: committedTurnPayload(turn)})

	first := reactor.Snapshot()
	require.NotNil(t, first.CommittedTurn)
	first.CommittedTurn.ID = "mutated"
	first.CommittedTurn.Revision = 99

	second := reactor.Snapshot()
	require.NotNil(t, second.CommittedTurn)
	assert.Equal(t, turn, *second.CommittedTurn)
}

func TestProviderErrorAndFailedInputUseDeterministicNonemptyIDs(t *testing.T) {
	reactor, _ := testReactor(1)
	epoch := startStreaming(t, reactor, "response-1")
	effects := applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventError, Epoch: epoch, Reason: "provider exhausted",
	}))
	assert.Equal(t, 1, responseTerminalCount(effects))
	require.Len(t, effects, 2)
	providerError := effects[0].ClientEvent.(protocol.ErrorEvent)
	done := effects[1].ClientEvent.(protocol.ResponseDoneEvent)
	assert.NotEmpty(t, providerError.EventID)
	assert.NotEmpty(t, done.EventID)
	assert.NotEqual(t, providerError.EventID, done.EventID)

	other, _ := testReactor(1)
	effects = applyOK(t, other, FailedInput{Source: "voice", Err: errors.New("socket closed")})
	require.Len(t, effects, 2)
	failed := effects[0].ClientEvent.(protocol.ErrorEvent)
	assert.NotEmpty(t, failed.EventID)
	assert.Contains(t, failed.Error.Message, "socket closed")
	assert.Equal(t, 1011, effects[1].Close.Code)
}

func TestProviderErrorWhilePendingTerminalizesWithoutEmptyResponseDone(t *testing.T) {
	reactor, fence := testReactor(11)
	createResponse(t, reactor)
	epoch := fence.Current()

	effects := applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind:   realtime.EventError,
		Epoch:  epoch,
		Reason: "creation failed",
	}))
	require.Len(t, effects, 1)
	assert.IsType(t, protocol.ErrorEvent{}, effects[0].ClientEvent)
	assert.Zero(t, responseTerminalCount(effects))
	assert.Equal(t, ResponseTerminal, reactor.Snapshot().Response)
	assert.Equal(t, epoch, fence.Current(), "provider error must not advance the epoch")
}

func TestClientAudioAppendIsOwnedByMediaLoop(t *testing.T) {
	reactor, _ := testReactor(1)
	before := reactor.Snapshot()
	effects, err := reactor.Apply(ClientInput{Event: protocol.InputAudioBufferAppendEvent{
		Type: "input_audio_buffer.append", Audio: "AQI=",
	}})
	require.ErrorIs(t, err, ErrMediaInput)
	assert.Empty(t, effects)
	assert.Equal(t, before, reactor.Snapshot())
}

func TestConversationItemCreateMapsTypedItemAndOwnsRawContent(t *testing.T) {
	reactor, _ := testReactor(1)
	content := json.RawMessage(`[{"type":"input_text","text":"hello"}]`)
	effects := applyOK(t, reactor, ClientInput{Event: protocol.ConversationItemCreateEvent{
		Type: "conversation.item.create",
		Item: protocol.ConversationItem{
			ID:      "item-1",
			Type:    "message",
			Role:    "user",
			Content: content,
		},
	}})
	require.Len(t, effects, 1)
	command := effects[0].VoiceCommand
	require.NotNil(t, command)
	assert.Equal(t, realtime.CommandCreateItem, command.Kind)
	assert.Equal(t, uint64(1), command.Sequence)
	content[2] = 'X'
	assert.JSONEq(t, `{"id":"item-1","type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]}`, string(command.Item))
}

func TestUserConversationItemRequiresStableIDWithoutMutation(t *testing.T) {
	reactor, _ := testReactor(1)
	before := mutationView(reactor)
	effects, err := reactor.Apply(ClientInput{Event: protocol.ConversationItemCreateEvent{
		Type: "conversation.item.create",
		Item: protocol.ConversationItem{
			Type:    "message",
			Role:    "user",
			Content: json.RawMessage(`[{"type":"input_text","text":"hello"}]`),
		},
	}})
	require.ErrorContains(t, err, "user conversation item requires an ID")
	assert.Empty(t, effects)
	assert.Equal(t, before, mutationView(reactor))
}

func TestNonUserConversationItemsDoNotInvalidateCommittedTurn(t *testing.T) {
	tests := []struct {
		name string
		item protocol.ConversationItem
	}{
		{
			name: "assistant message",
			item: protocol.ConversationItem{
				ID:      "assistant-item",
				Type:    "message",
				Role:    "assistant",
				Content: json.RawMessage(`[{"type":"text","text":"answer"}]`),
			},
		},
		{
			name: "function call output",
			item: protocol.ConversationItem{
				ID:     "tool-output",
				Type:   "function_call_output",
				CallID: "call-result",
				Output: `{"ok":true}`,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reactor, _ := testReactor(1)
			committed := identity.TurnKey{ID: "user-turn", Revision: 5}
			applyOK(t, reactor, DurableInput{
				Sequence: 1,
				Kind:     DurableKindTurnCommitted,
				Payload:  committedTurnPayload(committed),
			})
			applyOK(t, reactor, ClientInput{Event: protocol.ConversationItemCreateEvent{
				Type: "conversation.item.create",
				Item: test.item,
			}})
			require.NotNil(t, reactor.activeTurn)
			assert.Equal(t, committed, reactor.activeTurn.key)
			assert.True(t, reactor.activeTurn.committed)

			epoch := startStreaming(t, reactor, "response-1")
			effects := applyOK(t, reactor, currentVoice(reactor, backgroundToolEvent(
				epoch, "response-1", "call-next", json.RawMessage(`{"objective":"same turn"}`),
			)))
			require.Len(t, effects, 2)
			require.NotNil(t, effects[1].Async)
			assert.Equal(t, AsyncKindStartBackgroundTask, effects[1].Async.Kind)
		})
	}
}
