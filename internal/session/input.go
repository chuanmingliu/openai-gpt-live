package session

import (
	"encoding/json"

	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/protocol"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
)

type Input interface{ isSessionInput() }

type ConnectedInput struct {
	ProviderSession json.RawMessage
	Reconnect       *protocol.ReconnectMetadata
}

type ClientInput struct {
	Event protocol.ClientEvent
}

// VoiceInput identifies the live provider incarnation that produced Event.
// Incarnation must exactly match the reactor's current incarnation. Zero is
// never a bypass, including for provider-neutral events.
type VoiceInput struct {
	Incarnation identity.Incarnation
	Event       realtime.Event
}

type VoiceCommandCompletedInput struct {
	Sequence   uint64
	Kind       realtime.CommandKind
	Suppressed bool
}

type DurableInput struct {
	Sequence uint64
	Kind     string
	Payload  json.RawMessage
}

// TurnCommittedInput acknowledges that the exact finalized user transcript was
// committed durably. Sequence belongs to the per-session durable-work stream,
// not the realtime command stream.
type TurnCommittedInput struct {
	Sequence   uint64
	Turn       identity.TurnKey
	Transcript string
}

// TranscriptPersistedInput advances the durable-work fence for a finalized
// non-user transcript without authorizing a user turn.
type TranscriptPersistedInput struct {
	Sequence uint64
}

// HandoffAcceptedInput reports the durable outcome for one reserved tool call.
// A failed acceptance never carries a durable identifier.
type HandoffAcceptedInput struct {
	OriginEpoch identity.ResponseEpoch
	Turn        identity.TurnKey
	ResponseID  string
	CallID      string
	Accepted    bool
	HandoffID   identity.HandoffID
	Reason      string
}

// HandoffEventInput carries one durable event from the current-session
// directory into the serialized reactor. Scope is trusted server context, not
// provider or client input.
type HandoffEventInput struct {
	Scope handoff.Scope
	Event handoff.Event
}

type FailedInput struct {
	Source string
	Err    error
}

func (ConnectedInput) isSessionInput()             {}
func (ClientInput) isSessionInput()                {}
func (VoiceInput) isSessionInput()                 {}
func (VoiceCommandCompletedInput) isSessionInput() {}
func (DurableInput) isSessionInput()               {}
func (TurnCommittedInput) isSessionInput()         {}
func (TranscriptPersistedInput) isSessionInput()   {}
func (HandoffAcceptedInput) isSessionInput()       {}
func (HandoffEventInput) isSessionInput()          {}
func (FailedInput) isSessionInput()                {}
