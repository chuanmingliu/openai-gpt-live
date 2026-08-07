package session

import (
	"encoding/json"

	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/protocol"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
)

type Input interface{ isSessionInput() }

type ConnectedInput struct {
	ProviderSession json.RawMessage
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
	Sequence uint64
	Kind     realtime.CommandKind
}

type DurableInput struct {
	Sequence uint64
	Kind     string
	Payload  json.RawMessage
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
func (FailedInput) isSessionInput()                {}
