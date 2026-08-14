// Package realtime defines provider-neutral live voice contracts.
package realtime

import (
	"context"
	"encoding/json"

	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
)

type CommandKind uint8

const (
	CommandSessionUpdate CommandKind = iota + 1
	CommandAppendAudio
	CommandCommitAudio
	CommandCreateItem
	CommandCreateResponse
	CommandCancelResponse
	CommandBindResponseEpoch
	CommandAcknowledgeAudioCommit
)

type Command struct {
	Kind     CommandKind
	Sequence uint64
	AfterSeq uint64
	Epoch    identity.ResponseEpoch
	// InputItemID is internal audio-commit correlation. CommitAudio sets it when
	// an explicit speech edge established the provider item; AcknowledgeAudioCommit
	// sets it to the exact committed item. It is never serialized onto the wire.
	InputItemID              string
	InputAudioCommitSequence uint64
	Session                  SessionConfig
	Audio                    []byte
	Item                     json.RawMessage
	Response                 ResponseConfig
	ResponseID               string
}

type OpenRequest struct {
	SessionID   identity.SessionID
	Incarnation identity.Incarnation
	Model       string
}

type AudioFormat struct {
	Encoding   string
	SampleRate int
	Channels   int
}

type SessionConfig struct {
	Instructions  string
	InputFormat   AudioFormat
	OutputFormat  AudioFormat
	Voice         string
	Tools         []Tool
	ToolChoice    json.RawMessage
	TurnDetection json.RawMessage
}

type Tool struct {
	Type        string
	Name        string
	Description string
	Parameters  json.RawMessage
}

type ResponseConfig struct {
	Instructions string
	ToolChoice   json.RawMessage
	Metadata     map[string]json.RawMessage
	Input        json.RawMessage
}

type EventKind uint8

const (
	EventSessionCreated EventKind = iota + 1
	EventSessionUpdated
	EventSpeechStarted
	EventSpeechStopped
	EventTranscriptionDelta
	EventTranscriptionCompleted
	EventResponseCreated
	EventAudioDelta
	EventAudioDone
	EventTranscriptDelta
	EventTranscriptDone
	EventFunctionCallDone
	EventResponseDone
	EventError
	EventInputAudioCommitted
)

type Event struct {
	Kind            EventKind
	Epoch           identity.ResponseEpoch
	ProviderEventID string
	ResponseID      string
	ItemID          string
	PreviousItemID  *string
	// InputAudioCommitCandidate is set only when the transport observed this
	// committed event after registering the sole outstanding manual commit.
	// The reactor remains the authority that decides whether it resolves that
	// generation.
	InputAudioCommitCandidate bool
	InputAudioCommitSequence  uint64
	CallID                    string
	Name                      string
	Audio                     []byte
	Text                      string
	Arguments                 json.RawMessage
	Status                    string
	Reason                    string
	Payload                   json.RawMessage
	NeedsEpochBinding         bool
}

type Session interface {
	Send(context.Context, Command) error
	Receive(context.Context) (Event, error)
	Close(context.Context) error
}

type Connector interface {
	Connect(context.Context, OpenRequest) (Session, error)
}
