package session

import (
	"encoding/json"

	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/protocol"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
)

type Effect struct {
	ClientEvent  protocol.ServerEvent
	VoiceCommand *realtime.Command
	Async        *AsyncEffect
	Transcript   *TranscriptRequestedEffect
	Handoff      *HandoffRequestedEffect
	FlushBefore  identity.ResponseEpoch
	Close        *CloseEffect
}

type TranscriptRequestedEffect struct {
	Turn      identity.TurnKey
	Sequence  uint64
	Role      string
	FinalText string
}

type HandoffRequestedEffect struct {
	OriginEpoch  identity.ResponseEpoch
	ResponseID   string
	CallID       string
	ToolName     string
	Arguments    json.RawMessage
	Turn         identity.TurnKey
	Transcript   string
	RejectReason string
}

type AsyncEffect struct {
	Kind     string
	Sequence uint64
	Payload  json.RawMessage
}

type CloseEffect struct {
	Code   int
	Reason string
}
