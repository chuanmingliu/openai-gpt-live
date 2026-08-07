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
	FlushBefore  identity.ResponseEpoch
	Close        *CloseEffect
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
