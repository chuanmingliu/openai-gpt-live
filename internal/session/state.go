package session

import (
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
)

type IDs struct {
	SessionID      identity.SessionID
	ConversationID identity.ConversationID
	Incarnation    identity.Incarnation
}

type ResponseState uint8

const (
	ResponseIdle ResponseState = iota
	ResponsePending
	ResponseStreaming
	ResponseCancelRequested
	ResponseTerminal
)

type State struct {
	Response       ResponseState
	UserSpeaking   bool
	CommittedTurn  *identity.TurnKey
	LastDurableSeq uint64
}

type Snapshot struct {
	IDs                     IDs
	Response                ResponseState
	ResponseEpoch           identity.ResponseEpoch
	UserSpeaking            bool
	InputAudioCommitPending bool
	InputTurnMode           realtime.InputTurnMode
	CommittedTurn           *identity.TurnKey
	LastDurableSeq          uint64
}
