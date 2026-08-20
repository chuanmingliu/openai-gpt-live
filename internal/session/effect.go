package session

import (
	"encoding/json"

	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/protocol"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/chuanmingliu/openai-gpt-live/internal/toolpolicy"
)

type Effect struct {
	ClientEvent protocol.ServerEvent
	// InputTurnModeBoundary is published before ClientEvent can become
	// externally observable. A zero value carries no boundary transition.
	InputTurnModeBoundary realtime.InputTurnMode
	RequireClientWrite    bool
	PolicySuppressed      bool
	ResponseEpoch         identity.ResponseEpoch
	Incarnation           identity.Incarnation
	VoiceCommand          *realtime.Command
	ProfileBootstrap      *ProfileBootstrapCompletedInput
	PromptRender          *PromptRenderRequestedEffect
	Async                 *AsyncEffect
	Transcript            *TranscriptRequestedEffect
	Handoff               *HandoffRequestedEffect
	FlushBefore           identity.ResponseEpoch
	Close                 *CloseEffect
	HandoffPresentation   bool
}

// PromptRenderRequestedEffect is an immutable, bounded request for one
// finalized input turn. The dispatcher owns a clone before returning from
// admission.
type PromptRenderRequestedEffect struct {
	Incarnation   identity.Incarnation
	SnapshotHash  string
	Epoch         identity.ResponseEpoch
	InputRevision uint32
	Turn          identity.TurnKey
	Transcript    string
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
	ToolPolicy   toolpolicy.Policy
	Profile      handoff.ProfileSnapshot
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
