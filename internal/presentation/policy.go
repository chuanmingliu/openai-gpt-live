// Package presentation makes deterministic channel-selection decisions for
// durable handoff events. Content safety is deliberately evaluated elsewhere.
package presentation

import (
	"encoding/json"

	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/worker"
)

const defaultMaxSpokenPayloadBytes = 4 << 10

type Context struct {
	UserSpeaking   bool
	ResponseActive bool
}

type Decision struct {
	EmitVisual  bool
	SpeakNow    bool
	DeferSpeech bool
}

type Config struct {
	MaxSpokenPayloadBytes int
}

type Policy interface {
	Decide(Context, handoff.Event) Decision
}

type policy struct {
	maxSpokenPayloadBytes int
}

func NewPolicy(config Config) Policy {
	limit := config.MaxSpokenPayloadBytes
	if limit <= 0 || limit > 64<<10 {
		limit = defaultMaxSpokenPayloadBytes
	}
	return policy{maxSpokenPayloadBytes: limit}
}

func (p policy) Decide(context Context, event handoff.Event) Decision {
	decision := Decision{EmitVisual: true}
	if !speechCandidate(event) || len(event.Payload) > p.maxSpokenPayloadBytes {
		return decision
	}
	if context.UserSpeaking || context.ResponseActive {
		decision.DeferSpeech = true
		return decision
	}
	decision.SpeakNow = true
	return decision
}

func speechCandidate(event handoff.Event) bool {
	if event.Kind == "handoff.needs-input" && event.State == handoff.StateNeedsInput {
		return true
	}
	if event.Kind == "handoff.complete" && event.State == handoff.StateSucceeded {
		return true
	}
	if event.Kind == "handoff.progress" && event.State == handoff.StateRunning {
		var record worker.ProgressRecord
		return json.Unmarshal(event.Payload, &record) == nil && record.Code == worker.ProgressSpeakByDeadline
	}
	return false
}
