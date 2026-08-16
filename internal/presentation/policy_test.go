package presentation_test

import (
	"encoding/json"
	"testing"

	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/presentation"
	"github.com/stretchr/testify/assert"
)

func TestPolicyKeepsOrdinaryProgressVisualOnly(t *testing.T) {
	policy := presentation.NewPolicy(presentation.Config{MaxSpokenPayloadBytes: 1024})
	decision := policy.Decide(presentation.Context{}, handoff.Event{
		Kind: "handoff.progress", State: handoff.StateRunning, Payload: json.RawMessage(`{"summary":"halfway"}`),
	})

	assert.Equal(t, presentation.Decision{EmitVisual: true}, decision)
}

func TestPolicyDefersCompletionWhileUserSpeakingOrResponseActive(t *testing.T) {
	policy := presentation.NewPolicy(presentation.Config{MaxSpokenPayloadBytes: 1024})
	event := handoff.Event{
		Kind: "handoff.complete", State: handoff.StateSucceeded, Payload: json.RawMessage(`{"summary":"done"}`),
	}

	for _, context := range []presentation.Context{
		{UserSpeaking: true},
		{ResponseActive: true},
		{UserSpeaking: true, ResponseActive: true},
	} {
		decision := policy.Decide(context, event)
		assert.True(t, decision.EmitVisual)
		assert.False(t, decision.SpeakNow)
		assert.True(t, decision.DeferSpeech)
	}
}

func TestPolicySpeaksNeedsInputOnlyWhenConversationIsIdle(t *testing.T) {
	policy := presentation.NewPolicy(presentation.Config{MaxSpokenPayloadBytes: 1024})
	event := handoff.Event{
		Kind: "handoff.needs-input", State: handoff.StateNeedsInput, Payload: json.RawMessage(`{"summary":"which region?"}`),
	}

	assert.Equal(t, presentation.Decision{EmitVisual: true, SpeakNow: true}, policy.Decide(presentation.Context{}, event))
	assert.Equal(t, presentation.Decision{EmitVisual: true, DeferSpeech: true}, policy.Decide(presentation.Context{ResponseActive: true}, event))
}

func TestPolicyDoesNotSpeakOversizedCompletion(t *testing.T) {
	policy := presentation.NewPolicy(presentation.Config{MaxSpokenPayloadBytes: 8})
	decision := policy.Decide(presentation.Context{}, handoff.Event{
		Kind: "handoff.complete", State: handoff.StateSucceeded, Payload: json.RawMessage(`{"summary":"too large"}`),
	})

	assert.Equal(t, presentation.Decision{EmitVisual: true}, decision)
}

func TestPolicyDoesNotInventImportantBlockerSchema(t *testing.T) {
	policy := presentation.NewPolicy(presentation.Config{MaxSpokenPayloadBytes: 1024})
	important := handoff.Event{Kind: "handoff.progress", State: handoff.StateRunning, Payload: json.RawMessage(`{"important":true,"summary":"approval blocked"}`)}
	ordinary := handoff.Event{Kind: "handoff.progress", State: handoff.StateRunning, Payload: json.RawMessage(`{"important":false,"summary":"still working"}`)}

	assert.Equal(t, presentation.Decision{EmitVisual: true}, policy.Decide(presentation.Context{}, important))
	assert.Equal(t, presentation.Decision{EmitVisual: true}, policy.Decide(presentation.Context{}, ordinary))
}

func TestPolicySpeaksAllowlistedSpeakByDeadlineOnly(t *testing.T) {
	policy := presentation.NewPolicy(presentation.Config{MaxSpokenPayloadBytes: 1024})
	speak := handoff.Event{Kind: "handoff.progress", State: handoff.StateRunning, Payload: json.RawMessage(`{"summary":"still working","code":"speak_by_deadline"}`)}
	assert.Equal(t, presentation.Decision{EmitVisual: true, SpeakNow: true}, policy.Decide(presentation.Context{}, speak))
	assert.Equal(t, presentation.Decision{EmitVisual: true, DeferSpeech: true}, policy.Decide(presentation.Context{UserSpeaking: true}, speak))
}
