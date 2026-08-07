package presentation

import (
	"encoding/json"
	"testing"

	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/worker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSanitizeActualWorkerRecordsStripsInternalMetadata(t *testing.T) {
	tests := []struct {
		kind   string
		state  handoff.State
		record any
		want   string
	}{
		{"handoff.progress", handoff.StateRunning, worker.ProgressRecord{MutationID: "mutation-secret", ExpectedObjectiveRevision: 9, InputRevision: 7, Sequence: 3, Summary: "halfway"}, `{"summary":"halfway"}`},
		{"handoff.needs-input", handoff.StateNeedsInput, worker.NeedsInputRecord{MutationID: "mutation-secret", ExpectedObjectiveRevision: 9, Prompt: "choose", Schema: json.RawMessage(`{"type":"string"}`)}, `{"prompt":"choose","schema":{"type":"string"}}`},
		{"handoff.complete", handoff.StateSucceeded, worker.CompletionRecord{ExpectedObjectiveRevision: 9, Result: handoff.Result{MutationID: "mutation-secret", Summary: "done", Structured: json.RawMessage(`{"answer":42}`), SafetyDecision: json.RawMessage(`{"private":true}`), Usage: json.RawMessage(`{"tokens":99}`)}}, `{"summary":"done","structured":{"answer":42}}`},
		{"handoff.fail", handoff.StateFailed, worker.FailureRecord{ExpectedObjectiveRevision: 9, Failure: handoff.Failure{MutationID: "mutation-secret", Code: "failed", Retryable: true, Message: "try later"}}, `{"code":"failed","retryable":true,"message":"try later"}`},
		{"handoff.cancel", handoff.StateCancelled, worker.CancelledRecord{MutationID: "mutation-secret", ExpectedObjectiveRevision: 9, Reason: "stopped"}, `{"reason":"stopped"}`},
	}
	for _, test := range tests {
		t.Run(test.kind, func(t *testing.T) {
			payload, err := json.Marshal(test.record)
			require.NoError(t, err)
			public, err := SanitizeEvent(handoff.Event{Kind: test.kind, State: test.state, Payload: payload})
			require.NoError(t, err)
			assert.JSONEq(t, test.want, string(public.Details))
			for _, forbidden := range []string{"mutation-secret", "ExpectedObjectiveRevision", "InputRevision", "Sequence", "SafetyDecision", "Usage"} {
				assert.NotContains(t, string(public.Details), forbidden)
			}
		})
	}
}

func TestSanitizeRejectsUnknownAndContradictoryTerminalEvents(t *testing.T) {
	for _, event := range []handoff.Event{
		{Kind: "handoff.future", State: handoff.StateSucceeded, Payload: json.RawMessage(`{}`)},
		{Kind: "handoff.progress", State: handoff.StateSucceeded, Payload: json.RawMessage(`{}`)},
		{Kind: "handoff.complete", State: handoff.StateRunning, Payload: json.RawMessage(`{}`)},
	} {
		_, err := SanitizeEvent(event)
		assert.ErrorIs(t, err, ErrUnsupportedEvent)
	}
}

func TestSanitizeExactRepositoryKindStateAllowlist(t *testing.T) {
	tests := []handoff.Event{
		{Kind: "handoff.created", State: handoff.StatePending, Payload: json.RawMessage(`{}`)},
		{Kind: "handoff.leased", State: handoff.StateLeased, Payload: json.RawMessage(`{"worker_id":"private-worker","lease_version":7}`)},
		{Kind: "handoff.leased", State: handoff.StateRunning, Payload: json.RawMessage(`{"worker_id":"private-worker","lease_version":8}`)},
		{Kind: "handoff.running", State: handoff.StateRunning, Payload: json.RawMessage(`{}`)},
		{Kind: "handoff.steered", State: handoff.StateNeedsInput, Payload: json.RawMessage(`{"actor":"private-actor","expected_revision":3,"objective":"updated"}`)},
		{Kind: "handoff.input_provided", State: handoff.StateNeedsInput, Payload: json.RawMessage(`{"actor":"private-actor","expected_revision":3,"input":{"private":true}}`)},
		{Kind: "handoff.cancel_requested", State: handoff.StateRunning, Payload: json.RawMessage(`{"actor":"private-actor","reason":"stop"}`)},
		{Kind: "handoff.retry", State: handoff.StateRunning, Payload: json.RawMessage(`{"MutationID":"private-mutation","ExpectedObjectiveRevision":3,"RetryCount":2}`)},
		{Kind: "handoff.lease_expired", State: handoff.StatePending, Payload: json.RawMessage(`{}`)},
		{Kind: "handoff.lease_expired", State: handoff.StateRunning, Payload: json.RawMessage(`{}`)},
		{Kind: "handoff.cancelled", State: handoff.StateCancelled, Payload: json.RawMessage(`{}`)},
		{Kind: "handoff.expired", State: handoff.StateExpired, Payload: json.RawMessage(`{}`)},
	}
	for _, event := range tests {
		public, err := SanitizeEvent(event)
		require.NoError(t, err, "%s/%s", event.Kind, event.State)
		for _, forbidden := range []string{"private-worker", "private-actor", "private-mutation", "lease_version", "expected_revision", "private"} {
			assert.NotContains(t, string(public.Details), forbidden)
		}
	}
}
