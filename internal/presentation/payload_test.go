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
		{"handoff.progress", handoff.StateRunning, worker.ProgressRecord{MutationID: "mutation-secret", ExpectedObjectiveRevision: 9, InputRevision: 7, Sequence: 3, Phase: worker.ProgressPhaseSearching, Summary: "Searching the web."}, `{"phase":"searching","summary":"Searching the web."}`},
		{"handoff.needs-input", handoff.StateNeedsInput, worker.NeedsInputRecord{MutationID: "mutation-secret", ExpectedObjectiveRevision: 9, Prompt: "choose", Schema: json.RawMessage(`{"type":"string"}`)}, `{"prompt":"choose","schema":{"type":"string"}}`},
		{"handoff.complete", handoff.StateSucceeded, worker.CompletionRecord{ExpectedObjectiveRevision: 9, Result: handoff.Result{MutationID: "mutation-secret", Summary: "done", Structured: json.RawMessage(`{"text":"done","sources":[{"title":"Go Docs","url":"https://go.dev/doc/"}]}`), SafetyDecision: json.RawMessage(`{"private":true}`), Usage: json.RawMessage(`{"tokens":99}`)}}, `{"summary":"done","structured":{"text":"done","sources":[{"title":"Go Docs","url":"https://go.dev/doc/"}]}}`},
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

func TestSanitizeProgressPhaseRequiresExactEnumAndServerSummary(t *testing.T) {
	for _, test := range []struct {
		phase, summary, want string
	}{
		{worker.ProgressPhaseSearching, "Searching the web.", `{"phase":"searching","summary":"Searching the web."}`},
		{worker.ProgressPhaseReadingSources, "Reading sources.", `{"phase":"reading_sources","summary":"Reading sources."}`},
		{worker.ProgressPhasePreparingAnswer, "Preparing the answer.", `{"phase":"preparing_answer","summary":"Preparing the answer."}`},
	} {
		record := worker.ProgressRecord{MutationID: "private", ExpectedObjectiveRevision: 1, Phase: test.phase, Summary: test.summary}
		payload, err := json.Marshal(record)
		require.NoError(t, err)
		public, err := SanitizeEvent(handoff.Event{Kind: "handoff.progress", State: handoff.StateRunning, Payload: payload})
		require.NoError(t, err)
		assert.JSONEq(t, test.want, string(public.Details))
	}

	for _, record := range []worker.ProgressRecord{
		{Phase: "searching_private_query", Summary: "Searching the web."},
		{Phase: worker.ProgressPhaseSearching, Summary: "Searching private query terms."},
		{Phase: worker.ProgressPhaseReadingSources, Summary: "private snippet"},
	} {
		payload, err := json.Marshal(record)
		require.NoError(t, err)
		_, err = SanitizeEvent(handoff.Event{Kind: "handoff.progress", State: handoff.StateRunning, Payload: payload})
		require.Error(t, err)
	}
}

func TestSanitizeCompletionAllowsOnlyBoundedTextAndHTTPSources(t *testing.T) {
	valid := worker.CompletionRecord{Result: handoff.Result{Summary: "answer", Structured: json.RawMessage(`{"text":"answer","sources":[{"title":"Title","url":"https://example.com/path"}]}`)}}
	payload, err := json.Marshal(valid)
	require.NoError(t, err)
	public, err := SanitizeEvent(handoff.Event{Kind: "handoff.complete", State: handoff.StateSucceeded, Payload: payload})
	require.NoError(t, err)
	assert.JSONEq(t, `{"summary":"answer","structured":{"text":"answer","sources":[{"title":"Title","url":"https://example.com/path"}]}}`, string(public.Details))

	invalid := []json.RawMessage{
		json.RawMessage(`{"text":"answer","query":"private"}`),
		json.RawMessage(`{"text":"answer","snippet":"private"}`),
		json.RawMessage(`{"text":"answer","sources":[{"title":"Title","url":"http://example.com/"}]}`),
		json.RawMessage(`{"text":"answer","sources":[{"title":"","url":"https://example.com/"}]}`),
		json.RawMessage(`{"text":"answer","sources":[],"sources":[]}`),
	}
	for _, structured := range invalid {
		record := worker.CompletionRecord{Result: handoff.Result{Summary: "answer", Structured: structured}}
		raw, err := json.Marshal(record)
		require.NoError(t, err)
		_, err = SanitizeEvent(handoff.Event{Kind: "handoff.complete", State: handoff.StateSucceeded, Payload: raw})
		require.Error(t, err, string(structured))
	}

	redacted := worker.CompletionRecord{Result: handoff.Result{Summary: "safe", Structured: json.RawMessage(`{}`)}}
	payload, err = json.Marshal(redacted)
	require.NoError(t, err)
	public, err = SanitizeEvent(handoff.Event{Kind: "handoff.complete", State: handoff.StateSucceeded, Payload: payload})
	require.NoError(t, err)
	assert.JSONEq(t, `{"summary":"safe"}`, string(public.Details))
}

func TestSanitizeApprovalOutcomesAreContentFreeAndPresentationSafe(t *testing.T) {
	tests := []struct {
		kind, outcome, want string
		state               handoff.State
	}{
		{"tool.approval_approved", "approved", `{"summary":"Tool approval accepted."}`, handoff.StatePending},
		{"tool.approval_denied", "denied", `{"code":"approval_denied","retryable":false,"message":"Tool approval was denied."}`, handoff.StateFailed},
		{"tool.approval_expired", "expired", `{"code":"approval_expired","retryable":false,"message":"Tool approval expired."}`, handoff.StateFailed},
	}
	for _, test := range tests {
		t.Run(test.kind, func(t *testing.T) {
			payload := json.RawMessage(`{"invocation_id":"inv-1","outcome":"` + test.outcome + `","reason_hash":"hash","arguments":{"secret":"never-forward"}}`)
			public, err := SanitizeEvent(handoff.Event{Kind: test.kind, State: test.state, Payload: payload})
			require.NoError(t, err)
			assert.JSONEq(t, test.want, string(public.Details))
			assert.NotContains(t, string(public.Details), "secret")
			assert.NotContains(t, string(public.Details), "arguments")
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
