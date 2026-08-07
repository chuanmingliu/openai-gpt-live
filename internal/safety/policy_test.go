package safety_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/safety"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStaticPolicyDecisionMatrix(t *testing.T) {
	policy, err := safety.NewStaticPolicy(safety.StaticConfig{
		Mode: safety.StaticModeEnforce, MaxSummaryBytes: 32, MaxStructuredBytes: 32,
		RedactedSummary: "safe replacement", ClarificationPrompt: "Approval is required.",
	})
	require.NoError(t, err)
	base := validCandidate()

	tests := []struct {
		name string
		edit func(*safety.WorkerResultCandidate)
		want safety.Decision
	}{
		{"allow", func(*safety.WorkerResultCandidate) {}, safety.Decision{Action: safety.ActionAllow, ReasonCode: "policy_allowed"}},
		{"provider blocked", func(c *safety.WorkerResultCandidate) { c.ProviderBlocked = true }, safety.Decision{Action: safety.ActionTerminateSession, ReasonCode: "provider_blocked"}},
		{"approval required", func(c *safety.WorkerResultCandidate) { c.RequiresApproval = true }, safety.Decision{Action: safety.ActionRequestClarification, SafeSummary: "Approval is required.", ReasonCode: "approval_required"}},
		{"approved", func(c *safety.WorkerResultCandidate) { c.RequiresApproval, c.Approved = true, true }, safety.Decision{Action: safety.ActionAllow, ReasonCode: "policy_allowed"}},
		{"durable cancel", func(c *safety.WorkerResultCandidate) { c.RequestedAction = safety.ActionCancelResponse }, safety.Decision{Action: safety.ActionCancelResponse, ReasonCode: "safety_control"}},
		{"oversize", func(c *safety.WorkerResultCandidate) { c.Summary = strings.Repeat("x", 33) }, safety.Decision{Action: safety.ActionSuppress, ReasonCode: "policy_error"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			candidate := base.Clone()
			tc.edit(&candidate)
			assert.Equal(t, tc.want, safety.EvaluateOrSuppress(t.Context(), policy, candidate))
		})
	}
}

func TestStaticRedactionAndDefensiveCopies(t *testing.T) {
	policy, err := safety.NewStaticPolicy(safety.StaticConfig{
		Mode: safety.StaticModeRedact, MaxSummaryBytes: 64, MaxStructuredBytes: 64,
		RedactedSummary: "safe", ClarificationPrompt: "clarify",
	})
	require.NoError(t, err)
	candidate := validCandidate()
	seen := candidate.Structured
	decision := safety.EvaluateOrSuppress(t.Context(), policy, candidate)
	assert.Equal(t, safety.Decision{Action: safety.ActionRedact, SafeSummary: "safe", ReasonCode: "policy_redacted"}, decision)
	seen[0] = '['
	assert.Equal(t, safety.ActionRedact, safety.EvaluateOrSuppress(t.Context(), policy, validCandidate()).Action)
}

func TestDecisionActionSpecificValidation(t *testing.T) {
	valid := []safety.Decision{
		{Action: safety.ActionAllow, ReasonCode: "allowed"},
		{Action: safety.ActionRedact, SafeSummary: "safe", ReasonCode: "redacted"},
		{Action: safety.ActionSuppress, ReasonCode: "suppressed"},
		{Action: safety.ActionRequestClarification, SafeSummary: "question", ReasonCode: "approval_required"},
		{Action: safety.ActionCancelResponse, ReasonCode: "cancelled"},
		{Action: safety.ActionTerminateSession, ReasonCode: "terminated"},
	}
	for _, decision := range valid {
		assert.NoError(t, decision.Validate(), decision.Action)
	}
	invalid := []safety.Decision{
		{Action: safety.ActionAllow, SafeSummary: "unexpected", ReasonCode: "allowed"},
		{Action: safety.ActionRedact, ReasonCode: "redacted"},
		{Action: safety.ActionRequestClarification, ReasonCode: "clarify"},
		{Action: safety.ActionSuppress, SafeSummary: "leak", ReasonCode: "suppressed"},
		{Action: safety.ActionTerminateSession, SafeSummary: "leak", ReasonCode: "terminated"},
		{Action: safety.ActionSuppress, ReasonCode: "attacker supplied prose"},
	}
	for _, decision := range invalid {
		assert.Error(t, decision.Validate(), decision.Action)
	}
}

func TestPolicyErrorsFailClosed(t *testing.T) {
	decision := safety.EvaluateOrSuppress(t.Context(), policyFunc(func(context.Context, safety.WorkerResultCandidate) (safety.Decision, error) {
		return safety.Decision{}, errors.New("private provider detail")
	}), validCandidate())
	assert.Equal(t, safety.Decision{Action: safety.ActionSuppress, ReasonCode: "policy_error"}, decision)
}

func TestCandidateStrictBoundariesAndCloneIsolation(t *testing.T) {
	candidate := validCandidate()
	candidate.Summary = strings.Repeat("s", safety.MaximumSummaryBytes)
	candidate.Structured = json.RawMessage(`{"nested":[1,true,null]}`)
	assert.NoError(t, candidate.Validate())

	for _, edit := range []func(*safety.WorkerResultCandidate){
		func(c *safety.WorkerResultCandidate) { c.SessionID = " " },
		func(c *safety.WorkerResultCandidate) { c.Summary = strings.Repeat("s", safety.MaximumSummaryBytes+1) },
		func(c *safety.WorkerResultCandidate) { c.Structured = json.RawMessage(`{"x":1,"x":2}`) },
		func(c *safety.WorkerResultCandidate) {
			c.Structured = json.RawMessage(strings.Repeat("[", 33) + "0" + strings.Repeat("]", 33))
		},
		func(c *safety.WorkerResultCandidate) { c.Approved = true },
	} {
		invalid := validCandidate()
		edit(&invalid)
		assert.Error(t, invalid.Validate())
	}

	original := validCandidate()
	decision := safety.EvaluateOrSuppress(t.Context(), policyFunc(func(_ context.Context, candidate safety.WorkerResultCandidate) (safety.Decision, error) {
		candidate.Structured[0] = '['
		return safety.Decision{Action: safety.ActionAllow, ReasonCode: "allowed"}, nil
	}), original)
	assert.Equal(t, safety.ActionAllow, decision.Action)
	assert.Equal(t, byte('{'), original.Structured[0])
}

func validCandidate() safety.WorkerResultCandidate {
	return safety.WorkerResultCandidate{SessionID: identity.SessionID("session"), ConversationID: identity.ConversationID("conversation"), HandoffID: identity.HandoffID("handoff"), Channel: safety.ChannelWorker, Summary: "summary", Structured: json.RawMessage(`{"ok":true}`)}
}

type policyFunc func(context.Context, safety.WorkerResultCandidate) (safety.Decision, error)

func (f policyFunc) EvaluateWorkerResult(ctx context.Context, candidate safety.WorkerResultCandidate) (safety.Decision, error) {
	return f(ctx, candidate)
}

type nilMapPolicy map[string]string
type nilSlicePolicy []string
type nilChanPolicy chan struct{}

func (nilMapPolicy) EvaluateWorkerResult(context.Context, safety.WorkerResultCandidate) (safety.Decision, error) {
	panic("nil map policy called")
}
func (nilSlicePolicy) EvaluateWorkerResult(context.Context, safety.WorkerResultCandidate) (safety.Decision, error) {
	panic("nil slice policy called")
}
func (nilChanPolicy) EvaluateWorkerResult(context.Context, safety.WorkerResultCandidate) (safety.Decision, error) {
	panic("nil channel policy called")
}

func TestEvaluateOrSuppressTypedNilAndPanicMatrix(t *testing.T) {
	var nilFunc policyFunc
	var nilMap nilMapPolicy
	var nilSlice nilSlicePolicy
	var nilChan nilChanPolicy
	var nilPointer *safety.StaticPolicy
	policies := []safety.Policy{
		nilFunc, nilMap, nilSlice, nilChan, nilPointer,
		policyFunc(func(context.Context, safety.WorkerResultCandidate) (safety.Decision, error) { panic("attacker panic") }),
	}
	for _, policy := range policies {
		assert.NotPanics(t, func() {
			assert.Equal(t, safety.Decision{Action: safety.ActionSuppress, ReasonCode: "policy_error"}, safety.EvaluateOrSuppress(t.Context(), policy, validCandidate()))
		})
	}
}
