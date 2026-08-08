package handoff

import (
	"strings"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateApprovalResolutionExactContract(t *testing.T) {
	now := time.Date(2026, 8, 8, 1, 2, 3, 0, time.UTC)
	valid := ApprovalResolution{InvocationID: "invocation", Decision: ApprovalApproved, ActorTenantID: "tenant", ActorUserID: "user", Reason: "operator confirmed", IdempotencyKey: "decision-1", ResolvedAt: now}
	require.NoError(t, ValidateApprovalResolution(valid))

	for name, mutate := range map[string]func(*ApprovalResolution){
		"invocation": func(value *ApprovalResolution) { value.InvocationID = "" },
		"decision":   func(value *ApprovalResolution) { value.Decision = "allow" },
		"tenant":     func(value *ApprovalResolution) { value.ActorTenantID = "" },
		"user":       func(value *ApprovalResolution) { value.ActorUserID = "" },
		"reason":     func(value *ApprovalResolution) { value.Reason = strings.Repeat("x", MaximumApprovalReasonBytes+1) },
		"key":        func(value *ApprovalResolution) { value.IdempotencyKey = "" },
		"time":       func(value *ApprovalResolution) { value.ResolvedAt = time.Time{} },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			mutate(&candidate)
			assert.ErrorIs(t, ValidateApprovalResolution(candidate), ErrInvalidRequest)
		})
	}
}

func TestCloneToolApprovalIsValueSafe(t *testing.T) {
	original := ToolApproval{InvocationID: "invocation", HandoffID: identity.HandoffID("handoff"), ApprovalState: "pending", ActionSummary: "Send the bounded notification", RiskClass: ApprovalRiskSideEffecting, ExpiresAt: time.Now().UTC().Add(time.Minute)}
	clone := CloneToolApproval(original)
	assert.Equal(t, original, clone)
}
