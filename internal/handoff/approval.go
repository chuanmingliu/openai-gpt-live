package handoff

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
)

const (
	MaximumApprovalReasonBytes  = 2048
	MaximumApprovalSummaryBytes = 512
	ApprovalRiskSideEffecting   = "side_effecting"
)

type ApprovalDecision string

const (
	ApprovalApproved ApprovalDecision = "approved"
	ApprovalDenied   ApprovalDecision = "denied"
)

type ApprovalResolution struct {
	InvocationID   string
	Decision       ApprovalDecision
	ActorTenantID  string
	ActorUserID    string
	Reason         string
	IdempotencyKey string
	ResolvedAt     time.Time
}

// ToolApproval is the deliberately content-free public view of an invocation.
// It cannot carry arguments, tool output, provider payloads, or credentials.
type ToolApproval struct {
	InvocationID  string
	HandoffID     identity.HandoffID
	ApprovalState string
	ActionSummary string
	RiskClass     string
	ExpiresAt     time.Time
}

type ApprovalStore interface {
	ResolveApproval(context.Context, Scope, ApprovalResolution) error
	LoadApproval(context.Context, Scope, string) (ToolApproval, error)
}

type RetentionStore interface {
	MarkConversationDeleting(context.Context, Scope, string, string) error
	DeleteReadyConversations(context.Context, time.Time, int) (int, error)
}

func CloneToolApproval(in ToolApproval) ToolApproval { return in }

func ValidateApprovalResolution(value ApprovalResolution) error {
	if invalidApprovalText(value.InvocationID, 256, true) ||
		(value.Decision != ApprovalApproved && value.Decision != ApprovalDenied) ||
		invalidApprovalText(value.ActorTenantID, 256, true) ||
		invalidApprovalText(value.ActorUserID, 256, true) ||
		invalidApprovalText(value.Reason, MaximumApprovalReasonBytes, false) ||
		invalidApprovalText(value.IdempotencyKey, 256, true) || value.ResolvedAt.IsZero() {
		return fmt.Errorf("%w: approval resolution is invalid", ErrInvalidRequest)
	}
	return nil
}

func invalidApprovalText(value string, limit int, required bool) bool {
	return !utf8.ValidString(value) || len(value) > limit || (required && strings.TrimSpace(value) == "")
}
