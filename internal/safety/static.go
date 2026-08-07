package safety

import (
	"context"
	"errors"
	"strings"
	"unicode/utf8"
)

type StaticMode string

const (
	StaticModeEnforce StaticMode = "enforce"
	StaticModeRedact  StaticMode = "redact"
)

const (
	defaultRedactedSummary     = "A background task completed with protected details removed."
	defaultClarificationPrompt = "Approval is required before this task can continue."
)

// StaticConfig is copied by NewStaticPolicy and is immutable thereafter.
type StaticConfig struct {
	Mode                StaticMode
	MaxSummaryBytes     int
	MaxStructuredBytes  int
	RedactedSummary     string
	ClarificationPrompt string
}

type StaticPolicy struct{ config StaticConfig }

func NewStaticPolicy(config StaticConfig) (*StaticPolicy, error) {
	if config.Mode == "" {
		config.Mode = StaticModeEnforce
	}
	if config.MaxSummaryBytes == 0 {
		config.MaxSummaryBytes = MaximumSummaryBytes
	}
	if config.MaxStructuredBytes == 0 {
		config.MaxStructuredBytes = MaximumStructuredBytes
	}
	if config.RedactedSummary == "" {
		config.RedactedSummary = defaultRedactedSummary
	}
	if config.ClarificationPrompt == "" {
		config.ClarificationPrompt = defaultClarificationPrompt
	}
	if config.Mode != StaticModeEnforce && config.Mode != StaticModeRedact {
		return nil, errors.New("static safety mode is invalid")
	}
	if config.MaxSummaryBytes <= 0 || config.MaxSummaryBytes > MaximumSummaryBytes ||
		config.MaxStructuredBytes <= 0 || config.MaxStructuredBytes > MaximumStructuredBytes {
		return nil, errors.New("static safety limits are invalid")
	}
	if err := validateSafeMessage(config.RedactedSummary, config.MaxSummaryBytes); err != nil {
		return nil, errors.New("static redacted summary is invalid")
	}
	if err := validateSafeMessage(config.ClarificationPrompt, config.MaxSummaryBytes); err != nil {
		return nil, errors.New("static clarification prompt is invalid")
	}
	return &StaticPolicy{config: config}, nil
}

// NewStatic is a concise alias useful at composition roots.
func NewStatic(config StaticConfig) (*StaticPolicy, error) { return NewStaticPolicy(config) }

func (p *StaticPolicy) EvaluateWorkerResult(ctx context.Context, candidate WorkerResultCandidate) (Decision, error) {
	if p == nil || ctx == nil {
		return Decision{}, errors.New("static safety dependencies are required")
	}
	if err := ctx.Err(); err != nil {
		return Decision{}, err
	}
	if err := candidate.Validate(); err != nil || len(candidate.Summary) > p.config.MaxSummaryBytes || len(candidate.Structured) > p.config.MaxStructuredBytes {
		return Decision{}, errors.New("static safety candidate is invalid")
	}
	if candidate.ProviderBlocked {
		return Decision{Action: ActionTerminateSession, ReasonCode: "provider_blocked"}, nil
	}
	if candidate.RequestedAction == ActionTerminateSession {
		return Decision{Action: ActionTerminateSession, ReasonCode: "safety_control"}, nil
	}
	if candidate.RequestedAction == ActionCancelResponse {
		return Decision{Action: ActionCancelResponse, ReasonCode: "safety_control"}, nil
	}
	if candidate.RequiresApproval && !candidate.Approved {
		return Decision{Action: ActionRequestClarification, SafeSummary: p.config.ClarificationPrompt, ReasonCode: "approval_required"}, nil
	}
	if p.config.Mode == StaticModeRedact {
		return Decision{Action: ActionRedact, SafeSummary: p.config.RedactedSummary, ReasonCode: "policy_redacted"}, nil
	}
	return Decision{Action: ActionAllow, ReasonCode: "policy_allowed"}, nil
}

func validateSafeMessage(value string, maximum int) error {
	if strings.TrimSpace(value) == "" || len(value) > maximum || !utf8.ValidString(value) {
		return errors.New("safe message is invalid")
	}
	return nil
}
