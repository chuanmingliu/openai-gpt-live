package safety

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"unicode/utf8"

	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
)

type Action string
type Channel string

const (
	ChannelWorker Channel = "worker"
	ChannelVisual Channel = "visual"
	ChannelSpeech Channel = "speech"
)

const (
	ActionAllow                Action = "allow"
	ActionRedact               Action = "redact"
	ActionSuppress             Action = "suppress"
	ActionRequestClarification Action = "request_clarification"
	ActionCancelResponse       Action = "cancel_response"
	ActionTerminateSession     Action = "terminate_session"
)

type WorkerResultCandidate struct {
	SessionID        identity.SessionID
	ConversationID   identity.ConversationID
	HandoffID        identity.HandoffID
	Channel          Channel
	Summary          string
	Structured       json.RawMessage
	RequiresApproval bool
	Approved         bool
	ProviderBlocked  bool
}

func (c WorkerResultCandidate) Clone() WorkerResultCandidate {
	c.Structured = append(json.RawMessage(nil), c.Structured...)
	return c
}

func (c WorkerResultCandidate) Validate() error {
	if c.SessionID == "" || c.ConversationID == "" || c.HandoffID == "" || len(c.SessionID) > 256 || len(c.ConversationID) > 256 || len(c.HandoffID) > 256 {
		return errors.New("safety candidate identity is invalid")
	}
	if c.Channel != ChannelWorker && c.Channel != ChannelVisual && c.Channel != ChannelSpeech {
		return errors.New("safety candidate channel is invalid")
	}
	if strings.TrimSpace(c.Summary) == "" || len(c.Summary) > 16<<10 || !utf8.ValidString(c.Summary) {
		return errors.New("safety candidate summary is invalid")
	}
	if len(c.Structured) == 0 || len(c.Structured) > 64<<10 || !json.Valid(c.Structured) {
		return errors.New("safety candidate structured result is invalid")
	}
	return nil
}

type Decision struct {
	Action      Action `json:"action"`
	SafeSummary string `json:"safe_summary,omitempty"`
	ReasonCode  string `json:"reason_code"`
}

func (d Decision) Validate() error {
	switch d.Action {
	case ActionAllow, ActionRedact, ActionSuppress, ActionRequestClarification, ActionCancelResponse, ActionTerminateSession:
	default:
		return errors.New("safety decision action is invalid")
	}
	if strings.TrimSpace(d.ReasonCode) == "" || len(d.ReasonCode) > 256 || !utf8.ValidString(d.ReasonCode) {
		return errors.New("safety decision reason is invalid")
	}
	if len(d.SafeSummary) > 16<<10 || !utf8.ValidString(d.SafeSummary) || d.Action == ActionRedact && strings.TrimSpace(d.SafeSummary) == "" {
		return errors.New("safety decision summary is invalid")
	}
	return nil
}

type Policy interface {
	EvaluateWorkerResult(context.Context, WorkerResultCandidate) (Decision, error)
}

type DenyAll struct{}

func (DenyAll) EvaluateWorkerResult(ctx context.Context, candidate WorkerResultCandidate) (Decision, error) {
	if ctx == nil {
		return Decision{}, errors.New("safety context is required")
	}
	if err := ctx.Err(); err != nil {
		return Decision{}, err
	}
	return Decision{Action: ActionSuppress, ReasonCode: "deny_all"}, nil
}

func EvaluateOrSuppress(ctx context.Context, policy Policy, candidate WorkerResultCandidate) Decision {
	if nilPolicy(policy) || ctx == nil || ctx.Err() != nil || candidate.Validate() != nil {
		return Decision{Action: ActionSuppress, ReasonCode: "policy_error"}
	}
	decision, err := policy.EvaluateWorkerResult(ctx, candidate.Clone())
	if err != nil || decision.Validate() != nil {
		return Decision{Action: ActionSuppress, ReasonCode: "policy_error"}
	}
	return decision
}

func nilPolicy(policy Policy) bool {
	if policy == nil {
		return true
	}
	value := reflect.ValueOf(policy)
	return value.Kind() == reflect.Pointer && value.IsNil()
}
