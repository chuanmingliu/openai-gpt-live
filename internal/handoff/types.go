// Package handoff defines durable background-task handoffs independently of
// any persistence implementation.
package handoff

import (
	"encoding/json"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
)

type Scope struct {
	TenantID       string
	UserID         string
	ConversationID identity.ConversationID
}

type CreateRequest struct {
	Scope            Scope
	SessionID        identity.SessionID
	SourceTurn       identity.TurnKey
	UserRequest      string
	Objective        string
	ContextRef       string
	ContextHash      string
	Deadline         time.Time
	TokenBudget      int
	Priority         int16
	ToolAllowlist    []string
	ApprovalPolicy   json.RawMessage
	Versions         Versions
	PresentationHint string
	IdempotencyKey   string
}

type Versions struct {
	Model        string
	Prompt       string
	ToolRegistry string
	Config       string
}

type State string

const (
	StatePending    State = "pending"
	StateLeased     State = "leased"
	StateRunning    State = "running"
	StateNeedsInput State = "needs_input"
	StateSucceeded  State = "succeeded"
	StateFailed     State = "failed"
	StateCancelled  State = "cancelled"
	StateExpired    State = "expired"
)

type Snapshot struct {
	ID                identity.HandoffID
	Request           CreateRequest
	RequestHash       string
	State             State
	ObjectiveRevision uint32
	InputRevision     uint32
	ConsumedInput     uint32
	Attempt           uint32
	RetryCount        uint32
	Result            *Result
	Failure           *Failure
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

type Result struct {
	MutationID string
	Summary    string
	Structured json.RawMessage
	Usage      json.RawMessage
}

type Failure struct {
	MutationID string
	Code       string
	Retryable  bool
	Message    string
}

type Event struct {
	Seq            int64
	EventID        string
	HandoffID      identity.HandoffID
	TenantID       string
	ConversationID identity.ConversationID
	Kind           string
	State          State
	Attempt        uint32
	Payload        json.RawMessage
	CreatedAt      time.Time
}

type CreateRecord struct {
	HandoffID   identity.HandoffID
	EventID     string
	Request     CreateRequest
	RequestHash string
	CreatedAt   time.Time
}

type SteerRequest struct {
	Objective        string
	ExpectedRevision uint32
	Actor            string
	IdempotencyKey   string
}

type InputRequest struct {
	Input            json.RawMessage
	ExpectedRevision uint32
	Actor            string
	IdempotencyKey   string
}

type CancelRequest struct {
	Actor          string
	Reason         string
	IdempotencyKey string
}

type EventWakeup struct {
	TenantID       string
	ConversationID identity.ConversationID
	HighestSeq     int64
}

func CloneSnapshot(in Snapshot) Snapshot {
	out := in
	out.Request = cloneRequest(in.Request)
	if in.Result != nil {
		result := *in.Result
		result.Structured = cloneBytes(in.Result.Structured)
		result.Usage = cloneBytes(in.Result.Usage)
		out.Result = &result
	}
	if in.Failure != nil {
		failure := *in.Failure
		out.Failure = &failure
	}
	return out
}

func CloneEvent(in Event) Event {
	out := in
	out.Payload = cloneBytes(in.Payload)
	return out
}

func cloneSnapshots(in []Snapshot) []Snapshot {
	if in == nil {
		return nil
	}
	out := make([]Snapshot, len(in))
	for i := range in {
		out[i] = CloneSnapshot(in[i])
	}
	return out
}

func cloneRequest(in CreateRequest) CreateRequest {
	out := in
	out.ToolAllowlist = append([]string(nil), in.ToolAllowlist...)
	out.ApprovalPolicy = cloneBytes(in.ApprovalPolicy)
	return out
}

func cloneBytes[T ~[]byte](in T) T {
	return append(T(nil), in...)
}
