// Package handoff defines durable background-task handoffs independently of
// any persistence implementation.
package handoff

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
)

// LegacyProfileSnapshot is the explicit compatibility authority for handoffs
// created without a published vertical profile before snapshot persistence was
// introduced. New profile-backed sessions replace it at the session boundary.
func LegacyProfileSnapshot() ProfileSnapshot {
	return ProfileSnapshot{
		ProfileID: "builtin", ProfileVersion: 1, SnapshotHash: strings.Repeat("0", 64),
		EngineVersion: "jinja-go-v1/legacy", RecipeID: "builtin", RecipeVersion: 1,
		RecipeHash: strings.Repeat("0", 64), RegistryVersion: "builtin-v1",
		Evidence: json.RawMessage(`{}`),
	}
}

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
	ToolPolicy       json.RawMessage
	ApprovalPolicy   json.RawMessage
	Versions         Versions
	Profile          ProfileSnapshot
	PresentationHint string
	IdempotencyKey   string
}

// KnowledgeVersion pins one immutable tenant-scoped knowledge version.
type KnowledgeVersion struct {
	ID      string `json:"id"`
	Version uint64 `json:"version"`
	Hash    string `json:"hash"`
}

// ProfileSnapshot is the complete immutable vertical authority accepted with
// a durable handoff. It is never recomputed from the currently active profile.
type ProfileSnapshot struct {
	ProfileID       string             `json:"profile_id"`
	ProfileVersion  uint64             `json:"profile_version"`
	SnapshotHash    string             `json:"snapshot_hash"`
	EngineVersion   string             `json:"engine_version"`
	RecipeID        string             `json:"recipe_id"`
	RecipeVersion   uint64             `json:"recipe_version"`
	RecipeHash      string             `json:"recipe_hash"`
	RegistryVersion string             `json:"registry_version"`
	Knowledge       []KnowledgeVersion `json:"knowledge,omitempty"`
	Evidence        json.RawMessage    `json:"evidence"`
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
	Input             json.RawMessage
	Attempt           uint32
	RetryCount        uint32
	Result            *Result
	Failure           *Failure
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

type Result struct {
	MutationID     string
	Summary        string
	Structured     json.RawMessage
	SafetyDecision json.RawMessage
	Usage          json.RawMessage
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
	out.Input = cloneBytes(in.Input)
	if in.Result != nil {
		result := *in.Result
		result.Structured = cloneBytes(in.Result.Structured)
		result.SafetyDecision = cloneBytes(in.Result.SafetyDecision)
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
	if in.ToolAllowlist != nil {
		out.ToolAllowlist = append([]string{}, in.ToolAllowlist...)
	}
	out.ToolPolicy = cloneBytes(in.ToolPolicy)
	out.ApprovalPolicy = cloneBytes(in.ApprovalPolicy)
	out.Profile = cloneProfileSnapshot(in.Profile)
	return out
}

func cloneProfileSnapshot(in ProfileSnapshot) ProfileSnapshot {
	out := in
	out.Knowledge = append([]KnowledgeVersion(nil), in.Knowledge...)
	out.Evidence = cloneBytes(in.Evidence)
	return out
}

func cloneBytes[T ~[]byte](in T) T {
	return append(T(nil), in...)
}
