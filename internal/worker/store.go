// Package worker defines durable worker leasing independently of persistence.
package worker

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"strconv"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
)

var (
	ErrLeaseLost               = errors.New("worker lease lost")
	ErrToolNotAllowed          = errors.New("worker tool not allowed")
	ErrToolNotRegistered       = errors.New("worker tool not registered")
	ErrApprovalRequired        = errors.New("worker tool approval required")
	ErrApprovalDenied          = errors.New("worker tool approval denied")
	ErrApprovalExpired         = errors.New("worker tool approval expired")
	ErrInfrastructureUncertain = errors.New("worker infrastructure outcome uncertain")
)

type LeaseToken struct {
	TenantID       string
	ConversationID identity.ConversationID
	HandoffID      identity.HandoffID
	WorkerID       string
	Version        int64
	Attempt        uint32
}

type LeaseRequest struct {
	WorkerID      string
	LeaseDuration time.Duration
	Now           time.Time
}

type Lease struct {
	Token LeaseToken
	Job   handoff.Snapshot
}

type Control struct {
	CancelRequested   bool
	ObjectiveRevision uint32
	Objective         string
	InputRevision     uint32
	ConsumedInput     uint32
	Input             json.RawMessage
}

type RetryRecord struct {
	MutationID                string
	ExpectedObjectiveRevision uint32
	ReadyAt                   time.Time
	RetryCount                uint32
	Code                      string
}

type NeedsInputRecord struct {
	MutationID                string
	ExpectedObjectiveRevision uint32
	Prompt                    string
	Schema                    json.RawMessage
}

const ProgressSpeakByDeadline = "speak_by_deadline"

type ProgressRecord struct {
	MutationID                string
	ExpectedObjectiveRevision uint32
	InputRevision             uint32
	Sequence                  uint64
	Summary                   string
	Code                      string `json:"code,omitempty"`
}

type CancelledRecord struct {
	MutationID                string
	ExpectedObjectiveRevision uint32
	Reason                    string
}

type CompletionRecord struct {
	ExpectedObjectiveRevision uint32
	Result                    handoff.Result
}

type FailureRecord struct {
	ExpectedObjectiveRevision uint32
	Failure                   handoff.Failure
}

type ToolInvocationRecord struct {
	InvocationID      string
	ObjectiveRevision uint32
	ToolOrdinal       uint32
	ToolName          string
	IdempotencyKey    string
	ArgumentsHash     string
	Ciphertext        []byte
	Nonce             []byte
	KeyID             string
}

type ToolInvocation struct {
	Record            ToolInvocationRecord
	Status            string
	Result            json.RawMessage
	RequiresApproval  bool
	ApprovalState     ApprovalState
	ApprovalProof     string
	ResumeDisposition ApprovalResumeDisposition
}

type ApprovalResumeDisposition string

const (
	ApprovalResumeNone      ApprovalResumeDisposition = ""
	ApprovalResumeFresh     ApprovalResumeDisposition = "fresh"
	ApprovalResumeUncertain ApprovalResumeDisposition = "uncertain"
	ApprovalResumeSucceeded ApprovalResumeDisposition = "succeeded"
)

type ApprovalState string

const (
	ApprovalNotRequired ApprovalState = "not_required"
	ApprovalPending     ApprovalState = "pending"
	ApprovalApproved    ApprovalState = "approved"
	ApprovalDenied      ApprovalState = "denied"
	ApprovalExpired     ApprovalState = "expired"
)

type ToolCompletionRecord struct {
	InvocationID string
	MutationID   string
	Result       json.RawMessage
}

type Store interface {
	LeaseNext(context.Context, LeaseRequest) (Lease, bool, error)
	MarkRunning(context.Context, LeaseToken, time.Time) error
	Heartbeat(context.Context, LeaseToken, time.Time) (Control, error)
	ScheduleRetry(context.Context, LeaseToken, RetryRecord) error
	AppendProgress(context.Context, LeaseToken, ProgressRecord) error
	MarkNeedsInput(context.Context, LeaseToken, NeedsInputRecord) error
	Complete(context.Context, LeaseToken, CompletionRecord) error
	Fail(context.Context, LeaseToken, FailureRecord) error
	MarkCancelled(context.Context, LeaseToken, CancelledRecord) error
	ClaimApprovedToolInvocation(context.Context, LeaseToken, uint32) (ToolInvocation, bool, error)
	BeginToolInvocation(context.Context, LeaseToken, ToolInvocationRecord) (ToolInvocation, bool, error)
	CompleteToolInvocation(context.Context, LeaseToken, ToolCompletionRecord) error
	SweepExpired(context.Context, time.Time, int) (int, error)
}

type ToolGateMetadata struct {
	SideEffecting bool
}

// ToolGate is an immutable process-registry view. Implementations must return
// metadata only for the exact named immutable registry version and tool.
type ToolGate interface {
	Metadata(context.Context, string, string) (ToolGateMetadata, bool)
}

// StableToolInvocationID hashes unambiguous length-prefixed components. The
// decimal encodings make the identity portable across storage/language widths.
func StableToolInvocationID(tenantID string, conversationID identity.ConversationID, handoffID identity.HandoffID, objectiveRevision, toolOrdinal uint32, toolName, argumentsHash string) string {
	h := sha256.New()
	writePart(h, tenantID)
	writePart(h, string(conversationID))
	writePart(h, string(handoffID))
	writePart(h, strconv.FormatUint(uint64(objectiveRevision), 10))
	writePart(h, strconv.FormatUint(uint64(toolOrdinal), 10))
	writePart(h, toolName)
	writePart(h, argumentsHash)
	return hex.EncodeToString(h.Sum(nil))
}

func writePart(h hash.Hash, value string) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = h.Write(size[:])
	_, _ = h.Write([]byte(value))
}

// SetCanonicalToolApproval records approval metadata derived by the durable
// invocation gate. The proof lets downstream execution reject caller-synthesized
// booleans and stale or incomplete store implementations.
func SetCanonicalToolApproval(invocation *ToolInvocation, required bool, state ApprovalState) {
	if invocation == nil {
		return
	}
	invocation.RequiresApproval = required
	invocation.ApprovalState = state
	h := sha256.New()
	writePart(h, invocation.Record.InvocationID)
	writePart(h, strconv.FormatBool(required))
	writePart(h, string(state))
	invocation.ApprovalProof = hex.EncodeToString(h.Sum(nil))
}

func ValidateCanonicalToolApproval(invocation ToolInvocation) error {
	validState := invocation.ApprovalState == ApprovalNotRequired || invocation.ApprovalState == ApprovalPending || invocation.ApprovalState == ApprovalApproved || invocation.ApprovalState == ApprovalDenied || invocation.ApprovalState == ApprovalExpired
	if !validState || (!invocation.RequiresApproval && invocation.ApprovalState != ApprovalNotRequired) || (invocation.RequiresApproval && invocation.ApprovalState == ApprovalNotRequired) {
		return errors.New("worker tool approval metadata is invalid")
	}
	copy := invocation
	SetCanonicalToolApproval(&copy, invocation.RequiresApproval, invocation.ApprovalState)
	if invocation.ApprovalProof == "" || invocation.ApprovalProof != copy.ApprovalProof {
		return errors.New("worker tool approval proof is invalid")
	}
	return nil
}

func CloneToolInvocation(in ToolInvocation) ToolInvocation {
	out := in
	out.Record.Ciphertext = append([]byte(nil), in.Record.Ciphertext...)
	out.Record.Nonce = append([]byte(nil), in.Record.Nonce...)
	out.Result = append(json.RawMessage(nil), in.Result...)
	return out
}
