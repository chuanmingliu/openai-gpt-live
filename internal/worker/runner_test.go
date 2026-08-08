package worker

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/feature"
	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/reasoner"
	"github.com/chuanmingliu/openai-gpt-live/internal/safety"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type streamerFunc func(context.Context, reasoner.Request, func(reasoner.Chunk) error) (reasoner.Result, error)

func (f streamerFunc) Stream(ctx context.Context, request reasoner.Request, onChunk func(reasoner.Chunk) error) (reasoner.Result, error) {
	return f(ctx, request, onChunk)
}

type safetyPolicyFunc func(context.Context, safety.WorkerResultCandidate) (safety.Decision, error)

func (f safetyPolicyFunc) EvaluateWorkerResult(ctx context.Context, candidate safety.WorkerResultCandidate) (safety.Decision, error) {
	return f(ctx, candidate)
}

type workerSpanCapture struct {
	mu    sync.Mutex
	names []string
	attrs []string
}

func (c *workerSpanCapture) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, span := range spans {
		c.names = append(c.names, span.Name())
		for _, attr := range span.Attributes() {
			c.attrs = append(c.attrs, string(attr.Key)+"="+attr.Value.Emit())
		}
	}
	return nil
}
func (*workerSpanCapture) Shutdown(context.Context) error { return nil }

type memoryStore struct {
	mu                 sync.Mutex
	leases             []Lease
	leaseCalls         int
	markRunning        int
	heartbeat          func(context.Context, LeaseToken, time.Time) (Control, error)
	progress           []ProgressRecord
	retries            []RetryRecord
	needsInput         []NeedsInputRecord
	completed          []handoff.Result
	failed             []handoff.Failure
	cancelled          []CancelledRecord
	invocations        map[string]ToolInvocation
	onComplete         func()
	claimTool          func(LeaseToken, uint32) (ToolInvocation, bool, error)
	beginTool          func(ToolInvocationRecord) (ToolInvocation, bool, error)
	completeResult     func(handoff.Result) error
	progressResult     func(ProgressRecord) error
	retryResult        func(RetryRecord) error
	needsInputResult   func(NeedsInputRecord) error
	failResult         func(FailureRecord) error
	cancelResult       func(CancelledRecord) error
	completeToolResult func(ToolCompletionRecord) error
	leaseEntered       chan struct{}
	blockAcquisition   bool
}

func (s *memoryStore) LeaseNext(ctx context.Context, _ LeaseRequest) (Lease, bool, error) {
	s.mu.Lock()
	s.leaseCalls++
	if len(s.leases) > 0 {
		lease := s.leases[0]
		s.leases = s.leases[1:]
		s.mu.Unlock()
		return Lease{Token: lease.Token, Job: handoff.CloneSnapshot(lease.Job)}, true, nil
	}
	entered, block := s.leaseEntered, s.blockAcquisition
	s.mu.Unlock()
	if entered != nil {
		select {
		case entered <- struct{}{}:
		default:
			runtime.Gosched()
		}
	}
	if block {
		<-ctx.Done()
		return Lease{}, false, ctx.Err()
	}
	return Lease{}, false, nil
}

func (s *memoryStore) MarkRunning(context.Context, LeaseToken, time.Time) error {
	s.mu.Lock()
	s.markRunning++
	s.mu.Unlock()
	return nil
}
func (s *memoryStore) Heartbeat(ctx context.Context, token LeaseToken, expiry time.Time) (Control, error) {
	if s.heartbeat != nil {
		return s.heartbeat(ctx, token, expiry)
	}
	return Control{}, nil
}
func (s *memoryStore) ScheduleRetry(_ context.Context, _ LeaseToken, record RetryRecord) error {
	s.mu.Lock()
	s.retries = append(s.retries, record)
	result := s.retryResult
	s.mu.Unlock()
	if result != nil {
		return result(record)
	}
	return nil
}
func (s *memoryStore) AppendProgress(ctx context.Context, _ LeaseToken, record ProgressRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	s.progress = append(s.progress, record)
	result := s.progressResult
	s.mu.Unlock()
	if result != nil {
		return result(record)
	}
	return nil
}
func (s *memoryStore) MarkNeedsInput(_ context.Context, _ LeaseToken, record NeedsInputRecord) error {
	s.mu.Lock()
	s.needsInput = append(s.needsInput, record)
	done := s.onComplete
	result := s.needsInputResult
	s.mu.Unlock()
	if result != nil {
		if err := result(record); err != nil {
			return err
		}
	}
	if done != nil {
		done()
	}
	return nil
}
func (s *memoryStore) Complete(_ context.Context, _ LeaseToken, record CompletionRecord) error {
	result := record.Result
	s.mu.Lock()
	s.completed = append(s.completed, result)
	done := s.onComplete
	completeResult := s.completeResult
	s.mu.Unlock()
	if completeResult != nil {
		if err := completeResult(result); err != nil {
			return err
		}
	}
	if done != nil {
		done()
	}
	return nil
}
func (s *memoryStore) Fail(_ context.Context, _ LeaseToken, record FailureRecord) error {
	failure := record.Failure
	s.mu.Lock()
	s.failed = append(s.failed, failure)
	done := s.onComplete
	result := s.failResult
	s.mu.Unlock()
	if result != nil {
		if err := result(record); err != nil {
			return err
		}
	}
	if done != nil {
		done()
	}
	return nil
}
func (s *memoryStore) MarkCancelled(_ context.Context, _ LeaseToken, record CancelledRecord) error {
	s.mu.Lock()
	s.cancelled = append(s.cancelled, record)
	done := s.onComplete
	result := s.cancelResult
	s.mu.Unlock()
	if result != nil {
		if err := result(record); err != nil {
			return err
		}
	}
	if done != nil {
		done()
	}
	return nil
}

func (s *memoryStore) ClaimApprovedToolInvocation(_ context.Context, token LeaseToken, revision uint32) (ToolInvocation, bool, error) {
	if s.claimTool != nil {
		return s.claimTool(token, revision)
	}
	return ToolInvocation{}, false, nil
}

func (s *memoryStore) BeginToolInvocation(_ context.Context, _ LeaseToken, record ToolInvocationRecord) (ToolInvocation, bool, error) {
	if s.beginTool != nil {
		return s.beginTool(record)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.invocations == nil {
		s.invocations = make(map[string]ToolInvocation)
	}
	if existing, ok := s.invocations[record.InvocationID]; ok {
		return CloneToolInvocation(existing), false, nil
	}
	invocation := canonicalInvocation(record, "running", nil, false, ApprovalNotRequired)
	s.invocations[record.InvocationID] = invocation
	return CloneToolInvocation(invocation), true, nil
}

func canonicalInvocation(record ToolInvocationRecord, status string, result json.RawMessage, required bool, state ApprovalState) ToolInvocation {
	invocation := ToolInvocation{Record: record, Status: status, Result: append(json.RawMessage(nil), result...)}
	SetCanonicalToolApproval(&invocation, required, state)
	return invocation
}
func (s *memoryStore) CompleteToolInvocation(_ context.Context, _ LeaseToken, record ToolCompletionRecord) error {
	s.mu.Lock()
	if s.invocations == nil {
		s.invocations = make(map[string]ToolInvocation)
	}
	invocation := s.invocations[record.InvocationID]
	invocation.Status = "succeeded"
	invocation.Result = append(json.RawMessage(nil), record.Result...)
	s.invocations[record.InvocationID] = invocation
	result := s.completeToolResult
	s.mu.Unlock()
	if result != nil {
		return result(record)
	}
	return nil
}
func (s *memoryStore) SweepExpired(context.Context, time.Time, int) (int, error) { return 0, nil }

func workerLease() Lease {
	now := time.Now().UTC()
	return Lease{
		Token: LeaseToken{TenantID: "tenant", ConversationID: "conversation", HandoffID: "handoff", WorkerID: "worker", Version: 1, Attempt: 1},
		Job: handoff.Snapshot{ID: "handoff", State: handoff.StateLeased, CreatedAt: now, RetryCount: 0, Request: handoff.CreateRequest{
			Scope: handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "conversation"}, SessionID: "session", UserRequest: "research this", Objective: "objective sentinel",
			ContextRef: "context-ref", ContextHash: "context-hash", Deadline: now.Add(time.Hour), TokenBudget: 1024,
			ToolAllowlist: []string{}, ApprovalPolicy: json.RawMessage(`{"mode":"none"}`), Versions: handoff.Versions{Model: "model-v1", Prompt: "prompt-v1", ToolRegistry: "tools-v1", Config: "config-v1"},
		}},
	}
}

func allowPolicy() safety.Policy {
	return safetyPolicyFunc(func(_ context.Context, _ safety.WorkerResultCandidate) (safety.Decision, error) {
		return safety.Decision{Action: safety.ActionAllow, ReasonCode: "allowed"}, nil
	})
}

func testRunner(store Store, stream reasoner.Streamer, policy safety.Policy, workerClock clock.Clock, jitter clock.JitterSource, registry ToolRegistry) Runner {
	return Runner{Store: store, Reasoner: stream, Safety: policy, Tools: registry, Clock: workerClock, Jitter: jitter,
		Retry: RetryPolicy{Base: time.Second, Maximum: time.Minute, MaxRetries: 3}, WorkerID: "worker", LeaseDuration: 9 * time.Second, Concurrency: 1,
		PollInterval: time.Second, MaxOutputBytes: 64 << 10, MaxOutputTokens: 4096, MaxToolRounds: 3, MaxToolCalls: 8, MaxToolArgumentBytes: 16 << 10, MaxToolOutputBytes: 16 << 10,
		ProviderSafetyID: func(tenant, user string) (string, error) { return "private-safety-id", nil },
	}
}

func TestRunnerNormalCompletionPersistsSanitizedResultAndSafetyDecision(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	store := &memoryStore{leases: []Lease{workerLease()}, onComplete: cancel}
	registry, err := NewToolRegistry(map[string][]RegisteredTool{"tools-v1": {}})
	require.NoError(t, err)
	stream := streamerFunc(func(_ context.Context, request reasoner.Request, onChunk func(reasoner.Chunk) error) (reasoner.Result, error) {
		assert.Equal(t, "private-safety-id", request.SafetyUserID)
		assert.NotContains(t, request.Messages[0].Content, "objective sentinel")
		assert.Contains(t, request.Messages[1].Content, "objective sentinel")
		require.NoError(t, onChunk(reasoner.Chunk{Text: "done"}))
		return reasoner.Result{Text: "done", FinishReason: "stop", Usage: reasoner.Usage{PromptTokens: 2, CompletionTokens: 1, TotalTokens: 3}}, nil
	})
	runner := testRunner(store, stream, allowPolicy(), clock.Real{}, fixedJitter(0), registry)
	require.NoError(t, runner.Run(ctx))
	require.Len(t, store.completed, 1)
	assert.Equal(t, "done", store.completed[0].Summary)
	assert.JSONEq(t, `{"action":"allow","reason_code":"allowed"}`, string(store.completed[0].SafetyDecision))
	assert.Empty(t, store.failed)
}

type mutableShadowGate struct{ shadow atomic.Bool }

func (g *mutableShadowGate) Enabled(name string) bool {
	return name == feature.ShadowMode && g.shadow.Load()
}

func TestRunnerShadowExcludesSideEffectsButKeepsReadOnlyDefinitions(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	lease := workerLease()
	lease.Job.Request.ToolAllowlist = []string{"lookup", "charge"}
	store := &memoryStore{leases: []Lease{lease}, onComplete: cancel}
	registry, err := NewToolRegistry(map[string][]RegisteredTool{"tools-v1": {
		{Definition: reasoner.ToolDefinition{Name: "lookup", Parameters: json.RawMessage(`{"type":"object"}`)}, ReadOnly: true, Executor: toolExecutorFunc(func(context.Context, ToolExecutionRequest) (json.RawMessage, error) {
			return json.RawMessage(`{}`), nil
		})},
		{Definition: reasoner.ToolDefinition{Name: "charge", Parameters: json.RawMessage(`{"type":"object"}`)}, SideEffecting: true, StableIdempotency: true, Executor: toolExecutorFunc(func(context.Context, ToolExecutionRequest) (json.RawMessage, error) {
			t.Fatal("side effect executed")
			return nil, nil
		})},
	}})
	require.NoError(t, err)
	stream := streamerFunc(func(_ context.Context, request reasoner.Request, _ func(reasoner.Chunk) error) (reasoner.Result, error) {
		require.Len(t, request.Tools, 1)
		assert.Equal(t, "lookup", request.Tools[0].Name)
		return reasoner.Result{Text: "done"}, nil
	})
	gates := &mutableShadowGate{}
	gates.shadow.Store(true)
	runner := testRunner(store, stream, allowPolicy(), clock.Real{}, fixedJitter(0), registry)
	runner.FeatureGates = gates
	require.NoError(t, runner.Run(ctx))
	require.Len(t, store.completed, 1)
}

func TestRunnerRechecksShadowImmediatelyBeforeSideEffectInvocation(t *testing.T) {
	lease := workerLease()
	lease.Job.Request.ToolAllowlist = []string{"charge"}
	var executions atomic.Int32
	registry, err := NewToolRegistry(map[string][]RegisteredTool{"tools-v1": {{
		Definition: reasoner.ToolDefinition{Name: "charge", Parameters: json.RawMessage(`{"type":"object"}`)}, SideEffecting: true, StableIdempotency: true,
		Executor: toolExecutorFunc(func(context.Context, ToolExecutionRequest) (json.RawMessage, error) {
			executions.Add(1)
			return json.RawMessage(`{"charged":true}`), nil
		}),
	}}})
	require.NoError(t, err)
	gates := &mutableShadowGate{}
	store := &memoryStore{}
	store.beginTool = func(record ToolInvocationRecord) (ToolInvocation, bool, error) {
		gates.shadow.Store(true)
		return canonicalInvocation(record, "running", nil, false, ApprovalNotRequired), true, nil
	}
	stream := streamerFunc(func(_ context.Context, _ reasoner.Request, _ func(reasoner.Chunk) error) (reasoner.Result, error) {
		return reasoner.Result{ToolCalls: []reasoner.ToolCall{{ID: "call", Name: "charge", Arguments: json.RawMessage(`{}`)}}}, nil
	})
	runner := testRunner(store, stream, allowPolicy(), clock.Real{}, fixedJitter(0), registry)
	runner.FeatureGates = gates
	runner.ArgumentSealer, _ = NewAESArgumentSealer("test-key", bytes.Repeat([]byte{5}, 32), bytes.NewReader(bytes.Repeat([]byte{1}, 12)))
	require.NoError(t, runner.executeLease(t.Context(), lease))
	assert.Zero(t, executions.Load())
	require.Len(t, store.failed, 1)
}

func TestRunnerExecutionSpanReachesExporterWithoutRawIdentityAttributes(t *testing.T) {
	store := &memoryStore{}
	registry, err := NewToolRegistry(map[string][]RegisteredTool{"tools-v1": {}})
	require.NoError(t, err)
	stream := streamerFunc(func(_ context.Context, _ reasoner.Request, onChunk func(reasoner.Chunk) error) (reasoner.Result, error) {
		require.NoError(t, onChunk(reasoner.Chunk{Text: "done"}))
		return reasoner.Result{Text: "done", FinishReason: "stop"}, nil
	})
	capture := &workerSpanCapture{}
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(capture))
	t.Cleanup(func() { _ = provider.Shutdown(t.Context()) })
	runner := testRunner(store, stream, allowPolicy(), clock.Real{}, fixedJitter(0), registry)
	runner.Tracer = provider.Tracer("test")
	lease := workerLease()
	lease.Token.TenantID = "raw-private-tenant"
	lease.Job.Request.Scope.TenantID = lease.Token.TenantID
	require.NoError(t, runner.executeLease(t.Context(), lease))
	capture.mu.Lock()
	defer capture.mu.Unlock()
	assert.Contains(t, capture.names, "worker.execute")
	assert.Equal(t, []string{"result=ok"}, capture.attrs)
	assert.NotContains(t, strings.Join(capture.attrs, " "), lease.Token.TenantID)
}

func TestRunnerDeliversDurableInputInFirstReasonerRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	lease := workerLease()
	lease.Job.InputRevision, lease.Job.ConsumedInput = 2, 2
	lease.Job.Input = json.RawMessage(`{"answer":{"choice":"one"}}`)
	store := &memoryStore{leases: []Lease{lease}, onComplete: cancel}
	registry, _ := NewToolRegistry(map[string][]RegisteredTool{"tools-v1": {}})
	stream := streamerFunc(func(_ context.Context, request reasoner.Request, _ func(reasoner.Chunk) error) (reasoner.Result, error) {
		require.Len(t, request.Messages, 2)
		assert.Contains(t, request.Messages[1].Content, `"input":{"answer":{"choice":"one"}}`)
		return reasoner.Result{Text: "done", FinishReason: "stop"}, nil
	})
	runner := testRunner(store, stream, allowPolicy(), clock.Real{}, fixedJitter(0), registry)
	require.NoError(t, runner.Run(ctx))
	lease.Job.Input[0] = '['
	require.Len(t, store.completed, 1)
}

func TestRunnerRetryableProviderErrorSchedulesOneStableRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	store := &memoryStore{leases: []Lease{workerLease()}, onComplete: cancel}
	registry, _ := NewToolRegistry(map[string][]RegisteredTool{"tools-v1": {}})
	stream := streamerFunc(func(context.Context, reasoner.Request, func(reasoner.Chunk) error) (reasoner.Result, error) {
		return reasoner.Result{}, &reasoner.ProviderError{Code: "busy", Retryable: true, RetryAfter: 5 * time.Second}
	})
	runner := testRunner(store, stream, allowPolicy(), clock.Real{}, fixedJitter(time.Second), registry)
	store.onComplete = nil
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx) }()
	for {
		store.mu.Lock()
		scheduled := len(store.retries)
		store.mu.Unlock()
		if scheduled == 1 {
			break
		}
		select {
		case <-t.Context().Done():
			t.Fatal("retry was not scheduled")
		default:
		}
	}
	cancel()
	require.NoError(t, <-runDone)
	store.mu.Lock()
	defer store.mu.Unlock()
	require.Len(t, store.retries, 1)
	assert.Equal(t, uint32(1), store.retries[0].RetryCount)
	assert.Equal(t, "busy", store.retries[0].Code)
	assert.True(t, store.retries[0].ReadyAt.After(time.Now()))
}

func TestRunnerSafetyDenialFailsWithoutCompleting(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	store := &memoryStore{leases: []Lease{workerLease()}, onComplete: cancel}
	registry, _ := NewToolRegistry(map[string][]RegisteredTool{"tools-v1": {}})
	stream := streamerFunc(func(context.Context, reasoner.Request, func(reasoner.Chunk) error) (reasoner.Result, error) {
		return reasoner.Result{Text: "unsafe", FinishReason: "stop"}, nil
	})
	runner := testRunner(store, stream, safety.DenyAll{}, clock.Real{}, fixedJitter(0), registry)
	require.NoError(t, runner.Run(ctx))
	assert.Empty(t, store.completed)
	require.Len(t, store.failed, 1)
	assert.Equal(t, "safety_suppressed", store.failed[0].Code)
	assert.NotContains(t, store.failed[0].Message, "unsafe")
}

func TestRunnerSafetyErrorMalformedAndRedactionPaths(t *testing.T) {
	for _, tc := range []struct {
		name         string
		policy       safety.Policy
		wantComplete bool
	}{
		{"error", safetyPolicyFunc(func(context.Context, safety.WorkerResultCandidate) (safety.Decision, error) {
			return safety.Decision{}, errors.New("policy unavailable")
		}), false},
		{"malformed", safetyPolicyFunc(func(context.Context, safety.WorkerResultCandidate) (safety.Decision, error) {
			return safety.Decision{Action: "unknown", ReasonCode: "bad"}, nil
		}), false},
		{"redact", safetyPolicyFunc(func(context.Context, safety.WorkerResultCandidate) (safety.Decision, error) {
			return safety.Decision{Action: safety.ActionRedact, SafeSummary: "safe summary", ReasonCode: "redacted"}, nil
		}), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			store := &memoryStore{leases: []Lease{workerLease()}, onComplete: cancel}
			registry, _ := NewToolRegistry(map[string][]RegisteredTool{"tools-v1": {}})
			stream := streamerFunc(func(context.Context, reasoner.Request, func(reasoner.Chunk) error) (reasoner.Result, error) {
				return reasoner.Result{Text: "unsafe detail", FinishReason: "stop"}, nil
			})
			runner := testRunner(store, stream, tc.policy, clock.Real{}, fixedJitter(0), registry)
			require.NoError(t, runner.Run(ctx))
			if tc.wantComplete {
				require.Len(t, store.completed, 1)
				assert.Equal(t, "safe summary", store.completed[0].Summary)
				assert.JSONEq(t, `{}`, string(store.completed[0].Structured))
				assert.NotContains(t, string(store.completed[0].SafetyDecision), "unsafe detail")
			} else {
				assert.Empty(t, store.completed)
				require.Len(t, store.failed, 1)
				assert.NotContains(t, store.failed[0].Message, "unsafe detail")
			}
		})
	}
}

func TestRunnerSafetyClarificationUsesConstantDurableMutationWithoutCandidate(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	store := &memoryStore{leases: []Lease{workerLease()}, onComplete: cancel}
	registry, _ := NewToolRegistry(map[string][]RegisteredTool{"tools-v1": {}})
	stream := streamerFunc(func(context.Context, reasoner.Request, func(reasoner.Chunk) error) (reasoner.Result, error) {
		return reasoner.Result{Text: "unsafe-candidate-sentinel", FinishReason: "stop"}, nil
	})
	policy := safetyPolicyFunc(func(context.Context, safety.WorkerResultCandidate) (safety.Decision, error) {
		return safety.Decision{Action: safety.ActionRequestClarification, SafeSummary: "Approval is required before this task can continue.", ReasonCode: "approval_required"}, nil
	})
	runner := testRunner(store, stream, policy, clock.Real{}, fixedJitter(0), registry)
	require.NoError(t, runner.Run(ctx))
	assert.Empty(t, store.completed)
	assert.Empty(t, store.failed)
	require.Len(t, store.needsInput, 1)
	encoded, err := json.Marshal(store.needsInput[0])
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), "unsafe-candidate-sentinel")
	assert.Equal(t, "Approval is required before this task can continue.", store.needsInput[0].Prompt)
}

func TestRunnerProviderBlockedResultReachesPolicyWithoutProviderBytes(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	store := &memoryStore{leases: []Lease{workerLease()}, onComplete: cancel}
	registry, _ := NewToolRegistry(map[string][]RegisteredTool{"tools-v1": {}})
	stream := streamerFunc(func(context.Context, reasoner.Request, func(reasoner.Chunk) error) (reasoner.Result, error) {
		return reasoner.Result{FinishReason: "content_filter", ProviderBlocked: true}, nil
	})
	var seen safety.WorkerResultCandidate
	policy := safetyPolicyFunc(func(_ context.Context, candidate safety.WorkerResultCandidate) (safety.Decision, error) {
		seen = candidate.Clone()
		return safety.Decision{Action: safety.ActionTerminateSession, ReasonCode: "provider_blocked"}, nil
	})
	runner := testRunner(store, stream, policy, clock.Real{}, fixedJitter(0), registry)
	require.NoError(t, runner.Run(ctx))
	assert.True(t, seen.ProviderBlocked)
	assert.Empty(t, store.completed)
	require.Len(t, store.failed, 1)
	assert.Equal(t, "safety_terminated", store.failed[0].Code)
}

func TestRunnerLostLeaseCancelsReasonerAndNeverMutatesTerminal(t *testing.T) {
	manual := clock.NewManual(workerLease().Job.CreatedAt)
	started, cancelled := make(chan struct{}), make(chan struct{})
	store := &memoryStore{leases: []Lease{workerLease()}, heartbeat: func(context.Context, LeaseToken, time.Time) (Control, error) { return Control{}, ErrLeaseLost }}
	registry, _ := NewToolRegistry(map[string][]RegisteredTool{"tools-v1": {}})
	stream := streamerFunc(func(ctx context.Context, _ reasoner.Request, _ func(reasoner.Chunk) error) (reasoner.Result, error) {
		close(started)
		<-ctx.Done()
		close(cancelled)
		return reasoner.Result{}, ctx.Err()
	})
	ctx, cancel := context.WithCancel(t.Context())
	runner := testRunner(store, stream, allowPolicy(), manual, fixedJitter(0), registry)
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	<-started
lost:
	for {
		manual.Advance(3 * time.Second)
		select {
		case <-cancelled:
			break lost
		default:
		}
	}
	cancel()
	require.NoError(t, <-done)
	assert.Empty(t, store.completed)
	assert.Empty(t, store.failed)
	assert.Empty(t, store.retries)
	assert.Empty(t, store.cancelled)
}

func TestRunnerSteeringRestartsWithoutProviderRetry(t *testing.T) {
	manual := clock.NewManual(workerLease().Job.CreatedAt)
	firstStarted := make(chan struct{})
	var calls atomic.Int32
	store := &memoryStore{leases: []Lease{workerLease()}}
	store.heartbeat = func(context.Context, LeaseToken, time.Time) (Control, error) {
		return Control{ObjectiveRevision: 1, Objective: "revised objective"}, nil
	}
	ctx, cancel := context.WithCancel(t.Context())
	store.onComplete = cancel
	registry, _ := NewToolRegistry(map[string][]RegisteredTool{"tools-v1": {}})
	stream := streamerFunc(func(ctx context.Context, request reasoner.Request, _ func(reasoner.Chunk) error) (reasoner.Result, error) {
		if calls.Add(1) == 1 {
			close(firstStarted)
			<-ctx.Done()
			return reasoner.Result{}, ctx.Err()
		}
		assert.Contains(t, request.Messages[1].Content, "revised objective")
		return reasoner.Result{Text: "revised result", FinishReason: "stop"}, nil
	})
	runner := testRunner(store, stream, allowPolicy(), manual, fixedJitter(0), registry)
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	<-firstStarted
	for calls.Load() == 1 {
		manual.Advance(3 * time.Second)
		runtime.Gosched()
	}
	require.NoError(t, <-done)
	assert.Equal(t, int32(2), calls.Load())
	assert.Empty(t, store.retries)
	require.Len(t, store.completed, 1)
}

func TestRunnerRevisionFenceDuringProgressRestartsImmediatelyOnHeldLease(t *testing.T) {
	lease := workerLease()
	var progressAttempts, generations atomic.Int32
	store := &memoryStore{heartbeat: func(context.Context, LeaseToken, time.Time) (Control, error) {
		return Control{ObjectiveRevision: 1, Objective: "revised objective"}, nil
	}}
	store.progressResult = func(ProgressRecord) error {
		if progressAttempts.Add(1) == 1 {
			return ErrLeaseLost
		}
		return nil
	}
	registry, _ := NewToolRegistry(map[string][]RegisteredTool{"tools-v1": {}})
	stream := streamerFunc(func(_ context.Context, request reasoner.Request, onChunk func(reasoner.Chunk) error) (reasoner.Result, error) {
		if generations.Add(1) == 1 {
			return reasoner.Result{}, onChunk(reasoner.Chunk{Text: "discarded"})
		}
		assert.Contains(t, request.Messages[1].Content, "revised objective")
		return reasoner.Result{Text: "revised result"}, nil
	})
	runner := testRunner(store, stream, allowPolicy(), clock.Real{}, fixedJitter(0), registry)
	require.NoError(t, runner.executeLease(t.Context(), lease))
	assert.Equal(t, int32(2), generations.Load())
	assert.Equal(t, 1, store.markRunning)
	require.Len(t, store.completed, 1)
	assert.Equal(t, "revised result", store.completed[0].Summary)
}

func TestRunnerRevisionFenceDuringToolBeginAndCompletionRestartsImmediately(t *testing.T) {
	for _, boundary := range []string{"begin", "completion"} {
		t.Run(boundary, func(t *testing.T) {
			lease := workerLease()
			lease.Job.Request.ToolAllowlist = []string{"charge"}
			var generations, executions, begins, completions atomic.Int32
			tool := RegisteredTool{Definition: reasoner.ToolDefinition{Name: "charge", Parameters: json.RawMessage(`{"type":"object"}`)}, SideEffecting: true, StableIdempotency: true, Executor: toolExecutorFunc(func(context.Context, ToolExecutionRequest) (json.RawMessage, error) {
				executions.Add(1)
				return json.RawMessage(`{"ok":true}`), nil
			})}
			registry, err := NewToolRegistry(map[string][]RegisteredTool{"tools-v1": {tool}})
			require.NoError(t, err)
			store := &memoryStore{heartbeat: func(context.Context, LeaseToken, time.Time) (Control, error) {
				return Control{ObjectiveRevision: 1, Objective: "revised objective"}, nil
			}}
			if boundary == "begin" {
				store.beginTool = func(record ToolInvocationRecord) (ToolInvocation, bool, error) {
					if begins.Add(1) == 1 {
						return ToolInvocation{}, false, ErrLeaseLost
					}
					return canonicalInvocation(record, "running", nil, true, ApprovalApproved), true, nil
				}
			} else {
				store.completeToolResult = func(ToolCompletionRecord) error {
					if completions.Add(1) == 1 {
						return ErrLeaseLost
					}
					return nil
				}
			}
			stream := streamerFunc(func(_ context.Context, request reasoner.Request, _ func(reasoner.Chunk) error) (reasoner.Result, error) {
				if generations.Add(1) == 1 {
					return reasoner.Result{ToolCalls: []reasoner.ToolCall{{ID: "call", Name: "charge", Arguments: json.RawMessage(`{"amount":1}`)}}}, nil
				}
				assert.Contains(t, request.Messages[1].Content, "revised objective")
				return reasoner.Result{Text: "revised result"}, nil
			})
			runner := testRunner(store, stream, allowPolicy(), clock.NewManual(lease.Job.CreatedAt), fixedJitter(0), registry)
			nonces := make([]byte, 24)
			for i := range nonces {
				nonces[i] = byte(i + 1)
			}
			runner.ArgumentSealer, err = NewAESArgumentSealer("test-key", bytes.Repeat([]byte{5}, 32), bytes.NewReader(nonces))
			require.NoError(t, err)
			require.NoError(t, runner.executeLease(t.Context(), lease))
			assert.Equal(t, int32(2), generations.Load())
			if boundary == "begin" {
				assert.Zero(t, executions.Load())
			} else {
				assert.Equal(t, int32(1), executions.Load())
			}
			require.Len(t, store.completed, 1)
		})
	}
}

func TestRunnerApprovalProvenanceResetsWhenSteeringStartsNewRevision(t *testing.T) {
	manual := clock.NewManual(workerLease().Job.CreatedAt)
	lease := workerLease()
	lease.Job.CreatedAt = manual.Now()
	lease.Job.Request.Deadline = manual.Now().Add(time.Hour)
	lease.Job.Request.ToolAllowlist = []string{"charge"}
	approved := make(chan struct{})
	waitingForSteer := make(chan struct{})
	var beginOnce, waitOnce sync.Once
	store := &memoryStore{heartbeat: func(context.Context, LeaseToken, time.Time) (Control, error) {
		return Control{ObjectiveRevision: 1, Objective: "revised objective"}, nil
	}}
	store.beginTool = func(record ToolInvocationRecord) (ToolInvocation, bool, error) {
		beginOnce.Do(func() { close(approved) })
		return canonicalInvocation(record, "running", nil, true, ApprovalApproved), true, nil
	}
	tool := RegisteredTool{Definition: reasoner.ToolDefinition{Name: "charge", Parameters: json.RawMessage(`{"type":"object"}`)}, SideEffecting: true, StableIdempotency: true, Executor: toolExecutorFunc(func(context.Context, ToolExecutionRequest) (json.RawMessage, error) {
		return json.RawMessage(`{"ok":true}`), nil
	})}
	registry, err := NewToolRegistry(map[string][]RegisteredTool{"tools-v1": {tool}})
	require.NoError(t, err)
	var streams atomic.Int32
	stream := streamerFunc(func(ctx context.Context, request reasoner.Request, _ func(reasoner.Chunk) error) (reasoner.Result, error) {
		switch streams.Add(1) {
		case 1:
			return reasoner.Result{FinishReason: "tool_calls", ToolCalls: []reasoner.ToolCall{{ID: "call", Name: "charge", Arguments: json.RawMessage(`{"amount":1}`)}}}, nil
		case 2:
			waitOnce.Do(func() { close(waitingForSteer) })
			<-ctx.Done()
			return reasoner.Result{}, ctx.Err()
		default:
			assert.Contains(t, request.Messages[1].Content, "revised objective")
			return reasoner.Result{Text: "revision one result", FinishReason: "stop"}, nil
		}
	})
	var seen safety.WorkerResultCandidate
	policy := safetyPolicyFunc(func(_ context.Context, candidate safety.WorkerResultCandidate) (safety.Decision, error) {
		seen = candidate.Clone()
		return safety.Decision{Action: safety.ActionAllow, ReasonCode: "allowed"}, nil
	})
	runner := testRunner(store, stream, policy, manual, fixedJitter(0), registry)
	runner.ArgumentSealer, err = NewAESArgumentSealer("test-key", bytes.Repeat([]byte{5}, 32), bytes.NewReader(bytes.Repeat([]byte{1}, 24)))
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- runner.executeLease(t.Context(), lease) }()
	<-approved
	<-waitingForSteer
	var runErr error
	completed := false
	deadline := time.After(2 * time.Second)
	for !completed {
		manual.Advance(3 * time.Second)
		runtime.Gosched()
		select {
		case runErr = <-done:
			completed = true
		case <-deadline:
			t.Fatal("steered generation did not complete")
		default:
		}
	}
	require.NoError(t, runErr)
	assert.False(t, seen.RequiresApproval)
	assert.False(t, seen.Approved)
	require.Len(t, store.completed, 1)
	assert.Equal(t, "revision one result", store.completed[0].Summary)
}

func TestRunnerApprovalProvenanceUsesCurrentCanonicalBeginRetryOrReplay(t *testing.T) {
	for _, mode := range []string{"current", "ambiguous retry", "succeeded replay"} {
		t.Run(mode, func(t *testing.T) {
			var executions, beginAttempts atomic.Int32
			tool := RegisteredTool{Definition: reasoner.ToolDefinition{Name: "charge", Parameters: json.RawMessage(`{"type":"object"}`)}, SideEffecting: true, StableIdempotency: true, Executor: toolExecutorFunc(func(context.Context, ToolExecutionRequest) (json.RawMessage, error) {
				executions.Add(1)
				return json.RawMessage(`{"ok":true}`), nil
			})}
			var streams atomic.Int32
			stream := streamerFunc(func(context.Context, reasoner.Request, func(reasoner.Chunk) error) (reasoner.Result, error) {
				if streams.Add(1) == 1 {
					return reasoner.Result{FinishReason: "tool_calls", ToolCalls: []reasoner.ToolCall{{ID: "call", Name: "charge", Arguments: json.RawMessage(`{"amount":1}`)}}}, nil
				}
				return reasoner.Result{Text: "done", FinishReason: "stop"}, nil
			})
			ctx, _, store, runner := toolRunnerFixture(t, []RegisteredTool{tool}, stream)
			store.beginTool = func(record ToolInvocationRecord) (ToolInvocation, bool, error) {
				attempt := beginAttempts.Add(1)
				if mode == "ambiguous retry" && attempt == 1 {
					return ToolInvocation{}, false, errors.New("begin response unavailable")
				}
				if mode == "succeeded replay" {
					return canonicalInvocation(record, "succeeded", json.RawMessage(`{"ok":true}`), true, ApprovalApproved), false, nil
				}
				return canonicalInvocation(record, "running", nil, true, ApprovalApproved), true, nil
			}
			var seen safety.WorkerResultCandidate
			runner.Safety = safetyPolicyFunc(func(_ context.Context, candidate safety.WorkerResultCandidate) (safety.Decision, error) {
				seen = candidate.Clone()
				return safety.Decision{Action: safety.ActionAllow, ReasonCode: "allowed"}, nil
			})
			require.NoError(t, runner.Run(ctx))
			assert.True(t, seen.RequiresApproval)
			assert.True(t, seen.Approved)
			if mode == "ambiguous retry" {
				assert.Equal(t, int32(2), beginAttempts.Load())
			}
			if mode == "succeeded replay" {
				assert.Zero(t, executions.Load())
			} else {
				assert.Equal(t, int32(1), executions.Load())
			}
		})
	}
}

func TestJobExecutionApprovalFenceRejectsStaleSameRevisionGeneration(t *testing.T) {
	execution := &jobExecution{}
	stale := execution.beginGeneration(4)
	current := execution.beginGeneration(4)
	execution.markApproved(4, stale)
	required, approved := execution.approval(4, current)
	assert.False(t, required)
	assert.False(t, approved)
	execution.markApproved(4, current)
	required, approved = execution.approval(4, current)
	assert.True(t, required)
	assert.True(t, approved)
}

func TestRunnerProgressCoalescesThousandsOfChunksAndObservesFirstToken(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	store := &memoryStore{leases: []Lease{workerLease()}, onComplete: cancel}
	registry, _ := NewToolRegistry(map[string][]RegisteredTool{"tools-v1": {}})
	var firstTokens atomic.Int32
	stream := streamerFunc(func(_ context.Context, _ reasoner.Request, onChunk func(reasoner.Chunk) error) (reasoner.Result, error) {
		for range 5000 {
			if err := onChunk(reasoner.Chunk{Text: "x"}); err != nil {
				return reasoner.Result{}, err
			}
		}
		return reasoner.Result{Text: "summary", FinishReason: "stop"}, nil
	})
	runner := testRunner(store, stream, allowPolicy(), clock.NewManual(workerLease().Job.CreatedAt), fixedJitter(0), registry)
	runner.FirstTokenObserver = func(LeaseToken, time.Time) { firstTokens.Add(1) }
	require.NoError(t, runner.Run(ctx))
	assert.Equal(t, int32(1), firstTokens.Load())
	assert.LessOrEqual(t, len(store.progress), 1)
	for _, progress := range store.progress {
		assert.NotContains(t, progress.Summary, "x")
	}
}

func TestProgressMutationIDIsReplayStableAndGenerationScoped(t *testing.T) {
	token := workerLease().Token
	base := progressMutationID(token, 3, 2, 1)
	assert.Equal(t, base, progressMutationID(token, 3, 2, 1))
	changed := []string{
		progressMutationID(LeaseToken{TenantID: token.TenantID, ConversationID: token.ConversationID, HandoffID: token.HandoffID, WorkerID: token.WorkerID, Version: token.Version + 1, Attempt: token.Attempt}, 3, 2, 1),
		progressMutationID(LeaseToken{TenantID: token.TenantID, ConversationID: token.ConversationID, HandoffID: token.HandoffID, WorkerID: token.WorkerID, Version: token.Version, Attempt: token.Attempt + 1}, 3, 2, 1),
		progressMutationID(token, 4, 2, 1),
		progressMutationID(token, 3, 3, 1),
		progressMutationID(token, 3, 2, 2),
	}
	for _, candidate := range changed {
		assert.NotEqual(t, base, candidate)
	}
}

func TestProgressSummaryCrossesAggregateLimitOnFirstByte(t *testing.T) {
	store := &memoryStore{}
	builder := &resultBuilder{ctx: t.Context(), limit: len("x") + len("generation in progress") - 1, clock: clock.NewManual(time.Now()), store: store, token: workerLease().Token}
	err := builder.onChunk(reasoner.Chunk{Text: "x"})
	assert.ErrorIs(t, err, errOutputLimit)
	assert.Empty(t, store.progress)
}

func TestRunnerUsesExactlyConfiguredLongLivedAcquisitionLoops(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	store := &memoryStore{blockAcquisition: true, leaseEntered: make(chan struct{}, 8)}
	registry, _ := NewToolRegistry(map[string][]RegisteredTool{})
	runner := testRunner(store, streamerFunc(func(context.Context, reasoner.Request, func(reasoner.Chunk) error) (reasoner.Result, error) {
		return reasoner.Result{}, nil
	}), allowPolicy(), clock.Real{}, fixedJitter(0), registry)
	runner.Concurrency = 3
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	for range 3 {
		<-store.leaseEntered
	}
	cancel()
	require.NoError(t, <-done)
	store.mu.Lock()
	defer store.mu.Unlock()
	assert.Equal(t, 3, store.leaseCalls)
}

func TestNewRunnerRejectsTypedNilDependenciesAndInvalidBounds(t *testing.T) {
	registry, _ := NewToolRegistry(map[string][]RegisteredTool{})
	valid := testRunner(&memoryStore{}, streamerFunc(func(context.Context, reasoner.Request, func(reasoner.Chunk) error) (reasoner.Result, error) {
		return reasoner.Result{}, nil
	}), allowPolicy(), clock.Real{}, fixedJitter(0), registry)
	_, err := NewRunner(valid)
	require.NoError(t, err)
	var nilStore *memoryStore
	candidate := valid
	candidate.Store = nilStore
	_, err = NewRunner(candidate)
	assert.Error(t, err)
	var nilPolicy *safety.DenyAll
	candidate = valid
	candidate.Safety = nilPolicy
	_, err = NewRunner(candidate)
	assert.Error(t, err)
	candidate = valid
	candidate.LeaseDuration = time.Duration(2)
	_, err = NewRunner(candidate)
	assert.Error(t, err)
}

func TestRunnerShutdownWithHeldLeaseDoesNotCompleteOrFail(t *testing.T) {
	started := make(chan struct{})
	store := &memoryStore{leases: []Lease{workerLease()}}
	registry, _ := NewToolRegistry(map[string][]RegisteredTool{"tools-v1": {}})
	stream := streamerFunc(func(ctx context.Context, _ reasoner.Request, _ func(reasoner.Chunk) error) (reasoner.Result, error) {
		close(started)
		<-ctx.Done()
		return reasoner.Result{}, ctx.Err()
	})
	ctx, cancel := context.WithCancel(t.Context())
	runner := testRunner(store, stream, allowPolicy(), clock.Real{}, fixedJitter(0), registry)
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	<-started
	cancel()
	require.NoError(t, <-done)
	assert.Empty(t, store.completed)
	assert.Empty(t, store.failed)
	assert.Empty(t, store.retries)
	assert.Empty(t, store.cancelled)
}

func toolRunnerFixture(t *testing.T, tools []RegisteredTool, stream reasoner.Streamer) (context.Context, context.CancelFunc, *memoryStore, Runner) {
	t.Helper()
	lease := workerLease()
	lease.Job.Request.ToolAllowlist = make([]string, len(tools))
	for index := range tools {
		lease.Job.Request.ToolAllowlist[index] = tools[index].Definition.Name
	}
	registry, err := NewToolRegistry(map[string][]RegisteredTool{"tools-v1": tools})
	require.NoError(t, err)
	nonces := make([]byte, 12*64)
	for index := range 64 {
		for offset := range 12 {
			nonces[index*12+offset] = byte(index + 1)
		}
	}
	sealer, err := NewAESArgumentSealer("test-key", bytes.Repeat([]byte{5}, 32), bytes.NewReader(nonces))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	store := &memoryStore{leases: []Lease{lease}, onComplete: cancel}
	runner := testRunner(store, stream, allowPolicy(), clock.Real{}, fixedJitter(0), registry)
	runner.ArgumentSealer = sealer
	return ctx, cancel, store, runner
}

func TestRunnerToolReplayReusesSucceededResultAndStableIdempotency(t *testing.T) {
	var executions atomic.Int32
	tool := RegisteredTool{Definition: reasoner.ToolDefinition{Name: "lookup", Parameters: json.RawMessage(`{"type":"object"}`)}, ReadOnly: true, Executor: toolExecutorFunc(func(context.Context, ToolExecutionRequest) (json.RawMessage, error) {
		executions.Add(1)
		return json.RawMessage(`{"fresh":true}`), nil
	})}
	var calls atomic.Int32
	stream := streamerFunc(func(_ context.Context, request reasoner.Request, _ func(reasoner.Chunk) error) (reasoner.Result, error) {
		if calls.Add(1) == 1 {
			return reasoner.Result{FinishReason: "tool_calls", ToolCalls: []reasoner.ToolCall{{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"n":1.0}`)}}}, nil
		}
		assert.Equal(t, `{"cached":true}`, request.Messages[len(request.Messages)-1].Content)
		return reasoner.Result{Text: "done", FinishReason: "stop"}, nil
	})
	ctx, _, store, runner := toolRunnerFixture(t, []RegisteredTool{tool}, stream)
	var seenID string
	store.beginTool = func(record ToolInvocationRecord) (ToolInvocation, bool, error) {
		seenID = record.IdempotencyKey
		assert.Equal(t, record.InvocationID, record.IdempotencyKey)
		assert.NotContains(t, string(record.Ciphertext), `"n":1`)
		return canonicalInvocation(record, "succeeded", json.RawMessage(`{"cached":true}`), false, ApprovalNotRequired), false, nil
	}
	require.NoError(t, runner.Run(ctx))
	assert.Zero(t, executions.Load())
	assert.Len(t, seenID, 64)
	require.Len(t, store.completed, 1)
}

func TestRunnerRunningToolRecoveryUsesStableIdempotencyKey(t *testing.T) {
	var executions atomic.Int32
	var observed string
	tool := RegisteredTool{Definition: reasoner.ToolDefinition{Name: "charge", Parameters: json.RawMessage(`{"type":"object"}`)}, SideEffecting: true, StableIdempotency: true, Executor: toolExecutorFunc(func(_ context.Context, request ToolExecutionRequest) (json.RawMessage, error) {
		executions.Add(1)
		observed = request.IdempotencyKey
		return json.RawMessage(`{"ok":true}`), nil
	})}
	var rounds atomic.Int32
	stream := streamerFunc(func(context.Context, reasoner.Request, func(reasoner.Chunk) error) (reasoner.Result, error) {
		if rounds.Add(1) == 1 {
			return reasoner.Result{ToolCalls: []reasoner.ToolCall{{ID: "call", Name: "charge", Arguments: json.RawMessage(`{"amount":1}`)}}}, nil
		}
		return reasoner.Result{Text: "done"}, nil
	})
	ctx, _, store, runner := toolRunnerFixture(t, []RegisteredTool{tool}, stream)
	store.beginTool = func(record ToolInvocationRecord) (ToolInvocation, bool, error) {
		return canonicalInvocation(record, "running", nil, true, ApprovalApproved), false, nil
	}
	require.NoError(t, runner.Run(ctx))
	assert.Equal(t, int32(1), executions.Load())
	assert.Len(t, observed, 64)
	assert.Empty(t, store.failed)
}

func TestRunnerToolCompletionResponseLossRetriesWithoutDuplicateSideEffectOrBusinessFailure(t *testing.T) {
	var executions, completions atomic.Int32
	tool := RegisteredTool{Definition: reasoner.ToolDefinition{Name: "charge", Parameters: json.RawMessage(`{"type":"object"}`)}, SideEffecting: true, StableIdempotency: true, Executor: toolExecutorFunc(func(context.Context, ToolExecutionRequest) (json.RawMessage, error) {
		executions.Add(1)
		return json.RawMessage(`{"ok":true}`), nil
	})}
	var rounds atomic.Int32
	stream := streamerFunc(func(context.Context, reasoner.Request, func(reasoner.Chunk) error) (reasoner.Result, error) {
		if rounds.Add(1) == 1 {
			return reasoner.Result{ToolCalls: []reasoner.ToolCall{{ID: "call", Name: "charge", Arguments: json.RawMessage(`{}`)}}}, nil
		}
		return reasoner.Result{Text: "done"}, nil
	})
	ctx, _, store, runner := toolRunnerFixture(t, []RegisteredTool{tool}, stream)
	store.completeToolResult = func(ToolCompletionRecord) error {
		if completions.Add(1) == 1 {
			return errors.New("completion ACK lost")
		}
		return nil
	}
	require.NoError(t, runner.Run(ctx))
	assert.Equal(t, int32(1), executions.Load())
	assert.Equal(t, int32(2), completions.Load())
	assert.Empty(t, store.failed)
}

func TestRunnerProgressResponseLossReplaysSameScopedMutationWithoutFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	lease := workerLease()
	lease.Job.ObjectiveRevision, lease.Job.InputRevision = 3, 2
	store := &memoryStore{leases: []Lease{lease}, onComplete: cancel}
	var attempts atomic.Int32
	store.progressResult = func(ProgressRecord) error {
		if attempts.Add(1) == 1 {
			return errors.New("progress ACK lost")
		}
		return nil
	}
	registry, _ := NewToolRegistry(map[string][]RegisteredTool{"tools-v1": {}})
	stream := streamerFunc(func(_ context.Context, _ reasoner.Request, onChunk func(reasoner.Chunk) error) (reasoner.Result, error) {
		require.NoError(t, onChunk(reasoner.Chunk{Text: "d"}))
		return reasoner.Result{Text: "done"}, nil
	})
	runner := testRunner(store, stream, allowPolicy(), clock.NewManual(lease.Job.CreatedAt), fixedJitter(0), registry)
	require.NoError(t, runner.Run(ctx))
	require.Len(t, store.progress, 2)
	assert.Equal(t, store.progress[0].MutationID, store.progress[1].MutationID)
	assert.Equal(t, uint32(3), store.progress[0].ExpectedObjectiveRevision)
	assert.Empty(t, store.failed)
}

func TestRunnerUncertainProgressOutcomeAbandonsLeaseWithoutBusinessFailure(t *testing.T) {
	store := &memoryStore{leases: []Lease{workerLease()}}
	store.progressResult = func(ProgressRecord) error { return errors.New("database response unavailable") }
	registry, _ := NewToolRegistry(map[string][]RegisteredTool{"tools-v1": {}})
	stream := streamerFunc(func(_ context.Context, _ reasoner.Request, onChunk func(reasoner.Chunk) error) (reasoner.Result, error) {
		return reasoner.Result{}, onChunk(reasoner.Chunk{Text: "x"})
	})
	runner := testRunner(store, stream, allowPolicy(), clock.NewManual(workerLease().Job.CreatedAt), fixedJitter(0), registry)
	err := runner.Run(t.Context())
	assert.ErrorIs(t, err, ErrInfrastructureUncertain)
	assert.Empty(t, store.failed)
	assert.Empty(t, store.completed)
}

func TestRunnerApprovalMovesDurablyToNeedsInput(t *testing.T) {
	tool := RegisteredTool{Definition: reasoner.ToolDefinition{Name: "charge", Parameters: json.RawMessage(`{"type":"object"}`)}, SideEffecting: true, StableIdempotency: true, Executor: toolExecutorFunc(func(context.Context, ToolExecutionRequest) (json.RawMessage, error) {
		t.Fatal("approval-pending tool executed")
		return nil, nil
	})}
	stream := streamerFunc(func(context.Context, reasoner.Request, func(reasoner.Chunk) error) (reasoner.Result, error) {
		return reasoner.Result{FinishReason: "tool_calls", ToolCalls: []reasoner.ToolCall{{ID: "call", Name: "charge", Arguments: json.RawMessage(`{"amount":1}`)}}}, nil
	})
	ctx, cancel, store, runner := toolRunnerFixture(t, []RegisteredTool{tool}, stream)
	defer cancel()
	store.beginTool = func(record ToolInvocationRecord) (ToolInvocation, bool, error) {
		cancel()
		return canonicalInvocation(record, "pending", nil, true, ApprovalPending), true, ErrApprovalRequired
	}
	require.NoError(t, runner.Run(ctx))
	assert.Empty(t, store.needsInput, "the invocation store owns the atomic handoff transition")
	assert.Empty(t, store.completed)
	assert.Empty(t, store.failed)
}

func TestRunnerApprovalRequiredUsesSingleAtomicStoreTransition(t *testing.T) {
	lease := workerLease()
	lease.Job.Request.ToolAllowlist = []string{"charge"}
	tool := RegisteredTool{Definition: reasoner.ToolDefinition{Name: "charge", Parameters: json.RawMessage(`{"type":"object"}`)}, SideEffecting: true, StableIdempotency: true, Executor: toolExecutorFunc(func(context.Context, ToolExecutionRequest) (json.RawMessage, error) {
		t.Fatal("approval-pending tool executed")
		return nil, nil
	})}
	registry, _ := NewToolRegistry(map[string][]RegisteredTool{"tools-v1": {tool}})
	store := &memoryStore{}
	var attempts atomic.Int32
	store.beginTool = func(record ToolInvocationRecord) (ToolInvocation, bool, error) {
		attempts.Add(1)
		return canonicalInvocation(record, "pending", nil, true, ApprovalPending), true, ErrApprovalRequired
	}
	stream := streamerFunc(func(context.Context, reasoner.Request, func(reasoner.Chunk) error) (reasoner.Result, error) {
		return reasoner.Result{ToolCalls: []reasoner.ToolCall{{ID: "call", Name: "charge", Arguments: json.RawMessage(`{}`)}}}, nil
	})
	runner := testRunner(store, stream, allowPolicy(), clock.Real{}, fixedJitter(0), registry)
	runner.ArgumentSealer, _ = NewAESArgumentSealer("test-key", bytes.Repeat([]byte{5}, 32), bytes.NewReader(bytes.Repeat([]byte{1}, 12)))
	require.NoError(t, runner.executeLease(t.Context(), lease))
	assert.Equal(t, int32(1), attempts.Load())
	assert.Empty(t, store.needsInput, "a second handoff mutation would split the repository transaction")
	assert.Empty(t, store.failed)
	assert.Empty(t, store.completed)
}

func TestRunnerRetryFailAndCancelCommitResponseLossReplayExactMutation(t *testing.T) {
	t.Run("retry", func(t *testing.T) {
		lease := workerLease()
		store := &memoryStore{}
		var attempts atomic.Int32
		store.retryResult = func(RetryRecord) error {
			if attempts.Add(1) == 1 {
				return errors.New("retry ACK lost")
			}
			return nil
		}
		registry, _ := NewToolRegistry(map[string][]RegisteredTool{"tools-v1": {}})
		runner := testRunner(store, streamerFunc(func(context.Context, reasoner.Request, func(reasoner.Chunk) error) (reasoner.Result, error) {
			return reasoner.Result{}, &reasoner.ProviderError{Code: "busy", Retryable: true}
		}), allowPolicy(), clock.Real{}, fixedJitter(0), registry)
		require.NoError(t, runner.executeLease(t.Context(), lease))
		require.Len(t, store.retries, 2)
		assert.Equal(t, store.retries[0], store.retries[1])
		assert.Empty(t, store.failed)
	})
	t.Run("fail", func(t *testing.T) {
		lease := workerLease()
		store := &memoryStore{}
		var attempts atomic.Int32
		store.failResult = func(FailureRecord) error {
			if attempts.Add(1) == 1 {
				return errors.New("failure ACK lost")
			}
			return nil
		}
		registry, _ := NewToolRegistry(map[string][]RegisteredTool{"tools-v1": {}})
		runner := testRunner(store, streamerFunc(func(context.Context, reasoner.Request, func(reasoner.Chunk) error) (reasoner.Result, error) {
			return reasoner.Result{}, errors.New("generation failed")
		}), allowPolicy(), clock.Real{}, fixedJitter(0), registry)
		require.NoError(t, runner.executeLease(t.Context(), lease))
		require.Len(t, store.failed, 2)
		assert.Equal(t, store.failed[0], store.failed[1])
		assert.Empty(t, store.completed)
	})
	t.Run("cancel", func(t *testing.T) {
		lease := workerLease()
		control := Control{CancelRequested: true, ObjectiveRevision: 2, InputRevision: 3}
		store := &memoryStore{heartbeat: func(context.Context, LeaseToken, time.Time) (Control, error) { return control, nil }}
		var attempts atomic.Int32
		store.cancelResult = func(CancelledRecord) error {
			if attempts.Add(1) == 1 {
				return errors.New("cancel ACK lost")
			}
			return nil
		}
		runner := Runner{Store: store, Clock: clock.Real{}, LeaseDuration: 9 * time.Second}
		_, err := runner.recoverRevisionFence(t.Context(), lease, Control{})
		assert.ErrorIs(t, err, errFenceTerminal)
		require.Len(t, store.cancelled, 2)
		assert.Equal(t, store.cancelled[0], store.cancelled[1])
		assert.Equal(t, cancellationRecord(lease, control), store.cancelled[0])
	})
}

func TestRunnerUnresolvedRetryFailAndCancelACKAbandonsWithoutContradictoryMutation(t *testing.T) {
	t.Run("retry", func(t *testing.T) {
		lease := workerLease()
		store := &memoryStore{retryResult: func(RetryRecord) error { return errors.New("retry ACK unknown") }}
		registry, _ := NewToolRegistry(map[string][]RegisteredTool{"tools-v1": {}})
		runner := testRunner(store, streamerFunc(func(context.Context, reasoner.Request, func(reasoner.Chunk) error) (reasoner.Result, error) {
			return reasoner.Result{}, &reasoner.ProviderError{Code: "busy", Retryable: true}
		}), allowPolicy(), clock.Real{}, fixedJitter(0), registry)
		err := runner.executeLease(t.Context(), lease)
		assert.ErrorIs(t, err, ErrInfrastructureUncertain)
		require.Len(t, store.retries, 2)
		assert.Equal(t, store.retries[0], store.retries[1])
		assert.Empty(t, store.failed)
		assert.Empty(t, store.completed)
		assert.Empty(t, store.cancelled)
	})
	t.Run("fail", func(t *testing.T) {
		lease := workerLease()
		store := &memoryStore{failResult: func(FailureRecord) error { return errors.New("fail ACK unknown") }}
		registry, _ := NewToolRegistry(map[string][]RegisteredTool{"tools-v1": {}})
		runner := testRunner(store, streamerFunc(func(context.Context, reasoner.Request, func(reasoner.Chunk) error) (reasoner.Result, error) {
			return reasoner.Result{}, errors.New("generation failed")
		}), allowPolicy(), clock.Real{}, fixedJitter(0), registry)
		err := runner.executeLease(t.Context(), lease)
		assert.ErrorIs(t, err, ErrInfrastructureUncertain)
		require.Len(t, store.failed, 2)
		assert.Equal(t, store.failed[0], store.failed[1])
		assert.Empty(t, store.completed)
		assert.Empty(t, store.retries)
		assert.Empty(t, store.cancelled)
	})
	t.Run("cancel", func(t *testing.T) {
		lease := workerLease()
		control := Control{CancelRequested: true, ObjectiveRevision: 1, InputRevision: 1}
		store := &memoryStore{heartbeat: func(context.Context, LeaseToken, time.Time) (Control, error) { return control, nil }, cancelResult: func(CancelledRecord) error { return errors.New("cancel ACK unknown") }}
		runner := Runner{Store: store, Clock: clock.Real{}, LeaseDuration: 9 * time.Second}
		_, err := runner.recoverRevisionFence(t.Context(), lease, Control{})
		assert.ErrorIs(t, err, ErrInfrastructureUncertain)
		require.Len(t, store.cancelled, 2)
		assert.Equal(t, store.cancelled[0], store.cancelled[1])
		assert.Empty(t, store.completed)
		assert.Empty(t, store.failed)
		assert.Empty(t, store.retries)
	})
}

func TestExactMutationHelperPreservesSemanticErrorsAndClassifiesUnresolvedOutcome(t *testing.T) {
	for _, tc := range []struct {
		name     string
		err      error
		attempts int32
		want     error
	}{{"lease loss", ErrLeaseLost, 1, ErrLeaseLost}, {"domain", handoff.ErrInvalidTransition, 1, handoff.ErrInvalidTransition}, {"steering blocked", handoff.ErrSteeringBlocked, 1, handoff.ErrSteeringBlocked}, {"generic", errors.New("ACK unknown"), 2, ErrInfrastructureUncertain}} {
		t.Run(tc.name, func(t *testing.T) {
			var attempts atomic.Int32
			err := retryExactMutation(t.Context(), func() error { attempts.Add(1); return tc.err })
			assert.ErrorIs(t, err, tc.want)
			assert.Equal(t, tc.attempts, attempts.Load())
		})
	}
}

func TestRunnerSideEffectingToolsAreSequentialAndProviderOrdered(t *testing.T) {
	var mu sync.Mutex
	order := []string{}
	active, maxActive := 0, 0
	makeTool := func(name string) RegisteredTool {
		return RegisteredTool{Definition: reasoner.ToolDefinition{Name: name, Parameters: json.RawMessage(`{"type":"object"}`)}, SideEffecting: true, StableIdempotency: true, Executor: toolExecutorFunc(func(_ context.Context, request ToolExecutionRequest) (json.RawMessage, error) {
			mu.Lock()
			active++
			if active > maxActive {
				maxActive = active
			}
			order = append(order, name)
			active--
			mu.Unlock()
			return json.RawMessage(`{"tool":"` + name + `"}`), nil
		})}
	}
	tools := []RegisteredTool{makeTool("first"), makeTool("second")}
	var calls atomic.Int32
	stream := streamerFunc(func(_ context.Context, request reasoner.Request, _ func(reasoner.Chunk) error) (reasoner.Result, error) {
		if calls.Add(1) == 1 {
			return reasoner.Result{FinishReason: "tool_calls", ToolCalls: []reasoner.ToolCall{{ID: "a", Name: "first", Arguments: json.RawMessage(`{}`)}, {ID: "b", Name: "second", Arguments: json.RawMessage(`{}`)}}}, nil
		}
		assert.Equal(t, "a", request.Messages[len(request.Messages)-2].ToolCallID)
		assert.Equal(t, "b", request.Messages[len(request.Messages)-1].ToolCallID)
		return reasoner.Result{Text: "done", FinishReason: "stop"}, nil
	})
	ctx, _, _, runner := toolRunnerFixture(t, tools, stream)
	require.NoError(t, runner.Run(ctx))
	assert.Equal(t, []string{"first", "second"}, order)
	assert.Equal(t, 1, maxActive)
}

func TestRunnerParallelSafeReadOnlyGroupRunsConcurrentlyAndPreservesOrder(t *testing.T) {
	ready := make(chan string, 2)
	release := make(chan struct{})
	makeTool := func(name string) RegisteredTool {
		return RegisteredTool{Definition: reasoner.ToolDefinition{Name: name, Parameters: json.RawMessage(`{"type":"object"}`)}, ReadOnly: true, ParallelSafe: true, Executor: toolExecutorFunc(func(ctx context.Context, _ ToolExecutionRequest) (json.RawMessage, error) {
			ready <- name
			select {
			case <-release:
				return json.RawMessage(`{"tool":"` + name + `"}`), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		})}
	}
	tools := []RegisteredTool{makeTool("alpha"), makeTool("beta")}
	var calls atomic.Int32
	stream := streamerFunc(func(_ context.Context, request reasoner.Request, _ func(reasoner.Chunk) error) (reasoner.Result, error) {
		if calls.Add(1) == 1 {
			return reasoner.Result{FinishReason: "tool_calls", ToolCalls: []reasoner.ToolCall{{ID: "a", Name: "alpha", Arguments: json.RawMessage(`{}`)}, {ID: "b", Name: "beta", Arguments: json.RawMessage(`{}`)}}}, nil
		}
		assert.Contains(t, request.Messages[len(request.Messages)-2].Content, "alpha")
		assert.Contains(t, request.Messages[len(request.Messages)-1].Content, "beta")
		return reasoner.Result{Text: "done", FinishReason: "stop"}, nil
	})
	ctx, cancel, _, runner := toolRunnerFixture(t, tools, stream)
	guarded, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	go func() { <-ready; <-ready; close(release) }()
	err := runner.Run(guarded)
	cancel()
	require.NoError(t, err)
	assert.Equal(t, int32(2), calls.Load())
}

func TestRunnerParallelToolFailureCancelsPeer(t *testing.T) {
	bothStarted := make(chan struct{})
	peerCancelled := make(chan struct{})
	var starts atomic.Int32
	failing := RegisteredTool{Definition: reasoner.ToolDefinition{Name: "fail", Parameters: json.RawMessage(`{"type":"object"}`)}, ReadOnly: true, ParallelSafe: true, Executor: toolExecutorFunc(func(context.Context, ToolExecutionRequest) (json.RawMessage, error) {
		if starts.Add(1) == 2 {
			close(bothStarted)
		}
		<-bothStarted
		return nil, errors.New("boom")
	})}
	peer := RegisteredTool{Definition: reasoner.ToolDefinition{Name: "peer", Parameters: json.RawMessage(`{"type":"object"}`)}, ReadOnly: true, ParallelSafe: true, Executor: toolExecutorFunc(func(ctx context.Context, _ ToolExecutionRequest) (json.RawMessage, error) {
		if starts.Add(1) == 2 {
			close(bothStarted)
		}
		<-bothStarted
		<-ctx.Done()
		close(peerCancelled)
		return nil, ctx.Err()
	})}
	stream := streamerFunc(func(context.Context, reasoner.Request, func(reasoner.Chunk) error) (reasoner.Result, error) {
		return reasoner.Result{ToolCalls: []reasoner.ToolCall{{ID: "a", Name: "fail", Arguments: json.RawMessage(`{}`)}, {ID: "b", Name: "peer", Arguments: json.RawMessage(`{}`)}}}, nil
	})
	ctx, _, store, runner := toolRunnerFixture(t, []RegisteredTool{failing, peer}, stream)
	require.NoError(t, runner.Run(ctx))
	<-peerCancelled
	require.Len(t, store.failed, 1)
	assert.Empty(t, store.completed)
}

func TestRunnerMaximumToolRoundsAggregateCallsAndOutputFailClosed(t *testing.T) {
	tool := RegisteredTool{Definition: reasoner.ToolDefinition{Name: "lookup", Parameters: json.RawMessage(`{"type":"object"}`)}, ReadOnly: true, Executor: toolExecutorFunc(func(context.Context, ToolExecutionRequest) (json.RawMessage, error) {
		return json.RawMessage(`{"large":"` + strings.Repeat("x", 128) + `"}`), nil
	})}
	stream := streamerFunc(func(context.Context, reasoner.Request, func(reasoner.Chunk) error) (reasoner.Result, error) {
		return reasoner.Result{FinishReason: "tool_calls", ToolCalls: []reasoner.ToolCall{{ID: "call", Name: "lookup", Arguments: json.RawMessage(`{}`)}}}, nil
	})
	ctx, _, store, runner := toolRunnerFixture(t, []RegisteredTool{tool}, stream)
	runner.MaxToolRounds = 1
	runner.MaxToolOutputBytes = 64
	require.NoError(t, runner.Run(ctx))
	require.Len(t, store.failed, 1)
	assert.Empty(t, store.completed)
}

func TestRunnerMaximumToolRoundsAndAggregateCallCountAreIndependentBounds(t *testing.T) {
	for _, tc := range []struct {
		name                string
		maxRounds, maxCalls int
	}{{"rounds", 1, 8}, {"aggregate calls", 8, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			tool := RegisteredTool{Definition: reasoner.ToolDefinition{Name: "lookup", Parameters: json.RawMessage(`{"type":"object"}`)}, ReadOnly: true, Executor: toolExecutorFunc(func(context.Context, ToolExecutionRequest) (json.RawMessage, error) {
				return json.RawMessage(`{"ok":true}`), nil
			})}
			var call atomic.Int32
			stream := streamerFunc(func(context.Context, reasoner.Request, func(reasoner.Chunk) error) (reasoner.Result, error) {
				id := fmt.Sprintf("call-%d", call.Add(1))
				return reasoner.Result{FinishReason: "tool_calls", ToolCalls: []reasoner.ToolCall{{ID: id, Name: "lookup", Arguments: json.RawMessage(`{}`)}}}, nil
			})
			ctx, _, store, runner := toolRunnerFixture(t, []RegisteredTool{tool}, stream)
			runner.MaxToolRounds = tc.maxRounds
			runner.MaxToolCalls = tc.maxCalls
			require.NoError(t, runner.Run(ctx))
			assert.Empty(t, store.completed)
			require.Len(t, store.failed, 1)
		})
	}
}

func TestRunnerDurableCancelMarksCancelledUnderCurrentLease(t *testing.T) {
	manual := clock.NewManual(time.Now().UTC())
	lease := workerLease()
	lease.Job.CreatedAt = manual.Now()
	lease.Job.Request.Deadline = manual.Now().Add(time.Hour)
	started := make(chan struct{})
	store := &memoryStore{leases: []Lease{lease}, heartbeat: func(context.Context, LeaseToken, time.Time) (Control, error) {
		return Control{CancelRequested: true}, nil
	}}
	ctx, cancel := context.WithCancel(t.Context())
	store.onComplete = cancel
	registry, _ := NewToolRegistry(map[string][]RegisteredTool{"tools-v1": {}})
	stream := streamerFunc(func(ctx context.Context, _ reasoner.Request, _ func(reasoner.Chunk) error) (reasoner.Result, error) {
		close(started)
		<-ctx.Done()
		return reasoner.Result{}, ctx.Err()
	})
	runner := testRunner(store, stream, allowPolicy(), manual, fixedJitter(0), registry)
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	<-started
	for {
		manual.Advance(3 * time.Second)
		runtime.Gosched()
		store.mu.Lock()
		count := len(store.cancelled)
		store.mu.Unlock()
		if count == 1 {
			break
		}
	}
	require.NoError(t, <-done)
	require.Len(t, store.cancelled, 1)
	assert.Empty(t, store.completed)
	assert.Empty(t, store.failed)
}

func TestRunnerHeartbeatCombinedSteerAndCancelUsesCurrentControlWithoutRestart(t *testing.T) {
	manual := clock.NewManual(time.Now().UTC())
	lease := workerLease()
	lease.Job.CreatedAt = manual.Now()
	lease.Job.Request.Deadline = manual.Now().Add(time.Hour)
	current := Control{CancelRequested: true, ObjectiveRevision: 2, Objective: "new objective", InputRevision: 3, ConsumedInput: 3, Input: json.RawMessage(`{"answer":true}`)}
	started := make(chan struct{})
	store := &memoryStore{leases: []Lease{lease}, heartbeat: func(context.Context, LeaseToken, time.Time) (Control, error) { return cloneControl(current), nil }}
	ctx, cancel := context.WithCancel(t.Context())
	store.onComplete = cancel
	registry, _ := NewToolRegistry(map[string][]RegisteredTool{"tools-v1": {}})
	var generations atomic.Int32
	stream := streamerFunc(func(ctx context.Context, _ reasoner.Request, _ func(reasoner.Chunk) error) (reasoner.Result, error) {
		generations.Add(1)
		close(started)
		<-ctx.Done()
		return reasoner.Result{}, ctx.Err()
	})
	runner := testRunner(store, stream, allowPolicy(), manual, fixedJitter(0), registry)
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	<-started
	for {
		store.mu.Lock()
		cancelled := len(store.cancelled)
		store.mu.Unlock()
		if cancelled > 0 {
			break
		}
		manual.Advance(3 * time.Second)
		runtime.Gosched()
	}
	require.NoError(t, <-done)
	assert.Equal(t, int32(1), generations.Load())
	require.Len(t, store.cancelled, 1)
	assert.Equal(t, cancellationRecord(lease, current), store.cancelled[0])
	assert.Empty(t, store.completed)
	assert.Empty(t, store.failed)
}

func TestRunnerExactDeadlineCancelsBeforeHeartbeatAndCannotComplete(t *testing.T) {
	manual := clock.NewManual(time.Now().UTC())
	lease := workerLease()
	lease.Job.CreatedAt = manual.Now()
	lease.Job.Request.Deadline = manual.Now().Add(time.Second)
	started := make(chan struct{})
	store := &memoryStore{leases: []Lease{lease}}
	ctx, cancel := context.WithCancel(t.Context())
	store.onComplete = cancel
	registry, _ := NewToolRegistry(map[string][]RegisteredTool{"tools-v1": {}})
	stream := streamerFunc(func(ctx context.Context, _ reasoner.Request, onChunk func(reasoner.Chunk) error) (reasoner.Result, error) {
		close(started)
		<-ctx.Done()
		_ = onChunk(reasoner.Chunk{Text: "late secret"})
		return reasoner.Result{Text: "late secret", FinishReason: "stop"}, nil
	})
	runner := testRunner(store, stream, allowPolicy(), manual, fixedJitter(0), registry)
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	<-started
	for {
		manual.Advance(time.Second)
		runtime.Gosched()
		store.mu.Lock()
		failures := len(store.failed)
		store.mu.Unlock()
		if failures == 1 {
			break
		}
	}
	require.NoError(t, <-done)
	assert.Empty(t, store.completed)
	require.Len(t, store.failed, 1)
	assert.Equal(t, "deadline_expired", store.failed[0].Code)
	assert.Empty(t, store.progress)
}

func TestRunnerHeartbeatCadenceExpiryAndCleanup(t *testing.T) {
	manual := clock.NewManual(time.Now().UTC())
	lease := workerLease()
	lease.Job.CreatedAt = manual.Now()
	lease.Job.Request.Deadline = manual.Now().Add(time.Hour)
	started := make(chan struct{})
	var beats atomic.Int32
	var expiryMu sync.Mutex
	var expiries []time.Time
	store := &memoryStore{leases: []Lease{lease}}
	store.heartbeat = func(_ context.Context, _ LeaseToken, expiry time.Time) (Control, error) {
		count := beats.Load() + 1
		expiryMu.Lock()
		expiries = append(expiries, expiry)
		expiryMu.Unlock()
		beats.Store(count)
		return Control{CancelRequested: count == 3}, nil
	}
	ctx, cancel := context.WithCancel(t.Context())
	store.onComplete = cancel
	registry, _ := NewToolRegistry(map[string][]RegisteredTool{"tools-v1": {}})
	stream := streamerFunc(func(ctx context.Context, _ reasoner.Request, _ func(reasoner.Chunk) error) (reasoner.Result, error) {
		close(started)
		<-ctx.Done()
		return reasoner.Result{}, ctx.Err()
	})
	runner := testRunner(store, stream, allowPolicy(), manual, fixedJitter(0), registry)
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	<-started
	for beats.Load() < 1 {
		manual.Advance(3 * time.Second)
		runtime.Gosched()
	}
	for target := int32(2); target <= 3; target++ {
		manual.Advance(3 * time.Second)
		for beats.Load() < target {
			runtime.Gosched()
		}
	}
	require.NoError(t, <-done)
	expiryMu.Lock()
	require.Len(t, expiries, 3)
	expiryMu.Unlock()
	before := beats.Load()
	manual.Advance(time.Minute)
	runtime.Gosched()
	assert.Equal(t, before, beats.Load())
}

func TestRunnerDurableCancelPrecedesQueuedSteering(t *testing.T) {
	manual := clock.NewManual(time.Now().UTC())
	lease := workerLease()
	lease.Job.CreatedAt = manual.Now()
	lease.Job.Request.Deadline = manual.Now().Add(time.Hour)
	started, release := make(chan struct{}), make(chan struct{})
	var beats atomic.Int32
	store := &memoryStore{leases: []Lease{lease}}
	store.heartbeat = func(context.Context, LeaseToken, time.Time) (Control, error) {
		if beats.Add(1) == 1 {
			return Control{ObjectiveRevision: 1, Objective: "steered"}, nil
		}
		return Control{ObjectiveRevision: 1, Objective: "steered", CancelRequested: true}, nil
	}
	ctx, cancel := context.WithCancel(t.Context())
	store.onComplete = cancel
	registry, _ := NewToolRegistry(map[string][]RegisteredTool{"tools-v1": {}})
	stream := streamerFunc(func(context.Context, reasoner.Request, func(reasoner.Chunk) error) (reasoner.Result, error) {
		close(started)
		<-release
		return reasoner.Result{Text: "stale", FinishReason: "stop"}, nil
	})
	runner := testRunner(store, stream, allowPolicy(), manual, fixedJitter(0), registry)
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	<-started
	for beats.Load() < 2 {
		manual.Advance(3 * time.Second)
		runtime.Gosched()
	}
	close(release)
	require.NoError(t, <-done)
	require.Len(t, store.cancelled, 1)
	assert.Empty(t, store.completed)
}

func TestHeartbeatLeaseLossOutranksDeadlineAndSteering(t *testing.T) {
	events := make(chan heartbeatEvent, 3)
	events <- heartbeatEvent{cause: errSteered, control: Control{ObjectiveRevision: 1}}
	events <- heartbeatEvent{cause: ErrLeaseLost}
	events <- heartbeatEvent{cause: errDeadline}
	chosen := consumeHeartbeatEvent(events, errSteered)
	assert.ErrorIs(t, chosen.cause, ErrLeaseLost)
}

func TestRunnerRetryAfterWinsAndExhaustionFails(t *testing.T) {
	for _, exhausted := range []bool{false, true} {
		t.Run(fmt.Sprintf("exhausted=%t", exhausted), func(t *testing.T) {
			lease := workerLease()
			if exhausted {
				lease.Job.RetryCount = 3
			}
			ctx, cancel := context.WithCancel(t.Context())
			store := &memoryStore{leases: []Lease{lease}, onComplete: cancel}
			registry, _ := NewToolRegistry(map[string][]RegisteredTool{"tools-v1": {}})
			stream := streamerFunc(func(context.Context, reasoner.Request, func(reasoner.Chunk) error) (reasoner.Result, error) {
				return reasoner.Result{}, &reasoner.ProviderError{Code: "busy", Retryable: true, RetryAfter: 7 * time.Second}
			})
			runner := testRunner(store, stream, allowPolicy(), clock.Real{}, fixedJitter(time.Second), registry)
			if !exhausted {
				store.onComplete = nil
				done := make(chan error, 1)
				start := time.Now()
				go func() { done <- runner.Run(ctx) }()
				for {
					store.mu.Lock()
					n := len(store.retries)
					store.mu.Unlock()
					if n == 1 {
						break
					}
					runtime.Gosched()
				}
				cancel()
				require.NoError(t, <-done)
				assert.GreaterOrEqual(t, store.retries[0].ReadyAt.Sub(start), 7*time.Second)
			} else {
				require.NoError(t, runner.Run(ctx))
				assert.Empty(t, store.retries)
				require.Len(t, store.failed, 1)
			}
		})
	}
}

func TestRunnerCompletionResponseLossReplaysExactMutation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	store := &memoryStore{leases: []Lease{workerLease()}}
	var attempts atomic.Int32
	store.completeResult = func(handoff.Result) error {
		if attempts.Add(1) == 1 {
			return errors.New("response lost")
		}
		cancel()
		return nil
	}
	registry, _ := NewToolRegistry(map[string][]RegisteredTool{"tools-v1": {}})
	stream := streamerFunc(func(context.Context, reasoner.Request, func(reasoner.Chunk) error) (reasoner.Result, error) {
		return reasoner.Result{Text: "done", FinishReason: "stop"}, nil
	})
	runner := testRunner(store, stream, allowPolicy(), clock.Real{}, fixedJitter(0), registry)
	require.NoError(t, runner.Run(ctx))
	require.Len(t, store.completed, 2)
	assert.Equal(t, store.completed[0], store.completed[1])
}

func TestRunnerTerminalFenceCancellationExitsWithoutStartingAnotherGeneration(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	store := &memoryStore{leases: []Lease{workerLease()}, heartbeat: func(context.Context, LeaseToken, time.Time) (Control, error) {
		return Control{CancelRequested: true}, nil
	}}
	store.onComplete = cancel
	store.completeResult = func(handoff.Result) error { return ErrLeaseLost }
	registry, _ := NewToolRegistry(map[string][]RegisteredTool{"tools-v1": {}})
	var generations atomic.Int32
	stream := streamerFunc(func(context.Context, reasoner.Request, func(reasoner.Chunk) error) (reasoner.Result, error) {
		generations.Add(1)
		return reasoner.Result{Text: "done"}, nil
	})
	runner := testRunner(store, stream, allowPolicy(), clock.Real{}, fixedJitter(0), registry)
	require.NoError(t, runner.Run(ctx))
	assert.Equal(t, int32(1), generations.Load())
	require.Len(t, store.cancelled, 1)
}

func TestRunnerFinalTextWithoutCallbacksStillHonorsAggregateLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	store := &memoryStore{leases: []Lease{workerLease()}, onComplete: cancel}
	registry, _ := NewToolRegistry(map[string][]RegisteredTool{"tools-v1": {}})
	stream := streamerFunc(func(context.Context, reasoner.Request, func(reasoner.Chunk) error) (reasoner.Result, error) {
		return reasoner.Result{Text: strings.Repeat("x", 128), FinishReason: "stop"}, nil
	})
	runner := testRunner(store, stream, allowPolicy(), clock.Real{}, fixedJitter(0), registry)
	runner.MaxOutputBytes = 64
	runner.MaxToolArgumentBytes = 64
	runner.MaxToolOutputBytes = 64
	require.NoError(t, runner.Run(ctx))
	assert.Empty(t, store.completed)
	require.Len(t, store.failed, 1)
	assert.Equal(t, "output_limit", store.failed[0].Code)
}

func TestRunnerPostPolicyRedactionPayloadHonorsExactAggregateBoundary(t *testing.T) {
	text, safe := "x", "safe"
	structured, _ := json.Marshal(struct {
		Text string `json:"text"`
	}{text})
	usage, _ := json.Marshal(reasoner.Usage{})
	decision := safety.Decision{Action: safety.ActionRedact, SafeSummary: safe, ReasonCode: "redacted"}
	decisionJSON, _ := json.Marshal(decision)
	exact := len(text) + len(text) + len(structured) + len(usage) + len(safe) + len(`{}`) + len(decisionJSON)
	for _, tc := range []struct {
		name      string
		limit     int
		completes bool
	}{{"exact", exact, true}, {"first byte over", exact - 1, false}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			store := &memoryStore{leases: []Lease{workerLease()}, onComplete: cancel}
			registry, _ := NewToolRegistry(map[string][]RegisteredTool{"tools-v1": {}})
			policy := safetyPolicyFunc(func(context.Context, safety.WorkerResultCandidate) (safety.Decision, error) { return decision, nil })
			runner := testRunner(store, streamerFunc(func(context.Context, reasoner.Request, func(reasoner.Chunk) error) (reasoner.Result, error) {
				return reasoner.Result{Text: text}, nil
			}), policy, clock.Real{}, fixedJitter(0), registry)
			runner.MaxOutputBytes = tc.limit
			runner.MaxToolArgumentBytes = tc.limit
			runner.MaxToolOutputBytes = tc.limit
			require.NoError(t, runner.Run(ctx))
			if tc.completes {
				require.Len(t, store.completed, 1)
				assert.Equal(t, safe, store.completed[0].Summary)
				assert.Empty(t, store.failed)
			} else {
				assert.Empty(t, store.completed)
				require.Len(t, store.failed, 1)
				assert.Equal(t, "output_limit", store.failed[0].Code)
			}
		})
	}
}

func TestRunnerAggregateByteBudgetAndFirstTokenRemainLiveAcrossMultipleSteers(t *testing.T) {
	finalText := "c"
	structured, _ := json.Marshal(struct {
		Text string `json:"text"`
	}{finalText})
	usage, _ := json.Marshal(reasoner.Usage{})
	decisionJSON, _ := json.Marshal(safety.Decision{Action: safety.ActionAllow, ReasonCode: "allowed"})
	exact := len("a") + len("generation in progress") + len("b") + len(`{}`) + len(finalText) + len(finalText) + len(structured) + len(usage) + len(decisionJSON)
	for _, tc := range []struct {
		name      string
		limit     int
		completes bool
	}{{"exact after two steers", exact, true}, {"first byte over after two steers", exact - 1, false}} {
		t.Run(tc.name, func(t *testing.T) {
			lease := workerLease()
			lease.Job.Request.ToolAllowlist = []string{"lookup"}
			tool := RegisteredTool{Definition: reasoner.ToolDefinition{Name: "lookup", Parameters: json.RawMessage(`{"type":"object"}`)}, ReadOnly: true, Executor: toolExecutorFunc(func(context.Context, ToolExecutionRequest) (json.RawMessage, error) {
				t.Fatal("tool executed before begin fence")
				return nil, nil
			})}
			registry, _ := NewToolRegistry(map[string][]RegisteredTool{"tools-v1": {tool}})
			var heartbeats, generations, firstTokens atomic.Int32
			store := &memoryStore{heartbeat: func(context.Context, LeaseToken, time.Time) (Control, error) {
				revision := uint32(heartbeats.Add(1))
				return Control{ObjectiveRevision: revision, Objective: fmt.Sprintf("objective-%d", revision)}, nil
			}}
			store.progressResult = func(ProgressRecord) error { return ErrLeaseLost }
			store.beginTool = func(ToolInvocationRecord) (ToolInvocation, bool, error) { return ToolInvocation{}, false, ErrLeaseLost }
			stream := streamerFunc(func(_ context.Context, request reasoner.Request, onChunk func(reasoner.Chunk) error) (reasoner.Result, error) {
				switch generations.Add(1) {
				case 1:
					return reasoner.Result{}, onChunk(reasoner.Chunk{Text: "a"})
				case 2:
					assert.Contains(t, request.Messages[1].Content, "objective-1")
					require.NoError(t, onChunk(reasoner.Chunk{Text: "b"}))
					return reasoner.Result{ToolCalls: []reasoner.ToolCall{{ID: "call", Name: "lookup", Arguments: json.RawMessage(`{}`)}}}, nil
				default:
					assert.Contains(t, request.Messages[1].Content, "objective-2")
					return reasoner.Result{Text: finalText}, nil
				}
			})
			runner := testRunner(store, stream, allowPolicy(), clock.NewManual(lease.Job.CreatedAt), fixedJitter(0), registry)
			runner.MaxOutputBytes = tc.limit
			runner.MaxToolArgumentBytes = tc.limit
			runner.MaxToolOutputBytes = tc.limit
			runner.FirstTokenObserver = func(LeaseToken, time.Time) { firstTokens.Add(1) }
			runner.ArgumentSealer, _ = NewAESArgumentSealer("test-key", bytes.Repeat([]byte{5}, 32), bytes.NewReader(bytes.Repeat([]byte{1}, 12)))
			err := runner.executeLease(t.Context(), lease)
			require.NoError(t, err)
			assert.Equal(t, int32(3), generations.Load())
			assert.Equal(t, int32(1), firstTokens.Load())
			if tc.completes {
				require.Len(t, store.completed, 1)
				assert.Empty(t, store.failed)
			} else {
				assert.Empty(t, store.completed)
				require.Len(t, store.failed, 1)
				assert.Equal(t, "output_limit", store.failed[0].Code)
			}
		})
	}
}

func TestRunnerSteeringCannotResetTotalToolCallOrRoundCapsOrRepeatSideEffect(t *testing.T) {
	for _, tc := range []struct {
		name                string
		maxCalls, maxRounds int
	}{{"total calls", 1, 8}, {"total rounds", 8, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			lease := workerLease()
			lease.Job.Request.ToolAllowlist = []string{"charge"}
			var generations, executions, completions atomic.Int32
			tool := RegisteredTool{Definition: reasoner.ToolDefinition{Name: "charge", Parameters: json.RawMessage(`{"type":"object"}`)}, SideEffecting: true, StableIdempotency: true, Executor: toolExecutorFunc(func(context.Context, ToolExecutionRequest) (json.RawMessage, error) {
				executions.Add(1)
				return json.RawMessage(`{"ok":true}`), nil
			})}
			registry, _ := NewToolRegistry(map[string][]RegisteredTool{"tools-v1": {tool}})
			store := &memoryStore{heartbeat: func(context.Context, LeaseToken, time.Time) (Control, error) {
				return Control{ObjectiveRevision: 1, Objective: "revised"}, nil
			}}
			store.completeToolResult = func(ToolCompletionRecord) error {
				if completions.Add(1) == 1 {
					return ErrLeaseLost
				}
				return nil
			}
			stream := streamerFunc(func(_ context.Context, request reasoner.Request, _ func(reasoner.Chunk) error) (reasoner.Result, error) {
				generations.Add(1)
				if generations.Load() == 2 {
					assert.Contains(t, request.Messages[1].Content, "revised")
				}
				return reasoner.Result{ToolCalls: []reasoner.ToolCall{{ID: fmt.Sprintf("call-%d", generations.Load()), Name: "charge", Arguments: json.RawMessage(`{}`)}}}, nil
			})
			runner := testRunner(store, stream, allowPolicy(), clock.Real{}, fixedJitter(0), registry)
			runner.MaxToolCalls = tc.maxCalls
			runner.MaxToolRounds = tc.maxRounds
			runner.ArgumentSealer, _ = NewAESArgumentSealer("test-key", bytes.Repeat([]byte{5}, 32), bytes.NewReader(bytes.Repeat([]byte{1}, 24)))
			require.NoError(t, runner.executeLease(t.Context(), lease))
			assert.Equal(t, int32(2), generations.Load())
			assert.Equal(t, int32(1), executions.Load())
			require.Len(t, store.failed, 1)
			assert.Empty(t, store.completed)
		})
	}
}

var _ = fmt.Sprintf
var _ identity.HandoffID

func TestDenyAllSafetyPolicyIsDeterministicAndFailClosed(t *testing.T) {
	candidate := safety.WorkerResultCandidate{SessionID: "session", ConversationID: "conversation", HandoffID: "handoff", Channel: safety.ChannelWorker, Summary: "secret", Structured: json.RawMessage(`{"secret":true}`)}
	decision, err := (safety.DenyAll{}).EvaluateWorkerResult(t.Context(), candidate)
	require.NoError(t, err)
	assert.Equal(t, safety.ActionSuppress, decision.Action)
	assert.Equal(t, "deny_all", decision.ReasonCode)
	assert.Empty(t, decision.SafeSummary)
	cloned := candidate.Clone()
	cloned.Structured[0] = '['
	assert.Equal(t, byte('{'), candidate.Structured[0])
}

type toolExecutorFunc func(context.Context, ToolExecutionRequest) (json.RawMessage, error)

func (f toolExecutorFunc) Execute(ctx context.Context, request ToolExecutionRequest) (json.RawMessage, error) {
	return f(ctx, request)
}

func TestToolRegistryIsVersionExactImmutableAndRejectsUnsafeSideEffects(t *testing.T) {
	definition := reasoner.ToolDefinition{Name: "search", Description: "read only", Parameters: json.RawMessage(`{"type":"object"}`)}
	registry, err := NewToolRegistry(map[string][]RegisteredTool{
		"v1": {{Definition: definition, ReadOnly: true, ParallelSafe: true, Executor: toolExecutorFunc(func(context.Context, ToolExecutionRequest) (json.RawMessage, error) {
			return json.RawMessage(`{}`), nil
		})}, {Definition: reasoner.ToolDefinition{Name: "charge", Parameters: json.RawMessage(`{"type":"object"}`)}, SideEffecting: true, StableIdempotency: true, Executor: toolExecutorFunc(func(context.Context, ToolExecutionRequest) (json.RawMessage, error) {
			return json.RawMessage(`{}`), nil
		})}},
	})
	require.NoError(t, err)
	definition.Parameters[2] = 'x'
	assert.True(t, registry.Registered(t.Context(), "v1", "search"))
	assert.False(t, registry.Registered(t.Context(), "v2", "search"))
	metadata, ok := registry.Metadata(t.Context(), "v1", "search")
	require.True(t, ok)
	assert.False(t, metadata.SideEffecting)
	metadata, ok = registry.Metadata(t.Context(), "v1", "charge")
	require.True(t, ok)
	assert.True(t, metadata.SideEffecting)
	_, ok = registry.Metadata(t.Context(), "v2", "charge")
	assert.False(t, ok)
	resolved, ok := registry.Lookup("v1", "search")
	require.True(t, ok)
	assert.JSONEq(t, `{"type":"object"}`, string(resolved.Definition.Parameters))
	resolved.Definition.Parameters[2] = 'x'
	again, ok := registry.Lookup("v1", "search")
	require.True(t, ok)
	assert.JSONEq(t, `{"type":"object"}`, string(again.Definition.Parameters))

	_, err = NewToolRegistry(map[string][]RegisteredTool{"v1": {{
		Definition:    reasoner.ToolDefinition{Name: "charge", Parameters: json.RawMessage(`{"type":"object"}`)},
		SideEffecting: true,
		Executor:      toolExecutorFunc(func(context.Context, ToolExecutionRequest) (json.RawMessage, error) { return nil, nil }),
	}}})
	assert.Error(t, err)
}

func TestCanonicalToolArgumentsNormalizesObjectOrderAndNumbers(t *testing.T) {
	a, err := canonicalToolArguments(json.RawMessage(`{"b":1.0,"a":[1e-2,2]}`), 1024)
	require.NoError(t, err)
	b, err := canonicalToolArguments(json.RawMessage(`{ "a" : [0.0100,2.0], "b": 1 }`), 1024)
	require.NoError(t, err)
	assert.Equal(t, a, b)
	assert.Equal(t, `{"a":[0.01,2],"b":1}`, string(a))
	_, err = canonicalToolArguments(json.RawMessage(`{"x":1,"x":2}`), 1024)
	assert.Error(t, err)
	_, err = canonicalToolArguments(json.RawMessage(`[]`), 1024)
	assert.Error(t, err)
	_, err = canonicalToolArguments(json.RawMessage(`{"long":"value"}`), 4)
	assert.Error(t, err)
}

type repeatingReader struct{ block []byte }

func (r repeatingReader) Read(target []byte) (int, error) { return copy(target, r.block), nil }

func TestAESArgumentSealerRoundTripCopiesKeyAndRejectsNonceReuse(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	nonce := bytes.Repeat([]byte{3}, 12)
	sealer, err := NewAESArgumentSealer("tool-key-v1", key, repeatingReader{block: nonce})
	require.NoError(t, err)
	key[0] = 9
	plaintext := []byte(`{"secret":"sentinel-tool-argument"}`)
	sealed, err := sealer.Seal(plaintext)
	require.NoError(t, err)
	assert.Equal(t, "tool-key-v1", sealed.KeyID)
	assert.Equal(t, nonce, sealed.Nonce)
	assert.NotContains(t, string(sealed.Ciphertext), "sentinel-tool-argument")

	block, err := aes.NewCipher(bytes.Repeat([]byte{7}, 32))
	require.NoError(t, err)
	gcm, err := cipher.NewGCM(block)
	require.NoError(t, err)
	decoded, err := gcm.Open(nil, sealed.Nonce, sealed.Ciphertext, nil)
	require.NoError(t, err)
	assert.Equal(t, plaintext, decoded)
	opened, err := sealer.Open(sealed)
	require.NoError(t, err)
	assert.Equal(t, plaintext, opened)
	tampered := sealed
	tampered.Ciphertext = append([]byte(nil), sealed.Ciphertext...)
	tampered.Ciphertext[0] ^= 1
	_, err = sealer.Open(tampered)
	assert.Error(t, err)
	wrongKey := sealed
	wrongKey.KeyID = "other-key"
	_, err = sealer.Open(wrongKey)
	assert.Error(t, err)
	_, err = sealer.Seal([]byte(`{"next":true}`))
	assert.ErrorIs(t, err, ErrNonceReuse)
}

func TestAESArgumentSealerCloseZeroizesAndRejectsFurtherUse(t *testing.T) {
	sealer, err := NewAESArgumentSealer("rotation-v2", bytes.Repeat([]byte{4}, 32), bytes.NewReader(bytes.Repeat([]byte{1}, 24)))
	require.NoError(t, err)
	concrete := sealer.(*aesArgumentSealer)
	require.NoError(t, sealer.Close())
	assert.Nil(t, concrete.key)
	assert.Nil(t, concrete.used)
	_, err = sealer.Seal([]byte(`{}`))
	assert.ErrorContains(t, err, "closed")
	_, err = sealer.Open(SealedArguments{KeyID: "rotation-v2", Nonce: make([]byte, 12), Ciphertext: make([]byte, 16)})
	assert.ErrorContains(t, err, "unavailable")
	require.NoError(t, sealer.Close())
}

func TestAESArgumentSealerRejectsInvalidInputsAndNonceFailure(t *testing.T) {
	_, err := NewAESArgumentSealer("key", []byte("short"), bytes.NewReader(nil))
	assert.Error(t, err)
	sealer, err := NewAESArgumentSealer("key", bytes.Repeat([]byte{1}, 32), bytes.NewReader([]byte{1, 2}))
	require.NoError(t, err)
	_, err = sealer.Seal([]byte(`{}`))
	assert.True(t, errors.Is(err, ErrNonceSource), err)
}
