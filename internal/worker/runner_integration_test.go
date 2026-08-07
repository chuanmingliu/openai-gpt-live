//go:build integration

package worker_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/reasoner"
	"github.com/chuanmingliu/openai-gpt-live/internal/safety"
	"github.com/chuanmingliu/openai-gpt-live/internal/store/postgres"
	"github.com/chuanmingliu/openai-gpt-live/internal/testkit"
	"github.com/chuanmingliu/openai-gpt-live/internal/worker"
	"github.com/chuanmingliu/openai-gpt-live/migrations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type integrationStreamer func(context.Context, reasoner.Request, func(reasoner.Chunk) error) (reasoner.Result, error)

func (f integrationStreamer) Stream(ctx context.Context, request reasoner.Request, chunk func(reasoner.Chunk) error) (reasoner.Result, error) {
	return f(ctx, request, chunk)
}

type integrationPolicy struct{}

func (integrationPolicy) EvaluateWorkerResult(context.Context, safety.WorkerResultCandidate) (safety.Decision, error) {
	return safety.Decision{Action: safety.ActionAllow, ReasonCode: "integration_allow"}, nil
}

type zeroJitter struct{}

func (zeroJitter) Duration(time.Duration) time.Duration { return 0 }

type acquiredLeaseStore struct {
	worker.Store
	lease     worker.Lease
	delivered atomic.Bool
}

type terminalBoundaryStore struct {
	worker.Store
	before func(context.Context) error
	fired  atomic.Bool
}

type progressBoundaryStore struct {
	worker.Store
	before func(context.Context) error
	fired  atomic.Bool
}

func (s *progressBoundaryStore) AppendProgress(ctx context.Context, token worker.LeaseToken, record worker.ProgressRecord) error {
	if s.fired.CompareAndSwap(false, true) {
		if err := s.before(ctx); err != nil {
			return err
		}
	}
	return s.Store.AppendProgress(ctx, token, record)
}

type toolCompletionBoundaryStore struct {
	worker.Store
	before func(context.Context) error
	fired  atomic.Bool
}

type sideEffectSteerBoundaryStore struct {
	worker.Store
	before func(context.Context) error
	result chan error
	fired  atomic.Bool
}

type postCancelHeartbeatStore struct {
	worker.Store
	after  func(context.Context) error
	result chan error
	fired  atomic.Bool
}

func (s *toolCompletionBoundaryStore) CompleteToolInvocation(ctx context.Context, token worker.LeaseToken, record worker.ToolCompletionRecord) error {
	if s.fired.CompareAndSwap(false, true) {
		if err := s.before(ctx); err != nil {
			return err
		}
	}
	return s.Store.CompleteToolInvocation(ctx, token, record)
}

func (s *sideEffectSteerBoundaryStore) CompleteToolInvocation(ctx context.Context, token worker.LeaseToken, record worker.ToolCompletionRecord) error {
	if s.fired.CompareAndSwap(false, true) {
		s.result <- s.before(ctx)
	}
	return s.Store.CompleteToolInvocation(ctx, token, record)
}

func (s *postCancelHeartbeatStore) Heartbeat(ctx context.Context, token worker.LeaseToken, expiry time.Time) (worker.Control, error) {
	control, err := s.Store.Heartbeat(ctx, token, expiry)
	if err == nil && control.CancelRequested && s.fired.CompareAndSwap(false, true) {
		s.result <- s.after(ctx)
	}
	return control, err
}

func (s *terminalBoundaryStore) Complete(ctx context.Context, token worker.LeaseToken, record worker.CompletionRecord) error {
	if s.fired.CompareAndSwap(false, true) {
		if err := s.before(ctx); err != nil {
			return err
		}
	}
	return s.Store.Complete(ctx, token, record)
}

func (s *acquiredLeaseStore) LeaseNext(ctx context.Context, _ worker.LeaseRequest) (worker.Lease, bool, error) {
	if s.delivered.CompareAndSwap(false, true) {
		return worker.Lease{Token: s.lease.Token, Job: handoff.CloneSnapshot(s.lease.Job)}, true, nil
	}
	<-ctx.Done()
	return worker.Lease{}, false, ctx.Err()
}

func integrationRunner(t *testing.T, repo *postgres.Repository, manual *clock.Manual, registry worker.ToolRegistry, id string, stream reasoner.Streamer, concurrency int) *worker.Runner {
	t.Helper()
	runner, err := worker.NewRunner(worker.Runner{Store: repo, Reasoner: stream, Safety: integrationPolicy{}, Tools: registry, Clock: manual, Jitter: zeroJitter{},
		Retry: worker.RetryPolicy{Base: time.Second, Maximum: time.Minute, MaxRetries: 2}, WorkerID: id, LeaseDuration: 9 * time.Second, Concurrency: concurrency,
		PollInterval: time.Second, MaxOutputBytes: 64 << 10, MaxOutputTokens: 1024, MaxToolRounds: 2, MaxToolCalls: 4, MaxToolArgumentBytes: 1024, MaxToolOutputBytes: 1024,
		ProviderSafetyID: func(string, string) (string, error) { return "integration-safety-id", nil },
	})
	require.NoError(t, err)
	return runner
}

func insertIntegrationJobs(t *testing.T, now time.Time, count int) *postgres.Repository {
	t.Helper()
	pool := testkit.Postgres(t)
	require.NoError(t, migrations.Up(t.Context(), pool))
	manual := clock.NewManual(now)
	registry, err := worker.NewToolRegistry(map[string][]worker.RegisteredTool{})
	require.NoError(t, err)
	repo, err := postgres.NewRepository(pool, manual, postgres.WithToolGate(registry))
	require.NoError(t, err)
	for index := range count {
		tenant, conversation, handoffID := "tenant-integration", fmt.Sprintf("conversation-%d", index), fmt.Sprintf("handoff-%d", index)
		_, err := pool.Exec(t.Context(), `INSERT INTO conversations(tenant_id,id,user_id,retention_until,created_at,updated_at) VALUES($1,$2,'user-integration',$3,$4,$4)`, tenant, conversation, now.Add(24*time.Hour), now)
		require.NoError(t, err)
		_, err = pool.Exec(t.Context(), `INSERT INTO handoffs(tenant_id,conversation_id,id,user_id,session_id,source_turn_id,source_turn_revision,user_request,objective,context_ref,context_hash,deadline,token_budget,tool_allowlist,approval_policy,model_version,prompt_version,tool_registry_version,config_version,idempotency_key,request_hash,ready_at,created_at,updated_at)
			VALUES($1,$2,$3,'user-integration','session','turn',1,'request','objective','context','hash',$4,1024,'[]','{}','model','prompt','tools','config',$5,'hash',$6,$6,$6)`, tenant, conversation, handoffID, now.Add(time.Hour), "key-"+handoffID, now)
		require.NoError(t, err)
	}
	return repo
}

func TestTwoRunnersBoundLeasesFenceStaleCompletionAndRecoverAfterExpiry(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	pool := testkit.Postgres(t)
	require.NoError(t, migrations.Up(t.Context(), pool))
	manual := clock.NewManual(now)
	registry, err := worker.NewToolRegistry(map[string][]worker.RegisteredTool{})
	require.NoError(t, err)
	repo, err := postgres.NewRepository(pool, manual, postgres.WithToolGate(registry))
	require.NoError(t, err)
	for index := range 2 {
		conversation, handoffID := fmt.Sprintf("conversation-%d", index), fmt.Sprintf("handoff-%d", index)
		_, err := pool.Exec(t.Context(), `INSERT INTO conversations(tenant_id,id,user_id,retention_until,created_at,updated_at) VALUES('tenant-integration',$1,'user-integration',$2,$3,$3)`, conversation, now.Add(24*time.Hour), now)
		require.NoError(t, err)
		_, err = pool.Exec(t.Context(), `INSERT INTO handoffs(tenant_id,conversation_id,id,user_id,session_id,source_turn_id,source_turn_revision,user_request,objective,context_ref,context_hash,deadline,token_budget,tool_allowlist,approval_policy,model_version,prompt_version,tool_registry_version,config_version,idempotency_key,request_hash,ready_at,created_at,updated_at)
			VALUES('tenant-integration',$1,$2,'user-integration','session','turn',1,'request','objective','context','hash',$3,1024,'[]','{}','model','prompt','tools','config',$4,'hash',$5,$5,$5)`, conversation, handoffID, now.Add(time.Hour), "key-"+handoffID, now)
		require.NoError(t, err)
	}

	started := make(chan struct{}, 2)
	blocking := integrationStreamer(func(ctx context.Context, _ reasoner.Request, _ func(reasoner.Chunk) error) (reasoner.Result, error) {
		started <- struct{}{}
		<-ctx.Done()
		return reasoner.Result{}, ctx.Err()
	})
	ctxA, cancelA := context.WithCancel(t.Context())
	ctxB, cancelB := context.WithCancel(t.Context())
	runnerA := integrationRunner(t, repo, manual, registry, "runner-a", blocking, 1)
	runnerB := integrationRunner(t, repo, manual, registry, "runner-b", blocking, 1)
	doneA, doneB := make(chan error, 1), make(chan error, 1)
	go func() { doneA <- runnerA.Run(ctxA) }()
	go func() { doneB <- runnerB.Run(ctxB) }()
	<-started
	<-started
	var active int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM handoffs WHERE lease_owner IS NOT NULL`).Scan(&active))
	assert.Equal(t, 2, active)

	var stale worker.LeaseToken
	var conversation, handoffID, owner string
	var version, attempt int64
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT conversation_id,id,lease_owner,lease_version,attempt FROM handoffs WHERE lease_owner='runner-a'`).Scan(&conversation, &handoffID, &owner, &version, &attempt))
	stale = worker.LeaseToken{TenantID: "tenant-integration", ConversationID: identity.ConversationID(conversation), HandoffID: identity.HandoffID(handoffID), WorkerID: owner, Version: version, Attempt: uint32(attempt)}
	cancelA()
	cancelB()
	require.NoError(t, <-doneA)
	require.NoError(t, <-doneB)
	manual.Advance(10 * time.Second)
	count, err := repo.SweepExpired(t.Context(), manual.Now(), 10)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
	err = repo.Complete(t.Context(), stale, worker.CompletionRecord{ExpectedObjectiveRevision: 1, Result: handoff.Result{MutationID: "stale", Summary: "stale", Structured: json.RawMessage(`{}`), SafetyDecision: json.RawMessage(`{"action":"allow"}`), Usage: json.RawMessage(`{}`)}})
	assert.ErrorIs(t, err, worker.ErrLeaseLost)

	recoveryCtx, stopRecovery := context.WithCancel(t.Context())
	recovery := integrationRunner(t, repo, manual, registry, "runner-recovery", integrationStreamer(func(context.Context, reasoner.Request, func(reasoner.Chunk) error) (reasoner.Result, error) {
		return reasoner.Result{Text: "recovered", FinishReason: "stop"}, nil
	}), 2)
	recoveryDone := make(chan error, 1)
	go func() { recoveryDone <- recovery.Run(recoveryCtx) }()
	for {
		var succeeded int
		require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM handoffs WHERE state='succeeded'`).Scan(&succeeded))
		if succeeded == 2 {
			break
		}
		runtime.Gosched()
	}
	stopRecovery()
	require.NoError(t, <-recoveryDone)
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM handoffs WHERE lease_owner IS NOT NULL`).Scan(&active))
	assert.Zero(t, active)
}

func TestRunnerDeadlineCrossedAfterAcquisitionFailsDurablyWithoutTerminatingWorkerLoop(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	pool := testkit.Postgres(t)
	require.NoError(t, migrations.Up(t.Context(), pool))
	manual := clock.NewManual(now)
	registry, err := worker.NewToolRegistry(map[string][]worker.RegisteredTool{})
	require.NoError(t, err)
	repo, err := postgres.NewRepository(pool, manual, postgres.WithToolGate(registry))
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `INSERT INTO conversations(tenant_id,id,user_id,retention_until,created_at,updated_at) VALUES('tenant-integration','conversation-deadline','user-integration',$1,$2,$2)`, now.Add(24*time.Hour), now)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `INSERT INTO handoffs(tenant_id,conversation_id,id,user_id,session_id,source_turn_id,source_turn_revision,user_request,objective,context_ref,context_hash,deadline,token_budget,tool_allowlist,approval_policy,model_version,prompt_version,tool_registry_version,config_version,idempotency_key,request_hash,ready_at,created_at,updated_at)
		VALUES('tenant-integration','conversation-deadline','handoff-deadline','user-integration','session','turn',1,'request','objective','context','hash',$1,1024,'[]','{}','model','prompt','tools','config','key-deadline','hash',$2,$2,$2)`, now.Add(time.Hour), now)
	require.NoError(t, err)
	lease, ok, err := repo.LeaseNext(t.Context(), worker.LeaseRequest{WorkerID: "deadline-runner", LeaseDuration: 2 * time.Hour, Now: now})
	require.NoError(t, err)
	require.True(t, ok)
	manual.Advance(time.Hour)
	ctx, cancel := context.WithCancel(t.Context())
	store := &acquiredLeaseStore{Store: repo, lease: lease}
	runner := integrationRunner(t, repo, manual, registry, "deadline-runner", integrationStreamer(func(context.Context, reasoner.Request, func(reasoner.Chunk) error) (reasoner.Result, error) {
		t.Fatal("reasoner called after deadline")
		return reasoner.Result{}, nil
	}), 1)
	runner.Store = store
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	for {
		var state, failureCode string
		err = pool.QueryRow(t.Context(), `SELECT state,COALESCE(failure_code,'') FROM handoffs WHERE id=$1`, lease.Token.HandoffID).Scan(&state, &failureCode)
		require.NoError(t, err)
		if state == string(handoff.StateFailed) {
			assert.Equal(t, "deadline_expired", failureCode)
			break
		}
		runtime.Gosched()
	}
	cancel()
	require.NoError(t, <-done)
}

func TestRunnerTerminalBoundarySteeringRestartsAndCancellationWinsDurably(t *testing.T) {
	for _, action := range []string{"steer", "cancel"} {
		t.Run(action, func(t *testing.T) {
			now := time.Now().UTC().Truncate(time.Microsecond)
			pool := testkit.Postgres(t)
			require.NoError(t, migrations.Up(t.Context(), pool))
			manual := clock.NewManual(now)
			registry, err := worker.NewToolRegistry(map[string][]worker.RegisteredTool{})
			require.NoError(t, err)
			repo, err := postgres.NewRepository(pool, manual, postgres.WithToolGate(registry))
			require.NoError(t, err)
			conversation, handoffID := "conversation-boundary-"+action, "handoff-boundary-"+action
			_, err = pool.Exec(t.Context(), `INSERT INTO conversations(tenant_id,id,user_id,retention_until,created_at,updated_at) VALUES('tenant-integration',$1,'user-integration',$2,$3,$3)`, conversation, now.Add(24*time.Hour), now)
			require.NoError(t, err)
			_, err = pool.Exec(t.Context(), `INSERT INTO handoffs(tenant_id,conversation_id,id,user_id,session_id,source_turn_id,source_turn_revision,user_request,objective,context_ref,context_hash,deadline,token_budget,tool_allowlist,approval_policy,model_version,prompt_version,tool_registry_version,config_version,idempotency_key,request_hash,ready_at,created_at,updated_at)
				VALUES('tenant-integration',$1,$2,'user-integration','session','turn',1,'request','objective','context','hash',$3,1024,'[]','{}','model','prompt','tools','config',$4,'hash',$5,$5,$5)`, conversation, handoffID, now.Add(time.Hour), "key-"+action, now)
			require.NoError(t, err)
			scope := handoff.Scope{TenantID: "tenant-integration", UserID: "user-integration", ConversationID: identity.ConversationID(conversation)}
			boundary := &terminalBoundaryStore{Store: repo}
			if action == "steer" {
				boundary.before = func(ctx context.Context) error {
					_, err := repo.SteerHandoff(ctx, scope, identity.HandoffID(handoffID), handoff.SteerRequest{Objective: "revised", ExpectedRevision: 1, Actor: "user", IdempotencyKey: "boundary-steer"})
					return err
				}
			} else {
				boundary.before = func(ctx context.Context) error {
					_, err := repo.RequestHandoffCancel(ctx, scope, identity.HandoffID(handoffID), handoff.CancelRequest{Actor: "user", Reason: "stop", IdempotencyKey: "boundary-cancel"})
					return err
				}
			}
			var generations atomic.Int32
			runner := integrationRunner(t, repo, manual, registry, "boundary-runner", integrationStreamer(func(_ context.Context, request reasoner.Request, _ func(reasoner.Chunk) error) (reasoner.Result, error) {
				generations.Add(1)
				if action == "steer" && generations.Load() > 1 {
					assert.Contains(t, request.Messages[1].Content, "revised")
				}
				return reasoner.Result{Text: "done"}, nil
			}), 1)
			runner.Store = boundary
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() { done <- runner.Run(ctx) }()
			want := handoff.StateSucceeded
			if action == "cancel" {
				want = handoff.StateCancelled
			}
			for {
				var state string
				require.NoError(t, pool.QueryRow(t.Context(), `SELECT state FROM handoffs WHERE id=$1`, handoffID).Scan(&state))
				if state == string(want) {
					break
				}
				runtime.Gosched()
			}
			cancel()
			require.NoError(t, <-done)
			if action == "steer" {
				assert.Equal(t, int32(2), generations.Load())
			} else {
				assert.Equal(t, int32(1), generations.Load())
			}
		})
	}
}

func TestRunnerSteerRacingProgressAndToolCompletionRestartsWithoutLeaseExpiry(t *testing.T) {
	for _, boundaryName := range []string{"progress", "tool completion"} {
		t.Run(boundaryName, func(t *testing.T) {
			now := time.Now().UTC().Truncate(time.Microsecond)
			pool := testkit.Postgres(t)
			require.NoError(t, migrations.Up(t.Context(), pool))
			manual := clock.NewManual(now)
			var executions atomic.Int32
			var tools []worker.RegisteredTool
			allowlist := `[]`
			if boundaryName == "tool completion" {
				allowlist = `["charge"]`
				tools = []worker.RegisteredTool{{Definition: reasoner.ToolDefinition{Name: "charge", Parameters: json.RawMessage(`{"type":"object"}`)}, ReadOnly: true, Executor: integrationToolExecutor(func(context.Context, worker.ToolExecutionRequest) (json.RawMessage, error) {
					executions.Add(1)
					return json.RawMessage(`{"ok":true}`), nil
				})}}
			}
			registry, err := worker.NewToolRegistry(map[string][]worker.RegisteredTool{"tools": tools})
			require.NoError(t, err)
			repo, err := postgres.NewRepository(pool, manual, postgres.WithToolGate(registry))
			require.NoError(t, err)
			conversation, handoffID := "conversation-fence-"+boundaryName, "handoff-fence-"+boundaryName
			_, err = pool.Exec(t.Context(), `INSERT INTO conversations(tenant_id,id,user_id,retention_until,created_at,updated_at) VALUES('tenant-integration',$1,'user-integration',$2,$3,$3)`, conversation, now.Add(24*time.Hour), now)
			require.NoError(t, err)
			_, err = pool.Exec(t.Context(), `INSERT INTO handoffs(tenant_id,conversation_id,id,user_id,session_id,source_turn_id,source_turn_revision,user_request,objective,context_ref,context_hash,deadline,token_budget,tool_allowlist,approval_policy,model_version,prompt_version,tool_registry_version,config_version,idempotency_key,request_hash,ready_at,created_at,updated_at)
				VALUES('tenant-integration',$1,$2,'user-integration','session','turn',1,'request','objective','context','hash',$3,1024,$4::json,'{"mode":"none"}','model','prompt','tools','config',$5,'hash',$6,$6,$6)`, conversation, handoffID, now.Add(time.Hour), allowlist, "key-fence-"+boundaryName, now)
			require.NoError(t, err)
			scope := handoff.Scope{TenantID: "tenant-integration", UserID: "user-integration", ConversationID: identity.ConversationID(conversation)}
			steer := func(ctx context.Context) error {
				_, err := repo.SteerHandoff(ctx, scope, identity.HandoffID(handoffID), handoff.SteerRequest{Objective: "revised", ExpectedRevision: 1, Actor: "user", IdempotencyKey: "steer-fence"})
				return err
			}
			var boundary worker.Store
			if boundaryName == "progress" {
				boundary = &progressBoundaryStore{Store: repo, before: steer}
			} else {
				boundary = &toolCompletionBoundaryStore{Store: repo, before: steer}
			}
			var generations atomic.Int32
			stream := integrationStreamer(func(_ context.Context, request reasoner.Request, onChunk func(reasoner.Chunk) error) (reasoner.Result, error) {
				if generations.Add(1) == 1 {
					if boundaryName == "progress" {
						return reasoner.Result{}, onChunk(reasoner.Chunk{Text: "discarded"})
					}
					return reasoner.Result{ToolCalls: []reasoner.ToolCall{{ID: "call", Name: "charge", Arguments: json.RawMessage(`{}`)}}}, nil
				}
				assert.Contains(t, request.Messages[1].Content, "revised")
				return reasoner.Result{Text: "revised result"}, nil
			})
			runner := integrationRunner(t, repo, manual, registry, "fence-runner", stream, 1)
			runner.Store = boundary
			if boundaryName == "tool completion" {
				runner.ArgumentSealer, err = worker.NewAESArgumentSealer("integration-key", bytes.Repeat([]byte{5}, 32), bytes.NewReader(bytes.Repeat([]byte{1}, 12)))
				require.NoError(t, err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() { done <- runner.Run(ctx) }()
			for {
				var state string
				require.NoError(t, pool.QueryRow(t.Context(), `SELECT state FROM handoffs WHERE id=$1`, handoffID).Scan(&state))
				if state == string(handoff.StateSucceeded) {
					break
				}
				if state == string(handoff.StateNeedsInput) || state == string(handoff.StateFailed) || state == string(handoff.StateCancelled) || state == string(handoff.StateExpired) {
					cancel()
					require.NoError(t, <-done)
					t.Fatalf("unexpected terminal/boundary state %s", state)
				}
				runtime.Gosched()
			}
			cancel()
			require.NoError(t, <-done)
			assert.Equal(t, int32(2), generations.Load())
			if boundaryName == "tool completion" {
				assert.Equal(t, int32(1), executions.Load())
			}
			var version, attempt int64
			require.NoError(t, pool.QueryRow(t.Context(), `SELECT lease_version,attempt FROM handoffs WHERE id=$1`, handoffID).Scan(&version, &attempt))
			assert.Equal(t, int64(1), version)
			assert.Equal(t, int64(1), attempt)
		})
	}
}

func TestRunnerRejectsSteerAfterSideEffectWithoutIssuingRevisedInvocation(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	pool := testkit.Postgres(t)
	require.NoError(t, migrations.Up(t.Context(), pool))
	manual := clock.NewManual(now)
	var executions atomic.Int32
	tool := worker.RegisteredTool{Definition: reasoner.ToolDefinition{Name: "charge", Parameters: json.RawMessage(`{"type":"object"}`)}, SideEffecting: true, StableIdempotency: true, Executor: integrationToolExecutor(func(context.Context, worker.ToolExecutionRequest) (json.RawMessage, error) {
		executions.Add(1)
		return json.RawMessage(`{"charged":true}`), nil
	})}
	registry, err := worker.NewToolRegistry(map[string][]worker.RegisteredTool{"tools": {tool}})
	require.NoError(t, err)
	repo, err := postgres.NewRepository(pool, manual, postgres.WithToolGate(registry))
	require.NoError(t, err)
	conversation, handoffID := "conversation-side-effect-steer", "handoff-side-effect-steer"
	_, err = pool.Exec(t.Context(), `INSERT INTO conversations(tenant_id,id,user_id,retention_until,created_at,updated_at) VALUES('tenant-integration',$1,'user-integration',$2,$3,$3)`, conversation, now.Add(24*time.Hour), now)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `INSERT INTO handoffs(tenant_id,conversation_id,id,user_id,session_id,source_turn_id,source_turn_revision,user_request,objective,context_ref,context_hash,deadline,token_budget,tool_allowlist,approval_policy,model_version,prompt_version,tool_registry_version,config_version,idempotency_key,request_hash,ready_at,created_at,updated_at)
		VALUES('tenant-integration',$1,$2,'user-integration','session','turn',1,'request','objective','context','hash',$3,1024,'["charge"]'::json,'{"mode":"none"}','model','prompt','tools','config','key-side-effect-steer','hash',$4,$4,$4)`, conversation, handoffID, now.Add(time.Hour), now)
	require.NoError(t, err)
	scope := handoff.Scope{TenantID: "tenant-integration", UserID: "user-integration", ConversationID: identity.ConversationID(conversation)}
	steerResult := make(chan error, 1)
	boundary := &sideEffectSteerBoundaryStore{Store: repo, result: steerResult, before: func(ctx context.Context) error {
		_, err := repo.SteerHandoff(ctx, scope, identity.HandoffID(handoffID), handoff.SteerRequest{Objective: "revised", ExpectedRevision: 1, Actor: "user", IdempotencyKey: "side-effect-steer"})
		return err
	}}
	var providerCalls atomic.Int32
	stream := integrationStreamer(func(_ context.Context, request reasoner.Request, _ func(reasoner.Chunk) error) (reasoner.Result, error) {
		call := providerCalls.Add(1)
		revised := strings.Contains(request.Messages[1].Content, "revised")
		if call == 1 || (revised && len(request.Messages) == 2) {
			return reasoner.Result{ToolCalls: []reasoner.ToolCall{{ID: fmt.Sprintf("charge-%d", call), Name: "charge", Arguments: json.RawMessage(`{"amount":100}`)}}}, nil
		}
		return reasoner.Result{Text: "done"}, nil
	})
	runner := integrationRunner(t, repo, manual, registry, "side-effect-runner", stream, 1)
	runner.Store = boundary
	runner.MaxToolCalls = 4
	runner.MaxToolRounds = 4
	nonces := append(bytes.Repeat([]byte{1}, 12), bytes.Repeat([]byte{2}, 12)...)
	runner.ArgumentSealer, err = worker.NewAESArgumentSealer("integration-key", bytes.Repeat([]byte{5}, 32), bytes.NewReader(nonces))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	require.ErrorIs(t, <-steerResult, handoff.ErrSteeringBlocked)
	for {
		var state string
		require.NoError(t, pool.QueryRow(t.Context(), `SELECT state FROM handoffs WHERE id=$1`, handoffID).Scan(&state))
		if state == string(handoff.StateSucceeded) {
			break
		}
		if state == string(handoff.StateFailed) || state == string(handoff.StateCancelled) || state == string(handoff.StateExpired) || state == string(handoff.StateNeedsInput) {
			cancel()
			require.NoError(t, <-done)
			t.Fatalf("unexpected state %s", state)
		}
		runtime.Gosched()
	}
	cancel()
	require.NoError(t, <-done)
	assert.Equal(t, int32(1), executions.Load())
	assert.Equal(t, int32(2), providerCalls.Load())
	var revision int64
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT objective_revision FROM handoffs WHERE id=$1`, handoffID).Scan(&revision))
	assert.Zero(t, revision)
}

func TestRunnerRejectsSteerAfterCancelHeartbeatAndTerminalizesImmediately(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	pool := testkit.Postgres(t)
	require.NoError(t, migrations.Up(t.Context(), pool))
	manual := clock.NewManual(now)
	registry, err := worker.NewToolRegistry(map[string][]worker.RegisteredTool{"tools": {}})
	require.NoError(t, err)
	repo, err := postgres.NewRepository(pool, manual, postgres.WithToolGate(registry))
	require.NoError(t, err)
	conversation, handoffID := "conversation-post-cancel-heartbeat", "handoff-post-cancel-heartbeat"
	_, err = pool.Exec(t.Context(), `INSERT INTO conversations(tenant_id,id,user_id,retention_until,created_at,updated_at) VALUES('tenant-integration',$1,'user-integration',$2,$3,$3)`, conversation, now.Add(24*time.Hour), now)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `INSERT INTO handoffs(tenant_id,conversation_id,id,user_id,session_id,source_turn_id,source_turn_revision,user_request,objective,context_ref,context_hash,deadline,token_budget,tool_allowlist,approval_policy,model_version,prompt_version,tool_registry_version,config_version,idempotency_key,request_hash,ready_at,created_at,updated_at)
		VALUES('tenant-integration',$1,$2,'user-integration','session','turn',1,'request','objective','context','hash',$3,1024,'[]'::json,'{"mode":"none"}','model','prompt','tools','config','key-post-cancel-heartbeat','hash',$4,$4,$4)`, conversation, handoffID, now.Add(time.Hour), now)
	require.NoError(t, err)
	scope := handoff.Scope{TenantID: "tenant-integration", UserID: "user-integration", ConversationID: identity.ConversationID(conversation)}
	steerResult := make(chan error, 1)
	boundary := &postCancelHeartbeatStore{Store: repo, result: steerResult, after: func(ctx context.Context) error {
		_, err := repo.SteerHandoff(ctx, scope, identity.HandoffID(handoffID), handoff.SteerRequest{Objective: "too late", ExpectedRevision: 1, Actor: "user", IdempotencyKey: "post-cancel-heartbeat-steer"})
		return err
	}}
	started := make(chan struct{})
	var generations atomic.Int32
	stream := integrationStreamer(func(ctx context.Context, _ reasoner.Request, _ func(reasoner.Chunk) error) (reasoner.Result, error) {
		generations.Add(1)
		close(started)
		<-ctx.Done()
		return reasoner.Result{}, ctx.Err()
	})
	runner := integrationRunner(t, repo, manual, registry, "post-cancel-runner", stream, 1)
	runner.Store = boundary
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	defer func() {
		cancel()
		<-done
	}()
	<-started
	_, err = repo.RequestHandoffCancel(t.Context(), scope, identity.HandoffID(handoffID), handoff.CancelRequest{Actor: "user", Reason: "stop", IdempotencyKey: "post-heartbeat-cancel"})
	require.NoError(t, err)
	manual.Advance(3 * time.Second)
	require.ErrorIs(t, <-steerResult, handoff.ErrSteeringBlocked)
	for {
		var state string
		require.NoError(t, pool.QueryRow(t.Context(), `SELECT state FROM handoffs WHERE id=$1`, handoffID).Scan(&state))
		if state == string(handoff.StateCancelled) {
			break
		}
		runtime.Gosched()
	}
	assert.Equal(t, int32(1), generations.Load())
}

type integrationToolExecutor func(context.Context, worker.ToolExecutionRequest) (json.RawMessage, error)

func (f integrationToolExecutor) Execute(ctx context.Context, request worker.ToolExecutionRequest) (json.RawMessage, error) {
	return f(ctx, request)
}
