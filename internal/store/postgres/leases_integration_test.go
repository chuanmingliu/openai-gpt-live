//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/worker"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createRunnableHandoff(t *testing.T, repo *Repository, suffix string) handoff.CreateRecord {
	t.Helper()
	scope := testScope("tenant-"+suffix, "conversation-"+suffix)
	insertConversation(t, repo.pool, scope)
	record := prepareCreate(t, repo, scope, suffix)
	record.Request.ApprovalPolicy = json.RawMessage(`{"mode":"none"}`)
	record.RequestHash, _ = handoff.CanonicalRequestHash(record.Request)
	_, _, err := repo.CreateHandoff(t.Context(), record)
	require.NoError(t, err)
	return record
}

// Removing SKIP LOCKED or failing to increment both fencing components lets
// two workers own one attempt, or permits an ABA token to mutate a reacquired job.
func TestLeaseConcurrentAcquisitionAndABAFencing(t *testing.T) {
	repo, _, _ := repositoryHarness(t)
	first := createRunnableHandoff(t, repo, "lease-a")
	second := createRunnableHandoff(t, repo, "lease-b")
	now := repositoryTestNow.Add(time.Minute)

	type result struct {
		lease worker.Lease
		ok    bool
		err   error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	for _, id := range []string{"worker-a", "worker-b"} {
		go func(id string) {
			<-start
			lease, ok, err := repo.LeaseNext(t.Context(), worker.LeaseRequest{WorkerID: id, LeaseDuration: time.Minute, Now: now})
			results <- result{lease, ok, err}
		}(id)
	}
	close(start)
	a, b := <-results, <-results
	require.NoError(t, a.err)
	require.NoError(t, b.err)
	require.True(t, a.ok)
	require.True(t, b.ok)
	assert.NotEqual(t, a.lease.Token.HandoffID, b.lease.Token.HandoffID)
	assert.ElementsMatch(t, []any{first.HandoffID, second.HandoffID}, []any{a.lease.Token.HandoffID, b.lease.Token.HandoffID})
	assert.Equal(t, int64(1), a.lease.Token.Version)
	assert.Equal(t, uint32(1), a.lease.Token.Attempt)

	old := a.lease.Token
	_, err := repo.pool.Exec(t.Context(), `UPDATE handoffs SET lease_expires_at=$1 WHERE id=$2`, now.Add(-time.Second), old.HandoffID)
	require.NoError(t, err)
	reacquired, ok, err := repo.LeaseNext(t.Context(), worker.LeaseRequest{WorkerID: "worker-c", LeaseDuration: time.Minute, Now: now})
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, old.HandoffID, reacquired.Token.HandoffID)
	assert.Equal(t, old.Version+1, reacquired.Token.Version)
	assert.Equal(t, old.Attempt+1, reacquired.Token.Attempt)
	err = repo.Complete(t.Context(), old, worker.CompletionRecord{ExpectedObjectiveRevision: reacquired.Job.ObjectiveRevision, Result: handoff.Result{MutationID: "stale", Summary: "bad", Structured: json.RawMessage(`{}`), SafetyDecision: json.RawMessage(`{"action":"allow"}`), Usage: json.RawMessage(`{}`)}})
	assert.ErrorIs(t, err, worker.ErrLeaseLost)
}

func TestLeaseNextSkipsExternallyLockedHighestPriorityCandidate(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	locked := createRunnableHandoff(t, repo, "skip-locked-high")
	available := createRunnableHandoff(t, repo, "skip-locked-low")
	_, err := pool.Exec(t.Context(), `UPDATE handoffs SET priority=CASE id WHEN $1 THEN 100 ELSE 0 END`, locked.HandoffID)
	require.NoError(t, err)
	blocker, err := pool.Begin(t.Context())
	require.NoError(t, err)
	defer rollback(blocker)
	_, err = blocker.Exec(t.Context(), `SELECT 1 FROM handoffs WHERE tenant_id=$1 AND conversation_id=$2 AND id=$3 FOR UPDATE`, locked.Request.Scope.TenantID, locked.Request.Scope.ConversationID, locked.HandoffID)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	lease, ok, err := repo.LeaseNext(ctx, worker.LeaseRequest{WorkerID: "skip-worker", LeaseDuration: time.Minute, Now: repositoryTestNow.Add(time.Minute)})
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, available.HandoffID, lease.Token.HandoffID)
	assert.Equal(t, available.Request.Scope.TenantID, lease.Token.TenantID)
}

// Removing the expiry predicate from heartbeat revives dead work; failing to
// clone input exposes repository-owned memory to the worker.
func TestLeaseHeartbeatControlAndExpiredToken(t *testing.T) {
	repo, _, _ := repositoryHarness(t)
	record := createRunnableHandoff(t, repo, "heartbeat")
	now := repositoryTestNow.Add(time.Minute)
	lease, ok, err := repo.LeaseNext(t.Context(), worker.LeaseRequest{WorkerID: "worker", LeaseDuration: time.Minute, Now: now})
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, repo.MarkRunning(t.Context(), lease.Token, now))
	_, err = repo.SteerHandoff(t.Context(), record.Request.Scope, record.HandoffID, handoff.SteerRequest{Objective: "new objective", ExpectedRevision: 1, Actor: "user", IdempotencyKey: "steer"})
	require.NoError(t, err)
	control, err := repo.Heartbeat(t.Context(), lease.Token, now.Add(2*time.Minute))
	require.NoError(t, err)
	assert.Equal(t, uint32(1), control.ObjectiveRevision)
	assert.Equal(t, "new objective", control.Objective)
	_, err = repo.pool.Exec(t.Context(), `UPDATE handoffs SET lease_expires_at=$1 WHERE id=$2`, repositoryTestNow.Add(-time.Second), record.HandoffID)
	require.NoError(t, err)
	_, err = repo.Heartbeat(t.Context(), lease.Token, now.Add(3*time.Minute))
	assert.ErrorIs(t, err, worker.ErrLeaseLost)
}

func TestEveryPostLeaseMutationRejectsStaleToken(t *testing.T) {
	_, pool, manual := repositoryHarness(t)
	repo, err := NewRepository(pool, manual, WithToolGate(testToolGate{"search": true}))
	require.NoError(t, err)
	_, stale, now := runningLease(t, repo, "stale-matrix")
	_, err = pool.Exec(t.Context(), `UPDATE handoffs SET lease_expires_at=$1 WHERE tenant_id=$2 AND conversation_id=$3 AND id=$4`, now.Add(-time.Second), stale.Token.TenantID, stale.Token.ConversationID, stale.Token.HandoffID)
	require.NoError(t, err)
	current, ok, err := repo.LeaseNext(t.Context(), worker.LeaseRequest{WorkerID: "replacement", LeaseDuration: time.Minute, Now: now})
	require.NoError(t, err)
	require.True(t, ok)
	hash := fmt.Sprintf("%064x", 7)
	invocationID := worker.StableToolInvocationID(current.Token.TenantID, current.Token.ConversationID, current.Token.HandoffID, 0, 1, "search", hash)
	invocation := worker.ToolInvocationRecord{InvocationID: invocationID, ObjectiveRevision: 0, ToolOrdinal: 1, ToolName: "search", IdempotencyKey: invocationID, ArgumentsHash: hash, Ciphertext: make([]byte, 16), Nonce: make([]byte, 12), KeyID: "key"}
	_, _, err = repo.BeginToolInvocation(t.Context(), current.Token, invocation)
	require.NoError(t, err)
	checks := map[string]func() error{
		"MarkRunning": func() error { return repo.MarkRunning(t.Context(), stale.Token, now) },
		"Heartbeat":   func() error { _, err := repo.Heartbeat(t.Context(), stale.Token, now.Add(time.Minute)); return err },
		"ScheduleRetry": func() error {
			return repo.ScheduleRetry(t.Context(), stale.Token, worker.RetryRecord{MutationID: "stale-retry", ExpectedObjectiveRevision: stale.Job.ObjectiveRevision, ReadyAt: now.Add(time.Minute), RetryCount: 1, Code: "x"})
		},
		"AppendProgress": func() error {
			return repo.AppendProgress(t.Context(), stale.Token, worker.ProgressRecord{MutationID: "stale-progress", ExpectedObjectiveRevision: stale.Job.ObjectiveRevision, Sequence: 1, Summary: "x"})
		},
		"MarkNeedsInput": func() error {
			return repo.MarkNeedsInput(t.Context(), stale.Token, worker.NeedsInputRecord{MutationID: "stale-input", ExpectedObjectiveRevision: stale.Job.ObjectiveRevision, Prompt: "x", Schema: json.RawMessage(`{}`)})
		},
		"Complete": func() error {
			return repo.Complete(t.Context(), stale.Token, worker.CompletionRecord{ExpectedObjectiveRevision: stale.Job.ObjectiveRevision, Result: handoff.Result{MutationID: "stale-complete", Summary: "x", Structured: json.RawMessage(`{}`), SafetyDecision: json.RawMessage(`{"action":"allow"}`), Usage: json.RawMessage(`{}`)}})
		},
		"Fail": func() error {
			return repo.Fail(t.Context(), stale.Token, worker.FailureRecord{ExpectedObjectiveRevision: stale.Job.ObjectiveRevision, Failure: handoff.Failure{MutationID: "stale-fail", Code: "x", Message: "x"}})
		},
		"MarkCancelled": func() error {
			return repo.MarkCancelled(t.Context(), stale.Token, worker.CancelledRecord{MutationID: "stale-cancel", ExpectedObjectiveRevision: stale.Job.ObjectiveRevision})
		},
		"BeginToolInvocation": func() error { _, _, err := repo.BeginToolInvocation(t.Context(), stale.Token, invocation); return err },
		"CompleteToolInvocation": func() error {
			return repo.CompleteToolInvocation(t.Context(), stale.Token, worker.ToolCompletionRecord{InvocationID: invocationID, MutationID: "stale-tool", Result: json.RawMessage(`{}`)})
		},
	}
	for name, check := range checks {
		t.Run(name, func(t *testing.T) { assert.ErrorIs(t, check(), worker.ErrLeaseLost) })
	}
	_, err = repo.Heartbeat(t.Context(), current.Token, now.Add(time.Minute))
	require.NoError(t, err)
}

// A terminal mutation replay must compare its canonical payload, while a
// different terminal payload under the same mutation ID is a conflict.
func TestLeaseCompleteExactReplayAndConflict(t *testing.T) {
	repo, _, _ := repositoryHarness(t)
	record := createRunnableHandoff(t, repo, "complete")
	now := repositoryTestNow.Add(time.Minute)
	lease, ok, err := repo.LeaseNext(t.Context(), worker.LeaseRequest{WorkerID: "worker", LeaseDuration: time.Minute, Now: now})
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, repo.MarkRunning(t.Context(), lease.Token, now))
	result := handoff.Result{MutationID: "done", Summary: "finished", Structured: json.RawMessage(`{"b":2,"a":1}`), SafetyDecision: json.RawMessage(`{"reason_code":"checked","action":"allow"}`), Usage: json.RawMessage(`{"tokens":3}`)}
	completion := worker.CompletionRecord{ExpectedObjectiveRevision: lease.Job.ObjectiveRevision, Result: result}
	require.NoError(t, repo.Complete(t.Context(), lease.Token, completion))
	require.NoError(t, repo.Complete(t.Context(), lease.Token, completion))
	loaded, err := repo.GetHandoff(t.Context(), record.Request.Scope, record.HandoffID)
	require.NoError(t, err)
	require.NotNil(t, loaded.Result)
	assert.JSONEq(t, `{"action":"allow","reason_code":"checked"}`, string(loaded.Result.SafetyDecision))
	result.Summary = "different"
	completion.Result = result
	err = repo.Complete(t.Context(), lease.Token, completion)
	assert.True(t, errors.Is(err, handoff.ErrIdempotencyConflict), err)
	result.Summary = "finished"
	result.SafetyDecision = json.RawMessage(`{"action":"redact","reason_code":"checked"}`)
	completion.Result = result
	err = repo.Complete(t.Context(), lease.Token, completion)
	assert.ErrorIs(t, err, handoff.ErrIdempotencyConflict)
}

func runningLease(t *testing.T, repo *Repository, suffix string) (handoff.CreateRecord, worker.Lease, time.Time) {
	t.Helper()
	record := createRunnableHandoff(t, repo, suffix)
	now := repositoryTestNow.Add(time.Minute)
	lease, ok, err := repo.LeaseNext(t.Context(), worker.LeaseRequest{WorkerID: "worker-" + suffix, LeaseDuration: 5 * time.Minute, Now: now})
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, repo.MarkRunning(t.Context(), lease.Token, now))
	return record, lease, now
}

// Resetting lease_version on retry or ignoring ready_at lets a stale attempt
// execute concurrently with its scheduled retry.
func TestLeaseRetryReadinessAndNeedsInputReclamation(t *testing.T) {
	repo, pool, manual := repositoryHarness(t)
	record, lease, now := runningLease(t, repo, "retry")
	ready := now.Add(3 * time.Minute)
	require.NoError(t, repo.ScheduleRetry(t.Context(), lease.Token, worker.RetryRecord{MutationID: "retry-1", ExpectedObjectiveRevision: lease.Job.ObjectiveRevision, ReadyAt: ready, RetryCount: 1, Code: "provider"}))
	var state, owner string
	var version int64
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT state,COALESCE(lease_owner,''),lease_version FROM handoffs WHERE id=$1`, record.HandoffID).Scan(&state, &owner, &version))
	assert.Equal(t, "running", state)
	assert.Empty(t, owner)
	assert.Equal(t, lease.Token.Version, version)
	_, ok, err := repo.LeaseNext(t.Context(), worker.LeaseRequest{WorkerID: "early", LeaseDuration: time.Minute, Now: ready.Add(-time.Nanosecond)})
	require.NoError(t, err)
	assert.False(t, ok)
	manual.Advance(ready.Sub(repositoryTestNow))
	retry, ok, err := repo.LeaseNext(t.Context(), worker.LeaseRequest{WorkerID: "retry-worker", LeaseDuration: time.Minute, Now: ready})
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, lease.Token.Version+1, retry.Token.Version)
	require.NoError(t, repo.MarkNeedsInput(t.Context(), retry.Token, worker.NeedsInputRecord{MutationID: "question", ExpectedObjectiveRevision: retry.Job.ObjectiveRevision, Prompt: "Which?", Schema: json.RawMessage(`{"type":"string"}`)}))
	_, err = repo.ProvideHandoffInput(t.Context(), record.Request.Scope, record.HandoffID, handoff.InputRequest{Input: json.RawMessage(`{"answer":"one"}`), ExpectedRevision: 2, Actor: "user", IdempotencyKey: "out-of-order"})
	assert.ErrorIs(t, err, handoff.ErrRevisionConflict)
	input := handoff.InputRequest{Input: json.RawMessage(`{"answer":"one"}`), ExpectedRevision: 1, Actor: "user", IdempotencyKey: "input-1"}
	_, err = repo.ProvideHandoffInput(t.Context(), record.Request.Scope, record.HandoffID, input)
	require.NoError(t, err)
	_, err = repo.ProvideHandoffInput(t.Context(), record.Request.Scope, record.HandoffID, input)
	require.NoError(t, err)
	resumed, ok, err := repo.LeaseNext(t.Context(), worker.LeaseRequest{WorkerID: "input-worker", LeaseDuration: time.Minute, Now: ready.Add(time.Second)})
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, record.HandoffID, resumed.Token.HandoffID)
	assert.JSONEq(t, `{"answer":"one"}`, string(resumed.Job.Input))
	resumed.Job.Input[0] = '['
	control, err := repo.Heartbeat(t.Context(), resumed.Token, ready.Add(2*time.Minute))
	require.NoError(t, err)
	assert.Equal(t, uint32(1), control.InputRevision)
	assert.Equal(t, uint32(1), control.ConsumedInput)
	assert.JSONEq(t, `{"answer":"one"}`, string(control.Input))
	_, err = pool.Exec(t.Context(), `UPDATE handoffs SET lease_expires_at=$1 WHERE id=$2`, ready, record.HandoffID)
	require.NoError(t, err)
	recovered, ok, err := repo.LeaseNext(t.Context(), worker.LeaseRequest{WorkerID: "recovery-worker", LeaseDuration: time.Minute, Now: ready.Add(3 * time.Minute)})
	require.NoError(t, err)
	require.True(t, ok)
	assert.JSONEq(t, `{"answer":"one"}`, string(recovered.Job.Input))
	assert.Equal(t, uint32(1), recovered.Job.ConsumedInput)
}

func TestProgressAfterSteerAndRetryReacquireIsRevisionFencedAndReplaySafe(t *testing.T) {
	repo, pool, manual := repositoryHarness(t)
	record, lease, now := runningLease(t, repo, "progress-generations")
	first := worker.ProgressRecord{MutationID: "progress-version-1-attempt-1-objective-0-input-0-sequence-1", ExpectedObjectiveRevision: lease.Job.ObjectiveRevision, Sequence: 1, Summary: "first"}
	require.NoError(t, repo.AppendProgress(t.Context(), lease.Token, first))
	_, err := repo.SteerHandoff(t.Context(), record.Request.Scope, record.HandoffID, handoff.SteerRequest{Objective: "revised", ExpectedRevision: 1, Actor: "user", IdempotencyKey: "steer-progress"})
	require.NoError(t, err)
	stale := worker.ProgressRecord{MutationID: "progress-version-1-attempt-1-objective-0-input-0-sequence-2", ExpectedObjectiveRevision: 0, Sequence: 2, Summary: "stale"}
	err = repo.AppendProgress(t.Context(), lease.Token, stale)
	assert.ErrorIs(t, err, worker.ErrLeaseLost)
	steered := worker.ProgressRecord{MutationID: "progress-version-1-attempt-1-objective-1-input-0-sequence-1", ExpectedObjectiveRevision: 1, Sequence: 1, Summary: "steered"}
	require.NoError(t, repo.AppendProgress(t.Context(), lease.Token, steered))
	require.NoError(t, repo.AppendProgress(t.Context(), lease.Token, steered))
	ready := now.Add(time.Minute)
	require.NoError(t, repo.ScheduleRetry(t.Context(), lease.Token, worker.RetryRecord{MutationID: "retry-after-steer", ExpectedObjectiveRevision: 1, ReadyAt: ready, RetryCount: 1, Code: "provider"}))
	manual.Advance(ready.Sub(repositoryTestNow))
	reacquired, ok, err := repo.LeaseNext(t.Context(), worker.LeaseRequest{WorkerID: "retry-progress-worker", LeaseDuration: time.Minute, Now: ready})
	require.NoError(t, err)
	require.True(t, ok)
	retryProgress := worker.ProgressRecord{MutationID: "progress-version-2-attempt-2-objective-1-input-0-sequence-1", ExpectedObjectiveRevision: 1, Sequence: 1, Summary: "retry"}
	require.NoError(t, repo.AppendProgress(t.Context(), reacquired.Token, retryProgress))
	var events, distinct int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*),count(DISTINCT event_id) FROM handoff_events WHERE handoff_id=$1 AND kind='handoff.progress'`, record.HandoffID).Scan(&events, &distinct))
	assert.Equal(t, 3, events)
	assert.Equal(t, events, distinct)
}

func TestCancellationPendingAndCommittedBeforeCompletion(t *testing.T) {
	t.Run("pending", func(t *testing.T) {
		repo, _, _ := repositoryHarness(t)
		record := createRunnableHandoff(t, repo, "cancel-pending")
		_, err := repo.RequestHandoffCancel(t.Context(), record.Request.Scope, record.HandoffID, handoff.CancelRequest{Actor: "user", Reason: "stop", IdempotencyKey: "cancel"})
		require.NoError(t, err)
		count, err := repo.SweepExpired(t.Context(), repositoryTestNow.Add(time.Minute), 10)
		require.NoError(t, err)
		assert.Equal(t, 1, count)
		got, err := repo.GetHandoff(t.Context(), record.Request.Scope, record.HandoffID)
		require.NoError(t, err)
		assert.Equal(t, handoff.StateCancelled, got.State)
	})
	t.Run("running cancellation beats later completion", func(t *testing.T) {
		repo, _, _ := repositoryHarness(t)
		record, lease, now := runningLease(t, repo, "cancel-running")
		_, err := repo.RequestHandoffCancel(t.Context(), record.Request.Scope, record.HandoffID, handoff.CancelRequest{Actor: "user", Reason: "stop", IdempotencyKey: "cancel"})
		require.NoError(t, err)
		control, err := repo.Heartbeat(t.Context(), lease.Token, now.Add(4*time.Minute))
		require.NoError(t, err)
		assert.True(t, control.CancelRequested)
		err = repo.Complete(t.Context(), lease.Token, worker.CompletionRecord{Result: handoff.Result{MutationID: "late", Summary: "late", Structured: json.RawMessage(`{}`), SafetyDecision: json.RawMessage(`{"action":"allow"}`), Usage: json.RawMessage(`{}`)}})
		assert.ErrorIs(t, err, worker.ErrLeaseLost)
		_, err = repo.SweepExpired(t.Context(), now, 10)
		require.NoError(t, err)
		got, err := repo.GetHandoff(t.Context(), record.Request.Scope, record.HandoffID)
		require.NoError(t, err)
		assert.Equal(t, handoff.StateCancelled, got.State)
	})
}

func TestCancellationAndCompletionSerializeAtEventLock(t *testing.T) {
	run := func(t *testing.T, completionFirst bool) {
		base, pool, repositoryClock := repositoryHarness(t)
		record, lease, _ := runningLease(t, base, fmt.Sprintf("cancel-complete-%t", completionFirst))
		cancelRepo, err := NewRepository(pool, repositoryClock)
		require.NoError(t, err)
		completeRepo, err := NewRepository(pool, repositoryClock)
		require.NoError(t, err)
		entered, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		barrier := func(step string) error {
			if step == "event_lock" {
				once.Do(func() { close(entered) })
				select {
				case <-release:
					return nil
				case <-t.Context().Done():
					return t.Context().Err()
				}
			}
			return nil
		}
		firstDone, secondDone := make(chan error, 1), make(chan error, 1)
		if completionFirst {
			completeRepo.failpoint = barrier
			go func() {
				firstDone <- completeRepo.Complete(t.Context(), lease.Token, worker.CompletionRecord{Result: handoff.Result{MutationID: "complete-first", Summary: "done", Structured: json.RawMessage(`{}`), SafetyDecision: json.RawMessage(`{"action":"allow"}`), Usage: json.RawMessage(`{}`)}})
			}()
			<-entered
			go func() {
				_, err := cancelRepo.RequestHandoffCancel(t.Context(), record.Request.Scope, record.HandoffID, handoff.CancelRequest{Actor: "user", IdempotencyKey: "cancel-second"})
				secondDone <- err
			}()
		} else {
			cancelRepo.failpoint = barrier
			go func() {
				_, err := cancelRepo.RequestHandoffCancel(t.Context(), record.Request.Scope, record.HandoffID, handoff.CancelRequest{Actor: "user", IdempotencyKey: "cancel-first"})
				firstDone <- err
			}()
			<-entered
			go func() {
				secondDone <- completeRepo.Complete(t.Context(), lease.Token, worker.CompletionRecord{Result: handoff.Result{MutationID: "complete-second", Summary: "done", Structured: json.RawMessage(`{}`), SafetyDecision: json.RawMessage(`{"action":"allow"}`), Usage: json.RawMessage(`{}`)}})
			}()
		}
		waitForBlockedAdvisoryLock(t, pool, "pg_advisory_xact_lock")
		close(release)
		require.NoError(t, <-firstDone)
		secondErr := <-secondDone
		var wantState handoff.State
		if completionFirst {
			assert.ErrorIs(t, secondErr, handoff.ErrTerminalState)
			wantState = handoff.StateSucceeded
		} else {
			assert.ErrorIs(t, secondErr, worker.ErrLeaseLost)
			count, err := base.SweepExpired(t.Context(), repositoryTestNow.Add(time.Minute), 1)
			require.NoError(t, err)
			assert.Equal(t, 1, count)
			wantState = handoff.StateCancelled
		}
		got, err := base.GetHandoff(t.Context(), record.Request.Scope, record.HandoffID)
		require.NoError(t, err)
		assert.Equal(t, wantState, got.State)
		var terminals int
		require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM handoff_events
				WHERE tenant_id=$1 AND conversation_id=$2 AND handoff_id=$3
				  AND kind IN ('handoff.complete','handoff.failed','handoff.cancelled')`, lease.Token.TenantID, lease.Token.ConversationID, lease.Token.HandoffID).Scan(&terminals))
		assert.Equal(t, 1, terminals)
	}
	t.Run("completion wins", func(t *testing.T) { run(t, true) })
	t.Run("cancellation wins", func(t *testing.T) { run(t, false) })
}

// If SweepExpired decides from a stale pre-heartbeat snapshot, it can steal a
// lease that was extended while waiting for the row lock.
func TestLeaseHeartbeatThenSweepAndExpiredRecovery(t *testing.T) {
	repo, pool, manual := repositoryHarness(t)
	observed := 0
	repo.SetExpiredLeaseObserver(func(count int) { observed += count })
	record, lease, now := runningLease(t, repo, "sweep-race")
	manual.Advance(now.Sub(repositoryTestNow))
	_, err := repo.Heartbeat(t.Context(), lease.Token, now.Add(10*time.Minute))
	require.NoError(t, err)
	count, err := repo.SweepExpired(t.Context(), now.Add(6*time.Minute), 10)
	require.NoError(t, err)
	assert.Zero(t, count)
	_, err = pool.Exec(t.Context(), `UPDATE handoffs SET lease_expires_at=$1 WHERE id=$2`, now.Add(-time.Second), record.HandoffID)
	require.NoError(t, err)
	count, err = repo.SweepExpired(t.Context(), now, 10)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
	assert.Equal(t, 1, observed)
	_, err = repo.Heartbeat(t.Context(), lease.Token, now.Add(time.Minute))
	assert.ErrorIs(t, err, worker.ErrLeaseLost)
	recovered, ok, err := repo.LeaseNext(t.Context(), worker.LeaseRequest{WorkerID: "recovery", LeaseDuration: time.Minute, Now: now})
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, lease.Token.Version+1, recovered.Token.Version)
}

func TestExpiredLeaseObserverExcludesDeadlineAndCancellationTransitions(t *testing.T) {
	repo, _, _ := repositoryHarness(t)
	observed := 0
	repo.SetExpiredLeaseObserver(func(count int) { observed += count })
	deadline := createRunnableHandoff(t, repo, "observer-deadline")
	cancelled := createRunnableHandoff(t, repo, "observer-cancel")
	_, err := repo.RequestHandoffCancel(t.Context(), cancelled.Request.Scope, cancelled.HandoffID, handoff.CancelRequest{Actor: "user", Reason: "stop", IdempotencyKey: "observer-cancel"})
	require.NoError(t, err)
	count, err := repo.SweepExpired(t.Context(), deadline.Request.Deadline.Add(time.Second), 10)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
	assert.Zero(t, observed)
}

func TestSweepHoldingRowLockFencesConcurrentHeartbeat(t *testing.T) {
	base, pool, repositoryClock := repositoryHarness(t)
	record, lease, _ := runningLease(t, base, "sweep-heartbeat-barrier")
	_, err := pool.Exec(t.Context(), `UPDATE handoffs SET lease_expires_at=$1 WHERE tenant_id=$2 AND conversation_id=$3 AND id=$4`, repositoryTestNow.Add(30*time.Second), lease.Token.TenantID, lease.Token.ConversationID, lease.Token.HandoffID)
	require.NoError(t, err)
	sweepRepo, err := NewRepository(pool, repositoryClock)
	require.NoError(t, err)
	heartbeatRepo, err := NewRepository(pool, repositoryClock)
	require.NoError(t, err)
	locked, release := make(chan struct{}), make(chan struct{})
	sweepRepo.failpoint = func(step string) error {
		if step == "sweep_locked" {
			close(locked)
			select {
			case <-release:
				return nil
			case <-t.Context().Done():
				return t.Context().Err()
			}
		}
		return nil
	}
	sweepDone := make(chan error, 1)
	go func() {
		count, err := sweepRepo.SweepExpired(t.Context(), repositoryTestNow.Add(time.Minute), 1)
		if err == nil && count != 1 {
			err = fmt.Errorf("swept %d rows", count)
		}
		sweepDone <- err
	}()
	<-locked
	heartbeatDone := make(chan error, 1)
	go func() {
		_, err := heartbeatRepo.Heartbeat(t.Context(), lease.Token, repositoryTestNow.Add(5*time.Minute))
		heartbeatDone <- err
	}()
	waitForBlockedRepositoryQuery(t, pool, "UPDATE handoffs SET lease_expires_at=GREATEST")
	close(release)
	require.NoError(t, <-sweepDone)
	assert.ErrorIs(t, <-heartbeatDone, worker.ErrLeaseLost)
	got, err := base.GetHandoff(t.Context(), record.Request.Scope, record.HandoffID)
	require.NoError(t, err)
	assert.Equal(t, handoff.StateRunning, got.State)
}

func waitForBlockedRepositoryQuery(t *testing.T, pool interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, fragment string) {
	t.Helper()
	for {
		var blocked int
		err := pool.QueryRow(t.Context(), `SELECT count(*) FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%'||$1||'%'`, fragment).Scan(&blocked)
		require.NoError(t, err)
		if blocked > 0 {
			return
		}
		select {
		case <-t.Context().Done():
			t.Fatal("query never blocked on row lock")
		default:
			runtime.Gosched()
		}
	}
}

func waitForBlockedAdvisoryLock(t *testing.T, pool interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, fragment string) {
	t.Helper()
	for {
		var blocked int
		err := pool.QueryRow(t.Context(), `SELECT count(*)
			FROM pg_catalog.pg_locks lock_state
			JOIN pg_catalog.pg_stat_activity activity ON activity.pid=lock_state.pid
			WHERE lock_state.locktype='advisory' AND NOT lock_state.granted
			  AND activity.wait_event_type='Lock' AND activity.wait_event='advisory'
			  AND activity.query LIKE '%'||$1||'%'`, fragment).Scan(&blocked)
		require.NoError(t, err)
		if blocked > 0 {
			return
		}
		select {
		case <-t.Context().Done():
			t.Fatal("query never blocked on the event append advisory lock")
		default:
			runtime.Gosched()
		}
	}
}

func TestLeaseMutationFailpointsRollbackEventAndOutbox(t *testing.T) {
	for _, step := range []string{"worker_retry_update", "worker_retry_event", "worker_retry_wakeup"} {
		t.Run(step, func(t *testing.T) {
			repo, pool, _ := repositoryHarness(t)
			_, lease, now := runningLease(t, repo, "rollback-"+step)
			events, outbox := tableCount(t, pool, "handoff_events"), tableCount(t, pool, "outbox")
			repo.failpoint = func(got string) error {
				if got == step {
					return errors.New("injected")
				}
				return nil
			}
			err := repo.ScheduleRetry(t.Context(), lease.Token, worker.RetryRecord{MutationID: "retry", ReadyAt: now.Add(time.Minute), RetryCount: 1, Code: "x"})
			require.Error(t, err)
			assert.Equal(t, events, tableCount(t, pool, "handoff_events"))
			assert.Equal(t, outbox, tableCount(t, pool, "outbox"))
			var owner string
			require.NoError(t, pool.QueryRow(t.Context(), `SELECT lease_owner FROM handoffs WHERE id=$1`, lease.Token.HandoffID).Scan(&owner))
			assert.Equal(t, lease.Token.WorkerID, owner)
		})
	}
}

func TestLeaseEveryMutationStatementBoundaryRollsBack(t *testing.T) {
	type mutationCase struct {
		name string
		run  func(*Repository, worker.LeaseToken, time.Time) error
	}
	cases := []mutationCase{
		{"progress", func(repo *Repository, token worker.LeaseToken, _ time.Time) error {
			return repo.AppendProgress(t.Context(), token, worker.ProgressRecord{MutationID: "mutation", Sequence: 1, Summary: "working"})
		}},
		{"needs-input", func(repo *Repository, token worker.LeaseToken, _ time.Time) error {
			return repo.MarkNeedsInput(t.Context(), token, worker.NeedsInputRecord{MutationID: "mutation", Prompt: "question", Schema: json.RawMessage(`{}`)})
		}},
		{"complete", func(repo *Repository, token worker.LeaseToken, _ time.Time) error {
			return repo.Complete(t.Context(), token, worker.CompletionRecord{Result: handoff.Result{MutationID: "mutation", Summary: "done", Structured: json.RawMessage(`{}`), SafetyDecision: json.RawMessage(`{"action":"allow"}`), Usage: json.RawMessage(`{}`)}})
		}},
		{"fail", func(repo *Repository, token worker.LeaseToken, _ time.Time) error {
			return repo.Fail(t.Context(), token, worker.FailureRecord{Failure: handoff.Failure{MutationID: "mutation", Code: "provider", Retryable: false, Message: "failed"}})
		}},
		{"cancel", func(repo *Repository, token worker.LeaseToken, _ time.Time) error {
			return repo.MarkCancelled(t.Context(), token, worker.CancelledRecord{MutationID: "mutation", Reason: "stop"})
		}},
	}
	for _, tc := range cases {
		for _, suffix := range []string{"update", "event", "wakeup"} {
			step := "worker_" + tc.name + "_" + suffix
			t.Run(step, func(t *testing.T) {
				repo, pool, _ := repositoryHarness(t)
				_, lease, now := runningLease(t, repo, "all-rollback-"+tc.name+"-"+suffix)
				events, outbox := tableCount(t, pool, "handoff_events"), tableCount(t, pool, "outbox")
				repo.failpoint = func(got string) error {
					if got == step {
						return errors.New("injected")
					}
					return nil
				}
				require.Error(t, tc.run(repo, lease.Token, now))
				assert.Equal(t, events, tableCount(t, pool, "handoff_events"))
				assert.Equal(t, outbox, tableCount(t, pool, "outbox"))
				var state, owner string
				require.NoError(t, pool.QueryRow(t.Context(), `SELECT state,lease_owner FROM handoffs WHERE id=$1`, lease.Token.HandoffID).Scan(&state, &owner))
				assert.Equal(t, "running", state)
				assert.Equal(t, lease.Token.WorkerID, owner)
			})
		}
	}
}

func TestLeaseAcquisitionRunningAndSweepStatementBoundariesRollBack(t *testing.T) {
	for _, step := range []string{"lease_update", "lease_event", "lease_wakeup"} {
		t.Run(step, func(t *testing.T) {
			repo, pool, _ := repositoryHarness(t)
			record := createRunnableHandoff(t, repo, "lease-rollback-"+step)
			events, outbox := tableCount(t, pool, "handoff_events"), tableCount(t, pool, "outbox")
			repo.failpoint = func(got string) error {
				if got == step {
					return errors.New("injected")
				}
				return nil
			}
			_, _, err := repo.LeaseNext(t.Context(), worker.LeaseRequest{WorkerID: "worker", LeaseDuration: time.Minute, Now: repositoryTestNow.Add(time.Minute)})
			require.Error(t, err)
			assert.Equal(t, events, tableCount(t, pool, "handoff_events"))
			assert.Equal(t, outbox, tableCount(t, pool, "outbox"))
			var state string
			var owner *string
			require.NoError(t, pool.QueryRow(t.Context(), `SELECT state,lease_owner FROM handoffs WHERE id=$1`, record.HandoffID).Scan(&state, &owner))
			assert.Equal(t, "pending", state)
			assert.Nil(t, owner)
		})
	}
	for _, step := range []string{"running_update", "running_event", "running_wakeup"} {
		t.Run(step, func(t *testing.T) {
			repo, pool, _ := repositoryHarness(t)
			createRunnableHandoff(t, repo, "running-rollback-"+step)
			now := repositoryTestNow.Add(time.Minute)
			lease, ok, err := repo.LeaseNext(t.Context(), worker.LeaseRequest{WorkerID: "worker", LeaseDuration: time.Minute, Now: now})
			require.NoError(t, err)
			require.True(t, ok)
			events, outbox := tableCount(t, pool, "handoff_events"), tableCount(t, pool, "outbox")
			repo.failpoint = func(got string) error {
				if got == step {
					return errors.New("injected")
				}
				return nil
			}
			require.Error(t, repo.MarkRunning(t.Context(), lease.Token, now))
			assert.Equal(t, events, tableCount(t, pool, "handoff_events"))
			assert.Equal(t, outbox, tableCount(t, pool, "outbox"))
			var state string
			require.NoError(t, pool.QueryRow(t.Context(), `SELECT state FROM handoffs WHERE id=$1`, lease.Token.HandoffID).Scan(&state))
			assert.Equal(t, "leased", state)
		})
	}
	for _, step := range []string{"sweep_update", "sweep_event", "sweep_wakeup"} {
		t.Run(step, func(t *testing.T) {
			repo, pool, _ := repositoryHarness(t)
			record := createRunnableHandoff(t, repo, "sweep-rollback-"+step)
			_, err := repo.RequestHandoffCancel(t.Context(), record.Request.Scope, record.HandoffID, handoff.CancelRequest{Actor: "user", IdempotencyKey: "cancel"})
			require.NoError(t, err)
			events, outbox := tableCount(t, pool, "handoff_events"), tableCount(t, pool, "outbox")
			repo.failpoint = func(got string) error {
				if got == step {
					return errors.New("injected")
				}
				return nil
			}
			_, err = repo.SweepExpired(t.Context(), repositoryTestNow.Add(time.Minute), 1)
			require.Error(t, err)
			assert.Equal(t, events, tableCount(t, pool, "handoff_events"))
			assert.Equal(t, outbox, tableCount(t, pool, "outbox"))
			var state string
			require.NoError(t, pool.QueryRow(t.Context(), `SELECT state FROM handoffs WHERE id=$1`, record.HandoffID).Scan(&state))
			assert.Equal(t, "pending", state)
		})
	}
}

// Repository acquisition is one lease per call and does not silently release
// a successfully returned lease. Runner concurrency/shutdown belongs to Task 14.
func TestRepositoryLeavesCallerAcquiredLeasesHeld(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	for i := 0; i < 4; i++ {
		createRunnableHandoff(t, repo, fmt.Sprintf("bound-%d", i))
	}
	const acquisitions = 2
	var wg sync.WaitGroup
	for i := 0; i < acquisitions; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, ok, err := repo.LeaseNext(context.Background(), worker.LeaseRequest{WorkerID: fmt.Sprintf("w-%d", i), LeaseDuration: time.Minute, Now: repositoryTestNow.Add(time.Minute)})
			assert.NoError(t, err)
			assert.True(t, ok)
		}(i)
	}
	wg.Wait()
	var active int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM handoffs WHERE lease_owner IS NOT NULL`).Scan(&active))
	assert.Equal(t, acquisitions, active)
}

func TestLeaseTerminalCommitResponseLossReplaysExactly(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	_, lease, _ := runningLease(t, repo, "response-loss")
	result := handoff.Result{MutationID: "terminal", Summary: "done", Structured: json.RawMessage(`{"ok":true}`), SafetyDecision: json.RawMessage(`{"action":"allow"}`), Usage: json.RawMessage(`{}`)}
	repo.failpoint = func(step string) error {
		if step == "worker_complete_commit" {
			return errors.New("response lost")
		}
		return nil
	}
	completion := worker.CompletionRecord{ExpectedObjectiveRevision: lease.Job.ObjectiveRevision, Result: result}
	err := repo.Complete(t.Context(), lease.Token, completion)
	require.EqualError(t, err, "response lost")
	repo.failpoint = nil
	require.NoError(t, repo.Complete(t.Context(), lease.Token, completion))
	var terminals int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM handoff_events WHERE handoff_id=$1 AND kind='handoff.complete'`, lease.Token.HandoffID).Scan(&terminals))
	assert.Equal(t, 1, terminals)
}

func TestLeaseNonterminalAndTerminalCommitResponseLossReplayExactMutation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		action    string
		wantState handoff.State
		mutate    func(*Repository, worker.Lease, time.Time) error
	}{
		{"needs input", "needs-input", handoff.StateNeedsInput, func(repo *Repository, lease worker.Lease, _ time.Time) error {
			return repo.MarkNeedsInput(t.Context(), lease.Token, worker.NeedsInputRecord{MutationID: "needs-input-ack", ExpectedObjectiveRevision: lease.Job.ObjectiveRevision, Prompt: "question", Schema: json.RawMessage(`{"type":"object"}`)})
		}},
		{"retry", "retry", handoff.StateRunning, func(repo *Repository, lease worker.Lease, now time.Time) error {
			return repo.ScheduleRetry(t.Context(), lease.Token, worker.RetryRecord{MutationID: "retry-ack", ExpectedObjectiveRevision: lease.Job.ObjectiveRevision, ReadyAt: now.Add(time.Minute), RetryCount: 1, Code: "provider"})
		}},
		{"fail", "fail", handoff.StateFailed, func(repo *Repository, lease worker.Lease, _ time.Time) error {
			return repo.Fail(t.Context(), lease.Token, worker.FailureRecord{ExpectedObjectiveRevision: lease.Job.ObjectiveRevision, Failure: handoff.Failure{MutationID: "fail-ack", Code: "provider", Message: "failed"}})
		}},
		{"cancel", "cancel", handoff.StateCancelled, func(repo *Repository, lease worker.Lease, _ time.Time) error {
			return repo.MarkCancelled(t.Context(), lease.Token, worker.CancelledRecord{MutationID: "cancel-ack", ExpectedObjectiveRevision: lease.Job.ObjectiveRevision, Reason: "stop"})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo, pool, _ := repositoryHarness(t)
			record, lease, now := runningLease(t, repo, "ack-"+tc.action)
			repo.failpoint = func(step string) error {
				if step == "worker_"+tc.action+"_commit" {
					return errors.New("response lost")
				}
				return nil
			}
			err := tc.mutate(repo, lease, now)
			require.EqualError(t, err, "response lost")
			repo.failpoint = nil
			require.NoError(t, tc.mutate(repo, lease, now))
			got, err := repo.GetHandoff(t.Context(), record.Request.Scope, record.HandoffID)
			require.NoError(t, err)
			assert.Equal(t, tc.wantState, got.State)
			var events int
			require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM handoff_events WHERE handoff_id=$1 AND kind=$2`, record.HandoffID, "handoff."+tc.action).Scan(&events))
			assert.Equal(t, 1, events)
		})
	}
}

func TestSteeringVersusTerminalSerializes(t *testing.T) {
	for _, terminalFirst := range []bool{true, false} {
		name := "steering first"
		if terminalFirst {
			name = "terminal first"
		}
		t.Run(name, func(t *testing.T) {
			base, pool, repositoryClock := repositoryHarness(t)
			record, lease, _ := runningLease(t, base, fmt.Sprintf("steer-terminal-%t", terminalFirst))
			steerRepo, err := NewRepository(pool, repositoryClock)
			require.NoError(t, err)
			terminalRepo, err := NewRepository(pool, repositoryClock)
			require.NoError(t, err)
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			barrier := func(step string) error {
				if step == "event_lock" {
					once.Do(func() { close(entered) })
					select {
					case <-release:
						return nil
					case <-t.Context().Done():
						return t.Context().Err()
					}
				}
				return nil
			}
			firstDone, secondDone := make(chan error, 1), make(chan error, 1)
			steer := func() error {
				_, err := steerRepo.SteerHandoff(t.Context(), record.Request.Scope, record.HandoffID, handoff.SteerRequest{Objective: "revised", ExpectedRevision: 1, Actor: "user", IdempotencyKey: "steer"})
				return err
			}
			terminal := func() error {
				return terminalRepo.Complete(t.Context(), lease.Token, worker.CompletionRecord{ExpectedObjectiveRevision: lease.Job.ObjectiveRevision, Result: handoff.Result{MutationID: "done", Summary: "done", Structured: json.RawMessage(`{}`), SafetyDecision: json.RawMessage(`{"action":"allow"}`), Usage: json.RawMessage(`{}`)}})
			}
			if terminalFirst {
				terminalRepo.failpoint = barrier
				go func() { firstDone <- terminal() }()
				<-entered
				go func() { secondDone <- steer() }()
			} else {
				steerRepo.failpoint = barrier
				go func() { firstDone <- steer() }()
				<-entered
				go func() { secondDone <- terminal() }()
			}
			waitForBlockedAdvisoryLock(t, pool, "pg_advisory_xact_lock")
			close(release)
			require.NoError(t, <-firstDone)
			secondErr := <-secondDone
			if terminalFirst {
				assert.ErrorIs(t, secondErr, handoff.ErrTerminalState)
			} else {
				assert.ErrorIs(t, secondErr, worker.ErrLeaseLost)
			}
			got, err := base.GetHandoff(t.Context(), record.Request.Scope, record.HandoffID)
			require.NoError(t, err)
			if terminalFirst {
				assert.Equal(t, handoff.StateSucceeded, got.State)
			} else {
				assert.Equal(t, handoff.StateRunning, got.State)
			}
			var terminals int
			require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM handoff_events
				WHERE tenant_id=$1 AND conversation_id=$2 AND handoff_id=$3
				  AND kind IN ('handoff.complete','handoff.failed','handoff.cancelled')`, lease.Token.TenantID, lease.Token.ConversationID, lease.Token.HandoffID).Scan(&terminals))
			if terminalFirst {
				assert.Equal(t, 1, terminals)
			} else {
				assert.Equal(t, 0, terminals)
			}
		})
	}
}

func TestLeaseSameTextIDAcrossScopesHasIndependentLifecycle(t *testing.T) {
	_, pool, manual := repositoryHarness(t)
	repo, err := NewRepository(pool, manual, WithToolGate(testToolGate{"search": true}))
	require.NoError(t, err)
	create := func(scope handoff.Scope, suffix string) handoff.CreateRecord {
		insertConversation(t, repo.pool, scope)
		record := prepareCreate(t, repo, scope, suffix)
		record.HandoffID = "shared-handoff"
		record.EventID = "create-" + suffix
		record.Request.ApprovalPolicy = json.RawMessage(`{"mode":"none"}`)
		record.RequestHash, _ = handoff.CanonicalRequestHash(record.Request)
		_, _, err := repo.CreateHandoff(t.Context(), record)
		require.NoError(t, err)
		return record
	}
	a := create(testScope("tenant-shared-a", "conversation-a"), "shared-a")
	a2 := create(testScope("tenant-shared-a", "conversation-c"), "shared-c")
	b := create(testScope("tenant-shared-b", "conversation-b"), "shared-b")
	var invocationIDs []string
	for i, want := range []handoff.CreateRecord{a, a2, b} {
		lease, ok, err := repo.LeaseNext(t.Context(), worker.LeaseRequest{WorkerID: fmt.Sprintf("worker-%d", i), LeaseDuration: time.Minute, Now: repositoryTestNow.Add(time.Minute)})
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, want.Request.Scope.TenantID, lease.Token.TenantID)
		assert.Equal(t, want.Request.Scope.ConversationID, lease.Token.ConversationID)
		require.NoError(t, repo.MarkRunning(t.Context(), lease.Token, repositoryTestNow.Add(time.Minute)))
		hash := fmt.Sprintf("%064x", 99)
		invocationID := worker.StableToolInvocationID(lease.Token.TenantID, lease.Token.ConversationID, lease.Token.HandoffID, 0, 1, "search", hash)
		invocationIDs = append(invocationIDs, invocationID)
		_, _, err = repo.BeginToolInvocation(t.Context(), lease.Token, worker.ToolInvocationRecord{InvocationID: invocationID, ObjectiveRevision: 0, ToolOrdinal: 1, ToolName: "search", IdempotencyKey: invocationID, ArgumentsHash: hash, Ciphertext: make([]byte, 16), Nonce: make([]byte, 12), KeyID: "key"})
		require.NoError(t, err)
		require.NoError(t, repo.CompleteToolInvocation(t.Context(), lease.Token, worker.ToolCompletionRecord{InvocationID: invocationID, MutationID: fmt.Sprintf("tool-done-%d", i), Result: json.RawMessage(`{}`)}))
		require.NoError(t, repo.Complete(t.Context(), lease.Token, worker.CompletionRecord{Result: handoff.Result{MutationID: fmt.Sprintf("done-%d", i), Summary: "done", Structured: json.RawMessage(`{}`), SafetyDecision: json.RawMessage(`{"action":"allow"}`), Usage: json.RawMessage(`{}`)}}))
	}
	assert.Len(t, map[string]struct{}{invocationIDs[0]: {}, invocationIDs[1]: {}, invocationIDs[2]: {}}, 3)
}

func TestHeartbeatOutOfOrderNeverShortensLease(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	_, lease, now := runningLease(t, repo, "heartbeat-monotonic")
	require.NoError(t, heartbeatOnly(repo.Heartbeat(t.Context(), lease.Token, now.Add(10*time.Minute))))
	require.NoError(t, heartbeatOnly(repo.Heartbeat(t.Context(), lease.Token, now.Add(5*time.Minute))))
	var expires time.Time
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT lease_expires_at FROM handoffs WHERE tenant_id=$1 AND conversation_id=$2 AND id=$3`, lease.Token.TenantID, lease.Token.ConversationID, lease.Token.HandoffID).Scan(&expires))
	assert.Equal(t, now.Add(10*time.Minute), expires.UTC())
}

func heartbeatOnly(_ worker.Control, err error) error { return err }

func TestWorkerStoreMethodsRejectNilAndCancelledContexts(t *testing.T) {
	repo, _, _ := repositoryHarness(t)
	token := worker.LeaseToken{TenantID: "tenant", ConversationID: "conversation", HandoffID: "handoff", WorkerID: "worker", Version: 1, Attempt: 1}
	now := repositoryTestNow.Add(time.Minute)
	methods := map[string]func(context.Context) error{
		"LeaseNext": func(ctx context.Context) error {
			_, _, err := repo.LeaseNext(ctx, worker.LeaseRequest{WorkerID: "worker", LeaseDuration: time.Minute, Now: now})
			return err
		},
		"MarkRunning": func(ctx context.Context) error { return repo.MarkRunning(ctx, token, now) },
		"Heartbeat": func(ctx context.Context) error {
			_, err := repo.Heartbeat(ctx, token, now.Add(time.Minute))
			return err
		},
		"ScheduleRetry": func(ctx context.Context) error {
			return repo.ScheduleRetry(ctx, token, worker.RetryRecord{MutationID: "m", ReadyAt: now.Add(time.Minute), RetryCount: 1, Code: "x"})
		},
		"AppendProgress": func(ctx context.Context) error {
			return repo.AppendProgress(ctx, token, worker.ProgressRecord{MutationID: "m", Sequence: 1, Summary: "x"})
		},
		"MarkNeedsInput": func(ctx context.Context) error {
			return repo.MarkNeedsInput(ctx, token, worker.NeedsInputRecord{MutationID: "m", Prompt: "x", Schema: json.RawMessage(`{}`)})
		},
		"Complete": func(ctx context.Context) error {
			return repo.Complete(ctx, token, worker.CompletionRecord{Result: handoff.Result{MutationID: "m", Summary: "x", Structured: json.RawMessage(`{}`), SafetyDecision: json.RawMessage(`{"action":"allow"}`), Usage: json.RawMessage(`{}`)}})
		},
		"Fail": func(ctx context.Context) error {
			return repo.Fail(ctx, token, worker.FailureRecord{Failure: handoff.Failure{MutationID: "m", Code: "x", Message: "x"}})
		},
		"MarkCancelled": func(ctx context.Context) error {
			return repo.MarkCancelled(ctx, token, worker.CancelledRecord{MutationID: "m"})
		},
		"BeginToolInvocation": func(ctx context.Context) error {
			_, _, err := repo.BeginToolInvocation(ctx, token, worker.ToolInvocationRecord{})
			return err
		},
		"CompleteToolInvocation": func(ctx context.Context) error {
			return repo.CompleteToolInvocation(ctx, token, worker.ToolCompletionRecord{InvocationID: "invocation", MutationID: "m", Result: json.RawMessage(`{}`)})
		},
		"SweepExpired": func(ctx context.Context) error { _, err := repo.SweepExpired(ctx, now, 1); return err },
	}
	for name, method := range methods {
		t.Run(name+"/nil", func(t *testing.T) { assert.Error(t, method(nil)) })
		t.Run(name+"/cancelled", func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			assert.ErrorIs(t, method(ctx), context.Canceled)
		})
	}
}
