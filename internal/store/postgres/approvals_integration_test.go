//go:build integration

package postgres

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/worker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createPendingApproval(t *testing.T, suffix string, ttl time.Duration) (*Repository, handoff.Scope, worker.ToolInvocationRecord) {
	t.Helper()
	_, pool, manual := repositoryHarness(t)
	repo, err := NewRepository(pool, manual, WithToolGate(testToolGate{"search": true}), WithApprovalTTL(ttl))
	require.NoError(t, err)
	created := createRunnableHandoff(t, repo, suffix)
	_, err = pool.Exec(t.Context(), `UPDATE handoffs SET approval_policy='{"mode":"ask"}'::json WHERE tenant_id=$1 AND conversation_id=$2 AND id=$3`, created.Request.Scope.TenantID, created.Request.Scope.ConversationID, created.HandoffID)
	require.NoError(t, err)
	lease, ok, err := repo.LeaseNext(t.Context(), worker.LeaseRequest{WorkerID: "approval-worker", LeaseDuration: time.Minute, Now: repositoryTestNow})
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, repo.MarkRunning(t.Context(), lease.Token, repositoryTestNow))
	record := testToolRecord(lease.Token, 0, 1, "search")
	_, createdInvocation, err := repo.BeginToolInvocation(t.Context(), lease.Token, record)
	require.ErrorIs(t, err, worker.ErrApprovalRequired)
	require.True(t, createdInvocation)
	var state string
	var owner *string
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT state,lease_owner FROM handoffs WHERE tenant_id=$1 AND conversation_id=$2 AND id=$3`, lease.Token.TenantID, lease.Token.ConversationID, lease.Token.HandoffID).Scan(&state, &owner))
	assert.Equal(t, "needs_input", state)
	assert.Nil(t, owner)
	return repo, created.Request.Scope, record
}

func TestApprovalResolveReconcilesAmbiguousCommitWithoutASecondTransition(t *testing.T) {
	repo, scope, record := createPendingApproval(t, "resolve-ambiguous-commit", 5*time.Minute)
	repo.failpoint = func(step string) error {
		if step == "approval_commit" {
			return errors.New("ambiguous commit")
		}
		return nil
	}
	resolution := handoff.ApprovalResolution{
		InvocationID: record.InvocationID, Decision: handoff.ApprovalApproved,
		ActorTenantID: scope.TenantID, ActorUserID: scope.UserID, Reason: "confirmed",
		IdempotencyKey: "approval-ambiguous-commit", ResolvedAt: repositoryTestNow.Add(time.Minute),
	}
	require.NoError(t, repo.ResolveApproval(t.Context(), scope, resolution))
	repo.failpoint = nil
	require.NoError(t, repo.ResolveApproval(t.Context(), scope, resolution))
	var events int
	require.NoError(t, repo.pool.QueryRow(t.Context(), `SELECT count(*) FROM handoff_events
		WHERE tenant_id=$1 AND conversation_id=$2 AND kind='tool.approval_approved'`, scope.TenantID, scope.ConversationID).Scan(&events))
	assert.Equal(t, 1, events)
}

func TestApprovalResolveApproveIsScopedImmutableAndWakesExactlyOnce(t *testing.T) {
	repo, scope, record := createPendingApproval(t, "resolve-approve", 5*time.Minute)
	recovery, err := repo.LoadRecovery(t.Context(), scope, 8, 1024)
	require.NoError(t, err)
	require.Len(t, recovery.PendingApprovals, 1)
	assert.Equal(t, record.InvocationID, recovery.PendingApprovals[0].Approval.InvocationID)
	resolution := handoff.ApprovalResolution{InvocationID: record.InvocationID, Decision: handoff.ApprovalApproved, ActorTenantID: scope.TenantID, ActorUserID: scope.UserID, Reason: "confirmed", IdempotencyKey: "approval-decision-1", ResolvedAt: repositoryTestNow.Add(time.Minute)}
	require.NoError(t, repo.ResolveApproval(t.Context(), scope, resolution))
	require.NoError(t, repo.ResolveApproval(t.Context(), scope, resolution))

	changed := resolution
	changed.Reason = "changed"
	assert.ErrorIs(t, repo.ResolveApproval(t.Context(), scope, changed), handoff.ErrIdempotencyConflict)
	changed = resolution
	changed.IdempotencyKey = "other-key"
	changed.Decision = handoff.ApprovalDenied
	assert.ErrorIs(t, repo.ResolveApproval(t.Context(), scope, changed), handoff.ErrIdempotencyConflict)
	wrong := scope
	wrong.TenantID = "other-tenant"
	assert.ErrorIs(t, repo.ResolveApproval(t.Context(), wrong, resolution), handoff.ErrNotFound)

	approval, err := repo.LoadApproval(t.Context(), scope, record.InvocationID)
	require.NoError(t, err)
	assert.Equal(t, "approved", approval.ApprovalState)
	assert.Equal(t, handoff.ApprovalRiskSideEffecting, approval.RiskClass)
	assert.NotContains(t, approval.ActionSummary, "{")
	lease, ok, err := repo.LeaseNext(t.Context(), worker.LeaseRequest{WorkerID: "approval-worker-2", LeaseDuration: time.Minute, Now: repositoryTestNow.Add(time.Minute)})
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, record.InvocationID, worker.StableToolInvocationID(lease.Token.TenantID, lease.Token.ConversationID, lease.Token.HandoffID, record.ObjectiveRevision, record.ToolOrdinal, record.ToolName, record.ArgumentsHash))
}

func TestApprovedInvocationClaimDistinguishesFreshUncertainAndSucceededReplay(t *testing.T) {
	repo, scope, record := createPendingApproval(t, "claim-dispositions", 5*time.Minute)
	resolvedAt := repositoryTestNow.Add(time.Minute)
	require.NoError(t, repo.ResolveApproval(t.Context(), scope, handoff.ApprovalResolution{
		InvocationID: record.InvocationID, Decision: handoff.ApprovalApproved,
		ActorTenantID: scope.TenantID, ActorUserID: scope.UserID, Reason: "approved",
		IdempotencyKey: "claim-dispositions", ResolvedAt: resolvedAt,
	}))
	second, ok, err := repo.LeaseNext(t.Context(), worker.LeaseRequest{WorkerID: "claim-fresh", LeaseDuration: time.Minute, Now: resolvedAt})
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, repo.MarkRunning(t.Context(), second.Token, resolvedAt))
	fresh, found, err := repo.ClaimApprovedToolInvocation(t.Context(), second.Token, record.ObjectiveRevision)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, worker.ApprovalResumeFresh, fresh.ResumeDisposition)
	assert.Equal(t, "running", fresh.Status)
	assert.Equal(t, record, fresh.Record)

	_, err = repo.pool.Exec(t.Context(), `UPDATE handoffs SET lease_expires_at=$1 WHERE tenant_id=$2 AND conversation_id=$3 AND id=$4`,
		resolvedAt.Add(-time.Second), second.Token.TenantID, second.Token.ConversationID, second.Token.HandoffID)
	require.NoError(t, err)
	third, ok, err := repo.LeaseNext(t.Context(), worker.LeaseRequest{WorkerID: "claim-uncertain", LeaseDuration: time.Minute, Now: resolvedAt})
	require.NoError(t, err)
	require.True(t, ok)
	uncertain, found, err := repo.ClaimApprovedToolInvocation(t.Context(), third.Token, record.ObjectiveRevision)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, worker.ApprovalResumeUncertain, uncertain.ResumeDisposition)
	assert.Equal(t, record.InvocationID, uncertain.Record.InvocationID)

	require.NoError(t, repo.CompleteToolInvocation(t.Context(), third.Token, worker.ToolCompletionRecord{
		InvocationID: record.InvocationID, MutationID: "claim-dispositions-complete", Result: json.RawMessage(`{"ok":true}`),
	}))
	succeeded, found, err := repo.ClaimApprovedToolInvocation(t.Context(), third.Token, record.ObjectiveRevision)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, worker.ApprovalResumeSucceeded, succeeded.ResumeDisposition)
	assert.Equal(t, "succeeded", succeeded.Status)
	assert.JSONEq(t, `{"ok":true}`, string(succeeded.Result))
}

func TestApprovalResolveRejectsUnknownWrongUserAndOversizedFields(t *testing.T) {
	repo, scope, record := createPendingApproval(t, "resolve-negative-scope", 5*time.Minute)
	base := handoff.ApprovalResolution{InvocationID: record.InvocationID, Decision: handoff.ApprovalApproved,
		ActorTenantID: scope.TenantID, ActorUserID: scope.UserID, Reason: "confirmed",
		IdempotencyKey: "approval-negative", ResolvedAt: repositoryTestNow.Add(time.Minute)}

	wrongUserScope := scope
	wrongUserScope.UserID = "other-user"
	wrongUser := base
	wrongUser.ActorUserID = wrongUserScope.UserID
	assert.ErrorIs(t, repo.ResolveApproval(t.Context(), wrongUserScope, wrongUser), handoff.ErrNotFound)
	unknown := base
	unknown.InvocationID = "unknown-invocation"
	unknown.IdempotencyKey = "approval-unknown"
	assert.ErrorIs(t, repo.ResolveApproval(t.Context(), scope, unknown), handoff.ErrNotFound)
	for _, mutate := range []func(*handoff.ApprovalResolution){
		func(value *handoff.ApprovalResolution) { value.InvocationID = strings.Repeat("i", 257) },
		func(value *handoff.ApprovalResolution) { value.IdempotencyKey = strings.Repeat("k", 257) },
		func(value *handoff.ApprovalResolution) {
			value.Reason = strings.Repeat("r", handoff.MaximumApprovalReasonBytes+1)
		},
	} {
		invalid := base
		mutate(&invalid)
		assert.ErrorIs(t, repo.ResolveApproval(t.Context(), scope, invalid), handoff.ErrInvalidRequest)
	}
	approval, err := repo.LoadApproval(t.Context(), scope, record.InvocationID)
	require.NoError(t, err)
	assert.Equal(t, "pending", approval.ApprovalState)
}

func TestApprovalResolutionSerializesWithCancelSteerAndDelete(t *testing.T) {
	for _, action := range []string{"cancel", "steer", "delete"} {
		t.Run(action, func(t *testing.T) {
			repo, scope, record := createPendingApproval(t, "resolve-race-"+action, 5*time.Minute)
			pending, err := repo.LoadApproval(t.Context(), scope, record.InvocationID)
			require.NoError(t, err)
			actionRepo, err := NewRepository(repo.pool, repo.clock, WithToolGate(testToolGate{"search": true}))
			require.NoError(t, err)
			locked, release := make(chan struct{}), make(chan struct{})
			repo.failpoint = func(step string) error {
				if step == "approval_after_invocation_lock" {
					close(locked)
					<-release
				}
				return nil
			}
			resolutionDone := make(chan error, 1)
			go func() {
				resolutionDone <- repo.ResolveApproval(t.Context(), scope, handoff.ApprovalResolution{
					InvocationID: record.InvocationID, Decision: handoff.ApprovalApproved,
					ActorTenantID: scope.TenantID, ActorUserID: scope.UserID, Reason: "confirmed",
					IdempotencyKey: "approval-race-" + action, ResolvedAt: repositoryTestNow.Add(time.Minute),
				})
			}()
			<-locked
			actionDone := make(chan error, 1)
			go func() {
				switch action {
				case "cancel":
					_, err := actionRepo.RequestHandoffCancel(t.Context(), scope, pending.HandoffID, handoff.CancelRequest{Actor: scope.UserID, Reason: "stop", IdempotencyKey: "cancel-race"})
					actionDone <- err
				case "steer":
					_, err := actionRepo.SteerHandoff(t.Context(), scope, pending.HandoffID, handoff.SteerRequest{Actor: scope.UserID, Objective: "updated objective", ExpectedRevision: 1, IdempotencyKey: "steer-race"})
					actionDone <- err
				case "delete":
					actionDone <- actionRepo.MarkConversationDeleting(t.Context(), scope, scope.UserID, handoff.DeletionReasonUserRequested)
				}
			}()
			select {
			case err := <-actionDone:
				t.Fatalf("%s bypassed approval lock: %v", action, err)
			case <-time.After(50 * time.Millisecond):
			}
			close(release)
			require.NoError(t, <-resolutionDone)
			require.NoError(t, <-actionDone)
			var approvalEvents int
			require.NoError(t, repo.pool.QueryRow(t.Context(), `SELECT count(*) FROM handoff_events WHERE tenant_id=$1 AND conversation_id=$2 AND kind='tool.approval_approved'`, scope.TenantID, scope.ConversationID).Scan(&approvalEvents))
			assert.Equal(t, 1, approvalEvents)
		})
	}
}

func TestApprovalDenyExpiryAndSweeperAreTerminal(t *testing.T) {
	for _, tc := range []struct {
		name, decision, want string
		at                   time.Duration
	}{
		{name: "denied", decision: "denied", want: "denied", at: time.Minute},
		{name: "expiry boundary", decision: "approved", want: "expired", at: 2 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo, scope, record := createPendingApproval(t, "terminal-"+strings.ReplaceAll(tc.name, " ", "-"), 2*time.Minute)
			err := repo.ResolveApproval(t.Context(), scope, handoff.ApprovalResolution{InvocationID: record.InvocationID, Decision: handoff.ApprovalDecision(tc.decision), ActorTenantID: scope.TenantID, ActorUserID: scope.UserID, Reason: "bounded", IdempotencyKey: "terminal-decision", ResolvedAt: repositoryTestNow.Add(tc.at)})
			require.NoError(t, err)
			approval, err := repo.LoadApproval(t.Context(), scope, record.InvocationID)
			require.NoError(t, err)
			assert.Equal(t, tc.want, approval.ApprovalState)
			var state, failure string
			require.NoError(t, repo.pool.QueryRow(t.Context(), `SELECT state,failure_code FROM handoffs WHERE tenant_id=$1 AND conversation_id=$2`, scope.TenantID, scope.ConversationID).Scan(&state, &failure))
			assert.Equal(t, "failed", state)
			assert.Equal(t, "approval_"+tc.want, failure)
			_, leased, leaseErr := repo.LeaseNext(t.Context(), worker.LeaseRequest{WorkerID: "must-not-execute", LeaseDuration: time.Minute, Now: repositoryTestNow.Add(3 * time.Minute)})
			require.NoError(t, leaseErr)
			assert.False(t, leased, "denied or expired approval must have zero executor path")
		})
	}

	repo, scope, record := createPendingApproval(t, "expiry-sweep", time.Minute)
	count, err := repo.ExpireApprovals(t.Context(), repositoryTestNow.Add(time.Minute), 10)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
	approval, err := repo.LoadApproval(t.Context(), scope, record.InvocationID)
	require.NoError(t, err)
	assert.Equal(t, "expired", approval.ApprovalState)
}
