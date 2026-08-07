//go:build integration

package postgres

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/worker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testToolGate map[string]bool

func (g testToolGate) Metadata(_ context.Context, version, name string) (worker.ToolGateMetadata, bool) {
	sideEffecting, ok := g[name]
	return worker.ToolGateMetadata{SideEffecting: sideEffecting}, version == "tools-v3" && ok
}

type observingToolGate struct{ seen chan string }

func (g observingToolGate) Metadata(_ context.Context, version, name string) (worker.ToolGateMetadata, bool) {
	g.seen <- version
	return worker.ToolGateMetadata{}, version == "tools-v3" && name == "search"
}

// Replacing length-delimited hashing with concatenation changes this fixed
// vector and reintroduces ambiguous stable identities.
func TestToolInvocationStableIDVector(t *testing.T) {
	got := worker.StableToolInvocationID("tenant-1", "conversation-1", "handoff-1", 7, 3, "search", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	assert.Equal(t, "988e17850f0f5237994ab8a1b972af8a8ed8c22020656cc1c839c06eaba18099", got)
}

// Removing allowlist, process-registry, or durable approval checks permits a
// side effect that the immutable handoff did not authorize.
func TestToolInvocationRequiresRegistryAllowlistAndDurableApproval(t *testing.T) {
	_, pool, manual := repositoryHarness(t)
	seenVersion := make(chan string, 1)
	repo, err := NewRepository(pool, manual, WithToolGate(observingToolGate{seen: seenVersion}))
	require.NoError(t, err)
	record := createRunnableHandoff(t, repo, "tool")
	_, err = pool.Exec(t.Context(), `UPDATE handoffs SET approval_policy='{"mode":"ask"}'::json WHERE id=$1`, record.HandoffID)
	require.NoError(t, err)
	now := repositoryTestNow.Add(time.Minute)
	lease, ok, err := repo.LeaseNext(t.Context(), worker.LeaseRequest{WorkerID: "worker", LeaseDuration: 5 * time.Minute, Now: now})
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, repo.MarkRunning(t.Context(), lease.Token, now))

	hash := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	id := worker.StableToolInvocationID(record.Request.Scope.TenantID, record.Request.Scope.ConversationID, record.HandoffID, 0, 1, "search", hash)
	invocationRecord := worker.ToolInvocationRecord{InvocationID: id, ObjectiveRevision: 0, ToolOrdinal: 1, ToolName: "search", IdempotencyKey: id, ArgumentsHash: hash, Ciphertext: make([]byte, 32), Nonce: make([]byte, 12), KeyID: "key-1"}
	invocation, created, err := repo.BeginToolInvocation(t.Context(), lease.Token, invocationRecord)
	assert.Equal(t, "tools-v3", <-seenVersion)
	assert.ErrorIs(t, err, worker.ErrApprovalRequired)
	assert.True(t, created)
	assert.Equal(t, "pending", invocation.Status)
	var plaintextColumns int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM information_schema.columns WHERE table_name='tool_invocations' AND column_name IN ('arguments','plaintext')`).Scan(&plaintextColumns))
	assert.Zero(t, plaintextColumns)

	_, err = pool.Exec(t.Context(), `UPDATE tool_invocations SET approval_state='approved',approval_expires_at=$1 WHERE invocation_id=$2`, now.Add(time.Minute), id)
	require.NoError(t, err)
	invocation, created, err = repo.BeginToolInvocation(t.Context(), lease.Token, invocationRecord)
	require.NoError(t, err)
	assert.False(t, created)
	assert.Equal(t, "running", invocation.Status)

	bad := invocationRecord
	bad.ArgumentsHash = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	_, _, err = repo.BeginToolInvocation(t.Context(), lease.Token, bad)
	assert.ErrorIs(t, err, handoff.ErrIdempotencyConflict)
}

func TestToolInvocationCompletionIsLeaseFencedAndReplayable(t *testing.T) {
	_, pool, manual := repositoryHarness(t)
	repo, err := NewRepository(pool, manual, WithToolGate(testToolGate{"search": true}))
	require.NoError(t, err)
	createRunnableHandoff(t, repo, "tool-complete")
	now := repositoryTestNow.Add(time.Minute)
	lease, ok, err := repo.LeaseNext(t.Context(), worker.LeaseRequest{WorkerID: "worker", LeaseDuration: 5 * time.Minute, Now: now})
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, repo.MarkRunning(t.Context(), lease.Token, now))
	hash := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	id := worker.StableToolInvocationID(lease.Token.TenantID, lease.Token.ConversationID, lease.Token.HandoffID, 0, 1, "search", hash)
	_, _, err = repo.BeginToolInvocation(t.Context(), lease.Token, worker.ToolInvocationRecord{InvocationID: id, ObjectiveRevision: 0, ToolOrdinal: 1, ToolName: "search", IdempotencyKey: id, ArgumentsHash: hash, Ciphertext: make([]byte, 32), Nonce: make([]byte, 12), KeyID: "key"})
	require.NoError(t, err)
	completion := worker.ToolCompletionRecord{InvocationID: id, MutationID: "tool-done", Result: json.RawMessage(`{"ok":true}`)}
	require.NoError(t, repo.CompleteToolInvocation(t.Context(), lease.Token, completion))
	require.NoError(t, repo.CompleteToolInvocation(t.Context(), lease.Token, completion))
	stale := lease.Token
	stale.Version++
	err = repo.CompleteToolInvocation(t.Context(), stale, worker.ToolCompletionRecord{InvocationID: id, MutationID: "other", Result: json.RawMessage(`{}`)})
	assert.ErrorIs(t, err, worker.ErrLeaseLost)
}

func TestToolInvocationRegistryAllowlistApprovalDenialAndExpiry(t *testing.T) {
	for _, tc := range []struct {
		name     string
		gate     testToolGate
		approval string
		want     error
	}{
		{"registry", testToolGate{}, "", worker.ErrToolNotRegistered},
		{"allowlist", testToolGate{"other": true}, "", worker.ErrToolNotAllowed},
		{"denied", testToolGate{"search": true}, "denied", worker.ErrApprovalDenied},
		{"expired", testToolGate{"search": true}, "approved", worker.ErrApprovalExpired},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, pool, manual := repositoryHarness(t)
			repo, err := NewRepository(pool, manual, WithToolGate(tc.gate))
			require.NoError(t, err)
			record := createRunnableHandoff(t, repo, "deny-"+tc.name)
			if tc.name != "allowlist" {
				_, err = pool.Exec(t.Context(), `UPDATE handoffs SET approval_policy='{"mode":"ask"}'::json WHERE id=$1`, record.HandoffID)
				require.NoError(t, err)
			}
			lease, ok, err := repo.LeaseNext(t.Context(), worker.LeaseRequest{WorkerID: "worker", LeaseDuration: time.Minute, Now: repositoryTestNow.Add(time.Minute)})
			require.NoError(t, err)
			require.True(t, ok)
			require.NoError(t, repo.MarkRunning(t.Context(), lease.Token, repositoryTestNow.Add(time.Minute)))
			hash := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
			name := "search"
			if tc.name == "allowlist" {
				name = "other"
			}
			id := worker.StableToolInvocationID(record.Request.Scope.TenantID, record.Request.Scope.ConversationID, record.HandoffID, 0, 1, name, hash)
			rec := worker.ToolInvocationRecord{InvocationID: id, ObjectiveRevision: 0, ToolOrdinal: 1, ToolName: name, IdempotencyKey: id, ArgumentsHash: hash, Ciphertext: make([]byte, 16), Nonce: make([]byte, 12), KeyID: "key"}
			if tc.approval != "" {
				_, _, err = repo.BeginToolInvocation(t.Context(), lease.Token, rec)
				require.ErrorIs(t, err, worker.ErrApprovalRequired)
				_, err = pool.Exec(t.Context(), `UPDATE tool_invocations SET approval_state=$1,approval_expires_at=$2 WHERE invocation_id=$3`, tc.approval, repositoryTestNow.Add(-time.Second), id)
				require.NoError(t, err)
			}
			_, _, err = repo.BeginToolInvocation(t.Context(), lease.Token, rec)
			assert.ErrorIs(t, err, tc.want)
		})
	}
}

func TestToolInvocationPersistsCiphertextOnlyAndRejectsIdentityMismatch(t *testing.T) {
	_, pool, manual := repositoryHarness(t)
	repo, err := NewRepository(pool, manual, WithToolGate(testToolGate{"search": true}))
	require.NoError(t, err)
	_, lease, _ := runningLease(t, repo, "encrypted")
	canonicalArguments := []byte(`{"query":"secret-plaintext"}`)
	digest := sha256.Sum256(canonicalArguments)
	hash := hex.EncodeToString(digest[:])
	id := worker.StableToolInvocationID(lease.Token.TenantID, lease.Token.ConversationID, lease.Token.HandoffID, 0, 1, "search", hash)
	key := []byte("0123456789abcdef0123456789abcdef")
	nonce := []byte("123456789012")
	block, err := aes.NewCipher(key)
	require.NoError(t, err)
	var gcm cipher.AEAD
	gcm, err = cipher.NewGCM(block)
	require.NoError(t, err)
	ciphertext := gcm.Seal(nil, nonce, canonicalArguments, nil)
	assert.NotContains(t, string(ciphertext), "secret-plaintext")
	record := worker.ToolInvocationRecord{InvocationID: id, ObjectiveRevision: 0, ToolOrdinal: 1, ToolName: "search", IdempotencyKey: id, ArgumentsHash: hash, Ciphertext: ciphertext, Nonce: nonce, KeyID: "key"}
	_, _, err = repo.BeginToolInvocation(t.Context(), lease.Token, record)
	require.NoError(t, err)
	var storedCipher, storedNonce []byte
	var storedHash string
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT arguments_ciphertext,arguments_nonce,canonical_arguments_hash FROM tool_invocations WHERE invocation_id=$1`, id).Scan(&storedCipher, &storedNonce, &storedHash))
	assert.Equal(t, ciphertext, storedCipher)
	assert.Equal(t, nonce, storedNonce)
	assert.Equal(t, hash, storedHash)
	var toolRepresentation string
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT encode(arguments_ciphertext,'hex')||canonical_arguments_hash||COALESCE(result_metadata::text,'')||COALESCE(result::text,'') FROM tool_invocations WHERE invocation_id=$1`, id).Scan(&toolRepresentation))
	assert.NotContains(t, toolRepresentation, "secret-plaintext")
	var leaked int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT
		(SELECT count(*) FROM handoff_events WHERE payload::text LIKE '%secret-plaintext%')+
		(SELECT count(*) FROM outbox WHERE payload::text LIKE '%secret-plaintext%')`).Scan(&leaked))
	assert.Zero(t, leaked)
	bad := record
	bad.IdempotencyKey = "different"
	_, _, err = repo.BeginToolInvocation(t.Context(), lease.Token, bad)
	assert.ErrorIs(t, err, handoff.ErrIdempotencyConflict)
}

func TestToolInvocationExpiryDuringExecutionRejectsCompletion(t *testing.T) {
	_, pool, manual := repositoryHarness(t)
	repo, err := NewRepository(pool, manual, WithToolGate(testToolGate{"search": true}))
	require.NoError(t, err)
	_, lease, _ := runningLease(t, repo, "tool-expiry")
	hash := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	id := worker.StableToolInvocationID(lease.Token.TenantID, lease.Token.ConversationID, lease.Token.HandoffID, 0, 1, "search", hash)
	_, _, err = repo.BeginToolInvocation(t.Context(), lease.Token, worker.ToolInvocationRecord{InvocationID: id, ObjectiveRevision: 0, ToolOrdinal: 1, ToolName: "search", IdempotencyKey: id, ArgumentsHash: hash, Ciphertext: make([]byte, 16), Nonce: make([]byte, 12), KeyID: "key"})
	require.NoError(t, err)
	manual.Advance(10 * time.Minute)
	err = repo.CompleteToolInvocation(t.Context(), lease.Token, worker.ToolCompletionRecord{InvocationID: id, MutationID: "late", Result: json.RawMessage(`{}`)})
	assert.ErrorIs(t, err, worker.ErrLeaseLost)
	var status string
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT status FROM tool_invocations WHERE invocation_id=$1`, id).Scan(&status))
	assert.Equal(t, "running", status)
}

func TestToolInvocationStatementFailpointsRollback(t *testing.T) {
	_, pool, manual := repositoryHarness(t)
	repo, err := NewRepository(pool, manual, WithToolGate(testToolGate{"search": true}))
	require.NoError(t, err)
	_, lease, _ := runningLease(t, repo, "tool-rollback")
	hash := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	id := worker.StableToolInvocationID(lease.Token.TenantID, lease.Token.ConversationID, lease.Token.HandoffID, 0, 1, "search", hash)
	record := worker.ToolInvocationRecord{InvocationID: id, ObjectiveRevision: 0, ToolOrdinal: 1, ToolName: "search", IdempotencyKey: id, ArgumentsHash: hash, Ciphertext: make([]byte, 16), Nonce: make([]byte, 12), KeyID: "key"}
	repo.failpoint = func(step string) error {
		if step == "tool_invocation_insert" {
			return assert.AnError
		}
		return nil
	}
	_, _, err = repo.BeginToolInvocation(t.Context(), lease.Token, record)
	require.ErrorIs(t, err, assert.AnError)
	assert.Equal(t, int64(0), tableCount(t, pool, "tool_invocations"))
	repo.failpoint = nil
	_, _, err = repo.BeginToolInvocation(t.Context(), lease.Token, record)
	require.NoError(t, err)
	repo.failpoint = func(step string) error {
		if step == "tool_invocation_complete" {
			return assert.AnError
		}
		return nil
	}
	err = repo.CompleteToolInvocation(t.Context(), lease.Token, worker.ToolCompletionRecord{InvocationID: id, MutationID: "done", Result: json.RawMessage(`{}`)})
	require.ErrorIs(t, err, assert.AnError)
	var status string
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT status FROM tool_invocations WHERE invocation_id=$1`, id).Scan(&status))
	assert.Equal(t, "running", status)
}

func TestToolCompletionUsesExplicitInvocationID(t *testing.T) {
	_, pool, manual := repositoryHarness(t)
	repo, err := NewRepository(pool, manual, WithToolGate(testToolGate{"search": true}))
	require.NoError(t, err)
	_, lease, _ := runningLease(t, repo, "explicit-completion")
	makeRecord := func(ordinal uint32) worker.ToolInvocationRecord {
		hash := fmt.Sprintf("%064x", ordinal)
		id := worker.StableToolInvocationID(lease.Token.TenantID, lease.Token.ConversationID, lease.Token.HandoffID, 0, ordinal, "search", hash)
		return worker.ToolInvocationRecord{InvocationID: id, ObjectiveRevision: 0, ToolOrdinal: ordinal, ToolName: "search", IdempotencyKey: id, ArgumentsHash: hash, Ciphertext: make([]byte, 16), Nonce: make([]byte, 12), KeyID: "key"}
	}
	first, second := makeRecord(1), makeRecord(2)
	_, _, err = repo.BeginToolInvocation(t.Context(), lease.Token, first)
	require.NoError(t, err)
	_, _, err = repo.BeginToolInvocation(t.Context(), lease.Token, second)
	require.NoError(t, err)
	require.NoError(t, repo.CompleteToolInvocation(t.Context(), lease.Token, worker.ToolCompletionRecord{InvocationID: second.InvocationID, MutationID: "second-done", Result: json.RawMessage(`{"which":2}`)}))
	require.NoError(t, repo.CompleteToolInvocation(t.Context(), lease.Token, worker.ToolCompletionRecord{InvocationID: first.InvocationID, MutationID: "first-done", Result: json.RawMessage(`{"which":1}`)}))
	var firstResult, secondResult []byte
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT result::text FROM tool_invocations WHERE invocation_id=$1`, first.InvocationID).Scan(&firstResult))
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT result::text FROM tool_invocations WHERE invocation_id=$1`, second.InvocationID).Scan(&secondResult))
	assert.JSONEq(t, `{"which":1}`, string(firstResult))
	assert.JSONEq(t, `{"which":2}`, string(secondResult))
}

func TestToolApprovalPendingRebindsToReacquiredAttempt(t *testing.T) {
	_, pool, manual := repositoryHarness(t)
	repo, err := NewRepository(pool, manual, WithToolGate(testToolGate{"search": true}))
	require.NoError(t, err)
	record := createRunnableHandoff(t, repo, "approval-reacquire")
	_, err = pool.Exec(t.Context(), `UPDATE handoffs SET approval_policy='{"mode":"ask"}'::json WHERE tenant_id=$1 AND conversation_id=$2 AND id=$3`, record.Request.Scope.TenantID, record.Request.Scope.ConversationID, record.HandoffID)
	require.NoError(t, err)
	now := repositoryTestNow.Add(time.Minute)
	first, ok, err := repo.LeaseNext(t.Context(), worker.LeaseRequest{WorkerID: "first", LeaseDuration: time.Minute, Now: now})
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, repo.MarkRunning(t.Context(), first.Token, now))
	hash := fmt.Sprintf("%064x", 42)
	id := worker.StableToolInvocationID(first.Token.TenantID, first.Token.ConversationID, first.Token.HandoffID, 0, 1, "search", hash)
	recordInvocation := worker.ToolInvocationRecord{InvocationID: id, ObjectiveRevision: 0, ToolOrdinal: 1, ToolName: "search", IdempotencyKey: id, ArgumentsHash: hash, Ciphertext: make([]byte, 16), Nonce: make([]byte, 12), KeyID: "key"}
	_, _, err = repo.BeginToolInvocation(t.Context(), first.Token, recordInvocation)
	require.ErrorIs(t, err, worker.ErrApprovalRequired)
	_, err = pool.Exec(t.Context(), `UPDATE handoffs SET lease_expires_at=$1 WHERE tenant_id=$2 AND conversation_id=$3 AND id=$4`, now.Add(-time.Second), first.Token.TenantID, first.Token.ConversationID, first.Token.HandoffID)
	require.NoError(t, err)
	second, ok, err := repo.LeaseNext(t.Context(), worker.LeaseRequest{WorkerID: "second", LeaseDuration: time.Minute, Now: now})
	require.NoError(t, err)
	require.True(t, ok)
	_, err = pool.Exec(t.Context(), `UPDATE tool_invocations SET approval_state='approved',approval_expires_at=$1 WHERE invocation_id=$2`, now.Add(time.Minute), id)
	require.NoError(t, err)
	_, _, err = repo.BeginToolInvocation(t.Context(), first.Token, recordInvocation)
	assert.ErrorIs(t, err, worker.ErrLeaseLost)
	invocation, created, err := repo.BeginToolInvocation(t.Context(), second.Token, recordInvocation)
	require.NoError(t, err)
	assert.False(t, created)
	assert.Equal(t, "running", invocation.Status)
	var attempt int64
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT attempt FROM tool_invocations WHERE invocation_id=$1`, id).Scan(&attempt))
	assert.Equal(t, int64(second.Token.Attempt), attempt)
	require.NoError(t, repo.CompleteToolInvocation(t.Context(), second.Token, worker.ToolCompletionRecord{InvocationID: id, MutationID: "done", Result: json.RawMessage(`{"ok":true}`)}))
}

func TestToolRunningInvocationRebindsToReacquiredAttemptAndCompletes(t *testing.T) {
	_, pool, manual := repositoryHarness(t)
	repo, err := NewRepository(pool, manual, WithToolGate(testToolGate{"search": true}))
	require.NoError(t, err)
	_, first, now := runningLease(t, repo, "running-reacquire")
	hash := fmt.Sprintf("%064x", 43)
	id := worker.StableToolInvocationID(first.Token.TenantID, first.Token.ConversationID, first.Token.HandoffID, 0, 1, "search", hash)
	record := worker.ToolInvocationRecord{InvocationID: id, ObjectiveRevision: 0, ToolOrdinal: 1, ToolName: "search", IdempotencyKey: id, ArgumentsHash: hash, Ciphertext: make([]byte, 16), Nonce: make([]byte, 12), KeyID: "key"}
	_, created, err := repo.BeginToolInvocation(t.Context(), first.Token, record)
	require.NoError(t, err)
	assert.True(t, created)

	_, err = pool.Exec(t.Context(), `UPDATE handoffs SET lease_expires_at=$1 WHERE tenant_id=$2 AND conversation_id=$3 AND id=$4`, now.Add(-time.Second), first.Token.TenantID, first.Token.ConversationID, first.Token.HandoffID)
	require.NoError(t, err)
	second, ok, err := repo.LeaseNext(t.Context(), worker.LeaseRequest{WorkerID: "second", LeaseDuration: time.Minute, Now: now})
	require.NoError(t, err)
	require.True(t, ok)

	err = repo.CompleteToolInvocation(t.Context(), first.Token, worker.ToolCompletionRecord{InvocationID: id, MutationID: "old", Result: json.RawMessage(`{"old":true}`)})
	assert.ErrorIs(t, err, worker.ErrLeaseLost)
	invocation, created, err := repo.BeginToolInvocation(t.Context(), second.Token, record)
	require.NoError(t, err)
	assert.False(t, created)
	assert.Equal(t, "running", invocation.Status)
	var attempt int64
	var status string
	var result []byte
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT attempt,status,COALESCE(result::text,'') FROM tool_invocations WHERE invocation_id=$1`, id).Scan(&attempt, &status, &result))
	assert.Equal(t, int64(second.Token.Attempt), attempt)
	assert.Equal(t, "running", status)
	assert.Empty(t, result)
	err = repo.CompleteToolInvocation(t.Context(), first.Token, worker.ToolCompletionRecord{InvocationID: id, MutationID: "old-after-rebind", Result: json.RawMessage(`{"old":true}`)})
	assert.ErrorIs(t, err, worker.ErrLeaseLost)
	require.NoError(t, repo.CompleteToolInvocation(t.Context(), second.Token, worker.ToolCompletionRecord{InvocationID: id, MutationID: "replayed", Result: json.RawMessage(`{"ok":true}`)}))
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT status,result::text FROM tool_invocations WHERE invocation_id=$1`, id).Scan(&status, &result))
	assert.Equal(t, "succeeded", status)
	assert.JSONEq(t, `{"ok":true}`, string(result))
	succeeded, created, err := repo.BeginToolInvocation(t.Context(), second.Token, record)
	require.NoError(t, err)
	assert.False(t, created)
	assert.Equal(t, "succeeded", succeeded.Status)
	assert.JSONEq(t, `{"ok":true}`, string(succeeded.Result))
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT attempt,status,result::text FROM tool_invocations WHERE invocation_id=$1`, id).Scan(&attempt, &status, &result))
	assert.Equal(t, int64(second.Token.Attempt), attempt)
	assert.Equal(t, "succeeded", status)
	assert.JSONEq(t, `{"ok":true}`, string(result))
}

func TestToolInvocationAttemptRebindFailpointsRollbackAndRetry(t *testing.T) {
	for _, tc := range []struct {
		name             string
		approvalRequired bool
		failpoint        string
		priorStatus      string
		priorApproval    string
	}{
		{name: "approved pending", approvalRequired: true, failpoint: "tool_invocation_pending_rebind", priorStatus: "pending", priorApproval: "approved"},
		{name: "running", failpoint: "tool_invocation_running_rebind", priorStatus: "running", priorApproval: "not_required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, pool, manual := repositoryHarness(t)
			repo, err := NewRepository(pool, manual, WithToolGate(testToolGate{"search": true}))
			require.NoError(t, err)
			created := createRunnableHandoff(t, repo, "rebind-rollback-"+tc.name)
			if tc.approvalRequired {
				_, err = pool.Exec(t.Context(), `UPDATE handoffs SET approval_policy='{"mode":"ask"}'::json WHERE tenant_id=$1 AND conversation_id=$2 AND id=$3`, created.Request.Scope.TenantID, created.Request.Scope.ConversationID, created.HandoffID)
				require.NoError(t, err)
			}
			now := repositoryTestNow.Add(time.Minute)
			first, ok, err := repo.LeaseNext(t.Context(), worker.LeaseRequest{WorkerID: "first", LeaseDuration: time.Minute, Now: now})
			require.NoError(t, err)
			require.True(t, ok)
			require.NoError(t, repo.MarkRunning(t.Context(), first.Token, now))
			hash := fmt.Sprintf("%064x", 44)
			id := worker.StableToolInvocationID(first.Token.TenantID, first.Token.ConversationID, first.Token.HandoffID, 0, 1, "search", hash)
			record := worker.ToolInvocationRecord{InvocationID: id, ObjectiveRevision: 0, ToolOrdinal: 1, ToolName: "search", IdempotencyKey: id, ArgumentsHash: hash, Ciphertext: make([]byte, 16), Nonce: make([]byte, 12), KeyID: "key"}
			_, _, err = repo.BeginToolInvocation(t.Context(), first.Token, record)
			if tc.approvalRequired {
				require.ErrorIs(t, err, worker.ErrApprovalRequired)
				_, err = pool.Exec(t.Context(), `UPDATE tool_invocations SET approval_state='approved',approval_expires_at=$1 WHERE invocation_id=$2`, now.Add(time.Minute), id)
				require.NoError(t, err)
			} else {
				require.NoError(t, err)
			}
			_, err = pool.Exec(t.Context(), `UPDATE handoffs SET lease_expires_at=$1 WHERE tenant_id=$2 AND conversation_id=$3 AND id=$4`, now.Add(-time.Second), first.Token.TenantID, first.Token.ConversationID, first.Token.HandoffID)
			require.NoError(t, err)
			second, ok, err := repo.LeaseNext(t.Context(), worker.LeaseRequest{WorkerID: "second", LeaseDuration: time.Minute, Now: now})
			require.NoError(t, err)
			require.True(t, ok)

			repo.failpoint = func(step string) error {
				if step == tc.failpoint {
					return assert.AnError
				}
				return nil
			}
			_, _, err = repo.BeginToolInvocation(t.Context(), second.Token, record)
			require.ErrorIs(t, err, assert.AnError)
			var attempt int64
			var status, approval string
			var mutation *string
			var result []byte
			require.NoError(t, pool.QueryRow(t.Context(), `SELECT attempt,status,approval_state,mutation_id,COALESCE(result::text,'') FROM tool_invocations WHERE invocation_id=$1`, id).Scan(&attempt, &status, &approval, &mutation, &result))
			assert.Equal(t, int64(first.Token.Attempt), attempt)
			assert.Equal(t, tc.priorStatus, status)
			assert.Equal(t, tc.priorApproval, approval)
			assert.Nil(t, mutation)
			assert.Empty(t, result)

			repo.failpoint = nil
			invocation, replayCreated, err := repo.BeginToolInvocation(t.Context(), second.Token, record)
			require.NoError(t, err)
			assert.False(t, replayCreated)
			assert.Equal(t, "running", invocation.Status)
			require.NoError(t, pool.QueryRow(t.Context(), `SELECT attempt FROM tool_invocations WHERE invocation_id=$1`, id).Scan(&attempt))
			assert.Equal(t, int64(second.Token.Attempt), attempt)
			require.NoError(t, repo.CompleteToolInvocation(t.Context(), second.Token, worker.ToolCompletionRecord{InvocationID: id, MutationID: "done", Result: json.RawMessage(`{"ok":true}`)}))
		})
	}
}

func TestSteeringSerializesWithSideEffectingToolInvocation(t *testing.T) {
	t.Run("steer first fences old begin", func(t *testing.T) {
		_, pool, manual := repositoryHarness(t)
		repo, err := NewRepository(pool, manual, WithToolGate(testToolGate{"search": true}))
		require.NoError(t, err)
		record, lease, _ := runningLease(t, repo, "steer-first")
		invocation := testToolRecord(lease.Token, 0, 1, "search")
		updated, release := make(chan struct{}), make(chan struct{})
		repo.failpoint = func(step string) error {
			if step == "mutation_update" {
				close(updated)
				<-release
			}
			return nil
		}
		steerDone := make(chan error, 1)
		go func() {
			_, err := repo.SteerHandoff(t.Context(), record.Request.Scope, record.HandoffID, handoff.SteerRequest{Objective: "new", ExpectedRevision: 1, Actor: "user", IdempotencyKey: "steer-first"})
			steerDone <- err
		}()
		<-updated
		beginDone := make(chan error, 1)
		go func() {
			_, _, err := repo.BeginToolInvocation(t.Context(), lease.Token, invocation)
			beginDone <- err
		}()
		close(release)
		require.NoError(t, <-steerDone)
		assert.ErrorIs(t, <-beginDone, worker.ErrLeaseLost)
		assert.Equal(t, int64(0), tableCount(t, pool, "tool_invocations"))
	})

	t.Run("begin first blocks steer and completion succeeds", func(t *testing.T) {
		_, pool, manual := repositoryHarness(t)
		repo, err := NewRepository(pool, manual, WithToolGate(testToolGate{"search": true}))
		require.NoError(t, err)
		record, lease, _ := runningLease(t, repo, "begin-first")
		invocation := testToolRecord(lease.Token, 0, 1, "search")
		inserted, release := make(chan struct{}), make(chan struct{})
		repo.failpoint = func(step string) error {
			if step == "tool_invocation_insert" {
				close(inserted)
				<-release
			}
			return nil
		}
		beginDone := make(chan error, 1)
		go func() {
			_, _, err := repo.BeginToolInvocation(t.Context(), lease.Token, invocation)
			beginDone <- err
		}()
		<-inserted
		steerDone := make(chan error, 1)
		go func() {
			_, err := repo.SteerHandoff(t.Context(), record.Request.Scope, record.HandoffID, handoff.SteerRequest{Objective: "new", ExpectedRevision: 1, Actor: "user", IdempotencyKey: "begin-first"})
			steerDone <- err
		}()
		close(release)
		require.NoError(t, <-beginDone)
		assert.ErrorIs(t, <-steerDone, handoff.ErrSteeringBlocked)
		repo.failpoint = nil
		require.NoError(t, repo.CompleteToolInvocation(t.Context(), lease.Token, worker.ToolCompletionRecord{InvocationID: invocation.InvocationID, MutationID: "done", Result: json.RawMessage(`{"ok":true}`)}))
		var revision int64
		var status string
		require.NoError(t, pool.QueryRow(t.Context(), `SELECT objective_revision FROM handoffs WHERE id=$1`, record.HandoffID).Scan(&revision))
		require.NoError(t, pool.QueryRow(t.Context(), `SELECT status FROM tool_invocations WHERE invocation_id=$1`, invocation.InvocationID).Scan(&status))
		assert.Zero(t, revision)
		assert.Equal(t, "succeeded", status)
	})
}

func TestSteeringToolInvocationSafetyPolicy(t *testing.T) {
	for _, tc := range []struct {
		name          string
		sideEffecting bool
		approval      bool
		complete      bool
		missing       bool
		wantBlocked   bool
	}{
		{name: "running side effect", sideEffecting: true, wantBlocked: true},
		{name: "succeeded side effect remains blocked", sideEffecting: true, complete: true, wantBlocked: true},
		{name: "read only running remains steerable"},
		{name: "pending approval remains steerable", sideEffecting: true, approval: true},
		{name: "missing immutable metadata fails closed", sideEffecting: true, missing: true, wantBlocked: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, pool, manual := repositoryHarness(t)
			repo, err := NewRepository(pool, manual, WithToolGate(testToolGate{"search": tc.sideEffecting}))
			require.NoError(t, err)
			record, lease, _ := runningLease(t, repo, "steer-policy-"+tc.name)
			if tc.approval {
				_, err = pool.Exec(t.Context(), `UPDATE handoffs SET approval_policy='{"mode":"ask"}'::json WHERE id=$1`, record.HandoffID)
				require.NoError(t, err)
			}
			invocation := testToolRecord(lease.Token, 0, 1, "search")
			_, _, err = repo.BeginToolInvocation(t.Context(), lease.Token, invocation)
			if tc.approval {
				require.ErrorIs(t, err, worker.ErrApprovalRequired)
			} else {
				require.NoError(t, err)
			}
			if tc.complete {
				require.NoError(t, repo.CompleteToolInvocation(t.Context(), lease.Token, worker.ToolCompletionRecord{InvocationID: invocation.InvocationID, MutationID: "done", Result: json.RawMessage(`{"ok":true}`)}))
			}
			if tc.missing {
				repo.toolGate = testToolGate{}
			}
			request := handoff.SteerRequest{Objective: "new", ExpectedRevision: 1, Actor: "user", IdempotencyKey: "steer-policy"}
			_, err = repo.SteerHandoff(t.Context(), record.Request.Scope, record.HandoffID, request)
			if tc.wantBlocked {
				assert.ErrorIs(t, err, handoff.ErrSteeringBlocked)
				_, secondErr := repo.SteerHandoff(t.Context(), record.Request.Scope, record.HandoffID, request)
				assert.ErrorIs(t, secondErr, handoff.ErrSteeringBlocked)
				var revision int64
				require.NoError(t, pool.QueryRow(t.Context(), `SELECT objective_revision FROM handoffs WHERE id=$1`, record.HandoffID).Scan(&revision))
				assert.Zero(t, revision)
				return
			}
			require.NoError(t, err)
		})
	}
}

func testToolRecord(token worker.LeaseToken, revision, ordinal uint32, name string) worker.ToolInvocationRecord {
	hash := fmt.Sprintf("%064x", ordinal)
	id := worker.StableToolInvocationID(token.TenantID, token.ConversationID, token.HandoffID, revision, ordinal, name, hash)
	return worker.ToolInvocationRecord{InvocationID: id, ObjectiveRevision: revision, ToolOrdinal: ordinal, ToolName: name, IdempotencyKey: id, ArgumentsHash: hash, Ciphertext: make([]byte, 16), Nonce: make([]byte, 12), KeyID: "key"}
}
