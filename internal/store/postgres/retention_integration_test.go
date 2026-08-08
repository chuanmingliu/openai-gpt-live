//go:build integration

package postgres

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/admin"
	"github.com/chuanmingliu/openai-gpt-live/internal/auth"
	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/lifecycle"
	"github.com/chuanmingliu/openai-gpt-live/internal/worker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type retentionAdminAuth struct{ principal auth.Principal }

func (a retentionAdminAuth) Authenticate(context.Context, string) (auth.Principal, error) {
	return a.principal, nil
}

func TestConversationTombstoneSerializesTranscriptAndCursorMutations(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(context.Context, *Repository, handoff.Scope) error
		count  func(context.Context, *Repository, handoff.Scope) int
	}{
		{
			name: "transcript append",
			mutate: func(ctx context.Context, repo *Repository, scope handoff.Scope) error {
				_, err := repo.AppendCommittedItem(ctx, scope, ConversationItem{SessionID: "session-delete-race", Turn: identity.TurnKey{ID: "turn-delete-race", Revision: 1}, Sequence: 1, Role: "user", Content: "must-not-commit", ContentHash: "hash", CreatedAt: repositoryTestNow})
				return err
			},
			count: func(ctx context.Context, repo *Repository, scope handoff.Scope) int {
				var count int
				require.NoError(t, repo.pool.QueryRow(ctx, `SELECT count(*) FROM conversation_items WHERE tenant_id=$1 AND conversation_id=$2`, scope.TenantID, scope.ConversationID).Scan(&count))
				return count
			},
		},
		{
			name: "delivery cursor advance",
			mutate: func(ctx context.Context, repo *Repository, scope handoff.Scope) error {
				return repo.AdvanceDeliveryCursor(ctx, "consumer-delete-race", scope, 1)
			},
			count: func(ctx context.Context, repo *Repository, scope handoff.Scope) int {
				var count int
				require.NoError(t, repo.pool.QueryRow(ctx, `SELECT count(*) FROM delivery_cursors WHERE tenant_id=$1 AND conversation_id=$2`, scope.TenantID, scope.ConversationID).Scan(&count))
				return count
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, pool, manual := repositoryHarness(t)
			markRepo, err := NewRepository(pool, manual)
			require.NoError(t, err)
			mutationRepo, err := NewRepository(pool, manual)
			require.NoError(t, err)
			scope := testScope("tenant-delete-race", "conversation-delete-race")
			insertConversation(t, pool, scope)
			locked, release := make(chan struct{}), make(chan struct{})
			markRepo.failpoint = func(step string) error {
				if step == "deletion_after_conversation_lock" {
					close(locked)
					<-release
				}
				return nil
			}
			markDone := make(chan error, 1)
			go func() {
				markDone <- markRepo.MarkConversationDeleting(t.Context(), scope, scope.UserID, handoff.DeletionReasonUserRequested)
			}()
			<-locked
			mutationDone := make(chan error, 1)
			go func() { mutationDone <- tc.mutate(t.Context(), mutationRepo, scope) }()
			select {
			case err := <-mutationDone:
				t.Fatalf("mutation bypassed tombstone lock: %v", err)
			case <-time.After(50 * time.Millisecond):
			}
			close(release)
			require.NoError(t, <-markDone)
			assert.ErrorIs(t, <-mutationDone, handoff.ErrNotFound)
			assert.Zero(t, tc.count(t.Context(), mutationRepo, scope))
		})
	}
}

func TestConversationDeletionMarksCancelsProtectsLeaseThenCascadesAndAudits(t *testing.T) {
	_, pool, manual := repositoryHarness(t)
	repo, err := NewRepository(pool, manual, WithRetentionHMAC("retention-test-v1", []byte(strings.Repeat("k", 32))))
	require.NoError(t, err)
	created := createRunnableHandoff(t, repo, "delete-active")
	lease, ok, err := repo.LeaseNext(t.Context(), worker.LeaseRequest{WorkerID: "delete-worker", LeaseDuration: time.Minute, Now: repositoryTestNow})
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, repo.MarkRunning(t.Context(), lease.Token, repositoryTestNow))

	require.NoError(t, repo.MarkConversationDeleting(t.Context(), created.Request.Scope, created.Request.Scope.UserID, "user_requested"))
	require.NoError(t, repo.MarkConversationDeleting(t.Context(), created.Request.Scope, created.Request.Scope.UserID, "user_requested"))
	assert.ErrorIs(t, repo.MarkConversationDeleting(t.Context(), created.Request.Scope, created.Request.Scope.UserID, handoff.DeletionReasonPrivacyRequest), handoff.ErrIdempotencyConflict)
	assert.ErrorIs(t, repo.MarkConversationDeleting(t.Context(), created.Request.Scope, created.Request.Scope.UserID, "password=secret transcript text"), handoff.ErrInvalidRequest)
	var status, actor, reason string
	var cancelled bool
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT c.status,c.deletion_actor,c.deletion_reason_code,h.cancel_requested_at IS NOT NULL
		FROM conversations c JOIN handoffs h ON h.tenant_id=c.tenant_id AND h.conversation_id=c.id
		WHERE c.tenant_id=$1 AND c.id=$2`, created.Request.Scope.TenantID, created.Request.Scope.ConversationID).Scan(&status, &actor, &reason, &cancelled))
	assert.Equal(t, "deleted", status)
	assert.Equal(t, created.Request.Scope.UserID, actor)
	assert.Equal(t, "user_requested", reason)
	assert.True(t, cancelled)

	count, err := repo.DeleteReadyConversations(t.Context(), repositoryTestNow.Add(30*time.Second), 10)
	require.NoError(t, err)
	assert.Zero(t, count)
	count, err = repo.DeleteReadyConversations(t.Context(), repositoryTestNow.Add(2*time.Minute), 10)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
	var conversations int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM conversations WHERE tenant_id=$1 AND id=$2`, created.Request.Scope.TenantID, created.Request.Scope.ConversationID).Scan(&conversations))
	assert.Zero(t, conversations)
	var hash, auditActor, auditReason string
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT conversation_hash,actor,reason_code FROM deletion_audits WHERE tenant_id=$1`, created.Request.Scope.TenantID).Scan(&hash, &auditActor, &auditReason))
	assert.Len(t, hash, 64)
	assert.NotContains(t, hash, string(created.Request.Scope.ConversationID))
	assert.Equal(t, actor, auditActor)
	assert.Equal(t, reason, auditReason)
}

func TestRetentionTTLBatchBoundsAndTenantIsolation(t *testing.T) {
	_, pool, manual := repositoryHarness(t)
	repo, err := NewRepository(pool, manual, WithRetentionHMAC("retention-test-v1", []byte(strings.Repeat("h", 32))))
	require.NoError(t, err)
	for _, scope := range []handoff.Scope{testScope("tenant-a", "same-id"), testScope("tenant-b", "same-id")} {
		insertConversation(t, pool, scope)
		_, err := pool.Exec(t.Context(), `UPDATE conversations SET retention_until=$3 WHERE tenant_id=$1 AND id=$2`, scope.TenantID, scope.ConversationID, repositoryTestNow)
		require.NoError(t, err)
	}
	assert.Error(t, func() error { _, err := repo.DeleteReadyConversations(t.Context(), repositoryTestNow, 0); return err }())
	count, err := repo.DeleteReadyConversations(t.Context(), repositoryTestNow, 1)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
	count, err = repo.DeleteReadyConversations(t.Context(), repositoryTestNow, 1)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
	var audits int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM deletion_audits`).Scan(&audits))
	assert.Equal(t, 2, audits)
}

func TestRetentionSweepDoesNotLetLeaseProtectedOldestConversationStarveReadyWork(t *testing.T) {
	_, pool, manual := repositoryHarness(t)
	repo, err := NewRepository(pool, manual, WithRetentionHMAC("retention-test-v1", []byte(strings.Repeat("s", 32))))
	require.NoError(t, err)
	protected := createRunnableHandoff(t, repo, "starvation-protected")
	lease, ok, err := repo.LeaseNext(t.Context(), worker.LeaseRequest{
		WorkerID: "starvation-worker", LeaseDuration: 10 * time.Minute, Now: repositoryTestNow,
	})
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, repo.MarkRunning(t.Context(), lease.Token, repositoryTestNow))
	require.NoError(t, repo.MarkConversationDeleting(t.Context(), protected.Request.Scope, protected.Request.Scope.UserID, "user_requested"))

	ready := testScope("tenant-starvation", "ready-behind-protected")
	insertConversation(t, pool, ready)
	manual.Advance(time.Second)
	require.NoError(t, repo.MarkConversationDeleting(t.Context(), ready, ready.UserID, "user_requested"))

	count, err := repo.DeleteReadyConversations(t.Context(), repositoryTestNow.Add(2*time.Second), 1)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
	var protectedCount, readyCount int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM conversations WHERE tenant_id=$1 AND id=$2`,
		protected.Request.Scope.TenantID, protected.Request.Scope.ConversationID).Scan(&protectedCount))
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM conversations WHERE tenant_id=$1 AND id=$2`,
		ready.TenantID, ready.ConversationID).Scan(&readyCount))
	assert.Equal(t, 1, protectedCount, "the active lease must fence hard deletion")
	assert.Zero(t, readyCount, "eligible work behind the protected row must still be deleted")
}

func TestConversationHardDeleteCascadesCiphertextResultCursorAndJournal(t *testing.T) {
	_, pool, manual := repositoryHarness(t)
	repo, err := NewRepository(pool, manual, WithToolGate(testToolGate{"search": false}), WithRetentionHMAC("retention-test-v1", []byte(strings.Repeat("c", 32))))
	require.NoError(t, err)
	created := createRunnableHandoff(t, repo, "delete-sensitive-cascade")
	lease, ok, err := repo.LeaseNext(t.Context(), worker.LeaseRequest{WorkerID: "cascade-worker", LeaseDuration: time.Minute, Now: repositoryTestNow})
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, repo.MarkRunning(t.Context(), lease.Token, repositoryTestNow))
	record := testToolRecord(lease.Token, 0, 1, "search")
	_, made, err := repo.BeginToolInvocation(t.Context(), lease.Token, record)
	require.NoError(t, err)
	require.True(t, made)
	_, err = pool.Exec(t.Context(), `UPDATE tool_invocations SET status='succeeded',result='{"secret_result":"delete-me"}'::json,
		result_metadata='{"private":"delete-me"}'::json,mutation_id='cascade-result',updated_at=$2 WHERE invocation_id=$1`, record.InvocationID, repositoryTestNow)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `INSERT INTO delivery_cursors(consumer_id,tenant_id,conversation_id,last_sequence,created_at,updated_at)
		VALUES('cascade-consumer',$1,$2,0,$3,$3)`, created.Request.Scope.TenantID, created.Request.Scope.ConversationID, repositoryTestNow)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `INSERT INTO coordinator_work_journal(work_key,kind,tenant_id,user_id,conversation_id,session_id,
		delivery_affinity,incarnation,local_sequence,payload,payload_hash,created_at)
		VALUES('cascade-journal','handoff',$1,$2,$3,'cascade-session','cascade-affinity',1,1,'{"private":"delete-me"}'::json,repeat('a',64),$4)`,
		created.Request.Scope.TenantID, created.Request.Scope.UserID, created.Request.Scope.ConversationID, repositoryTestNow)
	require.NoError(t, err)
	outboxRows, err := pool.Query(t.Context(), `SELECT o.id FROM outbox o JOIN handoff_events e ON e.seq=o.event_seq
		WHERE e.tenant_id=$1 AND e.conversation_id=$2 ORDER BY o.id`, created.Request.Scope.TenantID, created.Request.Scope.ConversationID)
	require.NoError(t, err)
	var outboxIDs []int64
	for outboxRows.Next() {
		var id int64
		require.NoError(t, outboxRows.Scan(&id))
		outboxIDs = append(outboxIDs, id)
	}
	require.NoError(t, outboxRows.Err())
	outboxRows.Close()
	require.NotEmpty(t, outboxIDs, "cascade fixture must contain exact durable outbox rows")

	require.NoError(t, repo.MarkConversationDeleting(t.Context(), created.Request.Scope, created.Request.Scope.UserID, handoff.DeletionReasonPrivacyRequest))
	count, err := repo.DeleteReadyConversations(t.Context(), repositoryTestNow.Add(2*time.Minute), 1)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
	for _, table := range []string{"conversations", "conversation_items", "handoffs", "handoff_events", "tool_invocations", "delivery_cursors", "coordinator_work_journal"} {
		var remaining int
		query := `SELECT count(*) FROM ` + table + ` WHERE tenant_id=$1`
		if table != "conversations" {
			query += ` AND conversation_id=$2`
		} else {
			query += ` AND id=$2`
		}
		require.NoError(t, pool.QueryRow(t.Context(), query, created.Request.Scope.TenantID, created.Request.Scope.ConversationID).Scan(&remaining), table)
		assert.Zero(t, remaining, table)
	}
	var outboxRemaining int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM outbox WHERE id=ANY($1)`, outboxIDs).Scan(&outboxRemaining))
	assert.Zero(t, outboxRemaining, "captured outbox rows must cascade through their handoff events")
	var leaked int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM tool_invocations WHERE arguments_ciphertext::text LIKE '%delete-me%' OR result::text LIKE '%delete-me%'`).Scan(&leaked))
	assert.Zero(t, leaked)
	var audits int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM deletion_audits WHERE tenant_id=$1 AND reason_code=$2`, created.Request.Scope.TenantID, handoff.DeletionReasonPrivacyRequest).Scan(&audits))
	assert.Equal(t, 1, audits)
}

func TestRetentionConcurrentSweepersClaimDistinctRowsAndCancellationClaimsNone(t *testing.T) {
	_, pool, manual := repositoryHarness(t)
	repoA, err := NewRepository(pool, manual, WithRetentionHMAC("retention-test-v1", []byte(strings.Repeat("q", 32))))
	require.NoError(t, err)
	repoB, err := NewRepository(pool, manual, WithRetentionHMAC("retention-test-v1", []byte(strings.Repeat("q", 32))))
	require.NoError(t, err)
	for _, id := range []string{"concurrent-a", "concurrent-b"} {
		scope := testScope("tenant-concurrent", id)
		insertConversation(t, pool, scope)
		_, err := pool.Exec(t.Context(), `UPDATE conversations SET retention_until=$3 WHERE tenant_id=$1 AND id=$2`, scope.TenantID, scope.ConversationID, repositoryTestNow)
		require.NoError(t, err)
	}
	start := make(chan struct{})
	results := make(chan int, 2)
	errorsOut := make(chan error, 2)
	for _, repo := range []*Repository{repoA, repoB} {
		go func(repo *Repository) {
			<-start
			count, err := repo.DeleteReadyConversations(t.Context(), repositoryTestNow, 1)
			results <- count
			errorsOut <- err
		}(repo)
	}
	close(start)
	require.NoError(t, <-errorsOut)
	require.NoError(t, <-errorsOut)
	assert.Equal(t, 2, <-results+<-results)
	var audits int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM deletion_audits WHERE tenant_id='tenant-concurrent'`).Scan(&audits))
	assert.Equal(t, 2, audits)

	cancelScope := testScope("tenant-concurrent", "cancelled-sweep")
	insertConversation(t, pool, cancelScope)
	_, err = pool.Exec(t.Context(), `UPDATE conversations SET retention_until=$3 WHERE tenant_id=$1 AND id=$2`, cancelScope.TenantID, cancelScope.ConversationID, repositoryTestNow)
	require.NoError(t, err)
	cancelCtx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = repoA.DeleteReadyConversations(cancelCtx, repositoryTestNow, 1)
	assert.ErrorIs(t, err, context.Canceled)
	var remaining int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM conversations WHERE tenant_id=$1 AND id=$2`, cancelScope.TenantID, cancelScope.ConversationID).Scan(&remaining))
	assert.Equal(t, 1, remaining)
}

func TestAdminDeletionWithRealStoreAcceptsMarkButProtectsActiveLease(t *testing.T) {
	_, pool, manual := repositoryHarness(t)
	repo, err := NewRepository(pool, manual, WithRetentionHMAC("retention-test-v1", []byte(strings.Repeat("a", 32))))
	require.NoError(t, err)
	created := createRunnableHandoff(t, repo, "admin-real-store")
	_, ok, err := repo.LeaseNext(t.Context(), worker.LeaseRequest{WorkerID: "admin-delete-worker", LeaseDuration: time.Minute, Now: repositoryTestNow})
	require.NoError(t, err)
	require.True(t, ok)
	principal := auth.Principal{TenantID: created.Request.Scope.TenantID, UserID: "admin-user", Scopes: map[string]struct{}{admin.AdminScope: {}}}
	handler, err := admin.NewHandler(admin.Config{Authenticator: retentionAdminAuth{principal: principal}, Drainer: lifecycle.NewDrainer(), Deletions: repo})
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodDelete, "http://localhost/admin/v1/conversations/"+string(created.Request.Scope.ConversationID), strings.NewReader(`{"reason":"admin_requested"}`))
	request.RemoteAddr = "127.0.0.1:1234"
	request.Header.Set("Authorization", "Bearer admin-token")
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	assert.Equal(t, http.StatusAccepted, response.Code)

	count, err := repo.DeleteReadyConversations(t.Context(), repositoryTestNow.Add(30*time.Second), 1)
	require.NoError(t, err)
	assert.Zero(t, count)
	count, err = repo.DeleteReadyConversations(t.Context(), repositoryTestNow.Add(2*time.Minute), 1)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}
