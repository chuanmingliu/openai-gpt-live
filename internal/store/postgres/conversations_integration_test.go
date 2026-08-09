//go:build integration

package postgres

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/session"
	"github.com/chuanmingliu/openai-gpt-live/internal/testkit"
	"github.com/chuanmingliu/openai-gpt-live/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadRecoveryBoundsNewestCommittedTextAndIsolatesOwnership(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	scope := testScope("tenant-recovery", "conversation-recovery")
	insertConversation(t, pool, scope)
	for index, text := range []string{"old", "中", "new"} {
		_, err := repo.AppendCommittedItem(t.Context(), scope, ConversationItem{
			SessionID: "session-1", Turn: identity.TurnKey{ID: "turn-" + text, Revision: 1},
			Sequence: uint64(index + 1), Role: "user", Content: text, ContentHash: "hash-" + text,
			CreatedAt: repositoryTestNow.Add(time.Duration(index) * time.Second),
		})
		require.NoError(t, err)
	}

	snapshot, err := repo.LoadRecovery(t.Context(), scope, 2, len("中")+len("new"))
	require.NoError(t, err)
	require.Len(t, snapshot.CommittedItems, 2)
	assert.Equal(t, []string{"中", "new"}, []string{snapshot.CommittedItems[0].Content, snapshot.CommittedItems[1].Content})
	assert.Equal(t, scope.ConversationID, snapshot.ConversationID)

	wrong := scope
	wrong.UserID = "other-user"
	_, err = repo.LoadRecovery(t.Context(), wrong, 2, 32)
	assert.ErrorIs(t, err, handoff.ErrNotFound)
	_, err = repo.LoadRecovery(t.Context(), scope, 0, 32)
	assert.ErrorIs(t, err, session.ErrInvalidRecoveryBounds)
}

func TestLoadRecoveryReturnsActiveAllowlistAndLastScopedEventSequence(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	scope := testScope("tenant-recovery-active", "conversation-recovery-active")
	insertConversation(t, pool, scope)
	record := prepareCreate(t, repo, scope, "recovery-active")
	created, _, err := repo.CreateHandoff(t.Context(), record)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `UPDATE handoffs SET state='needs_input',input_revision=1,input='{"secret":"credential"}'::jsonb,
		safety_decision='{"internal":"deny"}'::jsonb
		WHERE tenant_id=$1 AND conversation_id=$2 AND id=$3`, scope.TenantID, scope.ConversationID, created.ID)
	require.NoError(t, err)

	snapshot, err := repo.LoadRecovery(t.Context(), scope, 8, 1024)
	require.NoError(t, err)
	require.Len(t, snapshot.ActiveHandoffs, 1)
	assert.Equal(t, handoff.StateNeedsInput, snapshot.ActiveHandoffs[0].State)
	assert.Nil(t, snapshot.ActiveHandoffs[0].Input)
	assert.Nil(t, snapshot.ActiveHandoffs[0].Request.ApprovalPolicy)
	assert.Empty(t, snapshot.ActiveHandoffs[0].Request.ContextRef)
	assert.Empty(t, snapshot.ActiveHandoffs[0].Request.IdempotencyKey)
	assert.Nil(t, snapshot.ActiveHandoffs[0].Result)
	assert.Nil(t, snapshot.ActiveHandoffs[0].Failure)
	assert.Positive(t, snapshot.LastEventSeq)
}

func TestLoadRecoveryAppliesByteBudgetInsideNewestFirstQuery(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	scope := testScope("tenant-recovery-bytes", "conversation-recovery-bytes")
	insertConversation(t, pool, scope)
	for index := 0; index < 16; index++ {
		text := strings.Repeat(string(rune('a'+index)), 60_000)
		_, err := repo.AppendCommittedItem(t.Context(), scope, ConversationItem{
			SessionID: "session-bytes", Turn: identity.TurnKey{ID: fmt.Sprintf("turn-%02d", index), Revision: 1},
			Sequence: uint64(index + 1), Role: "user", Content: text, ContentHash: fmt.Sprintf("hash-%02d", index),
			CreatedAt: repositoryTestNow.Add(time.Duration(index) * time.Second),
		})
		require.NoError(t, err)
	}
	snapshot, err := repo.LoadRecovery(t.Context(), scope, 10_000, 8)
	require.NoError(t, err)
	assert.Empty(t, snapshot.CommittedItems, "an oversized newest item prevents older fallback and large row transfer")
}

var repositoryTestNow = time.Date(2026, 8, 7, 4, 0, 0, 123456000, time.UTC)

func repositoryHarness(t *testing.T) (*Repository, *pgxpool.Pool, *clock.Manual) {
	t.Helper()
	pool := testkit.Postgres(t)
	require.NoError(t, migrations.Up(t.Context(), pool))
	manual := clock.NewManual(repositoryTestNow)
	repo, err := NewRepository(pool, manual)
	require.NoError(t, err)
	return repo, pool, manual
}

func insertConversation(t *testing.T, pool *pgxpool.Pool, scope handoff.Scope) {
	t.Helper()
	_, err := pool.Exec(t.Context(), `
		INSERT INTO conversations (tenant_id, id, user_id, retention_until, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $5)`,
		scope.TenantID, scope.ConversationID, scope.UserID, repositoryTestNow.Add(24*time.Hour), repositoryTestNow)
	require.NoError(t, err)
}

func testScope(tenant, conversation string) handoff.Scope {
	return handoff.Scope{TenantID: tenant, UserID: "user-1", ConversationID: identity.ConversationID(conversation)}
}

func TestProvisionConversationIsOwnerExactIdempotentAndRaceSafe(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	scope := testScope("tenant-provision", "conversation-provision")

	const racers = 16
	errs := make(chan error, racers)
	for index := 0; index < racers; index++ {
		go func() { errs <- repo.ProvisionConversation(t.Context(), scope, 24*time.Hour) }()
	}
	for index := 0; index < racers; index++ {
		require.NoError(t, <-errs)
	}

	var owner string
	var createdAt, retentionUntil time.Time
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT user_id,created_at,retention_until FROM conversations WHERE tenant_id=$1 AND id=$2`,
		scope.TenantID, scope.ConversationID).Scan(&owner, &createdAt, &retentionUntil))
	require.Equal(t, scope.UserID, owner)
	require.Equal(t, repositoryTestNow, createdAt.UTC())
	require.Equal(t, repositoryTestNow.Add(24*time.Hour), retentionUntil.UTC())

	require.NoError(t, repo.ProvisionConversation(t.Context(), scope, 48*time.Hour), "duplicate retry must not mutate the original retention contract")
	var duplicateRetention time.Time
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT retention_until FROM conversations WHERE tenant_id=$1 AND id=$2`,
		scope.TenantID, scope.ConversationID).Scan(&duplicateRetention))
	require.Equal(t, retentionUntil.UTC(), duplicateRetention.UTC())

	wrongOwner := scope
	wrongOwner.UserID = "other-user"
	require.ErrorIs(t, repo.ProvisionConversation(t.Context(), wrongOwner, 24*time.Hour), handoff.ErrNotFound)
}

func TestAppendCommittedItemIsIdempotentAndScopeExact(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	scope := testScope("tenant-a", "conversation-1")
	insertConversation(t, pool, scope)
	item := ConversationItem{
		SessionID: "session-1", Turn: identity.TurnKey{ID: "turn-1", Revision: 2}, Sequence: 7,
		Role: "user", Content: "final transcript", ContentHash: "sha256:final", CreatedAt: repositoryTestNow,
	}

	first, err := repo.AppendCommittedItem(t.Context(), scope, item)
	require.NoError(t, err)
	second, err := repo.AppendCommittedItem(t.Context(), scope, item)
	require.NoError(t, err)
	assert.Equal(t, first, second)
	assert.Equal(t, repositoryTestNow, first.CreatedAt)
	assert.Equal(t, repositoryTestNow, first.UpdatedAt)

	committed, err := repo.IsCommittedTurn(t.Context(), scope, item.Turn)
	require.NoError(t, err)
	assert.True(t, committed)
	otherUser := scope
	otherUser.UserID = "user-2"
	committed, err = repo.IsCommittedTurn(t.Context(), otherUser, item.Turn)
	require.NoError(t, err)
	assert.False(t, committed)
	assert.Equal(t, int64(1), tableCount(t, pool, "conversation_items"))
}

func TestAppendCommittedItemRejectsConflictingReplayAndSequence(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	scope := testScope("tenant-a", "conversation-1")
	insertConversation(t, pool, scope)
	item := ConversationItem{SessionID: "session-1", Turn: identity.TurnKey{ID: "turn-1", Revision: 1}, Sequence: 3,
		Role: "assistant", Content: "one", ContentHash: "hash-one", CreatedAt: repositoryTestNow}
	_, err := repo.AppendCommittedItem(t.Context(), scope, item)
	require.NoError(t, err)

	changed := item
	changed.Content, changed.ContentHash = "two", "hash-two"
	_, err = repo.AppendCommittedItem(t.Context(), scope, changed)
	assert.ErrorIs(t, err, handoff.ErrIdempotencyConflict)

	sequenceCollision := item
	sequenceCollision.Turn.ID = "turn-2"
	_, err = repo.AppendCommittedItem(t.Context(), scope, sequenceCollision)
	assert.ErrorIs(t, err, handoff.ErrIdempotencyConflict)

	turnCollision := item
	turnCollision.Sequence = 4
	_, err = repo.AppendCommittedItem(t.Context(), scope, turnCollision)
	assert.ErrorIs(t, err, handoff.ErrIdempotencyConflict)
	assert.Equal(t, int64(1), tableCount(t, pool, "conversation_items"))
}

func TestAppendCommittedItemValidatesFinalizedFields(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	scope := testScope("tenant-a", "conversation-1")
	insertConversation(t, pool, scope)
	valid := ConversationItem{SessionID: "session-1", Turn: identity.TurnKey{ID: "turn-1", Revision: 1}, Sequence: 1,
		Role: "user", Content: "final", ContentHash: "hash", CreatedAt: repositoryTestNow}
	for name, mutate := range map[string]func(*ConversationItem){
		"zero revision":   func(item *ConversationItem) { item.Turn.Revision = 0 },
		"empty content":   func(item *ConversationItem) { item.Content = "" },
		"invalid role":    func(item *ConversationItem) { item.Role = "partial" },
		"bigint overflow": func(item *ConversationItem) { item.Sequence = math.MaxUint64 },
	} {
		t.Run(name, func(t *testing.T) {
			item := valid
			mutate(&item)
			_, err := repo.AppendCommittedItem(t.Context(), scope, item)
			assert.ErrorIs(t, err, handoff.ErrInvalidRequest)
		})
	}
	assert.Equal(t, int64(0), tableCount(t, pool, "conversation_items"))
}

func tableCount(t *testing.T, pool *pgxpool.Pool, table string) int64 {
	t.Helper()
	var count int64
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM `+table).Scan(&count))
	return count
}
