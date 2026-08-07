//go:build integration

package postgres

import (
	"math"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/testkit"
	"github.com/chuanmingliu/openai-gpt-live/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
