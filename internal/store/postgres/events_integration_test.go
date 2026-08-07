//go:build integration

package postgres

import (
	"context"
	"testing"

	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadEventsUsesGlobalSequencePagingAndExactScope(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	scopeA := testScope("tenant-a", "shared")
	scopeB := testScope("tenant-b", "shared")
	insertConversation(t, pool, scopeA)
	insertConversation(t, pool, scopeB)
	var aSeq []int64
	for i, suffix := range []string{"a1", "b1", "a2"} {
		scope := scopeA
		if i == 1 {
			scope = scopeB
		}
		record := prepareCreate(t, repo, scope, suffix)
		_, _, err := repo.CreateHandoff(t.Context(), record)
		require.NoError(t, err)
		if scope.TenantID == scopeA.TenantID {
			var seq int64
			require.NoError(t, pool.QueryRow(t.Context(), `SELECT seq FROM handoff_events WHERE event_id=$1`, record.EventID).Scan(&seq))
			aSeq = append(aSeq, seq)
		}
	}
	page, err := repo.ReadEvents(t.Context(), scopeA, 0, 1)
	require.NoError(t, err)
	require.Len(t, page, 1)
	assert.Equal(t, aSeq[0], page[0].Seq)
	second, err := repo.ReadEvents(t.Context(), scopeA, page[0].Seq, 1)
	require.NoError(t, err)
	require.Len(t, second, 1)
	assert.Equal(t, aSeq[1], second[0].Seq)
	replay, err := repo.ReadEvents(t.Context(), scopeA, page[0].Seq, 10)
	require.NoError(t, err)
	assert.Equal(t, second, replay)
	empty, err := repo.ReadEvents(t.Context(), scopeA, second[0].Seq, 10)
	require.NoError(t, err)
	assert.Empty(t, empty)

	wrongUser := scopeA
	wrongUser.UserID = "other-user"
	leaked, err := repo.ReadEvents(t.Context(), wrongUser, 0, 10)
	require.NoError(t, err)
	assert.Empty(t, leaked)
}

func TestReadEventsRejectsInvalidBoundsAndCancellation(t *testing.T) {
	repo, _, _ := repositoryHarness(t)
	scope := testScope("tenant-a", "conversation-1")
	for _, tc := range []struct {
		after int64
		limit int
	}{{-1, 1}, {0, 0}, {0, maxEventPageSize + 1}} {
		_, err := repo.ReadEvents(t.Context(), scope, tc.after, tc.limit)
		assert.ErrorIs(t, err, handoff.ErrInvalidRequest)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := repo.ReadEvents(ctx, scope, 0, 1)
	assert.ErrorIs(t, err, context.Canceled)
}
