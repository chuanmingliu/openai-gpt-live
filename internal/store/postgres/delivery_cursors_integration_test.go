//go:build integration

package postgres

import (
	"math"
	"strings"
	"sync"
	"testing"

	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeliveryCursorMissingMonotonicAndTenantIsolated(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	scopeA := testScope("tenant-a", "shared")
	scopeB := testScope("tenant-b", "shared")
	insertConversation(t, pool, scopeA)
	insertConversation(t, pool, scopeB)
	for _, suffix := range []string{"cursor-monotonic-1", "cursor-monotonic-2"} {
		record := prepareCreate(t, repo, scopeA, suffix)
		_, _, err := repo.CreateHandoff(t.Context(), record)
		require.NoError(t, err)
	}
	events, err := repo.ReadEvents(t.Context(), scopeA, 0, 10)
	require.NoError(t, err)
	require.Len(t, events, 2)
	got, err := repo.LoadDeliveryCursor(t.Context(), "consumer-1", scopeA)
	require.NoError(t, err)
	assert.Zero(t, got)

	values := []int64{0, events[0].Seq, events[0].Seq, 0, events[0].Seq}
	start := make(chan struct{})
	errCh := make(chan error, len(values))
	var wg sync.WaitGroup
	for _, value := range values {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errCh <- repo.AdvanceDeliveryCursor(t.Context(), "consumer-1", scopeA, value)
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}
	got, err = repo.LoadDeliveryCursor(t.Context(), "consumer-1", scopeA)
	require.NoError(t, err)
	assert.Equal(t, events[0].Seq, got)
	require.NoError(t, repo.AdvanceDeliveryCursor(t.Context(), "consumer-1", scopeA, events[1].Seq))
	require.NoError(t, repo.AdvanceDeliveryCursor(t.Context(), "consumer-1", scopeA, events[0].Seq))
	got, err = repo.LoadDeliveryCursor(t.Context(), "consumer-1", scopeA)
	require.NoError(t, err)
	assert.Equal(t, events[1].Seq, got)
	got, err = repo.LoadDeliveryCursor(t.Context(), "consumer-1", scopeB)
	require.NoError(t, err)
	assert.Zero(t, got)
}

func TestDeliveryCursorCrashEquivalentReplayAndNoSkip(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	scope := testScope("tenant-a", "conversation-1")
	insertConversation(t, pool, scope)
	record := prepareCreate(t, repo, scope, "cursor")
	_, _, err := repo.CreateHandoff(t.Context(), record)
	require.NoError(t, err)
	first, err := repo.ReadEvents(t.Context(), scope, 0, 10)
	require.NoError(t, err)
	require.Len(t, first, 1)

	// Delivery before cursor advancement replays the identical durable event.
	replay, err := repo.ReadEvents(t.Context(), scope, 0, 10)
	require.NoError(t, err)
	assert.Equal(t, first, replay)
	require.NoError(t, repo.AdvanceDeliveryCursor(t.Context(), "consumer", scope, first[0].Seq))
	cursor, err := repo.LoadDeliveryCursor(t.Context(), "consumer", scope)
	require.NoError(t, err)
	next, err := repo.ReadEvents(t.Context(), scope, cursor, 10)
	require.NoError(t, err)
	assert.Empty(t, next)
}

func TestDeliveryCursorValidatesScopeConsumerAndSequence(t *testing.T) {
	repo, _, _ := repositoryHarness(t)
	scope := testScope("tenant-a", "conversation-1")
	for _, consumer := range []string{"", strings.Repeat("x", maxIdentityBytes+1)} {
		_, err := repo.LoadDeliveryCursor(t.Context(), consumer, scope)
		assert.ErrorIs(t, err, handoff.ErrInvalidRequest)
	}
	assert.ErrorIs(t, repo.AdvanceDeliveryCursor(t.Context(), "consumer", scope, -1), handoff.ErrInvalidRequest)
}

func TestDeliveryCursorRejectsForeignFutureAndSkippedSequences(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	scope := testScope("tenant-a", "conversation-1")
	foreign := testScope("tenant-b", "conversation-2")
	insertConversation(t, pool, scope)
	insertConversation(t, pool, foreign)

	firstRecord := prepareCreate(t, repo, scope, "cursor-first")
	_, _, err := repo.CreateHandoff(t.Context(), firstRecord)
	require.NoError(t, err)
	foreignRecord := prepareCreate(t, repo, foreign, "cursor-foreign")
	_, _, err = repo.CreateHandoff(t.Context(), foreignRecord)
	require.NoError(t, err)
	secondRecord := prepareCreate(t, repo, scope, "cursor-second")
	_, _, err = repo.CreateHandoff(t.Context(), secondRecord)
	require.NoError(t, err)

	local, err := repo.ReadEvents(t.Context(), scope, 0, 10)
	require.NoError(t, err)
	require.Len(t, local, 2)
	foreignEvents, err := repo.ReadEvents(t.Context(), foreign, 0, 10)
	require.NoError(t, err)
	require.Len(t, foreignEvents, 1)
	for _, invalid := range []int64{math.MaxInt64, foreignEvents[0].Seq, local[1].Seq} {
		err := repo.AdvanceDeliveryCursor(t.Context(), "strict-consumer", scope, invalid)
		assert.ErrorIs(t, err, handoff.ErrRevisionConflict)
		cursor, loadErr := repo.LoadDeliveryCursor(t.Context(), "strict-consumer", scope)
		require.NoError(t, loadErr)
		assert.Zero(t, cursor)
		pending, readErr := repo.ReadEvents(t.Context(), scope, cursor, 10)
		require.NoError(t, readErr)
		assert.Equal(t, local, pending)
	}

	require.NoError(t, repo.AdvanceDeliveryCursor(t.Context(), "strict-consumer", scope, local[0].Seq))
	require.NoError(t, repo.AdvanceDeliveryCursor(t.Context(), "strict-consumer", scope, 0))
	require.NoError(t, repo.AdvanceDeliveryCursor(t.Context(), "strict-consumer", scope, local[0].Seq))

	start := make(chan struct{})
	errs := make(chan error, 8)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- repo.AdvanceDeliveryCursor(t.Context(), "strict-consumer", scope, local[1].Seq)
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	cursor, err := repo.LoadDeliveryCursor(t.Context(), "strict-consumer", scope)
	require.NoError(t, err)
	assert.Equal(t, local[1].Seq, cursor)
}
