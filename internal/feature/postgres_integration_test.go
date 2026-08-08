//go:build integration

package feature

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/testkit"
	"github.com/chuanmingliu/openai-gpt-live/migrations"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func migratedFeatureStore(t *testing.T) (*PostgresStore, *clock.Manual) {
	t.Helper()
	pool := testkit.Postgres(t)
	require.NoError(t, migrations.Up(t.Context(), pool))
	manual := clock.NewManual(time.Unix(1_700_000_000, 0))
	store, err := NewPostgres(pool, manual, time.Second)
	require.NoError(t, err)
	return store, manual
}

func TestPostgresAuthoritySeedsAndMutatesAtomically(t *testing.T) {
	store, _ := migratedFeatureStore(t)
	snapshot, err := store.Load(t.Context())
	require.NoError(t, err)
	assert.Equal(t, Snapshot{Version: 1, Values: map[string]bool{HandoffsEnabled: true, ShadowMode: false}}, snapshot)

	snapshot, err = store.Set(t.Context(), HandoffsEnabled, false, "operator", "incident-123")
	require.NoError(t, err)
	assert.Equal(t, uint64(2), snapshot.Version)
	assert.False(t, snapshot.Values[HandoffsEnabled])
	var count int
	require.NoError(t, store.pool.QueryRow(t.Context(), `SELECT count(*) FROM runtime_feature_audit WHERE version=2 AND name=$1 AND enabled=false AND actor='operator' AND reason='incident-123'`, HandoffsEnabled).Scan(&count))
	assert.Equal(t, 1, count)
	_, err = store.pool.Exec(t.Context(), `UPDATE runtime_feature_audit SET reason='tampered' WHERE version=2`)
	assert.Error(t, err)
	_, err = store.pool.Exec(t.Context(), `DELETE FROM runtime_feature_audit WHERE version=2`)
	assert.Error(t, err)
	require.NoError(t, store.pool.QueryRow(t.Context(), `SELECT count(*) FROM runtime_feature_audit WHERE version=2`).Scan(&count))
	assert.Equal(t, 1, count)
}

func TestPostgresAuditRejectsUpdateDeleteAndTruncateWithoutRowLoss(t *testing.T) {
	store, _ := migratedFeatureStore(t)
	_, err := store.Set(t.Context(), HandoffsEnabled, false, "operator", "append-only")
	require.NoError(t, err)
	for name, statement := range map[string]string{
		"update":   `UPDATE runtime_feature_audit SET reason='tampered' WHERE version=2`,
		"delete":   `DELETE FROM runtime_feature_audit WHERE version=2`,
		"truncate": `TRUNCATE runtime_feature_audit`,
	} {
		t.Run(name, func(t *testing.T) {
			_, mutationErr := store.pool.Exec(t.Context(), statement)
			assert.Error(t, mutationErr)
			var count int
			require.NoError(t, store.pool.QueryRow(t.Context(), `SELECT count(*) FROM runtime_feature_audit WHERE version=2`).Scan(&count))
			assert.Equal(t, 1, count)
		})
	}
}

func TestPostgresCommitAcknowledgementLossReconcilesExactAuditWithoutMutationRetry(t *testing.T) {
	store, _ := migratedFeatureStore(t)
	beginCount := 0
	store.mutationHook = func(step string) {
		if step == "begin" {
			beginCount++
		}
	}
	later, err := NewPostgres(store.pool, clock.Real{}, time.Second)
	require.NoError(t, err)
	ackLost := errors.New("commit acknowledgement lost")
	store.commit = func(ctx context.Context, tx pgx.Tx) error {
		require.NoError(t, tx.Commit(ctx), "the real feature transaction must commit before acknowledgement is lost")
		_, laterErr := later.Set(t.Context(), ShadowMode, true, "later-operator", "later-serialized-write")
		require.NoError(t, laterErr)
		return ackLost
	}

	snapshot, err := store.Set(t.Context(), HandoffsEnabled, false, "operator", "ack-loss")
	require.NoError(t, err)
	assert.Equal(t, uint64(2), snapshot.Version, "reconciliation returns the exact committed mutation snapshot")
	assert.False(t, snapshot.Values[HandoffsEnabled])
	assert.Equal(t, 1, beginCount, "ambiguous commit must never retry the mutation")
	current, err := later.Load(t.Context())
	require.NoError(t, err)
	assert.Equal(t, uint64(3), current.Version, "a later mutation may advance authority before reconciliation")
	var exactAudits int
	require.NoError(t, store.pool.QueryRow(t.Context(), `SELECT count(*) FROM runtime_feature_audit WHERE version=2 AND name=$1 AND enabled=false AND actor='operator' AND reason='ack-loss'`, HandoffsEnabled).Scan(&exactAudits))
	assert.Equal(t, 1, exactAudits)
}

func TestPostgresConcurrentWritersHaveUniqueMonotonicVersionsAndAudits(t *testing.T) {
	store, _ := migratedFeatureStore(t)
	const writers = 24
	versions := make(chan uint64, writers)
	errorsFound := make(chan error, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			snapshot, err := store.Set(t.Context(), HandoffsEnabled, index%2 == 0, fmt.Sprintf("actor-%d", index), "concurrency-test")
			if err != nil {
				errorsFound <- err
				return
			}
			versions <- snapshot.Version
		}(i)
	}
	wg.Wait()
	close(errorsFound)
	for err := range errorsFound {
		require.NoError(t, err)
	}
	close(versions)
	seen := map[uint64]bool{}
	for version := range versions {
		assert.False(t, seen[version])
		seen[version] = true
	}
	assert.Len(t, seen, writers)
	final, err := store.Load(t.Context())
	require.NoError(t, err)
	assert.Equal(t, uint64(writers+1), final.Version)
	var audits int
	require.NoError(t, store.pool.QueryRow(t.Context(), `SELECT count(*) FROM runtime_feature_audit`).Scan(&audits))
	assert.Equal(t, writers, audits)
}

func TestPostgresCorruptionFailsClosed(t *testing.T) {
	store, _ := migratedFeatureStore(t)
	_, err := store.pool.Exec(t.Context(), `DELETE FROM runtime_feature_flags WHERE name=$1`, ShadowMode)
	require.NoError(t, err)
	_, err = store.Load(t.Context())
	assert.ErrorContains(t, err, "corrupt")
}

type pollingOnlyStore struct{ store *PostgresStore }

func (s pollingOnlyStore) Load(ctx context.Context) (Snapshot, error) { return s.store.Load(ctx) }
func (s pollingOnlyStore) Set(ctx context.Context, name string, enabled bool, actor, reason string) (Snapshot, error) {
	return s.store.Set(ctx, name, enabled, actor, reason)
}

func TestDroppedNotificationIsRepairedByBoundedPoll(t *testing.T) {
	store, manual := migratedFeatureStore(t)
	gates := NewWithClock(nil, manual)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- gates.Run(ctx, pollingOnlyStore{store}, time.Second) }()
	require.Eventually(t, func() bool { return gates.Snapshot().Version == 1 }, 5*time.Second, time.Millisecond)
	_, err := store.Set(t.Context(), HandoffsEnabled, false, "operator", "poll-repair")
	require.NoError(t, err)
	assert.True(t, gates.Enabled(HandoffsEnabled))
	manual.Advance(2 * time.Second)
	require.Eventually(t, func() bool { return gates.Snapshot().Version == 2 }, 5*time.Second, time.Millisecond)
	assert.False(t, gates.Enabled(HandoffsEnabled))
	cancel()
	assert.ErrorIs(t, <-done, context.Canceled)
}

func TestTwoGateInstancesObserveDisableAndListenerReconnects(t *testing.T) {
	pool := testkit.Postgres(t)
	require.NoError(t, migrations.Up(t.Context(), pool))
	firstStore, err := NewPostgres(pool, clock.Real{}, 10*time.Millisecond)
	require.NoError(t, err)
	secondStore, err := NewPostgres(pool, clock.Real{}, 10*time.Millisecond)
	require.NoError(t, err)
	firstConnected := make(chan uint32, 2)
	secondConnected := make(chan uint32, 1)
	firstStore.listenerHook = func(pid uint32) { firstConnected <- pid }
	secondStore.listenerHook = func(pid uint32) { secondConnected <- pid }
	first, second := New(nil), New(nil)
	ctx, cancel := context.WithCancel(t.Context())
	firstDone, secondDone := make(chan error, 1), make(chan error, 1)
	go func() { firstDone <- first.Run(ctx, firstStore, 50*time.Millisecond) }()
	go func() { secondDone <- second.Run(ctx, secondStore, 50*time.Millisecond) }()
	firstPID, secondPID := <-firstConnected, <-secondConnected
	assert.NotEqual(t, firstPID, secondPID)
	_, err = firstStore.Set(t.Context(), HandoffsEnabled, false, "operator", "two-instance-disable")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return !first.Enabled(HandoffsEnabled) && !second.Enabled(HandoffsEnabled) && first.Snapshot().Version == 2 && second.Snapshot().Version == 2
	}, 100*time.Millisecond, time.Millisecond)

	var terminated bool
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT pg_terminate_backend($1)`, firstPID).Scan(&terminated))
	assert.True(t, terminated)
	select {
	case replacement := <-firstConnected:
		assert.NotEqual(t, firstPID, replacement)
	case <-time.After(time.Second):
		t.Fatal("feature listener did not reconnect")
	}
	cancel()
	assert.ErrorIs(t, <-firstDone, context.Canceled)
	assert.ErrorIs(t, <-secondDone, context.Canceled)
}
