//go:build integration

package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func coordinatorRecord(scope handoff.Scope, kind, suffix string, local uint64) CoordinatorJournalRecord {
	payload, _ := json.Marshal(map[string]any{"kind": kind, "suffix": suffix, "local": local})
	return CoordinatorJournalRecord{
		WorkKey: "work-" + kind + "-" + suffix, Kind: kind, Scope: scope,
		SessionID: identity.SessionID("session-" + suffix), Incarnation: 1,
		LocalSequence: local, Payload: payload, DeliveryAffinity: "app-a",
	}
}

func TestCoordinatorJournalAffinityAndClaimGenerationFence(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	scope := testScope("tenant-journal", "conversation-affinity")
	insertConversation(t, pool, scope)
	record := coordinatorRecord(scope, CoordinatorKindHandoff, "affinity", 1)
	_, err := repo.AdmitCoordinatorWork(t.Context(), record)
	require.NoError(t, err)
	requireCoordinatorAffinity(t, repo, "app-a", "shared-worker", time.Second)

	foreign, err := repo.ClaimCoordinatorWork(t.Context(), CoordinatorKindHandoff, "app-b", "shared-worker", 20*time.Millisecond, 1)
	require.NoError(t, err)
	assert.Empty(t, foreign, "a non-host app must not claim another app's delivery")

	first, err := repo.ClaimCoordinatorWork(t.Context(), CoordinatorKindHandoff, "app-a", "shared-worker", 20*time.Millisecond, 1)
	require.NoError(t, err)
	require.Len(t, first, 1)
	assert.Equal(t, uint64(1), first[0].ClaimGeneration)

	var second []CoordinatorJournalRecord
	require.Eventually(t, func() bool {
		second, err = repo.ClaimCoordinatorWork(t.Context(), CoordinatorKindHandoff, "app-a", "shared-worker", time.Second, 1)
		return err == nil && len(second) == 1
	}, time.Second, 5*time.Millisecond)
	assert.Equal(t, uint64(2), second[0].ClaimGeneration)
	assert.ErrorIs(t, repo.RenewCoordinatorWork(t.Context(), record.WorkKey, "shared-worker", first[0].ClaimGeneration, time.Second), handoff.ErrNotFound)
	assert.ErrorIs(t, repo.AckCoordinatorWork(t.Context(), record.WorkKey, "shared-worker", first[0].ClaimGeneration), handoff.ErrNotFound)
	require.NoError(t, repo.RenewCoordinatorWork(t.Context(), record.WorkKey, "shared-worker", second[0].ClaimGeneration, time.Second))
	require.NoError(t, repo.AckCoordinatorWork(t.Context(), record.WorkKey, "shared-worker", second[0].ClaimGeneration))

	conflict := record
	conflict.DeliveryAffinity = "app-b"
	_, err = repo.AdmitCoordinatorWork(t.Context(), conflict)
	assert.ErrorIs(t, err, handoff.ErrIdempotencyConflict)
	assert.ErrorIs(t, func() error {
		_, claimErr := repo.ClaimCoordinatorWork(t.Context(), CoordinatorKindHandoff, "app-a", strings.Repeat("w", 257), time.Second, 1)
		return claimErr
	}(), handoff.ErrInvalidRequest)
}

// A delivery shard must have one live runtime owner even when that owner has
// already acknowledged its current item. Otherwise a second live gateway can
// consume newly admitted work and the pod-specific affinity bug merely turns
// into an unfenced shared-affinity race.
func TestCoordinatorJournalDeliveryShardLeaseTakeoverFencesFormerOwner(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	scope := testScope("tenant-journal", "conversation-shard-takeover")
	insertConversation(t, pool, scope)
	firstRecord := coordinatorRecord(scope, CoordinatorKindHandoff, "shard-a-first", 1)
	secondRecord := coordinatorRecord(scope, CoordinatorKindHandoff, "shard-a-second", 2)
	isolatedRecord := coordinatorRecord(scope, CoordinatorKindHandoff, "shard-b", 1)
	isolatedRecord.DeliveryAffinity = "app-b"
	for _, record := range []CoordinatorJournalRecord{firstRecord, secondRecord, isolatedRecord} {
		_, err := repo.AdmitCoordinatorWork(t.Context(), record)
		require.NoError(t, err)
	}

	const lease = 120 * time.Millisecond
	firstAffinityGeneration := requireCoordinatorAffinity(t, repo, "app-a", "pod-a", lease)
	first, err := repo.ClaimCoordinatorWork(t.Context(), CoordinatorKindHandoff, "app-a", "pod-a", lease, 1)
	require.NoError(t, err)
	require.Len(t, first, 1)
	assert.Equal(t, firstAffinityGeneration, first[0].AffinityGeneration)
	require.NoError(t, repo.AckCoordinatorWork(t.Context(), first[0].WorkKey, "pod-a", first[0].ClaimGeneration))

	_, acquired, err := repo.AcquireCoordinatorAffinity(t.Context(), "app-a", "pod-b", lease)
	require.NoError(t, err)
	assert.False(t, acquired)
	blocked, err := repo.ClaimCoordinatorWork(t.Context(), CoordinatorKindHandoff, "app-a", "pod-b", lease, 1)
	require.NoError(t, err)
	assert.Empty(t, blocked, "a replacement runtime must not enter a live delivery shard")

	requireCoordinatorAffinity(t, repo, "app-b", "pod-b", lease)
	otherShard, err := repo.ClaimCoordinatorWork(t.Context(), CoordinatorKindHandoff, "app-b", "pod-b", lease, 1)
	require.NoError(t, err)
	require.Len(t, otherShard, 1, "a different delivery shard must remain independently claimable")
	require.NoError(t, repo.AckCoordinatorWork(t.Context(), otherShard[0].WorkKey, "pod-b", otherShard[0].ClaimGeneration))

	require.NoError(t, repo.RenewCoordinatorAffinity(t.Context(), "app-a", "pod-a", firstAffinityGeneration, lease))
	owned, err := repo.ClaimCoordinatorWork(t.Context(), CoordinatorKindHandoff, "app-a", "pod-a", lease, 1)
	require.NoError(t, err)
	require.Len(t, owned, 1)
	assert.Equal(t, first[0].AffinityGeneration, owned[0].AffinityGeneration)

	var replacement []CoordinatorJournalRecord
	require.Eventually(t, func() bool {
		_, acquired, err = repo.AcquireCoordinatorAffinity(t.Context(), "app-a", "pod-b", lease)
		if err != nil || !acquired {
			return false
		}
		replacement, err = repo.ClaimCoordinatorWork(t.Context(), CoordinatorKindHandoff, "app-a", "pod-b", lease, 1)
		return err == nil && len(replacement) == 1
	}, 2*time.Second, 5*time.Millisecond)
	assert.Greater(t, replacement[0].AffinityGeneration, owned[0].AffinityGeneration)
	staleClaim, err := repo.ClaimCoordinatorWork(t.Context(), CoordinatorKindHandoff, "app-a", "pod-a", lease, 1)
	require.NoError(t, err)
	assert.Empty(t, staleClaim)
	assert.ErrorIs(t, repo.RenewCoordinatorWork(t.Context(), owned[0].WorkKey, "pod-a", owned[0].ClaimGeneration, lease), handoff.ErrNotFound)
	assert.ErrorIs(t, repo.AckCoordinatorWork(t.Context(), owned[0].WorkKey, "pod-a", owned[0].ClaimGeneration), handoff.ErrNotFound)
	require.NoError(t, repo.AckCoordinatorWork(t.Context(), replacement[0].WorkKey, "pod-b", replacement[0].ClaimGeneration))
}

func TestCoordinatorAffinityLeaseRenewsAndUsesDatabaseTime(t *testing.T) {
	repo, _, manual := repositoryHarness(t)
	const lease = 80 * time.Millisecond
	firstGeneration := requireCoordinatorAffinity(t, repo, "app-a", "pod-a", lease)
	_, acquired, err := repo.AcquireCoordinatorAffinity(t.Context(), "app-a", "pod-b", lease)
	require.NoError(t, err)
	assert.False(t, acquired)
	manual.Advance(365 * 24 * time.Hour)
	require.NoError(t, repo.RenewCoordinatorAffinity(t.Context(), "app-a", "pod-a", firstGeneration, lease))
	_, acquired, err = repo.AcquireCoordinatorAffinity(t.Context(), "app-a", "pod-b", lease)
	require.NoError(t, err)
	assert.False(t, acquired, "repository clock skew must not expire a database authority lease")

	var replacementGeneration uint64
	require.Eventually(t, func() bool {
		replacementGeneration, acquired, err = repo.AcquireCoordinatorAffinity(t.Context(), "app-a", "pod-b", lease)
		return err == nil && acquired
	}, time.Second, 5*time.Millisecond)
	assert.Greater(t, replacementGeneration, firstGeneration)
	assert.ErrorIs(t, repo.RenewCoordinatorAffinity(t.Context(), "app-a", "pod-a", firstGeneration, lease), handoff.ErrNotFound)

	_, _, err = repo.AcquireCoordinatorAffinity(nil, "app-a", "pod-a", lease)
	assert.ErrorIs(t, err, handoff.ErrInvalidRequest)
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, err = repo.AcquireCoordinatorAffinity(cancelled, "app-a", "pod-a", lease)
	assert.ErrorIs(t, err, context.Canceled)
	_, _, err = repo.AcquireCoordinatorAffinity(t.Context(), strings.Repeat("a", 257), "pod-a", lease)
	assert.ErrorIs(t, err, handoff.ErrInvalidRequest)
}

func TestCoordinatorJournalLeaseUsesDatabaseTimeAndBoundsDuration(t *testing.T) {
	repo, pool, manual := repositoryHarness(t)
	scope := testScope("tenant-journal", "conversation-db-time")
	insertConversation(t, pool, scope)
	record := coordinatorRecord(scope, CoordinatorKindHandoff, "db-time", 1)
	_, err := repo.AdmitCoordinatorWork(t.Context(), record)
	require.NoError(t, err)
	requireCoordinatorAffinity(t, repo, "app-a", "worker-a", time.Second)
	claimed, err := repo.ClaimCoordinatorWork(t.Context(), CoordinatorKindHandoff, "app-a", "worker-a", time.Second, 1)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	manual.Advance(365 * 24 * time.Hour)
	foreign, err := repo.ClaimCoordinatorWork(t.Context(), CoordinatorKindHandoff, "app-a", "worker-b", time.Second, 1)
	require.NoError(t, err)
	assert.Empty(t, foreign, "repository clock skew must not expire a database lease")

	for _, lease := range []time.Duration{postgresLeaseBelowMinimum(), postgresLeaseAboveMaximum()} {
		_, err = repo.ClaimCoordinatorWork(t.Context(), CoordinatorKindHandoff, "app-a", "worker", lease, 1)
		assert.ErrorIs(t, err, handoff.ErrInvalidRequest)
	}
	_, err = repo.ClaimCoordinatorWork(t.Context(), CoordinatorKindHandoff, "app-a", "worker", time.Second, 2)
	assert.ErrorIs(t, err, handoff.ErrInvalidRequest)
}

func postgresLeaseBelowMinimum() time.Duration { return CoordinatorMinLease - time.Nanosecond }
func postgresLeaseAboveMaximum() time.Duration { return CoordinatorMaxLease + time.Nanosecond }

func requireCoordinatorAffinity(t *testing.T, repo *Repository, affinity, owner string, lease time.Duration) uint64 {
	t.Helper()
	generation, acquired, err := repo.AcquireCoordinatorAffinity(t.Context(), affinity, owner, lease)
	require.NoError(t, err)
	require.True(t, acquired)
	require.NotZero(t, generation)
	return generation
}

func TestCoordinatorJournalAllocatesConversationWideSequencesAcrossSessions(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	scope := testScope("tenant-journal", "conversation-shared")
	insertConversation(t, pool, scope)
	records := []CoordinatorJournalRecord{
		coordinatorRecord(scope, CoordinatorKindTranscript, "a", 1),
		coordinatorRecord(scope, CoordinatorKindTranscript, "b", 1),
	}
	admitted := make([]CoordinatorJournalRecord, len(records))
	errs := make([]error, len(records))
	var wg sync.WaitGroup
	for index := range records {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			admitted[index], errs[index] = repo.AdmitCoordinatorWork(t.Context(), records[index])
		}(index)
	}
	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}
	assert.NotZero(t, admitted[0].StorageSequence)
	assert.NotEqual(t, admitted[0].StorageSequence, admitted[1].StorageSequence)

	appendErrs := make([]error, len(admitted))
	for index := range admitted {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			_, appendErrs[index] = repo.AppendCommittedItem(t.Context(), scope, ConversationItem{
				SessionID: admitted[index].SessionID,
				Turn:      identity.TurnKey{ID: fmt.Sprintf("turn-%d", index), Revision: 1},
				Sequence:  admitted[index].StorageSequence, Role: "user", Content: fmt.Sprintf("transcript-%d", index),
				ContentHash: fmt.Sprintf("hash-%d", index), CreatedAt: repositoryTestNow,
			})
		}(index)
	}
	wg.Wait()
	for _, err := range appendErrs {
		require.NoError(t, err)
	}
	assert.Equal(t, int64(2), tableCount(t, pool, "conversation_items"))

	for index, record := range records {
		replayed, err := repo.AdmitCoordinatorWork(t.Context(), record)
		require.NoError(t, err)
		assert.Equal(t, admitted[index].StorageSequence, replayed.StorageSequence)
	}
}

func TestCoordinatorJournalAdmissionRollbackAndConflict(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	scope := testScope("tenant-journal", "conversation-rollback")
	insertConversation(t, pool, scope)
	record := coordinatorRecord(scope, CoordinatorKindTranscript, "rollback", 1)
	repo.failpoint = func(step string) error {
		if step == "journal_after_insert" {
			return errors.New("injected journal failure")
		}
		return nil
	}
	_, err := repo.AdmitCoordinatorWork(t.Context(), record)
	require.Error(t, err)
	assert.Equal(t, int64(0), tableCount(t, pool, "coordinator_work_journal"))

	repo.failpoint = nil
	admitted, err := repo.AdmitCoordinatorWork(t.Context(), record)
	require.NoError(t, err)
	assert.NotZero(t, admitted.StorageSequence)
	conflict := record
	conflict.Payload = json.RawMessage(`{"changed":true}`)
	_, err = repo.AdmitCoordinatorWork(t.Context(), conflict)
	assert.ErrorIs(t, err, handoff.ErrIdempotencyConflict)
	assert.Equal(t, int64(1), tableCount(t, pool, "coordinator_work_journal"))
}

func TestCoordinatorJournalClaimLeaseHasSingleOwnerAndOwnerFencedAck(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	scope := testScope("tenant-journal", "conversation-claim")
	insertConversation(t, pool, scope)
	record := coordinatorRecord(scope, CoordinatorKindHandoff, "claim", 9)
	_, err := repo.AdmitCoordinatorWork(t.Context(), record)
	require.NoError(t, err)
	requireCoordinatorAffinity(t, repo, "app-a", "owner-a", time.Minute)

	type claimResult struct {
		records []CoordinatorJournalRecord
		err     error
	}
	start := make(chan struct{})
	results := make(chan claimResult, 2)
	for range 2 {
		go func() {
			<-start
			records, claimErr := repo.ClaimCoordinatorWork(t.Context(), CoordinatorKindHandoff, "app-a", "owner-a", time.Minute, 1)
			results <- claimResult{records: records, err: claimErr}
		}()
	}
	close(start)
	first, second := <-results, <-results
	require.NoError(t, first.err)
	require.NoError(t, second.err)
	claimed := first
	other := second
	if len(second.records) == 1 {
		claimed, other = second, first
	}
	require.Len(t, claimed.records, 1)
	assert.Empty(t, other.records)
	assert.ErrorIs(t, repo.AckCoordinatorWork(t.Context(), record.WorkKey, "owner-b", claimed.records[0].ClaimGeneration), handoff.ErrNotFound)
	require.NoError(t, repo.AckCoordinatorWork(t.Context(), record.WorkKey, "owner-a", claimed.records[0].ClaimGeneration))

	pending, err := repo.ClaimCoordinatorWork(t.Context(), CoordinatorKindHandoff, "app-a", "owner-c", time.Hour, 1)
	require.NoError(t, err)
	assert.Empty(t, pending)
	assert.Equal(t, int64(1), tableCount(t, pool, "coordinator_work_journal"))
}

func TestCoordinatorJournalRejectsCorruptDurableRecordOnClaim(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	scope := testScope("tenant-journal", "conversation-corrupt")
	insertConversation(t, pool, scope)
	record := coordinatorRecord(scope, CoordinatorKindHandoff, "corrupt", 1)
	_, err := repo.AdmitCoordinatorWork(t.Context(), record)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `UPDATE coordinator_work_journal SET payload_hash=$2 WHERE work_key=$1`, record.WorkKey, strings.Repeat("0", 64))
	require.NoError(t, err)
	requireCoordinatorAffinity(t, repo, "app-a", "owner", time.Minute)

	_, err = repo.ClaimCoordinatorWork(t.Context(), CoordinatorKindHandoff, "app-a", "owner", time.Minute, 1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "corrupt coordinator journal record")
}

func TestCoordinatorJournalPayloadDepth64PassesAnd65Fails(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	scope := testScope("tenant-journal", "conversation-depth")
	insertConversation(t, pool, scope)
	for _, depth := range []int{64, 65} {
		record := coordinatorRecord(scope, CoordinatorKindHandoff, fmt.Sprintf("depth-%d", depth), 1)
		record.Payload = json.RawMessage(strings.Repeat("[", depth) + strings.Repeat("]", depth))
		_, err := repo.AdmitCoordinatorWork(t.Context(), record)
		if depth == 64 {
			require.NoError(t, err)
		} else {
			assert.ErrorIs(t, err, handoff.ErrInvalidRequest)
		}
	}
}

func TestCoordinatorDeliveryShardMigrationRequeuesLegacyClaims(t *testing.T) {
	_, pool, _ := repositoryHarness(t)
	require.NoError(t, migrations.Steps(t.Context(), pool, -2))
	scope := testScope("tenant-journal", "conversation-shard-upgrade")
	insertConversation(t, pool, scope)
	record := coordinatorRecord(scope, CoordinatorKindHandoff, "shard-upgrade", 1)
	insertLegacyCoordinatorRecord(t, pool, record)
	_, err := pool.Exec(t.Context(), `UPDATE coordinator_work_journal
		SET claim_owner='legacy-pod',claim_until=CURRENT_TIMESTAMP+interval '1 minute',claim_generation=4
		WHERE work_key=$1`, record.WorkKey)
	require.NoError(t, err)

	require.NoError(t, migrations.Steps(t.Context(), pool, 1))
	var owner *string
	var claimUntil *time.Time
	var affinityGeneration *int64
	var claimGeneration int64
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT claim_owner,claim_until,claim_affinity_generation,claim_generation
		FROM coordinator_work_journal WHERE work_key=$1`, record.WorkKey).
		Scan(&owner, &claimUntil, &affinityGeneration, &claimGeneration))
	assert.Nil(t, owner)
	assert.Nil(t, claimUntil)
	assert.Nil(t, affinityGeneration)
	assert.Equal(t, int64(4), claimGeneration)
}

func TestCoordinatorDeliveryShardDownRefusesPendingAndPermitsDelivered(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	scope := testScope("tenant-journal", "conversation-shard-down")
	insertConversation(t, pool, scope)
	record := coordinatorRecord(scope, CoordinatorKindHandoff, "shard-down", 1)
	_, err := repo.AdmitCoordinatorWork(t.Context(), record)
	require.NoError(t, err)

	require.NoError(t, migrations.Steps(t.Context(), pool, -1))
	err = migrations.Steps(t.Context(), pool, -1)
	require.ErrorIs(t, err, migrations.ErrCoordinatorPendingWork)
	version, dirty, versionErr := migrations.Version(t.Context(), pool)
	require.NoError(t, versionErr)
	assert.Equal(t, uint(7), version)
	assert.False(t, dirty)
	var shardPresent, generationPresent bool
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT
		to_regclass('coordinator_delivery_shards') IS NOT NULL,
		EXISTS (SELECT 1 FROM information_schema.columns
			WHERE table_schema=current_schema() AND table_name='coordinator_work_journal'
				AND column_name='claim_affinity_generation')`).Scan(&shardPresent, &generationPresent))
	assert.True(t, shardPresent)
	assert.True(t, generationPresent)

	requireCoordinatorAffinity(t, repo, "app-a", "pod-a", time.Minute)
	claimed, err := repo.ClaimCoordinatorWork(t.Context(), CoordinatorKindHandoff, "app-a", "pod-a", time.Minute, 1)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	require.NoError(t, repo.AckCoordinatorWork(t.Context(), record.WorkKey, "pod-a", claimed[0].ClaimGeneration))
	require.NoError(t, migrations.Steps(t.Context(), pool, -1))
	version, dirty, err = migrations.Version(t.Context(), pool)
	require.NoError(t, err)
	assert.Equal(t, uint(6), version)
	assert.False(t, dirty)
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT
		to_regclass('coordinator_delivery_shards') IS NOT NULL,
		EXISTS (SELECT 1 FROM information_schema.columns
			WHERE table_schema=current_schema() AND table_name='coordinator_work_journal'
				AND column_name='claim_affinity_generation')`).Scan(&shardPresent, &generationPresent))
	assert.False(t, shardPresent)
	assert.False(t, generationPresent)
}

func TestCoordinatorJournalDownMigrationRefusesPendingAndPermitsDelivered(t *testing.T) {
	for _, operation := range []struct {
		name string
		run  func(*pgxpool.Pool) error
	}{
		{name: "steps", run: func(pool *pgxpool.Pool) error { return migrations.Steps(t.Context(), pool, -1) }},
		{name: "down", run: func(pool *pgxpool.Pool) error { return migrations.Down(t.Context(), pool) }},
	} {
		t.Run("pending refuses "+operation.name+" without dirtying schema", func(t *testing.T) {
			_, pool, _ := repositoryHarness(t)
			requireCoordinatorMigrationVersion2(t, pool)
			scope := testScope("tenant-journal", "conversation-down-pending-"+operation.name)
			insertConversation(t, pool, scope)
			record := coordinatorRecord(scope, CoordinatorKindHandoff, "down-pending-"+operation.name, 1)
			insertLegacyCoordinatorRecord(t, pool, record)
			refusal := operation.run(pool)
			require.ErrorIs(t, refusal, migrations.ErrCoordinatorPendingWork)
			require.ErrorContains(t, refusal, "undelivered replay work")
			version, dirty, versionErr := migrations.Version(t.Context(), pool)
			require.NoError(t, versionErr)
			assert.Equal(t, uint(2), version)
			assert.False(t, dirty)
			assert.ErrorContains(t, migrations.Check(t.Context(), pool), "schema version is behind")
			var present bool
			require.NoError(t, pool.QueryRow(t.Context(), `SELECT to_regclass('coordinator_work_journal') IS NOT NULL`).Scan(&present))
			assert.True(t, present)

			_, err := pool.Exec(t.Context(), `UPDATE coordinator_work_journal
				SET delivered=true,delivered_at=CURRENT_TIMESTAMP WHERE work_key=$1`, record.WorkKey)
			require.NoError(t, err)
			require.NoError(t, operation.run(pool))
		})
	}

	t.Run("delivered permits downgrade", func(t *testing.T) {
		_, pool, _ := repositoryHarness(t)
		requireCoordinatorMigrationVersion2(t, pool)
		scope := testScope("tenant-journal", "conversation-down-delivered")
		insertConversation(t, pool, scope)
		record := coordinatorRecord(scope, CoordinatorKindHandoff, "down-delivered", 1)
		insertLegacyCoordinatorRecord(t, pool, record)
		_, err := pool.Exec(t.Context(), `UPDATE coordinator_work_journal
			SET delivered=true,delivered_at=CURRENT_TIMESTAMP WHERE work_key=$1`, record.WorkKey)
		require.NoError(t, err)
		require.NoError(t, migrations.Steps(t.Context(), pool, -1))
		var present bool
		require.NoError(t, pool.QueryRow(t.Context(), `SELECT to_regclass('coordinator_work_journal') IS NOT NULL`).Scan(&present))
		assert.False(t, present)
	})
}

func TestCoordinatorJournalDownWaitsForConcurrentAdmissionThenRefuses(t *testing.T) {
	_, pool, _ := repositoryHarness(t)
	requireCoordinatorMigrationVersion2(t, pool)
	scope := testScope("tenant-journal", "conversation-down-race")
	insertConversation(t, pool, scope)
	record := coordinatorRecord(scope, CoordinatorKindHandoff, "down-race", 1)
	hash := sha256.Sum256(record.Payload)
	tx, err := pool.Begin(t.Context())
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.Background()) }()
	_, err = tx.Exec(t.Context(), `INSERT INTO coordinator_work_journal (
		work_key,kind,tenant_id,user_id,conversation_id,session_id,delivery_affinity,
		incarnation,local_sequence,payload,payload_hash,storage_sequence
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::json,$11,NULL)`,
		record.WorkKey, record.Kind, record.Scope.TenantID, record.Scope.UserID, record.Scope.ConversationID,
		record.SessionID, record.DeliveryAffinity, record.Incarnation, record.LocalSequence,
		record.Payload, hex.EncodeToString(hash[:]))
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- migrations.Steps(t.Context(), pool, -1) }()
	select {
	case downErr := <-done:
		t.Fatalf("downgrade did not wait for concurrent admission: %v", downErr)
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(t, tx.Commit(t.Context()))
	refusal := <-done
	require.ErrorIs(t, refusal, migrations.ErrCoordinatorPendingWork)
	require.ErrorContains(t, refusal, "undelivered replay work")
	version, dirty, err := migrations.Version(t.Context(), pool)
	require.NoError(t, err)
	assert.Equal(t, uint(2), version)
	assert.False(t, dirty)
	var count int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM coordinator_work_journal WHERE work_key=$1`, record.WorkKey).Scan(&count))
	assert.Equal(t, 1, count)
}

func requireCoordinatorMigrationVersion2(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	require.NoError(t, migrations.Steps(t.Context(), pool, -6))
	version, dirty, err := migrations.Version(t.Context(), pool)
	require.NoError(t, err)
	assert.Equal(t, uint(2), version)
	assert.False(t, dirty)
}

func insertLegacyCoordinatorRecord(t *testing.T, pool *pgxpool.Pool, record CoordinatorJournalRecord) {
	t.Helper()
	hash := sha256.Sum256(record.Payload)
	_, err := pool.Exec(t.Context(), `INSERT INTO coordinator_work_journal (
		work_key,kind,tenant_id,user_id,conversation_id,session_id,delivery_affinity,
		incarnation,local_sequence,payload,payload_hash,storage_sequence
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::json,$11,NULL)`,
		record.WorkKey, record.Kind, record.Scope.TenantID, record.Scope.UserID, record.Scope.ConversationID,
		record.SessionID, record.DeliveryAffinity, record.Incarnation, record.LocalSequence,
		record.Payload, hex.EncodeToString(hash[:]))
	require.NoError(t, err)
}
