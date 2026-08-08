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

func TestCoordinatorAffinityLockIsExclusiveReusableAndNamespaced(t *testing.T) {
	repo, _, _ := repositoryHarness(t)
	first, err := repo.AcquireCoordinatorAffinity(t.Context(), "app-a")
	require.NoError(t, err)
	require.NotNil(t, first)
	t.Cleanup(func() { _ = first.Close() })

	_, err = repo.AcquireCoordinatorAffinity(t.Context(), "app-a")
	assert.ErrorIs(t, err, ErrCoordinatorAffinityInUse)
	other, err := repo.AcquireCoordinatorAffinity(t.Context(), "app-b")
	require.NoError(t, err)
	require.NoError(t, other.Close())

	require.NoError(t, first.Close())
	restarted, err := repo.AcquireCoordinatorAffinity(t.Context(), "app-a")
	require.NoError(t, err)
	require.NoError(t, restarted.Close())

	_, err = repo.AcquireCoordinatorAffinity(nil, "app-a")
	assert.ErrorIs(t, err, handoff.ErrInvalidRequest)
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = repo.AcquireCoordinatorAffinity(cancelled, "app-a")
	assert.ErrorIs(t, err, context.Canceled)
	_, err = repo.AcquireCoordinatorAffinity(t.Context(), strings.Repeat("a", 257))
	assert.ErrorIs(t, err, handoff.ErrInvalidRequest)
}

func TestCoordinatorJournalLeaseUsesDatabaseTimeAndBoundsDuration(t *testing.T) {
	repo, pool, manual := repositoryHarness(t)
	scope := testScope("tenant-journal", "conversation-db-time")
	insertConversation(t, pool, scope)
	record := coordinatorRecord(scope, CoordinatorKindHandoff, "db-time", 1)
	_, err := repo.AdmitCoordinatorWork(t.Context(), record)
	require.NoError(t, err)
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

	type claimResult struct {
		owner   string
		records []CoordinatorJournalRecord
		err     error
	}
	start := make(chan struct{})
	results := make(chan claimResult, 2)
	for _, owner := range []string{"owner-a", "owner-b"} {
		go func(owner string) {
			<-start
			records, claimErr := repo.ClaimCoordinatorWork(t.Context(), CoordinatorKindHandoff, "app-a", owner, time.Minute, 1)
			results <- claimResult{owner: owner, records: records, err: claimErr}
		}(owner)
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
	assert.ErrorIs(t, repo.AckCoordinatorWork(t.Context(), record.WorkKey, other.owner, claimed.records[0].ClaimGeneration), handoff.ErrNotFound)
	require.NoError(t, repo.AckCoordinatorWork(t.Context(), record.WorkKey, claimed.owner, claimed.records[0].ClaimGeneration))

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

func TestCoordinatorJournalDownMigrationRefusesPendingAndPermitsDelivered(t *testing.T) {
	for _, operation := range []struct {
		name string
		run  func(*pgxpool.Pool) error
	}{
		{name: "steps", run: func(pool *pgxpool.Pool) error { return migrations.Steps(t.Context(), pool, -1) }},
		{name: "down", run: func(pool *pgxpool.Pool) error { return migrations.Down(t.Context(), pool) }},
	} {
		t.Run("pending refuses "+operation.name+" without dirtying schema", func(t *testing.T) {
			repo, pool, _ := repositoryHarness(t)
			requireCoordinatorMigrationVersion2(t, pool)
			scope := testScope("tenant-journal", "conversation-down-pending-"+operation.name)
			insertConversation(t, pool, scope)
			record := coordinatorRecord(scope, CoordinatorKindHandoff, "down-pending-"+operation.name, 1)
			_, err := repo.AdmitCoordinatorWork(t.Context(), record)
			require.NoError(t, err)
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

			claimed, claimErr := repo.ClaimCoordinatorWork(t.Context(), CoordinatorKindHandoff, "app-a", "worker", time.Minute, 1)
			require.NoError(t, claimErr)
			require.Len(t, claimed, 1)
			require.NoError(t, repo.AckCoordinatorWork(t.Context(), record.WorkKey, "worker", claimed[0].ClaimGeneration))
			require.NoError(t, operation.run(pool))
		})
	}

	t.Run("delivered permits downgrade", func(t *testing.T) {
		repo, pool, _ := repositoryHarness(t)
		requireCoordinatorMigrationVersion2(t, pool)
		scope := testScope("tenant-journal", "conversation-down-delivered")
		insertConversation(t, pool, scope)
		record := coordinatorRecord(scope, CoordinatorKindHandoff, "down-delivered", 1)
		_, err := repo.AdmitCoordinatorWork(t.Context(), record)
		require.NoError(t, err)
		claimed, err := repo.ClaimCoordinatorWork(t.Context(), CoordinatorKindHandoff, "app-a", "worker", time.Minute, 1)
		require.NoError(t, err)
		require.Len(t, claimed, 1)
		require.NoError(t, repo.AckCoordinatorWork(t.Context(), record.WorkKey, "worker", claimed[0].ClaimGeneration))
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
	require.NoError(t, migrations.Steps(t.Context(), pool, -2))
	version, dirty, err := migrations.Version(t.Context(), pool)
	require.NoError(t, err)
	assert.Equal(t, uint(2), version)
	assert.False(t, dirty)
}
