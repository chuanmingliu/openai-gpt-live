//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/toolpolicy"
	"github.com/chuanmingliu/openai-gpt-live/internal/worker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type integrationHandoffIDs struct {
	handoffID identity.HandoffID
	eventID   string
}

func (i integrationHandoffIDs) NewHandoffID() identity.HandoffID { return i.handoffID }
func (i integrationHandoffIDs) NewEventID() string               { return i.eventID }

func validCreateRecord(scope handoff.Scope, suffix string) handoff.CreateRecord {
	policy := toolpolicy.DefaultPolicy()
	policy.WebSearch.Enabled = false
	toolPolicy, _ := toolpolicy.MarshalCanonical(policy)
	request := handoff.CreateRequest{
		Scope: scope, SessionID: identity.SessionID("session-" + suffix), SourceTurn: identity.TurnKey{ID: "turn-" + suffix, Revision: 2},
		UserRequest: "remember exact request", Objective: "perform durable work", ContextRef: "context://" + suffix,
		ContextHash: "sha256:context-" + suffix, Deadline: repositoryTestNow.Add(time.Hour), TokenBudget: 4096, Priority: 12,
		ToolAllowlist: []string{"fetch", "search"}, ToolPolicy: toolPolicy, ApprovalPolicy: json.RawMessage(`{"mode":"ask"}`),
		Versions:         handoff.Versions{Model: "model-v1", Prompt: "prompt-v2", ToolRegistry: "tools-v3", Config: "config-v4"},
		PresentationHint: "compact", IdempotencyKey: "create-" + suffix,
	}
	hash, _ := handoff.CanonicalRequestHash(request)
	return handoff.CreateRecord{HandoffID: identity.HandoffID("handoff-" + suffix), EventID: "event-" + suffix,
		Request: request, RequestHash: hash, CreatedAt: repositoryTestNow}
}

func TestHandoffRoundTripPreservesExactToolPolicy(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	scope := testScope("tenant-tool-policy", "conversation-tool-policy")
	insertConversation(t, pool, scope)
	record := prepareCreate(t, repo, scope, "tool-policy")
	record.Request.ToolAllowlist = append(record.Request.ToolAllowlist, toolpolicy.WebSearchTool)
	record.Request.ToolPolicy, _ = toolpolicy.MarshalCanonical(toolpolicy.DefaultPolicy())
	record.RequestHash, _ = handoff.CanonicalRequestHash(record.Request)

	created, wasCreated, err := repo.CreateHandoff(t.Context(), record)
	require.NoError(t, err)
	require.True(t, wasCreated)
	loaded, err := repo.GetHandoff(t.Context(), record.Request.Scope, created.ID)
	require.NoError(t, err)
	assert.Equal(t, string(record.Request.ToolPolicy), string(loaded.Request.ToolPolicy))
	steered, err := repo.SteerHandoff(t.Context(), record.Request.Scope, created.ID, handoff.SteerRequest{
		Objective: "refined objective", ExpectedRevision: 1, Actor: record.Request.Scope.UserID, IdempotencyKey: "steer-tool-policy",
	})
	require.NoError(t, err)
	assert.Equal(t, string(record.Request.ToolPolicy), string(steered.Request.ToolPolicy))
}

func prepareCreate(t *testing.T, repo *Repository, scope handoff.Scope, suffix string) handoff.CreateRecord {
	t.Helper()
	var sequence uint64
	require.NoError(t, repo.pool.QueryRow(t.Context(), `SELECT coalesce(max(sequence),0)+1 FROM conversation_items
		WHERE tenant_id=$1 AND conversation_id=$2`, scope.TenantID, scope.ConversationID).Scan(&sequence))
	item := ConversationItem{SessionID: identity.SessionID("session-" + suffix), Turn: identity.TurnKey{ID: "turn-" + suffix, Revision: 2},
		Sequence: sequence, Role: "user", Content: "committed", ContentHash: "hash-" + suffix, CreatedAt: repositoryTestNow}
	_, err := repo.AppendCommittedItem(t.Context(), scope, item)
	require.NoError(t, err)
	return validCreateRecord(scope, suffix)
}

func TestCreateHandoffRoundTripIsAtomicAndIdempotent(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	scope := testScope("tenant-a", "conversation-1")
	insertConversation(t, pool, scope)
	record := prepareCreate(t, repo, scope, "one")

	first, created, err := repo.CreateHandoff(t.Context(), record)
	require.NoError(t, err)
	assert.True(t, created)
	assert.Equal(t, record.HandoffID, first.ID)
	assert.Equal(t, record.Request, first.Request)
	assert.Equal(t, record.RequestHash, first.RequestHash)
	assert.Equal(t, handoff.StatePending, first.State)
	assert.Equal(t, repositoryTestNow, first.CreatedAt)

	retry := record
	retry.HandoffID, retry.EventID = "untrusted-retry-id", "untrusted-retry-event"
	second, created, err := repo.CreateHandoff(t.Context(), retry)
	require.NoError(t, err)
	assert.False(t, created)
	assert.Equal(t, first, second)
	assert.Equal(t, int64(1), tableCount(t, pool, "handoffs"))
	assert.Equal(t, int64(1), tableCount(t, pool, "handoff_events"))
	assert.Equal(t, int64(2), tableCount(t, pool, "outbox"))

	rows, err := pool.Query(t.Context(), `SELECT topic, aggregate_id, event_seq, payload::text FROM outbox ORDER BY topic`)
	require.NoError(t, err)
	defer rows.Close()
	var seen []string
	for rows.Next() {
		var topic, aggregate, payload string
		var seq int64
		require.NoError(t, rows.Scan(&topic, &aggregate, &seq, &payload))
		assert.Equal(t, string(record.HandoffID), aggregate)
		assert.NotContains(t, payload, record.Request.Objective)
		assert.NotContains(t, payload, record.Request.UserRequest)
		assert.NotContains(t, payload, "approval")
		seen = append(seen, topic)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []string{"handoff.dispatch", "handoff.event"}, seen)
}

func TestCreateHandoffReplayPrecedesDeadlineAndCandidateValidation(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	scope := testScope("tenant-a", "conversation-replay-precedence")
	insertConversation(t, pool, scope)
	record := prepareCreate(t, repo, scope, "replay-precedence")
	first, created, err := repo.CreateHandoff(t.Context(), record)
	require.NoError(t, err)
	assert.True(t, created)

	replay := record
	replay.HandoffID, replay.EventID = "", ""
	replay.CreatedAt = record.Request.Deadline.Add(time.Hour)
	got, created, err := repo.CreateHandoff(t.Context(), replay)
	require.NoError(t, err)
	assert.False(t, created)
	assert.Equal(t, first, got)

	conflict := replay
	conflict.Request.Objective = "different exact request"
	conflict.RequestHash, err = handoff.CanonicalRequestHash(conflict.Request)
	require.NoError(t, err)
	_, _, err = repo.CreateHandoff(t.Context(), conflict)
	assert.ErrorIs(t, err, handoff.ErrIdempotencyConflict)

	absent := replay
	absent.Request.IdempotencyKey = "expired-absent"
	absent.RequestHash, err = handoff.CanonicalRequestHash(absent.Request)
	require.NoError(t, err)
	_, _, err = repo.CreateHandoff(t.Context(), absent)
	assert.ErrorIs(t, err, handoff.ErrInvalidRequest)
	assert.Equal(t, int64(1), tableCount(t, pool, "handoffs"))
}

func TestCreateHandoffExactReplayDoesNotTakeGlobalEventLock(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	scope := testScope("tenant-a", "conversation-replay-event-lock")
	insertConversation(t, pool, scope)
	record := prepareCreate(t, repo, scope, "replay-event-lock")
	first, _, err := repo.CreateHandoff(t.Context(), record)
	require.NoError(t, err)
	repo.failpoint = func(step string) error {
		if step == "before_event_lock" {
			return errors.New("replay took global event lock")
		}
		return nil
	}
	replayed, created, err := repo.CreateHandoff(t.Context(), record)
	require.NoError(t, err)
	assert.False(t, created)
	assert.Equal(t, first, replayed)
}

func TestCreateHandoffIdempotencyLockWaitIsContextCancellable(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	scope := testScope("tenant-a", "conversation-create-lock-cancel")
	insertConversation(t, pool, scope)
	record := prepareCreate(t, repo, scope, "create-lock-cancel")
	blocker, err := pool.Begin(t.Context())
	require.NoError(t, err)
	defer func() { _ = blocker.Rollback(context.Background()) }()
	_, err = blocker.Exec(t.Context(), `SELECT pg_advisory_xact_lock($1)`, handoffCreateLockID(scope.TenantID, record.Request.IdempotencyKey))
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	_, _, err = repo.CreateHandoff(ctx, record)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, int64(0), tableCount(t, pool, "handoffs"))
	require.NoError(t, blocker.Rollback(t.Context()))
	_, created, err := repo.CreateHandoff(t.Context(), record)
	require.NoError(t, err)
	assert.True(t, created)
}

func TestExpiredServiceCreateWaitsForConcurrentExactCommitThenReplays(t *testing.T) {
	repoA, pool, _ := repositoryHarness(t)
	repoB, err := NewRepository(pool, clock.NewManual(repositoryTestNow.Add(2*time.Hour)))
	require.NoError(t, err)
	scope := testScope("tenant-a", "conversation-expired-race")
	insertConversation(t, pool, scope)
	record := prepareCreate(t, repoA, scope, "expired-race")
	actualNow := time.Now().UTC()
	record.Request.Deadline = actualNow.Add(time.Hour)
	request := record.Request
	creator, err := handoff.NewService(repoA, clock.NewManual(actualNow), integrationHandoffIDs{
		handoffID: record.HandoffID, eventID: record.EventID,
	})
	require.NoError(t, err)
	expired, err := handoff.NewService(repoB, clock.NewManual(request.Deadline.Add(time.Hour)), integrationHandoffIDs{
		handoffID: "must-not-create", eventID: "must-not-create-event",
	})
	require.NoError(t, err)

	staged := make(chan struct{})
	release := make(chan struct{})
	var stagedOnce sync.Once
	var releaseOnce sync.Once
	repoA.failpoint = func(step string) error {
		if step == "insert_wakeup" {
			stagedOnce.Do(func() { close(staged) })
			select {
			case <-release:
				return nil
			case <-t.Context().Done():
				return t.Context().Err()
			}
		}
		return nil
	}
	cleanup := func() { releaseOnce.Do(func() { close(release) }) }
	defer cleanup()
	type result struct {
		snapshot handoff.Snapshot
		err      error
	}
	creatorDone := make(chan result, 1)
	go func() {
		snapshot, createErr := creator.Create(t.Context(), request)
		creatorDone <- result{snapshot: snapshot, err: createErr}
	}()
	<-staged
	expiredDone := make(chan result, 1)
	go func() {
		snapshot, createErr := expired.Create(t.Context(), request)
		expiredDone <- result{snapshot: snapshot, err: createErr}
	}()

	var expiredResult result
	returnedBeforeCommit := false
	select {
	case expiredResult = <-expiredDone:
		returnedBeforeCommit = true
	case <-time.After(100 * time.Millisecond):
	}
	cleanup()
	creatorResult := <-creatorDone
	if !returnedBeforeCommit {
		expiredResult = <-expiredDone
	}
	require.NoError(t, creatorResult.err)
	require.NoError(t, expiredResult.err)
	assert.False(t, returnedBeforeCommit, "expired replay must serialize behind the in-flight exact creator")
	assert.Equal(t, record.HandoffID, creatorResult.snapshot.ID)
	assert.Equal(t, creatorResult.snapshot, expiredResult.snapshot)
	assert.Equal(t, int64(1), tableCount(t, pool, "handoffs"))
}

func TestCreateHandoffRollbackAfterEveryStatement(t *testing.T) {
	for _, step := range []string{"insert_handoff", "insert_event", "insert_dispatch", "insert_wakeup"} {
		t.Run(step, func(t *testing.T) {
			repo, pool, _ := repositoryHarness(t)
			scope := testScope("tenant-a", "conversation-"+step)
			insertConversation(t, pool, scope)
			record := prepareCreate(t, repo, scope, step)
			repo.failpoint = func(got string) error {
				if got == step {
					return errors.New("injected statement failure")
				}
				return nil
			}
			_, _, err := repo.CreateHandoff(t.Context(), record)
			require.Error(t, err)
			assert.Equal(t, int64(0), tableCount(t, pool, "handoffs"))
			assert.Equal(t, int64(0), tableCount(t, pool, "handoff_events"))
			assert.Equal(t, int64(0), tableCount(t, pool, "outbox"))
		})
	}
}

func TestCreateHandoffCommitResponseLossRetryAndHashConflict(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	scope := testScope("tenant-a", "conversation-1")
	insertConversation(t, pool, scope)
	record := prepareCreate(t, repo, scope, "loss")
	repo.failpoint = func(step string) error {
		if step == "commit" {
			return errors.New("response lost")
		}
		return nil
	}
	_, _, err := repo.CreateHandoff(t.Context(), record)
	require.Error(t, err)
	repo.failpoint = nil
	original, created, err := repo.CreateHandoff(t.Context(), record)
	require.NoError(t, err)
	assert.False(t, created)
	assert.Equal(t, record.HandoffID, original.ID)

	mismatch := record
	mismatch.Request.Objective = "different committed request"
	mismatch.RequestHash, err = handoff.CanonicalRequestHash(mismatch.Request)
	require.NoError(t, err)
	_, _, err = repo.CreateHandoff(t.Context(), mismatch)
	assert.ErrorIs(t, err, handoff.ErrIdempotencyConflict)
	assert.Equal(t, int64(1), tableCount(t, pool, "handoffs"))
}

func TestCreateHandoffRequiresCommittedSourceTurnInSameTransaction(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	scope := testScope("tenant-a", "conversation-1")
	insertConversation(t, pool, scope)
	record := validCreateRecord(scope, "missing")
	_, _, err := repo.CreateHandoff(t.Context(), record)
	assert.ErrorIs(t, err, handoff.ErrUncommittedTurn)
	assert.Equal(t, int64(0), tableCount(t, pool, "handoffs"))

	wrongSession := ConversationItem{SessionID: "different-session", Turn: record.Request.SourceTurn, Sequence: 1,
		Role: "user", Content: "committed elsewhere", ContentHash: "hash", CreatedAt: repositoryTestNow}
	_, err = repo.AppendCommittedItem(t.Context(), scope, wrongSession)
	require.NoError(t, err)
	_, _, err = repo.CreateHandoff(t.Context(), record)
	assert.ErrorIs(t, err, handoff.ErrUncommittedTurn)

	_, err = pool.Exec(t.Context(), `DELETE FROM conversation_items WHERE tenant_id=$1 AND conversation_id=$2`, scope.TenantID, scope.ConversationID)
	require.NoError(t, err)
	matching := wrongSession
	matching.SessionID = record.Request.SessionID
	matching.Sequence = 2
	_, err = repo.AppendCommittedItem(t.Context(), scope, matching)
	require.NoError(t, err)
	created, wasCreated, err := repo.CreateHandoff(t.Context(), record)
	require.NoError(t, err)
	assert.True(t, wasCreated)
	_, err = pool.Exec(t.Context(), `DELETE FROM conversation_items WHERE tenant_id=$1 AND conversation_id=$2`, scope.TenantID, scope.ConversationID)
	require.NoError(t, err)
	replayed, wasCreated, err := repo.CreateHandoff(t.Context(), record)
	require.NoError(t, err)
	assert.False(t, wasCreated)
	assert.Equal(t, created, replayed)
}

func TestCreateHandoffConcurrentDuplicateCreatesOnce(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	scope := testScope("tenant-a", "conversation-1")
	insertConversation(t, pool, scope)
	record := prepareCreate(t, repo, scope, "race")
	const callers = 8
	start := make(chan struct{})
	results := make(chan bool, callers)
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			candidate := record
			candidate.HandoffID = identity.HandoffID(fmt.Sprintf("candidate-%d", i))
			candidate.EventID = fmt.Sprintf("candidate-event-%d", i)
			_, created, err := repo.CreateHandoff(t.Context(), candidate)
			results <- created
			errs <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	createdCount := 0
	for err := range errs {
		require.NoError(t, err)
	}
	for created := range results {
		if created {
			createdCount++
		}
	}
	assert.Equal(t, 1, createdCount)
	assert.Equal(t, int64(1), tableCount(t, pool, "handoffs"))
	assert.Equal(t, int64(1), tableCount(t, pool, "handoff_events"))
	assert.Equal(t, int64(2), tableCount(t, pool, "outbox"))
}

func TestGetAndListActiveAreScopeExact(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	scope := testScope("tenant-a", "shared")
	otherTenant := testScope("tenant-b", "shared")
	insertConversation(t, pool, scope)
	insertConversation(t, pool, otherTenant)
	first := prepareCreate(t, repo, scope, "a")
	second := prepareCreate(t, repo, otherTenant, "b")
	_, _, err := repo.CreateHandoff(t.Context(), first)
	require.NoError(t, err)
	_, _, err = repo.CreateHandoff(t.Context(), second)
	require.NoError(t, err)

	got, err := repo.GetHandoff(t.Context(), scope, first.HandoffID)
	require.NoError(t, err)
	assert.Equal(t, first.HandoffID, got.ID)
	wrongUser := scope
	wrongUser.UserID = "other-user"
	_, err = repo.GetHandoff(t.Context(), wrongUser, first.HandoffID)
	assert.ErrorIs(t, err, handoff.ErrNotFound)
	active, err := repo.ListActive(t.Context(), scope)
	require.NoError(t, err)
	require.Len(t, active, 1)
	assert.Equal(t, first.HandoffID, active[0].ID)
}

func TestCreateHandoffAllowsSameIDInDistinctTenants(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	scopeA := testScope("tenant-a", "shared")
	scopeB := testScope("tenant-b", "shared")
	insertConversation(t, pool, scopeA)
	insertConversation(t, pool, scopeB)
	recordA := prepareCreate(t, repo, scopeA, "same")
	recordB := prepareCreate(t, repo, scopeB, "same")
	recordB.HandoffID = recordA.HandoffID
	recordB.EventID = "event-other-tenant"
	_, created, err := repo.CreateHandoff(t.Context(), recordA)
	require.NoError(t, err)
	assert.True(t, created)
	_, created, err = repo.CreateHandoff(t.Context(), recordB)
	require.NoError(t, err)
	assert.True(t, created)
	assert.Equal(t, int64(2), tableCount(t, pool, "handoffs"))
}

func TestSteerInputAndCancelMutationsAreDurableAndIdempotent(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	scope := testScope("tenant-a", "conversation-1")
	insertConversation(t, pool, scope)
	record := prepareCreate(t, repo, scope, "mutate")
	_, _, err := repo.CreateHandoff(t.Context(), record)
	require.NoError(t, err)

	steer := handoff.SteerRequest{Objective: "revised objective", ExpectedRevision: 1, Actor: "user", IdempotencyKey: "steer-1"}
	steered, err := repo.SteerHandoff(t.Context(), scope, record.HandoffID, steer)
	require.NoError(t, err)
	assert.Equal(t, uint32(1), steered.ObjectiveRevision)
	assert.Equal(t, "revised objective", steered.Request.Objective)
	replayed, err := repo.SteerHandoff(t.Context(), scope, record.HandoffID, steer)
	require.NoError(t, err)
	assert.Equal(t, steered, replayed)
	changedSteer := steer
	changedSteer.Objective = "different objective"
	_, err = repo.SteerHandoff(t.Context(), scope, record.HandoffID, changedSteer)
	assert.ErrorIs(t, err, handoff.ErrIdempotencyConflict)

	_, err = pool.Exec(t.Context(), `UPDATE handoffs SET state='needs_input' WHERE tenant_id=$1 AND conversation_id=$2 AND id=$3`,
		scope.TenantID, scope.ConversationID, record.HandoffID)
	require.NoError(t, err)
	input := handoff.InputRequest{Input: json.RawMessage(`{"choice":"safe"}`), ExpectedRevision: 1, Actor: "user", IdempotencyKey: "input-1"}
	provided, err := repo.ProvideHandoffInput(t.Context(), scope, record.HandoffID, input)
	require.NoError(t, err)
	assert.Equal(t, uint32(1), provided.InputRevision)
	_, err = repo.ProvideHandoffInput(t.Context(), scope, record.HandoffID, input)
	require.NoError(t, err)
	changedInput := input
	changedInput.Input = json.RawMessage(`{"choice":"unsafe"}`)
	_, err = repo.ProvideHandoffInput(t.Context(), scope, record.HandoffID, changedInput)
	assert.ErrorIs(t, err, handoff.ErrIdempotencyConflict)

	cancel := handoff.CancelRequest{Actor: "user", Reason: "no longer needed", IdempotencyKey: "cancel-1"}
	cancelled, err := repo.RequestHandoffCancel(t.Context(), scope, record.HandoffID, cancel)
	require.NoError(t, err)
	assert.Equal(t, handoff.StateNeedsInput, cancelled.State, "request remains active for the worker to terminalize")
	_, err = repo.RequestHandoffCancel(t.Context(), scope, record.HandoffID, cancel)
	require.NoError(t, err)
	var requestedAt *time.Time
	var actor, reason string
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT cancel_requested_at,cancel_actor,cancel_reason FROM handoffs
		WHERE tenant_id=$1 AND conversation_id=$2 AND id=$3`, scope.TenantID, scope.ConversationID, record.HandoffID).
		Scan(&requestedAt, &actor, &reason))
	assert.NotNil(t, requestedAt)
	assert.Equal(t, "user", actor)
	assert.Equal(t, "no longer needed", reason)

	var eventCount, wakeupCount int64
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM handoff_events WHERE handoff_id=$1`, record.HandoffID).Scan(&eventCount))
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM outbox WHERE aggregate_id=$1`, record.HandoffID).Scan(&wakeupCount))
	assert.Equal(t, int64(4), eventCount)
	assert.Equal(t, int64(5), wakeupCount) // two create rows plus one per mutation
}

func TestSteerAndCancelOrderingPreservesLatestRevisionAndCommittedReplay(t *testing.T) {
	t.Run("steer first then cancel terminalizes latest revision", func(t *testing.T) {
		repo, _, _ := repositoryHarness(t)
		record, lease, now := runningLease(t, repo, "steer-before-cancel")
		steer := handoff.SteerRequest{Objective: "latest", ExpectedRevision: 1, Actor: "user", IdempotencyKey: "steer-before-cancel"}
		_, err := repo.SteerHandoff(t.Context(), record.Request.Scope, record.HandoffID, steer)
		require.NoError(t, err)
		_, err = repo.RequestHandoffCancel(t.Context(), record.Request.Scope, record.HandoffID, handoff.CancelRequest{Actor: "user", Reason: "stop", IdempotencyKey: "cancel-after-steer"})
		require.NoError(t, err)
		control, err := repo.Heartbeat(t.Context(), lease.Token, now.Add(time.Minute))
		require.NoError(t, err)
		assert.True(t, control.CancelRequested)
		assert.Equal(t, uint32(1), control.ObjectiveRevision)
		require.NoError(t, repo.MarkCancelled(t.Context(), lease.Token, worker.CancelledRecord{MutationID: "cancel-latest", ExpectedObjectiveRevision: 1}))
	})

	t.Run("cancel first rejects new steer but replays prior committed steer", func(t *testing.T) {
		repo, pool, _ := repositoryHarness(t)
		scope := testScope("tenant-a", "conversation-cancel-steer")
		insertConversation(t, pool, scope)
		record := prepareCreate(t, repo, scope, "cancel-steer-replay")
		_, _, err := repo.CreateHandoff(t.Context(), record)
		require.NoError(t, err)
		committed := handoff.SteerRequest{Objective: "committed", ExpectedRevision: 1, Actor: "user", IdempotencyKey: "committed-steer"}
		_, err = repo.SteerHandoff(t.Context(), scope, record.HandoffID, committed)
		require.NoError(t, err)
		_, err = repo.RequestHandoffCancel(t.Context(), scope, record.HandoffID, handoff.CancelRequest{Actor: "user", Reason: "stop", IdempotencyKey: "cancel-before-new-steer"})
		require.NoError(t, err)
		replayed, err := repo.SteerHandoff(t.Context(), scope, record.HandoffID, committed)
		require.NoError(t, err)
		assert.Equal(t, uint32(1), replayed.ObjectiveRevision)
		assert.Equal(t, "committed", replayed.Request.Objective)
		_, err = repo.SteerHandoff(t.Context(), scope, record.HandoffID, handoff.SteerRequest{Objective: "must reject", ExpectedRevision: 2, Actor: "user", IdempotencyKey: "new-steer-after-cancel"})
		assert.ErrorIs(t, err, handoff.ErrSteeringBlocked)
		var revision int64
		require.NoError(t, pool.QueryRow(t.Context(), `SELECT objective_revision FROM handoffs WHERE id=$1`, record.HandoffID).Scan(&revision))
		assert.Equal(t, int64(1), revision)
	})
}

func TestMutationRevisionStateConcurrencyAndTerminalProtection(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	scope := testScope("tenant-a", "conversation-1")
	insertConversation(t, pool, scope)
	record := prepareCreate(t, repo, scope, "guard")
	_, _, err := repo.CreateHandoff(t.Context(), record)
	require.NoError(t, err)

	_, err = repo.ProvideHandoffInput(t.Context(), scope, record.HandoffID,
		handoff.InputRequest{Input: json.RawMessage(`{"x":1}`), ExpectedRevision: 1, Actor: "user", IdempotencyKey: "too-early"})
	assert.ErrorIs(t, err, handoff.ErrInvalidTransition)
	_, err = repo.SteerHandoff(t.Context(), scope, record.HandoffID,
		handoff.SteerRequest{Objective: "wrong revision", ExpectedRevision: 2, Actor: "user", IdempotencyKey: "wrong-rev"})
	assert.ErrorIs(t, err, handoff.ErrRevisionConflict)

	start := make(chan struct{})
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func(i int) {
			<-start
			_, err := repo.SteerHandoff(t.Context(), scope, record.HandoffID,
				handoff.SteerRequest{Objective: fmt.Sprintf("objective-%d", i), ExpectedRevision: 1,
					Actor: "user", IdempotencyKey: fmt.Sprintf("concurrent-%d", i)})
			errs <- err
		}(i)
	}
	close(start)
	var successes, conflicts int
	for i := 0; i < 2; i++ {
		err := <-errs
		if err == nil {
			successes++
		} else if errors.Is(err, handoff.ErrRevisionConflict) {
			conflicts++
		} else {
			require.NoError(t, err)
		}
	}
	assert.Equal(t, 1, successes)
	assert.Equal(t, 1, conflicts)

	_, err = pool.Exec(t.Context(), `UPDATE handoffs SET state='cancelled', terminal_at=$4, terminal_mutation_id='terminal'
		WHERE tenant_id=$1 AND conversation_id=$2 AND id=$3`, scope.TenantID, scope.ConversationID, record.HandoffID, repositoryTestNow)
	require.NoError(t, err)
	_, err = repo.RequestHandoffCancel(t.Context(), scope, record.HandoffID,
		handoff.CancelRequest{Actor: "user", IdempotencyKey: "after-terminal"})
	assert.ErrorIs(t, err, handoff.ErrTerminalState)
	_, err = repo.SteerHandoff(t.Context(), scope, record.HandoffID,
		handoff.SteerRequest{Objective: "resurrect", ExpectedRevision: 2, Actor: "user", IdempotencyKey: "resurrect"})
	assert.ErrorIs(t, err, handoff.ErrTerminalState)
}

func TestConcurrentSameKeyMutationReplaysCommittedSnapshot(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	scope := testScope("tenant-a", "conversation-1")
	insertConversation(t, pool, scope)
	record := prepareCreate(t, repo, scope, "same-key")
	_, _, err := repo.CreateHandoff(t.Context(), record)
	require.NoError(t, err)
	request := handoff.SteerRequest{Objective: "single objective", ExpectedRevision: 1, Actor: "user", IdempotencyKey: "same-key"}
	start := make(chan struct{})
	results := make(chan handoff.Snapshot, 8)
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		go func() {
			<-start
			snapshot, err := repo.SteerHandoff(t.Context(), scope, record.HandoffID, request)
			results <- snapshot
			errs <- err
		}()
	}
	close(start)
	var original handoff.Snapshot
	for i := 0; i < 8; i++ {
		require.NoError(t, <-errs)
		snapshot := <-results
		if i == 0 {
			original = snapshot
		} else {
			assert.Equal(t, original, snapshot)
		}
	}
	var mutationEvents int64
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM handoff_events WHERE kind='handoff.steered'`).Scan(&mutationEvents))
	assert.Equal(t, int64(1), mutationEvents)
}

func TestMutationFailureAfterEachStatementRollsBack(t *testing.T) {
	for _, step := range []string{"mutation_update", "mutation_event", "mutation_wakeup"} {
		t.Run(step, func(t *testing.T) {
			repo, pool, _ := repositoryHarness(t)
			scope := testScope("tenant-a", "conversation-"+step)
			insertConversation(t, pool, scope)
			record := prepareCreate(t, repo, scope, step)
			_, _, err := repo.CreateHandoff(t.Context(), record)
			require.NoError(t, err)
			repo.failpoint = func(got string) error {
				if got == step {
					return errors.New("injected")
				}
				return nil
			}
			_, err = repo.SteerHandoff(t.Context(), scope, record.HandoffID,
				handoff.SteerRequest{Objective: "changed", ExpectedRevision: 1, Actor: "user", IdempotencyKey: "rollback"})
			require.Error(t, err)
			got, err := repo.GetHandoff(t.Context(), scope, record.HandoffID)
			require.NoError(t, err)
			assert.Zero(t, got.ObjectiveRevision)
			assert.Equal(t, record.Request.Objective, got.Request.Objective)
			assert.Equal(t, int64(1), tableCount(t, pool, "handoff_events"))
			assert.Equal(t, int64(2), tableCount(t, pool, "outbox"))
		})
	}
}

func TestMutationRejectsPersistedRevisionOutsideUint32(t *testing.T) {
	for _, tc := range []struct {
		name   string
		setup  func(*testing.T, *Repository, handoff.Scope, identity.HandoffID)
		mutate func(*testing.T, *Repository, handoff.Scope, identity.HandoffID) error
	}{
		{
			name: "objective revision",
			setup: func(t *testing.T, repo *Repository, scope handoff.Scope, id identity.HandoffID) {
				_, err := repo.pool.Exec(t.Context(), `UPDATE handoffs SET objective_revision=4294967296
					WHERE tenant_id=$1 AND conversation_id=$2 AND id=$3`, scope.TenantID, scope.ConversationID, id)
				require.NoError(t, err)
			},
			mutate: func(t *testing.T, repo *Repository, scope handoff.Scope, id identity.HandoffID) error {
				_, err := repo.SteerHandoff(t.Context(), scope, id, handoff.SteerRequest{
					Objective: "must not reset", ExpectedRevision: 1, Actor: "user", IdempotencyKey: "overflow"})
				return err
			},
		},
		{
			name: "input revision",
			setup: func(t *testing.T, repo *Repository, scope handoff.Scope, id identity.HandoffID) {
				_, err := repo.pool.Exec(t.Context(), `UPDATE handoffs SET state='needs_input',input_revision=4294967296,input='{}'::json
					WHERE tenant_id=$1 AND conversation_id=$2 AND id=$3`, scope.TenantID, scope.ConversationID, id)
				require.NoError(t, err)
			},
			mutate: func(t *testing.T, repo *Repository, scope handoff.Scope, id identity.HandoffID) error {
				_, err := repo.ProvideHandoffInput(t.Context(), scope, id, handoff.InputRequest{
					Input: json.RawMessage(`{"answer":1}`), ExpectedRevision: 1, Actor: "user", IdempotencyKey: "overflow"})
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo, pool, _ := repositoryHarness(t)
			scope := testScope("tenant-a", "conversation-1")
			insertConversation(t, pool, scope)
			record := prepareCreate(t, repo, scope, "overflow")
			_, _, err := repo.CreateHandoff(t.Context(), record)
			require.NoError(t, err)
			tc.setup(t, repo, scope, record.HandoffID)
			eventsBefore, outboxBefore := tableCount(t, pool, "handoff_events"), tableCount(t, pool, "outbox")
			err = tc.mutate(t, repo, scope, record.HandoffID)
			require.Error(t, err)
			assert.Equal(t, eventsBefore, tableCount(t, pool, "handoff_events"))
			assert.Equal(t, outboxBefore, tableCount(t, pool, "outbox"))
			var revision int64
			column := "objective_revision"
			if tc.name == "input revision" {
				column = "input_revision"
			}
			require.NoError(t, pool.QueryRow(t.Context(), `SELECT `+column+` FROM handoffs
				WHERE tenant_id=$1 AND conversation_id=$2 AND id=$3`, scope.TenantID, scope.ConversationID, record.HandoffID).Scan(&revision))
			assert.Equal(t, int64(4294967296), revision)
		})
	}
}

func TestEventAppendSequenceIsCommitOrdered(t *testing.T) {
	repoA, pool, repositoryClock := repositoryHarness(t)
	repoB, err := NewRepository(pool, repositoryClock)
	require.NoError(t, err)
	scope := testScope("tenant-a", "conversation-1")
	insertConversation(t, pool, scope)
	recordA := prepareCreate(t, repoA, scope, "ordered-a")
	recordB := prepareCreate(t, repoA, scope, "ordered-b")
	aInserted := make(chan struct{})
	releaseA := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseA) }) }
	defer release()
	repoA.failpoint = func(step string) error {
		if step == "insert_event" {
			close(aInserted)
			select {
			case <-releaseA:
				return nil
			case <-t.Context().Done():
				return t.Context().Err()
			}
		}
		return nil
	}
	aDone := make(chan error, 1)
	go func() { _, _, err := repoA.CreateHandoff(t.Context(), recordA); aDone <- err }()
	<-aInserted
	bDone := make(chan error, 1)
	go func() { _, _, err := repoB.CreateHandoff(t.Context(), recordB); bDone <- err }()

	for {
		select {
		case err := <-bDone:
			require.NoError(t, err)
			t.Fatal("higher sequence transaction committed while lower sequence transaction was blocked")
		default:
		}
		var waiting int
		require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM pg_locks
			WHERE locktype='advisory' AND NOT granted`).Scan(&waiting))
		if waiting > 0 {
			break
		}
		runtime.Gosched()
	}
	before, err := repoA.ReadEvents(t.Context(), scope, 0, 10)
	require.NoError(t, err)
	assert.Empty(t, before)
	release()
	require.NoError(t, <-aDone)
	require.NoError(t, <-bDone)
	events, err := repoA.ReadEvents(t.Context(), scope, 0, 10)
	require.NoError(t, err)
	require.Len(t, events, 2)
	assert.Equal(t, recordA.EventID, events[0].EventID)
	assert.Equal(t, recordB.EventID, events[1].EventID)
	assert.Less(t, events[0].Seq, events[1].Seq)
	require.NoError(t, repoA.AdvanceDeliveryCursor(t.Context(), "ordered", scope, events[0].Seq))
	require.NoError(t, repoA.AdvanceDeliveryCursor(t.Context(), "ordered", scope, events[1].Seq))
}

func TestEventAppendAdvisoryLockWaitIsContextCancellable(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	scope := testScope("tenant-a", "conversation-1")
	insertConversation(t, pool, scope)
	record := prepareCreate(t, repo, scope, "lock-cancel")
	blocker, err := pool.Begin(t.Context())
	require.NoError(t, err)
	defer func() { _ = blocker.Rollback(context.Background()) }()
	_, err = blocker.Exec(t.Context(), `SELECT pg_advisory_xact_lock($1)`, eventAppendAdvisoryLock)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { _, _, err := repo.CreateHandoff(ctx, record); done <- err }()
	for {
		select {
		case err := <-done:
			require.Error(t, err)
			t.Fatal("event append returned before blocked advisory lock was cancelled")
		default:
		}
		var waiting int
		require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM pg_locks
			WHERE locktype='advisory' AND NOT granted`).Scan(&waiting))
		if waiting > 0 {
			break
		}
		runtime.Gosched()
	}
	cancel()
	assert.ErrorIs(t, <-done, context.Canceled)
	assert.Equal(t, int64(0), tableCount(t, pool, "handoffs"))
	assert.Equal(t, int64(0), tableCount(t, pool, "handoff_events"))
	assert.Equal(t, int64(0), tableCount(t, pool, "outbox"))
}
