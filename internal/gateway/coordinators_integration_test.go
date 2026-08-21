//go:build integration

package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/feature"
	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/protocol"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/chuanmingliu/openai-gpt-live/internal/session"
	"github.com/chuanmingliu/openai-gpt-live/internal/store/postgres"
	"github.com/chuanmingliu/openai-gpt-live/internal/testkit"
	"github.com/chuanmingliu/openai-gpt-live/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type coordinatorIDs struct {
	handoffID identity.HandoffID
	eventID   string
}

type commitResponseLossRepository struct {
	handoff.Repository
	err error
}

func (r commitResponseLossRepository) CreateHandoff(ctx context.Context, record handoff.CreateRecord) (handoff.Snapshot, bool, error) {
	snapshot, created, err := r.Repository.CreateHandoff(ctx, record)
	if err != nil {
		return snapshot, created, err
	}
	return handoff.Snapshot{}, false, r.err
}

func (g coordinatorIDs) NewHandoffID() identity.HandoffID { return g.handoffID }
func (g coordinatorIDs) NewEventID() string               { return g.eventID }

func gatewayRepositoryHarness(t *testing.T) (*postgres.Repository, *pgxpool.Pool, *clock.Manual) {
	t.Helper()
	pool := testkit.Postgres(t)
	require.NoError(t, migrations.Up(t.Context(), pool))
	now := time.Now().UTC()
	manual := clock.NewManual(now)
	repository, err := postgres.NewRepository(pool, manual)
	require.NoError(t, err)
	return repository, pool, manual
}

func insertGatewayConversation(t *testing.T, pool *pgxpool.Pool, scope handoff.Scope, now time.Time) {
	t.Helper()
	_, err := pool.Exec(t.Context(), `
		INSERT INTO conversations (tenant_id,id,user_id,retention_until,created_at,updated_at)
		VALUES ($1,$2,$3,$4,$5,$5)`, scope.TenantID, scope.ConversationID, scope.UserID, now.Add(24*time.Hour), now)
	require.NoError(t, err)
}

func TestTwoAppsSharingPostgresCannotConsumeForeignAffinity(t *testing.T) {
	repository, pool, manual := gatewayRepositoryHarness(t)
	scope := handoff.Scope{TenantID: "tenant-affinity", UserID: "user-affinity", ConversationID: "conversation-affinity"}
	insertGatewayConversation(t, pool, scope, manual.Now())
	work := validTranscriptWork()
	work.Scope, work.SessionID, work.Incarnation = scope, "session-affinity", 4
	payload, err := json.Marshal(work)
	require.NoError(t, err)
	_, err = repository.AdmitCoordinatorWork(t.Context(), postgres.CoordinatorJournalRecord{
		WorkKey: transcriptWorkKey(work), Kind: postgres.CoordinatorKindTranscript, Scope: scope,
		SessionID: work.SessionID, Incarnation: work.Incarnation, LocalSequence: work.Sequence,
		DeliveryAffinity: "app-a", Payload: payload,
	})
	require.NoError(t, err)

	foreign := NewTranscriptCoordinator(repository, &submitterFake{err: session.ErrSessionNotActive}, 1)
	require.NoError(t, foreign.configureJournal(repository, "app-b", "worker-b", 20*time.Millisecond))
	foreignCtx, foreignCancel := context.WithTimeout(t.Context(), 70*time.Millisecond)
	require.NoError(t, foreign.Run(foreignCtx))
	foreignCancel()
	var itemCount int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM conversation_items`).Scan(&itemCount))
	assert.Zero(t, itemCount)
	var delivered bool
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT delivered FROM coordinator_work_journal WHERE work_key=$1`, transcriptWorkKey(work)).Scan(&delivered))
	assert.False(t, delivered)

	inputs := make(chan session.Input, 1)
	host := NewTranscriptCoordinator(repository, &submitterFake{inputs: inputs}, 1)
	require.NoError(t, host.configureJournal(repository, "app-a", "worker-a", 20*time.Millisecond))
	hostCtx, hostCancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- host.Run(hostCtx) }()
	committed := (<-inputs).(session.TurnCommittedInput)
	assert.Equal(t, work.Sequence, committed.Sequence)
	require.Eventually(t, func() bool {
		return pool.QueryRow(t.Context(), `SELECT delivered FROM coordinator_work_journal WHERE work_key=$1`, transcriptWorkKey(work)).Scan(&delivered) == nil && delivered
	}, time.Second, time.Millisecond)
	hostCancel()
	require.NoError(t, <-done)
}

func TestTranscriptCoordinatorDiscardsConflictingReplayWithoutChangingCommittedTurn(t *testing.T) {
	repository, pool, manual := gatewayRepositoryHarness(t)
	scope := handoff.Scope{TenantID: "tenant-conflict", UserID: "user-conflict", ConversationID: "conversation-conflict"}
	insertGatewayConversation(t, pool, scope, manual.Now())
	work := validTranscriptWork()
	work.Scope, work.SessionID, work.Incarnation = scope, "session-conflict", 4
	work.Turn = identity.TurnKey{ID: "turn-conflict", Revision: 1}
	work.FinalText = "conflicting later final"
	_, err := repository.AppendCommittedItem(t.Context(), scope, postgres.ConversationItem{
		SessionID: work.SessionID, Turn: work.Turn, Sequence: 1, Role: work.Role,
		Content: "authoritative first final", ContentHash: "authoritative-hash", CreatedAt: manual.Now(),
	})
	require.NoError(t, err)
	payload, err := json.Marshal(work)
	require.NoError(t, err)
	_, err = repository.AdmitCoordinatorWork(t.Context(), postgres.CoordinatorJournalRecord{
		WorkKey: transcriptWorkKey(work), Kind: postgres.CoordinatorKindTranscript, Scope: scope,
		SessionID: work.SessionID, Incarnation: work.Incarnation, LocalSequence: work.Sequence,
		DeliveryAffinity: "app-a", Payload: payload,
	})
	require.NoError(t, err)

	inputs := make(chan session.Input, 1)
	coordinator := NewTranscriptCoordinator(repository, &submitterFake{inputs: inputs}, 1)
	require.NoError(t, coordinator.configureJournal(repository, "app-a", "worker-a", 200*time.Millisecond))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- coordinator.Run(ctx) }()
	require.Eventually(t, func() bool {
		var count int
		return pool.QueryRow(t.Context(), `SELECT count(*) FROM coordinator_work_journal WHERE work_key=$1`, transcriptWorkKey(work)).Scan(&count) == nil && count == 0
	}, 2*time.Second, time.Millisecond)
	assert.Empty(t, inputs)
	var content string
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT content FROM conversation_items WHERE tenant_id=$1 AND conversation_id=$2`, scope.TenantID, scope.ConversationID).Scan(&content))
	assert.Equal(t, "authoritative first final", content)
	cancel()
	require.NoError(t, <-done)
}

func TestDisabledQueuedCreationWritesNoHandoffEventOutboxOrJournalRows(t *testing.T) {
	repository, pool, manual := gatewayRepositoryHarness(t)
	service, err := handoff.NewService(repository, manual, coordinatorIDs{handoffID: "must-not-create", eventID: "must-not-create"})
	require.NoError(t, err)
	inputs := make(chan session.Input, 1)
	coordinator := NewHandoffCoordinator(service, &submitterFake{inputs: inputs}, 1)
	require.NoError(t, coordinator.SetFeatureGates(staticFeatureGates{feature.HandoffsEnabled: false}))
	require.NoError(t, coordinator.configureJournal(repository, "disabled-affinity", "disabled-owner", 100*time.Millisecond))
	work := validHandoffWork()
	require.NoError(t, coordinator.Enqueue(work))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- coordinator.Run(ctx) }()
	rejected := (<-inputs).(session.HandoffAcceptedInput)
	assert.False(t, rejected.Accepted)
	assert.Equal(t, "handoffs_disabled", rejected.Reason)
	for _, table := range []string{"handoffs", "handoff_events", "outbox", "coordinator_work_journal"} {
		var count int
		require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM `+table).Scan(&count))
		assert.Zero(t, count, table)
	}
	cancel()
	require.NoError(t, <-done)
}

func TestHandoffAdmissionRaceDisableCommittedBeforeCreateWritesNothing(t *testing.T) {
	pool := testkit.Postgres(t)
	require.NoError(t, migrations.Up(t.Context(), pool))
	manual := clock.NewManual(time.Now().UTC())
	admissionReached := make(chan struct{})
	resumeAdmission := make(chan struct{})
	var once sync.Once
	repository, err := postgres.NewRepository(pool, manual, postgres.WithHandoffAdmissionHook(func() {
		once.Do(func() { close(admissionReached) })
		<-resumeAdmission
	}))
	require.NoError(t, err)
	work := validHandoffWork()
	work.Scope = handoff.Scope{TenantID: "tenant-admission-race", UserID: "user-admission-race", ConversationID: "conversation-admission-race"}
	work.SessionID = "session-admission-race"
	work.Turn = identity.TurnKey{ID: "turn-admission-race", Revision: 1}
	work.Snapshot.Deadline = manual.Now().Add(time.Hour)
	insertGatewayConversation(t, pool, work.Scope, manual.Now())
	_, err = repository.AppendCommittedItem(t.Context(), work.Scope, postgres.ConversationItem{
		SessionID: work.SessionID, Turn: work.Turn, Sequence: 1, Role: "user",
		Content: work.Transcript, ContentHash: "hash-admission-race", CreatedAt: manual.Now(),
	})
	require.NoError(t, err)
	service, err := handoff.NewService(repository, manual, coordinatorIDs{handoffID: "must-not-create-race", eventID: "must-not-create-race"})
	require.NoError(t, err)
	inputs := make(chan session.Input, 1)
	coordinator := NewHandoffCoordinator(service, &submitterFake{inputs: inputs}, 1)
	require.NoError(t, coordinator.SetFeatureGates(staticFeatureGates{feature.HandoffsEnabled: true}))
	require.NoError(t, coordinator.configureJournal(repository, "admission-race-affinity", "admission-race-owner", time.Second))
	require.NoError(t, coordinator.Enqueue(work))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- coordinator.Run(ctx) }()
	select {
	case <-admissionReached:
	case <-time.After(5 * time.Second):
		t.Fatal("create did not pause at transaction-coupled feature admission")
	}
	store, err := feature.NewPostgres(pool, clock.Real{}, 10*time.Millisecond)
	require.NoError(t, err)
	disabled, err := store.Set(t.Context(), feature.HandoffsEnabled, false, "operator", "admission-race")
	require.NoError(t, err)
	assert.False(t, disabled.Values[feature.HandoffsEnabled])
	close(resumeAdmission)
	rejected := (<-inputs).(session.HandoffAcceptedInput)
	assert.False(t, rejected.Accepted)
	assert.Equal(t, "handoffs_disabled", rejected.Reason)
	for _, table := range []string{"handoffs", "handoff_events", "outbox", "coordinator_work_journal"} {
		var count int
		require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM `+table).Scan(&count))
		assert.Zero(t, count, table)
	}
	cancel()
	require.NoError(t, <-done)
}

func TestAppsLeaseStableCoordinatorShardAndReplacementTakesOver(t *testing.T) {
	repository, pool, manual := gatewayRepositoryHarness(t)
	service, err := handoff.NewService(repository, manual, coordinatorIDs{handoffID: "unused-handoff", eventID: "unused-event"})
	require.NoError(t, err)
	newApp := func(affinity, owner string) *App {
		cfg := validConfig()
		cfg.Features.Handoffs = true
		app, appErr := New(cfg, testkit.NewFakeConnector(1), WithIDGenerator(deterministicIDs{}), WithDurableHandoffs(DurableDependencies{
			Service: service, ConversationWriter: repository, ConversationProvisioner: repository, ConversationTTL: 24 * time.Hour, Journal: repository,
			DeliveryAffinity: affinity, JournalOwner: owner, JournalClaimLease: 100 * time.Millisecond,
			HandoffQueueCapacity: 1, TranscriptQueueCapacity: 1, Snapshot: validHandoffWork().Snapshot,
		}), testHandoffPresentationOption())
		require.NoError(t, appErr)
		app.listener = newBlockingListener()
		return app
	}
	start := func(app *App) (context.CancelFunc, <-chan error) {
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- app.Run(ctx) }()
		return cancel, done
	}
	waitOwner := func(affinity, owner string) uint64 {
		var current string
		var generation uint64
		require.Eventually(t, func() bool {
			err := pool.QueryRow(t.Context(), `SELECT lease_owner,lease_generation
				FROM coordinator_delivery_shards WHERE delivery_affinity=$1`, affinity).Scan(&current, &generation)
			return err == nil && current == owner
		}, 2*time.Second, 5*time.Millisecond)
		return generation
	}

	firstApp := newApp("shared-affinity", "first")
	firstCancel, firstDone := start(firstApp)
	firstGeneration := waitOwner("shared-affinity", "first")
	duplicate := newApp("shared-affinity", "second")
	duplicateCancel, duplicateDone := start(duplicate)
	select {
	case duplicateErr := <-duplicateDone:
		t.Fatalf("standby app exited while the shard owner was live: %v", duplicateErr)
	case <-time.After(150 * time.Millisecond):
	}
	assert.Equal(t, firstGeneration, waitOwner("shared-affinity", "first"))

	otherApp := newApp("other-affinity", "other")
	otherCancel, otherDone := start(otherApp)
	waitOwner("other-affinity", "other")

	firstCancel()
	require.NoError(t, <-firstDone)
	replacementGeneration := waitOwner("shared-affinity", "second")
	assert.Greater(t, replacementGeneration, firstGeneration)
	duplicateCancel()
	require.NoError(t, <-duplicateDone)
	otherCancel()
	require.NoError(t, <-otherDone)
}

func TestPostgresRestartAfterCreateAndDeadlineReturnsSameIDExactlyOnce(t *testing.T) {
	repository, pool, manual := gatewayRepositoryHarness(t)
	scope := handoff.Scope{TenantID: "tenant-replay", UserID: "user-replay", ConversationID: "conversation-replay"}
	insertGatewayConversation(t, pool, scope, manual.Now())
	work := validHandoffWork()
	work.Scope, work.SessionID, work.Incarnation = scope, "session-replay", 5
	work.Turn = identity.TurnKey{ID: "turn-replay", Revision: 1}
	work.Transcript = "durable request"
	work.Snapshot.Deadline = manual.Now().Add(time.Minute)
	_, err := repository.AppendCommittedItem(t.Context(), scope, postgres.ConversationItem{
		SessionID: work.SessionID, Turn: work.Turn, Sequence: 1, Role: "user",
		Content: work.Transcript, ContentHash: "hash-replay", CreatedAt: manual.Now(),
	})
	require.NoError(t, err)
	service, err := handoff.NewService(repository, manual, coordinatorIDs{handoffID: "handoff-original", eventID: "event-original"})
	require.NoError(t, err)

	crash := errors.New("crash before live acknowledgement")
	first := NewHandoffCoordinator(service, &submitterFake{err: crash}, 1)
	require.NoError(t, first.configureJournal(repository, "app-a", "worker-before-crash", 200*time.Millisecond))
	require.NoError(t, first.Enqueue(work))
	require.ErrorIs(t, first.Run(t.Context()), crash)
	manual.Advance(2 * time.Minute)

	reactor := session.NewReactor(session.IDs{
		SessionID: work.SessionID, ConversationID: work.Scope.ConversationID, Incarnation: work.Incarnation,
	}, session.NewEpochFence(work.OriginEpoch-1))
	applyGatewayReactor(t, reactor, session.TurnCommittedInput{Sequence: 1, Turn: work.Turn, Transcript: work.Transcript})
	applyGatewayReactor(t, reactor, session.ClientInput{Event: protocol.ResponseCreateEvent{Type: "response.create"}})
	applyGatewayReactor(t, reactor, session.VoiceInput{Incarnation: work.Incarnation, Event: realtime.Event{
		Kind: realtime.EventResponseCreated, Epoch: work.OriginEpoch, ResponseID: work.ResponseID, Status: "in_progress",
	}})
	applyGatewayReactor(t, reactor, session.VoiceInput{Incarnation: work.Incarnation, Event: realtime.Event{
		Kind: realtime.EventFunctionCallDone, Epoch: work.OriginEpoch, ResponseID: work.ResponseID,
		ItemID: "tool-item", CallID: work.CallID, Name: work.ToolName, Arguments: work.Arguments,
	}})
	applyGatewayReactor(t, reactor, session.VoiceInput{Incarnation: work.Incarnation, Event: realtime.Event{
		Kind: realtime.EventResponseDone, Epoch: work.OriginEpoch, ResponseID: work.ResponseID, Status: "completed",
	}})
	submit := &reactorSubmitter{reactor: reactor}
	second := NewHandoffCoordinator(service, submit, 1)
	require.NoError(t, second.configureJournal(repository, "app-a", "worker-after-crash", 200*time.Millisecond))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- second.Run(ctx) }()
	require.Eventually(t, func() bool { return submit.itemCount() == 1 }, 2*time.Second, time.Millisecond)
	assert.Equal(t, 1, submit.itemCount())
	var shardOwner string
	var shardGeneration uint64
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT lease_owner,lease_generation
		FROM coordinator_delivery_shards WHERE delivery_affinity='app-a'`).Scan(&shardOwner, &shardGeneration))
	assert.Equal(t, "worker-after-crash", shardOwner)
	assert.Greater(t, shardGeneration, uint64(1))
	var delivered bool
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT delivered FROM coordinator_work_journal
		WHERE work_key=$1`, handoffWorkKey(mustCanonicalHandoffWork(t, work))).Scan(&delivered))
	assert.True(t, delivered)
	var handoffCount int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM handoffs WHERE id='handoff-original'`).Scan(&handoffCount))
	assert.Equal(t, 1, handoffCount)
	cancel()
	require.NoError(t, <-done)
}

func TestPostgresCommitResponseLossRemainsReplayableUntilPositiveRestart(t *testing.T) {
	repository, pool, manual := gatewayRepositoryHarness(t)
	scope := handoff.Scope{TenantID: "tenant-response-loss", UserID: "user-response-loss", ConversationID: "conversation-response-loss"}
	insertGatewayConversation(t, pool, scope, manual.Now())
	work := validHandoffWork()
	work.Scope, work.SessionID, work.Incarnation = scope, "session-response-loss", 6
	work.Turn = identity.TurnKey{ID: "turn-response-loss", Revision: 1}
	work.Transcript = "durable response-loss request"
	work.Snapshot.Deadline = manual.Now().Add(time.Minute)
	_, err := repository.AppendCommittedItem(t.Context(), scope, postgres.ConversationItem{
		SessionID: work.SessionID, Turn: work.Turn, Sequence: 1, Role: "user",
		Content: work.Transcript, ContentHash: "hash-response-loss", CreatedAt: manual.Now(),
	})
	require.NoError(t, err)
	responseLost := errors.New("commit response lost")
	firstService, err := handoff.NewService(commitResponseLossRepository{Repository: repository, err: responseLost}, manual, coordinatorIDs{
		handoffID: "handoff-response-loss", eventID: "event-response-loss",
	})
	require.NoError(t, err)
	firstInputs := make(chan session.Input, 1)
	first := NewHandoffCoordinator(firstService, &submitterFake{inputs: firstInputs}, 1)
	require.NoError(t, first.configureJournal(repository, "app-a", "worker-response-loss", 200*time.Millisecond))
	require.NoError(t, first.Enqueue(work))
	require.ErrorIs(t, first.Run(t.Context()), responseLost)
	select {
	case input := <-firstInputs:
		t.Fatalf("ambiguous commit submitted terminal completion: %#v", input)
	default:
	}
	var delivered bool
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT delivered FROM coordinator_work_journal WHERE work_key=$1`, handoffWorkKey(mustCanonicalHandoffWork(t, work))).Scan(&delivered))
	assert.False(t, delivered)
	var handoffCount int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM handoffs WHERE id='handoff-response-loss'`).Scan(&handoffCount))
	assert.Equal(t, 1, handoffCount)
	manual.Advance(2 * time.Minute)

	reactor := session.NewReactor(session.IDs{
		SessionID: work.SessionID, ConversationID: work.Scope.ConversationID, Incarnation: work.Incarnation,
	}, session.NewEpochFence(work.OriginEpoch-1))
	applyGatewayReactor(t, reactor, session.TurnCommittedInput{Sequence: 1, Turn: work.Turn, Transcript: work.Transcript})
	applyGatewayReactor(t, reactor, session.ClientInput{Event: protocol.ResponseCreateEvent{Type: "response.create"}})
	applyGatewayReactor(t, reactor, session.VoiceInput{Incarnation: work.Incarnation, Event: realtime.Event{
		Kind: realtime.EventResponseCreated, Epoch: work.OriginEpoch, ResponseID: work.ResponseID, Status: "in_progress",
	}})
	applyGatewayReactor(t, reactor, session.VoiceInput{Incarnation: work.Incarnation, Event: realtime.Event{
		Kind: realtime.EventFunctionCallDone, Epoch: work.OriginEpoch, ResponseID: work.ResponseID,
		ItemID: "tool-item-response-loss", CallID: work.CallID, Name: work.ToolName, Arguments: work.Arguments,
	}})
	applyGatewayReactor(t, reactor, session.VoiceInput{Incarnation: work.Incarnation, Event: realtime.Event{
		Kind: realtime.EventResponseDone, Epoch: work.OriginEpoch, ResponseID: work.ResponseID, Status: "completed",
	}})
	secondService, err := handoff.NewService(repository, manual, coordinatorIDs{handoffID: "must-not-generate", eventID: "must-not-generate"})
	require.NoError(t, err)
	submit := &reactorSubmitter{reactor: reactor}
	second := NewHandoffCoordinator(secondService, submit, 1)
	require.NoError(t, second.configureJournal(repository, "app-a", "worker-restart", 200*time.Millisecond))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- second.Run(ctx) }()
	require.Eventually(t, func() bool {
		return submit.itemCount() == 1 && pool.QueryRow(t.Context(), `SELECT delivered FROM coordinator_work_journal WHERE work_key=$1`, handoffWorkKey(mustCanonicalHandoffWork(t, work))).Scan(&delivered) == nil && delivered
	}, 2*time.Second, 5*time.Millisecond)
	assert.Equal(t, 1, submit.itemCount())
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM handoffs WHERE id='handoff-response-loss'`).Scan(&handoffCount))
	assert.Equal(t, 1, handoffCount)
	cancel()
	require.NoError(t, <-done)
}
