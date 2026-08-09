//go:build integration

package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
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

type signalingCoordinatorJournal struct {
	CoordinatorJournal
	ready chan struct{}
	once  sync.Once
}

type signalingAffinityCloser struct{ io.Closer }

func (j *signalingCoordinatorJournal) AcquireCoordinatorAffinity(ctx context.Context, affinity string) (io.Closer, error) {
	closer, err := j.CoordinatorJournal.AcquireCoordinatorAffinity(ctx, affinity)
	if err != nil {
		return nil, err
	}
	j.once.Do(func() { close(j.ready) })
	return &signalingAffinityCloser{Closer: closer}, nil
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

func TestAppsHoldExclusiveCoordinatorAffinityUntilTeardown(t *testing.T) {
	repository, _, manual := gatewayRepositoryHarness(t)
	service, err := handoff.NewService(repository, manual, coordinatorIDs{handoffID: "unused-handoff", eventID: "unused-event"})
	require.NoError(t, err)
	newApp := func(affinity, owner string) (*App, <-chan struct{}) {
		ready := make(chan struct{})
		journal := &signalingCoordinatorJournal{CoordinatorJournal: repository, ready: ready}
		cfg := validConfig()
		cfg.Features.Handoffs = true
		app, appErr := New(cfg, testkit.NewFakeConnector(1), WithIDGenerator(deterministicIDs{}), WithDurableHandoffs(DurableDependencies{
			Service: service, ConversationWriter: repository, ConversationProvisioner: repository, ConversationTTL: 24 * time.Hour, Journal: journal,
			DeliveryAffinity: affinity, JournalOwner: owner, JournalClaimLease: 100 * time.Millisecond,
			HandoffQueueCapacity: 1, TranscriptQueueCapacity: 1, Snapshot: validHandoffWork().Snapshot,
		}), testHandoffPresentationOption())
		require.NoError(t, appErr)
		app.listener = newBlockingListener()
		return app, ready
	}
	start := func(app *App) (context.CancelFunc, <-chan error) {
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- app.Run(ctx) }()
		return cancel, done
	}
	waitReady := func(ready <-chan struct{}) {
		select {
		case <-ready:
		case <-time.After(time.Second):
			t.Fatal("app did not acquire coordinator affinity")
		}
	}

	firstApp, firstReady := newApp("shared-affinity", "first")
	firstCancel, firstDone := start(firstApp)
	waitReady(firstReady)
	duplicate, _ := newApp("shared-affinity", "second")
	duplicateCtx, duplicateCancel := context.WithTimeout(t.Context(), time.Second)
	require.ErrorIs(t, duplicate.Run(duplicateCtx), postgres.ErrCoordinatorAffinityInUse)
	duplicateCancel()

	otherApp, otherReady := newApp("other-affinity", "other")
	otherCancel, otherDone := start(otherApp)
	waitReady(otherReady)
	unexpectedLock, lockErr := repository.AcquireCoordinatorAffinity(t.Context(), "shared-affinity")
	if lockErr == nil {
		require.NoError(t, unexpectedLock.Close())
	}
	require.ErrorIs(t, lockErr, postgres.ErrCoordinatorAffinityInUse)

	firstCancel()
	require.NoError(t, <-firstDone)
	restartedApp, restartedReady := newApp("shared-affinity", "restarted")
	restartedCancel, restartedDone := start(restartedApp)
	waitReady(restartedReady)
	restartedCancel()
	require.NoError(t, <-restartedDone)
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
