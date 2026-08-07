package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/protocol"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/chuanmingliu/openai-gpt-live/internal/session"
	"github.com/chuanmingliu/openai-gpt-live/internal/store/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type handoffServiceFake struct {
	creates   chan handoff.CreateRequest
	result    handoff.Snapshot
	err       error
	getResult handoff.Snapshot
	getErr    error
	steerErr  error
	cancelErr error
	steers    chan handoff.SteerRequest
	cancels   chan handoff.CancelRequest
}

type blockingHandoffService struct {
	handoffServiceFake
	started chan struct{}
	release chan struct{}
}

func (f *blockingHandoffService) Create(ctx context.Context, request handoff.CreateRequest) (handoff.Snapshot, error) {
	f.creates <- request
	close(f.started)
	select {
	case <-f.release:
		return f.result, f.err
	case <-ctx.Done():
		return handoff.Snapshot{}, ctx.Err()
	}
}

func (f *handoffServiceFake) Create(_ context.Context, request handoff.CreateRequest) (handoff.Snapshot, error) {
	f.creates <- request
	return f.result, f.err
}
func (f *handoffServiceFake) Get(context.Context, handoff.Scope, identity.HandoffID) (handoff.Snapshot, error) {
	if f.getErr != nil {
		return handoff.Snapshot{}, f.getErr
	}
	if f.getResult.ID == "" {
		return handoff.Snapshot{}, handoff.ErrNotFound
	}
	return f.getResult, nil
}
func (*handoffServiceFake) ListActive(context.Context, handoff.Scope) ([]handoff.Snapshot, error) {
	return nil, nil
}
func (f *handoffServiceFake) Steer(_ context.Context, _ handoff.Scope, id identity.HandoffID, request handoff.SteerRequest) (handoff.Snapshot, error) {
	f.steers <- request
	if f.steerErr != nil {
		return handoff.Snapshot{}, f.steerErr
	}
	return handoff.Snapshot{ID: id}, nil
}
func (*handoffServiceFake) ProvideInput(context.Context, handoff.Scope, identity.HandoffID, handoff.InputRequest) (handoff.Snapshot, error) {
	return handoff.Snapshot{}, nil
}
func (f *handoffServiceFake) Cancel(_ context.Context, _ handoff.Scope, id identity.HandoffID, request handoff.CancelRequest) (handoff.Snapshot, error) {
	f.cancels <- request
	if f.cancelErr != nil {
		return handoff.Snapshot{}, f.cancelErr
	}
	return handoff.Snapshot{ID: id}, nil
}

func validHandoffWork() HandoffWork {
	return HandoffWork{
		SessionID: "session", Incarnation: 3, OriginEpoch: 8,
		Scope:  handoff.Scope{TenantID: "tenant-trusted", UserID: "user-trusted", ConversationID: "conversation-trusted"},
		CallID: "call-1", ResponseID: "response-1", ToolName: handoff.ToolStartBackgroundTask,
		Arguments: json.RawMessage(`{"objective":"validated objective"}`),
		Turn:      identity.TurnKey{ID: "turn-1", Revision: 2}, Transcript: "trusted transcript",
		Snapshot: SessionHandoffSnapshot{
			ContextRef: "context://trusted", ContextHash: "hash-trusted", Deadline: time.Now().Add(time.Hour),
			TokenBudget: 99, Priority: 4, ToolAllowlist: []string{"search"},
			ApprovalPolicy:   json.RawMessage(`{"mode":"strict"}`),
			Versions:         handoff.Versions{Model: "m", Prompt: "p", ToolRegistry: "t", Config: "c"},
			PresentationHint: "server-policy",
		},
	}
}

func TestHandoffCoordinatorUsesTrustedWorkAndSnapshotThenSubmitsAcceptance(t *testing.T) {
	service := &handoffServiceFake{creates: make(chan handoff.CreateRequest, 1), result: handoff.Snapshot{ID: "handoff-1"}}
	submit := &submitterFake{inputs: make(chan session.Input, 1)}
	coordinator := NewHandoffCoordinator(service, submit, 1)
	work := validHandoffWork()
	require.NoError(t, coordinator.Enqueue(work))
	work.Arguments[14] = 'X'
	work.Snapshot.ToolAllowlist[0] = "attacker"
	work.Snapshot.ApprovalPolicy[9] = 'X'

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- coordinator.Run(ctx) }()
	request := <-service.creates
	assert.Equal(t, "tenant-trusted", request.Scope.TenantID)
	assert.Equal(t, "conversation-trusted", string(request.Scope.ConversationID))
	assert.Equal(t, "validated objective", request.Objective)
	assert.Equal(t, "trusted transcript", request.UserRequest)
	assert.Equal(t, 99, request.TokenBudget)
	assert.Equal(t, int16(4), request.Priority)
	assert.Equal(t, []string{"search"}, request.ToolAllowlist)
	assert.JSONEq(t, `{"mode":"strict"}`, string(request.ApprovalPolicy))
	assert.Equal(t, "server-policy", request.PresentationHint)
	assert.NotEmpty(t, request.IdempotencyKey)

	accepted := (<-submit.inputs).(session.HandoffAcceptedInput)
	assert.True(t, accepted.Accepted)
	assert.Equal(t, identity.HandoffID("handoff-1"), accepted.HandoffID)
	assert.Equal(t, work.CallID, accepted.CallID)
	cancel()
	require.NoError(t, <-done)
}

func TestHandoffCoordinatorRejectsAdversarialArgumentsAndOverflow(t *testing.T) {
	service := &handoffServiceFake{creates: make(chan handoff.CreateRequest, 1)}
	submit := &submitterFake{inputs: make(chan session.Input, 1)}
	coordinator := NewHandoffCoordinator(service, submit, 1)
	work := validHandoffWork()
	work.Arguments = json.RawMessage(`{"objective":"ok","token_budget":999999}`)
	require.NoError(t, coordinator.Enqueue(work))
	require.ErrorIs(t, coordinator.Enqueue(work), ErrCoordinatorOverflow)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- coordinator.Run(ctx) }()
	rejected := (<-submit.inputs).(session.HandoffAcceptedInput)
	assert.False(t, rejected.Accepted)
	assert.Empty(t, rejected.HandoffID)
	select {
	case request := <-service.creates:
		t.Fatalf("invalid model authority reached service: %#v", request)
	default:
	}
	cancel()
	require.NoError(t, <-done)
}

func TestHandoffCoordinatorRejectsInvalidDependencies(t *testing.T) {
	var service *handoffServiceFake
	var submit *submitterFake
	assert.Nil(t, NewHandoffCoordinator(service, &submitterFake{}, 1))
	assert.Nil(t, NewHandoffCoordinator(&handoffServiceFake{}, submit, 1))
	assert.Nil(t, NewHandoffCoordinator(&handoffServiceFake{}, &submitterFake{}, -1))
}

func TestHandoffCoordinatorSteersAndCancelsWithServerDerivedRevisionsAndKeys(t *testing.T) {
	service := &handoffServiceFake{
		creates:   make(chan handoff.CreateRequest, 1),
		getResult: handoff.Snapshot{ID: "handoff-2", ObjectiveRevision: 4, Request: handoff.CreateRequest{Objective: "old"}},
		steers:    make(chan handoff.SteerRequest, 1), cancels: make(chan handoff.CancelRequest, 1),
	}
	submit := &submitterFake{inputs: make(chan session.Input, 2)}
	coordinator := NewHandoffCoordinator(service, submit, 2)
	steer := validHandoffWork()
	steer.CallID = "call-steer"
	steer.ToolName = handoff.ToolSteerBackgroundTask
	steer.Arguments = json.RawMessage(`{"handoff_id":"handoff-2","objective":"new"}`)
	cancelWork := validHandoffWork()
	cancelWork.CallID = "call-cancel"
	cancelWork.ToolName = handoff.ToolCancelBackgroundTask
	cancelWork.Arguments = json.RawMessage(`{"handoff_id":"handoff-2","reason":"stop"}`)
	require.NoError(t, coordinator.Enqueue(steer))
	require.NoError(t, coordinator.Enqueue(cancelWork))

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- coordinator.Run(ctx) }()
	steerRequest := <-service.steers
	assert.Equal(t, uint32(5), steerRequest.ExpectedRevision)
	assert.Equal(t, "assistant", steerRequest.Actor)
	assert.NotEmpty(t, steerRequest.IdempotencyKey)
	cancelRequest := <-service.cancels
	assert.Equal(t, "assistant", cancelRequest.Actor)
	assert.Equal(t, "stop", cancelRequest.Reason)
	assert.NotEmpty(t, cancelRequest.IdempotencyKey)
	for range 2 {
		assert.True(t, (<-submit.inputs).(session.HandoffAcceptedInput).Accepted)
	}
	cancel()
	require.NoError(t, <-done)
}

func TestHandoffCoordinatorDisconnectDropsOnlyLiveDelivery(t *testing.T) {
	service := &handoffServiceFake{creates: make(chan handoff.CreateRequest, 1), result: handoff.Snapshot{ID: "committed-handoff"}}
	coordinator := NewHandoffCoordinator(service, &submitterFake{err: session.ErrSessionNotActive}, 2)
	work := validHandoffWork()
	require.NoError(t, coordinator.Enqueue(work))

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- coordinator.Run(ctx) }()
	request := <-service.creates
	assert.Equal(t, "validated objective", request.Objective, "disconnect did not cancel the authorized commit")

	cancel()
	require.NoError(t, <-done)
}

func TestHandoffCoordinatorDelegatesExpiredStartToExactServiceReplay(t *testing.T) {
	service := &handoffServiceFake{
		creates: make(chan handoff.CreateRequest, 1), result: handoff.Snapshot{ID: "already-committed"},
	}
	inputs := make(chan session.Input, 1)
	coordinator := NewHandoffCoordinator(service, &submitterFake{inputs: inputs}, 1)
	work := validHandoffWork()
	work.Snapshot.Deadline = time.Now().Add(-time.Minute)
	require.NoError(t, coordinator.Enqueue(work))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- coordinator.Run(ctx) }()
	var request handoff.CreateRequest
	select {
	case request = <-service.creates:
	case input := <-inputs:
		t.Fatalf("expired exact replay bypassed durable service: %#v", input)
	}
	assert.Equal(t, work.Snapshot.Deadline.UTC(), request.Deadline)
	accepted := (<-inputs).(session.HandoffAcceptedInput)
	assert.True(t, accepted.Accepted)
	assert.Equal(t, identity.HandoffID("already-committed"), accepted.HandoffID)
	cancel()
	require.NoError(t, <-done)
}

func TestHandoffCoordinatorEnforcesPerItemAndAggregateBytesBeforeCopy(t *testing.T) {
	coordinator := NewHandoffCoordinator(&handoffServiceFake{}, &submitterFake{}, 32)
	base := validHandoffWork()
	base.Transcript = strings.Repeat("t", session.DurableTranscriptBytes)
	accepted := 0
	for index := 0; index < 32; index++ {
		work := base
		work.CallID = fmt.Sprintf("call-%d", index)
		err := coordinator.Enqueue(work)
		if errors.Is(err, ErrCoordinatorOverflow) {
			break
		}
		require.NoError(t, err)
		accepted++
	}
	assert.Positive(t, accepted)
	assert.Less(t, accepted, 32)
	assert.LessOrEqual(t, coordinator.retainedBytes, CoordinatorAggregateBytes)

	over := base
	over.Arguments = append(json.RawMessage(nil), base.Arguments...)
	over.Arguments = append(over.Arguments, bytes.Repeat([]byte(" "), session.DurableArgumentsBytes-len(over.Arguments)+1)...)
	require.ErrorIs(t, coordinator.Enqueue(over), ErrCoordinatorWorkTooLarge)
}

func TestHandoffIdempotencyKeyIncludesExactCommittedTurn(t *testing.T) {
	first, err := canonicalizeHandoffWork(validHandoffWork())
	require.NoError(t, err)
	second := first
	second.Turn = identity.TurnKey{ID: "turn-2", Revision: 1}

	assert.NotEmpty(t, handoffIdempotencyKey(first, first.Arguments))
	assert.NotEqual(t, handoffIdempotencyKey(first, first.Arguments), handoffIdempotencyKey(second, second.Arguments))
}

func TestHandoffJournalReplaysCrashAfterOperationWithoutManualReenqueue(t *testing.T) {
	journal := newCoordinatorJournalFake()
	service := &handoffServiceFake{creates: make(chan handoff.CreateRequest, 2), result: handoff.Snapshot{ID: "handoff-replayed"}}
	crash := errors.New("crash before live acknowledgement")
	first := NewHandoffCoordinator(service, &submitterFake{err: crash}, 1)
	require.NoError(t, first.configureJournal(journal, "app-a", "owner-1", 100*time.Millisecond))
	require.NoError(t, first.Enqueue(validHandoffWork()))
	require.ErrorIs(t, first.Run(t.Context()), crash)
	firstRequest := <-service.creates

	accepted := make(chan session.Input, 1)
	second := NewHandoffCoordinator(service, &submitterFake{inputs: accepted}, 1)
	require.NoError(t, second.configureJournal(journal, "app-a", "owner-2", 100*time.Millisecond))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- second.Run(ctx) }()
	secondRequest := <-service.creates
	completion := (<-accepted).(session.HandoffAcceptedInput)
	assert.True(t, completion.Accepted)
	assert.Equal(t, identity.HandoffID("handoff-replayed"), completion.HandoffID)
	assert.Equal(t, firstRequest.IdempotencyKey, secondRequest.IdempotencyKey)
	require.Eventually(t, func() bool {
		return journal.isDelivered(handoffWorkKey(mustCanonicalHandoffWork(t, validHandoffWork())))
	}, time.Second, time.Millisecond)
	cancel()
	require.NoError(t, <-done)
}

type reactorSubmitter struct {
	mu          sync.Mutex
	reactor     *session.Reactor
	createItems int
}

func (s *reactorSubmitter) Submit(_ context.Context, _ identity.SessionID, _ identity.Incarnation, input session.Input) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	effects, err := s.reactor.Apply(input)
	if err != nil {
		return err
	}
	for _, effect := range effects {
		if effect.VoiceCommand != nil && effect.VoiceCommand.Kind == realtime.CommandCreateItem {
			s.createItems++
		}
	}
	return nil
}

func (s *reactorSubmitter) itemCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.createItems
}

func TestHandoffJournalReplayAfterSubmitBeforeAckEmitsExactlyOneReactorOutput(t *testing.T) {
	work := validHandoffWork()
	reactor := session.NewReactor(session.IDs{
		SessionID: work.SessionID, ConversationID: work.Scope.ConversationID, Incarnation: work.Incarnation,
	}, session.NewEpochFence(work.OriginEpoch-1))
	applyGatewayReactor(t, reactor, session.TurnCommittedInput{Sequence: 1, Turn: work.Turn, Transcript: work.Transcript})
	effects := applyGatewayReactor(t, reactor, session.ClientInput{Event: protocol.ResponseCreateEvent{Type: "response.create"}})
	require.Equal(t, work.OriginEpoch, effects[0].VoiceCommand.Epoch)
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

	journal := newCoordinatorJournalFake()
	journal.ackErr = errors.New("crash after submit")
	service := &handoffServiceFake{creates: make(chan handoff.CreateRequest, 2), result: handoff.Snapshot{ID: "handoff-once"}}
	submit := &reactorSubmitter{reactor: reactor}
	first := NewHandoffCoordinator(service, submit, 1)
	require.NoError(t, first.configureJournal(journal, "app-a", "owner-1", 100*time.Millisecond))
	require.NoError(t, first.Enqueue(work))
	require.ErrorIs(t, first.Run(t.Context()), journal.ackErr)
	assert.Equal(t, 1, submit.itemCount())

	journal.setAckError(nil)
	second := NewHandoffCoordinator(service, submit, 1)
	require.NoError(t, second.configureJournal(journal, "app-a", "owner-2", 100*time.Millisecond))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- second.Run(ctx) }()
	require.Eventually(t, func() bool { return journal.isDelivered(handoffWorkKey(mustCanonicalHandoffWork(t, work))) }, time.Second, time.Millisecond)
	assert.Equal(t, 1, submit.itemCount(), "reactor tombstone suppresses the replayed completion")
	cancel()
	require.NoError(t, <-done)
}

func TestHandoffJournalStaleIncarnationIsTerminallyAcknowledged(t *testing.T) {
	journal := newCoordinatorJournalFake()
	service := &handoffServiceFake{creates: make(chan handoff.CreateRequest, 1), result: handoff.Snapshot{ID: "durable-handoff"}}
	coordinator := NewHandoffCoordinator(service, &submitterFake{err: session.ErrSessionNotActive}, 1)
	require.NoError(t, coordinator.configureJournal(journal, "app-a", "owner", time.Minute))
	work := validHandoffWork()
	require.NoError(t, coordinator.Enqueue(work))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- coordinator.Run(ctx) }()
	<-service.creates
	require.Eventually(t, func() bool { return journal.isDelivered(handoffWorkKey(mustCanonicalHandoffWork(t, work))) }, time.Second, time.Millisecond)
	cancel()
	require.NoError(t, <-done)
}

func TestHandoffJournalTypedCorruptionFailsClosed(t *testing.T) {
	journal := newCoordinatorJournalFake()
	work := mustCanonicalHandoffWork(t, validHandoffWork())
	payload, err := json.Marshal(work)
	require.NoError(t, err)
	record := postgres.CoordinatorJournalRecord{
		WorkKey: handoffWorkKey(work), Kind: postgres.CoordinatorKindHandoff, Scope: work.Scope,
		SessionID: work.SessionID, Incarnation: work.Incarnation, LocalSequence: uint64(work.OriginEpoch), DeliveryAffinity: "app-a", Payload: payload,
	}
	record.Scope.UserID = "wrong-user"
	journal.records[record.WorkKey] = record
	coordinator := NewHandoffCoordinator(&handoffServiceFake{}, &submitterFake{}, 1)
	require.NoError(t, coordinator.configureJournal(journal, "app-a", "owner", time.Minute))
	require.EqualError(t, coordinator.Run(t.Context()), "corrupt handoff coordinator journal record")
}

func TestHandoffCoordinatorRenewsClaimWhileOperationExceedsLease(t *testing.T) {
	journal := newCoordinatorJournalFake()
	service := &blockingHandoffService{
		handoffServiceFake: handoffServiceFake{
			creates: make(chan handoff.CreateRequest, 1), result: handoff.Snapshot{ID: "handoff-long-operation"},
		},
		started: make(chan struct{}), release: make(chan struct{}),
	}
	inputs := make(chan session.Input, 1)
	coordinator := NewHandoffCoordinator(service, &submitterFake{inputs: inputs}, 1)
	require.NoError(t, coordinator.configureJournal(journal, "app-a", "worker-a", 30*time.Millisecond))
	require.NoError(t, coordinator.Enqueue(validHandoffWork()))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- coordinator.Run(ctx) }()
	<-service.started
	time.Sleep(90 * time.Millisecond)
	close(service.release)
	accepted := (<-inputs).(session.HandoffAcceptedInput)
	assert.Equal(t, identity.HandoffID("handoff-long-operation"), accepted.HandoffID)
	require.Eventually(t, func() bool {
		return journal.isDelivered(handoffWorkKey(mustCanonicalHandoffWork(t, validHandoffWork())))
	}, time.Second, time.Millisecond)
	assert.GreaterOrEqual(t, journal.renewals(), 2)
	cancel()
	require.NoError(t, <-done)
}

func TestHandoffCoordinatorCancellationStopsOperationAndLeaseWithoutDelivery(t *testing.T) {
	journal := newCoordinatorJournalFake()
	service := &blockingHandoffService{
		handoffServiceFake: handoffServiceFake{
			creates: make(chan handoff.CreateRequest, 1), result: handoff.Snapshot{ID: "must-not-deliver"},
		},
		started: make(chan struct{}), release: make(chan struct{}),
	}
	inputs := make(chan session.Input, 1)
	coordinator := NewHandoffCoordinator(service, &submitterFake{inputs: inputs}, 1)
	require.NoError(t, coordinator.configureJournal(journal, "app-a", "worker-a", 30*time.Millisecond))
	work := validHandoffWork()
	require.NoError(t, coordinator.Enqueue(work))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- coordinator.Run(ctx) }()
	<-service.started
	cancel()
	require.NoError(t, <-done)
	assert.False(t, journal.isDelivered(handoffWorkKey(mustCanonicalHandoffWork(t, work))))
	select {
	case input := <-inputs:
		t.Fatalf("cancelled lease submitted live completion: %#v", input)
	default:
	}
}

func TestHandoffCoordinatorLeaseLossCancelsOperationWithoutSubmitOrAck(t *testing.T) {
	leaseLost := errors.New("lease generation lost")
	journal := newCoordinatorJournalFake()
	journal.renewErr = leaseLost
	service := &blockingHandoffService{
		handoffServiceFake: handoffServiceFake{
			creates: make(chan handoff.CreateRequest, 1), result: handoff.Snapshot{ID: "must-not-deliver"},
		},
		started: make(chan struct{}), release: make(chan struct{}),
	}
	inputs := make(chan session.Input, 1)
	coordinator := NewHandoffCoordinator(service, &submitterFake{inputs: inputs}, 1)
	require.NoError(t, coordinator.configureJournal(journal, "app-a", "worker-a", 30*time.Millisecond))
	work := validHandoffWork()
	require.NoError(t, coordinator.Enqueue(work))
	err := coordinator.Run(t.Context())
	require.ErrorIs(t, err, leaseLost)
	assert.False(t, journal.isDelivered(handoffWorkKey(mustCanonicalHandoffWork(t, work))))
	select {
	case input := <-inputs:
		t.Fatalf("lost lease submitted live completion: %#v", input)
	default:
	}
}

func TestHandoffCoordinatorTransientServiceFailureDoesNotSubmitOrAck(t *testing.T) {
	transient := errors.New("database connection reset")
	journal := newCoordinatorJournalFake()
	inputs := make(chan session.Input, 1)
	coordinator := NewHandoffCoordinator(&handoffServiceFake{
		creates: make(chan handoff.CreateRequest, 1), err: transient,
	}, &submitterFake{inputs: inputs}, 1)
	require.NoError(t, coordinator.configureJournal(journal, "app-a", "worker-a", 100*time.Millisecond))
	work := validHandoffWork()
	require.NoError(t, coordinator.Enqueue(work))

	ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
	defer cancel()
	err := coordinator.Run(ctx)
	require.ErrorIs(t, err, transient)
	assert.False(t, journal.isDelivered(handoffWorkKey(mustCanonicalHandoffWork(t, work))))
	select {
	case input := <-inputs:
		t.Fatalf("transient service failure submitted terminal completion: %#v", input)
	default:
	}
}

func TestHandoffCoordinatorFailsClosedForNewWorkDuringDurabilityOutage(t *testing.T) {
	manual := clock.NewManual(time.Unix(0, 0))
	coordinator := NewHandoffCoordinator(&handoffServiceFake{
		creates: make(chan handoff.CreateRequest, 1), err: fmt.Errorf("%w: database unavailable", ErrTransientDurability),
	}, &submitterFake{}, 2)
	require.NoError(t, coordinator.configureOutage(30*time.Second, manual))
	require.NoError(t, coordinator.Enqueue(validHandoffWork()))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- coordinator.Run(ctx) }()
	require.Eventually(t, func() bool { return coordinator.unavailable.Load() }, time.Second, time.Millisecond)
	require.ErrorIs(t, coordinator.Enqueue(validHandoffWork()), ErrDurabilityUnavailable)
	cancel()
	require.NoError(t, <-done)
}

func TestDurabilityHealthKeepsIndependentLaneFailuresFenced(t *testing.T) {
	health := &durabilityHealth{}
	transcripts := NewTranscriptCoordinator(&transcriptWriterFake{}, &submitterFake{}, 1)
	handoffs := NewHandoffCoordinator(&handoffServiceFake{}, &submitterFake{}, 2)
	transcripts.health, handoffs.health = health, health
	transcripts.outageClock, handoffs.outageClock = clock.Real{}, clock.Real{}

	transcripts.beginOutage()
	// A healthy/no-op handoff lane may clear only its own bit.
	handoffs.beginOutage()
	handoffs.endOutage()
	require.ErrorIs(t, handoffs.Enqueue(validHandoffWork()), ErrDurabilityUnavailable)
	transcripts.endOutage()
	require.NoError(t, handoffs.Enqueue(validHandoffWork()))
}

func TestHandoffPeriodicReplayUsesSameStorageGraceDeadline(t *testing.T) {
	manual := clock.NewManual(time.Unix(0, 0))
	journal := newCoordinatorJournalFake()
	coordinator := NewHandoffCoordinator(&handoffServiceFake{}, &submitterFake{}, 1)
	require.NoError(t, coordinator.configureJournal(journal, "handoff-timer-affinity", "handoff-timer-owner", 100*time.Millisecond))
	require.NoError(t, coordinator.configureOutage(30*time.Second, manual))
	done := make(chan error, 1)
	go func() { done <- coordinator.Run(t.Context()) }()
	require.Eventually(t, func() bool {
		journal.mu.Lock()
		defer journal.mu.Unlock()
		return journal.claimCalls >= 1
	}, time.Second, time.Millisecond)
	journal.mu.Lock()
	journal.claimErr = fmt.Errorf("%w: handoff replay unavailable", ErrTransientDurability)
	journal.mu.Unlock()
	manual.Advance(50 * time.Millisecond)
	require.Eventually(t, func() bool { return coordinator.unavailable.Load() }, time.Second, time.Millisecond)
	time.Sleep(time.Millisecond)
	manual.Advance(30 * time.Second)
	require.ErrorIs(t, <-done, ErrDurabilityGraceExpired)
}

func TestHandoffCoordinatorTransientGetSteerAndCancelFailuresRemainReplayable(t *testing.T) {
	transient := errors.New("repository temporarily unavailable")
	for _, tc := range []struct {
		name    string
		work    HandoffWork
		service *handoffServiceFake
	}{
		{
			name: "get before steer", work: func() HandoffWork {
				work := validHandoffWork()
				work.ToolName = handoff.ToolSteerBackgroundTask
				work.Arguments = json.RawMessage(`{"handoff_id":"handoff-2","objective":"new"}`)
				return work
			}(), service: &handoffServiceFake{getErr: transient},
		},
		{
			name: "steer mutation", work: func() HandoffWork {
				work := validHandoffWork()
				work.ToolName = handoff.ToolSteerBackgroundTask
				work.Arguments = json.RawMessage(`{"handoff_id":"handoff-2","objective":"new"}`)
				return work
			}(), service: &handoffServiceFake{
				getResult: handoff.Snapshot{ID: "handoff-2", ObjectiveRevision: 2, Request: handoff.CreateRequest{Objective: "old"}},
				steers:    make(chan handoff.SteerRequest, 1), steerErr: transient,
			},
		},
		{
			name: "cancel mutation", work: func() HandoffWork {
				work := validHandoffWork()
				work.ToolName = handoff.ToolCancelBackgroundTask
				work.Arguments = json.RawMessage(`{"handoff_id":"handoff-2","reason":"stop"}`)
				return work
			}(), service: &handoffServiceFake{cancels: make(chan handoff.CancelRequest, 1), cancelErr: transient},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			journal := newCoordinatorJournalFake()
			inputs := make(chan session.Input, 1)
			coordinator := NewHandoffCoordinator(tc.service, &submitterFake{inputs: inputs}, 1)
			require.NoError(t, coordinator.configureJournal(journal, "app-a", "worker-a", 100*time.Millisecond))
			require.NoError(t, coordinator.Enqueue(tc.work))
			require.ErrorIs(t, coordinator.Run(t.Context()), transient)
			assert.False(t, journal.isDelivered(handoffWorkKey(mustCanonicalHandoffWork(t, tc.work))))
			select {
			case input := <-inputs:
				t.Fatalf("transient service failure submitted terminal completion: %#v", input)
			default:
			}
		})
	}
}

func TestHandoffCoordinatorStableDomainRejectionSubmitsAndAcknowledges(t *testing.T) {
	journal := newCoordinatorJournalFake()
	inputs := make(chan session.Input, 1)
	coordinator := NewHandoffCoordinator(&handoffServiceFake{
		creates: make(chan handoff.CreateRequest, 1), err: handoff.ErrUncommittedTurn,
	}, &submitterFake{inputs: inputs}, 1)
	require.NoError(t, coordinator.configureJournal(journal, "app-a", "worker-a", 100*time.Millisecond))
	work := validHandoffWork()
	require.NoError(t, coordinator.Enqueue(work))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- coordinator.Run(ctx) }()
	rejected := (<-inputs).(session.HandoffAcceptedInput)
	assert.False(t, rejected.Accepted)
	assert.Equal(t, "handoff_rejected", rejected.Reason)
	require.Eventually(t, func() bool {
		return journal.isDelivered(handoffWorkKey(mustCanonicalHandoffWork(t, work)))
	}, time.Second, time.Millisecond)
	cancel()
	require.NoError(t, <-done)
}

func TestHandoffCoordinatorSteeringBlockedSubmitsGenericRejectionAndAcknowledges(t *testing.T) {
	journal := newCoordinatorJournalFake()
	inputs := make(chan session.Input, 1)
	service := &handoffServiceFake{
		getResult: handoff.Snapshot{ID: "handoff-2", ObjectiveRevision: 2, Request: handoff.CreateRequest{Objective: "old"}},
		steers:    make(chan handoff.SteerRequest, 1),
		steerErr:  handoff.ErrSteeringBlocked,
	}
	coordinator := NewHandoffCoordinator(service, &submitterFake{inputs: inputs}, 1)
	require.NoError(t, coordinator.configureJournal(journal, "app-a", "worker-a", 100*time.Millisecond))
	work := validHandoffWork()
	work.ToolName = handoff.ToolSteerBackgroundTask
	work.Arguments = json.RawMessage(`{"handoff_id":"handoff-2","objective":"new"}`)
	require.NoError(t, coordinator.Enqueue(work))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- coordinator.Run(ctx) }()
	<-service.steers
	rejected := (<-inputs).(session.HandoffAcceptedInput)
	assert.False(t, rejected.Accepted)
	assert.Empty(t, rejected.HandoffID)
	assert.Equal(t, "handoff_rejected", rejected.Reason)
	require.Eventually(t, func() bool {
		return journal.isDelivered(handoffWorkKey(mustCanonicalHandoffWork(t, work)))
	}, time.Second, time.Millisecond)
	cancel()
	require.NoError(t, <-done)
}

func TestHandoffCoordinatorRenewsLeaseThroughBlockedSubmit(t *testing.T) {
	journal := newCoordinatorJournalFake()
	submit := &blockingSubmitter{
		started: make(chan struct{}), release: make(chan struct{}), inputs: make(chan session.Input, 1),
	}
	coordinator := NewHandoffCoordinator(&handoffServiceFake{
		creates: make(chan handoff.CreateRequest, 1), result: handoff.Snapshot{ID: "handoff-submit"},
	}, submit, 1)
	require.NoError(t, coordinator.configureJournal(journal, "app-a", "worker-a", 30*time.Millisecond))
	work := validHandoffWork()
	require.NoError(t, coordinator.Enqueue(work))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- coordinator.Run(ctx) }()
	<-submit.started
	time.Sleep(90 * time.Millisecond)
	reclaimed, err := journal.ClaimCoordinatorWork(t.Context(), postgres.CoordinatorKindHandoff, "app-a", "worker-b", 30*time.Millisecond, 1)
	close(submit.release)
	require.NoError(t, err)
	assert.Empty(t, reclaimed, "a live submit lease must not be reclaimable")
	accepted := (<-submit.inputs).(session.HandoffAcceptedInput)
	assert.True(t, accepted.Accepted)
	require.Eventually(t, func() bool {
		return journal.isDelivered(handoffWorkKey(mustCanonicalHandoffWork(t, work)))
	}, time.Second, time.Millisecond)
	cancel()
	require.NoError(t, <-done)
}

func TestHandoffCoordinatorRenewalFailureCancelsBlockedSubmitWithoutAck(t *testing.T) {
	leaseLost := errors.New("lease generation lost during submit")
	journal := newCoordinatorJournalFake()
	submit := &blockingSubmitter{
		started: make(chan struct{}), release: make(chan struct{}), inputs: make(chan session.Input, 1),
	}
	coordinator := NewHandoffCoordinator(&handoffServiceFake{
		creates: make(chan handoff.CreateRequest, 1), result: handoff.Snapshot{ID: "must-not-deliver"},
	}, submit, 1)
	require.NoError(t, coordinator.configureJournal(journal, "app-a", "worker-a", 30*time.Millisecond))
	work := validHandoffWork()
	require.NoError(t, coordinator.Enqueue(work))
	done := make(chan error, 1)
	go func() { done <- coordinator.Run(t.Context()) }()
	<-submit.started
	journal.setRenewError(leaseLost)
	require.ErrorIs(t, <-done, leaseLost)
	assert.False(t, journal.isDelivered(handoffWorkKey(mustCanonicalHandoffWork(t, work))))
	select {
	case input := <-submit.inputs:
		t.Fatalf("lost lease completed blocked submit: %#v", input)
	default:
	}
}

func mustCanonicalHandoffWork(t *testing.T, work HandoffWork) HandoffWork {
	t.Helper()
	canonical, err := canonicalizeHandoffWork(work)
	require.NoError(t, err)
	return canonical
}

func applyGatewayReactor(t *testing.T, reactor *session.Reactor, input session.Input) []session.Effect {
	t.Helper()
	effects, err := reactor.Apply(input)
	require.NoError(t, err)
	return effects
}
