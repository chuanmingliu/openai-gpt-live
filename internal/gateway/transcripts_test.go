package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/session"
	"github.com/chuanmingliu/openai-gpt-live/internal/store/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type transcriptWriterFake struct {
	items chan postgres.ConversationItem
	err   error
}

func (f *transcriptWriterFake) AppendCommittedItem(_ context.Context, _ handoff.Scope, item postgres.ConversationItem) (postgres.ConversationItem, error) {
	if f.err != nil {
		return postgres.ConversationItem{}, f.err
	}
	f.items <- item
	return item, nil
}

type submitterFake struct {
	inputs chan session.Input
	err    error
}

type blockingSubmitter struct {
	started chan struct{}
	release chan struct{}
	inputs  chan session.Input
	once    sync.Once
}

func (s *blockingSubmitter) Submit(ctx context.Context, _ identity.SessionID, _ identity.Incarnation, input session.Input) error {
	s.once.Do(func() { close(s.started) })
	select {
	case <-s.release:
		s.inputs <- input
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type coordinatorJournalFake struct {
	mu              sync.Mutex
	records         map[string]postgres.CoordinatorJournalRecord
	delivered       map[string]bool
	claimOwner      map[string]string
	claimUntil      map[string]time.Time
	claimGeneration map[string]uint64
	nextStorage     uint64
	ackErr          error
	renewCount      int
	renewErr        error
	affinityErr     error
	affinityLock    io.Closer
	affinityNil     bool
}

func newCoordinatorJournalFake() *coordinatorJournalFake {
	return &coordinatorJournalFake{
		records: make(map[string]postgres.CoordinatorJournalRecord), delivered: make(map[string]bool),
		claimOwner: make(map[string]string), claimUntil: make(map[string]time.Time), claimGeneration: make(map[string]uint64),
	}
}

func (f *coordinatorJournalFake) AcquireCoordinatorAffinity(context.Context, string) (io.Closer, error) {
	if f.affinityErr != nil {
		return nil, f.affinityErr
	}
	if f.affinityNil {
		return nil, nil
	}
	if f.affinityLock != nil {
		return f.affinityLock, nil
	}
	return io.NopCloser(strings.NewReader("")), nil
}

func (f *coordinatorJournalFake) AdmitCoordinatorWork(_ context.Context, record postgres.CoordinatorJournalRecord) (postgres.CoordinatorJournalRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if existing, ok := f.records[record.WorkKey]; ok {
		if string(existing.Payload) != string(record.Payload) || existing.DeliveryAffinity != record.DeliveryAffinity {
			return postgres.CoordinatorJournalRecord{}, handoff.ErrIdempotencyConflict
		}
		return existing, nil
	}
	record.Payload = append(json.RawMessage(nil), record.Payload...)
	if record.Kind == postgres.CoordinatorKindTranscript {
		f.nextStorage++
		record.StorageSequence = f.nextStorage
	}
	record.CreatedAt = repositoryTestTime()
	f.records[record.WorkKey] = record
	return record, nil
}

func (f *coordinatorJournalFake) ClaimCoordinatorWork(_ context.Context, kind, affinity, owner string, lease time.Duration, _ int) ([]postgres.CoordinatorJournalRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now()
	var records []postgres.CoordinatorJournalRecord
	for key, record := range f.records {
		if record.Kind == kind && record.DeliveryAffinity == affinity && !f.delivered[key] && (f.claimUntil[key].IsZero() || !f.claimUntil[key].After(now)) {
			f.claimOwner[key] = owner
			f.claimUntil[key] = now.Add(lease)
			f.claimGeneration[key]++
			record.ClaimGeneration = f.claimGeneration[key]
			record.Payload = append(json.RawMessage(nil), record.Payload...)
			records = append(records, record)
		}
	}
	return records, nil
}

func (f *coordinatorJournalFake) RenewCoordinatorWork(_ context.Context, key, owner string, generation uint64, lease time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.renewCount++
	if f.renewErr != nil {
		return f.renewErr
	}
	if f.claimOwner[key] != owner || f.claimGeneration[key] != generation || !f.claimUntil[key].After(time.Now()) {
		return handoff.ErrNotFound
	}
	f.claimUntil[key] = time.Now().Add(lease)
	return nil
}

func (f *coordinatorJournalFake) renewals() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.renewCount
}

func (f *coordinatorJournalFake) setRenewError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.renewErr = err
}

func (f *coordinatorJournalFake) AckCoordinatorWork(_ context.Context, key, owner string, generation uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ackErr != nil {
		return f.ackErr
	}
	if f.claimOwner[key] != owner || f.claimGeneration[key] != generation || !f.claimUntil[key].After(time.Now()) {
		return handoff.ErrNotFound
	}
	f.delivered[key] = true
	delete(f.claimOwner, key)
	delete(f.claimUntil, key)
	return nil
}

func (f *coordinatorJournalFake) setAckError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ackErr = err
}

func (f *coordinatorJournalFake) isDelivered(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.delivered[key]
}

func repositoryTestTime() time.Time { return time.Date(2026, 8, 7, 1, 0, 0, 0, time.UTC) }

func (f *submitterFake) Submit(_ context.Context, _ identity.SessionID, _ identity.Incarnation, input session.Input) error {
	if f.err != nil {
		return f.err
	}
	if f.inputs != nil {
		f.inputs <- input
	}
	return nil
}

func TestTranscriptCoordinatorPersistsFinalHashThenCommitsOnlyUserTurn(t *testing.T) {
	writer := &transcriptWriterFake{items: make(chan postgres.ConversationItem, 2)}
	submit := &submitterFake{inputs: make(chan session.Input, 2)}
	coordinator := NewTranscriptCoordinator(writer, submit, 2)
	require.NotNil(t, coordinator)

	user := TranscriptWork{
		SessionID: "session-1", Incarnation: 4,
		Scope: handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "conversation"},
		Turn:  identity.TurnKey{ID: "turn-1", Revision: 2}, Sequence: 7,
		Role: "user", FinalText: "final words",
	}
	require.NoError(t, coordinator.Enqueue(user))
	assistant := user
	assistant.Sequence = 8
	assistant.Role = "assistant"
	assistant.FinalText = "final answer"
	require.NoError(t, coordinator.Enqueue(assistant))

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- coordinator.Run(ctx) }()

	first := <-writer.items
	second := <-writer.items
	wantHash := sha256.Sum256([]byte("final words"))
	assert.Equal(t, hex.EncodeToString(wantHash[:]), first.ContentHash)
	assert.Equal(t, "final words", first.Content)
	assert.Equal(t, "user", first.Role)
	assert.False(t, first.CreatedAt.IsZero())
	assert.Equal(t, first.CreatedAt.Location(), time.UTC)
	assert.Equal(t, "assistant", second.Role)

	committed := (<-submit.inputs).(session.TurnCommittedInput)
	assert.Equal(t, user.Turn, committed.Turn)
	assert.Equal(t, user.Sequence, committed.Sequence)
	assert.Equal(t, user.FinalText, committed.Transcript)
	persisted := (<-submit.inputs).(session.TranscriptPersistedInput)
	assert.Equal(t, assistant.Sequence, persisted.Sequence)
	cancel()
	require.NoError(t, <-done)
}

func TestTranscriptCoordinatorOverflowCopyAndStorageFailure(t *testing.T) {
	writer := &transcriptWriterFake{items: make(chan postgres.ConversationItem, 1)}
	submit := &submitterFake{inputs: make(chan session.Input, 1)}
	coordinator := NewTranscriptCoordinator(writer, submit, 1)
	work := TranscriptWork{SessionID: "session", Incarnation: 1,
		Scope: handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "conversation"},
		Turn:  identity.TurnKey{ID: "turn", Revision: 1}, Sequence: 1, Role: "user", FinalText: "owned"}
	require.NoError(t, coordinator.Enqueue(work))
	work.FinalText = "mutated"
	require.ErrorIs(t, coordinator.Enqueue(work), ErrCoordinatorOverflow)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- coordinator.Run(ctx) }()
	assert.Equal(t, "owned", (<-writer.items).Content)
	<-submit.inputs
	cancel()
	require.NoError(t, <-done)

	storageErr := errors.New("storage unavailable")
	failed := NewTranscriptCoordinator(&transcriptWriterFake{err: storageErr}, submit, 1)
	require.NoError(t, failed.Enqueue(work))
	err := failed.Run(t.Context())
	require.ErrorIs(t, err, storageErr)
	select {
	case input := <-submit.inputs:
		t.Fatalf("storage failure submitted committed input: %#v", input)
	default:
	}
}

func TestTranscriptCoordinatorRejectsInvalidDependencies(t *testing.T) {
	var writer *transcriptWriterFake
	var submit *submitterFake
	assert.Nil(t, NewTranscriptCoordinator(writer, &submitterFake{}, 1))
	assert.Nil(t, NewTranscriptCoordinator(&transcriptWriterFake{}, submit, 1))
	assert.Nil(t, NewTranscriptCoordinator(&transcriptWriterFake{}, &submitterFake{}, 0))
}

func TestTranscriptCoordinatorEnforcesPerItemAndAggregateBytesBeforeCopy(t *testing.T) {
	coordinator := NewTranscriptCoordinator(&transcriptWriterFake{}, &submitterFake{}, 32)
	base := TranscriptWork{
		SessionID: "session", Incarnation: 1,
		Scope: handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "conversation"},
		Turn:  identity.TurnKey{ID: "turn", Revision: 1}, Sequence: 1, Role: "user",
		FinalText: strings.Repeat("x", session.DurableTranscriptBytes),
	}
	accepted := 0
	for index := 0; index < 32; index++ {
		work := base
		work.Sequence = uint64(index + 1)
		err := coordinator.Enqueue(work)
		if errors.Is(err, ErrCoordinatorOverflow) {
			break
		}
		require.NoError(t, err)
		accepted++
	}
	assert.Positive(t, accepted)
	assert.Less(t, accepted, 32, "aggregate byte budget must bind before count capacity")
	assert.LessOrEqual(t, coordinator.retainedBytes, CoordinatorAggregateBytes)

	over := base
	over.FinalText += "x"
	require.ErrorIs(t, coordinator.Enqueue(over), ErrCoordinatorWorkTooLarge)
}

func validTranscriptWork() TranscriptWork {
	return TranscriptWork{
		SessionID: "session", Incarnation: 3,
		Scope: handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "conversation"},
		Turn:  identity.TurnKey{ID: "turn", Revision: 1}, Sequence: 7, Role: "user", FinalText: "committed transcript",
	}
}

func TestTranscriptJournalReplaysCrashAfterAppendWithoutManualReenqueue(t *testing.T) {
	journal := newCoordinatorJournalFake()
	writer := &transcriptWriterFake{items: make(chan postgres.ConversationItem, 2)}
	crash := errors.New("crash after append")
	first := NewTranscriptCoordinator(writer, &submitterFake{err: crash}, 1)
	require.NoError(t, first.configureJournal(journal, "app-a", "owner-1", 100*time.Millisecond))
	work := validTranscriptWork()
	require.NoError(t, first.Enqueue(work))
	require.ErrorIs(t, first.Run(t.Context()), crash)
	firstItem := <-writer.items

	inputs := make(chan session.Input, 1)
	second := NewTranscriptCoordinator(writer, &submitterFake{inputs: inputs}, 1)
	require.NoError(t, second.configureJournal(journal, "app-a", "owner-2", 100*time.Millisecond))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- second.Run(ctx) }()
	secondItem := <-writer.items
	committed := (<-inputs).(session.TurnCommittedInput)
	assert.Equal(t, firstItem.Sequence, secondItem.Sequence, "journal reuses its conversation-wide allocation")
	assert.Equal(t, work.Sequence, committed.Sequence, "reactor fence remains session-local")
	require.Eventually(t, func() bool { return journal.isDelivered(transcriptWorkKey(work)) }, time.Second, time.Millisecond)
	cancel()
	require.NoError(t, <-done)
}

func TestTranscriptJournalStaleIncarnationIsTerminallyAcknowledged(t *testing.T) {
	journal := newCoordinatorJournalFake()
	writer := &transcriptWriterFake{items: make(chan postgres.ConversationItem, 1)}
	coordinator := NewTranscriptCoordinator(writer, &submitterFake{err: session.ErrSessionNotActive}, 1)
	require.NoError(t, coordinator.configureJournal(journal, "app-a", "owner", time.Minute))
	work := validTranscriptWork()
	require.NoError(t, coordinator.Enqueue(work))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- coordinator.Run(ctx) }()
	<-writer.items
	require.Eventually(t, func() bool { return journal.isDelivered(transcriptWorkKey(work)) }, time.Second, time.Millisecond)
	cancel()
	require.NoError(t, <-done)
}

func TestTranscriptJournalTypedCorruptionFailsClosed(t *testing.T) {
	journal := newCoordinatorJournalFake()
	work := validTranscriptWork()
	payload, err := json.Marshal(work)
	require.NoError(t, err)
	record := postgres.CoordinatorJournalRecord{
		WorkKey: transcriptWorkKey(work), Kind: postgres.CoordinatorKindTranscript, Scope: work.Scope,
		SessionID: work.SessionID, Incarnation: work.Incarnation, LocalSequence: work.Sequence,
		StorageSequence: 1, DeliveryAffinity: "app-a", Payload: payload,
	}
	record.SessionID = "wrong-session"
	journal.records[record.WorkKey] = record
	coordinator := NewTranscriptCoordinator(&transcriptWriterFake{}, &submitterFake{}, 1)
	require.NoError(t, coordinator.configureJournal(journal, "app-a", "owner", time.Minute))
	require.EqualError(t, coordinator.Run(t.Context()), "corrupt transcript coordinator journal record")
}

func TestTranscriptCoordinatorForeignAffinityCannotConsumeOrAcknowledge(t *testing.T) {
	journal := newCoordinatorJournalFake()
	work := validTranscriptWork()
	payload, err := json.Marshal(work)
	require.NoError(t, err)
	_, err = journal.AdmitCoordinatorWork(t.Context(), postgres.CoordinatorJournalRecord{
		WorkKey: transcriptWorkKey(work), Kind: postgres.CoordinatorKindTranscript, Scope: work.Scope,
		SessionID: work.SessionID, Incarnation: work.Incarnation, LocalSequence: work.Sequence,
		DeliveryAffinity: "app-a", Payload: payload,
	})
	require.NoError(t, err)

	writer := &transcriptWriterFake{items: make(chan postgres.ConversationItem, 1)}
	foreign := NewTranscriptCoordinator(writer, &submitterFake{err: session.ErrSessionNotActive}, 1)
	require.NoError(t, foreign.configureJournal(journal, "app-b", "worker-b", 20*time.Millisecond))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- foreign.Run(ctx) }()
	time.Sleep(60 * time.Millisecond)
	cancel()
	require.NoError(t, <-done)
	assert.Empty(t, writer.items)
	assert.False(t, journal.isDelivered(transcriptWorkKey(work)))
}

func TestTranscriptJournalRejectsUnknownTypedReplayFieldsBeforeAppend(t *testing.T) {
	journal := newCoordinatorJournalFake()
	work := validTranscriptWork()
	payload, err := json.Marshal(work)
	require.NoError(t, err)
	payload = append(payload[:len(payload)-1], []byte(`,"UnexpectedAuthority":true}`)...)
	record := postgres.CoordinatorJournalRecord{
		WorkKey: transcriptWorkKey(work), Kind: postgres.CoordinatorKindTranscript, Scope: work.Scope,
		SessionID: work.SessionID, Incarnation: work.Incarnation, LocalSequence: work.Sequence,
		StorageSequence: 1, DeliveryAffinity: "app-a", Payload: payload,
	}
	journal.records[record.WorkKey] = record
	writer := &transcriptWriterFake{items: make(chan postgres.ConversationItem, 1)}
	coordinator := NewTranscriptCoordinator(writer, &submitterFake{}, 1)
	require.NoError(t, coordinator.configureJournal(journal, "app-a", "owner", time.Minute))
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	err = coordinator.Run(ctx)
	require.ErrorContains(t, err, "corrupt transcript coordinator journal payload")
	assert.Empty(t, writer.items)
}

func TestTranscriptCoordinatorRenewsLeaseThroughBlockedSubmit(t *testing.T) {
	journal := newCoordinatorJournalFake()
	submit := &blockingSubmitter{
		started: make(chan struct{}), release: make(chan struct{}), inputs: make(chan session.Input, 1),
	}
	coordinator := NewTranscriptCoordinator(&transcriptWriterFake{items: make(chan postgres.ConversationItem, 1)}, submit, 1)
	require.NoError(t, coordinator.configureJournal(journal, "app-a", "worker-a", 30*time.Millisecond))
	work := validTranscriptWork()
	require.NoError(t, coordinator.Enqueue(work))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- coordinator.Run(ctx) }()
	<-submit.started
	time.Sleep(90 * time.Millisecond)
	reclaimed, err := journal.ClaimCoordinatorWork(t.Context(), postgres.CoordinatorKindTranscript, "app-a", "worker-b", 30*time.Millisecond, 1)
	close(submit.release)
	require.NoError(t, err)
	assert.Empty(t, reclaimed, "a live submit lease must not be reclaimable")
	<-submit.inputs
	require.Eventually(t, func() bool { return journal.isDelivered(transcriptWorkKey(work)) }, time.Second, time.Millisecond)
	cancel()
	require.NoError(t, <-done)
}

func TestTranscriptCoordinatorRenewalFailureCancelsBlockedSubmitWithoutAck(t *testing.T) {
	leaseLost := errors.New("lease generation lost during submit")
	journal := newCoordinatorJournalFake()
	submit := &blockingSubmitter{
		started: make(chan struct{}), release: make(chan struct{}), inputs: make(chan session.Input, 1),
	}
	coordinator := NewTranscriptCoordinator(&transcriptWriterFake{items: make(chan postgres.ConversationItem, 1)}, submit, 1)
	require.NoError(t, coordinator.configureJournal(journal, "app-a", "worker-a", 30*time.Millisecond))
	work := validTranscriptWork()
	require.NoError(t, coordinator.Enqueue(work))
	done := make(chan error, 1)
	go func() { done <- coordinator.Run(t.Context()) }()
	<-submit.started
	journal.setRenewError(leaseLost)
	require.ErrorIs(t, <-done, leaseLost)
	assert.False(t, journal.isDelivered(transcriptWorkKey(work)))
	select {
	case input := <-submit.inputs:
		t.Fatalf("lost lease completed blocked transcript submit: %#v", input)
	default:
	}
}
