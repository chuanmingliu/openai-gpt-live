package gateway

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

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
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
	mu                 sync.Mutex
	records            map[string]postgres.CoordinatorJournalRecord
	delivered          map[string]bool
	claimOwner         map[string]string
	claimUntil         map[string]time.Time
	claimGeneration    map[string]uint64
	nextStorage        uint64
	ackErr             error
	renewCount         int
	renewErr           error
	affinityErr        error
	affinityRenewErr   error
	affinityOwner      map[string]string
	affinityGeneration map[string]uint64
	affinityRenewCount int
	affinityRenewStart chan struct{}
	affinityRenewBlock <-chan struct{}
	claimErr           error
	claimCalls         int
}

func newCoordinatorJournalFake() *coordinatorJournalFake {
	return &coordinatorJournalFake{
		records: make(map[string]postgres.CoordinatorJournalRecord), delivered: make(map[string]bool),
		claimOwner: make(map[string]string), claimUntil: make(map[string]time.Time), claimGeneration: make(map[string]uint64),
		affinityOwner: make(map[string]string), affinityGeneration: make(map[string]uint64),
	}
}

func (f *coordinatorJournalFake) AcquireCoordinatorAffinity(_ context.Context, affinity, owner string, _ time.Duration) (uint64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.affinityErr != nil {
		return 0, false, f.affinityErr
	}
	if current := f.affinityOwner[affinity]; current != "" && current != owner {
		return 0, false, nil
	}
	if f.affinityGeneration[affinity] == 0 {
		f.affinityGeneration[affinity] = 1
	}
	f.affinityOwner[affinity] = owner
	return f.affinityGeneration[affinity], true, nil
}

func (f *coordinatorJournalFake) RenewCoordinatorAffinity(_ context.Context, affinity, owner string, generation uint64, _ time.Duration) error {
	f.mu.Lock()
	f.affinityRenewCount++
	if f.affinityRenewErr != nil {
		err := f.affinityRenewErr
		f.mu.Unlock()
		return err
	}
	if f.affinityOwner[affinity] != owner || f.affinityGeneration[affinity] != generation {
		f.mu.Unlock()
		return handoff.ErrNotFound
	}
	started, blocked := f.affinityRenewStart, f.affinityRenewBlock
	f.mu.Unlock()
	if started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	if blocked != nil {
		<-blocked
	}
	return nil
}

func (f *coordinatorJournalFake) affinityRenewals() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.affinityRenewCount
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
	f.claimCalls++
	if f.claimErr != nil {
		return nil, f.claimErr
	}
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

func TestCoordinatorAffinityAuthorityRenewsWithoutJournalWork(t *testing.T) {
	journal := newCoordinatorJournalFake()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- runCoordinatorAffinityAuthority(ctx, journal, "stable-shard", "pod-a", 30*time.Millisecond)
	}()
	require.Eventually(t, func() bool { return journal.affinityRenewals() >= 2 }, time.Second, time.Millisecond)
	cancel()
	require.NoError(t, <-done)
}

func TestCoordinatorAffinityAuthorityCancelsAtConfirmedLeaseWhenRenewBlocks(t *testing.T) {
	journal := newCoordinatorJournalFake()
	renewStarted := make(chan struct{}, 1)
	renewRelease := make(chan struct{})
	journal.affinityRenewStart = renewStarted
	journal.affinityRenewBlock = renewRelease

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	const lease = 30 * time.Millisecond
	go func() {
		done <- runCoordinatorAffinityAuthority(ctx, journal, "stable-shard", "pod-a", lease)
	}()

	select {
	case <-renewStarted:
	case <-time.After(time.Second):
		cancel()
		close(renewRelease)
		<-done
		t.Fatal("coordinator-affinity renewal did not start")
	}
	select {
	case err := <-done:
		close(renewRelease)
		cancel()
		require.ErrorIs(t, err, ErrCoordinatorAffinityLost)
		assert.Equal(t, 1, journal.affinityRenewals(), "one guard must have at most one abandoned authority operation")
	case <-time.After(3 * lease):
		cancel()
		close(renewRelease)
		<-done
		t.Fatal("blocked renewal outlived the last confirmed coordinator-affinity lease")
	}
}

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

type outageTranscriptWriter struct {
	mu      sync.Mutex
	failing bool
	items   []postgres.ConversationItem
}

func (w *outageTranscriptWriter) AppendCommittedItem(_ context.Context, _ handoff.Scope, item postgres.ConversationItem) (postgres.ConversationItem, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failing {
		return postgres.ConversationItem{}, fmt.Errorf("%w: database unavailable", ErrTransientDurability)
	}
	w.items = append(w.items, item)
	return item, nil
}

func TestTranscriptCoordinatorBuffersBoundedlyAndFlushesInOrderDuringStorageGrace(t *testing.T) {
	manual := clock.NewManual(time.Unix(0, 0))
	writer := &outageTranscriptWriter{failing: true}
	inputs := make(chan session.Input, 2)
	coordinator := NewTranscriptCoordinator(writer, &submitterFake{inputs: inputs}, 2)
	require.NoError(t, coordinator.configureOutage(30*time.Second, manual))
	first, second := validTranscriptWork(), validTranscriptWork()
	first.Sequence, first.FinalText = 1, "first"
	second.Sequence, second.FinalText = 2, "second"
	require.NoError(t, coordinator.Enqueue(first))
	require.NoError(t, coordinator.Enqueue(second))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- coordinator.Run(ctx) }()
	require.Eventually(t, func() bool { return coordinator.unavailable.Load() }, time.Second, time.Millisecond)
	writer.mu.Lock()
	writer.failing = false
	writer.mu.Unlock()
	manual.Advance(time.Second)
	require.Equal(t, uint64(1), (<-inputs).(session.TurnCommittedInput).Sequence)
	require.Equal(t, uint64(2), (<-inputs).(session.TurnCommittedInput).Sequence)
	writer.mu.Lock()
	require.Equal(t, []string{"first", "second"}, []string{writer.items[0].Content, writer.items[1].Content})
	writer.mu.Unlock()
	cancel()
	require.NoError(t, <-done)
}

func TestTranscriptCoordinatorClosesAtStorageGraceDeadline(t *testing.T) {
	manual := clock.NewManual(time.Unix(0, 0))
	coordinator := NewTranscriptCoordinator(&transcriptWriterFake{err: fmt.Errorf("%w: database unavailable", ErrTransientDurability)}, &submitterFake{}, 1)
	require.NoError(t, coordinator.configureOutage(30*time.Second, manual))
	require.NoError(t, coordinator.Enqueue(validTranscriptWork()))
	done := make(chan error, 1)
	go func() { done <- coordinator.Run(t.Context()) }()
	require.Eventually(t, func() bool { return coordinator.unavailable.Load() }, time.Second, time.Millisecond)
	// Let the retry waiter register with the manual clock before crossing the
	// entire deadline in one deterministic step.
	time.Sleep(time.Millisecond)
	manual.Advance(30 * time.Second)
	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrDurabilityGraceExpired)
	case <-time.After(time.Second):
		t.Fatal("coordinator did not close at the durability grace deadline")
	}
}

func TestTranscriptPeriodicReplayHonorsAbsoluteStorageGrace(t *testing.T) {
	for _, recoverBeforeDeadline := range []bool{true, false} {
		name := "expires_at_deadline"
		if recoverBeforeDeadline {
			name = "recovers_before_deadline"
		}
		t.Run(name, func(t *testing.T) {
			manual := clock.NewManual(time.Unix(0, 0))
			journal := newCoordinatorJournalFake()
			coordinator := NewTranscriptCoordinator(&transcriptWriterFake{}, &submitterFake{}, 1)
			require.NoError(t, coordinator.configureJournal(journal, "timer-affinity", "timer-owner", 100*time.Millisecond))
			require.NoError(t, coordinator.configureOutage(30*time.Second, manual))
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() { done <- coordinator.Run(ctx) }()
			require.Eventually(t, func() bool {
				journal.mu.Lock()
				defer journal.mu.Unlock()
				return journal.claimCalls >= 1
			}, time.Second, time.Millisecond)
			journal.mu.Lock()
			journal.claimErr = fmt.Errorf("%w: replay database unavailable", ErrTransientDurability)
			journal.mu.Unlock()
			manual.Advance(50 * time.Millisecond)
			require.Eventually(t, func() bool { return coordinator.unavailable.Load() }, time.Second, time.Millisecond)
			manual.Advance(29 * time.Second)
			select {
			case err := <-done:
				t.Fatalf("periodic replay ended before grace deadline: %v", err)
			default:
			}
			if recoverBeforeDeadline {
				journal.mu.Lock()
				journal.claimErr = nil
				journal.mu.Unlock()
			}
			time.Sleep(time.Millisecond)
			manual.Advance(time.Second)
			if recoverBeforeDeadline {
				require.Eventually(t, func() bool { return !coordinator.unavailable.Load() }, time.Second, time.Millisecond)
				cancel()
				require.NoError(t, <-done)
			} else {
				require.ErrorIs(t, <-done, ErrDurabilityGraceExpired)
				cancel()
			}
		})
	}
}

func TestDurabilityRetryClassificationExcludesDeterministicAndControlErrors(t *testing.T) {
	for name, err := range map[string]error{
		"transient":     fmt.Errorf("%w: unavailable", ErrTransientDurability),
		"corrupt":       errors.New("corrupt coordinator journal payload"),
		"lease lost":    handoff.ErrRevisionConflict,
		"stale session": session.ErrSessionNotActive,
		"cancelled":     context.Canceled,
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, name == "transient", transientDurabilityError(err))
		})
	}
	manual := clock.NewManual(time.Unix(0, 0))
	coordinator := NewTranscriptCoordinator(&transcriptWriterFake{}, &submitterFake{}, 1)
	require.NoError(t, coordinator.configureOutage(30*time.Second, manual))
	corrupt := errors.New("corrupt coordinator journal payload")
	calls := 0
	err := coordinator.retryDurability(t.Context(), func() error { calls++; return corrupt })
	require.ErrorIs(t, err, corrupt)
	require.Equal(t, 1, calls)
	require.False(t, coordinator.unavailable.Load())
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	err = coordinator.retryDurability(cancelled, func() error {
		return fmt.Errorf("%w: unavailable", ErrTransientDurability)
	})
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, coordinator.unavailable.Load())
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
