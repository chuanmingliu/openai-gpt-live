package handoff_test

import (
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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type pumpStore struct {
	mu         sync.Mutex
	events     map[handoff.Scope][]handoff.Event
	cursors    map[string]int64
	offered    chan handoff.Event
	offerCalls chan handoff.Event
	loads      chan handoff.Scope
	advances   []int64
	sinkErr    error
	advanceErr error
}

func newPumpStore() *pumpStore {
	return &pumpStore{
		events: make(map[handoff.Scope][]handoff.Event), cursors: make(map[string]int64),
		offered: make(chan handoff.Event, 32), offerCalls: make(chan handoff.Event, 32), loads: make(chan handoff.Scope, 32),
	}
}

func pumpKey(consumer string, scope handoff.Scope) string {
	return consumer + "\x00" + scope.TenantID + "\x00" + scope.UserID + "\x00" + string(scope.ConversationID)
}

func (s *pumpStore) ReadEvents(ctx context.Context, scope handoff.Scope, after int64, limit int) ([]handoff.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []handoff.Event
	for _, event := range s.events[scope] {
		if event.Seq > after {
			out = append(out, handoff.CloneEvent(event))
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}

func (s *pumpStore) LoadDeliveryCursor(ctx context.Context, consumer string, scope handoff.Scope) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case s.loads <- scope:
	default:
	}
	return s.cursors[pumpKey(consumer, scope)], nil
}

func (s *pumpStore) AdvanceDeliveryCursor(ctx context.Context, consumer string, scope handoff.Scope, seq int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.advanceErr != nil {
		return s.advanceErr
	}
	s.cursors[pumpKey(consumer, scope)] = seq
	s.advances = append(s.advances, seq)
	return nil
}

func (s *pumpStore) OfferHandoffEvent(ctx context.Context, _ handoff.Scope, event handoff.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.offerCalls <- handoff.CloneEvent(event)
	s.mu.Lock()
	err := s.sinkErr
	s.mu.Unlock()
	if err != nil {
		return err
	}
	s.offered <- handoff.CloneEvent(event)
	return nil
}

func pumpEvent(scope handoff.Scope, seq int64, kind string, state handoff.State) handoff.Event {
	return handoff.Event{
		Seq: seq, EventID: "event-" + time.Unix(seq, 0).UTC().Format("150405"), HandoffID: "handoff-1",
		TenantID: scope.TenantID, ConversationID: scope.ConversationID, Kind: kind, State: state,
		Payload: json.RawMessage(`{"summary":"bounded"}`), CreatedAt: time.Unix(seq, 0).UTC(),
	}
}

func runPump(t *testing.T, pump *handoff.EventPump) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- pump.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			assert.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			t.Error("event pump did not stop")
		}
	})
	return cancel, done
}

func TestEventPumpOrdersOffersAndAdvancesEachExactCursor(t *testing.T) {
	scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "conversation"}
	store := newPumpStore()
	store.events[scope] = []handoff.Event{
		pumpEvent(scope, 7, "handoff.created", handoff.StatePending),
		pumpEvent(scope, 11, "handoff.completed", handoff.StateSucceeded),
	}
	pump := &handoff.EventPump{Reader: store, Cursors: store, Sink: store, Clock: clock.NewManual(time.Unix(0, 0)), PollInterval: time.Second}
	require.NoError(t, pump.Register(handoff.EventSubscription{ConsumerID: "client", Scope: scope}))
	runPump(t, pump)

	assert.Equal(t, int64(7), (<-store.offered).Seq)
	assert.Equal(t, int64(11), (<-store.offered).Seq)
	require.Eventually(t, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		return assert.Equal(t, []int64{7, 11}, store.advances)
	}, time.Second, time.Millisecond)
}

func TestEventPumpExplicitSafetySuppressionAdvancesCursor(t *testing.T) {
	scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "conversation"}
	store := newPumpStore()
	store.events[scope] = []handoff.Event{pumpEvent(scope, 1, "handoff.progress", handoff.StateRunning)}
	store.sinkErr = handoff.ErrPresentationSuppressed
	pump := &handoff.EventPump{Reader: store, Cursors: store, Sink: store, Clock: clock.Real{}, PollInterval: time.Millisecond}
	require.NoError(t, pump.Register(handoff.EventSubscription{ConsumerID: "consumer", Scope: scope}))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- pump.Run(ctx) }()
	require.Eventually(t, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		return store.cursors[pumpKey("consumer", scope)] == 1
	}, time.Second, time.Millisecond)
	cancel()
	assert.ErrorIs(t, <-done, context.Canceled)
}

func TestEventPumpRecordsPresentationDelayAndReplayDuplicateAtDeliveryBoundary(t *testing.T) {
	scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "conversation"}
	store := newPumpStore()
	event := pumpEvent(scope, 1, "handoff.completed", handoff.StateSucceeded)
	store.events[scope] = []handoff.Event{event}
	store.cursors[pumpKey("consumer", scope)] = 1
	manual := clock.NewManual(event.CreatedAt.Add(40 * time.Millisecond))
	presented := make(chan struct {
		delay  time.Duration
		result string
	}, 1)
	duplicates := make(chan string, 1)
	pump := &handoff.EventPump{
		Reader: store, Cursors: store, Sink: store, Clock: manual, PollInterval: time.Second,
		Metrics: handoff.EventPumpMetrics{
			Presentation: func(delay time.Duration, result string) {
				presented <- struct {
					delay  time.Duration
					result string
				}{delay, result}
			},
			Duplicate: func(result string) { duplicates <- result },
		},
	}
	subscription := handoff.EventSubscription{ConsumerID: "consumer", Scope: scope}
	require.NoError(t, pump.RegisterReplay(subscription, 0))
	runPump(t, pump)
	assert.Equal(t, int64(1), (<-store.offered).Seq)
	metric := <-presented
	assert.Equal(t, 40*time.Millisecond, metric.delay)
	assert.Equal(t, "ok", metric.result)
	assert.Equal(t, "replay", <-duplicates)
}

func TestEventPumpRejectsWrappedJoinedAndForgedPresentationSuppression(t *testing.T) {
	tests := map[string]error{
		"wrapped": fmt.Errorf("context: %w", handoff.ErrPresentationSuppressed),
		"joined":  errors.Join(errors.New("context"), handoff.ErrPresentationSuppressed),
		"forged":  errors.New(handoff.ErrPresentationSuppressed.Error()),
	}
	for name, sinkErr := range tests {
		t.Run(name, func(t *testing.T) {
			scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "conversation"}
			store := newPumpStore()
			store.events[scope] = []handoff.Event{pumpEvent(scope, 1, "handoff.progress", handoff.StateRunning)}
			store.sinkErr = sinkErr
			pump := &handoff.EventPump{Reader: store, Cursors: store, Sink: store, Clock: clock.Real{}, PollInterval: time.Millisecond}
			require.NoError(t, pump.Register(handoff.EventSubscription{ConsumerID: "consumer", Scope: scope}))
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() { done <- pump.Run(ctx) }()
			select {
			case <-store.offerCalls:
			case <-time.After(time.Second):
				t.Fatal("event was not offered")
			}
			cancel()
			assert.ErrorIs(t, <-done, context.Canceled)
			store.mu.Lock()
			defer store.mu.Unlock()
			assert.Zero(t, store.cursors[pumpKey("consumer", scope)])
		})
	}
}

func TestEventPumpReconnectReplayUsesClientCursorWithoutRollingBackGlobalCursor(t *testing.T) {
	scope := handoff.Scope{TenantID: "tenant-replay", UserID: "user", ConversationID: "conversation"}
	store := newPumpStore()
	for seq := int64(1); seq <= 6; seq++ {
		kind, state := "handoff.progress", handoff.StateRunning
		if seq == 4 {
			kind, state = "handoff.needs_input", handoff.StateNeedsInput
		}
		store.events[scope] = append(store.events[scope], pumpEvent(scope, seq, kind, state))
	}
	subscription := handoff.EventSubscription{ConsumerID: "client", Scope: scope}
	store.cursors[pumpKey(subscription.ConsumerID, scope)] = 5
	pump := &handoff.EventPump{Reader: store, Cursors: store, Sink: store, Clock: clock.NewManual(time.Unix(0, 0)), PollInterval: time.Second}
	require.NoError(t, pump.RegisterReplay(subscription, 2))
	_, _ = runPump(t, pump)

	for _, seq := range []int64{3, 4, 6} {
		event := <-store.offered
		assert.Equal(t, seq, event.Seq)
		assert.Equal(t, pumpEvent(scope, seq, event.Kind, event.State).EventID, event.EventID)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	assert.Equal(t, int64(6), store.cursors[pumpKey(subscription.ConsumerID, scope)])
	assert.Equal(t, []int64{6}, store.advances, "replay ACKs never roll the global cursor backward")
}

func TestEventPumpCoalescesOnlyConsecutiveProgressWithoutCursorSkips(t *testing.T) {
	scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "conversation"}
	store := newPumpStore()
	store.events[scope] = []handoff.Event{
		pumpEvent(scope, 1, "handoff.progress", handoff.StateRunning),
		pumpEvent(scope, 2, "handoff.progress", handoff.StateRunning),
		pumpEvent(scope, 3, "handoff.needs_input", handoff.StateNeedsInput),
	}
	pump := &handoff.EventPump{Reader: store, Cursors: store, Sink: store, Clock: clock.NewManual(time.Unix(0, 0)), PollInterval: time.Second}
	require.NoError(t, pump.Register(handoff.EventSubscription{ConsumerID: "client", Scope: scope}))
	runPump(t, pump)

	assert.Equal(t, int64(2), (<-store.offered).Seq)
	assert.Equal(t, int64(3), (<-store.offered).Seq)
	require.Eventually(t, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		return assert.Equal(t, []int64{1, 2, 3}, store.advances)
	}, time.Second, time.Millisecond)
}

func TestEventPumpScopesNotificationsAndRepairsLostWakeups(t *testing.T) {
	manual := clock.NewManual(time.Unix(0, 0))
	wakeups := make(chan handoff.EventWakeup, 4)
	store := newPumpStore()
	one := handoff.Scope{TenantID: "tenant-a", UserID: "user-a", ConversationID: "shared"}
	two := handoff.Scope{TenantID: "tenant-b", UserID: "user-b", ConversationID: "shared"}
	pump := &handoff.EventPump{Reader: store, Cursors: store, Notifications: wakeups, Sink: store, Clock: manual, PollInterval: time.Second}
	require.NoError(t, pump.Register(handoff.EventSubscription{ConsumerID: "client", Scope: one}))
	require.NoError(t, pump.Register(handoff.EventSubscription{ConsumerID: "client", Scope: two}))
	runPump(t, pump)
	<-store.loads
	<-store.loads

	store.mu.Lock()
	store.events[one] = []handoff.Event{pumpEvent(one, 1, "handoff.created", handoff.StatePending)}
	store.events[two] = []handoff.Event{pumpEvent(two, 2, "handoff.created", handoff.StatePending)}
	store.mu.Unlock()
	wakeups <- handoff.EventWakeup{TenantID: one.TenantID, ConversationID: one.ConversationID, HighestSeq: 1}
	assert.Equal(t, one.TenantID, (<-store.offered).TenantID)
	<-store.loads
	require.Eventually(t, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		return store.cursors[pumpKey("client", one)] == 1
	}, time.Second, time.Millisecond)
	wakeups <- handoff.EventWakeup{TenantID: one.TenantID, ConversationID: one.ConversationID, HighestSeq: 1}
	wakeups <- handoff.EventWakeup{TenantID: one.TenantID, ConversationID: one.ConversationID, HighestSeq: 1}
	<-store.loads
	<-store.loads
	select {
	case duplicate := <-store.offered:
		t.Fatalf("duplicate notification replayed acknowledged event %s", duplicate.EventID)
	default:
	}

	manual.Advance(time.Second)
	assert.Equal(t, two.TenantID, (<-store.offered).TenantID)
}

func TestEventPumpLeavesFailingEventReplayableAcrossRestart(t *testing.T) {
	scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "conversation"}
	store := newPumpStore()
	store.events[scope] = []handoff.Event{pumpEvent(scope, 9, "handoff.completed", handoff.StateSucceeded)}
	store.advanceErr = errors.New("cursor unavailable")
	first := &handoff.EventPump{Reader: store, Cursors: store, Sink: store, Clock: clock.NewManual(time.Unix(0, 0)), PollInterval: time.Second}
	require.NoError(t, first.Register(handoff.EventSubscription{ConsumerID: "stable", Scope: scope}))
	cancel, _ := runPump(t, first)
	delivered := <-store.offered
	cancel()

	store.mu.Lock()
	store.advanceErr = nil
	store.mu.Unlock()
	second := &handoff.EventPump{Reader: store, Cursors: store, Sink: store, Clock: clock.NewManual(time.Unix(0, 0)), PollInterval: time.Second}
	require.NoError(t, second.Register(handoff.EventSubscription{ConsumerID: "stable", Scope: scope}))
	runPump(t, second)
	replayed := <-store.offered
	assert.Equal(t, delivered.EventID, replayed.EventID)
	assert.Equal(t, delivered.Seq, replayed.Seq)
}

func TestEventPumpValidatesDependenciesSubscriptionsAndBackpressure(t *testing.T) {
	scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: identity.ConversationID("conversation")}
	store := newPumpStore()
	pump := &handoff.EventPump{Reader: store, Cursors: store, Sink: store, Clock: clock.NewManual(time.Unix(0, 0)), PollInterval: time.Millisecond}
	assert.Error(t, pump.Register(handoff.EventSubscription{}))
	assert.Error(t, pump.Register(handoff.EventSubscription{ConsumerID: "client", Scope: handoff.Scope{TenantID: "tenant", ConversationID: "conversation"}}))
	require.NoError(t, pump.Register(handoff.EventSubscription{ConsumerID: "client", Scope: scope}))
	require.NoError(t, pump.Register(handoff.EventSubscription{ConsumerID: "client", Scope: scope}), "exact registration is idempotent")
	assert.Error(t, (&handoff.EventPump{}).Run(context.Background()))
	assert.Error(t, pump.Run(nil))

	store.events[scope] = []handoff.Event{pumpEvent(scope, 1, "handoff.created", handoff.StatePending)}
	store.sinkErr = handoff.ErrNoLiveSession
	manual := pump.Clock.(*clock.Manual)
	runPump(t, pump)
	<-store.offerCalls
	manual.Advance(time.Millisecond)
	store.mu.Lock()
	assert.Zero(t, store.cursors[pumpKey("client", scope)])
	store.sinkErr = nil
	store.mu.Unlock()
	manual.Advance(time.Millisecond)
	assert.Equal(t, int64(1), (<-store.offered).Seq, "offline event replays after a live session returns")
}

func TestEventPumpRejectsExcessivelyDeepPayloadBeforeSink(t *testing.T) {
	scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "conversation"}
	store := newPumpStore()
	event := pumpEvent(scope, 1, "handoff.completed", handoff.StateSucceeded)
	event.Payload = json.RawMessage(strings.Repeat(`{"nested":`, 40) + `0` + strings.Repeat(`}`, 40))
	store.events[scope] = []handoff.Event{event}
	pump := &handoff.EventPump{Reader: store, Cursors: store, Sink: store, Clock: clock.NewManual(time.Unix(0, 0)), PollInterval: time.Second}
	require.NoError(t, pump.Register(handoff.EventSubscription{ConsumerID: "client", Scope: scope}))
	runPump(t, pump)

	select {
	case <-store.offered:
		t.Fatal("excessively deep payload reached presentation sink")
	case <-time.After(50 * time.Millisecond):
	}
	store.mu.Lock()
	assert.Zero(t, store.cursors[pumpKey("client", scope)])
	store.mu.Unlock()
}

func TestEventPumpRegisterUnregisterRaceDoesNotReuseAnotherScope(t *testing.T) {
	store := newPumpStore()
	pump := &handoff.EventPump{Reader: store, Cursors: store, Sink: store, Clock: clock.NewManual(time.Unix(0, 0)), PollInterval: time.Second}
	runPump(t, pump)
	var group sync.WaitGroup
	for index := 0; index < 200; index++ {
		scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: identity.ConversationID("conversation-" + time.Unix(int64(index), 0).Format("150405"))}
		subscription := handoff.EventSubscription{ConsumerID: "client", Scope: scope}
		group.Add(1)
		go func() {
			defer group.Done()
			require.NoError(t, pump.Register(subscription))
			pump.Unregister(subscription)
		}()
	}
	group.Wait()
	finalScope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "final"}
	require.NoError(t, pump.Register(handoff.EventSubscription{ConsumerID: "client", Scope: finalScope}))
}
