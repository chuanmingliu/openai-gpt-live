package session

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type subscriptionRecorder struct {
	mu           sync.Mutex
	registered   map[handoff.EventSubscription]int
	unregistered map[handoff.EventSubscription]int
}

func newSubscriptionRecorder() *subscriptionRecorder {
	return &subscriptionRecorder{registered: make(map[handoff.EventSubscription]int), unregistered: make(map[handoff.EventSubscription]int)}
}

func (r *subscriptionRecorder) Register(subscription handoff.EventSubscription) error {
	r.mu.Lock()
	r.registered[subscription]++
	r.mu.Unlock()
	return nil
}

func (r *subscriptionRecorder) Unregister(subscription handoff.EventSubscription) {
	r.mu.Lock()
	r.unregistered[subscription]++
	r.mu.Unlock()
}

func currentRuntime(scope handoff.Scope, sessionID identity.SessionID) (*sessionRuntime, chan Input, chan struct{}) {
	inbox := make(chan reactorMessage, 4)
	slots := make(chan struct{}, 4)
	for range cap(slots) {
		slots <- struct{}{}
	}
	done := make(chan struct{})
	inputs := make(chan Input, 4)
	state := &runState{ids: IDs{SessionID: sessionID, ConversationID: scope.ConversationID, Incarnation: 1}}
	runtime := &sessionRuntime{
		key: runtimeKey{sessionID: sessionID, incarnation: 1}, subject: Subject{TenantID: scope.TenantID, UserID: scope.UserID},
		inbox: inbox, done: done, state: state, accepting: true, inboxSlots: slots,
	}
	go func() {
		for {
			select {
			case message := <-inbox:
				inputs <- message.input
				if message.done != nil {
					message.done <- nil
				}
				slots <- struct{}{}
			case <-done:
				return
			}
		}
	}()
	return runtime, inputs, done
}

func presentationManager(recorder HandoffSubscriptionRegistrar) *Manager {
	return &Manager{
		cfg:    Config{HandoffConsumerID: "stable-client", HandoffSubscriptions: recorder},
		active: make(map[runtimeKey]*sessionRuntime), current: make(map[handoff.Scope]*sessionRuntime),
	}
}

func TestCurrentSessionDirectoryUsesFullTenantUserConversationScope(t *testing.T) {
	manager := presentationManager(newSubscriptionRecorder())
	one := handoff.Scope{TenantID: "tenant-a", UserID: "user-a", ConversationID: "shared"}
	two := handoff.Scope{TenantID: "tenant-b", UserID: "user-b", ConversationID: "shared"}
	runtimeOne, inputOne, doneOne := currentRuntime(one, "session-a")
	runtimeTwo, inputTwo, doneTwo := currentRuntime(two, "session-b")
	t.Cleanup(func() { close(doneOne); close(doneTwo) })
	require.NoError(t, manager.register(runtimeOne))
	require.NoError(t, manager.register(runtimeTwo))

	require.NoError(t, manager.OfferHandoffEvent(t.Context(), one, handoff.Event{Seq: 1}))
	require.NoError(t, manager.OfferHandoffEvent(t.Context(), two, handoff.Event{Seq: 2}))
	assert.Equal(t, int64(1), (<-inputOne).(HandoffEventInput).Event.Seq)
	assert.Equal(t, int64(2), (<-inputTwo).(HandoffEventInput).Event.Seq)
}

func TestCurrentSessionReplacementCannotDeliverToOrUnregisterStaleRuntime(t *testing.T) {
	recorder := newSubscriptionRecorder()
	manager := presentationManager(recorder)
	scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "conversation"}
	oldRuntime, oldInputs, oldDone := currentRuntime(scope, "old-session")
	newRuntime, newInputs, newDone := currentRuntime(scope, "new-session")
	t.Cleanup(func() { close(oldDone); close(newDone) })
	require.NoError(t, manager.register(oldRuntime))
	require.NoError(t, manager.register(newRuntime))
	manager.unregister(oldRuntime.key, oldRuntime)

	require.NoError(t, manager.OfferHandoffEvent(t.Context(), scope, handoff.Event{Seq: 3}))
	assert.Equal(t, int64(3), (<-newInputs).(HandoffEventInput).Event.Seq)
	select {
	case <-oldInputs:
		t.Fatal("stale runtime received current handoff")
	default:
	}

	subscription := handoff.EventSubscription{ConsumerID: "stable-client", Scope: scope}
	recorder.mu.Lock()
	assert.Zero(t, recorder.unregistered[subscription], "stale teardown cannot unregister replacement delivery")
	recorder.mu.Unlock()
	manager.unregister(newRuntime.key, newRuntime)
	assert.ErrorIs(t, manager.OfferHandoffEvent(t.Context(), scope, handoff.Event{Seq: 4}), handoff.ErrNoLiveSession)
	recorder.mu.Lock()
	assert.Equal(t, 1, recorder.unregistered[subscription])
	recorder.mu.Unlock()
}

func TestCurrentSessionOfferFailsReplayablyWhenDisconnectWinsAcceptance(t *testing.T) {
	manager := presentationManager(newSubscriptionRecorder())
	scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "conversation"}
	runtime, _, done := currentRuntime(scope, "session")
	t.Cleanup(func() { close(done) })
	require.NoError(t, manager.register(runtime))
	runtime.stop()

	err := manager.OfferHandoffEvent(t.Context(), scope, handoff.Event{Seq: 1})
	assert.ErrorIs(t, err, handoff.ErrNoLiveSession)
}

func TestCurrentSessionReplacementCannotWinBetweenLookupAndVisualAcceptance(t *testing.T) {
	manager := presentationManager(newSubscriptionRecorder())
	manager.cfg.EgressBlockTimeout = time.Second
	scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "conversation"}
	inbox := make(chan reactorMessage, 1)
	slots := make(chan struct{}, 1)
	slots <- struct{}{}
	done := make(chan struct{})
	oldRuntime := &sessionRuntime{
		key: runtimeKey{sessionID: "old", incarnation: 1}, subject: Subject{TenantID: scope.TenantID, UserID: scope.UserID},
		inbox: inbox, done: done, state: &runState{ids: IDs{SessionID: "old", ConversationID: scope.ConversationID, Incarnation: 1}},
		accepting: true, inboxSlots: slots,
	}
	require.NoError(t, manager.register(oldRuntime))
	offered := make(chan error, 1)
	go func() { offered <- manager.OfferHandoffEvent(context.Background(), scope, handoff.Event{Seq: 1}) }()
	message := <-inbox

	newRuntime, _, newDone := currentRuntime(scope, "new")
	t.Cleanup(func() { close(done); close(newDone) })
	replaced := make(chan error, 1)
	go func() { replaced <- manager.register(newRuntime) }()
	select {
	case err := <-replaced:
		t.Fatalf("replacement won before visual acceptance: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	message.done <- nil
	require.NoError(t, <-offered)
	require.NoError(t, <-replaced)
}

func TestCurrentSessionRegistrationAndUnregistrationAreRaceSafe(t *testing.T) {
	manager := presentationManager(newSubscriptionRecorder())
	scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "conversation"}
	var group sync.WaitGroup
	for index := 0; index < 100; index++ {
		runtime, _, done := currentRuntime(scope, identity.SessionID(time.Unix(int64(index), 0).Format("150405.000000000")))
		group.Add(1)
		go func() {
			defer group.Done()
			if manager.register(runtime) == nil {
				manager.unregister(runtime.key, runtime)
			}
			close(done)
		}()
		group.Add(1)
		go func() {
			defer group.Done()
			err := manager.OfferHandoffEvent(context.Background(), scope, handoff.Event{Seq: 1})
			if err != nil && !errors.Is(err, handoff.ErrNoLiveSession) && !errors.Is(err, ErrControlOverflow) {
				t.Errorf("unexpected offer error: %v", err)
			}
		}()
	}
	group.Wait()
}

var _ HandoffSubscriptionRegistrar = (*subscriptionRecorder)(nil)
