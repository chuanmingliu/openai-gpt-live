package session

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/presentation"
	"github.com/chuanmingliu/openai-gpt-live/internal/protocol"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/chuanmingliu/openai-gpt-live/internal/safety"
	"github.com/chuanmingliu/openai-gpt-live/internal/testkit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type deliveryStore struct {
	mu         sync.Mutex
	event      handoff.Event
	cursor     int64
	advanceErr error
	loads      int
	advances   int
}

func (s *deliveryStore) ReadEvents(ctx context.Context, _ handoff.Scope, after int64, _ int) ([]handoff.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.event.Seq <= after {
		return nil, nil
	}
	return []handoff.Event{handoff.CloneEvent(s.event)}, nil
}

func (s *deliveryStore) LoadDeliveryCursor(ctx context.Context, _ string, _ handoff.Scope) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loads++
	return s.cursor, nil
}

func (s *deliveryStore) AdvanceDeliveryCursor(ctx context.Context, _ string, _ handoff.Scope, seq int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.advances++
	if s.advanceErr != nil {
		return s.advanceErr
	}
	s.cursor = seq
	return nil
}

type deliveryClient struct {
	done             chan struct{}
	sessionStarted   chan struct{}
	sessionRelease   chan struct{}
	visualStarted    chan protocol.ServerEvent
	visualRelease    chan struct{}
	visualErr        error
	reconnectStarted chan protocol.SessionReconnectUpdatedEvent
	reconnectRelease chan struct{}
	closeOnce        sync.Once
	sessionOnce      sync.Once
}

func newDeliveryClient() *deliveryClient {
	released := make(chan struct{})
	close(released)
	return &deliveryClient{
		done: make(chan struct{}), sessionStarted: make(chan struct{}), sessionRelease: released,
		visualStarted: make(chan protocol.ServerEvent, 8), visualRelease: released,
	}
}

func (c *deliveryClient) Read(ctx context.Context) (protocol.ClientEvent, error) {
	select {
	case <-c.done:
		return nil, io.EOF
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *deliveryClient) Write(ctx context.Context, event protocol.ServerEvent) error {
	if event.EventType() == "session.created" {
		c.sessionOnce.Do(func() { close(c.sessionStarted) })
		select {
		case <-c.sessionRelease:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if event.EventType() == "live.handoff.created" || event.EventType() == "live.handoff.needs_input" || event.EventType() == "live.tool.approval.required" || event.EventType() == "live.session.degraded" {
		select {
		case c.visualStarted <- event:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case <-c.visualRelease:
			return c.visualErr
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if typed, ok := event.(protocol.SessionReconnectUpdatedEvent); ok && c.reconnectStarted != nil {
		select {
		case c.reconnectStarted <- typed:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case <-c.reconnectRelease:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (c *deliveryClient) Close(context.Context, int, string) error {
	c.closeOnce.Do(func() { close(c.done) })
	return nil
}

type deliveryStack struct {
	managerCancel context.CancelFunc
	pumpCancel    context.CancelFunc
	managerDone   <-chan error
	pumpDone      <-chan error
	manual        *clock.Manual
}

type deliverySequenceIDs struct {
	mu       sync.Mutex
	sessions []identity.SessionID
	next     int
}

func (i *deliverySequenceIDs) NewSessionID() identity.SessionID {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.next >= len(i.sessions) {
		return "delivery-extra"
	}
	sessionID := i.sessions[i.next]
	i.next++
	return sessionID
}

func (*deliverySequenceIDs) NewConversationID() identity.ConversationID { return "unused" }

type deliveryRecordingSink struct {
	manager *Manager
	results chan error
}

type blockingContextSafety struct {
	started   chan struct{}
	cancelled chan struct{}
	mu        sync.Mutex
	calls     int
}

func (p *blockingContextSafety) EvaluateWorkerResult(ctx context.Context, _ safety.WorkerResultCandidate) (safety.Decision, error) {
	p.mu.Lock()
	p.calls++
	call := p.calls
	p.mu.Unlock()
	if call == 1 {
		return safety.Decision{Action: safety.ActionAllow, ReasonCode: "visual_allow"}, nil
	}
	close(p.started)
	<-ctx.Done()
	close(p.cancelled)
	return safety.Decision{}, ctx.Err()
}

type countingSafety struct {
	mu    sync.Mutex
	calls int
}

func (p *countingSafety) EvaluateWorkerResult(context.Context, safety.WorkerResultCandidate) (safety.Decision, error) {
	p.mu.Lock()
	p.calls++
	p.mu.Unlock()
	return safety.Decision{Action: safety.ActionAllow, ReasonCode: "allow"}, nil
}

type cleanupVoice struct {
	closeStarted chan struct{}
	closeRelease chan struct{}
	closeDone    chan struct{}
	startOnce    sync.Once
	doneOnce     sync.Once
}

func (v *cleanupVoice) Send(ctx context.Context, _ realtime.Command) error {
	return ctx.Err()
}

func (v *cleanupVoice) Receive(ctx context.Context) (realtime.Event, error) {
	<-ctx.Done()
	return realtime.Event{}, ctx.Err()
}

func (v *cleanupVoice) Close(ctx context.Context) error {
	v.startOnce.Do(func() { close(v.closeStarted) })
	var err error
	select {
	case <-v.closeRelease:
	case <-ctx.Done():
		err = ctx.Err()
	}
	v.doneOnce.Do(func() { close(v.closeDone) })
	return err
}

type cleanupConnector struct {
	mu     sync.Mutex
	voices []*cleanupVoice
	next   int
}

type blockingTombstoneOwners struct {
	*MemoryReconnectOwners
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingTombstoneOwners) Tombstone(
	ctx context.Context,
	_ identity.SessionID,
	_ identity.Incarnation,
	_ string,
	_ identity.ResponseEpoch,
	_ bool,
) error {
	s.once.Do(func() { close(s.started) })
	select {
	case <-s.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type signalingSubscriptions struct {
	mu         sync.Mutex
	registered int
	second     chan struct{}
	once       sync.Once
}

type blockingSecondRegisterSubscriptions struct {
	mu      sync.Mutex
	calls   int
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingSecondRegisterSubscriptions) Register(handoff.EventSubscription) error {
	s.mu.Lock()
	s.calls++
	second := s.calls == 2
	s.mu.Unlock()
	if !second {
		return nil
	}
	s.once.Do(func() { close(s.started) })
	<-s.release
	return nil
}

func (*blockingSecondRegisterSubscriptions) Unregister(handoff.EventSubscription) {}
func (s *blockingSecondRegisterSubscriptions) BeginSubscription(subscription handoff.EventSubscription) (handoff.SubscriptionAttempt, error) {
	if err := s.Register(subscription); err != nil {
		return nil, err
	}
	return &callbackSubscriptionAttempt{}, nil
}
func (*blockingSecondRegisterSubscriptions) BeginReplay(handoff.EventSubscription, int64) (handoff.ReplayAttempt, error) {
	return &replayAttemptCallbacks{}, nil
}

type reentrantRegisterSubscriptions struct {
	mu      sync.Mutex
	calls   int
	manager *Manager
	scope   handoff.Scope
	event   handoff.Event
}

func (s *reentrantRegisterSubscriptions) Register(handoff.EventSubscription) error {
	s.mu.Lock()
	s.calls++
	second := s.calls == 2
	s.mu.Unlock()
	if !second {
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- s.manager.OfferHandoffEvent(context.Background(), s.scope, s.event) }()
	select {
	case err := <-done:
		return err
	case <-time.After(time.Second):
		return errors.New("synchronous reentrant offer blocked behind registrar callback")
	}
}

func (*reentrantRegisterSubscriptions) Unregister(handoff.EventSubscription) {}
func (s *reentrantRegisterSubscriptions) BeginSubscription(subscription handoff.EventSubscription) (handoff.SubscriptionAttempt, error) {
	if err := s.Register(subscription); err != nil {
		return nil, err
	}
	return &callbackSubscriptionAttempt{}, nil
}
func (*reentrantRegisterSubscriptions) BeginReplay(handoff.EventSubscription, int64) (handoff.ReplayAttempt, error) {
	return &replayAttemptCallbacks{}, nil
}

type racingReplaySubscriptions struct {
	mu            sync.Mutex
	registered    bool
	replayOverlay bool
	replayAfter   int64
	replayStarted chan struct{}
	replayRelease chan struct{}
	once          sync.Once
}

type blockingUnregisterSubscriptions struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (*blockingUnregisterSubscriptions) Register(handoff.EventSubscription) error { return nil }

func (s *blockingUnregisterSubscriptions) Unregister(handoff.EventSubscription) {
	s.once.Do(func() { close(s.started) })
	<-s.release
}
func (s *blockingUnregisterSubscriptions) BeginSubscription(handoff.EventSubscription) (handoff.SubscriptionAttempt, error) {
	return &callbackSubscriptionAttempt{commit: func() (handoff.SubscriptionLease, error) {
		return &callbackSubscriptionLease{release: func() { s.Unregister(handoff.EventSubscription{}) }}, nil
	}}, nil
}
func (*blockingUnregisterSubscriptions) BeginReplay(handoff.EventSubscription, int64) (handoff.ReplayAttempt, error) {
	return &replayAttemptCallbacks{}, nil
}

type reentrantUnregisterSubscriptions struct {
	manager  *Manager
	runtime  *sessionRuntime
	once     sync.Once
	timedOut atomic.Bool
}

type replayRegistrarProbe struct {
	pump            *handoff.EventPump
	mu              sync.Mutex
	registerCalls   int
	replayInstalled chan struct{}
	replayRelease   <-chan struct{}
	replayErr       error
	restoreErr      error
	installOnce     sync.Once
}

type gatedCommitReplayRegistrar struct {
	pump          *handoff.EventPump
	commitStarted chan struct{}
	commitRelease <-chan struct{}
	wakeResults   <-chan error
	waitCtx       context.Context
}

func (r *gatedCommitReplayRegistrar) Register(subscription handoff.EventSubscription) error {
	return r.pump.Register(subscription)
}

func (r *gatedCommitReplayRegistrar) Unregister(subscription handoff.EventSubscription) {
	r.pump.Unregister(subscription)
}

func (r *gatedCommitReplayRegistrar) BeginSubscription(subscription handoff.EventSubscription) (handoff.SubscriptionAttempt, error) {
	return r.pump.BeginSubscription(subscription)
}

func (r *gatedCommitReplayRegistrar) BeginReplay(subscription handoff.EventSubscription, after int64) (handoff.ReplayAttempt, error) {
	attempt, err := r.pump.BeginReplay(subscription, after)
	if err != nil {
		return nil, err
	}
	return &gatedCommitReplayAttempt{
		ReplayAttempt: attempt, commitStarted: r.commitStarted, commitRelease: r.commitRelease,
		wakeResults: r.wakeResults, waitCtx: r.waitCtx,
	}, nil
}

type gatedCommitReplayAttempt struct {
	handoff.ReplayAttempt
	commitStarted chan struct{}
	commitRelease <-chan struct{}
	wakeResults   <-chan error
	waitCtx       context.Context
	startedOnce   sync.Once
}

func (a *gatedCommitReplayAttempt) Commit() (handoff.SubscriptionLease, error) {
	lease, err := a.ReplayAttempt.Commit()
	a.startedOnce.Do(func() { close(a.commitStarted) })
	select {
	case <-a.commitRelease:
	case <-a.waitCtx.Done():
	}
	return lease, err
}

func (a *gatedCommitReplayAttempt) Notify() {
	a.ReplayAttempt.Notify()
	if a.wakeResults != nil {
		select {
		case <-a.wakeResults:
		case <-a.waitCtx.Done():
		}
	}
}

type preCommitReplayRegistrar struct {
	pump          *handoff.EventPump
	commitStarted chan struct{}
	commitRelease <-chan struct{}
	waitCtx       context.Context
}

func (r *preCommitReplayRegistrar) Register(subscription handoff.EventSubscription) error {
	return r.pump.Register(subscription)
}

func (r *preCommitReplayRegistrar) Unregister(subscription handoff.EventSubscription) {
	r.pump.Unregister(subscription)
}

func (r *preCommitReplayRegistrar) BeginSubscription(subscription handoff.EventSubscription) (handoff.SubscriptionAttempt, error) {
	return r.pump.BeginSubscription(subscription)
}

func (r *preCommitReplayRegistrar) BeginReplay(subscription handoff.EventSubscription, after int64) (handoff.ReplayAttempt, error) {
	attempt, err := r.pump.BeginReplay(subscription, after)
	if err != nil {
		return nil, err
	}
	return &preCommitReplayAttempt{
		ReplayAttempt: attempt, commitStarted: r.commitStarted, commitRelease: r.commitRelease, waitCtx: r.waitCtx,
	}, nil
}

type preCommitReplayAttempt struct {
	handoff.ReplayAttempt
	commitStarted chan struct{}
	commitRelease <-chan struct{}
	waitCtx       context.Context
	startedOnce   sync.Once
}

func (a *preCommitReplayAttempt) Commit() (handoff.SubscriptionLease, error) {
	a.startedOnce.Do(func() { close(a.commitStarted) })
	select {
	case <-a.commitRelease:
		return a.ReplayAttempt.Commit()
	case <-a.waitCtx.Done():
		return nil, a.waitCtx.Err()
	}
}

func (r *replayRegistrarProbe) Register(subscription handoff.EventSubscription) error {
	r.mu.Lock()
	r.registerCalls++
	call := r.registerCalls
	r.mu.Unlock()
	if call > 1 && r.restoreErr != nil {
		return r.restoreErr
	}
	return r.pump.Register(subscription)
}

func (r *replayRegistrarProbe) RegisterReplay(subscription handoff.EventSubscription, after int64) error {
	if err := r.pump.RegisterReplay(subscription, after); err != nil {
		return err
	}
	if r.replayInstalled != nil {
		r.installOnce.Do(func() { close(r.replayInstalled) })
	}
	if r.replayRelease != nil {
		<-r.replayRelease
	}
	return r.replayErr
}

func (r *replayRegistrarProbe) BeginReplay(subscription handoff.EventSubscription, after int64) (handoff.ReplayAttempt, error) {
	attempt, err := r.pump.BeginReplay(subscription, after)
	if err != nil {
		return nil, err
	}
	if r.replayInstalled != nil {
		r.installOnce.Do(func() { close(r.replayInstalled) })
	}
	if r.replayRelease != nil {
		<-r.replayRelease
	}
	return attempt, r.replayErr
}

func (r *replayRegistrarProbe) Unregister(subscription handoff.EventSubscription) {
	r.pump.Unregister(subscription)
}

func (r *replayRegistrarProbe) BeginSubscription(subscription handoff.EventSubscription) (handoff.SubscriptionAttempt, error) {
	attempt, err := r.pump.BeginSubscription(subscription)
	if err != nil {
		return nil, err
	}
	return &callbackSubscriptionAttempt{
		commit: func() (handoff.SubscriptionLease, error) {
			r.mu.Lock()
			r.registerCalls++
			r.mu.Unlock()
			return attempt.Commit()
		},
		rollback: attempt.Rollback,
	}, nil
}

type observingDeliveryStore struct {
	*deliveryStore
	readAfter chan int64
	readDone  chan int64
	release   <-chan struct{}
}

type sequenceDeliveryStore struct {
	mu       sync.Mutex
	events   []handoff.Event
	cursor   int64
	advances []int64
}

func (s *sequenceDeliveryStore) ReadEvents(ctx context.Context, _ handoff.Scope, after int64, limit int) ([]handoff.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	events := make([]handoff.Event, 0, limit)
	for _, event := range s.events {
		if event.Seq <= after {
			continue
		}
		events = append(events, handoff.CloneEvent(event))
		if len(events) == limit {
			break
		}
	}
	return events, nil
}

func (s *sequenceDeliveryStore) LoadDeliveryCursor(ctx context.Context, _ string, _ handoff.Scope) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cursor, nil
}

func (s *sequenceDeliveryStore) AdvanceDeliveryCursor(ctx context.Context, _ string, _ handoff.Scope, seq int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	s.cursor = seq
	s.advances = append(s.advances, seq)
	s.mu.Unlock()
	return nil
}

func (s *observingDeliveryStore) ReadEvents(ctx context.Context, scope handoff.Scope, after int64, limit int) ([]handoff.Event, error) {
	select {
	case s.readAfter <- after:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if s.release != nil {
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	events, err := s.deliveryStore.ReadEvents(ctx, scope, after, limit)
	if s.readDone != nil {
		select {
		case s.readDone <- after:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return events, err
}

func (*reentrantUnregisterSubscriptions) Register(handoff.EventSubscription) error { return nil }

func (s *reentrantUnregisterSubscriptions) Unregister(handoff.EventSubscription) {
	s.once.Do(func() {
		done := make(chan error, 1)
		go func() { done <- s.manager.unregister(s.runtime.key, s.runtime) }()
		select {
		case <-done:
		case <-time.After(time.Second):
			s.timedOut.Store(true)
		}
	})
}
func (s *reentrantUnregisterSubscriptions) BeginSubscription(handoff.EventSubscription) (handoff.SubscriptionAttempt, error) {
	return &callbackSubscriptionAttempt{commit: func() (handoff.SubscriptionLease, error) {
		return &callbackSubscriptionLease{release: func() { s.Unregister(handoff.EventSubscription{}) }}, nil
	}}, nil
}
func (*reentrantUnregisterSubscriptions) BeginReplay(handoff.EventSubscription, int64) (handoff.ReplayAttempt, error) {
	return &replayAttemptCallbacks{}, nil
}

func (s *racingReplaySubscriptions) Register(handoff.EventSubscription) error {
	s.mu.Lock()
	s.registered = true
	s.mu.Unlock()
	return nil
}

func (s *racingReplaySubscriptions) RegisterReplay(_ handoff.EventSubscription, after int64) error {
	s.mu.Lock()
	s.registered = true
	s.replayOverlay = true
	s.replayAfter = after
	s.mu.Unlock()
	s.once.Do(func() { close(s.replayStarted) })
	<-s.replayRelease
	return nil
}

func (s *racingReplaySubscriptions) BeginReplay(_ handoff.EventSubscription, after int64) (handoff.ReplayAttempt, error) {
	s.once.Do(func() { close(s.replayStarted) })
	<-s.replayRelease
	return &replayAttemptCallbacks{commit: func() {
		s.mu.Lock()
		s.registered = true
		s.replayOverlay = true
		s.replayAfter = after
		s.mu.Unlock()
	}, rollback: func() {
		s.mu.Lock()
		s.replayOverlay = false
		s.mu.Unlock()
	}}, nil
}

func (s *racingReplaySubscriptions) BeginSubscription(subscription handoff.EventSubscription) (handoff.SubscriptionAttempt, error) {
	if err := s.Register(subscription); err != nil {
		return nil, err
	}
	return &callbackSubscriptionAttempt{}, nil
}

func (s *racingReplaySubscriptions) Unregister(handoff.EventSubscription) {
	s.mu.Lock()
	s.registered = false
	s.replayOverlay = false
	s.mu.Unlock()
}

func (s *signalingSubscriptions) Register(handoff.EventSubscription) error {
	s.mu.Lock()
	s.registered++
	second := s.registered == 2
	s.mu.Unlock()
	if second {
		s.once.Do(func() { close(s.second) })
	}
	return nil
}

func (*signalingSubscriptions) Unregister(handoff.EventSubscription) {}

func (s *signalingSubscriptions) BeginSubscription(subscription handoff.EventSubscription) (handoff.SubscriptionAttempt, error) {
	if err := s.Register(subscription); err != nil {
		return nil, err
	}
	return &callbackSubscriptionAttempt{}, nil
}

func (*signalingSubscriptions) BeginReplay(handoff.EventSubscription, int64) (handoff.ReplayAttempt, error) {
	return &replayAttemptCallbacks{}, nil
}

func (c *cleanupConnector) Connect(ctx context.Context, _ realtime.OpenRequest) (realtime.Session, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.next >= len(c.voices) {
		return nil, errors.New("no cleanup voice available")
	}
	voice := c.voices[c.next]
	c.next++
	return voice, nil
}

func (s *deliveryRecordingSink) OfferHandoffEvent(ctx context.Context, scope handoff.Scope, event handoff.Event) error {
	err := s.manager.OfferHandoffEvent(ctx, scope, event)
	s.results <- err
	return err
}

func startOwnedEventPump(t *testing.T, pump *handoff.EventPump) {
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
}

func startDeliveryStack(t *testing.T, store *deliveryStore, scope handoff.Scope, client *deliveryClient, timeout time.Duration, suffix string) deliveryStack {
	t.Helper()
	manual := clock.NewManual(time.Unix(0, 0))
	pump := &handoff.EventPump{Reader: store, Cursors: store, Clock: manual, PollInterval: time.Second}
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: timeout,
		HandoffConsumerID: "public-client", HandoffSubscriptions: pump,
		PresentationPolicy: presentation.NewPolicy(presentation.Config{}), SafetyPolicy: &presentationSafety{},
	}, testkit.NewFakeConnector(8), managerIDs{session: identity.SessionID("delivery-" + suffix), conversation: scope.ConversationID})
	pump.Sink = manager
	pumpCtx, pumpCancel := context.WithCancel(context.Background())
	pumpDone := make(chan error, 1)
	go func() { pumpDone <- pump.Run(pumpCtx) }()
	managerCtx, managerCancel := context.WithCancel(context.Background())
	managerDone := make(chan error, 1)
	go func() {
		managerDone <- manager.Run(managerCtx, OpenRequest{Subject: Subject{
			TenantID: scope.TenantID, UserID: scope.UserID, ExpiresAt: time.Now().Add(time.Hour),
		}, ConversationID: scope.ConversationID}, client)
	}()
	return deliveryStack{managerCancel: managerCancel, pumpCancel: pumpCancel, managerDone: managerDone, pumpDone: pumpDone, manual: manual}
}

func (s deliveryStack) stop(t *testing.T) {
	t.Helper()
	s.managerCancel()
	s.pumpCancel()
	select {
	case <-s.managerDone:
	case <-time.After(time.Second):
		t.Fatal("manager did not stop")
	}
	select {
	case <-s.pumpDone:
	case <-time.After(time.Second):
		t.Fatal("pump did not stop")
	}
}

func TestManagerPublishesRuntimeOnlyAfterSessionCreatedWrite(t *testing.T) {
	scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "conversation-ready"}
	store := &deliveryStore{event: handoff.Event{Seq: 1, EventID: "ready-event", HandoffID: "handoff", TenantID: scope.TenantID, ConversationID: scope.ConversationID, Kind: "handoff.created", State: handoff.StatePending, Payload: json.RawMessage(`{}`)}}
	client := newDeliveryClient()
	client.sessionRelease = make(chan struct{})
	stack := startDeliveryStack(t, store, scope, client, time.Second, "ready")
	defer stack.stop(t)
	select {
	case <-client.sessionStarted:
	case <-time.After(time.Second):
		t.Fatal("session.created write did not start")
	}
	store.mu.Lock()
	assert.Zero(t, store.loads, "event pump must not observe a pre-ready runtime")
	store.mu.Unlock()
	close(client.sessionRelease)
	select {
	case event := <-client.visualStarted:
		assert.Equal(t, "live.handoff.created", event.EventType())
	case <-time.After(time.Second):
		t.Fatal("handoff was not delivered after readiness")
	}
}

func TestEventPumpCursorWaitsForExactClientWriteAndReplaysAfterCursorCrash(t *testing.T) {
	scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "conversation-replay"}
	store := &deliveryStore{advanceErr: errors.New("cursor commit crashed"), event: handoff.Event{Seq: 1, EventID: "stable-event", HandoffID: "handoff", TenantID: scope.TenantID, ConversationID: scope.ConversationID, Kind: "handoff.created", State: handoff.StatePending, Payload: json.RawMessage(`{}`)}}
	firstClient := newDeliveryClient()
	first := startDeliveryStack(t, store, scope, firstClient, time.Second, "first")
	firstWritten := <-firstClient.visualStarted
	assert.Equal(t, "stable-event", firstWritten.(protocol.LiveHandoffCreatedEvent).EventID)
	require.Eventually(t, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		return store.advances == 1 && store.cursor == 0
	}, time.Second, time.Millisecond)
	first.stop(t)

	store.mu.Lock()
	store.advanceErr = nil
	store.mu.Unlock()
	secondClient := newDeliveryClient()
	second := startDeliveryStack(t, store, scope, secondClient, time.Second, "second")
	defer second.stop(t)
	replayed := <-secondClient.visualStarted
	assert.Equal(t, "stable-event", replayed.(protocol.LiveHandoffCreatedEvent).EventID)
	require.Eventually(t, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		return store.cursor == 1
	}, time.Second, time.Millisecond)
}

func TestEventPumpTimeoutAndWriteFailureKeepCursorReplayable(t *testing.T) {
	for _, test := range []struct {
		name    string
		failure error
		block   bool
	}{
		{name: "write failure", failure: io.ErrClosedPipe},
		{name: "write timeout", block: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: identity.ConversationID("conversation-" + test.name)}
			store := &deliveryStore{event: handoff.Event{Seq: 1, EventID: "event", HandoffID: "handoff", TenantID: scope.TenantID, ConversationID: scope.ConversationID, Kind: "handoff.created", State: handoff.StatePending, Payload: json.RawMessage(`{}`)}}
			client := newDeliveryClient()
			client.visualErr = test.failure
			if test.block {
				client.visualRelease = make(chan struct{})
			}
			stack := startDeliveryStack(t, store, scope, client, 20*time.Millisecond, test.name)
			<-client.visualStarted
			time.Sleep(30 * time.Millisecond)
			store.mu.Lock()
			assert.Zero(t, store.cursor)
			store.mu.Unlock()
			if test.block {
				close(client.visualRelease)
			}
			stack.stop(t)
		})
	}
}

func TestTimedOutPresentationKeepsRuntimeAndStableEventReplayable(t *testing.T) {
	scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "conversation-replace"}
	store := &deliveryStore{event: handoff.Event{
		Seq: 7, EventID: "stable-needs-input", HandoffID: "handoff", TenantID: scope.TenantID,
		ConversationID: scope.ConversationID, Kind: "handoff.needs-input", State: handoff.StateNeedsInput,
		Payload: json.RawMessage(`{"Prompt":"choose a region","Schema":{}}`),
	}}
	manual := clock.NewManual(time.Unix(0, 0))
	pump := &handoff.EventPump{Reader: store, Cursors: store, Clock: manual, PollInterval: time.Second}
	connector := testkit.NewFakeConnector(8)
	safetyPolicy := &presentationSafety{}
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: 20 * time.Millisecond,
		HandoffConsumerID: "public-client", HandoffSubscriptions: pump,
		PresentationPolicy: presentation.NewPolicy(presentation.Config{}), SafetyPolicy: safetyPolicy,
	}, connector, &deliverySequenceIDs{sessions: []identity.SessionID{"delivery-old"}})
	sink := &deliveryRecordingSink{manager: manager, results: make(chan error, 8)}
	pump.Sink = sink

	run := func(client *deliveryClient) (context.CancelFunc, <-chan error) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- manager.Run(ctx, OpenRequest{Subject: Subject{
				TenantID: scope.TenantID, UserID: scope.UserID, ExpiresAt: time.Now().Add(time.Hour),
			}, ConversationID: scope.ConversationID}, client)
		}()
		_, err := connector.NextSession(t.Context())
		require.NoError(t, err)
		return cancel, done
	}
	stop := func(cancel context.CancelFunc, done <-chan error) {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("manager did not stop")
		}
	}

	oldClient := newDeliveryClient()
	oldClient.visualRelease = make(chan struct{})
	oldCancel, oldDone := run(oldClient)
	defer stop(oldCancel, oldDone)
	require.Eventually(t, func() bool {
		manager.activeMu.RLock()
		defer manager.activeMu.RUnlock()
		return manager.current[scope] != nil
	}, time.Second, time.Millisecond)
	pumpCtx, pumpCancel := context.WithCancel(context.Background())
	pumpDone := make(chan error, 1)
	go func() { pumpDone <- pump.Run(pumpCtx) }()
	defer func() {
		pumpCancel()
		select {
		case <-pumpDone:
		case <-time.After(time.Second):
			t.Fatal("pump did not stop")
		}
	}()
	require.Eventually(t, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		return store.loads > 0
	}, time.Second, time.Millisecond, "event pump did not load the registered subscription")
	var oldVisual protocol.ServerEvent
	select {
	case oldVisual = <-oldClient.visualStarted:
	case err := <-sink.results:
		t.Fatalf("old presentation failed before its visual write: %v (safety evaluations: %d)", err, len(safetyPolicy.seen))
	case <-time.After(time.Second):
		t.Fatal("old presentation did not start")
	}
	select {
	case err := <-sink.results:
		require.ErrorIs(t, err, ErrControlOverflow)
	case <-time.After(time.Second):
		t.Fatal("bounded offer did not return")
	}
	assert.Equal(t, int64(1), manager.ActiveSessions(), "offer timeout must not quiesce the runtime")
	store.mu.Lock()
	assert.Zero(t, store.cursor)
	store.mu.Unlock()
	close(oldClient.visualRelease)
	manual.Advance(time.Second)
	var replayed protocol.ServerEvent
	select {
	case replayed = <-oldClient.visualStarted:
	case <-time.After(time.Second):
		t.Fatal("expired presentation was not replayed")
	}
	oldEvent := oldVisual.(protocol.LiveHandoffNeedsInputEvent)
	replayedEvent := replayed.(protocol.LiveHandoffNeedsInputEvent)
	assert.Equal(t, oldEvent.EventID, replayedEvent.EventID)
	assert.Equal(t, int64(7), oldEvent.SequenceID)
	assert.Equal(t, oldEvent.SequenceID, replayedEvent.SequenceID)
	require.Eventually(t, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		return store.cursor == 7 && store.advances == 1
	}, time.Second, time.Millisecond)
}

func TestOfferTimeoutDoesNotHoldDirectoryLockOrQuiesceProvider(t *testing.T) {
	scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "conversation-context"}
	policy := &blockingContextSafety{started: make(chan struct{}), cancelled: make(chan struct{})}
	oldVoice := &cleanupVoice{closeStarted: make(chan struct{}), closeRelease: make(chan struct{}), closeDone: make(chan struct{})}
	replacementRelease := make(chan struct{})
	close(replacementRelease)
	replacementVoice := &cleanupVoice{closeStarted: make(chan struct{}), closeRelease: replacementRelease, closeDone: make(chan struct{})}
	connector := &cleanupConnector{voices: []*cleanupVoice{oldVoice, replacementVoice}}
	subscriptions := &signalingSubscriptions{second: make(chan struct{})}
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: 20 * time.Millisecond,
		HandoffConsumerID: "public-client", HandoffSubscriptions: subscriptions,
		PresentationPolicy: presentation.NewPolicy(presentation.Config{}), SafetyPolicy: policy,
	}, connector, &deliverySequenceIDs{sessions: []identity.SessionID{"context-old", "context-replacement"}})

	run := func(client *deliveryClient) (context.CancelFunc, <-chan error) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- manager.Run(ctx, OpenRequest{Subject: Subject{
				TenantID: scope.TenantID, UserID: scope.UserID, ExpiresAt: time.Now().Add(time.Hour),
			}, ConversationID: scope.ConversationID}, client)
		}()
		return cancel, done
	}
	waitCurrent := func(sessionID identity.SessionID) bool {
		manager.activeMu.RLock()
		defer manager.activeMu.RUnlock()
		return manager.current[scope] != nil && manager.current[scope].key.sessionID == sessionID
	}

	oldCancel, oldDone := run(newDeliveryClient())
	defer oldCancel()
	require.Eventually(t, func() bool { return waitCurrent("context-old") }, time.Second, time.Millisecond)
	offered := make(chan error, 1)
	go func() {
		offered <- manager.OfferHandoffEvent(context.Background(), scope, handoff.Event{
			Seq: 1, EventID: "context-event", HandoffID: "handoff", TenantID: scope.TenantID,
			ConversationID: scope.ConversationID, Kind: "handoff.needs-input", State: handoff.StateNeedsInput,
			Payload: json.RawMessage(`{"Prompt":"choose","Schema":{}}`),
		})
	}()
	select {
	case <-policy.started:
	case <-time.After(time.Second):
		t.Fatal("presentation safety did not start")
	}
	select {
	case err := <-offered:
		require.ErrorIs(t, err, ErrControlOverflow)
	case <-time.After(time.Second):
		t.Fatal("bounded caller wait did not return")
	}
	select {
	case <-oldVoice.closeStarted:
		t.Fatal("offer timeout quiesced the provider")
	default:
	}

	replacementCancel, replacementDone := run(newDeliveryClient())
	defer func() {
		replacementCancel()
		select {
		case <-replacementDone:
		case <-time.After(time.Second):
			t.Fatal("replacement manager did not stop")
		}
	}()
	select {
	case <-subscriptions.second:
	case <-time.After(time.Second):
		t.Fatal("replacement publication was blocked by the timed-out offer")
	}
	require.Eventually(t, func() bool { return waitCurrent("context-replacement") }, time.Second, time.Millisecond,
		"replacement registration callback returned before directory publication completed")
	oldCancel()
	select {
	case <-policy.cancelled:
	case <-time.After(time.Second):
		t.Fatal("old safety work did not stop with its session")
	}
	close(oldVoice.closeRelease)
	select {
	case <-oldDone:
	case <-time.After(time.Second):
		t.Fatal("old manager did not stop")
	}
}

func TestReactorLoopAppliesAcceptedOfferWithSessionContextAfterCallerExpiry(t *testing.T) {
	policy := &countingSafety{}
	state := newRunState(Config{
		IngressFrames: 1, EgressFrames: 1, ControlEvents: 1, WriterControlEvents: 1,
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
		PresentationPolicy: presentation.NewPolicy(presentation.Config{}), SafetyPolicy: policy,
	}, IDs{SessionID: "expired", ConversationID: "conversation", Incarnation: 1}, nil, nil, func() {})
	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	loopDone := make(chan error, 1)
	go func() { loopDone <- state.reactorLoop(runCtx) }()
	offerCtx, cancelOffer := context.WithCancel(context.Background())
	cancelOffer()
	messageDone := make(chan error, 1)
	<-state.inboxSlots
	state.inbox <- reactorMessage{offerCtx: offerCtx, done: messageDone, input: HandoffEventInput{
		Scope: handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "conversation"},
		Event: handoff.Event{
			Seq: 1, EventID: "expired-event", HandoffID: "handoff", TenantID: "tenant", ConversationID: "conversation",
			Kind: "handoff.needs-input", State: handoff.StateNeedsInput, Payload: json.RawMessage(`{"Prompt":"choose","Schema":{}}`),
		},
	}}
	var control clientControl
	select {
	case control = <-state.clientCtl:
	case <-time.After(time.Second):
		t.Fatal("accepted offer did not reach the client queue")
	}
	require.NotZero(t, control.completionSequence)
	require.NoError(t, state.enqueueCompletion(runCtx, ClientEffectCompletedInput{Sequence: control.completionSequence}))
	require.NoError(t, <-messageDone)
	stop()
	require.ErrorIs(t, <-loopDone, context.Canceled)
	policy.mu.Lock()
	assert.Positive(t, policy.calls)
	policy.mu.Unlock()
}

func TestReplacementFencesTimedOutAcceptedOfferBeforeOldEffects(t *testing.T) {
	scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "replacement-fence"}
	applyStarted := make(chan struct{})
	applyRelease := make(chan struct{})
	var startedOnce sync.Once
	cfg := Config{
		IngressFrames: 1, EgressFrames: 1, ControlEvents: 2, WriterControlEvents: 2,
		IngressBlockTimeout: time.Second, EgressBlockTimeout: 20 * time.Millisecond,
		reactorApplyGate: func(input Input) <-chan struct{} {
			if _, ok := input.(HandoffEventInput); ok {
				startedOnce.Do(func() { close(applyStarted) })
				return applyRelease
			}
			return nil
		},
	}
	manager := NewManager(cfg, testkit.NewFakeConnector(1), managerIDs{})
	require.NoError(t, manager.validationErr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	state := newRunState(manager.cfg, IDs{SessionID: "old", ConversationID: scope.ConversationID, Incarnation: 1},
		&recordingVoice{sent: make(chan realtime.Command, 2)}, newChannelClient(2), cancel)
	state.subject = Subject{TenantID: scope.TenantID, UserID: scope.UserID}
	runtime := &sessionRuntime{
		key: runtimeKey{sessionID: "old", incarnation: 1}, subject: state.subject,
		inbox: state.inbox, done: ctx.Done(), state: state, accepting: true, inboxSlots: state.inboxSlots,
	}
	state.runtime = runtime
	require.NoError(t, manager.register(runtime))
	loopDone := make(chan error, 1)
	go func() { loopDone <- state.reactorLoop(ctx) }()

	offered := make(chan error, 1)
	go func() {
		offered <- manager.OfferHandoffEvent(context.Background(), scope, handoff.Event{
			Seq: 1, EventID: "stale-offer", HandoffID: "handoff", TenantID: scope.TenantID,
			ConversationID: scope.ConversationID, Kind: "tool.approval_required", State: handoff.StateNeedsInput,
			Payload: json.RawMessage(`{"invocation_id":"invoke","handoff_id":"handoff","action_summary":"approve","expires_at":"2099-01-01T00:00:00Z","risk_class":"low"}`),
		})
	}()
	select {
	case <-applyStarted:
	case <-time.After(time.Second):
		t.Fatal("old offer did not reach gated apply")
	}
	require.ErrorIs(t, <-offered, ErrControlOverflow)

	replacement, _, replacementDone := currentRuntime(scope, "replacement")
	defer close(replacementDone)
	replacement.publicationRequiresCommit = true
	require.NoError(t, manager.register(replacement))
	close(applyRelease)
	time.Sleep(20 * time.Millisecond)
	select {
	case control := <-state.clientCtl:
		t.Fatalf("stale accepted offer emitted client effect after replacement: %T", control.event)
	default:
	}
	select {
	case control := <-state.voiceCtl:
		t.Fatalf("stale accepted offer emitted provider effect after replacement: %v", control.command.Kind)
	default:
	}
	cancel()
	require.ErrorIs(t, <-loopDone, context.Canceled)
}

func TestOfferDeadlineBoundsReconnectRotationAdmission(t *testing.T) {
	credentials, err := NewReconnectCredentials([]byte(strings.Repeat("q", 32)), clock.Real{})
	require.NoError(t, err)
	scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "rotation-deadline"}
	manager := NewManager(Config{
		IngressFrames: 1, EgressFrames: 1, ControlEvents: 2, WriterControlEvents: 2,
		IngressBlockTimeout: time.Second, EgressBlockTimeout: 20 * time.Millisecond,
		HandoffConsumerID: "public-client", HandoffSubscriptions: &signalingSubscriptions{},
		PresentationPolicy: presentation.NewPolicy(presentation.Config{}), SafetyPolicy: &presentationSafety{},
		ReconnectCredentials: credentials, ReconnectOwners: NewMemoryReconnectOwners(clock.Real{}), ReconnectOwnerID: "rotation-test",
	}, testkit.NewFakeConnector(1), managerIDs{})
	require.NoError(t, manager.validationErr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	state := newRunState(manager.cfg, IDs{SessionID: "rotation", ConversationID: scope.ConversationID, Incarnation: 1},
		&recordingVoice{sent: make(chan realtime.Command, 2)}, newChannelClient(2), cancel)
	state.subject = Subject{TenantID: scope.TenantID, UserID: scope.UserID}
	runtime := &sessionRuntime{
		key: runtimeKey{sessionID: "rotation", incarnation: 1}, subject: state.subject,
		inbox: state.inbox, done: ctx.Done(), state: state, accepting: true, inboxSlots: state.inboxSlots,
	}
	state.runtime = runtime
	require.NoError(t, manager.register(runtime))
	runtime.reconnectRotateOnce.Do(func() {
		runtime.reconnectRotate = make(chan struct{}, 1)
		runtime.reconnectRotate <- struct{}{}
	})
	<-runtime.reconnectRotate
	defer func() { runtime.reconnectRotate <- struct{}{} }()
	loopDone := make(chan error, 1)
	go func() { loopDone <- state.reactorLoop(ctx) }()

	offered := make(chan error, 1)
	go func() {
		offered <- manager.OfferHandoffEvent(context.Background(), scope, handoff.Event{
			Seq: 1, EventID: "rotation-event", HandoffID: "handoff", TenantID: scope.TenantID,
			ConversationID: scope.ConversationID, Kind: "tool.approval_required", State: handoff.StateNeedsInput,
			Payload: json.RawMessage(`{"invocation_id":"invoke","handoff_id":"handoff","action_summary":"approve","expires_at":"2099-01-01T00:00:00Z","risk_class":"low"}`),
		})
	}()
	select {
	case control := <-state.clientCtl:
		require.NotZero(t, control.completionSequence)
		require.NoError(t, state.enqueueCompletion(ctx, ClientEffectCompletedInput{Sequence: control.completionSequence}))
	case <-time.After(time.Second):
		t.Fatal("visual write was not admitted")
	}
	select {
	case err := <-offered:
		require.ErrorIs(t, err, ErrControlOverflow)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("offer deadline did not bound reconnect rotation admission")
	}
	cancel()
	require.ErrorIs(t, <-loopDone, context.Canceled)
}

func TestExpiredOfferRotationWriteKeepsSessionAndCredentialRetryable(t *testing.T) {
	manual := clock.NewManual(time.Now().UTC())
	credentials, err := NewReconnectCredentials([]byte(strings.Repeat("w", 32)), manual)
	require.NoError(t, err)
	owners := NewMemoryReconnectOwners(manual)
	connector := testkit.NewFakeConnector(4)
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: 20 * time.Millisecond,
		HandoffConsumerID: "public-client", HandoffSubscriptions: &signalingSubscriptions{},
		PresentationPolicy: presentation.NewPolicy(presentation.Config{}), SafetyPolicy: &presentationSafety{},
		ReconnectCredentials: credentials, ReconnectOwners: owners, ReconnectOwnerID: "rotation-write",
		ReconnectCredentialTTL: time.Minute, Clock: manual,
	}, connector, managerIDs{session: "rotation-write-session", conversation: "rotation-write-conversation"})
	require.NoError(t, manager.validationErr)
	release := make(chan struct{})
	client := newDeliveryClient()
	client.reconnectStarted = make(chan protocol.SessionReconnectUpdatedEvent, 2)
	client.reconnectRelease = release
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- manager.Run(ctx, OpenRequest{Subject: Subject{
			TenantID: "tenant", UserID: "user", ExpiresAt: manual.Now().Add(time.Hour),
		}, RequestedModel: "provider-model"}, client)
	}()
	_, err = connector.NextSession(t.Context())
	require.NoError(t, err)
	<-client.sessionStarted
	var runtime *sessionRuntime
	scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "rotation-write-conversation"}
	require.Eventually(t, func() bool {
		manager.activeMu.RLock()
		candidate := manager.active[runtimeKey{sessionID: "rotation-write-session", incarnation: 1}]
		current := manager.current[scope]
		pendingAttempt := manager.pendingPublications[scope]
		if candidate == nil || current != candidate || pendingAttempt != nil {
			manager.activeMu.RUnlock()
			return false
		}
		candidate.mu.Lock()
		pending := candidate.publicationPending
		candidate.mu.Unlock()
		manager.activeMu.RUnlock()
		runtime = candidate
		return !pending
	}, time.Second, time.Millisecond, "runtime must be the durably committed current presentation owner")
	event := handoff.Event{
		Seq: 7, EventID: "rotation-write-event", HandoffID: "handoff", TenantID: "tenant",
		ConversationID: runtime.state.ids.ConversationID, Kind: "tool.approval_required", State: handoff.StateNeedsInput,
		Payload: json.RawMessage(`{"invocation_id":"invoke","handoff_id":"handoff","action_summary":"approve","expires_at":"2099-01-01T00:00:00Z","risk_class":"low"}`),
	}
	first := make(chan error, 1)
	go func() { first <- manager.OfferHandoffEvent(context.Background(), runtime.handoffScope(), event) }()
	var attempted protocol.SessionReconnectUpdatedEvent
	select {
	case attempted = <-client.reconnectStarted:
	case <-time.After(time.Second):
		t.Fatal("post-rotate credential delivery did not start")
	}
	require.ErrorIs(t, <-first, ErrControlOverflow)
	select {
	case runErr := <-done:
		t.Fatalf("offer timeout terminated the live session: %v", runErr)
	case <-time.After(50 * time.Millisecond):
	}
	require.Equal(t, int64(1), manager.ActiveSessions())
	close(release)
	require.Eventually(t, func() bool {
		runtime.reconnectMu.Lock()
		defer runtime.reconnectMu.Unlock()
		return runtime.reconnectDelivered
	}, time.Second, time.Millisecond)
	require.NoError(t, manager.OfferHandoffEvent(t.Context(), runtime.handoffScope(), event))
	runtime.reconnectMu.Lock()
	require.Equal(t, attempted.Reconnect.Credential, runtime.reconnect.Credential)
	runtime.reconnectMu.Unlock()
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestCommittedRotationRetriesSameCredentialAfterClientQueueOverflow(t *testing.T) {
	manual := clock.NewManual(time.Now().UTC())
	credentials, err := NewReconnectCredentials([]byte(strings.Repeat("y", 32)), manual)
	require.NoError(t, err)
	owners := &reconnectOwnersProbe{
		MemoryReconnectOwners: NewMemoryReconnectOwners(manual), rotateStarted: make(chan struct{}), rotateRelease: make(chan struct{}),
	}
	manager := NewManager(Config{
		IngressFrames: 1, EgressFrames: 1, ControlEvents: 2, WriterControlEvents: 1,
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
		ReconnectCredentials: credentials, ReconnectOwners: owners, ReconnectOwnerID: "rotation-overflow",
		ReconnectCredentialTTL: time.Minute, Clock: manual,
	}, testkit.NewFakeConnector(1), managerIDs{})
	require.NoError(t, manager.validationErr)
	scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "rotation-overflow"}
	subject := Subject{TenantID: scope.TenantID, UserID: scope.UserID, ExpiresAt: manual.Now().Add(time.Hour)}
	issued, owner, err := manager.issueReconnectCredential(subject, scope, "rotation-overflow", 1, 0, "provider-model", ReconnectOwnerEstablishing)
	require.NoError(t, err)
	_, err = owners.Create(t.Context(), owner, manager.cfg.ReconnectOwnerLeaseDuration)
	require.NoError(t, err)
	_, err = owners.Publish(t.Context(), owner.Key(), 0, true, manager.cfg.ReconnectOwnerLeaseDuration)
	require.NoError(t, err)

	release := make(chan struct{})
	client := newDeliveryClient()
	client.reconnectStarted = make(chan protocol.SessionReconnectUpdatedEvent, 2)
	client.reconnectRelease = release
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	state := newRunState(manager.cfg, IDs{SessionID: "rotation-overflow", ConversationID: scope.ConversationID, Incarnation: 1},
		&recordingVoice{sent: make(chan realtime.Command, 1)}, client, cancel)
	state.subject = subject
	runtime := &sessionRuntime{
		key: runtimeKey{sessionID: "rotation-overflow", incarnation: 1}, subject: subject,
		state: state, inbox: state.inbox, inboxSlots: state.inboxSlots, done: ctx.Done(), accepting: true,
		reconnect: &issued, reconnectDelivered: true, reconnectLossDeadline: manual.Now().Add(time.Minute),
	}
	state.runtime = runtime
	reactorDone := make(chan error, 1)
	writerDone := make(chan error, 1)
	go func() { reactorDone <- state.reactorLoop(ctx) }()
	go func() { writerDone <- state.clientWriter(ctx) }()

	first := make(chan error, 1)
	go func() { first <- manager.rotateReconnectCredential(context.Background(), runtime, 7, false) }()
	select {
	case <-owners.rotateStarted:
	case <-time.After(time.Second):
		t.Fatal("durable rotation did not start")
	}
	fake := protocol.SessionReconnectUpdatedEvent{Type: "session.reconnect.updated", EventID: "fake",
		Reconnect: protocol.ReconnectMetadata{Credential: "fake"}}
	require.NoError(t, state.enqueueClientControl(clientControl{event: fake}))
	select {
	case attempted := <-client.reconnectStarted:
		require.Equal(t, "fake", attempted.Reconnect.Credential)
	case <-time.After(time.Second):
		t.Fatal("client queue blocker did not reach writer")
	}
	close(owners.rotateRelease)
	require.ErrorIs(t, <-first, ErrControlOverflow)
	runtime.reconnectMu.Lock()
	committed := *runtime.reconnect
	require.Equal(t, int64(7), committed.Claims.Cursor)
	require.False(t, runtime.reconnectDelivered)
	runtime.reconnectMu.Unlock()
	close(release)
	require.Eventually(t, func() bool { return len(state.clientSlots) == 1 }, time.Second, time.Millisecond)

	require.NoError(t, manager.rotateReconnectCredential(t.Context(), runtime, 7, false))
	select {
	case retried := <-client.reconnectStarted:
		require.Equal(t, committed.Credential, retried.Reconnect.Credential)
	case <-time.After(time.Second):
		t.Fatal("committed credential was not re-delivered on same-cursor retry")
	}
	cancel()
	require.ErrorIs(t, <-reactorDone, context.Canceled)
	require.ErrorIs(t, <-writerDone, context.Canceled)
}

func TestFailedInitialSameScopePublicationRestoresPriorRuntime(t *testing.T) {
	scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "rollback-initial"}
	manager := NewManager(Config{IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second}, testkit.NewFakeConnector(1), managerIDs{})
	require.NoError(t, manager.validationErr)
	previous, _, previousDone := currentRuntime(scope, "previous")
	defer close(previousDone)
	require.NoError(t, manager.register(previous))
	previous.mu.Lock()
	originalGeneration := previous.acceptanceGeneration
	previous.mu.Unlock()

	replacement, _, replacementDone := currentRuntime(scope, "replacement")
	defer close(replacementDone)
	replacement.publicationRequiresCommit = true
	require.NoError(t, manager.register(replacement))
	manager.activeMu.RLock()
	logical := manager.logical[replacement.key.sessionID]
	manager.activeMu.RUnlock()
	require.NotNil(t, logical)
	manager.rollbackInitialPublication(replacement, logical)

	manager.activeMu.RLock()
	require.Same(t, previous, manager.current[scope])
	manager.activeMu.RUnlock()
	previous.mu.Lock()
	require.True(t, previous.accepting)
	require.Equal(t, originalGeneration, previous.acceptanceGeneration)
	previous.mu.Unlock()
}

func TestStagedReplacementOfferIsReplayableAfterPublicationRollback(t *testing.T) {
	scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "staged-publication-rollback"}
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
	}, testkit.NewFakeConnector(1), managerIDs{})
	require.NoError(t, manager.validationErr)
	previous, previousInputs, previousDone := currentRuntime(scope, "staged-publication-previous")
	t.Cleanup(func() { close(previousDone) })
	require.NoError(t, manager.register(previous))
	manager.commitPublication(previous)

	replacement, replacementInputs, replacementDone := currentRuntime(scope, "staged-publication-replacement")
	t.Cleanup(func() { close(replacementDone) })
	replacement.publicationRequiresCommit = true
	require.NoError(t, manager.register(replacement))
	event := handoff.Event{
		Seq: 1, EventID: "staged-publication-event", HandoffID: "handoff", TenantID: scope.TenantID,
		ConversationID: scope.ConversationID, Kind: "handoff.created", State: handoff.StatePending,
		Payload: json.RawMessage(`{}`),
	}
	cursor := int64(0)
	offerErr := manager.OfferHandoffEvent(t.Context(), scope, event)
	if offerErr == nil {
		cursor = event.Seq
	}
	assert.ErrorIs(t, offerErr, handoff.ErrNoLiveSession, "an offer cannot become durable while its runtime publication is still staged")
	manager.activeMu.RLock()
	logical := manager.logical[replacement.key.sessionID]
	manager.activeMu.RUnlock()
	require.NotNil(t, logical)
	manager.rollbackInitialPublication(replacement, logical)
	assert.Zero(t, cursor, "a rolled-back staged runtime must not advance the delivery cursor")
	select {
	case input := <-replacementInputs:
		t.Errorf("rolled-back staged runtime applied offer sequence %d", input.(HandoffEventInput).Event.Seq)
	default:
	}

	require.NoError(t, manager.OfferHandoffEvent(t.Context(), scope, event), "the same offer must replay to the restored owner")
	replayed := (<-previousInputs).(HandoffEventInput)
	require.Equal(t, event.Seq, replayed.Event.Seq)
}

func TestBlockingRegisterCallbackDoesNotStallCurrentPresentation(t *testing.T) {
	scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "blocking-register-callback"}
	registrar := &blockingSecondRegisterSubscriptions{started: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(registrar.release) }) }
	t.Cleanup(release)
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
		HandoffConsumerID: "callback-client", HandoffSubscriptions: registrar,
		PresentationPolicy: presentation.NewPolicy(presentation.Config{}), SafetyPolicy: &presentationSafety{},
	}, testkit.NewFakeConnector(1), managerIDs{})
	require.NoError(t, manager.validationErr)
	previous, previousInputs, previousDone := currentRuntime(scope, "blocking-register-previous")
	replacement, _, replacementDone := currentRuntime(scope, "blocking-register-replacement")
	t.Cleanup(func() { close(previousDone); close(replacementDone) })
	require.NoError(t, manager.register(previous))
	published := make(chan error, 1)
	go func() { published <- manager.register(replacement) }()
	select {
	case <-registrar.started:
	case <-time.After(time.Second):
		t.Fatal("replacement did not reach blocking Register callback")
	}

	offered := make(chan error, 1)
	event := handoff.Event{Seq: 1, EventID: "blocking-register-event", HandoffID: "handoff", TenantID: scope.TenantID,
		ConversationID: scope.ConversationID, Kind: "handoff.created", State: handoff.StatePending, Payload: json.RawMessage(`{}`)}
	go func() { offered <- manager.OfferHandoffEvent(context.Background(), scope, event) }()
	select {
	case offerErr := <-offered:
		require.NoError(t, offerErr)
		require.Equal(t, event.Seq, (<-previousInputs).(HandoffEventInput).Event.Seq)
	case <-time.After(time.Second):
		release()
		t.Fatal("blocking Register callback stalled the current presentation owner")
	}
	release()
	require.NoError(t, <-published)
}

func TestSynchronousReentrantRegisterCallbackDoesNotDeadlock(t *testing.T) {
	scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "reentrant-register-callback"}
	event := handoff.Event{Seq: 1, EventID: "reentrant-register-event", HandoffID: "handoff", TenantID: scope.TenantID,
		ConversationID: scope.ConversationID, Kind: "handoff.created", State: handoff.StatePending, Payload: json.RawMessage(`{}`)}
	registrar := &reentrantRegisterSubscriptions{scope: scope, event: event}
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
		HandoffConsumerID: "callback-client", HandoffSubscriptions: registrar,
		PresentationPolicy: presentation.NewPolicy(presentation.Config{}), SafetyPolicy: &presentationSafety{},
	}, testkit.NewFakeConnector(1), managerIDs{})
	registrar.manager = manager
	require.NoError(t, manager.validationErr)
	previous, previousInputs, previousDone := currentRuntime(scope, "reentrant-register-previous")
	replacement, _, replacementDone := currentRuntime(scope, "reentrant-register-replacement")
	t.Cleanup(func() { close(previousDone); close(replacementDone) })
	require.NoError(t, manager.register(previous))
	require.NoError(t, manager.register(replacement), "Register callback must be able to synchronously reenter the same presentation scope")
	require.Equal(t, event.Seq, (<-previousInputs).(HandoffEventInput).Event.Seq)
}

func TestRollbackUnregisterCallbacksRunWithoutPresentationLock(t *testing.T) {
	t.Run("blocking", func(t *testing.T) {
		scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "blocking-rollback-unregister"}
		registrar := &blockingUnregisterSubscriptions{started: make(chan struct{}), release: make(chan struct{})}
		manager := &Manager{
			cfg:    Config{HandoffConsumerID: "callback-client", HandoffSubscriptions: registrar},
			active: make(map[runtimeKey]*sessionRuntime), current: make(map[handoff.Scope]*sessionRuntime),
			logical: make(map[identity.SessionID]*logicalSession),
		}
		runtime, _, runtimeDone := currentRuntime(scope, "blocking-rollback-runtime")
		t.Cleanup(func() { close(runtimeDone) })
		require.NoError(t, manager.register(runtime))
		logical := manager.logical[runtime.key.sessionID]
		rolledBack := make(chan struct{})
		go func() {
			manager.rollbackInitialPublication(runtime, logical)
			close(rolledBack)
		}()
		select {
		case <-registrar.started:
		case <-time.After(time.Second):
			t.Fatal("rollback did not reach blocking Unregister callback")
		}
		presentationAvailable := runtime.presentationMu.TryLock()
		if presentationAvailable {
			runtime.presentationMu.Unlock()
		}
		close(registrar.release)
		select {
		case <-rolledBack:
		case <-time.After(time.Second):
			t.Fatal("rollback did not finish after Unregister callback released")
		}
		require.True(t, presentationAvailable, "blocking Unregister callback retained the rolled-back runtime presentation lock")
	})

	t.Run("synchronous reentrant same-scope unregister", func(t *testing.T) {
		scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "reentrant-rollback-unregister"}
		registrar := &reentrantUnregisterSubscriptions{}
		manager := &Manager{
			cfg:    Config{HandoffConsumerID: "callback-client", HandoffSubscriptions: registrar},
			active: make(map[runtimeKey]*sessionRuntime), current: make(map[handoff.Scope]*sessionRuntime),
			logical: make(map[identity.SessionID]*logicalSession),
		}
		runtime, _, runtimeDone := currentRuntime(scope, "reentrant-rollback-runtime")
		t.Cleanup(func() { close(runtimeDone) })
		registrar.manager, registrar.runtime = manager, runtime
		require.NoError(t, manager.register(runtime))
		manager.rollbackInitialPublication(runtime, manager.logical[runtime.key.sessionID])
		require.False(t, registrar.timedOut.Load(), "rollback Unregister callback deadlocked while synchronously reentering the same scope")
	})
}

func TestReconnectReplayPublicationCannotLeakOverlayToConcurrentInitialOwner(t *testing.T) {
	scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "replay-publication-race"}
	registrar := &racingReplaySubscriptions{replayStarted: make(chan struct{}), replayRelease: make(chan struct{})}
	manager := &Manager{
		cfg:    Config{HandoffConsumerID: "replay-client", HandoffSubscriptions: registrar},
		active: make(map[runtimeKey]*sessionRuntime), current: make(map[handoff.Scope]*sessionRuntime),
		logical: make(map[identity.SessionID]*logicalSession),
	}
	replayAfter := int64(5)
	replacement, _, replacementDone := currentRuntime(scope, "replay-race-reconnect")
	t.Cleanup(func() { close(replacementDone) })
	replacement.key.incarnation = 2
	replacement.state.ids.Incarnation = 2
	replacement.replayAfter = &replayAfter
	logical := &logicalSession{scope: scope, highWater: 1, reconnecting: true, turnBoundary: true, state: logicalTombstoned}
	manager.logical[replacement.key.sessionID] = logical
	reconnectPublished := make(chan error, 1)
	go func() { reconnectPublished <- manager.registerReplacement(replacement, logical, 1) }()
	select {
	case <-registrar.replayStarted:
	case <-time.After(time.Second):
		t.Fatal("reconnect did not install its attempt-owned replay overlay")
	}

	initial, _, initialDone := currentRuntime(scope, "replay-race-initial")
	t.Cleanup(func() { close(initialDone) })
	initialPublished := make(chan error, 1)
	go func() { initialPublished <- manager.register(initial) }()
	var initialErr error
	select {
	case initialErr = <-initialPublished:
	case <-time.After(time.Second):
		close(registrar.replayRelease)
		t.Fatal("concurrent initial publication did not resolve against the pending replay attempt")
	}
	close(registrar.replayRelease)
	reconnectErr := <-reconnectPublished

	manager.activeMu.RLock()
	current := manager.current[scope]
	manager.activeMu.RUnlock()
	registrar.mu.Lock()
	registered, replayOverlay, retainedAfter := registrar.registered, registrar.replayOverlay, registrar.replayAfter
	registrar.mu.Unlock()
	require.True(t, registered)
	if initialErr == nil {
		require.ErrorIs(t, reconnectErr, ErrStaleReconnect)
		require.Same(t, initial, current)
		require.False(t, replayOverlay, "a losing reconnect attempt leaked its lower replay cursor onto the initial owner")
		return
	}
	require.ErrorIs(t, initialErr, ErrStaleReconnect)
	require.NoError(t, reconnectErr)
	require.Same(t, replacement, current)
	require.True(t, replayOverlay)
	require.Equal(t, replayAfter, retainedAfter)
}

func TestPendingReplayPublicationCannotDeliverThroughActivePreviousOwner(t *testing.T) {
	scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "pending-replay-active-previous"}
	event := handoff.Event{
		Seq: 1, EventID: "pending-replay-event", HandoffID: "handoff", TenantID: scope.TenantID,
		ConversationID: scope.ConversationID, Kind: "handoff.created", State: handoff.StatePending,
		Payload: json.RawMessage(`{}`),
	}
	store := &deliveryStore{event: event, cursor: 1}
	manual := clock.NewManual(time.Unix(0, 0))
	pump := &handoff.EventPump{Reader: store, Cursors: store, Clock: manual, PollInterval: time.Second}
	replayInstalled := make(chan struct{})
	replayRelease := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(replayRelease) }) }
	t.Cleanup(release)
	registrar := &replayRegistrarProbe{pump: pump, replayInstalled: replayInstalled, replayRelease: replayRelease}
	manager := &Manager{
		cfg:    Config{HandoffConsumerID: "replay-client", HandoffSubscriptions: registrar, EgressBlockTimeout: time.Second},
		active: make(map[runtimeKey]*sessionRuntime), current: make(map[handoff.Scope]*sessionRuntime),
		logical: make(map[identity.SessionID]*logicalSession), pendingPublications: make(map[handoff.Scope]*publicationAttempt),
	}
	offerResults := make(chan error, 4)
	pump.Sink = &deliveryRecordingSink{manager: manager, results: offerResults}
	previous, previousInputs, previousDone := currentRuntime(scope, "pending-replay-previous")
	replacement, replacementInputs, replacementDone := currentRuntime(scope, "pending-replay-replacement")
	t.Cleanup(func() { close(previousDone); close(replacementDone) })
	require.NoError(t, manager.register(previous))
	startOwnedEventPump(t, pump)
	require.Eventually(t, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		return store.loads > 0
	}, time.Second, time.Millisecond, "event pump did not complete its initial stored-cursor poll")

	replacement.key.incarnation = 2
	replacement.state.ids.Incarnation = 2
	replayAfter := int64(0)
	replacement.replayAfter = &replayAfter
	logical := &logicalSession{scope: scope, highWater: 1, reconnecting: true, turnBoundary: true, state: logicalTombstoned}
	manager.activeMu.Lock()
	manager.logical[replacement.key.sessionID] = logical
	manager.activeMu.Unlock()
	published := make(chan error, 1)
	go func() { published <- manager.registerReplacement(replacement, logical, 1) }()
	select {
	case <-replayInstalled:
	case <-time.After(time.Second):
		t.Fatal("replacement did not install its replay overlay")
	}
	pendingOfferErr := manager.OfferHandoffEvent(t.Context(), scope, event)
	assert.ErrorIs(t, pendingOfferErr, handoff.ErrNoLiveSession,
		"a replay publication attempt must not deliver through the still-current previous owner even on a direct wakeup")
	select {
	case input := <-previousInputs:
		t.Errorf("pending replay sequence %d reached the previous owner", input.(HandoffEventInput).Event.Seq)
	default:
	}

	release()
	require.NoError(t, <-published)
	manual.Advance(time.Second)
	select {
	case input := <-replacementInputs:
		require.Equal(t, event.Seq, input.(HandoffEventInput).Event.Seq, "the unchanged replay must reach the committed replacement")
	case <-time.After(time.Second):
		t.Fatal("committed replacement did not receive the replay rejected during publication")
	}
}

func TestCommittedReplayWakesPromptlyAfterManagerPublicationFenceClears(t *testing.T) {
	scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "replay-post-publication-wake"}
	event := handoff.Event{
		Seq: 1, EventID: "replay-post-publication-event", HandoffID: "handoff", TenantID: scope.TenantID,
		ConversationID: scope.ConversationID, Kind: "handoff.completed", State: handoff.StateSucceeded,
		Payload: json.RawMessage(`{}`),
	}
	store := &deliveryStore{event: event, cursor: 1}
	pump := &handoff.EventPump{Reader: store, Cursors: store, Clock: clock.NewManual(time.Unix(0, 0)), PollInterval: time.Hour}
	offerResults := make(chan error, 4)
	commitStarted := make(chan struct{})
	commitRelease := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(commitRelease) }) }
	t.Cleanup(release)
	waitCtx, cancelWait := context.WithTimeout(t.Context(), 2*time.Second)
	t.Cleanup(cancelWait)
	registrar := &gatedCommitReplayRegistrar{
		pump: pump, commitStarted: commitStarted, commitRelease: commitRelease,
		wakeResults: offerResults, waitCtx: waitCtx,
	}
	manager := &Manager{
		cfg:    Config{HandoffConsumerID: "replay-client", HandoffSubscriptions: registrar, EgressBlockTimeout: time.Second},
		active: make(map[runtimeKey]*sessionRuntime), current: make(map[handoff.Scope]*sessionRuntime),
		logical: make(map[identity.SessionID]*logicalSession), pendingPublications: make(map[handoff.Scope]*publicationAttempt),
	}
	pump.Sink = &deliveryRecordingSink{manager: manager, results: offerResults}
	previous, _, previousDone := currentRuntime(scope, "replay-wake-previous")
	replacement, replacementInputs, replacementDone := currentRuntime(scope, "replay-wake-replacement")
	t.Cleanup(func() { close(previousDone); close(replacementDone) })
	require.NoError(t, manager.register(previous))
	startOwnedEventPump(t, pump)
	require.Eventually(t, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		return store.loads > 0
	}, time.Second, time.Millisecond, "event pump did not complete its initial stored-cursor poll")

	replacement.key.incarnation = 2
	replacement.state.ids.Incarnation = 2
	replayAfter := int64(0)
	replacement.replayAfter = &replayAfter
	logical := &logicalSession{scope: scope, highWater: 1, reconnecting: true, turnBoundary: true, state: logicalTombstoned}
	manager.activeMu.Lock()
	manager.logical[replacement.key.sessionID] = logical
	manager.activeMu.Unlock()
	published := make(chan error, 1)
	go func() { published <- manager.registerReplacement(replacement, logical, 1) }()
	select {
	case <-commitStarted:
	case <-time.After(time.Second):
		t.Fatal("replay publication did not reach its activation boundary")
	}
	release()
	select {
	case err := <-published:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("replay publication did not finish after activation was released")
	}

	select {
	case input := <-replacementInputs:
		require.Equal(t, event.Seq, input.(HandoffEventInput).Event.Seq)
	case <-time.After(time.Second):
		t.Fatal("committed replay waited for periodic polling after the Manager publication fence cleared")
	}
	require.NoError(t, waitCtx.Err(), "gated replay helper reached its explicit deadline")
}

func TestCanceledOlderGenerationOfferCannotEnterReplacementBeforeReplay(t *testing.T) {
	scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "canceled-generation-offer"}
	event := func(seq int64, eventID string) handoff.Event {
		return handoff.Event{
			Seq: seq, EventID: eventID, HandoffID: "handoff", TenantID: scope.TenantID,
			ConversationID: scope.ConversationID, Kind: "handoff.completed", State: handoff.StateSucceeded,
			Payload: json.RawMessage(`{}`),
		}
	}
	store := &sequenceDeliveryStore{
		cursor: 5,
		events: []handoff.Event{
			event(1, "event-1"), event(2, "event-2"), event(3, "event-3"),
			event(4, "event-4"), event(5, "event-5"), event(6, "event-6"),
		},
	}
	pump := &handoff.EventPump{Reader: store, Cursors: store, Clock: clock.NewManual(time.Unix(0, 0)), PollInterval: time.Hour}
	commitStarted := make(chan struct{})
	commitRelease := make(chan struct{})
	var commitReleaseOnce sync.Once
	releaseCommit := func() { commitReleaseOnce.Do(func() { close(commitRelease) }) }
	t.Cleanup(releaseCommit)
	waitCtx, cancelWait := context.WithTimeout(t.Context(), 3*time.Second)
	t.Cleanup(cancelWait)
	registrar := &preCommitReplayRegistrar{
		pump: pump, commitStarted: commitStarted, commitRelease: commitRelease, waitCtx: waitCtx,
	}
	offerPaused := make(chan struct{})
	var pauseOnce sync.Once
	manager := &Manager{
		cfg: Config{
			HandoffConsumerID: "replay-client", HandoffSubscriptions: registrar, EgressBlockTimeout: time.Second,
			offerRevalidationGate: func() <-chan struct{} {
				pauseOnce.Do(func() { close(offerPaused) })
				return nil
			},
		},
		active: make(map[runtimeKey]*sessionRuntime), current: make(map[handoff.Scope]*sessionRuntime),
		logical: make(map[identity.SessionID]*logicalSession), pendingPublications: make(map[handoff.Scope]*publicationAttempt),
	}
	offerResults := make(chan error, 16)
	pump.Sink = &deliveryRecordingSink{manager: manager, results: offerResults}
	previous, previousInputs, previousDone := currentRuntimeWithCapacity(scope, "canceled-generation-previous", 16)
	replacement, replacementInputs, replacementDone := currentRuntimeWithCapacity(scope, "canceled-generation-replacement", 16)
	t.Cleanup(func() { close(previousDone); close(replacementDone) })
	require.NoError(t, manager.register(previous))

	replacement.key.incarnation = 2
	replacement.state.ids.Incarnation = 2
	replayAfter := int64(0)
	replacement.replayAfter = &replayAfter
	logical := &logicalSession{scope: scope, highWater: 1, reconnecting: true, turnBoundary: true, state: logicalTombstoned}
	manager.activeMu.Lock()
	manager.logical[replacement.key.sessionID] = logical
	manager.activeMu.Unlock()
	replacement.presentationMu.Lock()
	presentationLocked := true
	defer func() {
		if presentationLocked {
			replacement.presentationMu.Unlock()
		}
	}()
	published := make(chan error, 1)
	go func() { published <- manager.registerReplacement(replacement, logical, 1) }()
	select {
	case <-commitStarted:
	case <-time.After(time.Second):
		t.Fatal("replacement publication did not pause before replay activation")
	}
	manager.activeMu.RLock()
	current := manager.current[scope]
	manager.activeMu.RUnlock()
	require.Same(t, replacement, current, "replacement must be directory-current before replay activation")

	startOwnedEventPump(t, pump)
	select {
	case <-offerPaused:
	case <-time.After(time.Second):
		t.Fatal("older-generation offer did not pass replacement snapshot and slot reservation")
	}
	releaseCommit()
	select {
	case err := <-published:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("replacement publication did not clear its replay fence")
	}
	replacement.presentationMu.Unlock()
	presentationLocked = false

	select {
	case err := <-offerResults:
		assert.ErrorIs(t, err, context.Canceled,
			"the canceled older-generation sequence 6 must be rejected before canonical replacement acceptance")
	case <-time.After(time.Second):
		t.Fatal("canceled older-generation offer did not resolve")
	}
	require.Eventually(t, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		return store.cursor == 6 && assert.Equal(t, []int64{6}, store.advances)
	}, time.Second, time.Millisecond, "the replacement replay did not finish at sequence 6")

	var got []int64
	for {
		select {
		case input := <-replacementInputs:
			got = append(got, input.(HandoffEventInput).Event.Seq)
		default:
			goto drained
		}
	}
drained:
	require.Equal(t, []int64{1, 2, 3, 4, 5, 6}, got,
		"replacement must observe only the activated replay in durable sequence order")
	select {
	case input := <-previousInputs:
		t.Fatalf("older-generation event reached previous runtime: sequence %d", input.(HandoffEventInput).Event.Seq)
	default:
	}
	require.NoError(t, waitCtx.Err(), "pre-commit replay helper reached its explicit deadline")
}

func TestStaleManagerUnregisterCannotDeleteCommittedReconnectSubscription(t *testing.T) {
	scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "stale-unregister-reconnect"}
	event := handoff.Event{
		Seq: 1, EventID: "replacement-event", HandoffID: "handoff", TenantID: scope.TenantID,
		ConversationID: scope.ConversationID, Kind: "handoff.created", State: handoff.StatePending,
		Payload: json.RawMessage(`{}`),
	}
	store := &deliveryStore{event: event}
	manual := clock.NewManual(time.Unix(0, 0))
	pump := &handoff.EventPump{Reader: store, Cursors: store, Clock: manual, PollInterval: time.Second}
	credentials, err := NewReconnectCredentials([]byte(strings.Repeat("u", 32)), clock.Real{})
	require.NoError(t, err)
	owners := &blockingTombstoneOwners{
		MemoryReconnectOwners: NewMemoryReconnectOwners(clock.Real{}),
		started:               make(chan struct{}),
		release:               make(chan struct{}),
	}
	var releaseOnce sync.Once
	releaseTombstone := func() { releaseOnce.Do(func() { close(owners.release) }) }
	t.Cleanup(releaseTombstone)
	manager := &Manager{
		cfg: Config{
			HandoffConsumerID: "replay-client", HandoffSubscriptions: pump, EgressBlockTimeout: time.Second,
			ReconnectCredentials: credentials, ReconnectOwners: owners, ReconnectOwnerID: "stale-unregister-owner",
		},
		active: make(map[runtimeKey]*sessionRuntime), current: make(map[handoff.Scope]*sessionRuntime),
		logical: make(map[identity.SessionID]*logicalSession), pendingPublications: make(map[handoff.Scope]*publicationAttempt),
	}
	pump.Sink = manager

	previous, _, previousDone := currentRuntime(scope, "stale-unregister-session")
	replacement, replacementInputs, replacementDone := currentRuntime(scope, "stale-unregister-session")
	t.Cleanup(func() { close(previousDone); close(replacementDone) })
	require.NoError(t, manager.register(previous))
	manager.commitPublication(previous)

	unregistered := make(chan error, 1)
	go func() { unregistered <- manager.unregister(previous.key, previous) }()
	select {
	case <-owners.started:
	case <-time.After(time.Second):
		t.Fatal("stale unregister did not pause after removing directory ownership")
	}
	manager.activeMu.Lock()
	logical := manager.logical[previous.key.sessionID]
	require.NotNil(t, logical)
	logical.reconnecting = true
	manager.activeMu.Unlock()
	replacement.key.incarnation = 2
	replacement.state.ids.Incarnation = 2
	replacement.publicationRequiresCommit = true
	replayAfter := int64(0)
	replacement.replayAfter = &replayAfter
	require.NoError(t, manager.registerReplacement(replacement, logical, 1))
	manager.commitPublication(replacement)

	releaseTombstone()
	select {
	case err := <-unregistered:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("stale unregister did not finish after owner I/O resumed")
	}
	startOwnedEventPump(t, pump)
	select {
	case input := <-replacementInputs:
		require.Equal(t, event.Seq, input.(HandoffEventInput).Event.Seq)
	case <-time.After(time.Second):
		t.Fatal("stale unregister deleted the committed replacement subscription")
	}
}

func TestManagerReplayCommitFailureStaysPendingAndDoesNotNotify(t *testing.T) {
	scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "replay-commit-failure"}
	commitErr := errors.New("commit reservation lost")
	registrar := &replayCommitProbeRegistrar{commitErr: commitErr}
	manager := &Manager{
		cfg:    Config{HandoffConsumerID: "replay-client", HandoffSubscriptions: registrar, EgressBlockTimeout: time.Second},
		active: make(map[runtimeKey]*sessionRuntime), current: make(map[handoff.Scope]*sessionRuntime),
		logical: make(map[identity.SessionID]*logicalSession), pendingPublications: make(map[handoff.Scope]*publicationAttempt),
	}
	previous, _, previousDone := currentRuntime(scope, "replay-commit-failure-session")
	replacement, _, replacementDone := currentRuntime(scope, "replay-commit-failure-session")
	t.Cleanup(func() { close(previousDone); close(replacementDone) })
	require.NoError(t, manager.register(previous))
	replacement.key.incarnation = 2
	replacement.state.ids.Incarnation = 2
	replacement.publicationRequiresCommit = true
	replayAfter := int64(0)
	replacement.replayAfter = &replayAfter
	logical := &logicalSession{scope: scope, highWater: 1, reconnecting: true, turnBoundary: true, state: logicalTombstoned}
	manager.activeMu.Lock()
	manager.logical[replacement.key.sessionID] = logical
	manager.activeMu.Unlock()
	require.NoError(t, manager.registerReplacement(replacement, logical, 1))

	err := manager.commitPublication(replacement)
	require.ErrorIs(t, err, ErrRecoveryUnavailable)
	require.ErrorIs(t, err, commitErr)
	require.Zero(t, registrar.notifyCount.Load(), "failed replay commit must not notify delivery as if activation succeeded")
	manager.activeMu.RLock()
	require.NotNil(t, manager.pendingPublications[scope], "failed replay commit must leave the publication fence visible")
	require.Same(t, replacement, manager.current[scope])
	manager.activeMu.RUnlock()
	replacement.mu.Lock()
	require.True(t, replacement.publicationPending)
	require.NotNil(t, replacement.publicationAttempt)
	require.Nil(t, replacement.handoffSubscriptionLease)
	replacement.mu.Unlock()
	manager.rollbackReplacementPublication(replacement, logical, 1, 0)
}

func TestManagerPostCommitOwnershipLossReleasesLeaseWithoutNotify(t *testing.T) {
	scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "replay-post-commit-loss"}
	commitRelease := make(chan struct{})
	var commitReleaseOnce sync.Once
	releaseCommit := func() { commitReleaseOnce.Do(func() { close(commitRelease) }) }
	t.Cleanup(releaseCommit)
	registrar := &replayCommitProbeRegistrar{commitStarted: make(chan struct{}), commitRelease: commitRelease}
	manager := &Manager{
		cfg:    Config{HandoffConsumerID: "replay-client", HandoffSubscriptions: registrar, EgressBlockTimeout: time.Second},
		active: make(map[runtimeKey]*sessionRuntime), current: make(map[handoff.Scope]*sessionRuntime),
		logical: make(map[identity.SessionID]*logicalSession), pendingPublications: make(map[handoff.Scope]*publicationAttempt),
	}
	previous, _, previousDone := currentRuntime(scope, "replay-post-commit-loss-session")
	replacement, _, replacementDone := currentRuntime(scope, "replay-post-commit-loss-session")
	t.Cleanup(func() { close(previousDone); close(replacementDone) })
	require.NoError(t, manager.register(previous))
	replacement.key.incarnation = 2
	replacement.state.ids.Incarnation = 2
	replacement.publicationRequiresCommit = true
	replayAfter := int64(0)
	replacement.replayAfter = &replayAfter
	logical := &logicalSession{scope: scope, highWater: 1, reconnecting: true, turnBoundary: true, state: logicalTombstoned}
	manager.activeMu.Lock()
	manager.logical[replacement.key.sessionID] = logical
	manager.activeMu.Unlock()
	require.NoError(t, manager.registerReplacement(replacement, logical, 1))

	committed := make(chan error, 1)
	go func() { committed <- manager.commitPublication(replacement) }()
	select {
	case <-registrar.commitStarted:
	case <-time.After(time.Second):
		t.Fatal("replay commit did not reach the external commit boundary")
	}
	manager.activeMu.Lock()
	manager.current[scope] = previous
	manager.activeMu.Unlock()
	releaseCommit()
	select {
	case err := <-committed:
		require.ErrorIs(t, err, ErrStaleReconnect)
	case <-time.After(time.Second):
		t.Fatal("post-commit ownership-loss cleanup did not finish")
	}
	require.Equal(t, int32(1), registrar.releaseCount.Load(), "lost ownership after commit must release the exact committed lease")
	require.Zero(t, registrar.notifyCount.Load(), "lost ownership after commit must not notify delivery as if publication succeeded")
	replacement.mu.Lock()
	require.True(t, replacement.publicationPending)
	require.NotNil(t, replacement.publicationAttempt)
	require.Nil(t, replacement.handoffSubscriptionLease)
	replacement.mu.Unlock()
	manager.rollbackReplacementPublication(replacement, logical, 1, 0)
}

func TestReplayCompensationPreservesPreviousSubscriptionAndExactOverlay(t *testing.T) {
	partialErr := errors.New("replay callback failed after mutation")
	restoreErr := errors.New("base subscription restore failed")
	for _, test := range []struct {
		name        string
		callbackErr error
		rollback    bool
	}{
		{name: "partial mutation callback error", callbackErr: partialErr},
		{name: "rollback restore error", rollback: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: identity.ConversationID("replay-compensation-" + strings.ReplaceAll(test.name, " ", "-"))}
			stale := handoff.Event{
				Seq: 1, EventID: "stale-replay-event", HandoffID: "handoff", TenantID: scope.TenantID,
				ConversationID: scope.ConversationID, Kind: "handoff.created", State: handoff.StatePending,
				Payload: json.RawMessage(`{}`),
			}
			baseStore := &deliveryStore{event: stale, cursor: 1}
			readRelease := make(chan struct{})
			var readReleaseOnce sync.Once
			releaseReads := func() { readReleaseOnce.Do(func() { close(readRelease) }) }
			t.Cleanup(releaseReads)
			store := &observingDeliveryStore{
				deliveryStore: baseStore, readAfter: make(chan int64, 8), readDone: make(chan int64, 8), release: readRelease,
			}
			manual := clock.NewManual(time.Unix(0, 0))
			pump := &handoff.EventPump{Reader: store, Cursors: store, Clock: manual, PollInterval: time.Second}
			registrar := &replayRegistrarProbe{pump: pump, replayErr: test.callbackErr, restoreErr: restoreErr}
			manager := &Manager{
				cfg:    Config{HandoffConsumerID: "replay-client", HandoffSubscriptions: registrar, EgressBlockTimeout: time.Second},
				active: make(map[runtimeKey]*sessionRuntime), current: make(map[handoff.Scope]*sessionRuntime),
				logical: make(map[identity.SessionID]*logicalSession), pendingPublications: make(map[handoff.Scope]*publicationAttempt),
			}
			pump.Sink = manager
			previous, previousInputs, previousDone := currentRuntime(scope, identity.SessionID("previous-"+test.name))
			replacement, _, replacementDone := currentRuntime(scope, identity.SessionID("replacement-"+test.name))
			t.Cleanup(func() { close(previousDone); close(replacementDone) })
			require.NoError(t, manager.register(previous))
			replacement.key.incarnation = 2
			replacement.state.ids.Incarnation = 2
			replacement.publicationRequiresCommit = test.rollback
			replayAfter := int64(0)
			replacement.replayAfter = &replayAfter
			logical := &logicalSession{scope: scope, highWater: 1, reconnecting: true, turnBoundary: true, state: logicalTombstoned}
			manager.activeMu.Lock()
			manager.logical[replacement.key.sessionID] = logical
			manager.activeMu.Unlock()

			publicationErr := manager.registerReplacement(replacement, logical, 1)
			if test.rollback {
				require.NoError(t, publicationErr)
				manager.rollbackReplacementPublication(replacement, logical, 1, 0)
			} else {
				require.ErrorIs(t, publicationErr, ErrRecoveryUnavailable)
				require.NotErrorIs(t, publicationErr, restoreErr, "transactional replay rollback must not use fallible base restoration")
			}
			registrar.mu.Lock()
			require.Equal(t, 1, registrar.registerCalls, "replay compensation must leave the existing base registration untouched")
			registrar.mu.Unlock()
			manager.activeMu.RLock()
			current := manager.current[scope]
			manager.activeMu.RUnlock()
			require.Same(t, previous, current)
			previous.mu.Lock()
			require.True(t, previous.accepting)
			previous.mu.Unlock()

			startOwnedEventPump(t, pump)
			var observedRead bool
			select {
			case after := <-store.readAfter:
				observedRead = true
				assert.Equal(t, int64(1), after, "compensation must remove only its replay overlay and retain the stored base cursor")
			case <-time.After(time.Second):
				t.Error("published previous owner lost its base event-pump subscription during replay compensation")
			}
			releaseReads()
			if !observedRead {
				return
			}
			select {
			case <-store.readDone:
			case <-time.After(time.Second):
				t.Fatal("event pump did not complete its compensated subscription read")
			}
			select {
			case input := <-previousInputs:
				t.Errorf("cancelled replay overlay leaked stale sequence %d", input.(HandoffEventInput).Event.Seq)
			default:
			}

			fresh := stale
			fresh.Seq = 2
			fresh.EventID = "fresh-event"
			baseStore.mu.Lock()
			baseStore.event = fresh
			baseStore.mu.Unlock()
			manual.Advance(time.Second)
			select {
			case input := <-previousInputs:
				require.Equal(t, fresh.Seq, input.(HandoffEventInput).Event.Seq)
			case <-time.After(time.Second):
				t.Fatal("restored previous owner did not receive a fresh event after replay compensation")
			}
		})
	}
}

func TestFailedReconnectSameScopePublicationRestoresPriorRuntime(t *testing.T) {
	scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "rollback-reconnect"}
	manager := NewManager(Config{IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second}, testkit.NewFakeConnector(1), managerIDs{})
	require.NoError(t, manager.validationErr)
	previous, _, previousDone := currentRuntime(scope, "session")
	defer close(previousDone)
	previous.key.incarnation = 1
	previous.state.ids.Incarnation = 1
	replayAfter := int64(0)
	replacement, _, replacementDone := currentRuntime(scope, "session")
	defer close(replacementDone)
	replacement.key.incarnation = 2
	replacement.state.ids.Incarnation = 2
	replacement.replayAfter = &replayAfter
	replacement.publicationRequiresCommit = true
	logical := &logicalSession{
		scope: scope, subject: cloneSubject(previous.subject), highWater: 1,
		state: logicalTombstoned, reconnecting: true, turnBoundary: true,
	}
	manager.activeMu.Lock()
	manager.active[previous.key] = previous
	manager.current[scope] = previous
	manager.logical[previous.key.sessionID] = logical
	manager.activeMu.Unlock()
	previous.mu.Lock()
	originalGeneration := previous.acceptanceGeneration
	previous.mu.Unlock()

	require.NoError(t, manager.registerReplacement(replacement, logical, 1))
	manager.rollbackReplacementPublication(replacement, logical, 1, 0)

	manager.activeMu.RLock()
	require.Same(t, previous, manager.current[scope])
	manager.activeMu.RUnlock()
	previous.mu.Lock()
	require.True(t, previous.accepting)
	require.Equal(t, originalGeneration, previous.acceptanceGeneration)
	previous.mu.Unlock()
}

func TestSameScopeReplacementRollbackLockOrderCompletes(t *testing.T) {
	scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "replacement-rollback-lock-order"}
	offerAtRevalidation := make(chan struct{})
	releaseOffer := make(chan struct{})
	var gateOnce sync.Once
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
		offerRevalidationGate: func() <-chan struct{} {
			gateOnce.Do(func() { close(offerAtRevalidation) })
			return releaseOffer
		},
	}, testkit.NewFakeConnector(1), managerIDs{})
	require.NoError(t, manager.validationErr)
	previous, _, previousDone := currentRuntime(scope, "replacement-rollback-previous")
	t.Cleanup(func() { close(previousDone) })
	require.NoError(t, manager.register(previous))
	previous.mu.Lock()
	originalGeneration := previous.acceptanceGeneration
	previous.mu.Unlock()

	offerDone := make(chan error, 1)
	go func() {
		offerDone <- manager.OfferHandoffEvent(context.Background(), scope, handoff.Event{
			Seq: 1, EventID: "replacement-rollback-event", HandoffID: "handoff", TenantID: scope.TenantID,
			ConversationID: scope.ConversationID, Kind: "handoff.created", State: handoff.StatePending,
			Payload: json.RawMessage(`{}`),
		})
	}()
	select {
	case <-offerAtRevalidation:
	case <-time.After(time.Second):
		t.Fatal("offer did not reach ownership revalidation")
	}

	replacement, _, replacementDone := currentRuntime(scope, "replacement-rollback-new")
	t.Cleanup(func() { close(replacementDone) })
	replacement.publicationRequiresCommit = true
	replacementDonePublishing := make(chan error, 1)
	go func() {
		if err := manager.register(replacement); err != nil {
			replacementDonePublishing <- err
			return
		}
		manager.activeMu.RLock()
		logical := manager.logical[replacement.key.sessionID]
		manager.activeMu.RUnlock()
		manager.rollbackInitialPublication(replacement, logical)
		replacementDonePublishing <- nil
	}()
	select {
	case err := <-replacementDonePublishing:
		require.NoError(t, err)
	case <-time.After(250 * time.Millisecond):
		close(releaseOffer)
		t.Fatal("same-scope replacement rollback deadlocked with offer ownership revalidation")
	}
	close(releaseOffer)
	select {
	case err := <-offerDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("offer did not complete after same-scope replacement rollback")
	}

	manager.activeMu.RLock()
	require.Same(t, previous, manager.current[scope])
	manager.activeMu.RUnlock()
	previous.mu.Lock()
	require.True(t, previous.accepting)
	require.Equal(t, originalGeneration, previous.acceptanceGeneration)
	previous.mu.Unlock()
}
