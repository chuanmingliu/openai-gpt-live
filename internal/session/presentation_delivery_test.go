package session

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
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
	done           chan struct{}
	sessionStarted chan struct{}
	sessionRelease chan struct{}
	visualStarted  chan protocol.ServerEvent
	visualRelease  chan struct{}
	visualErr      error
	closeOnce      sync.Once
	sessionOnce    sync.Once
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

type signalingSubscriptions struct {
	mu         sync.Mutex
	registered int
	second     chan struct{}
	once       sync.Once
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
	require.True(t, waitCurrent("context-replacement"))
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
	require.NotNil(t, control.written)
	control.written <- nil
	require.NoError(t, <-messageDone)
	stop()
	require.ErrorIs(t, <-loopDone, context.Canceled)
	policy.mu.Lock()
	assert.Positive(t, policy.calls)
	policy.mu.Unlock()
}
