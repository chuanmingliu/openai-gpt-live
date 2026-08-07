package session

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	rand "math/rand/v2"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/presentation"
	"github.com/chuanmingliu/openai-gpt-live/internal/protocol"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/chuanmingliu/openai-gpt-live/internal/safety"
	"golang.org/x/sync/errgroup"
)

const (
	defaultIngressFrames       = 25
	defaultEgressFrames        = 75
	defaultControlEvents       = 256
	defaultWriterControlEvents = 64
	maximumQueueCapacity       = 10_000
	pcmFrameBytes              = 960
	internalCloseTimeout       = time.Second
	presentationOfferTimeout   = 250 * time.Millisecond
	defaultReconnectRetries    = 2
	defaultReconnectRetryBase  = 100 * time.Millisecond
	defaultReconnectRetryMax   = time.Second
	maximumLogicalSessions     = 10_000
)

var (
	ErrInvalidManagerConfig = errors.New("invalid session manager config")
	ErrSessionNotActive     = errors.New("session is not active")
	ErrSessionAlreadyActive = errors.New("session is already active")
	ErrIngressBackpressure  = errors.New("session ingress backpressure timeout")
	ErrEgressBackpressure   = errors.New("session egress backpressure timeout")
	ErrControlOverflow      = errors.New("session control queue is full")
	ErrUnsupportedAsync     = errors.New("async session effects are unsupported")
)

var errGracefulSessionClose = errors.New("session closed")
var errEgressInvalidated = errors.New("egress epoch invalidated")

type ClientConn interface {
	Read(context.Context) (protocol.ClientEvent, error)
	Write(context.Context, protocol.ServerEvent) error
	Close(context.Context, int, string) error
}

type Config struct {
	IngressFrames         int
	EgressFrames          int
	ControlEvents         int
	WriterControlEvents   int
	IngressBlockTimeout   time.Duration
	EgressBlockTimeout    time.Duration
	DurableDispatcher     DurableEffectDispatcher
	HandoffConsumerID     string
	HandoffSubscriptions  HandoffSubscriptionRegistrar
	PresentationPolicy    presentation.Policy
	SafetyPolicy          safety.Policy
	RecoveryStore         RecoveryStore
	RecoveryItemLimit     int
	RecoveryByteLimit     int
	ReconnectMaxRetries   int
	ReconnectRetryBase    time.Duration
	ReconnectRetryMax     time.Duration
	Clock                 clock.Clock
	Jitter                clock.JitterSource
	reactorApplyGate      func(Input) <-chan struct{}
	reconnectBoundaryGate func() <-chan struct{}
	initialRegisterGate   func() <-chan struct{}
}

type HandoffSubscriptionRegistrar interface {
	Register(handoff.EventSubscription) error
	Unregister(handoff.EventSubscription)
}

type HandoffReplayRegistrar interface {
	RegisterReplay(handoff.EventSubscription, int64) error
}

type TrustedSessionContext struct {
	IDs     IDs
	Subject Subject
}

type DurableEffectDispatcher interface {
	DispatchTranscript(context.Context, TrustedSessionContext, TranscriptRequestedEffect) error
	DispatchHandoff(context.Context, TrustedSessionContext, HandoffRequestedEffect) error
}

type MetricsHooks struct {
	QueueOverflow func(string)
	AudioDropped  func(int)
}

func normalizeMetricsHooks(hooks MetricsHooks) MetricsHooks {
	if hooks.QueueOverflow == nil {
		hooks.QueueOverflow = func(string) {}
	}
	if hooks.AudioDropped == nil {
		hooks.AudioDropped = func(int) {}
	}
	return hooks
}

func (h MetricsHooks) queueOverflow(queue string) {
	if h.QueueOverflow != nil {
		h.QueueOverflow(queue)
	}
}

func (h MetricsHooks) audioDropped(bytes int) {
	if h.AudioDropped != nil {
		h.AudioDropped(bytes)
	}
}

type Subject struct {
	TenantID    string
	UserID      string
	Scopes      map[string]struct{}
	Permissions json.RawMessage
	ExpiresAt   time.Time
}

type OpenRequest struct {
	Subject        Subject
	RequestedModel string
	ConversationID identity.ConversationID
	ReconnectToken string
}

type IDGenerator interface {
	NewSessionID() identity.SessionID
	NewConversationID() identity.ConversationID
}

type Manager struct {
	cfg       Config
	connector realtime.Connector
	ids       IDGenerator

	validationErr error
	activeMu      sync.RWMutex
	active        map[runtimeKey]*sessionRuntime
	current       map[handoff.Scope]*sessionRuntime
	logical       map[identity.SessionID]*logicalSession
	metricsMu     sync.RWMutex
	metrics       MetricsHooks
}

type logicalSession struct {
	scope         handoff.Scope
	subject       Subject
	model         string
	highWater     identity.Incarnation
	responseEpoch identity.ResponseEpoch
	runtime       *sessionRuntime
	reconnecting  bool
	turnBoundary  bool
	state         logicalSessionState
}

type logicalSessionState uint8

const (
	logicalEstablishing logicalSessionState = iota
	logicalActive
	logicalTombstoned
	logicalEstablishmentFailed
)

type managerJitter struct{}

func (managerJitter) Duration(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(max) + 1))
}

type runtimeKey struct {
	sessionID   identity.SessionID
	incarnation identity.Incarnation
}

type sessionRuntime struct {
	key      runtimeKey
	subject  Subject
	inbox    chan reactorMessage
	done     <-chan struct{}
	quiesced <-chan struct{}
	state    *runState

	mu          sync.Mutex
	accepting   bool
	inboxSlots  chan struct{}
	replayAfter *int64
}

// fenceAtTurnBoundary linearizes the final boundary decision with every
// external intake path. Callers that acquired the acceptance gate first are
// fully represented in the boundary high-waters; callers arriving afterward
// observe a closed gate and cannot enqueue into the old runtime.
func (r *sessionRuntime) fenceAtTurnBoundary(ctx context.Context, gate func() <-chan struct{}) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.accepting {
		// A previous reconnect may have closed intake but timed out while
		// waiting for bounded cleanup. A later caller is allowed to continue
		// waiting on that same fenced runtime; concurrent reconnects remain
		// excluded by logicalSession.reconnecting under activeMu.
		if !r.atTurnBoundary() {
			return ErrReconnectNotAtBoundary
		}
		return nil
	}
	if !r.atTurnBoundary() {
		return ErrReconnectNotAtBoundary
	}
	if gate != nil {
		if wait := gate(); wait != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-wait:
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	r.accepting = false
	// Internal provider/control completion can move the boundary while the
	// test barrier is open. Recheck after closing intake and reopen the gate if
	// the session is no longer clean.
	if !r.atTurnBoundary() {
		r.accepting = true
		return ErrReconnectNotAtBoundary
	}
	return nil
}

func (r *sessionRuntime) handoffSubscription(consumer string) handoff.EventSubscription {
	return handoff.EventSubscription{ConsumerID: consumer, Scope: r.handoffScope()}
}

func (r *sessionRuntime) stop() {
	r.mu.Lock()
	r.accepting = false
	r.mu.Unlock()
}

func (r *sessionRuntime) atTurnBoundary() bool {
	if r == nil || r.state == nil || r.state.partialInput.Load() {
		return false
	}
	if r.state.ingressCompleted.Load() != r.state.latestIngressSequence() {
		return false
	}
	if r.state.controlAccepted.Load() != r.state.controlApplied.Load() || r.state.pendingVoice.Load() != 0 {
		return false
	}
	if r.state.externalAccepted.Load() != r.state.externalApplied.Load() {
		return false
	}
	r.state.boundaryMu.RLock()
	snapshot := r.state.boundary
	r.state.boundaryMu.RUnlock()
	if snapshot.UserSpeaking {
		return false
	}
	return snapshot.Response == ResponseIdle || snapshot.Response == ResponseTerminal
}

type reactorMessage struct {
	input               Input
	ingressAfter        uint64
	egressAfter         uint64
	done                chan error
	requireClientWrites bool
	offerCtx            context.Context
	controlSequence     uint64
}

type ingressFrame struct {
	sequence uint64
	audio    []byte
}

type outputPCMFramer struct {
	tail     []byte
	hasAudio bool
}

func (f *outputPCMFramer) push(audio []byte, emit func([]byte) error) error {
	if len(audio) == 0 {
		return nil
	}
	f.hasAudio = true
	if len(f.tail) != 0 {
		needed := pcmFrameBytes - len(f.tail)
		if len(audio) < needed {
			f.tail = append(f.tail, audio...)
			return nil
		}
		frame := make([]byte, pcmFrameBytes)
		copy(frame, f.tail)
		copy(frame[len(f.tail):], audio[:needed])
		f.tail = nil
		audio = audio[needed:]
		if err := emit(frame); err != nil {
			return err
		}
	}
	for len(audio) >= pcmFrameBytes {
		frame := append([]byte(nil), audio[:pcmFrameBytes]...)
		audio = audio[pcmFrameBytes:]
		if err := emit(frame); err != nil {
			return err
		}
	}
	f.tail = append(f.tail[:0], audio...)
	return nil
}

func (f *outputPCMFramer) flush() ([]byte, error) {
	if !f.hasAudio {
		return nil, errors.New("PCM commit is empty")
	}
	if len(f.tail)%2 != 0 {
		return nil, errors.New("PCM commit ends with a split sample")
	}
	tail := append([]byte(nil), f.tail...)
	f.reset()
	return tail, nil
}

func (f *outputPCMFramer) reset() {
	f.tail = nil
	f.hasAudio = false
}

type egressFrame struct {
	order       uint64
	sequence    uint64
	incarnation identity.Incarnation
	epoch       identity.ResponseEpoch
	eventID     string
	responseID  string
	itemID      string
	audio       []byte
}

type voiceControl struct {
	command realtime.Command
}

type clientControl struct {
	order    uint64
	afterSeq uint64
	event    protocol.ServerEvent
	close    *CloseEffect
	written  chan error
	writeCtx context.Context
}

type sequenceProgress struct {
	mu        sync.Mutex
	completed uint64
	pending   map[uint64]struct{}
	changed   chan struct{}
}

func newSequenceProgress() *sequenceProgress {
	return &sequenceProgress{pending: make(map[uint64]struct{}), changed: make(chan struct{}, 1)}
}

func (p *sequenceProgress) mark(sequence uint64) {
	if sequence == 0 {
		return
	}
	p.mu.Lock()
	if sequence > p.completed {
		p.pending[sequence] = struct{}{}
		for {
			if _, ok := p.pending[p.completed+1]; !ok {
				break
			}
			delete(p.pending, p.completed+1)
			p.completed++
		}
	}
	p.mu.Unlock()
	select {
	case p.changed <- struct{}{}:
	default:
	}
}

func (p *sequenceProgress) reached(sequence uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.completed >= sequence
}

type activeAudioWrite struct {
	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

func (a *activeAudioWrite) begin(parent context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	a.mu.Lock()
	a.cancel = cancel
	a.done = done
	a.mu.Unlock()
	return ctx, func() {
		cancel()
		a.mu.Lock()
		if a.done == done {
			a.cancel = nil
			a.done = nil
		}
		close(done)
		a.mu.Unlock()
	}
}

func (a *activeAudioWrite) cancelAndWait() error {
	a.mu.Lock()
	cancel, done := a.cancel, a.done
	if cancel != nil {
		cancel()
	}
	a.mu.Unlock()
	if done == nil {
		return nil
	}
	timer := time.NewTimer(internalCloseTimeout)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		return errors.New("cancel active client audio write: internal timeout")
	}
}

type runState struct {
	cfg          Config
	ids          IDs
	runtime      *sessionRuntime
	voice        realtime.Session
	client       ClientConn
	fence        *EpochFence
	inbox        chan reactorMessage
	ingress      chan ingressFrame
	ingressSlots chan struct{}
	voiceCtl     chan voiceControl
	egress       chan egressFrame
	clientCtl    chan clientControl
	inboxSlots   chan struct{}
	voiceSlots   chan struct{}
	clientSlots  chan struct{}
	egressSlots  chan struct{}
	progress     *sequenceProgress
	activeWrite  *activeAudioWrite
	metrics      MetricsHooks
	subject      Subject
	dispatcher   DurableEffectDispatcher

	ingressSequence uint64
	egressSequence  uint64
	eventSequence   uint64
	sequenceMu      sync.Mutex

	outputMu    sync.Mutex
	outputOrder uint64

	closeClientOnce sync.Once
	closeClientDone chan struct{}
	closeClientMu   sync.Mutex
	closeClientErr  error
	graceful        atomic.Bool

	cancel context.CancelFunc

	boundaryMu       sync.RWMutex
	boundary         Snapshot
	partialInput     atomic.Bool
	ingressCompleted atomic.Uint64
	controlAccepted  atomic.Uint64
	controlApplied   atomic.Uint64
	externalAccepted atomic.Uint64
	externalApplied  atomic.Uint64
	pendingVoice     atomic.Int64
}

func NewManager(cfg Config, connector realtime.Connector, ids IDGenerator) *Manager {
	normalized, err := normalizeManagerConfig(cfg)
	return &Manager{
		cfg:           normalized,
		connector:     connector,
		ids:           ids,
		validationErr: err,
		active:        make(map[runtimeKey]*sessionRuntime),
		current:       make(map[handoff.Scope]*sessionRuntime),
		logical:       make(map[identity.SessionID]*logicalSession),
		metrics:       normalizeMetricsHooks(MetricsHooks{}),
	}
}

func (m *Manager) SetMetricsHooks(hooks MetricsHooks) {
	if m != nil {
		m.metricsMu.Lock()
		m.metrics = normalizeMetricsHooks(hooks)
		m.metricsMu.Unlock()
	}
}

func (m *Manager) metricsSnapshot() MetricsHooks {
	m.metricsMu.RLock()
	defer m.metricsMu.RUnlock()
	return m.metrics
}

func normalizeManagerConfig(cfg Config) (Config, error) {
	defaults := []struct {
		value *int
		name  string
		def   int
	}{
		{&cfg.IngressFrames, "ingress frames", defaultIngressFrames},
		{&cfg.EgressFrames, "egress frames", defaultEgressFrames},
		{&cfg.ControlEvents, "control events", defaultControlEvents},
		{&cfg.WriterControlEvents, "writer control events", defaultWriterControlEvents},
	}
	for _, setting := range defaults {
		if *setting.value == 0 {
			*setting.value = setting.def
		}
		if *setting.value < 0 || *setting.value > maximumQueueCapacity {
			return cfg, fmt.Errorf("%w: %s must be between 1 and %d", ErrInvalidManagerConfig, setting.name, maximumQueueCapacity)
		}
	}
	if cfg.IngressBlockTimeout <= 0 {
		return cfg, fmt.Errorf("%w: ingress block timeout must be positive", ErrInvalidManagerConfig)
	}
	if cfg.EgressBlockTimeout <= 0 {
		return cfg, fmt.Errorf("%w: egress block timeout must be positive", ErrInvalidManagerConfig)
	}
	if cfg.RecoveryItemLimit == 0 {
		cfg.RecoveryItemLimit = DefaultRecoveryItemLimit
	}
	if cfg.RecoveryByteLimit == 0 {
		cfg.RecoveryByteLimit = DefaultRecoveryByteLimit
	}
	if cfg.RecoveryItemLimit < 0 || cfg.RecoveryItemLimit > MaximumRecoveryItemLimit ||
		cfg.RecoveryByteLimit < 0 || cfg.RecoveryByteLimit > MaximumRecoveryByteLimit {
		return cfg, fmt.Errorf("%w: recovery bounds are unsafe", ErrInvalidManagerConfig)
	}
	if cfg.ReconnectMaxRetries == 0 {
		cfg.ReconnectMaxRetries = defaultReconnectRetries
	}
	if cfg.ReconnectMaxRetries < 0 || cfg.ReconnectMaxRetries > 16 {
		return cfg, fmt.Errorf("%w: reconnect retries are unsafe", ErrInvalidManagerConfig)
	}
	if cfg.ReconnectRetryBase == 0 {
		cfg.ReconnectRetryBase = defaultReconnectRetryBase
	}
	if cfg.ReconnectRetryMax == 0 {
		cfg.ReconnectRetryMax = defaultReconnectRetryMax
	}
	if cfg.ReconnectRetryBase < time.Millisecond || cfg.ReconnectRetryMax < cfg.ReconnectRetryBase || cfg.ReconnectRetryMax > time.Minute {
		return cfg, fmt.Errorf("%w: reconnect retry bounds are unsafe", ErrInvalidManagerConfig)
	}
	if isNilInterface(cfg.Clock) {
		cfg.Clock = clock.Real{}
	}
	if isNilInterface(cfg.Jitter) {
		cfg.Jitter = managerJitter{}
	}
	presentationConfigured := cfg.HandoffConsumerID != "" || !isNilInterface(cfg.HandoffSubscriptions) ||
		!isNilInterface(cfg.PresentationPolicy) || !isNilInterface(cfg.SafetyPolicy)
	if presentationConfigured {
		if strings.TrimSpace(cfg.HandoffConsumerID) == "" || len(cfg.HandoffConsumerID) > DurableIdentifierBytes ||
			!utf8.ValidString(cfg.HandoffConsumerID) ||
			isNilInterface(cfg.HandoffSubscriptions) || isNilInterface(cfg.PresentationPolicy) || isNilInterface(cfg.SafetyPolicy) {
			return cfg, fmt.Errorf("%w: handoff presentation dependencies are incomplete", ErrInvalidManagerConfig)
		}
	}
	return cfg, nil
}

func (m *Manager) Run(parent context.Context, request OpenRequest, client ClientConn) error {
	if m == nil {
		return errors.New("session manager is nil")
	}
	if m.validationErr != nil {
		return m.validationErr
	}
	if parent == nil {
		return errors.New("session context is nil")
	}
	if isNilInterface(m.connector) {
		return errors.New("realtime connector is nil")
	}
	if isNilInterface(m.ids) {
		return errors.New("session ID generator is nil")
	}
	if isNilInterface(client) {
		return errors.New("client connection is nil")
	}
	if err := validateSubject(request.Subject); err != nil {
		return err
	}
	subject := cloneSubject(request.Subject)

	sessionID := m.ids.NewSessionID()
	if sessionID == "" {
		return errors.New("generated session ID is empty")
	}
	conversationID := request.ConversationID
	if conversationID == "" {
		conversationID = m.ids.NewConversationID()
	}
	if conversationID == "" {
		return errors.New("conversation ID is empty")
	}
	ids := IDs{SessionID: sessionID, ConversationID: conversationID, Incarnation: 1}
	scope := handoff.Scope{TenantID: subject.TenantID, UserID: subject.UserID, ConversationID: conversationID}
	m.activeMu.Lock()
	if m.logical == nil {
		m.logical = make(map[identity.SessionID]*logicalSession)
	}
	if _, exists := m.logical[sessionID]; exists || len(m.logical) >= maximumLogicalSessions {
		m.activeMu.Unlock()
		return ErrSessionAlreadyActive
	}
	logical := &logicalSession{
		scope: scope, subject: cloneSubject(subject), model: request.RequestedModel, highWater: ids.Incarnation, turnBoundary: false,
		state: logicalEstablishing,
	}
	m.logical[sessionID] = logical
	m.activeMu.Unlock()
	defer m.failInitialEstablishment(sessionID, logical)

	sessionCtx, cancel := subjectContext(parent, subject.ExpiresAt)
	defer cancel()
	if err := sessionCtx.Err(); err != nil {
		return fmt.Errorf("subject expired: %w", err)
	}
	voice, err := m.connector.Connect(sessionCtx, realtime.OpenRequest{
		SessionID: sessionID, Incarnation: ids.Incarnation, Model: request.RequestedModel,
	})
	if err != nil {
		return fmt.Errorf("connect realtime session: %w", err)
	}
	if isNilInterface(voice) {
		return errors.New("realtime connector returned a nil session")
	}

	group, groupCtx := errgroup.WithContext(sessionCtx)
	quiesced := make(chan struct{})
	var quiescedOnce sync.Once
	markQuiesced := func() { quiescedOnce.Do(func() { close(quiesced) }) }
	state := newRunState(m.cfg, ids, voice, client, cancel)
	state.subject = cloneSubject(subject)
	state.metrics = m.metricsSnapshot()
	runtime := &sessionRuntime{
		key:        runtimeKey{sessionID: sessionID, incarnation: ids.Incarnation},
		subject:    subject,
		inbox:      state.inbox,
		done:       groupCtx.Done(),
		quiesced:   quiesced,
		state:      state,
		accepting:  true,
		inboxSlots: state.inboxSlots,
	}
	state.runtime = runtime
	var firstErr error
	var firstErrOnce sync.Once
	runLoop := func(name string, loop func(context.Context) error) func() error {
		return func() error {
			err := loop(groupCtx)
			runtime.stop()
			if err != nil && !errors.Is(err, errGracefulSessionClose) && !state.graceful.Load() {
				if !(errors.Is(err, context.Canceled) && groupCtx.Err() != nil) {
					firstErrOnce.Do(func() { firstErr = fmt.Errorf("%s: %w", name, err) })
				}
			}
			return err
		}
	}

	group.Go(runLoop("client reader", state.clientReader))
	group.Go(runLoop("voice writer", state.voiceWriter))
	group.Go(runLoop("voice reader", state.voiceReader))
	group.Go(runLoop("client writer", state.clientWriter))
	group.Go(runLoop("reactor", state.reactorLoop))
	abortBeforePublication := func(cause error) error {
		cancel()
		_ = group.Wait()
		state.closeClient(1000, "session ended")
		closeCtx, closeCancel := context.WithTimeout(context.Background(), internalCloseTimeout)
		_ = voice.Close(closeCtx)
		closeCancel()
		markQuiesced()
		if firstErr != nil {
			return firstErr
		}
		return cause
	}

	// A runtime is not externally discoverable until ConnectedInput has been
	// processed and the session.created frame has actually reached ClientConn.
	// This prevents the durable pump from overtaking session establishment.
	<-state.inboxSlots
	connected := reactorMessage{input: ConnectedInput{ProviderSession: json.RawMessage(`{}`)}, done: make(chan error, 1), requireClientWrites: true}
	state.acceptControl(&connected)
	state.inbox <- connected
	select {
	case err := <-connected.done:
		if err != nil {
			return abortBeforePublication(fmt.Errorf("establish client session: %w", err))
		}
	case <-groupCtx.Done():
		return abortBeforePublication(groupCtx.Err())
	}
	if err := groupCtx.Err(); err != nil {
		return abortBeforePublication(err)
	}
	if m.cfg.initialRegisterGate != nil {
		if gate := m.cfg.initialRegisterGate(); gate != nil {
			select {
			case <-groupCtx.Done():
				return abortBeforePublication(groupCtx.Err())
			case <-gate:
			}
		}
	}
	if err := groupCtx.Err(); err != nil {
		return abortBeforePublication(err)
	}
	if err := m.register(runtime); err != nil {
		return abortBeforePublication(err)
	}
	defer m.unregister(runtime.key, runtime)

	waitErr := group.Wait()
	cancel()
	state.closeClient(1000, "session ended")
	closeCtx, closeCancel := context.WithTimeout(context.Background(), internalCloseTimeout)
	closeErr := voice.Close(closeCtx)
	closeCancel()
	markQuiesced()

	if firstErr != nil {
		return firstErr
	}
	cleanupErr := errors.Join(state.clientCloseError(), closeErr)
	if errors.Is(waitErr, errGracefulSessionClose) || state.graceful.Load() {
		return cleanupErr
	}
	if sessionCtx.Err() != nil {
		return sessionCtx.Err()
	}
	if waitErr != nil {
		return waitErr
	}
	if cleanupErr != nil {
		return cleanupErr
	}
	return nil
}

func (m *Manager) failInitialEstablishment(sessionID identity.SessionID, logical *logicalSession) {
	m.activeMu.Lock()
	defer m.activeMu.Unlock()
	if m.logical[sessionID] != logical || logical.state != logicalEstablishing {
		return
	}
	logical.state = logicalEstablishmentFailed
	logical.runtime = nil
	logical.reconnecting = false
	logical.turnBoundary = false
}

// Reconnect replaces a logical session through an exact-incarnation
// compare-and-swap. Scope is expected to come from the authenticated transport
// and is checked against the immutable scope retained by the manager.
func (m *Manager) Reconnect(parent context.Context, request ReconnectRequest, client ClientConn) error {
	if m == nil || m.validationErr != nil {
		if m != nil && m.validationErr != nil {
			return m.validationErr
		}
		return ErrStaleReconnect
	}
	if parent == nil || isNilInterface(client) {
		return ErrStaleReconnect
	}
	if err := validateReconnectRequest(request); err != nil {
		return err
	}
	m.activeMu.RLock()
	logical := m.logical[request.SessionID]
	checkErr := m.checkReconnectLocked(request, logical)
	m.activeMu.RUnlock()
	if checkErr != nil {
		return checkErr
	}
	if isNilInterface(m.cfg.RecoveryStore) {
		return ErrRecoveryUnavailable
	}

	// The durable cursor preflight is observational and intentionally happens
	// before the current runtime is fenced. Client-invalid future cursors and
	// unavailable recovery cannot disturb an otherwise healthy live session.
	snapshot, err := m.cfg.RecoveryStore.LoadRecovery(parent, request.Scope, m.cfg.RecoveryItemLimit, m.cfg.RecoveryByteLimit)
	if err != nil {
		if parent.Err() != nil {
			return parent.Err()
		}
		return ErrRecoveryUnavailable
	}
	snapshot, err = validateRecoverySnapshot(request.Scope, snapshot, m.cfg.RecoveryItemLimit, m.cfg.RecoveryByteLimit)
	if err != nil {
		return err
	}
	if request.LastEventSeq > snapshot.LastEventSeq {
		return ErrStaleReconnect
	}
	if err := parent.Err(); err != nil {
		return err
	}

	// Re-run the complete CAS after the preflight read. A competing reconnect,
	// new accepted control, or boundary transition loses without being stopped.
	m.activeMu.Lock()
	if m.logical[request.SessionID] != logical {
		m.activeMu.Unlock()
		return ErrStaleReconnect
	}
	if err := m.checkReconnectLocked(request, logical); err != nil {
		m.activeMu.Unlock()
		return err
	}
	old := logical.runtime
	if old != nil {
		if err := old.fenceAtTurnBoundary(parent, m.cfg.reconnectBoundaryGate); err != nil {
			m.activeMu.Unlock()
			return err
		}
	}
	logical.reconnecting = true
	subject := cloneSubject(logical.subject)
	model := logical.model
	initialEpoch := logical.responseEpoch + 1
	newIncarnation := logical.highWater + 1
	m.activeMu.Unlock()

	published := false
	defer func() {
		if published {
			return
		}
		m.activeMu.Lock()
		if current := m.logical[request.SessionID]; current == logical && current.highWater == request.LastIncarnation {
			current.reconnecting = false
		}
		m.activeMu.Unlock()
	}()
	if old != nil {
		if err := m.stopAndQuiesceRuntimeReason(parent, old, 1001, "session reconnected"); err != nil {
			m.closeUnpublishedClient(parent, client, 1011, "recovery unavailable")
			return err
		}
	}
	if err := parent.Err(); err != nil {
		return err
	}

	// The preflight exists only to reject an invalid client cursor without
	// disturbing the live runtime. Once old intake is fenced and the runtime is
	// fully quiesced, read a fresh authoritative snapshot so durable commits
	// completed before quiescence are included in provider restoration.
	snapshot, err = m.cfg.RecoveryStore.LoadRecovery(parent, request.Scope, m.cfg.RecoveryItemLimit, m.cfg.RecoveryByteLimit)
	if err != nil {
		m.closeUnpublishedClient(parent, client, 1011, "recovery unavailable")
		if parent.Err() != nil {
			return parent.Err()
		}
		return ErrRecoveryUnavailable
	}
	snapshot, err = validateRecoverySnapshot(request.Scope, snapshot, m.cfg.RecoveryItemLimit, m.cfg.RecoveryByteLimit)
	if err != nil || request.LastEventSeq > snapshot.LastEventSeq {
		m.closeUnpublishedClient(parent, client, 1011, "recovery unavailable")
		if parent.Err() != nil {
			return parent.Err()
		}
		return ErrRecoveryUnavailable
	}

	sessionCtx, cancel := subjectContext(parent, subject.ExpiresAt)
	defer cancel()
	voice, err := m.connectReplacement(sessionCtx, realtime.OpenRequest{
		SessionID: request.SessionID, Incarnation: newIncarnation, Model: model,
	})
	if err != nil {
		m.closeUnpublishedClient(parent, client, 1011, "recovery unavailable")
		if sessionCtx.Err() != nil {
			return sessionCtx.Err()
		}
		return ErrRecoveryExhausted
	}
	commands, err := recoveryCommands(snapshot.CommittedItems)
	if err == nil {
		for _, command := range commands {
			if err = voice.Send(sessionCtx, command); err != nil {
				break
			}
		}
	}
	if err != nil {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), internalCloseTimeout)
		_ = voice.Close(closeCtx)
		closeCancel()
		m.closeUnpublishedClient(parent, client, 1011, "recovery unavailable")
		if sessionCtx.Err() != nil {
			return sessionCtx.Err()
		}
		return ErrRecoveryUnavailable
	}

	ids := IDs{SessionID: request.SessionID, ConversationID: request.Scope.ConversationID, Incarnation: newIncarnation}
	err = m.runReplacement(sessionCtx, cancel, subject, ids, client, voice, initialEpoch, logical, request.LastIncarnation, request.LastEventSeq)
	m.activeMu.RLock()
	published = logical.highWater == newIncarnation
	m.activeMu.RUnlock()
	return err
}

// activeMu must be held for reading or writing.
func (m *Manager) checkReconnectLocked(request ReconnectRequest, logical *logicalSession) error {
	if logical == nil || logical.scope != request.Scope || logical.highWater != request.LastIncarnation || logical.reconnecting ||
		uint64(logical.highWater) == math.MaxUint64 || uint64(logical.responseEpoch) == math.MaxUint64 {
		return ErrStaleReconnect
	}
	if logical.state == logicalEstablishing || logical.state == logicalEstablishmentFailed {
		return ErrStaleReconnect
	}
	if logical.state == logicalActive && logical.runtime == nil || logical.state == logicalTombstoned && logical.runtime != nil {
		return ErrStaleReconnect
	}
	if logical.runtime != nil && !logical.runtime.atTurnBoundary() || logical.runtime == nil && !logical.turnBoundary {
		return ErrReconnectNotAtBoundary
	}
	return nil
}

func (m *Manager) connectReplacement(ctx context.Context, request realtime.OpenRequest) (realtime.Session, error) {
	var lastErr error
	for attempt := 0; attempt <= m.cfg.ReconnectMaxRetries; attempt++ {
		voice, err := m.connector.Connect(ctx, request)
		if err == nil && !isNilInterface(voice) {
			return voice, nil
		}
		lastErr = err
		if attempt == m.cfg.ReconnectMaxRetries {
			break
		}
		delay := m.cfg.ReconnectRetryBase
		for step := 0; step < attempt && delay < m.cfg.ReconnectRetryMax; step++ {
			if delay > m.cfg.ReconnectRetryMax/2 {
				delay = m.cfg.ReconnectRetryMax
				break
			}
			delay *= 2
		}
		if delay < m.cfg.ReconnectRetryMax {
			delay += m.cfg.Jitter.Duration(delay)
			if delay > m.cfg.ReconnectRetryMax {
				delay = m.cfg.ReconnectRetryMax
			}
		}
		ticker := m.cfg.Clock.NewTicker(delay)
		select {
		case <-ctx.Done():
			ticker.Stop()
			return nil, ctx.Err()
		case <-ticker.C():
			ticker.Stop()
		}
	}
	return nil, lastErr
}

func (m *Manager) runReplacement(sessionCtx context.Context, cancel context.CancelFunc, subject Subject, ids IDs, client ClientConn,
	voice realtime.Session, initialEpoch identity.ResponseEpoch, logical *logicalSession, expected identity.Incarnation, replayAfter int64,
) error {
	group, groupCtx := errgroup.WithContext(sessionCtx)
	quiesced := make(chan struct{})
	var quiescedOnce sync.Once
	markQuiesced := func() { quiescedOnce.Do(func() { close(quiesced) }) }
	state := newRunState(m.cfg, ids, voice, client, cancel)
	state.fence = NewEpochFence(initialEpoch)
	state.boundary = Snapshot{IDs: ids, Response: ResponseIdle, ResponseEpoch: initialEpoch}
	state.subject = cloneSubject(subject)
	state.metrics = m.metricsSnapshot()
	runtime := &sessionRuntime{
		key: runtimeKey{sessionID: ids.SessionID, incarnation: ids.Incarnation}, subject: subject,
		inbox: state.inbox, done: groupCtx.Done(), quiesced: quiesced, state: state,
		accepting: true, inboxSlots: state.inboxSlots,
	}
	state.runtime = runtime
	runtime.replayAfter = &replayAfter
	var firstErr error
	var firstErrOnce sync.Once
	runLoop := func(name string, loop func(context.Context) error) func() error {
		return func() error {
			err := loop(groupCtx)
			runtime.stop()
			if err != nil && !errors.Is(err, errGracefulSessionClose) && !state.graceful.Load() &&
				!(errors.Is(err, context.Canceled) && groupCtx.Err() != nil) {
				firstErrOnce.Do(func() { firstErr = fmt.Errorf("%s: %w", name, err) })
			}
			return err
		}
	}
	group.Go(runLoop("client reader", state.clientReader))
	group.Go(runLoop("voice writer", state.voiceWriter))
	group.Go(runLoop("voice reader", state.voiceReader))
	group.Go(runLoop("client writer", state.clientWriter))
	group.Go(runLoop("reactor", state.reactorLoop))
	abort := func(cause error) error {
		cancel()
		_ = group.Wait()
		state.closeClient(1000, "session ended")
		closeCtx, closeCancel := context.WithTimeout(context.Background(), internalCloseTimeout)
		_ = voice.Close(closeCtx)
		closeCancel()
		markQuiesced()
		if firstErr != nil {
			return firstErr
		}
		return cause
	}
	<-state.inboxSlots
	connected := reactorMessage{input: ConnectedInput{ProviderSession: json.RawMessage(`{}`)}, done: make(chan error, 1), requireClientWrites: true}
	state.acceptControl(&connected)
	state.inbox <- connected
	select {
	case err := <-connected.done:
		if err != nil {
			return abort(ErrRecoveryUnavailable)
		}
	case <-groupCtx.Done():
		return abort(groupCtx.Err())
	}
	if err := m.registerReplacement(runtime, logical, expected); err != nil {
		return abort(err)
	}
	defer m.unregister(runtime.key, runtime)

	waitErr := group.Wait()
	cancel()
	state.closeClient(1000, "session ended")
	closeCtx, closeCancel := context.WithTimeout(context.Background(), internalCloseTimeout)
	closeErr := voice.Close(closeCtx)
	closeCancel()
	markQuiesced()
	if firstErr != nil {
		return firstErr
	}
	cleanupErr := errors.Join(state.clientCloseError(), closeErr)
	if errors.Is(waitErr, errGracefulSessionClose) || state.graceful.Load() {
		return cleanupErr
	}
	if sessionCtx.Err() != nil {
		return sessionCtx.Err()
	}
	if waitErr != nil {
		return waitErr
	}
	return cleanupErr
}

func (m *Manager) registerReplacement(runtime *sessionRuntime, logical *logicalSession, expected identity.Incarnation) error {
	m.activeMu.Lock()
	defer m.activeMu.Unlock()
	if m.logical[runtime.key.sessionID] != logical || !logical.reconnecting || logical.highWater != expected ||
		logical.scope != runtime.handoffScope() || runtime.key.incarnation != expected+1 ||
		logical.state == logicalEstablishing || logical.state == logicalEstablishmentFailed {
		return ErrStaleReconnect
	}
	if _, exists := m.active[runtime.key]; exists {
		return ErrStaleReconnect
	}
	scope := runtime.handoffScope()
	if !isNilInterface(m.cfg.HandoffSubscriptions) {
		replay, ok := m.cfg.HandoffSubscriptions.(HandoffReplayRegistrar)
		if !ok || isNilInterface(replay) {
			return ErrRecoveryUnavailable
		}
		if err := replay.RegisterReplay(runtime.handoffSubscription(m.cfg.HandoffConsumerID), *runtime.replayAfter); err != nil {
			return ErrRecoveryUnavailable
		}
	}
	m.active[runtime.key] = runtime
	m.current[scope] = runtime
	logical.highWater = runtime.key.incarnation
	logical.responseEpoch = runtime.state.fence.Current()
	logical.runtime = runtime
	logical.reconnecting = false
	logical.turnBoundary = true
	logical.state = logicalActive
	return nil
}

func newRunState(cfg Config, ids IDs, voice realtime.Session, client ClientConn, cancel context.CancelFunc) *runState {
	state := &runState{
		cfg:             cfg,
		ids:             ids,
		voice:           voice,
		client:          client,
		fence:           NewEpochFence(0),
		inbox:           make(chan reactorMessage, cfg.ControlEvents),
		ingress:         make(chan ingressFrame, cfg.IngressFrames),
		ingressSlots:    make(chan struct{}, cfg.IngressFrames),
		voiceCtl:        make(chan voiceControl, cfg.WriterControlEvents),
		egress:          make(chan egressFrame, cfg.EgressFrames),
		clientCtl:       make(chan clientControl, cfg.WriterControlEvents),
		inboxSlots:      make(chan struct{}, cfg.ControlEvents),
		voiceSlots:      make(chan struct{}, cfg.WriterControlEvents),
		clientSlots:     make(chan struct{}, cfg.WriterControlEvents),
		egressSlots:     make(chan struct{}, cfg.EgressFrames),
		progress:        newSequenceProgress(),
		activeWrite:     &activeAudioWrite{},
		closeClientDone: make(chan struct{}),
		metrics:         normalizeMetricsHooks(MetricsHooks{}),
		dispatcher:      cfg.DurableDispatcher,
		cancel:          cancel,
		boundary:        Snapshot{IDs: ids, Response: ResponseIdle},
	}
	for range cfg.ControlEvents {
		state.inboxSlots <- struct{}{}
	}
	for range cfg.IngressFrames {
		state.ingressSlots <- struct{}{}
	}
	for range cfg.WriterControlEvents {
		state.voiceSlots <- struct{}{}
		state.clientSlots <- struct{}{}
	}
	for range cfg.EgressFrames {
		state.egressSlots <- struct{}{}
	}
	return state
}

func (m *Manager) Submit(ctx context.Context, sessionID identity.SessionID, incarnation identity.Incarnation, input Input) error {
	if m == nil {
		return ErrSessionNotActive
	}
	if ctx == nil {
		return errors.New("submit context is nil")
	}
	if input == nil {
		return errors.New("session input is nil")
	}
	key := runtimeKey{sessionID: sessionID, incarnation: incarnation}
	m.activeMu.RLock()
	runtime := m.active[key]
	m.activeMu.RUnlock()
	if runtime == nil {
		return ErrSessionNotActive
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-runtime.done:
		return ErrSessionNotActive
	default:
	}
	select {
	case <-runtime.inboxSlots:
	case <-runtime.done:
		return ErrSessionNotActive
	case <-ctx.Done():
		return ctx.Err()
	}
	message := reactorMessage{input: input, ingressAfter: runtime.state.latestIngressSequence(), done: make(chan error, 1)}
	runtime.mu.Lock()
	if err := ctx.Err(); err != nil {
		runtime.mu.Unlock()
		runtime.inboxSlots <- struct{}{}
		return err
	}
	if !runtime.accepting {
		runtime.mu.Unlock()
		runtime.inboxSlots <- struct{}{}
		return ErrSessionNotActive
	}
	select {
	case <-runtime.done:
		runtime.mu.Unlock()
		runtime.inboxSlots <- struct{}{}
		return ErrSessionNotActive
	default:
	}
	runtime.state.acceptControl(&message)
	runtime.inbox <- message
	runtime.mu.Unlock()
	select {
	case err := <-message.done:
		return err
	default:
	}
	select {
	case err := <-message.done:
		return err
	case <-runtime.done:
		select {
		case err := <-message.done:
			return err
		default:
			return ErrSessionNotActive
		}
	case <-ctx.Done():
		select {
		case err := <-message.done:
			return err
		default:
			return ctx.Err()
		}
	}
}

func (m *Manager) register(runtime *sessionRuntime) error {
	m.activeMu.Lock()
	defer m.activeMu.Unlock()
	if _, exists := m.active[runtime.key]; exists {
		return ErrSessionAlreadyActive
	}
	scope := runtime.handoffScope()
	if m.logical == nil {
		m.logical = make(map[identity.SessionID]*logicalSession)
	}
	logical := m.logical[runtime.key.sessionID]
	if logical == nil {
		// register is also used by focused package tests that construct a
		// runtime directly. Treat that as an initial publication, never as a
		// reconnect or a reset of an existing high-water record.
		logical = &logicalSession{
			scope: scope, subject: cloneSubject(runtime.subject), highWater: runtime.key.incarnation,
			state: logicalEstablishing,
		}
		m.logical[runtime.key.sessionID] = logical
	}
	if logical.scope != scope || logical.highWater != runtime.key.incarnation || logical.state != logicalEstablishing ||
		logical.runtime != nil || logical.reconnecting {
		return ErrStaleReconnect
	}
	if !isNilInterface(m.cfg.HandoffSubscriptions) {
		if err := m.cfg.HandoffSubscriptions.Register(runtime.handoffSubscription(m.cfg.HandoffConsumerID)); err != nil {
			return fmt.Errorf("register handoff presentation: %w", err)
		}
	}
	if m.current == nil {
		m.current = make(map[handoff.Scope]*sessionRuntime)
	}
	m.active[runtime.key] = runtime
	m.current[scope] = runtime
	logical.runtime = runtime
	logical.responseEpoch = runtime.state.fence.Current()
	logical.reconnecting = false
	logical.turnBoundary = true
	logical.state = logicalActive
	return nil
}

func (m *Manager) unregister(key runtimeKey, runtime *sessionRuntime) {
	m.activeMu.Lock()
	if m.active[key] == runtime {
		delete(m.active, key)
	}
	scope := runtime.handoffScope()
	unregisterPresentation := m.current[scope] == runtime
	if unregisterPresentation {
		delete(m.current, scope)
	}
	if logical := m.logical[key.sessionID]; logical != nil && logical.runtime == runtime {
		logical.runtime = nil
		logical.responseEpoch = runtime.state.fence.Current()
		logical.turnBoundary = runtime.atTurnBoundary()
		logical.state = logicalTombstoned
	}
	m.activeMu.Unlock()
	if unregisterPresentation && !isNilInterface(m.cfg.HandoffSubscriptions) {
		m.cfg.HandoffSubscriptions.Unregister(runtime.handoffSubscription(m.cfg.HandoffConsumerID))
	}
}

func (r *sessionRuntime) handoffScope() handoff.Scope {
	if r == nil || r.state == nil {
		return handoff.Scope{}
	}
	return handoff.Scope{TenantID: r.subject.TenantID, UserID: r.subject.UserID, ConversationID: r.state.ids.ConversationID}
}

// OfferHandoffEvent atomically resolves and submits to the current runtime for
// the full trusted scope. It deliberately stores neither a session ID nor an
// incarnation between offers.
func (m *Manager) OfferHandoffEvent(ctx context.Context, scope handoff.Scope, event handoff.Event) error {
	if m == nil {
		return handoff.ErrNoLiveSession
	}
	if ctx == nil {
		return errors.New("handoff offer context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for name, value := range map[string]string{
		"tenant ID": scope.TenantID, "user ID": scope.UserID, "conversation ID": string(scope.ConversationID),
	} {
		if strings.TrimSpace(value) == "" || len(value) > DurableIdentifierBytes || !utf8.ValidString(value) {
			return fmt.Errorf("handoff offer %s is invalid", name)
		}
	}

	timeout := m.cfg.EgressBlockTimeout
	if timeout <= 0 {
		timeout = presentationOfferTimeout
	}
	offerCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	m.activeMu.RLock()
	runtime := m.current[scope]
	if runtime == nil {
		m.activeMu.RUnlock()
		return handoff.ErrNoLiveSession
	}
	defer m.activeMu.RUnlock()
	select {
	case <-runtime.inboxSlots:
	case <-runtime.done:
		return handoff.ErrNoLiveSession
	case <-offerCtx.Done():
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return ErrControlOverflow
	}
	message := reactorMessage{
		input:        HandoffEventInput{Scope: scope, Event: handoff.CloneEvent(event)},
		ingressAfter: runtime.state.latestIngressSequence(), done: make(chan error, 1),
		offerCtx: offerCtx,
	}
	runtime.mu.Lock()
	if !runtime.accepting {
		runtime.mu.Unlock()
		runtime.inboxSlots <- struct{}{}
		return handoff.ErrNoLiveSession
	}
	select {
	case <-runtime.done:
		runtime.mu.Unlock()
		runtime.inboxSlots <- struct{}{}
		return handoff.ErrNoLiveSession
	default:
	}
	runtime.state.acceptControl(&message)
	runtime.inbox <- message
	runtime.mu.Unlock()
	select {
	case err := <-message.done:
		if err != nil && offerCtx.Err() != nil {
			m.stopAndQuiesceRuntime(runtime)
		}
		return err
	case <-runtime.done:
		m.stopAndQuiesceRuntime(runtime)
		select {
		case err := <-message.done:
			return err
		default:
			return handoff.ErrNoLiveSession
		}
	case <-offerCtx.Done():
		select {
		case err := <-message.done:
			if err != nil {
				m.stopAndQuiesceRuntime(runtime)
			}
			return err
		default:
			m.stopAndQuiesceRuntime(runtime)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return ErrControlOverflow
		}
	}
}

func (m *Manager) stopAndQuiesceRuntime(runtime *sessionRuntime) {
	_ = m.stopAndQuiesceRuntimeReason(context.Background(), runtime, 1011, "handoff presentation delivery timed out")
}

func (m *Manager) stopAndQuiesceRuntimeReason(parent context.Context, runtime *sessionRuntime, code int, reason string) error {
	if runtime == nil {
		return nil
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(parent, internalCloseTimeout)
	defer cleanupCancel()
	runtime.stop()
	if runtime.state != nil {
		if runtime.state.cancel != nil {
			runtime.state.cancel()
		}
		if !isNilInterface(runtime.state.client) {
			runtime.state.closeClientContext(cleanupCtx, code, reason)
		}
	}
	if runtime.quiesced != nil {
		select {
		case <-runtime.quiesced:
			return nil
		case <-cleanupCtx.Done():
			return cleanupCtx.Err()
		}
	}
	return nil
}

func (m *Manager) closeUnpublishedClient(parent context.Context, client ClientConn, code int, reason string) {
	if isNilInterface(client) {
		return
	}
	ctx, cancel := context.WithTimeout(parent, internalCloseTimeout)
	defer cancel()
	_ = client.Close(ctx, code, reason)
}

func (s *runState) clientReader(ctx context.Context) error {
	framer := protocol.NewPCMFramer()
	var latestSequence uint64
	for {
		event, err := s.client.Read(ctx)
		if err != nil {
			return err
		}
		if s.runtime == nil {
			return ErrSessionNotActive
		}
		err = s.runtime.acceptClientEvent(ctx, func() error {
			switch typed := event.(type) {
			case protocol.InputAudioBufferAppendEvent:
				pcm, decodeErr := protocol.DecodePCM24k(typed, s.cfg.IngressFrames*pcmFrameBytes)
				if decodeErr != nil {
					return decodeErr
				}
				if len(pcm) != 0 {
					s.partialInput.Store(true)
				}
				frames, pushErr := framer.Push(pcm)
				if pushErr != nil {
					return pushErr
				}
				for _, audio := range frames {
					latestSequence = s.nextIngressSequence()
					if enqueueErr := s.enqueueIngress(ctx, ingressFrame{sequence: latestSequence, audio: append([]byte(nil), audio...)}); enqueueErr != nil {
						return enqueueErr
					}
				}
			case protocol.InputAudioBufferCommitEvent:
				tail, flushErr := framer.Flush()
				if flushErr != nil {
					return s.enqueueClientControl(clientControl{event: protocol.ErrorEvent{
						Type:    "error",
						EventID: s.nextManagerEventID(),
						Error: protocol.ProtocolError{
							Type: "invalid_request_error", Code: "invalid_audio_commit", Message: flushErr.Error(),
						},
					}})
				}
				if len(tail) != 0 {
					latestSequence = s.nextIngressSequence()
					if enqueueErr := s.enqueueIngress(ctx, ingressFrame{sequence: latestSequence, audio: append([]byte(nil), tail...)}); enqueueErr != nil {
						return enqueueErr
					}
				}
				if enqueueErr := s.enqueueReactor(reactorMessage{input: ClientInput{Event: typed}, ingressAfter: latestSequence}); enqueueErr != nil {
					return enqueueErr
				}
				s.partialInput.Store(false)
			default:
				return s.enqueueReactor(reactorMessage{input: ClientInput{Event: event}, ingressAfter: latestSequence})
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
}

func (r *sessionRuntime) acceptClientEvent(ctx context.Context, accept func() error) error {
	r.mu.Lock()
	if err := ctx.Err(); err != nil {
		r.mu.Unlock()
		return err
	}
	if !r.accepting {
		r.mu.Unlock()
		return ErrSessionNotActive
	}
	select {
	case <-r.done:
		r.mu.Unlock()
		return ErrSessionNotActive
	default:
	}
	sequence := r.state.externalAccepted.Add(1)
	r.mu.Unlock()
	if err := accept(); err != nil {
		return err
	}
	r.state.externalApplied.Store(sequence)
	return nil
}

func (s *runState) enqueueIngress(ctx context.Context, frame ingressFrame) error {
	timer := time.NewTimer(s.cfg.IngressBlockTimeout)
	defer timer.Stop()
	select {
	case <-s.ingressSlots:
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		s.metrics.queueOverflow("ingress")
		return ErrIngressBackpressure
	}
	s.ingress <- frame
	return nil
}

func (s *runState) enqueueReactor(message reactorMessage) error {
	select {
	case <-s.inboxSlots:
	default:
		s.metrics.queueOverflow("reactor")
		return ErrControlOverflow
	}
	s.acceptControl(&message)
	s.inbox <- message
	return nil
}

func (s *runState) acceptControl(message *reactorMessage) {
	message.controlSequence = s.controlAccepted.Add(1)
}

func (s *runState) enqueueReactorSync(ctx context.Context, message reactorMessage) error {
	message.done = make(chan error, 1)
	if err := s.enqueueReactor(message); err != nil {
		return err
	}
	select {
	case err := <-message.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *runState) voiceWriter(ctx context.Context) error {
	var completedMedia uint64
	pending := make([]voiceControl, 0, s.cfg.WriterControlEvents)
	for {
		for {
			select {
			case control := <-s.voiceCtl:
				pending = append(pending, control)
			default:
				goto controlsDrained
			}
		}
	controlsDrained:
		selected := -1
		if len(pending) != 0 && pending[0].command.AfterSeq <= completedMedia {
			selected = 0
		} else {
			for index := 1; index < len(pending); index++ {
				if pending[index].command.Kind == realtime.CommandCancelResponse {
					selected = index
					break
				}
			}
		}
		if selected >= 0 {
			control := pending[selected]
			pending = append(pending[:selected], pending[selected+1:]...)
			err := s.voice.Send(ctx, control.command)
			s.releaseVoiceSlot()
			if err != nil {
				return err
			}
			if err := s.enqueueReactor(reactorMessage{input: VoiceCommandCompletedInput{
				Sequence: control.command.Sequence, Kind: control.command.Kind,
			}}); err != nil {
				return err
			}
			continue
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case control := <-s.voiceCtl:
			pending = append(pending, control)
		case frame := <-s.ingress:
			command := realtime.Command{Kind: realtime.CommandAppendAudio, Sequence: frame.sequence, Audio: append([]byte(nil), frame.audio...)}
			err := s.voice.Send(ctx, command)
			s.releaseIngressSlot()
			if err != nil {
				return err
			}
			completedMedia = frame.sequence
			s.ingressCompleted.Store(completedMedia)
		}
	}
}

func (s *runState) voiceReader(ctx context.Context) error {
	framer := &outputPCMFramer{}
	var streamResponse string
	var streamItem string
	var streamEpoch identity.ResponseEpoch
	streamHasAudio := false
	for {
		event, err := s.voice.Receive(ctx)
		if err != nil {
			return err
		}
		if streamHasAudio && !s.fence.Accept(streamEpoch) {
			framer.reset()
			streamHasAudio = false
			streamResponse, streamItem, streamEpoch = "", "", 0
		}
		if event.Kind == realtime.EventAudioDelta {
			if event.ResponseID == "" || event.ItemID == "" || event.Epoch == 0 {
				return errors.New("provider audio requires response ID, item ID, and epoch")
			}
			if streamHasAudio && (streamResponse != event.ResponseID || streamItem != event.ItemID || streamEpoch != event.Epoch) {
				return errors.New("provider audio response changed before audio.done")
			}
			if !s.fence.Accept(event.Epoch) {
				continue
			}
			limit := s.cfg.EgressFrames * pcmFrameBytes
			if len(event.Audio) > limit {
				return fmt.Errorf("provider audio event exceeds %d bytes: got %d", limit, len(event.Audio))
			}
			streamHasAudio = true
			streamResponse, streamItem, streamEpoch = event.ResponseID, event.ItemID, event.Epoch
			err := framer.push(event.Audio, func(audio []byte) error {
				accepted, enqueueErr := s.enqueueEgress(ctx, event, audio)
				if enqueueErr != nil {
					return enqueueErr
				}
				if !accepted {
					return errEgressInvalidated
				}
				return nil
			})
			if errors.Is(err, errEgressInvalidated) {
				framer.reset()
				streamHasAudio = false
				streamResponse, streamItem, streamEpoch = "", "", 0
				continue
			}
			if err != nil {
				return err
			}
			continue
		}

		if event.Kind == realtime.EventAudioDone && streamHasAudio {
			if event.ResponseID != streamResponse || event.ItemID != streamItem || event.Epoch != streamEpoch {
				return errors.New("provider audio.done does not match active audio")
			}
			tail, err := framer.flush()
			if err != nil {
				return err
			}
			if len(tail) != 0 {
				_, err := s.enqueueEgress(ctx, event, tail)
				if err != nil {
					return err
				}
			}
			streamHasAudio = false
			streamResponse, streamItem, streamEpoch = "", "", 0
		}

		latest := s.latestEgressSequence()
		if err := s.enqueueReactorSync(ctx, reactorMessage{
			input: VoiceInput{Incarnation: s.ids.Incarnation, Event: event}, egressAfter: latest,
		}); err != nil {
			return err
		}
	}
}

func (s *runState) enqueueEgress(ctx context.Context, event realtime.Event, audio []byte) (bool, error) {
	sequence := s.nextEgressSequence()
	if !s.fence.Accept(event.Epoch) {
		s.progress.mark(sequence)
		s.metrics.audioDropped(len(audio))
		return false, nil
	}
	timer := time.NewTimer(s.cfg.EgressBlockTimeout)
	defer timer.Stop()
	select {
	case <-s.egressSlots:
	case <-ctx.Done():
		s.progress.mark(sequence)
		return false, ctx.Err()
	case <-timer.C:
		s.progress.mark(sequence)
		s.metrics.queueOverflow("egress")
		return false, ErrEgressBackpressure
	}
	if !s.fence.Accept(event.Epoch) {
		s.egressSlots <- struct{}{}
		s.progress.mark(sequence)
		s.metrics.audioDropped(len(audio))
		return false, nil
	}
	s.outputMu.Lock()
	s.outputOrder++
	frame := egressFrame{
		order: s.outputOrder, sequence: sequence, incarnation: s.ids.Incarnation, epoch: event.Epoch,
		eventID:    fmt.Sprintf("session-%s-audio-%06d", s.ids.SessionID, sequence),
		responseID: event.ResponseID, itemID: event.ItemID, audio: append([]byte(nil), audio...),
	}
	s.egress <- frame
	s.outputMu.Unlock()
	return true, nil
}

func (s *runState) reactorLoop(ctx context.Context) error {
	reactor := NewReactorWithPresentation(s.ids, s.fence, s.cfg.PresentationPolicy, s.cfg.SafetyPolicy)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case message := <-s.inbox:
			if s.cfg.reactorApplyGate != nil {
				if gate := s.cfg.reactorApplyGate(message.input); gate != nil {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-gate:
					}
				}
			}
			dispatchCtx, finishDispatch := offerDispatchContext(ctx, message.offerCtx)
			effects, err := reactor.ApplyContext(dispatchCtx, message.input)
			if err == nil {
				if message.requireClientWrites {
					for index := range effects {
						if _, ok := effects[index].ClientEvent.(protocol.SessionCreatedEvent); ok {
							effects[index].RequireClientWrite = true
						}
					}
				}
				err = s.dispatchEffects(dispatchCtx, effects, message.ingressAfter, message.egressAfter)
			}
			if err == nil {
				s.boundaryMu.Lock()
				s.boundary = reactor.Snapshot()
				s.boundaryMu.Unlock()
				if _, ok := message.input.(VoiceCommandCompletedInput); ok {
					s.pendingVoice.Add(-1)
				}
			}
			finishDispatch()
			s.controlApplied.Store(message.controlSequence)
			s.inboxSlots <- struct{}{}
			if message.done != nil {
				message.done <- err
			}
			if err != nil {
				return err
			}
		}
	}
}

func offerDispatchContext(sessionCtx, offerCtx context.Context) (context.Context, func()) {
	if offerCtx == nil {
		return sessionCtx, func() {}
	}
	ctx, cancel := context.WithCancel(offerCtx)
	stop := context.AfterFunc(sessionCtx, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}

func (s *runState) dispatchEffects(ctx context.Context, effects []Effect, ingressAfter, egressAfter uint64) error {
	for _, effect := range effects {
		if effect.FlushBefore != 0 {
			if err := s.activeWrite.cancelAndWait(); err != nil {
				return err
			}
		}
	}
	writeAcks := make([]<-chan error, 0, len(effects))
	for _, effect := range effects {
		if err := ctx.Err(); err != nil {
			return err
		}
		if effect.ClientEvent != nil {
			control := clientControl{afterSeq: egressAfter, event: effect.ClientEvent, writeCtx: ctx}
			if effect.RequireClientWrite {
				control.written = make(chan error, 1)
				writeAcks = append(writeAcks, control.written)
			}
			if err := s.enqueueClientControl(control); err != nil {
				return err
			}
		}
		if effect.Close != nil {
			closeCopy := *effect.Close
			if err := s.enqueueClientControl(clientControl{afterSeq: egressAfter, close: &closeCopy}); err != nil {
				return err
			}
		}
	}
	for _, written := range writeAcks {
		select {
		case err := <-written:
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for _, effect := range effects {
		if err := ctx.Err(); err != nil {
			return err
		}
		if effect.VoiceCommand == nil {
			continue
		}
		command := *effect.VoiceCommand
		if command.Kind != realtime.CommandCancelResponse && command.Kind != realtime.CommandBindResponseEpoch {
			command.AfterSeq = ingressAfter
		}
		select {
		case <-s.voiceSlots:
		default:
			s.metrics.queueOverflow("voice_control")
			return ErrControlOverflow
		}
		s.pendingVoice.Add(1)
		s.voiceCtl <- voiceControl{command: command}
	}
	for _, effect := range effects {
		if effect.Transcript != nil || effect.Handoff != nil {
			if isNilInterface(s.dispatcher) {
				return fmt.Errorf("%w: durable typed effect", ErrUnsupportedAsync)
			}
			trusted := TrustedSessionContext{IDs: s.ids, Subject: cloneSubject(s.subject)}
			if effect.Transcript != nil {
				owned := *effect.Transcript
				if err := s.dispatcher.DispatchTranscript(ctx, trusted, owned); err != nil {
					return err
				}
			}
			if effect.Handoff != nil {
				owned := *effect.Handoff
				owned.Arguments = append(json.RawMessage(nil), effect.Handoff.Arguments...)
				if err := s.dispatcher.DispatchHandoff(ctx, trusted, owned); err != nil {
					return err
				}
			}
		}
		if effect.Async != nil && effect.Handoff == nil {
			return fmt.Errorf("%w: %s", ErrUnsupportedAsync, effect.Async.Kind)
		}
	}
	return nil
}

func (s *runState) enqueueClientControl(control clientControl) error {
	select {
	case <-s.clientSlots:
	default:
		s.metrics.queueOverflow("client_control")
		return ErrControlOverflow
	}
	s.outputMu.Lock()
	control.order = s.outputOrder + 1
	select {
	case s.clientCtl <- control:
		s.outputOrder++
		s.outputMu.Unlock()
		return nil
	default:
		s.outputMu.Unlock()
		s.clientSlots <- struct{}{}
		s.metrics.queueOverflow("client_control")
		return ErrControlOverflow
	}
}

func (s *runState) clientWriter(ctx context.Context) error {
	defer s.closeClient(1000, "session ended")
	var pendingControl *clientControl
	var pendingFrame *egressFrame
	nextOrder := uint64(1)
	for {
		if pendingControl != nil && pendingControl.order == nextOrder && s.progress.reached(pendingControl.afterSeq) {
			control := *pendingControl
			pendingControl = nil
			nextOrder++
			if control.event != nil {
				writeCtx := ctx
				if control.writeCtx != nil {
					writeCtx = control.writeCtx
				}
				if err := writeCtx.Err(); err != nil {
					if control.written != nil {
						control.written <- err
					}
					s.releaseClientSlot()
					return err
				}
				err := s.client.Write(writeCtx, control.event)
				if control.written != nil {
					control.written <- err
				}
				if err != nil {
					s.releaseClientSlot()
					return err
				}
			}
			if control.close != nil {
				s.graceful.Store(true)
				s.closeClient(control.close.Code, control.close.Reason)
				s.releaseClientSlot()
				s.cancel()
				return errGracefulSessionClose
			}
			s.releaseClientSlot()
			continue
		}
		if pendingFrame != nil && pendingFrame.order == nextOrder {
			frame := *pendingFrame
			pendingFrame = nil
			nextOrder++
			if frame.incarnation != s.ids.Incarnation || !s.fence.Accept(frame.epoch) {
				s.progress.mark(frame.sequence)
				s.metrics.audioDropped(len(frame.audio))
				s.releaseEgressSlot()
				continue
			}
			writeCtx, finish := s.activeWrite.begin(ctx)
			if frame.incarnation != s.ids.Incarnation || !s.fence.Accept(frame.epoch) {
				finish()
				s.progress.mark(frame.sequence)
				s.metrics.audioDropped(len(frame.audio))
				s.releaseEgressSlot()
				continue
			}
			event := protocol.ResponseOutputAudioDeltaEvent{
				Type: "response.output_audio.delta", EventID: frame.eventID,
				ResponseID: frame.responseID, ItemID: frame.itemID,
				Delta: base64.StdEncoding.EncodeToString(frame.audio),
			}
			err := s.client.Write(writeCtx, event)
			finish()
			s.progress.mark(frame.sequence)
			s.releaseEgressSlot()
			if err != nil {
				if errors.Is(err, context.Canceled) && !s.fence.Accept(frame.epoch) {
					continue
				}
				return err
			}
			continue
		}

		var controls <-chan clientControl
		if pendingControl == nil {
			controls = s.clientCtl
		}
		var frames <-chan egressFrame
		if pendingFrame == nil {
			frames = s.egress
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case control := <-controls:
			pendingControl = &control
		case frame := <-frames:
			pendingFrame = &frame
		case <-s.progress.changed:
		}
	}
}

func (s *runState) releaseVoiceSlot() {
	if s.voiceSlots != nil {
		s.voiceSlots <- struct{}{}
	}
}

func (s *runState) releaseIngressSlot() {
	if s.ingressSlots != nil {
		s.ingressSlots <- struct{}{}
	}
}

func (s *runState) releaseClientSlot() {
	if s.clientSlots != nil {
		s.clientSlots <- struct{}{}
	}
}

func (s *runState) releaseEgressSlot() {
	if s.egressSlots != nil {
		s.egressSlots <- struct{}{}
	}
}

func (s *runState) closeClient(code int, reason string) {
	ctx, cancel := context.WithTimeout(context.Background(), internalCloseTimeout)
	defer cancel()
	s.closeClientContext(ctx, code, reason)
}

func (s *runState) closeClientContext(ctx context.Context, code int, reason string) {
	s.closeClientOnce.Do(func() {
		if s.closeClientDone == nil {
			s.closeClientDone = make(chan struct{})
		}
		go func() {
			err := s.client.Close(ctx, code, reason)
			s.closeClientMu.Lock()
			s.closeClientErr = err
			s.closeClientMu.Unlock()
			close(s.closeClientDone)
		}()
	})
	select {
	case <-s.closeClientDone:
	case <-ctx.Done():
	}
}

func (s *runState) clientCloseError() error {
	s.closeClientMu.Lock()
	defer s.closeClientMu.Unlock()
	return s.closeClientErr
}

func (s *runState) nextIngressSequence() uint64 {
	s.sequenceMu.Lock()
	defer s.sequenceMu.Unlock()
	s.ingressSequence++
	return s.ingressSequence
}

func (s *runState) nextEgressSequence() uint64 {
	s.sequenceMu.Lock()
	defer s.sequenceMu.Unlock()
	s.egressSequence++
	return s.egressSequence
}

func (s *runState) latestIngressSequence() uint64 {
	s.sequenceMu.Lock()
	defer s.sequenceMu.Unlock()
	return s.ingressSequence
}

func (s *runState) latestEgressSequence() uint64 {
	s.sequenceMu.Lock()
	defer s.sequenceMu.Unlock()
	return s.egressSequence
}

func (s *runState) nextManagerEventID() string {
	s.sequenceMu.Lock()
	defer s.sequenceMu.Unlock()
	s.eventSequence++
	return fmt.Sprintf("session-%s-manager-%06d", s.ids.SessionID, s.eventSequence)
}

func validateSubject(subject Subject) error {
	if strings.TrimSpace(subject.TenantID) == "" {
		return errors.New("subject tenant ID is required")
	}
	if strings.TrimSpace(subject.UserID) == "" {
		return errors.New("subject user ID is required")
	}
	if !subject.ExpiresAt.IsZero() && !subject.ExpiresAt.After(time.Now()) {
		return errors.New("subject is expired")
	}
	return nil
}

func subjectContext(parent context.Context, expiresAt time.Time) (context.Context, context.CancelFunc) {
	if expiresAt.IsZero() {
		return context.WithCancel(parent)
	}
	return context.WithDeadline(parent, expiresAt)
}

func cloneSubject(subject Subject) Subject {
	copy := subject
	if subject.Scopes != nil {
		copy.Scopes = make(map[string]struct{}, len(subject.Scopes))
		for scope := range subject.Scopes {
			copy.Scopes[scope] = struct{}{}
		}
	}
	copy.Permissions = append(json.RawMessage(nil), subject.Permissions...)
	return copy
}

func isNilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
