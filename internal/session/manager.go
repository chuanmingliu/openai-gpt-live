package session

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	rand "math/rand/v2"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/feature"
	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/presentation"
	"github.com/chuanmingliu/openai-gpt-live/internal/protocol"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/chuanmingliu/openai-gpt-live/internal/safety"
	"github.com/chuanmingliu/openai-gpt-live/internal/toolpolicy"
	"golang.org/x/sync/errgroup"
)

const (
	defaultIngressFrames          = 25
	defaultEgressFrames           = 75
	defaultControlEvents          = 256
	defaultWriterControlEvents    = 64
	maximumQueueCapacity          = 10_000
	pcmFrameBytes                 = 960
	internalCloseTimeout          = time.Second
	presentationOfferTimeout      = 250 * time.Millisecond
	defaultReconnectRetries       = 2
	defaultReconnectRetryBase     = 100 * time.Millisecond
	defaultReconnectRetryMax      = time.Second
	defaultReconnectTombstoneTTL  = 10 * time.Minute
	defaultMaxReconnectTombstones = 4_096
	defaultReconnectCredentialTTL = 5 * time.Minute
	defaultReconnectOwnerLease    = 30 * time.Second
	defaultReconnectHeartbeat     = 10 * time.Second
	defaultReconnectOperation     = time.Second
	defaultReconnectMaxOperations = 128
	defaultReconnectCleanup       = 30 * time.Second
	defaultReconnectCleanupBatch  = 128
	maximumLogicalSessions        = 10_000
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
	IngressFrames                   int
	EgressFrames                    int
	ControlEvents                   int
	WriterControlEvents             int
	IngressBlockTimeout             time.Duration
	EgressBlockTimeout              time.Duration
	DurableDispatcher               DurableEffectDispatcher
	HandoffConsumerID               string
	HandoffSubscriptions            HandoffSubscriptionRegistrar
	PresentationPolicy              presentation.Policy
	SafetyPolicy                    safety.Policy
	RecoveryStore                   RecoveryStore
	RecoveryItemLimit               int
	RecoveryByteLimit               int
	ReconnectMaxRetries             int
	ReconnectRetryBase              time.Duration
	ReconnectRetryMax               time.Duration
	ReconnectTombstoneTTL           time.Duration
	MaxReconnectTombstones          int
	ReconnectCredentials            ReconnectCredentials
	ReconnectOwners                 ReconnectOwnerStore
	ReconnectOwnerID                string
	ReconnectCredentialTTL          time.Duration
	ReconnectCredentialOverlap      time.Duration
	ReconnectOwnerLeaseDuration     time.Duration
	ReconnectOwnerHeartbeatInterval time.Duration
	ReconnectOwnerOperationTimeout  time.Duration
	ReconnectOwnerMaxOperations     int
	ReconnectCleanupInterval        time.Duration
	ReconnectCleanupBatch           int
	InitialSession                  realtime.SessionConfig
	ToolCatalog                     toolpolicy.Catalog
	AsyncModel                      string
	HandoffToolsEnabled             bool
	Clock                           clock.Clock
	Jitter                          clock.JitterSource
	FeatureGates                    interface{ Enabled(string) bool }
	ApprovalStore                   handoff.ApprovalStore
	ApprovalDispatcher              ApprovalResolutionDispatcher
	ConversationProvisioner         ConversationProvisioner
	ConversationTTL                 time.Duration
	reactorApplyGate                func(Input) <-chan struct{}
	reconnectBoundaryGate           func() <-chan struct{}
	initialRegisterGate             func() <-chan struct{}
	reconnectHeartbeatReady         func()
}

type ConversationProvisioner interface {
	ProvisionConversation(context.Context, handoff.Scope, time.Duration) error
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

type ApprovalResolutionRequestedEffect struct {
	ExternalSequence uint64
	InvocationID     string
	Decision         handoff.ApprovalDecision
	Reason           string
	IdempotencyKey   string
	ResolvedAt       time.Time
}

type ApprovalResolutionDispatcher interface {
	DispatchApproval(TrustedSessionContext, ApprovalResolutionRequestedEffect) error
}

type MetricsHooks struct {
	QueueOverflow       func(string)
	AudioDropped        func(int)
	SessionStarted      func()
	SessionEnded        func()
	AudioTransit        func(time.Duration)
	ReconnectRestore    func(time.Duration, string)
	ReconnectTombstones func(int)
	StaleOutputRejected func()
	QueueState          func(string, int, int, time.Duration)
	QueueBlocked        func(string, time.Duration)
	QueueOldest         func(string, time.Duration)
	BargeinSuppression  func(time.Duration)
}

func normalizeMetricsHooks(hooks MetricsHooks) MetricsHooks {
	if hooks.QueueOverflow == nil {
		hooks.QueueOverflow = func(string) {}
	}
	if hooks.AudioDropped == nil {
		hooks.AudioDropped = func(int) {}
	}
	if hooks.SessionStarted == nil {
		hooks.SessionStarted = func() {}
	}
	if hooks.SessionEnded == nil {
		hooks.SessionEnded = func() {}
	}
	if hooks.AudioTransit == nil {
		hooks.AudioTransit = func(time.Duration) {}
	}
	if hooks.ReconnectRestore == nil {
		hooks.ReconnectRestore = func(time.Duration, string) {}
	}
	if hooks.ReconnectTombstones == nil {
		hooks.ReconnectTombstones = func(int) {}
	}
	if hooks.StaleOutputRejected == nil {
		hooks.StaleOutputRejected = func() {}
	}
	if hooks.QueueState == nil {
		hooks.QueueState = func(string, int, int, time.Duration) {}
	}
	if hooks.QueueBlocked == nil {
		hooks.QueueBlocked = func(string, time.Duration) {}
	}
	if hooks.QueueOldest == nil {
		hooks.QueueOldest = func(string, time.Duration) {}
	}
	if hooks.BargeinSuppression == nil {
		hooks.BargeinSuppression = func(time.Duration) {}
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
	// Ephemeral is assigned only by the authenticated admission boundary for
	// the exact configured synthetic identity. It is never client-controlled.
	Ephemeral bool
}

type OpenRequest struct {
	Subject        Subject
	RequestedModel string
	ConversationID identity.ConversationID
}

type IDGenerator interface {
	NewSessionID() identity.SessionID
	NewConversationID() identity.ConversationID
}

type Manager struct {
	cfg       Config
	connector realtime.Connector
	ids       IDGenerator

	validationErr                 error
	activeMu                      sync.RWMutex
	active                        map[runtimeKey]*sessionRuntime
	current                       map[handoff.Scope]*sessionRuntime
	logical                       map[identity.SessionID]*logicalSession
	metricsMu                     sync.RWMutex
	metrics                       MetricsHooks
	reconnectMaintenanceMu        sync.RWMutex
	reconnectMaintenanceAttempted bool
	reconnectMaintenanceErr       error
	reconnectMaintenanceSucceeded time.Time
	reconnectOperationSlots       chan struct{}
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
	tombstonedAt  time.Time
	state         logicalSessionState
}

type logicalSessionState uint8

const (
	logicalEstablishing logicalSessionState = iota
	logicalActive
	logicalTombstoned
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

	mu                    sync.Mutex
	accepting             bool
	inboxSlots            chan struct{}
	replayAfter           *int64
	reconnectMu           sync.Mutex
	reconnectRotateOnce   sync.Once
	reconnectRotate       chan struct{}
	presentationMu        sync.Mutex
	acceptanceGeneration  uint64
	publicationPrevious   *sessionRuntime
	publicationAccepting  bool
	publicationGeneration uint64
	reconnect             *IssuedReconnectCredential
	reconnectDelivered    bool
	reconnectLossDeadline time.Time
}

func (r *sessionRuntime) commitPublication() {
	if r == nil {
		return
	}
	r.publicationPrevious = nil
	r.publicationAccepting = false
	r.publicationGeneration = 0
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
	input                Input
	ingressAfter         uint64
	egressAfter          uint64
	done                 chan error
	requireClientWrites  bool
	offerCtx             context.Context
	controlSequence      uint64
	completion           bool
	acceptanceChecked    bool
	acceptanceGeneration uint64
}

type ingressFrame struct {
	sequence   uint64
	audio      []byte
	enqueuedAt time.Time
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
	enqueuedAt  time.Time
}

type voiceControl struct {
	command             realtime.Command
	sent                chan error
	barrier             *voiceSendBarrier
	handoffPresentation bool
}

// voiceSendBarrier lets client-writer work depend on an exact provider write
// without making the sole reactor wait for either writer. Closing done
// broadcasts the result to every later client effect in the same composition.
type voiceSendBarrier struct {
	done chan struct{}
	err  error
}

func newVoiceSendBarrier() *voiceSendBarrier {
	return &voiceSendBarrier{done: make(chan struct{})}
}

func (b *voiceSendBarrier) complete(err error) {
	if b == nil {
		return
	}
	b.err = err
	close(b.done)
}

func (b *voiceSendBarrier) wait(ctx context.Context) error {
	if b == nil {
		return nil
	}
	select {
	case <-b.done:
		return b.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

type clientControl struct {
	order              uint64
	afterSeq           uint64
	afterVoice         *voiceSendBarrier
	event              protocol.ServerEvent
	close              *CloseEffect
	completionSequence uint64
	writeCtx           context.Context
	epoch              identity.ResponseEpoch
	incarnation        identity.Incarnation
}

type durableEffectControl struct {
	trusted    TrustedSessionContext
	transcript *TranscriptRequestedEffect
	handoff    *HandoffRequestedEffect
	sequence   uint64
	kind       string
}

type clientEffectWaiter struct {
	done             chan error
	policySuppressed bool
	onSuccess        func()
}

type presentationSafetyWork struct {
	sequence             uint64
	request              presentationSafetyRequest
	done                 chan error
	acceptanceChecked    bool
	acceptanceGeneration uint64
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

func (p *sequenceProgress) current() uint64 {
	if p == nil {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.completed
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

func (a *activeAudioWrite) cancelActive() {
	a.mu.Lock()
	if a.cancel != nil {
		a.cancel()
	}
	a.mu.Unlock()
}

type runState struct {
	cfg                Config
	ids                IDs
	runtime            *sessionRuntime
	voice              realtime.Session
	client             ClientConn
	fence              *EpochFence
	inbox              chan reactorMessage
	completions        chan reactorMessage
	ingress            chan ingressFrame
	ingressSlots       chan struct{}
	voiceCtl           chan voiceControl
	egress             chan egressFrame
	clientCtl          chan clientControl
	inboxSlots         chan struct{}
	voiceSlots         chan struct{}
	clientSlots        chan struct{}
	egressSlots        chan struct{}
	progress           *sequenceProgress
	activeWrite        *activeAudioWrite
	activeControlWrite *activeAudioWrite
	metrics            MetricsHooks
	subject            Subject
	dispatcher         DurableEffectDispatcher
	safetyWork         chan presentationSafetyWork
	durableCtl         chan durableEffectControl
	durableSlots       chan struct{}
	voiceReadReady     chan struct{}
	voiceReadReadyOnce sync.Once

	ingressSequence uint64
	egressSequence  uint64
	eventSequence   uint64
	sequenceMu      sync.Mutex
	toolPolicyMu    sync.RWMutex
	toolPolicy      toolpolicy.Policy

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
	externalProgress *sequenceProgress
	pendingVoice     atomic.Int64
	pendingVoiceMu   sync.Mutex
	pendingVoiceByID map[uint64]realtime.CommandKind
	effectSequence   atomic.Uint64
	clientEffectMu   sync.Mutex
	clientWaiters    map[uint64]clientEffectWaiter
	durableEffectMu  sync.Mutex
	pendingDurable   map[uint64]string
	handoffEpoch     atomic.Uint64
}

func NewManager(cfg Config, connector realtime.Connector, ids IDGenerator) *Manager {
	normalized, err := normalizeManagerConfig(cfg)
	operationCapacity := normalized.ReconnectOwnerMaxOperations
	if operationCapacity < 1 || operationCapacity > 4_096 {
		operationCapacity = 0
	}
	return &Manager{
		cfg:                     normalized,
		connector:               connector,
		ids:                     ids,
		validationErr:           err,
		active:                  make(map[runtimeKey]*sessionRuntime),
		current:                 make(map[handoff.Scope]*sessionRuntime),
		logical:                 make(map[identity.SessionID]*logicalSession),
		metrics:                 normalizeMetricsHooks(MetricsHooks{}),
		reconnectOperationSlots: make(chan struct{}, operationCapacity),
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

func (m *Manager) observeReconnectTombstones(count int) {
	hooks := m.metricsSnapshot()
	if hooks.ReconnectTombstones != nil {
		hooks.ReconnectTombstones(count)
	}
}

// ActiveSessions returns only currently published runtimes; it never exposes
// session identities.
func (m *Manager) ActiveSessions() int64 {
	if m == nil {
		return 0
	}
	m.activeMu.RLock()
	defer m.activeMu.RUnlock()
	return int64(len(m.active))
}

func (m *Manager) ReconnectEnabled() bool {
	return m != nil && m.validationErr == nil && !isNilInterface(m.cfg.ReconnectCredentials) && !isNilInterface(m.cfg.ReconnectOwners)
}

func (m *Manager) now() time.Time {
	if m != nil && !isNilInterface(m.cfg.Clock) {
		return m.cfg.Clock.Now()
	}
	return time.Now()
}

// pruneLogicalSessions is intentionally independent of session creation so a
// quiet process releases expired reconnect metadata without requiring churn.
func (m *Manager) pruneLogicalSessions() {
	if m == nil {
		return
	}
	m.activeMu.Lock()
	count := m.pruneLogicalLocked(m.now())
	m.activeMu.Unlock()
	m.observeReconnectTombstones(count)
}

// activeMu must be held.
func (m *Manager) pruneLogicalLocked(now time.Time) int {
	if m.logical == nil {
		return 0
	}
	ttl := m.cfg.ReconnectTombstoneTTL
	if ttl <= 0 {
		ttl = defaultReconnectTombstoneTTL
	}
	budget := m.cfg.MaxReconnectTombstones
	if budget <= 0 {
		budget = defaultMaxReconnectTombstones
	}
	for sessionID, logical := range m.logical {
		if logical == nil || logical.state != logicalTombstoned || logical.runtime != nil || logical.reconnecting || logical.tombstonedAt.IsZero() {
			continue
		}
		if !now.Before(logical.tombstonedAt.Add(ttl)) {
			delete(m.logical, sessionID)
		}
	}
	for m.countTombstonesLocked() > budget {
		if !m.evictOldestTombstoneLocked() {
			break
		}
	}
	return m.countTombstonesLocked()
}

// activeMu must be held.
func (m *Manager) countTombstonesLocked() int {
	count := 0
	for _, logical := range m.logical {
		if logical != nil && logical.state == logicalTombstoned && logical.runtime == nil && !logical.reconnecting {
			count++
		}
	}
	return count
}

// activeMu must be held. Active, establishing, and reconnecting entries are
// deliberately ineligible even when their timestamps are malformed.
func (m *Manager) evictOldestTombstoneLocked() bool {
	var selected identity.SessionID
	var selectedAt time.Time
	for sessionID, logical := range m.logical {
		if logical == nil || logical.state != logicalTombstoned || logical.runtime != nil || logical.reconnecting {
			continue
		}
		if selected == "" || logical.tombstonedAt.Before(selectedAt) || logical.tombstonedAt.Equal(selectedAt) && sessionID < selected {
			selected, selectedAt = sessionID, logical.tombstonedAt
		}
	}
	if selected == "" {
		return false
	}
	delete(m.logical, selected)
	return true
}

func minimalReconnectSubject(subject Subject) Subject {
	return Subject{TenantID: subject.TenantID, UserID: subject.UserID, ExpiresAt: subject.ExpiresAt, Ephemeral: subject.Ephemeral}
}

func normalizeManagerConfig(cfg Config) (Config, error) {
	cfg.InitialSession = cloneRealtimeSessionConfig(cfg.InitialSession)
	if emptyToolCatalog(cfg.ToolCatalog) {
		cfg.ToolCatalog = toolpolicy.DefaultCatalog()
	}
	cfg.ToolCatalog.WebSearch.Regions = append([]string(nil), cfg.ToolCatalog.WebSearch.Regions...)
	if _, err := defaultToolPolicy(cfg.ToolCatalog); err != nil {
		return cfg, fmt.Errorf("%w: tool catalog: %v", ErrInvalidManagerConfig, err)
	}
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
	if cfg.ReconnectTombstoneTTL == 0 {
		cfg.ReconnectTombstoneTTL = defaultReconnectTombstoneTTL
	}
	if cfg.ReconnectTombstoneTTL < time.Second || cfg.ReconnectTombstoneTTL > 24*time.Hour {
		return cfg, fmt.Errorf("%w: reconnect tombstone TTL is unsafe", ErrInvalidManagerConfig)
	}
	if cfg.MaxReconnectTombstones == 0 {
		cfg.MaxReconnectTombstones = defaultMaxReconnectTombstones
	}
	if cfg.MaxReconnectTombstones < 1 || cfg.MaxReconnectTombstones >= maximumLogicalSessions {
		return cfg, fmt.Errorf("%w: reconnect tombstone budget is unsafe", ErrInvalidManagerConfig)
	}
	reconnectAuthorityConfigured := !isNilInterface(cfg.ReconnectCredentials) || !isNilInterface(cfg.ReconnectOwners) || cfg.ReconnectOwnerID != "" ||
		cfg.ReconnectCredentialTTL != 0 || cfg.ReconnectCredentialOverlap != 0 || cfg.ReconnectOwnerLeaseDuration != 0 ||
		cfg.ReconnectOwnerHeartbeatInterval != 0 || cfg.ReconnectOwnerOperationTimeout != 0 || cfg.ReconnectOwnerMaxOperations != 0 ||
		cfg.ReconnectCleanupInterval != 0 || cfg.ReconnectCleanupBatch != 0
	if reconnectAuthorityConfigured {
		if isNilInterface(cfg.ReconnectCredentials) || isNilInterface(cfg.ReconnectOwners) || !boundedReconnectValue(cfg.ReconnectOwnerID) {
			return cfg, fmt.Errorf("%w: reconnect authority dependencies are incomplete", ErrInvalidManagerConfig)
		}
		if cfg.ReconnectCredentialTTL == 0 {
			cfg.ReconnectCredentialTTL = defaultReconnectCredentialTTL
		}
		if cfg.ReconnectCredentialTTL < 10*time.Second || cfg.ReconnectCredentialTTL > time.Hour {
			return cfg, fmt.Errorf("%w: reconnect credential TTL is unsafe", ErrInvalidManagerConfig)
		}
		if cfg.ReconnectCredentialOverlap == 0 {
			cfg.ReconnectCredentialOverlap = cfg.ReconnectCredentialTTL / 4
		}
		if cfg.ReconnectCredentialOverlap < time.Second || cfg.ReconnectCredentialOverlap > cfg.ReconnectCredentialTTL/2 {
			return cfg, fmt.Errorf("%w: reconnect credential overlap is unsafe", ErrInvalidManagerConfig)
		}
		if cfg.ReconnectOwnerLeaseDuration == 0 {
			cfg.ReconnectOwnerLeaseDuration = defaultReconnectOwnerLease
		}
		if cfg.ReconnectOwnerHeartbeatInterval == 0 {
			cfg.ReconnectOwnerHeartbeatInterval = defaultReconnectHeartbeat
			if refreshCadence := cfg.ReconnectCredentialTTL / 6; cfg.ReconnectOwnerHeartbeatInterval > refreshCadence {
				cfg.ReconnectOwnerHeartbeatInterval = refreshCadence
			}
		}
		if cfg.ReconnectOwnerLeaseDuration < 3*time.Second || cfg.ReconnectOwnerLeaseDuration > 10*time.Minute ||
			cfg.ReconnectOwnerHeartbeatInterval < time.Second || cfg.ReconnectOwnerHeartbeatInterval > cfg.ReconnectOwnerLeaseDuration/2 {
			return cfg, fmt.Errorf("%w: reconnect owner lease is unsafe", ErrInvalidManagerConfig)
		}
		if cfg.ReconnectOwnerHeartbeatInterval > cfg.ReconnectCredentialTTL/6 {
			return cfg, fmt.Errorf("%w: reconnect credential refresh cadence is unsafe", ErrInvalidManagerConfig)
		}
		if cfg.ReconnectOwnerOperationTimeout == 0 {
			cfg.ReconnectOwnerOperationTimeout = defaultReconnectOperation
		}
		if cfg.ReconnectOwnerOperationTimeout < time.Millisecond || cfg.ReconnectOwnerOperationTimeout > 30*time.Second ||
			cfg.ReconnectOwnerOperationTimeout > cfg.ReconnectOwnerLeaseDuration-cfg.ReconnectOwnerHeartbeatInterval {
			return cfg, fmt.Errorf("%w: reconnect owner operation timeout is unsafe", ErrInvalidManagerConfig)
		}
		if cfg.ReconnectOwnerMaxOperations == 0 {
			cfg.ReconnectOwnerMaxOperations = defaultReconnectMaxOperations
		}
		if cfg.ReconnectOwnerMaxOperations < 1 || cfg.ReconnectOwnerMaxOperations > 4_096 {
			return cfg, fmt.Errorf("%w: reconnect owner operation concurrency is unsafe", ErrInvalidManagerConfig)
		}
		if cfg.ReconnectCleanupInterval == 0 {
			cfg.ReconnectCleanupInterval = defaultReconnectCleanup
		}
		if cfg.ReconnectCleanupBatch == 0 {
			cfg.ReconnectCleanupBatch = defaultReconnectCleanupBatch
		}
		if cfg.ReconnectCleanupInterval < time.Second || cfg.ReconnectCleanupInterval > time.Hour ||
			cfg.ReconnectCleanupBatch < 1 || cfg.ReconnectCleanupBatch > 10_000 {
			return cfg, fmt.Errorf("%w: reconnect cleanup bounds are unsafe", ErrInvalidManagerConfig)
		}
	}
	if isNilInterface(cfg.Clock) {
		cfg.Clock = clock.Real{}
	}
	if isNilInterface(cfg.Jitter) {
		cfg.Jitter = managerJitter{}
	}
	provisioningConfigured := !isNilInterface(cfg.ConversationProvisioner) || cfg.ConversationTTL != 0
	if provisioningConfigured && (isNilInterface(cfg.ConversationProvisioner) || cfg.ConversationTTL < time.Hour || cfg.ConversationTTL > 10*365*24*time.Hour) {
		return cfg, fmt.Errorf("%w: conversation provisioning dependencies are incomplete", ErrInvalidManagerConfig)
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

func (m *Manager) Run(parent context.Context, request OpenRequest, client ClientConn) (runErr error) {
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
	tombstones := m.pruneLogicalLocked(m.now())
	if _, exists := m.logical[sessionID]; !exists && len(m.logical) >= maximumLogicalSessions && m.evictOldestTombstoneLocked() {
		tombstones--
	}
	if _, exists := m.logical[sessionID]; exists || len(m.logical) >= maximumLogicalSessions {
		m.activeMu.Unlock()
		m.observeReconnectTombstones(tombstones)
		return ErrSessionAlreadyActive
	}
	logical := &logicalSession{
		scope: scope, subject: cloneSubject(subject), model: request.RequestedModel, highWater: ids.Incarnation, turnBoundary: false,
		state: logicalEstablishing,
	}
	m.logical[sessionID] = logical
	m.activeMu.Unlock()
	m.observeReconnectTombstones(tombstones)
	defer m.failInitialEstablishment(sessionID, logical)

	sessionCtx, cancel := subjectContext(parent, subject.ExpiresAt)
	defer cancel()
	if err := sessionCtx.Err(); err != nil {
		return fmt.Errorf("subject expired: %w", err)
	}
	if !subject.Ephemeral && !isNilInterface(m.cfg.ConversationProvisioner) {
		if err := m.cfg.ConversationProvisioner.ProvisionConversation(sessionCtx, scope, m.cfg.ConversationTTL); err != nil {
			return fmt.Errorf("provision conversation: %w", err)
		}
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
	if err := m.sendInitialSession(sessionCtx, voice); err != nil {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), internalCloseTimeout)
		_ = voice.Close(closeCtx)
		closeCancel()
		return fmt.Errorf("register server session tools: %w", err)
	}
	var reconnectMetadata *protocol.ReconnectMetadata
	var reconnectIssued *IssuedReconnectCredential
	reconnectOwnerCreated := false
	reconnectOwnerPublished := false
	reconnectAdvertisementAttempted := false
	var reconnectOwnerLossDeadline time.Time
	if m.ReconnectEnabled() {
		issued, owner, issueErr := m.issueReconnectCredential(subject, scope, sessionID, ids.Incarnation, 0, request.RequestedModel, ReconnectOwnerEstablishing)
		if issueErr != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), internalCloseTimeout)
			_ = voice.Close(closeCtx)
			closeCancel()
			return ErrRecoveryUnavailable
		}
		owner.TurnBoundary = true
		createStarted := m.now()
		createLease, createErr := m.cfg.ReconnectOwners.Create(sessionCtx, owner, m.cfg.ReconnectOwnerLeaseDuration)
		if createErr != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), internalCloseTimeout)
			_ = voice.Close(closeCtx)
			closeCancel()
			return ErrRecoveryUnavailable
		}
		reconnectOwnerLossDeadline = reconnectLossDeadline(createStarted, createLease,
			m.cfg.ReconnectOwnerHeartbeatInterval, m.cfg.ReconnectOwnerLeaseDuration)
		if !m.now().Before(reconnectOwnerLossDeadline) {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), internalCloseTimeout)
			_ = voice.Close(closeCtx)
			closeCancel()
			return ErrRecoveryUnavailable
		}
		reconnectOwnerCreated = true
		reconnectIssued = &issued
		reconnectMetadata = reconnectMetadataFromIssued(issued)
		defer func() {
			if !reconnectOwnerPublished && !reconnectAdvertisementAttempted {
				ctx, cleanupCancel := context.WithTimeout(context.Background(), internalCloseTimeout)
				_ = m.cfg.ReconnectOwners.Delete(ctx, sessionID, ids.Incarnation, m.cfg.ReconnectOwnerID)
				cleanupCancel()
			}
		}()
	}

	group, groupCtx := errgroup.WithContext(sessionCtx)
	quiesced := make(chan struct{})
	var quiescedOnce sync.Once
	markQuiesced := func() { quiescedOnce.Do(func() { close(quiesced) }) }
	state := newRunState(m.cfg, ids, voice, client, cancel)
	state.subject = cloneSubject(subject)
	state.metrics = m.metricsSnapshot()
	runtime := &sessionRuntime{
		key:                   runtimeKey{sessionID: sessionID, incarnation: ids.Incarnation},
		subject:               subject,
		inbox:                 state.inbox,
		done:                  groupCtx.Done(),
		quiesced:              quiesced,
		state:                 state,
		accepting:             true,
		inboxSlots:            state.inboxSlots,
		reconnectLossDeadline: reconnectOwnerLossDeadline,
	}
	if reconnectIssued != nil {
		copy := *reconnectIssued
		runtime.reconnect = &copy
		runtime.reconnectDelivered = true
	}
	state.runtime = runtime
	var firstErr error
	var firstErrOnce sync.Once
	runLoop := func(name string, loop func(context.Context) error) func() error {
		return func() error {
			err := loop(groupCtx)
			runtime.stop()
			if err != nil && !errors.Is(err, errGracefulSessionClose) && !state.graceful.Load() {
				if !(groupCtx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, io.EOF))) {
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
	group.Go(runLoop("presentation safety", state.presentationSafetyLoop))
	group.Go(runLoop("durable dispatcher", state.durableWriter))
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
	reconnectAdvertisementAttempted = reconnectMetadata != nil
	<-state.inboxSlots
	connected := reactorMessage{input: ConnectedInput{ProviderSession: json.RawMessage(`{}`), Reconnect: reconnectMetadata}, done: make(chan error, 1), requireClientWrites: true}
	state.acceptControl(&connected)
	state.inbox <- connected
	select {
	case err := <-connected.done:
		if err != nil {
			return abortBeforePublication(fmt.Errorf("establish client session: %w", err))
		}
		state.voiceReadReadyOnce.Do(func() { close(state.voiceReadReady) })
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
	if reconnectOwnerCreated {
		publishStarted := m.now()
		key := runtime.reconnectKey()
		lease, err := m.runReconnectOwnerLeaseOperation(sessionCtx, reconnectOwnerLossDeadline, func(operationCtx context.Context) (ReconnectOwnerLease, error) {
			return m.cfg.ReconnectOwners.Publish(operationCtx, key, runtime.state.fence.Current(), runtime.atTurnBoundary(), m.cfg.ReconnectOwnerLeaseDuration)
		})
		if err != nil {
			m.rollbackInitialPublication(runtime, logical)
			return abortBeforePublication(ErrRecoveryUnavailable)
		}
		reconnectOwnerPublished = true
		lossDeadline := reconnectLossDeadline(publishStarted, lease, m.cfg.ReconnectOwnerHeartbeatInterval, m.cfg.ReconnectOwnerLeaseDuration)
		runtime.setReconnectLossDeadline(lossDeadline)
		group.Go(runLoop("reconnect owner heartbeat", func(ctx context.Context) error { return m.maintainReconnectOwner(ctx, runtime, lossDeadline) }))
	}
	runtime.commitPublication()
	hooks := m.metricsSnapshot()
	hooks.SessionStarted()
	defer func() {
		runErr = errors.Join(runErr, m.unregister(runtime.key, runtime))
		hooks.SessionEnded()
	}()

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
	if m.logical[sessionID] != logical || logical.state != logicalEstablishing {
		m.activeMu.Unlock()
		return
	}
	delete(m.logical, sessionID)
	tombstones := m.countTombstonesLocked()
	m.activeMu.Unlock()
	m.observeReconnectTombstones(tombstones)
}

// Reconnect replaces a logical session through an exact-incarnation
// compare-and-swap. Scope is expected to come from the authenticated transport
// and is checked against the immutable scope retained by the manager.
func (m *Manager) Reconnect(parent context.Context, request ReconnectRequest, client ClientConn) (returnErr error) {
	if m == nil || m.validationErr != nil {
		if m != nil && m.validationErr != nil {
			return m.validationErr
		}
		return ErrStaleReconnect
	}
	started := m.now()
	if parent == nil || isNilInterface(client) {
		return ErrStaleReconnect
	}
	if err := validateReconnectRequest(request); err != nil {
		return err
	}
	if uint64(request.LastIncarnation) == math.MaxUint64 {
		return ErrStaleReconnect
	}
	authoritativeOwners := m.ReconnectEnabled()
	if authoritativeOwners {
		if err := validateSubject(request.Subject); err != nil || request.Credential.Scope != request.Scope ||
			request.Credential.SessionID != request.SessionID || request.Credential.Incarnation != request.LastIncarnation ||
			request.Credential.Cursor != request.LastEventSeq || validateReconnectClaims(request.Credential, m.now(), true) != nil {
			return ErrStaleReconnect
		}
	}
	m.activeMu.Lock()
	tombstones := m.pruneLogicalLocked(m.now())
	logical := m.logical[request.SessionID]
	remoteOwner := logical == nil && authoritativeOwners
	var checkErr error
	if !remoteOwner {
		checkErr = m.checkReconnectLocked(request, logical)
	}
	m.activeMu.Unlock()
	m.observeReconnectTombstones(tombstones)
	if checkErr != nil {
		m.metricsSnapshot().ReconnectRestore(m.now().Sub(started), "rejected")
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
	var old *sessionRuntime
	var subject Subject
	var model string
	if logical != nil {
		if err := m.checkReconnectLocked(request, logical); err != nil {
			m.activeMu.Unlock()
			return err
		}
		old = logical.runtime
		if old != nil {
			if err := old.fenceAtTurnBoundary(parent, m.cfg.reconnectBoundaryGate); err != nil {
				m.activeMu.Unlock()
				return err
			}
		}
		logical.reconnecting = true
		subject = cloneSubject(logical.subject)
		model = logical.model
	} else {
		if !authoritativeOwners {
			m.activeMu.Unlock()
			return ErrStaleReconnect
		}
		subject = cloneSubject(request.Subject)
		model = request.Credential.Model
	}
	newIncarnation := request.LastIncarnation + 1
	m.activeMu.Unlock()

	var durablyPublished atomic.Bool
	var advertisementAttempted atomic.Bool
	claimSucceeded := false
	remoteLogicalCreated := false
	var claimedOwner ReconnectOwner
	var claimedNext ReconnectOwner
	var claimedLossDeadline time.Time
	defer func() {
		if claimSucceeded && !durablyPublished.Load() && !advertisementAttempted.Load() {
			ctx, cancel := context.WithTimeout(context.Background(), internalCloseTimeout)
			abortErr := m.cfg.ReconnectOwners.AbortClaim(ctx, claimedNext.Key(), claimedOwner)
			cancel()
			if abortErr != nil {
				returnErr = errors.Join(returnErr, ErrRecoveryUnavailable, fmt.Errorf("abort reconnect claim: %w", abortErr))
			}
		}
		m.activeMu.Lock()
		if remoteLogicalCreated && !durablyPublished.Load() && m.logical[request.SessionID] == logical {
			delete(m.logical, request.SessionID)
		} else if current := m.logical[request.SessionID]; current != nil && current == logical && current.highWater == request.LastIncarnation {
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
	desiredNextEpoch := identity.ResponseEpoch(0)
	m.activeMu.RLock()
	if logical != nil && logical.responseEpoch > desiredNextEpoch {
		desiredNextEpoch = logical.responseEpoch
	}
	m.activeMu.RUnlock()
	if old != nil && old.state != nil && old.state.fence != nil && old.state.fence.Current() > desiredNextEpoch {
		desiredNextEpoch = old.state.fence.Current()
	}
	if uint64(desiredNextEpoch) == math.MaxUint64 {
		return ErrStaleReconnect
	}
	desiredNextEpoch++
	var reconnectMetadata *protocol.ReconnectMetadata
	var reconnectIssued *IssuedReconnectCredential
	if authoritativeOwners {
		issued, nextOwner, issueErr := m.issueReconnectCredential(subject, request.Scope, request.SessionID, newIncarnation,
			request.LastEventSeq, model, ReconnectOwnerClaiming)
		if issueErr != nil {
			return ErrRecoveryUnavailable
		}
		nextOwner.ResponseEpoch = desiredNextEpoch
		nextOwner.PreviousAcceptUntil = earlierTime(request.Credential.ExpiresAt,
			m.now().Add(m.cfg.ReconnectOwnerLeaseDuration+m.cfg.ReconnectCredentialOverlap))
		nextOwner.PreviousCredentialID = request.Credential.CredentialID
		nextOwner.PreviousIncarnation = request.Credential.Incarnation
		nextOwner.PreviousCursor = request.Credential.Cursor
		nextOwner.PreviousExpiresAt = request.Credential.ExpiresAt
		claimStarted := m.now()
		var claimLease ReconnectOwnerLease
		claimedOwner, claimLease, err = m.cfg.ReconnectOwners.Claim(parent, request.Credential, nextOwner, m.cfg.ReconnectOwnerLeaseDuration)
		if err != nil {
			return ErrStaleReconnect
		}
		claimedLossDeadline = reconnectLossDeadline(claimStarted, claimLease,
			m.cfg.ReconnectOwnerHeartbeatInterval, m.cfg.ReconnectOwnerLeaseDuration)
		if !m.now().Before(claimedLossDeadline) {
			return ErrRecoveryUnavailable
		}
		restagedExisting := (claimedOwner.State == ReconnectOwnerClaiming || claimedOwner.State == ReconnectOwnerActive) && claimedOwner.Incarnation == newIncarnation &&
			reconnectPreviousClaims(claimedOwner) == request.Credential
		if restagedExisting {
			issued, issueErr = m.cfg.ReconnectCredentials.Issue(claimedOwner.Claims())
			if issueErr != nil {
				return ErrRecoveryUnavailable
			}
			nextOwner = claimedOwner
			nextOwner.OwnerID = m.cfg.ReconnectOwnerID
			nextOwner.State = ReconnectOwnerClaiming
		}
		claimSucceeded, claimedNext = true, nextOwner
		expectedClaimedIncarnation := request.LastIncarnation
		if restagedExisting {
			expectedClaimedIncarnation = newIncarnation
		}
		if claimedOwner.Scope != request.Scope || claimedOwner.Model != model || claimedOwner.Incarnation != expectedClaimedIncarnation {
			return ErrStaleReconnect
		}
		reconnectIssued = &issued
		reconnectMetadata = reconnectMetadataFromIssued(issued)
		if logical == nil {
			logical = &logicalSession{scope: request.Scope, subject: minimalReconnectSubject(subject), model: model,
				highWater: request.LastIncarnation, responseEpoch: claimedOwner.ResponseEpoch, reconnecting: true,
				turnBoundary: true, tombstonedAt: m.now(), state: logicalTombstoned}
			m.activeMu.Lock()
			if m.logical[request.SessionID] != nil {
				m.activeMu.Unlock()
				return ErrStaleReconnect
			}
			if len(m.logical) >= maximumLogicalSessions {
				_ = m.evictOldestTombstoneLocked()
			}
			if len(m.logical) >= maximumLogicalSessions {
				m.activeMu.Unlock()
				return ErrRecoveryUnavailable
			}
			m.logical[request.SessionID] = logical
			remoteLogicalCreated = true
			m.activeMu.Unlock()
		}
	}

	// The old runtime is now fenced and fully quiesced. Its live epoch fence is
	// the authority; the logical cache may trail provider/control work that was
	// accepted immediately before the intake fence closed.
	m.activeMu.Lock()
	if m.logical[request.SessionID] != logical || logical.highWater != request.LastIncarnation || !logical.reconnecting {
		m.activeMu.Unlock()
		return ErrStaleReconnect
	}
	authoritativeEpoch := logical.responseEpoch
	if authoritativeOwners && claimedOwner.ResponseEpoch > authoritativeEpoch {
		authoritativeEpoch = claimedOwner.ResponseEpoch
	}
	if old != nil && old.state != nil && old.state.fence != nil && old.state.fence.Current() > authoritativeEpoch {
		authoritativeEpoch = old.state.fence.Current()
	}
	if uint64(authoritativeEpoch) == math.MaxUint64 {
		m.activeMu.Unlock()
		return ErrStaleReconnect
	}
	logical.responseEpoch = authoritativeEpoch
	initialEpoch := authoritativeEpoch + 1
	m.activeMu.Unlock()

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
	if err := m.sendInitialSession(sessionCtx, voice); err != nil {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), internalCloseTimeout)
		_ = voice.Close(closeCtx)
		closeCancel()
		m.closeUnpublishedClient(parent, client, 1011, "recovery unavailable")
		if sessionCtx.Err() != nil {
			return sessionCtx.Err()
		}
		return ErrRecoveryUnavailable
	}
	commands, err := recoveryCommands(snapshot.CommittedItems)
	var activeEvents []protocol.ServerEvent
	if err == nil {
		var activeCommands []realtime.Command
		activeCommands, activeEvents, err = activeHandoffRecovery(snapshot.ActiveHandoffs, snapshot.LastEventSeq)
		commands = append(commands, activeCommands...)
	}
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
	err = m.runReplacement(sessionCtx, cancel, subject, ids, client, voice, initialEpoch, logical, request.LastIncarnation,
		request.LastEventSeq, reconnectIssued, reconnectMetadata, activeEvents, snapshot.PendingApprovals,
		claimedLossDeadline, advertisementAttempted.Store, durablyPublished.Store)
	result := "ok"
	if err != nil {
		result = "error"
	}
	m.metricsSnapshot().ReconnectRestore(m.cfg.Clock.Now().Sub(started), result)
	return err
}

func (m *Manager) sendInitialSession(ctx context.Context, voice realtime.Session) error {
	config := cloneRealtimeSessionConfig(m.cfg.InitialSession)
	if len(config.Tools) == 0 && len(config.ToolChoice) == 0 && config.Instructions == "" &&
		config.InputFormat == (realtime.AudioFormat{}) && config.OutputFormat == (realtime.AudioFormat{}) &&
		config.Voice == "" && len(config.TurnDetection) == 0 {
		return nil
	}
	return voice.Send(ctx, realtime.Command{Kind: realtime.CommandSessionUpdate, Session: config})
}

func (m *Manager) issueReconnectCredential(subject Subject, scope handoff.Scope, sessionID identity.SessionID, incarnation identity.Incarnation,
	cursor int64, model string, state ReconnectOwnerState,
) (IssuedReconnectCredential, ReconnectOwner, error) {
	if !m.ReconnectEnabled() {
		return IssuedReconnectCredential{}, ReconnectOwner{}, ErrRecoveryUnavailable
	}
	expiresAt := m.now().Add(m.cfg.ReconnectCredentialTTL)
	if !subject.ExpiresAt.IsZero() && subject.ExpiresAt.Before(expiresAt) {
		expiresAt = subject.ExpiresAt
	}
	issued, err := m.cfg.ReconnectCredentials.Issue(ReconnectClaims{Scope: scope, SessionID: sessionID, Incarnation: incarnation,
		Cursor: cursor, Model: model, ExpiresAt: expiresAt})
	if err != nil {
		return IssuedReconnectCredential{}, ReconnectOwner{}, err
	}
	owner := ReconnectOwner{Scope: scope, SessionID: sessionID, Incarnation: incarnation, Cursor: cursor, Model: model,
		CredentialID: issued.Claims.CredentialID, OwnerID: m.cfg.ReconnectOwnerID, State: state,
		ExpiresAt: issued.Claims.ExpiresAt, SubjectExpiresAt: subject.ExpiresAt, Ephemeral: subject.Ephemeral}
	return issued, owner, nil
}

func reconnectMetadataFromIssued(issued IssuedReconnectCredential) *protocol.ReconnectMetadata {
	return &protocol.ReconnectMetadata{Credential: issued.Credential, SessionID: string(issued.Claims.SessionID),
		ConversationID: string(issued.Claims.Scope.ConversationID), Incarnation: uint64(issued.Claims.Incarnation),
		Cursor: issued.Claims.Cursor, Model: issued.Claims.Model, ExpiresAt: issued.Claims.ExpiresAt}
}

func (r *sessionRuntime) reconnectKey() ReconnectOwnerKey {
	if r == nil {
		return ReconnectOwnerKey{}
	}
	r.reconnectMu.Lock()
	defer r.reconnectMu.Unlock()
	if r.reconnect == nil {
		return ReconnectOwnerKey{SessionID: r.key.sessionID, Incarnation: r.key.incarnation}
	}
	return ReconnectOwnerKey{SessionID: r.key.sessionID, Incarnation: r.key.incarnation,
		OwnerID: r.state.cfg.ReconnectOwnerID, CredentialID: r.reconnect.Claims.CredentialID}
}

func (r *sessionRuntime) setReconnectLossDeadline(deadline time.Time) {
	if r == nil {
		return
	}
	r.reconnectMu.Lock()
	r.reconnectLossDeadline = deadline
	r.reconnectMu.Unlock()
}

func (r *sessionRuntime) currentReconnectLossDeadline() time.Time {
	if r == nil {
		return time.Time{}
	}
	r.reconnectMu.Lock()
	defer r.reconnectMu.Unlock()
	return r.reconnectLossDeadline
}

func reconnectLossDeadline(operationStarted time.Time, lease ReconnectOwnerLease, heartbeat, maximum time.Duration) time.Time {
	confirmed := lease.ExpiresAt.Sub(lease.DatabaseNow)
	if confirmed > maximum {
		confirmed = maximum
	}
	confirmed -= heartbeat
	if confirmed < 0 {
		confirmed = 0
	}
	return operationStarted.Add(confirmed)
}

type reconnectOwnerLeaseResult struct {
	lease ReconnectOwnerLease
	err   error
}

func (m *Manager) acquireReconnectOperation() bool {
	if m == nil || m.reconnectOperationSlots == nil {
		return false
	}
	select {
	case m.reconnectOperationSlots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (m *Manager) releaseReconnectOperation() {
	if m == nil || m.reconnectOperationSlots == nil {
		return
	}
	<-m.reconnectOperationSlots
}

func (m *Manager) runReconnectOwnerLeaseOperation(ctx context.Context, lossDeadline time.Time,
	operation func(context.Context) (ReconnectOwnerLease, error),
) (ReconnectOwnerLease, error) {
	if ctx == nil || operation == nil || lossDeadline.IsZero() {
		return ReconnectOwnerLease{}, ErrRecoveryUnavailable
	}
	remaining := lossDeadline.Sub(m.now())
	if remaining <= 0 {
		return ReconnectOwnerLease{}, ErrRecoveryUnavailable
	}
	if !m.acquireReconnectOperation() {
		return ReconnectOwnerLease{}, ErrRecoveryUnavailable
	}
	operationWindow := m.cfg.ReconnectOwnerOperationTimeout
	if operationWindow <= 0 || operationWindow > remaining {
		operationWindow = remaining
	}
	operationCtx, cancel := context.WithTimeout(ctx, operationWindow)
	defer cancel()
	lossTimer := m.cfg.Clock.NewTicker(remaining)
	defer lossTimer.Stop()
	result := make(chan reconnectOwnerLeaseResult, 1)
	go func() {
		defer m.releaseReconnectOperation()
		lease, err := operation(operationCtx)
		result <- reconnectOwnerLeaseResult{lease: lease, err: err}
	}()
	select {
	case <-ctx.Done():
		return ReconnectOwnerLease{}, ctx.Err()
	case <-lossTimer.C():
		return ReconnectOwnerLease{}, ErrRecoveryUnavailable
	case <-operationCtx.Done():
		if ctx.Err() != nil {
			return ReconnectOwnerLease{}, ctx.Err()
		}
		return ReconnectOwnerLease{}, ErrRecoveryUnavailable
	case completed := <-result:
		if !m.now().Before(lossDeadline) {
			return ReconnectOwnerLease{}, ErrRecoveryUnavailable
		}
		return completed.lease, completed.err
	}
}

func (m *Manager) runReconnectOwnerOperation(ctx context.Context, lossDeadline time.Time, operation func(context.Context) error) error {
	_, err := m.runReconnectOwnerLeaseOperation(ctx, lossDeadline, func(operationCtx context.Context) (ReconnectOwnerLease, error) {
		return ReconnectOwnerLease{}, operation(operationCtx)
	})
	return err
}

func (m *Manager) maintainReconnectOwner(ctx context.Context, runtime *sessionRuntime, lossDeadline time.Time) error {
	ticker := m.cfg.Clock.NewTicker(m.cfg.ReconnectOwnerHeartbeatInterval)
	defer ticker.Stop()
	if m.cfg.reconnectHeartbeatReady != nil {
		m.cfg.reconnectHeartbeatReady()
	}
	for {
		remaining := lossDeadline.Sub(m.now())
		if remaining <= 0 {
			return ErrRecoveryUnavailable
		}
		lossTimer := m.cfg.Clock.NewTicker(remaining)
		select {
		case <-ctx.Done():
			lossTimer.Stop()
			return nil
		case <-lossTimer.C():
			lossTimer.Stop()
			return ErrRecoveryUnavailable
		case <-ticker.C():
			lossTimer.Stop()
			operationStarted := m.now()
			lease, err := m.heartbeatReconnectOwner(ctx, runtime, lossDeadline)
			if err == nil {
				lossDeadline = reconnectLossDeadline(operationStarted, lease, m.cfg.ReconnectOwnerHeartbeatInterval, m.cfg.ReconnectOwnerLeaseDuration)
				if !m.now().Before(lossDeadline) {
					return ErrRecoveryUnavailable
				}
				runtime.setReconnectLossDeadline(lossDeadline)
				if m.reconnectRefreshDue(runtime) {
					if err := m.rotateReconnectCredential(ctx, runtime, -1, true); err != nil {
						return err
					}
				}
				continue
			}
			if errors.Is(err, ErrStaleReconnect) || !m.now().Before(lossDeadline) {
				return ErrRecoveryUnavailable
			}
		}
	}
}

func (m *Manager) heartbeatReconnectOwner(ctx context.Context, runtime *sessionRuntime, lossDeadline time.Time) (ReconnectOwnerLease, error) {
	runtime.reconnectMu.Lock()
	if runtime.reconnect == nil {
		runtime.reconnectMu.Unlock()
		return ReconnectOwnerLease{}, ErrRecoveryUnavailable
	}
	key := ReconnectOwnerKey{SessionID: runtime.key.sessionID, Incarnation: runtime.key.incarnation,
		OwnerID: m.cfg.ReconnectOwnerID, CredentialID: runtime.reconnect.Claims.CredentialID}
	runtime.reconnectMu.Unlock()
	return m.runReconnectOwnerLeaseOperation(ctx, lossDeadline, func(operationCtx context.Context) (ReconnectOwnerLease, error) {
		return m.cfg.ReconnectOwners.Heartbeat(operationCtx, key, runtime.state.fence.Current(), runtime.atTurnBoundary(), m.cfg.ReconnectOwnerLeaseDuration)
	})
}

func (m *Manager) reconnectRefreshDue(runtime *sessionRuntime) bool {
	runtime.reconnectMu.Lock()
	defer runtime.reconnectMu.Unlock()
	if runtime.reconnect == nil {
		return false
	}
	return !m.now().Before(runtime.reconnect.Claims.ExpiresAt.Add(-m.cfg.ReconnectCredentialTTL / 3))
}

func (m *Manager) rotateReconnectCredential(ctx context.Context, runtime *sessionRuntime, cursor int64, force bool) error {
	if !m.ReconnectEnabled() || runtime == nil {
		return nil
	}
	runtime.reconnectRotateOnce.Do(func() {
		runtime.reconnectRotate = make(chan struct{}, 1)
		runtime.reconnectRotate <- struct{}{}
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-runtime.reconnectRotate:
	}
	defer func() { runtime.reconnectRotate <- struct{}{} }()
	runtime.reconnectMu.Lock()
	if runtime.reconnect == nil {
		runtime.reconnectMu.Unlock()
		return ErrRecoveryUnavailable
	}
	current := *runtime.reconnect
	delivered := runtime.reconnectDelivered
	lossDeadline := runtime.reconnectLossDeadline
	runtime.reconnectMu.Unlock()
	if cursor < 0 {
		cursor = current.Claims.Cursor
	}
	if cursor < current.Claims.Cursor {
		return ErrStaleReconnect
	}
	if !force && cursor == current.Claims.Cursor && delivered {
		return nil
	}
	issued := current
	if force || cursor != current.Claims.Cursor {
		scope := runtime.handoffScope()
		var err error
		issued, _, err = m.issueReconnectCredential(runtime.subject, scope, runtime.key.sessionID, runtime.key.incarnation,
			cursor, current.Claims.Model, ReconnectOwnerActive)
		if err != nil {
			return ErrRecoveryUnavailable
		}
		overlapUntil := earlierTime(current.Claims.ExpiresAt, m.now().Add(m.cfg.ReconnectCredentialOverlap))
		if !overlapUntil.After(m.now()) {
			return ErrRecoveryUnavailable
		}
		key := ReconnectOwnerKey{SessionID: runtime.key.sessionID, Incarnation: runtime.key.incarnation,
			OwnerID: m.cfg.ReconnectOwnerID, CredentialID: current.Claims.CredentialID}
		if err := m.runReconnectOwnerOperation(ctx, lossDeadline, func(operationCtx context.Context) error {
			return m.cfg.ReconnectOwners.Rotate(operationCtx, key, current.Claims, issued.Claims, overlapUntil)
		}); err != nil {
			return ErrRecoveryUnavailable
		}
		runtime.reconnectMu.Lock()
		if runtime.reconnect == nil || runtime.reconnect.Claims != current.Claims {
			runtime.reconnectMu.Unlock()
			return ErrStaleReconnect
		}
		copy := issued
		runtime.reconnect = &copy
		runtime.reconnectDelivered = false
		runtime.reconnectMu.Unlock()
	}
	event := protocol.SessionReconnectUpdatedEvent{Type: "session.reconnect.updated",
		EventID: "reconnect-" + issued.Claims.CredentialID, Reconnect: *reconnectMetadataFromIssued(issued)}
	written := make(chan error, 1)
	sequence := runtime.state.nextEffectSequence()
	runtime.state.registerClientWaiter(sequence, clientEffectWaiter{done: written, onSuccess: func() {
		runtime.reconnectMu.Lock()
		if runtime.reconnect != nil && runtime.reconnect.Claims == issued.Claims {
			runtime.reconnectDelivered = true
		}
		runtime.reconnectMu.Unlock()
	}})
	if err := runtime.state.enqueueClientControl(clientControl{event: event, completionSequence: sequence}); err != nil {
		runtime.state.completeClientEffect(ClientEffectCompletedInput{Sequence: sequence})
		return err
	}
	select {
	case err := <-written:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// RunReconnectMaintenance owns bounded local tombstone pruning and global
// durable expiry cleanup for the lifetime of the gateway process.
func (m *Manager) RunReconnectMaintenance(ctx context.Context) error {
	if m == nil || !m.ReconnectEnabled() {
		return nil
	}
	if ctx == nil {
		return errors.New("reconnect maintenance context is nil")
	}
	m.sweepReconnectOwners(ctx)
	ticker := m.cfg.Clock.NewTicker(m.cfg.ReconnectCleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C():
			m.sweepReconnectOwners(ctx)
		}
	}
}

func (m *Manager) sweepReconnectOwners(ctx context.Context) {
	m.activeMu.Lock()
	tombstones := m.pruneLogicalLocked(m.now())
	m.activeMu.Unlock()
	m.observeReconnectTombstones(tombstones)
	operationCtx, cancel := context.WithTimeout(ctx, m.cfg.ReconnectOwnerOperationTimeout)
	result := make(chan error, 1)
	var err error
	if !m.acquireReconnectOperation() {
		err = ErrRecoveryUnavailable
	} else {
		go func() {
			defer m.releaseReconnectOperation()
			_, cleanupErr := m.cfg.ReconnectOwners.CleanupExpired(operationCtx, m.cfg.ReconnectCleanupBatch)
			result <- cleanupErr
		}()
		select {
		case <-ctx.Done():
			err = ctx.Err()
		case <-operationCtx.Done():
			err = operationCtx.Err()
		case err = <-result:
		}
	}
	cancel()
	m.reconnectMaintenanceMu.Lock()
	m.reconnectMaintenanceAttempted = true
	m.reconnectMaintenanceErr = err
	if err == nil {
		m.reconnectMaintenanceSucceeded = m.now()
	}
	m.reconnectMaintenanceMu.Unlock()
}

func (m *Manager) ReconnectReady(context.Context) error {
	if m == nil || !m.ReconnectEnabled() {
		return nil
	}
	m.reconnectMaintenanceMu.RLock()
	defer m.reconnectMaintenanceMu.RUnlock()
	if !m.reconnectMaintenanceAttempted {
		return errors.New("reconnect owner maintenance has not completed")
	}
	if m.reconnectMaintenanceErr != nil {
		return fmt.Errorf("reconnect owner maintenance: %w", m.reconnectMaintenanceErr)
	}
	if m.reconnectMaintenanceSucceeded.IsZero() ||
		m.now().Sub(m.reconnectMaintenanceSucceeded) > 2*m.cfg.ReconnectCleanupInterval+m.cfg.ReconnectOwnerOperationTimeout {
		return errors.New("reconnect owner maintenance is stale")
	}
	return nil
}

// activeMu must be held for reading or writing.
func (m *Manager) checkReconnectLocked(request ReconnectRequest, logical *logicalSession) error {
	if logical == nil || logical.scope != request.Scope || logical.highWater != request.LastIncarnation || logical.reconnecting ||
		uint64(logical.highWater) == math.MaxUint64 || uint64(logical.responseEpoch) == math.MaxUint64 {
		return ErrStaleReconnect
	}
	if logical.state == logicalEstablishing {
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
	reconnectIssued *IssuedReconnectCredential, reconnectMetadata *protocol.ReconnectMetadata, activeHandoffs []protocol.ServerEvent,
	pendingApprovals []RecoveryApproval, ownerLossDeadline time.Time, markAdvertisement func(bool), markPublished func(bool),
) (runErr error) {
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
		accepting: true, inboxSlots: state.inboxSlots, reconnectLossDeadline: ownerLossDeadline,
	}
	if reconnectIssued != nil {
		copy := *reconnectIssued
		runtime.reconnect = &copy
		runtime.reconnectDelivered = true
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
				!(groupCtx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, io.EOF))) {
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
	group.Go(runLoop("presentation safety", state.presentationSafetyLoop))
	group.Go(runLoop("durable dispatcher", state.durableWriter))
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
	if reconnectMetadata != nil && markAdvertisement != nil {
		markAdvertisement(true)
	}
	<-state.inboxSlots
	connected := reactorMessage{input: ConnectedInput{ProviderSession: json.RawMessage(`{}`), Reconnect: reconnectMetadata}, done: make(chan error, 1), requireClientWrites: true}
	state.acceptControl(&connected)
	state.inbox <- connected
	select {
	case err := <-connected.done:
		if err != nil {
			return abort(ErrRecoveryUnavailable)
		}
		state.voiceReadReadyOnce.Do(func() { close(state.voiceReadReady) })
	case <-groupCtx.Done():
		return abort(groupCtx.Err())
	}
	for _, event := range activeHandoffs {
		written := make(chan error, 1)
		sequence := state.nextEffectSequence()
		state.registerClientWaiter(sequence, clientEffectWaiter{done: written})
		if err := state.enqueueClientControl(clientControl{event: event, completionSequence: sequence}); err != nil {
			state.completeClientEffect(ClientEffectCompletedInput{Sequence: sequence})
			return abort(err)
		}
		select {
		case err := <-written:
			if err != nil {
				return abort(err)
			}
		case <-groupCtx.Done():
			return abort(groupCtx.Err())
		}
	}
	for _, pending := range pendingApprovals {
		if pending.Sequence > replayAfter {
			continue
		}
		approval := pending.Approval
		event := protocol.LiveToolApprovalRequiredEvent{Type: "live.tool.approval.required", EventID: pending.EventID, SequenceID: pending.Sequence,
			InvocationID: approval.InvocationID, HandoffID: string(approval.HandoffID), ActionSummary: approval.ActionSummary, ExpiresAt: approval.ExpiresAt, RiskClass: approval.RiskClass}
		if err := state.enqueueClientControl(clientControl{event: event}); err != nil {
			return abort(err)
		}
	}
	if err := m.registerReplacement(runtime, logical, expected); err != nil {
		return abort(err)
	}
	if m.ReconnectEnabled() {
		publishStarted := m.now()
		key := runtime.reconnectKey()
		lease, err := m.runReconnectOwnerLeaseOperation(sessionCtx, ownerLossDeadline, func(operationCtx context.Context) (ReconnectOwnerLease, error) {
			return m.cfg.ReconnectOwners.Publish(operationCtx, key, state.fence.Current(), runtime.atTurnBoundary(), m.cfg.ReconnectOwnerLeaseDuration)
		})
		if err != nil {
			m.rollbackReplacementPublication(runtime, logical, expected, initialEpoch-1)
			return abort(ErrRecoveryUnavailable)
		}
		if markPublished != nil {
			markPublished(true)
		}
		lossDeadline := reconnectLossDeadline(publishStarted, lease, m.cfg.ReconnectOwnerHeartbeatInterval, m.cfg.ReconnectOwnerLeaseDuration)
		runtime.setReconnectLossDeadline(lossDeadline)
		group.Go(runLoop("reconnect owner heartbeat", func(ctx context.Context) error { return m.maintainReconnectOwner(ctx, runtime, lossDeadline) }))
	}
	runtime.commitPublication()
	hooks := m.metricsSnapshot()
	hooks.SessionStarted()
	defer func() {
		runErr = errors.Join(runErr, m.unregister(runtime.key, runtime))
		hooks.SessionEnded()
	}()

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
	scope := runtime.handoffScope()
	m.activeMu.RLock()
	previous := m.current[scope]
	m.activeMu.RUnlock()
	if previous != nil && previous != runtime {
		previous.presentationMu.Lock()
		defer previous.presentationMu.Unlock()
	}
	m.activeMu.Lock()
	defer m.activeMu.Unlock()
	if m.logical[runtime.key.sessionID] != logical || !logical.reconnecting || logical.highWater != expected ||
		logical.scope != runtime.handoffScope() || runtime.key.incarnation != expected+1 ||
		logical.state == logicalEstablishing {
		return ErrStaleReconnect
	}
	if _, exists := m.active[runtime.key]; exists {
		return ErrStaleReconnect
	}
	if m.current[scope] != previous {
		return ErrStaleReconnect
	}
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
	if previous != nil && previous != runtime {
		previous.mu.Lock()
		runtime.publicationPrevious = previous
		runtime.publicationAccepting = previous.accepting
		runtime.publicationGeneration = previous.acceptanceGeneration
		previous.accepting = false
		previous.acceptanceGeneration++
		previous.mu.Unlock()
	}
	m.current[scope] = runtime
	logical.highWater = runtime.key.incarnation
	logical.responseEpoch = runtime.state.fence.Current()
	logical.runtime = runtime
	logical.reconnecting = false
	logical.turnBoundary = true
	logical.tombstonedAt = time.Time{}
	logical.state = logicalActive
	return nil
}

func (m *Manager) rollbackInitialPublication(runtime *sessionRuntime, logical *logicalSession) {
	if runtime == nil || logical == nil {
		return
	}
	previous := runtime.publicationPrevious
	if previous != nil {
		previous.presentationMu.Lock()
		defer previous.presentationMu.Unlock()
	}
	m.activeMu.Lock()
	if m.active[runtime.key] == runtime {
		delete(m.active, runtime.key)
	}
	if m.current[runtime.handoffScope()] == runtime {
		if previous != nil && m.active[previous.key] == previous {
			previous.mu.Lock()
			previous.accepting = runtime.publicationAccepting
			previous.acceptanceGeneration = runtime.publicationGeneration
			previous.mu.Unlock()
			m.current[runtime.handoffScope()] = previous
		} else {
			delete(m.current, runtime.handoffScope())
		}
	}
	if m.logical[runtime.key.sessionID] == logical && logical.runtime == runtime {
		logical.runtime = nil
		logical.state = logicalEstablishing
		logical.reconnecting = false
		logical.turnBoundary = false
	}
	m.activeMu.Unlock()
	if previous == nil && !isNilInterface(m.cfg.HandoffSubscriptions) {
		m.cfg.HandoffSubscriptions.Unregister(runtime.handoffSubscription(m.cfg.HandoffConsumerID))
	}
}

func (m *Manager) rollbackReplacementPublication(runtime *sessionRuntime, logical *logicalSession, expected identity.Incarnation, epoch identity.ResponseEpoch) {
	if runtime == nil || logical == nil {
		return
	}
	previous := runtime.publicationPrevious
	if previous != nil {
		previous.presentationMu.Lock()
		defer previous.presentationMu.Unlock()
	}
	m.activeMu.Lock()
	if m.active[runtime.key] == runtime {
		delete(m.active, runtime.key)
	}
	if m.current[runtime.handoffScope()] == runtime {
		if previous != nil && m.active[previous.key] == previous {
			previous.mu.Lock()
			previous.accepting = runtime.publicationAccepting
			previous.acceptanceGeneration = runtime.publicationGeneration
			previous.mu.Unlock()
			m.current[runtime.handoffScope()] = previous
		} else {
			delete(m.current, runtime.handoffScope())
		}
	}
	if m.logical[runtime.key.sessionID] == logical && logical.runtime == runtime && logical.highWater == runtime.key.incarnation {
		logical.highWater = expected
		logical.responseEpoch = epoch
		logical.runtime = nil
		logical.reconnecting = true
		logical.turnBoundary = true
		logical.tombstonedAt = m.now()
		logical.state = logicalTombstoned
	}
	m.activeMu.Unlock()
	if previous == nil && !isNilInterface(m.cfg.HandoffSubscriptions) {
		m.cfg.HandoffSubscriptions.Unregister(runtime.handoffSubscription(m.cfg.HandoffConsumerID))
	}
}

func newRunState(cfg Config, ids IDs, voice realtime.Session, client ClientConn, cancel context.CancelFunc) *runState {
	policy, _ := defaultToolPolicy(cfg.ToolCatalog)
	state := &runState{
		cfg:                cfg,
		ids:                ids,
		voice:              voice,
		client:             client,
		fence:              NewEpochFence(0),
		inbox:              make(chan reactorMessage, cfg.ControlEvents),
		completions:        make(chan reactorMessage, cfg.ControlEvents+3*cfg.WriterControlEvents),
		ingress:            make(chan ingressFrame, cfg.IngressFrames),
		ingressSlots:       make(chan struct{}, cfg.IngressFrames),
		voiceCtl:           make(chan voiceControl, cfg.WriterControlEvents),
		egress:             make(chan egressFrame, cfg.EgressFrames),
		clientCtl:          make(chan clientControl, cfg.WriterControlEvents),
		safetyWork:         make(chan presentationSafetyWork, cfg.ControlEvents),
		durableCtl:         make(chan durableEffectControl, cfg.WriterControlEvents),
		durableSlots:       make(chan struct{}, cfg.WriterControlEvents),
		voiceReadReady:     make(chan struct{}),
		inboxSlots:         make(chan struct{}, cfg.ControlEvents),
		voiceSlots:         make(chan struct{}, cfg.WriterControlEvents),
		clientSlots:        make(chan struct{}, cfg.WriterControlEvents),
		egressSlots:        make(chan struct{}, cfg.EgressFrames),
		progress:           newSequenceProgress(),
		externalProgress:   newSequenceProgress(),
		activeWrite:        &activeAudioWrite{},
		activeControlWrite: &activeAudioWrite{},
		closeClientDone:    make(chan struct{}),
		metrics:            normalizeMetricsHooks(MetricsHooks{}),
		dispatcher:         cfg.DurableDispatcher,
		cancel:             cancel,
		boundary:           Snapshot{IDs: ids, Response: ResponseIdle},
		toolPolicy:         policy,
		pendingVoiceByID:   make(map[uint64]realtime.CommandKind),
		clientWaiters:      make(map[uint64]clientEffectWaiter),
		pendingDurable:     make(map[uint64]string),
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
		state.durableSlots <- struct{}{}
	}
	for range cfg.EgressFrames {
		state.egressSlots <- struct{}{}
	}
	return state
}

func emptyToolCatalog(catalog toolpolicy.Catalog) bool {
	web := catalog.WebSearch
	return !web.Enabled && !web.ForceStrict && len(web.Regions) == 0 && web.MaximumResults == 0 && web.MaximumTimeoutMS == 0
}

func defaultToolPolicy(catalog toolpolicy.Catalog) (toolpolicy.Policy, error) {
	return toolpolicy.DefaultPolicyForCatalog(catalog)
}

func (s *runState) replaceToolPolicy(policy toolpolicy.Policy) {
	s.toolPolicyMu.Lock()
	s.toolPolicy = policy
	s.toolPolicyMu.Unlock()
}

func (s *runState) toolPolicySnapshot() toolpolicy.Policy {
	s.toolPolicyMu.RLock()
	defer s.toolPolicyMu.RUnlock()
	return s.toolPolicy
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
	scope := runtime.handoffScope()
	m.activeMu.RLock()
	previous := m.current[scope]
	m.activeMu.RUnlock()
	if previous != nil && previous != runtime {
		previous.presentationMu.Lock()
		defer previous.presentationMu.Unlock()
	}
	m.activeMu.Lock()
	defer m.activeMu.Unlock()
	if m.current[scope] != previous {
		return ErrStaleReconnect
	}
	if _, exists := m.active[runtime.key]; exists {
		return ErrSessionAlreadyActive
	}
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
	if previous != nil && previous != runtime {
		previous.mu.Lock()
		runtime.publicationPrevious = previous
		runtime.publicationAccepting = previous.accepting
		runtime.publicationGeneration = previous.acceptanceGeneration
		previous.accepting = false
		previous.acceptanceGeneration++
		previous.mu.Unlock()
	}
	m.current[scope] = runtime
	logical.runtime = runtime
	logical.responseEpoch = runtime.state.fence.Current()
	logical.reconnecting = false
	logical.turnBoundary = true
	logical.tombstonedAt = time.Time{}
	logical.state = logicalActive
	return nil
}

func (m *Manager) unregister(key runtimeKey, runtime *sessionRuntime) error {
	var unregisterErr error
	var reconnectEpoch identity.ResponseEpoch
	var reconnectBoundary bool
	tombstoneReconnectOwner := false
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
		reconnectEpoch = runtime.state.fence.Current()
		reconnectBoundary = runtime.atTurnBoundary()
		logical.responseEpoch = reconnectEpoch
		logical.turnBoundary = reconnectBoundary
		logical.subject = minimalReconnectSubject(logical.subject)
		logical.tombstonedAt = m.now()
		logical.state = logicalTombstoned
		tombstoneReconnectOwner = m.ReconnectEnabled()
	}
	tombstones := m.pruneLogicalLocked(m.now())
	m.activeMu.Unlock()
	m.observeReconnectTombstones(tombstones)
	if tombstoneReconnectOwner {
		ctx, cancel := context.WithTimeout(context.Background(), internalCloseTimeout)
		err := m.cfg.ReconnectOwners.Tombstone(ctx, key.sessionID, key.incarnation, m.cfg.ReconnectOwnerID, reconnectEpoch, reconnectBoundary)
		cancel()
		if err != nil {
			unregisterErr = fmt.Errorf("tombstone reconnect owner: %w", err)
		}
	}
	if unregisterPresentation && !isNilInterface(m.cfg.HandoffSubscriptions) {
		m.cfg.HandoffSubscriptions.Unregister(runtime.handoffSubscription(m.cfg.HandoffConsumerID))
	}
	return unregisterErr
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
	m.activeMu.RUnlock()
	if runtime == nil {
		return handoff.ErrNoLiveSession
	}
	runtime.presentationMu.Lock()
	select {
	case <-runtime.inboxSlots:
	case <-runtime.done:
		runtime.presentationMu.Unlock()
		return handoff.ErrNoLiveSession
	case <-offerCtx.Done():
		runtime.presentationMu.Unlock()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return ErrControlOverflow
	}
	message := reactorMessage{
		input:        HandoffEventInput{Scope: scope, Event: handoff.CloneEvent(event)},
		ingressAfter: runtime.state.latestIngressSequence(), done: make(chan error, 1),
	}
	runtime.mu.Lock()
	m.activeMu.RLock()
	current := m.current[scope]
	m.activeMu.RUnlock()
	if current != runtime || !runtime.accepting {
		runtime.mu.Unlock()
		runtime.inboxSlots <- struct{}{}
		runtime.presentationMu.Unlock()
		return handoff.ErrNoLiveSession
	}
	select {
	case <-runtime.done:
		runtime.mu.Unlock()
		runtime.inboxSlots <- struct{}{}
		runtime.presentationMu.Unlock()
		return handoff.ErrNoLiveSession
	default:
	}
	runtime.state.acceptControl(&message)
	message.acceptanceChecked = true
	message.acceptanceGeneration = runtime.acceptanceGeneration
	runtime.inbox <- message
	runtime.mu.Unlock()
	runtime.presentationMu.Unlock()
	select {
	case err := <-message.done:
		if (err == nil || err == handoff.ErrPresentationSuppressed) && event.Seq > 0 && m.ReconnectEnabled() {
			if rotateErr := m.rotateReconnectCredential(offerCtx, runtime, event.Seq, false); rotateErr != nil {
				if offerCtx.Err() != nil && ctx.Err() == nil {
					return ErrControlOverflow
				}
				return rotateErr
			}
		}
		return err
	case <-runtime.done:
		select {
		case err := <-message.done:
			return err
		default:
			return handoff.ErrNoLiveSession
		}
	case <-offerCtx.Done():
		select {
		case err := <-message.done:
			return err
		default:
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return ErrControlOverflow
		}
	}
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
		err = s.runtime.acceptClientEvent(ctx, func(sequence uint64) (bool, error) {
			switch typed := event.(type) {
			case protocol.LiveToolPolicyUpdateEvent:
				canonical, canonicalErr := toolpolicy.Canonicalize(typed.Tools, s.cfg.ToolCatalog)
				if canonicalErr != nil {
					return false, canonicalErr
				}
				s.replaceToolPolicy(canonical)
				return true, s.enqueueClientControl(clientControl{event: protocol.LiveToolPolicyUpdatedEvent{
					Type: "live.tool_policy.updated", EventID: typed.EventID, Tools: canonical, Model: s.cfg.AsyncModel,
				}})
			case protocol.LiveToolApprovalResolveEvent:
				if isNilInterface(s.cfg.ApprovalDispatcher) {
					return true, s.enqueueClientControl(clientControl{event: s.approvalProtocolError("approval_unavailable")})
				}
				request := ApprovalResolutionRequestedEffect{
					ExternalSequence: sequence,
					InvocationID:     typed.InvocationID,
					Decision:         handoff.ApprovalDecision(typed.Decision),
					Reason:           typed.Reason,
					IdempotencyKey:   typed.IdempotencyKey,
					ResolvedAt:       s.cfg.Clock.Now().UTC(),
				}
				trusted := TrustedSessionContext{IDs: s.ids, Subject: cloneSubject(s.subject)}
				if dispatchErr := s.cfg.ApprovalDispatcher.DispatchApproval(trusted, request); dispatchErr != nil {
					return true, s.enqueueClientControl(clientControl{event: s.approvalProtocolError("approval_backpressure")})
				}
				// The durable boundary is the app-owned queue admission. Store
				// resolution and optional live UX completion no longer belong to
				// this session incarnation.
				return true, nil
			case protocol.InputAudioBufferAppendEvent:
				pcm, decodeErr := protocol.DecodePCM24k(typed, s.cfg.IngressFrames*pcmFrameBytes)
				if decodeErr != nil {
					return false, decodeErr
				}
				if len(pcm) != 0 {
					s.partialInput.Store(true)
				}
				frames, pushErr := framer.Push(pcm)
				if pushErr != nil {
					return false, pushErr
				}
				for _, audio := range frames {
					latestSequence = s.nextIngressSequence()
					if enqueueErr := s.enqueueIngress(ctx, ingressFrame{sequence: latestSequence, audio: append([]byte(nil), audio...)}); enqueueErr != nil {
						return false, enqueueErr
					}
				}
			case protocol.InputAudioBufferCommitEvent:
				tail, flushErr := framer.Flush()
				if flushErr != nil {
					return true, s.enqueueClientControl(clientControl{event: protocol.ErrorEvent{
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
						return false, enqueueErr
					}
				}
				if enqueueErr := s.enqueueReactor(reactorMessage{input: ClientInput{Event: typed}, ingressAfter: latestSequence}); enqueueErr != nil {
					return false, enqueueErr
				}
				s.partialInput.Store(false)
			default:
				return true, s.enqueueReactor(reactorMessage{input: ClientInput{Event: event}, ingressAfter: latestSequence})
			}
			return true, nil
		})
		if err != nil {
			return err
		}
	}
}

func (s *runState) approvalProtocolError(code string) protocol.ErrorEvent {
	return protocol.ErrorEvent{Type: "error", EventID: s.nextManagerEventID(), Error: protocol.ProtocolError{
		Type: "invalid_request_error", Code: code, Message: "approval resolution was not accepted",
	}}
}

func (r *sessionRuntime) acceptClientEvent(ctx context.Context, accept func(uint64) (bool, error)) error {
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
	applied, err := accept(sequence)
	if err != nil {
		return err
	}
	if applied {
		r.state.markExternalApplied(sequence)
	}
	return nil
}

func (s *runState) markExternalApplied(sequence uint64) {
	if s == nil || sequence == 0 {
		return
	}
	if s.externalProgress == nil {
		s.externalApplied.Store(sequence)
		return
	}
	s.externalProgress.mark(sequence)
	s.externalApplied.Store(s.externalProgress.current())
}

func (s *runState) enqueueIngress(ctx context.Context, frame ingressFrame) error {
	started := s.cfg.Clock.Now()
	timer := time.NewTimer(s.cfg.IngressBlockTimeout)
	defer timer.Stop()
	select {
	case <-s.ingressSlots:
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		s.metrics.QueueBlocked("ingress", nonnegativeDuration(s.cfg.Clock.Now().Sub(started)))
		s.metrics.queueOverflow("ingress")
		return ErrIngressBackpressure
	}
	s.metrics.QueueBlocked("ingress", nonnegativeDuration(s.cfg.Clock.Now().Sub(started)))
	frame.enqueuedAt = s.cfg.Clock.Now()
	s.ingress <- frame
	s.metrics.QueueState("ingress", len(s.ingress), len(s.ingress)*pcmFrameBytes, time.Duration(len(s.ingress))*20*time.Millisecond)
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

func (s *runState) enqueueCompletion(ctx context.Context, input Input) error {
	return s.enqueueCompletionMessage(ctx, reactorMessage{input: input, completion: true})
}

func (s *runState) enqueueCompletionMessage(ctx context.Context, message reactorMessage) error {
	message.completion = true
	select {
	case s.completions <- message:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *runState) trackVoice(command realtime.Command) {
	s.pendingVoiceMu.Lock()
	if _, exists := s.pendingVoiceByID[command.Sequence]; !exists {
		s.pendingVoiceByID[command.Sequence] = command.Kind
		s.pendingVoice.Add(1)
	}
	s.pendingVoiceMu.Unlock()
}

func (s *runState) completeVoice(input VoiceCommandCompletedInput) bool {
	s.pendingVoiceMu.Lock()
	kind, exists := s.pendingVoiceByID[input.Sequence]
	if exists && kind == input.Kind {
		delete(s.pendingVoiceByID, input.Sequence)
		s.pendingVoice.Add(-1)
	}
	s.pendingVoiceMu.Unlock()
	return exists && kind == input.Kind
}

func (s *runState) nextEffectSequence() uint64 {
	return s.effectSequence.Add(1)
}

func (s *runState) registerClientWaiter(sequence uint64, waiter clientEffectWaiter) {
	if sequence == 0 || (waiter.done == nil && waiter.onSuccess == nil) {
		return
	}
	s.clientEffectMu.Lock()
	s.clientWaiters[sequence] = waiter
	s.clientEffectMu.Unlock()
}

func (s *runState) completeClientEffect(input ClientEffectCompletedInput) (clientEffectWaiter, bool) {
	s.clientEffectMu.Lock()
	waiter, ok := s.clientWaiters[input.Sequence]
	if ok {
		delete(s.clientWaiters, input.Sequence)
	}
	s.clientEffectMu.Unlock()
	return waiter, ok
}

func (s *runState) trackDurableEffect(sequence uint64, kind string) {
	s.durableEffectMu.Lock()
	s.pendingDurable[sequence] = kind
	s.durableEffectMu.Unlock()
}

func (s *runState) completeDurableEffect(input DurableEffectCompletedInput) bool {
	s.durableEffectMu.Lock()
	kind, ok := s.pendingDurable[input.Sequence]
	if ok && kind == input.Kind {
		delete(s.pendingDurable, input.Sequence)
	}
	s.durableEffectMu.Unlock()
	return ok && kind == input.Kind
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
			if (control.handoffPresentation && s.cfg.FeatureGates != nil && s.cfg.FeatureGates.Enabled(feature.ShadowMode)) || s.shadowHandoffEpoch(control.command.Epoch) {
				s.releaseVoiceSlot()
				control.barrier.complete(nil)
				if control.sent != nil {
					control.sent <- nil
				}
				if err := s.enqueueCompletion(ctx, VoiceCommandCompletedInput{
					Sequence: control.command.Sequence, Kind: control.command.Kind, Suppressed: true,
				}); err != nil {
					return err
				}
				continue
			}
			err := s.voice.Send(ctx, control.command)
			s.releaseVoiceSlot()
			control.barrier.complete(err)
			if control.sent != nil {
				control.sent <- err
			}
			if err != nil {
				return err
			}
			if err := s.enqueueCompletion(ctx, VoiceCommandCompletedInput{
				Sequence: control.command.Sequence, Kind: control.command.Kind,
			}); err != nil {
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
			s.metrics.QueueOldest("ingress", nonnegativeDuration(s.cfg.Clock.Now().Sub(frame.enqueuedAt)))
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
	select {
	case <-s.voiceReadReady:
	case <-ctx.Done():
		return ctx.Err()
	}
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
			if s.shadowHandoffEpoch(event.Epoch) {
				framer.reset()
				streamHasAudio = false
				streamResponse, streamItem, streamEpoch = "", "", 0
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
		message := reactorMessage{
			input: VoiceInput{Incarnation: s.ids.Incarnation, Event: event}, egressAfter: latest,
		}
		var enqueueErr error
		if event.Kind == realtime.EventResponseCreated || event.Kind == realtime.EventSpeechStarted {
			// Establish a new stream or speech fence before accepting later
			// provider media into the output order. This waits only for pure
			// reactor apply and bounded effect admission, never for provider or
			// client I/O completion.
			enqueueErr = s.enqueueReactorSync(ctx, message)
		} else {
			enqueueErr = s.enqueueReactor(message)
		}
		if enqueueErr != nil {
			return enqueueErr
		}
	}
}

func (s *runState) enqueueEgress(ctx context.Context, event realtime.Event, audio []byte) (bool, error) {
	sequence := s.nextEgressSequence()
	if !s.fence.Accept(event.Epoch) {
		s.progress.mark(sequence)
		s.metrics.audioDropped(len(audio))
		s.metrics.BargeinSuppression(20 * time.Millisecond)
		return false, nil
	}
	timer := time.NewTimer(s.cfg.EgressBlockTimeout)
	started := s.cfg.Clock.Now()
	defer timer.Stop()
	select {
	case <-s.egressSlots:
	case <-ctx.Done():
		s.progress.mark(sequence)
		return false, ctx.Err()
	case <-timer.C:
		s.metrics.QueueBlocked("egress", nonnegativeDuration(s.cfg.Clock.Now().Sub(started)))
		s.progress.mark(sequence)
		s.metrics.queueOverflow("egress")
		return false, ErrEgressBackpressure
	}
	s.metrics.QueueBlocked("egress", nonnegativeDuration(s.cfg.Clock.Now().Sub(started)))
	if !s.fence.Accept(event.Epoch) {
		s.egressSlots <- struct{}{}
		s.progress.mark(sequence)
		s.metrics.audioDropped(len(audio))
		s.metrics.BargeinSuppression(20 * time.Millisecond)
		return false, nil
	}
	s.outputMu.Lock()
	s.outputOrder++
	frame := egressFrame{
		order: s.outputOrder, sequence: sequence, incarnation: s.ids.Incarnation, epoch: event.Epoch,
		eventID:    fmt.Sprintf("session-%s-audio-%06d", s.ids.SessionID, sequence),
		responseID: event.ResponseID, itemID: event.ItemID, audio: append([]byte(nil), audio...), enqueuedAt: s.cfg.Clock.Now(),
	}
	s.egress <- frame
	s.metrics.QueueState("egress", len(s.egress), len(s.egress)*pcmFrameBytes, time.Duration(len(s.egress))*20*time.Millisecond)
	s.outputMu.Unlock()
	return true, nil
}

func (s *runState) reactorLoop(ctx context.Context) error {
	reactor := newReactorWithPresentationAndHandoffTools(s.ids, s.fence, s.cfg.PresentationPolicy, s.cfg.SafetyPolicy, s.cfg.HandoffToolsEnabled)
	reactor.deferPresentationSafety = s.runtime != nil
	var deferredCreate *reactorMessage
	for {
		var message reactorMessage
		select {
		case message = <-s.completions:
		default:
		}
		if message.input != nil {
			// Reserved completions always take priority over ordinary intake.
		} else if deferredCreate != nil && s.pendingVoice.Load() == 0 {
			message = *deferredCreate
			deferredCreate = nil
		} else if deferredCreate != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case message = <-s.completions:
			}
		} else {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case message = <-s.completions:
			case message = <-s.inbox:
			}
		}
		if deferredCreate == nil && s.pendingVoice.Load() != 0 && reactor.Snapshot().Response == ResponseCancelRequested {
			if client, ok := message.input.(ClientInput); ok {
				if _, create := client.Event.(protocol.ResponseCreateEvent); create {
					copy := message
					deferredCreate = &copy
					continue
				}
			}
		}
		{
			presentationLocked := false
			if s.cfg.reactorApplyGate != nil {
				if gate := s.cfg.reactorApplyGate(message.input); gate != nil {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-gate:
					}
				}
			}
			if message.acceptanceChecked && s.runtime != nil {
				s.runtime.presentationMu.Lock()
				presentationLocked = true
				s.runtime.mu.Lock()
				accepted := s.runtime.accepting && s.runtime.acceptanceGeneration == message.acceptanceGeneration
				s.runtime.mu.Unlock()
				if !accepted {
					s.runtime.presentationMu.Unlock()
					if !message.completion {
						s.controlApplied.Store(message.controlSequence)
						s.inboxSlots <- struct{}{}
					}
					if message.done != nil {
						message.done <- handoff.ErrNoLiveSession
					}
					continue
				}
			}
			effects, err := reactor.ApplyContext(ctx, message.input)
			if err == nil {
				for index := range effects {
					async := effects[index].Async
					if async == nil || async.Kind != asyncKindPresentationSafety {
						continue
					}
					var request presentationSafetyRequest
					if decodeErr := json.Unmarshal(async.Payload, &request); decodeErr != nil {
						err = decodeErr
						break
					}
					work := presentationSafetyWork{
						sequence: async.Sequence, request: request, done: message.done,
						acceptanceChecked: message.acceptanceChecked, acceptanceGeneration: message.acceptanceGeneration,
					}
					select {
					case s.safetyWork <- work:
						message.done = nil
						effects[index].Async = nil
					default:
						s.metrics.queueOverflow("presentation_safety")
						err = ErrControlOverflow
					}
					break
				}
			}
			markHandoffPresentationEffects(message.input, effects, s.handoffEpoch.Load())
			policySuppressed := false
			for index := range effects {
				policySuppressed = policySuppressed || effects[index].PolicySuppressed
			}
			if err == nil {
				if message.requireClientWrites {
					for index := range effects {
						if _, ok := effects[index].ClientEvent.(protocol.SessionCreatedEvent); ok {
							effects[index].RequireClientWrite = true
						}
					}
				}
				var clientCompletion uint64
				clientCompletion, err = s.dispatchEffectsQueued(ctx, effects, message.ingressAfter, message.egressAfter, true)
				if err == nil && message.done != nil && clientCompletion != 0 {
					s.registerClientWaiter(clientCompletion, clientEffectWaiter{done: message.done, policySuppressed: policySuppressed})
					message.done = nil
				}
			}
			if err == nil {
				s.boundaryMu.Lock()
				s.boundary = reactor.Snapshot()
				s.boundaryMu.Unlock()
				if completed, ok := message.input.(VoiceCommandCompletedInput); ok {
					s.completeVoice(completed)
				}
			}
			if completed, ok := message.input.(DurableEffectCompletedInput); ok {
				s.completeDurableEffect(completed)
			}
			if completed, ok := message.input.(ApprovalResolutionCompletedInput); ok && err == nil {
				s.markExternalApplied(completed.ExternalSequence)
			}
			if completed, ok := message.input.(ClientEffectCompletedInput); ok {
				if waiter, found := s.completeClientEffect(completed); found {
					completionErr := completed.Err
					if completionErr == nil && waiter.policySuppressed {
						completionErr = handoff.ErrPresentationSuppressed
					}
					if completionErr == nil && waiter.onSuccess != nil {
						waiter.onSuccess()
					}
					if waiter.done != nil {
						waiter.done <- completionErr
					}
				}
			}
			if !message.completion {
				s.controlApplied.Store(message.controlSequence)
				s.inboxSlots <- struct{}{}
			}
			if message.done != nil {
				resultErr := err
				if resultErr == nil && policySuppressed {
					resultErr = handoff.ErrPresentationSuppressed
				}
				message.done <- resultErr
			}
			if presentationLocked {
				s.runtime.presentationMu.Unlock()
			}
			if err != nil {
				return err
			}
		}
	}
}

func markHandoffPresentationEffects(input Input, effects []Effect, handoffEpoch uint64) {
	handoffPresentation := false
	switch typed := input.(type) {
	case HandoffEventInput, HandoffAcceptedInput, HandoffSafetyCompletedInput:
		handoffPresentation = true
	case VoiceInput:
		handoffPresentation = handoffEpoch != 0 && handoffEpoch == uint64(typed.Event.Epoch)
	}
	if !handoffPresentation {
		return
	}
	for index := range effects {
		effects[index].HandoffPresentation = true
	}
}

func (s *runState) dispatchEffects(ctx context.Context, effects []Effect, ingressAfter, egressAfter uint64) error {
	_, err := s.dispatchEffectsQueued(ctx, effects, ingressAfter, egressAfter, false)
	return err
}

func (s *runState) dispatchEffectsQueued(ctx context.Context, effects []Effect, ingressAfter, egressAfter uint64, queueDurable bool) (uint64, error) {
	var clientCompletion uint64
	var voiceBarrier *voiceSendBarrier
	for _, effect := range effects {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		// These are the final visual and speech side-effect boundaries. Each
		// effect re-reads the gate so a transition during dispatch cannot leak.
		if effect.HandoffPresentation && s.cfg.FeatureGates != nil && s.cfg.FeatureGates.Enabled(feature.ShadowMode) {
			if effect.VoiceCommand != nil && effect.VoiceCommand.Kind == realtime.CommandCreateResponse {
				if err := s.enqueueCompletion(ctx, VoiceCommandCompletedInput{Sequence: effect.VoiceCommand.Sequence, Kind: effect.VoiceCommand.Kind, Suppressed: true}); err != nil {
					return 0, err
				}
			}
			continue
		}
		if effect.FlushBefore != 0 {
			s.activeWrite.cancelActive()
			s.activeControlWrite.cancelActive()
		}
		if effect.ClientEvent != nil {
			control := clientControl{afterSeq: egressAfter, afterVoice: voiceBarrier, event: effect.ClientEvent, writeCtx: ctx, epoch: effect.ResponseEpoch, incarnation: effect.Incarnation,
				completionSequence: s.nextEffectSequence()}
			if effect.RequireClientWrite {
				clientCompletion = control.completionSequence
			}
			if err := s.enqueueClientControl(control); err != nil {
				return 0, err
			}
		}
		if effect.Close != nil {
			closeCopy := *effect.Close
			if err := s.enqueueClientControl(clientControl{afterSeq: egressAfter, close: &closeCopy}); err != nil {
				return 0, err
			}
		}
		if effect.VoiceCommand != nil {
			command := *effect.VoiceCommand
			if effect.HandoffPresentation && command.Kind == realtime.CommandCreateResponse && command.Epoch != 0 {
				s.handoffEpoch.Store(uint64(command.Epoch))
			}
			if command.Kind != realtime.CommandCancelResponse && command.Kind != realtime.CommandBindResponseEpoch {
				command.AfterSeq = ingressAfter
			}
			select {
			case <-s.voiceSlots:
			default:
				s.metrics.queueOverflow("voice_control")
				return 0, ErrControlOverflow
			}
			voiceBarrier = newVoiceSendBarrier()
			control := voiceControl{command: command, barrier: voiceBarrier, handoffPresentation: effect.HandoffPresentation}
			s.trackVoice(command)
			s.voiceCtl <- control
		}
		if effect.Transcript != nil || effect.Handoff != nil {
			trusted := TrustedSessionContext{IDs: s.ids, Subject: cloneSubject(s.subject)}
			if queueDurable {
				select {
				case <-s.durableSlots:
				default:
					s.metrics.queueOverflow("durable_effect")
					return 0, ErrControlOverflow
				}
				control := durableEffectControl{trusted: trusted, sequence: s.nextEffectSequence()}
				if effect.Transcript != nil {
					owned := *effect.Transcript
					control.transcript = &owned
					control.kind = "transcript"
				}
				if effect.Handoff != nil {
					if isNilInterface(s.dispatcher) {
						s.durableSlots <- struct{}{}
						return 0, fmt.Errorf("%w: durable handoff effect", ErrUnsupportedAsync)
					}
					owned := *effect.Handoff
					owned.Arguments = append(json.RawMessage(nil), effect.Handoff.Arguments...)
					owned.ToolPolicy = s.toolPolicySnapshot()
					control.handoff = &owned
					if control.kind == "" {
						control.kind = "handoff"
					} else {
						control.kind = "transcript+handoff"
					}
				}
				s.trackDurableEffect(control.sequence, control.kind)
				s.durableCtl <- control
				continue
			}
			if effect.Transcript != nil && !isNilInterface(s.dispatcher) {
				owned := *effect.Transcript
				if err := s.dispatcher.DispatchTranscript(ctx, trusted, owned); err != nil {
					return 0, err
				}
			}
			if effect.Handoff != nil {
				if isNilInterface(s.dispatcher) {
					return 0, fmt.Errorf("%w: durable handoff effect", ErrUnsupportedAsync)
				}
				owned := *effect.Handoff
				owned.Arguments = append(json.RawMessage(nil), effect.Handoff.Arguments...)
				owned.ToolPolicy = s.toolPolicySnapshot()
				if err := s.dispatcher.DispatchHandoff(ctx, trusted, owned); err != nil {
					return 0, err
				}
			}
		}
		if effect.Async != nil && effect.Handoff == nil {
			return 0, fmt.Errorf("%w: %s", ErrUnsupportedAsync, effect.Async.Kind)
		}
	}
	return clientCompletion, nil
}

func (s *runState) durableWriter(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case control := <-s.durableCtl:
			var dispatchErr error
			if control.transcript != nil && !isNilInterface(s.dispatcher) {
				dispatchErr = s.dispatcher.DispatchTranscript(ctx, control.trusted, *control.transcript)
			}
			if dispatchErr == nil && control.handoff != nil {
				dispatchErr = s.dispatcher.DispatchHandoff(ctx, control.trusted, *control.handoff)
			}
			s.durableSlots <- struct{}{}
			if err := s.enqueueCompletion(ctx, DurableEffectCompletedInput{
				Sequence: control.sequence, Kind: control.kind, Err: dispatchErr,
			}); err != nil {
				return err
			}
			if dispatchErr != nil {
				return nil
			}
		}
	}
}

func (s *runState) presentationSafetyLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case work := <-s.safetyWork:
			visual := safety.EvaluateOrSuppress(ctx, s.cfg.SafetyPolicy, work.request.Visual)
			if err := ctx.Err(); err != nil {
				return err
			}
			var speech safety.Decision
			if work.request.Speech != nil {
				speech = safety.EvaluateOrSuppress(ctx, s.cfg.SafetyPolicy, *work.request.Speech)
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			if err := s.enqueueCompletionMessage(ctx, reactorMessage{
				input: HandoffSafetyCompletedInput{Sequence: work.sequence, Visual: visual, Speech: speech},
				done:  work.done, acceptanceChecked: work.acceptanceChecked, acceptanceGeneration: work.acceptanceGeneration,
			}); err != nil {
				return err
			}
		}
	}
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

func (s *runState) enqueueClientEffectCompletion(ctx context.Context, control clientControl, effectErr error) error {
	if control.completionSequence == 0 {
		return nil
	}
	return s.enqueueCompletion(ctx, ClientEffectCompletedInput{Sequence: control.completionSequence, Err: effectErr})
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
				if control.epoch != 0 && (control.incarnation != s.ids.Incarnation || !s.fence.Accept(control.epoch)) {
					s.releaseClientSlot()
					if err := s.enqueueClientEffectCompletion(ctx, control, nil); err != nil {
						return err
					}
					continue
				}
				writeCtx := ctx
				if control.writeCtx != nil {
					writeCtx = control.writeCtx
				}
				if err := control.afterVoice.wait(writeCtx); err != nil {
					s.releaseClientSlot()
					if completionErr := s.enqueueClientEffectCompletion(ctx, control, err); completionErr != nil {
						return completionErr
					}
					return nil
				}
				if err := writeCtx.Err(); err != nil {
					s.releaseClientSlot()
					if completionErr := s.enqueueClientEffectCompletion(ctx, control, err); completionErr != nil {
						return completionErr
					}
					return nil
				}
				finish := func() {}
				if control.epoch != 0 {
					writeCtx, finish = s.activeControlWrite.begin(writeCtx)
				}
				if control.epoch != 0 && (control.incarnation != s.ids.Incarnation || !s.fence.Accept(control.epoch)) {
					finish()
					s.releaseClientSlot()
					if err := s.enqueueClientEffectCompletion(ctx, control, nil); err != nil {
						return err
					}
					continue
				}
				if (strings.HasPrefix(control.event.EventType(), "live.handoff.") || s.shadowHandoffEpoch(control.epoch)) && s.cfg.FeatureGates != nil && s.cfg.FeatureGates.Enabled(feature.ShadowMode) {
					finish()
					s.releaseClientSlot()
					if err := s.enqueueClientEffectCompletion(ctx, control, nil); err != nil {
						return err
					}
					continue
				}
				err := s.client.Write(writeCtx, control.event)
				finish()
				if err != nil {
					s.releaseClientSlot()
					if errors.Is(err, context.Canceled) && control.epoch != 0 && !s.fence.Accept(control.epoch) {
						if completionErr := s.enqueueClientEffectCompletion(ctx, control, nil); completionErr != nil {
							return completionErr
						}
						continue
					}
					if completionErr := s.enqueueClientEffectCompletion(ctx, control, err); completionErr != nil {
						return completionErr
					}
					return nil
				}
				s.releaseClientSlot()
				if err := s.enqueueClientEffectCompletion(ctx, control, nil); err != nil {
					return err
				}
				continue
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
			s.metrics.QueueOldest("egress", nonnegativeDuration(s.cfg.Clock.Now().Sub(frame.enqueuedAt)))
			pendingFrame = nil
			nextOrder++
			if frame.incarnation != s.ids.Incarnation || !s.fence.Accept(frame.epoch) || s.shadowHandoffEpoch(frame.epoch) {
				s.metrics.StaleOutputRejected()
				s.progress.mark(frame.sequence)
				s.metrics.audioDropped(len(frame.audio))
				s.releaseEgressSlot()
				continue
			}
			writeCtx, finish := s.activeWrite.begin(ctx)
			if frame.incarnation != s.ids.Incarnation || !s.fence.Accept(frame.epoch) || s.shadowHandoffEpoch(frame.epoch) {
				s.metrics.StaleOutputRejected()
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
			s.metrics.AudioTransit(s.cfg.Clock.Now().Sub(frame.enqueuedAt))
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

func (s *runState) shadowHandoffEpoch(epoch identity.ResponseEpoch) bool {
	return s != nil && epoch != 0 && s.handoffEpoch.Load() == uint64(epoch) && s.cfg.FeatureGates != nil && s.cfg.FeatureGates.Enabled(feature.ShadowMode)
}

func (s *runState) releaseVoiceSlot() {
	if s.voiceSlots != nil {
		s.voiceSlots <- struct{}{}
	}
}

func (s *runState) releaseIngressSlot() {
	if s.ingressSlots != nil {
		s.ingressSlots <- struct{}{}
		s.metrics.QueueState("ingress", len(s.ingress), len(s.ingress)*pcmFrameBytes, time.Duration(len(s.ingress))*20*time.Millisecond)
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
		s.metrics.QueueState("egress", len(s.egress), len(s.egress)*pcmFrameBytes, time.Duration(len(s.egress))*20*time.Millisecond)
	}
}

func nonnegativeDuration(d time.Duration) time.Duration {
	if d < 0 {
		return 0
	}
	return d
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
