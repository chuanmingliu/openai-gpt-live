package session

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/protocol"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
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
	Close(code int, reason string) error
}

type Config struct {
	IngressFrames       int
	EgressFrames        int
	ControlEvents       int
	WriterControlEvents int
	IngressBlockTimeout time.Duration
	EgressBlockTimeout  time.Duration
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
	metricsMu     sync.RWMutex
	metrics       MetricsHooks
}

type runtimeKey struct {
	sessionID   identity.SessionID
	incarnation identity.Incarnation
}

type sessionRuntime struct {
	key     runtimeKey
	subject Subject
	inbox   chan reactorMessage
	done    <-chan struct{}
	state   *runState

	mu         sync.Mutex
	accepting  bool
	inboxSlots chan struct{}
}

func (r *sessionRuntime) stop() {
	r.mu.Lock()
	r.accepting = false
	r.mu.Unlock()
}

type reactorMessage struct {
	input        Input
	ingressAfter uint64
	egressAfter  uint64
	done         chan error
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

	ingressSequence uint64
	egressSequence  uint64
	eventSequence   uint64
	sequenceMu      sync.Mutex

	outputMu    sync.Mutex
	outputOrder uint64

	closeClientOnce sync.Once
	closeClientErr  error
	graceful        atomic.Bool

	cancel context.CancelFunc
}

func NewManager(cfg Config, connector realtime.Connector, ids IDGenerator) *Manager {
	normalized, err := normalizeManagerConfig(cfg)
	return &Manager{
		cfg:           normalized,
		connector:     connector,
		ids:           ids,
		validationErr: err,
		active:        make(map[runtimeKey]*sessionRuntime),
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

	sessionCtx, cancel := context.WithDeadline(parent, subject.ExpiresAt)
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
	state := newRunState(m.cfg, ids, voice, client, cancel)
	state.metrics = m.metricsSnapshot()
	runtime := &sessionRuntime{
		key:        runtimeKey{sessionID: sessionID, incarnation: ids.Incarnation},
		subject:    subject,
		inbox:      state.inbox,
		done:       groupCtx.Done(),
		state:      state,
		accepting:  true,
		inboxSlots: state.inboxSlots,
	}
	if err := m.register(runtime); err != nil {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), internalCloseTimeout)
		defer closeCancel()
		_ = voice.Close(closeCtx)
		return err
	}
	defer m.unregister(runtime.key, runtime)

	<-state.inboxSlots
	state.inbox <- reactorMessage{input: ConnectedInput{ProviderSession: json.RawMessage(`{}`)}}

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

	waitErr := group.Wait()
	cancel()
	state.closeClient(1000, "session ended")
	closeCtx, closeCancel := context.WithTimeout(context.Background(), internalCloseTimeout)
	closeErr := voice.Close(closeCtx)
	closeCancel()

	if firstErr != nil {
		return firstErr
	}
	cleanupErr := errors.Join(state.closeClientErr, closeErr)
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

func newRunState(cfg Config, ids IDs, voice realtime.Session, client ClientConn, cancel context.CancelFunc) *runState {
	state := &runState{
		cfg:          cfg,
		ids:          ids,
		voice:        voice,
		client:       client,
		fence:        NewEpochFence(0),
		inbox:        make(chan reactorMessage, cfg.ControlEvents),
		ingress:      make(chan ingressFrame, cfg.IngressFrames),
		ingressSlots: make(chan struct{}, cfg.IngressFrames),
		voiceCtl:     make(chan voiceControl, cfg.WriterControlEvents),
		egress:       make(chan egressFrame, cfg.EgressFrames),
		clientCtl:    make(chan clientControl, cfg.WriterControlEvents),
		inboxSlots:   make(chan struct{}, cfg.ControlEvents),
		voiceSlots:   make(chan struct{}, cfg.WriterControlEvents),
		clientSlots:  make(chan struct{}, cfg.WriterControlEvents),
		egressSlots:  make(chan struct{}, cfg.EgressFrames),
		progress:     newSequenceProgress(),
		activeWrite:  &activeAudioWrite{},
		metrics:      normalizeMetricsHooks(MetricsHooks{}),
		cancel:       cancel,
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
	m.active[runtime.key] = runtime
	return nil
}

func (m *Manager) unregister(key runtimeKey, runtime *sessionRuntime) {
	m.activeMu.Lock()
	if m.active[key] == runtime {
		delete(m.active, key)
	}
	m.activeMu.Unlock()
}

func (s *runState) clientReader(ctx context.Context) error {
	framer := protocol.NewPCMFramer()
	var latestSequence uint64
	for {
		event, err := s.client.Read(ctx)
		if err != nil {
			return err
		}
		switch typed := event.(type) {
		case protocol.InputAudioBufferAppendEvent:
			pcm, err := protocol.DecodePCM24k(typed, s.cfg.IngressFrames*pcmFrameBytes)
			if err != nil {
				return err
			}
			frames, err := framer.Push(pcm)
			if err != nil {
				return err
			}
			for _, audio := range frames {
				latestSequence = s.nextIngressSequence()
				if err := s.enqueueIngress(ctx, ingressFrame{sequence: latestSequence, audio: append([]byte(nil), audio...)}); err != nil {
					return err
				}
			}
		case protocol.InputAudioBufferCommitEvent:
			tail, err := framer.Flush()
			if err != nil {
				if queueErr := s.enqueueClientControl(clientControl{event: protocol.ErrorEvent{
					Type:    "error",
					EventID: s.nextManagerEventID(),
					Error: protocol.ProtocolError{
						Type: "invalid_request_error", Code: "invalid_audio_commit", Message: err.Error(),
					},
				}}); queueErr != nil {
					return queueErr
				}
				continue
			}
			if len(tail) != 0 {
				latestSequence = s.nextIngressSequence()
				if err := s.enqueueIngress(ctx, ingressFrame{sequence: latestSequence, audio: append([]byte(nil), tail...)}); err != nil {
					return err
				}
			}
			if err := s.enqueueReactor(reactorMessage{input: ClientInput{Event: typed}, ingressAfter: latestSequence}); err != nil {
				return err
			}
		default:
			if err := s.enqueueReactor(reactorMessage{input: ClientInput{Event: event}, ingressAfter: latestSequence}); err != nil {
				return err
			}
		}
	}
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
	s.inbox <- message
	return nil
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
	reactor := NewReactor(s.ids, s.fence)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case message := <-s.inbox:
			effects, err := reactor.Apply(message.input)
			if err == nil {
				err = s.dispatchEffects(ctx, effects, message.ingressAfter, message.egressAfter)
			}
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

func (s *runState) dispatchEffects(ctx context.Context, effects []Effect, ingressAfter, egressAfter uint64) error {
	for _, effect := range effects {
		if effect.FlushBefore != 0 {
			if err := s.activeWrite.cancelAndWait(); err != nil {
				return err
			}
		}
	}
	for _, effect := range effects {
		if effect.ClientEvent != nil {
			if err := s.enqueueClientControl(clientControl{afterSeq: egressAfter, event: effect.ClientEvent}); err != nil {
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
	for _, effect := range effects {
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
		s.voiceCtl <- voiceControl{command: command}
	}
	for _, effect := range effects {
		if effect.Async != nil {
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
				if err := s.client.Write(ctx, control.event); err != nil {
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
	s.closeClientOnce.Do(func() {
		s.closeClientErr = s.client.Close(code, reason)
	})
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
	if subject.ExpiresAt.IsZero() || !subject.ExpiresAt.After(time.Now()) {
		return errors.New("subject is expired")
	}
	return nil
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
