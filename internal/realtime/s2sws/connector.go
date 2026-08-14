// Package s2sws adapts an OpenAI-compatible Realtime WebSocket provider to
// the provider-neutral realtime voice contracts.
package s2sws

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/coder/websocket"
)

const (
	receiveQueueCapacity     = 256
	writeQueueCapacity       = 64
	correlationQueueCapacity = 64
	explicitPendingLimit     = 64
	responseHistoryLimit     = 1024
	implicitEventLimit       = 16
	implicitByteLimit        = 2 << 20
	implicitAggregateEvents  = 64
	implicitAggregateBytes   = 8 << 20
	implicitUnboundLimit     = 8
)

var (
	ErrUnknownResponseEpoch  = errors.New("unknown response epoch")
	ErrImplicitBufferFull    = errors.New("implicit response buffer limit exceeded")
	ErrImplicitAggregateFull = errors.New("implicit response aggregate buffer limit exceeded")
	ErrImplicitResponseLimit = errors.New("implicit unbound response limit exceeded")
	ErrPendingResponseLimit  = errors.New("pending explicit response limit exceeded")
	ErrAcceptedEventLimit    = errors.New("accepted event queue limit exceeded")
	ErrSessionClosed         = errors.New("realtime WebSocket session closed")
)

type Config struct {
	URL            string
	APIKey         string
	Model          string
	ConnectTimeout time.Duration
	MaxFrameBytes  int64
}

type Connector struct {
	cfg      Config
	endpoint *url.URL
}

func New(cfg Config) (*Connector, error) {
	endpoint, err := url.Parse(cfg.URL)
	if err != nil || endpoint.Host == "" || (endpoint.Scheme != "ws" && endpoint.Scheme != "wss") ||
		endpoint.User != nil || endpoint.Opaque != "" || endpoint.OmitHost || endpoint.RawQuery != "" ||
		endpoint.ForceQuery || endpoint.Fragment != "" || endpoint.RawFragment != "" {
		return nil, errors.New("URL must be an absolute ws or wss endpoint")
	}
	if cfg.ConnectTimeout <= 0 {
		return nil, errors.New("connect timeout must be positive")
	}
	if cfg.MaxFrameBytes <= 0 {
		return nil, errors.New("maximum frame size must be positive")
	}
	endpointCopy := *endpoint
	return &Connector{cfg: cfg, endpoint: &endpointCopy}, nil
}

func (c *Connector) Connect(ctx context.Context, request realtime.OpenRequest) (realtime.Session, error) {
	if c == nil || c.endpoint == nil {
		return nil, errors.New("connector is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	model := c.cfg.Model
	if request.Model != "" {
		model = request.Model
	}
	if model == "" {
		return nil, errors.New("realtime model is required")
	}

	endpoint := *c.endpoint
	query := endpoint.Query()
	query.Set("model", model)
	endpoint.RawQuery = query.Encode()
	header := make(http.Header)
	if c.cfg.APIKey != "" {
		header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	}
	dialContext, cancelDial := context.WithTimeout(ctx, c.cfg.ConnectTimeout)
	defer cancelDial()
	conn, _, err := websocket.Dial(dialContext, endpoint.String(), &websocket.DialOptions{HTTPHeader: header})
	if err != nil {
		return nil, fmt.Errorf("connect realtime WebSocket: %w", err)
	}
	conn.SetReadLimit(c.cfg.MaxFrameBytes)

	sessionContext, cancelSession := context.WithCancel(context.Background())
	session := &voiceSession{
		conn:           conn,
		ctx:            sessionContext,
		cancel:         cancelSession,
		writeMessage:   conn.Write,
		closeHandshake: conn.Close,
		closeNow:       conn.CloseNow,
		maxFrameBytes:  c.cfg.MaxFrameBytes,
		writes:         make(chan writeRequest, writeQueueCapacity),
		closeWrites:    make(chan struct{}, 1),
		reads:          make(chan socketRead, 1),
		correlations:   make(chan correlationRequest, correlationQueueCapacity),
		events:         make(chan realtime.Event, receiveQueueCapacity),
		closing:        make(chan struct{}),
		closeDone:      make(chan struct{}),
		done:           make(chan struct{}),
		responses:      make(map[string]*responseCorrelation),
		pending:        make(map[identity.ResponseEpoch]struct{}),
	}
	go session.readLoop()
	go session.mappingLoop()
	go session.writeLoop()
	return session, nil
}

type writeRequest struct {
	ctx     context.Context
	command realtime.Command
	result  chan error
}

type socketRead struct {
	messageType websocket.MessageType
	raw         []byte
	err         error
}

type correlationKind uint8

const (
	correlationRegister correlationKind = iota + 1
	correlationBind
	correlationCancel
)

type correlationRequest struct {
	kind       correlationKind
	ctx        context.Context
	epoch      identity.ResponseEpoch
	responseID string
	result     chan error
}

type bufferedResponseEvent struct {
	event        realtime.Event
	rawBytes     int
	decodedBytes int
	ownedBytes   int
}

type responseCorrelation struct {
	epoch         identity.ResponseEpoch
	cancelEpoch   identity.ResponseEpoch
	bound         bool
	terminal      bool
	buffered      []bufferedResponseEvent
	bufferedBytes int
	unbound       bool
}

type voiceSession struct {
	conn           *websocket.Conn
	ctx            context.Context
	cancel         context.CancelFunc
	writeMessage   func(context.Context, websocket.MessageType, []byte) error
	closeHandshake func(websocket.StatusCode, string) error
	closeNow       func() error
	maxFrameBytes  int64

	writes       chan writeRequest
	closeWrites  chan struct{}
	reads        chan socketRead
	correlations chan correlationRequest
	events       chan realtime.Event
	closing      chan struct{}
	closeDone    chan struct{}
	done         chan struct{}

	responses     map[string]*responseCorrelation
	responseOrder []string
	pending       map[identity.ResponseEpoch]struct{}

	implicitUnbound        int
	implicitBufferedEvents int
	implicitBufferedBytes  int
	mappedEvents           []realtime.Event
	mappingActive          bool

	terminalOnce        sync.Once
	terminalMu          sync.RWMutex
	terminalErr         error
	closeOnce           sync.Once
	closeCompleteOnce   sync.Once
	closeStateMu        sync.Mutex
	terminalHandedOff   bool
	closeResultMu       sync.RWMutex
	closeResult         error
	activeWriteMu       sync.Mutex
	activeWriteCancel   context.CancelFunc
	terminalRequestOnce sync.Once
	terminalRequests    chan error
}

func (s *voiceSession) Send(ctx context.Context, command realtime.Command) error {
	if ctx == nil {
		return errors.New("send context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-s.closing:
		return ErrSessionClosed
	default:
	}
	request := writeRequest{ctx: ctx, command: cloneCommand(command), result: make(chan error, 1)}
	select {
	case s.writes <- request:
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closing:
		return ErrSessionClosed
	case <-s.done:
		return s.sessionError()
	}
	return s.waitForSendResult(ctx, request.result)
}

func (s *voiceSession) waitForSendResult(ctx context.Context, result <-chan error) error {
	select {
	case err := <-result:
		return s.normalizeSendResult(err)
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closing:
		return ErrSessionClosed
	case <-s.done:
		select {
		case err := <-result:
			return s.normalizeSendResult(err)
		default:
			return s.sessionError()
		}
	}
}

func (s *voiceSession) normalizeSendResult(err error) error {
	if s.isClosing() {
		return ErrSessionClosed
	}
	return err
}

func (s *voiceSession) Receive(ctx context.Context) (realtime.Event, error) {
	if ctx == nil {
		return realtime.Event{}, errors.New("receive context is nil")
	}
	for {
		closing := s.closing
		if s.terminalHandoffActive() {
			// A writer failure owns terminal state even if Close arrives while
			// its accepted FIFO tail is draining. Keep that tail receivable;
			// Send still observes closing and rejects new work.
			closing = nil
		}
		select {
		case event := <-s.events:
			return cloneEvent(event), nil
		default:
		}
		select {
		case event := <-s.events:
			return cloneEvent(event), nil
		case <-ctx.Done():
			return realtime.Event{}, ctx.Err()
		case <-closing:
			if s.terminalHandoffActive() {
				continue
			}
			return realtime.Event{}, ErrSessionClosed
		case <-s.done:
			select {
			case event := <-s.events:
				return cloneEvent(event), nil
			default:
				return realtime.Event{}, s.sessionError()
			}
		}
	}
}

func (s *voiceSession) Close(ctx context.Context) error {
	if ctx == nil {
		return errors.New("close context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-s.closeDone:
		return s.sharedCloseResult()
	default:
	}
	switch s.startClose() {
	case closeAlreadyComplete:
		return nil
	case closeWaitTerminal:
		select {
		case <-s.done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	default:
		select {
		case <-s.closeDone:
			return s.sharedCloseResult()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

type closeStartMode uint8

const (
	closeAlreadyComplete closeStartMode = iota
	closeWaitHandshake
	closeWaitTerminal
)

func (s *voiceSession) startClose() closeStartMode {
	s.closeStateMu.Lock()
	defer s.closeStateMu.Unlock()

	if s.terminalHandedOff {
		s.closeOnce.Do(func() {
			close(s.closing)
			s.cancelActiveWrite()
		})
		return closeWaitTerminal
	}
	if s.isClosing() {
		return closeWaitHandshake
	}
	select {
	case <-s.done:
		return closeAlreadyComplete
	default:
	}
	s.closeOnce.Do(func() {
		close(s.closing)
		s.closeWrites <- struct{}{}
		s.cancelActiveWrite()
	})
	return closeWaitHandshake
}

func (s *voiceSession) readLoop() {
	for {
		messageType, raw, err := s.conn.Read(s.ctx)
		result := socketRead{messageType: messageType, raw: raw, err: err}
		select {
		case s.reads <- result:
		case <-s.done:
			return
		}
		if err != nil {
			return
		}
	}
}

func (s *voiceSession) writeLoop() {
	for {
		select {
		case <-s.closeWrites:
			s.finishClose()
			return
		default:
		}
		select {
		case <-s.closeWrites:
			s.finishClose()
			return
		case request := <-s.writes:
			select {
			case <-s.closing:
				request.result <- ErrSessionClosed
				continue
			default:
			}
			if err := request.ctx.Err(); err != nil {
				request.result <- err
				continue
			}
			if request.command.Kind == realtime.CommandBindResponseEpoch {
				err := s.correlate(request.ctx, correlationRequest{kind: correlationBind, epoch: request.command.Epoch, responseID: request.command.ResponseID})
				request.result <- err
				continue
			}

			raw, err := marshalCommand(request.command)
			if err == nil && int64(len(raw)) > s.maxFrameBytes {
				err = fmt.Errorf("outbound frame is %d bytes, limit is %d", len(raw), s.maxFrameBytes)
			}
			if err == nil && request.command.Kind == realtime.CommandCreateResponse {
				err = s.correlate(request.ctx, correlationRequest{kind: correlationRegister, epoch: request.command.Epoch})
			}
			if err == nil && request.command.Kind == realtime.CommandCancelResponse && request.command.ResponseID != "" {
				err = s.correlate(request.ctx, correlationRequest{
					kind: correlationCancel, epoch: request.command.Epoch, responseID: request.command.ResponseID,
				})
			}
			attemptedWrite := false
			if err == nil {
				writeContext, finishWrite, ok := s.beginWrite(request.ctx)
				if !ok {
					err = ErrSessionClosed
				} else {
					attemptedWrite = true
					err = s.writeMessage(writeContext, websocket.MessageText, raw)
					finishWrite()
				}
			}
			request.result <- err
			if attemptedWrite && err != nil {
				if s.beginTerminalHandoff(err) {
					return
				}
			}
		case <-s.done:
			if s.takeCloseAfterTermination() {
				s.finishClose()
			}
			return
		}
	}
}

// beginTerminalHandoff and startClose share closeStateMu so exactly one path
// wins: Close either queues work to a live writer, or observes that terminal
// ownership has moved to the mapping loop and waits for its ordered drain.
func (s *voiceSession) beginTerminalHandoff(err error) bool {
	s.closeStateMu.Lock()
	defer s.closeStateMu.Unlock()
	if s.isClosing() {
		return false
	}
	s.terminalHandedOff = true
	s.terminate(err)
	return true
}

func (s *voiceSession) terminalHandoffActive() bool {
	s.closeStateMu.Lock()
	active := s.terminalHandedOff
	s.closeStateMu.Unlock()
	return active
}

// takeCloseAfterTermination synchronizes a terminal writer exit with close
// initiation. If it finds no request while holding closeStateMu, future Close
// calls will observe done and cannot enqueue one after the writer exits.
func (s *voiceSession) takeCloseAfterTermination() bool {
	s.closeStateMu.Lock()
	defer s.closeStateMu.Unlock()
	select {
	case <-s.closeWrites:
		return true
	default:
		return false
	}
}

func (s *voiceSession) finishClose() {
drain:
	for {
		select {
		case queued := <-s.writes:
			queued.result <- ErrSessionClosed
		default:
			break drain
		}
	}
	err := s.closeHandshake(websocket.StatusNormalClosure, "")
	if errors.Is(err, net.ErrClosed) {
		err = nil
	}
	s.publishTerminal(ErrSessionClosed)
	s.completeClose(err)
}

func (s *voiceSession) completeClose(err error) {
	s.closeCompleteOnce.Do(func() {
		s.closeResultMu.Lock()
		s.closeResult = err
		s.closeResultMu.Unlock()
		close(s.closeDone)
	})
}

func (s *voiceSession) sharedCloseResult() error {
	s.closeResultMu.RLock()
	err := s.closeResult
	s.closeResultMu.RUnlock()
	return err
}

func (s *voiceSession) beginWrite(parent context.Context) (context.Context, func(), bool) {
	writeContext, cancel := context.WithCancel(parent)
	s.activeWriteMu.Lock()
	if s.isClosing() {
		s.activeWriteMu.Unlock()
		cancel()
		return nil, nil, false
	}
	s.activeWriteCancel = cancel
	s.activeWriteMu.Unlock()
	finish := func() {
		s.activeWriteMu.Lock()
		s.activeWriteCancel = nil
		s.activeWriteMu.Unlock()
		cancel()
	}
	return writeContext, finish, true
}

func (s *voiceSession) cancelActiveWrite() {
	s.activeWriteMu.Lock()
	if s.activeWriteCancel != nil {
		s.activeWriteCancel()
	}
	s.activeWriteMu.Unlock()
}

func (s *voiceSession) isClosing() bool {
	select {
	case <-s.closing:
		return true
	default:
		return false
	}
}

func (s *voiceSession) correlate(ctx context.Context, request correlationRequest) error {
	request.ctx = ctx
	request.result = make(chan error, 1)
	select {
	case s.correlations <- request:
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closing:
		return ErrSessionClosed
	case <-s.done:
		return s.sessionError()
	}
	select {
	case err := <-request.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closing:
		return ErrSessionClosed
	case <-s.done:
		return s.sessionError()
	}
}

func (s *voiceSession) mappingLoop() {
	s.mappingActive = true
	var terminalReadErr error
	terminalRequests := s.terminationRequests()
	defer func() {
		s.mappingActive = false
		s.releaseAllImplicitBuffers()
		for index := range s.mappedEvents {
			zeroRealtimeEvent(&s.mappedEvents[index])
		}
		s.mappedEvents = nil
	}()
	for {
		if terminalReadErr != nil && len(s.mappedEvents) == 0 {
			s.publishTerminal(terminalReadErr)
			return
		}
		var reads <-chan socketRead
		// Bound all accepted-but-unreceived events together. This guarantees
		// the public queue has room for the mapping-owned FIFO after terminal
		// intake stops, even when no receiver remains to make progress.
		if terminalReadErr == nil && len(s.events)+len(s.mappedEvents)+s.implicitBufferedEvents < receiveQueueCapacity {
			reads = s.reads
		}
		var deliveries chan<- realtime.Event
		var next realtime.Event
		if len(s.mappedEvents) != 0 {
			deliveries = s.events
			next = s.mappedEvents[0]
		}
		select {
		case err := <-terminalRequests:
			if terminalReadErr == nil {
				terminalReadErr = err
			}
		case read := <-reads:
			if read.err != nil {
				terminalReadErr = read.err
				continue
			}
			if read.messageType != websocket.MessageText {
				terminalReadErr = errors.New("provider sent non-text WebSocket message")
				continue
			}
			mapped, err := mapServerEvent(read.raw, s.maxFrameBytes)
			if err != nil {
				terminalReadErr = err
				continue
			}
			if mapped.ignore {
				continue
			}
			acceptedEvents := len(s.events) + len(s.mappedEvents) + s.implicitBufferedEvents
			if expansion := s.acceptedEventExpansion(mapped); expansion > receiveQueueCapacity-acceptedEvents {
				terminalReadErr = fmt.Errorf("%w: maximum %d", ErrAcceptedEventLimit, receiveQueueCapacity)
				continue
			}
			if err := s.handleMapped(mapped); err != nil {
				terminalReadErr = err
				continue
			}
		case request := <-s.correlations:
			if err := request.ctx.Err(); err != nil {
				request.result <- err
				continue
			}
			var err error
			switch request.kind {
			case correlationRegister:
				err = s.registerExplicit(request.epoch)
			case correlationBind:
				err = s.bindImplicit(request)
			case correlationCancel:
				err = s.markCancelled(request)
			default:
				err = errors.New("unknown correlation request")
			}
			request.result <- err
		case deliveries <- next:
			// Ownership of the event's backing storage moved to the public
			// channel. Detach the queue slot without scrubbing transferred data.
			s.mappedEvents[0] = realtime.Event{}
			s.mappedEvents = s.mappedEvents[1:]
			if len(s.mappedEvents) == 0 {
				s.mappedEvents = nil
			}
		case <-s.done:
			return
		}
	}
}

func (s *voiceSession) acceptedEventExpansion(mapped mappedServerEvent) int {
	if mapped.responseSpecific && mapped.event.Kind == realtime.EventTranscriptDone && len(s.pending) == 0 {
		if _, exists := s.responses[mapped.event.ResponseID]; !exists {
			return 2
		}
	}
	return 1
}

func (s *voiceSession) registerExplicit(epoch identity.ResponseEpoch) error {
	if epoch == 0 {
		return errors.New("response epoch must be positive")
	}
	if _, exists := s.pending[epoch]; exists {
		return fmt.Errorf("response epoch %d is already pending", epoch)
	}
	if len(s.pending) >= explicitPendingLimit {
		return fmt.Errorf("%w: maximum %d", ErrPendingResponseLimit, explicitPendingLimit)
	}
	s.pending[epoch] = struct{}{}
	return nil
}

func (s *voiceSession) resolvePendingExplicit(epoch identity.ResponseEpoch, hasEpoch bool) (bool, error) {
	if len(s.pending) == 0 {
		if hasEpoch {
			return false, fmt.Errorf("response.created echoed unregistered live_epoch %d", epoch)
		}
		return false, nil
	}
	if !hasEpoch {
		return false, errors.New("explicit response.created is missing live_epoch metadata")
	}
	if _, exists := s.pending[epoch]; !exists {
		return false, fmt.Errorf("explicit response.created has unmatched live_epoch %d", epoch)
	}
	delete(s.pending, epoch)
	return true, nil
}

func (s *voiceSession) handleMapped(mapped mappedServerEvent) error {
	if mapped.created != nil {
		return s.handleResponseCreated(mapped)
	}
	if mapped.event.Kind == realtime.EventError && mapped.event.Epoch != 0 {
		// A provider rejection of response.create is terminal for that pending
		// command even though no response.created will arrive to consume it.
		delete(s.pending, mapped.event.Epoch)
	}
	if !mapped.responseSpecific {
		return s.deliver(s.ctx, mapped.event)
	}
	correlation, exists := s.responses[mapped.event.ResponseID]
	if !exists {
		if len(s.pending) != 0 || mapped.event.Kind != realtime.EventTranscriptDone {
			return fmt.Errorf("%w: %q", ErrUnknownResponseEpoch, mapped.event.ResponseID)
		}
		if err := s.beginImplicitResponseFromOutput(mapped.event.ResponseID); err != nil {
			return err
		}
		correlation = s.responses[mapped.event.ResponseID]
	}
	if correlation.bound {
		mapped.event.Epoch = correlation.epoch
		if mapped.event.Kind == realtime.EventResponseDone {
			if correlation.cancelEpoch != 0 {
				mapped.event.Epoch = correlation.cancelEpoch
			}
			correlation.terminal = true
		}
		return s.deliver(s.ctx, mapped.event)
	}
	if len(correlation.buffered) == implicitEventLimit || correlation.bufferedBytes+mapped.frameBytes > implicitByteLimit {
		return fmt.Errorf("%w for response %q", ErrImplicitBufferFull, mapped.event.ResponseID)
	}
	buffered := newBufferedResponseEvent(mapped)
	if s.implicitBufferedEvents == implicitAggregateEvents || buffered.ownedBytes > implicitAggregateBytes-s.implicitBufferedBytes {
		zeroRealtimeEvent(&buffered.event)
		return fmt.Errorf("%w for response %q", ErrImplicitAggregateFull, mapped.event.ResponseID)
	}
	correlation.buffered = append(correlation.buffered, buffered)
	correlation.bufferedBytes += mapped.frameBytes
	s.implicitBufferedEvents++
	s.implicitBufferedBytes += buffered.ownedBytes
	return nil
}

// beginImplicitResponseFromOutput normalizes the known server-VAD variant that
// starts with a terminal transcript chunk and omits response.created. All
// other response-scoped first events remain strict above, and a pending
// response.create may never use this compatibility path because its
// live_epoch must be echoed by the provider.
func (s *voiceSession) beginImplicitResponseFromOutput(responseID string) error {
	if responseID == "" {
		return errors.New("implicit provider output response.id is required")
	}
	if s.implicitUnbound == implicitUnboundLimit {
		return ErrImplicitResponseLimit
	}
	s.responses[responseID] = &responseCorrelation{unbound: true}
	s.responseOrder = append(s.responseOrder, responseID)
	s.implicitUnbound++
	if err := s.trimResponseHistory(); err != nil {
		return err
	}
	return s.deliver(s.ctx, realtime.Event{
		Kind:              realtime.EventResponseCreated,
		ResponseID:        responseID,
		Status:            "in_progress",
		NeedsEpochBinding: true,
	})
}

func (s *voiceSession) markCancelled(request correlationRequest) error {
	if request.responseID == "" || request.epoch == 0 {
		return errors.New("response ID and epoch are required for cancellation")
	}
	correlation, exists := s.responses[request.responseID]
	if !exists {
		// Preserve the provider's response.cancel behavior for callers which
		// name an unknown response. There is no local stream to re-fence.
		return nil
	}
	if !correlation.bound {
		return fmt.Errorf("response %q is not bound to an epoch", request.responseID)
	}
	if correlation.cancelEpoch != 0 && correlation.cancelEpoch != request.epoch {
		return fmt.Errorf("response %q cancellation is already bound to epoch %d", request.responseID, correlation.cancelEpoch)
	}
	correlation.cancelEpoch = request.epoch
	return nil
}

func (s *voiceSession) handleResponseCreated(mapped mappedServerEvent) error {
	created := mapped.created
	if created.responseID == "" {
		return errors.New("response.created response.id is required")
	}
	if _, exists := s.responses[created.responseID]; exists {
		return fmt.Errorf("duplicate response.created for %q", created.responseID)
	}

	explicit, err := s.resolvePendingExplicit(created.epoch, created.hasEpoch)
	if err != nil {
		return err
	}
	correlation := &responseCorrelation{}
	if explicit {
		correlation.bound = true
		correlation.epoch = created.epoch
		mapped.event.Epoch = created.epoch
	} else {
		if s.implicitUnbound == implicitUnboundLimit {
			return ErrImplicitResponseLimit
		}
		correlation.unbound = true
		s.implicitUnbound++
		mapped.event.NeedsEpochBinding = true
	}
	s.responses[created.responseID] = correlation
	s.responseOrder = append(s.responseOrder, created.responseID)
	if err := s.trimResponseHistory(); err != nil {
		return err
	}
	return s.deliver(s.ctx, mapped.event)
}

func (s *voiceSession) bindImplicit(request correlationRequest) error {
	if request.responseID == "" || request.epoch == 0 {
		return errors.New("response ID and epoch are required for binding")
	}
	correlation, exists := s.responses[request.responseID]
	if !exists {
		return fmt.Errorf("%w: %q", ErrUnknownResponseEpoch, request.responseID)
	}
	if correlation.bound && correlation.epoch != request.epoch {
		return fmt.Errorf("response %q is already bound to epoch %d", request.responseID, correlation.epoch)
	}
	if correlation.unbound {
		correlation.unbound = false
		if s.implicitUnbound > 0 {
			s.implicitUnbound--
		}
	}
	correlation.bound = true
	correlation.epoch = request.epoch
	for len(correlation.buffered) > 0 {
		buffered := &correlation.buffered[0]
		buffered.event.Epoch = request.epoch
		if err := s.deliver(request.ctx, buffered.event); err != nil {
			return err
		}
		if buffered.event.Kind == realtime.EventResponseDone {
			correlation.terminal = true
		}
		correlation.bufferedBytes -= buffered.rawBytes
		s.releaseBufferedEvent(buffered)
		correlation.buffered[0] = bufferedResponseEvent{}
		correlation.buffered = correlation.buffered[1:]
	}
	correlation.buffered = nil
	correlation.bufferedBytes = 0
	return nil
}

func newBufferedResponseEvent(mapped mappedServerEvent) bufferedResponseEvent {
	event := cloneEvent(mapped.event)
	decoded := ownedDecodedEventBytes(event)
	owned := cap(event.Payload) + decoded
	return bufferedResponseEvent{event: event, rawBytes: mapped.frameBytes, decodedBytes: decoded, ownedBytes: owned}
}

func ownedDecodedEventBytes(event realtime.Event) int {
	return cap(event.Audio) + cap(event.Arguments) + len(event.ProviderEventID) + len(event.ResponseID) +
		len(event.ItemID) + len(event.CallID) + len(event.Name) + len(event.Text) + len(event.Status) + len(event.Reason)
}

func (s *voiceSession) releaseBufferedEvent(buffered *bufferedResponseEvent) {
	if buffered == nil {
		return
	}
	if s.implicitBufferedEvents > 0 {
		s.implicitBufferedEvents--
	}
	if buffered.ownedBytes <= s.implicitBufferedBytes {
		s.implicitBufferedBytes -= buffered.ownedBytes
	} else {
		s.implicitBufferedBytes = 0
	}
	zeroRealtimeEvent(&buffered.event)
	buffered.rawBytes = 0
	buffered.decodedBytes = 0
	buffered.ownedBytes = 0
}

func (s *voiceSession) releaseCorrelationBuffer(correlation *responseCorrelation) {
	if correlation == nil {
		return
	}
	if correlation.unbound {
		correlation.unbound = false
		if s.implicitUnbound > 0 {
			s.implicitUnbound--
		}
	}
	for index := range correlation.buffered {
		s.releaseBufferedEvent(&correlation.buffered[index])
	}
	correlation.buffered = nil
	correlation.bufferedBytes = 0
}

func (s *voiceSession) releaseAllImplicitBuffers() {
	for _, correlation := range s.responses {
		s.releaseCorrelationBuffer(correlation)
	}
	s.implicitUnbound = 0
	s.implicitBufferedEvents = 0
	s.implicitBufferedBytes = 0
}

func zeroRealtimeEvent(event *realtime.Event) {
	if event == nil {
		return
	}
	for index := range event.Audio {
		event.Audio[index] = 0
	}
	for index := range event.Arguments {
		event.Arguments[index] = 0
	}
	for index := range event.Payload {
		event.Payload[index] = 0
	}
	event.Audio = nil
	event.Arguments = nil
	event.Payload = nil
	event.ProviderEventID = ""
	event.ResponseID = ""
	event.ItemID = ""
	event.CallID = ""
	event.Name = ""
	event.Text = ""
	event.Status = ""
	event.Reason = ""
}

func (s *voiceSession) trimResponseHistory() error {
	for len(s.responses) > responseHistoryLimit {
		removed := false
		for index, responseID := range s.responseOrder {
			if correlation := s.responses[responseID]; correlation != nil && correlation.terminal {
				delete(s.responses, responseID)
				s.responseOrder = append(s.responseOrder[:index], s.responseOrder[index+1:]...)
				removed = true
				break
			}
		}
		if !removed {
			return errors.New("response correlation history is full")
		}
	}
	return nil
}

func (s *voiceSession) deliver(ctx context.Context, event realtime.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-s.done:
		return s.sessionError()
	default:
	}
	if !s.mappingActive {
		select {
		case s.events <- cloneEvent(event):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-s.done:
			return s.sessionError()
		}
	}
	s.mappedEvents = append(s.mappedEvents, cloneEvent(event))
	return nil
}

func (s *voiceSession) terminate(err error) {
	if err == nil {
		err = io.EOF
	}
	select {
	case s.terminationRequests() <- err:
	default:
	}
}

func (s *voiceSession) terminationRequests() chan error {
	s.terminalRequestOnce.Do(func() { s.terminalRequests = make(chan error, 1) })
	return s.terminalRequests
}

func (s *voiceSession) publishTerminal(err error) {
	if err == nil {
		err = io.EOF
	}
	s.terminalOnce.Do(func() {
		s.terminalMu.Lock()
		s.terminalErr = err
		s.terminalMu.Unlock()
		close(s.done)
		s.cancel()
		_ = s.closeNow()
	})
}

func (s *voiceSession) sessionError() error {
	if s.isClosing() && !s.terminalHandoffActive() {
		return ErrSessionClosed
	}
	s.terminalMu.RLock()
	err := s.terminalErr
	s.terminalMu.RUnlock()
	if err == nil {
		return ErrSessionClosed
	}
	return err
}
