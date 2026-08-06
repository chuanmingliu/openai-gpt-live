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
	responseHistoryLimit     = 1024
	implicitEventLimit       = 16
	implicitByteLimit        = 2 << 20
)

var (
	ErrUnknownResponseEpoch = errors.New("unknown response epoch")
	ErrImplicitBufferFull   = errors.New("implicit response buffer limit exceeded")
	ErrSessionClosed        = errors.New("realtime WebSocket session closed")
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
	if err != nil || endpoint.Host == "" || (endpoint.Scheme != "ws" && endpoint.Scheme != "wss") {
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
		conn:          conn,
		ctx:           sessionContext,
		cancel:        cancelSession,
		maxFrameBytes: c.cfg.MaxFrameBytes,
		writes:        make(chan writeRequest, writeQueueCapacity),
		closeWrites:   make(chan writeRequest, 1),
		reads:         make(chan socketRead, 1),
		correlations:  make(chan correlationRequest, correlationQueueCapacity),
		events:        make(chan realtime.Event, receiveQueueCapacity),
		closing:       make(chan struct{}),
		done:          make(chan struct{}),
		responses:     make(map[string]*responseCorrelation),
		pending:       make(map[identity.ResponseEpoch]struct{}),
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
)

type correlationRequest struct {
	kind       correlationKind
	ctx        context.Context
	epoch      identity.ResponseEpoch
	responseID string
	result     chan error
}

type bufferedResponseEvent struct {
	event realtime.Event
	bytes int
}

type responseCorrelation struct {
	epoch         identity.ResponseEpoch
	bound         bool
	terminal      bool
	buffered      []bufferedResponseEvent
	bufferedBytes int
}

type voiceSession struct {
	conn          *websocket.Conn
	ctx           context.Context
	cancel        context.CancelFunc
	maxFrameBytes int64

	writes       chan writeRequest
	closeWrites  chan writeRequest
	reads        chan socketRead
	correlations chan correlationRequest
	events       chan realtime.Event
	closing      chan struct{}
	done         chan struct{}

	responses     map[string]*responseCorrelation
	responseOrder []string
	pending       map[identity.ResponseEpoch]struct{}

	terminalOnce      sync.Once
	terminalMu        sync.RWMutex
	terminalErr       error
	closeOnce         sync.Once
	activeWriteMu     sync.Mutex
	activeWriteCancel context.CancelFunc
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
	select {
	case err := <-request.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closing:
		return ErrSessionClosed
	case <-s.done:
		select {
		case err := <-request.result:
			return err
		default:
			return s.sessionError()
		}
	}
}

func (s *voiceSession) Receive(ctx context.Context) (realtime.Event, error) {
	if ctx == nil {
		return realtime.Event{}, errors.New("receive context is nil")
	}
	for {
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
		case <-s.closing:
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
	case <-s.done:
		return nil
	default:
	}

	var closeErr error
	s.closeOnce.Do(func() {
		close(s.closing)
		request := writeRequest{ctx: ctx, result: make(chan error, 1)}
		select {
		case s.closeWrites <- request:
			s.cancelActiveWrite()
		case <-ctx.Done():
			closeErr = ctx.Err()
			s.terminate(ctx.Err())
		case <-s.done:
		}
	})
	return closeErr
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
		case request := <-s.closeWrites:
			s.finishClose(request)
			return
		default:
		}
		select {
		case request := <-s.closeWrites:
			s.finishClose(request)
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
			attemptedWrite := false
			if err == nil {
				writeContext, finishWrite, ok := s.beginWrite(request.ctx)
				if !ok {
					err = ErrSessionClosed
				} else {
					attemptedWrite = true
					err = s.conn.Write(writeContext, websocket.MessageText, raw)
					finishWrite()
				}
			}
			request.result <- err
			if attemptedWrite && err != nil && !s.isClosing() {
				s.terminate(err)
				return
			}
		case <-s.done:
			return
		}
	}
}

func (s *voiceSession) finishClose(request writeRequest) {
drain:
	for {
		select {
		case queued := <-s.writes:
			queued.result <- ErrSessionClosed
		default:
			break drain
		}
	}
	err := s.conn.Close(websocket.StatusNormalClosure, "")
	if errors.Is(err, net.ErrClosed) {
		err = nil
	}
	request.result <- err
	s.terminate(ErrSessionClosed)
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
	for {
		select {
		case read := <-s.reads:
			if read.err != nil {
				s.terminate(read.err)
				return
			}
			if read.messageType != websocket.MessageText {
				s.terminate(errors.New("provider sent non-text WebSocket message"))
				return
			}
			mapped, err := mapServerEvent(read.raw, s.maxFrameBytes)
			if err != nil {
				s.terminate(err)
				return
			}
			if mapped.ignore {
				continue
			}
			if err := s.handleMapped(mapped); err != nil {
				s.terminate(err)
				return
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
			default:
				err = errors.New("unknown correlation request")
			}
			request.result <- err
		case <-s.done:
			return
		}
	}
}

func (s *voiceSession) registerExplicit(epoch identity.ResponseEpoch) error {
	if epoch == 0 {
		return errors.New("response epoch must be positive")
	}
	if _, exists := s.pending[epoch]; exists {
		return fmt.Errorf("response epoch %d is already pending", epoch)
	}
	s.pending[epoch] = struct{}{}
	return nil
}

func (s *voiceSession) handleMapped(mapped mappedServerEvent) error {
	if mapped.created != nil {
		return s.handleResponseCreated(mapped)
	}
	if !mapped.responseSpecific {
		return s.deliver(s.ctx, mapped.event)
	}
	correlation, exists := s.responses[mapped.event.ResponseID]
	if !exists {
		return fmt.Errorf("%w: %q", ErrUnknownResponseEpoch, mapped.event.ResponseID)
	}
	if correlation.bound {
		mapped.event.Epoch = correlation.epoch
		if mapped.event.Kind == realtime.EventResponseDone {
			correlation.terminal = true
		}
		return s.deliver(s.ctx, mapped.event)
	}
	if len(correlation.buffered) == implicitEventLimit || correlation.bufferedBytes+mapped.frameBytes > implicitByteLimit {
		return fmt.Errorf("%w for response %q", ErrImplicitBufferFull, mapped.event.ResponseID)
	}
	correlation.buffered = append(correlation.buffered, bufferedResponseEvent{event: cloneEvent(mapped.event), bytes: mapped.frameBytes})
	correlation.bufferedBytes += mapped.frameBytes
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

	correlation := &responseCorrelation{}
	if len(s.pending) > 0 {
		if !created.hasEpoch {
			return errors.New("explicit response.created is missing live_epoch metadata")
		}
		if _, exists := s.pending[created.epoch]; !exists {
			return fmt.Errorf("explicit response.created has unmatched live_epoch %d", created.epoch)
		}
		delete(s.pending, created.epoch)
		correlation.bound = true
		correlation.epoch = created.epoch
		mapped.event.Epoch = created.epoch
	} else {
		if created.hasEpoch {
			return fmt.Errorf("response.created echoed unregistered live_epoch %d", created.epoch)
		}
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
	correlation.bound = true
	correlation.epoch = request.epoch
	for len(correlation.buffered) > 0 {
		buffered := correlation.buffered[0]
		buffered.event.Epoch = request.epoch
		if err := s.deliver(request.ctx, buffered.event); err != nil {
			return err
		}
		if buffered.event.Kind == realtime.EventResponseDone {
			correlation.terminal = true
		}
		correlation.bufferedBytes -= buffered.bytes
		correlation.buffered = correlation.buffered[1:]
	}
	return nil
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
	select {
	case s.events <- cloneEvent(event):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return s.sessionError()
	}
}

func (s *voiceSession) terminate(err error) {
	if err == nil {
		err = io.EOF
	}
	s.terminalOnce.Do(func() {
		s.terminalMu.Lock()
		s.terminalErr = err
		s.terminalMu.Unlock()
		close(s.done)
		s.cancel()
		_ = s.conn.CloseNow()
	})
}

func (s *voiceSession) sessionError() error {
	s.terminalMu.RLock()
	err := s.terminalErr
	s.terminalMu.RUnlock()
	if err == nil {
		return ErrSessionClosed
	}
	return err
}
