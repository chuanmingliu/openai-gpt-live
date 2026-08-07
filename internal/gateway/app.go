// Package gateway composes the downstream transport and realtime session
// manager into one HTTP server.
package gateway

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/config"
	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/presentation"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/chuanmingliu/openai-gpt-live/internal/safety"
	"github.com/chuanmingliu/openai-gpt-live/internal/session"
	transportws "github.com/chuanmingliu/openai-gpt-live/internal/transport/ws"
)

const (
	defaultIngressBlockTimeout   = 250 * time.Millisecond
	defaultEgressBlockTimeout    = 250 * time.Millisecond
	defaultWebSocketReadTimeout  = 30 * time.Second
	defaultWebSocketWriteTimeout = 10 * time.Second
	defaultShutdownTimeout       = 10 * time.Second
	defaultDevelopmentSubjectTTL = 15 * time.Minute
	defaultMaxJSONDepth          = 32
)

type Option func(*App) error

type App struct {
	cfg             config.Config
	ids             session.IDGenerator
	admission       transportws.Admission
	managerConfig   session.Config
	wsOptions       transportws.Options
	manager         *session.Manager
	handler         http.Handler
	server          *http.Server
	listener        net.Listener
	shutdownTimeout time.Duration
	activeHandlers  handlerTracker
	durable         *DurableDependencies
	handoffs        *HandoffCoordinator
	transcripts     *TranscriptCoordinator
	presentation    *HandoffPresentationDependencies
	eventPump       *handoff.EventPump
	eventListener   HandoffEventListener
	eventWakeups    chan handoff.EventWakeup
}

type DurableDependencies struct {
	Service                 handoff.Service
	ConversationWriter      ConversationWriter
	Journal                 CoordinatorJournal
	DeliveryAffinity        string
	JournalOwner            string
	JournalClaimLease       time.Duration
	HandoffQueueCapacity    int
	TranscriptQueueCapacity int
	Snapshot                SessionHandoffSnapshot
}

type HandoffEventListener interface {
	Run(context.Context, chan<- handoff.EventWakeup) error
}

type HandoffPresentationDependencies struct {
	Reader         handoff.EventReader
	Cursors        handoff.DeliveryCursorStore
	Listener       HandoffEventListener
	Clock          clock.Clock
	PollInterval   time.Duration
	ConsumerID     string
	WakeupCapacity int
	Policy         presentation.Policy
	Safety         safety.Policy
}

func WithHandoffPresentation(dependencies HandoffPresentationDependencies) Option {
	return func(app *App) error {
		copy := dependencies
		app.presentation = &copy
		return nil
	}
}

func WithDurableHandoffs(dependencies DurableDependencies) Option {
	return func(app *App) error {
		copy := dependencies
		copy.Snapshot = cloneHandoffSnapshot(dependencies.Snapshot)
		app.durable = &copy
		return nil
	}
}

func WithIDGenerator(ids session.IDGenerator) Option {
	return func(app *App) error {
		if isNil(ids) {
			return errors.New("session ID generator is required")
		}
		app.ids = ids
		return nil
	}
}

func WithAdmission(admission transportws.Admission) Option {
	return func(app *App) error {
		if isNil(admission) {
			return errors.New("admission is required")
		}
		app.admission = admission
		return nil
	}
}

func New(cfg config.Config, connector realtime.Connector, opts ...Option) (*App, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate gateway config: %w", err)
	}
	if isNil(connector) {
		return nil, errors.New("realtime connector is required")
	}
	if cfg.Gateway.WebSocket.MaxFrameBytes < 0 {
		return nil, errors.New("gateway websocket maximum frame bytes must not be negative")
	}
	if (cfg.Gateway.TLS.CertFile == "") != (cfg.Gateway.TLS.KeyFile == "") {
		return nil, errors.New("TLS certificate and key must be configured together")
	}

	managerConfig := session.Config{
		IngressFrames:       cfg.Gateway.Session.IngressFrames,
		EgressFrames:        cfg.Gateway.Session.EgressFrames,
		ControlEvents:       cfg.Gateway.Session.ControlEvents,
		WriterControlEvents: cfg.Gateway.Session.WriterControlEvents,
		IngressBlockTimeout: cfg.Gateway.Session.IngressBlockTimeout,
		EgressBlockTimeout:  cfg.Gateway.Session.EgressBlockTimeout,
	}
	if managerConfig.IngressBlockTimeout == 0 {
		managerConfig.IngressBlockTimeout = defaultIngressBlockTimeout
	}
	if managerConfig.EgressBlockTimeout == 0 {
		managerConfig.EgressBlockTimeout = defaultEgressBlockTimeout
	}
	wsOptions := transportws.Options{
		MaxFrameBytes: int64(cfg.Gateway.WebSocket.MaxFrameBytes),
		MaxJSONDepth:  defaultMaxJSONDepth,
		MaxAudioBytes: cfg.Gateway.Session.IngressAudioBytes,
		ReadTimeout:   cfg.Gateway.WebSocket.ReadTimeout,
		WriteTimeout:  cfg.Gateway.WebSocket.WriteTimeout,
	}
	if wsOptions.ReadTimeout == 0 {
		wsOptions.ReadTimeout = defaultWebSocketReadTimeout
	}
	if wsOptions.WriteTimeout == 0 {
		wsOptions.WriteTimeout = defaultWebSocketWriteTimeout
	}
	shutdownTimeout := cfg.Shutdown.DrainTimeout
	if shutdownTimeout == 0 {
		shutdownTimeout = defaultShutdownTimeout
	}
	if managerConfig.IngressBlockTimeout < 0 || managerConfig.EgressBlockTimeout < 0 ||
		wsOptions.ReadTimeout < 0 || wsOptions.WriteTimeout < 0 || shutdownTimeout < 0 {
		return nil, errors.New("gateway timeouts must not be negative")
	}

	app := &App{
		cfg: cfg, ids: randomIDs{}, managerConfig: managerConfig,
		wsOptions: wsOptions, shutdownTimeout: shutdownTimeout,
	}
	for _, option := range opts {
		if option == nil {
			return nil, errors.New("gateway option is nil")
		}
		if err := option(app); err != nil {
			return nil, err
		}
	}
	if cfg.Mode == "local" {
		if app.admission == nil {
			app.admission = developmentAdmission{}
		}
	} else if app.admission == nil {
		return nil, errors.New("production admission must be explicitly configured")
	}
	if isNil(app.ids) {
		return nil, errors.New("session ID generator is required")
	}

	if cfg.Features.Handoffs {
		if app.durable == nil || isNil(app.durable.Service) || isNil(app.durable.ConversationWriter) ||
			isNil(app.durable.Journal) || app.durable.DeliveryAffinity == "" || app.durable.JournalOwner == "" || app.durable.JournalClaimLease <= 0 ||
			app.durable.HandoffQueueCapacity <= 0 || app.durable.TranscriptQueueCapacity <= 0 ||
			validateHandoffSnapshot(app.durable.Snapshot) != nil {
			return nil, errors.New("durable handoff dependencies and valid server snapshot are required")
		}
		if app.presentation == nil || isNil(app.presentation.Reader) || isNil(app.presentation.Cursors) ||
			isNil(app.presentation.Listener) || isNil(app.presentation.Clock) || strings.TrimSpace(app.presentation.ConsumerID) == "" ||
			len(app.presentation.ConsumerID) > session.DurableIdentifierBytes || !utf8.ValidString(app.presentation.ConsumerID) || app.presentation.PollInterval < time.Millisecond ||
			app.presentation.PollInterval > 24*time.Hour || app.presentation.WakeupCapacity < 0 || app.presentation.WakeupCapacity > 10_000 {
			return nil, errors.New("handoff presentation dependencies are required and must be bounded")
		}
		if app.presentation.WakeupCapacity == 0 {
			app.presentation.WakeupCapacity = 64
		}
		if isNil(app.presentation.Policy) {
			app.presentation.Policy = presentation.NewPolicy(presentation.Config{})
		}
		if isNil(app.presentation.Safety) {
			app.presentation.Safety = safety.DenyAll{}
		}
		app.eventWakeups = make(chan handoff.EventWakeup, app.presentation.WakeupCapacity)
		app.eventPump = &handoff.EventPump{
			Reader: app.presentation.Reader, Cursors: app.presentation.Cursors, Notifications: app.eventWakeups,
			Clock: app.presentation.Clock, PollInterval: app.presentation.PollInterval,
		}
		app.eventListener = app.presentation.Listener
		managerConfig.HandoffConsumerID = app.presentation.ConsumerID
		managerConfig.HandoffSubscriptions = app.eventPump
		managerConfig.PresentationPolicy = app.presentation.Policy
		managerConfig.SafetyPolicy = app.presentation.Safety
		dispatcher := &durableDispatcher{snapshot: cloneHandoffSnapshot(app.durable.Snapshot)}
		managerConfig.DurableDispatcher = dispatcher
		app.managerConfig = managerConfig
		app.manager = session.NewManager(managerConfig, connector, app.ids)
		app.eventPump.Sink = app.manager
		app.handoffs = NewHandoffCoordinator(app.durable.Service, app.manager, app.durable.HandoffQueueCapacity)
		app.transcripts = NewTranscriptCoordinator(app.durable.ConversationWriter, app.manager, app.durable.TranscriptQueueCapacity)
		if app.handoffs == nil || app.transcripts == nil {
			return nil, errors.New("durable handoff coordinators are invalid")
		}
		if err := app.handoffs.configureJournal(app.durable.Journal, app.durable.DeliveryAffinity, app.durable.JournalOwner+":handoff", app.durable.JournalClaimLease); err != nil {
			return nil, err
		}
		if err := app.transcripts.configureJournal(app.durable.Journal, app.durable.DeliveryAffinity, app.durable.JournalOwner+":transcript", app.durable.JournalClaimLease); err != nil {
			return nil, err
		}
		dispatcher.handoffs = app.handoffs
		dispatcher.transcripts = app.transcripts
	} else {
		if app.durable != nil || app.presentation != nil {
			return nil, errors.New("durable handoff dependencies supplied while feature is disabled")
		}
		app.manager = session.NewManager(managerConfig, connector, app.ids)
	}
	app.handler = &trackedHandler{next: transportws.NewHandler(wsOptions, app.manager, app.admission), active: &app.activeHandlers}
	app.server = &http.Server{Addr: cfg.Gateway.ListenAddr, Handler: app.handler}
	return app, nil
}

type durableDispatcher struct {
	handoffs    *HandoffCoordinator
	transcripts *TranscriptCoordinator
	snapshot    SessionHandoffSnapshot
}

func (d *durableDispatcher) DispatchTranscript(_ context.Context, trusted session.TrustedSessionContext, effect session.TranscriptRequestedEffect) error {
	if d == nil || d.transcripts == nil {
		return errors.New("transcript coordinator is unavailable")
	}
	return d.transcripts.Enqueue(TranscriptWork{
		SessionID: trusted.IDs.SessionID, Incarnation: trusted.IDs.Incarnation,
		Scope: handoff.Scope{TenantID: trusted.Subject.TenantID, UserID: trusted.Subject.UserID, ConversationID: trusted.IDs.ConversationID},
		Turn:  effect.Turn, Sequence: effect.Sequence, Role: effect.Role, FinalText: effect.FinalText,
	})
}

func (d *durableDispatcher) DispatchHandoff(_ context.Context, trusted session.TrustedSessionContext, effect session.HandoffRequestedEffect) error {
	if d == nil || d.handoffs == nil {
		return errors.New("handoff coordinator is unavailable")
	}
	return d.handoffs.Enqueue(HandoffWork{
		SessionID: trusted.IDs.SessionID, Incarnation: trusted.IDs.Incarnation,
		OriginEpoch: effect.OriginEpoch,
		Scope:       handoff.Scope{TenantID: trusted.Subject.TenantID, UserID: trusted.Subject.UserID, ConversationID: trusted.IDs.ConversationID},
		CallID:      effect.CallID, ResponseID: effect.ResponseID, ToolName: effect.ToolName,
		Arguments: append(json.RawMessage(nil), effect.Arguments...), Turn: effect.Turn,
		Transcript: effect.Transcript, Snapshot: cloneHandoffSnapshot(d.snapshot), RejectReason: effect.RejectReason,
	})
}

func validateHandoffSnapshot(snapshot SessionHandoffSnapshot) error {
	_, err := handoff.ValidateCreateRequest(handoff.CreateRequest{
		Scope:     handoff.Scope{TenantID: "snapshot-validation", UserID: "snapshot-validation", ConversationID: "snapshot-validation"},
		SessionID: "snapshot-validation", SourceTurn: identity.TurnKey{ID: "snapshot-validation", Revision: 1},
		UserRequest: "snapshot-validation", Objective: "snapshot-validation",
		ContextRef: snapshot.ContextRef, ContextHash: snapshot.ContextHash, Deadline: snapshot.Deadline,
		TokenBudget: snapshot.TokenBudget, Priority: snapshot.Priority,
		ToolAllowlist:  append([]string(nil), snapshot.ToolAllowlist...),
		ApprovalPolicy: append(json.RawMessage(nil), snapshot.ApprovalPolicy...),
		Versions:       snapshot.Versions, PresentationHint: snapshot.PresentationHint,
		IdempotencyKey: "snapshot-validation",
	}, time.Now().UTC())
	return err
}

func (a *App) Run(ctx context.Context) (runErr error) {
	if a == nil || a.server == nil {
		return errors.New("gateway app is nil")
	}
	if ctx == nil {
		return errors.New("gateway context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.durable != nil {
		affinityLock, err := a.durable.Journal.AcquireCoordinatorAffinity(ctx, a.durable.DeliveryAffinity)
		if err != nil {
			return fmt.Errorf("acquire coordinator delivery affinity: %w", err)
		}
		if isNil(affinityLock) {
			return errors.New("acquire coordinator delivery affinity: nil lock")
		}
		defer func() {
			if err := affinityLock.Close(); err != nil {
				runErr = errors.Join(runErr, fmt.Errorf("release coordinator delivery affinity: %w", err))
			}
		}()
	}
	serveCtx, cancelServe := context.WithCancel(ctx)
	defer cancelServe()
	listener := a.listener
	if listener == nil {
		var err error
		listener, err = net.Listen("tcp", a.server.Addr)
		if err != nil {
			return fmt.Errorf("listen gateway: %w", err)
		}
	}
	a.server.BaseContext = func(net.Listener) context.Context { return serveCtx }
	type coordinatorRunResult struct {
		name string
		err  error
	}
	coordinatorResult := make(chan coordinatorRunResult, 4)
	coordinatorDone := make(chan struct{})
	coordinatorCount := 0
	var coordinatorWG sync.WaitGroup
	startCoordinator := func(name string, run func(context.Context) error) {
		coordinatorCount++
		coordinatorWG.Add(1)
		go func() {
			defer coordinatorWG.Done()
			coordinatorResult <- coordinatorRunResult{name: name, err: run(serveCtx)}
		}()
	}
	if a.transcripts != nil {
		startCoordinator("transcript coordinator", a.transcripts.Run)
	}
	if a.handoffs != nil {
		startCoordinator("handoff coordinator", a.handoffs.Run)
	}
	if a.eventListener != nil {
		startCoordinator("handoff event listener", func(ctx context.Context) error { return a.eventListener.Run(ctx, a.eventWakeups) })
	}
	if a.eventPump != nil {
		startCoordinator("handoff event pump", a.eventPump.Run)
	}
	go func() {
		coordinatorWG.Wait()
		close(coordinatorDone)
	}()
	serveResult := make(chan error, 1)
	go func() {
		var err error
		if a.cfg.Gateway.TLS.CertFile != "" {
			err = a.server.ServeTLS(listener, a.cfg.Gateway.TLS.CertFile, a.cfg.Gateway.TLS.KeyFile)
		} else {
			err = a.server.Serve(listener)
		}
		serveResult <- err
	}()

	var serveErr error
	serveObserved := false
	var coordinatorErr error
	select {
	case serveErr = <-serveResult:
		serveObserved = true
	case result := <-coordinatorResult:
		if result.err == nil && ctx.Err() == nil {
			result.err = errors.New("stopped unexpectedly")
		}
		if result.err != nil {
			coordinatorErr = fmt.Errorf("%s: %w", result.name, result.err)
		}
	case <-ctx.Done():
	}

	// Every exit after Serve starts owns the same bounded teardown. In
	// particular, a permanent listener failure does not cancel request contexts
	// on its own, and Shutdown does not track hijacked WebSockets.
	cancelServe()
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), a.shutdownTimeout)
	defer cancelShutdown()
	var cleanupErrs []error
	if err := a.server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		cleanupErrs = append(cleanupErrs, fmt.Errorf("shutdown gateway: %w", err))
		if closeErr := a.server.Close(); closeErr != nil && !errors.Is(closeErr, http.ErrServerClosed) {
			cleanupErrs = append(cleanupErrs, fmt.Errorf("close gateway: %w", closeErr))
		}
	}
	if !serveObserved {
		select {
		case serveErr = <-serveResult:
			serveObserved = true
		case <-shutdownCtx.Done():
			cleanupErrs = append(cleanupErrs, fmt.Errorf("wait for gateway server: %w", shutdownCtx.Err()))
			_ = a.server.Close()
		}
	}
	if err := a.activeHandlers.drain(shutdownCtx); err != nil {
		cleanupErrs = append(cleanupErrs, fmt.Errorf("wait for active gateway sessions: %w", err))
		_ = a.server.Close()
	}
	if coordinatorCount != 0 {
		select {
		case <-coordinatorDone:
		case <-shutdownCtx.Done():
			cleanupErrs = append(cleanupErrs, fmt.Errorf("wait for durable coordinators: %w", shutdownCtx.Err()))
		}
	}

	var primaryErr error
	if serveObserved && serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		primaryErr = fmt.Errorf("serve gateway: %w", serveErr)
	}
	return errors.Join(append([]error{coordinatorErr, primaryErr}, cleanupErrs...)...)
}

type trackedHandler struct {
	next   http.Handler
	active *handlerTracker
}

func (h *trackedHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !h.active.begin() {
		http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
		return
	}
	defer h.active.end()
	h.next.ServeHTTP(w, r)
}

type handlerTracker struct {
	mu       sync.Mutex
	active   int
	draining bool
	done     chan struct{}
}

func (t *handlerTracker) begin() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.draining {
		return false
	}
	t.active++
	return true
}

func (t *handlerTracker) end() {
	t.mu.Lock()
	t.active--
	if t.draining && t.active == 0 {
		close(t.done)
	}
	t.mu.Unlock()
}

func (t *handlerTracker) drain(ctx context.Context) error {
	t.mu.Lock()
	if !t.draining {
		t.draining = true
		t.done = make(chan struct{})
		if t.active == 0 {
			close(t.done)
		}
	}
	done := t.done
	t.mu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type developmentAdmission struct{}

func (developmentAdmission) Admit(context.Context, *http.Request) (session.Subject, func(), error) {
	return session.Subject{
		TenantID:  "development",
		UserID:    "local-simulator",
		Scopes:    map[string]struct{}{"realtime": {}},
		ExpiresAt: time.Now().Add(defaultDevelopmentSubjectTTL),
	}, func() {}, nil
}

type randomIDs struct{}

func (randomIDs) NewSessionID() identity.SessionID {
	return identity.SessionID(randomID())
}

func (randomIDs) NewConversationID() identity.ConversationID {
	return identity.ConversationID(randomID())
}

func randomID() string {
	raw := make([]byte, 24)
	if _, err := rand.Read(raw); err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}
