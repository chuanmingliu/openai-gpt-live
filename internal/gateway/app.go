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
	"github.com/chuanmingliu/openai-gpt-live/internal/lifecycle"
	"github.com/chuanmingliu/openai-gpt-live/internal/presentation"
	"github.com/chuanmingliu/openai-gpt-live/internal/profile"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/chuanmingliu/openai-gpt-live/internal/safety"
	"github.com/chuanmingliu/openai-gpt-live/internal/session"
	"github.com/chuanmingliu/openai-gpt-live/internal/toolpolicy"
	transportws "github.com/chuanmingliu/openai-gpt-live/internal/transport/ws"
	"go.opentelemetry.io/otel/trace"
)

const (
	defaultIngressBlockTimeout   = 250 * time.Millisecond
	defaultEgressBlockTimeout    = 250 * time.Millisecond
	defaultWebSocketReadTimeout  = 30 * time.Second
	defaultWebSocketWriteTimeout = 10 * time.Second
	defaultShutdownTimeout       = 10 * time.Second
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
	drainer         *lifecycle.Drainer
	metricsHooks    session.MetricsHooks
	handoffMetrics  HandoffMetricsHooks
	tracer          trace.Tracer
	durable         *DurableDependencies
	handoffs        *HandoffCoordinator
	transcripts     *TranscriptCoordinator
	approvals       *ApprovalCoordinator
	presentation    *HandoffPresentationDependencies
	eventPump       *handoff.EventPump
	eventListener   HandoffEventListener
	eventWakeups    chan handoff.EventWakeup
	featureGates    interface{ Enabled(string) bool }
	approvalStore   handoff.ApprovalStore
}

type DurableDependencies struct {
	Service                 handoff.Service
	ConversationWriter      ConversationWriter
	ConversationProvisioner session.ConversationProvisioner
	ConversationTTL         time.Duration
	RecoveryStore           session.RecoveryStore
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

type HandoffMetricsHooks struct {
	Acknowledgement func(time.Duration, string)
	Presentation    func(time.Duration, string)
	Duplicate       func(string)
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

func WithReconnectAuthority(credentials session.ReconnectCredentials, owners session.ReconnectOwnerStore, ownerID string, ttl time.Duration) Option {
	return func(app *App) error {
		if isNil(credentials) || isNil(owners) {
			return errors.New("reconnect authority is required")
		}
		app.managerConfig.ReconnectCredentials = credentials
		app.managerConfig.ReconnectOwners = owners
		app.managerConfig.ReconnectOwnerID = ownerID
		app.managerConfig.ReconnectCredentialTTL = ttl
		app.wsOptions.ReconnectCredentials = credentials
		return nil
	}
}

func WithCanaryRuntimeIdentity(identity transportws.RuntimeIdentity) Option {
	return func(app *App) error {
		if err := identity.Validate(); err != nil {
			return fmt.Errorf("configure synthetic canary runtime identity: %w", err)
		}
		app.wsOptions.CanaryRuntimeIdentity = identity
		return nil
	}
}

func WithDrainer(drainer *lifecycle.Drainer) Option {
	return func(app *App) error {
		if drainer == nil {
			return errors.New("gateway drainer is required")
		}
		app.drainer = drainer
		return nil
	}
}

func WithSessionMetrics(hooks session.MetricsHooks) Option {
	return func(app *App) error {
		app.metricsHooks = hooks
		return nil
	}
}
func WithHandoffMetrics(hooks HandoffMetricsHooks) Option {
	return func(app *App) error {
		app.handoffMetrics = hooks
		return nil
	}
}
func WithTracer(tracer trace.Tracer) Option {
	return func(app *App) error {
		if tracer == nil {
			return errors.New("gateway tracer is required")
		}
		app.tracer = tracer
		return nil
	}
}

func WithFeatureGates(gates interface{ Enabled(string) bool }) Option {
	return func(app *App) error {
		if isNil(gates) {
			return errors.New("gateway feature gates are required")
		}
		app.featureGates = gates
		return nil
	}
}

// WithVerticalProfiles supplies the exact published-profile resolver. The
// resolver and the configuration must agree: a configured feature without a
// resolver, or a resolver without the feature, fails closed in New so no
// session can ever select a profile the deployment did not authorize.
func WithVerticalProfiles(resolver profile.Resolver) Option {
	return func(app *App) error {
		if isNil(resolver) {
			return errors.New("vertical profile resolver is required")
		}
		app.wsOptions.ProfileResolver = resolver
		app.wsOptions.VerticalProfiles = true
		return nil
	}
}

func WithApprovalStore(store handoff.ApprovalStore) Option {
	return func(app *App) error {
		if isNil(store) {
			return errors.New("gateway approval store is required")
		}
		app.approvalStore = store
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
		RecoveryItemLimit:   session.DefaultRecoveryItemLimit,
		RecoveryByteLimit:   session.DefaultRecoveryByteLimit,
		ReconnectMaxRetries: cfg.Gateway.Reconnect.MaxRetries,
		ReconnectRetryBase:  cfg.Gateway.Reconnect.RetryBase,
		ReconnectRetryMax:   cfg.Gateway.Reconnect.RetryMax,
		HandoffToolsEnabled: cfg.Features.Handoffs,
		ToolCatalog:         cfg.ToolCatalog(),
		AsyncModel:          cfg.Worker.Reasoner.Model,
	}
	if cfg.Features.Handoffs {
		managerConfig.InitialSession = session.ReservedHandoffSessionConfig()
	}
	if managerConfig.IngressBlockTimeout == 0 {
		managerConfig.IngressBlockTimeout = defaultIngressBlockTimeout
	}
	if managerConfig.EgressBlockTimeout == 0 {
		managerConfig.EgressBlockTimeout = defaultEgressBlockTimeout
	}
	handoffsEnabled := cfg.Features.Handoffs
	wsOptions := transportws.Options{
		MaxFrameBytes:   int64(cfg.Gateway.WebSocket.MaxFrameBytes),
		MaxJSONDepth:    defaultMaxJSONDepth,
		MaxAudioBytes:   cfg.Gateway.Session.IngressAudioBytes,
		ReadTimeout:     cfg.Gateway.WebSocket.ReadTimeout,
		WriteTimeout:    cfg.Gateway.WebSocket.WriteTimeout,
		AdvertisedModel: cfg.Gateway.AdvertisedModel,
		ProviderModel:   cfg.Gateway.Voice.Model,
		HandoffsEnabled: &handoffsEnabled,
	}
	if wsOptions.ReadTimeout == 0 {
		wsOptions.ReadTimeout = defaultWebSocketReadTimeout
	}
	if wsOptions.WriteTimeout == 0 {
		wsOptions.WriteTimeout = defaultWebSocketWriteTimeout
	}
	shutdownTimeout := cfg.Shutdown.GatewayDrainTimeout
	if shutdownTimeout == 0 {
		shutdownTimeout = 30 * time.Second
	}
	if managerConfig.IngressBlockTimeout < 0 || managerConfig.EgressBlockTimeout < 0 ||
		wsOptions.ReadTimeout < 0 || wsOptions.WriteTimeout < 0 || shutdownTimeout < 0 {
		return nil, errors.New("gateway timeouts must not be negative")
	}

	app := &App{
		cfg: cfg, ids: randomIDs{}, managerConfig: managerConfig,
		wsOptions: wsOptions, shutdownTimeout: shutdownTimeout, drainer: lifecycle.NewDrainer(),
	}
	app.tracer = trace.NewNoopTracerProvider().Tracer("live.gateway")
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
	managerConfig = app.managerConfig
	wsOptions = app.wsOptions
	if cfg.Features.VerticalProfiles.Enabled != wsOptions.VerticalProfiles {
		return nil, errors.New("vertical profile resolution and configuration must be enabled together")
	}
	if cfg.Mode == "production" && (isNil(managerConfig.ReconnectCredentials) || isNil(managerConfig.ReconnectOwners)) {
		return nil, errors.New("production reconnect authority must be explicitly configured")
	}
	if isNil(app.ids) {
		return nil, errors.New("session ID generator is required")
	}
	managerConfig.FeatureGates = app.featureGates
	var approvals *approvalDispatcher
	if !isNil(app.approvalStore) {
		approvals = &approvalDispatcher{}
		managerConfig.ApprovalDispatcher = approvals
	}

	if !cfg.Features.Handoffs && app.presentation != nil {
		return nil, errors.New("handoff presentation dependencies supplied while feature is disabled")
	}
	if app.durable == nil {
		if cfg.Features.Handoffs {
			return nil, errors.New("durable handoff dependencies and valid server snapshot are required")
		}
		if cfg.Mode == "production" {
			return nil, errors.New("production conversation recovery durability is required")
		}
	}
	var dispatcher *durableDispatcher
	if app.durable != nil {
		if isNil(app.durable.ConversationWriter) || isNil(app.durable.ConversationProvisioner) ||
			app.durable.ConversationTTL < time.Hour || app.durable.ConversationTTL > 10*365*24*time.Hour ||
			isNil(app.durable.Journal) || strings.TrimSpace(app.durable.DeliveryAffinity) == "" ||
			strings.TrimSpace(app.durable.JournalOwner) == "" || app.durable.JournalClaimLease <= 0 ||
			app.durable.TranscriptQueueCapacity <= 0 {
			return nil, errors.New("conversation recovery durability dependencies are required and must be bounded")
		}
		managerConfig.ConversationProvisioner = app.durable.ConversationProvisioner
		managerConfig.ConversationTTL = app.durable.ConversationTTL
		managerConfig.RecoveryStore = app.durable.RecoveryStore
		if isNil(managerConfig.RecoveryStore) {
			if store, ok := app.durable.ConversationWriter.(session.RecoveryStore); ok {
				managerConfig.RecoveryStore = store
			}
		}
		if cfg.Mode == "production" && isNil(managerConfig.RecoveryStore) {
			return nil, errors.New("production conversation recovery durability is required")
		}
		dispatcher = &durableDispatcher{snapshot: cloneHandoffSnapshot(app.durable.Snapshot)}
		managerConfig.DurableDispatcher = dispatcher
	}

	if cfg.Features.Handoffs {
		if isNil(app.durable.Service) || app.durable.HandoffQueueCapacity <= 0 || validateHandoffSnapshot(app.durable.Snapshot) != nil {
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
			return nil, errors.New("explicit handoff presentation safety policy is required")
		}
		app.eventWakeups = make(chan handoff.EventWakeup, app.presentation.WakeupCapacity)
		app.eventPump = &handoff.EventPump{
			Reader: app.presentation.Reader, Cursors: app.presentation.Cursors, Notifications: app.eventWakeups,
			Clock: app.presentation.Clock, PollInterval: app.presentation.PollInterval,
			Metrics: handoff.EventPumpMetrics{Presentation: app.handoffMetrics.Presentation, Duplicate: app.handoffMetrics.Duplicate},
			Tracer:  app.tracer, Gates: app.featureGates,
		}
		app.eventListener = app.presentation.Listener
		managerConfig.HandoffConsumerID = app.presentation.ConsumerID
		managerConfig.HandoffSubscriptions = app.eventPump
		managerConfig.PresentationPolicy = app.presentation.Policy
		managerConfig.SafetyPolicy = app.presentation.Safety
		managerConfig.Clock = app.presentation.Clock
	}

	app.managerConfig = managerConfig
	app.manager = session.NewManager(managerConfig, connector, app.ids)
	if approvals != nil {
		app.approvals = NewApprovalCoordinator(
			app.approvalStore, app.manager, defaultApprovalQueueCapacity, defaultApprovalRequestTimeout,
		)
		if app.approvals == nil {
			return nil, errors.New("durable approval coordinator is invalid")
		}
		approvals.coordinator = app.approvals
	}
	if app.durable != nil {
		app.transcripts = NewTranscriptCoordinator(app.durable.ConversationWriter, app.manager, app.durable.TranscriptQueueCapacity)
		if app.transcripts == nil {
			return nil, errors.New("durable transcript coordinator is invalid")
		}
		transcriptClock := clock.Clock(clock.Real{})
		if app.presentation != nil && !isNil(app.presentation.Clock) {
			transcriptClock = app.presentation.Clock
		}
		if cfg.Gateway.StorageGrace > 0 {
			app.transcripts.health = &durabilityHealth{}
			if err := app.transcripts.configureOutage(cfg.Gateway.StorageGrace, transcriptClock); err != nil {
				return nil, err
			}
		}
		if err := app.transcripts.configureJournal(app.durable.Journal, app.durable.DeliveryAffinity, app.durable.JournalOwner, app.durable.JournalClaimLease); err != nil {
			return nil, err
		}
		dispatcher.transcripts = app.transcripts
	}
	if cfg.Features.Handoffs {
		app.eventPump.Sink = app.manager
		app.handoffs = NewHandoffCoordinator(app.durable.Service, app.manager, app.durable.HandoffQueueCapacity)
		if app.handoffs == nil {
			return nil, errors.New("durable handoff coordinator is invalid")
		}
		if app.featureGates != nil {
			if err := app.handoffs.SetFeatureGates(app.featureGates); err != nil {
				return nil, err
			}
		}
		app.handoffs.tracer = app.tracer
		app.handoffs.metricsClock = app.presentation.Clock
		app.handoffs.ackMetric = app.handoffMetrics.Acknowledgement
		if cfg.Gateway.StorageGrace > 0 {
			app.handoffs.health = app.transcripts.health
			if err := app.handoffs.configureOutage(cfg.Gateway.StorageGrace, app.presentation.Clock); err != nil {
				return nil, err
			}
		}
		if err := app.handoffs.configureJournal(app.durable.Journal, app.durable.DeliveryAffinity, app.durable.JournalOwner, app.durable.JournalClaimLease); err != nil {
			return nil, err
		}
		dispatcher.handoffs = app.handoffs
	}
	app.manager.SetMetricsHooks(app.metricsHooks)
	app.handler = &trackedHandler{next: transportws.NewHandler(wsOptions, app.manager, app.admission), active: &app.activeHandlers, drainer: app.drainer}
	app.server = &http.Server{Addr: cfg.Gateway.ListenAddr, Handler: app.handler}
	return app, nil
}

type durableDispatcher struct {
	handoffs    *HandoffCoordinator
	transcripts *TranscriptCoordinator
	snapshot    SessionHandoffSnapshot
}

func (d *durableDispatcher) DispatchTranscript(_ context.Context, trusted session.TrustedSessionContext, effect session.TranscriptRequestedEffect) error {
	if trusted.Subject.Ephemeral {
		return nil
	}
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
	if trusted.Subject.Ephemeral {
		return nil
	}
	if d == nil || d.handoffs == nil {
		return errors.New("handoff coordinator is unavailable")
	}
	policy, err := toolpolicy.MarshalCanonical(effect.ToolPolicy)
	if err != nil {
		return errors.New("handoff tool policy is invalid")
	}
	snapshot := cloneHandoffSnapshot(d.snapshot)
	if effect.Profile.ProfileID != "" {
		snapshot.Profile = cloneHandoffProfile(effect.Profile)
	}
	return d.handoffs.Enqueue(HandoffWork{
		SessionID: trusted.IDs.SessionID, Incarnation: trusted.IDs.Incarnation,
		OriginEpoch: effect.OriginEpoch,
		Scope:       handoff.Scope{TenantID: trusted.Subject.TenantID, UserID: trusted.Subject.UserID, ConversationID: trusted.IDs.ConversationID},
		CallID:      effect.CallID, ResponseID: effect.ResponseID, ToolName: effect.ToolName,
		Arguments: append(json.RawMessage(nil), effect.Arguments...), Turn: effect.Turn,
		Transcript: effect.Transcript, ToolPolicy: policy, Snapshot: snapshot, RejectReason: effect.RejectReason,
	})
}

func validateHandoffSnapshot(snapshot SessionHandoffSnapshot) error {
	if len(snapshot.ToolPolicy) == 0 {
		policy := toolpolicy.DefaultPolicy()
		policy.WebSearch.Enabled = false
		snapshot.ToolPolicy, _ = toolpolicy.MarshalCanonical(policy)
	}
	_, err := handoff.ValidateCreateRequest(handoff.CreateRequest{
		Scope:     handoff.Scope{TenantID: "snapshot-validation", UserID: "snapshot-validation", ConversationID: "snapshot-validation"},
		SessionID: "snapshot-validation", SourceTurn: identity.TurnKey{ID: "snapshot-validation", Revision: 1},
		UserRequest: "snapshot-validation", Objective: "snapshot-validation",
		ContextRef: snapshot.ContextRef, ContextHash: snapshot.ContextHash, Deadline: snapshot.Deadline,
		TokenBudget: snapshot.TokenBudget, Priority: snapshot.Priority,
		ToolAllowlist:  append([]string(nil), snapshot.ToolAllowlist...),
		ToolPolicy:     append(json.RawMessage(nil), snapshot.ToolPolicy...),
		ApprovalPolicy: append(json.RawMessage(nil), snapshot.ApprovalPolicy...),
		Versions:       snapshot.Versions, Profile: cloneHandoffProfile(snapshot.Profile), PresentationHint: snapshot.PresentationHint,
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
	// Process cancellation is a request to begin draining, not permission to
	// tear down already-admitted client/provider streams. The drain sequence
	// below owns the only cancellation of serving I/O.
	serveCtx, cancelServe := context.WithCancel(context.WithoutCancel(ctx))
	serveCtx, runSpan := a.tracer.Start(serveCtx, "gateway.run")
	defer runSpan.End()
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
	coordinatorResult := make(chan coordinatorRunResult, 6)
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
	if a.approvals != nil {
		startCoordinator("approval coordinator", a.approvals.Run)
	}
	if a.eventListener != nil {
		startCoordinator("handoff event listener", func(ctx context.Context) error { return a.eventListener.Run(ctx, a.eventWakeups) })
	}
	if a.eventPump != nil {
		startCoordinator("handoff event pump", a.eventPump.Run)
	}
	if a.manager != nil && a.manager.ReconnectEnabled() {
		startCoordinator("reconnect owner maintenance", a.manager.RunReconnectMaintenance)
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
	forceCancellation := false
	select {
	case serveErr = <-serveResult:
		serveObserved = true
		forceCancellation = true
	case result := <-coordinatorResult:
		forceCancellation = true
		if result.err == nil && ctx.Err() == nil {
			result.err = errors.New("stopped unexpectedly")
		}
		if result.err != nil {
			coordinatorErr = fmt.Errorf("%s: %w", result.name, result.err)
		}
	case <-ctx.Done():
	case <-a.drainer.Begun():
	}

	// Every exit after Serve starts owns the same bounded teardown. In
	// particular, a permanent listener failure does not cancel request contexts
	// on its own, and Shutdown does not track hijacked WebSockets.
	a.drainer.Begin()
	if forceCancellation {
		cancelServe()
	}
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), a.shutdownTimeout)
	defer cancelShutdown()
	var cleanupErrs []error
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- a.server.Shutdown(shutdownCtx) }()
	activeDone := make(chan error, 1)
	go func() { activeDone <- a.activeHandlers.drain(shutdownCtx) }()
	drainErr := a.drainer.Wait(shutdownCtx)
	if drainErr != nil {
		cleanupErrs = append(cleanupErrs, fmt.Errorf("drain tracked gateway work: %w", drainErr))
		_ = a.server.Close()
	}
	// Existing sessions keep their contexts until work finishes or the public
	// drain deadline expires. Only then are remaining provider/client I/O calls
	// force-cancelled.
	cancelServe()
	if err := <-shutdownDone; err != nil && !errors.Is(err, http.ErrServerClosed) {
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
	if err := <-activeDone; err != nil {
		cleanupErrs = append(cleanupErrs, fmt.Errorf("wait for active gateway sessions: %w", err))
		_ = a.server.Close()
	}
	cleanupTimeout := a.cfg.Shutdown.CleanupTimeout
	if cleanupTimeout <= 0 {
		cleanupTimeout = 5 * time.Second
	}
	cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancelCleanup()
	if drainErr != nil {
		if err := a.drainer.Wait(cleanupCtx); err != nil {
			cleanupErrs = append(cleanupErrs, fmt.Errorf("force-clean active gateway sessions: %w", err))
		}
	}
	if coordinatorCount != 0 {
		select {
		case <-coordinatorDone:
		case <-cleanupCtx.Done():
			cleanupErrs = append(cleanupErrs, fmt.Errorf("wait for durable coordinators: %w", cleanupCtx.Err()))
		}
	}

	var primaryErr error
	if serveObserved && serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		primaryErr = fmt.Errorf("serve gateway: %w", serveErr)
	}
	return errors.Join(append([]error{coordinatorErr, primaryErr}, cleanupErrs...)...)
}

type trackedHandler struct {
	next    http.Handler
	active  *handlerTracker
	drainer *lifecycle.Drainer
}

func (h *trackedHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	token, ok := h.drainer.Acquire()
	if !ok {
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	defer token.Release()
	if !h.active.begin() {
		http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
		return
	}
	defer h.active.end()
	h.next.ServeHTTP(w, r)
}

func (a *App) ActiveSessions() int64 {
	if a == nil || a.manager == nil {
		return 0
	}
	return a.manager.ActiveSessions()
}

// ReconnectReady reports whether the bounded durable-owner maintenance loop
// has completed a successful sweep. It is a no-op when reconnect is disabled.
func (a *App) ReconnectReady(ctx context.Context) error {
	if a == nil || a.manager == nil {
		return errors.New("gateway session manager is unavailable")
	}
	return a.manager.ReconnectReady(ctx)
}
func (a *App) CapacitySnapshot() (active, limit int64) {
	if a == nil {
		return 0, 0
	}
	active = a.ActiveSessions()
	limit = int64(a.cfg.Admission.MaxSessions)
	return active, limit
}
func (a *App) Drainer() *lifecycle.Drainer {
	if a == nil {
		return nil
	}
	return a.drainer
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
		TenantID: "development",
		UserID:   "local-simulator",
		Scopes:   map[string]struct{}{"realtime": {}},
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
