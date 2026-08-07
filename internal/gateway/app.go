// Package gateway composes the downstream transport and realtime session
// manager into one HTTP server.
package gateway

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"sync"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/config"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
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

	app.manager = session.NewManager(managerConfig, connector, app.ids)
	app.handler = &trackedHandler{next: transportws.NewHandler(wsOptions, app.manager, app.admission), active: &app.activeHandlers}
	app.server = &http.Server{Addr: cfg.Gateway.ListenAddr, Handler: app.handler}
	return app, nil
}

func (a *App) Run(ctx context.Context) error {
	if a == nil || a.server == nil {
		return errors.New("gateway app is nil")
	}
	if ctx == nil {
		return errors.New("gateway context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
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
	select {
	case serveErr = <-serveResult:
		serveObserved = true
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

	var primaryErr error
	if serveObserved && serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		primaryErr = fmt.Errorf("serve gateway: %w", serveErr)
	}
	return errors.Join(append([]error{primaryErr}, cleanupErrs...)...)
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
