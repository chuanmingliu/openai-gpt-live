package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/admin"
	"github.com/chuanmingliu/openai-gpt-live/internal/admission"
	"github.com/chuanmingliu/openai-gpt-live/internal/auth"
	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/config"
	"github.com/chuanmingliu/openai-gpt-live/internal/feature"
	"github.com/chuanmingliu/openai-gpt-live/internal/gateway"
	"github.com/chuanmingliu/openai-gpt-live/internal/health"
	"github.com/chuanmingliu/openai-gpt-live/internal/lifecycle"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime/s2sws"
	"github.com/chuanmingliu/openai-gpt-live/internal/secretbuf"
	"github.com/chuanmingliu/openai-gpt-live/internal/session"
	"github.com/chuanmingliu/openai-gpt-live/internal/store/postgres"
	"github.com/chuanmingliu/openai-gpt-live/internal/telemetry"
	"github.com/chuanmingliu/openai-gpt-live/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const providerConnectTimeout = 10 * time.Second

var commandOutput io.Writer = os.Stdout

func main() {
	slog.SetDefault(slog.New(telemetry.NewRedactingHandler(slog.Default().Handler(), nil)))
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:], os.LookupEnv); err != nil {
		log.Printf("live gateway stopped: %v", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, lookup func(string) (string, bool)) (runErr error) {
	if handled, err := runCommand(ctx, args, lookup); handled {
		return err
	}
	flags := flag.NewFlagSet("live-gateway", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	configPath := flags.String("config", "", "path to gateway YAML configuration")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("extra arguments are not accepted")
	}
	if *configPath == "" {
		return errors.New("-config is required")
	}
	cfg, err := config.LoadUnresolved(*configPath)
	if err != nil {
		return fmt.Errorf("load gateway configuration: %w", err)
	}
	slog.SetDefault(slog.New(telemetry.NewRedactingHandler(slog.Default().Handler(), cfg.Safety.RedactionDeniedKeys)))
	if err := validateComposition(cfg); err != nil {
		return err
	}
	if err := verifyExpectedHash(cfg, lookup); err != nil {
		return err
	}
	if err := cfg.ResolveSecrets(lookup); err != nil {
		return fmt.Errorf("load gateway configuration: %w", err)
	}
	admissionController, closeAdmission, err := composeAdmission(ctx, cfg, lookup)
	if err != nil {
		return err
	}
	if closeAdmission != nil {
		defer func() {
			runErr = finishAdmission(runErr, closeAdmission)
		}()
	}
	featureRuntime, err := composeGatewayFeatures(ctx, cfg, lookup)
	if err != nil {
		return err
	}
	if featureRuntime != nil {
		defer featureRuntime.close()
	}
	apiKey := ""
	if cfg.Gateway.Voice.APIKeyEnv != "" {
		value, ok := lookup(cfg.Gateway.Voice.APIKeyEnv)
		if !ok {
			return errors.New("configured provider credential is unavailable")
		}
		apiKey = value
	}
	connector, err := s2sws.New(s2sws.Config{
		URL:            cfg.Gateway.Voice.URL,
		APIKey:         apiKey,
		Model:          cfg.Gateway.Voice.Model,
		ConnectTimeout: providerConnectTimeout,
		MaxFrameBytes:  int64(cfg.Gateway.WebSocket.MaxFrameBytes),
	})
	if err != nil {
		return fmt.Errorf("configure realtime connector: %w", err)
	}
	configHash, err := config.Hash(cfg)
	if err != nil {
		return err
	}
	registry := prometheus.NewRegistry()
	provider := cfg.Gateway.Voice.Provider
	if provider == "" {
		provider = "other"
	}
	instance := cfg.Admission.InstanceID
	if instance == "" {
		instance = "local"
	}
	metrics, err := telemetry.NewMetrics(registry, telemetry.MetricsConfig{Labels: telemetry.MetricLabels{Component: "gateway", Provider: provider, Region: "local", Instance: instance, ConfigHash: configHash}, AllowedComponents: []string{"gateway"}, AllowedProviders: []string{provider}, AllowedRegions: []string{"local"}, AllowedInstances: []string{instance}, AllowedConfigHashes: []string{configHash}, AllowedQueues: []string{"ingress", "egress", "reactor", "voice_control", "client_control", "handoff", "outbox"}, AllowedResults: []string{"ok", "error", "rejected", "dropped", "timeout", "suppressed", "replay", "shadow"}})
	if err != nil {
		return fmt.Errorf("configure gateway metrics: %w", err)
	}
	traceRuntime, err := telemetry.NewRuntime(ctx, telemetry.RuntimeConfig{Endpoint: cfg.Telemetry.OTLPEndpoint, Protocol: cfg.Telemetry.Protocol, Timeout: cfg.Telemetry.ExportTimeout, ServiceName: cfg.Telemetry.ServiceName, Component: "gateway", Provider: provider, Region: "local", Instance: instance, ConfigHash: configHash, Production: cfg.Mode == "production"})
	if err != nil {
		return fmt.Errorf("configure gateway telemetry: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		runErr = errors.Join(runErr, traceRuntime.Close(closeCtx))
	}()
	drainer := lifecycle.NewDrainer()
	ready := health.NewRegistry()
	_ = ready.RegisterDrainer(drainer)
	_ = ready.RegisterRequired("telemetry", health.CheckFunc(func(context.Context) error { return traceRuntime.Ready() }))
	if admissionController != nil {
		_ = ready.RegisterRequired("admission", admissionController)
	}
	if featureRuntime != nil {
		_ = ready.RegisterRequired("runtime_features", health.CheckFunc(func(context.Context) error {
			if !featureRuntime.gates.Healthy() {
				return errors.New("runtime feature refresh is unhealthy")
			}
			return nil
		}))
	}
	audioObserver := metrics.BindAudioTransit()
	ingressQueue, egressQueue := metrics.BindQueue("ingress"), metrics.BindQueue("egress")
	bargeObserver := metrics.BargeinSuppression.WithLabelValues("ok")
	queueObserver := func(name string) telemetry.QueueObserver {
		if name == "ingress" {
			return ingressQueue
		}
		if name == "egress" {
			return egressQueue
		}
		return telemetry.QueueObserver{}
	}
	var appOptions = []gateway.Option{gateway.WithDrainer(drainer), gateway.WithTracer(traceRuntime.TracerProvider().Tracer("live.gateway")), gateway.WithHandoffMetrics(gateway.HandoffMetricsHooks{Acknowledgement: func(d time.Duration, result string) { metrics.HandoffACK.WithLabelValues(result).Observe(d.Seconds()) }, Presentation: func(d time.Duration, result string) {
		metrics.PresentationDelay.WithLabelValues(result).Observe(d.Seconds())
	}, Duplicate: func(result string) { metrics.DuplicateDeliveries.WithLabelValues(result).Inc() }}), gateway.WithSessionMetrics(session.MetricsHooks{SessionStarted: metrics.SessionStarted, SessionEnded: metrics.SessionEnded, AudioTransit: audioObserver.ObserveDuration, QueueOverflow: func(q string) { metrics.ObserveQueueOverflow(q, "dropped") }, StaleOutputRejected: func() { metrics.StaleOutputRejected.WithLabelValues("rejected").Inc() }, ReconnectRestore: func(d time.Duration, result string) {
		metrics.ObserveSeconds(metrics.ReconnectRestore, result, d.Seconds())
	}, QueueState: func(name string, depth, bytes int, buffered time.Duration) {
		queueObserver(name).Set(depth, bytes, buffered)
	}, QueueBlocked: func(name string, d time.Duration) { queueObserver(name).ObserveBlocked(d) }, QueueOldest: func(name string, d time.Duration) { queueObserver(name).ObserveOldest(d) }, BargeinSuppression: func(d time.Duration) { bargeObserver.Observe(d.Seconds()) }})}
	if featureRuntime != nil {
		appOptions = append(appOptions, gateway.WithFeatureGates(featureRuntime.gates))
		appOptions = append(appOptions, gateway.WithApprovalStore(featureRuntime.repo))
	}
	if admissionController != nil {
		admissionController.SetMetricsObserver(metrics.ObserveAdmission)
		appOptions = append(appOptions, gateway.WithAdmission(admissionController))
	}
	app, err := gateway.New(cfg, connector, appOptions...)
	if err != nil {
		return fmt.Errorf("configure gateway: %w", err)
	}
	diagnostics, err := startGatewayDiagnostics(ctx, cfg, admissionController, app, drainer, ready, registry, featureRuntime)
	if err != nil {
		return err
	}
	if diagnostics != nil {
		defer diagnostics.close()
	}
	return runGateway(ctx, app, diagnostics)
}

type gatewayFeatureRuntime struct {
	pool            *pgxpool.Pool
	store           *feature.PostgresStore
	repo            *postgres.Repository
	gates           *feature.Gates
	cancel          context.CancelFunc
	done            chan error
	shutdownTimeout time.Duration
}

var composeGatewayRepositoryHook func(*postgres.Repository) error

func composeGatewayFeatures(ctx context.Context, cfg config.Config, lookup func(string) (string, bool)) (*gatewayFeatureRuntime, error) {
	if cfg.Mode != "production" {
		return nil, nil
	}
	databaseURL := cfg.Store.URL
	if cfg.Store.DatabaseURLEnv != "" {
		value, ok := lookup(cfg.Store.DatabaseURLEnv)
		if !ok || value == "" {
			return nil, errors.New("configured database credential is unavailable")
		}
		databaseURL = value
	}
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, errors.New("configure runtime feature storage")
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, errors.New("runtime feature storage is unavailable")
	}
	if err := migrations.Check(ctx, pool); err != nil {
		pool.Close()
		return nil, errors.New("runtime feature schema is unavailable")
	}
	store, err := feature.NewPostgres(pool, clock.Real{}, time.Second, cfg.Shutdown.CleanupTimeout)
	if err != nil {
		pool.Close()
		return nil, err
	}
	repositoryOptions := []postgres.RepositoryOption{postgres.WithApprovalTTL(cfg.Store.Retention.ApprovalTTL)}
	var retentionKey []byte
	defer clear(retentionKey)
	if cfg.Store.Retention.HMACKeyEnv != "" {
		encoded, ok := lookup(cfg.Store.Retention.HMACKeyEnv)
		if !ok {
			pool.Close()
			return nil, errors.New("retention HMAC key is unavailable")
		}
		decodeErr := secretbuf.WithDecodedBase64(encoded, func(key []byte) error {
			if len(key) < 32 || len(key) > 4096 {
				return errors.New("retention HMAC key length is invalid")
			}
			retentionKey = append([]byte(nil), key...)
			return nil
		})
		if decodeErr != nil {
			pool.Close()
			return nil, errors.New("retention HMAC key is invalid")
		}
		repositoryOptions = append(repositoryOptions, postgres.WithRetentionHMAC(cfg.Store.Retention.HMACKeyID, retentionKey))
	}
	repo, err := postgres.NewRepository(pool, clock.Real{}, repositoryOptions...)
	if err != nil {
		pool.Close()
		return nil, err
	}
	if composeGatewayRepositoryHook != nil {
		if err := composeGatewayRepositoryHook(repo); err != nil {
			repo.CloseSensitive()
			pool.Close()
			return nil, err
		}
	}
	gates := feature.New(nil)
	snapshot, err := store.Load(ctx)
	if err != nil || !gates.Apply(snapshot) {
		repo.CloseSensitive()
		pool.Close()
		return nil, errors.New("runtime feature authority is unavailable")
	}
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	runtime := &gatewayFeatureRuntime{pool: pool, store: store, repo: repo, gates: gates, cancel: cancel, done: make(chan error, 1), shutdownTimeout: cfg.Shutdown.CleanupTimeout}
	go func() {
		runtime.done <- gates.Run(runCtx, store, cfg.Features.RefreshInterval, cfg.Shutdown.CleanupTimeout)
	}()
	return runtime, nil
}

func (r *gatewayFeatureRuntime) close() {
	if r == nil {
		return
	}
	r.cancel()
	select {
	case <-r.done:
	case <-time.After(r.shutdownTimeout):
	}
	if r.repo != nil {
		r.repo.CloseSensitive()
	}
	r.pool.Close()
}

type gatewayCapacity struct{ app *gateway.App }

func (c gatewayCapacity) Capacity() admin.Capacity {
	active, limit := c.app.CapacitySnapshot()
	available := limit - active
	if available < 0 {
		available = 0
	}
	return admin.Capacity{Active: active, Limit: limit, Available: available}
}

type diagnosticsServer struct {
	server   *http.Server
	listener net.Listener
	errors   chan error
}

func startGatewayDiagnostics(ctx context.Context, cfg config.Config, authenticator auth.Authenticator, app *gateway.App, drainer *lifecycle.Drainer, ready *health.Registry, registry *prometheus.Registry, features *gatewayFeatureRuntime) (*diagnosticsServer, error) {
	if cfg.Admin.ListenAddr == "" {
		return nil, nil
	}
	if authenticator == nil {
		return nil, errors.New("admin authentication is unavailable")
	}
	if err := admin.ValidateListenAddress(ctx, cfg.Admin.ListenAddr, cfg.Mode == "production"); err != nil {
		return nil, err
	}
	adminConfig := admin.Config{Authenticator: authenticator, Drainer: drainer, Capacity: gatewayCapacity{app}, Sessions: app}
	if features != nil {
		adminConfig.FeatureStore, adminConfig.FeatureGates = features.store, features.gates
		adminConfig.Deletions = features.repo
	}
	adminHandler, err := admin.NewHandler(adminConfig)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("/health/live", health.LiveHandler())
	mux.Handle("/health/ready", ready.ReadyHandler())
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	mux.Handle("/admin/", adminHandler)
	server := admin.Server(cfg.Admin.ListenAddr, mux)
	listener, err := net.Listen("tcp", cfg.Admin.ListenAddr)
	if err != nil {
		return nil, errors.New("listen diagnostics")
	}
	d := &diagnosticsServer{server: server, listener: listener, errors: make(chan error, 1)}
	go func() {
		err := server.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		d.errors <- err
	}()
	return d, nil
}
func (d *diagnosticsServer) close() {
	if d == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = d.server.Shutdown(ctx)
	_ = d.listener.Close()
}
func runGateway(ctx context.Context, app *gateway.App, d *diagnosticsServer) error {
	if d == nil {
		return app.Run(ctx)
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	appDone := make(chan error, 1)
	go func() { appDone <- app.Run(runCtx) }()
	select {
	case err := <-appDone:
		return err
	case err := <-d.errors:
		cancel()
		return errors.Join(err, <-appDone)
	}
}

func runCommand(ctx context.Context, args []string, lookup func(string) (string, bool)) (bool, error) {
	if len(args) == 0 {
		return false, nil
	}
	switch args[0] {
	case "check-config", "config-hash":
		flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
		flags.SetOutput(io.Discard)
		path := flags.String("config", "", "configuration path")
		if err := flags.Parse(args[1:]); err != nil {
			return true, err
		}
		if *path == "" || flags.NArg() != 0 {
			return true, errors.New("--config is required and extra arguments are not accepted")
		}
		cfg, err := config.LoadUnresolved(*path)
		if err != nil {
			return true, err
		}
		if args[0] == "config-hash" {
			hash, err := config.Hash(cfg)
			if err != nil {
				return true, err
			}
			_, err = fmt.Fprintln(commandOutput, hash)
			return true, err
		}
		return true, nil
	case "drain":
		flags := flag.NewFlagSet("drain", flag.ContinueOnError)
		flags.SetOutput(io.Discard)
		raw := flags.String("url", "", "loopback admin URL")
		if err := flags.Parse(args[1:]); err != nil {
			return true, err
		}
		if *raw == "" || flags.NArg() != 0 {
			return true, errors.New("--url is required and extra arguments are not accepted")
		}
		token := ""
		if lookup != nil {
			token, _ = lookup("LIVE_ADMIN_BEARER_TOKEN")
		}
		return true, admin.DrainURL(ctx, nil, *raw, token)
	default:
		return false, nil
	}
}

func verifyExpectedHash(cfg config.Config, lookup func(string) (string, bool)) error {
	if lookup == nil {
		return nil
	}
	expected, ok := lookup("LIVE_EXPECTED_CONFIG_HASH")
	if !ok {
		return nil
	}
	actual, err := config.Hash(cfg)
	if err != nil {
		return err
	}
	return config.ValidateExpected(actual, expected)
}

func finishAdmission(runErr error, closeAdmission func() error) error {
	if closeAdmission == nil {
		return runErr
	}
	if err := closeAdmission(); err != nil {
		return errors.Join(runErr, fmt.Errorf("close admission storage: %w", err))
	}
	return runErr
}

func composeAdmission(ctx context.Context, cfg config.Config, lookup func(string) (string, bool)) (*admission.Controller, func() error, error) {
	var authenticator auth.Authenticator
	requiredScopes := cfg.Auth.OIDCRequiredScopes
	if cfg.Mode == "local" && cfg.Auth.StaticBearerEnv != "" {
		token, ok := lookup(cfg.Auth.StaticBearerEnv)
		if !ok || token == "" {
			return nil, nil, errors.New("configured static credential is unavailable")
		}
		scopes := make(map[string]struct{}, len(cfg.Auth.StaticScopes))
		for _, scope := range cfg.Auth.StaticScopes {
			scopes[scope] = struct{}{}
		}
		authenticator = auth.NewStatic([]byte(token), auth.Principal{TenantID: cfg.Auth.StaticTenantID, UserID: cfg.Auth.StaticUserID, Scopes: scopes})
		if len(requiredScopes) == 0 {
			requiredScopes = []string{"realtime"}
		}
	} else if cfg.Auth.OIDCIssuer != "" {
		oidcAuthenticator, err := auth.NewOIDC(ctx, auth.OIDCConfig{Issuer: cfg.Auth.OIDCIssuer, Audience: cfg.Auth.OIDCAudience, TenantClaim: cfg.Auth.OIDCTenantClaim, UserClaim: cfg.Auth.OIDCUserClaim, ScopeClaim: cfg.Auth.OIDCScopeClaim, RequiredScopes: cfg.Auth.OIDCRequiredScopes, MaxKeySetStaleness: cfg.Auth.OIDCCacheOutage, HTTPTimeout: cfg.Auth.OIDCHTTPTimeout})
		if err != nil {
			return nil, nil, errors.New("OIDC authentication is unavailable")
		}
		authenticator = oidcAuthenticator
	} else if cfg.Mode == "local" {
		return nil, nil, nil
	} else {
		return nil, nil, errors.New("production authentication is unavailable")
	}
	controllerConfig := admission.Config{RequireTLS: cfg.Mode == "production", BehindIngress: cfg.Gateway.BehindIngress, TrustedProxyCIDRs: cfg.Gateway.TrustedProxies, AllowedOrigins: cfg.Auth.AllowedOrigins, AllowEmptyOrigin: cfg.Auth.AllowNonBrowser, GlobalRate: cfg.Admission.GlobalRate, GlobalBurst: cfg.Admission.GlobalBurst, IPRate: cfg.Admission.IPRate, IPBurst: cfg.Admission.IPBurst, IPKeyTTL: cfg.Admission.IPKeyTTL, MaxIPKeys: cfg.Admission.MaxIPKeys, RequiredScopes: requiredScopes, Limits: admission.Limits{Global: cfg.Admission.MaxSessions, PerTenant: cfg.Admission.MaxSessionsPerTenant, PerUser: cfg.Admission.MaxSessionsPerUser}, RequireStore: cfg.Mode == "production"}
	if cfg.Mode != "production" {
		controller, err := admission.New(authenticator, controllerConfig)
		if err != nil {
			return nil, nil, fmt.Errorf("configure admission: %w", err)
		}
		return controller, nil, nil
	}
	databaseURL := cfg.Store.URL
	if cfg.Store.DatabaseURLEnv != "" {
		value, ok := lookup(cfg.Store.DatabaseURLEnv)
		if !ok || value == "" {
			return nil, nil, errors.New("configured database credential is unavailable")
		}
		databaseURL = value
	}
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, nil, errors.New("configure admission storage")
	}
	closePool := func() { pool.Close() }
	if err = pool.Ping(ctx); err != nil {
		closePool()
		return nil, nil, errors.New("admission storage is unavailable")
	}
	instanceID := cfg.Admission.InstanceID
	if instanceID == "" {
		instanceID, err = admission.RandomInstanceID()
		if err != nil {
			closePool()
			return nil, nil, errors.New("generate admission instance identity")
		}
	}
	store, err := admission.NewPostgres(pool, admission.PostgresConfig{InstanceID: instanceID, LeaseDuration: cfg.Admission.LeaseDuration, HeartbeatInterval: cfg.Admission.HeartbeatInterval})
	if err != nil {
		closePool()
		return nil, nil, fmt.Errorf("configure admission storage: %w", err)
	}
	controller, err := admission.New(authenticator, controllerConfig, store)
	if err != nil {
		closeErr := store.Close()
		closePool()
		if closeErr != nil {
			return nil, nil, errors.Join(fmt.Errorf("configure admission: %w", err), fmt.Errorf("close admission storage: %w", closeErr))
		}
		return nil, nil, fmt.Errorf("configure admission: %w", err)
	}
	return controller, func() error {
		err := store.Close()
		closePool()
		return err
	}, nil
}

func validateComposition(cfg config.Config) error {
	if cfg.Features.Handoffs {
		// The reviewed safety policy now exists, but this CLI still has no durable
		// repository/event-listener composition. Starting without those components
		// would acknowledge work that can never be delivered.
		return errors.New("handoff delivery is unavailable: durable repository and event listener composition is required")
	}
	return nil
}
