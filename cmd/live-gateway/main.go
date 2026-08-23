package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/chuanmingliu/openai-gpt-live/internal/admin"
	"github.com/chuanmingliu/openai-gpt-live/internal/admission"
	"github.com/chuanmingliu/openai-gpt-live/internal/auth"
	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/config"
	"github.com/chuanmingliu/openai-gpt-live/internal/feature"
	"github.com/chuanmingliu/openai-gpt-live/internal/gateway"
	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/health"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/lifecycle"
	"github.com/chuanmingliu/openai-gpt-live/internal/presentation"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime/s2sws"
	"github.com/chuanmingliu/openai-gpt-live/internal/safety"
	"github.com/chuanmingliu/openai-gpt-live/internal/secretbuf"
	"github.com/chuanmingliu/openai-gpt-live/internal/session"
	"github.com/chuanmingliu/openai-gpt-live/internal/store/postgres"
	"github.com/chuanmingliu/openai-gpt-live/internal/telemetry"
	transportws "github.com/chuanmingliu/openai-gpt-live/internal/transport/ws"
	"github.com/chuanmingliu/openai-gpt-live/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const providerConnectTimeout = 10 * time.Second

var commandOutput io.Writer = os.Stdout

func main() {
	telemetry.InstallDefaultRedactingLogger(nil)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:], os.LookupEnv); err != nil {
		slog.Error("live gateway stopped", "error", err)
		os.Exit(1)
	}
}

func deploymentRegion(mode string, lookup func(string) (string, bool)) (string, error) {
	region, regionOK := lookup("LIVE_DEPLOYMENT_REGION")
	if !regionOK && mode != "production" {
		return "local", nil
	}
	allowed, allowedOK := lookup("LIVE_ALLOWED_REGIONS")
	if !regionOK || !allowedOK {
		return "", errors.New("production requires LIVE_DEPLOYMENT_REGION and LIVE_ALLOWED_REGIONS")
	}
	return telemetry.ResolveDeploymentRegion(region, allowed)
}

func metricInstance(mode string, lookup func(string) (string, bool)) (string, error) {
	instance, ok := lookup("LIVE_METRIC_INSTANCE")
	if !ok || instance == "" {
		if mode == "production" {
			return "", errors.New("production requires LIVE_METRIC_INSTANCE")
		}
		return "local", nil
	}
	return instance, nil
}

func gatewayMetricLabels(cfg config.Config, configHash string, lookup func(string) (string, bool)) (telemetry.MetricLabels, error) {
	provider := cfg.Gateway.Voice.Provider
	if provider == "" {
		provider = "other"
	}
	instance, err := metricInstance(cfg.Mode, lookup)
	if err != nil {
		return telemetry.MetricLabels{}, err
	}
	region, err := deploymentRegion(cfg.Mode, lookup)
	if err != nil {
		return telemetry.MetricLabels{}, err
	}
	return telemetry.MetricLabels{Component: "gateway", Provider: provider, Region: region, Instance: instance, ConfigHash: configHash}, nil
}

func canaryRuntimeIdentity(mode, configHash, region string, lookup func(string) (string, bool)) (transportws.RuntimeIdentity, error) {
	if mode != "production" {
		return transportws.RuntimeIdentity{}, nil
	}
	image, ok := lookup("LIVE_GATEWAY_IMAGE")
	if !ok || image == "" {
		return transportws.RuntimeIdentity{}, errors.New("production requires LIVE_GATEWAY_IMAGE")
	}
	identity := transportws.RuntimeIdentity{ConfigHash: configHash, Region: region, GatewayImage: image}
	if err := identity.Validate(); err != nil {
		return transportws.RuntimeIdentity{}, fmt.Errorf("production canary runtime identity: %w", err)
	}
	return identity, nil
}

func loadGatewayConfig(path string) (config.Config, error) {
	if path == "" {
		return config.DefaultLocalGateway()
	}
	return config.LoadUnresolved(path)
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
	cfg, err := loadGatewayConfig(*configPath)
	if err != nil {
		return fmt.Errorf("load gateway configuration: %w", err)
	}
	telemetry.InstallDefaultRedactingLogger(cfg.Safety.RedactionDeniedKeys)
	if err := validateComposition(cfg); err != nil {
		return err
	}
	if err := verifyExpectedHash(cfg, lookup); err != nil {
		return err
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
	metricLabels, err := gatewayMetricLabels(cfg, configHash, lookup)
	if err != nil {
		return err
	}
	provider, region, instance := metricLabels.Provider, metricLabels.Region, metricLabels.Instance
	canaryIdentity, err := canaryRuntimeIdentity(cfg.Mode, configHash, region, lookup)
	if err != nil {
		return err
	}
	if err := telemetry.RegisterRuntimeCollectors(registry, metricLabels); err != nil {
		return fmt.Errorf("configure gateway runtime metrics: %w", err)
	}
	metrics, err := telemetry.NewMetrics(registry, telemetry.MetricsConfig{Labels: metricLabels, AllowedComponents: []string{"gateway"}, AllowedProviders: []string{provider}, AllowedRegions: []string{region}, AllowedInstances: []string{instance}, AllowedConfigHashes: []string{configHash}, AllowedQueues: []string{"ingress", "egress", "reactor", "voice_control", "client_control", "handoff", "outbox"}, AllowedResults: []string{"ok", "error", "rejected", "dropped", "timeout", "suppressed", "replay", "shadow"}})
	if err != nil {
		return fmt.Errorf("configure gateway metrics: %w", err)
	}
	traceRuntime, err := telemetry.NewRuntime(ctx, telemetry.RuntimeConfig{Endpoint: cfg.Telemetry.OTLPEndpoint, Protocol: cfg.Telemetry.Protocol, Timeout: cfg.Telemetry.ExportTimeout, ServiceName: cfg.Telemetry.ServiceName, Component: "gateway", Provider: provider, Region: region, Instance: instance, ConfigHash: configHash, Production: cfg.Mode == "production"})
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
	if cfg.Mode == "production" {
		appOptions = append(appOptions, gateway.WithCanaryRuntimeIdentity(canaryIdentity))
	}
	if featureRuntime != nil {
		appOptions = append(appOptions, gateway.WithFeatureGates(featureRuntime.gates))
		appOptions = append(appOptions, gateway.WithApprovalStore(featureRuntime.repo))
	}
	durableOptions, err := composeGatewayDurability(cfg, featureRuntime, configHash, instance, region)
	if err != nil {
		return err
	}
	appOptions = append(appOptions, durableOptions...)
	reconnectOption, err := composeGatewayReconnect(cfg, featureRuntime, lookup)
	if err != nil {
		return err
	}
	if reconnectOption != nil {
		appOptions = append(appOptions, reconnectOption)
	}
	if admissionController != nil {
		admissionController.SetMetricsObserver(metrics.ObserveAdmission)
		appOptions = append(appOptions, gateway.WithAdmission(admissionController))
	}
	app, err := gateway.New(cfg, connector, appOptions...)
	if err != nil {
		return fmt.Errorf("configure gateway: %w", err)
	}
	if cfg.Mode == "production" {
		_ = ready.RegisterRequired("reconnect_owners", health.CheckFunc(app.ReconnectReady))
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

func resolveGatewayDatabaseURL(cfg config.Config, lookup func(string) (string, bool)) (string, error) {
	databaseURL := cfg.Store.URL
	if cfg.Store.DatabaseURLEnv != "" {
		value, ok := lookup(cfg.Store.DatabaseURLEnv)
		if !ok || value == "" {
			return "", errors.New("configured database credential is unavailable")
		}
		databaseURL = value
	}
	if cfg.Mode == "production" && strings.Contains(databaseURL, "sslmode=disable") {
		return "", errors.New("production database URL must not disable SSL")
	}
	return databaseURL, nil
}

func composeGatewayFeatures(ctx context.Context, cfg config.Config, lookup func(string) (string, bool)) (*gatewayFeatureRuntime, error) {
	if cfg.Mode != "production" {
		return nil, nil
	}
	databaseURL, err := resolveGatewayDatabaseURL(cfg, lookup)
	if err != nil {
		return nil, err
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
	server          *http.Server
	listener        net.Listener
	metricsServer   *http.Server
	metricsListener net.Listener
	errors          chan error
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
	d := &diagnosticsServer{server: server, listener: listener, errors: make(chan error, 2)}
	go func() {
		err := server.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		d.errors <- err
	}()
	if cfg.Mode == "production" {
		metricsMux := http.NewServeMux()
		metricsMux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
		d.metricsServer = &http.Server{Handler: metricsMux, ReadHeaderTimeout: 5 * time.Second}
		d.metricsListener, err = net.Listen("tcp", ":9091")
		if err != nil {
			d.close()
			return nil, errors.New("listen gateway metrics")
		}
		go func() {
			err := d.metricsServer.Serve(d.metricsListener)
			if errors.Is(err, http.ErrServerClosed) {
				err = nil
			}
			d.errors <- err
		}()
	}
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
	if d.metricsServer != nil {
		_ = d.metricsServer.Shutdown(ctx)
	}
	if d.metricsListener != nil {
		_ = d.metricsListener.Close()
	}
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
	controllerConfig := admission.Config{RequireTLS: cfg.Mode == "production", BehindIngress: cfg.Gateway.BehindIngress, TrustedProxyCIDRs: cfg.Gateway.TrustedProxies, AllowedOrigins: cfg.Auth.AllowedOrigins, AllowEmptyOrigin: cfg.Auth.AllowNonBrowser, GlobalRate: cfg.Admission.GlobalRate, GlobalBurst: cfg.Admission.GlobalBurst, IPRate: cfg.Admission.IPRate, IPBurst: cfg.Admission.IPBurst, IPKeyTTL: cfg.Admission.IPKeyTTL, MaxIPKeys: cfg.Admission.MaxIPKeys, RequiredScopes: requiredScopes, EphemeralTenantID: cfg.Auth.EphemeralTenantID, EphemeralUserID: cfg.Auth.EphemeralUserID, Limits: admission.Limits{Global: cfg.Admission.MaxSessions, PerTenant: cfg.Admission.MaxSessionsPerTenant, PerUser: cfg.Admission.MaxSessionsPerUser}, RequireStore: cfg.Mode == "production"}
	if cfg.Mode != "production" {
		controller, err := admission.New(authenticator, controllerConfig)
		if err != nil {
			return nil, nil, fmt.Errorf("configure admission: %w", err)
		}
		return controller, nil, nil
	}
	databaseURL, err := resolveGatewayDatabaseURL(cfg, lookup)
	if err != nil {
		return nil, nil, err
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
	if cfg.Features.Handoffs && cfg.Mode != "production" {
		return errors.New("handoff delivery requires production durability composition")
	}
	return nil
}

type gatewayHandoffIDs struct{}

func (gatewayHandoffIDs) NewHandoffID() identity.HandoffID {
	return identity.HandoffID(randomGatewayIdentifier("handoff"))
}

func (gatewayHandoffIDs) NewEventID() string { return randomGatewayIdentifier("event") }

func randomGatewayIdentifier(prefix string) string {
	var raw [24]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return ""
	}
	return prefix + "-" + base64.RawURLEncoding.EncodeToString(raw[:])
}

type gatewayCryptoJitter struct{}

func (gatewayCryptoJitter) Duration(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return 0
	}
	return time.Duration(binary.BigEndian.Uint64(raw[:]) % (uint64(max) + 1))
}

func composeGatewayDurability(cfg config.Config, runtime *gatewayFeatureRuntime, configHash, runtimeInstance, deliveryRegion string) ([]gateway.Option, error) {
	if cfg.Mode != "production" {
		return nil, nil
	}
	if runtime == nil || runtime.repo == nil {
		return nil, errors.New("conversation durability runtime is unavailable")
	}
	owner, err := gatewayDurabilityIdentity(runtimeInstance)
	if err != nil {
		return nil, err
	}
	shard, err := gatewayDeliveryShard(deliveryRegion)
	if err != nil {
		return nil, err
	}
	dependencies := gateway.DurableDependencies{
		ConversationWriter: runtime.repo, ConversationProvisioner: runtime.repo,
		ConversationTTL: cfg.Store.Retention.ConversationTTL, RecoveryStore: runtime.repo, Journal: runtime.repo,
		DeliveryAffinity: shard, JournalOwner: owner, JournalClaimLease: cfg.Admission.LeaseDuration,
		TranscriptQueueCapacity: cfg.Gateway.Session.ControlEvents,
	}
	if !cfg.Features.Handoffs {
		return []gateway.Option{gateway.WithDurableHandoffs(dependencies)}, nil
	}
	if runtime.pool == nil {
		return nil, errors.New("handoff durability runtime is unavailable")
	}
	service, err := handoff.NewService(runtime.repo, clock.Real{}, gatewayHandoffIDs{})
	if err != nil {
		return nil, fmt.Errorf("configure handoff service: %w", err)
	}
	listener, err := postgres.NewListener(runtime.pool, clock.Real{}, gatewayCryptoJitter{}, postgres.ListenerConfig{
		ReconnectDelay: maxDuration(cfg.Worker.PollInterval, 10*time.Millisecond),
		MaxJitter:      100 * time.Millisecond,
	})
	if err != nil {
		return nil, fmt.Errorf("configure handoff listener: %w", err)
	}
	policy, err := safety.NewStaticPolicy(safety.StaticConfig{
		Mode: safety.StaticMode(cfg.Safety.Mode), MaxSummaryBytes: cfg.Safety.MaxSummaryBytes,
		MaxStructuredBytes: cfg.Safety.MaxStructuredBytes, RedactedSummary: cfg.Safety.RedactedSummary,
		ClarificationPrompt: cfg.Safety.ClarificationPrompt,
	})
	if err != nil {
		return nil, fmt.Errorf("configure handoff presentation safety: %w", err)
	}
	snapshot := gateway.SessionHandoffSnapshot{
		ContextRef: "context://conversation/current", ContextHash: configHash,
		Deadline:    time.Date(9999, time.December, 31, 23, 59, 59, 0, time.UTC),
		TokenBudget: cfg.Worker.MaxOutputTokens, ToolAllowlist: []string{}, ApprovalPolicy: json.RawMessage(`{"mode":"none"}`),
		Versions:         handoff.Versions{Model: cfg.Worker.Reasoner.Model, Prompt: cfg.Safety.PolicyVersion, ToolRegistry: configHash, Config: configHash},
		PresentationHint: "brief",
	}
	dependencies.Service = service
	dependencies.HandoffQueueCapacity = cfg.Gateway.Session.ControlEvents
	dependencies.Snapshot = snapshot
	return []gateway.Option{
		gateway.WithDurableHandoffs(dependencies),
		gateway.WithHandoffPresentation(gateway.HandoffPresentationDependencies{
			Reader: runtime.repo, Cursors: runtime.repo, Listener: listener, Clock: clock.Real{},
			PollInterval: maxDuration(cfg.Worker.PollInterval, 10*time.Millisecond), ConsumerID: owner,
			WakeupCapacity: cfg.Gateway.Session.ControlEvents, Policy: presentation.NewPolicy(presentation.Config{}), Safety: policy,
		}),
	}, nil
}

func gatewayDurabilityIdentity(runtimeInstance string) (string, error) {
	if strings.TrimSpace(runtimeInstance) == "" || strings.TrimSpace(runtimeInstance) != runtimeInstance ||
		len(runtimeInstance) > session.DurableIdentifierBytes || !utf8.ValidString(runtimeInstance) {
		return "", errors.New("gateway runtime identity is required for durable ownership")
	}
	digest := sha256.Sum256([]byte(runtimeInstance))
	return "gateway-" + hex.EncodeToString(digest[:16]), nil
}

func gatewayDeliveryShard(region string) (string, error) {
	if strings.TrimSpace(region) == "" || strings.TrimSpace(region) != region ||
		len(region) > session.DurableIdentifierBytes || !utf8.ValidString(region) {
		return "", errors.New("gateway delivery region is required for durable ownership")
	}
	digest := sha256.Sum256([]byte("gateway-delivery-shard/v1\x00" + region))
	return "gateway-shard-" + hex.EncodeToString(digest[:16]), nil
}

func composeGatewayReconnect(cfg config.Config, runtime *gatewayFeatureRuntime, lookup func(string) (string, bool)) (gateway.Option, error) {
	if cfg.Mode != "production" {
		return nil, nil
	}
	if runtime == nil || runtime.pool == nil {
		return nil, errors.New("reconnect authority runtime is unavailable")
	}
	ownerID, err := metricInstance(cfg.Mode, lookup)
	if err != nil {
		return nil, fmt.Errorf("configure reconnect owner identity: %w", err)
	}
	if lookup == nil {
		return nil, errors.New("reconnect HMAC key is unavailable")
	}
	encoded, ok := lookup(cfg.Gateway.Reconnect.HMACKeyEnv)
	if !ok {
		return nil, errors.New("reconnect HMAC key is unavailable")
	}
	var credentials session.ReconnectCredentials
	if err := secretbuf.WithDecodedBase64(encoded, func(key []byte) error {
		if len(key) < 32 || len(key) > 4096 {
			return errors.New("reconnect HMAC key length is invalid")
		}
		configured, err := session.NewReconnectCredentials(key, clock.Real{})
		if err != nil {
			return err
		}
		credentials = configured
		return nil
	}); err != nil {
		return nil, errors.New("reconnect HMAC key is invalid")
	}
	owners, err := postgres.NewReconnectOwners(runtime.pool)
	if err != nil {
		return nil, fmt.Errorf("configure reconnect owner store: %w", err)
	}
	return gateway.WithReconnectAuthority(credentials, owners, ownerID, cfg.Gateway.Reconnect.CredentialTTL), nil
}

func maxDuration(left, right time.Duration) time.Duration {
	if left > right {
		return left
	}
	return right
}
