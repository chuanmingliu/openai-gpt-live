package main

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/adaptercatalog"
	knowledgeadapter "github.com/chuanmingliu/openai-gpt-live/internal/adapters/knowledge"
	"github.com/chuanmingliu/openai-gpt-live/internal/admin"
	"github.com/chuanmingliu/openai-gpt-live/internal/auth"
	"github.com/chuanmingliu/openai-gpt-live/internal/cliopt"
	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/config"
	"github.com/chuanmingliu/openai-gpt-live/internal/feature"
	"github.com/chuanmingliu/openai-gpt-live/internal/health"
	"github.com/chuanmingliu/openai-gpt-live/internal/knowledge"
	"github.com/chuanmingliu/openai-gpt-live/internal/lifecycle"
	"github.com/chuanmingliu/openai-gpt-live/internal/profile"
	"github.com/chuanmingliu/openai-gpt-live/internal/prompttmpl"
	"github.com/chuanmingliu/openai-gpt-live/internal/reasoner/deepseek"
	retentioncontrol "github.com/chuanmingliu/openai-gpt-live/internal/retention"
	"github.com/chuanmingliu/openai-gpt-live/internal/safety"
	"github.com/chuanmingliu/openai-gpt-live/internal/secretbuf"
	"github.com/chuanmingliu/openai-gpt-live/internal/store/postgres"
	"github.com/chuanmingliu/openai-gpt-live/internal/telemetry"
	"github.com/chuanmingliu/openai-gpt-live/internal/tools/websearch"
	"github.com/chuanmingliu/openai-gpt-live/internal/worker"
	"github.com/chuanmingliu/openai-gpt-live/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type resolvedSecrets struct {
	DatabaseURL       string
	ProviderAPIKey    string
	PrivacyHMACKey    []byte
	ToolEncryptionKey []byte
	RetentionHMACKey  []byte
	AdminBearer       string
	DeploymentRegion  string
	raw               []string
}

func (s *resolvedSecrets) clear() {
	clear(s.PrivacyHMACKey)
	clear(s.ToolEncryptionKey)
	clear(s.RetentionHMACKey)
	s.DatabaseURL, s.ProviderAPIKey, s.AdminBearer = "", "", ""
	for index := range s.raw {
		s.raw[index] = ""
	}
}

type serviceRuntime interface {
	Run(context.Context) error
	Close()
}

type runtimeBuilder func(context.Context, config.Config, resolvedSecrets) (serviceRuntime, error)

const (
	workerSecretMaximumBytes     = 16 * 1024
	workerHMACEncodedMaximumSize = 4 * ((4096 + 2) / 3)
)

type backendFlag struct {
	value   string
	present bool
}

func (b *backendFlag) Set(value string) error {
	if b.present {
		return errors.New("worker backend was provided more than once")
	}
	if value != "none" && value != "durable" {
		return errors.New("worker backend is invalid")
	}
	b.value = value
	b.present = true
	return nil
}

func (*backendFlag) String() string { return "" }

var buildRuntimeRepositoryHook func(*postgres.Repository) error

var commandOutput io.Writer = os.Stdout

func main() {
	telemetry.InstallDefaultRedactingLogger(nil)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := runWithBuilders(ctx, os.Args[1:], os.LookupEnv, buildRuntime, buildIdleRuntime); err != nil {
		slog.Error("live worker stopped", "error", err)
		os.Exit(1)
	}
}

func runWithBuilders(ctx context.Context, args []string, lookup func(string) (string, bool), durableBuilder runtimeBuilder, idleBuilder idleRuntimeBuilder) error {
	if ctx == nil {
		return errors.New("worker context is required")
	}
	if handled, err := runCommand(ctx, args, lookup); handled {
		return err
	}
	flags := flag.NewFlagSet("live-worker", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var configPath cliopt.OptionalPath
	var backend backendFlag
	flags.Var(&configPath, "config", "path to worker YAML configuration")
	flags.Var(&backend, "backend", "worker backend: none or durable")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("positional arguments are not accepted")
	}
	if backend.value == "none" {
		if configPath.Present() {
			return errors.New("-config is not accepted with --backend none")
		}
		runtime, err := idleBuilder(ctx)
		if err != nil {
			return err
		}
		if runtime == nil {
			return errors.New("idle worker runtime builder returned nil")
		}
		defer runtime.Close()
		err = runtime.Run(ctx)
		if errors.Is(err, context.Canceled) && ctx.Err() != nil {
			return nil
		}
		return err
	}
	if !configPath.Present() {
		return errors.New("-config is required")
	}
	if lookup == nil {
		lookup = os.LookupEnv
	}
	cfg, err := config.LoadUnresolved(configPath.Path())
	if err != nil {
		return fmt.Errorf("load worker configuration: %w", err)
	}
	if err := verifyExpectedHash(cfg, lookup); err != nil {
		return err
	}
	telemetry.InstallDefaultRedactingLogger(cfg.Safety.RedactionDeniedKeys)
	secrets, err := resolveSecrets(cfg, lookup)
	if err != nil {
		return err
	}
	redactions := append([]string(nil), secrets.raw...)
	defer secrets.clear()
	runtime, err := durableBuilder(ctx, cfg, secrets)
	// Builders own any copies needed by the runtime. Clear the resolver-owned
	// byte buffers before Run begins, on both success and error paths.
	secrets.clear()
	if err != nil {
		return redactError(err, redactions)
	}
	if runtime == nil {
		return errors.New("worker runtime builder returned nil")
	}
	defer runtime.Close()
	err = runtime.Run(ctx)
	if errors.Is(err, context.Canceled) && ctx.Err() != nil {
		return nil
	}
	return redactError(err, redactions)
}

func runCommand(ctx context.Context, args []string, lookup func(string) (string, bool)) (bool, error) {
	if len(args) == 0 {
		return false, nil
	}
	switch args[0] {
	case "check-config", "config-hash":
		flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
		flags.SetOutput(io.Discard)
		var path cliopt.OptionalPath
		flags.Var(&path, "config", "configuration path")
		if err := flags.Parse(args[1:]); err != nil {
			return true, err
		}
		if !path.Present() || flags.NArg() != 0 {
			return true, errors.New("--config is required and extra arguments are not accepted")
		}
		cfg, err := config.LoadUnresolved(path.Path())
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

func resolveSecrets(cfg config.Config, lookup func(string) (string, bool)) (resolvedSecrets, error) {
	var secrets resolvedSecrets
	region, regionOK := lookup("LIVE_DEPLOYMENT_REGION")
	allowed, allowedOK := lookup("LIVE_ALLOWED_REGIONS")
	if !regionOK && cfg.Mode != "production" {
		secrets.DeploymentRegion = "local"
	} else if !regionOK || !allowedOK {
		return resolvedSecrets{}, errors.New("production requires LIVE_DEPLOYMENT_REGION and LIVE_ALLOWED_REGIONS")
	} else {
		var err error
		secrets.DeploymentRegion, err = telemetry.ResolveDeploymentRegion(region, allowed)
		if err != nil {
			return resolvedSecrets{}, err
		}
	}
	require := func(name, label string, maximumBytes int) (string, error) {
		if name == "" {
			return "", fmt.Errorf("%s environment variable is not configured", label)
		}
		value, err := secretbuf.LookupText(lookup, name, maximumBytes)
		if err != nil {
			return "", fmt.Errorf("configured %s %s is unavailable", label, name)
		}
		secrets.raw = append(secrets.raw, value)
		return value, nil
	}
	var err error
	secrets.DatabaseURL, err = require(cfg.Store.DatabaseURLEnv, "database URL", workerSecretMaximumBytes)
	if err != nil {
		return resolvedSecrets{}, err
	}
	secrets.ProviderAPIKey, err = require(cfg.Worker.Reasoner.APIKeyEnv, "provider credential", workerSecretMaximumBytes)
	if err != nil {
		return resolvedSecrets{}, err
	}
	privacy, err := require(cfg.Safety.PrivacyHMACKeyEnv, "privacy HMAC key", workerSecretMaximumBytes)
	if err != nil {
		return resolvedSecrets{}, err
	}
	secrets.PrivacyHMACKey = append([]byte(nil), privacy...)
	if len(secrets.PrivacyHMACKey) < 32 {
		secrets.clear()
		return resolvedSecrets{}, errors.New("configured privacy HMAC key is too short")
	}
	if name := cfg.Features.ToolEncryptionKeyEnv; name != "" {
		encoded, err := require(name, "tool encryption key", workerHMACEncodedMaximumSize)
		if err != nil {
			secrets.clear()
			return resolvedSecrets{}, err
		}
		if err := secretbuf.WithDecodedBase64(encoded, func(key []byte) error {
			if len(key) != 32 {
				return errors.New("tool encryption key length is invalid")
			}
			secrets.ToolEncryptionKey = append([]byte(nil), key...)
			return nil
		}); err != nil {
			secrets.clear()
			return resolvedSecrets{}, errors.New("configured tool encryption key is invalid")
		}
	}
	if cfg.Admin.ListenAddr != "" && !cfg.IsProduction() && cfg.UsesStaticAuth() {
		secrets.AdminBearer, err = require(cfg.Auth.StaticBearerEnv, "admin bearer", workerSecretMaximumBytes)
		if err != nil {
			secrets.clear()
			return resolvedSecrets{}, err
		}
	}
	if cfg.Store.Retention.HMACKeyEnv != "" {
		encoded, err := require(cfg.Store.Retention.HMACKeyEnv, "retention HMAC key", workerHMACEncodedMaximumSize)
		if err != nil {
			secrets.clear()
			return resolvedSecrets{}, err
		}
		if err := secretbuf.WithDecodedBase64(encoded, func(key []byte) error {
			if len(key) < 32 || len(key) > 4096 {
				return errors.New("retention HMAC key length is invalid")
			}
			secrets.RetentionHMACKey = append([]byte(nil), key...)
			return nil
		}); err != nil {
			secrets.clear()
			return resolvedSecrets{}, errors.New("configured retention HMAC key is invalid")
		}
	}
	return secrets, nil
}

func redactError(err error, secrets []string) error {
	if err == nil {
		return nil
	}
	owned := append([]string(nil), secrets...)
	sort.Slice(owned, func(i, j int) bool { return len(owned[i]) > len(owned[j]) })
	message := err.Error()
	for _, secret := range owned {
		if secret != "" {
			message = strings.ReplaceAll(message, secret, "[redacted]")
		}
	}
	return errors.New(message)
}

type cryptoJitter struct{}

func (cryptoJitter) Duration(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return 0
	}
	return time.Duration(binary.BigEndian.Uint64(raw[:]) % (uint64(max) + 1))
}

type productionRuntime struct {
	pool               *pgxpool.Pool
	runner             *worker.Runner
	publisher          *postgres.OutboxPublisher
	repo               *postgres.Repository
	clock              clock.Clock
	sweeper            *retentioncontrol.Sweeper
	secretKey          []byte
	sealer             worker.ToolArgumentSealer
	drainTimeout       time.Duration
	components         []func(context.Context) error
	closeOnce          sync.Once
	drained            atomic.Bool
	closeRequested     atomic.Bool
	drainer            *lifecycle.Drainer
	cleanupTimeout     time.Duration
	telemetry          *telemetry.Runtime
	diagnostics        *http.Server
	diagnosticListener net.Listener
	metricsServer      *http.Server
	metricsListener    net.Listener
	gates              *feature.Gates
	featureStore       *feature.PostgresStore
	featureInterval    time.Duration
}

func buildRuntime(ctx context.Context, cfg config.Config, secrets resolvedSecrets) (serviceRuntime, error) {
	defer secrets.clear()
	poolConfig, err := pgxpool.ParseConfig(secrets.DatabaseURL)
	if err != nil {
		return nil, errors.New("database URL is invalid")
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, errors.New("database connection failed")
	}
	closeOnError := true
	var argumentSealer worker.ToolArgumentSealer
	var repo *postgres.Repository
	var traceRuntime *telemetry.Runtime
	var diagnosticListener net.Listener
	var metricsListener net.Listener
	defer func() {
		if closeOnError {
			if diagnosticListener != nil {
				_ = diagnosticListener.Close()
			}
			if metricsListener != nil {
				_ = metricsListener.Close()
			}
			if traceRuntime != nil {
				closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_ = traceRuntime.Close(closeCtx)
				cancel()
			}
			if argumentSealer != nil {
				_ = argumentSealer.Close()
			}
			if repo != nil {
				repo.CloseSensitive()
			}
			pool.Close()
		}
	}()
	if err := pool.Ping(ctx); err != nil {
		return nil, errors.New("database ping failed")
	}
	if err := migrations.Check(ctx, pool); err != nil {
		return nil, fmt.Errorf("database schema check failed: %w", err)
	}
	realClock := clock.Real{}
	jitter := cryptoJitter{}
	configHash, err := config.Hash(cfg)
	if err != nil {
		return nil, err
	}
	provider := cfg.Worker.Reasoner.Provider
	if provider == "" {
		provider = "other"
	}
	instance := workerIdentity()
	metricRegistry := prometheus.NewRegistry()
	region := secrets.DeploymentRegion
	metricLabels := telemetry.MetricLabels{Component: "worker", Provider: provider, Region: region, Instance: instance, ConfigHash: configHash}
	if err := telemetry.RegisterRuntimeCollectors(metricRegistry, metricLabels); err != nil {
		return nil, err
	}
	metrics, err := telemetry.NewMetrics(metricRegistry, telemetry.MetricsConfig{Labels: metricLabels, AllowedComponents: []string{"worker"}, AllowedProviders: []string{provider}, AllowedRegions: []string{region}, AllowedInstances: []string{instance}, AllowedConfigHashes: []string{configHash}, AllowedQueues: []string{"worker", "outbox"}, AllowedResults: []string{"ok", "error", "scheduled", "expired", "shadow"}})
	if err != nil {
		return nil, err
	}
	traceRuntime, err = telemetry.NewRuntime(ctx, telemetry.RuntimeConfig{Endpoint: cfg.Telemetry.OTLPEndpoint, Protocol: cfg.Telemetry.Protocol, Timeout: cfg.Telemetry.ExportTimeout, ServiceName: cfg.Telemetry.ServiceName, Component: "worker", Provider: provider, Region: region, Instance: instance, ConfigHash: configHash, Production: cfg.Mode == "production"})
	if err != nil {
		return nil, err
	}
	var knowledgeSearchers []knowledgeadapter.Searcher
	if cfg.Features.VerticalProfiles.Enabled {
		// The material view and the final worker repository share one pool. The
		// separate view breaks the catalog/repository construction cycle without
		// widening either interface: search sees knowledge.Store only, while the
		// final repository receives the compiled tool gate below.
		materialRepo, materialErr := postgres.NewRepository(pool, realClock)
		if materialErr != nil {
			return nil, materialErr
		}
		knowledgeSearchers = []knowledgeadapter.Searcher{knowledge.NewSearcher(materialRepo.KnowledgeStore())}
	}
	registry, err := workerRegistryForConfig(cfg, configHash, net.DefaultResolver, knowledgeSearchers...)
	if err != nil {
		return nil, err
	}
	repositoryOptions := []postgres.RepositoryOption{postgres.WithToolGate(registry), postgres.WithApprovalTTL(cfg.Store.Retention.ApprovalTTL)}
	if len(secrets.RetentionHMACKey) != 0 {
		repositoryOptions = append(repositoryOptions, postgres.WithRetentionHMAC(cfg.Store.Retention.HMACKeyID, secrets.RetentionHMACKey))
	}
	repo, err = postgres.NewRepository(pool, realClock, repositoryOptions...)
	if err != nil {
		return nil, err
	}
	if buildRuntimeRepositoryHook != nil {
		if err := buildRuntimeRepositoryHook(repo); err != nil {
			return nil, err
		}
	}
	featureStore, err := feature.NewPostgres(pool, realClock, time.Second, cfg.Shutdown.CleanupTimeout)
	if err != nil {
		return nil, err
	}
	gates := feature.New(nil)
	authority, err := featureStore.Load(ctx)
	if err != nil || !gates.Apply(authority) {
		return nil, errors.New("runtime feature authority is unavailable")
	}
	client, err := deepseek.New(deepseek.Config{BaseURL: cfg.Worker.Reasoner.BaseURL, APIKey: secrets.ProviderAPIKey, MaxOutputBytes: cfg.Worker.MaxOutputBytes, MaxToolCalls: 64, MaxToolArgumentBytes: minInt(cfg.Worker.MaxOutputBytes, 256<<10)}, &http.Client{Timeout: cfg.Worker.RequestTimeout})
	if err != nil {
		return nil, fmt.Errorf("configure DeepSeek client: %w", err)
	}
	privacyKey := append([]byte(nil), secrets.PrivacyHMACKey...)
	staticPolicy, err := safety.NewStaticPolicy(safety.StaticConfig{Mode: safety.StaticMode(cfg.Safety.Mode), MaxSummaryBytes: cfg.Safety.MaxSummaryBytes, MaxStructuredBytes: cfg.Safety.MaxStructuredBytes, RedactedSummary: cfg.Safety.RedactedSummary, ClarificationPrompt: cfg.Safety.ClarificationPrompt})
	if err != nil {
		clear(privacyKey)
		return nil, fmt.Errorf("configure static safety policy: %w", err)
	}
	drainer := lifecycle.NewDrainer()
	runnerConfig := worker.Runner{Store: repo, Reasoner: client, Safety: staticPolicy, Tools: registry, Clock: realClock, Jitter: jitter, Drainer: drainer, Tracer: traceRuntime.TracerProvider().Tracer("live.worker"),
		FeatureGates: gates,
		Retry:        worker.RetryPolicy{Base: cfg.Worker.RetryBase, Maximum: cfg.Worker.RetryMax, MaxRetries: uint32(cfg.Worker.MaxRetries)}, WorkerID: workerIdentity(),
		LeaseDuration: cfg.Worker.LeaseDuration, HeartbeatInterval: cfg.Worker.HeartbeatInterval, Concurrency: cfg.Worker.Concurrency,
		PollInterval: cfg.Worker.PollInterval, MaxOutputBytes: cfg.Worker.MaxOutputBytes, MaxOutputTokens: cfg.Worker.MaxOutputTokens, MaxToolRounds: cfg.Worker.MaxToolRounds, MaxToolCalls: 64,
		MaxToolArgumentBytes: minInt(cfg.Worker.MaxOutputBytes, 256<<10), MaxToolOutputBytes: minInt(cfg.Worker.MaxOutputBytes, 256<<10),
		ProviderSafetyID: func(tenant, user string) (string, error) { return safety.ProviderSafetyID(privacyKey, tenant, user) },
		Metrics: worker.MetricsHooks{Queue: func(d time.Duration) { metrics.WorkerQueue.WithLabelValues("ok").Observe(d.Seconds()) }, FirstToken: func(d time.Duration) { metrics.ReasonerFirstToken.WithLabelValues("ok").Observe(d.Seconds()) }, Duration: func(d time.Duration, result string) {
			metrics.WorkerDuration.WithLabelValues(result).Observe(d.Seconds())
		}, Retry: func(result string) { metrics.WorkerRetries.WithLabelValues(result).Inc() }},
	}
	if cfg.Features.VerticalProfiles.Enabled {
		engine := prompttmpl.NewMiniJinja()
		recipes, recipeErr := worker.NewRecipeResolver(profile.NewResolver(repo, engine), engine, registry)
		if recipeErr != nil {
			clear(privacyKey)
			return nil, errors.New("configure worker profile recipes")
		}
		runnerConfig.Recipes = recipes
	}
	if len(secrets.ToolEncryptionKey) == 32 {
		argumentSealer, err = worker.NewAESArgumentSealer(cfg.Features.ToolEncryptionKeyID, secrets.ToolEncryptionKey, rand.Reader)
		if err != nil {
			clear(privacyKey)
			return nil, err
		}
		runnerConfig.ArgumentSealer = argumentSealer
	}
	runner, err := worker.NewRunner(runnerConfig)
	if err != nil {
		clear(privacyKey)
		return nil, err
	}
	publisher, err := postgres.NewOutboxPublisher(pool, realClock, jitter, postgres.OutboxPublisherConfig{BatchSize: 64, PollInterval: maxDuration(cfg.Worker.PollInterval, 10*time.Millisecond), MaxJitter: 100 * time.Millisecond})
	if err != nil {
		clear(privacyKey)
		return nil, err
	}
	outboxLag := metrics.OutboxLag.WithLabelValues("outbox")
	publisher.SetLagObserver(func(d time.Duration) { outboxLag.Observe(d.Seconds()) })
	publisher.SetTracer(traceRuntime.TracerProvider().Tracer("live.store"))
	repo.SetExpiredLeaseObserver(func(count int) { metrics.ExpiredLeases.WithLabelValues("worker").Add(float64(count)) })
	var deletionStore retentioncontrol.Store
	if len(secrets.RetentionHMACKey) >= 32 {
		deletionStore = repo
	}
	sweeper, err := retentioncontrol.NewSweeper(retentioncontrol.Config{
		Store: deletionStore, Leases: repo, Approvals: repo, Clock: realClock, Drainer: drainer,
		Interval: cfg.Store.Retention.SweepInterval, BatchSize: cfg.Store.Retention.DeletionBatch,
		RetryBase: cfg.Worker.RetryBase, RetryMax: cfg.Worker.RetryMax, MaxRetries: cfg.Worker.MaxRetries,
		FailureThreshold: 3, OperationTimeout: cfg.Worker.RequestTimeout, Transient: postgres.IsTransientDurabilityError,
	})
	if err != nil {
		return nil, err
	}
	ready := health.NewRegistry()
	_ = ready.RegisterDrainer(drainer)
	_ = ready.RegisterRequired("database", health.CheckFunc(func(checkCtx context.Context) error { return pool.Ping(checkCtx) }))
	_ = ready.RegisterRequired("telemetry", health.CheckFunc(func(context.Context) error { return traceRuntime.Ready() }))
	_ = ready.RegisterRequired("retention", sweeper)
	_ = ready.RegisterRequired("runtime_features", health.CheckFunc(func(context.Context) error {
		if !gates.Healthy() {
			return errors.New("runtime feature refresh is unhealthy")
		}
		return nil
	}))
	var diagnostics *http.Server
	if cfg.Admin.ListenAddr != "" {
		if err := admin.ValidateListenAddress(ctx, cfg.Admin.ListenAddr, cfg.Mode == "production"); err != nil {
			return nil, err
		}
		authenticator, err := workerAdminAuthenticator(ctx, cfg, secrets)
		if err != nil {
			return nil, err
		}
		adminConfig := admin.Config{Authenticator: authenticator, Drainer: drainer, Capacity: workerCapacity{drainer: drainer, limit: int64(cfg.Worker.Concurrency)}, FeatureStore: featureStore, FeatureGates: gates}
		if len(secrets.RetentionHMACKey) >= 32 {
			adminConfig.Deletions = repo
		}
		adminHandler, err := admin.NewHandler(adminConfig)
		if err != nil {
			return nil, err
		}
		mux := http.NewServeMux()
		mux.Handle("/health/live", health.LiveHandler())
		mux.Handle("/health/ready", ready.ReadyHandler())
		mux.Handle("/metrics", promhttp.HandlerFor(metricRegistry, promhttp.HandlerOpts{}))
		mux.Handle("/admin/", adminHandler)
		diagnostics = admin.Server(cfg.Admin.ListenAddr, mux)
		diagnosticListener, err = net.Listen("tcp", cfg.Admin.ListenAddr)
		if err != nil {
			return nil, errors.New("listen worker diagnostics")
		}
	}
	var metricsServer *http.Server
	if cfg.Mode == "production" {
		metricsMux := http.NewServeMux()
		metricsMux.Handle("/metrics", promhttp.HandlerFor(metricRegistry, promhttp.HandlerOpts{}))
		metricsServer = &http.Server{Handler: metricsMux, ReadHeaderTimeout: 5 * time.Second}
		metricsListener, err = net.Listen("tcp", ":9091")
		if err != nil {
			return nil, errors.New("listen worker metrics")
		}
	}
	closeOnError = false
	runtime := &productionRuntime{pool: pool, runner: runner, publisher: publisher, repo: repo, clock: realClock,
		sweeper: sweeper, secretKey: privacyKey, sealer: argumentSealer, drainer: drainer, drainTimeout: cfg.Shutdown.WorkerDrainTimeout,
		cleanupTimeout: cfg.Shutdown.CleanupTimeout, telemetry: traceRuntime, diagnostics: diagnostics,
		diagnosticListener: diagnosticListener, metricsServer: metricsServer, metricsListener: metricsListener, gates: gates, featureStore: featureStore, featureInterval: cfg.Features.RefreshInterval}
	return runtime, nil
}

func workerRegistryForConfig(cfg config.Config, configHash string, resolver websearch.Resolver, knowledgeSearchers ...knowledgeadapter.Searcher) (worker.ToolRegistry, error) {
	manifests := make([]adaptercatalog.Manifest, 0, 2)
	bindings := make(map[string]adaptercatalog.Binding, 2)
	installed := make([]adaptercatalog.Installed, 0, 2)
	if cfg.Tools.WebSearch.Enabled {
		executor, err := websearch.New(resolver)
		if err != nil {
			return nil, fmt.Errorf("configure web search tool: %w", err)
		}
		native := websearch.RegisteredTool(executor)
		timeout := cfg.Tools.WebSearch.Timeout
		if timeout == 0 {
			timeout = 5 * time.Second
		}
		concurrency := cfg.Worker.Concurrency
		if concurrency <= 0 {
			concurrency = 1
		}
		concurrency = minInt(concurrency, 64)
		resultBytes := cfg.Worker.MaxOutputBytes
		if resultBytes <= 0 {
			resultBytes = 256 << 10
		}
		resultBytes = minInt(resultBytes, 256<<10)
		manifests = append(manifests, adaptercatalog.Manifest{
			AdapterID: "web_search", AdapterVersion: "native-v1", RegistryVersion: configHash,
			Tool: native.Definition.Clone(), ReadOnly: true, ParallelSafe: true,
			StableIdempotency: true, Approval: adaptercatalog.ApprovalNone,
			Timeout: timeout, MaxConcurrency: concurrency, MaxArgumentBytes: 64 << 10,
			MaxResultBytes: resultBytes, DestinationID: "public_web",
			TransportClass: adaptercatalog.TransportNative, HealthClass: adaptercatalog.HealthRequired,
		})
		bindings["web_search"] = adaptercatalog.Binding{Enabled: true}
		installed = append(installed, adaptercatalog.Installed{AdapterID: "web_search", AdapterVersion: "native-v1", DestinationID: "public_web", TransportClass: adaptercatalog.TransportNative, Executor: native.Executor, Projector: native.Projector, StartedPhase: native.StartedPhase, CompletedPhase: native.CompletedPhase})
	}
	if cfg.Features.VerticalProfiles.Enabled {
		if len(knowledgeSearchers) != 1 || knowledgeSearchers[0] == nil {
			return nil, errors.New("configure knowledge search dependency")
		}
		executor, err := knowledgeadapter.New(knowledgeSearchers[0])
		if err != nil {
			return nil, errors.New("configure knowledge search tool")
		}
		manifests = append(manifests, knowledgeadapter.Manifest(configHash))
		bindings[knowledgeadapter.AdapterID] = adaptercatalog.Binding{Enabled: true}
		installed = append(installed, knowledgeadapter.Installed(executor))
	}
	if len(manifests) == 0 {
		return worker.NewToolRegistry(map[string][]worker.RegisteredTool{configHash: {}})
	}
	catalog, err := adaptercatalog.New(configHash, manifests, nil)
	if err != nil {
		return nil, fmt.Errorf("configure worker adapter catalog: %w", err)
	}
	return adaptercatalog.CompileRegistry(catalog, bindings, installed)
}

type workerCapacity struct {
	drainer *lifecycle.Drainer
	limit   int64
}

func (c workerCapacity) Capacity() admin.Capacity {
	active := c.drainer.Active()
	available := c.limit - active
	if available < 0 {
		available = 0
	}
	return admin.Capacity{Active: active, Limit: c.limit, Available: available}
}
func workerAdminAuthenticator(ctx context.Context, cfg config.Config, secrets resolvedSecrets) (auth.Authenticator, error) {
	if !cfg.IsProduction() && cfg.UsesStaticAuth() {
		scopes := make(map[string]struct{}, len(cfg.Auth.StaticScopes))
		for _, scope := range cfg.Auth.StaticScopes {
			scopes[scope] = struct{}{}
		}
		return auth.NewStatic([]byte(secrets.AdminBearer), auth.Principal{TenantID: cfg.Auth.StaticTenantID, UserID: cfg.Auth.StaticUserID, Scopes: scopes}), nil
	}
	return auth.NewOIDC(ctx, auth.OIDCConfig{Issuer: cfg.Auth.OIDCIssuer, Audience: cfg.Auth.OIDCAudience, TenantClaim: cfg.Auth.OIDCTenantClaim, UserClaim: cfg.Auth.OIDCUserClaim, ScopeClaim: cfg.Auth.OIDCScopeClaim, RequiredScopes: []string{admin.AdminScope}, MaxKeySetStaleness: cfg.Auth.OIDCCacheOutage, HTTPTimeout: cfg.Auth.OIDCHTTPTimeout})
}

func (r *productionRuntime) Run(ctx context.Context) error {
	if r.drainer == nil {
		r.drainer = lifecycle.NewDrainer()
	}
	return r.runProduction(ctx)
}

func (r *productionRuntime) runProduction(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	diagnosticErrors := make(chan error, 2)
	if r.diagnostics != nil {
		go func() {
			err := r.diagnostics.Serve(r.diagnosticListener)
			if errors.Is(err, http.ErrServerClosed) {
				err = nil
			}
			diagnosticErrors <- err
		}()
	}
	if r.metricsServer != nil {
		go func() {
			err := r.metricsServer.Serve(r.metricsListener)
			if errors.Is(err, http.ErrServerClosed) {
				err = nil
			}
			diagnosticErrors <- err
		}()
	}
	components := r.components
	if len(components) == 0 {
		components = []func(context.Context) error{r.runner.Run, r.publisher.Run, r.sweeper.Run}
		if r.gates != nil && r.featureStore != nil {
			components = append(components, func(ctx context.Context) error {
				return r.gates.Run(ctx, r.featureStore, r.featureInterval, r.cleanupTimeout)
			})
		}
	}
	errorsOut := make(chan error, len(components))
	var group sync.WaitGroup
	for _, component := range components {
		group.Add(1)
		go func(run func(context.Context) error) {
			defer group.Done()
			if err := run(runCtx); err != nil && !errors.Is(err, context.Canceled) {
				select {
				case errorsOut <- err:
				default:
				}
			}
		}(component)
	}
	groupDone := make(chan struct{})
	go func() {
		group.Wait()
		r.drained.Store(true)
		if r.closeRequested.Load() {
			r.closeResources()
		}
		close(groupDone)
	}()
	var terminalErr error
	select {
	case terminalErr = <-errorsOut:
	case terminalErr = <-diagnosticErrors:
		if terminalErr == nil {
			terminalErr = errors.New("worker diagnostics stopped unexpectedly")
		}
	case <-groupDone:
		terminalErr = errors.New("worker components stopped unexpectedly")
	case <-ctx.Done():
	case <-r.drainer.Begun():
	}
	// Every exit source takes this same path: unready first, then bounded grace,
	// forced cancellation, bounded cleanup, and ordered resource shutdown.
	r.drainer.Begin()
	r.shutdownDiagnostics()
	timeout := r.drainTimeout
	if timeout <= 0 {
		timeout = 120 * time.Second
	}
	timer := r.clock.NewTicker(timeout)
	timedOut := false
	select {
	case <-r.drainer.Done():
	case <-timer.C():
		timedOut = true
	}
	timer.Stop()
	cancel()
	cleanup := r.cleanupTimeout
	if cleanup <= 0 {
		cleanup = minDuration(5*time.Second, timeout)
	}
	cleanupTimer := r.clock.NewTicker(cleanup)
	defer cleanupTimer.Stop()
	select {
	case <-groupDone:
	case <-cleanupTimer.C():
		timedOut = true
	}
	if timedOut {
		r.closeResources()
		return errors.Join(terminalErr, errors.New("worker drain timeout"))
	}
	select {
	case err := <-errorsOut:
		return errors.Join(terminalErr, err)
	default:
		return terminalErr
	}
}

func (r *productionRuntime) Close() {
	r.closeRequested.Store(true)
	if r.drained.Load() {
		r.closeResources()
	}
}

func (r *productionRuntime) closeResources() {
	r.closeOnce.Do(func() {
		r.shutdownDiagnostics()
		if r.telemetry != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = r.telemetry.Close(ctx)
			cancel()
		}
		clear(r.secretKey)
		r.secretKey = nil
		if r.sealer != nil {
			_ = r.sealer.Close()
		}
		if r.repo != nil {
			r.repo.CloseSensitive()
		}
		if r.pool != nil {
			r.pool.Close()
		}
	})
}

func (r *productionRuntime) shutdownDiagnostics() {
	if r == nil {
		return
	}
	if r.diagnostics != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = r.diagnostics.Shutdown(ctx)
		cancel()
	}
	if r.diagnosticListener != nil {
		_ = r.diagnosticListener.Close()
	}
	if r.metricsServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = r.metricsServer.Shutdown(ctx)
		cancel()
	}
	if r.metricsListener != nil {
		_ = r.metricsListener.Close()
	}
}

func workerIdentity() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "worker"
	}
	identity := fmt.Sprintf("%s-%d", host, os.Getpid())
	if len(identity) > 256 {
		identity = identity[:256]
	}
	return identity
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
