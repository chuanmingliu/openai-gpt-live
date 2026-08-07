package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/config"
	"github.com/chuanmingliu/openai-gpt-live/internal/reasoner/deepseek"
	"github.com/chuanmingliu/openai-gpt-live/internal/safety"
	"github.com/chuanmingliu/openai-gpt-live/internal/store/postgres"
	"github.com/chuanmingliu/openai-gpt-live/internal/telemetry"
	"github.com/chuanmingliu/openai-gpt-live/internal/worker"
	"github.com/chuanmingliu/openai-gpt-live/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
)

type resolvedSecrets struct {
	DatabaseURL       string
	ProviderAPIKey    string
	PrivacyHMACKey    []byte
	ToolEncryptionKey []byte
	raw               []string
}

func (s *resolvedSecrets) clear() {
	clear(s.PrivacyHMACKey)
	clear(s.ToolEncryptionKey)
	s.DatabaseURL, s.ProviderAPIKey = "", ""
	for index := range s.raw {
		s.raw[index] = ""
	}
}

type serviceRuntime interface {
	Run(context.Context) error
	Close()
}

type runtimeBuilder func(context.Context, config.Config, resolvedSecrets) (serviceRuntime, error)

func main() {
	slog.SetDefault(slog.New(telemetry.NewRedactingHandler(slog.Default().Handler(), nil)))
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := runWithBuilder(ctx, os.Args[1:], os.LookupEnv, buildRuntime); err != nil {
		log.Printf("live worker stopped: %v", err)
		os.Exit(1)
	}
}

func runWithBuilder(ctx context.Context, args []string, lookup func(string) (string, bool), builder runtimeBuilder) error {
	if ctx == nil {
		return errors.New("worker context is required")
	}
	flags := flag.NewFlagSet("live-worker", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	configPath := flags.String("config", "", "path to worker YAML configuration")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("positional arguments are not accepted")
	}
	if *configPath == "" {
		return errors.New("-config is required")
	}
	if lookup == nil {
		lookup = os.LookupEnv
	}
	cfg, err := config.Load(*configPath, lookup)
	if err != nil {
		return fmt.Errorf("load worker configuration: %w", err)
	}
	slog.SetDefault(slog.New(telemetry.NewRedactingHandler(slog.Default().Handler(), cfg.Safety.RedactionDeniedKeys)))
	secrets, err := resolveSecrets(cfg, lookup)
	if err != nil {
		return err
	}
	redactions := append([]string(nil), secrets.raw...)
	defer secrets.clear()
	runtime, err := builder(ctx, cfg, secrets)
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

func resolveSecrets(cfg config.Config, lookup func(string) (string, bool)) (resolvedSecrets, error) {
	var secrets resolvedSecrets
	require := func(name, label string) (string, error) {
		if name == "" {
			return "", fmt.Errorf("%s environment variable is not configured", label)
		}
		value, ok := lookup(name)
		if !ok || value == "" {
			return "", fmt.Errorf("configured %s %s is unavailable", label, name)
		}
		secrets.raw = append(secrets.raw, value)
		return value, nil
	}
	var err error
	secrets.DatabaseURL, err = require(cfg.Store.DatabaseURLEnv, "database URL")
	if err != nil {
		return resolvedSecrets{}, err
	}
	secrets.ProviderAPIKey, err = require(cfg.Worker.Reasoner.APIKeyEnv, "provider credential")
	if err != nil {
		return resolvedSecrets{}, err
	}
	privacy, err := require(cfg.Safety.PrivacyHMACKeyEnv, "privacy HMAC key")
	if err != nil {
		return resolvedSecrets{}, err
	}
	secrets.PrivacyHMACKey = append([]byte(nil), privacy...)
	if len(secrets.PrivacyHMACKey) < 32 {
		secrets.clear()
		return resolvedSecrets{}, errors.New("configured privacy HMAC key is too short")
	}
	if name := cfg.Features.ToolEncryptionKeyEnv; name != "" {
		encoded, err := require(name, "tool encryption key")
		if err != nil {
			secrets.clear()
			return resolvedSecrets{}, err
		}
		key, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil || len(key) != 32 {
			clear(key)
			secrets.clear()
			return resolvedSecrets{}, errors.New("configured tool encryption key is invalid")
		}
		secrets.ToolEncryptionKey = append([]byte(nil), key...)
		clear(key)
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
	pool           *pgxpool.Pool
	runner         *worker.Runner
	publisher      *postgres.OutboxPublisher
	repo           *postgres.Repository
	clock          clock.Clock
	sweepEvery     time.Duration
	secretKey      []byte
	sealer         worker.ToolArgumentSealer
	drainTimeout   time.Duration
	components     []func(context.Context) error
	closeOnce      sync.Once
	drained        atomic.Bool
	closeRequested atomic.Bool
}

func buildRuntime(ctx context.Context, cfg config.Config, secrets resolvedSecrets) (serviceRuntime, error) {
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
	defer func() {
		if closeOnError {
			if argumentSealer != nil {
				_ = argumentSealer.Close()
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
	registry, err := worker.NewToolRegistry(map[string][]worker.RegisteredTool{})
	if err != nil {
		return nil, err
	}
	repo, err := postgres.NewRepository(pool, realClock, postgres.WithToolGate(registry))
	if err != nil {
		return nil, err
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
	runnerConfig := worker.Runner{Store: repo, Reasoner: client, Safety: staticPolicy, Tools: registry, Clock: realClock, Jitter: jitter,
		Retry: worker.RetryPolicy{Base: cfg.Worker.RetryBase, Maximum: cfg.Worker.RetryMax, MaxRetries: uint32(cfg.Worker.MaxRetries)}, WorkerID: workerIdentity(), LeaseDuration: cfg.Worker.LeaseDuration, Concurrency: cfg.Worker.Concurrency,
		PollInterval: cfg.Worker.PollInterval, MaxOutputBytes: cfg.Worker.MaxOutputBytes, MaxOutputTokens: cfg.Worker.MaxOutputTokens, MaxToolRounds: cfg.Worker.MaxToolRounds, MaxToolCalls: 64,
		MaxToolArgumentBytes: minInt(cfg.Worker.MaxOutputBytes, 256<<10), MaxToolOutputBytes: minInt(cfg.Worker.MaxOutputBytes, 256<<10),
		ProviderSafetyID: func(tenant, user string) (string, error) { return safety.ProviderSafetyID(privacyKey, tenant, user) },
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
	sweepEvery := cfg.Store.Retention.SweepInterval
	if sweepEvery <= 0 {
		sweepEvery = time.Minute
	}
	closeOnError = false
	return &productionRuntime{pool: pool, runner: runner, publisher: publisher, repo: repo, clock: realClock, sweepEvery: sweepEvery, secretKey: privacyKey, sealer: argumentSealer, drainTimeout: cfg.Shutdown.DrainTimeout}, nil
}

func (r *productionRuntime) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	components := r.components
	if len(components) == 0 {
		components = []func(context.Context) error{r.runner.Run, r.publisher.Run, r.sweep}
	}
	errorsOut := make(chan error, len(components))
	var group sync.WaitGroup
	run := func(component func(context.Context) error) {
		group.Add(1)
		go func() {
			defer group.Done()
			if err := component(runCtx); err != nil && !errors.Is(err, context.Canceled) {
				select {
				case errorsOut <- err:
					cancel()
				default:
				}
			}
		}()
	}
	for _, component := range components {
		run(component)
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
	var firstErr error
	select {
	case <-ctx.Done():
		cancel()
	case firstErr = <-errorsOut:
		cancel()
	case <-groupDone:
	}
	drainTimeout := r.drainTimeout
	if drainTimeout <= 0 {
		drainTimeout = 10 * time.Second
	}
	drain := r.clock.NewTicker(drainTimeout)
	defer drain.Stop()
	select {
	case <-groupDone:
	case <-drain.C():
		return errors.New("worker drain timeout")
	}
	select {
	case err := <-errorsOut:
		return err
	default:
		return firstErr
	}
}

func (r *productionRuntime) sweep(ctx context.Context) error {
	ticker := r.clock.NewTicker(r.sweepEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case now := <-ticker.C():
			if _, err := r.repo.SweepExpired(ctx, now.UTC(), 256); err != nil {
				return err
			}
		}
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
		clear(r.secretKey)
		r.secretKey = nil
		if r.sealer != nil {
			_ = r.sealer.Close()
		}
		if r.pool != nil {
			r.pool.Close()
		}
	})
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
