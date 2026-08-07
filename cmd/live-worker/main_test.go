package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/config"
	"github.com/chuanmingliu/openai-gpt-live/internal/worker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeRuntime struct {
	started chan resolvedSecrets
	drained chan struct{}
}

func TestProductionRuntimeGracefullyDrainsComponents(t *testing.T) {
	manual := clock.NewManual(time.Now().UTC())
	started := make(chan struct{})
	runtimeUnderTest := &productionRuntime{clock: manual, drainTimeout: time.Second, components: []func(context.Context) error{
		func(ctx context.Context) error { close(started); <-ctx.Done(); return ctx.Err() },
	}}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runtimeUnderTest.Run(ctx) }()
	<-started
	cancel()
	require.NoError(t, <-done)
	runtimeUnderTest.Close()
}

func TestProductionRuntimeDrainTimeoutDefersResourceCloseUntilStuckComponentReturns(t *testing.T) {
	manual := clock.NewManual(time.Now().UTC())
	started, release := make(chan struct{}), make(chan struct{})
	sealer, err := worker.NewAESArgumentSealer("rotation-v2", make([]byte, 32), strings.NewReader(strings.Repeat("n", 24)))
	require.NoError(t, err)
	runtimeUnderTest := &productionRuntime{clock: manual, drainTimeout: time.Second, sealer: sealer, secretKey: []byte("private"), components: []func(context.Context) error{
		func(context.Context) error { close(started); <-release; return nil },
	}}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runtimeUnderTest.Run(ctx) }()
	<-started
	cancel()
	for {
		manual.Advance(time.Second)
		runtime.Gosched()
		select {
		case runErr := <-done:
			assert.EqualError(t, runErr, "worker drain timeout")
			goto timedOut
		default:
		}
	}
timedOut:
	runtimeUnderTest.Close()
	_, err = sealer.Seal([]byte(`{}`))
	require.NoError(t, err, "resources must remain open while a component is still running")
	close(release)
	for !runtimeUnderTest.drained.Load() {
		runtime.Gosched()
	}
	runtimeUnderTest.Close()
	_, err = sealer.Seal([]byte(`{}`))
	assert.ErrorContains(t, err, "closed")
}

func (r *fakeRuntime) Run(ctx context.Context) error {
	<-ctx.Done()
	close(r.drained)
	return ctx.Err()
}

func (r *fakeRuntime) Close() {}

func writeWorkerConfig(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "worker.yaml")
	data := `mode: local
worker:
  concurrency: 2
  lease_duration: 9s
  heartbeat_interval: 2s
  poll_interval: 10ms
  request_timeout: 2s
  retry_base: 10ms
  retry_max: 1s
  max_retries: 3
  max_output_bytes: 65536
  max_tool_rounds: 3
  reasoner:
    provider: deepseek
    base_url: https://provider.invalid/v1
    model: reasoner-v1
    api_key_env: TEST_DEEPSEEK_KEY
store:
  database_url_env: TEST_DATABASE_URL
safety:
  privacy_hmac_key_env: TEST_PRIVACY_KEY
features:
  tool_encryption_key_env: TEST_TOOL_KEY
  tool_encryption_key_id: test-v1
`
	require.NoError(t, os.WriteFile(path, []byte(data), 0o600))
	return path
}

func dummySecrets() map[string]string {
	return map[string]string{
		"TEST_DEEPSEEK_KEY": "sentinel-provider-secret",
		"TEST_DATABASE_URL": "postgres://worker:sentinel-db-secret@db.invalid/live?sslmode=require",
		"TEST_PRIVACY_KEY":  strings.Repeat("p", 32),
		"TEST_TOOL_KEY":     "BQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQU=",
	}
}

func lookupFrom(values map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) { value, ok := values[name]; return value, ok }
}

func TestRunRequiresConfig(t *testing.T) {
	err := runWithBuilder(t.Context(), nil, lookupFrom(dummySecrets()), func(context.Context, config.Config, resolvedSecrets) (serviceRuntime, error) {
		t.Fatal("builder called")
		return nil, nil
	})
	assert.ErrorContains(t, err, "-config is required")
}

func TestRunResolvesSecretsAndDrainsOnCancellation(t *testing.T) {
	path := writeWorkerConfig(t)
	ctx, cancel := context.WithCancel(t.Context())
	runtime := &fakeRuntime{started: make(chan resolvedSecrets, 1), drained: make(chan struct{})}
	builder := func(_ context.Context, cfg config.Config, secrets resolvedSecrets) (serviceRuntime, error) {
		assert.Equal(t, 2, cfg.Worker.Concurrency)
		runtime.started <- secrets
		return runtime, nil
	}
	done := make(chan error, 1)
	go func() { done <- runWithBuilder(ctx, []string{"-config", path}, lookupFrom(dummySecrets()), builder) }()
	var resolved resolvedSecrets
	select {
	case resolved = <-runtime.started:
	case err := <-done:
		t.Fatalf("worker stopped before startup: %v", err)
	}
	assert.Equal(t, "sentinel-provider-secret", resolved.ProviderAPIKey)
	assert.Contains(t, resolved.DatabaseURL, "sentinel-db-secret")
	assert.Len(t, resolved.PrivacyHMACKey, 32)
	assert.Len(t, resolved.ToolEncryptionKey, 32)
	cancel()
	require.NoError(t, <-done)
	<-runtime.drained
}

func TestRunRedactsEveryResolvedSecretFromStartupErrors(t *testing.T) {
	path := writeWorkerConfig(t)
	values := dummySecrets()
	builder := func(context.Context, config.Config, resolvedSecrets) (serviceRuntime, error) {
		return nil, errors.New(strings.Join([]string{values["TEST_DEEPSEEK_KEY"], values["TEST_DATABASE_URL"], values["TEST_PRIVACY_KEY"], values["TEST_TOOL_KEY"]}, " "))
	}
	err := runWithBuilder(t.Context(), []string{"-config", path}, lookupFrom(values), builder)
	require.Error(t, err)
	for _, secret := range values {
		assert.NotContains(t, err.Error(), secret)
	}
	assert.Contains(t, err.Error(), "[redacted]")
}

func TestRunRejectsMissingConfiguredSecretBeforeStartup(t *testing.T) {
	path := writeWorkerConfig(t)
	values := dummySecrets()
	delete(values, "TEST_PRIVACY_KEY")
	err := runWithBuilder(t.Context(), []string{"-config", path}, lookupFrom(values), func(context.Context, config.Config, resolvedSecrets) (serviceRuntime, error) {
		t.Fatal("builder called")
		return nil, nil
	})
	assert.ErrorContains(t, err, "TEST_PRIVACY_KEY")
}
