package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"net"
	"net/http"
	"os"
	"os/exec"
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

const workerFatalLogHelperEnv = "LIVE_TEST_WORKER_FATAL_LOG_HELPER"

func TestWorkerFatalErrorIsStructuredRedactedAndExitsOne(t *testing.T) {
	const sentinel = "W0RKS3C"
	path := filepath.Join(t.TempDir(), "worker.yaml")
	require.NoError(t, os.WriteFile(path, []byte("worker:\n  concurrency: "+sentinel+"\n"), 0o600))
	_, fixtureErr := config.LoadUnresolved(path)
	require.ErrorContains(t, fixtureErr, sentinel, "fixture must prove the underlying error contains the sensitive sentinel")

	if os.Getenv(workerFatalLogHelperEnv) == "1" {
		os.Args = []string{"live-worker", "-config", path}
		main()
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestWorkerFatalErrorIsStructuredRedactedAndExitsOne$")
	command.Env = append(os.Environ(), workerFatalLogHelperEnv+"=1")
	output, err := command.CombinedOutput()
	require.Error(t, err, "fatal helper unexpectedly exited successfully: %s", output)
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	assert.Equal(t, 1, exitErr.ExitCode(), "fatal helper output: %s", output)
	assert.NotContains(t, string(output), sentinel)
	assert.Contains(t, string(output), "level=ERROR")
	assert.Contains(t, string(output), `msg="live worker stopped"`)
	assert.Contains(t, string(output), "error=[REDACTED]")
}

type fakeRuntime struct {
	started chan resolvedSecrets
	drained chan struct{}
}

type functionRuntime struct{ run func(context.Context) error }

func (r *functionRuntime) Run(ctx context.Context) error { return r.run(ctx) }
func (*functionRuntime) Close()                          {}

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

func TestProductionRuntimeDrainTimeoutClosesResourcesEvenWhenComponentIsStuck(t *testing.T) {
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
	assert.ErrorContains(t, err, "closed", "the cleanup bound must force resource closure")
	close(release)
	for !runtimeUnderTest.drained.Load() {
		runtime.Gosched()
	}
	runtimeUnderTest.Close()
	_, err = sealer.Seal([]byte(`{}`))
	assert.ErrorContains(t, err, "closed")
}

func TestProductionRuntimeComponentFailureDrainsAndBoundsIgnoringPeer(t *testing.T) {
	manual := clock.NewManual(time.Now().UTC())
	fault := errors.New("component fault")
	fail := make(chan struct{})
	release := make(chan struct{})
	peerStarted := make(chan struct{})
	runtimeUnderTest := &productionRuntime{
		clock: manual, drainTimeout: time.Second, cleanupTimeout: time.Second,
		components: []func(context.Context) error{
			func(context.Context) error { <-fail; return fault },
			func(context.Context) error { close(peerStarted); <-release; return nil },
		},
	}
	done := make(chan error, 1)
	go func() { done <- runtimeUnderTest.Run(t.Context()) }()
	<-peerStarted
	close(fail)
	for runtimeUnderTest.drainer == nil || runtimeUnderTest.drainer.Accepting() {
		runtime.Gosched()
	}
	var runErr error
	for runErr == nil {
		manual.Advance(time.Second)
		runtime.Gosched()
		select {
		case runErr = <-done:
		default:
		}
	}
	assert.ErrorIs(t, runErr, fault)
	assert.ErrorContains(t, runErr, "worker drain timeout")
	close(release)
}

type diagnosticFailureListener struct{ err error }

func (l diagnosticFailureListener) Accept() (net.Conn, error) { return nil, l.err }
func (diagnosticFailureListener) Close() error                { return nil }
func (diagnosticFailureListener) Addr() net.Addr              { return diagnosticFailureAddr("diagnostics") }

type diagnosticFailureAddr string

func (a diagnosticFailureAddr) Network() string { return "test" }
func (a diagnosticFailureAddr) String() string  { return string(a) }

func TestProductionRuntimeDiagnosticsFailureUsesSameBoundedDrain(t *testing.T) {
	manual := clock.NewManual(time.Now().UTC())
	fault := errors.New("diagnostic listener fault")
	release := make(chan struct{})
	peerStarted := make(chan struct{})
	runtimeUnderTest := &productionRuntime{
		clock: manual, drainTimeout: time.Second, cleanupTimeout: time.Second,
		diagnostics:        &http.Server{Handler: http.NewServeMux()},
		diagnosticListener: diagnosticFailureListener{err: fault},
		components: []func(context.Context) error{
			func(context.Context) error { close(peerStarted); <-release; return nil },
		},
	}
	done := make(chan error, 1)
	go func() { done <- runtimeUnderTest.Run(t.Context()) }()
	<-peerStarted
	for runtimeUnderTest.drainer == nil || runtimeUnderTest.drainer.Accepting() {
		runtime.Gosched()
	}
	var runErr error
	for runErr == nil {
		manual.Advance(time.Second)
		runtime.Gosched()
		select {
		case runErr = <-done:
		default:
		}
	}
	assert.ErrorContains(t, runErr, fault.Error())
	assert.ErrorContains(t, runErr, "worker drain timeout")
	close(release)
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
  retention:
    hmac_key_env: TEST_RETENTION_KEY
    hmac_key_id: retention-key-v1
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
		"TEST_DEEPSEEK_KEY":  "sentinel-provider-secret",
		"TEST_DATABASE_URL":  "postgres://worker:sentinel-db-secret@db.invalid/live?sslmode=require",
		"TEST_PRIVACY_KEY":   strings.Repeat("p", 32),
		"TEST_TOOL_KEY":      "BQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQU=",
		"TEST_RETENTION_KEY": base64.StdEncoding.EncodeToString(bytes.Repeat([]byte("r"), 32)),
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

func TestRunClearsResolvedRetentionSourceBeforeRuntimeRun(t *testing.T) {
	path := writeWorkerConfig(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started := make(chan struct{})
	builder := func(_ context.Context, _ config.Config, secrets resolvedSecrets) (serviceRuntime, error) {
		require.NotEmpty(t, secrets.RetentionHMACKey)
		ownedView := secrets.RetentionHMACKey
		return &functionRuntime{run: func(runCtx context.Context) error {
			for _, value := range ownedView {
				assert.Zero(t, value, "resolver-owned retention key remains live when Run begins")
			}
			close(started)
			<-runCtx.Done()
			return runCtx.Err()
		}}, nil
	}
	done := make(chan error, 1)
	go func() { done <- runWithBuilder(ctx, []string{"-config", path}, lookupFrom(dummySecrets()), builder) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("runtime did not start")
	}
	cancel()
	require.NoError(t, <-done)
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

func TestConfigCommandsDoNotLookupSecretsOrBuildRuntime(t *testing.T) {
	path := writeWorkerConfig(t)
	old := commandOutput
	defer func() { commandOutput = old }()
	var output bytes.Buffer
	commandOutput = &output
	lookup := func(string) (string, bool) { t.Fatal("secret lookup"); return "", false }
	builder := func(context.Context, config.Config, resolvedSecrets) (serviceRuntime, error) {
		t.Fatal("builder called")
		return nil, nil
	}
	if err := runWithBuilder(t.Context(), []string{"check-config", "--config", path}, lookup, builder); err != nil {
		t.Fatal(err)
	}
	if err := runWithBuilder(t.Context(), []string{"config-hash", "--config", path}, lookup, builder); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(output.String(), "v1:") {
		t.Fatalf("output %q", output.String())
	}
}

func TestExpectedHashMismatchPrecedesSecretResolutionAndBuilder(t *testing.T) {
	path := writeWorkerConfig(t)
	lookups := 0
	lookup := func(name string) (string, bool) {
		lookups++
		if name != "LIVE_EXPECTED_CONFIG_HASH" {
			t.Fatalf("unexpected lookup %s", name)
		}
		return "v1:" + strings.Repeat("0", 64), true
	}
	err := runWithBuilder(t.Context(), []string{"--config", path}, lookup, func(context.Context, config.Config, resolvedSecrets) (serviceRuntime, error) {
		t.Fatal("builder called")
		return nil, nil
	})
	if err == nil || !strings.Contains(err.Error(), "config hash mismatch") {
		t.Fatalf("got %v", err)
	}
	if lookups != 1 {
		t.Fatalf("lookups=%d", lookups)
	}
}
