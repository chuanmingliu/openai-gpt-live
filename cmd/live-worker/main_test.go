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
	"sync/atomic"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/config"
	"github.com/chuanmingliu/openai-gpt-live/internal/knowledge"
	"github.com/chuanmingliu/openai-gpt-live/internal/toolpolicy"
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
	started    chan resolvedSecrets
	configured chan config.Config
	drained    chan struct{}
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

func TestWorkerRegistryRegistersWebSearchOnlyWhenConfigured(t *testing.T) {
	for _, test := range []struct {
		name    string
		enabled bool
		want    bool
	}{
		{name: "enabled", enabled: true, want: true},
		{name: "disabled", enabled: false, want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := config.Config{}
			cfg.Tools.WebSearch.Enabled = test.enabled
			registry, err := workerRegistryForConfig(cfg, "config-v1", net.DefaultResolver)
			require.NoError(t, err)
			registered, ok := registry.Lookup("config-v1", toolpolicy.WebSearchTool)
			assert.Equal(t, test.want, ok)
			if test.want {
				assert.False(t, registered.SideEffecting)
				assert.True(t, registered.ReadOnly)
				assert.Equal(t, worker.ProgressPhaseSearching, registered.StartedPhase)
				assert.Equal(t, worker.ProgressPhaseReadingSources, registered.CompletedPhase)
			}
			_, wrongVersion := registry.Lookup("other-config", toolpolicy.WebSearchTool)
			assert.False(t, wrongVersion)
		})
	}
}

func TestWorkerRegistryRegistersKnowledgeOnlyWithProfileRuntimeAndDependency(t *testing.T) {
	cfg := config.Config{}
	cfg.Features.VerticalProfiles.Enabled = true
	_, err := workerRegistryForConfig(cfg, "config-v1", net.DefaultResolver)
	require.Error(t, err)
	registry, err := workerRegistryForConfig(cfg, "config-v1", net.DefaultResolver, knowledgeSearcherFunc(func(context.Context, knowledge.SearchRequest) ([]knowledge.Result, error) { return nil, nil }))
	require.NoError(t, err)
	registered, ok := registry.Lookup("config-v1", "knowledge_search")
	require.True(t, ok)
	assert.Equal(t, "knowledge.search", registered.AdapterID)
	assert.True(t, registered.ReadOnly)
}

type knowledgeSearcherFunc func(context.Context, knowledge.SearchRequest) ([]knowledge.Result, error)

func (function knowledgeSearcherFunc) Search(ctx context.Context, request knowledge.SearchRequest) ([]knowledge.Result, error) {
	return function(ctx, request)
}

func TestDevelopmentDurableWorkerUsesStaticAdminAuthenticator(t *testing.T) {
	cfg := config.Config{
		Mode: "development_durable",
		Auth: config.AuthConfig{
			StaticBearerEnv: "LIVE_STATIC_BEARER_TOKEN",
			StaticTenantID:  "development",
			StaticUserID:    "durable-demo",
			StaticScopes:    []string{"realtime", "live.admin"},
		},
	}
	secrets := resolvedSecrets{AdminBearer: "development-token"}
	authenticator, err := workerAdminAuthenticator(t.Context(), cfg, secrets)
	require.NoError(t, err)

	principal, err := authenticator.Authenticate(t.Context(), "development-token")
	require.NoError(t, err)
	assert.Equal(t, "development", principal.TenantID)
	assert.Contains(t, principal.Scopes, "live.admin")
}

func TestDevelopmentDurableWorkerResolvesStaticAdminBearer(t *testing.T) {
	cfg := config.Config{
		Mode:   "development_durable",
		Admin:  config.AdminConfig{ListenAddr: "127.0.0.1:9090"},
		Auth:   config.AuthConfig{StaticBearerEnv: "LIVE_STATIC_BEARER_TOKEN"},
		Store:  config.StoreConfig{DatabaseURLEnv: "DATABASE_URL"},
		Worker: config.WorkerConfig{Reasoner: config.ReasonerConfig{APIKeyEnv: "DEEPSEEK_API_KEY"}},
		Safety: config.SafetyConfig{PrivacyHMACKeyEnv: "LIVE_PRIVACY_HMAC_KEY"},
	}
	values := map[string]string{
		"DATABASE_URL":             "postgres://live@postgres/live?sslmode=disable",
		"DEEPSEEK_API_KEY":         "reasoner-secret",
		"LIVE_PRIVACY_HMAC_KEY":    strings.Repeat("p", 32),
		"LIVE_STATIC_BEARER_TOKEN": "development-token",
	}

	secrets, err := resolveSecrets(cfg, lookupFrom(values))
	require.NoError(t, err)
	defer secrets.clear()
	assert.Equal(t, "development-token", secrets.AdminBearer)
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

func runDurableWorker(ctx context.Context, args []string, lookup func(string) (string, bool), builder runtimeBuilder) error {
	return runWithBuilders(ctx, args, lookup, builder, func(context.Context) (serviceRuntime, error) {
		return nil, errors.New("idle builder must not run")
	})
}

type cancellableRuntime struct {
	started chan struct{}
	drained chan struct{}
}

func (r *cancellableRuntime) Run(ctx context.Context) error {
	close(r.started)
	<-ctx.Done()
	close(r.drained)
	return ctx.Err()
}

func (*cancellableRuntime) Close() {}

func TestRunBackendNoneDoesNotReadConfigResolveSecretsOrBuildDurableRuntime(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	runtime := &cancellableRuntime{started: make(chan struct{}), drained: make(chan struct{})}
	var lookups, durableBuilds, idleBuilds atomic.Int32
	done := make(chan error, 1)
	go func() {
		done <- runWithBuilders(ctx, []string{"--backend", "none"}, func(string) (string, bool) {
			lookups.Add(1)
			return "", false
		}, func(context.Context, config.Config, resolvedSecrets) (serviceRuntime, error) {
			durableBuilds.Add(1)
			return nil, errors.New("durable builder must not run")
		}, func(context.Context) (serviceRuntime, error) {
			idleBuilds.Add(1)
			return runtime, nil
		})
	}()

	select {
	case <-runtime.started:
	case err := <-done:
		t.Fatalf("idle worker stopped before startup: %v", err)
	case <-time.After(time.Second):
		t.Fatal("idle worker did not start")
	}
	assert.Zero(t, lookups.Load())
	assert.Zero(t, durableBuilds.Load())
	assert.Equal(t, int32(1), idleBuilds.Load())
	cancel()
	require.NoError(t, <-done)
	select {
	case <-runtime.drained:
	case <-time.After(time.Second):
		t.Fatal("idle runtime did not join after cancellation")
	}
}

func TestRunBackendNoneRejectsEveryExplicitConfigFlag(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
	}{
		{name: "path", args: []string{"--backend", "none", "-config", "worker.yaml"}},
		{name: "equals empty", args: []string{"--backend", "none", "-config="}},
		{name: "long empty", args: []string{"--backend", "none", "--config", ""}},
	} {
		t.Run(test.name, func(t *testing.T) {
			assertRunRejectsBeforeLookupOrBuild(t, test.args)
		})
	}
}

func TestRunRejectsInvalidBackendFlagCombinationsBeforeConstruction(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
	}{
		{name: "durable without config", args: []string{"--backend", "durable"}},
		{name: "unknown backend", args: []string{"--backend", "unknown"}},
		{name: "duplicate backend", args: []string{"--backend", "none", "-backend", "durable"}},
		{name: "duplicate config", args: []string{"--backend", "durable", "-config", "one.yaml", "--config", "two.yaml"}},
		{name: "none positional", args: []string{"--backend", "none", "extra"}},
		{name: "durable positional", args: []string{"--backend", "durable", "-config", "worker.yaml", "extra"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			assertRunRejectsBeforeLookupOrBuild(t, test.args)
		})
	}
}

func assertRunRejectsBeforeLookupOrBuild(t *testing.T, args []string) {
	t.Helper()
	var lookups, durableBuilds, idleBuilds atomic.Int32
	err := runWithBuilders(t.Context(), args, func(string) (string, bool) {
		lookups.Add(1)
		return "", false
	}, func(context.Context, config.Config, resolvedSecrets) (serviceRuntime, error) {
		durableBuilds.Add(1)
		return nil, nil
	}, func(context.Context) (serviceRuntime, error) {
		idleBuilds.Add(1)
		return nil, nil
	})
	require.Error(t, err)
	assert.Zero(t, lookups.Load())
	assert.Zero(t, durableBuilds.Load())
	assert.Zero(t, idleBuilds.Load())
}

func TestRunSelectsDurableBackendWithOrWithoutExplicitFlag(t *testing.T) {
	path := writeWorkerConfig(t)
	for _, test := range []struct {
		name string
		args []string
	}{
		{name: "legacy config", args: []string{"-config", path}},
		{name: "explicit durable", args: []string{"--backend", "durable", "-config", path}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			runtime := &cancellableRuntime{started: make(chan struct{}), drained: make(chan struct{})}
			var durableBuilds, idleBuilds atomic.Int32
			done := make(chan error, 1)
			go func() {
				done <- runWithBuilders(ctx, test.args, lookupFrom(dummySecrets()), func(context.Context, config.Config, resolvedSecrets) (serviceRuntime, error) {
					durableBuilds.Add(1)
					return runtime, nil
				}, func(context.Context) (serviceRuntime, error) {
					idleBuilds.Add(1)
					return nil, errors.New("idle builder must not run")
				})
			}()
			select {
			case <-runtime.started:
			case err := <-done:
				t.Fatalf("durable worker stopped before startup: %v", err)
			case <-time.After(time.Second):
				t.Fatal("durable worker did not start")
			}
			assert.Equal(t, int32(1), durableBuilds.Load())
			assert.Zero(t, idleBuilds.Load())
			cancel()
			require.NoError(t, <-done)
			select {
			case <-runtime.drained:
			case <-time.After(time.Second):
				t.Fatal("durable runtime did not join after cancellation")
			}
		})
	}
}

func durableWorkerSecretConfig() config.Config {
	return config.Config{
		Mode:     "local",
		Store:    config.StoreConfig{DatabaseURLEnv: "TEST_DATABASE_URL", Retention: config.RetentionConfig{HMACKeyEnv: "TEST_RETENTION_KEY"}},
		Worker:   config.WorkerConfig{Reasoner: config.ReasonerConfig{APIKeyEnv: "TEST_DEEPSEEK_KEY"}},
		Safety:   config.SafetyConfig{PrivacyHMACKeyEnv: "TEST_PRIVACY_KEY"},
		Features: config.FeatureConfig{ToolEncryptionKeyEnv: "TEST_TOOL_KEY"},
		Admin:    config.AdminConfig{ListenAddr: "127.0.0.1:9090"},
		Auth:     config.AuthConfig{StaticBearerEnv: "TEST_ADMIN_BEARER"},
	}
}

func durableWorkerSecrets() map[string]string {
	values := dummySecrets()
	values["TEST_ADMIN_BEARER"] = "admin-bearer"
	return values
}

func TestResolveDurableWorkerSecretsAreBounded(t *testing.T) {
	invalidUTF8 := string([]byte{0xff})
	for _, test := range []struct {
		name   string
		secret string
		value  string
		remove bool
	}{
		{name: "database missing", secret: "TEST_DATABASE_URL", remove: true},
		{name: "database empty", secret: "TEST_DATABASE_URL", value: ""},
		{name: "database whitespace", secret: "TEST_DATABASE_URL", value: " sentinel "},
		{name: "database control", secret: "TEST_DATABASE_URL", value: "database\nurl"},
		{name: "database invalid utf8", secret: "TEST_DATABASE_URL", value: invalidUTF8},
		{name: "database oversized", secret: "TEST_DATABASE_URL", value: strings.Repeat("d", workerSecretMaximumBytes+1)},
		{name: "reasoner missing", secret: "TEST_DEEPSEEK_KEY", remove: true},
		{name: "reasoner empty", secret: "TEST_DEEPSEEK_KEY", value: ""},
		{name: "reasoner whitespace", secret: "TEST_DEEPSEEK_KEY", value: " sentinel "},
		{name: "reasoner control", secret: "TEST_DEEPSEEK_KEY", value: "key\nvalue"},
		{name: "reasoner invalid utf8", secret: "TEST_DEEPSEEK_KEY", value: invalidUTF8},
		{name: "reasoner oversized", secret: "TEST_DEEPSEEK_KEY", value: strings.Repeat("r", workerSecretMaximumBytes+1)},
		{name: "privacy missing", secret: "TEST_PRIVACY_KEY", remove: true},
		{name: "privacy empty", secret: "TEST_PRIVACY_KEY", value: ""},
		{name: "privacy whitespace", secret: "TEST_PRIVACY_KEY", value: " sentinel "},
		{name: "privacy control", secret: "TEST_PRIVACY_KEY", value: "privacy\nkey"},
		{name: "privacy invalid utf8", secret: "TEST_PRIVACY_KEY", value: invalidUTF8},
		{name: "privacy oversized", secret: "TEST_PRIVACY_KEY", value: strings.Repeat("p", workerSecretMaximumBytes+1)},
		{name: "privacy too short", secret: "TEST_PRIVACY_KEY", value: strings.Repeat("p", 31)},
		{name: "retention missing", secret: "TEST_RETENTION_KEY", remove: true},
		{name: "retention empty", secret: "TEST_RETENTION_KEY", value: ""},
		{name: "retention whitespace", secret: "TEST_RETENTION_KEY", value: " sentinel "},
		{name: "retention control", secret: "TEST_RETENTION_KEY", value: "retention\nkey"},
		{name: "retention invalid utf8", secret: "TEST_RETENTION_KEY", value: invalidUTF8},
		{name: "retention oversized", secret: "TEST_RETENTION_KEY", value: strings.Repeat("a", workerHMACEncodedMaximumSize+1)},
		{name: "retention decoded too short", secret: "TEST_RETENTION_KEY", value: base64.StdEncoding.EncodeToString(bytes.Repeat([]byte("r"), 31))},
		{name: "retention decoded too long", secret: "TEST_RETENTION_KEY", value: base64.StdEncoding.EncodeToString(bytes.Repeat([]byte("r"), 4097))},
		{name: "tool missing", secret: "TEST_TOOL_KEY", remove: true},
		{name: "tool empty", secret: "TEST_TOOL_KEY", value: ""},
		{name: "tool whitespace", secret: "TEST_TOOL_KEY", value: " sentinel "},
		{name: "tool control", secret: "TEST_TOOL_KEY", value: "tool\nkey"},
		{name: "tool invalid utf8", secret: "TEST_TOOL_KEY", value: invalidUTF8},
		{name: "tool oversized", secret: "TEST_TOOL_KEY", value: strings.Repeat("a", workerHMACEncodedMaximumSize+1)},
		{name: "tool decoded wrong size", secret: "TEST_TOOL_KEY", value: base64.StdEncoding.EncodeToString(bytes.Repeat([]byte("t"), 31))},
		{name: "admin missing", secret: "TEST_ADMIN_BEARER", remove: true},
		{name: "admin empty", secret: "TEST_ADMIN_BEARER", value: ""},
		{name: "admin whitespace", secret: "TEST_ADMIN_BEARER", value: " sentinel "},
		{name: "admin control", secret: "TEST_ADMIN_BEARER", value: "admin\ntoken"},
		{name: "admin invalid utf8", secret: "TEST_ADMIN_BEARER", value: invalidUTF8},
		{name: "admin oversized", secret: "TEST_ADMIN_BEARER", value: strings.Repeat("a", workerSecretMaximumBytes+1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			values := durableWorkerSecrets()
			if test.remove {
				delete(values, test.secret)
			} else {
				values[test.secret] = test.value
			}
			secrets, err := resolveSecrets(durableWorkerSecretConfig(), lookupFrom(values))
			secrets.clear()
			require.Error(t, err)
			if test.value != "" {
				assert.NotContains(t, err.Error(), test.value)
			}
		})
	}
}

func TestRunRequiresConfig(t *testing.T) {
	err := runWithBuilders(t.Context(), nil, lookupFrom(dummySecrets()), func(context.Context, config.Config, resolvedSecrets) (serviceRuntime, error) {
		return nil, errors.New("durable builder must not run")
	}, func(context.Context) (serviceRuntime, error) {
		return nil, errors.New("idle builder must not run")
	})
	assert.ErrorContains(t, err, "-config is required")
}

func TestRunRejectsEnabledWebSearchWhenDefaultEncryptionSecretIsMissing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker.yaml")
	data := `mode: local
worker:
  concurrency: 1
  lease_duration: 9s
  heartbeat_interval: 2s
  reasoner:
    base_url: https://provider.invalid
    model: reasoner-v1
    api_key_env: TEST_DEEPSEEK_KEY
store:
  database_url_env: TEST_DATABASE_URL
safety:
  privacy_hmac_key_env: TEST_PRIVACY_KEY
features:
  tool_encryption_key_id: search-v1
tools:
  web_search: {enabled: true}
`
	require.NoError(t, os.WriteFile(path, []byte(data), 0o600))
	secrets := dummySecrets()
	delete(secrets, "TEST_TOOL_KEY")
	called := false
	err := runDurableWorker(t.Context(), []string{"-config", path}, lookupFrom(secrets), func(context.Context, config.Config, resolvedSecrets) (serviceRuntime, error) {
		called = true
		return nil, errors.New("builder must not be called")
	})
	require.ErrorContains(t, err, "LIVE_TOOL_ENCRYPTION_KEY")
	assert.False(t, called)
}

func TestRunResolvesSecretsAndDrainsOnCancellation(t *testing.T) {
	path := writeWorkerConfig(t)
	ctx, cancel := context.WithCancel(t.Context())
	runtime := &fakeRuntime{started: make(chan resolvedSecrets, 1), configured: make(chan config.Config, 1), drained: make(chan struct{})}
	builder := func(_ context.Context, cfg config.Config, secrets resolvedSecrets) (serviceRuntime, error) {
		runtime.configured <- cfg
		runtime.started <- secrets
		return runtime, nil
	}
	done := make(chan error, 1)
	go func() { done <- runDurableWorker(ctx, []string{"-config", path}, lookupFrom(dummySecrets()), builder) }()
	var resolved resolvedSecrets
	select {
	case resolved = <-runtime.started:
	case err := <-done:
		t.Fatalf("worker stopped before startup: %v", err)
	}
	assert.Equal(t, 2, (<-runtime.configured).Worker.Concurrency)
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
	cleared := make(chan []byte, 1)
	builder := func(_ context.Context, _ config.Config, secrets resolvedSecrets) (serviceRuntime, error) {
		ownedView := secrets.RetentionHMACKey
		return &functionRuntime{run: func(runCtx context.Context) error {
			cleared <- append([]byte(nil), ownedView...)
			close(started)
			<-runCtx.Done()
			return runCtx.Err()
		}}, nil
	}
	done := make(chan error, 1)
	go func() { done <- runDurableWorker(ctx, []string{"-config", path}, lookupFrom(dummySecrets()), builder) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("runtime did not start")
	}
	for _, value := range <-cleared {
		assert.Zero(t, value, "resolver-owned retention key remains live when Run begins")
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
	err := runDurableWorker(t.Context(), []string{"-config", path}, lookupFrom(values), builder)
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
	err := runDurableWorker(t.Context(), []string{"-config", path}, lookupFrom(values), func(context.Context, config.Config, resolvedSecrets) (serviceRuntime, error) {
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
	if err := runDurableWorker(t.Context(), []string{"check-config", "--config", path}, lookup, builder); err != nil {
		t.Fatal(err)
	}
	if err := runDurableWorker(t.Context(), []string{"config-hash", "--config", path}, lookup, builder); err != nil {
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
	err := runDurableWorker(t.Context(), []string{"--config", path}, lookup, func(context.Context, config.Config, resolvedSecrets) (serviceRuntime, error) {
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
