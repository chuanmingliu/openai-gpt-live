# Credential-free Backend Startup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Start the loopback gateway and an explicitly idle worker without configuration, credentials, PostgreSQL, or an available inference provider while preserving strict durable and production behavior.

**Architecture:** Add one typed built-in gateway configuration and select it only when `live-gateway` receives no `-config`. Remove the gateway's broad shared-config secret sweep so active gateway modules resolve only their own dependencies. Add a second `serviceRuntime` implementation for `live-worker --backend none`; it owns only loopback health diagnostics and reports live but not ready, while the existing durable runtime remains the default whenever `-config` is supplied.

**Tech Stack:** Go 1.26, standard `flag`, `net/http`, and `net` packages, existing `internal/config`, `internal/health`, gateway/worker composition roots, and `testify`.

**Spec:** `docs/superpowers/specs/2026-08-23-credential-free-backend-startup-design.md`

## Global Constraints

- Preserve all pre-existing modified and untracked files; stage only files named by the current task.
- Default credential-free listeners must be literal loopback: gateway `127.0.0.1:8080`, idle worker diagnostics `127.0.0.1:9091`.
- The built-in gateway names no credential environment variable and enables no durable handoff or side-effecting tool.
- A supplied credential reference remains strict; never turn missing configured authentication into anonymous access.
- `live-worker -config <path>` remains a backward-compatible durable invocation.
- `--backend none` performs no config read, secret lookup, provider construction, PostgreSQL connection, migration check, telemetry construction, or admin-handler construction.
- Idle-worker liveness is HTTP 200; readiness is HTTP 503 with an empty body.
- Do not add frontend files, static assets, browser routes, database migrations, dependencies, deployment-manifest edits, or a `.env` file.
- Do not log or return credential values, database URLs, upstream response bodies, audio, transcripts, prompts, or raw protocol frames.
- Focused command/config/provider tests were green before implementation. Several `internal/gateway` tests already fail in the pre-existing dirty session work; record those separately and do not repair or revert them as part of this plan.

## File Structure

- Create `internal/config/default_gateway.go`: owns the built-in typed local gateway configuration.
- Create `internal/config/default_gateway_test.go`: proves the default is loopback-only, credential-free, structurally valid, and non-durable.
- Modify `cmd/live-gateway/main.go`: chooses the built-in config when no path is supplied and removes the broad shared secret sweep.
- Modify `cmd/live-gateway/main_test.go`: covers default selection, worker-secret isolation, and strict gateway credential references.
- Create `cmd/live-worker/idle.go`: owns idle diagnostics, lifecycle, listener construction, and the idle-runtime builder.
- Create `cmd/live-worker/idle_test.go`: covers content-free health behavior and bounded cancellation.
- Modify `cmd/live-worker/main.go`: parses `--backend`, selects idle or durable construction, and preserves legacy durable invocation.
- Modify `cmd/live-worker/main_test.go`: covers backend selection, invalid flag combinations, zero secret lookup in idle mode, and durable compatibility.
- Modify `README.md`: replaces the credential-first opening with a backend-only quickstart, then documents optional voice and durable paths.

---

### Task 1: Built-in Credential-free Gateway Configuration

**Files:**
- Create: `internal/config/default_gateway.go`
- Create: `internal/config/default_gateway_test.go`

**Interfaces:**
- Consumes: existing `Config.applyDefaults()` and `Config.Validate()` in `internal/config/config.go`.
- Produces: `func DefaultLocalGateway() (Config, error)` for the gateway composition root.

- [ ] **Step 1: Write the failing default-configuration test**

Create `internal/config/default_gateway_test.go`:

```go
package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultLocalGatewayIsLoopbackCredentialFreeAndNonDurable(t *testing.T) {
	cfg, err := DefaultLocalGateway()
	require.NoError(t, err)

	assert.Equal(t, "local", cfg.Mode)
	assert.Equal(t, "127.0.0.1:8080", cfg.Gateway.ListenAddr)
	assert.Equal(t, "local", cfg.Gateway.AdvertisedModel)
	assert.Equal(t, "speech_to_speech", cfg.Gateway.Voice.Provider)
	assert.Equal(t, "ws://127.0.0.1:8765/v1/realtime", cfg.Gateway.Voice.URL)
	assert.Equal(t, "local", cfg.Gateway.Voice.Model)

	for name, reference := range map[string]string{
		"voice":      cfg.Gateway.Voice.APIKeyEnv,
		"reasoner":   cfg.Worker.Reasoner.APIKeyEnv,
		"database":   cfg.Store.DatabaseURLEnv,
		"static auth": cfg.Auth.StaticBearerEnv,
		"privacy":    cfg.Safety.PrivacyHMACKeyEnv,
		"retention":  cfg.Store.Retention.HMACKeyEnv,
		"reconnect":  cfg.Gateway.Reconnect.HMACKeyEnv,
		"tool":       cfg.Features.ToolEncryptionKeyEnv,
	} {
		assert.Empty(t, reference, name)
	}
	assert.False(t, cfg.Features.Handoffs)
	assert.False(t, cfg.Features.SideEffectingTools)

	require.NoError(t, cfg.ResolveSecrets(func(name string) (string, bool) {
		t.Fatalf("default config attempted secret lookup %q", name)
		return "", false
	}))
}
```

- [ ] **Step 2: Run the test and confirm RED**

Run:

```sh
env GOCACHE=/tmp/openai-gpt-live-plan-cache go test -count=1 ./internal/config -run '^TestDefaultLocalGatewayIsLoopbackCredentialFreeAndNonDurable$'
```

Expected: compilation fails because `DefaultLocalGateway` is undefined.

- [ ] **Step 3: Add the minimal typed default**

Create `internal/config/default_gateway.go`:

```go
package config

import "fmt"

// DefaultLocalGateway returns the loopback-only, credential-free backend
// configuration used when live-gateway starts without -config.
func DefaultLocalGateway() (Config, error) {
	cfg := Config{
		Mode: "local",
		Gateway: GatewayConfig{
			AdvertisedModel: "local",
			ListenAddr:      "127.0.0.1:8080",
			Voice: VoiceConfig{
				Role:     "realtime_voice",
				Provider: "speech_to_speech",
				URL:      "ws://127.0.0.1:8765/v1/realtime",
				Model:    "local",
			},
		},
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("validate default local gateway configuration: %w", err)
	}
	return cfg, nil
}
```

- [ ] **Step 4: Run the focused test and confirm GREEN**

Run:

```sh
env GOCACHE=/tmp/openai-gpt-live-plan-cache go test -count=1 ./internal/config -run '^TestDefaultLocalGatewayIsLoopbackCredentialFreeAndNonDurable$'
```

Expected: PASS.

- [ ] **Step 5: Run the complete config package**

Run:

```sh
env GOCACHE=/tmp/openai-gpt-live-plan-cache go test -count=1 ./internal/config
```

Expected: PASS with no warnings.

- [ ] **Step 6: Commit the focused deliverable**

```sh
git add internal/config/default_gateway.go internal/config/default_gateway_test.go
git commit -m "feat: add credential-free gateway defaults"
```

---

### Task 2: Gateway Default Startup and Component-scoped Secrets

**Files:**
- Modify: `cmd/live-gateway/main.go:118-181`
- Modify: `cmd/live-gateway/main_test.go:182-260`

**Interfaces:**
- Consumes: `config.DefaultLocalGateway() (config.Config, error)` from Task 1.
- Produces: `func loadGatewayConfig(path string) (config.Config, error)` used by normal startup; gateway startup no longer calls `Config.ResolveSecrets` over the complete shared config.

- [ ] **Step 1: Write failing tests for default selection and worker-secret isolation**

Append to `cmd/live-gateway/main_test.go`:

```go
func TestLoadGatewayConfigUsesCredentialFreeDefaultWhenPathIsEmpty(t *testing.T) {
	cfg, err := loadGatewayConfig("")
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:8080", cfg.Gateway.ListenAddr)
	assert.Equal(t, "ws://127.0.0.1:8765/v1/realtime", cfg.Gateway.Voice.URL)
	assert.Empty(t, cfg.Gateway.Voice.APIKeyEnv)
}

func TestGatewayStartupDoesNotResolveWorkerOnlySecrets(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shared-local.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
mode: local
gateway:
  listen_addr: 127.0.0.1:0
  advertised_model: local
  voice:
    provider: speech_to_speech
    url: ws://127.0.0.1:8765/v1/realtime
    model: local
worker:
  reasoner:
    api_key_env: WORKER_PROVIDER_SECRET
store:
  database_url_env: WORKER_DATABASE_SECRET
  retention:
    hmac_key_env: WORKER_RETENTION_SECRET
    hmac_key_id: retention-v1
safety:
  privacy_hmac_key_env: WORKER_PRIVACY_SECRET
features:
  tool_encryption_key_env: WORKER_TOOL_SECRET
  tool_encryption_key_id: tool-v1
`), 0o600))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	allowedRuntimeLookups := map[string]struct{}{
		"LIVE_EXPECTED_CONFIG_HASH": {},
		"LIVE_METRIC_INSTANCE":      {},
		"LIVE_DEPLOYMENT_REGION":    {},
	}
	err := run(ctx, []string{"-config", path}, func(name string) (string, bool) {
		if _, allowed := allowedRuntimeLookups[name]; !allowed {
			t.Fatalf("gateway resolved inactive secret %q", name)
		}
		return "", false
	})
	assert.ErrorIs(t, err, context.Canceled)
}
```

- [ ] **Step 2: Run the two tests and confirm RED**

Run:

```sh
env GOCACHE=/tmp/openai-gpt-live-plan-cache go test -count=1 ./cmd/live-gateway -run '^(TestLoadGatewayConfigUsesCredentialFreeDefaultWhenPathIsEmpty|TestGatewayStartupDoesNotResolveWorkerOnlySecrets)$'
```

Expected: compilation fails because `loadGatewayConfig` is undefined. This is the
required RED result; after the helper is introduced in Step 3, the worker-secret
test separately proves that the broad `Config.ResolveSecrets` call is gone.

- [ ] **Step 3: Add config selection and remove the broad secret sweep**

Add this helper near `run` in `cmd/live-gateway/main.go`:

```go
func loadGatewayConfig(path string) (config.Config, error) {
	if path == "" {
		return config.DefaultLocalGateway()
	}
	return config.LoadUnresolved(path)
}
```

Then change normal startup from:

```go
if *configPath == "" {
	return errors.New("-config is required")
}
cfg, err := config.LoadUnresolved(*configPath)
```

to:

```go
cfg, err := loadGatewayConfig(*configPath)
```

Delete only this broad call:

```go
if err := cfg.ResolveSecrets(lookup); err != nil {
	return fmt.Errorf("load gateway configuration: %w", err)
}
```

Keep all active-module lookups and errors that follow it, including provider,
static authentication, PostgreSQL, reconnect, retention, expected-hash, region,
metric-instance, and production identity checks.

- [ ] **Step 4: Run the focused tests and confirm GREEN**

Run:

```sh
env GOCACHE=/tmp/openai-gpt-live-plan-cache go test -count=1 ./cmd/live-gateway -run '^(TestLoadGatewayConfigUsesCredentialFreeDefaultWhenPathIsEmpty|TestGatewayStartupDoesNotResolveWorkerOnlySecrets)$'
```

Expected: PASS. The second test returns `context.Canceled` and performs only the allowlisted non-secret runtime lookups.

- [ ] **Step 5: Add and run a regression test for explicit gateway credentials**

Append:

```go
func TestGatewayConfiguredVoiceCredentialRemainsStrict(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
mode: local
gateway:
  voice:
    url: ws://127.0.0.1:8765/v1/realtime
    model: local
    api_key_env: LOCAL_PROVIDER_SECRET
`), 0o600))

	err := run(t.Context(), []string{"-config", path}, func(name string) (string, bool) {
		return "", false
	})
	assert.EqualError(t, err, "configured provider credential is unavailable")
}
```

Run:

```sh
env GOCACHE=/tmp/openai-gpt-live-plan-cache go test -count=1 ./cmd/live-gateway -run '^TestGatewayConfiguredVoiceCredentialRemainsStrict$'
```

Expected: PASS without changing the provider lookup.

- [ ] **Step 6: Run gateway and config regression packages**

Run:

```sh
env GOCACHE=/tmp/openai-gpt-live-plan-cache go test -count=1 ./internal/config ./cmd/live-gateway
```

Expected: PASS.

- [ ] **Step 7: Commit the focused deliverable**

```sh
git add cmd/live-gateway/main.go cmd/live-gateway/main_test.go
git commit -m "feat: start gateway without credentials"
```

---

### Task 3: Idle Worker Runtime and Content-free Health

**Files:**
- Create: `cmd/live-worker/idle.go`
- Create: `cmd/live-worker/idle_test.go`

**Interfaces:**
- Consumes: existing `serviceRuntime`, `health.LiveHandler()`, `health.Registry`, and `health.CheckFunc`.
- Produces: `type idleRuntimeBuilder func(context.Context) (serviceRuntime, error)`, `func buildIdleRuntime(context.Context) (serviceRuntime, error)`, and an `idleRuntime` implementing `Run(context.Context) error` and `Close()`.

- [ ] **Step 1: Write failing handler and lifecycle tests**

Create `cmd/live-worker/idle_test.go`:

```go
package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIdleWorkerHealthIsLiveButNotReady(t *testing.T) {
	handler := idleWorkerHandler()

	for _, test := range []struct {
		path string
		code int
		body string
	}{
		{path: "/health/live", code: http.StatusOK, body: "ok\n"},
		{path: "/health/ready", code: http.StatusServiceUnavailable, body: ""},
		{path: "/admin/v1/drain", code: http.StatusNotFound, body: "404 page not found\n"},
	} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, test.path, nil))
		assert.Equal(t, test.code, recorder.Code, test.path)
		assert.Equal(t, test.body, recorder.Body.String(), test.path)
		assert.Equal(t, "no-store", recorder.Header().Get("Cache-Control"), test.path)
	}
}

func TestIdleWorkerHealthRejectsMutationMethods(t *testing.T) {
	recorder := httptest.NewRecorder()
	idleWorkerHandler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/health/live", nil))
	assert.Equal(t, http.StatusMethodNotAllowed, recorder.Code)
	assert.Equal(t, "GET, HEAD", recorder.Header().Get("Allow"))
}

type blockingIdleListener struct {
	closed chan struct{}
	once   sync.Once
}

func newBlockingIdleListener() *blockingIdleListener {
	return &blockingIdleListener{closed: make(chan struct{})}
}

func (l *blockingIdleListener) Accept() (net.Conn, error) {
	<-l.closed
	return nil, net.ErrClosed
}

func (l *blockingIdleListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func (*blockingIdleListener) Addr() net.Addr { return idleTestAddr("idle") }

type idleTestAddr string

func (a idleTestAddr) Network() string { return "test" }
func (a idleTestAddr) String() string  { return string(a) }

func TestIdleRuntimeStaysAliveUntilCancellationAndCloses(t *testing.T) {
	listener := newBlockingIdleListener()
	runtime := newIdleRuntime(listener)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()

	select {
	case err := <-done:
		t.Fatalf("idle runtime stopped early: %v", err)
	default:
	}
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	runtime.Close()
	require.NoError(t, listener.Close())
}
```

- [ ] **Step 2: Run the tests and confirm RED**

Run:

```sh
env GOCACHE=/tmp/openai-gpt-live-plan-cache go test -count=1 ./cmd/live-worker -run '^TestIdle'
```

Expected: compilation fails because `idleWorkerHandler` and `newIdleRuntime` are undefined.

- [ ] **Step 3: Implement the content-free handler and idle runtime**

Create `cmd/live-worker/idle.go` with these declarations and behavior:

```go
package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/health"
)

const idleWorkerListenAddress = "127.0.0.1:9091"

var errIdleWorkerNotReady = errors.New("durable worker backend is disabled")

type idleRuntimeBuilder func(context.Context) (serviceRuntime, error)

type idleRuntime struct {
	server    *http.Server
	listener  net.Listener
	closeOnce sync.Once
}

func idleWorkerHandler() http.Handler {
	ready := health.NewRegistry()
	_ = ready.RegisterRequired("backend", health.CheckFunc(func(context.Context) error {
		return errIdleWorkerNotReady
	}))
	mux := http.NewServeMux()
	mux.Handle("/health/live", health.LiveHandler())
	mux.Handle("/health/ready", ready.ReadyHandler())
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		mux.ServeHTTP(w, req)
	})
}

func newIdleRuntime(listener net.Listener) *idleRuntime {
	return &idleRuntime{
		server: &http.Server{
			Handler:           idleWorkerHandler(),
			ReadHeaderTimeout: 5 * time.Second,
		},
		listener: listener,
	}
}

func buildIdleRuntime(context.Context) (serviceRuntime, error) {
	listener, err := net.Listen("tcp", idleWorkerListenAddress)
	if err != nil {
		return nil, errors.New("listen idle worker diagnostics")
	}
	return newIdleRuntime(listener), nil
}

func (r *idleRuntime) Run(ctx context.Context) error {
	if r == nil || r.server == nil || r.listener == nil || ctx == nil {
		return errors.New("idle worker runtime is invalid")
	}
	serveDone := make(chan error, 1)
	go func() {
		err := r.server.Serve(r.listener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		serveDone <- err
	}()

	select {
	case err := <-serveDone:
		if err == nil {
			return errors.New("idle worker diagnostics stopped unexpectedly")
		}
		return err
	case <-ctx.Done():
		r.Close()
		<-serveDone
		return ctx.Err()
	}
}

func (r *idleRuntime) Close() {
	if r == nil {
		return
	}
	r.closeOnce.Do(func() {
		if r.server != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = r.server.Shutdown(ctx)
			cancel()
		}
		if r.listener != nil {
			_ = r.listener.Close()
		}
	})
}
```

- [ ] **Step 4: Run the idle tests and confirm GREEN**

Run:

```sh
env GOCACHE=/tmp/openai-gpt-live-plan-cache go test -count=1 ./cmd/live-worker -run '^TestIdle'
```

Expected: PASS. The second listener close proves that runtime shutdown and an
explicit repeated close are idempotent.

- [ ] **Step 5: Run the full worker command package**

Run:

```sh
env GOCACHE=/tmp/openai-gpt-live-plan-cache go test -count=1 ./cmd/live-worker
```

Expected: PASS.

- [ ] **Step 6: Commit the focused deliverable**

```sh
git add cmd/live-worker/idle.go cmd/live-worker/idle_test.go
git commit -m "feat: add idle worker runtime"
```

---

### Task 4: Worker Backend Selection and Durable Compatibility

**Files:**
- Modify: `cmd/live-worker/main.go:64-137`
- Modify: `cmd/live-worker/main_test.go:55-63,251-380`

**Interfaces:**
- Consumes: `idleRuntimeBuilder` and `buildIdleRuntime` from Task 3; existing `runtimeBuilder` and `buildRuntime` durable path.
- Produces: `func runWithBuilders(context.Context, []string, func(string) (string, bool), runtimeBuilder, idleRuntimeBuilder) error`; the existing `runWithBuilder` becomes a compatibility wrapper.

- [ ] **Step 1: Write failing selection and validation tests**

Append to `cmd/live-worker/main_test.go`:

```go
func TestRunBackendNoneDoesNotReadConfigResolveSecretsOrBuildDurableRuntime(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	started := make(chan struct{})
	idleBuilder := func(context.Context) (serviceRuntime, error) {
		return &functionRuntime{run: func(runCtx context.Context) error {
			close(started)
			<-runCtx.Done()
			return runCtx.Err()
		}}, nil
	}
	done := make(chan error, 1)
	go func() {
		done <- runWithBuilders(ctx, []string{"--backend", "none"}, func(name string) (string, bool) {
			t.Fatalf("idle worker looked up environment variable %q", name)
			return "", false
		}, func(context.Context, config.Config, resolvedSecrets) (serviceRuntime, error) {
			t.Fatal("durable builder called")
			return nil, nil
		}, idleBuilder)
	}()
	<-started
	cancel()
	require.NoError(t, <-done)
}

func TestRunRejectsInvalidBackendFlagCombinationsBeforeConstruction(t *testing.T) {
	for name, args := range map[string][]string{
		"unknown":          {"--backend", "mystery"},
		"none with config": {"--backend", "none", "-config", "worker.yaml"},
		"durable no config": {"--backend", "durable"},
		"positional":       {"--backend", "none", "extra"},
	} {
		t.Run(name, func(t *testing.T) {
			err := runWithBuilders(t.Context(), args, func(string) (string, bool) {
				t.Fatal("environment lookup occurred")
				return "", false
			}, func(context.Context, config.Config, resolvedSecrets) (serviceRuntime, error) {
				t.Fatal("durable builder called")
				return nil, nil
			}, func(context.Context) (serviceRuntime, error) {
				t.Fatal("idle builder called")
				return nil, nil
			})
			require.Error(t, err)
		})
	}
}
```

Keep the existing `TestRunRequiresConfig`; it continues to prove that omitting
both `--backend` and `-config` is a usage error.

- [ ] **Step 2: Run the selection tests and confirm RED**

Run:

```sh
env GOCACHE=/tmp/openai-gpt-live-plan-cache go test -count=1 ./cmd/live-worker -run '^(TestRunBackendNoneDoesNotReadConfigResolveSecretsOrBuildDurableRuntime|TestRunRejectsInvalidBackendFlagCombinationsBeforeConstruction|TestRunRequiresConfig)$'
```

Expected: compilation fails because `runWithBuilders` is undefined.

- [ ] **Step 3: Introduce the selector without changing the durable body**

In `cmd/live-worker/main.go`, make `runWithBuilder` a wrapper:

```go
func runWithBuilder(ctx context.Context, args []string, lookup func(string) (string, bool), builder runtimeBuilder) error {
	return runWithBuilders(ctx, args, lookup, builder, buildIdleRuntime)
}
```

Move the existing body into `runWithBuilders` and add the idle builder parameter:

```go
func runWithBuilders(
	ctx context.Context,
	args []string,
	lookup func(string) (string, bool),
	durableBuilder runtimeBuilder,
	idleBuilder idleRuntimeBuilder,
) error {
```

After the existing subcommand check, parse both flags:

```go
flags := flag.NewFlagSet("live-worker", flag.ContinueOnError)
flags.SetOutput(io.Discard)
backend := flags.String("backend", "", "worker backend: none or durable")
configPath := flags.String("config", "", "path to worker YAML configuration")
```

After positional-argument validation, select the backend before assigning
`os.LookupEnv` or loading config:

```go
selectedBackend := *backend
if selectedBackend == "" && *configPath != "" {
	selectedBackend = "durable"
}
switch selectedBackend {
case "none":
	if *configPath != "" {
		return errors.New("backend none does not accept -config")
	}
	if idleBuilder == nil {
		return errors.New("idle worker runtime builder is required")
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
case "durable":
	if *configPath == "" {
		return errors.New("-config is required for durable backend")
	}
case "":
	return errors.New("-config is required")
default:
	return errors.New("worker backend must be none or durable")
}
```

After this switch, leave the current lookup defaulting, config load, hash check,
secret resolution, redaction, durable builder call, runtime run, and cancellation
normalization unchanged, except rename `builder` to `durableBuilder`.

- [ ] **Step 4: Run the selection tests and confirm GREEN**

Run:

```sh
env GOCACHE=/tmp/openai-gpt-live-plan-cache go test -count=1 ./cmd/live-worker -run '^(TestRunBackendNoneDoesNotReadConfigResolveSecretsOrBuildDurableRuntime|TestRunRejectsInvalidBackendFlagCombinationsBeforeConstruction|TestRunRequiresConfig)$'
```

Expected: PASS.

- [ ] **Step 5: Prove legacy and explicit durable forms are equivalent**

Add this table test using the existing `writeWorkerConfig`, `dummySecrets`, and
`fakeRuntime` helpers:

```go
func TestRunSelectsDurableBackendWithOrWithoutExplicitFlag(t *testing.T) {
	for name, argsForPath := range map[string]func(string) []string{
		"legacy":   func(path string) []string { return []string{"-config", path} },
		"explicit": func(path string) []string { return []string{"--backend", "durable", "-config", path} },
	} {
		t.Run(name, func(t *testing.T) {
			path := writeWorkerConfig(t)
			ctx, cancel := context.WithCancel(t.Context())
			runtime := &fakeRuntime{started: make(chan resolvedSecrets, 1), drained: make(chan struct{})}
			done := make(chan error, 1)
			go func() {
				done <- runWithBuilders(ctx, argsForPath(path), lookupFrom(dummySecrets()), func(_ context.Context, _ config.Config, secrets resolvedSecrets) (serviceRuntime, error) {
					runtime.started <- secrets
					return runtime, nil
				}, func(context.Context) (serviceRuntime, error) {
					t.Fatal("idle builder called")
					return nil, nil
				})
			}()
			resolved := <-runtime.started
			assert.NotEmpty(t, resolved.DatabaseURL)
			assert.NotEmpty(t, resolved.ProviderAPIKey)
			cancel()
			require.NoError(t, <-done)
		})
	}
}
```

Run:

```sh
env GOCACHE=/tmp/openai-gpt-live-plan-cache go test -count=1 ./cmd/live-worker -run '^TestRunSelectsDurableBackendWithOrWithoutExplicitFlag$'
```

Expected: PASS for both table rows.

- [ ] **Step 6: Run all worker command tests**

Run:

```sh
env GOCACHE=/tmp/openai-gpt-live-plan-cache go test -count=1 ./cmd/live-worker
```

Expected: PASS.

- [ ] **Step 7: Commit the focused deliverable**

```sh
git add cmd/live-worker/main.go cmd/live-worker/main_test.go
git commit -m "feat: select idle or durable worker backend"
```

---

### Task 5: Backend-first README and Credential-free Smoke

**Files:**
- Modify: `README.md`

**Interfaces:**
- Consumes: no-config `live-gateway`, `live-worker --backend none`, gateway `/v1/realtime/capabilities`, idle-worker `/health/live`, and idle-worker `/health/ready`.
- Produces: the user-facing quickstart and verification commands requested by the feature.

- [ ] **Step 1: Rewrite the README opening around the backend-only contract**

Keep the title `# GPT Live Go framework`, then make the first sections exactly
cover these topics in this order:

1. **What it is** — Go backend only; OpenAI Realtime-compatible WebSocket gateway;
   optional durable async worker; no frontend or browser assets.
2. **Requirements** — Go 1.26 only for credential-free startup. PostgreSQL and
   provider credentials belong under optional durable/provider sections.
3. **Quick start without credentials** — two terminal commands:

   ```sh
   go run ./cmd/live-gateway
   go run ./cmd/live-worker --backend none
   ```

4. **Verify the processes** — exact commands and expected status:

   ```sh
   curl --fail http://127.0.0.1:8080/v1/realtime/capabilities
   curl --fail http://127.0.0.1:9091/health/live
   curl --silent --output /dev/null --write-out '%{http_code}\n' \
     http://127.0.0.1:9091/health/ready
   ```

   State that capabilities returns JSON, liveness returns `ok`, and idle readiness
   intentionally returns `503` because no durable backend is selected.
5. **Add local speech** — point readers to the official Hugging Face
   `speech-to-speech` repository and its loopback
   `ws://127.0.0.1:8765/v1/realtime` server. State that the Go gateway starts even
   when it is absent, while realtime sessions require it.
6. **Use explicit configuration** — retain the existing gateway example command
   and document both durable worker forms:

   ```sh
   go run ./cmd/live-gateway -config configs/gateway.example.yaml
   go run ./cmd/live-worker -config configs/worker.example.yaml
   go run ./cmd/live-worker --backend durable -config configs/worker.example.yaml
   ```

7. **Security boundary** — defaults are loopback-only and unauthenticated; never
   expose them directly; production remains OIDC/TLS/PostgreSQL/secret-manager
   controlled.
8. **Protocol and operations links** — link to `docs/protocol.md`,
   `docs/security.md`, `docs/operations.md`, `docs/testing.md`, and
   `deploy/README.md` rather than repeating the previous multi-page runbook.
9. **Design references** — link directly to the official Hugging Face
   `speech-to-speech` and Qwen Audio Agent repositories and summarize the two
   adopted patterns: local OpenAI Realtime transport and explicit no-backend
   worker lifecycle.

Remove credential generation and Compose requirements from the quickstart. Keep
them only in the explicit durable/deployment section or linked deployment docs.

- [ ] **Step 2: Run static README assertions**

Run:

```sh
rg -n 'go run ./cmd/live-gateway$|go run ./cmd/live-worker --backend none|127\.0\.0\.1:8765/v1/realtime|intentionally returns `503`|backend only' README.md
```

Expected: every required quickstart/security statement has a match.

Run:

```sh
rg -n 'LIVE_STATIC_BEARER_TOKEN|DEEPSEEK_API_KEY|DATABASE_URL' README.md
```

Expected: any remaining matches occur only in the optional explicit durable or
deployment section, never in the credential-free quickstart.

- [ ] **Step 3: Run focused tests, vet, and builds**

Run:

```sh
env GOCACHE=/tmp/openai-gpt-live-plan-cache go test -count=1 ./internal/config ./cmd/live-gateway ./cmd/live-worker
env GOCACHE=/tmp/openai-gpt-live-plan-cache go vet ./...
env GOCACHE=/tmp/openai-gpt-live-plan-cache go build ./...
```

Expected: all three commands exit 0.

- [ ] **Step 4: Perform the clean-environment process smoke**

Build both commands into a newly created directory under `/tmp`, then start them
in separate terminal sessions with an empty environment except for `PATH`:

```sh
env GOCACHE=/tmp/openai-gpt-live-plan-cache go build -o /tmp/gpt-live-gateway-smoke ./cmd/live-gateway
env GOCACHE=/tmp/openai-gpt-live-plan-cache go build -o /tmp/gpt-live-worker-smoke ./cmd/live-worker
env -i PATH=/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin /tmp/gpt-live-gateway-smoke
env -i PATH=/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin /tmp/gpt-live-worker-smoke --backend none
```

From a third session, run:

```sh
curl --fail http://127.0.0.1:8080/v1/realtime/capabilities
curl --fail http://127.0.0.1:9091/health/live
curl --silent --output /dev/null --write-out '%{http_code}\n' http://127.0.0.1:9091/health/ready
```

Expected: capabilities request succeeds, liveness succeeds with `ok`, readiness
prints `503`, neither process exits, and neither process requests a credential.
Send SIGTERM to only the two owned smoke processes and verify both exit 0.

- [ ] **Step 5: Run the repository verification ladder and classify baseline failures**

Run:

```sh
env GOCACHE=/tmp/openai-gpt-live-plan-cache go test -race -count=1 ./...
env GOCACHE=/tmp/openai-gpt-live-plan-cache go mod tidy -diff
make verify-deploy-static
git diff --check
```

Expected for task-owned packages and static checks: PASS. If the full race suite
still reports the previously observed `internal/gateway` failures in dirty
session/reactor code, record their exact test names and output separately. Do not
change or revert those user-owned files unless a new failure is demonstrably
caused by this implementation.

- [ ] **Step 6: Inspect scope and commit documentation**

Run:

```sh
git status --short
git diff -- README.md
```

Verify the task added no frontend/static files and did not modify deployment
manifests, migrations, dependencies, or user-owned dirty files.

Commit:

```sh
git add README.md
git commit -m "docs: add credential-free backend quickstart"
```

- [ ] **Step 7: Capture final evidence**

Run fresh after the documentation commit:

```sh
env GOCACHE=/tmp/openai-gpt-live-plan-cache go test -count=1 ./internal/config ./cmd/live-gateway ./cmd/live-worker
env GOCACHE=/tmp/openai-gpt-live-plan-cache go vet ./...
env GOCACHE=/tmp/openai-gpt-live-plan-cache go build ./...
git diff --check c91d8d1..HEAD
git status --short
```

Report exact pass/fail counts, the credential-free smoke results, the current
branch, the commits created by this plan, and the pre-existing dirty files that
remain untouched.
