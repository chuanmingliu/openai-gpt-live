# Production Deployment Security Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Harden the production deployment boundary with exact secret inventories, explicit egress routing, rotating shutdown identity, dependency-aware readiness, and provider-controlled error containment.

**Architecture:** Production composition is closed and component-scoped. Each process receives only the secrets it resolves. A shared immutable network-routing policy chooses direct versus proxied destinations and fails unknown production egress closed. A thresholded speech circuit drives readiness without making readiness checks dial. `live-prestop` reads the current projected identity at execution time. Provider failures are normalized to a closed enum before worker retry or persistence.

**Tech Stack:** Go 1.26, Kubernetes/Kustomize YAML, Docker Compose, existing deployment validator and mutation harness, OIDC, OTLP HTTP exporter, Coder WebSocket adapter, DeepSeek-compatible reasoner, PostgreSQL integration tests.

**Spec:** `docs/superpowers/specs/2026-08-27-production-ready-backend-design.md`

## Global Constraints

- Cross-plan order is strict: complete all of `2026-08-28-production-backend-integration.md`, then all of `2026-08-28-realtime-runtime-hardening.md`, before starting this plan. Shared files such as `cmd/live-worker/main.go`, `internal/realtime/s2sws/connector.go`, and `internal/worker/runner.go` have one sequential owner, never concurrent implementers.
- Provider-error Task 5 consumes the closed failure-class handoff established by runtime Task 8; it extends that type through worker persistence rather than defining a second incompatible public mapping.
- Production configuration never inherits local loopback permissiveness.
- Manifests and validators enforce exact allowlists. A passing subset check is insufficient.
- Task 1 may preserve the repository's existing `LIVE_ADMIN_BEARER_TOKEN` environment seam only as an unreleasable transitional commit; Task 2 must remove it before any later task or release-candidate gate. Never place bearer-token values in command arguments, logs, evidence, or error messages.
- Runtime routing tests must observe proxy/direct connections. Merely asserting a `NO_PROXY` string is not evidence.
- Readiness checks are side-effect-free snapshots; a single supervised probe loop owns dependency I/O.
- The canonical `deepseek-v4-flash` and `thinking: {type: disabled}` request contract remains mandatory.
- Production handoffs remain disabled until the release plan's live provider, rollback, drain, privacy, and operator gates are complete.
- Do not introduce dynamic installers, source checkouts, mutable image tags, or provider credentials into tests or manifests.

---

## Task 1: Enforce Exact Component Secret Inventories

**Files:**

- Modify: `cmd/live-gateway/main_test.go`
- Modify: `deploy/k8s/base/gateway-deployment.yaml`
- Modify: `deploy/k8s/base/worker-deployment.yaml`
- Modify: `deploy/docker-compose.yml`
- Modify: `deploy/compose.development-durable.yml`
- Modify: `deploy/scripts/durable-compose.sh`
- Modify: `deploy/validate/main.go`
- Modify: `deploy/validate/main_test.go`
- Modify: `deploy/scripts/test-validate-deploy.sh`

**Interfaces:**

- Consumes: gateway `func run(ctx context.Context, args []string, lookup func(string) (string, bool)) error`; integration Task 4's worker `func runWithBuilders(ctx context.Context, args []string, lookup func(string) (string, bool), durableBuilder runtimeBuilder, idleBuilder idleRuntimeBuilder) error`; secret references `Config.Store.DatabaseURLEnv`, `Config.Gateway.Voice.APIKeyEnv`, `Config.Gateway.Reconnect.HMACKeyEnv`, `Config.Store.Retention.HMACKeyEnv`, `Config.Worker.Reasoner.APIKeyEnv`, `Config.Safety.PrivacyHMACKeyEnv`, and `Config.Features.ToolEncryptionKeyEnv`; rendered Compose service `environment` maps; rendered Kubernetes container `env` lists.
- Produces: `func validateProductionSecretInventory(component string, names []string) error`; exact transitional inventory constants `gatewayProductionSecretNames []string` and `workerProductionSecretNames []string`; manifests in which the gateway and worker receive only their owned secret names plus the explicitly transitional `LIVE_ADMIN_BEARER_TOKEN` environment value that Task 2 removes.

- [ ] Add one table-driven RED gateway lookup test for `DEEPSEEK_API_KEY`, `LIVE_PRIVACY_HMAC_KEY`, and `LIVE_TOOL_ENCRYPTION_KEY`.
- [ ] Add one validator mutation case per worker-only secret injected into the gateway.
- [ ] Add one validator mutation case per required gateway secret removed from the gateway.
- [ ] Add one validator mutation case per required worker secret removed from the worker.
- [ ] Add the durable Compose assertion that the gateway environment omits every worker-only secret.
- [ ] Run the focused RED tests and confirm at least one injected or missing name is falsely accepted.

```sh
go test -race -count=1 ./cmd/live-gateway ./deploy/validate -run 'Test.*(Secret|Environment)'
./deploy/scripts/test-validate-deploy.sh
```

- [ ] Add the closed production inventory constants with `LIVE_ADMIN_BEARER_TOKEN` as the exact transitional preStop requirement for both workloads:

```text
gateway: DATABASE_URL, S2S_API_KEY, LIVE_RETENTION_HMAC_KEY, LIVE_RECONNECT_HMAC_KEY,
         LIVE_ADMIN_BEARER_TOKEN
worker:  DATABASE_URL, DEEPSEEK_API_KEY, LIVE_PRIVACY_HMAC_KEY,
         LIVE_RETENTION_HMAC_KEY, LIVE_TOOL_ENCRYPTION_KEY, LIVE_ADMIN_BEARER_TOKEN
```

- [ ] Update the validator to compare sorted actual and expected names, rejecting both missing and extra names.
- [ ] Keep `LIVE_STATIC_BEARER_TOKEN` as an explicit local-only addition outside the production constants.
- [ ] Remove worker-only gateway lookups from `durable-compose.sh`.
- [ ] Update the Kubernetes and Compose environments to match the transitional exact inventories.
- [ ] Re-run the focused tests and require PASS.
- [ ] Render Compose/Kubernetes through the static verifier and require PASS.
- [ ] Commit the exact secret-inventory slice.

```sh
go test -race -count=10 ./cmd/live-gateway ./deploy/validate
./deploy/scripts/test-validate-deploy.sh
make verify-deploy-static
git add cmd/live-gateway/main_test.go deploy/k8s/base/gateway-deployment.yaml \
  deploy/k8s/base/worker-deployment.yaml deploy/docker-compose.yml \
  deploy/compose.development-durable.yml deploy/scripts/durable-compose.sh \
  deploy/validate/main.go deploy/validate/main_test.go \
  deploy/scripts/test-validate-deploy.sh
git commit -m 'fix: enforce least privilege deployment secrets'
```

---

## Task 2: Read Rotating Projected Identity in preStop

**Files:**

- Modify: `deploy/prestop/main.go`
- Modify: `deploy/prestop/main_test.go`
- Modify: `deploy/k8s/base/gateway-deployment.yaml`
- Modify: `deploy/k8s/base/worker-deployment.yaml`
- Modify: `deploy/validate/main.go`
- Modify: `deploy/validate/main_test.go`
- Modify: `deploy/scripts/deploy-k8s.sh`
- Modify: `deploy/k8s/secrets.example.env`
- Modify: `deploy/scripts/test-validate-deploy.sh`

**Interfaces:**

- Consumes: Task 1's `validateProductionSecretInventory`; Kubernetes projected-volume layout `token -> ..data/token` and `..data -> ..TIMESTAMP_DIRECTORY`; existing `type drainFunc func(context.Context, string, string) error`.
- Produces: `type tokenSource func(context.Context) ([]byte, error)`; `func newProjectedTokenSource(mountRoot, relativePath string, maximumBytes int64) (tokenSource, error)`; `func execute(ctx context.Context, client *http.Client, baseURL string, readToken tokenSource, interval time.Duration, drain drainFunc) error`; `live-prestop --token-file /var/run/live-admin/token`.

- [ ] Add the RED rotation test for `token -> ..data/token` and atomic `..data` replacement; assert the drain request uses only the post-rotation bytes.
- [ ] Add one RED rejection test each for mount escape, extra symlink hop, invalid timestamp name, and a non-regular final inode.
- [ ] Add one RED rejection test each for oversized, empty, surrounding-whitespace, control-character, and invalid-UTF-8 token content.
- [ ] Add one RED rejection test each for unsafe mode and inode replacement during a read.
- [ ] Capture stdout, stderr, and returned errors in the rejection table; assert neither the token sentinel nor resolved filesystem path appears.
- [ ] Add one manifest mutation case each for environment token, absent mount, writable mount, wrong path, extra projected key, and wrong mode.
- [ ] Run the focused RED tests and confirm the current environment-token implementation fails them.
- [ ] Change the execution seam:

```go
type tokenSource func(context.Context) ([]byte, error)

func execute(
    ctx context.Context,
    client *http.Client,
    baseURL string,
    readToken tokenSource,
    interval time.Duration,
    drain drainFunc,
) error
```

- [ ] Implement `newProjectedTokenSource` by opening the trusted mount root once and resolving only the two Kubernetes-managed relative links beneath it.
- [ ] `fstat` the final regular file before and after a bounded read; reject identity, type, size, or mode changes.
- [ ] Add `--token-file /var/run/live-admin/token` and call `readToken` immediately before each drain request.
- [ ] Remove `LIVE_ADMIN_BEARER_TOKEN` from both production pod environments.
- [ ] Update Task 1's final production inventory constants so they contain no transitional admin bearer.
- [ ] Add the exact read-only projected OIDC token volume and mount without `subPath`.
- [ ] Set the projected file mode and pod `fsGroup` so numeric non-root processes can read but no container can write.
- [ ] Make the validator reject a service-account token projection in place of the externally rotated OIDC token.
- [ ] Re-run the focused race tests and require PASS.
- [ ] Re-run static deployment verification and require PASS.
- [ ] Commit the rotating preStop identity slice.

```sh
go test -race -count=25 ./deploy/prestop ./deploy/validate
./deploy/scripts/test-validate-deploy.sh
make verify-deploy-static
git add deploy/prestop/main.go deploy/prestop/main_test.go \
  deploy/k8s/base/gateway-deployment.yaml deploy/k8s/base/worker-deployment.yaml \
  deploy/validate/main.go deploy/validate/main_test.go deploy/scripts/deploy-k8s.sh \
  deploy/k8s/secrets.example.env deploy/scripts/test-validate-deploy.sh
git commit -m 'fix: read rotating shutdown identity at execution'
```

---

## Task 3: Introduce a Closed Production Egress Policy

**Files:**

- Create: `internal/netroute/policy.go`
- Create: `internal/netroute/policy_test.go`
- Modify: `internal/realtime/s2sws/connector.go`
- Modify: `internal/realtime/s2sws/connector_test.go`
- Modify: `internal/telemetry/runtime.go`
- Modify: `internal/telemetry/runtime_test.go`
- Modify: `internal/auth/oidc.go`
- Modify: `internal/auth/oidc_test.go`
- Modify: `internal/reasoner/deepseek/client.go`
- Modify: `internal/reasoner/deepseek/client_test.go`
- Modify: `cmd/live-gateway/main.go`
- Modify: `cmd/live-gateway/main_test.go`
- Modify: `cmd/live-worker/main.go`
- Modify: `cmd/live-worker/main_test.go`
- Modify: `deploy/k8s/overlays/production/patch.yaml`
- Modify: `deploy/scripts/render-kustomize.sh`
- Modify: `deploy/validate/main.go`
- Modify: `deploy/validate/main_test.go`

**Interfaces:**

- Consumes: `func s2sws.New(cfg s2sws.Config) (*s2sws.Connector, error)`; `func auth.NewOIDC(ctx context.Context, cfg auth.OIDCConfig) (*auth.OIDC, error)`; `func telemetry.NewRuntime(ctx context.Context, cfg telemetry.RuntimeConfig) (*telemetry.Runtime, error)`; `func deepseek.New(config deepseek.Config, httpClient *http.Client) (*deepseek.Client, error)`.
- Produces: `func NewProductionPolicy(proxyURL string, direct, proxied []string) (*netroute.Policy, error)`; `func (p *netroute.Policy) Proxy(request *http.Request) (*url.URL, error)`; `func (p *netroute.Policy) HTTPClient(timeout time.Duration) *http.Client`; `s2sws.Config.HTTPClient *http.Client`; `telemetry.RuntimeConfig.HTTPClient *http.Client`.

- [ ] Add RED direct-observer cases for exact origins, default ports, IPv4/IPv6 loopback, `.svc`, and `.svc.cluster.local`.
- [ ] Add RED rejection cases for suffix confusion, userinfo, mixed-case ambiguity, direct/proxied overlap, and unknown destinations.
- [ ] Add a RED proxy-observer case proving external speech and reasoner traffic traverses only the configured proxy.
- [ ] Add a RED redirect case proving every HTTP redirect is refused before a second origin is contacted.
- [ ] Run `go test -race -count=1 ./internal/netroute` and confirm the missing policy fails.
- [ ] Define the immutable policy through these public methods; all fields remain private:

```go
func NewProductionPolicy(proxyURL string, direct, proxied []string) (*Policy, error)
func (p *Policy) Proxy(*http.Request) (*url.URL, error)
func (p *Policy) HTTPClient(timeout time.Duration) *http.Client
```

- [ ] Normalize direct origins to loopback or exact `.svc`/`.svc.cluster.local` hosts and reject every external direct origin.
- [ ] Normalize proxied origins as exact reviewed external speech/reasoner origins and reject overlap with direct origins.
- [ ] Make `Policy.Proxy` fail closed for an origin absent from both sets.
- [ ] Make `Policy.HTTPClient` clone a bounded transport, use `Policy.Proxy`, preserve TLS verification, and set `CheckRedirect` to return `http.ErrUseLastResponse`.
- [ ] Add `HTTPClient *http.Client` to `s2sws.Config` and pass it as `websocket.DialOptions.HTTPClient`.
- [ ] Add the malicious discovery test whose `jwks_uri` changes origin.
- [ ] Require discovered `jwks_uri` to share the issuer origin; reject cross-origin discovery even when the second origin is otherwise routable.
- [ ] Add `HTTPClient *http.Client` to `telemetry.RuntimeConfig` and use its transport for the OTLP HTTP exporter.
- [ ] Inject the same policy client into OIDC, telemetry, speech, and DeepSeek composition.
- [ ] Render deterministic `HTTPS_PROXY` and `NO_PROXY` only as defense in depth; keep the injected transport authoritative.
- [ ] Run observer tests proving OIDC/OTLP direct, speech/reasoner proxied, and unknown egress refused.
- [ ] Run the focused race suite and require PASS.
- [ ] Run static deployment verification and require PASS.
- [ ] Commit the closed egress-policy slice.

```sh
go test -race -count=10 ./internal/netroute ./internal/realtime/s2sws \
  ./internal/auth ./internal/telemetry ./internal/reasoner/deepseek \
  ./cmd/live-gateway ./cmd/live-worker
make verify-deploy-static
git add internal/netroute/policy.go internal/netroute/policy_test.go \
  internal/realtime/s2sws/connector.go internal/realtime/s2sws/connector_test.go \
  internal/telemetry/runtime.go internal/telemetry/runtime_test.go \
  internal/auth/oidc.go internal/auth/oidc_test.go \
  internal/reasoner/deepseek/client.go internal/reasoner/deepseek/client_test.go \
  cmd/live-gateway/main.go cmd/live-gateway/main_test.go \
  cmd/live-worker/main.go cmd/live-worker/main_test.go \
  deploy/k8s/overlays/production/patch.yaml deploy/scripts/render-kustomize.sh \
  deploy/validate/main.go deploy/validate/main_test.go
git commit -m 'fix: enforce explicit production egress routing'
```

---

## Task 4: Add a Hysteretic Speech Readiness Circuit

**Files:**

- Create: `internal/health/dependency_circuit.go`
- Create: `internal/health/dependency_circuit_test.go`
- Modify: `internal/config/config.go`
- Modify: `internal/config/config_test.go`
- Modify: `internal/config/hash.go`
- Modify: `internal/config/hash_test.go`
- Modify: `internal/realtime/s2sws/connector.go`
- Modify: `internal/realtime/s2sws/connector_test.go`
- Modify: `cmd/live-gateway/main.go`
- Modify: `cmd/live-gateway/main_test.go`
- Modify: `deploy/config/production.yaml`
- Modify: `deploy/k8s/base/production.yaml`

**Interfaces:**

- Consumes: Task 3's policy-backed `*s2sws.Connector`; `func (r *health.Registry) RegisterRequired(name string, checker health.Checker) error`; `type health.Checker interface { Check(context.Context) error }`.
- Produces: `config.GatewayConfig.SpeechReadiness config.CircuitConfig`; `type health.DependencyCircuitConfig struct { ProbeInterval time.Duration; ProbeTimeout time.Duration; FailureThreshold int; SuccessThreshold int }`; `type health.ProbeFunc func(context.Context) error`; `func health.NewDependencyCircuit(cfg health.DependencyCircuitConfig, probe health.ProbeFunc) (*health.DependencyCircuit, error)`; `func (c *health.DependencyCircuit) Run(ctx context.Context) error`; `func (c *health.DependencyCircuit) Observe(err error)`; `func (c *health.DependencyCircuit) Check(ctx context.Context) error`; `func (c *s2sws.Connector) Probe(ctx context.Context) error`.
- Normalized production defaults are exact: `ProbeInterval=10s`, `ProbeTimeout=2s`, `FailureThreshold=3`, and `SuccessThreshold=2`. Validation accepts interval `[5s,60s]`, timeout `[100ms,5s]` with `ProbeTimeout < ProbeInterval`, and each threshold `[1,100]`; all four normalized values enter the canonical config hash and both production YAML files.

- [ ] Add a RED circuit test proving a new production circuit is unready before any successful probe.
- [ ] Add a RED circuit test proving initial readiness requires `SuccessThreshold` consecutive successes.
- [ ] Add a RED circuit test proving fewer than `FailureThreshold` failures remain ready after that initial recovery.
- [ ] Add a RED circuit test proving the threshold failure trips readiness.
- [ ] Add a RED circuit test proving recovery requires `SuccessThreshold` consecutive successes.
- [ ] Add a RED registry test proving readiness HTTP requests never invoke the probe.
- [ ] Add a RED concurrency test proving one `Run` call owns one probe loop.
- [ ] Add a RED cancellation test proving the loop joins and leaves no timer or goroutine.
- [ ] Add a RED public-error test proving no provider detail leaves `Check`.
- [ ] Add a RED local-composition test proving zero circuit creation and zero speech network calls.
- [ ] Add RED default/boundary tables proving the exact defaults above; accept both endpoints of every closed range, reject values immediately outside them, reject `ProbeTimeout >= ProbeInterval`, and prove the two production YAMLs normalize to the same values and canonical hash.
- [ ] Run the focused RED suite and confirm the missing circuit fails.
- [ ] Add validated configuration and hash ownership:

```go
type CircuitConfig struct {
    ProbeInterval    time.Duration
    ProbeTimeout     time.Duration
    FailureThreshold int
    SuccessThreshold int
}
```

- [ ] Add `SpeechReadiness CircuitConfig` to `GatewayConfig` with YAML key `speech_readiness`.
- [ ] Normalize omitted production values to exactly `10s`, `2s`, `3`, and `2`; validate interval `[5s,60s]`, timeout `[100ms,5s]` and strictly less than interval, and integer thresholds `[1,100]`.
- [ ] Include all four normalized values in the canonical configuration hash.
- [ ] Set the exact normalized defaults explicitly in `deploy/config/production.yaml` and `deploy/k8s/base/production.yaml`, and assert both files produce the same canonical hash as the typed default.
- [ ] Convert `config.CircuitConfig` field-for-field to `health.DependencyCircuitConfig` in gateway composition; neither package imports the other.
- [ ] Implement `DependencyCircuit.Run` as the only owner of scheduled dependency I/O.
- [ ] Initialize `DependencyCircuit` unready and implement `Observe` as the locked hysteresis transition.
- [ ] Implement `Check` as a side-effect-free snapshot.
- [ ] Implement `Connector.Probe(ctx)` as one bounded authenticated handshake followed by an immediate graceful close through Task 3's routing/TLS client.
- [ ] Start the circuit under the gateway lifecycle supervisor only in production composition.
- [ ] Register `ready.RegisterRequired("speech", circuit)` only in production composition.
- [ ] Keep local composition circuit-free and process-ready before first speech use.
- [ ] Re-run the focused race suite and require PASS.
- [ ] Commit the speech readiness-circuit slice.

```sh
go test -race -count=25 ./internal/health ./internal/realtime/s2sws \
  ./cmd/live-gateway ./internal/config
git add internal/health/dependency_circuit.go internal/health/dependency_circuit_test.go \
  internal/config/config.go internal/config/config_test.go internal/config/hash.go \
  internal/config/hash_test.go internal/realtime/s2sws/connector.go \
  internal/realtime/s2sws/connector_test.go cmd/live-gateway/main.go \
  cmd/live-gateway/main_test.go deploy/config/production.yaml \
  deploy/k8s/base/production.yaml
git commit -m 'fix: gate production readiness on stable speech health'
```

---

## Task 5: Normalize Provider Errors Before Retry or Persistence

**Files:**

- Modify: `internal/reasoner/reasoner.go`
- Create: `internal/reasoner/reasoner_test.go`
- Modify: `internal/reasoner/deepseek/errors.go`
- Modify: `internal/reasoner/deepseek/client.go`
- Modify: `internal/reasoner/deepseek/client_test.go`
- Modify: `internal/worker/store.go`
- Modify: `internal/worker/runner.go`
- Modify: `internal/worker/runner_test.go`
- Modify: `internal/store/postgres/leases.go`
- Modify: `internal/store/postgres/leases_integration_test.go`
- Create: `test/integration/provider_error_privacy_test.go`
- Modify: `deploy/scripts/run-go-test-no-skip.sh`
- Modify: `deploy/scripts/test-go-test-no-skip.sh`

**Interfaces:**

- Consumes: integration Task 6's strict zero-skip Go wrapper; runtime Task 8's `type failure.Class string`, constants `failure.Internal`, `failure.Timeout`, `failure.RateLimited`, `failure.TemporarilyUnavailable`, `failure.ResourceExhausted`, `failure.InvalidResponse`, and `failure.TransportFailure`, plus `func (c failure.Class) Valid() bool` and `func (c failure.Class) Code() string`; `reasoner.Streamer.Stream(ctx context.Context, request reasoner.Request, onChunk func(reasoner.Chunk) error) (reasoner.Result, error)`; `worker.Store.ScheduleRetry(context.Context, worker.LeaseToken, worker.RetryRecord) error`; `func (r *postgres.Repository) ScheduleRetry(ctx context.Context, token worker.LeaseToken, record worker.RetryRecord) error`.
- Produces: `type reasoner.ProviderError struct { Class failure.Class; Retryable bool; RetryAfter time.Duration }`; `func (e *reasoner.ProviderError) Error() string`; existing durable `worker.RetryRecord.Code string` populated only from `failure.Class.Code()`; preserved command contract `deploy/scripts/run-go-test-no-skip.sh --expect-minimum N [--expect-top-level SORTED_NAMES] -- go test -json arguments`, where `N` is positive.

- [ ] Add the request-contract assertion for `deepseek-v4-flash`, `DisableThinking`, and JSON `"thinking":{"type":"disabled"}`.
- [ ] Add one RED adapter test rejecting returned `reasoning_content`.
- [ ] Add one malicious in-process provider fixture that repeats a unique sentinel in HTTP status metadata, JSON code/message/body, request ID, and SSE error.
- [ ] RED-assert the sentinel is absent from the adapter error value and `Error()` string.
- [ ] RED-assert the sentinel is absent from `worker.RetryRecord` and presentation events.
- [ ] RED-assert the sentinel is absent from captured logs and metrics labels.
- [ ] Add PostgreSQL integration test `TestScheduleRetryPersistsOnlyFailureClass` for the durable retry column and `TestMaliciousProviderErrorNeverPersistsOrPresents` for handoff rows, journal events, and outbox rows.
- [ ] Extend the integration-plan shell-wrapper fixtures with provider-privacy sentinel output and tagged-integration package-terminal cases; preserve all existing pass/skip/malformed/truncated/zero/child-failure coverage.
- [ ] Run the focused RED unit tests and confirm provider-controlled material still crosses at least one boundary.
- [ ] Reuse the `internal/failure.Class` created by runtime Task 8; do not define another string domain:

```go
type ProviderError struct {
    Class      failure.Class
    Retryable  bool
    RetryAfter time.Duration
}

func (e *ProviderError) Error() string { return "reasoner provider error" }

type RetryRecord struct {
    MutationID                string
    ExpectedObjectiveRevision uint32
    ReadyAt                   time.Time
    RetryCount                uint32
    Code                      string
}
```

- [ ] Map raw status/code/body inside the DeepSeek adapter and discard the raw values before returning.
- [ ] Keep `RetryRecord.Code` as the existing durable string representation; validate the class and assign only `providerError.Class.Code()` when constructing a retry.
- [ ] Reconstruct `candidate := failure.Class(record.Code)` and require `candidate.Valid()` before PostgreSQL persistence.
- [ ] Map an invalid durable value to `failure.InvalidResponse.Code()`.
- [ ] Bind only the resulting validated fixed code to the durable column.
- [ ] Replace `tool_output_limit` string inspection with one package-private typed sentinel.
- [ ] Feed the same `failure.Class` into the runtime plan's content-free session mapping.
- [ ] Preserve the integration wrapper's strict JSON, clean-exit, no-skip, positive-count, complete-package, bounded-capture, content-free-summary contract while extending its boundary fixtures; do not add an allow-skip mode.
- [ ] Run the wrapper boundary test and require PASS.
- [ ] Run the focused unit race suite and require PASS.
- [ ] Require a nonempty integration database before invoking the provider-privacy test:

```sh
test -n "${TEST_DATABASE_URL:-}" || { echo 'TEST_DATABASE_URL is required for provider privacy integration' >&2; exit 1; }
TEST_DATABASE_URL="$TEST_DATABASE_URL" \
./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 2 \
  --expect-top-level TestMaliciousProviderErrorNeverPersistsOrPresents,TestScheduleRetryPersistsOnlyFailureClass -- \
  go test -json -race -count=1 -tags=integration \
  ./internal/store/postgres ./test/integration \
  -run '^(TestScheduleRetryPersistsOnlyFailureClass|TestMaliciousProviderErrorNeverPersistsOrPresents)$'
```

- [ ] Commit the provider-error boundary and zero-skip integration wrapper.

```sh
./deploy/scripts/test-go-test-no-skip.sh
go test -race -count=25 ./internal/reasoner/... ./internal/worker ./internal/presentation
test -n "${TEST_DATABASE_URL:-}"
TEST_DATABASE_URL="$TEST_DATABASE_URL" \
./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 2 \
  --expect-top-level TestMaliciousProviderErrorNeverPersistsOrPresents,TestScheduleRetryPersistsOnlyFailureClass -- \
  go test -json -race -count=1 -tags=integration \
  ./internal/store/postgres ./test/integration \
  -run '^(TestScheduleRetryPersistsOnlyFailureClass|TestMaliciousProviderErrorNeverPersistsOrPresents)$'
git add internal/reasoner/reasoner.go internal/reasoner/reasoner_test.go \
  internal/reasoner/deepseek/errors.go internal/reasoner/deepseek/client.go \
  internal/reasoner/deepseek/client_test.go internal/worker/store.go \
  internal/worker/runner.go internal/worker/runner_test.go \
  internal/store/postgres/leases.go internal/store/postgres/leases_integration_test.go \
  test/integration/provider_error_privacy_test.go deploy/scripts/run-go-test-no-skip.sh \
  deploy/scripts/test-go-test-no-skip.sh
git commit -m 'fix: contain provider controlled failures'
```

---

## Task 6: Pin Production Model and Handoff Promotion Preconditions

**Files:**

- Modify: `internal/config/config_test.go`
- Modify: `internal/reasoner/deepseek/client_test.go`
- Modify: `deploy/validate/main.go`
- Modify: `deploy/validate/main_test.go`
- Modify: `deploy/config/production.yaml` only if a test exposes drift
- Modify: `deploy/k8s/base/production.yaml` only if a test exposes drift
- Modify: `deploy/scripts/test-validate-deploy.sh`

**Interfaces:**

- Consumes: `config.WorkerConfig.Reasoner config.ReasonerConfig`; `config.FeatureConfig.Handoffs bool`; `reasoner.Request.DisableThinking bool`; DeepSeek `providerRequest.Thinking *providerThinking` where ``type providerThinking struct { Type string `json:"type"` }``.
- Produces: production constants `productionReasonerModel = "deepseek-v4-flash"` and `productionHandoffsEnabled = false`; validator checks that accept only JSON `"thinking":{"type":"disabled"}` and reject `features.handoffs: true` for the current release.

- [ ] Add one characterization test for each production config source requiring `deepseek-v4-flash`.
- [ ] Add one request-serialization test requiring `"thinking":{"type":"disabled"}`.
- [ ] Add one characterization test requiring production `features.handoffs: false`.
- [ ] Add one mutation case each for a retired model, thinking enabled, thinking omitted, handoffs enabled, and a model outside the compiled allowlist.
- [ ] Run the focused RED mutation suite and confirm at least one drift is accepted today.
- [ ] Add the two production constants and compare rendered values against them.
- [ ] Reject `features.handoffs: true` unconditionally for this release; do not add a runtime release-evidence flag.
- [ ] Map every rejection to one stable content-free validator class.
- [ ] Re-run the focused tests and require PASS.
- [ ] Leave production YAML byte-identical if it already satisfies the tested contract.
- [ ] Run static verification and require PASS.
- [ ] Commit the production model and handoff-policy guard.

```sh
go test -race -count=10 ./internal/config ./internal/reasoner/deepseek ./deploy/validate \
  -run 'Test.*(ProductionModel|Thinking|Handoff)'
./deploy/scripts/test-validate-deploy.sh
make verify-deploy-static
git add internal/config/config_test.go internal/reasoner/deepseek/client_test.go \
  deploy/validate/main.go deploy/validate/main_test.go \
  deploy/scripts/test-validate-deploy.sh deploy/config/production.yaml \
  deploy/k8s/base/production.yaml
git commit -m 'test: pin production reasoner and handoff policy'
```

---

## Task 7: Production Security Regression Gate

**Files:**

- Verify only: all files changed by Tasks 1-6
- Evidence remains untracked: `artifacts/` is not modified or committed by this plan

**Interfaces:**

- Consumes: Tasks 1-6 production contracts; Task 5's `run-go-test-no-skip.sh --expect-minimum N [--expect-top-level SORTED_NAMES] -- go test -json arguments`; caller-provided nonempty `TEST_DATABASE_URL` for an already-running disposable PostgreSQL database.
- Produces: a clean immutable implementation commit whose unit, integration, and static deployment gates pass; the only allowed claim is `release-candidate gates passed`.

- [ ] Require `git status --porcelain` to be empty and record `candidate_commit=$(git rev-parse HEAD)` before verification.
- [ ] Run the exact secret, token rotation, proxy-routing, speech-circuit, model-policy, and malicious-provider unit packages together under `-race`.

```sh
./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 1 -- \
  go test -json -race -count=1 ./cmd/live-gateway ./cmd/live-worker ./deploy/prestop \
    ./deploy/validate ./internal/netroute ./internal/health ./internal/realtime/s2sws \
    ./internal/auth ./internal/telemetry ./internal/reasoner/... ./internal/worker \
    ./internal/presentation
```

- [ ] Require a nonempty integration database URL; absence is a failed prerequisite, never a skip.

```sh
test -n "${TEST_DATABASE_URL:-}" || { echo 'TEST_DATABASE_URL is required for production security integration' >&2; exit 1; }
```

- [ ] Run every PostgreSQL integration test as exact JSON through the zero-skip parser.

```sh
TEST_DATABASE_URL="$TEST_DATABASE_URL" \
./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 1 -- \
  go test -json -race -count=1 -tags=integration \
  ./migrations ./internal/store/postgres ./internal/admission ./internal/feature \
  ./internal/gateway ./internal/worker ./cmd/live-gateway ./cmd/live-worker \
  ./test/integration/...
```

- [ ] Render and validate Compose/Kubernetes statically; these commands may invoke `docker compose config` but must not start containers or contact a provider.

```sh
./deploy/scripts/test-validate-deploy.sh
make verify-deploy-static
```

- [ ] Run the repository-wide compiler and analysis gates:

```sh
test_packages=$(go list -f '{{if or .TestGoFiles .XTestGoFiles}}{{.ImportPath}}{{end}}' ./...) || exit 1
test -n "$test_packages" || exit 1
./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 1 -- \
  go test -json -race -count=1 $test_packages
go vet ./...
go build ./...
go mod tidy -diff
make verify-deploy-static
git diff --check
```

- [ ] Re-read `git rev-parse HEAD` and require it equals `candidate_commit` with an empty worktree.
- [ ] Record only `release-candidate gates passed` after all commands succeed.
- [ ] Defer Docker engine, disposable cluster, external speech/reasoner, canary, rollback, capacity/chaos, privacy review, and signing to the explicit manual promotion boundary in the browser/docs/release plan.

## Plan Completion Criteria

- Gateway and worker pods receive exact least-privilege secrets and no cross-component credentials.
- Shutdown reads a rotated projected OIDC token at execution time without leaking it.
- OIDC/OTLP/internal traffic bypasses the proxy, external speech/reasoner traffic uses it, and unknown egress fails closed.
- Production readiness changes only after thresholded speech health and stable recovery; local mode performs no probe.
- Provider-controlled text cannot reach retry records, PostgreSQL, presentation, telemetry, public errors, or release evidence.
- Production model/thinking/catalog/handoff policy is statically pinned and cannot drift silently.
