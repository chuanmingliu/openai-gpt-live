# Production Backend Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add credential-free local startup for the gateway, idle worker, and canonical loopback browser without weakening any explicitly configured durable or production path.

**Architecture:** A typed built-in local gateway configuration is selected only when `live-gateway` receives no `-config`. The worker selects either a dependency-free idle runtime or the existing strict durable runtime. The canonical browser BFF supports an optional bearer only in direct loopback mode; `/setup` and durable wizard paths continue to pass an explicit bearer. Component-scoped secret resolution prevents the gateway from reading worker-only credentials.

**Tech Stack:** Go 1.26, `net/http`, existing YAML configuration/domain types, canonical `demo/live-client` BFF, shell boundary tests, Node test runner.

**Spec:** `docs/superpowers/specs/2026-08-27-production-ready-backend-design.md`

## Global Constraints

- Work only in `/Users/chuanmingliu/Documents/openai-gpt-live/.worktrees/production-ready-backend` on `codex/production-ready-backend`.
- Do not modify, reset, clean, or delete anything in the inherited `codex/credential-free-backend` worktree. It is an audit artifact.
- Do not cherry-pick the older browser or README commits wholesale. Recreate each retained contract under a failing test against the canonical code.
- Credential-free means process startup, not successful inference. No test may claim inference works without an external speech service.
- An explicit configuration remains strict. Missing, empty, whitespace-only, malformed, or oversized referenced secrets fail before readiness.
- The local defaults remain loopback-only and must never be reused by production manifests.
- Preserve the canonical `/setup`, vertical-profile, Web Search, handoff-progress, protocol-log, reconnect, and barge-in behavior.
- Every task follows red-green-refactor and ends with the listed focused gate before its commit.
- Do not start Docker, PostgreSQL, a speech provider, or any external service in the unit-test tasks below.
- Cross-plan order is strict: complete this plan first, then realtime runtime hardening, then deployment/security, then browser/docs/release. Do not execute plans concurrently because they deliberately share composition-root and adapter files.
- Execution starts only after these four plan documents are the sole paths in one commit with the exact subject `docs: plan production-ready backend hardening`. That immutable commit is the implementation-range base used by Task 7; fail before Task 1 if the exact subject is absent or non-unique.
- Redis fast/slow thinking and automatic handoff decisions remain deferred. Do not introduce Redis, a cache authority, or a new handoff-decision model in this execution series.

---

## Task 1: Add the Typed Local Gateway Default

**Files:**

- Create: `internal/config/default_gateway.go`
- Create: `internal/config/default_gateway_test.go`

**Interfaces:**

- Consumes: `config.Config`, `func (Config) Validate() error`, and package-private `func (*Config) applyDefaults()` from the existing configuration domain.
- Produces: `func config.DefaultLocalGateway() (config.Config, error)`, which Task 2 and the credential-free process smoke use without environment or filesystem access.

- [ ] Write `TestDefaultLocalGatewayIsLoopbackCredentialFreeAndNonDurable` first. Assert the exact listener, model alias, speech URL/provider, disabled durable features, empty credential references, and a successful `Validate()`.
- [ ] Run the focused test and observe the compile failure for the missing constructor.

```sh
env GOCACHE=/tmp/gpt-live-production-cache \
  go test -count=1 ./internal/config \
  -run '^TestDefaultLocalGatewayIsLoopbackCredentialFreeAndNonDurable$'
```

- [ ] Implement the exported constructor without reading the environment or filesystem:

```go
package config

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
        return Config{}, err
    }
    return cfg, nil
}
```

- [ ] Use the current config field names rather than introducing aliases merely to match the sketch. Keep OIDC, PostgreSQL, telemetry, handoffs, tools, retention, and secret-manager references disabled/empty.
- [ ] Add an ownership test: mutating a returned slice/map cannot affect a later default.
- [ ] Re-run the focused package under the race detector and require PASS.

```sh
go test -race -count=10 ./internal/config -run '^TestDefaultLocalGateway'
```

- [ ] Stage the two owned paths and commit the independently green task.

```sh
git add internal/config/default_gateway.go internal/config/default_gateway_test.go
git commit -m 'feat: add credential-free gateway defaults'
```

---

## Task 2: Select the Local Default and Scope Gateway Secrets

**Files:**

- Modify: `cmd/live-gateway/main.go`
- Modify: `cmd/live-gateway/main_test.go`
- Create: `internal/cliopt/path.go`
- Create: `internal/cliopt/path_test.go`
- Create: `internal/secretbuf/text.go`
- Create: `internal/secretbuf/text_test.go`

**Interfaces:**

- Consumes: `func config.DefaultLocalGateway() (config.Config, error)`, `func config.LoadUnresolved(string) (config.Config, error)`, `func requiresRuntimeRepository(config.Config) bool`, and the existing `func run(context.Context, []string, func(string) (string, bool)) error` composition root.
- Produces: exported zero-value `type cliopt.OptionalPath struct { ... }` implementing `flag.Value`, with `func (p *OptionalPath) Set(string) error`, `func (p *OptionalPath) String() string`, `func (p OptionalPath) Present() bool`, and `func (p OptionalPath) Path() string`; package-private `type gatewayConfigSource struct { Path string; Explicit bool }`; `func loadGatewayConfig(gatewayConfigSource) (config.Config, error)`; `const localGatewayLivenessAddress = "127.0.0.1:9090"`; a supervised loopback-only liveness server exposing only `GET`/`HEAD /health/live` and created only for `gatewayConfigSource{Explicit:false}`; `func secretbuf.LookupText(func(string) (string, bool), string, int) (string, error)`; `func resolveGatewayProviderCredential(string, func(string) (string, bool)) (string, error)`; and `func resolveGatewayDatabaseURL(config.Config, func(string) (string, bool)) (string, error)`. Only `gatewayConfigSource{Explicit:false}` selects the typed local default; an explicitly supplied empty or duplicate path is invalid. Retention, reconnect, and static-auth consumers use the same bounded lookup rule inside their existing constructors.

- [ ] Add RED loader and inactive-secret tests:
  - `TestLoadGatewayConfigUsesCredentialFreeDefaultWhenConfigFlagAbsent`
  - `TestGatewayExplicitEmptyConfigNeverDowngradesToLocal`
  - `TestGatewayStartupDoesNotResolveInactiveWorkerSecrets`
- [ ] Add RED local diagnostics tests:
  - `TestLocalGatewayLivenessIsLoopbackHealthOnly`
  - `TestLocalGatewayLivenessJoinsOnShutdown`
  - `TestExplicitConfigNeverStartsDefaultLiveness`
- [ ] Add `TestOptionalPathRejectsEmptyInvalidAndDuplicateConfigFlags` in `internal/cliopt`.
- [ ] Add RED strict credential tests:
  - `TestGatewayConfiguredVoiceCredentialRemainsStrict`
  - `TestGatewayConfiguredStaticBearerRemainsStrict`
  - `TestGatewayConfiguredDatabaseCredentialRemainsStrict`
- [ ] Add RED production and local-policy tests:
  - `TestGatewayProductionDatabaseURLRejectsEnvironmentTLSDisableBeforeDial`
  - `TestLocalDeploymentRegionDoesNotReadAllowedRegionPolicy`
- [ ] Make the lookup spy fail the test if the gateway asks for reasoner, privacy-HMAC, or tool-encryption environment names.
- [ ] Table-test every gateway-owned textual credential—speech API key, static bearer, and database URL—for missing, empty, whitespace, invalid UTF-8, control characters, and more than 16 KiB. Add equivalent encoded-size checks before decoding retention and reconnect keys. Assert only stable content-free error classes.
- [ ] Run the RED set and record the failures in the task report.

```sh
go test -count=1 ./cmd/live-gateway -run \
  '^(TestLoadGatewayConfigUsesCredentialFreeDefaultWhenConfigFlagAbsent|TestGatewayExplicitEmptyConfigNeverDowngradesToLocal|TestGatewayStartupDoesNotResolveInactiveWorkerSecrets|TestGatewayConfigured(Voice|StaticBearer|Database)CredentialRemainsStrict|TestGatewayProductionDatabaseURLRejectsEnvironmentTLSDisableBeforeDial|TestLocalDeploymentRegionDoesNotReadAllowedRegionPolicy|TestLocalGatewayLiveness.*|TestExplicitConfigNeverStartsDefaultLiveness)$'
go test -count=1 ./internal/cliopt -run '^TestOptionalPathRejectsEmptyInvalidAndDuplicateConfigFlags$'
```

- [ ] Add the private loader seam:

```go
type gatewayConfigSource struct {
    Path     string
    Explicit bool
}

func loadGatewayConfig(source gatewayConfigSource) (config.Config, error) {
    if !source.Explicit {
        return config.DefaultLocalGateway()
    }
    if source.Path == "" {
        return config.Config{}, errExplicitConfigPathRequired
    }
    return config.LoadUnresolved(source.Path)
}
```

- [ ] Implement `cliopt.OptionalPath` as the sole `-config`/`--config` parser used by gateway and worker. Its first `Set` rejects empty, invalid UTF-8, controls, and paths over 4 KiB; a second `Set` always rejects the duplicate; `String` never returns the configured path. Bind it with `flag.Var`, and derive `gatewayConfigSource.Explicit` only from `Present()`—never from the resulting string value or `FlagSet.Visit`.
- [ ] Reject `live-gateway -config=`, `live-gateway --config ""`, and duplicate config flags with one stable content-free error before environment lookup, listener creation, file access, or fallback to `DefaultLocalGateway`.
- [ ] Add the local liveness listener at exactly `127.0.0.1:9090` only when the parsed config source has `Explicit == false`. It is unauthenticated but exposes only `GET`/`HEAD /health/live` (`200`, GET body `ok\n`); mutation methods return `405`, all other paths return `404`, no admin/readiness/metrics routes are registered, and supervisor cancellation closes and joins it within the configured shutdown bound. Every explicit config—including explicit `mode: local`—retains its existing authenticated admin/health composition and never creates or double-binds this default listener.

- [ ] Add one owned bounded-text helper. It returns stable errors and never includes the value:

```go
func LookupText(
    lookup func(string) (string, bool),
    name string,
    maximumBytes int,
) (string, error)
```

- [ ] Delete `resolveGatewayConfigSecrets` and remove the broad gateway call to `Config.ResolveSecrets`. Keep `Config.ResolveSecrets` intact for compatibility, but do not call it from gateway composition.
- [ ] Resolve only speech, static-auth, database, retention, and reconnect values inside the component that consumes each value. Factor `resolveGatewayProviderCredential`, `resolveGatewayStaticBearer`, `resolveGatewayDatabaseURL`, `resolveGatewayRetentionKey`, and `resolveGatewayReconnectKey`; validate before any listener, dial, or constructor.
- [ ] Prove with a lookup spy that worker reasoner, privacy-HMAC, and tool-encryption names are never requested by the gateway.
- [ ] Use `requiresRuntimeRepository(cfg)` consistently in both admission and feature composition. Preserve support for `development_durable`.
- [ ] In production, reject a resolved database URL containing `sslmode=disable`; do not normalize it into a passing value.
- [ ] Move `LIVE_ALLOWED_REGIONS` lookup after the local/non-production return so local startup makes no policy lookup.
- [ ] Re-run focused and full gateway tests and require PASS.

```sh
go test -race -count=10 ./cmd/live-gateway ./internal/cliopt -run \
  'Test(LoadGatewayConfig|GatewayStartup|GatewayConfigured|GatewayProductionDatabase|LocalDeploymentRegion|LocalGatewayLiveness|ExplicitConfigNeverStartsDefaultLiveness|OptionalPath)'
go test -race -count=1 ./cmd/live-gateway
```

- [ ] Stage the six owned paths and commit the independently green task.

```sh
git add cmd/live-gateway/main.go cmd/live-gateway/main_test.go \
  internal/cliopt/path.go internal/cliopt/path_test.go \
  internal/secretbuf/text.go internal/secretbuf/text_test.go
git commit -m 'feat: start gateway without credentials'
```

---

## Task 3: Add the Dependency-Free Idle Worker Runtime

**Files:**

- Create: `cmd/live-worker/idle.go`
- Create: `cmd/live-worker/idle_test.go`

**Interfaces:**

- Consumes: the existing `type serviceRuntime interface { Run(context.Context) error; Close() }` contract from `cmd/live-worker/main.go`.
- Produces: `const idleWorkerListenAddress = "127.0.0.1:9091"`; `type idleRuntimeBuilder func(context.Context) (serviceRuntime, error)`, `func idleWorkerHandler() http.Handler`, `func newIdleRuntime(net.Listener) *idleRuntime`, and `func buildIdleRuntime(context.Context) (serviceRuntime, error)` for Task 4.

- [ ] Write RED tests for the exact HTTP contract:
  - `GET` and `HEAD /health/live` return `200` and `ok\n` for GET.
  - `GET` and `HEAD /health/ready` return a content-free `503`.
  - mutation methods return `405`.
  - every other path returns `404`.
  - shutdown closes the owned listener and is idempotent.
- [ ] Run `go test ./cmd/live-worker -run '^TestIdle'` and observe the missing symbols.
- [ ] Implement these seams around the existing `serviceRuntime` interface:

```go
type idleRuntimeBuilder func(context.Context) (serviceRuntime, error)

func idleWorkerHandler() http.Handler
func newIdleRuntime(net.Listener) *idleRuntime
func buildIdleRuntime(context.Context) (serviceRuntime, error)
```

- [ ] Bind exactly `127.0.0.1:9091`. Construct no config loader, provider, database, secret resolver, lease manager, or admin API.
- [ ] Bound server shutdown and preserve content-free errors.
- [ ] Run focused race tests and require PASS.

```sh
go test -race -count=25 ./cmd/live-worker -run '^TestIdle'
```

- [ ] Stage the two owned paths and commit the independently green task.

```sh
git add cmd/live-worker/idle.go cmd/live-worker/idle_test.go
git commit -m 'feat: add idle worker runtime'
```

---

## Task 4: Add Explicit Worker Backend Selection

**Files:**

- Modify: `cmd/live-worker/main.go`
- Modify: `cmd/live-worker/main_test.go`

**Interfaces:**

- Consumes: `type runtimeBuilder func(context.Context, config.Config, resolvedSecrets) (serviceRuntime, error)`, `type idleRuntimeBuilder func(context.Context) (serviceRuntime, error)`, Task 2's `cliopt.OptionalPath`, and `func secretbuf.LookupText(func(string) (string, bool), string, int) (string, error)`.
- Produces: package-private duplicate-aware `type backendFlag struct { ... }` implementing `flag.Value`; `func runWithBuilders(context.Context, []string, func(string) (string, bool), runtimeBuilder, idleRuntimeBuilder) error`; `--backend none` selects the idle builder, while `--backend durable -config PATH` and legacy `-config PATH` select the durable builder.

- [ ] Add the RED backend-selection tests:
  - `TestRunBackendNoneDoesNotReadConfigResolveSecretsOrBuildDurableRuntime`
  - `TestRunBackendNoneRejectsEveryExplicitConfigFlag`
  - `TestRunRejectsInvalidBackendFlagCombinationsBeforeConstruction`
  - `TestRunSelectsDurableBackendWithOrWithoutExplicitFlag`
- [ ] Add the RED bounded-secret test `TestResolveDurableWorkerSecretsAreBounded`.
- [ ] Preserve and update the RED compatibility test `TestRunRequiresConfig`.
- [ ] Cover `none + -config PATH`, `none + -config=`, `none + --config ""`, durable without config, unknown backend, duplicate backend/config flags, and positional arguments. Assert rejection before builders or lookups run.
- [ ] Table-test durable worker database URL, reasoner API key, privacy HMAC key, retention HMAC key, tool-encryption key, and admin bearer for missing, empty, whitespace/control, invalid UTF-8, and encoded/raw size violations. Reuse `secretbuf.LookupText`, then apply exact decoded-key constraints. Assert rejection before listener, database, or provider construction.
- [ ] Keep assertions on the test goroutine. The goroutine running the service reports through buffered channels; it must not call `t.Fatal`, `require.*`, or `assert.*`.
- [ ] Run the RED selector and require failures for the missing `runWithBuilders`/backend behavior before editing production code.

```sh
go test -count=1 ./cmd/live-worker -run \
  '^(TestRunBackendNone|TestRunRejectsInvalidBackend|TestRunSelectsDurable|TestRunRequiresConfig|TestResolveDurableWorkerSecretsAreBounded)'
```

- [ ] Replace the single-builder seam with:

```go
func runWithBuilders(
    ctx context.Context,
    args []string,
    lookup func(string) (string, bool),
    durableBuilder runtimeBuilder,
    idleBuilder idleRuntimeBuilder,
) error
```

- [ ] Implement CLI semantics:
  - `--backend none`: idle, no config, no lookups.
  - `--backend durable -config PATH`: strict durable.
  - `-config PATH` without backend: backward-compatible durable.
  - no arguments: fail with the existing durable-config error; only the gateway has an implicit local mode.
- [ ] Parse config with Task 2's duplicate-aware `cliopt.OptionalPath`; `--backend none` rejects every explicitly present config flag, including an empty one, before lookup or construction.
- [ ] Parse backend with the package-private one-shot `backendFlag`; its first value must be exactly `none` or `durable`, and a second `-backend`/`--backend` occurrence is rejected rather than last-value-wins.
- [ ] Prove cancellation joins the runtime and no listener survives.
- [ ] Run the focused and full worker package under the race detector and require PASS.

```sh
go test -race -count=25 ./cmd/live-worker -run \
  '^(TestRunBackendNone|TestRunRejectsInvalidBackend|TestRunSelectsDurable|TestRunRequiresConfig|TestResolveDurableWorkerSecretsAreBounded)'
go test -race -count=1 ./cmd/live-worker
```

- [ ] Stage the two owned paths and commit the independently green task.

```sh
git add cmd/live-worker/main.go cmd/live-worker/main_test.go
git commit -m 'feat: select idle or durable worker backend'
```

---

## Task 5: Make the Canonical Direct Browser Credential-Optional

**Files:**

- Modify: `demo/live-client/config.go`
- Modify: `demo/live-client/config_test.go`
- Modify: `demo/live-client/server.go`
- Modify: `demo/live-client/server_test.go`
- Modify: `Makefile`
- Modify: `deploy/scripts/demo-wizard.sh`
- Modify: `deploy/scripts/test-demo-wizard.sh`

**Interfaces:**

- Consumes: `func parseConfig([]string) (demoConfig, error)`, `func resolveBearer(demoConfig, func(string) (string, bool)) ([]byte, error)`, and `func newHandler(demoConfig, []byte, dialUpstream) (http.Handler, error)`.
- Produces: the same Go signatures with credential-optional loopback semantics; direct defaults are `ws://127.0.0.1:8080/v1/realtime`, model `local`, and empty `BearerEnv`, while wizard/operator callers explicitly pass `-bearer-env LIVE_DEMO_BEARER`.

- [ ] Add RED tests:
  - `TestParseConfigDefaultsToCredentialFreeGateway`
  - `TestResolveBearerSkipsLookupWhenUnconfigured`
  - `TestResolveBearerKeepsExplicitEnvironmentStrict`
  - `TestHandlerOmitsAuthorizationWithoutBearer`
  - preserve the existing authenticated `Authorization: Bearer ...` test.
- [ ] Run the RED selector and require the current authenticated defaults/header behavior to fail the new credential-optional assertions.

```sh
go test -count=1 ./demo/live-client -run \
  'Test(ParseConfigDefaults|ResolveBearer|HandlerOmitsAuthorization|Handler.*Authorization)'
```

- [ ] Set direct defaults to:

```text
GatewayURL = ws://127.0.0.1:8080/v1/realtime
Model      = local
BearerEnv  = ""
```

- [ ] Empty `BearerEnv` must perform zero environment lookups and return a nil/empty bearer as an intentional state.
- [ ] Explicit `-bearer-env NAME` remains strict, bounded, nonempty, valid UTF-8, and control-free.
- [ ] Allow `newHandler` to receive an empty bearer. In `server.go`'s upstream request construction, omit the `Authorization` header entirely rather than sending an empty bearer.
- [ ] Keep exact-origin, redirect rejection, URL validation, frame limits, no-store headers, protocol logs, profiles, tools, and `/setup` untouched.
- [ ] Make advanced wizard/operator launches pass `-bearer-env LIVE_DEMO_BEARER` explicitly so their durable authenticated contract does not silently change.
- [ ] Remove only the Makefile's unconditional `LIVE_DEMO_BEARER` prerequisite.
- [ ] Run Go, shell, and all browser tests and require PASS.

```sh
go test -race -count=10 ./demo/live-client -run \
  'Test(ParseConfigDefaults|ResolveBearer|HandlerOmitsAuthorization|Handler.*Authorization)'
./deploy/scripts/test-demo-wizard.sh
node --test demo/live-client/web/*.test.mjs
```

- [ ] Stage the seven owned paths and commit the independently green task.

```sh
git add demo/live-client/config.go demo/live-client/config_test.go \
  demo/live-client/server.go demo/live-client/server_test.go \
  Makefile deploy/scripts/demo-wizard.sh deploy/scripts/test-demo-wizard.sh
git commit -m 'feat: make browser quickstart credential optional'
```

---

## Task 6: Add Zero-Skip Runners and an Empty-Environment Process Smoke Gate

**Files:**

- Create: `deploy/scripts/test-credential-free-startup.sh`
- Create: `deploy/scripts/test-credential-free-startup-boundaries.sh`
- Create: `deploy/scripts/run-go-test-no-skip.sh`
- Create: `deploy/scripts/test-go-test-no-skip.sh`
- Create: `deploy/scripts/run-node-test-no-skip.mjs`
- Create: `deploy/scripts/test-node-test-no-skip.mjs`
- Modify: `Makefile`

**Interfaces:**

- Consumes: the Task 2 no-argument `live-gateway` CLI, Task 4 `live-worker --backend none`, and their loopback health/capabilities HTTP contracts.
- Produces: `run-go-test-no-skip.sh --expect-minimum N [--expect-top-level SORTED_NAMES] -- go test -json arguments`, where `N` is positive and optional `SORTED_NAMES` is a comma-separated exact set of distinct top-level test names; `run-node-test-no-skip.mjs --expect-tests N -- node --test files`, where `N` is positive; executable `deploy/scripts/test-credential-free-startup.sh`, executable `deploy/scripts/test-credential-free-startup-boundaries.sh`, and the phony `make test-credential-free-startup` release-gate input used by Task 7 and every later plan.

- [ ] Write RED Go-wrapper fixtures for pass, child failure, package skip, named-test skip, malformed/truncated JSON, zero tests, missing package terminal, sentinel leakage, malformed/duplicate top-level expectations, a missing expected top-level test, an unexpected top-level test, and subtests that must not satisfy a missing top-level expectation.
- [ ] Write RED Node-wrapper fixtures for pass, failure, root-count mismatch, skip/disabled annotation, truncated TAP, oversized output, and sentinel leakage.
- [ ] Implement the Go runner so every line is valid `go test -json`, every `Action:"skip"` fails, at least `--expect-minimum` named tests ran, every package terminated, and the child exited zero. When `--expect-top-level` is present, compare the exact distinct `Action:"run"` names without `/` against the supplied sorted set; subtests never count. Bound captured output and print only `go_test_summary tests=N packages=P skips=0`.
- [ ] Implement the Node runner with an 8 MiB capture bound, one complete root `1..N` plan, exactly `--expect-tests N`, zero `not ok`, skip, or disabled annotations, and a clean child exit. Print exactly `node_test_summary tests=N skips=0` and suppress child output.
- [ ] Run both wrapper boundary suites and require PASS before using either runner as evidence.

```sh
./deploy/scripts/test-go-test-no-skip.sh
node deploy/scripts/test-node-test-no-skip.mjs
```

- [ ] Write the boundary test first using fake gateway/worker binaries. Prove the harness rejects: early exit, non-loopback bind, nonempty worker readiness body, worker readiness `200`, failed SIGTERM join, and any unexpected environment dependency.
- [ ] Run the boundary test and require failure because the real smoke harness and Make target do not exist yet.

```sh
./deploy/scripts/test-credential-free-startup-boundaries.sh
```

- [ ] Implement a trap-safe smoke harness. Use `mktemp -d`; track exact child PIDs; send `TERM`; wait with a bounded deadline; never use broad `pkill` or a workspace-wide cleanup.
- [ ] Build real binaries and run each with a controlled environment containing only a known `PATH` and required macOS loader variables:

```sh
smoke_dir="$(mktemp -d /tmp/gpt-live-credential-free.XXXXXX)"
env GOCACHE=/tmp/gpt-live-production-cache \
  go build -o "$smoke_dir/live-gateway" ./cmd/live-gateway
env GOCACHE=/tmp/gpt-live-production-cache \
  go build -o "$smoke_dir/live-worker" ./cmd/live-worker
env -i PATH="$PATH" "$smoke_dir/live-gateway"
env -i PATH="$PATH" "$smoke_dir/live-worker" --backend none
```

- [ ] Have the real smoke own a listener on `127.0.0.1:8765`. Verify gateway capabilities on `http://127.0.0.1:8080/v1/realtime/capabilities`, gateway liveness on `http://127.0.0.1:9090/health/live`, worker liveness on `http://127.0.0.1:9091/health/live`, worker readiness as a content-free `503` on `http://127.0.0.1:9091/health/ready`, and zero accepted speech connections before a realtime session.
- [ ] Add `make test-credential-free-startup`; do not add Docker or an external provider to the default target.
- [ ] Run the boundary and real smoke and require PASS.

```sh
./deploy/scripts/test-credential-free-startup-boundaries.sh
make test-credential-free-startup
```

- [ ] Stage the seven owned paths and commit the independently green task.

```sh
git add deploy/scripts/test-credential-free-startup.sh \
  deploy/scripts/test-credential-free-startup-boundaries.sh \
  deploy/scripts/run-go-test-no-skip.sh deploy/scripts/test-go-test-no-skip.sh \
  deploy/scripts/run-node-test-no-skip.mjs deploy/scripts/test-node-test-no-skip.mjs \
  Makefile
git commit -m 'test: add credential-free verification foundations'
```

---

## Task 7: Integration Regression Gate

**Files:**

- Verify only: all paths changed by Tasks 1-6.
- Create: `docs/superpowers/reports/2026-08-28-production-backend-integration.md`

**Interfaces:**

- Consumes: `config.DefaultLocalGateway`, `runWithBuilders`, credential-optional browser composition, both zero-skip runners, and `make test-credential-free-startup` from Tasks 1-6.
- Produces: a committed implementation report containing the exact HEAD, commands, pass counts, zero-skip statement, and owned-path audit required before the runtime-hardening plan starts.

- [ ] Confirm no deployment manifest selects `--backend none`; Compose and Kubernetes continue to pass explicit durable configuration.
- [ ] Confirm the gateway never reads worker-only secrets by running with sentinel lookup callbacks under the race detector.
- [ ] Run the complete integration matrix:

```sh
./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 1 -- \
  go test -json -race -count=1 ./internal/config ./cmd/live-gateway ./cmd/live-worker ./demo/live-client
node deploy/scripts/run-node-test-no-skip.mjs --expect-tests 214 -- \
  node --test demo/live-client/web/*.test.mjs
./deploy/scripts/test-demo-wizard.sh
make test-credential-free-startup
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

- [ ] Record exact commands, pass counts, and any environmental limitation in the implementation report. A skipped test is not evidence.
- [ ] Resolve the unique plan-base commit by its exact subject, then audit the complete committed implementation range rather than the empty working-tree diff:

```sh
plan_base=$(git log --format=%H --grep='^docs: plan production-ready backend hardening$' -1)
printf '%s\n' "$plan_base" | rg -q '^[0-9a-f]{40}$'
test "$(git log --format=%H --grep='^docs: plan production-ready backend hardening$' | wc -l | tr -d '[:space:]')" -eq 1
git diff --name-only "$plan_base"..HEAD
```

- [ ] Confirm that range contains only Tasks 1–6 owned paths and record its exact base/head in the report.
- [ ] Stage and commit the report after every command passes on the same HEAD:

```sh
git add docs/superpowers/reports/2026-08-28-production-backend-integration.md
git commit -m 'docs: record production backend integration evidence'
```

## Plan Completion Criteria

- The gateway starts with no arguments and no credentials on loopback.
- `live-worker --backend none` starts without config, lookups, or dependencies and remains intentionally not ready for work.
- Explicit gateway and durable worker configuration stays strict and preserves database TLS validation.
- The canonical direct browser omits authentication only when the bearer option is intentionally absent; `/setup` and advanced durable paths remain explicit.
- The empty-environment smoke is checked in, deterministic, bounded, and part of the release-candidate verification inputs.
