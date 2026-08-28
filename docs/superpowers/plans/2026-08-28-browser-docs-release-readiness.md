# Browser, Documentation, and Release Readiness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the separate loopback browser and `/setup` tooling portable and diagnostically reliable, document the backend-only production boundary accurately, and make release evidence fail on skips or missing mandatory gates.

**Architecture:** The browser BFF remains an independent loopback development process and is excluded from production images/ingress. `/setup` launches only explicitly configured, revision-pinned local speech sources. Browser audio retains the canonical streaming resampler, playback generation fence, protocol-log split view, and handoff progress. Transport liveness uses bounded server-owned heartbeat rather than disconnecting quiet browsers. The release gate consumes exact, versioned, zero-skip evidence and distinguishes automated release-candidate evidence from operator-owned production promotion.

**Tech Stack:** Go 1.26 BFF and WebSocket transport, browser ES modules/AudioWorklet, Node test runner, Python speech launcher, shell boundary tests, release-gate evidence package, Markdown documentation tests.

**Spec:** `docs/superpowers/specs/2026-08-27-production-ready-backend-design.md`

## Global Constraints

- This plan runs after the integration, runtime, and deployment/security plans.
- The browser and `/setup` are development tooling only. Do not add them to production gateway/worker binaries, images, services, ingress, auth, or readiness.
- No checked-in active guide or launcher may contain `/Users/chuanmingliu`, another home path, a runtime clone/install, a mutable revision, or a plaintext secret.
- Browser-provided values never become executable paths or argv for the speech launcher.
- Preserve the canonical 24-kHz wire audio, streaming capture/playback resampling, response-generation fence, protocol-log bounds, and handoff progress semantics.
- Release scripts may not treat a skipped test, missing external authority, or unavailable service as passing evidence.
- External/manual gates are reported honestly as blockers; they are not automated away.

---

## Task 1: Make `/setup` Speech Launch Configuration Portable

**Files:**

- Modify: `demo/live-client/config.go`
- Modify: `demo/live-client/config_test.go`
- Modify: `demo/live-client/operator_runner.go`
- Modify: `demo/live-client/operator_runner_test.go`
- Modify: `deploy/scripts/operator-speech-launcher.py`
- Modify: `deploy/scripts/test-operator-speech-launcher.py`
- Modify: `deploy/scripts/demo-wizard.sh`
- Modify: `deploy/scripts/test-demo-wizard.sh`

**Interfaces:**

- Consumes: `func parseConfig(args []string) (demoConfig, error)`; `func newLocalOperatorRunner(cfg operatorConfig, factory commandFactory, probe readinessProbe) (operatorRunner, error)`; `func (runner *localOperatorRunner) startSpeech(ctx context.Context) error`.
- Produces: server-only flags `-operator-speech-root`, `-operator-speech-revision`, `-operator-speech-profile`, `-operator-speech-environment`, `-operator-speech-executable`, `-operator-speech-python`, and `-operator-speech-command`; launcher argv `python launcher --root ABSOLUTE_ROOT --revision LOWERCASE_40_HEX --profile ABSOLUTE_PROFILE --environment ABSOLUTE_ENVIRONMENT --executable ABSOLUTE_EXECUTABLE`; immutable Python `SpeechLaunchConfig(root: Path, revision: str, profile: Path, environment: Path, executable: Path)`.

- [ ] Add a RED Go parse test proving omitted speech trust fields reject operator mode.
- [ ] Add a RED Go runner test proving browser request/query/body values never alter launcher argv.
- [ ] Add a RED Python test proving a fully explicit trusted configuration succeeds.
- [ ] Add a RED Python test proving `/Users/...` or another implicit machine default is never consulted.
- [ ] Add a RED wizard test proving all seven server-owned speech flags are forwarded exactly once.
- [ ] Run the focused RED tests and confirm the current positional-profile launcher fails.
- [ ] Expand the server-owned operator configuration to this complete shape:

```go
type operatorConfig struct {
    Enabled           bool
    StateDir          string
    RepositoryRoot    string
    SpeechRoot        string
    SpeechRevision    string
    SpeechProfile     string
    SpeechEnvironment string
    SpeechExecutable  string
    SpeechPython      string
    SpeechCommand     string
    Readiness         map[operatorComponent]string
    ClientStart       func(context.Context) error
    ClientStop        func(context.Context) error
    clientSessions    *clientSessionSet
    supervisor        *operatorSupervisor
}
```

- [ ] Parse and validate all new server flags before constructing the operator runner.
- [ ] Make `startSpeech` pass the exact flag/value argv from the Interfaces block to the fixed interpreter and launcher.
- [ ] Parse those flags into one frozen Python `SpeechLaunchConfig`; remove every machine-specific global.
- [ ] Require an explicit absolute root and a lowercase 40-hex reviewed commit.
- [ ] Reject symlinks and group/world-writable components between the trust root and each reviewed file.
- [ ] Require the checkout and reviewed files to be owned by the invoking user; do not require ordinary user-owned parent directories to be root-owned or non-writable.
- [ ] Require regular executable/profile/environment files, repository `HEAD`, and reviewed file bytes to match the selected revision.
- [ ] Build a child environment from a closed allowlist and omit unrelated BFF, database, and provider secrets.
- [ ] Install `TERM`/`INT` handlers before unmasking signals, own the child process group, and bound cleanup.
- [ ] Add one static test forbidding clone, checkout, package-install, and download primitives in the launcher.
- [ ] Add one regression each for symlink swap, inode replacement, dirty reviewed file, untracked reviewed path, and revision mismatch.
- [ ] Add one regression each for immediate signal, a `TERM`-ignoring child, and path/secret leakage.
- [ ] Re-run Go, Python, and shell gates and require PASS.
- [ ] Commit the portable speech-launch slice.

```sh
go test -race -count=10 ./demo/live-client -run 'Test.*Speech|TestOperator'
python3 deploy/scripts/test-operator-speech-launcher.py
./deploy/scripts/test-demo-wizard.sh
git add demo/live-client/config.go demo/live-client/config_test.go \
  demo/live-client/operator_runner.go demo/live-client/operator_runner_test.go \
  deploy/scripts/operator-speech-launcher.py \
  deploy/scripts/test-operator-speech-launcher.py \
  deploy/scripts/demo-wizard.sh deploy/scripts/test-demo-wizard.sh
git commit -m 'fix: make setup speech launch portable'
```

---

## Task 2: Preserve Browser Audio, Barge-In, Logs, and Handoff UX

**Files:**

- Modify tests first: `demo/live-client/web/audio-core.test.mjs`
- Modify tests first: `demo/live-client/web/browser-runtime.test.mjs`
- Modify tests first: `demo/live-client/web/client-state.test.mjs`
- Modify tests first: `demo/live-client/web/protocol-state.test.mjs`
- Modify tests first: `demo/live-client/web/tool-state.test.mjs`
- Modify production only if a test exposes a regression:
  - `demo/live-client/web/audio-core.mjs`
  - `demo/live-client/web/capture-worklet.js`
  - `demo/live-client/web/playback-worklet.js`
  - `demo/live-client/web/browser-runtime.mjs`
  - `demo/live-client/web/client-state.mjs`
  - `demo/live-client/web/protocol-state.mjs`
  - `demo/live-client/web/tool-state.mjs`
  - `demo/live-client/web/app.js`
  - `demo/live-client/web/styles.css`

**Interfaces:**

- Consumes: `new StreamingPCM16Capture(inputRate, 24000, 960)` with `push(Float32Array) -> Uint8Array[]`; `new PCMPlaybackQueue(outputRate, maximumSeconds)` with `append(Int16Array, generation) -> boolean`, `pullInto(Float32Array) -> number`, and `clear(generation)`; `createBrowserRuntime(dependencies) -> {start, stop, mute, resumeAudio, saveToolPolicy, testToolPolicy}`; `ClientPresentation.apply(effect) -> boolean`; `ProtocolState.consume(event) -> object[]`; `ToolState.consume(event) -> {accepted: boolean, created: boolean, phaseChanged: boolean, overflow: boolean}`.
- Produces: no new browser-to-gateway protocol; exactly six new top-level Node characterization tests, taking the runtime plan's reviewed 218-test browser suite to exactly 224 tests; any implementation fix preserves the public functions above and the 24-kHz PCM16 wire contract.

- [ ] Add top-level test `hardware-rate capture remains continuous at 44.1 kHz and 48 kHz` with irregular chunk boundaries and exact 24-kHz frame accounting.
- [ ] Add top-level test `playback preserves pitch when AudioContext honors or ignores 24 kHz` with both 24-kHz and 48-kHz output contexts.
- [ ] Add top-level test `two turns survive barge-in generation fencing` that clears before user-speaking state, rejects stale append/ack, and accepts the next response.
- [ ] Add top-level test `live protocol and transcript panes remain independently bounded` covering scroll ownership, count/byte limits, and sanitization while audio is active.
- [ ] Add top-level test `handoff progress is monotonic accessible and voice-independent` covering `role=progressbar` and terminal stability.
- [ ] Add top-level test `session failure retains diagnostics and releases every browser resource` covering microphone tracks, worklets, timers, and socket ownership.
- [ ] Run the 224-test Node suite before any production edit and record which of the six new tests fail.
- [ ] If all six pass, leave production files unchanged.
- [ ] If any new test fails, make the smallest native fix without adding a browser-to-gateway protocol field.
- [ ] Re-run the complete 224-test Node suite and require 224 passes, zero failures, zero skips, and zero disabled tests.
- [ ] Run the BFF Go race suite and require PASS.
- [ ] Commit the six characterization tests and only the production files required by a failing regression.

```sh
node deploy/scripts/run-node-test-no-skip.mjs --expect-tests 224 -- \
  node --test demo/live-client/web/*.test.mjs
go test -race -count=10 ./demo/live-client
git add demo/live-client/web/audio-core.test.mjs \
  demo/live-client/web/browser-runtime.test.mjs demo/live-client/web/client-state.test.mjs \
  demo/live-client/web/protocol-state.test.mjs demo/live-client/web/tool-state.test.mjs \
  demo/live-client/web/audio-core.mjs demo/live-client/web/capture-worklet.js \
  demo/live-client/web/playback-worklet.js demo/live-client/web/browser-runtime.mjs \
  demo/live-client/web/client-state.mjs demo/live-client/web/protocol-state.mjs \
  demo/live-client/web/tool-state.mjs demo/live-client/web/app.js \
  demo/live-client/web/styles.css
git commit -m 'test: lock browser realtime diagnostics and audio'
```

---

## Task 3: Keep Quiet Browser Sessions Alive Safely

**Files:**

- Modify: `internal/transport/ws/handler.go`
- Modify: `internal/transport/ws/conn.go`
- Create: `internal/transport/ws/conn_test.go`
- Modify: `internal/transport/ws/handler_test.go`
- Modify: `demo/live-client/server_test.go`

**Interfaces:**

- Consumes: `clock.Clock` with `Now() time.Time` and `NewTicker(time.Duration) clock.Ticker`; `clock.Ticker` with `C() <-chan time.Time` and `Stop()`; `func normalizeOptions(opts Options) (Options, protocol.Limits, error)`; `func (c *clientConn) Read(ctx context.Context) (protocol.ClientEvent, error)`; `func (c *clientConn) Write(ctx context.Context, event protocol.ServerEvent) error`; `func (c *clientConn) Close(ctx context.Context, code int, reason string) error`; existing `writeGate chan struct{}` and `writeMu sync.Mutex`; `func (m *session.Manager) Run(parent context.Context, request session.OpenRequest, client session.ClientConn) error`; `func (m *session.Manager) Reconnect(parent context.Context, request session.ReconnectRequest, client session.ClientConn) error`; `handler.serveRealtime(http.ResponseWriter, *http.Request)`'s accepted `*websocket.Conn`, `sessionCtx`, `cancel`, and reconnect-control branch.
- Produces: `ws.Options.HeartbeatInterval time.Duration`, `ws.Options.PongTimeout time.Duration`, and `ws.Options.HeartbeatFailures int`; defaults `20*time.Second`, `2*time.Second`, and `3`; `handler.heartbeatClock clock.Clock`; `func newClientConn(conn *websocket.Conn, limits protocol.Limits, readTimeout, writeTimeout, heartbeatInterval, pongTimeout time.Duration, heartbeatFailures int, heartbeatClock clock.Clock) *clientConn`; `func heartbeatAttemptTimeout(pongTimeout, writeTimeout time.Duration) time.Duration`; `func (c *clientConn) Heartbeat(ctx context.Context) error`; `type sessionRunFunc func(context.Context) error`; `func runWithHeartbeat(ctx context.Context, cancel context.CancelFunc, client *clientConn, run sessionRunFunc) error`.

- [ ] Add the package-private `clock.Clock` field and inject `clock.Real{}` from `NewHandler`.
- [ ] Add a RED loopback test that advances beyond the old 30-second idle interval, then completes a valid turn on the same connection.
- [ ] Add a RED missing-pong test that reaches exactly `HeartbeatFailures` and observes the stable close reason.
- [ ] Add a RED blocked-peer test proving the pong timeout cancels the ping write.
- [ ] Add a RED concurrent-write test proving text and ping frames are serialized.
- [ ] Add a RED shutdown test proving the ticker stops and heartbeat returns.
- [ ] Add a RED late-pong test proving a pong after close cannot resurrect state or panic.
- [ ] Add RED option-normalization tests for defaults `HeartbeatInterval=20s`, `PongTimeout=2s`, and `HeartbeatFailures=3`.
- [ ] Add RED option-validation tests for interval outside `[5s,60s]`, pong timeout outside `[100ms,5s]`, pong timeout greater than or equal to interval, and failures outside `[1,5]`.
- [ ] Add a RED compatibility test proving zero heartbeat fields normalize successfully when an existing caller supplies a positive `WriteTimeout` shorter than the default pong timeout.
- [ ] Add a RED attempt-budget test proving every ping uses `min(PongTimeout, WriteTimeout)` without mutating either normalized option.
- [ ] Add a RED handler test proving exactly one heartbeat goroutine starts after WebSocket accept for both `Run` and `Reconnect` session branches.
- [ ] Add a RED handler test proving a normal manager/session return cancels and synchronously joins that heartbeat goroutine before `client.finish` and handler return.
- [ ] Add a RED handler test proving a terminal heartbeat error cancels the shared session context, unblocks the manager/session call, is joined, and yields only a stable content-free close reason.
- [ ] Run the focused RED suite and confirm quiet sessions no longer close on the application read deadline.
- [ ] Normalize zero values to interval `20s`, pong timeout `2s`, and failure threshold `3`.
- [ ] Validate interval in `[5s,60s]`, pong timeout in `[100ms,5s]` and strictly less than interval, and failure threshold in `[1,5]`.
- [ ] Do not reject options by comparing pong timeout with the independently configurable positive write timeout.
- [ ] Implement `heartbeatAttemptTimeout` as the smaller positive value of normalized `PongTimeout` and `WriteTimeout`, and derive a fresh ping context with that budget on every attempt.
- [ ] Implement `Heartbeat` with one `clock.Ticker` and `websocket.Conn.Ping(pongCtx)`.
- [ ] Acquire the existing `writeGate` and `writeMu` around each ping so text, ping, and close never write concurrently.
- [ ] Count consecutive bounded ping failures and close only at `HeartbeatFailures` with a content-free reason.
- [ ] Stop the ticker and return on context cancellation or connection close.
- [ ] Implement `runWithHeartbeat` so it starts exactly one heartbeat goroutine with a one-result buffered channel.
- [ ] Run the reconnect-control plus manager/session closure synchronously on the same context.
- [ ] Make a terminal heartbeat error cancel that shared context with a stable content-free transport error.
- [ ] On every return path, call `cancel` and receive the heartbeat result before `runWithHeartbeat` returns.
- [ ] Call `runWithHeartbeat` immediately after accept/client construction around both `Run` and `Reconnect`.
- [ ] Preserve the manager/session error when it terminates first; otherwise normalize the heartbeat failure to one stable content-free transport error.
- [ ] Keep heartbeat ownership entirely inside `serveRealtime`: no detached goroutine may outlive manager return, lease-monitor join, `client.finish`, or handler return.
- [ ] Make steady-state `clientConn.Read(ctx)` call `c.conn.Read(ctx)` without a per-idle deadline.
- [ ] Retain `ReadTimeout` only for the bounded initial `readReconnectControl` exchange; muting the microphone is not a transport failure.
- [ ] Re-run the focused loopback race tests and require PASS.
- [ ] Re-run the BFF quiet-WebSocket tests and require PASS.
- [ ] Commit the bounded heartbeat slice.

```sh
go test -race -count=50 ./internal/transport/ws -run 'Test.*(Heartbeat|Quiet|Pong|Idle)'
go test -race -count=10 ./demo/live-client -run 'Test.*Quiet.*WebSocket'
git add internal/transport/ws/handler.go internal/transport/ws/conn.go \
  internal/transport/ws/conn_test.go internal/transport/ws/handler_test.go \
  demo/live-client/server_test.go
git commit -m 'fix: keep quiet realtime clients connected'
```

---

## Task 4: Prove Browser Tooling Is Absent from Production Artifacts

**Files:**

- Modify: `deploy/validate/main.go`
- Modify: `deploy/validate/main_test.go`
- Modify: `deploy/scripts/test-validate-deploy.sh`
- Modify: `Dockerfile` only if a test exposes inclusion
- Modify: `deploy/scripts/verify-deploy-static.sh`

**Interfaces:**

- Consumes: `type document = map[string]any`; `func validate(docs []document, overlay, readiness string) error`; root `Dockerfile` targets `gateway`, `worker`, `migrate`, and `load`; rendered production Service/Ingress/Deployment resources.
- Produces: validator mutations `production-browser-assets`, `production-setup-route`, `production-demo-port`, `production-demo-bearer`, and `production-operator-route`; static rule that production images, listeners, Services, and Ingresses contain none of the corresponding development surfaces.

- [ ] Add the `production-browser-assets` RED mutation to a production image context.
- [ ] Add the `production-setup-route` RED mutation to production Ingress.
- [ ] Add the `production-demo-port` RED mutation to the gateway Deployment and Service.
- [ ] Add the `production-demo-bearer` RED mutation to the production gateway environment.
- [ ] Add the `production-operator-route` RED mutation to production Ingress.
- [ ] Run the focused RED validator suite and confirm each mutation currently escapes at least one boundary.
- [ ] Add a static dependency/image-context check excluding `demo/live-client/web` and operator scripts from production targets.
- [ ] Add a rendered-resource check excluding the demo listener, `/setup`, and operator endpoints from production.
- [ ] Scope the new rules to production resources so development Compose and operator artifacts remain valid.
- [ ] Scope text scans to active production inputs; do not inspect archived specs or plans.
- [ ] Re-run the focused validator suite and require PASS.
- [ ] Run the complete static deployment verifier and require PASS.
- [ ] Commit the production-artifact separation guard.

```sh
go test -race -count=10 ./deploy/validate -run 'Test.*(Browser|Setup|ProductionArtifact)'
./deploy/scripts/test-validate-deploy.sh
make verify-deploy-static
git add deploy/validate/main.go deploy/validate/main_test.go \
  deploy/scripts/verify-deploy-static.sh deploy/scripts/test-validate-deploy.sh Dockerfile
git commit -m 'test: exclude browser tooling from production artifacts'
```

---

## Task 5: Rewrite the Active Quickstart and Operations Documentation

**Files:**

- Modify: `README.md`
- Modify: `demo/live-client/README.md`
- Modify: `deploy/README.md`
- Modify: `docs/protocol.md`
- Modify: `docs/security.md`
- Modify: `docs/operations.md`
- Modify: `docs/testing.md`
- Modify: `docs/limitations.md`
- Modify: `docs/demo/realtime-browser-client.md`
- Modify: `skills/gpt-live-go-framework/SKILL.md`
- Modify: `skills/gpt-live-go-framework/references/architecture.md`
- Modify: `skills/gpt-live-go-framework/references/configuration.md`
- Modify: `skills/gpt-live-go-framework/references/operations.md`
- Modify: `skills/gpt-live-go-framework/references/verification.md`
- Modify: `demo/live-client/docs_test.go`
- Modify: `deploy/scripts/verify-deploy-static.sh`
- Modify: `go.mod`
- Modify: `go.sum`

**Interfaces:**

- Consumes: Task 1's seven portable speech flags; Task 3's heartbeat settings; deployment-plan secret inventories, egress policy, speech circuit, projected token, and `failure.Class`; active-guide path list in `demo/live-client/docs_test.go`.
- Produces: `func activeGuidePaths() []string`; `func TestActiveGuidesContainNoMachinePathsOrRetiredInstructions(t *testing.T)`; Go tool directive `tool golang.org/x/vuln/cmd/govulncheck` pinned by `require golang.org/x/vuln v1.1.4`; documented command `go tool govulncheck ./...`.

- [ ] Add a RED table test rejecting `/Users/` and other home-directory examples in every active guide.
- [ ] Add a RED table test rejecting retired model names and dynamic package installers in every active guide.
- [ ] Add a RED table test rejecting plaintext secret examples and obsolete broad gateway-secret claims.
- [ ] Add a RED assertion rejecting the false claim that no browser exists anywhere in the repository.
- [ ] Run the documentation test and confirm the current guides fail.
- [ ] Rewrite the root README opening to state that production deployables are the backend gateway and optional worker.
- [ ] Add the separate loopback browser and `/setup` development-tooling statement immediately after that boundary.
- [ ] Add credential-free process-start requirements and these exact three-terminal commands:

```sh
go run ./cmd/live-gateway
go run ./cmd/live-worker --backend none
go run ./demo/live-client  # optional development tooling
```

- [ ] Add exact loopback diagnostics after the three-terminal quickstart: capabilities at `http://127.0.0.1:8080/v1/realtime/capabilities`, gateway liveness at `http://127.0.0.1:9090/health/live`, idle-worker liveness at `http://127.0.0.1:9091/health/live`, and the expected content-free `503` at `http://127.0.0.1:9091/health/ready`; then document bounded Ctrl-C stop commands.
- [ ] Add optional external speech configuration and explicit durable/production configuration.
- [ ] Add the warning that loopback defaults must never be exposed.
- [ ] State that inference requires a compatible speech service and durable work requires PostgreSQL plus a configured reasoner.
- [ ] Document provider-write deadlines and provider-VAD/manual authority.
- [ ] Document heartbeat and profile-owned prompts/tools.
- [ ] Document stable public failure classes and production proxy routing.
- [ ] Document speech-readiness hysteresis, projected preStop identity, and exact secret inventories.
- [ ] Document release-candidate versus production-promotion evidence and each external blocker.
- [ ] Update the tracked framework skill and four references to the same contracts.
- [ ] Add `require golang.org/x/vuln v1.1.4` and `tool golang.org/x/vuln/cmd/govulncheck`, then run `go mod tidy` to update `go.sum`.
- [ ] Re-run the documentation test and require PASS.
- [ ] Run the active-guide forbidden-text command and require no match.
- [ ] Run static deployment verification and require PASS.
- [ ] Commit the documentation and pinned vulnerability-tool slice.

```sh
go test -count=1 ./demo/live-client -run 'Test.*Doc'
make verify-deploy-static
if rg -n '/Users/|deepseek-chat|npx|npm install|pip install' \
  README.md demo/live-client/README.md deploy/README.md docs/protocol.md \
  docs/security.md docs/operations.md docs/testing.md docs/limitations.md \
  docs/demo/realtime-browser-client.md skills/gpt-live-go-framework; then exit 1; fi
git add README.md demo/live-client/README.md deploy/README.md docs/protocol.md \
  docs/security.md docs/operations.md docs/testing.md docs/limitations.md \
  docs/demo/realtime-browser-client.md skills/gpt-live-go-framework/SKILL.md \
  skills/gpt-live-go-framework/references/architecture.md \
  skills/gpt-live-go-framework/references/configuration.md \
  skills/gpt-live-go-framework/references/operations.md \
  skills/gpt-live-go-framework/references/verification.md \
  demo/live-client/docs_test.go deploy/scripts/verify-deploy-static.sh go.mod go.sum
git commit -m 'docs: publish portable backend quickstart'
```

---

## Task 6: Fail Release Evidence on Skips or Missing Gates

**Files:**

- Modify: `Makefile`
- Modify: `.gitignore`
- Modify: `docs/operations.md`
- Modify: `demo/live-client/docs_test.go`
- Modify: `deploy/scripts/run-go-test-no-skip.sh`
- Modify: `deploy/scripts/test-go-test-no-skip.sh`
- Modify: `deploy/scripts/run-load-evidence.sh`
- Create: `deploy/scripts/test-run-load-evidence.sh`
- Modify: `deploy/scripts/run-node-test-no-skip.mjs`
- Modify: `deploy/scripts/test-node-test-no-skip.mjs`
- Modify: `deploy/scripts/run-release-gate.sh`
- Create: `deploy/scripts/run-release-phase-one.sh`
- Create: `deploy/scripts/finalize-release-archive.sh`
- Create: `deploy/scripts/verify-release-archive.sh`
- Modify: `deploy/scripts/test-release-gate-boundaries.sh`
- Modify: `internal/releasegate/evidence.go`
- Create: `internal/releasegate/evidence_test.go`
- Create: `internal/releasegate/promotion.go`
- Create: `internal/releasegate/promotion_test.go`
- Modify: `internal/releasegate/check.go`
- Modify: `internal/releasegate/check_test.go`
- Modify: `cmd/live-release-gate/main.go`
- Modify: `cmd/live-release-gate/main_test.go`

**Interfaces:**

- Consumes: integration Task 6's `run-go-test-no-skip.sh --expect-minimum N [--expect-top-level SORTED_NAMES] -- go test -json arguments` and Node zero-skip runner as extended by deployment Task 5; Task 2's exact 224-test browser suite; `func releasegate.ExpectedCommand(gateID string) ([]string, bool)`; `releasegate.ExpectedProducers map[string]string`; `func releasegate.VerifyUnsignedArchive(document []byte, options releasegate.VerifyOptions) (releasegate.Evidence, error)`; `func releasegate.FinalizeVerifiedArchive(writer io.Writer, unsignedDocument []byte, attestation releasegate.SignedAttestation, options releasegate.VerifyOptions) (releasegate.Evidence, error)`; `func releasegate.VerifyArchive(document []byte, options releasegate.VerifyOptions) (releasegate.Evidence, error)`.
- Produces: `run-node-test-no-skip.mjs --expect-tests N -- node --test files`, where `N` is a positive base-10 integer; a `run-load-evidence.sh` in which every Go invocation is delegated to `run-go-test-no-skip.sh` with `-json` and a positive expected count; twelve exact `release-producer-v2/*` identities and mirrored command vectors in `run-release-gate.sh` and `internal/releasegate/evidence.go`; `run-release-phase-one.sh --commit SHA --config-hash HASH --evidence-out ABSOLUTE_EVIDENCE --unsigned-archive-out ABSOLUTE_ARCHIVE --receipt-out ABSOLUTE_RECEIPT`; `finalize-release-archive.sh --unsigned-archive ABSOLUTE_ARCHIVE --attestation ABSOLUTE_ATTESTATION --attestation-key release-operator=ABSOLUTE_PUBLIC_KEY --archive-out ABSOLUTE_ARCHIVE --receipt-out ABSOLUTE_RECEIPT --commit SHA --config-hash HASH`; `verify-release-archive.sh --archive ABSOLUTE_ARCHIVE --attestation-key release-operator=ABSOLUTE_PUBLIC_KEY --commit SHA --config-hash HASH`; exact `PhaseOneReceipt`, `ArchiveVerificationReceipt`, `PromotionCondition`, `PromotionRecord`, `PromotionAttestation`, and `PromotionEvidence` types shown below; `func DecodePhaseOneReceipt(document []byte) (PhaseOneReceipt, error)`; `func DecodeArchiveVerificationReceipt(document []byte) (ArchiveVerificationReceipt, error)`; `func UnsignedEvidenceSHA256(evidence Evidence) (string, error)`; `func DecodePromotionRecord(document []byte) (PromotionRecord, error)`; `func DecodePromotionAttestation(document []byte) (PromotionAttestation, error)`; `func DecodePromotionEvidence(document []byte) (PromotionEvidence, error)`; `func PromotionStatementSHA256(record PromotionRecord) (string, error)`; `func VerifyPromotionEvidence(evidence PromotionEvidence, expectedCommit, expectedConfigHash, expectedReleaseEvidenceSHA256 string, keys map[string]ed25519.PublicKey, now time.Time) error`; CLI subcommands `verify-unsigned-archive`, `promotion-record`, `promotion-statement`, `promotion-collect`, and `promotion-verify`, where verified pass exits 0, verified blocked exits 3, and invalid input exits 1.

- [ ] Extend the Go-wrapper fixture with pass, child failure, package skip, test skip, malformed JSON, truncated stream, zero tests, sentinel leakage, and an exact-top-level case proving subtests cannot substitute for a missing required top-level test.
- [ ] Require every input line to be valid `go test -json`, reject any `Action:"skip"`, require at least `--expect-minimum` named test runs, and require complete package terminals plus a clean child exit.
- [ ] Keep Go-wrapper stdout exactly `go_test_summary tests=N packages=P skips=0`, where `N` and `P` are base-10 counts, and suppress child output.
- [ ] Preserve and extend the integration-plan Node TAP wrapper with its maximum 8 MiB child-output budget; do not introduce a second runner.
- [ ] Require the Node wrapper's exact `--expect-tests` count, one complete root `1..N` plan, zero `not ok`, zero skip directives, zero disabled-test annotations, and a clean child exit.
- [ ] Add one Node-wrapper fixture each for nested subtests, truncated TAP, skip, disabled-test annotation, count mismatch, failure, oversized output, and sentinel leakage.
- [ ] Pin the browser producer command to exactly:

```sh
node deploy/scripts/run-node-test-no-skip.mjs --expect-tests 224 -- \
  node --test demo/live-client/web/*.test.mjs
```

- [ ] Replace each integration and contract `go test` producer with the Go JSON wrapper and an explicit nonempty prerequisite check.
- [ ] Keep the wrapper's “reject every JSON skip action” rule strict. For a repository-wide unit/race run, first require a successful standalone `go list -f '{{if or .TestGoFiles .XTestGoFiles}}{{.ImportPath}}{{end}}' ./...`, then validate its newline-separated test-bearing package set is nonempty, pass only that set to the wrapper, and keep a separate `go build ./...` for every no-test package. Do not pipe `go list` into a command that could hide its exit status. Add boundary fixtures proving: a no-test package is built but cannot inject a package-level skip into evidence; a real test skip still fails; and a fake `go list` that emits a partial package list before exiting nonzero cannot launch the test child or produce passing evidence.
- [ ] Route every Go test launched by `run-load-evidence.sh` through the same JSON wrapper; `test-run-load-evidence.sh` must prove a skipped child, zero selected tests, malformed JSON, and wrapper bypass all fail without a passing artifact.
- [ ] Make the formal barge-in load artifact run these exact three tests together with `--expect-minimum 3 --expect-top-level TestBargeInAcknowledgedFenceSuppressesEveryStaleFrame,TestContinuousBargeWithProfileRenderingPreservesEpochAndProgress,TestRealtimePressureBlockedWriteOmittedTerminalRecovery`: the acknowledged fence, continuous profile render, and combined pressure/recovery tests. This exact top-level set and selector must be the final post-adapter-change producer, not an earlier unit-only gate.
- [ ] Keep each manifest `selector` as the exact child `go test -json ...` command, but execute it only through the zero-skip wrapper; use the exact top-level assertion above for `barge-in-20.json` and `--expect-minimum 1` for each single-test unit/capacity/chaos/soak artifact. Apply the same rule to smoke mode. Update `internal/releasegate/check.go`'s `formal_barge_20` selector to the exact three-test expression and assert the formal manifest rejects the former two-test selector.
- [ ] Pin `barge-in-20.json` to this exact child command in the script, manifest, release checker, and boundary fixture:

```sh
go test -json -count=1 -tags=load ./test/load/... \
  -run '^(TestContinuousBargeWithProfileRenderingPreservesEpochAndProgress|TestBargeInAcknowledgedFenceSuppressesEveryStaleFrame|TestRealtimePressureBlockedWriteOmittedTerminalRecovery)$' \
  -timeout=2m
```

- [ ] Update `run-release-gate.sh` command vectors first.
- [ ] Mirror those byte-for-byte command vectors in `internal/releasegate/evidence.go` `expectedCommands`.
- [ ] Use these exact v2 producer commands; keep them as single shell strings in both mirrored locations:

```sh
# release-producer-v2/simulator-voice-contract
sh -c 'test -n "${S2S_REALTIME_URL:-}" && S2S_REALTIME_URL="$S2S_REALTIME_URL" ./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 10 --expect-top-level TestContractPartialAdmissionFailureDrainsEverySuccessfulSession,TestDialReadyPostHandshakeCloseIsNeverSkippable,TestReadinessRetriesRememberSuccessfulHandshakeAcrossServiceLoss,TestReadinessUnavailableBeforeAnyHandshakeRemainsSkippable,TestRealtimeImmediateCreateCancelThenNextTurnContract,TestS2SConnectorForwardsPCMBytesExactly,TestSimulatorDeclaredConcurrency,TestSimulatorFunctionCallOneActiveResponseAndCancellation,TestSimulatorSessionUpdateAndPCM24AppendCommitAcceptance,TestSimulatorVADImplicitResponseBindsEpochBeforeAudioAndCompletesOnce -- go test -json -count=1 -tags=contract ./test/contract/...'

# release-producer-v2/durable-acceptance-integration
sh -c 'test -n "${TEST_DATABASE_URL:-}" && TEST_DATABASE_URL="$TEST_DATABASE_URL" ./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 1 -- go test -json -count=1 -tags=integration ./test/integration -run "^TestVoiceContinuesDuringDurableHandoff$"'

# release-producer-v2/correlation-integration
sh -c 'test -n "${TEST_DATABASE_URL:-}" && TEST_DATABASE_URL="$TEST_DATABASE_URL" ./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 2 --expect-top-level TestDuplicateCompletionHasOneExecutorEffectAndOneClientTerminal,TestTwoLiveWebSocketsKeepPCMEventsAndResponseEpochsIsolated -- go test -json -count=1 -tags=integration ./test/integration -run "^(TestTwoLiveWebSocketsKeepPCMEventsAndResponseEpochsIsolated|TestDuplicateCompletionHasOneExecutorEffectAndOneClientTerminal)$"'

# release-producer-v2/presentation-integration
sh -c 'test -n "${TEST_DATABASE_URL:-}" && TEST_DATABASE_URL="$TEST_DATABASE_URL" ./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 3 --expect-top-level TestDroppedListenerNotificationRepairsThroughPollAndCursor,TestUnsafeWorkerResultSuppressesDurableClientAndProviderPresentation,TestWorkerCompletionDuringSpeechRemainsVisualOnly -- go test -json -count=1 -tags=integration ./test/integration -run "^(TestDroppedListenerNotificationRepairsThroughPollAndCursor|TestUnsafeWorkerResultSuppressesDurableClientAndProviderPresentation|TestWorkerCompletionDuringSpeechRemainsVisualOnly)$"'

# release-producer-v2/steer-cancel-integration
sh -c 'test -n "${TEST_DATABASE_URL:-}" && TEST_DATABASE_URL="$TEST_DATABASE_URL" ./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 3 --expect-top-level TestPostgresLeaseExpiryFencesOldOwner,TestPostgresServicePersistsSteeringAndCancelRequest,TestProviderSteerAndCancelTraverseDurableWorkerLifecycle -- go test -json -count=1 -tags=integration ./test/integration -run "^(TestProviderSteerAndCancelTraverseDurableWorkerLifecycle|TestPostgresServicePersistsSteeringAndCancelRequest|TestPostgresLeaseExpiryFencesOldOwner)$"'

# release-producer-v2/epoch-fencing-race
sh -c './deploy/scripts/run-go-test-no-skip.sh --expect-minimum 3 --expect-top-level TestEpochFenceMonotonicallyAdvancesAndAcceptsOnlyCurrent,TestOldEpochProviderErrorCannotTerminateCurrentResponse,TestResponseOutputRequiresCurrentEpochAndIncarnation -- go test -json -race -count=1 ./internal/session/... -run "^(TestEpochFenceMonotonicallyAdvancesAndAcceptsOnlyCurrent|TestResponseOutputRequiresCurrentEpochAndIncarnation|TestOldEpochProviderErrorCannotTerminateCurrentResponse)$"'

# release-producer-v2/reconnect-integration
sh -c 'test -n "${TEST_DATABASE_URL:-}" && TEST_DATABASE_URL="$TEST_DATABASE_URL" ./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 1 -- go test -json -count=1 -tags=integration ./test/integration -run "^TestReconnectDisconnectsProviderWithQueuedAudioAndFencesStaleOutput$"'

# release-producer-v2/restart-durability-integration
sh -c 'test -n "${TEST_DATABASE_URL:-}" && TEST_DATABASE_URL="$TEST_DATABASE_URL" ./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 2 --expect-top-level TestPostgresDeliveryCursorReadsDurableSuffixInOrder,TestRunnerCancellationRestartRecoversExpiredLease -- go test -json -count=1 -tags=integration ./test/integration -run "^(TestRunnerCancellationRestartRecoversExpiredLease|TestPostgresDeliveryCursorReadsDurableSuffixInOrder)$"'

# release-producer-v2/verification-suites
sh -c 'test_packages=$(go list -f "{{if or .TestGoFiles .XTestGoFiles}}{{.ImportPath}}{{end}}" ./...) && test -n "$test_packages" && go vet ./... && ./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 1 -- go test -json -race -count=1 $test_packages && go build ./... && go mod tidy -diff && make test-credential-free-startup && ./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 1 -- go test -json -race -count=1 ./demo/live-client && node deploy/scripts/run-node-test-no-skip.mjs --expect-tests 224 -- node --test demo/live-client/web/*.test.mjs && python3 deploy/scripts/test-operator-speech-launcher.py && ./deploy/scripts/test-demo-wizard.sh && make verify-deploy-static && test -n "${TEST_DATABASE_URL:-}" && TEST_DATABASE_URL="$TEST_DATABASE_URL" ./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 1 -- go test -json -race -count=1 -tags=integration ./migrations ./internal/store/postgres ./internal/admission ./internal/feature ./internal/gateway ./internal/worker ./cmd/live-gateway ./cmd/live-worker ./test/integration/... && test -n "${S2S_REALTIME_URL:-}" && S2S_REALTIME_URL="$S2S_REALTIME_URL" ./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 10 --expect-top-level TestContractPartialAdmissionFailureDrainsEverySuccessfulSession,TestDialReadyPostHandshakeCloseIsNeverSkippable,TestReadinessRetriesRememberSuccessfulHandshakeAcrossServiceLoss,TestReadinessUnavailableBeforeAnyHandshakeRemainsSkippable,TestRealtimeImmediateCreateCancelThenNextTurnContract,TestS2SConnectorForwardsPCMBytesExactly,TestSimulatorDeclaredConcurrency,TestSimulatorFunctionCallOneActiveResponseAndCancellation,TestSimulatorSessionUpdateAndPCM24AppendCommitAcceptance,TestSimulatorVADImplicitResponseBindsEpochBeforeAudioAndCompletesOnce -- go test -json -count=1 -tags=contract ./test/contract/... && go tool govulncheck ./...'

# release-producer-v2/formal-slo-load
sh -c 'test -n "${LIVE_EVIDENCE_DIR:-}" && ./deploy/scripts/run-load-evidence.sh formal'

# release-producer-v2/privacy-scan
sh -c './deploy/scripts/run-go-test-no-skip.sh --expect-minimum 1 -- go test -json -count=1 ./internal/safety ./internal/telemetry ./internal/reasoner/... && test -n "${TEST_DATABASE_URL:-}" && TEST_DATABASE_URL="$TEST_DATABASE_URL" ./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 2 --expect-top-level TestMaliciousProviderErrorNeverPersistsOrPresents,TestScheduleRetryPersistsOnlyFailureClass -- go test -json -race -count=1 -tags=integration ./internal/store/postgres ./test/integration -run "^(TestScheduleRetryPersistsOnlyFailureClass|TestMaliciousProviderErrorNeverPersistsOrPresents)$"'

# release-producer-v2/deployment-drain — executed only inside Task 7's formal boundary
sh -c './deploy/scripts/run-go-test-no-skip.sh --expect-minimum 1 -- go test -json -race -count=1 ./internal/admin ./internal/lifecycle ./deploy/prestop ./deploy/validate ./internal/netroute ./internal/health && make verify-deploy-static && make verify-deploy && make smoke-deploy'
```

- [ ] Bump `simulator-voice` to `release-producer-v2/simulator-voice-contract`.
- [ ] Bump `durable-acceptance` to `release-producer-v2/durable-acceptance-integration`.
- [ ] Bump `correlation` to `release-producer-v2/correlation-integration`.
- [ ] Bump `presentation` to `release-producer-v2/presentation-integration`.
- [ ] Bump `steer-cancel` to `release-producer-v2/steer-cancel-integration`.
- [ ] Bump `epoch-fencing` to `release-producer-v2/epoch-fencing-race`.
- [ ] Bump `reconnect` to `release-producer-v2/reconnect-integration`.
- [ ] Bump `restart-durability` to `release-producer-v2/restart-durability-integration`.
- [ ] Bump `verification-suites` to `release-producer-v2/verification-suites`.
- [ ] Bump `slo-load` to `release-producer-v2/formal-slo-load`.
- [ ] Bump `privacy-no-audio-persistence` to `release-producer-v2/privacy-scan`.
- [ ] Bump `deployment-drain` to `release-producer-v2/deployment-drain`.
- [ ] Add race, vet, build, tidy-diff, static deployment, and credential-free startup to verification evidence.
- [ ] Add zero-skip integration/contract and browser Go/Node/BFF to verification evidence.
- [ ] Add `go tool govulncheck ./...` to verification evidence.
- [ ] Add the malicious-provider PostgreSQL persistence test to privacy evidence.
- [ ] Add projected-token rotation, exact routing, speech circuit, static Compose/render validation, and Kubernetes schema validation to deployment/drain evidence.
- [ ] Keep live Compose shutdown and disposable-cluster execution outside automatic unit gates; they enter only Task 7's explicit formal boundary.
- [ ] Add sentinel tests proving evidence contains no command output, provider text, paths, tokens, prompts, transcript, tool arguments, or database URLs.
- [ ] Add the exact content-free archive-flow receipts:

```go
const PhaseOneReceiptSchemaVersion = "release-phase-one-v1"
const ArchiveVerificationReceiptSchemaVersion = "release-archive-verification-v1"

type PhaseOneReceipt struct {
    SchemaVersion         string    `json:"schema_version"`
    Commit                string    `json:"commit"`
    ConfigHash            string    `json:"config_hash"`
    StatementSHA256       string    `json:"statement_sha256"`
    SignedAt              time.Time `json:"signed_at"`
    EvidenceSHA256        string    `json:"evidence_sha256"`
    UnsignedArchiveSHA256 string    `json:"unsigned_archive_sha256"`
}

type ArchiveVerificationReceipt struct {
    SchemaVersion                  string    `json:"schema_version"`
    Commit                         string    `json:"commit"`
    ConfigHash                     string    `json:"config_hash"`
    AttestedUnsignedEvidenceSHA256 string    `json:"attested_unsigned_evidence_sha256"`
    SignedEvidenceSHA256           string    `json:"signed_evidence_sha256"`
    SignedArchiveSHA256            string    `json:"signed_archive_sha256"`
    VerifiedAt                     time.Time `json:"verified_at"`
}
```

- [ ] Implement `func DecodePhaseOneReceipt(document []byte) (PhaseOneReceipt, error)` and `func DecodeArchiveVerificationReceipt(document []byte) (ArchiveVerificationReceipt, error)` with strict canonical JSON and `MaximumAttestationBytes`.
- [ ] Implement `func UnsignedEvidenceSHA256(evidence Evidence) (string, error)` by cloning `Evidence`, setting `Attestations = nil` to reproduce phase one's canonical JSON `null`, encoding canonical JSON, and hashing those exact bytes.
- [ ] Add `verify-unsigned-archive`; after full archive verification, print exactly `release_unsigned_archive_verified evidence_sha256=HEX` and nothing else on stdout.
- [ ] Change `verify-archive` success stdout to exactly `release_archive_verified attested_unsigned_evidence_sha256=HEX signed_evidence_sha256=HEX`.
- [ ] Make invalid unsigned or signed archives exit 1 and never print either `*_verified` prefix.
- [ ] Make `run-release-gate.sh` exit reserved status 42 only after every producer succeeds, root `evidence.json` is canonical, and the exclusive unsigned archive is complete.
- [ ] On that status-42 path, print exactly `release_phase_one signing_required statement_sha256=HEX signed_at=UTC_RFC3339` and no local path or command output.
- [ ] Implement `run-release-phase-one.sh` to invoke `run-release-gate.sh` directly, bound captured output to 4 KiB, accept only exit 42, and map verified phase-one success to exit 0.
- [ ] Have `run-release-phase-one.sh` invoke `verify-unsigned-archive`, require its evidence digest to equal the bounded no-follow root `evidence.json` digest, and atomically write canonical `PhaseOneReceipt` mode `0600`.
- [ ] Make successful `run-release-phase-one.sh` stdout exactly `release_phase_one_ready`; never print an artifact path.
- [ ] Wire the phase-one archive check to this exact CLI call, with the same exact release inputs used by collection:

```sh
"$release_gate" verify-unsigned-archive \
  --archive "$unsigned_archive" \
  --commit "$commit" \
  --config-hash "$config_hash" \
  $release_input_args
```

- [ ] Implement `finalize-release-archive.sh` so its only evidence inputs are the no-follow unsigned archive and offline attestation; do not reopen the mutable bundle or rerun a producer.
- [ ] Have `finalize-release-archive.sh` create the signed archive exclusively, immediately call `verify-release-archive.sh`, and atomically write canonical `ArchiveVerificationReceipt` mode `0600`.
- [ ] Make successful `finalize-release-archive.sh` stdout exactly `release_archive_finalized`; never print an artifact path.
- [ ] Implement `verify-release-archive.sh` as a no-write wrapper around the exact `verify-archive` command; success stdout is the exact `release_archive_verified ...` line and invalid input exits 1 without that prefix.
- [ ] Make all three release-archive wrappers require and forward this exact lexicographically sorted release-input set: `candidate_canary_halt_policy_sha256`, `candidate_canary_public_key_sha256`, `candidate_gateway_image`, `candidate_migrate_image`, `candidate_worker_image`, `deployment_region`, `egress_proxy_cidr`, `manifest_sha256`, `oidc_issuer`, `optional_provider_result`, `production_host`, `production_tls_secret`, `run_provider_tests`, `trusted_proxy_cidr`, and `voice_url`.
- [ ] Wire the finalizer and verifier respectively to these exact CLI calls, where `$release_input_args` contains one validated `--release-input name=value` pair for every name above; after `finalize-archive` succeeds, the finalizer invokes the verifier wrapper before it writes the receipt:

```sh
"$release_gate" finalize-archive \
  --unsigned-archive "$unsigned_archive" \
  --attestation "$attestation" \
  --attestation-key "$attestation_key" \
  --archive-out "$signed_archive" \
  --commit "$commit" \
  --config-hash "$config_hash" \
  $release_input_args
"$release_gate" verify-archive \
  --archive "$signed_archive" \
  --commit "$commit" \
  --config-hash "$config_hash" \
  --attestation-key "$attestation_key" \
  $release_input_args
```

- [ ] Add quiet `release-gate-phase-one`, `release-gate-finalize-archive`, and `release-gate-verify-archive` Make targets that call only those wrappers with the exact supplied paths and never echo path-bearing recipes.
- [ ] Replace the mutable phase-two `make release-gate-verify` instructions in `docs/operations.md` with the exact phase-one, offline signing, archive-finalization, and archive-verification commands from Task 7.
- [ ] Extend `TestActiveGuidesContainNoMachinePathsOrRetiredInstructions` to reject the retired mutable phase-two command.
- [ ] Add boundary tests for producer exit 1 versus signing-required exit 42, malformed phase output, oversized capture, changed root evidence, unsigned-archive mutation, wrong attestation, pre-existing signed output, and post-finalization archive mutation.
- [ ] Run those archive-flow boundary tests only against fixture children and temporary files; they must not start Docker, Kind, Kubernetes, speech, or a reasoner.
- [ ] Add the exact promotion types used by Tasks 7-8:

```go
const PromotionSchemaVersion = "production-promotion-evidence-v1"

type PromotionCondition struct {
    ID             string    `json:"id"`
    Status         string    `json:"status"`
    EvidenceSHA256 string    `json:"evidence_sha256"`
    Reviewer       string    `json:"reviewer"`
    VerifiedAt     time.Time `json:"verified_at"`
}

type PromotionRecord struct {
    SchemaVersion         string               `json:"schema_version"`
    Commit                string               `json:"commit"`
    ConfigHash            string               `json:"config_hash"`
    ReleaseEvidenceSHA256 string               `json:"release_evidence_sha256"`
    GeneratedAt           time.Time            `json:"generated_at"`
    Status                string               `json:"status"`
    Conditions            []PromotionCondition `json:"conditions"`
}

type PromotionAttestation struct {
    Signer          string    `json:"signer"`
    StatementSHA256 string    `json:"statement_sha256"`
    Signature       string    `json:"signature"`
    SignedAt        time.Time `json:"signed_at"`
}

type PromotionEvidence struct {
    Record      PromotionRecord      `json:"record"`
    Attestation PromotionAttestation `json:"attestation"`
}
```

- [ ] Add a RED decoder table proving duplicate keys, unknown fields, trailing JSON, noncanonical JSON, and an over-limit document are rejected for record, attestation, and combined evidence.
- [ ] Add a RED verification test for a wrong `ReleaseEvidenceSHA256` and for a root `evidence.json` whose bytes change after the record is created.
- [ ] Add a RED verification test proving the release/privacy public key cannot authorize promotion.
- [ ] Add a RED verification test for wrong signer `release-operator` and a tampered `promotion-operator` signature.
- [ ] Add one RED condition-set test each for a missing, duplicate, unsorted, or unknown condition ID.
- [ ] Add one RED condition-state test each for `blocked` with a digest, `passed` without a digest, an unknown status, and an aggregate status that disagrees with its conditions.
- [ ] Add RED boundary tests at exactly `now-24h` and `now+5m`, plus one test each just outside those bounds.
- [ ] Add RED ordering tests for `VerifiedAt > GeneratedAt` and `SignedAt < GeneratedAt`.
- [ ] Add RED CLI tests proving valid `passed` exits 0 with the exact passed summary, valid `blocked` exits 3 with only sorted missing IDs, and invalid evidence exits 1 without a success summary.
- [ ] Implement `func DecodePromotionRecord(document []byte) (PromotionRecord, error)` with duplicate-key rejection, unknown-field rejection, EOF enforcement, canonical-JSON enforcement, and `MaximumEvidenceBytes`.
- [ ] Implement `func DecodePromotionAttestation(document []byte) (PromotionAttestation, error)` with duplicate-key rejection, unknown-field rejection, EOF enforcement, canonical-JSON enforcement, and `MaximumAttestationBytes`.
- [ ] Implement `func DecodePromotionEvidence(document []byte) (PromotionEvidence, error)` with recursive duplicate-key rejection, unknown-field rejection, EOF enforcement, canonical-JSON enforcement, and `MaximumEvidenceBytes`.
- [ ] Implement `func PromotionStatementSHA256(record PromotionRecord) (string, error)` over canonical JSON.
- [ ] Implement `func VerifyPromotionEvidence(evidence PromotionEvidence, expectedCommit, expectedConfigHash, expectedReleaseEvidenceSHA256 string, keys map[string]ed25519.PublicKey, now time.Time) error`.
- [ ] Accept only signer `promotion-operator` from the separately supplied promotion key map; never fall back to the privacy release-attestation key.
- [ ] Require the exact seven sorted condition IDs defined in Task 8, each exactly once; `passed` requires a 64-hex digest, `blocked` requires an empty digest, and aggregate status is derived from the conditions.
- [ ] Reuse `MaximumEvidenceBytes = 4 << 20`, `MaximumAttestationBytes = 64 << 10`, `maximumEvidenceAge = 24*time.Hour`, and `maximumFutureSkew = 5*time.Minute` for promotion reads and time validation.
- [ ] Require `now-24h <= VerifiedAt <= GeneratedAt <= SignedAt <= now+5m` for every condition and attestation.
- [ ] Add `promotion-record --bundle-root ABSOLUTE_BUNDLE --commit SHA --config-hash HASH --release-evidence-sha256 DIGEST --condition id=status,digest,reviewer,verified_at --record-out promotion/record.json`.
- [ ] Require `promotion-record` to receive the Task 7 release-evidence digest and exactly seven condition flags.
- [ ] Require each condition reviewer to match `[A-Za-z0-9._@-]{1,128}` and each verification time to be UTC RFC3339.
- [ ] Make `promotion-record`, `promotion-statement`, `promotion-collect`, and `promotion-verify` use bounded no-follow reads under the supplied bundle root.
- [ ] Make every promotion command use atomic private writes under the supplied bundle root.
- [ ] Make `promotion-verify` hash root `evidence.json` and compare it to `ReleaseEvidenceSHA256`.
- [ ] Make `promotion-verify` exit 0 and print exactly `production_promotion_verified status=passed blockers=0` for a valid passed record.
- [ ] Make `promotion-verify` exit 3 for a valid blocked record; print `production_promotion_verified status=blocked blockers=N` followed only by the `N` lexicographically sorted blocked condition IDs.
- [ ] Make malformed, stale, wrongly signed, or mismatched promotion evidence exit 1 and never print `production_promotion_verified`.
- [ ] Add the anchored `/artifacts/` entry to `.gitignore` and assert `git check-ignore artifacts/release/probe` succeeds, so generated evidence never dirties or enters the candidate commit.
- [ ] Run the Go-wrapper boundary suite and require PASS.
- [ ] Run the Node-wrapper boundary suite and require PASS.
- [ ] Run release-gate shell and Go boundary suites and require PASS.
- [ ] Commit the zero-skip release-producer slice.

```sh
./deploy/scripts/test-go-test-no-skip.sh
./deploy/scripts/test-run-load-evidence.sh
node deploy/scripts/test-node-test-no-skip.mjs
./deploy/scripts/test-release-gate-boundaries.sh
go test -count=1 ./demo/live-client -run 'Test.*Doc'
go test -race -count=10 ./internal/releasegate ./cmd/live-release-gate
git add Makefile .gitignore docs/operations.md demo/live-client/docs_test.go \
  deploy/scripts/run-go-test-no-skip.sh deploy/scripts/test-go-test-no-skip.sh \
  deploy/scripts/run-load-evidence.sh deploy/scripts/test-run-load-evidence.sh \
  deploy/scripts/run-node-test-no-skip.mjs deploy/scripts/test-node-test-no-skip.mjs \
  deploy/scripts/run-release-gate.sh deploy/scripts/run-release-phase-one.sh \
  deploy/scripts/finalize-release-archive.sh deploy/scripts/verify-release-archive.sh \
  deploy/scripts/test-release-gate-boundaries.sh \
  internal/releasegate/evidence.go internal/releasegate/evidence_test.go \
  internal/releasegate/promotion.go \
  internal/releasegate/promotion_test.go internal/releasegate/check.go \
  internal/releasegate/check_test.go \
  cmd/live-release-gate/main.go cmd/live-release-gate/main_test.go
git commit -m 'fix: require zero skip release evidence'
```

---

## Task 7: Review, Then Run the Full Release-Candidate Matrix

**Files:**

- Verify only: all implementation files changed by the four production-readiness plans
- Generate, never commit: `artifacts/release/$candidate_commit/$RELEASE_CONFIG_HASH/evidence.json`
- Generate, never commit: `artifacts/release/$candidate_commit/$RELEASE_CONFIG_HASH/phase-one.json`
- Generate, never commit: `artifacts/release/$candidate_commit/$RELEASE_CONFIG_HASH/archive-verification.json`
- Generate, never commit: `artifacts/release-archives/$candidate_commit/${RELEASE_CONFIG_HASH#v1:}.unsigned.tar`
- Generate, never commit: `artifacts/release-archives/$candidate_commit/${RELEASE_CONFIG_HASH#v1:}.signed.tar`

**Interfaces:**

- Consumes: Task 6's Go/Node zero-skip wrappers, twelve exact release producers, `run-release-phase-one.sh`, `finalize-release-archive.sh`, `verify-release-archive.sh`, and CLI subcommands `verify-unsigned-archive` and `verify-archive`; an already-running disposable PostgreSQL service through `TEST_DATABASE_URL`; an operator-provisioned speech contract through `S2S_REALTIME_URL` only at the explicit contract boundary.
- Produces: canonical root `type releasegate.Evidence struct { SchemaVersion string; Commit string; ConfigHash string; GeneratedAt time.Time; ReleaseInputs []releasegate.ReleaseInput; Gates []releasegate.GateEvidence; Artifacts []releasegate.ArtifactEvidence; Attestations []releasegate.SignedAttestation }` at `artifacts/release/$candidate_commit/$RELEASE_CONFIG_HASH/evidence.json`; canonical `releasegate.PhaseOneReceipt` and `releasegate.ArchiveVerificationReceipt` alongside it; immutable unsigned archive `artifacts/release-archives/$candidate_commit/${RELEASE_CONFIG_HASH#v1:}.unsigned.tar`; immutable signed archive `artifacts/release-archives/$candidate_commit/${RELEASE_CONFIG_HASH#v1:}.signed.tar`; every file binds the frozen commit/config hash and every required gate is nonzero-skip and successful.

- [ ] Request independent code, security/privacy, runtime/barge-in, deployment, and browser reviews before producing evidence.
- [ ] For each P0/P1/P2 finding, leave Task 7, return to the owning implementation task, add one RED regression, fix it, and commit before restarting Task 7 from the first review step.
- [ ] Discard any earlier candidate bundle after a review-driven change.
- [ ] Require a clean worktree and bind the candidate variables:

```sh
test -z "$(git status --porcelain)"
candidate_commit=$(git rev-parse HEAD)
printf '%s\n' "$candidate_commit" | rg -q '^[0-9a-f]{40}$'
: "${RELEASE_CONFIG_HASH:?RELEASE_CONFIG_HASH must be the reviewed v1 hash}"
printf '%s\n' "$RELEASE_CONFIG_HASH" | rg -q '^v1:[0-9a-f]{64}$'
release_bundle="$PWD/artifacts/release/$candidate_commit/$RELEASE_CONFIG_HASH"
release_evidence="$release_bundle/evidence.json"
phase_one_receipt="$release_bundle/phase-one.json"
archive_verification_receipt="$release_bundle/archive-verification.json"
release_archive_dir="$PWD/artifacts/release-archives/$candidate_commit"
unsigned_archive="$release_archive_dir/${RELEASE_CONFIG_HASH#v1:}.unsigned.tar"
signed_archive="$release_archive_dir/${RELEASE_CONFIG_HASH#v1:}.signed.tar"
```

- [ ] Run repository unit, compiler, tidy, credential-free startup, browser Go, and static deployment gates without starting Docker services or contacting a provider:

```sh
test_packages=$(go list -f '{{if or .TestGoFiles .XTestGoFiles}}{{.ImportPath}}{{end}}' ./...) || exit 1
test -n "$test_packages" || exit 1
./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 1 -- \
  go test -json -race -count=1 $test_packages
go vet ./...
go build ./...
go mod tidy -diff
make test-credential-free-startup
./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 1 -- \
  go test -json -race -count=1 ./demo/live-client
python3 deploy/scripts/test-operator-speech-launcher.py
./deploy/scripts/test-demo-wizard.sh
./deploy/scripts/test-validate-deploy.sh
make verify-deploy-static
go tool govulncheck ./...
git diff --check
```

- [ ] Run the exact 224-test browser suite through the Node zero-skip wrapper:

```sh
node deploy/scripts/run-node-test-no-skip.mjs --expect-tests 224 -- \
  node --test demo/live-client/web/*.test.mjs
```

- [ ] Require a nonempty disposable PostgreSQL URL; absence blocks the candidate.

```sh
test -n "${TEST_DATABASE_URL:-}" || { echo 'TEST_DATABASE_URL is required for release integration' >&2; exit 1; }
```

- [ ] Run migration/schema/profile/knowledge/handoff integration as exact JSON with zero skips:

```sh
TEST_DATABASE_URL="$TEST_DATABASE_URL" \
./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 1 -- \
  go test -json -race -count=1 -tags=integration \
  ./migrations ./internal/store/postgres ./internal/admission ./internal/feature \
  ./internal/gateway ./internal/worker ./cmd/live-gateway ./cmd/live-worker \
  ./test/integration/...
```

- [ ] Run provider-VAD/manual ordering, two-turn recovery, blocked-write cancellation, 1,025-cycle correlation, byte-pressure, and reconnect unit gates:

```sh
./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 1 -- \
  go test -json -race -count=25 ./internal/realtime/s2sws ./internal/session ./internal/transport/ws \
    -run 'Test.*(ProviderVAD|Manual|TwoTurn|Blocked|1025|Byte|Reconnect|Barge)'
```

- [ ] Run continuous barge-in load fencing without an external provider:

```sh
./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 3 \
  --expect-top-level TestBargeInAcknowledgedFenceSuppressesEveryStaleFrame,TestContinuousBargeWithProfileRenderingPreservesEpochAndProgress,TestRealtimePressureBlockedWriteOmittedTerminalRecovery -- \
  go test -json -count=1 -tags=load ./test/load/... \
  -run '^(TestContinuousBargeWithProfileRenderingPreservesEpochAndProgress|TestBargeInAcknowledgedFenceSuppressesEveryStaleFrame|TestRealtimePressureBlockedWriteOmittedTerminalRecovery)$' \
  -timeout=2m
```

- [ ] Enter the explicit speech-contract boundary, require the operator-provisioned endpoint, and reject skips:

```sh
test -n "${S2S_REALTIME_URL:-}" || { echo 'S2S_REALTIME_URL is required for the speech contract gate' >&2; exit 1; }
contract_tests='TestContractPartialAdmissionFailureDrainsEverySuccessfulSession,TestDialReadyPostHandshakeCloseIsNeverSkippable,TestReadinessRetriesRememberSuccessfulHandshakeAcrossServiceLoss,TestReadinessUnavailableBeforeAnyHandshakeRemainsSkippable,TestRealtimeImmediateCreateCancelThenNextTurnContract,TestS2SConnectorForwardsPCMBytesExactly,TestSimulatorDeclaredConcurrency,TestSimulatorFunctionCallOneActiveResponseAndCancellation,TestSimulatorSessionUpdateAndPCM24AppendCommitAcceptance,TestSimulatorVADImplicitResponseBindsEpochBeforeAudioAndCompletesOnce'
S2S_REALTIME_URL="$S2S_REALTIME_URL" \
./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 10 --expect-top-level "$contract_tests" -- \
  go test -json -count=1 -tags=contract ./test/contract/...
```

- [ ] Reconfirm the clean immutable candidate before any manual infrastructure gate:

```sh
test "$(git rev-parse HEAD)" = "$candidate_commit"
test -z "$(git status --porcelain)"
```

- [ ] Enter the explicit manual infrastructure boundary; these commands require Docker/Kind/Kubernetes prerequisites and may start disposable services, so they are never part of automatic unit gates:

```sh
make verify-deploy
make smoke-deploy
```

- [ ] Require the formal topology, immutable candidate image/SBOM/provenance directory, canary receipt, production manifest, release inputs, and `LIVE_EVIDENCE_DIR` documented in `docs/operations.md`.
- [ ] Run phase one through the exact wrapper; the wrapper accepts only the reserved internal signing-required exit 42 and itself returns 0 only after unsigned-archive verification:

```sh
RELEASE_COMMIT="$candidate_commit" \
RELEASE_CONFIG_HASH="$RELEASE_CONFIG_HASH" \
RELEASE_EVIDENCE_OUT="$release_evidence" \
RELEASE_UNSIGNED_ARCHIVE_OUT="$unsigned_archive" \
RELEASE_PHASE_ONE_RECEIPT_OUT="$phase_one_receipt" \
make release-gate-phase-one
```

Expected: all twelve producers pass; canonical `evidence.json`, the immutable unsigned archive, and `phase-one.json` are written; the Make target exits 0. A producer failure, malformed status-42 sentinel, missing gate, skip, or invalid archive exits nonzero and writes no passing receipt.

- [ ] Recompute bounded digests and validate the durable phase-one receipt before sending its exact `statement_sha256` and `signed_at` to the independent signer:

```sh
test -f "$release_evidence" && test ! -L "$release_evidence"
test -f "$unsigned_archive" && test ! -L "$unsigned_archive"
test -f "$phase_one_receipt" && test ! -L "$phase_one_receipt"
test "$(wc -c <"$release_evidence" | tr -d '[:space:]')" -gt 0
test "$(wc -c <"$release_evidence" | tr -d '[:space:]')" -le 4194304
test "$(wc -c <"$unsigned_archive" | tr -d '[:space:]')" -gt 0
test "$(wc -c <"$unsigned_archive" | tr -d '[:space:]')" -le 69206016
test "$(wc -c <"$phase_one_receipt" | tr -d '[:space:]')" -gt 0
test "$(wc -c <"$phase_one_receipt" | tr -d '[:space:]')" -le 65536
root_evidence_sha256=$(openssl dgst -sha256 <"$release_evidence" | awk 'NF {print $NF}')
unsigned_archive_sha256=$(openssl dgst -sha256 <"$unsigned_archive" | awk 'NF {print $NF}')
printf '%s\n' "$root_evidence_sha256" | rg -q '^[0-9a-f]{64}$'
printf '%s\n' "$unsigned_archive_sha256" | rg -q '^[0-9a-f]{64}$'
jq -e --arg commit "$candidate_commit" --arg hash "$RELEASE_CONFIG_HASH" \
  --arg evidence "$root_evidence_sha256" --arg archive "$unsigned_archive_sha256" \
  '.schema_version == "release-phase-one-v1" and .commit == $commit and .config_hash == $hash and
   .evidence_sha256 == $evidence and .unsigned_archive_sha256 == $archive and
   (.statement_sha256 | test("^[0-9a-f]{64}$")) and
   (.signed_at | test("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]+)?Z$"))' \
  "$phase_one_receipt" >/dev/null
```

- [ ] After the signer returns the detached `release-operator` attestation and separate public key, finalize solely from the immutable unsigned archive and verify the resulting signed archive:

```sh
: "${RELEASE_ATTESTATION_FILE:?RELEASE_ATTESTATION_FILE is required}"
case "$RELEASE_ATTESTATION_FILE" in /*) ;; *) exit 1 ;; esac
: "${RELEASE_ATTESTATION_KEY:?RELEASE_ATTESTATION_KEY is required}"
case "$RELEASE_ATTESTATION_KEY" in release-operator=/*) ;; *) exit 1 ;; esac
RELEASE_COMMIT="$candidate_commit" \
RELEASE_CONFIG_HASH="$RELEASE_CONFIG_HASH" \
RELEASE_UNSIGNED_ARCHIVE_OUT="$unsigned_archive" \
RELEASE_SIGNED_ARCHIVE_OUT="$signed_archive" \
RELEASE_ARCHIVE_VERIFICATION_RECEIPT_OUT="$archive_verification_receipt" \
make release-gate-finalize-archive
RELEASE_COMMIT="$candidate_commit" \
RELEASE_CONFIG_HASH="$RELEASE_CONFIG_HASH" \
RELEASE_SIGNED_ARCHIVE_OUT="$signed_archive" \
make release-gate-verify-archive
```

- [ ] Re-validate the signed archive receipt, root unsigned evidence identity, and frozen candidate without reopening producer artifacts:

```sh
test -f "$signed_archive" && test ! -L "$signed_archive"
test -f "$archive_verification_receipt" && test ! -L "$archive_verification_receipt"
test "$(wc -c <"$signed_archive" | tr -d '[:space:]')" -gt 0
test "$(wc -c <"$signed_archive" | tr -d '[:space:]')" -le 69206016
test "$(wc -c <"$archive_verification_receipt" | tr -d '[:space:]')" -gt 0
test "$(wc -c <"$archive_verification_receipt" | tr -d '[:space:]')" -le 65536
signed_archive_sha256=$(openssl dgst -sha256 <"$signed_archive" | awk 'NF {print $NF}')
printf '%s\n' "$signed_archive_sha256" | rg -q '^[0-9a-f]{64}$'
jq -e --arg commit "$candidate_commit" --arg hash "$RELEASE_CONFIG_HASH" \
  --arg evidence "$root_evidence_sha256" --arg archive "$signed_archive_sha256" \
  '.schema_version == "release-archive-verification-v1" and .commit == $commit and
   .config_hash == $hash and .attested_unsigned_evidence_sha256 == $evidence and
   .signed_archive_sha256 == $archive and
   (.signed_evidence_sha256 | test("^[0-9a-f]{64}$"))' \
  "$archive_verification_receipt" >/dev/null
jq -e --arg commit "$candidate_commit" --arg hash "$RELEASE_CONFIG_HASH" \
  '.schema_version == "release-evidence-v2" and .commit == $commit and
   .config_hash == $hash and (.attestations | length) == 0' \
  "$release_evidence" >/dev/null
test "$(git rev-parse HEAD)" = "$candidate_commit"
test -z "$(git status --porcelain)"
```

- [ ] Treat any later source, config, test, producer, or documentation change as invalidating the candidate and both evidence phases.

## Task 8: Record External Production-Promotion Blockers

**Files:**

- Verify only: `internal/releasegate/promotion.go`
- Verify only: `artifacts/release/$candidate_commit/$RELEASE_CONFIG_HASH/evidence.json`
- Verify only: `artifacts/release/$candidate_commit/$RELEASE_CONFIG_HASH/archive-verification.json`
- Verify only: `artifacts/release-archives/$candidate_commit/${RELEASE_CONFIG_HASH#v1:}.signed.tar`
- Generate, never commit: `artifacts/release/$candidate_commit/$RELEASE_CONFIG_HASH/promotion/record.json`
- Generate, never commit: `artifacts/release/$candidate_commit/$RELEASE_CONFIG_HASH/promotion/attestation.json`
- Generate, never commit: `artifacts/release/$candidate_commit/$RELEASE_CONFIG_HASH/promotion/evidence.json`

**Interfaces:**

- Consumes: Task 6's exact `PromotionRecord` schema; `promotion-record --bundle-root ABSOLUTE_BUNDLE --commit SHA --config-hash HASH --release-evidence-sha256 DIGEST --condition id=status,digest,reviewer,verified_at` repeated exactly seven times plus `--record-out RELATIVE_PATH`; `promotion-statement --bundle-root ABSOLUTE_BUNDLE --record RELATIVE_PATH --commit SHA --config-hash HASH`; `promotion-collect --bundle-root ABSOLUTE_BUNDLE --record RELATIVE_PATH --attestation RELATIVE_PATH --evidence-out RELATIVE_PATH`; and `promotion-verify --bundle-root ABSOLUTE_BUNDLE --evidence RELATIVE_PATH --commit SHA --config-hash HASH --attestation-key promotion-operator=ABSOLUTE_PUBLIC_KEY`; Task 7's verified immutable signed archive, `ArchiveVerificationReceipt`, and matching root unsigned `evidence.json` for the same commit/config hash; a separate offline Ed25519 promotion authority named `promotion-operator`, with public verification key supplied only as `PROMOTION_ATTESTATION_KEY=promotion-operator=/absolute/path/to/public-key.pem`.
- Produces: durable signed `production-promotion-evidence-v1` at `artifacts/release/$candidate_commit/$RELEASE_CONFIG_HASH/promotion/evidence.json`; aggregate `status` is `passed` only when all seven exact conditions are passed, otherwise `blocked`; production handoffs remain disabled in either case for this release.

- [ ] Rebind the exact Task 7 paths and verify its immutable signed archive before creating a promotion record:

```sh
release_bundle="$PWD/artifacts/release/$candidate_commit/$RELEASE_CONFIG_HASH"
release_evidence="$release_bundle/evidence.json"
archive_verification_receipt="$release_bundle/archive-verification.json"
release_archive_dir="$PWD/artifacts/release-archives/$candidate_commit"
signed_archive="$release_archive_dir/${RELEASE_CONFIG_HASH#v1:}.signed.tar"
: "${RELEASE_ATTESTATION_KEY:?RELEASE_ATTESTATION_KEY is required}"
case "$RELEASE_ATTESTATION_KEY" in release-operator=/*) ;; *) exit 1 ;; esac
RELEASE_COMMIT="$candidate_commit" \
RELEASE_CONFIG_HASH="$RELEASE_CONFIG_HASH" \
RELEASE_SIGNED_ARCHIVE_OUT="$signed_archive" \
make release-gate-verify-archive
```

- [ ] Bound and hash root evidence plus signed archive, then require the verified receipt to bind both:

```sh
test -f "$release_evidence" && test ! -L "$release_evidence"
test -f "$signed_archive" && test ! -L "$signed_archive"
test -f "$archive_verification_receipt" && test ! -L "$archive_verification_receipt"
release_evidence_bytes=$(wc -c <"$release_evidence" | tr -d '[:space:]')
case "$release_evidence_bytes" in ''|*[!0-9]*) exit 1 ;; esac
test "$release_evidence_bytes" -gt 0
test "$release_evidence_bytes" -le 4194304
signed_archive_bytes=$(wc -c <"$signed_archive" | tr -d '[:space:]')
case "$signed_archive_bytes" in ''|*[!0-9]*) exit 1 ;; esac
test "$signed_archive_bytes" -gt 0
test "$signed_archive_bytes" -le 69206016
release_evidence_sha256=$(openssl dgst -sha256 <"$release_evidence" | awk 'NF {print $NF}')
signed_archive_sha256=$(openssl dgst -sha256 <"$signed_archive" | awk 'NF {print $NF}')
printf '%s\n' "$release_evidence_sha256" | rg -q '^[0-9a-f]{64}$'
printf '%s\n' "$signed_archive_sha256" | rg -q '^[0-9a-f]{64}$'
jq -e --arg commit "$candidate_commit" --arg hash "$RELEASE_CONFIG_HASH" \
  --arg evidence "$release_evidence_sha256" --arg archive "$signed_archive_sha256" \
  '.schema_version == "release-archive-verification-v1" and .commit == $commit and
   .config_hash == $hash and .attested_unsigned_evidence_sha256 == $evidence and
   .signed_archive_sha256 == $archive' \
  "$archive_verification_receipt" >/dev/null
```

- [ ] Create the promotion directory with mode `0700` beneath the exact Task 7 bundle:

```sh
bundle="$PWD/artifacts/release/$candidate_commit/$RELEASE_CONFIG_HASH"
promotion_dir="$bundle/promotion"
promotion_record="$promotion_dir/record.json"
promotion_attestation="$promotion_dir/attestation.json"
promotion_evidence="$promotion_dir/evidence.json"
install -d -m 0700 "$promotion_dir"
```
- [ ] Record these seven condition IDs in lexicographic order and exactly once:

```text
capacity-chaos-two-hour-soak
canary-observation
independent-privacy-signoff
live-speech-reasoner-contract
physical-playback-acoustic-barge-in
production-identities-and-secret-delivery
rollback-authority-execution
```

- [ ] For each available condition, set `status` to `passed`, bind the reviewed content-free artifact by its lowercase 64-hex SHA-256, and record a bounded reviewer identity plus UTC timestamp.
- [ ] For each unavailable condition, set `status` to `blocked` with an empty digest; never substitute a skip, fixture, smoke result, or mock.
- [ ] Write the exact seven-condition `PromotionRecord` from Task 6 atomically with mode `0600`; each shell variable is mandatory, and the CLI derives the aggregate status rather than trusting operator input:

```sh
bundle="$PWD/artifacts/release/$candidate_commit/$RELEASE_CONFIG_HASH"
promotion_dir="$bundle/promotion"
promotion_record="$promotion_dir/record.json"
promotion_attestation="$promotion_dir/attestation.json"
promotion_evidence="$promotion_dir/evidence.json"
test -f "$release_evidence"
test ! -L "$release_evidence"
release_evidence_bytes=$(wc -c <"$release_evidence" | tr -d '[:space:]')
case "$release_evidence_bytes" in ''|*[!0-9]*) exit 1 ;; esac
test "$release_evidence_bytes" -gt 0
test "$release_evidence_bytes" -le 4194304
release_evidence_sha256=$(openssl dgst -sha256 <"$release_evidence" | awk 'NF {print $NF}')
printf '%s\n' "$release_evidence_sha256" | rg -q '^[0-9a-f]{64}$'
go run ./cmd/live-release-gate promotion-record \
  --bundle-root "$bundle" \
  --commit "$candidate_commit" \
  --config-hash "$RELEASE_CONFIG_HASH" \
  --release-evidence-sha256 "$release_evidence_sha256" \
  --condition "capacity-chaos-two-hour-soak=${PROMOTION_CAPACITY_STATUS:?required},${PROMOTION_CAPACITY_SHA256-},${PROMOTION_CAPACITY_REVIEWER:?required},${PROMOTION_CAPACITY_VERIFIED_AT:?required}" \
  --condition "canary-observation=${PROMOTION_CANARY_STATUS:?required},${PROMOTION_CANARY_SHA256-},${PROMOTION_CANARY_REVIEWER:?required},${PROMOTION_CANARY_VERIFIED_AT:?required}" \
  --condition "independent-privacy-signoff=${PROMOTION_PRIVACY_STATUS:?required},${PROMOTION_PRIVACY_SHA256-},${PROMOTION_PRIVACY_REVIEWER:?required},${PROMOTION_PRIVACY_VERIFIED_AT:?required}" \
  --condition "live-speech-reasoner-contract=${PROMOTION_LIVE_PROVIDER_STATUS:?required},${PROMOTION_LIVE_PROVIDER_SHA256-},${PROMOTION_LIVE_PROVIDER_REVIEWER:?required},${PROMOTION_LIVE_PROVIDER_VERIFIED_AT:?required}" \
  --condition "physical-playback-acoustic-barge-in=${PROMOTION_PHYSICAL_AUDIO_STATUS:?required},${PROMOTION_PHYSICAL_AUDIO_SHA256-},${PROMOTION_PHYSICAL_AUDIO_REVIEWER:?required},${PROMOTION_PHYSICAL_AUDIO_VERIFIED_AT:?required}" \
  --condition "production-identities-and-secret-delivery=${PROMOTION_IDENTITIES_STATUS:?required},${PROMOTION_IDENTITIES_SHA256-},${PROMOTION_IDENTITIES_REVIEWER:?required},${PROMOTION_IDENTITIES_VERIFIED_AT:?required}" \
  --condition "rollback-authority-execution=${PROMOTION_ROLLBACK_STATUS:?required},${PROMOTION_ROLLBACK_SHA256-},${PROMOTION_ROLLBACK_REVIEWER:?required},${PROMOTION_ROLLBACK_VERIFIED_AT:?required}" \
  --record-out promotion/record.json
```

- [ ] Validate the unsigned record and print its canonical statement digest:

```sh
go run ./cmd/live-release-gate promotion-statement \
  --bundle-root "$bundle" \
  --record promotion/record.json \
  --commit "$candidate_commit" \
  --config-hash "$RELEASE_CONFIG_HASH"
```

- [ ] On the authorized offline signer, sign the exact 32-byte statement digest and install the bounded attestation into the private bundle:

```sh
: "${PROMOTION_SIGNING_KEY:?PROMOTION_SIGNING_KEY must name the offline Ed25519 private key}"
case "$PROMOTION_SIGNING_KEY" in /*) ;; *) exit 1 ;; esac
statement=$(go run ./cmd/live-release-gate promotion-statement \
  --bundle-root "$bundle" \
  --record promotion/record.json \
  --commit "$candidate_commit" \
  --config-hash "$RELEASE_CONFIG_HASH")
printf '%s\n' "$statement" | rg -q '^[0-9a-f]{64}$'
signing_scratch=$(mktemp -d "${TMPDIR:-/tmp}/gpt-live-promotion-sign.XXXXXXXX")
trap 'rm -rf "$signing_scratch"' EXIT HUP INT TERM
printf '%s' "$statement" | xxd -r -p >"$signing_scratch/statement.bin"
openssl pkeyutl -sign -rawin -inkey "$PROMOTION_SIGNING_KEY" \
  -in "$signing_scratch/statement.bin" -out "$signing_scratch/signature.bin"
signature=$(openssl base64 -A -in "$signing_scratch/signature.bin")
signed_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
jq -n --arg signer promotion-operator --arg statement "$statement" \
  --arg signature "$signature" --arg signed_at "$signed_at" \
  '{signer:$signer,statement_sha256:$statement,signature:$signature,signed_at:$signed_at}' \
  >"$signing_scratch/attestation.json"
install -m 0600 "$signing_scratch/attestation.json" "$promotion_attestation"
```
- [ ] Collect the bounded record and attestation atomically into the final durable evidence file:

```sh
go run ./cmd/live-release-gate promotion-collect \
  --bundle-root "$bundle" \
  --record promotion/record.json \
  --attestation promotion/attestation.json \
  --evidence-out promotion/evidence.json
```

- [ ] Immediately before promotion verification, re-verify the signed release archive and require its receipt still binds the current bounded root evidence:

```sh
: "${RELEASE_ATTESTATION_KEY:?RELEASE_ATTESTATION_KEY is required}"
case "$RELEASE_ATTESTATION_KEY" in release-operator=/*) ;; *) exit 1 ;; esac
RELEASE_COMMIT="$candidate_commit" \
RELEASE_CONFIG_HASH="$RELEASE_CONFIG_HASH" \
RELEASE_SIGNED_ARCHIVE_OUT="$signed_archive" \
make release-gate-verify-archive
release_evidence_sha256=$(openssl dgst -sha256 <"$release_evidence" | awk 'NF {print $NF}')
signed_archive_sha256=$(openssl dgst -sha256 <"$signed_archive" | awk 'NF {print $NF}')
printf '%s\n' "$release_evidence_sha256" | rg -q '^[0-9a-f]{64}$'
printf '%s\n' "$signed_archive_sha256" | rg -q '^[0-9a-f]{64}$'
jq -e --arg evidence "$release_evidence_sha256" --arg archive "$signed_archive_sha256" \
  '.attested_unsigned_evidence_sha256 == $evidence and .signed_archive_sha256 == $archive' \
  "$archive_verification_receipt" >/dev/null
```

- [ ] Build the reviewed CLI once so its distinct blocked exit 3 is observable, then verify signature, schema, commit, config hash, condition set, aggregate status, age, and content-free bounds:

```sh
: "${PROMOTION_ATTESTATION_KEY:?PROMOTION_ATTESTATION_KEY is required}"
case "$PROMOTION_ATTESTATION_KEY" in promotion-operator=/*) ;; *) exit 1 ;; esac
promotion_verify_scratch=$(mktemp -d "${TMPDIR:-/tmp}/gpt-live-promotion-verify.XXXXXXXX")
trap 'rm -rf "$promotion_verify_scratch"' EXIT HUP INT TERM
go build -trimpath -o "$promotion_verify_scratch/live-release-gate" ./cmd/live-release-gate || exit 1
promotion_verify_exit=0
"$promotion_verify_scratch/live-release-gate" promotion-verify \
  --bundle-root "$bundle" \
  --evidence promotion/evidence.json \
  --commit "$candidate_commit" \
  --config-hash "$RELEASE_CONFIG_HASH" \
  --attestation-key "$PROMOTION_ATTESTATION_KEY" \
  >"$promotion_verify_scratch/output" || promotion_verify_exit=$?
test "$(wc -c <"$promotion_verify_scratch/output" | tr -d '[:space:]')" -le 4096
test "$(git rev-parse HEAD)" = "$candidate_commit"
test -z "$(git status --porcelain)"
```

- [ ] Accept exit 0 only with the exact one-line passed contract:

```sh
if test "$promotion_verify_exit" -eq 0; then
  test "$(wc -l <"$promotion_verify_scratch/output" | tr -d '[:space:]')" -eq 1
  rg -x 'production_promotion_verified status=passed blockers=0' \
    "$promotion_verify_scratch/output" >/dev/null
fi
```

- [ ] Accept exit 3 only when the header count and following lines exactly equal the verified record's sorted blocked IDs:

```sh
if test "$promotion_verify_exit" -eq 3; then
  promotion_header=$(sed -n '1p' "$promotion_verify_scratch/output")
  printf '%s\n' "$promotion_header" | rg -q \
    '^production_promotion_verified status=blocked blockers=[1-7]$'
  reported_blockers=${promotion_header##*=}
  actual_blockers=$(sed -n '2,$p' "$promotion_verify_scratch/output")
  actual_blocker_count=$(printf '%s\n' "$actual_blockers" | awk 'NF {count++} END {print count+0}')
  test "$actual_blocker_count" -eq "$reported_blockers"
  test -f "$promotion_evidence" && test ! -L "$promotion_evidence"
  promotion_evidence_bytes=$(wc -c <"$promotion_evidence" | tr -d '[:space:]')
  case "$promotion_evidence_bytes" in ''|*[!0-9]*) exit 1 ;; esac
  test "$promotion_evidence_bytes" -gt 0
  test "$promotion_evidence_bytes" -le 4194304
  expected_blockers=$(jq -er \
    '[.record.conditions[] | select(.status == "blocked") | .id] |
     if length > 0 and . == (sort | unique) then .[] else error("invalid blockers") end' \
    "$promotion_evidence")
  expected_blocker_count=$(printf '%s\n' "$expected_blockers" | awk 'NF {count++} END {print count+0}')
  test "$expected_blocker_count" -eq "$reported_blockers"
  test "$actual_blockers" = "$expected_blockers"
  invalid_blockers=$(printf '%s\n' "$actual_blockers" | rg -v -x \
    '(capacity-chaos-two-hour-soak|canary-observation|independent-privacy-signoff|live-speech-reasoner-contract|physical-playback-acoustic-barge-in|production-identities-and-secret-delivery|rollback-authority-execution)' || true)
  test -z "$invalid_blockers"
  test "$(printf '%s\n' "$actual_blockers" | LC_ALL=C sort -u)" = "$actual_blockers"
fi
```

- [ ] Reject every exit other than 0 or 3; exit 0 means external promotion evidence passed, while exit 3 means promotion remains blocked by only the emitted IDs:

```sh
case "$promotion_verify_exit" in 0|3) ;; *) exit "$promotion_verify_exit" ;; esac
```

- [ ] Do not change `features.handoffs` in this implementation branch for either verified outcome.
- [ ] Keep `record.json`, `attestation.json`, and `evidence.json` outside Git.
- [ ] Treat the final implementation or review-fix commit immediately before Task 7's clean-worktree check as the last code commit, then freeze that exact `HEAD` before Tasks 7-8 create evidence.
- [ ] Invalidate every generated artifact after any code or documentation commit following the freeze.

## Plan Completion Criteria

- `/setup` uses explicit portable, revision-pinned local speech configuration with no checked-in user path.
- Browser capture/playback works at common hardware sample rates, survives multiple turns and barge-in, and retains bounded protocol/transcript diagnostics.
- Quiet browsers stay connected through bounded heartbeat while dead peers terminate cleanly.
- Production artifacts contain no browser, `/setup`, operator, demo bearer, or development listener.
- Active documentation accurately separates credential-free startup, working inference, development tooling, and production deployment.
- Release evidence fails on skips/missing tests and contains no private payload.
- Automated success is reported only as release-candidate evidence; external promotion blockers remain explicit.
