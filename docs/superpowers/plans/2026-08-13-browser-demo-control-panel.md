# Browser Demo Control Panel Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a loopback-only `/setup` page with safe, fixed controls for starting, stopping, and checking Speech, PostgreSQL, Gateway, Worker, and the browser client.

**Architecture:** Extend the existing Go BFF with a narrowly scoped operator controller. A supervisor exposes typed component/action operations, invokes only fixed repository commands, and returns content-free state through a same-origin JSON API; embedded static assets render the five-card setup page. Secrets remain server-side and process ownership/dependency rules are enforced below HTTP.

**Tech Stack:** Go 1.26, `net/http`, `os/exec`, embedded HTML/CSS/ES modules, Node's built-in test runner, existing Make/Compose wrappers.

## Global Constraints

- Preserve all pre-existing dirty browser/audio files; do not revert or stage unrelated changes.
- Bind every operator surface to the existing literal loopback BFF only.
- Browser requests and responses contain no credential, environment value, command, path, URL, raw log, transcript, audio, provider payload, or database URL.
- The operator API accepts only exact component/action enums and no arbitrary argument fields.
- Mutation requests are same-origin JSON POST requests with a 4 KiB body cap and no trailing JSON.
- Each component has at most one operation in flight and every operation has a fixed deadline.
- Stop only demonstrably owned processes; never delete the PostgreSQL volume.
- Worker is optional for voice-only use; BFF Open is gated on Speech + Gateway + Client readiness.
- No provider calls, credentials, Docker startup, or destructive shutdown occur in automated tests.

---

### Task 1: Typed Supervisor and Dependency State Machine

**Files:**
- Create: `demo/live-client/operator_supervisor.go`
- Create: `demo/live-client/operator_supervisor_test.go`

**Interfaces:**
- Produces `type operatorComponent string` with exact values `speech`, `postgres`, `gateway`, `worker`, `client`.
- Produces `type operatorAction string` with exact values `start`, `stop`, `migrate`.
- Produces `type componentStatus struct { Component operatorComponent; State string; Code string; Ready bool; Busy bool }`.
- Produces `type operatorRunner interface { Run(context.Context, fixedOperation) error; Ready(context.Context, operatorComponent) bool; Owned(operatorComponent) bool }`.
- Produces `newOperatorSupervisor(operatorRunner, time.Duration) *operatorSupervisor`, `Snapshot(context.Context) []componentStatus`, and `Apply(context.Context, operatorComponent, operatorAction) componentStatus`.

- [ ] **Step 1: Write state-machine RED tests**

Add table tests proving exact enum rejection, Speech independent start, PostgreSQL start/migrate ordering, Gateway dependency on Speech+PostgreSQL+migration, Worker dependency on PostgreSQL/migration, Client dependency on Gateway, Worker not required by Client, and unsafe Speech/PostgreSQL stop conflicts.

```go
func TestOperatorSupervisorEnforcesDependenciesWithoutRequiringWorkerForVoice(t *testing.T) {
	runner := newFakeOperatorRunner()
	supervisor := newOperatorSupervisor(runner, 100*time.Millisecond)

	assert.Equal(t, "dependency_not_ready", supervisor.Apply(t.Context(), componentGateway, actionStart).Code)
	runner.ready[componentSpeech] = true
	runner.ready[componentPostgres] = true
	runner.migrated = true
	assert.Equal(t, "ready", supervisor.Apply(t.Context(), componentGateway, actionStart).State)
	assert.Equal(t, "ready", supervisor.Apply(t.Context(), componentClient, actionStart).State)
}
```

- [ ] **Step 2: Run the focused test and confirm RED**

Run: `go test ./demo/live-client -run '^TestOperatorSupervisor' -count=1`

Expected: FAIL because supervisor types and behavior do not exist.

- [ ] **Step 3: Implement the minimal synchronized state machine**

Use a mutex-protected map for in-flight state, one deadline context per action, exact dependency checks, and allowlisted codes: `ready`, `stopped`, `operation_in_progress`, `missing_configuration`, `dependency_not_ready`, `dependency_running`, `startup_timeout`, `process_exited`, `operation_failed`. Do not store raw errors in public state.

- [ ] **Step 4: Add hostile lifecycle tests**

Prove duplicate operations do not invoke the runner twice, timeouts clean only the newly owned operation, stopping an unowned process is rejected, and snapshots stay bounded/content-free when runner errors contain credential, command, path, transcript, and audio sentinels.

- [ ] **Step 5: Run Task 1 GREEN gates**

Run: `go test -race ./demo/live-client -run '^TestOperatorSupervisor' -count=10`

Expected: PASS with no race or leaked sentinel.

- [ ] **Step 6: Commit Task 1**

```bash
git add demo/live-client/operator_supervisor.go demo/live-client/operator_supervisor_test.go
git commit -m "feat: add demo operator supervisor"
```

### Task 2: Fixed Local Process Runner

**Files:**
- Create: `demo/live-client/operator_runner.go`
- Create: `demo/live-client/operator_runner_test.go`
- Modify: `demo/live-client/config.go`
- Modify: `demo/live-client/config_test.go`

**Interfaces:**
- Consumes Task 1 `operatorRunner` and `fixedOperation`.
- Produces `type operatorConfig struct` containing only server-startup configuration: enabled flag, private state directory, fixed repository root, fixed speech command/profile selected at process startup, and fixed readiness endpoints.
- Produces `newLocalOperatorRunner(operatorConfig, commandFactory, readinessProbe) (operatorRunner, error)`.
- Extends CLI with `-operator` and server-side-only fixed configuration flags/environment references; browser input cannot change them.

- [ ] **Step 1: Write runner RED tests with fake executables**

Create private temporary scripts that record argv/env names without values. Assert exact operations map only to reviewed entry points:

```text
speech start  -> fixed speech launcher/profile
postgres start/migrate/stop -> deploy/scripts/durable-compose.sh fixed verbs
gateway start/stop -> fixed Compose service operations
worker start/stop -> fixed Compose service operations
client -> already-running BFF ownership, never a child command
```

Assert no browser-derived field can enter argv and no secret value appears in the recorder file.

- [ ] **Step 2: Run runner tests and confirm RED**

Run: `go test ./demo/live-client -run '^TestLocalOperatorRunner' -count=1`

Expected: FAIL because the runner and operator config do not exist.

- [ ] **Step 3: Implement fixed command construction and ownership**

Use `exec.CommandContext` with constant argv assembled from validated startup configuration. Inherit the prepared server environment without serializing it. Redirect child stdout/stderr to the operator terminal, keep only PID/exit class internally, store PID files under a `0700` directory with `0600` files, validate process identity before stop, and use TERM then bounded KILL only for owned children.

- [ ] **Step 4: Implement bounded readiness probes**

Probe only fixed loopback endpoints with 50 ms connect and 100 ms total timeouts. Speech readiness must use the reviewed content-free pool/readiness endpoint configured at server startup; PostgreSQL readiness uses the fixed Compose health operation; gateway/worker/BFF use their fixed health endpoints. Return booleans/classes only.

- [ ] **Step 5: Add validation and privacy regressions**

Reject non-loopback readiness endpoints, relative/out-of-root executable/profile paths, writable-by-others operator state, unknown component/action pairs, and missing required server-side configuration. Prove error strings and public statuses exclude injected secrets, URLs, paths, commands, transcript, audio, and provider JSON.

- [ ] **Step 6: Run Task 2 GREEN gates**

Run: `go test -race ./demo/live-client -run '^(TestLocalOperatorRunner|TestParseConfig.*Operator)' -count=10`

Expected: PASS.

- [ ] **Step 7: Commit Task 2**

```bash
git add demo/live-client/operator_runner.go demo/live-client/operator_runner_test.go demo/live-client/config.go demo/live-client/config_test.go
git commit -m "feat: run fixed local demo services"
```

### Task 3: Same-Origin Operator HTTP API

**Files:**
- Create: `demo/live-client/operator_api.go`
- Create: `demo/live-client/operator_api_test.go`
- Modify: `demo/live-client/server.go`
- Modify: `demo/live-client/server_test.go`

**Interfaces:**
- Consumes Task 1 `operatorSupervisor`.
- Adds `GET /api/operator/status` returning `{ "components": [...] }`.
- Adds `POST /api/operator/action` accepting exactly `{ "component": "...", "action": "..." }`.
- Adds `GET /setup`, `/setup.css`, and `/setup.mjs` embedded routes.

- [ ] **Step 1: Write API RED tests**

Assert exact success schemas and that mutations reject missing/foreign Origin, wrong method, non-JSON media type, bodies over 4 KiB, unknown/trailing fields, duplicate JSON keys, trailing JSON, invalid enums, and operator-disabled configuration.

- [ ] **Step 2: Run API tests and confirm RED**

Run: `go test ./demo/live-client -run '^TestOperatorAPI' -count=1`

Expected: FAIL with missing routes/types.

- [ ] **Step 3: Implement strict handlers**

Use `http.MaxBytesReader`, a duplicate-key-safe exact JSON decoder, exact same-origin comparison to `cfg.Origin`, `Cache-Control: no-store`, and stable status mapping: 200 snapshot/success, 400 malformed, 403 origin, 409 dependency/in-flight, 422 invalid operation, 503 disabled/unavailable. Return only `component`, `state`, `code`, `ready`, and `busy`.

- [ ] **Step 4: Add security/header tests**

Require CSP to keep `connect-src 'self'`, setup assets to be no-store, no CORS wildcard, no bearer/provider/database/environment values in page/API/header bodies, and `/setup` unavailable on non-loopback configuration (which config validation already rejects).

- [ ] **Step 5: Run Task 3 GREEN gates**

Run: `go test -race ./demo/live-client -run '^(TestOperatorAPI|TestSetupAssets|TestStaticAssets)' -count=10`

Expected: PASS.

- [ ] **Step 6: Commit Task 3**

```bash
git add demo/live-client/operator_api.go demo/live-client/operator_api_test.go demo/live-client/server.go demo/live-client/server_test.go
git commit -m "feat: expose loopback demo operator API"
```

### Task 4: Accessible Setup Page

**Files:**
- Create: `demo/live-client/web/setup.html`
- Create: `demo/live-client/web/setup.css`
- Create: `demo/live-client/web/setup.mjs`
- Create: `demo/live-client/web/setup.test.mjs`

**Interfaces:**
- Consumes Task 3 status/action JSON.
- Produces five fixed cards with `data-component`, action buttons, content-free status text, a global live announcement, and an `/` Open voice demo link.

- [ ] **Step 1: Write browser-state RED tests**

With injected fetch/timer/DOM ports, assert fixed component rendering, two-second bounded polling with one request in flight, button disablement while busy, dependency-aware action enablement, Worker optional label, error-code copy, and Open voice demo enabled only for Speech+Gateway+Client readiness.

- [ ] **Step 2: Run browser tests and confirm RED**

Run: `node --test demo/live-client/web/setup.test.mjs`

Expected: FAIL because setup module/assets do not exist.

- [ ] **Step 3: Implement the minimal controller and semantic markup**

Use real buttons, headings, lists, `aria-live="polite"`, visible keyboard focus, reduced-motion-safe styling, no inline script/style, no external asset/font, no storage/cookie/URL parameter, and `fetch(..., {credentials: "same-origin"})`. Map only allowlisted codes to fixed human text.

- [ ] **Step 4: Add temporal/error browser tests**

Cover double-click suppression, late poll not overwriting a newer action response, abort on page teardown, network failure recovery, malformed response fail-closed behavior, and no rendering of injected response strings outside exact enums.

- [ ] **Step 5: Run Task 4 GREEN gates**

Run: `node --test demo/live-client/web/setup.test.mjs`

Expected: PASS.

Run: `go test -race ./demo/live-client -run '^(TestSetupAssets|TestOperatorAPI)' -count=1`

Expected: PASS.

- [ ] **Step 6: Commit Task 4**

```bash
git add demo/live-client/web/setup.html demo/live-client/web/setup.css demo/live-client/web/setup.mjs demo/live-client/web/setup.test.mjs
git commit -m "feat: add browser demo setup controls"
```

### Task 5: Startup Wiring, Documentation, and Final Verification

**Files:**
- Modify: `demo/live-client/main.go`
- Modify: `demo/live-client/README.md`
- Modify: `README.md`
- Modify: `docs/demo/realtime-browser-client.md`
- Modify: `deploy/scripts/demo-wizard.sh`
- Modify: `deploy/scripts/test-demo-wizard.sh`
- Modify: `Makefile`
- Test: `demo/live-client/docs_test.go`

**Interfaces:**
- Wires operator configuration/supervisor into `demoHandler` only when explicitly enabled.
- Adds wizard choice **Browser control panel** that starts the BFF/operator page from existing server-side configuration and prints `http://127.0.0.1:3000/setup`.

- [ ] **Step 1: Write wiring/docs RED tests**

Require the wizard/menu and quick-start docs to explain: credentials remain terminal/server-side, `/setup` controls fixed services, Worker is optional for voice-only, dependency stop order, no database volume deletion, and `/setup` is local development only. Assert the wizard passes no secret in argv and prints the setup URL.

- [ ] **Step 2: Run tests and confirm RED**

Run: `go test ./demo/live-client -run '^(TestOperatorMain|TestRealtimeBrowser.*ControlPanel)' -count=1`

Run: `./deploy/scripts/test-demo-wizard.sh`

Expected: FAIL because startup/menu/docs wiring is absent.

- [ ] **Step 3: Implement startup and concise operator guidance**

Construct the runner/supervisor once in `run`, attach it to the handler, stop owned children during graceful BFF shutdown, and add the wizard/menu/docs entry. Do not make operator mode the production/default path; require the explicit local flag.

- [ ] **Step 4: Run focused integration gates**

Run: `go test -race -count=1 ./demo/live-client`

Run: `node --test demo/live-client/web/*.test.mjs`

Run: `./deploy/scripts/test-demo-wizard.sh`

Expected: PASS.

- [ ] **Step 5: Run repository verification**

Run: `make test-demo-live-client`

Run: `go test -race -count=1 ./...`

Run: `go vet ./...`

Run: `go build ./...`

Run: `go mod tidy -diff`

Run: `make verify-deploy-static`

Run: `bash -n deploy/scripts/demo-wizard.sh deploy/scripts/test-demo-wizard.sh`

Run: `git diff --check`

Expected: PASS. Loopback test sandbox denials may be rerun with local-listener permission; do not classify them as product failures without reproducing outside that restriction.

- [ ] **Step 6: Perform privacy and scope audit**

Search the changed files for storage/cookie/query/hash use, arbitrary command execution, raw `os.Environ`, response inclusion of errors/argv/env/paths/URLs, credential names/values, transcript/audio/provider-body logging, public binds, tunnels, and destructive volume operations. Review every match; negative tests and symbolic environment names are allowed only where required.

- [ ] **Step 7: Commit Task 5**

```bash
git add demo/live-client/main.go demo/live-client/README.md README.md docs/demo/realtime-browser-client.md deploy/scripts/demo-wizard.sh deploy/scripts/test-demo-wizard.sh Makefile demo/live-client/docs_test.go
git commit -m "docs: operate browser demo control panel"
```

## Final Acceptance

- `/setup` shows Speech, PostgreSQL, Gateway, Worker, and Client independently.
- Fixed Start/Stop/Migrate controls respect dependency and ownership rules.
- Voice-only use does not require Worker; durable async tools clearly do.
- Open voice demo is available only when Speech, Gateway, and Client are ready.
- Browser/API never receive secrets, raw commands, paths, URLs, logs, transcripts, audio, provider payloads, or database URLs.
- Duplicate, late, malformed, cross-origin, oversized, and concurrent requests fail closed.
- Existing realtime audio, tool-policy, wizard, deployment, race, and privacy tests remain green.
