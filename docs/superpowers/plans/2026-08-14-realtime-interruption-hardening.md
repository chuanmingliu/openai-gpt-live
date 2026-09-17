# Realtime Interruption Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make barge-in, interruption, durable approvals, retry scheduling, and tombstoned delivery fail closed without blocking the realtime reactor or terminating an otherwise healthy voice call.

**Architecture:** The reactor remains the sole state owner but performs no synchronous client, provider, database, or coordinator I/O. It emits bounded effects and consumes typed completions on later turns; the provider adapter preserves event order while continuing to service correlation requests under downstream backpressure. Durable work is app-owned, while worker retry and delivery-store transitions remain transactionally explicit.

**Tech Stack:** Go 1.26, WebSocket realtime adapter, PostgreSQL/pgx, existing session reactor/effect queues, existing gateway coordinators and telemetry.

## Global Constraints

- The product is realtime: speech-start must advance the authoritative response fence without waiting for unrelated I/O, and no stale-epoch audio may be written afterward.
- The reactor must not synchronously perform or wait for WebSocket, provider, database, or durable-coordinator I/O.
- Every new queue, retry, wait, and completion path must be bounded, content-free in telemetry, and fail closed.
- Preserve provider event ordering and the existing response epoch/fence contract; do not drop protocol events to make progress.
- Accepted durable approval decisions must survive session disconnect and remain idempotent.
- Preserve the approved two-phase privacy deletion policy. Do not change retention hard-delete eligibility for review item 8.
- Do not edit or stage the user's existing browser/audio files under `demo/live-client/web/` or `docs/research/`.
- Use RED-first behavior tests for every finding, then focused race tests, full race tests, vet/build/tidy/static checks, and an independent task review.

---

### Task 1: Nonblocking realtime reactor and adapter correlation progress

**Files:**
- Modify: `internal/realtime/s2sws/connector.go`
- Modify: `internal/realtime/s2sws/connector_test.go`
- Create: `internal/realtime/s2sws/connector_internal_test.go` for no-network tests of the unexported mapping loop
- Modify: `internal/session/manager.go`
- Modify: `internal/session/manager_test.go`
- Modify: `internal/session/presentation_delivery_test.go`
- Modify if required for typed completions: `internal/session/reactor.go`, `internal/session/reactor_test.go`

**Interfaces:**
- Consumes: existing `responseFence`, reactor inbox, effect types, voice/client writer queues, adapter correlation requests.
- Produces: ordered adapter delivery that services correlations while downstream event delivery is backpressured; effect completion inputs that reconcile client/provider/durable I/O on later reactor turns; offer completion tokens independent of the caller wait context.

- [ ] **Step 1: Write failing adapter progress tests**

Add a no-network test that fills/blocks the public adapter event consumer, submits additional mapped events, then submits a cancel/bind correlation. Assert correlation completes within the test deadline, mapped events retain FIFO order, and no event is silently dropped. The test must fail against the current blocking `mappingLoop.deliver` path.

- [ ] **Step 2: Write failing manager/reactor latency tests**

Gate an earlier client visual write and provider send, deliver `speech_started`, and assert the response fence advances before either gate is released. Add a composition test proving a saturated adapter event path cannot deadlock barge-in cancel. Assert stale audio is rejected at the writer boundary. Because the current synchronous cancel path implicitly serializes a queued client `response.create`, add a regression with a blocked cancel send plus queued create and preserve deterministic processing through the asynchronous conversion; the create must not be lost or terminate the healthy session.

- [ ] **Step 3: Implement bounded adapter delivery arbitration**

Replace blocking mapped-event delivery with a bounded FIFO owned by the mapping loop. When downstream events are full, stop taking additional socket events at the configured bound but continue selecting correlation/control requests and downstream FIFO sends. Preserve exact FIFO ordering, existing per-response bounds, and teardown semantics.

- [ ] **Step 4: Make reactor effects asynchronous and completion-correlated**

Remove synchronous client-write/provider-send/durable-dispatch waits from the reactor loop. Enqueue bounded effects, then feed typed success/failure completion inputs back to the reactor. Cancellation failure remains session-fatal through supervision, but the first speech-start turn must not wait for the provider cancel acknowledgment.

- [ ] **Step 5: Make handoff offer timeout local and shorten directory locking**

Snapshot the runtime under `activeMu`, release the lock, and recheck the runtime acceptance generation at enqueue. Bound only offer admission/caller wait; never attach the offer deadline to apply/dispatch and never quiesce the session solely because an offer timed out. Cursor advancement still requires the correlated successful visual write; an expired attempt remains replayable with the same stable event identity. Credential rotation and client writes must run without `activeMu` held.

- [ ] **Step 6: Run focused and race verification**

Run the exact new regressions repeatedly under `-race`, then the complete `internal/session` and `internal/realtime/s2sws` suites. Listener-backed tests may be rerun with loopback permission; do not weaken them for the sandbox.

- [ ] **Step 7: Commit and report**

Commit only Task 1 files and record the RED causes, completion contracts, queue bounds, race commands, and any environment-only limitations.

---

### Task 2: Turn fencing, implicit-response suppression, and durable approval ownership

**Files:**
- Modify: `internal/session/reactor.go`
- Modify: `internal/session/reactor_test.go`
- Modify: `internal/session/manager.go`
- Modify: `internal/session/manager_test.go`
- Modify: `internal/gateway/app.go`
- Modify: `internal/gateway/app_test.go`
- Modify if required: gateway coordinator files/tests that own durable approval work
- Modify: `internal/realtime/s2sws/connector.go`, `internal/realtime/s2sws/connector_test.go` only for pending-correlation cleanup required by the implicit-response test

**Interfaces:**
- Consumes: current uncommitted input item/revision state, `UserSpeaking`, response bindings, app-owned coordinator pattern.
- Produces: stale-input transcript rejection; bind-and-cancel/drop handling for implicit responses received while speaking; bounded app-owned approval resolution with optional typed live-session completion.

- [ ] **Step 1: Write failing stale-input transcript tests**

Cover `speech_started(B)` followed by both delta and completion for old item A. Assert no client event, no active-turn replacement, no sequence allocation, and no transcript persistence effect. Preserve the provider-compatibility path only when no newer provider item has been observed.

- [ ] **Step 2: Write failing implicit-response tests**

Cover an implicit `response.created` reachable from idle/terminal while `UserSpeaking`. Assert it cannot publish or stream over the user, does not leak adapter pending/unbound budget, and is reconciled with an explicit bind then cancel/drop path. Preserve the established provider contract that cancel-before-response-ID uses an empty response ID. Do not simply delete the oldest pending explicit epoch: first prove the provider ordering contract; if the provider can suppress `response.created`, use a bounded explicitly correlated cancelled-pending tombstone that can safely consume late created/done frames.

- [ ] **Step 3: Implement current-item and speaking fences**

Keep a current provider input-item generation in ephemeral reactor state. Reject a mismatched old item while a newer uncommitted turn exists. Add the `UserSpeaking` guard to implicit response handling without leaving the adapter response permanently unbound.

- [ ] **Step 4: Write failing disconnect-after-approval-accept test**

Block durable approval resolution after the manager accepts it, disconnect the live session, release the store/coordinator, and assert exactly one durable resolution completes. Reconnect/recovery must observe the resolved state; stale UX completion delivery may be ignored.

- [ ] **Step 5: Move approval resolution to bounded app ownership**

Use the existing app-owned coordinator pattern with a bounded queue and bounded request deadline. Carry trusted scope/actor, idempotency key, decision, and resolution time. Do not inherit `sessionCtx`; send a typed completion to the matching live incarnation only for UX/high-water bookkeeping.

- [ ] **Step 6: Run focused/race tests, commit, and report**

Run reactor, manager, gateway, and adapter focused tests repeatedly under `-race`, then full affected packages. Commit only Task 2 files and report queue/deadline/idempotency behavior.

---

### Task 3: Retry scheduling cannot strand work or terminate the runner

**Files:**
- Modify: `internal/worker/retry.go`
- Modify: `internal/worker/retry_test.go`
- Modify: `internal/worker/runner.go`
- Modify: `internal/worker/runner_test.go`
- Modify: `internal/worker/runner_integration_test.go`
- Modify if the transaction clock belongs there: `internal/store/postgres/leases.go`, `internal/store/postgres/leases_integration_test.go`

**Interfaces:**
- Consumes: retry policy delay/jitter, store transaction clock, leased handoff mutation APIs.
- Produces: persisted retry readiness strictly after the authoritative store time; stable terminal failure when a known pre-mutation retry request is invalid; no process-level runner exit for a job-local retry failure.

- [ ] **Step 1: Write failing zero-jitter tests**

At unit level, prove zero/negative jitter still yields a persistable future instant. At PostgreSQL integration level, drive a retryable provider error with zero jitter and assert the runner stays alive, lease is released, `ready_at` is strictly after transaction time, and the job is reacquirable.

- [ ] **Step 2: Write failing retry-persistence fallback test**

Force a known pre-mutation semantic scheduling error and assert the runner performs one stable `Fail` mutation instead of returning a fatal runner error or leaving a running lease. Preserve fail-closed behavior for infrastructure-uncertain acknowledgments where a contradictory second mutation is unsafe.

- [ ] **Step 3: Implement transaction-safe readiness and fallback**

Clamp readiness using the authoritative store transaction clock to at least one supported timestamp tick in the future, keeping row and event payload identical. Classify job-local semantic scheduling failure separately from runner-integrity failure and terminally fail the handoff when mutation certainty permits.

- [ ] **Step 4: Run unit/race/integration tests, commit, and report**

Run worker tests repeatedly under `-race`, tagged PostgreSQL integration with an explicitly supplied database when available, and tagged compile otherwise. Commit only Task 3 files and record any runtime authority gap honestly.

---

### Task 4: Tombstoned delivery stops without cursor-zero replay

**Files:**
- Modify: `internal/store/postgres/delivery_cursors.go`
- Modify: `internal/store/postgres/delivery_cursors_integration_test.go`
- Modify: `internal/store/postgres/events.go`
- Modify: relevant events integration tests
- Modify: `internal/handoff/event_pump.go`
- Modify: `internal/handoff/event_pump_test.go`

**Interfaces:**
- Consumes: authorized live conversation identity, delivery cursor, event stream, event-pump subscription registry.
- Produces: `ErrNotFound` for missing/deleted/wrong-owner conversations at cursor load/read/advance; zero only for an authorized live conversation with no cursor; exact subscription removal on terminal not-found.

- [ ] **Step 1: Write failing store race-boundary tests**

Test deleted/wrong-owner/missing conversation behavior for cursor load and event read. Add tombstone races between cursor load, event read, and cursor advance. Assert no historical event is returned after tombstoning and only a live authorized conversation without a cursor returns zero.

- [ ] **Step 2: Write failing event-pump removal tests**

At load and read boundaries, return `ErrNotFound`; assert the exact subscription is removed and no visual offer is emitted. At the advance boundary, the visual has already been confirmed by contract, so assert terminal unsubscribe and no repeated offer. Unrelated subscriptions must continue, while generic transient failures remain retryable without moving the cursor.

- [ ] **Step 3: Implement live-conversation store predicates and pump termination**

Drive cursor load from the authorized conversation and left-join the cursor. Enforce the same live-conversation predicate in event reads. Teach the pump to unregister on `ErrNotFound` while preserving existing retry semantics for transient failures.

- [ ] **Step 4: Run unit/race/integration tests, commit, and report**

Run handoff and store tests under `-race`; run tagged PostgreSQL tests when an explicit database is available and tagged compile otherwise. Do not modify retention deletion semantics.

---

### Task 5: Whole-wave verification and independent review

**Files:**
- Create: `.superpowers/sdd/2026-08-14-realtime-interruption-hardening/final-report.md`
- Modify only if a verified review finding requires it: files from Tasks 1-4

- [ ] **Step 1: Run full verification**

Run `go test -race -count=1 ./...`, repeated focused realtime/worker/delivery regressions, `go vet ./...`, `go build ./...`, `go mod tidy -diff`, tagged integration/chaos/load/soak compile/discovery, `make verify-deploy-static`, `gofmt`, `git diff --check`, and privacy scans. Add two deterministic bounded stress tests rather than one opaque soak: (A) 128 adapter cycles within 10 seconds covering full downstream events, explicit/implicit create, empty-ID cancel-before-created under the declared provider contract, streaming cancel, exact FIFO/correlation progress, per-cycle pending/unbound budget recovery, and clean goroutine exit; (B) 100 manager cycles within 15 seconds covering pending/streaming interruption, old audio/transcript/done around speech start, periodically blocked client writes and cancel sends, double barge-in, monotonic fences, one terminal per response, no stale media/transcript, bounded queue high-water, and a final clean response proving continued usability. Prefer explicit counters and wait groups over flaky global goroutine counts; run the focused stress repeatedly under `-race`. Rerun loopback-only sandbox failures with permission; never label an unexecuted provider/database runtime as passed.

- [ ] **Step 2: Independent whole-wave review**

Review all eight valid findings plus the partial implicit-response finding against current code. Explicitly confirm item 8 remained unchanged. Require exact P0/P1/P2 counts and verify no synchronous I/O remains in the reactor, all new queues/deadlines are bounded, and no stale transcript/audio can cross the interruption fence.

- [ ] **Step 3: Resolve findings and write final report**

Apply only verified findings through RED/GREEN fix rounds, rerun affected and full gates, and write the final evidence report with commits, commands, results, external gaps, and preserved unrelated worktree changes.
