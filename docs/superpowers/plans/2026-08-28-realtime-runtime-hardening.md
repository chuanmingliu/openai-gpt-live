# Realtime Runtime Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the remaining realtime deadlock, liveness, correlation, input-boundary, resource-retention, and profile-authority risks while preserving the existing barge-in and two-turn voice contracts.

**Architecture:** Manager/runtime locking follows one documented order with no channel or I/O waits under directory locks. Provider-VAD and manual input use one generation-aware media boundary. Provider writes have independent deadlines and cancellation uses a progress-safe control lane. Adapter/session queues use weighted byte admission. The admitted profile becomes the immutable prompt/tool/handoff authority. Public failure surfaces expose only stable internal classes.

**Tech Stack:** Go 1.26, existing realtime protocol and `s2sws` adapter, `internal/session` single-reactor model, race detector, contract/load tagged tests, PostgreSQL integration tests for durable privacy.

**Spec:** `docs/superpowers/specs/2026-08-27-production-ready-backend-design.md`

## Global Constraints

- Preserve the authoritative barge-in order: advance response epoch and reject stale output before cancel/flush work.
- Browser playback clearing is secondary UX; it must never become the server's cancellation authority.
- Do not reintroduce synchronous provider, browser, safety, template, or durable I/O on the sole reactor.
- Provider-confirmed turn mode remains immutable for one session. Do not infer commit ownership from arrival order, FIFO assumptions, `previous_item_id`, or mutable current-turn state.
- Manual mode permits one outstanding commit; provider-VAD mode rejects client commit.
- Every queue has both count and retained-byte bounds. Reserve before cloning or decoding and release on every dequeue, drop, cancellation, and close path.
- Detailed provider/database/internal errors stay private. Client events and WebSocket close reasons are stable and content-free.
- Continuous interruption, two-turn recovery, reconnect, and profile snapshot tests are mandatory release gates, not optional load tests.
- Consume the backend-integration plan's strict Go/Node zero-skip runners. Every tagged contract/integration/load command requires its external prerequisite explicitly and runs through the wrapper; an absent or unreachable provider/database is a failure, never a skip. Repository-wide Go tests select only test-bearing packages through a successful standalone `go list`, while `go build ./...` covers all packages.
- Implement tasks in order. Later tasks rely on the lock, input-boundary, and writer contracts established earlier.
- Cross-plan order is strict: finish the complete backend-integration plan first; finish this complete runtime plan second; only then begin deployment/security. This prevents concurrent edits to `cmd/live-worker`, `internal/realtime/s2sws/connector.go`, and `internal/worker/runner.go`.

---

## Task 1: Eliminate the Manager/Runtime Lock Inversion

**Files:**

- Modify: `internal/session/manager.go`
- Modify: `internal/session/presentation_delivery_test.go`
- Modify: `internal/session/manager_test.go`

**Interfaces:**

- Consumes: `func (m *Manager) OfferHandoffEvent(ctx context.Context, scope handoff.Scope, event handoff.Event) error`, `func (m *Manager) Reconnect(parent context.Context, request ReconnectRequest, client ClientConn) error`, and `func (r *sessionRuntime) atTurnBoundary() bool`.
- Produces: package-private `Config.offerRevalidationGate func() <-chan struct{}` for deterministic scheduling only; production behavior is unchanged when it is nil. Every nested acquisition obeys `presentationMu -> Manager.activeMu -> sessionRuntime.mu`, and `reconnectMu` is never held across a directory lock, channel wait, or I/O.

- [ ] Add the lock-order comment next to `sessionRuntime`:

```text
presentationMu -> Manager.activeMu -> sessionRuntime.mu
reconnectMu is never held across directory locks, channel waits, or I/O
```

- [ ] Add `offerRevalidationGate func() <-chan struct{}` to `Config` and invoke it immediately before `OfferHandoffEvent` revalidates runtime ownership.
- [ ] Write `TestOfferReconnectLockOrderCompletes` so an offer pauses at `offerRevalidationGate` while reconnect reaches the directory boundary.
- [ ] Write `TestSameScopeReplacementRollbackLockOrderCompletes` with the same deterministic two-owner schedule.
- [ ] Write `TestShutdownUnregisterLockOrderCompletes` with the same deterministic two-owner schedule.
- [ ] Run the three tests at count 100 and retain the timeout/deadlock output as RED evidence.

```sh
go test -race ./internal/session -run \
  'Test(OfferReconnectLockOrder|SameScope.*LockOrder|Shutdown.*LockOrder)' -count=100
```

- [ ] Refactor offer ownership revalidation to acquire `presentationMu`, then `activeMu`, then `sessionRuntime.mu`.
- [ ] Move inbox admission and completion waits after all three ownership locks are released.
- [ ] Keep the generation and current-runtime checks in one `activeMu`/`sessionRuntime.mu` critical section so stale offers remain replayable without publishing an effect.
- [ ] Update `Reconnect`, `register`, `registerReplacement`, rollback, unregister, and shutdown to follow the same order.
- [ ] Add a local safety comment at every intentionally non-nested acquisition explaining which inverse edge is impossible.
- [ ] Run the focused ownership race set.

```sh
go test -race ./internal/session -run \
  'Test(OfferTimeout|ReplacementFences|FailedInitialSameScope|FailedReconnectSameScope|ManagerReconnect)' -count=25
```

- [ ] Run the complete session race suite.

```sh
go test -race -count=1 ./internal/session
```

- [ ] Commit the lock-order fix.

```sh
git add internal/session/manager.go internal/session/presentation_delivery_test.go internal/session/manager_test.go
git commit -m 'fix: linearize realtime manager lock ownership'
```

---

## Task 2: Make Provider Commit the Local Media Boundary

**Files:**

- Modify: `internal/session/manager.go`
- Modify: `internal/session/manager_test.go`
- Create: `internal/session/provider_transport.go`
- Create: `internal/session/provider_transport_test.go`
- Modify: `internal/session/input.go`
- Modify: `internal/session/effect.go`
- Modify: `internal/session/reactor.go`
- Modify: `internal/session/reactor_test.go`
- Modify: `internal/realtime/voice.go`
- Modify: `internal/realtime/s2sws/connector.go`
- Modify: `internal/realtime/s2sws/connector_internal_test.go`
- Modify: `internal/realtime/s2sws/commit_ack_test.go`
- Modify: `internal/realtime/s2sws/input_turn_mode_test.go`
- Modify: `internal/protocol/codec.go`
- Modify: `internal/protocol/codec_test.go`

**Interfaces:**

- Consumes: `func protocol.NewPCMFramer() *protocol.PCMFramer`, `func (f *protocol.PCMFramer) Push(pcm []byte) ([][]byte, error)`, `func (f *protocol.PCMFramer) Flush() ([]byte, error)`, `realtime.Connector.Connect(context.Context, realtime.OpenRequest) (realtime.Session, error)`, `realtime.Event.ItemID string`, and immutable `realtime.InputTurnMode`.
- Produces: `type realtime.InputCommitToken uint64`, `type realtime.ProviderConnectionGeneration uint64`, `const initialProviderConnectionGeneration realtime.ProviderConnectionGeneration = 1`, `var ErrProviderConnectionGenerationExhausted = errors.New("provider connection generation exhausted")`, `var ErrStaleProviderConnection = errors.New("stale provider connection")`, `func nextProviderConnectionGeneration(current realtime.ProviderConnectionGeneration) (realtime.ProviderConnectionGeneration, error)`, `realtime.OpenRequest.ProviderConnectionGeneration realtime.ProviderConnectionGeneration`, `realtime.Command.InputCommitToken realtime.InputCommitToken`, `realtime.Event.ProviderConnectionGeneration realtime.ProviderConnectionGeneration`, and `realtime.Event.InputAudioGeneration uint64`.
- Produces: `func (f *protocol.PCMFramer) Discard()`, `func newInputMediaBoundary(mode realtime.InputTurnMode) (*inputMediaBoundary, error)`, `func (b *inputMediaBoundary) Append(pcm []byte) ([]realtime.Command, error)`, `func (b *inputMediaBoundary) BindProviderItem(itemID string) (uint64, error)`, `func (b *inputMediaBoundary) ProviderCommit(itemID string) (uint64, error)`, `func (b *inputMediaBoundary) BeginManualCommit() (realtime.InputCommitToken, []realtime.Command, error)`, `func (b *inputMediaBoundary) BindManualCommit(token realtime.InputCommitToken, commitSequence uint64) error`, `func (b *inputMediaBoundary) AbortManualCommit(token realtime.InputCommitToken)`, `func (b *inputMediaBoundary) ManualCommitAcknowledged(itemID string, commitSequence uint64) (uint64, error)`, and `func (b *inputMediaBoundary) Partial() bool`.
- Produces: `ConnectedInput.ProviderConnectionGeneration realtime.ProviderConnectionGeneration`, `ClientInput.InputCommitToken realtime.InputCommitToken`, `type ProviderConnectionRotatedInput struct { Previous realtime.ProviderConnectionGeneration; Current realtime.ProviderConnectionGeneration }`, `func (r *Reactor) applyProviderConnectionRotated(input ProviderConnectionRotatedInput) ([]Effect, error)`, `type ProviderRotationRequestedEffect struct { Expected realtime.ProviderConnectionGeneration; RetainedInputItems int }`, and `Effect.ProviderRotation *ProviderRotationRequestedEffect`. The session constants are `providerInputItemHistoryLimit = 1024` and `providerInputItemHistoryRotateAt = 960`; an authoritative commit records pending rotation once the current provider generation retains 960 input-item tombstones, and the reactor emits the request on the first subsequent clean boundary before a 1,025th entry.
- Produces: package-private `type providerTransportLease struct { Generation realtime.ProviderConnectionGeneration; Session realtime.Session }`, `func newProviderTransportOwner(initial providerTransportLease) (*providerTransportOwner, error)`, `func (owner *providerTransportOwner) Run(ctx context.Context, admit func(realtime.ProviderConnectionGeneration, realtime.Event) error) error`, `func (owner *providerTransportOwner) Send(ctx context.Context, expected realtime.ProviderConnectionGeneration, command realtime.Command) error`, `func (owner *providerTransportOwner) Rotate(ctx context.Context, expected realtime.ProviderConnectionGeneration, replacement providerTransportLease) error`, and `func (owner *providerTransportOwner) Close(ctx context.Context) error`. The owner has one joined rotation loop, restarts one generation-scoped receive lane per lease, and never holds its state lock across `Session.Send`, `Session.Receive`, or `Session.Close`.
- Produces: package-private `runState.inputAdmissionMu sync.Mutex`, `func (s *runState) admitAudioAppend(ctx context.Context, pcm []byte) error`, `func (s *runState) admitManualCommit(ctx context.Context, event protocol.InputAudioBufferCommitEvent) error`, `func (s *runState) rotateProviderConnection(ctx context.Context, expected realtime.ProviderConnectionGeneration) error`, and a manager-owned generation fence that accepts events only when `event.ProviderConnectionGeneration` equals the currently published provider connection.

- [ ] Write `TestProviderVADCommitDiscardsExactGenerationTail` with a 959-byte first-turn tail and assert the second turn contains none of those bytes.
- [ ] Write `TestProviderVADCommitClearsExactGenerationForReconnect` and require partial-input/reconnect eligibility to clear at the matching authoritative `input_audio_buffer.committed` event even when transcription completes later.
- [ ] Write `TestProviderVADDuplicateCommitCannotClearNewGeneration` with two provider item IDs and a late duplicate commit for the first item.
- [ ] Write `TestProviderVADClientCommitIsNonfatalAndWritesNothing` and assert no `CommandCommitAudio` reaches the adapter.
- [ ] Write `TestManualBeginFreezesAndAdvancesExactGeneration` and race `Append` against `BeginManualCommit`; later audio must enter only the advanced generation.
- [ ] Write `TestManualCommitTailPrecedesCommitDispatch` and assert every returned tail append is dispatched before `CommandCommitAudio`.
- [ ] Write `TestManualCommitBindRequiresExactTokenAndPositiveSequence` and reject zero, stale, mismatched, and duplicate token/sequence pairs.
- [ ] Write `TestManualCommitAcknowledgementRequiresExactPendingSequence` and reject zero, stale, or mismatched adapter acknowledgement sequences without clearing partial/newer generation state.
- [ ] Write `TestManualCommitAdmissionAndDispatchFailuresReleaseToken` for tail admission failure, reactor rejection, command send failure, cancellation, and close.
- [ ] Add table cases for 2-, 959-, 960-, and 962-byte appends to the provider-VAD and manual tests.
- [ ] Write `TestPCMFramerDiscardResetsPartialAndIsIdempotentWhenEmpty` in `internal/protocol/codec_test.go`.
- [ ] Write `TestProviderConnectionRotatesBeforeInputHistoryLimit` and observe one reactor `ProviderRotationRequestedEffect` and one manager-owned rotation before the 1,024-entry hard limit.
- [ ] Write `TestProviderConnectionGeneration1025VADTurnsAnd1026Succeeds`; complete 1,025 provider-VAD turns, join at least one clean-boundary rotation, and complete turn 1,026 on the replacement connection.
- [ ] Write `TestLateTurnOneDuplicateBeforeRotationCannotMutateNewerTurn` and `TestLateTurnOneDuplicateAfterRotationCannotMutateNewerTurn`; neither duplicate may clear or bind the current input generation.
- [ ] Write `TestProviderConnectionGenerationStampsEveryMappedEvent` for session, input, output, and provider-error events on two adapter connections.
- [ ] Write `TestInitialProviderConnectionGenerationIsOne`, `TestBrowserReconnectAdvancesProviderConnectionGeneration`, and `TestProviderConnectionGenerationOverflowFailsClosed` for `math.MaxUint64`.
- [ ] Write `TestProviderTransportOwnerRotateRestartsReceiveWithoutEndingBrowserSession` and close the old session while the replacement completes another input/output turn.
- [ ] Write `TestProviderTransportOwnerRejectsStaleSendLease` and race an old-generation send against publication of the replacement.
- [ ] Write `TestProviderTransportOwnerJoinsOldReceiveBeforeReleasingLease` and `TestProviderTransportOwnerCloseJoinsRotationLoop` with deterministic receive/close gates.
- [ ] Write `TestProviderTransportRotationFenceDoesNotBlockReceiveJoin`; the admission callback must reject a fenced old event immediately rather than waiting on paused ingress.
- [ ] Run the boundary and rotation tests before production edits; retain the compile/behavior failures as RED evidence.

```sh
go test -race ./internal/session -run \
  'Test(ProviderVAD|Manual(Begin|Commit)|LateTurnOne)' -count=100
go test -race ./internal/session -run \
  'Test(ProviderConnection|InitialProviderConnection|BrowserReconnectAdvancesProviderConnection|ProviderTransportOwner)' -count=1
go test -race ./internal/realtime/s2sws -run \
  'Test(ProviderConnection|LateTurnOne|ManualCommit)' -count=25
go test -race ./internal/protocol -run 'TestPCMFramerDiscard' -count=100
```

Expected: FAIL because the media boundary, two-phase token, initial-generation/transport owner, rotation effect/fence, and `PCMFramer.Discard` do not exist yet.

- [ ] Add `PCMFramer.Discard` with reset-after-partial and idempotent-empty behavior.
- [ ] Add `InputAudioGeneration uint64` to `realtime.Event`; adapters never serialize that field.
- [ ] Add `InputCommitToken` to `session.ClientInput`/`realtime.Command` ownership without serializing it to the provider.
- [ ] Replace the `clientReader`-local framer and detached `partialInput` atomic with one `inputMediaBoundary` owned by `runState`:

```go
type inputMediaBoundary struct {
    mu                       sync.Mutex
    mode                     realtime.InputTurnMode
    generation               uint64
    nextManualToken          realtime.InputCommitToken
    framer                   protocol.PCMFramer
    partial                  bool
    providerItemGenerations  map[string]uint64
    manualPendingToken       realtime.InputCommitToken
    manualPendingSequence    uint64
    manualPendingGeneration  uint64
}

func (b *inputMediaBoundary) Append([]byte) ([]realtime.Command, error)
func (b *inputMediaBoundary) BindProviderItem(itemID string) (uint64, error)
func (b *inputMediaBoundary) ProviderCommit(itemID string) (uint64, error)
func (b *inputMediaBoundary) BeginManualCommit() (realtime.InputCommitToken, []realtime.Command, error)
func (b *inputMediaBoundary) BindManualCommit(realtime.InputCommitToken, uint64) error
func (b *inputMediaBoundary) AbortManualCommit(realtime.InputCommitToken)
func (b *inputMediaBoundary) ManualCommitAcknowledged(itemID string, commitSequence uint64) (uint64, error)
func (b *inputMediaBoundary) Partial() bool
```

- [ ] In `EventSpeechStarted` handling, call `BindProviderItem(event.ItemID)` and stamp its result into `event.InputAudioGeneration` before reactor admission.
- [ ] Make duplicate binding of the same provider item idempotent, reject a conflicting duplicate, and fail closed before retaining more than 64 uncommitted item bindings.
- [ ] In provider-VAD `EventInputAudioCommitted` handling, call `ProviderCommit(event.ItemID)` and stamp the retired generation into `event.InputAudioGeneration`; reject an unbound item ID without consulting current-turn state.
- [ ] In `ProviderCommit`, advance the generation before later appends and call `PCMFramer.Discard` so provider-VAD never flushes an already committed tail.
- [ ] At client commit admission, call `BeginManualCommit`; under the boundary lock freeze the current generation, advance it before any later append, allocate one positive token, and return the exact `PCMFramer.Flush` tail commands.
- [ ] Hold `inputAdmissionMu` across `BeginManualCommit`, admission of every returned tail command, and admission of the token-bearing `ClientInput`; `admitAudioAppend` uses the same lock so advanced-generation audio cannot enter between tail and commit.
- [ ] Reserve count and byte capacity for the complete tail-plus-commit admission batch before publishing its first tail; on reservation failure, call `AbortManualCommit(token)` and publish nothing.
- [ ] Transfer the reserved batch in tail-then-commit order without an intermediate fallible admission; if cancellation/close defeats a partially published batch, abort the token and terminalize the provider connection instead of mixing it with the advanced generation.
- [ ] Carry the token through the reactor without exposing it to the browser and copy it onto the resulting `CommandCommitAudio`.
- [ ] Immediately before dispatching that exact `CommandCommitAudio`, call `BindManualCommit(token, command.Sequence)`; require a positive, strictly increasing adapter sequence.
- [ ] On command-send failure, cancellation, suppression, or close before acknowledgement, call `AbortManualCommit(token)` exactly once.
- [ ] In manual `EventInputAudioCommitted` handling, call `ManualCommitAcknowledged(event.ItemID, event.InputAudioCommitSequence)` and stamp only the exactly matched pending generation; manual mode never calls `BindProviderItem` or `ProviderCommit`.
- [ ] Keep `CommandAcknowledgeAudioCommit` as adapter-only correlation with exact `InputItemID` and `InputAudioCommitSequence`; never serialize it as a provider protocol command.
- [ ] Make `sessionRuntime.atTurnBoundary` call `inputMediaBoundary.Partial()`.
- [ ] Stamp each `s2sws` session and every mapped event with its immutable `ProviderConnectionGeneration` from `realtime.OpenRequest`.
- [ ] Seed initial establishment with generation 1, carry it in `ConnectedInput` into the reactor ledger, and reject every zero generation.
- [ ] Persist the last provider generation on the logical session and use `nextProviderConnectionGeneration` for both browser reconnect replacement and internal rotation; fail closed on `math.MaxUint64` instead of wrapping to zero.
- [ ] Add the immutable connection generation to `providerInputAuthorityLedger` and scope its acknowledged input-item history, generation records, and duplicate tombstones to that value.
- [ ] At the authoritative input commit that reaches 960 retained item IDs, set one pending rotation intent for the exact current generation even if its transcript remains open.
- [ ] On every later reactor transition that can make `atTurnBoundary()` true, emit the pending `ProviderRotationRequestedEffect` exactly once; transcript completion may expose the boundary but never creates or retargets the intent.
- [ ] Have the manager consume `Effect.ProviderRotation`; reject stale/duplicate requests, pause browser ingress, and install a nonblocking provider-admission fence before calling `rotateProviderConnection` with the exact expected generation.
- [ ] Make the provider admission callback return `ErrStaleProviderConnection` immediately for fenced/old events; never wait on the paused browser-ingress gate from the receive owner.
- [ ] Replace `runState`'s immutable `voice` reads/writes with one `providerTransportOwner`; its `Run` method owns and joins the receive lane instead of letting an old-session close terminate the session errgroup.
- [ ] Route every provider send through `providerTransportOwner.Send` with the command's expected lease generation; reject a stale lease before provider I/O.
- [ ] In `rotateProviderConnection`, open generation `current+1`, replay the immutable provider session configuration/recovery context, and complete the exact provider-confirmed turn-mode/session-update handshake while old-provider admission remains paused.
- [ ] Reject any candidate input/output event before that handshake completes; retain only its typed confirmation, then atomically publish the replacement using the existing reconnect ownership and lock order.
- [ ] Call `providerTransportOwner.Rotate` to cancel and join the old receive lane, atomically publish the replacement lease, start its receive lane, and close/join only the old adapter session.
- [ ] Treat cancellation/EOF from the intentionally retired receive lane as rotation completion; only an error from the currently published lease may terminalize the browser session.
- [ ] Reject every old-generation event after publication, then apply `ProviderConnectionRotatedInput{Previous: current, Current: current+1}` through the reserved reactor-control lane.
- [ ] On that exact input, require a clean boundary and matching ledger generation, clear only the old generation's history/tombstones, and resume ingress; a mismatch terminalizes instead of rolling back to the old connection.
- [ ] Run the focused provider-VAD and manual race tests.

```sh
go test -race ./internal/session -run \
  'Test(ProviderVAD|Manual(Begin|Commit)|LateTurnOne)' -count=50
go test -race ./internal/session -run \
  'Test(ProviderConnection|InitialProviderConnection|BrowserReconnectAdvancesProviderConnection|ProviderTransportOwner)' -count=1
```

- [ ] Run adapter rotation, protocol, and contract tests.

```sh
go test -race ./internal/realtime/s2sws -run \
  'Test(ProviderVAD|ManualCommit|ProviderConnection|LateTurnOne|SessionUpdatesSerialize)' -count=25
go test -race ./internal/protocol -run 'TestPCMFramer' -count=50
test -n "${S2S_REALTIME_URL:-}" || { echo 'S2S_REALTIME_URL is required' >&2; exit 1; }
S2S_REALTIME_URL="$S2S_REALTIME_URL" \
./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 1 -- \
  go test -json -race -tags=contract ./test/contract -run 'Test.*(VAD|Commit)' -count=1
```

- [ ] Commit the input-boundary fix.

```sh
git add internal/session/manager.go internal/session/manager_test.go internal/session/input.go \
  internal/session/provider_transport.go internal/session/provider_transport_test.go \
  internal/session/effect.go internal/session/reactor.go internal/session/reactor_test.go \
  internal/realtime/voice.go internal/realtime/s2sws/connector.go \
  internal/realtime/s2sws/connector_internal_test.go internal/realtime/s2sws/commit_ack_test.go \
  internal/realtime/s2sws/input_turn_mode_test.go internal/protocol/codec.go \
  internal/protocol/codec_test.go
git commit -m 'fix: bind provider commits to input media generations'
```

---

## Task 3: Bound Provider Writes and Prioritize Cancellation

**Files:**

- Modify: `internal/realtime/voice.go`
- Modify: `internal/realtime/s2sws/connector.go`
- Modify: `internal/realtime/s2sws/mapping.go`
- Modify: `internal/realtime/s2sws/connector_test.go`
- Modify: `internal/realtime/s2sws/connector_internal_test.go`
- Modify: `internal/realtime/s2sws/input_turn_mode_test.go`
- Modify: `internal/realtime/s2sws/presentation_mapping_test.go`
- Create: `internal/realtime/s2sws/watchdog.go`
- Create: `internal/realtime/s2sws/watchdog_test.go`
- Modify: `internal/session/manager.go`
- Modify: `internal/session/manager_test.go`
- Modify: `internal/config/config.go`
- Modify: `internal/config/config_test.go`
- Modify: `internal/config/hash.go`
- Modify: `internal/config/hash_test.go`
- Modify: `cmd/live-gateway/main.go`
- Modify: `cmd/live-gateway/main_test.go`
- Modify: `test/contract/s2s_realtime_test.go`

**Interfaces:**

- Consumes: `func s2sws.New(cfg s2sws.Config) (*s2sws.Connector, error)`, thread-safe `func (conn *websocket.Conn) CloseNow() error`, `func (s *voiceSession) beginTerminalHandoff(err error) bool`, and the existing separate manager lanes `runState.ingress chan ingressFrame` and `runState.voiceCtl chan voiceControl`.
- Produces: `config.GatewayConfig.ProviderWriteTimeout time.Duration` with YAML key `provider_write_timeout`, `s2sws.Config.WriteTimeout time.Duration`, provider-neutral sentinels `var realtime.ErrWriteTimeout = errors.New("realtime provider write timed out")`, `var realtime.ErrSessionUpdateRejected = errors.New("realtime provider rejected session update")`, and `var realtime.ErrProviderFailure = errors.New("realtime provider failure")`. Zero `s2sws.Config.WriteTimeout` normalizes to `5 * time.Second`; values below `10 * time.Millisecond`, above `30 * time.Second`, or negative are rejected.
- Produces: package-private `const sessionUpdateEventPrefix = "live-session-update-"`, `func sessionUpdateEventID(generation realtime.ProviderConnectionGeneration, sequence, operationID uint64) (string, error)`, and `func sessionUpdateCorrelation(eventID string) (realtime.ProviderConnectionGeneration, uint64, bool)`. The grammar is the prefix plus canonical base-10 generation, sequence, and operation ID with no leading zeroes; generation and operation ID are positive while manager-owned initial sequence zero is valid.
- Produces: package-private `type pendingSessionUpdate struct { EventID string; Sequence uint64; Done chan struct{} }`, `func (s *voiceSession) beginInputTurnUpdate(ctx context.Context, turnDetection json.RawMessage, sequence uint64) (pendingSessionUpdate, error)`, `func (s *voiceSession) confirmInputTurnMode(event realtime.Event) (uint64, bool, error)`, and `func (s *voiceSession) rejectInputTurnUpdate(eventID string) (uint64, bool, error)`. Every outbound `session.update` receives one unique server-generated wire ID. A matching `error.error.event_id` rejection releases only the exact retained `{EventID, Sequence}`; a successful provider-generated `session.updated.event_id` is not an echo and acknowledges the sole outstanding update only after returned configuration and connection generation validate.
- Produces: package-private `type writeWatchdogArm struct { OperationID uint64; Deadline time.Time }`, `func newWriteWatchdog(abortWrite func() error, expire func(operationID uint64)) *writeWatchdog`, `func (w *writeWatchdog) Arm(operationID uint64, deadline time.Time) error`, `func (w *writeWatchdog) Disarm(operationID uint64)`, and `func (w *writeWatchdog) Close() error`. One watchdog goroutine and one timer are owned by each `voiceSession`; `func (s *voiceSession) beginWrite(parent context.Context) (context.Context, uint64, func(), bool)` arms it and the returned completion disarms that exact operation.

- [ ] Write `TestWriteDeadlineBoundsBlockedAppend` with a fake wire writer blocked beyond `WriteTimeout`.
- [ ] Write `TestWriteDeadlineBoundsBlockedResponseCreate` with the same deterministic writer gate.
- [ ] Write `TestCancelPriorityWinsAfterInflightMedia` and assert cancel is the next wire write once the in-flight send releases.
- [ ] Write `TestCancelPriorityTerminalizesBlockedWriter` and require stable timeout classification when the in-flight write never releases.
- [ ] Write `TestCorrelatedWritesReceiveIndependentDeadlines` for session update and manual-commit acknowledgement work.
- [ ] Write `TestWatchdogAbortsContextIgnoringWrite` with a writer that ignores context until the transport's thread-safe `CloseNow`/`AbortWrite` hook runs.
- [ ] Write `TestWriteWatchdogOwnsSingleLane` and issue 1,025 sequential operations while asserting one watchdog goroutine and one active timer maximum.
- [ ] Write `TestDisarmedWriteWatchdogCannotAbortLaterOperation` so a late timer for operation N cannot close operation N+1.
- [ ] Write `TestBlockedWriteCloseJoinsOwnedGoroutines` and assert the writer, watchdog, and correlation owners all return to zero.
- [ ] Write `TestSessionUpdateWireEventIDIsUniqueAndCorrelated` for repeated sequence zero across two provider generations and two sequential nonzero updates; all outbound IDs must be distinct and parse to the exact retained sequence for rejection correlation.
- [ ] Write `TestSessionUpdatedProviderEventIDIsNotClientEcho` using an arbitrary provider-generated acknowledgement ID and require the sole outstanding update to clear only after its returned mode/configuration validates.
- [ ] Write `TestSessionUpdateMatchingRejectionReleasesLatchAndTerminalizes` and require the exact provider `error.event_id` to clear only its retained `{EventID, Sequence}` before typed terminal handoff.
- [ ] Write `TestSessionUpdateUncorrelatedErrorTerminalizesWithoutGuessingLatch` and assert an absent, malformed, stale, or foreign `error.event_id` never releases a pending update by FIFO or sequence guess.
- [ ] Write `TestCorrelatedResponseErrorWithoutSessionUpdatePendingRemainsNonterminal`; with no pending update latch, preserve its typed response create/cancel correlation and keep the adapter usable.
- [ ] Write `TestSessionUpdateRejectionReconnectsWithFreshLatch` in `internal/session/manager_test.go`; reject the first provider's update, join teardown, reconnect, acknowledge a new unique update, and complete a second turn.
- [ ] Write `TestBlockedProviderBargeInRecoverySecondTurn` in `internal/session/manager_test.go`: block media, inject `speech_started`, observe the epoch fence, then require bounded cancel/terminalization, reconnect, and a complete second turn.
- [ ] Write `TestProviderWriteTimeoutDefaultsValidationAndCanonicalHash` in `internal/config` for zero, below-minimum, exact bounds, above-maximum, and two distinct hash values.
- [ ] Write `TestGatewayPassesProviderWriteTimeoutToS2SWS` in `cmd/live-gateway/main_test.go`.
- [ ] Add external acceptance `TestRealtimeImmediateCreateCancelThenNextTurnContract` in `test/contract/s2s_realtime_test.go`: create then cancel before `response.created`, drain the provider's declared terminal sequence, create again, and require the next response to complete. It must use the required endpoint and never convert unreachable/unsupported behavior into a skip.
- [ ] Run the adapter and manager tests before changing production code; retain the timeout/undefined-field output as RED evidence.

```sh
go test -race ./internal/realtime/s2sws -run \
  'Test(WriteDeadline|CancelPriority|CorrelatedWrites|CorrelatedResponse|Watchdog|WriteWatchdog|DisarmedWriteWatchdog|BlockedWrite|SessionUpdate)' -count=100
go test -race ./internal/session -run \
  'Test(.*BlockedProvider.*(Cancel|Recovery|SecondTurn)|SessionUpdateRejectionReconnectsWithFreshLatch)' -count=100
go test -race ./internal/config ./cmd/live-gateway -run 'Test.*ProviderWriteTimeout' -count=50
```

Expected: FAIL because the event-ID rejection path, watchdog lane, and provider write-timeout configuration do not exist yet.

- [ ] Add `GatewayConfig.ProviderWriteTimeout` to YAML decode, defaults, validation, canonical hash, and hash tests.
- [ ] Pass `cfg.Gateway.ProviderWriteTimeout` into `s2sws.Config.WriteTimeout` in `cmd/live-gateway/main.go` and assert the composed value in `main_test.go`.
- [ ] Normalize and validate `s2sws.Config.WriteTimeout` in `s2sws.New` using the exact bounds in the interface block.
- [ ] Generate a unique bounded `event_id` for every `CommandSessionUpdate` from the immutable provider generation, exact command sequence, and strictly increasing session-local operation ID; retain it together with the exact sequence.
- [ ] Serialize that exact ID on the outbound `session.update`; do not reuse the provider session ID or client event ID.
- [ ] On `session.updated`, ignore its provider-generated event ID, validate the returned turn mode/configuration and immutable provider generation, then atomically publish the sole pending acknowledgement sequence and release the latch.
- [ ] While an update latch is pending, compare only `error.error.event_id`; on an exact match, atomically release the latch, return `realtime.ErrSessionUpdateRejected`, and begin terminal handoff.
- [ ] While an update latch is pending, treat an absent, malformed, stale, or foreign error correlation as uncorrelated: do not mutate the pending update and terminalize with `realtime.ErrProviderFailure` so teardown/reconnect creates a fresh latch.
- [ ] With no update latch pending, preserve ordinary typed response create/cancel error correlation and do not terminalize solely because the event is uncorrelated to a session update.
- [ ] On close or cancellation, clear and close the pending update waiter exactly once before joining adapter owners.
- [ ] Construct one `writeWatchdog` when `voiceSession` starts and give it the transport's thread-safe `CloseNow` as `abortWrite`.
- [ ] Change `voiceSession.beginWrite` to derive each write context with `context.WithDeadline(parent, deadline)`, allocate a strictly increasing operation ID, and arm the single watchdog lane with `{operationID, deadline}`.
- [ ] Make the returned write completion disarm only its exact operation ID before releasing the active-write owner.
- [ ] Route session updates, append audio, create item/response, cancel response, and manual-commit acknowledgement through the same deadline-bearing write primitive.
- [ ] Keep `runState.ingress` for media and `runState.voiceCtl` for control; reserve control capacity before media selection and do not create per-command goroutines.
- [ ] Preserve FIFO within media and control classes while selecting cancel/close before queued append/create commands after the active operation returns.
- [ ] On watchdog expiry, verify the operation ID is still active, call `abortWrite` first, cancel the operation context, and invoke `beginTerminalHandoff(realtime.ErrWriteTimeout)` through the existing terminal-once gate.
- [ ] Make watchdog expiry enqueue terminal ownership to the existing writer/mapping owner; the watchdog lane never calls its own join path.
- [ ] Close and join the watchdog lane during every adapter close path; never wait for the context-ignoring writer before invoking transport abort.
- [ ] Assert pending receipts, update latches, correlations, queue credits, and owned goroutines return to zero on rejection, timeout, cancellation, and close.
- [ ] Keep the raw provider error body private; Task 8 replaces its temporary correlation-only mapping with the closed failure event contract before release.
- [ ] Run the full adapter and session race suites.

```sh
go test -race -count=10 ./internal/realtime/s2sws ./internal/session
```

- [ ] Run configuration, composition, and provider contract tests.

```sh
go test -race ./internal/config ./cmd/live-gateway -count=10
test -n "${S2S_REALTIME_URL:-}" || { echo 'S2S_REALTIME_URL is required' >&2; exit 1; }
S2S_REALTIME_URL="$S2S_REALTIME_URL" \
./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 1 \
  --expect-top-level TestRealtimeImmediateCreateCancelThenNextTurnContract -- \
  go test -json -race -tags=contract ./test/contract \
    -run '^TestRealtimeImmediateCreateCancelThenNextTurnContract$' -count=1
```

- [ ] Commit the bounded-write change.

```sh
git add internal/realtime/voice.go internal/realtime/s2sws/connector.go \
  internal/realtime/s2sws/mapping.go internal/realtime/s2sws/connector_test.go \
  internal/realtime/s2sws/connector_internal_test.go internal/realtime/s2sws/input_turn_mode_test.go \
  internal/realtime/s2sws/presentation_mapping_test.go internal/realtime/s2sws/watchdog.go \
  internal/realtime/s2sws/watchdog_test.go \
  internal/session/manager.go internal/session/manager_test.go \
  internal/config/config.go internal/config/config_test.go internal/config/hash.go \
  internal/config/hash_test.go cmd/live-gateway/main.go cmd/live-gateway/main_test.go \
  test/contract/s2s_realtime_test.go
git commit -m 'fix: bound provider writes and prioritize barge in'
```

---

## Task 4: Compact Cancelled-Response Correlation

**Files:**

- Modify: `internal/realtime/s2sws/connector.go`
- Modify: `internal/realtime/s2sws/connector_internal_test.go`
- Modify: `internal/realtime/s2sws/connector_unit_test.go`

**Interfaces:**

- Consumes: `realtime.CommandCreateResponse`, `realtime.CommandCancelResponse`, `realtime.Event.Epoch identity.ResponseEpoch`, and the adapter's exact live-pending correlation map.
- Produces: package-private `type retiredEpochs struct { retiredThrough identity.ResponseEpoch; recent map[identity.ResponseEpoch]struct{}; bytes int }`, `func (r *retiredEpochs) retire(epoch identity.ResponseEpoch) error`, `func (r *retiredEpochs) contains(epoch identity.ResponseEpoch) bool`, and `func (r *retiredEpochs) retainedBytes() int`. `recent` holds at most 64 out-of-order epochs; exact live pending entries are checked before `contains`.

- [ ] Write `TestCancelledPendingPressure1025CyclesAdmitsNextLiveCreate` with no provider created/done events.
- [ ] Write `TestLateCancelledCycleOneCannotRecoverOutput` for a very late first-cycle `response.created`.
- [ ] Write `TestCancelledCorrelationPreservesLiveGaps` for mixed cancelled/live gaps and an epoch adjacent to the retired floor.
- [ ] Write `TestReorderedProviderErrorCannotReviveRetiredEpoch` for reordered provider errors.
- [ ] Write `TestCancelledCorrelationBytesRemainFixed` and assert both retained entry count and bytes.
- [ ] Run all five tests and retain the unbounded-correlation failure as RED evidence.

```sh
go test -race ./internal/realtime/s2sws -run \
  'Test.*(1025|CancelledPendingPressure|LateCancelled|PreservesLiveGaps|ReorderedProviderError|CorrelationBytes)' -count=25
```

- [ ] Add `retiredEpochs` with a monotonic retired-through value and a 64-entry out-of-order set.
- [ ] Replace one-map-entry-per-cancel tombstones with `retiredEpochs.retire`.
- [ ] Check the exact live pending map before `retiredEpochs.contains` so live gaps are never suppressed.
- [ ] Drop audio, transcript, done, and error events for retired or too-old ambiguous epochs without rebuilding response state.
- [ ] Retire response-ID history when the reactor authoritatively terminalizes a response even if `response.done` never arrives.
- [ ] Run the full adapter race suite.

```sh
go test -race -count=10 ./internal/realtime/s2sws
```

- [ ] Commit compact correlation.

```sh
git add internal/realtime/s2sws/connector.go \
  internal/realtime/s2sws/connector_internal_test.go \
  internal/realtime/s2sws/connector_unit_test.go
git commit -m 'fix: compact cancelled response correlation'
```

---

## Task 5: Add Adapter Retained-Byte Credits

**Files:**

- Create: `internal/realtime/s2sws/credits.go`
- Create: `internal/realtime/s2sws/credits_test.go`
- Modify: `internal/realtime/s2sws/connector.go`
- Modify: `internal/realtime/s2sws/connector_internal_test.go`

**Interfaces:**

- Consumes: `func New(cfg Config) (*Connector, error)`, `Config.MaxFrameBytes int64`, and the adapter's write, public-receive, mapped-receive, decoded-PCM, and correlation owners.
- Produces: `func newByteCredits(limit int) (*byteCredits, error)`, `func (c *byteCredits) Reserve(n int) bool`, `func (c *byteCredits) Release(n int)`, `func (c *byteCredits) Used() int`, `type OwnershipSnapshot struct { OwnedGoroutines int; WriteBytes int; ReceiveBytes int; CorrelationBytes int }`, plus `Config.WriteQueueBytes int`, `Config.ReceiveQueueBytes int`, `Config.CorrelationBytes int`, and `Config.ObserveOwnership func(OwnershipSnapshot)`. Zero byte limits normalize to 4 MiB, 8 MiB, and 1 MiB respectively; every value must be in `[1, 64 MiB]` after normalization. A nil observer is a no-op; observations run outside accounting locks and contain counts only.

- [ ] Write `TestByteCreditsExactLimitAndLimitPlusOne` in `credits_test.go`.
- [ ] Write `TestQueueByteBoundaries` for write, public receive, mapped receive, decoded PCM, raw JSON, response IDs, and correlation metadata.
- [ ] Write `TestFrameBudgetRejectsBeforeCloneOrDecode` using frames near `MaxFrameBytes` and a clone/decode observation hook.
- [ ] Write `TestAggregateByteBudgetNeverExceedsConfiguredLimit` with concurrent producer floods.
- [ ] Write `TestCloseReleasesEveryOutstandingCredit` for queued, mapping-rejected, correlated, and terminal-drain owners.
- [ ] Write `TestOwnershipObserverReportsCountsAndZeroAfterClose` and assert callbacks run outside credit locks.
- [ ] Run the credit tests before adding production code; retain the undefined primitive/config failure as RED evidence.

```sh
go test -race ./internal/realtime/s2sws -run \
  'Test.*(QueueByte|AggregateByte|Credit|FrameBudget|OwnershipObserver)' -count=50
```

Expected: FAIL because the adapter byte-credit primitive and byte-limit configuration do not exist yet.

- [ ] Implement the package-private weighted-credit primitive without using channel `len` as authority:

```go
type byteCredits struct {
    mu    sync.Mutex
    limit int
    used  int
}

func newByteCredits(limit int) (*byteCredits, error)
func (c *byteCredits) Reserve(n int) bool
func (c *byteCredits) Release(n int)
func (c *byteCredits) Used() int
```

- [ ] Add the exact adapter config fields:

```go
type Config struct {
    URL              string
    APIKey           string
    Model            string
    ConnectTimeout   time.Duration
    CommitAckTimeout time.Duration
    WriteTimeout     time.Duration
    MaxFrameBytes    int64
    WriteQueueBytes  int
    ReceiveQueueBytes int
    CorrelationBytes int
    ObserveOwnership func(OwnershipSnapshot)
}
```

- [ ] Normalize zero to the 4 MiB/8 MiB/1 MiB defaults in `s2sws.New`; reject values outside `[1, 64 MiB]`.
- [ ] Reserve write bytes before copying command audio, item JSON, response JSON, or identifiers into the write queue.
- [ ] Reserve receive bytes before cloning raw frames or decoding PCM and transfer the exact charge to the mapped event owner.
- [ ] Charge correlation IDs and metadata to `CorrelationBytes` and release them on acknowledgement, retirement, or terminalization.
- [ ] Release every charge on dequeue, mapping rejection, cancellation, terminal drain, and idempotent close.
- [ ] Emit count-only `OwnershipSnapshot` values after ownership changes and a final all-zero snapshot after every close path; never invoke the observer while holding a credit/correlation lock.
- [ ] Run the focused byte-budget race tests.

```sh
go test -race ./internal/realtime/s2sws -run \
  'Test.*(QueueByte|AggregateByte|Credit|FrameBudget|OwnershipObserver)' -count=50
```

- [ ] Run the complete adapter race suite.

```sh
go test -race -count=10 ./internal/realtime/s2sws
```

- [ ] Commit adapter retained-byte accounting.

```sh
git add internal/realtime/s2sws/credits.go internal/realtime/s2sws/credits_test.go \
  internal/realtime/s2sws/connector.go internal/realtime/s2sws/connector_internal_test.go
git commit -m 'fix: bound realtime adapter retained bytes'
```

---

## Task 6: Add Session and Profile-Render Byte Credits

**Files:**

- Create: `internal/session/credits.go`
- Create: `internal/session/credits_test.go`
- Modify: `internal/session/manager.go`
- Modify: `internal/session/manager_test.go`
- Modify: `internal/session/profile.go`
- Modify: `internal/session/profile_test.go`
- Modify: `internal/session/profile_renderer.go`
- Modify: `internal/session/profile_renderer_test.go`
- Modify: `internal/config/config.go`
- Modify: `internal/config/config_test.go`
- Modify: `internal/config/hash.go`
- Modify: `internal/config/hash_test.go`
- Modify: `internal/gateway/app.go`
- Modify: `internal/gateway/app_test.go`

**Interfaces:**

- Consumes: `func NewManager(cfg Config, connector realtime.Connector, ids IDGenerator) *Manager`, current `func NewProfileRenderDispatcher(engine prompttmpl.Engine, workers, capacity int, limits prompttmpl.Limits) (*ProfileRenderDispatcher, error)`, and `func (dispatcher *ProfileRenderDispatcher) Dispatch(ctx context.Context, runtime *ProfileRuntime, request PromptRenderRequestedEffect, complete promptRenderCompletion) error`.
- Produces: package-private `func newByteCredits(limit int) (*byteCredits, error)`, `func (c *byteCredits) Reserve(n int) bool`, `func (c *byteCredits) Release(n int)`, and `func (c *byteCredits) Used() int` inside package `session`; `session.Config.ControlQueueBytes`, `CompletionQueueBytes`, `DurableQueueBytes`, `ProfileRenderBytes`, and `ProfileRuntimeBytes` as `int`; and matching YAML fields `control_queue_bytes`, `completion_queue_bytes`, `durable_queue_bytes`, `profile_render_bytes`, and `profile_runtime_bytes` on `config.SessionConfig`.
- Produces: `type ProfileRenderOption func(*profileRenderConfig) error`; `func WithProfileRenderByteLimit(limit int) ProfileRenderOption`; and updated `func NewProfileRenderDispatcher(engine prompttmpl.Engine, workers, capacity int, limits prompttmpl.Limits, options ...ProfileRenderOption) (*ProfileRenderDispatcher, error)`. Queue/render defaults are 2 MiB, 1 MiB, 4 MiB, and 2 MiB; the manager-wide profile-runtime default is 32 MiB. Every value must be in `[1, 64 MiB]` after zero normalization.
- Produces: package-private `func profileRuntimeRetainedBytes(runtime *ProfileRuntime) (int, error)`, `type profileRuntimeLease struct { Runtime *ProfileRuntime; credits *byteCredits; bytes int; once sync.Once }`, `func (m *Manager) reserveProfileRuntime(runtime *ProfileRuntime) (*profileRuntimeLease, error)`, and `func (lease *profileRuntimeLease) Release()`. The deterministic charge is checked addition of `Compiled.ResidentBytes()`, every retained template name/source and schema value in sorted-key order, canonical snapshot and prompt-definition bytes, session-context bytes, and the rendered `System` and `Opening` strings.

- [ ] Write `TestSessionByteCreditsExactLimitAndLimitPlusOne` in `credits_test.go`.
- [ ] Write `TestControlQueueBytesReleaseOnCancelAndQueueFull` for manager control payloads.
- [ ] Write `TestCompletionQueueBytesPreservesReservedProgress` for typed completions at the ordinary inbox limit.
- [ ] Write `TestDurableAndSafetyQueueBytesReleaseOnFailure` for dispatcher failure and cancellation.
- [ ] Write `TestRenderDispatcherByteLimitAndLimitPlusOne` for transcript and immutable runtime-reference charges.
- [ ] Write `TestRenderDispatcherCreditsReleaseAfterPanicFailureAndClose` for worker panic, render failure, and dispatcher close.
- [ ] Write `TestProfileRuntimeRetainedBytesIncludesEveryOwnedMaterial` and vary compiled resident bytes, template source, snapshot, prompt definition, session context, system, and opening one at a time.
- [ ] Write `TestProfileRuntimeManagerCreditExactLimitAndLimitPlusOne` and assert the rejected runtime is never published.
- [ ] Write `TestProfileRuntimeManagerCreditsAggregateAcrossConcurrentSessions` and assert aggregate manager use never exceeds `ProfileRuntimeBytes`.
- [ ] Write `TestProfileRuntimeCreditsReleaseOnRegistrationRollbackAndReconnectReplacement` for failed initial registration, same-scope rollback, and successful replacement publication.
- [ ] Write `TestProfileRuntimeCreditsReleaseOnUnregisterAndShutdown` and require manager-wide use to return to zero exactly once.
- [ ] Write `TestSessionByteLimitsDefaultsValidationAndCanonicalHash` for all five YAML fields, including distinct `ProfileRuntimeBytes` hashes.
- [ ] Write `TestGatewayPassesAllSessionByteLimitsToManager` and assert the manager-wide profile-runtime limit separately from the render-dispatch limit.
- [ ] Run the focused tests before adding production fields or credits; retain the compile/limit failures as RED evidence.

```sh
go test -race ./internal/session -run \
  'Test.*(QueueByte|RenderDispatcher.*Byte|ProfileRuntime|Aggregate|Credit)' -count=50
go test -race ./internal/config ./internal/gateway -run 'Test.*SessionByteLimits' -count=50
```

Expected: FAIL because the session/render/runtime credit pools and their five configuration fields do not exist yet.

- [ ] Add the exact byte-limit fields to both `config.SessionConfig` and `session.Config`:

```go
ControlQueueBytes    int
CompletionQueueBytes int
DurableQueueBytes    int
ProfileRenderBytes   int
ProfileRuntimeBytes  int
```

- [ ] Add the 2 MiB/1 MiB/4 MiB/2 MiB/32 MiB defaults and `[1, 64 MiB]` validation to configuration loading and `normalizeManagerConfig`.
- [ ] Add all five normalized byte limits to canonical configuration hashing.
- [ ] Pass the five values from `config.SessionConfig` through `gateway.New` into `session.Config`.
- [ ] Pass `WithProfileRenderByteLimit(ProfileRenderBytes)` when constructing the profile renderer.
- [ ] Implement session `byteCredits` with the exact method set in the interface block.
- [ ] Reserve control bytes before cloning event IDs, transcript strings, JSON, profile references, and ordinary reactor payloads.
- [ ] Reserve completion bytes from a separate pool so typed-completion progress survives ordinary inbox exhaustion.
- [ ] Reserve durable/safety bytes before copying requested effects and transfer the charge to the dispatcher owner.
- [ ] Charge fixed-size PCM as `frameCount * frameBytes` plus its exact metadata bytes.
- [ ] Charge profile rendering once for transcript bytes plus bounded snapshot/runtime-reference metadata; do not duplicate immutable runtime ownership per queued item.
- [ ] Release charges on success, rejection, queue-full, cancellation, supersession, worker failure/panic, terminal drain, and `Close`.
- [ ] Implement `profileRuntimeRetainedBytes` with overflow-safe integer addition, sorted map keys, canonical encodings, and `Compiled.ResidentBytes() > 0`; reject nil, invalid, or unmeasurable runtimes.
- [ ] Construct one `profileRuntimeCredits` pool on `Manager`; no dispatcher, session, or reconnect path creates a private runtime-credit pool.
- [ ] Reserve the complete runtime charge after compile/render succeeds but before active-map registration or provider/session publication.
- [ ] Attach the returned lease to the candidate `sessionRuntime` and transfer it only when that exact runtime wins publication.
- [ ] Release the candidate lease on provider-open failure, initial registration failure, same-scope replacement rollback, and cancelled reconnect.
- [ ] After a successful reconnect/replacement swap, release the prior runtime lease only after the old runtime is fenced and joined.
- [ ] Release the active lease on unregister, terminal close, and manager shutdown through `profileRuntimeLease.Release`; double-close is a no-op.
- [ ] Run the focused session byte-budget tests.

```sh
go test -race ./internal/session -run \
  'Test.*(QueueByte|RenderDispatcher.*Byte|ProfileRuntime|Aggregate|Credit)' -count=50
```

- [ ] Run configuration/hash, gateway composition, and complete session race tests.

```sh
go test -race ./internal/config ./internal/gateway -count=10
go test -race -count=5 ./internal/session
```

- [ ] Commit session and renderer retained-byte accounting.

```sh
git add internal/session/credits.go internal/session/credits_test.go \
  internal/session/manager.go internal/session/manager_test.go \
  internal/session/profile.go internal/session/profile_test.go \
  internal/session/profile_renderer.go internal/session/profile_renderer_test.go \
  internal/config/config.go internal/config/config_test.go internal/config/hash.go \
  internal/config/hash_test.go internal/gateway/app.go internal/gateway/app_test.go
git commit -m 'fix: bound session and prompt render retention'
```

---

## Task 7: Make Profile Tool and Handoff Authority Immutable

**Files:**

- Modify: `internal/profile/types.go`
- Modify: `internal/profile/types_test.go`
- Modify: `internal/profile/publisher.go`
- Modify: `internal/profile/publisher_test.go`
- Modify: `internal/profile/resolver_test.go`
- Modify: `internal/config/config.go`
- Modify: `internal/config/config_test.go`
- Modify: `internal/config/hash.go`
- Modify: `internal/config/hash_test.go`
- Modify: `internal/session/profile.go`
- Modify: `internal/session/profile_test.go`
- Modify: `internal/session/effect.go`
- Modify: `internal/session/reactor.go`
- Modify: `internal/session/reactor_test.go`
- Modify: `internal/session/manager.go`
- Modify: `internal/session/manager_test.go`
- Modify: `internal/session/reconnect_credentials.go`
- Modify: `internal/session/reconnect_credentials_test.go`
- Modify: `internal/gateway/app.go`
- Modify: `internal/gateway/app_test.go`
- Modify: `internal/gateway/handoffs.go`
- Modify: `internal/gateway/handoffs_test.go`
- Modify: `internal/handoff/types.go`
- Modify: `internal/handoff/state.go`
- Modify: `internal/handoff/state_test.go`
- Modify: `internal/worker/recipes.go`
- Modify: `internal/worker/recipes_test.go`
- Modify: `internal/worker/runner.go`
- Modify: `internal/worker/runner_test.go`
- Modify: `internal/worker/runner_integration_test.go`
- Modify: `internal/store/postgres/profiles_integration_test.go`
- Modify: `cmd/live-gateway/main.go`
- Modify: `cmd/live-gateway/main_test.go`
- Modify: `demo/live-client/profile_api.go`
- Modify: `demo/live-client/profile_api_test.go`
- Modify: `demo/live-client/profile_dto.go`
- Modify: `demo/live-client/web/profile-api.mjs`
- Modify: `demo/live-client/web/profile-api.test.mjs`
- Modify: `demo/live-client/web/recipe-editor.mjs`
- Modify: `demo/live-client/web/recipe-editor.test.mjs`
- Modify: `demo/live-client/web/setup-profile.mjs`
- Modify: `demo/live-client/web/setup-profile.test.mjs`
- Modify: `demo/live-client/web/setup.html`
- Modify: `demo/live-client/web/setup.mjs`
- Modify: `demo/live-client/web/setup.test.mjs`
- Modify: `demo/live-client/web/setup.css`
- Modify: `test/integration/vertical_profile_setup_test.go`
- Modify: `test/integration/vertical_profile_worker_test.go`

**Interfaces:**

- Consumes: `func prepareProfileRuntime(ctx context.Context, selected profile.Resolved, engine prompttmpl.Engine) (*ProfileRuntime, error)`, `func (runtime *ProfileRuntime) HandoffProfileSnapshot(transcript string) (handoff.ProfileSnapshot, bool)`, `gateway.SessionHandoffSnapshot`, `handoff.CreateRequest`, `func (c *reconnectCredentials) Issue(claims ReconnectClaims) (IssuedReconnectCredential, error)`, `func (c *reconnectCredentials) Verify(credential string) (ReconnectClaims, error)`, and `worker.RecipeResolver.ResolveAndRender(context.Context, handoff.ProfileSnapshot, worker.RecipeEvidence) (worker.RenderedRecipe, error)`.
- Produces: authoritative `profile.Manifest.DefaultRecipeID string` and derived `profile.Snapshot.DefaultRecipeID string`, each tagged `json:"default_recipe_id,omitempty"`; plus `func (manifest Manifest) ValidateAuthority() error`. `Manifest.Clone`, draft-compatible `Manifest.Validate`, `Manifest.Hash`, `Manifest.CanonicalJSON`, strict `Manifest.ValidateAuthority`, and `Manifest.ValidateSnapshot` include it. `Manifest.Validate` accepts an empty default only as a repairable recipe-bearing draft; a nonempty value must match an exact recipe. `ValidateAuthority`, snapshot validation, preview, and publication require a nonempty exact default whenever recipes exist.
- Produces: setup DTO `profileDraftResponse.DefaultRecipeID string` with JSON key `default_recipe_id`; `PUT /api/setup/v1/profiles/{id}/default-recipe` with `If-Match` generation and body `{"recipe_id":"<id>"}`; `func (handler *demoHandler) serveProfileDefaultRecipe(http.ResponseWriter, *http.Request, profile.ID)`; JS `profileAPI.setDefaultRecipe(profileID, generation, recipeID, signal)`; and workspace `setDefaultRecipe(recipeID)`. Creating the first new recipe sets that exact ID in the same manifest mutation; a legacy recipe-bearing draft with an empty default remains loadable but cannot preview/publish until repaired through this endpoint; deleting the last clears it; deleting the selected recipe while others remain returns `default_recipe_required` until the operator explicitly selects another.
- Produces: `func bindProfileHandoffAuthority(runtime *ProfileRuntime, catalog toolpolicy.Catalog, model string) error`; and new fields on the existing `session.HandoffRequestedEffect`: `ToolAllowlist []string`, `ApprovalPolicy json.RawMessage`, and `Versions handoff.Versions`. `ToolPolicy toolpolicy.Policy` and `Profile handoff.ProfileSnapshot` remain on that effect. The gateway copies those fields into its existing `SessionHandoffSnapshot` and then into the existing `handoff.CreateRequest`; no parallel authority type is introduced.
- Produces: `config.ReconnectConfig.LegacyV1AcceptUntil time.Time` with YAML key `legacy_v1_accept_until` and `canonicalReconnect.LegacyV1AcceptUntil string` with JSON key `legacy_v1_accept_until`; `type ReconnectCredentialOption func(*reconnectCredentialConfig) error`; `func WithLegacyV1AcceptUntil(cutoff time.Time, credentialTTL time.Duration) ReconnectCredentialOption`; and updated `func NewReconnectCredentials(key []byte, source clock.Clock, options ...ReconnectCredentialOption) (ReconnectCredentials, error)`. A zero cutoff is the production-safe default and rejects every `lrt1`; a nonzero cutoff is normalized to UTC millisecond precision, included in the canonical config hash, and must be no later than `source.Now().Add(credentialTTL)`.
- Produces: reconnect credentials issued as `lrt2.<payload>.<mac>`. `Verify` accepts an otherwise valid, unexpired `lrt1` only when `now.Before(LegacyV1AcceptUntil)` and only in two complete claim shapes: all four profile claims absent binds `profile.Builtin()`, while all four present and valid preserve that exact admitted profile snapshot. Any partial/mixed claim set, exact-cutoff verification, expired token, zero cutoff, or far-future configured cutoff is invalid. The unchanged `ReconnectCredentials` interface remains `Issue(ReconnectClaims) (IssuedReconnectCredential, error)` and `Verify(string) (ReconnectClaims, error)`.

- [ ] Add characterization `TestProfileSystemAndTurnPromptsRemainServerOwned` for `system.j2`, off-reactor turn rendering, and rejection of client instruction/tool/tool-choice replacement.
- [ ] Write `TestProfileKnowledgeOnlyRecipeNarrowsDeploymentWebSearch` and assert the durable `CreateRequest.ToolAllowlist` remains knowledge-only.
- [ ] Write `TestProfileBoundClientCannotEnableAbsentWebSearch` for `live.tool_policy.update`.
- [ ] Write `TestManifestDefaultRecipeIDIsAuthoritative` for zero, one, and multiple recipes, unknown IDs, clone isolation, canonical JSON, distinct manifest hashes, and the unchanged fixed hash for a no-recipe manifest whose empty field is omitted.
- [ ] Write `TestPublisherCopiesDefaultRecipeIDIntoSnapshotBeforeHash` and require `Manifest.ValidateSnapshot` to reject a changed or omitted derived value.
- [ ] Write `TestLegacyRecipeDraftWithoutDefaultLoadsAndPersistsExplicitRepair` against PostgreSQL; read a canonical pre-field manifest, save an explicit matching ID, and read back the repaired draft.
- [ ] Write `TestSetupLegacyDefaultRecipeRepairAndRepublish` through the HTTP setup boundary and `TestVerticalProfileDefaultRecipeRepair` through PostgreSQL-backed setup; both must end in a newly published strict snapshot.
- [ ] Write `TestPublishedRecipeSnapshotWithoutDefaultFailsClosedUntilRepublished`; no runtime, reconnect, or worker path may infer a default from recipe order.
- [ ] Write `TestProfileMultipleRecipesRequireDefaultRecipe` and assert runtime/worker selection never falls back to lexicographic order.
- [ ] Write `TestSetupFirstRecipeEstablishesDefaultRecipeInSameMutation` and verify the returned DTO generation and `default_recipe_id`.
- [ ] Write `TestSetupDefaultRecipeLifecycle` for explicit switch, delete-nondefault, reject deleting the selected recipe while alternatives remain, and clear-on-last-delete.
- [ ] Add exactly four new top-level JS tests—one each to reject malformed DTO defaults, render the selected recipe, persist a selection through refresh, and surface `default_recipe_required` without changing local authority—taking the integration-plan baseline from 214 to exactly 218 tests.
- [ ] Update every recipe-bearing manifest/snapshot fixture in profile resolver, PostgreSQL, setup, and vertical worker tests with an explicit matching default; keep only the named legacy-repair fixtures empty.
- [ ] Write `TestProfileSnapshotSurvivesReconnectActivationAndRevocation` for admitted sessions and already accepted work.
- [ ] Write `TestProfileCatalogIncompatibilityFailsBeforeSessionPublication` using a binding absent from the deployment catalog.
- [ ] Add a fixed signed `lrt1` fixture with all profile fields absent and a clock before its expiry.
- [ ] Add a fixed signed profile-aware `lrt1` fixture with all four valid profile claims and a clock before its expiry.
- [ ] Write `TestReconnectLegacyCredentialRejectedByDefault`, `TestReconnectLegacyCredentialBindsBuiltinBeforeCutoff`, and `TestReconnectProfileAwareV1PreservesExactSnapshotBeforeCutoff`.
- [ ] Write `TestReconnectLegacyCredentialRejectsPartialProfileClaims`, `TestReconnectLegacyCredentialRejectsAtCutoff`, `TestReconnectLegacyCredentialRejectsAfterCutoff`, and `TestReconnectLegacyCredentialStillHonorsNormalExpiry`.
- [ ] Write `TestReconnectLegacyFarFutureCutoffRejected` and `TestCanonicalHashIncludesLegacyV1AcceptUntil`; compare zero, two distinct UTC cutoffs, and equivalent time zones normalized to the same millisecond.
- [ ] Run the authority, setup, and reconnect tests before production edits; retain the undefined-field, incorrect-selection, and cutoff failures as RED evidence.

```sh
go test -race ./internal/profile ./internal/session ./internal/gateway ./internal/handoff ./internal/worker \
  -run 'Test(Profile|ManifestDefaultRecipe|PublisherCopiesDefaultRecipe|PublishedRecipe|Reconnect(Legacy|ProfileAwareV1))' -count=20
go test -race ./internal/config ./cmd/live-gateway ./demo/live-client \
  -run 'Test.*(LegacyV1|DefaultRecipe)' -count=20
test -n "${TEST_DATABASE_URL:-}" || { echo 'TEST_DATABASE_URL is required' >&2; exit 1; }
TEST_DATABASE_URL="$TEST_DATABASE_URL" \
./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 1 \
  --expect-top-level TestLegacyRecipeDraftWithoutDefaultLoadsAndPersistsExplicitRepair -- \
  go test -json -race -tags=integration ./internal/store/postgres \
    -run '^TestLegacyRecipeDraftWithoutDefaultLoadsAndPersistsExplicitRepair$' -count=1
node deploy/scripts/run-node-test-no-skip.mjs --expect-tests 218 -- \
  node --test demo/live-client/web/*.test.mjs
```

Expected: FAIL because manifest default-recipe authority/legacy repair, its `/setup` lifecycle, immutable handoff fields, `lrt2`, and the bounded `lrt1` cutoff do not exist yet.

- [ ] Add `DefaultRecipeID string \`json:"default_recipe_id,omitempty"\`` to `profile.Manifest` and copy it in `Manifest.Clone`.
- [ ] Include the manifest default in draft-compatible validation, canonical JSON, and manifest hashing; when it is nonempty, require exact recipe membership.
- [ ] Add `Manifest.ValidateAuthority` and require a nonempty matching default for every recipe-bearing authority value.
- [ ] Add the same field to `profile.Snapshot` and include it in clone, canonical JSON, and snapshot hashing.
- [ ] Pass `Snapshot.DefaultRecipeID` into the manifest projection inside `canonicalSnapshot`; keep the empty `omitempty` representation so existing no-recipe canonical fixtures remain byte-for-byte stable.
- [ ] Reject zero recipes with a nonempty default everywhere; permit an empty default with recipes only in a mutable draft and return `default_recipe_required` from preview/publication.
- [ ] Make `Draft.Validate` use draft-compatible `Manifest.Validate`; make `Published.Validate`, `Resolved.Validate`, and `Manifest.ValidateSnapshot` use strict authority validation.
- [ ] In `Resolved.Validate`, copy `resolved.Snapshot.DefaultRecipeID` into the reconstructed manifest before strict validation.
- [ ] In `Publisher.prepare`, call `manifest.ValidateAuthority`, then copy `manifest.DefaultRecipeID` unchanged into the candidate snapshot before calculating `SnapshotHash`.
- [ ] Make `Manifest.ValidateSnapshot` require exact default equality in addition to the existing recipe equality.
- [ ] Add `default_recipe_id` to draft DTO encoding and strict JS decoding.
- [ ] Let strict DTO decoding represent the one bounded incomplete state `{recipes: nonempty, default_recipe_id: ""}` as `defaultRecipeRequired: true`; reject every nonempty unknown ID.
- [ ] Route `PUT /api/setup/v1/profiles/{id}/default-recipe` to `serveProfileDefaultRecipe`; enforce JSON content type, exact body fields, `If-Match`, and existing recipe membership.
- [ ] When creating the first recipe, set `Manifest.DefaultRecipeID` in that same optimistic mutation; never infer a default while reading or publishing.
- [ ] When deleting the last recipe, clear the default in that same mutation.
- [ ] Reject deletion of the selected recipe while another recipe exists; do not choose a replacement lexicographically.
- [ ] Add `setDefaultRecipe` to the browser API/workspace and update local draft state only from the returned authoritative DTO.
- [ ] Render an accessible recipe default selector on `/setup`, disable it during mutations, and preserve focus/status/error behavior on refresh and conflict.
- [ ] Disable preview/publish while `defaultRecipeRequired` is true and direct the operator to the explicit selector; this is the bounded repair path for persisted pre-field drafts.
- [ ] Fail closed when resolving an old published recipe snapshot without a default and require repair plus a newly published version; never synthesize a default at runtime or reconnect.
- [ ] Implement `bindProfileHandoffAuthority` to resolve `DefaultRecipeID`, recipe binding IDs, binding tool IDs, approval requirements, registry version, and configured async model before session publication.
- [ ] Keep the existing gateway and durable request shapes; the atomic admission snapshot remains:

```go
type SessionHandoffSnapshot struct {
    ContextRef       string
    ContextHash      string
    Deadline         time.Time
    TokenBudget      int
    Priority         int16
    ToolAllowlist  []string
    ToolPolicy     json.RawMessage
    ApprovalPolicy json.RawMessage
    Versions       handoff.Versions
    Profile        handoff.ProfileSnapshot
    PresentationHint string
}
```

- [ ] Add `ToolAllowlist`, `ApprovalPolicy`, and `Versions` to `HandoffRequestedEffect`; deep-clone every slice/JSON field at the reactor, manager, and durable-dispatch boundaries.
- [ ] Copy the admitted effect authority over the deployment snapshot in `durableDispatcher.DispatchHandoff`, validate it with `handoff.ValidateCreateRequest`, and enqueue one owned `SessionHandoffSnapshot`.
- [ ] Persist profile, tool allowlist, tool policy, approval policy, model, prompt, registry, and config versions through the existing `handoff.CreateRequest` canonicalization and clone paths.
- [ ] Make `live.tool_policy.update` compute only an intersection with admitted authority and reject every added tool or raised bound.
- [ ] Add `LegacyV1AcceptUntil time.Time \`yaml:"legacy_v1_accept_until"\`` to `config.ReconnectConfig`; leave its normalized default at the zero time.
- [ ] Canonicalize a nonzero cutoff as UTC RFC3339Nano after millisecond truncation and include it in `canonicalReconnect`; encode zero as the empty canonical value.
- [ ] Add `WithLegacyV1AcceptUntil`; normalize its cutoff and reject invalid TTL, invalid dates, and a cutoff later than `source.Now().Add(credentialTTL)`.
- [ ] Pass the configured cutoff and credential TTL from `cmd/live-gateway` into `NewReconnectCredentials`; a zero cutoff passes no migration authority.
- [ ] Change reconnect issuance to `lrt2` while keeping the public `ReconnectCredentials` method set unchanged.
- [ ] Add a dedicated `lrt1` verify branch that first rejects a zero cutoff or `!now.Before(cutoff)`, then validates the existing signature and normal token expiry before inspecting profile claims.
- [ ] In that branch, bind the exact immutable `profile.Builtin()` claims when all four profile fields are absent.
- [ ] In that branch, preserve the exact profile ID, version, snapshot hash, and engine version when all four fields are present and valid.
- [ ] Reject every `lrt1` payload containing only one, two, or three profile fields; do not repair or default a partial claim set.
- [ ] Keep worker profile/recipe/catalog re-resolution and exact comparison as defense in depth, but allow an exactly admitted authority to execute instead of returning `profile_incompatible`.
- [ ] Run the focused profile, authority, reconnect, and worker race tests.

```sh
go test -race ./internal/profile ./internal/session ./internal/gateway ./internal/handoff ./internal/worker \
  -run 'Test(Profile|ManifestDefaultRecipe|PublisherCopiesDefaultRecipe|PublishedRecipe|Reconnect(Legacy|ProfileAwareV1))' -count=20
go test -race ./internal/config ./cmd/live-gateway ./demo/live-client \
  -run 'Test.*(LegacyV1|DefaultRecipe)' -count=20
test -n "${TEST_DATABASE_URL:-}" || { echo 'TEST_DATABASE_URL is required' >&2; exit 1; }
TEST_DATABASE_URL="$TEST_DATABASE_URL" \
./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 1 \
  --expect-top-level TestLegacyRecipeDraftWithoutDefaultLoadsAndPersistsExplicitRepair -- \
  go test -json -race -tags=integration ./internal/store/postgres \
    -run '^TestLegacyRecipeDraftWithoutDefaultLoadsAndPersistsExplicitRepair$' -count=1
node deploy/scripts/run-node-test-no-skip.mjs --expect-tests 218 -- \
  node --test demo/live-client/web/*.test.mjs
```

- [ ] Run the PostgreSQL-backed vertical profile integration tests.

```sh
test -n "${TEST_DATABASE_URL:-}" || { echo 'TEST_DATABASE_URL is required' >&2; exit 1; }
TEST_DATABASE_URL="$TEST_DATABASE_URL" \
./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 3 \
  --expect-top-level TestInsuranceProfileSetupFlow,TestInsuranceRecipeUsesPinnedKnowledgeAndFailureDoesNotEndVoice,TestVerticalProfileDefaultRecipeRepair -- \
  go test -json -race -tags=integration ./test/integration \
    -run '^(TestInsuranceProfileSetupFlow|TestInsuranceRecipeUsesPinnedKnowledgeAndFailureDoesNotEndVoice|TestVerticalProfileDefaultRecipeRepair)$' -count=1
```

- [ ] Commit immutable profile handoff authority.

```sh
git add internal/profile/types.go internal/profile/types_test.go internal/profile/publisher.go \
  internal/profile/publisher_test.go internal/profile/resolver_test.go \
  internal/config/config.go internal/config/config_test.go \
  internal/config/hash.go internal/config/hash_test.go \
  internal/session/profile.go internal/session/profile_test.go internal/session/effect.go \
  internal/session/reactor.go internal/session/reactor_test.go internal/session/manager.go \
  internal/session/manager_test.go internal/session/reconnect_credentials.go \
  internal/session/reconnect_credentials_test.go internal/gateway/app.go internal/gateway/app_test.go \
  internal/gateway/handoffs.go internal/gateway/handoffs_test.go \
  internal/handoff/types.go internal/handoff/state.go internal/handoff/state_test.go internal/worker/recipes.go \
  internal/worker/recipes_test.go internal/worker/runner.go internal/worker/runner_test.go \
  internal/worker/runner_integration_test.go internal/store/postgres/profiles_integration_test.go \
  cmd/live-gateway/main.go cmd/live-gateway/main_test.go \
  demo/live-client/profile_api.go demo/live-client/profile_api_test.go demo/live-client/profile_dto.go \
  demo/live-client/web/profile-api.mjs demo/live-client/web/profile-api.test.mjs \
  demo/live-client/web/recipe-editor.mjs demo/live-client/web/recipe-editor.test.mjs \
  demo/live-client/web/setup-profile.mjs demo/live-client/web/setup-profile.test.mjs \
  demo/live-client/web/setup.html demo/live-client/web/setup.mjs demo/live-client/web/setup.test.mjs \
  demo/live-client/web/setup.css test/integration/vertical_profile_setup_test.go \
  test/integration/vertical_profile_worker_test.go
git commit -m 'fix: bind immutable profile handoff authority'
```

---

## Task 8: Make Session Failure Presentation Content-Free

**Files:**

- Create: `internal/failure/class.go`
- Create: `internal/failure/class_test.go`
- Modify: `internal/realtime/voice.go`
- Modify: `internal/realtime/s2sws/mapping.go`
- Modify: `internal/realtime/s2sws/connector.go`
- Modify: `internal/realtime/s2sws/connector_internal_test.go`
- Modify: `internal/realtime/s2sws/presentation_mapping_test.go`
- Modify: `internal/session/input.go`
- Modify: `internal/session/reactor.go`
- Modify: `internal/session/reactor_test.go`
- Modify: `internal/session/manager_test.go`
- Modify: `internal/transport/ws/handler_test.go`
- Modify: `test/integration/e2e_test.go`
- Create: `test/integration/realtime_provider_error_privacy_test.go`

**Interfaces:**

- Consumes: `func (r *Reactor) applyFailed(input FailedInput) ([]Effect, error)`, `protocol.ProtocolError`, `realtime.ErrWriteTimeout`, Task 3's exact locally generated response/session-update event IDs, and the deployment/security plan's future `reasoner.ProviderError.Class failure.Class`; durable `worker.RetryRecord.Code` remains a `string` populated only from `failure.Class.Code()` and revalidated before persistence.
- Produces: closed serialized `type failure.Class string` with constants `failure.Internal = "internal"`, `failure.Timeout = "timeout"`, `failure.RateLimited = "rate_limited"`, `failure.TemporarilyUnavailable = "temporarily_unavailable"`, `failure.ResourceExhausted = "resource_exhausted"`, `failure.InvalidResponse = "invalid_response"`, and `failure.TransportFailure = "transport_failure"`; `func (c Class) Valid() bool`; and `func (c Class) Code() string`. `Code` returns `"internal"` for an invalid cast.
- Produces: `type realtime.FailureCorrelationKind uint8` with `FailureCorrelationNone`, `FailureCorrelationResponseCreate`, `FailureCorrelationResponseCancel`, and `FailureCorrelationSessionUpdate`; `type realtime.FailureCorrelation struct { Kind FailureCorrelationKind; LocalEventID string; Epoch identity.ResponseEpoch; SessionUpdateSequence uint64 }`; and new `realtime.Event` fields `FailureClass failure.Class` and `FailureCorrelation realtime.FailureCorrelation`. For `EventError`, `ProviderEventID`, `Payload`, `Reason`, `Status`, `Text`, `Arguments`, `ResponseID`, `ItemID`, `CallID`, and `Name` are always empty.
- Produces: package-private `func mapProviderError(raw []byte) (realtime.Event, error)`, `func classifyProviderError(errorType, code string, status int) failure.Class`, and `func parseFailureCorrelation(eventID string) realtime.FailureCorrelation`. Only fixed provider type/code/status values map to the seven classes, and correlation is populated only for the exact local response-create, response-cancel, or session-update ID grammar; unknown values become `failure.Internal` plus `FailureCorrelationNone`.
- Produces: `session.FailedInput` becomes `type FailedInput struct { Class failure.Class }`. `func normalizeSessionFailure(err error) failure.Class` is the first raw-error boundary, and `func publicSessionFailure(class failure.Class) (protocol.ProtocolError, CloseEffect)` is the sole client/close mapping; both map unknown input to `failure.Internal`.

- [ ] Create `class_test.go` with an exhaustive table for all seven valid classes and their fixed serialized codes, plus empty and forged string casts that must be invalid and map to internal.
- [ ] Replace the existing `socket closed` expectation with `TestFailedInputPublicPresentationIsContentFree` using `FailedInput{Class: failure.TransportFailure}`.
- [ ] Write `TestFailedInputSentinelAbsentFromEverySurface` by passing a malicious raw sentinel only to `normalizeSessionFailure` and then carrying its returned class through the reactor.
- [ ] Assert the sentinel is absent from the client error, WebSocket close reason, replay state, captured structured logs, and every metrics label.
- [ ] Write `TestProviderErrorMappingDiscardsRawPayloadReasonAndEventID`; place distinct sentinels in message, nested body, URL, reason, unknown fields, and a foreign event ID.
- [ ] Write `TestProviderErrorMapsOnlyAllowlistedClassAndLocalCorrelation` as an exhaustive table over the fixed type/code/status allowlist, all three exact local correlation grammars, malformed IDs, and forged class strings.
- [ ] Write `TestProviderErrorSanitizedBeforeMappedQueueAdmission` and observe that the raw-frame credit is released before the content-free `EventError` is enqueued.
- [ ] Create `TestMaliciousRealtimeProviderErrorNeverEscapesAdapter` with an in-process provider and real adapter/session/WebSocket path; assert the sentinel is absent from client events, close reason, logs, metrics, recovery, durable rows, and all retained ownership snapshots.
- [ ] Update manager and integration fixtures to use `FailedInput{Class: failure.Internal}`.
- [ ] Run the internal and malicious-provider tests before creating `class.go` or changing production mapping; retain the undefined-type/old-public-message/raw-payload output as RED evidence.

```sh
go test -race ./internal/failure ./internal/realtime/s2sws ./internal/session ./internal/transport/ws \
  -run 'Test(Class|ProviderError|.*FailedInput.*ContentFree|.*FailedInputSentinel|.*CloseReason.*ContentFree)' -count=50
test -n "${TEST_DATABASE_URL:-}" || { echo 'TEST_DATABASE_URL is required' >&2; exit 1; }
TEST_DATABASE_URL="$TEST_DATABASE_URL" \
./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 1 \
  --expect-top-level TestMaliciousRealtimeProviderErrorNeverEscapesAdapter -- \
  go test -json -race -tags=integration ./test/integration \
    -run '^TestMaliciousRealtimeProviderErrorNeverEscapesAdapter$' -count=1
```

Expected: FAIL because the closed failure domain and immediate sanitized provider-error event contract do not exist yet.

- [ ] Implement the closed enum and fixed code mapping exactly:

```go
type Class string

const (
    Internal               Class = "internal"
    Timeout                Class = "timeout"
    RateLimited            Class = "rate_limited"
    TemporarilyUnavailable Class = "temporarily_unavailable"
    ResourceExhausted      Class = "resource_exhausted"
    InvalidResponse        Class = "invalid_response"
    TransportFailure      Class = "transport_failure"
)

func (c Class) Valid() bool
func (c Class) Code() string
```

- [ ] Replace `FailedInput.Source` and `FailedInput.Err` with `FailedInput.Class failure.Class`; no session input carries raw failure text.
- [ ] Validate `failure.Class.Valid()` at every session input and public-mapping boundary; invalid string casts normalize to `failure.Internal` and are never persisted or labeled verbatim.
- [ ] Add the closed `FailureCorrelationKind`, `FailureCorrelation`, and failure fields to `realtime.Event`; update clone/retained-byte helpers without charging or copying raw provider error bytes.
- [ ] In `mapServerEvent`, branch on envelope type `error` before constructing the ordinary raw-payload event and call only `mapProviderError`.
- [ ] In `mapProviderError`, decode only bounded type, code, status, and `error.event_id`; never copy the original frame or message/body/reason fields into the returned event.
- [ ] Map only the documented fixed type/code/status allowlist through `classifyProviderError`; validate the class before returning and default unknown input to `failure.Internal`.
- [ ] Parse correlation only through exact local-ID parsers; retain the bounded local ID plus typed epoch/sequence and clear all raw provider identity fields.
- [ ] Update response retirement and Task 3 session-update rejection to consume only `FailureCorrelation`; while an update is pending, an uncorrelated error terminalizes generically without guessing, while the no-update path preserves typed response correlation.
- [ ] Admit the sanitized `EventError` with its validated class before any content-free adapter terminalization so the manager constructs `FailedInput{Class: event.FailureClass}` without reparsing an error string.
- [ ] Release the raw receive-frame owner immediately after mapping and before enqueueing the sanitized event.
- [ ] Implement `publicSessionFailure` with fixed class codes, fixed client messages, close code 1011, and fixed content-free close reasons.
- [ ] Change `Reactor.applyFailed` to call only `publicSessionFailure` and never `error.Error()`.
- [ ] Implement `normalizeSessionFailure`: map `realtime.ErrWriteTimeout` to `failure.Timeout`, `realtime.ErrSessionUpdateRejected` to `failure.InvalidResponse`, `realtime.ErrProviderFailure` to `failure.Internal`, other known typed errors to their closed class, and unknown errors to `failure.Internal`; discard the raw value before constructing `FailedInput`.
- [ ] Restrict private logs to `failure.Class.Code()` and bounded trusted identifiers; omit raw code, message, body, URL, prompt, transcript, and tool data.
- [ ] Run the focused failure and adapter-mapping tests, including the new `internal/failure` selectors.

```sh
go test -race ./internal/failure ./internal/realtime/s2sws ./internal/session ./internal/transport/ws \
  -run 'Test(Class|ProviderError|.*FailedInput.*ContentFree|.*FailedInputSentinel|.*CloseReason.*ContentFree)' -count=50
```

- [ ] Run the affected integration package to prove typed inputs remain fenced correctly.

```sh
test -n "${TEST_DATABASE_URL:-}" || { echo 'TEST_DATABASE_URL is required' >&2; exit 1; }
TEST_DATABASE_URL="$TEST_DATABASE_URL" \
./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 2 \
  --expect-top-level TestMaliciousRealtimeProviderErrorNeverEscapesAdapter,TestReconnectDisconnectsProviderWithQueuedAudioAndFencesStaleOutput -- \
  go test -json -race -tags=integration ./test/integration \
    -run '^Test(MaliciousRealtimeProviderErrorNeverEscapesAdapter|ReconnectDisconnectsProviderWithQueuedAudioAndFencesStaleOutput)$' -count=1
```

- [ ] Commit the shared content-free failure domain.

```sh
git add internal/failure/class.go internal/failure/class_test.go \
  internal/realtime/voice.go internal/realtime/s2sws/mapping.go \
  internal/realtime/s2sws/connector.go internal/realtime/s2sws/connector_internal_test.go \
  internal/realtime/s2sws/presentation_mapping_test.go \
  internal/session/input.go internal/session/reactor.go \
  internal/session/reactor_test.go internal/session/manager_test.go \
  internal/transport/ws/handler_test.go test/integration/e2e_test.go \
  test/integration/realtime_provider_error_privacy_test.go
git commit -m 'fix: sanitize realtime public failures'
```

---

## Task 9: Continuous Barge-In and Recovery Acceptance Gate

**Files:**

- Modify: `test/load/bargein_test.go`
- Create: `test/load/realtime_pressure_test.go`

**Interfaces:**

- Consumes: `TestContinuousBargeWithProfileRenderingPreservesEpochAndProgress`, `TestBargeInAcknowledgedFenceSuppressesEveryStaleFrame`, `s2sws.Config.WriteTimeout time.Duration`, `s2sws.Config.ObserveOwnership func(s2sws.OwnershipSnapshot)`, `session.Config.ProfileRuntimeBytes int`, `realtime.Event.ProviderConnectionGeneration realtime.ProviderConnectionGeneration`, and `session.MetricsHooks.QueueState func(name string, depth, bytes int, buffered time.Duration)`.
- Produces: load test `func TestRealtimePressureBlockedWriteOmittedTerminalRecovery(t *testing.T)` and test-only `type realtimePressureCounters struct { OwnedGoroutines atomic.Int64; RetainedBytes atomic.Int64; Correlations atomic.Int64 }` with `func (c *realtimePressureCounters) snapshot() (goroutines, retainedBytes, correlations int64)`. The final assertions require all three values to be zero and a successful second turn.

- [ ] Preserve `TestContinuousBargeWithProfileRenderingPreservesEpochAndProgress` as the readable 100-cycle case.
- [ ] Create `TestRealtimePressureBlockedWriteOmittedTerminalRecovery` with deterministic provider write and event gates.
- [ ] Run that pressure test through a real `s2sws.Connector` pointed at an in-process WebSocket provider; wire `Config.ObserveOwnership` and `session.MetricsHooks.QueueState` into `realtimePressureCounters`.
- [ ] Add 1,025 create/cancel cycles in which the provider omits both `response.created` and `response.done`.
- [ ] Add 1,025 complete provider-VAD input turns and require turn 1,026 to succeed after a clean-boundary provider-connection rotation.
- [ ] Inject double barge plus old audio, transcript, error, and done events on both sides of each epoch fence.
- [ ] Inject a late turn-one input commit before and after provider rotation and assert neither event mutates the current media generation.
- [ ] Track owned goroutines, retained bytes, and live correlations with `realtimePressureCounters` and explicit wait groups.
- [ ] Release the provider gate after the pressure cycles and complete one clean second input/response turn.
- [ ] Assert maximum cancellation latency never exceeds `ProviderWriteTimeout` plus the test's 100 ms scheduling allowance.
- [ ] Assert the final counter snapshot is `(0, 0, 0)`; `runtime.NumGoroutine` may be diagnostic only.
- [ ] Run the complete runtime acceptance gate.

```sh
./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 1 -- \
  go test -json -race -count=1 ./cmd/live-gateway ./demo/live-client ./internal/config ./internal/failure \
    ./internal/profile ./internal/protocol ./internal/realtime/s2sws ./internal/session \
    ./internal/store/postgres ./internal/transport/ws
node deploy/scripts/run-node-test-no-skip.mjs --expect-tests 218 -- \
  node --test demo/live-client/web/*.test.mjs
test -n "${S2S_REALTIME_URL:-}" || { echo 'S2S_REALTIME_URL is required' >&2; exit 1; }
contract_tests='TestContractPartialAdmissionFailureDrainsEverySuccessfulSession,TestDialReadyPostHandshakeCloseIsNeverSkippable,TestReadinessRetriesRememberSuccessfulHandshakeAcrossServiceLoss,TestReadinessUnavailableBeforeAnyHandshakeRemainsSkippable,TestRealtimeImmediateCreateCancelThenNextTurnContract,TestS2SConnectorForwardsPCMBytesExactly,TestSimulatorDeclaredConcurrency,TestSimulatorFunctionCallOneActiveResponseAndCancellation,TestSimulatorSessionUpdateAndPCM24AppendCommitAcceptance,TestSimulatorVADImplicitResponseBindsEpochBeforeAudioAndCompletesOnce'
S2S_REALTIME_URL="$S2S_REALTIME_URL" \
./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 10 --expect-top-level "$contract_tests" -- \
  go test -json -race -tags=contract ./test/contract -count=1
test -n "${TEST_DATABASE_URL:-}" || { echo 'TEST_DATABASE_URL is required' >&2; exit 1; }
runtime_integration_tests='TestInsuranceProfileSetupFlow,TestInsuranceRecipeUsesPinnedKnowledgeAndFailureDoesNotEndVoice,TestMaliciousRealtimeProviderErrorNeverEscapesAdapter,TestReconnectDisconnectsProviderWithQueuedAudioAndFencesStaleOutput,TestVerticalProfileDefaultRecipeRepair'
TEST_DATABASE_URL="$TEST_DATABASE_URL" \
./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 5 --expect-top-level "$runtime_integration_tests" -- \
  go test -json -race -tags=integration ./test/integration \
    -run '^(TestInsuranceProfileSetupFlow|TestInsuranceRecipeUsesPinnedKnowledgeAndFailureDoesNotEndVoice|TestMaliciousRealtimeProviderErrorNeverEscapesAdapter|TestReconnectDisconnectsProviderWithQueuedAudioAndFencesStaleOutput|TestVerticalProfileDefaultRecipeRepair)$' -count=1
TEST_DATABASE_URL="$TEST_DATABASE_URL" \
./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 1 \
  --expect-top-level TestLegacyRecipeDraftWithoutDefaultLoadsAndPersistsExplicitRepair -- \
  go test -json -race -tags=integration ./internal/store/postgres \
    -run '^TestLegacyRecipeDraftWithoutDefaultLoadsAndPersistsExplicitRepair$' -count=1
runtime_load_tests='TestBargeInAcknowledgedFenceSuppressesEveryStaleFrame,TestContinuousBargeWithProfileRenderingPreservesEpochAndProgress,TestRealtimePressureBlockedWriteOmittedTerminalRecovery'
./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 30 --expect-top-level "$runtime_load_tests" -- \
  go test -json -race -tags=load ./test/load \
    -run '^(TestBargeInAcknowledgedFenceSuppressesEveryStaleFrame|TestContinuousBargeWithProfileRenderingPreservesEpochAndProgress|TestRealtimePressureBlockedWriteOmittedTerminalRecovery)$' -count=10
test_packages=$(go list -f '{{if or .TestGoFiles .XTestGoFiles}}{{.ImportPath}}{{end}}' ./...) || exit 1
test -n "$test_packages" || exit 1
./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 1 -- \
  go test -json -race -count=1 $test_packages
go vet ./...
go build ./...
git diff --check
```

- [ ] Record queue high-water marks, maximum cancellation latency, two-turn success, and the final `(0, 0, 0)` owner snapshot in the execution handoff.
- [ ] Commit the acceptance tests before requesting review so the review starts from a clean exact HEAD.

```sh
git add test/load/bargein_test.go test/load/realtime_pressure_test.go
git commit -m 'test: stress continuous realtime interruption'
```

- [ ] Require a clean worktree, record `git rev-parse HEAD`, and request an independent runtime review of that exact commit.

```sh
test -z "$(git status --porcelain)"
git rev-parse HEAD
```

- [ ] For each P0/P1/P2 finding, start from a clean worktree and write one regression that fails under the full runtime test command before editing production code.

```sh
test -z "$(git status --porcelain)"
go test -race -count=1 ./cmd/live-gateway ./demo/live-client ./internal/config ./internal/failure \
  ./internal/protocol ./internal/realtime/s2sws ./internal/session ./internal/transport/ws \
  ./internal/profile ./internal/gateway ./internal/handoff ./internal/store/postgres ./internal/worker
```

Expected: FAIL in the newly added regression for that one finding.

- [ ] Implement only that review finding and rerun the same command until it passes.

```sh
go test -race -count=1 ./cmd/live-gateway ./demo/live-client ./internal/config ./internal/failure \
  ./internal/protocol ./internal/realtime/s2sws ./internal/session ./internal/transport/ws \
  ./internal/profile ./internal/gateway ./internal/handoff ./internal/store/postgres ./internal/worker
```

Expected: PASS.

- [ ] Stage every affected runtime path for that one finding, verify no out-of-scope path is staged, and create a separate verified commit; repeat these three review-fix steps independently for every finding.

```sh
git add -- cmd/live-gateway demo/live-client internal/config internal/failure internal/gateway \
  internal/handoff internal/profile internal/protocol internal/realtime internal/session \
  internal/store/postgres internal/transport/ws internal/worker test/contract test/load test/integration
test -z "$(git diff --name-only)"
test -z "$(git status --porcelain | rg '^\?\?')"
test -z "$(git diff --cached --name-only | rg -v '^(cmd/live-gateway/|demo/live-client/|internal/(config|failure|gateway|handoff|profile|protocol|realtime|session|store/postgres|transport/ws|worker)/|test/(contract|load|integration)/)')"
git diff --cached --check
git commit -m 'fix: resolve realtime review finding'
```

- [ ] Rerun the entire runtime acceptance gate after all review-fix commits.

```sh
./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 1 -- \
  go test -json -race -count=1 ./cmd/live-gateway ./demo/live-client ./internal/config ./internal/failure \
    ./internal/profile ./internal/protocol ./internal/realtime/s2sws ./internal/session \
    ./internal/store/postgres ./internal/transport/ws
node deploy/scripts/run-node-test-no-skip.mjs --expect-tests 218 -- \
  node --test demo/live-client/web/*.test.mjs
test -n "${S2S_REALTIME_URL:-}" || { echo 'S2S_REALTIME_URL is required' >&2; exit 1; }
contract_tests='TestContractPartialAdmissionFailureDrainsEverySuccessfulSession,TestDialReadyPostHandshakeCloseIsNeverSkippable,TestReadinessRetriesRememberSuccessfulHandshakeAcrossServiceLoss,TestReadinessUnavailableBeforeAnyHandshakeRemainsSkippable,TestRealtimeImmediateCreateCancelThenNextTurnContract,TestS2SConnectorForwardsPCMBytesExactly,TestSimulatorDeclaredConcurrency,TestSimulatorFunctionCallOneActiveResponseAndCancellation,TestSimulatorSessionUpdateAndPCM24AppendCommitAcceptance,TestSimulatorVADImplicitResponseBindsEpochBeforeAudioAndCompletesOnce'
S2S_REALTIME_URL="$S2S_REALTIME_URL" \
./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 10 --expect-top-level "$contract_tests" -- \
  go test -json -race -tags=contract ./test/contract -count=1
test -n "${TEST_DATABASE_URL:-}" || { echo 'TEST_DATABASE_URL is required' >&2; exit 1; }
runtime_integration_tests='TestInsuranceProfileSetupFlow,TestInsuranceRecipeUsesPinnedKnowledgeAndFailureDoesNotEndVoice,TestMaliciousRealtimeProviderErrorNeverEscapesAdapter,TestReconnectDisconnectsProviderWithQueuedAudioAndFencesStaleOutput,TestVerticalProfileDefaultRecipeRepair'
TEST_DATABASE_URL="$TEST_DATABASE_URL" \
./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 5 --expect-top-level "$runtime_integration_tests" -- \
  go test -json -race -tags=integration ./test/integration \
    -run '^(TestInsuranceProfileSetupFlow|TestInsuranceRecipeUsesPinnedKnowledgeAndFailureDoesNotEndVoice|TestMaliciousRealtimeProviderErrorNeverEscapesAdapter|TestReconnectDisconnectsProviderWithQueuedAudioAndFencesStaleOutput|TestVerticalProfileDefaultRecipeRepair)$' -count=1
TEST_DATABASE_URL="$TEST_DATABASE_URL" \
./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 1 \
  --expect-top-level TestLegacyRecipeDraftWithoutDefaultLoadsAndPersistsExplicitRepair -- \
  go test -json -race -tags=integration ./internal/store/postgres \
    -run '^TestLegacyRecipeDraftWithoutDefaultLoadsAndPersistsExplicitRepair$' -count=1
runtime_load_tests='TestBargeInAcknowledgedFenceSuppressesEveryStaleFrame,TestContinuousBargeWithProfileRenderingPreservesEpochAndProgress,TestRealtimePressureBlockedWriteOmittedTerminalRecovery'
./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 30 --expect-top-level "$runtime_load_tests" -- \
  go test -json -race -tags=load ./test/load \
    -run '^(TestBargeInAcknowledgedFenceSuppressesEveryStaleFrame|TestContinuousBargeWithProfileRenderingPreservesEpochAndProgress|TestRealtimePressureBlockedWriteOmittedTerminalRecovery)$' -count=10
test_packages=$(go list -f '{{if or .TestGoFiles .XTestGoFiles}}{{.ImportPath}}{{end}}' ./...) || exit 1
test -n "$test_packages" || exit 1
./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 1 -- \
  go test -json -race -count=1 $test_packages
go vet ./...
go build ./...
git diff --check
```

- [ ] Request re-review of the new exact HEAD and repeat the one-finding RED/GREEN/commit cycle until the independent reviewer reports no P0/P1/P2 finding.
- [ ] Confirm the reviewed HEAD is unchanged and the worktree is clean before handing this plan to deployment/security.

```sh
git rev-parse HEAD
test -z "$(git status --porcelain)"
```

## Plan Completion Criteria

- Reconnect, offer, replacement, and shutdown cannot deadlock under deterministic race schedules.
- Provider-VAD and manual input boundaries cannot cross-contaminate turns or strand reconnect eligibility.
- Manual commit admission freezes one generation before the reactor binds its exact adapter sequence; every failed two-phase path releases its token.
- Provider input history rotates at a clean boundary before 1,024 tombstones and old-connection duplicates cannot mutate the replacement generation.
- A blocked provider write cannot hold cancellation beyond the configured operation deadline, even when the wire writer ignores context.
- A rejected or uncorrelated `session.update` cannot strand its latch; teardown joins and reconnect starts with fresh exact correlation.
- Continuous cancellation remains bounded through at least 1,025 omitted-terminal cycles and ends with a usable second turn.
- Adapter/session/profile queues and manager-wide retained profile runtimes remain within declared count and byte budgets.
- Profile prompt, tool, recipe, approval, model, registry, and knowledge authority remain immutable across client updates and reconnect.
- `Manifest.DefaultRecipeID` is the sole recipe-selection authority and survives draft UI, publication, session admission, reconnect, and worker execution unchanged.
- Pre-field recipe drafts remain explicitly repairable in `/setup`, while pre-field published recipe snapshots fail closed until a repaired version is republished.
- Production rejects `lrt1` by default; any migration window is canonically hashed, short-lived, and exclusive at its cutoff.
- Provider errors are sanitized before mapped-queue admission, and no provider/internal sentinel appears in public realtime output or close reasons.
