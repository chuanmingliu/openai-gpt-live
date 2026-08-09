# Testing

The repository has four deliberately separate suites. The default suite is
network-free. PostgreSQL, the local speech simulator, and DeepSeek are selected
only by an explicit build tag and environment configuration.

## Suite matrix

| Suite | Gate | External dependency | Command |
|---|---|---|---|
| Unit, fuzz seeds, and static package tests | Pull request | None | `go test -race -count=1 ./...` |
| Deterministic integration | Pull request when PostgreSQL 16 is available | PostgreSQL 16 only | `TEST_DATABASE_URL=postgres://live:live@127.0.0.1:5432/live?sslmode=disable go test -race -count=1 -tags=integration ./test/integration/...` |
| Local simulator contract | Nightly/manual | Configured speech-to-speech service | `S2S_REALTIME_URL=ws://127.0.0.1:8765/v1/realtime go test -count=1 -tags=contract ./test/contract/...` |
| DeepSeek live provider | Manual only; never a pull-request gate | Configured DeepSeek endpoint | `go test -count=1 -tags=liveprovider ./test/liveprovider/...` |
| 250-session capacity | Nightly/manual | Disposable in-process gateway/provider | `go test -count=1 -tags=load -run '^TestGateway250Sessions$' -timeout=20m ./test/load/...` |
| Soak | Manual release (2h) | Disposable in-process gateway/provider | `LIVE_SOAK_DURATION=2h go test -count=1 -tags=soak -run '^TestGatewaySoak$' -timeout=150m ./test/load/...` |
| Chaos | Nightly/manual | Explicit disposable Compose/Toxiproxy topology | `go test -count=1 -tags=chaos -timeout=30m ./test/chaos/...` |

`make load-smoke` and `make load-evidence` are deliberately different gates.
Both require an absolute `LIVE_EVIDENCE_DIR` and emit a new JSONL bundle with a
SHA-256 for every command artifact. Smoke runs the two-second capacity harness,
the exact 20-sample barge test, and the ten-second soak harness. Formal evidence
runs the race/unit gate, exact ten-minute capacity with the checked-in host
profile and SLO enforcement, the exact barge selector, all six application-path
chaos cases, and the exact two-hour soak. A profile mismatch, absent chaos
topology, shortened duration, skipped scenario, or failed SLO prevents a passed
formal manifest.

The tagged packages are absent from `go test ./...` by design. Confirm discovery
without running a service:

```bash
go test -tags=integration ./test/integration/... -list .
go test -tags=contract ./test/contract/... -list .
go test -tags=liveprovider ./test/liveprovider/... -list .
go test -tags=load ./test/load/... -list .
go test -tags=soak ./test/load/... -list .
go test -tags=chaos ./test/chaos/... -list .
```

Current exact tagged discovery (Task 22 Round 3):

```text
# integration
TestTwoLiveWebSocketsKeepPCMEventsAndResponseEpochsIsolated
TestVoiceContinuesDuringDurableHandoff
TestDroppedListenerNotificationRepairsThroughPollAndCursor
TestDuplicateCompletionHasOneExecutorEffectAndOneClientTerminal
TestApprovalDecisionsTraverseSessionProtocolPostgresAndRunner
TestShadowTransitionSuppressesDurableClientAndProviderPresentation
TestUnsafeWorkerResultSuppressesDurableClientAndProviderPresentation
TestWorkerCompletionDuringSpeechRemainsVisualOnly
TestProviderSteerAndCancelTraverseDurableWorkerLifecycle
TestRunnerCancellationRestartRecoversExpiredLease
TestReconnectDisconnectsProviderWithQueuedAudioAndFencesStaleOutput
TestPostgresServicePersistsSteeringAndCancelRequest
TestPostgresServiceCreateIsIdempotentAfterAmbiguousCallerOutcome
TestPostgresLeaseExpiryFencesOldOwner
TestPostgresDuplicateCompletionCreatesOneTerminalEvent
TestPostgresDeliveryCursorReadsDurableSuffixInOrder
TestPostgresEventReadsAreScopeIsolated
TestApprovalResolutionValidationRejectsIncompleteRecords
TestPostgresDeletionTombstoneRejectsRecovery
TestRetentionUserAndTTLDeletionProtectLeaseCascadeChildrenAndFenceClients
TestOIDCExpiryRejectsBeforeSessionAdmission
TestDistributedAdmissionLimitsSerializeAcrossInstances
TestFeatureGateAndSafetyHelpersFailClosed
TestPostgresCursorRejectsSkippedSequenceAndPreservesSuffix

# contract
TestDialReadyPostHandshakeCloseIsNeverSkippable
TestReadinessRetriesRememberSuccessfulHandshakeAcrossServiceLoss
TestReadinessUnavailableBeforeAnyHandshakeRemainsSkippable
TestS2SConnectorForwardsPCMBytesExactly
TestContractPartialAdmissionFailureDrainsEverySuccessfulSession
TestSimulatorSessionUpdateAndPCM24AppendCommitAcceptance
TestSimulatorFunctionCallOneActiveResponseAndCancellation
TestSimulatorDeclaredConcurrency
TestSimulatorVADImplicitResponseBindsEpochBeforeAudioAndCompletesOnce

# liveprovider
TestDeepSeekAuthenticationModelSSEAndUsage
TestDeepSeekCancellationPropagatesAfterRequestStarts
TestDeepSeekLiveCancellationAfterFirstChunk
```

Every test operation has a deadline. Normal deterministic cases should finish
within seconds. A simulator VAD/STT/LLM/TTS turn is allowed up to 90 seconds,
and a live-provider request up to 45 seconds. A timeout is a failure after a
connection or provider request has started.

Task 24 chaos tests require all fifteen explicit names before any connection:
`CHAOS_DATABASE_URL`, `CHAOS_TOXIPROXY_URL`, `CHAOS_POSTGRES_PROXY`, `CHAOS_VOICE_PROXY`,
`CHAOS_DEEPSEEK_PROXY`, `CHAOS_VOICE_CONTROL_URL`,
`CHAOS_DEEPSEEK_CONTROL_URL`, `CHAOS_GATEWAY_URL`,
`CHAOS_GATEWAY_HEALTH_URL`, `CHAOS_WORKER_HEALTH_URL`, `CHAOS_BEARER_TOKEN`,
`CHAOS_OIDC_CONTROL_URL`,
`CHAOS_COMPOSE_PROJECT`, `CHAOS_COMPOSE_BASE`, and
`CHAOS_COMPOSE_OVERRIDE`. `CHAOS_DATABASE_URL` is the in-network disposable
PostgreSQL TLS route through `chaos-edge:15433`; there is no database URL alias. For
the default disposable Compose ports the other URLs
are `http://127.0.0.1:18474`, the proxy names are `postgres`, `voice`, and
`deepseek`, provider controls are `http://127.0.0.1:18101` and
`http://127.0.0.1:18102`, the WebSocket is
`ws://127.0.0.1:18080/v1/realtime`, and gateway/worker admin bases are
`http://127.0.0.1:19092` and `http://127.0.0.1:19093`. Compose file names must
be absolute; use a unique disposable project and the repository's
`deploy/docker-compose.yml` plus `deploy/compose.chaos.yml`. Missing names skip
cleanly before connection; `make chaos` and formal evidence fail prerequisite
checks instead of reporting a skipped suite as evidence. Every fault has a
unique toxic or provider control and bounded cleanup.

The chaos override selects PostgreSQL trust authentication only inside the
disposable Compose network, avoiding a materialized test credential, and uses
the required `CHAOS_DATABASE_URL`. Its deterministic voice and DeepSeek peers expose only
bounded fault controls and timing/count statistics. The standard deployment
does not inherit this override.

## Deterministic integration

Use PostgreSQL 16. The explicit `TEST_DATABASE_URL` path creates a random schema
for each test and drops it during cleanup, so the configured role needs schema
create/drop permission. The database itself must be disposable test data.

```bash
docker run --name gpt-live-test-postgres --rm \
  -e POSTGRES_USER=live -e POSTGRES_PASSWORD=live -e POSTGRES_DB=live \
  -p 127.0.0.1:5432:5432 postgres:16-alpine
```

Then, in another terminal:

```bash
TEST_DATABASE_URL=postgres://live:live@127.0.0.1:5432/live?sslmode=disable \
  go test -race -count=1 -tags=integration ./test/integration/...
```

When `TEST_DATABASE_URL` is unset, the existing test harness may use a
PostgreSQL 16 Testcontainers instance. The Task 22 integration command uses the
explicit URL so CI behavior is predictable. Realtime and DeepSeek dependencies
inside this suite are in-process deterministic fakes; this suite makes no
external provider call.

The vertical cases drive a real downstream WebSocket, session manager,
transcript and handoff coordinators, PostgreSQL repository, LISTEN-backed event
pump, delivery cursor, worker runner, and an in-process SSE reasoner. They cover
voice continuity, visual-only completion during speech, provider-originated
steer/cancel through worker cancellation, actual runner cancellation and
expired-lease restart, reconnect with queued PCM and strict durable replay,
poll repair after a lost LISTEN notification, simultaneous WebSocket PCM/event
isolation and stale response-epoch fencing, duplicate completion idempotency,
approval approve/deny/expiry through the downstream protocol and real runner,
shadow/unsafe presentation suppression, and retention deletion with active-lease
protection, child-table cascade, and post-delete recovery fences. Tests whose
names begin with `TestPostgres` are deliberately narrower repository/service
supplements; they do not claim a full transport lifecycle.

The coordinator's exact crash point is deliberately package-private. Keep that
failpoint private and include its production-boundary regressions in the Task 22
gate instead of weakening the seam for an external test:

```bash
go test -race ./internal/gateway \
  -run '^(TestHandoffJournalReplaysCrashAfterOperationWithoutManualReenqueue|TestTranscriptJournalReplaysCrashAfterAppendWithoutManualReenqueue)$' \
  -count=20
```

Task 22 also updates one existing test,
`internal/retention/sweeper_test.go`, so its manual-clock retry waits on an
observable transition under the race detector. No retention production code,
other production package, migration, or simulator source is changed.

## Local speech-to-speech simulator contract

The checked-out simulator at `/Users/chuanmingliu/speech-to-speech` documents
this source-checkout command:

```bash
cd /Users/chuanmingliu/speech-to-speech
uv run speech-to-speech \
  --mode realtime \
  --stt parakeet-tdt \
  --llm_backend transformers \
  --tts kokoro \
  --model_name "Qwen/Qwen3-4B-Instruct-2507" \
  --llm_device mps \
  --llm_torch_dtype float16 \
  --enable_live_transcription
```

Start only a profile already configured for the local machine. Model downloads,
provider credentials, microphone access, or edits to the simulator checkout are
not part of this test command. The default documented endpoint is
`ws://127.0.0.1:8765/v1/realtime`.

```bash
S2S_REALTIME_URL=ws://127.0.0.1:8765/v1/realtime \
S2S_CONTRACT_CONCURRENCY=1 \
  go test -count=1 -tags=contract ./test/contract/...
```

`S2S_CONTRACT_CONCURRENCY` is optional and must be from 1 through 64. Set it to
the configured pipeline-pool capacity; the suite opens exactly that many
simultaneous sessions. The tests use generated bounded PCM16LE at 24 kHz and
never use recorded audio. A deterministic local WebSocket peer byte-compares
the Go connector's decoded PCM, while the simulator case makes the narrower
documented claim that append/commit is accepted because that protocol exposes
no audio checksum or identity acknowledgement. The simulator suite covers
session creation/update, conversation item creation, response creation,
one-active-response enforcement, cancellation, tool-call configuration,
VAD/final transcription, implicit response binding, audio, and exactly one
matching response completion. The simulator implements bounded
WebSocket `input_audio_buffer.commit`; its current Realtime Engine documentation
and router do not declare WebSocket `input_audio_buffer.clear`, so clear is not
asserted as a Go framework requirement.

Simulator readiness is observed by connecting through the real Go connector.
Only the simulator's exact temporary `session slots are in use` event is
retryable. Readiness retains cumulative handshake history: after any attempt
upgrades successfully, every later network, timeout, close, or protocol error
fails even if that later attempt itself fails before upgrade. Only an endpoint
that never handshook is eligible for the unconfigured/unreachable skip. The
suite has no fixed settling sleep. Partial concurrency admission always drains
every result and immediately closes all successful peers, and all
connector/session cleanup uses bounded contexts or immediate WebSocket close.

Known local-checkout blocker (observed 2026-08-08): the implicit VAD response
path currently emits response-specific text/audio before bindable
`response.created` metadata. In the simulator source,
`handlers/response.py:on_assistant_text` establishes the current response ID,
then `handlers/audio.py:begin_audio_response` emits `response.created` only when
that ID is still absent. The Go connector correctly fails closed with the
content-free diagnostic `simulator emitted response-specific output before
bindable response.created metadata`. Keep the selected VAD test enabled and
failing until the simulator event order is corrected; do not waive epoch
binding. The remaining simulator contract cases can be isolated with:

```bash
S2S_REALTIME_URL=ws://127.0.0.1:8765/v1/realtime \
S2S_CONTRACT_CONCURRENCY=1 \
  go test -count=1 -tags=contract ./test/contract/... \
  -run 'TestSimulator(SessionUpdateAndPCM24AppendCommitAcceptance|FunctionCallOneActiveResponseAndCancellation|DeclaredConcurrency)$'
```

If `S2S_REALTIME_URL` is absent or the endpoint is unreachable, contract tests
skip with a reason. Once a WebSocket handshake succeeds, protocol mismatches do
not become skips.

## DeepSeek live-provider suite

The suite requires all three variables below. Values belong in the process
environment or an operator-managed secret store, never in source files:

- `DEEPSEEK_API_KEY`
- `DEEPSEEK_BASE_URL`
- `DEEPSEEK_MODEL`

Run:

```bash
go test -count=1 -tags=liveprovider ./test/liveprovider/...
```

Missing configuration produces a clear skip. After a request is attempted,
authentication, transport, SSE, usage, model, or cancellation failures fail the
test. Cancellation is synchronized after response headers or a first chunk,
not pre-cancelled before the HTTP request. The fixture is fixed, synthetic, and
bounded. The suite never logs or
snapshots credentials, headers, provider bodies, prompt text, chunk text,
reasoning, tool arguments, or result text.

## Privacy and secret handling

- Do not use `go test -v` instrumentation that prints request or response
  payloads. Test failures report bounded event types, status classes, codes,
  counts, identifiers, and lifecycle state only.
- Never place credentials in command history, test flags, fixtures, golden
  files, `t.Log`, failure formatting, or environment dumps.
- Never read `.env`, shell history, provider caches, or secret-bearing config as
  part of test discovery. Configure variables through the existing operator
  environment.
- Raw/base64 audio, transcripts, prompts, reasoning deltas, tool arguments, and
  worker results must not appear in test output. Generated PCM stays ephemeral.
- Run live-provider tests only in an isolated manual environment. They are not
  a pull-request or automatic nightly gate.

## Failure triage

- `configured TEST_DATABASE_URL is unreachable`: verify PostgreSQL 16 is
  listening on the URL and the role can create/drop schemas. Do not point the
  suite at production.
- migration or relation errors: verify the tagged command targets this checkout
  and uses a fresh test schema. Run `go test -tags=integration ./test/integration -list .`
  to distinguish discovery from database startup.
- simulator handshake failure: verify the endpoint uses `ws://` or `wss://`,
  ends in `/v1/realtime`, and the configured service has a free pipeline unit.
- `session_limit_reached`: set `S2S_CONTRACT_CONCURRENCY` to the declared pool
  capacity and stop unrelated simulator clients.
- simulator VAD timeout: verify live transcription, VAD, STT, LLM, and TTS are
  all enabled in the selected profile. A connected-but-incomplete pipeline is a
  contract failure, not a skip.
- DeepSeek live-provider failure: check variable presence, selected model access,
  endpoint compatibility, and account limits outside test logs. Provider bodies
  and credentials must remain scrubbed.
- race-only failure: rerun the exact suite with `-race -count=10` and preserve
  only stack traces and content-free lifecycle metadata.

Before merging, also run:

```bash
go vet ./...
go build ./cmd/...
go mod tidy -diff
test -z "$(gofmt -l .)"
git diff --check
```
