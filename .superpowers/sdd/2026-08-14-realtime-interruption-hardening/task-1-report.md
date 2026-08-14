# Task 1 report: nonblocking realtime reactor and adapter correlation progress

## Scope

Base: `e75213d45e35509bb6865e30b59d6653696e52e8`

Owned production files:

- `internal/realtime/s2sws/connector.go`
- `internal/session/manager.go`
- `internal/session/reactor.go`

Owned tests:

- `internal/realtime/s2sws/connector_internal_test.go`
- `internal/session/manager_test.go`
- `internal/session/presentation_delivery_test.go`

The pre-existing dirty `demo/live-client/web`, `docs/research`, and plan files were not edited or staged.

## RED evidence and root causes

1. `TestMappingLoopServicesCorrelationWhileEventDeliveryIsBackpressured`
   failed with `correlation did not progress while public event delivery was backpressured` after 100 ms. The sole mapping/correlation owner was blocked in the second direct send to `events`.
2. `TestSpeechStartedAdvancesFenceWhileEarlierVisualWriteIsBlocked`
   failed because the fence did not advance within 100 ms while an earlier exact approval visual write was gated. The reactor was waiting inline on `clientControl.written`.
3. `TestResponseCreateWaitsForBlockedBargeCancelCompletion`
   initially failed with `invalid session transition: response.create from state 3`. Removing the synchronous cancel acknowledgment exposed a formerly implicit ordering dependency, so a bounded single response-create deferral is now released by the typed cancel completion.
4. The first full adapter run found transferred payload/audio zeroing: the FIFO scrubbed a queue slot after channel ownership had moved. Sent slots now detach without mutating backing storage; only unsent teardown entries are scrubbed.
5. Repeated client-control capacity testing found provider intake could start before exact `session.created` readiness released the one-slot client queue. Provider reads now open only after that write completes.

## Completion contracts

- Adapter mapped events enter a mapping-loop-owned FIFO. Correlation requests and FIFO-head sends remain selectable when public delivery is full.
- The reactor performs pure apply plus bounded admission. It does not wait for client writes, voice sends, active-write cancellation completion, or durable dispatcher calls.
- Voice sends feed existing `VoiceCommandCompletedInput` values back on later reactor turns. Provider send/cancel failure remains fatal because the supervised voice writer returns the transport error.
- Durable calls run in a supervised bounded dispatcher loop and feed `DurableEffectCompletedInput` back on later reactor turns. Dispatcher failure remains session-fatal.
- Exact client-write completion is carried by the original buffered offer token. Cursor success is returned only after that exact write succeeds. An expired caller stops waiting but does not cancel apply/dispatch, quiesce the runtime, or change the stable event identity.
- Cancellation advances the atomic response fence in the first speech-start apply turn. Active audio/control writes are cancelled without a reactor-side wait, and client writer epoch/incarnation checks remain the final stale-audio rejection boundary.
- One queued `response.create` may be deferred while a cancel command is pending. It resumes only after the correlated cancel completion, preserving deterministic create-after-cancel behavior without blocking the reactor on provider I/O.
- Same-scope presentation/replacement serialization is runtime-local. `activeMu` is held only to snapshot/publish directory state; cross-scope registration, client writes, reconnect rotation, and offer waiting do not hold it.

## Bounds and ordering

- Public adapter channel: `receiveQueueCapacity` = 256.
- Mapping-loop FIFO socket-read watermark: 256. Socket reads stop at the watermark while correlations and downstream sends continue. Correlation buffer release can add only the separately bounded aggregate implicit buffer (`implicitAggregateEvents` = 64), so total adapter-owned mapped/correlation event storage remains bounded.
- Reactor inbox: configured `ControlEvents` with explicit slot accounting.
- Voice/client/durable effect queues: configured `WriterControlEvents`, each with independent slot accounting.
- Adapter FIFO preserves exact accepted mapped-event order and does not drop events.
- Explicit response creation is applied before its media is admitted to client output order; this waits only for pure apply/bounded effect admission, never client/provider/durable I/O.

## Verification

RED commands were run before production edits. Final verification commands:

```text
go test -race ./internal/realtime/s2sws -run '^TestMappingLoopServicesCorrelationWhileEventDeliveryIsBackpressured$' -count=20 -timeout=60s
go test -race ./internal/session -run '^(TestSpeechStartedAdvancesFenceWhileEarlierVisualWriteIsBlocked|TestResponseCreateWaitsForBlockedBargeCancelCompletion|TestTimedOutPresentationKeepsRuntimeAndStableEventReplayable|TestOfferTimeoutDoesNotHoldDirectoryLockOrQuiesceProvider|TestManagerClientControlAcceptsCapacityAndFailsOnFirstExcessControl)$' -count=20 -timeout=120s
go test -race ./internal/realtime/s2sws -count=1 -timeout=180s
go test -race ./internal/session -count=1 -timeout=240s
```

The listener-backed adapter full/race suites require loopback bind permission in the managed sandbox. They were rerun with that permission and passed; no listener test was weakened.

## Self-review

- Restored the pre-existing implicit-buffer zeroing test after noticing the internal test file was already tracked.
- Verified transferred adapter event storage is not scrubbed after channel send.
- Verified offer timeout paths contain no runtime stop/quiesce action and no offer deadline is attached to reactor apply, client writes, provider sends, durable dispatch, or reconnect rotation.
- Verified cancellation transport errors and durable dispatcher errors still terminate their supervised session loops.
- Review item 8 was not implemented.
