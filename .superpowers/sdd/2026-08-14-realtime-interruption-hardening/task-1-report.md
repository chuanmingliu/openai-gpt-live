# Task 1 report: nonblocking realtime reactor and adapter correlation progress

## Scope

Base: `e75213d45e35509bb6865e30b59d6653696e52e8`

Owned production files:

- `internal/realtime/s2sws/connector.go`
- `internal/session/manager.go`
- `internal/session/reactor.go`

Owned tests:

- `internal/realtime/s2sws/connector_internal_test.go`
- `internal/realtime/s2sws/connector_test.go`
- `internal/session/manager_test.go`
- `internal/session/presentation_delivery_test.go`

The pre-existing dirty `demo/live-client/web`, `docs/research`, and plan files were not edited or staged. Review item 8 was not implemented.

## RED evidence and root causes

Initial implementation REDs:

1. `TestMappingLoopServicesCorrelationWhileEventDeliveryIsBackpressured` timed out after 100 ms because the sole mapping/correlation owner blocked on a direct public-event send.
2. `TestSpeechStartedAdvancesFenceWhileEarlierVisualWriteIsBlocked` could not advance the fence while the reactor waited inline for the exact client write.
3. Async cancel exposed a create-ordering dependency: a queued `response.create` applied in `ResponseCancelRequested` before cancel completion.
4. Repeated adapter testing exposed transferred payload/audio zeroing after FIFO ownership moved to the public channel.

Review-fix REDs, each captured before its production fix (or by reverting the relevant fix in isolation):

1. `TestResponseCreateWaitsForBlockedBargeCancelCompletion` with legal `ControlEvents: 1` failed with `response.create did not resume after cancel completion`; the deferred create retained the only ordinary credit needed by completion.
2. `TestReplacementFencesTimedOutAcceptedOfferBeforeOldEffects` observed a client effect from an accepted old-runtime offer after same-scope replacement publication.
3. `TestShadowSuppressedVoiceCompletionPreservesExactOutstandingCount` covers an unmatched synthetic completion that the former unconditional decrement drove below zero.
4. `TestSpeechStartedAdvancesFenceWhilePresentationSafetyIsBlocked` failed its 100 ms fence assertion when safety evaluation was restored to the sole reactor.
5. `TestOfferDeadlineBoundsReconnectRotationAdmission` hung beyond 100 ms when reconnect rotation was restored to the original long-lived caller context.
6. The complete adapter suite failed `TestProviderErrorPrecedesPolicyClose`: the accepted final structured error could remain in the FIFO while the following close published terminal state first.
7. The new provider-send and real manager-plus-`s2sws` saturation tests cover the remaining review gaps. Against the original synchronous/direct-delivery paths, the former blocks the speech fence and the latter strands bind/cancel correlation behind a full public channel.

## Completion and linearization contracts

- Adapter mapped events enter a mapping-loop-owned FIFO. FIFO-head delivery and bind/cancel/register correlations remain selectable when public delivery is full. Socket intake stops at the configured watermark.
- A terminal socket/protocol error stops further reads, drains all already accepted mapped events in order, then publishes terminal state. Transferred channel storage is detached without scrubbing; only still-owned teardown entries are zeroed.
- The reactor performs pure apply and bounded queue admission. It never waits for provider send, client write, active-write cancellation, durable dispatch, or safety policy work.
- Voice sends reconcile through `VoiceCommandCompletedInput` on a reserved completion lane. Outstanding commands are keyed by exact sequence and kind; unmatched/synthetic completion cannot decrement the count.
- The reserved completion channel is bounded at `ControlEvents + 2*WriterControlEvents`, covering the maximum accepted safety work plus voice-writer work without consuming ordinary reactor credit. Reserved completions receive priority, including while a single create is deferred behind cancel completion.
- Durable completion was removed because successful durable dispatch has no reactor state to reconcile. Durable dispatcher failure remains session-fatal through supervision.
- Safety evaluation runs in one bounded supervised worker queue (`ControlEvents`). Its correlated `HandoffSafetyCompletedInput` returns through the reserved lane and carries the original offer completion token and acceptance generation. Policy errors retain the existing bounded suppress behavior; internal/session cancellation remains fatal/terminating as before.
- Exact client-write completion remains the buffered offer token rather than a reactor input because it reconciles caller/cursor progress, not reactor state. Cursor success still requires the exact visual write.
- Same-scope offer admission snapshots and rechecks the current runtime while serialized. Replacement closes the prior runtime and advances its acceptance generation before publication. Final apply/effect admission rechecks that generation under the same runtime-local presentation mutex, so either old bounded dispatch or replacement publication wins atomically. Different scopes remain independent.
- Offer timeout bounds admission, caller wait, and context-aware reconnect-rotation lock/store/write work. The offer context is not attached to reactor apply/effect dispatch and timeout never quiesces the runtime. An expired event remains replayable under its stable identity.
- Empty-ID cancel behavior and cursor-after-write behavior are unchanged.

## Bounds and ordering

- Public adapter channel: 256 events.
- Mapping-loop socket FIFO watermark: 256 events. Bind can additionally transfer only the separately bounded implicit aggregate (64 events).
- Reactor ordinary inbox: `ControlEvents`, with explicit slots.
- Reactor completion lane: `ControlEvents + 2*WriterControlEvents`.
- Safety work queue: `ControlEvents`.
- Voice, client, and durable writer queues: independent `WriterControlEvents` bounds.
- One response-create may be deferred while cancellation is outstanding. It resumes only after exact cancel completion.
- Adapter accepted events retain FIFO/no-drop ordering, including immediately before terminal close.

## Verification

Focused final commands:

```text
go test -race ./internal/session -run '^(TestSpeechStartedAdvancesFenceWhileEarlierProviderSendIsBlocked|TestSpeechStartedAdvancesFenceWhilePresentationSafetyIsBlocked|TestOfferDeadlineBoundsReconnectRotationAdmission|TestReplacementFencesTimedOutAcceptedOfferBeforeOldEffects|TestResponseCreateWaitsForBlockedBargeCancelCompletion|TestShadowSuppressedVoiceCompletionPreservesExactOutstandingCount)$' -count=10 -timeout=180s
go test -race ./internal/realtime/s2sws -run '^(TestMappingLoopServicesCorrelationWhileEventDeliveryIsBackpressured|TestManagerBargeCancelProgressesThroughSaturatedAdapterDelivery|TestProviderErrorPrecedesPolicyClose)$' -count=10 -timeout=180s
go test ./internal/session -count=1 -timeout=240s
go test ./internal/realtime/s2sws -count=1 -timeout=180s
go test -race ./internal/session -count=1 -timeout=240s
go test -race ./internal/realtime/s2sws -count=1 -timeout=180s
```

All passed. The listener-backed adapter/composition tests initially failed only because managed sandbox loopback bind was denied; they passed with loopback permission. No listener test was weakened.

## Self-review

- Checked presentation/runtime lock order and ensured no external client/provider/store work runs under `activeMu`.
- Checked replacement both re-reads the snapped current runtime and fences its generation before publication, including a nil-to-non-nil concurrent publication.
- Checked every typed completion uses reserved bounded admission and does not release ordinary inbox credit.
- Checked pending-voice decrement is exact and cannot underflow on duplicate, mismatched, or shadow-suppressed completion.
- Checked safety cancellation cannot strand the sole reactor; its bounded pending state is session-owned.
- Checked reconnect rotation acquisition is context-aware and its offer caller observes the configured local timeout.
- Checked accepted adapter FIFO entries drain before any terminal error becomes visible.
- Checked only Task 1 files plus this report are staged.
