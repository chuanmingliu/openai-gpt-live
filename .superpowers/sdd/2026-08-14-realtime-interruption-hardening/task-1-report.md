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
- `internal/session/presentation_manager_test.go` (narrow ownership expansion authorized for the obsolete replacement assertion)

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

Round-two review REDs, captured before the corresponding production edit:

1. Exact reviewer repro `TestReplacementFencesTimedOutAcceptedOfferBeforeOldEffects` timed out at the synchronous replacement registration: the test-only apply gate was held inside `presentationMu`, so replacement could not publish and the gate could not be released. The two shadow regressions also read the obsolete ordinary inbox and respectively observed `ResponsePending` and timed out.
2. New `TestFailedInitialSameScopePublicationRestoresPriorRuntime` and `TestFailedReconnectSameScopePublicationRestoresPriorRuntime` both observed no current runtime after rollback. Registration had fenced the prior generation but rollback only removed the failed publication.
3. New `TestExpiredOfferRotationWriteKeepsSessionAndCredentialRetryable` showed an expired offer context terminating the current session after durable credential rotation. New `TestCommittedRotationRetriesSameCredentialAfterClientQueueOverflow` showed a committed cursor becoming an unrecoverable early-return after notification queue overflow.
4. New adapter regression `TestConcurrentTerminationDrainsAcceptedMappedFIFO` observed the concurrent writer error before the three accepted private-FIFO events. Outbound failure published terminal state outside the mapping owner.
5. New client and durable completion tests timed out waiting for typed completion inputs on the reserved lane, including with the ordinary inbox saturated; the two writer results still bypassed the reactor boundary.
6. During complete race verification, `TestDispatchFlushFencesActiveAndQueuedResponseControlsBeforeCancelOrTerminate/cancel` exposed a client error write racing ahead of provider cancellation. Making the provider observation unbuffered made this RED deterministic.

## Completion and linearization contracts

- Adapter mapped events enter a mapping-loop-owned FIFO. FIFO-head delivery and bind/cancel/register correlations remain selectable when public delivery is full. Socket intake stops at the configured watermark.
- A terminal socket/protocol error or concurrent outbound failure is requested through the mapping owner. It stops further reads, drains all already accepted mapped events in order, then publishes terminal state. Transferred channel storage is detached without scrubbing; only still-owned teardown entries are zeroed.
- The reactor performs pure apply and bounded queue admission. It never waits for provider send, client write, active-write cancellation, durable dispatch, or safety policy work.
- Voice sends reconcile through `VoiceCommandCompletedInput` on a reserved completion lane. Outstanding commands are keyed by exact sequence and kind; unmatched/synthetic completion cannot decrement the count.
- The reserved completion channel is bounded at `ControlEvents + 3*WriterControlEvents`, covering maximum accepted safety, voice, client, and durable work without consuming ordinary reactor credit. Reserved completions receive priority, including while a single create is deferred behind cancel completion.
- Client and durable writers publish typed success/failure completion inputs on that reserved lane. Required client-write callers are correlated by exact sequence; durable work is correlated by exact sequence and kind. Writer failures remain session-fatal when the typed completion is applied, rather than escaping through an untyped writer goroutine/channel path.
- Safety evaluation runs in one bounded supervised worker queue (`ControlEvents`). Its correlated `HandoffSafetyCompletedInput` returns through the reserved lane and carries the original offer completion token and acceptance generation. Policy errors retain the existing bounded suppress behavior; internal/session cancellation remains fatal/terminating as before.
- Exact client-write completion is now a typed reactor input correlated to the buffered offer token. Cursor success still requires that exact visual write.
- Same-scope offer admission snapshots and rechecks the current runtime while serialized. Replacement closes the prior runtime and advances its acceptance generation before publication. Final apply/effect admission rechecks that generation under the runtime-local presentation mutex. A failed publication restores the prior runtime's exact accepting flag, generation, and current pointer while it remains active; successful publication clears the rollback snapshot. Different scopes remain independent.
- Offer timeout bounds admission, caller wait, and context-aware reconnect-rotation lock/store work. Once rotation commits, the reconnect client notification belongs to the session/client writer rather than the expiring offer. Delivery is marked only by its exact typed completion; an undelivered committed credential remains retryable at the same cursor. Offer expiry never quiesces the runtime.
- Client effects ordered after a provider command use a bounded typed send barrier between the two writers. This preserves cancel-before-client-error composition while the sole reactor continues processing completions and new inputs.
- Empty-ID cancel behavior and cursor-after-write behavior are unchanged.

## Bounds and ordering

- Public adapter channel: 256 events.
- Mapping-loop socket FIFO watermark: 256 events. Bind can additionally transfer only the separately bounded implicit aggregate (64 events).
- Reactor ordinary inbox: `ControlEvents`, with explicit slots.
- Reactor completion lane: `ControlEvents + 3*WriterControlEvents`.
- Safety work queue: `ControlEvents`.
- Voice, client, and durable writer queues: independent `WriterControlEvents` bounds.
- One response-create may be deferred while cancellation is outstanding. It resumes only after exact cancel completion.
- Adapter accepted events retain FIFO/no-drop ordering, including immediately before terminal close.

## Verification

Round-two final commands:

```text
go test -race ./internal/session -run '^(TestReplacementFencesTimedOutAcceptedOfferBeforeOldEffects|TestShadowFromStartSuppressesEveryHandoffAcceptanceReinjectionAndReconcilesState|TestQueuedHandoffAcceptanceSuppressionReconcilesBeforeProviderSend|TestFailedInitialSameScopePublicationRestoresPriorRuntime|TestFailedReconnectSameScopePublicationRestoresPriorRuntime|TestExpiredOfferRotationWriteKeepsSessionAndCredentialRetryable|TestCommittedRotationRetriesSameCredentialAfterClientQueueOverflow|TestClientWriterPublishesTypedCompletionOnReservedLane|TestDurableWriterPublishesTypedCompletionOnReservedLane|TestClientWriterPublishesTypedFailureCompletion|TestDurableWriterPublishesTypedFailureCompletion)$' -count=20 -timeout=240s
go test -race ./internal/session -run '^TestDispatchFlushFencesActiveAndQueuedResponseControlsBeforeCancelOrTerminate/cancel$' -count=20 -timeout=30s
go test -race ./internal/realtime/s2sws -run '^(TestConcurrentTerminationDrainsAcceptedMappedFIFO|TestProviderErrorPrecedesPolicyClose|TestMappingLoopServicesCorrelationWhileEventDeliveryIsBackpressured|TestManagerBargeCancelProgressesThroughSaturatedAdapterDelivery)$' -count=20 -timeout=240s
go test ./internal/session -count=1 -timeout=240s
go test ./internal/realtime/s2sws -count=5 -timeout=180s
go test -race ./internal/session -count=3 -timeout=300s
go test -race ./internal/realtime/s2sws -count=3 -timeout=300s
```

All passed. Listener-backed adapter/composition tests used the required loopback permission. No listener test was weakened.

## Self-review

- Checked presentation/runtime lock order and ensured no external client/provider/store work runs under `activeMu`; the artificial apply gate now runs before the production presentation critical section.
- Checked replacement re-reads the snapped current runtime, fences its generation before publication, and restores the exact prior state on failed publication while that runtime remains active.
- Checked every voice, safety, client, and durable typed completion uses reserved bounded admission and does not release ordinary inbox credit.
- Checked pending-voice decrement is exact and cannot underflow on duplicate, mismatched, or shadow-suppressed completion.
- Checked safety cancellation cannot strand the sole reactor; its bounded pending state is session-owned.
- Checked reconnect rotation acquisition/store is context-aware, post-commit notification is session-owned, the offer caller observes its local timeout, and same-cursor retry reuses the exact committed credential until delivery completes.
- Checked accepted adapter FIFO entries drain before read or concurrent-writer terminal errors become visible.
- Checked provider cancel completion precedes dependent client error writes without waiting on the sole reactor.
- Checked only Task 1 files plus this report are staged.
