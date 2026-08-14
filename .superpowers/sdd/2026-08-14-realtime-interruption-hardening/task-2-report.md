# Task 2 report: turn fencing, implicit-response suppression, and durable approval ownership

## Scope

Base: `b15252e`

Owned production files:

- `internal/session/reactor.go`
- `internal/session/manager.go`
- `internal/gateway/app.go`
- `internal/gateway/approvals.go`
- `internal/realtime/s2sws/connector.go`

Owned tests:

- `internal/session/reactor_test.go`
- `internal/session/manager_test.go`
- `internal/gateway/app_test.go`
- `internal/gateway/approvals_test.go`
- `internal/realtime/s2sws/connector_test.go`

The pre-existing dirty `demo/live-client/web`, `docs/research`, and plan files were not edited or staged. Review item 8 was not implemented.

## RED evidence

1. `TestNewSpeechItemRejectsLateOlderInputTranscriptsWithoutMutation` showed both stale paths after `speech_started(B)`: delta A published and replaced B with revision A:3; completion A also allocated durable transcript sequence 1 and emitted `TranscriptRequestedEffect`.
2. `TestImplicitCreatedWhileSpeakingBindsCancelsAndNeverPublishes` showed an implicit `response.created` while `UserSpeaking` advanced to streaming and published `response.created` instead of reconciling the adapter response.
3. `TestEmptyIDCancelTombstonesExactlyOnePendingCreateAndAllowsNextImplicitResponse` showed create(epoch 7) followed by the supported empty-ID cancel(epoch 8) left epoch 7 pending; a late created/done was published and a provider that omitted created would poison the next metadata-less implicit response.
4. `TestEmptyIDCancelRefusesAmbiguousPendingCreatesWithoutDeletingEither` showed an empty-ID cancel succeeded with two pending explicit epochs. That made an “oldest pending” cleanup unsafe and proved the need for an exact-one invariant rather than map/order deletion.
5. `TestManagerTransfersAcceptedApprovalBeyondSessionDisconnect` initially did not compile because manager intake exposed only the session-owned `ApprovalStore` path. The accepted request could not cross a context-free app dispatcher seam.
6. The gateway coordinator REDs initially did not compile because no bounded approval coordinator, work type, or trusted dispatcher existed.
7. Full gateway verification exposed `TestGatewayDropsLateAudioFromInterruptedEpoch`: provider audio read after `speech_started` could overtake asynchronous speech-edge application. Synchronizing only the pure reactor apply made the speech fence authoritative before later media.

## Implemented contracts

### Provider input-item fence

- The reactor retains one ephemeral `currentProviderInputItem` established only by `speech_started`.
- Once a provider item has been observed, transcription delta/completion for another item is silently rejected before event-ID, revision, effect-sequence, durable-sequence, active-turn, or persistence mutation.
- Providers without speech edges retain the established transcription-driven item/revision compatibility path. A later transcript cannot supersede an already observed newer provider item.
- `speech_started` is synchronously applied at the manager's pure reactor boundary, so later provider media cannot overtake the fence. It still waits for no provider send, client write, store call, or safety call.

### Implicit response while speaking

- An implicit created frame while `UserSpeaking` is explicitly bound to a fresh epoch, immediately fenced again, flushed, and cancelled by its known response ID.
- The reactor remains terminal, publishes no `response.created`, streams no old bind-epoch media, and suppresses matching cancellation completion/done/error UX.
- The earlier pending-response barge contract is unchanged: cancel-before-response-ID still sends an empty response ID.

### Empty-ID cancelled-pending correlation

- The adapter transfers a pending create into `cancelledPending[createEpoch] = cancelEpoch` only when exactly one pending create exists. Zero pending remains a provider-only empty-ID cancel; multiple pending fails closed before wire output and deletes neither epoch.
- Pending plus cancelled-pending entries share the existing limit of 64. A withholding provider therefore cannot grow tombstones without bound.
- A late created carrying the exact create epoch is consumed into a suppressed response correlation at the cancel epoch. Its response-scoped frames are dropped through done; done makes the history entry terminal.
- A create rejection removes the exact cancelled-pending tombstone. The next metadata-less implicit response is no longer poisoned by the cancelled explicit epoch.

### App-owned approval resolution

- Session intake builds a trusted request from authenticated tenant/user, current conversation/session/incarnation, validated decision, idempotency key, reason, and one UTC resolution time.
- Intake performs a nonblocking admission through `ApprovalResolutionDispatcher`; no session context is passed. Successful app-queue admission completes the session intake boundary, so disconnect cannot fossilize reconnect as “not at boundary.”
- `ApprovalCoordinator` owns a 64-entry queue. Protocol field limits bound each work item; invalid scope/resolution fails before admission.
- Each admitted work item receives a five-second absolute deadline. Store resolution runs under the app `Run` context plus that deadline, never `sessionCtx`.
- The exact caller-supplied idempotency key and resolution time reach `ResolveApproval` unchanged. Store idempotency remains authoritative.
- Resolution is called exactly once for each admitted queue entry. Store rejection/timeout produces an optional typed `ApprovalResolutionCompletedInput` without terminating the coordinator.
- Typed completion is submitted only to the exact `(sessionID, incarnation)` via `Manager.Submit`. A disconnected or replaced incarnation returns `ErrSessionNotActive`; that stale UX completion is ignored and cannot cancel or undo the store operation.
- `App.Run` supervises the approval coordinator alongside the existing app-owned durable coordinators, and its result channel is sized for all six possible coordinator loops.

## Verification

Focused session race tests, including stale input, implicit speaking suppression, approval disconnect, and the nonblocking speech fences:

```text
GOCACHE=/private/tmp/gpt-live-task2-go-cache go test -race ./internal/session -run '^(TestNewSpeechItemRejectsLateOlderInputTranscriptsWithoutMutation|TestImplicitCreatedWhileSpeakingBindsCancelsAndNeverPublishes|TestManagerTransfersAcceptedApprovalBeyondSessionDisconnect|TestSpeechStartedAdvancesFenceWhileEarlierVisualWriteIsBlocked|TestSpeechStartedAdvancesFenceWhileEarlierProviderSendIsBlocked|TestSpeechStartedAdvancesFenceWhilePresentationSafetyIsBlocked)$' -count=20 -timeout=240s
```

PASS.

Focused gateway race tests for bounded approval ownership and the speech fence:

```text
GOCACHE=/private/tmp/gpt-live-task2-go-cache go test -race ./internal/gateway -run '^(TestApprovalCoordinatorCompletesAcceptedResolutionOnceAfterLiveSessionDisconnect|TestApprovalCoordinatorQueueIsBounded|TestApprovalDispatcherBindsOnlyTrustedScopeAndActor|TestNewWiresApprovalResolutionThroughAppOwnedCoordinator|TestGatewayDropsLateAudioFromInterruptedEpoch)$' -count=20 -timeout=240s
```

PASS.

Focused adapter race tests for empty-ID encoding, exact tombstones, ordinary named cancellation, correlation progress, and saturated manager composition:

```text
GOCACHE=/private/tmp/gpt-live-task2-go-cache go test -race ./internal/realtime/s2sws -run '^(TestEmptyIDCancelTombstonesExactlyOnePendingCreateAndAllowsNextImplicitResponse|TestEmptyIDCancelRefusesAmbiguousPendingCreatesWithoutDeletingEither|TestCancelOmitsAnEmptyResponseID|TestCancelAdvancesOnlyTerminalCorrelation|TestMappingLoopServicesCorrelationWhileEventDeliveryIsBackpressured|TestManagerBargeCancelProgressesThroughSaturatedAdapterDelivery)$' -count=20 -timeout=240s
```

PASS.

Full affected suites:

```text
GOCACHE=/private/tmp/gpt-live-task2-go-cache go test ./internal/session ./internal/gateway ./internal/realtime/s2sws -count=1 -timeout=300s
GOCACHE=/private/tmp/gpt-live-task2-go-cache go test -race ./internal/session ./internal/gateway ./internal/realtime/s2sws -count=1 -timeout=300s
GOCACHE=/private/tmp/gpt-live-task2-go-cache go vet ./internal/session ./internal/gateway ./internal/realtime/s2sws
GOCACHE=/private/tmp/gpt-live-task2-go-cache go test ./... -count=1 -timeout=300s
git diff --check
```

All passed. Listener-backed gateway/adapter tests used loopback permission; no listener test was weakened.

## Self-review

- Checked stale item rejection occurs before all synth-ID, revision, sequence, active-turn, and durable-effect mutation.
- Checked no-speech-edge compatibility remains covered by final transcript and ABA revision tests.
- Checked implicit suppression orders bind before flush/cancel and uses a newer cancel fence; old bind-epoch media is therefore rejected even if binding releases buffered adapter frames.
- Checked the cancelled-pending structure is explicitly create-epoch correlated, bounded together with pending creates, and never deletes an arbitrary map/oldest epoch.
- Checked ordinary named cancellation correlation and empty-ID provider JSON remain unchanged.
- Checked app approval queue admission is the disconnect-safe ownership point, while store deadline and live completion are independently bounded.
- Checked trusted actor/scope cannot be supplied by the client, idempotency/time are not regenerated in the coordinator, and stale completion targets no replacement incarnation.
- Checked no Task 1 FIFO/terminal ownership contract or review item 8 behavior was changed.
