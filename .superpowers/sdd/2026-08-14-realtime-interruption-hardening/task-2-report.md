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

Review-fix round 1 REDs:

1. `TestClientCreatedUserTurnRejectsLateOlderProviderTranscriptsWithoutMutation` showed `speech_started(A)`, `speech_stopped(A)`, client user item B, then late A delta/completion still replaced B; completion also allocated transcript sequence 1 and durable work.
2. `TestCancelledPendingRejectsMetadataLessCreatedAndTranscriptFirstOutput` showed metadata-less created and transcript-done-first output were reclassified as implicit with both one and two cancelled tombstones.
3. `TestApprovalCoordinatorDoesNotBlockStoreProgressOnOptionalLiveCompletion` timed out waiting for approval B's store call while approval A's optional `Submit` remained blocked.
4. `TestApprovalCoordinatorStoreDeadlineBeginsWhenQueuedWorkExecutes` observed approval B start with `context deadline exceeded` because queue and completion wait had consumed its admission-time store deadline.

Review-fix round 2 REDs:

1. `TestAudioCommitAfterClientTurnAcceptsNewProviderTranscriptAndKeepsOlderItemsFenced` showed transcript-only provider turn A, client user turn B, then `input_audio_buffer.commit` followed by provider turn C without `speech_started` produced zero effects: B remained permanently authoritative and both C delta and completion were dropped.
2. `TestCommittedAudioTurnCannotBeClaimedByAnOlderABAItem` showed the missing commit-generation model also allowed an older item B to be accepted as the next turn in an ABA sequence, mutating the active transcript and preventing the genuinely new item D from being admitted.
3. `TestAudioCommitFencesPriorTranscriptOnlyItemBeforeOpeningNewClaim` isolated the direct compatibility seam: transcript-only A followed by commit allowed a late A completion to consume the open claim, advance A's revision, and block new item B. This proved the commit boundary itself must remember the prior active item, even when no client or speech boundary had previously established provider authority.

Review-fix round 3 REDs:

1. `TestClientUserItemCannotReuseHistoricalProviderItemWithoutSpeechEdge` showed `speech_started(A) -> client B -> client A` succeeded, after which late original A output published and persisted. The shared establishment helper did not distinguish client reuse from an explicit provider speech generation.
2. `TestConsecutiveAudioCommitsRetainOneOrderedClaimPerTurn` showed three admitted commits collapsed into one boolean: B was accepted, while legitimate C was dropped and could not reach D.
3. `TestAllTranscriptOnlyHistoryItemsFenceLaterCommitClaim` showed transcript-only A and B followed by client C retained only B/C; late A stole the next commit claim, mutated/persisted revision A:4, and caused genuine D to be dropped.
4. `TestAudioCommitGenerationLedgerFailsClosedAtCapacity` showed the 65th outstanding commit was still admitted because the boolean had no cardinality or capacity contract.
5. Self-review added `TestCompletedSpeechGenerationDoesNotConsumeLaterAudioCommit`: after speech-established A had already completed, its stale speech-to-commit marker consumed a later commit for B, causing legitimate B to be dropped. Completion now closes an unmatched speech generation before any later commit boundary.

## Implemented contracts

### Provider input-item fence

- The reactor now owns an explicit provider-input authority ledger: a 64-entry fixed FIFO of admitted audio-commit generations, the current item/open-completion state, speech-to-commit correlation state, and a 1024-entry exact historical item set.
- Every commit that is not the matching commit for an already observed `speech_started` edge appends one generation. The first unseen provider item consumes exactly the oldest generation; N consecutive admitted commits therefore retain N ordered claims. The 65th outstanding commit fails before command sequence allocation or ledger mutation, and drained ring capacity is reusable.
- Every accepted transcript-only, client-created, speech-established, or commit-claimed item enters history. Historical output cannot consume a later commit generation, including the oldest item after multiple transcript-only turns. History never evicts; a new ID at capacity fails closed.
- Pure providers without speech edges retain transcription-driven ABA revision compatibility while no explicit authority exists. Once client, speech, or committed-audio authority exists, mismatched historical output is a total no-op and an unseen item requires an outstanding committed generation.
- Client-created user items must use a new historical ID and are rejected while committed audio generations are pending. Only explicit `speech_started` may deliberately establish a new generation for a retained ID; it consumes the oldest pending commit when commit arrived first, or marks the next commit as its matching generation when speech arrived first.
- A claimed item's delta and matching completion remain admissible while its transcript is open. Once complete, a later outstanding commit fences additional output for that item, preventing a completed prior generation from reopening itself. Completion also closes any unmatched speech-first generation, so a later commit cannot be misclassified as belonging to already completed speech.
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
- While any cancelled tombstone remains, metadata-less created or transcript-first output is ambiguous and fails closed before creating an implicit correlation or consuming implicit event/byte budget. Once the exact tombstone is consumed, legitimate implicit-created and transcript-first compatibility paths remain available.

### App-owned approval resolution

- Session intake builds a trusted request from authenticated tenant/user, current conversation/session/incarnation, validated decision, idempotency key, reason, and one UTC resolution time.
- Intake performs a nonblocking admission through `ApprovalResolutionDispatcher`; no session context is passed. Successful app-queue admission completes the session intake boundary, so disconnect cannot fossilize reconnect as “not at boundary.”
- `ApprovalCoordinator` owns a 64-entry queue. Protocol field limits bound each work item; invalid scope/resolution fails before admission.
- Each dequeued work item receives a fresh five-second absolute store deadline when durable execution begins. Queue wait consumes none of that budget. Store resolution runs under the app `Run` context plus that deadline, never `sessionCtx`.
- The exact caller-supplied idempotency key and resolution time reach `ResolveApproval` unchanged. Store idempotency remains authoritative.
- Resolution is called exactly once for each admitted queue entry. Store rejection/timeout produces an optional typed `ApprovalResolutionCompletedInput` without terminating the coordinator.
- Typed completion is submitted only to the exact `(sessionID, incarnation)` via `Manager.Submit`. A disconnected or replaced incarnation returns `ErrSessionNotActive`; that stale UX completion is ignored and cannot cancel or undo the store operation.
- Optional typed completion uses a separate 64-entry lane and one independently timeout-bounded worker. Store processing never waits for that lane; saturation drops only optional UX/high-water completion.
- `App.Run` supervises the approval coordinator alongside the existing app-owned durable coordinators, and its result channel is sized for all six possible coordinator loops.

## Verification

Focused session race tests, including stale input, implicit speaking suppression, approval disconnect, and the nonblocking speech fences:

```text
GOCACHE=/private/tmp/gpt-live-task2-go-cache go test -race ./internal/session -run '^(TestNewSpeechItemRejectsLateOlderInputTranscriptsWithoutMutation|TestImplicitCreatedWhileSpeakingBindsCancelsAndNeverPublishes|TestManagerTransfersAcceptedApprovalBeyondSessionDisconnect|TestSpeechStartedAdvancesFenceWhileEarlierVisualWriteIsBlocked|TestSpeechStartedAdvancesFenceWhileEarlierProviderSendIsBlocked|TestSpeechStartedAdvancesFenceWhilePresentationSafetyIsBlocked)$' -count=20 -timeout=240s
```

PASS.

Review-fix round 1 exact repros:

```text
GOCACHE=/private/tmp/gpt-live-task2-go-cache go test -race ./internal/session -run '^TestClientCreatedUserTurnRejectsLateOlderProviderTranscriptsWithoutMutation$' -count=50 -timeout=240s
GOCACHE=/private/tmp/gpt-live-task2-go-cache go test -race ./internal/realtime/s2sws -run '^(TestCancelledPendingRejectsMetadataLessCreatedAndTranscriptFirstOutput|TestEmptyIDCancelTombstonesExactlyOnePendingCreateAndAllowsNextImplicitResponse)$' -count=50 -timeout=240s
GOCACHE=/private/tmp/gpt-live-task2-go-cache go test -race ./internal/gateway -run '^(TestApprovalCoordinatorDoesNotBlockStoreProgressOnOptionalLiveCompletion|TestApprovalCoordinatorStoreDeadlineBeginsWhenQueuedWorkExecutes|TestApprovalCoordinatorCompletesAcceptedResolutionOnceAfterLiveSessionDisconnect|TestApprovalCoordinatorQueueIsBounded)$' -count=50 -timeout=240s
```

All passed.

Review-fix round 2 mixed-mode and ABA exact repros:

```text
go test -race ./internal/session -run '^(TestAudioCommitAfterClientTurnAcceptsNewProviderTranscriptAndKeepsOlderItemsFenced|TestAudioCommitFencesPriorTranscriptOnlyItemBeforeOpeningNewClaim|TestCommittedAudioTurnCannotBeClaimedByAnOlderABAItem)$' -count=100 -timeout=240s
```

PASS.

Review-fix round 3 architecture, adversarial model, and capacity repros:

```text
go test -race ./internal/session -run '^(TestClientUserItemCannotReuseHistoricalProviderItemWithoutSpeechEdge|TestConsecutiveAudioCommitsRetainOneOrderedClaimPerTurn|TestCompletedSpeechGenerationDoesNotConsumeLaterAudioCommit|TestAllTranscriptOnlyHistoryItemsFenceLaterCommitClaim|TestAudioCommitGenerationLedgerFailsClosedAtCapacity|TestProviderInputAuthorityLedgerMatchesAdversarialGenerationModel|TestProviderInputGenerationLedgerReusesCapacityAfterRingWrap)$' -count=100 -timeout=240s
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
GOCACHE=/private/tmp/gpt-live-task2-go-cache go test -race ./internal/session ./internal/gateway ./internal/realtime/s2sws -count=3 -timeout=300s
GOCACHE=/private/tmp/gpt-live-task2-go-cache go vet ./internal/session ./internal/gateway ./internal/realtime/s2sws
GOCACHE=/private/tmp/gpt-live-task2-go-cache go test ./... -count=1 -timeout=300s
git diff --check
```

All passed. Listener-backed gateway/adapter tests used loopback permission; no listener test was weakened.

## Self-review

- Checked stale item rejection occurs before all synth-ID, revision, sequence, active-turn, and durable-effect mutation.
- Checked no-speech-edge compatibility remains covered by final transcript and ABA revision tests, including pure transcript-only A/B/A before explicit authority and transcript-only A crossing client B and audio commit into transcript-only C.
- Checked the fixed generation FIFO preserves commit cardinality and ordering across three consecutive claims, rejects overflow without sequence or state mutation, and reuses all capacity after head/tail ring wrap.
- Checked every accepted transcript item enters bounded exact history; oldest late A cannot steal a later claim after transcript-only A/B and client C. Client historical reuse is rejected, while intentional reuse remains available only through `speech_started`.
- Checked current-item delta/completion remains accepted until complete, while a later committed generation fences repeated output from the completed item.
- Checked a newer client-created user turn advances the same authoritative item fence without changing providers that have emitted no speech edge and no newer client user item.
- Checked implicit suppression orders bind before flush/cancel and uses a newer cancel fence; old bind-epoch media is therefore rejected even if binding releases buffered adapter frames.
- Checked the cancelled-pending structure is explicitly create-epoch correlated, bounded together with pending creates, and never deletes an arbitrary map/oldest epoch.
- Checked metadata-less ambiguity returns before implicit correlation/buffer mutation for one and multiple tombstones; exact tombstone consumption restores legitimate implicit compatibility.
- Checked ordinary named cancellation correlation and empty-ID provider JSON remain unchanged.
- Checked app approval queue admission is the disconnect-safe ownership point, while store deadline and live completion are independently bounded.
- Checked optional completion has its own bounded capacity/timeout and no wait on the durable loop; every store deadline is assigned only after dequeue.
- Checked trusted actor/scope cannot be supplied by the client, idempotency/time are not regenerated in the coordinator, and stale completion targets no replacement incarnation.
- Checked no Task 1 FIFO/terminal ownership contract or review item 8 behavior was changed.
