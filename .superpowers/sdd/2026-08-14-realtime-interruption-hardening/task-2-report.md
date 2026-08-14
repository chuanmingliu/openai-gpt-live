# Task 2 report: turn fencing, implicit-response suppression, and durable approval ownership

## Scope

Base: `b15252e`

Approved single-flight correction base: `8090b21`

Owned production files:

- `internal/session/reactor.go`
- `internal/realtime/voice.go`
- `internal/realtime/s2sws/mapping.go`
- `internal/session/manager.go`
- `internal/gateway/app.go`
- `internal/gateway/approvals.go`
- `internal/realtime/s2sws/connector.go`
- `internal/protocol/server.go`
- `internal/protocol/codec.go`
- `internal/protocol/capabilities.go`
- `internal/session/state.go`

Owned tests:

- `internal/session/reactor_test.go`
- `internal/session/manager_test.go`
- `internal/gateway/app_test.go`
- `internal/gateway/approvals_test.go`
- `internal/realtime/s2sws/connector_test.go`
- `internal/realtime/s2sws/commit_ack_test.go`
- `internal/protocol/codec_test.go`
- `internal/protocol/capabilities_test.go`

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

Review-fix round 4 REDs and boundary characterization:

1. `TestInterleavedClaimedAudioGenerationsRetainTheirOwnCompletion` showed commits B/C followed by delta B, delta C, and completion B collapsed into the single current-item slot. Completion B was dropped instead of being emitted and persisted against B's original turn.
2. `TestClientUserItemSupersedesOutstandingAudioGenerations` showed a valid newer client turn returned `ErrInvalidTransition` while either unclaimed or claimed-open audio work remained, instead of becoming authoritative and fencing the older generation.
3. Hypothesis 1 cleared unknown older generations. `TestClientSupersededCommitCannotStealALaterAudioGeneration` then showed commit A, client B, commit C, and late A let A steal C's live claim and replace B.
4. Hypothesis 2 retained arrival-ordered suppression tombstones and closed A-before-C, but independent review supplied the inverse: commit A, client B, commit C, delta/completion C, then late A. The tombstone swallowed valid C and late A stole C's live slot. `TestProviderCommitAcknowledgementsDisambiguateClientSupersededGenerations` and `TestUnacknowledgedSupersededGenerationFailsClosedWithoutGuessing` made C-before-A, A-withheld, and no-ack ambiguity executable before hypothesis 3.
5. Hypothesis 3 maps the provider's exact `input_audio_buffer.committed` acknowledgement. The initial tests first failed to compile without the provider-neutral event, then failed with `unsupported session input`; after the mapping seam existed they still required exact generation binding rather than ASR arrival guesses.
6. `TestClaimedOpenAudioGenerationRetainsCapacityUntilCompletion` showed that claiming a generation with a delta released its slot early, allowing a 65th live generation before the matching completion.
7. `TestProviderInputItemIDsValidateBeforeRetention` showed client, speech, and transcription-delta paths retained invalid and oversized IDs, including a 1 MiB ID. It now exercises client, speech, commit-acknowledgement, delta, and completion paths at the exact 256-byte success boundary plus 257-byte, 1 MiB, whitespace-only, invalid-UTF-8, and oversized acknowledgement-predecessor rejection before mutation.
8. `TestProviderInputHistoryExactCapacityNeverEvictsRejectsClientReuseAndAllowsSpeechReuse` characterized the already-correct count boundary and made it load-bearing: exactly 1024 retained 256-byte IDs are accepted, the 1025th fails before turn/sequence/history mutation, neither endpoint is evicted, client reuse is rejected, and explicit speech reuse remains valid. The added exact byte-account assertion was initially absent from the implementation.
9. The final provider-seam audit invalidated hypothesis 3's binding premise. Neither the official success event nor the configured simulator provides an exact commit-command correlation, and the official protocol does not declare FIFO acknowledgement pairing. The hypothesis implementation and tests were therefore returned to `HEAD` instead of being committed.

Approved single-flight correction REDs:

1. The protocol, adapter, and reactor tests failed to compile because `InputAudioBufferCommittedEvent`, `EventInputAudioCommitted`, nullable `PreviousItemID`, and `ErrAudioCommitInFlight` did not exist.
2. The local speech service's focused tests produced seven expected failures: successful commits returned `None`, the router emitted no success event, and the OpenAI SDK loopback timed out waiting for `input_audio_buffer.committed`.
3. `TestManagerRejectsSecondCommitUntilAckAndOrdersAckBeforeTranscript` showed the remaining session-layer bug after the reactor guard landed: a second buffered commit returned `reactor: input audio commit acknowledgement is pending` from `Manager.Run`, terminating the session instead of using the existing nonfatal protocol-error lane.
4. The first transport deadline implementation let any unseen automatic/VAD acknowledgement stop the sole timer. Duplicate/ABA and explicit-speech tests then showed that an item-only tombstone could either release the wrong later slot or reject a legitimate explicitly re-established item.
5. `TestPreRegistrationAckIsIneligibleForLaterCommit`, `TestReadBufferedBeforeRegistrationRemainsIneligible`, and `TestBufferedCandidateFromPriorSlotCannotAuthorizeNextSlot` showed that a boolean candidate marker was not causal: an already-read frame or delayed prior-slot frame could clear a newer reactor generation.
6. `TestCorrelateWaitsForDefinitiveResultAfterEnqueue` showed cancellation could win after mapper mutation and leave a ghost timeout for a commit that was never written. `TestCommitSequenceMustBePositiveAndNeverReused` showed zero/reused transport tokens collapsed the cross-slot ABA fence.
7. Provider mapping accepted a committed event with an omitted `previous_item_id`, while the public codec correctly required the standard field. The RED distinguishes omission from the valid first-conversation value `null`.
8. `TestClientCommitSemanticErrorDispatchOverflowRemainsFatal` showed the nonfatal duplicate-commit flag also suppressed a later client-control overflow, silently losing the rejection instead of tearing down.

## Implemented contracts

### Provider input-item fence — historical blocker and approved resolution

Round 4 cannot establish the required exact generation mapping inside this repository:

- The official [`input_audio_buffer.commit`](https://developers.openai.com/api/reference/resources/realtime/client-events#input_audio_buffer-commit) event accepts an optional client `event_id`, but the official [`input_audio_buffer.committed`](https://developers.openai.com/api/reference/resources/realtime/server-events#input_audio_buffer-committed) success event contains only its own server `event_id`, `item_id`, `type`, and `previous_item_id`. It does not echo the commit's client event ID.
- The server event defines `previous_item_id` as the preceding item in the whole Conversation. Assistant messages, system messages, function calls, and function-call outputs may sit between audio commits, so it is not the previous user-input ID and cannot identify a commit generation by itself.
- The Realtime guide says WebSocket events share one channel and ordering in the push-to-talk discussion. That establishes transport/control order; it does not declare that independently produced success acknowledgements for repeated commits are FIFO-correlated to those commands. WebSocket delivery preserves the order an endpoint sends, not cross-direction causal pairing.
- `input_audio_buffer.committed` may also be generated by server VAD rather than a client command. Binding the oldest unacknowledged client generation on every such event would therefore be incorrect without a provider-mode invariant.
- The configured `/Users/chuanmingliu/speech-to-speech` service does not emit `input_audio_buffer.committed` at all. `handlers/audio.py::handle_audio_commit` only clears `audio_buffer_has_data` and returns error-or-`None`; `websocket_router.py::_dispatch_client_event` sends an event only for the error case. Its existing tests assert only that state transition. A Go implementation that requires the acknowledgement would break the shipped local demo/provider contract.

The rejected predecessor implementation is unsafe for the required adversarial case: commit A, client B, an assistant/tool item, commit C, C transcript/completion, then late A. C's `previous_item_id` is the assistant/tool item, not B, so predecessor matching cannot select C exactly. Binding acknowledgements oldest-first would make this case safe only if FIFO success correlation were guaranteed, and no such official or configured-provider contract was found. Per the three-hypothesis stop rule, no fourth heuristic was added and the unfinished product/test changes were reverted.

The audit identified two safe architecture options:

1. **Recommended: one outstanding manual commit plus a mandatory committed acknowledgement.** Do not admit/send the next manual commit until the prior `input_audio_buffer.committed` arrives. Its `item_id` is then unambiguous without assuming acknowledgement FIFO or inspecting `previous_item_id`. This is compatible with the official event shape, but it changes the current multiple-outstanding-commit requirement and requires controller approval. The local speech service must import/add `InputAudioBufferCommittedEvent` to its server-event union, retain the whole-conversation predecessor when it allocates the input item, return that exact item/predecessor on successful commit, and synchronously send it from the already-serial WebSocket dispatch loop. Provider tests must prove one success acknowledgement per nonempty commit, the acknowledgement item ID equals later transcription IDs, assistant/tool predecessors are preserved but not used as correlation, empty commit still returns only the error, and the next commit is not admitted before acknowledgement.
2. **Provider-specific echoed commit correlation.** Add a client commit correlation ID and require the custom speech service to echo it on the success event, preserving multiple outstanding commits and exact B/C interleaving. Because the official committed event has no such field, this needs explicitly split adapter/provider modes plus separate contract tests; presenting it as generic OpenAI compatibility would be incorrect.

The controller approved option 1. It is now implemented end to end:

- The Go reactor admits exactly one unacknowledged manual `input_audio_buffer.commit`. A second commit returns the stable `ErrAudioCommitInFlight` before sequence or ledger mutation. WebSocket intake maps that condition to the fixed, content-free `input_audio_buffer_commit_in_flight` protocol error and keeps the session alive; direct `Manager.Submit` receives the typed error without terminating the runtime.
- The adapter registers the sole commit before writing it, requires a standard `input_audio_buffer.committed` event, and terminates deterministically with `ErrAudioCommitAckTimeout` after five seconds by default. Accepted events drain before the timeout is published. A raw acknowledgement never stops the timer by itself: only the reactor's ordered semantic-release command can do so.
- Every manual commit carries a positive, strictly increasing generation token. The adapter stamps an eligible post-registration acknowledgement with that token, the reactor requires the exact pending token, and the semantic release must echo the same token and an item observed in that transport slot. A read-loop watermark excludes frames already read or buffered at registration. Historical committed-item tombstones and the token jointly prevent duplicate, ABA, automatic-VAD, and delayed prior-slot acknowledgements from clearing later commit C; an exact known item re-established by `speech_started` remains valid.
- The acknowledgement's exact `item_id` binds the sole pending generation. `previous_item_id` is preserved as nullable protocol data and forwarded to the client, but it is never used for correlation. No client event-ID echo, predecessor graph, deferred acknowledgement chain, or ASR arrival-order heuristic exists.
- Provider ingress and the public codec require the `previous_item_id` member while accepting explicit JSON `null` for the first conversation item. Omission and non-string/non-null values fail closed.
- A newer client user item suppresses the unknown older generation without discarding its acknowledgement slot. The eventual old acknowledgement records and retires only that tombstone; it cannot replace the client turn. A later commit/ack receives a new immutable turn, and late old acknowledgement/transcript events remain no-ops.
- Explicit `speech_started` authority and transcript-only compatibility remain available on their unambiguous paths. A transcript cannot claim an unacknowledged manual commit. Acknowledged live generations retain immutable turns through interleaved deltas until their exact completion, with an aggregate limit of 64.
- Retained item IDs are cloned, valid UTF-8, free of whitespace/control characters, and at most 256 bytes. History is exactly bounded to 1024 entries and 256 KiB with no eviction. Capacity/validation failures leave the full deep-copied ledger, turn/revision, transcript sequence, and effect sequence unchanged.
- The manager processes committed acknowledgements synchronously before accepting later provider transcript/media ordering, exposes pending-ack state in its reconnect boundary, preserves buffered client audio across a rejected early commit, and does not deadlock across repeated commit/ack/barge cycles.
- The native `/Users/chuanmingliu/speech-to-speech` service now allocates or reuses the input-audio item at successful commit, captures the whole-conversation predecessor before advancing the tail, and emits the SDK's standard `InputAudioBufferCommittedEvent` from the serialized router result path. Empty commits remain error-only. Assistant/tool predecessors, two sequential commits, event ordering, and SDK decoding are covered without any cross-repository runtime link.

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

Review-fix round 4 hypothesis tests (superseded, retained as TDD evidence):

```text
GOCACHE=/private/tmp/gpt-live-task2-round4-cache go test -race ./internal/session -run '^(TestProviderCommitAcknowledgementsDisambiguateClientSupersededGenerations|TestUnacknowledgedSupersededGenerationFailsClosedWithoutGuessing|TestClientUserItemSupersedesOutstandingAudioGenerations|TestClientSupersededCommitCannotStealALaterAudioGeneration|TestInterleavedClaimedAudioGenerationsRetainTheirOwnCompletion|TestClaimedOpenAudioGenerationRetainsCapacityUntilCompletion|TestConsecutiveAudioCommitsRetainOneOrderedClaimPerTurn|TestProviderInputAuthorityLedgerMatchesAdversarialGenerationModel|TestProviderInputGenerationLedgerReusesCapacityAfterRingWrap)$' -count=100 -timeout=300s
GOCACHE=/private/tmp/gpt-live-task2-round4-cache go test -race ./internal/session -run '^(TestProviderInputItemIDsValidateBeforeRetention|TestProviderInputHistoryExactCapacityNeverEvictsRejectsClientReuseAndAllowsSpeechReuse|TestAudioCommitGenerationLedgerFailsClosedAtCapacity)$' -count=10 -timeout=300s
GOCACHE=/private/tmp/gpt-live-task2-round4-cache go test -race ./internal/realtime/s2sws -run '^TestReceiveMapsEveryProviderEvent$' -count=20 -timeout=300s
```

These passed against the discarded predecessor-matching hypothesis. They prove that implementation was internally consistent with its test model, not that the test model matched the provider contract. The provider-seam audit later showed that `previous_item_id` is the whole-conversation predecessor, successful acknowledgements do not echo the client commit ID, acknowledgement FIFO is undeclared, and the configured provider emits no success acknowledgement. The hypothesis code and tests named above are therefore no longer present after the apply-patch return to `HEAD`; these commands are not completion evidence.

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

Historical round-4 repository gates before the provider-seam blocker was established:

```text
GOCACHE=/private/tmp/gpt-live-task2-round4-cache go test ./internal/session -count=1 -timeout=300s
GOCACHE=/private/tmp/gpt-live-task2-round4-cache go test -race ./internal/session ./internal/gateway ./internal/realtime/s2sws -count=3 -timeout=300s
GOCACHE=/private/tmp/gpt-live-task2-round4-cache go test ./... -count=1 -timeout=300s
GOCACHE=/private/tmp/gpt-live-task2-round4-cache go test -race ./... -count=1 -timeout=300s
GOCACHE=/private/tmp/gpt-live-task2-round4-cache go vet ./...
GOCACHE=/private/tmp/gpt-live-task2-round4-cache go build ./...
GOCACHE=/private/tmp/gpt-live-task2-round4-cache go mod tidy -diff
make verify-deploy-static
git diff --check
```

All passed at that hypothesis revision. The first full-repository race attempt had one unrelated notification-order failure in `internal/handoff`; Task 2's `internal/session` package passed in that run. The isolated handoff test then passed `-race -count=20`, and the complete fresh `go test -race ./...` rerun passed every package. The static deployment gate emitted a sandbox-only module stat-cache warning but completed successfully; the separately permissioned repository build was clean. These gates cannot validate a missing external correlation contract and are superseded as Task 2 completion evidence by the blocker above.

Interim blocked-state cleanup verification:

```text
git diff --exit-code HEAD -- internal/session/reactor.go internal/session/reactor_test.go internal/realtime/voice.go internal/realtime/s2sws/mapping.go internal/realtime/s2sws/connector_test.go
git diff --check -- .superpowers/sdd/2026-08-14-realtime-interruption-hardening/task-2-report.md
```

Both passed at the provider-seam checkpoint. Controller approval then authorized the single-flight correction described above.

Approved single-flight correction gates:

```text
go test ./internal/protocol ./internal/session -count=1
go test ./internal/realtime/s2sws -count=1
go test ./internal/realtime/s2sws ./internal/session -run 'TestAudioCommit|TestPreRegistration|TestBufferedCandidate|TestReadBuffered|TestCorrelate|TestExplicitSpeechReuse|TestUnknownTranscriptAndDuplicateAck|TestHistoricalAck|TestDifferentAutoCommit|TestSemanticAck|TestKnownManualItem|TestManagerRejectsSecondCommit|TestManagerSubmitReturnsCommit|TestClientCommitSemantic' -count=100
go test -race ./internal/realtime/s2sws ./internal/session -run 'TestAudioCommit|TestPreRegistration|TestBufferedCandidate|TestReadBuffered|TestCorrelate|TestExplicitSpeechReuse|TestUnknownTranscriptAndDuplicateAck|TestHistoricalAck|TestDifferentAutoCommit|TestSemanticAck|TestKnownManualItem|TestManagerRejectsSecondCommit|TestManagerSubmitReturnsCommit|TestClientCommitSemantic' -count=20
go test -race ./internal/protocol ./internal/realtime/s2sws ./internal/session -count=1
go test ./...
go vet ./...
git diff --check
```

All passed. The repeated tests cover acknowledgement-before-next-commit, deterministic no-ack teardown, pre-registration/buffered/delayed event causality, duplicate and explicit ABA behavior, automatic VAD separation, client supersession, semantic-error backpressure, and continuous commit/ack/barge progress. Final independent review reported P0/P1/P2 = 0/0/0.

Native speech-service gates at commit `919560114785c7220e209710b06d1f2620e9add8`:

```text
PYTHONPATH=src .venv/bin/pytest -q tests/openai_realtime
.venv/bin/ruff check src/speech_to_speech/api/openai_realtime/handlers/audio.py src/speech_to_speech/api/openai_realtime/service.py src/speech_to_speech/api/openai_realtime/websocket_router.py tests/openai_realtime/test_openai_client.py tests/openai_realtime/test_realtime_service.py tests/openai_realtime/test_webrtc.py tests/openai_realtime/test_websocket_router.py
```

The test package passed 228 tests with one pre-existing Starlette deprecation warning; Ruff passed. Tests used local service/router/WebRTC/SDK loopback paths only, with no live provider calls or credential access.

## Self-review

- Checked stale item rejection occurs before all synth-ID, revision, sequence, active-turn, and durable-effect mutation.
- Checked no-speech-edge compatibility remains covered by final transcript and ABA revision tests, including pure transcript-only A/B/A before explicit authority and transcript-only A crossing client B and audio commit into transcript-only C.
- Confirmed the round-3 ledger's counterexample—commit A, client B, commit C, C transcript/completion, then late A—is closed by admission uniqueness and exact per-slot generation tokens, not by predecessor or arrival inference.
- Confirmed the official success event does not echo the client commit ID and its predecessor is the whole-conversation tail; assistant/tool insertion defeats input-only predecessor matching.
- Confirmed WebSocket command/event ordering is insufficient to infer FIFO success correlation across directions, and the configured local provider emits no committed success event.
- Preserved the acceptance criteria: 64 total live generation records including tombstones; 1024 cloned IDs and at most 256 KiB with 256-byte valid-UTF-8/content-free identifiers; exact 1024/1025 no-eviction/reuse coverage; and deep-copied ledger/transcript-sequence fields in failure-atomicity views.
- Checked raw acknowledgements cannot stop/reset the adapter deadline; only an exact reactor-authorized release does. The token, read watermark, observed-item set, and no-eviction history jointly close prior-slot, duplicate, ABA, auto-VAD, and cancellation races.
- Checked the speech-service change is native to its repository and the Go implementation has no symlink, import, runtime dependency, or copied linkage to Qwen or another reference repository.
- No protected demo dirt, review item 8 behavior, supported empty-ID cancellation path, or expert item 8 contract was changed.
- Checked implicit suppression orders bind before flush/cancel and uses a newer cancel fence; old bind-epoch media is therefore rejected even if binding releases buffered adapter frames.
- Checked the cancelled-pending structure is explicitly create-epoch correlated, bounded together with pending creates, and never deletes an arbitrary map/oldest epoch.
- Checked metadata-less ambiguity returns before implicit correlation/buffer mutation for one and multiple tombstones; exact tombstone consumption restores legitimate implicit compatibility.
- Checked ordinary named cancellation correlation and empty-ID provider JSON remain unchanged.
- Checked app approval queue admission is the disconnect-safe ownership point, while store deadline and live completion are independently bounded.
- Checked optional completion has its own bounded capacity/timeout and no wait on the durable loop; every store deadline is assigned only after dequeue.
- Checked trusted actor/scope cannot be supplied by the client, idempotency/time are not regenerated in the coordinator, and stale completion targets no replacement incarnation.
- Checked no Task 1 FIFO/terminal ownership contract or review item 8 behavior was changed.
