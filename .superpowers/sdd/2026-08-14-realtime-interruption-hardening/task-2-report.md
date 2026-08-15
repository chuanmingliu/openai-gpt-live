# Task 2 report: turn fencing, implicit-response suppression, and durable approval ownership

## Scope

Base: `b15252e`

Approved single-flight correction base: `8090b21`

Final mode-boundary Go base: `0456d4aa58ba98de8ad696e657a5c6a5c9067dbe`

Native speech base: `919560114785c7220e209710b06d1f2620e9add8`

Owned production files:

- `internal/session/reactor.go`
- `internal/session/effect.go`
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

Final mode-boundary additions/updates are confined to:

- `internal/realtime/input_turn_mode.go`
- `internal/realtime/voice.go`
- `internal/realtime/s2sws/connector.go`
- `internal/realtime/s2sws/mapping.go`
- `internal/session/state.go`
- `internal/session/effect.go`
- `internal/session/reactor.go`
- `internal/session/manager.go`
- `internal/realtime/s2sws/{commit_ack,input_turn_mode,connector,connector_internal,connector_unit}_test.go`
- `internal/session/{input_turn_mode,reactor,manager,presentation}_test.go`
- `internal/gateway/app_test.go`

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

Interim single-flight correction REDs:

1. The protocol, adapter, and reactor tests failed to compile because `InputAudioBufferCommittedEvent`, `EventInputAudioCommitted`, nullable `PreviousItemID`, and `ErrAudioCommitInFlight` did not exist.
2. The local speech service's focused tests produced seven expected failures: successful commits returned `None`, the router emitted no success event, and the OpenAI SDK loopback timed out waiting for `input_audio_buffer.committed`.
3. `TestManagerRejectsSecondCommitUntilAckAndOrdersAckBeforeTranscript` showed the remaining session-layer bug after the reactor guard landed: a second buffered commit returned `reactor: input audio commit acknowledgement is pending` from `Manager.Run`, terminating the session instead of using the existing nonfatal protocol-error lane.
4. The first transport deadline implementation let any unseen automatic/VAD acknowledgement stop the sole timer. Duplicate/ABA and explicit-speech tests then showed that an item-only tombstone could either release the wrong later slot or reject a legitimate explicitly re-established item.
5. `TestPreRegistrationAckIsIneligibleForLaterCommit`, `TestReadBufferedBeforeRegistrationRemainsIneligible`, and `TestBufferedCandidateFromPriorSlotCannotAuthorizeNextSlot` showed that a boolean candidate marker was not causal: an already-read frame or delayed prior-slot frame could clear a newer reactor generation.
6. `TestCorrelateWaitsForDefinitiveResultAfterEnqueue` showed cancellation could win after mapper mutation and leave a ghost timeout for a commit that was never written. `TestCommitSequenceMustBePositiveAndNeverReused` showed zero/reused transport tokens collapsed the cross-slot ABA fence.
7. Provider mapping accepted a committed event with an omitted `previous_item_id`, while the public codec correctly required the standard field. The RED distinguishes omission from the valid first-conversation value `null`.
8. `TestClientCommitSemanticErrorDispatchOverflowRemainsFatal` showed the nonfatal duplicate-commit flag also suppressed a later client-control overflow, silently losing the rejection instead of tearing down.

Final mode-boundary correction REDs:

1. `TestInputTurnModeRequiresActualProviderSessionUpdated`,
   `TestAudioWaitsForMatchingProviderSessionUpdated`, and the mismatch/missing/unknown confirmation
   table showed that socket write completion synthesized acceptance, pre-confirmation audio reached
   the wire, and incomplete or contradictory provider configuration could enable the wrong owner.
2. `TestProviderVADRejectsManualCommitAfterProviderConfirmation` and
   `TestProviderVADNeverWritesManualCommit` showed both `server_vad` and `semantic_vad` still admitted
   explicit client commits. `TestManualModeRejectsProviderOwnedSpeechAndUnsolicitedCommit` showed the
   inverse: provider speech/automatic commit could enter a manual stream. Mode switching was also
   accepted after confirmation.
3. Pure-manual delayed duplicate/ABA, multiple-fresh-acknowledgement, and client-supersession tests
   showed the interim read watermark could let an old or provider-owned acknowledgement release a
   newer slot. A separate 1025-ack provider-VAD test showed automatic traffic incorrectly consumed
   the bounded manual acknowledgement history.
4. The native speech RED suite produced nine expected failures: explicit commit was accepted in
   provider-VAD configurations; a transcript could overtake the matching committed event; and
   client, assistant, tool-call, and function-output interposition all allowed a stale speculative
   input item to be reused while rolling the conversation tail backward. Reopen after a tail advance
   exposed the same rollback. A transport-gated router test then made the two concurrent outbound
   producers deterministic without timing sleeps.
5. A final cross-layer RED showed that serialization at the connector was not sufficient by itself:
   the reactor could register browser update 73 while the connector still held it behind the
   manager's sequence-zero initial update, then consume the initial provider acknowledgement as if
   it acknowledged 73. `TestSequenceZeroInitialAckCannotConsumePendingClientMode` and
   `TestSessionUpdatedCarriesOutstandingLocalSequence` failed to compile until the event could carry
   the sole locally outstanding session-update sequence. This sequence identifies an already
   serialized causal slot; it does not infer a provider correlation from arrival.
6. Sequence zero was then shown to be insufficient on its own because it represented both the real
   manager bootstrap command and an unsolicited event observed before any update was outstanding.
   `TestUnsolicitedSequenceZeroCannotConfirmExpectedInitialMode` and
   `TestUnsolicitedSessionUpdatedIsNotMarkedAsAcknowledgement` forced an explicit acknowledgement-
   present bit. Only `present + exact sequence` can now confirm configuration; a false bit or wrong
   sequence leaves all pending state unchanged.
7. Final independent review found that `speech_started(A) -> speech_started(B) -> delayed
   speech_stopped(A)` cleared `UserSpeaking` for B. That reopened the implicit-response path across
   the live barge fence. The stop path also omitted the shared 256-byte/UTF-8/content-free item-ID
   validation. The corrected tests failed first on the stale stop event, the cleared snapshot, the
   missing stop for B, and every invalid-ID case. The reactor now tracks the active speech item
   independently of the active conversation turn, ignores a validated stale stop without mutation,
   and clears the fence only for the matching item.
8. The next independent pass showed that accepting the superseding `speech_started(B)` internally
   while suppressing its client event exposed `speech_started(A) -> speech_stopped(B)`. The corrected
   tests first failed because B produced no public start. A distinct B now emits its own start without
   another epoch advance, flush, or cancel; a same-ID duplicate and delayed stop A remain no-ops, and
   the matching stop B carries B's identity.
9. `TestClientWriterPublishesInputTurnModeBeforeSessionUpdated` showed the final manager ordering
   gap: the reactor queued the acknowledged `session.updated` client effect before publishing the
   confirmed mode snapshot. The client writer could therefore expose the acknowledgement while the
   reader still saw `Unspecified` and rejected an immediate append/commit. The RED initially failed
   to compile without an explicit effect boundary. The acknowledged effect now carries the confirmed
   mode through the output queue, and the writer publishes it under the boundary lock immediately
   before the external write.

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

The controller initially approved option 1. Final adversarial review then found that its
read-watermark implementation was still unsound: a provider-owned VAD acknowledgement and a
client-owned manual acknowledgement can share the same uncorrelated event shape. Arrival,
watermark, item-predecessor, and FIFO rules cannot prove which authority produced such an event.
The following final mode-boundary correction supersedes that interim implementation:

- Go now has an explicit provider-neutral input-turn mode: `manual` for
  `turn_detection:null`, and provider-VAD for `server_vad` or `semantic_vad`. The mode is
  requested through session configuration, confirmed only by the provider's actual
  `session.updated`, and immutable for the lifetime of the audio stream. Socket write completion
  is not confirmation. Session updates are serialized because provider acknowledgements carry no
  request correlation; the connector stamps the acknowledgement with the sole locally outstanding
  command sequence plus an explicit acknowledgement-present bit. The bit distinguishes a real
  sequence-zero manager bootstrap acknowledgement from unsolicited/pre-registration provider
  events; the exact sequence prevents that bootstrap acknowledgement from consuming a later
  browser update. Audio is rejected content-free and nonfatally until exact confirmation. The
  acknowledged client effect also carries a pre-write boundary, so the confirmed mode is published
  before `session.updated` can become observable and immediate causally later audio cannot be falsely
  rejected.
- In provider-VAD mode the client and gateway never send `input_audio_buffer.commit`; an attempted
  explicit commit receives a nonfatal `manual_audio_commit_disabled` error. Provider
  `speech_started`, `speech_stopped`, and `input_audio_buffer.committed` events own the item
  directly and never enter or consume the manual acknowledgement slot/history. A stop must match
  the independently tracked active speech item; a delayed stop for a superseded item cannot clear
  `UserSpeaking` or reopen the implicit-response path. A distinct overlapping start is forwarded
  with its new item identity but does not repeat the epoch advance, flush, or cancel.
- In manual mode provider speech edges are invalid. Exactly one outstanding client commit means
  the sole fresh committed acknowledgement is causal. The private positive command sequence is
  used only to carry that already-established local slot through adapter/reactor handoff; it does
  not infer provider correlation. Delayed duplicate/ABA item IDs are permanent bounded history
  no-ops and cannot release a later slot. Multiple fresh acknowledgements fail closed.
- No read watermark, `previous_item_id`, arrival order, ASR order, or cross-direction FIFO is used
  for correlation. `previous_item_id` remains required protocol data; explicit JSON `null` is
  valid for the first conversation item, while omission is rejected.
- Provider transcripts have no independent authority: a same-item transcription is accepted only
  after its item has a committed owner. Client supersession, assistant/tool/function-output
  interposition, and delayed duplicate ABA events cannot revive or steal a turn.
- The native `/Users/chuanmingliu/speech-to-speech` service deliberately exposes only the
  provider-VAD capability. It rejects `turn_detection:null` and every explicit client commit,
  emits the standard `input_audio_buffer.committed` event and mutates commit state at provider
  speech-stop, and uses a per-item readiness fence so the committed event's transport send finishes
  before any same-item transcript is emitted. Conversation-tail advancement is monotonic: a
  speculative input item is reused only while it is still current and its predecessor snapshot
  matches; client, assistant, tool-call, or function-output advancement forces a fresh item without
  rolling the tail backward.
- Retained item IDs remain cloned, valid UTF-8, free of whitespace/control characters, and at most
  256 bytes. Manual duplicate history remains exactly bounded to 1024 entries and 256 KiB with no
  eviction; provider-VAD traffic does not consume that capacity. Live input generations remain
  bounded to 64, and admission/capacity failures remain mutation-free.

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

Interim single-flight correction gates (superseded by the final mode boundary):

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

These passed at the interim revision. They remain useful historical regression evidence for admission,
timeouts, error backpressure, and commit/barge progress, but they are not final completion evidence:
the subsequent adversarial review proved that its automatic-VAD separation still depended on an
uncorrelated read watermark. The final mode-boundary gates below supersede this result.

Final Go mode-boundary/adversarial gates:

```text
GOCACHE=/private/tmp/task2-breaker5-go-cache go test ./internal/session ./internal/realtime/s2sws -run 'Test(HistoricalAckCannotReleaseNewCommit|HistoricalManualItemIDCannotBeReused|PriorManualSequenceCannotStealNextCommit|UnknownTranscriptAndDuplicateAckCannotReleaseLaterCommit|ClientTurnSupersedesPendingManualCommitWithoutLateAckStealingAuthority|ClientSupersessionRetainsOnlyUnknownAckTombstone|ManualSlotRejectsMultipleFreshAcknowledgements|ProviderVADAcknowledgementsDoNotConsumeManualHistory|SequenceZeroInitialAckCannotConsumePendingClientMode|UnsolicitedSequenceZeroCannotConfirmExpectedInitialMode|WrongNonzeroSessionUpdateSequenceCannotConsumePendingMode|ManagerInitialSessionAckCannotConsumePendingBrowserMode|SessionUpdatedCarriesOutstandingLocalSequence|UnsolicitedSessionUpdatedIsNotMarkedAsAcknowledgement|ClientWriterPublishesInputTurnModeBeforeSessionUpdated|ImplicitCreatedWhileSpeakingBindsCancelsAndNeverPublishes|SpeechEdgesAndExplicitCancelFenceInOrder|DifferentSpeechItemWhileSpeakingSupersedesCommittedEligibility)$' -count=100 -timeout=180s
GOCACHE=/private/tmp/task2-breaker5-go-cache go test -race ./internal/session ./internal/realtime/s2sws -run 'Test(HistoricalAckCannotReleaseNewCommit|HistoricalManualItemIDCannotBeReused|PriorManualSequenceCannotStealNextCommit|UnknownTranscriptAndDuplicateAckCannotReleaseLaterCommit|ClientTurnSupersedesPendingManualCommitWithoutLateAckStealingAuthority|ClientSupersessionRetainsOnlyUnknownAckTombstone|ManualSlotRejectsMultipleFreshAcknowledgements|ProviderVADAcknowledgementsDoNotConsumeManualHistory|SequenceZeroInitialAckCannotConsumePendingClientMode|UnsolicitedSequenceZeroCannotConfirmExpectedInitialMode|WrongNonzeroSessionUpdateSequenceCannotConsumePendingMode|ManagerInitialSessionAckCannotConsumePendingBrowserMode|SessionUpdatedCarriesOutstandingLocalSequence|UnsolicitedSessionUpdatedIsNotMarkedAsAcknowledgement|ClientWriterPublishesInputTurnModeBeforeSessionUpdated|ImplicitCreatedWhileSpeakingBindsCancelsAndNeverPublishes|SpeechEdgesAndExplicitCancelFenceInOrder|DifferentSpeechItemWhileSpeakingSupersedesCommittedEligibility)$' -count=20 -timeout=240s
GOCACHE=/private/tmp/task2-breaker5-go-cache go test ./internal/session ./internal/realtime/s2sws -count=10
GOCACHE=/private/tmp/task2-breaker5-go-cache go test -race ./internal/session ./internal/realtime/s2sws -count=1
GOCACHE=/private/tmp/task2-breaker5-go-cache go test ./... -count=1 -timeout=180s
GOCACHE=/private/tmp/task2-breaker5-go-cache go test -race ./... -count=1 -timeout=180s
GOCACHE=/private/tmp/task2-breaker5-go-cache go test -race ./internal/protocol ./internal/realtime/s2sws ./internal/session ./internal/gateway -count=3
GOCACHE=/private/tmp/task2-breaker5-go-cache go vet ./...
GOCACHE=/private/tmp/task2-breaker5-go-cache go build ./...
GOCACHE=/private/tmp/task2-breaker5-go-cache go mod tidy -diff
git diff --check
```

All passed. The final 18-test causality/edge/publication slice passed 100 consecutive ordinary runs
(session 0.871s, adapter 12.239s) and 20 race-enabled runs (session 1.576s, adapter 4.730s).
Full session/adapter packages passed 10 consecutive runs (30.838s/6.079s); the whole repository
passed fresh ordinary and race runs. Protocol/adapter/session/gateway passed three race-enabled
runs (2.150s/7.895s/12.036s/6.125s). Vet, the clean permissioned build, module tidiness,
formatting, and whitespace verification were clean.

An earlier independent review exposed the stale `speech_stopped` fence bug. The corrected focused
speech-edge/identifier/implicit-response slice passed 20 ordinary and 20 race-enabled runs. Full
session/adapter passed 10 runs (30.880s/5.169s); protocol/adapter/session/gateway passed three race
runs (2.346s/7.797s/12.214s/6.304s); the full repository passed ordinary (26.62s) and a clean fresh
race rerun (16.19s). The first race attempt hit the pre-existing unrelated
`internal/handoff::TestEventPumpScopesNotificationsAndRepairsLostWakeups` ordering flake; that exact
test immediately passed 20 race runs before the complete clean rerun. Vet, clean permissioned build,
module tidiness, formatting, and `git diff --check` passed again.

The next independent pass reported P0/P1/P2 = 0/2/1: the two P1s were the overlapping public
speech-edge mismatch and the `session.updated`/mode-publication race described above; the P2 was
this report's missing final verdict. Both code findings were reproduced before their production
changes and passed the final gates above. The report was then refreshed and the exact post-fix diff
was submitted to a fresh independent reviewer; its final verdict is recorded below.

Final independent Go rereview: P0/P1/P2 = 0/0/0, READY TO COMMIT. The reviewer confirmed the
overlapping speech edge is identity-coherent without a second fence/cancel, the exact acknowledged
mode crosses the ordered effect queue and is locked into the reader boundary before client write,
failed/cancelled runtimes do not share that state, reconnect starts unconfirmed, and the connector's
acknowledgement-presence/exact-sequence separation remains intact. Its focused race slice passed 20
runs, full session/adapter/gateway suites passed with loopback permission, and `git diff --check`
was clean on the reviewed diff.

Pre-review native speech-service mode/fence gates (superseded; working tree based on
`919560114785c7220e209710b06d1f2620e9add8`):

```text
PYTHONPATH=src .venv/bin/pytest -q -p no:cacheprovider tests/openai_realtime/test_realtime_service.py::TestInputCommitOwnershipAndTailIntegrity tests/openai_realtime/test_realtime_service.py::TestHandleAudioCommit tests/openai_realtime/test_websocket_router.py::TestClientEventDispatch::test_same_item_transcript_waits_for_commit_transport_send tests/openai_realtime/test_websocket_router.py::TestClientEventDispatch::test_session_update_success_emits_effective_session_updated
PYTHONPATH=src .venv/bin/pytest -q -p no:cacheprovider tests/openai_realtime/test_realtime_service.py tests/openai_realtime/test_websocket_router.py tests/openai_realtime/test_webrtc.py -k 'not WebRTCLoopback'
PYTHONPATH=src .venv/bin/pytest -q -p no:cacheprovider tests/openai_realtime
PYTHONPATH=src .venv/bin/pytest -q -p no:cacheprovider tests/openai_realtime/test_openai_client.py tests/openai_realtime/test_webrtc.py
PYTHONPATH=src .venv/bin/ruff check --no-cache src/speech_to_speech/api/openai_realtime/handlers/audio.py src/speech_to_speech/api/openai_realtime/handlers/conversation.py src/speech_to_speech/api/openai_realtime/handlers/response.py src/speech_to_speech/api/openai_realtime/handlers/session.py src/speech_to_speech/api/openai_realtime/runtime_config.py src/speech_to_speech/api/openai_realtime/service.py src/speech_to_speech/api/openai_realtime/websocket_router.py tests/openai_realtime/test_openai_client.py tests/openai_realtime/test_realtime_service.py tests/openai_realtime/test_webrtc.py tests/openai_realtime/test_websocket_router.py
git diff --check
```

The focused ownership/fence slice passed 12 tests. The non-listener service/router/WebRTC slice
passed 202 tests with 6 loopback tests deselected. The complete Realtime suite passed 234 tests in
50.53 seconds with one pre-existing Starlette deprecation warning; the separate SDK/WebRTC
loopback slice passed 35 tests in 43.58 seconds with the same warning. Ruff and `git diff --check`
passed. These gates were not sufficient completion evidence: the ensuing independent review found
five P1 and three P2 issues, so this revision was not committed. Direct Task 2 findings were that
speech-start and speech-stop could expose different item identities; failed/no-op sends could open
the fence and lose staged events; an unserialized conversation event could overtake a blocked
commit; `semantic_vad` was advertised without being honored; and cumulative STT snapshots were
mis-emitted as deltas. It also found unbounded item records/staged bytes, four static-format/type
errors, and incomplete capability documentation. The final speech hardening and fresh gates below
supersede these results. Tests used only local service/router/WebRTC/SDK loopback paths, with no live
provider calls or credential access.

The first hardened replacement then passed 247 tests plus mypy/Ruff/format/diff gates, but its fresh
adversarial rereview still failed P0/P1/P2 = 0/5/0, so it also remained uncommitted. The five P1s
were: missing identified-transcript mappings fell back to the current assistant item; an evicted
duplicate stop synthesized and committed a new input item; in-band `response.input` items did not
advance the conversation tail; response creation could overtake speech-start cleanup after the
outbound lock was released; and WebRTC send-failure close could hang/cancel before releasing the
claimed pipeline unit. Those mechanisms were corrected, but the next rereview found one further P1:
a multi-item `response.input` batch could apply its valid prefix to chat/tail state before rejecting a
later invalid item. The final implementation validates and stages the entire batch against isolated
chat state, then publishes all normalized items and tail advances atomically only after full success.

Final native speech evidence at commit `d0270d79ccb6f5c99f76499bb1dbe8a65608cf51`:

```text
PYTHONPATH=src .venv/bin/pytest -q -p no:cacheprovider tests/openai_realtime
```

The exact suite passed 255 tests with one unrelated Starlette/httpx deprecation warning. Mypy over
the 15 changed source files was clean; Ruff lint was clean; Ruff `format --check` over 24 files was
clean; and `git diff --check` passed. The commit contains exactly 15 owned tracked source, README,
and test files, with unrelated speech-repository dirt preserved. A fresh independent rereview of the
committed implementation returned P0/P1/P2 = 0/0/0 and READY.

## Self-review

- Checked mode authority comes only from the configured `turn_detection` value and the matching
  provider `session.updated`; write completion, a missing value, an unknown value, or a mismatched
  value cannot enable audio.
- Checked the confirmed mode's output marker is applied before the acknowledged `session.updated`
  write. The client cannot observe that acknowledgement while the reader still sees `Unspecified`;
  cancelled, suppressed, or never-admitted writes do not bypass the writer's existing gates.
- Checked speech-stop ownership is separate from the mutable active conversation turn. A validated
  delayed stop for A cannot clear B's live speaking fence, publish a stop for A, or let an implicit
  response stream across the barge boundary.
- Checked a distinct overlapping speech start B publishes B's identity without a second epoch,
  flush, or cancellation; a same-ID duplicate stays silent, delayed stop A is ignored, and stop B
  closes the currently exposed edge with B's identity.
- Checked manual and provider-VAD ownership cannot coexist or switch within a live audio stream.
  Provider speech/auto-commit cannot touch the manual slot, and client commit cannot reach the wire
  in provider-VAD mode.
- Checked the round-3 counterexample—commit A, client B, commit C, C transcript/completion, then late
  A—is closed by mutually exclusive authority plus one manual slot and permanent duplicate history,
  not by predecessor, watermark, arrival, ASR, or FIFO inference.
- Checked a provider transcript cannot create its own input authority. Same-item transcripts remain
  closed until committed ownership exists; transcript-before-commit therefore cannot publish or
  durably advance a turn.
- Checked the official success event does not echo the client commit ID and its predecessor is the
  whole-conversation tail; assistant/tool insertion therefore remains test evidence against
  predecessor correlation rather than an input-generation key.
- Preserved the acceptance criteria: 64 total live generation records including tombstones; 1024
  cloned reactor item IDs and at most 256 KiB with 256-byte valid-UTF-8/content-free identifiers;
  exact 1024/1025 no-eviction/reuse coverage; and deep-copied ledger/transcript-sequence fields in
  failure-atomicity views. The adapter's separately bounded manual-ack history is not consumed by
  provider-VAD traffic.
- Checked raw manual acknowledgements cannot stop/reset the adapter deadline; only the exact
  reactor-authorized release of the sole pending manual slot does. Duplicate and delayed ABA items
  remain no-ops, while multiple distinct acknowledgements fail closed.
- Checked the speech provider emits and sends `input_audio_buffer.committed` before releasing its
  same-item transcript fence. Its tail version/predecessor snapshot prevents a speculative input
  item from rolling conversation history backward across client, assistant, tool-call, or
  function-output interposition.
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
