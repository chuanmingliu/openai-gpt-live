# Vertical Profile Realtime Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Resolve one immutable published profile before a voice session starts, apply its system/opening/per-turn prompts without blocking the realtime reactor, preserve it across reconnect and handoff, and prove continuous barge-in remains safe.

**Architecture:** Resolve profile query values after authentication but before `session.Manager.Run`, carry an owned `profile.Resolved` in `session.OpenRequest`, and bind its immutable snapshot into reconnect credentials. Session-level instructions join the provider bootstrap; opening and per-turn rendering use typed, bounded asynchronous effects whose completions are fenced by incarnation, profile hash, input revision, and output epoch.

**Tech Stack:** Go 1.26, existing `internal/transport/ws`, `internal/session`, `internal/realtime/s2sws`, PostgreSQL profile resolver, coder/websocket, race detector.

**Spec:** `docs/superpowers/specs/2026-08-15-vertical-profile-prompt-platform-design.md`

## Global Constraints

- Complete `docs/superpowers/plans/2026-08-15-vertical-profile-foundation.md` first; use its exact `profile.Resolved`, `profile.Resolver`, `ResolvePurpose`, and `prompttmpl` interfaces.
- Resolve profiles before microphone/audio admission; the browser submits only bounded profile ID/version query values and cannot submit prompt, tool, knowledge, model, or endpoint data.
- Reconnect retains the exact profile ID, version, engine version, and snapshot hash; it never resolves “latest.”
- Per-turn rendering, database access, knowledge, worker, client write, and provider write never run synchronously in reactor apply.
- Preserve output-epoch advance-before-cancel, final egress fences, typed reserved completion progress, provider-confirmed input-turn mode, and the single-flight audio-commit contract.
- Repeated barge-in never duplicates provider cancel, response terminal, opening, handoff, or durable work.
- No prompt source/output, transcript, customer data, provider payload, or credential enters logs or metric labels.
- Preserve pre-existing dirty browser/audio files and stage only exact task paths.

---

## File Structure

- `internal/config/config.go` — static `vertical_profiles` gate and bounded renderer queue/worker settings.
- `internal/transport/ws/profile.go` — exact query parsing, pre-open resolve, reconnect match, and safe selection projection.
- `internal/protocol/server.go` / `codec.go` — `live.profile.selected` event.
- `internal/session/profile.go` — session-owned snapshot clone, bootstrap state, and render request/completion types.
- `internal/session/profile_renderer.go` — bounded supervised render dispatcher outside the reactor.
- `internal/session/reactor.go` / `manager.go` — profile bootstrap, opening, per-turn effect, completion, and stale fencing.
- `internal/session/reconnect_credentials.go` — exact profile claims.
- `internal/gateway/app.go` / `cmd/live-gateway/main.go` — resolver and render-pool composition.
- `migrations/000010_handoff_profile_snapshot.*.sql` — durable profile/recipe/evidence columns.
- `internal/gateway/handoffs.go` / `internal/handoff/types.go` — exact snapshot propagation.
- `test/load/bargein_test.go` — deterministic continuous-barge regression matrix.

---

### Task 1: Feature Configuration and Pre-Open Profile Resolution

**Files:**
- Modify: `internal/config/config.go`
- Modify: `internal/config/config_test.go`
- Modify: `internal/config/hash.go`
- Modify: `internal/config/hash_test.go`
- Create: `internal/transport/ws/profile.go`
- Create: `internal/transport/ws/profile_test.go`
- Modify: `internal/transport/ws/handler.go`
- Modify: `internal/transport/ws/handler_test.go`

**Interfaces:**
- Consumes: foundation `profile.Resolver`, `VersionRef`, `Resolved`, `ResolveNewSession`, and `Builtin()`.
- Produces: `config.FeatureConfig.VerticalProfiles`, render-pool bounds, `ws.Options.ProfileResolver`, and `ws.Options.VerticalProfiles`.
- Produces: `session.OpenRequest.Profile profile.Resolved` before manager admission.

- [ ] **Step 1: Write RED config and query-resolution tests**

```go
func TestVerticalProfilesChangeConfigHashAndRequireDurability(t *testing.T) {
	cfg := validConfig()
	cfg.Features.VerticalProfiles = VerticalProfileConfig{Enabled: true, RenderWorkers: 4, RenderQueue: 64}
	cfg.Mode = "local"
	require.Error(t, cfg.Validate())
	cfg.Mode, cfg.Store.DatabaseURLEnv = "development_durable", "DATABASE_URL"
	require.NoError(t, cfg.Validate())
}

func TestProfileQueryResolvesAfterAdmissionBeforeManagerRun(t *testing.T) {
	resolver := &fakeProfileResolver{snapshot: publishedSnapshot("insurance", 3)}
	manager := &recordingManager{}
	res := dialProfileHandler(t, resolver, manager, "/v1/realtime?model=gpt-live&profile=insurance&profile_version=3", admittedSubject("tenant-a"))
	require.Equal(t, http.StatusSwitchingProtocols, res.StatusCode)
	require.Equal(t, []resolveCall{{TenantID: "tenant-a", Ref: profile.VersionRef{ID: "insurance", Version: 3}, Purpose: profile.ResolveNewSession}}, resolver.calls)
	require.Equal(t, resolver.snapshot.Snapshot.SnapshotHash, manager.opened.Profile.Snapshot.SnapshotHash)
}
```

Add missing pair, duplicate query key, leading-zero/overflow version, invalid UTF-8/control/space/over-256 ID, unknown/revoked profile, feature-disabled custom profile, and default built-in cases.

- [ ] **Step 2: Run tests RED**

Run: `go test ./internal/config ./internal/transport/ws -run 'TestVerticalProfiles|TestProfileQuery' -count=1`

Expected: FAIL because the gate, resolver, and query contract do not exist.

- [ ] **Step 3: Add strict configuration and hash binding**

```go
type VerticalProfileConfig struct {
	Enabled bool `yaml:"enabled"`
	RenderWorkers int `yaml:"render_workers"`
	RenderQueue int `yaml:"render_queue"`
}

type FeatureConfig struct { VerticalProfiles VerticalProfileConfig `yaml:"vertical_profiles"` }
```

Default workers to 4 and queue to 64 only when enabled; validate workers 1–64 and queue 1–1,024. Require durable mode and handoffs for custom profiles, hash all three values, and keep the built-in profile available when disabled.

- [ ] **Step 4: Implement exact query parsing and pre-open resolve**

```go
func resolveProfile(ctx context.Context, rawQuery, tenantID string, enabled bool, resolver profile.Resolver) (profile.Resolved, error)
```

Allow only the existing `model`/`reconnect` keys plus one `profile` and one `profile_version`; reject all duplicates and unknown profile keys. Resolve with `profile.ResolveNewSession` after subject admission and before `websocket.Accept` for a new session. Pass the owned result in `session.OpenRequest`. Return only HTTP 400/403/503 classes, never repository or prompt details.

- [ ] **Step 5: Run focused tests GREEN**

Run: `go test -race ./internal/config ./internal/transport/ws -run 'TestVerticalProfiles|TestProfileQuery|TestAdmission' -count=10`

Expected: PASS with resolver call count exactly one.

- [ ] **Step 6: Commit**

```bash
git add internal/config/config.go internal/config/config_test.go internal/config/hash.go internal/config/hash_test.go internal/transport/ws/profile.go internal/transport/ws/profile_test.go internal/transport/ws/handler.go internal/transport/ws/handler_test.go
git commit -m "feat: resolve vertical profiles before realtime admission"
```

---

### Task 2: Profile Selection Event and Reconnect Binding

**Files:**
- Modify: `internal/protocol/server.go`
- Modify: `internal/protocol/codec.go`
- Modify: `internal/protocol/codec_test.go`
- Modify: `internal/protocol/capabilities.go`
- Modify: `internal/protocol/capabilities_test.go`
- Modify: `internal/session/reconnect_credentials.go`
- Modify: `internal/session/reconnect_credentials_test.go`
- Modify: `internal/session/manager.go`
- Modify: `internal/session/manager_test.go`
- Modify: `internal/transport/ws/reconnect_test.go`

**Interfaces:**
- Consumes: Task 1 resolved `profile.Resolved` and its immutable `Snapshot`.
- Produces: `protocol.LiveProfileSelectedEvent` and reconnect claims `ProfileID`, `ProfileVersion`, `ProfileSnapshotHash`, `ProfileEngineVersion`.
- Produces: `session.OpenRequest.Profile` and `ReconnectRequest.Profile` exact-match behavior.

- [ ] **Step 1: Write RED protocol and reconnect tests**

```go
func TestLiveProfileSelectedRoundTripContainsSafeIdentityOnly(t *testing.T) {
	event := LiveProfileSelectedEvent{Type: "live.profile.selected", EventID: "profile-1", Profile: SelectedProfile{ID: "insurance", Version: 3, SnapshotHash: strings.Repeat("a", 64)}}
	raw := mustEncodeServer(t, event)
	assert.NotContains(t, string(raw), "system.j2")
	assert.NotContains(t, string(raw), "prompt")
}

func TestReconnectRejectsDifferentProfileSnapshot(t *testing.T) {
	claims := signedClaims(t, profileRef("insurance", 3, hashA))
	err := reconnectWithQuery(t, claims, profileRef("insurance", 3, hashB))
	require.ErrorIs(t, err, session.ErrInvalidReconnectCredential)
}
```

Add malformed hash/version, missing claims, revoked reconnect, same exact snapshot success, stable event ID/replay, and capability flag cases.

- [ ] **Step 2: Run tests RED**

Run: `go test ./internal/protocol ./internal/session ./internal/transport/ws -run 'TestLiveProfile|TestReconnect.*Profile' -count=1`

Expected: FAIL because the protocol event and claims are absent.

- [ ] **Step 3: Add exact safe protocol types**

```go
type SelectedProfile struct { ID string `json:"id"`; Version uint64 `json:"version"`; SnapshotHash string `json:"snapshot_hash"` }
type LiveProfileSelectedEvent struct { Type string `json:"type"`; EventID string `json:"event_id"`; Profile SelectedProfile `json:"profile"` }
```

Expose `vertical_profiles: true` in capabilities only when enabled. Decoder/encoder limits remain 64 KiB and strict; do not include descriptions, prompts, recipes, tools, or knowledge in the event.

- [ ] **Step 4: Bind reconnect credentials and logical session state**

Add the four profile fields to signed claims and stable serialization. `Manager.Run` copies the snapshot into the logical session before publication; `Reconnect` resolves with `profile.ResolveReconnect` and verifies the result exactly matches claims and retained logical state. Revoked profiles fail before reconnect publication. Emit `live.profile.selected` on initial open and every successful reconnect with a stable attempt event ID.

- [ ] **Step 5: Run protocol/reconnect gates GREEN**

Run: `go test -race ./internal/protocol ./internal/session ./internal/transport/ws -run 'TestLiveProfile|TestReconnect.*Profile|TestCapabilities' -count=20`

Expected: PASS with no credential bytes or prompt content in errors.

- [ ] **Step 6: Commit**

```bash
git add internal/protocol/server.go internal/protocol/codec.go internal/protocol/codec_test.go internal/protocol/capabilities.go internal/protocol/capabilities_test.go internal/session/reconnect_credentials.go internal/session/reconnect_credentials_test.go internal/session/manager.go internal/session/manager_test.go internal/transport/ws/reconnect_test.go
git commit -m "feat: bind profile snapshots to realtime reconnect"
```

---

### Task 3: Session Bootstrap, System Prompt, and Opening Response

**Files:**
- Create: `internal/session/profile.go`
- Create: `internal/session/profile_test.go`
- Modify: `internal/session/manager.go`
- Modify: `internal/session/manager_test.go`
- Modify: `internal/session/reactor.go`
- Modify: `internal/session/reactor_test.go`
- Modify: `internal/session/effect.go`

**Interfaces:**
- Consumes: profile snapshot and compiled prompt bundle.
- Produces: session-owned bootstrap config and `ProfileBootstrapCompletedInput`.
- Produces: exactly one opening `realtime.CommandCreateResponse` after provider session configuration acknowledgement.

- [ ] **Step 1: Write RED bootstrap-order tests**

```go
func TestProfileSystemInstructionsJoinReservedToolsBeforeProviderAck(t *testing.T) {
	manager := managerWithProfile(t, profileWithSystem("Insurance rules."))
	voice := runManager(t, manager)
	update := receiveVoiceCommand(t, voice)
	assert.Equal(t, "Insurance rules.", update.Session.Instructions)
	assert.NotEmpty(t, update.Session.Tools)
}

func TestOpeningStartsOnlyAfterMatchingProviderSessionUpdated(t *testing.T) {
	manager, voice := managerWithOpening(t, "Welcome")
	requireNoVoiceCreate(t, voice)
	submitProviderSessionUpdated(t, manager, 99)
	requireNoVoiceCreate(t, voice)
	submitProviderSessionUpdated(t, manager, manager.ProfileBootstrapSequence())
	assert.Equal(t, "Welcome", requireVoiceCreate(t, voice).Response.Instructions)
	reconnectManager(t, manager)
	submitProviderSessionUpdated(t, manager, manager.ProfileBootstrapSequence())
	requireNoVoiceCreate(t, voice)
}
```

Add render failure-before-audio, browser instruction override rejection, no-opening built-in, reconnect idempotency, and cancellation-before-opening cases.

- [ ] **Step 2: Run tests RED**

Run: `go test ./internal/session -run 'TestProfileSystem|TestOpening|TestProfileBootstrap' -count=1`

Expected: FAIL because bootstrap is global and opening state is absent.

- [ ] **Step 3: Make initial provider configuration session-owned**

```go
type ProfileRuntime struct { Profile profile.Resolved; Compiled prompttmpl.CompiledBundle; SessionContext prompttmpl.Context }
type ProfileBootstrapCompletedInput struct { Sequence uint64; Incarnation identity.Incarnation; SnapshotHash string }
```

Clone `ProfileRuntime` into the logical/runtime session. Merge rendered system instructions with the fixed reserved-tool configuration server-side; client `session.update.instructions`, tools, model, and metadata remain unable to replace profile-owned values. `sendInitialSession` takes the runtime bootstrap config rather than process-global instructions.

- [ ] **Step 4: Emit one fenced opening response**

After the exact provider session-update completion, enqueue `ProfileBootstrapCompletedInput` through the reserved typed lane. Reactor verifies incarnation and snapshot hash, then emits one `CommandCreateResponse` with the rendered opening as server-owned response instructions and `ToolChoice:"none"`. Advance/opening state transactionally so duplicate completions and reconnect replay do not duplicate speech.

- [ ] **Step 5: Run full session tests GREEN**

Run: `go test -race ./internal/session -run 'TestProfileSystem|TestOpening|TestProfileBootstrap|TestManagerInitialSession' -count=20`

Expected: PASS and no PCM accepted before mode/bootstrap acknowledgement.

- [ ] **Step 6: Commit**

```bash
git add internal/session/profile.go internal/session/profile_test.go internal/session/manager.go internal/session/manager_test.go internal/session/reactor.go internal/session/reactor_test.go internal/session/effect.go
git commit -m "feat: bootstrap realtime sessions from profile prompts"
```

---

### Task 4: Asynchronous Per-Turn Prompt Rendering

**Files:**
- Create: `internal/session/profile_renderer.go`
- Create: `internal/session/profile_renderer_test.go`
- Modify: `internal/session/input.go`
- Modify: `internal/session/effect.go`
- Modify: `internal/session/reactor.go`
- Modify: `internal/session/reactor_test.go`
- Modify: `internal/session/manager.go`
- Modify: `internal/session/manager_test.go`

**Interfaces:**
- Produces: `PromptRenderRequestedEffect` and reserved `PromptRenderCompletedInput`.
- Produces: `NewProfileRenderDispatcher(engine, workers, capacity, limits) (*ProfileRenderDispatcher, error)`.
- Consumed by Task 6 and gateway composition.

- [ ] **Step 1: Write RED nonblocking and stale-completion tests**

```go
func TestBlockedTurnRenderCannotDelaySpeechStartFence(t *testing.T) {
	gate := make(chan struct{})
	manager := managerWithBlockingRenderer(t, gate)
	completeTranscript(t, manager, "quote details")
	old := managerFence(t, manager)
	speechStarted(t, manager, "next-turn")
	assert.Greater(t, managerFence(t, manager), old)
}

func TestLateTurnRenderCannotCreateResponseAfterBarge(t *testing.T) {
	gate := make(chan struct{})
	manager, voice := managerWithBlockingRendererAndVoice(t, gate)
	completeTranscript(t, manager, "quote details")
	request := requireRenderRequest(t, manager)
	speechStarted(t, manager, "next-turn")
	close(gate)
	submitRenderedInstructions(t, manager, request, "stale instructions")
	requireNoVoiceCreate(t, voice)
}
```

Add queue full, timeout, cancellation, render-limit, wrong profile hash, wrong incarnation, duplicate completion, no-turn-template compatibility, and provider-without-explicit-instructions rejection.

- [ ] **Step 2: Run tests RED**

Run: `go test ./internal/session -run 'TestBlockedTurnRender|TestLateTurnRender|TestPromptRender' -count=1`

Expected: FAIL because rendering effects/dispatcher do not exist.

- [ ] **Step 3: Define typed request/completion and bounded dispatcher**

```go
type PromptRenderRequestedEffect struct { Incarnation identity.Incarnation; SnapshotHash string; Epoch identity.ResponseEpoch; InputRevision uint32; Turn identity.TurnKey; Transcript string }
type PromptRenderCompletedInput struct { Incarnation identity.Incarnation; SnapshotHash string; Epoch identity.ResponseEpoch; InputRevision uint32; Turn identity.TurnKey; Instructions string; ErrClass string }
```

Dispatcher admission clones and byte-accounts requests, never blocks reactor dispatch, uses fixed workers and queue capacity, renders under the profile limit/context, and submits completion through the reserved typed lane. Overflow returns `render_limit_exceeded` for that response only.

- [ ] **Step 4: Gate provider response creation on exact completion**

For profiles with `turn_instruction.j2`, configure compatible server VAD with automatic response creation disabled. Final transcript produces the render effect; exact current completion emits `CommandCreateResponse{Response.Instructions: rendered}`. Any mismatch, timeout, limit, cancellation, speech-start advance, or render error suppresses response creation and emits only a bounded `live.session.degraded` code.

- [ ] **Step 5: Run focused race and package gates GREEN**

Run: `go test -race ./internal/session -run 'TestBlockedTurnRender|TestLateTurnRender|TestPromptRender' -count=50`

Run: `go test -race ./internal/session -count=3`

Expected: PASS with renderer goroutines joined on manager shutdown.

- [ ] **Step 6: Commit**

```bash
git add internal/session/profile_renderer.go internal/session/profile_renderer_test.go internal/session/input.go internal/session/effect.go internal/session/reactor.go internal/session/reactor_test.go internal/session/manager.go internal/session/manager_test.go
git commit -m "feat: render per-turn prompts outside the realtime reactor"
```

---

### Task 5: Durable Handoff Profile Snapshot

**Files:**
- Create: `migrations/000010_handoff_profile_snapshot.up.sql`
- Create: `migrations/000010_handoff_profile_snapshot.down.sql`
- Modify: `migrations/embed_test.go`
- Modify: `internal/handoff/types.go`
- Modify: `internal/handoff/state_test.go`
- Modify: `internal/gateway/handoffs.go`
- Modify: `internal/gateway/handoffs_test.go`
- Modify: `internal/store/postgres/handoffs.go`
- Modify: `internal/store/postgres/handoffs_integration_test.go`

**Interfaces:**
- Produces: `handoff.ProfileSnapshot` containing profile/recipe/catalog/knowledge hashes and bounded evidence.
- Consumed by the worker/materials plan.

- [ ] **Step 1: Write RED durable round-trip and stale-fence tests**

```go
func TestHandoffPersistsExactProfileRecipeAndKnowledgeSnapshot(t *testing.T) {
	want := handoff.ProfileSnapshot{ProfileID: "insurance", ProfileVersion: 3, SnapshotHash: hashA, RecipeID: "policy_followup", RecipeVersion: 2, RegistryVersion: "tools-v4", Knowledge: []handoff.KnowledgeVersion{{ID: "policy", Version: 7, Hash: hashB}}}
	got := createAndLoadHandoff(t, want)
	assert.Equal(t, want, got.Request.Profile)
}
```

Add wrong tenant, >32 knowledge refs, >256 KiB evidence, mutation-after-enqueue, old-epoch handoff result, revocation-after-acceptance, and migration rollback tests.

- [ ] **Step 2: Run tests RED**

Run: `go test ./internal/handoff ./internal/gateway ./internal/store/postgres -run 'TestHandoff.*Profile|TestProfile.*Handoff' -count=1`

Expected: FAIL because durable profile fields are absent.

- [ ] **Step 3: Add the bounded durable value and migration**

```go
type ProfileSnapshot struct { ProfileID string; ProfileVersion uint64; SnapshotHash, EngineVersion, RecipeID string; RecipeVersion uint64; RecipeHash, RegistryVersion string; Knowledge []KnowledgeVersion; Evidence json.RawMessage }
```

Migration adds non-null safe defaults for existing handoffs and checked columns for IDs, versions, hashes, and JSON byte bounds. New profile-backed handoffs require nonempty exact values at application validation; legacy rows retain explicit `builtin/1` compatibility.

- [ ] **Step 4: Propagate and persist exact cloned bytes**

Attach the session snapshot and selected server-owned recipe to `HandoffRequestedEffect`, `HandoffWork`, `handoff.CreateRequest`, SQL insert/load, clone/hash, journal replay, and idempotency comparison. Model/browser arguments remain evidence data and cannot select registry version or widen bindings.

- [ ] **Step 5: Run durable gates GREEN**

Run: `go test -race ./migrations ./internal/handoff ./internal/gateway ./internal/store/postgres -run 'TestHandoff.*Profile|TestProfile.*Handoff|TestSchema' -count=3`

Expected: PASS with real PostgreSQL evidence when available.

- [ ] **Step 6: Commit**

```bash
git add migrations/000010_handoff_profile_snapshot.up.sql migrations/000010_handoff_profile_snapshot.down.sql migrations/embed_test.go internal/handoff/types.go internal/handoff/state_test.go internal/gateway/handoffs.go internal/gateway/handoffs_test.go internal/store/postgres/handoffs.go internal/store/postgres/handoffs_integration_test.go
git commit -m "feat: persist profile snapshots on durable handoffs"
```

---

### Task 6: Gateway Composition and Continuous Barge-In Regression Matrix

**Files:**
- Modify: `internal/gateway/app.go`
- Modify: `internal/gateway/app_test.go`
- Modify: `cmd/live-gateway/main.go`
- Modify: `cmd/live-gateway/main_test.go`
- Modify: `cmd/live-gateway/main_integration_test.go`
- Modify: `test/load/bargein_test.go`
- Create: `test/integration/vertical_profile_session_test.go`

**Interfaces:**
- Consumes: Tasks 1–5 and foundation publisher/resolver/store.
- Produces: fully composed profile resolver/render dispatcher lifecycle.

- [ ] **Step 1: Write RED composition and continuous-interruption tests**

```go
func TestContinuousBargeWithProfileRenderingPreservesEpochAndProgress(t *testing.T) {
	h := newProfileBargeHarness(t, 17)
	for cycle := 0; cycle < 100; cycle++ {
		h.StartTurn(cycle, cycle%2 == 0)
		h.SetRenderBlocked(cycle%5 == 0)
		h.SetClientWriteBlocked(cycle%7 == 0)
		h.SetCancelSendBlocked(cycle%11 == 0)
		h.Barge(cycle)
		h.InjectDuplicateSpeechStart(cycle)
		h.InjectLateOldEpochOutput(cycle)
		h.ReleaseAll(cycle)
		h.RequireProgressAndNoStaleOutput(cycle)
	}
	h.RequireBoundedAndQuiescent()
}

func TestV1SessionAndReconnectRemainPinnedAfterV2Publication(t *testing.T) {
	env := verticalProfileEnvironment(t)
	v1 := env.PublishAndActivate("insurance", 1)
	session := env.Open("insurance", v1.Version)
	v2 := env.PublishAndActivate("insurance", 2)
	assert.Equal(t, v1.SnapshotHash, env.Reconnect(session.Claims).SnapshotHash)
	assert.Equal(t, v2.SnapshotHash, env.Open("insurance", v2.Version).SnapshotHash)
}
```

Assert no old-epoch media/transcript, at most one terminal/handoff/task per response, queue high-water within declared bounds, no renderer retention after shutdown, and speech fence advance before every held completion.

- [ ] **Step 2: Run tests RED**

Run: `go test ./internal/gateway ./cmd/live-gateway ./test/load ./test/integration -run 'TestContinuousBargeWithProfile|TestV1Session' -count=1`

Expected: FAIL because gateway composition and integration fixtures are incomplete.

- [ ] **Step 3: Compose resolver, cache, dispatcher, and lifecycle**

Add `gateway.WithProfileResolver`, `WithProfileEngine`, and constructor validation. In `cmd/live-gateway`, build the PostgreSQL repository only when vertical profiles are enabled, construct the bounded compile cache and render dispatcher, pass it to manager/transport, and close/join dispatcher before the store pool. Readiness fails if migration 9/10, engine version, or catalog version is incompatible.

- [ ] **Step 4: Complete deterministic integration fixtures**

Use fake provider and real manager/gateway; use test PostgreSQL for version publication. Gates and counters—not sleeps or `runtime.NumGoroutine`—control render, provider send, and browser write. A two-minute wall deadline is a test failsafe, not the correctness assertion.

- [ ] **Step 5: Run final runtime gates GREEN**

Run: `go test -race ./internal/protocol ./internal/transport/ws ./internal/session ./internal/gateway ./cmd/live-gateway -count=3`

Run: `go test -race ./test/load -run TestContinuousBargeWithProfileRenderingPreservesEpochAndProgress -count=20`

Run: `go test ./... -count=1 && go vet ./... && go build ./... && go mod tidy -diff && git diff --check`

Expected: PASS. PostgreSQL integration and audible/live-provider acceptance remain separately recorded.

- [ ] **Step 6: Commit**

```bash
git add internal/gateway/app.go internal/gateway/app_test.go cmd/live-gateway/main.go cmd/live-gateway/main_test.go cmd/live-gateway/main_integration_test.go test/load/bargein_test.go test/integration/vertical_profile_session_test.go
git commit -m "feat: run immutable profiles through realtime sessions"
```

---

## Runtime Acceptance Gate

The runtime plan is accepted only when initial/reconnect selection, system/opening/turn rendering, durable handoff snapshotting, migration evidence, continuous barge-in race repetitions, and the full repository gates pass. Manual acceptance must hear the opening, interrupt pending and streaming replies repeatedly, and confirm no stale audio resumes; automated tests cannot substitute for that acoustic check.
