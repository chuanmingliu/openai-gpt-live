# Vertical Profile Setup Experience Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn `/setup` into the approved profile workspace, expose strict loopback profile/material APIs, add the published-profile selector to the voice page, and verify a complete insurance-sales workflow without weakening existing lifecycle or barge-in behavior.

**Architecture:** The loopback BFF composes the shared profile publisher/store directly with a private PostgreSQL pool and serves exact same-origin administration endpoints. Dependency-free browser modules keep runtime controls, draft editing, prompts, recipes, adapters, knowledge, and publication separate; the voice page forwards only profile ID/version and waits for exact profile plus provider acknowledgements before capture.

**Tech Stack:** Go 1.26, pgx v5, existing loopback BFF, dependency-free ECMAScript modules with Node 22 tests, HTML/CSS, existing wizard/Compose/static gates.

**Spec:** `docs/superpowers/specs/2026-08-15-vertical-profile-prompt-platform-design.md`

## Global Constraints

- Complete the foundation, runtime, and worker/material plans first and consume their exact shared interfaces.
- `/setup` remains loopback-only and keeps the existing Runtime service controls fully functional.
- The browser never receives or submits database URLs, credentials, secret values/references, commands, provider endpoints, model configuration, raw MCP JSON, or executable adapter fields.
- Draft saves use exact generation/ETag; stale writes return 409 and never overwrite newer state. Publication names one exact generation and is atomic.
- Template editor paths are logical bundle names only; every prompt source is a `.j2` blob, not an operator filesystem path.
- Browser configuration is held in controller memory only; no cookie, local/session storage, query fragment, URL credential, or console log stores prompts/material/customer data.
- Voice selection is immutable after WebSocket establishment; capture waits for exact `live.profile.selected` and provider `session.updated` acknowledgements.
- Existing protected dirty browser/audio files must be recorded before work, merged rather than replaced, staged with `git add -p`, and checked against their pre-task digest after every overlapping task.

---

## File Structure

- `demo/live-client/profile_service.go` — BFF composition and lifecycle for profile publisher, knowledge ingestor, and PostgreSQL pool.
- `demo/live-client/profile_api.go` — strict path/method/schema/origin/body/error handling.
- `demo/live-client/web/profile-api.mjs` — exact response decoders and abortable same-origin client.
- `demo/live-client/web/profile-draft-state.mjs` — generation-aware immutable editor state.
- `demo/live-client/web/setup-profile.mjs` — tab/controller orchestration and accessibility.
- `demo/live-client/web/profile-editor.mjs` — prompt tree, source editor, context/fixture preview.
- `demo/live-client/web/recipe-editor.mjs` — worker recipe and binding editor.
- `demo/live-client/web/knowledge-editor.mjs` — upload, preview, publish, and tombstone UI.
- `demo/live-client/web/voice-profile-state.mjs` — safe published list, immutable selection, and ack gate.
- `demo/live-client/web/setup.html` / `setup.css` — approved seven-tab layout.
- `demo/live-client/web/index.html` / `app.js` / `browser-runtime.mjs` — published selector and capture gating.
- `deploy/scripts/demo-wizard.sh` / Compose/config docs — private durable profile setup.

---

### Task 1: Loopback Profile Service Composition and API Boundary

**Files:**
- Modify: `demo/live-client/config.go`
- Modify: `demo/live-client/config_test.go`
- Create: `demo/live-client/profile_service.go`
- Create: `demo/live-client/profile_service_test.go`
- Create: `demo/live-client/profile_api.go`
- Create: `demo/live-client/profile_api_test.go`
- Modify: `demo/live-client/server.go`
- Modify: `demo/live-client/server_test.go`

**Interfaces:**
- Consumes: `profile.Store`, `Publisher`, `Resolver`, `knowledge.Ingestor`, and adapter catalog.
- Produces: `profileAdminService` and routes under `/api/setup/v1/*`.
- Consumed by Tasks 2–5.

- [ ] **Step 1: Write RED lifecycle and hostile HTTP tests**

```go
func TestProfileAdminRequiresLoopbackOperatorDurableConfiguration(t *testing.T) {
	_, err := parseConfig([]string{"-operator", "-vertical-profiles", "-profile-database-url-env", "DATABASE_URL"})
	require.ErrorContains(t, err, "profile_operator_identity_required")
}

func TestSetupAPIRejectsWrongOriginMediaMethodUnknownFieldAndOversize(t *testing.T) {
	store := &countingProfileStore{}
	for _, tc := range []struct{name string; req *http.Request; status int}{
		{"origin", requestWithOrigin("https://evil.invalid"), 403},
		{"method", requestWithMethod(http.MethodTrace), 405},
		{"media", requestWithMediaType("text/plain"), 415},
		{"json", requestWithBody(`{"unknown":true}`), 400},
		{"body", requestWithBody(strings.Repeat("x", maxProfileBody+1)), 413},
	} {
		t.Run(tc.name, func(t *testing.T) { assert.Equal(t, tc.status, serveProfileAPIWithStore(t, store, tc.req).Code) })
	}
	assert.Zero(t, store.Calls())
}
```

Add pool open/close, server shutdown ordering, missing schema/readiness, request-cancel-after-admission, duplicate headers/JSON keys, encoded traversal, path normalization, raw SQL/error privacy, and operator Client Stop/Start persistence cases.

- [ ] **Step 2: Run tests RED**

Run: `go test ./demo/live-client -run 'TestProfileAdmin|TestSetupAPI' -count=1`

Expected: FAIL because service/config/routes are absent.

- [ ] **Step 3: Add private exact configuration**

```go
type profileAdminConfig struct { Enabled bool; DatabaseURLEnv, TenantID, Actor string; RequestTimeout time.Duration }
```

Add flags `-vertical-profiles`, `-profile-database-url-env`, `-profile-tenant`, and `-profile-actor`. Validate environment-name syntax, bounded tenant/actor IDs, loopback operator mode, and 1–30 s timeout. Resolve the URL value from environment without returning/logging it; construct one bounded pgx pool and close it after HTTP/profile actions join.

- [ ] **Step 4: Implement one strict router boundary**

Dispatch exact decoded path segments under `/api/setup/v1/`; reject escaped slash, dot segments, empty IDs, duplicate query keys, wrong method/media/origin, and trailing JSON. Use body caps: 1 MiB JSON, 64 KiB/template, 8 MiB/document upload. Map only stable classes/statuses and add `Cache-Control:no-store`; never serialize dependency errors.

- [ ] **Step 5: Run focused lifecycle/API gates GREEN**

Run: `go test -race ./demo/live-client -run 'TestProfileAdmin|TestSetupAPI|TestOperatorAPI' -count=20`

Expected: PASS; Client Stop closes `/ws` but `/setup` and profile drafts remain available, matching existing operator lifecycle semantics.

- [ ] **Step 6: Commit**

```bash
git add demo/live-client/config.go demo/live-client/config_test.go demo/live-client/profile_service.go demo/live-client/profile_service_test.go demo/live-client/profile_api.go demo/live-client/profile_api_test.go demo/live-client/server.go demo/live-client/server_test.go
git commit -m "feat: expose loopback vertical profile administration"
```

---

### Task 2: Profiles, Draft Generations, Validation, and Publication API

**Files:**
- Modify: `demo/live-client/profile_api.go`
- Modify: `demo/live-client/profile_api_test.go`
- Create: `demo/live-client/profile_dto.go`
- Create: `demo/live-client/profile_dto_test.go`

**Interfaces:**
- Produces exact endpoints for list/create/load/save, template get/put/delete, validate/preview/publish/activate/revoke.
- Produces safe `publishedProfileSummary` consumed by Task 6.

- [ ] **Step 1: Write RED exact route/DTO tests**

```go
func TestDraftSaveRequiresIfMatchGeneration(t *testing.T) {
	req := jsonRequest(http.MethodPut, "/api/setup/v1/profiles/insurance/draft", `{"display_name":"Insurance"}`)
	res := serveProfileAPI(t, req)
	assert.Equal(t, http.StatusPreconditionRequired, res.Code)
}

func TestStaleDraftReturns409WithoutNewerManifest(t *testing.T) {
	store := storeWithDraft(draftAtGeneration(4, "newer-source"))
	req := jsonRequest(http.MethodPut, "/api/setup/v1/profiles/insurance/draft", `{"display_name":"stale"}`)
	req.Header.Set("If-Match", `"3"`)
	res := serveProfileAPIWithStore(t, store, req)
	assert.Equal(t, http.StatusConflict, res.Code)
	assert.JSONEq(t, `{"error":{"code":"stale_draft"}}`, res.Body.String())
	assert.Equal(t, "newer-source", store.Draft().Templates["system.j2"])
	assert.EqualValues(t, 4, store.Draft().Generation)
}

func TestTemplatePutAtomicallyStoresBlobAndAdvancesDraftGeneration(t *testing.T) {
	store := storeWithDraft(draftAtGeneration(4, "old-source"))
	req := textRequest(http.MethodPut, "/api/setup/v1/profiles/insurance/templates/system.j2", "new-source")
	req.Header.Set("If-Match", `"4"`)
	res := serveProfileAPIWithStore(t, store, req)
	assert.Equal(t, http.StatusOK, res.Code)
	assert.Equal(t, "new-source", store.Draft().Templates["system.j2"])
	assert.EqualValues(t, 5, store.Draft().Generation)
}
```

Cover cursor/list limits, create ID collisions, inactive/revoked summaries, exact generation, template path/media/body/hash/save/delete atomicity, validate/preview no mutation, publish exact generation, failed publish leaves active, activate/revoke, and safe field allowlists.

- [ ] **Step 2: Run RED**

Run: `go test ./demo/live-client -run 'TestDraft|TestProfileList|TestPublish|TestActivate|TestRevoke' -count=1`

Expected: FAIL because DTOs/routes are incomplete.

- [ ] **Step 3: Implement strict DTO projections**

```go
type draftResponse struct { ID string `json:"id"`; Generation int64 `json:"generation"`; Manifest profile.Manifest `json:"manifest"` }
type publishedProfileSummary struct { ID, DisplayName, Description string; Version uint64; SnapshotHash string; Active, Revoked bool }
```

Decode DTOs field-by-field with duplicate/unknown rejection; never accept tenant, actor, hashes, engine version, registry version, published timestamps, or authority flags from the browser. Server injects tenant/actor and computes hashes.

- [ ] **Step 4: Implement route semantics**

Use `If-Match:"<generation>"` for draft and template mutation and exact `expected_generation` for validate/preview/publish. `GET|PUT|DELETE /profiles/{id}/templates/{logical-path}` permits only normalized bundle-local `.j2` paths and `text/plain; charset=utf-8`; PUT/DELETE calls the store's atomic template mutation and returns the new draft ETag. Return ETag on draft reads/saves. `GET /profiles?state=published&limit=...&cursor=...` returns only authorized safe summaries. Use 201 create/publish, 200 read/save/preview/template, 204 delete/activate/revoke, 409 stale/conflict, 422 validation, and 503 unavailable.

- [ ] **Step 5: Run API gates GREEN**

Run: `go test -race ./demo/live-client -run 'TestDraft|TestProfileList|TestPublish|TestActivate|TestRevoke' -count=20`

Expected: PASS with content-free generic errors and authorized preview detail only.

- [ ] **Step 6: Commit**

```bash
git add demo/live-client/profile_api.go demo/live-client/profile_api_test.go demo/live-client/profile_dto.go demo/live-client/profile_dto_test.go
git commit -m "feat: manage versioned profile drafts"
```

---

### Task 3: Profile Workspace Shell and Prompt Editor

**Files:**
- Create: `demo/live-client/web/profile-api.mjs`
- Create: `demo/live-client/web/profile-api.test.mjs`
- Create: `demo/live-client/web/profile-draft-state.mjs`
- Create: `demo/live-client/web/profile-draft-state.test.mjs`
- Create: `demo/live-client/web/setup-profile.mjs`
- Create: `demo/live-client/web/setup-profile.test.mjs`
- Create: `demo/live-client/web/profile-editor.mjs`
- Create: `demo/live-client/web/profile-editor.test.mjs`
- Modify: `demo/live-client/web/setup.html`
- Modify: `demo/live-client/web/setup.css`
- Modify: `demo/live-client/web/setup.mjs`
- Modify: `demo/live-client/server.go`
- Modify: `demo/live-client/server_test.go`

**Interfaces:**
- Consumes: Tasks 1–2 API.
- Produces: approved Runtime/Profile/Prompts/Worker recipes/Tools & adapters/Knowledge/Review & publish tabs.

- [ ] **Step 1: Record the protected browser baseline and write RED model tests**

Run: `git status --short` and record SHA-256 plus diff for every already-dirty browser/audio file; do not stash or reset it.

```js
test('stale save keeps local source and offers reload instead of overwriting', async () => {
  const model = createDraftModel(draft(3));
  await assert.rejects(model.save(apiReturning(409, {code: 'stale_draft'})));
  assert.equal(model.template('system.j2'), 'local unsaved source');
  assert.equal(model.status().conflict, true);
});
```

Add strict response decode, abort/destroy, tab keyboard/ARIA, Runtime preservation, no DOM `innerHTML`, template logical path, unsupported syntax, preview bounds, and no browser storage/console cases.

- [ ] **Step 2: Run browser tests RED**

Run: `node --test demo/live-client/web/profile-api.test.mjs demo/live-client/web/profile-draft-state.test.mjs demo/live-client/web/setup-profile.test.mjs demo/live-client/web/profile-editor.test.mjs`

Expected: FAIL because modules/layout are absent.

- [ ] **Step 3: Implement immutable API/state ports**

`profile-api.mjs` accepts injected `fetch`, sends same-origin credentials, exact media and abort signals, and validates every key/type/bound before returning owned data. `profile-draft-state.mjs` keeps one generation and immutable change set; save success replaces generation, 409 marks conflict, reload is explicit, and destroy clears source/preview bytes.

- [ ] **Step 4: Implement the approved seven-tab layout and prompt editor**

Keep existing service cards inside Runtime. Profile has identity/version state. Prompts uses three panes: logical `.j2` file tree, plain-text source editor, and schema/fixture/render preview. Disable publish while dirty/unvalidated/conflicted. Use text nodes and form values only; show stable error class plus authorized line/column without echoing rejected source in announcements.

- [ ] **Step 5: Serve assets and run browser/BFF gates GREEN**

Add exact GET routes and MIME types for new modules. Run:

```bash
node --test demo/live-client/web/*.test.mjs
go test -race ./demo/live-client -run 'TestSetupAssets|TestStaticAssets|TestProfile' -count=10
```

Expected: PASS; baseline dirty-file content remains present in the merged result.

- [ ] **Step 6: Commit only reviewed hunks**

Use `git add` for new files and `git add -p` for pre-existing dirty setup/server assets. Inspect `git diff --cached` and compare protected baseline digests before:

```bash
git commit -m "feat: edit profile prompts in setup"
```

---

### Task 4: Worker Recipe and Adapter Binding Tabs

**Files:**
- Create: `demo/live-client/web/recipe-editor.mjs`
- Create: `demo/live-client/web/recipe-editor.test.mjs`
- Create: `demo/live-client/web/adapter-editor.mjs`
- Create: `demo/live-client/web/adapter-editor.test.mjs`
- Modify: `demo/live-client/web/setup-profile.mjs`
- Modify: `demo/live-client/web/setup.html`
- Modify: `demo/live-client/web/setup.css`
- Modify: `demo/live-client/profile_api.go`
- Modify: `demo/live-client/profile_api_test.go`

**Interfaces:**
- Consumes: adapter catalog and profile Worker Recipe domain.
- Produces: safe catalog GET and draft recipe/binding edits only.

- [ ] **Step 1: Write RED no-loosening and recipe UX tests**

```js
test('adapter editor exposes tightening controls but no endpoint command or secret fields', () => {
  const model = decodeCatalog(catalogFixture());
  assert.deepEqual(Object.keys(model.adapters[0].binding), ['enabled', 'timeout_ms', 'max_concurrency', 'max_result_bytes']);
});

test('worker objective is identified as async worker prompt, not handoff policy', () => {
  assert.match(renderRecipeHelp(), /asynchronous worker objective/i);
  assert.doesNotMatch(renderRecipeHelp(), /decides.*handoff/i);
});
```

Add recipe count/ID/template/schema/limit bounds, missing catalog version, side-effect approval display, draft conflict, and malformed catalog fail-closed cases.

- [ ] **Step 2: Run RED**

Run: `node --test demo/live-client/web/recipe-editor.test.mjs demo/live-client/web/adapter-editor.test.mjs && go test ./demo/live-client -run TestAdapterCatalogAPI -count=1`

Expected: FAIL because tabs/API projection are absent.

- [ ] **Step 3: Implement safe catalog projection and editors**

API returns manifest identity, display metadata, stable tool schema, classifications, installed health class, and tighten-only maxima—never destination details, transports, secret refs, or executable configuration. Recipe editor manages `workers/<id>.j2`, evidence schema, bounds, approvals, and binding IDs. Adapter editor uses checkboxes/bounded numeric controls constrained by server maxima.

- [ ] **Step 4: Run UI/API gates GREEN**

Run: `node --test demo/live-client/web/recipe-editor.test.mjs demo/live-client/web/adapter-editor.test.mjs && go test -race ./demo/live-client -run 'TestAdapterCatalogAPI|TestProfile' -count=20`

Expected: PASS and hostile manual DOM values are rejected again server-side.

- [ ] **Step 5: Commit reviewed hunks**

```bash
git add demo/live-client/web/recipe-editor.mjs demo/live-client/web/recipe-editor.test.mjs demo/live-client/web/adapter-editor.mjs demo/live-client/web/adapter-editor.test.mjs demo/live-client/profile_api.go demo/live-client/profile_api_test.go
git add -p demo/live-client/web/setup-profile.mjs demo/live-client/web/setup.html demo/live-client/web/setup.css
git commit -m "feat: configure worker recipes and adapters"
```

---

### Task 5: Knowledge Upload, Preview, Publication, and Tombstones

**Files:**
- Create: `demo/live-client/web/knowledge-editor.mjs`
- Create: `demo/live-client/web/knowledge-editor.test.mjs`
- Modify: `demo/live-client/web/setup-profile.mjs`
- Modify: `demo/live-client/web/setup.html`
- Modify: `demo/live-client/web/setup.css`
- Modify: `demo/live-client/profile_api.go`
- Modify: `demo/live-client/profile_api_test.go`

**Interfaces:**
- Consumes: knowledge ingestor/store from the worker/material plan.
- Produces: upload/preview/publish/list/tombstone endpoints and bounded UI.

- [ ] **Step 1: Write RED upload and privacy tests**

```go
func TestKnowledgeUploadRejectsOversizeWrongSignatureAndTrailingMultipart(t *testing.T) {
	ingestor := &countingIngestor{}
	for _, tc := range []struct{name string; upload []byte; status int}{
		{"upload limit", bytes.Repeat([]byte{'x'}, maxUploadBytes+1), 413},
		{"signature", append([]byte("not-a-docx"), []byte("secret")...), 422},
		{"trailing multipart", multipartWithTrailingPart(t), 422},
	} {
		res := uploadKnowledge(t, ingestor, "secret-customer-name.docx", tc.upload)
		assert.Equal(t, tc.status, res.Code)
		assert.NotContains(t, res.Body.String(), "secret-customer-name")
	}
	assert.Zero(t, ingestor.Calls())
}
```

```js
test('destroy clears selected file and extracted preview', async () => {
  const controller = createKnowledgeController(fakePorts());
  controller.load(previewFixture());
  controller.destroy();
  assert.deepEqual(controller.snapshot(), emptyKnowledgeState());
});
```

Add encrypted/scanned unsupported display, citation preview, exact preview hash on publish, stale/tombstoned version, cancellation, upload retry, no raw browser persistence, and bounded filename/error announcements.

- [ ] **Step 2: Run RED**

Run: `go test ./demo/live-client -run TestKnowledgeAPI -count=1 && node --test demo/live-client/web/knowledge-editor.test.mjs`

Expected: FAIL because routes/editor are absent.

- [ ] **Step 3: Implement strict multipart and exact publish flow**

Accept one file and bounded metadata fields, stream through `http.MaxBytesReader`, reject unexpected parts/headers, and pass owned bytes to `Ingestor.Preview`. Return bounded excerpts/citations and preview hash only to the authorized editor. Publish requires exact preview hash and profile draft generation; tombstone uses POST with exact version, never DELETE-by-path ambiguity.

- [ ] **Step 4: Implement the Knowledge tab**

Show pack/version/status/classification, file selection, extraction progress class, bounded excerpts/citations, publish, and tombstone. Never inject document HTML; use text nodes. Disable profile publication when referenced versions are draft/tombstoned/unavailable.

- [ ] **Step 5: Run knowledge UI/API gates GREEN**

Run: `go test -race ./demo/live-client -run 'TestKnowledgeAPI|TestProfile' -count=20 && node --test demo/live-client/web/knowledge-editor.test.mjs demo/live-client/web/setup-profile.test.mjs`

Expected: PASS with no raw upload retained after successful extraction.

- [ ] **Step 6: Commit reviewed hunks**

```bash
git add demo/live-client/web/knowledge-editor.mjs demo/live-client/web/knowledge-editor.test.mjs demo/live-client/profile_api.go demo/live-client/profile_api_test.go
git add -p demo/live-client/web/setup-profile.mjs demo/live-client/web/setup.html demo/live-client/web/setup.css
git commit -m "feat: manage versioned knowledge in setup"
```

---

### Task 6: Voice Profile Selector, Proxy Query, and Capture Ack Gate

**Files:**
- Create: `demo/live-client/web/voice-profile-state.mjs`
- Create: `demo/live-client/web/voice-profile-state.test.mjs`
- Modify: `demo/live-client/web/index.html`
- Modify: `demo/live-client/web/styles.css`
- Modify: `demo/live-client/web/app.js`
- Modify: `demo/live-client/web/browser-runtime.mjs`
- Modify: `demo/live-client/web/browser-runtime.test.mjs`
- Modify: `demo/live-client/web/protocol-state.mjs`
- Modify: `demo/live-client/web/protocol-state.test.mjs`
- Modify: `demo/live-client/proxy.go`
- Modify: `demo/live-client/proxy_test.go`
- Modify: `demo/live-client/server.go`
- Modify: `demo/live-client/server_test.go`

**Interfaces:**
- Consumes: safe published profile list and runtime `live.profile.selected` event.
- Produces: immutable pre-connect `{id,version}` selection and two-ack capture gate.

- [ ] **Step 1: Record overlapping dirty-file digests and write RED tests**

```js
test('capture waits for exact profile and provider acknowledgements', () => {
  const state = createVoiceProfileState({id: 'insurance', version: 3});
  state.onProviderUpdated();
  assert.equal(state.captureReady(), false);
  state.onProfileSelected({id: 'insurance', version: 3, snapshot_hash: HASH});
  assert.equal(state.captureReady(), true);
});

test('mismatched profile ack fails closed and selection cannot change after connect', () => {
  const state = createVoiceProfileState({id: 'insurance', version: 3});
  state.onConnected();
  assert.throws(() => state.select({id: 'claims', version: 1}), /selection_locked/);
  state.onProfileSelected({id: 'insurance', version: 4, snapshot_hash: HASH});
  state.onProviderUpdated();
  assert.equal(state.captureReady(), false);
  assert.equal(state.failureClass(), 'profile_mismatch');
});
```

Add strict published-list decoder, list failure built-in fallback, duplicate/malformed ack, reconnect exact selection, no PCM before both acks, no storage, query encoding, proxy unknown/duplicate/traversal rejection, and existing tool-policy/audio gate coexistence.

- [ ] **Step 2: Run RED**

Run: `node --test demo/live-client/web/voice-profile-state.test.mjs demo/live-client/web/browser-runtime.test.mjs demo/live-client/web/protocol-state.test.mjs && go test ./demo/live-client -run 'TestProxy.*Profile|TestWebSocket.*Profile' -count=1`

Expected: FAIL because selector/query/ack gate are absent.

- [ ] **Step 3: Implement in-memory immutable selection and strict proxy forwarding**

Fetch safe summaries before Start. Default to `builtin/1`; disable selector from connect until disconnect. Browser opens `/ws?profile=<encoded>&profile_version=<decimal>`. BFF parses one exact pair, validates ID/version, and forwards only those two values plus its fixed model to the gateway; it never forwards arbitrary browser query keys.

- [ ] **Step 4: Add the profile acknowledgement to capture readiness**

Decode `live.profile.selected` in protocol state and require exact selected ID/version plus valid hash. `browser-runtime` treats profile and provider acknowledgements as separate monotonic gates; microphone worklet starts only after both, and any mismatch/degraded/reconnect failure closes capture. Preserve tool-policy acknowledgement, audio ordering, and barge-in flush behavior.

- [ ] **Step 5: Run repeated browser/BFF gates GREEN**

Run:

```bash
node --test demo/live-client/web/*.test.mjs
go test -race ./demo/live-client -run 'TestProxy|TestWebSocket|TestStaticAssets' -count=20
```

Expected: PASS with protected baseline diffs preserved.

- [ ] **Step 6: Commit only exact merged hunks**

Use `git add` for the new module and `git add -p` for every protected file. Inspect cached diff and digest preservation before:

```bash
git commit -m "feat: select profiles before voice capture"
```

---

### Task 7: Deployment, Wizard, Documentation, and End-to-End Acceptance

**Files:**
- Modify: `deploy/config/development-control-panel.yaml`
- Modify: `deploy/config/development-durable.yaml`
- Modify: `deploy/compose.development-control-panel.yml`
- Modify: `deploy/compose.development-durable.yml`
- Modify: `deploy/scripts/demo-wizard.sh`
- Modify: `deploy/scripts/test-demo-wizard.sh`
- Modify: `deploy/scripts/test-control-panel-compose-render.sh`
- Modify: `deploy/scripts/verify-deploy-static.sh`
- Modify: `demo/live-client/README.md`
- Modify: `docs/demo/realtime-browser-client.md`
- Modify: `docs/protocol.md`
- Modify: `docs/security.md`
- Modify: `docs/operations.md`
- Create: `test/integration/vertical_profile_setup_test.go`

**Interfaces:**
- Consumes: all prior plans.
- Produces: operator-ready `/setup` flow and release evidence.

- [ ] **Step 1: Write RED render/wizard/e2e tests**

```go
func TestInsuranceProfileSetupFlow(t *testing.T) {
	env := newSetupE2E(t, realPostgres(t), fakeRealtimeProviders(t))
	draft := env.CreateInsuranceDraft()
	draft = env.SaveTemplates(draft, insuranceTemplateBundle())
	draft = env.BindRecipeCatalogAndKnowledge(draft, "policy_followup", "tools-v1", "policy-v1")
	require.Contains(t, env.Preview(draft).Opening, "insurance")
	v1 := env.PublishAndActivate(draft)
	session := env.SelectAndOpen(v1.Ref)
	v2 := env.PublishAndActivate(env.Revise(draft, insuranceV2Bundle()))
	assert.Equal(t, v1.SnapshotHash, env.Reconnect(session).SnapshotHash)
	assert.Equal(t, v2.SnapshotHash, env.SelectAndOpen(v2.Ref).SnapshotHash)
}
```

Shell boundaries prove DB URL stays off argv/output, Compose services receive the same private DSN, migrations 9–11 run before readiness, voice-only wizard omits durable secrets, and static deploy rejects vertical profiles without schema/catalog configuration.

- [ ] **Step 2: Run tests RED**

Run: `go test ./test/integration -run TestInsuranceProfileSetupFlow -count=1 && bash deploy/scripts/test-demo-wizard.sh && bash deploy/scripts/test-control-panel-compose-render.sh`

Expected: FAIL until deployment and wizard flags are wired.

- [ ] **Step 3: Wire private deployment configuration and interactive wizard**

Add non-secret feature/bound settings to YAML; derive the same encoded DSN through the existing durable wrapper for migrate/gateway/worker/BFF without publishing PostgreSQL. Wizard explains built-in voice-only versus durable vertical-profile mode, requires private DB/auth/tool secrets only for the latter, starts no service before explicit buttons, and prints the exact `/setup` URL without credentials.

- [ ] **Step 4: Complete protocol/security/operator documentation**

Document Jinja-Go v1, every tab/API, Worker Recipe meaning, adapter scaffold, MCP pinning, knowledge limits, profile/reconnect events, capture gate, rollback/revocation, privacy, and explicit exclusions. Include step-by-step insurance-sales example and manual continuous-barge checklist.

- [ ] **Step 5: Run full automated gates**

```bash
make test-demo-live-client
go test -race -count=1 ./...
go vet ./...
go build ./...
go mod tidy -diff
make verify-deploy-static
bash -n deploy/scripts/*.sh
git diff --check
```

Run privacy scans for secret-looking values plus prompt/material/transcript fixtures outside testdata. Real PostgreSQL integration is mandatory; Docker/provider absence is recorded, not converted to PASS.

- [ ] **Step 6: Perform manual acceptance**

Start the interactive demo; publish an insurance profile; select it; verify the microphone waits for both acks; hear the opening; converse and barge repeatedly; run cited async knowledge work; publish v2; verify connected/reconnected v1 and new v2 behavior; revoke and test new/reconnect failure. Record audible/provider/PostgreSQL evidence separately from automated tests.

- [ ] **Step 7: Commit**

```bash
git add deploy/config/development-control-panel.yaml deploy/config/development-durable.yaml deploy/compose.development-control-panel.yml deploy/compose.development-durable.yml deploy/scripts/demo-wizard.sh deploy/scripts/test-demo-wizard.sh deploy/scripts/test-control-panel-compose-render.sh deploy/scripts/verify-deploy-static.sh demo/live-client/README.md docs/demo/realtime-browser-client.md docs/protocol.md docs/security.md docs/operations.md test/integration/vertical_profile_setup_test.go
git commit -m "docs: operate vertical profile setup workflow"
```

---

## Complete Program Acceptance Gate

The program is complete only when all four plans pass independent review, the full repository is green under race/static gates, migrations 9–11 run against real PostgreSQL, `/setup` completes the insurance workflow, exact v1/v2/reconnect/revocation behavior is proven, privacy scans are clean, protected user browser changes remain intact, and manual audible continuous-barge acceptance confirms stale output never resumes.
