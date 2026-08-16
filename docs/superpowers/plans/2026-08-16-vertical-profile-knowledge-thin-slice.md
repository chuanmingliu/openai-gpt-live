# Vertical Profile Knowledge — Thin Slice Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Unblock knowledge-bearing profiles and the `/setup` Knowledge tab with the smallest correct subsystem: immutable versioned packs, text/Markdown ingestion, a publisher-side reference validator, and deterministic cited `knowledge.search`. Binary document extraction is deliberately deferred and, when it arrives, lands out of process.

**Architecture:** Identical domain and authority model to `2026-08-15-vertical-profile-worker-materials.md` Tasks 4–6 — packs are tenant-scoped and immutable, a profile pins exact versions, and the worker reads only what the handoff pinned. The reduction is entirely in the *input* stage: this plan accepts UTF-8 text and Markdown only, so it adds no PDF/DOCX parser, no new module dependency, and no untrusted-binary parsing inside the BFF or worker process. Every interface is shaped so the extractor can be added later without changing a caller.

**Tech Stack:** Go 1.26, PostgreSQL 16 (`postgres:16-alpine`, pinned by digest in `deploy/docker-compose.yml`), pgx v5, existing worker/reasoner/tool-policy packages. No new module dependencies.

**Supersedes:** Tasks 4–6 of `docs/superpowers/plans/2026-08-15-vertical-profile-worker-materials.md`, and unblocks Task 5 of `docs/superpowers/plans/2026-08-15-vertical-profile-setup-experience.md`.

**Spec:** `docs/superpowers/specs/2026-08-15-vertical-profile-prompt-platform-design.md`

---

## Why this shape

Three observations drove the re-scope.

1. **The publisher seam is already cut and empty.** `profile.KnowledgeReferenceValidator` is a `NewPublisher` dependency, and `demo/live-client/profile_service.go` passes `nil` for it. `publisher.go` then rejects any manifest carrying knowledge refs with `ErrProfileIncompatible`. So today a knowledge-bearing profile fails closed correctly but can never be published. Implementing the validator is a small, discrete task that the original plan never names — and it is the actual unblock for the `/setup` tab.

2. **Binary extraction is the riskiest code in the original plan and the least load-bearing.** PDF and DOCX parsing of operator-supplied files (malformed streams, zip bombs, deep recursion, parser panics) was specified to run in-process alongside the profile store and a live audio path. Deferring it removes that surface entirely at close to zero cost for a loopback developer demo, where converting a source document to Markdown is a one-line operation for the operator.

3. **Postgres full-text search is not the free win it looks like.** Stock PostgreSQL ships no Chinese text-search parser; `zhparser`/`pg_jieba` are external extensions absent from the pinned `postgres:16-alpine` image. Since the deployed tool configs already list `cn-zh` as a search region, CJK is a live requirement, and a hand-written deterministic ranker is cheaper than committing to a custom database image. The original plan's choice stands.

---

## Global Constraints

Inherited unchanged from the worker/materials plan, plus the reductions below.

- The exact durable profile snapshot from migration 9 remains the authority; this plan adds migration 10 only. The source plans assumed profiles were migration 10; they landed in this repository as `000009_vertical_profiles`.
- A profile may pin knowledge versions; it may never supply excerpt text, tokens, ranking weights, queries, storage locations, or tenant identity.
- `knowledge.search` is read-only, parallel-safe, and reads only versions pinned on the handoff. A tombstoned version stays readable for work already pinned to it and is unbindable for anything new.
- Knowledge failure updates only the durable task and never blocks or terminates realtime voice.
- No excerpt, query, citation text, upload byte, customer content, or credential enters logs or metric labels.
- Uploads, normalization, excerpts, results, and deadlines keep exact byte/count bounds; raw upload bytes are never persisted.
- **New:** accepted input is UTF-8 text and Markdown only. Every other format — including PDF and DOCX — is rejected at the format gate with the stable `unsupported_content` code, not with a parse attempt.
- **New:** no module dependency is added by this plan. `go mod tidy -diff` must stay clean.
- External repositories are reference material only; all code is implemented natively here with no symlinks, imports, or runtime linkage.

---

## Prerequisite: verify what already exists

The profile stack (publish → resolve → `live.profile.selected` → capture) has been exercised only against fakes; no run has touched real PostgreSQL. Stacking a new subsystem on an unverified one compounds untested surface.

- [x] **Step 0: Prove the existing profile path end-to-end against disposable PostgreSQL**

Run: `make durable-demo-migrate && make durable-demo-up`

Run: `go test -race -tags=integration ./internal/store/postgres -run 'TestProfile' -count=1`

Then, through `make demo-wizard` → Browser control panel with `LIVE_PROFILE_DATABASE_URL` exported: create a draft, publish and activate it, select it on the voice page, and confirm capture opens only after the exact `live.profile.selected` acknowledgement.

Expected: PASS against real PostgreSQL. A skip is not acceptance.

Done via `deploy/scripts/verify-durable-store.sh`, which shares one disposable
container across the run. All four suites green at `d130f2c`: schema up/down/up,
knowledge store, profile store, and knowledge+profile under `-race`.

---

## File Structure

- `internal/knowledge/types.go` / `store.go` — versioned pack domain and interfaces.
- `internal/knowledge/ingest.go` — format gate, normalization, chunking, citations, preview/publish.
- `internal/knowledge/search.go` — deterministic CJK-aware lexical ranking.
- `internal/knowledge/validator.go` — `profile.KnowledgeReferenceValidator` implementation.
- `internal/adapters/knowledge/executor.go` — worker tool adapter with pinned pack authority.
- `migrations/000010_knowledge_packs.*.sql` — tenant-scoped packs, versions, excerpts, tombstones.
- `internal/store/postgres/knowledge.go` — durable knowledge store.
- `demo/live-client/knowledge_api.go` — loopback upload/preview/publish/tombstone endpoints.
- `demo/live-client/web/knowledge-editor.mjs` — bounded Knowledge tab.

Deliberately absent: `internal/knowledge/extract/*`. See "Deferred".

---

### Task 1: Versioned Knowledge Domain and PostgreSQL Store

Unchanged in substance from the original Task 4. This is the foundation and carries none of the deferred risk.

**Files:**
- Create: `internal/knowledge/types.go`
- Create: `internal/knowledge/types_test.go`
- Create: `internal/knowledge/store.go`
- Create: `migrations/000010_knowledge_packs.up.sql`
- Create: `migrations/000010_knowledge_packs.down.sql`
- Modify: `migrations/embed_test.go`
- Create: `internal/store/postgres/knowledge.go`
- Create: `internal/store/postgres/knowledge_integration_test.go`
- Modify: `internal/store/postgres/schema_integration_test.go`

**Interfaces:**
- Produces: immutable `knowledge.Pack`, `Version`, `Excerpt`, `Citation`, `Store`, and stable errors.
- Consumed by Tasks 2, 3, 4, and 5.

- [x] **Step 1: Write RED domain and repository tests**

```go
func TestKnowledgeVersionIsImmutableTenantScopedAndTombstoned(t *testing.T) {
	repo := migratedRepository(t)
	v := publishKnowledge(t, repo, "tenant-a", "policy", []knowledge.Excerpt{excerpt("coverage")})
	require.NoError(t, repo.Tombstone(t.Context(), "tenant-a", v.Ref, "operator", time.Now()))
	_, ok, err := repo.Resolve(t.Context(), "tenant-b", v.Ref)
	require.NoError(t, err)
	assert.False(t, ok)
}
```

Add exact version allocation, content-hash mismatch, duplicate citation, 2,048 excerpt cap, 4 KiB/excerpt, 8 MiB aggregate normalized bytes, publication transaction rollback, binding-to-tombstone rejection, and retention transition tests.

- [ ] **Step 2: Run tests RED**

Run: `go test ./internal/knowledge ./internal/store/postgres -run 'TestKnowledge' -count=1`

Expected: FAIL because domain, migration, and store are absent.

- [ ] **Step 3: Define bounded domain and migration**

```go
type VersionRef struct { PackID string; Version uint64; Hash string }
type Citation struct { SourceTitle, Section string; Page int; ExcerptHash string }
type Excerpt struct { Ordinal int; Text string; Tokens []string; Citation Citation }
type Store interface {
	CreateDraft(context.Context, Draft) (Draft, error)
	Publish(context.Context, PublishRecord) (Version, error)
	Resolve(context.Context, string, VersionRef) (Version, bool, error)
	Tombstone(context.Context, string, VersionRef, string, time.Time) error
	Search(context.Context, SearchRequest) ([]Excerpt, error)
}
```

`VersionRef` must stay assignment-compatible in shape with `profile.KnowledgeRef{ID, Version, Hash}`; Task 3 converts between them explicitly rather than sharing a type across the package boundary.

Migration creates tenant-scoped packs, versions, and excerpts with foreign keys and checked hashes/counts/bytes. Tombstone metadata is explicit. Raw uploads are not stored.

- [ ] **Step 4: Implement pgx transactions and deterministic ordering**

Insert a version and all excerpts in one transaction; verify aggregate bytes, count, and hash before SQL; resolve by exact tenant/ref/hash; list by stable `(pack_id, version)` cursor; return excerpts ordered by ordinal. Tombstones block new resolve and binding but do not mutate existing excerpts before retention deletion.

- [ ] **Step 5: Run PostgreSQL gates GREEN**

Run: `go test -race ./migrations ./internal/knowledge ./internal/store/postgres -run 'TestKnowledge|TestSchema' -count=3`

Expected: PASS against real disposable PostgreSQL; a skip is not acceptance.

- [ ] **Step 6: Commit**

```bash
git add internal/knowledge migrations/000010_knowledge_packs.up.sql migrations/000010_knowledge_packs.down.sql migrations/embed_test.go internal/store/postgres/knowledge.go internal/store/postgres/knowledge_integration_test.go internal/store/postgres/schema_integration_test.go
git commit -m "feat: persist immutable knowledge packs"
```

---

### Task 2: Text and Markdown Ingestion

Replaces the original Task 5. Same `Ingestor` contract; the extractor subpackage and its two pinned dependencies are removed, and the format gate becomes a closed allowlist.

**Files:**
- Create: `internal/knowledge/ingest.go`
- Create: `internal/knowledge/ingest_test.go`

**Interfaces:**
- Consumes: Task 1 store and types.
- Produces: `knowledge.Ingestor.Validate`, `Preview`, and `Publish`.
- Produces: stable `ErrUnsupportedContent` and `ErrContentLimit`.

- [x] **Step 1: Write RED format-gate and boundary tests**

```go
func TestOnlyTextAndMarkdownAreAccepted(t *testing.T) {
	for _, fixture := range []string{"policy.pdf", "policy.docx", "policy.zip", "policy.rtf"} {
		_, err := ingestor(t).Validate(t.Context(), loadDocument(t, fixture))
		require.ErrorIs(t, err, knowledge.ErrUnsupportedContent)
	}
}

func TestFormatGateRejectsBinaryContentBeforeAnyDecode(t *testing.T) {
	document := documentNamed("notes.md", []byte("%PDF-1.7\n%\xE2\xE3\xCF\xD3"))
	_, err := ingestor(t).Validate(t.Context(), document)
	require.ErrorIs(t, err, knowledge.ErrUnsupportedContent)
}
```

The second test is the one that matters: a truthful extension must not be sufficient. Add signature/extension mismatch, 8 MiB upload+1, invalid UTF-8, NUL and control characters, 4 MiB normalized text+1, 2,048 excerpt+1, deterministic CJK chunking, cancellation, and no-raw-upload-retention tests.

- [ ] **Step 2: Run tests RED**

Run: `go test ./internal/knowledge -run 'TestOnly|TestFormatGate|TestIngest' -count=1`

Expected: FAIL because the ingestor is absent.

- [ ] **Step 3: Implement the closed format gate**

Accept exactly two content classes: `text/plain` and `text/markdown`, each requiring a matching extension *and* content that decodes as strict UTF-8 with no NUL bytes and no C0 control characters other than tab and newline. Reject everything else as `unsupported_content` before any decode or allocation proportional to input. Do not sniff for, name, or special-case PDF/DOCX beyond the rejection — a future extractor registers itself here rather than editing this branch.

- [ ] **Step 4: Implement deterministic normalization, excerpts, and preview/publish**

Normalize Unicode whitespace without changing semantic characters. Split Latin words and CJK rune 1/2-grams. Create ≤4 KiB UTF-8 excerpts with 256-byte overlap at paragraph boundaries. Derive `Citation.Section` from Markdown heading context and leave `Page` zero for text input. Cap 2,048 excerpts and 8 MiB aggregate; hash exact normalized bytes and citations. `Preview` returns bounded excerpts and citations without persisting; `Publish` persists the already-previewed exact hash and discards upload bytes.

- [ ] **Step 5: Run ingestion/fuzz/static gates GREEN**

Run: `go test -race ./internal/knowledge -count=10`

Run: `go test ./internal/knowledge -run=^$ -fuzz=FuzzIngestNeverPanics -fuzztime=20s`

Run: `CGO_ENABLED=0 go test ./internal/knowledge && go mod tidy -diff`

Expected: PASS with no external binary, no OCR, and no new module requirement.

- [ ] **Step 6: Commit**

```bash
git add internal/knowledge/ingest.go internal/knowledge/ingest_test.go
git commit -m "feat: ingest bounded text knowledge"
```

---

### Task 3: Publisher Reference Validator

New task with no counterpart in the original plan. This is the change that actually unblocks knowledge-bearing profiles.

**Files:**
- Create: `internal/knowledge/validator.go`
- Create: `internal/knowledge/validator_test.go`
- Modify: `demo/live-client/profile_service.go`
- Modify: `demo/live-client/profile_api_test.go`

**Interfaces:**
- Produces: `knowledge.NewReferenceValidator(Store) profile.KnowledgeReferenceValidator`.
- Consumed by `profile.NewPublisher`'s fourth argument, currently `nil` in the demo composition.

- [x] **Step 1: Write RED validator and publication tests**

```go
func TestPublicationRequiresEveryKnowledgeRefToResolveExactly(t *testing.T) {
	validator := knowledge.NewReferenceValidator(store)
	published := publishPack(t, store, "tenant-a", "policy")
	require.NoError(t, validator.ValidateReferences(t.Context(), "tenant-a", []profile.KnowledgeRef{
		{ID: published.Ref.PackID, Version: published.Ref.Version, Hash: published.Ref.Hash},
	}))

	for name, ref := range map[string]profile.KnowledgeRef{
		"wrong hash":     {ID: "policy", Version: 1, Hash: strings.Repeat("a", 64)},
		"wrong version":  {ID: "policy", Version: 2, Hash: published.Ref.Hash},
		"unknown pack":   {ID: "absent", Version: 1, Hash: published.Ref.Hash},
		"other tenant":   {ID: "policy", Version: 1, Hash: published.Ref.Hash},
	} {
		t.Run(name, func(t *testing.T) {
			tenant := "tenant-a"
			if name == "other tenant" { tenant = "tenant-b" }
			require.Error(t, validator.ValidateReferences(t.Context(), tenant, []profile.KnowledgeRef{ref}))
		})
	}
}
```

Add: a tombstoned version is unbindable for new publication; duplicate refs to the same pack are rejected; the 32-ref manifest cap is enforced before any store call; and a store outage fails publication closed rather than publishing an unvalidated manifest.

- [ ] **Step 2: Run tests RED**

Run: `go test ./internal/knowledge ./demo/live-client -run 'TestPublication|TestProfile' -count=1`

Expected: FAIL because the validator is absent.

- [ ] **Step 3: Implement the validator**

Resolve each ref by exact tenant, pack ID, version, *and* hash. Reject on any mismatch, on any tombstoned version, on duplicates, and on more than 32 refs. Return only stable classes so `publisher.go` maps them to `ErrProfileIncompatible`; never surface store error text.

- [ ] **Step 4: Compose it in the loopback BFF**

Replace the `nil` fourth argument to `profile.NewPublisher` in `newProfileAdminService` with the real validator when the profile database is configured. Keep the current fail-closed behaviour exactly when it is not: a manifest with knowledge refs and no validator must still be rejected, so a misconfigured deployment cannot publish an unresolvable profile.

- [ ] **Step 5: Run gates GREEN**

Run: `go test -race ./internal/knowledge ./internal/profile ./demo/live-client -count=3`

Expected: PASS, including the existing publisher tests that assert nil-validator rejection.

- [ ] **Step 6: Commit**

```bash
git add internal/knowledge/validator.go internal/knowledge/validator_test.go demo/live-client/profile_service.go demo/live-client/profile_api_test.go
git commit -m "feat: validate pinned knowledge on publication"
```

---

### Task 4: Deterministic Knowledge Search Adapter

Unchanged in substance from the original Task 6.

**Files:**
- Create: `internal/knowledge/search.go`
- Create: `internal/knowledge/search_test.go`
- Create: `internal/adapters/knowledge/executor.go`
- Create: `internal/adapters/knowledge/executor_test.go`
- Create: `internal/adapters/knowledge/manifest.json`
- Modify: `internal/worker/tools.go`
- Modify: `internal/worker/runner_test.go`
- Modify: `cmd/live-worker/main.go`

**Interfaces:**
- Produces: read-only tool `knowledge.search` with arguments `{query:string,max_results?:integer}` and bounded cited results.
- Consumes: exact tenant and knowledge versions from `worker.ToolExecutionRequest.Authority`.

- [x] **Step 1: Write RED ranking and authority tests**

```go
func TestSearchUsesOnlyPinnedVersionsAndStableCJKRanking(t *testing.T) {
	result := search(t, authority("tenant-a", ref("policy", 2)), "保险 等待期", 5)
	require.NotEmpty(t, result)
	assert.Equal(t, "policy", result[0].Citation.PackID)
	assert.Equal(t, uint64(2), result[0].Citation.Version)
}

func TestModelArgumentsCannotSelectTenantOrPack(t *testing.T) {
	_, err := executor(t).Execute(t.Context(), requestWithArgs(`{"query":"coverage","tenant":"other","pack":"secret"}`))
	require.Error(t, err)
}
```

Add 0/513-byte query, control characters, topK 1/10/11, tie-break by pack/version/ordinal, tombstoned-but-pinned accepted work, 64 KiB result cap, cancellation, parallel safety, and no query/content logging.

- [ ] **Step 2: Run tests RED**

Run: `go test ./internal/knowledge ./internal/adapters/knowledge ./internal/worker -run 'TestSearch|TestModelArguments' -count=1`

Expected: FAIL because search and adapter are absent.

- [ ] **Step 3: Implement deterministic lexical ranking**

Tokenize normalized lowercase Latin words and CJK rune 1/2-grams. Score exact phrase, token intersection, and bounded term frequency with integer arithmetic only — no floating point, so ordering is bit-identical across platforms. Sort by score descending, then pack ID, version, and excerpt ordinal. Return top 1–10 results with source title, page/section, excerpt hash, bounded excerpt text, and confidence class `high|medium|low`.

- [ ] **Step 4: Register the read-only adapter with exact authority**

Tenant and pinned versions come from the handoff authority only; model-supplied arguments carrying tenant, pack, or version fields are a hard error, not a silently ignored field. The adapter is parallel-safe and performs no write.

- [ ] **Step 5: Run gates GREEN**

Run: `go test -race ./internal/knowledge ./internal/adapters/knowledge ./internal/worker -count=5`

Expected: PASS, with ranking stable across repeated runs.

- [ ] **Step 6: Commit**

```bash
git add internal/knowledge/search.go internal/knowledge/search_test.go internal/adapters/knowledge internal/worker/tools.go internal/worker/runner_test.go cmd/live-worker/main.go
git commit -m "feat: search pinned knowledge with citations"
```

---

### Task 5: Knowledge Tab in /setup

Fulfils Task 5 of the setup-experience plan, restricted to the formats Task 2 accepts.

**Files:**
- Create: `demo/live-client/knowledge_api.go`
- Create: `demo/live-client/knowledge_api_test.go`
- Create: `demo/live-client/web/knowledge-editor.mjs`
- Create: `demo/live-client/web/knowledge-editor.test.mjs`
- Modify: `demo/live-client/web/setup-profile.mjs`
- Modify: `demo/live-client/web/setup.html`
- Modify: `demo/live-client/web/setup.css`
- Modify: `demo/live-client/web/setup.mjs`

**Interfaces:**
- Consumes: Task 2 ingestor and Task 1 store.
- Produces: `POST .../knowledge` (upload+preview), `POST .../knowledge/{id}/publish`, `GET .../knowledge`, `POST .../knowledge/{id}/{version}/tombstone`, and a `'knowledge'` entry in `PROFILE_TABS`.

- [x] **Step 1: Write RED upload and privacy tests**

```go
func TestKnowledgeUploadRejectsOversizeWrongSignatureAndTrailingMultipart(t *testing.T) {
	for name, upload := range hostileUploads() {
		t.Run(name, func(t *testing.T) {
			response := uploadKnowledge(t, handler, "secret-customer-name.md", upload)
			assert.Equal(t, http.StatusUnprocessableEntity, response.Code)
			assert.NotContains(t, response.Body.String(), "secret-customer-name")
		})
	}
}
```

Assert the filename never round-trips into a response or error body, that excerpt text never appears in any log line, and that the browser cannot supply tenant, actor, pack hash, or excerpt ordinals. Mirror the existing `TestSetupAPIRejectsHostileRequestsBeforeStore` pattern: hostile requests must reach zero store calls.

- [ ] **Step 2: Run tests RED**

Run: `go test ./demo/live-client -run 'TestKnowledge' -count=1 && node --test demo/live-client/web/knowledge-editor.test.mjs`

Expected: FAIL because the endpoints and module are absent.

- [ ] **Step 3: Implement bounded endpoints**

Multipart with one file part and a hard 8 MiB `MaxBytesReader`, same-origin and exact-`Origin` enforcement as the existing profile routes, `If-Match` generation on every draft-mutating call. Preview returns bounded excerpts; publish takes the previously previewed hash; tombstone names an exact version.

- [ ] **Step 4: Implement the Knowledge tab**

Add `'knowledge'` to `PROFILE_TABS` between `'workers'` and `'publish'`. Render excerpts and citations as text nodes only. State the accepted formats in the panel note — "UTF-8 text and Markdown; convert other documents before uploading" — so the restriction is visible rather than discovered through an error.

- [ ] **Step 5: Run UI/API gates GREEN**

Run: `go test -race ./demo/live-client -run 'TestKnowledge|TestProfile' -count=20`

Run: `node --test demo/live-client/web/*.test.mjs`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add demo/live-client/knowledge_api.go demo/live-client/knowledge_api_test.go demo/live-client/web/knowledge-editor.mjs demo/live-client/web/knowledge-editor.test.mjs demo/live-client/web/setup-profile.mjs demo/live-client/web/setup.html demo/live-client/web/setup.css demo/live-client/web/setup.mjs
git commit -m "feat: manage versioned knowledge in setup"
```

---

## Deferred: binary document extraction

Not "later, same way" — later, *differently*.

If PDF and DOCX ingestion is justified by real demand, it lands as a separate short-lived subprocess with no network access, a read-only input pipe, hard `rlimit` memory and CPU caps, and a wall-clock deadline enforced by the parent. It communicates extracted UTF-8 text and citations over stdout and is killed on any limit breach; a parser panic kills a process that owns nothing rather than corrupting one that owns the profile store and a live audio path.

Task 2's format gate is the registration point: an extractor adds a content class there and returns the same normalized text the text path produces. No other file changes. The two dependencies the original plan pinned (`pdfcpu` v0.13.0, `ledongthuc/pdf` at `5959a40`) are not added by this plan and should be re-evaluated against that boundary rather than inherited.

---

## Delta summary

| Original plan | This plan | Rationale |
| --- | --- | --- |
| Task 4: domain + store | Task 1, unchanged | Foundation, no deferred risk |
| Task 5: text/MD/PDF/DOCX extraction, 2 new deps | Task 2: text/MD only, no new deps | Removes untrusted binary parsing from the process |
| — | Task 3: publisher reference validator | The actual unblock; absent from the original plan |
| Task 6: search adapter | Task 4, unchanged | Ranking requirements are unaffected |
| Setup plan Task 5 | Task 5, formats restricted | Depends only on what Tasks 1–3 deliver |
| — | Step 0 prerequisite | Existing profile path is unverified against real PostgreSQL |

Estimated reduction: roughly 8 files and 2 module dependencies removed, one small file added, and the highest-severity failure mode in the original design eliminated rather than bounded.
