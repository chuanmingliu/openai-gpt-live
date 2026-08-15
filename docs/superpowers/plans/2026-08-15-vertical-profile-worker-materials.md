# Vertical Profile Worker, Adapter, and Knowledge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Render immutable named worker recipes, expose only reviewed adapter bindings, provide a safe adapter scaffold, ingest bounded knowledge packs, and execute deterministic cited `knowledge.search` without affecting realtime voice.

**Architecture:** Compile a server-owned adapter catalog into the existing immutable worker registry; profile bindings may only tighten it. Workers resolve the exact durable profile/recipe snapshot and render one objective before reasoning. Knowledge ingestion is a bounded PostgreSQL-backed pipeline, and lexical search is an ordinary read-only adapter whose authority is the exact knowledge versions pinned on the handoff.

**Tech Stack:** Go 1.26, existing worker/reasoner/tool-policy packages, PostgreSQL 16, pgx v5, `pdfcpu` v0.13.0, `ledongthuc/pdf` at revision `5959a40`, standard `archive/zip` and `encoding/xml` for DOCX.

**Spec:** `docs/superpowers/specs/2026-08-15-vertical-profile-prompt-platform-design.md`

## Global Constraints

- Complete the foundation and runtime plans first; every job uses the exact durable profile snapshot from migration 10.
- Worker Recipe means the async worker objective template, not the automatic handoff-decision policy; automatic handoff remains deferred.
- The proposed Redis fast/slow-thinking cache and automatic handoff classifier remain explicitly deferred; this plan adds no Redis dependency, cache authority, or handoff-decision behavior.
- The browser/profile can bind only installed catalog entries and tighten limits; it cannot provide commands, packages, URLs, transports, schemas, secret values, environment names, or raw MCP JSON.
- Side effects retain durable approval, encrypted arguments, stable idempotency, and exact completion semantics; `knowledge.search` is read-only and parallel-safe.
- Worker, adapter, knowledge, or queue failure updates only the durable task and never blocks or terminates realtime voice.
- Uploads, extraction, search, evidence, tool output, and final output have exact byte/count/deadline bounds.
- No prompt, knowledge excerpt, query, tool result, customer content, provider payload, credential, or reasoning enters logs or metric labels.
- External repositories are reference material only; adapter code is implemented natively in this repository with no symlinks, imports, or runtime linkage.

---

## File Structure

- `internal/adaptercatalog/manifest.go` — closed-world manifest and binding validation.
- `internal/adaptercatalog/catalog.go` — immutable registry-versioned catalog.
- `internal/adapters/*` — native reviewed adapter implementations.
- `cmd/live-adapter-scaffold` — bounded code generator for one native adapter skeleton.
- `internal/worker/recipes.go` — exact snapshot resolve and worker objective render.
- `internal/knowledge/types.go` / `store.go` — versioned pack domain and interfaces.
- `internal/knowledge/ingest.go` — validation, normalization, chunking, citations, and publication.
- `internal/knowledge/extract/*` — text/Markdown/PDF/DOCX extractors.
- `internal/knowledge/search.go` — deterministic CJK-aware lexical ranking.
- `internal/adapters/knowledge/executor.go` — worker tool adapter with pinned pack authority.
- `migrations/000011_knowledge_packs.*.sql` — tenant-scoped packs, versions, excerpts, and tombstones.
- `internal/store/postgres/knowledge.go` — durable knowledge store.

---

### Task 1: Closed-World Adapter Catalog and Profile Bindings

**Files:**
- Create: `internal/adaptercatalog/manifest.go`
- Create: `internal/adaptercatalog/manifest_test.go`
- Create: `internal/adaptercatalog/catalog.go`
- Create: `internal/adaptercatalog/catalog_test.go`
- Modify: `internal/worker/tools.go`
- Modify: `internal/worker/tools_test.go`
- Modify: `cmd/live-worker/main.go`
- Modify: `cmd/live-worker/main_test.go`

**Interfaces:**
- Produces: `adaptercatalog.Manifest`, `Binding`, `Catalog`, `New`, `ValidateBinding`, and `CompileRegistry`.
- Produces: exact conversion to existing `worker.RegisteredTool` without browser-controlled executable fields.
- Consumed by Tasks 2, 3, 6 and the `/setup` plan.

- [ ] **Step 1: Write RED manifest and no-loosening tests**

```go
func TestBindingMayTightenButNeverWidenManifest(t *testing.T) {
	manifest := readOnlyManifest("knowledge.search", 5*time.Second, 64<<10, 8)
	_, err := ValidateBinding(manifest, Binding{Enabled: true, Timeout: 6*time.Second, MaxResultBytes: 64 << 10, MaxConcurrency: 8})
	require.ErrorIs(t, err, ErrBindingExpansion)
	got, err := ValidateBinding(manifest, Binding{Enabled: true, Timeout: time.Second, MaxResultBytes: 8 << 10, MaxConcurrency: 2})
	require.NoError(t, err)
	assert.Equal(t, time.Second, got.Timeout)
}

func TestManifestRejectsRuntimeExecutableConfiguration(t *testing.T) {
	_, err := DecodeManifest([]byte(`{"id":"x","command":"npx"}`))
	require.Error(t, err)
}
```

Add duplicate tool IDs, invalid JSON Schema, unknown secret reference, side-effect without approval/idempotency, read-only marked side-effecting, mutable registry, destination mismatch, and result/concurrency/deadline boundaries.

- [ ] **Step 2: Run tests RED**

Run: `go test ./internal/adaptercatalog ./internal/worker ./cmd/live-worker -run 'TestBinding|TestManifest|TestCatalog' -count=1`

Expected: FAIL because the catalog package is absent.

- [ ] **Step 3: Define exact manifest and binding types**

```go
type Manifest struct {
	AdapterID, AdapterVersion, RegistryVersion string
	Tool reasoner.ToolDefinition
	ReadOnly, SideEffecting, ParallelSafe, StableIdempotency bool
	Approval string
	Timeout time.Duration
	MaxConcurrency, MaxArgumentBytes, MaxResultBytes int
	DestinationID, TransportClass string
	SecretRefs []string
	HealthClass string
}
type Binding struct { Enabled bool; Timeout time.Duration; MaxConcurrency, MaxResultBytes int }
```

Decode strict JSON with duplicate-key rejection and exact enums. IDs are 1–256 bytes; schemas are one object, depth ≤32 and ≤64 KiB; arguments/results ≤256 KiB; timeout 100 ms–30 s; concurrency 1–64; secret refs are server manifest metadata only.

- [ ] **Step 4: Compile immutable catalog entries into the worker registry**

`CompileRegistry` takes installed native executors/projectors keyed by exact adapter ID/version, validates identity and metadata equality, and emits cloned `worker.RegisteredTool` values. Extend `RegisteredTool` with adapter identity, timeout/result/concurrency limits, and enforce them in execution. Do not introduce a generic command or arbitrary HTTP executor.

- [ ] **Step 5: Run catalog and worker gates GREEN**

Run: `go test -race ./internal/adaptercatalog ./internal/worker ./cmd/live-worker -run 'TestBinding|TestManifest|TestCatalog|TestTool' -count=10`

Expected: PASS with stable canonical manifest/registry hashes.

- [ ] **Step 6: Commit**

```bash
git add internal/adaptercatalog internal/worker/tools.go internal/worker/tools_test.go cmd/live-worker/main.go cmd/live-worker/main_test.go
git commit -m "feat: compile reviewed adapter manifests"
```

---

### Task 2: Native Adapter Scaffold

**Files:**
- Create: `cmd/live-adapter-scaffold/main.go`
- Create: `cmd/live-adapter-scaffold/main_test.go`
- Create: `cmd/live-adapter-scaffold/templates/adapter.go.tmpl`
- Create: `cmd/live-adapter-scaffold/templates/adapter_test.go.tmpl`
- Create: `cmd/live-adapter-scaffold/templates/manifest.json.tmpl`
- Modify: `Makefile`
- Create: `docs/adapters.md`

**Interfaces:**
- Consumes: Task 1 manifest and worker executor contracts.
- Produces: `make adapter-scaffold NAME=policy_lookup` creating only `internal/adapters/policy_lookup/*`.

- [ ] **Step 1: Write RED path, collision, and output tests**

```go
func TestScaffoldCreatesOneNativeAdapterWithoutExecutableConfig(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, run([]string{"-root", root, "-name", "policy_lookup"}))
	manifest := read(t, filepath.Join(root, "internal/adapters/policy_lookup/manifest.json"))
	assert.NotContains(t, manifest, "command")
	assert.NotContains(t, manifest, "url")
}

func TestScaffoldRejectsHostileTargets(t *testing.T) {
	for _, name := range []string{"../escape", "a/b", "Upper", "trailing-", ""} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, run([]string{"-root", t.TempDir(), "-name", name}))
		})
	}
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "internal/adapters"), 0o700))
	require.NoError(t, os.Symlink(t.TempDir(), filepath.Join(root, "internal/adapters/policy_lookup")))
	require.Error(t, run([]string{"-root", root, "-name", "policy_lookup"}))
}
```

- [ ] **Step 2: Run tests RED**

Run: `go test ./cmd/live-adapter-scaffold -count=1`

Expected: FAIL because the command does not exist.

- [ ] **Step 3: Implement the bounded embedded generator**

Use `//go:embed templates/*`, accept names matching `^[a-z][a-z0-9_]{0,63}$`, canonicalize and inode-check the repository root, require the target not to exist, write files to a private sibling temp directory with `O_EXCL|O_NOFOLLOW`, fsync, then atomically rename. Generated tests include timeout, cancellation, result bound, privacy, and fake executor behavior.

- [ ] **Step 4: Wire the Make target and native implementation guide**

```make
adapter-scaffold:
	go run ./cmd/live-adapter-scaffold -root . -name "$(NAME)"
```

Document manifest review, native registration, MCP pinning rules, secrets by reference, and the prohibition on cross-repo linkage/runtime installation.

- [ ] **Step 5: Run scaffold gates GREEN**

Run: `go test -race ./cmd/live-adapter-scaffold -count=20 && make adapter-scaffold NAME=plan_probe` in a disposable copied test root, then compile the generated package.

Expected: PASS; repository root remains unchanged by the disposable probe.

- [ ] **Step 6: Commit**

```bash
git add cmd/live-adapter-scaffold Makefile docs/adapters.md
git commit -m "feat: scaffold native reviewed adapters"
```

---

### Task 3: Immutable Worker Recipe Rendering

**Files:**
- Create: `internal/worker/recipes.go`
- Create: `internal/worker/recipes_test.go`
- Modify: `internal/worker/runner.go`
- Modify: `internal/worker/runner_test.go`
- Modify: `internal/worker/store.go`
- Modify: `cmd/live-worker/main.go`
- Modify: `cmd/live-worker/main_integration_test.go`

**Interfaces:**
- Consumes: durable `handoff.ProfileSnapshot`, `profile.Resolver`, `prompttmpl.Engine`, and Task 1 adapter catalog.
- Produces: `worker.RecipeResolver.ResolveAndRender(context.Context, handoff.ProfileSnapshot, worker.RecipeEvidence) (RenderedRecipe, error)`.

- [ ] **Step 1: Write RED exact-snapshot and failure-isolation tests**

```go
func TestWorkerRendersExactPinnedRecipeNotActiveVersion(t *testing.T) {
	resolver := profilesWithActiveV2AndPinnedV1()
	got, err := newRecipeResolver(resolver).ResolveAndRender(t.Context(), pinnedV1Handoff(), evidence("renewal"))
	require.NoError(t, err)
	assert.Contains(t, got.Objective, "v1")
	assert.NotContains(t, got.Objective, "v2")
}

func TestRecipeRenderFailureFailsTaskWithoutTouchingVoice(t *testing.T) {
	voice := &countingVoiceGateway{}
	runner := newRunnerWithRecipeResolver(failingRecipeResolver{err: prompttmpl.ErrRenderLimit}, voice)
	err := runner.RunOnce(t.Context(), leasedTask("task-1"))
	require.ErrorIs(t, err, worker.ErrProfileIncompatible)
	assert.Equal(t, 0, voice.SendCalls())
	assert.Equal(t, "render_limit_exceeded", runner.LastFailureClass())
}
```

Add wrong hash/version, revoked-after-accepted still resolves immutable row, evidence schema/byte bounds, stale worker revision, adapter allowlist expansion, and output mutation cases.

- [ ] **Step 2: Run tests RED**

Run: `go test ./internal/worker ./cmd/live-worker -run 'TestWorkerRenders|TestRecipe' -count=1`

Expected: FAIL because the resolver is absent and runner still uses the request objective directly.

- [ ] **Step 3: Implement exact recipe resolve and render**

```go
type RenderedRecipe struct { Objective, System string; ToolAllowlist []string; RegistryVersion, RecipeHash, SnapshotHash string; Limits RecipeLimits }
type RecipeResolver interface { ResolveAndRender(context.Context, handoff.ProfileSnapshot, RecipeEvidence) (RenderedRecipe, error) }
```

Resolve with `profile.ResolveAcceptedWork` by tenant/profile/version, verify every stored hash equals the handoff snapshot, validate cloned evidence against the recipe schema, render only `workers/<recipe-id>.j2`, and intersect recipe bindings with catalog policy. This purpose permits an exact immutable version revoked after task acceptance but never a hash/version mismatch. Keep the fixed worker system instruction separate from untrusted rendered objective/evidence.

- [ ] **Step 4: Integrate before reasoner generation**

At lease start, render once per objective revision and build the reasoner user message from the rendered objective plus canonical evidence. Steering increments the objective revision and rerenders using the same immutable recipe/profile; it cannot change recipe ID, registry, or knowledge versions. Stable error codes are `profile_incompatible`, `template_invalid`, `context_invalid`, and `render_limit_exceeded`.

- [ ] **Step 5: Run worker gates GREEN**

Run: `go test -race ./internal/worker ./cmd/live-worker -run 'TestWorkerRenders|TestRecipe|TestRunner' -count=20`

Expected: PASS; queue/first-token metrics contain timings/classes only.

- [ ] **Step 6: Commit**

```bash
git add internal/worker/recipes.go internal/worker/recipes_test.go internal/worker/runner.go internal/worker/runner_test.go internal/worker/store.go cmd/live-worker/main.go cmd/live-worker/main_integration_test.go
git commit -m "feat: render pinned worker recipes"
```

---

### Task 4: Versioned Knowledge Domain and PostgreSQL Store

**Files:**
- Create: `internal/knowledge/types.go`
- Create: `internal/knowledge/types_test.go`
- Create: `internal/knowledge/store.go`
- Create: `migrations/000011_knowledge_packs.up.sql`
- Create: `migrations/000011_knowledge_packs.down.sql`
- Modify: `migrations/embed_test.go`
- Create: `internal/store/postgres/knowledge.go`
- Create: `internal/store/postgres/knowledge_integration_test.go`
- Modify: `internal/store/postgres/schema_integration_test.go`

**Interfaces:**
- Produces: immutable `knowledge.Pack`, `Version`, `Excerpt`, `Citation`, `Store`, and stable errors.
- Consumed by Tasks 5 and 6 and the `/setup` plan.

- [ ] **Step 1: Write RED domain and repository tests**

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

Add exact version allocation, content-hash mismatch, duplicate citation, 2,048 excerpt cap, 4 KiB/excerpt, 8 MiB aggregate normalized bytes, publication transaction rollback, profile binding to tombstone rejection, and retention transition tests.

- [ ] **Step 2: Run tests RED**

Run: `go test ./internal/knowledge ./internal/store/postgres -run 'TestKnowledge' -count=1`

Expected: FAIL because domain/migration/store are absent.

- [ ] **Step 3: Define bounded domain and migration**

```go
type VersionRef struct { PackID string; Version uint64; Hash string }
type Citation struct { SourceTitle, Section string; Page int; ExcerptHash string }
type Excerpt struct { Ordinal int; Text string; Tokens []string; Citation Citation }
type Store interface { CreateDraft(context.Context, Draft) (Draft, error); Publish(context.Context, PublishRecord) (Version, error); Resolve(context.Context, string, VersionRef) (Version, bool, error); Tombstone(context.Context, string, VersionRef, string, time.Time) error; Search(context.Context, SearchRequest) ([]Excerpt, error) }
```

Migration creates tenant-scoped packs, versions, and excerpts with foreign keys and checked hashes/counts/bytes; tombstone metadata is explicit and raw uploads are not stored.

- [ ] **Step 4: Implement pgx transactions and deterministic ordering**

Insert a version and all excerpts in one transaction, verify aggregate bytes/count/hash before SQL, resolve by exact tenant/ref/hash, list by stable `(pack_id,version)` cursor, and return excerpts ordered by ordinal. Tombstones block new resolve/binding but do not mutate old excerpts before retention deletion.

- [ ] **Step 5: Run PostgreSQL gates GREEN**

Run: `go test -race ./migrations ./internal/knowledge ./internal/store/postgres -run 'TestKnowledge|TestSchema' -count=3`

Expected: PASS against real disposable PostgreSQL; a skip is not acceptance.

- [ ] **Step 6: Commit**

```bash
git add internal/knowledge migrations/000011_knowledge_packs.up.sql migrations/000011_knowledge_packs.down.sql migrations/embed_test.go internal/store/postgres/knowledge.go internal/store/postgres/knowledge_integration_test.go internal/store/postgres/schema_integration_test.go
git commit -m "feat: persist immutable knowledge packs"
```

---

### Task 5: Bounded Document Extraction and Ingestion

**Files:**
- Create: `internal/knowledge/extract/extract.go`
- Create: `internal/knowledge/extract/extract_test.go`
- Create: `internal/knowledge/extract/docx.go`
- Create: `internal/knowledge/extract/docx_test.go`
- Create: `internal/knowledge/extract/pdf.go`
- Create: `internal/knowledge/extract/pdf_test.go`
- Create: `internal/knowledge/ingest.go`
- Create: `internal/knowledge/ingest_test.go`
- Modify: `go.mod`
- Modify: `go.sum`

**Interfaces:**
- Consumes: Task 4 knowledge store/types.
- Produces: `knowledge.Ingestor.Validate`, `Preview`, and `Publish`.
- Produces: `extract.Extractor.Extract(context.Context, Document) (Extracted, error)`.

- [ ] **Step 1: Write RED format and hostile-boundary tests**

```go
func TestEncryptedAndScannedPDFAreUnsupported(t *testing.T) {
	for _, fixture := range []string{"encrypted.pdf", "image-only.pdf"} {
		_, err := extractor(t).Extract(t.Context(), loadDocument(t, fixture))
		require.ErrorIs(t, err, knowledge.ErrUnsupportedContent)
	}
}

func TestDOCXRejectsZipBombTraversalAndExcessEntries(t *testing.T) {
	for _, tc := range []struct {
		name string
		doc Document
	}{
		{"traversal", docxWithEntry("../word/document.xml", []byte("x"))},
		{"absolute", docxWithEntry("/word/document.xml", []byte("x"))},
		{"excess entries", docxWithEntries(257, 1)},
		{"expanded bytes", docxWithEntries(1, 32<<20+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := extractor(t).Extract(t.Context(), tc.doc)
			require.ErrorIs(t, err, knowledge.ErrContentLimit)
		})
	}
}
```

Add signature/extension mismatch, 8 MiB upload+1, 32 MiB expanded+1, 256 zip entries+1, PDF page 500/501, invalid UTF-8, 4 MiB normalized text+1, 2,048 excerpt+1, deterministic CJK chunking, cancellation, and no raw-upload retention tests.

- [ ] **Step 2: Add pinned dependencies and run RED**

Run: `go get github.com/pdfcpu/pdfcpu@v0.13.0 github.com/ledongthuc/pdf@5959a40`

Run: `go test ./internal/knowledge/... -run 'TestEncrypted|TestDOCX|TestIngest' -count=1`

Expected: FAIL because extractors and ingestor are absent.

- [ ] **Step 3: Implement strict text/Markdown/DOCX extraction**

Validate magic bytes before dispatch. Text/Markdown decode strict UTF-8 and normalize CRLF/NUL/control characters. DOCX uses `zip.NewReader`, rejects absolute/dot/symlink paths, caps entries and cumulative uncompressed bytes before reading, and streams `word/document.xml` tokens; only text, paragraph, table-cell, tab, and break semantics are retained.

- [ ] **Step 4: Implement validated PDF extraction**

Use `pdfcpu/pkg/api.Validate` with a no-disk configuration to validate structure and reject encrypted/password-protected input; use the pinned pure-Go PDF reader to extract page text in order under the page/text/time limits. Empty or image-only output is `unsupported_content`. Parser panics are contained and mapped to the same class; no temporary file survives the request.

- [ ] **Step 5: Implement deterministic normalization, excerpts, and preview/publish**

Normalize Unicode whitespace without changing semantic characters, split Latin words and CJK rune 1/2-grams, create ≤4 KiB UTF-8 excerpts with 256-byte overlap at paragraph boundaries, cap 2,048 excerpts/8 MiB aggregate, and hash exact normalized bytes/citations. `Preview` returns bounded excerpts/citations; `Publish` persists the already previewed exact hash and discards upload bytes.

- [ ] **Step 6: Run extraction/fuzz/static gates GREEN**

Run: `go test -race ./internal/knowledge/... -count=10`

Run: `go test ./internal/knowledge/extract -run=^$ -fuzz=FuzzDocumentExtractionNeverPanics -fuzztime=20s`

Run: `CGO_ENABLED=0 go test ./internal/knowledge/... && go mod tidy -diff`

Expected: PASS with no external binary or OCR invocation.

- [ ] **Step 7: Commit**

```bash
git add go.mod go.sum internal/knowledge/extract internal/knowledge/ingest.go internal/knowledge/ingest_test.go
git commit -m "feat: ingest bounded text knowledge"
```

---

### Task 6: Deterministic Knowledge Search Adapter

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

- [ ] **Step 1: Write RED ranking and authority tests**

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

Add 0/513-byte query, control chars, topK 1/10/11, tie-break by pack/version/ordinal, tombstoned-but-pinned accepted work, 64 KiB result cap, cancellation, parallel safety, and no query/content logs.

- [ ] **Step 2: Run tests RED**

Run: `go test ./internal/knowledge ./internal/adapters/knowledge ./internal/worker -run 'TestSearch|TestModelArguments' -count=1`

Expected: FAIL because search and adapter are absent.

- [ ] **Step 3: Implement deterministic lexical ranking**

Tokenize normalized lowercase Latin words and CJK rune 1/2-grams. Score exact phrase, token intersection, and bounded term frequency with integer arithmetic only. Sort by score descending, then pack ID, version, and excerpt ordinal. Return top 1–10 results with source title, page/section, excerpt hash, bounded excerpt text, and confidence class `high|medium|low`.

- [ ] **Step 4: Register the read-only adapter with exact authority**

Extend `ToolExecutionRequest` with cloned `ExecutionAuthority{TenantID, ProfileSnapshotHash, Knowledge []knowledge.VersionRef}` populated by the runner, never model arguments. Register manifest as read-only, parallel-safe, timeout 5 s, max result 64 KiB, max concurrency 8, no secrets, fixed destination `postgres-knowledge-v1`.

- [ ] **Step 5: Run worker/search gates GREEN**

Run: `go test -race ./internal/knowledge ./internal/adapters/knowledge ./internal/worker ./cmd/live-worker -run 'TestSearch|TestKnowledge|TestTool' -count=20`

Expected: PASS with stable JSON/citation ordering.

- [ ] **Step 6: Commit**

```bash
git add internal/knowledge/search.go internal/knowledge/search_test.go internal/adapters/knowledge internal/worker/tools.go internal/worker/runner_test.go cmd/live-worker/main.go
git commit -m "feat: search pinned knowledge with citations"
```

---

### Task 7: Worker and Materials Vertical Integration

**Files:**
- Create: `test/integration/vertical_profile_worker_test.go`
- Modify: `cmd/live-worker/main_integration_test.go`
- Modify: `internal/gateway/handoffs_test.go`
- Modify: `docs/operations.md`
- Modify: `docs/security.md`
- Modify: `docs/adapters.md`

**Interfaces:**
- Consumes: Tasks 1–6 and runtime handoff snapshot.
- Produces: tested insurance recipe → `knowledge.search` → cited completion path.

- [ ] **Step 1: Write the RED end-to-end worker test**

```go
func TestInsuranceRecipeUsesPinnedKnowledgeAndFailureDoesNotEndVoice(t *testing.T) {
	env := newVerticalWorkerEnvironment(t)
	pinned := env.PublishInsuranceProfile(1, env.PublishKnowledge("policy", 2))
	first := env.AcceptHandoff(pinned, "policy_followup")
	env.PublishInsuranceProfile(2, env.PublishKnowledge("policy", 3))
	result := env.RunWorker(first)
	require.Equal(t, pinned.SnapshotHash, result.ProfileSnapshotHash)
	require.Equal(t, pinned.Knowledge[0].Hash, result.Citations[0].PackHash)
	env.FailKnowledgeSearch()
	second := env.AcceptHandoff(pinned, "policy_followup")
	require.ErrorIs(t, env.RunWorkerError(second), worker.ErrKnowledgeUnavailable)
	assert.True(t, env.VoiceSessionAlive())
}
```

Add side-effect approval unchanged, explicit cancellation, steering same recipe, registry mismatch, knowledge unavailable, and content-free telemetry assertions.

- [ ] **Step 2: Run integration RED**

Run: `go test ./test/integration ./cmd/live-worker -run 'TestInsuranceRecipe|TestVerticalProfile' -count=1`

Expected: FAIL until full worker composition and fixtures are connected.

- [ ] **Step 3: Complete composition and operator documentation**

Wire profile resolver, prompt engine/cache, adapter catalog, knowledge store, and runner authority in `cmd/live-worker`. Readiness fails when enabled schema/catalog/engine versions mismatch. Document adapter review, knowledge limits, tombstone/retention, and failure isolation without promising OCR/vector/Redis behavior.

- [ ] **Step 4: Run final worker/material gates GREEN**

Run: `go test -race ./internal/adaptercatalog ./internal/knowledge/... ./internal/adapters/... ./internal/worker ./cmd/live-worker ./test/integration -count=3`

Run: `go test ./... -count=1 && go vet ./... && go build ./... && go mod tidy -diff && git diff --check`

Expected: PASS with real PostgreSQL integration; provider-live behavior remains manual evidence.

- [ ] **Step 5: Commit**

```bash
git add test/integration/vertical_profile_worker_test.go cmd/live-worker/main_integration_test.go internal/gateway/handoffs_test.go docs/operations.md docs/security.md docs/adapters.md
git commit -m "test: verify vertical profile worker materials"
```

---

## Worker and Materials Acceptance Gate

Acceptance requires immutable recipe rendering, catalog no-loosening, native scaffold safety, real PostgreSQL knowledge versioning, hostile PDF/DOCX fixtures, exact lexical/citation results, worker failure isolation, approval/idempotency regression gates, CGO-disabled builds, privacy scans, and independent review.
