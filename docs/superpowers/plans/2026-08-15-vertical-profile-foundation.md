# Vertical Profile Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the deterministic native-Go template engine, immutable profile domain, PostgreSQL draft/publication store, and built-in fallback profile required by every later vertical-profile slice.

**Architecture:** Put all Jinja-like behavior behind `prompttmpl.Engine`, backed by the pinned native Go MiniJinja port and a stricter repository-owned source policy. Store mutable drafts and immutable published manifests in PostgreSQL; publication compiles fixtures and hashes a canonical manifest in one transaction, while the built-in voice profile works without PostgreSQL.

**Tech Stack:** Go 1.26, `github.com/mitsuhiko/minijinja/minijinja-go/v2` v2.24.0, PostgreSQL 16, pgx v5, embedded SQL migrations, testify.

**Spec:** `docs/superpowers/specs/2026-08-15-vertical-profile-prompt-platform-design.md`

## Global Constraints

- Runtime has no Python interpreter, Jinja2 package, filesystem template loader, dynamic package installation, or CGO dependency.
- `Jinja-Go v1` accepts only the documented deterministic subset; strict undefined, fuel, recursion, include depth, input bytes, collection counts, and UTF-8 output limits are mandatory.
- Templates receive cloned scalar/map/list data only; no application objects, methods, credentials, handles, provider payloads, or arbitrary metadata.
- PostgreSQL is authoritative for custom drafts and published versions; hashes identify content but never authorize it.
- Every stored record and query is tenant-scoped; fixtures are synthetic and content-free logs never expose prompt source or output.
- The checked-in voice-only profile remains usable when PostgreSQL and vertical profiles are disabled.
- Preserve all pre-existing dirty browser/audio files; stage only paths named by the current task.

---

## File Structure

- `internal/prompttmpl/engine.go` — engine-neutral compile/render contracts, limits, error classes, and capped writer.
- `internal/prompttmpl/policy.go` — conservative Jinja-Go v1 lexical/source policy.
- `internal/prompttmpl/minijinja.go` — the only MiniJinja dependency adapter.
- `internal/prompttmpl/context.go` — context schema and cloned, bounded value validation.
- `internal/prompttmpl/cache.go` — bounded content-hash compiled-bundle cache.
- `internal/prompttmpl/testdata/jinja_go_v1.json` — sanitized compatibility/security corpus derived from the supplied demonstration.
- `internal/profile/types.go` — canonical domain types and immutable snapshot contracts.
- `internal/profile/store.go` — storage interfaces and stable domain errors.
- `internal/profile/publisher.go` — validation, fixture rendering, canonical hashes, publication, activation, and revocation.
- `internal/profile/builtin.go` / `internal/profile/builtin/*` — embedded voice-only profile.
- `migrations/000009_vertical_profiles.*.sql` — tenant-scoped blobs, drafts, and versions.
- `internal/store/postgres/profiles.go` — pgx implementation of `profile.Store`.

---

### Task 1: Jinja-Go v1 Contract and Compatibility Corpus

**Files:**
- Create: `internal/prompttmpl/engine.go`
- Create: `internal/prompttmpl/policy.go`
- Create: `internal/prompttmpl/engine_test.go`
- Create: `internal/prompttmpl/policy_test.go`
- Create: `internal/prompttmpl/testdata/jinja_go_v1.json`

**Interfaces:**
- Produces: `prompttmpl.Engine`, `CompiledBundle`, `Bundle`, `Context`, `Limits`, `CompileError`, and `RenderError`.
- Produces: `ValidateSource(name, source string, bundleNames []string) error` and `ValidateBundleSources(map[string]string) error`.
- Consumed by Tasks 2, 3, 5 and all later plan files.

- [ ] **Step 1: Write the failing contract and corpus tests**

```go
func TestJinjaGoV1Corpus(t *testing.T) {
	for _, tc := range loadCorpus(t) {
		err := ValidateBundleSources(tc.Templates)
		if tc.Accepted { require.NoError(t, err, tc.Name) } else { require.Error(t, err, tc.Name) }
	}
}

func TestCappedWriterRejectsLimitPlusOneWithoutPartialSuccess(t *testing.T) {
	var dst bytes.Buffer
	w := NewCappedWriter(&dst, 4)
	_, err := w.Write([]byte("12345"))
	require.ErrorIs(t, err, ErrRenderLimit)
}
```

The JSON corpus must include variables, maps, lists, indexing, `if`, `for`, deterministic `set`, `is defined`, `none`, comparisons, whitespace control, Unicode, macros, and quoted static includes. It must reject `.append`, `.update`, dotted calls, undeclared calls, `random`, dynamic includes, imports outside the bundle, recursive include cycles, filesystem-like names, and the credential/logging portions of the reference demo.

- [ ] **Step 2: Run the focused tests RED**

Run: `go test ./internal/prompttmpl -run 'TestJinjaGoV1Corpus|TestCappedWriter' -count=1`

Expected: FAIL because `prompttmpl` and the corpus do not exist.

- [ ] **Step 3: Define the engine-neutral API and exact limits**

```go
type Limits struct {
	TemplateBytes, BundleBytes, ContextBytes, OutputBytes int
	CollectionItems, IncludeDepth, RecursionDepth int
	Fuel uint64
	RenderTimeout time.Duration
}

type Bundle struct { Templates map[string]string; Schema ContextSchema }
type Context map[string]any

type CompiledBundle interface {
	Render(context.Context, string, Context) (string, error)
	Hash() string
	EngineVersion() string
}

type Engine interface {
	Compile(context.Context, Bundle, Limits) (CompiledBundle, error)
}
```

Set defaults and maxima in constants: 64 KiB/template, 512 KiB/bundle, 256 KiB/context, 64 KiB/output, 1,024 collection items, include depth 8, recursion depth 32, fuel 1,000,000, and render timeout 250 ms. `Limits.Validate` rejects zero-after-defaulting, negative values, and any value above these maxima.

- [ ] **Step 4: Implement the conservative source policy**

```go
func ValidateSource(name, source string, bundleNames []string) error {
	if err := validateLogicalName(name); err != nil { return err }
	tokens, err := lexSource(source)
	if err != nil { return normalizePolicyError(name, err) }
	policy := newSourcePolicy(name, bundleNames)
	for _, token := range tokens {
		if err := policy.Accept(token); err != nil { return normalizePolicyError(name, err) }
	}
	return nil
}

func ValidateBundleSources(templates map[string]string) error {
	graph := newIncludeGraph(len(templates))
	names := sortedTemplateNames(templates)
	for name, source := range templates {
		meta, err := inspectSource(name, source, names)
		if err != nil { return err }
		graph.Add(name, meta.StaticDependencies)
	}
	return graph.ValidateAcyclic(8)
}
```

Use a small delimiter-aware lexer, not regular expressions alone. It must ignore delimiters inside quoted strings, bound token count to source bytes, normalize logical names to `[a-z0-9][a-z0-9_./-]*\.j2`, and return only `template_invalid` plus template name/line/column—never source text.

- [ ] **Step 5: Run the contract suite GREEN and fuzz the lexer**

Run: `go test -race ./internal/prompttmpl -count=1`

Add `FuzzValidateSourceNeverPanics` and run: `go test ./internal/prompttmpl -run=^$ -fuzz=FuzzValidateSourceNeverPanics -fuzztime=10s`

Expected: PASS with no panic, hang, or unbounded allocation.

- [ ] **Step 6: Commit**

```bash
git add internal/prompttmpl/engine.go internal/prompttmpl/policy.go internal/prompttmpl/engine_test.go internal/prompttmpl/policy_test.go internal/prompttmpl/testdata/jinja_go_v1.json
git commit -m "feat: define bounded Jinja-Go template contract"
```

---

### Task 2: Native MiniJinja Adapter and Bounded Cache

**Files:**
- Create: `internal/prompttmpl/minijinja.go`
- Create: `internal/prompttmpl/minijinja_test.go`
- Create: `internal/prompttmpl/cache.go`
- Create: `internal/prompttmpl/cache_test.go`
- Modify: `go.mod`
- Modify: `go.sum`

**Interfaces:**
- Consumes: Task 1 `Engine`, `Bundle`, `Limits`, `ValidateSource`, `ValidateBundleSources`, and capped writer.
- Produces: `prompttmpl.NewMiniJinja() Engine` and `prompttmpl.NewCache(engine Engine, entries, bytes int) (*Cache, error)`.
- Consumed by Tasks 3 and 5.

- [ ] **Step 1: Add RED tests for strict rendering and hostile templates**

```go
func TestMiniJinjaStrictStaticBundle(t *testing.T) {
	compiled := compile(t, Bundle{Templates: map[string]string{
		"system.j2": `{% include "fragments/rules.j2" %} {{ customer.name }}`,
		"fragments/rules.j2": `Use approved facts only.`,
	}})
	got, err := compiled.Render(t.Context(), "system.j2", Context{"customer": map[string]any{"name": "李明"}})
	require.NoError(t, err)
	assert.Equal(t, "Use approved facts only. 李明", got)
}

func TestMiniJinjaRejectsUndefinedAndOutOfFuel(t *testing.T) {
	_, err := render(t, `{{ missing }}`, Context{})
	require.Error(t, err)
	_, err = renderWithFuel(t, `{% for x in values %}{{ x }}{% endfor %}`, hugeValues(), 8)
	require.ErrorIs(t, err, ErrRenderLimit)
}
```

Add cancellation, recursion, output limit, macro-only call, dynamic include, method/reflection, concurrent render, and cache eviction/byte-accounting cases.

- [ ] **Step 2: Add the exact dependency and verify RED**

Run: `go get github.com/mitsuhiko/minijinja/minijinja-go/v2@v2.24.0`

Run: `go test ./internal/prompttmpl -run 'TestMiniJinja|TestCache' -count=1`

Expected: FAIL because the adapter and cache are absent.

- [ ] **Step 3: Implement the empty-environment adapter**

```go
env := minijinja.EmptyEnvironment()
env.SetUndefinedBehavior(minijinja.UndefinedStrict)
env.SetRecursionLimit(limits.RecursionDepth)
fuel := limits.Fuel
env.SetFuel(&fuel)
env.SetLoader(func(name string) (string, error) {
	source, ok := ownedTemplates[name]
	if !ok { return "", minijinja.NewError(minijinja.ErrTemplateNotFound, "template unavailable") }
	return source, nil
})
```

Register only reviewed pure filters (`lower`, `upper`, `trim`, `length`, `join`, `default`) and the `defined` test. Do not call `NewEnvironment`, `AddFunction`, or expose globals. Render with `RenderToWriteCtx` into the capped writer under a child context with `RenderTimeout`; normalize dependency errors to stable repository error classes without source or rendered bytes.

- [ ] **Step 4: Implement the immutable bounded cache**

```go
type Cache struct {
	engine Engine
	maxEntries, maxBytes int
	mu sync.Mutex
	lru *list.List
	byKey map[string]*list.Element
	inflight map[string]*compileCall
	residentBytes int
}

func (c *Cache) Compile(ctx context.Context, bundle Bundle, limits Limits) (CompiledBundle, error)
```

Clone source/schema on admission, account source plus compiled estimate bytes, deduplicate concurrent misses with one in-flight entry, never cache failures, and evict least-recently-used entries until both limits fit. A cache loss must only cause recompilation.

- [ ] **Step 5: Run focused and module gates GREEN**

Run: `go test -race ./internal/prompttmpl -count=10`

Run: `go mod tidy -diff && go vet ./internal/prompttmpl`

Expected: PASS; `CGO_ENABLED=0 go test ./internal/prompttmpl` must also pass.

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum internal/prompttmpl/minijinja.go internal/prompttmpl/minijinja_test.go internal/prompttmpl/cache.go internal/prompttmpl/cache_test.go
git commit -m "feat: render bounded prompts with native MiniJinja"
```

---

### Task 3: Typed Context Schema and Canonical Profile Domain

**Files:**
- Create: `internal/prompttmpl/context.go`
- Create: `internal/prompttmpl/context_test.go`
- Create: `internal/profile/types.go`
- Create: `internal/profile/types_test.go`
- Create: `internal/profile/store.go`

**Interfaces:**
- Produces: `prompttmpl.ValidateContext(ContextSchema, Context, templateName string) (Context, error)`.
- Produces: `profile.ID`, `VersionRef`, `Blob`, `Draft`, `Published`, `Manifest`, `PromptBundle`, `WorkerRecipe`, `AdapterBinding`, `KnowledgeRef`, `Snapshot`, and `Resolved`.
- Produces: `profile.Store` methods used by Tasks 4 and 5.

- [ ] **Step 1: Write RED schema, cloning, and canonical hash tests**

```go
func TestValidateContextRejectsUndeclaredAndOversizedValues(t *testing.T) {
	schema := ContextSchema{Fields: map[string]Field{"customer_name": {Type: TypeString, Required: true, MaxBytes: 64, Templates: []string{"system.j2"}}}}
	_, err := ValidateContext(schema, Context{"customer_name": "Li", "secret": "x"}, "system.j2")
	require.ErrorIs(t, err, ErrContextInvalid)
}

func TestManifestHashIgnoresMapInsertionOrder(t *testing.T) {
	a := validManifest(map[string]TemplateRef{"system.j2": {Hash: strings.Repeat("a", 64)}})
	b := validManifest(map[string]TemplateRef{"system.j2": {Hash: strings.Repeat("a", 64)}})
	assert.Equal(t, mustHash(t, a), mustHash(t, b))
}
```

Add null/required behavior, template ACLs, invalid UTF-8, depth/count/aggregate bytes, mutation-after-validation, ID/version bounds, duplicate bindings, unknown logical template, and exact hash tests.

- [ ] **Step 2: Run tests RED**

Run: `go test ./internal/prompttmpl ./internal/profile -run 'TestValidateContext|TestManifest' -count=1`

Expected: FAIL because the context validator and profile package are absent.

- [ ] **Step 3: Implement schema validation and cloning**

```go
type FieldType string
const ( TypeString FieldType = "string"; TypeBool FieldType = "bool"; TypeNumber FieldType = "number"; TypeObject FieldType = "object"; TypeArray FieldType = "array" )
type Field struct { Type FieldType; Required bool; MaxBytes, MaxItems int; Templates []string; Sensitivity string; Source string }
type ContextSchema struct { Fields map[string]Field }
```

Walk only `nil`, string, bool, `json.Number`, `map[string]any`, and `[]any`; reject Go structs, pointers, floats with NaN/Inf, cyclic values, excess depth 16, or aggregate bytes above engine limits. Return an owned tree.

- [ ] **Step 4: Define immutable profile types and store contract**

```go
type Store interface {
	PutBlob(context.Context, string, Blob) error
	LoadBlob(context.Context, string, string) (Blob, bool, error)
	LoadDraft(context.Context, string, ID) (Draft, bool, error)
	SaveDraft(context.Context, Draft, int64) (Draft, error)
	SaveTemplate(context.Context, string, ID, string, []byte, int64, string) (Draft, error)
	DeleteTemplate(context.Context, string, ID, string, int64, string) (Draft, error)
	Publish(context.Context, PublishRecord) (Published, error)
	Resolve(context.Context, string, ID, uint64) (Published, bool, error)
	ListPublished(context.Context, string, int, string) ([]PublishedSummary, string, error)
	Activate(context.Context, string, ID, uint64, string) error
	Revoke(context.Context, string, ID, uint64, string, time.Time) error
}
```

`Blob` contains media type, exact owned bytes, byte count, and lowercase SHA-256 hash; `PutBlob` recomputes the hash and is idempotent only for identical tenant-scoped content. `SaveTemplate` and `DeleteTemplate` validate the logical path and perform blob/ref plus optimistic draft-generation mutation in one transaction, preventing orphaned manifest references. `Snapshot` contains tenant, profile ID/version, engine version, bundle hash, system/opening/turn refs, cloned recipes/bindings/knowledge refs, adapter-registry version, and aggregate snapshot hash. `Resolved` contains that `Snapshot` plus the owned `prompttmpl.Bundle` loaded from those exact blobs; callers never reopen files or select a different blob. Validation rejects more than 64 templates, 32 recipes, 64 bindings, 32 knowledge refs, 256-byte IDs, and non-lowercase SHA-256 hashes.

- [ ] **Step 5: Run package gates GREEN**

Run: `go test -race ./internal/prompttmpl ./internal/profile -count=10`

Expected: PASS with mutation tests proving returned values are owned.

- [ ] **Step 6: Commit**

```bash
git add internal/prompttmpl/context.go internal/prompttmpl/context_test.go internal/profile/types.go internal/profile/types_test.go internal/profile/store.go
git commit -m "feat: define immutable vertical profile domain"
```

---

### Task 4: PostgreSQL Draft and Published Profile Store

**Files:**
- Create: `migrations/000009_vertical_profiles.up.sql`
- Create: `migrations/000009_vertical_profiles.down.sql`
- Modify: `migrations/embed_test.go`
- Create: `internal/store/postgres/profiles.go`
- Create: `internal/store/postgres/profiles_integration_test.go`
- Modify: `internal/store/postgres/schema_integration_test.go`

**Interfaces:**
- Consumes: Task 3 `profile.Store` and domain types.
- Produces: `postgres.Repository` implementation of `profile.Store`.
- Consumed by Task 5 and every later plan.

- [ ] **Step 1: Write migration and repository RED integration tests**

```go
func TestProfileDraftSaveUsesExactGeneration(t *testing.T) {
	repo := migratedRepository(t)
	first := saveDraft(t, repo, draft("tenant-a", "insurance-sales"), 0)
	_, err := repo.SaveDraft(t.Context(), firstWithDescription(first, "stale"), 0)
	require.ErrorIs(t, err, profile.ErrStaleDraft)
}

func TestProfilePublishIsTenantScopedAndImmutable(t *testing.T) {
	repo := migratedRepository(t)
	published := publishRecord(t, repo, "tenant-a")
	_, ok, err := repo.Resolve(t.Context(), "tenant-b", published.Ref.ID, published.Ref.Version)
	require.NoError(t, err)
	assert.False(t, ok)
}
```

Add concurrent save, atomic template save/delete plus generation conflict, exact version allocation, transaction rollback, one-active-version partial unique index, revocation, cursor pagination, blob hash/content mismatch, and down/up migration cases.

- [ ] **Step 2: Run PostgreSQL tests RED**

Run: `go test ./internal/store/postgres -run 'TestProfile|TestSchema' -count=1`

Expected: FAIL because migration 9 and repository methods do not exist.

- [ ] **Step 3: Add the tenant-scoped schema**

```sql
CREATE TABLE profile_blobs (
  tenant_id text NOT NULL,
  content_hash text NOT NULL CHECK (content_hash ~ '^[0-9a-f]{64}$'),
  media_type text NOT NULL,
  content bytea NOT NULL,
  content_bytes integer NOT NULL CHECK (content_bytes > 0 AND content_bytes <= 524288),
  created_at timestamptz NOT NULL,
  PRIMARY KEY (tenant_id, content_hash)
);
CREATE TABLE profile_drafts (
  tenant_id text NOT NULL, profile_id text NOT NULL, generation bigint NOT NULL,
  manifest jsonb NOT NULL, updated_at timestamptz NOT NULL, updated_by text NOT NULL,
  PRIMARY KEY (tenant_id, profile_id)
);
CREATE TABLE profile_versions (
  tenant_id text NOT NULL, profile_id text NOT NULL, version bigint NOT NULL,
  draft_generation bigint NOT NULL, engine_version text NOT NULL,
  snapshot_hash text NOT NULL, manifest jsonb NOT NULL, active boolean NOT NULL DEFAULT false,
  revoked_at timestamptz, published_at timestamptz NOT NULL, published_by text NOT NULL,
  PRIMARY KEY (tenant_id, profile_id, version)
);
CREATE UNIQUE INDEX profile_one_active_version ON profile_versions (tenant_id, profile_id) WHERE active;
```

The down migration drops only these objects in reverse dependency order.

- [ ] **Step 4: Implement exact optimistic concurrency and publication transaction**

Use `UPDATE ... WHERE generation=$expected RETURNING generation` for existing drafts and an insert-only generation 1 path for expected generation 0. Lock the draft row `FOR UPDATE`, allocate `MAX(version)+1` under the same transaction, insert immutable JSON/hashes, and update active flags atomically. Map no-row generation mismatch to `profile.ErrStaleDraft`, unique conflicts to `profile.ErrPublicationConflict`, and never return raw SQL text.

- [ ] **Step 5: Run migration, integration, and race gates GREEN**

Run: `go test -race ./migrations ./internal/store/postgres -run 'TestProfile|TestSchema|TestEmbedded' -count=1`

Expected: PASS against disposable PostgreSQL. If Docker/PostgreSQL is unavailable, record the external blocker; do not treat skipped integration tests as acceptance.

- [ ] **Step 6: Commit**

```bash
git add migrations/000009_vertical_profiles.up.sql migrations/000009_vertical_profiles.down.sql migrations/embed_test.go internal/store/postgres/profiles.go internal/store/postgres/profiles_integration_test.go internal/store/postgres/schema_integration_test.go
git commit -m "feat: persist versioned vertical profiles"
```

---

### Task 5: Publisher, Resolver, and Built-In Voice Profile

**Files:**
- Create: `internal/profile/publisher.go`
- Create: `internal/profile/publisher_test.go`
- Create: `internal/profile/resolver.go`
- Create: `internal/profile/resolver_test.go`
- Create: `internal/profile/builtin.go`
- Create: `internal/profile/builtin/system.j2`
- Create: `internal/profile/builtin/context.schema.json`
- Create: `internal/profile/builtin/fixture.json`
- Create: `internal/profile/builtin_test.go`

**Interfaces:**
- Consumes: Tasks 1–4 engine, cache, schema, domain, and store.
- Produces: `profile.Publisher.Validate`, `Preview`, `Publish`, `Activate`, `Revoke`.
- Produces: `profile.Resolver.Resolve(ctx, tenantID string, ref VersionRef, purpose ResolvePurpose) (Resolved, error)` and `profile.Builtin() Resolved`.
- Consumed by the runtime, worker/materials, and `/setup` plan files.

- [ ] **Step 1: Write RED publication and built-in tests**

```go
func TestPublishCompilesEveryTemplateAndFixtureBeforeStoreMutation(t *testing.T) {
	store := &fakeStore{draft: invalidFixtureDraft()}
	_, err := NewPublisher(store, testEngine(), testCatalog(), emptyKnowledgeValidator()).Publish(t.Context(), PublishCommand{TenantID: "tenant", ProfileID: "insurance", ExpectedGeneration: 3, Actor: "operator"})
	require.ErrorIs(t, err, prompttmpl.ErrTemplateInvalid)
	assert.Zero(t, store.publishCalls)
}

func TestBuiltInProfileRequiresNoStoreAndHasStableHash(t *testing.T) {
	a, b := Builtin(), Builtin()
	require.NoError(t, a.Snapshot.Validate())
	assert.Equal(t, a.Snapshot.SnapshotHash, b.Snapshot.SnapshotHash)
}
```

Add incompatible per-turn instructions, missing adapter registry, revoked resolve, active-version swap, failed publish leaves active unchanged, fixture privacy, and canonical hash cases.

- [ ] **Step 2: Run tests RED**

Run: `go test ./internal/profile -run 'TestPublish|TestBuiltIn|TestResolve' -count=1`

Expected: FAIL because publisher/resolver/built-in composition is absent.

- [ ] **Step 3: Implement validation, preview, and atomic publication input**

```go
type Publisher struct { Store Store; Engine prompttmpl.Engine; Catalog AdapterCatalog; Knowledge KnowledgeReferenceValidator }
type PublishCommand struct { TenantID string; ProfileID ID; ExpectedGeneration int64; Actor string; Activate bool }
type PreviewResult struct { Outputs map[string]string; BundleHash, SnapshotHash string }

type AdapterCatalog interface {
	ValidateBindings(context.Context, string, []AdapterBinding) error
	Version() string
}

type KnowledgeReferenceValidator interface {
	ValidateReferences(context.Context, string, []KnowledgeRef) error
}
```

Load the exact draft, validate all referenced blob hashes, compile once, render every synthetic fixture, verify adapter capabilities and knowledge references through the two interfaces, canonicalize the manifest, then call `Store.Publish` with those exact bytes/hashes. Until the worker/materials plan installs a knowledge validator, the default validator accepts an empty list only and returns `profile_incompatible` for non-empty references. Preview uses the same validation path but performs no mutation.

- [ ] **Step 4: Embed the minimal built-in profile and resolver fallback**

`system.j2` must preserve the current concise server-owned behavior and reserved handoff-tool policy; it has no opening or per-turn template, no custom worker recipe, and no knowledge. Define the purpose-aware resolver exactly:

```go
type ResolvePurpose string

const (
	ResolveNewSession ResolvePurpose = "new_session"
	ResolveReconnect ResolvePurpose = "reconnect"
	ResolveAcceptedWork ResolvePurpose = "accepted_work"
)

type Resolved struct {
	Snapshot Snapshot
	Bundle prompttmpl.Bundle
}

type Resolver interface {
	Resolve(context.Context, string, VersionRef, ResolvePurpose) (Resolved, error)
}
```

`Resolve` accepts `builtin/1` without a store and otherwise requires a configured store. New-session and reconnect purposes reject inactive, revoked, or mismatched-engine versions. Accepted durable work may resolve an exact immutable version revoked after acceptance, but must still verify tenant, version, engine, bundle, and snapshot hashes. Every return value owns its bundle/context maps and slices.

- [ ] **Step 5: Run the complete foundation gate GREEN**

Run: `go test -race ./internal/prompttmpl ./internal/profile ./internal/store/postgres ./migrations -count=1`

Run: `go vet ./internal/prompttmpl ./internal/profile ./internal/store/postgres && go mod tidy -diff && git diff --check`

Expected: PASS, with real PostgreSQL integration evidence when available.

- [ ] **Step 6: Commit**

```bash
git add internal/profile/publisher.go internal/profile/publisher_test.go internal/profile/resolver.go internal/profile/resolver_test.go internal/profile/builtin.go internal/profile/builtin internal/profile/builtin_test.go
git commit -m "feat: publish immutable vertical profile snapshots"
```

---

## Foundation Acceptance Gate

Run:

```bash
go test -race ./internal/prompttmpl ./internal/profile ./internal/store/postgres ./migrations -count=3
CGO_ENABLED=0 go test ./internal/prompttmpl ./internal/profile -count=1
go vet ./...
go build ./...
go mod tidy -diff
git diff --check
```

Acceptance requires the sanitized reference corpus, exact boundary tests, real PostgreSQL draft/publication evidence, stable built-in hash, no prompt content in logs/errors, and a clean independent review before starting the runtime plan.
