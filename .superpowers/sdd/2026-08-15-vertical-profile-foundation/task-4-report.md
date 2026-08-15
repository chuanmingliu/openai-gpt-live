# Task 4 report — PostgreSQL Draft and Published Profile Store

Date: 2026-08-15

Baseline: `4c26b66860b7` (Task 3). The implementation used only the original Task 4 paths plus the review-authorized `internal/profile/types.go`, `internal/profile/types_test.go`, and `internal/store/postgres/repository.go` overlaps.

## RED evidence

Tests were written before their corresponding migration/repository behavior. The important failing observations were:

- `env GOCACHE=/private/tmp/codex-task4-go-cache go test ./migrations -count=1`
  - RED: migration catalog expected version 9 while `latestVersion` was 8, and the embedded `000009_vertical_profiles` files did not exist.
- `env GOCACHE=/private/tmp/codex-task4-go-cache go test -tags=integration ./internal/store/postgres -run 'TestProfile|TestSchema' -count=1`
  - RED at compile time: `*Repository` did not implement the new `profile.Store` methods and migration-9 catalog/schema assertions had no implementation.
- Focused real-PostgreSQL template test after the first repository pass:
  - RED: saving an empty template returned the stable `profile_store_unavailable` error because a nil zero-length slice was sent as SQL `NULL` rather than non-null `BYTEA`.
  - GREEN after using an owned, non-nil zero-length slice at every blob insert boundary.
- Focused canonical-JSON publication test:
  - RED: a valid raw JSON number such as `1.2300e+2` produced `stale_draft` because `SaveDraft` returned the caller representation rather than PostgreSQL JSONB's persisted canonical representation.
  - GREEN after all draft mutations return and hash the database-owned JSONB representation.
- Focused exact revocation-retry test:
  - RED: the same requested time containing sub-microsecond precision returned `publication_conflict` on retry because PostgreSQL timestamps are microsecond-precise.
  - GREEN after canonicalizing all persisted/audit times to UTC microseconds.
- `env GOCACHE=/private/tmp/codex-task4-go-cache go test -tags=integration ./internal/store/postgres -run '^TestProfileDraftSaveUsesExactGenerationAndOwnedCopies$' -count=1`
  - RED (real PostgreSQL): the insert-only retry expected `stale_draft` but received `profile_invalid` at `profiles_integration_test.go:35`.
  - GREEN in 1.938s after allowing `expectedGeneration=0` to reach the insert-only conflict while retaining exact generation checks for updates.

Additional post-mutation failpoint tests prove failed `SaveTemplate` and `DeleteTemplate` transactions roll back both the generation/ref mutation and any newly inserted blob.

## Implementation

- Added embedded migration 9 with bounded, tenant-scoped `profile_blobs`, `profile_drafts`, and `profile_versions`; named checks and uniqueness constraints; exact manifest and snapshot JSON; publication/activation/revocation audit metadata; duplicate-draft-generation prevention; and the one-active-version partial unique index.
- Advanced `migrations.Check` authority to version 9 and added exact embedded names/checksums plus up/down/up, catalog, default, nullability, constraint, and index assertions.
- Added normative `profile.Store` ownership and concurrency documentation plus advisory `NextVersion`.
- Implemented the full PostgreSQL `profile.Store` boundary with pre-SQL validation, tenant scoping, stable content-free errors, deep copy-in/copy-out, content-integrity rechecks, exact optimistic generations, atomic template changes, serialized publication allocation, immutable resolve, keyset pagination, active swaps, and exact revocation retries.
- Added real PostgreSQL integration coverage for concurrency, rollback/cancellation, zero-byte blobs, corruption, isolation, aliasing, raw JSON stability, publication allocation, pagination boundaries, activation/revocation audit semantics, and privacy-safe error classification.

Migration SHA-256 catalog:

- `000009_vertical_profiles.up.sql`: `4361880ea8bcadad18d1bff08647146bd38fcddf3206bcde9a5f303e9a2780c4`
- `000009_vertical_profiles.down.sql`: `14ddeb515492515b2f23a0afe754c1ab31cfa534ec93bed974f46b87798dd39e`

## Final verification

The integration harness used a real disposable `postgres:16-alpine` container through Docker Engine 29.2.1. No PostgreSQL test was accepted through a skip.

- `env GOCACHE=/private/tmp/codex-task4-go-cache go test ./migrations -count=1`
  - PASS: `ok github.com/chuanmingliu/openai-gpt-live/migrations 3.838s`
- `env GOCACHE=/private/tmp/codex-task4-go-cache go test -tags=integration ./internal/store/postgres -run 'TestProfile|TestSchema' -count=1`
  - PASS: `ok github.com/chuanmingliu/openai-gpt-live/internal/store/postgres 44.306s`
- `env GOCACHE=/private/tmp/codex-task4-go-cache go test -race -tags=integration ./migrations ./internal/store/postgres -run 'TestProfile|TestSchema|TestEmbedded' -count=1`
  - PASS: migrations 2.118s; PostgreSQL repository 48.302s.
- `env GOCACHE=/private/tmp/codex-task4-go-cache go vet ./migrations ./internal/profile ./internal/store/postgres`
  - PASS (exit 0, no diagnostics).
- `env GOCACHE=/private/tmp/codex-task4-go-cache go test ./... -count=1`
  - PASS for every package; slowest package was `demo/live-client` at 31.116s.
- `env GOCACHE=/private/tmp/codex-task4-go-cache go vet ./...`
  - PASS (exit 0, no diagnostics).
- `env GOCACHE=/private/tmp/codex-task4-go-cache go build ./...`
  - PASS (exit 0, no diagnostics).
- `env GOCACHE=/private/tmp/codex-task4-go-cache go mod tidy -diff`
  - PASS (exit 0, no diff).
- `env GOCACHE=/private/tmp/codex-task4-go-cache go test -race ./internal/prompttmpl ./internal/profile ./internal/store/postgres ./migrations -count=3`
  - PASS: prompttmpl 5.681s, profile 4.558s, PostgreSQL 6.211s, migrations 5.711s.
- `env CGO_ENABLED=0 GOCACHE=/private/tmp/codex-task4-go-cache go test ./internal/prompttmpl ./internal/profile -count=1`
  - PASS: prompttmpl 1.020s; profile 2.484s.
- `git diff --check`
  - PASS (exit 0, no diagnostics).

## Privacy and ownership audit

- Every repository query/mutation includes the explicit tenant key; hashes are identifiers, never authority. Wrong-tenant resolves/loads are indistinguishable from absence, and wrong-tenant mutations cannot acquire the tenant/profile draft lock.
- SQL/driver/failpoint errors are mapped to stable domain errors, caller cancellation, or the generic content-free `profile_store_unavailable`; tests inject prompt text, SQL fragments, and DSN-like strings and prove none are returned.
- Mutable input and output maps, slices, byte slices, `json.RawMessage`, nested component records, and time pointers are cloned. Load paths also recompute blob hashes/counts and reconstruct/validate published records before returning them.
- Only the owned Task 4 paths are staged for the Task 4 commit. Pre-existing dirty browser/audio files, `.zshrc`, research notes, and the interruption plan remain untouched and unstaged.

Concerns: none. The opaque list cursor intentionally contains only the bounded keyset (`profile_id`, `version`) and no manifest, snapshot, blob, tenant, or credential content.

## Fix round 1/5 — independent review corrections

Review range: `bc39269e90b80fb80018426af97ef8bd8bd832ef..HEAD`.

### Strict RED evidence

- `env GOCACHE=/private/tmp/codex-task4-fix-go-cache go test ./internal/profile -run 'TestManifestCanonicalJSON|TestSnapshotCanonicalJSON' -count=1`
  - RED at compile time: `Manifest.CanonicalJSON` and `Snapshot.CanonicalJSON` were undefined.
  - GREEN: 2.201s focused; 0.363s full package.
- `env GOCACHE=/private/tmp/codex-task4-fix-go-cache go test -tags=integration ./internal/store/postgres -run 'TestProfileSaveDraft|TestProfileResolvePreservesTask3' -count=1`
  - RED against PostgreSQL 16: the `1e100000` insert returned `profile_invalid`; invalid insert readback left one committed row; invalid update readback advanced generation 1→2 and stored the rewrite; missing/cross-tenant/media/count/fixture/corrupt blob references were all accepted; and JSONB rewrote `1.2300e+2` to `123.00`.
  - GREEN after exact JSON storage, explicit draft transactions/readback validation, and ref integrity: 7.935s.
- `env GOCACHE=/private/tmp/codex-task4-fix-go-cache go test -tags=integration ./internal/store/postgres -run 'TestSchemaProfileV9|TestProfileRevoke|TestProfileBlob' -count=1`
  - RED: nonempty content with `ContentBytes=0` was stored; a revocation five minutes before activation succeeded and made the row inactive; the corrected FK catalog probe returned no rows.
  - GREEN after exact count enforcement, Store/SQL chronology checks, composite restrictive FK, and scalar/snapshot coherence: 4.157s. The expanded five-field coherence mutation test passed in 4.475s.
- `env GOCACHE=/private/tmp/codex-task4-fix-go-cache go test -tags=integration ./internal/store/postgres -run '^TestProfileStoreUnavailable' -count=1`
  - RED at compile time: public `profile.ErrStoreUnavailable` was undefined.
  - GREEN against a failpoint and an actually closed pool, including transient classification and cause redaction: 4.061s.
- `env GOCACHE=/private/tmp/codex-task4-fix-go-cache go test ./migrations -count=1`
  - RED: the exact migration checksum changed from `66599846…` to `4361880e…`.
  - GREEN after updating the embedded catalog.

### Fix implementation

- Added owned deterministic `Manifest.CanonicalJSON` and `Snapshot.CanonicalJSON`. Both preserve Task 3's hash-significant number lexemes and component ordering; snapshot persistence retains `SnapshotHash` even though hash computation omits it.
- Changed manifest/snapshot columns from JSONB to exact PostgreSQL JSON and persisted only Task 3 canonical bytes. Insert and update expansion probes preserve `1e100000` without rewriting or post-validation growth.
- Made `SaveDraft` explicitly transactional. The persisted readback is domain-validated and byte-compared before commit, so an invalid database rewrite rolls back both inserts and updates. Every template and fixture ref is rehashed and matched to exact same-tenant hash/media/count bytes before commit.
- Added `profile_versions_draft_fkey` on `(tenant_id,profile_id)` with `ON DELETE RESTRICT`; direct orphan, cross-tenant, and owning-draft deletion probes fail on that named constraint.
- Added `profile_versions_snapshot_coherence_check` for tenant, profile ID, version, engine version, and snapshot hash, with a direct mutation test for every scalar.
- Added `profile_versions_chronology_check` and Store validation rejecting `revoked_at < activated_at`; readback validation also fails closed on reversed chronology.
- Added public content-free `profile.ErrStoreUnavailable`, mapped all generic Store failures to it, and classified it transient in `postgres.IsTransientDurabilityError` without retaining raw causes.
- Made `PutBlob` treat `ContentBytes` as exact: nonempty content plus zero is invalid, while genuinely empty content remains valid.

### Fix-round final verification

The same real disposable `postgres:16-alpine` harness was used; no integration test was skipped.

- `env GOCACHE=/private/tmp/codex-task4-fix-go-cache go test ./migrations ./internal/profile ./internal/store/postgres -count=1`
  - PASS: migrations 0.501s, profile 2.045s, PostgreSQL 2.364s.
- `env GOCACHE=/private/tmp/codex-task4-fix-go-cache go test -tags=integration ./internal/store/postgres -run 'TestProfile|TestSchema' -count=1`
  - PASS: PostgreSQL repository 50.148s.
- `env GOCACHE=/private/tmp/codex-task4-fix-go-cache go test -race -tags=integration ./migrations ./internal/store/postgres -run 'TestProfile|TestSchema|TestEmbedded' -count=1`
  - PASS: migrations 2.893s; PostgreSQL repository 55.055s.
- `env GOCACHE=/private/tmp/codex-task4-fix-go-cache go test ./... -count=1`
  - PASS for every package; slowest package was `demo/live-client` at 30.376s.
- `env GOCACHE=/private/tmp/codex-task4-fix-go-cache go vet ./...`
  - PASS (exit 0, no diagnostics).
- `env GOCACHE=/private/tmp/codex-task4-fix-go-cache go build ./...`
  - PASS (exit 0, no diagnostics).
- `env GOCACHE=/private/tmp/codex-task4-fix-go-cache go mod tidy -diff`
  - PASS (exit 0, no diff).
- `env GOCACHE=/private/tmp/codex-task4-fix-go-cache go test -race ./internal/prompttmpl ./internal/profile ./internal/store/postgres ./migrations -count=3`
  - PASS: prompttmpl 3.616s, profile 3.084s, PostgreSQL 3.598s, migrations 4.550s.
- `env CGO_ENABLED=0 GOCACHE=/private/tmp/codex-task4-fix-go-cache go test ./internal/prompttmpl ./internal/profile -count=1`
  - PASS: prompttmpl 0.516s; profile 1.416s.
- `git diff --check`
  - PASS (exit 0, no diagnostics).

Fix-round privacy/ownership audit: exact canonical payloads remain tenant-scoped and are never emitted in errors; both failpoint and closed-pool raw causes collapse to the public content-free sentinel. Only review-authorized Task 4 paths will be staged. Protected browser/audio changes and unrelated notes remain untouched. Concerns: none.

## Fix round 2/5 — exact raw database readbacks

Review range: `d19258c871d9c1800a736d14f7258d5d89cad34d..HEAD`.

### Strict RED evidence

- `env GOCACHE=/private/tmp/codex-task4-fix2-go-cache go test -tags=integration ./internal/store/postgres -run 'TestProfileSaveDraftRejectsDomainValidNoncanonicalDatabaseReadback|TestProfileSaveDraftRejectsDatabaseMetadataRewrite|TestProfileLoadDraftRejectsNoncanonicalStoredManifest|TestProfileTemplateMutationsRejectNoncanonicalDatabaseReadback|TestProfileResolveRejectsNoncanonicalStoredDocuments' -count=1 -v`
  - RED on real PostgreSQL 16 in 8.920s. Domain-valid whitespace rewrites committed both SaveDraft insert and update; the update advanced generation 1→2. An actor rewrite also committed. LoadDraft normalized a whitespace-corrupt manifest. SaveTemplate committed its blob/ref/generation and DeleteTemplate committed its removal/generation despite noncanonical returned bytes. Resolve normalized both whitespace-corrupt manifest and snapshot documents.
  - The first sandboxed invocation could not access Docker and was not accepted as RED; the elevated invocation connected to Docker Desktop 29.2.1 and produced the behavioral failures above.

### Fix implementation

- Added a raw draft readback envelope that owns `manifest::text`. SaveDraft, SaveTemplate, and DeleteTemplate compare the exact returned tenant, profile ID, generation, timestamp, actor, and manifest bytes with their intended values before any JSON decode and before transaction commit.
- Draft decoding now re-encodes through `Manifest.CanonicalJSON` and requires byte-for-byte identity, so LoadDraft and draft-for-update paths reject persisted whitespace, key-order, component-order, or numeric-representation corruption instead of normalizing it.
- Published decoding now requires both raw manifest and snapshot bytes to equal `Manifest.CanonicalJSON` and `Snapshot.CanonicalJSON`; valid canonical numeric lexemes and snapshot hashes remain unchanged.
- Added real PostgreSQL BEFORE INSERT/UPDATE domain-valid representation-changing triggers for initial/update SaveDraft and both template mutations, with rollback/no-generation-change/no-orphan-blob assertions. Added direct stored-draft and published manifest/snapshot corruption tests plus exhaustive returned-field mismatch coverage.

### Fix-round final verification

The real integration harness used disposable `postgres:16-alpine` containers through Docker Desktop 29.2.1. No PostgreSQL test was accepted through a skip.

- Focused GREEN command matching the RED selection:
  - PASS for every regression. One unrelated testcontainer address lookup transiently failed during the combined rerun; its isolated retry, `... -run '^TestProfileSaveDraftRejectsDatabaseMetadataRewrite$'`, passed in 2.744s.
- `env GOCACHE=/private/tmp/codex-task4-fix2-go-cache go test -tags=integration ./internal/store/postgres -run 'TestProfile|TestSchemaProfileV9' -count=1 -v`
  - PASS: PostgreSQL repository 28.360s.
- `env GOCACHE=/private/tmp/codex-task4-fix2-go-cache go test -race -tags=integration ./migrations ./internal/store/postgres -run 'TestProfile|TestSchema|TestEmbedded' -count=1`
  - Initial combined attempt: migrations PASS 3.949s; PostgreSQL package encountered an external testcontainer startup failure in the first harness after 61.51s, with no code or race diagnostic.
  - Clean retry without package-level Docker contention, `... go test -race -tags=integration ./internal/store/postgres -run 'TestProfile|TestSchema|TestEmbedded' -count=1`, PASS: 64.813s.
- `env GOCACHE=/private/tmp/codex-task4-fix2-go-cache go test ./migrations ./internal/profile ./internal/store/postgres -count=1`
  - PASS: migrations 1.116s; profile 1.432s; PostgreSQL 1.748s.
- `env GOCACHE=/private/tmp/codex-task4-fix2-go-cache go test ./... -count=1`
  - PASS for every package after running with localhost bind authority; slowest package was `demo/live-client` at 31.412s. The sandbox-only attempt failed solely because `httptest` could not bind ephemeral loopback ports and was not accepted.
- `env GOCACHE=/private/tmp/codex-task4-fix2-go-cache go vet ./...`
  - PASS (exit 0, no diagnostics).
- `env GOCACHE=/private/tmp/codex-task4-fix2-go-cache go build ./...`
  - PASS (exit 0, no diagnostics) on a clean authorized rerun.
- `env GOCACHE=/private/tmp/codex-task4-fix2-go-cache go mod tidy -diff`
  - PASS (exit 0, no diff).
- `env GOCACHE=/private/tmp/codex-task4-fix2-go-cache go test -race ./internal/prompttmpl ./internal/profile ./internal/store/postgres ./migrations -count=3`
  - PASS: prompttmpl 4.527s; profile 4.096s; PostgreSQL 5.005s; migrations 4.618s.
- `env CGO_ENABLED=0 GOCACHE=/private/tmp/codex-task4-fix2-go-cache go test ./internal/prompttmpl ./internal/profile -count=1`
  - PASS: prompttmpl 2.483s; profile 2.097s.
- `git diff --check`
  - PASS (exit 0, no diagnostics).

Fix-round privacy/ownership audit: exact raw bytes are retained only long enough to validate and are never included in returned errors. All corruption and trigger failures collapse to `profile.ErrInvalidProfile`; transaction rollback prevents failed mutations from becoming visible. Only `internal/store/postgres/profiles.go`, `internal/store/postgres/profiles_integration_test.go`, and this authorized report are part of this round. Protected browser/audio changes and unrelated notes remain untouched. Concerns: none.
