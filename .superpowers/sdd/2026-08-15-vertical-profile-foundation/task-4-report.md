# Task 4 report — PostgreSQL Draft and Published Profile Store

Date: 2026-08-15

Baseline: `4c26b66860b7` (Task 3). The implementation was made only in the nine paths assigned by `task-4-brief.md`.

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

- Added embedded migration 9 with bounded, tenant-scoped `profile_blobs`, `profile_drafts`, and `profile_versions`; named checks and uniqueness constraints; exact manifest and snapshot JSONB; publication/activation/revocation audit metadata; duplicate-draft-generation prevention; and the one-active-version partial unique index.
- Advanced `migrations.Check` authority to version 9 and added exact embedded names/checksums plus up/down/up, catalog, default, nullability, constraint, and index assertions.
- Added normative `profile.Store` ownership and concurrency documentation plus advisory `NextVersion`.
- Implemented the full PostgreSQL `profile.Store` boundary with pre-SQL validation, tenant scoping, stable content-free errors, deep copy-in/copy-out, content-integrity rechecks, exact optimistic generations, atomic template changes, serialized publication allocation, immutable resolve, keyset pagination, active swaps, and exact revocation retries.
- Added real PostgreSQL integration coverage for concurrency, rollback/cancellation, zero-byte blobs, corruption, isolation, aliasing, raw JSON stability, publication allocation, pagination boundaries, activation/revocation audit semantics, and privacy-safe error classification.

Migration SHA-256 catalog:

- `000009_vertical_profiles.up.sql`: `6659984635cfe12f5cf735277579f1b0e309d88e910f2bdea6c921ed345fb4f5`
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
