# Final whole-foundation fix report — round 1/5

Date: 2026-08-16

Baseline: `0aa1b2f6f8c8e0a098d707871ff05cf0f350b1ac`

Implementation commit: `476c9e433cf4f2c5a40206ebd9bf863a77431b0b fix: close vertical profile foundation invariants`

This round closes all three P1 findings and the P2 finding in `final-review.md`. It changes only the paths authorized by `final-fix-1-brief.md`; the evidence report is committed separately so it can record the immutable implementation commit.

## Strict RED evidence

Tests were added or tightened before production changes. The load-bearing failing observations were:

- Worker objective binding and composite engine identity:
  - Focused `internal/profile` regressions admitted a recipe whose objective tuple appeared under `system.j2` or a fragment, admitted a missing or mismatched `workers/<recipe-id>.j2` binding, and accepted implementation-less or malformed engine identities in `Snapshot`, `Published`, and `PublishRecord`.
  - GREEN after manifest/publication validation requires the exact derived worker logical name and tuple, and all three domain records use the shared 255-byte composite identity validator.
- Whole-Bundle envelope:
  - Focused MiniJinja and Cache tests admitted schema metadata/ACL and aggregate-cardinality payloads beyond the configured bundle limits; Cache also reached its supplied engine for an already-cancelled oversized bundle.
  - GREEN after both entry points apply the same cancellation-aware, overflow-safe envelope before clone, sort, or fingerprint work. Tests cover exact byte/cardinality limits, limit+1, template count, schema-field count, nested ACLs, and pre-cancelled input.
- SQL and Store engine boundaries:
  - Real PostgreSQL direct-schema tests accepted missing prefix/suffix, empty or invalid segments, and 256-byte identities. The Store could also read a deliberately corrupted implementation-less identity.
  - GREEN after migration 9 enforces the same composite grammar/255-byte limit and Store write/read paths reject noncompliant identities. The updated embedded migration checksum is `363d71258d0fecd30538eaf3bbeb323a1e9c87ac7004226a39ea32f9ca8d7729`.
- Database-owned publication/lifecycle readback:
  - Real PostgreSQL trigger regressions initially allowed canonical Publish and Revoke rewrites to commit. The first Activate setup accidentally used a clock before `published_at`; after correcting that fixture to reach the mutation, the audit rewrite reproduced the same missing-readback defect.
  - GREEN after Publish, Activate, Revoke, and active-version deactivation use full-row `RETURNING`, retain exact raw JSON bytes, compare all 15 canonical/audit fields, and roll back on any difference. An exhaustive regression mutates each field independently.

## Implementation

- Worker recipes now bind their `Objective` only to `workers/<recipe-id>.j2`, with exact `TemplateRef` equality. System, fragment, absent worker path, and mismatched tuple cases fail closed.
- `Snapshot`, `Published`, `PublishRecord`, PostgreSQL list/read paths, Store writes, and migration 9 share the `jinja-go-v1/<implementation>` identity contract, including exact 255/256-byte boundaries and forbidden empty/dot segments.
- PostgreSQL publication and lifecycle mutations now construct a database-owned canonical readback, compare every immutable and audit value before commit, and return only existing content-free public errors on mismatch. Prior-active deactivation is checked by the same exact mechanism.
- MiniJinja and Cache bound template counts/names/sources, schema field counts/names/metadata, and nested template ACL counts/names before preprocessing. Accounting uses the cache's length-prefixed canonical primitives, bounded integer subtraction, aggregate item limits, and per-item cancellation checks.

## GREEN and acceptance evidence

All commands ran from the final owned implementation tree. Sandbox-only loopback failures were not accepted. One first ordinary integration run had a single PostgreSQL container-address startup failure and was discarded; the complete repeat below passed with no skips.

- Focused local regressions for worker binding, domain identity, and MiniJinja/Cache envelopes:
  - PASS: `internal/prompttmpl` 0.824s; `internal/profile` 1.253s.
- Focused real PostgreSQL readback/trigger/Store/schema regressions:
  - `go test -v -tags=integration ./internal/store/postgres -run 'TestProfile(PublishedReadbackRequiresEveryExactReturnedField|PublishRollsBackExactCanonicalDatabaseRewrite|ActivateRollsBackDatabaseAuditRewrite|RevokeRollsBackDatabaseAuditRewrite|StoreRejectsNoncompositeEngineIdentityOnWriteAndRead)$|TestSchemaProfileEngineIdentityMatchesGoBoundary$' -count=1`
  - PASS in 7.272s, no skip. Publish, Activate, and Revoke trigger rewrites rolled back; all 15 readback fields, Store write/read identity, and direct SQL 255/256/grammar cases passed.
- `go test ./internal/prompttmpl ./internal/profile ./internal/store/postgres ./migrations -count=1`
  - PASS: prompttmpl 2.247s; profile 3.036s; PostgreSQL 4.221s; migrations 5.005s.
- `go test -race ./internal/prompttmpl ./internal/profile ./internal/store/postgres ./migrations -count=3`
  - PASS: prompttmpl 1.651s; profile 2.310s; PostgreSQL 2.723s; migrations 3.302s.
- `CGO_ENABLED=0 go test ./internal/prompttmpl ./internal/profile -count=1`
  - PASS: prompttmpl 0.830s; profile 1.618s.
- Real PostgreSQL 16 ordinary: `go test -v -tags=integration ./internal/store/postgres -run 'TestProfile|TestSchema' -count=1`
  - PASS in 65.683s against actual `postgres:16-alpine` containers; no test skipped.
- Real PostgreSQL 16 race: `go test -race -json -tags=integration ./migrations ./internal/store/postgres -run 'TestProfile|TestSchema|TestEmbedded' -count=1`
  - PASS: migrations 2.095s; PostgreSQL 71.298s. JSON filtering reported exactly `skip_actions=0 fail_actions=0`.
- `go test ./... -count=1`
  - PASS for every package; `demo/live-client` passed in 45.026s.
- `go test -race ./... -count=1`
  - PASS for every package; `demo/live-client` passed in 45.301s.
- `go test ./internal/prompttmpl -run=^$ -fuzz=FuzzValidateSourceNeverPanics -fuzztime=10s`
  - PASS after 3,348,343 executions; no panic or hang.
- `go vet ./...`, `go build ./...`, `go mod tidy -diff`, changed-owned-Go-file `gofmt -d`, plain and baseline-range `git diff --check`
  - PASS with no diagnostics or diff.
- `make verify-deploy-static`
  - PASS: durable Compose wrapper, deploy validation, live load/loadgen, supply-evidence semantic boundary, release-gate boundary, and final static deployment verification.

## Privacy, ownership, and self-review

- New public failure paths return only existing stable profile/template sentinels; no prompt source, manifest/snapshot bytes, database text, trigger values, actors, or raw dependency errors are interpolated or logged.
- Raw publication documents are cloned on readback; manifest/snapshot domain values and returned `Published` records remain deeply owned. The envelope rejects before Cache cloning/fingerprinting and before MiniJinja preprocessing.
- The changed production diff adds no logging, filesystem/environment/process/network access, or raw-error formatting.
- Self-review found and corrected one inaccurate envelope-accounting comment. A second full inspection found no remaining load-bearing issue. The five deferred P3 dispositions remain unchanged.

## Exact scope and commit audit

Before the implementation commit, `git diff --cached --name-only` contained exactly these 15 authorized paths:

- `internal/profile/publisher_test.go`
- `internal/profile/store.go`
- `internal/profile/types.go`
- `internal/profile/types_test.go`
- `internal/prompttmpl/cache.go`
- `internal/prompttmpl/cache_test.go`
- `internal/prompttmpl/engine.go`
- `internal/prompttmpl/engine_test.go`
- `internal/prompttmpl/minijinja.go`
- `internal/prompttmpl/minijinja_test.go`
- `internal/store/postgres/profiles.go`
- `internal/store/postgres/profiles_integration_test.go`
- `internal/store/postgres/schema_integration_test.go`
- `migrations/000009_vertical_profiles.up.sql`
- `migrations/embed_test.go`

`git diff --cached --check` passed. Commit `476c9e433cf4f2c5a40206ebd9bf863a77431b0b` contains 792 insertions and 132 deletions across only those paths.

Protected browser/audio files, `.zshrc`, research notes, and the realtime interruption plan remained untouched and unstaged. No path outside the brief was required.

Concerns: none.
