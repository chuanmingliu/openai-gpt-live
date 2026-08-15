# Task 5 report — Publisher, Resolver, and Built-In Voice Profile

Date: 2026-08-15

Baseline: `e9077dc03fc4` (Task 4). The implementation changes only the Task 5-owned profile publisher/resolver/built-in files, the authorized `Cache.Version()` addition and test, and this report.

## RED evidence

Tests were written before each corresponding production behavior. The important failing observations were:

- `env GOCACHE=/private/tmp/codex-task5-go-cache go test ./internal/profile ./internal/prompttmpl -run 'TestPublish|TestPreview|TestValidate|TestResolve|TestBuiltIn|TestCacheVersion' -count=1`
  - RED at compile time: `Publisher`, `PublishCommand`, `ResolvePurpose`, `Builtin`, and `Cache.Version` were undefined.
- `TestPublishValidatesSourcePolicyIndependentlyOfSuppliedEngine`
  - RED: a deliberately permissive supplied engine admitted an unsafe template call.
  - GREEN after the publisher independently applies the shared bundle source-policy validator before its single supplied-engine compilation.
- `TestPublishRejectsAdvisoryVersionOutsideStoreDomainBeforeMutation`
  - RED: advisory `NextVersion = MaxInt64 + 1` reached publication.
  - GREEN after rejecting versions outside the PostgreSQL/store domain before mutation.
- `TestPublisherPreservesDependencyCancellationClasses`
  - RED: catalog/knowledge cancellation was mapped to `profile_incompatible`, and engine cancellation was mapped to `template_invalid`.
  - GREEN after preserving `context.Canceled` and `context.DeadlineExceeded` while continuing to redact all other dependency errors.
- `TestPublishStopsWhenDependencyCancelsContextButReturnsSuccess` and `TestResolveStopsWhenEngineCancelsContextButReturnsCompiledBundle`
  - RED: a dependency could cancel the context, return success, and allow the operation to continue successfully.
  - GREEN after explicit post-dependency context fences prevent publication or resolution from continuing.
- `TestPublishRejectsOutOfRangeAuditClockBeforeMutation`
  - RED: a year-10000 publisher clock reached Store publication.
  - GREEN after UTC/microsecond canonicalization plus the repository's bounded audit-time domain check.

## Implementation

- Added one shared, bounded publisher preparation pipeline used by `Validate`, `Preview`, and `Publish`. It validates boundary inputs before Store access, loads one exact draft generation, owns every dependency result, verifies every exact blob tuple, validates source policy independently, compiles once, parses strict fixtures with duplicate/unknown/trailing/depth/item/surrogate checks, applies context ACL/schema validation, renders exact expected output, validates adapter capabilities and knowledge references, pins stable engine/catalog versions, obtains one advisory version, and builds one complete immutable snapshot.
- `Validate` and `Preview` perform no Store mutation; only `Preview` returns owned synthetic output. `Publish` hands the exact prepared manifest/snapshot/generation/version/audit input to the Store and returns the Store's exact `Published` result. Activation and revocation delegate exact identities with stable content-free error mapping and deterministic clock support.
- Added exact purpose-aware custom resolution. New sessions and reconnects require active, unrevoked versions; accepted work may reopen an exact inactive/revoked immutable version. Every path rechecks tenant/ref/lifecycle/engine/snapshot/manifest/blob integrity, reloads all template and fixture blobs, validates sources through the configured engine, and returns deep-owned state.
- Added the Store-free `builtin/1` fallback from embedded checked-in assets. It has one static system template, an empty typed schema, one exact synthetic fixture, no optional prompt templates, recipes, bindings, or knowledge, and no filesystem/environment/network/database/clock dependency. The stable snapshot hash is `e31054fe0e94fab962253ac0a86d26ecdd05893d72520a9446befa9be7cfc568`.
- Added dynamic, nil-safe `Cache.Version()` delegation so the bounded cache satisfies the engine identity contract without freezing a stale underlying version.

## Final verification

The PostgreSQL integration harness used a real disposable PostgreSQL 16 container. A sandbox-only attempt that could not reach Docker was not accepted as evidence; all recorded integration results below are connected, non-skipped runs.

- `env GOCACHE=/private/tmp/codex-task5-go-cache go test ./internal/profile -run 'TestPublish|TestPreview|TestValidate|TestResolve|TestBuiltIn' -count=1`
  - PASS: profile 0.429s; final staged-tree repeat 2.828s.
- `env GOCACHE=/private/tmp/codex-task5-go-cache go test -race ./internal/prompttmpl ./internal/profile -count=10`
  - PASS: prompttmpl 2.146s; profile 3.037s.
- `env GOCACHE=/private/tmp/codex-task5-go-cache go test -race ./internal/prompttmpl ./internal/profile ./internal/store/postgres ./migrations -count=3`
  - PASS: prompttmpl 1.452s; profile 2.256s; PostgreSQL 2.201s; migrations 3.622s.
- `env CGO_ENABLED=0 GOCACHE=/private/tmp/codex-task5-go-cache go test ./internal/prompttmpl ./internal/profile -count=1`
  - PASS: prompttmpl 0.536s; profile 1.020s.
- `env GOCACHE=/private/tmp/codex-task5-go-cache go test -tags=integration ./internal/store/postgres -run 'TestProfile|TestSchema' -count=1`
  - PASS against real PostgreSQL: 58.558s.
- `env GOCACHE=/private/tmp/codex-task5-go-cache go test -race -tags=integration ./migrations ./internal/store/postgres -run 'TestProfile|TestSchema|TestEmbedded' -count=1`
  - PASS against real PostgreSQL: migrations 2.278s; PostgreSQL repository 63.601s.
- `env GOCACHE=/private/tmp/codex-task5-go-cache go test ./... -count=1`
  - PASS for every package; the browser demo package passed in 34.375s.
- `env GOCACHE=/private/tmp/codex-task5-go-cache go vet ./...`
  - PASS (exit 0, no diagnostics).
- `env GOPROXY=off GOCACHE=/private/tmp/codex-task5-go-cache go build ./...`
  - PASS (exit 0, no diagnostics).
- `env GOCACHE=/private/tmp/codex-task5-go-cache go mod tidy -diff`
  - PASS (exit 0, no diff). An earlier sandboxed invocation without the task-local `GOCACHE` could not read the host Go cache and was not accepted.
- `git diff --check`
  - PASS (exit 0, no diagnostics).
- `make verify-deploy-static`
  - PASS: durable Compose wrapper, deploy validation, live load/loadgen, supply-evidence boundary, release-gate boundary, and final static deployment verification all passed.

## Privacy, determinism, and ownership audit

- Focused privacy/ownership tests passed for publisher dependency/store redaction, resolver dependency/store redaction, render-limit redaction, preview ownership and zero mutation, resolved snapshot/bundle/schema ownership, and built-in clone isolation.
- A static forbidden-API scan of `publisher.go`, `resolver.go`, and `builtin.go` found no formatting of raw errors, logging, filesystem/environment/process/network/database access, or other external built-in input path.
- Prompt source, fixture context/expected/rendered output, knowledge data, credentials, URLs, raw adapter/model output, and reasoning are never interpolated into public errors or logs. Unrecognized dependency errors collapse to stable sentinels; cancellation classes remain recognizable.
- All mutable Store, engine, catalog, validator, fixture-output, bundle, schema, binding, knowledge, snapshot, and built-in values are cloned at their ownership boundaries. Caller or dependency mutation cannot alter a later publication, resolution, preview, or built-in result.
- Hashes and the built-in profile are deterministic: embedded assets are the only source; source-map ordering is canonicalized by the existing domain hash contracts; no wall clock participates in bundle or snapshot hashes.

## Exact staging audit

`git diff --cached --name-only` contained exactly these 12 authorized paths:

- `.superpowers/sdd/2026-08-15-vertical-profile-foundation/task-5-report.md`
- `internal/profile/builtin.go`
- `internal/profile/builtin/context.schema.json`
- `internal/profile/builtin/fixture.json`
- `internal/profile/builtin/system.j2`
- `internal/profile/builtin_test.go`
- `internal/profile/publisher.go`
- `internal/profile/publisher_test.go`
- `internal/profile/resolver.go`
- `internal/profile/resolver_test.go`
- `internal/prompttmpl/cache.go`
- `internal/prompttmpl/cache_test.go`

`git diff --cached --check` passed with no diagnostics. The report alone required `git add -f` because `.superpowers` is repository-ignored; no other ignored path was added. Pre-existing browser/audio modifications, `.zshrc`, research notes, and the realtime interruption plan remained untouched and unstaged.

Concerns: none.
