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

## Fix round 1/5 — composite compatibility and exact boundary corrections

Date: 2026-08-16

Review range: `e9077dc03fc47f26ebe7c04831b97d833f579dff..8ec48eece86c23221b8b0eaf418897b7f8b02e48`. This round addresses all six Important findings and the two binding Minors in `task-5-review.md`. The progress ruling additionally authorizes only `internal/prompttmpl/engine.go`, `engine_test.go`, `minijinja.go`, and `minijinja_test.go` for the composite identity change.

### Strict RED evidence

- `env GOCACHE=/private/tmp/codex-task5-fix1-go-cache go test ./internal/prompttmpl -run 'TestValidateEngineIdentity|TestMiniJinjaReportsComposite|TestCacheRejectsNoncompliant|TestCacheVersionAndCompileFailClosed' -count=1`
  - RED at compile time: `ValidateEngineIdentity` was undefined. The new cache cases additionally describe rejection of implementation-only identities, typed-nil engines, and manually corrupt state without panic.
  - GREEN after adding the bounded `jinja-go-v1/<implementation>` grammar, rotating MiniJinja to `jinja-go-v1/minijinja-go/v2.24.0`, and validating cache construction/version/compilation.
- Focused publisher/resolver/built-in identity and asset selection:
  - RED: publisher and custom resolver each reached Store once for `minijinja-go/v2.24.0`; the built-in returned a 490-byte trimmed source rather than the 491-byte checked-in source; the identity-only intermediate built-in hash changed to `485d0ed0...` while the old literal remained pinned.
  - GREEN after pre-Store composite-identity validation and exact embedded-source use.
- `TestMiniJinjaRendersCheckedInTrailingNewlineExactly`
  - RED: source `"safe\n"` rendered as `"safe"` under MiniJinja's default whitespace configuration.
  - GREEN after enabling `KeepTrailingNewline`; the fixture now expects the exact embedded bytes, the exact system blob hash is `86141d8177594727d8043b7842762654496d41e0846470c71577f0ea7bb376f4`, and the final built-in snapshot hash is `70fb32f9d75a9eb5708d9e6d3783243b4aaf9ee551dd6e11f5c26539b2c3ec7c`.
- `TestValidateAndPreviewPreserveTrustedSourcePolicyLocationWithoutSource` and `TestPublishPreservesOnlyReviewedCompileErrorTemplateMetadata`
  - RED: both policy and typed compile failures collapsed to bare `template_invalid`; `system.j2:2:1` and the reviewed `system.j2` compile metadata were lost.
  - GREEN after retaining the repository-owned policy diagnostic and reconstructing typed compile metadata only for an actual loaded bundle member. Arbitrary dependency template/source/error text remains absent.
- Signed Store-domain and lifecycle tests:
  - RED: Activate accepted `MaxInt64+1`, custom Resolve called Store once for it, and valid revoked new-session/reconnect records with a retired engine returned `profile_incompatible`.
  - GREEN after applying the positive signed-`bigint` boundary before Store access and classifying valid revocation before current-engine compatibility for new/reconnect only. Accepted work continues to enforce the exact engine.
- Cancellation precedence suite:
  - RED: cancellation lost to stale/missing draft/blob, denied capability, invalid engine/catalog identity, rendered-output mismatch, invalid clocks, and missing resolver results; fixture validation rendered a second fixture after the first render canceled; successful Publish/Activate/Revoke returns masked cancellation; post-compile engine cancellation could return a resolved profile.
  - GREEN after immediate fences following every external Store, Engine, `CompiledBundle.Render`, catalog, capability, knowledge-validator, and publisher-clock return, before error/payload/business classification or another mutation. Fixture loops now fence at bounded iteration and post-render boundaries.
- `TestDecodeSyntheticFixtureAdmitsExactAggregateItemLimit`
  - RED: an exact aggregate of 1,024 root/context keys and array values returned `template_invalid`.
  - GREEN after permitting equality and continuing to reject 1,025.

### Fix implementation

- Added the shared bounded engine identity contract and stable `engine_identity_invalid` class. MiniJinja and compiled bundles report the composite identity; publisher, resolver, built-in resolution, and cache fail closed on any implementation-only or malformed identity.
- Hardened Cache against typed-nil engines and corrupt internal state, and required stable, compliant engine/compiled identities before cache admission. `Version` returns empty and `Compile` returns a stable typed error rather than panicking.
- Preserved exact content-free source-policy locations and allowlisted typed compile template metadata while continuing to collapse arbitrary dependency strings.
- Applied the Store's signed version domain to Activate, Revoke, and custom Resolve before Store access, and corrected revoked error precedence without weakening accepted-work integrity checks.
- Added immediate cancellation fences at every reviewed dependency/callback boundary and bounded fixture-loop points.
- Hash, compile, and render now use the exact checked-in `system.j2` bytes including its terminal newline. The fixture and stable literal hashes were rotated deliberately with the engine identity and exact source.
- Corrected the strict fixture aggregate counter to admit exactly 1,024 items and reject one past.

### Fix-round final verification

The PostgreSQL commands used real disposable PostgreSQL 16 containers; no integration result was skipped.

- `env GOCACHE=/private/tmp/codex-task5-fix1-go-cache go test ./internal/prompttmpl ./internal/profile -count=1`
  - PASS: prompttmpl 1.049s; profile 1.592s.
- `env GOCACHE=/private/tmp/codex-task5-fix1-go-cache go test ./internal/profile -run 'TestPublish|TestPreview|TestValidate|TestResolve|TestBuiltIn|TestDecodeSyntheticFixture' -count=1`
  - PASS: profile 3.207s.
- `env GOCACHE=/private/tmp/codex-task5-fix1-go-cache go test -race ./internal/prompttmpl ./internal/profile -count=10`
  - PASS: prompttmpl 1.338s; profile 1.661s.
- `env GOCACHE=/private/tmp/codex-task5-fix1-go-cache go test -race ./internal/prompttmpl ./internal/profile ./internal/store/postgres ./migrations -count=3`
  - PASS: prompttmpl 1.319s; profile 1.502s; PostgreSQL 1.534s; migrations 2.040s.
- `env CGO_ENABLED=0 GOCACHE=/private/tmp/codex-task5-fix1-go-cache go test ./internal/prompttmpl ./internal/profile -count=1`
  - PASS: prompttmpl 0.908s; profile 0.479s.
- `env GOCACHE=/private/tmp/codex-task5-fix1-go-cache go test -tags=integration ./internal/store/postgres -run 'TestProfile|TestSchema' -count=1`
  - PASS against real PostgreSQL: 58.464s.
- `env GOCACHE=/private/tmp/codex-task5-fix1-go-cache go test -race -tags=integration ./migrations ./internal/store/postgres -run 'TestProfile|TestSchema|TestEmbedded' -count=1`
  - PASS against real PostgreSQL: migrations 2.214s; PostgreSQL 64.552s.
- `env GOCACHE=/private/tmp/codex-task5-fix1-go-cache go test ./... -count=1`
  - PASS for every package; the browser demo package passed in 31.898s.
- `env GOCACHE=/private/tmp/codex-task5-fix1-go-cache go vet ./...`, `env GOPROXY=off GOCACHE=/private/tmp/codex-task5-fix1-go-cache go build ./...`, `env GOCACHE=/private/tmp/codex-task5-fix1-go-cache go mod tidy -diff`, `git diff --check`, and owned-file `gofmt -d`
  - PASS with no diagnostic or diff.
- `make verify-deploy-static`
  - PASS: durable Compose wrapper, deploy validation, live load/loadgen, supply-evidence boundary, release-gate boundary, and final static deployment verification.

Fix-round privacy/ownership audit: focused no-leak tests passed for unknown compile/render causes, trusted policy locations, allowlisted/untrusted compile metadata, publisher/resolver dependency errors, preview/resolver ownership, built-in clones, and corrupt cache state. Static scans found no logging, filesystem/environment/process/network/database access in the changed production paths and no raw-error formatting in publisher/resolver/built-in composition. Protected browser/audio changes and unrelated files remain untouched.

Fix-round exact staging audit: `git diff --cached --name-only` contained exactly these 14 authorized changed paths:

- `.superpowers/sdd/2026-08-15-vertical-profile-foundation/task-5-report.md`
- `internal/profile/builtin.go`
- `internal/profile/builtin/fixture.json`
- `internal/profile/builtin_test.go`
- `internal/profile/publisher.go`
- `internal/profile/publisher_test.go`
- `internal/profile/resolver.go`
- `internal/profile/resolver_test.go`
- `internal/prompttmpl/cache.go`
- `internal/prompttmpl/cache_test.go`
- `internal/prompttmpl/engine.go`
- `internal/prompttmpl/engine_test.go`
- `internal/prompttmpl/minijinja.go`
- `internal/prompttmpl/minijinja_test.go`

`git diff --cached --check` passed with no diagnostics. `internal/profile/builtin/system.j2` and `context.schema.json` remain byte-for-byte unchanged; the exact terminal newline was already checked in and is now preserved rather than trimmed. Protected browser/audio files, `.zshrc`, research notes, and the realtime interruption plan remain unstaged.

Fix-round concerns: none.
