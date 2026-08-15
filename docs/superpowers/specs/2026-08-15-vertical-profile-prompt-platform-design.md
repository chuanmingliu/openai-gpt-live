# Vertical Profile and Prompt Platform Design

## Goal

Extend the loopback `/setup` experience from service lifecycle control into a
safe configuration workspace for deploying the realtime application into a
vertical domain such as insurance sales. An operator can author and publish
versioned prompt templates, named asynchronous worker recipes, reviewed tool
bindings, and bounded knowledge packs. A voice session selects one immutable
published profile before microphone capture begins.

The design uses a native Go, Jinja-style template renderer. Python and Jinja2
are not runtime dependencies. The supplied Python demonstration is reference
material for the compatibility corpus only; it is never executed, imported, or
copied into production.

## Scope

Phase 1 includes:

- versioned vertical profiles with drafts and immutable published versions;
- Jinja-style `.j2` system, opening, per-turn, worker, and fragment templates;
- a documented, deterministic `Jinja-Go v1` language subset;
- named asynchronous worker recipes;
- bindings to a server-owned catalog of reviewed adapters and MCP-backed tools;
- an adapter scaffold for developers;
- versioned knowledge packs for bounded text-bearing documents;
- `/setup` authoring, validation, preview, and publication tabs;
- a safe published-profile selector on the voice client;
- per-session immutable profile snapshots and evidence hashes.

Phase 1 explicitly excludes:

- an automatic handoff-decision or fast/slow-thinking engine;
- Redis caching, speculative retrieval, or retrieval prefetch;
- model-selected prompt fragments or executable skills;
- OCR, URL crawling, shared-drive synchronization, embeddings, or vector search;
- arbitrary browser-supplied MCP configuration, commands, endpoints, or schemas;
- runtime installation of adapter packages;
- a production administration surface.

These exclusions preserve a stable contract so later retrieval and handoff
work can replace internal implementations without changing published profile
semantics.

## Canonical domain language

- **Vertical Profile** — the stable identity of one domain configuration, such
  as `insurance-sales`.
- **Profile Draft** — the only mutable authoring state for a profile.
- **Published Profile Version** — an immutable, validated version that new
  sessions may select.
- **Prompt Bundle** — the templates, context schema, fragments, and fixtures
  referenced by one published profile version.
- **Session Profile Snapshot** — the exact resolved profile version, prompt
  hashes, adapter-registry version, recipe hashes, and knowledge versions pinned
  to one voice session.
- **Worker Recipe** — a named asynchronous-work objective template plus its
  allowed tool bindings, result contract, and execution limits. It is the prompt
  for an asynchronous worker, not the handoff-decision policy.
- **Adapter Binding** — a profile-level restriction of one server-owned adapter
  catalog entry.
- **Knowledge Pack Version** — an immutable, normalized collection of bounded
  excerpts and citation metadata.

These terms are used in storage, APIs, UI text, events, tests, and operational
documentation. Alternative names must not be introduced for the same concepts.

## Authority and runtime architecture

PostgreSQL is authoritative for custom profile drafts, published versions,
prompt content, adapter bindings, and knowledge metadata. Content-addressed
blobs hold immutable template and normalized knowledge content. Browser state,
local storage, query parameters, environment variables, and arbitrary files are
not authorities for published custom profiles.

The loopback BFF serves the `/setup` administration UI and a strict same-origin
API. Publishing validates an exact draft generation and atomically creates a
complete published version. It does not modify the currently active version or
any connected session.

At session establishment:

1. The client submits only an authorized published profile ID and version.
2. The gateway resolves the entire Session Profile Snapshot server-side.
3. The gateway compiles or loads prompt templates by content hash.
4. It renders the session system prompt and opening message from the validated
   server-owned context.
5. It sends the exact provider configuration and profile-selection event.
6. Microphone capture remains disabled until both the profile selection and
   provider configuration are acknowledged.

The browser cannot override profile-owned prompts, worker recipes, tools,
knowledge versions, provider URLs, or models. Reconnect retains the exact
snapshot rather than resolving the latest profile version. Durable handoffs
store the snapshot identifiers and hashes required to reproduce their worker
instructions and evidence boundary.

A checked-in built-in profile remains available for the current voice-only demo
without PostgreSQL. Creating, editing, or publishing custom profiles requires
the durable PostgreSQL deployment. If vertical profiles are enabled while the
schema or adapter catalog is incompatible, readiness fails closed.

## Realtime and barge-in invariants

This feature must preserve the interruption hardening already implemented in
the realtime path. Profile resolution and session-level rendering occur before
audio admission. Per-turn rendering, knowledge lookup, adapter execution, and
worker dispatch never execute synchronously on the sole realtime reactor.

The following invariants are load-bearing acceptance requirements:

- the authoritative output epoch advances before provider cancel or browser
  flush on barge-in;
- stale audio, transcripts, provider completions, prompt results, and tool
  results are rejected at their final egress or mutation boundary;
- provider cancel correlation and typed completion lanes continue to make
  progress under bounded backpressure;
- repeated or continuous barge-in is monotonic and idempotent: it does not
  duplicate cancellation, terminal events, handoffs, or worker submissions;
- client and durable I/O remain outside the reactor apply path;
- a slow template render, knowledge lookup, worker, adapter, database call, or
  browser write cannot delay the speech-start fence;
- cancellation of live presentation does not cancel already accepted durable
  work unless the user performs the separate explicit task-cancel operation;
- the provider-confirmed input-turn mode and single-flight audio-commit contract
  remain unchanged.

Any new asynchronous operation carries the session incarnation, profile
snapshot hash, input revision, and output epoch needed to reject a late result.
No implementation may trade these fences for lower apparent latency.

## Prompt bundle and template language

### Logical bundle

Each published Prompt Bundle has this logical structure:

```text
prompt-bundle/
  system.j2
  opening_message.j2
  turn_instruction.j2
  workers/*.j2
  fragments/*.j2
  context.schema.json
  fixtures/*.json
```

These are logical names stored as content-addressed records. Runtime rendering
does not read operator-selected filesystem paths.

### `prompttmpl.Engine`

All rendering goes through an internal Go interface, `prompttmpl.Engine`, so
the parser implementation can change without changing profile or runtime
contracts. The selected Go library must pass the compatibility and security
corpus before adoption. The engine is identified in every published profile as
`jinja-go-v1` plus an engine implementation version.

`Jinja-Go v1` supports:

- scalar variables, typed maps, typed lists, and bounded indexing;
- `if`, `for`, and deterministic `set` statements;
- `is defined`, `none`, comparisons, and boolean expressions;
- whitespace-control syntax;
- a small allowlist of pure deterministic filters;
- macros and static includes resolved within the published bundle;
- Unicode and Chinese text.

It rejects:

- arbitrary function or method calls and object mutation;
- reflection and access to files, environment, network, processes, or clocks;
- dynamic includes and paths outside the bundle;
- unbounded recursion, collection growth, or output;
- nondeterministic operations such as `random`;
- runtime registration of executable filters;
- model-authored or customer-authored template source.

The supplied dynamic Jinja demonstration uses Python-only constructs such as
mutable namespaces, method calls, and random selection. Publication reports
those constructs as unsupported. Equivalent business calculations and parsing
belong in typed Go context builders. A dynamic skill becomes a trusted,
published fragment selected by a stable fragment ID, never executable template
behavior.

### Rendering lifecycle

- `system.j2` renders once when the session snapshot is established and becomes
  server-owned session instructions.
- `opening_message.j2` renders an assistant opening response. It never creates a
  fabricated user message.
- The finalized user transcript remains the user message.
- `turn_instruction.j2` renders a response-level instruction for that user turn.
  A profile using it is compatible only with an adapter that supports explicit
  per-response instructions; otherwise profile publication or session start
  fails closed.
- A handoff selects exactly one named Worker Recipe. Its `.j2` objective renders
  from the immutable session and handoff evidence snapshot.

Every render uses strict undefined values, bounded input collections, bounded
fuel/time, bounded include depth, and bounded UTF-8 output. A per-turn render is
performed in a supervised worker boundary and returns a typed completion. If it
misses its deadline or becomes stale, no response is started with old or partial
instructions.

## Typed render context

`context.schema.json` declares every value visible to templates:

- stable field name and type;
- whether it is required;
- scalar length, collection count, and aggregate byte limits;
- trusted source and lifecycle;
- sensitivity class;
- templates allowed to access it.

Optional values are explicitly `null` or empty. An undeclared or missing
required value is an error. Context builders clone and validate their inputs;
templates never receive mutable application objects, credentials, provider raw
payloads, database handles, or arbitrary metadata.

Each bundle includes synthetic fixtures. Publication compiles all templates,
validates all fixture contexts, renders expected outputs, and reports exact
content-free error locations. Fixtures must not contain customer data, real
credentials, or production knowledge.

## Worker recipes

A Worker Recipe contains:

- stable recipe ID and version;
- objective template hash;
- allowed Adapter Bindings;
- required and optional evidence schema;
- maximum objective, evidence, tool-result, and final-result sizes;
- execution, tool, and overall deadlines;
- approval requirements for side-effecting tools;
- final structured result schema.

The objective template is the asynchronous worker prompt. It is not a prompt
for deciding whether a handoff should occur. In Phase 1, the existing explicit,
server-owned handoff path selects a recipe by ID. Automatic handoff selection is
deferred.

Worker execution is isolated from the voice session. A queue or worker failure
updates the durable task and presentation card but does not terminate voice.
Accepted work keeps its immutable recipe and profile snapshot even if a new
profile version is published or the caller barges in. Explicit task cancellation
uses the durable cancellation path and idempotency contract.

## Adapter and MCP catalog

The browser configures only Adapter Bindings to entries already installed in a
server-owned catalog. A catalog entry declares:

- adapter ID, version, name, and description;
- stable tool IDs and input/output schemas;
- read-only or side-effecting classification;
- approval and idempotency requirements;
- operation deadlines, concurrency, and result bounds;
- fixed destination identity and transport class;
- secret-reference names, never secret values;
- health behavior and registry version.

A profile may disable tools or tighten their bounds. It cannot loosen catalog
policy or submit an endpoint, executable command, package name, schema, secret,
environment variable, or raw MCP JSON. Publication pins the deployed adapter
registry version, and a missing or incompatible entry fails publication or new
session resolution.

Side-effecting tools require durable approval, encrypted stored arguments,
idempotency, and exact completion correlation. Read-only and side-effecting
results share bounded structured-result validation. Tool failure fails the task,
not the voice session.

The repository will expose a reviewed developer command such as:

```text
make adapter-scaffold NAME=policy_lookup
```

The scaffold creates native interfaces, manifest validation, local fakes,
privacy tests, timeout/cancellation tests, and registration instructions.
Developers implement and deploy the adapter in the current repository; it does
not link to reference repositories. An MCP-backed adapter additionally pins its
transport, server implementation, method allowlist, schemas, restart policy,
health check, and secret references. The deployment installs it; `/setup` never
runs `npx` or downloads code.

## Knowledge packs

Phase 1 accepts bounded UTF-8 text, Markdown, text-bearing PDF, and DOCX files.
Upload validation checks declared extension, file signature, size, archive
expansion, paths, UTF-8 normalization, and tenant ownership. Scanned or
encrypted documents return `unsupported_content` rather than silently producing
empty knowledge.

Ingestion is a bounded pipeline:

1. validate and classify the upload;
2. extract text with a reviewed parser;
3. normalize it deterministically;
4. split it into bounded excerpts;
5. attach source title, page/section, excerpt hash, classification, and citation
   metadata;
6. require operator preview and explicit publication.

Updates create immutable Knowledge Pack Versions. PostgreSQL stores normalized
content, hashes, citations, classification, and publication metadata. Raw
uploads are discarded after successful extraction by default. Every record is
tenant-scoped; shared packs cannot contain customer-specific PII.

Workers access knowledge only through the compiled read-only
`knowledge.search` adapter. Phase 1 uses deterministic bounded lexical retrieval
with CJK-aware token and n-gram matching. Results contain a bounded number of
excerpts, citations, and a coarse confidence class. Redis or vector retrieval
may later replace this implementation without changing the adapter contract.

Deletion first tombstones a version so new profiles cannot bind it, then applies
the configured retention policy. Connected sessions and accepted tasks retain
only their already authorized immutable snapshot semantics.

## `/setup` experience

The existing Runtime control panel remains available. The approved profile
workspace adds these tabs:

1. **Runtime** — existing service lifecycle controls.
2. **Profile** — identity, description, compatibility, and active versions.
3. **Prompts** — bundle file list, template editor, context schema, fixture
   preview, and compiled output.
4. **Worker recipes** — named recipe objectives, evidence schemas, limits, and
   adapter bindings.
5. **Tools & adapters** — reviewed catalog entries and profile-level
   restrictions.
6. **Knowledge** — upload, extraction preview, citation inspection, versioning,
   publication, and tombstones.
7. **Review & publish** — validation summary, hashes, compatibility, generation,
   and atomic publication.

Draft saves use an exact generation/ETag. A stale save returns HTTP 409
`stale_draft`; it never overwrites newer work. Publication names the exact draft
generation and returns the immutable published version and hashes. Safe button
states and compatibility summaries are derived server-side.

The voice page receives a selector containing only safe metadata for authorized
Published Profile Versions. Selection is immutable after establishment. Capture
waits for the exact `live.profile.selected` event and provider configuration
acknowledgement.

## Administration API

The conceptual API is:

```text
/api/setup/v1/profiles
/api/setup/v1/profiles/{id}/draft
/api/setup/v1/profiles/{id}/templates/{path}
/api/setup/v1/profiles/{id}/validate
/api/setup/v1/profiles/{id}/preview
/api/setup/v1/profiles/{id}/publish
/api/setup/v1/adapter-catalog
/api/setup/v1/knowledge-packs
/api/setup/v1/knowledge-packs/{id}/versions
```

The implementation uses exact request and response schemas, strict HTTP
methods, strict JSON and media types, same-origin checks, bounded bodies, bounded
path segments, bounded list counts, and server-owned timeouts. It remains bound
to loopback for the demo.

Responses never contain credentials, environment values, commands, provider
URLs, raw model output, customer data, full prompt content outside explicit
editor/preview authorization, or extracted material outside explicit knowledge
preview authorization. Configuration is not stored in cookies, local storage,
query parameters, URL fragments, or browser logs. A production administration
surface requires a separate security and identity review.

## Storage and publication

The durable schema adds tenant-scoped records for profiles, draft generations,
published profile versions, template blobs, prompt bundles, worker recipes,
adapter bindings, knowledge packs, knowledge versions, and publication audit
metadata. The implementation plan will assign the next migration number after
the repository's current schema; this design does not assume a hard-coded
number.

Publication is one PostgreSQL transaction that:

1. locks and verifies the exact draft generation;
2. validates template compilation and fixtures;
3. validates adapter catalog and provider compatibility;
4. validates referenced knowledge versions and tenant ownership;
5. materializes the canonical profile manifest;
6. hashes every component and the aggregate snapshot;
7. creates an immutable published version;
8. optionally marks it active for new sessions.

Content-addressed blobs are immutable and deduplicated within their permitted
tenant boundary. Hashes are integrity identifiers, not authorization tokens.
Authorization is always checked independently.

## Failure handling and rollback

Stable error classes include:

- `stale_draft`
- `template_invalid`
- `context_invalid`
- `render_timeout`
- `render_limit_exceeded`
- `profile_incompatible`
- `profile_revoked`
- `adapter_unavailable`
- `knowledge_unavailable`
- `publication_conflict`
- `unsupported_content`

Errors are content-free unless the caller is explicitly authorized for the
corresponding draft or preview. Logs contain identifiers, hashes, bounded timing,
and error classes only—not prompts, knowledge excerpts, customer content,
credentials, raw provider payloads, or reasoning.

Published versions are immutable. An operator activates a prior or newer
version for future sessions. Revocation prevents new sessions and reconnects
from selecting that version; already connected sessions retain their snapshot
until disconnect unless an operator invokes a separate emergency termination.
Draft, validation, preview, ingestion, or publication failure never changes the
active version.

An initial profile or rendering failure occurs before microphone admission. A
per-turn rendering failure suppresses that response rather than reusing stale or
partial instructions. A worker, adapter, or knowledge failure updates only the
durable task. Evidence and tool results cannot alter profile policy, recipe
identity, or adapter allowlists.

## Security and resource bounds

- Only authorized editors may view or mutate drafts and previews.
- Runtime values are data, never template source or executable configuration.
- AST validation rejects calls, mutation, dynamic includes, unsupported nodes,
  recursion, and nondeterminism.
- Render context bytes, collection counts, include depth, operations, time, and
  UTF-8 output all have exact limits.
- The compiled-template cache is bounded, keyed by content and engine hashes,
  and safe to lose; a miss recompiles from immutable content.
- Uploads and archive extraction have exact per-file, per-request, expansion,
  excerpt-count, and aggregate-byte limits.
- Adapter results, worker evidence, and final results have exact schemas and
  byte limits.
- Every durable row and lookup is tenant-scoped.
- Synthetic fixtures contain no production data.

## Delivery slices

Implementation proceeds in independently reviewable slices:

1. Build the `prompttmpl.Engine` interface and the sanitized Jinja-Go
   compatibility/security corpus; choose the implementation that passes it.
2. Add the profile domain, PostgreSQL schema, store, draft concurrency, and
   atomic publication.
3. Integrate immutable session selection, system/opening/turn rendering, capture
   gating, reconnect behavior, and barge-in fencing into the gateway.
4. Add Worker Recipe snapshotting and objective rendering.
5. Add the adapter catalog, adapter scaffold, and reviewed binding validation.
6. Add bounded knowledge ingestion and deterministic `knowledge.search`.
7. Add the `/setup` authoring API/UI and the main-page published-profile
   selector.

Each slice has its own RED-to-GREEN tests and independent review. A slice does
not weaken earlier lifecycle, privacy, interruption, or durability gates.

## Testing and acceptance

### Automated tests

- Golden compatibility tests use a sanitized form of the supplied demonstration
  and cover supported syntax, explicit unsupported syntax, strict undefined
  values, Unicode, whitespace, and deterministic output.
- Parser, AST, context, API path, upload, and archive boundaries are fuzzed.
- Exact render, include, collection, upload, extraction, worker, adapter, and
  knowledge limits are tested at the boundary and one past it.
- PostgreSQL integration tests cover draft conflicts, atomic publication,
  rollback, tenant isolation, revocation, tombstones, and retention transitions.
- Gateway tests prove no microphone PCM before profile and provider acks;
  browser prompt/tool overrides are rejected; v1 sessions stay on v1 after v2
  publication; reconnect retains v1; and revocation blocks new selection and
  reconnect.
- Worker tests prove exact recipe/profile/catalog/knowledge hashes, bounded
  evidence and results, side-effect approval, idempotency, and voice isolation.
- Browser tests cover every tab, template editing, fixture preview, stale saves,
  validation, publication, profile selection, immutable selection, and safe
  failure states.
- Privacy and static scans prove that secrets, prompts, materials, customer
  content, raw tool/provider payloads, and reasoning do not enter logs, URLs,
  browser storage, or generic error responses.
- Repository gates include ordinary and race tests, vet, build, module
  consistency, static deployment validation, formatting, and diff checks.

### Barge-in and latency regression tests

- Hold template, safety, client-write, knowledge, worker, and adapter completions
  independently; inject speech start; verify the output fence advances before
  releasing any gate.
- Run deterministic continuous-barge scenarios with pending and streaming
  responses, slow client writes, blocked provider cancel, duplicate speech
  edges, late transcripts/audio/done, and concurrent durable handoff.
- Assert no old-epoch audio or transcript after each fence, no duplicate
  terminal/handoff/task, bounded queue high-water marks, progress within the
  established deadlines, and clean worker teardown.
- Run the interruption matrix repeatedly under the race detector. Tests use
  deterministic gates and counters rather than wall-clock sleeps or global
  goroutine counts.

### Manual acceptance

1. Start the interactive demo and open `/setup`.
2. Create and publish an insurance-sales profile with templates, a worker
   recipe, reviewed adapters, and a small knowledge pack.
3. Select it on the voice page and confirm microphone capture remains disabled
   until the exact selection and provider configuration are acknowledged.
4. Hear the rendered opening, converse, interrupt repeatedly, and confirm stale
   speech never resumes.
5. Start an asynchronous task and verify bounded cited knowledge results without
   interrupting voice.
6. Publish v2 while a v1 session is connected; confirm v1 stays unchanged and a
   new session can select v2.
7. Reconnect the v1 session and verify its exact snapshot; revoke a version and
   verify new selection/reconnect fails closed.

Automated tests do not claim acoustic quality, actual provider behavior, or
production database readiness. Audible interruption, live provider calls, and a
real PostgreSQL deployment remain explicit acceptance evidence.

## Deployment and compatibility

Vertical profiles are feature-gated. With the feature disabled, the built-in
voice-only profile and current demo behavior remain unchanged. With it enabled,
readiness requires the compatible schema, template-engine version, and adapter
catalog.

The Session Profile Snapshot records engine version, profile version, prompt and
recipe hashes, adapter-registry version, knowledge versions, and aggregate
snapshot hash. The runtime configuration hash remains separate so operators can
distinguish content publication from deployment configuration.

Rolling back to an older binary preserves the checked-in built-in profile. It
does not silently flatten, partially render, or reinterpret newer custom
profiles; unsupported profiles become unavailable until a compatible binary is
restored. No Python interpreter or Python package is introduced into runtime or
deployment images.

## Success criteria

The feature is complete when a local operator can safely author, preview,
publish, select, and roll back a versioned vertical profile; the gateway and
worker consume one immutable snapshot; templates render deterministically in
native Go; adapters and knowledge remain reviewed and bounded; browser input
cannot widen server policy; and the existing low-latency barge-in, durability,
privacy, and lifecycle guarantees remain demonstrably intact.
