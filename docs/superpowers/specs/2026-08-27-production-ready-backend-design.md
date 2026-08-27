# Production-Ready Credential-Free Backend Design

**Date:** 2026-08-27

**Branch:** `codex/production-ready-backend`

**Base:** `codex/live-browser-demo` at `7474789`

## Goal

Produce a release-candidate GPT Live Go framework whose deployable gateway and
worker are pure backend services, whose local process startup requires no
credentials, and whose explicitly configured production paths remain strict,
authenticated, durable, and fail closed.

The repository may retain the loopback browser demo and `/setup` control panel
as development tooling. Neither surface is part of the production gateway,
worker, image, authentication boundary, or release-evidence controller.

This work does not claim credential-free inference. A realtime conversation
still needs a compatible speech service, and durable asynchronous work still
needs PostgreSQL and a configured reasoner. It also does not claim that a code
revision is deployed safely merely because automated tests pass. Production
identity, secrets, TLS, provider access, capacity, canary observation, acoustic
barge-in, and rollback authority remain operator-owned release gates.

## Starting Point and Preservation Rule

The existing `codex/credential-free-backend` worktree contains a reviewed
credential-free startup implementation plus a large inherited, uncommitted
vertical-profile/runtime overlay. The overlay is not a safe integration base:

- it introduces a manager/runtime lock inversion capable of global deadlock;
- its manual audio-commit acknowledgement cannot be serialized by the speech
  adapter;
- provider-VAD input never clears the reconnect partial-input boundary;
- its selected profile is metadata-only and never renders into the provider
  prompt;
- it enables production handoffs without complete production composition; and
- an accidental untracked directory with spaces prevents `go list ./...` and
  the repository test gate from enumerating packages.

That worktree must remain untouched as an audit artifact. No reset, checkout,
clean, or deletion is permitted there. Implementation occurs only in the new
isolated branch created from the reviewed browser/runtime branch.

## Reference Architecture

The design borrows boundaries, not code or runtime dependencies, from:

- Hugging Face `speech-to-speech`: a separately deployed OpenAI
  Realtime-compatible speech endpoint;
- Qwen Audio Agent: an explicit no-backend mode in which the realtime frontend
  remains available while background execution is disabled.

All gateway, protocol, audio, session, profile, worker, deployment, and browser
implementations remain native to this repository. There are no cross-repository
imports, symlinks, launch-time source checkouts, or hidden package installers.

## Process Architecture

### Core gateway

`live-gateway` remains the only public realtime backend.

When invoked without `-config`, it uses a typed built-in local configuration:

- listener `127.0.0.1:8080`;
- advertised model `local`;
- speech endpoint `ws://127.0.0.1:8765/v1/realtime`;
- no provider credential reference;
- no database, durable handoff, OIDC, telemetry, or secret-manager dependency;
- no network connection before a realtime session is opened.

The gateway can therefore start, publish liveness and capabilities, and stop
cleanly with an empty environment. If the local speech endpoint is absent, only
the attempted realtime session fails with a bounded content-free class.

An explicit configuration never inherits this permissive behavior. A named
credential environment variable must exist and contain a nonempty bounded
value. Empty, whitespace, malformed, or missing credentials fail before the
listener becomes ready. Production can never downgrade from OIDC or provider
authentication to the local unauthenticated path.

### Worker

`live-worker --backend none` owns only loopback diagnostics and process
lifecycle. It reads no config file, resolves no environment variable, opens no
database/provider connection, constructs no admin API, and performs no job.

- `/health/live` returns `200`.
- `/health/ready` returns content-free `503`, because no job backend is
  selected.

`live-worker --backend durable -config <path>` remains strict. Supplying a
config without `--backend` keeps backward-compatible durable selection.
`--backend none` plus `-config`, durable without config, unknown backends, and
positional arguments all fail before creating resources.

### Development browser and `/setup`

The canonical loopback BFF remains a separate `demo/live-client` process. It is
not linked into `live-gateway` or `live-worker` and is excluded from production
images and ingress.

Its direct quickstart defaults to the credential-free loopback gateway. The
advanced `/setup` control panel may compose speech, PostgreSQL, migration,
gateway, worker, and browser services only after explicit local actions. It
remains loopback-only, exact-origin, no-store, and content-free at every API
error boundary.

Portable documentation and configuration replace machine-specific repository
paths. A speech source/profile path, when used by the development operator,
must be an explicit validated absolute path plus reviewed revision; no user
home path is checked into the generic quickstart.

## Integration Strategy

The reviewed `codex/live-browser-demo` branch is authoritative for:

- barge-in fencing and continuous interruption tests;
- single-flight audio commits and provider-VAD authority;
- reconnect ownership and profile snapshot binding;
- immutable profiles, prompt rendering, worker recipes, adapters, knowledge,
  Web Search, handoff progress, and durable storage;
- the loopback `/setup` operator and lifecycle safety.

Credential-free backend commits are integrated selectively. The gateway
default, component-scoped secret resolution, idle worker runtime, backend
selection, TLS-validation fix, and worker test-goroutine fix are retained. The
older simplified browser implementation is not cherry-picked over the more
mature canonical BFF; its required no-credential behavior is reintroduced via
tests against the canonical browser boundary.

Conflicts are resolved by contracts, never by preferring an entire side of a
file. Every behavior-changing resolution receives a failing regression first.

## Runtime Hardening

### Locking and lifecycle

There must be one documented lock order across manager-global state, runtime
state, presentation serialization, reconnect rotation, and publication. No
code may acquire `activeMu` while holding a runtime mutex if the inverse order
exists elsewhere. Reconnect, same-scope replacement, handoff presentation,
registration, and shutdown receive deterministic gated race tests.

### Realtime writes and barge-in

Every provider write has an operation deadline independent of session lifetime.
Audio append, response creation, and control cancellation use bounded queues or
separate progress-safe lanes so a stalled audio write cannot delay the
authoritative speech-start fence or cancellation indefinitely.

The server advances the response epoch and rejects stale output before sending
cancel/flush work. Browser playback clear remains secondary UX, not the
authority. Existing continuous-barge tests remain mandatory and are expanded
to cover blocked provider writes, repeated interrupts, omitted terminal events,
and recovery after every cycle.

### Input-turn authority

The negotiated provider-confirmed turn mode remains immutable for a session.

- Provider-VAD mode rejects client commits, binds authoritative provider
  committed events, clears partial-input/reconnect state at the exact causal
  boundary, and cannot carry a partial PCM tail into the next turn.
- Manual mode permits one outstanding commit and releases it only through the
  exact correlated provider acknowledgement.
- Session updates are serialized through actual provider acknowledgements;
  initial bootstrap acknowledgement cannot consume a later browser update.

Adapter-only commands are handled at an explicit internal boundary and are
never accidentally serialized as provider protocol messages.

### Bounds and privacy

Realtime queues are bounded by both event count and retained bytes. Raw frames,
decoded PCM, transcript fragments, provider events, correlation tombstones,
and profile/template material all contribute to declared aggregate budgets.

Client errors and WebSocket close reasons use stable internal codes. Provider,
database, filesystem, URL, prompt, transcript, tool, credential, and response
body details never reach public close reasons, durable progress, telemetry, or
release evidence.

## Profiles, Tools, and Handoffs

An admitted profile is an immutable runtime authority, not metadata.

1. PostgreSQL resolves one exact active profile snapshot for tenant, profile,
   version, engine identity, and purpose.
2. The server renders the reviewed Jinja-compatible system/user prompt bundle
   outside the sole realtime reactor.
3. The first provider session update contains the server-owned rendered prompt.
4. A client cannot replace server-owned instructions, tools, adapter catalog,
   worker objective, or knowledge references.
5. Reconnect and accepted work retain the same snapshot identity and hash.

Tool policy is composed explicitly from the selected deployment catalog and
worker model. There is no silent broad default. Development Web Search keeps
the catalog-enforced strict limits, fixed destination, stable progress phases,
and content-free failure classes.

Production templates keep handoffs disabled until all of these are true:

- the database schema and runtime authority are current;
- the exact reasoner model and thinking-disabled request contract are verified;
- live provider contract evidence is mandatory for the release candidate;
- the worker catalog contains only reviewed compiled adapters; and
- rollback/drain behavior has passed the release gate.

## Provider and Durable Error Handling

Provider-controlled error codes and messages are untrusted content. They are
mapped immediately to a closed internal enum such as timeout, rate-limited,
temporarily unavailable, resource exhausted, invalid response, or transport
failure. Raw provider code/message/body is discarded before retry,
persistence, telemetry, outbox, progress, or client presentation.

The DeepSeek-compatible request always carries the reviewed
thinking-disabled shape. Reasoning content is rejected and never displayed or
persisted. A malicious provider-code sentinel test must prove absence from
PostgreSQL journal/outbox rows, logs, metrics, client events, and evidence.

## Production Deployment Hardening

### Least-privilege secrets

Each pod receives only secrets it actively resolves. The gateway does not
receive worker reasoner, worker privacy, or worker tool-encryption credentials.
Deployment validation rejects both missing required keys and extra
cross-component secrets.

### Proxy and internal traffic

Production proxy configuration explicitly bypasses in-cluster OIDC, OTLP,
Kubernetes service, and loopback destinations while external speech and
reasoner traffic use the reviewed egress path. Render and runtime tests prove
the routing decision; a merely present `NO_PROXY` string is insufficient.

### Readiness

Gateway readiness covers admission, telemetry, durable authority, reconnect
ownership, and a bounded speech dependency circuit. Speech failures trip only
after a threshold and recovery requires stable successes, preventing readiness
storms. Credential-free local mode may remain process-ready before speech is
available because the documented local contract defers provider dialing;
production uses the dependency circuit.

### Shutdown identity

Production preStop reads a rotated projected OIDC token at execution time. It
does not retain a pod-start bearer in the process environment. Signal handling,
authenticated drain, readiness removal, active WebSocket quiescence, worker
lease release, and cleanup are bounded and independently tested.

## Documentation

The root `README.md` starts with:

1. what is deployable: backend gateway plus optional worker;
2. requirements for credential-free process startup;
3. exact three-terminal quickstart for gateway, idle worker, and optional demo;
4. content-free curl checks and stop commands;
5. optional external speech configuration;
6. explicit durable/production configuration;
7. security warning that loopback defaults must never be exposed.

It must not claim the repository contains no frontend while also shipping a
development browser. It instead states that production binaries and images
contain no frontend, while the repository includes separate loopback tooling.

`docs/protocol.md`, security, operations, deployment, and framework skill
references are updated with every public event/configuration change. No active
guide contains a machine-specific path, retired model, dynamic package install,
plaintext secret example, or unsupported production claim.

## Verification and Release Evidence

Every implementation cycle follows red-green-refactor. The final candidate
requires fresh evidence for:

```sh
go test -race -count=1 ./...
go vet ./...
go build ./...
go mod tidy -diff
make verify-deploy-static
govulncheck ./...
git diff --check
```

Additional mandatory gates:

- credential-free gateway/idle-worker process smoke under `env -i`;
- full browser Node suite and loopback BFF security tests;
- PostgreSQL migration/schema/profile/knowledge/handoff integration with zero
  skips;
- speech protocol contract tests, including provider-VAD/manual-mode ordering;
- continuous barge-in race/load tests, blocked-write cancellation, 1,025-cycle
  correlation pressure, byte-budget pressure, reconnect, and two-turn recovery;
- Compose render/health/shutdown tests;
- Kubernetes validation, immutable image/SBOM/provenance checks, and disposable
  cluster gate;
- malicious provider error/privacy persistence test;
- token rotation and proxy-routing tests.

Formal production promotion additionally requires reviewed secrets and identity,
live speech/reasoner provider evidence, physical audible playback and acoustic
barge-in, capacity/chaos/two-hour soak evidence, canary observation, rollback,
and independent privacy signing. Missing external authority is reported as a
blocker, never converted into a skip or a production-ready claim.

## Acceptance Criteria

- `live-gateway` and `live-worker --backend none` start and stop with an empty
  environment and contact no external dependency before use.
- Explicit credentials are strict and nonempty; production cannot downgrade to
  local unauthenticated behavior.
- No P0/P1 finding from the production audit remains.
- P2 security, privacy, lifecycle, and resource-bound findings are either fixed
  or explicitly blocked by external evidence with no unsafe default.
- The canonical vertical-profile, handoff, Web Search, `/setup`, and barge-in
  contracts remain functional and independently bounded.
- Production manifests enforce least privilege, correct proxy routing, rotating
  shutdown identity, and dependency-aware readiness.
- The README quickstart is portable, credential-free for process startup, and
  accurately separates development tooling from production services.
- All required automated gates pass freshly; all external/manual gates are
  recorded honestly.

## Non-Goals

- Embedding or importing Hugging Face or Qwen source.
- Shipping provider credentials, production identities, TLS keys, database
  passwords, or image digests.
- Making inference work without a speech/model backend.
- Exposing `/setup` or the loopback BFF as a production admin/auth proxy.
- Replacing PostgreSQL durable authority with Redis.
- Implementing the previously deferred Redis fast/slow-thinking or automatic
  handoff decision mechanism.
