# Credential-free backend startup design

**Date:** 2026-08-23

## Goal

Make the repository start as a backend framework without requiring credentials,
PostgreSQL, or an available inference provider. The default gateway remains an
OpenAI Realtime-compatible backend. The worker gains an explicit no-backend
runtime that stays alive without claiming it can process durable jobs.

The credential-free path is for local development and framework integration. It
must not weaken an explicitly configured authenticated or production runtime.

## Startup contract

The repository will support these commands:

```sh
# Local backend gateway. No config file or environment variable is required.
go run ./cmd/live-gateway

# Intentional worker with no durable backend. No config or credential is required.
go run ./cmd/live-worker --backend none

# Existing durable worker behavior. Configuration and dependencies are required.
go run ./cmd/live-worker --backend durable -config configs/worker.example.yaml
```

For backward compatibility, `live-worker -config <path>` continues to select the
durable backend when `--backend` is omitted. Existing gateway invocations with
`-config <path>` continue to use the supplied file.

Credential-free startup does not mean credential-free inference. A gateway
session still needs a reachable speech-to-speech backend. A durable worker still
needs PostgreSQL and its selected reasoner credentials.

## Non-goals

- Do not add a frontend, static assets, a browser server, or microphone handling.
- Do not embed or launch Hugging Face `speech-to-speech` or Qwen Audio Agent.
- Do not add credential hot reload, a credential HTTP endpoint, or a secret store.
- Do not make the durable worker useful without PostgreSQL.
- Do not relax production OIDC, TLS, PostgreSQL, HMAC, image, region, or provider
  requirements.
- Do not merge or modify the separate browser-demo worktree.

## Reference patterns

The design adapts two first-party patterns recorded in
`docs/research/backend-reference-patterns.md`:

- Hugging Face `speech-to-speech` exposes a loopback OpenAI Realtime-compatible
  `/v1/realtime` endpoint and supports a fully local model path with an empty
  provider credential.
- Qwen Audio Agent distinguishes an absent backend from an owned or external
  backend. Its explicit `--backend none` mode lets the gateway run without
  pretending that background execution is available.

These are architectural references only. The Go service retains its current
protocol, audio, session, and deployment implementations.

## Architecture

### Core gateway

`live-gateway` remains the core backend process. Its existing provider-neutral
`realtime.Connector` seam and `s2sws` adapter remain unchanged.

When no `-config` flag is supplied, the command constructs a built-in local
configuration with these non-secret values:

| Setting | Default |
|---|---|
| Mode | `local` |
| Gateway listener | `127.0.0.1:8080` |
| Public model alias | `local` |
| Speech provider | `speech_to_speech` |
| Speech URL | `ws://127.0.0.1:8765/v1/realtime` |
| Speech model | `local` |
| Provider credential reference | omitted |
| Client authentication | omitted |
| PostgreSQL/durability | omitted |
| Worker/reasoner credential references | omitted |
| Handoffs and side-effecting tools | disabled |

The default binds only to literal loopback. It uses the existing local
development admission identity and is not an authenticated deployment mode.
Changing the listener to a non-loopback address requires an explicit config file
and remains operator-owned.

Provider construction performs no network dial. Therefore the HTTP server and
`GET /v1/realtime/capabilities` can start while the configured local speech
backend is absent. The first realtime session performs the provider dial. A
failed dial terminates that session with the existing sanitized provider error;
it does not stop the gateway process.

### Component-scoped secret resolution

The gateway composition root will stop calling the broad
`Config.ResolveSecrets` method over the complete shared configuration. That call
currently forces the gateway to resolve worker-only reasoner, privacy, retention,
database, and tool credentials even when local gateway composition never uses
them.

Each active gateway module already owns or will own its credential lookup:

- the speech adapter resolves the configured voice credential;
- local static admission resolves the configured bearer;
- production admission and durability resolve PostgreSQL;
- production reconnect resolves its HMAC key;
- production feature/durability composition resolves the retention material it
  uses.

An explicitly configured credential remains strict. If a config names a voice or
static-auth environment variable and that variable is missing, startup fails
rather than silently downgrading to an unauthenticated mode. The credential-free
default works because it names no credential, not because errors are ignored.

Production continues to validate its full required composition before listening.
No production check is moved to request time.

### Worker backend selection

`live-worker` gains a small backend selector at its composition root:

```text
none     -> idle worker runtime
durable  -> existing PostgreSQL + reasoner runtime
```

The selector is intentionally smaller than a provider registry. There are only
two real implementations, so a wider adapter framework would be hypothetical.
The existing `serviceRuntime` interface is the seam: both the idle runtime and
the existing production runtime implement `Run(context.Context) error` and
`Close()`.

The CLI rules are:

- `--backend none` rejects `-config`; it performs no config read or environment
  lookup.
- `--backend durable` requires `-config` and follows the existing strict startup
  path.
- omitting `--backend` while supplying `-config` selects `durable` for backward
  compatibility.
- omitting both remains a usage error. The quickstart names `--backend none`
  explicitly so an idle worker is never accidental.
- unknown backend values and positional arguments fail before any listener or
  dependency is created.

### Idle worker runtime

The idle runtime owns only process lifecycle and loopback diagnostics. It does
not construct a PostgreSQL pool, migration checker, repository, DeepSeek client,
runner, publisher, retention sweeper, feature store, telemetry exporter, or
admin handler.

It listens on `127.0.0.1:9091` and exposes:

| Endpoint | Result | Meaning |
|---|---|---|
| `GET /health/live` | `200` | The process and diagnostics loop are alive. |
| `GET /health/ready` | `503` | No durable backend is selected; the worker cannot accept jobs. |

The readiness response remains content-free and does not reveal configuration,
credentials, provider state, or filesystem paths. Returning `503` is deliberate:
the process is healthy, but it must not claim database or provider readiness.

The listener is fixed to loopback in this change. A listener bind failure is a
startup error. Context cancellation or `SIGTERM` closes the diagnostics server
within the existing cleanup bound and returns success from the command.

## Interfaces and data flow

Credential-free gateway flow:

```text
CLI without -config
  -> built-in typed local config
  -> structural validation
  -> unauthenticated loopback gateway composition
  -> HTTP/WebSocket listener starts
  -> a client opens /v1/realtime
  -> s2sws dials the configured local speech backend
```

Idle worker flow:

```text
CLI --backend none
  -> validate flag combination
  -> construct idle runtime
  -> bind loopback diagnostics
  -> remain alive until cancellation
```

Durable worker flow remains:

```text
CLI [-backend durable] -config <path>
  -> strict config parse
  -> strict secret resolution
  -> PostgreSQL and migration readiness
  -> reasoner/repository/worker composition
  -> durable loops start
```

No new protocol event, audio format, persistence record, public route, or worker
job schema is introduced.

## Error handling

- Invalid built-in defaults are treated as a programmer error covered by tests;
  the command still returns a sanitized configuration error rather than panics.
- An unavailable speech backend affects the opening realtime session, not gateway
  liveness.
- Explicit config files retain strict unknown-field and secret-reference checks.
- Idle-worker invalid flags fail immediately and do not fall through to durable
  secret resolution.
- Idle diagnostics return method-not-allowed for non-GET health requests and 404
  for every other path.
- No error includes a secret value, database URL, provider response body, or raw
  WebSocket content.

## Security properties

- Credential-free defaults are loopback-only.
- No default credential, placeholder token, generated key, or `.env` file is
  introduced.
- No missing credential causes a configured authenticated mode to become
  unauthenticated.
- The idle worker mounts no admin operations.
- Production configuration, deployment validators, and immutable config hashing
  remain authoritative.
- The browser-demo worktree and untracked visual artifacts remain outside this
  backend runtime and outside task commits.

## Compatibility and migration

- `live-gateway -config <path>` remains supported.
- `live-worker -config <path>` remains the existing durable invocation.
- Compose and Kubernetes continue to pass their existing config paths and retain
  durable behavior.
- Existing provider, session, protocol, storage, and deployment interfaces do not
  change.
- The new gateway default is additive; scripts that previously asserted that
  missing `-config` is an error must be updated to assert the built-in local
  startup contract instead.

## Documentation

`README.md` will be rewritten to lead with the backend-only, credential-free
quickstart:

1. start `live-gateway` in one terminal;
2. optionally start `live-worker --backend none` in another;
3. inspect gateway capabilities and worker liveness/readiness;
4. optionally start Hugging Face `speech-to-speech` on loopback for real voice;
5. move to explicit gateway and durable-worker configuration only when external
   dependencies are needed.

The README will state that the default service is not safe to expose beyond
loopback and that an idle worker cannot execute jobs.

## Test strategy

Implementation follows red-green-refactor. Tests are added before production
changes and are run once to confirm the expected failure.

### Gateway tests

- The built-in gateway configuration validates with an empty environment lookup.
- It binds to loopback, names no credential, disables durable features, and points
  to the local `/v1/realtime` endpoint.
- Running without `-config` reaches runtime composition instead of returning the
  former `-config is required` error.
- A supplied shared local config does not cause gateway lookup of worker-only
  secret references.
- Supplied gateway credential references remain strict.

### Worker tests

- `--backend none` performs zero config reads, environment lookups, or durable
  builder calls.
- The idle runtime remains blocked until cancellation, then closes cleanly.
- Idle liveness returns 200 and readiness returns 503.
- Unknown backends, `none` plus `-config`, and durable mode without a config fail
  before runtime construction.
- `-config <path>` without an explicit backend still resolves secrets and builds
  the durable runtime.

### Repository verification

- Run focused command/config/idle-runtime tests first.
- Build all commands and run `go vet`.
- Run the full repository tests and report any failures separately from the
  already observed failures in the pre-existing uncommitted gateway/session work.
- Run `git diff --check` and inspect the diff so task commits include only the
  design/research and implementation files.
- Perform a credential-free process smoke with a clean environment: verify the
  gateway capabilities endpoint, idle-worker liveness, idle-worker non-readiness,
  and bounded shutdown without starting PostgreSQL or a provider.

## Expected file changes

- `internal/config/` for the built-in local gateway configuration and its tests.
- `cmd/live-gateway/main.go` and tests for optional config and component-scoped
  secret resolution.
- `cmd/live-worker/main.go`, worker tests, and a focused idle-runtime module for
  backend selection and diagnostics.
- `README.md` for the backend-first quickstart and explicit durable path.
- No frontend, static, image, database migration, or deployment-manifest change.
