# Browser Demo Control Panel Design

## Goal

Add a loopback-only `/setup` page that lets a local operator start, stop, and
inspect each part of the realtime demo in dependency order without reconstructing
terminal commands. The existing `/` voice client remains unchanged.

## User flow

The page presents five fixed service cards:

1. **Speech** — readiness, Start, and Stop for the reviewed local streaming
   speech service.
2. **PostgreSQL** — readiness, Start, Migrate, and Stop for the disposable
   durable-demo database.
3. **Gateway** — readiness, Start, and Stop, with the fixed local speech
   endpoint and advertised model.
4. **Worker** — readiness, Start, and Stop; marked optional for voice-only use
   and required for durable asynchronous tools.
5. **Client** — BFF readiness, Start, Stop, and Open voice demo. Open is enabled
   only when Speech, Gateway, and BFF are ready.

Every card shows `stopped`, `starting`, `ready`, `stopping`, or `failed` plus a
bounded content-free diagnostic. Buttons are disabled while their service has
an operation in flight. The page polls a content-free status snapshot at a
bounded interval.

## Architecture

The existing loopback BFF serves `/setup` and a small same-origin operator API.
The API accepts only fixed action and component enums; it never accepts a shell
command, path, URL, environment-variable name, model, credential, or arbitrary
argument. A process supervisor owns only processes it starts and records their
PIDs in the existing private wizard state directory.

The supervisor invokes reviewed, fixed repository entry points:

- speech: the checked-in speech launcher/profile selected by server-side
  operator configuration;
- PostgreSQL/migration: the existing durable Compose wrapper and targets;
- gateway and worker: the existing durable Compose services;
- client: the current BFF lifecycle.

Status combines owned-process state with bounded readiness checks. A successful
HTTP request means only local process readiness, not provider health or audible
acceptance.

## Secret boundary

The browser never receives or submits gateway, database, speech, DeepSeek,
MiniMax, HMAC, or encryption credentials. Required values must already exist in
the server-side operator environment or private local configuration. Missing
values produce only an allowlisted missing-variable class; values are never
returned.

The operator API binds to `127.0.0.1`, enforces same-origin requests, rejects
non-JSON and unknown fields, caps request bodies, and uses POST for mutations.
It emits no raw process logs, environment, command lines, transcripts, audio,
provider payloads, or database URLs. It starts no tunnel and exposes no public
listener.

## Lifecycle and dependencies

- Speech can start independently.
- PostgreSQL must be ready and migrated before durable gateway/worker startup.
- Gateway requires Speech and PostgreSQL readiness.
- Worker requires PostgreSQL readiness but is not required for voice-only use.
- BFF requires Gateway readiness.
- Stop refuses unsafe dependency removal by default: stopping Speech or
  PostgreSQL while dependants are running returns a conflict with the exact
  dependant classes. The operator stops dependants first.
- Browser refresh or disconnect does not stop services. Only explicit actions
  do.
- Shutdown never deletes the PostgreSQL volume.

## Failure handling

Each action has one in-flight slot and a bounded deadline. Duplicate actions
return the current operation state. A failed command becomes `failed` with an
allowlisted code such as `missing_configuration`, `dependency_not_ready`,
`startup_timeout`, or `process_exited`; raw stderr is retained only in the
operator terminal, not sent to the browser. Partial startup stops only the
newly owned process for that action.

## Testing and acceptance

- Unit tests cover the state machine, dependency ordering, duplicate actions,
  timeouts, partial cleanup, and ownership-safe stops.
- HTTP tests cover loopback/same-origin enforcement, exact schemas, body caps,
  action allowlists, concurrent actions, and content-free responses.
- Browser tests cover card states, button enablement, polling, failure notices,
  and Open voice demo gating.
- Hostile tests prove secrets, commands, paths, URLs, process logs, transcript,
  and audio cannot enter API responses or browser assets.
- Existing BFF, realtime audio, tool-policy, wizard, deployment-static, race,
  and privacy gates remain green.

This control panel is a local development convenience. It is not a production
admin surface, deployment controller, or release-evidence producer.
