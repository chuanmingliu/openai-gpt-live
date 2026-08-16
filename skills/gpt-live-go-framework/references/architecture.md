# Architecture reference

## Contents

- Component ownership
- Speech path
- Asynchronous handoff path
- Reconnect and fencing
- PostgreSQL durability
- Feature and security boundaries
- Source map

## Component ownership

| Component | Owns | Must not own |
|---|---|---|
| `live-gateway` | Client auth/origin/admission, public WebSocket, speech provider connection, PCM queues, session reactor, presentation, reconnect, durable coordination | Background model execution or direct side effects |
| `live-worker` | Durable job leasing, DeepSeek streaming, retries, safety evaluation, approvals, registered tools, terminal persistence | Client or speech-provider sockets |
| PostgreSQL | Conversations, committed items, handoffs/events, approvals, leases, outbox, delivery cursors, idempotency, runtime features, reconnect owners, coordinator shard leases, retention | PCM, partial transcription, playback/VAD state, sockets, raw provider deltas |
| Speech provider | Realtime audio/transcription/response generation through the outbound adapter | Client authentication or durable framework state |
| DeepSeek | Streaming async reasoning through Chat Completions/SSE | Direct client presentation or durable authority |

The gateway and worker are independently scalable. `LISTEN/NOTIFY` is a wake-up optimization; durable polling and cursors repair missed notifications.

## Speech path

1. Client requests `GET /v1/realtime`, authenticates to the gateway, supplies an exact allowed Origin when browser policy applies, and optionally names `gateway.advertised_model`.
2. Gateway maps the public model to `gateway.voice.model` and opens the configured `ws://` or `wss://` provider URL.
3. The connector adds the provider model query and bearer header. Configured URLs cannot contain userinfo, query, or fragment.
4. A single reader, writer, and reactor boundary keep session state serialized. PCM queues and control queues are independently bounded.
5. Barge-in/cancel advances the response epoch before later provider output can be presented. Old-incarnation or old-epoch events are suppressed and counted.

The supported media contract is `pcm_s16le`, 24,000 Hz, mono, two bytes/sample, 960-byte full frames. The repository implements an explicit Realtime subset, not every upstream event.

## Asynchronous handoff path

Async delegation requires both:

- Static capability: `features.handoffs: true`, which lets the gateway inject the server-owned tools `start_background_task`, `steer_background_task`, and `cancel_background_task`.
- Runtime authority: the PostgreSQL `handoffs_enabled` feature must be enabled. The authenticated admin endpoint changes it with an audited reason.

Flow:

1. Provider calls a server-injected reserved tool.
2. Gateway validates the exact schema and atomically writes the handoff plus journal/outbox records.
3. Worker claims the job with a lease, builds a bounded DeepSeek request from committed durable state, and streams SSE.
4. Retryable transport, 408, 429, and 5xx failures use bounded full-jitter retry and respect `Retry-After` only within the job deadline.
5. Safety policy suppresses, redacts, or requests durable approval. Side-effecting tool inputs are encrypted and idempotent; no shipped external tools are registered by default.
6. Worker persists progress/terminal state. Gateway replays exact durable suffixes and presents only policy-approved public fields.

Voice continues independently of background work. Barge-in does not cancel a durable handoff; only the explicit cancel tool does.

## Reconnect and fencing

- Normal `session.created` and later `session.reconnect.updated` events carry bounded reconnect metadata.
- Reconnect selects the non-secret URL marker `?reconnect=1`; the first post-upgrade text frame is exactly `{"type":"session.reconnect","credential":"..."}`.
- Credentials bind tenant, user, conversation, session, incarnation, cursor, provider model, expiry, and credential ID with HMAC-SHA256.
- Durable owner compare-and-swap, lease heartbeat, previous-token overlap, exact abort/publish transitions, global expiry cleanup, and operation deadlines prevent duplicate ownership and stranded claims.
- Recovery replays only committed durable state and advances the response epoch after the old provider boundary is quiescent.

## PostgreSQL durability

Embedded schema currently ends at version 8:

1. Core conversations, items, handoffs, approvals, tools, leases, outbox, cursors, admission, deletion.
2. Coordinator journal.
3. Tool-scope constraints.
4. Runtime feature authority.
5. Deletion provenance.
6. Reconnect owner lifecycle.
7. Stable coordinator delivery shards with generation fencing.
8. Persisted handoff context prefix for the worker reasoner.

Coordinator claims are fenced by a stable region-derived shard plus a pod-specific lease owner and monotonically increasing generation. Replacement pods take over only after authority expiry and replay unacknowledged durable work.

## Feature and security boundaries

- With static handoffs off, capability output omits `live.handoff.*`, reserved tools are not injected, and provider calls with the same names are ordinary calls.
- Durable conversations/transcripts/reconnect remain production capabilities even when handoffs are off.
- Production authentication is OIDC-only; local static bearer mode cannot be promoted.
- Admin is loopback and bearer protected. Production metrics use a separate `:9091` listener scraped through a restricted PodMonitor.
- PCM, transcripts, prompts, model reasoning, tool inputs/results, credentials, and private keys never belong in logs, telemetry, or release evidence.
- The exact synthetic identity receives server-authoritative no-persistence behavior; any mismatch follows normal durability.

## Source map

| Concern | Primary files |
|---|---|
| Strict config/defaults/hash | `internal/config/config.go`, `internal/config/hash.go` |
| Gateway composition | `cmd/live-gateway/main.go`, `internal/gateway/app.go` |
| Session state/reconnect | `internal/session/manager.go`, `reactor.go`, `recovery.go`, `reconnect_credentials.go` |
| Public WebSocket/protocol | `internal/transport/ws/`, `internal/protocol/`, `docs/protocol.md` |
| S2S adapter | `internal/realtime/s2sws/` |
| Handoffs/approvals | `internal/handoff/`, `internal/gateway/handoffs.go` |
| DeepSeek/worker | `internal/reasoner/deepseek/`, `internal/worker/`, `cmd/live-worker/main.go` |
| PostgreSQL | `internal/store/postgres/`, `migrations/` |
| Admission | `internal/admission/` |
| Safety/privacy | `internal/safety/`, `internal/telemetry/redact.go`, `docs/security.md` |
| Deploy validation | `deploy/validate/`, `deploy/scripts/`, `deploy/k8s/` |
| Load/chaos/release | `internal/loadgen/`, `test/load/`, `test/chaos/`, `internal/releasegate/` |
