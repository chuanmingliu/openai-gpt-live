# Configuration reference

## Contents

- Parsing and validation semantics
- Speech-to-speech mapping
- Asynchronous DeepSeek mapping
- Handoff enablement
- Defaults
- Secret inventory
- Production inputs
- Immutable config hash
- Validation commands
- Configuration failures

## Parsing and validation semantics

Configuration is strict YAML. Unknown fields and likely plaintext-secret fields fail. Fields ending in `_env` contain environment-variable names, never values. Duration fields use Go syntax such as `250ms`, `30s`, and `2h`.

`check-config` and `config-hash` call the unresolved loader: they parse, default, validate names and structure, but do not prove secret presence, database connectivity/schema, provider authentication, OIDC discovery, telemetry reachability, or runtime composition. Actual component startup performs those checks.

The gateway uses the broad shared-config secret resolver. With the combined deploy config, it also requires named worker secrets such as `DEEPSEEK_API_KEY` and tool/privacy keys. The worker resolves only its component secrets.

Production authentication consumes exactly one strict bearer from the HTTP `Authorization` header. Native clients can set it directly; the standard browser `WebSocket` API cannot. The repository ships no cookie, query-token, subprotocol-token, or auth-bootstrap mechanism, and the nginx Ingress is not an auth proxy. Browser use therefore requires a separately reviewed BFF/authentication proxy that injects the OIDC bearer. Never place credentials in the URL.

The generic runtime may allow missing Origin only with explicit `auth.allow_non_browser: true`. The shipped Kubernetes production topology is browser-facing and its validator requires the exact ingress Origin, so non-browser-only production needs a coordinated topology/validator change.

## Speech-to-speech mapping

```yaml
gateway:
  advertised_model: my-public-live-model
  voice:
    role: realtime_voice
    provider: speech_to_speech
    url: wss://speech.example.com/v1/realtime
    model: deepseek-v4-flash
    api_key_env: S2S_API_KEY
  audio:
    sample_rate: 24000
    channels: 1
    bytes_per_sample: 2
    frame_bytes: 960
```

- `advertised_model` is the client-facing alias.
- `voice.model` is the provider model. The gateway translates the alias and reconnects with this trusted value.
- Configure only the base Realtime WebSocket endpoint. The connector adds `?model=<voice.model>` and `Authorization: Bearer <S2S_API_KEY>`.
- URL userinfo, query, fragment, and opaque form fail. Production requires `wss://`; local accepts `ws://`.
- Provider connect timeout is currently fixed at 10 seconds.
- `role` and `provider` are labels; they do not select another implementation. Gateway currently constructs `s2sws`.
- The selected speech service must configure `deepseek-v4-flash` with thinking
  disabled. That provider-specific service setting is external to the Go
  connector; verify it from the reviewed service configuration and startup.

## Asynchronous DeepSeek mapping

```yaml
worker:
  concurrency: 16
  lease_duration: 30s
  heartbeat_interval: 10s
  poll_interval: 1s
  request_timeout: 30s
  retry_base: 1s
  retry_max: 30s
  max_retries: 3
  max_output_bytes: 1048576
  max_output_tokens: 8192
  max_tool_rounds: 8
  reasoner:
    role: async_gpt_5_5
    provider: deepseek
    base_url: https://api.deepseek.com
    model: deepseek-v4-flash
    api_key_env: DEEPSEEK_API_KEY
```

- The client appends `/chat/completions` to `base_url`; do not append it twice.
- Production requires HTTPS and rejects credentials, query, or fragment in the base URL.
- DeepSeek receives bearer auth and an SSE request with usage enabled.
- The gateway stamps `reasoner.model` into each durable handoff; the worker uses that immutable job version.
- `request_timeout` is the HTTP timeout. Retryable classes are transport errors, 408, 429, and 5xx.
- `heartbeat_interval` must be strictly less than half `lease_duration`.
- `role` and `provider` are labels; worker currently constructs the DeepSeek adapter.
- Initial and tool-follow-up requests include the official
  `{"thinking":{"type":"disabled"}}` shape. Provider reasoning content is
  rejected rather than presented or persisted.

## Handoff enablement

Provider credentials do not expose async delegation by themselves.

1. Set `features.handoffs: true` in the reviewed production-mode configuration used by the gateway. Local mode deliberately rejects this flag because it lacks durable coordinator composition.
2. Keep the PostgreSQL runtime `handoffs_enabled` authority enabled. Fresh migrations initialize it enabled; existing environments may not.
3. Verify `/v1/realtime/capabilities` includes `live.handoff.*` and the gateway injects all three reserved tools.

The checked-in production template leaves the static flag off. Enabling it requires a reviewed, synchronized edit to both `deploy/config/production.yaml` and `deploy/k8s/base/production.yaml`; static deployment verification rejects drift.

Optional side-effecting tools are separate:

```yaml
features:
  handoffs: true
  side_effecting_tools: true
  tool_encryption_key_env: LIVE_TOOL_ENCRYPTION_KEY
  tool_encryption_key_id: production-tools-v1
  runtime_refresh_interval: 1s
```

The shipped worker registry contains no external tools. Enabling the flag and encryption key does not register tools.

`development_durable` is the loopback exception that composes PostgreSQL,
static local authentication, handoffs, the worker, and the compiled Web Search
catalog. It is rejected by production rendering and release promotion. Use
`deploy/config/development-durable.yaml`; do not weaken local-mode validation.

## Defaults

| Area | Effective loader defaults |
|---|---|
| Gateway | listen `:8080`, storage grace `30s`, reconnect 2 retries (up to 3 attempts)/`100ms`–`1s`, credential TTL `5m` |
| Audio/session | 24 kHz mono PCM16LE, 960-byte frame; ingress 25 frames/24,000 bytes; egress 75 frames/72,000 bytes; control 256; writer control 64 |
| WebSocket | max frame 1 MiB; runtime read/write defaults 30s/10s |
| Admission | 1000 global, 100/tenant, 10/user; lease `30s`, heartbeat `10s`; global 100 rate/burst; IP 10 rate/20 burst |
| Worker | concurrency 1; lease `30s`, heartbeat `10s`, poll `1s`, request `30s`; retry `1s`–`30s` x3; 1 MiB/8192 tokens/8 rounds |
| Retention | conversation `720h`, sweep `1m`, deletion batch 100, approval `5m` |
| Safety | `static-v1`, `enforce`, summary 16 KiB, structured 64 KiB |
| Shutdown | gateway `30s`, worker `120s`, cleanup `5s` |

Omitted session block timeouts and WebSocket read/write timeouts hash as zero even though runtime supplies defaults. Explicitly adding their effective values changes the config hash.

## Secret inventory

| Environment name | Component | Format |
|---|---|---|
| `DATABASE_URL` | gateway, worker, migrate | PostgreSQL URL; production gateway rejects `sslmode=disable`; standalone worker/migrate pass the DSN through; deployed production must use verified TLS |
| `S2S_API_KEY` | gateway | Nonempty provider bearer credential |
| `DEEPSEEK_API_KEY` | worker; also gateway with combined config | Nonempty, ≤16 KiB, no whitespace/control bytes |
| `LIVE_STATIC_BEARER_TOKEN` | local gateway/worker admin | Local-only scoped bearer; production forbids static auth |
| `LIVE_ADMIN_BEARER_TOKEN` | drain/preStop | Local equals static token; production is a narrow OIDC admin bearer |
| `LIVE_PRIVACY_HMAC_KEY` | worker and shared gateway config | Raw nonempty value; worker requires at least 32 bytes |
| `LIVE_RETENTION_HMAC_KEY` | gateway, worker | Base64 of 32–4096 decoded bytes; pair with immutable key ID |
| `LIVE_RECONNECT_HMAC_KEY` | production gateway | Base64 of 32–4096 decoded bytes; pair with immutable key ID |
| `LIVE_TOOL_ENCRYPTION_KEY` | gateway shared config, worker | Base64 of exactly 32 decoded bytes; pair with immutable key ID |

`S2S_API_KEY` and `DEEPSEEK_API_KEY` may be issued by one provider account but
remain independently named, process-scoped secret references. Never substitute
a browser field or raw value into either `_env` field.

Generate development-only values without printing them into tracked files:

```sh
export LIVE_PRIVACY_HMAC_KEY="$(openssl rand -base64 32)"
export LIVE_RETENTION_HMAC_KEY="$(openssl rand -base64 32)"
export LIVE_RECONNECT_HMAC_KEY="$(openssl rand -base64 32)"
export LIVE_TOOL_ENCRYPTION_KEY="$(openssl rand -base64 32)"
```

Keep real values in an external secret manager. Key IDs are non-secret immutable identifiers and are included in the config hash.

## Production inputs

Production rendering additionally requires reviewed non-secret values:

- `LIVE_PRODUCTION_HOST`
- `LIVE_PRODUCTION_TLS_SECRET`
- `LIVE_PRODUCTION_TRUSTED_PROXY_CIDR`
- `LIVE_PRODUCTION_EGRESS_PROXY_CIDR`
- `LIVE_PRODUCTION_VOICE_URL`
- `LIVE_PRODUCTION_OIDC_ISSUER`
- `LIVE_DEPLOYMENT_REGION`
- `LIVE_PRODUCTION_GATEWAY_IMAGE`
- `LIVE_PRODUCTION_WORKER_IMAGE`
- `LIVE_PRODUCTION_MIGRATE_IMAGE`

Images are distinct canonical `repository/path@sha256:<64 lowercase hex>` references. Production runtime also requires `LIVE_ALLOWED_REGIONS`, gateway `LIVE_METRIC_INSTANCE`, exact `LIVE_GATEWAY_IMAGE`, and `LIVE_EXPECTED_CONFIG_HASH`; Kubernetes wires these. External speech and DeepSeek use the reviewed egress proxy on TCP/3128.

The raw production file contains substitution tokens and is intentionally not runnable. `render-kustomize.sh production` emits Kubernetes manifests; it does not emit a standalone service-config file. The embedded ConfigMap data inside the validated render is the runtime config.

## Immutable config hash

`config-hash` emits `v1:<64 lowercase hex>` over canonical defaulted non-secret behavior.

Included: provider URLs/models, public model, operational bounds, auth policy, admission, safety, telemetry, shutdown, feature flags, retention and key IDs.

Excluded: secret values, secret environment names, TLS file paths, role labels, and `store.database_url_env`. Relevant lists are sorted. Do not hand-edit `LIVE_EXPECTED_CONFIG_HASH`; regenerate after every semantic config change.

## Validation commands

```sh
go run ./cmd/live-gateway check-config --config deploy/config/local.yaml
go run ./cmd/live-worker check-config --config deploy/config/local.yaml
go run ./cmd/live-gateway config-hash --config deploy/config/local.yaml
go run ./cmd/live-worker config-hash --config deploy/config/local.yaml

DATABASE_URL='postgres://...' go run ./cmd/live-migrate -check
make verify-deploy-static
```

The two hashes match for the same file. For production, provide all reviewed render inputs and use `deploy/scripts/render-kustomize.sh production`, then the structured deployment validator/full gate.

## Configuration failures

| Symptom | Check |
|---|---|
| Provider 401/403 | Correct secret env is present in the component; URL contains no credential; model/account access is valid |
| Speech handshake 404 | URL ends at the provider Realtime path; do not pre-add model query |
| DeepSeek 404 | Base path is correct and does not already end in `/chat/completions` |
| Model rejected at gateway | Client model equals `advertised_model`; provider model belongs only in `voice.model` |
| `config hash mismatch` | Same rendered config and defaults produced the pod annotation/environment value |
| Handoffs absent | Static flag, runtime authority, capabilities, and provider tool configuration all agree |
| Worker loses leases | Heartbeat is below half the lease; DB latency fits the confirmed authority window |
| Production startup passes check but exits | Resolve secrets, OIDC/OTLP, region/instance/image identity, DB schema, and composition requirements |
