# Custom GPT-Live browser client

Trusted-edge BFF + browser client for this repository's GPT Live stack.

- **Realtime path:** the configured speech-to-speech WebSocket (`gateway.voice`).
- **Async path:** `live-worker` + DeepSeek Chat Completions. DeepSeek runs only when the gateway's two-key handoff switch is on (`features.handoffs` and the PostgreSQL `handoffs_enabled` flag). Local composition still refuses handoffs; that is not bypassed here.
- The browser never receives the process credential. The BFF injects `Authorization` on the upstream dial.

This process is a credential-injecting edge, not an OIDC login broker. Put it on the same trusted network as `live-gateway`.

## Prerequisites

1. GPT Live topology is up (Compose or focused processes): PostgreSQL, `live-gateway`, `live-worker`, and the speech-to-speech service.
2. Gateway advertised model matches `-model` (Compose default: `gpt-live-simulator`).
3. The named bearer environment variable equals a credential the gateway will accept (local static bearer, or the production workload identity the gateway expects).
4. DeepSeek key is in the worker environment (`DEEPSEEK_API_KEY`) if handoffs are enabled.
5. End-to-end DeepSeek handoffs require production or chaos composition (local Compose still refuses `features.handoffs`). Background work appears in the browser as `live.handoff.*` status, not as automatic speech.

## Local

1. From `deploy/`, start PostgreSQL, speech-to-speech, gateway, and worker (`docker compose up -d`) after exporting the Compose secrets listed in the root README. The gateway listens on `127.0.0.1:18080`.
2. In the repo root, export the demo bearer from the same local static token the gateway accepts:

```sh
export LIVE_DEMO_BEARER="$LIVE_STATIC_BEARER_TOKEN"
make demo-live-client
```

3. Open `http://127.0.0.1:3000`. The Setup widget on the page checks BFF readiness and whether the gateway host accepts TCP. Click Recheck after Compose finishes.
4. Click Start, grant the microphone, speak, interrupt playback by speaking, then Stop.

Focused processes instead of Compose: migrate, then `go run ./cmd/live-gateway` and `go run ./cmd/live-worker` as in the root README, then the same `make demo-live-client` step.

```sh
curl --fail http://127.0.0.1:3000/health/live
curl --fail http://127.0.0.1:3000/health/ready
```

Stop with Ctrl+C. SIGINT/SIGTERM refuse new sessions, cancel active sockets, and wait up to `-drain-timeout`. That stops only this process. It does not delete PostgreSQL data.

## Production

Production mode refuses to start without HTTPS origin and either TLS files or ingress TLS.

```sh
go run ./demo/live-client \
  -mode production \
  -behind-ingress \
  -listen 0.0.0.0:3000 \
  -origin https://voice.example \
  -gateway-url wss://gateway.internal/v1/realtime \
  -model gpt-live-real \
  -max-sessions 64
```

Or terminate TLS on the process (`-tls-cert` and `-tls-key`, TLS 1.3). Do not set both process TLS and `-behind-ingress`.

Kubernetes probes: `/health/live` (liveness) and `/health/ready` (readiness; 503 while draining).

The gateway URL must be `wss`, or `ws` only to a loopback/private address. The browser origin must be `https` and an exact match. Wildcard origins are rejected.

## Flags

| Flag | Default | Notes |
|---|---|---|
| `-mode` | `local` | `local` or `production` |
| `-listen` | `127.0.0.1:3000` | Local must be loopback. Production may bind a public or empty host. |
| `-gateway-url` | `ws://127.0.0.1:18080/v1/realtime` | No userinfo, query, or fragment. Path must be `/v1/realtime`. |
| `-model` | `gpt-live-simulator` | Added as the upstream `model` query by the BFF. |
| `-origin` | `http://127.0.0.1:3000` | Exact browser origin. Local `http` and must match listen. Production `https`. |
| `-bearer-env` | `LIVE_DEMO_BEARER` | Environment variable that holds the credential. |
| `-tls-cert` / `-tls-key` | empty | Required together in production unless `-behind-ingress`. |
| `-behind-ingress` | false | TLS terminates at ingress. Incompatible with process TLS files. |
| `-max-sessions` | `8` | Concurrent browser sockets. 1–1024. |
| `-handshake-timeout` | `10s` | Upstream dial deadline. |
| `-idle-timeout` | `5m` | Closes a socket with no frames in either direction. |
| `-max-session` | `2h` | Hard session lifetime. |
| `-drain-timeout` | `15s` | Shutdown wait for hijacked WebSockets. |

Startup rejects non-loopback listen in local mode, credentialed or query-bearing gateway URLs, foreign or wildcard origins, and production without TLS or ingress.

## Tests

```sh
LIVE_DEMO_BEARER=test-only-bearer make test-demo-live-client
```
