# Custom GPT-Live browser demo

Loopback-only BFF + browser client for this repository's GPT Live stack.

- **Realtime path:** the configured speech-to-speech WebSocket (`gateway.voice`).
- **Async path:** `live-worker` + DeepSeek Chat Completions.
- The browser never receives `LIVE_DEMO_BEARER`. The BFF injects `Authorization` on the upstream dial.

## Prerequisites

1. Local GPT Live topology is up (Compose or focused processes): PostgreSQL, `live-gateway`, `live-worker`, and the speech-to-speech service.
2. Gateway advertised model matches `-model` (Compose default: `gpt-live-simulator`).
3. `LIVE_DEMO_BEARER` equals the local static bearer (`LIVE_STATIC_BEARER_TOKEN`).
4. DeepSeek key is in the worker environment (`DEEPSEEK_API_KEY`). Handoffs still require production-mode composition; this demo is the voice client, not a local-mode handoff bypass.

## Start

```sh
export LIVE_DEMO_BEARER="$LIVE_STATIC_BEARER_TOKEN"
make demo-live-client
```

Open `http://127.0.0.1:3000`. Click Start, grant the microphone, speak, interrupt playback by speaking, then Stop.

```sh
curl --fail http://127.0.0.1:3000/health/ready
```

Stop with Ctrl+C. That stops only this demo process. It does not delete PostgreSQL data.

## Flags

| Flag | Default |
|---|---|
| `-listen` | `127.0.0.1:3000` |
| `-gateway-url` | `ws://127.0.0.1:18080/v1/realtime` |
| `-model` | `gpt-live-simulator` |
| `-origin` | `http://127.0.0.1:3000` |
| `-bearer-env` | `LIVE_DEMO_BEARER` |

Non-loopback listen addresses, credentialed/query gateway URLs, and foreign origins are rejected at startup.

## Tests

```sh
LIVE_DEMO_BEARER=test-only-bearer make test-demo-live-client
```

This is not a production OIDC/BFF. Do not expose it.
