# GPT Live Go framework

## What it is

GPT Live Go is a Go **backend only** framework: an OpenAI Realtime-compatible
WebSocket gateway plus an optional durable asynchronous worker. It includes no
frontend, browser server, or static assets. The gateway owns the realtime
protocol boundary; the worker is used only when a durable backend is selected.

## Requirements

Credential-free startup requires only Go 1.26. PostgreSQL and provider
credentials are required only for the optional durable-worker and provider
paths described below.

## Quick start without credentials

Run these in separate terminals:

```sh
go run ./cmd/live-gateway
go run ./cmd/live-worker --backend none
```

The gateway starts on `127.0.0.1:8080` with its local OpenAI
Realtime-compatible configuration. The worker's explicit `--backend none` mode
starts diagnostics only: it does not read configuration, look up credentials,
connect to PostgreSQL, or process durable jobs.

## Verify the processes

```sh
curl --fail http://127.0.0.1:8080/v1/realtime/capabilities
curl --fail http://127.0.0.1:9091/health/live
curl --silent --output /dev/null --write-out '%{http_code}\n' \
  http://127.0.0.1:9091/health/ready
```

Capabilities returns JSON, liveness returns `ok`, and idle-worker readiness
intentionally returns `503`: the process is alive but cannot accept durable
jobs without a selected backend.

## Add local speech

For realtime sessions, run the official
[Hugging Face `speech-to-speech`](https://github.com/huggingface/speech-to-speech)
server on its loopback OpenAI Realtime endpoint,
`ws://127.0.0.1:8765/v1/realtime`. The Go gateway still starts when that server
is absent, but a realtime session requires it and will fail cleanly if it cannot
be reached. This repository neither embeds nor launches the speech service.

## Use explicit configuration

Use explicit configuration once the gateway needs a chosen speech provider or a
worker needs durable jobs. The legacy worker form remains compatible and selects
the durable backend when a configuration file is supplied.

```sh
go run ./cmd/live-gateway -config configs/gateway.example.yaml
go run ./cmd/live-worker -config configs/worker.example.yaml
go run ./cmd/live-worker --backend durable -config configs/worker.example.yaml
```

The configured gateway accepts the advertised public model, then dials the
configured speech endpoint for a session. Configured credentials remain strict:
if a configuration names a credential environment variable, it must be supplied.
The durable worker requires PostgreSQL, migrations, and its configured reasoner
credentials; `--backend none` is deliberately not a substitute for that runtime.

## Security boundary

The credential-free defaults are loopback-only and unauthenticated. Never expose
them directly. Production remains controlled by OIDC, TLS, PostgreSQL, and a
secret manager through reviewed configuration and deployment procedures.

## Protocol and operations

Read the focused references for the supported protocol and production operation:
[protocol](docs/protocol.md), [security](docs/security.md),
[operations](docs/operations.md), [testing](docs/testing.md), and
[deployment](deploy/README.md).

## Design references

This design adopts two external patterns without importing either project:
[Hugging Face `speech-to-speech`](https://github.com/huggingface/speech-to-speech)
provides the local OpenAI Realtime transport pattern, and
[Qwen Audio Agent](https://github.com/QwenAudio/qwen-audio-agent) provides the
explicit no-backend worker lifecycle pattern. The Go service keeps its own
protocol, audio, session, and deployment implementations.

## Optional durable and provider paths

The examples in [configs/gateway.example.yaml](configs/gateway.example.yaml) and
[configs/worker.example.yaml](configs/worker.example.yaml) are for explicitly
configured processes. Configure a gateway speech endpoint and its credential
reference only when the endpoint needs one. Configure `DATABASE_URL` and
`DEEPSEEK_API_KEY` only for the durable worker, and provide those values through
a secret manager or secure process environment; never commit them or place them
in a tracked command.

For a production-like local topology, PostgreSQL, additional HMAC material,
authentication, provider credentials, and Compose inputs are documented in
[deploy/README.md](deploy/README.md). `LIVE_STATIC_BEARER_TOKEN` is an optional
explicit local-auth setting; production forbids static auth and uses OIDC.

## Runtime contracts

- `live-gateway` without `-config` binds `127.0.0.1:8080`, serves capabilities,
  and defers the local speech-provider dial until a realtime session opens.
- `live-worker --backend none` binds diagnostics on `127.0.0.1:9091`; liveness
  is `200`, readiness is content-free `503`, and the process owns no durable
  dependencies or admin operations.
- `live-worker -config <path>` and `live-worker --backend durable -config <path>`
  use the strict durable runtime. `--backend none` rejects `-config`, and
  `--backend durable` requires it.

No provider, database, cluster, registry, secret file, or external identity is
contacted implicitly by the credential-free startup path.
