---
name: gpt-live-go-framework
description: Use when configuring, extending, deploying, debugging, or verifying the GPT Live Go repository, especially Realtime speech-to-speech WebSockets, DeepSeek asynchronous handoffs, PostgreSQL durability, reconnect, authentication, Kubernetes or Compose, load and chaos evidence, release gates, or privacy-sensitive operations.
---

# GPT Live Go Framework

Treat configuration, runtime composition, and release evidence as separate trust boundaries. Valid YAML does not prove secrets, PostgreSQL, providers, telemetry, or topology are ready.

## Start

1. Resolve the root with `git rev-parse --show-toplevel`; confirm `go.mod` declares `github.com/chuanmingliu/openai-gpt-live`.
2. Classify local/production, voice-only/durable handoffs, and direct/Compose/Kubernetes.
3. Read the repository `README.md` and the relevant reference:

| Task | Required reference |
|---|---|
| Provider, YAML, secrets, hash | `references/configuration.md` |
| Ownership, protocol, durability | `references/architecture.md` |
| Run, deploy, test, troubleshoot | `references/operations.md` |

Read the exact source/config named there. Source and validators win disagreements.

## Configuration workflow

1. Start from `deploy/config/{local,production}.yaml` or `configs/*.example.yaml`; values never go in `_env` fields.
   Use focused gateway/worker files for direct host processes; use the combined config only in isolated containers or pods.
2. Keep `gateway.advertised_model` (client alias) distinct from `gateway.voice.model` (provider model).
3. Use a bare speech URL; production requires `wss://`. Configure DeepSeek's parent base URL; its adapter appends `/chat/completions`.
4. Durable delegation requires production mode, static `features.handoffs: true`, and enabled PostgreSQL runtime authority. A key alone enables nothing.
5. Inject component-scoped secrets from the configuration reference.
6. Run `check-config` and `config-hash`, then actual startup or the tagged boundary. `check-config` uses unresolved config only.
7. Synchronize canonical config, Kubernetes copy, substitutions, environment inventory, and deploy validator.

## Non-negotiable invariants

- Clients reach only the gateway; never expose providers.
- Audio is 24 kHz mono PCM16LE, 960 bytes/20 ms, bounded, never persisted/logged.
- One reactor owns session state; incarnation/epoch fence stale output.
- PostgreSQL owns committed durability, leases, cursors, reconnect, and coordinator authority.
- Reserved handoff tools are server-owned.
- Static auth is local-only; browser production uses OIDC/exact origins, while deliberate non-browser mode is explicit; production also requires TLS/proxy identity and default deny.
- Browser JavaScript cannot set the bearer header; no bootstrap ships. Require a reviewed auth proxy/BFF or native client, never a query token.
- Secret values stay outside config hashes/evidence. Missing, skipped, unknown, unsafe, or uncertain state fails closed.

## Verification ladder

```sh
go test -race ./path/to/affected/package
go test -race -count=1 ./...
go vet ./...
go build ./...
go mod tidy -diff
make verify-deploy-static
```

Run tagged suites only with their documented dependency. Never reinterpret a skip, smoke, fixture, or unpinned host as release evidence.

## Common mistakes

| Mistake | Correction |
|---|---|
| URL contains model/token | Use fields; adapters add query/bearer. |
| DeepSeek base ends in endpoint | Use parent; client appends it. |
| Provider labels imply plugins | Runtime adapters are fixed today. |
| Side-effect flag implies tools | Shipped registry is empty. |
| Shared production network namespace | Admin/metrics ports collide; isolate pods. |
| Raw-event debugging | Log only bounded metadata, never content/credentials. |
