# GPT Live Go framework

This repository is a production-oriented Go gateway and durable worker framework for an OpenAI Realtime-compatible speech experience. The gateway owns authenticated WebSockets, bounded PCM transport, response epoch fencing, reconnect, and presentation. PostgreSQL owns durable conversations, handoffs, approvals, leases, idempotency, outbox delivery, retention, and runtime feature authority. Workers execute bounded background reasoning and approved tools without exposing either provider directly to clients.

The supported wire subset is in [docs/protocol.md](docs/protocol.md). Read [docs/security.md](docs/security.md) before configuring credentials or retention, [docs/operations.md](docs/operations.md) before deployment, and [docs/rollout.md](docs/rollout.md) before changing production traffic. Known model and simulator constraints are explicit in [docs/limitations.md](docs/limitations.md).

## Local verification

The default unit suite makes no provider or Internet connection:

```sh
go vet ./...
go test ./...
go test -race ./...
go build ./cmd/...
make verify-deploy-static
```

PostgreSQL integration, local speech contract, live-provider, chaos, load, and soak suites are separately tagged and require their documented explicit environment. Formal Task 24 evidence is produced only by `make load-evidence`; `make load-smoke` is never release evidence.

## Release evidence

`make release-gate` runs the 12 acceptance gates in fixed order, verifies the closed-world working evidence, and emits one canonical immutable unsigned archive for review. Supply the exact deployed non-secret hash as `RELEASE_CONFIG_HASH`. The command deliberately stops short of approval until an authorized operator signs the privacy statement out of band; phase 2 derives a signed archive from those same reviewed bytes and verifies it directly. The complete two-phase flow is in [docs/operations.md](docs/operations.md).

No production speech provider, reasoner, database, cluster, or secret file is contacted implicitly.
