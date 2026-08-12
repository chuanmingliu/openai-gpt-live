# Operations reference

## Contents

- Local workflow
- Realtime browser operator guide
- Service endpoints
- Compose
- Kubernetes
- Verification suites
- Load, chaos, and release evidence
- Troubleshooting
- Local skill installation

## Local workflow

Prerequisites: Go 1.26, PostgreSQL 16 for durable paths, an OpenAI-Realtime-compatible S2S endpoint, and a DeepSeek-compatible SSE Chat Completions endpoint for async work.

For the focused realtime-only browser path, route operators to
[`docs/demo/realtime-browser-client.md`](../../../docs/demo/realtime-browser-client.md).
It is the authority for the exact speech/gateway/BFF configuration, hidden
credential prompts, content-free checks, owned-process stop commands, and the
manual audible-playback/barge-in checklist. That path does not require a
DeepSeek API key, PostgreSQL handoff configuration, or `live-worker`.

Network-free checks:

```sh
go vet ./...
go test ./...
go test -race -count=1 ./...
go build ./...
go mod tidy -diff
make verify-deploy-static
```

Validate the canonical local config and compute its expected hash:

```sh
go run ./cmd/live-gateway check-config --config deploy/config/local.yaml
go run ./cmd/live-worker check-config --config deploy/config/local.yaml
export LIVE_EXPECTED_CONFIG_HASH="$(go run ./cmd/live-gateway config-hash --config deploy/config/local.yaml)"
```

Run migrations against a disposable/local database before services:

```sh
go run ./cmd/live-migrate -database-url-env DATABASE_URL -direction up
go run ./cmd/live-migrate -database-url-env DATABASE_URL -check
```

Start gateway and worker with `-config <path>`. A combined config requires all named secrets in the gateway environment. Direct host processes need distinct admin ports; the shipped Compose/Kubernetes topology isolates the same ports in separate containers/pods.

The local gateway can exercise authenticated speech, and the focused worker can exercise its adapter/runtime. End-to-end gateway-to-worker handoffs require the validated production durability composition; use the disposable production-like chaos topology for local coverage rather than forcing `features.handoffs` into local mode.

## Service endpoints

| Endpoint | Purpose |
|---|---|
| `GET /v1/realtime` | Authenticated WebSocket |
| `GET /v1/realtime/capabilities` | Public supported subset/capability document |
| `/health/live` | Process liveness on diagnostics listener |
| `/health/ready` | Dependency/readiness state on diagnostics listener |
| `/metrics` | Prometheus; production also exposes metrics-only `:9091` |
| `GET /admin/v1/capacity` | Authenticated bounded capacity snapshot |
| `GET /admin/v1/sessions` | Authenticated active-session snapshot |
| `POST /admin/v1/drain` | Stop readiness/admission and begin bounded drain |
| `PUT /admin/v1/features/handoffs` | Change runtime handoff authority with reason |
| `DELETE /admin/v1/conversations/<id>` | Authenticated durable deletion |

Compose publishes gateway `18080`, gateway diagnostics `19090`, and worker diagnostics `19091`, all on loopback.

## Compose

`make smoke-deploy` is the bounded supported smoke path. It requires:

- `POSTGRES_PASSWORD`, `DATABASE_URL`
- `S2S_API_KEY`, `DEEPSEEK_API_KEY`
- `LIVE_STATIC_BEARER_TOKEN`, `LIVE_ADMIN_BEARER_TOKEN` (equal locally)
- privacy, retention, reconnect, and tool keys
- `S2S_IMAGE`, `S2S_CONFIG_PATH`

The Compose file maps the worker/gateway provider credential from `FORMAL_DEEPSEEK_API_KEY`; set it to the same local DeepSeek value when invoking Compose. Repository scripts disable `.env` loading.

Compose order is PostgreSQL health, migration completion, then gateway/worker. It uses read-only filesystems, tmpfs, dropped capabilities, no-new-privileges, and bounded stop grace. The smoke opens one content-free authenticated session and verifies drain. Because the Compose project name is fixed to `gpt-live` and cleanup runs `down --volumes`, never run it alongside a `gpt-live` project or volume that must be preserved.

The separate `development_durable` browser profile does not accept a host
DSN. Its override derives container-only `DATABASE_URL` values from
`POSTGRES_PASSWORD` and keeps PostgreSQL off host ports. Run
`make durable-demo-migrate` for migration plus schema `-check`, then
`make durable-demo-up`; do not print resolved Compose configuration.

## Kubernetes

Static validation:

```sh
make verify-deploy-static
```

Full validation requires Docker, `kubectl`, `kubeconform`, `kind`, reviewed production render inputs, immutable images, and local cluster authority:

```sh
make verify-deploy
```

Apply:

```sh
deploy/scripts/deploy-k8s.sh local

PROMETHEUS_ADAPTER_CUSTOM_METRICS_READY=true \
  deploy/scripts/deploy-k8s.sh production
```

Precreate out-of-band:

- `gpt-live/live-secrets` with the overlay-required keys listed in `deploy/k8s/secrets.example.env`; production excludes the local-only static bearer, and preflight checks presence rather than rejecting extras.
- Production TLS Secret with `tls.crt` and `tls.key`.
- `gpt-live/live-synthetic-identity` containing exactly nonempty `data.token` for the renewable synthetic OIDC identity.

Apply order is namespace, external-secret preflight, default deny, migration-only policy, migration Job, workloads, then canary readiness. Production external providers traverse the reviewed egress proxy. The validator rejects missing/extra objects, duplicate YAML identities/keys, selector/port/config/hash drift, unsafe security contexts, and unresolved placeholders.

## Verification suites

| Boundary | Command |
|---|---|
| PostgreSQL integration | `TEST_DATABASE_URL=... go test -race -count=1 -tags=integration ./test/integration/...` |
| Local S2S contract | `S2S_REALTIME_URL=ws://127.0.0.1:8765/v1/realtime go test -count=1 -tags=contract ./test/contract/...` |
| Live DeepSeek | `DEEPSEEK_API_KEY=... DEEPSEEK_BASE_URL=... DEEPSEEK_MODEL=... go test -count=1 -tags=liveprovider ./test/liveprovider/...` |
| Capacity | `make capacity` |
| Barge fencing | `make barge` |
| Exact soak | `LIVE_SOAK_DURATION=2h make soak` |
| Disposable chaos | Configure all documented `CHAOS_*` names, then `make chaos` |

Missing explicit provider configuration may skip before connecting. After a handshake/request starts, transport/protocol/provider failure is a failure, never a skip.

## Load, chaos, and release evidence

```sh
LIVE_EVIDENCE_DIR=/absolute/operator/path make load-smoke
LIVE_EVIDENCE_DIR=/absolute/operator/path make load-evidence
```

Smoke is short and never release evidence. Formal requires exact 10-minute pinned capacity/SLO, 20-sample barge fencing, all six application chaos cases, and exact two-hour soak.

Release collection:

```sh
export RELEASE_CONFIG_HASH='v1:<64-lowercase-hex>'
export LIVE_EVIDENCE_DIR="$PWD/artifacts/task24-formal"
make release-gate
```

The complete unsigned gate intentionally exits 2 for independent offline Ed25519 privacy signing. Never sign a failed/incomplete/stale/modified bundle. Protected candidate deployment, at least 1,800 seconds of canary observation, rollback confirmation, exact OCI/SBOM/provenance, immutable archive transfer, and human authority are external release requirements.

## Troubleshooting

| Symptom | Action |
|---|---|
| `configured TEST_DATABASE_URL is unreachable` | Use disposable PostgreSQL 16 and a role allowed to create/drop schemas |
| `session_limit_reached` | Match `S2S_CONTRACT_CONCURRENCY` to free simulator pool slots and close other clients |
| Output before `response.created` | Known implicit-VAD simulator ordering defect; do not weaken epoch binding |
| DeepSeek 429 | Confirm bounded `Retry-After`, retry deadline, model/account quota, and no secret/body logging |
| Readiness false | Check DB schema/lease authority, provider/telemetry construction, runtime feature refresh, and drain state |
| Queue overflow | Halt rollout, shed admission, disable handoffs, drain the affected config hash; fix bottleneck rather than enlarging bounds live |
| Stale output/cross-session leak | Critical: disable handoffs, drain traffic, preserve content-free evidence, rotate credentials if indicated, and roll back |
| Lost notification | Durable poll/cursor repair should recover; inspect outbox age, cursor continuity, and coordinator shard authority |
| Reconnect rejected | Verify non-secret `reconnect=1`, first control frame, credential TTL/scope/model, owner lease, and previous-token overlap |

Never troubleshoot by dumping raw WebSocket frames, PCM, transcript text, prompts, reasoning, tool arguments/results, bearer tokens, database URLs, or provider bodies.

## Local skill installation

The versioned source is `skills/gpt-live-go-framework`; the standard local destination is `${CODEX_HOME:-$HOME/.codex}/skills/gpt-live-go-framework`.

```sh
codex_skill_root="${CODEX_HOME:-$HOME/.codex}/skills"
mkdir -p "$codex_skill_root"
rsync -a skills/gpt-live-go-framework/ "$codex_skill_root/gpt-live-go-framework/"
```

This deliberately omits `--delete`, so destination-only files remain. It can still overwrite a modified same-path file: preview with `rsync -ani`, diff/back up customizations, and keep reusable changes in the tracked source rather than only in the installed copy.
