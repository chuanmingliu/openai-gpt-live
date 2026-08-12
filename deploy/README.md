# Deployment guide

`make verify-deploy-static` validates the two Compose renders, the local
Kustomize render, an explicitly marked non-deployable production fixture,
canonical configuration hashes, structured per-workload hardening policies, and
the complete Kubernetes resource graph (selectors, ports, scaling, disruption,
Ingress, ConfigMap projection, hashes, and environment wiring), plus repository
secret policy without contacting a cluster. Fixture success is not
production readiness: an unrendered production overlay and a production render
without operator inputs are both required to fail. `make verify-deploy` is the release
gate: it additionally requires Docker, kubeconform, and kind, builds all four
image targets, runs the service config commands with a read-only root filesystem
and no capabilities, performs strict schema validation, and uses server-side
dry-run of an actual deploy-ready production render in a disposable kind cluster.
It therefore requires the reviewed production inputs listed below; fixture mode
is never used by the full gate. Missing tools or inputs are failures, never skips.

## Configuration and secrets

`deploy/config/local.yaml` is canonical local configuration. Production config
is a non-secret template whose reviewed values are supplied only to
`render-kustomize.sh`; the script runs the shipped binary to validate the final
config and compute its hash. The matching Kubernetes copies are checked for
drift. The same generated hash is placed in the pod annotation and
`LIVE_EXPECTED_CONFIG_HASH`; the binaries independently reject drift and use the
same bounded value in telemetry. Secret values are not part of the hash.

Create the preexisting `gpt-live/live-secrets` Kubernetes Secret out of band
using the exact keys in `k8s/secrets.example.env`; never commit a generated
Secret. The apply preflight checks presence and required keys without printing
values. Local `LIVE_STATIC_BEARER_TOKEN` is sourced only through
`secretKeyRef{name: live-secrets, key: LIVE_STATIC_BEARER_TOKEN}`. Compose passes those
same environment names from the invoking process and explicitly disables `.env`
loading in repository scripts. `S2S_IMAGE` and `S2S_CONFIG_PATH` select the
external simulator image and its non-secret configuration. Admin/health ports
are loopback-published only; the public Realtime port defaults to 18080.
For local static authentication, `LIVE_ADMIN_BEARER_TOKEN` and
`LIVE_STATIC_BEARER_TOKEN` must reference the same scoped value because preStop
uses the former while the local admin handler validates the latter.

The durable browser-tool composition is a separate loopback profile:

```sh
go run ./cmd/live-gateway check-config --config deploy/config/development-durable.yaml
go run ./cmd/live-worker check-config --config deploy/config/development-durable.yaml
make durable-demo-migrate
make durable-demo-up
```

It uses PostgreSQL schema v8, `deepseek-v4-flash` with thinking disabled,
static local authentication, durable handoffs, and the compiled Web Search
catalog. Follow `docs/demo/realtime-browser-client.md` for the required secret
prompts, migration check, content-free gateway/worker readiness, BFF and Tools
drawer flow, privacy inspection, idle checks, and non-destructive stop. The
speech and worker credentials may derive from one provider account, but
`S2S_API_KEY` and `DEEPSEEK_API_KEY` remain separate process secret references.
`development_durable` is rejected by production rendering and promotion.
The durable override derives a container-only `DATABASE_URL` from the hidden
`POSTGRES_PASSWORD`; PostgreSQL is not host-published. Migration and `-check`
therefore run inside Compose before the application services, and resolved
Compose output must not be printed or attached to reports.

The canary identity is a second, separately managed Secret named
`gpt-live/live-synthetic-identity`. It must contain exactly one nonempty key,
`data.token`. In production, the platform identity authority must continuously
renew that short-lived OIDC bearer before expiry for audience `gpt-live`, scope
`realtime`, tenant `gpt-live-synthetic`, and user `release-canary`; do not copy a
long-lived token into this Secret. The projected file is mode `0440`, and the
canary rereads it before every probe so rotation does not require a rollout. For
local-only validation, `data.token` may contain the same scoped value supplied
as `LIVE_STATIC_BEARER_TOKEN`. `deploy-k8s.sh` fails before migration when the
Secret or its exact key inventory is absent, and it waits up to five minutes for
the canary Deployment to become Ready after applying workloads.

Production rendering fails closed unless all of these non-secret environment
inputs are present and reviewed:

- `LIVE_PRODUCTION_HOST`, `LIVE_PRODUCTION_TLS_SECRET`
- `LIVE_PRODUCTION_TRUSTED_PROXY_CIDR`, `LIVE_PRODUCTION_EGRESS_PROXY_CIDR`
- `LIVE_PRODUCTION_VOICE_URL`, `LIVE_PRODUCTION_OIDC_ISSUER`
- `LIVE_PRODUCTION_GATEWAY_IMAGE`, `LIVE_PRODUCTION_WORKER_IMAGE`, and
  `LIVE_PRODUCTION_MIGRATE_IMAGE`, each in canonical
  `registry/repository-path@sha256:<64 lowercase hex>` form (no URL scheme,
  whitespace, or tag; repository name at most 255 bytes)

Example/test/reserved hosts and networks, the old generic host/TLS/CIDR values,
repeated or duplicate digests, mutable images, and unresolved tokens are
rejected. `LIVE_PRODUCTION_RENDER_MODE=fixture` is reserved to repository tests;
its rendered metadata says `fixture`, and `deploy-k8s.sh` never enables it.
Native NetworkPolicy cannot filter external FQDNs: external speech and DeepSeek
requests use `http://egress-proxy.platform.svc:3128`, and the production policy
admits exactly TCP/3128 to the reviewed stable egress-proxy CIDR—not generic
TCP/443 to that CIDR.

## Apply and drain order

The checked-in `gpt-live` Namespace is applied first and has explicit repository
ownership. `deploy/scripts/deploy-k8s.sh production` refuses to run until the operator
attests that Prometheus Adapter exposes `custom.metrics.k8s.io`. It validates the
deploy-ready render, checks external Secrets, then applies default-deny and the
migration-specific allow policy before starting the Job. Migration egress is
exactly kube-system DNS TCP/UDP 53 plus labeled in-cluster PostgreSQL TCP/5432;
the PostgreSQL peer is bound to the `gpt-live` namespace identity. Application
OIDC and OTLP peers are likewise bound to `identity` and `observability`; the
ingress controller and egress proxy remain bound to `ingress-nginx` and
`platform`.

There is no migration identity, telemetry, provider, or Internet path. The script waits up
to ten minutes for completion, then applies workloads. Run down/up migration
checks only on a disposable database. Production gateway scaling combines CPU with
`live_sessions_active`; worker scaling combines CPU with queue age and never
uses active sessions.

Before apply, the same Go YAML parser validates unique decoded object identities,
rejects duplicate keys, aliases/merge ambiguity, and any missing or extra graph
edge. That graph includes the canary WSS/origin, hardened Secret projection,
readiness and metrics endpoints, overlay-specific network paths, and the exact
runtime PodMonitor and metrics-ingress selectors. Its `--split job|workloads`
mode then emits each validated object exactly once; no line-oriented YAML
splitter is used.

Gateway metric identity is deliberately separate from
`admission.instance_id` and from the canonical configuration hash. Kubernetes
projects each gateway pod's unique `metadata.name` into `LIVE_METRIC_INSTANCE`;
the production binary fails closed if it is absent, and the PodMonitor preserves
that exporter label with `honorLabels: true` so replica-local series do not
collapse.

## Protected candidate canary release edge

The first phase of `manual-release-gate` cannot collect unsigned evidence until
the `release-candidate-canary` protected environment approves and completes an
explicit candidate deployment edge. That edge consumes the three immutable
image digests, rendered production manifest and its SHA-256, exact config hash,
commit, and reviewed region. The dedicated runner must provide
`/usr/local/libexec/gpt-live/candidate-canary-edge`; both that executable and
its containing directory are root-owned, are not symlinks, and are not writable
by group or other users. The repository wrapper fails closed when any of those
conditions or the external authority is unavailable.

The platform authority owns deployment, observation, halt evaluation, and
rollback. It observes for at least 1,800 seconds and halts on any readiness or
synthetic failure, cross-session leak, queue overflow, or stale-output
violation, as fixed by
`deploy/release/candidate-canary-halt-policy-v1.json`. It must restore the prior
state on success and failure. Only after rollback may it emit the canonical,
content-free receipt and detached Ed25519 signature. The workflow verifies that
signature with `RELEASE_CANARY_RECEIPT_PUBLIC_KEY_BASE64`, binds the public-key
and halt-policy digests into the human-reviewed release inputs, and reverifies
the receipt during collection, signed-evidence finalization, and promotion.
Missing approval, runner authority, rollback confirmation, observation time,
receipt, or signature prevents evidence collection and promotion.

Gateway pods have 45 seconds and workers 135 seconds to terminate. The preStop
hook invokes a fixed image helper, which reads no secret argument: the helper
resolves `LIVE_ADMIN_BEARER_TOKEN` from its environment and sends it only
in the loopback Authorization header. The request and readiness polls have hard
deadlines, the helper waits until readiness falls, and a single monotonic
35/125-second context budget includes both drain and every poll. This leaves
shutdown margin inside the pod grace periods. In production,
`LIVE_ADMIN_BEARER_TOKEN` must be a narrowly scoped token
accepted by the configured OIDC policy; rotate it through the external secret
manager.

## Web Search production roadmap

The loopback Web Search preset is not a production deployment recipe.
Production enablement requires reviewed OIDC/BFF authentication, PostgreSQL
TLS and backups, external secret rotation, immutable images and configuration
hashes, bounded egress through the reviewed proxy, privacy-safe telemetry,
capacity/chaos/soak evidence, canary observation, and tested rollback. Future
MCP integrations are compiled, typed, operator-owned catalog adapters with
fixed destinations and bounds. Browser-supplied raw server JSON, packages,
commands, paths, environment, headers, or credentials remain rejected.

## Local smoke

After exporting every name listed by `smoke-compose.sh` and selecting the S2S
image/config, run `make smoke-deploy`. The bounded check migrates the disposable
database, waits for readiness, establishes one authenticated WebSocket session,
sends and stores no audio/transcript content, then verifies graceful drain. The
script removes only its own Compose volumes. The fuller simulator audio check is
currently subject to the Task 22 implicit-VAD event-order limitation; it is not
reported as a deployment-readiness failure.

The `load` Docker target contains the statically linked, non-root `live-load`
binary. It requires a `capacity` or `soak` subcommand and an explicit local or
private `-gateway-url`; an absent or public endpoint fails closed with exit 64
and a content-free message. Bearer values are accepted only through a named
environment variable and are never included in reports or logs.

For Task 24, `make load-smoke` creates a short harness bundle while
`make load-evidence` creates the exact formal bundle. Set
`LIVE_EVIDENCE_DIR` to an absolute operator-owned artifact directory. Each
bundle is JSONL with explicit selectors, SHA-256 artifact digests, and a final
status. Formal evidence cannot silently substitute smoke durations: it requires
the exact ten-minute pinned-profile capacity/SLO gate, 20-sample barge gate,
six-case disposable application chaos gate, and two-hour soak gate.

The Go 1.26 and Alpine runtime bases are pinned by immutable digest. Runtime
package installation was eliminated: CA data is copied from the pinned Go base,
and the runtime user/shell come from the pinned Alpine bytes. Rebuilding does not
resolve a mutable base tag or live package repository.
