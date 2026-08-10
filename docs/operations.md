# Operations

## Deploy and migrate

Build `gateway`, `worker`, and `migrate` Docker targets for linux/amd64 and linux/arm64. Production references must be `ghcr.io/chuanmingliu/openai-gpt-live/<component>@sha256:<digest>`; mutable tags are not deployment inputs. CI emits BuildKit SBOM and provenance and keyless Cosign signatures for each digest.

Validate the environment-specific render, precreate external Secrets, verify Prometheus Adapter custom metrics, apply the namespace/default-deny/migration egress policy, run the one-shot migration, then apply workloads. Before upgrade, take and restore-test an encrypted PostgreSQL backup. Run down/up migration checks only on a disposable database. The migrate command is serialized and reports schema version without echoing connection material.

Gateway drain is 30 seconds within a 45-second pod grace period; worker drain is 120 seconds within 135 seconds. Drain first makes readiness false, stops admissions/leases, waits for owned work, and then cancels remaining I/O. Do not terminate the old config hash until its active sessions, journal work, and leases reach the documented bounds.

## Observability and synthetic canary

Import `deploy/observability/grafana-dashboard.json` and apply `deploy/observability/alerts.yaml`. Dashboards are filterable by instance, provider, region, and immutable config hash. Page on stale output, any queue overflow, two-window SLO burn, cross-session leakage, or readiness loss; investigate outbox lag, expired leases, and sustained goroutine/heap growth.

A dedicated deployed synthetic client continuously opens two simultaneous isolated sessions through the reviewed production TLS host and origin. It sends a different bounded non-audio sentinel through each real provider session and fails if either sentinel is presented to the other session. It publishes `live_synthetic_cross_session_leaks_total` plus `live_synthetic_canary_last_success_timestamp_seconds`. The locked workload rereads a renewable token from its mounted file for every probe. Admission grants no-persistence behavior only to the exact configured synthetic tenant and subject; incomplete or mismatched configuration fails closed. The gateway still exercises the real isolated session/provider path, but it does not provision a conversation or persist transcript, item, or handoff records for that identity. Other identities retain normal durable behavior. Ordinary production audio is never watermarked.

Gateway and worker expose their custom, Go, and process series on the dedicated `metrics` port. The shipped `PodMonitor` and ingress policy allow only the observability namespace to scrape that port; readiness panels and alerts use kube-state-metrics `kube_pod_status_ready` rather than an unprovisioned black-box series.

## Release gate and human signature

Set the exact hash computed from the deploy-ready non-secret config and a separate absolute Task 24 evidence directory, then run all producers:

```sh
export RELEASE_CONFIG_HASH='v1:<64-lowercase-hex>'
export LIVE_EVIDENCE_DIR="$PWD/artifacts/task24-formal"
make release-gate
```

The unsigned run first writes the private closed-world working directory `artifacts/release/<commit>/<config-hash>/`, then descriptor-captures and fully verifies it into one canonical immutable archive under `artifacts/release-archives/<commit>/`. It prints `statement_sha256=<hash>`, `signed_at=<UTC timestamp>`, and the archive path, and exits with status 2 only after all producers and operational observations pass. The manual workflow uploads only that canonical archive and prints the upload action's bare lowercase SHA-256 digest. Confirm that digest when starting phase 2. Never sign a failed, incomplete, stale, modified, or differently digested diagnostic bundle.

The authorized human independently reviews that exact unsigned archive and repository/runtime policy, keeps the Ed25519 private key outside the repository and runner, and signs the 32-byte statement digest. That digest binds the commit, release config hash, observed formal-chaos runtime config hash and formal config document digest, generation time, all reviewed non-secret release inputs, every one of the 12 versioned producer records and execution times, all gate summary hashes, candidate image/SBOM/provenance metadata, the exact rendered production manifest, and the fixed named, bounded formal and operational reports. Formal-chaos results are never described as production execution. Raw test output is never archived:

```sh
statement='<hash printed by the unsigned gate>'
signed_at='<exact signed_at timestamp printed by the same unsigned gate>'
printf '%s' "$statement" | xxd -r -p > /secure/tmp/gpt-live-statement.bin
openssl pkeyutl -sign -rawin -inkey /secure/operator/release-ed25519.pem \
  -in /secure/tmp/gpt-live-statement.bin -out /secure/tmp/gpt-live-signature.bin
signature=$(base64 < /secure/tmp/gpt-live-signature.bin | tr -d '\n')
jq -n --arg gate_id privacy-no-audio-persistence \
  --arg signer release-operator --arg statement_sha256 "$statement" \
  --arg signature "$signature" --arg signed_at "$signed_at" \
  '{gate_id:$gate_id,signer:$signer,statement_sha256:$statement_sha256,signature:$signature,signed_at:$signed_at}' \
  > /secure/tmp/privacy-attestation.json
```

The trusted public-key file contains only the base64 encoding of the raw 32-byte Ed25519 public key. Phase 2 downloads the exact source-run archive, validates the supplied bare Actions digest against the source artifact metadata, and runs `live-release-gate finalize-archive`. Finalization derives a signed canonical archive from only the captured unsigned bytes plus the authorized attestation; it never extracts or re-walks the old working directory. It then runs `verify-archive` directly.

Verification rejects noncanonical archives/JSON, bundle or per-gate evidence older than 24 hours, overlapping or reversed gate chronology, future timestamps, wrong commit/config/reviewed inputs, unexpected versioned producers, unsafe or escaping paths, symlinks, excessive entries/depth/bytes, oversized or prohibited-content artifacts, undeclared bundle files, missing/malformed artifact hashes, SHA tampering, wrong gate order, duplicate/missing/nonzero gates, untrusted signers, statement mismatch, altered signing time, and forged signatures. Phase 2 uploads only `signed-release-evidence.tar` inside `signed-release-evidence-<commit>-<64-hex-config-digest>` and records its bare Actions artifact digest. Promotion downloads and runs `verify-archive` before receiving package authority.

Candidate collection verifies an exact linux/amd64 and linux/arm64 OCI index, platform-addressed SPDX/SLSA documents, and an exact source commit material before their summaries enter evidence. After keyless signing, CI verifies and archives the exact Fulcio issuer and workflow identity. A consumer can apply the same policy:

```sh
cosign verify \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity "https://github.com/chuanmingliu/openai-gpt-live/.github/workflows/release-gate.yml@refs/heads/<reviewed-branch>" \
  'ghcr.io/chuanmingliu/openai-gpt-live/<component>@sha256:<digest>'
```

## Backup and incidents

Backups must be encrypted, access-controlled, retention-aligned, and restore-tested. A restore is promoted only after schema check, config-hash verification, outbox/cursor consistency, and synthetic isolation pass. For incidents, preserve machine evidence and timestamps but never capture customer media or content. Use the database, provider, overload, and rollback runbooks in this directory.
