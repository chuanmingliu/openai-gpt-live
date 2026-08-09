#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$root"
for tool in go kubectl docker; do
  command -v "$tool" >/dev/null 2>&1 || { echo "missing deployment prerequisite: $tool" >&2; exit 127; }
done

cache=${TMPDIR:-/tmp}/gpt-live-task23-go-cache
scratch=$(mktemp -d "${TMPDIR:-/tmp}/gpt-live-deploy-static.XXXXXX")
trap 'rm -rf "$scratch"' EXIT HUP INT TERM

(cd "$root" && GOCACHE="$cache" go run ./cmd/live-gateway check-config --config "$root/deploy/config/local.yaml")
local_hash=$(cd "$root" && GOCACHE="$cache" go run ./cmd/live-gateway config-hash --config "$root/deploy/config/local.yaml")
local_copy_hash=$(cd "$root" && GOCACHE="$cache" go run ./cmd/live-gateway config-hash --config "$root/deploy/k8s/overlays/local/local.yaml")
[ "$local_hash" = "$local_copy_hash" ] || { echo "local Kubernetes config copy/hash drift" >&2; exit 1; }
grep -q "config-hash: $local_hash" "$root/deploy/k8s/overlays/local/patch.yaml" || { echo "local manifest hash is stale" >&2; exit 1; }
grep -v '^#' "$root/deploy/config/production.yaml" >"$scratch/production-config.yaml"
grep -v '^#' "$root/deploy/k8s/base/production.yaml" >"$scratch/production-copy.yaml"
cmp -s "$scratch/production-config.yaml" "$scratch/production-copy.yaml" || { echo "production Kubernetes config copy drift" >&2; exit 1; }

env -i PATH="$PATH" docker compose --env-file /dev/null -f "$root/deploy/docker-compose.yml" config >"$scratch/compose.yaml"
env -i PATH="$PATH" docker compose --env-file /dev/null -f "$root/deploy/docker-compose.yml" -f "$root/deploy/compose.chaos.yml" config >"$scratch/compose-chaos.yaml"
"$root/deploy/scripts/render-kustomize.sh" local >"$scratch/local.yaml"
LIVE_PRODUCTION_RENDER_MODE=fixture "$root/deploy/scripts/render-kustomize.sh" production >"$scratch/production-fixture.yaml"
"$root/deploy/scripts/validate-render.sh" local local "$scratch/local.yaml"
"$root/deploy/scripts/validate-render.sh" production fixture "$scratch/production-fixture.yaml"
(cd "$root" && GOCACHE="$cache" go test ./deploy/validate)
(cd "$root" && GOCACHE="$cache" go test ./cmd/live-load ./internal/loadgen)
"$root/deploy/scripts/test-validate-deploy.sh"

# Deploy readiness must fail closed when reviewed production inputs are absent.
if (
  unset LIVE_PRODUCTION_RENDER_MODE LIVE_PRODUCTION_HOST LIVE_PRODUCTION_TLS_SECRET LIVE_PRODUCTION_TRUSTED_PROXY_CIDR LIVE_PRODUCTION_EGRESS_PROXY_CIDR LIVE_PRODUCTION_VOICE_URL LIVE_PRODUCTION_OIDC_ISSUER LIVE_PRODUCTION_GATEWAY_IMAGE LIVE_PRODUCTION_WORKER_IMAGE LIVE_PRODUCTION_MIGRATE_IMAGE
  "$root/deploy/scripts/render-kustomize.sh" production >/dev/null 2>&1
); then
  echo "production render accepted absent operator inputs" >&2
  exit 1
fi

if rg -n --glob '*.yaml' --glob '*.yml' '(password|api[_-]?key|bearer|token):[[:space:]]+[^{$<[:space:]]' "$root/deploy"; then
  echo "possible inline credential value in deploy files" >&2
  exit 1
fi
if rg -n --glob '*.yaml' --glob '*.yml' 'kind:[[:space:]]+Secret' "$root/deploy"; then
  echo "checked-in Secret resource detected" >&2
  exit 1
fi

[ "$(grep -Ec '^ARG (GO_IMAGE|RUNTIME_IMAGE)=.*@sha256:[0-9a-f]{64}$' "$root/Dockerfile")" -eq 2 ] || { echo "Docker base images must use immutable digests" >&2; exit 1; }
if grep -Eq '(^|[[:space:]])(apk|apt-get|dnf|yum)[[:space:]]+(add|install)' "$root/Dockerfile"; then
  echo "Dockerfile must not install packages from a mutable live repository" >&2
  exit 1
fi
if rg -n -i '(insert|update)[^;]*(raw_audio|pcm|audio_bytes)' "$root/migrations" -g '*.sql'; then
  echo "audio persistence pattern detected in migrations" >&2
  exit 1
fi
if rg -n --hidden --glob '!.git/**' --glob '!go.sum' -- '-----BEGIN (RSA |EC |OPENSSH )?PRIVATE KEY-----' "$root"; then
  echo "private key material detected" >&2
  exit 1
fi
if [ -e "$root/deploy/scripts/live-load-placeholder.sh" ] || ! grep -q '/out/live-load ./cmd/live-load' "$root/Dockerfile"; then
  echo "Docker load target is not the real live-load binary" >&2
  exit 1
fi

echo "static deployment verification passed"
