#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$root"
for tool in go kubectl docker jq; do
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

(cd "$root" && GOCACHE="$cache" go run ./cmd/live-gateway check-config --config "$root/deploy/config/development-durable.yaml")
(cd "$root" && GOCACHE="$cache" go run ./cmd/live-worker check-config --config "$root/deploy/config/development-durable.yaml")
durable_gateway_hash=$(cd "$root" && GOCACHE="$cache" go run ./cmd/live-gateway config-hash --config "$root/deploy/config/development-durable.yaml")
durable_worker_hash=$(cd "$root" && GOCACHE="$cache" go run ./cmd/live-worker config-hash --config "$root/deploy/config/development-durable.yaml")
[ "$durable_gateway_hash" = "$durable_worker_hash" ] || { echo "durable gateway/worker config hash drift" >&2; exit 1; }
for vertical_config in "$root/deploy/config/development-durable.yaml" "$root/deploy/config/development-control-panel.yaml"; do
  grep -A4 'vertical_profiles:' "$vertical_config" | grep -q 'enabled: true' || {
    echo "vertical profile deployment config is not enabled: $vertical_config" >&2
    exit 1
  }
done

env -i PATH="$PATH" docker compose --env-file /dev/null -f "$root/deploy/docker-compose.yml" config >"$scratch/compose.yaml"
env -i PATH="$PATH" docker compose --env-file /dev/null -f "$root/deploy/docker-compose.yml" -f "$root/deploy/compose.chaos.yml" config >"$scratch/compose-chaos.yaml"
env -i PATH="$PATH" \
  POSTGRES_PASSWORD='p@ss:/?#% with space' \
  DATABASE_URL='postgres://live:p%40ss%3A%2F%3F%23%25%20with%20space@postgres:5432/live?sslmode=disable' \
  S2S_API_KEY=speech-local \
  DEEPSEEK_API_KEY=reasoner-local \
  LIVE_STATIC_BEARER_TOKEN=bearer-local \
  LIVE_ADMIN_BEARER_TOKEN=bearer-local \
  LIVE_PRIVACY_HMAC_KEY=privacy-local \
  LIVE_RETENTION_HMAC_KEY=retention-local \
  LIVE_RECONNECT_HMAC_KEY=reconnect-local \
  LIVE_TOOL_ENCRYPTION_KEY=tool-local \
  LIVE_EXPECTED_CONFIG_HASH="$durable_gateway_hash" \
  S2S_IMAGE=speech-to-speech:local \
  S2S_CONFIG_PATH="$root/deploy/config/local.yaml" \
  docker compose --env-file /dev/null -f "$root/deploy/docker-compose.yml" -f "$root/deploy/compose.development-durable.yml" config >"$scratch/compose-development-durable.yaml"
"$root/deploy/scripts/test-durable-compose.sh"
"$root/deploy/scripts/render-kustomize.sh" local >"$scratch/local.yaml"
LIVE_PRODUCTION_RENDER_MODE=fixture "$root/deploy/scripts/render-kustomize.sh" production >"$scratch/production-fixture.yaml"
"$root/deploy/scripts/validate-render.sh" local local "$scratch/local.yaml"
"$root/deploy/scripts/validate-render.sh" production fixture "$scratch/production-fixture.yaml"
(cd "$root" && GOCACHE="$cache" go test ./deploy/validate)
(cd "$root" && GOCACHE="$cache" go test ./cmd/live-load ./internal/loadgen)
"$root/deploy/scripts/test-validate-deploy.sh"
"$root/deploy/scripts/test-release-gate-boundaries.sh"

# Deploy readiness must fail closed when reviewed production inputs are absent.
if (
  unset LIVE_PRODUCTION_RENDER_MODE LIVE_PRODUCTION_HOST LIVE_PRODUCTION_TLS_SECRET LIVE_PRODUCTION_TRUSTED_PROXY_CIDR LIVE_PRODUCTION_EGRESS_PROXY_CIDR LIVE_PRODUCTION_VOICE_URL LIVE_PRODUCTION_OIDC_ISSUER LIVE_DEPLOYMENT_REGION LIVE_PRODUCTION_GATEWAY_IMAGE LIVE_PRODUCTION_WORKER_IMAGE LIVE_PRODUCTION_MIGRATE_IMAGE
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
if rg -n --glob '!**/references/verification.md' '(deepseek-chat|deepseek-reasoner)' \
  "$root/README.md" "$root/docs/demo" "$root/deploy/config" \
  "$root/deploy/k8s" "$root/configs" "$root/skills/gpt-live-go-framework"; then
  echo "retired DeepSeek model identifier detected in active docs/config/skill" >&2
  exit 1
fi
if rg -n --glob '!**/references/verification.md' '(npx[[:space:]]+-y|@modelcontextprotocol/server-duckduckgo)' \
  "$root/README.md" "$root/docs/demo" "$root/deploy/config" "$root/deploy/k8s" \
  "$root/configs" "$root/skills/gpt-live-go-framework"; then
  echo "dynamic tool package instruction detected" >&2
  exit 1
fi
if rg -n 'go run ./cmd/live-migrate -database-url-env DATABASE_URL' \
  "$root/docs/demo" "$root/skills/gpt-live-go-framework/references/realtime-web-search.md"; then
  echo "durable Compose guide contains a host migration command" >&2
  exit 1
fi
if rg -n -F 'postgres://live:${POSTGRES_PASSWORD}' \
  "$root/Makefile" "$root/README.md" "$root/docs/demo" \
  "$root/deploy/compose.development-durable.yml" \
  "$root/skills/gpt-live-go-framework/references/realtime-web-search.md"; then
  echo "raw PostgreSQL password interpolation detected in URI" >&2
  exit 1
fi
if [ -e "$root/deploy/scripts/live-load-placeholder.sh" ] || ! grep -q '/out/live-load ./cmd/live-load' "$root/Dockerfile"; then
  echo "Docker load target is not the real live-load binary" >&2
  exit 1
fi

echo "static deployment verification passed"
