#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$root"
for name in POSTGRES_PASSWORD DATABASE_URL S2S_API_KEY DEEPSEEK_API_KEY LIVE_STATIC_BEARER_TOKEN LIVE_ADMIN_BEARER_TOKEN LIVE_PRIVACY_HMAC_KEY LIVE_RETENTION_HMAC_KEY LIVE_TOOL_ENCRYPTION_KEY S2S_IMAGE S2S_CONFIG_PATH; do
  eval "present=\${$name+x}"
  [ "$present" = x ] || { echo "missing smoke prerequisite environment name: $name" >&2; exit 1; }
done
[ "$LIVE_ADMIN_BEARER_TOKEN" = "$LIVE_STATIC_BEARER_TOKEN" ] || { echo "local smoke requires LIVE_ADMIN_BEARER_TOKEN and LIVE_STATIC_BEARER_TOKEN to reference the same scoped value" >&2; exit 1; }
for tool in docker go curl; do
  command -v "$tool" >/dev/null 2>&1 || { echo "missing smoke prerequisite: $tool" >&2; exit 127; }
done
cache=${TMPDIR:-/tmp}/gpt-live-task23-go-cache
LIVE_EXPECTED_CONFIG_HASH=$(cd "$root" && GOCACHE="$cache" go run ./cmd/live-gateway config-hash --config "$root/deploy/config/local.yaml")
export LIVE_EXPECTED_CONFIG_HASH
compose="docker compose --env-file /dev/null -f $root/deploy/docker-compose.yml"
cleanup() { $compose down --volumes --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT HUP INT TERM
$compose up --detach --build --wait --wait-timeout 180
(cd "$root" && GOCACHE="$cache" LIVE_SMOKE_GATEWAY_URL="ws://127.0.0.1:${GATEWAY_PORT:-18080}/v1/realtime" go run ./deploy/scripts/smoke-client.go)
$compose exec -T gateway /usr/local/bin/live-gateway drain --url http://127.0.0.1:9090/admin/v1/drain
i=0
while [ "$i" -lt 15 ]; do
  code=$(curl --silent --output /dev/null --write-out '%{http_code}' "http://127.0.0.1:${GATEWAY_HEALTH_PORT:-19090}/health/ready" || true)
  [ "$code" = 503 ] && exit 0
  i=$((i+1))
  sleep 1
done
echo "gateway readiness did not fall after drain" >&2
exit 1
