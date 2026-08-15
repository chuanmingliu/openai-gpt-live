#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$root"

command -v docker >/dev/null 2>&1 || {
  echo "docker is required for control-panel Compose render verification" >&2
  exit 127
}

# No daemon, provider, volume, or container mutation is involved: `config`
# only renders the three checked-in Compose files.  Empty credentials exercise
# the exact preflight path used by every identity/stop operation.
env -u POSTGRES_PASSWORD -u DATABASE_URL -u S2S_API_KEY -u DEEPSEEK_API_KEY \
  -u LIVE_STATIC_BEARER_TOKEN -u LIVE_ADMIN_BEARER_TOKEN -u LIVE_PRIVACY_HMAC_KEY \
  -u LIVE_RETENTION_HMAC_KEY -u LIVE_RECONNECT_HMAC_KEY -u LIVE_TOOL_ENCRYPTION_KEY \
  -u LIVE_EXPECTED_CONFIG_HASH \
  docker compose --project-name gpt-live-durable-demo \
    -f deploy/docker-compose.yml \
    -f deploy/compose.development-durable.yml \
    -f deploy/compose.development-control-panel.yml config -q

echo "control-panel Compose render test passed"
