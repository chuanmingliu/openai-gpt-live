#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
scratch=$(mktemp -d "${TMPDIR:-/tmp}/gpt-live-durable-compose-test.XXXXXX")
trap 'rm -rf "$scratch"' EXIT HUP INT TERM

cat >"$scratch/docker" <<'EOF'
#!/bin/sh
set -eu

printf '%s\n' "$*" >>"${DURABLE_DOCKER_CALLS:-/dev/null}"

case "$*" in
  *' ps -q '*|*' rm --stop '*) [ -z "${DATABASE_URL-}" ] || exit 1 ;;
  *) expected_database_url='postgres://live:p%40ss%3A%2F%3F%23%25%20with%20space@postgres:5432/live?sslmode=disable'; [ "${DATABASE_URL-}" = "$expected_database_url" ] || { echo "durable Compose wrapper did not RFC 3986-encode the password" >&2; exit 1; } ;;
esac
case "$*" in
  'compose --project-name gpt-live-durable-demo -f deploy/docker-compose.yml -f deploy/compose.development-durable.yml run --rm --no-deps migrate -check'|\
  'compose --project-name gpt-live-durable-demo -f deploy/docker-compose.yml -f deploy/compose.development-control-panel.yml up --build --wait --no-deps postgres'|\
  'compose --project-name gpt-live-durable-demo -f deploy/docker-compose.yml -f deploy/compose.development-control-panel.yml run --rm --no-deps migrate'|\
  'compose --project-name gpt-live-durable-demo -f deploy/docker-compose.yml -f deploy/compose.development-control-panel.yml exec -T postgres pg_isready -U live -d live'|\
  'compose --project-name gpt-live-durable-demo -f deploy/docker-compose.yml -f deploy/compose.development-control-panel-identities.yml ps -q postgres'|\
  'compose --project-name gpt-live-durable-demo -f deploy/docker-compose.yml -f deploy/compose.development-control-panel-identities.yml rm --stop --force postgres'|\
  'compose --project-name gpt-live-durable-demo -f deploy/docker-compose.yml -f deploy/compose.development-control-panel.yml up --build --wait --no-deps gateway'|\
  'compose --project-name gpt-live-durable-demo -f deploy/docker-compose.yml -f deploy/compose.development-control-panel-identities.yml ps -q gateway'|\
  'compose --project-name gpt-live-durable-demo -f deploy/docker-compose.yml -f deploy/compose.development-control-panel-identities.yml rm --stop --force gateway'|\
  'compose --project-name gpt-live-durable-demo -f deploy/docker-compose.yml -f deploy/compose.development-control-panel.yml up --build --wait --no-deps worker'|\
  'compose --project-name gpt-live-durable-demo -f deploy/docker-compose.yml -f deploy/compose.development-control-panel-identities.yml ps -q worker'|\
  'compose --project-name gpt-live-durable-demo -f deploy/docker-compose.yml -f deploy/compose.development-control-panel-identities.yml rm --stop --force worker') ;;
  *) echo "durable Compose wrapper accepted an unexpected command" >&2; exit 1 ;;
esac
EOF
chmod +x "$scratch/docker"
: >"$scratch/docker.calls"

PATH="$scratch:$PATH" POSTGRES_PASSWORD='p@ss:/?#% with space' \
  "$root/deploy/scripts/durable-compose.sh" migrate-check

for verb in postgres-start postgres-migrate postgres-ready gateway-start worker-start; do
  PATH="$scratch:$PATH" POSTGRES_PASSWORD='p@ss:/?#% with space' \
    S2S_API_KEY=test LIVE_STATIC_BEARER_TOKEN=test LIVE_ADMIN_BEARER_TOKEN=test \
    LIVE_PRIVACY_HMAC_KEY=test LIVE_RETENTION_HMAC_KEY=test LIVE_RECONNECT_HMAC_KEY=test \
    LIVE_TOOL_ENCRYPTION_KEY=test LIVE_EXPECTED_CONFIG_HASH=test DEEPSEEK_API_KEY=test \
    "$root/deploy/scripts/durable-compose.sh" "$verb"
done

# Compose always parses the complete merged model.  These identity and stop
# verbs are deliberately credential-independent; no service secret is needed
# merely to determine whether a private owner proof still matches.
for verb in postgres-id postgres-stop gateway-id gateway-stop worker-id worker-stop; do
  env -u POSTGRES_PASSWORD -u S2S_API_KEY -u DEEPSEEK_API_KEY -u LIVE_STATIC_BEARER_TOKEN \
    -u LIVE_ADMIN_BEARER_TOKEN -u LIVE_PRIVACY_HMAC_KEY -u LIVE_RETENTION_HMAC_KEY \
    -u LIVE_RECONNECT_HMAC_KEY -u LIVE_TOOL_ENCRYPTION_KEY -u LIVE_EXPECTED_CONFIG_HASH \
    PATH="$scratch:$PATH" "$root/deploy/scripts/durable-compose.sh" "$verb"
done

if env -u POSTGRES_PASSWORD "$root/deploy/scripts/durable-compose.sh" migrate-check \
  >"$scratch/missing-password.out" 2>"$scratch/missing-password.err"; then
  echo "durable Compose wrapper accepted a missing password" >&2
  exit 1
fi
grep -Fx 'POSTGRES_PASSWORD is required' "$scratch/missing-password.err" >/dev/null || {
  echo "durable Compose wrapper missing-password error is not content-free" >&2
  exit 1
}

# Ordinary durable startup retains its canonical fail-closed model. A missing
# service credential must be rejected by the wrapper before Docker receives an
# `up` request; this is intentionally distinct from credential-free operator
# identity/stop verbs above.
for missing in POSTGRES_PASSWORD S2S_API_KEY DEEPSEEK_API_KEY LIVE_STATIC_BEARER_TOKEN LIVE_ADMIN_BEARER_TOKEN LIVE_PRIVACY_HMAC_KEY LIVE_RETENTION_HMAC_KEY LIVE_RECONNECT_HMAC_KEY LIVE_TOOL_ENCRYPTION_KEY LIVE_EXPECTED_CONFIG_HASH; do
  : >"$scratch/docker.calls"
  if PATH="$scratch:$PATH" DURABLE_DOCKER_CALLS="$scratch/docker.calls" \
    POSTGRES_PASSWORD=test S2S_API_KEY=test DEEPSEEK_API_KEY=test \
    LIVE_STATIC_BEARER_TOKEN=test LIVE_ADMIN_BEARER_TOKEN=test \
    LIVE_PRIVACY_HMAC_KEY=test LIVE_RETENTION_HMAC_KEY=test LIVE_RECONNECT_HMAC_KEY=test \
    LIVE_TOOL_ENCRYPTION_KEY=test LIVE_EXPECTED_CONFIG_HASH=test \
    sh -c 'unset "$1"; exec "$2" up' sh "$missing" "$root/deploy/scripts/durable-compose.sh" \
    >"$scratch/$missing.out" 2>"$scratch/$missing.err"; then
    echo "ordinary durable startup accepted missing $missing" >&2
    exit 1
  fi
  [ ! -s "$scratch/docker.calls" ] || { echo "ordinary durable startup invoked Docker with missing $missing" >&2; exit 1; }
done

echo "durable Compose wrapper tests passed"
