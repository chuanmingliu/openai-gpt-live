#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$root"

[ "$#" -eq 1 ] || {
  echo "usage: deploy/scripts/durable-compose.sh {postgres-start|postgres-migrate|postgres-ready|postgres-id|postgres-stop|gateway-start|gateway-id|gateway-stop|worker-start|worker-id|worker-stop}" >&2
  exit 2
}
operation=${1:-}
case "${1:-}" in
  migrate-up)
    set -- up --build --wait postgres migrate
    ;;
  migrate-check)
    set -- run --rm --no-deps migrate -check
    ;;
  up)
    set -- up --build --wait speech-to-speech gateway worker
    ;;
  down)
    set -- down
    ;;
  postgres-start)
    set -- up --build --wait --no-deps postgres
    ;;
  postgres-migrate)
    set -- run --rm --no-deps migrate
    ;;
  postgres-ready)
    set -- exec -T postgres pg_isready -U live -d live
    ;;
  postgres-id)
    set -- ps -q postgres
    ;;
  postgres-stop)
    set -- rm --stop --force postgres
    ;;
  gateway-start)
    set -- up --build --wait --no-deps gateway
    ;;
  gateway-id)
    set -- ps -q gateway
    ;;
  gateway-stop)
    set -- rm --stop --force gateway
    ;;
  worker-start)
    set -- up --build --wait --no-deps worker
    ;;
  worker-id)
    set -- ps -q worker
    ;;
  worker-stop)
    set -- rm --stop --force worker
    ;;
  *)
    echo "usage: deploy/scripts/durable-compose.sh {migrate-up|migrate-check|up|down}" >&2
    exit 2
    ;;
esac

case "$operation" in
  *-id|*-stop) ;;
  postgres-start|postgres-migrate|postgres-ready|migrate-up|migrate-check|up|gateway-start|worker-start)
    [ -n "${POSTGRES_PASSWORD:-}" ] || { echo "POSTGRES_PASSWORD is required" >&2; exit 1; }
    command -v jq >/dev/null 2>&1 || { echo "jq is required to encode the durable PostgreSQL password" >&2; exit 127; }
    encoded_password=$(printf '%s' "$POSTGRES_PASSWORD" | jq -sRr @uri)
    DATABASE_URL="postgres://live:${encoded_password}@postgres:5432/live?sslmode=disable"
    export DATABASE_URL
    unset encoded_password
    ;;
esac

case "$operation" in
  up)
    [ -n "${S2S_API_KEY:-}" ] || { echo "S2S_API_KEY is required" >&2; exit 1; }
    [ -n "${DEEPSEEK_API_KEY:-}" ] || { echo "DEEPSEEK_API_KEY is required" >&2; exit 1; }
    [ -n "${LIVE_STATIC_BEARER_TOKEN:-}" ] || { echo "LIVE_STATIC_BEARER_TOKEN is required" >&2; exit 1; }
    [ -n "${LIVE_ADMIN_BEARER_TOKEN:-}" ] || { echo "LIVE_ADMIN_BEARER_TOKEN is required" >&2; exit 1; }
    [ -n "${LIVE_PRIVACY_HMAC_KEY:-}" ] || { echo "LIVE_PRIVACY_HMAC_KEY is required" >&2; exit 1; }
    [ -n "${LIVE_RETENTION_HMAC_KEY:-}" ] || { echo "LIVE_RETENTION_HMAC_KEY is required" >&2; exit 1; }
    [ -n "${LIVE_RECONNECT_HMAC_KEY:-}" ] || { echo "LIVE_RECONNECT_HMAC_KEY is required" >&2; exit 1; }
    [ -n "${LIVE_TOOL_ENCRYPTION_KEY:-}" ] || { echo "LIVE_TOOL_ENCRYPTION_KEY is required" >&2; exit 1; }
    [ -n "${LIVE_EXPECTED_CONFIG_HASH:-}" ] || { echo "LIVE_EXPECTED_CONFIG_HASH is required" >&2; exit 1; }
    ;;
esac

case "$operation" in
  gateway-start)
    [ -n "${S2S_API_KEY:-}" ] || { echo "S2S_API_KEY is required" >&2; exit 1; }
    [ -n "${LIVE_STATIC_BEARER_TOKEN:-}" ] || { echo "LIVE_STATIC_BEARER_TOKEN is required" >&2; exit 1; }
    [ -n "${LIVE_ADMIN_BEARER_TOKEN:-}" ] || { echo "LIVE_ADMIN_BEARER_TOKEN is required" >&2; exit 1; }
    [ -n "${LIVE_PRIVACY_HMAC_KEY:-}" ] || { echo "LIVE_PRIVACY_HMAC_KEY is required" >&2; exit 1; }
    [ -n "${LIVE_RETENTION_HMAC_KEY:-}" ] || { echo "LIVE_RETENTION_HMAC_KEY is required" >&2; exit 1; }
    [ -n "${LIVE_RECONNECT_HMAC_KEY:-}" ] || { echo "LIVE_RECONNECT_HMAC_KEY is required" >&2; exit 1; }
    [ -n "${LIVE_TOOL_ENCRYPTION_KEY:-}" ] || { echo "LIVE_TOOL_ENCRYPTION_KEY is required" >&2; exit 1; }
    [ -n "${LIVE_EXPECTED_CONFIG_HASH:-}" ] || { echo "LIVE_EXPECTED_CONFIG_HASH is required" >&2; exit 1; }
    ;;
  worker-start)
    [ -n "${DEEPSEEK_API_KEY:-}" ] || { echo "DEEPSEEK_API_KEY is required" >&2; exit 1; }
    [ -n "${LIVE_STATIC_BEARER_TOKEN:-}" ] || { echo "LIVE_STATIC_BEARER_TOKEN is required" >&2; exit 1; }
    [ -n "${LIVE_ADMIN_BEARER_TOKEN:-}" ] || { echo "LIVE_ADMIN_BEARER_TOKEN is required" >&2; exit 1; }
    [ -n "${LIVE_PRIVACY_HMAC_KEY:-}" ] || { echo "LIVE_PRIVACY_HMAC_KEY is required" >&2; exit 1; }
    [ -n "${LIVE_RETENTION_HMAC_KEY:-}" ] || { echo "LIVE_RETENTION_HMAC_KEY is required" >&2; exit 1; }
    [ -n "${LIVE_TOOL_ENCRYPTION_KEY:-}" ] || { echo "LIVE_TOOL_ENCRYPTION_KEY is required" >&2; exit 1; }
    [ -n "${LIVE_EXPECTED_CONFIG_HASH:-}" ] || { echo "LIVE_EXPECTED_CONFIG_HASH is required" >&2; exit 1; }
    ;;
esac

case "$operation" in
  *-id|*-stop)
    exec docker compose \
      --project-name gpt-live-durable-demo \
      -f deploy/docker-compose.yml \
      -f deploy/compose.development-control-panel-identities.yml \
      "$@"
    ;;
  postgres-*|gateway-*|worker-*)
    exec docker compose \
      --project-name gpt-live-durable-demo \
      -f deploy/docker-compose.yml \
      -f deploy/compose.development-control-panel.yml \
      "$@"
    ;;
  *)
    exec docker compose \
      --project-name gpt-live-durable-demo \
      -f deploy/docker-compose.yml \
      -f deploy/compose.development-durable.yml \
      "$@"
    ;;
esac
