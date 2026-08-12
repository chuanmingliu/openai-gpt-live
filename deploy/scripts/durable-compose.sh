#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$root"

[ -n "${POSTGRES_PASSWORD:-}" ] || {
  echo "POSTGRES_PASSWORD is required" >&2
  exit 1
}
command -v jq >/dev/null 2>&1 || {
  echo "jq is required to encode the durable PostgreSQL password" >&2
  exit 127
}

# Keep the raw password off argv and stdout. jq reads it from stdin; command
# substitution captures only its RFC 3986 encoding for the Compose child.
encoded_password=$(printf '%s' "$POSTGRES_PASSWORD" | jq -sRr @uri)
DATABASE_URL="postgres://live:${encoded_password}@postgres:5432/live?sslmode=disable"
export DATABASE_URL
unset encoded_password

[ "$#" -eq 1 ] || {
  echo "usage: deploy/scripts/durable-compose.sh {migrate-up|migrate-check|up|down}" >&2
  exit 2
}
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
  *)
    echo "usage: deploy/scripts/durable-compose.sh {migrate-up|migrate-check|up|down}" >&2
    exit 2
    ;;
esac

exec docker compose \
  --project-name gpt-live-durable-demo \
  -f deploy/docker-compose.yml \
  -f deploy/compose.development-durable.yml \
  "$@"
