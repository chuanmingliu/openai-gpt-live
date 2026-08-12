#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
scratch=$(mktemp -d "${TMPDIR:-/tmp}/gpt-live-durable-compose-test.XXXXXX")
trap 'rm -rf "$scratch"' EXIT HUP INT TERM

cat >"$scratch/docker" <<'EOF'
#!/bin/sh
set -eu

expected_database_url='postgres://live:p%40ss%3A%2F%3F%23%25%20with%20space@postgres:5432/live?sslmode=disable'
[ "${DATABASE_URL-}" = "$expected_database_url" ] || {
  echo "durable Compose wrapper did not RFC 3986-encode the password" >&2
  exit 1
}
[ "$*" = 'compose --project-name gpt-live-durable-demo -f deploy/docker-compose.yml -f deploy/compose.development-durable.yml run --rm --no-deps migrate -check' ] || {
  echo "durable Compose wrapper changed the migration check command" >&2
  exit 1
}
EOF
chmod +x "$scratch/docker"

PATH="$scratch:$PATH" POSTGRES_PASSWORD='p@ss:/?#% with space' \
  "$root/deploy/scripts/durable-compose.sh" migrate-check

if env -u POSTGRES_PASSWORD "$root/deploy/scripts/durable-compose.sh" migrate-check \
  >"$scratch/missing-password.out" 2>"$scratch/missing-password.err"; then
  echo "durable Compose wrapper accepted a missing password" >&2
  exit 1
fi
grep -Fx 'POSTGRES_PASSWORD is required' "$scratch/missing-password.err" >/dev/null || {
  echo "durable Compose wrapper missing-password error is not content-free" >&2
  exit 1
}

echo "durable Compose wrapper tests passed"
