#!/usr/bin/env bash
#
# Runs the PostgreSQL-backed integration suites.
#
# One disposable container serves the whole run: testkit gives every test its
# own isolated schema, so a shared server is as isolated as a container per test
# and avoids the container churn that makes long -race runs flaky. The password
# is generated per run, lives only in this process and its children, and is
# never written to the log.
#
# Output is duplicated to a log file so the run can be reviewed after the fact.

set -euo pipefail

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$root"

log="$root/durable-store-verification.log"
: >"$log"

container=""
cleanup() {
  if [[ -n "$container" ]]; then
    docker rm --force "$container" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT HUP INT TERM

say() { printf '%s\n' "$*" | tee -a "$log"; }
fail() { printf '%s\n' "$*" | tee -a "$log" >&2; exit 1; }

say "== environment =="
command -v go >/dev/null 2>&1 || fail "go is not installed or not on PATH"
say "go: $(go version)"
command -v docker >/dev/null 2>&1 || fail "docker is not installed or not on PATH"
docker info >/dev/null 2>&1 || fail "the Docker daemon is not reachable; start Docker Desktop and retry"
say "docker: $(docker version --format '{{.Server.Version}}' 2>/dev/null || echo unknown)"
say "repository: $root"
say "commit: $(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
say ""

image=$(grep -o 'postgres:16-alpine@sha256:[0-9a-f]\{64\}' "$root/deploy/docker-compose.yml" | head -1)
[[ -n "$image" ]] || fail "pinned PostgreSQL image not found in deploy/docker-compose.yml"

say "== starting one disposable PostgreSQL =="
say "image: $image"
docker pull "$image" >>"$log" 2>&1 || fail "could not pull the pinned image"

# Generated per run and never echoed. The container is published on loopback
# only and destroyed on exit, including on failure.
password=$(openssl rand -hex 24)
container=$(docker run --detach --rm \
  --publish 127.0.0.1:0:5432 \
  --env POSTGRES_DB=live \
  --env POSTGRES_USER=live \
  --env POSTGRES_PASSWORD="$password" \
  "$image" \
  -c max_connections=400) || fail "could not start the PostgreSQL container"
say "container: ${container:0:12}"

port=$(docker port "$container" 5432/tcp | head -1 | sed 's/.*://')
[[ -n "$port" ]] || fail "could not resolve the published port"

say "waiting for readiness"
ready=false
for _ in $(seq 1 60); do
  if docker exec "$container" pg_isready -U live -d live >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 1
done
[[ "$ready" == true ]] || fail "PostgreSQL did not become ready within 60s"

export TEST_DATABASE_URL="postgres://live:${password}@127.0.0.1:${port}/live?sslmode=disable"
say "ready on 127.0.0.1:${port}"
say ""

status=0

run_suite() {
  local label="$1"
  shift
  say "== $label =="
  local started
  started=$(date +%s)
  if "$@" >>"$log" 2>&1; then
    say "$label: PASS ($(( $(date +%s) - started ))s)"
  else
    say "$label: FAIL ($(( $(date +%s) - started ))s)"
    status=1
  fi
  say ""
}

# Schema first: a migration fault would make every store failure ambiguous.
run_suite "schema up/down/up" \
  go test -tags=integration ./internal/store/postgres/ -run 'TestSchema' -count=1 -timeout 20m -v

run_suite "knowledge store" \
  go test -tags=integration ./internal/store/postgres/ -run 'TestKnowledge' -count=1 -timeout 20m -v

run_suite "profile store" \
  go test -tags=integration ./internal/store/postgres/ -run 'TestProfile' -count=1 -timeout 20m -v

# The race detector is where a store bug most often shows up as flakiness
# rather than a clean failure.
run_suite "knowledge + profile under -race" \
  go test -race -tags=integration ./internal/store/postgres/ -run 'TestKnowledge|TestProfile' -count=1 -timeout 25m

say "== summary =="
if [[ "$status" -eq 0 ]]; then
  say "all durable store suites passed"
else
  say "at least one suite failed; search this log for '--- FAIL' for the first cause"
fi
say "full output: $log"
exit "$status"
