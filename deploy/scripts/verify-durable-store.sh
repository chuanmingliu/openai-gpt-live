#!/usr/bin/env bash
#
# Runs the PostgreSQL-backed integration suites against disposable containers.
# Every test provisions its own database through testcontainers, so this needs
# only a running Docker daemon and a Go toolchain. No credential is read,
# written, or printed: the containers generate their own throwaway passwords.
#
# Output is duplicated to a log file so the run can be reviewed after the fact.

set -euo pipefail

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$root"

log="$root/durable-store-verification.log"
: >"$log"

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

# Testcontainers pulls this image on first use. Pull it up front so a slow pull
# is not misread as a hung test.
say "== pulling the pinned PostgreSQL image =="
postgres_image=$(grep -o 'postgres:16-alpine@sha256:[0-9a-f]\{64\}' "$root/deploy/docker-compose.yml" | head -1)
if [[ -n "$postgres_image" ]]; then
  say "image: $postgres_image"
  docker pull "$postgres_image" >>"$log" 2>&1 || fail "could not pull $postgres_image"
else
  say "warning: pinned image not found in deploy/docker-compose.yml; testcontainers will resolve its own"
fi
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
  go test -tags=integration ./internal/store/postgres/ -run 'TestSchema' -count=1 -timeout 15m -v

run_suite "knowledge store" \
  go test -tags=integration ./internal/store/postgres/ -run 'TestKnowledge' -count=1 -timeout 15m -v

run_suite "profile store" \
  go test -tags=integration ./internal/store/postgres/ -run 'TestProfile' -count=1 -timeout 15m -v

# The race detector is where a store bug most often shows up as flakiness
# rather than a clean failure.
run_suite "knowledge + profile under -race" \
  go test -race -tags=integration ./internal/store/postgres/ -run 'TestKnowledge|TestProfile' -count=1 -timeout 20m

say "== summary =="
if [[ "$status" -eq 0 ]]; then
  say "all durable store suites passed"
else
  say "at least one suite failed; search this log for '--- FAIL' for the first cause"
fi
say "full output: $log"
exit "$status"
