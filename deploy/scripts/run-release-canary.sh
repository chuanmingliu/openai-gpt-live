#!/bin/sh
set -eu

[ "$#" -eq 2 ] || { echo "usage: run-release-canary.sh <binary> <evidence-out>" >&2; exit 64; }
binary=$1
evidence_out=$2
case "$binary" in /*) ;; *) echo "canary binary must be absolute" >&2; exit 64 ;; esac
case "$evidence_out" in /*) ;; *) echo "canary evidence output must be absolute" >&2; exit 64 ;; esac
[ -x "$binary" ] || { echo "canary binary is not executable" >&2; exit 64; }
[ ! -e "$evidence_out" ] || { echo "canary evidence output already exists" >&2; exit 1; }

for name in LIVE_CANARY_GATEWAY_URL LIVE_CANARY_ORIGIN LIVE_CANARY_RETENTION \
  LIVE_CANARY_BEARER_TOKEN_FILE LIVE_DEPLOYMENT_REGION LIVE_EXPECTED_CONFIG_HASH \
  LIVE_EXPECTED_GATEWAY_IMAGE
do
  value=$(printenv "$name" 2>/dev/null || true)
  [ -n "$value" ] || { echo "missing required canary runtime input: $name" >&2; exit 1; }
done
[ "$LIVE_CANARY_RETENTION" = none ] || { echo "release canary retention must be none" >&2; exit 1; }

"$binary" --once --evidence-out "$evidence_out"
