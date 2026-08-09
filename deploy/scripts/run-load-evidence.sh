#!/bin/sh
set -eu

mode=${1:-}
case "$mode" in
  formal|smoke) ;;
  *) echo "usage: run-load-evidence.sh formal|smoke" >&2; exit 64 ;;
esac

: "${LIVE_EVIDENCE_DIR:?LIVE_EVIDENCE_DIR must name an explicit artifact directory}"
case "$LIVE_EVIDENCE_DIR" in
  /*) ;;
  *) echo "LIVE_EVIDENCE_DIR must be absolute" >&2; exit 64 ;;
esac

if [ "$mode" = formal ]; then
  : "${CHAOS_TOXIPROXY_URL:?formal evidence requires the disposable chaos topology}"
  : "${CHAOS_POSTGRES_PROXY:?formal evidence requires the disposable chaos topology}"
  : "${CHAOS_VOICE_PROXY:?formal evidence requires the disposable chaos topology}"
  : "${CHAOS_DEEPSEEK_PROXY:?formal evidence requires the disposable chaos topology}"
  : "${CHAOS_VOICE_CONTROL_URL:?formal evidence requires the disposable chaos topology}"
  : "${CHAOS_DEEPSEEK_CONTROL_URL:?formal evidence requires the disposable chaos topology}"
  : "${CHAOS_GATEWAY_URL:?formal evidence requires the disposable chaos topology}"
  : "${CHAOS_GATEWAY_HEALTH_URL:?formal evidence requires the disposable chaos topology}"
  : "${CHAOS_WORKER_HEALTH_URL:?formal evidence requires the disposable chaos topology}"
  : "${CHAOS_BEARER_TOKEN:?formal evidence requires the disposable chaos topology}"
  : "${CHAOS_OIDC_CONTROL_URL:?formal evidence requires the disposable chaos topology}"
  : "${CHAOS_COMPOSE_PROJECT:?formal evidence requires the disposable chaos topology}"
  : "${CHAOS_COMPOSE_BASE:?formal evidence requires the disposable chaos topology}"
  : "${CHAOS_COMPOSE_OVERRIDE:?formal evidence requires the disposable chaos topology}"
  : "${CHAOS_DATABASE_URL:?formal evidence requires the disposable chaos topology}"
fi

mkdir -p "$LIVE_EVIDENCE_DIR"
bundle=$(mktemp -d "$LIVE_EVIDENCE_DIR/task24-$mode.XXXXXXXX")
manifest="$bundle/manifest.jsonl"
printf '{"schema_version":"task24-evidence-v1","evidence_class":"%s","status":"running"}\n' "$mode" >"$manifest"
status=failed
finish_manifest() {
  printf '{"status":"%s"}\n' "$status" >>"$manifest"
}
trap finish_manifest EXIT HUP INT TERM

digest_file() {
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    sha256sum "$1" | awk '{print $1}'
  fi
}

record() {
  artifact=$1
  selector=$2
  shift 2
  "$@" >"$bundle/$artifact" 2>&1
  digest=$(digest_file "$bundle/$artifact")
  printf '{"artifact":"%s","selector":"%s","sha256":"%s"}\n' "$artifact" "$selector" "$digest" >>"$manifest"
}

record unit-race.jsonl 'go test -race ./cmd/live-load ./internal/loadgen' \
  go test -json -race -count=1 ./cmd/live-load ./internal/loadgen

if [ "$mode" = formal ]; then
  record capacity-profile-slo.jsonl '^TestGateway250Sessions$; capacity_10m; checked-in exact profile; SLO enforced' \
    env LIVE_CAPACITY_DURATION=10m LIVE_ENFORCE_PINNED_SLO=1 \
    go test -json -count=1 -tags=load -run '^TestGateway250Sessions$' -timeout=20m ./test/load/...
  record barge-in.jsonl '^TestBargeInAcknowledgedFenceSuppressesEveryStaleFrame$; 20 samples' \
    go test -json -count=1 -tags=load -run '^TestBargeInAcknowledgedFenceSuppressesEveryStaleFrame$' -timeout=2m ./test/load/...
  record chaos.jsonl 'six named application-path chaos scenarios' \
    go test -json -count=1 -tags=chaos -run '^(TestPostgreSQLDisconnect|TestDeepSeek429HonorsBoundedRetryAfter|TestSlowClientBackpressure|TestStalledVoiceProvider|TestKilledWorkerLeaseRecovery|TestLostNotificationPollRepair)$' -timeout=30m ./test/chaos/...
  record soak-2h.jsonl '^TestGatewaySoak$; soak_2h' \
    env LIVE_SOAK_DURATION=2h \
    go test -json -count=1 -tags=soak -run '^TestGatewaySoak$' -timeout=150m ./test/load/...
else
  record capacity-smoke.jsonl '^TestGateway250Sessions$; smoke only' \
    env LIVE_CAPACITY_DURATION="${LIVE_SMOKE_CAPACITY_DURATION:-2s}" LIVE_ENFORCE_PINNED_SLO= \
    go test -json -count=1 -tags=load -run '^TestGateway250Sessions$' -timeout=2m ./test/load/...
  record barge-in-smoke.jsonl '^TestBargeInAcknowledgedFenceSuppressesEveryStaleFrame$; 20 samples' \
    go test -json -count=1 -tags=load -run '^TestBargeInAcknowledgedFenceSuppressesEveryStaleFrame$' -timeout=2m ./test/load/...
  record soak-smoke.jsonl '^TestGatewaySoak$; smoke only' \
    env LIVE_SOAK_DURATION="${LIVE_SMOKE_SOAK_DURATION:-10s}" \
    go test -json -count=1 -tags=soak -run '^TestGatewaySoak$' -timeout=2m ./test/load/...
fi

status=passed
trap - EXIT HUP INT TERM
finish_manifest
printf '%s\n' "$bundle"
