#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
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
reports=$bundle/.reports.jsonl
: >"$reports"

write_manifest() {
  result=$1
  jq -s --arg status "$result" \
    '{schema_version:"formal-source-manifest-v1",status:$status,reports:.}' \
    "$reports" >"$bundle/.manifest.json"
  mv "$bundle/.manifest.json" "$bundle/manifest.json"
}
status=failed
finish_manifest() {
  write_manifest "$status" || true
  rm -f "$reports"
}
trap finish_manifest EXIT HUP INT TERM

record() {
  artifact=$1
  selector=$2
  duration_seconds=$3
  sample_count=$4
  chaos_case=$5
  shift 5
  started_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
  "$@" >"$bundle/$artifact" 2>&1
  finished_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
  metadata=$("$root/deploy/scripts/inspect-formal-artifact.sh" "$bundle/$artifact")
  source_name=${metadata%% *}
  source_sha256=${metadata#* }
  [ "$source_name" = "$artifact" ]
  jq -cn \
    --arg name "$artifact" --arg selector "$selector" \
    --arg started_at "$started_at" --arg finished_at "$finished_at" \
    --arg source_sha256 "$source_sha256" --arg chaos_case "$chaos_case" \
    --argjson duration_seconds "$duration_seconds" --argjson sample_count "$sample_count" \
    '{name:$name,selector:$selector,started_at:$started_at,finished_at:$finished_at,source_sha256:$source_sha256,duration_seconds:$duration_seconds,sample_count:$sample_count,chaos_case:$chaos_case}' \
    >>"$reports"
}

record unit-race.json 'go test -json -race -count=1 ./cmd/live-load ./internal/loadgen' 0 0 '' \
  go test -json -race -count=1 ./cmd/live-load ./internal/loadgen

if [ "$mode" = formal ]; then
  record capacity-profile-slo-10m.json 'go test -json -count=1 -tags=load -run ^TestGateway250Sessions$ -timeout=20m ./test/load/...' 600 0 '' \
    env LIVE_CAPACITY_DURATION=10m LIVE_ENFORCE_PINNED_SLO=1 \
    go test -json -count=1 -tags=load -run '^TestGateway250Sessions$' -timeout=20m ./test/load/...
  record barge-in-20.json 'go test -json -count=1 -tags=load -run ^TestBargeInAcknowledgedFenceSuppressesEveryStaleFrame$ -timeout=2m ./test/load/...' 0 20 '' \
    go test -json -count=1 -tags=load -run '^TestBargeInAcknowledgedFenceSuppressesEveryStaleFrame$' -timeout=2m ./test/load/...
  record chaos-postgresql-disconnect.json 'go test -json -count=1 -tags=chaos -run ^TestPostgreSQLDisconnect$ -timeout=30m ./test/chaos/...' 0 0 postgresql_disconnect \
    go test -json -count=1 -tags=chaos -run '^TestPostgreSQLDisconnect$' -timeout=30m ./test/chaos/...
  record chaos-deepseek-429.json 'go test -json -count=1 -tags=chaos -run ^TestDeepSeek429HonorsBoundedRetryAfter$ -timeout=30m ./test/chaos/...' 0 0 deepseek_429 \
    go test -json -count=1 -tags=chaos -run '^TestDeepSeek429HonorsBoundedRetryAfter$' -timeout=30m ./test/chaos/...
  record chaos-slow-client-backpressure.json 'go test -json -count=1 -tags=chaos -run ^TestSlowClientBackpressure$ -timeout=30m ./test/chaos/...' 0 0 slow_client_backpressure \
    go test -json -count=1 -tags=chaos -run '^TestSlowClientBackpressure$' -timeout=30m ./test/chaos/...
  record chaos-stalled-voice-provider.json 'go test -json -count=1 -tags=chaos -run ^TestStalledVoiceProvider$ -timeout=30m ./test/chaos/...' 0 0 stalled_voice_provider \
    go test -json -count=1 -tags=chaos -run '^TestStalledVoiceProvider$' -timeout=30m ./test/chaos/...
  record chaos-killed-worker-lease-recovery.json 'go test -json -count=1 -tags=chaos -run ^TestKilledWorkerLeaseRecovery$ -timeout=30m ./test/chaos/...' 0 0 killed_worker_lease_recovery \
    go test -json -count=1 -tags=chaos -run '^TestKilledWorkerLeaseRecovery$' -timeout=30m ./test/chaos/...
  record chaos-lost-notification-poll-repair.json 'go test -json -count=1 -tags=chaos -run ^TestLostNotificationPollRepair$ -timeout=30m ./test/chaos/...' 0 0 lost_notification_poll_repair \
    go test -json -count=1 -tags=chaos -run '^TestLostNotificationPollRepair$' -timeout=30m ./test/chaos/...
  record soak-2h.json 'go test -json -count=1 -tags=soak -run ^TestGatewaySoak$ -timeout=150m ./test/load/...' 7200 0 '' \
    env LIVE_SOAK_DURATION=2h \
    go test -json -count=1 -tags=soak -run '^TestGatewaySoak$' -timeout=150m ./test/load/...
else
  record capacity-smoke.json 'smoke-only TestGateway250Sessions' 0 0 '' \
    env LIVE_CAPACITY_DURATION="${LIVE_SMOKE_CAPACITY_DURATION:-2s}" LIVE_ENFORCE_PINNED_SLO= \
    go test -json -count=1 -tags=load -run '^TestGateway250Sessions$' -timeout=2m ./test/load/...
  record barge-in-smoke.json 'smoke-only TestBargeInAcknowledgedFenceSuppressesEveryStaleFrame' 0 20 '' \
    go test -json -count=1 -tags=load -run '^TestBargeInAcknowledgedFenceSuppressesEveryStaleFrame$' -timeout=2m ./test/load/...
  record soak-smoke.json 'smoke-only TestGatewaySoak' 0 0 '' \
    env LIVE_SOAK_DURATION="${LIVE_SMOKE_SOAK_DURATION:-10s}" \
    go test -json -count=1 -tags=soak -run '^TestGatewaySoak$' -timeout=2m ./test/load/...
fi

status=passed
write_manifest "$status"
rm -f "$reports"
trap - EXIT HUP INT TERM
printf '%s\n' "$bundle"
