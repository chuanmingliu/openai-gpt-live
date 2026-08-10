#!/bin/sh
set -uf

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
evidence_out=
commit=
config_hash=
attestation=
attestation_key=
verify_existing=0
while [ "$#" -gt 0 ]; do
  case "$1" in
    --evidence-out) evidence_out=${2-}; shift 2 ;;
    --commit) commit=${2-}; shift 2 ;;
    --config-hash) config_hash=${2-}; shift 2 ;;
    --attestation) attestation=${2-}; shift 2 ;;
    --attestation-key) attestation_key=${2-}; shift 2 ;;
    --verify-existing) verify_existing=1; shift ;;
    *) echo "unknown release-gate argument: $1" >&2; exit 64 ;;
  esac
done

case "$evidence_out" in
  /*/artifacts/release/"$commit"/"$config_hash"/evidence.json) ;;
  *) echo "--evidence-out must be the absolute artifacts/release/<commit>/<config-hash>/evidence.json path" >&2; exit 64 ;;
esac
case "$commit" in
  ???????*) ;;
  *) echo "--commit is required" >&2; exit 64 ;;
esac
case "$config_hash" in
  v1:????????????????????????????????????????????????????????????????) ;;
  *) echo "--config-hash is malformed" >&2; exit 64 ;;
esac

bundle=$(dirname -- "$evidence_out")
gates=$bundle/gates
if [ "$verify_existing" -eq 0 ]; then
  mkdir -p "$(dirname -- "$bundle")"
  mkdir -m 700 "$bundle" || { echo "release bundle must be a new private directory" >&2; exit 1; }
fi
mkdir -p "$gates" "$bundle/artifacts"
export RELEASE_BUNDLE_ARTIFACTS="$bundle/artifacts"
release_gate_dir=$(mktemp -d "${TMPDIR:-/tmp}/gpt-live-release-gate.XXXXXX")
trap 'rm -rf "$release_gate_dir"' EXIT HUP INT TERM
release_gate="$release_gate_dir/live-release-gate"
(cd "$root" && go build -trimpath -o "$release_gate" ./cmd/live-release-gate) || exit 1
artifact_specs="$release_gate_dir/artifacts.spec"
: >"$artifact_specs"
add_artifact() {
  [ "$#" -eq 2 ] || exit 1
  printf '%s=%s\n' "$1" "$2" >>"$artifact_specs"
}

failed=0
record() {
  gate_id=$1
  producer=$2
  shift 2
  "$release_gate" record \
    --gate-id "$gate_id" \
    --producer "$producer" \
    --artifact-root "$bundle" \
    --artifact "gates/$gate_id.summary.json" \
    --evidence-out "$gates/$gate_id.json" \
    -- "$@" || failed=1
}

cd "$root" || exit 1

if [ "$verify_existing" -eq 0 ]; then
  # Keep this order synchronized with releasegate.RequiredGateIDs. Every producer
  # receives an explicit evidence output and writes only content-safe test output.
  record simulator-voice release-producer-v1/simulator-voice-contract sh -c 'test -n "${S2S_REALTIME_URL:-}" && go test -json -count=1 -tags=contract ./test/contract/...'
  record durable-acceptance release-producer-v1/durable-acceptance-integration sh -c 'test -n "${TEST_DATABASE_URL:-}" && go test -json -count=1 -tags=integration -run "^TestVoiceContinuesDuringDurableHandoff$" ./test/integration/...'
  record correlation release-producer-v1/correlation-integration sh -c 'test -n "${TEST_DATABASE_URL:-}" && go test -json -count=1 -tags=integration -run "^(TestTwoLiveWebSocketsKeepPCMEventsAndResponseEpochsIsolated|TestDuplicateCompletionHasOneExecutorEffectAndOneClientTerminal)$" ./test/integration/...'
  record presentation release-producer-v1/presentation-integration sh -c 'test -n "${TEST_DATABASE_URL:-}" && go test -json -count=1 -tags=integration -run "^(TestDroppedListenerNotificationRepairsThroughPollAndCursor|TestUnsafeWorkerResultSuppressesDurableClientAndProviderPresentation|TestWorkerCompletionDuringSpeechRemainsVisualOnly)$" ./test/integration/...'
  record steer-cancel release-producer-v1/steer-cancel-integration sh -c 'test -n "${TEST_DATABASE_URL:-}" && go test -json -count=1 -tags=integration -run "^(TestProviderSteerAndCancelTraverseDurableWorkerLifecycle|TestPostgresServicePersistsSteeringAndCancelRequest|TestPostgresLeaseExpiryFencesOldOwner)$" ./test/integration/...'
  record epoch-fencing release-producer-v1/epoch-fencing-race go test -json -race -count=1 -run '^(TestEpochFenceMonotonicallyAdvancesAndAcceptsOnlyCurrent|TestResponseOutputRequiresCurrentEpochAndIncarnation|TestOldEpochProviderErrorCannotTerminateCurrentResponse)$' ./internal/session/...
  record reconnect release-producer-v1/reconnect-integration sh -c 'test -n "${TEST_DATABASE_URL:-}" && go test -json -count=1 -tags=integration -run "^TestReconnectDisconnectsProviderWithQueuedAudioAndFencesStaleOutput$" ./test/integration/...'
  record restart-durability release-producer-v1/restart-durability-integration sh -c 'test -n "${TEST_DATABASE_URL:-}" && go test -json -count=1 -tags=integration -run "^(TestRunnerCancellationRestartRecoversExpiredLease|TestPostgresDeliveryCursorReadsDurableSuffixInOrder)$" ./test/integration/...'
  record verification-suites release-producer-v1/verification-suites sh -c 'go vet ./... && go test ./... && go test -race ./... && test -n "${TEST_DATABASE_URL:-}" && go test -tags=integration ./test/integration/... && test -n "${S2S_REALTIME_URL:-}" && go test -tags=contract ./test/contract/... && govulncheck ./...'
  record slo-load release-producer-v1/formal-slo-load sh -c 'test -n "${LIVE_EVIDENCE_DIR:-}" && ./deploy/scripts/run-load-evidence.sh formal'
  record privacy-no-audio-persistence release-producer-v1/privacy-scan sh -c 'go test -json -count=1 ./internal/safety ./internal/telemetry'
  record deployment-drain release-producer-v1/deployment-drain sh -c 'make verify-deploy && go test -json -count=1 ./internal/admin ./internal/lifecycle ./deploy/prestop ./deploy/validate'

	  if [ "$failed" -eq 0 ]; then
	    : "${CHAOS_DATABASE_URL:?formal database URL is required for migration evidence}"
	    "$root/deploy/scripts/run-release-migration-evidence.sh" "$bundle/artifacts/migration-rollback.json" || failed=1
	    if [ "$failed" -eq 0 ]; then
	      printf '{"schema_version":"release-artifact-v2","artifact_type":"vulnerability_report","status":"passed","suite":"govulncheck"}\n' >"$bundle/artifacts/vulnerability-report.json"
      dashboard_sha=$(shasum -a 256 deploy/observability/grafana-dashboard.json | awk '{print $1}')
      printf '{"schema_version":"release-artifact-v2","artifact_type":"dashboard_snapshot","status":"passed","source_sha256":"%s"}\n' "$dashboard_sha" >"$bundle/artifacts/dashboard-snapshot.json"
      : "${RELEASE_CANDIDATE_CANARY_RECEIPT:?verified candidate canary receipt is required}"
      cp "$RELEASE_CANDIDATE_CANARY_RECEIPT" "$bundle/artifacts/candidate-canary-receipt.json" || failed=1
      : "${RELEASE_PRODUCTION_MANIFEST:?production manifest is required}"
      manifest_sha=$(shasum -a 256 "$RELEASE_PRODUCTION_MANIFEST" | awk '{print $1}')
      printf '{"schema_version":"release-artifact-v2","artifact_type":"production_manifest","status":"passed","source_sha256":"%s"}\n' "$manifest_sha" >"$bundle/artifacts/production-manifest.json"
	      : "${RELEASE_CANDIDATE_DIR:?candidate evidence directory is required}"
	      for target in gateway migrate worker; do
	        "$root/deploy/scripts/verify-candidate-artifact-layout.sh" "$target" "$RELEASE_CANDIDATE_DIR/$target" || failed=1
	        cp "$RELEASE_CANDIDATE_DIR/$target/$target.sbom-summary.json" "$bundle/artifacts/$target-sbom.json" || failed=1
	        cp "$RELEASE_CANDIDATE_DIR/$target/$target.provenance-summary.json" "$bundle/artifacts/$target-provenance.json" || failed=1
	      done
    fi
  fi
fi


collect_args=
for gate_id in \
  simulator-voice durable-acceptance correlation presentation steer-cancel \
  epoch-fencing reconnect restart-durability verification-suites slo-load \
  privacy-no-audio-persistence deployment-drain
do
  collect_args="$collect_args --gate-evidence $gates/$gate_id.json"
done

add_artifact candidate_canary_receipt artifacts/candidate-canary-receipt.json
add_artifact dashboard_snapshot artifacts/dashboard-snapshot.json
if [ "$verify_existing" -eq 0 ]; then
formal_bundle=$(find "$LIVE_EVIDENCE_DIR" -mindepth 1 -maxdepth 1 -type d -name 'task24-formal.*' -print)
[ -n "$formal_bundle" ] && [ "$(printf '%s\n' "$formal_bundle" | wc -l | tr -d ' ')" -eq 1 ] || { echo 'exact formal evidence bundle is required' >&2; exit 1; }
: "${RELEASE_FORMAL_POSTGRES_CONTAINER_ID:?formal postgres identity is required}"
: "${RELEASE_FORMAL_TOXIPROXY_CONTAINER_ID:?formal toxiproxy identity is required}"
: "${RELEASE_FORMAL_GATEWAY_CONTAINER_ID:?formal gateway identity is required}"
: "${RELEASE_FORMAL_WORKER_CONTAINER_ID:?formal worker identity is required}"
: "${RELEASE_FORMAL_OIDC_CONTAINER_ID:?formal OIDC identity is required}"
: "${RELEASE_FORMAL_VOICE_CONTAINER_ID:?formal voice identity is required}"
: "${RELEASE_FORMAL_DEEPSEEK_CONTAINER_ID:?formal DeepSeek identity is required}"
: "${RELEASE_FORMAL_RUNTIME_CONFIG_HASH:?observed formal runtime config hash is required}"
: "${RELEASE_FORMAL_CONFIG_SHA256:?formal config document hash is required}"
: "${RELEASE_FORMAL_CONFIG_SOURCE:?formal config source is required}"
formal_manifest="$formal_bundle/manifest.json"
topology_source="$LIVE_EVIDENCE_DIR/topology/topology-identity.json"
[ -f "$formal_manifest" ] && [ -f "$topology_source" ] || { echo 'formal topology identity and manifest are required' >&2; exit 1; }
jq -e --arg runtime "$RELEASE_FORMAL_RUNTIME_CONFIG_HASH" --arg config "$RELEASE_FORMAL_CONFIG_SHA256" \
  --arg source "$RELEASE_FORMAL_CONFIG_SOURCE" \
  --arg gateway "$RELEASE_FORMAL_GATEWAY_CONTAINER_ID" --arg worker "$RELEASE_FORMAL_WORKER_CONTAINER_ID" \
  '.execution_environment == "formal-chaos" and .runtime_config_hash == $runtime and .formal_config_sha256 == $config and .gateway == {container_id:$gateway,runtime_config_hash:$runtime,formal_config_source:$source,formal_config_read_only:true} and .worker == {container_id:$worker,runtime_config_hash:$runtime,formal_config_source:$source,formal_config_read_only:true}' \
  "$topology_source" >/dev/null || { echo 'formal topology/config observation does not match inspected runtime' >&2; exit 1; }
formal_started=$(jq -er '[.reports[].started_at] | min' "$formal_manifest")
formal_finished=$(jq -er '[.reports[].finished_at] | max' "$formal_manifest")
topology_sha=$(shasum -a 256 "$topology_source" | awk '{print $1}')
manifest_sha=$(shasum -a 256 "$formal_manifest" | awk '{print $1}')
mkdir -m 700 "$bundle/artifacts/formal"
write_formal() {
  formal_name=$1 source_name=$2 source_sha=$3 selector=$4 started=$5 finished=$6 duration=$7 samples=$8 chaos=$9
  jq -cn --arg type "$formal_name" --arg source_name "$source_name" --arg source_sha "$source_sha" --arg selector "$selector" --arg started "$started" --arg finished "$finished" --arg commit "$commit" --arg release_config "$config_hash" --arg runtime_config "$RELEASE_FORMAL_RUNTIME_CONFIG_HASH" --arg formal_config "$RELEASE_FORMAL_CONFIG_SHA256" --arg region "$RELEASE_INPUT_deployment_region" --arg gateway "$RELEASE_INPUT_candidate_gateway_image" --arg migrate "$RELEASE_INPUT_candidate_migrate_image" --arg worker "$RELEASE_INPUT_candidate_worker_image" --arg topology "$topology_sha" --arg manifest "$manifest_sha" --arg production "$RELEASE_INPUT_manifest_sha256" --arg chaos "$chaos" --arg postgres "$RELEASE_FORMAL_POSTGRES_CONTAINER_ID" --arg toxiproxy "$RELEASE_FORMAL_TOXIPROXY_CONTAINER_ID" --arg gateway_id "$RELEASE_FORMAL_GATEWAY_CONTAINER_ID" --arg worker_id "$RELEASE_FORMAL_WORKER_CONTAINER_ID" --arg oidc "$RELEASE_FORMAL_OIDC_CONTAINER_ID" --arg voice "$RELEASE_FORMAL_VOICE_CONTAINER_ID" --arg deepseek "$RELEASE_FORMAL_DEEPSEEK_CONTAINER_ID" --argjson duration "$duration" --argjson samples "$samples" '{schema_version:"release-artifact-v2",artifact_type:$type,status:"passed",source_name:$source_name,source_sha256:$source_sha,selector:$selector,started_at:$started,finished_at:$finished,commit:$commit,release_config_hash:$release_config,runtime_config_hash:$runtime_config,formal_config_sha256:$formal_config,execution_environment:"formal-chaos",region:$region,candidate_gateway_image:$gateway,candidate_migrate_image:$migrate,candidate_worker_image:$worker,topology_sha256:$topology,formal_manifest_sha256:$manifest,production_manifest_sha256:$production,duration_seconds:$duration,sample_count:$samples,chaos_case:$chaos,containers:{postgres:$postgres,toxiproxy:$toxiproxy,gateway:$gateway_id,worker:$worker_id,oidc:$oidc,voice:$voice,deepseek:$deepseek}}' >"$bundle/artifacts/formal/$formal_name.json"
  add_artifact "$formal_name" "artifacts/formal/$formal_name.json"
}
write_formal formal_topology_identity topology-identity.json "$topology_sha" 'inspected Compose images, container runtime config, and mounted formal config' "$formal_started" "$formal_finished" 0 0 ''
write_formal formal_manifest manifest.json "$manifest_sha" 'formal evidence manifest' "$formal_started" "$formal_finished" 0 0 ''
while IFS='|' read -r formal_name source_name selector started finished source_sha duration samples chaos; do
  write_formal "$formal_name" "$source_name" "$source_sha" "$selector" "$started" "$finished" "$duration" "$samples" "$chaos"
done <<EOF
$(jq -r '.reports[] | [.name, .name, .selector, .started_at, .finished_at, .source_sha256, .duration_seconds, .sample_count, .chaos_case] | @tsv' "$formal_manifest" | while IFS="$(printf '\t')" read -r source_name ignored selector started finished source_sha duration samples chaos; do
  case "$source_name" in
    unit-race.json) formal_name=formal_unit_race ;;
    capacity-profile-slo-10m.json) formal_name=formal_capacity_slo_10m ;;
    barge-in-20.json) formal_name=formal_barge_20 ;;
    chaos-postgresql-disconnect.json) formal_name=formal_chaos_postgresql_disconnect ;;
    chaos-deepseek-429.json) formal_name=formal_chaos_deepseek_429 ;;
    chaos-slow-client-backpressure.json) formal_name=formal_chaos_slow_client_backpressure ;;
    chaos-stalled-voice-provider.json) formal_name=formal_chaos_stalled_voice_provider ;;
    chaos-killed-worker-lease-recovery.json) formal_name=formal_chaos_killed_worker_lease_recovery ;;
    chaos-lost-notification-poll-repair.json) formal_name=formal_chaos_lost_notification_poll_repair ;;
    soak-2h.json) formal_name=formal_soak_2h ;;
    *) exit 1 ;;
  esac
  printf '%s|%s|%s|%s|%s|%s|%s|%s|%s\n' "$formal_name" "$source_name" "$selector" "$started" "$finished" "$source_sha" "$duration" "$samples" "$chaos"
done)
EOF
else
  for formal_name in \
    formal_topology_identity formal_manifest formal_unit_race formal_capacity_slo_10m formal_barge_20 \
    formal_chaos_postgresql_disconnect formal_chaos_deepseek_429 formal_chaos_slow_client_backpressure \
    formal_chaos_stalled_voice_provider formal_chaos_killed_worker_lease_recovery \
    formal_chaos_lost_notification_poll_repair formal_soak_2h
  do
    add_artifact "$formal_name" "artifacts/formal/$formal_name.json"
  done
fi
add_artifact gateway_provenance artifacts/gateway-provenance.json
add_artifact gateway_sbom artifacts/gateway-sbom.json
add_artifact migrate_provenance artifacts/migrate-provenance.json
add_artifact migrate_sbom artifacts/migrate-sbom.json
add_artifact migration_rollback artifacts/migration-rollback.json
add_artifact production_manifest artifacts/production-manifest.json
add_artifact vulnerability_report artifacts/vulnerability-report.json
add_artifact worker_provenance artifacts/worker-provenance.json
add_artifact worker_sbom artifacts/worker-sbom.json

LC_ALL=C sort "$artifact_specs" -o "$artifact_specs"
duplicate_artifacts=$(cut -d= -f1 "$artifact_specs" | uniq -d)
[ -z "$duplicate_artifacts" ] || { echo "duplicate release artifact name: $duplicate_artifacts" >&2; exit 1; }
artifact_args=
while IFS= read -r artifact_spec; do
  [ -n "$artifact_spec" ] || { echo "empty release artifact specification" >&2; exit 1; }
  artifact_args="$artifact_args --artifact $artifact_spec"
done <"$artifact_specs"

release_input_args=
for input_name in \
  candidate_canary_halt_policy_sha256 candidate_canary_public_key_sha256 \
  candidate_gateway_image candidate_migrate_image candidate_worker_image deployment_region \
  egress_proxy_cidr manifest_sha256 oidc_issuer optional_provider_result production_host \
  production_tls_secret run_provider_tests trusted_proxy_cidr voice_url
do
  input_value=$(printenv "RELEASE_INPUT_$input_name" 2>/dev/null || true)
  [ -n "$input_value" ] || { echo "missing reviewed release input: $input_name" >&2; exit 1; }
  release_input_args="$release_input_args --release-input $input_name=$input_value"
done

# Paths are generated locally without whitespace. shellcheck disable=SC2086
if [ -n "$attestation" ]; then
  preserve=
  if [ "$verify_existing" -eq 1 ]; then preserve=--preserve-generated-at; fi
  "$release_gate" collect --commit "$commit" --config-hash "$config_hash" \
    --evidence-out "$evidence_out" $collect_args $artifact_args $release_input_args $preserve --attestation "$attestation" || exit 1
else
  "$release_gate" collect --commit "$commit" --config-hash "$config_hash" \
    --evidence-out "$evidence_out" $collect_args $artifact_args $release_input_args || exit 1
fi

if [ "$failed" -ne 0 ]; then
  echo "one or more release producers failed; evidence was retained for diagnosis" >&2
  exit 1
fi
if [ -z "$attestation" ] || [ -z "$attestation_key" ]; then
	# This exact UTC timestamp and immutable archive are the only inputs the
	# independent reviewer signs. Phase two never reopens this mutable bundle.
	signed_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
	unsigned_archive_out=${RELEASE_UNSIGNED_ARCHIVE_OUT:-$root/artifacts/release-archives/$commit/${config_hash#v1:}.unsigned.tar}
	mkdir -p "$(dirname -- "$unsigned_archive_out")"
	# Paths and reviewed inputs are generated locally without whitespace. shellcheck disable=SC2086
	statement_hash=$($release_gate snapshot-unsigned --evidence "$evidence_out" --archive-out "$unsigned_archive_out" \
	  --commit "$commit" --config-hash "$config_hash" --signer release-operator --signed-at "$signed_at" \
	  $release_input_args) || exit 1
  echo "signed human privacy attestation required" >&2
  echo "statement_sha256=$statement_hash" >&2
	echo "signed_at=$signed_at" >&2
	echo "unsigned_archive=$unsigned_archive_out" >&2
  echo "see docs/operations.md for the offline Ed25519 signing command" >&2
  exit 2
fi
"$release_gate" verify --evidence "$evidence_out" --commit "$commit" \
  --config-hash "$config_hash" $release_input_args --attestation-key "$attestation_key"
