#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
scratch=$(mktemp -d "${TMPDIR:-/tmp}/gpt-live-release-boundaries.XXXXXX")
trap 'rm -rf "$scratch"' EXIT HUP INT TERM

exact=$scratch/exact.bin
dd if=/dev/zero of="$exact" bs=1048576 count=1 2>/dev/null
"$root/deploy/scripts/inspect-formal-artifact.sh" "$exact" >/dev/null
printf x >>"$exact"
if "$root/deploy/scripts/inspect-formal-artifact.sh" "$exact" >/dev/null 2>&1; then
  echo "formal artifact limit-plus-one was accepted" >&2
  exit 1
fi

env_file=$scratch/github-env
marker=$scratch/injected
: >"$env_file"
hostile="https://\$(touch\${IFS}$marker)"
GITHUB_ENV="$env_file" \
INPUT_CONFIG_HASH=v1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
INPUT_DEPLOYMENT_REGION=us-east-1 INPUT_EGRESS_PROXY_CIDR=10.0.0.0/8 \
INPUT_OIDC_ISSUER="$hostile" INPUT_PRODUCTION_HOST=live.example \
INPUT_PRODUCTION_TLS_SECRET=live-tls INPUT_RUN_PROVIDER_TESTS=false \
INPUT_TRUSTED_PROXY_CIDR=10.1.0.0/16 INPUT_VOICE_URL=wss://voice.example \
  "$root/deploy/scripts/bind-release-inputs.sh"
[ ! -e "$marker" ] || { echo "release input was evaluated as shell source" >&2; exit 1; }
grep -F "RELEASE_INPUT_oidc_issuer=$hostile" "$env_file" >/dev/null

# The release one-shot wrapper must forward every validated runtime binding to
# the executable, especially the exact candidate gateway digest.
canary_fixture=$scratch/canary-fixture
canary_marker=$scratch/canary-invoked
cat >"$canary_fixture" <<'EOF'
#!/bin/sh
set -eu
for name in LIVE_CANARY_GATEWAY_URL LIVE_CANARY_ORIGIN LIVE_CANARY_RETENTION \
  LIVE_CANARY_BEARER_TOKEN_FILE LIVE_DEPLOYMENT_REGION LIVE_EXPECTED_CONFIG_HASH \
  LIVE_EXPECTED_GATEWAY_IMAGE
do
  test -n "$(printenv "$name")"
done
test "$1" = --once
test "$2" = --evidence-out
printf invoked >"$CANARY_FIXTURE_MARKER"
printf '{}\n' >"$3"
EOF
chmod 0755 "$canary_fixture"
canary_output=$scratch/canary.json
CANARY_FIXTURE_MARKER=$canary_marker \
LIVE_CANARY_GATEWAY_URL=wss://live.example/v1/realtime \
LIVE_CANARY_ORIGIN=https://live.example LIVE_CANARY_RETENTION=none \
LIVE_CANARY_BEARER_TOKEN_FILE=$scratch/token LIVE_DEPLOYMENT_REGION=us-east-1 \
LIVE_EXPECTED_CONFIG_HASH=v1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
LIVE_EXPECTED_GATEWAY_IMAGE=ghcr.io/chuanmingliu/openai-gpt-live/gateway@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
  "$root/deploy/scripts/run-release-canary.sh" "$canary_fixture" "$canary_output"
[ "$(cat "$canary_marker")" = invoked ] && [ -s "$canary_output" ]
rm -f "$canary_marker" "$canary_output"
if CANARY_FIXTURE_MARKER=$canary_marker \
  LIVE_CANARY_GATEWAY_URL=wss://live.example/v1/realtime \
  LIVE_CANARY_ORIGIN=https://live.example LIVE_CANARY_RETENTION=none \
  LIVE_CANARY_BEARER_TOKEN_FILE=$scratch/token LIVE_DEPLOYMENT_REGION=us-east-1 \
  LIVE_EXPECTED_CONFIG_HASH=v1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  "$root/deploy/scripts/run-release-canary.sh" "$canary_fixture" "$canary_output" 2>/dev/null
then
  echo "one-shot canary ran without the expected gateway image" >&2
  exit 1
fi
[ ! -e "$canary_marker" ]

# docker wait reports the container exit in stdout while the CLI itself exits
# zero. A nonzero down migration must therefore prevent passed evidence.
fake_docker=$scratch/docker
docker_state=$scratch/docker-state
cat >"$fake_docker" <<'EOF'
#!/bin/sh
set -eu
postgres=2222222222222222222222222222222222222222222222222222222222222222
up1=1111111111111111111111111111111111111111111111111111111111111111
down=3333333333333333333333333333333333333333333333333333333333333333
up2=4444444444444444444444444444444444444444444444444444444444444444
if [ "$1" = compose ]; then
  shift
  while [ "$1" != ps ] && [ "$1" != run ]; do shift; done
  if [ "$1" = ps ]; then printf '%s\n' "$postgres"; exit 0; fi
  direction=$6
  case "$direction" in
    down) printf '%s\n' "$down" ;;
    up)
      if [ -e "$FAKE_DOCKER_STATE" ]; then printf '%s\n' "$up2"; else : >"$FAKE_DOCKER_STATE"; printf '%s\n' "$up1"; fi ;;
    *) exit 91 ;;
  esac
  exit 0
fi
case "$1" in
  inspect)
    id=$4
    if [ "$id" = "$postgres" ]; then printf '%s\n' "$RELEASE_POSTGRES_IMAGE"; else printf '%s\n' "$RELEASE_INPUT_candidate_migrate_image"; fi ;;
  exec) printf '160010\n' ;;
  wait)
    if [ "$2" = "$down" ]; then printf '%s\n' "${FAKE_DOWN_EXIT:-17}"; else printf '0\n'; fi ;;
  rm) : ;;
  *) exit 92 ;;
esac
EOF
chmod 0755 "$fake_docker"
migration_output=$scratch/migration.json
if PATH="$scratch:$PATH" FAKE_DOCKER_STATE=$docker_state FAKE_DOWN_EXIT=17 \
  RELEASE_POSTGRES_IMAGE=postgres:16-alpine@sha256:57c72fd2a128e416c7fcc499958864df5301e940bca0a56f58fddf30ffc07777 \
  RELEASE_INPUT_candidate_migrate_image=ghcr.io/chuanmingliu/openai-gpt-live/migrate@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee \
  CHAOS_COMPOSE_PROJECT=fixture CHAOS_COMPOSE_BASE=base.yml CHAOS_COMPOSE_OVERRIDE=chaos.yml \
  "$root/deploy/scripts/run-release-migration-evidence.sh" "$migration_output" 2>/dev/null
then
  echo "nonzero down migration produced passed evidence" >&2
  exit 1
fi
[ ! -e "$migration_output" ]
rm -f "$docker_state"
PATH="$scratch:$PATH" FAKE_DOCKER_STATE=$docker_state FAKE_DOWN_EXIT=0 \
RELEASE_POSTGRES_IMAGE=postgres:16-alpine@sha256:57c72fd2a128e416c7fcc499958864df5301e940bca0a56f58fddf30ffc07777 \
RELEASE_INPUT_candidate_migrate_image=ghcr.io/chuanmingliu/openai-gpt-live/migrate@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee \
CHAOS_COMPOSE_PROJECT=fixture CHAOS_COMPOSE_BASE=base.yml CHAOS_COMPOSE_OVERRIDE=chaos.yml \
  "$root/deploy/scripts/run-release-migration-evidence.sh" "$migration_output"
jq -e '.status == "passed" and [.executions[].direction] == ["up","down","up"] and [.executions[].exit_code] == [0,0,0]' "$migration_output" >/dev/null

# The topology capture observes both running candidates, their exact mounted
# config document, and the runtime config hash injected into each process.
formal_bin=$scratch/formal-bin
mkdir "$formal_bin"
cat >"$formal_bin/docker" <<'EOF'
#!/bin/sh
set -eu
if [ "$1" = inspect ]; then
  id=$2
  hash=$FORMAL_RUNTIME_HASH
  if [ "$id" = "$FORMAL_WORKER_ID" ]; then hash=${FAKE_WORKER_HASH:-$hash}; fi
  jq -cn --arg id "$id" --arg hash "$hash" --arg source "$FORMAL_CONFIG_SOURCE" \
    --arg database "$FORMAL_DATABASE_SECRET" --arg provider "$FORMAL_PROVIDER_SECRET" \
    --arg bearer "$FORMAL_BEARER_SECRET" --arg hmac "$FORMAL_HMAC_SECRET" \
    --arg encryption "$FORMAL_ENCRYPTION_SECRET" \
    '[{Id:$id,Config:{Image:"candidate@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",Env:[("LIVE_EXPECTED_CONFIG_HASH="+$hash),("DATABASE_URL="+$database),("DEEPSEEK_API_KEY="+$provider),("LIVE_STATIC_BEARER_TOKEN="+$bearer),("LIVE_PRIVACY_HMAC_KEY="+$hmac),("LIVE_TOOL_ENCRYPTION_KEY="+$encryption)]},Mounts:[{Source:$source,Destination:"/etc/live/config.yaml",RW:false}]}]'
  exit "${FAKE_INSPECT_EXIT:-0}"
fi
if [ "$1" = compose ]; then printf '[]\n'; exit 0; fi
exit 93
EOF
chmod 0755 "$formal_bin/docker"
formal_identity=$scratch/formal-identity
formal_gateway=3333333333333333333333333333333333333333333333333333333333333333
formal_worker=4444444444444444444444444444444444444444444444444444444444444444
formal_runtime_hash=v1:2ea1a23fa0ef112326013843a8df0efb476ca34fd209ce75507a8469a6ecfaac
formal_database_secret=postgres://formal:DB_PASSWORD_NEVER_PERSIST@database/formal
formal_provider_secret=sk-live-PROVIDER-NEVER-PERSIST
formal_bearer_secret=bearer-FORMAL-NEVER-PERSIST
formal_hmac_secret=hmac-FORMAL-NEVER-PERSIST
formal_encryption_secret=encryption-FORMAL-NEVER-PERSIST
formal_capture_output=$scratch/formal-capture-output
PATH="$formal_bin:$PATH" FORMAL_RUNTIME_HASH=$formal_runtime_hash \
FORMAL_GATEWAY_ID=$formal_gateway FORMAL_WORKER_ID=$formal_worker \
FORMAL_CONFIG_SOURCE=$root/deploy/config/chaos.yaml \
FORMAL_DATABASE_SECRET=$formal_database_secret FORMAL_PROVIDER_SECRET=$formal_provider_secret \
FORMAL_BEARER_SECRET=$formal_bearer_secret FORMAL_HMAC_SECRET=$formal_hmac_secret \
FORMAL_ENCRYPTION_SECRET=$formal_encryption_secret \
CHAOS_COMPOSE_PROJECT=fixture CHAOS_COMPOSE_BASE=base.yml CHAOS_COMPOSE_OVERRIDE=chaos.yml \
  "$root/deploy/scripts/capture-formal-topology.sh" "$formal_identity" \
  "$root/deploy/config/chaos.yaml" "$formal_runtime_hash" "$formal_gateway" "$formal_worker" \
  >"$formal_capture_output" 2>&1
for secret in "$formal_database_secret" "$formal_provider_secret" "$formal_bearer_secret" "$formal_hmac_secret" "$formal_encryption_secret"; do
  if grep -FR -- "$secret" "$formal_identity" "$formal_capture_output" >/dev/null; then
    echo "formal topology capture persisted or emitted an inspect secret" >&2
    exit 1
  fi
done
[ "$(find "$formal_identity" -type f -maxdepth 1 -print | wc -l | tr -d ' ')" -eq 1 ] || {
  echo "formal topology capture retained raw inspection material" >&2
  exit 1
}
[ ! -e "$formal_identity/gateway-inspect.json" ] && [ ! -e "$formal_identity/worker-inspect.json" ]
jq -e --arg runtime "$formal_runtime_hash" --arg document 8bf602e10803869ccd1d85f52be0de81eb052c99b946f5485577450967d99326 \
  --arg source "$root/deploy/config/chaos.yaml" \
  '.runtime_config_hash == $runtime and .formal_config_sha256 == $document and .gateway == {container_id:"3333333333333333333333333333333333333333333333333333333333333333",runtime_config_hash:$runtime,formal_config_source:$source,formal_config_read_only:true} and .worker == {container_id:"4444444444444444444444444444444444444444444444444444444444444444",runtime_config_hash:$runtime,formal_config_source:$source,formal_config_read_only:true}' \
  "$formal_identity/topology-identity.json" >/dev/null
rm -rf "$formal_identity"

if PATH="$formal_bin:$PATH" FORMAL_RUNTIME_HASH=$formal_runtime_hash FAKE_INSPECT_EXIT=42 \
  FORMAL_GATEWAY_ID=$formal_gateway FORMAL_WORKER_ID=$formal_worker \
  FORMAL_CONFIG_SOURCE=$root/deploy/config/chaos.yaml \
  FORMAL_DATABASE_SECRET=$formal_database_secret FORMAL_PROVIDER_SECRET=$formal_provider_secret \
  FORMAL_BEARER_SECRET=$formal_bearer_secret FORMAL_HMAC_SECRET=$formal_hmac_secret \
  FORMAL_ENCRYPTION_SECRET=$formal_encryption_secret \
  CHAOS_COMPOSE_PROJECT=fixture CHAOS_COMPOSE_BASE=base.yml CHAOS_COMPOSE_OVERRIDE=chaos.yml \
  "$root/deploy/scripts/capture-formal-topology.sh" "$formal_identity" \
  "$root/deploy/config/chaos.yaml" "$formal_runtime_hash" "$formal_gateway" "$formal_worker" \
  >"$formal_capture_output" 2>&1
then
  evidence_files=$(find "$formal_identity" -type f -maxdepth 1 -print | wc -l | tr -d ' ')
  topology_present=no
  [ ! -e "$formal_identity/topology-identity.json" ] || topology_present=yes
  echo "docker inspect exit 42 was masked: capture_exit=0 evidence_files=$evidence_files topology_present=$topology_present" >&2
  exit 1
fi
[ ! -e "$formal_identity/topology-identity.json" ] || {
  echo "failed docker inspect retained a topology artifact" >&2
  exit 1
}

if PATH="$formal_bin:$PATH" FORMAL_RUNTIME_HASH=$formal_runtime_hash \
  FAKE_WORKER_HASH=v1:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff \
  FORMAL_GATEWAY_ID=$formal_gateway FORMAL_WORKER_ID=$formal_worker \
  FORMAL_CONFIG_SOURCE=$root/deploy/config/chaos.yaml \
  FORMAL_DATABASE_SECRET=$formal_database_secret FORMAL_PROVIDER_SECRET=$formal_provider_secret \
  FORMAL_BEARER_SECRET=$formal_bearer_secret FORMAL_HMAC_SECRET=$formal_hmac_secret \
  FORMAL_ENCRYPTION_SECRET=$formal_encryption_secret \
  CHAOS_COMPOSE_PROJECT=fixture CHAOS_COMPOSE_BASE=base.yml CHAOS_COMPOSE_OVERRIDE=chaos.yml \
  "$root/deploy/scripts/capture-formal-topology.sh" "$formal_identity" \
  "$root/deploy/config/chaos.yaml" "$formal_runtime_hash" "$formal_gateway" "$formal_worker" 2>/dev/null
then
  echo "formal topology accepted a worker running the wrong config hash" >&2
  exit 1
fi
[ ! -e "$formal_identity/topology-identity.json" ]

fake_bin=$scratch/fake-bin
formal_root=$scratch/formal
mkdir -p "$fake_bin" "$formal_root"
printf '%s\n' '#!/bin/sh' 'printf '\''{"Action":"pass","Package":"formal-fixture"}\\n'\''' >"$fake_bin/go"
chmod 0755 "$fake_bin/go"
PATH="$fake_bin:$PATH" LIVE_EVIDENCE_DIR="$formal_root" \
CHAOS_TOXIPROXY_URL=http://toxiproxy CHAOS_POSTGRES_PROXY=postgres:5432 \
CHAOS_VOICE_PROXY=voice:8000 CHAOS_DEEPSEEK_PROXY=deepseek:8080 \
CHAOS_VOICE_CONTROL_URL=http://voice CHAOS_DEEPSEEK_CONTROL_URL=http://deepseek \
CHAOS_GATEWAY_URL=http://gateway CHAOS_GATEWAY_HEALTH_URL=http://gateway-health \
CHAOS_WORKER_HEALTH_URL=http://worker-health CHAOS_BEARER_TOKEN=synthetic-fixture \
CHAOS_OIDC_CONTROL_URL=http://oidc CHAOS_COMPOSE_PROJECT=formal-fixture \
CHAOS_COMPOSE_BASE=deploy/docker-compose.yml CHAOS_COMPOSE_OVERRIDE=deploy/compose.chaos.yml \
CHAOS_DATABASE_URL=postgres://fixture \
  "$root/deploy/scripts/run-load-evidence.sh" formal >/dev/null
formal_bundle=$(find "$formal_root" -mindepth 1 -maxdepth 1 -type d)
[ -n "$formal_bundle" ] && [ "$(printf '%s\n' "$formal_bundle" | wc -l | tr -d ' ')" -eq 1 ]
expected_formal='barge-in-20.json
capacity-profile-slo-10m.json
chaos-deepseek-429.json
chaos-killed-worker-lease-recovery.json
chaos-lost-notification-poll-repair.json
chaos-postgresql-disconnect.json
chaos-slow-client-backpressure.json
chaos-stalled-voice-provider.json
manifest.json
soak-2h.json
unit-race.json'
actual_formal=$(find "$formal_bundle" -type f -maxdepth 1 -exec basename {} \; | LC_ALL=C sort)
[ "$actual_formal" = "$expected_formal" ] || { echo "formal producer did not retain the exact bounded source inventory" >&2; exit 1; }
jq -e '.schema_version == "formal-source-manifest-v1" and .status == "passed" and (.reports | length) == 10 and ([.reports[].name] | unique | length) == 10' "$formal_bundle/manifest.json" >/dev/null

# The release wrapper must translate the fixed Task 24 source inventory into
# the fixed, authenticated formal inventory. Generic numbered summaries or a
# copied raw tree would make the formal gate optional and unauditable again.
wrapper=$root/deploy/scripts/run-release-gate.sh
rg -F 'formal_topology_identity' "$wrapper" >/dev/null
rg -F 'formal_soak_2h' "$wrapper" >/dev/null
rg -F 'if [ "$verify_existing" -eq 0 ]; then' "$wrapper" >/dev/null
! rg -F 'formal_%03d' "$wrapper"
! rg -F 'cp -R "$LIVE_EVIDENCE_DIR"' "$wrapper"
rg -F 'run-release-migration-evidence.sh' "$wrapper" >/dev/null
! rg -F 'go run ./cmd/live-migrate' "$wrapper"

# Promotion receives upload-artifact's documented bare SHA-256 output and
# downloads a prior run, so the workflow contract must accept that shape and
# retain the required Actions read permission.
candidate_layout=$scratch/candidate-layout
mkdir "$candidate_layout"
for suffix in digest index.json supply-validation.json sbom.json provenance.json sbom-summary.json provenance-summary.json oci.tar; do
  : >"$candidate_layout/gateway.$suffix"
done
"$root/deploy/scripts/verify-candidate-artifact-layout.sh" gateway "$candidate_layout"
: >"$candidate_layout/unexpected"
if "$root/deploy/scripts/verify-candidate-artifact-layout.sh" gateway "$candidate_layout" 2>/dev/null; then
  echo "candidate layout accepted an undeclared downloaded file" >&2
  exit 1
fi

workflow=$root/.github/workflows/release-gate.yml
ruby - "$workflow" <<'RUBY'
require "yaml"
steps = YAML.load_file(ARGV.fetch(0)).fetch("jobs").fetch("collect-unsigned-evidence").fetch("steps")
upload = steps.index { |step| step["id"] == "upload-unsigned" }
cleanup = steps.index { |step| step["name"] == "Clean release evidence workspace and token material" }
abort "release cleanup must follow the required unsigned archive upload" unless upload && cleanup && cleanup > upload
step = steps.fetch(cleanup)
abort "release cleanup must run unconditionally" unless step["if"] == "always()"
run = step.fetch("run")
%w[artifacts/task24-formal candidate-canary-ed25519.pub.base64 unsigned-release-evidence.tar].each do |material|
  abort "release cleanup omits #{material}" unless run.include?(material)
end
RUBY
rg -F 'environment: release-candidate-canary' "$workflow" >/dev/null
rg -F 'run-candidate-canary-edge.sh' "$workflow" >/dev/null
rg -F 'candidate-canary-receipt-${{ github.sha }}' "$workflow" >/dev/null
rg -F 'snapshot-unsigned' "$wrapper" >/dev/null
rg -F 'finalize-archive' "$workflow" >/dev/null
[ "$(rg -c 'verify-archive' "$workflow")" -ge 2 ]
! rg -F 'path: artifacts/release/${{ github.sha }}/${{ inputs.config_hash }}/' "$workflow"
! rg -F '${{ runner.temp }}/${{ matrix.target }}.oci.tar' "$workflow"
rg -F 'path: candidate-artifacts/${{ matrix.target }}/' "$workflow" >/dev/null
rg -F 'verify-candidate-artifact-layout.sh' "$workflow" >/dev/null
rg -F 'UNSIGNED_ARCHIVE_ARTIFACT_DIGEST' "$workflow" >/dev/null
rg -F 'actions: read' "$workflow" >/dev/null
rg -F '^[0-9a-f]{64}$' "$workflow" >/dev/null
awk '
  /promote-reviewed-digests:/ { promotion=1 }
  promotion && /actions: read/ { actions_read=1 }
  /SIGNED_BUNDLE_ARTIFACT_DIGEST:/ { digest_env=1 }
  digest_env && index($0, "[[ \"$SIGNED_BUNDLE_ARTIFACT_DIGEST\" =~ ^[0-9a-f]{64}$ ]]") { bare_digest=1 }
  END { exit !(actions_read && bare_digest) }
' "$workflow"

# Supply validation consumes the raw in-toto blobs from the OCI descriptor
# graph, not Buildx's predicate-only convenience views, and the exact result is
# identity-attested and reverified before an immutable index is promoted.
python3 "$root/deploy/scripts/test-validate-supply-evidence.py"
rg -F 'type=oci,dest=${{ github.workspace }}/candidate-artifacts/${{ matrix.target }}/${{ matrix.target }}.oci.tar' "$workflow" >/dev/null
[ "$(rg -c 'validate-supply-evidence.py' "$workflow")" -eq 2 ]
rg -F 'cosign verify-attestation --type "$predicate_type"' "$workflow" >/dev/null
! rg -F -- '--format '\''{{json .SBOM}}'\''' "$workflow"
! rg -F -- '--format '\''{{json .Provenance}}'\''' "$workflow"

echo "release gate boundary tests passed"
