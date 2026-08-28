#!/usr/bin/env bash
set -euo pipefail

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
scratch=$(mktemp -d "${TMPDIR:-/tmp}/gpt-live-demo-wizard-test.XXXXXX")
trap 'rm -rf "$scratch"' EXIT HUP INT TERM

fail() {
  printf '%s\n' "$1" >&2
  exit 1
}

[[ -x "$root/deploy/scripts/demo-wizard.sh" ]] || fail "demo wizard is missing or not executable"
grep -F 'demo-wizard:' "$root/Makefile" >/dev/null || fail "Makefile demo-wizard target is missing"

mkdir -p "$scratch/bin" "$scratch/state"
speech_config="$scratch/speech.yaml"
printf '%s\n' 'transport: websocket' >"$speech_config"
operator_speech_root="$scratch/operator-speech"
mkdir -p "$operator_speech_root/configs" "$operator_speech_root/bin"
operator_speech_profile="$operator_speech_root/configs/tencent-deepseek-minimax.json"
operator_speech_env="$operator_speech_root/.env.local"
operator_speech_executable="$operator_speech_root/bin/speech-to-speech"
printf '%s\n' '{"mode":"realtime","ws_port":8765}' >"$operator_speech_profile"
printf '%s\n' 'DEEPSEEK_API_KEY=test-only' >"$operator_speech_env"
printf '%s\n' '#!/bin/sh' 'exit 0' >"$operator_speech_executable"
chmod 600 "$operator_speech_profile" "$operator_speech_env"
chmod 700 "$operator_speech_executable"

cat >"$scratch/bin/go" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$DEMO_TEST_CALLS"
if [[ "$*" == 'run ./cmd/live-gateway config-hash --config deploy/config/development-durable.yaml' ]]; then
  printf '%s\n' 'v1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
  exit 0
fi
if [[ "$*" == 'run ./cmd/live-gateway config-hash --config deploy/config/development-control-panel.yaml' ]]; then
  printf '%s\n' 'v1:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
  exit 0
fi
if [[ "${1:-}" == 'build' && "${2:-}" == '-o' ]]; then
  output=$3
  cat >"$output" <<'SCRIPT'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "live-client $*" >>"${0}.calls"
if [[ -n "${DEMO_TEST_LIVE_CLIENT_EXIT:-}" ]]; then exit 98; fi
[[ -n "${LIVE_DEMO_BEARER:-}" ]] || exit 91
[[ "$*" == *'-bearer-env LIVE_DEMO_BEARER'* ]] || exit 99
if [[ "$*" != *'-operator'* ]]; then
  for forbidden in POSTGRES_PASSWORD S2S_API_KEY DEEPSEEK_API_KEY LIVE_STATIC_BEARER_TOKEN LIVE_ADMIN_BEARER_TOKEN LIVE_PRIVACY_HMAC_KEY LIVE_RETENTION_HMAC_KEY LIVE_RECONNECT_HMAC_KEY LIVE_TOOL_ENCRYPTION_KEY; do
    [[ -z "${!forbidden:-}" ]] || exit 94
  done
fi
[[ "$*" == *'-model gpt-live-simulator'* || "$*" == *'-model client-model'* ]] || exit 92
if [[ "$*" == *'-operator'* ]]; then
  for required in POSTGRES_PASSWORD S2S_API_KEY LIVE_STATIC_BEARER_TOKEN LIVE_ADMIN_BEARER_TOKEN LIVE_PRIVACY_HMAC_KEY LIVE_RETENTION_HMAC_KEY LIVE_RECONNECT_HMAC_KEY LIVE_TOOL_ENCRYPTION_KEY LIVE_EXPECTED_CONFIG_HASH; do
    [[ -n "${!required:-}" ]] || exit 95
  done
  printf '%s\n' 'operator diagnostic sentinel' >&2
  if [[ "$*" == *'-vertical-profiles'* ]]; then
    [[ "$*" != *'postgres://'* ]] || exit 96
    [[ -n "${LIVE_PROFILE_DATABASE_URL:-}" ]] || exit 97
    printf '%s\n' 'profile database sentinel' >&2
  fi
fi
if [[ "$0" != *'state-attached/live-client' ]]; then
  /bin/sleep 1
  exit 0
fi
trap 'exit 0' TERM INT
while :; do sleep 1; done
SCRIPT
  chmod +x "$output"
  exit 0
fi
if [[ "$1 $2" == 'run ./demo/live-client' ]]; then
  [[ "${LIVE_DEMO_BEARER:-}" == 'local-bearer-sentinel' ]] || exit 91
  [[ "$*" == *'-model gpt-live-simulator'* || "$*" == *'-model client-model'* ]] || exit 92
  trap 'exit 0' TERM INT
  while :; do sleep 1; done
fi
exit 93
EOF

cat >"$scratch/bin/make" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$DEMO_TEST_CALLS"
case "$1" in
  durable-demo-migrate)
    [[ "${POSTGRES_PASSWORD:-}" == 'postgres-password-sentinel' ]] || exit 81
    [[ "${S2S_API_KEY:-}" == 'speech-key-sentinel' ]] || exit 82
    [[ "${DEEPSEEK_API_KEY:-}" == 'deepseek-key-sentinel' ]] || exit 83
    [[ "${LIVE_STATIC_BEARER_TOKEN:-}" == 'local-bearer-sentinel' ]] || exit 84
    [[ -n "${LIVE_EXPECTED_CONFIG_HASH:-}" ]] || exit 85
    ;;
  durable-demo-up|durable-demo-down) ;;
  *) exit 86 ;;
esac
EOF

cat >"$scratch/bin/curl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$DEMO_TEST_CALLS"
case "$*" in
  *3000/health/ready*)
    [[ -z "${DEMO_TEST_STALE_READY:-}" ]] || exit 0
    if [[ -n "${DEMO_TEST_READY_AFTER:-}" ]]; then
      printf '%s\n' attempt >>"${DEMO_TEST_READY_COUNTER:?}"
      attempts=$(wc -l <"$DEMO_TEST_READY_COUNTER" | tr -d ' ')
      [[ "$attempts" -ge "$DEMO_TEST_READY_AFTER" ]] || exit 22
    fi
    for _ in {1..20}; do
      find "${DEMO_WIZARD_STATE_DIR:?}" -name 'live-client.calls' -type f -exec grep -E '^live-client ' {} \; | grep -q . && exit "${DEMO_TEST_CURL_EXIT:-0}"
      sleep 0.01
    done
    exit 22
    ;;
  *19090/health/ready*|*19091/health/ready*) exit "${DEMO_TEST_CURL_EXIT:-0}" ;;
  *) exit 87 ;;
esac
EOF

cat >"$scratch/bin/openssl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
case "$*" in
  'rand -hex 32') printf '%064d\n' 0 ;;
  'rand -base64 32') printf '%s\n' 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' ;;
  *) exit 88 ;;
esac
EOF

cat >"$scratch/bin/docker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
[[ "$*" == 'compose version' ]]
EOF

cat >"$scratch/bin/ps" <<'EOF'
#!/usr/bin/env bash
if [[ -n "${DEMO_TEST_PRESERVE_COMMAND:-}" ]]; then
  printf '%s\n' "$DEMO_TEST_PRESERVE_COMMAND"
  exit 0
fi
exec /bin/ps "$@"
EOF

cat >"$scratch/bin/sleep" <<'EOF'
#!/usr/bin/env bash
if [[ -n "${DEMO_TEST_FAST_WAIT:-}" ]]; then exit 0; fi
exec /bin/sleep "$@"
EOF

chmod +x "$scratch/bin/"*

run_wizard() {
  local name="$1" input="$2"
  : >"$scratch/$name.calls"
  DEMO_TEST_CALLS="$scratch/$name.calls" \
  DEMO_WIZARD_STATE_DIR="$scratch/state-$name" \
  DEMO_OPERATOR_SPEECH_ROOT="$operator_speech_root" \
  DEMO_OPERATOR_SPEECH_PROFILE="$operator_speech_profile" \
  DEMO_OPERATOR_SPEECH_ENV="$operator_speech_env" \
  DEMO_OPERATOR_SPEECH_EXECUTABLE="$operator_speech_executable" \
  LIVE_PROFILE_DATABASE_URL="${LIVE_PROFILE_DATABASE_URL:-}" \
  PATH="$scratch/bin:/usr/bin:/bin" \
  "$root/deploy/scripts/demo-wizard.sh" \
    >"$scratch/$name.out" 2>"$scratch/$name.err" <<<"$input"
}

full_input=$(printf '1\n%s\n%s\n%s\n%s\nspeech-image:local\n%s\n' \
  'postgres-password-sentinel' 'local-bearer-sentinel' 'speech-key-sentinel' \
  'deepseek-key-sentinel' "$speech_config")
run_wizard full "$full_input"
grep -Fx 'durable-demo-migrate' "$scratch/full.calls" >/dev/null || fail "full mode skipped migration"
grep -Fx 'durable-demo-up' "$scratch/full.calls" >/dev/null || fail "full mode skipped durable startup"
grep -F -- '-gateway-url ws://127.0.0.1:18080/v1/realtime -model gpt-live-simulator' "$scratch/state-full/live-client.calls" >/dev/null || {
  sed 's/^/call: /' "$scratch/full.calls" "$scratch/state-full/live-client.calls" >&2
  fail "full mode used the wrong BFF model"
}
grep -F 'http://127.0.0.1:3000/' "$scratch/full.out" >/dev/null || fail "full mode omitted the demo URL"

for secret in postgres-password-sentinel local-bearer-sentinel speech-key-sentinel deepseek-key-sentinel; do
  if grep -R -F "$secret" "$scratch/full.out" "$scratch/full.err" "$scratch/full.calls" "$scratch/state-full" 2>/dev/null; then
    fail "full mode emitted or persisted a secret"
  fi
done
[[ ! -e "$root/.env" ]] || fail "wizard wrote a repository .env file"

client_input=$(printf '2\n%s\n\nclient-model\n' 'local-bearer-sentinel')
run_wizard client "$client_input"
grep -F -- '-gateway-url ws://127.0.0.1:18080/v1/realtime -model client-model' "$scratch/state-client/live-client.calls" >/dev/null || fail "client mode changed the requested BFF config"
if grep -F 'durable-demo-' "$scratch/client.calls" >/dev/null; then
  fail "client mode contacted the durable composition"
fi

run_wizard status $'3\n'
grep -F 'gateway: ready' "$scratch/status.out" >/dev/null || fail "status omitted gateway readiness"
grep -F 'worker: ready' "$scratch/status.out" >/dev/null || fail "status omitted worker readiness"

run_wizard stop $'4\npostgres-password-sentinel\n'
grep -Fx 'durable-demo-down' "$scratch/stop.calls" >/dev/null || fail "stop mode skipped durable shutdown"

control_input=$(printf '5\n%s\n%s\n%s\n\n' \
  'postgres-password-sentinel' 'local-bearer-sentinel' 'speech-key-sentinel')
run_wizard control "$control_input"
grep -F 'http://127.0.0.1:3000/setup' "$scratch/control.out" >/dev/null || fail "control-panel mode omitted the setup URL"
grep -F -- '-operator -operator-state-dir' "$scratch/state-control/live-client.calls" >/dev/null || fail "control-panel mode did not explicitly enable local operator controls"
grep -F -- '-operator-speech-ready-url http://127.0.0.1:8766/v1/pool' "$scratch/state-control/live-client.calls" >/dev/null || fail "control-panel mode did not fix external speech readiness"
grep -F -- '-operator-repository-root' "$scratch/state-control/live-client.calls" >/dev/null || fail "control-panel mode omitted the fixed repository root"
grep -Fx 'operator diagnostic sentinel' "$scratch/control.err" >/dev/null || fail "control-panel mode discarded the operator-only diagnostic"
if grep -F 'durable-demo-' "$scratch/control.calls" >/dev/null || grep -F 'docker ' "$scratch/control.calls" >/dev/null; then
  fail "control-panel mode performed live service operations before browser authorization"
fi
for secret in postgres-password-sentinel local-bearer-sentinel speech-key-sentinel deepseek-key-sentinel; do
  if grep -F "$secret" "$scratch/state-control/live-client.calls" "$scratch/control.out" "$scratch/control.err"; then
    fail "control-panel mode placed a secret in argv or output"
  fi
done

if grep -F -- '-vertical-profiles' "$scratch/state-control/live-client.calls" >/dev/null; then
  fail "control-panel mode enabled vertical profiles without a profile database"
fi
grep -F 'Vertical profiles: off' "$scratch/control.out" >/dev/null || fail "control-panel mode did not report that profiles are off"

# Success must keep the wizard attached to its owned BFF. If the recipe shell
# exits, launch environments may reap the background child; the BFF then
# correctly shuts down Gateway/Speech and the browser reports a transport
# close. Stop the exact fake BFF to release this dedicated foreground probe.
: >"$scratch/attached.calls"
DEMO_TEST_CALLS="$scratch/attached.calls" \
  DEMO_WIZARD_STATE_DIR="$scratch/state-attached" \
  DEMO_OPERATOR_SPEECH_ROOT="$operator_speech_root" \
  DEMO_OPERATOR_SPEECH_PROFILE="$operator_speech_profile" \
  DEMO_OPERATOR_SPEECH_ENV="$operator_speech_env" \
  DEMO_OPERATOR_SPEECH_EXECUTABLE="$operator_speech_executable" \
  PATH="$scratch/bin:/usr/bin:/bin" \
  "$root/deploy/scripts/demo-wizard.sh" \
    >"$scratch/attached.out" 2>"$scratch/attached.err" <<<"$control_input" &
attached_wizard_pid=$!
for _ in {1..100}; do
  grep -F 'Browser control panel ready' "$scratch/attached.out" >/dev/null 2>&1 && break
  /bin/sleep 0.02
done
grep -F 'Browser control panel ready' "$scratch/attached.out" >/dev/null || fail "attached wizard did not become ready"
kill -0 "$attached_wizard_pid" 2>/dev/null || fail "successful wizard exited while its BFF was still running"
IFS= read -r attached_bff_pid <"$scratch/state-attached/bff.pid"
[[ "$attached_bff_pid" =~ ^[1-9][0-9]*$ ]] || fail "attached wizard did not retain BFF ownership"
kill "$attached_bff_pid"
wait "$attached_wizard_pid" 2>/dev/null || true

# Operator construction may perform bounded local ownership discovery before
# the listener is published. The wizard must allow more than its historical
# two-second readiness window instead of killing a healthy delayed BFF.
: >"$scratch/delayed-ready.calls"
DEMO_TEST_CALLS="$scratch/delayed-ready.calls" \
  DEMO_TEST_READY_AFTER=100 DEMO_TEST_READY_COUNTER="$scratch/delayed-ready.attempts" \
  DEMO_TEST_FAST_WAIT=1 DEMO_WIZARD_STATE_DIR="$scratch/state-delayed-ready" \
  DEMO_OPERATOR_SPEECH_ROOT="$operator_speech_root" \
  DEMO_OPERATOR_SPEECH_PROFILE="$operator_speech_profile" \
  DEMO_OPERATOR_SPEECH_ENV="$operator_speech_env" \
  DEMO_OPERATOR_SPEECH_EXECUTABLE="$operator_speech_executable" \
  PATH="$scratch/bin:/usr/bin:/bin" \
  "$root/deploy/scripts/demo-wizard.sh" \
    >"$scratch/delayed-ready.out" 2>"$scratch/delayed-ready.err" <<<"$control_input"
grep -F 'Browser control panel ready' "$scratch/delayed-ready.out" >/dev/null || fail "wizard rejected a delayed healthy BFF"
[[ "$(wc -l <"$scratch/delayed-ready.attempts" | tr -d ' ')" -eq 100 ]] || fail "delayed readiness fixture did not exercise 100 probes"

if DEMO_TEST_STALE_READY=1 DEMO_TEST_LIVE_CLIENT_EXIT=1 run_wizard stale-listener "$control_input"; then
  fail "control-panel mode accepted readiness from a stale port listener"
fi
grep -F 'browser-bff port is already in use' "$scratch/stale-listener.err" >/dev/null || {
  fail "stale port rejection lacks a concise action"
}
[[ ! -s "$scratch/state-stale-listener/bff.pid" ]] || fail "stale port rejection overwrote BFF ownership proof"

# With a profile database URL exported, the wizard enables the profile tabs and
# passes the variable name only. The URL must reach the child environment and
# never appear in argv, stdout, or any state file.
LIVE_PROFILE_DATABASE_URL='postgres://live:profile-url-sentinel@127.0.0.1:5432/live?sslmode=disable' \
  run_wizard profiles "$control_input"
grep -F -- '-vertical-profiles -profile-tenant development -profile-actor operator -profile-database-url-env LIVE_PROFILE_DATABASE_URL -profile-adapter-registry-version v1:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' \
  "$scratch/state-profiles/live-client.calls" >/dev/null || {
  sed 's/^/call: /' "$scratch/state-profiles/live-client.calls" >&2
  fail "profile mode did not pass the profile flags by environment-variable name"
}
grep -F 'Vertical profiles: on' "$scratch/profiles.out" >/dev/null || fail "profile mode did not report that profiles are on"
grep -F 'Tools & adapters' "$scratch/profiles.out" >/dev/null || fail "profile mode omitted the adapter workspace"
grep -Fx 'profile database sentinel' "$scratch/profiles.err" >/dev/null || fail "profile mode did not forward the profile database URL to the child"
for location in "$scratch/state-profiles/live-client.calls" "$scratch/profiles.out" "$scratch/profiles.err" "$scratch/profiles.calls"; do
  if grep -F 'profile-url-sentinel' "$location" >/dev/null; then
    fail "profile mode leaked the profile database URL"
  fi
done

: >"$scratch/failure.calls"
if DEMO_TEST_CALLS="$scratch/failure.calls" DEMO_TEST_CURL_EXIT=22 \
  DEMO_WIZARD_STATE_DIR="$scratch/state-failure" PATH="$scratch/bin:/usr/bin:/bin" \
  "$root/deploy/scripts/demo-wizard.sh" >"$scratch/failure.out" 2>"$scratch/failure.err" <<<"$full_input"; then
  fail "wizard accepted failed readiness"
fi
[[ ! -s "$scratch/state-failure/bff.pid" ]] || fail "failed startup retained a BFF pid"
grep -F 'did not become ready' "$scratch/failure.err" >/dev/null || fail "failed readiness lacks a concise action"

grep -F -- '--connect-timeout 0.05 --max-time 0.1' "$scratch/full.calls" >/dev/null || fail "readiness requests are not bounded"

# A prior BFF can still be performing bounded server cleanup. Exercise the
# real trap/function body with a delayed owned PID (all wait primitives are
# local shell fakes, so no process is killed). Its proof must survive the
# nonzero replacement path and EXIT trap.
preserve_state="$scratch/state-preserve"
mkdir -p "$preserve_state"
printf '%s\n' '#!/bin/sh' >"$preserve_state/live-client"
chmod 700 "$preserve_state/live-client"
printf '%s\n' '42' >"$preserve_state/bff.pid"
sed '/^_clear$/,$d' "$root/deploy/scripts/demo-wizard.sh" >"$scratch/wizard-functions.sh"
if DEMO_WIZARD_STATE_DIR="$preserve_state" bash -c '
  set -euo pipefail
  source "$1"
  startup_complete=false
  bff_pid=""
  ps() { printf "%s --operator\\n" "$state_dir/live-client"; }
  kill() { return 0; }
  sleep() { :; }
  stop_current_bff
' bash "$scratch/wizard-functions.sh" >"$scratch/preserve.out" 2>"$scratch/preserve.err"; then
  fail "wizard replaced a BFF whose cleanup proof was still live"
fi
grep -Fx '  ⚠ browser BFF cleanup is still in progress; preserving ownership proof' "$scratch/preserve.out" >/dev/null || {
  fail "failed replacement omitted its intentional cleanup warning"
}
[[ "$(wc -l <"$scratch/preserve.out" | tr -d ' ')" -eq 1 ]] || fail "failed replacement emitted unexpected stdout"
[[ ! -s "$scratch/preserve.err" ]] || fail "failed replacement emitted unexpected stderr"
[[ -f "$preserve_state/bff.pid" && -f "$preserve_state/live-client" ]] || fail "failed replacement deleted the prior BFF proof"

printf '%s\n' 'interactive demo wizard boundary tests passed'
