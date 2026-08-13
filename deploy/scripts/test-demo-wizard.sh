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

cat >"$scratch/bin/go" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$DEMO_TEST_CALLS"
if [[ "$*" == 'run ./cmd/live-gateway config-hash --config deploy/config/development-durable.yaml' ]]; then
  printf '%s\n' 'v1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
  exit 0
fi
if [[ "${1:-}" == 'build' && "${2:-}" == '-o' ]]; then
  output=$3
  cat >"$output" <<'SCRIPT'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "live-client $*" >>"${0}.calls"
[[ -n "${LIVE_DEMO_BEARER:-}" ]] || exit 91
for forbidden in POSTGRES_PASSWORD S2S_API_KEY DEEPSEEK_API_KEY LIVE_STATIC_BEARER_TOKEN LIVE_ADMIN_BEARER_TOKEN LIVE_PRIVACY_HMAC_KEY LIVE_RETENTION_HMAC_KEY LIVE_RECONNECT_HMAC_KEY LIVE_TOOL_ENCRYPTION_KEY; do
  [[ -z "${!forbidden:-}" ]] || exit 94
done
[[ "$*" == *'-model gpt-live-simulator'* || "$*" == *'-model client-model'* ]] || exit 92
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

chmod +x "$scratch/bin/"*

run_wizard() {
  local name="$1" input="$2"
  : >"$scratch/$name.calls"
  DEMO_TEST_CALLS="$scratch/$name.calls" \
  DEMO_WIZARD_STATE_DIR="$scratch/state-$name" \
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

: >"$scratch/failure.calls"
if DEMO_TEST_CALLS="$scratch/failure.calls" DEMO_TEST_CURL_EXIT=22 \
  DEMO_WIZARD_STATE_DIR="$scratch/state-failure" PATH="$scratch/bin:/usr/bin:/bin" \
  "$root/deploy/scripts/demo-wizard.sh" >"$scratch/failure.out" 2>"$scratch/failure.err" <<<"$full_input"; then
  fail "wizard accepted failed readiness"
fi
[[ ! -s "$scratch/state-failure/bff.pid" ]] || fail "failed startup retained a BFF pid"
grep -F 'did not become ready' "$scratch/failure.err" >/dev/null || fail "failed readiness lacks a concise action"

grep -F -- '--connect-timeout 0.05 --max-time 0.1' "$scratch/full.calls" >/dev/null || fail "readiness requests are not bounded"

printf '%s\n' 'interactive demo wizard boundary tests passed'
