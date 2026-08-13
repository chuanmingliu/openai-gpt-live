#!/usr/bin/env bash
#
# Interactive, session-only launcher for the loopback realtime browser demo.
# Secret values are inherited only by child processes and are never persisted.

set -euo pipefail

if [[ -t 1 ]] && command -v tput >/dev/null 2>&1 && [[ "$(tput colors 2>/dev/null || echo 0)" -ge 8 ]]; then
  BOLD=$(tput bold); DIM=$(tput dim); RESET=$(tput sgr0)
  BLUE=$(tput setaf 4); GREEN=$(tput setaf 2); YELLOW=$(tput setaf 3); RED=$(tput setaf 1)
else
  BOLD=""; DIM=""; RESET=""; BLUE=""; GREEN=""; YELLOW=""; RED=""
fi

TOTAL_STAGES=0
_STAGE_INDEX=0

_clear() {
  [[ -t 1 ]] || return 0
  if command -v tput >/dev/null 2>&1; then tput clear; else printf '\033[2J\033[3J\033[H'; fi
}

stage() {
  _clear
  _STAGE_INDEX=$((_STAGE_INDEX + 1))
  printf '\n%s%s▸ Stage %s/%s · %s%s\n' \
    "$BOLD" "$BLUE" "$_STAGE_INDEX" "$TOTAL_STAGES" "$1" "$RESET"
}

say()  { printf '  %s\n' "$1"; }
note() { printf '  %s%s%s\n' "$DIM" "$1" "$RESET"; }
warn() { printf '  %s⚠ %s%s\n' "$YELLOW" "$1" "$RESET"; }

ask() {
  local key="$1" prompt="$2" default_value="${3:-}" input=""
  if [[ -n "$default_value" ]]; then
    printf '  %s%s%s %s[%s]%s ' "$BOLD" "$prompt" "$RESET" "$DIM" "$default_value" "$RESET"
  else
    printf '  %s%s%s ' "$BOLD" "$prompt" "$RESET"
  fi
  read -r input || true
  [[ -z "$input" ]] && input="$default_value"
  printf -v "$key" '%s' "$input"
}

ask_secret() {
  local key="$1" prompt="$2" input=""
  printf '  %s%s%s ' "$BOLD" "$prompt" "$RESET"
  read -rs input || true
  printf '\n'
  printf -v "$key" '%s' "$input"
}

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$root"
umask 077
state_dir=${DEMO_WIZARD_STATE_DIR:-"${TMPDIR:-/tmp}/gpt-live-demo-wizard-${UID}"}
mkdir -p "$state_dir"
chmod 700 "$state_dir"

bff_pid=""
durable_started=false
startup_complete=false

stop_current_bff() {
  local pid="" command=""
  [[ -f "$state_dir/bff.pid" ]] || return 0
  IFS= read -r pid <"$state_dir/bff.pid" || true
  if [[ "$pid" =~ ^[1-9][0-9]*$ ]] && kill -0 "$pid" 2>/dev/null; then
    command=$(ps -p "$pid" -o command= 2>/dev/null || true)
    if [[ "$command" == "$state_dir/live-client"* ]]; then
      kill "$pid" 2>/dev/null || true
      for _ in {1..20}; do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.05
      done
    else
      warn "saved browser PID is not owned by this wizard; leaving it untouched"
    fi
  fi
  rm -f "$state_dir/bff.pid" "$state_dir/live-client"
}

cleanup_failed_start() {
  local exit_code=$?
  if [[ "$startup_complete" != true ]]; then
    if [[ -n "$bff_pid" ]] && kill -0 "$bff_pid" 2>/dev/null; then
      kill "$bff_pid" 2>/dev/null || true
      wait "$bff_pid" 2>/dev/null || true
    fi
    rm -f "$state_dir/bff.pid" "$state_dir/live-client"
    if [[ "$durable_started" == true && -n "${POSTGRES_PASSWORD:-}" ]]; then
      make durable-demo-down >/dev/null 2>&1 || true
    fi
  fi
  return "$exit_code"
}
trap cleanup_failed_start EXIT HUP INT TERM

require_command() {
  command -v "$1" >/dev/null 2>&1 || {
    printf 'missing prerequisite: %s\n' "$1" >&2
    exit 1
  }
}

require_nonempty() {
  local label="$1" value="$2"
  [[ -n "$value" ]] || {
    printf '%s is required\n' "$label" >&2
    exit 1
  }
}

wait_ready() {
  local label="$1" url="$2"
  for _ in {1..20}; do
    if curl --fail --silent --output /dev/null \
      --connect-timeout 0.05 --max-time 0.1 "$url"; then
      printf '  %s: ready\n' "$label"
      return 0
    fi
    sleep 0.1
  done
  printf '%s did not become ready; choose Check status, then consult the advanced operator guide\n' "$label" >&2
  return 1
}

check_status() {
  local label="$1" url="$2"
  if curl --fail --silent --output /dev/null \
    --connect-timeout 0.05 --max-time 0.1 "$url"; then
    printf '  %s: ready\n' "$label"
  else
    printf '  %s: not ready\n' "$label"
  fi
}

start_bff() {
  local bearer="$1" gateway_url="$2" model="$3"
  stop_current_bff
  rm -f "$state_dir/live-client.calls"
  go build -o "$state_dir/live-client" ./demo/live-client
  env -i PATH="$PATH" LIVE_DEMO_BEARER="$bearer" "$state_dir/live-client" \
    -gateway-url "$gateway_url" -model "$model" >/dev/null 2>&1 &
  bff_pid=$!
  printf '%s\n' "$bff_pid" >"$state_dir/bff.pid"
  wait_ready browser-bff http://127.0.0.1:3000/health/ready
}

print_success() {
  startup_complete=true
  trap - EXIT HUP INT TERM
  printf '\n%s%s✓ Demo ready%s\n' "$BOLD" "$GREEN" "$RESET"
  printf '  Open: http://127.0.0.1:3000/\n'
  printf '  Grant microphone and speaker permission, then select Start listening.\n'
  printf '  Run make demo-wizard again for status or shutdown.\n\n'
}

run_full() {
  TOTAL_STAGES=4
  for command in go make docker curl jq openssl; do require_command "$command"; done
  docker compose version >/dev/null 2>&1 || {
    printf 'Docker Compose is unavailable\n' >&2
    exit 1
  }

  stage "Credentials"
  say "Values are hidden and remain only in this wizard session and its child services."
  ask_secret POSTGRES_PASSWORD "Disposable PostgreSQL password:"
  ask_secret LIVE_STATIC_BEARER_TOKEN "New local gateway bearer:"
  ask_secret S2S_API_KEY "Speech-service bearer:"
  ask_secret DEEPSEEK_API_KEY "DeepSeek bearer:"
  require_nonempty "PostgreSQL password" "$POSTGRES_PASSWORD"
  require_nonempty "local gateway bearer" "$LIVE_STATIC_BEARER_TOKEN"
  require_nonempty "speech-service bearer" "$S2S_API_KEY"
  require_nonempty "DeepSeek bearer" "$DEEPSEEK_API_KEY"

  stage "Speech service"
  ask S2S_IMAGE "Reviewed speech image:"
  ask S2S_CONFIG_PATH "Absolute non-secret speech config path:"
  require_nonempty "speech image" "$S2S_IMAGE"
  [[ "$S2S_CONFIG_PATH" == /* && -f "$S2S_CONFIG_PATH" ]] || {
    printf 'speech config must be an existing absolute file\n' >&2
    exit 1
  }

  export POSTGRES_PASSWORD LIVE_STATIC_BEARER_TOKEN S2S_API_KEY DEEPSEEK_API_KEY S2S_IMAGE S2S_CONFIG_PATH
  export LIVE_ADMIN_BEARER_TOKEN="$LIVE_STATIC_BEARER_TOKEN"
  export LIVE_DEMO_BEARER="$LIVE_STATIC_BEARER_TOKEN"
  export LIVE_PRIVACY_HMAC_KEY="$(openssl rand -hex 32)"
  export LIVE_RETENTION_HMAC_KEY="$(openssl rand -base64 32)"
  export LIVE_RECONNECT_HMAC_KEY="$(openssl rand -base64 32)"
  export LIVE_TOOL_ENCRYPTION_KEY="$(openssl rand -base64 32)"
  export LIVE_EXPECTED_CONFIG_HASH="$(go run ./cmd/live-gateway config-hash --config deploy/config/development-durable.yaml)"
  require_nonempty "config hash" "$LIVE_EXPECTED_CONFIG_HASH"

  stage "PostgreSQL and services"
  say "Migrating the private PostgreSQL database, then starting speech, gateway, and worker."
  make durable-demo-migrate
  make durable-demo-up
  durable_started=true
  wait_ready gateway http://127.0.0.1:19090/health/ready
  wait_ready worker http://127.0.0.1:19091/health/ready

  stage "Browser client"
  start_bff "$LIVE_STATIC_BEARER_TOKEN" ws://127.0.0.1:18080/v1/realtime gpt-live-simulator
  print_success
}

run_client() {
  TOTAL_STAGES=2
  for command in go curl; do require_command "$command"; done

  stage "Gateway"
  say "This mode starts only the browser BFF; your loopback gateway must already be running."
  ask_secret LIVE_DEMO_BEARER "Local gateway bearer:"
  ask GATEWAY_URL "Gateway WebSocket URL:" ws://127.0.0.1:18080/v1/realtime
  ask GATEWAY_MODEL "Gateway advertised model:" gpt-live-simulator
  require_nonempty "local gateway bearer" "$LIVE_DEMO_BEARER"
  require_nonempty "gateway URL" "$GATEWAY_URL"
  require_nonempty "gateway model" "$GATEWAY_MODEL"

  stage "Browser client"
  start_bff "$LIVE_DEMO_BEARER" "$GATEWAY_URL" "$GATEWAY_MODEL"
  print_success
}

run_status() {
  TOTAL_STAGES=1
  require_command curl
  stage "Local status"
  check_status browser-bff http://127.0.0.1:3000/health/ready
  check_status gateway http://127.0.0.1:19090/health/ready
  check_status worker http://127.0.0.1:19091/health/ready
  startup_complete=true
}

run_stop() {
  TOTAL_STAGES=1
  stage "Stop local demo"
  stop_current_bff
  ask_secret POSTGRES_PASSWORD "PostgreSQL password (Enter to stop only the browser client):"
  if [[ -n "$POSTGRES_PASSWORD" ]]; then
    export POSTGRES_PASSWORD
    make durable-demo-down
    printf '  durable services: stopped; PostgreSQL volume preserved\n'
  else
    printf '  durable services: unchanged\n'
  fi
  printf '  browser client: stopped if owned by this wizard\n'
  startup_complete=true
}

_clear
printf '\n%s%sGPT Live realtime demo%s\n' "$BOLD" "$BLUE" "$RESET"
printf '  1) Full durable demo\n'
printf '  2) Client only\n'
printf '  3) Check status\n'
printf '  4) Stop demo\n\n'
printf '  Select [1-4]: '
read -r choice || true

case "$choice" in
  1) run_full ;;
  2) run_client ;;
  3) run_status ;;
  4) run_stop ;;
  *) printf 'select 1, 2, 3, or 4\n' >&2; exit 2 ;;
esac
