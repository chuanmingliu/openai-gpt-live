#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$root"
smoke_dir=$(mktemp -d "${TMPDIR:-/tmp}/gpt-live-credential-free.XXXXXX")
gateway_pid=
worker_pid=
speech_pid=

cleanup() {
  status=$?
  trap - EXIT HUP INT TERM
  for pid in "$worker_pid" "$gateway_pid" "$speech_pid"; do
    [ -n "$pid" ] || continue
    kill -TERM "$pid" 2>/dev/null || true
  done
  attempts=0
  while [ "$attempts" -lt 20 ]; do
    alive=false
    for pid in "$worker_pid" "$gateway_pid" "$speech_pid"; do
      [ -z "$pid" ] || ! kill -0 "$pid" 2>/dev/null || alive=true
    done
    [ "$alive" = true ] || break
    sleep 0.05
    attempts=$((attempts + 1))
  done
  for pid in "$worker_pid" "$gateway_pid" "$speech_pid"; do
    [ -n "$pid" ] || continue
    kill -KILL "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  done
  rm -rf "$smoke_dir"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM

fail() {
  echo 'credential_free_startup: verification failed' >&2
  exit 1
}

gateway_binary=
worker_binary=
speech_port=8765
while [ "$#" -gt 0 ]; do
  case "$1" in
    --gateway-binary)
      [ "$#" -ge 2 ] && [ -z "$gateway_binary" ] || fail
      gateway_binary=$2
      shift 2
      ;;
    --worker-binary)
      [ "$#" -ge 2 ] && [ -z "$worker_binary" ] || fail
      worker_binary=$2
      shift 2
      ;;
    --speech-port)
      [ "$#" -ge 2 ] || fail
      case "$2" in *[!0-9]*|'') fail ;; esac
      [ "$2" -ge 1024 ] && [ "$2" -le 65535 ] || fail
      speech_port=$2
      shift 2
      ;;
    *) fail ;;
  esac
done

if [ -n "$gateway_binary" ] || [ -n "$worker_binary" ]; then
  [ -x "$gateway_binary" ] && [ -x "$worker_binary" ] || fail
else
  [ "$speech_port" = 8765 ] || fail
  gateway_binary=$smoke_dir/live-gateway
  worker_binary=$smoke_dir/live-worker
  env GOCACHE=/tmp/gpt-live-production-cache go build -o "$gateway_binary" ./cmd/live-gateway \
    >/dev/null 2>&1 || fail
  env GOCACHE=/tmp/gpt-live-production-cache go build -o "$worker_binary" ./cmd/live-worker \
    >/dev/null 2>&1 || fail
fi

speech_count=$smoke_dir/speech-count
speech_ready=$smoke_dir/speech-ready
env -i PATH="$PATH" python3 - "$speech_count" "$speech_ready" "$speech_port" <<'PY' >/dev/null 2>&1 &
import os
import select
import signal
import socket
import sys

count_path, ready_path, port_text = sys.argv[1:]
running = True
accepted = 0

def stop(_signal, _frame):
    global running
    running = False

signal.signal(signal.SIGTERM, stop)
listener = socket.socket()
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(("127.0.0.1", int(port_text)))
listener.listen()
with open(count_path, "w", encoding="ascii") as handle:
    handle.write("0\n")
with open(ready_path, "w", encoding="ascii") as handle:
    handle.write("ready\n")
while running:
    readable, _, _ = select.select([listener], [], [], 0.1)
    if readable:
        connection, _ = listener.accept()
        connection.close()
        accepted += 1
        with open(count_path, "w", encoding="ascii") as handle:
            handle.write(f"{accepted}\n")
listener.close()
PY
speech_pid=$!

attempts=0
while [ ! -s "$speech_ready" ] && kill -0 "$speech_pid" 2>/dev/null && [ "$attempts" -lt 50 ]; do
  sleep 0.05
  attempts=$((attempts + 1))
done
[ -s "$speech_ready" ] && kill -0 "$speech_pid" 2>/dev/null || fail

if [ "$(uname -s)" = Darwin ]; then
  env -i PATH="$PATH" \
    DYLD_LIBRARY_PATH="${DYLD_LIBRARY_PATH-}" \
    DYLD_FALLBACK_LIBRARY_PATH="${DYLD_FALLBACK_LIBRARY_PATH-}" \
    "$gateway_binary" >/dev/null 2>&1 &
  gateway_pid=$!
  env -i PATH="$PATH" \
    DYLD_LIBRARY_PATH="${DYLD_LIBRARY_PATH-}" \
    DYLD_FALLBACK_LIBRARY_PATH="${DYLD_FALLBACK_LIBRARY_PATH-}" \
    "$worker_binary" --backend none >/dev/null 2>&1 &
  worker_pid=$!
else
  env -i PATH="$PATH" "$gateway_binary" >/dev/null 2>&1 &
  gateway_pid=$!
  env -i PATH="$PATH" "$worker_binary" --backend none >/dev/null 2>&1 &
  worker_pid=$!
fi

fetch() {
  url=$1
  want_status=$2
  body_file=$3
  attempts=0
  while [ "$attempts" -lt 50 ]; do
    kill -0 "$gateway_pid" 2>/dev/null && kill -0 "$worker_pid" 2>/dev/null || return 1
    set +e
    status=$(curl --noproxy '*' -sS --connect-timeout 0.1 --max-time 0.2 \
      --max-filesize 4096 -o "$body_file" -w '%{http_code}' "$url" 2>/dev/null)
    curl_status=$?
    set -e
    if [ "$curl_status" -eq 0 ] && [ "$status" = "$want_status" ]; then
      [ "$(wc -c <"$body_file")" -le 4096 ] || return 1
      return 0
    fi
    sleep 0.05
    attempts=$((attempts + 1))
  done
  return 1
}

fetch http://127.0.0.1:8080/v1/realtime/capabilities 200 "$smoke_dir/capabilities" || fail
python3 - "$smoke_dir/capabilities" <<'PY' >/dev/null 2>&1 || fail
import json
import sys
with open(sys.argv[1], "rb") as handle:
    value = json.load(handle)
if not isinstance(value, dict) or not value:
    raise SystemExit(1)
PY
fetch http://127.0.0.1:9090/health/live 200 "$smoke_dir/gateway-live" || fail
fetch http://127.0.0.1:9091/health/live 200 "$smoke_dir/worker-live" || fail
fetch http://127.0.0.1:9091/health/ready 503 "$smoke_dir/worker-ready" || fail
[ ! -s "$smoke_dir/worker-ready" ] || fail

assert_listeners() {
  pid=$1
  shift
  lsof -nP -a -p "$pid" -iTCP -sTCP:LISTEN -Fn \
    >"$smoke_dir/listeners-$pid" 2>/dev/null || return 1
  sed -n 's/^n//p' "$smoke_dir/listeners-$pid" | LC_ALL=C sort >"$smoke_dir/listeners-$pid.actual"
  printf '%s\n' "$@" | LC_ALL=C sort >"$smoke_dir/listeners-$pid.expected"
  cmp -s "$smoke_dir/listeners-$pid.expected" "$smoke_dir/listeners-$pid.actual"
}

assert_listeners "$speech_pid" "127.0.0.1:$speech_port" || fail
assert_listeners "$gateway_pid" '127.0.0.1:8080' '127.0.0.1:9090' || fail
assert_listeners "$worker_pid" '127.0.0.1:9091' || fail
[ "$(cat "$speech_count")" = 0 ] || fail

stop_and_join() {
  label=$1
  pid=$2
  marker=$smoke_dir/$label-forced-kill
  kill -TERM "$pid" 2>/dev/null || return 1
  (
    attempts=0
    while kill -0 "$pid" 2>/dev/null && [ "$attempts" -lt 50 ]; do
      sleep 0.1
      attempts=$((attempts + 1))
    done
    if kill -0 "$pid" 2>/dev/null; then
      : >"$marker"
      kill -KILL "$pid" 2>/dev/null || true
    fi
  ) &
  watcher=$!
  wait "$pid" 2>/dev/null || true
  wait "$watcher" 2>/dev/null || true
  [ ! -e "$marker" ]
}

stop_and_join worker "$worker_pid" || fail
worker_pid=
stop_and_join gateway "$gateway_pid" || fail
gateway_pid=
stop_and_join speech "$speech_pid" || fail
speech_pid=

echo 'credential_free_startup_summary gateway=ready worker=live speech_connections=0'
