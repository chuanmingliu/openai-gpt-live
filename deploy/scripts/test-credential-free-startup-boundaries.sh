#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
harness=$root/deploy/scripts/test-credential-free-startup.sh
scratch=$(mktemp -d "${TMPDIR:-/tmp}/gpt-live-credential-boundaries.XXXXXX")
trap 'rm -rf "$scratch"' EXIT HUP INT TERM
speech_port=28765

[ -x "$harness" ] || {
  echo 'credential-free startup harness is unavailable' >&2
  exit 1
}

cat >"$scratch/fake-service.py" <<'PY'
import http.server
import os
import signal
import socket
import sys

role, scenario = sys.argv[1:]
if scenario == "early-exit" and role == "gateway":
    raise SystemExit(17)
if scenario == "environment-dependency" and role == "gateway" and "UNEXPECTED_DEPENDENCY" not in os.environ:
    raise SystemExit(18)

class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if role == "gateway":
            if self.path == "/v1/realtime/capabilities":
                self.send_response(200); self.end_headers(); self.wfile.write(b'{"model":"fixture"}')
            elif self.path == "/health/live":
                self.send_response(200); self.end_headers(); self.wfile.write(b"ok\\n")
            else:
                self.send_response(404); self.end_headers()
        else:
            if self.path == "/health/live":
                self.send_response(200); self.end_headers(); self.wfile.write(b"ok\\n")
            elif self.path == "/health/ready":
                status = 200 if scenario == "ready-200" else 503
                body = b"not ready" if scenario == "ready-body" else b""
                self.send_response(status); self.end_headers(); self.wfile.write(body)
            else:
                self.send_response(404); self.end_headers()
    def log_message(self, *_):
        pass

host = "0.0.0.0" if scenario == "non-loopback" and role == "gateway" else "127.0.0.1"
ports = [8080, 9090] if role == "gateway" else [9091]
servers = []
for port in ports:
    server = http.server.HTTPServer((host, port), Handler, bind_and_activate=False)
    server.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.server_bind(); server.server_activate()
    servers.append(server)

if scenario == "term-hang" and role == "worker":
    signal.signal(signal.SIGTERM, signal.SIG_IGN)

import threading
for server in servers[1:]:
    threading.Thread(target=server.serve_forever, daemon=True).start()
servers[0].serve_forever()
PY

cat >"$scratch/live-gateway" <<EOF
#!/bin/sh
exec python3 "$scratch/fake-service.py" gateway "\$(cat "$scratch/scenario")"
EOF
cat >"$scratch/live-worker" <<EOF
#!/bin/sh
[ "\${1-}" = --backend ] && [ "\${2-}" = none ]
exec python3 "$scratch/fake-service.py" worker "\$(cat "$scratch/scenario")"
EOF
chmod 0755 "$scratch/live-gateway" "$scratch/live-worker"

printf '%s\n' healthy >"$scratch/scenario"
if ! "$harness" --gateway-binary "$scratch/live-gateway" --worker-binary "$scratch/live-worker" --speech-port "$speech_port" \
  >"$scratch/healthy.out" 2>"$scratch/healthy.err"; then
  echo 'credential-free smoke rejected the healthy fixture' >&2
  exit 1
fi
[ "$(cat "$scratch/healthy.out")" = 'credential_free_startup_summary gateway=ready worker=live speech_connections=0' ]
[ ! -s "$scratch/healthy.err" ]

for scenario in early-exit non-loopback ready-body ready-200 term-hang environment-dependency; do
  printf '%s\n' "$scenario" >"$scratch/scenario"
  if UNEXPECTED_DEPENDENCY=present \
    "$harness" --gateway-binary "$scratch/live-gateway" --worker-binary "$scratch/live-worker" --speech-port "$speech_port" \
      >"$scratch/$scenario.out" 2>"$scratch/$scenario.err"
  then
    echo "credential-free smoke accepted $scenario" >&2
    exit 1
  fi
  if grep -F 'UNEXPECTED_DEPENDENCY' "$scratch/$scenario.out" "$scratch/$scenario.err" >/dev/null; then
    echo "credential-free smoke leaked environment content" >&2
    exit 1
  fi
  [ "$(wc -c <"$scratch/$scenario.out")" -lt 4096 ]
  [ "$(wc -c <"$scratch/$scenario.err")" -lt 4096 ]
done

echo 'credential-free startup boundary tests passed'
