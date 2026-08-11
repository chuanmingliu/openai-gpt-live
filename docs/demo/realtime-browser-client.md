# Realtime browser client operator guide

This guide is the focused local path for one realtime voice turn through the
browser BFF and `live-gateway`. Run the commands from the repository root in
one `zsh` session so the recorded process IDs and hidden environment values
remain available for the stop step.

You provide:

- the speech WebSocket URL, provider model, and API key;
- one local gateway static bearer; and
- browser microphone/speaker permission and the input/output device choice.

The repository provides:

- the fixed 24 kHz mono PCM16LE and 960-byte/20 ms frame values;
- loopback BFF defaults;
- the gateway and demo build/start commands below; and
- content-free process, health, and stop checks.

Not needed for this realtime-only voice path:

- a DeepSeek API key;
- PostgreSQL handoff configuration; or
- `live-worker`.

Those components are required only for durable asynchronous handoffs. Your
chosen speech service can still require its own model-provider credential.
Keep every credential in the operator shell environment: do not put one in a
tracked `.env`, YAML, URL query, browser storage, or command-line argument.

## 1. Configure the speech service

Use an OpenAI-Realtime-compatible WebSocket service. The copyable reference
path below follows the public
[`speech-to-speech` quickstart](https://github.com/huggingface/speech-to-speech#quickstart),
which serves `ws://127.0.0.1:8765/v1/realtime` and accepts the Realtime model
name `local`. Install `speech-to-speech` as described there before continuing.
The exact launch command here is verified against the supplied local checkout:
its `pyproject.toml` maps `speech-to-speech` directly to
`speech_to_speech.s2s_pipeline:main`, and its local `--help` exposes flags at
the top level. It has no `serve` subcommand.

Prompt for the reference service's own LLM credential without putting its
value in shell history, then start only that service:

```zsh
read -r -s "OPENAI_API_KEY?Speech service LLM API key (hidden): "
export OPENAI_API_KEY
printf '\n'

speech-to-speech &
SPEECH_PID=$!
kill -0 "$SPEECH_PID"
```

Wait for the service's documented startup indication. Then accept the
reference endpoint/model defaults or enter the bare WebSocket URL and model
for a different service. The gateway adds the model query itself, so the URL
must not contain a query string. The reference server does not require client
authentication; enter `not-needed` at its hidden bearer prompt. For another
service, enter the actual gateway-to-speech bearer.

```zsh
read -r "SPEECH_WS_URL?Speech WebSocket URL [ws://127.0.0.1:8765/v1/realtime]: "
SPEECH_WS_URL="${SPEECH_WS_URL:-ws://127.0.0.1:8765/v1/realtime}"
read -r "SPEECH_MODEL?Speech model [local]: "
SPEECH_MODEL="${SPEECH_MODEL:-local}"

read -r -s "S2S_API_KEY?Gateway-to-speech bearer (hidden; use not-needed for the reference server): "
export S2S_API_KEY
printf '\n'
```

If you use an existing managed speech service, do not start the reference
process; supply its reviewed `wss://` URL/model/key and omit `SPEECH_PID` from
the stop step.

## 2. Configure the gateway

Create an untracked temporary runtime directory. This focused config has no
worker, store, handoff, DeepSeek, or PostgreSQL settings. It contains only
environment-variable names for credentials; the values never enter YAML.

```zsh
DEMO_RUNTIME_DIR="$(mktemp -d "${TMPDIR:-/tmp}/gpt-live-browser.XXXXXX")"
GATEWAY_CONFIG="$DEMO_RUNTIME_DIR/gateway.yaml"

wait_for_ready() {
  local service_name="$1"
  local owned_pid="$2"
  local ready_url="$3"
  local attempt

  for attempt in {1..50}; do
    if curl --fail --silent --output /dev/null "$ready_url"; then
      printf '%s\n' "$service_name ready"
      return 0
    fi
    if ! kill -0 "$owned_pid" 2>/dev/null; then
      printf '%s\n' "$service_name exited before readiness" >&2
      return 1
    fi
    sleep 0.1
  done
  printf '%s\n' "$service_name readiness timed out" >&2
  return 1
}

cat >"$GATEWAY_CONFIG" <<YAML
mode: local
gateway:
  advertised_model: gpt-live-real
  listen_addr: 127.0.0.1:18080
  voice:
    role: realtime_voice
    provider: speech_to_speech
    url: "${SPEECH_WS_URL}"
    model: "${SPEECH_MODEL}"
    api_key_env: S2S_API_KEY
  audio:
    sample_rate: 24000
    channels: 1
    bytes_per_sample: 2
    frame_bytes: 960
auth:
  static_bearer_env: LIVE_STATIC_BEARER_TOKEN
  static_tenant_id: browser-demo
  static_user_id: local-operator
  static_scopes: [realtime, live.admin]
  allowed_origins: [http://127.0.0.1:3000]
admin:
  listen_addr: 127.0.0.1:19090
YAML

go run ./cmd/live-gateway check-config --config "$GATEWAY_CONFIG"

read -r -s "LIVE_STATIC_BEARER_TOKEN?New local gateway bearer (hidden): "
export LIVE_STATIC_BEARER_TOKEN
export LIVE_DEMO_BEARER="$LIVE_STATIC_BEARER_TOKEN"
printf '\n'

go build -o "$DEMO_RUNTIME_DIR/live-gateway" ./cmd/live-gateway
"$DEMO_RUNTIME_DIR/live-gateway" -config "$GATEWAY_CONFIG" &
GATEWAY_PID=$!

wait_for_ready gateway "$GATEWAY_PID" http://127.0.0.1:19090/health/ready
```

`check-config` validates structure without reading secret values. The readiness
helper makes at most 50 content-free attempts, 0.1 seconds apart, and fails if
the owned process exits or the five-second bound expires. A successful check
proves only the local gateway process is ready; it does not prove provider
authentication, a successful speech turn, or audible playback.

## 3. Start the browser BFF

The flags below spell out the BFF's loopback defaults. Its bearer environment
must contain the same static bearer as the gateway. The bearer is injected
only into the upstream gateway request and is not put in browser assets, URL
queries, subprotocols, cookies, browser storage, response headers, or logs.

```zsh
go build -o "$DEMO_RUNTIME_DIR/live-client" ./demo/live-client
"$DEMO_RUNTIME_DIR/live-client" \
  -listen 127.0.0.1:3000 \
  -gateway-url ws://127.0.0.1:18080/v1/realtime \
  -model gpt-live-real \
  -origin http://127.0.0.1:3000 \
  -bearer-env LIVE_DEMO_BEARER &
BFF_PID=$!

wait_for_ready browser-bff "$BFF_PID" http://127.0.0.1:3000/health/ready
```

Open [http://127.0.0.1:3000](http://127.0.0.1:3000) in a browser on the same
machine. Do not expose either loopback listener through a tunnel or public
bind.

## 4. Verify audible playback and barge-in

Automated tests cannot hear the selected speaker, grant physical microphone
permission, or decide whether interruption feels correct. The operator owns
these physical acceptance claims. Record PASS/FAIL (without transcripts,
audio, or credentials) for each item:

- Select the intended browser/OS microphone and speaker, click **Start
  listening**, grant microphone access, and confirm the page reaches
  **Listening**. If the page offers **Enable audio**, select it and confirm the
  paused-audio message clears.
- Speak one normal turn. Confirm the page shows a finalized **You** turn, a
  finalized **Assistant** turn, and you physically hear the corresponding
  assistant response from the intended speaker. A transcript alone is not an
  audible-playback pass.
- While a second assistant response is physically audible, speak a new turn.
  Confirm the page changes to **You are speaking**, queued assistant playback
  stops, and superseded audio does not resume afterward. Only the operator may
  mark barge-in PASS.
- Select **Mute**, speak, and confirm no new voice frame/turn is accepted;
  select **Unmute** before continuing.
- Select the page's **Stop** control and confirm the page reports **Stopped**.
  This releases browser-owned microphone/audio/WebSocket resources but does
  not terminate the three local processes.

The content-free automated gate remains separate:

```zsh
make test-demo-live-client
```

## 5. Stop only the demo processes

Run these commands promptly in the same shell that captured the PIDs. They
signal only the BFF, gateway, and optional reference speech process started by
this guide, in dependency order:

```zsh
kill -INT "$BFF_PID"
wait "$BFF_PID" || true

kill -INT "$GATEWAY_PID"
wait "$GATEWAY_PID" || true

# Run these two lines only when Step 1 set SPEECH_PID.
kill -INT "$SPEECH_PID"
wait "$SPEECH_PID" || true

unset OPENAI_API_KEY S2S_API_KEY LIVE_STATIC_BEARER_TOKEN LIVE_DEMO_BEARER
```

Do not use a broad `pkill`, `killall`, Compose teardown, volume deletion, or a
database migration to stop this focused path. These commands do not stop an
unrelated worker or database and do not delete PostgreSQL or other durable
state. The temporary config and binaries remain in `DEMO_RUNTIME_DIR` for the
operator to inspect or remove separately.
