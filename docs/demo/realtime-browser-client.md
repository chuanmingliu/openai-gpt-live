# Realtime durable Web Search operator guide

For an interactive first run, use the [concise client quick
start](../../demo/live-client/README.md) and run `make demo-wizard`. Continue
with this guide when you need the complete manual acceptance, idle-pool,
privacy, or troubleshooting procedure.

This is the complete loopback `development_durable` path: speech-to-speech,
PostgreSQL schema v8, the durable gateway and worker, the browser BFF, and the
server-owned Web Search preset. Run commands from the repository root in one
`zsh` session. This mode is for development and acceptance only; it is not
production or release evidence.

## Browser control panel

Choose **Browser control panel** in `make demo-wizard` to explicitly enable the
loopback operator surface, then open
[http://127.0.0.1:3000/setup](http://127.0.0.1:3000/setup). Credentials remain
in the terminal or server environment. The page can operate only the fixed
services Speech, PostgreSQL, Gateway, Worker, and Client; it cannot submit a
command, path, URL, environment name, model, or credential. Worker is optional
for voice-only use and required for durable asynchronous tools. PostgreSQL
must be started and migrated before Gateway or Worker, and Gateway additionally
requires Speech. Stop dependencies in this order: Client, Worker, Gateway,
PostgreSQL, then Speech. Graceful shutdown stops only demonstrably owned
processes and does not delete the PostgreSQL volume.

The fixed local speech launcher reads
`/Users/chuanmingliu/speech-to-speech/.env.local` only after requiring private
mode `0600`, loads the reviewed profile beneath the fixed external trust root,
and creates a private ephemeral copy pinned to `127.0.0.1:8766`. The dedicated
non-secret control-panel gateway configuration reaches the same service at
`ws://host.docker.internal:8766/v1/realtime`; neither endpoint is browser
configurable. `/setup` is for local development only. It is not production
administration, a deployment controller, or release evidence.

The launcher executes the reviewed `/Users/chuanmingliu/speech-to-speech`
checkout at `d0270d79ccb6f5c99f76499bb1dbe8a65608cf51`, rather than a separate
mutable worktree. Its stdout/stderr are retained only in the operator terminal;
the API exposes fixed status codes, never child diagnostics. Client Stop closes
active browser voice WebSockets and rejects later `/ws` upgrades while keeping
`/setup` available so the operator can Start it again. During BFF shutdown,
every owned cleanup step has its own bounded
deadline and ownership proof remains until the wizard confirms process exit.

The speech service and worker both use `deepseek-v4-flash` with thinking
disabled. The worker enforces `{"thinking":{"type":"disabled"}}` on its
initial and tool-follow-up requests and discards provider reasoning content.
The separately supplied speech service must be configured for the same model
and thinking-disabled behavior before it is selected by `S2S_IMAGE` and
`S2S_CONFIG_PATH`. Speech and worker credentials may belong to the same
provider account, but `S2S_API_KEY` and `DEEPSEEK_API_KEY` remain separate
process secret references. Never copy either value between browser fields.

The browser controls only a bounded policy. It cannot choose a model,
destination, command, package, schema, environment variable, credential,
header, or executable. The first catalog contains one compiled, read-only
executor named `web_search`. raw `mcpServers` JSON is intentionally rejected.
Reviewed MCP adapters are future compiled, operator-owned catalog entries, not
runtime browser installs.

## 1. Configure the speech service and all secret references

Use an external secret manager in shared environments. For a disposable local
run, prompt without echoing values and generate independent HMAC/encryption
keys in memory. Do not use a tracked `.env` file, command-line secret argument,
URL credential, shell tracing, or support log.

```zsh
set +x

read -r -s "POSTGRES_PASSWORD?Disposable PostgreSQL password (hidden): "
export POSTGRES_PASSWORD
printf '\n'

read -r -s "LIVE_STATIC_BEARER_TOKEN?New local bearer (hidden): "
export LIVE_STATIC_BEARER_TOKEN
export LIVE_ADMIN_BEARER_TOKEN="$LIVE_STATIC_BEARER_TOKEN"
export LIVE_DEMO_BEARER="$LIVE_STATIC_BEARER_TOKEN"
printf '\n'

export LIVE_PRIVACY_HMAC_KEY="$(openssl rand -hex 32)"
export LIVE_RETENTION_HMAC_KEY="$(openssl rand -base64 32)"
export LIVE_RECONNECT_HMAC_KEY="$(openssl rand -base64 32)"
export LIVE_TOOL_ENCRYPTION_KEY="$(openssl rand -base64 32)"

read -r -s "S2S_API_KEY?Speech service bearer (hidden): "
export S2S_API_KEY
printf '\n'
read -r -s "DEEPSEEK_API_KEY?Worker DeepSeek bearer (hidden): "
export DEEPSEEK_API_KEY
printf '\n'

read -r "S2S_IMAGE?Reviewed speech image: "
export S2S_IMAGE
read -r "S2S_CONFIG_PATH?Absolute non-secret speech config path: "
export S2S_CONFIG_PATH

export LIVE_EXPECTED_CONFIG_HASH="$(
  go run ./cmd/live-gateway config-hash \
    --config deploy/config/development-durable.yaml
)"
```

The Make targets delegate to the fixed
`deploy/scripts/durable-compose.sh` wrapper. It requires `jq`, reads
`POSTGRES_PASSWORD` only from the environment, percent-encodes the password
through standard input, and exports one container-only `DATABASE_URL` only to
the Compose child process for migrate, gateway, and worker. Reserved URI
characters such as `@`, `:`, `/`, `?`, and `#` therefore remain password data.
Its host is the internal service name `postgres`; the database has no published
host port and this flow creates no host DSN. Do not invoke the wrapper under
shell tracing, run `docker compose config` in a shared terminal, or attach
resolved Compose output to a report because interpolation includes the encoded
password. The speech configuration selected above must serve
`ws://speech-to-speech:8000/v1/realtime`, select `deepseek-v4-flash`, disable
thinking, and provide a content-free pool-idle signal. Verify those external
facts from the reviewed speech-service configuration; this repository cannot
infer them from an image name.

## 2. Migrate PostgreSQL to schema v8

Run both migration and its clean-version check inside the Compose network:

```zsh
make durable-demo-migrate
```

The target starts only PostgreSQL and the one-shot embedded migrator, waits for
migration success, then runs that same image with `-check` on the private
Compose network. It must say `schema check passed at version 8` and prints no
DSN or credential. A dirty schema or any version other than 8 is a stop
condition. PostgreSQL remains internal and running for the next step.

## 3. Start the speech WebSocket

Validate the exact durable config before starting anything:

```zsh
go run ./cmd/live-gateway check-config \
  --config deploy/config/development-durable.yaml
go run ./cmd/live-worker check-config \
  --config deploy/config/development-durable.yaml
```

After the migration check passes, start the reviewed speech image and the two
application services:

```zsh
make durable-demo-up
```

Compose still gates gateway and worker on the successfully completed migrate
service. Treat a speech startup that does not confirm `deepseek-v4-flash` and
thinking disabled as a failure; do not fall back to a different model.

## 4. Start the durable gateway and worker and verify readiness

`make durable-demo-up` uses `deploy/config/development-durable.yaml`, where
handoffs and the immutable Web Search catalog are enabled. Verify content-free
readiness with bounded requests:

```zsh
wait_for_http() {
  local service_name="$1"
  local ready_url="$2"
  local attempt

  for attempt in {1..20}; do
    if curl --fail --silent --output /dev/null \
      --connect-timeout 0.05 --max-time 0.1 "$ready_url"; then
      printf '%s\n' "$service_name ready"
      return 0
    fi
    sleep 0.1
  done
  printf '%s\n' "$service_name readiness timed out" >&2
  return 1
}

wait_for_http gateway http://127.0.0.1:19090/health/ready
wait_for_http worker http://127.0.0.1:19091/health/ready
```

Every request uses a 0.05-second connect timeout and 0.1-second total timeout.
The configured worst case is
20 attempts x (0.1-second curl maximum + 0.1-second sleep) = 4.0 seconds.
Readiness is content-free and proves process composition only; it does not
prove a live speech turn, provider quota, Web Search egress, or audible output.

## 5. Start the browser BFF

The BFF reads `LIVE_DEMO_BEARER` from its process environment and injects it
only into the upstream gateway connection. Browser JavaScript never receives
the bearer.

```zsh
make demo-live-client &
BFF_PID=$!

wait_for_ready() {
  local service_name="$1"
  local owned_pid="$2"
  local ready_url="$3"
  local attempt

  for attempt in {1..20}; do
    if curl --fail --silent --output /dev/null \
      --connect-timeout 0.05 --max-time 0.1 "$ready_url"; then
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

wait_for_ready browser-bff "$BFF_PID" http://127.0.0.1:3000/health/ready
```

Open [http://127.0.0.1:3000/](http://127.0.0.1:3000/) on the same machine.
Do not expose the BFF, gateway, admin listeners, PostgreSQL, or speech service
through a tunnel or public bind.

## 6. Configure and test the Tools drawer

Before starting the voice session, expand **Tools** and review the server-owned
**Web Search** preset:

1. Turn on **Enable Web Search**.
2. Choose **SafeSearch**: `Strict` or `Moderate`. The durable catalog has
   `force_strict: true`, so the server may tighten Moderate to Strict.
3. Choose **Region**: Worldwide, United States, United Kingdom, Australia,
   Canada, or China · Chinese.
4. Set **Maximum results** from 1 through 10; the durable deployment maximum is
   5.
5. Set **Timeout (ms)** from 1000 through 10000; the durable deployment maximum
   is 8000.
6. Select **Test**. It always runs the fixed diagnostic query
   `DuckDuckGo privacy search`; browser-supplied query text is ignored.
7. Require **Web Search test passed.**, select **Save**, and require the
   `deepseek-v4-flash` acknowledgement before speaking.

The controls can only tighten the compiled catalog. A Test success checks the
bounded adapter path but does not save the session policy. Save sends the
closed-world policy to the gateway; it does not install or start an executor.

Click **Start listening**, grant microphone access, and speak a request such
as “Search the web for the current Go release notes and summarize the sources.”

## 7. Verify progress while continuing realtime speech

The task card must advance monotonically through the stable phases `queued`,
`searching`, `reading_sources`, `preparing_answer`, then one terminal state:
`completed`, `failed`, or `cancelled`. It may also show `needs_input`. There
are no percentages, chain of thought, provider reasoning, raw queries, tool
arguments, transport logs, or credentials in a progress card.

While the card is active, continue a normal voice conversation. Confirm speech
input, live transcription, and assistant audio continue independently. While
assistant audio is physically audible, speak again and require playback to
stop without superseded audio resuming. These are manual audible voice and
manual barge-in acceptance checks; automated tests cannot claim them.

On completion, the same card shows only the bounded final summary and approved
HTTPS source links. On failure or cancellation, realtime voice remains usable.
Disconnect/reconnect may restore a durable card; repeated events must not
duplicate work or regress its phase.

With DevTools open, complete the privacy inspection without pasting any secret:

- **Address bar**: URL is exactly the loopback origin/path with no token.
- **Sources**: loaded assets contain no `LIVE_STATIC_BEARER_TOKEN`,
  `LIVE_DEMO_BEARER`, `Authorization`, or bearer value.
- **Network**: `/ws` has no query token, cookie token, subprotocol token, or
  browser-visible `Authorization` header.
- **Application**: Local Storage, Session Storage, Cookies, IndexedDB, and
  Cache Storage contain no credential.
- **Console**: no credential, raw audio/transcript, Web Search query, tool
  argument, provider reasoning, or result body appears.

Do not paste the bearer value into DevTools. The demo does not persist raw
audio or transcripts. PostgreSQL persists only committed durable coordination,
policy, progress, reconnect, and bounded presentation records.

## 8. Stop and prove every pool is idle

Select the page **Stop** control first. It must report **Stopped**. Then check
the gateway and worker through authenticated, content-free admin endpoints:

```zsh
curl --fail --silent --show-error \
  --connect-timeout 0.05 --max-time 0.1 \
  --header "Authorization: Bearer $LIVE_STATIC_BEARER_TOKEN" \
  http://127.0.0.1:19090/admin/v1/sessions \
  | jq -e '.active == 0' >/dev/null

curl --fail --silent --show-error \
  --connect-timeout 0.05 --max-time 0.1 \
  --header "Authorization: Bearer $LIVE_ADMIN_BEARER_TOKEN" \
  http://127.0.0.1:19091/admin/v1/capacity \
  | jq -e '.Active == 0 or .active == 0' >/dev/null
```

For the reference speech service, require its reviewed content-free pool
endpoint to report no unit in use. A host-run reference service uses:

```zsh
curl --fail --silent --show-error \
  --connect-timeout 0.05 --max-time 0.1 \
  http://127.0.0.1:8765/v1/pool \
  | jq -e '.in_use == 0 and all(.units[]; .state == "idle")' >/dev/null
```

For a containerized or managed speech service, use its equivalent reviewed
content-free signal; never print a diagnostic body. After all three checks:

```zsh
kill -INT "$BFF_PID"
wait "$BFF_PID" || true
make durable-demo-down

unset POSTGRES_PASSWORD S2S_API_KEY DEEPSEEK_API_KEY
unset LIVE_STATIC_BEARER_TOKEN LIVE_ADMIN_BEARER_TOKEN LIVE_DEMO_BEARER
unset LIVE_PRIVACY_HMAC_KEY LIVE_RETENTION_HMAC_KEY
unset LIVE_RECONNECT_HMAC_KEY LIVE_TOOL_ENCRYPTION_KEY
unset LIVE_EXPECTED_CONFIG_HASH S2S_IMAGE S2S_CONFIG_PATH
```

`make durable-demo-down` does not use `--volumes`; the PostgreSQL volume is
retained for deliberate inspection or a later run. Do not use broad `pkill`,
`killall`, or volume deletion as part of this guide.

## 9. Troubleshoot without exposing content

| Symptom | Content-free check and action |
|---|---|
| PostgreSQL/migration failure | Rerun `make durable-demo-migrate` and require clean version 8. Inspect only container status/exit code; do not print the resolved Compose environment. Never run down migrations on a non-disposable database. |
| Worker not ready or no lease progress | Check `/health/ready`, then `/admin/v1/capacity`. Confirm the config hash, schema, worker-only `DEEPSEEK_API_KEY` reference, and that `heartbeat_interval` is below half the lease. Do not dump jobs. |
| Search Test or task times out | Keep the 1–10 second policy bound. Check approved DNS/HTTPS egress and the provider status class without logging query text, response HTML, tool arguments, or credentials. Voice should remain connected. |
| Browser has no audio | Confirm microphone/speaker selection, permission, **Enable audio**, 24 kHz PCM16, and the speech pool signal. A transcript is not an audible pass. |
| Policy rejected | Restore only known fields and allowed values, Test, then Save. Browser input cannot add tools, commands, destinations, headers, credentials, packages, models, or raw server definitions. |

Run the content-free automated browser gate separately:

```zsh
make test-demo-live-client
```

## Vertical profiles

`/setup` exposes profile administration only when the browser BFF runs with the
loopback profile flags:

```bash
go run ./demo/live-client \
  -operator \
  -vertical-profiles \
  -profile-tenant local \
  -profile-actor operator \
  -profile-database-url-env LIVE_PROFILE_DATABASE_URL
```

The database URL is resolved from that environment variable name inside the
process. It is never a flag value, never returned by an endpoint, and never
rendered in the browser. Without the flags every `/api/setup/v1/*` route answers
`404` and the demo keeps using the built-in voice profile.

### Trust boundary

The browser may submit only a profile ID, a description, logical `.j2` template
paths and their plain-text source, synthetic fixtures, an exact draft
generation, and a published version number. Tenant, actor, engine version,
adapter registry version, snapshot hashes, and publication timestamps are
server-owned. Adapter bindings fail closed because the loopback demo installs no
adapters, and raw `mcpServers` JSON is intentionally rejected here as well.

### Worker recipes

A worker recipe lets a profile run background work during a live turn. The
browser may submit only the recipe ID and its numeric guardrails — approval
requirement, execution, tool and overall timeouts, and the objective, evidence,
tool-result and final-result byte ceilings. Every value is bounded before it
reaches the domain: timeouts must be positive and at most 24 hours, byte limits
positive and at most 1 MiB.

The objective template is never browser-supplied. The service requires
`workers/<id>.j2` to already exist in the draft bundle, answers
`422 objective_template_missing` when it does not, and relinks the recipe's
objective to that logical name on every save. Saving bumps the recipe version
and the draft generation together, so a stale editor cannot silently rewrite a
recipe that changed elsewhere.

### Draft generations

Draft and template writes carry `If-Match: "<generation>"`. A missing header is
`428`; a losing write is `409 stale_draft`, leaves the stored draft untouched,
and keeps unsaved source in the editor until an explicit **Reload draft**.
Publication names one exact generation and can succeed only once.

### Enabling the feature

Two independent switches must agree. The gateway needs
`features.vertical_profiles.enabled: true` with bounded render workers and
queue, and it fails closed at startup if that is set without durable profile
storage. The loopback BFF needs `-vertical-profiles` plus the name of an
environment variable holding the profile database URL. The demo wizard's browser
control panel supplies the BFF half automatically when
`LIVE_PROFILE_DATABASE_URL` is exported before it runs, forwarding the value
through the child environment only. Neither half accepts a URL as a flag value.

### Selecting a profile for a session

The voice page lists the built-in profile plus every non-revoked published
version. The selection is locked from connect until disconnect. The browser
opens `/ws?profile=<id>&profile_version=<n>`; the BFF accepts only that exact
pair, adds its own fixed model, and forwards no other browser query key.

Microphone capture requires three acknowledgements: the provider
`session.updated`, the tool-policy acknowledgement, and — for a custom profile —
an exact `live.profile.selected` event whose ID, version, and snapshot hash match
the selection. A mismatch stops the session with `profile-mismatch` instead of
streaming audio, so stale or substituted profiles can never reach the microphone.

### Knowledge

Reference material is versioned and content addressed exactly like a profile.
The browser may submit a pack ID and one document; tenant, actor, version
number, and content hash are all server-owned.

Ingestion accepts UTF-8 text and Markdown only. The format gate checks the
extension *and* the bytes, so a PDF renamed to `.md` is rejected before any
decode — every binary container fails either the encoding check or the control
character check. Binary extraction is deliberately deferred; when it lands it
belongs in a separate short-lived process rather than in the BFF.

Preview and publish are two steps on purpose. Preview returns the exact excerpts
and their content hash; publish re-sends the document and the service re-derives
excerpts from those bytes, refusing unless they hash to the previewed value. An
operator therefore cannot publish material nobody reviewed, and a file that
changed between the two steps fails rather than slipping through.

Pinning is what connects the two halves. The browser names a pack and a version
number; the service resolves the content hash from the store and writes the
reference into the draft under the usual `If-Match` generation, so a stale
editor cannot pin against a draft that moved. One version per pack: pinning
again replaces rather than accumulates.

A profile pins knowledge by exact pack, version, and hash. Publication resolves
every reference and refuses tombstoned, duplicate, or unresolvable ones, so a
profile can never reference material the deployment cannot serve. Retiring a
version removes it from new selection while work already pinned to it keeps
reading exactly the bytes it was promised.

### Rollback

Publishing a new version does not change connected sessions; each session keeps
its immutable snapshot until it disconnects. **Activate** chooses the version
offered to new sessions, and **Revoke** removes a version from selection and
reconnect. Neither changes the immutable published content.

## Production roadmap

Production requires reviewed OIDC/BFF authentication, `wss://`, PostgreSQL TLS,
external secret management and rotation, immutable images/config hashes,
default-deny egress through the reviewed proxy, telemetry redaction, canary and
rollback evidence, capacity/chaos/soak gates, and operator-owned retention.
Promoting the loopback demo is forbidden. Future MCP-backed tools require a
compiled typed adapter, fixed destination/transport, bounded schema and output,
idempotency and approval policy where relevant, privacy tests, static catalog
registration, and deployment review before a browser preset can expose them.
