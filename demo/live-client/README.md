# Realtime voice demo — quick start

Run everything from the repository root:

```sh
make demo-wizard
```

The interactive menu offers:

1. **Full durable demo** — starts private PostgreSQL, speech, gateway, worker,
   Web Search, and the browser client.
2. **Client only** — starts the browser client for an existing loopback
   gateway.
3. **Check status** — checks browser, gateway, and worker readiness.
4. **Stop demo** — stops the wizard-owned browser and, when authorized, the
   durable Compose services without deleting the PostgreSQL volume.
5. **Browser control panel** — explicitly starts the local-only BFF operator
   surface at **[http://127.0.0.1:3000/setup](http://127.0.0.1:3000/setup)**;
   services remain stopped until you choose a fixed action in the page.

The Browser control panel keeps every credential in the terminal or server
environment. The browser can control only the fixed services Speech,
PostgreSQL, Gateway, Worker, and Client; it cannot choose a command, path,
URL, model, environment name, or credential. Worker is optional for
voice-only use and is required for durable asynchronous tools. Stop owned
dependencies in this order: Client, Worker, Gateway, PostgreSQL, then Speech.
Shutdown does not delete the PostgreSQL volume. `/setup` is for local
development only, not production administration or release evidence.

## Recommended first run

Choose **Full durable demo**. The wizard asks for:

- hidden: a disposable PostgreSQL password, a new local gateway bearer, the
  speech-service credential, and the DeepSeek credential;
- visible: the reviewed speech image and the absolute path to its non-secret
  configuration file.

Secret values exist only in the current wizard session and the child services
it starts. The wizard does not write `.env` or credential files.

When startup succeeds, open:

**[http://127.0.0.1:3000/](http://127.0.0.1:3000/)**

Grant browser **microphone and speaker** permission, select **Start
listening**, and speak. For Web Search, open **Tools**, enable Web Search, run
**Test**, then **Save** before starting the voice session.

Run `make demo-wizard` again and choose **Check status** or **Stop demo**.

## Client-only shortcut

Choose **Client only** when the gateway is already running. Enter its local
bearer, loopback WebSocket URL, and advertised model. Defaults are:

```text
ws://127.0.0.1:18080/v1/realtime
gpt-live-simulator
```

The browser never receives the bearer; the loopback BFF injects it into the
gateway connection.

## Vertical profiles in /setup

`/setup` has five tabs. **Runtime** keeps the existing service controls.
**Profile**, **Prompts**, **Workers**, and **Review & publish** appear when the
BFF runs with loopback profile administration:

```bash
go run ./demo/live-client \
  -operator \
  -vertical-profiles \
  -profile-tenant local \
  -profile-actor operator \
  -profile-database-url-env LIVE_PROFILE_DATABASE_URL
```

`-profile-database-url-env` names an environment variable; the URL value itself
is never accepted on the command line, returned by any endpoint, or rendered in
the browser. Without these flags every `/api/setup/v1/*` route answers `404` and
the demo keeps using the built-in voice profile.

Typical flow:

1. **Profile** — enter a profile ID and select **Open or create draft**. A new
   draft is seeded with a valid `system.j2` and one synthetic fixture, so it is
   publishable immediately.
2. **Prompts** — open `system.j2`, edit the source, and **Save template**. Add
   `opening_message.j2` or `turn_instruction.j2` with **Add template**. Paths are
   logical bundle names, never operator filesystem paths.
3. **Workers** — a worker recipe runs background work while the voice turn
   keeps talking, so the assistant never narrates its reasoning and mentions a
   result only once it is back. Add the objective template `workers/<id>.j2` on
   the Prompts tab first, then set the recipe's timeouts and payload limits and
   **Save recipe**. The service links the objective itself; the browser never
   supplies a template reference, a command, or an adapter field.
4. **Review & publish** — **Validate**, **Preview** the rendered fixtures, then
   **Publish and activate**. Published versions are immutable; use **Activate**
   and **Revoke** to control which version new sessions may select.

Draft writes carry the exact `If-Match` generation. A concurrent change returns
`409 stale_draft`, keeps your unsaved source in the editor, and requires an
explicit **Reload draft** before saving again.

### Starting from the demo wizard

`make demo-wizard` → **Browser control panel** enables the profile tabs when
`LIVE_PROFILE_DATABASE_URL` is already exported in that shell. The wizard passes
the variable *name* to the BFF and forwards the value only through the child
process environment; it never constructs, prints, or stores the URL. Without
that variable the wizard starts the control panel with profiles off and says so.
The durable PostgreSQL service is deliberately not published to the host, so
point the variable at a database you can reach from this machine.

The gateway must also have `features.vertical_profiles.enabled: true`, which
`deploy/config/development-control-panel.yaml` now sets. A gateway configured
for profiles without durable profile storage refuses to start, and a session may
never select a profile the deployment did not authorize.

On the voice page, **Vertical profile** lists the built-in profile plus every
non-revoked published version. The choice is locked once a session starts. The
browser sends only `profile` and `profile_version`; the BFF validates that exact
pair, adds its own model, and forwards nothing else. Microphone capture waits for
the gateway's exact `live.profile.selected` acknowledgement in addition to the
existing provider and tool-policy acknowledgements, and a mismatched
acknowledgement stops the session instead of streaming audio.

## Requirements and help

Full mode requires Go, Docker Compose, `curl`, `jq`, `openssl`, a reviewed
speech-service image/configuration, and valid speech/DeepSeek credentials.
Client-only mode requires Go, `curl`, and an already-running loopback gateway.
Control-panel mode additionally uses the fixed external speech installation
at `ws://127.0.0.1:8766/v1/realtime`. Its private `.env.local` must be mode
`0600`; the checked-in launcher creates and removes a private non-secret
runtime profile pinned to loopback port 8766. The reviewed source is the fixed
`/Users/chuanmingliu/speech-to-speech` checkout (revision
`d0270d79ccb6f5c99f76499bb1dbe8a65608cf51`); its launcher diagnostics remain
in the terminal and are never returned by `/setup`.

For manual audio/barge-in acceptance, idle-pool checks, troubleshooting, and
security details, use the
[complete realtime browser operator guide](../../docs/demo/realtime-browser-client.md).

This is a loopback development demo, not production or release evidence.
