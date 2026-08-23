# Local Voice Test UI Design

## Goal

Add a development-only browser UI that exercises the credential-free GPT Live
gateway through its public Realtime WebSocket boundary. A developer can start
the gateway, idle worker, optional local speech service, and UI; grant microphone
permission; speak; hear assistant audio; and inspect live transcripts and
connection state.

The production gateway and worker remain backend-only. The UI is a separate
loopback command and is never mounted into either service.

## Scope

The first version provides:

- Start and Stop controls;
- microphone mute/unmute;
- connection, listening, user-speaking, assistant-thinking, and
  assistant-speaking states;
- live user and assistant transcripts;
- realtime assistant audio playback;
- playback interruption when server VAD reports new user speech;
- a concise bounded event/status panel for diagnosing connection and protocol
  failures; and
- capability discovery from the running gateway.

It does not provide accounts, production authentication, conversation history,
deployment manifests, persistent browser storage, arbitrary event injection, or
direct browser-to-provider access.

## Chosen Architecture

Create `demo/live-client`, a small Go browser-facing backend (BFF) with embedded
dependency-free browser assets.

The BFF binds `127.0.0.1:3000`, serves the UI, exposes a same-origin `/ws`
WebSocket, and relays bounded text frames to
`ws://127.0.0.1:8080/v1/realtime?model=local`. It also exposes a same-origin
`/api/capabilities` endpoint that fetches the gateway capability document.

This is preferred over a direct static client because the gateway's browser
origin and authentication policies remain server-owned. It is preferred over
serving UI assets from `live-gateway` because the gateway and worker must remain
pure backend services.

## Process Topology

The local topology is:

1. `live-gateway` on `127.0.0.1:8080`;
2. optional `live-worker --backend none` diagnostics on `127.0.0.1:9091`;
3. optional Hugging Face-compatible speech service on
   `127.0.0.1:8765/v1/realtime`; and
4. `demo/live-client` on `127.0.0.1:3000`.

The browser reaches only the demo BFF. The BFF reaches only the configured GPT
Live gateway. The gateway remains the only component that reaches the speech
provider.

The UI can load and display gateway capabilities when the speech service is
absent. Opening a realtime session then fails with a sanitized connection error,
because the gateway deliberately defers its provider dial until session start.

## Configuration

Command flags configure non-secret values:

- `-listen`, default `127.0.0.1:3000`;
- `-gateway-url`, default `ws://127.0.0.1:8080/v1/realtime`;
- `-model`, default `local`;
- `-origin`, default `http://127.0.0.1:3000`;
- `-bearer-env`, empty by default; and
- `-max-frame-bytes`, bounded by the gateway capability limit.

Credential-free startup performs no credential lookup. If `-bearer-env` is
explicitly supplied for a configured local gateway, the BFF requires that
environment variable and injects its value only into the upstream
`Authorization` header. The value never reaches browser JavaScript, URLs,
cookies, storage, responses, or logs.

Startup rejects non-loopback listeners, mismatched origins, unsafe gateway URLs,
userinfo/query/fragment in the configured base URL, unsafe model names, and
unbounded frame limits.

## HTTP and WebSocket Boundary

The BFF serves only these routes:

- `GET /` and explicit embedded asset paths;
- `GET /health/live`, returning `ok\n`;
- `GET /api/capabilities`, returning the validated upstream JSON document; and
- `GET /ws`, accepting an exact-origin WebSocket upgrade.

All other routes return 404. Static and API responses carry `no-store`,
`nosniff`, `Referrer-Policy: no-referrer`, and a restrictive Content Security
Policy. WebSocket upgrades with missing or foreign browser origins fail before
an upstream dial.

The proxy accepts text WebSocket messages only, applies an explicit read limit
in both directions, and never parses or logs audio, transcript, or credential
payloads. It appends exactly one `model` query parameter to a copied validated
upstream URL. Either relay direction ending cancels and closes both sockets with
a bounded public close class; upstream bodies and raw errors are not exposed.

## Browser Audio Flow

After Start:

1. fetch and validate capabilities;
2. request microphone permission;
3. create an `AudioContext` and capture `AudioWorklet`;
4. open the same-origin `/ws` connection;
5. wait for `session.created`;
6. send one supported `session.update` selecting 24 kHz PCM input/output and
   `{"type":"server_vad"}` turn detection; and
7. begin streaming exact `input_audio_buffer.append` events.

Capture converts browser float samples into 24 kHz mono signed PCM16 using a
stateful streaming resampler. It batches exact 20 ms, 960-byte frames and sends
them as base64 JSON. The framing layer retains only a bounded sub-frame suffix.

Assistant `response.output_audio.delta` payloads are decoded into PCM16 and
queued on one playback timeline. On `input_audio_buffer.speech_started`, the UI
immediately clears queued playback and shows the user-speaking state. The server
and provider remain authoritative for response cancellation, response IDs, and
epochs.

Mute stops sending new frames without destroying the session. Stop closes media
tracks, worklets, sockets, playback queues, and the audio context.

## Transcript and State Flow

User transcript deltas and completed events are correlated by item ID and
content index. Assistant transcript deltas and terminal events are correlated
by response/item identity available in the supported protocol. Duplicate
terminal events do not duplicate visible turns.

Transcript text is rendered only through DOM `textContent`. Audio, transcript,
and event data remain in memory for the active page and are cleared on reload or
Stop. The status panel contains only bounded event classes and sanitized error
categories, never raw payloads.

The reducer owns deterministic state transitions so protocol behavior can be
tested independently from the DOM and Web Audio APIs.

## User Interface

Use one responsive page with:

- a prominent Start/Stop control;
- a mute control enabled only during an active session;
- a compact connection/state badge;
- separate user and assistant transcript cards;
- a small gateway capability summary; and
- a collapsible bounded event/status panel.

Keyboard focus, visible labels, color contrast, reduced-motion behavior, and
screen-reader status announcements are required. The UI should remain useful at
mobile and desktop widths without introducing a frontend build tool.

## Security and Privacy

- Every listener is loopback-only.
- The browser is admitted only from the exact configured origin.
- No credential is required or looked up by default.
- Optional gateway credentials remain in the BFF process only.
- No audio, transcript, event payload, header, credential, or provider body is
  logged or persisted.
- The UI uses no local storage, session storage, cookies, analytics, CDN assets,
  or external scripts.
- The BFF never connects directly to a speech provider.
- The demo is explicitly local development tooling and is not a production auth
  proxy.

## Error Handling

Microphone denial leaves no socket or audio context running. Capability,
gateway, and provider failures produce distinct sanitized UI categories with a
retry action. Malformed, binary, or oversized frames close the affected session.
Unexpected upstream termination stops capture and playback but preserves the
visible status until the user retries.

The Go process treats context cancellation and normal HTTP shutdown as success,
uses a bounded shutdown timeout, and returns sanitized startup errors.

## File Boundaries

- `demo/live-client/main.go`: flags, optional secret lookup, lifecycle.
- `demo/live-client/config.go`: strict configuration validation.
- `demo/live-client/server.go`: embedded assets, headers, routes, capabilities.
- `demo/live-client/proxy.go`: upstream dial and bounded relay.
- `demo/live-client/web/index.html`: accessible document structure.
- `demo/live-client/web/styles.css`: responsive presentation.
- `demo/live-client/web/audio-core.mjs`: resampling, PCM conversion, framing.
- `demo/live-client/web/protocol-state.mjs`: event classification and reducer.
- `demo/live-client/web/capture-worklet.js`: microphone processing.
- `demo/live-client/web/playback-worklet.js`: bounded playback queue.
- `demo/live-client/web/app.js`: DOM, browser resources, and orchestration.
- adjacent Go and Node test files: unit and boundary verification.
- `README.md`: exact startup, test, security, and stop instructions.

No gateway, worker, deployment, migration, or production configuration file is
modified to host the UI.

## Verification

Go tests cover configuration, zero default credential lookup, optional bearer
injection, exact-origin rejection, restrictive headers, capability proxying,
bounded text relay, binary/oversized rejection, close propagation, and graceful
shutdown.

Node's built-in test runner covers stateful resampling, PCM saturation, exact
960-byte framing, bounded base64 decoding, transcript assembly, terminal dedupe,
and playback-clear effects. No npm dependency is added.

The final browser smoke starts the credential-free gateway and UI in empty
environments, verifies capabilities, opens the page, confirms a session failure
is sanitized when speech is absent, then repeats with a compatible local speech
service to validate microphone capture, transcripts, audible playback, barge-in,
mute, Stop, and resource cleanup.

Repository verification includes focused Go/Node tests, targeted vet/build,
static security assertions, browser console/network inspection, and preservation
of all pre-existing user-owned dirty files.
