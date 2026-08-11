# Realtime Browser Client Hardening Design

## Goal

Harden the existing loopback browser voice demo so it behaves as a genuine
realtime client: microphone capture, server-VAD turn detection, incremental
transcripts, audible assistant playback, and barge-in must remain responsive
under normal provider burstiness, browser scheduling delays, reconnects, and
longer conversations.

This pass covers realtime voice only. Durable PostgreSQL/DeepSeek handoff UX is
a later phase and must not add latency or state coupling to the voice path.

## Realtime Invariants

- Capture emits 24 kHz mono PCM16LE in exact 20 ms / 960-byte frames.
- Capture and playback AudioWorklet callbacks do no blocking I/O and avoid
  per-quantum allocation where practical.
- Every queue is bounded. Overflow terminates the session safely instead of
  increasing latency without limit.
- Browser playback keeps only 100--250 ms of scheduled look-ahead. A larger
  bounded reservoir may absorb provider bursts, but it is fed into playback in
  realtime rather than scheduled all at once.
- `input_audio_buffer.speech_started` clears scheduled and reservoir audio
  before the UI reports user speech.
- Cancelled-response audio cannot become audible after a barge-in generation
  changes.
- Transcript work and DOM rendering never block the audio worklet path.
- The UI reports audible playback only after the playback worklet confirms that
  samples are being consumed.
- When a realtime guarantee cannot be maintained, the client fails closed with
  bounded, actionable status and releases its resources.

## Trust Boundary

The development BFF continues to bind to a literal loopback address and serves
one browser WebSocket per session. For this demo, the configured gateway must
also be a literal loopback address. The BFF rejects remote `ws://` and `wss://`
gateway hosts rather than forwarding the local bearer or microphone traffic to
an unintended endpoint.

The browser receives no gateway bearer. It stores no bearer in source, URLs,
cookies, WebSocket subprotocols, local storage, session storage, or responses.
The exact Origin gate remains a browser cross-origin defense, not local-process
authentication; documentation states that the development host is trusted.

The page must say that the HTTP/BFF boundary is loopback-only while microphone
audio is forwarded through the configured gateway to configured providers. It
must distinguish in-memory/no-demo-persistence behavior from network locality.

## Browser Architecture

### Session controller

Extract browser lifecycle and rendering decisions from DOM event handlers into
a dependency-free session controller. The controller owns:

- connection and audio state transitions;
- generation changes across start, stop, failure, and retry;
- unfinished user/assistant transcript entries;
- mute state and accessibility state;
- playback buffering versus confirmed playback; and
- bounded visible transcript history.

The controller emits small effects consumed by `app.js`. Audio payload bytes do
not enter controller history.

### Capture path

The capture worklet owns the stateful source-rate-to-24-kHz resampler and exact
960-byte framing. Its hot path reuses staging buffers and carries fractional
resampler position across arbitrary browser quantum boundaries. Tests cover
44.1 kHz and 48 kHz sources, irregular chunking, long-run output counts, reset,
and exact little-endian frames.

The main thread performs WebSocket serialization only after a complete frame is
received. Muting resets partial capture state and exposes `aria-pressed=true`.

### Playback path

The main thread decodes bounded PCM deltas into a bounded reservoir. A single
timer feeds no more than 250 ms ahead to the playback worklet. The worklet uses
a cursor/ring-style queue and writes into the browser-provided output channel;
it does not allocate a fresh output array per render quantum.

The worklet reports `playing` only after it writes at least one non-empty output
quantum and reports `idle` after the queue drains. Incoming bytes put the UI in
`assistant-buffering`; only `playing` changes it to `assistant-speaking`.
Suspended audio contexts produce an actionable `audio-suspended` state with an
explicit user-gesture resume action.

Both the reservoir and worklet queue carry the current playback generation.
Clear increments the generation and discards queued messages from older
generations even if they arrive after the clear message.

### Transcript path

Protocol state keeps bounded per-event assembly. Visual deltas are coalesced at
animation-frame cadence instead of forcing layout for every provider event.
Stop, failure, and new start finalize or discard unfinished view entries so the
next session cannot append to an earlier partial turn.

The DOM retains a bounded number of finalized turns. Visual streaming remains
available, while a separate polite live region announces only finalized turns
to assistive technology.

## Provider Pacing and Barge-In

The speech service must not implement realtime pacing by sleeping inside the
single coroutine that also processes VAD and cancellation events. The current
experimental `WebSocketTransport.send_audio_chunk` sleep is replaced by a
cancellable, generation-aware outbound scheduler or removed when browser pacing
is authoritative.

If service-side pacing remains:

- VAD/control events are processed independently from audio pacing;
- each queued audio unit carries its response generation;
- discard/cancel advances the generation and wakes the scheduler;
- a sleeper checks generation again before sending; and
- close cancels the scheduler and leaves no pending audio task.

The maximum added barge-in delay from the pacing layer is one scheduling tick,
not an audio batch duration.

## Interaction and Accessibility

- Start, Mute, and Stop controls appear before the transcript and remain visible
  on common mobile and laptop viewports.
- Recovery messages describe the enabled action, for example, “Select Start
  listening to reconnect,” rather than instructing the user to select disabled
  Stop.
- Muted, buffering, speaking, suspended, reconnecting, and failed states are
  distinct and truthful.
- Mute exposes `aria-pressed`; finalized transcript announcements are atomic;
  focus-visible controls and reduced-motion behavior remain.
- Normal text meets WCAG AA contrast and size expectations.
- The UI offers a bounded speaker check/resume action without generating a
  provider response or retaining test audio.

## Bounds

- Browser/BFF WebSocket text frame: 64 KiB.
- Capture frame: exactly 960 bytes.
- Decoded provider audio delta: at most 64 KiB.
- Playback reservoir: bounded duration, with 100--250 ms scheduled look-ahead.
- Playback worklet queue: bounded independently from the reservoir.
- Protocol identifier maps: bounded.
- Visible transcript turns: fixed maximum; older finalized nodes are removed.
- Transcript text: existing 64 KiB per bounded assembly.

All limit failures are content-free and terminate or reset the affected session
without logging audio, transcripts, headers, provider bodies, or credentials.

## Verification

### Automated

- Go configuration tests reject every non-loopback gateway authority.
- Proxy tests exercise text, binary, oversize, and close behavior in both
  directions without exposing the bearer.
- Pure JavaScript tests cover controller state, retry isolation, mute,
  buffering-to-playing acknowledgement, bounded transcript retention, playback
  generation fencing, long 44.1/48 kHz resampling, and overflow.
- Browser integration tests exercise Start/Stop lifecycle with injected media,
  WebSocket, AudioContext, and worklet boundaries; they do not access a physical
  microphone in CI.
- Python tests reproduce cancellation during an in-flight pacing wait and prove
  that stale audio is not sent and VAD/control processing remains prompt.
- Focused race tests, the repository race suite, vet, build, tidy-diff, static
  deployment checks, formatting, and diff checks remain green.

### Manual acceptance

Against the configured real local gateway and speech service, the operator:

1. grants microphone permission and reaches Listening;
2. speaks a bounded non-sensitive phrase and sees a finalized user transcript;
3. sees the assistant transcript and hears assistant audio;
4. speaks over playback and confirms audio stops immediately before the new
   turn;
5. confirms the UI distinguishes buffering from actual speaking;
6. mutes/unmutes and verifies capture state;
7. stops and confirms both AudioContexts, microphone tracks, sockets, and the
   provider pool are idle; and
8. confirms no bearer appears in page source, URLs, storage, responses, or
   browser-visible logs.

Physical speaker output and acoustic barge-in require the operator; automation
must not claim those observations.

## Operator Configuration Boundary

Implementation and tests use no real credentials. At final handoff, the operator
receives step-by-step instructions for only the values that require human or
provider authority:

- speech-service WebSocket URL, model, and API key;
- local gateway static bearer used only by the BFF process;
- DeepSeek and PostgreSQL values for the later durable-handoff phase;
- browser microphone and speaker permission/device selection; and
- the manual audible-playback/barge-in acceptance checklist.

No configuration step writes secrets into tracked files or browser storage.

## Out of Scope for This Pass

- PostgreSQL/DeepSeek handoff presentation, approvals, or task history;
- production browser OIDC/BFF authentication;
- multi-account product UX;
- native mobile packaging;
- provider selection from the browser; and
- deployment-manifest changes for this development-only BFF.

