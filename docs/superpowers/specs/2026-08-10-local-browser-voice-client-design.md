# Local Browser Voice Client Design

## Goal

Provide a local browser demo for the running GPT Live service with hands-free
microphone streaming, server-side voice activity detection, interruption of
assistant playback when the user speaks, and live user/assistant transcripts.
The demo must traverse the GPT Live gateway and the configured
Tencent/DeepSeek/MiniMax speech service rather than connecting directly to the
speech provider.

## Scope

The demo is a development tool, not a production authentication solution. It
binds only to loopback, supports one browser session per WebSocket, and targets
the configured local gateway model. It does not add cookie authentication,
query-token authentication, a production browser protocol, account management,
conversation persistence controls, or deployment manifests.

## Architecture

The demo consists of a small Go server and static browser assets under
`demo/live-client`.

The Go server:

- listens on `127.0.0.1:3000` by default;
- serves the embedded HTML, JavaScript, CSS, and AudioWorklet assets;
- accepts a same-origin browser WebSocket at `/ws`;
- opens the upstream GPT Live WebSocket using a configured gateway URL and
  model;
- injects the local bearer from a process environment variable into the
  upstream `Authorization` header;
- relays bounded text frames in both directions without parsing or logging
  media, transcripts, tokens, or credentials; and
- propagates normal close status while failing closed on oversized, binary, or
  malformed transport usage.

The browser never receives the bearer and never places credentials in a URL,
cookie, local storage, session storage, or WebSocket subprotocol.

## Browser Interaction

The page presents:

- a start/stop control;
- a microphone mute control;
- connection, listening, user-speaking, assistant-thinking, and
  assistant-speaking states;
- a scrolling transcript with separate user and assistant turns; and
- concise permission, connection, and protocol error messages.

After the user presses Start, the browser requests microphone permission,
creates an `AudioContext`, loads an `AudioWorklet`, and connects to `/ws`. It
waits for `session.created`, then sends one `session.update` enabling server VAD
and interruption. It does not trigger generation independently: the speech
service creates responses from completed VAD turns.

## Audio Flow

The capture worklet converts browser float samples into 24 kHz mono PCM16. It
uses a stateful resampler so source rates such as 44.1 kHz and 48 kHz do not
reset at chunk boundaries. The main thread batches exactly 20 ms / 960-byte
frames, base64-encodes them, and sends `input_audio_buffer.append` events.

The browser decodes `response.output_audio.delta` into PCM16 and schedules it
through one playback timeline. On
`input_audio_buffer.speech_started`, it immediately clears scheduled playback
and changes the state to user-speaking. The provider remains the authority for
response cancellation and emits the matching terminal response event; the
browser does not invent response IDs or epochs.

Audio buffers and decoded transcript strings exist only in browser memory for
the active page. The demo writes neither to disk.

## Transcript Flow

User text is assembled from
`conversation.item.input_audio_transcription.delta` and finalized by
`conversation.item.input_audio_transcription.completed`. Assistant text is
assembled from supported response transcript/text delta events and finalized by
their matching done events. Duplicate terminal events do not duplicate turns.
The UI renders text through DOM `textContent`, never HTML interpolation.

## Security and Bounds

- Both HTTP and WebSocket listeners bind to loopback only.
- The server accepts only the exact configured HTTP origin.
- Static responses use a restrictive Content Security Policy and
  `nosniff`/no-store headers.
- Browser-to-BFF and upstream-to-BFF frames have explicit byte limits.
- Only text WebSocket messages are accepted; audio remains base64 inside the
  validated JSON protocol.
- The bearer is required through a named process environment variable and is
  cleared from owned startup state after the upstream dial configuration is
  built where practical.
- Logs contain bounded event classes and lifecycle state only. They exclude
  WebSocket payloads, transcripts, audio, provider responses, headers, and
  secrets.
- Stop closes the microphone tracks, AudioWorklet, browser socket, upstream
  socket, and AudioContext.

## Configuration

Command flags configure only non-secret values:

- listen address, default `127.0.0.1:3000`;
- upstream gateway base WebSocket URL, default
  `ws://127.0.0.1:18080/v1/realtime`;
- advertised gateway model, default `gpt-live-real`;
- accepted browser origin, default `http://127.0.0.1:3000`; and
- bearer environment-variable name, default `LIVE_DEMO_BEARER`.

Startup rejects non-loopback listen addresses, non-HTTP browser origins,
upstream URLs containing credentials/query/fragment, empty or unsafe model
names, missing bearer values, and mismatched origin/listen configuration.

## Error Handling

The server returns content-free HTTP errors and WebSocket close reasons. A
failed upstream dial closes the browser socket without exposing the provider or
gateway response body. The browser stops capture and playback after terminal
transport errors but preserves the visible status so the user can retry.
Microphone denial is handled before WebSocket startup and leaves no active
resources.

## Verification

Go tests cover:

- strict configuration validation;
- exact authorization-header injection into a fake upstream;
- same-origin acceptance and hostile-origin rejection;
- bidirectional text relay and close propagation;
- binary/oversized-frame rejection;
- static asset, CSP, no-store, and content-type headers; and
- bearer absence without secret-bearing output.

JavaScript tests cover the stateful PCM resampler, 960-byte framing, transcript
assembly, duplicate terminal handling, and playback reset on speech start.

The final local smoke uses a browser against `127.0.0.1:3000` and verifies:

1. microphone permission and session connection;
2. a spoken turn produces a finalized user transcript;
3. the real provider produces assistant transcript and audible playback;
4. speaking during playback clears queued audio and produces a fresh turn; and
5. Stop returns the provider pool to idle and leaves no microphone track live.

