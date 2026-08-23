# Local Voice Test UI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a loopback-only browser UI that tests the credential-free GPT Live gateway with microphone audio, playback, transcripts, barge-in, capabilities, and sanitized status.

**Architecture:** A separate Go BFF under `demo/live-client` embeds dependency-free browser assets, serves them at `127.0.0.1:3000`, proxies capabilities over HTTP, and relays a same-origin browser WebSocket to the gateway. Browser modules own audio conversion and protocol state; neither `live-gateway` nor `live-worker` serves frontend assets.

**Tech Stack:** Go 1.26, `github.com/coder/websocket` v1.8.15, `golang.org/x/sync/errgroup`, standard `embed`/`net/http`, browser Web Audio/AudioWorklet APIs, ECMAScript modules, Node 22 built-in tests.

**Spec:** `docs/superpowers/specs/2026-08-23-local-voice-test-ui-design.md`

## Global Constraints

- Keep `live-gateway` and `live-worker` backend-only; UI files live only under `demo/live-client`.
- Bind the demo to `127.0.0.1:3000` by default and reject non-loopback listeners.
- Connect only to the GPT Live gateway, default `ws://127.0.0.1:8080/v1/realtime?model=local`; never connect the BFF or browser directly to the speech provider.
- Perform zero credential lookup by default. `-bearer-env` is optional and, when set, is injected only in upstream HTTP/WebSocket headers.
- Accept exactly `http://127.0.0.1:3000` by default and reject missing or foreign WebSocket origins before dialing upstream.
- Permit text WebSocket frames only and bound frames in both directions.
- Capture/play `pcm_s16le`, 24,000 Hz, mono, two bytes/sample, exact 960-byte/20 ms input frames.
- Use provider/server VAD as turn authority and clear queued playback on `input_audio_buffer.speech_started`.
- Keep audio, transcripts, event payloads, provider bodies, headers, and credentials out of logs, persistence, browser storage, and test output.
- Use no npm dependency, CDN asset, analytics, cookie, local storage, session storage, deployment manifest, migration, or production configuration change.
- Preserve all pre-existing modified and untracked user files; stage only task-owned files.

---

## File Structure

- `demo/live-client/config.go` — flags, URL/origin/model/frame validation, optional bearer lookup.
- `demo/live-client/config_test.go` — credential-free defaults and unsafe-input tests.
- `demo/live-client/main.go` — process entry point and signal lifecycle.
- `demo/live-client/server.go` — embedded assets, HTTP routes, headers, capabilities proxy, shutdown.
- `demo/live-client/server_test.go` — route, security-header, capabilities, and lifecycle tests.
- `demo/live-client/proxy.go` — upstream URL/header construction and bounded bidirectional relay.
- `demo/live-client/proxy_test.go` — exact origin, optional bearer, relay, frame, and close tests.
- `demo/live-client/web/index.html` — accessible page structure.
- `demo/live-client/web/styles.css` — responsive presentation.
- `demo/live-client/web/audio-core.mjs` — resampling, PCM conversion, framing, and decoding.
- `demo/live-client/web/audio-core.test.mjs` — pure audio primitive tests.
- `demo/live-client/web/protocol-state.mjs` — event reducer and bounded status model.
- `demo/live-client/web/protocol-state.test.mjs` — transcript, dedupe, and barge-in tests.
- `demo/live-client/web/capture-worklet.js` — microphone capture and exact-frame delivery.
- `demo/live-client/web/playback-worklet.js` — bounded assistant-audio playback queue.
- `demo/live-client/web/app.js` — DOM, WebSocket, microphone, playback, and teardown orchestration.
- `Makefile` — demo run/test targets.
- `README.md` — exact browser-demo quickstart and boundary.

---

### Task 1: Credential-free Demo Configuration and Process Shell

**Files:**
- Create: `demo/live-client/config.go`
- Create: `demo/live-client/config_test.go`
- Create: `demo/live-client/main.go`

**Interfaces:**
- Produces: `type demoConfig struct { ListenAddr, GatewayURL, Model, Origin, BearerEnv string; MaxFrameBytes int64 }`.
- Produces: `parseConfig(args []string) (demoConfig, error)`.
- Produces: `resolveBearer(cfg demoConfig, lookup func(string) (string, bool)) ([]byte, error)`.
- Produces: `(demoConfig).upstreamWebSocketURL() (*url.URL, error)` and `(demoConfig).capabilitiesURL() (*url.URL, error)` for Task 2.
- Task 2 replaces the temporary `run` stub with the HTTP runtime.

- [ ] **Step 1: Write failing default and secret-boundary tests**

```go
func TestParseConfigUsesCredentialFreeLoopbackDefaults(t *testing.T) {
	cfg, err := parseConfig(nil)
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:3000", cfg.ListenAddr)
	assert.Equal(t, "ws://127.0.0.1:8080/v1/realtime", cfg.GatewayURL)
	assert.Equal(t, "local", cfg.Model)
	assert.Equal(t, "http://127.0.0.1:3000", cfg.Origin)
	assert.Empty(t, cfg.BearerEnv)
	assert.EqualValues(t, 1<<20, cfg.MaxFrameBytes)
}

func TestResolveBearerDoesNotLookupByDefault(t *testing.T) {
	cfg, err := parseConfig(nil)
	require.NoError(t, err)
	bearer, err := resolveBearer(cfg, func(name string) (string, bool) {
		t.Fatalf("unexpected credential lookup %q", name)
		return "", false
	})
	require.NoError(t, err)
	assert.Empty(t, bearer)
}

func TestResolveBearerRequiresExplicitReference(t *testing.T) {
	cfg, err := parseConfig([]string{"-bearer-env", "LIVE_DEMO_BEARER"})
	require.NoError(t, err)
	_, err = resolveBearer(cfg, func(name string) (string, bool) {
		assert.Equal(t, "LIVE_DEMO_BEARER", name)
		return "", false
	})
	assert.EqualError(t, err, "configured demo bearer is unavailable")
}
```

- [ ] **Step 2: Write failing unsafe-input tests**

```go
func TestParseConfigRejectsUnsafeNetworkInputs(t *testing.T) {
	for _, args := range [][]string{
		{"-listen", "0.0.0.0:3000"},
		{"-origin", "https://example.com"},
		{"-gateway-url", "ws://user:pass@127.0.0.1:8080/v1/realtime"},
		{"-gateway-url", "ws://127.0.0.1:8080/v1/realtime?token=x"},
		{"-gateway-url", "ws://127.0.0.1:8080/other"},
		{"-model", "bad model\n"},
		{"-max-frame-bytes", "0"},
	} {
		_, err := parseConfig(args)
		require.Error(t, err, "args=%q", args)
	}
}

func TestUpstreamURLsAreDerivedWithoutMutatingBase(t *testing.T) {
	cfg, err := parseConfig(nil)
	require.NoError(t, err)
	wsURL, err := cfg.upstreamWebSocketURL()
	require.NoError(t, err)
	assert.Equal(t, "ws://127.0.0.1:8080/v1/realtime?model=local", wsURL.String())
	capabilities, err := cfg.capabilitiesURL()
	require.NoError(t, err)
	assert.Equal(t, "http://127.0.0.1:8080/v1/realtime/capabilities", capabilities.String())
}
```

- [ ] **Step 3: Run the focused tests and verify RED**

Run:

```sh
env GOCACHE=/tmp/gpt-live-ui-config-red go test -count=1 ./demo/live-client -run 'Test(ParseConfig|ResolveBearer|UpstreamURLs)'
```

Expected: compilation fails because `parseConfig`, `resolveBearer`, and `demoConfig` do not exist.

- [ ] **Step 4: Implement strict parsing and URL derivation**

Implement these defaults and flag bindings:

```go
type demoConfig struct {
	ListenAddr   string
	GatewayURL   string
	Model        string
	Origin       string
	BearerEnv    string
	MaxFrameBytes int64
}

func defaultConfig() demoConfig {
	return demoConfig{
		ListenAddr: "127.0.0.1:3000",
		GatewayURL: "ws://127.0.0.1:8080/v1/realtime",
		Model: "local",
		Origin: "http://127.0.0.1:3000",
		MaxFrameBytes: 1 << 20,
	}
}
```

Use `flag.NewFlagSet`, `net.SplitHostPort`, `net.ParseIP(host).IsLoopback()`, and
`url.Parse`. Require the origin host/port to equal the listener, the gateway path
to equal `/v1/realtime`, and model/bearer environment names to match bounded ASCII
allowlists. Permit only `ws`/`wss` upstream and no userinfo, query, fragment, or
opaque URL. Limit `MaxFrameBytes` to `[960, 1<<20]`.

URL methods copy the parsed base. `upstreamWebSocketURL` sets exactly one `model`
query value. `capabilitiesURL` maps `ws` to `http` and `wss` to `https`, appends
`/capabilities`, and contains no model query.

Implement optional bearer resolution exactly:

```go
func resolveBearer(cfg demoConfig, lookup func(string) (string, bool)) ([]byte, error) {
	if cfg.BearerEnv == "" {
		return nil, nil
	}
	value, ok := lookup(cfg.BearerEnv)
	if !ok || value == "" || len(value) > 8<<10 {
		return nil, errors.New("configured demo bearer is unavailable")
	}
	return []byte(value), nil
}
```

- [ ] **Step 5: Add the process shell**

`main` must use `signal.NotifyContext` for `os.Interrupt` and `syscall.SIGTERM`,
clear the optional bearer byte slice, and log sanitized constant messages only:

```go
func main() {
	cfg, err := parseConfig(os.Args[1:])
	if err != nil { log.Fatal("demo configuration failed") }
	bearer, err := resolveBearer(cfg, os.LookupEnv)
	if err != nil { log.Fatal(err) }
	defer clear(bearer)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg, bearer); err != nil { log.Fatal("demo server stopped") }
}
```

Add a temporary `run(context.Context, demoConfig, []byte) error` that returns
`errors.New("demo server is not implemented")`; Task 2 replaces it.

- [ ] **Step 6: Run tests and commit**

```sh
env GOCACHE=/tmp/gpt-live-ui-config-green go test -count=1 ./demo/live-client -run 'Test(ParseConfig|ResolveBearer|UpstreamURLs)'
gofmt -w demo/live-client/*.go
git diff --check -- demo/live-client
git add demo/live-client/config.go demo/live-client/config_test.go demo/live-client/main.go
git commit -m "feat: validate local voice UI configuration"
```

Expected: focused tests pass and the commit contains only Task 1 files.

---

### Task 2: Secure Embedded Server, Capabilities Proxy, and WebSocket Relay

**Files:**
- Create: `demo/live-client/server.go`
- Create: `demo/live-client/server_test.go`
- Create: `demo/live-client/proxy.go`
- Create: `demo/live-client/proxy_test.go`
- Create: `demo/live-client/web/index.html`
- Create: `demo/live-client/web/styles.css`
- Modify: `demo/live-client/main.go`

**Interfaces:**
- Consumes: `demoConfig`, `upstreamWebSocketURL`, `capabilitiesURL`, and optional bearer from Task 1.
- Produces: `type upstreamDependencies struct { Dial dialUpstream; Fetch fetchCapabilities }`.
- Produces: `newHandler(cfg demoConfig, bearer []byte, dependencies upstreamDependencies) (http.Handler, error)`.
- Produces: `proxyWebSocket(ctx context.Context, browser, upstream *websocket.Conn, maxBytes int64) error`.
- Produces: `run(ctx context.Context, cfg demoConfig, bearer []byte) error`.
- Test helpers in `server_test.go` produce `mustTestHandler(t *testing.T, deps upstreamDependencies) http.Handler` and `mustTestHandlerWithBearer(t *testing.T, bearer []byte, deps upstreamDependencies) http.Handler`.
- Test helpers in `proxy_test.go` produce `newEchoUpstream(t *testing.T, expectedAuthorization string) (*httptest.Server, <-chan bool)`, `newDemoTestServer(t *testing.T, gatewayURL string, bearer []byte) *httptest.Server`, `dialDemo(t *testing.T, baseURL, origin string) *websocket.Conn`, `writeText(t *testing.T, conn *websocket.Conn, payload []byte)`, and `readText(t *testing.T, conn *websocket.Conn) []byte`.
- Tasks 3 and 4 add assets under the already embedded `web/*` subtree without changing the server interface.

- [ ] **Step 1: Write failing route and security tests**

```go
func TestStaticAndHealthRoutesUseStrictHeaders(t *testing.T) {
	handler := mustTestHandler(t, upstreamDependencies{})
	for _, path := range []string{"/", "/styles.css", "/health/live"} {
		r := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:3000"+path, nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		assert.Equal(t, http.StatusOK, w.Code, path)
		assert.Equal(t, "no-store", w.Header().Get("Cache-Control"))
		assert.Equal(t, "nosniff", w.Header().Get("X-Content-Type-Options"))
		assert.Contains(t, w.Header().Get("Content-Security-Policy"), "default-src 'self'")
	}
}

func TestCapabilitiesUsesOptionalAuthorizationWithoutExposingIt(t *testing.T) {
	seen := make(chan http.Header, 1)
	deps := upstreamDependencies{Fetch: func(_ context.Context, _ *url.URL, header http.Header) ([]byte, error) {
		seen <- header.Clone()
		return []byte(`{"protocol":"openai-realtime-subset","version":"1.0.0"}`), nil
	}}
	handler := mustTestHandlerWithBearer(t, []byte("sentinel"), deps)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "http://127.0.0.1:3000/api/capabilities", nil))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "Bearer sentinel", (<-seen).Get("Authorization"))
	assert.NotContains(t, w.Body.String(), "sentinel")
}
```

- [ ] **Step 2: Write failing WebSocket boundary tests**

```go
func TestWebSocketRejectsForeignOriginBeforeDial(t *testing.T) {
	var calls atomic.Int32
	deps := upstreamDependencies{Dial: func(context.Context, *url.URL, http.Header) (*websocket.Conn, error) {
		calls.Add(1)
		return nil, errors.New("must not dial")
	}}
	server := httptest.NewServer(mustTestHandler(t, deps))
	defer server.Close()
	header := http.Header{"Origin": []string{"http://evil.invalid"}}
	_, response, err := websocket.Dial(t.Context(), wsURL(server.URL)+"/ws", &websocket.DialOptions{HTTPHeader: header})
	require.Error(t, err)
	assert.Equal(t, http.StatusForbidden, response.StatusCode)
	assert.Zero(t, calls.Load())
}

func TestProxyInjectsBearerAndRelaysText(t *testing.T) {
	upstream, authorized := newEchoUpstream(t, "Bearer sentinel")
	demo := newDemoTestServer(t, upstream.URL, []byte("sentinel"))
	conn := dialDemo(t, demo.URL, "http://127.0.0.1:3000")
	writeText(t, conn, []byte(`{"type":"session.update","session":{"type":"realtime"}}`))
	assert.JSONEq(t, `{"type":"session.update","session":{"type":"realtime"}}`, string(readText(t, conn)))
	assert.True(t, <-authorized)
}
```

Add table cases proving browser and upstream binary frames close with
`websocket.StatusUnsupportedData`, oversized text closes with
`websocket.StatusMessageTooBig`, and a normal close never contains an upstream
body or raw secret.

- [ ] **Step 3: Run route/proxy tests and verify RED**

```sh
env GOCACHE=/tmp/gpt-live-ui-server-red go test -count=1 ./demo/live-client -run 'Test(Static|Capabilities|WebSocket|Proxy)'
```

Expected: compilation fails because handler, dependency, and proxy interfaces do not exist.

- [ ] **Step 4: Implement embedded routes and security headers**

Embed `web/*` and serve explicit paths only. The initial `index.html` needs a
heading and “UI loading” status; Task 4 replaces its content.

```go
//go:embed web/*
var webAssets embed.FS

func browserHeaders(header http.Header) {
	header.Set("Cache-Control", "no-store")
	header.Set("X-Content-Type-Options", "nosniff")
	header.Set("Referrer-Policy", "no-referrer")
	header.Set("Content-Security-Policy", "default-src 'self'; connect-src 'self'; script-src 'self'; style-src 'self'; worker-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'")
}
```

`/api/capabilities` adds optional authorization, applies a 5-second timeout,
limits the upstream body to 64 KiB, requires HTTP 200 and valid JSON, and returns
only `application/json`. Failures return a content-free 502.

For `/ws`, compare `r.Header.Get("Origin") == cfg.Origin` before
`websocket.Accept` or upstream dial. After exact validation, accept with
`InsecureSkipVerify: true` because the application performed the stricter check.

- [ ] **Step 5: Implement bounded upstream dial and relay**

Define:

```go
type dialUpstream func(context.Context, *url.URL, http.Header) (*websocket.Conn, error)
type fetchCapabilities func(context.Context, *url.URL, http.Header) ([]byte, error)
type upstreamDependencies struct { Dial dialUpstream; Fetch fetchCapabilities }
```

Build a fresh header per request; set `Authorization` only when `len(bearer) > 0`.
Copy the bearer into the header immediately before dialing and clear owned
temporary byte copies after use.

The relay uses `errgroup.WithContext` and a shared helper:

```go
func copyText(ctx context.Context, destination, source *websocket.Conn, max int64) error {
	source.SetReadLimit(max)
	for {
		kind, payload, err := source.Read(ctx)
		if err != nil { return err }
		if kind != websocket.MessageText { return errTextFramesRequired }
		if int64(len(payload)) > max { return errFrameTooLarge }
		if err := destination.Write(ctx, websocket.MessageText, payload); err != nil { return err }
	}
}
```

Map only known public close classes. Use constant close reasons such as `text
frames required`, `frame too large`, and `upstream unavailable`; never include
raw errors.

- [ ] **Step 6: Replace the run stub with graceful HTTP lifecycle**

Use `net.Listen`, `http.Server{ReadHeaderTimeout: 5*time.Second}`, a serving error
channel, and a 5-second shutdown context. Treat cancellation followed by
`http.ErrServerClosed` as success. Return constant errors for listen/serve
failures.

- [ ] **Step 7: Run tests, race check, and commit**

```sh
env GOCACHE=/tmp/gpt-live-ui-server-green go test -count=1 ./demo/live-client
env GOCACHE=/tmp/gpt-live-ui-server-green go test -race -count=1 ./demo/live-client
env GOCACHE=/tmp/gpt-live-ui-server-green go vet ./demo/live-client
gofmt -w demo/live-client/*.go
git diff --check -- demo/live-client
git add demo/live-client/main.go demo/live-client/server.go demo/live-client/server_test.go demo/live-client/proxy.go demo/live-client/proxy_test.go demo/live-client/web/index.html demo/live-client/web/styles.css
git commit -m "feat: add secure local voice UI proxy"
```

Expected: all Go demo tests and vet pass.

---

### Task 3: Stateful PCM Audio Primitives and Worklets

**Files:**
- Create: `demo/live-client/web/audio-core.mjs`
- Create: `demo/live-client/web/audio-core.test.mjs`
- Create: `demo/live-client/web/capture-worklet.js`
- Create: `demo/live-client/web/playback-worklet.js`

**Interfaces:**
- Produces: `floatToPCM16(Float32Array) -> Int16Array`.
- Produces: `new StreamingLinearResampler(inputRate, outputRate).push(Float32Array) -> Float32Array`.
- Produces: `new PCM16Framer(960).push(Int16Array) -> Uint8Array[]`.
- Produces: `decodePCM16Base64(encoded, maxBytes) -> Int16Array`.
- Capture worklet posts `{type: "frame", pcm: ArrayBuffer}` with exactly 960 bytes.
- Playback worklet accepts `{type: "append", pcm: ArrayBuffer}` and `{type: "clear"}`.
- Task 4 consumes every exported primitive and worklet message contract.

- [ ] **Step 1: Write failing pure audio tests**

```js
import test from 'node:test';
import assert from 'node:assert/strict';
import { floatToPCM16, StreamingLinearResampler, PCM16Framer, decodePCM16Base64 } from './audio-core.mjs';

test('PCM conversion saturates outside minus one to one', () => {
  assert.deepEqual([...floatToPCM16(Float32Array.from([-2, -1, 0, 1, 2]))],
    [-32768, -32768, 0, 32767, 32767]);
});

test('streaming resampler preserves continuity across pushes', () => {
  const one = new StreamingLinearResampler(48000, 24000);
  const split = [...one.push(Float32Array.from([0, .25, .5])), ...one.push(Float32Array.from([.75, 1, .5]))];
  const two = new StreamingLinearResampler(48000, 24000);
  const joined = [...two.push(Float32Array.from([0, .25, .5, .75, 1, .5]))];
  assert.deepEqual(split, joined);
});

test('framer emits exact 960-byte frames and retains only suffix', () => {
  const framer = new PCM16Framer(960);
  assert.equal(framer.push(new Int16Array(479)).length, 0);
  const frames = framer.push(new Int16Array(481));
  assert.equal(frames.length, 2);
  assert.equal(frames[0].byteLength, 960);
  assert.equal(frames[1].byteLength, 960);
});

test('base64 decoder rejects malformed odd and oversized PCM', () => {
  assert.throws(() => decodePCM16Base64('%%%', 960));
  assert.throws(() => decodePCM16Base64('AA==', 960));
  assert.throws(() => decodePCM16Base64(Buffer.alloc(962).toString('base64'), 960));
});
```

- [ ] **Step 2: Run Node tests and verify RED**

```sh
node --test demo/live-client/web/audio-core.test.mjs
```

Expected: module-not-found failure for `audio-core.mjs`.

- [ ] **Step 3: Implement bounded pure audio primitives**

`floatToPCM16` clamps samples, maps negative values with 32768 and non-negative
values with 32767, and returns a new exact-length array.

`StreamingLinearResampler` validates positive finite rates, retains fractional
source position and one boundary sample across pushes, and never resets phase.
It returns floats so quantization happens once after resampling.

`PCM16Framer` validates the exact 960-byte frame size, stores a byte suffix no
larger than 959 bytes, copies emitted frames, and exposes `reset()` for Stop.

`decodePCM16Base64` uses strict base64 validation, requires a positive even byte
count no larger than `maxBytes`, and returns a copied little-endian `Int16Array`.
Browser code must not depend on Node's `Buffer`; tests may use it only to create fixtures.

- [ ] **Step 4: Implement capture and playback worklets**

The capture processor imports the audio primitives, constructs its resampler
from `sampleRate` to 24000, converts output to PCM16, frames to 960 bytes, and
posts transferable frame buffers. It accepts `reset` and `enabled` messages.

The playback processor maintains a bounded FIFO of copied PCM16 samples. It
converts samples to floats during `process`, outputs silence when empty, and
handles `append` and immediate `clear` messages. Bound the queue to five seconds
of 24 kHz mono audio; discard oldest queued samples on overflow and post a
bounded `overflow` status.

- [ ] **Step 5: Run Node tests and static checks, then commit**

```sh
node --test demo/live-client/web/audio-core.test.mjs
node --check demo/live-client/web/audio-core.mjs
node --check demo/live-client/web/capture-worklet.js
node --check demo/live-client/web/playback-worklet.js
git diff --check -- demo/live-client/web
git add demo/live-client/web/audio-core.mjs demo/live-client/web/audio-core.test.mjs demo/live-client/web/capture-worklet.js demo/live-client/web/playback-worklet.js
git commit -m "feat: add bounded browser audio pipeline"
```

Expected: all four commands exit 0.

---

### Task 4: Protocol Reducer and Accessible Voice UI

**Files:**
- Create: `demo/live-client/web/protocol-state.mjs`
- Create: `demo/live-client/web/protocol-state.test.mjs`
- Create: `demo/live-client/web/app.js`
- Modify: `demo/live-client/web/index.html`
- Modify: `demo/live-client/web/styles.css`
- Modify: `demo/live-client/server_test.go`

**Interfaces:**
- Consumes: Task 3 audio exports and worklet messages.
- Produces: `initialProtocolState()` and `reduceProtocolEvent(state, event)`.
- Produces: browser startup/teardown that owns all media, worklet, WebSocket, and DOM resources.
- Server tests consume the final asset paths and required security-compatible markup.

- [ ] **Step 1: Write failing reducer tests**

```js
import test from 'node:test';
import assert from 'node:assert/strict';
import { initialProtocolState, reduceProtocolEvent } from './protocol-state.mjs';

test('user transcript deltas finalize once', () => {
  let state = initialProtocolState();
  state = reduceProtocolEvent(state, {type:'conversation.item.input_audio_transcription.delta', item_id:'u1', content_index:0, delta:'hel'});
  state = reduceProtocolEvent(state, {type:'conversation.item.input_audio_transcription.completed', item_id:'u1', content_index:0, transcript:'hello'});
  const duplicate = reduceProtocolEvent(state, {type:'conversation.item.input_audio_transcription.completed', item_id:'u1', content_index:0, transcript:'hello'});
  assert.deepEqual(duplicate.turns, state.turns);
  assert.equal(state.turns.at(-1).text, 'hello');
});

test('speech start requests playback clear and user-speaking state', () => {
  const state = reduceProtocolEvent(initialProtocolState(), {type:'input_audio_buffer.speech_started', item_id:'u1', audio_start_ms:0});
  assert.equal(state.phase, 'user-speaking');
  assert.equal(state.effect, 'clear-playback');
});

test('unknown and error events become bounded status without payload echo', () => {
  const secret = 'sentinel-private-payload';
  const state = reduceProtocolEvent(initialProtocolState(), {type:'error', error:{code:'provider_unavailable', message:secret}});
  assert.equal(state.status.at(-1).kind, 'protocol-error');
  assert.doesNotMatch(JSON.stringify(state), /sentinel-private-payload/);
});
```

Add assistant transcript/audio/response-done cases and enforce at most 50 status
entries and 100 transcript turns.

- [ ] **Step 2: Run reducer tests and verify RED**

```sh
node --test demo/live-client/web/protocol-state.test.mjs
```

Expected: module-not-found failure for `protocol-state.mjs`.

- [ ] **Step 3: Implement immutable bounded protocol state**

State shape:

```js
{
  phase: 'idle',
  connected: false,
  muted: false,
  turns: [],
  drafts: {},
  finalized: {},
  status: [],
  effect: null
}
```

Correlate user drafts by `item_id:content_index` and assistant drafts by
`response_id:item_id:content_index`. Copy arrays/maps on change. Store only known
event classes and sanitized error codes. Never retain whole raw events.

- [ ] **Step 4: Build the accessible document and responsive styling**

The final HTML must contain stable IDs used by `app.js`:

```html
<main id="app">
  <header><p class="eyebrow">Local development tool</p><h1>GPT Live Voice Test</h1><p id="connection-status" role="status" aria-live="polite">Idle</p></header>
  <section class="controls" aria-label="Voice controls">
    <button id="start-stop" type="button">Start</button>
    <button id="mute" type="button" disabled>Mute microphone</button>
  </section>
  <section id="capabilities" aria-labelledby="capabilities-title"><h2 id="capabilities-title">Gateway</h2></section>
  <section id="transcript" aria-labelledby="transcript-title"><h2 id="transcript-title">Transcript</h2><ol id="turns"></ol></section>
  <details><summary>Connection events</summary><ol id="events"></ol></details>
</main>
```

Load only external module scripts. Use a calm responsive two-column desktop
layout collapsing to one column below 760px, visible focus rings, AA contrast,
and `prefers-reduced-motion`. Do not use inline handlers/styles or remote fonts.

- [ ] **Step 5: Implement browser orchestration and complete teardown**

On Start:

1. fetch `/api/capabilities` and verify PCM16/24000/mono;
2. request `getUserMedia({audio:{channelCount:1, echoCancellation:true, noiseSuppression:true}, video:false})`;
3. create/resume an `AudioContext`;
4. register capture/playback worklets and nodes;
5. connect `new WebSocket(new URL('/ws', location.href).href.replace(/^http/, 'ws'))`;
6. on `session.created`, send exactly:

```js
socket.send(JSON.stringify({
  type: 'session.update',
  session: {
    type: 'realtime',
    audio: {
      input: {format: {type: 'audio/pcm', rate: 24000}, turn_detection: {type: 'server_vad'}},
      output: {format: {type: 'audio/pcm', rate: 24000}}
    },
    output_modalities: ['audio']
  }
}));
```

Base64-encode each 960-byte capture frame and send only while open and unmuted.
Decode output audio with Task 3's bounded decoder and transfer it to playback.
Apply reducer effects; `clear-playback` posts `{type:'clear'}` immediately.

One idempotent `stopSession(reason)` must stop media tracks, disconnect nodes,
clear worklets, remove handlers, close the WebSocket, close the AudioContext,
clear resource references, and restore enabled controls. Microphone denial must
call the same teardown without ever opening a socket.

- [ ] **Step 6: Add final asset and CSP tests**

Extend `server_test.go` to GET every explicit asset and assert status/content type.
Assert `index.html` contains no inline `<script>`, no `style=`, and no external
`http` asset. Assert the CSP allows self scripts/workers and contains no
`unsafe-inline` or `unsafe-eval`.

- [ ] **Step 7: Run JavaScript and Go tests, then commit**

```sh
node --test demo/live-client/web/audio-core.test.mjs demo/live-client/web/protocol-state.test.mjs
node --check demo/live-client/web/app.js
node --check demo/live-client/web/protocol-state.mjs
env GOCACHE=/tmp/gpt-live-ui-web-green go test -count=1 ./demo/live-client
git diff --check -- demo/live-client
git add demo/live-client/web demo/live-client/server_test.go
git commit -m "feat: add local realtime voice test UI"
```

Expected: Node and Go suites pass with no console/static syntax error.

---

### Task 5: Developer Commands, Documentation, and Browser Smoke

**Files:**
- Modify: `Makefile`
- Modify: `README.md`

**Interfaces:**
- Consumes: complete `demo/live-client` command and web UI.
- Produces: `make demo-live-client` and `make test-demo-live-client`.
- Produces: exact user-facing startup, verification, security, and stop steps.

- [ ] **Step 1: Add Make targets**

Add phony targets without changing existing targets:

```make
demo-live-client:
	go run ./demo/live-client

test-demo-live-client:
	go test -count=1 ./demo/live-client
	node --test demo/live-client/web/audio-core.test.mjs demo/live-client/web/protocol-state.test.mjs
```

Add both names to `.PHONY`.

- [ ] **Step 2: Document the exact local UI quickstart**

Add a `## Test with the browser UI` section after `Add local speech`:

```sh
go run ./cmd/live-gateway
go run ./cmd/live-worker --backend none
go run ./demo/live-client
```

Tell the user to open `http://127.0.0.1:3000`, press Start, grant microphone
permission, and use Mute/Stop. State explicitly that the page/capabilities load
without speech, while realtime conversation requires the compatible local
speech endpoint at `ws://127.0.0.1:8765/v1/realtime`. Document optional configured
gateway auth as:

```sh
LIVE_DEMO_BEARER=... go run ./demo/live-client -bearer-env LIVE_DEMO_BEARER
```

Keep this only in an optional subsection and warn against shell-history exposure.
Repeat that the demo is loopback development tooling, not a production proxy.

- [ ] **Step 3: Run focused automated verification**

```sh
env GOCACHE=/tmp/gpt-live-ui-final go test -count=1 ./demo/live-client ./internal/config ./cmd/live-gateway ./cmd/live-worker
env GOCACHE=/tmp/gpt-live-ui-final go vet ./demo/live-client ./internal/config ./cmd/live-gateway ./cmd/live-worker
env GOCACHE=/tmp/gpt-live-ui-final go build ./demo/live-client ./cmd/live-gateway ./cmd/live-worker
node --test demo/live-client/web/audio-core.test.mjs demo/live-client/web/protocol-state.test.mjs
make test-demo-live-client
git diff --check
```

Expected: all focused commands exit 0. Full-repository failures caused by the
pre-existing malformed untracked `internal/profile/...` path or dirty gateway
session work must be recorded separately and not repaired in this task.

- [ ] **Step 4: Perform credential-free process and HTTP smoke**

Build gateway and UI binaries into a new `mktemp -d` directory. Start both with
empty environments except `PATH`; the UI must not request a credential.

```sh
env -i PATH=/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin ./live-gateway
env -i PATH=/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin ./live-client
curl --fail http://127.0.0.1:3000/health/live
curl --fail http://127.0.0.1:3000/api/capabilities
curl --fail http://127.0.0.1:3000/
```

Expected: `ok`, capability JSON, and the UI document. Send SIGTERM only to the
owned process PIDs and verify both exit 0.

- [ ] **Step 5: Run browser smoke**

With the gateway and UI running, open `http://127.0.0.1:3000` in a real browser.
Verify no console errors, no external network requests, and no credential in the
page/network URLs. Press Start without a speech server and confirm the UI reports
a sanitized gateway/provider-unavailable state and releases the microphone.

When a compatible local speech service is available at
`ws://127.0.0.1:8765/v1/realtime`, verify one spoken turn, finalized user and
assistant transcripts, audible response playback, barge-in clearing playback,
mute suppressing new frames, and Stop releasing the microphone/audio context.
If the service is unavailable in the execution environment, record that single
manual audio check as unavailable; fake-upstream Go tests and browser resource
cleanup checks remain mandatory and must pass.

- [ ] **Step 6: Inspect scope and commit**

```sh
git status --short
git diff -- Makefile README.md
git diff --check
git add Makefile README.md
git commit -m "docs: add local voice UI quickstart"
```

Expected: the commit contains only Makefile and README changes; no production
service, deployment, migration, dependency, or user-owned dirty file is staged.

- [ ] **Step 7: Capture final branch evidence**

```sh
git log --oneline --decorate -8
git diff --stat 620286f..HEAD
git status --short --branch
```

Report task commits, focused test/vet/build results, Node results, process smoke,
browser smoke, any unavailable real-speech check, and every untouched pre-existing
dirty file category.
