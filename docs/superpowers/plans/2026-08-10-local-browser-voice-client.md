# Local Browser Voice Client Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a loopback-only browser demo that streams microphone PCM through GPT Live, plays real provider audio, supports server-VAD barge-in, and renders live user/assistant transcripts without exposing the gateway bearer.

**Architecture:** A small Go BFF serves embedded static assets and relays one browser WebSocket to the authenticated GPT Live WebSocket. Browser AudioWorklets perform stateful 24 kHz PCM capture and queued playback; pure ECMAScript modules own framing and transcript state so Node's built-in test runner can verify them without a browser.

**Tech Stack:** Go 1.26, `github.com/coder/websocket` v1.8.15, standard `embed`/`net/http`, browser Web Audio API and AudioWorklet, ECMAScript modules, Node 22 built-in test runner.

## Global Constraints

- Bind HTTP and WebSocket listeners to loopback only; the default is `127.0.0.1:3000`.
- Connect by default to `ws://127.0.0.1:18080/v1/realtime?model=gpt-live-real`.
- Read the bearer only from the configured process environment variable; never send it to browser code, a URL, cookie, storage, log, or response.
- Accept exactly one configured browser origin and reject binary or oversized WebSocket frames.
- Capture and play 24 kHz mono signed PCM16 in exact 20 ms / 960-byte frames.
- Use server VAD as turn authority; on speech start, clear queued playback without inventing response IDs or epochs.
- Render transcripts with `textContent` only and keep audio/transcript data in page memory.
- Add no npm dependency, no production auth mechanism, no deployment manifest, and no direct browser-to-provider path.
- Logs and test output contain event classes and status only, never payloads, media, transcripts, provider bodies, headers, or credentials.

---

## File Structure

- `demo/live-client/main.go` — CLI parsing, secret lookup, lifecycle, and HTTP server startup.
- `demo/live-client/config.go` — non-secret option parsing and strict loopback/origin/upstream/model validation.
- `demo/live-client/config_test.go` — configuration and secret-presence boundary tests.
- `demo/live-client/server.go` — embedded assets, security headers, health route, exact-origin gate, and route registration.
- `demo/live-client/server_test.go` — static asset, CSP/no-store, health, and hostile-origin tests.
- `demo/live-client/proxy.go` — bounded bidirectional text-frame relay and credential-injecting upstream dial.
- `demo/live-client/proxy_test.go` — fake-upstream auth, relay, close, binary, and size-limit tests.
- `demo/live-client/web/index.html` — accessible single-page controls, status, and transcript containers.
- `demo/live-client/web/styles.css` — responsive local demo presentation.
- `demo/live-client/web/audio-core.mjs` — stateful resampling, PCM16 conversion, exact framing, and base64 decoding.
- `demo/live-client/web/audio-core.test.mjs` — Node tests for continuity, frame size, saturation, and decoding.
- `demo/live-client/web/protocol-state.mjs` — typed event classification and duplicate-safe transcript/state reducer.
- `demo/live-client/web/protocol-state.test.mjs` — Node tests for transcript assembly, terminal dedupe, and barge-in effects.
- `demo/live-client/web/capture-worklet.js` — microphone worklet using `audio-core.mjs`.
- `demo/live-client/web/playback-worklet.js` — bounded PCM queue with immediate clear control.
- `demo/live-client/web/app.js` — DOM, microphone, WebSocket, protocol events, and resource teardown.
- `Makefile` — `demo-live-client` and `test-demo-live-client` entry points.
- `README.md` — exact local demo prerequisites, start URL, controls, security boundary, and stop procedure.

---

### Task 1: Strict Demo Configuration and Secret Boundary

**Files:**
- Create: `demo/live-client/config.go`
- Create: `demo/live-client/config_test.go`
- Create: `demo/live-client/main.go`

**Interfaces:**
- Produces: `parseConfig(args []string) (demoConfig, error)`.
- Produces: `resolveBearer(cfg demoConfig, lookup func(string) (string, bool)) ([]byte, error)`.
- Produces: `demoConfig.upstreamURL() (*url.URL, error)` for Task 2.
- `demoConfig` fields: `ListenAddr`, `GatewayURL`, `Model`, `Origin`, `BearerEnv`, and `MaxFrameBytes`.

- [ ] **Step 1: Write failing configuration tests**

```go
func TestParseConfigAcceptsCanonicalLoopbackDefaults(t *testing.T) {
	cfg, err := parseConfig(nil)
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:3000", cfg.ListenAddr)
	assert.Equal(t, "ws://127.0.0.1:18080/v1/realtime", cfg.GatewayURL)
	assert.Equal(t, "gpt-live-real", cfg.Model)
	assert.Equal(t, "http://127.0.0.1:3000", cfg.Origin)
	assert.Equal(t, "LIVE_DEMO_BEARER", cfg.BearerEnv)
	assert.EqualValues(t, 64<<10, cfg.MaxFrameBytes)
}

func TestParseConfigRejectsUnsafeNetworkAndURLInputs(t *testing.T) {
	for _, args := range [][]string{
		{"-listen", "0.0.0.0:3000"},
		{"-origin", "https://example.com"},
		{"-gateway-url", "ws://user:pass@127.0.0.1:18080/v1/realtime"},
		{"-gateway-url", "ws://127.0.0.1:18080/v1/realtime?token=x"},
		{"-model", "bad model\n"},
	} {
		_, err := parseConfig(args)
		require.Error(t, err)
	}
}

func TestResolveBearerRequiresNonemptyBoundedValueWithoutEchoingIt(t *testing.T) {
	cfg, err := parseConfig(nil)
	require.NoError(t, err)
	_, err = resolveBearer(cfg, func(string) (string, bool) { return "", false })
	assert.EqualError(t, err, "configured demo bearer is unavailable")
	secret := "sentinel-demo-secret"
	value, err := resolveBearer(cfg, func(string) (string, bool) { return secret, true })
	require.NoError(t, err)
	assert.Equal(t, []byte(secret), value)
	assert.NotContains(t, fmt.Sprint(err), secret)
}
```

- [ ] **Step 2: Run the tests and verify the RED failure**

Run: `GOTOOLCHAIN=local GOCACHE=/tmp/gpt-live-demo-config-red go test ./demo/live-client -run 'Test(ParseConfig|ResolveBearer)' -count=1`

Expected: FAIL because `demoConfig`, `parseConfig`, and `resolveBearer` do not exist.

- [ ] **Step 3: Implement strict configuration parsing**

```go
type demoConfig struct {
	ListenAddr, GatewayURL, Model, Origin, BearerEnv string
	MaxFrameBytes int64
}

func parseConfig(args []string) (demoConfig, error) {
	cfg := demoConfig{
		ListenAddr: "127.0.0.1:3000",
		GatewayURL: "ws://127.0.0.1:18080/v1/realtime",
		Model: "gpt-live-real", Origin: "http://127.0.0.1:3000",
		BearerEnv: "LIVE_DEMO_BEARER", MaxFrameBytes: 64 << 10,
	}
	flags := flag.NewFlagSet("live-client", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&cfg.ListenAddr, "listen", cfg.ListenAddr, "loopback HTTP listen address")
	flags.StringVar(&cfg.GatewayURL, "gateway-url", cfg.GatewayURL, "GPT Live WebSocket URL without query")
	flags.StringVar(&cfg.Model, "model", cfg.Model, "advertised gateway model")
	flags.StringVar(&cfg.Origin, "origin", cfg.Origin, "exact browser origin")
	flags.StringVar(&cfg.BearerEnv, "bearer-env", cfg.BearerEnv, "environment variable containing the bearer")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 { return demoConfig{}, errors.New("invalid demo arguments") }
	if err := cfg.validate(); err != nil { return demoConfig{}, err }
	return cfg, nil
}
```

Validation must use `net.SplitHostPort`, `net.ParseIP(...).IsLoopback`, and `url.Parse`. The gateway URL permits only `ws`/`wss`, an absolute host, path `/v1/realtime`, and no userinfo/query/fragment/opaque value. The exact origin must use `http`, have no credentials/path/query/fragment, and resolve to the configured listen host and port. Model and environment names must use bounded ASCII allowlists.

- [ ] **Step 4: Add the startup shell without starting the server yet**

```go
func main() {
	cfg, err := parseConfig(os.Args[1:])
	if err != nil { log.Fatal("demo configuration failed") }
	bearer, err := resolveBearer(cfg, os.LookupEnv)
	if err != nil { log.Fatal(err) }
	defer clear(bearer)
	if err := run(context.Background(), cfg, bearer); err != nil {
		log.Fatal("demo server stopped")
	}
}
```

Use a temporary `run` stub returning `errors.New("demo server is not implemented")`; Task 2 replaces it. Never pass `err` to `log.Fatal` where it could contain a provider body or URL.

- [ ] **Step 5: Run focused tests and commit**

Run: `GOTOOLCHAIN=local GOCACHE=/tmp/gpt-live-demo-config-green go test ./demo/live-client -run 'Test(ParseConfig|ResolveBearer)' -count=1`

Expected: PASS.

```bash
git add demo/live-client/config.go demo/live-client/config_test.go demo/live-client/main.go
git commit -m "feat: validate local voice demo configuration"
```

---

### Task 2: Embedded HTTP Server and Authenticated WebSocket Proxy

**Files:**
- Create: `demo/live-client/server.go`
- Create: `demo/live-client/server_test.go`
- Create: `demo/live-client/proxy.go`
- Create: `demo/live-client/proxy_test.go`
- Create: `demo/live-client/web/index.html`
- Create: `demo/live-client/web/styles.css`
- Modify: `demo/live-client/main.go`

**Interfaces:**
- Consumes: `demoConfig` and `demoConfig.upstreamURL()` from Task 1.
- Produces: `newHandler(cfg demoConfig, bearer []byte, dial dialUpstream) (http.Handler, error)`.
- Produces: `proxyWebSocket(ctx context.Context, browser, upstream *websocket.Conn, maxBytes int64) error`.
- Produces: `run(ctx context.Context, cfg demoConfig, bearer []byte) error`.
- `dialUpstream` signature: `func(context.Context, *url.URL, http.Header) (*websocket.Conn, error)`.

- [ ] **Step 1: Write failing HTTP security and origin tests**

```go
func TestStaticAssetsCarryStrictHeaders(t *testing.T) {
	handler := mustTestHandler(t, nil)
	request := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:3000/", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Contains(t, response.Header().Get("Content-Security-Policy"), "default-src 'self'")
	assert.Equal(t, "nosniff", response.Header().Get("X-Content-Type-Options"))
	assert.Equal(t, "no-store", response.Header().Get("Cache-Control"))
}

func TestWebSocketRejectsForeignOriginBeforeDial(t *testing.T) {
	var calls atomic.Int32
	handler := mustTestHandler(t, func(context.Context, *url.URL, http.Header) (*websocket.Conn, error) {
		calls.Add(1)
		return nil, errors.New("must not dial")
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	header := http.Header{"Origin": []string{"http://evil.invalid"}}
	_, response, err := websocket.Dial(t.Context(), wsURL(server.URL)+"/ws", &websocket.DialOptions{HTTPHeader: header})
	require.Error(t, err)
	assert.Equal(t, http.StatusForbidden, response.StatusCode)
	assert.Zero(t, calls.Load())
}
```

- [ ] **Step 2: Write failing proxy tests**

Create a fake upstream `httptest.Server` that accepts WebSockets, captures only whether `Authorization == "Bearer sentinel"`, and echoes one text frame.

```go
func TestProxyInjectsBearerAndRelaysTextWithoutBrowserExposure(t *testing.T) {
	upstream, authorized := newEchoUpstream(t, "Bearer sentinel")
	demo := newDemoTestServer(t, upstream, []byte("sentinel"))
	conn := dialDemo(t, demo.URL, "http://127.0.0.1:3000")
	writeText(t, conn, []byte(`{"type":"response.create"}`))
	assert.JSONEq(t, `{"type":"response.create"}`, string(readText(t, conn)))
	assert.True(t, <-authorized)
}

func TestProxyRejectsBinaryAndOversizedFrames(t *testing.T) {
	// Binary closes with StatusUnsupportedData; a text payload above the
	// configured limit closes with StatusMessageTooBig.
}
```

- [ ] **Step 3: Run the focused RED tests**

Run: `GOTOOLCHAIN=local GOCACHE=/tmp/gpt-live-demo-proxy-red go test ./demo/live-client -run 'Test(Static|WebSocket|Proxy)' -count=1`

Expected: FAIL because `newHandler`, the embedded assets, and proxy functions do not exist.

- [ ] **Step 4: Implement embedded assets and exact security headers**

```go
//go:embed web/*
var webAssets embed.FS

func setBrowserHeaders(header http.Header, websocketOrigin string) {
	header.Set("Cache-Control", "no-store")
	header.Set("X-Content-Type-Options", "nosniff")
	header.Set("Referrer-Policy", "no-referrer")
	header.Set("Content-Security-Policy", "default-src 'self'; connect-src 'self' "+websocketOrigin+"; media-src 'none'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'")
}
```

Derive `websocketOrigin` once from the already validated `cfg.Origin` by mapping
`http` to `ws` and `https` to `wss` while retaining the exact host and port. Do
not accept a free-form CSP value. Serve `/`, `/styles.css`, and later
JavaScript/worklet assets from the embedded subtree with explicit content
types. Serve `/health/ready` as `ready\n`. Return 404 for every other HTTP
path. For `/ws`, require `r.Header.Get("Origin") == cfg.Origin` before calling
`websocket.Accept`.

- [ ] **Step 5: Implement the bounded credential-injecting relay**

```go
func copyText(ctx context.Context, destination, source *websocket.Conn, max int64) error {
	source.SetReadLimit(max)
	for {
		kind, payload, err := source.Read(ctx)
		if err != nil { return err }
		if kind != websocket.MessageText {
			return websocket.CloseError{Code: websocket.StatusUnsupportedData, Reason: "text frames required"}
		}
		if int64(len(payload)) > max { return errFrameTooLarge }
		if err := destination.Write(ctx, websocket.MessageText, payload); err != nil { return err }
	}
}
```

Build the upstream URL by copying the validated URL and setting exactly one `model` query value. Inject `Authorization: Bearer <secret>` only into the upstream dial header. Accept the browser socket only after exact-origin validation. Use `errgroup.WithContext` for both copy directions; cancel and close both sockets when either direction ends. Map only bounded close classes to the browser and never include upstream bodies or raw errors in close reasons.

- [ ] **Step 6: Replace the startup stub with a bounded HTTP server**

```go
func run(ctx context.Context, cfg demoConfig, bearer []byte) error {
	handler, err := newHandler(cfg, bearer, defaultDial)
	if err != nil { return err }
	listener, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil { return errors.New("listen demo server") }
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	go func() { <-ctx.Done(); shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second); defer cancel(); _ = server.Shutdown(shutdown) }()
	return server.Serve(listener)
}
```

Treat `http.ErrServerClosed` after cancellation as success.

- [ ] **Step 7: Run tests and commit**

Run: `GOTOOLCHAIN=local GOCACHE=/tmp/gpt-live-demo-proxy-green go test -race ./demo/live-client -run 'Test(Static|WebSocket|Proxy)' -count=1`

Expected: PASS.

```bash
git add demo/live-client
git commit -m "feat: add authenticated local voice demo proxy"
```

---

### Task 3: Stateful PCM Capture, Resampling, Framing, and Playback Queue

**Files:**
- Create: `demo/live-client/web/audio-core.mjs`
- Create: `demo/live-client/web/audio-core.test.mjs`
- Create: `demo/live-client/web/capture-worklet.js`
- Create: `demo/live-client/web/playback-worklet.js`
- Modify: `demo/live-client/server.go`
- Modify: `demo/live-client/server_test.go`

**Interfaces:**
- Produces: `StreamingLinearResampler(inputRate, outputRate).push(Float32Array) -> Int16Array`.
- Produces: `PCM16Framer(frameBytes).push(Int16Array) -> Uint8Array[]`.
- Produces: `decodePCM16Base64(encoded) -> Int16Array`.
- Capture worklet posts `{type: "frame", pcm: ArrayBuffer}` containing exactly 960 bytes.
- Playback worklet accepts `{type: "append", pcm: ArrayBuffer}` and `{type: "clear"}`.

- [ ] **Step 1: Write failing Node tests for audio primitives**

```js
test('48 kHz input becomes contiguous 24 kHz PCM16', () => {
  const resampler = new StreamingLinearResampler(48000, 24000);
  const left = resampler.push(Float32Array.from([0, 0.25, 0.5]));
  const right = resampler.push(Float32Array.from([0.75, 1, 0.5]));
  assert.deepEqual([...left, ...right], [0, 16384, 32767]);
});

test('framer emits only exact 960-byte frames and retains the suffix', () => {
  const framer = new PCM16Framer(960);
  assert.equal(framer.push(new Int16Array(479)).length, 0);
  const frames = framer.push(new Int16Array(481));
  assert.equal(frames.length, 2);
  assert.equal(frames[0].byteLength, 960);
  assert.equal(frames[1].byteLength, 960);
});

test('PCM conversion saturates values outside [-1, 1]', () => {
  assert.deepEqual([...floatToPCM16(Float32Array.from([-2, -1, 0, 1, 2]))], [-32768, -32768, 0, 32767, 32767]);
});
```

- [ ] **Step 2: Run Node tests and verify RED**

Run: `node --test demo/live-client/web/audio-core.test.mjs`

Expected: FAIL because `audio-core.mjs` and its exports do not exist.

- [ ] **Step 3: Implement pure stateful audio primitives**

Keep fractional source position and the final prior sample across `push` calls. Allocate exact-capacity output arrays. `PCM16Framer` stores only the sub-frame suffix and enforces a maximum retained byte count of 960. `decodePCM16Base64` rejects malformed/odd/oversized decoded audio before returning a copied `Int16Array`.

```js
export class PCM16Framer {
  constructor(frameBytes = 960) { this.frameBytes = frameBytes; this.pending = new Uint8Array(0); }
  push(samples) {
    const incoming = new Uint8Array(samples.buffer, samples.byteOffset, samples.byteLength);
    const joined = new Uint8Array(this.pending.length + incoming.length);
    joined.set(this.pending); joined.set(incoming, this.pending.length);
    const frames = [];
    for (let offset = 0; offset + this.frameBytes <= joined.length; offset += this.frameBytes) {
      frames.push(joined.slice(offset, offset + this.frameBytes));
    }
    this.pending = joined.slice(frames.length * this.frameBytes);
    return frames;
  }
}
```

- [ ] **Step 4: Implement capture and playback AudioWorklets**

`capture-worklet.js` imports `StreamingLinearResampler` and `PCM16Framer`, processes channel zero, and transfers each exact frame buffer to the main thread. It emits no frames while muted.

`playback-worklet.js` owns a bounded queue of at most ten seconds of 24 kHz samples. It outputs silence when empty, consumes samples monotonically, and clears queued/current buffers immediately on `{type:"clear"}`. It posts only state changes (`playing`/`idle`) to the main thread.

- [ ] **Step 5: Register assets and verify headers/content types**

Extend `server.go` to serve `.mjs` and worklet `.js` files as `text/javascript; charset=utf-8`. Extend `server_test.go` to request each exact path and assert status 200, no-store, CSP, and correct type.

- [ ] **Step 6: Run tests and commit**

Run: `node --test demo/live-client/web/audio-core.test.mjs`

Run: `GOTOOLCHAIN=local GOCACHE=/tmp/gpt-live-demo-audio-go go test ./demo/live-client -run TestStaticAssetsCarryStrictHeaders -count=1`

Expected: both PASS.

```bash
git add demo/live-client/web demo/live-client/server.go demo/live-client/server_test.go
git commit -m "feat: add bounded browser audio pipeline"
```

---

### Task 4: Protocol State, Live Transcripts, Server VAD, and Barge-In UI

**Files:**
- Create: `demo/live-client/web/protocol-state.mjs`
- Create: `demo/live-client/web/protocol-state.test.mjs`
- Create: `demo/live-client/web/app.js`
- Modify: `demo/live-client/web/index.html`
- Modify: `demo/live-client/web/styles.css`
- Modify: `demo/live-client/server.go`

**Interfaces:**
- Produces: `ProtocolState.consume(event) -> Effect[]`.
- Effects are closed objects: `{type:"status", value}`, `{type:"user-delta", text}`, `{type:"user-done", text}`, `{type:"assistant-delta", text}`, `{type:"assistant-done", text}`, `{type:"audio", encoded}`, `{type:"clear-playback"}`, and `{type:"error", code}`.
- Produces browser lifecycle functions `startSession()`, `stopSession()`, and `setMuted(boolean)`.

- [ ] **Step 1: Write failing protocol reducer tests**

```js
test('speech start clears playback before reporting user speech', () => {
  const effects = new ProtocolState().consume({type: 'input_audio_buffer.speech_started'});
  assert.deepEqual(effects.map(effect => effect.type), ['clear-playback', 'status']);
  assert.equal(effects[1].value, 'user-speaking');
});

test('transcript deltas finalize once despite duplicate done', () => {
  const state = new ProtocolState();
  state.consume({type: 'conversation.item.input_audio_transcription.delta', item_id: 'msg_1', delta: 'hel'});
  state.consume({type: 'conversation.item.input_audio_transcription.completed', item_id: 'msg_1', transcript: 'hello'});
  const duplicate = state.consume({type: 'conversation.item.input_audio_transcription.completed', item_id: 'msg_1', transcript: 'hello'});
  assert.deepEqual(duplicate, []);
});

test('audio and assistant transcript events are classified without retaining raw envelopes', () => {
  const state = new ProtocolState();
  assert.deepEqual(state.consume({type:'response.output_audio.delta', delta:'AAA='}), [{type:'audio', encoded:'AAA='}]);
  assert.deepEqual(state.consume({type:'response.output_audio_transcript.done', response_id:'r1', transcript:'hi'}), [{type:'assistant-done', text:'hi'}]);
});
```

- [ ] **Step 2: Run Node tests and verify RED**

Run: `node --test demo/live-client/web/protocol-state.test.mjs`

Expected: FAIL because `ProtocolState` does not exist.

- [ ] **Step 3: Implement bounded duplicate-safe protocol state**

Use maps capped at 64 active/finalized item or response IDs. Reject IDs/text beyond 256/64 KiB. Never store full envelopes or audio. Normalize server errors to a bounded code and discard provider messages.

```js
export class ProtocolState {
  consume(event) {
    if (!event || typeof event.type !== 'string') return [{type:'error', code:'invalid-event'}];
    switch (event.type) {
      case 'input_audio_buffer.speech_started':
        return [{type:'clear-playback'}, {type:'status', value:'user-speaking'}];
      case 'response.output_audio.delta':
        return typeof event.delta === 'string' ? [{type:'audio', encoded:event.delta}] : [{type:'error', code:'invalid-audio'}];
      // Implement every effect listed in Interfaces with exact ID/size checks.
    }
  }
}
```

- [ ] **Step 4: Implement the accessible page and browser lifecycle**

`index.html` contains buttons `#start`, `#stop`, `#mute`, status `#status`, transcript log `#transcript`, and an `aria-live="polite"` error/status region. No inline script or style is allowed under the CSP.

`app.js`:

1. calls `getUserMedia({audio:{channelCount:1, echoCancellation:true, noiseSuppression:true, autoGainControl:true}})`;
2. creates a 24 kHz playback context when supported and otherwise passes the actual rate to the playback worklet;
3. loads both worklets;
4. opens `ws://${location.host}/ws`;
5. after `session.created`, sends exactly one bounded `session.update` with `type:"realtime"` and `audio.input.turn_detection` set to `{type:"server_vad", threshold:0.5, prefix_padding_ms:300, silence_duration_ms:500, create_response:true, interrupt_response:true}`;
6. forwards capture frames as base64 `input_audio_buffer.append` events;
7. applies reducer effects to DOM text nodes and playback; and
8. performs idempotent teardown on Stop, socket close, permission denial, or page unload.

- [ ] **Step 5: Run protocol tests and static server tests**

Run: `node --test demo/live-client/web/protocol-state.test.mjs demo/live-client/web/audio-core.test.mjs`

Run: `GOTOOLCHAIN=local GOCACHE=/tmp/gpt-live-demo-ui-go go test ./demo/live-client -count=1`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add demo/live-client/web demo/live-client/server.go
git commit -m "feat: add hands-free voice demo interface"
```

---

### Task 5: Developer Commands, Documentation, and Real-Stack Verification

**Files:**
- Modify: `Makefile`
- Modify: `README.md`
- Test: `demo/live-client/*_test.go`
- Test: `demo/live-client/web/*.test.mjs`

**Interfaces:**
- Produces: `make demo-live-client` requiring `LIVE_DEMO_BEARER` and using canonical defaults.
- Produces: `make test-demo-live-client` running Go race tests plus Node tests.

- [ ] **Step 1: Add failing Makefile contract checks**

Extend a Go test or a small shell assertion in the Make target test path to require both target names and ensure `demo-live-client` does not contain a literal bearer value.

```sh
rg -n '^demo-live-client:|^test-demo-live-client:' Makefile
! rg -n 'local-demo-token-change-me|Authorization: Bearer [^$]' Makefile demo/live-client
```

Run before editing: the first command must fail because the targets do not exist.

- [ ] **Step 2: Add exact Make targets**

```make
.PHONY: demo-live-client test-demo-live-client
demo-live-client:
	@test -n "$$LIVE_DEMO_BEARER"
	go run ./demo/live-client

test-demo-live-client:
	go test -race -count=1 ./demo/live-client
	node --test demo/live-client/web/audio-core.test.mjs demo/live-client/web/protocol-state.test.mjs
```

- [ ] **Step 3: Document exact startup and safety boundary**

Add a README section that states the prerequisites:

- PostgreSQL container, async worker, real speech service, and GPT Live gateway health URLs;
- `LIVE_DEMO_BEARER` must equal the local gateway static bearer;
- `make demo-live-client`, then open `http://127.0.0.1:3000`;
- click Start, grant microphone permission, speak normally, interrupt playback by speaking, and click Stop;
- browser connects only to the loopback BFF; the bearer is injected server-side;
- this local BFF is not the production OIDC/BFF solution; and
- exact stop commands for the demo server only, without deleting PostgreSQL data.

- [ ] **Step 4: Run the complete automated gate**

Run: `LIVE_DEMO_BEARER='test-only-bearer' make test-demo-live-client`

Expected: Go race and both Node suites PASS.

Run: `GOTOOLCHAIN=local GOCACHE=/tmp/gpt-live-demo-full-go go test -race -count=1 ./...`

Expected: PASS.

Run: `go vet ./... && go build ./... && go mod tidy -diff && git diff --check`

Expected: all exit 0 and tidy reports no module changes.

- [ ] **Step 5: Start the real demo and run the browser smoke**

Retrieve the local bearer outside the browser, set `LIVE_DEMO_BEARER`, and run `make demo-live-client`. Verify `curl --fail http://127.0.0.1:3000/health/ready` returns `ready`.

In the browser at `http://127.0.0.1:3000`:

1. click Start and grant microphone permission;
2. speak a bounded non-sensitive phrase;
3. confirm the finalized user transcript appears;
4. confirm an assistant transcript appears and audible MiniMax audio plays;
5. speak during playback and confirm queued audio stops before the new turn;
6. click Stop and confirm the speech provider `/v1/pool` returns `in_use:0`; and
7. inspect the browser console/network metadata only to confirm no bearer appears in page source, WebSocket URL, storage, or response headers.

- [ ] **Step 6: Commit final integration**

```bash
git add Makefile README.md demo/live-client
git commit -m "docs: add local browser voice demo workflow"
```

---

## Final Review Checklist

- [ ] Every spec requirement maps to Tasks 1–5.
- [ ] No browser asset contains a bearer, provider key, OIDC token, or database URL.
- [ ] No WebSocket URL or subprotocol carries a credential.
- [ ] Foreign origins, binary frames, and oversized frames fail before upstream forwarding.
- [ ] Capture is exactly 24 kHz mono PCM16 in 960-byte frames across chunk boundaries.
- [ ] Speech start clears queued audio without generating a fake response ID/epoch.
- [ ] User and assistant transcripts finalize once and render through `textContent`.
- [ ] Stop releases microphone tracks, AudioContexts, worklets, and both sockets.
- [ ] The existing production config edits and unrelated `.zshrc` are not staged by demo commits.
