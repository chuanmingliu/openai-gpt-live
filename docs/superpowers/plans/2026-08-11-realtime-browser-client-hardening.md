# Realtime Browser Client Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Harden the loopback browser voice demo so microphone capture, live transcripts, audible playback, and server-VAD barge-in remain bounded, truthful, and responsive under provider bursts, browser scheduling delays, retries, and longer conversations.

**Architecture:** Preserve the dependency-free Go BFF and static browser application. Extract testable browser presentation/runtime state, make AudioWorklet hot paths allocation-conscious, keep pacing in the browser with generation fencing, and restore the speech WebSocket transport to non-blocking delivery so control events cannot wait behind audio pacing.

**Tech Stack:** Go 1.26, `github.com/coder/websocket` v1.8.15, browser Web Audio/AudioWorklet, ECMAScript modules with Node 22 built-in tests, Python 3.10+ with pytest/pytest-asyncio.

## Global Constraints

- Realtime voice is the only product scope in this pass; PostgreSQL/DeepSeek handoff UX remains later work.
- Capture stays 24 kHz mono PCM16LE in exact 20 ms / 960-byte frames.
- Audio callbacks perform no blocking I/O and avoid per-render-quantum allocation where practical.
- Browser playback schedules only 100--250 ms ahead; every queue and text assembly remains bounded.
- `speech_started` clears reservoir and worklet audio before user-speaking state is rendered.
- Cancelled audio is rejected by a playback generation fence.
- Browser and gateway bearers never enter URLs, assets, storage, responses, or logs.
- The demo BFF and its configured gateway are literal loopback endpoints.
- No npm dependency, production auth mechanism, browser-to-provider connection, or deployment manifest is added.
- Tests never access the physical microphone; audible output and acoustic barge-in remain explicit operator acceptance steps.

---

## File Structure

- `demo/live-client/config.go` — strict loopback listener, origin, and upstream gateway validation.
- `demo/live-client/config_test.go` — accepted/rejected network and secret-reference boundaries.
- `demo/live-client/proxy.go` — bounded bidirectional text relay and content-free close mapping.
- `demo/live-client/proxy_test.go` — both relay directions, frame classes, limits, closes, and bearer privacy.
- `demo/live-client/web/client-state.mjs` — pure session/presentation/transcript lifecycle state.
- `demo/live-client/web/client-state.test.mjs` — retry, mute, playback acknowledgement, transcript bound, and state tests.
- `demo/live-client/web/browser-runtime.mjs` — dependency-injected microphone, AudioContext, worklet, WebSocket, and teardown orchestration.
- `demo/live-client/web/browser-runtime.test.mjs` — fake-boundary Start/Stop/failure/retry tests without a physical device.
- `demo/live-client/web/app.js` — DOM adapter and event wiring only.
- `demo/live-client/web/audio-core.mjs` — bounded capture/playback primitives and generation-aware queues.
- `demo/live-client/web/audio-core.test.mjs` — long irregular resampling, allocation-facing APIs, bounds, pacing, and generation tests.
- `demo/live-client/web/capture-worklet.js` — realtime capture adapter using reusable pipeline state.
- `demo/live-client/web/playback-worklet.js` — allocation-free `pullInto` rendering and generation fence.
- `demo/live-client/web/protocol-state.mjs` — bounded wire event classification.
- `demo/live-client/web/index.html` — visible controls, truthful privacy copy, final-turn live region, audio resume control.
- `demo/live-client/web/styles.css` — responsive controls, muted/buffering states, and accessible contrast.
- `demo/live-client/server.go` — embeds and serves the new ECMAScript modules.
- `demo/live-client/server_test.go` — asset/header coverage for every served module.
- `/Users/chuanmingliu/speech-to-speech/src/speech_to_speech/api/openai_realtime/transports.py` — non-blocking WebSocket audio delivery contract.
- `/Users/chuanmingliu/speech-to-speech/tests/openai_realtime/test_transports.py` — realtime no-sleep and discard contract.
- `README.md` — operator configuration and manual acceptance instructions.

---

### Task 1: Enforce the Literal-Loopback BFF Boundary

**Files:**
- Modify: `demo/live-client/config.go:56-91`
- Modify: `demo/live-client/config_test.go:26-45`

**Interfaces:**
- Consumes: `demoConfig.GatewayURL string`.
- Produces: `validateLoopbackWebSocket(*url.URL) error`, called by `demoConfig.validate`.

- [ ] **Step 1: Write the failing remote-gateway tests**

Add cases proving that plaintext, TLS, DNS, wildcard, and unspecified remote authorities fail while IPv4 and IPv6 loopback literals pass:

```go
func TestParseConfigRequiresLiteralLoopbackGateway(t *testing.T) {
	for _, endpoint := range []string{
		"ws://example.com/v1/realtime",
		"wss://example.com/v1/realtime",
		"ws://localhost:18080/v1/realtime",
		"ws://0.0.0.0:18080/v1/realtime",
		"ws://192.0.2.1:18080/v1/realtime",
	} {
		_, err := parseConfig([]string{"-gateway-url", endpoint})
		require.Error(t, err, endpoint)
	}
	for _, endpoint := range []string{
		"ws://127.0.0.1:18080/v1/realtime",
		"ws://[::1]:18080/v1/realtime",
	} {
		_, err := parseConfig([]string{"-gateway-url", endpoint})
		require.NoError(t, err, endpoint)
	}
}
```

- [ ] **Step 2: Run the focused test and observe RED**

Run: `go test ./demo/live-client -run '^TestParseConfigRequiresLiteralLoopbackGateway$' -count=1`

Expected: FAIL because remote gateway URLs currently pass.

- [ ] **Step 3: Implement literal-loopback authority validation**

Use an explicit numeric host and port; do not resolve DNS:

```go
func validateLoopbackWebSocket(endpoint *url.URL) error {
	host := endpoint.Hostname()
	if host == "" || endpoint.Port() == "" {
		return errors.New("demo gateway URL is invalid")
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return errors.New("demo gateway URL must be loopback")
	}
	return nil
}
```

Call it after the existing URL-shape checks.

- [ ] **Step 4: Run focused and package tests GREEN**

Run: `go test -race -count=1 ./demo/live-client`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add demo/live-client/config.go demo/live-client/config_test.go
git commit -m "fix: restrict demo gateway to loopback"
```

---

### Task 2: Complete Bidirectional Proxy Verification

**Files:**
- Modify: `demo/live-client/proxy_test.go`
- Modify only if a test exposes a defect: `demo/live-client/proxy.go`

**Interfaces:**
- Consumes: `proxyWebSocket(ctx, browser, upstream, maxBytes)` and `safeBrowserClose(error)`.
- Produces: regression coverage for upstream-to-browser text, binary/oversize rejection, normal close, and cancellation.

- [ ] **Step 1: Add upstream-first behavioral tests**

Create a configurable test upstream that can write before reading:

```go
type upstreamAction struct {
	kind    websocket.MessageType
	payload []byte
	close   websocket.StatusCode
}

func TestProxyRelaysUpstreamTextToBrowser(t *testing.T) {
	conn := dialDemoWithUpstreamAction(t, upstreamAction{
		kind: websocket.MessageText, payload: []byte(`{"type":"session.created"}`),
	})
	kind, payload, err := conn.Read(t.Context())
	require.NoError(t, err)
	assert.Equal(t, websocket.MessageText, kind)
	assert.JSONEq(t, `{"type":"session.created"}`, string(payload))
}
```

Add separate tests for upstream binary, `maxBytes+1`, normal closure, and abnormal upstream failure. Assert only bounded browser close reasons.

- [ ] **Step 2: Run the new bidirectional characterization tests**

Run: `go test ./demo/live-client -run '^TestProxy.*Upstream' -count=1`

Expected: PASS because the production relay is intended to be symmetric. If a behavior test fails, preserve that failure as RED, apply the smallest correction to `copyText`/`safeBrowserClose`, and rerun this exact selector before continuing. Do not add payload logging or response-body forwarding.

- [ ] **Step 3: Run the package race gate GREEN**

Run: `go test -race -count=1 ./demo/live-client`

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add demo/live-client/proxy.go demo/live-client/proxy_test.go
git commit -m "test: cover both demo proxy directions"
```

---

### Task 3: Extract Truthful, Bounded Client Presentation State

**Files:**
- Create: `demo/live-client/web/client-state.mjs`
- Create: `demo/live-client/web/client-state.test.mjs`
- Modify: `demo/live-client/web/app.js`
- Modify: `demo/live-client/web/protocol-state.mjs`

**Interfaces:**
- Consumes: protocol effects `{type, text?, value?, encoded?, code?}`.
- Produces: `ClientPresentation`, with `beginSession()`, `endSession()`, `setMuted(boolean)`, `apply(effect)`, `snapshot()`, and `drainAnnouncements()`.
- Produces snapshot shape `{status, muted, turns, announcement, playbackGeneration}`.

- [ ] **Step 1: Write failing lifecycle and bound tests**

```js
test('a retry cannot append to the failed session partial turn', () => {
  const state = new ClientPresentation({maximumTurns: 4});
  state.beginSession();
  state.apply({type: 'user-delta', text: 'old partial'});
  state.endSession('error');
  state.beginSession();
  state.apply({type: 'user-delta', text: 'new'});
  assert.deepEqual(state.snapshot().turns.map((turn) => turn.text), ['old partial', 'new']);
});

test('incoming audio buffers before playback acknowledgement', () => {
  const state = new ClientPresentation({maximumTurns: 4});
  state.beginSession();
  state.apply({type: 'audio-buffered'});
  assert.equal(state.snapshot().status, 'assistant-buffering');
  state.apply({type: 'audio-playing'});
  assert.equal(state.snapshot().status, 'assistant-speaking');
});

test('finalized transcript history evicts the oldest turn', () => {
  const state = new ClientPresentation({maximumTurns: 2});
  state.beginSession();
  for (const text of ['one', 'two', 'three']) {
    state.apply({type: 'user-done', text});
  }
  assert.deepEqual(state.snapshot().turns.map((turn) => turn.text), ['two', 'three']);
});
```

Add separate mute/`aria-pressed`, final-only announcement, clear-before-user-speaking, and end-session tests.

- [ ] **Step 2: Run the new suite and observe RED**

Run: `node --test demo/live-client/web/client-state.test.mjs`

Expected: FAIL because the module does not exist.

- [ ] **Step 3: Implement the minimal pure state object**

```js
export class ClientPresentation {
  constructor({maximumTurns = 100} = {}) {
    if (!Number.isInteger(maximumTurns) || maximumTurns < 1 || maximumTurns > 500) {
      throw new RangeError('invalid transcript bound');
    }
    this.maximumTurns = maximumTurns;
    this.beginSession();
  }

  beginSession() {
    this.status = 'connecting';
    this.muted = false;
    this.active = {user: null, assistant: null};
    this.playbackGeneration = (this.playbackGeneration ?? 0) + 1;
    this.announcement = '';
  }
}
```

Complete effect handling using content-free state. Preserve finalized prior turns, finalize partial view entries on end, and trim after every completed turn.

- [ ] **Step 4: Adapt `app.js` to render snapshots**

Replace global `activeTurns` and early speaking status with controller effects:

```js
presentation.apply({type: 'audio-buffered'});
renderPresentation(presentation.snapshot());

playbackNode.port.onmessage = ({data}) => {
  if (data?.type === 'playing') presentation.apply({type: 'audio-playing'});
  if (data?.type === 'idle') presentation.apply({type: 'audio-idle'});
  renderPresentation(presentation.snapshot());
};
```

Route snapshot rendering through one pending animation frame so a burst of
transcript deltas produces at most one DOM/layout update per display frame:

```js
let renderFrame = null;
function scheduleRender() {
  if (renderFrame !== null) return;
  renderFrame = requestAnimationFrame(() => {
    renderFrame = null;
    renderPresentation(presentation.snapshot());
  });
}
```

- [ ] **Step 5: Run pure browser state suites GREEN**

Run: `node --test demo/live-client/web/client-state.test.mjs demo/live-client/web/protocol-state.test.mjs`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add demo/live-client/web/client-state.mjs demo/live-client/web/client-state.test.mjs demo/live-client/web/app.js demo/live-client/web/protocol-state.mjs
git commit -m "refactor: isolate realtime client state"
```

---

### Task 4: Make Capture and Playback Hot Paths Allocation-Conscious

**Files:**
- Modify: `demo/live-client/web/audio-core.mjs`
- Modify: `demo/live-client/web/audio-core.test.mjs`
- Modify: `demo/live-client/web/capture-worklet.js`
- Modify: `demo/live-client/web/playback-worklet.js`

**Interfaces:**
- Produces: `StreamingPCM16Capture.push(Float32Array): Uint8Array[]`.
- Produces: `PCMPlaybackQueue.pullInto(Float32Array): number`.
- Produces: `PCMPlaybackQueue.append(Int16Array, generation): boolean` and `clear(generation): void`.

- [ ] **Step 1: Write failing long-run capture tests**

```js
test('44.1 kHz irregular capture stays within one output sample over sixty seconds', () => {
  const pipeline = new StreamingPCM16Capture(44100, 24000, 960);
  let outputBytes = 0;
  let offset = 0;
  const inputSamples = 44100 * 60;
  while (offset < inputSamples) {
    const count = Math.min([127, 128, 129, 257][offset % 4], inputSamples - offset);
    for (const frame of pipeline.push(new Float32Array(count))) outputBytes += frame.byteLength;
    offset += count;
  }
  assert.ok(Math.abs((outputBytes / 2) - (24000 * 60)) <= 1);
});
```

Add the same count test for 48 kHz and a reset test proving no stale suffix crosses mute.

- [ ] **Step 2: Write failing `pullInto` and generation tests**

```js
test('playback writes into the provided browser quantum', () => {
  const queue = new PCMPlaybackQueue(48000, 1);
  queue.append(Int16Array.from([0, 32767, 0]), 1);
  const output = new Float32Array(4);
  assert.equal(queue.pullInto(output), 4);
  assert.ok(Math.abs(output[1] - 0.5) < 0.001);
});

test('late audio from a cleared generation is rejected', () => {
  const queue = new PCMPlaybackQueue(24000, 1);
  queue.clear(2);
  assert.equal(queue.append(new Int16Array(240), 1), false);
  assert.equal(queue.queuedSourceSamples, 0);
});
```

- [ ] **Step 3: Run the focused Node suite and observe RED**

Run: `node --test demo/live-client/web/audio-core.test.mjs`

Expected: FAIL because `StreamingPCM16Capture`, `pullInto`, and generation parameters do not exist.

- [ ] **Step 4: Implement reusable capture staging and queue cursors**

Use one fractional resampler state and allocate only transferable completed frames. Replace `segments.shift()` with `headIndex`, compacting only when the consumed prefix exceeds a fixed threshold:

```js
pullInto(output) {
  if (!(output instanceof Float32Array)) throw new TypeError('output must be Float32Array');
  output.fill(0);
  let written = 0;
  while (written < output.length && this.queuedSourceSamples >= 2) {
    output[written] = this.interpolateHead();
    this.advanceSourcePosition();
    written += 1;
  }
  return written;
}
```

The capture pipeline may allocate one 960-byte transferable frame per 20 ms, but not joined arrays per browser render quantum.

- [ ] **Step 5: Update both worklets**

Capture:

```js
this.pipeline = new StreamingPCM16Capture(sampleRate, 24000, 960);
for (const frame of this.pipeline.push(channel)) {
  this.port.postMessage({type: 'frame', pcm: frame.buffer}, [frame.buffer]);
}
```

Playback:

```js
const written = this.queue.pullInto(channels[0]);
for (let index = 1; index < channels.length; index += 1) channels[index].fill(0);
if (written > 0) this.setPlaying(true);
if (this.queue.queuedSourceSamples < 2) this.setPlaying(false);
```

- [ ] **Step 6: Run the Node audio suite GREEN**

Run: `node --test demo/live-client/web/audio-core.test.mjs`

Expected: PASS, including the sixty-second logical sample-count tests without waiting sixty wall-clock seconds.

- [ ] **Step 7: Commit**

```bash
git add demo/live-client/web/audio-core.mjs demo/live-client/web/audio-core.test.mjs demo/live-client/web/capture-worklet.js demo/live-client/web/playback-worklet.js
git commit -m "perf: bound realtime browser audio paths"
```

---

### Task 5: Fence Playback Across Barge-In

**Files:**
- Modify: `demo/live-client/web/app.js`
- Modify: `demo/live-client/web/client-state.mjs`
- Modify: `demo/live-client/web/client-state.test.mjs`
- Modify: `demo/live-client/web/playback-worklet.js`
- Modify: `demo/live-client/web/audio-core.test.mjs`

**Interfaces:**
- Consumes: protocol `clear-playback` effect before `user-speaking`.
- Produces: monotonically increasing integer `playbackGeneration` carried on every `append` and `clear` worklet message.

- [ ] **Step 1: Write the failing delayed-message regression**

```js
test('speech start advances playback generation before user-speaking', () => {
  const state = new ClientPresentation();
  state.beginSession();
  const before = state.snapshot().playbackGeneration;
  state.apply({type: 'clear-playback'});
  state.apply({type: 'status', value: 'user-speaking'});
  const after = state.snapshot();
  assert.equal(after.playbackGeneration, before + 1);
  assert.equal(after.status, 'user-speaking');
});
```

Add a queue-level test that simulates append generation 7, clear generation 8, then a delayed append generation 7.

- [ ] **Step 2: Run both focused suites and observe RED**

Run: `node --test demo/live-client/web/client-state.test.mjs demo/live-client/web/audio-core.test.mjs`

Expected: FAIL because generation is not carried end to end.

- [ ] **Step 3: Thread generation through reservoir and worklet messages**

```js
playbackNode.port.postMessage({
  type: 'append',
  generation: presentation.snapshot().playbackGeneration,
  pcm: samples.buffer,
}, [samples.buffer]);

playbackNode.port.postMessage({type: 'clear', generation});
```

The worklet ignores append messages whose generation differs from its current generation.

- [ ] **Step 4: Run focused suites GREEN**

Run: `node --test demo/live-client/web/client-state.test.mjs demo/live-client/web/audio-core.test.mjs demo/live-client/web/protocol-state.test.mjs`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add demo/live-client/web/app.js demo/live-client/web/client-state.mjs demo/live-client/web/client-state.test.mjs demo/live-client/web/playback-worklet.js demo/live-client/web/audio-core.test.mjs
git commit -m "fix: fence browser playback on barge in"
```

---

### Task 6: Remove Blocking Provider WebSocket Pacing

**Files:**
- Modify: `/Users/chuanmingliu/speech-to-speech/tests/openai_realtime/test_transports.py`
- Modify: `/Users/chuanmingliu/speech-to-speech/src/speech_to_speech/api/openai_realtime/transports.py`

**Interfaces:**
- Consumes: `WebSocketTransport.send_audio_chunk(service, session_id, pcm)`.
- Produces: immediate event encoding/sending; browser pacing remains authoritative.

- [ ] **Step 1: Replace pacing expectations with a failing no-sleep contract**

```python
@pytest.mark.asyncio
async def test_websocket_audio_delivery_never_waits_on_playback_pacing(monkeypatch) -> None:
    transport = WebSocketTransport(object())
    sent: list[list[object]] = []

    async def record(events: list[object]) -> None:
        sent.append(events)

    monkeypatch.setattr(transport, "send_events", record)
    pcm = bytes(6400)

    async def send_twice() -> None:
        await transport.send_audio_chunk(_Service(), "session", pcm)
        await transport.send_audio_chunk(_Service(), "session", pcm)

    await asyncio.wait_for(send_twice(), timeout=0.05)

    assert sent == [[pcm], [pcm]]
```

Keep a discard test asserting it is safe and synchronous, without claiming it cancels browser-buffered audio.

- [ ] **Step 2: Run the focused Python test and observe RED**

Run from `/Users/chuanmingliu/speech-to-speech`:

`uv run pytest -q tests/openai_realtime/test_transports.py`

Expected: FAIL because the second send currently calls the injected sleeper.

- [ ] **Step 3: Remove transport timing state and clarify the contract**

```python
async def send_audio_chunk(self, service: RealtimeService, session_id: str, pcm: bytes) -> None:
    await self.send_events(service.encode_audio_chunk(session_id, pcm))

def discard_pending_audio(self) -> None:
    """Discard transport-owned pending output; WebSocket has none.

    The WebSocket client owns playback buffering and must clear it when the
    server emits speech_started/cancellation events.
    """
```

Remove `asyncio`, `time`, injected clock/sleep, and `_next_audio_at` from this transport.

- [ ] **Step 4: Run focused Python tests GREEN**

Run: `uv run pytest -q tests/openai_realtime/test_transports.py`

Expected: PASS.

- [ ] **Step 5: Commit only the owned speech transport files**

From `/Users/chuanmingliu/speech-to-speech`, first inspect `git diff --` for the two paths. Do not stage `.DS_Store`, `.zshrc`, VAD/TTS, or any other user-owned change.

```bash
git add src/speech_to_speech/api/openai_realtime/transports.py tests/openai_realtime/test_transports.py
git commit -m "fix: keep websocket audio delivery nonblocking"
```

If either target file contains overlapping user changes unrelated to pacing, stop and request direction instead of committing.

---

### Task 7: Add Dependency-Injected Browser Lifecycle Coverage

**Files:**
- Create: `demo/live-client/web/browser-runtime.mjs`
- Create: `demo/live-client/web/browser-runtime.test.mjs`
- Modify: `demo/live-client/web/app.js`
- Modify: `demo/live-client/server.go`
- Modify: `demo/live-client/server_test.go`
- Modify: `Makefile`

**Interfaces:**
- Produces: `createBrowserRuntime(dependencies)` returning `{start(), stop(), mute(boolean), resumeAudio()}`.
- Dependencies: `{mediaDevices, AudioContext, WebSocket, location, presentation, onSnapshot, onNotice}`.

- [ ] **Step 1: Write the failing lifecycle test with real controller logic**

```js
test('start, server acknowledgement, playback, barge-in, and stop release every boundary', async () => {
  const harness = new BrowserHarness();
  const runtime = createBrowserRuntime(harness.dependencies());

  await runtime.start();
  harness.server({type: 'session.created'});
  harness.server({type: 'session.updated'});
  harness.server({type: 'response.output_audio.delta', delta: harness.pcmBase64});
  harness.playback({type: 'playing'});
  harness.server({type: 'input_audio_buffer.speech_started'});
  await runtime.stop();

  assert.equal(harness.sentSessionUpdates, 1);
  assert.ok(harness.messages.some((message) => message.type === 'clear'));
  assert.equal(harness.microphoneTrack.stopped, true);
  assert.equal(harness.captureContext.closed, true);
  assert.equal(harness.playbackContext.closed, true);
  assert.equal(harness.socket.closeCode, 1000);
});
```

Add separate suspended-audio resume, failure/retry, late-generation callback, and double-start idempotency tests. Fakes implement observable behavior; they do not duplicate production state logic.

The suspended-audio test calls `runtime.resumeAudio()` and asserts that the fake
playback context was resumed, exactly one oscillator was started/stopped within
200 ms, and the fake WebSocket sent no additional event.

- [ ] **Step 2: Run the new suite and observe RED**

Run: `node --test demo/live-client/web/browser-runtime.test.mjs`

Expected: FAIL because `browser-runtime.mjs` does not exist.

- [ ] **Step 3: Extract orchestration from `app.js`**

```js
export function createBrowserRuntime(dependencies) {
  const {
    mediaDevices, AudioContext, WebSocket, location,
    presentation, onSnapshot, onNotice,
  } = dependencies;
  // Own generation, media stream, contexts, worklets and socket here.
  return {start, stop, mute, resumeAudio};
}
```

Keep `app.js` responsible only for querying DOM nodes, calling the runtime, and rendering snapshots.

Implement `resumeAudio()` as an explicit user-gesture operation. It resumes the
playback context and plays a locally generated, low-volume 200 ms oscillator
through a gain node; it sends no WebSocket event and stores no samples:

```js
async function resumeAudio() {
  await playbackContext.resume();
  const oscillator = playbackContext.createOscillator();
  const gain = playbackContext.createGain();
  gain.gain.value = 0.03;
  oscillator.connect(gain).connect(playbackContext.destination);
  oscillator.start();
  oscillator.stop(playbackContext.currentTime + 0.2);
}
```

- [ ] **Step 4: Serve and verify the new modules**

Add exact routes/content types for `/client-state.mjs` and `/browser-runtime.mjs`; update the asset table test to include both.

- [ ] **Step 5: Add the new Node suites to the Make target**

```make
test-demo-live-client:
	go test -race -count=1 ./demo/live-client
	node --test demo/live-client/web/*.test.mjs
```

- [ ] **Step 6: Run the complete demo gate GREEN**

Run: `make test-demo-live-client`

Expected: Go race PASS and every Node suite PASS.

- [ ] **Step 7: Commit**

```bash
git add Makefile demo/live-client/server.go demo/live-client/server_test.go demo/live-client/web/app.js demo/live-client/web/browser-runtime.mjs demo/live-client/web/browser-runtime.test.mjs
git commit -m "test: exercise realtime browser lifecycle"
```

---

### Task 8: Correct Realtime UX, Privacy Copy, and Accessibility

**Files:**
- Modify: `demo/live-client/web/index.html`
- Modify: `demo/live-client/web/styles.css`
- Modify: `demo/live-client/web/app.js`
- Modify: `demo/live-client/web/client-state.test.mjs`
- Modify: `demo/live-client/server_test.go`

**Interfaces:**
- Consumes: `ClientPresentation.snapshot()`.
- Produces: controls before transcript, `#final-announcement`, `#resume-audio`, truthful privacy copy, and state-specific rendering.

- [ ] **Step 1: Add failing static and state assertions**

In Go, assert controls occur before the transcript and privacy copy names provider forwarding:

```go
body := response.Body.String()
assert.Less(t, strings.Index(body, `class="controls"`), strings.Index(body, `id="transcript"`))
assert.Contains(t, body, "Audio is forwarded to your configured providers")
assert.Contains(t, body, `id="final-announcement"`)
assert.Contains(t, body, `id="resume-audio"`)
```

In the client-state suite, assert `muted`, `assistant-buffering`, `audio-suspended`, and recovery copy point to enabled actions.

- [ ] **Step 2: Run focused tests and observe RED**

Run:

`go test ./demo/live-client -run '^TestStaticAssets' -count=1`

`node --test demo/live-client/web/client-state.test.mjs`

Expected: FAIL because the new structure/copy/state does not exist.

- [ ] **Step 3: Update markup and rendering**

Move `.controls` immediately after `.console-header`. Add:

```html
<button id="resume-audio" class="button button-secondary" type="button" hidden>
  Enable audio
</button>
<p id="final-announcement" class="visually-hidden" aria-live="polite" aria-atomic="true"></p>
```

Remove `aria-live`/`aria-relevant` from the streaming transcript log and send only finalized turns to `#final-announcement`. Set `aria-pressed` on Mute.

- [ ] **Step 4: Correct responsive layout, state styles, and contrast**

Use a visible/sticky control bar, remove the idle transcript minimum that pushes Start below the fold, and raise footer text to at least `0.75rem` with a color meeting 4.5:1 contrast. Add `.visually-hidden` using the standard clipped accessible pattern.

- [ ] **Step 5: Run focused and complete demo gates GREEN**

Run: `make test-demo-live-client`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add demo/live-client/web/index.html demo/live-client/web/styles.css demo/live-client/web/app.js demo/live-client/web/client-state.test.mjs demo/live-client/server_test.go
git commit -m "fix: make realtime demo states truthful"
```

---

### Task 9: Document Exact Operator Configuration and Acceptance

**Files:**
- Modify: `README.md:66-116`
- Create: `docs/demo/realtime-browser-client.md`
- Create: `demo/live-client/docs_test.go`
- Modify: `skills/gpt-live-go-framework/references/operations.md`

**Interfaces:**
- Consumes: actual demo flags, gateway/speech config fields, and secret environment names.
- Produces: one copyable local startup path, documented secret variable names, exact stop commands, and the manual acceptance checklist.

- [ ] **Step 1: Write a documentation contract check**

Add `demo/live-client/docs_test.go`, which verifies the guide contains the exact required headings and no inline credential assignment:

```go
func TestRealtimeBrowserOperatorGuideHasExactStepsWithoutSecrets(t *testing.T) {
	content, err := os.ReadFile("../../../docs/demo/realtime-browser-client.md")
	require.NoError(t, err)
	text := string(content)
	for _, heading := range []string{
		"1. Configure the speech service",
		"2. Configure the gateway",
		"3. Start the browser BFF",
		"4. Verify audible playback and barge-in",
		"5. Stop only the demo processes",
	} {
		assert.Contains(t, text, heading)
	}
	assert.NotRegexp(t, `(?m)^(S2S_API_KEY|LIVE_DEMO_BEARER|DEEPSEEK_API_KEY)=\S+`, text)
}
```

- [ ] **Step 2: Run the contract and observe RED**

Run: `go test ./demo/live-client -run '^TestRealtimeBrowserOperatorGuideHasExactStepsWithoutSecrets$' -count=1`

Expected: FAIL because the guide does not exist.

- [ ] **Step 3: Write the operator guide**

The guide must distinguish:

```text
You provide:
- speech WebSocket URL/model/API key
- one local gateway static bearer
- browser microphone/speaker permission and device choice

The repository provides:
- 24 kHz/960-byte protocol values
- loopback BFF defaults
- gateway and demo commands
- content-free health and stop checks

Not needed for realtime-only voice:
- DeepSeek API key
- PostgreSQL handoff configuration
- live-worker
```

Use hidden shell prompts or the operator shell environment. Do not suggest tracked `.env`, query tokens, browser storage, or command-line secret arguments.

- [ ] **Step 4: Cross-link README and the framework skill**

Keep the README quick start short and link the detailed guide. Update operations reference routing so future configuration work finds the guide.

- [ ] **Step 5: Run documentation and static gates GREEN**

Run:

`make test-demo-live-client`

`make verify-deploy-static`

`git diff --check`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add README.md docs/demo/realtime-browser-client.md demo/live-client/docs_test.go skills/gpt-live-go-framework/references/operations.md
git commit -m "docs: add realtime browser operator guide"
```

---

### Task 10: Final Verification and Physical Acceptance Handoff

**Files:**
- Modify only if verification exposes a defect: affected implementation/test file, with a new RED test first.
- Record results in the final handoff; do not commit credentials, raw logs, audio, or transcripts.

**Interfaces:**
- Consumes: all prior task outputs.
- Produces: fresh automated evidence plus an operator-only physical checklist.

- [ ] **Step 1: Run focused demo and provider tests**

From the GPT Live worktree:

```bash
make test-demo-live-client
```

From `/Users/chuanmingliu/speech-to-speech`:

```bash
uv run pytest -q tests/openai_realtime/test_transports.py
```

Expected: all tests PASS.

- [ ] **Step 2: Run full Go verification**

```bash
GOTOOLCHAIN=local GOCACHE=/tmp/gpt-live-realtime-demo-go-cache go test -race -count=1 ./...
GOTOOLCHAIN=local GOCACHE=/tmp/gpt-live-realtime-demo-vet-cache go vet ./...
GOTOOLCHAIN=local GOCACHE=/tmp/gpt-live-realtime-demo-build-cache go build ./...
GOTOOLCHAIN=local GOCACHE=/tmp/gpt-live-realtime-demo-tidy-cache go mod tidy -diff
make verify-deploy-static
git diff --check
```

Expected: every command exits zero. Loopback tests may require explicit sandbox permission; do not reinterpret denied binds as code failures.

- [ ] **Step 3: Run privacy and scope scans**

```bash
rg -n 'Authorization|Bearer|api[_-]?key|token|transcript|audio' demo/live-client docs/demo/realtime-browser-client.md
git status --short
```

Review every match. Expected: bearer use is server-only/config-name/test sentinel; transcript/audio matches are protocol/UI code, never persisted content. The index contains only intended commits.

- [ ] **Step 4: Start the configured local services without echoing secrets**

Follow `docs/demo/realtime-browser-client.md`. Start the speech service, gateway, and BFF in separate owned terminals. Verify content-free readiness and speech pool state only.

- [ ] **Step 5: Hand the physical acceptance checklist to the operator**

The operator confirms:

1. Start and microphone permission reach Listening.
2. A spoken phrase yields a finalized user transcript.
3. Assistant transcript appears and audio is physically audible.
4. Speaking over playback stops old audio immediately and starts a fresh turn.
5. Buffering and speaking states match actual playback.
6. Mute stops capture and exposes its state; Unmute restores it.
7. Stop releases microphone, AudioContexts, sockets, and provider pool.
8. Browser-visible URLs, storage, headers, source, and logs contain no bearer.

Do not claim items 3, 4, or 8 until the operator reports them.

- [ ] **Step 6: Record the verified outcome without an empty commit**

If Steps 1--5 expose a new defect, stop this verification task, add a new named RED→GREEN task to this plan with exact files and commands, and execute it before repeating Task 10. If no defect is found, leave the branch unchanged and report the exact command results plus the operator's pending or completed physical checks.

---

### Task 11: Close Independent Final-Review Realtime Gaps

**Files:**
- Modify: `demo/live-client/server.go`, `demo/live-client/server_test.go`
- Modify: `demo/live-client/web/browser-runtime.mjs`, `demo/live-client/web/browser-runtime.test.mjs`
- Modify: `demo/live-client/web/client-state.mjs`, `demo/live-client/web/client-state.test.mjs`
- Modify: `demo/live-client/web/protocol-state.mjs`, `demo/live-client/web/protocol-state.test.mjs`
- Modify: `demo/live-client/web/playback-worklet.js`, `demo/live-client/web/audio-core.test.mjs`
- Modify: `docs/demo/realtime-browser-client.md`, `demo/live-client/docs_test.go`

**Interfaces:**
- Produces a no-redirect upstream WebSocket dialer that cannot forward the bearer beyond the validated literal-loopback endpoint.
- Produces bounded browser microphone egress, stable-ID transcript attribution with per-turn and aggregate byte limits, sequenced playback acknowledgements, generation-fenced establishment deadlines, and capture-device/context lifecycle failure handling.
- Produces exact content-free provider-pool/session-idle and browser bearer-inspection operator checks.

- [x] **Step 1: Add focused RED regressions**

Add tests proving: a loopback redirect target is never contacted or given the bearer; a permanently backed-up browser socket fails closed; interleaved transcript IDs remain separate and bounded after more than 64 IDs; stale playback acknowledgements cannot cross barge-in or a newer append revision; open/`session.created`/`session.updated` deadlines tear down every resource with distinct notices; capture suspension, microphone removal, and permission revocation are generation-fenced and release resources; non-microphone initialization failures are not mislabeled; and the operator guide contains exact content-free pool/idle and DevTools bearer checks.

Run the focused Go and Node selectors and confirm every new assertion fails for the missing behavior before modifying production code.

- [x] **Step 2: Implement the smallest bounded security and realtime corrections**

Use a dedicated no-redirect HTTP client for `websocket.Dial`; enforce a small explicit `bufferedAmount` budget before every microphone send; retain `item_id`/`response_id` in protocol effects and key presentation turns by stable ID under explicit per-turn and aggregate UTF-8 byte budgets; carry playback generation plus monotonically increasing queue revision through worklet acknowledgements; add bounded, generation-fenced socket/session establishment timers; observe capture `AudioContext.statechange` and microphone track `ended`; and map initialization errors by stage without exposing content.

- [x] **Step 3: Complete the operator contract**

Document exact content-free provider-pool/session-idle commands and an explicit browser DevTools URL/source/network/storage/log inspection that confirms the bearer is absent. Lock the required commands and surfaces in `docs_test.go`.

- [x] **Step 4: Run GREEN gates**

Run all focused tests, `make test-demo-live-client`, relevant Go race tests, `make verify-deploy-static`, `git diff --check`, and the scope/privacy scan. Record exact RED/GREEN evidence in the task report.
