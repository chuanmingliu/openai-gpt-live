import test from 'node:test';
import assert from 'node:assert/strict';

import {createBrowserRuntime} from './browser-runtime.mjs';
import {ClientPresentation} from './client-state.mjs';

const SCHEDULED_PCM_BASE64 = Buffer.alloc(16000).toString('base64');
const DEFAULT_TOOL_POLICY = {
	web_search: {enabled: true, safe_search: 'strict', region: 'wt-wt', max_results: 5, timeout_ms: 8000},
};

class FakeGraphNode {
  constructor() {
    this.connections = [];
    this.disconnected = false;
  }

  connect(target) {
    this.connections.push(target);
    return target;
  }

  disconnect() {
    this.disconnected = true;
  }
}

class FakeAudioWorkletNode extends FakeGraphNode {
  constructor(context, name) {
    super();
    this.context = context;
    this.name = name;
    this.port = {
      messages: [],
      onmessage: null,
      postMessage: (message) => {
        this.port.messages.push(message);
        context.harness.timeline.push({kind: 'worklet-message', message});
      },
    };
    context.harness.workletNodes.push(this);
  }
}

globalThis.AudioWorkletNode = FakeAudioWorkletNode;

class BrowserHarness {
  constructor({getUserMedia, socketReadyState = 1, failWorkletModule = false, captureResumeFailure = false, fetchHandler} = {}) {
    this.contexts = [];
    this.getUserMediaCalls = 0;
    this.gains = [];
    this.mediaSources = [];
    this.mediaStreams = [];
    this.notices = [];
    this.oscillators = [];
    this.requests = [];
    this.sent = [];
    this.snapshots = [];
    this.sockets = [];
    this.timeline = [];
    this.timers = [];
    this.workletNodes = [];
    this.getUserMedia = getUserMedia ?? (() => this.makeMediaStream());
    this.socketReadyState = socketReadyState;
    this.failWorkletModule = failWorkletModule;
    this.captureResumeFailure = captureResumeFailure;
    this.captureResumePending = false;
    this.fetchHandler = fetchHandler ?? (async (path) => ({
      ok: true,
      status: 200,
      json: async () => path === '/api/tool-policy' ? structuredClone(DEFAULT_TOOL_POLICY) : {ok: true, result_count: 1},
    }));
    this.fetch = async (path, options = {}) => {
      this.requests.push({path, options});
      return this.fetchHandler(path, options);
    };
    this.presentation = new ClientPresentation();
    this.presentation.endSession('idle');

    const harness = this;
    this.AudioContext = class {
      constructor(options = {}) {
        this.harness = harness;
        this.options = options;
        this.currentTime = 3;
        this.state = 'suspended';
        this.destination = new FakeGraphNode();
        this.closed = false;
        this.resumeCalls = 0;
        this.listeners = new Map();
        this.audioWorklet = {
          addModule: async (path) => {
            if (harness.failWorkletModule) throw new Error('module unavailable');
            this.modulePath = path;
          },
        };
        harness.contexts.push(this);
        if (options.sampleRate === 24000) harness.playbackContext = this;
        else harness.captureContext = this;
      }

      createMediaStreamSource(stream) {
        const source = new FakeGraphNode();
        source.stream = stream;
        harness.mediaSources.push(source);
        return source;
      }

      createGain() {
        const gain = new FakeGraphNode();
        gain.gain = {value: 1};
        harness.gains.push(gain);
        return gain;
      }

      createOscillator() {
        const oscillator = new FakeGraphNode();
        oscillator.started = 0;
        oscillator.stopped = 0;
        oscillator.start = (when = this.currentTime) => {
          oscillator.started += 1;
          oscillator.startTime = when;
        };
        oscillator.stop = (when) => {
          oscillator.stopped += 1;
          oscillator.stopTime = when;
        };
        harness.oscillators.push(oscillator);
        return oscillator;
      }

      async resume() {
        this.resumeCalls += 1;
        if (this === harness.captureContext && harness.captureResumeFailure) {
          throw new Error('capture resume unavailable');
        }
        if (this === harness.captureContext && harness.captureResumePending) {
          return new Promise(() => {});
        }
        this.setState('running');
      }

      addEventListener(type, callback) {
        const listeners = this.listeners.get(type) ?? [];
        listeners.push(callback);
        this.listeners.set(type, listeners);
      }

      setState(state) {
        this.state = state;
        for (const callback of this.listeners.get('statechange') ?? []) callback();
      }

      async close() {
        this.closed = true;
        this.setState('closed');
      }
    };

    this.WebSocket = class {
      static CONNECTING = 0;
      static OPEN = 1;

      constructor(url) {
        this.url = url;
        this.readyState = harness.socketReadyState;
        this.bufferedAmount = 0;
        this.listeners = new Map();
        this.sent = [];
        this.closeCode = null;
        harness.sockets.push(this);
        harness.socket = this;
      }

      addEventListener(type, callback, options = {}) {
        const listeners = this.listeners.get(type) ?? [];
        listeners.push({callback, once: options.once === true});
        this.listeners.set(type, listeners);
      }

      send(raw) {
        const message = JSON.parse(raw);
        this.sent.push(message);
        harness.sent.push(message);
      }

      close(code, reason) {
        this.closeCode = code;
        this.closeReason = reason;
        this.readyState = 3;
      }

      emit(type, event = {}) {
        const listeners = this.listeners.get(type) ?? [];
        this.listeners.set(type, listeners.filter(({once}) => !once));
        for (const {callback} of listeners) callback(event);
      }
    };

    this.mediaDevices = {
      getUserMedia: async (constraints) => {
        this.getUserMediaCalls += 1;
        this.mediaConstraints = constraints;
        return this.getUserMedia(constraints);
      },
    };
  }

  dependencies() {
    return {
      mediaDevices: this.mediaDevices,
      AudioContext: this.AudioContext,
      WebSocket: this.WebSocket,
      fetch: this.fetch,
      location: {protocol: 'http:', host: '127.0.0.1:3000'},
      presentation: this.presentation,
      onSnapshot: (snapshot) => {
        this.snapshots.push(snapshot);
        this.timeline.push({kind: 'snapshot', snapshot});
      },
      onNotice: (notice) => this.notices.push(notice),
    };
  }

  makeMediaStream() {
    const track = {
      stopped: false,
      listeners: new Map(),
      addEventListener(type, callback, options = {}) {
        const listeners = this.listeners.get(type) ?? [];
        listeners.push({callback, once: options.once === true});
        this.listeners.set(type, listeners);
      },
      emit(type) {
        const listeners = this.listeners.get(type) ?? [];
        this.listeners.set(type, listeners.filter(({once}) => !once));
        for (const {callback} of listeners) callback();
      },
      stop() {
        this.stopped = true;
      },
    };
    const stream = {getTracks: () => [track]};
    this.mediaStreams.push(stream);
    this.microphoneTrack = track;
    return stream;
  }

  installFakeTimers() {
    this.originalSetTimeout = globalThis.setTimeout;
    this.originalClearTimeout = globalThis.clearTimeout;
    globalThis.setTimeout = (callback, delay) => {
      const timer = {callback, delay, cleared: false, fired: false};
      this.timers.push(timer);
      return timer;
    };
    globalThis.clearTimeout = (timer) => {
      timer.cleared = true;
    };
  }

  restoreTimers() {
    globalThis.setTimeout = this.originalSetTimeout;
    globalThis.clearTimeout = this.originalClearTimeout;
  }

  fireTimers({includeCleared = false} = {}) {
    for (const timer of [...this.timers]) {
      if (timer.fired || (timer.cleared && !includeCleared)) continue;
      timer.fired = true;
      timer.callback();
    }
  }

  fireNextTimer() {
    const timer = this.activeTimers[0];
    assert.ok(timer, 'expected one active timer');
    timer.fired = true;
    timer.callback();
  }

  server(event, socket = this.socket) {
    socket.emit('message', {data: JSON.stringify(event)});
  }

  capture(event, node = this.captureNode) {
    node.port.onmessage({data: event});
  }

  playback(event, node = this.playbackNode) {
    node.port.onmessage({data: event});
  }

  get captureNode() {
    return [...this.workletNodes].reverse().find(({name}) => name === 'live-capture');
  }

  get playbackNode() {
    return [...this.workletNodes].reverse().find(({name}) => name === 'live-playback');
  }

  get messages() {
    return this.workletNodes.flatMap((node) => node.port.messages);
  }

  get sentSessionUpdates() {
    return this.sent.filter(({type}) => type === 'session.update').length;
  }

  get activeTimers() {
    return this.timers.filter(({cleared, fired}) => !cleared && !fired);
  }
}

test('accepted session sends current policy once per socket and reconnect resends it', async (t) => {
  const harness = new BrowserHarness();
  harness.installFakeTimers();
  t.after(() => harness.restoreTimers());
  const runtime = createBrowserRuntime(harness.dependencies());
  await runtime.start();
  harness.server({type: 'session.created'});
  harness.server({type: 'session.updated'});
  await new Promise((resolve) => setImmediate(resolve));

  let updates = harness.sent.filter(({type}) => type === 'live.tool_policy.update');
  assert.equal(updates.length, 1);
  assert.deepEqual(updates[0].tools, DEFAULT_TOOL_POLICY);
  harness.server({
    type: 'live.tool_policy.updated', event_id: updates[0].event_id,
    tools: DEFAULT_TOOL_POLICY, model: 'deepseek-v4-flash',
  });
  assert.equal(harness.presentation.snapshot().tools.saveStatus, 'saved');
  assert.equal(harness.activeTimers.length, 0);

  harness.server({type: 'session.updated'});
  assert.equal(harness.sent.filter(({type}) => type === 'live.tool_policy.update').length, 1);
  assert.equal(harness.activeTimers.length, 0);
  await runtime.stop();
  await runtime.start();
  const second = harness.socket;
  harness.server({type: 'session.created'}, second);
  harness.server({type: 'session.updated'}, second);
  await new Promise((resolve) => setImmediate(resolve));
  updates = second.sent.filter(({type}) => type === 'live.tool_policy.update');
  assert.equal(updates.length, 1);
  assert.deepEqual(updates[0].tools, DEFAULT_TOOL_POLICY);
  await runtime.stop();
});

test('capture stays closed until delayed restored policy receives its exact acknowledgement', async () => {
	let resolvePolicy;
	const restored = structuredClone(DEFAULT_TOOL_POLICY);
	restored.web_search.enabled = false;
	const harness = new BrowserHarness({fetchHandler: async (path) => {
		if (path === '/api/tool-policy') return new Promise((resolve) => { resolvePolicy = resolve; });
		return {ok: true, json: async () => ({ok: true})};
	}});
	const runtime = createBrowserRuntime(harness.dependencies());
	await runtime.start();
	harness.server({type: 'session.created'});
	harness.server({type: 'session.updated'});
	harness.capture({type: 'frame', pcm: new ArrayBuffer(960)});
	assert.equal(harness.sent.filter(({type}) => type === 'input_audio_buffer.append').length, 0);
	assert.equal(harness.presentation.snapshot().status, 'configuring');

	resolvePolicy({ok: true, json: async () => restored});
	await new Promise((resolve) => setImmediate(resolve));
	const update = harness.sent.find(({type}) => type === 'live.tool_policy.update');
	assert.deepEqual(update.tools, restored);
	harness.capture({type: 'frame', pcm: new ArrayBuffer(960)});
	assert.equal(harness.sent.filter(({type}) => type === 'input_audio_buffer.append').length, 0);

	harness.server({type: 'live.tool_policy.updated', event_id: update.event_id, tools: restored, model: 'deepseek-v4-flash'});
	harness.capture({type: 'frame', pcm: new ArrayBuffer(960)});
	assert.equal(harness.sent.filter(({type}) => type === 'input_audio_buffer.append').length, 1);
	assert.equal(harness.presentation.snapshot().status, 'listening');
	await runtime.stop();
});

test('failed policy restore sends disabled fail-closed policy and resumes only after exact ack', async () => {
	const harness = new BrowserHarness({fetchHandler: async (path) => {
		if (path === '/api/tool-policy') throw new Error('private restore failure');
		return {ok: true, json: async () => ({ok: true})};
	}});
	const runtime = createBrowserRuntime(harness.dependencies());
	await runtime.start();
	harness.server({type: 'session.created'});
	harness.server({type: 'session.updated'});
	await new Promise((resolve) => setImmediate(resolve));
	const update = harness.sent.find(({type}) => type === 'live.tool_policy.update');
	assert.deepEqual(update.tools, {
		web_search: {enabled: false, safe_search: 'strict', region: 'wt-wt', max_results: 5, timeout_ms: 8000},
	});

	harness.server({type: 'live.tool_policy.updated', event_id: update.event_id, tools: DEFAULT_TOOL_POLICY, model: 'deepseek-v4-flash'});
	harness.capture({type: 'frame', pcm: new ArrayBuffer(960)});
	assert.equal(harness.sent.filter(({type}) => type === 'input_audio_buffer.append').length, 0);
	assert.equal(harness.presentation.snapshot().status, 'configuring');

	harness.server({type: 'live.tool_policy.updated', event_id: update.event_id, tools: update.tools, model: 'deepseek-v4-flash'});
	harness.capture({type: 'frame', pcm: new ArrayBuffer(960)});
	assert.equal(harness.sent.filter(({type}) => type === 'input_audio_buffer.append').length, 1);
	assert.equal(harness.presentation.snapshot().status, 'listening');
	await runtime.stop();
});

test('a new-session restore failure cannot resend a previously enabled policy or open capture before disabled ack', async () => {
  let policyLoads = 0;
  const harness = new BrowserHarness({fetchHandler: async (path) => {
    if (path !== '/api/tool-policy') return {ok: true, json: async () => ({ok: true})};
    policyLoads += 1;
    if (policyLoads === 1) return {ok: true, json: async () => structuredClone(DEFAULT_TOOL_POLICY)};
    throw new Error('current session restore failure');
  }});
  const runtime = createBrowserRuntime(harness.dependencies());

  await runtime.start();
  harness.server({type: 'session.created'});
  harness.server({type: 'session.updated'});
  await new Promise((resolve) => setImmediate(resolve));
  const enabledUpdate = harness.socket.sent.find(({type}) => type === 'live.tool_policy.update');
  assert.deepEqual(enabledUpdate.tools, DEFAULT_TOOL_POLICY);
  harness.server({
    type: 'live.tool_policy.updated', event_id: enabledUpdate.event_id,
    tools: enabledUpdate.tools, model: 'deepseek-v4-flash',
  });
  await runtime.stop();

  await runtime.start();
  const second = harness.socket;
  harness.server({type: 'session.created'}, second);
  harness.server({type: 'session.updated'}, second);
  await new Promise((resolve) => setImmediate(resolve));
  const updates = second.sent.filter(({type}) => type === 'live.tool_policy.update');
  assert.equal(updates.length, 1);
  assert.deepEqual(updates[0].tools, {
    web_search: {enabled: false, safe_search: 'strict', region: 'wt-wt', max_results: 5, timeout_ms: 8000},
  });
  assert.equal(updates.some(({tools}) => tools.web_search.enabled), false);

  harness.capture({type: 'frame', pcm: new ArrayBuffer(960)});
  assert.equal(second.sent.filter(({type}) => type === 'input_audio_buffer.append').length, 0);
  harness.server({
    type: 'live.tool_policy.updated', event_id: updates[0].event_id,
    tools: DEFAULT_TOOL_POLICY, model: 'deepseek-v4-flash',
  }, second);
  harness.capture({type: 'frame', pcm: new ArrayBuffer(960)});
  assert.equal(second.sent.filter(({type}) => type === 'input_audio_buffer.append').length, 0);

  harness.server({
    type: 'live.tool_policy.updated', event_id: updates[0].event_id,
    tools: updates[0].tools, model: 'deepseek-v4-flash',
  }, second);
  harness.capture({type: 'frame', pcm: new ArrayBuffer(960)});
  assert.equal(second.sent.filter(({type}) => type === 'input_audio_buffer.append').length, 1);
  await runtime.stop();
});

test('gateway canonical SafeSearch tightening activates the exact saved policy', async () => {
	const requested = structuredClone(DEFAULT_TOOL_POLICY);
	requested.web_search.safe_search = 'moderate';
	const harness = new BrowserHarness();
	const runtime = createBrowserRuntime(harness.dependencies());
	await runtime.start();
	harness.server({type: 'session.created'});
	harness.server({type: 'session.updated'});
	await new Promise((resolve) => setImmediate(resolve));
	let update = harness.sent.filter(({type}) => type === 'live.tool_policy.update').at(-1);
	harness.server({type: 'live.tool_policy.updated', event_id: update.event_id, tools: update.tools, model: 'deepseek-v4-flash'});

	await runtime.saveToolPolicy(requested);
	update = harness.sent.filter(({type}) => type === 'live.tool_policy.update').at(-1);
	assert.deepEqual(update.tools, requested);
	harness.server({type: 'live.tool_policy.updated', event_id: update.event_id, tools: DEFAULT_TOOL_POLICY, model: 'deepseek-v4-flash'});
	assert.equal(harness.presentation.snapshot().tools.saveStatus, 'saved');
	assert.deepEqual(harness.presentation.snapshot().tools.policy, DEFAULT_TOOL_POLICY);
	await runtime.stop();
});

test('policy load save and diagnostic test use same-origin no-store bounded requests and stable statuses', async () => {
  const disabled = structuredClone(DEFAULT_TOOL_POLICY);
  disabled.web_search.enabled = false;
  const harness = new BrowserHarness();
  const runtime = createBrowserRuntime(harness.dependencies());
  await runtime.start();
  harness.server({type: 'session.created'});
  harness.server({type: 'session.updated'});
  await new Promise((resolve) => setImmediate(resolve));

  await runtime.saveToolPolicy(disabled);
  const put = harness.requests.find(({path, options}) => path === '/api/tool-policy' && options.method === 'PUT');
  assert.ok(put);
  assert.equal(put.options.cache, 'no-store');
  assert.equal(put.options.headers['Content-Type'], 'application/json');
  assert.deepEqual(JSON.parse(put.options.body), disabled);
  assert.equal(put.path.includes('?'), false);
  assert.equal(put.path.includes('#'), false);
  assert.equal(harness.presentation.snapshot().tools.saveStatus, 'awaiting-ack');

  const sent = harness.sent.filter(({type}) => type === 'live.tool_policy.update').at(-1);
  harness.server({type: 'live.tool_policy.updated', event_id: sent.event_id, tools: disabled, model: 'deepseek-v4-flash'});
  assert.equal(harness.presentation.snapshot().tools.saveStatus, 'saved');

  await runtime.testToolPolicy();
  const diagnostic = harness.requests.find(({path, options}) => path === '/api/tool-policy/test' && options.method === 'POST');
  assert.ok(diagnostic);
  assert.equal(diagnostic.options.cache, 'no-store');
  assert.equal(diagnostic.path.includes('?'), false);
  assert.equal(harness.presentation.snapshot().tools.testStatus, 'passed');
  await runtime.stop();
});

test('policy request failures expose only stable state and never private response content', async () => {
  const harness = new BrowserHarness({fetchHandler: async (path, options) => {
    if (path === '/api/tool-policy' && options.method === 'GET') return {ok: true, json: async () => structuredClone(DEFAULT_TOOL_POLICY)};
    throw new Error('private query, credential, and provider body');
  }});
  const runtime = createBrowserRuntime(harness.dependencies());
  await runtime.start();
  await runtime.saveToolPolicy(DEFAULT_TOOL_POLICY);
  await runtime.testToolPolicy();
  const serialized = JSON.stringify(harness.presentation.snapshot());
  assert.equal(harness.presentation.snapshot().tools.saveStatus, 'error');
  assert.equal(harness.presentation.snapshot().tools.testStatus, 'error');
  assert.doesNotMatch(serialized, /private|credential|provider body/);
  await runtime.stop();
});

test('overlapping saves serialize persistence and leave BFF browser and gateway on the latest accepted policy', async () => {
  const first = structuredClone(DEFAULT_TOOL_POLICY);
  first.web_search.region = 'uk-en';
  const second = structuredClone(DEFAULT_TOOL_POLICY);
  second.web_search.region = 'cn-zh';
  let resolveFirst;
  let resolveSecond;
  const firstResponse = new Promise((resolve) => { resolveFirst = resolve; });
  const secondResponse = new Promise((resolve) => { resolveSecond = resolve; });
  let putCount = 0;
  const harness = new BrowserHarness({fetchHandler: async (path, options) => {
    if (path === '/api/tool-policy' && options.method === 'GET') return {ok: true, json: async () => structuredClone(DEFAULT_TOOL_POLICY)};
    if (path === '/api/tool-policy' && options.method === 'PUT') return putCount++ === 0 ? firstResponse : secondResponse;
    return {ok: true, json: async () => ({ok: true})};
  }});
  const runtime = createBrowserRuntime(harness.dependencies());
  await runtime.start();
  harness.server({type: 'session.created'});
  harness.server({type: 'session.updated'});
  await new Promise((resolve) => setImmediate(resolve));

  const saveFirst = runtime.saveToolPolicy(first);
  await new Promise((resolve) => setImmediate(resolve));
  const saveSecond = runtime.saveToolPolicy(second);
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(harness.requests.filter(({options}) => options.method === 'PUT').length, 1);

  resolveFirst({ok: true});
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(harness.requests.filter(({options}) => options.method === 'PUT').length, 2);
  resolveSecond({ok: true});
  await Promise.all([saveFirst, saveSecond]);

  const puts = harness.requests.filter(({options}) => options.method === 'PUT').map(({options}) => JSON.parse(options.body));
  assert.deepEqual(puts, [first, second]);
  const updates = harness.sent.filter(({type}) => type === 'live.tool_policy.update');
  assert.deepEqual(updates.slice(-2).map(({tools}) => tools), [first, second]);
  assert.deepEqual(harness.presentation.snapshot().tools.policy, second);

  harness.server({
    type: 'live.tool_policy.updated', event_id: updates.at(-2).event_id,
    tools: second, model: 'deepseek-v4-flash',
  });
  assert.equal(harness.presentation.snapshot().tools.saveStatus, 'awaiting-ack');
  harness.server({
    type: 'live.tool_policy.updated', event_id: updates.at(-1).event_id,
    tools: second, model: 'deepseek-v4-flash',
  });
  assert.equal(harness.presentation.snapshot().tools.saveStatus, 'saved');
  await runtime.stop();
});

test('session acceptance during a failed save sends only the last persisted policy', async () => {
  const draft = structuredClone(DEFAULT_TOOL_POLICY);
  draft.web_search.enabled = false;
  let rejectSave;
  const failedResponse = new Promise((_resolve, reject) => { rejectSave = reject; });
  const harness = new BrowserHarness({fetchHandler: async (path, options) => {
    if (path === '/api/tool-policy' && options.method === 'GET') return {ok: true, json: async () => structuredClone(DEFAULT_TOOL_POLICY)};
    if (path === '/api/tool-policy' && options.method === 'PUT') return failedResponse;
    return {ok: true, json: async () => ({ok: true})};
  }});
  const runtime = createBrowserRuntime(harness.dependencies());
  await runtime.start();
  const save = runtime.saveToolPolicy(draft);
  await new Promise((resolve) => setImmediate(resolve));
  harness.server({type: 'session.created'});
  harness.server({type: 'session.updated'});
  await new Promise((resolve) => setImmediate(resolve));

  const updates = harness.sent.filter(({type}) => type === 'live.tool_policy.update');
  assert.equal(updates.length, 1);
  assert.deepEqual(updates[0].tools, DEFAULT_TOOL_POLICY);
  rejectSave(new Error('private failure body'));
  await save;
  assert.deepEqual(harness.presentation.snapshot().tools.policy, DEFAULT_TOOL_POLICY);
  assert.equal(harness.presentation.snapshot().tools.saveStatus, 'error');
  await runtime.stop();
});

test('Test completion success or failure cannot mask a later no-socket Save', async () => {
  for (const passed of [true, false]) {
    let resolveTest;
    const testResponse = new Promise((resolve) => { resolveTest = resolve; });
    const saved = structuredClone(DEFAULT_TOOL_POLICY);
    saved.web_search.region = passed ? 'uk-en' : 'cn-zh';
    const harness = new BrowserHarness({fetchHandler: async (path, options) => {
      if (path === '/api/tool-policy/test' && options.method === 'POST') return testResponse;
      if (path === '/api/tool-policy' && options.method === 'PUT') return {ok: true};
      return {ok: true, json: async () => structuredClone(DEFAULT_TOOL_POLICY)};
    }});
    const runtime = createBrowserRuntime(harness.dependencies());

    const testing = runtime.testToolPolicy();
    await new Promise((resolve) => setImmediate(resolve));
    await runtime.saveToolPolicy(saved);
    assert.equal(harness.presentation.snapshot().tools.lastOperation, 'save');
    assert.equal(harness.presentation.snapshot().tools.saveStatus, 'saved');

    resolveTest({ok: passed});
    await testing;
    assert.equal(harness.presentation.snapshot().tools.testStatus, passed ? 'passed' : 'error');
    assert.equal(harness.presentation.snapshot().tools.lastOperation, 'save');
    assert.equal(harness.presentation.snapshot().tools.saveStatus, 'saved');
  }
});

test('late initial GET success cannot replace a no-socket persisted Save', async () => {
  let resolveGET;
  let resolveMedia;
  const getResponse = new Promise((resolve) => { resolveGET = resolve; });
  const media = new Promise((resolve) => { resolveMedia = resolve; });
  const saved = structuredClone(DEFAULT_TOOL_POLICY);
  saved.web_search.region = 'uk-en';
  const harness = new BrowserHarness({
    getUserMedia: () => media,
    fetchHandler: async (path, options) => {
      if (path === '/api/tool-policy' && options.method === 'GET') return getResponse;
      if (path === '/api/tool-policy' && options.method === 'PUT') return {ok: true};
      return {ok: true, json: async () => ({ok: true})};
    },
  });
  const runtime = createBrowserRuntime(harness.dependencies());
  const starting = runtime.start();
  await new Promise((resolve) => setImmediate(resolve));
  await runtime.saveToolPolicy(saved);
  assert.equal(harness.sockets.length, 0);

  resolveGET({ok: true, json: async () => structuredClone(DEFAULT_TOOL_POLICY)});
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(harness.presentation.snapshot().tools.policy, saved);
  assert.equal(harness.presentation.snapshot().tools.saveStatus, 'saved');

  resolveMedia(harness.makeMediaStream());
  await starting;
  await runtime.stop();
});

test('late initial GET failure cannot fail a no-socket persisted Save', async () => {
  let rejectGET;
  let resolveMedia;
  const getResponse = new Promise((_resolve, reject) => { rejectGET = reject; });
  const media = new Promise((resolve) => { resolveMedia = resolve; });
  const saved = structuredClone(DEFAULT_TOOL_POLICY);
  saved.web_search.region = 'uk-en';
  const harness = new BrowserHarness({
    getUserMedia: () => media,
    fetchHandler: async (path, options) => {
      if (path === '/api/tool-policy' && options.method === 'GET') return getResponse;
      if (path === '/api/tool-policy' && options.method === 'PUT') return {ok: true};
      return {ok: true, json: async () => ({ok: true})};
    },
  });
  const runtime = createBrowserRuntime(harness.dependencies());
  const starting = runtime.start();
  await new Promise((resolve) => setImmediate(resolve));
  await runtime.saveToolPolicy(saved);
  assert.equal(harness.sockets.length, 0);

  rejectGET(new Error('stale load failure'));
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(harness.presentation.snapshot().tools.policy, saved);
  assert.equal(harness.presentation.snapshot().tools.saveStatus, 'saved');

  resolveMedia(harness.makeMediaStream());
  await starting;
  await runtime.stop();
});

test('handoff progress updates no playback worklet state and leaves voice status unchanged', async () => {
  const harness = new BrowserHarness();
  const runtime = createBrowserRuntime(harness.dependencies());
  await runtime.start();
  harness.server({type: 'session.created'});
  harness.server({type: 'session.updated'});
  await new Promise((resolve) => setImmediate(resolve));
  const beforeMessages = harness.playbackNode.port.messages.length;
  const beforeStatus = harness.presentation.snapshot().status;

  harness.server({
    type: 'live.handoff.progress', event_id: 'event-2', handoff_id: 'handoff-1', sequence_id: 2,
    status: 'running', details: {phase: 'searching', summary: 'Searching the web.'},
  });

  assert.equal(harness.playbackNode.port.messages.length, beforeMessages);
  assert.equal(harness.presentation.snapshot().status, beforeStatus);
  assert.equal(harness.presentation.snapshot().timeline.at(-1).kind, 'tool');
  await runtime.stop();
});

test('active tool-card pressure fails the session with content-free state intact', async () => {
	const harness = new BrowserHarness();
	const runtime = createBrowserRuntime(harness.dependencies());
	await runtime.start();
	harness.server({type: 'session.created'});
	harness.server({type: 'session.updated'});
	await new Promise((resolve) => setImmediate(resolve));
	const policyUpdate = harness.sent.find(({type}) => type === 'live.tool_policy.update');
	harness.server({type: 'live.tool_policy.updated', event_id: policyUpdate.event_id, tools: policyUpdate.tools, model: 'deepseek-v4-flash'});
	for (let index = 0; index < 65; index += 1) {
		harness.server({
			type: 'live.handoff.created', handoff_id: `pressure-${index}`, sequence_id: index + 1,
			status: 'pending', details: {},
		});
	}
	await new Promise((resolve) => setImmediate(resolve));
	assert.equal(harness.notices.at(-1), 'tool-state-overflow');
	assert.equal(harness.presentation.snapshot().status, 'error');
	assert.equal(harness.presentation.snapshot().tools.cards.length, 64);
	assert.equal(harness.presentation.snapshot().tools.cards.some(({id}) => id === 'pressure-0'), true);
	assert.doesNotMatch(JSON.stringify(harness.presentation.snapshot()), /private|query|snippet|provider/);
});

test('start gates configuration and PCM, playback acknowledgement, barge-in, and stop release every boundary', async (t) => {
  const harness = new BrowserHarness();
  harness.installFakeTimers();
  t.after(() => harness.restoreTimers());
  const runtime = createBrowserRuntime(harness.dependencies());

  await runtime.start();
  assert.equal(harness.sentSessionUpdates, 0);

  harness.server({type: 'session.created'});
  assert.equal(harness.sentSessionUpdates, 1);

  harness.capture({type: 'frame', pcm: new ArrayBuffer(960)});
  assert.equal(harness.sent.filter(({type}) => type === 'input_audio_buffer.append').length, 0);

	harness.server({type: 'session.updated'});
	await new Promise((resolve) => setImmediate(resolve));
	const policyUpdate = harness.sent.find(({type}) => type === 'live.tool_policy.update');
	harness.server({type: 'live.tool_policy.updated', event_id: policyUpdate.event_id, tools: policyUpdate.tools, model: 'deepseek-v4-flash'});
	harness.capture({type: 'frame', pcm: new ArrayBuffer(960)});
	assert.equal(harness.sent.filter(({type}) => type === 'input_audio_buffer.append').length, 1);

  const playbackGeneration = harness.presentation.snapshot().playbackGeneration;
  harness.server({type: 'response.output_audio.delta', delta: SCHEDULED_PCM_BASE64});
  assert.equal(harness.snapshots.at(-1).status, 'assistant-buffering');
  assert.equal(harness.snapshots.some(({status}) => status === 'assistant-speaking'), false);
  const appendMessage = harness.playbackNode.port.messages.find(({type}) => type === 'append');
  assert.equal(appendMessage.generation, playbackGeneration);
  assert.equal(harness.activeTimers.length, 1);

  harness.playback({type: 'playing', generation: appendMessage.generation, revision: appendMessage.revision});
  assert.equal(harness.snapshots.at(-1).status, 'assistant-speaking');

  const beforeBarge = harness.timeline.length;
  harness.server({type: 'input_audio_buffer.speech_started'});

  assert.equal(harness.sentSessionUpdates, 1);
  const bargeTimeline = harness.timeline.slice(beforeBarge);
  const clearIndex = bargeTimeline.findIndex(({kind, message}) => kind === 'worklet-message' && message.type === 'clear');
  const speakingIndex = bargeTimeline.findIndex(({kind, snapshot}) => kind === 'snapshot' && snapshot.status === 'user-speaking');
  assert.ok(clearIndex >= 0 && clearIndex < speakingIndex);
  const clearMessage = harness.playbackNode.port.messages.find(({type}) => type === 'clear');
  assert.equal(clearMessage.generation, harness.presentation.snapshot().playbackGeneration);
  assert.equal(harness.activeTimers.length, 0);

  harness.server({type: 'response.output_audio.delta', delta: SCHEDULED_PCM_BASE64});
  assert.equal(harness.playbackNode.port.messages.filter(({type}) => type === 'append').at(-1).generation, harness.presentation.snapshot().playbackGeneration);
  assert.equal(harness.activeTimers.length, 1);

  const captureNode = harness.captureNode;
  const playbackNode = harness.playbackNode;
  const mediaSource = harness.mediaSources[0];
  const silentGain = harness.gains[0];
  const socket = harness.socket;

  await runtime.stop();

  assert.equal(harness.microphoneTrack.stopped, true);
  assert.equal(harness.captureContext.closed, true);
  assert.equal(harness.playbackContext.closed, true);
  assert.equal(socket.closeCode, 1000);
  assert.ok(new TextEncoder().encode(socket.closeReason).byteLength <= 123);
  assert.equal(socket.readyState, 3);
  assert.ok(captureNode.port.messages.some(({type, value}) => type === 'mute' && value === true));
  assert.equal(captureNode.disconnected, true);
  assert.equal(playbackNode.disconnected, true);
  assert.equal(mediaSource.disconnected, true);
  assert.equal(silentGain.disconnected, true);
  assert.equal(harness.activeTimers.length, 0);
  assert.equal(harness.snapshots.at(-1).status, 'stopped');

  const messageCount = harness.messages.length;
  harness.fireTimers({includeCleared: true});
  assert.equal(harness.messages.length, messageCount);
});

test('resumeAudio resumes suspended playback with one local 200 ms low-volume tone and no socket event', async () => {
  const harness = new BrowserHarness();
  const runtime = createBrowserRuntime(harness.dependencies());
  await runtime.start();
  harness.server({type: 'session.created'});
  harness.server({type: 'session.updated'});
  await new Promise((resolve) => setImmediate(resolve));
  harness.server({type: 'response.output_audio.delta', delta: SCHEDULED_PCM_BASE64});
  assert.equal(harness.presentation.snapshot().status, 'assistant-buffering');
  harness.playbackContext.setState('suspended');
  assert.equal(harness.presentation.snapshot().status, 'audio-suspended');
  const resumeCalls = harness.playbackContext.resumeCalls;
  const sentEvents = harness.sent.length;

  await runtime.resumeAudio();

  assert.equal(harness.playbackContext.resumeCalls, resumeCalls + 1);
  assert.equal(harness.oscillators.length, 1);
  assert.equal(harness.oscillators[0].started, 1);
  assert.equal(harness.oscillators[0].stopped, 1);
  assert.ok(harness.oscillators[0].stopTime > harness.oscillators[0].startTime);
  assert.ok(harness.oscillators[0].stopTime <= harness.oscillators[0].startTime + 0.2);
  assert.equal(harness.gains.at(-1).gain.value, 0.03);
  assert.equal(harness.sent.length, sentEvents);
  assert.equal(harness.presentation.snapshot().status, 'assistant-buffering');
  await runtime.stop();
});

test('microphone failure releases the failed generation and permits a retry', async () => {
  let attempts = 0;
  const harness = new BrowserHarness({
    getUserMedia: () => {
      attempts += 1;
      if (attempts === 1) throw new Error('permission denied');
      return harness.makeMediaStream();
    },
  });
  const runtime = createBrowserRuntime(harness.dependencies());

  await runtime.start();
  assert.equal(harness.snapshots.at(-1).status, 'error');
  assert.equal(harness.notices.at(-1), 'microphone-unavailable');

  await runtime.start();
  assert.equal(harness.getUserMediaCalls, 2);
  assert.equal(harness.sockets.length, 1);
  assert.equal(harness.snapshots.at(-1).status, 'connecting');
  assert.equal(harness.notices.at(-1), '');
  await runtime.stop();
});

test('allocated resources and scheduled playback are released after a provider error', async (t) => {
  const harness = new BrowserHarness();
  harness.installFakeTimers();
  t.after(() => harness.restoreTimers());
  const runtime = createBrowserRuntime(harness.dependencies());
  await runtime.start();
  harness.server({type: 'session.created'});
  harness.server({type: 'session.updated'});
  await new Promise((resolve) => setImmediate(resolve));
  const policyUpdate = harness.sent.find(({type}) => type === 'live.tool_policy.update');
  harness.server({type: 'live.tool_policy.updated', event_id: policyUpdate.event_id, tools: policyUpdate.tools, model: 'deepseek-v4-flash'});
  harness.server({type: 'response.output_audio.delta', delta: SCHEDULED_PCM_BASE64});
  assert.equal(harness.activeTimers.length, 1);

  const captureNode = harness.captureNode;
  const playbackNode = harness.playbackNode;
  const mediaSource = harness.mediaSources[0];
  const silentGain = harness.gains[0];
  const socket = harness.socket;

  harness.server({type: 'error', error: {code: 'rate_limit', message: 'provider detail'}});
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(harness.notices.at(-1), 'gateway-error');
  assert.equal(harness.snapshots.at(-1).status, 'error');
  assert.equal(harness.microphoneTrack.stopped, true);
  assert.equal(harness.captureContext.closed, true);
  assert.equal(harness.playbackContext.closed, true);
  assert.equal(socket.closeCode, 1000);
  assert.ok(new TextEncoder().encode(socket.closeReason).byteLength <= 123);
  assert.equal(socket.readyState, 3);
  assert.ok(captureNode.port.messages.some(({type, value}) => type === 'mute' && value === true));
  assert.equal(captureNode.disconnected, true);
  assert.equal(playbackNode.disconnected, true);
  assert.equal(mediaSource.disconnected, true);
  assert.equal(silentGain.disconnected, true);
  assert.equal(harness.activeTimers.length, 0);

  const messageCount = harness.messages.length;
  harness.fireTimers({includeCleared: true});
  assert.equal(harness.messages.length, messageCount);
});

test('callbacks from a released generation cannot mutate a retried session', async () => {
  const harness = new BrowserHarness();
  const runtime = createBrowserRuntime(harness.dependencies());
  await runtime.start();
  const staleSocket = harness.socket;
  const stalePlaybackNode = harness.playbackNode;
  await runtime.stop();
  await runtime.start();
  const currentSocket = harness.socket;
  const snapshotCount = harness.snapshots.length;

  harness.playback({type: 'playing', generation: 1, revision: 1}, stalePlaybackNode);
  harness.server({type: 'session.created'}, staleSocket);
  staleSocket.emit('close');

  assert.equal(harness.snapshots.length, snapshotCount);
  assert.equal(harness.snapshots.at(-1).status, 'connecting');
  assert.equal(staleSocket.sent.length, 0);
  assert.equal(currentSocket.closeCode, null);
  harness.server({type: 'session.created'}, currentSocket);
  assert.equal(currentSocket.sent.filter(({type}) => type === 'session.update').length, 1);
  await runtime.stop();
});

test('late stop completion cannot overwrite a restarted generation', async () => {
  const mediaResolvers = [];
  const harness = new BrowserHarness({
    getUserMedia: () => new Promise((resolve) => mediaResolvers.push(resolve)),
  });
  const runtime = createBrowserRuntime(harness.dependencies());
  const firstStart = runtime.start();
  const stopping = runtime.stop();
  const secondStart = runtime.start();

  await stopping;
  assert.equal(harness.snapshots.at(-1).status, 'connecting');

  const staleStream = harness.makeMediaStream();
  mediaResolvers[0](staleStream);
  await firstStart;
  assert.equal(staleStream.getTracks()[0].stopped, true);

  mediaResolvers[1](harness.makeMediaStream());
  await secondStart;
  assert.equal(harness.sockets.length, 1);
  await runtime.stop();
});

test('double start shares one in-flight acquisition and creates one browser session', async () => {
  let resolveMedia;
  const media = new Promise((resolve) => {
    resolveMedia = resolve;
  });
  const harness = new BrowserHarness({getUserMedia: () => media});
  const runtime = createBrowserRuntime(harness.dependencies());

  const first = runtime.start();
  const second = runtime.start();
  assert.equal(harness.getUserMediaCalls, 1);
  resolveMedia(harness.makeMediaStream());
  await Promise.all([first, second]);

  assert.equal(harness.contexts.length, 2);
  assert.equal(harness.sockets.length, 1);
  await runtime.stop();
});

test('permanently backed-up microphone socket fails closed with a content-free notice', async () => {
  const harness = new BrowserHarness();
  const runtime = createBrowserRuntime(harness.dependencies());
  await runtime.start();
  harness.server({type: 'session.created'});
	harness.server({type: 'session.updated'});
	await new Promise((resolve) => setImmediate(resolve));
	const policyUpdate = harness.sent.find(({type}) => type === 'live.tool_policy.update');
	harness.server({type: 'live.tool_policy.updated', event_id: policyUpdate.event_id, tools: policyUpdate.tools, model: 'deepseek-v4-flash'});
	harness.socket.bufferedAmount = Number.MAX_SAFE_INTEGER;

  harness.capture({type: 'frame', pcm: new ArrayBuffer(960)});
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(harness.sent.filter(({type}) => type === 'input_audio_buffer.append').length, 0);
  assert.equal(harness.notices.at(-1), 'microphone-backpressure');
  assert.equal(harness.snapshots.at(-1).status, 'error');
  assert.equal(harness.microphoneTrack.stopped, true);
  assert.equal(harness.captureContext.closed, true);
  assert.equal(harness.playbackContext.closed, true);
  assert.equal(harness.socket.closeCode, 1000);
});

test('playback acknowledgements are fenced by generation and newest queue revision', async () => {
  const harness = new BrowserHarness();
  const runtime = createBrowserRuntime(harness.dependencies());
  await runtime.start();
  harness.server({type: 'session.created'});
  harness.server({type: 'session.updated'});
  harness.server({type: 'response.output_audio.delta', delta: SCHEDULED_PCM_BASE64});
  const first = harness.playbackNode.port.messages.filter(({type}) => type === 'append').at(-1);

  harness.server({type: 'input_audio_buffer.speech_started'});
  harness.playback({type: 'playing', generation: first.generation, revision: first.revision});
  assert.equal(harness.presentation.snapshot().status, 'user-speaking');

  harness.server({type: 'response.output_audio.delta', delta: SCHEDULED_PCM_BASE64});
  const newest = harness.playbackNode.port.messages.filter(({type}) => type === 'append').at(-1);
  assert.ok(newest.revision > first.revision);
  assert.notEqual(newest.generation, first.generation);
  harness.playback({type: 'idle', generation: newest.generation, revision: newest.revision - 1});
  assert.equal(harness.presentation.snapshot().status, 'assistant-buffering');
  harness.playback({type: 'playing', generation: newest.generation, revision: newest.revision});
  assert.equal(harness.presentation.snapshot().status, 'assistant-speaking');
  await runtime.stop();
});

test('current idle acknowledgement keeps buffering and repumps a delayed reservoir', async (t) => {
  const harness = new BrowserHarness();
  harness.installFakeTimers();
  t.after(() => harness.restoreTimers());
  const runtime = createBrowserRuntime(harness.dependencies());
  await runtime.start();
  harness.server({type: 'session.created'});
  harness.server({type: 'session.updated'});
  await new Promise((resolve) => setImmediate(resolve));
  const policyUpdate = harness.sent.find(({type}) => type === 'live.tool_policy.update');
  harness.server({type: 'live.tool_policy.updated', event_id: policyUpdate.event_id, tools: policyUpdate.tools, model: 'deepseek-v4-flash'});
  harness.server({type: 'response.output_audio.delta', delta: SCHEDULED_PCM_BASE64});

  const first = harness.playbackNode.port.messages.filter(({type}) => type === 'append').at(-1);
  harness.playback({type: 'playing', generation: first.generation, revision: first.revision});
  assert.equal(harness.presentation.snapshot().status, 'assistant-speaking');
  const delayedRefill = harness.activeTimers[0];
  assert.ok(delayedRefill);
  harness.playbackContext.currentTime += 0.05;

  harness.playback({type: 'idle', generation: first.generation, revision: first.revision});

  const appends = harness.playbackNode.port.messages.filter(({type}) => type === 'append');
  assert.equal(harness.presentation.snapshot().status, 'assistant-buffering');
  assert.equal(delayedRefill.cleared, true);
  assert.equal(appends.length, 2);
  assert.equal(appends[1].generation, first.generation);
  assert.ok(appends[1].revision > first.revision);
  assert.equal(harness.activeTimers.length, 1);
  assert.notEqual(harness.activeTimers[0], delayedRefill);
  await runtime.stop();
});

test('open socket without session.created hits its fenced establishment deadline', async (t) => {
  const harness = new BrowserHarness();
  harness.installFakeTimers();
  t.after(() => harness.restoreTimers());
  const runtime = createBrowserRuntime(harness.dependencies());
  await runtime.start();
  assert.equal(harness.activeTimers.length, 1);

  harness.fireNextTimer();
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(harness.notices.at(-1), 'session-created-timeout');
  assert.equal(harness.microphoneTrack.stopped, true);
  assert.equal(harness.captureContext.closed, true);
  assert.equal(harness.playbackContext.closed, true);
  assert.equal(harness.socket.closeCode, 1000);
  assert.equal(harness.activeTimers.length, 0);
});

test('session.created without session.updated hits its fenced establishment deadline', async (t) => {
  const harness = new BrowserHarness();
  harness.installFakeTimers();
  t.after(() => harness.restoreTimers());
  const runtime = createBrowserRuntime(harness.dependencies());
  await runtime.start();
  harness.server({type: 'session.created'});
  assert.equal(harness.activeTimers.length, 1);

  harness.fireNextTimer();
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(harness.notices.at(-1), 'session-updated-timeout');
  assert.equal(harness.microphoneTrack.stopped, true);
  assert.equal(harness.captureContext.closed, true);
  assert.equal(harness.playbackContext.closed, true);
  assert.equal(harness.socket.closeCode, 1000);
  assert.equal(harness.activeTimers.length, 0);
});

test('socket that never opens hits a distinct establishment deadline', async (t) => {
  const harness = new BrowserHarness({socketReadyState: 0});
  harness.installFakeTimers();
  t.after(() => harness.restoreTimers());
  const runtime = createBrowserRuntime(harness.dependencies());
  await runtime.start();
  assert.equal(harness.activeTimers.length, 1);

  harness.fireNextTimer();
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(harness.notices.at(-1), 'socket-open-timeout');
  assert.equal(harness.microphoneTrack.stopped, true);
  assert.equal(harness.captureContext.closed, true);
  assert.equal(harness.playbackContext.closed, true);
  assert.equal(harness.socket.closeCode, 1000);
  assert.equal(harness.activeTimers.length, 0);
});

test('capture suspension that cannot resume fails closed and releases every resource', async () => {
  const harness = new BrowserHarness();
  const runtime = createBrowserRuntime(harness.dependencies());
  await runtime.start();
  harness.server({type: 'session.created'});
  harness.server({type: 'session.updated'});
  harness.captureResumeFailure = true;

  harness.captureContext.setState('suspended');
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(harness.notices.at(-1), 'microphone-suspended');
  assert.equal(harness.microphoneTrack.stopped, true);
  assert.equal(harness.captureContext.closed, true);
  assert.equal(harness.playbackContext.closed, true);
  assert.equal(harness.socket.closeCode, 1000);
});

test('capture suspension resume is bounded when the browser promise never settles', async (t) => {
  const harness = new BrowserHarness();
  harness.installFakeTimers();
  t.after(() => harness.restoreTimers());
  const runtime = createBrowserRuntime(harness.dependencies());
  await runtime.start();
	harness.server({type: 'session.created'});
	harness.server({type: 'session.updated'});
	await new Promise((resolve) => setImmediate(resolve));
	const policyUpdate = harness.sent.find(({type}) => type === 'live.tool_policy.update');
	harness.server({type: 'live.tool_policy.updated', event_id: policyUpdate.event_id, tools: policyUpdate.tools, model: 'deepseek-v4-flash'});
	harness.captureResumePending = true;

  harness.captureContext.setState('suspended');
  assert.equal(harness.activeTimers.length, 1);
  harness.fireNextTimer();
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(harness.notices.at(-1), 'microphone-suspended');
  assert.equal(harness.microphoneTrack.stopped, true);
  assert.equal(harness.captureContext.closed, true);
  assert.equal(harness.playbackContext.closed, true);
  assert.equal(harness.socket.closeCode, 1000);
  assert.equal(harness.activeTimers.length, 0);
});

for (const cause of ['device removal', 'permission revocation']) {
  test(`microphone ${cause} ends capture and releases every resource`, async () => {
    const harness = new BrowserHarness();
    const runtime = createBrowserRuntime(harness.dependencies());
    await runtime.start();
    harness.server({type: 'session.created'});
    harness.server({type: 'session.updated'});

    harness.microphoneTrack.emit('ended');
    await new Promise((resolve) => setImmediate(resolve));

    assert.equal(harness.notices.at(-1), 'microphone-ended');
    assert.equal(harness.microphoneTrack.stopped, true);
    assert.equal(harness.captureContext.closed, true);
    assert.equal(harness.playbackContext.closed, true);
    assert.equal(harness.socket.closeCode, 1000);
  });
}

test('ended event from a released microphone generation cannot fail a retry', async () => {
  const harness = new BrowserHarness();
  const runtime = createBrowserRuntime(harness.dependencies());
  await runtime.start();
  const staleTrack = harness.microphoneTrack;
  await runtime.stop();
  await runtime.start();
  const currentSocket = harness.socket;

  staleTrack.emit('ended');
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(harness.presentation.snapshot().status, 'connecting');
  assert.equal(currentSocket.closeCode, null);
  await runtime.stop();
});

test('audio worklet initialization failure is not mislabeled as a microphone failure', async () => {
  const harness = new BrowserHarness({failWorkletModule: true});
  const runtime = createBrowserRuntime(harness.dependencies());

  await runtime.start();

  assert.equal(harness.notices.at(-1), 'audio-initialization-failed');
  assert.equal(harness.microphoneTrack.stopped, true);
  assert.equal(harness.captureContext.closed, true);
  assert.equal(harness.playbackContext.closed, true);
  assert.equal(harness.sockets.length, 0);
});
