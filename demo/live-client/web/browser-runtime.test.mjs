import test from 'node:test';
import assert from 'node:assert/strict';

import {createBrowserRuntime} from './browser-runtime.mjs';
import {ClientPresentation} from './client-state.mjs';

const SCHEDULED_PCM_BASE64 = Buffer.alloc(16000).toString('base64');

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
  constructor({getUserMedia, socketReadyState = 1, failWorkletModule = false, captureResumeFailure = false} = {}) {
    this.contexts = [];
    this.getUserMediaCalls = 0;
    this.gains = [];
    this.mediaSources = [];
    this.mediaStreams = [];
    this.notices = [];
    this.oscillators = [];
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
  harness.server({type: 'response.output_audio.delta', delta: SCHEDULED_PCM_BASE64});

  const first = harness.playbackNode.port.messages.filter(({type}) => type === 'append').at(-1);
  const delayedRefill = harness.activeTimers[0];
  assert.ok(delayedRefill);
  harness.playbackContext.currentTime += 0.2;

  harness.playback({type: 'idle', generation: first.generation, revision: first.revision});

  const appends = harness.playbackNode.port.messages.filter(({type}) => type === 'append');
  assert.equal(harness.presentation.snapshot().status, 'assistant-buffering');
  assert.equal(delayedRefill.cleared, true);
  assert.equal(appends.length, 2);
  assert.equal(appends[1].generation, first.generation);
  assert.ok(appends[1].revision > first.revision);
  assert.equal(harness.activeTimers.length, 0);
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
