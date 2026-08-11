import test from 'node:test';
import assert from 'node:assert/strict';

import {createBrowserRuntime} from './browser-runtime.mjs';
import {ClientPresentation} from './client-state.mjs';

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
  constructor({getUserMedia} = {}) {
    this.contexts = [];
    this.getUserMediaCalls = 0;
    this.gains = [];
    this.mediaStreams = [];
    this.notices = [];
    this.oscillators = [];
    this.sent = [];
    this.snapshots = [];
    this.sockets = [];
    this.timeline = [];
    this.workletNodes = [];
    this.getUserMedia = getUserMedia ?? (() => this.makeMediaStream());
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
        this.audioWorklet = {
          addModule: async (path) => {
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
        this.state = 'running';
      }

      async close() {
        this.closed = true;
        this.state = 'closed';
      }
    };

    this.WebSocket = class {
      static CONNECTING = 0;
      static OPEN = 1;

      constructor(url) {
        this.url = url;
        this.readyState = 1;
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
      stop() {
        this.stopped = true;
      },
    };
    const stream = {getTracks: () => [track]};
    this.mediaStreams.push(stream);
    this.microphoneTrack = track;
    return stream;
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
}

test('start, server acknowledgement, PCM, playback, barge-in, and stop release every boundary', async () => {
  const harness = new BrowserHarness();
  const runtime = createBrowserRuntime(harness.dependencies());

  await runtime.start();
  harness.server({type: 'session.created'});
  harness.server({type: 'session.updated'});
  harness.capture({type: 'frame', pcm: new ArrayBuffer(960)});
  harness.server({type: 'response.output_audio.delta', delta: 'AAABAA=='});
  harness.playback({type: 'playing'});
  const beforeBarge = harness.timeline.length;
  harness.server({type: 'input_audio_buffer.speech_started'});

  assert.equal(harness.sentSessionUpdates, 1);
  assert.equal(harness.sent.filter(({type}) => type === 'input_audio_buffer.append').length, 1);
  assert.ok(harness.messages.some((message) => message.type === 'append'));
  const bargeTimeline = harness.timeline.slice(beforeBarge);
  const clearIndex = bargeTimeline.findIndex(({kind, message}) => kind === 'worklet-message' && message.type === 'clear');
  const speakingIndex = bargeTimeline.findIndex(({kind, snapshot}) => kind === 'snapshot' && snapshot.status === 'user-speaking');
  assert.ok(clearIndex >= 0 && clearIndex < speakingIndex);
  assert.ok(harness.snapshots.some(({status}) => status === 'assistant-speaking'));

  await runtime.stop();

  assert.equal(harness.microphoneTrack.stopped, true);
  assert.equal(harness.captureContext.closed, true);
  assert.equal(harness.playbackContext.closed, true);
  assert.equal(harness.socket.closeCode, 1000);
  assert.equal(harness.snapshots.at(-1).status, 'stopped');
});

test('resumeAudio resumes suspended playback with one local 200 ms low-volume tone and no socket event', async () => {
  const harness = new BrowserHarness();
  const runtime = createBrowserRuntime(harness.dependencies());
  await runtime.start();
  harness.playbackContext.state = 'suspended';
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

test('provider-specific errors retain the generic gateway notice contract', async () => {
  const harness = new BrowserHarness();
  const runtime = createBrowserRuntime(harness.dependencies());
  await runtime.start();

  harness.server({type: 'error', error: {code: 'rate_limit', message: 'provider detail'}});
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(harness.notices.at(-1), 'gateway-error');
  assert.equal(harness.snapshots.at(-1).status, 'error');
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

  harness.playback({type: 'playing'}, stalePlaybackNode);
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
