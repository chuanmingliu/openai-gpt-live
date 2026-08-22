import {decodePCM16Base64, PCMPlaybackPacer, SinglePendingTimer} from './audio-core.mjs';
import {ProtocolState, buildAudioAppend, buildSessionUpdate} from './protocol-state.mjs';
import {buildToolPolicyUpdate, normalizeToolPolicy} from './tool-state.mjs';

const ESTABLISHMENT_TIMEOUT_MS = 5000;
const CAPTURE_RESUME_TIMEOUT_MS = 1000;
const MAXIMUM_SOCKET_BUFFERED_BYTES = 32 << 10;
const TOOL_REQUEST_TIMEOUT_MS = 12000;

const NOTICE_CODES = new Set([
  'audio-overflow',
  'audio-initialization-failed',
  'gateway-error',
  'invalid-audio',
  'invalid-event',
  'invalid-transcript',
  'microphone-unavailable',
  'microphone-backpressure',
  'microphone-ended',
  'microphone-suspended',
  'profile-mismatch',
  'profile-unavailable',
  'session-created-timeout',
  'session-updated-timeout',
  'services-not-ready',
  'socket-open-timeout',
  'transport-closed',
  'tool-state-overflow',
]);

export function createBrowserRuntime(dependencies) {
  const {
    mediaDevices,
    AudioContext,
    WebSocket,
    fetch: fetchImpl,
    location,
    presentation,
    onSnapshot,
    onNotice,
    onProtocolEvent = () => {},
    // profileState is optional. When present it owns the immutable selection
    // and the second capture acknowledgement gate; the builtin default keeps
    // existing single-ack behavior for voice-only gateways.
    profileState = null,
  } = dependencies;

  let generation = 0;
  let protocol = new ProtocolState();
  let mediaStream = null;
  let captureContext = null;
  let playbackContext = null;
  let captureNode = null;
  let playbackNode = null;
  let mediaSource = null;
  let silentGain = null;
  let socket = null;
  let streamReady = false;
  let sessionConfigured = false;
  let sessionUpdateSent = false;
  let socketOpenObserved = false;
  let initialPolicySent = false;
  let policyLoadPromise = null;
  let nextPolicyEventID = 1;
  let policySaveQueue = Promise.resolve();
  let startPromise = null;
  let policyAccepted = false;
  let playbackRevision = 0;
  let captureResumePending = false;
  const playbackPacer = new PCMPlaybackPacer(120);
  const playbackFeedTimer = new SinglePendingTimer();
  const establishmentTimer = new SinglePendingTimer();
  const captureResumeTimer = new SinglePendingTimer();

  function reportProtocolEvent(direction, event) {
    try {
      onProtocolEvent(direction, event);
    } catch {
      // Diagnostics are observational and must never affect the voice path.
    }
  }

  function publishSnapshot() {
    onSnapshot(presentation.snapshot());
  }

  // captureGateOpen requires the tool-policy acknowledgement plus, when a
  // custom profile is selected, the exact live.profile.selected acknowledgement
  // and the provider session.updated acknowledgement. Any mismatch fails closed.
  function captureGateOpen() {
    if (!policyAccepted) return false;
    if (profileState === null) return true;
    return profileState.captureReady();
  }

  // openCaptureIfReady promotes a pending session to listening once the last
  // outstanding acknowledgement arrives, in whichever order they land.
  function openCaptureIfReady(currentGeneration) {
    if (generation !== currentGeneration || streamReady || !captureGateOpen()) return;
    establishmentTimer.clear();
    streamReady = true;
    presentation.apply({type: 'status', value: 'listening'});
    publishSnapshot();
  }

  async function boundedFetch(path, options) {
    if (typeof fetchImpl !== 'function') throw new TypeError('fetch unavailable');
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), TOOL_REQUEST_TIMEOUT_MS);
    try {
      return await fetchImpl(path, {...options, signal: controller.signal, cache: 'no-store'});
    } finally {
      clearTimeout(timer);
    }
  }

  async function loadToolPolicy(currentGeneration) {
    const loadGeneration = presentation.tools.beginPolicyLoad();
    try {
      const response = await boundedFetch('/api/tool-policy', {method: 'GET'});
      if (!response.ok) throw new Error('tool policy unavailable');
      const policy = normalizeToolPolicy(await response.json());
      if (generation === currentGeneration) presentation.tools.loadPolicy(policy, loadGeneration);
    } catch {
      if (generation === currentGeneration) presentation.tools.failLoad(loadGeneration);
    } finally {
      if (generation === currentGeneration) publishSnapshot();
    }
  }

  function sendCurrentPolicy(currentGeneration, ownedSocket, initial) {
    if (generation !== currentGeneration || socket !== ownedSocket || !sessionConfigured || ownedSocket.readyState !== WebSocket.OPEN) return false;
    if (initial && initialPolicySent) return false;
    const eventID = `browser-policy-${nextPolicyEventID++}`;
    const policy = presentation.tools.snapshot().policy;
    const event = buildToolPolicyUpdate(policy, eventID);
    ownedSocket.send(JSON.stringify(event));
    reportProtocolEvent('outbound', event);
    if (initial) initialPolicySent = true;
    presentation.tools.markPolicySent(eventID, policy);
    publishSnapshot();
    return true;
  }

  function reflectPlaybackState(currentGeneration, ownedPlaybackContext) {
    if (generation !== currentGeneration || playbackContext !== ownedPlaybackContext) return;
    if (ownedPlaybackContext.state === 'suspended') presentation.apply({type: 'audio-suspended'});
    if (ownedPlaybackContext.state === 'running') presentation.apply({type: 'audio-resumed'});
    publishSnapshot();
  }

  function clearPlayback() {
    const playbackGeneration = presentation.snapshot().playbackGeneration;
    playbackFeedTimer.clear();
    playbackPacer.clear(playbackContext?.currentTime ?? 0);
    playbackRevision += 1;
    playbackNode?.port.postMessage({type: 'clear', generation: playbackGeneration, revision: playbackRevision});
  }

  function pumpPlayback(currentGeneration, force = false) {
    if (generation !== currentGeneration || !playbackContext || !playbackNode) return;
    const delaySeconds = playbackPacer.refill(playbackContext.currentTime, (samples) => {
      playbackRevision += 1;
      playbackNode.port.postMessage({
        type: 'append',
        generation: presentation.snapshot().playbackGeneration,
        revision: playbackRevision,
        pcm: samples.buffer,
      }, [samples.buffer]);
    }, force);
    if (delaySeconds !== null) {
      playbackFeedTimer.request(
        () => pumpPlayback(currentGeneration),
        Math.max(5, Math.ceil(delaySeconds * 1000)),
      );
    }
  }

  function applyEffect(effect, currentGeneration) {
    if (generation !== currentGeneration) return;
    if (effect.type === 'tool-policy-accepted') {
      if (presentation.tools.acceptPolicy(effect.event_id, effect.policy, effect.model)) {
        policyAccepted = true;
        if (captureGateOpen()) {
          establishmentTimer.clear();
          streamReady = true;
          presentation.apply({type: 'status', value: 'listening'});
        }
      }
      publishSnapshot();
      return;
    }
    if (effect.type === 'status' && effect.value === 'listening' && !streamReady) return;
    if (!presentation.apply(effect)) {
      void failSession(effect.type === 'handoff-card' ? 'tool-state-overflow' : 'invalid-transcript', currentGeneration);
      return;
    }
    switch (effect.type) {
      case 'clear-playback':
        clearPlayback();
        break;
      case 'audio': {
        presentation.apply({type: 'audio-buffered'});
        try {
          const samples = decodePCM16Base64(effect.encoded);
          if (!playbackPacer.append(samples)) {
            void failSession('audio-overflow', currentGeneration);
            break;
          }
          pumpPlayback(currentGeneration);
        } catch {
          void failSession('invalid-audio', currentGeneration);
        }
        break;
      }
      case 'audio-end':
        playbackFeedTimer.clear();
        pumpPlayback(currentGeneration, true);
        break;
      case 'error':
        void failSession(NOTICE_CODES.has(effect.code) ? effect.code : 'gateway-error', currentGeneration);
        break;
    }
    publishSnapshot();
  }

  function armEstablishmentDeadline(code, currentGeneration, ownedSocket) {
    establishmentTimer.clear();
    establishmentTimer.request(() => {
      if (generation === currentGeneration && socket === ownedSocket) {
        void failSession(code, currentGeneration);
      }
    }, ESTABLISHMENT_TIMEOUT_MS);
  }

  function handleSocketOpen(currentGeneration, ownedSocket) {
    if (generation !== currentGeneration || socket !== ownedSocket || socketOpenObserved) return;
    socketOpenObserved = true;
    armEstablishmentDeadline('session-created-timeout', currentGeneration, ownedSocket);
  }

  function handleServerMessage(raw, currentGeneration, ownedSocket) {
    if (generation !== currentGeneration || socket !== ownedSocket) return;
    if (typeof raw !== 'string' || raw.length > (64 << 10)) {
      void failSession('invalid-event', currentGeneration);
      return;
    }
    let event;
    try {
      event = JSON.parse(raw);
    } catch {
      void failSession('invalid-event', currentGeneration);
      return;
    }
    reportProtocolEvent('inbound', event);
    if (event?.type === 'session.created') {
      if (!socketOpenObserved || ownedSocket.readyState !== WebSocket.OPEN || sessionUpdateSent) {
        void failSession('invalid-event', currentGeneration);
        return;
      }
      establishmentTimer.clear();
      const update = buildSessionUpdate();
      ownedSocket.send(JSON.stringify(update));
      reportProtocolEvent('outbound', update);
      sessionUpdateSent = true;
      armEstablishmentDeadline('session-updated-timeout', currentGeneration, ownedSocket);
    } else if (event?.type === 'session.updated') {
      if (!sessionUpdateSent) {
        void failSession('invalid-event', currentGeneration);
        return;
      }
      if (!sessionConfigured) {
        sessionConfigured = true;
        profileState?.onProviderUpdated();
        armEstablishmentDeadline('session-updated-timeout', currentGeneration, ownedSocket);
        void (policyLoadPromise ?? Promise.resolve()).then(() => {
          sendCurrentPolicy(currentGeneration, ownedSocket, true);
        });
      }
    } else if (event?.type === 'live.profile.selected') {
      if (profileState !== null && !profileState.onProfileSelected(event.profile)) {
        void failSession('profile-mismatch', currentGeneration);
        return;
      }
      openCaptureIfReady(currentGeneration);
    }
    for (const effect of protocol.consume(event)) applyEffect(effect, currentGeneration);
  }

  async function initialize(currentGeneration) {
    try {
      const response = await boundedFetch('/api/voice-readiness', {
        method: 'GET',
        credentials: 'same-origin',
      });
      if (!response.ok) throw new Error('voice path unavailable');
    } catch {
      if (generation === currentGeneration) await failSession('services-not-ready', currentGeneration);
      return;
    }
    if (generation !== currentGeneration) return;

    let acquired;
    try {
      acquired = await mediaDevices.getUserMedia({
        audio: {channelCount: 1, echoCancellation: true, noiseSuppression: true, autoGainControl: true},
      });
    } catch {
      if (generation === currentGeneration) await failSession('microphone-unavailable', currentGeneration);
      return;
    }
    if (generation !== currentGeneration) {
      for (const track of acquired.getTracks()) track.stop();
      return;
    }

    mediaStream = acquired;
    const ownedMediaStream = acquired;
    for (const track of acquired.getTracks()) {
      track.addEventListener?.('ended', () => {
        if (generation === currentGeneration && mediaStream === ownedMediaStream) {
          void failSession('microphone-ended', currentGeneration);
        }
      }, {once: true});
    }

    try {
      captureContext = new AudioContext({latencyHint: 'interactive'});
      playbackContext = new AudioContext({sampleRate: 24000, latencyHint: 'interactive'});
      await Promise.all([
        captureContext.audioWorklet.addModule('/capture-worklet.js'),
        playbackContext.audioWorklet.addModule('/playback-worklet.js'),
      ]);
      if (generation !== currentGeneration) return;

      const ownedPlaybackContext = playbackContext;
      const ownedCaptureContext = captureContext;
      ownedPlaybackContext.addEventListener?.('statechange', () => {
        reflectPlaybackState(currentGeneration, ownedPlaybackContext);
      });
      ownedCaptureContext.addEventListener?.('statechange', () => {
        if (generation !== currentGeneration || captureContext !== ownedCaptureContext || ownedCaptureContext.state !== 'suspended' || captureResumePending) return;
        captureResumePending = true;
        captureResumeTimer.request(() => {
          if (generation === currentGeneration && captureContext === ownedCaptureContext && ownedCaptureContext.state !== 'running') {
            void failSession('microphone-suspended', currentGeneration);
          }
        }, CAPTURE_RESUME_TIMEOUT_MS);
        void ownedCaptureContext.resume().then(() => {
          captureResumeTimer.clear();
          if (generation === currentGeneration && captureContext === ownedCaptureContext && ownedCaptureContext.state !== 'running') {
            return failSession('microphone-suspended', currentGeneration);
          }
          return undefined;
        }).catch(() => {
          captureResumeTimer.clear();
          return failSession('microphone-suspended', currentGeneration);
        }).finally(() => {
          captureResumePending = false;
        });
      });

      const ownedCaptureNode = new AudioWorkletNode(captureContext, 'live-capture', {
        numberOfInputs: 1,
        numberOfOutputs: 1,
        outputChannelCount: [1],
      });
      const ownedPlaybackNode = new AudioWorkletNode(playbackContext, 'live-playback', {
        numberOfInputs: 0,
        numberOfOutputs: 1,
        outputChannelCount: [1],
      });
      captureNode = ownedCaptureNode;
      playbackNode = ownedPlaybackNode;
      mediaSource = captureContext.createMediaStreamSource(mediaStream);
      silentGain = captureContext.createGain();
      silentGain.gain.value = 0;
      mediaSource.connect(captureNode).connect(silentGain).connect(captureContext.destination);
      playbackNode.connect(playbackContext.destination);
      captureNode.port.onmessage = ({data}) => {
        if (generation !== currentGeneration || captureNode !== ownedCaptureNode) return;
        if (!streamReady || presentation.isMuted() || socket?.readyState !== WebSocket.OPEN || data?.type !== 'frame' || !(data.pcm instanceof ArrayBuffer)) return;
        try {
          const event = buildAudioAppend(new Uint8Array(data.pcm));
          const encoded = JSON.stringify(event);
          if (!Number.isFinite(socket.bufferedAmount) || socket.bufferedAmount + encoded.length > MAXIMUM_SOCKET_BUFFERED_BYTES) {
            void failSession('microphone-backpressure', currentGeneration);
            return;
          }
          socket.send(encoded);
          reportProtocolEvent('outbound', event);
        } catch {
          void failSession('invalid-audio', currentGeneration);
        }
      };
      playbackNode.port.onmessage = ({data}) => {
        if (generation !== currentGeneration || playbackNode !== ownedPlaybackNode) return;
        const playbackGeneration = presentation.snapshot().playbackGeneration;
        if (data?.generation !== playbackGeneration || data?.revision !== playbackRevision) return;
        if (data?.type === 'overflow') void failSession('audio-overflow', currentGeneration);
        if (data?.type === 'playing') presentation.apply({type: 'audio-playing'});
        if (data?.type === 'idle') {
          playbackPacer.rebuffer(playbackContext.currentTime);
          if (playbackPacer.queuedSamples > 0) {
            presentation.apply({type: 'audio-idle'});
            presentation.apply({type: 'audio-buffered'});
            playbackFeedTimer.clear();
            pumpPlayback(currentGeneration);
          } else {
            presentation.apply({type: 'audio-idle'});
          }
        }
        publishSnapshot();
      };
      await Promise.all([captureContext.resume(), playbackContext.resume()]);
      if (generation !== currentGeneration) return;
      reflectPlaybackState(currentGeneration, ownedPlaybackContext);
    } catch {
      if (generation === currentGeneration) await failSession('audio-initialization-failed', currentGeneration);
      return;
    }

    try {
      const scheme = location.protocol === 'https:' ? 'wss:' : 'ws:';
      const profileQuery = profileState?.query() ?? '';
      const ownedSocket = new WebSocket(`${scheme}//${location.host}/ws${profileQuery === '' ? '' : `?${profileQuery}`}`);
      socket = ownedSocket;
      ownedSocket.addEventListener('open', () => {
        handleSocketOpen(currentGeneration, ownedSocket);
      }, {once: true});
      ownedSocket.addEventListener('message', ({data}) => {
        handleServerMessage(data, currentGeneration, ownedSocket);
      });
      ownedSocket.addEventListener('close', ({code} = {}) => {
        if (generation === currentGeneration && socket === ownedSocket) {
          reportProtocolEvent('inbound', {
            type: 'live.transport.closed',
            code: Number.isInteger(code) && code >= 1000 && code <= 4999 ? code : 1006,
          });
          void failSession('transport-closed', currentGeneration);
        }
      }, {once: true});
      ownedSocket.addEventListener('error', () => {
        if (generation === currentGeneration && socket === ownedSocket) {
          void failSession('transport-closed', currentGeneration);
        }
      }, {once: true});
      if (ownedSocket.readyState === WebSocket.OPEN) handleSocketOpen(currentGeneration, ownedSocket);
      else armEstablishmentDeadline('socket-open-timeout', currentGeneration, ownedSocket);
    } catch {
      if (generation === currentGeneration) await failSession('transport-closed', currentGeneration);
    }
  }

  function start() {
    if (startPromise) return startPromise;
    if (mediaStream || socket) return Promise.resolve();

    const currentGeneration = ++generation;
    profileState?.onConnected();
    streamReady = false;
    policyAccepted = false;
    sessionConfigured = false;
    sessionUpdateSent = false;
    socketOpenObserved = false;
    initialPolicySent = false;
    protocol = new ProtocolState();
    presentation.beginSession();
    onNotice('');
    publishSnapshot();

    policyLoadPromise = loadToolPolicy(currentGeneration);
    const pending = initialize(currentGeneration);
    startPromise = pending;
    void pending.then(() => {
      if (startPromise === pending) startPromise = null;
    });
    return pending;
  }

  async function releaseResources(closeSocket) {
    profileState?.onDisconnected();
    streamReady = false;
    policyAccepted = false;
    sessionConfigured = false;
    sessionUpdateSent = false;
    socketOpenObserved = false;
    initialPolicySent = false;
    captureResumePending = false;
    establishmentTimer.clear();
    captureResumeTimer.clear();
    clearPlayback();

    const ownedSocket = socket;
    socket = null;
    if (closeSocket && ownedSocket && ownedSocket.readyState <= WebSocket.OPEN) {
      ownedSocket.close(1000, 'demo stopped');
    }

    captureNode?.port.postMessage({type: 'mute', value: true});
    try { mediaSource?.disconnect(); } catch {}
    try { captureNode?.disconnect(); } catch {}
    try { silentGain?.disconnect(); } catch {}
    try { playbackNode?.disconnect(); } catch {}
    captureNode = null;
    playbackNode = null;
    mediaSource = null;
    silentGain = null;

    if (mediaStream) {
      for (const track of mediaStream.getTracks()) track.stop();
    }
    mediaStream = null;

    const ownedCaptureContext = captureContext;
    const ownedPlaybackContext = playbackContext;
    captureContext = null;
    playbackContext = null;
    await Promise.allSettled([
      ownedCaptureContext?.close(),
      ownedPlaybackContext?.close(),
    ].filter(Boolean));
  }

  async function stop() {
    const stopGeneration = ++generation;
    startPromise = null;
    await releaseResources(true);
    if (generation !== stopGeneration) return;
    presentation.endSession('stopped');
    onNotice('');
    publishSnapshot();
  }

  async function failSession(code, currentGeneration) {
    if (generation !== currentGeneration) return;
    const failureGeneration = ++generation;
    await releaseResources(true);
    if (generation !== failureGeneration) return;
    presentation.endSession('error');
    onNotice(code);
    publishSnapshot();
  }

  function mute(value) {
    if (!captureNode) return;
    presentation.setMuted(value);
    captureNode.port.postMessage({type: 'mute', value: Boolean(value)});
    publishSnapshot();
  }

  async function resumeAudio() {
    const ownedPlaybackContext = playbackContext;
    if (!ownedPlaybackContext) return;
    await ownedPlaybackContext.resume();
    if (playbackContext !== ownedPlaybackContext) return;
    reflectPlaybackState(generation, ownedPlaybackContext);
    if (ownedPlaybackContext.state !== 'running') return;
    const oscillator = ownedPlaybackContext.createOscillator();
    const gain = ownedPlaybackContext.createGain();
    gain.gain.value = 0.03;
    oscillator.connect(gain).connect(ownedPlaybackContext.destination);
    oscillator.start();
    oscillator.stop(ownedPlaybackContext.currentTime + 0.2);
  }

  async function persistToolPolicy(canonical) {
    const operation = presentation.tools.beginSave(canonical);
    publishSnapshot();
    try {
      const response = await boundedFetch('/api/tool-policy', {
        method: 'PUT',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(canonical),
      });
      if (!response.ok) throw new Error('tool policy save failed');
      presentation.tools.commitSave(canonical, operation);
      if (socket) sendCurrentPolicy(generation, socket, false);
    } catch {
      presentation.tools.failSave(operation);
    }
    publishSnapshot();
  }

  function saveToolPolicy(policy) {
    let canonical;
    try {
      canonical = normalizeToolPolicy(policy);
    } catch {
      presentation.tools.failSave();
      publishSnapshot();
      return Promise.resolve();
    }
    const pending = policySaveQueue.then(() => persistToolPolicy(canonical));
    policySaveQueue = pending.catch(() => {});
    return pending;
  }

  async function testToolPolicy() {
    const operation = presentation.tools.beginTest();
    publishSnapshot();
    let passed = false;
    try {
      const response = await boundedFetch('/api/tool-policy/test', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: '{}',
      });
      passed = response.ok;
    } catch {}
    presentation.tools.finishTest(passed, operation);
    publishSnapshot();
  }

  return {start, stop, mute, resumeAudio, saveToolPolicy, testToolPolicy};
}
