import {decodePCM16Base64, PCMPlaybackPacer, SinglePendingTimer} from './audio-core.mjs';
import {ProtocolState, buildAudioAppend, buildSessionUpdate} from './protocol-state.mjs';

const ESTABLISHMENT_TIMEOUT_MS = 5000;
const CAPTURE_RESUME_TIMEOUT_MS = 1000;
const MAXIMUM_SOCKET_BUFFERED_BYTES = 32 << 10;

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
  'session-created-timeout',
  'session-updated-timeout',
  'socket-open-timeout',
  'transport-closed',
]);

export function createBrowserRuntime(dependencies) {
  const {
    mediaDevices,
    AudioContext,
    WebSocket,
    location,
    presentation,
    onSnapshot,
    onNotice,
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
  let sessionUpdateSent = false;
  let socketOpenObserved = false;
  let startPromise = null;
  let playbackRevision = 0;
  let captureResumePending = false;
  const playbackPacer = new PCMPlaybackPacer(120);
  const playbackFeedTimer = new SinglePendingTimer();
  const establishmentTimer = new SinglePendingTimer();
  const captureResumeTimer = new SinglePendingTimer();

  function publishSnapshot() {
    onSnapshot(presentation.snapshot());
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

  function pumpPlayback(currentGeneration) {
    if (generation !== currentGeneration || !playbackContext || !playbackNode) return;
    const delaySeconds = playbackPacer.refill(playbackContext.currentTime, (samples) => {
      playbackRevision += 1;
      playbackNode.port.postMessage({
        type: 'append',
        generation: presentation.snapshot().playbackGeneration,
        revision: playbackRevision,
        pcm: samples.buffer,
      }, [samples.buffer]);
    });
    if (delaySeconds !== null) {
      playbackFeedTimer.request(
        () => pumpPlayback(currentGeneration),
        Math.max(5, Math.ceil(delaySeconds * 1000)),
      );
    }
  }

  function applyEffect(effect, currentGeneration) {
    if (generation !== currentGeneration) return;
    if (!presentation.apply(effect)) {
      void failSession('invalid-transcript', currentGeneration);
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
    if (event?.type === 'session.created') {
      if (!socketOpenObserved || ownedSocket.readyState !== WebSocket.OPEN || sessionUpdateSent) {
        void failSession('invalid-event', currentGeneration);
        return;
      }
      establishmentTimer.clear();
      ownedSocket.send(JSON.stringify(buildSessionUpdate()));
      sessionUpdateSent = true;
      armEstablishmentDeadline('session-updated-timeout', currentGeneration, ownedSocket);
    } else if (event?.type === 'session.updated') {
      if (!sessionUpdateSent) {
        void failSession('invalid-event', currentGeneration);
        return;
      }
      establishmentTimer.clear();
      streamReady = true;
    }
    for (const effect of protocol.consume(event)) applyEffect(effect, currentGeneration);
  }

  async function initialize(currentGeneration) {
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
          const encoded = JSON.stringify(buildAudioAppend(new Uint8Array(data.pcm)));
          if (!Number.isFinite(socket.bufferedAmount) || socket.bufferedAmount + encoded.length > MAXIMUM_SOCKET_BUFFERED_BYTES) {
            void failSession('microphone-backpressure', currentGeneration);
            return;
          }
          socket.send(encoded);
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
      const ownedSocket = new WebSocket(`${scheme}//${location.host}/ws`);
      socket = ownedSocket;
      ownedSocket.addEventListener('open', () => {
        handleSocketOpen(currentGeneration, ownedSocket);
      }, {once: true});
      ownedSocket.addEventListener('message', ({data}) => {
        handleServerMessage(data, currentGeneration, ownedSocket);
      });
      ownedSocket.addEventListener('close', () => {
        if (generation === currentGeneration && socket === ownedSocket) {
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
    streamReady = false;
    sessionUpdateSent = false;
    socketOpenObserved = false;
    protocol = new ProtocolState();
    presentation.beginSession();
    onNotice('');
    publishSnapshot();

    const pending = initialize(currentGeneration);
    startPromise = pending;
    void pending.then(() => {
      if (startPromise === pending) startPromise = null;
    });
    return pending;
  }

  async function releaseResources(closeSocket) {
    streamReady = false;
    sessionUpdateSent = false;
    socketOpenObserved = false;
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

  return {start, stop, mute, resumeAudio};
}
