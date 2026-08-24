import { decodePCM16Base64 } from './audio-core.mjs';
import { initialProtocolState, reduceProtocolEvent } from './protocol-state.mjs';

const CAPTURE_FRAME_BYTES = 960;
const MAX_OUTPUT_AUDIO_BYTES = 96_000;
const MAX_STATUS = 50;

const elements = typeof document === 'undefined' ? null : {
  startStop: document.querySelector('#start-stop'),
  mute: document.querySelector('#mute'),
  status: document.querySelector('#connection-status'),
  capabilities: document.querySelector('#capabilities'),
  turns: document.querySelector('#turns'),
  events: document.querySelector('#events'),
};

let state = initialProtocolState();
let activeSession = null;

if (elements) {
  elements.startStop.addEventListener('click', () => {
    if (activeSession) {
      stopSession('user-stopped');
      return;
    }
    void startSession();
  });

  elements.mute.addEventListener('click', () => {
    if (!activeSession?.captureNode) {
      return;
    }
    state = { ...state, muted: !state.muted };
    activeSession.captureNode.port.postMessage({ type: 'enabled', enabled: !state.muted && activeSession.configured });
    render();
  });

  render();
}

async function startSession() {
  const session = createSessionResources();
  activeSession = session;
  state = initialProtocolState();
  setGatewayMessage('Verifying local gateway audio contract…');
  render();

  try {
    await fetchCapabilities();
    if (!isActive(session)) {
      stopSession('cancelled', session);
      return;
    }
    setGatewayMessage('Verified PCM16 · 24 kHz · mono. Preparing microphone…');

    session.stream = await navigator.mediaDevices.getUserMedia({
      audio: { channelCount: 1, echoCancellation: true, noiseSuppression: true },
      video: false,
    });
    if (!isActive(session)) {
      stopSession('cancelled', session);
      return;
    }

    session.audioContext = new AudioContext({ latencyHint: 'interactive' });
    await session.audioContext.audioWorklet.addModule('/capture-worklet.js');
    await session.audioContext.audioWorklet.addModule('/playback-worklet.js');
    if (!isActive(session)) {
      stopSession('cancelled', session);
      return;
    }

    session.sourceNode = session.audioContext.createMediaStreamSource(session.stream);
    session.captureNode = new AudioWorkletNode(session.audioContext, 'capture-processor');
    session.playbackNode = new AudioWorkletNode(session.audioContext, 'playback-processor');
    session.silentGainNode = session.audioContext.createGain();
    session.silentGainNode.gain.value = 0;
    session.sourceNode.connect(session.captureNode);
    session.captureNode.connect(session.silentGainNode).connect(session.audioContext.destination);
    session.playbackNode.connect(session.audioContext.destination);
    session.captureNode.port.onmessage = (message) => handleCaptureFrame(session, message.data);
    session.playbackNode.port.onmessage = (message) => handlePlaybackMessage(session, message.data);
    await session.audioContext.resume();
    if (!isActive(session)) {
      stopSession('cancelled', session);
      return;
    }

    session.socket = new WebSocket(new URL('/ws', location.href).href.replace(/^http/, 'ws'));
    session.socket.onmessage = (message) => handleSocketMessage(session, message.data);
    session.socket.onerror = () => stopSession('connection-error', session);
    session.socket.onclose = () => stopSession('connection-closed', session);
    setGatewayMessage('Microphone ready. Connecting to the local gateway…');
    render();
  } catch {
    stopSession('start-failed', session);
  }
}

function createSessionResources() {
  return {
    stopped: false,
    updateSent: false,
    configured: false,
    stream: null,
    audioContext: null,
    sourceNode: null,
    captureNode: null,
    playbackNode: null,
    silentGainNode: null,
    socket: null,
  };
}

async function fetchCapabilities() {
  const response = await fetch('/api/capabilities', { headers: { Accept: 'application/json' } });
  if (!response.ok) {
    throw new Error('capabilities request failed');
  }
  const document = await response.json();
  const audio = document?.audio;
  if (audio?.encoding !== 'pcm_s16le' || audio?.transport !== 'base64' || audio?.sample_rate !== 24_000 || audio?.channels !== 1 ||
    audio?.bytes_per_sample !== 2 || audio?.frame_bytes !== CAPTURE_FRAME_BYTES) {
    throw new Error('unsupported audio contract');
  }
}

function handleCaptureFrame(session, message) {
  if (!canSendCaptureFrame({
    active: isActive(session),
    configured: session.configured,
    muted: state.muted,
    socketOpen: session.socket?.readyState === WebSocket.OPEN,
    message,
  })) {
    return;
  }

  session.socket.send(JSON.stringify({
    type: 'input_audio_buffer.append',
    audio: base64Encode(message.pcm),
  }));
}

function handlePlaybackMessage(session, message) {
  if (!isActive(session) || message?.type !== 'overflow') {
    return;
  }
  recordLocalStatus('playback-overflow');
  render();
}

function handleSocketMessage(session, payload) {
  if (!isActive(session) || typeof payload !== 'string') {
    return;
  }

  let event;
  try {
    event = JSON.parse(payload);
  } catch {
    recordLocalStatus('invalid-message');
    render();
    return;
  }
  if (!event || typeof event !== 'object' || Array.isArray(event)) {
    recordLocalStatus('invalid-message');
    render();
    return;
  }

  if (event.type === 'response.output_audio.delta') {
    appendOutputAudio(session, event.delta);
  }
  state = reduceProtocolEvent(state, event);
  applyEffect(session, state.effect);
  state = { ...state, effect: null };

  const readiness = sessionReadinessAfter(session, event.type);
  if (readiness.sendUpdate && session.socket?.readyState === WebSocket.OPEN) {
    session.socket.send(JSON.stringify({
      type: 'session.update',
      session: {
        type: 'realtime',
        audio: {
          input: { format: { type: 'audio/pcm', rate: 24000 }, turn_detection: { type: 'server_vad' } },
          output: { format: { type: 'audio/pcm', rate: 24000 } },
        },
        output_modalities: ['audio'],
      },
    }));
    session.updateSent = true;
    setGatewayMessage('Awaiting local gateway session acknowledgement…');
  }
  if (readiness.enableCapture) {
    session.configured = true;
    session.captureNode?.port.postMessage({ type: 'enabled', enabled: !state.muted });
    setGatewayMessage('Connected with verified PCM16 audio. Speak when ready.');
  }
  render();
}

function appendOutputAudio(session, encoded) {
  try {
    const samples = decodePCM16Base64(encoded, MAX_OUTPUT_AUDIO_BYTES);
    const pcm = samples.buffer.slice(samples.byteOffset, samples.byteOffset + samples.byteLength);
    session.playbackNode?.port.postMessage({ type: 'append', pcm }, [pcm]);
  } catch {
    recordLocalStatus('invalid-audio');
  }
}

function applyEffect(session, effect) {
  if (effect === 'clear-playback') {
    session.playbackNode?.port.postMessage({ type: 'clear' });
  }
}

function stopSession(reason, session = activeSession) {
  if (!session) {
    restoreIdleControls();
    return;
  }

  const wasActive = activeSession === session;
  session.stopped = true;
  if (wasActive) {
    activeSession = null;
  }

  session.captureNode?.port.postMessage({ type: 'enabled', enabled: false });
  session.captureNode?.port.postMessage({ type: 'reset' });
  session.playbackNode?.port.postMessage({ type: 'clear' });
  if (session.captureNode) {
    session.captureNode.port.onmessage = null;
  }
  if (session.playbackNode) {
    session.playbackNode.port.onmessage = null;
  }
  session.sourceNode?.disconnect();
  session.captureNode?.disconnect();
  session.playbackNode?.disconnect();
  session.silentGainNode?.disconnect();
  session.stream?.getTracks().forEach((track) => track.stop());

  if (session.socket) {
    session.socket.onmessage = null;
    session.socket.onerror = null;
    session.socket.onclose = null;
    if (session.socket.readyState === WebSocket.CONNECTING || session.socket.readyState === WebSocket.OPEN) {
      session.socket.close();
    }
  }
  if (session.audioContext && session.audioContext.state !== 'closed') {
    void session.audioContext.close();
  }

  session.stream = null;
  session.audioContext = null;
  session.sourceNode = null;
  session.captureNode = null;
  session.playbackNode = null;
  session.silentGainNode = null;
  session.socket = null;
  session.updateSent = false;
  session.configured = false;

  if (wasActive) {
    state = initialProtocolState();
    if (reason === 'start-failed') {
      state = reduceProtocolEvent(state, { type: 'client.start_failed' });
    } else {
      recordLocalStatus(reason === 'user-stopped' ? 'stopped' : 'disconnected');
    }
    setGatewayMessage('Waiting to verify the local gateway audio contract.');
    restoreIdleControls();
  }
}

function isActive(session) {
  return activeSession === session && !session.stopped;
}

export function sessionReadinessAfter(session, eventType) {
  const updateSent = Boolean(session?.updateSent);
  const configured = Boolean(session?.configured);
  if (eventType === 'session.created' && !updateSent) {
    return { updateSent: true, configured: false, sendUpdate: true, enableCapture: false };
  }
  if (eventType === 'session.updated' && updateSent && !configured) {
    return { updateSent: true, configured: true, sendUpdate: false, enableCapture: true };
  }
  return { updateSent, configured, sendUpdate: false, enableCapture: false };
}

export function canSendCaptureFrame({ active, configured, muted, socketOpen, message }) {
  return active && configured && !muted && socketOpen && message?.type === 'frame' &&
    message.pcm instanceof ArrayBuffer && message.pcm.byteLength === CAPTURE_FRAME_BYTES;
}

function recordLocalStatus(kind) {
  state = { ...state, status: [...state.status, { kind }].slice(-MAX_STATUS) };
}

function restoreIdleControls() {
  render();
}

function setGatewayMessage(message) {
  const existing = elements.capabilities.querySelector('p');
  if (existing) {
    existing.textContent = message;
  }
}

function render() {
  elements.startStop.textContent = activeSession ? 'Stop' : 'Start';
  elements.startStop.disabled = false;
  elements.mute.disabled = !activeSession?.stream;
  elements.mute.textContent = state.muted ? 'Unmute microphone' : 'Mute microphone';
  elements.status.textContent = connectionLabel();
  renderTurns();
  renderEvents();
}

function connectionLabel() {
  if (!activeSession) {
    return 'Idle';
  }
  return {
    idle: 'Starting local voice session…',
    ready: 'Connected',
    'user-speaking': 'Listening…',
    waiting: 'Waiting for a response…',
    'assistant-thinking': 'Assistant is responding…',
    'assistant-speaking': 'Assistant is speaking…',
    error: 'Protocol error',
  }[state.phase] ?? 'Connecting…';
}

function renderTurns() {
  elements.turns.replaceChildren();
  for (const turn of state.turns) {
    elements.turns.append(createTurnElement(turn));
  }
  for (const draft of Object.values(state.drafts)) {
    elements.turns.append(createTurnElement(draft, true));
  }
}

function createTurnElement(turn, isDraft = false) {
  const item = document.createElement('li');
  item.className = `turn turn--${turn.speaker}${isDraft ? ' turn--draft' : ''}`;
  const speaker = document.createElement('span');
  speaker.className = 'turn__speaker';
  speaker.textContent = turn.speaker === 'assistant' ? 'Assistant' : 'You';
  const text = document.createElement('span');
  text.textContent = turn.text || '…';
  item.append(speaker, text);
  return item;
}

function renderEvents() {
  elements.events.replaceChildren();
  for (const entry of state.status) {
    const item = document.createElement('li');
    item.textContent = statusLabel(entry);
    elements.events.append(item);
  }
}

export function statusLabel(entry) {
  const labels = {
    'session-created': 'Session created.',
    'session-updated': 'Session updated.',
    'session-reconnect-updated': 'Session reconnected.',
    'user-speaking': 'Speech detected; clearing playback.',
    'user-finished-speaking': 'Speech ended.',
    'audio-committed': 'Audio submitted.',
    'response-created': 'Response started.',
    'assistant-audio': 'Audio received.',
    'assistant-audio-done': 'Audio completed.',
    'response-done': 'Response completed.',
    'protocol-error': `Protocol error${entry.code ? ` (${entry.code})` : ''}.`,
    'unknown-event': 'Unsupported protocol event ignored.',
    'playback-overflow': 'Playback buffer was trimmed.',
    'invalid-audio': 'Invalid output audio was ignored.',
    'invalid-message': 'Invalid gateway message was ignored.',
    'start-failed': 'Unable to start the local voice session.',
    stopped: 'Session stopped.',
    disconnected: 'Session disconnected.',
  };
  return labels[entry.kind] ?? 'Gateway status updated.';
}

function base64Encode(buffer) {
  const bytes = new Uint8Array(buffer);
  let binary = '';
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary);
}
