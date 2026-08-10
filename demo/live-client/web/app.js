import {decodePCM16Base64} from './audio-core.mjs';
import {ProtocolState, buildAudioAppend, buildSessionUpdate} from './protocol-state.mjs';

const elements = {
  connection: document.querySelector('.connection'),
  empty: document.querySelector('#empty-transcript'),
  mute: document.querySelector('#mute'),
  notice: document.querySelector('#notice'),
  start: document.querySelector('#start'),
  status: document.querySelector('#status'),
  stop: document.querySelector('#stop'),
  transcript: document.querySelector('#transcript'),
};

const statusLabels = {
  idle: 'Ready to start',
  connecting: 'Connecting microphone…',
  configuring: 'Configuring server VAD…',
  listening: 'Listening',
  'user-speaking': 'You are speaking',
  'assistant-thinking': 'Assistant is thinking',
  'assistant-speaking': 'Assistant is speaking',
  stopped: 'Stopped',
  error: 'Session needs attention',
};

const errorLabels = {
  'audio-overflow': 'Playback could not keep up. The session was stopped safely.',
  'gateway-error': 'The gateway ended this turn. Stop and try again.',
  'invalid-audio': 'The gateway returned invalid audio. The session was stopped.',
  'invalid-event': 'The gateway returned an invalid event. The session was stopped.',
  'invalid-transcript': 'The gateway returned an invalid transcript event. The session was stopped.',
  'microphone-unavailable': 'Microphone access is required. Check browser permission and try again.',
  'transport-closed': 'The local voice connection closed. Stop and try again.',
};

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
let muted = false;
let playbackActive = false;
let intentionalStop = false;
const activeTurns = {user: null, assistant: null};

function setStatus(value) {
  elements.connection.dataset.state = value;
  elements.status.textContent = statusLabels[value] ?? statusLabels.error;
}

function setControls(active) {
  elements.start.disabled = active;
  elements.stop.disabled = !active;
  elements.mute.disabled = !active;
}

function setNotice(code = '') {
  elements.notice.textContent = code ? (errorLabels[code] ?? 'The session stopped safely. Try again.') : '';
}

function createTurn(role) {
  elements.empty.hidden = true;
  const turn = document.createElement('article');
  turn.className = `turn ${role}`;
  const label = document.createElement('div');
  label.className = 'turn-role';
  label.textContent = role === 'user' ? 'You' : 'Assistant';
  const text = document.createElement('p');
  text.className = 'turn-text';
  turn.append(label, text);
  elements.transcript.append(turn);
  activeTurns[role] = {turn, text};
  return activeTurns[role];
}

function renderTranscript(role, text, done) {
  const entry = activeTurns[role] ?? createTurn(role);
  entry.text.textContent = done ? text : entry.text.textContent + text;
  if (done) {
    entry.turn.classList.add('final');
    activeTurns[role] = null;
  }
  elements.transcript.scrollTop = elements.transcript.scrollHeight;
}

function applyEffect(effect) {
  switch (effect.type) {
    case 'status':
      setStatus(effect.value === 'listening' && playbackActive ? 'assistant-speaking' : effect.value);
      break;
    case 'clear-playback':
      playbackActive = false;
      playbackNode?.port.postMessage({type: 'clear'});
      break;
    case 'user-delta':
      renderTranscript('user', effect.text, false);
      break;
    case 'user-done':
      renderTranscript('user', effect.text, true);
      break;
    case 'assistant-delta':
      renderTranscript('assistant', effect.text, false);
      break;
    case 'assistant-done':
      renderTranscript('assistant', effect.text, true);
      break;
    case 'audio': {
      try {
        const samples = decodePCM16Base64(effect.encoded);
        playbackActive = true;
        playbackNode?.port.postMessage({type: 'append', pcm: samples.buffer}, [samples.buffer]);
        setStatus('assistant-speaking');
      } catch {
        void failSession('invalid-audio');
      }
      break;
    }
    case 'error':
      void failSession(errorLabels[effect.code] ? effect.code : 'gateway-error');
      break;
  }
}

function handleServerMessage(raw) {
  if (typeof raw !== 'string' || raw.length > (64 << 10)) {
    void failSession('invalid-event');
    return;
  }
  let event;
  try {
    event = JSON.parse(raw);
  } catch {
    void failSession('invalid-event');
    return;
  }
  if (event?.type === 'session.created') {
    if (socket?.readyState !== WebSocket.OPEN || sessionUpdateSent) {
      void failSession('invalid-event');
      return;
    }
    socket.send(JSON.stringify(buildSessionUpdate()));
    sessionUpdateSent = true;
  } else if (event?.type === 'session.updated') {
    streamReady = true;
  }
  for (const effect of protocol.consume(event)) applyEffect(effect);
}

async function startSession() {
  if (mediaStream || socket) return;
  const currentGeneration = ++generation;
  intentionalStop = false;
  streamReady = false;
  sessionUpdateSent = false;
  playbackActive = false;
  muted = false;
  protocol = new ProtocolState();
  setNotice();
  setControls(true);
  setStatus('connecting');
  elements.mute.textContent = '⌁ Mute';

  try {
    const acquired = await navigator.mediaDevices.getUserMedia({
      audio: {channelCount: 1, echoCancellation: true, noiseSuppression: true, autoGainControl: true},
    });
    if (generation !== currentGeneration) {
      for (const track of acquired.getTracks()) track.stop();
      return;
    }
    mediaStream = acquired;
    captureContext = new AudioContext({latencyHint: 'interactive'});
    playbackContext = new AudioContext({sampleRate: 24000, latencyHint: 'interactive'});
    await Promise.all([
      captureContext.audioWorklet.addModule('/capture-worklet.js'),
      playbackContext.audioWorklet.addModule('/playback-worklet.js'),
    ]);
    if (generation !== currentGeneration) return;

    captureNode = new AudioWorkletNode(captureContext, 'live-capture', {numberOfInputs: 1, numberOfOutputs: 1, outputChannelCount: [1]});
    playbackNode = new AudioWorkletNode(playbackContext, 'live-playback', {numberOfInputs: 0, numberOfOutputs: 1, outputChannelCount: [1]});
    mediaSource = captureContext.createMediaStreamSource(mediaStream);
    silentGain = captureContext.createGain();
    silentGain.gain.value = 0;
    mediaSource.connect(captureNode).connect(silentGain).connect(captureContext.destination);
    playbackNode.connect(playbackContext.destination);
    captureNode.port.onmessage = ({data}) => {
      if (!streamReady || muted || socket?.readyState !== WebSocket.OPEN || data?.type !== 'frame' || !(data.pcm instanceof ArrayBuffer)) return;
      try {
        socket.send(JSON.stringify(buildAudioAppend(new Uint8Array(data.pcm))));
      } catch {
        void failSession('invalid-audio');
      }
    };
    playbackNode.port.onmessage = ({data}) => {
      if (data?.type === 'overflow') void failSession('audio-overflow');
      if (data?.type === 'playing') playbackActive = true;
      if (data?.type === 'idle') {
        playbackActive = false;
        if (elements.connection.dataset.state === 'assistant-speaking') setStatus('listening');
      }
    };
    await Promise.all([captureContext.resume(), playbackContext.resume()]);

    const scheme = location.protocol === 'https:' ? 'wss:' : 'ws:';
    socket = new WebSocket(`${scheme}//${location.host}/ws`);
    socket.addEventListener('message', ({data}) => handleServerMessage(data));
    socket.addEventListener('close', () => {
      if (!intentionalStop) void failSession('transport-closed');
    }, {once: true});
    socket.addEventListener('error', () => {
      if (!intentionalStop) void failSession('transport-closed');
    }, {once: true});
  } catch {
    await failSession('microphone-unavailable');
  }
}

async function releaseResources(closeSocket) {
  streamReady = false;
  sessionUpdateSent = false;
  playbackActive = false;
  const ownedSocket = socket;
  socket = null;
  if (closeSocket && ownedSocket && ownedSocket.readyState <= WebSocket.OPEN) ownedSocket.close(1000, 'demo stopped');
  captureNode?.port.postMessage({type: 'mute', value: true});
  playbackNode?.port.postMessage({type: 'clear'});
  try { mediaSource?.disconnect(); } catch {}
  try { captureNode?.disconnect(); } catch {}
  try { silentGain?.disconnect(); } catch {}
  try { playbackNode?.disconnect(); } catch {}
  captureNode = null;
  playbackNode = null;
  mediaSource = null;
  silentGain = null;
  if (mediaStream) for (const track of mediaStream.getTracks()) track.stop();
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

async function stopSession() {
  generation += 1;
  intentionalStop = true;
  await releaseResources(true);
  muted = false;
  setControls(false);
  setNotice();
  setStatus('stopped');
}

async function failSession(code) {
  generation += 1;
  intentionalStop = true;
  await releaseResources(true);
  setControls(false);
  setStatus('error');
  setNotice(code);
}

function setMuted(value) {
  if (!captureNode) return;
  muted = value;
  captureNode.port.postMessage({type: 'mute', value});
  elements.mute.textContent = value ? '⌁ Unmute' : '⌁ Mute';
  if (value) setStatus('listening');
}

elements.start.addEventListener('click', () => { void startSession(); });
elements.stop.addEventListener('click', () => { void stopSession(); });
elements.mute.addEventListener('click', () => { setMuted(!muted); });
window.addEventListener('beforeunload', () => {
  intentionalStop = true;
  if (socket?.readyState <= WebSocket.OPEN) socket.close(1000, 'page closed');
  if (mediaStream) for (const track of mediaStream.getTracks()) track.stop();
});
