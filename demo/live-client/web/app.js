import {decodePCM16Base64, PCMPlaybackPacer, SinglePendingTimer} from './audio-core.mjs';
import {ClientPresentation} from './client-state.mjs';
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

const announcement = document.createElement('p');
announcement.setAttribute('aria-live', 'polite');
announcement.style.cssText = 'position:absolute;width:1px;height:1px;padding:0;margin:-1px;overflow:hidden;clip:rect(0,0,0,0);white-space:nowrap;border:0';
elements.transcript.before(announcement);
elements.transcript.setAttribute('aria-live', 'off');

const statusLabels = {
  idle: 'Ready to start',
  connecting: 'Connecting microphone…',
  configuring: 'Configuring server VAD…',
  listening: 'Listening',
  'user-speaking': 'You are speaking',
  'assistant-thinking': 'Assistant is thinking',
  'assistant-buffering': 'Assistant is preparing to speak',
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
const presentation = new ClientPresentation();
presentation.endSession('idle');
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
const playbackPacer = new PCMPlaybackPacer(120);
const playbackFeedTimer = new SinglePendingTimer();
let intentionalStop = false;
let noticeCode = '';
let renderFrame = null;

function renderPresentation(snapshot) {
  const active = !['idle', 'stopped', 'error'].includes(snapshot.status);
  elements.connection.dataset.state = snapshot.status;
  elements.status.textContent = statusLabels[snapshot.status] ?? statusLabels.error;
  elements.start.disabled = active;
  elements.stop.disabled = !active;
  elements.mute.disabled = !active;
  elements.mute.setAttribute('aria-pressed', String(snapshot.muted));
  elements.mute.innerHTML = `<span aria-hidden="true">⌁</span> ${snapshot.muted ? 'Unmute' : 'Mute'}`;
  elements.notice.textContent = noticeCode ? (errorLabels[noticeCode] ?? 'The session stopped safely. Try again.') : '';
  elements.empty.hidden = snapshot.turns.length > 0;
  for (const turn of elements.transcript.querySelectorAll('article.turn')) turn.remove();
  for (const item of snapshot.turns) {
    const turn = document.createElement('article');
    turn.className = `turn ${item.role}${item.final ? ' final' : ''}`;
    const label = document.createElement('div');
    label.className = 'turn-role';
    label.textContent = item.role === 'user' ? 'You' : 'Assistant';
    const text = document.createElement('p');
    text.className = 'turn-text';
    text.textContent = item.text;
    turn.append(label, text);
    elements.transcript.append(turn);
  }
  elements.transcript.scrollTop = elements.transcript.scrollHeight;
  announcement.textContent = presentation.drainAnnouncements().join(' ');
}

function scheduleRender() {
  if (renderFrame !== null) return;
  renderFrame = requestAnimationFrame(() => {
    renderFrame = null;
    renderPresentation(presentation.snapshot());
  });
}

function setNotice(code = '') {
  noticeCode = code;
  scheduleRender();
}

function clearPlayback() {
  const playbackGeneration = presentation.snapshot().playbackGeneration;
  playbackFeedTimer.clear();
  playbackPacer.clear(playbackContext?.currentTime ?? 0);
  playbackNode?.port.postMessage({type: 'clear', generation: playbackGeneration});
}

function pumpPlayback() {
  if (!playbackContext || !playbackNode) return;
  const delaySeconds = playbackPacer.refill(playbackContext.currentTime, (samples) => {
    playbackNode.port.postMessage({
      type: 'append',
      generation: presentation.snapshot().playbackGeneration,
      pcm: samples.buffer,
    }, [samples.buffer]);
  });
  if (delaySeconds !== null) {
    playbackFeedTimer.request(pumpPlayback, Math.max(5, Math.ceil(delaySeconds * 1000)));
  }
}

function applyEffect(effect) {
  presentation.apply(effect);
  switch (effect.type) {
    case 'clear-playback':
      clearPlayback();
      break;
    case 'audio': {
      presentation.apply({type: 'audio-buffered'});
      try {
        const samples = decodePCM16Base64(effect.encoded);
        if (!playbackPacer.append(samples)) {
          void failSession('audio-overflow');
          break;
        }
        pumpPlayback();
      } catch {
        void failSession('invalid-audio');
      }
      break;
    }
    case 'error':
      void failSession(errorLabels[effect.code] ? effect.code : 'gateway-error');
      break;
  }
  scheduleRender();
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
  protocol = new ProtocolState();
  presentation.beginSession();
  setNotice();
  scheduleRender();

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
      if (!streamReady || presentation.isMuted() || socket?.readyState !== WebSocket.OPEN || data?.type !== 'frame' || !(data.pcm instanceof ArrayBuffer)) return;
      try {
        socket.send(JSON.stringify(buildAudioAppend(new Uint8Array(data.pcm))));
      } catch {
        void failSession('invalid-audio');
      }
    };
    playbackNode.port.onmessage = ({data}) => {
      if (data?.type === 'overflow') void failSession('audio-overflow');
      if (data?.type === 'playing') presentation.apply({type: 'audio-playing'});
      if (data?.type === 'idle') presentation.apply({type: 'audio-idle'});
      scheduleRender();
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
  clearPlayback();
  const ownedSocket = socket;
  socket = null;
  if (closeSocket && ownedSocket && ownedSocket.readyState <= WebSocket.OPEN) ownedSocket.close(1000, 'demo stopped');
  captureNode?.port.postMessage({type: 'mute', value: true});
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
  presentation.endSession('stopped');
  setNotice();
  scheduleRender();
}

async function failSession(code) {
  generation += 1;
  intentionalStop = true;
  await releaseResources(true);
  presentation.endSession('error');
  setNotice(code);
  scheduleRender();
}

function setMuted(value) {
  if (!captureNode) return;
  presentation.setMuted(value);
  captureNode.port.postMessage({type: 'mute', value});
  scheduleRender();
}

elements.start.addEventListener('click', () => { void startSession(); });
elements.stop.addEventListener('click', () => { void stopSession(); });
elements.mute.addEventListener('click', () => { setMuted(!presentation.isMuted()); });
window.addEventListener('beforeunload', () => {
  intentionalStop = true;
  playbackFeedTimer.clear();
  if (socket?.readyState <= WebSocket.OPEN) socket.close(1000, 'page closed');
  if (mediaStream) for (const track of mediaStream.getTracks()) track.stop();
});

scheduleRender();
