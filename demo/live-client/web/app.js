import {createBrowserRuntime} from './browser-runtime.mjs';
import {ClientPresentation} from './client-state.mjs';

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

const presentation = new ClientPresentation();
presentation.endSession('idle');
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

const runtime = createBrowserRuntime({
  mediaDevices: navigator.mediaDevices,
  AudioContext: window.AudioContext,
  WebSocket: window.WebSocket,
  location: window.location,
  presentation,
  onSnapshot: scheduleRender,
  onNotice: setNotice,
});

elements.start.addEventListener('click', () => { void runtime.start(); });
elements.stop.addEventListener('click', () => { void runtime.stop(); });
elements.mute.addEventListener('click', () => { runtime.mute(!presentation.isMuted()); });
window.addEventListener('beforeunload', () => { void runtime.stop(); });

scheduleRender();
