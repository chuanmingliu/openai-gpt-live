import {createBrowserRuntime} from './browser-runtime.mjs';
import {ClientPresentation} from './client-state.mjs';

const terminalStatuses = new Set(['idle', 'stopped', 'error']);

const statusLabels = {
  idle: 'Ready to start',
  connecting: 'Connecting microphone…',
  configuring: 'Configuring server VAD…',
  listening: 'Listening',
  'user-speaking': 'You are speaking',
  'assistant-thinking': 'Assistant is thinking',
  'assistant-buffering': 'Assistant audio is buffered. Select Stop to end this session.',
  'assistant-speaking': 'Assistant is speaking',
  'audio-suspended': 'Audio is paused. Select Enable audio to resume playback.',
  stopped: 'Stopped',
  error: 'Session needs attention',
};

const errorLabels = {
  'audio-overflow': 'Playback could not keep up. Select Start listening to reconnect.',
  'audio-initialization-failed': 'Browser audio could not start. Check audio permissions, then select Start listening to reconnect.',
  'gateway-error': 'The gateway ended this turn. Select Start listening to reconnect.',
  'invalid-audio': 'The gateway returned invalid audio. Select Start listening to reconnect.',
  'invalid-event': 'The gateway returned an invalid event. Select Start listening to reconnect.',
  'invalid-transcript': 'The gateway returned an invalid transcript event. Select Start listening to reconnect.',
  'microphone-unavailable': 'Microphone access is required. Check browser permission, then select Start listening to reconnect.',
  'microphone-backpressure': 'The local connection could not keep up with microphone audio. Select Start listening to reconnect.',
  'microphone-ended': 'Microphone access ended. Check the selected device and browser permission, then select Start listening to reconnect.',
  'microphone-suspended': 'Microphone capture paused and could not resume. Select Start listening to reconnect.',
  'session-created-timeout': 'The gateway did not create a voice session in time. Select Start listening to reconnect.',
  'session-updated-timeout': 'The gateway did not confirm voice configuration in time. Select Start listening to reconnect.',
  'socket-open-timeout': 'The local voice connection did not open in time. Select Start listening to reconnect.',
  'transport-closed': 'The local voice connection closed. Select Start listening to reconnect.',
};

export function presentationUI(snapshot, noticeCode = '') {
  const active = !terminalStatuses.has(snapshot.status);
  const muted = Boolean(snapshot.muted);
  const audioSuspended = snapshot.status === 'audio-suspended';
  return {
    muteDisabled: !active,
    muteLabel: muted ? 'Unmute' : 'Mute',
    noticeText: noticeCode ? (errorLabels[noticeCode] ?? 'The session stopped safely. Select Start listening to reconnect.') : '',
    resumeAudioDisabled: !audioSuspended,
    resumeAudioHidden: !audioSuspended,
    startDisabled: active,
    statusText: muted && !audioSuspended
      ? 'Microphone muted. Select Unmute to send your voice.'
      : (statusLabels[snapshot.status] ?? statusLabels.error),
    stopDisabled: !active,
  };
}

if (typeof document !== 'undefined') {
  const elements = {
    connection: document.querySelector('.connection'),
    empty: document.querySelector('#empty-transcript'),
    finalAnnouncement: document.querySelector('#final-announcement'),
    mute: document.querySelector('#mute'),
    notice: document.querySelector('#notice'),
    resumeAudio: document.querySelector('#resume-audio'),
    start: document.querySelector('#start'),
    status: document.querySelector('#status'),
    stop: document.querySelector('#stop'),
    transcript: document.querySelector('#transcript'),
  };

  const presentation = new ClientPresentation();
  presentation.endSession('idle');
  let noticeCode = '';
  let renderFrame = null;

  function renderPresentation(snapshot) {
    const ui = presentationUI(snapshot, noticeCode);
    elements.connection.dataset.state = snapshot.status;
    elements.status.textContent = ui.statusText;
    elements.start.disabled = ui.startDisabled;
    elements.stop.disabled = ui.stopDisabled;
    elements.mute.disabled = ui.muteDisabled;
    elements.mute.setAttribute('aria-pressed', String(snapshot.muted));
    elements.mute.innerHTML = `<span aria-hidden="true">⌁</span> ${ui.muteLabel}`;
    elements.resumeAudio.disabled = ui.resumeAudioDisabled;
    elements.resumeAudio.hidden = ui.resumeAudioHidden;
    elements.notice.textContent = ui.noticeText;
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
    elements.finalAnnouncement.textContent = presentation.drainAnnouncements().join(' ');
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
  elements.resumeAudio.addEventListener('click', () => { void runtime.resumeAudio(); });
  window.addEventListener('beforeunload', () => { void runtime.stop(); });

  scheduleRender();
}
