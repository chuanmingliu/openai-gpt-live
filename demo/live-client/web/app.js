import {createBrowserRuntime} from './browser-runtime.mjs';
import {ClientPresentation} from './client-state.mjs';
import {ProtocolEventJournal, protocolEventDetail} from './protocol-log.mjs';
import {createVoiceProfileState} from './voice-profile-state.mjs';

const terminalStatuses = new Set(['idle', 'stopped', 'error']);

const statusLabels = {
  idle: 'Ready to start',
  connecting: 'Connecting microphone…',
  configuring: 'Configuring server VAD…',
  listening: 'Listening',
  'user-speaking': 'You are speaking',
  'assistant-thinking': 'Response pending',
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
  'services-not-ready': 'The local voice services are not ready. Open /setup and start Speech, PostgreSQL, and Gateway first.',
  'socket-open-timeout': 'The local voice connection did not open in time. Select Start listening to reconnect.',
  'transport-closed': 'The local voice connection closed. Select Start listening to reconnect.',
  'profile-mismatch': 'The gateway confirmed a different profile version. Select Start listening to reconnect.',
  'profile-unavailable': 'The selected profile is unavailable. Choose another profile, then select Start listening.',
};

const toolSaveLabels = {
  ready: 'Policy ready.',
  saving: 'Saving policy…',
  'awaiting-ack': 'Waiting for gateway confirmation…',
  saved: 'Policy saved and active.',
  error: 'Policy action failed. Try again.',
};

const toolTestLabels = {
  running: 'Testing Web Search…',
  passed: 'Web Search test passed.',
  error: 'Web Search test failed.',
};

export function toolPolicyStatusText(tools) {
  if (tools.lastOperation === 'test') {
    if (tools.testStatus === 'error' && tools.testCode === 'provider_challenge') {
      return 'DuckDuckGo temporarily rejected automated search. Try again later.';
    }
    if (tools.testStatus === 'error' && tools.testCode === 'network_policy') {
      return 'Local DNS or proxy policy blocked DuckDuckGo Web Search.';
    }
    return toolTestLabels[tools.testStatus] ?? '';
  }
  return toolSaveLabels[tools.saveStatus] ?? '';
}

const toolPhaseLabels = {
  queued: 'Queued',
  searching: 'Searching',
  reading_sources: 'Reading sources',
  preparing_answer: 'Preparing answer',
  needs_input: 'Needs input',
  completed: 'Completed',
  failed: 'Failed',
  cancelled: 'Stopped',
};

export function toolProgressView(item) {
  const step = Number.isInteger(item?.progressStep) ? Math.min(4, Math.max(1, item.progressStep)) : 1;
  let state = 'active';
  if (item?.phase === 'completed') state = 'completed';
  else if (item?.phase === 'failed') state = 'failed';
  else if (item?.phase === 'cancelled') state = 'cancelled';
  else if (item?.phase === 'needs_input') state = 'needs-input';
  return {
    label: toolPhaseLabels[item?.phase] ?? 'Updated',
    state,
    step,
    segments: Array.from({length: 4}, (_, index) => index < step),
  };
}

export function safeSourceHref(value) {
  if (typeof value !== 'string' || value.length > 2048 || !/^[\x20-\x7e]+$/.test(value)) return null;
  try {
    const parsed = new URL(value);
    return parsed.protocol === 'https:' && parsed.username === '' && parsed.password === '' ? parsed.href : null;
  } catch {
    return null;
  }
}

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
    protocolClear: document.querySelector('#protocol-clear'),
    protocolEmpty: document.querySelector('#empty-protocol-log'),
    protocolLog: document.querySelector('#protocol-log'),
    resumeAudio: document.querySelector('#resume-audio'),
    start: document.querySelector('#start'),
    status: document.querySelector('#status'),
    stop: document.querySelector('#stop'),
    toolAnnouncement: document.querySelector('#tool-announcement'),
    toolEnabled: document.querySelector('#tool-web-search'),
    toolForm: document.querySelector('#tool-policy-form'),
    toolMaxResults: document.querySelector('#tool-max-results'),
    toolRegion: document.querySelector('#tool-region'),
    toolSafeSearch: document.querySelector('#tool-safe-search'),
    toolSave: document.querySelector('#tool-save'),
    toolStatus: document.querySelector('#tool-policy-status'),
    toolTest: document.querySelector('#tool-test'),
    toolTimeout: document.querySelector('#tool-timeout'),
    transcript: document.querySelector('#transcript'),
    voiceProfile: document.querySelector('#voice-profile'),
    voiceProfileNote: document.querySelector('#voice-profile-note'),
  };

  const presentation = new ClientPresentation();
  const protocolJournal = new ProtocolEventJournal();
  const profileState = createVoiceProfileState();
  presentation.endSession('idle');
  let noticeCode = '';
  let renderFrame = null;
  let protocolRenderFrame = null;
  let toolFormDirty = false;

  function renderTurn(item) {
    const turn = document.createElement('article');
    turn.className = `turn ${item.role}${item.final ? ' final' : ''}`;
    const label = document.createElement('div');
    label.className = 'turn-role';
    label.textContent = item.role === 'user' ? 'You' : 'Assistant';
    const text = document.createElement('p');
    text.className = 'turn-text';
    text.textContent = item.text;
    turn.append(label, text);
    return turn;
  }

  function renderToolCard(item) {
    const progressView = toolProgressView(item);
    const card = document.createElement('article');
    card.className = `tool-card state-${progressView.state}${item.terminal ? ' terminal' : ''}`;
    const header = document.createElement('div');
    header.className = 'tool-card-header';
    const title = document.createElement('span');
    title.className = 'tool-card-title';
    title.textContent = 'Web Search';
    const phase = document.createElement('span');
    phase.className = 'tool-card-phase';
    phase.textContent = progressView.label;
    header.append(title, phase);
    const progress = document.createElement('div');
    progress.className = 'tool-card-progress';
    progress.setAttribute('role', 'progressbar');
    progress.setAttribute('aria-label', 'Web Search progress');
    progress.setAttribute('aria-valuemin', '1');
    progress.setAttribute('aria-valuemax', '4');
    progress.setAttribute('aria-valuenow', String(progressView.step));
    progress.setAttribute('aria-valuetext', progressView.label);
    for (const complete of progressView.segments) {
      const segment = document.createElement('span');
      segment.className = `tool-card-progress-segment${complete ? ' complete' : ''}`;
      segment.setAttribute('aria-hidden', 'true');
      progress.append(segment);
    }
    const summary = document.createElement('p');
    summary.className = 'tool-card-summary';
    summary.textContent = item.summary;
    card.append(header, progress, summary);
    if (item.sources.length > 0) {
      const sources = document.createElement('ol');
      sources.className = 'tool-sources';
      for (const source of item.sources) {
        const href = safeSourceHref(source.url);
        if (!href) continue;
        const row = document.createElement('li');
        const anchor = document.createElement('a');
        anchor.href = href;
        anchor.target = '_blank';
        anchor.rel = 'noopener noreferrer';
        anchor.textContent = source.title;
        row.append(anchor);
        sources.append(row);
      }
      if (sources.children.length > 0) card.append(sources);
    }
    return card;
  }

  function renderToolPolicy(tools) {
    if (!toolFormDirty) {
      elements.toolEnabled.checked = tools.policy.web_search.enabled;
      elements.toolSafeSearch.value = tools.policy.web_search.safe_search;
      elements.toolRegion.value = tools.policy.web_search.region;
      elements.toolMaxResults.value = String(tools.policy.web_search.max_results);
      elements.toolTimeout.value = String(tools.policy.web_search.timeout_ms);
    }
    elements.toolSave.disabled = tools.saveStatus === 'saving' || tools.saveStatus === 'awaiting-ack';
    elements.toolTest.disabled = tools.testStatus === 'running';
    elements.toolStatus.textContent = toolPolicyStatusText(tools);
  }

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
    elements.empty.hidden = snapshot.timeline.length > 0;
    for (const item of elements.transcript.querySelectorAll('article.turn, article.tool-card')) item.remove();
    for (const item of snapshot.timeline) elements.transcript.append(item.kind === 'tool' ? renderToolCard(item) : renderTurn(item));
    renderToolPolicy(snapshot.tools);
    elements.transcript.scrollTop = elements.transcript.scrollHeight;
    elements.finalAnnouncement.textContent = presentation.drainAnnouncements().join(' ');
    elements.toolAnnouncement.textContent = presentation.drainToolAnnouncements().join(' ');
  }

  function renderProtocolLog() {
    const entries = protocolJournal.snapshot();
    elements.protocolEmpty.hidden = entries.length > 0;
    for (const item of elements.protocolLog.querySelectorAll('.protocol-event')) item.remove();
    for (const entry of entries) {
      const row = document.createElement('div');
      row.className = `protocol-event ${entry.direction}`;
      const time = document.createElement('time');
      time.className = 'protocol-time';
      const instant = new Date(entry.at);
      time.dateTime = instant.toISOString();
      time.textContent = instant.toISOString().slice(11, 23);
      const direction = document.createElement('span');
      direction.className = 'protocol-direction';
      direction.textContent = entry.direction === 'inbound' ? '← IN' : '→ OUT';
      const event = document.createElement('span');
      event.className = 'protocol-type';
      event.textContent = entry.type;
      const detailText = protocolEventDetail(entry);
      if (detailText !== '') {
        const detail = document.createElement('small');
        detail.className = 'protocol-detail';
        detail.textContent = detailText;
        event.append(detail);
      }
      row.append(time, direction, event);
      elements.protocolLog.append(row);
    }
    elements.protocolLog.scrollTop = elements.protocolLog.scrollHeight;
  }

  function scheduleRender() {
    if (renderFrame !== null) return;
    renderFrame = requestAnimationFrame(() => {
      renderFrame = null;
      renderPresentation(presentation.snapshot());
    });
  }

  function scheduleProtocolRender() {
    if (protocolRenderFrame !== null) return;
    protocolRenderFrame = requestAnimationFrame(() => {
      protocolRenderFrame = null;
      renderProtocolLog();
    });
  }

  function setNotice(code = '') {
    noticeCode = code;
    // A failed or stopped session releases the selection so a different
    // published profile can be chosen before reconnecting.
    if (elements.voiceProfile) elements.voiceProfile.disabled = false;
    scheduleRender();
  }

  // Published options are loaded once before the first session; a failure
  // leaves only the built-in profile selectable.
  async function loadVoiceProfiles() {
    if (!elements.voiceProfile) return;
    try {
      const response = await fetch('/api/setup/v1/profiles?state=published', {
        cache: 'no-store',
        credentials: 'same-origin',
      });
      if (!response.ok) return;
      const options = profileState.loadPublished(await response.json());
      elements.voiceProfile.replaceChildren();
      for (const option of options) {
        const item = document.createElement('option');
        item.value = `${option.id}/${option.version}`;
        item.textContent = option.id === 'builtin'
          ? 'Built-in voice assistant'
          : `${option.id} v${option.version}${option.active ? ' (active)' : ''}`;
        elements.voiceProfile.append(item);
      }
      const selection = profileState.selection();
      elements.voiceProfile.value = `${selection.id}/${selection.version}`;
    } catch {
      // The built-in profile remains selectable.
    }
  }

  const runtime = createBrowserRuntime({
    mediaDevices: navigator.mediaDevices,
    AudioContext: window.AudioContext,
    WebSocket: window.WebSocket,
    location: window.location,
    presentation,
    onSnapshot: scheduleRender,
    onNotice: setNotice,
    onProtocolEvent: (direction, event) => {
      protocolJournal.record(direction, event);
      scheduleProtocolRender();
    },
    fetch: window.fetch.bind(window),
    profileState,
  });

  if (elements.voiceProfile) {
    elements.voiceProfile.addEventListener('change', () => {
      const [id, version] = elements.voiceProfile.value.split('/');
      try {
        profileState.select({id, version: Number(version)});
      } catch {
        const selection = profileState.selection();
        elements.voiceProfile.value = `${selection.id}/${selection.version}`;
      }
    });
  }

  elements.start.addEventListener('click', () => {
    protocolJournal.clear();
    scheduleProtocolRender();
    if (elements.voiceProfile) elements.voiceProfile.disabled = true;
    void runtime.start();
  });
  elements.stop.addEventListener('click', () => {
    void runtime.stop().finally(() => {
      if (elements.voiceProfile) elements.voiceProfile.disabled = false;
    });
  });
  elements.mute.addEventListener('click', () => { runtime.mute(!presentation.isMuted()); });
  elements.resumeAudio.addEventListener('click', () => { void runtime.resumeAudio(); });
  elements.protocolClear.addEventListener('click', () => {
    protocolJournal.clear();
    scheduleProtocolRender();
  });
  elements.toolForm.addEventListener('input', () => { toolFormDirty = true; });
  elements.toolForm.addEventListener('submit', (event) => {
    event.preventDefault();
    const policy = {
      web_search: {
        enabled: elements.toolEnabled.checked,
        safe_search: elements.toolSafeSearch.value,
        region: elements.toolRegion.value,
        max_results: Number(elements.toolMaxResults.value),
        timeout_ms: Number(elements.toolTimeout.value),
      },
    };
    toolFormDirty = false;
    void runtime.saveToolPolicy(policy);
  });
  elements.toolTest.addEventListener('click', () => { void runtime.testToolPolicy(); });
  window.addEventListener('beforeunload', () => { void runtime.stop(); });

  scheduleRender();
  scheduleProtocolRender();
  void loadVoiceProfiles();
}
