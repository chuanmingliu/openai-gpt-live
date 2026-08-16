import {createProfileAPI} from './profile-api.mjs';
import {createProfileWorkspace, profileNoticeText, PROFILE_TABS} from './setup-profile.mjs';

export const COMPONENTS = Object.freeze(['speech', 'postgres', 'gateway', 'worker', 'client']);

const STATES = new Set(['ready', 'stopped', 'starting', 'stopping', 'failed']);
const CODES = new Set([
  'ready', 'stopped', 'dependency_not_ready', 'dependency_running', 'operation_in_progress',
  'process_exited', 'missing_configuration', 'startup_timeout', 'operation_failed',
]);
const OPERATIONS = new Set([
  'speech:start', 'speech:stop', 'postgres:start', 'postgres:stop', 'postgres:migrate',
  'gateway:start', 'gateway:stop', 'worker:start', 'worker:stop', 'client:start', 'client:stop',
]);
const DEPENDENCIES = Object.freeze({
  speech: [], postgres: [], gateway: ['speech', 'postgres'], worker: ['postgres'], client: ['gateway'],
});
const DEPENDANTS = Object.freeze({
  speech: ['gateway'], postgres: ['gateway', 'worker'], gateway: ['client'], worker: [], client: [],
});
const CODE_COPY = Object.freeze({
  ready: 'Ready.',
  stopped: 'Stopped.',
  dependency_not_ready: 'A required service is not ready.',
  dependency_running: 'A dependent service is still running.',
  operation_in_progress: 'An operation is already in progress.',
  process_exited: 'The local process is not available.',
  missing_configuration: 'Configuration is unavailable.',
  startup_timeout: 'The operation timed out.',
  operation_failed: 'The operation could not be completed.',
});
const STATE_COPY = Object.freeze({
  ready: 'State: Ready', stopped: 'State: Stopped', starting: 'State: Starting',
  stopping: 'State: Stopping', failed: 'State: Failed', unknown: 'State: Unavailable',
});
const UNKNOWN = Object.freeze({state: 'unknown', code: 'unknown', ready: false, busy: false, owned: false});

function initialSnapshot() {
  return new Map(COMPONENTS.map((component) => [component, {...UNKNOWN}]));
}

function statusIsValid(value, expectedComponent) {
  return value !== null && typeof value === 'object'
    && Object.keys(value).length === 6
    && Object.keys(value).every((key) => ['component', 'state', 'code', 'ready', 'busy', 'owned'].includes(key))
    && value.component === expectedComponent
    && STATES.has(value.state)
    && CODES.has(value.code)
    && typeof value.ready === 'boolean'
    && typeof value.busy === 'boolean'
    && typeof value.owned === 'boolean';
}

function decodeSnapshot(value) {
  if (value === null || typeof value !== 'object' || Array.isArray(value)
      || Object.keys(value).length !== 1 || !Array.isArray(value.components)
      || value.components.length !== COMPONENTS.length) return null;
  const result = new Map();
  for (const status of value.components) {
    if (status === null || typeof status !== 'object' || !COMPONENTS.includes(status.component)
        || result.has(status.component) || !statusIsValid(status, status.component)) return null;
    result.set(status.component, {
      state: status.state, code: status.code, ready: status.ready, busy: status.busy, owned: status.owned,
    });
  }
  return result.size === COMPONENTS.length ? result : null;
}

function decodeActionStatus(value) {
  if (value === null || typeof value !== 'object' || !COMPONENTS.includes(value.component)
      || !statusIsValid(value, value.component)) return null;
  return {component: value.component, status: {state: value.state, code: value.code, ready: value.ready, busy: value.busy, owned: value.owned}};
}

function createModel(snapshot, localBusy, migrationConfirmed) {
  const ready = (component) => snapshot.get(component)?.ready === true;
  const actionInFlight = localBusy.size > 0;
  return {
    components: COMPONENTS.map((component) => {
      const status = snapshot.get(component) || UNKNOWN;
      const busy = localBusy.has(component) || status.busy;
      const dependenciesReady = DEPENDENCIES[component].every(ready)
        && (!['gateway', 'worker'].includes(component) || migrationConfirmed);
      const dependantsStopped = DEPENDANTS[component].every((dependant) => !ready(dependant)
        && !(snapshot.get(dependant)?.state === 'failed' && snapshot.get(dependant)?.owned)
        && !localBusy.has(dependant) && !snapshot.get(dependant)?.busy);
      const ownedUnready = status.state === 'failed' && status.owned;
      const actions = {
        start: !actionInFlight && !busy && !status.ready && !ownedUnready && dependenciesReady,
        stop: !actionInFlight && !busy && (status.ready || ownedUnready) && dependantsStopped,
      };
      if (component === 'postgres') actions.migrate = !actionInFlight && !busy && status.ready;
      return {
        component,
        label: component === 'worker' ? 'Worker (optional)' : component[0].toUpperCase() + component.slice(1),
        state: status.state,
        stateLabel: STATE_COPY[status.state] || STATE_COPY.unknown,
        status: CODE_COPY[status.code] || 'Status unavailable.',
        actions,
      };
    }),
    readyForVoiceDemo: ready('speech') && ready('gateway') && ready('client'),
  };
}

function fixedFailure(dom, snapshot, localBusy, migrationConfirmed, message) {
  dom.announce(message);
  dom.render(createModel(snapshot, localBusy, migrationConfirmed));
  dom.setVoiceDemo(false);
}

export function createSetupController({fetch, timers, dom}) {
  if (typeof fetch !== 'function' || !timers || typeof timers.setTimeout !== 'function'
      || typeof timers.clearTimeout !== 'function' || !dom || typeof dom.render !== 'function'
      || typeof dom.announce !== 'function' || typeof dom.setVoiceDemo !== 'function') {
    throw new TypeError('setup controller requires fetch, timer, and DOM ports');
  }
  let snapshot = initialSnapshot();
  let migrationConfirmed = false;
  const localBusy = new Set();
  const actionControllers = new Set();
  let poll = null;
  let pollTimer = null;
  let epoch = 0;
  let started = false;
  let destroyed = false;

  const render = () => {
    const model = createModel(snapshot, localBusy, migrationConfirmed);
    dom.render(model);
    dom.setVoiceDemo(model.readyForVoiceDemo);
  };
  const schedule = () => {
    if (!started || destroyed || pollTimer !== null || localBusy.size > 0) return;
    pollTimer = timers.setTimeout(() => {
      pollTimer = null;
      void refresh();
    }, 2000);
  };
  const failClosed = (message) => {
    snapshot = initialSnapshot();
    migrationConfirmed = false;
    fixedFailure(dom, snapshot, localBusy, migrationConfirmed, message);
  };
  const applySnapshot = (value) => {
    const decoded = decodeSnapshot(value);
    if (decoded === null) {
      failClosed('Setup status is unavailable.');
      return false;
    }
    snapshot = decoded;
    const postgres = snapshot.get('postgres');
    if (!postgres.ready || postgres.state === 'stopped' || postgres.state === 'failed') migrationConfirmed = false;
    render();
    return true;
  };
  const refresh = async () => {
    if (destroyed || poll !== null || localBusy.size > 0) return;
    const requestEpoch = ++epoch;
    const controller = new AbortController();
    poll = controller;
    try {
      const result = await fetch('/api/operator/status', {credentials: 'same-origin', signal: controller.signal});
      const body = result && typeof result.json === 'function' ? await result.json() : null;
      if (!destroyed && requestEpoch === epoch && result?.ok && !controller.signal.aborted) applySnapshot(body);
      else if (!destroyed && requestEpoch === epoch && !controller.signal.aborted) failClosed('Setup status is unavailable.');
    } catch (_) {
      if (!destroyed && requestEpoch === epoch && !controller.signal.aborted) failClosed('Unable to contact local setup service.');
    } finally {
      if (poll === controller) poll = null;
      if (!destroyed && requestEpoch === epoch) schedule();
    }
  };
  const act = async (component, action) => {
    if (destroyed || !OPERATIONS.has(`${component}:${action}`) || localBusy.size > 0) return;
    if (poll !== null) {
      poll.abort();
      poll = null;
    }
    if (pollTimer !== null) {
      timers.clearTimeout(pollTimer);
      pollTimer = null;
    }
    const requestEpoch = ++epoch;
    const controller = new AbortController();
    actionControllers.add(controller);
    localBusy.add(component);
    render();
    try {
      const result = await fetch('/api/operator/action', {
        method: 'POST', credentials: 'same-origin', signal: controller.signal,
        headers: {'Content-Type': 'application/json'}, body: JSON.stringify({component, action}),
      });
      const body = result && typeof result.json === 'function' ? await result.json() : null;
      const decoded = decodeActionStatus(body);
      if (!destroyed && requestEpoch === epoch && decoded !== null) {
        if (decoded.component !== component) {
          failClosed('Setup action is unavailable.');
          return;
        }
        snapshot.set(decoded.component, decoded.status);
        if (component === 'postgres' && (!decoded.status.ready || decoded.status.state === 'stopped' || decoded.status.state === 'failed')) {
          migrationConfirmed = false;
        }
        if (action === 'migrate') {
          migrationConfirmed = result.ok && decoded.status.ready && decoded.status.code === 'ready';
        }
        if (!result.ok) dom.announce(CODE_COPY[decoded.status.code] || 'Setup action is unavailable.');
      } else if (!destroyed && requestEpoch === epoch) {
        failClosed('Setup action is unavailable.');
      }
    } catch (_) {
      if (!destroyed && requestEpoch === epoch) failClosed('Unable to contact local setup service.');
    } finally {
      actionControllers.delete(controller);
      localBusy.delete(component);
      if (!destroyed && requestEpoch === epoch) {
        render();
        schedule();
      }
    }
  };
  const start = () => {
    if (destroyed || started) return;
    started = true;
    void refresh();
  };
  const destroy = () => {
    destroyed = true;
    epoch += 1;
    if (pollTimer !== null) timers.clearTimeout(pollTimer);
    pollTimer = null;
    if (poll !== null) poll.abort();
    poll = null;
    for (const controller of actionControllers) controller.abort();
    actionControllers.clear();
    localBusy.clear();
    snapshot = initialSnapshot();
    migrationConfirmed = false;
    render();
  };
  render();
  return {start, refresh, act, destroy, applySnapshot, dom};
}

export function createSetupDOMPort(document) {
  const announcement = document.querySelector('#setup-announcement');
  const voiceDemo = document.querySelector('#open-voice-demo');
  const cards = new Map(COMPONENTS.map((component) => [component, document.querySelector(`[data-component="${component}"]`)]));
  const setVoiceDemo = (enabled) => {
    voiceDemo.toggleAttribute('aria-disabled', !enabled);
    if (enabled) {
      voiceDemo.setAttribute('href', '/');
      voiceDemo.removeAttribute('tabindex');
    } else {
      voiceDemo.removeAttribute('href');
      voiceDemo.setAttribute('tabindex', '-1');
    }
  };
  return {
    announce(message) { announcement.textContent = message; },
    setVoiceDemo,
    render(model) {
      for (const item of model.components) {
        const card = cards.get(item.component);
        if (!card) continue;
        card.dataset.state = item.state;
        card.setAttribute('aria-busy', String(item.state === 'starting' || item.state === 'stopping'));
        card.querySelector('[data-state-label]').textContent = item.stateLabel;
        card.querySelector('[data-status]').textContent = item.status;
        for (const [action, enabled] of Object.entries(item.actions)) {
          const button = card.querySelector(`[data-action="${action}"]`);
          if (button) button.disabled = !enabled;
        }
      }
    },
  };
}

// createProfileDOMPort renders the Profile, Prompts, and Review & publish tabs
// using text nodes and form values only. No template source, preview output, or
// notice text is ever injected as HTML.
export function createProfileDOMPort(document) {
  const announcement = document.querySelector('#profile-announcement');
  const notice = document.querySelector('#profile-notice');
  const current = document.querySelector('#profile-current');
  const generation = document.querySelector('#profile-generation');
  const descriptionInput = document.querySelector('#profile-description');
  const templateList = document.querySelector('#template-list');
  const templateSource = document.querySelector('#template-source');
  const publishedList = document.querySelector('#published-list');
  const previewOutput = document.querySelector('#profile-preview-output');
  const buttons = {
    saveDescription: document.querySelector('#profile-save-description'),
    reload: document.querySelector('#profile-reload'),
    templateSave: document.querySelector('#template-save'),
    templateDelete: document.querySelector('#template-delete'),
    templateAdd: document.querySelector('#template-add'),
    validate: document.querySelector('#profile-validate'),
    preview: document.querySelector('#profile-preview'),
    publish: document.querySelector('#profile-publish'),
  };
  const tabs = new Map(PROFILE_TABS.map((name) => [name, document.querySelector(`#tab-${name}`)]));
  const panels = new Map(PROFILE_TABS.map((name) => [name, document.querySelector(`#panel-${name}`)]));
  let sourceDirtyByUser = false;

  function renderTabs(active) {
    for (const [name, tab] of tabs) {
      if (!tab) continue;
      const selected = name === active;
      tab.setAttribute('aria-selected', String(selected));
      tab.tabIndex = selected ? 0 : -1;
      const panel = panels.get(name);
      if (panel) panel.hidden = !selected;
    }
  }

  function renderTemplates(model) {
    templateList.replaceChildren();
    const templates = model.draft ? model.draft.templates : [];
    for (const entry of templates) {
      const row = document.createElement('li');
      row.setAttribute('aria-current', String(entry.path === model.selectedPath));
      const label = document.createElement('span');
      label.textContent = entry.path;
      const open = document.createElement('button');
      open.type = 'button';
      open.dataset.templatePath = entry.path;
      open.textContent = entry.path === model.selectedPath ? 'Selected' : 'Open';
      open.disabled = model.busy;
      row.append(label, open);
      templateList.append(row);
    }
  }

  function renderPublished(model) {
    publishedList.replaceChildren();
    for (const entry of model.published) {
      const row = document.createElement('li');
      row.dataset.active = String(entry.active);
      row.dataset.revoked = String(entry.revoked);
      const label = document.createElement('span');
      label.textContent = `${entry.id} v${entry.version}`;
      const meta = document.createElement('span');
      meta.className = 'version-meta';
      meta.textContent = entry.revoked ? 'Revoked' : entry.active ? 'Active for new sessions' : 'Published';
      const controls = document.createElement('span');
      controls.className = 'actions';
      if (!entry.revoked) {
        const activate = document.createElement('button');
        activate.type = 'button';
        activate.dataset.versionAction = 'activate';
        activate.dataset.versionId = entry.id;
        activate.dataset.version = String(entry.version);
        activate.textContent = 'Activate';
        activate.disabled = model.busy || entry.active;
        const revoke = document.createElement('button');
        revoke.type = 'button';
        revoke.dataset.versionAction = 'revoke';
        revoke.dataset.versionId = entry.id;
        revoke.dataset.version = String(entry.version);
        revoke.textContent = 'Revoke';
        revoke.disabled = model.busy;
        controls.append(activate, revoke);
      }
      row.append(label, meta, controls);
      publishedList.append(row);
    }
  }

  return {
    announce(message) { announcement.textContent = message; },
    markSourceDirty() { sourceDirtyByUser = true; },
    render(model) {
      renderTabs(model.tab);
      notice.textContent = profileNoticeText(model.notice);
      current.textContent = model.draft ? model.draft.id : 'No draft loaded.';
      generation.textContent = model.draft ? String(model.draft.generation) : '—';
      if (model.draft && !sourceDirtyByUser) descriptionInput.value = model.draft.description;
      const hasDraft = model.draft !== null;
      const hasTemplate = model.selectedPath !== '';
      buttons.saveDescription.disabled = !hasDraft || model.busy;
      buttons.reload.disabled = !hasDraft || model.busy;
      buttons.templateSave.disabled = !hasTemplate || model.busy || !model.dirty;
      buttons.templateDelete.disabled = !hasTemplate || model.busy || model.selectedPath === 'system.j2';
      buttons.templateAdd.disabled = !hasDraft || model.busy;
      buttons.validate.disabled = !hasDraft || model.busy;
      buttons.preview.disabled = !hasDraft || model.busy;
      buttons.publish.disabled = model.publishDisabled || model.busy;
      templateSource.disabled = !hasTemplate || model.busy;
      if (templateSource.value !== model.source) templateSource.value = model.source;
      previewOutput.textContent = model.preview
        ? Object.entries(model.preview.outputs).map(([name, output]) => `${name}\n${output}`).join('\n\n')
        : "Run Preview to render the profile's synthetic fixtures.";
      renderTemplates(model);
      renderPublished(model);
    },
  };
}

function bootProfileWorkspace(document, window) {
  if (!document.querySelector('#panel-profile')) return null;
  const dom = createProfileDOMPort(document);
  const workspace = createProfileWorkspace({api: createProfileAPI(window.fetch.bind(window)), dom});
  const idInput = document.querySelector('#profile-id');
  const descriptionInput = document.querySelector('#profile-description');
  const templateSource = document.querySelector('#template-source');
  const newPathInput = document.querySelector('#template-new-path');

  document.querySelector('#profile-open-form').addEventListener('submit', (event) => {
    event.preventDefault();
    void workspace.openOrCreate(idInput.value.trim(), descriptionInput.value);
  });
  document.querySelector('#profile-save-description').addEventListener('click', () => {
    void workspace.saveDescription(descriptionInput.value);
  });
  document.querySelector('#profile-reload').addEventListener('click', () => { void workspace.reloadDraft(); });
  descriptionInput.addEventListener('input', () => dom.markSourceDirty());
  templateSource.addEventListener('input', () => workspace.editSource(templateSource.value));
  document.querySelector('#template-save').addEventListener('click', () => {
    void workspace.saveTemplate(workspace.snapshot().selectedPath, templateSource.value);
  });
  document.querySelector('#template-delete').addEventListener('click', () => {
    void workspace.deleteTemplate(workspace.snapshot().selectedPath);
  });
  document.querySelector('#template-add-form').addEventListener('submit', (event) => {
    event.preventDefault();
    const path = newPathInput.value.trim();
    if (path !== '') void workspace.saveTemplate(path, '');
  });
  document.querySelector('#profile-validate').addEventListener('click', () => { void workspace.validateDraft(); });
  document.querySelector('#profile-preview').addEventListener('click', () => { void workspace.previewDraft(); });
  document.querySelector('#profile-publish').addEventListener('click', () => { void workspace.publishDraft(true); });
  document.querySelector('#profile-refresh').addEventListener('click', () => { void workspace.refreshPublished(); });
  document.addEventListener('click', (event) => {
    const tab = event.target.closest('[role="tab"][data-tab]');
    if (tab) {
      workspace.selectTab(tab.dataset.tab);
      return;
    }
    const template = event.target.closest('button[data-template-path]');
    if (template && !template.disabled) {
      void workspace.selectTemplate(template.dataset.templatePath);
      return;
    }
    const version = event.target.closest('button[data-version-action]');
    if (version && !version.disabled) {
      const id = version.dataset.versionId;
      const number = Number(version.dataset.version);
      if (version.dataset.versionAction === 'activate') void workspace.activateVersion(id, number);
      else void workspace.revokeVersion(id, number);
    }
  });
  return workspace;
}

function boot(document, window) {
  const dom = createSetupDOMPort(document);
  const controller = createSetupController({fetch: window.fetch.bind(window), timers: window, dom});
  const workspace = bootProfileWorkspace(document, window);
  document.addEventListener('click', (event) => {
    const button = event.target.closest('button[data-action]');
    if (!button || button.disabled) return;
    const card = button.closest('[data-component]');
    if (card) void controller.act(card.dataset.component, button.dataset.action);
  });
  window.addEventListener('pagehide', () => {
    controller.destroy();
    workspace?.destroy();
  }, {once: true});
  controller.start();
  void workspace?.refreshPublished();
}

if (typeof document !== 'undefined' && typeof window !== 'undefined') boot(document, window);
