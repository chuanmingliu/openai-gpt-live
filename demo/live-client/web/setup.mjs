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
const UNKNOWN = Object.freeze({state: 'unknown', code: 'unknown', ready: false, busy: false});

function initialSnapshot() {
  return {migrated: false, components: new Map(COMPONENTS.map((component) => [component, {...UNKNOWN}]))};
}

function statusIsValid(value, expectedComponent) {
  return value !== null && typeof value === 'object'
    && Object.keys(value).length === 5
    && Object.keys(value).every((key) => ['component', 'state', 'code', 'ready', 'busy'].includes(key))
    && value.component === expectedComponent
    && STATES.has(value.state)
    && CODES.has(value.code)
    && typeof value.ready === 'boolean'
    && typeof value.busy === 'boolean';
}

function decodeSnapshot(value) {
  if (value === null || typeof value !== 'object' || Array.isArray(value)
      || Object.keys(value).length !== 2 || typeof value.migrated !== 'boolean' || !Array.isArray(value.components)
      || value.components.length !== COMPONENTS.length) return null;
  const result = new Map();
  for (const status of value.components) {
    if (status === null || typeof status !== 'object' || !COMPONENTS.includes(status.component)
        || result.has(status.component) || !statusIsValid(status, status.component)) return null;
    result.set(status.component, {
      state: status.state, code: status.code, ready: status.ready, busy: status.busy,
    });
  }
  return result.size === COMPONENTS.length ? {migrated: value.migrated, components: result} : null;
}

function decodeActionStatus(value) {
  if (value === null || typeof value !== 'object' || !COMPONENTS.includes(value.component)
      || !statusIsValid(value, value.component)) return null;
  return {component: value.component, status: {state: value.state, code: value.code, ready: value.ready, busy: value.busy}};
}

function createModel(snapshot, localBusy) {
  const ready = (component) => snapshot.components.get(component)?.ready === true;
  const actionInFlight = localBusy.size > 0;
  return {
    components: COMPONENTS.map((component) => {
      const status = snapshot.components.get(component) || UNKNOWN;
      const busy = localBusy.has(component) || status.busy;
      const dependenciesReady = DEPENDENCIES[component].every(ready)
        && (!['gateway', 'worker'].includes(component) || snapshot.migrated);
      const dependantsStopped = DEPENDANTS[component].every((dependant) => !ready(dependant)
        && !localBusy.has(dependant) && !snapshot.components.get(dependant)?.busy);
      const actions = {
        start: !actionInFlight && !busy && !status.ready && dependenciesReady,
        stop: !actionInFlight && !busy && status.ready && dependantsStopped,
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

function fixedFailure(dom, snapshot, localBusy, message) {
  dom.announce(message);
  dom.render(createModel(snapshot, localBusy));
  dom.setVoiceDemo(false);
}

export function createSetupController({fetch, timers, dom}) {
  if (typeof fetch !== 'function' || !timers || typeof timers.setTimeout !== 'function'
      || typeof timers.clearTimeout !== 'function' || !dom || typeof dom.render !== 'function'
      || typeof dom.announce !== 'function' || typeof dom.setVoiceDemo !== 'function') {
    throw new TypeError('setup controller requires fetch, timer, and DOM ports');
  }
  let snapshot = initialSnapshot();
  const localBusy = new Set();
  const actionControllers = new Set();
  let poll = null;
  let pollTimer = null;
  let epoch = 0;
  let started = false;
  let destroyed = false;

  const render = () => {
    const model = createModel(snapshot, localBusy);
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
    fixedFailure(dom, snapshot, localBusy, message);
  };
  const applySnapshot = (value) => {
    const decoded = decodeSnapshot(value);
    if (decoded === null) {
      failClosed('Setup status is unavailable.');
      return false;
    }
    snapshot = decoded;
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
        snapshot.components.set(decoded.component, decoded.status);
        if (action === 'migrate' && result.ok && decoded.status.ready && decoded.status.code === 'ready') snapshot.migrated = true;
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

function boot(document, window) {
  const dom = createSetupDOMPort(document);
  const controller = createSetupController({fetch: window.fetch.bind(window), timers: window, dom});
  document.addEventListener('click', (event) => {
    const button = event.target.closest('button[data-action]');
    if (!button || button.disabled) return;
    const card = button.closest('[data-component]');
    if (card) void controller.act(card.dataset.component, button.dataset.action);
  });
  window.addEventListener('pagehide', () => controller.destroy(), {once: true});
  controller.start();
}

if (typeof document !== 'undefined' && typeof window !== 'undefined') boot(document, window);
