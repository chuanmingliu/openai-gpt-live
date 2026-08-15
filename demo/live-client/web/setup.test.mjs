import assert from 'node:assert/strict';
import test from 'node:test';

import {COMPONENTS, createSetupController} from './setup.mjs';

function snapshot(overrides = {}) {
  const ready = new Set(overrides.ready || []);
  const busy = new Set(overrides.busy || []);
  const codes = overrides.codes || {};
  return {
    components: COMPONENTS.map((component) => ({
      component,
      state: ready.has(component) ? 'ready' : busy.has(component) ? 'starting' : 'stopped',
      code: codes[component] || (ready.has(component) ? 'ready' : busy.has(component) ? 'operation_in_progress' : 'stopped'),
      ready: ready.has(component),
      busy: busy.has(component),
    })),
  };
}

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((nextResolve, nextReject) => {
    resolve = nextResolve;
    reject = nextReject;
  });
  return {promise, resolve, reject};
}

function response(body, ok = true) {
  return {ok, json: async () => body};
}

function fakeTimers() {
  const callbacks = [];
  return {
    setTimeout(callback, delay) {
      callbacks.push({callback, delay});
      return callbacks.length;
    },
    clearTimeout(id) {
      callbacks[id - 1] = null;
    },
    take() {
      while (callbacks.length > 0) {
        const callback = callbacks.shift();
        if (callback !== null) return callback;
      }
      return null;
    },
  };
}

async function settle() {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

function fakeDOM() {
  return {
    renders: [],
    announcements: [],
    demoEnabled: false,
    render(model) { this.renders.push(model); },
    announce(message) { this.announcements.push(message); },
    setVoiceDemo(enabled) { this.demoEnabled = enabled; },
  };
}

test('renders exactly the five fixed components and enables the voice demo only when its prerequisites are ready', async () => {
  const dom = fakeDOM();
  const controller = createSetupController({
    fetch: async () => response(snapshot({ready: ['speech', 'postgres', 'gateway', 'client']})),
    timers: fakeTimers(),
    dom,
  });

  await controller.refresh();

  assert.deepEqual(dom.renders.at(-1).components.map((item) => item.component), COMPONENTS);
  assert.equal(dom.renders.at(-1).components.find((item) => item.component === 'worker').label, 'Worker (optional)');
  assert.equal(dom.demoEnabled, true);
});

test('polls no faster than every two seconds with only one status request in flight', async () => {
  const dom = fakeDOM();
  const timers = fakeTimers();
  const pending = deferred();
  let calls = 0;
  const controller = createSetupController({
    fetch: () => { calls += 1; return pending.promise; }, timers, dom,
  });

  controller.start();
  controller.refresh();
  assert.equal(calls, 1);
  pending.resolve(response(snapshot()));
  await Promise.resolve();
  await Promise.resolve();
  assert.equal(timers.take().delay, 2000);
});

test('status snapshots alone never enable migration-gated starts', async () => {
  const dom = fakeDOM();
  const controller = createSetupController({
    fetch: async () => response(snapshot({ready: ['speech', 'postgres']})),
    timers: fakeTimers(), dom,
  });

  await controller.refresh();

  const cards = dom.renders.at(-1).components;
  assert.equal(cards.find((item) => item.component === 'gateway').actions.start, false);
  assert.equal(cards.find((item) => item.component === 'worker').actions.start, false);
  assert.equal(cards.find((item) => item.component === 'client').actions.start, false);
});

test('successful current-generation migrate unlocks durable starts, while a busy dependant keeps stops disabled', async () => {
  const dom = fakeDOM();
  const actions = [response({component: 'postgres', state: 'ready', code: 'ready', ready: true, busy: false})];
  const controller = createSetupController({
    fetch: async (url) => url.endsWith('/status')
      ? response(snapshot({ready: ['speech', 'postgres'], busy: ['gateway']}))
      : actions.shift(),
    timers: fakeTimers(), dom,
  });

  await controller.applySnapshot(snapshot({ready: ['speech', 'postgres']}));
  await controller.act('postgres', 'migrate');

  let cards = dom.renders.at(-1).components;
  assert.equal(cards.find((item) => item.component === 'gateway').actions.start, true);
  assert.equal(cards.find((item) => item.component === 'worker').actions.start, true);

  await controller.refresh();

  cards = dom.renders.at(-1).components;
  assert.equal(cards.find((item) => item.component === 'gateway').actions.start, false);
  assert.equal(cards.find((item) => item.component === 'speech').actions.stop, false);
  assert.equal(cards.find((item) => item.component === 'postgres').actions.stop, false);
});

test('uses fixed error-code copy and never renders unknown response strings', async () => {
  const dom = fakeDOM();
  const controller = createSetupController({
    fetch: async () => response(snapshot({codes: {speech: 'missing_configuration'}})),
    timers: fakeTimers(), dom,
  });

  await controller.refresh();
  assert.equal(dom.renders.at(-1).components[0].status, 'Configuration is unavailable.');

  const injected = snapshot();
  injected.components[0].code = '<img src=x onerror=alert(1)>';
  await controller.applySnapshot(injected);
  assert.equal(dom.renders.at(-1).components[0].status, 'Status unavailable.');
  assert.equal(dom.renders.at(-1).components[0].state, 'unknown');
});

test('owned-unready failed services remain stoppable and block dependency removal', async () => {
  const dom = fakeDOM();
  const value = snapshot({ready: ['speech', 'postgres']});
  const gateway = value.components.find((component) => component.component === 'gateway');
  gateway.state = 'failed';
  gateway.code = 'process_exited';
  const controller = createSetupController({fetch: async () => response(value), timers: fakeTimers(), dom});

  await controller.refresh();

  const cards = dom.renders.at(-1).components;
  assert.equal(cards.find((item) => item.component === 'gateway').actions.stop, true);
  assert.equal(cards.find((item) => item.component === 'gateway').actions.start, false);
  assert.equal(cards.find((item) => item.component === 'speech').actions.stop, false);
  assert.equal(cards.find((item) => item.component === 'postgres').actions.stop, false);
});

test('suppresses double clicks and fences a late poll behind a newer action response', async () => {
  const dom = fakeDOM();
  const timers = fakeTimers();
  const poll = deferred();
  let actionCalls = 0;
  const controller = createSetupController({
    fetch: (url) => {
      if (url.endsWith('/status')) return poll.promise;
      actionCalls += 1;
      return Promise.resolve(response({component: 'speech', state: 'ready', code: 'ready', ready: true, busy: false}));
    }, timers, dom,
  });

  controller.refresh();
  const first = controller.act('speech', 'start');
  const second = controller.act('speech', 'start');
  const competing = controller.act('postgres', 'start');
  await Promise.all([first, second, competing]);
  poll.resolve(response(snapshot({codes: {speech: 'operation_failed'}})));
  await Promise.resolve();
  await Promise.resolve();

  assert.equal(actionCalls, 1);
  assert.equal(dom.renders.at(-1).components[0].state, 'ready');
});

test('failed or late migrate responses never unlock durable starts', async () => {
  const dom = fakeDOM();
  const timers = fakeTimers();
  const delayedMigrate = deferred();
  const controller = createSetupController({
    fetch: (url) => {
      if (url.endsWith('/status')) return Promise.resolve(response(snapshot({ready: ['speech', 'postgres']})));
      return delayedMigrate.promise;
    }, timers, dom,
  });

  await controller.applySnapshot(snapshot({ready: ['speech', 'postgres']}));
  const migrate = controller.act('postgres', 'migrate');
  controller.destroy();
  delayedMigrate.resolve(response({component: 'postgres', state: 'ready', code: 'ready', ready: true, busy: false}));
  await migrate;

  let cards = dom.renders.at(-1).components;
  assert.equal(cards.find((item) => item.component === 'gateway').actions.start, false);
  assert.equal(cards.find((item) => item.component === 'worker').actions.start, false);

  const failed = createSetupController({
    fetch: async () => response({component: 'postgres', state: 'failed', code: 'operation_failed', ready: false, busy: false}, false),
    timers: fakeTimers(), dom: fakeDOM(),
  });
  await failed.applySnapshot(snapshot({ready: ['speech', 'postgres']}));
  await failed.act('postgres', 'migrate');

  cards = failed.dom.renders.at(-1).components;
  assert.equal(cards.find((item) => item.component === 'gateway').actions.start, false);
  assert.equal(cards.find((item) => item.component === 'worker').actions.start, false);
});

test('a periodic poll after two seconds preserves migration confirmation while a fresh controller starts locked', async () => {
  const dom = fakeDOM();
  const timers = fakeTimers();
  const snapshots = [
    snapshot({ready: ['speech', 'postgres']}),
    snapshot({ready: ['speech', 'postgres']}),
  ];
  const controller = createSetupController({
    fetch: async (url) => url.endsWith('/status')
      ? response(snapshots.shift())
      : response({component: 'postgres', state: 'ready', code: 'ready', ready: true, busy: false}),
    timers, dom,
  });

  controller.start();
  await settle();
  await controller.act('postgres', 'migrate');
  assert.equal(dom.renders.at(-1).components.find((item) => item.component === 'gateway').actions.start, true);
  const timer = timers.take();
  assert.equal(timer.delay, 2000);
  timer.callback();
  await settle();
  assert.equal(dom.renders.at(-1).components.find((item) => item.component === 'gateway').actions.start, true);

  const fresh = createSetupController({fetch: async () => response(snapshot({ready: ['speech', 'postgres']})), timers: fakeTimers(), dom: fakeDOM()});
  await fresh.refresh();
  assert.equal(fresh.dom.renders.at(-1).components.find((item) => item.component === 'gateway').actions.start, false);
});

test('postgres loss in a later status poll relocks a prior migration confirmation', async () => {
  const dom = fakeDOM();
  const snapshots = [snapshot({ready: ['speech', 'postgres']}), snapshot({ready: ['speech']})];
  const controller = createSetupController({
    fetch: async (url) => url.endsWith('/status')
      ? response(snapshots.shift())
      : response({component: 'postgres', state: 'ready', code: 'ready', ready: true, busy: false}),
    timers: fakeTimers(), dom,
  });

  await controller.refresh();
  await controller.act('postgres', 'migrate');
  assert.equal(dom.renders.at(-1).components.find((item) => item.component === 'gateway').actions.start, true);
  await controller.refresh();
  assert.equal(dom.renders.at(-1).components.find((item) => item.component === 'gateway').actions.start, false);
});

test('uses a valid non-success action status for the requested component but fails closed for another component', async () => {
  const dom = fakeDOM();
  const responses = [
    response({component: 'gateway', state: 'failed', code: 'dependency_not_ready', ready: false, busy: false}, false),
    response({component: 'postgres', state: 'ready', code: 'ready', ready: true, busy: false}),
  ];
  const controller = createSetupController({fetch: async () => responses.shift(), timers: fakeTimers(), dom});

  await controller.act('gateway', 'start');
  assert.equal(dom.renders.at(-1).components.find((item) => item.component === 'gateway').status, 'A required service is not ready.');
  assert.equal(dom.announcements.at(-1), 'A required service is not ready.');

  await controller.act('speech', 'start');
  assert.equal(dom.renders.at(-1).components.find((item) => item.component === 'postgres').state, 'unknown');
  assert.equal(dom.announcements.at(-1), 'Setup action is unavailable.');
});

test('aborts on teardown and recovers from network and malformed response failures without using response content', async () => {
  const dom = fakeDOM();
  const timers = fakeTimers();
  const pending = deferred();
  const controller = createSetupController({fetch: () => pending.promise, timers, dom});

  controller.refresh();
  controller.destroy();
  assert.equal(dom.renders.at(-1).components[0].state, 'unknown');

  const recovery = createSetupController({
    fetch: async () => { throw new Error('provider-secret'); }, timers: fakeTimers(), dom: fakeDOM(),
  });
  await recovery.refresh();
  assert.match(recovery.dom.announcements.at(-1), /Unable to contact local setup service/);
  await recovery.applySnapshot({components: [{component: 'speech', state: 'ready', code: 'ready', ready: true, busy: false}]});
  assert.equal(recovery.dom.renders.at(-1).components[0].status, 'Status unavailable.');
});

test('aborts an in-flight action when the page is torn down', async () => {
  const pending = deferred();
  let actionSignal;
  const controller = createSetupController({
    fetch: (url, options) => {
      if (url.endsWith('/action')) actionSignal = options.signal;
      return pending.promise;
    }, timers: fakeTimers(), dom: fakeDOM(),
  });

  void controller.act('speech', 'start');
  controller.destroy();

  assert.equal(actionSignal.aborted, true);
  pending.reject(new Error('cancelled'));
});
