import assert from 'node:assert/strict';
import test from 'node:test';

import {createProfileWorkspace, profileNoticeText, PROFILE_TABS} from './setup-profile.mjs';

const HASH = 'c'.repeat(64);

function recipeBody(id, version = 1, overrides = {}) {
  return Object.freeze({
    id,
    version,
    objective: `workers/${id}.j2`,
    requireApproval: false,
    executionTimeoutMS: 30000,
    toolTimeoutMS: 10000,
    overallTimeoutMS: 60000,
    maxObjectiveBytes: 8192,
    maxEvidenceBytes: 32768,
    maxToolResultBytes: 16384,
    maxFinalResultBytes: 16384,
    bindingIDs: Object.freeze([]),
    ...overrides,
  });
}

function draftBody(generation, templates = ['system.j2'], recipes = []) {
  return Object.freeze({
    id: 'insurance',
    generation,
    description: 'Insurance sales',
    templates: Object.freeze(templates.map((path) => Object.freeze({path, hash: HASH, bytes: 10}))),
    fixtures: Object.freeze([]),
    recipes: Object.freeze(recipes),
    knowledge: Object.freeze([]),
    bindings: Object.freeze([]),
  });
}

function apiError(code) {
  const error = new Error(code);
  error.code = code;
  return error;
}

function fakeAPI(overrides = {}) {
  return {
    listPublished: async () => Object.freeze([]),
    adapterCatalog: async () => Object.freeze({registryVersion: 'registry-v1', adapters: Object.freeze([])}),
    createProfile: async () => draftBody(1),
    loadDraft: async () => draftBody(1),
    saveDescription: async () => draftBody(2),
    templateSource: async () => 'template source',
    saveTemplate: async () => draftBody(2),
    deleteTemplate: async () => true,
    validate: async () => true,
    preview: async () => Object.freeze({bundleHash: HASH, snapshotHash: HASH, outputs: Object.freeze({})}),
    publish: async () => Object.freeze({id: 'insurance', version: 1, snapshotHash: HASH, active: true}),
    activate: async () => true,
    revoke: async () => true,
    saveRecipe: async () => draftBody(2, ['system.j2', 'workers/lookup.j2'], [recipeBody('lookup')]),
    pinKnowledge: async () => draftBody(2),
    unpinKnowledge: async () => true,
    deleteRecipe: async () => true,
    saveBinding: async () => draftBody(2),
    deleteBinding: async () => draftBody(2),
    ...overrides,
  };
}

function harness(overrides = {}) {
  const rendered = [];
  const announced = [];
  const workspace = createProfileWorkspace({
    api: fakeAPI(overrides),
    dom: {
      render: (model) => rendered.push(model),
      announce: (message) => announced.push(message),
    },
  });
  return {workspace, rendered, announced};
}

test('tabs are the approved set and default to runtime', () => {
  const {workspace} = harness();
  assert.deepEqual([...PROFILE_TABS], ['runtime', 'profile', 'prompts', 'workers', 'adapters', 'knowledge', 'publish']);
  assert.equal(workspace.snapshot().tab, 'runtime');
  workspace.selectTab('prompts');
  assert.equal(workspace.snapshot().tab, 'prompts');
  workspace.selectTab('nonsense');
  assert.equal(workspace.snapshot().tab, 'prompts');
});

test('openOrCreate loads an existing draft', async () => {
  const {workspace} = harness();
  await workspace.openOrCreate('insurance');
  const snapshot = workspace.snapshot();
  assert.equal(snapshot.profileID, 'insurance');
  assert.equal(snapshot.draft.generation, 1);
  assert.equal(snapshot.notice, 'loaded');
});

test('openOrCreate creates when the draft is absent', async () => {
  let created = false;
  const {workspace} = harness({
    loadDraft: async () => null,
    createProfile: async () => {
      created = true;
      return draftBody(1);
    },
  });
  await workspace.openOrCreate('insurance', 'desc');
  assert.equal(created, true);
  assert.equal(workspace.snapshot().draft.generation, 1);
});

test('stale template save keeps local source, marks conflict, and disables publish', async () => {
  const {workspace} = harness({
    saveTemplate: async () => {
      throw apiError('stale_draft');
    },
  });
  await workspace.openOrCreate('insurance');
  await workspace.selectTemplate('system.j2');
  workspace.editSource('local unsaved source');
  await workspace.saveTemplate('system.j2', 'local unsaved source');
  const snapshot = workspace.snapshot();
  assert.equal(snapshot.source, 'local unsaved source');
  assert.equal(snapshot.conflict, true);
  assert.equal(snapshot.notice, 'stale_draft');
  assert.equal(snapshot.publishDisabled, true);
});

test('publish is refused while the editor is dirty', async () => {
  let publishCalls = 0;
  const {workspace} = harness({
    publish: async () => {
      publishCalls += 1;
      return Object.freeze({id: 'insurance', version: 1, snapshotHash: HASH, active: true});
    },
  });
  await workspace.openOrCreate('insurance');
  await workspace.selectTemplate('system.j2');
  workspace.editSource('changed but unsaved');
  await workspace.publishDraft(true);
  assert.equal(publishCalls, 0);
  assert.equal(workspace.snapshot().notice, 'profile_invalid');
});

test('successful publish refreshes the published list', async () => {
  const {workspace} = harness({
    listPublished: async () => Object.freeze([
      Object.freeze({id: 'insurance', version: 1, active: true, revoked: false, snapshotHash: HASH}),
    ]),
  });
  await workspace.openOrCreate('insurance');
  await workspace.publishDraft(true);
  const snapshot = workspace.snapshot();
  assert.equal(snapshot.notice, 'published');
  assert.equal(snapshot.published.length, 1);
  assert.equal(snapshot.published[0].version, 1);
});

test('a 404 from the disabled admin API maps to admin_disabled', async () => {
  const {workspace} = harness({
    listPublished: async () => {
      throw apiError('http_404');
    },
  });
  await workspace.refreshPublished();
  assert.equal(workspace.snapshot().notice, 'admin_disabled');
});

test('only one action runs at a time', async () => {
  let release;
  const gate = new Promise((resolve) => {
    release = resolve;
  });
  const {workspace} = harness({
    loadDraft: async () => {
      await gate;
      return draftBody(1);
    },
  });
  const first = workspace.openOrCreate('insurance');
  const second = workspace.refreshPublished();
  assert.equal(await second, false);
  assert.equal(workspace.snapshot().notice, 'busy');
  release();
  await first;
  assert.equal(workspace.snapshot().draft.generation, 1);
});

test('destroy aborts in-flight work and clears state', async () => {
  let sawAbort = false;
  const {workspace} = harness({
    loadDraft: (id, signal) => new Promise((_, reject) => {
      signal.addEventListener('abort', () => {
        sawAbort = true;
        const error = new Error('aborted');
        error.name = 'AbortError';
        reject(error);
      });
    }),
  });
  const pending = workspace.openOrCreate('insurance');
  workspace.destroy();
  await pending;
  assert.equal(sawAbort, true);
});

test('notice copy never echoes template source', () => {
  for (const code of ['stale_draft', 'profile_invalid', 'store_unavailable', 'unknown_code']) {
    const text = profileNoticeText(code);
    assert.ok(!text.includes('{{'));
    assert.ok(text.length < 120);
  }
});

test('saveRecipe advances the generation and surfaces the linked objective', async () => {
  const calls = [];
  const {workspace, announced} = harness({
    saveRecipe: async (id, generation, recipeID, settings) => {
      calls.push({id, generation, recipeID, settings});
      return draftBody(2, ['system.j2', 'workers/lookup.j2'], [recipeBody('lookup')]);
    },
  });
  await workspace.openOrCreate('insurance');
  assert.deepEqual([...workspace.snapshot().recipes], []);

  const settings = {require_approval: true, execution_timeout_ms: 30000, tool_timeout_ms: 10000,
    overall_timeout_ms: 60000, max_objective_bytes: 8192, max_evidence_bytes: 32768,
    max_tool_result_bytes: 16384, max_final_result_bytes: 16384};
  assert.equal(await workspace.saveRecipe('lookup', settings), true);

  assert.deepEqual(calls, [{id: 'insurance', generation: 1, recipeID: 'lookup', settings}]);
  const snapshot = workspace.snapshot();
  assert.equal(snapshot.draft.generation, 2);
  assert.equal(snapshot.recipes.length, 1);
  assert.equal(snapshot.recipes[0].objective, 'workers/lookup.j2');
  assert.equal(snapshot.conflict, false);
  assert.equal(announced.at(-1), 'Worker recipe saved.');
});

test('saveRecipe reports a missing objective template without touching the draft', async () => {
  const {workspace, announced} = harness({
    saveRecipe: async () => { throw apiError('objective_template_missing'); },
  });
  await workspace.openOrCreate('insurance');
  assert.equal(await workspace.saveRecipe('lookup', {}), false);
  assert.equal(workspace.snapshot().draft.generation, 1);
  assert.deepEqual([...workspace.snapshot().recipes], []);
  assert.equal(announced.at(-1), 'Add the objective template for this worker before saving the recipe.');
});

test('saveRecipe marks a conflict on a stale generation and blocks publish', async () => {
  const {workspace, announced} = harness({
    saveRecipe: async () => { throw apiError('stale_draft'); },
  });
  await workspace.openOrCreate('insurance');
  assert.equal(await workspace.saveRecipe('lookup', {}), false);
  const snapshot = workspace.snapshot();
  assert.equal(snapshot.conflict, true);
  assert.equal(snapshot.publishDisabled, true);
  assert.equal(announced.at(-1), 'The draft changed elsewhere. Reload before saving again.');
});

test('deleteRecipe reloads the draft so the recipe list matches the store', async () => {
  const calls = [];
  let loads = 0;
  const {workspace, announced} = harness({
    loadDraft: async () => {
      loads += 1;
      return loads === 1
        ? draftBody(1, ['system.j2', 'workers/lookup.j2'], [recipeBody('lookup')])
        : draftBody(2, ['system.j2', 'workers/lookup.j2'], []);
    },
    deleteRecipe: async (id, generation, recipeID) => { calls.push({id, generation, recipeID}); return true; },
  });
  await workspace.openOrCreate('insurance');
  assert.equal(workspace.snapshot().recipes.length, 1);
  assert.equal(await workspace.deleteRecipe('lookup'), true);
  assert.deepEqual(calls, [{id: 'insurance', generation: 1, recipeID: 'lookup'}]);
  assert.deepEqual([...workspace.snapshot().recipes], []);
  assert.equal(workspace.snapshot().draft.generation, 2);
  assert.equal(announced.at(-1), 'Worker recipe removed.');
});

test('recipe actions refuse to run concurrently', async () => {
  let release;
  const gate = new Promise((resolve) => { release = resolve; });
  const {workspace, announced} = harness({
    saveRecipe: async () => { await gate; return draftBody(2, ['system.j2'], [recipeBody('lookup')]); },
  });
  await workspace.openOrCreate('insurance');
  const first = workspace.saveRecipe('lookup', {});
  assert.equal(await workspace.deleteRecipe('lookup'), false);
  assert.equal(announced.at(-1), 'Another profile action is still running.');
  release();
  assert.equal(await first, true);
});

test('adapter bindings advance the draft and never accept authority outside the API port', async () => {
  const calls = [];
  const catalog = Object.freeze({registryVersion: 'registry-v1', adapters: Object.freeze([{id: 'knowledge.search'}])});
  const bound = {...draftBody(2), bindings: Object.freeze([{id: 'knowledge.search', version: 'v1', toolIDs: Object.freeze(['knowledge_search']), maxResultBytes: 8192, requireApproval: false}])};
  const {workspace} = harness({
    adapterCatalog: async () => catalog,
    saveBinding: async (id, generation, adapterID, binding) => { calls.push({id, generation, adapterID, binding}); return bound; },
    deleteBinding: async () => draftBody(3),
  });
  await workspace.openOrCreate('insurance');
  await workspace.saveBinding('knowledge.search', {max_result_bytes: 8192, require_approval: false});
  assert.equal(workspace.snapshot().bindings[0].id, 'knowledge.search');
  assert.deepEqual(calls, [{id: 'insurance', generation: 1, adapterID: 'knowledge.search', binding: {max_result_bytes: 8192, require_approval: false}}]);
  await workspace.deleteBinding('knowledge.search');
  assert.deepEqual([...workspace.snapshot().bindings], []);
});

test('pinning knowledge advances the generation and shows the resolved hash', async () => {
  const calls = [];
  const pinned = draftBody(2);
  const withKnowledge = Object.freeze({
    ...pinned,
    knowledge: Object.freeze([Object.freeze({id: 'policy', version: 1, hash: 'd'.repeat(64)})]),
  });
  const {workspace, announced} = harness({
    pinKnowledge: async (id, generation, packID, version) => {
      calls.push({id, generation, packID, version});
      return withKnowledge;
    },
  });
  await workspace.openOrCreate('insurance');
  assert.deepEqual([...workspace.snapshot().knowledge], []);

  assert.equal(await workspace.pinKnowledge('policy', 1), true);
  assert.deepEqual(calls, [{id: 'insurance', generation: 1, packID: 'policy', version: 1}]);
  const snapshot = workspace.snapshot();
  assert.equal(snapshot.draft.generation, 2);
  assert.equal(snapshot.knowledge.length, 1);
  assert.equal(snapshot.knowledge[0].hash.length, 64, 'the hash comes from the service, not the browser');
  assert.equal(announced.at(-1), 'Knowledge version pinned to this draft.');
});

test('a pin refused by the service leaves the draft untouched', async () => {
  const {workspace, announced} = harness({
    pinKnowledge: async () => { throw apiError('knowledge_tombstoned'); },
  });
  await workspace.openOrCreate('insurance');
  assert.equal(await workspace.pinKnowledge('policy', 1), false);
  assert.equal(workspace.snapshot().draft.generation, 1);
  assert.deepEqual([...workspace.snapshot().knowledge], []);
  assert.equal(announced.at(-1), 'That knowledge version is retired.');
});

test('unpinning reloads the draft so the pinned list matches the store', async () => {
  let loads = 0;
  const {workspace} = harness({
    loadDraft: async () => {
      loads += 1;
      const base = draftBody(loads === 1 ? 1 : 2);
      return loads === 1
        ? Object.freeze({...base, knowledge: Object.freeze([Object.freeze({id: 'policy', version: 1, hash: 'e'.repeat(64)})])})
        : base;
    },
  });
  await workspace.openOrCreate('insurance');
  assert.equal(workspace.snapshot().knowledge.length, 1);
  assert.equal(await workspace.unpinKnowledge('policy'), true);
  assert.deepEqual([...workspace.snapshot().knowledge], []);
});
