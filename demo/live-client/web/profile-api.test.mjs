import assert from 'node:assert/strict';
import test from 'node:test';

import {createProfileAPI, decodeDraft, decodePublishedList, decodePreview} from './profile-api.mjs';

const HASH = 'a'.repeat(64);

function draftBody(overrides = {}) {
  return {
    id: 'insurance',
    generation: 1,
    description: 'Insurance sales',
    templates: [{path: 'system.j2', hash: HASH, bytes: 42}],
    fixtures: [{path: 'default.json', hash: HASH, bytes: 24}],
    ...overrides,
  };
}

function jsonResponse(status, body) {
  return {
    ok: status >= 200 && status < 300,
    status,
    json: async () => body,
    text: async () => JSON.stringify(body),
  };
}

test('decodeDraft accepts exact shape and freezes the result', () => {
  const draft = decodeDraft(draftBody());
  assert.equal(draft.id, 'insurance');
  assert.equal(draft.generation, 1);
  assert.equal(draft.templates[0].path, 'system.j2');
  assert.ok(Object.isFrozen(draft));
  assert.ok(Object.isFrozen(draft.templates[0]));
});

test('decodeDraft rejects missing, reserved, and malformed fields', () => {
  assert.throws(() => decodeDraft(null));
  assert.throws(() => decodeDraft(draftBody({id: 'builtin'})));
  assert.throws(() => decodeDraft(draftBody({generation: 0})));
  assert.throws(() => decodeDraft(draftBody({templates: [{path: '../x.j2', hash: HASH, bytes: 1}]})));
  assert.throws(() => decodeDraft(draftBody({templates: [{path: 'system.j2', hash: 'short', bytes: 1}]})));
});

test('decodePublishedList rejects malformed entries and keeps safe fields only', () => {
  const list = decodePublishedList({profiles: [
    {id: 'insurance', version: 2, active: true, revoked: false, published_at: 'x', snapshot_hash: HASH, engine_version: 'e'},
  ]});
  assert.equal(list.length, 1);
  assert.deepEqual(Object.keys(list[0]).sort(), ['active', 'id', 'revoked', 'snapshotHash', 'version']);
  assert.throws(() => decodePublishedList({profiles: [{id: 'builtin', version: 1, active: true, revoked: false, snapshot_hash: HASH}]}));
  assert.throws(() => decodePublishedList({profiles: [{id: 'a', version: 0, active: true, revoked: false, snapshot_hash: HASH}]}));
});

test('decodePreview bounds outputs and requires integrity hashes', () => {
  const preview = decodePreview({bundle_hash: HASH, snapshot_hash: HASH, outputs: {'default.json': 'rendered'}});
  assert.equal(preview.outputs['default.json'], 'rendered');
  assert.throws(() => decodePreview({bundle_hash: 'bad', snapshot_hash: HASH, outputs: {}}));
  assert.throws(() => decodePreview({bundle_hash: HASH, snapshot_hash: HASH, outputs: {big: 'x'.repeat((64 << 10) + 1)}}));
});

test('saveDescription sends exact If-Match and same-origin options', async () => {
  const calls = [];
  const api = createProfileAPI(async (path, options) => {
    calls.push({path, options});
    return jsonResponse(200, draftBody({generation: 2, description: 'updated'}));
  });
  const draft = await api.saveDescription('insurance', 1, 'updated');
  assert.equal(draft.generation, 2);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].path, '/api/setup/v1/profiles/insurance/draft');
  assert.equal(calls[0].options.method, 'PUT');
  assert.equal(calls[0].options.headers['If-Match'], '"1"');
  assert.equal(calls[0].options.cache, 'no-store');
  assert.equal(calls[0].options.credentials, 'same-origin');
});

test('stale save surfaces the stable stale_draft code', async () => {
  const api = createProfileAPI(async () => jsonResponse(409, {error: {code: 'stale_draft'}}));
  await assert.rejects(api.saveDescription('insurance', 1, 'stale'), (error) => error.code === 'stale_draft');
});

test('malformed success bodies are rejected, not partially decoded', async () => {
  const api = createProfileAPI(async () => jsonResponse(200, {id: 'insurance', generation: 'NaN'}));
  await assert.rejects(api.loadDraft('insurance'), (error) => error.code === 'invalid_response');
});

test('template paths are validated before any request is sent', async () => {
  let called = false;
  const api = createProfileAPI(async () => {
    called = true;
    return jsonResponse(200, draftBody());
  });
  await assert.rejects(api.templateSource('insurance', '../secrets'), (error) => error.code === 'invalid_template_path');
  await assert.rejects(api.saveTemplate('insurance', 1, 'System.j2', 'x'), (error) => error.code === 'invalid_template_path');
  assert.equal(called, false);
});

test('network failures map to a stable class and aborts propagate', async () => {
  const api = createProfileAPI(async () => {
    throw new TypeError('fetch failed');
  });
  await assert.rejects(api.listPublished(), (error) => error.code === 'network_unavailable');

  const aborting = createProfileAPI(async () => {
    const error = new Error('aborted');
    error.name = 'AbortError';
    throw error;
  });
  await assert.rejects(aborting.listPublished(), (error) => error.name === 'AbortError');
});

function recipeEntry(overrides = {}) {
  return {
    id: 'lookup',
    version: 1,
    objective: 'workers/lookup.j2',
    require_approval: false,
    execution_timeout_ms: 30000,
    tool_timeout_ms: 10000,
    overall_timeout_ms: 60000,
    max_objective_bytes: 8192,
    max_evidence_bytes: 32768,
    max_tool_result_bytes: 16384,
    max_final_result_bytes: 16384,
    ...overrides,
  };
}

test('decodeDraft maps recipes to camel case and defaults to an empty list', () => {
  const draft = decodeDraft(draftBody({recipes: [recipeEntry({require_approval: true})]}));
  assert.equal(draft.recipes.length, 1);
  assert.equal(draft.recipes[0].objective, 'workers/lookup.j2');
  assert.equal(draft.recipes[0].requireApproval, true);
  assert.equal(draft.recipes[0].overallTimeoutMS, 60000);
  assert.ok(Object.isFrozen(draft.recipes[0]));
  assert.deepEqual([...decodeDraft(draftBody()).recipes], []);
});

test('decodeDraft rejects recipes whose objective is not the linked worker template', () => {
  assert.throws(() => decodeDraft(draftBody({recipes: [recipeEntry({objective: 'system.j2'})]})));
  assert.throws(() => decodeDraft(draftBody({recipes: [recipeEntry({objective: '../escape.j2'})]})));
  assert.throws(() => decodeDraft(draftBody({recipes: [recipeEntry({id: 'Lookup'})]})));
  assert.throws(() => decodeDraft(draftBody({recipes: [recipeEntry({version: 0})]})));
  assert.throws(() => decodeDraft(draftBody({recipes: [recipeEntry({require_approval: 'yes'})]})));
  assert.throws(() => decodeDraft(draftBody({recipes: [recipeEntry({overall_timeout_ms: -1})]})));
  assert.throws(() => decodeDraft(draftBody({recipes: {}})));
});

test('saveRecipe sends the exact generation and rejects hostile recipe IDs before fetching', async () => {
  const requests = [];
  const api = createProfileAPI(async (path, init) => {
    requests.push({path, init});
    return jsonResponse(200, draftBody({generation: 3, recipes: [recipeEntry({version: 2})]}));
  });
  const draft = await api.saveRecipe('insurance', 2, 'lookup', {require_approval: true});
  assert.equal(draft.recipes[0].version, 2);
  assert.equal(requests.length, 1);
  assert.equal(requests[0].path, '/api/setup/v1/profiles/insurance/recipes/lookup');
  assert.equal(requests[0].init.method, 'PUT');
  assert.equal(requests[0].init.headers['If-Match'], '"2"');
  assert.equal(requests[0].init.credentials, 'same-origin');

  for (const bad of ['../escape', 'Lookup', 'a/b', '', 'x'.repeat(65)]) {
    await assert.rejects(() => api.saveRecipe('insurance', 2, bad, {}), (error) => error.code === 'invalid_recipe_id');
    await assert.rejects(() => api.deleteRecipe('insurance', 2, bad), (error) => error.code === 'invalid_recipe_id');
  }
  assert.equal(requests.length, 1);
});

test('deleteRecipe surfaces the server error code', async () => {
  const api = createProfileAPI(async () => jsonResponse(409, {error: {code: 'stale_draft'}}));
  await assert.rejects(() => api.deleteRecipe('insurance', 2, 'lookup'), (error) => error.code === 'stale_draft');
});
