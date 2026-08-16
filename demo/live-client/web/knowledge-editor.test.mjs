import assert from 'node:assert/strict';
import test from 'node:test';

import {
  ACCEPTED_EXTENSIONS,
  acceptedFile,
  createKnowledgeAPI,
  createKnowledgeController,
  decodePreview,
  decodeVersions,
  MAX_UPLOAD_BYTES,
} from './knowledge-editor.mjs';

const HASH = 'a'.repeat(64);
const OTHER_HASH = 'b'.repeat(64);

function fakeFile(name, size = 128) {
  return {name, size};
}

function previewBody(overrides = {}) {
  return {
    pack_id: 'policy',
    hash: HASH,
    title: 'policy.md',
    media_type: 'text/markdown',
    excerpts: [{ordinal: 0, text: 'waiting period is thirty days', section: 'Coverage'}],
    ...overrides,
  };
}

function jsonResponse(status, body) {
  return {ok: status >= 200 && status < 300, status, json: async () => body};
}

class FakeFormData {
  constructor() { this.entries = []; }
  append(name, value, filename) { this.entries.push({name, value, filename}); }
}

test('accepted extensions are exactly the text formats the server ingests', () => {
  assert.deepEqual([...ACCEPTED_EXTENSIONS], ['.md', '.markdown', '.txt', '.text']);
  for (const name of ['policy.md', 'POLICY.MD', 'notes.txt', 'a.markdown', 'b.text']) {
    assert.equal(acceptedFile(fakeFile(name)), true, name);
  }
  for (const name of ['policy.pdf', 'policy.docx', 'policy', 'policy.md.exe', 'policy.zip']) {
    assert.equal(acceptedFile(fakeFile(name)), false, name);
  }
  assert.equal(acceptedFile(fakeFile('policy.md', 0)), false, 'an empty file carries no material');
  assert.equal(acceptedFile(fakeFile('policy.md', MAX_UPLOAD_BYTES + 1)), false);
  assert.equal(acceptedFile(null), false);
});

test('decodePreview requires dense ordinals and a real hash', () => {
  const preview = decodePreview(previewBody());
  assert.equal(preview.packID, 'policy');
  assert.equal(preview.excerpts[0].section, 'Coverage');
  assert.ok(Object.isFrozen(preview.excerpts[0]));

  assert.throws(() => decodePreview(null));
  assert.throws(() => decodePreview(previewBody({hash: 'short'})));
  assert.throws(() => decodePreview(previewBody({excerpts: []})), undefined, 'a preview with no excerpts is not a preview');
  assert.throws(() => decodePreview(previewBody({
    excerpts: [{ordinal: 1, text: 'x', section: ''}],
  })), undefined, 'ordinals must start at zero and stay dense');
  assert.throws(() => decodePreview(previewBody({
    excerpts: [{ordinal: 0, text: 'x'.repeat(4097), section: ''}],
  })));
});

test('decodeVersions keeps metadata only', () => {
  const versions = decodeVersions({versions: [
    {version: 2, hash: HASH, title: 'policy.md', excerpt_count: 3, published_at: 'x', tombstoned: false},
  ]});
  assert.equal(versions.length, 1);
  assert.deepEqual(Object.keys(versions[0]).sort(), ['excerptCount', 'hash', 'title', 'tombstoned', 'version']);
  assert.throws(() => decodeVersions({versions: [{version: 0, hash: HASH, title: '', excerpt_count: 1, tombstoned: false}]}));
  assert.throws(() => decodeVersions({versions: [{version: 1, hash: 'short', title: '', excerpt_count: 1, tombstoned: false}]}));
});

test('upload sends multipart and rejects a hostile pack id before fetching', async () => {
  const calls = [];
  const api = createKnowledgeAPI(async (path, init) => {
    calls.push({path, init});
    return jsonResponse(200, previewBody());
  }, FakeFormData);

  const preview = await api.preview('policy', fakeFile('policy.md'));
  assert.equal(preview.hash, HASH);
  assert.equal(calls[0].path, '/api/setup/v1/knowledge/policy');
  assert.equal(calls[0].init.method, 'POST');
  assert.equal(calls[0].init.credentials, 'same-origin');
  assert.deepEqual(calls[0].init.body.entries.map((entry) => entry.name), ['document']);

  for (const bad of ['../escape', 'builtin', '', 'a/b', 'x'.repeat(65)]) {
    await assert.rejects(() => api.preview(bad, fakeFile('policy.md')), (error) => error.code === 'invalid_pack_id');
  }
  await assert.rejects(() => api.preview('policy', fakeFile('policy.pdf')), (error) => error.code === 'unsupported_content');
  assert.equal(calls.length, 1, 'nothing hostile reached the network');
});

test('publish carries the previewed hash and refuses one that was never previewed', async () => {
  const calls = [];
  const api = createKnowledgeAPI(async (path, init) => {
    calls.push({path, init});
    return jsonResponse(201, {pack_id: 'policy', version: 1, hash: HASH});
  }, FakeFormData);

  const published = await api.publish('policy', fakeFile('policy.md'), HASH);
  assert.equal(published.version, 1);
  assert.deepEqual(calls[0].init.body.entries.map((entry) => entry.name), ['hash', 'document']);
  assert.equal(calls[0].init.body.entries[0].value, HASH);

  await assert.rejects(() => api.publish('policy', fakeFile('policy.md'), 'short'),
    (error) => error.code === 'invalid_response');
  assert.equal(calls.length, 1);
});

test('server error codes surface unchanged', async () => {
  const api = createKnowledgeAPI(async () => jsonResponse(422, {error: {code: 'unsupported_content'}}), FakeFormData);
  await assert.rejects(() => api.preview('policy', fakeFile('policy.md')), (error) => error.code === 'unsupported_content');

  const offline = createKnowledgeAPI(async () => { throw new Error('offline'); }, FakeFormData);
  await assert.rejects(() => offline.preview('policy', fakeFile('policy.md')),
    (error) => error.code === 'network_unavailable');
});

function harness(overrides = {}) {
  const rendered = [];
  const api = {
    preview: async () => decodePreview(previewBody()),
    publish: async () => Object.freeze({packID: 'policy', version: 1, hash: HASH}),
    listVersions: async () => Object.freeze([
      Object.freeze({version: 1, hash: HASH, title: 'policy.md', excerptCount: 1, tombstoned: false}),
    ]),
    tombstone: async () => true,
    ...overrides,
  };
  const controller = createKnowledgeController({api, render: (model) => rendered.push(model)});
  return {controller, rendered};
}

test('selecting a new file invalidates the previous preview', async () => {
  const {controller} = harness();
  controller.setPack('policy');
  controller.selectFile(fakeFile('policy.md'));
  assert.equal(await controller.previewFile(), true);
  assert.equal(controller.snapshot().preview.hash, HASH);
  assert.equal(controller.snapshot().publishDisabled, false);

  controller.selectFile(fakeFile('other.md'));
  assert.equal(controller.snapshot().preview, null,
    'a new file must never inherit the previous file\'s reviewed hash');
  assert.equal(controller.snapshot().publishDisabled, true);
});

test('changing pack clears the preview because the hash is pack scoped', async () => {
  const {controller} = harness();
  controller.setPack('policy');
  controller.selectFile(fakeFile('policy.md'));
  await controller.previewFile();
  controller.setPack('other');
  assert.equal(controller.snapshot().preview, null);
  assert.deepEqual([...controller.snapshot().versions], []);
});

test('publishing sends the reviewed hash then clears the staged file', async () => {
  const seen = [];
  const {controller} = harness({
    publish: async (packID, file, hash) => {
      seen.push({packID, filename: file.name, hash});
      return Object.freeze({packID, version: 1, hash});
    },
  });
  controller.setPack('policy');
  controller.selectFile(fakeFile('policy.md'));
  await controller.previewFile();
  assert.equal(await controller.publishPreviewed(), true);

  assert.deepEqual(seen, [{packID: 'policy', filename: 'policy.md', hash: HASH}]);
  const snapshot = controller.snapshot();
  assert.equal(snapshot.preview, null);
  assert.equal(snapshot.filename, '');
  assert.equal(snapshot.versions.length, 1);
  assert.equal(snapshot.notice, 'published');
});

test('publishing without a preview is refused locally', async () => {
  let published = false;
  const {controller} = harness({publish: async () => { published = true; return {}; }});
  controller.setPack('policy');
  controller.selectFile(fakeFile('policy.md'));
  assert.equal(await controller.publishPreviewed(), false);
  assert.equal(published, false, 'unreviewed material never reaches the network');
  assert.equal(controller.snapshot().notice, 'unsupported_content');
});

test('an unsupported file is reported without a request', async () => {
  let previewed = false;
  const {controller} = harness({preview: async () => { previewed = true; return {}; }});
  controller.setPack('policy');
  controller.selectFile(fakeFile('policy.pdf'));
  assert.equal(controller.snapshot().notice, 'unsupported_content');
  assert.equal(controller.snapshot().publishDisabled, true);
  assert.equal(previewed, false);
});

test('one action runs at a time', async () => {
  let release;
  const gate = new Promise((resolve) => { release = resolve; });
  const {controller} = harness({preview: async () => { await gate; return decodePreview(previewBody()); }});
  controller.setPack('policy');
  controller.selectFile(fakeFile('policy.md'));
  const first = controller.previewFile();
  assert.equal(await controller.refreshVersions(), false);
  assert.equal(controller.snapshot().notice, 'busy');
  release();
  assert.equal(await first, true);
});

test('tombstoning refreshes the list and reports the server code on failure', async () => {
  const {controller} = harness();
  controller.setPack('policy');
  assert.equal(await controller.tombstoneVersion(1), true);
  assert.equal(controller.snapshot().notice, 'tombstoned');

  const failing = harness({
    tombstone: async () => { throw Object.assign(new Error('x'), {code: 'knowledge_not_found'}); },
  });
  failing.controller.setPack('policy');
  assert.equal(await failing.controller.tombstoneVersion(9), false);
  assert.equal(failing.controller.snapshot().notice, 'knowledge_not_found');
});

test('destroy drops retained material and stops rendering', async () => {
  const {controller, rendered} = harness();
  controller.setPack('policy');
  controller.selectFile(fakeFile('policy.md'));
  await controller.previewFile();
  const before = rendered.length;

  controller.destroy();
  const snapshot = controller.snapshot();
  assert.equal(snapshot.preview, null);
  assert.equal(snapshot.filename, '');
  assert.equal(snapshot.packID, '');
  controller.setPack('again');
  assert.equal(rendered.length, before, 'a destroyed controller renders nothing further');
});

test('decodePreview rejects a pack id the browser would not have asked for', () => {
  assert.throws(() => decodePreview(previewBody({pack_id: 'builtin/../x'})));
  assert.throws(() => decodePreview(previewBody({hash: OTHER_HASH.toUpperCase()})));
});
