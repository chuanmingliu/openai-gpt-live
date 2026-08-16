import assert from 'node:assert/strict';
import test from 'node:test';

import {createDraftModel} from './profile-draft-state.mjs';

const HASH = 'b'.repeat(64);

function draft(generation, templates = ['system.j2']) {
  return Object.freeze({
    id: 'insurance',
    generation,
    description: '',
    templates: Object.freeze(templates.map((path) => Object.freeze({path, hash: HASH, bytes: 10}))),
    fixtures: Object.freeze([]),
  });
}

test('stale save keeps local unsaved source and reports conflict', () => {
  const model = createDraftModel();
  model.loadDraft(draft(3));
  model.selectTemplate('system.j2', 'saved source');
  model.editSource('local unsaved source');
  model.markConflict();
  const snapshot = model.snapshot();
  assert.equal(snapshot.source, 'local unsaved source');
  assert.equal(snapshot.dirty, true);
  assert.equal(snapshot.conflict, true);
  assert.equal(snapshot.draft.generation, 3);
});

test('applySaved replaces the generation and clears dirtiness', () => {
  const model = createDraftModel();
  model.loadDraft(draft(3));
  model.selectTemplate('system.j2', 'old');
  model.editSource('new source');
  model.applySaved(draft(4));
  const snapshot = model.snapshot();
  assert.equal(snapshot.draft.generation, 4);
  assert.equal(snapshot.dirty, false);
  assert.equal(snapshot.conflict, false);
});

test('reloading a draft without the selected template clears the editor', () => {
  const model = createDraftModel();
  model.loadDraft(draft(3, ['system.j2', 'opening_message.j2']));
  model.selectTemplate('opening_message.j2', 'welcome');
  model.loadDraft(draft(4, ['system.j2']));
  const snapshot = model.snapshot();
  assert.equal(snapshot.selectedPath, '');
  assert.equal(snapshot.source, '');
});

test('destroy clears retained source and preview bytes and rejects reuse', () => {
  const model = createDraftModel();
  model.loadDraft(draft(3));
  model.selectTemplate('system.j2', 'sensitive prompt source');
  model.setPreview({snapshotHash: HASH, outputs: {}});
  model.destroy();
  assert.throws(() => model.snapshot());
  assert.throws(() => model.editSource('more'));
});

test('editSource rejects non-string values', () => {
  const model = createDraftModel();
  model.loadDraft(draft(1));
  assert.throws(() => model.editSource(42), TypeError);
});
