import assert from 'node:assert/strict';
import test from 'node:test';

import {createVoiceProfileState, BUILTIN_PROFILE} from './voice-profile-state.mjs';

const HASH = 'd'.repeat(64);

function publishedList() {
  return {profiles: [
    {id: 'insurance', version: 3, active: true, revoked: false, snapshot_hash: HASH},
    {id: 'insurance', version: 2, active: false, revoked: true, snapshot_hash: HASH},
    {id: 'claims', version: 1, active: false, revoked: false, snapshot_hash: HASH},
  ]};
}

test('builtin is the default and revoked versions are never selectable', () => {
  const state = createVoiceProfileState();
  assert.deepEqual(state.selection(), {id: 'builtin', version: 1});
  const options = state.loadPublished(publishedList());
  assert.equal(options[0].id, 'builtin');
  assert.ok(options.some((option) => option.id === 'insurance' && option.version === 3));
  assert.ok(!options.some((option) => option.id === 'insurance' && option.version === 2));
});

test('builtin selection sends no profile query; custom sends the exact pair', () => {
  const state = createVoiceProfileState();
  assert.equal(state.query(), '');
  state.loadPublished(publishedList());
  state.select({id: 'insurance', version: 3});
  assert.equal(state.query(), 'profile=insurance&profile_version=3');
});

test('capture waits for exact profile and provider acknowledgements', () => {
  const state = createVoiceProfileState();
  state.loadPublished(publishedList());
  state.select({id: 'insurance', version: 3});
  state.onConnected();
  state.onProviderUpdated();
  assert.equal(state.captureReady(), false);
  assert.equal(state.profileAckPending(), true);
  state.onProfileSelected({id: 'insurance', version: 3, snapshot_hash: HASH});
  assert.equal(state.captureReady(), true);
  assert.equal(state.profileAckPending(), false);
});

test('builtin selection requires only the provider acknowledgement', () => {
  const state = createVoiceProfileState();
  state.onConnected();
  assert.equal(state.captureReady(), false);
  state.onProviderUpdated();
  assert.equal(state.captureReady(), true);
  assert.equal(state.profileAckPending(), false);
});

test('mismatched profile ack fails closed and selection locks after connect', () => {
  const state = createVoiceProfileState();
  state.loadPublished(publishedList());
  state.select({id: 'insurance', version: 3});
  state.onConnected();
  assert.throws(() => state.select({id: 'claims', version: 1}), /selection_locked/);
  state.onProfileSelected({id: 'insurance', version: 4, snapshot_hash: HASH});
  state.onProviderUpdated();
  assert.equal(state.captureReady(), false);
  assert.equal(state.failureClass(), 'profile_mismatch');
});

test('malformed acknowledgements fail closed', () => {
  const state = createVoiceProfileState();
  state.loadPublished(publishedList());
  state.select({id: 'insurance', version: 3});
  state.onConnected();
  state.onProfileSelected({id: 'insurance', version: 3, snapshot_hash: 'not-a-hash'});
  assert.equal(state.failureClass(), 'profile_mismatch');
  assert.equal(state.captureReady(), false);
});

test('disconnect unlocks selection and clears acknowledgements', () => {
  const state = createVoiceProfileState();
  state.loadPublished(publishedList());
  state.select({id: 'insurance', version: 3});
  state.onConnected();
  state.onProviderUpdated();
  state.onProfileSelected({id: 'insurance', version: 3, snapshot_hash: HASH});
  state.onDisconnected();
  assert.equal(state.captureReady(), false);
  state.select({id: 'claims', version: 1});
  assert.deepEqual(state.selection(), {id: 'claims', version: 1});
});

test('selection outside the published options is refused', () => {
  const state = createVoiceProfileState();
  assert.throws(() => state.select({id: 'insurance', version: 3}), /selection_unavailable/);
  assert.throws(() => state.select({id: 'x y', version: 1}), /selection_unavailable/);
});

test('reloading the list keeps a locked selection even if it disappears', () => {
  const state = createVoiceProfileState();
  state.loadPublished(publishedList());
  state.select({id: 'insurance', version: 3});
  state.onConnected();
  state.loadPublished({profiles: []});
  assert.deepEqual(state.selection(), {id: 'insurance', version: 3});
  state.onDisconnected();
  state.loadPublished({profiles: []});
  assert.deepEqual(state.selection(), {id: BUILTIN_PROFILE.id, version: BUILTIN_PROFILE.version});
});
