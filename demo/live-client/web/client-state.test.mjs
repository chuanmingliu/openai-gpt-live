import test from 'node:test';
import assert from 'node:assert/strict';

import {presentationUI} from './app.js';
import {ClientPresentation} from './client-state.mjs';

test('a retry cannot append to the failed session partial turn', () => {
  const state = new ClientPresentation({maximumTurns: 4});
  state.beginSession();
  state.apply({type: 'user-delta', text: 'old partial'});
  state.endSession('error');
  state.beginSession();
  state.apply({type: 'user-delta', text: 'new'});
  assert.deepEqual(state.snapshot().turns.map((turn) => turn.text), ['old partial', 'new']);
});

test('a retry does not announce a finalized turn from the failed session again', () => {
  const state = new ClientPresentation();
  state.apply({type: 'user-delta', text: 'old partial'});
  state.endSession('error');
  state.beginSession();
  assert.deepEqual(state.drainAnnouncements(), []);
});

test('incoming audio buffers before playback acknowledgement', () => {
  const state = new ClientPresentation({maximumTurns: 4});
  state.beginSession();
  state.apply({type: 'audio-buffered'});
  assert.equal(state.snapshot().status, 'assistant-buffering');
  state.apply({type: 'audio-playing'});
  assert.equal(state.snapshot().status, 'assistant-speaking');
});

test('listening does not hide acknowledged playback until playback is idle', () => {
  const state = new ClientPresentation();
  state.apply({type: 'audio-buffered'});
  state.apply({type: 'audio-playing'});
  state.apply({type: 'status', value: 'listening'});
  assert.equal(state.snapshot().status, 'assistant-speaking');
  state.apply({type: 'audio-idle'});
  assert.equal(state.snapshot().status, 'listening');
});

test('finalized transcript history evicts the oldest turn', () => {
  const state = new ClientPresentation({maximumTurns: 2});
  state.beginSession();
  for (const text of ['one', 'two', 'three']) {
    state.apply({type: 'user-done', text});
  }
  assert.deepEqual(state.snapshot().turns.map((turn) => turn.text), ['two', 'three']);
});

test('muting updates the pressed state without storing audio', () => {
  const state = new ClientPresentation();
  state.setMuted(true);
  const snapshot = state.snapshot();
  assert.equal(snapshot.muted, true);
  assert.equal(snapshot.turns.length, 0);
});

test('truthful presentation states name controls that remain enabled', () => {
  const state = new ClientPresentation();
  state.beginSession();

  state.setMuted(true);
  let ui = presentationUI(state.snapshot());
  assert.match(ui.statusText, /Select Unmute/);
  assert.equal(ui.muteDisabled, false);

  state.setMuted(false);
  state.apply({type: 'audio-buffered'});
  ui = presentationUI(state.snapshot());
  assert.match(ui.statusText, /Assistant audio is buffered/);
  assert.equal(ui.stopDisabled, false);

  state.apply({type: 'audio-suspended'});
  ui = presentationUI(state.snapshot());
  assert.equal(state.snapshot().status, 'audio-suspended');
  assert.match(ui.statusText, /Select Enable audio/);
  assert.equal(ui.resumeAudioHidden, false);
  assert.equal(ui.resumeAudioDisabled, false);

  state.apply({type: 'audio-resumed'});
  assert.equal(state.snapshot().status, 'assistant-buffering');

  state.endSession('error');
  ui = presentationUI(state.snapshot(), 'transport-closed');
  assert.match(ui.noticeText, /Select Start listening to reconnect/);
  assert.equal(ui.startDisabled, false);
});

test('mute can be read as content-free scalar state', () => {
  const state = new ClientPresentation();
  state.apply({type: 'user-done', text: 'a retained transcript turn'});
  state.setMuted(true);
  assert.equal(state.isMuted(), true);
  state.setMuted(false);
  assert.equal(state.isMuted(), false);
});

test('only finalized transcript turns are announced', () => {
  const state = new ClientPresentation();
  state.apply({type: 'assistant-delta', text: 'partial'});
  assert.equal(state.snapshot().announcement, '');
  assert.deepEqual(state.drainAnnouncements(), []);
  state.apply({type: 'assistant-done', text: 'final answer'});
  assert.equal(state.snapshot().announcement, 'Assistant: final answer');
  assert.deepEqual(state.drainAnnouncements(), ['Assistant: final answer']);
  assert.equal(state.snapshot().announcement, '');
});

test('pending announcements retain no more than the transcript bound', () => {
  const state = new ClientPresentation({maximumTurns: 2});
  for (const text of ['one', 'two', 'three', 'four', 'five']) {
    state.apply({type: 'user-done', text});
  }
  assert.deepEqual(state.drainAnnouncements(), ['You: four', 'You: five']);
});

test('clearing playback happens before user speech and invalidates queued playback', () => {
  const state = new ClientPresentation();
  const before = state.snapshot().playbackGeneration;
  state.apply({type: 'audio-buffered'});
  state.apply({type: 'clear-playback'});
  state.apply({type: 'status', value: 'user-speaking'});
  const snapshot = state.snapshot();
  assert.equal(snapshot.playbackGeneration, before + 1);
  assert.equal(snapshot.status, 'user-speaking');
});

test('speech start advances playback generation before user-speaking', () => {
  const state = new ClientPresentation();
  state.beginSession();
  const before = state.snapshot().playbackGeneration;
  state.apply({type: 'clear-playback'});
  state.apply({type: 'status', value: 'user-speaking'});
  const after = state.snapshot();
  assert.equal(after.playbackGeneration, before + 1);
  assert.equal(after.status, 'user-speaking');
});

test('a stale playback acknowledgement cannot override user speech after playback is cleared', () => {
  const state = new ClientPresentation();
  state.apply({type: 'audio-buffered'});
  state.apply({type: 'clear-playback'});
  state.apply({type: 'status', value: 'user-speaking'});
  state.apply({type: 'audio-playing'});
  assert.equal(state.snapshot().status, 'user-speaking');
});

test('ending a session finalizes partial turns and records the terminal status', () => {
  const state = new ClientPresentation();
  state.apply({type: 'user-delta', text: 'unfinished'});
  state.endSession('error');
  const snapshot = state.snapshot();
  assert.equal(snapshot.status, 'error');
  assert.deepEqual(snapshot.turns, [{role: 'user', text: 'unfinished', final: true}]);
});

test('rejects transcript bounds outside the supported range', () => {
  assert.throws(() => new ClientPresentation({maximumTurns: 0}), /invalid transcript bound/);
  assert.throws(() => new ClientPresentation({maximumTurns: 501}), /invalid transcript bound/);
});
