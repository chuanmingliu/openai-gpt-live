import test from 'node:test';
import assert from 'node:assert/strict';

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
