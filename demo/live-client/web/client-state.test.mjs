import test from 'node:test';
import assert from 'node:assert/strict';

import {presentationUI} from './app.js';
import {ClientPresentation} from './client-state.mjs';

test('a retry cannot append to the failed session partial turn', () => {
  const state = new ClientPresentation({maximumTurns: 4});
  state.beginSession();
  state.apply({type: 'user-delta', item_id: 'old', text: 'old partial'});
  state.endSession('error');
  state.beginSession();
  state.apply({type: 'user-delta', item_id: 'new', text: 'new'});
  assert.deepEqual(state.snapshot().turns.map((turn) => turn.text), ['old partial', 'new']);
});

test('a retry does not announce a finalized turn from the failed session again', () => {
  const state = new ClientPresentation();
  state.apply({type: 'user-delta', item_id: 'old', text: 'old partial'});
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
    state.apply({type: 'user-done', item_id: `item-${text}`, text});
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
  state.apply({type: 'user-done', item_id: 'item-retained', text: 'a retained transcript turn'});
  state.setMuted(true);
  assert.equal(state.isMuted(), true);
  state.setMuted(false);
  assert.equal(state.isMuted(), false);
});

test('only finalized transcript turns are announced', () => {
  const state = new ClientPresentation();
  state.apply({type: 'assistant-delta', response_id: 'response-1', text: 'partial'});
  assert.equal(state.snapshot().announcement, '');
  assert.deepEqual(state.drainAnnouncements(), []);
  state.apply({type: 'assistant-done', response_id: 'response-1', text: 'final answer'});
  assert.equal(state.snapshot().announcement, 'Assistant: final answer');
  assert.deepEqual(state.drainAnnouncements(), ['Assistant: final answer']);
  assert.equal(state.snapshot().announcement, '');
});

test('pending announcements retain no more than the transcript bound', () => {
  const state = new ClientPresentation({maximumTurns: 2});
  for (const text of ['one', 'two', 'three', 'four', 'five']) {
    state.apply({type: 'user-done', item_id: `item-${text}`, text});
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
  state.apply({type: 'user-delta', item_id: 'unfinished-item', text: 'unfinished'});
  state.endSession('error');
  const snapshot = state.snapshot();
  assert.equal(snapshot.status, 'error');
  assert.deepEqual(snapshot.turns, [{id: 'unfinished-item', role: 'user', text: 'unfinished', final: true}]);
});

test('rejects transcript bounds outside the supported range', () => {
  assert.throws(() => new ClientPresentation({maximumTurns: 0}), /invalid transcript bound/);
  assert.throws(() => new ClientPresentation({maximumTurns: 501}), /invalid transcript bound/);
});

test('interleaved transcript identifiers never concatenate across turns', () => {
  const state = new ClientPresentation();
  state.apply({type: 'user-delta', item_id: 'item-a', text: 'hel'});
  state.apply({type: 'user-delta', item_id: 'item-b', text: 'oth'});
  state.apply({type: 'user-delta', item_id: 'item-a', text: 'lo'});
  state.apply({type: 'user-done', item_id: 'item-a', text: 'hello'});
  state.apply({type: 'user-done', item_id: 'item-b', text: 'other'});

  assert.deepEqual(state.snapshot().turns, [
    {id: 'item-a', role: 'user', text: 'hello', final: true},
    {id: 'item-b', role: 'user', text: 'other', final: true},
  ]);
});

test('presentation evicts the oldest active identifier beyond its 64-ID bound', () => {
  const state = new ClientPresentation({maximumTurns: 100});
  for (let index = 0; index < 65; index += 1) {
    assert.equal(state.apply({type: 'assistant-delta', response_id: `response-${index}`, text: 'x'}), true);
  }
  const turns = state.snapshot().turns;
  assert.equal(turns.length, 64);
  assert.equal(turns.some(({id}) => id === 'response-0'), false);
  assert.equal(turns.some(({id}) => id === 'response-64'), true);
});

test('per-turn and aggregate UTF-8 transcript byte budgets remain bounded', () => {
  const state = new ClientPresentation({maximumTurnBytes: 5, maximumTranscriptBytes: 8});
  assert.equal(state.apply({type: 'user-delta', item_id: 'one', text: '12345'}), true);
  assert.equal(state.apply({type: 'user-delta', item_id: 'two', text: '678'}), true);
  assert.equal(state.apply({type: 'user-delta', item_id: 'three', text: '9'}), true);
  assert.equal(state.snapshot().turns.reduce((total, turn) => total + Buffer.byteLength(turn.text), 0) <= 8, true);
  assert.equal(state.snapshot().turns.some(({id}) => id === 'one'), false);
  assert.equal(state.apply({type: 'user-delta', item_id: 'two', text: '901'}), false);
});

test('timeline orders transcript turns and tool cards once while card updates retain position', () => {
  const state = new ClientPresentation();
  state.apply({type: 'user-done', item_id: 'turn-1', text: 'Find the latest docs.'});
  state.apply({type: 'handoff-card', event: {
    type: 'live.handoff.created', handoff_id: 'handoff-1', sequence_id: 1, status: 'pending', details: {},
  }});
  state.apply({type: 'assistant-done', response_id: 'turn-2', text: 'I started that.'});
  state.apply({type: 'handoff-card', event: {
    type: 'live.handoff.progress', handoff_id: 'handoff-1', sequence_id: 2, status: 'running',
    details: {phase: 'searching', summary: 'Searching the web.'},
  }});

  assert.deepEqual(state.snapshot().timeline.map(({kind, id, phase}) => ({kind, id, phase})), [
    {kind: 'turn', id: 'turn-1', phase: undefined},
    {kind: 'tool', id: 'handoff-1', phase: 'searching'},
    {kind: 'turn', id: 'turn-2', phase: undefined},
  ]);
});

test('handoff progress never changes voice status or playback generation', () => {
  const state = new ClientPresentation();
  state.apply({type: 'status', value: 'assistant-speaking'});
  const before = state.snapshot();
  state.apply({type: 'handoff-card', event: {
    type: 'live.handoff.progress', handoff_id: 'handoff-1', sequence_id: 2, status: 'running',
    details: {phase: 'searching', summary: 'Searching the web.'},
  }});
  const after = state.snapshot();
  assert.equal(after.status, before.status);
  assert.equal(after.playbackGeneration, before.playbackGeneration);
  assert.deepEqual(state.drainToolAnnouncements(), ['Searching the web.']);
});
