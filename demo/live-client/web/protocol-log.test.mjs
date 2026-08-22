import assert from 'node:assert/strict';
import test from 'node:test';

import {ProtocolEventJournal, protocolEventDetail} from './protocol-log.mjs';

test('protocol journal retains only bounded redacted metadata', () => {
  let now = 1000;
  const journal = new ProtocolEventJournal({maximumEntries: 3, now: () => now++});

  journal.record('inbound', {
    type: 'response.output_audio_transcript.delta',
    response_id: 'private-response-id',
    delta: 'private transcript text',
  });
  assert.doesNotMatch(JSON.stringify(journal.snapshot()), /private-response-id|private transcript text/);
  journal.record('inbound', {type: 'response.done', response: {status: 'completed', id: 'private-response-id'}});
  journal.record('inbound', {type: 'error', error: {code: 'private-code', message: 'private provider detail'}});
  journal.record('inbound', {type: 'session.updated', session: {instructions: 'private system prompt'}});

  const snapshot = journal.snapshot();
  assert.equal(snapshot.length, 3);
  assert.deepEqual(snapshot.map(({type}) => type), ['response.done', 'error', 'session.updated']);
  assert.equal(protocolEventDetail(snapshot[0]), 'status=completed');
  const serialized = JSON.stringify(snapshot);
  assert.doesNotMatch(serialized, /private-response-id|private-code|private system prompt|private provider detail/);
});

test('protocol journal aggregates consecutive high-rate audio frames without retaining base64', () => {
  const journal = new ProtocolEventJournal({maximumEntries: 4, now: () => 2000});

  journal.record('outbound', {type: 'input_audio_buffer.append', audio: 'QUJD'});
  journal.record('outbound', {type: 'input_audio_buffer.append', audio: 'REVG'});
  journal.record('inbound', {type: 'response.output_audio.delta', delta: 'R0hJ'});

  const snapshot = journal.snapshot();
  assert.deepEqual(snapshot, [
    {sequence: 1, at: 2000, direction: 'outbound', type: 'input_audio_buffer.append', count: 2, bytes: 8},
    {sequence: 2, at: 2000, direction: 'inbound', type: 'response.output_audio.delta', count: 1, bytes: 4},
  ]);
  assert.equal(protocolEventDetail(snapshot[0]), '2 events · 8 encoded chars');
  assert.doesNotMatch(JSON.stringify(snapshot), /QUJD|REVG|R0hJ/);
});

test('protocol journal clear releases retained session metadata', () => {
  const journal = new ProtocolEventJournal({now: () => 3000});
  journal.record('inbound', {type: 'session.created'});
  journal.clear();
  assert.deepEqual(journal.snapshot(), []);
});
