import test from 'node:test';
import assert from 'node:assert/strict';
import { initialProtocolState, reduceProtocolEvent } from './protocol-state.mjs';

test('user transcript deltas finalize once', () => {
  let state = initialProtocolState();
  state = reduceProtocolEvent(state, {
    type: 'conversation.item.input_audio_transcription.delta',
    item_id: 'u1',
    content_index: 0,
    delta: 'hel',
  });
  state = reduceProtocolEvent(state, {
    type: 'conversation.item.input_audio_transcription.completed',
    item_id: 'u1',
    content_index: 0,
    transcript: 'hello',
  });
  const duplicate = reduceProtocolEvent(state, {
    type: 'conversation.item.input_audio_transcription.completed',
    item_id: 'u1',
    content_index: 0,
    transcript: 'hello',
  });

  assert.deepEqual(duplicate.turns, state.turns);
  assert.equal(state.turns.at(-1).text, 'hello');
  assert.equal(state.turns.at(-1).speaker, 'user');
  assert.deepEqual(state.drafts, {});
});

test('speech start requests playback clear and user-speaking state', () => {
  const state = reduceProtocolEvent(initialProtocolState(), {
    type: 'input_audio_buffer.speech_started',
    item_id: 'u1',
    audio_start_ms: 0,
  });

  assert.equal(state.phase, 'user-speaking');
  assert.equal(state.effect, 'clear-playback');
});

test('user drafts accumulate deltas under an item content key', () => {
  let state = initialProtocolState();
  state = reduceProtocolEvent(state, {
    type: 'conversation.item.input_audio_transcription.delta',
    item_id: 'u1',
    content_index: 0,
    delta: 'hel',
  });
  state = reduceProtocolEvent(state, {
    type: 'conversation.item.input_audio_transcription.delta',
    item_id: 'u1',
    content_index: 0,
    delta: 'lo',
  });

  assert.equal(state.drafts['user:u1:0'].text, 'hello');
});

test('assistant transcript deltas finalize under their response key', () => {
  let state = initialProtocolState();
  state = reduceProtocolEvent(state, {
    type: 'response.output_audio_transcript.delta',
    response_id: 'r1',
    item_id: 'a1',
    content_index: 0,
    delta: 'Hi',
  });
  state = reduceProtocolEvent(state, {
    type: 'response.output_audio_transcript.done',
    response_id: 'r1',
    item_id: 'a1',
    content_index: 0,
    transcript: 'Hi there',
  });

  assert.deepEqual(state.turns, [{ speaker: 'assistant', text: 'Hi there' }]);
  assert.deepEqual(state.drafts, {});
});

test('assistant audio changes phase without retaining encoded audio', () => {
  const encodedAudio = 'AQIDBA==';
  const state = reduceProtocolEvent(initialProtocolState(), {
    type: 'response.output_audio.delta',
    response_id: 'r1',
    item_id: 'a1',
    delta: encodedAudio,
  });

  assert.equal(state.phase, 'assistant-speaking');
  assert.equal(state.status.at(-1).kind, 'assistant-audio');
  assert.doesNotMatch(JSON.stringify(state), /AQIDBA==/);
});

test('response completion returns to ready when connected', () => {
  let state = reduceProtocolEvent(initialProtocolState(), { type: 'session.created' });
  state = reduceProtocolEvent(state, { type: 'response.created', response: { id: 'r1' } });
  state = reduceProtocolEvent(state, { type: 'response.done', response: { id: 'r1' } });

  assert.equal(state.connected, true);
  assert.equal(state.phase, 'ready');
  assert.equal(state.effect, null);
});

test('unknown and error events become bounded status without payload echo', () => {
  const secret = 'sentinel-private-payload';
  let state = reduceProtocolEvent(initialProtocolState(), {
    type: 'error',
    error: { code: 'provider_unavailable', message: secret },
  });
  for (let index = 0; index < 60; index += 1) {
    state = reduceProtocolEvent(state, { type: `unknown-${index}`, payload: secret });
  }

  assert.equal(state.status.at(-1).kind, 'unknown-event');
  assert.ok(state.status.length <= 50);
  assert.doesNotMatch(JSON.stringify(state), /sentinel-private-payload/);
});

test('only known protocol error codes are retained', () => {
  const secretCode = 'sentinel-private-code';
  const state = reduceProtocolEvent(initialProtocolState(), {
    type: 'error',
    error: { code: secretCode },
  });

  assert.equal(state.status.at(-1).code, 'unknown');
  assert.doesNotMatch(JSON.stringify(state), /sentinel-private-code/);
});

test('local startup failure keeps a sanitized reachable status', () => {
  const secret = 'sentinel-private-exception';
  const state = reduceProtocolEvent(initialProtocolState(), {
    type: 'client.start_failed',
    error: { message: secret },
  });

  assert.equal(state.phase, 'idle');
  assert.equal(state.status.at(-1).kind, 'start-failed');
  assert.doesNotMatch(JSON.stringify(state), /sentinel-private-exception/);
});

test('unavailable local session retains a fixed status without close payload', () => {
  const secret = 'sentinel-private-close-reason';
  const state = reduceProtocolEvent(initialProtocolState(), {
    type: 'client.session_unavailable',
    close_reason: secret,
  });

  assert.equal(state.phase, 'idle');
  assert.equal(state.status.at(-1).kind, 'session-unavailable');
  assert.doesNotMatch(JSON.stringify(state), /sentinel-private-close-reason/);
});

test('transcript history retains its newest one hundred finalized turns', () => {
  let state = initialProtocolState();
  for (let index = 0; index < 102; index += 1) {
    state = reduceProtocolEvent(state, {
      type: 'conversation.item.input_audio_transcription.completed',
      item_id: `u${index}`,
      content_index: 0,
      transcript: `turn ${index}`,
    });
  }

  assert.equal(state.turns.length, 100);
  assert.deepEqual(state.turns[0], { speaker: 'user', text: 'turn 2' });
  assert.deepEqual(state.turns.at(-1), { speaker: 'user', text: 'turn 101' });
});
