import test from 'node:test';
import assert from 'node:assert/strict';

import {ProtocolState, buildAudioAppend, buildSessionUpdate} from './protocol-state.mjs';

test('session stays configuring until the upstream update is acknowledged', () => {
	const state = new ProtocolState();
	assert.deepEqual(state.consume({type: 'session.created'}), [{type: 'status', value: 'configuring'}]);
	assert.deepEqual(state.consume({type: 'session.updated'}), []);
});

test('speech start clears playback before reporting user speech', () => {
  const effects = new ProtocolState().consume({type: 'input_audio_buffer.speech_started'});
  assert.deepEqual(effects.map((effect) => effect.type), ['clear-playback', 'status']);
  assert.equal(effects[1].value, 'user-speaking');
});

test('user transcript deltas finalize once despite a duplicate terminal event', () => {
  const state = new ProtocolState();
  assert.deepEqual(
    state.consume({type: 'conversation.item.input_audio_transcription.delta', item_id: 'msg_1', delta: 'hel'}),
    [{type: 'user-delta', item_id: 'msg_1', text: 'hel'}],
  );
  assert.deepEqual(
    state.consume({type: 'conversation.item.input_audio_transcription.completed', item_id: 'msg_1', transcript: 'hello'}),
    [{type: 'user-done', item_id: 'msg_1', text: 'hello'}],
  );
  assert.deepEqual(
    state.consume({type: 'conversation.item.input_audio_transcription.completed', item_id: 'msg_1', transcript: 'hello'}),
    [],
  );
});

test('assistant transcript chunks remain one turn and finalize at response.done', () => {
  const state = new ProtocolState();
  assert.deepEqual(
    state.consume({type: 'response.output_audio_transcript.delta', response_id: 'resp_1', delta: 'hi'}),
    [{type: 'assistant-delta', response_id: 'resp_1', text: 'hi'}],
  );
  assert.deepEqual(
    state.consume({type: 'response.output_audio_transcript.done', response_id: 'resp_1', transcript: 'hi there'}),
    [{type: 'assistant-delta', response_id: 'resp_1', text: ' there'}],
  );
  assert.deepEqual(
    state.consume({type: 'response.output_audio_transcript.done', response_id: 'resp_1', transcript: 'changed'}),
    [{type: 'assistant-delta', response_id: 'resp_1', text: 'changed'}],
  );
  assert.deepEqual(
    state.consume({type: 'response.output_audio_transcript.done', response_id: 'resp_1', transcript: 'changed'}),
    [],
  );
  assert.deepEqual(
    state.consume({type: 'response.done', response: {id: 'resp_1', status: 'completed'}}),
    [{type: 'assistant-done', response_id: 'resp_1', text: 'hi therechanged'}, {type: 'status', value: 'listening'}],
  );
  assert.deepEqual(
    state.consume({type: 'response.done', response: {id: 'resp_1', status: 'completed'}}),
    [{type: 'status', value: 'listening'}],
  );
});

test('audio is classified without retaining its raw envelope', () => {
  const state = new ProtocolState();
  assert.deepEqual(
    state.consume({type: 'response.output_audio.delta', response_id: 'resp_1', delta: 'AAA='}),
    [{type: 'audio', encoded: 'AAA='}],
  );
});

test('malformed identifiers and oversized transcript text fail closed', () => {
  const state = new ProtocolState();
  assert.deepEqual(
    state.consume({type: 'conversation.item.input_audio_transcription.delta', item_id: '', delta: 'x'}),
    [{type: 'error', code: 'invalid-transcript'}],
  );
  assert.deepEqual(
    state.consume({type: 'response.output_audio_transcript.done', response_id: 'r', transcript: 'x'.repeat((64 << 10) + 1)}),
    [{type: 'error', code: 'invalid-transcript'}],
  );
});

test('gateway errors are reduced to a bounded code without provider messages', () => {
  const state = new ProtocolState();
  assert.deepEqual(
    state.consume({type: 'error', error: {code: 'rate_limit', message: 'private provider body'}}),
    [{type: 'error', code: 'rate_limit'}],
  );
  assert.deepEqual(
    state.consume({type: 'error', error: {code: 'bad code', message: 'private'}}),
    [{type: 'error', code: 'gateway-error'}],
  );
});

test('session update enables exact 24 kHz server VAD and interruption without creating a response', () => {
  assert.deepEqual(buildSessionUpdate(), {
    type: 'session.update',
    session: {
      type: 'realtime',
      audio: {
        input: {
          format: {type: 'audio/pcm', rate: 24000},
          turn_detection: {
            type: 'server_vad',
            threshold: 0.5,
            prefix_padding_ms: 300,
            silence_duration_ms: 500,
            create_response: true,
            interrupt_response: true,
          },
        },
        output: {format: {type: 'audio/pcm', rate: 24000}},
      },
    },
  });
});

test('audio append accepts only one exact 20 ms PCM frame', () => {
  const frame = Uint8Array.from({length: 960}, (_, index) => index % 251);
  const event = buildAudioAppend(frame);
  assert.equal(event.type, 'input_audio_buffer.append');
  assert.equal(event.event_id, undefined);
  assert.deepEqual(Uint8Array.from(Buffer.from(event.audio, 'base64')), frame);
  assert.throws(() => buildAudioAppend(new Uint8Array(958)), /960-byte/);
});

test('transcript identifier stores stay bounded after more than 64 interleaved IDs', () => {
  const state = new ProtocolState();
  for (let index = 0; index < 65; index += 1) {
    assert.deepEqual(
      state.consume({type: 'conversation.item.input_audio_transcription.delta', item_id: `msg_${index}`, delta: `${index}`}),
      [{type: 'user-delta', item_id: `msg_${index}`, text: `${index}`}],
    );
  }
  assert.equal(state.users.size, 64);
  assert.equal(state.users.has('msg_0'), false);
  assert.equal(state.users.has('msg_64'), true);
});

test('interleaved assistant transcript effects retain response attribution', () => {
  const state = new ProtocolState();
  assert.deepEqual(
    state.consume({type: 'response.output_audio_transcript.delta', response_id: 'resp_a', delta: 'alpha'}),
    [{type: 'assistant-delta', response_id: 'resp_a', text: 'alpha'}],
  );
  assert.deepEqual(
    state.consume({type: 'response.output_audio_transcript.delta', response_id: 'resp_b', delta: 'beta'}),
    [{type: 'assistant-delta', response_id: 'resp_b', text: 'beta'}],
  );
  assert.deepEqual(
    state.consume({type: 'response.done', response: {id: 'resp_b', status: 'cancelled'}}),
    [{type: 'assistant-done', response_id: 'resp_b', text: 'beta'}, {type: 'status', value: 'listening'}],
  );
  assert.deepEqual(
    state.consume({type: 'response.done', response: {id: 'resp_a', status: 'completed'}}),
    [{type: 'assistant-done', response_id: 'resp_a', text: 'alpha'}, {type: 'status', value: 'listening'}],
  );
});

test('tool policy acknowledgement is reduced to a canonical accepted effect', () => {
  const tools = {web_search: {enabled: false, safe_search: 'strict', region: 'cn-zh', max_results: 1, timeout_ms: 1000}};
  assert.deepEqual(new ProtocolState().consume({
    type: 'live.tool_policy.updated', event_id: 'policy-1', tools, model: 'deepseek-v4-flash',
  }), [{type: 'tool-policy-accepted', event_id: 'policy-1', policy: tools, model: 'deepseek-v4-flash'}]);
});

test('tool policy acknowledgement rejects blank identity and unexpected model', () => {
  const tools = {web_search: {enabled: false, safe_search: 'strict', region: 'cn-zh', max_results: 1, timeout_ms: 1000}};
  for (const event of [
    {type: 'live.tool_policy.updated', event_id: '', tools, model: 'deepseek-v4-flash'},
    {type: 'live.tool_policy.updated', event_id: 'policy-1', tools, model: ''},
  ]) {
    assert.deepEqual(new ProtocolState().consume(event), [{type: 'error', code: 'invalid-event'}]);
  }
});

test('handoff events normalize only exact monotonic display fields', () => {
  const event = {
    type: 'live.handoff.progress', event_id: 'event-2', handoff_id: 'handoff-1', sequence_id: 2,
    status: 'running', details: {phase: 'searching', summary: 'Searching the web.', query: 'private query'},
  };
  assert.deepEqual(new ProtocolState().consume(event), [{
    type: 'handoff-card',
    event: {
      type: 'live.handoff.progress', handoff_id: 'handoff-1', sequence_id: 2, status: 'running',
      details: {phase: 'searching', summary: 'Searching the web.'},
    },
  }]);
});

test('handoff envelopes reject invalid sequence, status, phase, and details without retaining content', () => {
  const base = {
    type: 'live.handoff.progress', event_id: 'event-2', handoff_id: 'handoff-1', sequence_id: 2,
    status: 'running', details: {phase: 'searching', summary: 'Searching the web.'},
  };
  for (const event of [
    {...base, sequence_id: 0},
    {...base, status: 'completed'},
    {...base, details: {...base.details, phase: 'fetching_query'}},
    {...base, details: []},
    {...base, command: 'npx'},
  ]) {
    assert.deepEqual(new ProtocolState().consume(event), [{type: 'error', code: 'invalid-event'}]);
  }
});

test('handoff phases accept only the exact phase belonging to each event class', () => {
  const state = new ProtocolState();
  assert.equal(state.consume({
    type: 'live.handoff.created', handoff_id: 'h1', sequence_id: 1, status: 'pending', details: {phase: 'queued'},
  })[0].type, 'handoff-card');
  assert.equal(state.consume({
    type: 'live.handoff.completed', handoff_id: 'h1', sequence_id: 2, status: 'succeeded', details: {phase: 'completed'},
  })[0].type, 'handoff-card');
  assert.deepEqual(state.consume({
    type: 'live.handoff.progress', handoff_id: 'h1', sequence_id: 3, status: 'running', details: {phase: 'completed'},
  }), [{type: 'error', code: 'invalid-event'}]);
});
