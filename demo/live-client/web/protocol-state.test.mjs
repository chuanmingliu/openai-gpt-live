import test from 'node:test';
import assert from 'node:assert/strict';

import {ProtocolState, buildAudioAppend, buildSessionUpdate} from './protocol-state.mjs';

test('session stays configuring until the upstream update is acknowledged', () => {
  const state = new ProtocolState();
  assert.deepEqual(state.consume({type: 'session.created'}), [{type: 'status', value: 'configuring'}]);
  assert.deepEqual(state.consume({type: 'session.updated'}), [{type: 'status', value: 'listening'}]);
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
    [{type: 'user-delta', text: 'hel'}],
  );
  assert.deepEqual(
    state.consume({type: 'conversation.item.input_audio_transcription.completed', item_id: 'msg_1', transcript: 'hello'}),
    [{type: 'user-done', text: 'hello'}],
  );
  assert.deepEqual(
    state.consume({type: 'conversation.item.input_audio_transcription.completed', item_id: 'msg_1', transcript: 'hello'}),
    [],
  );
});

test('assistant transcript deltas and terminal text are classified once', () => {
  const state = new ProtocolState();
  assert.deepEqual(
    state.consume({type: 'response.output_audio_transcript.delta', response_id: 'resp_1', delta: 'hi'}),
    [{type: 'assistant-delta', text: 'hi'}],
  );
  assert.deepEqual(
    state.consume({type: 'response.output_audio_transcript.done', response_id: 'resp_1', transcript: 'hi there'}),
    [{type: 'assistant-done', text: 'hi there'}],
  );
  assert.deepEqual(
    state.consume({type: 'response.output_audio_transcript.done', response_id: 'resp_1', transcript: 'changed'}),
    [],
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
