const MAX_IDENTIFIERS = 64;
const MAX_TEXT_BYTES = 64 << 10;
const ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$/;
const ERROR_CODE_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;

function validID(value) {
  return typeof value === 'string' && ID_PATTERN.test(value);
}

function validText(value) {
  return typeof value === 'string' && new TextEncoder().encode(value).byteLength <= MAX_TEXT_BYTES;
}

function insertBounded(map, key, value) {
  if (!map.has(key) && map.size >= MAX_IDENTIFIERS) map.delete(map.keys().next().value);
  map.set(key, value);
}

export function buildSessionUpdate() {
  return {
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
  };
}

export function buildAudioAppend(frame) {
  if (!(frame instanceof Uint8Array) || frame.byteLength !== 960) throw new RangeError('audio append requires one 960-byte frame');
  return {type: 'input_audio_buffer.append', audio: encodeBytesBase64(frame)};
}

export class ProtocolState {
  constructor() {
    this.users = new Map();
    this.assistants = new Map();
    this.userSpeaking = false;
  }

  consume(event) {
    if (!event || typeof event !== 'object' || typeof event.type !== 'string') {
      return [{type: 'error', code: 'invalid-event'}];
    }
    switch (event.type) {
      case 'session.created':
        return [{type: 'status', value: 'configuring'}];
      case 'session.updated':
        return [{type: 'status', value: 'listening'}];
      case 'input_audio_buffer.speech_started':
        this.userSpeaking = true;
        return [{type: 'clear-playback'}, {type: 'status', value: 'user-speaking'}];
      case 'input_audio_buffer.speech_stopped':
      case 'response.created':
        this.userSpeaking = false;
        return [{type: 'status', value: 'assistant-thinking'}];
      case 'conversation.item.input_audio_transcription.delta':
        return this.transcriptDelta(this.users, event.item_id, event.delta, 'user-delta');
      case 'conversation.item.input_audio_transcription.completed':
        return this.transcriptDone(this.users, event.item_id, event.transcript, 'user-done');
      case 'response.output_audio_transcript.delta':
        return this.transcriptDelta(this.assistants, event.response_id, event.delta, 'assistant-delta');
      case 'response.output_audio_transcript.done':
        return this.transcriptDone(this.assistants, event.response_id, event.transcript, 'assistant-done');
      case 'response.output_audio.delta':
        if (typeof event.delta !== 'string' || event.delta.length === 0 || event.delta.length > MAX_TEXT_BYTES) {
          return [{type: 'error', code: 'invalid-audio'}];
        }
        return [{type: 'audio', encoded: event.delta}];
      case 'response.done':
        return this.userSpeaking ? [] : [{type: 'status', value: 'listening'}];
      case 'error': {
        const code = event.error?.code;
        return [{type: 'error', code: typeof code === 'string' && ERROR_CODE_PATTERN.test(code) ? code : 'gateway-error'}];
      }
      default:
        return [];
    }
  }

  transcriptDelta(store, id, text, effectType) {
    if (!validID(id) || !validText(text)) return [{type: 'error', code: 'invalid-transcript'}];
    const current = store.get(id);
    if (current?.done) return [];
    const combined = (current?.text ?? '') + text;
    if (!validText(combined)) return [{type: 'error', code: 'invalid-transcript'}];
    insertBounded(store, id, {text: combined, done: false});
    return text.length === 0 ? [] : [{type: effectType, text}];
  }

  transcriptDone(store, id, text, effectType) {
    if (!validID(id) || !validText(text)) return [{type: 'error', code: 'invalid-transcript'}];
    if (store.get(id)?.done) return [];
    insertBounded(store, id, {text, done: true});
    return [{type: effectType, text}];
  }
}
import {encodeBytesBase64} from './audio-core.mjs';
