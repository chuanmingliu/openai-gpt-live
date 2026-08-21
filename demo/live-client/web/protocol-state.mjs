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
        return [];
      case 'live.tool_policy.updated':
        return this.toolPolicyAccepted(event);
      case 'live.handoff.created':
      case 'live.handoff.progress':
      case 'live.handoff.needs_input':
      case 'live.handoff.completed':
      case 'live.handoff.failed':
      case 'live.handoff.cancelled':
        return this.handoffCard(event);
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
        return this.assistantTranscriptPart(event.response_id, event.transcript);
      case 'response.output_audio.delta':
        if (typeof event.delta !== 'string' || event.delta.length === 0 || event.delta.length > MAX_TEXT_BYTES) {
          return [{type: 'error', code: 'invalid-audio'}];
        }
        return [{type: 'audio', encoded: event.delta}];
      case 'response.output_audio.done':
        return [{type: 'audio-end'}];
      case 'response.done':
        return this.responseDone(event.response?.id);
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
    if (text.length === 0) return [];
    return [{type: effectType, [store === this.users ? 'item_id' : 'response_id']: id, text}];
  }

  transcriptDone(store, id, text, effectType) {
    if (!validID(id) || !validText(text)) return [{type: 'error', code: 'invalid-transcript'}];
    if (store.get(id)?.done) return [];
    insertBounded(store, id, {text, done: true});
    return [{type: effectType, [store === this.users ? 'item_id' : 'response_id']: id, text}];
  }

  assistantTranscriptPart(id, text) {
    if (!validID(id) || !validText(text)) return [{type: 'error', code: 'invalid-transcript'}];
    const current = this.assistants.get(id);
    if (current?.done) return [];
    if (current?.lastPart === text) return [];
    let combined = text;
    let delta = text;
    if (current) {
      if (text === current.text) delta = '';
      else if (text.startsWith(current.text)) delta = text.slice(current.text.length);
      else {
        combined = current.text + text;
        if (!validText(combined)) return [{type: 'error', code: 'invalid-transcript'}];
      }
    }
    insertBounded(this.assistants, id, {text: combined, done: false, lastPart: text});
    return delta.length === 0 ? [] : [{type: 'assistant-delta', response_id: id, text: delta}];
  }

  responseDone(id) {
    const effects = [];
    if (validID(id)) {
      const transcript = this.assistants.get(id);
      if (transcript && !transcript.done) {
        insertBounded(this.assistants, id, {text: transcript.text, done: true, lastPart: transcript.lastPart});
        effects.push({type: 'assistant-done', response_id: id, text: transcript.text});
      }
    }
    if (!this.userSpeaking) effects.push({type: 'status', value: 'listening'});
    return effects;
  }

  toolPolicyAccepted(event) {
    if (!event || Object.keys(event).sort().join(',') !== 'event_id,model,tools,type' || event.model !== 'deepseek-v4-flash') {
      return [{type: 'error', code: 'invalid-event'}];
    }
    try {
      const update = buildToolPolicyUpdate(event.tools, event.event_id);
      return [{type: 'tool-policy-accepted', event_id: update.event_id, policy: update.tools, model: event.model}];
    } catch {
      return [{type: 'error', code: 'invalid-event'}];
    }
  }

  handoffCard(event) {
    try {
      return [{type: 'handoff-card', event: normalizeHandoffEvent(event)}];
    } catch {
      return [{type: 'error', code: 'invalid-event'}];
    }
  }
}
import {encodeBytesBase64} from './audio-core.mjs';
import {buildToolPolicyUpdate, normalizeHandoffEvent} from './tool-state.mjs';
