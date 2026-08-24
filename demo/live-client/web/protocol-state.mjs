const MAX_STATUS = 50;
const MAX_TURNS = 100;
const MAX_DRAFTS = 100;
const MAX_FINALIZED = 200;
const MAX_TEXT_LENGTH = 4_000;
const MAX_IDENTIFIER_LENGTH = 160;
const KNOWN_ERROR_CODES = new Set([
  'authentication_error',
  'connection_error',
  'internal_error',
  'invalid_request',
  'invalid_request_error',
  'provider_unavailable',
  'rate_limit_exceeded',
  'server_error',
  'unsupported_event',
]);

export function initialProtocolState() {
  return {
    phase: 'idle',
    connected: false,
    muted: false,
    turns: [],
    drafts: {},
    finalized: {},
    status: [],
    effect: null,
  };
}

export function reduceProtocolEvent(state, event) {
  const type = typeof event?.type === 'string' ? event.type : '';

  switch (type) {
    case 'session.created':
    case 'session.updated':
    case 'session.reconnect.updated':
      return withStatus({ ...state, connected: true, phase: 'ready', effect: null }, type.replaceAll('.', '-'));

    case 'input_audio_buffer.speech_started':
      return withStatus({ ...state, phase: 'user-speaking', effect: 'clear-playback' }, 'user-speaking');

    case 'input_audio_buffer.speech_stopped':
      return withStatus({ ...state, phase: 'waiting', effect: null }, 'user-finished-speaking');

    case 'input_audio_buffer.committed':
      return withStatus({ ...state, phase: 'waiting', effect: null }, 'audio-committed');

    case 'conversation.item.input_audio_transcription.delta':
      return appendDraft(state, userKey(event), 'user', event.delta);

    case 'conversation.item.input_audio_transcription.completed':
      return finalizeDraft(state, userKey(event), 'user', event.transcript);

    case 'response.created':
      return withStatus({ ...state, phase: 'assistant-thinking', effect: null }, 'response-created');

    case 'response.output_audio.delta':
      return withStatus({ ...state, phase: 'assistant-speaking', effect: null }, 'assistant-audio');

    case 'response.output_audio.done':
      return withStatus({ ...state, effect: null }, 'assistant-audio-done');

    case 'response.output_audio_transcript.delta':
      return appendDraft(state, assistantKey(event), 'assistant', event.delta);

    case 'response.output_audio_transcript.done':
      return finalizeDraft(state, assistantKey(event), 'assistant', event.transcript);

    case 'response.done':
      return withStatus({ ...state, phase: state.connected ? 'ready' : 'idle', effect: null }, 'response-done');

    case 'error':
      return withStatus({ ...state, phase: 'error', effect: null }, 'protocol-error', sanitizedErrorCode(event?.error?.code));

    default:
      return withStatus({ ...state, effect: null }, 'unknown-event');
  }
}

function userKey(event) {
  return draftKey('user', event?.item_id, event?.content_index);
}

function assistantKey(event) {
  const responseID = validIdentifier(event?.response_id);
  const itemID = validIdentifier(event?.item_id);
  if (!responseID || !itemID || !validContentIndex(event?.content_index)) {
    return null;
  }
  return `assistant:${responseID}:${itemID}:${event.content_index}`;
}

function draftKey(prefix, itemID, contentIndex) {
  const safeItemID = validIdentifier(itemID);
  if (!safeItemID || !validContentIndex(contentIndex)) {
    return null;
  }
  return `${prefix}:${safeItemID}:${contentIndex}`;
}

function validIdentifier(value) {
  return typeof value === 'string' && value.length > 0 && value.length <= MAX_IDENTIFIER_LENGTH ? value : null;
}

function validContentIndex(value) {
  return Number.isInteger(value) && value >= 0 && value <= 10_000;
}

function appendDraft(state, key, speaker, delta) {
  if (!key || state.finalized[key] || typeof delta !== 'string' || delta.length === 0) {
    return state;
  }

  const prior = state.drafts[key];
  const draft = `${typeof prior?.text === 'string' ? prior.text : ''}${delta}`.slice(0, MAX_TEXT_LENGTH);
  const drafts = boundedRecord(state.drafts, key, { speaker, text: draft }, MAX_DRAFTS);
  return { ...state, drafts, effect: null };
}

function finalizeDraft(state, key, speaker, transcript) {
  if (!key || state.finalized[key]) {
    return state;
  }

  const draft = state.drafts[key];
  const text = typeof transcript === 'string'
    ? transcript.slice(0, MAX_TEXT_LENGTH)
    : typeof draft?.text === 'string'
      ? draft.text
      : '';
  const turns = boundedAppend(state.turns, { speaker, text }, MAX_TURNS);
  const finalized = boundedRecord(state.finalized, key, true, MAX_FINALIZED);
  const drafts = { ...state.drafts };
  delete drafts[key];
  return { ...state, turns, drafts, finalized, effect: null };
}

function withStatus(state, kind, code) {
  const entry = code ? { kind, code } : { kind };
  return { ...state, status: boundedAppend(state.status, entry, MAX_STATUS) };
}

function boundedAppend(entries, entry, max) {
  return [...entries, entry].slice(-max);
}

function boundedRecord(record, key, value, max) {
  const next = { ...record };
  if (!Object.hasOwn(next, key) && Object.keys(next).length >= max) {
    delete next[Object.keys(next)[0]];
  }
  next[key] = value;
  return next;
}

function sanitizedErrorCode(value) {
  if (typeof value !== 'string' || !KNOWN_ERROR_CODES.has(value)) {
    return 'unknown';
  }
  return value;
}
