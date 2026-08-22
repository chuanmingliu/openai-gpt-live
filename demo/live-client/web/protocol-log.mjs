const DEFAULT_MAXIMUM_ENTRIES = 200;
const MAXIMUM_ENTRIES = 1000;
const MAXIMUM_EVENT_TYPE_BYTES = 128;

const aggregateTypes = new Set([
  'input_audio_buffer.append',
  'response.output_audio.delta',
  'conversation.item.input_audio_transcription.delta',
  'response.output_audio_transcript.delta',
]);

const transcriptTypes = new Set([
  'conversation.item.input_audio_transcription.delta',
  'response.output_audio_transcript.delta',
]);

const responseStatuses = new Set(['completed', 'cancelled', 'failed', 'incomplete']);
const textEncoder = new TextEncoder();

function eventType(event) {
  const type = event?.type;
  if (typeof type !== 'string' || type.length === 0 || textEncoder.encode(type).length > MAXIMUM_EVENT_TYPE_BYTES) return 'unknown';
  return /^[a-z0-9_.-]+$/.test(type) ? type : 'unknown';
}

function payloadBytes(type, event) {
  if (type === 'input_audio_buffer.append') return typeof event?.audio === 'string' ? event.audio.length : 0;
  if (type === 'response.output_audio.delta') return typeof event?.delta === 'string' ? event.delta.length : 0;
  if (transcriptTypes.has(type)) return typeof event?.delta === 'string' ? textEncoder.encode(event.delta).length : 0;
  return 0;
}

export function protocolEventDetail(entry) {
  if (entry?.type === 'response.done') {
    return responseStatuses.has(entry.status) ? `status=${entry.status}` : '';
  }
  if (entry?.type === 'live.transport.closed') {
    return Number.isInteger(entry.code) && entry.code >= 1000 && entry.code <= 4999 ? `code=${entry.code}` : '';
  }
  if (!Number.isSafeInteger(entry?.bytes) || entry.bytes <= 0) return entry?.count > 1 ? `${entry.count} events` : '';
  const unit = transcriptTypes.has(entry.type) ? 'text bytes' : 'encoded chars';
  return entry.count > 1 ? `${entry.count} events · ${entry.bytes} ${unit}` : `${entry.bytes} ${unit}`;
}

export class ProtocolEventJournal {
  constructor({maximumEntries = DEFAULT_MAXIMUM_ENTRIES, now = () => Date.now()} = {}) {
    if (!Number.isSafeInteger(maximumEntries) || maximumEntries < 1 || maximumEntries > MAXIMUM_ENTRIES) {
      throw new RangeError('invalid protocol event bound');
    }
    if (typeof now !== 'function') throw new TypeError('protocol event clock is required');
    this.maximumEntries = maximumEntries;
    this.now = now;
    this.entries = [];
    this.nextSequence = 1;
  }

  record(direction, event) {
    if (direction !== 'inbound' && direction !== 'outbound') throw new TypeError('invalid protocol event direction');
    const type = eventType(event);
    const bytes = payloadBytes(type, event);
    const at = this.now();
    const previous = this.entries.at(-1);
    if (aggregateTypes.has(type) && previous?.direction === direction && previous.type === type) {
      previous.count += 1;
      previous.bytes += bytes;
      previous.at = at;
      return;
    }
    const entry = {sequence: this.nextSequence++, at, direction, type, count: 1, bytes};
    if (type === 'response.done' && responseStatuses.has(event?.response?.status)) entry.status = event.response.status;
    if (type === 'live.transport.closed' && Number.isInteger(event?.code) && event.code >= 1000 && event.code <= 4999) {
      entry.code = event.code;
    }
    this.entries.push(entry);
    if (this.entries.length > this.maximumEntries) this.entries.splice(0, this.entries.length - this.maximumEntries);
  }

  clear() {
    this.entries.length = 0;
    this.nextSequence = 1;
  }

  snapshot() {
    return this.entries.map((entry) => ({...entry}));
  }
}
