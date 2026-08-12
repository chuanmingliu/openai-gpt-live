const ROLES = new Set(['user', 'assistant']);
const DEFAULT_MAXIMUM_ACTIVE_TURNS = 64;
const DEFAULT_MAXIMUM_TURN_BYTES = 64 << 10;
const DEFAULT_MAXIMUM_TRANSCRIPT_BYTES = 256 << 10;
const textEncoder = new TextEncoder();
import {ToolState} from './tool-state.mjs';

function transcriptRole(type) {
  if (type === 'user-delta' || type === 'user-done') return 'user';
  if (type === 'assistant-delta' || type === 'assistant-done') return 'assistant';
  return null;
}

function effectID(effect, role) {
  const value = role === 'user' ? effect.item_id : effect.response_id;
  return typeof value === 'string' && value.length > 0 ? value : null;
}

function byteLength(text) {
  return textEncoder.encode(text).byteLength;
}

export class ClientPresentation {
  constructor({
    maximumTurns = 100,
    maximumActiveTurns = DEFAULT_MAXIMUM_ACTIVE_TURNS,
    maximumTurnBytes = DEFAULT_MAXIMUM_TURN_BYTES,
    maximumTranscriptBytes = DEFAULT_MAXIMUM_TRANSCRIPT_BYTES,
  } = {}) {
    if (!Number.isInteger(maximumTurns) || maximumTurns < 1 || maximumTurns > 500) {
      throw new RangeError('invalid transcript bound');
    }
    if (!Number.isInteger(maximumActiveTurns) || maximumActiveTurns < 1 || maximumActiveTurns > 64) {
      throw new RangeError('invalid active transcript bound');
    }
    if (!Number.isInteger(maximumTurnBytes) || maximumTurnBytes < 1 || maximumTurnBytes > DEFAULT_MAXIMUM_TURN_BYTES) {
      throw new RangeError('invalid per-turn transcript bound');
    }
    if (!Number.isInteger(maximumTranscriptBytes) || maximumTranscriptBytes < maximumTurnBytes || maximumTranscriptBytes > (4 << 20)) {
      throw new RangeError('invalid aggregate transcript bound');
    }
    this.maximumTurns = maximumTurns;
    this.maximumActiveTurns = maximumActiveTurns;
    this.maximumTurnBytes = maximumTurnBytes;
    this.maximumTranscriptBytes = maximumTranscriptBytes;
    this.turns = [];
    this.active = new Map();
    this.tools = new ToolState();
    this.turnOrder = new Map();
    this.toolOrder = new Map();
    this.nextOrder = 1;
    this.totalTranscriptBytes = 0;
    this.announcements = [];
    this.playbackGeneration = 0;
    this.beginSession();
  }

  beginSession() {
    this.status = 'connecting';
    this.audioSuspended = false;
    this.muted = false;
    this.active = new Map();
    this.playbackGeneration += 1;
    this.announcement = '';
    this.announcements = [];
  }

  endSession(status = 'stopped') {
    for (const turn of [...this.active.values()]) this.finishActive(turn);
    this.status = status;
    this.audioSuspended = false;
    this.playbackGeneration += 1;
  }

  setMuted(value) {
    this.muted = Boolean(value);
  }

  isMuted() {
    return this.muted;
  }

  apply(effect) {
    if (!effect || typeof effect !== 'object') return true;
    const role = transcriptRole(effect.type);
    if (role) {
      const id = effectID(effect, role);
      if (!id || typeof effect.text !== 'string') return false;
      if (effect.type.endsWith('-delta')) return this.appendTurn(role, id, effect.text);
      return this.completeTurn(role, id, effect.text);
    }

    switch (effect.type) {
      case 'tool-policy-accepted':
        this.tools.acceptPolicy(effect.event_id, effect.policy, effect.model);
        break;
      case 'handoff-card': {
        const result = this.tools.consume(effect.event);
        if (result.created && !this.toolOrder.has(effect.event.handoff_id)) {
          this.toolOrder.set(effect.event.handoff_id, this.nextOrder++);
        }
        this.trimToolOrder();
        break;
      }
      case 'status':
        if (typeof effect.value === 'string' && !(effect.value === 'listening' && this.playbackIsActive())) {
          this.status = effect.value;
        }
        break;
      case 'audio-buffered':
        if (this.status !== 'assistant-speaking') this.status = 'assistant-buffering';
        break;
      case 'audio-playing':
        if (this.status !== 'user-speaking' && this.status !== 'error' && this.status !== 'stopped') {
          this.status = 'assistant-speaking';
        }
        break;
      case 'audio-idle':
        if (this.status === 'assistant-buffering' || this.status === 'assistant-speaking') this.status = 'listening';
        break;
      case 'audio-suspended':
        this.audioSuspended = true;
        break;
      case 'audio-resumed':
        this.audioSuspended = false;
        break;
      case 'clear-playback':
        this.playbackGeneration += 1;
        break;
      case 'error':
        this.status = 'error';
        break;
    }
    return true;
  }

  snapshot() {
    const turns = this.turns.map(({id, role, text, final}) => ({id, role, text, final}));
    const tools = this.tools.snapshot();
    const timeline = [
      ...turns.map((turn) => ({kind: 'turn', order: this.turnOrder.get(`${turn.role}:${turn.id}`), ...turn})),
      ...tools.cards.map((card) => ({kind: 'tool', order: this.toolOrder.get(card.id), ...card})),
    ].filter(({order}) => Number.isSafeInteger(order)).sort((left, right) => left.order - right.order)
      .map(({order: _order, ...item}) => item);
    return {
      status: this.audioSuspended ? 'audio-suspended' : this.status,
      muted: this.muted,
      turns,
      timeline,
      tools,
      announcement: this.announcement,
      playbackGeneration: this.playbackGeneration,
    };
  }

  drainAnnouncements() {
    const announcements = this.announcements;
    this.announcements = [];
    this.announcement = '';
    return announcements;
  }

  drainToolAnnouncements() {
    return this.tools.drainAnnouncements();
  }

  appendTurn(role, id, text) {
    const key = `${role}:${id}`;
    const current = this.active.get(key);
    if (current) {
      const combined = current.text + text;
      const combinedBytes = byteLength(combined);
      if (combinedBytes > this.maximumTurnBytes) return false;
      this.totalTranscriptBytes += combinedBytes - current.bytes;
      current.text = combined;
      current.bytes = combinedBytes;
      this.trimTranscriptBytes();
      return true;
    }
    return this.createTurn(role, id, text, false);
  }

  completeTurn(role, id, text) {
    const key = `${role}:${id}`;
    const current = this.active.get(key);
    if (!current) return this.createTurn(role, id, text, true);
    const completedBytes = byteLength(text);
    if (completedBytes > this.maximumTurnBytes) return false;
    this.totalTranscriptBytes += completedBytes - current.bytes;
    current.text = text;
    current.bytes = completedBytes;
    this.finishActive(current);
    return true;
  }

  createTurn(role, id, text, final) {
    const bytes = byteLength(text);
    if (bytes > this.maximumTurnBytes) return false;
    while (!final && this.active.size >= this.maximumActiveTurns) {
      this.removeTurn(this.active.values().next().value);
    }
    const turn = {id, role, text, final: false, bytes};
    this.turns.push(turn);
    this.turnOrder.set(`${role}:${id}`, this.nextOrder++);
    this.totalTranscriptBytes += bytes;
    this.active.set(`${role}:${id}`, turn);
    if (final) this.finishActive(turn);
    else this.trimTranscriptBytes();
    return true;
  }

  finishActive(turn) {
    if (!turn || turn.final) return;
    turn.final = true;
    this.active.delete(`${turn.role}:${turn.id}`);
    this.announce(turn);
    this.trimCompletedTurns();
    this.trimTranscriptBytes();
  }

  removeTurn(turn) {
    if (!turn) return;
    const index = this.turns.indexOf(turn);
    if (index >= 0) this.turns.splice(index, 1);
    this.active.delete(`${turn.role}:${turn.id}`);
    this.turnOrder.delete(`${turn.role}:${turn.id}`);
    this.totalTranscriptBytes -= turn.bytes;
  }

  trimToolOrder() {
    const retained = new Set(this.tools.snapshot().cards.map(({id}) => id));
    for (const id of this.toolOrder.keys()) {
      if (!retained.has(id)) this.toolOrder.delete(id);
    }
  }

  announce(turn) {
    const announcement = `${turn.role === 'user' ? 'You' : 'Assistant'}: ${turn.text}`;
    this.announcement = announcement;
    this.announcements.push(announcement);
    if (this.announcements.length > this.maximumTurns) this.announcements.shift();
  }

  playbackIsActive() {
    return this.status === 'assistant-buffering' || this.status === 'assistant-speaking';
  }

  trimCompletedTurns() {
    while (this.turns.filter((turn) => turn.final).length > this.maximumTurns) {
      this.removeTurn(this.turns.find((turn) => turn.final));
    }
  }

  trimTranscriptBytes() {
    while (this.totalTranscriptBytes > this.maximumTranscriptBytes) {
      this.removeTurn(this.turns.find((turn) => turn.final) ?? this.turns[0]);
    }
  }
}
