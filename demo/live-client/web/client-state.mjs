const ROLES = new Set(['user', 'assistant']);

function transcriptRole(type) {
  if (type === 'user-delta' || type === 'user-done') return 'user';
  if (type === 'assistant-delta' || type === 'assistant-done') return 'assistant';
  return null;
}

export class ClientPresentation {
  constructor({maximumTurns = 100} = {}) {
    if (!Number.isInteger(maximumTurns) || maximumTurns < 1 || maximumTurns > 500) {
      throw new RangeError('invalid transcript bound');
    }
    this.maximumTurns = maximumTurns;
    this.turns = [];
    this.announcements = [];
    this.playbackGeneration = 0;
    this.beginSession();
  }

  beginSession() {
    this.status = 'connecting';
    this.muted = false;
    this.active = {user: null, assistant: null};
    this.playbackGeneration += 1;
    this.announcement = '';
    this.announcements = [];
  }

  endSession(status = 'stopped') {
    for (const role of ROLES) this.finishActive(role);
    this.status = status;
    this.playbackGeneration += 1;
  }

  setMuted(value) {
    this.muted = Boolean(value);
  }

  isMuted() {
    return this.muted;
  }

  apply(effect) {
    if (!effect || typeof effect !== 'object') return;
    const role = transcriptRole(effect.type);
    if (role) {
      if (typeof effect.text !== 'string') return;
      if (effect.type.endsWith('-delta')) this.appendTurn(role, effect.text);
      else this.completeTurn(role, effect.text);
      return;
    }

    switch (effect.type) {
      case 'status':
        if (typeof effect.value === 'string' && !(effect.value === 'listening' && this.playbackIsActive())) {
          this.status = effect.value;
        }
        break;
      case 'audio-buffered':
        this.status = 'assistant-buffering';
        break;
      case 'audio-playing':
        if (this.status !== 'user-speaking' && this.status !== 'error' && this.status !== 'stopped') {
          this.status = 'assistant-speaking';
        }
        break;
      case 'audio-idle':
        if (this.status === 'assistant-buffering' || this.status === 'assistant-speaking') this.status = 'listening';
        break;
      case 'clear-playback':
        this.playbackGeneration += 1;
        break;
      case 'error':
        this.status = 'error';
        break;
    }
  }

  snapshot() {
    return {
      status: this.status,
      muted: this.muted,
      turns: this.turns.map(({role, text, final}) => ({role, text, final})),
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

  appendTurn(role, text) {
    const current = this.active[role];
    if (current) {
      current.text += text;
      return;
    }
    const turn = {role, text, final: false};
    this.turns.push(turn);
    this.active[role] = turn;
  }

  completeTurn(role, text) {
    const current = this.active[role];
    if (current) {
      current.text = text;
      this.finishActive(role);
      return;
    }
    const turn = {role, text, final: false};
    this.turns.push(turn);
    this.active[role] = turn;
    this.finishActive(role);
  }

  finishActive(role) {
    const current = this.active[role];
    if (!current) return;
    current.final = true;
    this.active[role] = null;
    this.announce(current);
    this.trimCompletedTurns();
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
      const index = this.turns.findIndex((turn) => turn.final);
      this.turns.splice(index, 1);
    }
  }
}
