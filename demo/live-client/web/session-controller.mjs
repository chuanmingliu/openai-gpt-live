const MAX_TURNS = 50;
const MAX_HANDOFFS = 16;
const MAX_AUDIO_B64 = 64 * 1024;

export class SessionController {
  constructor() {
    this.generation = 1;
    this.turns = [];
    this.handoffs = [];
    this.userOpen = null;
    this.assistantOpen = null;
    this.status = "idle";
    this.muted = false;
  }

  start() {
    this.turns = [];
    this.handoffs = [];
    this.userOpen = null;
    this.assistantOpen = null;
    this.status = "connecting";
    this.generation += 1;
    return [{ type: "status", value: "connecting" }, { type: "clear-transcript" }, { type: "clear-handoffs" }, { type: "clear-playback", generation: this.generation }];
  }

  apply(effect) {
    if (!effect) {
      return [];
    }
    switch (effect.type) {
      case "status":
        this.status = effect.value;
        return [effect];
      case "clear-playback":
        this.generation += 1;
        return [{ type: "clear-playback", generation: this.generation }, { type: "status", value: effect.value || this.status }];
      case "audio":
        if (typeof effect.encoded !== "string" || effect.encoded.length === 0) {
          return [{ type: "error", code: "invalid-audio" }];
        }
        if (effect.encoded.length > MAX_AUDIO_B64) {
          return [{ type: "error", code: "audio-overflow" }];
        }
        return [{ type: "audio", encoded: effect.encoded, generation: this.generation }];
      case "user-delta":
      case "user-done":
        return this.record("user", effect.text, effect.type === "user-done");
      case "assistant-delta":
      case "assistant-done":
        return this.record("assistant", effect.text, effect.type.endsWith("done"));
      case "handoff":
        return this.recordHandoff(effect);
      case "error":
        return [effect];
      default:
        return [];
    }
  }

  record(role, text, done) {
    const openKey = role === "user" ? "userOpen" : "assistantOpen";
    let turn = this[openKey];
    if (!turn) {
      turn = { role, text };
      this.turns.push(turn);
      this[openKey] = turn;
      while (this.turns.length > MAX_TURNS) {
        this.turns.shift();
      }
    }
    turn.text = text;
    const out = [{ type: "render-turn", role, text, done: false }];
    if (done) {
      this[openKey] = null;
      out.push({ type: "announce", role, text });
    }
    return out;
  }

  recordHandoff(effect) {
    if (!effect.id || typeof effect.id !== "string") {
      return [];
    }
    let item = this.handoffs.find((handoff) => handoff.id === effect.id);
    if (!item) {
      item = { id: effect.id, status: effect.status, summary: effect.summary || "" };
      this.handoffs.push(item);
      while (this.handoffs.length > MAX_HANDOFFS) {
        this.handoffs.shift();
      }
    } else {
      item.status = effect.status;
      if (effect.summary) {
        item.summary = effect.summary;
      }
    }
    return [{ type: "render-handoff", id: item.id, status: item.status, summary: item.summary }];
  }

  setMuted(next) {
    this.muted = Boolean(next);
    return [{ type: "mute", value: this.muted }];
  }

  stop() {
    this.status = "idle";
    this.userOpen = null;
    this.assistantOpen = null;
    this.generation += 1;
    return [{ type: "status", value: "idle" }, { type: "clear-playback", generation: this.generation }];
  }
}
