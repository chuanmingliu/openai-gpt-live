const MAX_TURNS = 50;
const MAX_AUDIO_B64 = 64 * 1024;

export class SessionController {
  constructor() {
    this.generation = 1;
    this.turns = [];
    this.userOpen = null;
    this.assistantOpen = null;
    this.status = "idle";
    this.muted = false;
  }

  start() {
    this.turns = [];
    this.userOpen = null;
    this.assistantOpen = null;
    this.status = "connecting";
    this.generation += 1;
    return [{ type: "status", value: "connecting" }, { type: "clear-transcript" }, { type: "clear-playback", generation: this.generation }];
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
