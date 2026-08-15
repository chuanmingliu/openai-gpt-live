const MAX_IDS = 64;
const MAX_ID_BYTES = 256;
const MAX_TEXT_BYTES = 64 * 1024;
const MAX_DRAFT_BYTES = 64 * 1024;

export class ProtocolState {
  constructor() {
    this.userDrafts = new Map();
    this.finalUsers = new Set();
    this.assistantDrafts = new Map();
    this.finalAssistants = new Set();
    this.status = "idle";
  }

  consume(event) {
    if (!event || typeof event.type !== "string") {
      return [{ type: "error", code: "invalid-event" }];
    }
    switch (event.type) {
      case "session.created":
        this.status = "listening";
        return [{ type: "status", value: "listening" }];
      case "session.updated":
        return [{ type: "status", value: this.status || "listening" }];
      case "input_audio_buffer.speech_started":
        this.status = "user-speaking";
        return [{ type: "clear-playback" }, { type: "status", value: "user-speaking" }];
      case "input_audio_buffer.speech_stopped":
        this.status = "assistant-thinking";
        return [{ type: "status", value: "assistant-thinking" }];
      case "response.created":
        this.status = "assistant-speaking";
        return [{ type: "status", value: "assistant-speaking" }];
      case "response.done":
        this.status = "listening";
        return [{ type: "status", value: "listening" }];
      case "conversation.item.input_audio_transcription.delta":
        return this.userDelta(event.item_id, event.delta);
      case "conversation.item.input_audio_transcription.completed":
        return this.userDone(event.item_id, event.transcript);
      case "response.output_audio_transcript.delta":
      case "response.output_text.delta":
        return this.assistantDelta(event.response_id || event.item_id, event.delta);
      case "response.output_audio_transcript.done":
      case "response.output_text.done":
        return this.assistantDone(event.response_id || event.item_id, event.transcript || event.text);
      case "response.output_audio.delta":
        return typeof event.delta === "string" ? [{ type: "audio", encoded: event.delta }] : [{ type: "error", code: "invalid-audio" }];
      case "error":
        return [{ type: "error", code: boundedCode(event.error && event.error.code) }];
      default:
        return [];
    }
  }

  userDelta(id, text) {
    if (!validID(id) || !validText(text)) {
      return [{ type: "error", code: "invalid-transcript" }];
    }
    if (this.finalUsers.has(id)) {
      return [];
    }
    const next = (this.userDrafts.get(id) || "") + text;
    if (next.length > MAX_DRAFT_BYTES) {
      return [{ type: "error", code: "invalid-transcript" }];
    }
    remember(this.userDrafts, id, next);
    return [{ type: "user-delta", text: next }];
  }

  userDone(id, text) {
    if (!validID(id) || !validText(text)) {
      return [{ type: "error", code: "invalid-transcript" }];
    }
    if (this.finalUsers.has(id)) {
      return [];
    }
    rememberSet(this.finalUsers, id);
    this.userDrafts.delete(id);
    return [{ type: "user-done", text }];
  }

  assistantDelta(id, text) {
    if (!validID(id) || !validText(text)) {
      return [{ type: "error", code: "invalid-transcript" }];
    }
    if (this.finalAssistants.has(id)) {
      return [];
    }
    const next = (this.assistantDrafts.get(id) || "") + text;
    if (next.length > MAX_DRAFT_BYTES) {
      return [{ type: "error", code: "invalid-transcript" }];
    }
    remember(this.assistantDrafts, id, next);
    return [{ type: "assistant-delta", text: next }];
  }

  assistantDone(id, text) {
    if (!validID(id) || !validText(text)) {
      return [{ type: "error", code: "invalid-transcript" }];
    }
    if (this.finalAssistants.has(id)) {
      return [];
    }
    rememberSet(this.finalAssistants, id);
    this.assistantDrafts.delete(id);
    return [{ type: "assistant-done", text }];
  }
}

function validID(value) {
  return typeof value === "string" && value.length > 0 && value.length <= MAX_ID_BYTES;
}

function validText(value) {
  return typeof value === "string" && value.length > 0 && value.length <= MAX_TEXT_BYTES;
}

function remember(map, key, value) {
  map.set(key, value);
  while (map.size > MAX_IDS) {
    const first = map.keys().next().value;
    map.delete(first);
  }
}

function rememberSet(set, key) {
  set.add(key);
  while (set.size > MAX_IDS) {
    const first = set.values().next().value;
    set.delete(first);
  }
}

function boundedCode(value) {
  if (typeof value !== "string" || value.length === 0 || value.length > 64) {
    return "protocol-error";
  }
  return value;
}
