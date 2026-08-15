import test from "node:test";
import assert from "node:assert/strict";
import { ProtocolState } from "./protocol-state.mjs";

test("speech start clears playback before reporting user speech", () => {
  const effects = new ProtocolState().consume({ type: "input_audio_buffer.speech_started" });
  assert.deepEqual(effects.map((effect) => effect.type), ["clear-playback", "status"]);
  assert.equal(effects[1].value, "user-speaking");
});

test("transcript deltas finalize once despite duplicate done", () => {
  const state = new ProtocolState();
  state.consume({ type: "conversation.item.input_audio_transcription.delta", item_id: "msg_1", delta: "hel" });
  state.consume({ type: "conversation.item.input_audio_transcription.completed", item_id: "msg_1", transcript: "hello" });
  const duplicate = state.consume({ type: "conversation.item.input_audio_transcription.completed", item_id: "msg_1", transcript: "hello" });
  assert.deepEqual(duplicate, []);
});

test("accumulated transcript drafts fail closed", () => {
  const state = new ProtocolState();
  const chunk = "x".repeat(32 * 1024);
  state.consume({ type: "response.output_text.delta", response_id: "r1", delta: chunk });
  const overflow = state.consume({ type: "response.output_text.delta", response_id: "r1", delta: chunk + "y" });
  assert.deepEqual(overflow, [{ type: "error", code: "invalid-transcript" }]);
});

test("audio and assistant transcript events are classified without retaining raw envelopes", () => {
  const state = new ProtocolState();
  assert.deepEqual(state.consume({ type: "response.output_audio.delta", delta: "AAA=" }), [{ type: "audio", encoded: "AAA=" }]);
  assert.deepEqual(state.consume({ type: "response.output_audio_transcript.done", response_id: "r1", transcript: "hi" }), [{ type: "assistant-done", text: "hi" }]);
});
