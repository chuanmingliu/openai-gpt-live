import test from "node:test";
import assert from "node:assert/strict";
import { SessionController } from "./session-controller.mjs";

test("barge-in increments playback generation", () => {
  const session = new SessionController();
  session.start();
  const first = session.apply({ type: "audio", encoded: "AAA=" });
  const cleared = session.apply({ type: "clear-playback" });
  const second = session.apply({ type: "audio", encoded: "AAA=" });
  assert.equal(first[0].generation + 1, cleared[0].generation);
  assert.equal(second[0].generation, cleared[0].generation);
});

test("transcript history is bounded", () => {
  const session = new SessionController();
  for (let i = 0; i < 60; i += 1) {
    session.apply({ type: "user-done", text: `turn-${i}` });
  }
  assert.equal(session.turns.length, 50);
});

test("oversized audio fails closed", () => {
  const session = new SessionController();
  const effects = session.apply({ type: "audio", encoded: "a".repeat(64 * 1024 + 1) });
  assert.deepEqual(effects, [{ type: "error", code: "audio-overflow" }]);
});
