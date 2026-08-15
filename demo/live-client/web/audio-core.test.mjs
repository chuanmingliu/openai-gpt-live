import test from "node:test";
import assert from "node:assert/strict";
import { StreamingLinearResampler, PCM16Framer, floatToPCM16, decodePCM16Base64 } from "./audio-core.mjs";

test("48 kHz input becomes contiguous 24 kHz PCM16", () => {
  const resampler = new StreamingLinearResampler(48000, 24000);
  const left = resampler.push(Float32Array.from([0, 0.25, 0.5]));
  const right = resampler.push(Float32Array.from([0.75, 1, 0.5]));
  assert.deepEqual([...left, ...right], [0, 16384, 32767]);
});

test("framer emits only exact 960-byte frames and retains the suffix", () => {
  const framer = new PCM16Framer(960);
  assert.equal(framer.push(new Int16Array(479)).length, 0);
  const frames = framer.push(new Int16Array(481));
  assert.equal(frames.length, 2);
  assert.equal(frames[0].byteLength, 960);
  assert.equal(frames[1].byteLength, 960);
});

test("PCM conversion saturates values outside [-1, 1]", () => {
  assert.deepEqual([...floatToPCM16(Float32Array.from([-2, -1, 0, 1, 2]))], [-32768, -32768, 0, 32767, 32767]);
});

test("decodePCM16Base64 rejects odd payloads", () => {
  assert.throws(() => decodePCM16Base64(btoa("abc")));
});
