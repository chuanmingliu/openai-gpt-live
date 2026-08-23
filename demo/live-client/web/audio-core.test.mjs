import test from 'node:test';
import assert from 'node:assert/strict';
import { floatToPCM16, StreamingLinearResampler, PCM16Framer, decodePCM16Base64 } from './audio-core.mjs';

test('PCM conversion saturates outside minus one to one', () => {
  assert.deepEqual([...floatToPCM16(Float32Array.from([-2, -1, 0, 1, 2]))],
    [-32768, -32768, 0, 32767, 32767]);
});

test('streaming resampler preserves continuity across pushes', () => {
  const one = new StreamingLinearResampler(48000, 24000);
  const split = [...one.push(Float32Array.from([0, .25, .5])), ...one.push(Float32Array.from([.75, 1, .5]))];
  const two = new StreamingLinearResampler(48000, 24000);
  const joined = [...two.push(Float32Array.from([0, .25, .5, .75, 1, .5]))];
  assert.deepEqual(split, joined);
});

test('framer emits exact 960-byte frames and retains only suffix', () => {
  const framer = new PCM16Framer(960);
  assert.equal(framer.push(new Int16Array(479)).length, 0);
  const frames = framer.push(new Int16Array(481));
  assert.equal(frames.length, 2);
  assert.equal(frames[0].byteLength, 960);
  assert.equal(frames[1].byteLength, 960);
});

test('base64 decoder rejects malformed odd and oversized PCM', () => {
  assert.throws(() => decodePCM16Base64('%%%', 960));
  assert.throws(() => decodePCM16Base64('AA==', 960));
  assert.throws(() => decodePCM16Base64(Buffer.alloc(962).toString('base64'), 960));
});
