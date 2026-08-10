import test from 'node:test';
import assert from 'node:assert/strict';

import {
  PCM16Framer,
  PCMPlaybackQueue,
  StreamingLinearResampler,
  decodePCM16Base64,
  encodeBytesBase64,
  floatToPCM16,
} from './audio-core.mjs';

test('48 kHz input remains contiguous when chunked into 24 kHz PCM16', () => {
  const resampler = new StreamingLinearResampler(48000, 24000);
  const left = resampler.push(Float32Array.from([0, 0.25, 0.5]));
  const right = resampler.push(Float32Array.from([0.75, 1, 0.5]));
  assert.deepEqual([...left, ...right], [0, 16384, 32767]);
});

test('framer emits only exact little-endian 960-byte frames and retains the suffix', () => {
  const framer = new PCM16Framer(960);
  assert.equal(framer.push(new Int16Array(479).fill(1)).length, 0);
  const frames = framer.push(new Int16Array(481).fill(2));
  assert.equal(frames.length, 2);
  assert.equal(frames[0].byteLength, 960);
  assert.equal(frames[1].byteLength, 960);
  assert.equal(frames[0][0], 1);
  assert.equal(frames[0][1], 0);
  assert.equal(frames[0][958], 2);
  assert.equal(frames[0][959], 0);
  assert.equal(framer.pendingBytes, 0);
});

test('PCM conversion saturates values outside the normalized range', () => {
  assert.deepEqual(
    [...floatToPCM16(Float32Array.from([-2, -1, 0, 1, 2]))],
    [-32768, -32768, 0, 32767, 32767],
  );
});

test('base64 audio decoding is exact and rejects malformed, odd, or oversized input', () => {
  assert.deepEqual([...decodePCM16Base64('AQD//w==')], [1, -1]);
  assert.throws(() => decodePCM16Base64('%%%'), /invalid audio/);
  assert.throws(() => decodePCM16Base64('AQ=='), /invalid audio/);
  assert.throws(() => decodePCM16Base64(encodeBytesBase64(new Uint8Array(65 << 10))), /audio is too large/);
});

test('byte base64 encoding is lossless across chunk boundaries', () => {
  const bytes = Uint8Array.from({length: 70000}, (_, index) => index % 251);
  const decoded = Uint8Array.from(Buffer.from(encodeBytesBase64(bytes), 'base64'));
  assert.deepEqual(decoded, bytes);
});

test('playback queue resamples continuously and clears queued speech immediately', () => {
  const queue = new PCMPlaybackQueue(48000, 1);
  assert.equal(queue.append(Int16Array.from([0, 32767, 0])), true);
  const output = queue.pull(4);
  assert.equal(output[0], 0);
  assert.ok(Math.abs(output[1] - 0.5) < 0.001);
  assert.ok(Math.abs(output[2] - 1) < 0.001);
  assert.ok(Math.abs(output[3] - 0.5) < 0.001);
  queue.clear();
  assert.deepEqual([...queue.pull(8)], [...new Float32Array(8)]);
});

test('playback queue refuses audio beyond its fixed ten-second-style bound', () => {
  const queue = new PCMPlaybackQueue(24000, 0.01);
  assert.equal(queue.append(new Int16Array(240)), true);
  assert.equal(queue.append(new Int16Array(1)), false);
  assert.equal(queue.queuedSourceSamples, 240);
});
