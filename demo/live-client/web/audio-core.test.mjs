import test from 'node:test';
import assert from 'node:assert/strict';

import * as audioCore from './audio-core.mjs';
import {
  PCM16Framer,
  PCMPlaybackQueue,
  StreamingLinearResampler,
  decodePCM16Base64,
  encodeBytesBase64,
  floatToPCM16,
} from './audio-core.mjs';

test('playback reservoir preserves FIFO audio across partial reads', () => {
  assert.equal(typeof audioCore.PCMChunkBuffer, 'function');
  const reservoir = new audioCore.PCMChunkBuffer(0.01);
  const first = Int16Array.from([1, 2, 3, 4]);
  assert.equal(reservoir.append(first), true);
  assert.equal(reservoir.append(Int16Array.from([5, 6])), true);
  first.fill(99);

  assert.deepEqual([...reservoir.take(3)], [1, 2, 3]);
  assert.deepEqual([...reservoir.take(3)], [4, 5, 6]);
  assert.equal(reservoir.queuedSamples, 0);
});

test('playback reservoir refuses audio beyond its fixed duration and clears immediately', () => {
  assert.equal(typeof audioCore.PCMChunkBuffer, 'function');
  const reservoir = new audioCore.PCMChunkBuffer(0.01);
  assert.equal(reservoir.append(new Int16Array(240)), true);
  assert.equal(reservoir.append(new Int16Array(1)), false);
  assert.equal(reservoir.queuedSamples, 240);
  reservoir.clear();
  assert.equal(reservoir.queuedSamples, 0);
  assert.equal(reservoir.take(10).length, 0);
});

test('playback pacer feeds only a short real-time window ahead', () => {
  assert.equal(typeof audioCore.PCMPlaybackPacer, 'function');
  const pacer = new audioCore.PCMPlaybackPacer();
  assert.equal(pacer.append(Int16Array.from({length: 12000}, (_, index) => index)), true);
  const delivered = [];

  assert.equal(pacer.refill(10, (samples) => delivered.push(...samples)), 0.15);
  assert.equal(delivered.length, 6000);
  assert.equal(pacer.queuedSamples, 6000);

  assert.equal(pacer.refill(10.15, (samples) => delivered.push(...samples)), 0.15);
  assert.equal(delivered.length, 9600);
  assert.equal(pacer.queuedSamples, 2400);

  assert.equal(pacer.refill(10.25, (samples) => delivered.push(...samples)), null);
  assert.deepEqual(delivered, Array.from({length: 12000}, (_, index) => index));
});

test('playback pacer clears buffered speech and resets its playback clock', () => {
  assert.equal(typeof audioCore.PCMPlaybackPacer, 'function');
  const pacer = new audioCore.PCMPlaybackPacer();
  pacer.append(new Int16Array(12000));
  pacer.refill(4, () => {});
  pacer.clear(20);
  assert.equal(pacer.queuedSamples, 0);
  assert.equal(pacer.bufferedUntil, 20);
  assert.equal(pacer.refill(20, () => {}), null);
});

test('playback feed timer keeps only one pending refill', () => {
  assert.equal(typeof audioCore.SinglePendingTimer, 'function');
  const scheduled = [];
  const cancelled = [];
  const timer = new audioCore.SinglePendingTimer(
    (callback, delay) => {
      scheduled.push({callback, delay});
      return scheduled.length;
    },
    (handle) => cancelled.push(handle),
  );
  const fired = [];

  assert.equal(timer.request(() => fired.push('first'), 150), true);
  assert.equal(timer.request(() => fired.push('duplicate'), 50), false);
  assert.equal(scheduled.length, 1);
  scheduled[0].callback();
  assert.deepEqual(fired, ['first']);

  assert.equal(timer.request(() => fired.push('second'), 25), true);
  timer.clear();
  assert.deepEqual(cancelled, [2]);
});

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
