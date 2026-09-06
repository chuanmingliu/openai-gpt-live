import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';

const source = readFileSync(new URL('./playback-worklet.js', import.meta.url), 'utf8');

function playback(rate) {
  let Processor;
  vm.runInNewContext(source, {
    sampleRate: rate, ArrayBuffer, Int16Array, Uint8Array, DataView,
    AudioWorkletProcessor: class { port = { postMessage() {} }; },
    registerProcessor(_name, constructor) { Processor = constructor; },
  });
  const processor = new Processor();
  return {
    append(samples) {
      processor.port.onmessage({ data: { type: 'append', pcm: Int16Array.from(samples).buffer } });
    },
    clear() { processor.port.onmessage({ data: { type: 'clear' } }); },
    render(frames) {
      const output = new Float32Array(frames);
      processor.process([], [[output]]);
      return output;
    },
  };
}

for (const rate of [24000, 44100, 48000]) {
  test(`24 kHz PCM lasts 20 ms at ${rate} Hz output`, () => {
    const player = playback(rate);
    player.append(new Int16Array(480).fill(16384));
    const output = player.render(rate / 50);
    assert.ok(output.every(value => value === 0.5), '20 ms must contain audio throughout');
    assert.ok(player.render(128).every(value => value === 0), 'queue drains after 20 ms');
  });
}

test('48 kHz playback interpolates and preserves phase across render and append boundaries', () => {
  const player = playback(48000);
  player.append([0, 8192]);
  assert.deepEqual([...player.render(3)], [0, 0.125, 0.25]);
  player.append([16384, 24576]);
  assert.deepEqual([...player.render(5)], [0.375, 0.5, 0.625, 0.75, 0.75]);
  assert.deepEqual([...player.render(2)], [0, 0]);
});

test('clear resets fractional playback position and removes old audio', () => {
  const player = playback(48000);
  player.append([8192, 16384]);
  player.render(1);
  player.clear();
  assert.deepEqual([...player.render(4)], [0, 0, 0, 0]);
  player.append([24576, 8192]);
  assert.deepEqual([...player.render(2)], [0.75, 0.5]);
});

test('overflow retains at most five seconds of the newest provider audio', () => {
  const player = playback(48000);
  player.append(new Int16Array(24000 * 5).fill(8192));
  player.append(new Int16Array(480).fill(24576));
  player.render(48000 * 5 - 960);
  assert.ok(player.render(960).every(value => value === 0.75));
  assert.ok(player.render(128).every(value => value === 0));
});
