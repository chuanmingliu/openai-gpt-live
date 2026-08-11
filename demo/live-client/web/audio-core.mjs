const MAX_AUDIO_BYTES = 64 << 10;
const QUEUE_COMPACTION_THRESHOLD = 32;
const BASE64_PATTERN = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;

function sampleToPCM16(value) {
  const bounded = Math.max(-1, Math.min(1, Number.isFinite(value) ? value : 0));
  return bounded < 0 ? Math.round(bounded * 32768) : Math.round(bounded * 32767);
}

export function floatToPCM16(samples) {
  if (!(samples instanceof Float32Array)) throw new TypeError('samples must be Float32Array');
  const output = new Int16Array(samples.length);
  for (let index = 0; index < samples.length; index += 1) {
    output[index] = sampleToPCM16(samples[index]);
  }
  return output;
}

export class StreamingLinearResampler {
  constructor(inputRate, outputRate) {
    if (!Number.isFinite(inputRate) || !Number.isFinite(outputRate) || inputRate < 8000 || inputRate > 192000 || outputRate < 8000 || outputRate > 192000) {
      throw new RangeError('unsupported sample rate');
    }
    this.ratio = inputRate / outputRate;
    this.pending = new Float32Array(0);
    this.position = 0;
  }

  push(samples) {
    if (!(samples instanceof Float32Array)) throw new TypeError('samples must be Float32Array');
    if (samples.length === 0) return new Int16Array(0);
    const joined = new Float32Array(this.pending.length + samples.length);
    joined.set(this.pending);
    joined.set(samples, this.pending.length);

    const maximumOutput = Math.max(0, Math.ceil((joined.length - 1 - this.position) / this.ratio));
    const output = new Int16Array(maximumOutput);
    let count = 0;
    while (this.position + 1 < joined.length) {
      const lower = Math.floor(this.position);
      const fraction = this.position - lower;
      const sample = joined[lower] + ((joined[lower + 1] - joined[lower]) * fraction);
      output[count] = sampleToPCM16(sample);
      count += 1;
      this.position += this.ratio;
    }

    const consumed = Math.floor(this.position);
    this.pending = joined.slice(Math.min(consumed, joined.length));
    this.position -= consumed;
    return output.slice(0, count);
  }

  reset() {
    this.pending = new Float32Array(0);
    this.position = 0;
  }
}

export class StreamingPCM16Capture {
  constructor(inputRate, outputRate, frameBytes = 960) {
    if (!Number.isFinite(inputRate) || !Number.isFinite(outputRate) || inputRate < 8000 || inputRate > 192000 || outputRate < 8000 || outputRate > 192000) {
      throw new RangeError('unsupported sample rate');
    }
    if (!Number.isInteger(frameBytes) || frameBytes <= 0 || frameBytes > MAX_AUDIO_BYTES || frameBytes % 2 !== 0) {
      throw new RangeError('invalid PCM frame size');
    }
    this.inputRate = inputRate;
    this.outputRate = outputRate;
    this.frameBytes = frameBytes;
    this.frame = new Uint8Array(frameBytes);
    this.completedFrames = [];
    this.reset();
  }

  get stagedSamples() {
    return this.frameOffset / 2;
  }

  push(samples) {
    if (!(samples instanceof Float32Array)) throw new TypeError('samples must be Float32Array');
    this.completedFrames.length = 0;
    for (let index = 0; index < samples.length; index += 1) {
      const currentSample = samples[index];
      if (!this.hasPreviousSample) {
        this.previousSample = currentSample;
        this.hasPreviousSample = true;
        continue;
      }
      while (this.phase < this.outputRate) {
        const fraction = this.phase / this.outputRate;
        const sample = this.previousSample + ((currentSample - this.previousSample) * fraction);
        this.writeSample(sampleToPCM16(sample));
        this.phase += this.inputRate;
      }
      this.phase -= this.outputRate;
      this.previousSample = currentSample;
    }
    return this.completedFrames;
  }

  reset() {
    this.completedFrames.length = 0;
    this.frameOffset = 0;
    this.phase = 0;
    this.previousSample = 0;
    this.hasPreviousSample = false;
  }

  writeSample(sample) {
    this.frame[this.frameOffset] = sample & 0xff;
    this.frame[this.frameOffset + 1] = (sample >>> 8) & 0xff;
    this.frameOffset += 2;
    if (this.frameOffset !== this.frameBytes) return;
    this.completedFrames.push(this.frame);
    this.frame = new Uint8Array(this.frameBytes);
    this.frameOffset = 0;
  }
}

export class PCM16Framer {
  constructor(frameBytes = 960) {
    if (!Number.isInteger(frameBytes) || frameBytes <= 0 || frameBytes > MAX_AUDIO_BYTES || frameBytes % 2 !== 0) {
      throw new RangeError('invalid PCM frame size');
    }
    this.frameBytes = frameBytes;
    this.pending = new Uint8Array(0);
  }

  get pendingBytes() {
    return this.pending.byteLength;
  }

  push(samples) {
    if (!(samples instanceof Int16Array)) throw new TypeError('samples must be Int16Array');
    const incoming = new Uint8Array(samples.length * 2);
    const view = new DataView(incoming.buffer);
    for (let index = 0; index < samples.length; index += 1) {
      view.setInt16(index * 2, samples[index], true);
    }
    const joined = new Uint8Array(this.pending.length + incoming.length);
    joined.set(this.pending);
    joined.set(incoming, this.pending.length);
    const frames = [];
    let offset = 0;
    while (offset + this.frameBytes <= joined.length) {
      frames.push(joined.slice(offset, offset + this.frameBytes));
      offset += this.frameBytes;
    }
    this.pending = joined.slice(offset);
    return frames;
  }

  reset() {
    this.pending = new Uint8Array(0);
  }
}

export class PCMChunkBuffer {
  constructor(maximumSeconds = 120) {
    if (!Number.isFinite(maximumSeconds) || maximumSeconds <= 0 || maximumSeconds > 120) {
      throw new RangeError('invalid PCM buffer bound');
    }
    this.maximumSamples = Math.floor(24000 * maximumSeconds);
    this.segments = [];
    this.headIndex = 0;
    this.headOffset = 0;
    this.queuedSamples = 0;
  }

  append(samples) {
    if (!(samples instanceof Int16Array)) throw new TypeError('samples must be Int16Array');
    if (samples.length === 0) return true;
    if (this.queuedSamples + samples.length > this.maximumSamples) return false;
    this.segments.push(samples.slice());
    this.queuedSamples += samples.length;
    return true;
  }

  take(maximumSamples) {
    if (!Number.isInteger(maximumSamples) || maximumSamples <= 0 || maximumSamples > (64 << 10)) {
      throw new RangeError('invalid PCM take size');
    }
    const count = Math.min(maximumSamples, this.queuedSamples);
    const output = new Int16Array(count);
    let written = 0;
    while (written < count) {
      const segment = this.segments[this.headIndex];
      const available = segment.length - this.headOffset;
      const copied = Math.min(available, count - written);
      output.set(segment.subarray(this.headOffset, this.headOffset + copied), written);
      written += copied;
      this.headOffset += copied;
      if (this.headOffset === segment.length) {
        this.headIndex += 1;
        this.headOffset = 0;
      }
    }
    this.queuedSamples -= count;
    if (this.headIndex === this.segments.length) {
      this.segments.length = 0;
      this.headIndex = 0;
    } else if (this.headIndex >= QUEUE_COMPACTION_THRESHOLD && this.headIndex * 2 >= this.segments.length) {
      this.segments.copyWithin(0, this.headIndex);
      this.segments.length -= this.headIndex;
      this.headIndex = 0;
    }
    return output;
  }

  clear() {
    this.segments.length = 0;
    this.headIndex = 0;
    this.headOffset = 0;
    this.queuedSamples = 0;
  }
}

export class PCMPlaybackPacer {
  constructor(maximumSeconds = 120) {
    this.buffer = new PCMChunkBuffer(maximumSeconds);
    this.bufferedUntil = 0;
    this.targetSeconds = 0.25;
    this.lowWaterSeconds = 0.1;
  }

  get queuedSamples() {
    return this.buffer.queuedSamples;
  }

  append(samples) {
    return this.buffer.append(samples);
  }

  refill(currentTime, consume) {
    if (!Number.isFinite(currentTime) || currentTime < 0) throw new RangeError('invalid playback time');
    if (typeof consume !== 'function') throw new TypeError('consume must be a function');
    this.bufferedUntil = Math.max(this.bufferedUntil, currentTime);
    while (this.buffer.queuedSamples > 0) {
      const aheadSeconds = this.bufferedUntil - currentTime;
      if (aheadSeconds >= this.targetSeconds) break;
      const requiredSamples = Math.max(1, Math.round((this.targetSeconds - aheadSeconds) * 24000));
      const samples = this.buffer.take(Math.min(requiredSamples, 64 << 10));
      consume(samples);
      this.bufferedUntil += samples.length / 24000;
    }
    if (this.buffer.queuedSamples === 0) return null;
    const delaySeconds = Math.max(0.005, (this.bufferedUntil - currentTime) - this.lowWaterSeconds);
    return delaySeconds;
  }

  clear(currentTime = 0) {
    if (!Number.isFinite(currentTime) || currentTime < 0) throw new RangeError('invalid playback time');
    this.buffer.clear();
    this.bufferedUntil = currentTime;
  }
}

export class SinglePendingTimer {
  constructor(
    schedule = (callback, delay) => setTimeout(callback, delay),
    cancel = (handle) => clearTimeout(handle),
  ) {
    if (typeof schedule !== 'function' || typeof cancel !== 'function') throw new TypeError('timer functions are required');
    this.schedule = schedule;
    this.cancel = cancel;
    this.handle = null;
  }

  request(callback, delayMilliseconds) {
    if (typeof callback !== 'function') throw new TypeError('timer callback is required');
    if (!Number.isFinite(delayMilliseconds) || delayMilliseconds < 0 || delayMilliseconds > 120000) {
      throw new RangeError('invalid timer delay');
    }
    if (this.handle !== null) return false;
    this.handle = this.schedule(() => {
      this.handle = null;
      callback();
    }, delayMilliseconds);
    return true;
  }

  clear() {
    if (this.handle === null) return;
    this.cancel(this.handle);
    this.handle = null;
  }
}

export class PCMPlaybackQueue {
  constructor(outputRate, maximumSeconds = 10) {
    if (!Number.isFinite(outputRate) || outputRate < 8000 || outputRate > 192000) throw new RangeError('unsupported sample rate');
    if (!Number.isFinite(maximumSeconds) || maximumSeconds <= 0 || maximumSeconds > 10) throw new RangeError('invalid playback bound');
    this.sourceStep = 24000 / outputRate;
    this.maximumSourceSamples = Math.floor(24000 * maximumSeconds);
    this.segments = [];
    this.headIndex = 0;
    this.headOffset = 0;
    this.sourcePosition = 0;
    this.queuedSourceSamples = 0;
    this.generation = null;
  }

  append(samples, generation = this.generation ?? 0) {
    if (!(samples instanceof Int16Array)) throw new TypeError('samples must be Int16Array');
    if (!Number.isSafeInteger(generation) || generation < 0) throw new RangeError('invalid playback generation');
    if (this.generation === null) this.generation = generation;
    if (generation !== this.generation) return false;
    if (samples.length === 0) return true;
    if (this.queuedSourceSamples + samples.length > this.maximumSourceSamples) return false;
    this.segments.push(samples.slice());
    this.queuedSourceSamples += samples.length;
    return true;
  }

  pull(frameCount) {
    if (!Number.isInteger(frameCount) || frameCount < 0 || frameCount > 16384) throw new RangeError('invalid playback frame count');
    const output = new Float32Array(frameCount);
    this.pullInto(output);
    return output;
  }

  pullInto(output) {
    if (!(output instanceof Float32Array)) throw new TypeError('output must be Float32Array');
    if (output.length > 16384) throw new RangeError('invalid playback frame count');
    output.fill(0);
    let written = 0;
    while (written < output.length) {
      const owedSamples = Math.floor(this.sourcePosition);
      if (owedSamples > 0) {
        this.sourcePosition -= this.consume(owedSamples);
        if (this.sourcePosition >= 1) break;
      }
      if (this.queuedSourceSamples < 2) break;
      const first = this.peek(0) / 32768;
      const second = this.peek(1) / 32768;
      output[written] = first + ((second - first) * this.sourcePosition);
      written += 1;
      this.sourcePosition += this.sourceStep;
      const consumed = Math.floor(this.sourcePosition);
      if (consumed > 0) {
        this.sourcePosition -= this.consume(consumed);
      }
    }
    return written;
  }

  clear(generation = (this.generation ?? -1) + 1) {
    if (!Number.isSafeInteger(generation) || generation < 0) throw new RangeError('invalid playback generation');
    if (this.generation !== null && generation < this.generation) return;
    this.generation = generation;
    this.segments.length = 0;
    this.headIndex = 0;
    this.headOffset = 0;
    this.sourcePosition = 0;
    this.queuedSourceSamples = 0;
  }

  peek(offset) {
    let remaining = this.headOffset + offset;
    for (let index = this.headIndex; index < this.segments.length; index += 1) {
      const segment = this.segments[index];
      if (remaining < segment.length) return segment[remaining];
      remaining -= segment.length;
    }
    return 0;
  }

  consume(count) {
    const consumed = Math.min(count, this.queuedSourceSamples);
    let remaining = consumed;
    this.queuedSourceSamples -= remaining;
    while (remaining > 0 && this.headIndex < this.segments.length) {
      const available = this.segments[this.headIndex].length - this.headOffset;
      if (remaining < available) {
        this.headOffset += remaining;
        remaining = 0;
        break;
      }
      remaining -= available;
      this.headIndex += 1;
      this.headOffset = 0;
    }
    if (this.headIndex === this.segments.length) {
      this.segments.length = 0;
      this.headIndex = 0;
    } else if (this.headIndex >= QUEUE_COMPACTION_THRESHOLD && this.headIndex * 2 >= this.segments.length) {
      this.segments.copyWithin(0, this.headIndex);
      this.segments.length -= this.headIndex;
      this.headIndex = 0;
    }
    return consumed;
  }
}

export function decodePCM16Base64(encoded) {
  if (typeof encoded !== 'string' || encoded.length === 0 || encoded.length % 4 !== 0 || !BASE64_PATTERN.test(encoded)) {
    throw new TypeError('invalid audio');
  }
  let binary;
  try {
    binary = atob(encoded);
  } catch {
    throw new TypeError('invalid audio');
  }
  if (binary.length > MAX_AUDIO_BYTES) throw new RangeError('audio is too large');
  if (binary.length === 0 || binary.length % 2 !== 0) throw new TypeError('invalid audio');
  const samples = new Int16Array(binary.length / 2);
  for (let index = 0; index < samples.length; index += 1) {
    const low = binary.charCodeAt(index * 2);
    const high = binary.charCodeAt((index * 2) + 1);
    const unsigned = low | (high << 8);
    samples[index] = unsigned >= 0x8000 ? unsigned - 0x10000 : unsigned;
  }
  return samples;
}

export function encodeBytesBase64(bytes) {
  if (!(bytes instanceof Uint8Array)) throw new TypeError('bytes must be Uint8Array');
  let binary = '';
  const chunkBytes = 0x8000;
  for (let offset = 0; offset < bytes.length; offset += chunkBytes) {
    binary += String.fromCharCode(...bytes.subarray(offset, Math.min(offset + chunkBytes, bytes.length)));
  }
  return btoa(binary);
}
