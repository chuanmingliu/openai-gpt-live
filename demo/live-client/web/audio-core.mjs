const MAX_AUDIO_BYTES = 64 << 10;
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

export class PCMPlaybackQueue {
  constructor(outputRate, maximumSeconds = 10) {
    if (!Number.isFinite(outputRate) || outputRate < 8000 || outputRate > 192000) throw new RangeError('unsupported sample rate');
    if (!Number.isFinite(maximumSeconds) || maximumSeconds <= 0 || maximumSeconds > 10) throw new RangeError('invalid playback bound');
    this.sourceStep = 24000 / outputRate;
    this.maximumSourceSamples = Math.floor(24000 * maximumSeconds);
    this.segments = [];
    this.headOffset = 0;
    this.sourcePosition = 0;
    this.queuedSourceSamples = 0;
  }

  append(samples) {
    if (!(samples instanceof Int16Array)) throw new TypeError('samples must be Int16Array');
    if (samples.length === 0) return true;
    if (this.queuedSourceSamples + samples.length > this.maximumSourceSamples) return false;
    this.segments.push(samples.slice());
    this.queuedSourceSamples += samples.length;
    return true;
  }

  pull(frameCount) {
    if (!Number.isInteger(frameCount) || frameCount < 0 || frameCount > 16384) throw new RangeError('invalid playback frame count');
    const output = new Float32Array(frameCount);
    for (let index = 0; index < output.length; index += 1) {
      const consumed = Math.floor(this.sourcePosition);
      if (consumed > 0) {
        this.consume(consumed);
        this.sourcePosition -= consumed;
      }
      if (this.queuedSourceSamples < 2) break;
      const first = this.peek(0) / 32768;
      const second = this.peek(1) / 32768;
      output[index] = first + ((second - first) * this.sourcePosition);
      this.sourcePosition += this.sourceStep;
    }
    return output;
  }

  clear() {
    this.segments = [];
    this.headOffset = 0;
    this.sourcePosition = 0;
    this.queuedSourceSamples = 0;
  }

  peek(offset) {
    let remaining = this.headOffset + offset;
    for (const segment of this.segments) {
      if (remaining < segment.length) return segment[remaining];
      remaining -= segment.length;
    }
    return 0;
  }

  consume(count) {
    let remaining = Math.min(count, this.queuedSourceSamples);
    this.queuedSourceSamples -= remaining;
    while (remaining > 0 && this.segments.length > 0) {
      const available = this.segments[0].length - this.headOffset;
      if (remaining < available) {
        this.headOffset += remaining;
        return;
      }
      remaining -= available;
      this.segments.shift();
      this.headOffset = 0;
    }
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
