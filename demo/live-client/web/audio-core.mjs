/**
 * Convert normalized audio samples to signed 16-bit PCM without mutating the
 * source array. Negative samples use the full negative range; positive samples
 * use the full positive range.
 */
export function floatToPCM16(samples) {
  if (!(samples instanceof Float32Array)) {
    throw new TypeError('samples must be a Float32Array');
  }

  const pcm = new Int16Array(samples.length);
  for (let index = 0; index < samples.length; index += 1) {
    const sample = Math.max(-1, Math.min(1, samples[index]));
    pcm[index] = sample < 0 ? Math.round(sample * 32768) : Math.round(sample * 32767);
  }
  return pcm;
}

/**
 * Resample chunks as one continuous input stream. It retains exactly one input
 * boundary sample, which is enough for the next linear interpolation.
 */
export class StreamingLinearResampler {
  constructor(inputRate, outputRate) {
    if (!Number.isFinite(inputRate) || inputRate <= 0 || !Number.isFinite(outputRate) || outputRate <= 0) {
      throw new RangeError('inputRate and outputRate must be positive finite numbers');
    }

    this.step = inputRate / outputRate;
    this.reset();
  }

  reset() {
    this.nextPosition = 0;
    this.totalInputSamples = 0;
    this.boundarySample = 0;
    this.hasBoundarySample = false;
  }

  push(samples) {
    if (!(samples instanceof Float32Array)) {
      throw new TypeError('samples must be a Float32Array');
    }
    if (samples.length === 0) {
      return new Float32Array(0);
    }

    const start = this.totalInputSamples;
    const end = start + samples.length;
    const output = [];

    while (this.nextPosition < end) {
      const lowerIndex = Math.floor(this.nextPosition);
      const fraction = this.nextPosition - lowerIndex;

      // An interpolated output needs the sample on either side of its source
      // position. Exact positions can use the final available sample directly.
      if (fraction !== 0 && lowerIndex + 1 >= end) {
        break;
      }

      let lower;
      if (lowerIndex === start - 1 && this.hasBoundarySample) {
        lower = this.boundarySample;
      } else if (lowerIndex >= start && lowerIndex < end) {
        lower = samples[lowerIndex - start];
      } else {
        break;
      }

      if (fraction === 0) {
        output.push(lower);
      } else {
        const upper = samples[lowerIndex + 1 - start];
        output.push(lower + ((upper - lower) * fraction));
      }
      this.nextPosition += this.step;
    }

    this.totalInputSamples = end;
    this.boundarySample = samples[samples.length - 1];
    this.hasBoundarySample = true;
    return Float32Array.from(output);
  }
}

/** Buffer PCM samples into exact-size, little-endian byte frames. */
export class PCM16Framer {
  constructor(frameBytes) {
    if (frameBytes !== 960) {
      throw new RangeError('frameBytes must be exactly 960');
    }

    this.frameBytes = frameBytes;
    this.reset();
  }

  reset() {
    this.suffix = new Uint8Array(0);
  }

  push(samples) {
    if (!(samples instanceof Int16Array)) {
      throw new TypeError('samples must be an Int16Array');
    }

    const bytes = new Uint8Array(samples.byteLength);
    const view = new DataView(bytes.buffer);
    for (let index = 0; index < samples.length; index += 1) {
      view.setInt16(index * 2, samples[index], true);
    }

    const pending = new Uint8Array(this.suffix.length + bytes.length);
    pending.set(this.suffix);
    pending.set(bytes, this.suffix.length);

    const frames = [];
    let offset = 0;
    while (pending.length - offset >= this.frameBytes) {
      frames.push(pending.slice(offset, offset + this.frameBytes));
      offset += this.frameBytes;
    }
    this.suffix = pending.slice(offset);
    return frames;
  }
}

const BASE64 = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;

/** Decode a strict, little-endian PCM16 base64 payload without Node globals. */
export function decodePCM16Base64(encoded, maxBytes) {
  if (typeof encoded !== 'string' || encoded.length === 0 || encoded.length % 4 !== 0 || !BASE64.test(encoded)) {
    throw new TypeError('encoded must be strict base64');
  }
  if (!Number.isInteger(maxBytes) || maxBytes <= 0) {
    throw new RangeError('maxBytes must be a positive integer');
  }

  let binary;
  try {
    binary = atob(encoded);
  } catch {
    throw new TypeError('encoded must be strict base64');
  }

  const byteLength = binary.length;
  if (byteLength === 0 || byteLength % 2 !== 0 || byteLength > maxBytes) {
    throw new RangeError('decoded PCM must have a positive even byte length within maxBytes');
  }

  const bytes = new Uint8Array(byteLength);
  for (let index = 0; index < byteLength; index += 1) {
    bytes[index] = binary.charCodeAt(index);
  }

  const view = new DataView(bytes.buffer);
  const pcm = new Int16Array(byteLength / 2);
  for (let index = 0; index < pcm.length; index += 1) {
    pcm[index] = view.getInt16(index * 2, true);
  }
  return pcm;
}
