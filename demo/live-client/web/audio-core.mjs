const MAX_DECODED_BYTES = 64 * 1024;

export function floatToPCM16(samples) {
  const out = new Int16Array(samples.length);
  for (let i = 0; i < samples.length; i += 1) {
    const value = samples[i];
    if (value >= 1) {
      out[i] = 32767;
    } else if (value <= -1) {
      out[i] = -32768;
    } else {
      out[i] = value < 0 ? Math.round(value * 32768) : Math.round(value * 32767);
    }
  }
  return out;
}

export class StreamingLinearResampler {
  constructor(inputRate, outputRate) {
    this.inputRate = inputRate;
    this.outputRate = outputRate;
    this.ratio = inputRate / outputRate;
    this.position = 0;
    this.previous = 0;
    this.hasPrevious = false;
  }

  push(chunk) {
    if (!chunk || chunk.length === 0) {
      return new Int16Array(0);
    }
    const produced = [];
    let index = this.position;
    while (index < chunk.length) {
      const whole = Math.floor(index);
      const frac = index - whole;
      const left = whole < 0 ? (this.hasPrevious ? this.previous : chunk[0]) : chunk[Math.min(whole, chunk.length - 1)];
      const right = whole + 1 < chunk.length ? chunk[whole + 1] : left;
      produced.push(left + (right - left) * frac);
      index += this.ratio;
    }
    this.position = index - chunk.length;
    this.previous = chunk[chunk.length - 1];
    this.hasPrevious = true;
    return floatToPCM16(Float32Array.from(produced));
  }
}

export class PCM16Framer {
  constructor(frameBytes = 960) {
    this.frameBytes = frameBytes;
    this.pending = new Uint8Array(0);
  }

  push(samples) {
    const incoming = new Uint8Array(samples.buffer, samples.byteOffset, samples.byteLength);
    const joined = new Uint8Array(this.pending.length + incoming.length);
    joined.set(this.pending);
    joined.set(incoming, this.pending.length);
    const frames = [];
    for (let offset = 0; offset + this.frameBytes <= joined.length; offset += this.frameBytes) {
      frames.push(joined.slice(offset, offset + this.frameBytes));
    }
    this.pending = joined.slice(frames.length * this.frameBytes);
    if (this.pending.length > this.frameBytes) {
      this.pending = this.pending.slice(this.pending.length - this.frameBytes);
    }
    return frames;
  }
}

export function decodePCM16Base64(encoded) {
  if (typeof encoded !== "string" || encoded.length === 0) {
    throw new Error("invalid-audio");
  }
  const binary = atob(encoded);
  if (binary.length === 0 || binary.length % 2 !== 0 || binary.length > MAX_DECODED_BYTES) {
    throw new Error("invalid-audio");
  }
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i += 1) {
    bytes[i] = binary.charCodeAt(i);
  }
  return new Int16Array(bytes.buffer.slice(0));
}

export function encodePCM16Base64(bytes) {
  let binary = "";
  for (let i = 0; i < bytes.length; i += 1) {
    binary += String.fromCharCode(bytes[i]);
  }
  return btoa(binary);
}
