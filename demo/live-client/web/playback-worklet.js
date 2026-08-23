const MAX_SAMPLES = 24_000 * 5;

class PlaybackProcessor extends AudioWorkletProcessor {
  constructor() {
    super();
    this.queue = new Int16Array(MAX_SAMPLES);
    this.head = 0;
    this.length = 0;

    this.port.onmessage = ({ data }) => {
      if (data?.type === 'clear') {
        this.head = 0;
        this.length = 0;
      } else if (data?.type === 'append' && data.pcm instanceof ArrayBuffer && data.pcm.byteLength % 2 === 0) {
        this.append(data.pcm);
      }
    };
  }

  append(buffer) {
    const bytes = new Uint8Array(buffer);
    const incoming = bytes.length / 2;
    const dropped = Math.max(0, this.length + incoming - MAX_SAMPLES);
    if (dropped > 0) {
      const discard = Math.min(dropped, this.length);
      this.head = (this.head + discard) % MAX_SAMPLES;
      this.length -= discard;
      if (incoming >= MAX_SAMPLES) {
        this.head = 0;
        this.length = 0;
      }
      this.port.postMessage({ type: 'overflow' });
    }

    const firstSample = Math.max(0, incoming - (MAX_SAMPLES - this.length));
    const view = new DataView(buffer);
    for (let index = firstSample; index < incoming; index += 1) {
      const tail = (this.head + this.length) % MAX_SAMPLES;
      this.queue[tail] = view.getInt16(index * 2, true);
      this.length += 1;
    }
  }

  dequeue() {
    if (this.length === 0) {
      return 0;
    }
    const sample = this.queue[this.head];
    this.head = (this.head + 1) % MAX_SAMPLES;
    this.length -= 1;
    return sample / 32768;
  }

  process(_inputs, outputs) {
    const output = outputs[0];
    if (!output) {
      return true;
    }

    const frames = output[0]?.length ?? 0;
    for (let frame = 0; frame < frames; frame += 1) {
      const sample = this.dequeue();
      for (const channel of output) {
        channel[frame] = sample;
      }
    }
    return true;
  }
}

registerProcessor('playback-processor', PlaybackProcessor);
