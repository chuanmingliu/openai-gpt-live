const MAX_SAMPLES = 24000 * 10;

class PlaybackProcessor extends AudioWorkletProcessor {
  constructor() {
    super();
    this.queue = [];
    this.offset = 0;
    this.playing = false;
    this.port.onmessage = (event) => {
      const message = event.data || {};
      if (message.type === "append" && message.pcm) {
        const samples = new Int16Array(message.pcm);
        if (this.queuedSamples() + samples.length <= MAX_SAMPLES) {
          this.queue.push(samples);
        }
      }
      if (message.type === "clear") {
        this.queue = [];
        this.offset = 0;
        this.setPlaying(false);
      }
    };
  }

  queuedSamples() {
    let total = -this.offset;
    for (const buffer of this.queue) {
      total += buffer.length;
    }
    return Math.max(0, total);
  }

  setPlaying(next) {
    if (this.playing === next) {
      return;
    }
    this.playing = next;
    this.port.postMessage({ type: next ? "playing" : "idle" });
  }

  process(_inputs, outputs) {
    const output = outputs[0] && outputs[0][0];
    if (!output) {
      return true;
    }
    for (let i = 0; i < output.length; i += 1) {
      if (this.queue.length === 0) {
        output[i] = 0;
        this.setPlaying(false);
        continue;
      }
      const current = this.queue[0];
      output[i] = current[this.offset] / 32768;
      this.offset += 1;
      this.setPlaying(true);
      if (this.offset >= current.length) {
        this.queue.shift();
        this.offset = 0;
      }
    }
    return true;
  }
}

registerProcessor("playback-processor", PlaybackProcessor);
