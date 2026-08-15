import { StreamingLinearResampler, PCM16Framer } from "./audio-core.mjs";

class CaptureProcessor extends AudioWorkletProcessor {
  constructor(options) {
    super();
    const outputRate = 24000;
    this.muted = false;
    this.resampler = new StreamingLinearResampler(sampleRate, outputRate);
    this.framer = new PCM16Framer(960);
    this.port.onmessage = (event) => {
      if (event.data && event.data.type === "mute") {
        this.muted = Boolean(event.data.value);
      }
    };
  }

  process(inputs) {
    const channel = inputs[0] && inputs[0][0];
    if (!channel || this.muted) {
      return true;
    }
    const pcm = this.resampler.push(channel);
    for (const frame of this.framer.push(pcm)) {
      this.port.postMessage({ type: "frame", pcm: frame.buffer }, [frame.buffer]);
    }
    return true;
  }
}

registerProcessor("capture-processor", CaptureProcessor);
