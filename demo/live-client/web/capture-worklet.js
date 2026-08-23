import { floatToPCM16, PCM16Framer, StreamingLinearResampler } from './audio-core.mjs';

class CaptureProcessor extends AudioWorkletProcessor {
  constructor() {
    super();
    this.enabled = false;
    this.resampler = new StreamingLinearResampler(sampleRate, 24000);
    this.framer = new PCM16Framer(960);

    this.port.onmessage = ({ data }) => {
      if (data?.type === 'reset') {
        this.resampler.reset();
        this.framer.reset();
      } else if (data?.type === 'enabled') {
        this.enabled = Boolean(data.enabled);
      }
    };
  }

  process(inputs) {
    const samples = inputs[0]?.[0];
    if (!this.enabled || !(samples instanceof Float32Array) || samples.length === 0) {
      return true;
    }

    const resampled = this.resampler.push(samples);
    const frames = this.framer.push(floatToPCM16(resampled));
    for (const frame of frames) {
      this.port.postMessage({ type: 'frame', pcm: frame.buffer }, [frame.buffer]);
    }
    return true;
  }
}

registerProcessor('capture-processor', CaptureProcessor);
