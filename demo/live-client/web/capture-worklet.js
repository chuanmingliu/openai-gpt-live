import {PCM16Framer, StreamingLinearResampler} from './audio-core.mjs';

class LiveCaptureProcessor extends AudioWorkletProcessor {
  constructor() {
    super();
    this.muted = false;
    this.resetPipeline();
    this.port.onmessage = ({data}) => {
      if (!data || data.type !== 'mute' || typeof data.value !== 'boolean') return;
      this.muted = data.value;
      if (this.muted) this.resetPipeline();
    };
  }

  resetPipeline() {
    this.resampler = new StreamingLinearResampler(sampleRate, 24000);
    this.framer = new PCM16Framer(960);
  }

  process(inputs) {
    const channel = inputs[0]?.[0];
    if (this.muted || !channel || channel.length === 0) return true;
    const samples = this.resampler.push(channel);
    for (const frame of this.framer.push(samples)) {
      this.port.postMessage({type: 'frame', pcm: frame.buffer}, [frame.buffer]);
    }
    return true;
  }
}

registerProcessor('live-capture', LiveCaptureProcessor);
