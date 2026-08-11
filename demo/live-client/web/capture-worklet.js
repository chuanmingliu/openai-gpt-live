import {StreamingPCM16Capture} from './audio-core.mjs';

class LiveCaptureProcessor extends AudioWorkletProcessor {
  constructor() {
    super();
    this.muted = false;
    this.pipeline = new StreamingPCM16Capture(sampleRate, 24000, 960);
    this.port.onmessage = ({data}) => {
      if (!data || data.type !== 'mute' || typeof data.value !== 'boolean') return;
      this.muted = data.value;
      if (this.muted) this.pipeline.reset();
    };
  }

  process(inputs) {
    const channel = inputs[0]?.[0];
    if (this.muted || !channel || channel.length === 0) return true;
    for (const frame of this.pipeline.push(channel)) {
      this.port.postMessage({type: 'frame', pcm: frame.buffer}, [frame.buffer]);
    }
    return true;
  }
}

registerProcessor('live-capture', LiveCaptureProcessor);
