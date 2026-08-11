import {PCMPlaybackQueue} from './audio-core.mjs';

class LivePlaybackProcessor extends AudioWorkletProcessor {
  constructor() {
    super();
    this.queue = new PCMPlaybackQueue(sampleRate, 10);
    this.playing = false;
    this.port.onmessage = ({data}) => {
      if (!data || typeof data.type !== 'string') return;
      if (data.type === 'clear') {
        this.queue.clear();
        this.setPlaying(false);
        return;
      }
      if (data.type !== 'append' || !(data.pcm instanceof ArrayBuffer) || data.pcm.byteLength === 0 || data.pcm.byteLength > (64 << 10) || data.pcm.byteLength % 2 !== 0) return;
      if (!this.queue.append(new Int16Array(data.pcm))) {
        this.queue.clear();
        this.setPlaying(false);
        this.port.postMessage({type: 'overflow'});
        return;
      }
    };
  }

  setPlaying(value) {
    if (this.playing === value) return;
    this.playing = value;
    this.port.postMessage({type: value ? 'playing' : 'idle'});
  }

  process(_inputs, outputs) {
    const channels = outputs[0];
    if (!channels || channels.length === 0) return true;
    const written = this.queue.pullInto(channels[0]);
    for (let index = 1; index < channels.length; index += 1) channels[index].fill(0);
    if (written > 0) this.setPlaying(true);
    if (this.queue.queuedSourceSamples < 2) this.setPlaying(false);
    return true;
  }
}

registerProcessor('live-playback', LivePlaybackProcessor);
