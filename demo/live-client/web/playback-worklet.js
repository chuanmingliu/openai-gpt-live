import {PCMPlaybackQueue} from './audio-core.mjs';

class LivePlaybackProcessor extends AudioWorkletProcessor {
  constructor() {
    super();
    this.queue = new PCMPlaybackQueue(sampleRate, 10);
    this.generation = null;
    this.playing = false;
    this.port.onmessage = ({data}) => {
      if (!data || typeof data.type !== 'string') return;
      if (data.type === 'clear') {
        if (!Number.isSafeInteger(data.generation) || data.generation < 0) return;
        if (this.generation !== null && data.generation < this.generation) return;
        this.generation = data.generation;
        this.queue.clear(data.generation);
        this.setPlaying(false);
        return;
      }
      if (data.type !== 'append' || !Number.isSafeInteger(data.generation) || data.generation < 0 || !(data.pcm instanceof ArrayBuffer) || data.pcm.byteLength === 0 || data.pcm.byteLength > (64 << 10) || data.pcm.byteLength % 2 !== 0) return;
      if (this.generation === null) this.generation = data.generation;
      if (data.generation !== this.generation) return;
      if (!this.queue.append(new Int16Array(data.pcm), data.generation)) {
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
