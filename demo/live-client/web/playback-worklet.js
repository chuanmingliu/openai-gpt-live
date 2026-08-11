import {PCMPlaybackQueue} from './audio-core.mjs';

class LivePlaybackProcessor extends AudioWorkletProcessor {
  constructor() {
    super();
    this.queue = new PCMPlaybackQueue(sampleRate, 10);
    this.generation = null;
    this.revision = 0;
    this.playing = false;
    this.playingRevision = 0;
    this.port.onmessage = ({data}) => {
      if (!data || typeof data.type !== 'string') return;
      if (data.type === 'clear') {
        if (!Number.isSafeInteger(data.generation) || data.generation < 0 || !Number.isSafeInteger(data.revision) || data.revision < 1) return;
        if (this.generation !== null && data.generation < this.generation) return;
        if (data.generation === this.generation && data.revision <= this.revision) return;
        this.generation = data.generation;
        this.revision = data.revision;
        this.queue.clear(data.generation);
        this.setPlaying(false);
        return;
      }
      if (data.type !== 'append' || !Number.isSafeInteger(data.generation) || data.generation < 0 || !Number.isSafeInteger(data.revision) || data.revision < 1 || !(data.pcm instanceof ArrayBuffer) || data.pcm.byteLength === 0 || data.pcm.byteLength > (64 << 10) || data.pcm.byteLength % 2 !== 0) return;
      if (this.generation === null) this.generation = data.generation;
      if (data.generation !== this.generation) return;
      if (data.revision <= this.revision) return;
      this.revision = data.revision;
      if (!this.queue.append(new Int16Array(data.pcm), data.generation)) {
        this.queue.clear(this.generation);
        this.setPlaying(false);
        this.port.postMessage({type: 'overflow', generation: this.generation, revision: this.revision});
        return;
      }
    };
  }

  setPlaying(value) {
    if (value && this.playing && this.playingRevision === this.revision) return;
    if (!value && !this.playing) return;
    this.playing = value;
    if (value) this.playingRevision = this.revision;
    this.port.postMessage({type: value ? 'playing' : 'idle', generation: this.generation, revision: this.revision});
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
