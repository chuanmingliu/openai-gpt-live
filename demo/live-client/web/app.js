import { ProtocolState } from "./protocol-state.mjs";
import { SessionController } from "./session-controller.mjs";
import { decodePCM16Base64, encodePCM16Base64 } from "./audio-core.mjs";

const statusNode = document.getElementById("status");
const errorNode = document.getElementById("error");
const transcriptNode = document.getElementById("transcript");
const liveRegion = document.getElementById("live-announce");
const startButton = document.getElementById("start");
const stopButton = document.getElementById("stop");
const muteButton = document.getElementById("mute");
const MAX_VISIBLE_TURNS = 50;

const SESSION_UPDATE = {
  type: "session.update",
  session: {
    type: "realtime",
    audio: {
      input: {
        format: { type: "audio/pcm", rate: 24000 },
        turn_detection: {
          type: "server_vad",
          threshold: 0.5,
          prefix_padding_ms: 300,
          silence_duration_ms: 500,
          create_response: true,
          interrupt_response: true,
        },
      },
    },
  },
};

let socket = null;
let captureContext = null;
let playbackContext = null;
let captureNode = null;
let playbackNode = null;
let mediaStream = null;
let protocol = new ProtocolState();
let session = new SessionController();
let renderers = new Map();
let stopped = true;

startButton.addEventListener("click", () => {
  startSession().catch((error) => {
    showError(error && error.name === "NotAllowedError" ? "microphone-denied" : "start-failed");
    stopSession();
  });
});
stopButton.addEventListener("click", () => stopSession());
muteButton.addEventListener("click", () => applyAll(session.setMuted(!session.muted)));
window.addEventListener("pagehide", () => stopSession());

async function startSession() {
  if (!stopped) {
    return;
  }
  stopped = false;
  hideError();
  applyAll(session.start());
  startButton.disabled = true;
  stopButton.disabled = false;
  muteButton.disabled = false;
  protocol = new ProtocolState();
  renderers = new Map();
  mediaStream = await navigator.mediaDevices.getUserMedia({
    audio: { channelCount: 1, echoCancellation: true, noiseSuppression: true, autoGainControl: true },
  });
  if (stopped) {
    stopTracks(mediaStream);
    mediaStream = null;
    return;
  }
  captureContext = new AudioContext();
  try {
    playbackContext = new AudioContext({ sampleRate: 24000 });
  } catch {
    playbackContext = new AudioContext();
  }
  await captureContext.audioWorklet.addModule("/capture-worklet.js");
  await playbackContext.audioWorklet.addModule("/playback-worklet.js");
  if (stopped) {
    stopSession();
    return;
  }
  captureNode = new AudioWorkletNode(captureContext, "capture-processor");
  playbackNode = new AudioWorkletNode(playbackContext, "playback-processor");
  playbackNode.port.postMessage({ type: "clear", generation: session.generation });
  captureContext.createMediaStreamSource(mediaStream).connect(captureNode);
  playbackNode.connect(playbackContext.destination);
  captureNode.port.onmessage = (event) => {
    if (!event.data || event.data.type !== "frame") {
      return;
    }
    sendJSON({
      type: "input_audio_buffer.append",
      audio: encodePCM16Base64(new Uint8Array(event.data.pcm)),
    });
  };
  socket = new WebSocket(`${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/ws`);
  socket.addEventListener("message", onSocketMessage);
  socket.addEventListener("close", () => stopSession());
  socket.addEventListener("error", () => {
    showError("socket-failed");
    stopSession();
  });
}

function onSocketMessage(event) {
  let payload;
  try {
    payload = JSON.parse(event.data);
  } catch {
    showError("invalid-event");
    return;
  }
  if (payload && payload.type === "session.created") {
    sendJSON(SESSION_UPDATE);
  }
  for (const effect of protocol.consume(payload)) {
    applyAll(session.apply(effect));
  }
}

function sendJSON(payload) {
  if (!socket || socket.readyState !== WebSocket.OPEN) {
    return false;
  }
  try {
    socket.send(JSON.stringify(payload));
    return true;
  } catch {
    showError("socket-failed");
    stopSession();
    return false;
  }
}

function applyAll(effects) {
  for (const effect of effects) {
    applyEffect(effect);
  }
}

function applyEffect(effect) {
  switch (effect.type) {
    case "status":
      statusNode.textContent = effect.value;
      break;
    case "clear-transcript":
      transcriptNode.replaceChildren();
      renderers = new Map();
      break;
    case "clear-playback":
      if (playbackNode) {
        playbackNode.port.postMessage({ type: "clear", generation: effect.generation });
      }
      break;
    case "audio":
      try {
        const pcm = decodePCM16Base64(effect.encoded);
        if (playbackNode) {
          playbackNode.port.postMessage({ type: "append", pcm: pcm.buffer, generation: effect.generation }, [pcm.buffer]);
        }
        if (playbackContext && playbackContext.state === "suspended") {
          playbackContext.resume();
        }
      } catch {
        showError("invalid-audio");
      }
      break;
    case "render-turn":
      writeTurn(effect.role, effect.text);
      break;
    case "announce":
      if (liveRegion) {
        liveRegion.textContent = effect.text;
      }
      break;
    case "mute":
      muteButton.textContent = effect.value ? "Unmute mic" : "Mute mic";
      muteButton.setAttribute("aria-pressed", effect.value ? "true" : "false");
      if (captureNode) {
        captureNode.port.postMessage({ type: "mute", value: effect.value });
      }
      break;
    case "error":
      showError(effect.code);
      break;
    default:
      break;
  }
}

function writeTurn(role, text) {
  let item = renderers.get(role);
  if (!item) {
    item = document.createElement("li");
    item.className = role;
    transcriptNode.appendChild(item);
    renderers.set(role, item);
  }
  item.textContent = text;
  if (session[role === "user" ? "userOpen" : "assistantOpen"] === null) {
    renderers.delete(role);
  }
  while (transcriptNode.children.length > MAX_VISIBLE_TURNS) {
    transcriptNode.removeChild(transcriptNode.firstElementChild);
  }
}

function showError(code) {
  errorNode.hidden = false;
  errorNode.textContent = code;
}

function hideError() {
  errorNode.hidden = true;
  errorNode.textContent = "";
}

function stopTracks(stream) {
  if (!stream) {
    return;
  }
  for (const track of stream.getTracks()) {
    track.stop();
  }
}

function stopSession() {
  if (stopped && !socket && !mediaStream) {
    return;
  }
  stopped = true;
  applyAll(session.stop());
  const current = socket;
  socket = null;
  if (current) {
    current.close();
  }
  if (captureNode) {
    captureNode.port.onmessage = null;
    captureNode.disconnect();
    captureNode = null;
  }
  if (playbackNode) {
    playbackNode.disconnect();
    playbackNode = null;
  }
  stopTracks(mediaStream);
  mediaStream = null;
  if (captureContext) {
    captureContext.close();
    captureContext = null;
  }
  if (playbackContext) {
    playbackContext.close();
    playbackContext = null;
  }
  startButton.disabled = false;
  stopButton.disabled = true;
  muteButton.disabled = true;
  muteButton.textContent = "Mute mic";
  muteButton.setAttribute("aria-pressed", "false");
}
