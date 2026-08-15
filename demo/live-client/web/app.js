import { ProtocolState } from "./protocol-state.mjs";
import { decodePCM16Base64, encodePCM16Base64 } from "./audio-core.mjs";

const statusNode = document.getElementById("status");
const errorNode = document.getElementById("error");
const transcriptNode = document.getElementById("transcript");
const startButton = document.getElementById("start");
const stopButton = document.getElementById("stop");
const muteButton = document.getElementById("mute");

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
let muted = false;
let protocol = new ProtocolState();
let userTurn = null;
let assistantTurn = null;
let stopped = true;

startButton.addEventListener("click", () => {
  startSession().catch((error) => {
    showError(error && error.name === "NotAllowedError" ? "microphone-denied" : "start-failed");
    stopSession();
  });
});
stopButton.addEventListener("click", () => stopSession());
muteButton.addEventListener("click", () => setMuted(!muted));
window.addEventListener("pagehide", () => stopSession());

async function startSession() {
  if (!stopped) {
    return;
  }
  stopped = false;
  hideError();
  setStatus("connecting");
  startButton.disabled = true;
  stopButton.disabled = false;
  muteButton.disabled = false;
  protocol = new ProtocolState();
  mediaStream = await navigator.mediaDevices.getUserMedia({
    audio: { channelCount: 1, echoCancellation: true, noiseSuppression: true, autoGainControl: true },
  });
  captureContext = new AudioContext();
  const playbackOptions = {};
  if (typeof AudioContext !== "undefined") {
    playbackOptions.sampleRate = 24000;
  }
  try {
    playbackContext = new AudioContext(playbackOptions);
  } catch {
    playbackContext = new AudioContext();
  }
  await captureContext.audioWorklet.addModule("/capture-worklet.js");
  await playbackContext.audioWorklet.addModule("/playback-worklet.js");
  captureNode = new AudioWorkletNode(captureContext, "capture-processor");
  playbackNode = new AudioWorkletNode(playbackContext, "playback-processor");
  const source = captureContext.createMediaStreamSource(mediaStream);
  source.connect(captureNode);
  playbackNode.connect(playbackContext.destination);
  captureNode.port.onmessage = (event) => {
    if (!socket || socket.readyState !== WebSocket.OPEN || !event.data || event.data.type !== "frame") {
      return;
    }
    socket.send(JSON.stringify({
      type: "input_audio_buffer.append",
      audio: encodePCM16Base64(new Uint8Array(event.data.pcm)),
    }));
  };
  playbackNode.port.onmessage = () => {};
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
  if (payload && payload.type === "session.created" && socket && socket.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify(SESSION_UPDATE));
  }
  for (const effect of protocol.consume(payload)) {
    applyEffect(effect);
  }
}

function applyEffect(effect) {
  switch (effect.type) {
    case "status":
      setStatus(effect.value);
      break;
    case "clear-playback":
      if (playbackNode) {
        playbackNode.port.postMessage({ type: "clear" });
      }
      break;
    case "audio":
      try {
        const pcm = decodePCM16Base64(effect.encoded);
        if (playbackNode) {
          playbackNode.port.postMessage({ type: "append", pcm: pcm.buffer }, [pcm.buffer]);
        }
        if (playbackContext && playbackContext.state === "suspended") {
          playbackContext.resume();
        }
      } catch {
        showError("invalid-audio");
      }
      break;
    case "user-delta":
      userTurn = writeTurn("user", effect.text, userTurn);
      break;
    case "user-done":
      userTurn = writeTurn("user", effect.text, userTurn);
      userTurn = null;
      break;
    case "assistant-delta":
      assistantTurn = writeTurn("assistant", effect.text, assistantTurn);
      break;
    case "assistant-done":
      assistantTurn = writeTurn("assistant", effect.text, assistantTurn);
      assistantTurn = null;
      break;
    case "error":
      showError(effect.code);
      break;
    default:
      break;
  }
}

function writeTurn(role, text, existing) {
  const item = existing || document.createElement("li");
  item.className = role;
  item.textContent = text;
  if (!existing) {
    transcriptNode.appendChild(item);
  }
  return item;
}

function setMuted(next) {
  muted = next;
  muteButton.textContent = muted ? "Unmute mic" : "Mute mic";
  if (captureNode) {
    captureNode.port.postMessage({ type: "mute", value: muted });
  }
}

function setStatus(value) {
  statusNode.textContent = value;
}

function showError(code) {
  errorNode.hidden = false;
  errorNode.textContent = code;
}

function hideError() {
  errorNode.hidden = true;
  errorNode.textContent = "";
}

function stopSession() {
  if (stopped && !socket && !mediaStream) {
    return;
  }
  stopped = true;
  if (socket) {
    socket.close();
    socket = null;
  }
  if (captureNode) {
    captureNode.port.onmessage = null;
    captureNode.disconnect();
    captureNode = null;
  }
  if (playbackNode) {
    playbackNode.port.postMessage({ type: "clear" });
    playbackNode.disconnect();
    playbackNode = null;
  }
  if (mediaStream) {
    for (const track of mediaStream.getTracks()) {
      track.stop();
    }
    mediaStream = null;
  }
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
  muted = false;
  muteButton.textContent = "Mute mic";
  if (statusNode.textContent !== "idle") {
    setStatus("idle");
  }
}
