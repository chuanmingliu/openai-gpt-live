# OpenAI GPT-Live: source dossier

- **Compiled:** 2026-08-13
- **Scope:** Official OpenAI product, engineering, safety, help, and IETF material, plus one independent client-package analysis. Not a description of this repository.
- **Rule:** Architecture claims are quoted from the source that owns them. Reverse-engineered client details are labeled as such.

## 1. What OpenAI says GPT-Live is

GPT-Live is OpenAI’s **third-generation voice system** and a **new generation of voice models** that power ChatGPT Voice.

> “We’re launching GPT‑Live, a new generation of voice models that make talking with AI feel much more like having a real conversation.”
> — [Introducing GPT-Live](https://openai.com/index/introducing-gpt-live/), 8 Jul 2026

> “GPT‑Live, our third-generation voice system, removes the turn detector from the audio path. Its voice model is full-duplex, which means it can listen and speak at the same time.”
> — [How we built a realtime system…](https://openai.com/index/continuous-voice-interaction-with-gpt-live/), 3 Aug 2026

Two product models shipped at launch:

| Model | Role in ChatGPT |
|---|---|
| **GPT-Live-1** | Default Voice model for Go, Plus, Pro, and paid workspace Voice |
| **GPT-Live-1 mini** | Default for Free users |

Intelligence / background-reasoning variants (product names, not separate public APIs):

- GPT-Live-1 Instant and GPT-Live-1 mini → background **GPT-5.5 Instant**
- GPT-Live-1 Medium and GPT-Live-1 High → background **GPT-5.5 Thinking** at medium / high effort

Source: launch post eval notes.

As of 8 Jul 2026, GPT-Live is **in ChatGPT**, with an **upcoming API**. Developers can join a waitlist. The public Realtime API still documents `gpt-realtime-2.1` / `gpt-realtime-2` as the developer speech-to-speech models; `gpt-live-transcribe` is the live-transcription model. Do not treat “GPT-Live” and “Realtime API gpt-realtime-*” as the same product.

---

## 2. Three generations of OpenAI voice (official)

OpenAI’s own genealogy:

1. **Cascaded voice (original ChatGPT Voice)**
   STT → LLM → TTS in series. Slow, stilted; information lost across models.
   Launch post: “the original ChatGPT Voice chained three models together.”

2. **Turn-based speech-to-speech (Advanced Voice Mode / GPT-4o voice)**
   One model processes and generates audio, but still waits for a **turn detector** to decide the user has finished. Silence / noise is mistaken for end-of-turn.

3. **GPT-Live (full-duplex, turnless)**
   No turn detector on the audio path. The model continuously processes input while generating output and decides many times per second whether to speak, listen, pause, interrupt, or invoke a tool.

---

## 3. The two architectural changes

The launch post names exactly two:

### 3.1 Continuous interaction (full duplex)

> “Instead of processing a sequence of separate messages, GPT‑Live continuously processes input while generating output. The model can therefore make interaction decisions many times per second: whether to speak, continue listening, pause, interrupt, or invoke a tool.”
> — [Introducing GPT-Live](https://openai.com/index/introducing-gpt-live/)

This is what enables backchannel acknowledgements (“mhmm”, “yeah”), thinking pauses without interruption, live translation, and barge-in.

### 3.2 Delegation for deeper work

> “Second, we decoupled GPT‑Live — which handles continuous interaction — from deeper work. When a question requires search, reasoning, or more agentic capabilities, GPT‑Live can delegate the task to another model like GPT‑5.5. This allows it to keep the conversation going, even as it handles multiple tasks in the background.”
> — same

Engineering restatement:

> “GPT‑Live puts the voice model in control of the conversation: audio flows in and out of the model, while deeper reasoning and tool use happen asynchronously. The system’s primary job is to sustain an uninterrupted media loop. Other work, such as invoking frontier models and persisting the conversation, happens off the live path.”
> — [continuous-voice-interaction](https://openai.com/index/continuous-voice-interaction-with-gpt-live/)

---

## 4. System architecture (official components)

```text
 Client (browser / iOS / Android / desktop)
   |  signaling: HTTP or WebSocket (Cloudflare geo/proximity steered)
   |  media:    WebRTC over UDP  (WARP + Instant Connect on startup)
   v
 Global Relay  (stateless-ish UDP forwarder, Go, SO_REUSEPORT)
   |  routes first STUN packet by ICE ufrag
   |  Redis: <client IP:port → transceiver IP:port>
   v
 Transceiver   (stateful WebRTC terminator, Go + Pion)
   |  owns ICE, DTLS, SRTP, session lifecycle
   |  converts media/events to simpler internal protocols
   v
 Media frontend + stateful inference  (Go; replaced Python asyncio)
   |  dedicated fast path: client <-> voice model
   |  every audio frame on schedule
   |
   +-- Voice model (GPT-Live-1 / mini)  <-- live path
   |
   +-- async RPC boundary ------------------------------------------
        |  application server
        |  prefills a frontier-model inference session at voice start
        |  stable session affinity + prompt caching
        |  derives discrete turns from continuous speech
        v
      Frontier model (GPT-5.5 Instant / Thinking)
        tools, search, agents
        cannot stall the media loop
```

### 4.1 Dedicated media path vs application logic

> “Audio moves between the client and the voice model on a dedicated fast path. Delegation, tool use, and other application work happen behind an asynchronous RPC boundary. A slow tool call or backend service can delay its own result, but cannot stall the flow of media.”
>
> “We wrote the media frontend and inference logic in Go, replacing a previous Python `asyncio` implementation. This significantly improved the smoothness of frame delivery, with the new system’s p95 matching the previous system’s p50.”
> — [continuous-voice-interaction](https://openai.com/index/continuous-voice-interaction-with-gpt-live/)

Named pieces: **media frontend**, **inference logic**, **asynchronous RPC boundary**, **application server**.

### 4.2 Relay + transceiver (scale path)

Source: [How OpenAI delivers low-latency voice AI at scale](https://openai.com/index/delivering-low-latency-voice-ai-at-scale/), 4 May 2026 (Yi Zhang, William McDonald). This predates GPT-Live by ~2 months and is the transport foundation GPT-Live reused.

**Why not SFU:** most sessions are 1:1 user↔model. An SFU treats the AI as another participant. OpenAI chose a **transceiver**: terminate WebRTC at the edge, then speak simpler internal protocols to inference, transcription, speech generation, tool use, and orchestration.

**Why not one-port-per-session:** Kubernetes + cloud LBs cannot expose tens of thousands of UDP ports per service.

**Shipped design:**

| Layer | Job | Does not do |
|---|---|---|
| **Relay** | Lightweight UDP forwarder. Reads STUN/ufrag on first packet. Forwards DTLS/RTP/RTCP opaquely. | Decrypt, ICE, codec negotiation |
| **Transceiver** | Only owner of WebRTC session state: ICE, DTLS handshake, SRTP keys, lifecycle. Signaling (SDP, codecs, ICE credentials). Upstream to backends. | Public per-session UDP ports |

Implementation facts:

- First transceiver was **one Go service on Pion** (signaling + media). Powers ChatGPT voice, Realtime API WebRTC, research.
- Relay is **Go in userspace**. No kernel bypass. `SO_REUSEPORT` + `runtime.LockOSThread` + preallocated buffers.
- Server **ufrag is generated with routing metadata** (cluster + owning transceiver).
- SDP answer advertises a **shared relay VIP:port** (example form `203.0.113.10:3478`).
- First client media packet is usually a **STUN binding request**.
- Relay session is an in-memory map + timers. Restart recovers from the next STUN, or earlier from **Redis** `<client IP+port, transceiver IP+port>`.
- **Global Relay**: geographically distributed identical ingress. Shortens first hop.
- Signaling: **Cloudflare geo and proximity steering** so the initial HTTP/WebSocket hits a nearby transceiver cluster. That request pins session location and which Global Relay address is advertised.

Requirements they designed for:

- Global reach for **>900 million weekly active users**
- Fast connection setup
- Low, stable media RTT, low jitter and packet loss (barge-in is called out by name)

### 4.3 Stateful inference and model-instance handoff

> “When a transition is needed, we can warm a replacement model instance alongside the existing one, prefill it with the current session context, run inference against both in parallel, and cut over when the new instance is fully ready.”

The same mechanism is used for **dynamic context compaction**. Compaction invalidates the **KV cache** and would otherwise require a blocking prefill. Instead the original instance keeps chatting while a replacement is prefills on the compacted context.

This is **model-instance handoff**, not the product “handoff” of work to GPT-5.5.

### 4.4 Delegation path (talking vs thinking)

On voice-session start, the **application server**:

1. Creates an inference session for the frontier model
2. **Prefills** it with the initial conversation context (prompt processed before first delegation)
3. Keeps that session for the whole voice conversation
4. Uses **stable session affinity** + **prompt caching**
5. Tunes reasoning effort, output limits, tool schemas, and model-tool round trips

> “The voice model can briefly keep the exchange moving while a frontier model reasons or uses tools, but it cannot hide an arbitrarily slow response. We therefore treated the full delegation loop—routing, prompt processing, inference, and tool calls—as part of the responsiveness budget.”

### 4.5 Deriving discrete turns from continuous speech

The live path is turnless. ChatGPT UI, analytics, and safety still want user/assistant messages.

The application server:

- Uses **partial transcripts and timing** to infer who has the floor
- Keeps the newest message **provisional** (text, timing, speaker can change)
- **Finalizes** once a speaker has held the floor long enough
- Does **not** turn a brief “mm hmm” into its own message
- Maintains two views:
  - **Speculative** → UI (can update)
  - **Authoritative** → analytics / logging

---

## 5. Transport: WebRTC, WARP, Instant Connect

### 5.1 Why WebRTC

> “WebRTC provides the transport foundation. It’s designed for low-latency media, and can continue operating through packet loss, clock drift, and client connection changes. If packets arrive late, WebRTC can subtly stretch audio to prevent gaps, and then briefly accelerate playback to catch back up to real time.”

Client-side: ICE, DTLS, SRTP, codec negotiation, RTCP, echo cancellation, jitter buffer.

Developer Realtime API still offers **WebRTC** (browsers/mobile), **WebSocket** (server media pipelines), and **SIP** (telephony). GPT-Live ChatGPT path is WebRTC.

### 5.2 WARP — WebRTC Abridged Roundtrip Protocol

- IETF individual draft: [draft-uberti-tsvwg-warp-00](https://datatracker.ietf.org/doc/draft-uberti-tsvwg-warp/) (22 Jul 2026)
- Authors: **Justin Uberti (OpenAI)**, Philipp Hancke (Meta)
- Related: [SPED](https://datatracker.ietf.org/doc/draft-hancke-webrtc-sped/) (DTLS over ICE), [SNAP](https://datatracker.ietf.org/doc/draft-hancke-tsvwg-snap/) (pre-negotiated SCTP), DTLS 1.3 ([RFC 9147](https://www.rfc-editor.org/rfc/rfc9147.html)), skip DCEP ([RFC 8832](https://www.rfc-editor.org/rfc/rfc8832.html))
- Implemented in **libwebrtc** and **Pion**

Vanilla WebRTC setup (draft analysis):

| Step | Protocol | Cumulative RTT |
|---|---|---|
| SDP offer/answer | signaling | 1 |
| ICE connectivity | ICE | 2 |
| DTLS 1.2 4-way | DTLS | 4 (media ready) |
| SCTP 4-way | SCTP | 6 (data ready) |
| DCEP | data channels | 6–6.5 |

OpenAI blog: WARP reduces media+data startup **from six RTTs to one**.
IETF draft abstract: optimizations together reduce setup **from 6 RTTs to 2 RTTs**.
Treat the blog number as OpenAI’s product stack (WARP + Instant Connect); treat the draft as the protocol-only claim.

### 5.3 Instant Connect

> “It negotiates these parameters ahead of time without reserving server capacity and without any changes to existing WebRTC implementations.”
>
> “If the pre-negotiated parameters are valid, the server can materialize the session when the first media packet arrives. If they are stale or invalid, the signaling flow is already underway, so the client can fall back with no additional latency.”
>
> “the client can now start a session with a single UDP packet.”
> — [continuous-voice-interaction](https://openai.com/index/continuous-voice-interaction-with-gpt-live/)

Instant Connect is **not** in the IETF WARP draft. It is an OpenAI signaling-path invention: pre-share SDP so the first media packet can instantiate the session.

---

## 6. Product surface in ChatGPT

### 6.1 Voice modes (Help Center)

Source: [ChatGPT Voice](https://help.openai.com/en/articles/20001274)

| Mode | What it is |
|---|---|
| **Live** | GPT-Live-1 / mini. Full duplex. Search, memory, visual widgets, text+images. No video, screen share, connected apps, or plugins at launch. |
| **Advanced** | Previous realtime (AVM). Still used for video / screen share and custom GPTs (Shimmer). |
| **Standard** | Cascaded STT → generate. Audio deleted after transcription. |

Voices remastered for GPT-Live: Arbor, Breeze, Cove, Ember, Juniper, Maple, Sol, Spruce, Vale.

Max Live conversation: **2 hours**. Usage is a rolling 24-hour budget (Free / Go / Plus / Pro tiers listed in the help article).

### 6.2 Two Voice products

1. **Voice in Chat** — everyday conversation. Web, iOS, Android, desktop Chat. Powered by GPT-Live.
2. **Voice in Work and Codex** — desktop macOS/Windows only (paired iOS remote). Start / prioritize / interrupt / redirect tasks; coordinate multiple agents; spoken or on-screen progress. Uses Work/Codex **tools and permissions**, not a privileged path. Delegated tasks draw the normal agent usage pool; connected Voice time is metered separately (~6 credits/minute on flexible Business/Enterprise).

150M+ people talk to ChatGPT via Voice or Dictation each week (launch post).

### 6.3 Launch limitations

- No voice+video or screen share on Live (use Advanced)
- Language quality uneven; some accents / fluency gaps
- Live not for custom GPTs
- Designed for one-on-one, not multi-speaker
- SynthID watermarking added 31 Jul 2026 on supported generated audio ([verify](https://openai.com/research/verify/))

---

## 7. Safety (system card + launch)

Sources: [GPT-Live System Card](https://deploymentsafety.openai.com/gpt-live) (8 Jul 2026, eval correction 4 Aug 2026); launch post.

- Same training / monitoring / enforcement stack as text models
- **Voice-native** evals: production audio (opt-in, PII-scrubbed) and synthetic spoken adversarial prompts
- Mid-generation safeguards: while speaking, the system can **steer**, play a **spoken safety message**, show **text resources**, or **end the call**
- Self-harm: crisis-helpline flows adapted for voice
- Teens: age-appropriate training; Parental Controls; parent notify on high-risk self-harm
- Predefined voices only; **not for impersonation**
- Preparedness: without delegation, neither Live-1 nor mini is High on Bio/Chem, Cyber, or Self-Improvement
- **Delegation inherits the delegated model’s safeguards**
- Live models themselves “lack broad access to tools independently of the models to which they delegate, and do not have code execution capability”
- Automated monitors can interrupt / end the call or degrade experience on repeated abuse
- 4 Aug 2026: eval config mismatch; some scores corrected; “overall safety conclusions unchanged”

---

## 8. Production rollout engineering

Silent **shadow traffic**: a growing share of production ChatGPT Voice sessions was also sent to GPT-Live **read-only**, while AVM still served users.

Lessons OpenAI published:

- Capacity ≠ GPU throughput. Measure **concurrent sessions that keep every frame on schedule**
- A supporting CPU/queue component saturated before GPUs
- Geography is first-order; break latency down by source region
- Long sessions: memory and persistence pressure
- Reconnects: compaction + state restoration
- Disconnects: shutdown-handshake races
- Aggregated dashboards hid unhealthy engines; they added granular telemetry, known-good config validation, staged ramps, path kill switches

---

## 9. Public developer API (related, not GPT-Live itself)

[Realtime and audio](https://developers.openai.com/api/docs/guides/realtime) as of mid-Aug 2026:

| Goal | Model / API |
|---|---|
| Low-latency voice agent | `gpt-realtime-2.1` (and `gpt-realtime-2`, `gpt-realtime-1.5`) on `/v1/realtime` |
| Live translation | `gpt-realtime-translate` on `/v1/realtime/translations` |
| Live transcription | **`gpt-live-transcribe`** |
| File STT / TTS | request-based audio APIs |

Connection methods: WebRTC, WebSocket, SIP.

GA notes: `POST /v1/realtime/client_secrets` for ephemeral browser creds; `/v1/realtime/calls` for WebRTC; `OpenAI-Safety-Identifier` header.

The **GPT-Live API is forthcoming**. `gpt-live-transcribe` is the only current public model with the Live name.

Earlier Realtime lineage (for context, not GPT-Live):

- Aug 2025: `gpt-realtime` GA, async function calling
- May 2026: `gpt-realtime-2`, `gpt-realtime-whisper`, `gpt-realtime-translate`
- Later: `gpt-realtime-2.1`, `gpt-live-transcribe`

---

## 10. Independent client analysis (not official)

[Zico (@zicojzc), 24 Jul 2026](https://x.com/zicojzc/status/2080665138370388218) inspected ChatGPT macOS **26.721.31836** (build 5828). Author’s own labels: official behavior / visible interfaces / inference.

What the package showed (client-side only):

- Persistent **WebRTC** session: AudioContext + AudioWorklet + `RTCPeerConnection`
- Worklet preroll: **30s ring buffer**, leading-signal threshold **0.003**, **100 ms** preroll so the first consonant is not clipped
- Data channel name: **`oai-events`**
- Default realtime model id in schema: **`gpt-live-1-boulder-alpha`** (client default; remotely overridable)
- Voice thread `source` / start kind: **`realtime_voice`**
- Event: **`handoff_request`** with handoff ID, input transcript, active transcript
- Coordinator modes: converse here / quick check here / delegate blocking work
- Tools to list/create/fork/read/wait/steer threads; observe up to **8** threads
- Spoken return path is explicit: **`speak_to_user`** — ordinary Codex text is silent
- Screen-context tool (screenshot + Accessibility Tree) for “this Slack thread”
- Session is not live until start accepted, App Server reports realtime started, WebRTC connects, init finishes
- Host coordinator: one voice session per machine; idle cleanup waits while orchestrated work is active

This is **not** a public OpenAI contract. Event names and model identifiers must not be treated as API.

It is consistent with official prose: live conversation plane vs durable execution plane; Voice uses Work/Codex tools and permissions.

---

## 11. Desktop Work / Codex (official, later than launch)

Help and learn.chatgpt.com (after 23 Jul 2026) add a **second official duplex split** that is not in the 8 Jul launch post:

> “GPT-Live manages the live conversation, while GPT-5.6 Terra starts and coordinates tasks in the app.”
> “It brings progress, blockers, and results back to your voice conversation so you can keep talking while work continues.”
> “ChatGPT Voice can start separate threads for longer tasks, check existing threads, and send follow-up instructions.”

Screen context is official on macOS: an **appshot** of the frontmost window (image + accessible text, including off-screen scroll). Orgs can disable it.

A chat or task **must begin in Voice** to stay in Voice. Tasks that start as text get dictation, not Live.

This is product language. It does **not** publish `handoff_request` / `speak_to_user`. Those remain reverse-engineered.

Do not assume ChatGPT Voice Live’s background delegate is still GPT-5.5 everywhere. Launch said GPT-5.5 and “we’ll continuously update.” Desktop Work/Codex coordinator is named **GPT-5.6 Terra**.

## 12. `gpt-live-transcribe` (public API; not GPT-Live-1)

Shipped 28–29 Jul 2026. Low-latency streaming STT. **$0.017/min**, same as `gpt-realtime-whisper`.

- Session `type: "transcription"`, model `gpt-live-transcribe`
- Events: `conversation.item.input_audio_transcription.delta` / `.completed`
- Config: `prompt`, `keywords`, `languages` (plural; do not send `language`), `delay` ∈ {minimal, low, medium, high, xhigh}
- Official examples use **24 kHz PCM**
- No word timestamps, speaker labels, confidence, or detected-language field
- Quality (OpenAI): Context Aware ASR semantic **38.5% → 44.6%** with free-form context; Common Voice 22-lang TER **19.70%** vs 20.33% Whisper-1; Real-World Audio 9-lang TER **9.60%** vs 11.65%

`gpt-transcribe` is the file / committed-turn sibling ($0.0045/min), WebSocket-oriented.

## 13. Safety scores (system card, corrected 4 Aug 2026)

Not prevalence-weighted. Adversarially selected hard cases vs Advanced Voice Mode.

**Production prompts (corrected)**

| Category | AVM | Live-1 | AVM mini | Live-1 mini |
|---|---:|---:|---:|---:|
| Sexual | 0.96 | 0.97 | 0.97 | 0.92 |
| Illicit | 0.74 | 0.95 | 0.60 | 0.91 |
| Mental health | 0.90 | 0.90 | 0.78 | 0.85 |
| Personal data | 0.96 | 0.96 | 0.95 | 0.96 |
| Emotional reliance | 0.88 | **0.82** | 0.78 | 0.78 |
| Self-harm | 0.89 | 0.97 | 0.81 | 0.95 |

**Synthetic prompts (corrected)**

| Category | AVM | Live-1 | AVM mini | Live-1 mini |
|---|---:|---:|---:|---:|
| Sexual | 0.76 | 0.95 | 0.85 | 0.95 |
| Illicit | 0.63 | 0.98 | 0.63 | 0.95 |
| Mental health | 0.57 | 0.85 | 0.47 | 0.80 |
| Personal data | 0.90 | 0.98 | 0.79 | 0.98 |
| Emotional reliance | 0.72 | 0.93 | 0.72 | 0.87 |
| Self-harm | 0.72 | 0.96 | 0.71 | 0.95 |
| Hate | 0.87 | 1.00 | 0.82 | 0.99 |
| Gore | 0.61 | 0.93 | 0.80 | 0.90 |

Official regressions called out: Live-1 emotional reliance 0.88→0.82 (prod); mini sexual 0.97→0.92 (prod). None below launch standards.

Capability charts on the launch page (GPQA, BrowseComp, preference) are **images**. Press transcribed numbers exist; they are not official prose. See dossier notes in the compiled briefing.

## 14. Rollout timeline (official)

| Date | Event |
|---|---|
| 2026-07-08 | Launch. Consumer web/iOS/Android. API waitlist. |
| 2026-07-09 | Fully rolled out to Go, Plus, Pro. Free in progress. |
| 2026-07-23 | Voice in Work and Codex on desktop (Plus/Pro/Business/Edu/Enterprise). iOS remote. |
| 2026-07-27 | GPT-Live Voice available to Edu, Business, Enterprise globally. |
| 2026-07-28/29 | `gpt-live-transcribe` + `gpt-transcribe` in the API. |
| 2026-07-31 | SynthID on supported GPT-Live audio. |
| 2026-08-03 | Architecture blog + official X thread. |
| 2026-08-04 | System-card eval correction. |
| 2026-08-07 | Files + Projects in Voice. Enterprise Live default **without** Early Model Access. Background-conversation inactivity check. |

WARP IETF §4.5 (unnamed production): setup latency reduced by **~4 RTT p50** and **~12 RTT p95**.

## 15. Officially unspecified

OpenAI has **not** published:

- Internal RPC schemas between transceiver, media frontend, and application server
- How the voice model represents “invoke a tool” vs “delegate to GPT-5.5”
- Exact first-audio or barge-in millisecond SLOs (only “sub-second”, p95=old p50 for frame smoothness)
- Multi-region session migration of a live KV cache (they do **warm parallel instance + cutover**, not live KV move)
- Public GPT-Live model cards / API event list
- Whether ChatGPT Live uses the same `/v1/realtime` event shapes as the developer Realtime API

---

## 12. Source list

### Official — product

| URL | What it is |
|---|---|
| https://openai.com/index/introducing-gpt-live/ | Launch, 8 Jul 2026. Full-duplex + delegation. Models, evals, Voice UX, safety, availability. SynthID update 31 Jul. |
| https://help.openai.com/en/articles/20001274 | ChatGPT Voice help. Live vs Advanced vs Standard. Limits. Work/Codex Voice. Data retention. |
| https://help.openai.com/en/articles/20001275-chatgpt-work-and-codex | Work + Codex product; Voice as control surface. |
| https://openai.com/form/gpt-live-1-in-the-api/ | API waitlist. |
| https://openai.com/research/verify/ | SynthID audio verification. |

### Official — engineering

| URL | What it is |
|---|---|
| https://openai.com/index/continuous-voice-interaction-with-gpt-live/ | 3 Aug 2026. Uberti & Malkani. Media frontend (Go), async RPC, instance handoff, compaction, delegation, turn derivation, WARP, Instant Connect, shadow traffic. |
| https://openai.com/index/delivering-low-latency-voice-ai-at-scale/ | 4 May 2026. Zhang & McDonald. Relay + transceiver, ufrag routing, Global Relay, Cloudflare steering, Pion, Redis. |

### Official — safety

| URL | What it is |
|---|---|
| https://deploymentsafety.openai.com/gpt-live | System card HTML. |
| https://deploymentsafety.openai.com/gpt-live/gpt-live.pdf | System card PDF. |

### Official — IETF / WebRTC

| URL | What it is |
|---|---|
| https://datatracker.ietf.org/doc/draft-uberti-tsvwg-warp/ | WARP draft-00. |
| https://datatracker.ietf.org/doc/draft-hancke-webrtc-sped/ | SPED: DTLS over ICE. |
| https://datatracker.ietf.org/doc/draft-hancke-tsvwg-snap/ | SNAP: pre-negotiated SCTP. |
| https://www.rfc-editor.org/rfc/rfc9147.html | DTLS 1.3. |
| https://github.com/fippo/warp-snap-sped | Draft source. |

### Official — developer Realtime (adjacent)

| URL | What it is |
|---|---|
| https://developers.openai.com/api/docs/guides/realtime | Architecture chooser; `/v1/realtime`; `gpt-live-transcribe`. |
| https://developers.openai.com/api/docs/guides/voice-agents | Speech-to-speech vs cascaded agents. |
| https://developers.openai.com/api/docs/models/gpt-realtime-2 | Realtime-2 model card. |
| https://developers.openai.com/api/docs/models/gpt-live-transcribe | Live transcription model. |
| https://openai.com/index/advancing-voice-intelligence-with-new-models-in-the-api/ | 7 May 2026. Realtime-2, Whisper streaming, translate. |
| https://openai.com/index/introducing-gpt-realtime/ | 28 Aug 2025. First production S2S + async tools. |

### Official — later product / API

| URL | What it is |
|---|---|
| https://learn.chatgpt.com/docs/features/voice | Desktop Voice, appshot, duplex split with GPT-5.6 Terra. |
| https://developers.openai.com/codex/features/voice | Codex Voice. |
| https://developers.openai.com/api/docs/models/gpt-live-transcribe | Live transcription model card. |
| https://developers.openai.com/api/docs/guides/realtime-transcription | Transcription sessions. |
| https://developers.openai.com/api/docs/guides/content-provenance | SynthID / provenance checks. |
| https://openai.com/live/ | Livestream landing. |
| https://x.com/OpenAI/status/2084378415818579975 | Official 3 Aug architecture thread. |

### Independent

| URL | What it is |
|---|---|
| https://x.com/zicojzc/status/2080665138370388218 | Desktop package analysis. Handoff protocol, `oai-events`, `speak_to_user`. Not a public contract. |

### Historical context (pre-Live)

| URL | What it is |
|---|---|
| https://openai.com/index/chatgpt-can-now-see-hear-and-speak/ | Original cascaded Voice. |
| https://openai.com/index/hello-gpt-4o/ | Advanced Voice Mode. |
