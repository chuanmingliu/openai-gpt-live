# PRD: OpenAI GPT-Live

| Field | Value |
|---|---|
| **Status** | Reconstructive PRD from public OpenAI sources (not an OpenAI-published spec) |
| **Product** | GPT-Live — third-generation ChatGPT Voice system |
| **Compiled** | 2026-08-15 |
| **Audience** | Product, design, and engineering teams reconstructing or implementing a GPT-Live-shaped voice + delegation system |
| **Evidence rule** | **O** = official OpenAI prose. **RE** = reverse-engineered client / inferred. Do not ship RE names as public API. |
| **Source dossier** | [docs/research/openai-gpt-live.md](../research/openai-gpt-live.md) |

This document collects every confirmed product, architecture, safety, and constraint claim about GPT-Live into one requirements file. It describes **what OpenAI shipped and required of itself**, not this repository’s implementation.

---

## 1. Problem

Voice AI that feels like a person must satisfy two clocks at once.

1. **Conversational clock.** Humans hand off in a fraction of a second. A turn detector that waits for silence either cuts the user off or feels sluggish. Cascaded STT→LLM→TTS adds serial latency and drops tone and pacing. **O**
2. **Work clock.** Search, reasoning, coding, and multi-agent tasks take seconds to minutes. Putting that work on the same path as speech makes the assistant either slow or shallow. **O**

Previous OpenAI generations failed one of those clocks:

| Generation | Official name | Failure |
|---|---|---|
| 1 | Cascaded / Standard Voice | STT → LLM → TTS. Slow, stilted, information lost across models. **O** |
| 2 | Advanced Voice Mode | Native speech-to-speech, still gated by a **silence-based turn detector**. Rigid back-and-forth; pauses mistaken for end-of-turn. **O** |

**Problem statement.** Ship a voice system that listens and speaks at the same time, never stalls the audio path on tools or frontier inference, and still delivers frontier-quality answers and durable agent work.

---

## 2. Vision

> “A world where collaborating with AI feels as fluid and responsive as working with another person, while reasoning and complex task execution happen seamlessly in the background.” **O** — [Introducing GPT-Live](https://openai.com/index/introducing-gpt-live/)

> “The system’s primary job is to sustain an uninterrupted media loop. Other work, such as invoking frontier models and persisting the conversation, happens off the live path.” **O** — [continuous-voice-interaction](https://openai.com/index/continuous-voice-interaction-with-gpt-live/)

Voice is the **control surface**. Screens hold state, diffs, tables, and approvals. Workers hold slow execution.

---

## 3. Goals

**G1.** Remove the turn detector from the audio path. The voice model is full-duplex and decides many times per second whether to speak, listen, pause, interrupt, or invoke a tool. **O**

**G2.** Keep every audio frame on schedule. A slow tool, frontier call, or persistence write must not stall media. **O**

**G3.** Delegate search, reasoning, and agentic work to a frontier model (launch: GPT-5.5; desktop Work/Codex coordinator: GPT-5.6 Terra) while the conversation continues. **O**

**G4.** Start listening after a single UDP packet when Instant Connect + WARP succeed; fall back to normal signaling with no extra latency if pre-negotiated parameters are stale. **O**

**G5.** Derive a ChatGPT-compatible turn history (provisional UI + authoritative log) from overlapping continuous speech without imposing turn-taking on the live path. **O**

**G6.** On desktop, let one voice conversation start, check, steer, and coordinate multiple Work/Codex threads using the **same tools and permissions** as the selected experience. **O**

**G7.** Speak only high-value updates (milestones, blockers, concise completions). Detailed work stays visual. **O** (product) / **RE** (`speak_to_user`)

**G8.** Safety can act **while the model is speaking**: steer, spoken safety line, text resources, or end the call. **O**

**G9.** Measure capacity as concurrent sessions that keep frames on schedule, not GPU QPS. **O**

---

## 4. Non-goals (launch and current public surface)

| Non-goal | Source |
|---|---|
| Video or screen sharing on Live | Use Advanced Voice Mode. **O** |
| Custom GPTs on Live | GPTs stay on Advanced + Shimmer. **O** |
| Connected apps / plugins on Live Chat | **O** at launch; desktop Work/Codex uses that experience’s tools |
| Multi-speaker / group conversation | Designed for one-on-one. **O** |
| Preset ChatGPT personalities on Live | **O** |
| Public GPT-Live-1 speech-to-speech API | Waitlist only as of 2026-08-15. **O** |
| Exactly-once external side effects | Not claimed. **O** (Realtime lineage: at-least-once tools) |
| Live KV-cache migration between instances | Warm a replacement, prefill, dual-run, cut over. **O** |
| Voice impersonation / cloning | Predefined voices only. **O** |
| Linux desktop Voice as a documented first-class surface | Docs specify macOS/Windows (+ iOS remote). **O** |

---

## 5. Users and surfaces

### 5.1 Personas

| Persona | Job |
|---|---|
| Everyday ChatGPT user | Hands-free chat, commute, language practice, stories. 150M+ people/week use Voice or Dictation. **O** |
| Paid power user | Instant vs Medium/High intelligence; longer Live budget; files + Projects in Voice (from 7 Aug 2026). **O** |
| Knowledge worker (desktop) | Talk through a brief, start Codex tests, check blockers, share the frontmost window. **O** |
| Workspace admin | Enable/disable Voice; disable screen context / appshots; parental controls for teens. **O** |
| Developer (future) | GPT-Live-1 API waitlist. Today: `gpt-realtime-2.1` and `gpt-live-transcribe`. **O** |

### 5.2 Surfaces

| Surface | What GPT-Live does | Transport |
|---|---|---|
| **Voice in Chat** — web, iOS, Android, desktop Chat | Full-duplex conversation, search, memory, visual widgets, text+images; files+Projects from 7 Aug | WebRTC **O** |
| **Voice in Work and Codex** — desktop macOS/Windows, paired iOS remote | Start / prioritize / interrupt / redirect tasks; coordinate agents; spoken or on-screen progress | WebRTC + App Server **O** |
| **Dictation** | Speech → editable text, not Live | Not this product **O** |
| **Advanced / Standard** | Legacy; video/screen share; custom GPTs | Separate **O** |

A desktop chat or task **must begin in Voice** to stay in Voice. Text-started threads get dictation. Resume via “Start voice chat.” **O**

Only **one** voice chat may be active across the desktop app. **O**

---

## 6. Product principles

1. **The voice must flow.** Media path is small, dedicated, and never waits on application logic. **O**
2. **Two clocks, two planes.** Conversation stays sub-second; work may take minutes. **O**
3. **One assistant, many workers.** The user hears one persona. Execution is elsewhere. **O**
4. **Same permissions.** Voice is not a privileged path around Work/Codex policy. **O**
5. **Speech is scarce.** Screen is dense. Do not narrate logs. **O** / **RE**
6. **Safety is mid-generation.** Unsafe output can be interrupted while speaking. **O**
7. **Clients still speak standard WebRTC.** Complexity lives in a thin relay, not in every backend or a custom client protocol. **O**

---

## 7. Functional requirements

IDs are reconstructive. Priority: P0 = shipped as core Live behavior. P1 = desktop / later official. P2 = adjacent public API.

### 7.1 Continuous conversation (P0)

| ID | Requirement | Evidence |
|---|---|---|
| FR-C1 | The voice model SHALL listen and speak concurrently (full duplex). | **O** |
| FR-C2 | The system SHALL NOT place a turn detector on the audio path. | **O** |
| FR-C3 | The model SHALL decide many times per second whether to speak, continue listening, pause, interrupt, or invoke a tool. | **O** |
| FR-C4 | The user SHALL be able to interrupt while the assistant is speaking. The assistant SHALL follow the latest part of the conversation. | **O** |
| FR-C5 | The assistant MAY emit brief backchannels (“mhmm”, “yeah”, “got it”) without treating them as full turns. | **O** |
| FR-C6 | A user pause to think SHALL NOT automatically trigger a response. A user MAY say “wait until I ask you to respond.” | **O** |
| FR-C7 | The system SHOULD prefer the user’s voice over background noise / passing traffic. | **O** |
| FR-C8 | Spoken responses SHALL appear alongside streamed text in the chat. | **O** |
| FR-C9 | The user MAY type or attach a supported image in the same Live chat. | **O** |
| FR-C10 | Changing the selected voice mid-call SHALL start a new voice call in the same chat. | **O** |
| FR-C11 | One Live conversation SHALL last at most **2 hours**. | **O** |
| FR-C12 | One Voice conversation at a time (product-wide on desktop). | **O** |

### 7.2 Session start and recovery (P0)

| ID | Requirement | Evidence |
|---|---|---|
| FR-S1 | Signaling SHALL use HTTP or WebSocket, geo-steered to a nearby transceiver cluster. | **O** |
| FR-S2 | Media SHALL use WebRTC (ICE, DTLS, SRTP, RTCP, client AEC + jitter buffer). | **O** |
| FR-S3 | When WARP + Instant Connect succeed, the client SHALL start the session with a **single UDP packet**. The server MAY materialize the session on first media packet. | **O** |
| FR-S4 | If Instant Connect parameters are stale or invalid, the in-flight standard signaling path SHALL complete with no additional latency. | **O** |
| FR-S5 | WARP SHALL collapse ICE+DTLS+SCTP+DCEP (SPED, DTLS 1.3, SNAP, pre-negotiated data channels). Target: 6 RTT → 1 (product stack) or 6 → 2 (protocol-only). | **O** |
| FR-S6 | Late packets MAY be time-stretched then accelerated so playback stays gap-free. | **O** |
| FR-S7 | Reconnects SHALL restore compacted state without dropping the media loop when a model-instance handoff is used. | **O** |
| FR-S8 | **RE** Client SHOULD buffer ~100 ms of preroll (30 s ring, threshold ~0.003) so the first consonant is not clipped during WebRTC start. | **RE** |

### 7.3 Conversation state (P0)

| ID | Requirement | Evidence |
|---|---|---|
| FR-T1 | The application server SHALL maintain two views: a **speculative** view for the UI (mutable) and an **authoritative** transcript for analytics/safety. | **O** |
| FR-T2 | The newest message SHALL remain provisional (text, timing, speaker may change) until a speaker holds the floor long enough. | **O** |
| FR-T3 | Brief acknowledgements SHALL NOT necessarily become their own messages. Substantive interjections often SHALL. | **O** |
| FR-T4 | Displayed assistant text SHOULD stay coherent even if the user speaks in the middle. | **O** |
| FR-T5 | The post-call chat transcript MAY not be verbatim when speech overlaps or noise is high. This SHALL be documented. | **O** |
| FR-T6 | Long sessions SHALL compact context off the live path: warm a replacement instance, prefill compacted context, dual-run, cut over. Compaction invalidates the KV cache. | **O** |

### 7.4 Delegation to a frontier model (P0)

| ID | Requirement | Evidence |
|---|---|---|
| FR-D1 | Questions that need search, deeper reasoning, or agentic work SHALL be delegated off the live path. Launch background model: **GPT-5.5**. Continuously update as new frontier models ship. | **O** |
| FR-D2 | Instant / mini SHALL use GPT-5.5 Instant. Medium / High SHALL use GPT-5.5 Thinking at the matching effort. | **O** |
| FR-D3 | At voice-session start, the application server SHALL create and **prefill** a frontier inference session so the first delegation does not pay cold prompt cost. | **O** |
| FR-D4 | That session SHALL stay up for the voice call with **stable affinity** and **prompt caching**. Worker failure MUST remain recoverable. | **O** |
| FR-D5 | Reasoning effort, output limits, tool schemas, and model-tool round trips SHALL be treated as part of the **responsiveness budget**. The voice model MAY fill time but MUST NOT hide an arbitrarily slow result. | **O** |
| FR-D6 | A slow tool or backend SHALL delay only its own result, never media. | **O** |
| FR-D7 | Delegated work SHALL inherit the **safety training of the model that executes it**. | **O** |
| FR-D8 | The Live models themselves SHALL NOT have independent code execution or broad tools at launch. | **O** |

### 7.5 Desktop Work / Codex coordination (P1)

| ID | Requirement | Evidence |
|---|---|---|
| FR-W1 | GPT-Live SHALL manage the live conversation. **GPT-5.6 Terra** SHALL start and coordinate tasks in the app. | **O** |
| FR-W2 | Voice SHALL start separate threads, check existing threads, and send follow-up instructions. | **O** |
| FR-W3 | Voice SHALL bring progress, blockers, and results back into the voice conversation. | **O** |
| FR-W4 | Voice SHALL use the tools and permissions of the selected experience (Chat / Work / Codex). Not a privileged bypass. | **O** |
| FR-W5 | Example intents MUST work: review a launch brief; start a Codex test run; summarize blocking tasks. | **O** |
| FR-W6 | On macOS, “Take a look at this” SHALL capture an **appshot** of the frontmost window (image + accessible text, including off-screen scroll) when Screen context is on. Orgs MAY disable. | **O** |
| FR-W7 | Appshots require Screen Recording + Accessibility. Sensitive windows SHOULD be avoided; some web apps yield screenshot-only. | **O** |
| FR-W8 | Voice time and Codex/Work task budget SHALL be metered separately. Hitting one MUST NOT silently consume the other without notice. | **O** |
| FR-W9 | **RE** Handoffs SHOULD carry a stable ID, input transcript, and active transcript so several background tasks do not mix. | **RE** |
| FR-W10 | **RE** Ordinary worker text is silent. Speech occurs only via an explicit speak path. | **RE** |
| FR-W11 | **RE** Coordinator modes: converse here / quick local check / delegate blocking work. Observe a bounded set of threads (package: 8). | **RE** |

### 7.6 Presentation (P0 / P1)

| ID | Requirement | Evidence |
|---|---|---|
| FR-P1 | Live Chat MAY show visual cards (weather, stocks, sports, and similar widgets). | **O** |
| FR-P2 | From 7 Aug 2026, Live SHALL support file uploads and Projects (project chats, sources, instructions). | **O** |
| FR-P3 | Spoken output SHOULD be short. Diffs, tables, citations, retries stay on screen. | **O** / **RE** |
| FR-P4 | The assistant MUST NOT claim a desktop task finished before the execution layer confirms. | **RE** (consistent with official “progress, blockers, and results”) |

### 7.7 Safety (P0)

| ID | Requirement | Evidence |
|---|---|---|
| FR-SAFE1 | Inputs and outputs SHALL be checked as the conversation unfolds. | **O** |
| FR-SAFE2 | On unsafe output the system SHALL be able to: steer, play a spoken safety message, show text resources, or **end the call**. | **O** |
| FR-SAFE3 | Self-harm flows SHALL offer expert-vetted crisis helplines in voice. | **O** |
| FR-SAFE4 | Teen accounts: age-appropriate training; Parental Controls gate Voice; linked parents MAY be notified on high-risk self-harm. | **O** |
| FR-SAFE5 | Only predefined remastered voices. The model MUST NOT imitate a real person’s voice. | **O** |
| FR-SAFE6 | Automated monitors MAY interrupt, end the call, or degrade experience on repeated abuse. | **O** |
| FR-SAFE7 | Without delegation, Live-1 / mini SHALL NOT be High on Bio/Chem, Cyber, or Self-Improvement. | **O** |
| FR-SAFE8 | Supported generated audio SHALL carry SynthID (from 31 Jul 2026). Verification via `/research/verify/` and provenance API. Absence ≠ not-OpenAI. | **O** |
| FR-SAFE9 | Voice-native evals (production + synthetic audio) SHALL stay at or above launch standards vs Advanced Voice Mode. Known regression: Live-1 emotional reliance 0.88→0.82. | **O** |

### 7.8 Data (P0)

| ID | Requirement | Evidence |
|---|---|---|
| FR-DATA1 | Live/Advanced audio (and Advanced video) clips SHALL be stored with the chat transcript and retained **30 days**. | **O** |
| FR-DATA2 | Deleting a chat SHALL delete associated clips within 30 days unless legal/safety requires otherwise. | **O** |
| FR-DATA3 | Standard Voice SHALL delete audio after transcription (even on failure), unless the user opted into training. | **O** |
| FR-DATA4 | Audio/video SHALL NOT be used for training unless the user opts into “Include your audio/video recordings.” Business/Enterprise/Edu cannot share clips. | **O** |
| FR-DATA5 | Transcripts MAY be used for training if “Improve the model for everyone” is on (plan-dependent). | **O** |

### 7.9 Adjacent public API (P2)

| ID | Requirement | Evidence |
|---|---|---|
| FR-API1 | Do not document GPT-Live-1 as a public Realtime model until the waitlist converts. | **O** |
| FR-API2 | `gpt-live-transcribe` is the public live-STT model: 24 kHz PCM, delay ∈ {minimal, low, medium, high, xhigh}, $0.017/min. | **O** |
| FR-API3 | Current public S2S voice agents use `gpt-realtime-2.1` on `/v1/realtime` (WebRTC / WebSocket / SIP). | **O** |
| FR-API4 | Browser Realtime samples use data channel **`oai-events`**. | **O** (API samples) |

---

## 8. Architecture requirements

### 8.1 Logical architecture

```text
 Client (web / iOS / Android / desktop)
   |  signaling: HTTP or WebSocket  — Cloudflare geo / proximity
   |  media:    WebRTC UDP          — WARP + Instant Connect
   v
 Global Relay          stateless-ish UDP forwarder (Go)
   |  first STUN: route by server ICE ufrag (cluster + transceiver)
   |  Redis: <client IP:port → transceiver IP:port>
   v
 Transceiver           only WebRTC owner (Go + Pion)
   |  ICE, DTLS, SRTP, SDP, session lifecycle
   |  converts media/events to internal protocols
   v
 Media frontend        dedicated fast path (Go; replaced Python asyncio)
   |  every frame on schedule
   |
   +-- GPT-Live-1 / mini     LIVE PATH
   |
   +-- async RPC ------------------------------------------------
        Application server
          • prefilled frontier session + affinity + prompt cache
          • provisional vs authoritative turns
          • tools / search / widgets
          • desktop: GPT-5.6 Terra + Work/Codex threads
```

### 8.2 Named components (must exist in any faithful reconstruction)

| Component | Must | Must not |
|---|---|---|
| **Relay** | Forward UDP; parse STUN/ufrag only; tiny in-memory flow + Redis; `SO_REUSEPORT` + pinned OS threads acceptable | Decrypt; run ICE/DTLS; codec negotiate |
| **Transceiver** | Own all WebRTC state; terminate client; speak simple internal protocols upstream | Expose one public UDP port per session |
| **Media frontend** | Isolated from app logic; Go-class frame smoothness (new p95 ≤ old p50) | Block on tools or DB |
| **Stateful inference** | Continuous audio in/out; instance handoff + compaction off-path | Require a turn detector |
| **Application server** | Prefill frontier session; segment turns; desktop thread tools | Write to the media socket |
| **Frontier / Terra** | Do deep work; inherit own safety | Stall speech |

### 8.3 Transport constraints (infra post)

- Most sessions are **1:1**. Prefer transceiver over SFU. **O**
- Kubernetes cannot expose tens of thousands of UDP ports. Use a **shared VIP:port**. **O**
- ICE/DTLS are sticky. First packet MUST land on the owning transceiver without a hot-path external lookup. Encode routing in the **server ufrag**. **O**
- Global Relay shortens the first hop. Signaling geo-steer and media ingress MUST agree on cluster. **O**
- Design target: **>900M WAU** reach; low jitter/loss so barge-in stays crisp. **O**

### 8.4 Production operations

- Shadow traffic: serve AVM, run Live read-only on a growing share of real sessions before cutover. **O**
- Capacity question: *how many concurrent sessions keep every frame on schedule?* **O**
- Break latency down by **source geography**. Distant GPUs add delay at startup and streaming. **O**
- Long sessions: memory/persistence. Reconnects: compaction/restore. Disconnects: shutdown-handshake races. **O**
- Dashboards MUST NOT hide a single unhealthy engine inside an aggregate. Need path kill switches and known-good config validation. **O**

---

## 9. Non-functional requirements

| ID | Requirement | Evidence |
|---|---|---|
| NFR-1 | Conversational responsiveness: **sub-second** human expectation. | **O** (no official first-audio ms) |
| NFR-2 | Media frontend rewrite: new **p95** frame delivery matches previous **p50**. | **O** |
| NFR-3 | Session start: 6 RTT → 1 with Instant Connect + WARP; IETF WARP alone 6 → 2; unnamed prod ~4 RTT p50 / ~12 RTT p95 saved. | **O** |
| NFR-4 | Interaction decisions: many times per second. | **O** |
| NFR-5 | Max Live session: 2 hours. Desktop Voice: rolling **5-hour** allowance. Consumer Chat Voice: rolling **24-hour** allowance. | **O** |
| NFR-6 | Consumer Chat Voice (24h): Pro $200 unlimited Live-1; Pro $100 12h Instant + 12h Med/High + 24h mini; Go/Plus 1h + 1h + 2h mini; Free limited mini; Business 1h+1h then 5 credits/min; Ent/Edu flexible 5 credits/min. | **O** |
| NFR-7 | Desktop Voice (5h): Plus ~15–30 min; Pro 5x ~1–2.5 h; Pro 20x unlimited **voice** (tasks still use Codex budget); Business/legacy Ent/Edu ~45 min; flexible ~**6 credits/min**. Not available via API key. | **O** |
| NFR-8 | Voices: Arbor, Breeze, Cove, Ember, Juniper, Maple, Sol, Spruce, Vale (remastered). | **O** |
| NFR-9 | Languages: optimized for popular ChatGPT languages; some accents/fluency gaps accepted at launch. | **O** |
| NFR-10 | GPT-5.6 Terra is the everyday Work/Codex workhorse (272k context in CLI metadata). Sol/Luna are sibling SKUs, not the Voice coordinator. | **O** |

OpenAI has **not** published first-audio or barge-in millisecond SLOs for GPT-Live. Do not invent them.

---

## 10. Success metrics (official)

Human evals (matched 5–10 min conversations vs Advanced Voice Mode): overall preference, turn-taking, interruptions, flow, naturalness. Live-1 and mini “strongly preferred.” **O**

Capability (Live with delegation vs AVM):

- **GPQA** — Live-1 “substantially outperforms”
- **BrowseComp** — “strong gains”
- **τ³-Voice Telecom** (internal variant) — outperforms AVM

Numeric chart values were published as **images**. Third-party transcriptions exist; they are not official prose. Do not put unofficial chart numbers in launch claims.

Operational:

- Concurrent sessions with frames on schedule
- Startup RTT (WARP / Instant Connect)
- Frame-delivery p50/p95
- Shadow-test error rate before cutover
- Safety scores vs AVM (tables in system card)

---

## 11. Rollout (actual)

| Date | Event |
|---|---|
| 2026-05-04 | Relay + transceiver engineering post (already serving ChatGPT Voice + Realtime WebRTC). |
| 2026-07-08 | GPT-Live launch. Consumer web/iOS/Android. API waitlist. Not Business/Ent/Edu at launch. |
| 2026-07-09 | Go / Plus / Pro full rollout; Free in progress. |
| 2026-07-21 | Codex CLI: streaming **realtime V3** conversations (internal name). |
| 2026-07-23 | Voice in Work and Codex on desktop (Plus/Pro/Business/Edu/Enterprise). iOS remote. |
| 2026-07-27 | Live Voice for Edu / Business / Enterprise globally. |
| 2026-07-28/29 | `gpt-live-transcribe` + `gpt-transcribe` in the API. |
| 2026-07-31 | SynthID on supported GPT-Live audio. |
| 2026-08-03 | Six-month architecture blog. |
| 2026-08-04 | System-card eval correction. |
| 2026-08-07 | Files + Projects in Voice. Faster Android connect. Enterprise Live default without Early Model Access. Background-conversation inactivity check. |

---

## 12. Open questions (officially unspecified)

1. Public GPT-Live-1 model ids, prices, and `/v1/realtime` event schema.
2. Whether Instant Connect / WARP are on the public Realtime API today.
3. First-audio and barge-in milliseconds.
4. Voice-model training details (codec, tokenizer, duplex objective).
5. Internal RPC between transceiver, media frontend, and application server.
6. Whether Chat Voice Live’s delegate is still GPT-5.5 or has moved with “continuously update.”
7. Whether the model tolerates its own voice on the uplink (community, unanswered).
8. Multi-region live KV-cache move (explicitly not the shipped handoff).

**RE names that MUST NOT become a public contract:** `handoff_request`, `speak_to_user`, `gpt-live-1-boulder-alpha`, 8-thread observer limit, 100 ms preroll constants.

---

## 13. Requirements traceability (sources)

| Theme | Primary source |
|---|---|
| Product definition, duplex, delegation, evals, launch limits | https://openai.com/index/introducing-gpt-live/ |
| Media frontend, Go rewrite, instance handoff, compaction, turn derivation, WARP, Instant Connect, shadow traffic | https://openai.com/index/continuous-voice-interaction-with-gpt-live/ |
| Relay, transceiver, ufrag, Global Relay, Cloudflare, Pion, Redis | https://openai.com/index/delivering-low-latency-voice-ai-at-scale/ |
| Live vs Advanced vs Standard, 24h limits, data retention, Work/Codex Voice | https://help.openai.com/en/articles/20001274 |
| Desktop Voice UX, one-session, must-start-in-voice, appshot | https://learn.chatgpt.com/docs/features/voice |
| Duplex split GPT-Live / GPT-5.6 Terra; 5h desktop limits; 6 cpm | https://learn.chatgpt.com/docs/pricing |
| Appshot capture + permissions | https://learn.chatgpt.com/docs/appshots |
| Safety, mid-call interrupt, preparedness, score tables | https://deploymentsafety.openai.com/gpt-live |
| WARP protocol | https://datatracker.ietf.org/doc/draft-uberti-tsvwg-warp/ |
| Public STT / Realtime (adjacent) | https://developers.openai.com/api/docs/guides/realtime |
| Files + Projects in Voice | https://help.openai.com/en/articles/6825453-chatgpt-release-notes (7 Aug 2026) |
| Desktop Voice launch | same, 23 Jul 2026; learn.chatgpt.com changelog 26.715 |
| Independent desktop protocol | https://x.com/zicojzc/status/2080665138370388218 |
| Quoted excerpts and tables | [docs/research/openai-gpt-live.md](../research/openai-gpt-live.md) |

---

## 14. One-sentence product contract

**GPT-Live is a full-duplex conversational coordinator on a dedicated WebRTC media path: it listens and speaks without a turn detector, keeps every audio frame off the application clock, delegates thinking and tools to a frontier or Work/Codex worker, and returns only the updates worth hearing — with safety able to interrupt speech in flight.**
