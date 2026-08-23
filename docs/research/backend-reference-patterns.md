# Backend reference patterns: credential-free local startup

> Primary-source research, checked 2026-08-23. This note distinguishes **credential-free
> local process startup** (a local gateway can bind and start/attach a backend without
> receiving a new provider key) from **credential-free inference** (which still requires
> local models, cached assets, or a separately authenticated provider). Do not expose any
> of the loopback-only examples directly to a network.

## Executive takeaways

Both projects make the local controller a narrow process owner, not a credential broker.
The most reusable shape for this repository is:

1. Start a gateway on loopback by default; require an explicit secure reverse-proxy/auth
   deployment decision before exposing it.
2. Parse selectors and defaults before constructing providers. Only import, configure, and
   start the selected implementation.
3. Model the optional backend as absent/owned/external. `absent` preserves core voice
   functionality, `owned` starts a local child that retains its own user configuration, and
   `external` connects without changing the remote service.
4. Pass a minimal declared environment/stdio protocol to a child; never copy the gateway's
   realtime, identity, or unrelated credential state into it.
5. Use one composition root and a thin CLI bootstrap, so importing/test-constructing the
   service does not bind a port or launch a provider.

The references are patterns, not drop-in dependencies. In particular, Hugging Face's default
LLM is remote, while Qwen Audio Agent's realtime Qwen service requires a DashScope key; their
credential-free affordances concern local transport, optional backend activation, and reuse
of credentials that the selected backend already owns.

## 1. Hugging Face `speech-to-speech`

### CLI/configuration and lazy backend construction

- The CLI has three intentionally separate modes: `serve` (API only), `talk` (local
  microphone/speaker client), and `local` (compose server and client over loopback). The
  lightweight `talk` path does not import the pipeline until a server-running command is
  selected. [CLI source](https://github.com/huggingface/speech-to-speech/blob/main/src/speech_to_speech/cli.py),
  [command documentation](https://github.com/huggingface/speech-to-speech/blob/main/README.md#commands)
- `serve` defaults to `127.0.0.1:8765`; its own argument help calls the network API
  unauthenticated and requires an explicit `--host 0.0.0.0` to expose it. `local` forces the
  same loopback host. This is the clearest startup safety default to reuse. [server argument
  source](https://github.com/huggingface/speech-to-speech/blob/main/src/speech_to_speech/arguments_classes/realtime_server_arguments.py),
  [loopback composition](https://github.com/huggingface/speech-to-speech/blob/main/src/speech_to_speech/s2s_pipeline.py#L538-L568)
- The parser first selects STT, LLM, and TTS names, then adds only the selected backends'
  configuration dataclasses to its full parser. Inactive known options are accepted only for
  compatibility and logged as ignored. This avoids a configuration surface that requires
  credentials/dependencies for providers the process will not use. [selection and parser
  construction](https://github.com/huggingface/speech-to-speech/blob/main/src/speech_to_speech/s2s_pipeline.py#L156-L239),
  [user-facing behavior](https://github.com/huggingface/speech-to-speech/blob/main/README.md#supported-components)
- The backend registry stores static metadata and a factory. The actual module is imported
  inside that factory only when the selected handler is created; optional-dependency failures
  become an actionable install-extra error. This is a directly reusable lazy-provider
  pattern. [registry/factory implementation](https://github.com/huggingface/speech-to-speech/blob/main/src/speech_to_speech/backend_registry.py#L75-L214)

### Audio pipeline and API boundary

- A pipeline unit gives each concurrent session its own VAD/STT/LLM/TTS queues, stop/cancel
  state, and input/output queues. A single Realtime server owns a pool of these units. This
  makes audio-stage isolation explicit while keeping the transport separate from inference.
  [pipeline-unit construction](https://github.com/huggingface/speech-to-speech/blob/main/src/speech_to_speech/s2s_pipeline.py#L420-L536),
  [pipeline overview](https://github.com/huggingface/speech-to-speech/blob/main/README.md#how-it-works)
- Its Realtime boundary uses the core OpenAI Realtime event set over WebSocket and WebRTC:
  `/v1/realtime` accepts session/audio/conversation/response events and returns VAD,
  transcription, audio deltas, tools, and terminal events. The local SDK example deliberately
  uses `api_key="not-needed"`. This is useful as a compatible, no-client-secret local
  WebSocket contract; put authentication at a gateway when moving it off loopback.
  [Realtime API documentation](https://github.com/huggingface/speech-to-speech/blob/main/README.md#realtime-api)
- Optional HTTP LLM proxy endpoints are disabled by default. When enabled, they proxy to the
  configured upstream but do no access control or throttling themselves; the documentation
  explicitly says to use only a trusted network or an access-control gateway. Reuse the
  **disabled-by-default extension** idea, not an unauthenticated remote proxy.
  [LLM proxy documentation](https://github.com/huggingface/speech-to-speech/blob/main/README.md#llm-proxy)

### Credential-free quickstart variants

The normal quickstart needs `OPENAI_API_KEY`, but an officially documented local-server path
does not pass a provider credential to S2S: run a local `llama-server`, then set
`--responses_api_base_url http://127.0.0.1:8080/v1 --responses_api_api_key ""`. With models
cached locally, `HF_HUB_OFFLINE=1` prevents Hub access. This is the concrete precedent for
making an empty credential valid only for an explicitly selected loopback provider.

```sh
# terminal 1
llama-server -hf ggml-org/gemma-4-E4B-it-GGUF -np 2 -c 65536 -fa on --swa-full

# terminal 2
HF_HUB_OFFLINE=1 speech-to-speech serve \
  --model_name ggml-org/gemma-4-E4B-it-GGUF \
  --responses_api_base_url http://127.0.0.1:8080/v1 \
  --responses_api_api_key ""
```

[Fully local and offline instructions](https://github.com/huggingface/speech-to-speech/blob/main/README.md#fully-local),
[offline-operation constraints](https://github.com/huggingface/speech-to-speech/blob/main/README.md#offline-operation)

## 2. Qwen Audio Agent `qwen-audio-agent`

### Optional backend, default inheritance, and process ownership

- Configuration has a fixed, inspectable precedence: CLI, process environment, `.env.local`,
  `.env`, user config file, then built-in defaults. `qwenaudio config` reveals the concrete
  user-scoped configuration location. This provides a clear place for defaults without
  committing secrets. [configuration precedence](https://github.com/QwenAudio/qwen-audio-agent/blob/main/docs/configuration.md#configuration)
- `AGENT_PROTOCOL` has no default. Leaving it empty preserves frontend realtime voice chat;
  backend-required requests fail clearly instead of guessing, and `qwenaudio --backend none`
  makes the mode explicit. This is the strongest reference for an optional provider/backend
  that should not block credential-free gateway startup. [optional backend behavior](https://github.com/QwenAudio/qwen-audio-agent/blob/main/docs/configuration.md#selecting-a-backend)
- The published defaults combine a loopback gateway (`HOST=127.0.0.1`, `PORT=3101`), native
  permission mode, an empty backend-model override (use the backend's existing model), and a
  local S2S URL default. This is an excellent minimal configuration surface: local connectivity
  is the default; provider/model override is opt-in. [advanced defaults](https://github.com/QwenAudio/qwen-audio-agent/blob/main/docs/configuration.md#advanced-settings)
- The architecture distinguishes `owned` from `external`: owned backends are local children
  stopped with the gateway but load their own user config/models/tools/MCP; external backends
  are only connected to and retain their own configuration/state. Backend child processes get
  only OS variables plus that plugin's declared credential namespace; gateway identity,
  realtime, memory, and other backend secrets do not cross the boundary. [process-ownership
  contract](https://github.com/QwenAudio/qwen-audio-agent/blob/main/docs/architecture.md#process-ownership)
- Existing user configuration is deliberately reused. For example, Qwen Code is launched only
  as `qwen --acp` and keeps its own auth/provider/model/MCP/Skill/session configuration; the
  backend model is overridden only when the explicit override is set. The same documentation
  describes OpenClaw external mode as a black box whose credentials are not read, copied, or
  changed. [backend integration details](https://github.com/QwenAudio/qwen-audio-agent/blob/main/docs/configuration.md#selecting-a-backend)

### Interfaces and audio/runtime separation

- WebUI, TUI, and Desktop speak WebSocket/HTTP only to the Realtime Gateway. The Gateway
  queues Work and talks to all backend varieties through a shared ACP adapter, while
  backend-specific API details stay in the server agent layer. This is a useful separation for
  a local browser/audio client: clients should never need a provider token or adapter-specific
  protocol. [dependency direction](https://github.com/QwenAudio/qwen-audio-agent/blob/main/docs/architecture.md#dependency-direction)
- The HTTP/WebSocket app is an injectable composition root: importing its factory does not bind
  a port; the CLI/Desktop bootstrap is thin, and tests can inject isolated services. This is a
  direct startup-testability pattern. [composition-root statement](https://github.com/QwenAudio/qwen-audio-agent/blob/main/docs/architecture.md#process-ownership)
- Realtime speech remains responsive while backend work enters an owner FIFO and a fixed,
  persistent backend session. The submission tool returns immediately; writes are serialized
  both by the Gateway queue and ACP adapter. This is relevant if a local audio gateway must be
  usable with no backend, or while a previously configured backend is slow. [nonblocking
  flow](https://github.com/QwenAudio/qwen-audio-agent/blob/main/docs/architecture.md#nonblocking-request-flow),
  [fixed session](https://github.com/QwenAudio/qwen-audio-agent/blob/main/docs/architecture.md#fixed-backend-agent-session)
- The default remote-access stance is local-only: literal loopback Host/Origin are trusted;
  other-device access requires a reverse proxy that authenticates users, terminates HTTPS, and
  forwards WebSockets. Do not translate this into a bare `0.0.0.0` flag. [remote-access
  guidance](https://github.com/QwenAudio/qwen-audio-agent/blob/main/docs/configuration.md#remote-access-security)

### Credential-free quickstart and checks

Use the process paths below when the selected backend is already installed/configured by the
user; they add no key to the gateway and preserve native authentication. The realtime Qwen
service itself remains separately configured (or can point at the documented local S2S URL).

```sh
# Read-only readiness check: no install, login, credential validation, or model modification.
qwenaudio setup --backend codex

# Explicitly retain a voice-only gateway; no backend agent is launched.
qwenaudio --backend none

# Attach via the selected backend's existing user configuration.
AGENT_PROTOCOL=qwen qwenaudio
```

The setup command's non-mutating contract and the frontend-only mode are documented in the
[configuration guide](https://github.com/QwenAudio/qwen-audio-agent/blob/main/docs/configuration.md).

## Adaptation checklist for GPT Live Go

- Add an explicit local/no-provider profile whose `check-config` never resolves or validates
  credentials and whose listener is fixed to loopback unless an authenticated deployment mode
  is selected.
- Preserve `voice` and a remote provider as different adapters. Permit a local S2S adapter to
  omit its credential only when its parsed URL is loopback/private under an explicit operator
  policy; never infer this from an empty key alone.
- Represent backend selection as `none | owned | external`. `none` must still construct the
  gateway and public capability document, with backend-dependent actions rejected clearly.
- Put adapter metadata (factory, capability flags, config type, allowed environment names) in
  a registry. Parse only the selected adapter's config and instantiate/import it only after
  validation and selection.
- Keep the browser/mobile transport on one stable WebSocket contract; own provider credentials
  only in the trusted gateway adapter. Expose a plain local health/profile endpoint for UI
  discovery, not secrets.
- Make `check/setup` a no-network, no-login, no-install readiness command. Keep an explicit
  separate command/flag for bootstrap/download/auth actions.

## Primary sources

- [huggingface/speech-to-speech repository](https://github.com/huggingface/speech-to-speech)
- [QwenAudio/qwen-audio-agent repository](https://github.com/QwenAudio/qwen-audio-agent)
