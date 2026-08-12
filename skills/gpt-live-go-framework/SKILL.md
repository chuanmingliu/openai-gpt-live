---
name: gpt-live-go-framework
description: Use when configuring, extending, deploying, debugging, or verifying the GPT Live Go repository, especially Realtime speech, durable asynchronous handoffs, Web Search tools, PostgreSQL, browser demos, release gates, or privacy-sensitive operations.
---

# GPT Live Go Framework

Treat configuration, runtime composition, browser policy, and release evidence
as separate trust boundaries. Source and validators win disagreements.

## Start

1. Resolve the root with `git rev-parse --show-toplevel`; require `go.mod` to
   declare `github.com/chuanmingliu/openai-gpt-live`.
2. Classify local, `development_durable`, or production; voice-only or durable;
   direct, Compose, or Kubernetes.
3. Read `README.md` and the required reference:

| Work | Reference |
|---|---|
| YAML, providers, secrets, hashes | `references/configuration.md` |
| Durable browser Web Search and progress | `references/realtime-web-search.md` |
| Test and privacy evidence | `references/verification.md` |
| Ownership and protocol | `references/architecture.md` |
| Deployment and troubleshooting | `references/operations.md` |

## Invariants

- Clients reach only the gateway; browsers use the loopback BFF or a reviewed
  production auth proxy. A bearer never enters a URL, asset, storage, or log.
- Audio is 24 kHz mono PCM16LE, 960 bytes/20 ms, bounded, and never persisted.
- PostgreSQL owns committed durability, leases, policy, progress, cursors,
  reconnect, and runtime handoff authority.
- Speech and worker use `deepseek-v4-flash`; worker thinking is disabled and
  reasoning content is discarded. Their credentials remain separate process
  references even when one provider account supplies both.
- Web Search is a compiled, read-only, closed-world tool. Browser controls only
  tighten policy. Raw server definitions and runtime package execution are not
  extension mechanisms.
- Secret values stay outside YAML, config hashes, evidence, browser surfaces,
  and troubleshooting output. Unknown, skipped, unsafe, or uncertain state
  fails closed.

## Workflow

1. Start from a checked-in example; `_env` fields contain names only.
2. Validate unresolved config and compare component hashes.
3. For durable Web Search, follow the exact ordered runbook in
   `references/realtime-web-search.md`.
4. Run the relevant ladder in `references/verification.md` before claiming a
   result. A smoke, fixture, skip, loopback run, or manual transcript is not
   production evidence.

## Common mistakes

| Mistake | Correction |
|---|---|
| URL contains model/token | Use model and secret-reference fields. |
| DeepSeek base includes the endpoint | Use the parent; adapter appends it. |
| Side-effect flag implies a tool | Registry/catalog ownership is separate. |
| Progress includes percent/reasoning | Use only stable phases and bounded summaries. |
| Local demo called production-ready | Follow reviewed deployment/release gates. |
