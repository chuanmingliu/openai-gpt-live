# Security and privacy

## Trust boundaries

Clients authenticate only to the gateway. Production uses OIDC issuer, audience, scope, tenant, and user claims; browser origins are allowlisted and proxy identity is accepted only from configured trusted CIDRs. Static bearer mode is local-only. The speech provider and background reasoner are outbound dependencies behind the gateway/worker and egress policy. Never expose speech-to-speech directly.

The admin listener is loopback-only and separately scoped. The metrics listener is separate from admin and Kubernetes permits only the observability namespace to reach it. Drain and feature changes require authenticated POST/PUT operations. Kubernetes defaults to denied ingress/egress, disables service-account token mounting, drops all capabilities, uses a read-only root, numeric UID/GID 65532, and restricts outbound provider traffic to the reviewed proxy.

## Data handling

PCM is bounded in memory and is not written to PostgreSQL, evidence, telemetry, or application logs. Conversation retention stores bounded committed text and public durable records only. Tool inputs are validated, encrypted with an externally supplied key, and protected by idempotency and approval. Presentation strips internal worker, lease, mutation, and private metadata before a client or speech provider can see it.

The release canary's no-persistence exception is server-side and exact: both configured synthetic tenant and authenticated subject must match. Missing or partially configured identity fails admission, and any other subject follows the ordinary durable conversation/item/handoff path. The exception changes neither protocol nor wire events and does not bypass real provider session isolation.

Logs and telemetry contain stable classes, counts, durations, bounded allowlisted labels, and HMAC-derived identifiers. They must never contain audio, transcript text, model reasoning, executable tool inputs/results, bearer values, database credentials, provider credentials, private keys, or secret material. Release evidence follows the same content-free rule.

Reconnect credentials are sent only in the bounded authenticated WebSocket
control frame after upgrade; they are never query parameters or HTTP headers.
Production ingress access logging is disabled so neither supported reconnect
metadata nor future request-target extensions can enter the proxy access log.

## Secrets and rotation

Configuration names secret-bearing environment variables but never embeds their values. Kubernetes Secrets are created out of band and excluded from Git. Rotate OIDC/admin credentials, provider credentials, privacy/retention HMAC keys, and tool-encryption keys through the external secret manager. Key IDs are immutable configuration; keep old decryption material for the bounded retention window, then remove it after data expiry.

## Safety and abuse limits

All queues, frames, sessions, tenant/user admission, retries, tool rounds, output bytes/tokens, deadlines, leases, and retention batches are bounded. Side-effecting tools require the configured policy and an unexpired durable approval. Safety failures suppress or degrade content; they do not fall through. Urgent safety can fence active speech before a fixed permitted announcement.

Report suspected cross-session output as a critical incident: disable handoffs, drain traffic, preserve content-free metrics/evidence, rotate affected credentials if indicated, and follow [runbooks/rollback.md](runbooks/rollback.md).

## Vertical profiles, adapters, and knowledge

Prompt templates are immutable logical Jinja-Go bundle entries, never filesystem paths. Published snapshots bind the exact engine identity, recipe hashes, reviewed adapter registry, and knowledge content hashes. Rendered prompts and recipe evidence remain untrusted user-message data beneath fixed server-owned system instructions.

Adapter manifests are a closed server-owned allowlist. A profile may disable or tighten a reviewed binding but cannot choose code, transport, destination, secrets, timeout, concurrency, or a larger result allowance. MCP tools require the same pinned native wrapper and must not become a generic MCP, HTTP, shell, installer, or cross-repository execution escape hatch.

Knowledge ingestion accepts bounded UTF-8 text/Markdown and stores immutable cited excerpts. Search authority is copied from the accepted handoff, not accepted from model arguments. Tombstones prevent new selection but preserve already accepted work; retention must therefore cover the maximum accepted-work lifetime. Knowledge dependency, timeout, or citation failures remain isolated to the background task and must not weaken the realtime output epoch fence, audio-commit single flight, or barge-in cancellation path.
