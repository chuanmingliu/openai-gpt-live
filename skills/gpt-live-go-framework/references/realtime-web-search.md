# Durable realtime Web Search

Use this reference for the browser Tools drawer, `development_durable`, Web
Search policy, progress cards, or speech-plus-background-tool acceptance.
The canonical human runbook is `docs/demo/realtime-browser-client.md`.

## Fixed contract

| Concern | Exact bound |
|---|---|
| Mode | `development_durable`, loopback/development only |
| Speech and worker model | `deepseek-v4-flash`; thinking disabled |
| Executor | compiled, server-owned, read-only `web_search` |
| SafeSearch | `strict` or `moderate`; catalog may force strict |
| Regions | `wt-wt`, `us-en`, `uk-en`, `au-en`, `ca-en`, `cn-zh` |
| Results | browser 1–10; durable catalog maximum 5 |
| Timeout | browser 1000–10000 ms; durable catalog maximum 8000 ms |
| Progress | `queued`, `searching`, `reading_sources`, `preparing_answer`, then `completed`, `failed`, or `cancelled`; optional `needs_input` |

The browser cannot supply a model, query for the Test operation, destination,
command, package, executable, schema, environment, credential, path, header,
or server definition. Test always uses `DuckDuckGo privacy search`. Save sends
only the canonical policy; the acknowledgement must name
`deepseek-v4-flash`.

## Ordered operator flow

1. In a non-traced shell, securely prompt/export `POSTGRES_PASSWORD`,
   `DATABASE_URL`, `LIVE_STATIC_BEARER_TOKEN`, `LIVE_ADMIN_BEARER_TOKEN`,
   `LIVE_RETENTION_HMAC_KEY`, `LIVE_RECONNECT_HMAC_KEY`,
   `LIVE_PRIVACY_HMAC_KEY`, `LIVE_TOOL_ENCRYPTION_KEY`, `S2S_API_KEY`, and
   `DEEPSEEK_API_KEY`. Set `LIVE_DEMO_BEARER` symbolically from the local
   static bearer. Select reviewed non-secret `S2S_IMAGE` and absolute
   `S2S_CONFIG_PATH`. Values never enter docs, YAML, commands, browser, or logs.
2. Run `go run ./cmd/live-migrate -database-url-env DATABASE_URL -direction up`
   then the same command with `-check`; require clean schema version 8.
3. Check both components against `deploy/config/development-durable.yaml`.
   Verify the external speech configuration selects the model and
   thinking-disabled behavior.
4. Compute `LIVE_EXPECTED_CONFIG_HASH` from that file and run
   `make durable-demo-up`. Require content-free readiness at gateway
   `http://127.0.0.1:19090/health/ready` and worker
   `http://127.0.0.1:19091/health/ready`.
5. Set `LIVE_DEMO_BEARER` and run `make demo-live-client`. Open only
   `http://127.0.0.1:3000/`.
6. In Tools, Enable Web Search, select allowed SafeSearch/Region/results/
   timeout, run Test, require its fixed diagnostic to pass, Save, and require
   the matching server acknowledgement. Start listening and speak a search
   request.
7. Require monotonic stable phases while continuing speech. Manually hear a
   response and manually interrupt audible assistant output; automation cannot
   claim acoustic playback or barge-in.
8. Stop the browser session. Require gateway sessions, worker capacity, and
   the reviewed speech pool signal all idle without printing their bodies.
   Stop the BFF, run `make durable-demo-down`, and unset secrets. Do not delete
   the PostgreSQL volume as part of the runbook.

## Privacy boundary

Ephemeral raw audio, transcripts, Test/search query text, private tool
arguments/results, response HTML, provider reasoning, credentials, and
transport logs do not cross into progress, telemetry, browser storage, or
support output. PostgreSQL stores committed durable coordination and bounded
presentation data. Cards expose stable phase text, then only a bounded final
summary and approved HTTPS source links.

The first executor uses fixed DuckDuckGo HTTPS behavior with public-address
resolution checks and bounded compressed/decompressed input. Search or worker
failure must not close realtime voice. Raw `mcpServers` JSON is rejected.
Future MCP-backed catalog entries require compiled typed adapters, fixed
destinations, bounds, idempotency/approval policy, privacy tests, and operator
deployment review.

## Troubleshooting

| Symptom | Inspect without content |
|---|---|
| Schema/startup | Migrator `-check`, readiness, exact config hash |
| No worker progress | Worker capacity/readiness, leases, model access |
| Search timeout | Approved DNS/HTTPS egress and status class; keep bounds |
| No audio | Device permission, audio enable, PCM contract, speech pool |
| Policy rejected | Restore exact known fields and catalog-bounded values |

Use `references/verification.md` before reporting success.
