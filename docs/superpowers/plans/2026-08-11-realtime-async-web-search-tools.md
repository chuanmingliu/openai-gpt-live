# Realtime Async Web Search Tools Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a safe, durable Web Search preset with truthful in-chat progress while keeping realtime voice independent, and move both speech and worker reasoning to `deepseek-v4-flash` with thinking disabled.

**Architecture:** Introduce one shared closed-world tool-policy package, persist its canonical snapshot on every handoff, and register a bounded in-repository DuckDuckGo executor in the existing worker registry. Add a loopback-only durable development composition, a same-origin BFF policy surface, and bounded browser progress cards; none of these paths may enter or delay the audio worklet/VAD/barge-in path.

**Tech Stack:** Go 1.26, PostgreSQL 16 with embedded migrations, `github.com/coder/websocket` v1.8.15, `golang.org/x/net/html` v0.53.0, DeepSeek OpenAI-compatible HTTP/SSE, dependency-free browser ECMAScript modules with Node 22 tests, Python 3.10+ with pytest.

## Global Constraints

- Realtime voice remains primary; search, PostgreSQL, worker, and DeepSeek latency never block microphone capture, VAD, playback, or barge-in.
- The only first-version executor is server-owned, read-only `web_search`; browser input cannot define commands, packages, schemas, environment, credentials, paths, headers, or destinations.
- Never execute `npx -y @modelcontextprotocol/server-duckduckgo`; it is not an official MCP package and dynamic installation is out of scope.
- Browser tool policy permits only `enabled`, `safe_search`, `region`, `max_results`, and `timeout_ms`.
- SafeSearch is `strict` or `moderate`; initial regions are `wt-wt`, `us-en`, `uk-en`, `au-en`, `ca-en`, and `cn-zh`; result count is 1--10; timeout is 1,000--10,000 ms.
- Web search uses only `POST https://html.duckduckgo.com/html/`, follows zero redirects, rejects non-public resolution, and caps compressed/decompressed bodies at 512 KiB/1 MiB.
- Queries are 1--512 UTF-8 bytes; titles are 256 Unicode scalars; snippets 1,024 scalars; result URLs 2,048 ASCII bytes.
- Both speech and worker requests use `deepseek-v4-flash` and `{"thinking":{"type":"disabled"}}`; `reasoning_content` is discarded.
- Progress phases are exactly `queued`, `searching`, `reading_sources`, `preparing_answer`, then one terminal state; no fabricated percentage.
- Browser card state is capped at 64 active, 64 terminal, 8 KiB summary/card, 10 sources/card, and 256 KiB aggregate text.
- No query, result content, provider body, transcript, audio, credential, or internal reasoning enters logs or metric labels.
- `development_durable` is loopback/development-only and must be rejected by Kubernetes production rendering and release promotion.
- Preserve the existing uncommitted realtime/audio changes; stage and commit only files named by each task.
- Before Task 1, record `git status --short` and the existing audio diff. Do not
  stash, discard, or absorb it. Task 10 must use `git add -p` for overlapping
  browser files unless those edits have already been committed by their owner.

---

## File Structure

- `internal/toolpolicy/policy.go` — canonical closed-world catalog and session policy.
- `internal/toolpolicy/policy_test.go` — enum, bound, canonicalization, and no-loosening tests.
- `internal/config/config.go` / `internal/config/hash.go` — `development_durable`, catalog YAML, and config-hash binding.
- `internal/protocol/client.go` / `server.go` / `codec.go` / `capabilities.go` — policy update/ack events.
- `internal/session/manager.go` / `effect.go` — session-owned policy, acknowledgement, and trusted handoff attachment.
- `internal/handoff/types.go` / `state.go` — canonical durable policy snapshot.
- `migrations/000008_handoff_tool_policy.*.sql` — persisted canonical policy.
- `internal/store/postgres/handoffs.go` — insert/read exact policy JSON.
- `internal/reasoner/deepseek/client.go` — official thinking-disabled request payload.
- `/Users/chuanmingliu/speech-to-speech/src/speech_to_speech/LLM/base_openai_compatible_language_model.py` — DeepSeek-specific thinking control.
- `internal/tools/websearch/client.go` — fixed-origin HTTP/DNS/redirect/body policy and HTML parsing.
- `internal/tools/websearch/executor.go` — immutable worker definition, arguments, output, and public source projection.
- `internal/worker/tools.go` / `runner.go` / `store.go` — policy-aware execution and stable progress phases.
- `internal/presentation/payload.go` — public phase and bounded source projection only.
- `cmd/live-worker/main.go` — immutable `web_search` registration.
- `cmd/live-gateway/main.go` — durable development composition and allowlisted snapshot.
- `deploy/config/development-durable.yaml` — non-secret durable demo config.
- `deploy/compose.development-durable.yml` — local PostgreSQL/gateway/worker override.
- `demo/live-client/tool_policy.go` — same-origin BFF policy storage and fixed diagnostic test.
- `demo/live-client/web/tool-state.mjs` — bounded drawer/card state.
- `demo/live-client/web/index.html` / `styles.css` / `app.js` — compact Tools drawer and timeline cards.
- `demo/live-client/web/browser-runtime.mjs` / `protocol-state.mjs` — policy resend and handoff event effects.
- `test/integration/web_search_handoff_test.go` — fake-provider durable vertical path.
- `docs/demo/realtime-browser-client.md`, `README.md`, `deploy/README.md`, `docs/protocol.md` — full operator contract.
- `skills/gpt-live-go-framework/` — verified local skill and configuration/runbook references.

The work stays in one plan because the policy schema, persisted handoff bytes,
worker registry version, progress payloads, and browser decoder form one
versioned vertical contract. Tasks still end in independent reviewable commits;
Task 5 is the only commit in the external speech repository.

---

### Task 1: Canonical Tool Policy and Durable Development Mode

**Files:**
- Create: `internal/toolpolicy/policy.go`
- Create: `internal/toolpolicy/policy_test.go`
- Modify: `internal/config/config.go`
- Modify: `internal/config/config_test.go`
- Modify: `internal/config/hash.go`
- Modify: `internal/config/hash_test.go`

**Interfaces:**
- Produces: `toolpolicy.Policy`, `toolpolicy.WebSearchPolicy`, `toolpolicy.Catalog`, `toolpolicy.Canonicalize(Policy, Catalog) (Policy, error)`, and `toolpolicy.MarshalCanonical(Policy) (json.RawMessage, error)`.
- Produces: `config.Config.IsProduction() bool`, `IsDurable() bool`, `UsesStaticAuth() bool`, and `ToolCatalog() toolpolicy.Catalog`.
- Consumed by Tasks 2, 3, 6, 7, 8, and 9.

- [ ] **Step 1: Write failing closed-world policy tests**

```go
func TestCanonicalizeRejectsPolicyExpansion(t *testing.T) {
	catalog := Catalog{WebSearch: WebSearchCatalog{
		Enabled: true, ForceStrict: true, Regions: []string{"wt-wt", "us-en"},
		MaximumResults: 5, MaximumTimeoutMS: 8_000,
	}}
	_, err := Canonicalize(Policy{WebSearch: WebSearchPolicy{
		Enabled: true, SafeSearch: SafeSearchModerate, Region: "cn-zh",
		MaxResults: 10, TimeoutMS: 10_000,
	}}, catalog)
	require.Error(t, err)
}

func TestCanonicalizeReturnsExactStableJSON(t *testing.T) {
	policy, err := Canonicalize(DefaultPolicy(), DefaultCatalog())
	require.NoError(t, err)
	raw, err := MarshalCanonical(policy)
	require.NoError(t, err)
	assert.JSONEq(t, `{"web_search":{"enabled":true,"safe_search":"moderate","region":"wt-wt","max_results":5,"timeout_ms":8000}}`, string(raw))
}
```

Add tables for duplicate/unsorted regions, unknown SafeSearch, off SafeSearch,
0/11 results, 999/10001 ms, and catalog-disabled requests.

- [ ] **Step 2: Run the policy tests RED**

Run: `go test ./internal/toolpolicy -count=1`

Expected: FAIL because the package and symbols do not exist.

- [ ] **Step 3: Implement the canonical policy package**

Use exact value types and return owned, sorted copies:

```go
const WebSearchTool = "web_search"

type SafeSearch string

const (
	SafeSearchStrict   SafeSearch = "strict"
	SafeSearchModerate SafeSearch = "moderate"
)

type WebSearchPolicy struct {
	Enabled    bool       `json:"enabled"`
	SafeSearch SafeSearch `json:"safe_search"`
	Region     string     `json:"region"`
	MaxResults int        `json:"max_results"`
	TimeoutMS  int        `json:"timeout_ms"`
}

type Policy struct {
	WebSearch WebSearchPolicy `json:"web_search"`
}

type WebSearchCatalog struct {
	Enabled          bool
	ForceStrict      bool
	Regions          []string
	MaximumResults   int
	MaximumTimeoutMS int
}

type Catalog struct {
	WebSearch WebSearchCatalog
}
```

Decode with `json.Decoder.DisallowUnknownFields`, reject trailing JSON, sort and
deduplicate catalog regions, and remarshal normalized structs for canonical
bytes. Never accept a destination or model field.

- [ ] **Step 4: Write failing mode/config/hash tests**

```go
func TestDevelopmentDurableSeparatesSecurityAndDurabilitySemantics(t *testing.T) {
	cfg := validConfig()
	cfg.Mode = "development_durable"
	cfg.Auth.StaticBearerEnv = "LIVE_STATIC_BEARER_TOKEN"
	cfg.Store.DatabaseURLEnv = "DATABASE_URL"
	cfg.Features.Handoffs = true
	require.NoError(t, cfg.Validate())
	assert.False(t, cfg.IsProduction())
	assert.True(t, cfg.IsDurable())
	assert.True(t, cfg.UsesStaticAuth())
}

func TestToolCatalogChangesConfigHash(t *testing.T) {
	a, b := validConfig(), validConfig()
	b.Tools.WebSearch.MaxResults = a.Tools.WebSearch.MaxResults + 1
	assert.NotEqual(t, mustHash(t, a), mustHash(t, b))
}
```

- [ ] **Step 5: Implement config fields, defaults, validation, and hash binding**

Add:

```go
type ToolCatalogConfig struct {
	WebSearch WebSearchToolConfig `yaml:"web_search"`
}

type WebSearchToolConfig struct {
	Enabled     bool          `yaml:"enabled"`
	ForceStrict bool          `yaml:"force_strict"`
	Regions     []string      `yaml:"regions"`
	MaxResults int           `yaml:"max_results"`
	Timeout    time.Duration `yaml:"timeout"`
}
```

Add `Tools ToolCatalogConfig` to `Config`. Bounds, regions, SafeSearch, result
count, and timeout use the approved defaults; `Enabled` remains false unless
the YAML explicitly enables Web Search.
`development_durable` requires static auth identity/scopes, PostgreSQL reference,
retention/reconnect HMAC references, `features.handoffs=true`, and a nonempty
enabled tool catalog; it uses local HTTP/WS transport rules, not production TLS.
Hash the canonical catalog and the exact mode. Do not treat it as production in
telemetry or deployment identity.

- [ ] **Step 6: Run focused tests GREEN**

Run: `go test -race -count=1 ./internal/toolpolicy ./internal/config`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/toolpolicy internal/config/config.go internal/config/config_test.go internal/config/hash.go internal/config/hash_test.go
git commit -m "feat: define canonical web search policy"
```

---

### Task 2: Policy Protocol, Session Ownership, and Reconnect Resend

**Files:**
- Modify: `internal/protocol/client.go`
- Modify: `internal/protocol/server.go`
- Modify: `internal/protocol/codec.go`
- Modify: `internal/protocol/codec_test.go`
- Modify: `internal/protocol/capabilities.go`
- Modify: `internal/protocol/capabilities_test.go`
- Modify: `internal/session/effect.go`
- Modify: `internal/session/manager.go`
- Modify: `internal/session/manager_test.go`
- Modify: `internal/gateway/app.go`
- Modify: `internal/gateway/app_test.go`

**Interfaces:**
- Consumes: `toolpolicy.Catalog` and `toolpolicy.Policy` from Task 1.
- Produces client event `protocol.LiveToolPolicyUpdateEvent` and server event `protocol.LiveToolPolicyUpdatedEvent`.
- Produces `session.Config.ToolCatalog toolpolicy.Catalog`, `session.Config.AsyncModel string`, and `session.HandoffRequestedEffect.ToolPolicy toolpolicy.Policy`.
- Consumed by Tasks 3, 8, 9, and 10.

- [ ] **Step 1: Write codec RED tests for exact events**

```go
func TestDecodeClientCanonicalizesToolPolicyUpdate(t *testing.T) {
	event, err := DecodeClient([]byte(`{"type":"live.tool_policy.update","event_id":"policy-1","tools":{"web_search":{"enabled":true,"safe_search":"moderate","region":"wt-wt","max_results":5,"timeout_ms":8000}}}`), testLimits)
	require.NoError(t, err)
	assert.Equal(t, "policy-1", event.(LiveToolPolicyUpdateEvent).EventID)
}

func TestDecodeClientRejectsToolPolicyExecutableFields(t *testing.T) {
	_, err := DecodeClient([]byte(`{"type":"live.tool_policy.update","event_id":"policy-1","tools":{"web_search":{"enabled":true,"safe_search":"moderate","region":"wt-wt","max_results":5,"timeout_ms":8000,"command":"npx"}}}`), testLimits)
	require.Error(t, err)
}
```

Add unknown tool, duplicate JSON key, unsafe region, excess results/timeout, and
server acknowledgement round-trip cases.

- [ ] **Step 2: Run protocol RED**

Run: `go test ./internal/protocol -run 'ToolPolicy|Capabilities' -count=1`

Expected: FAIL because the event types are unsupported.

- [ ] **Step 3: Add typed client/server events and capabilities**

```go
type LiveToolPolicyUpdateEvent struct {
	Type    string            `json:"type"`
	EventID string            `json:"event_id"`
	Tools   toolpolicy.Policy `json:"tools"`
}

type LiveToolPolicyUpdatedEvent struct {
	Type    string            `json:"type"`
	EventID string            `json:"event_id"`
	Tools   toolpolicy.Policy `json:"tools"`
	Model   string            `json:"model"`
}
```

Register `live.tool_policy.update` and `live.tool_policy.updated` in codec and
capabilities. Validate exact top-level keys, the existing event-ID grammar, and
Task 1 canonical policy bounds.

- [ ] **Step 4: Write manager RED tests for non-provider ownership**

```go
func TestManagerAcknowledgesPolicyWithoutSendingProviderCommand(t *testing.T) {
	client, provider := newManagerHarness(t)
	client.Send(protocol.LiveToolPolicyUpdateEvent{Type: "live.tool_policy.update", EventID: "policy-1", Tools: toolpolicy.DefaultPolicy()})
	ack := client.NextServerEvent(t).(protocol.LiveToolPolicyUpdatedEvent)
	assert.Equal(t, "deepseek-v4-flash", ack.Model)
	provider.AssertNoCommand(t)
}

func TestManagerAttachesCanonicalPolicyToEveryHandoff(t *testing.T) {
	dispatcher := &capturingDispatcher{}
	manager := newManagerWithCatalog(t, dispatcher)
	manager.ApplyPolicyAndDispatchReservedStart(t)
	assert.Equal(t, toolpolicy.DefaultPolicy(), dispatcher.Handoff.ToolPolicy)
}
```

- [ ] **Step 5: Implement session-local policy state**

Add a mutex-protected canonical policy to `runState`. In `clientReader`, handle
`LiveToolPolicyUpdateEvent` before reactor dispatch: canonicalize against
`Config.ToolCatalog`, replace the owned policy, and enqueue the exact ack with
model `Config.AsyncModel`, whose composition value is exactly
`deepseek-v4-flash`. It must never reach `Reactor.applyClient` or the
realtime provider. When copying a `HandoffRequestedEffect` for the durable
dispatcher, attach an owned current policy snapshot.

The default policy comes from `Config.ToolCatalog`; reconnect does not mutate it.
The browser/BFF resends the policy after each accepted session update in Task 9.
`gateway.New` maps `cfg.ToolCatalog()` and `cfg.Worker.Reasoner.Model` into the
session manager config; no browser field can supply `AsyncModel`.

- [ ] **Step 6: Run protocol/session race tests GREEN**

Run: `go test -race -count=1 ./internal/protocol ./internal/session`

Expected: PASS with no provider command for policy messages.

- [ ] **Step 7: Commit**

```bash
git add internal/protocol internal/session/effect.go internal/session/manager.go internal/session/manager_test.go internal/gateway/app.go internal/gateway/app_test.go
git commit -m "feat: add session web search policy protocol"
```

---

### Task 3: Persist the Exact Tool Policy with Every Handoff

**Files:**
- Modify: `internal/handoff/types.go`
- Modify: `internal/handoff/state.go`
- Modify: `internal/handoff/state_test.go`
- Create: `migrations/000008_handoff_tool_policy.up.sql`
- Create: `migrations/000008_handoff_tool_policy.down.sql`
- Modify: `migrations/embed.go`
- Modify: `migrations/embed_test.go`
- Modify: `internal/store/postgres/handoffs.go`
- Modify: `internal/store/postgres/handoffs_integration_test.go`
- Modify: `internal/store/postgres/schema_integration_test.go`
- Modify: `internal/gateway/handoffs.go`
- Modify: `internal/gateway/handoffs_test.go`
- Modify: `internal/gateway/app.go`
- Modify: `internal/gateway/app_test.go`

**Interfaces:**
- Consumes: `session.HandoffRequestedEffect.ToolPolicy` from Task 2.
- Produces: `handoff.CreateRequest.ToolPolicy json.RawMessage`, always canonical and durable.
- Produces migration schema version 8.
- Consumed by Task 7 worker execution.

- [ ] **Step 1: Write handoff validation RED tests**

```go
func TestNormalizeCreateRequestCanonicalizesToolPolicy(t *testing.T) {
	request := validCreateRequest()
	request.ToolPolicy = json.RawMessage(`{"web_search":{"timeout_ms":8000,"max_results":5,"region":"wt-wt","safe_search":"moderate","enabled":true}}`)
	normalized, err := NormalizeCreateRequest(request)
	require.NoError(t, err)
	assert.JSONEq(t, `{"web_search":{"enabled":true,"safe_search":"moderate","region":"wt-wt","max_results":5,"timeout_ms":8000}}`, string(normalized.ToolPolicy))
}

func TestNormalizeCreateRequestRejectsPolicyNotInAllowlist(t *testing.T) {
	request := validCreateRequest()
	request.ToolAllowlist = nil
	request.ToolPolicy = mustPolicyJSON(t, enabledPolicy())
	_, err := NormalizeCreateRequest(request)
	require.Error(t, err)
}
```

- [ ] **Step 2: Run RED**

Run: `go test ./internal/handoff -run ToolPolicy -count=1`

Expected: FAIL because `CreateRequest.ToolPolicy` does not exist.

- [ ] **Step 3: Add canonical policy to request hashing/cloning**

Add `ToolPolicy json.RawMessage` to `CreateRequest`. Decode and canonicalize it
with the Task 1 package; require enabled `web_search` iff `ToolAllowlist`
contains `web_search`. Include canonical bytes in request hashing, cloning,
idempotency comparisons, and aggregate byte accounting.

- [ ] **Step 4: Write migration/store RED tests**

```go
func TestHandoffRoundTripPreservesExactToolPolicy(t *testing.T) {
	repo := integrationRepository(t)
	request := validCreateRequest()
	request.ToolPolicy = mustPolicyJSON(t, enabledPolicy())
	created := createHandoff(t, repo, request)
	loaded, err := repo.Get(t.Context(), request.Scope, created.ID)
	require.NoError(t, err)
	assert.Equal(t, string(request.ToolPolicy), string(loaded.Request.ToolPolicy))
}
```

Add an up/down/up schema test proving v8 is exact and down refuses data loss when
non-default policy rows exist.

- [ ] **Step 5: Implement migration and repository queries**

Use:

```sql
ALTER TABLE handoffs
  ADD COLUMN tool_policy jsonb NOT NULL
  DEFAULT '{"web_search":{"enabled":false,"safe_search":"moderate","region":"wt-wt","max_results":5,"timeout_ms":8000}}'::jsonb;
```

Update every insert/select/scan path in `handoffs.go`. Return canonical JSON from
Go normalization rather than trusting PostgreSQL key order. Set
`latestVersion = 8` and add exact checksum/catalog assertions.

- [ ] **Step 6: Attach the session snapshot in the gateway**

Add `ToolPolicy` to `HandoffWork` and `SessionHandoffSnapshot`. The durable
dispatcher copies `effect.ToolPolicy`; the start coordinator uses that copy in
`CreateRequest`. The request allowlist is the intersection of the immutable
catalog snapshot and that policy: it contains `web_search` only when the
session policy enables it. Include the policy in coordinator retained-byte
limits and clone helpers. Steering reads the original persisted policy and
cannot replace or loosen it.

- [ ] **Step 7: Run focused and tagged compile gates GREEN**

Run:

```bash
go test -race -count=1 ./internal/handoff ./internal/gateway ./migrations
go test -race -tags=integration -run '^$' ./internal/store/postgres
```

Expected: PASS; the second command compiles without starting PostgreSQL.

- [ ] **Step 8: Commit**

```bash
git add internal/handoff internal/gateway migrations internal/store/postgres/handoffs.go internal/store/postgres/handoffs_integration_test.go internal/store/postgres/schema_integration_test.go
git commit -m "feat: persist handoff tool policy"
```

---

### Task 4: Use DeepSeek V4 Flash with Official Thinking Disabled in Go

**Files:**
- Modify: `internal/reasoner/reasoner.go`
- Modify: `internal/reasoner/deepseek/client.go`
- Modify: `internal/reasoner/deepseek/client_test.go`
- Modify: `internal/worker/runner.go`
- Modify: `internal/worker/runner_test.go`

**Interfaces:**
- Produces: `reasoner.Request.DisableThinking bool`.
- DeepSeek maps `DisableThinking=true` to exact provider JSON
  `"thinking":{"type":"disabled"}`.
- Consumed by every worker generation and tool-follow-up request.

- [ ] **Step 1: Write request-body RED tests**

```go
func TestStreamUsesV4FlashAndOfficialThinkingDisabled(t *testing.T) {
	captured := captureDeepSeekRequest(t)
	request := reasoner.Request{
		Model: "deepseek-v4-flash", Messages: []reasoner.Message{{Role: reasoner.RoleUser, Content: "hello"}},
		MaxOutputTokens: 64, DisableThinking: true,
	}
	_, err := captured.Client.Stream(t.Context(), request, func(reasoner.Chunk) error { return nil })
	require.NoError(t, err)
	assert.JSONEq(t, `{"model":"deepseek-v4-flash","messages":[{"role":"user","content":"hello"}],"max_tokens":64,"stream":true,"stream_options":{"include_usage":true},"thinking":{"type":"disabled"}}`, string(captured.Body))
}
```

Retain a stream containing `reasoning_content:"private"` and assert chunks,
results, logs, and errors contain none of it.

- [ ] **Step 2: Run RED**

Run: `go test ./internal/reasoner/deepseek -run 'V4Flash|ReasoningContent' -count=1`

Expected: FAIL because the request lacks `thinking`.

- [ ] **Step 3: Implement the official field and worker invariant**

```go
type providerThinking struct {
	Type string `json:"type"`
}

type providerRequest struct {
	Model         string             `json:"model"`
	Messages      []providerMessage  `json:"messages"`
	Tools         []providerTool     `json:"tools,omitempty"`
	MaxTokens     int                `json:"max_tokens"`
	Stream        bool               `json:"stream"`
	StreamOptions providerStreamOpts `json:"stream_options"`
	Thinking      *providerThinking  `json:"thinking,omitempty"`
	UserID        string             `json:"user_id,omitempty"`
}
```

Set `Thinking=&providerThinking{Type:"disabled"}` only from the trusted request
flag. The worker sets `DisableThinking:true` for the initial request and every
clone/tool round. Keep `reasoning_content` absent from public result types.

- [ ] **Step 4: Run focused tests GREEN**

Run: `go test -race -count=1 ./internal/reasoner/... ./internal/worker`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/reasoner internal/worker/runner.go internal/worker/runner_test.go
git commit -m "feat: disable DeepSeek V4 thinking"
```

---

### Task 5: Use the Official DeepSeek Thinking Contract in Speech-to-Speech

**Files (external repository):**
- Modify: `/Users/chuanmingliu/speech-to-speech/src/speech_to_speech/LLM/base_openai_compatible_language_model.py`
- Modify: `/Users/chuanmingliu/speech-to-speech/src/speech_to_speech/arguments_classes/responses_api_language_model_arguments.py`
- Modify: `/Users/chuanmingliu/speech-to-speech/tests/test_chat_completions_backend.py`
- Modify: `/Users/chuanmingliu/speech-to-speech/tests/test_responses_api_language_model.py`
- Modify: `/Users/chuanmingliu/speech-to-speech/tests/test_custom_service_handlers.py`
- Modify: `/Users/chuanmingliu/speech-to-speech/configs/tencent-deepseek-minimax.json`

**Interfaces:**
- Consumes: DeepSeek base URL and `responses_api_disable_thinking`.
- Produces exact `extra_body={"thinking":{"type":"disabled"}}` for official DeepSeek only.
- Preserves existing OpenAI and generic Qwen/vLLM behavior.

- [ ] **Step 1: Write provider-specific RED tests**

```python
def test_deepseek_uses_official_thinking_disabled_body():
    build = ChatCompletionsApiModelHandler._build_extra_body
    assert build("https://api.deepseek.com", True, None) == {
        "thinking": {"type": "disabled"}
    }
    assert build("https://api.deepseek.com/v1/", True, None) == {
        "thinking": {"type": "disabled"}
    }

def test_generic_openai_compatible_keeps_chat_template_contract():
    build = ChatCompletionsApiModelHandler._build_extra_body
    assert build("http://127.0.0.1:9000/v1", True, None) == {
        "chat_template_kwargs": {"enable_thinking": False}
    }
```

Update the Responses API request test to assert the exact DeepSeek body.

- [ ] **Step 2: Run RED in the speech repository**

Run:

```bash
cd /Users/chuanmingliu/speech-to-speech
uv run pytest -q tests/test_chat_completions_backend.py tests/test_responses_api_language_model.py
```

Expected: FAIL because DeepSeek currently receives the generic
`chat_template_kwargs` field.

- [ ] **Step 3: Implement canonical DeepSeek endpoint detection**

Parse `base_url` with `urllib.parse.urlsplit`. Match only scheme `https`, host
`api.deepseek.com`, no userinfo/query/fragment, and path `""`, `/`, or `/v1`.
When `disable_thinking` is true, return the official `thinking` body before the
generic provider branch. Do not include provider keys for official OpenAI.

Update the argument help and checked profile to:

```json
"model_name": "deepseek-v4-flash",
"responses_api_disable_thinking": true
```

- [ ] **Step 4: Run speech tests GREEN**

Run:

```bash
uv run pytest -q tests/test_chat_completions_backend.py tests/test_responses_api_language_model.py tests/test_custom_service_handlers.py
```

Expected: PASS.

- [ ] **Step 5: Commit only the speech files**

```bash
git -C /Users/chuanmingliu/speech-to-speech add src/speech_to_speech/LLM/base_openai_compatible_language_model.py src/speech_to_speech/arguments_classes/responses_api_language_model_arguments.py tests/test_chat_completions_backend.py tests/test_responses_api_language_model.py tests/test_custom_service_handlers.py configs/tencent-deepseek-minimax.json
git -C /Users/chuanmingliu/speech-to-speech commit -m "feat: configure DeepSeek V4 Flash"
```

Before committing, inspect `git -C /Users/chuanmingliu/speech-to-speech diff --cached --name-only` and abort if any unrelated pre-existing dirty file is staged.

---

### Task 6: Implement the Bounded DuckDuckGo Web Search Adapter

**Files:**
- Create: `internal/tools/websearch/client.go`
- Create: `internal/tools/websearch/client_test.go`
- Create: `internal/tools/websearch/executor.go`
- Create: `internal/tools/websearch/executor_test.go`
- Modify: `go.mod`
- Modify: `go.sum`

**Interfaces:**
- Consumes: `toolpolicy.WebSearchPolicy` and `worker.ToolExecutionRequest`.
- Produces: `websearch.New(resolver Resolver) (*Executor, error)`, `Executor.Execute`, `Executor.Project`, and `websearch.RegisteredTool(Executor) worker.RegisteredTool`.
- Output JSON: `{"query":"...","results":[{"title":"...","snippet":"...","url":"https://..."}]}`; query/snippets remain worker-private.
- Public projection JSON: `{"sources":[{"title":"...","url":"https://..."}]}`.

- [ ] **Step 1: Write HTTP-policy and parser RED tests**

```go
func TestSearchUsesExactPOSTAndZeroRedirects(t *testing.T) {
	executor, capture := newExecutorHarness(t, publicResolver())
	results, err := executor.Search(t.Context(), Request{Query: "golang", Policy: toolpolicy.DefaultPolicy().WebSearch})
	require.NoError(t, err)
	assert.Equal(t, "POST", capture.Method)
	assert.Equal(t, "https://html.duckduckgo.com/html/", capture.URL)
	assert.Len(t, results, 1)
}

func TestSearchRejectsPrivateResolutionBeforeDial(t *testing.T) {
	executor, err := New(staticResolver{netip.MustParseAddr("127.0.0.1")})
	require.NoError(t, err)
	_, err = executor.Search(t.Context(), validRequest())
	require.Error(t, err)
}

func TestSearchRejectsCompressedAndDecompressedOverflow(t *testing.T) {
	for _, size := range []int{(512 << 10) + 1, (1 << 20) + 1} {
		executor := overflowExecutor(t, size)
		_, err := executor.Search(t.Context(), validRequest())
		require.Error(t, err)
	}
}
```

Add tests for DNS rebinding across every dial, IPv4/IPv6 private/link-local/
metadata/multicast, 3xx with public `Location`, non-HTML type, invalid UTF-8,
20-candidate/10-result limits, URL scheme/control/length, query and field bounds,
cancellation, and no content in returned errors.

- [ ] **Step 2: Run RED**

Run: `go test ./internal/tools/websearch -count=1`

Expected: FAIL because the package does not exist.

- [ ] **Step 3: Implement the secure HTTP client and parser**

Use a dedicated `http.Transport` with proxy disabled, Task 1 timeout, and a
`DialContext` that resolves the exact host and rejects every non-public
`netip.Addr` before dialing. Set `CheckRedirect` to `http.ErrUseLastResponse`.
Use `io.LimitReader` before and after supported content decoding; reject an
overflow byte. Parse with `golang.org/x/net/html`, not regular expressions.

The executor accepts this exact argument schema:

```json
{
  "type": "object",
  "additionalProperties": false,
  "required": ["query"],
  "properties": {
    "query": {"type": "string", "minLength": 1, "maxLength": 512}
  }
}
```

Canonicalize arguments, apply only the persisted policy, and return bounded
JSON. `Project` reparses executor output and retains only source title/HTTPS URL.
`RegisteredTool` is `ReadOnly:true`, `ParallelSafe:true`,
`StableIdempotency:true`, `SideEffecting:false`.

- [ ] **Step 4: Run focused race tests and tidy GREEN**

Run:

```bash
go test -race -count=1 ./internal/tools/websearch
go mod tidy -diff
```

Expected: PASS; `golang.org/x/net v0.53.0` moves from indirect to direct with no
version change.

- [ ] **Step 5: Commit**

```bash
git add internal/tools/websearch go.mod go.sum
git commit -m "feat: add bounded web search executor"
```

---

### Task 7: Register Web Search and Emit Durable Stable Progress

**Files:**
- Modify: `internal/worker/tools.go`
- Modify: `internal/worker/store.go`
- Modify: `internal/worker/runner.go`
- Modify: `internal/worker/runner_test.go`
- Modify: `internal/worker/runner_integration_test.go`
- Modify: `internal/store/postgres/leases.go`
- Modify: `internal/store/postgres/leases_integration_test.go`
- Modify: `internal/presentation/payload.go`
- Modify: `internal/presentation/payload_test.go`
- Modify: `cmd/live-worker/main.go`
- Modify: `cmd/live-worker/main_test.go`

**Interfaces:**
- Consumes: durable `CreateRequest.ToolPolicy` and Task 6 registered tool.
- Extends `worker.ToolExecutionRequest` with `Policy toolpolicy.Policy`.
- Extends `worker.RegisteredTool` with `StartedPhase`, `CompletedPhase`, and `Projector ToolResultProjector`.
- Extends `worker.ProgressRecord` with `Phase string`.
- Produces completion structured JSON `{"text":"...","sources":[...]}`.

- [ ] **Step 1: Write runner RED tests for phases and policy**

```go
func TestRunnerUsesPersistedPolicyAndEmitsStableSearchPhases(t *testing.T) {
	store, executor := webSearchRunnerHarness(t)
	runOneJob(t, store)
	assert.Equal(t, toolpolicy.DefaultPolicy(), executor.Request.Policy)
	assert.Equal(t, []string{"searching", "reading_sources", "preparing_answer"}, store.ProgressPhases())
}

func TestRunnerFencesLateSearchProgressAfterLeaseLoss(t *testing.T) {
	store := leaseLosingSearchStore(t)
	err := runOneJobExpectError(t, store)
	require.ErrorIs(t, err, worker.ErrLeaseLost)
	assert.Empty(t, store.ProgressAfterLoss())
}
```

Add restart/idempotent replay, disabled-policy invocation, unknown phase,
projection rejection, and source omission when safety redacts structured output.

- [ ] **Step 2: Run RED**

Run: `go test ./internal/worker ./internal/presentation -run 'WebSearch|ProgressPhase' -count=1`

Expected: FAIL because tool execution has no policy/phase/projection contract.

- [ ] **Step 3: Implement generic bounded phase/projector metadata**

```go
type ToolResultProjector interface {
	Project(json.RawMessage) (json.RawMessage, error)
}

type RegisteredTool struct {
	Definition        reasoner.ToolDefinition
	SideEffecting     bool
	ReadOnly          bool
	ParallelSafe      bool
	StableIdempotency bool
	StartedPhase      string
	CompletedPhase    string
	Executor          ToolExecutor
	Projector         ToolResultProjector
}
```

Allow only empty phases or the exact stable enum. Emit `searching` immediately
before `Execute`, `reading_sources` only after validated output, and
`preparing_answer` immediately before the next DeepSeek stream. Use the existing
lease-bound `AppendProgress`; phase mutation IDs remain deterministic.

The execution accumulates at most one 32 KiB public projection. On final text,
marshal `text` plus projected `sources`; never copy query/snippet/private tool
output into the completion projection.

- [ ] **Step 4: Sanitize public progress and completion**

Update `presentation.SanitizeEvent` to expose progress as:

```json
{"phase":"searching","summary":"Searching the web."}
```

Validate exact enum and fixed server-generated summary. For completion, validate
the structured `{text,sources}` object with Task 6 title/URL/source-count bounds
before forwarding it. Internal mutation/revision/tool metadata remains stripped.

- [ ] **Step 5: Register the immutable tool in the worker**

Construct the adapter once in `runWorker`, then:

```go
registered := websearch.RegisteredTool(searchExecutor)
registry, err := worker.NewToolRegistry(map[string][]worker.RegisteredTool{
	configHash: []worker.RegisteredTool{registered},
})
```

Require `cfg.Tools.WebSearch.Enabled`; otherwise register the same version with
an empty list. Do not use feature `side_effecting_tools` for this read-only tool.

- [ ] **Step 6: Run worker/presentation/store gates GREEN**

Run:

```bash
go test -race -count=1 ./internal/worker ./internal/presentation ./cmd/live-worker
go test -race -tags=integration -run '^$' ./internal/store/postgres ./internal/worker
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/worker internal/store/postgres/leases.go internal/store/postgres/leases_integration_test.go internal/presentation cmd/live-worker
git commit -m "feat: execute durable web search jobs"
```

---

### Task 8: Compose `development_durable` Locally and Fail Closed Elsewhere

**Files:**
- Modify: `cmd/live-gateway/main.go`
- Modify: `cmd/live-gateway/main_test.go`
- Modify: `cmd/live-worker/main.go`
- Modify: `cmd/live-worker/main_test.go`
- Create: `deploy/config/development-durable.yaml`
- Create: `deploy/compose.development-durable.yml`
- Modify: `deploy/docker-compose.yml`
- Modify: `deploy/scripts/verify-deploy-static.sh`
- Modify: `deploy/scripts/test-validate-deploy.sh`
- Modify: `deploy/validate/main.go`
- Modify: `deploy/validate/main_test.go`
- Modify: `Makefile`

**Interfaces:**
- Consumes: mode helpers and catalog from Task 1, migration v8, registry from Task 7.
- Produces: `make durable-demo-up`, `make durable-demo-down`, and a checked local durable config.

- [ ] **Step 1: Write composition RED tests**

```go
func TestDevelopmentDurableUsesStaticAuthAndPostgresDurability(t *testing.T) {
	cfg := validDevelopmentDurableConfig()
	assert.NoError(t, validateComposition(cfg))
	assert.True(t, requiresRuntimeRepository(cfg))
	assert.True(t, allowsStaticAuthenticator(cfg))
	assert.False(t, requiresProductionTLS(cfg))
}

func TestLocalStillRejectsHandoffs(t *testing.T) {
	cfg := validLocalConfig()
	cfg.Features.Handoffs = true
	require.Error(t, validateComposition(cfg))
}
```

Add tests that `development_durable` cannot pass the Kubernetes render validator
or release fixture and that production cannot use static auth/plain WS.

- [ ] **Step 2: Run RED**

Run: `go test ./cmd/live-gateway ./cmd/live-worker ./deploy/validate -run 'DevelopmentDurable|LocalStillRejects' -count=1`

Expected: FAIL because composition recognizes only local/production.

- [ ] **Step 3: Separate durability from transport security decisions**

Use `cfg.IsDurable()` for PostgreSQL pool, migrations, feature store, admission
store, conversation provisioning, reconnect owners, handoff coordinators, and
worker execution. Use `cfg.IsProduction()` only for TLS, OIDC, production metrics
listener, image identity, ingress, and production release checks. Use
`cfg.UsesStaticAuth()` for local static auth.

Set the gateway handoff snapshot to `ToolAllowlist: []string{"web_search"}` only
when the catalog enables it. The registry/version/config fields remain the exact
config hash.

- [ ] **Step 4: Add non-secret durable config and Compose override**

`development-durable.yaml` uses static loopback auth references, PostgreSQL,
reconnect/retention/tool key references, handoffs enabled, fixed search catalog,
and `deepseek-v4-flash`. The override mounts it into gateway/worker, requires
migrate success, exposes only loopback ports, and does not place secret values in
YAML.

Add Make targets that require the existing named environment variables and run:

```bash
docker compose -f deploy/docker-compose.yml -f deploy/compose.development-durable.yml up --build --wait postgres migrate gateway worker
```

Down removes only the explicit `gpt-live-durable-demo` project and never the
canonical `gpt-live` project.

- [ ] **Step 5: Extend static validation**

Verify exact mode/config/model/tool catalog, gateway-only reconnect secret,
worker-only reasoner credential, read-only filesystems, loopback ports, no
plaintext secrets, and explicit project scoping. Add a mutation proving
`development_durable` is rejected from production manifests.

- [ ] **Step 6: Run composition/static gates GREEN**

Run:

```bash
go test -race -count=1 ./cmd/live-gateway ./cmd/live-worker ./deploy/validate
make verify-deploy-static
```

Expected: PASS without starting Docker or using credentials.

- [ ] **Step 7: Commit**

```bash
git add cmd/live-gateway cmd/live-worker deploy Makefile
git commit -m "feat: add durable local tool composition"
```

---

### Task 9: Add the Same-Origin BFF Tool Policy and Diagnostic Surface

**Files:**
- Create: `demo/live-client/tool_policy.go`
- Create: `demo/live-client/tool_policy_test.go`
- Modify: `demo/live-client/server.go`
- Modify: `demo/live-client/server_test.go`
- Modify: `demo/live-client/config.go`
- Modify: `demo/live-client/config_test.go`

**Interfaces:**
- Consumes: Task 1 policy and Task 6 web search executor.
- Produces: `GET /api/tool-policy`, `PUT /api/tool-policy`, and `POST /api/tool-policy/test`.
- The browser sends the returned canonical policy over the gateway WebSocket after `session.updated`.

- [ ] **Step 1: Write BFF RED tests**

```go
func TestToolPolicyAPIStoresOnlyCanonicalSafeFields(t *testing.T) {
	handler := newDemoHandlerWithTools(t)
	put := httptest.NewRequest(http.MethodPut, "/api/tool-policy", strings.NewReader(`{"web_search":{"enabled":true,"safe_search":"moderate","region":"wt-wt","max_results":5,"timeout_ms":8000}}`))
	put.Header.Set("Origin", "http://127.0.0.1:3000")
	put.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, put)
	assert.Equal(t, http.StatusNoContent, response.Code)
}

func TestToolPolicyAPIRejectsExecutableAndCrossOriginInput(t *testing.T) {
	for _, body := range []string{`{"command":"npx"}`, `{"mcpServers":{}}`} {
		assertRejectedToolPolicy(t, body)
	}
}
```

Add 4 KiB body limit, trailing JSON, GET no-store, fixed diagnostic query,
bounded `{ok,result_count}` response, timeout/cancellation, and no query/result
content in logs/errors.

- [ ] **Step 2: Run RED**

Run: `go test ./demo/live-client -run ToolPolicy -count=1`

Expected: FAIL with 404/missing handlers.

- [ ] **Step 3: Implement in-memory canonical policy storage**

Use a mutex and owned `toolpolicy.Policy`; never browser storage. Require exact
Origin on state-changing requests and `application/json`. `PUT` canonicalizes
against the BFF's fixed catalog. `POST /test` ignores request body, runs the
fixed query `DuckDuckGo privacy search`, and returns only:

```json
{"ok":true,"result_count":3}
```

It uses Task 6's same executor and limits; no bearer or worker admin credential
is involved.

- [ ] **Step 4: Run package race tests GREEN**

Run: `go test -race -count=1 ./demo/live-client`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add demo/live-client/config.go demo/live-client/config_test.go demo/live-client/server.go demo/live-client/server_test.go demo/live-client/tool_policy.go demo/live-client/tool_policy_test.go
git commit -m "feat: expose safe demo tool policy"
```

---

### Task 10: Build the Tools Drawer and Durable Progress Cards

**Files:**
- Create: `demo/live-client/web/tool-state.mjs`
- Create: `demo/live-client/web/tool-state.test.mjs`
- Modify: `demo/live-client/web/protocol-state.mjs`
- Modify: `demo/live-client/web/protocol-state.test.mjs`
- Modify: `demo/live-client/web/client-state.mjs`
- Modify: `demo/live-client/web/client-state.test.mjs`
- Modify: `demo/live-client/web/browser-runtime.mjs`
- Modify: `demo/live-client/web/browser-runtime.test.mjs`
- Modify: `demo/live-client/web/index.html`
- Modify: `demo/live-client/web/styles.css`
- Modify: `demo/live-client/web/app.js`
- Modify: `demo/live-client/server.go`
- Modify: `demo/live-client/server_test.go`

**Interfaces:**
- Consumes: Task 2 wire events and Task 9 same-origin API.
- Produces: `ToolState`, `buildToolPolicyUpdate(policy, eventID)`, and snapshot field `timeline` containing ordered transcript turns and bounded tool cards.
- Keeps all tool effects out of `audio-core.mjs`, capture/playback worklets, and audio scheduling.

- [ ] **Step 1: Write pure-state RED tests**

```js
test('progress updates one card monotonically', () => {
  const tools = new ToolState();
  tools.consume({type: 'live.handoff.created', handoff_id: 'h1', sequence_id: 1, status: 'pending', details: {}});
  tools.consume({type: 'live.handoff.progress', handoff_id: 'h1', sequence_id: 2, status: 'running', details: {phase: 'searching', summary: 'Searching the web.'}});
  tools.consume({type: 'live.handoff.progress', handoff_id: 'h1', sequence_id: 2, status: 'running', details: {phase: 'searching', summary: 'duplicate'}});
  assert.deepEqual(tools.snapshot().cards.map(({id, phase}) => ({id, phase})), [{id: 'h1', phase: 'searching'}]);
});

test('terminal card rejects a later progress regression', () => {
  const tools = completedToolState();
  tools.consume(progressEvent({sequence_id: 99, phase: 'searching'}));
  assert.equal(tools.snapshot().cards[0].phase, 'completed');
});
```

Add 64-active/64-terminal/256-KiB eviction, 8-KiB summary, 10-source,
HTTPS/title/URL bounds, unknown details, accessibility announcement, disabled
policy, save/ack, and reconnect resend tests.

- [ ] **Step 2: Run Node RED**

Run: `node --test demo/live-client/web/tool-state.test.mjs demo/live-client/web/protocol-state.test.mjs demo/live-client/web/browser-runtime.test.mjs`

Expected: FAIL because tool state/effects do not exist.

- [ ] **Step 3: Implement strict policy and handoff effects**

`ProtocolState.consume` returns:

```js
{type: 'tool-policy-accepted', policy: event.tools, model: event.model}
{type: 'handoff-card', event: normalizedHandoffEvent}
```

Only accept the exact phase/status/sequence/details shapes. `ToolState` owns
policy/cards and returns cloned snapshots. `ClientPresentation` assigns one
monotonic local order when a transcript turn or handoff card is first observed;
card updates retain their original position, and `snapshot().timeline` merges
turns/cards by that order. On every `session.updated`,
`browser-runtime.mjs` sends the canonical current policy exactly once for that
socket; the ack unlocks Save status. No tool event calls `clearPlayback`,
`pumpPlayback`, or changes voice status.

At runtime start, fetch `GET /api/tool-policy` with `cache:"no-store"`. Save uses
`PUT /api/tool-policy`; Test uses `POST /api/tool-policy/test`; both use a bounded
AbortController deadline and display only stable success/error text. No policy
or diagnostic response enters browser storage or a URL.

- [ ] **Step 4: Implement the compact accessible drawer and cards**

Add a `details`/button-controlled drawer containing Web Search switch,
SafeSearch select, region select, max-results number, timeout number, Test, and
Save. Model text is read-only `deepseek-v4-flash · thinking disabled`.

Render cards as `article` elements within the chronological transcript using
`textContent`. Create source anchors only after `new URL(url)` confirms HTTPS;
set `target="_blank"` and `rel="noopener noreferrer"`. Announce only stable
phase changes through a polite live region. Keep voice controls visible.

- [ ] **Step 5: Serve the new module and run browser tests GREEN**

Run:

```bash
make test-demo-live-client
```

Expected: Go demo race tests and all Node tests PASS. Existing audio ordering,
playback reservoir, VAD, and barge-in regressions remain green.

- [ ] **Step 6: Commit**

```bash
git add -p demo/live-client/web
git add demo/live-client/server.go demo/live-client/server_test.go
git diff --cached --check
git commit -m "feat: show durable search progress in demo"
```

---

### Task 11: Prove the Durable Vertical Path and Realtime Non-Interference

**Files:**
- Create: `test/integration/web_search_handoff_test.go`
- Create: `test/internal/fakesearch/executor.go`
- Create: `test/internal/fakesearch/executor_test.go`
- Modify: `test/internal/loadgateway/server.go`
- Modify: `test/internal/loadgateway/server_test.go`
- Modify: `Makefile`

**Interfaces:**
- Consumes: all Tasks 1--10.
- Produces deterministic tagged test `TestDurableWebSearchProgressAndVoiceRemainIndependent`.

- [ ] **Step 1: Write the tagged end-to-end RED test**

```go
func TestDurableWebSearchProgressAndVoiceRemainIndependent(t *testing.T) {
	harness := newDurableSearchHarness(t)
	client := harness.ConnectVoice(t)
	client.ApplyPolicy(t, toolpolicy.DefaultPolicy())
	handoffID := client.StartSearchTurn(t, "Find the official Go website")
	client.SendAndReceivePCMWhile(t, handoffID, 50)
	harness.RestartWorker(t)
	card := client.WaitForTerminalCard(t, handoffID)
	assert.Equal(t, []string{"queued", "searching", "reading_sources", "preparing_answer", "completed"}, card.Phases)
	assert.Equal(t, "https://go.dev/", card.Sources[0].URL)
	assert.Zero(t, client.AudioOrderingFailures())
}
```

Add cancellation with late fake result, gateway reconnect/recovery, duplicate
progress, disabled policy, search timeout, DeepSeek failure, worker lease loss,
and no private query/snippet in client-visible frames.

- [ ] **Step 2: Run tagged RED/compile**

Run:

```bash
go test -tags=integration -run '^TestDurableWebSearchProgressAndVoiceRemainIndependent$' ./test/integration -count=1
```

Expected: FAIL until the harness and vertical behavior are complete. If
`TEST_DATABASE_URL` is absent, the test must skip before opening a connection;
CI supplies disposable PostgreSQL.

- [ ] **Step 3: Implement deterministic fake search/DeepSeek boundaries**

The fake search executor implements the exact immutable `ToolExecutor` contract
and returns the same bounded result schema as Task 6 without network access.
Task 6's parser/SSRF/HTTP tests remain the authority for the real adapter. The
fake DeepSeek server requests `web_search`, then consumes the tool output and
streams a final bounded answer. Both record only counters and phase markers;
fixtures use non-sensitive fixed text.

The voice fake continuously echoes ordered watermark PCM during background work
and accepts a barge-in fence. Assert the job path never blocks those frames.

- [ ] **Step 4: Run integration and realtime regressions GREEN**

Run:

```bash
go test -race -count=1 ./test/internal/fakesearch ./test/internal/loadgateway
go test -race -tags=integration -run '^TestDurableWebSearch' ./test/integration -count=1 -timeout=10m
make test-demo-live-client
```

Expected: PASS with PostgreSQL configured; otherwise only the tagged runtime
test cleanly skips before connection and the compile/discovery command passes.

- [ ] **Step 5: Commit**

```bash
git add test/integration/web_search_handoff_test.go test/internal/fakesearch test/internal/loadgateway Makefile
git commit -m "test: verify durable realtime web search"
```

---

### Task 12: Write the Complete Operator Guide and Package the Local Skill

**Files:**
- Modify: `README.md`
- Modify: `docs/demo/realtime-browser-client.md`
- Modify: `deploy/README.md`
- Modify: `docs/protocol.md`
- Modify: `configs/worker.example.yaml`
- Modify: `deploy/config/local.yaml`
- Modify: `deploy/config/production.yaml`
- Modify: `deploy/k8s/base/production.yaml`
- Modify: `deploy/k8s/overlays/local/local.yaml`
- Modify: `skills/gpt-live-go-framework/SKILL.md`
- Modify: `skills/gpt-live-go-framework/references/configuration.md`
- Create: `skills/gpt-live-go-framework/references/realtime-web-search.md`
- Create: `skills/gpt-live-go-framework/references/verification.md`
- Modify: `/Users/chuanmingliu/.codex/skills/gpt-live-go-framework/SKILL.md`
- Modify: `/Users/chuanmingliu/.codex/skills/gpt-live-go-framework/references/configuration.md`
- Create: `/Users/chuanmingliu/.codex/skills/gpt-live-go-framework/references/realtime-web-search.md`
- Create: `/Users/chuanmingliu/.codex/skills/gpt-live-go-framework/references/verification.md`
- Modify: `demo/live-client/docs_test.go`
- Modify: `deploy/scripts/verify-deploy-static.sh`

**Interfaces:**
- Consumes verified implementation behavior only.
- Produces exact speech, worker, PostgreSQL, demo, policy, privacy, and future reviewed-MCP instructions.

- [ ] **Step 1: Read the required skill-authoring workflow**

Run: `sed -n '1,260p' /Users/chuanmingliu/.codex/plugins/cache/claude-plugins-official/superpowers/6.2.0/skills/writing-skills/SKILL.md`

Expected: the implementer follows its tests and packaging rules before editing
the local skill.

- [ ] **Step 2: Write docs RED tests for exact safe instructions**

```go
func TestRealtimeGuideUsesV4FlashAndNeverDynamicMCPInstall(t *testing.T) {
	guide := readRepoFile(t, "../../docs/demo/realtime-browser-client.md")
	assert.Contains(t, guide, "deepseek-v4-flash")
	assert.Contains(t, guide, "development_durable")
	assert.NotContains(t, guide, "@modelcontextprotocol/server-duckduckgo")
	assert.NotContains(t, guide, "npx -y")
}
```

Add checks for external secret-reference names, exact startup/health commands,
Tools drawer fields, stable phases, no browser credentials, and manual audible
voice/barge-in acceptance.

- [ ] **Step 3: Run RED**

Run: `go test ./demo/live-client -run Guide -count=1`

Expected: FAIL because the guide does not contain the durable tool setup.

- [ ] **Step 4: Update every active model/config example**

Replace active `deepseek-chat`/`deepseek-reasoner` values with
`deepseek-v4-flash` in current examples, deploy configs, fixtures, and operator
docs. Do not rewrite historical approved design documents. Document that the
speech and worker credentials may be the same provider account but remain
separate process secret references.

- [ ] **Step 5: Write the step-by-step operator flow**

The guide must include, in order:

1. create/export PostgreSQL, static bearer, admin, retention, reconnect, privacy,
   tool encryption, speech, and DeepSeek secrets without echoing values;
2. migrate PostgreSQL to schema v8;
3. start the speech WebSocket on the configured URL with
   `deepseek-v4-flash` and thinking disabled;
4. start durable gateway and worker and verify content-free readiness;
5. start the BFF and open `http://127.0.0.1:3000/`;
6. configure the Tools drawer, run the fixed Test search, save, and speak a
   search request;
7. verify progress cards while continuing to speak/barge in;
8. stop and verify gateway/worker/speech pools are idle; and
9. troubleshoot PostgreSQL, worker, search timeout, browser audio, and policy
   rejection without printing secrets.

Explain that reviewed MCP adapters are compiled/operator-owned future catalog
entries; raw `mcpServers` JSON is intentionally rejected.

- [ ] **Step 6: Update and validate the local skill**

Keep `SKILL.md` concise and route tool work to the new reference. The reference
must reproduce the exact bounds, mode, model, startup sequence, progress states,
privacy boundary, and verification commands. Add a skill test or static script
that rejects dynamic package instructions and retired model identifiers.

- [ ] **Step 7: Install the verified local skill package**

After the repository skill content passes its own checks, apply the same exact
content to `/Users/chuanmingliu/.codex/skills/gpt-live-go-framework`. Do not copy
unrelated files or credentials. Verify byte equality:

```bash
diff -qr skills/gpt-live-go-framework /Users/chuanmingliu/.codex/skills/gpt-live-go-framework
```

Expected: no output and exit 0.

- [ ] **Step 8: Run docs/static/skill gates GREEN**

Run:

```bash
go test -count=1 ./demo/live-client
make verify-deploy-static
rg -n 'deepseek-chat|deepseek-reasoner|npx -y|server-duckduckgo' README.md docs/demo deploy/config deploy/k8s configs skills/gpt-live-go-framework
```

Expected: tests/static verification PASS. The final `rg` may match explicit
warning text only; inspect each match and reject any executable instruction or
active configuration.

- [ ] **Step 9: Commit the repository skill and docs**

```bash
git add README.md docs/demo/realtime-browser-client.md deploy/README.md docs/protocol.md configs/worker.example.yaml deploy/config deploy/k8s skills/gpt-live-go-framework demo/live-client/docs_test.go deploy/scripts/verify-deploy-static.sh
git commit -m "docs: add durable web search runbook"
```

---

### Task 13: Final Verification and Manual Realtime Acceptance

**Files:**
- Modify only if a gate exposes a defect: files already owned by Tasks 1--12.
- Create: `.superpowers/sdd/2026-08-11-realtime-async-web-search-tools/verification-report.md` (ignored evidence ledger; do not commit unless repository policy changes).

**Interfaces:**
- Consumes the complete feature.
- Produces fresh, exact source/test/static/manual evidence and an honest external-authority list.

- [ ] **Step 1: Run focused stress/race tests**

Run:

```bash
go test -race -count=20 ./internal/toolpolicy ./internal/tools/websearch ./internal/reasoner/deepseek ./internal/worker ./internal/presentation ./demo/live-client
node --test demo/live-client/web/*.test.mjs
```

Expected: PASS.

- [ ] **Step 2: Run full repository gates**

Run:

```bash
go test -race -count=1 ./...
go vet ./...
go build ./...
go mod tidy -diff
make verify-deploy-static
git diff --check
```

Expected: PASS. If loopback tests are sandbox-denied, rerun the exact command
with loopback permission and record both outcomes.

- [ ] **Step 3: Run tagged compile/discovery gates**

Run:

```bash
go test -race -tags=integration -run '^$' ./...
go test -race -tags=contract -run '^$' ./...
go test -race -tags=chaos -run '^$' ./...
go test -race -tags=load -run '^$' ./...
go test -race -tags=soak -run '^$' ./...
go test -race -tags=liveprovider -run '^$' ./...
```

Expected: PASS without external connections.

- [ ] **Step 4: Run privacy and scope scans**

Run:

```bash
rg -n 'console\.(log|debug)|localStorage|sessionStorage|document\.cookie|reasoning_content|Authorization.*query|api[_-]?key.*json' demo internal cmd deploy
rg -n 'exec\.Command|os/exec|npx|mcpServers|server-duckduckgo' internal/tools demo cmd
```

Expected: only tests, deny lists, server-side header construction, and explicit
documentation warnings. Review every match; no raw content or dynamic execution
path may remain.

- [ ] **Step 5: Run the disposable durable smoke when authorized**

With operator-provided secret references and an explicitly isolated Compose
project:

```bash
make durable-demo-up
make demo-live-client
```

Open `http://127.0.0.1:3000/`, grant microphone/speaker permission, run Test
search, save policy, start voice, ask a non-sensitive search question, continue
speaking during search, barge in over assistant audio, and verify one ordered
progress card reaches Completed with safe sources. Stop and confirm gateway,
worker, PostgreSQL task ownership, BFF, and speech provider pool are idle.

Physical audibility and acoustic barge-in are operator observations; automation
must not claim them.

- [ ] **Step 6: Record evidence and commit any gate-driven fixes separately**

Write exact commands, exit codes, hashes, durations, skips, and external
authorities to the ignored verification report. If a gate required a source fix,
repeat its RED/GREEN cycle and commit only that cohesive fix before rerunning the
entire final gate set.

External live DuckDuckGo, DeepSeek, physical audio, and PostgreSQL/Compose tests
remain explicitly unclaimed unless actually run with operator authority.
