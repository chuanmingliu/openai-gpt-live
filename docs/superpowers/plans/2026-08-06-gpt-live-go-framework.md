# GPT-Live Go Framework Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a production-shaped Go framework that exposes an OpenAI-compatible Realtime WebSocket, keeps voice traffic on a bounded low-latency path through the existing speech-to-speech simulator, and delegates durable asynchronous work to DeepSeek-backed workers.

**Architecture:** Two Go binaries share provider-neutral domain packages. live-gateway owns downstream and upstream WebSockets, a single-owner session reactor, explicit handoff acceptance, and result presentation. live-worker leases durable jobs from PostgreSQL, streams DeepSeek, runs approved tools, and persists fenced results. PostgreSQL transactions plus an outbox are the only durable coordination mechanism; raw audio and partial model output never enter it.

**Tech Stack:** Go 1.26, github.com/coder/websocket, pgx/v5, PostgreSQL 16, DeepSeek's OpenAI-compatible Chat Completions SSE API, OpenTelemetry, Prometheus, Docker Compose, Kubernetes/Kustomize, and the local speech-to-speech /v1/realtime WebSocket.

**Approved design:** [2026-08-06-gpt-live-go-framework-design.md](../specs/2026-08-06-gpt-live-go-framework-design.md)

## Global Constraints

- Module path is github.com/chuanmingliu/openai-gpt-live and the minimum Go version is 1.26.
- Do not edit or import runtime code from /Users/chuanmingliu/speech-to-speech. Treat its /v1/realtime service as an external provider contract.
- The WebSocket audio contract is mono PCM16 little-endian at 24 kHz, base64 encoded. One 20 ms accounting frame is exactly 960 bytes.
- Default queue capacities are 25 audio frames ingress, 75 audio frames egress, 256 reactor inputs, and 64 writer-control events.
- Exactly one goroutine reads and exactly one goroutine writes each WebSocket. No database, provider, encoding, decoding, or tool call runs synchronously in the reactor.
- Every outbound audio frame is checked against the current incarnation and response epoch immediately before the socket write.
- Raw PCM, partial transcripts, and DeepSeek token deltas are never persisted. Finalized transcript text may be persisted under the configured retention policy but is never logged. Credentials and plaintext tool arguments are never logged; replayable side-effecting tool arguments are persisted only as bounded AES-256-GCM ciphertext plus a canonical hash and key ID.
- Configuration labels the speech-to-speech adapter as the simulated realtime voice role and DeepSeek deepseek-chat as the simulated asynchronous GPT-5.5 role; these labels never claim model equivalence.
- Default unit tests use no network, Docker, wall-clock sleeps, or cloud credentials. Integration, contract, live-provider, chaos, load, and soak suites use explicit build tags.
- Every implementation task starts with a failing test, ends with focused tests plus the stated broader verification, and creates one scoped commit. Never stage the unrelated workspace file .zshrc.
- New external dependencies are added only in the task that first uses them, followed by go mod tidy and govulncheck review.
- Feature work stops on race findings, stale-epoch audio, cross-session contamination, or loss of an acknowledged handoff.

---

## Exact File Map

The implementation creates or intentionally modifies the following paths:

~~~text
.dockerignore
.github/workflows/ci.yml
.github/workflows/nightly.yml
.github/workflows/release-gate.yml
.gitignore
Dockerfile
Makefile
README.md
go.mod
go.sum

cmd/live-gateway/main.go
cmd/live-load/main.go
cmd/live-migrate/main.go
cmd/live-release-gate/main.go
cmd/live-worker/main.go

configs/gateway.example.yaml
configs/worker.example.yaml

internal/admin/http.go
internal/admin/http_test.go
internal/admission/controller.go
internal/admission/controller_test.go
internal/admission/origin.go
internal/admission/postgres.go
internal/admission/postgres_integration_test.go
internal/admission/quota.go
internal/auth/auth.go
internal/auth/auth_test.go
internal/auth/oidc.go
internal/auth/oidc_test.go
internal/auth/static.go
internal/clock/clock.go
internal/clock/manual.go
internal/clock/manual_test.go
internal/config/config.go
internal/config/config_test.go
internal/config/hash.go
internal/config/hash_test.go
internal/feature/gates.go
internal/feature/gates_test.go
internal/feature/postgres.go
internal/feature/postgres_integration_test.go
internal/gateway/app.go
internal/gateway/app_test.go
internal/gateway/handoffs.go
internal/gateway/handoffs_test.go
internal/gateway/transcripts.go
internal/gateway/transcripts_test.go
internal/handoff/approval.go
internal/handoff/approval_test.go
internal/handoff/errors.go
internal/handoff/event_pump.go
internal/handoff/event_pump_test.go
internal/handoff/repository.go
internal/handoff/service.go
internal/handoff/service_test.go
internal/handoff/state.go
internal/handoff/state_test.go
internal/handoff/tools.go
internal/handoff/tools_test.go
internal/handoff/types.go
internal/health/http.go
internal/health/http_test.go
internal/health/registry.go
internal/identity/identity.go
internal/lifecycle/drainer.go
internal/lifecycle/drainer_test.go
internal/loadgen/metrics.go
internal/loadgen/runner.go
internal/loadgen/runner_test.go
internal/presentation/policy.go
internal/presentation/policy_test.go
internal/protocol/client.go
internal/protocol/capabilities.go
internal/protocol/capabilities_test.go
internal/protocol/codec.go
internal/protocol/codec_test.go
internal/protocol/fuzz_test.go
internal/protocol/server.go
internal/realtime/s2sws/connector.go
internal/realtime/s2sws/connector_test.go
internal/realtime/s2sws/mapping.go
internal/realtime/voice.go
internal/reasoner/deepseek/client.go
internal/reasoner/deepseek/client_test.go
internal/reasoner/deepseek/config.go
internal/reasoner/deepseek/errors.go
internal/reasoner/deepseek/sse.go
internal/reasoner/deepseek/sse_test.go
internal/reasoner/reasoner.go
internal/releasegate/check.go
internal/releasegate/check_test.go
internal/releasegate/evidence.go
internal/retention/sweeper.go
internal/retention/sweeper_test.go
internal/safety/policy.go
internal/safety/policy_test.go
internal/safety/privacy.go
internal/safety/privacy_test.go
internal/safety/static.go
internal/session/effect.go
internal/session/epoch.go
internal/session/input.go
internal/session/manager.go
internal/session/manager_test.go
internal/session/reactor.go
internal/session/reactor_test.go
internal/session/recovery.go
internal/session/recovery_test.go
internal/session/state.go
internal/store/postgres/conversations.go
internal/store/postgres/conversations_integration_test.go
internal/store/postgres/delivery_cursors.go
internal/store/postgres/delivery_cursors_integration_test.go
internal/store/postgres/events.go
internal/store/postgres/events_integration_test.go
internal/store/postgres/handoffs.go
internal/store/postgres/handoffs_integration_test.go
internal/store/postgres/leases.go
internal/store/postgres/leases_integration_test.go
internal/store/postgres/listener.go
internal/store/postgres/outbox.go
internal/store/postgres/outbox_integration_test.go
internal/store/postgres/approvals.go
internal/store/postgres/approvals_integration_test.go
internal/store/postgres/repository.go
internal/store/postgres/retention.go
internal/store/postgres/retention_integration_test.go
internal/store/postgres/schema_integration_test.go
internal/store/postgres/tool_invocations.go
internal/store/postgres/tool_invocations_integration_test.go
internal/telemetry/correlation.go
internal/telemetry/metrics.go
internal/telemetry/metrics_test.go
internal/telemetry/redact.go
internal/telemetry/redact_test.go
internal/telemetry/runtime.go
internal/testkit/audio.go
internal/testkit/deepseek_server.go
internal/testkit/fake_voice.go
internal/testkit/jwks.go
internal/testkit/postgres.go
internal/transport/ws/conn.go
internal/transport/ws/handler.go
internal/transport/ws/handler_test.go
internal/worker/heartbeat.go
internal/worker/retry.go
internal/worker/retry_test.go
internal/worker/runner.go
internal/worker/runner_test.go
internal/worker/store.go
internal/worker/tools.go

migrations/000001_durable_handoffs.down.sql
migrations/000001_durable_handoffs.up.sql
migrations/000002_runtime_flags.down.sql
migrations/000002_runtime_flags.up.sql
migrations/embed.go

deploy/compose.chaos.yml
deploy/docker-compose.yml
deploy/k8s/base/configmap.yaml
deploy/k8s/base/gateway-deployment.yaml
deploy/k8s/base/gateway-service.yaml
deploy/k8s/base/hpa.yaml
deploy/k8s/base/ingress.yaml
deploy/k8s/base/kustomization.yaml
deploy/k8s/base/migrate-job.yaml
deploy/k8s/base/network-policy.yaml
deploy/k8s/base/pdb.yaml
deploy/k8s/base/worker-deployment.yaml
deploy/k8s/overlays/local/kustomization.yaml
deploy/k8s/overlays/local/patch.yaml
deploy/k8s/overlays/production/kustomization.yaml
deploy/k8s/overlays/production/patch.yaml
deploy/k8s/secrets.example.env
deploy/observability/alerts.yaml
deploy/observability/grafana-dashboard.json

docs/limitations.md
docs/operations.md
docs/protocol.md
docs/rollout.md
docs/security.md
docs/testing.md
docs/runbooks/database-outage.md
docs/runbooks/queue-overload.md
docs/runbooks/rollback.md
docs/runbooks/voice-provider-outage.md

test/chaos/scenarios_test.go
test/contract/s2s_realtime_test.go
test/integration/e2e_test.go
test/integration/recovery_test.go
test/liveprovider/deepseek_test.go
test/load/gateway_test.go
test/load/soak_test.go
~~~

---

## Task 1: Bootstrap the module, identities, clock, and typed configuration

**Files**

- Create: go.mod
- Create: go.sum
- Create: .gitignore
- Create: Makefile
- Create: internal/identity/identity.go
- Create: internal/clock/clock.go
- Create: internal/clock/manual.go
- Create: internal/clock/manual_test.go
- Create: internal/config/config.go
- Create: internal/config/config_test.go
- Create: configs/gateway.example.yaml
- Create: configs/worker.example.yaml

**Stable interfaces**

~~~go
package identity

type SessionID string
type ConversationID string
type HandoffID string
type Incarnation uint64
type ResponseEpoch uint64

type TurnKey struct {
	ID       string
	Revision uint32
}

type Metadata struct {
	SessionID      SessionID
	ConversationID ConversationID
	Incarnation    Incarnation
	ResponseEpoch  ResponseEpoch
	Turn            *TurnKey
	Sequence        uint64
}
~~~

~~~go
package clock

type Clock interface {
	Now() time.Time
	NewTicker(time.Duration) Ticker
}

type Ticker interface {
	C() <-chan time.Time
	Stop()
}

type JitterSource interface {
	Duration(max time.Duration) time.Duration
}
~~~

Configuration loads YAML, expands only named environment references, applies defaults, then validates. Secret values are never accepted directly in checked-in YAML.

~~~go
type Config struct {
	Mode      string          `yaml:"mode"`
	Gateway   GatewayConfig   `yaml:"gateway"`
	Worker    WorkerConfig    `yaml:"worker"`
	Store     StoreConfig     `yaml:"store"`
	Auth      AuthConfig      `yaml:"auth"`
	Admission AdmissionConfig `yaml:"admission"`
	Safety    SafetyConfig    `yaml:"safety"`
	Telemetry TelemetryConfig `yaml:"telemetry"`
	Shutdown  ShutdownConfig  `yaml:"shutdown"`
	Admin     AdminConfig     `yaml:"admin"`
	Features  FeatureConfig   `yaml:"features"`
}

func Load(path string, lookup func(string) (string, bool)) (Config, error)
func (c Config) Validate() error

type WorkerConfig struct {
	Concurrency       int            `yaml:"concurrency"`
	LeaseDuration     time.Duration  `yaml:"lease_duration"`
	HeartbeatInterval time.Duration  `yaml:"heartbeat_interval"`
	PollInterval      time.Duration  `yaml:"poll_interval"`
	RequestTimeout    time.Duration  `yaml:"request_timeout"`
	RetryBase         time.Duration  `yaml:"retry_base"`
	RetryMax          time.Duration  `yaml:"retry_max"`
	MaxRetries        int            `yaml:"max_retries"`
	MaxOutputBytes    int            `yaml:"max_output_bytes"`
	MaxOutputTokens   int            `yaml:"max_output_tokens"`
	MaxToolRounds     int            `yaml:"max_tool_rounds"`
	Reasoner          ReasonerConfig `yaml:"reasoner"`
}

type ReasonerConfig struct {
	Role       string `yaml:"role"`
	Provider   string `yaml:"provider"`
	BaseURL    string `yaml:"base_url"`
	Model      string `yaml:"model"`
	APIKeyEnv  string `yaml:"api_key_env"`
}
~~~

GatewayConfig fixes advertised model, listen/TLS/proxy settings, voice URL/model/key environment name, PCM format, queue limits, WebSocket limits, provider retry budget, storage grace, and reconnect limits. WorkerConfig fixes DeepSeek base URL/model/key environment name, concurrency, lease duration, heartbeat interval, polling, request timeout, retry base/max/count, output byte/token limits, and maximum tool rounds. The remaining structs carry every auth, admission, safety, telemetry, shutdown, admin, retention, and feature key introduced by later tasks; later tasks may add behavior but may not invent untyped configuration.

The checked-in examples use these explicit simulator mappings:

~~~yaml
gateway:
  advertised_model: gpt-live-simulator
  voice:
    role: realtime_voice
    provider: speech_to_speech
    url: ws://127.0.0.1:8000/v1/realtime
    model: s2s-realtime
worker:
  reasoner:
    role: async_gpt_5_5
    provider: deepseek
    base_url: https://api.deepseek.com
    model: deepseek-chat
    api_key_env: DEEPSEEK_API_KEY
~~~

**Steps**

- [ ] Write TestLoadAppliesRealtimeDefaults, TestRejectsNonWebSocketVoiceURL, TestRejectsNonFrameAlignedAudioLimits, TestMissingSecretEnvironmentVariable, and TestManualClockTicker in the listed test files.

~~~go
func TestLoadAppliesRealtimeDefaults(t *testing.T) {
	cfg, err := LoadFixture(t, "gateway:\n  voice:\n    url: ws://127.0.0.1:8000/v1/realtime\n", nil)
	require.NoError(t, err)
	assert.Equal(t, 25, cfg.Gateway.Session.IngressFrames)
	assert.Equal(t, 75, cfg.Gateway.Session.EgressFrames)
	assert.Equal(t, 256, cfg.Gateway.Session.ControlEvents)
	assert.Equal(t, 64, cfg.Gateway.Session.WriterControlEvents)
	assert.Equal(t, 24_000, cfg.Gateway.Audio.SampleRate)
}
~~~

- [ ] Run go test ./internal/config ./internal/clock ./internal/identity and verify it fails because the packages do not exist.
- [ ] Initialize the module, add gopkg.in/yaml.v3 and testify, implement the types/defaults/validation, and make the manual clock advance tickers synchronously without sleeps.
- [ ] Put only environment-variable names in example YAML: S2S_API_KEY, DEEPSEEK_API_KEY, DATABASE_URL, LIVE_STATIC_BEARER_TOKEN, and LIVE_PRIVACY_HMAC_KEY.
- [ ] Add LIVE_TOOL_ENCRYPTION_KEY as a required 32-byte base64 secret when side-effecting tools are enabled. Validate heartbeat interval below half the lease duration, positive worker concurrency, bounded queue sizes, and positive retry/output limits.
- [ ] In mode: production reject ws:// voice URLs, http:// reasoner/OIDC/OTLP URLs, PostgreSQL sslmode=disable, static authentication, wildcard origins, empty trusted-proxy configuration behind ingress, and plaintext secrets in YAML. Local mode permits the checked-in loopback simulator values.
- [ ] Run go mod tidy, go test -race ./internal/config ./internal/clock ./internal/identity, and go vet ./....
- [ ] Commit.

~~~bash
git add go.mod go.sum .gitignore Makefile internal/identity internal/clock internal/config configs
git commit -m "chore: bootstrap GPT-Live Go module"
~~~

---

## Task 2: Implement bounded Realtime protocol DTOs and PCM validation

**Files**

- Create: internal/protocol/client.go
- Create: internal/protocol/capabilities.go
- Create: internal/protocol/capabilities_test.go
- Create: internal/protocol/server.go
- Create: internal/protocol/codec.go
- Create: internal/protocol/codec_test.go
- Create: internal/protocol/fuzz_test.go
- Create: internal/testkit/audio.go

**Stable interfaces**

~~~go
type Limits struct {
	MaxFrameBytes int
	MaxJSONDepth  int
	MaxAudioBytes int
}

type ClientEvent interface {
	EventType() string
	isClientEvent()
}

type ServerEvent interface {
	EventType() string
	isServerEvent()
}

func DecodeClient(raw []byte, limits Limits) (ClientEvent, error)
func DecodeServer(raw []byte, limits Limits) (ServerEvent, error)
func Encode(event any) ([]byte, error)
func DecodePCM24k(event InputAudioBufferAppendEvent, maxBytes int) ([]byte, error)

type PCMFramer struct {
	tail []byte
}

func NewPCMFramer() *PCMFramer
func (f *PCMFramer) Push(pcm []byte) ([][]byte, error)
func (f *PCMFramer) Flush() ([]byte, error)
func (f *PCMFramer) Reset()

func Capabilities(limits Limits) CapabilityDocument
~~~

Implement typed DTOs for session.update, input_audio_buffer.append, input_audio_buffer.commit, conversation.item.create, response.create, response.cancel, the documented server events, and live.* extensions. Retain unknown provider fields only in explicitly bounded json.RawMessage fields.

**Steps**

- [ ] Write dispatch and validation tests for every supported client event and round-trip tests for every server event.

~~~go
func TestPCMFramerCarriesTailsAcrossAppends(t *testing.T) {
	pcm := testkit.RampPCM(1_920)
	framer := NewPCMFramer()
	first, err := framer.Push(pcm[:480])
	require.NoError(t, err)
	assert.Empty(t, first)
	frames, err := framer.Push(pcm[480:1_440])
	require.NoError(t, err)
	require.Len(t, frames, 1)
	assert.Equal(t, pcm[:960], frames[0])
	tail, err := framer.Flush()
	require.NoError(t, err)
	assert.Equal(t, pcm[960:1_440], tail)
}
~~~

- [ ] Test invalid base64, a sample split across append events, an odd total byte count at commit, a 480-byte final tail, empty commit, framer reset, non-24-kHz session formats, oversized messages, excessive JSON depth, missing type, unknown type, duplicate sequence IDs, and truncated JSON. Never discard a valid even-byte tail.
- [ ] Test CapabilityDocument exact supported events, live.* extensions, PCM contract, configured limits, and protocol version.
- [ ] Seed FuzzDecodeClient with one valid example of each client event and malformed boundary inputs.
- [ ] Run go test ./internal/protocol and verify undefined DTO/codec failures.
- [ ] Implement a depth-scanning decoder before json.Unmarshal, strict required-field checks, base64 decoded-length checks before allocation, stateful 960-byte framing, and the immutable capability document.
- [ ] Run go test ./internal/protocol and go test ./internal/protocol -run=^$ -fuzz=FuzzDecodeClient -fuzztime=10s.
- [ ] Run go test -race ./... and commit.

~~~bash
git add internal/protocol internal/testkit/audio.go
git commit -m "feat: add bounded realtime protocol"
~~~

---

## Task 3: Define provider-neutral voice contracts and deterministic fakes

**Files**

- Create: internal/realtime/voice.go
- Create: internal/testkit/fake_voice.go

**Stable interfaces**

~~~go
type CommandKind uint8

const (
	CommandSessionUpdate CommandKind = iota + 1
	CommandAppendAudio
	CommandCommitAudio
	CommandCreateItem
	CommandCreateResponse
	CommandCancelResponse
	CommandBindResponseEpoch
)

type Command struct {
	Kind      CommandKind
	Sequence  uint64
	AfterSeq  uint64
	Epoch     identity.ResponseEpoch
	Session   SessionConfig
	Audio     []byte
	Item      json.RawMessage
	Response  ResponseConfig
	ResponseID string
}

type OpenRequest struct {
	SessionID   identity.SessionID
	Incarnation identity.Incarnation
	Model       string
}

type AudioFormat struct {
	Encoding   string
	SampleRate int
	Channels   int
}

type SessionConfig struct {
	Instructions  string
	InputFormat   AudioFormat
	OutputFormat  AudioFormat
	Voice         string
	Tools         []Tool
	ToolChoice    json.RawMessage
	TurnDetection json.RawMessage
}

type Tool struct {
	Type        string
	Name        string
	Description string
	Parameters  json.RawMessage
}

type ResponseConfig struct {
	Instructions string
	ToolChoice   json.RawMessage
	Metadata     map[string]json.RawMessage
	Input        json.RawMessage
}

type EventKind uint8

const (
	EventSessionCreated EventKind = iota + 1
	EventSessionUpdated
	EventSpeechStarted
	EventSpeechStopped
	EventTranscriptionDelta
	EventTranscriptionCompleted
	EventResponseCreated
	EventAudioDelta
	EventAudioDone
	EventTranscriptDelta
	EventTranscriptDone
	EventFunctionCallDone
	EventResponseDone
	EventError
)

type Event struct {
	Kind            EventKind
	Epoch           identity.ResponseEpoch
	ProviderEventID string
	ResponseID      string
	ItemID          string
	CallID          string
	Name            string
	Audio           []byte
	Text            string
	Arguments       json.RawMessage
	Status          string
	Reason          string
	Payload         json.RawMessage
	NeedsEpochBinding bool
}

type Session interface {
	Send(context.Context, Command) error
	Receive(context.Context) (Event, error)
	Close(context.Context) error
}

type Connector interface {
	Connect(context.Context, OpenRequest) (Session, error)
}
~~~

~~~go
// internal/testkit/fake_voice.go
func NewFakeConnector(buffer int) *FakeConnector
func (f *FakeConnector) NextSession(context.Context) (*FakeVoiceSession, error)
func (s *FakeVoiceSession) Emit(context.Context, realtime.Event) error
func (s *FakeVoiceSession) NextCommand(context.Context) (realtime.Command, error)
func (s *FakeVoiceSession) Fail(error)
~~~

**Steps**

- [ ] Write tests that assert connect cancellation, ordered command receipt, ordered event delivery, failure injection, and idempotent close without wall-clock sleeps.

~~~go
func TestFakeVoicePreservesCommandOrder(t *testing.T) {
	fake := testkit.NewFakeConnector(4)
	s, err := fake.Connect(t.Context(), realtime.OpenRequest{SessionID: "s1"})
	require.NoError(t, err)
	require.NoError(t, s.Send(t.Context(), realtime.Command{Kind: realtime.CommandAppendAudio, Sequence: 1}))
	require.NoError(t, s.Send(t.Context(), realtime.Command{Kind: realtime.CommandCommitAudio, AfterSeq: 1}))
	server, err := fake.NextSession(t.Context())
	require.NoError(t, err)
	appendCommand, err := server.NextCommand(t.Context())
	require.NoError(t, err)
	commitCommand, err := server.NextCommand(t.Context())
	require.NoError(t, err)
	assert.Equal(t, uint64(1), appendCommand.Sequence)
	assert.Equal(t, uint64(1), commitCommand.AfterSeq)
}
~~~

- [ ] Run go test ./internal/realtime ./internal/testkit and verify failures for missing contracts.
- [ ] Implement fixed-capacity fake channels, context-aware send/receive, and a single close guard. Copy byte slices at package boundaries to prevent test aliasing.
- [ ] Run go test -race ./internal/realtime ./internal/testkit and commit.

~~~bash
git add internal/realtime/voice.go internal/testkit/fake_voice.go
git commit -m "feat: define live voice provider port"
~~~

---

## Task 4: Adapt the speech-to-speech WebSocket provider

**Files**

- Create: internal/realtime/s2sws/connector.go
- Create: internal/realtime/s2sws/mapping.go
- Create: internal/realtime/s2sws/connector_test.go

**Stable interface**

~~~go
type Config struct {
	URL            string
	APIKey         string
	Model          string
	ConnectTimeout time.Duration
	MaxFrameBytes  int64
}

func New(cfg Config) (*Connector, error)
func (c *Connector) Connect(context.Context, realtime.OpenRequest) (realtime.Session, error)
~~~

For explicit response.create, the adapter injects live_epoch into response.metadata and binds the returned response ID immediately. The simulator can also create a response implicitly after VAD/transcription without metadata. In that case the adapter returns response.created with NeedsEpochBinding=true, buffers at most 16 subsequent events or 2 MiB for that response, and releases them only after CommandBindResponseEpoch carries the reactor-selected epoch. Output that arrives without any preceding response.created is a protocol error. The adapter maps duplicate terminal events losslessly; only the reactor de-duplicates them.

**Steps**

- [ ] Build an httptest WebSocket server and write tests for all outbound command mappings and all inbound event mappings.

~~~go
func TestConnectorRejectsAudioForUnknownResponse(t *testing.T) {
	server := newScriptedVoiceServer(t, serverEvent("response.output_audio.delta", map[string]any{
		"response_id": "missing",
		"delta": base64.StdEncoding.EncodeToString(make([]byte, 960)),
	}))
	session := connectTestSession(t, server.URL)
	_, err := session.Receive(t.Context())
	assert.ErrorIs(t, err, s2sws.ErrUnknownResponseEpoch)
}
~~~

- [ ] Add tests for serialized writes under concurrent callers, API key omission when empty, cancellation, oversized frames, malformed base64, speech start/stop, tool arguments, and lossless duplicate response terminals.
- [ ] Add TestImplicitResponseWaitsForReactorEpochBinding: receive metadata-less response.created, verify later audio remains buffered, send CommandBindResponseEpoch, then receive audio stamped with that epoch. Test buffer count/byte overflow and unknown audio without response.created.
- [ ] Run go test ./internal/realtime/s2sws and verify failure.
- [ ] Add github.com/coder/websocket, implement one internal writer loop and one reader loop, bounded codec mapping, ping/close handling, explicit epoch lookup, and the bounded implicit-response binding handshake.
- [ ] Run go test -race ./internal/realtime/s2sws and go test -race ./internal/realtime ./internal/protocol.
- [ ] Commit.

~~~bash
git add go.mod go.sum internal/realtime/s2sws
git commit -m "feat: adapt speech-to-speech websocket"
~~~

---

## Task 5: Build the pure session reactor and response epoch fence

**Files**

- Create: internal/session/input.go
- Create: internal/session/effect.go
- Create: internal/session/state.go
- Create: internal/session/epoch.go
- Create: internal/session/reactor.go
- Create: internal/session/reactor_test.go

**Stable interfaces**

~~~go
type EpochFence struct {
	value atomic.Uint64
}

func NewEpochFence(initial identity.ResponseEpoch) *EpochFence
func (f *EpochFence) Current() identity.ResponseEpoch
func (f *EpochFence) Advance() identity.ResponseEpoch
func (f *EpochFence) Accept(epoch identity.ResponseEpoch) bool

type Reactor struct {
	ids      IDs
	state    State
	fence    *EpochFence
	nextSeq  uint64
}

func NewReactor(ids IDs, fence *EpochFence) *Reactor
func (r *Reactor) Apply(Input) ([]Effect, error)
func (r *Reactor) Snapshot() Snapshot

type IDs struct {
	SessionID      identity.SessionID
	ConversationID identity.ConversationID
	Incarnation    identity.Incarnation
}

type ResponseState uint8

const (
	ResponseIdle ResponseState = iota
	ResponsePending
	ResponseStreaming
	ResponseCancelRequested
	ResponseTerminal
)

type State struct {
	Response       ResponseState
	UserSpeaking   bool
	CommittedTurn  *identity.TurnKey
	LastDurableSeq uint64
}

type Input interface{ isSessionInput() }
type ConnectedInput struct{ ProviderSession json.RawMessage }
type ClientInput struct{ Event protocol.ClientEvent }
type VoiceInput struct{ Event realtime.Event }
type VoiceCommandCompletedInput struct {
	Sequence uint64
	Kind     realtime.CommandKind
}
type DurableInput struct {
	Sequence uint64
	Kind     string
	Payload  json.RawMessage
}
type FailedInput struct {
	Source string
	Err    error
}

type Effect struct {
	ClientEvent  protocol.ServerEvent
	VoiceCommand *realtime.Command
	Async        *AsyncEffect
	FlushBefore  identity.ResponseEpoch
	Close        *CloseEffect
}

type AsyncEffect struct {
	Kind     string
	Sequence uint64
	Payload  json.RawMessage
}

type CloseEffect struct {
	Code   int
	Reason string
}

type Snapshot struct {
	IDs              IDs
	Response         ResponseState
	ResponseEpoch    identity.ResponseEpoch
	UserSpeaking     bool
	CommittedTurn    *identity.TurnKey
	LastDurableSeq   uint64
}
~~~

Response state is idle to pending to streaming to cancel_requested to terminal to idle. The reactor is the sole owner of mutable state. Apply is pure with respect to I/O and returns effects for supervised loops.

**Steps**

- [ ] Write the full transition table and tests for create, response.created, streaming, cancel, terminal, duplicate terminal, provider error, and reset.
- [ ] Write epoch tests for response start, barge-in, explicit cancellation, provider reconnect, old audio, and old incarnation.

~~~go
func TestBargeInFencesQueuedAudio(t *testing.T) {
	fence := NewEpochFence(1)
	r := NewReactor(testIDs(), fence)
	effects, err := r.Apply(VoiceInput{Event: realtime.Event{Kind: realtime.EventSpeechStarted}})
	require.NoError(t, err)
	assert.Equal(t, identity.ResponseEpoch(2), fence.Current())
	assert.Contains(t, effects, Effect{FlushBefore: 2})
	assert.False(t, fence.Accept(1))
}
~~~

- [ ] Test exactly one terminal response event, no epoch advance for a spurious cancel, durable input sequence de-duplication, and bounded pending tool calls awaiting committed transcript.
- [ ] Test metadata-less implicit response.created advances the epoch and emits CommandBindResponseEpoch before any same-response audio is accepted.
- [ ] Test the reactor synthesizes downstream session.created on ConnectedInput and session.updated only after the upstream session-update command completion; do not depend on the simulator emitting session.updated.
- [ ] Run go test ./internal/session and verify missing implementation failures.
- [ ] Implement typed Inputs and Effects, table-driven transition validation, monotonic sequence allocation, and atomic epoch fencing. Do not start goroutines in this package yet.
- [ ] Run go test -race ./internal/session and commit.

~~~bash
git add internal/session/input.go internal/session/effect.go internal/session/state.go internal/session/epoch.go internal/session/reactor.go internal/session/reactor_test.go
git commit -m "feat: enforce session response epochs"
~~~

---

## Task 6: Run bounded session media and control loops

**Files**

- Create: internal/session/manager.go
- Create: internal/session/manager_test.go

**Stable interface**

~~~go
type ClientConn interface {
	Read(context.Context) (protocol.ClientEvent, error)
	Write(context.Context, protocol.ServerEvent) error
	Close(code int, reason string) error
}

type Config struct {
	IngressFrames       int
	EgressFrames        int
	ControlEvents       int
	WriterControlEvents int
	IngressBlockTimeout time.Duration
	EgressBlockTimeout  time.Duration
}

type Subject struct {
	TenantID           string
	UserID             string
	Scopes             map[string]struct{}
	Permissions        json.RawMessage
	ExpiresAt          time.Time
}

type OpenRequest struct {
	Subject          Subject
	RequestedModel   string
	ConversationID   identity.ConversationID
	ReconnectToken   string
}

type IDGenerator interface {
	NewSessionID() identity.SessionID
	NewConversationID() identity.ConversationID
}

type Manager struct {
	cfg       Config
	connector realtime.Connector
	ids       IDGenerator
}

func NewManager(cfg Config, connector realtime.Connector, ids IDGenerator) *Manager
func (m *Manager) Run(context.Context, OpenRequest, ClientConn) error
func (m *Manager) Submit(
	context.Context,
	identity.SessionID,
	identity.Incarnation,
	Input,
) error
~~~

Manager.Run starts five supervised loops with errgroup.WithContext: client reader, voice writer, voice reader, client writer, and reactor. Separate fixed-capacity audio queues bypass the reactor after validation. One PCMFramer belongs to the client-reader loop; commit flushes its valid even-byte tail before CommandCommitAudio. Control commands with AfterSeq wait for the matching media sequence; cancellation may bypass the barrier. Subject is trusted admission output, never reconstructed from client JSON.

**Steps**

- [ ] Write gate-controlled tests for frame splitting and commit ordering, current-epoch forwarding, stale queued audio after barge-in, ingress overflow, egress slow-client failure, control overflow, transport failure teardown, and concurrent session isolation.
- [ ] Add tests for tails spanning multiple append events, commit of a sub-frame even-byte tail, odd total bytes at commit, tenant spoofing in client metadata, and Subject expiry cancelling every loop.

~~~go
func TestManagerDropsQueuedAudioAfterBargeIn(t *testing.T) {
	client := newGateClient()
	manager, voice := newTestManager(t)
	run := startManager(t, manager, client)
	voice.Emit(audioEvent(1, testkit.RampPCM(960)))
	client.WaitUntilWriteBlocked(t)
	voice.Emit(realtime.Event{Kind: realtime.EventSpeechStarted, Epoch: 1})
	voice.Emit(audioEvent(1, testkit.RampPCM(960)))
	client.UnblockWrites()
	client.AssertNoAudioForEpoch(t, 1)
	run.CancelAndWait(t)
}
~~~

- [ ] Assert the stale-audio test uses gates/channels rather than time.Sleep and checks the epoch immediately before ClientConn.Write.
- [ ] Run go test ./internal/session -run TestManager and verify failure.
- [ ] Add golang.org/x/sync/errgroup, implement the five loops, explicit queue ownership, close sync.Once, overflow errors, and metrics hooks with no-op defaults.
- [ ] Run go test -race ./internal/session and repeat the concurrent isolation test 100 times.
- [ ] Commit.

~~~bash
git add go.mod go.sum internal/session/manager.go internal/session/manager_test.go
git commit -m "feat: run bounded realtime sessions"
~~~

---

## Task 7: Expose the downstream WebSocket and complete the first vertical slice

**Files**

- Create: internal/transport/ws/conn.go
- Create: internal/transport/ws/handler.go
- Create: internal/transport/ws/handler_test.go
- Create: internal/gateway/app.go
- Create: internal/gateway/app_test.go
- Create: cmd/live-gateway/main.go

**Stable interfaces**

~~~go
type Options struct {
	MaxFrameBytes  int64
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
}

type Admission interface {
	Admit(
		context.Context,
		*http.Request,
	) (session.Subject, func(), error)
}

type AdmissionFunc func(
	context.Context,
	*http.Request,
) (session.Subject, func(), error)

func NewHandler(opts Options, manager *session.Manager, admission Admission) http.Handler

type Option func(*App) error

func New(cfg config.Config, connector realtime.Connector, opts ...Option) (*App, error)
func WithIDGenerator(ids session.IDGenerator) Option
func (a *App) Run(context.Context) error
~~~

**Steps**

- [ ] Write handler tests for binary-frame rejection, message limit, close codes, context cancellation, one serialized writer, and /v1/realtime routing.
- [ ] Test GET /v1/realtime/capabilities returns the exact protocol capability document without upgrading. Test that admission completes before websocket.Accept and its release function runs exactly once.
- [ ] Write TestGatewayAudioVerticalSlice using a real downstream test WebSocket and FakeConnector.

~~~go
func TestGatewayAudioVerticalSlice(t *testing.T) {
	client, voice := startGatewayHarness(t)
	client.ExpectType(t, "session.created")
	client.AppendPCM(t, testkit.RampPCM(1_920))
	client.CommitAudio(t)
	assert.Equal(t, []uint64{1, 2}, voice.NextAudioSequences(t, 2))
	assert.Equal(t, uint64(2), voice.NextCommand(t.Context()).AfterSeq)
	voice.Emit(realtime.Event{Kind: realtime.EventResponseCreated, ResponseID: "r-1"})
	voice.Emit(audioEvent(1, testkit.RampPCM(960)))
	client.ExpectPCM(t, testkit.RampPCM(960))
	voice.Emit(realtime.Event{Kind: realtime.EventResponseDone, ResponseID: "r-1"})
	client.ExpectResponseDone(t, "r-1")
}
~~~

- [ ] Extend the vertical slice to emit speech_started followed by late epoch-1 audio and assert the client never receives it.
- [ ] Run go test ./internal/transport/ws ./internal/gateway ./cmd/live-gateway and verify failures.
- [ ] Implement the transport adapter, capability endpoint, HTTP server composition, signal-aware executable, configuration loading, and the fake-provider integration path. Local mode uses an explicitly configured trusted development Subject; production startup requires the Task 17 admission controller.
- [ ] Run go test -race ./..., go vet ./..., and go build ./cmd/live-gateway.
- [ ] Commit.

~~~bash
git add internal/transport/ws internal/gateway cmd/live-gateway
git commit -m "feat: expose realtime websocket gateway"
~~~

---

## Task 8: Define the durable handoff domain and reserved tool protocol

**Files**

- Create: internal/handoff/types.go
- Create: internal/handoff/state.go
- Create: internal/handoff/errors.go
- Create: internal/handoff/repository.go
- Create: internal/handoff/service.go
- Create: internal/handoff/state_test.go
- Create: internal/handoff/service_test.go
- Create: internal/handoff/tools.go
- Create: internal/handoff/tools_test.go

**Stable interfaces**

~~~go
type Scope struct {
	TenantID       string
	UserID         string
	ConversationID identity.ConversationID
}

type CreateRequest struct {
	Scope            Scope
	SessionID        identity.SessionID
	SourceTurn       identity.TurnKey
	UserRequest      string
	Objective        string
	ContextRef       string
	ContextHash      string
	Deadline         time.Time
	TokenBudget      int
	Priority         int16
	ToolAllowlist    []string
	ApprovalPolicy   json.RawMessage
	Versions         Versions
	PresentationHint string
	IdempotencyKey   string
}

type Versions struct {
	Model        string
	Prompt       string
	ToolRegistry string
	Config       string
}

type State string

const (
	StatePending    State = "pending"
	StateLeased     State = "leased"
	StateRunning    State = "running"
	StateNeedsInput State = "needs_input"
	StateSucceeded  State = "succeeded"
	StateFailed     State = "failed"
	StateCancelled  State = "cancelled"
	StateExpired    State = "expired"
)

type Snapshot struct {
	ID                identity.HandoffID
	Request           CreateRequest
	RequestHash       string
	State             State
	ObjectiveRevision uint32
	InputRevision     uint32
	ConsumedInput     uint32
	Attempt           uint32
	RetryCount        uint32
	Result            *Result
	Failure           *Failure
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

type Result struct {
	MutationID string
	Summary    string
	Structured json.RawMessage
	Usage      json.RawMessage
}

type Failure struct {
	MutationID string
	Code       string
	Retryable  bool
	Message    string
}

type Event struct {
	Seq            int64
	EventID        string
	HandoffID      identity.HandoffID
	TenantID       string
	ConversationID identity.ConversationID
	Kind           string
	State          State
	Attempt        uint32
	Payload        json.RawMessage
	CreatedAt      time.Time
}

type EventReader interface {
	ReadEvents(context.Context, Scope, int64, int) ([]Event, error)
}

type CreateRecord struct {
	HandoffID identity.HandoffID
	EventID   string
	Request   CreateRequest
	RequestHash string
	CreatedAt time.Time
}

type SteerRequest struct {
	Objective        string
	ExpectedRevision uint32
	Actor            string
	IdempotencyKey   string
}

type InputRequest struct {
	Input            json.RawMessage
	ExpectedRevision uint32
	Actor            string
	IdempotencyKey   string
}

type CancelRequest struct {
	Actor          string
	Reason         string
	IdempotencyKey string
}

type Service interface {
	Create(context.Context, CreateRequest) (Snapshot, error)
	Get(context.Context, Scope, identity.HandoffID) (Snapshot, error)
	ListActive(context.Context, Scope) ([]Snapshot, error)
	Steer(context.Context, Scope, identity.HandoffID, SteerRequest) (Snapshot, error)
	ProvideInput(context.Context, Scope, identity.HandoffID, InputRequest) (Snapshot, error)
	Cancel(context.Context, Scope, identity.HandoffID, CancelRequest) (Snapshot, error)
}

type EventWakeup struct {
	TenantID       string
	ConversationID identity.ConversationID
	HighestSeq      int64
}

type Repository interface {
	IsCommittedTurn(context.Context, Scope, identity.TurnKey) (bool, error)
	CreateHandoff(context.Context, CreateRecord) (Snapshot, bool, error)
	GetHandoff(context.Context, Scope, identity.HandoffID) (Snapshot, error)
	ListActive(context.Context, Scope) ([]Snapshot, error)
	SteerHandoff(context.Context, Scope, identity.HandoffID, SteerRequest) (Snapshot, error)
	ProvideHandoffInput(context.Context, Scope, identity.HandoffID, InputRequest) (Snapshot, error)
	RequestHandoffCancel(context.Context, Scope, identity.HandoffID, CancelRequest) (Snapshot, error)
}
~~~

Allowed states are pending, leased, running, needs_input, succeeded, failed, cancelled, and expired. Every terminal state rejects every later transition.

CreateRecord contains the server-generated handoff/event IDs, validated CreateRequest, canonical request hash, and creation time. SteerRequest contains objective text, expected objective revision, actor, and idempotency key. InputRequest contains correlated input JSON, expected input revision, actor, and idempotency key. CancelRequest contains actor, reason, and idempotency key. Constructors cap user request at 16 KiB, objective at 8 KiB, context reference at 2 KiB, each JSON policy/result at 64 KiB, each allowlist at 64 names, and deep-copy every byte slice/json.RawMessage on input and output.

**Steps**

- [ ] Write table tests for every allowed and forbidden transition, terminal immutability, validation, canonical request hashing, idempotency replay, idempotency conflict, tenant-scoped not-found behavior, and server-generated identifiers.
- [ ] Write parser tests for start_background_task, steer_background_task, and cancel_background_task. Reject model-supplied authoritative IDs on start.

~~~go
func TestCreateRequiresCommittedTurn(t *testing.T) {
	service, repo := newServiceHarness(t)
	turn := identity.TurnKey{ID: "turn-1", Revision: 3}
	repo.SetTurnCommitted(turn, false)
	_, err := service.Create(t.Context(), validCreateRequest(turn))
	assert.ErrorIs(t, err, handoff.ErrUncommittedTurn)
}
~~~

- [ ] Run go test ./internal/handoff and verify failure.
- [ ] Implement immutable requests/snapshots, transition validation, SHA-256 canonical JSON request hashes, injected clock and ID generator, and bounded tool schemas.
- [ ] Ensure the service repository port has no pgx types and that cross-tenant mismatches always return ErrNotFound.
- [ ] Run go test -race ./internal/handoff and commit.

~~~bash
git add internal/handoff
git commit -m "feat: define explicit durable handoffs"
~~~

---

## Task 9: Create the PostgreSQL schema and migration harness

**Files**

- Create: migrations/000001_durable_handoffs.up.sql
- Create: migrations/000001_durable_handoffs.down.sql
- Create: migrations/embed.go
- Create: internal/testkit/postgres.go
- Create: internal/store/postgres/schema_integration_test.go
- Create: cmd/live-migrate/main.go

The migration creates conversations, conversation_items, handoffs, handoff_events, outbox, tool_invocations, delivery_cursors, session_admission_leases, and deletion_audits. Use text plus named CHECK constraints rather than PostgreSQL enums.

- conversations uses composite primary/unique identity (tenant_id, id), user_id, status, retention_until, created_at, and updated_at.
- conversation_items uses tenant/conversation composite foreign keys, session ID, turn ID/revision, monotonic sequence, role, finalized content, content hash, and timestamps; uniqueness covers sequence and turn/revision/role.
- handoffs stores every CreateRequest field, request hash, state, objective/input/consumed-input revisions, ready_at, attempt, retry count, nullable lease owner/expiry, lease_version BIGINT NOT NULL DEFAULT 0, cancellation request, terminal mutation ID, structured result, safety decision, usage, and terminal timestamps.
- handoff_events uses BIGSERIAL seq, unique event_id, tenant/conversation/handoff composite identity, kind/state/attempt, payload capped at 64 KiB, and created_at.
- outbox stores topic, aggregate ID, event sequence, bounded notification payload, created/published times, attempts, and last error with unique (topic, event_seq).
- tool_invocations stores server invocation ID, handoff/attempt/objective revision/tool ordinal/name, approval state/expiry, stable idempotency key, canonical argument hash, AES-GCM ciphertext/nonce/key ID, result metadata, mutation ID, status, and timestamps.
- delivery_cursors keys consumer_id plus tenant/conversation, stores last delivered event sequence, and permits at-least-once replay.
- session_admission_leases keys instance/session, tenant/user, expiry, and heartbeat time and indexes active tenant/user counts.
- deletion_audits stores tenant ID, keyed conversation hash, actor, reason code, and deletion time but no transcript, result, arguments, or raw conversation ID.

**Required schema invariants**

~~~sql
ALTER TABLE handoffs
  ADD CONSTRAINT handoffs_tenant_idempotency_uniq
  UNIQUE (tenant_id, idempotency_key);
ALTER TABLE handoffs
  ADD CONSTRAINT handoffs_state_check
  CHECK (state IN (
    'pending', 'leased', 'running', 'needs_input',
    'succeeded', 'failed', 'cancelled', 'expired'
  ));
ALTER TABLE handoffs
  ADD CONSTRAINT handoffs_token_budget_check CHECK (token_budget > 0);
ALTER TABLE handoffs
  ADD CONSTRAINT handoffs_attempt_check CHECK (attempt >= 0 AND retry_count >= 0);
ALTER TABLE handoffs
  ADD CONSTRAINT handoffs_lease_tuple_check CHECK (
  lease_version >= 0
  AND (
  (lease_owner IS NULL AND lease_expires_at IS NULL)
  OR
  (lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
  )
);

CREATE INDEX handoffs_runnable_idx
  ON handoffs (priority DESC, ready_at, created_at)
  WHERE state IN ('pending', 'running') AND lease_owner IS NULL;

CREATE INDEX handoffs_expired_lease_idx
  ON handoffs (lease_expires_at)
  WHERE lease_owner IS NOT NULL;

CREATE INDEX handoffs_supplied_input_idx
  ON handoffs (updated_at)
  WHERE state = 'needs_input' AND input_revision > consumed_input_revision;

CREATE INDEX handoff_events_cursor_idx
  ON handoff_events (tenant_id, conversation_id, seq);

CREATE INDEX outbox_unpublished_idx
  ON outbox (created_at)
  WHERE published_at IS NULL;
~~~

**Steps**

- [ ] Write integration tests, behind the integration build tag, for every unique/check constraint, tenant foreign keys, global event sequence, required indexes, and repeatable up/down/up migration.
- [ ] Populate 10,000 mixed-state handoffs and use EXPLAIN assertions to prove LeaseNext selects the runnable, expired-lease, and supplied-input indexes rather than scanning the table.

~~~go
//go:build integration

func TestSchemaRejectsPartialLease(t *testing.T) {
	db := testkit.Postgres(t)
	_, err := db.Exec(t.Context(), insertHandoffWithLeaseOwnerOnly)
	assert.Error(t, err)
}
~~~

- [ ] Run TEST_DATABASE_URL=postgres://live:live@127.0.0.1:5432/live?sslmode=disable go test -tags=integration ./internal/store/postgres -run TestSchema and verify missing migration failures.
- [ ] Add github.com/jackc/pgx/v5, github.com/testcontainers/testcontainers-go, github.com/golang-migrate/migrate/v4/source/iofs, and github.com/golang-migrate/migrate/v4/database/pgx/v5. In migrations/embed.go use //go:embed *.sql and expose Up, Down, Steps, and Check functions used by tests and live-migrate.
- [ ] internal/testkit.Postgres uses TEST_DATABASE_URL when it is set; otherwise it starts PostgreSQL 16 through testcontainers. It never falls back from a supplied but unreachable URL to another database.
- [ ] Store finalized transcript content and hashes, but give raw audio, partial transcript, and token-delta fields no schema columns.
- [ ] Make down migration drop objects in reverse dependency order. Make live-migrate support -direction=up, -direction=down, -steps, and -check without printing the database URL.
- [ ] Run the schema suite twice against a fresh PostgreSQL 16 container, then go build ./cmd/live-migrate.
- [ ] Commit.

~~~bash
git add go.mod go.sum migrations internal/testkit/postgres.go internal/store/postgres/schema_integration_test.go cmd/live-migrate
git commit -m "feat: add durable handoff schema"
~~~

---

## Task 10: Persist handoffs atomically and publish a durable event feed

**Files**

- Create: internal/store/postgres/repository.go
- Create: internal/store/postgres/conversations.go
- Create: internal/store/postgres/conversations_integration_test.go
- Create: internal/store/postgres/handoffs.go
- Create: internal/store/postgres/handoffs_integration_test.go
- Create: internal/store/postgres/events.go
- Create: internal/store/postgres/events_integration_test.go
- Create: internal/store/postgres/delivery_cursors.go
- Create: internal/store/postgres/delivery_cursors_integration_test.go
- Create: internal/store/postgres/outbox.go
- Create: internal/store/postgres/outbox_integration_test.go
- Create: internal/store/postgres/listener.go

**Stable interfaces**

~~~go
func (r *Repository) CreateHandoff(
	ctx context.Context,
	record handoff.CreateRecord,
) (snapshot handoff.Snapshot, created bool, err error)

func (r *Repository) AppendCommittedItem(
	ctx context.Context,
	scope handoff.Scope,
	item ConversationItem,
) (ConversationItem, error)

func (r *Repository) ReadEvents(
	ctx context.Context,
	scope handoff.Scope,
	afterSeq int64,
	limit int,
) ([]handoff.Event, error)

func (r *Repository) LoadDeliveryCursor(
	context.Context,
	string,
	handoff.Scope,
) (int64, error)
func (r *Repository) AdvanceDeliveryCursor(
	context.Context,
	string,
	handoff.Scope,
	int64,
) error

func (p *OutboxPublisher) Run(context.Context) error
func (l *Listener) Run(context.Context, chan<- handoff.EventWakeup) error
~~~

CreateHandoff performs one transaction: insert the handoff, append handoff.created, insert handoff.dispatch and handoff.event outbox rows, then commit. An idempotency conflict reloads the existing row under lock and succeeds only when its canonical request hash matches.

**Steps**

- [ ] Write failure-injection integration tests that roll back after each statement, plus crash-equivalent retry after commit but before acknowledgment. Implement and test GetHandoff, ListActive, SteerHandoff, ProvideHandoffInput, and RequestHandoffCancel before Task 11 consumes them.
- [ ] Test duplicate committed items, strict sequence order, cursor replay, page boundaries, tenant filtering, duplicate notifications, and notification loss.
- [ ] Test monotonic delivery cursors, delivery-before-cursor crash (duplicate replay allowed with identical event ID), cursor-before-next-read crash (no skipped event), and identical conversation IDs in different tenants.

~~~go
func TestCreateHandoffRetryAfterCommitReturnsOriginal(t *testing.T) {
	repo := newRepositoryHarness(t)
	first, created, err := repo.CreateHandoff(t.Context(), validRecord())
	require.NoError(t, err)
	assert.True(t, created)
	second, created, err := repo.CreateHandoff(t.Context(), validRecord())
	require.NoError(t, err)
	assert.False(t, created)
	assert.Equal(t, first.ID, second.ID)
}
~~~

- [ ] Run the focused integration suites and verify failures.
- [ ] Implement pgx transactions without an ORM. Bound JSON payloads in application validation and database checks.
- [ ] Have OutboxPublisher claim rows with FOR UPDATE SKIP LOCKED, issue pg_notify, and mark them published in the same transaction. Notification payloads contain tenant ID, conversation ID, and event sequence, never transcript or result content.
- [ ] Inject clock.Clock and clock.JitterSource into OutboxPublisher and Listener. Listener reconnects with bounded jitter and emits wakeups only; ReadEvents remains authoritative and is always polled. Test malformed payload, notification-channel backpressure, cancellation, and reconnect without time.Sleep.
- [ ] Run TEST_DATABASE_URL=postgres://live:live@127.0.0.1:5432/live?sslmode=disable go test -race -tags=integration ./internal/store/postgres and commit.

~~~bash
git add internal/store/postgres internal/handoff/repository.go
git commit -m "feat: persist and publish handoff events"
~~~

---

## Task 11: Connect committed voice turns to durable handoff acceptance

**Files**

- Create: internal/gateway/handoffs.go
- Create: internal/gateway/handoffs_test.go
- Create: internal/gateway/transcripts.go
- Create: internal/gateway/transcripts_test.go
- Modify: internal/session/input.go
- Modify: internal/session/effect.go
- Modify: internal/session/reactor.go
- Modify: internal/session/reactor_test.go
- Modify: internal/gateway/app.go
- Modify: internal/gateway/app_test.go

**Stable interfaces**

~~~go
type HandoffWork struct {
	SessionID   identity.SessionID
	Incarnation identity.Incarnation
	Scope       handoff.Scope
	CallID      string
	ResponseID  string
	ToolName    string
	Arguments   json.RawMessage
	Turn        identity.TurnKey
	Transcript  string
	Snapshot    SessionHandoffSnapshot
}

type SessionHandoffSnapshot struct {
	ContextRef       string
	ContextHash      string
	Deadline         time.Time
	TokenBudget      int
	Priority         int16
	ToolAllowlist    []string
	ApprovalPolicy   json.RawMessage
	Versions         handoff.Versions
	PresentationHint string
}

type TranscriptWork struct {
	SessionID   identity.SessionID
	Incarnation identity.Incarnation
	Scope       handoff.Scope
	Turn        identity.TurnKey
	Sequence    uint64
	Role        string
	FinalText   string
}

type SessionSubmitter interface {
	Submit(
		context.Context,
		identity.SessionID,
		identity.Incarnation,
		session.Input,
	) error
}

type HandoffCoordinator struct {
	service handoff.Service
	submit  SessionSubmitter
	work    chan HandoffWork
}

func NewHandoffCoordinator(handoff.Service, SessionSubmitter, int) *HandoffCoordinator
func (c *HandoffCoordinator) Enqueue(HandoffWork) error
func (c *HandoffCoordinator) Run(context.Context) error

func NewTranscriptCoordinator(
	ConversationWriter,
	SessionSubmitter,
	int,
) *TranscriptCoordinator
func (c *TranscriptCoordinator) Enqueue(TranscriptWork) error
func (c *TranscriptCoordinator) Run(context.Context) error
~~~

The reactor only enqueues bounded work. The transcript coordinator persists finalized user/assistant transcript items and submits TurnCommittedInput only after commit. A pending start tool can attach only after that input. The handoff coordinator performs database I/O outside the session context's media path. Both coordinators derive work from appCtx plus the durable request deadline, never sessionCtx; disconnect suppresses stale-incarnation delivery but does not undo committed work.

After the handoff transaction commits, the coordinator submits acceptance. The reactor retains it by call ID until the originating response is terminal, then sends ordered conversation.item.create(function_call_output) followed by response.create under a new epoch. If the response becomes terminal before persistence completes, acceptance sends immediately when it arrives. A failed transaction produces accepted:false and no durable identifier.

**Steps**

- [ ] Test that a tool request before transcript completion is held, then attached to the committed TurnKey; a superseded revision is rejected.
- [ ] Test finalized user and assistant transcript persistence, duplicate completion, storage failure, and the exact append committed turn to start_background_task to atomic handoff sequence.
- [ ] Test start, steer, and cancel, coordinator overflow, disconnect during persistence, crash-after-commit-before-ack replay, and exactly one accepted output.
- [ ] Test persistence-before-originating-response-terminal and response-terminal-before-persistence races. In both cases function_call_output precedes the new response.create and exactly one new epoch is allocated.
- [ ] Prove HandoffWork.Scope comes only from trusted session.Subject and SessionHandoffSnapshot comes only from server configuration. Model arguments cannot raise token budget/priority, add tools, weaken approval, extend deadline, or replace version identifiers.

~~~go
func TestAcceptedOutputFollowsDurableCommit(t *testing.T) {
	service := newBlockingHandoffService()
	coordinator, submit := newCoordinatorHarness(service)
	work := validHandoffWork()
	submit.SetOriginatingResponseTerminal(work.ResponseID)
	require.NoError(t, coordinator.Enqueue(work))
	service.WaitUntilCreateCalled(t)
	submit.AssertEmpty(t)
	service.Commit(identity.HandoffID("h-1"))
	output := submit.Next(t).(session.HandoffAcceptedInput)
	assert.Equal(t, identity.HandoffID("h-1"), output.HandoffID)
}
~~~

- [ ] Run go test ./internal/gateway ./internal/session and verify failures.
- [ ] Add transcript-persistence, HandoffRequested, and HandoffCompleted effects; fixed-capacity coordinator queues; and idempotency derived from tenant, conversation, committed turn, call ID, and canonical arguments.
- [ ] Ensure barge-in cancels only response generation; it never calls handoff.Cancel. Only cancel_background_task or an authorized client control cancels durable work.
- [ ] Run go test -race ./internal/gateway ./internal/session ./internal/handoff and the fake-provider vertical slice.
- [ ] Commit.

~~~bash
git add internal/gateway internal/session internal/handoff
git commit -m "feat: accept explicit durable voice handoffs"
~~~

---

## Task 12: Implement fenced worker leases, steering, cancellation, and tool idempotency

**Files**

- Create: internal/worker/store.go
- Create: internal/store/postgres/leases.go
- Create: internal/store/postgres/leases_integration_test.go
- Create: internal/store/postgres/tool_invocations.go
- Create: internal/store/postgres/tool_invocations_integration_test.go

**Stable interfaces**

~~~go
type LeaseToken struct {
	HandoffID identity.HandoffID
	WorkerID  string
	Version   int64
	Attempt   uint32
}

type LeaseRequest struct {
	WorkerID     string
	LeaseDuration time.Duration
	Now          time.Time
}

type Lease struct {
	Token LeaseToken
	Job   handoff.Snapshot
}

type Control struct {
	CancelRequested  bool
	ObjectiveRevision uint32
	Objective         string
	InputRevision     uint32
	ConsumedInput     uint32
	Input             json.RawMessage
}

type RetryRecord struct {
	MutationID string
	ReadyAt    time.Time
	RetryCount uint32
	Code       string
}

type NeedsInputRecord struct {
	MutationID string
	Prompt     string
	Schema     json.RawMessage
}

type ProgressRecord struct {
	MutationID string
	Sequence   uint64
	Summary    string
}

type CancelledRecord struct {
	MutationID string
	Reason     string
}

type ToolInvocationRecord struct {
	InvocationID    string
	ObjectiveRevision uint32
	ToolOrdinal     uint32
	ToolName        string
	IdempotencyKey  string
	ArgumentsHash   string
	Ciphertext      []byte
	Nonce           []byte
	KeyID           string
}

type ToolInvocation struct {
	Record ToolInvocationRecord
	Status string
	Result json.RawMessage
}

type ToolCompletionRecord struct {
	MutationID string
	Result     json.RawMessage
}

type Store interface {
	LeaseNext(context.Context, LeaseRequest) (Lease, bool, error)
	MarkRunning(context.Context, LeaseToken, time.Time) error
	Heartbeat(context.Context, LeaseToken, time.Time) (Control, error)
	ScheduleRetry(context.Context, LeaseToken, RetryRecord) error
	AppendProgress(context.Context, LeaseToken, ProgressRecord) error
	MarkNeedsInput(context.Context, LeaseToken, NeedsInputRecord) error
	Complete(context.Context, LeaseToken, handoff.Result) error
	Fail(context.Context, LeaseToken, handoff.Failure) error
	MarkCancelled(context.Context, LeaseToken, CancelledRecord) error
	BeginToolInvocation(context.Context, LeaseToken, ToolInvocationRecord) (ToolInvocation, bool, error)
	CompleteToolInvocation(context.Context, LeaseToken, ToolCompletionRecord) error
	SweepExpired(context.Context, time.Time, int) (int, error)
}
~~~

Every mutation after leasing includes handoff ID, lease owner, lease version, and execution attempt in its WHERE clause. Zero rows affected returns worker.ErrLeaseLost unless the same terminal mutation ID is already committed, in which case the retry succeeds idempotently. lease_version is monotonically incremented and never reset when owner/expiry are cleared.

**Steps**

- [ ] Write integration tests for SKIP LOCKED concurrency, lease version increment, heartbeat extension, expired lease recovery, stale completion rejection, pending cancellation, running cancellation, steering revision, and supplied input revision.
- [ ] Add release/reacquire ABA coverage, cancellation-versus-completion, steering-versus-terminal, duplicate/out-of-order input revisions, expiry during tool execution, sweeper-versus-heartbeat, shutdown with held leases, and a proof that active leases never exceed Runner.Concurrency.
- [ ] For ScheduleRetry, keep state running, clear owner/expiry, preserve and later increment lease_version, and set future ready_at. LeaseNext may reclaim pending/retry-ready rows, expired active leases, or needs_input only when input_revision is greater than consumed_input_revision.
- [ ] Test the tool invocation key and replay behavior.

~~~text
sha256(
  handoff_id | objective_revision | tool_ordinal |
  tool_name | canonical_arguments_hash
)
~~~

- [ ] Verify a matching duplicate returns the stored result and a mismatched request hash returns ErrIdempotencyConflict.
- [ ] Run the focused integration suite and verify failures.
- [ ] Implement leases as short transactions. Heartbeat returns control changes and extends the lease; lease loss cancels reasoner/tool contexts.
- [ ] Make every ScheduleRetry, AppendProgress, MarkNeedsInput, Complete, Fail, MarkCancelled, and SweepExpired transaction update the handoff, append a uniquely identified event, and insert its outbox row atomically. Add rollback-after-each-statement and terminal commit-success/response-loss/retry tests.
- [ ] Generate the stable invocation ID before any side effect. Store bounded canonical arguments as AES-256-GCM ciphertext with nonce/key ID plus plaintext SHA-256 hash; never store plaintext or emit it to logs. Require side-effecting Tool.Execute to accept and honor the stable idempotency key, or reject its registration.
- [ ] Enforce that a tool exists in both the immutable handoff allowlist and the process registry, and that required approval is durably recorded before invocation.
- [ ] Run TEST_DATABASE_URL=postgres://live:live@127.0.0.1:5432/live?sslmode=disable go test -race -tags=integration ./internal/store/postgres -run 'Test(Lease|Cancellation|Steering|Tool)' and commit.

~~~bash
git add internal/worker/store.go internal/store/postgres/leases.go internal/store/postgres/leases_integration_test.go internal/store/postgres/tool_invocations.go internal/store/postgres/tool_invocations_integration_test.go
git commit -m "feat: add fenced durable worker leases"
~~~

---

## Task 13: Implement the DeepSeek streaming reasoner adapter

**Files**

- Create: internal/reasoner/reasoner.go
- Create: internal/reasoner/deepseek/config.go
- Create: internal/reasoner/deepseek/client.go
- Create: internal/reasoner/deepseek/sse.go
- Create: internal/reasoner/deepseek/errors.go
- Create: internal/reasoner/deepseek/client_test.go
- Create: internal/reasoner/deepseek/sse_test.go
- Create: internal/testkit/deepseek_server.go
- Create: internal/safety/privacy.go
- Create: internal/safety/privacy_test.go

**Stable interface**

~~~go
type Streamer interface {
	Stream(
		ctx context.Context,
		request Request,
		onChunk func(Chunk) error,
	) (Result, error)
}

type Request struct {
	Model           string
	Messages        []Message
	Tools           []ToolDefinition
	MaxOutputTokens int
	SafetyUserID    string
}

type Chunk struct {
	Text      string
	ToolCalls []ToolCallDelta
}

type Result struct {
	Text              string
	ToolCalls         []ToolCall
	FinishReason      string
	ProviderRequestID string
	Usage             Usage
}

type ToolCallDelta struct {
	Index     int
	ID        string
	Name      string
	Arguments string
}

type ToolCall struct {
	ID        string
	Name      string
	Arguments json.RawMessage
}
~~~

Request contains stateless messages, model, maximum output tokens, tools, and the privacy-preserving safety user ID. Chunk exposes answer text and tool-call fragments, but not reasoning_content. Result contains final answer, structured tool calls, finish reason, provider request ID, and usage.

**Steps**

- [ ] Script an httptest server for normal SSE plus [DONE], comments, blank lines, usage-only chunks, arbitrary read splits, 429 with Retry-After, 500, nonretryable 400, stall/cancel, malformed JSON, premature EOF, and insufficient_system_resource.
- [ ] Add fragmented tool-call arguments by index/ID, multiple tool calls, duplicate call IDs, malformed completed arguments, assistant/tool continuation messages, maximum tool-round exhaustion, aggregate output limit, and a policy that executes tool calls sequentially by index unless a registry explicitly marks a read-only tool parallel-safe.

~~~go
func TestStreamTreatsTruncatedSSEAsRetryable(t *testing.T) {
	server := testkit.NewDeepSeekServer(t, testkit.TruncatedStream())
	client := deepseek.New(testConfig(server.URL), server.Client())
	_, err := client.Stream(t.Context(), validRequest(), func(reasoner.Chunk) error { return nil })
	var providerErr *reasoner.ProviderError
	require.ErrorAs(t, err, &providerErr)
	assert.True(t, providerErr.Retryable)
}
~~~

- [ ] Run go test ./internal/reasoner/deepseek and verify failure.
- [ ] Implement POST /chat/completions with stream:true, data-only SSE parsing, a 1 MiB event limit, body close on every path, prompt/result size bounds, tool-call assembly, and prompt cancellation.
- [ ] Classify 408, 429, transport failures, truncated streams, insufficient_system_resource, and 5xx as retryable. Classify other 4xx as permanent. Parse seconds or HTTP-date Retry-After.
- [ ] Never persist or log reasoning_content or raw token chunks.
- [ ] Implement ProviderSafetyID here, before provider composition, as HMAC-SHA256 over length-delimited tenant/user IDs with unpadded base64url output. Tests prove raw tenant/user IDs never enter serialized DeepSeek requests.
- [ ] Run go test -race ./internal/reasoner/deepseek and commit.

~~~bash
git add internal/reasoner internal/testkit/deepseek_server.go internal/safety/privacy.go internal/safety/privacy_test.go
git commit -m "feat: add DeepSeek streaming reasoner"
~~~

---

## Task 14: Execute durable jobs in the live-worker binary

**Files**

- Create: internal/worker/retry.go
- Create: internal/worker/retry_test.go
- Create: internal/worker/heartbeat.go
- Create: internal/worker/tools.go
- Create: internal/worker/runner.go
- Create: internal/worker/runner_test.go
- Create: internal/safety/policy.go
- Create: cmd/live-worker/main.go

**Stable interface**

~~~go
type Runner struct {
	Store         Store
	Reasoner      reasoner.Streamer
	Safety        safety.Policy
	Tools         ToolRegistry
	Clock         clock.Clock
	Jitter        clock.JitterSource
	Retry         RetryPolicy
	WorkerID      string
	LeaseDuration time.Duration
	Concurrency   int
}

func (r *Runner) Run(context.Context) error
~~~

Retry uses full jitter over min(maximum, base times two to retryCount), honors a longer valid Retry-After, never crosses the handoff deadline, and never retries permanent provider errors. Steering restarts generation with a new execution attempt without incrementing the provider retry counter.

**Steps**

- [ ] Write unit tests for normal completion, bounded progress coalescing, first-token timing, cancellation, lost lease during streaming, steering restart, needs input, deadline expiry, retry exhaustion, provider Retry-After, duplicate completion, tool replay after crash, maximum tool rounds/aggregate output, sequential side-effect calls, and concurrency bulkheads.

~~~go
func TestLostLeaseCancelsReasonerAndCannotComplete(t *testing.T) {
	store, stream := newWorkerHarness(t)
	runner := newRunner(store, stream)
	run := startRunner(t, runner)
	stream.WaitUntilStarted(t)
	store.LoseLease()
	store.FireHeartbeat()
	stream.AssertContextCancelled(t)
	store.AssertNoCompletion(t)
	run.CancelAndWait(t)
}
~~~

- [ ] Run go test ./internal/worker and verify failure.
- [ ] Define safety.Policy, WorkerResultCandidate, and Decision in internal/safety/policy.go, plus a fail-closed DenyAll implementation. Implement a fixed-size execution pool, heartbeat at leaseDuration/3, one bounded in-memory result builder per job, and no goroutine per token.
- [ ] Persist only transitions, at-most-one progress update per second, needs-input data, final structured result, usage, and safety decision.
- [ ] Compose live-worker from config, pgx pool, DeepSeek client, tool registry, the fail-closed safety policy, outbox publisher, lease sweeper, and signal-aware drain. Task 18 supplies the production static policy without changing Runner.
- [ ] Run go test -race ./internal/worker ./internal/reasoner/deepseek, go build ./cmd/live-worker, and a two-worker lease integration test.
- [ ] Commit.

~~~bash
git add internal/worker internal/safety/policy.go cmd/live-worker
git commit -m "feat: execute durable DeepSeek handoffs"
~~~

---

## Task 15: Bridge durable events into visual and spoken presentation

**Files**

- Create: internal/handoff/event_pump.go
- Create: internal/handoff/event_pump_test.go
- Create: internal/presentation/policy.go
- Create: internal/presentation/policy_test.go
- Modify: internal/gateway/app.go
- Modify: internal/session/input.go
- Modify: internal/session/reactor.go
- Modify: cmd/live-gateway/main.go

**Stable interfaces**

~~~go
type PresentationSink interface {
	OfferHandoffEvent(context.Context, Scope, Event) error
}

type EventPump struct {
	Reader        EventReader
	Cursors       DeliveryCursorStore
	Notifications <-chan EventWakeup
	Sink          PresentationSink
	Clock         clock.Clock
	PollInterval  time.Duration
}

type EventSubscription struct {
	ConsumerID string
	Scope      Scope
}

func (p *EventPump) Register(EventSubscription) error
func (p *EventPump) Unregister(EventSubscription)
func (p *EventPump) Run(context.Context) error

type Policy interface {
	Decide(Context, handoff.Event) Decision
}
~~~

Every allowed durable event first becomes the corresponding live.handoff.* visual event. Ordinary progress is visual-only and coalesced. needs_input, important blockers, and concise completion summaries may be spoken when the user is not speaking and no response is active. EventPump never stores a session incarnation: the gateway PresentationSink resolves the current live session atomically from tenant/conversation through SessionDirectory. ErrNoLiveSession leaves the cursor unchanged for reconnect replay.

**Steps**

- [ ] Test ordered cursor replay, duplicate/lost notifications, reconnect cursor continuation, progress coalescing, completion while speaking, completion during another response, needs-input speech, urgent safety interruption, and provider instruction rejection.
- [ ] Test identical conversation IDs under separate tenants, offline conversation events, Register/Unregister races, gateway crash after socket delivery before cursor commit, cursor commit before next read, sink backpressure, and pump restart from the stored cursor.

~~~go
func TestCompletionWhileUserSpeaksStaysVisual(t *testing.T) {
	policy := presentation.NewPolicy(testConfig())
	decision := policy.Decide(presentation.Context{UserSpeaking: true}, completedEvent())
	assert.True(t, decision.EmitVisual)
	assert.False(t, decision.SpeakNow)
	assert.True(t, decision.DeferSpeech)
}
~~~

- [ ] Run go test ./internal/handoff ./internal/presentation ./internal/gateway and verify failure.
- [ ] Implement EventPump so tenant-scoped notifications only wake registered ReadEvents, clock-driven periodic polling repairs every notification gap, and the delivery cursor advances only after the sink confirms a visual write. Replayed duplicates preserve event ID and sequence for client de-duplication.
- [ ] For selected speech, emit an out-of-band response.create with a fixed non-overridable template and a bounded structured result encoded as untrusted quoted data. Never create fake user speech or concatenate result text into session instructions.
- [ ] Invoke safety.Policy for the visual channel and again for the speech channel. The app remains fail-closed with safety.DenyAll until Task 18 installs the configured policy; tests inject an explicit allow policy.
- [ ] If the provider rejects out-of-band instructions, retain the visual event and emit live.session.degraded.
- [ ] Run go test -race ./internal/handoff ./internal/presentation ./internal/gateway ./internal/session and commit.

~~~bash
git add internal/handoff/event_pump.go internal/handoff/event_pump_test.go internal/presentation internal/gateway/app.go internal/session cmd/live-gateway/main.go
git commit -m "feat: present durable handoff results"
~~~

---

## Task 16: Add reconnect fencing and bounded conversation recovery

**Files**

- Create: internal/session/recovery.go
- Create: internal/session/recovery_test.go
- Modify: internal/session/manager.go
- Modify: internal/session/manager_test.go
- Modify: internal/gateway/app.go
- Modify: internal/store/postgres/conversations.go
- Modify: internal/store/postgres/conversations_integration_test.go

**Stable interfaces**

~~~go
type RecoverySnapshot struct {
	ConversationID identity.ConversationID
	CommittedItems []CommittedItem
	ActiveHandoffs []handoff.Snapshot
	LastEventSeq   int64
}

type RecoveryStore interface {
	LoadRecovery(
		context.Context,
		handoff.Scope,
		int,
		int,
	) (RecoverySnapshot, error)
}

type ReconnectRequest struct {
	Scope           handoff.Scope
	SessionID       identity.SessionID
	LastIncarnation identity.Incarnation
	LastEventSeq    int64
}
~~~

A reconnect always allocates a greater incarnation, advances the response epoch, opens a new provider session, restores bounded committed text plus active handoff state, and resumes only at the next conversational turn. It never replays a partial microphone utterance.

**Steps**

- [ ] Write tests for monotonically increasing incarnations, stale-incarnation rejection, old provider output after reconnect, bounded transcript restoration, active-job restoration, unresolved needs-input restoration, and no partial-audio replay.
- [ ] Test provider reconnect with jitter only at a turn boundary and exhaustion of its small retry budget.

~~~go
func TestReconnectRejectsOldIncarnationOutput(t *testing.T) {
	h := newRecoveryHarness(t)
	old := h.Connect(identity.Incarnation(1))
	current := h.Reconnect(identity.Incarnation(1))
	assert.Equal(t, identity.Incarnation(2), current.Incarnation)
	old.Emit(audioEvent(9, testkit.RampPCM(960)))
	h.Client.AssertNoAudio(t)
}
~~~

- [ ] Test the PostgreSQL outage policy: live audio continues for at most the configured 30-second grace period, new handoffs fail closed, committed text uses bounded count/bytes, recovery flushes in order, and exhaustion closes the session.
- [ ] Run go test ./internal/session ./internal/gateway and the focused conversation integration tests; verify failure.
- [ ] Implement the recovery loader and state transition. Fence every submit by SessionID plus incarnation and keep the durable job context independent of sessionCtx.
- [ ] Run go test -race ./internal/session ./internal/gateway and TEST_DATABASE_URL=postgres://live:live@127.0.0.1:5432/live?sslmode=disable go test -race -tags=integration ./internal/store/postgres -run TestLoadRecovery.
- [ ] Commit.

~~~bash
git add internal/session/recovery.go internal/session/recovery_test.go internal/session/manager.go internal/session/manager_test.go internal/gateway/app.go internal/store/postgres/conversations.go internal/store/postgres/conversations_integration_test.go
git commit -m "feat: recover fenced realtime sessions"
~~~

---

## Task 17: Authenticate and admit sessions before WebSocket upgrade

**Files**

- Create: internal/auth/auth.go
- Create: internal/auth/static.go
- Create: internal/auth/oidc.go
- Create: internal/auth/auth_test.go
- Create: internal/auth/oidc_test.go
- Create: internal/admission/origin.go
- Create: internal/admission/quota.go
- Create: internal/admission/postgres.go
- Create: internal/admission/postgres_integration_test.go
- Create: internal/admission/controller.go
- Create: internal/admission/controller_test.go
- Create: internal/testkit/jwks.go
- Modify: internal/config/config.go
- Modify: internal/config/config_test.go
- Modify: configs/gateway.example.yaml
- Modify: internal/transport/ws/handler.go
- Modify: internal/transport/ws/handler_test.go
- Modify: internal/session/manager.go
- Modify: internal/session/manager_test.go

**Stable interfaces**

~~~go
type Principal struct {
	TenantID string
	UserID   string
	Scopes   map[string]struct{}
	ExpiresAt time.Time
}

type Authenticator interface {
	Authenticate(context.Context, string) (Principal, error)
}

type ReleaseFunc = func()

func (c *Controller) Admit(
	ctx context.Context,
	r *http.Request,
) (session.Subject, ReleaseFunc, error)
~~~

Admission runs this order before websocket.Accept: method/TLS/trusted-proxy/origin checks, per-instance global/IP token bucket, bearer parsing and OIDC verification, then PostgreSQL-backed expiring tenant/user session lease. ReleaseFunc deletes the distributed lease and is idempotent. Existing sessions continue through a brief database outage; new production admissions fail closed while readiness is false.

**Steps**

- [ ] Write table tests for missing/malformed bearer tokens, wrong issuer/audience, expired JWT, missing claims, missing scope, static-token constant-time comparison, TLS rejection, trusted-proxy handling, exact origin, wildcard origin rejection, quota exhaustion, and double release.
- [ ] Test invalid-token floods hit the IP/global bucket before JWT verification, inactive limiter keys are evicted, OIDC JWKS rotation, cached keys during a bounded issuer outage, and socket cancellation exactly at Principal.ExpiresAt. Static development subjects have zero ExpiresAt.
- [ ] Integration-test distributed tenant/user quotas across two controller instances, lease heartbeat, stale lease expiry, crash recovery, and cross-tenant reconnect rejection.

~~~go
func TestAdmissionHappensBeforeUpgrade(t *testing.T) {
	authn := &recordingAuthenticator{err: auth.ErrUnauthorized}
	manager := testManager()
	handler := ws.NewHandler(testOptions(), manager, admission.New(authn, testLimits()))
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, bearerRequest("bad"))
	assert.Equal(t, http.StatusUnauthorized, resp.Code)
	assert.False(t, manager.WasRun())
}
~~~

- [ ] Use httptest plus an in-memory RSA JWKS server for OIDC tests; no internet or real secret.
- [ ] Run go test ./internal/auth ./internal/admission ./internal/transport/ws and verify failure.
- [ ] Add github.com/coreos/go-oidc/v3/oidc. Validate issuer, audience, expiry, tenant/user claims, and required scopes. Convert Principal to session.Subject once; never accept tenant, user, permissions, or expiry from client events.
- [ ] Trust X-Forwarded-Proto only when RemoteAddr is inside an explicitly configured trusted proxy CIDR. Reject empty Origin except for a separately configured non-browser client policy.
- [ ] Run go test -race ./internal/auth ./internal/admission ./internal/transport/ws and TEST_DATABASE_URL=postgres://live:live@127.0.0.1:5432/live?sslmode=disable go test -race -tags=integration ./internal/admission.

~~~bash
git add go.mod go.sum internal/auth internal/admission internal/testkit/jwks.go internal/config configs/gateway.example.yaml internal/transport/ws internal/session/manager.go internal/session/manager_test.go
git commit -m "feat: secure realtime session admission"
~~~

---

## Task 18: Enforce worker-result safety, privacy IDs, and log redaction

**Files**

- Modify: internal/safety/policy.go
- Create: internal/safety/static.go
- Create: internal/safety/policy_test.go
- Modify: internal/safety/privacy.go
- Modify: internal/safety/privacy_test.go
- Create: internal/telemetry/redact.go
- Create: internal/telemetry/redact_test.go
- Modify: internal/worker/runner.go
- Modify: internal/worker/runner_test.go
- Modify: internal/presentation/policy.go
- Modify: internal/presentation/policy_test.go
- Modify: internal/session/input.go
- Modify: internal/session/reactor.go
- Modify: internal/session/reactor_test.go
- Modify: internal/gateway/app.go
- Modify: internal/gateway/app_test.go
- Modify: internal/config/config.go
- Modify: configs/gateway.example.yaml
- Modify: configs/worker.example.yaml

**Stable interfaces**

~~~go
type Action string

type Channel string

const (
	ChannelWorker Channel = "worker"
	ChannelVisual Channel = "visual"
	ChannelSpeech Channel = "speech"
)

const (
	ActionAllow                Action = "allow"
	ActionRedact               Action = "redact"
	ActionSuppress             Action = "suppress"
	ActionRequestClarification Action = "request_clarification"
	ActionCancelResponse       Action = "cancel_response"
	ActionTerminateSession     Action = "terminate_session"
)

type WorkerResultCandidate struct {
	SessionID        identity.SessionID
	ConversationID   identity.ConversationID
	HandoffID        identity.HandoffID
	Channel          Channel
	Summary          string
	Structured       json.RawMessage
	RequiresApproval bool
	Approved         bool
	ProviderBlocked  bool
}

type Decision struct {
	Action      Action
	SafeSummary string
	ReasonCode  string
}

type Policy interface {
	EvaluateWorkerResult(
		context.Context,
		WorkerResultCandidate,
	) (Decision, error)
}

func ProviderSafetyID(secret []byte, tenantID, userID string) string
func NewRedactingHandler(next slog.Handler, deniedKeys []string) slog.Handler
func EvaluateOrSuppress(
	context.Context,
	Policy,
	WorkerResultCandidate,
) Decision
~~~

ProviderSafetyID is HMAC-SHA256 over length-delimited tenant and user IDs, returned as unpadded base64url.

**Steps**

- [ ] Test provider block to terminate, unapproved protected action to clarification, oversize to suppress, redaction, allowed unchanged, and policy error to suppress.
- [ ] Add end-to-end reactor tests: redact exposes only SafeSummary; suppress emits nothing; request_clarification emits a bounded needs-input event; cancel_response advances the epoch before provider cancel; terminate_session advances the epoch and produces a terminal CloseEffect. Gate the client writer and prove no queued unsafe/stale bytes escape.
- [ ] Test delimiter-ambiguity resistance and deterministic privacy IDs without exposing either raw identifier.
- [ ] Feed sentinel credentials, base64 PCM, transcript text, and tool arguments through nested slog groups and assert none appear in output.

~~~go
func TestPolicyErrorsFailClosed(t *testing.T) {
	decision := safety.EvaluateOrSuppress(t.Context(), errorPolicy{}, validCandidate())
	assert.Equal(t, safety.ActionSuppress, decision.Action)
	assert.Equal(t, "policy_error", decision.ReasonCode)
}
~~~

- [ ] Run go test ./internal/safety ./internal/telemetry ./internal/worker ./internal/presentation and verify failure.
- [ ] Implement size limits, approval checks, provider-block handling, HMAC privacy ID, and recursive slog attribute redaction.
- [ ] Use the single safety.Policy contract created in Task 14 unchanged. Evaluate before persisting a presentable result and again for the selected visual or speech channel immediately before reinjection. Route cancel/terminate decisions through incarnation-fenced Manager.Submit inputs.
- [ ] Run go test -race ./internal/safety ./internal/telemetry ./internal/worker ./internal/presentation and commit.

~~~bash
git add internal/safety internal/telemetry/redact.go internal/telemetry/redact_test.go internal/worker internal/presentation internal/session internal/gateway internal/config configs
git commit -m "feat: enforce result safety and privacy"
~~~

---

## Task 19: Add metrics, traces, health, diagnostics, config hash, and graceful drain

**Files**

- Create: internal/telemetry/runtime.go
- Create: internal/telemetry/metrics.go
- Create: internal/telemetry/correlation.go
- Create: internal/telemetry/metrics_test.go
- Create: internal/health/registry.go
- Create: internal/health/http.go
- Create: internal/health/http_test.go
- Create: internal/lifecycle/drainer.go
- Create: internal/lifecycle/drainer_test.go
- Create: internal/config/hash.go
- Create: internal/config/hash_test.go
- Create: internal/admin/http.go
- Create: internal/admin/http_test.go
- Modify: internal/session/manager.go
- Modify: internal/worker/runner.go
- Modify: internal/gateway/app.go
- Modify: cmd/live-gateway/main.go
- Modify: cmd/live-worker/main.go

**Required metric families**

~~~text
live_sessions_active
live_session_admissions_total
live_audio_frame_transit_seconds
live_queue_buffered_milliseconds
live_queue_depth
live_queue_bytes
live_queue_oldest_item_age_seconds
live_queue_blocked_seconds
live_queue_overflows_total
live_bargein_suppression_seconds
live_handoff_ack_seconds
live_worker_queue_seconds
live_reasoner_first_token_seconds
live_worker_duration_seconds
live_worker_retries_total
live_presentation_delay_seconds
live_outbox_lag_seconds
live_expired_leases_total
live_duplicate_deliveries_total
live_reconnect_restore_seconds
live_stale_output_rejected_total
~~~

Metric labels are bounded to component, provider, region, instance, config_hash, queue, and result. IDs belong only in traces and content-free logs.

**Steps**

- [ ] Test exact names, label sets, histogram buckets at 1/3/5/10/15/30/50/100 ms, duplicate registration behavior, config hash stability with secret values redacted, health codes, admin scope, and idempotent drain.
- [ ] Add BenchmarkObserveAudioTransit with pre-bound observers, zero allocations/op, no per-frame log/span, and at most 250 ns/op on the recorded release-runner profile.

~~~go
func TestReadinessFailsDuringDrain(t *testing.T) {
	d := lifecycle.NewDrainer()
	registry := health.NewRegistry()
	registry.RegisterDrainer(d)
	d.Begin()
	response := httptest.NewRecorder()
	registry.ReadyHandler().ServeHTTP(response, httptest.NewRequest("GET", "/health/ready", nil))
	assert.Equal(t, http.StatusServiceUnavailable, response.Code)
}
~~~

- [ ] Run go test ./internal/telemetry ./internal/health ./internal/lifecycle ./internal/config ./internal/admin and verify failure.
- [ ] Add Prometheus client and OpenTelemetry SDK/exporter. Both binaries start a health/metrics/admin HTTP listener. Implement /health/live without dependency calls, /health/ready with bounded checks, /metrics, /admin/v1/capacity, and content-free /admin/v1/sessions.
- [ ] Implement a loopback-only POST /admin/v1/drain plus a binary drain --url command for Kubernetes preStop. On drain request or SIGTERM: become unready, stop new sessions/leases, allow configured 30-second gateway and 120-second worker deadlines, then cancel remaining I/O.
- [ ] Add tested check-config --config <path> and config-hash --config <path> subcommands to gateway and worker. config-hash canonicalizes non-secret configuration; startup rejects LIVE_EXPECTED_CONFIG_HASH when it differs.
- [ ] Instrument the media path without allocations per frame beyond the existing event object; benchmark before accepting instrumentation changes.
- [ ] Run go test -race ./..., go vet ./..., go build ./cmd/..., and commit.

~~~bash
git add go.mod go.sum internal/telemetry internal/health internal/lifecycle internal/config internal/admin internal/session internal/worker internal/gateway cmd
git commit -m "feat: add production health and telemetry"
~~~

---

## Task 20: Add a cluster-wide emergency handoff kill switch

**Files**

- Create: migrations/000002_runtime_flags.up.sql
- Create: migrations/000002_runtime_flags.down.sql
- Create: internal/feature/gates.go
- Create: internal/feature/gates_test.go
- Create: internal/feature/postgres.go
- Create: internal/feature/postgres_integration_test.go
- Modify: internal/admin/http.go
- Modify: internal/admin/http_test.go
- Modify: internal/gateway/handoffs.go
- Modify: internal/gateway/handoffs_test.go
- Modify: internal/store/postgres/repository.go
- Modify: configs/gateway.example.yaml

**Stable interfaces**

~~~go
const HandoffsEnabled = "handoffs_enabled"
const ShadowMode = "shadow_mode"

type Snapshot struct {
	Version uint64
	Values  map[string]bool
}

type Store interface {
	Load(context.Context) (Snapshot, error)
	Set(context.Context, string, bool, string, string) (Snapshot, error)
}

func (g *Gates) Enabled(name string) bool
func (g *Gates) Apply(snapshot Snapshot) bool
func (g *Gates) RecordRefreshFailure()
func (g *Gates) Run(context.Context, Store, time.Duration) error
~~~

**Steps**

- [ ] Unit-test monotonic snapshot application, stale snapshot rejection, concurrent readers, and fail-safe disabled-state retention after store loss.
- [ ] Integration-test atomic version/audit updates and repair after a dropped notification.
- [ ] Test authenticated PUT /admin/v1/features/handoffs with body {"enabled":false,"reason":"incident-123"}.
- [ ] Define shadow_mode exactly: durable handoffs and read-only tools execute, externally side-effecting tools are disabled, all live.handoff.* client events and spoken reinjection are suppressed, and bounded outcome/latency metrics carry result=shadow. Voice traffic remains normal. Add integration tests proving zero user-visible worker output.

~~~go
func TestUnavailableStoreCannotReenableDisabledGate(t *testing.T) {
	gates := feature.New(map[string]bool{feature.HandoffsEnabled: true})
	gates.Apply(feature.Snapshot{Version: 2, Values: map[string]bool{feature.HandoffsEnabled: false}})
	gates.RecordRefreshFailure()
	assert.False(t, gates.Enabled(feature.HandoffsEnabled))
}
~~~

- [ ] Run unit and integration tests and verify failure.
- [ ] Implement PostgreSQL as authority, LISTEN/NOTIFY as acceleration, and one-second polling as gap repair.
- [ ] Check the gate immediately before the handoff creation transaction. When disabled, reject new handoffs and emit live.session.degraded while existing voice continues.
- [ ] Check shadow mode at tool registration and at both visual/speech presentation boundaries so a mid-job mode change cannot leak output.
- [ ] Verify two gateway instances observe disablement within two refresh intervals, run migration down/up, and commit.

~~~bash
git add migrations/000002_runtime_flags.up.sql migrations/000002_runtime_flags.down.sql internal/feature internal/admin internal/gateway/handoffs.go internal/gateway/handoffs_test.go internal/store/postgres/repository.go configs/gateway.example.yaml
git commit -m "feat: add emergency handoff kill switch"
~~~

---

## Task 21: Add explicit approvals, retention, and tenant-scoped deletion

**Files**

- Create: internal/handoff/approval.go
- Create: internal/handoff/approval_test.go
- Create: internal/store/postgres/approvals.go
- Create: internal/store/postgres/approvals_integration_test.go
- Create: internal/store/postgres/retention.go
- Create: internal/store/postgres/retention_integration_test.go
- Create: internal/retention/sweeper.go
- Create: internal/retention/sweeper_test.go
- Modify: internal/protocol/client.go
- Modify: internal/protocol/server.go
- Modify: internal/protocol/capabilities.go
- Modify: internal/protocol/capabilities_test.go
- Modify: internal/session/reactor.go
- Modify: internal/session/reactor_test.go
- Modify: internal/session/recovery.go
- Modify: internal/session/recovery_test.go
- Modify: internal/worker/heartbeat.go
- Modify: internal/worker/runner.go
- Modify: internal/admin/http.go
- Modify: internal/admin/http_test.go
- Modify: internal/config/config.go
- Modify: configs/gateway.example.yaml
- Modify: configs/worker.example.yaml

**Stable interfaces**

~~~go
type ApprovalDecision string

const (
	ApprovalApproved ApprovalDecision = "approved"
	ApprovalDenied   ApprovalDecision = "denied"
)

type ApprovalResolution struct {
	InvocationID  string
	Decision      ApprovalDecision
	ActorTenantID string
	ActorUserID   string
	Reason        string
	IdempotencyKey string
	ResolvedAt    time.Time
}

type ApprovalStore interface {
	ResolveApproval(context.Context, handoff.Scope, ApprovalResolution) error
	LoadApproval(context.Context, handoff.Scope, string) (ToolApproval, error)
}

type RetentionStore interface {
	MarkConversationDeleting(context.Context, handoff.Scope, string, string) error
	DeleteReadyConversations(context.Context, time.Time, int) (int, error)
}
~~~

The server emits live.tool.approval.required with invocation ID, bounded action summary, expiry, and risk class. An authenticated client answers with live.tool.approval.resolve. The tenant/user come from session.Subject, not the event body. Approval is invocation-specific, expires, and cannot be changed after approve/deny.

Deletion is two phase. The request marks the conversation deleting and atomically requests cancellation for every nonterminal handoff. The sweeper hard-deletes finalized transcript/result/ciphertext rows only after active leases end or expire, then writes a content-free deletion audit containing tenant ID, a keyed conversation hash, actor, reason, and time.

**Steps**

- [ ] Write protocol and domain tests for approve, deny, duplicate matching resolution, conflicting resolution, expiry, wrong tenant/user, unknown invocation, and oversized reason.
- [ ] Test worker pause to needs_input, approval wakeup, denial terminal failure, approval expiry, and reconnect restoration of unresolved approval.

~~~go
func TestApprovalCannotCrossTenant(t *testing.T) {
	store := newApprovalHarness(t)
	err := store.ResolveApproval(t.Context(), scope("tenant-b"), resolutionForTenantA())
	assert.ErrorIs(t, err, handoff.ErrNotFound)
}
~~~

- [ ] Integration-test mark-delete with running jobs, cancellation observation, lease expiry, referential cleanup, batch retention, tenant isolation, idempotent deletion, and content-free audit preservation.
- [ ] Implement DELETE /admin/v1/conversations/{id} for the owning user or live.admin scope. It returns 202 after durable mark/cancel, never exposes whether another tenant's ID exists, and cannot hard-delete an active lease directly.
- [ ] Inject clock.Clock into approval expiry and retention sweeper; tests use no time.Sleep.
- [ ] Run go test -race ./internal/handoff ./internal/retention ./internal/session ./internal/worker ./internal/admin and the approval/retention integration suites.
- [ ] Commit.

~~~bash
git add internal/handoff/approval.go internal/handoff/approval_test.go internal/store/postgres/approvals.go internal/store/postgres/approvals_integration_test.go internal/store/postgres/retention.go internal/store/postgres/retention_integration_test.go internal/retention internal/protocol internal/session internal/worker internal/admin internal/config configs
git commit -m "feat: add approvals and retention controls"
~~~

---

## Task 22: Add deterministic integration and provider contract suites

**Files**

- Create: test/integration/e2e_test.go
- Create: test/integration/recovery_test.go
- Create: test/contract/s2s_realtime_test.go
- Create: test/liveprovider/deepseek_test.go
- Create: docs/testing.md

**Suite contract**

~~~bash
go test ./...
TEST_DATABASE_URL=postgres://live:live@127.0.0.1:5432/live?sslmode=disable go test -tags=integration ./test/integration/...
S2S_REALTIME_URL=ws://127.0.0.1:8000/v1/realtime go test -tags=contract ./test/contract/...
go test -tags=liveprovider ./test/liveprovider/...
~~~

The live-provider suite is opt-in and never a pull-request gate. It prints no prompts, credentials, transcripts, or result text.

**Steps**

- [ ] First write a failing deterministic end-to-end test that starts PostgreSQL, an in-process fake voice server, fake DeepSeek, gateway, and worker.

~~~go
func TestVoiceContinuesDuringDurableHandoff(t *testing.T) {
	system := startDeterministicSystem(t)
	client := system.ConnectClient(t)
	client.CommitUserTurn(t, "research the request")
	system.Voice.RequestBackgroundTask(t)
	client.ExpectHandoffCreated(t)
	client.ExpectAudioBeforeWorkerCompletes(t)
	system.Reasoner.Complete(t)
	client.ExpectHandoffCompleted(t)
}
~~~

- [ ] Cover worker completion during speech, steering, explicit cancellation, crash after persistence before ack, lease recovery after worker cancellation, provider disconnect with queued audio, reconnect with active jobs, duplicate completion, lost notification, and two-session contamination.
- [ ] Add approval/denial, retention/deletion, OIDC expiry, distributed admission limits, shadow mode, unsafe-result suppression, and event-cursor crash/replay cases.
- [ ] In the simulator contract suite validate its documented event subset, PCM24/base64 forwarding, cancellation, function calls, one-active-response behavior, and declared concurrency through S2S_CONTRACT_CONCURRENCY.
- [ ] Add a real simulator VAD to finalized transcription to metadata-less implicit response.created to audio test. Prove the binding handshake assigns a new epoch before audio delivery and produces exactly one matching response.done.
- [ ] In the optional DeepSeek suite validate authentication, model configuration, SSE completion, usage, and cancellation using bounded non-sensitive fixtures.
- [ ] Run unit and integration suites to green; run contract tests against /Users/chuanmingliu/speech-to-speech using its configured local service.
- [ ] Document exact prerequisites, build tags, secret handling, and failure triage. Commit.

~~~bash
git add test/integration test/contract test/liveprovider docs/testing.md
git commit -m "test: add realtime and recovery contracts"
~~~

---

## Task 23: Package hardened local and Kubernetes deployments

**Files**

- Create: Dockerfile
- Create: .dockerignore
- Create: deploy/docker-compose.yml
- Create: deploy/compose.chaos.yml
- Create: deploy/k8s/base/kustomization.yaml
- Create: deploy/k8s/base/configmap.yaml
- Create: deploy/k8s/base/gateway-deployment.yaml
- Create: deploy/k8s/base/gateway-service.yaml
- Create: deploy/k8s/base/worker-deployment.yaml
- Create: deploy/k8s/base/migrate-job.yaml
- Create: deploy/k8s/base/pdb.yaml
- Create: deploy/k8s/base/hpa.yaml
- Create: deploy/k8s/base/ingress.yaml
- Create: deploy/k8s/base/network-policy.yaml
- Create: deploy/k8s/overlays/local/kustomization.yaml
- Create: deploy/k8s/overlays/local/patch.yaml
- Create: deploy/k8s/overlays/production/kustomization.yaml
- Create: deploy/k8s/overlays/production/patch.yaml
- Create: deploy/k8s/secrets.example.env
- Modify: Makefile

**Deployment invariants**

- The Dockerfile has gateway, worker, migrate, and load targets; final images run as a numeric non-root UID and contain no compiler or credentials.
- Workloads use a read-only root filesystem, drop all Linux capabilities, and set seccompProfile RuntimeDefault.
- Gateway terminationGracePeriodSeconds is 45; worker is 135. Both expose loopback admin plus pod health listeners; preStop invokes the binary drain --url command and waits for readiness to fall before returning.
- PDB minAvailable is 1. hpa.yaml contains separate gateway and worker HPAs. Base scaling is CPU; the production gateway patch adds live_sessions_active only when the documented Prometheus Adapter/custom-metrics API prerequisite is installed. Worker scaling uses queue age, not active sessions.
- Production Ingress terminates TLS and supplies explicit trusted proxy CIDRs. The production overlay must set certificate secret/host or validation fails.
- Standard Kubernetes NetworkPolicy permits DNS, in-cluster PostgreSQL/OIDC/OTLP services, and only a platform egress proxy stable CIDR for external speech/DeepSeek traffic. Production sets HTTPS_PROXY and the proxy CIDR; it never pretends native NetworkPolicy can filter external FQDNs.
- Compose accepts S2S_IMAGE and S2S_CONFIG_PATH and injects only named secret environment variables.

**Steps**

- [ ] Add a failing make verify-deploy target that builds all image targets, runs docker compose config, and runs kubectl kustomize on local and production overlays.
- [ ] Run make verify-deploy and verify failure before manifests exist.
- [ ] Implement multi-stage images, Compose topology, migrations, health checks, dependency ordering, Ingress/TLS, Kustomize bases/overlays, quotas, and security contexts.
- [ ] Generate the immutable hash once with config-hash, inject it into the pod annotation and LIVE_EXPECTED_CONFIG_HASH, and export the same value as the bounded telemetry label. Keep actual Secret resources outside Git; secrets.example.env lists names only.
- [ ] Define deployment order: apply the migration Job, kubectl wait for condition=complete, then apply gateway/worker resources. Down/up migration verification is allowed only against disposable databases.
- [ ] Make verify-deploy run image builds, docker compose config, both kustomize renders, kubeconform strict validation, and kubectl server-side dry-run in an ephemeral kind cluster.
- [ ] Run make verify-deploy and docker run --read-only --cap-drop=ALL with live-gateway check-config --config /etc/live/config.yaml and live-worker check-config --config /etc/live/config.yaml.
- [ ] Run a Compose smoke session through gateway to the local simulator, then commit.

~~~bash
git add Dockerfile .dockerignore Makefile deploy
git commit -m "build: add hardened service deployment"
~~~

---

## Task 24: Prove chaos behavior, 250-session capacity, and soak stability

**Files**

- Create: cmd/live-load/main.go
- Create: internal/loadgen/runner.go
- Create: internal/loadgen/metrics.go
- Create: internal/loadgen/runner_test.go
- Create: test/chaos/scenarios_test.go
- Create: test/load/gateway_test.go
- Create: test/load/soak_test.go
- Modify: deploy/compose.chaos.yml
- Modify: Makefile

**Exact gates**

~~~text
Concurrent fake-provider sessions: at least 250
Gateway audio transit p95: below 15 ms
Gateway audio transit p99: below 30 ms
Barge-in suppression p95: below 100 ms
Cross-session or stale-epoch output: zero
Queue overflow at declared capacity: zero
Post-teardown goroutines: baseline plus at most 10
Post-teardown forced-GC heap: baseline plus at most 16 MiB
Two-hour heap regression slope: at most 4 MiB/hour
~~~

**Steps**

- [ ] Unit-test load scheduling, deterministic audio checksums, percentile calculation, cross-session sentinels, failure thresholds, and report serialization.
- [ ] Write chaos cases for PostgreSQL disconnect, DeepSeek 429/Retry-After, slow client, stalled voice provider, killed worker, and lost notification.

~~~go
func TestGateway250Sessions(t *testing.T) {
	report := loadgen.Run(t.Context(), loadgen.Config{
		GatewayURL: "ws://127.0.0.1:8080/v1/realtime",
		Sessions:   250,
		Duration:   10 * time.Minute,
	})
	assert.Less(t, report.AudioP95, 15*time.Millisecond)
	assert.Less(t, report.AudioP99, 30*time.Millisecond)
	assert.Zero(t, report.StaleAudio)
	assert.Zero(t, report.CrossSessionAudio)
}
~~~

- [ ] Run the unit tests and verify failure before implementing the load generator.
- [ ] deploy/compose.chaos.yml adds Toxiproxy in front of PostgreSQL and the fake voice/DeepSeek endpoints. Tests use CHAOS_DATABASE_URL plus Toxiproxy's control API, own worker process handles, and always remove toxics/processes in cleanup.
- [ ] Implement cmd/live-load with an explicit gateway URL, identity-preserving fake echo provider, generated per-session PCM watermarks containing monotonic send timestamps, fixed connection concurrency, HDR-style latency recording, bounded reports, and no captured transcript content.
- [ ] Measure gateway transit from load-client send to byte-identical echo receipt and export the gateway's internal transit metric separately. Record CPU model, core count, memory, OS, Go version, and config hash; enforce 15/30 ms release thresholds only on the pinned self-hosted runner profile.
- [ ] Run:

~~~bash
TEST_DATABASE_URL=postgres://live:live@127.0.0.1:5432/live?sslmode=disable go test -tags=chaos -timeout=30m ./test/chaos/...
go test -tags=load -run TestGateway250Sessions -timeout=20m ./test/load/...
LIVE_SOAK_DURATION=2h go test -tags=soak -run TestGatewaySoak -timeout=150m ./test/load/...
~~~

- [ ] Investigate and fix every leak, stale frame, contamination, or SLO miss before marking the task complete.
- [ ] Commit.

~~~bash
git add cmd/live-load internal/loadgen test/chaos test/load deploy/compose.chaos.yml Makefile
git commit -m "test: add chaos capacity and soak gates"
~~~

---

## Task 25: Add CI, dashboards, runbooks, rollout, and machine-readable release gates

**Files**

- Create: cmd/live-release-gate/main.go
- Create: internal/releasegate/evidence.go
- Create: internal/releasegate/check.go
- Create: internal/releasegate/check_test.go
- Create: .github/workflows/ci.yml
- Create: .github/workflows/nightly.yml
- Create: .github/workflows/release-gate.yml
- Create: deploy/observability/grafana-dashboard.json
- Create: deploy/observability/alerts.yaml
- Create: README.md
- Create: docs/protocol.md
- Create: docs/security.md
- Create: docs/operations.md
- Create: docs/rollout.md
- Create: docs/limitations.md
- Create: docs/runbooks/database-outage.md
- Create: docs/runbooks/voice-provider-outage.md
- Create: docs/runbooks/queue-overload.md
- Create: docs/runbooks/rollback.md
- Modify: Makefile

**Pull-request gate**

~~~bash
go vet ./...
go test ./...
go test -race ./...
TEST_DATABASE_URL=postgres://live:live@127.0.0.1:5432/live?sslmode=disable go test -tags=integration ./test/integration/...
govulncheck ./...
go build ./cmd/...
make verify-deploy
~~~

Nightly adds protocol fuzzing, chaos, 250-session load, and a 30-minute soak. The manual release workflow runs the two-hour soak and optional provider tests only when their secrets are explicitly supplied.

**Evidence contract**

~~~go
type Evidence struct {
	SchemaVersion string
	Commit        string
	ConfigHash    string
	GeneratedAt   time.Time
	Gates         []GateEvidence
	Attestations  []SignedAttestation
}

type GateEvidence struct {
	GateID         string
	Command        []string
	StartedAt      time.Time
	FinishedAt     time.Time
	ExitCode       int
	ArtifactPath   string
	ArtifactSHA256 string
}

type SignedAttestation struct {
	GateID          string
	Signer          string
	StatementSHA256 string
	Signature       string
	SignedAt        time.Time
}
~~~

live-release-gate writes artifacts/release/<commit>/<config-hash>/evidence.json. It rejects evidence older than 24 hours, a commit/config mismatch, missing artifact hashes, duplicate/missing gate IDs, nonzero exits, and missing signed human attestations. The 12 gate IDs map respectively to simulator voice, durable acceptance, correlation, presentation, steer/cancel, epoch fencing, reconnect, restart durability, verification suites, SLO/load, privacy/no-audio-persistence, and deployment/drain evidence.

**Steps**

- [ ] Write a failing make release-gate check that requires machine-readable evidence for all 12 acceptance gates in the approved design.
- [ ] Implement live-release-gate collect and verify subcommands plus tests for schema validation, freshness, SHA-256 tampering, wrong commit/config, and signed-attestation failure. Each gate producer receives an explicit --evidence-out path; make release-gate invokes all 12 in the order above.
- [ ] Add dashboards split by instance, provider, region, and config hash. Alert on stale output, queue overflow, SLO burn, outbox lag, expired leases, readiness, and unexpected goroutine/heap growth. A deployed synthetic canary continuously publishes the cross-session sentinel metric; ordinary production audio is never watermarked.
- [ ] Document the Realtime subset and live.* extensions, reserved tools, audio format, authentication, retention, safety limits, deployment, migration, backup, drain, and incident procedures.
- [ ] In docs/limitations.md explicitly state that the cascaded simulator is not a native full-duplex voice model and cannot provide GPT-Live's model-native mid-generation audio safety.
- [ ] In docs/rollout.md specify the implemented shadow mode, then internal, 1%, 5%, 25%, and 100% canaries by immutable config hash; observe 30 minutes at internal/1%/5% and 60 minutes at 25%.
- [ ] Halt rollout on any stale output, cross-session leakage, two consecutive five-minute SLO breaches, or error-rate regression above one percentage point. Roll back by disabling handoffs first, then deploy the last known-good authenticated gateway image. Never expose speech-to-speech directly.
- [ ] The release workflow uses Buildx to publish linux/amd64 and linux/arm64 gateway/worker/migrate images to ghcr.io/chuanmingliu/openai-gpt-live by immutable digest, emits SBOM and provenance, signs digests with keyless Cosign, and makes production manifests consume digests rather than mutable tags.
- [ ] Run make release-gate and ensure the evidence bundle contains test logs, config hash, dashboard snapshot, vulnerability report, migration rollback check, and human verification that no raw audio is persisted/logged.
- [ ] Commit.

~~~bash
git add cmd/live-release-gate internal/releasegate .github deploy/observability README.md docs Makefile
git commit -m "docs: add operations and release gates"
~~~

---

## Final Verification

- [ ] Run gofmt on every Go file and verify git diff --check.
- [ ] Run go mod tidy and verify go mod tidy produces no further diff.
- [ ] Run go vet ./..., go test ./..., go test -race ./..., and govulncheck ./....
- [ ] Run integration migrations up/down/up against PostgreSQL 16.
- [ ] Run the deterministic integration suite and the local speech-to-speech contract suite.
- [ ] Run make verify-deploy, the chaos suite, the 250-session load gate, and the two-hour soak gate.
- [ ] Confirm the DeepSeek live-provider suite is opt-in, secrets are absent from Git and logs, and default tests require no network.
- [ ] Search tracked files for placeholder markers and remove every unfinished marker:

~~~bash
git grep -nE 'TO''DO|T''BD|FIX''ME|IMPLEMENT_''ME|panic\("not implemented"\)' -- ':!docs/superpowers/plans/2026-08-06-gpt-live-go-framework.md'
~~~

- [ ] Search for forbidden raw-content persistence/logging fields and manually review every match:

~~~bash
git grep -nE 'audio_base64|raw_audio|reasoning_content|tool_arguments|authorization|api_key'
~~~

- [ ] Trace interface consistency end to end: identity types, protocol to realtime mapping, reactor effects, handoff repository, worker store, safety policy, presentation sink, and composition roots.
- [ ] Run make release-gate and archive its machine-readable evidence with the immutable configuration hash.

The framework is complete only when all 12 release acceptance gates in the approved design pass. A green unit suite alone is not completion.
