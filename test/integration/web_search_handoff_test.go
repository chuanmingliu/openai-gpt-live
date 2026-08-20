//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/feature"
	"github.com/chuanmingliu/openai-gpt-live/internal/gateway"
	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/presentation"
	"github.com/chuanmingliu/openai-gpt-live/internal/protocol"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/chuanmingliu/openai-gpt-live/internal/reasoner/deepseek"
	"github.com/chuanmingliu/openai-gpt-live/internal/session"
	"github.com/chuanmingliu/openai-gpt-live/internal/store/postgres"
	"github.com/chuanmingliu/openai-gpt-live/internal/testkit"
	"github.com/chuanmingliu/openai-gpt-live/internal/toolpolicy"
	transportws "github.com/chuanmingliu/openai-gpt-live/internal/transport/ws"
	"github.com/chuanmingliu/openai-gpt-live/internal/worker"
	"github.com/chuanmingliu/openai-gpt-live/migrations"
	"github.com/chuanmingliu/openai-gpt-live/test/internal/fakesearch"
	"github.com/chuanmingliu/openai-gpt-live/test/internal/loadgateway"
	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDurableWebSearchProgressAndVoiceRemainIndependent(t *testing.T) {
	if os.Getenv("TEST_DATABASE_URL") == "" {
		t.Skip("TEST_DATABASE_URL is required for the durable web search boundary")
	}
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

func TestDurableWebSearchDuplicateProgressDeliveryUpdatesOneTimelinePhase(t *testing.T) {
	boundary := newReconnectClient()
	client := &durableSearchClient{cards: make(map[string]*durableSearchCard)}
	events := []protocol.ServerEvent{
		protocol.LiveHandoffCreatedEvent{Type: "live.handoff.created", EventID: "created", SequenceID: 1, HandoffID: "handoff-duplicate", Status: "pending", Details: json.RawMessage(`{}`)},
		protocol.LiveHandoffProgressEvent{Type: "live.handoff.progress", EventID: "searching", SequenceID: 2, HandoffID: "handoff-duplicate", Status: "running", Details: json.RawMessage(`{"phase":"searching","summary":"Searching the web."}`)},
		protocol.LiveHandoffProgressEvent{Type: "live.handoff.progress", EventID: "searching", SequenceID: 2, HandoffID: "handoff-duplicate", Status: "running", Details: json.RawMessage(`{"phase":"searching","summary":"Searching the web."}`)},
		protocol.LiveHandoffProgressEvent{Type: "live.handoff.progress", EventID: "searching-newer", SequenceID: 3, HandoffID: "handoff-duplicate", Status: "running", Details: json.RawMessage(`{"phase":"searching","summary":"Searching the web."}`)},
		protocol.LiveHandoffProgressEvent{Type: "live.handoff.progress", EventID: "reading", SequenceID: 4, HandoffID: "handoff-duplicate", Status: "running", Details: json.RawMessage(`{"phase":"reading_sources","summary":"Reading sources."}`)},
		protocol.LiveHandoffCompletedEvent{Type: "live.handoff.completed", EventID: "completed", SequenceID: 5, HandoffID: "handoff-duplicate", Status: "succeeded", Details: json.RawMessage(`{"summary":"done","structured":{"text":"done","sources":[]}}`)},
	}
	for _, event := range events {
		require.NoError(t, boundary.Write(t.Context(), event))
		client.consumeServerEvent(t, readReconnectEvent(t, boundary))
	}

	assert.Equal(t, []deliveredProgress{
		{Sequence: 2, EventID: "searching", Phase: "searching"},
		{Sequence: 2, EventID: "searching", Phase: "searching"},
		{Sequence: 3, EventID: "searching-newer", Phase: "searching"},
		{Sequence: 4, EventID: "reading", Phase: "reading_sources"},
	}, client.DeliveredProgress(), "the assertion observes duplicates before sequence/phase normalization")
	assert.Equal(t, []string{"handoff-duplicate"}, client.cardOrder)
	assert.Equal(t, []string{"queued", "searching", "reading_sources", "completed"}, client.cards["handoff-duplicate"].Phases)
}

func TestDurableWebSearchCancellationFencesLateFakeResult(t *testing.T) {
	executor := fakesearch.New(fakesearch.Controlled(), fakesearch.IgnoreCancellation())
	harness := newDurableSearchHarness(t, withSearchExecutor(executor))
	client := harness.ConnectVoice(t)
	client.ApplyPolicy(t, toolpolicy.DefaultPolicy())
	handoffID := client.StartSearchTurn(t, "Cancel the bounded search")
	require.Eventually(t, func() bool { return executor.Calls() == 1 }, integrationTimeout, 10*time.Millisecond)

	snapshot, err := harness.service.Get(t.Context(), harness.scope, identity.HandoffID(handoffID))
	require.NoError(t, err)
	_, err = harness.service.Cancel(t.Context(), harness.scope, snapshot.ID, handoff.CancelRequest{
		Actor: "test-user", Reason: "bounded cancellation", IdempotencyKey: "cancel-late-result",
	})
	require.NoError(t, err)
	select {
	case <-executor.ContextDone():
	case <-time.After(integrationTimeout):
		t.Fatal("worker did not propagate cancellation to fake search")
	}
	executor.Release()
	card := client.WaitForTerminalCard(t, handoffID)
	assert.Equal(t, "cancelled", card.Status)
	assert.NotContains(t, card.Phases, "reading_sources")
	assert.NotContains(t, card.Phases, "completed")
	assert.Zero(t, client.AudioOrderingFailures())
}

func TestDurableWebSearchDisabledPolicyCannotInvokeExecutor(t *testing.T) {
	executor := fakesearch.New()
	harness := newDurableSearchHarness(t, withSearchExecutor(executor))
	client := harness.ConnectVoice(t)
	disabled := toolpolicy.DefaultPolicy()
	disabled.WebSearch.Enabled = false
	client.ApplyPolicy(t, disabled)
	handoffID := client.StartSearchTurn(t, "Search must remain disabled")
	card := client.WaitForTerminalCard(t, handoffID)
	assert.Equal(t, "failed", card.Status)
	assert.Zero(t, executor.Calls())
	assert.Zero(t, client.AudioOrderingFailures())
}

func TestDurableWebSearchTimeoutDoesNotInterruptVoice(t *testing.T) {
	executor := fakesearch.New(fakesearch.WithError(context.DeadlineExceeded))
	harness := newDurableSearchHarness(t, withSearchExecutor(executor))
	client := harness.ConnectVoice(t)
	client.ApplyPolicy(t, toolpolicy.DefaultPolicy())
	handoffID := client.StartSearchTurn(t, "Exercise a bounded timeout")
	client.SendAndReceivePCMWhile(t, handoffID, 8)
	card := client.WaitForTerminalCard(t, handoffID)
	assert.Equal(t, "failed", card.Status)
	assert.Zero(t, client.AudioOrderingFailures())
}

func TestDurableWebSearchDeepSeekFailureDoesNotInterruptVoice(t *testing.T) {
	harness := newDurableSearchHarness(t, withDeepSeekFailure())
	client := harness.ConnectVoice(t)
	client.ApplyPolicy(t, toolpolicy.DefaultPolicy())
	handoffID := client.StartSearchTurn(t, "Exercise a bounded reasoner failure")
	client.SendAndReceivePCMWhile(t, handoffID, 8)
	card := client.WaitForTerminalCard(t, handoffID)
	assert.Equal(t, "failed", card.Status)
	assert.Zero(t, harness.executor.Calls())
	assert.Zero(t, client.AudioOrderingFailures())
}

func TestDurableWebSearchGatewayReconnectReplaysPolicyAndOneCard(t *testing.T) {
	harness := newDurableSearchHarness(t)
	client := harness.ConnectVoice(t)
	policy := toolpolicy.DefaultPolicy()
	client.ApplyPolicy(t, policy)
	handoffID := client.StartSearchTurn(t, "Recover the durable search card")
	require.Eventually(t, func() bool { return harness.executor.Calls() == 1 }, integrationTimeout, 10*time.Millisecond)
	client.finishOriginForReconnect(t)

	reconnected := newReconnectClient()
	reconnectCtx, cancelReconnect := context.WithCancel(context.Background())
	reconnectResult := make(chan error, 1)
	go func() {
		reconnectResult <- harness.manager.Reconnect(reconnectCtx, session.ReconnectRequest{
			Scope: harness.scope, SessionID: "session-e2e", LastIncarnation: 1, LastEventSeq: 0,
		}, reconnected)
	}()
	t.Cleanup(func() {
		cancelReconnect()
		select {
		case err := <-reconnectResult:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("reconnect cleanup failed: %v", err)
			}
		case <-time.After(integrationTimeout):
			t.Error("reconnect cleanup timed out")
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), integrationTimeout)
	defer cancel()
	newVoice, err := harness.connector.NextSession(ctx)
	require.NoError(t, err)
	recovered := &durableSearchClient{harness: harness, voice: newVoice, cards: make(map[string]*durableSearchCard)}
	require.Equal(t, "session.created", readReconnectEvent(t, reconnected).EventType())
	reconnected.reads <- protocol.LiveToolPolicyUpdateEvent{Type: "live.tool_policy.update", EventID: "policy-reconnect", Tools: policy}

	acknowledged := false
	for !acknowledged || recovered.cards[handoffID] == nil || !containsPhase(recovered.cards[handoffID].Phases, "searching") {
		event := readReconnectEvent(t, reconnected)
		if ack, ok := event.(protocol.LiveToolPolicyUpdatedEvent); ok {
			assert.Equal(t, policy, ack.Tools)
			assert.Equal(t, "deepseek-v4-flash", ack.Model)
			acknowledged = true
			continue
		}
		recovered.consumeServerEvent(t, event)
	}
	harness.executor.Release()
	for recovered.cards[handoffID].Status != "completed" {
		recovered.consumeServerEvent(t, readReconnectEvent(t, reconnected))
	}
	card := recovered.cards[handoffID]
	assert.Equal(t, []string{"queued", "searching", "reading_sources", "preparing_answer", "completed"}, card.Phases)
	assert.Equal(t, []string{handoffID}, recovered.cardOrder)
	assert.Equal(t, 1, harness.executor.Calls(), "gateway replay must not duplicate committed worker execution")
}

type durableSearchOption func(*durableSearchOptions)

type durableSearchOptions struct {
	executor     *fakesearch.Executor
	deepSeekFail bool
}

func withSearchExecutor(executor *fakesearch.Executor) durableSearchOption {
	return func(options *durableSearchOptions) { options.executor = executor }
}

func withDeepSeekFailure() durableSearchOption {
	return func(options *durableSearchOptions) { options.deepSeekFail = true }
}

type durableSearchHarness struct {
	repository *postgres.Repository
	service    handoff.Service
	manual     *clock.Manual
	scope      handoff.Scope
	manager    *session.Manager
	connector  *observedConnector
	server     *httptest.Server
	executor   *fakesearch.Executor
	registry   worker.ToolRegistry
	reasoner   *fakeSearchDeepSeek
	newRunner  func(string, clock.Clock) *worker.Runner
	workerMu   sync.Mutex
	worker     *durableWorkerProcess
}

type durableWorkerProcess struct {
	cancel context.CancelFunc
	done   chan error
	once   sync.Once
	err    error
}

func (process *durableWorkerProcess) stop(t *testing.T) {
	t.Helper()
	process.once.Do(func() {
		process.cancel()
		select {
		case process.err = <-process.done:
		case <-time.After(integrationTimeout):
			process.err = errors.New("durable worker stop timed out")
		}
	})
	require.NoError(t, process.err)
}

func newDurableSearchHarness(t *testing.T, configure ...durableSearchOption) *durableSearchHarness {
	t.Helper()
	if os.Getenv("TEST_DATABASE_URL") == "" {
		t.Skip("TEST_DATABASE_URL is required before opening the durable boundary")
	}
	options := durableSearchOptions{executor: fakesearch.New(fakesearch.Controlled())}
	for _, apply := range configure {
		apply(&options)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	pool := testkit.Postgres(t)
	require.NoError(t, migrations.Up(t.Context(), pool))
	manual := clock.NewManual(now)
	registry, err := worker.NewToolRegistry(map[string][]worker.RegisteredTool{"tools-web-v1": {fakesearch.RegisteredTool(options.executor)}})
	require.NoError(t, err)
	repository, err := postgres.NewRepository(pool, manual, postgres.WithToolGate(registry))
	require.NoError(t, err)
	scope := handoff.Scope{TenantID: "tenant-web-search", UserID: "user-web-search", ConversationID: "conversation-e2e"}
	_, err = pool.Exec(t.Context(), `INSERT INTO conversations(tenant_id,id,user_id,retention_until,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$5)`,
		scope.TenantID, scope.ConversationID, scope.UserID, now.Add(24*time.Hour), now)
	require.NoError(t, err)
	service, err := handoff.NewService(repository, manual, handoffIDs{})
	require.NoError(t, err)
	policyRaw, err := toolpolicy.MarshalCanonical(toolpolicy.DefaultPolicy())
	require.NoError(t, err)

	connector := newObservedConnector(128)
	dispatcher := &deterministicDispatcher{work: make(chan gateway.HandoffWork, 8), snapshot: gateway.SessionHandoffSnapshot{
		ContextRef: "context://web-search", ContextHash: "sha256:web-search", Deadline: now.Add(time.Hour), TokenBudget: 512, Priority: 12,
		ToolAllowlist: []string{toolpolicy.WebSearchTool}, ToolPolicy: policyRaw, ApprovalPolicy: json.RawMessage(`{"mode":"none"}`),
		Versions:         handoff.Versions{Model: "deepseek-v4-flash", Prompt: "prompt-web-v1", ToolRegistry: "tools-web-v1", Config: "config-web-v1"},
		Profile:          handoff.LegacyProfileSnapshot(),
		PresentationHint: "compact",
	}}
	gates := feature.New(map[string]bool{feature.HandoffsEnabled: true})
	wakeups := make(chan handoff.EventWakeup, 64)
	pump := &handoff.EventPump{Reader: repository, Cursors: repository, Notifications: wakeups, Clock: clock.Real{}, PollInterval: 10 * time.Millisecond, Gates: gates}
	manager := session.NewManager(session.Config{
		IngressFrames: 128, EgressFrames: 128, ControlEvents: 128, WriterControlEvents: 64,
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
		DurableDispatcher: dispatcher, HandoffConsumerID: "web-search-client", HandoffSubscriptions: pump,
		PresentationPolicy: presentation.NewPolicy(presentation.Config{}), SafetyPolicy: allowPolicy{}, FeatureGates: gates,
		RecoveryStore: repository, ApprovalStore: repository, ToolCatalog: toolpolicy.DefaultCatalog(), Clock: clock.Real{},
	}, connector, deterministicIDs{})
	pump.Sink = manager
	transcripts := gateway.NewTranscriptCoordinator(repository, manager, 16)
	handoffs := gateway.NewHandoffCoordinator(service, manager, 16)
	require.NoError(t, handoffs.SetFeatureGates(gates))
	dispatcher.setCoordinators(transcripts, handoffs)
	listener, err := postgres.NewListener(pool, clock.Real{}, zeroJitter{}, postgres.ListenerConfig{ReconnectDelay: 10 * time.Millisecond})
	require.NoError(t, err)
	componentCtx, cancelComponents := context.WithCancel(context.Background())
	componentResults := make(chan error, 4)
	go func() { componentResults <- transcripts.Run(componentCtx) }()
	go func() { componentResults <- handoffs.Run(componentCtx) }()
	go func() { componentResults <- pump.Run(componentCtx) }()
	go func() { componentResults <- listener.Run(componentCtx, wakeups) }()
	t.Cleanup(func() {
		cancelComponents()
		for range 4 {
			select {
			case componentErr := <-componentResults:
				if componentErr != nil && !errors.Is(componentErr, context.Canceled) {
					t.Errorf("durable component cleanup failed: %v", componentErr)
				}
			case <-time.After(integrationTimeout):
				t.Error("durable component cleanup timed out")
			}
		}
	})
	handler := transportws.NewHandler(transportws.Options{MaxFrameBytes: 16 << 10, MaxAudioBytes: 8 << 10, ReadTimeout: integrationTimeout, WriteTimeout: integrationTimeout}, manager,
		transportws.AdmissionFunc(func(context.Context, *http.Request) (session.Subject, func(), error) {
			return session.Subject{TenantID: scope.TenantID, UserID: scope.UserID, Scopes: map[string]struct{}{"realtime": {}}, ExpiresAt: time.Now().Add(time.Hour)}, func() {}, nil
		}))
	server := httptest.NewServer(handler)
	t.Cleanup(func() { server.CloseClientConnections(); server.Close() })

	reasonerServer := newFakeSearchDeepSeek(t, options.deepSeekFail)
	reasonerClient, err := deepseek.New(deepseek.Config{BaseURL: reasonerServer.URL(), MaxOutputBytes: 8 << 10}, reasonerServer.Client())
	require.NoError(t, err)
	newRunner := func(workerID string, runnerClock clock.Clock) *worker.Runner {
		sealer, sealErr := worker.NewAESArgumentSealer("task11-key", bytes.Repeat([]byte{7}, 32), rand.Reader)
		require.NoError(t, sealErr)
		runner, runnerErr := worker.NewRunner(worker.Runner{
			Store: repository, Reasoner: reasonerClient, Safety: allowPolicy{}, Tools: registry, ArgumentSealer: sealer,
			Clock: runnerClock, Jitter: zeroJitter{}, Retry: worker.RetryPolicy{Base: 20 * time.Millisecond, Maximum: 50 * time.Millisecond, MaxRetries: 1},
			WorkerID: workerID, LeaseDuration: 3 * time.Second, HeartbeatInterval: time.Second, Concurrency: 1, PollInterval: 10 * time.Millisecond,
			MaxOutputBytes: 16 << 10, MaxOutputTokens: 256, MaxToolRounds: 1, MaxToolCalls: 1, MaxToolArgumentBytes: 1024, MaxToolOutputBytes: 8 << 10,
			ProviderSafetyID: func(string, string) (string, error) { return "safety-task11", nil },
		})
		require.NoError(t, runnerErr)
		return runner
	}
	harness := &durableSearchHarness{repository: repository, service: service, manual: manual, scope: scope, manager: manager, connector: connector,
		server: server, executor: options.executor, registry: registry, reasoner: reasonerServer, newRunner: newRunner}
	harness.startWorker(t, "worker-task11", clock.Real{})
	t.Cleanup(func() {
		harness.workerMu.Lock()
		process := harness.worker
		harness.workerMu.Unlock()
		if process != nil {
			process.stop(t)
		}
	})
	return harness
}

func (h *durableSearchHarness) startWorker(t *testing.T, workerID string, runnerClock clock.Clock) {
	t.Helper()
	runner := h.newRunner(workerID, runnerClock)
	ctx, cancel := context.WithCancel(context.Background())
	process := &durableWorkerProcess{cancel: cancel, done: make(chan error, 1)}
	go func() { process.done <- runner.Run(ctx) }()
	h.workerMu.Lock()
	h.worker = process
	h.workerMu.Unlock()
}

func (h *durableSearchHarness) RestartWorker(t *testing.T) {
	t.Helper()
	require.Eventually(t, func() bool { return h.executor.Calls() == 1 }, integrationTimeout, 10*time.Millisecond)
	h.workerMu.Lock()
	old := h.worker
	h.workerMu.Unlock()
	old.stop(t)
	h.manual.Advance(10 * time.Second)
	h.startWorker(t, "worker-task11-restarted", h.manual)
	require.Eventually(t, func() bool { return h.executor.Calls() == 2 }, integrationTimeout, 10*time.Millisecond)
	h.executor.Release()
}

func (h *durableSearchHarness) ConnectVoice(t *testing.T) *durableSearchClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), integrationTimeout)
	defer cancel()
	conn, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(h.server.URL, "http")+"/v1/realtime", nil)
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.CloseNow() })
	voice, err := h.connector.NextSession(ctx)
	require.NoError(t, err)
	client := &durableSearchClient{harness: h, conn: conn, voice: voice, cards: make(map[string]*durableSearchCard)}
	client.readType(t, "session.created")
	return client
}

type fakeSearchDeepSeek struct {
	server        *httptest.Server
	fail          bool
	requests      atomic.Int64
	toolDecisions atomic.Int64
	finalAnswers  atomic.Int64
}

func newFakeSearchDeepSeek(t *testing.T, fail bool) *fakeSearchDeepSeek {
	t.Helper()
	fake := &fakeSearchDeepSeek{fail: fail}
	fake.server = httptest.NewServer(http.HandlerFunc(fake.serveHTTP))
	t.Cleanup(func() { fake.server.CloseClientConnections(); fake.server.Close() })
	return fake
}

func (f *fakeSearchDeepSeek) URL() string          { return f.server.URL }
func (f *fakeSearchDeepSeek) Client() *http.Client { return f.server.Client() }

func (f *fakeSearchDeepSeek) serveHTTP(writer http.ResponseWriter, request *http.Request) {
	f.requests.Add(1)
	defer request.Body.Close()
	body, err := io.ReadAll(io.LimitReader(request.Body, 1<<20))
	if err != nil || len(body) == 1<<20 {
		http.Error(writer, "invalid request", http.StatusBadRequest)
		return
	}
	if f.fail {
		http.Error(writer, "unavailable", http.StatusServiceUnavailable)
		return
	}
	var decoded struct {
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
		Thinking struct {
			Type string `json:"type"`
		} `json:"thinking"`
	}
	if json.Unmarshal(body, &decoded) != nil || decoded.Thinking.Type != "disabled" {
		http.Error(writer, "invalid request", http.StatusBadRequest)
		return
	}
	hasToolOutput := false
	for _, message := range decoded.Messages {
		if message.Role == "tool" {
			hasToolOutput = strings.Contains(message.Content, `"snippet":"Build simple, secure, scalable systems."`)
		}
	}
	writer.Header().Set("Content-Type", "text/event-stream")
	if !hasToolOutput {
		f.toolDecisions.Add(1)
		_, _ = io.WriteString(writer, "data: {\"id\":\"task11-tool\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"search-call\",\"type\":\"function\",\"function\":{\"name\":\"web_search\",\"arguments\":\"{\\\"query\\\":\\\"private-search-marker\\\"}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n")
		return
	}
	f.finalAnswers.Add(1)
	_, _ = io.WriteString(writer, "data: {\"id\":\"task11-final\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"The official Go website is go.dev.\"},\"finish_reason\":\"stop\"}]}\n\ndata: {\"id\":\"task11-final\",\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2}}\n\ndata: [DONE]\n\n")
}

type durableSearchSource struct {
	Title string `json:"title"`
	URL   string `json:"url"`
}

type durableSearchCard struct {
	ID       string
	Status   string
	Sequence int64
	Phases   []string
	Sources  []durableSearchSource
}

type deliveredProgress struct {
	Sequence int64
	EventID  string
	Phase    string
}

type durableSearchClient struct {
	harness      *durableSearchHarness
	conn         *websocket.Conn
	voice        *observedVoice
	cards        map[string]*durableSearchCard
	cardOrder    []string
	rawFrames    []json.RawMessage
	progressSeen []deliveredProgress
	policy       toolpolicy.Policy
	responseID   string
	epoch        identity.ResponseEpoch
	itemID       string
	audioFailure atomic.Int64
}

func (c *durableSearchClient) ApplyPolicy(t *testing.T, policy toolpolicy.Policy) {
	t.Helper()
	writeClientEvent(t, c.conn, protocol.LiveToolPolicyUpdateEvent{Type: "live.tool_policy.update", EventID: "policy-task11", Tools: policy})
	raw := c.readType(t, "live.tool_policy.updated")
	var acknowledged protocol.LiveToolPolicyUpdatedEvent
	require.NoError(t, json.Unmarshal(raw, &acknowledged))
	assert.Equal(t, policy, acknowledged.Tools)
	assert.Equal(t, "deepseek-v4-flash", acknowledged.Model)
	c.policy = acknowledged.Tools
}

func (c *durableSearchClient) StartSearchTurn(t *testing.T, request string) string {
	t.Helper()
	emitVoice(t, c.voice, realtime.Event{Kind: realtime.EventSpeechStarted, ProviderEventID: "task11-speech-started", ItemID: "turn-task11"})
	c.readType(t, "input_audio_buffer.speech_started")
	emitVoice(t, c.voice, realtime.Event{Kind: realtime.EventSpeechStopped, ProviderEventID: "task11-speech-stopped", ItemID: "turn-task11"})
	c.readType(t, "input_audio_buffer.speech_stopped")
	emitVoice(t, c.voice, realtime.Event{Kind: realtime.EventTranscriptionCompleted, ProviderEventID: "task11-transcript", ItemID: "turn-task11", Text: request})
	c.readType(t, "conversation.item.input_audio_transcription.completed")
	writeClientEvent(t, c.conn, protocol.ResponseCreateEvent{Type: "response.create"})
	create := nextVoiceCommand(t, c.voice)
	require.Equal(t, realtime.CommandCreateResponse, create.Kind)
	c.epoch, c.responseID, c.itemID = create.Epoch, "voice-task11", "audio-task11"
	emitVoice(t, c.voice, realtime.Event{Kind: realtime.EventResponseCreated, Epoch: c.epoch, ProviderEventID: "task11-response-created", ResponseID: c.responseID, Status: "in_progress"})
	c.readType(t, "response.created")
	arguments, err := json.Marshal(map[string]string{"objective": request})
	require.NoError(t, err)
	emitVoice(t, c.voice, realtime.Event{Kind: realtime.EventFunctionCallDone, Epoch: c.epoch, ProviderEventID: "task11-handoff", ResponseID: c.responseID,
		ItemID: "tool-task11", CallID: "call-task11", Name: handoff.ToolStartBackgroundTask, Arguments: arguments})
	c.readType(t, "response.function_call_arguments.done")
	createdRaw := c.readType(t, "live.handoff.created")
	var created protocol.LiveHandoffCreatedEvent
	require.NoError(t, json.Unmarshal(createdRaw, &created))
	snapshot, err := c.harness.service.Get(t.Context(), c.harness.scope, identity.HandoffID(created.HandoffID))
	require.NoError(t, err)
	wantPolicy, err := toolpolicy.MarshalCanonical(c.policy)
	require.NoError(t, err)
	assert.Equal(t, string(wantPolicy), string(snapshot.Request.ToolPolicy))
	if c.policy.WebSearch.Enabled {
		assert.Equal(t, []string{toolpolicy.WebSearchTool}, snapshot.Request.ToolAllowlist)
	} else {
		assert.Empty(t, snapshot.Request.ToolAllowlist)
	}
	return created.HandoffID
}

func (c *durableSearchClient) SendAndReceivePCMWhile(t *testing.T, _ string, frames int) {
	t.Helper()
	for sequence := uint32(1); sequence <= uint32(frames); sequence++ {
		if sequence == 5 {
			c.bargeIn(t, sequence)
		}
		pcm := loadgateway.WatermarkPCM(sequence)
		writeClientEvent(t, c.conn, protocol.InputAudioBufferAppendEvent{Type: "input_audio_buffer.append", Audio: base64.StdEncoding.EncodeToString(pcm)})
		forwarded := nextVoiceCommand(t, c.voice)
		if forwarded.Kind != realtime.CommandAppendAudio || !bytes.Equal(forwarded.Audio, pcm) {
			c.audioFailure.Add(1)
		}
		emitVoice(t, c.voice, realtime.Event{Kind: realtime.EventAudioDelta, Epoch: c.epoch, ProviderEventID: "task11-audio", ResponseID: c.responseID, ItemID: c.itemID, Audio: pcm})
		raw := c.readType(t, "response.output_audio.delta")
		var delta protocol.ResponseOutputAudioDeltaEvent
		if json.Unmarshal(raw, &delta) != nil {
			c.audioFailure.Add(1)
			continue
		}
		decoded, err := base64.StdEncoding.DecodeString(delta.Delta)
		observed, ok := loadgateway.WatermarkSequence(decoded)
		if err != nil || !ok || observed != sequence {
			c.audioFailure.Add(1)
		}
	}
}

func (c *durableSearchClient) bargeIn(t *testing.T, sequence uint32) {
	t.Helper()
	oldEpoch, oldResponse := c.epoch, c.responseID
	emitVoice(t, c.voice, realtime.Event{Kind: realtime.EventSpeechStarted, ProviderEventID: "task11-barge", ItemID: "turn-barge"})
	c.readType(t, "input_audio_buffer.speech_started")
	cancel := nextVoiceCommand(t, c.voice)
	require.Equal(t, realtime.CommandCancelResponse, cancel.Kind)
	require.Greater(t, cancel.Epoch, oldEpoch)
	emitVoice(t, c.voice, realtime.Event{Kind: realtime.EventResponseDone, Epoch: cancel.Epoch, ProviderEventID: "task11-cancelled", ResponseID: oldResponse, Status: "cancelled"})
	c.readType(t, "response.done")
	require.Equal(t, realtime.CommandCreateItem, nextVoiceCommand(t, c.voice).Kind)
	followup := nextVoiceCommand(t, c.voice)
	require.Equal(t, realtime.CommandCreateResponse, followup.Kind)
	c.epoch, c.responseID, c.itemID = followup.Epoch, "voice-task11-followup", "audio-task11-followup"
	emitVoice(t, c.voice, realtime.Event{Kind: realtime.EventResponseCreated, Epoch: c.epoch, ProviderEventID: "task11-followup-created", ResponseID: c.responseID, Status: "in_progress"})
	c.readType(t, "response.created")
	emitVoice(t, c.voice, realtime.Event{Kind: realtime.EventSpeechStopped, ProviderEventID: "task11-barge-stopped", ItemID: "turn-barge"})
	c.readType(t, "input_audio_buffer.speech_stopped")
	emitVoice(t, c.voice, realtime.Event{Kind: realtime.EventAudioDelta, Epoch: oldEpoch, ProviderEventID: "task11-stale", ResponseID: oldResponse, ItemID: "stale-task11", Audio: loadgateway.WatermarkPCM(sequence - 1)})
}

func (c *durableSearchClient) finishOriginForReconnect(t *testing.T) {
	t.Helper()
	writeClientEvent(t, c.conn, protocol.ResponseCancelEvent{Type: "response.cancel", ResponseID: c.responseID})
	cancel := nextVoiceCommand(t, c.voice)
	require.Equal(t, realtime.CommandCancelResponse, cancel.Kind)
	emitVoice(t, c.voice, realtime.Event{Kind: realtime.EventResponseDone, Epoch: cancel.Epoch, ProviderEventID: "task11-reconnect-origin-done", ResponseID: c.responseID, Status: "cancelled"})
	c.readType(t, "response.done")
	require.Equal(t, realtime.CommandCreateItem, nextVoiceCommand(t, c.voice).Kind)
	followup := nextVoiceCommand(t, c.voice)
	require.Equal(t, realtime.CommandCreateResponse, followup.Kind)
	emitVoice(t, c.voice, realtime.Event{Kind: realtime.EventResponseCreated, Epoch: followup.Epoch, ProviderEventID: "task11-reconnect-followup", ResponseID: "task11-reconnect-followup", Status: "in_progress"})
	c.readType(t, "response.created")
	emitVoice(t, c.voice, realtime.Event{Kind: realtime.EventResponseDone, Epoch: followup.Epoch, ProviderEventID: "task11-reconnect-followup-done", ResponseID: "task11-reconnect-followup", Status: "completed"})
	c.readType(t, "response.done")
}

func (c *durableSearchClient) WaitForTerminalCard(t *testing.T, handoffID string) durableSearchCard {
	t.Helper()
	deadline := time.Now().Add(integrationTimeout)
	for time.Now().Before(deadline) {
		if card := c.cards[handoffID]; card != nil && (card.Status == "completed" || card.Status == "failed" || card.Status == "cancelled") {
			assert.Equal(t, []string{handoffID}, c.cardOrder, "browser timeline must retain one stable card")
			for _, frame := range c.rawFrames {
				assert.NotContains(t, string(frame), "private-search-marker")
				assert.NotContains(t, string(frame), "Build simple, secure, scalable systems.")
			}
			return *card
		}
		c.readAny(t, time.Until(deadline))
	}
	t.Fatal("durable search terminal card timed out")
	return durableSearchCard{}
}

func (c *durableSearchClient) AudioOrderingFailures() int { return int(c.audioFailure.Load()) }

func (c *durableSearchClient) DeliveredProgress() []deliveredProgress {
	return append([]deliveredProgress(nil), c.progressSeen...)
}

func (c *durableSearchClient) readType(t *testing.T, wanted string) json.RawMessage {
	t.Helper()
	deadline := time.Now().Add(integrationTimeout)
	for time.Now().Before(deadline) {
		raw, eventType := c.readAny(t, time.Until(deadline))
		if eventType == wanted {
			return raw
		}
	}
	t.Fatalf("client event %q timed out", wanted)
	return nil
}

func (c *durableSearchClient) readAny(t *testing.T, timeout time.Duration) (json.RawMessage, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	kind, raw, err := c.conn.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, websocket.MessageText, kind)
	owned := append(json.RawMessage(nil), raw...)
	c.rawFrames = append(c.rawFrames, owned)
	var envelope struct {
		Type string `json:"type"`
	}
	require.NoError(t, json.Unmarshal(raw, &envelope))
	c.consumeCard(t, envelope.Type, raw)
	return owned, envelope.Type
}

func (c *durableSearchClient) consumeCard(t *testing.T, eventType string, raw json.RawMessage) {
	t.Helper()
	var event struct {
		HandoffID  string          `json:"handoff_id"`
		EventID    string          `json:"event_id"`
		SequenceID int64           `json:"sequence_id"`
		Status     string          `json:"status"`
		Details    json.RawMessage `json:"details"`
	}
	switch eventType {
	case "live.handoff.created", "live.handoff.progress", "live.handoff.completed", "live.handoff.failed", "live.handoff.cancelled":
		if json.Unmarshal(raw, &event) != nil || event.HandoffID == "" {
			t.Fatal("invalid durable search card event")
		}
	default:
		return
	}
	progressPhase := ""
	if eventType == "live.handoff.progress" {
		var details struct {
			Phase string `json:"phase"`
		}
		if json.Unmarshal(event.Details, &details) == nil && details.Phase != "" {
			progressPhase = details.Phase
			c.progressSeen = append(c.progressSeen, deliveredProgress{Sequence: event.SequenceID, EventID: event.EventID, Phase: details.Phase})
		}
	}
	card := c.cards[event.HandoffID]
	if card == nil {
		card = &durableSearchCard{ID: event.HandoffID}
		c.cards[event.HandoffID] = card
		c.cardOrder = append(c.cardOrder, event.HandoffID)
	}
	if event.SequenceID <= card.Sequence {
		return
	}
	card.Sequence = event.SequenceID
	switch eventType {
	case "live.handoff.created":
		card.Status = "queued"
		card.Phases = appendPhase(card.Phases, "queued")
	case "live.handoff.progress":
		card.Status = event.Status
		if progressPhase != "" {
			card.Phases = appendPhase(card.Phases, progressPhase)
		}
	case "live.handoff.completed":
		card.Status = "completed"
		card.Phases = appendPhase(card.Phases, "completed")
		var details struct {
			Structured struct {
				Sources []durableSearchSource `json:"sources"`
			} `json:"structured"`
		}
		require.NoError(t, json.Unmarshal(event.Details, &details))
		card.Sources = append([]durableSearchSource(nil), details.Structured.Sources...)
	case "live.handoff.failed":
		card.Status = "failed"
		card.Phases = appendPhase(card.Phases, "failed")
	case "live.handoff.cancelled":
		card.Status = "cancelled"
		card.Phases = appendPhase(card.Phases, "cancelled")
	}
}

func (c *durableSearchClient) consumeServerEvent(t *testing.T, event protocol.ServerEvent) {
	t.Helper()
	raw, err := json.Marshal(event)
	require.NoError(t, err)
	c.rawFrames = append(c.rawFrames, append(json.RawMessage(nil), raw...))
	c.consumeCard(t, event.EventType(), raw)
}

func containsPhase(phases []string, wanted string) bool {
	for _, phase := range phases {
		if phase == wanted {
			return true
		}
	}
	return false
}

func appendPhase(phases []string, phase string) []string {
	if len(phases) == 0 || phases[len(phases)-1] != phase {
		return append(phases, phase)
	}
	return phases
}
