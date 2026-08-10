//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
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
	"github.com/chuanmingliu/openai-gpt-live/internal/reasoner"
	"github.com/chuanmingliu/openai-gpt-live/internal/reasoner/deepseek"
	"github.com/chuanmingliu/openai-gpt-live/internal/safety"
	"github.com/chuanmingliu/openai-gpt-live/internal/session"
	"github.com/chuanmingliu/openai-gpt-live/internal/store/postgres"
	"github.com/chuanmingliu/openai-gpt-live/internal/testkit"
	transportws "github.com/chuanmingliu/openai-gpt-live/internal/transport/ws"
	"github.com/chuanmingliu/openai-gpt-live/internal/worker"
	"github.com/chuanmingliu/openai-gpt-live/migrations"
	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const integrationTimeout = 8 * time.Second

type deterministicIDs struct{}

func (deterministicIDs) NewSessionID() identity.SessionID           { return "session-e2e" }
func (deterministicIDs) NewConversationID() identity.ConversationID { return "conversation-e2e" }

type sequenceSessionIDs struct {
	sessions      atomic.Uint64
	conversations atomic.Uint64
}

func (i *sequenceSessionIDs) NewSessionID() identity.SessionID {
	return identity.SessionID("session-boundary-" + strconv.FormatUint(i.sessions.Add(1), 10))
}

func (i *sequenceSessionIDs) NewConversationID() identity.ConversationID {
	return identity.ConversationID("conversation-boundary-" + strconv.FormatUint(i.conversations.Add(1), 10))
}

type handoffIDs struct{}

func (handoffIDs) NewHandoffID() identity.HandoffID { return "handoff-e2e" }
func (handoffIDs) NewEventID() string               { return "event-created-e2e" }

type zeroJitter struct{}

func (zeroJitter) Duration(time.Duration) time.Duration { return 0 }

type allowPolicy struct{}

func (allowPolicy) EvaluateWorkerResult(context.Context, safety.WorkerResultCandidate) (safety.Decision, error) {
	return safety.Decision{Action: safety.ActionAllow, ReasonCode: "test_allow"}, nil
}

type suppressPolicy struct{}

func (suppressPolicy) EvaluateWorkerResult(context.Context, safety.WorkerResultCandidate) (safety.Decision, error) {
	return safety.Decision{Action: safety.ActionSuppress, ReasonCode: "test_suppress"}, nil
}

type toggleSafetyPolicy struct{ suppress atomic.Bool }

func (p *toggleSafetyPolicy) EvaluateWorkerResult(context.Context, safety.WorkerResultCandidate) (safety.Decision, error) {
	if p.suppress.Load() {
		return safety.Decision{Action: safety.ActionSuppress, ReasonCode: "test_suppress"}, nil
	}
	return safety.Decision{Action: safety.ActionAllow, ReasonCode: "test_allow"}, nil
}

type deterministicOptions struct {
	dropNotifications  bool
	gates              *feature.Gates
	presentationSafety safety.Policy
	reasonerTool       bool
	toolSideEffecting  bool
	duplicateComplete  bool
	approvalPolicy     json.RawMessage
	approvalTTL        time.Duration
}

type deterministicOption func(*deterministicOptions)

func withoutListenerDelivery(options *deterministicOptions) { options.dropNotifications = true }
func withFeatureGates(gates *feature.Gates) deterministicOption {
	return func(options *deterministicOptions) { options.gates = gates }
}
func withPresentationSafety(policy safety.Policy) deterministicOption {
	return func(options *deterministicOptions) { options.presentationSafety = policy }
}
func withReasonerTool(options *deterministicOptions) {
	options.reasonerTool = true
	options.approvalPolicy = json.RawMessage(`{"mode":"none"}`)
}
func withApprovalReasonerTool(options *deterministicOptions) {
	options.reasonerTool = true
	options.toolSideEffecting = true
	options.approvalPolicy = json.RawMessage(`{"mode":"ask"}`)
	options.approvalTTL = 5 * time.Second
}
func withDuplicateCompletion(options *deterministicOptions) { options.duplicateComplete = true }

type toolExecutor func(context.Context, worker.ToolExecutionRequest) (json.RawMessage, error)

func (f toolExecutor) Execute(ctx context.Context, request worker.ToolExecutionRequest) (json.RawMessage, error) {
	return f(ctx, request)
}

type completionStore struct {
	worker.Store
	completed chan struct{}
	once      sync.Once
	duplicate bool
	attempts  atomic.Int32
}

// observedConnector preserves the provider-bound command stream independently
// from the fake provider's receive queue. That lets disconnect tests prove a
// watermark reached the old provider queue without draining it first.
type observedConnector struct {
	inner    *testkit.FakeConnector
	sessions chan *observedVoice
}

type observedVoice struct {
	*testkit.FakeVoiceSession
	sent chan realtime.Command
}

func newObservedConnector(buffer int) *observedConnector {
	return &observedConnector{inner: testkit.NewFakeConnector(buffer), sessions: make(chan *observedVoice, buffer)}
}

func (c *observedConnector) Connect(ctx context.Context, request realtime.OpenRequest) (realtime.Session, error) {
	connected, err := c.inner.Connect(ctx, request)
	if err != nil {
		return nil, err
	}
	voice := &observedVoice{FakeVoiceSession: connected.(*testkit.FakeVoiceSession), sent: make(chan realtime.Command, 64)}
	select {
	case c.sessions <- voice:
		return voice, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *observedConnector) NextSession(ctx context.Context) (*observedVoice, error) {
	select {
	case voice := <-c.sessions:
		return voice, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (v *observedVoice) Send(ctx context.Context, command realtime.Command) error {
	if err := v.FakeVoiceSession.Send(ctx, command); err != nil {
		return err
	}
	command.Audio = append([]byte(nil), command.Audio...)
	select {
	case v.sent <- command:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func nextObservedCommand(t *testing.T, voice *observedVoice, wanted realtime.CommandKind) realtime.Command {
	t.Helper()
	deadline := time.NewTimer(integrationTimeout)
	defer deadline.Stop()
	for {
		select {
		case command := <-voice.sent:
			if command.Kind == wanted {
				return command
			}
		case <-deadline.C:
			t.Fatalf("provider did not accept command kind %d", wanted)
		}
	}
}

type reconnectClient struct {
	reads     chan protocol.ClientEvent
	writes    chan protocol.ServerEvent
	done      chan struct{}
	closeOnce sync.Once
}

func newReconnectClient() *reconnectClient {
	return &reconnectClient{reads: make(chan protocol.ClientEvent), writes: make(chan protocol.ServerEvent, 32), done: make(chan struct{})}
}

func (c *reconnectClient) Read(ctx context.Context) (protocol.ClientEvent, error) {
	select {
	case event := <-c.reads:
		return event, nil
	case <-c.done:
		return nil, context.Canceled
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *reconnectClient) Write(ctx context.Context, event protocol.ServerEvent) error {
	select {
	case c.writes <- event:
		return nil
	case <-c.done:
		return context.Canceled
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *reconnectClient) Close(context.Context, int, string) error {
	c.closeOnce.Do(func() { close(c.done) })
	return nil
}

func readReconnectEvent(t *testing.T, client *reconnectClient) protocol.ServerEvent {
	t.Helper()
	deadline := time.NewTimer(integrationTimeout)
	defer deadline.Stop()
	select {
	case event := <-client.writes:
		return event
	case <-deadline.C:
		t.Fatal("reconnect client event receive timed out")
		return nil
	}
}

func (s *completionStore) Complete(ctx context.Context, token worker.LeaseToken, record worker.CompletionRecord) error {
	s.attempts.Add(1)
	if err := s.Store.Complete(ctx, token, record); err != nil {
		return err
	}
	if s.duplicate {
		s.attempts.Add(1)
		if err := s.Store.Complete(ctx, token, record); err != nil {
			return err
		}
	}
	s.once.Do(func() { close(s.completed) })
	return nil
}

type deterministicDispatcher struct {
	mu          sync.RWMutex
	transcripts *gateway.TranscriptCoordinator
	handoffs    *gateway.HandoffCoordinator
	snapshot    gateway.SessionHandoffSnapshot
	work        chan gateway.HandoffWork
}

func (d *deterministicDispatcher) DispatchTranscript(ctx context.Context, trusted session.TrustedSessionContext, effect session.TranscriptRequestedEffect) error {
	scope := handoff.Scope{TenantID: trusted.Subject.TenantID, UserID: trusted.Subject.UserID, ConversationID: trusted.IDs.ConversationID}
	d.mu.RLock()
	coordinator := d.transcripts
	d.mu.RUnlock()
	if coordinator == nil {
		return errors.New("deterministic transcript coordinator is unavailable")
	}
	return coordinator.Enqueue(gateway.TranscriptWork{SessionID: trusted.IDs.SessionID, Incarnation: trusted.IDs.Incarnation,
		Scope: scope, Turn: effect.Turn, Sequence: effect.Sequence, Role: effect.Role, FinalText: effect.FinalText})
}

func (d *deterministicDispatcher) DispatchHandoff(ctx context.Context, trusted session.TrustedSessionContext, effect session.HandoffRequestedEffect) error {
	scope := handoff.Scope{TenantID: trusted.Subject.TenantID, UserID: trusted.Subject.UserID, ConversationID: trusted.IDs.ConversationID}
	work := gateway.HandoffWork{SessionID: trusted.IDs.SessionID, Incarnation: trusted.IDs.Incarnation, OriginEpoch: effect.OriginEpoch,
		Scope: scope, CallID: effect.CallID, ResponseID: effect.ResponseID, ToolName: effect.ToolName,
		Arguments: append(json.RawMessage(nil), effect.Arguments...), Turn: effect.Turn, Transcript: effect.Transcript,
		Snapshot: d.snapshot, RejectReason: effect.RejectReason}
	select {
	case d.work <- work:
	default:
	}
	d.mu.RLock()
	coordinator := d.handoffs
	d.mu.RUnlock()
	if coordinator == nil {
		return errors.New("deterministic handoff coordinator is unavailable")
	}
	return coordinator.Enqueue(work)
}

func (d *deterministicDispatcher) setCoordinators(transcripts *gateway.TranscriptCoordinator, handoffs *gateway.HandoffCoordinator) {
	d.mu.Lock()
	d.transcripts, d.handoffs = transcripts, handoffs
	d.mu.Unlock()
}

type deterministicSystem struct {
	client           *websocket.Conn
	voice            *observedVoice
	connector        *observedConnector
	manager          *session.Manager
	repository       *postgres.Repository
	service          handoff.Service
	manual           *clock.Manual
	scope            handoff.Scope
	workerDone       <-chan struct{}
	reasonerStarted  <-chan struct{}
	reasonerGo       chan struct{}
	releaseReasoner  func()
	workerCancel     context.CancelFunc
	workerResult     <-chan error
	workerStopped    <-chan struct{}
	newRunner        func(string, worker.Store, clock.Clock) (*worker.Runner, error)
	server           *httptest.Server
	handoffWork      <-chan gateway.HandoffWork
	reasonerRequests <-chan struct{}
	toolExecutions   <-chan string
	toolInvocations  <-chan worker.ToolExecutionRequest
	completionStore  *completionStore
}

func startDeterministicSystem(t *testing.T, configure ...deterministicOption) *deterministicSystem {
	t.Helper()
	options := deterministicOptions{presentationSafety: allowPolicy{}, approvalPolicy: json.RawMessage(`{"mode":"ask"}`)}
	for _, apply := range configure {
		apply(&options)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	pool := testkit.Postgres(t)
	require.NoError(t, migrations.Up(t.Context(), pool))
	manual := clock.NewManual(now)
	toolExecutions := make(chan string, 8)
	toolInvocations := make(chan worker.ToolExecutionRequest, 8)
	noopTool := func(name string) worker.RegisteredTool {
		return worker.RegisteredTool{Definition: reasoner.ToolDefinition{Name: name, Parameters: json.RawMessage(`{"type":"object"}`)},
			SideEffecting: options.toolSideEffecting, ReadOnly: !options.toolSideEffecting, ParallelSafe: !options.toolSideEffecting, StableIdempotency: true,
			Executor: toolExecutor(func(_ context.Context, request worker.ToolExecutionRequest) (json.RawMessage, error) {
				select {
				case toolExecutions <- name:
				default:
				}
				select {
				case toolInvocations <- request:
				default:
				}
				return json.RawMessage(`{"ok":true}`), nil
			})}
	}
	registry, err := worker.NewToolRegistry(map[string][]worker.RegisteredTool{"tools-v3": {noopTool("fetch"), noopTool("search")}})
	require.NoError(t, err)
	repositoryOptions := []postgres.RepositoryOption{postgres.WithToolGate(registry)}
	if options.approvalTTL > 0 {
		repositoryOptions = append(repositoryOptions, postgres.WithApprovalTTL(options.approvalTTL))
	}
	repository, err := postgres.NewRepository(pool, manual, repositoryOptions...)
	require.NoError(t, err)
	scope := handoff.Scope{TenantID: "tenant-e2e", UserID: "user-e2e", ConversationID: "conversation-e2e"}
	_, err = pool.Exec(t.Context(), `INSERT INTO conversations(tenant_id,id,user_id,retention_until,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$5)`,
		scope.TenantID, scope.ConversationID, scope.UserID, now.Add(24*time.Hour), now)
	require.NoError(t, err)
	service, err := handoff.NewService(repository, manual, handoffIDs{})
	require.NoError(t, err)

	connector := newObservedConnector(8)
	dispatcher := &deterministicDispatcher{work: make(chan gateway.HandoffWork, 8), snapshot: gateway.SessionHandoffSnapshot{
		ContextRef: "context://e2e", ContextHash: "sha256:context-e2e", Deadline: now.Add(time.Hour), TokenBudget: 128, Priority: 12,
		ToolAllowlist: []string{"fetch", "search"}, ApprovalPolicy: options.approvalPolicy,
		Versions: handoff.Versions{Model: "model-v1", Prompt: "prompt-v2", ToolRegistry: "tools-v3", Config: "config-v4"}, PresentationHint: "brief",
	}}
	wakeups := make(chan handoff.EventWakeup, 32)
	var pumpNotifications <-chan handoff.EventWakeup = wakeups
	if options.dropNotifications {
		pumpNotifications = nil
	}
	pump := &handoff.EventPump{Reader: repository, Cursors: repository, Notifications: pumpNotifications, Clock: clock.Real{}, PollInterval: 10 * time.Millisecond, Gates: options.gates}
	manager := session.NewManager(session.Config{
		IngressFrames: 8, EgressFrames: 8, ControlEvents: 32, WriterControlEvents: 16,
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
		DurableDispatcher: dispatcher, HandoffConsumerID: "e2e-client", HandoffSubscriptions: pump,
		PresentationPolicy: presentation.NewPolicy(presentation.Config{}), SafetyPolicy: options.presentationSafety, FeatureGates: options.gates,
		RecoveryStore: repository, ApprovalStore: repository, Clock: clock.Real{},
	}, connector, deterministicIDs{})
	pump.Sink = manager
	transcripts := gateway.NewTranscriptCoordinator(repository, manager, 8)
	handoffs := gateway.NewHandoffCoordinator(service, manager, 8)
	require.NotNil(t, transcripts)
	require.NotNil(t, handoffs)
	if options.gates != nil {
		require.NoError(t, handoffs.SetFeatureGates(options.gates))
	}
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
					t.Errorf("deterministic component cleanup failed: %v", componentErr)
				}
			case <-time.After(integrationTimeout):
				t.Error("deterministic component cleanup timed out")
			}
		}
	})
	handler := transportws.NewHandler(transportws.Options{MaxFrameBytes: 16 << 10, MaxAudioBytes: 8 << 10, ReadTimeout: integrationTimeout, WriteTimeout: integrationTimeout}, manager,
		transportws.AdmissionFunc(func(context.Context, *http.Request) (session.Subject, func(), error) {
			return session.Subject{TenantID: scope.TenantID, UserID: scope.UserID, Scopes: map[string]struct{}{"realtime": {}}, ExpiresAt: time.Now().Add(time.Hour)}, func() {}, nil
		}))
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.CloseClientConnections()
		server.Close()
	})
	dialCtx, cancelDial := context.WithTimeout(t.Context(), integrationTimeout)
	defer cancelDial()
	client, response, err := websocket.Dial(dialCtx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/realtime", nil)
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.CloseNow() })
	voiceCtx, cancelVoice := context.WithTimeout(t.Context(), integrationTimeout)
	defer cancelVoice()
	voice, err := connector.NextSession(voiceCtx)
	require.NoError(t, err)

	reasonerGo := make(chan struct{})
	var releaseReasonerOnce sync.Once
	releaseReasoner := func() { releaseReasonerOnce.Do(func() { close(reasonerGo) }) }
	reasonerStarted := make(chan struct{})
	reasonerRequests := make(chan struct{}, 8)
	var reasonerOnce sync.Once
	var reasonerRequestCount atomic.Int32
	reasonerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reasonerOnce.Do(func() { close(reasonerStarted) })
		select {
		case reasonerRequests <- struct{}{}:
		default:
		}
		select {
		case <-reasonerGo:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		if options.reasonerTool && reasonerRequestCount.Add(1) == 1 {
			_, _ = w.Write([]byte("data: {\"id\":\"request-e2e\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"tool-call-e2e\",\"function\":{\"name\":\"fetch\",\"arguments\":\"{}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n"))
			return
		}
		reasonerRequestCount.Add(1)
		_, _ = w.Write([]byte("data: {\"id\":\"request-e2e\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"bounded\"},\"finish_reason\":\"stop\"}]}\n\n"))
		_, _ = w.Write([]byte("data: {\"id\":\"request-e2e\",\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2}}\n\ndata: [DONE]\n\n"))
	}))
	t.Cleanup(func() {
		reasonerServer.CloseClientConnections()
		reasonerServer.Close()
	})
	t.Cleanup(releaseReasoner)
	reasonerClient, err := deepseek.New(deepseek.Config{BaseURL: reasonerServer.URL, MaxOutputBytes: 1024}, reasonerServer.Client())
	require.NoError(t, err)
	newRunner := func(workerID string, store worker.Store, runnerClock clock.Clock) (*worker.Runner, error) {
		argumentSealer, err := worker.NewAESArgumentSealer("e2e-tool-key", []byte("0123456789abcdef0123456789abcdef"), rand.Reader)
		if err != nil {
			return nil, err
		}
		return worker.NewRunner(worker.Runner{
			Store: store, Reasoner: reasonerClient, Safety: allowPolicy{}, Tools: registry,
			ArgumentSealer: argumentSealer,
			Clock:          runnerClock, Jitter: zeroJitter{}, Retry: worker.RetryPolicy{Base: 20 * time.Millisecond, Maximum: 50 * time.Millisecond, MaxRetries: 1},
			WorkerID: workerID, LeaseDuration: 3 * time.Second, HeartbeatInterval: time.Second, Concurrency: 1, PollInterval: 10 * time.Millisecond,
			MaxOutputBytes: 4096, MaxOutputTokens: 128, MaxToolRounds: 1, MaxToolCalls: 1, MaxToolArgumentBytes: 1024, MaxToolOutputBytes: 1024,
			ProviderSafetyID: func(string, string) (string, error) { return "safety-e2e", nil },
		})
	}
	completed := make(chan struct{})
	store := &completionStore{Store: repository, completed: completed, duplicate: options.duplicateComplete}
	runner, err := newRunner("worker-e2e", store, clock.Real{})
	require.NoError(t, err)
	workerCtx, workerCancel := context.WithCancel(context.Background())
	workerResult := make(chan error, 1)
	workerStopped := make(chan struct{})
	go func() {
		defer close(workerStopped)
		workerResult <- runner.Run(workerCtx)
	}()
	t.Cleanup(func() {
		workerCancel()
		select {
		case err := <-workerResult:
			assert.NoError(t, err)
		case <-time.After(integrationTimeout):
			t.Error("worker cleanup timed out")
		}
	})
	return &deterministicSystem{client: client, voice: voice, connector: connector, manager: manager, repository: repository, service: service, manual: manual, scope: scope,
		workerDone: completed, reasonerStarted: reasonerStarted, reasonerGo: reasonerGo, releaseReasoner: releaseReasoner, reasonerRequests: reasonerRequests,
		workerCancel: workerCancel, workerResult: workerResult, workerStopped: workerStopped, newRunner: newRunner,
		server: server, handoffWork: dispatcher.work, toolExecutions: toolExecutions, toolInvocations: toolInvocations, completionStore: store}
}

func writeClientEvent(t *testing.T, conn *websocket.Conn, value any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), integrationTimeout)
	defer cancel()
	require.NoError(t, wsjsonWrite(ctx, conn, value))
}

func wsjsonWrite(ctx context.Context, conn *websocket.Conn, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, raw)
}

func readEventType(t *testing.T, conn *websocket.Conn, wanted string) json.RawMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), integrationTimeout)
	defer cancel()
	for {
		kind, raw, err := conn.Read(ctx)
		require.NoError(t, err)
		require.Equal(t, websocket.MessageText, kind)
		var envelope struct {
			Type string `json:"type"`
		}
		require.NoError(t, json.Unmarshal(raw, &envelope))
		if envelope.Type == wanted {
			return append(json.RawMessage(nil), raw...)
		}
	}
}

func emitVoice(t *testing.T, voice *observedVoice, event realtime.Event) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), integrationTimeout)
	defer cancel()
	require.NoError(t, voice.Emit(ctx, event))
}

func nextVoiceCommand(t *testing.T, voice *observedVoice) realtime.Command {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), integrationTimeout)
	defer cancel()
	command, err := voice.NextCommand(ctx)
	require.NoError(t, err)
	return command
}

func beginDurableHandoff(t *testing.T, system *deterministicSystem) (realtime.Command, protocol.LiveHandoffCreatedEvent) {
	t.Helper()
	readEventType(t, system.client, "session.created")
	emitVoice(t, system.voice, realtime.Event{Kind: realtime.EventSpeechStarted, ProviderEventID: "voice-speech-started", ItemID: "turn-e2e"})
	readEventType(t, system.client, "input_audio_buffer.speech_started")
	emitVoice(t, system.voice, realtime.Event{Kind: realtime.EventSpeechStopped, ProviderEventID: "voice-speech-stopped", ItemID: "turn-e2e"})
	readEventType(t, system.client, "input_audio_buffer.speech_stopped")
	emitVoice(t, system.voice, realtime.Event{Kind: realtime.EventTranscriptionCompleted, ProviderEventID: "voice-transcript", ItemID: "turn-e2e", Text: "bounded synthetic request"})
	readEventType(t, system.client, "conversation.item.input_audio_transcription.completed")
	writeClientEvent(t, system.client, protocol.ResponseCreateEvent{Type: "response.create"})
	create := nextVoiceCommand(t, system.voice)
	require.Equal(t, realtime.CommandCreateResponse, create.Kind)
	emitVoice(t, system.voice, realtime.Event{Kind: realtime.EventResponseCreated, Epoch: create.Epoch, ProviderEventID: "voice-created", ResponseID: "voice-response", Status: "in_progress"})
	readEventType(t, system.client, "response.created")
	emitVoice(t, system.voice, realtime.Event{Kind: realtime.EventFunctionCallDone, Epoch: create.Epoch, ProviderEventID: "voice-tool", ResponseID: "voice-response", ItemID: "tool-e2e", CallID: "call-e2e", Name: handoff.ToolStartBackgroundTask, Arguments: json.RawMessage(`{"objective":"bounded synthetic objective"}`)})
	readEventType(t, system.client, "response.function_call_arguments.done")

	createdRaw := readEventType(t, system.client, "live.handoff.created")
	var createdEvent protocol.LiveHandoffCreatedEvent
	require.NoError(t, json.Unmarshal(createdRaw, &createdEvent))
	created, err := system.service.Get(t.Context(), system.scope, identity.HandoffID(createdEvent.HandoffID))
	require.NoError(t, err)
	assert.Equal(t, string(created.ID), createdEvent.HandoffID)
	select {
	case work := <-system.handoffWork:
		assert.Equal(t, handoff.ToolStartBackgroundTask, work.ToolName)
		assert.Empty(t, work.RejectReason)
	case <-time.After(integrationTimeout):
		t.Fatal("start handoff work did not traverse the session dispatcher")
	}
	select {
	case <-system.reasonerStarted:
	case <-time.After(integrationTimeout):
		t.Fatal("worker did not enter the bounded reasoner request")
	}
	select {
	case <-system.reasonerRequests:
	case <-time.After(integrationTimeout):
		t.Fatal("reasoner request lifecycle signal timed out")
	}
	return create, createdEvent
}

func terminalizeHandoffOrigin(t *testing.T, system *deterministicSystem) {
	t.Helper()
	writeClientEvent(t, system.client, protocol.ResponseCancelEvent{Type: "response.cancel", ResponseID: "voice-response"})
	cancel := nextVoiceCommand(t, system.voice)
	require.Equal(t, realtime.CommandCancelResponse, cancel.Kind)
	emitVoice(t, system.voice, realtime.Event{Kind: realtime.EventResponseDone, Epoch: cancel.Epoch, ProviderEventID: "origin-terminal", ResponseID: "voice-response", Status: "cancelled"})
	readEventType(t, system.client, "response.done")
	functionOutput := nextVoiceCommand(t, system.voice)
	require.Equal(t, realtime.CommandCreateItem, functionOutput.Kind)
	followup := nextVoiceCommand(t, system.voice)
	require.Equal(t, realtime.CommandCreateResponse, followup.Kind)
	emitVoice(t, system.voice, realtime.Event{Kind: realtime.EventResponseCreated, Epoch: followup.Epoch, ProviderEventID: "origin-followup-created", ResponseID: "origin-followup", Status: "in_progress"})
	readEventType(t, system.client, "response.created")
	emitVoice(t, system.voice, realtime.Event{Kind: realtime.EventResponseDone, Epoch: followup.Epoch, ProviderEventID: "origin-followup-done", ResponseID: "origin-followup", Status: "completed"})
	readEventType(t, system.client, "response.done")
}

func terminalDurableEvent(t *testing.T, system *deterministicSystem, after int64) handoff.Event {
	t.Helper()
	var terminal handoff.Event
	require.Eventually(t, func() bool {
		events, err := system.repository.ReadEvents(t.Context(), system.scope, after, 32)
		if err != nil {
			return false
		}
		for _, event := range events {
			switch event.State {
			case handoff.StateSucceeded, handoff.StateFailed, handoff.StateCancelled, handoff.StateExpired:
				terminal = event
				return true
			}
		}
		return false
	}, integrationTimeout, 10*time.Millisecond)
	return terminal
}

func assertNoClientEventType(t *testing.T, conn *websocket.Conn, forbidden string, duration time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), duration)
	defer cancel()
	for {
		kind, raw, err := conn.Read(ctx)
		if errors.Is(err, context.DeadlineExceeded) {
			return
		}
		require.NoError(t, err)
		require.Equal(t, websocket.MessageText, kind)
		var envelope struct {
			Type string `json:"type"`
		}
		require.NoError(t, json.Unmarshal(raw, &envelope))
		assert.NotEqual(t, forbidden, envelope.Type)
	}
}

func TestTwoLiveWebSocketsKeepPCMEventsAndResponseEpochsIsolated(t *testing.T) {
	connector := newObservedConnector(4)
	manager := session.NewManager(session.Config{
		IngressFrames: 4, EgressFrames: 4, ControlEvents: 16, WriterControlEvents: 8,
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
		Clock: clock.Real{},
	}, connector, &sequenceSessionIDs{})
	handler := transportws.NewHandler(transportws.Options{MaxFrameBytes: 16 << 10, MaxAudioBytes: 8 << 10, ReadTimeout: integrationTimeout, WriteTimeout: integrationTimeout}, manager,
		transportws.AdmissionFunc(func(context.Context, *http.Request) (session.Subject, func(), error) {
			return session.Subject{TenantID: "tenant-boundary", UserID: "user-boundary", Scopes: map[string]struct{}{"realtime": {}}, ExpiresAt: time.Now().Add(time.Hour)}, func() {}, nil
		}))
	server := httptest.NewServer(handler)
	t.Cleanup(func() { server.CloseClientConnections(); server.Close() })

	dial := func() (*websocket.Conn, *observedVoice) {
		ctx, cancel := context.WithTimeout(t.Context(), integrationTimeout)
		defer cancel()
		client, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/realtime", nil)
		if response != nil && response.Body != nil {
			defer response.Body.Close()
		}
		require.NoError(t, err)
		t.Cleanup(func() { _ = client.CloseNow() })
		voice, err := connector.NextSession(ctx)
		require.NoError(t, err)
		readEventType(t, client, "session.created")
		return client, voice
	}
	clientA, voiceA := dial()
	clientB, voiceB := dial()

	pcmA, pcmB := testkit.RampPCM(960), bytes.Repeat([]byte{0x5a, 0xa5}, 480)
	writeClientEvent(t, clientA, protocol.InputAudioBufferAppendEvent{Type: "input_audio_buffer.append", Audio: base64.StdEncoding.EncodeToString(pcmA)})
	writeClientEvent(t, clientB, protocol.InputAudioBufferAppendEvent{Type: "input_audio_buffer.append", Audio: base64.StdEncoding.EncodeToString(pcmB)})
	commandA, commandB := nextVoiceCommand(t, voiceA), nextVoiceCommand(t, voiceB)
	require.Equal(t, realtime.CommandAppendAudio, commandA.Kind)
	require.Equal(t, realtime.CommandAppendAudio, commandB.Kind)
	assert.Equal(t, pcmA, commandA.Audio)
	assert.Equal(t, pcmB, commandB.Audio)

	writeClientEvent(t, clientA, protocol.ResponseCreateEvent{Type: "response.create"})
	writeClientEvent(t, clientB, protocol.ResponseCreateEvent{Type: "response.create"})
	createA, createB := nextVoiceCommand(t, voiceA), nextVoiceCommand(t, voiceB)
	require.Equal(t, realtime.CommandCreateResponse, createA.Kind)
	require.Equal(t, realtime.CommandCreateResponse, createB.Kind)
	emitVoice(t, voiceA, realtime.Event{Kind: realtime.EventResponseCreated, Epoch: createA.Epoch, ProviderEventID: "created-a", ResponseID: "response-a", Status: "in_progress"})
	emitVoice(t, voiceB, realtime.Event{Kind: realtime.EventResponseCreated, Epoch: createB.Epoch, ProviderEventID: "created-b", ResponseID: "response-b", Status: "in_progress"})
	readEventType(t, clientA, "response.created")
	readEventType(t, clientB, "response.created")

	audioA, audioB := bytes.Repeat([]byte{0x31}, 960), bytes.Repeat([]byte{0x42}, 960)
	emitVoice(t, voiceA, realtime.Event{Kind: realtime.EventAudioDelta, Epoch: createA.Epoch, ProviderEventID: "audio-a", ResponseID: "response-a", ItemID: "item-a", Audio: audioA})
	emitVoice(t, voiceB, realtime.Event{Kind: realtime.EventAudioDelta, Epoch: createB.Epoch, ProviderEventID: "audio-b", ResponseID: "response-b", ItemID: "item-b", Audio: audioB})
	for _, observed := range []struct {
		client *websocket.Conn
		want   []byte
		item   string
	}{{clientA, audioA, "item-a"}, {clientB, audioB, "item-b"}} {
		raw := readEventType(t, observed.client, "response.output_audio.delta")
		var event protocol.ResponseOutputAudioDeltaEvent
		require.NoError(t, json.Unmarshal(raw, &event))
		assert.Equal(t, observed.item, event.ItemID)
		assert.Equal(t, base64.StdEncoding.EncodeToString(observed.want), event.Delta)
	}
	emitVoice(t, voiceA, realtime.Event{Kind: realtime.EventAudioDone, Epoch: createA.Epoch, ProviderEventID: "audio-done-a", ResponseID: "response-a", ItemID: "item-a"})
	emitVoice(t, voiceB, realtime.Event{Kind: realtime.EventAudioDone, Epoch: createB.Epoch, ProviderEventID: "audio-done-b", ResponseID: "response-b", ItemID: "item-b"})
	readEventType(t, clientA, "response.output_audio.done")
	readEventType(t, clientB, "response.output_audio.done")

	emitVoice(t, voiceA, realtime.Event{Kind: realtime.EventAudioDelta, Epoch: createA.Epoch + 1, ProviderEventID: "stale-a", ResponseID: "response-a", ItemID: "stale-item-a", Audio: []byte("stale")})
	emitVoice(t, voiceB, realtime.Event{Kind: realtime.EventAudioDelta, Epoch: createB.Epoch, ProviderEventID: "after-stale-b", ResponseID: "response-b", ItemID: "item-b-2", Audio: bytes.Repeat([]byte{0x7c}, 960)})
	afterRaw := readEventType(t, clientB, "response.output_audio.delta")
	var after protocol.ResponseOutputAudioDeltaEvent
	require.NoError(t, json.Unmarshal(afterRaw, &after))
	assert.Equal(t, "item-b-2", after.ItemID)
	assertNoClientEventType(t, clientA, "response.output_audio.delta", 250*time.Millisecond)
}

func TestVoiceContinuesDuringDurableHandoff(t *testing.T) {
	system := startDeterministicSystem(t)
	create, createdEvent := beginDurableHandoff(t, system)

	emitVoice(t, system.voice, realtime.Event{Kind: realtime.EventAudioDelta, Epoch: create.Epoch, ProviderEventID: "voice-audio", ResponseID: "voice-response", ItemID: "audio-e2e", Audio: testkit.RampPCM(960)})
	readEventType(t, system.client, "response.output_audio.delta")
	select {
	case <-system.workerDone:
		t.Fatal("worker completed before the voice audio boundary")
	default:
	}

	system.releaseReasoner()
	select {
	case <-system.workerDone:
	case <-time.After(integrationTimeout):
		t.Fatal("worker completion timed out")
	}
	events, err := system.repository.ReadEvents(t.Context(), system.scope, createdEvent.SequenceID, 32)
	require.NoError(t, err)
	var completed handoff.Event
	for _, event := range events {
		if event.State == handoff.StateSucceeded {
			completed = event
		}
	}
	require.NotZero(t, completed.Seq)
	completedRaw := readEventType(t, system.client, "live.handoff.completed")
	var completedEvent protocol.LiveHandoffCompletedEvent
	require.NoError(t, json.Unmarshal(completedRaw, &completedEvent))
	assert.Equal(t, createdEvent.HandoffID, completedEvent.HandoffID)
	assert.Greater(t, completedEvent.SequenceID, createdEvent.SequenceID)
	require.Eventually(t, func() bool {
		cursor, cursorErr := system.repository.LoadDeliveryCursor(t.Context(), "e2e-client", system.scope)
		return cursorErr == nil && cursor == completedEvent.SequenceID
	}, integrationTimeout, 10*time.Millisecond)
}

func TestDroppedListenerNotificationRepairsThroughPollAndCursor(t *testing.T) {
	system := startDeterministicSystem(t, withoutListenerDelivery)
	_, created := beginDurableHandoff(t, system)
	require.Eventually(t, func() bool {
		cursor, err := system.repository.LoadDeliveryCursor(t.Context(), "e2e-client", system.scope)
		return err == nil && cursor >= created.SequenceID
	}, integrationTimeout, 10*time.Millisecond)
	terminalizeHandoffOrigin(t, system)
	system.releaseReasoner()
	completedRaw := readEventType(t, system.client, "live.handoff.completed")
	var completed protocol.LiveHandoffCompletedEvent
	require.NoError(t, json.Unmarshal(completedRaw, &completed))
	assert.Equal(t, created.HandoffID, completed.HandoffID)
	require.Eventually(t, func() bool {
		cursor, err := system.repository.LoadDeliveryCursor(t.Context(), "e2e-client", system.scope)
		return err == nil && cursor == completed.SequenceID
	}, integrationTimeout, 10*time.Millisecond)
}

func TestDuplicateCompletionHasOneExecutorEffectAndOneClientTerminal(t *testing.T) {
	system := startDeterministicSystem(t, withReasonerTool, withDuplicateCompletion)
	_, created := beginDurableHandoff(t, system)
	terminalizeHandoffOrigin(t, system)
	system.releaseReasoner()
	select {
	case tool := <-system.toolExecutions:
		assert.Equal(t, "fetch", tool)
	case <-time.After(integrationTimeout):
		t.Fatal("worker executor did not observe the deterministic tool call")
	}
	completedRaw := readEventType(t, system.client, "live.handoff.completed")
	var completed protocol.LiveHandoffCompletedEvent
	require.NoError(t, json.Unmarshal(completedRaw, &completed))
	assert.Equal(t, created.HandoffID, completed.HandoffID)
	require.Equal(t, int32(2), system.completionStore.attempts.Load(), "test failpoint must issue duplicate completion commits")
	select {
	case duplicateTool := <-system.toolExecutions:
		t.Fatalf("duplicate completion re-executed tool class %q", duplicateTool)
	default:
	}
	assertNoClientEventType(t, system.client, "live.handoff.completed", 250*time.Millisecond)
	events, err := system.repository.ReadEvents(t.Context(), system.scope, created.SequenceID, 32)
	require.NoError(t, err)
	terminalCount := 0
	for _, event := range events {
		if event.State == handoff.StateSucceeded {
			terminalCount++
		}
	}
	assert.Equal(t, 1, terminalCount)
}

func TestApprovalDecisionsTraverseSessionProtocolPostgresAndRunner(t *testing.T) {
	for _, outcome := range []string{"approved", "denied", "expired"} {
		t.Run(outcome, func(t *testing.T) {
			system := startDeterministicSystem(t, withApprovalReasonerTool)
			_, created := beginDurableHandoff(t, system)
			terminalizeHandoffOrigin(t, system)
			system.releaseReasoner()

			requiredRaw := readEventType(t, system.client, "live.tool.approval.required")
			var requiredEvent protocol.LiveToolApprovalRequiredEvent
			require.NoError(t, json.Unmarshal(requiredRaw, &requiredEvent))
			assert.Equal(t, created.HandoffID, requiredEvent.HandoffID)
			require.NotEmpty(t, requiredEvent.InvocationID)
			assert.Equal(t, handoff.ApprovalRiskSideEffecting, requiredEvent.RiskClass)

			switch outcome {
			case "approved", "denied":
				writeClientEvent(t, system.client, protocol.LiveToolApprovalResolveEvent{
					Type: "live.tool.approval.resolve", InvocationID: requiredEvent.InvocationID,
					Decision: outcome, Reason: "deterministic " + outcome, IdempotencyKey: "approval-" + outcome,
				})
			case "expired":
				expired, err := system.repository.ExpireApprovals(t.Context(), requiredEvent.ExpiresAt, 10)
				require.NoError(t, err)
				require.Equal(t, 1, expired)
			}

			if outcome == "approved" {
				select {
				case invocation := <-system.toolInvocations:
					assert.Equal(t, requiredEvent.InvocationID, invocation.InvocationID, "approval must replay the exact sealed invocation")
					assert.Equal(t, requiredEvent.InvocationID, invocation.IdempotencyKey)
					assert.JSONEq(t, `{}`, string(invocation.Arguments))
				case <-time.After(integrationTimeout):
					t.Fatal("approved invocation did not reach the real Runner executor")
				}
				completedRaw := readEventType(t, system.client, "live.handoff.completed")
				var completed protocol.LiveHandoffCompletedEvent
				require.NoError(t, json.Unmarshal(completedRaw, &completed))
				assert.Equal(t, created.HandoffID, completed.HandoffID)
				select {
				case duplicate := <-system.toolInvocations:
					t.Fatalf("approved invocation executed twice: %s", duplicate.InvocationID)
				default:
				}
			} else {
				failedRaw := readEventType(t, system.client, "live.handoff.failed")
				var failed protocol.LiveHandoffFailedEvent
				require.NoError(t, json.Unmarshal(failedRaw, &failed))
				assert.Equal(t, created.HandoffID, failed.HandoffID)
				select {
				case invocation := <-system.toolInvocations:
					t.Fatalf("terminal %s approval reached executor: %s", outcome, invocation.InvocationID)
				default:
				}
			}

			approval, err := system.repository.LoadApproval(t.Context(), system.scope, requiredEvent.InvocationID)
			require.NoError(t, err)
			assert.Equal(t, outcome, approval.ApprovalState)
		})
	}
}

func TestShadowTransitionSuppressesDurableClientAndProviderPresentation(t *testing.T) {
	gates := feature.New(map[string]bool{feature.HandoffsEnabled: true, feature.ShadowMode: false})
	system := startDeterministicSystem(t, withFeatureGates(gates))
	_, created := beginDurableHandoff(t, system)
	terminalizeHandoffOrigin(t, system)
	require.True(t, gates.Apply(feature.Snapshot{Version: 1, Values: map[string]bool{feature.HandoffsEnabled: true, feature.ShadowMode: true}}))
	system.releaseReasoner()
	terminal := terminalDurableEvent(t, system, created.SequenceID)
	require.Eventually(t, func() bool {
		cursor, err := system.repository.LoadDeliveryCursor(t.Context(), "e2e-client", system.scope)
		return err == nil && cursor == terminal.Seq
	}, integrationTimeout, 10*time.Millisecond)
	probeCtx, cancelProbe := context.WithTimeout(t.Context(), 250*time.Millisecond)
	_, err := system.voice.NextCommand(probeCtx)
	cancelProbe()
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assertNoClientEventType(t, system.client, "live.handoff.completed", 250*time.Millisecond)
}

func TestUnsafeWorkerResultSuppressesDurableClientAndProviderPresentation(t *testing.T) {
	policy := &toggleSafetyPolicy{}
	system := startDeterministicSystem(t, withPresentationSafety(policy))
	_, created := beginDurableHandoff(t, system)
	terminalizeHandoffOrigin(t, system)
	policy.suppress.Store(true)
	system.releaseReasoner()
	terminal := terminalDurableEvent(t, system, created.SequenceID)
	require.Eventually(t, func() bool {
		cursor, err := system.repository.LoadDeliveryCursor(t.Context(), "e2e-client", system.scope)
		return err == nil && cursor == terminal.Seq
	}, integrationTimeout, 10*time.Millisecond)
	probeCtx, cancelProbe := context.WithTimeout(t.Context(), 250*time.Millisecond)
	_, err := system.voice.NextCommand(probeCtx)
	cancelProbe()
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assertNoClientEventType(t, system.client, "live.handoff.completed", 250*time.Millisecond)
}

func TestWorkerCompletionDuringSpeechRemainsVisualOnly(t *testing.T) {
	system := startDeterministicSystem(t)
	create, created := beginDurableHandoff(t, system)
	emitVoice(t, system.voice, realtime.Event{Kind: realtime.EventSpeechStarted, ProviderEventID: "interrupt-started", ItemID: "turn-interrupt"})
	readEventType(t, system.client, "input_audio_buffer.speech_started")
	cancel := nextVoiceCommand(t, system.voice)
	require.Equal(t, realtime.CommandCancelResponse, cancel.Kind)
	assert.Greater(t, cancel.Epoch, create.Epoch)
	emitVoice(t, system.voice, realtime.Event{Kind: realtime.EventResponseDone, Epoch: cancel.Epoch, ProviderEventID: "interrupt-cancelled", ResponseID: "voice-response", Status: "cancelled"})
	readEventType(t, system.client, "response.done")
	functionOutput := nextVoiceCommand(t, system.voice)
	require.Equal(t, realtime.CommandCreateItem, functionOutput.Kind)
	followup := nextVoiceCommand(t, system.voice)
	require.Equal(t, realtime.CommandCreateResponse, followup.Kind)
	emitVoice(t, system.voice, realtime.Event{Kind: realtime.EventResponseCreated, Epoch: followup.Epoch, ProviderEventID: "followup-created", ResponseID: "followup-response", Status: "in_progress"})
	readEventType(t, system.client, "response.created")
	emitVoice(t, system.voice, realtime.Event{Kind: realtime.EventResponseDone, Epoch: followup.Epoch, ProviderEventID: "followup-done", ResponseID: "followup-response", Status: "completed"})
	readEventType(t, system.client, "response.done")
	system.releaseReasoner()
	select {
	case <-system.workerDone:
	case <-time.After(integrationTimeout):
		t.Fatal("worker completion during speech timed out")
	}
	completedRaw := readEventType(t, system.client, "live.handoff.completed")
	var completed protocol.LiveHandoffCompletedEvent
	require.NoError(t, json.Unmarshal(completedRaw, &completed))
	assert.Equal(t, created.HandoffID, completed.HandoffID)
	probeCtx, probeCancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
	defer probeCancel()
	_, err := system.voice.NextCommand(probeCtx)
	assert.ErrorIs(t, err, context.DeadlineExceeded, "completion while the user speaks must not start provider speech")
}

func TestProviderSteerAndCancelTraverseDurableWorkerLifecycle(t *testing.T) {
	system := startDeterministicSystem(t)
	_, created := beginDurableHandoff(t, system)
	beforeSteer, err := system.service.Get(t.Context(), system.scope, identity.HandoffID(created.HandoffID))
	require.NoError(t, err)
	writeClientEvent(t, system.client, protocol.ResponseCancelEvent{Type: "response.cancel", ResponseID: "voice-response"})
	startCancel := nextVoiceCommand(t, system.voice)
	require.Equal(t, realtime.CommandCancelResponse, startCancel.Kind)
	emitVoice(t, system.voice, realtime.Event{Kind: realtime.EventResponseDone, Epoch: startCancel.Epoch, ProviderEventID: "start-done", ResponseID: "voice-response", Status: "cancelled"})
	readEventType(t, system.client, "response.done")
	startOutput := nextVoiceCommand(t, system.voice)
	require.Equal(t, realtime.CommandCreateItem, startOutput.Kind)
	steerResponse := nextVoiceCommand(t, system.voice)
	require.Equal(t, realtime.CommandCreateResponse, steerResponse.Kind)
	emitVoice(t, system.voice, realtime.Event{Kind: realtime.EventResponseCreated, Epoch: steerResponse.Epoch, ProviderEventID: "steer-response-created", ResponseID: "steer-response", Status: "in_progress"})
	readEventType(t, system.client, "response.created")
	steerArguments, err := json.Marshal(map[string]string{
		"handoff_id": created.HandoffID,
		"objective":  "revised bounded objective",
	})
	require.NoError(t, err)
	emitVoice(t, system.voice, realtime.Event{
		Kind: realtime.EventFunctionCallDone, Epoch: steerResponse.Epoch, ProviderEventID: "voice-steer",
		ResponseID: "steer-response", ItemID: "tool-steer", CallID: "call-steer",
		Name: handoff.ToolSteerBackgroundTask, Arguments: steerArguments,
	})
	readEventType(t, system.client, "response.function_call_arguments.done")
	select {
	case work := <-system.handoffWork:
		assert.Equal(t, handoff.ToolSteerBackgroundTask, work.ToolName)
		assert.Empty(t, work.RejectReason)
	case <-time.After(integrationTimeout):
		t.Fatal("steer work did not traverse the session dispatcher")
	}
	require.Eventually(t, func() bool {
		snapshot, getErr := system.service.Get(t.Context(), system.scope, identity.HandoffID(created.HandoffID))
		return getErr == nil && snapshot.Request.Objective == "revised bounded objective" && snapshot.ObjectiveRevision > beforeSteer.ObjectiveRevision
	}, integrationTimeout, 10*time.Millisecond)
	steeredRaw := readEventType(t, system.client, "live.handoff.progress")
	var steered protocol.LiveHandoffProgressEvent
	require.NoError(t, json.Unmarshal(steeredRaw, &steered))
	assert.Equal(t, created.HandoffID, steered.HandoffID)
	assert.Greater(t, steered.SequenceID, created.SequenceID)
	writeClientEvent(t, system.client, protocol.ResponseCancelEvent{Type: "response.cancel", ResponseID: "steer-response"})
	steerCancel := nextVoiceCommand(t, system.voice)
	require.Equal(t, realtime.CommandCancelResponse, steerCancel.Kind)
	emitVoice(t, system.voice, realtime.Event{Kind: realtime.EventResponseDone, Epoch: steerCancel.Epoch, ProviderEventID: "steer-response-done", ResponseID: "steer-response", Status: "cancelled"})
	readEventType(t, system.client, "response.done")
	steerOutput := nextVoiceCommand(t, system.voice)
	require.Equal(t, realtime.CommandCreateItem, steerOutput.Kind)
	cancelResponse := nextVoiceCommand(t, system.voice)
	require.Equal(t, realtime.CommandCreateResponse, cancelResponse.Kind)
	emitVoice(t, system.voice, realtime.Event{Kind: realtime.EventResponseCreated, Epoch: cancelResponse.Epoch, ProviderEventID: "cancel-response-created", ResponseID: "cancel-response", Status: "in_progress"})
	readEventType(t, system.client, "response.created")

	cancelArguments, err := json.Marshal(map[string]string{
		"handoff_id": created.HandoffID,
		"reason":     "bounded test cancellation",
	})
	require.NoError(t, err)
	emitVoice(t, system.voice, realtime.Event{
		Kind: realtime.EventFunctionCallDone, Epoch: cancelResponse.Epoch, ProviderEventID: "voice-cancel",
		ResponseID: "cancel-response", ItemID: "tool-cancel", CallID: "call-cancel",
		Name: handoff.ToolCancelBackgroundTask, Arguments: cancelArguments,
	})
	readEventType(t, system.client, "response.function_call_arguments.done")
	require.Eventually(t, func() bool {
		events, readErr := system.repository.ReadEvents(t.Context(), system.scope, steered.SequenceID, 8)
		if readErr != nil {
			return false
		}
		for _, event := range events {
			if event.Kind == "handoff.cancel_requested" && string(event.HandoffID) == created.HandoffID {
				return true
			}
		}
		return false
	}, integrationTimeout, 10*time.Millisecond)
	cancelRequestedRaw := readEventType(t, system.client, "live.handoff.progress")
	var cancelRequested protocol.LiveHandoffProgressEvent
	require.NoError(t, json.Unmarshal(cancelRequestedRaw, &cancelRequested))
	assert.Equal(t, created.HandoffID, cancelRequested.HandoffID)
	assert.Greater(t, cancelRequested.SequenceID, steered.SequenceID)

	// The worker heartbeat observes the durable cancellation and terminalizes
	// without releasing the blocked reasoner response.
	system.manual.Advance(2 * time.Second)
	cancelledRaw := readEventType(t, system.client, "live.handoff.cancelled")
	var cancelled protocol.LiveHandoffCancelledEvent
	require.NoError(t, json.Unmarshal(cancelledRaw, &cancelled))
	assert.Equal(t, created.HandoffID, cancelled.HandoffID)
	assert.Greater(t, cancelled.SequenceID, cancelRequested.SequenceID)
}

func TestRunnerCancellationRestartRecoversExpiredLease(t *testing.T) {
	system := startDeterministicSystem(t)
	_, created := beginDurableHandoff(t, system)
	system.workerCancel()
	select {
	case <-system.workerStopped:
	case <-time.After(integrationTimeout):
		t.Fatal("cancelled worker did not stop")
	}
	before, err := system.service.Get(t.Context(), system.scope, identity.HandoffID(created.HandoffID))
	require.NoError(t, err)
	assert.Equal(t, handoff.StateRunning, before.State)
	assert.Equal(t, uint32(1), before.Attempt)

	system.manual.Advance(10 * time.Second)
	restarted, err := system.newRunner("worker-restarted", system.repository, system.manual)
	require.NoError(t, err)
	restartCtx, cancelRestart := context.WithCancel(context.Background())
	restartResult := make(chan error, 1)
	go func() { restartResult <- restarted.Run(restartCtx) }()
	t.Cleanup(func() {
		cancelRestart()
		select {
		case runErr := <-restartResult:
			assert.NoError(t, runErr)
		case <-time.After(integrationTimeout):
			t.Error("restarted worker cleanup timed out")
		}
	})
	select {
	case <-system.reasonerRequests:
	case <-time.After(integrationTimeout):
		t.Fatal("restarted worker did not recover the expired lease")
	}
	system.releaseReasoner()
	completedRaw := readEventType(t, system.client, "live.handoff.completed")
	var completed protocol.LiveHandoffCompletedEvent
	require.NoError(t, json.Unmarshal(completedRaw, &completed))
	assert.Equal(t, created.HandoffID, completed.HandoffID)
	after, err := system.service.Get(t.Context(), system.scope, identity.HandoffID(created.HandoffID))
	require.NoError(t, err)
	assert.Equal(t, handoff.StateSucceeded, after.State)
	assert.Equal(t, uint32(2), after.Attempt)
}

func TestReconnectDisconnectsProviderWithQueuedAudioAndFencesStaleOutput(t *testing.T) {
	system := startDeterministicSystem(t)
	create, created := beginDurableHandoff(t, system)
	// Reach an actual turn boundary before reconnect. The durable handoff remains
	// active because its worker request is still blocked.
	emitVoice(t, system.voice, realtime.Event{Kind: realtime.EventSpeechStarted, ProviderEventID: "reconnect-speech-started", ItemID: "turn-reconnect"})
	readEventType(t, system.client, "input_audio_buffer.speech_started")
	cancelOrigin := nextVoiceCommand(t, system.voice)
	require.Equal(t, realtime.CommandCancelResponse, cancelOrigin.Kind)
	emitVoice(t, system.voice, realtime.Event{Kind: realtime.EventResponseDone, Epoch: cancelOrigin.Epoch, ProviderEventID: "origin-done", ResponseID: "voice-response", Status: "cancelled"})
	readEventType(t, system.client, "response.done")
	functionOutput := nextVoiceCommand(t, system.voice)
	require.Equal(t, realtime.CommandCreateItem, functionOutput.Kind)
	followup := nextVoiceCommand(t, system.voice)
	require.Equal(t, realtime.CommandCreateResponse, followup.Kind)
	emitVoice(t, system.voice, realtime.Event{Kind: realtime.EventResponseCreated, Epoch: followup.Epoch, ProviderEventID: "followup-created", ResponseID: "followup-response", Status: "in_progress"})
	readEventType(t, system.client, "response.created")
	emitVoice(t, system.voice, realtime.Event{Kind: realtime.EventResponseDone, Epoch: followup.Epoch, ProviderEventID: "followup-done", ResponseID: "followup-response", Status: "completed"})
	readEventType(t, system.client, "response.done")
	emitVoice(t, system.voice, realtime.Event{Kind: realtime.EventSpeechStopped, ProviderEventID: "reconnect-speech-stopped", ItemID: "turn-reconnect"})
	readEventType(t, system.client, "input_audio_buffer.speech_stopped")

	watermark := testkit.RampPCM(960)
	writeClientEvent(t, system.client, protocol.InputAudioBufferAppendEvent{Type: "input_audio_buffer.append", Audio: base64.StdEncoding.EncodeToString(watermark)})
	forwarded := nextVoiceCommand(t, system.voice)
	require.Equal(t, realtime.CommandAppendAudio, forwarded.Kind)
	assert.Equal(t, watermark, forwarded.Audio)
	observedWatermark := nextObservedCommand(t, system.voice, realtime.CommandAppendAudio)
	assert.Equal(t, watermark, observedWatermark.Audio)
	queued := testkit.RampPCM(1_920)
	writeClientEvent(t, system.client, protocol.InputAudioBufferAppendEvent{Type: "input_audio_buffer.append", Audio: base64.StdEncoding.EncodeToString(queued)})
	queuedFirst := nextObservedCommand(t, system.voice, realtime.CommandAppendAudio)
	queuedSecond := nextObservedCommand(t, system.voice, realtime.CommandAppendAudio)
	assert.Equal(t, queued[:960], queuedFirst.Audio)
	assert.Equal(t, queued[960:], queuedSecond.Audio)
	writeClientEvent(t, system.client, protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"})
	nextObservedCommand(t, system.voice, realtime.CommandCommitAudio)
	// Manager.Submit captures the latest ingress high-water and returns only
	// after the reactor applies it. Replaying the already-observed durable
	// created event is a side-effect-free synchronization barrier that proves
	// every queued PCM frame and commit completed before reconnect begins.
	durable, err := system.repository.ReadEvents(t.Context(), system.scope, 0, 8)
	require.NoError(t, err)
	require.NotEmpty(t, durable)
	require.NoError(t, system.manager.Submit(t.Context(), "session-e2e", 1, session.HandoffEventInput{Scope: system.scope, Event: durable[0]}))

	reconnected := newReconnectClient()
	reconnectCtx, cancelReconnect := context.WithCancel(context.Background())
	reconnectResult := make(chan error, 1)
	go func() {
		request := session.ReconnectRequest{Scope: system.scope, SessionID: "session-e2e", LastIncarnation: 1, LastEventSeq: 0}
		reconnectResult <- system.manager.Reconnect(reconnectCtx, request, reconnected)
	}()
	t.Cleanup(func() {
		cancelReconnect()
		select {
		case reconnectErr := <-reconnectResult:
			if reconnectErr != nil && !errors.Is(reconnectErr, context.Canceled) {
				t.Errorf("reconnect cleanup failed: %v", reconnectErr)
			}
		case <-time.After(integrationTimeout):
			t.Error("reconnect cleanup timed out")
		}
	})
	newVoiceCtx, cancelNewVoice := context.WithTimeout(t.Context(), integrationTimeout)
	newVoiceResult := make(chan *observedVoice, 1)
	newVoiceError := make(chan error, 1)
	go func() {
		voice, nextErr := system.connector.NextSession(newVoiceCtx)
		if nextErr != nil {
			newVoiceError <- nextErr
			return
		}
		newVoiceResult <- voice
	}()
	var newVoice *observedVoice
	select {
	case newVoice = <-newVoiceResult:
	case reconnectErr := <-reconnectResult:
		require.NoError(t, reconnectErr, "reconnect rejected before opening replacement provider")
	case err := <-newVoiceError:
		require.NoError(t, err)
	}
	cancelNewVoice()
	readCtx, cancelRead := context.WithTimeout(t.Context(), integrationTimeout)
	var readErr error
	for readErr == nil {
		_, _, readErr = system.client.Read(readCtx)
	}
	cancelRead()
	require.Error(t, readErr, "replacement must close the old client transport")
	connected := readReconnectEvent(t, reconnected)
	require.Equal(t, "session.created", connected.EventType(), "replacement readiness must precede durable replay")
	replayed := readReconnectEvent(t, reconnected)
	require.Equal(t, "live.handoff.created", replayed.EventType(), "first replayed durable event must preserve sequence order")
	replayedCreated, ok := replayed.(protocol.LiveHandoffCreatedEvent)
	require.True(t, ok)
	assert.Equal(t, created.EventID, replayedCreated.EventID)
	assert.Equal(t, created.SequenceID, replayedCreated.SequenceID)
	restored := nextVoiceCommand(t, newVoice)
	require.Equal(t, realtime.CommandCreateItem, restored.Kind)
	assert.NotEqual(t, realtime.CommandAppendAudio, restored.Kind)
	assert.Error(t, system.voice.Emit(t.Context(), realtime.Event{Kind: realtime.EventAudioDelta, Epoch: create.Epoch, ResponseID: "voice-response", ItemID: "stale-audio", Audio: watermark}))
	assert.ErrorIs(t, system.manager.Submit(t.Context(), "session-e2e", 1, session.FailedInput{Source: "stale", Err: errors.New("stale")}), session.ErrSessionNotActive)

	system.releaseReasoner()
	var completedEvent protocol.LiveHandoffCompletedEvent
	lastSequence := replayedCreated.SequenceID
	for completedEvent.EventType() == "" {
		event := readReconnectEvent(t, reconnected)
		switch typed := event.(type) {
		case protocol.LiveHandoffProgressEvent:
			require.Equal(t, created.HandoffID, typed.HandoffID)
			require.Greater(t, typed.SequenceID, lastSequence)
			lastSequence = typed.SequenceID
		case protocol.LiveHandoffCompletedEvent:
			completedEvent = typed
		default:
			t.Fatalf("unexpected reconnect event class %q before completion", event.EventType())
		}
	}
	assert.Equal(t, created.HandoffID, completedEvent.HandoffID)
	assert.Greater(t, completedEvent.SequenceID, lastSequence)
	require.Eventually(t, func() bool {
		cursor, cursorErr := system.repository.LoadDeliveryCursor(t.Context(), "e2e-client", system.scope)
		return cursorErr == nil && cursor == completedEvent.SequenceID
	}, integrationTimeout, 10*time.Millisecond)
}
