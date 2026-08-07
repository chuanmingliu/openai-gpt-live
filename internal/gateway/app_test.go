package gateway

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/config"
	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/health"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/presentation"
	"github.com/chuanmingliu/openai-gpt-live/internal/protocol"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/chuanmingliu/openai-gpt-live/internal/safety"
	"github.com/chuanmingliu/openai-gpt-live/internal/session"
	"github.com/chuanmingliu/openai-gpt-live/internal/store/postgres"
	"github.com/chuanmingliu/openai-gpt-live/internal/testkit"
	transportws "github.com/chuanmingliu/openai-gpt-live/internal/transport/ws"
	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const gatewayTestTimeout = 3 * time.Second

type deterministicIDs struct{}

func (deterministicIDs) NewSessionID() identity.SessionID           { return "session-fixed" }
func (deterministicIDs) NewConversationID() identity.ConversationID { return "conversation-fixed" }

type gatewayEventStoreFake struct{}

func (gatewayEventStoreFake) ReadEvents(context.Context, handoff.Scope, int64, int) ([]handoff.Event, error) {
	return nil, nil
}
func (gatewayEventStoreFake) LoadDeliveryCursor(context.Context, string, handoff.Scope) (int64, error) {
	return 0, nil
}
func (gatewayEventStoreFake) AdvanceDeliveryCursor(context.Context, string, handoff.Scope, int64) error {
	return nil
}

type gatewayEventListenerFake struct{ err error }

func (l gatewayEventListenerFake) Run(ctx context.Context, _ chan<- handoff.EventWakeup) error {
	if l.err != nil {
		return l.err
	}
	<-ctx.Done()
	return ctx.Err()
}

type gatewayAllowSafety struct{}

func (gatewayAllowSafety) EvaluateWorkerResult(context.Context, safety.WorkerResultCandidate) (safety.Decision, error) {
	return safety.Decision{Action: safety.ActionAllow, ReasonCode: "test"}, nil
}

func testHandoffPresentationOption() Option {
	return testHandoffPresentationOptionWithListener(gatewayEventListenerFake{})
}

func testHandoffPresentationOptionWithListener(listener HandoffEventListener) Option {
	store := gatewayEventStoreFake{}
	return WithHandoffPresentation(HandoffPresentationDependencies{
		Reader: store, Cursors: store, Listener: listener, Clock: clock.Real{},
		PollInterval: 10 * time.Millisecond, ConsumerID: "gateway-test", WakeupCapacity: 8,
		Policy: presentation.NewPolicy(presentation.Config{}), Safety: gatewayAllowSafety{},
	})
}

func validConfig() config.Config {
	return config.Config{
		Mode: "local",
		Gateway: config.GatewayConfig{
			AdvertisedModel: "model-default",
			ListenAddr:      "127.0.0.1:0",
			Audio: config.AudioConfig{
				SampleRate: 24_000, Channels: 1, BytesPerSample: 2, FrameBytes: 960,
			},
			Session: config.SessionConfig{
				IngressFrames: 4, EgressFrames: 4, ControlEvents: 16, WriterControlEvents: 8,
				IngressAudioBytes: 3_840, EgressAudioBytes: 3_840,
			},
			WebSocket: config.WebSocketConfig{MaxFrameBytes: 8 << 10},
		},
		Worker: config.WorkerConfig{
			Concurrency: 1, LeaseDuration: 30 * time.Second, HeartbeatInterval: 10 * time.Second,
			RetryBase: time.Second, RetryMax: time.Second, MaxRetries: 1,
			MaxOutputBytes: 1024, MaxOutputTokens: 128, MaxToolRounds: 1,
		},
	}
}

func TestNewMapsConfigAndAppliesBoundedTimeoutDefaults(t *testing.T) {
	cfg := validConfig()
	connector := testkit.NewFakeConnector(8)
	app, err := New(cfg, connector, WithIDGenerator(deterministicIDs{}))
	require.NoError(t, err)
	require.Equal(t, cfg.Gateway.WebSocket.MaxFrameBytes, int(app.wsOptions.MaxFrameBytes))
	require.Positive(t, app.wsOptions.ReadTimeout)
	require.Positive(t, app.wsOptions.WriteTimeout)
	require.Positive(t, app.managerConfig.IngressBlockTimeout)
	require.Positive(t, app.managerConfig.EgressBlockTimeout)
	require.Equal(t, cfg.Gateway.Session.IngressFrames, app.managerConfig.IngressFrames)
	require.Equal(t, cfg.Gateway.Session.EgressFrames, app.managerConfig.EgressFrames)
	require.Equal(t, session.DefaultRecoveryItemLimit, app.managerConfig.RecoveryItemLimit)
	require.Equal(t, session.DefaultRecoveryByteLimit, app.managerConfig.RecoveryByteLimit)

	cfg.Gateway.WebSocket.ReadTimeout = 17 * time.Second
	cfg.Gateway.WebSocket.WriteTimeout = 19 * time.Second
	cfg.Gateway.Session.IngressBlockTimeout = 23 * time.Millisecond
	cfg.Gateway.Session.EgressBlockTimeout = 29 * time.Millisecond
	app, err = New(cfg, connector, WithIDGenerator(deterministicIDs{}))
	require.NoError(t, err)
	require.Equal(t, 17*time.Second, app.wsOptions.ReadTimeout)
	require.Equal(t, 19*time.Second, app.wsOptions.WriteTimeout)
	require.Equal(t, 23*time.Millisecond, app.managerConfig.IngressBlockTimeout)
	require.Equal(t, 29*time.Millisecond, app.managerConfig.EgressBlockTimeout)
}

func TestNewWiresRecoveryStoreAndReconnectRetryConfiguration(t *testing.T) {
	cfg := validConfig()
	cfg.Features.Handoffs = true
	cfg.Gateway.Reconnect = config.ReconnectConfig{MaxRetries: 4, RetryBase: 20 * time.Millisecond, RetryMax: 80 * time.Millisecond}
	store := &gatewayRecoveryStore{}
	dependencies := DurableDependencies{
		Service:            &handoffServiceFake{creates: make(chan handoff.CreateRequest, 1)},
		ConversationWriter: &transcriptWriterFake{}, RecoveryStore: store,
		Journal: newCoordinatorJournalFake(), DeliveryAffinity: "recovery-affinity", JournalOwner: "recovery-owner",
		JournalClaimLease: time.Minute, HandoffQueueCapacity: 1, TranscriptQueueCapacity: 1,
		Snapshot: validHandoffWork().Snapshot,
	}
	app, err := New(cfg, testkit.NewFakeConnector(2), WithIDGenerator(deterministicIDs{}),
		WithDurableHandoffs(dependencies), testHandoffPresentationOption())
	require.NoError(t, err)
	require.Same(t, store, app.managerConfig.RecoveryStore)
	require.Equal(t, 4, app.managerConfig.ReconnectMaxRetries)
	require.Equal(t, 20*time.Millisecond, app.managerConfig.ReconnectRetryBase)
	require.Equal(t, 80*time.Millisecond, app.managerConfig.ReconnectRetryMax)
}

type gatewayRecoveryStore struct{}

func (*gatewayRecoveryStore) LoadRecovery(_ context.Context, scope handoff.Scope, _, _ int) (session.RecoverySnapshot, error) {
	return session.RecoverySnapshot{ConversationID: scope.ConversationID}, nil
}

func TestNewUsesRandomURLSafeIDsAndAllowsInjection(t *testing.T) {
	app, err := New(validConfig(), testkit.NewFakeConnector(4))
	require.NoError(t, err)
	first := string(app.ids.NewSessionID())
	second := string(app.ids.NewSessionID())
	require.NotEmpty(t, first)
	require.NotEqual(t, first, second)
	require.True(t, regexp.MustCompile(`^[A-Za-z0-9_-]+$`).MatchString(first))
	require.GreaterOrEqual(t, len(first), 32)

	app, err = New(validConfig(), testkit.NewFakeConnector(4), WithIDGenerator(deterministicIDs{}))
	require.NoError(t, err)
	require.Equal(t, identity.SessionID("session-fixed"), app.ids.NewSessionID())
}

func TestNewRejectsNilDependenciesInvalidTLSAndTypedNilIDs(t *testing.T) {
	cfg := validConfig()
	_, err := New(cfg, nil)
	require.Error(t, err)
	var ids *deterministicIDs
	_, err = New(cfg, testkit.NewFakeConnector(4), WithIDGenerator(ids))
	require.Error(t, err)
	cfg.Gateway.TLS.CertFile = "cert.pem"
	_, err = New(cfg, testkit.NewFakeConnector(4))
	require.ErrorContains(t, err, "TLS")
}

func TestNewHandoffsFeatureFailsClosedAndFeatureOffStaysCompatible(t *testing.T) {
	cfg := validConfig()
	_, err := New(cfg, testkit.NewFakeConnector(4), WithIDGenerator(deterministicIDs{}))
	require.NoError(t, err)

	cfg.Features.Handoffs = true
	_, err = New(cfg, testkit.NewFakeConnector(4), WithIDGenerator(deterministicIDs{}))
	require.ErrorContains(t, err, "durable handoff dependencies")

	service := &handoffServiceFake{creates: make(chan handoff.CreateRequest, 1)}
	writer := &transcriptWriterFake{items: make(chan postgres.ConversationItem, 1)}
	dependencies := DurableDependencies{
		Service: service, ConversationWriter: writer, HandoffQueueCapacity: 2, TranscriptQueueCapacity: 2,
		Journal: newCoordinatorJournalFake(), DeliveryAffinity: "app-test-affinity", JournalOwner: "app-test", JournalClaimLease: time.Minute,
		Snapshot: validHandoffWork().Snapshot,
	}
	_, err = New(cfg, testkit.NewFakeConnector(4), WithIDGenerator(deterministicIDs{}), WithDurableHandoffs(dependencies))
	require.ErrorContains(t, err, "handoff presentation")
	store := gatewayEventStoreFake{}
	missingSafety := WithHandoffPresentation(HandoffPresentationDependencies{
		Reader: store, Cursors: store, Listener: gatewayEventListenerFake{}, Clock: clock.Real{},
		PollInterval: 10 * time.Millisecond, ConsumerID: "gateway-test", Policy: presentation.NewPolicy(presentation.Config{}),
	})
	_, err = New(cfg, testkit.NewFakeConnector(4), WithIDGenerator(deterministicIDs{}), WithDurableHandoffs(dependencies), missingSafety)
	require.ErrorContains(t, err, "explicit handoff presentation safety")
	app, err := New(cfg, testkit.NewFakeConnector(4), WithIDGenerator(deterministicIDs{}), WithDurableHandoffs(dependencies), testHandoffPresentationOption())
	require.NoError(t, err)
	require.NotNil(t, app.handoffs)
	require.NotNil(t, app.transcripts)
	require.NotNil(t, app.managerConfig.DurableDispatcher)
}

func TestDurableDispatcherBuildsScopeOnlyFromTrustedManagerContext(t *testing.T) {
	snapshot := validHandoffWork().Snapshot
	handoffs := NewHandoffCoordinator(&handoffServiceFake{creates: make(chan handoff.CreateRequest, 1)}, &submitterFake{}, 1)
	transcripts := NewTranscriptCoordinator(&transcriptWriterFake{}, &submitterFake{}, 1)
	dispatcher := &durableDispatcher{handoffs: handoffs, transcripts: transcripts, snapshot: snapshot}
	trusted := session.TrustedSessionContext{
		IDs:     session.IDs{SessionID: "trusted-session", ConversationID: "trusted-conversation", Incarnation: 7},
		Subject: session.Subject{TenantID: "trusted-tenant", UserID: "trusted-user"},
	}
	require.NoError(t, dispatcher.DispatchHandoff(t.Context(), trusted, session.HandoffRequestedEffect{
		OriginEpoch: 3, Turn: identity.TurnKey{ID: "trusted-turn", Revision: 1}, Transcript: "trusted transcript",
		ResponseID: "response", CallID: "call", ToolName: handoff.ToolCancelBackgroundTask,
		Arguments: json.RawMessage(`{"handoff_id":"h-1"}`),
	}))
	queued := <-handoffs.work
	assert.Equal(t, "trusted-tenant", queued.Scope.TenantID)
	assert.Equal(t, "trusted-user", queued.Scope.UserID)
	assert.Equal(t, identity.ConversationID("trusted-conversation"), queued.Scope.ConversationID)
	assert.Equal(t, identity.SessionID("trusted-session"), queued.SessionID)
	assert.Equal(t, identity.Incarnation(7), queued.Incarnation)
	assert.Equal(t, snapshot, queued.Snapshot)
}

func TestRunPropagatesUnexpectedCoordinatorFailure(t *testing.T) {
	sentinel := errors.New("transcript store failed")
	cfg := validConfig()
	cfg.Features.Handoffs = true
	dependencies := DurableDependencies{
		Service:              &handoffServiceFake{creates: make(chan handoff.CreateRequest, 1)},
		ConversationWriter:   &transcriptWriterFake{err: sentinel},
		Journal:              newCoordinatorJournalFake(),
		DeliveryAffinity:     "run-test-affinity",
		JournalOwner:         "run-test",
		JournalClaimLease:    time.Minute,
		HandoffQueueCapacity: 1, TranscriptQueueCapacity: 1, Snapshot: validHandoffWork().Snapshot,
	}
	app, err := New(cfg, testkit.NewFakeConnector(1), WithIDGenerator(deterministicIDs{}), WithDurableHandoffs(dependencies), testHandoffPresentationOption())
	require.NoError(t, err)
	listener := newBlockingListener()
	app.listener = listener
	require.NoError(t, app.transcripts.Enqueue(TranscriptWork{
		SessionID: "session", Incarnation: 1,
		Scope: handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "conversation"},
		Turn:  identity.TurnKey{ID: "turn", Revision: 1}, Sequence: 1, Role: "user", FinalText: "text",
	}))

	err = app.Run(t.Context())
	require.ErrorIs(t, err, sentinel)
}

func TestRunPropagatesUnexpectedHandoffListenerFailure(t *testing.T) {
	sentinel := errors.New("event listener failed")
	cfg := validConfig()
	cfg.Features.Handoffs = true
	dependencies := DurableDependencies{
		Service:            &handoffServiceFake{creates: make(chan handoff.CreateRequest, 1)},
		ConversationWriter: &transcriptWriterFake{items: make(chan postgres.ConversationItem, 1)},
		Journal:            newCoordinatorJournalFake(), DeliveryAffinity: "listener-test-affinity", JournalOwner: "listener-test", JournalClaimLease: time.Minute,
		HandoffQueueCapacity: 1, TranscriptQueueCapacity: 1, Snapshot: validHandoffWork().Snapshot,
	}
	app, err := New(cfg, testkit.NewFakeConnector(1), WithIDGenerator(deterministicIDs{}), WithDurableHandoffs(dependencies), testHandoffPresentationOptionWithListener(gatewayEventListenerFake{err: sentinel}))
	require.NoError(t, err)
	app.listener = newBlockingListener()

	err = app.Run(t.Context())
	require.ErrorIs(t, err, sentinel)
}

func TestRunFailsBeforeServingWhenCoordinatorAffinityIsAlreadyOwned(t *testing.T) {
	journal := newCoordinatorJournalFake()
	journal.affinityErr = postgres.ErrCoordinatorAffinityInUse
	cfg := validConfig()
	cfg.Features.Handoffs = true
	dependencies := DurableDependencies{
		Service:            &handoffServiceFake{creates: make(chan handoff.CreateRequest, 1)},
		ConversationWriter: &transcriptWriterFake{items: make(chan postgres.ConversationItem, 1)},
		Journal:            journal, DeliveryAffinity: "shared-affinity", JournalOwner: "second-app", JournalClaimLease: time.Minute,
		HandoffQueueCapacity: 1, TranscriptQueueCapacity: 1, Snapshot: validHandoffWork().Snapshot,
	}
	app, err := New(cfg, testkit.NewFakeConnector(1), WithIDGenerator(deterministicIDs{}), WithDurableHandoffs(dependencies), testHandoffPresentationOption())
	require.NoError(t, err)
	app.listener = newBlockingListener()
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	err = app.Run(ctx)
	require.ErrorIs(t, err, postgres.ErrCoordinatorAffinityInUse)
}

func TestRunRejectsNilAffinityLockAndJoinsReleaseFailure(t *testing.T) {
	newDurableApp := func(journal *coordinatorJournalFake) *App {
		cfg := validConfig()
		cfg.Features.Handoffs = true
		app, err := New(cfg, testkit.NewFakeConnector(1), WithIDGenerator(deterministicIDs{}), WithDurableHandoffs(DurableDependencies{
			Service:            &handoffServiceFake{creates: make(chan handoff.CreateRequest, 1)},
			ConversationWriter: &transcriptWriterFake{items: make(chan postgres.ConversationItem, 1)},
			Journal:            journal, DeliveryAffinity: "app-affinity", JournalOwner: "app-owner", JournalClaimLease: time.Minute,
			HandoffQueueCapacity: 1, TranscriptQueueCapacity: 1, Snapshot: validHandoffWork().Snapshot,
		}), testHandoffPresentationOption())
		require.NoError(t, err)
		app.listener = newBlockingListener()
		return app
	}

	nilJournal := newCoordinatorJournalFake()
	nilJournal.affinityNil = true
	require.ErrorContains(t, newDurableApp(nilJournal).Run(t.Context()), "nil lock")

	releaseErr := errors.New("affinity connection close failed")
	releaseJournal := newCoordinatorJournalFake()
	releaseJournal.affinityLock = errorCloser{err: releaseErr}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- newDurableApp(releaseJournal).Run(ctx) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, releaseErr)
}

type blockingListener struct {
	closed chan struct{}
	once   sync.Once
}

type errorCloser struct{ err error }

func (c errorCloser) Close() error { return c.err }

func newBlockingListener() *blockingListener { return &blockingListener{closed: make(chan struct{})} }
func (l *blockingListener) Accept() (net.Conn, error) {
	<-l.closed
	return nil, net.ErrClosed
}
func (l *blockingListener) Close() error { l.once.Do(func() { close(l.closed) }); return nil }
func (*blockingListener) Addr() net.Addr { return staticAddr("blocking") }

func TestLocalAdmissionUsesOnlyFixedTrustedDevelopmentSubject(t *testing.T) {
	app, err := New(validConfig(), testkit.NewFakeConnector(4), WithIDGenerator(deterministicIDs{}))
	require.NoError(t, err)
	request := httptest.NewRequest("GET", "/v1/realtime?tenant=attacker&user=attacker", nil)
	request.Header.Set("X-Tenant-ID", "attacker")
	subject, release, err := app.admission.Admit(t.Context(), request)
	require.NoError(t, err)
	require.NotNil(t, release)
	release()
	require.Equal(t, "development", subject.TenantID)
	require.Equal(t, "local-simulator", subject.UserID)
	require.Contains(t, subject.Scopes, "realtime")
	require.True(t, subject.ExpiresAt.IsZero())
}

func TestProductionFailsClosedWithoutExplicitAdmission(t *testing.T) {
	cfg := validConfig()
	cfg.Mode = "production"
	cfg.Gateway.TLS.CertFile = "server.crt"
	cfg.Gateway.TLS.KeyFile = "server.key"
	cfg.Auth.OIDCIssuer = "https://issuer.example"
	cfg.Auth.OIDCAudience = "live"
	cfg.Auth.OIDCRequiredScopes = []string{"realtime"}
	cfg.Auth.AllowedOrigins = []string{"https://app.example"}
	cfg.Store.DatabaseURLEnv = "DATABASE_URL"
	_, err := New(cfg, testkit.NewFakeConnector(4), WithIDGenerator(deterministicIDs{}))
	require.ErrorContains(t, err, "admission")

	admission := transportws.AdmissionFunc(func(context.Context, *http.Request) (session.Subject, func(), error) {
		return session.Subject{TenantID: "tenant", UserID: "user", Scopes: map[string]struct{}{}, ExpiresAt: time.Now().Add(time.Minute)}, nil, nil
	})
	_, err = New(cfg, testkit.NewFakeConnector(4), WithIDGenerator(deterministicIDs{}), WithAdmission(admission))
	require.NoError(t, err)
}

func TestRunStopsOnContextCancellationWithoutFixedPort(t *testing.T) {
	app, err := New(validConfig(), testkit.NewFakeConnector(4), WithIDGenerator(deterministicIDs{}))
	require.NoError(t, err)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	started := make(chan struct{})
	app.listener = &signalingListener{Listener: listener, started: started}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- app.Run(ctx) }()
	select {
	case <-started:
	case <-time.After(gatewayTestTimeout):
		t.Fatal("gateway did not begin serving")
	}
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(gatewayTestTimeout):
		t.Fatal("gateway did not stop after context cancellation")
	}
}

type signalingListener struct {
	net.Listener
	started chan struct{}
	once    sync.Once
}

func (l *signalingListener) Accept() (net.Conn, error) {
	l.once.Do(func() { close(l.started) })
	return l.Listener.Accept()
}

func TestRunPropagatesUnexpectedServeError(t *testing.T) {
	app, err := New(validConfig(), testkit.NewFakeConnector(4), WithIDGenerator(deterministicIDs{}))
	require.NoError(t, err)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	require.NoError(t, listener.Close())
	app.listener = listener
	err = app.Run(t.Context())
	require.Error(t, err)
	require.False(t, errors.Is(err, context.Canceled))
}

func TestRunCleansActiveWebSocketBeforeReturningUnexpectedServeError(t *testing.T) {
	sentinelServeErr := errors.New("sentinel permanent accept failure")
	failAccept := make(chan struct{})
	acceptStarted := make(chan struct{})
	baseListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	listener := &acceptOnceThenErrorListener{
		Listener: baseListener,
		fail:     failAccept,
		err:      sentinelServeErr,
		started:  acceptStarted,
	}

	releaseStarted := make(chan struct{})
	allowRelease := make(chan struct{})
	var allowReleaseOnce sync.Once
	completeRelease := func() { allowReleaseOnce.Do(func() { close(allowRelease) }) }
	t.Cleanup(completeRelease)
	var releases atomic.Int32
	admission := transportws.AdmissionFunc(func(context.Context, *http.Request) (session.Subject, func(), error) {
		return session.Subject{TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Minute)}, func() {
			if releases.Add(1) == 1 {
				close(releaseStarted)
			}
			<-allowRelease
		}, nil
	})
	connector := testkit.NewFakeConnector(4)
	app, err := New(validConfig(), connector, WithIDGenerator(deterministicIDs{}), WithAdmission(admission))
	require.NoError(t, err)
	app.listener = listener
	runResult := make(chan error, 1)
	go func() { runResult <- app.Run(t.Context()) }()
	select {
	case <-acceptStarted:
	case <-time.After(gatewayTestTimeout):
		t.Fatal("gateway did not begin accepting")
	}

	dialContext, cancelDial := context.WithTimeout(t.Context(), gatewayTestTimeout)
	defer cancelDial()
	client, response, err := websocket.Dial(dialContext, "ws://"+listener.Addr().String()+"/v1/realtime", nil)
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}
	require.NoError(t, err)
	defer client.CloseNow()
	voice, err := connector.NextSession(dialContext)
	require.NoError(t, err)
	(&gatewayClient{t: t, conn: client}).expectType("session.created")

	socketClosed := make(chan error, 1)
	go func() {
		_, _, err := client.Read(t.Context())
		socketClosed <- err
	}()
	providerClosed := make(chan error, 1)
	go func() {
		_, err := voice.NextCommand(t.Context())
		providerClosed <- err
	}()
	close(failAccept)

	waitLifecycle := func(name string, event <-chan error) error {
		t.Helper()
		select {
		case err := <-event:
			return err
		case err := <-runResult:
			t.Fatalf("Run returned before %s cleanup completed: %v", name, err)
			return nil
		case <-time.After(gatewayTestTimeout):
			t.Fatalf("timed out waiting for %s cleanup", name)
			return nil
		}
	}
	require.Error(t, waitLifecycle("client socket", socketClosed))
	require.ErrorIs(t, waitLifecycle("provider session", providerClosed), testkit.ErrFakeVoiceClosed)
	select {
	case <-releaseStarted:
	case err := <-runResult:
		t.Fatalf("Run returned before admission release started: %v", err)
	case <-time.After(gatewayTestTimeout):
		t.Fatal("timed out waiting for admission release")
	}
	require.Equal(t, int32(1), releases.Load())
	select {
	case err := <-runResult:
		t.Fatalf("Run returned before admission release completed: %v", err)
	default:
	}
	completeRelease()
	select {
	case err := <-runResult:
		require.Error(t, err)
		require.ErrorIs(t, err, sentinelServeErr)
	case <-time.After(gatewayTestTimeout):
		t.Fatal("Run did not return the sentinel serve error after bounded cleanup")
	}
	require.Equal(t, int32(1), releases.Load())
}

func TestRunPreservesUnexpectedServeErrorWhenCleanupFails(t *testing.T) {
	sentinelServeErr := errors.New("sentinel permanent serve failure")
	app, err := New(validConfig(), testkit.NewFakeConnector(4), WithIDGenerator(deterministicIDs{}))
	require.NoError(t, err)
	app.listener = permanentErrorListener{err: sentinelServeErr}
	app.shutdownTimeout = 10 * time.Millisecond
	require.True(t, app.activeHandlers.begin())

	err = app.Run(t.Context())

	require.ErrorIs(t, err, sentinelServeErr)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	app.activeHandlers.end()
}

type permanentErrorListener struct {
	err error
}

func (l permanentErrorListener) Accept() (net.Conn, error) { return nil, l.err }
func (permanentErrorListener) Close() error                { return nil }
func (permanentErrorListener) Addr() net.Addr              { return staticAddr("permanent-error") }

type staticAddr string

func (a staticAddr) Network() string { return "test" }
func (a staticAddr) String() string  { return string(a) }

type acceptOnceThenErrorListener struct {
	net.Listener
	fail    <-chan struct{}
	err     error
	started chan struct{}
	once    sync.Once
	accepts atomic.Int32
}

func (l *acceptOnceThenErrorListener) Accept() (net.Conn, error) {
	l.once.Do(func() { close(l.started) })
	if l.accepts.Add(1) == 1 {
		return l.Listener.Accept()
	}
	<-l.fail
	return nil, l.err
}

func TestRunDrainsWebSocketsOnSignalAndAdminTrigger(t *testing.T) {
	for _, trigger := range []string{"signal", "admin"} {
		t.Run(trigger, func(t *testing.T) {
			releaseStarted := make(chan struct{})
			allowRelease := make(chan struct{})
			var allowReleaseOnce sync.Once
			completeRelease := func() { allowReleaseOnce.Do(func() { close(allowRelease) }) }
			t.Cleanup(completeRelease)
			admission := transportws.AdmissionFunc(func(context.Context, *http.Request) (session.Subject, func(), error) {
				return session.Subject{TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Minute)}, func() {
					close(releaseStarted)
					<-allowRelease
				}, nil
			})
			cfg := validConfig()
			cfg.Shutdown.GatewayDrainTimeout = 150 * time.Millisecond
			cfg.Shutdown.CleanupTimeout = time.Second
			connector := testkit.NewFakeConnector(4)
			app, err := New(cfg, connector, WithIDGenerator(deterministicIDs{}), WithAdmission(admission))
			require.NoError(t, err)
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			started := make(chan struct{})
			app.listener = &signalingListener{Listener: listener, started: started}
			ctx, cancelRun := context.WithCancel(t.Context())
			defer cancelRun()
			done := make(chan error, 1)
			go func() { done <- app.Run(ctx) }()
			select {
			case <-started:
			case <-time.After(gatewayTestTimeout):
				t.Fatal("gateway did not begin serving")
			}
			dialContext, cancelDial := context.WithTimeout(t.Context(), gatewayTestTimeout)
			defer cancelDial()
			url := "ws://" + listener.Addr().String() + "/v1/realtime"
			client, _, err := websocket.Dial(dialContext, url, nil)
			require.NoError(t, err)
			defer client.CloseNow()
			gatewayClient := &gatewayClient{t: t, conn: client}
			gatewayClient.expectType("session.created")
			voice, err := connector.NextSession(dialContext)
			require.NoError(t, err)

			if trigger == "signal" {
				cancelRun()
			} else {
				app.Drainer().Begin()
			}
			select {
			case <-app.Drainer().Begun():
			case <-time.After(gatewayTestTimeout):
				t.Fatal("drain did not begin")
			}
			if app.Drainer().Accepting() {
				t.Fatal("gateway still accepting after drain trigger")
			}
			ready := health.NewRegistry()
			require.NoError(t, ready.RegisterDrainer(app.Drainer()))
			response := httptest.NewRecorder()
			ready.ReadyHandler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
			require.Equal(t, http.StatusServiceUnavailable, response.Code)

			_, rejected, dialErr := websocket.Dial(dialContext, url, nil)
			require.Error(t, dialErr)
			if rejected != nil {
				require.Equal(t, http.StatusServiceUnavailable, rejected.StatusCode)
				rejected.Body.Close()
			}
			pcm := testkit.RampPCM(960)
			gatewayClient.appendPCM(pcm)
			command := nextCommand(t, voice)
			require.Equal(t, realtime.CommandAppendAudio, command.Kind)
			require.Equal(t, pcm, command.Audio, "existing websocket I/O must survive the grace period")

			clientClosed := make(chan error, 1)
			go func() {
				_, _, err := client.Read(t.Context())
				clientClosed <- err
			}()
			select {
			case err := <-clientClosed:
				require.Error(t, err)
			case <-time.After(gatewayTestTimeout):
				t.Fatal("active websocket was not force-closed at the drain deadline")
			}
			select {
			case <-releaseStarted:
			case <-time.After(gatewayTestTimeout):
				t.Fatal("active websocket release did not start")
			}
			completeRelease()
			select {
			case runErr := <-done:
				require.ErrorContains(t, runErr, "drain")
			case <-time.After(gatewayTestTimeout):
				t.Fatal("Run did not finish after forced websocket release")
			}
		})
	}
}

type gatewayClient struct {
	t    *testing.T
	conn *websocket.Conn
}

func startGatewayHarness(t *testing.T) (*gatewayClient, *testkit.FakeVoiceSession) {
	t.Helper()
	connector := testkit.NewFakeConnector(64)
	app, err := New(validConfig(), connector, WithIDGenerator(deterministicIDs{}))
	require.NoError(t, err)
	server := httptest.NewServer(app.handler)
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(t.Context(), gatewayTestTimeout)
	t.Cleanup(cancel)
	conn, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/realtime", nil)
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.CloseNow() })
	voice, err := connector.NextSession(ctx)
	require.NoError(t, err)
	return &gatewayClient{t: t, conn: conn}, voice
}

func (c *gatewayClient) write(raw []byte) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(c.t.Context(), gatewayTestTimeout)
	defer cancel()
	require.NoError(c.t, c.conn.Write(ctx, websocket.MessageText, append([]byte(nil), raw...)))
}

func (c *gatewayClient) nextEvent() (string, []byte) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(c.t.Context(), gatewayTestTimeout)
	defer cancel()
	typ, raw, err := c.conn.Read(ctx)
	require.NoError(c.t, err)
	require.Equal(c.t, websocket.MessageText, typ)
	var envelope struct {
		Type string `json:"type"`
	}
	require.NoError(c.t, json.Unmarshal(raw, &envelope))
	return envelope.Type, append([]byte(nil), raw...)
}

func (c *gatewayClient) expectType(want string) []byte {
	c.t.Helper()
	typ, raw := c.nextEvent()
	require.Equal(c.t, want, typ)
	return raw
}

func (c *gatewayClient) appendPCM(pcm []byte) {
	c.t.Helper()
	raw, err := json.Marshal(map[string]string{
		"type":  "input_audio_buffer.append",
		"audio": base64.StdEncoding.EncodeToString(pcm),
	})
	require.NoError(c.t, err)
	c.write(raw)
}

func (c *gatewayClient) commitAudio() {
	c.write([]byte(`{"type":"input_audio_buffer.commit"}`))
}

func (c *gatewayClient) expectPCM(want []byte) {
	c.t.Helper()
	raw := c.expectType("response.output_audio.delta")
	var event protocol.ResponseOutputAudioDeltaEvent
	require.NoError(c.t, json.Unmarshal(raw, &event))
	got, err := base64.StdEncoding.DecodeString(event.Delta)
	require.NoError(c.t, err)
	require.Equal(c.t, want, got)
}

func (c *gatewayClient) expectResponseDone(responseID string) {
	c.t.Helper()
	raw := c.expectType("response.done")
	var event protocol.ResponseDoneEvent
	require.NoError(c.t, json.Unmarshal(raw, &event))
	require.Equal(c.t, responseID, event.Response.ID)
}

func nextCommand(t *testing.T, voice *testkit.FakeVoiceSession) realtime.Command {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), gatewayTestTimeout)
	defer cancel()
	command, err := voice.NextCommand(ctx)
	require.NoError(t, err)
	return command
}

func emit(t *testing.T, voice *testkit.FakeVoiceSession, event realtime.Event) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), gatewayTestTimeout)
	defer cancel()
	require.NoError(t, voice.Emit(ctx, event))
}

func establishResponse(t *testing.T, client *gatewayClient, voice *testkit.FakeVoiceSession) {
	t.Helper()
	// The provider connector marks unsolicited response.created events for an
	// epoch binding; false is reserved for explicit response.create commands.
	emit(t, voice, realtime.Event{
		Kind: realtime.EventResponseCreated, ProviderEventID: "provider-created-1",
		ResponseID: "r-1", Status: "in_progress", NeedsEpochBinding: true,
	})
	bind := nextCommand(t, voice)
	require.Equal(t, realtime.CommandBindResponseEpoch, bind.Kind)
	require.Equal(t, identity.ResponseEpoch(1), bind.Epoch)
	client.expectType("response.created")
}

func TestGatewayAudioVerticalSlice(t *testing.T) {
	client, voice := startGatewayHarness(t)
	client.expectType("session.created")
	client.appendPCM(testkit.RampPCM(1_920))
	client.commitAudio()
	first := nextCommand(t, voice)
	second := nextCommand(t, voice)
	require.Equal(t, []uint64{1, 2}, []uint64{first.Sequence, second.Sequence})
	require.Equal(t, testkit.RampPCM(1_920), append(first.Audio, second.Audio...))
	commit := nextCommand(t, voice)
	require.Equal(t, realtime.CommandCommitAudio, commit.Kind)
	require.Equal(t, uint64(2), commit.AfterSeq)

	establishResponse(t, client, voice)
	emit(t, voice, realtime.Event{
		Kind: realtime.EventAudioDelta, Epoch: 1, ProviderEventID: "provider-audio-1",
		ResponseID: "r-1", ItemID: "item-1", Audio: testkit.RampPCM(960),
	})
	client.expectPCM(testkit.RampPCM(960))
	emit(t, voice, realtime.Event{
		Kind: realtime.EventResponseDone, Epoch: 1, ProviderEventID: "provider-done-1",
		ResponseID: "r-1", Status: "completed",
	})
	client.expectResponseDone("r-1")
}

func TestGatewayDropsLateAudioFromInterruptedEpoch(t *testing.T) {
	client, voice := startGatewayHarness(t)
	client.expectType("session.created")
	establishResponse(t, client, voice)
	emit(t, voice, realtime.Event{Kind: realtime.EventSpeechStarted, ProviderEventID: "provider-speech-started", ItemID: "user-item"})
	emit(t, voice, realtime.Event{
		Kind: realtime.EventAudioDelta, Epoch: 1, ProviderEventID: "provider-late-audio",
		ResponseID: "r-1", ItemID: "assistant-item", Audio: testkit.RampPCM(960),
	})
	emit(t, voice, realtime.Event{Kind: realtime.EventSpeechStopped, ProviderEventID: "provider-speech-stopped", ItemID: "user-item"})
	cancel := nextCommand(t, voice)
	require.Equal(t, realtime.CommandCancelResponse, cancel.Kind)
	require.Equal(t, identity.ResponseEpoch(2), cancel.Epoch)

	seen := map[string]bool{}
	for range 3 {
		typ, _ := client.nextEvent()
		require.NotEqual(t, "response.output_audio.delta", typ, "late epoch-1 audio crossed the speech fence")
		seen[typ] = true
	}
	require.Equal(t, map[string]bool{
		"input_audio_buffer.speech_started": true,
		"input_audio_buffer.speech_stopped": true,
		"response.done":                     true,
	}, seen)
}
