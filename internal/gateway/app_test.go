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

	"github.com/chuanmingliu/openai-gpt-live/internal/config"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/protocol"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/chuanmingliu/openai-gpt-live/internal/session"
	"github.com/chuanmingliu/openai-gpt-live/internal/testkit"
	transportws "github.com/chuanmingliu/openai-gpt-live/internal/transport/ws"
	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

const gatewayTestTimeout = 3 * time.Second

type deterministicIDs struct{}

func (deterministicIDs) NewSessionID() identity.SessionID           { return "session-fixed" }
func (deterministicIDs) NewConversationID() identity.ConversationID { return "conversation-fixed" }

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
	require.WithinDuration(t, time.Now().Add(defaultDevelopmentSubjectTTL), subject.ExpiresAt, 2*time.Second)
}

func TestProductionFailsClosedWithoutExplicitAdmission(t *testing.T) {
	cfg := validConfig()
	cfg.Mode = "production"
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

func TestRunWaitsForActiveWebSocketReleaseDuringShutdown(t *testing.T) {
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
	connector := testkit.NewFakeConnector(4)
	app, err := New(validConfig(), connector, WithIDGenerator(deterministicIDs{}), WithAdmission(admission))
	require.NoError(t, err)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	started := make(chan struct{})
	app.listener = &signalingListener{Listener: listener, started: started}
	ctx, cancelRun := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- app.Run(ctx) }()
	select {
	case <-started:
	case <-time.After(gatewayTestTimeout):
		t.Fatal("gateway did not begin serving")
	}
	dialContext, cancelDial := context.WithTimeout(t.Context(), gatewayTestTimeout)
	defer cancelDial()
	client, _, err := websocket.Dial(dialContext, "ws://"+listener.Addr().String()+"/v1/realtime", nil)
	require.NoError(t, err)
	defer client.CloseNow()
	gatewayClient := &gatewayClient{t: t, conn: client}
	gatewayClient.expectType("session.created")
	clientClosed := make(chan error, 1)
	go func() {
		_, _, err := client.Read(t.Context())
		clientClosed <- err
	}()
	cancelRun()
	select {
	case err := <-clientClosed:
		require.Error(t, err)
	case <-time.After(gatewayTestTimeout):
		t.Fatal("active websocket read did not unblock")
	}
	select {
	case <-releaseStarted:
	case <-time.After(gatewayTestTimeout):
		t.Fatal("active websocket release did not start")
	}
	select {
	case err := <-done:
		t.Fatalf("Run returned before active websocket release completed: %v", err)
	default:
	}
	completeRelease()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(gatewayTestTimeout):
		t.Fatal("Run did not finish after active websocket release")
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
