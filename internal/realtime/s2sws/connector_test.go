package s2sws_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/protocol"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime/s2sws"
	sessionpkg "github.com/chuanmingliu/openai-gpt-live/internal/session"
	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type capturedRequest struct {
	path   string
	query  url.Values
	header http.Header
}

type serverAction struct {
	payload []byte
	ping    bool
	close   bool
	status  websocket.StatusCode
	reason  string
	ack     chan error
}

type testVoiceServer struct {
	URL       string
	requests  chan capturedRequest
	received  chan []byte
	peerClose chan error
	actions   chan serverAction
	cancel    context.CancelFunc
	server    *httptest.Server
}

type compositionClient struct {
	createdStarted chan struct{}
	createdRelease chan struct{}
	createdOnce    sync.Once
}

func (c *compositionClient) Read(ctx context.Context) (protocol.ClientEvent, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (c *compositionClient) Write(ctx context.Context, event protocol.ServerEvent) error {
	if event.EventType() == "session.created" {
		c.createdOnce.Do(func() { close(c.createdStarted) })
		select {
		case <-c.createdRelease:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (*compositionClient) Close(context.Context, int, string) error { return nil }

type compositionIDs struct{}

func (compositionIDs) NewSessionID() identity.SessionID           { return "composition-session" }
func (compositionIDs) NewConversationID() identity.ConversationID { return "composition-conversation" }

type observedContext struct {
	context.Context
	observed chan struct{}
	once     sync.Once
}

func newObservedContext(parent context.Context) *observedContext {
	return &observedContext{Context: parent, observed: make(chan struct{})}
}

func (c *observedContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.observed) })
	return c.Context.Done()
}

type controlledDeadlineContext struct {
	parent       context.Context
	done         chan struct{}
	observed     chan struct{}
	observeOnce  sync.Once
	deadlineOnce sync.Once
}

func newControlledDeadlineContext(parent context.Context) *controlledDeadlineContext {
	return &controlledDeadlineContext{
		parent:   parent,
		done:     make(chan struct{}),
		observed: make(chan struct{}),
	}
}

func (c *controlledDeadlineContext) Deadline() (time.Time, bool) { return time.Time{}, false }

func (c *controlledDeadlineContext) Done() <-chan struct{} {
	c.observeOnce.Do(func() { close(c.observed) })
	return c.done
}

func (c *controlledDeadlineContext) Err() error {
	select {
	case <-c.done:
		return context.DeadlineExceeded
	default:
		return c.parent.Err()
	}
}

func (c *controlledDeadlineContext) Value(key any) any { return c.parent.Value(key) }

func (c *controlledDeadlineContext) expire() {
	c.deadlineOnce.Do(func() { close(c.done) })
}

func newTestVoiceServer(t *testing.T) *testVoiceServer {
	return newTestVoiceServerWithReadGate(t, nil)
}

func newTestVoiceServerWithReadGate(t *testing.T, readGate <-chan struct{}) *testVoiceServer {
	t.Helper()

	serverContext, cancel := context.WithCancel(context.Background())
	voice := &testVoiceServer{
		requests:  make(chan capturedRequest, 1),
		received:  make(chan []byte, 512),
		peerClose: make(chan error, 1),
		actions:   make(chan serverAction),
		cancel:    cancel,
	}
	voice.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			select {
			case voice.peerClose <- err:
			default:
			}
			return
		}
		defer conn.CloseNow()
		voice.requests <- capturedRequest{
			path:   r.URL.Path,
			query:  r.URL.Query(),
			header: r.Header.Clone(),
		}

		readErrors := make(chan error, 1)
		go func() {
			for {
				if readGate != nil {
					select {
					case <-readGate:
					case <-serverContext.Done():
						return
					}
				}
				messageType, raw, err := conn.Read(serverContext)
				if err != nil {
					readErrors <- err
					return
				}
				if messageType != websocket.MessageText {
					readErrors <- fmt.Errorf("client message type = %v, want text", messageType)
					return
				}
				select {
				case voice.received <- append([]byte(nil), raw...):
				case <-serverContext.Done():
					return
				}
			}
		}()

		for {
			select {
			case action := <-voice.actions:
				if action.ping {
					err = conn.Ping(serverContext)
				} else if action.close {
					err = conn.Close(action.status, action.reason)
				} else {
					err = conn.Write(serverContext, websocket.MessageText, action.payload)
				}
				action.ack <- err
				if action.close || err != nil {
					return
				}
			case err = <-readErrors:
				select {
				case voice.peerClose <- err:
				default:
				}
				return
			case <-serverContext.Done():
				return
			}
		}
	}))
	voice.URL = "ws" + strings.TrimPrefix(voice.server.URL, "http") + "/v1/realtime"
	t.Cleanup(func() {
		cancel()
		voice.server.Close()
	})
	return voice
}

func (s *testVoiceServer) ping(t *testing.T) {
	t.Helper()
	ack := make(chan error, 1)
	select {
	case s.actions <- serverAction{ping: true, ack: ack}:
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
	select {
	case err := <-ack:
		require.NoError(t, err)
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
}

func (s *testVoiceServer) sendRaw(t *testing.T, raw []byte) {
	t.Helper()
	ack := make(chan error, 1)
	select {
	case s.actions <- serverAction{payload: append([]byte(nil), raw...), ack: ack}:
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
	select {
	case err := <-ack:
		require.NoError(t, err)
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
}

func (s *testVoiceServer) sendEvent(t *testing.T, event any) {
	t.Helper()
	raw, err := json.Marshal(event)
	require.NoError(t, err)
	s.sendRaw(t, raw)
}

func (s *testVoiceServer) closePeer(t *testing.T, status websocket.StatusCode, reason string) {
	t.Helper()
	ack := make(chan error, 1)
	select {
	case s.actions <- serverAction{close: true, status: status, reason: reason, ack: ack}:
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
	select {
	case err := <-ack:
		require.NoError(t, err)
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
}

func (s *testVoiceServer) nextClientFrame(t *testing.T) []byte {
	t.Helper()
	select {
	case raw := <-s.received:
		return raw
	case err := <-s.peerClose:
		t.Fatalf("connection closed before client frame: %v", err)
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
	return nil
}

func connectTestSession(t *testing.T, serverURL string, mutate func(*s2sws.Config), request realtime.OpenRequest) realtime.Session {
	t.Helper()
	cfg := s2sws.Config{
		URL:            serverURL,
		ConnectTimeout: time.Second,
		MaxFrameBytes:  4 << 20,
		Model:          "config-model",
	}
	if mutate != nil {
		mutate(&cfg)
	}
	connector, err := s2sws.New(cfg)
	require.NoError(t, err)
	session, err := connector.Connect(t.Context(), request)
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close(context.Background()) })
	return session
}

func receiveEvent(t *testing.T, session realtime.Session) realtime.Event {
	t.Helper()
	event, err := session.Receive(t.Context())
	require.NoError(t, err)
	return event
}

func configureInputTurnMode(
	t *testing.T,
	server *testVoiceServer,
	session realtime.Session,
	turnDetection json.RawMessage,
	providerTurnDetection any,
	want realtime.InputTurnMode,
) {
	t.Helper()
	const sequence = uint64(9001)
	require.NoError(t, session.Send(t.Context(), realtime.Command{
		Kind:     realtime.CommandSessionUpdate,
		Sequence: sequence,
		Session:  realtime.SessionConfig{TurnDetection: turnDetection},
	}))
	var update struct {
		Type string `json:"type"`
	}
	require.NoError(t, json.Unmarshal(server.nextClientFrame(t), &update))
	require.Equal(t, "session.update", update.Type)
	server.sendEvent(t, map[string]any{
		"type": "session.updated",
		"session": map[string]any{
			"type": "realtime",
			"audio": map[string]any{
				"input": map[string]any{"turn_detection": providerTurnDetection},
			},
		},
	})
	confirmed := receiveEvent(t, session)
	require.Equal(t, realtime.EventSessionUpdated, confirmed.Kind)
	require.True(t, confirmed.SessionUpdateAcknowledgement)
	require.Equal(t, sequence, confirmed.SessionUpdateSequence)
	require.Equal(t, want, confirmed.InputTurnMode)
}

func configureManualInputTurn(t *testing.T, server *testVoiceServer, session realtime.Session) {
	t.Helper()
	configureInputTurnMode(t, server, session, json.RawMessage(`null`), nil, realtime.InputTurnModeManual)
}

func configureProviderVADInputTurn(t *testing.T, server *testVoiceServer, session realtime.Session) {
	t.Helper()
	configureInputTurnMode(t, server, session, json.RawMessage(`{"type":"server_vad"}`), map[string]any{"type": "server_vad"}, realtime.InputTurnModeProviderVAD)
}

func TestManagerBargeCancelProgressesThroughSaturatedAdapterDelivery(t *testing.T) {
	server := newTestVoiceServer(t)
	connector, err := s2sws.New(s2sws.Config{
		URL: server.URL, ConnectTimeout: time.Second, MaxFrameBytes: 1 << 20, Model: "composition-model",
	})
	require.NoError(t, err)
	manager := sessionpkg.NewManager(sessionpkg.Config{
		ControlEvents: 1000, WriterControlEvents: 1000,
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
		InitialSession: realtime.SessionConfig{TurnDetection: json.RawMessage(`{"type":"server_vad"}`)},
	}, connector, compositionIDs{})
	client := &compositionClient{createdStarted: make(chan struct{}), createdRelease: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- manager.Run(ctx, sessionpkg.OpenRequest{Subject: sessionpkg.Subject{
			TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour),
		}}, client)
	}()
	select {
	case <-client.createdStarted:
	case err := <-done:
		t.Fatalf("manager stopped before session.created gate: %v", err)
	case <-time.After(time.Second):
		t.Fatal("session.created did not reach gate")
	}
	var initial struct {
		Type string `json:"type"`
	}
	require.NoError(t, json.Unmarshal(server.nextClientFrame(t), &initial))
	require.Equal(t, "session.update", initial.Type)
	server.sendEvent(t, map[string]any{
		"type": "session.updated",
		"session": map[string]any{
			"type": "realtime",
			"audio": map[string]any{
				"input": map[string]any{"turn_detection": map[string]any{"type": "server_vad"}},
			},
		},
	})

	server.sendEvent(t, map[string]any{
		"type": "response.created", "event_id": "composition-created",
		"response": map[string]any{"id": "composition-response", "status": "in_progress"},
	})
	for index := range 300 {
		server.sendEvent(t, map[string]any{
			"type": "input_audio_buffer.speech_started", "event_id": fmt.Sprintf("composition-speech-%03d", index),
			"item_id": fmt.Sprintf("composition-item-%03d", index), "audio_start_ms": index,
		})
	}
	close(client.createdRelease)

	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case raw := <-server.received:
			var frame struct {
				Type string `json:"type"`
			}
			require.NoError(t, json.Unmarshal(raw, &frame))
			if frame.Type == "response.cancel" {
				cancel()
				require.ErrorIs(t, <-done, context.Canceled)
				return
			}
		case err := <-done:
			t.Fatalf("manager stopped before barge cancel correlation: %v", err)
		case <-deadline.C:
			t.Fatal("barge cancel did not progress through saturated adapter delivery")
		}
	}
}

// TestNewRejectsInvalidConfig catches constructors that defer invalid endpoint
// and resource-limit failures until a live connection attempt.
func TestNewRejectsInvalidConfig(t *testing.T) {
	valid := s2sws.Config{
		URL:            "ws://127.0.0.1:8080/v1/realtime",
		ConnectTimeout: time.Second,
		MaxFrameBytes:  1 << 20,
	}

	tests := []struct {
		name   string
		mutate func(*s2sws.Config)
	}{
		{name: "empty URL", mutate: func(cfg *s2sws.Config) { cfg.URL = "" }},
		{name: "HTTP URL", mutate: func(cfg *s2sws.Config) { cfg.URL = "https://example.com/v1/realtime" }},
		{name: "relative URL", mutate: func(cfg *s2sws.Config) { cfg.URL = "/v1/realtime" }},
		{name: "userinfo", mutate: func(cfg *s2sws.Config) { cfg.URL = "wss://user@example.com/v1/realtime" }},
		{name: "query", mutate: func(cfg *s2sws.Config) { cfg.URL = "wss://example.com/v1/realtime?tenant=other" }},
		{name: "fragment", mutate: func(cfg *s2sws.Config) { cfg.URL = "wss://example.com/v1/realtime#hidden" }},
		{name: "opaque", mutate: func(cfg *s2sws.Config) { cfg.URL = "wss:provider-opaque" }},
		{name: "zero connect timeout", mutate: func(cfg *s2sws.Config) { cfg.ConnectTimeout = 0 }},
		{name: "negative connect timeout", mutate: func(cfg *s2sws.Config) { cfg.ConnectTimeout = -time.Second }},
		{name: "zero frame limit", mutate: func(cfg *s2sws.Config) { cfg.MaxFrameBytes = 0 }},
		{name: "negative frame limit", mutate: func(cfg *s2sws.Config) { cfg.MaxFrameBytes = -1 }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := valid
			test.mutate(&cfg)
			_, err := s2sws.New(cfg)
			require.Error(t, err)
		})
	}
}

// TestNewAcceptsCanonicalWebSocketEndpoint catches over-validation that rejects
// an ordinary absolute provider path or an optional API key/model.
func TestNewAcceptsCanonicalWebSocketEndpoint(t *testing.T) {
	connector, err := s2sws.New(s2sws.Config{
		URL:            "wss://provider.example/v1/realtime",
		ConnectTimeout: time.Second,
		MaxFrameBytes:  1 << 20,
	})
	require.NoError(t, err)
	require.NotNil(t, connector)
}

// TestConnectPreservesURLAndAppliesModelAndAuth catches dial construction that
// fails to derive the sole model query or emits empty auth.
func TestConnectPreservesURLAndAppliesModelAndAuth(t *testing.T) {
	tests := []struct {
		name          string
		apiKey        string
		configModel   string
		requestModel  string
		wantModel     string
		wantAuth      string
		wantAuthField bool
	}{
		{
			name:          "request model overrides config and API key is sent",
			apiKey:        "secret",
			configModel:   "config-model",
			requestModel:  "request-model",
			wantModel:     "request-model",
			wantAuth:      "Bearer secret",
			wantAuthField: true,
		},
		{
			name:        "config model fallback and empty API key omitted",
			configModel: "config-model",
			wantModel:   "config-model",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := newTestVoiceServer(t)
			session := connectTestSession(t, server.URL, func(cfg *s2sws.Config) {
				cfg.APIKey = test.apiKey
				cfg.Model = test.configModel
			}, realtime.OpenRequest{SessionID: "session-1", Model: test.requestModel})
			require.NotNil(t, session)

			request := <-server.requests
			assert.Equal(t, "/v1/realtime", request.path)
			assert.Equal(t, []string{test.wantModel}, request.query["model"])
			assert.Len(t, request.query, 1)
			assert.Equal(t, test.wantAuth, request.header.Get("Authorization"))
			_, present := request.header["Authorization"]
			assert.Equal(t, test.wantAuthField, present)
		})
	}
}

func TestReservedToolRegistrationUsesActualUpstreamSessionUpdateWireEvent(t *testing.T) {
	server := newTestVoiceServer(t)
	session := connectTestSession(t, server.URL, nil, realtime.OpenRequest{})
	require.NoError(t, session.Send(t.Context(), realtime.Command{
		Kind:    realtime.CommandSessionUpdate,
		Session: sessionpkg.ReservedHandoffSessionConfig(),
	}))
	raw := server.nextClientFrame(t)
	var event struct {
		Type    string `json:"type"`
		Session struct {
			Tools []struct {
				Type       string          `json:"type"`
				Name       string          `json:"name"`
				Parameters json.RawMessage `json:"parameters"`
			} `json:"tools"`
		} `json:"session"`
	}
	require.NoError(t, json.Unmarshal(raw, &event))
	require.Equal(t, "session.update", event.Type)
	require.Len(t, event.Session.Tools, 3)
	require.Equal(t, []string{
		"start_background_task", "steer_background_task", "cancel_background_task",
	}, []string{event.Session.Tools[0].Name, event.Session.Tools[1].Name, event.Session.Tools[2].Name})
	for _, tool := range event.Session.Tools {
		require.Equal(t, "function", tool.Type)
		var schema map[string]any
		require.NoError(t, json.Unmarshal(tool.Parameters, &schema))
		require.Equal(t, false, schema["additionalProperties"])
	}
}

// TestConnectRequiresAnEffectiveModel catches dials without a trusted model
// source when both the request and connector model are empty.
func TestConnectRequiresAnEffectiveModel(t *testing.T) {
	connector, err := s2sws.New(s2sws.Config{
		URL:            "ws://127.0.0.1:1/v1/realtime",
		ConnectTimeout: time.Second,
		MaxFrameBytes:  1 << 20,
	})
	require.NoError(t, err)

	_, err = connector.Connect(t.Context(), realtime.OpenRequest{})
	require.Error(t, err)
}

// TestConnectHonorsCancellation catches dials that outlive their caller.
func TestConnectHonorsCancellation(t *testing.T) {
	connector, err := s2sws.New(s2sws.Config{
		URL:            "ws://127.0.0.1:1/v1/realtime",
		Model:          "model",
		ConnectTimeout: time.Second,
		MaxFrameBytes:  1 << 20,
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err = connector.Connect(ctx, realtime.OpenRequest{})
	require.ErrorIs(t, err, context.Canceled)
}

// TestSendMapsEveryCommandToGoldenJSON catches field drift at the provider
// boundary, including raw insertion and accidental emission of bind commands.
func TestSendMapsEveryCommandToGoldenJSON(t *testing.T) {
	server := newTestVoiceServer(t)
	session := connectTestSession(t, server.URL, nil, realtime.OpenRequest{Model: "request-model"})

	update := realtime.Command{
		Kind: realtime.CommandSessionUpdate,
		Session: realtime.SessionConfig{
			Instructions: "be concise",
			InputFormat:  realtime.AudioFormat{Encoding: "pcm16le", SampleRate: 24_000, Channels: 1},
			OutputFormat: realtime.AudioFormat{Encoding: "pcm16le", SampleRate: 24_000, Channels: 1},
			Voice:        "alloy",
			Tools: []realtime.Tool{{
				Type:        "function",
				Name:        "lookup",
				Description: "look it up",
				Parameters:  json.RawMessage(`{"type":"object","required":["q"]}`),
			}},
			ToolChoice:    json.RawMessage(`{"type":"function","name":"lookup"}`),
			TurnDetection: json.RawMessage(`null`),
		},
	}
	require.NoError(t, session.Send(t.Context(), update))
	assert.JSONEq(t,
		`{"type":"session.update","session":{"type":"realtime","instructions":"be concise","audio":{"input":{"format":{"type":"audio/pcm","rate":24000},"turn_detection":null},"output":{"format":{"type":"audio/pcm","rate":24000},"voice":"alloy"}},"tools":[{"type":"function","name":"lookup","description":"look it up","parameters":{"type":"object","required":["q"]}}],"tool_choice":{"type":"function","name":"lookup"}}}`,
		string(server.nextClientFrame(t)),
	)
	server.sendEvent(t, map[string]any{
		"type": "session.updated",
		"session": map[string]any{
			"type": "realtime",
			"audio": map[string]any{
				"input": map[string]any{"turn_detection": nil},
			},
		},
	})
	confirmed := receiveEvent(t, session)
	require.True(t, confirmed.SessionUpdateAcknowledgement)
	require.Equal(t, realtime.InputTurnModeManual, confirmed.InputTurnMode)

	commands := []realtime.Command{
		{Kind: realtime.CommandAppendAudio, Sequence: 3, Audio: []byte{1, 2}},
		{Kind: realtime.CommandCommitAudio, Sequence: 4, AfterSeq: 3},
		{Kind: realtime.CommandCreateItem, Item: json.RawMessage(`{"type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]}`)},
	}
	for _, command := range commands {
		require.NoError(t, session.Send(t.Context(), command))
	}

	want := []string{
		`{"type":"input_audio_buffer.append","audio":"AQI="}`,
		`{"type":"input_audio_buffer.commit"}`,
		`{"type":"conversation.item.create","item":{"type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]}}`,
	}
	for _, expected := range want {
		assert.JSONEq(t, expected, string(server.nextClientFrame(t)))
	}

	server.sendEvent(t, map[string]any{
		"type":     "response.created",
		"event_id": "created-implicit",
		"response": map[string]any{"id": "implicit", "status": "in_progress"},
	})
	created := receiveEvent(t, session)
	require.True(t, created.NeedsEpochBinding)
	require.NoError(t, session.Send(t.Context(), realtime.Command{
		Kind:       realtime.CommandBindResponseEpoch,
		ResponseID: "implicit",
		Epoch:      identity.ResponseEpoch(41),
	}))
	require.NoError(t, session.Send(t.Context(), realtime.Command{
		Kind:  realtime.CommandCreateResponse,
		Epoch: identity.ResponseEpoch(42),
		Response: realtime.ResponseConfig{
			Instructions: "answer",
			ToolChoice:   json.RawMessage(`"none"`),
			Metadata: map[string]json.RawMessage{
				"caller":     json.RawMessage(`"kept"`),
				"live_epoch": json.RawMessage(`"wrong"`),
			},
			Input: json.RawMessage(`[{"type":"message","role":"user"}]`),
		},
	}))
	require.NoError(t, session.Send(t.Context(), realtime.Command{
		Kind:       realtime.CommandCancelResponse,
		Epoch:      identity.ResponseEpoch(43),
		ResponseID: "response-to-cancel",
	}))

	assert.JSONEq(t,
		`{"type":"response.create","event_id":"live-response-create-42","response":{"instructions":"answer","tool_choice":"none","metadata":{"caller":"kept","live_epoch":"42"},"input":[{"type":"message","role":"user"}]}}`,
		string(server.nextClientFrame(t)),
	)
	assert.JSONEq(t,
		`{"type":"response.cancel","event_id":"live-response-cancel-43","response_id":"response-to-cancel"}`,
		string(server.nextClientFrame(t)),
	)
}

// TestCancelOmitsAnEmptyResponseID catches an optional field encoded as an
// empty provider identifier.
func TestCancelOmitsAnEmptyResponseID(t *testing.T) {
	server := newTestVoiceServer(t)
	session := connectTestSession(t, server.URL, nil, realtime.OpenRequest{Model: "model"})
	require.NoError(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCancelResponse}))
	assert.JSONEq(t, `{"type":"response.cancel"}`, string(server.nextClientFrame(t)))
}

func TestEmptyIDCancelTombstonesExactlyOnePendingCreateAndAllowsNextImplicitResponse(t *testing.T) {
	server := newTestVoiceServer(t)
	session := connectTestSession(t, server.URL, nil, realtime.OpenRequest{Model: "model"})
	configureProviderVADInputTurn(t, server, session)

	require.NoError(t, session.Send(t.Context(), realtime.Command{
		Kind: realtime.CommandCreateResponse, Epoch: 7,
	}))
	server.nextClientFrame(t)
	require.NoError(t, session.Send(t.Context(), realtime.Command{
		Kind: realtime.CommandCancelResponse, Epoch: 8,
	}))
	assert.JSONEq(t, `{"type":"response.cancel","event_id":"live-response-cancel-8"}`, string(server.nextClientFrame(t)))

	server.sendEvent(t, map[string]any{
		"type": "response.created", "event_id": "late-cancelled-created",
		"response": map[string]any{"id": "late-cancelled", "status": "in_progress", "metadata": map[string]any{"live_epoch": "7"}},
	})
	server.sendEvent(t, map[string]any{
		"type": "response.done", "event_id": "late-cancelled-done",
		"response": map[string]any{"id": "late-cancelled", "status": "cancelled"},
	})
	server.sendEvent(t, map[string]any{
		"type": "input_audio_buffer.speech_started", "event_id": "next-speech", "item_id": "next-user",
	})
	next := receiveEvent(t, session)
	assert.Equal(t, realtime.EventSpeechStarted, next.Kind, "late cancelled create/done must be consumed by its tombstone")
	assert.Equal(t, "next-user", next.ItemID)

	server.sendEvent(t, map[string]any{
		"type": "response.created", "event_id": "next-implicit-created",
		"response": map[string]any{"id": "next-implicit", "status": "in_progress"},
	})
	implicit := receiveEvent(t, session)
	assert.Equal(t, realtime.EventResponseCreated, implicit.Kind)
	assert.Equal(t, "next-implicit", implicit.ResponseID)
	assert.True(t, implicit.NeedsEpochBinding, "cancelled pending correlation must not poison the next implicit response")
	require.NoError(t, session.Send(t.Context(), realtime.Command{
		Kind: realtime.CommandBindResponseEpoch, Epoch: 9, ResponseID: "next-implicit",
	}))
}

func TestEmptyIDCancelRefusesAmbiguousPendingCreatesWithoutDeletingEither(t *testing.T) {
	server := newTestVoiceServer(t)
	session := connectTestSession(t, server.URL, nil, realtime.OpenRequest{Model: "model"})
	for _, epoch := range []identity.ResponseEpoch{11, 12} {
		require.NoError(t, session.Send(t.Context(), realtime.Command{
			Kind: realtime.CommandCreateResponse, Epoch: epoch,
		}))
		server.nextClientFrame(t)
	}

	err := session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCancelResponse, Epoch: 13})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exactly one pending response")

	for _, epoch := range []identity.ResponseEpoch{11, 12} {
		created := sendAndReceiveEvent(t, server, session, map[string]any{
			"type": "response.created",
			"response": map[string]any{
				"id": fmt.Sprintf("still-pending-%d", epoch), "status": "in_progress",
				"metadata": map[string]any{"live_epoch": fmt.Sprint(epoch)},
			},
		})
		assert.Equal(t, epoch, created.Epoch)
	}
}

func TestCancelledPendingRejectsMetadataLessCreatedAndTranscriptFirstOutput(t *testing.T) {
	for _, test := range []struct {
		name       string
		tombstones int
		wire       map[string]any
	}{
		{
			name: "one tombstone metadata-less created", tombstones: 1,
			wire: map[string]any{
				"type":     "response.created",
				"response": map[string]any{"id": "ambiguous-created", "status": "in_progress"},
			},
		},
		{
			name: "one tombstone transcript-first output", tombstones: 1,
			wire: map[string]any{
				"type": "response.output_audio_transcript.done", "response_id": "ambiguous-transcript",
				"item_id": "assistant-item", "transcript": "must not become implicit",
			},
		},
		{
			name: "multiple tombstones metadata-less created", tombstones: 2,
			wire: map[string]any{
				"type":     "response.created",
				"response": map[string]any{"id": "ambiguous-multiple", "status": "in_progress"},
			},
		},
		{
			name: "multiple tombstones transcript-first output", tombstones: 2,
			wire: map[string]any{
				"type": "response.output_audio_transcript.done", "response_id": "ambiguous-multiple-transcript",
				"item_id": "assistant-item", "transcript": "must not become implicit",
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := newTestVoiceServer(t)
			session := connectTestSession(t, server.URL, nil, realtime.OpenRequest{Model: "model"})
			for index := 0; index < test.tombstones; index++ {
				createEpoch := identity.ResponseEpoch(20 + index*2)
				require.NoError(t, session.Send(t.Context(), realtime.Command{
					Kind: realtime.CommandCreateResponse, Epoch: createEpoch,
				}))
				server.nextClientFrame(t)
				require.NoError(t, session.Send(t.Context(), realtime.Command{
					Kind: realtime.CommandCancelResponse, Epoch: createEpoch + 1,
				}))
				server.nextClientFrame(t)
			}

			server.sendEvent(t, test.wire)
			_, err := session.Receive(t.Context())
			require.Error(t, err, "ambiguous cancelled output must fail closed instead of consuming implicit budget")
		})
	}
}

// TestCancelAdvancesOnlyTerminalCorrelation proves that the provider's
// response.done acknowledgment crosses the cancel fence while stale media
// remains on the response's pre-cancel epoch for the reactor to suppress.
func TestCancelAdvancesOnlyTerminalCorrelation(t *testing.T) {
	server := newTestVoiceServer(t)
	session := connectTestSession(t, server.URL, nil, realtime.OpenRequest{Model: "model"})

	require.NoError(t, session.Send(t.Context(), realtime.Command{
		Kind: realtime.CommandCreateResponse, Epoch: identity.ResponseEpoch(7),
	}))
	server.nextClientFrame(t)
	created := sendAndReceiveEvent(t, server, session, map[string]any{
		"type": "response.created", "event_id": "cancel-created",
		"response": map[string]any{"id": "cancelled-response", "status": "in_progress", "metadata": map[string]any{"live_epoch": "7"}},
	})
	assert.Equal(t, identity.ResponseEpoch(7), created.Epoch)

	require.NoError(t, session.Send(t.Context(), realtime.Command{
		Kind: realtime.CommandCancelResponse, Epoch: identity.ResponseEpoch(8), ResponseID: "cancelled-response",
	}))
	assert.JSONEq(t,
		`{"type":"response.cancel","event_id":"live-response-cancel-8","response_id":"cancelled-response"}`,
		string(server.nextClientFrame(t)),
	)

	staleAudio := sendAndReceiveEvent(t, server, session, map[string]any{
		"type": "response.output_audio.delta", "event_id": "stale-after-cancel",
		"response_id": "cancelled-response", "item_id": "old-item", "delta": "AAE=",
	})
	assert.Equal(t, realtime.EventAudioDelta, staleAudio.Kind)
	assert.Equal(t, identity.ResponseEpoch(7), staleAudio.Epoch)

	doneWire := map[string]any{
		"type": "response.done", "event_id": "cancel-done",
		"response": map[string]any{"id": "cancelled-response", "status": "cancelled"},
	}
	done := sendAndReceiveEvent(t, server, session, doneWire)
	assert.Equal(t, realtime.EventResponseDone, done.Kind)
	assert.Equal(t, identity.ResponseEpoch(8), done.Epoch)

	doneWire["event_id"] = "cancel-done-duplicate"
	duplicate := sendAndReceiveEvent(t, server, session, doneWire)
	assert.Equal(t, identity.ResponseEpoch(8), duplicate.Epoch)

	errorEvent := sendAndReceiveEvent(t, server, session, map[string]any{
		"type": "error", "event_id": "cancel-error",
		"error": map[string]any{"type": "invalid_request_error", "event_id": "live-response-cancel-8", "message": "private provider detail"},
	})
	assert.Equal(t, realtime.EventError, errorEvent.Kind)
	assert.Equal(t, identity.ResponseEpoch(8), errorEvent.Epoch)
}

func TestProviderDisconnectAfterCancelRemainsTerminal(t *testing.T) {
	server := newTestVoiceServer(t)
	session := connectTestSession(t, server.URL, nil, realtime.OpenRequest{Model: "model"})

	require.NoError(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCreateResponse, Epoch: 11}))
	server.nextClientFrame(t)
	sendAndReceiveEvent(t, server, session, map[string]any{
		"type":     "response.created",
		"response": map[string]any{"id": "disconnect-response", "status": "in_progress", "metadata": map[string]any{"live_epoch": "11"}},
	})
	require.NoError(t, session.Send(t.Context(), realtime.Command{
		Kind: realtime.CommandCancelResponse, Epoch: 12, ResponseID: "disconnect-response",
	}))
	server.nextClientFrame(t)
	server.closePeer(t, websocket.StatusGoingAway, "provider restart")

	_, err := session.Receive(t.Context())
	require.Error(t, err)
	assert.Equal(t, websocket.StatusGoingAway, websocket.CloseStatus(err))
}

func sendAndReceiveEvent(t *testing.T, server *testVoiceServer, session realtime.Session, wire map[string]any) realtime.Event {
	t.Helper()
	raw, err := json.Marshal(wire)
	require.NoError(t, err)
	server.sendRaw(t, raw)
	event := receiveEvent(t, session)
	assert.JSONEq(t, string(raw), string(event.Payload))
	return event
}

// TestReceiveMapsEveryProviderEvent catches schema drift between nested
// response payloads and top-level output identifiers. It also proves that
// arbitrary-sized PCM deltas and JSON-in-a-string arguments survive intact.
func TestReceiveMapsEveryProviderEvent(t *testing.T) {
	server := newTestVoiceServer(t)
	session := connectTestSession(t, server.URL, nil, realtime.OpenRequest{Model: "model"})
	configureProviderVADInputTurn(t, server, session)
	previousItemID := "assistant-or-tool-item"

	tests := []struct {
		wire map[string]any
		want realtime.Event
	}{
		{
			wire: map[string]any{"type": "session.created", "event_id": "evt-session-created", "session": map[string]any{"id": "s1"}},
			want: realtime.Event{Kind: realtime.EventSessionCreated, ProviderEventID: "evt-session-created"},
		},
		{
			wire: map[string]any{"type": "session.updated", "event_id": "evt-session-updated", "session": map[string]any{"id": "s1"}},
			want: realtime.Event{Kind: realtime.EventSessionUpdated, ProviderEventID: "evt-session-updated"},
		},
		{
			wire: map[string]any{"type": "input_audio_buffer.speech_started", "event_id": "evt-start", "item_id": "utterance", "audio_start_ms": 17},
			want: realtime.Event{Kind: realtime.EventSpeechStarted, ProviderEventID: "evt-start", ItemID: "utterance"},
		},
		{
			wire: map[string]any{"type": "input_audio_buffer.committed", "event_id": "evt-committed", "previous_item_id": previousItemID, "item_id": "utterance"},
			want: realtime.Event{Kind: realtime.EventInputAudioCommitted, ProviderEventID: "evt-committed", ItemID: "utterance", PreviousItemID: &previousItemID},
		},
		{
			wire: map[string]any{"type": "input_audio_buffer.committed", "event_id": "evt-committed-null", "previous_item_id": nil, "item_id": "first-utterance"},
			want: realtime.Event{Kind: realtime.EventInputAudioCommitted, ProviderEventID: "evt-committed-null", ItemID: "first-utterance"},
		},
		{
			wire: map[string]any{"type": "input_audio_buffer.speech_stopped", "event_id": "evt-stop", "item_id": "utterance", "audio_end_ms": 29},
			want: realtime.Event{Kind: realtime.EventSpeechStopped, ProviderEventID: "evt-stop", ItemID: "utterance"},
		},
		{
			wire: map[string]any{"type": "conversation.item.input_audio_transcription.delta", "event_id": "evt-input-delta", "item_id": "utterance", "content_index": 0, "delta": "hel"},
			want: realtime.Event{Kind: realtime.EventTranscriptionDelta, ProviderEventID: "evt-input-delta", ItemID: "utterance", Text: "hel"},
		},
		{
			wire: map[string]any{"type": "conversation.item.input_audio_transcription.completed", "event_id": "evt-input-done", "item_id": "utterance", "content_index": 0, "transcript": "hello"},
			want: realtime.Event{Kind: realtime.EventTranscriptionCompleted, ProviderEventID: "evt-input-done", ItemID: "utterance", Text: "hello"},
		},
	}

	for _, test := range tests {
		got := sendAndReceiveEvent(t, server, session, test.wire)
		assert.Equal(t, test.want.Kind, got.Kind)
		assert.Equal(t, test.want.ProviderEventID, got.ProviderEventID)
		assert.Equal(t, test.want.ItemID, got.ItemID)
		assert.Equal(t, test.want.PreviousItemID, got.PreviousItemID)
		assert.Equal(t, test.want.Text, got.Text)
	}

	server.sendEvent(t, map[string]any{
		"type":     "conversation.item.created",
		"event_id": "ignored-item",
		"item":     map[string]any{"id": "item-created", "type": "message"},
	})
	next := sendAndReceiveEvent(t, server, session, map[string]any{
		"type":     "session.updated",
		"event_id": "after-ignored-item",
		"session":  map[string]any{"id": "s1"},
	})
	assert.Equal(t, "after-ignored-item", next.ProviderEventID)

	require.NoError(t, session.Send(t.Context(), realtime.Command{
		Kind:  realtime.CommandCreateResponse,
		Epoch: identity.ResponseEpoch(7),
	}))
	server.nextClientFrame(t)
	created := sendAndReceiveEvent(t, server, session, map[string]any{
		"type":     "response.created",
		"event_id": "evt-created",
		"response": map[string]any{
			"id":       "response-7",
			"status":   "in_progress",
			"metadata": map[string]any{"live_epoch": "7"},
		},
	})
	assert.Equal(t, realtime.EventResponseCreated, created.Kind)
	assert.Equal(t, identity.ResponseEpoch(7), created.Epoch)
	assert.Equal(t, "response-7", created.ResponseID)
	assert.Equal(t, "in_progress", created.Status)
	assert.False(t, created.NeedsEpochBinding)

	pcm := []byte{0, 1, 2, 3, 4, 5}
	responseTests := []struct {
		wire map[string]any
		want realtime.Event
	}{
		{
			wire: map[string]any{"type": "response.output_audio.delta", "event_id": "evt-audio", "response_id": "response-7", "item_id": "assistant-item", "delta": base64.StdEncoding.EncodeToString(pcm)},
			want: realtime.Event{Kind: realtime.EventAudioDelta, ProviderEventID: "evt-audio", ItemID: "assistant-item", Audio: pcm},
		},
		{
			wire: map[string]any{"type": "response.output_audio.done", "event_id": "evt-audio-done", "response_id": "response-7", "item_id": "assistant-item"},
			want: realtime.Event{Kind: realtime.EventAudioDone, ProviderEventID: "evt-audio-done", ItemID: "assistant-item"},
		},
		{
			wire: map[string]any{"type": "response.output_audio_transcript.delta", "event_id": "evt-transcript", "response_id": "response-7", "item_id": "assistant-item", "delta": "wor"},
			want: realtime.Event{Kind: realtime.EventTranscriptDelta, ProviderEventID: "evt-transcript", ItemID: "assistant-item", Text: "wor"},
		},
		{
			wire: map[string]any{"type": "response.output_audio_transcript.done", "event_id": "evt-transcript-done", "response_id": "response-7", "item_id": "assistant-item", "transcript": "world"},
			want: realtime.Event{Kind: realtime.EventTranscriptDone, ProviderEventID: "evt-transcript-done", ItemID: "assistant-item", Text: "world"},
		},
		{
			wire: map[string]any{"type": "response.function_call_arguments.done", "event_id": "evt-tool", "response_id": "response-7", "item_id": "tool-item", "call_id": "call-1", "name": "weather", "arguments": `{"city":"Paris"}`},
			want: realtime.Event{Kind: realtime.EventFunctionCallDone, ProviderEventID: "evt-tool", ItemID: "tool-item", CallID: "call-1", Name: "weather", Arguments: json.RawMessage(`{"city":"Paris"}`)},
		},
	}
	for _, test := range responseTests {
		got := sendAndReceiveEvent(t, server, session, test.wire)
		assert.Equal(t, test.want.Kind, got.Kind)
		assert.Equal(t, identity.ResponseEpoch(7), got.Epoch)
		assert.Equal(t, "response-7", got.ResponseID)
		assert.Equal(t, test.want.ProviderEventID, got.ProviderEventID)
		assert.Equal(t, test.want.ItemID, got.ItemID)
		assert.Equal(t, test.want.Text, got.Text)
		assert.Equal(t, test.want.CallID, got.CallID)
		assert.Equal(t, test.want.Name, got.Name)
		assert.Equal(t, []byte(test.want.Audio), got.Audio)
		if test.want.Arguments != nil {
			assert.JSONEq(t, string(test.want.Arguments), string(got.Arguments))
		}
	}

	doneWire := map[string]any{
		"type":     "response.done",
		"event_id": "evt-done-1",
		"response": map[string]any{
			"id":             "response-7",
			"status":         "failed",
			"status_details": map[string]any{"reason": "pool_exhausted"},
		},
	}
	done := sendAndReceiveEvent(t, server, session, doneWire)
	assert.Equal(t, realtime.EventResponseDone, done.Kind)
	assert.Equal(t, identity.ResponseEpoch(7), done.Epoch)
	assert.Equal(t, "failed", done.Status)
	assert.Equal(t, "pool_exhausted", done.Reason)

	doneWire["event_id"] = "evt-done-duplicate"
	duplicate := sendAndReceiveEvent(t, server, session, doneWire)
	assert.Equal(t, identity.ResponseEpoch(7), duplicate.Epoch)
	assert.Equal(t, "evt-done-duplicate", duplicate.ProviderEventID)

	require.NoError(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCreateResponse, Epoch: identity.ResponseEpoch(8)}))
	server.nextClientFrame(t)
	newer := sendAndReceiveEvent(t, server, session, map[string]any{
		"type":     "response.created",
		"event_id": "evt-created-newer",
		"response": map[string]any{"id": "response-8", "status": "in_progress", "metadata": map[string]any{"live_epoch": "8"}},
	})
	assert.Equal(t, identity.ResponseEpoch(8), newer.Epoch)

	doneWire["event_id"] = "evt-done-late-old"
	lateOld := sendAndReceiveEvent(t, server, session, doneWire)
	assert.Equal(t, identity.ResponseEpoch(7), lateOld.Epoch)

	providerError := sendAndReceiveEvent(t, server, session, map[string]any{
		"type":     "error",
		"event_id": "evt-error",
		"error": map[string]any{
			"type":    "server_error",
			"code":    "pool_exhausted",
			"message": "pool exhausted",
			"details": map[string]any{"available": 0},
		},
	})
	assert.Equal(t, realtime.EventError, providerError.Kind)
	assert.Equal(t, "pool exhausted", providerError.Reason)
	assert.Contains(t, string(providerError.Payload), `"available":0`)
}

func TestPresentationProviderErrorsRetainCreateEpochBeforeDuringAndAfterStreaming(t *testing.T) {
	server := newTestVoiceServer(t)
	session := connectTestSession(t, server.URL, nil, realtime.OpenRequest{Model: "model"})

	for _, epoch := range []identity.ResponseEpoch{21, 22} {
		require.NoError(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCreateResponse, Epoch: epoch}))
		var outbound map[string]any
		require.NoError(t, json.Unmarshal(server.nextClientFrame(t), &outbound))
		createEventID, ok := outbound["event_id"].(string)
		require.True(t, ok)
		assert.Equal(t, fmt.Sprintf("live-response-create-%d", epoch), createEventID)
		if epoch == 22 {
			created := sendAndReceiveEvent(t, server, session, map[string]any{
				"type": "response.created", "event_id": "created-22",
				"response": map[string]any{"id": "response-22", "status": "in_progress", "metadata": map[string]any{"live_epoch": "22"}},
			})
			assert.Equal(t, epoch, created.Epoch)
		}
		errorEvent := sendAndReceiveEvent(t, server, session, map[string]any{
			"type": "error", "event_id": fmt.Sprintf("error-%d", epoch),
			"error": map[string]any{"type": "invalid_request_error", "event_id": createEventID, "message": "private provider detail"},
		})
		assert.Equal(t, realtime.EventError, errorEvent.Kind)
		assert.Equal(t, epoch, errorEvent.Epoch)
	}

	late := sendAndReceiveEvent(t, server, session, map[string]any{
		"type": "error", "event_id": "late-error-22",
		"error": map[string]any{"type": "invalid_request_error", "event_id": "live-response-create-22", "message": "late private detail"},
	})
	assert.Equal(t, identity.ResponseEpoch(22), late.Epoch)
}

// TestImplicitResponseWaitsForReactorEpochBinding catches output leaking with
// epoch zero. A later session event fences the reader and proves the response
// output was processed but withheld before the bind.
func TestImplicitResponseWaitsForReactorEpochBinding(t *testing.T) {
	server := newTestVoiceServer(t)
	session := connectTestSession(t, server.URL, nil, realtime.OpenRequest{Model: "model"})
	server.sendEvent(t, map[string]any{
		"type":     "response.created",
		"event_id": "implicit-created",
		"response": map[string]any{"id": "implicit", "status": "in_progress"},
	})
	created := receiveEvent(t, session)
	assert.Equal(t, realtime.EventResponseCreated, created.Kind)
	assert.Zero(t, created.Epoch)
	assert.True(t, created.NeedsEpochBinding)

	pcm := []byte{3, 1, 4, 1, 5, 9}
	server.sendEvent(t, map[string]any{
		"type":        "response.output_audio.delta",
		"event_id":    "implicit-audio",
		"response_id": "implicit",
		"item_id":     "assistant-item",
		"delta":       base64.StdEncoding.EncodeToString(pcm),
	})
	server.sendEvent(t, map[string]any{
		"type":     "session.updated",
		"event_id": "reader-fence",
		"session":  map[string]any{"id": "session"},
	})
	fence := receiveEvent(t, session)
	assert.Equal(t, realtime.EventSessionUpdated, fence.Kind)
	assert.Equal(t, "reader-fence", fence.ProviderEventID)

	require.NoError(t, session.Send(t.Context(), realtime.Command{
		Kind:       realtime.CommandBindResponseEpoch,
		ResponseID: "implicit",
		Epoch:      identity.ResponseEpoch(91),
	}))
	audio := receiveEvent(t, session)
	assert.Equal(t, realtime.EventAudioDelta, audio.Kind)
	assert.Equal(t, identity.ResponseEpoch(91), audio.Epoch)
	assert.Equal(t, pcm, audio.Audio)
}

// TestImplicitResponseCanStartWithResponseScopedOutput covers providers whose
// server-VAD path omits response.created and starts with a terminal transcript.
// The adapter must establish one implicit response before releasing output so
// the reactor, downstream client, and stale-output fence all share an epoch.
func TestImplicitResponseCanStartWithResponseScopedOutput(t *testing.T) {
	server := newTestVoiceServer(t)
	session := connectTestSession(t, server.URL, nil, realtime.OpenRequest{Model: "model"})

	server.sendEvent(t, map[string]any{
		"type":        "response.output_audio_transcript.done",
		"event_id":    "provider-transcript-chunk",
		"response_id": "implicit-without-created",
		"item_id":     "assistant-item",
		"transcript":  "bounded assistant text",
	})
	created := receiveEvent(t, session)
	assert.Equal(t, realtime.EventResponseCreated, created.Kind)
	assert.Equal(t, "implicit-without-created", created.ResponseID)
	assert.Equal(t, "in_progress", created.Status)
	assert.True(t, created.NeedsEpochBinding)

	require.NoError(t, session.Send(t.Context(), realtime.Command{
		Kind:       realtime.CommandBindResponseEpoch,
		ResponseID: "implicit-without-created",
		Epoch:      identity.ResponseEpoch(92),
	}))
	transcript := receiveEvent(t, session)
	assert.Equal(t, realtime.EventTranscriptDone, transcript.Kind)
	assert.Equal(t, identity.ResponseEpoch(92), transcript.Epoch)
	assert.Equal(t, "provider-transcript-chunk", transcript.ProviderEventID)
}

// TestImplicitResponseBufferCountBoundary catches off-by-one logic around the
// exact sixteen-event allowance and proves FIFO release.
func TestImplicitResponseBufferCountBoundary(t *testing.T) {
	t.Run("sixteen events are released FIFO", func(t *testing.T) {
		server := newTestVoiceServer(t)
		session := connectTestSession(t, server.URL, nil, realtime.OpenRequest{Model: "model"})
		server.sendEvent(t, map[string]any{"type": "response.created", "response": map[string]any{"id": "count-ok", "status": "in_progress"}})
		receiveEvent(t, session)
		for index := range 16 {
			server.sendEvent(t, map[string]any{
				"type":        "response.output_audio_transcript.delta",
				"event_id":    fmt.Sprintf("buffered-%02d", index),
				"response_id": "count-ok",
				"item_id":     "item",
				"delta":       fmt.Sprintf("text-%02d", index),
			})
		}
		server.sendEvent(t, map[string]any{"type": "session.updated", "event_id": "count-fence", "session": map[string]any{}})
		assert.Equal(t, "count-fence", receiveEvent(t, session).ProviderEventID)
		require.NoError(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandBindResponseEpoch, ResponseID: "count-ok", Epoch: 101}))
		for index := range 16 {
			event := receiveEvent(t, session)
			assert.Equal(t, fmt.Sprintf("buffered-%02d", index), event.ProviderEventID)
			assert.Equal(t, fmt.Sprintf("text-%02d", index), event.Text)
			assert.Equal(t, identity.ResponseEpoch(101), event.Epoch)
		}
	})

	t.Run("seventeenth event fails", func(t *testing.T) {
		server := newTestVoiceServer(t)
		session := connectTestSession(t, server.URL, nil, realtime.OpenRequest{Model: "model"})
		server.sendEvent(t, map[string]any{"type": "response.created", "response": map[string]any{"id": "count-overflow", "status": "in_progress"}})
		receiveEvent(t, session)
		for index := range 17 {
			server.sendEvent(t, map[string]any{
				"type":        "response.output_audio_transcript.delta",
				"event_id":    fmt.Sprintf("overflow-%02d", index),
				"response_id": "count-overflow",
				"item_id":     "item",
				"delta":       "x",
			})
		}
		_, err := session.Receive(t.Context())
		require.ErrorIs(t, err, s2sws.ErrImplicitBufferFull)
	})
}

func sizedTranscriptFrame(t *testing.T, responseID string, targetBytes int) []byte {
	t.Helper()
	prefix := fmt.Sprintf(`{"type":"response.output_audio_transcript.delta","event_id":"sized","response_id":%q,"item_id":"item","delta":"`, responseID)
	suffix := `"}`
	require.GreaterOrEqual(t, targetBytes, len(prefix)+len(suffix))
	return []byte(prefix + strings.Repeat("x", targetBytes-len(prefix)-len(suffix)) + suffix)
}

// TestImplicitResponseBufferByteBoundary catches implementations that use a
// decoded field estimate instead of the exact bounded provider frame size.
func TestImplicitResponseBufferByteBoundary(t *testing.T) {
	const limit = 2 << 20
	t.Run("exactly two MiB is accepted", func(t *testing.T) {
		server := newTestVoiceServer(t)
		session := connectTestSession(t, server.URL, func(cfg *s2sws.Config) { cfg.MaxFrameBytes = limit + 4096 }, realtime.OpenRequest{Model: "model"})
		server.sendEvent(t, map[string]any{"type": "response.created", "response": map[string]any{"id": "bytes-ok", "status": "in_progress"}})
		receiveEvent(t, session)
		server.sendRaw(t, sizedTranscriptFrame(t, "bytes-ok", limit))
		server.sendEvent(t, map[string]any{"type": "session.updated", "event_id": "bytes-fence", "session": map[string]any{}})
		assert.Equal(t, "bytes-fence", receiveEvent(t, session).ProviderEventID)
		require.NoError(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandBindResponseEpoch, ResponseID: "bytes-ok", Epoch: 111}))
		buffered := receiveEvent(t, session)
		assert.Equal(t, identity.ResponseEpoch(111), buffered.Epoch)
		assert.Len(t, buffered.Payload, limit)
	})

	t.Run("two MiB plus one fails", func(t *testing.T) {
		server := newTestVoiceServer(t)
		session := connectTestSession(t, server.URL, func(cfg *s2sws.Config) { cfg.MaxFrameBytes = limit + 4096 }, realtime.OpenRequest{Model: "model"})
		server.sendEvent(t, map[string]any{"type": "response.created", "response": map[string]any{"id": "bytes-overflow", "status": "in_progress"}})
		receiveEvent(t, session)
		server.sendRaw(t, sizedTranscriptFrame(t, "bytes-overflow", limit+1))
		_, err := session.Receive(t.Context())
		require.ErrorIs(t, err, s2sws.ErrImplicitBufferFull)
	})
}

func TestImplicitResponsesEnforceAggregateOwnedByteBudgetAcrossIDs(t *testing.T) {
	const frameBytes = 1536 << 10
	server := newTestVoiceServer(t)
	session := connectTestSession(t, server.URL, func(cfg *s2sws.Config) {
		cfg.MaxFrameBytes = 2 << 20
	}, realtime.OpenRequest{Model: "model"})
	for index := range 2 {
		responseID := fmt.Sprintf("aggregate-%d", index)
		server.sendEvent(t, map[string]any{"type": "response.created", "response": map[string]any{"id": responseID, "status": "in_progress"}})
		receiveEvent(t, session)
		server.sendRaw(t, sizedTranscriptFrame(t, responseID, frameBytes))
	}
	server.sendEvent(t, map[string]any{"type": "response.created", "response": map[string]any{"id": "aggregate-overflow", "status": "in_progress"}})
	receiveEvent(t, session)
	server.sendRaw(t, sizedTranscriptFrame(t, "aggregate-overflow", frameBytes))
	_, err := session.Receive(t.Context())
	require.ErrorIs(t, err, s2sws.ErrImplicitAggregateFull)
}

func TestImplicitResponseCountIsBoundedBeforeEpochBinding(t *testing.T) {
	server := newTestVoiceServer(t)
	session := connectTestSession(t, server.URL, nil, realtime.OpenRequest{Model: "model"})
	for index := range 8 {
		server.sendEvent(t, map[string]any{"type": "response.created", "response": map[string]any{
			"id": fmt.Sprintf("unbound-%d", index), "status": "in_progress",
		}})
		receiveEvent(t, session)
	}
	server.sendEvent(t, map[string]any{"type": "response.created", "response": map[string]any{"id": "unbound-overflow", "status": "in_progress"}})
	_, err := session.Receive(t.Context())
	require.ErrorIs(t, err, s2sws.ErrImplicitResponseLimit)
}

func TestBindingImplicitResponseReleasesAggregateBudget(t *testing.T) {
	const frameBytes = 1536 << 10
	server := newTestVoiceServer(t)
	session := connectTestSession(t, server.URL, func(cfg *s2sws.Config) {
		cfg.MaxFrameBytes = 2 << 20
	}, realtime.OpenRequest{Model: "model"})
	for _, responseID := range []string{"release-a", "release-b"} {
		server.sendEvent(t, map[string]any{"type": "response.created", "response": map[string]any{"id": responseID, "status": "in_progress"}})
		receiveEvent(t, session)
		server.sendRaw(t, sizedTranscriptFrame(t, responseID, frameBytes))
	}
	require.NoError(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandBindResponseEpoch, ResponseID: "release-a", Epoch: 301}))
	require.Equal(t, "release-a", receiveEvent(t, session).ResponseID)

	server.sendEvent(t, map[string]any{"type": "response.created", "response": map[string]any{"id": "release-c", "status": "in_progress"}})
	receiveEvent(t, session)
	server.sendRaw(t, sizedTranscriptFrame(t, "release-c", frameBytes))
	require.NoError(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandBindResponseEpoch, ResponseID: "release-c", Epoch: 302}))
	require.Equal(t, "release-c", receiveEvent(t, session).ResponseID)
}

// TestBindResponseEpochRacesReader catches bind/read interleavings that lose,
// duplicate, or stamp output with epoch zero.
func TestBindResponseEpochRacesReader(t *testing.T) {
	server := newTestVoiceServer(t)
	session := connectTestSession(t, server.URL, nil, realtime.OpenRequest{Model: "model"})
	for index := range 64 {
		responseID := fmt.Sprintf("race-%02d", index)
		epoch := identity.ResponseEpoch(index + 1)
		server.sendEvent(t, map[string]any{"type": "response.created", "response": map[string]any{"id": responseID, "status": "in_progress"}})
		receiveEvent(t, session)

		raw, err := json.Marshal(map[string]any{
			"type":        "response.output_audio.delta",
			"event_id":    fmt.Sprintf("race-event-%02d", index),
			"response_id": responseID,
			"item_id":     "item",
			"delta":       "AAE=",
		})
		require.NoError(t, err)
		start := make(chan struct{})
		bindResult := make(chan error, 1)
		writeResult := make(chan error, 1)
		go func() {
			<-start
			bindResult <- session.Send(t.Context(), realtime.Command{Kind: realtime.CommandBindResponseEpoch, ResponseID: responseID, Epoch: epoch})
		}()
		go func() {
			<-start
			ack := make(chan error, 1)
			server.actions <- serverAction{payload: raw, ack: ack}
			writeResult <- <-ack
		}()
		close(start)
		require.NoError(t, <-bindResult)
		require.NoError(t, <-writeResult)
		event := receiveEvent(t, session)
		assert.Equal(t, fmt.Sprintf("race-event-%02d", index), event.ProviderEventID)
		assert.Equal(t, epoch, event.Epoch)
	}
}

// TestResponseSpecificEventsRequireCreated catches simulator deviations that
// silently synthesize response correlation for transcript/tool/terminal data.
func TestResponseSpecificEventsRequireCreated(t *testing.T) {
	tests := []struct {
		name string
		wire map[string]any
	}{
		{
			name: "audio",
			wire: map[string]any{"type": "response.output_audio.delta", "response_id": "missing", "item_id": "item", "delta": "AAE="},
		},
		{
			name: "transcript",
			wire: map[string]any{"type": "response.output_audio_transcript.delta", "response_id": "missing", "item_id": "item", "delta": "x"},
		},
		{
			name: "tool",
			wire: map[string]any{"type": "response.function_call_arguments.done", "response_id": "missing", "item_id": "item", "call_id": "call", "name": "tool", "arguments": `{}`},
		},
		{
			name: "terminal",
			wire: map[string]any{"type": "response.done", "response": map[string]any{"id": "missing", "status": "completed"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := newTestVoiceServer(t)
			session := connectTestSession(t, server.URL, nil, realtime.OpenRequest{Model: "model"})
			server.sendEvent(t, test.wire)
			_, err := session.Receive(t.Context())
			require.ErrorIs(t, err, s2sws.ErrUnknownResponseEpoch)
		})
	}
}

// TestExplicitResponseRequiresMatchingStringEpoch catches implicit fallback
// when an explicit create is awaiting a strict echoed metadata binding.
func TestExplicitResponseRequiresMatchingStringEpoch(t *testing.T) {
	tests := []struct {
		name     string
		metadata any
	}{
		{name: "missing metadata"},
		{name: "numeric metadata", metadata: map[string]any{"live_epoch": 17}},
		{name: "malformed decimal", metadata: map[string]any{"live_epoch": "not-a-number"}},
		{name: "non-canonical decimal", metadata: map[string]any{"live_epoch": "017"}},
		{name: "mismatched epoch", metadata: map[string]any{"live_epoch": "18"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := newTestVoiceServer(t)
			session := connectTestSession(t, server.URL, nil, realtime.OpenRequest{Model: "model"})
			require.NoError(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCreateResponse, Epoch: 17}))
			server.nextClientFrame(t)
			response := map[string]any{"id": "explicit", "status": "in_progress"}
			if test.metadata != nil {
				response["metadata"] = test.metadata
			}
			server.sendEvent(t, map[string]any{"type": "response.created", "response": response})
			_, err := session.Receive(t.Context())
			require.Error(t, err)
		})
	}
}

// TestExplicitPendingResponseLimit catches a withholding provider growing the
// pending response.create correlation set without bound.
func TestExplicitPendingResponseLimit(t *testing.T) {
	const limit = 64

	fill := func(t *testing.T, session realtime.Session, server *testVoiceServer) {
		t.Helper()
		for value := 1; value <= limit; value++ {
			require.NoError(t, session.Send(t.Context(), realtime.Command{
				Kind:  realtime.CommandCreateResponse,
				Epoch: identity.ResponseEpoch(value),
			}))
		}
		for value := 1; value <= limit; value++ {
			var frame struct {
				Type     string `json:"type"`
				Response struct {
					Metadata map[string]string `json:"metadata"`
				} `json:"response"`
			}
			require.NoError(t, json.Unmarshal(server.nextClientFrame(t), &frame))
			assert.Equal(t, "response.create", frame.Type)
			assert.Equal(t, fmt.Sprintf("%d", value), frame.Response.Metadata["live_epoch"])
		}
	}

	t.Run("accepts exact limit", func(t *testing.T) {
		server := newTestVoiceServer(t)
		session := connectTestSession(t, server.URL, nil, realtime.OpenRequest{Model: "model"})
		fill(t, session, server)
	})

	t.Run("rejects first over limit without a frame", func(t *testing.T) {
		server := newTestVoiceServer(t)
		session := connectTestSession(t, server.URL, nil, realtime.OpenRequest{Model: "model"})
		fill(t, session, server)

		err := session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCreateResponse, Epoch: limit + 1})
		require.ErrorIs(t, err, s2sws.ErrPendingResponseLimit)
		require.NoError(t, session.Send(t.Context(), realtime.Command{
			Kind: realtime.CommandCreateItem,
			Item: json.RawMessage(`{"type":"message"}`),
		}))
		assert.JSONEq(t, `{"type":"conversation.item.create","item":{"type":"message"}}`, string(server.nextClientFrame(t)))
	})

	t.Run("matching response created restores capacity", func(t *testing.T) {
		server := newTestVoiceServer(t)
		session := connectTestSession(t, server.URL, nil, realtime.OpenRequest{Model: "model"})
		fill(t, session, server)

		err := session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCreateResponse, Epoch: limit + 1})
		require.ErrorIs(t, err, s2sws.ErrPendingResponseLimit)
		server.sendEvent(t, map[string]any{
			"type": "response.created",
			"response": map[string]any{
				"id":       "response-1",
				"status":   "in_progress",
				"metadata": map[string]any{"live_epoch": "1"},
			},
		})
		created := receiveEvent(t, session)
		assert.Equal(t, identity.ResponseEpoch(1), created.Epoch)

		require.NoError(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCreateResponse, Epoch: limit + 1}))
		var retried struct {
			Response struct {
				Metadata map[string]string `json:"metadata"`
			} `json:"response"`
		}
		require.NoError(t, json.Unmarshal(server.nextClientFrame(t), &retried))
		assert.Equal(t, "65", retried.Response.Metadata["live_epoch"])
	})
}

// TestExplicitCreateRegistersBeforeProviderEcho catches the write/register
// race by having the provider echo response.created as soon as each create
// frame is read.
func TestExplicitCreateRegistersBeforeProviderEcho(t *testing.T) {
	server := newTestVoiceServer(t)
	session := connectTestSession(t, server.URL, nil, realtime.OpenRequest{Model: "model"})
	const responses = 128
	echoResult := make(chan error, 1)
	go func() {
		for index := 1; index <= responses; index++ {
			raw := <-server.received
			var create struct {
				Type     string `json:"type"`
				Response struct {
					Metadata map[string]string `json:"metadata"`
				} `json:"response"`
			}
			if err := json.Unmarshal(raw, &create); err != nil {
				echoResult <- err
				return
			}
			if create.Type != "response.create" {
				echoResult <- fmt.Errorf("frame type = %q", create.Type)
				return
			}
			epoch := create.Response.Metadata["live_epoch"]
			response, err := json.Marshal(map[string]any{
				"type":     "response.created",
				"event_id": fmt.Sprintf("created-%03d", index),
				"response": map[string]any{
					"id":       fmt.Sprintf("response-%03d", index),
					"status":   "in_progress",
					"metadata": map[string]any{"live_epoch": epoch},
				},
			})
			if err != nil {
				echoResult <- err
				return
			}
			ack := make(chan error, 1)
			server.actions <- serverAction{payload: response, ack: ack}
			if err := <-ack; err != nil {
				echoResult <- err
				return
			}
		}
		echoResult <- nil
	}()

	for index := 1; index <= responses; index++ {
		epoch := identity.ResponseEpoch(index)
		require.NoError(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCreateResponse, Epoch: epoch}))
		created := receiveEvent(t, session)
		assert.Equal(t, realtime.EventResponseCreated, created.Kind)
		assert.Equal(t, epoch, created.Epoch)
		assert.Equal(t, fmt.Sprintf("response-%03d", index), created.ResponseID)
		assert.False(t, created.NeedsEpochBinding)
	}
	require.NoError(t, <-echoResult)
}

// TestConcurrentSendSerializesFrames catches concurrent caller writes that
// corrupt or merge WebSocket messages.
func TestConcurrentSendSerializesFrames(t *testing.T) {
	server := newTestVoiceServer(t)
	session := connectTestSession(t, server.URL, nil, realtime.OpenRequest{Model: "model"})
	configureManualInputTurn(t, server, session)
	const sends = 128
	start := make(chan struct{})
	results := make(chan error, sends)
	var workers sync.WaitGroup
	for index := range sends {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			results <- session.Send(t.Context(), realtime.Command{
				Kind:  realtime.CommandAppendAudio,
				Audio: []byte{byte(index), byte(index >> 8)},
			})
		}()
	}
	close(start)

	seen := make(map[string]int, sends)
	for range sends {
		raw := server.nextClientFrame(t)
		var frame struct {
			Type  string `json:"type"`
			Audio string `json:"audio"`
		}
		require.NoError(t, json.Unmarshal(raw, &frame))
		assert.Equal(t, "input_audio_buffer.append", frame.Type)
		seen[frame.Audio]++
	}
	workers.Wait()
	close(results)
	for err := range results {
		require.NoError(t, err)
	}
	for index := range sends {
		encoded := base64.StdEncoding.EncodeToString([]byte{byte(index), byte(index >> 8)})
		assert.Equal(t, 1, seen[encoded], "audio frame %d", index)
	}
}

// TestSendOwnsCommandBytesOnReturn catches asynchronous writers retaining a
// caller-owned audio slice after Send has accepted it.
func TestSendOwnsCommandBytesOnReturn(t *testing.T) {
	server := newTestVoiceServer(t)
	session := connectTestSession(t, server.URL, nil, realtime.OpenRequest{Model: "model"})
	configureManualInputTurn(t, server, session)
	audio := []byte{1, 2, 3, 4}
	require.NoError(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandAppendAudio, Audio: audio}))
	for index := range audio {
		audio[index] = 9
	}
	assert.JSONEq(t, `{"type":"input_audio_buffer.append","audio":"AQIDBA=="}`, string(server.nextClientFrame(t)))
}

// TestSendRejectsMalformedOrOversizedCommands catches invalid raw insertion,
// non-string metadata, incompatible PCM declarations, and oversized frames.
func TestSendRejectsMalformedOrOversizedCommands(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*s2sws.Config)
		command realtime.Command
	}{
		{name: "empty audio", command: realtime.Command{Kind: realtime.CommandAppendAudio}},
		{name: "malformed item", command: realtime.Command{Kind: realtime.CommandCreateItem, Item: json.RawMessage(`{"type":`)}},
		{name: "non-object item", command: realtime.Command{Kind: realtime.CommandCreateItem, Item: json.RawMessage(`[]`)}},
		{name: "malformed tool parameters", command: realtime.Command{Kind: realtime.CommandSessionUpdate, Session: realtime.SessionConfig{Tools: []realtime.Tool{{Type: "function", Name: "x", Parameters: json.RawMessage(`{"type":`)}}}}},
		{name: "incompatible input audio", command: realtime.Command{Kind: realtime.CommandSessionUpdate, Session: realtime.SessionConfig{InputFormat: realtime.AudioFormat{Encoding: "pcm16le", SampleRate: 16_000, Channels: 1}}}},
		{name: "malformed response input", command: realtime.Command{Kind: realtime.CommandCreateResponse, Epoch: 1, Response: realtime.ResponseConfig{Input: json.RawMessage(`[`)}}},
		{name: "non-string metadata", command: realtime.Command{Kind: realtime.CommandCreateResponse, Epoch: 1, Response: realtime.ResponseConfig{Metadata: map[string]json.RawMessage{"caller": json.RawMessage(`7`)}}}},
		{name: "oversized frame", mutate: func(cfg *s2sws.Config) { cfg.MaxFrameBytes = 64 }, command: realtime.Command{Kind: realtime.CommandCreateItem, Item: json.RawMessage(`{"type":"message","content":"this payload deliberately exceeds the configured sixty-four-byte frame bound"}`)}},
		{name: "unknown command", command: realtime.Command{Kind: realtime.CommandKind(255)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := newTestVoiceServer(t)
			session := connectTestSession(t, server.URL, test.mutate, realtime.OpenRequest{Model: "model"})
			require.Error(t, session.Send(t.Context(), test.command))
			require.NoError(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCancelResponse}))
			assert.JSONEq(t, `{"type":"response.cancel"}`, string(server.nextClientFrame(t)))
		})
	}
}

// TestReceiveRejectsMalformedProviderData catches bad JSON, non-canonical
// base64, JSON-in-string argument corruption, and transport frame overflow.
func TestReceiveRejectsMalformedProviderData(t *testing.T) {
	tests := []struct {
		name     string
		maxFrame int64
		raw      []byte
	}{
		{name: "malformed JSON", raw: []byte(`{"type":`)},
		{name: "invalid UTF-8", raw: []byte{'{', '"', 't', 'y', 'p', 'e', '"', ':', '"', 0xff, '"', '}'}},
		{name: "invalid base64", raw: []byte(`{"type":"response.output_audio.delta","response_id":"missing","delta":"!!!="}`)},
		{name: "unpadded base64", raw: []byte(`{"type":"response.output_audio.delta","response_id":"missing","delta":"AAE"}`)},
		{name: "non-canonical base64", raw: []byte(`{"type":"response.output_audio.delta","response_id":"missing","delta":"AB=="}`)},
		{name: "malformed function arguments", raw: []byte(`{"type":"response.function_call_arguments.done","response_id":"missing","arguments":"{"}`)},
		{name: "committed event missing previous item", raw: []byte(`{"type":"input_audio_buffer.committed","event_id":"missing-previous","item_id":"input"}`)},
		{name: "oversized frame", maxFrame: 128, raw: bytes.Repeat([]byte("x"), 129)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := newTestVoiceServer(t)
			session := connectTestSession(t, server.URL, func(cfg *s2sws.Config) {
				if test.maxFrame != 0 {
					cfg.MaxFrameBytes = test.maxFrame
				}
			}, realtime.OpenRequest{Model: "model"})
			server.sendRaw(t, test.raw)
			_, err := session.Receive(t.Context())
			require.Error(t, err)
			assert.False(t, errors.Is(err, s2sws.ErrUnknownResponseEpoch), "invalid data must fail before correlation")
		})
	}
}

// TestCanceledOperationsDoNotPoisonSession catches direct socket reads/writes
// using per-call contexts instead of the adapter's internal loops.
func TestCanceledOperationsDoNotPoisonSession(t *testing.T) {
	server := newTestVoiceServer(t)
	session := connectTestSession(t, server.URL, nil, realtime.OpenRequest{Model: "model"})
	canceled, cancel := context.WithCancel(t.Context())
	cancel()

	require.ErrorIs(t, session.Send(canceled, realtime.Command{Kind: realtime.CommandCommitAudio}), context.Canceled)
	_, err := session.Receive(canceled)
	require.ErrorIs(t, err, context.Canceled)

	require.NoError(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCancelResponse}))
	assert.JSONEq(t, `{"type":"response.cancel"}`, string(server.nextClientFrame(t)))
	server.sendEvent(t, map[string]any{"type": "session.created", "event_id": "still-open", "session": map[string]any{}})
	assert.Equal(t, "still-open", receiveEvent(t, session).ProviderEventID)
}

// TestProviderPingKeepsSessionUsable catches a read loop that fails to service
// control frames while waiting for provider events.
func TestProviderPingKeepsSessionUsable(t *testing.T) {
	server := newTestVoiceServer(t)
	session := connectTestSession(t, server.URL, nil, realtime.OpenRequest{Model: "model"})
	server.ping(t)
	server.sendEvent(t, map[string]any{"type": "session.created", "event_id": "after-ping", "session": map[string]any{}})
	assert.Equal(t, "after-ping", receiveEvent(t, session).ProviderEventID)
}

// TestProviderErrorPrecedesPolicyClose catches terminal signaling that wins a
// race with the provider's final structured error event.
func TestProviderErrorPrecedesPolicyClose(t *testing.T) {
	server := newTestVoiceServer(t)
	session := connectTestSession(t, server.URL, nil, realtime.OpenRequest{Model: "model"})
	server.sendEvent(t, map[string]any{
		"type":     "error",
		"event_id": "pool-error",
		"error":    map[string]any{"message": "pool exhausted", "code": "pool_exhausted"},
	})
	server.closePeer(t, websocket.StatusPolicyViolation, "pool exhausted")

	event := receiveEvent(t, session)
	assert.Equal(t, realtime.EventError, event.Kind)
	assert.Equal(t, "pool-error", event.ProviderEventID)
	assert.Equal(t, "pool exhausted", event.Reason)
	_, err := session.Receive(t.Context())
	require.Error(t, err)
	assert.Equal(t, websocket.StatusPolicyViolation, websocket.CloseStatus(err))
	_, repeated := session.Receive(t.Context())
	require.EqualError(t, repeated, err.Error())
}

// TestPeerCloseUnblocksOperations catches loops stranded after the remote end
// completes a close handshake.
func TestPeerCloseUnblocksOperations(t *testing.T) {
	server := newTestVoiceServer(t)
	session := connectTestSession(t, server.URL, nil, realtime.OpenRequest{Model: "model"})
	receiveResult := make(chan error, 1)
	go func() {
		_, err := session.Receive(t.Context())
		receiveResult <- err
	}()
	server.closePeer(t, websocket.StatusGoingAway, "shutdown")
	err := <-receiveResult
	require.Error(t, err)
	assert.Equal(t, websocket.StatusGoingAway, websocket.CloseStatus(err))
	for range 16 {
		_, repeated := session.Receive(t.Context())
		require.EqualError(t, repeated, err.Error())
		sendErr := session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCommitAudio})
		require.EqualError(t, sendErr, err.Error())
	}
}

// TestCloseIsConcurrentAndIdempotent catches multiple normal close writes and
// callers left blocked behind the first close.
func TestCloseIsConcurrentAndIdempotent(t *testing.T) {
	server := newTestVoiceServer(t)
	session := connectTestSession(t, server.URL, nil, realtime.OpenRequest{Model: "model"})
	receiveResult := make(chan error, 1)
	go func() {
		_, err := session.Receive(t.Context())
		receiveResult <- err
	}()

	const closers = 32
	start := make(chan struct{})
	results := make(chan error, closers)
	var workers sync.WaitGroup
	for range closers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			results <- session.Close(t.Context())
		}()
	}
	close(start)
	workers.Wait()
	close(results)
	for err := range results {
		require.NoError(t, err)
	}
	require.ErrorIs(t, <-receiveResult, s2sws.ErrSessionClosed)
	require.ErrorIs(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCommitAudio}), s2sws.ErrSessionClosed)
	_, receiveErr := session.Receive(t.Context())
	require.ErrorIs(t, receiveErr, s2sws.ErrSessionClosed)
	require.NoError(t, session.Close(t.Context()))

	select {
	case err := <-server.peerClose:
		assert.Equal(t, websocket.StatusNormalClosure, websocket.CloseStatus(err))
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
	select {
	case err := <-server.peerClose:
		t.Fatalf("observed a second close: %v", err)
	default:
	}
}

// TestCloseCallersShareOneHandshake catches Close returning after merely
// enqueueing a close frame, binding the shared handshake to the first caller's
// context, or starting another handshake for later callers.
func TestCloseCallersShareOneHandshake(t *testing.T) {
	readGate := make(chan struct{})
	var releaseGate sync.Once
	server := newTestVoiceServerWithReadGate(t, readGate)
	session := connectTestSession(t, server.URL, nil, realtime.OpenRequest{Model: "model"})
	t.Cleanup(func() { releaseGate.Do(func() { close(readGate) }) })

	receiveResult := make(chan error, 1)
	go func() {
		_, err := session.Receive(t.Context())
		receiveResult <- err
	}()

	firstContext := newControlledDeadlineContext(t.Context())
	firstResult := make(chan error, 1)
	go func() { firstResult <- session.Close(firstContext) }()
	require.ErrorIs(t, <-receiveResult, s2sws.ErrSessionClosed)
	select {
	case err := <-firstResult:
		t.Fatalf("Close returned before the peer acknowledged the handshake: %v", err)
	case <-firstContext.observed:
	}
	firstContext.expire()
	require.ErrorIs(t, <-firstResult, context.DeadlineExceeded)

	const laterCallers = 16
	laterResults := make(chan error, laterCallers)
	laterContexts := make([]*observedContext, 0, laterCallers)
	for range laterCallers {
		closeContext := newObservedContext(t.Context())
		laterContexts = append(laterContexts, closeContext)
		go func() { laterResults <- session.Close(closeContext) }()
	}
	for _, closeContext := range laterContexts {
		select {
		case err := <-laterResults:
			t.Fatalf("later Close returned before the shared handshake completed: %v", err)
		case <-closeContext.observed:
		}
	}
	select {
	case err := <-laterResults:
		t.Fatalf("Close returned while the peer read gate was closed: %v", err)
	default:
	}

	releaseGate.Do(func() { close(readGate) })
	for range laterCallers {
		require.NoError(t, <-laterResults)
	}
	require.NoError(t, session.Close(t.Context()))
	for range 16 {
		require.ErrorIs(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCommitAudio}), s2sws.ErrSessionClosed)
		_, receiveErr := session.Receive(t.Context())
		require.ErrorIs(t, receiveErr, s2sws.ErrSessionClosed)
	}

	select {
	case err := <-server.peerClose:
		assert.Equal(t, websocket.StatusNormalClosure, websocket.CloseStatus(err))
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
	select {
	case err := <-server.peerClose:
		t.Fatalf("observed a second close: %v", err)
	default:
	}
}

// TestCloseUnblocksBlockedSends catches a normal close request queued behind a
// data write whose peer has stopped reading.
func TestCloseUnblocksBlockedSends(t *testing.T) {
	readGate := make(chan struct{})
	var releaseGate sync.Once
	server := newTestVoiceServerWithReadGate(t, readGate)
	session := connectTestSession(t, server.URL, func(cfg *s2sws.Config) {
		cfg.MaxFrameBytes = 4 << 20
	}, realtime.OpenRequest{Model: "model"})
	t.Cleanup(func() { releaseGate.Do(func() { close(readGate) }) })

	largeItem := json.RawMessage(`{"type":"message","content":"` + strings.Repeat("x", 3<<20) + `"}`)
	const senders = 8
	started := make(chan struct{}, senders)
	results := make(chan error, senders)
	for range senders {
		go func() {
			started <- struct{}{}
			results <- session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCreateItem, Item: largeItem})
		}()
	}
	for range senders {
		<-started
	}

	closeResult := make(chan error, 1)
	go func() { closeResult <- session.Close(t.Context()) }()
	for range senders {
		require.ErrorIs(t, <-results, s2sws.ErrSessionClosed)
	}
	require.ErrorIs(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCommitAudio}), s2sws.ErrSessionClosed)
	releaseGate.Do(func() { close(readGate) })
	require.NoError(t, <-closeResult)
}
