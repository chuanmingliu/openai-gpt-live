package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/protocol"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/chuanmingliu/openai-gpt-live/internal/session"
	"github.com/chuanmingliu/openai-gpt-live/internal/testkit"
	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

const testTimeout = 3 * time.Second

type fixedIDs struct{}

func (fixedIDs) NewSessionID() identity.SessionID           { return "session-test" }
func (fixedIDs) NewConversationID() identity.ConversationID { return "conversation-test" }

func validManager(connector realtime.Connector) *session.Manager {
	return session.NewManager(session.Config{
		IngressFrames:       4,
		EgressFrames:        4,
		ControlEvents:       8,
		WriterControlEvents: 4,
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
	}, connector, fixedIDs{})
}

func trustedAdmission(released *atomic.Int32) AdmissionFunc {
	return func(context.Context, *http.Request) (session.Subject, func(), error) {
		return session.Subject{
				TenantID:  "tenant-test",
				UserID:    "user-test",
				Scopes:    map[string]struct{}{"realtime": {}},
				ExpiresAt: time.Now().Add(time.Minute),
			}, func() {
				if released != nil {
					released.Add(1)
				}
			}, nil
	}
}

func testHandler(t *testing.T, opts Options) (http.Handler, *testkit.FakeConnector, *atomic.Int32) {
	t.Helper()
	connector := testkit.NewFakeConnector(32)
	released := &atomic.Int32{}
	return NewHandler(opts, validManager(connector), trustedAdmission(released)), connector, released
}

func dialRealtime(t *testing.T, server *httptest.Server) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	t.Cleanup(cancel)
	conn, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/realtime", nil)
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.CloseNow() })
	return conn
}

func readServerType(t *testing.T, conn *websocket.Conn) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	typ, raw, err := conn.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, websocket.MessageText, typ)
	var envelope struct {
		Type string `json:"type"`
	}
	require.NoError(t, json.Unmarshal(raw, &envelope))
	return envelope.Type
}

func TestHandlerCapabilitiesAreExactAndDoNotUpgrade(t *testing.T) {
	handler, _, _ := testHandler(t, Options{MaxFrameBytes: 4096, MaxJSONDepth: 7, MaxAudioBytes: 2048})
	request := httptest.NewRequest(http.MethodGet, "/v1/realtime/capabilities", nil)
	request.Header.Set("Connection", "Upgrade")
	request.Header.Set("Upgrade", "websocket")
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "application/json", recorder.Header().Get("Content-Type"))
	want, err := json.Marshal(protocol.Capabilities(protocol.Limits{
		MaxFrameBytes: 4096,
		MaxJSONDepth:  7,
		MaxAudioBytes: 2048,
	}))
	require.NoError(t, err)
	require.JSONEq(t, string(want), recorder.Body.String())
	require.Empty(t, recorder.Header().Get("Sec-WebSocket-Accept"))
}

func TestHandlerRejectsWrongRoutesAndMethodsBeforeAdmission(t *testing.T) {
	var admissions atomic.Int32
	handler := NewHandler(Options{MaxFrameBytes: 1024}, validManager(testkit.NewFakeConnector(4)), AdmissionFunc(
		func(context.Context, *http.Request) (session.Subject, func(), error) {
			admissions.Add(1)
			return session.Subject{}, nil, nil
		},
	))

	tests := []struct {
		method string
		path   string
		status int
	}{
		{http.MethodPost, "/v1/realtime", http.StatusMethodNotAllowed},
		{http.MethodPost, "/v1/realtime/capabilities", http.StatusMethodNotAllowed},
		{http.MethodGet, "/v1/realtime/extra", http.StatusNotFound},
	}
	for _, tt := range tests {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(tt.method, tt.path, nil))
		require.Equal(t, tt.status, recorder.Code)
	}
	require.Zero(t, admissions.Load())
}

func TestHandlerRejectsInvalidDependenciesAndOptions(t *testing.T) {
	tests := []http.Handler{
		NewHandler(Options{MaxFrameBytes: 1024}, nil, trustedAdmission(nil)),
		NewHandler(Options{MaxFrameBytes: 1024}, validManager(testkit.NewFakeConnector(4)), nil),
		NewHandler(Options{MaxFrameBytes: -1}, validManager(testkit.NewFakeConnector(4)), trustedAdmission(nil)),
		NewHandler(Options{MaxFrameBytes: 1024, MaxJSONDepth: -1}, validManager(testkit.NewFakeConnector(4)), trustedAdmission(nil)),
		NewHandler(Options{MaxFrameBytes: 1024, MaxAudioBytes: -1}, validManager(testkit.NewFakeConnector(4)), trustedAdmission(nil)),
	}
	for _, handler := range tests {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/realtime", nil))
		require.Equal(t, http.StatusInternalServerError, recorder.Code)
	}
}

func TestAdmissionCompletesBeforeAcceptAndReleaseRunsOnce(t *testing.T) {
	entered := make(chan struct{})
	continueAdmission := make(chan struct{})
	released := make(chan struct{})
	var releases atomic.Int32
	admission := AdmissionFunc(func(context.Context, *http.Request) (session.Subject, func(), error) {
		close(entered)
		<-continueAdmission
		return session.Subject{
				TenantID: "tenant", UserID: "user", Scopes: map[string]struct{}{}, ExpiresAt: time.Now().Add(time.Minute),
			}, func() {
				releases.Add(1)
				close(released)
			}, nil
	})
	handler := NewHandler(Options{MaxFrameBytes: 4096}, validManager(testkit.NewFakeConnector(4)), admission)
	server := httptest.NewServer(handler)
	defer server.Close()
	type dialResult struct {
		conn *websocket.Conn
		err  error
	}
	result := make(chan dialResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		defer cancel()
		conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/realtime", nil)
		result <- dialResult{conn: conn, err: err}
	}()

	<-entered
	select {
	case early := <-result:
		if early.conn != nil {
			_ = early.conn.CloseNow()
		}
		t.Fatal("websocket accepted before admission completed")
	default:
	}
	close(continueAdmission)
	dialed := <-result
	require.NoError(t, dialed.err)
	require.NotNil(t, dialed.conn)
	require.Equal(t, "session.created", readServerType(t, dialed.conn))
	require.NoError(t, dialed.conn.Close(websocket.StatusNormalClosure, "done"))
	select {
	case <-released:
	case <-time.After(testTimeout):
		t.Fatal("admission release did not run")
	}
	require.Equal(t, int32(1), releases.Load())
}

func TestAdmissionRejectionStaysHTTP(t *testing.T) {
	admission := AdmissionFunc(func(context.Context, *http.Request) (session.Subject, func(), error) {
		return session.Subject{}, nil, context.Canceled
	})
	server := httptest.NewServer(NewHandler(Options{MaxFrameBytes: 1024}, validManager(testkit.NewFakeConnector(4)), admission))
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	conn, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/realtime", nil)
	require.Error(t, err)
	require.Nil(t, conn)
	require.NotNil(t, response)
	require.Equal(t, http.StatusUnauthorized, response.StatusCode)
	_ = response.Body.Close()
}

func TestConnectionRejectsBinaryMalformedUnknownAndOversizeFrames(t *testing.T) {
	tests := []struct {
		name      string
		limit     int64
		typ       websocket.MessageType
		payload   []byte
		wantClose websocket.StatusCode
	}{
		{"binary", 4096, websocket.MessageBinary, []byte{1}, websocket.StatusUnsupportedData},
		{"malformed", 4096, websocket.MessageText, []byte(`{"type":`), websocket.StatusInvalidFramePayloadData},
		{"unknown", 4096, websocket.MessageText, []byte(`{"type":"client.secret"}`), websocket.StatusPolicyViolation},
		{"oversize", 31, websocket.MessageText, []byte(`{"type":"input_audio_buffer.commit"}`), websocket.StatusMessageTooBig},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, _, _ := testHandler(t, Options{MaxFrameBytes: tt.limit})
			server := httptest.NewServer(handler)
			defer server.Close()
			conn := dialRealtime(t, server)
			require.Equal(t, "session.created", readServerType(t, conn))
			ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
			defer cancel()
			require.NoError(t, conn.Write(ctx, tt.typ, tt.payload))
			_, _, err := conn.Read(ctx)
			require.Error(t, err)
			require.Equal(t, tt.wantClose, websocket.CloseStatus(err))
		})
	}
}

func TestHandlerReleaseRunsOnUpgradeFailure(t *testing.T) {
	var releases atomic.Int32
	handler := NewHandler(Options{MaxFrameBytes: 1024}, validManager(testkit.NewFakeConnector(4)), trustedAdmission(&releases))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/realtime", nil))
	require.Equal(t, http.StatusUpgradeRequired, recorder.Code)
	require.Equal(t, int32(1), releases.Load())
}

func TestCapabilitySlicesCannotMutateFutureResponses(t *testing.T) {
	handler, _, _ := testHandler(t, Options{MaxFrameBytes: 1024})
	read := func() protocol.CapabilityDocument {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/realtime/capabilities", nil))
		var document protocol.CapabilityDocument
		require.NoError(t, json.NewDecoder(recorder.Body).Decode(&document))
		return document
	}
	first := read()
	first.ClientEvents[0] = "mutated"
	second := read()
	require.Equal(t, "session.update", second.ClientEvents[0])
}

func TestHandlerDoesNotTrustClientIdentityParameters(t *testing.T) {
	var admitted session.Subject
	requests := make(chan realtime.OpenRequest, 1)
	connector := &recordingConnector{delegate: testkit.NewFakeConnector(4), requests: requests}
	admission := AdmissionFunc(func(context.Context, *http.Request) (session.Subject, func(), error) {
		admitted = session.Subject{TenantID: "trusted", UserID: "trusted-user", Scopes: map[string]struct{}{}, ExpiresAt: time.Now().Add(time.Minute)}
		return admitted, nil, nil
	})
	server := httptest.NewServer(NewHandler(Options{MaxFrameBytes: 4096}, validManager(connector), admission))
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	url := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/realtime?tenant=attacker&user=attacker&model=test-model"
	conn, response, err := websocket.Dial(ctx, url, nil)
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}
	require.NoError(t, err)
	defer conn.CloseNow()
	require.Equal(t, "trusted", admitted.TenantID)
	require.Equal(t, "session.created", readServerType(t, conn))
	select {
	case request := <-requests:
		require.Equal(t, "test-model", request.Model)
	case <-time.After(testTimeout):
		t.Fatal("connector did not receive open request")
	}
}

type recordingConnector struct {
	delegate *testkit.FakeConnector
	requests chan realtime.OpenRequest
}

func (c *recordingConnector) Connect(ctx context.Context, request realtime.OpenRequest) (realtime.Session, error) {
	select {
	case c.requests <- request:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return c.delegate.Connect(ctx, request)
}

func TestCapabilityEndpointBodyIsBounded(t *testing.T) {
	handler, _, _ := testHandler(t, Options{MaxFrameBytes: 1024})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/realtime/capabilities", nil))
	body, err := io.ReadAll(io.LimitReader(recorder.Body, 16<<10))
	require.NoError(t, err)
	require.Less(t, len(body), 16<<10)
}

func openConnectionAdapter(t *testing.T, opts Options) (*clientConn, *websocket.Conn) {
	t.Helper()
	serverConn := make(chan *clientConn, 1)
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		normalized, limits, err := normalizeOptions(opts)
		if err != nil {
			_ = conn.CloseNow()
			return
		}
		serverConn <- newClientConn(conn, limits, normalized.ReadTimeout, normalized.WriteTimeout)
		<-done
	}))
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	t.Cleanup(cancel)
	client, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}
	require.NoError(t, err)
	adapter := <-serverConn
	t.Cleanup(func() {
		_ = client.CloseNow()
		close(done)
	})
	return adapter, client
}

func TestConnectionConcurrentWritesRemainWholeFrames(t *testing.T) {
	adapter, client := openConnectionAdapter(t, Options{MaxFrameBytes: 8 << 10})
	const writes = 32
	results := make(chan error, writes)
	for i := range writes {
		i := i
		go func() {
			results <- adapter.Write(t.Context(), protocol.ErrorEvent{
				Type: "error", EventID: fmt.Sprintf("event-%02d", i),
				Error: protocol.ProtocolError{Type: "test", Message: fmt.Sprintf("message-%02d", i)},
			})
		}()
	}
	seen := make(map[string]bool, writes)
	for range writes {
		ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
		typ, raw, err := client.Read(ctx)
		cancel()
		require.NoError(t, err)
		require.Equal(t, websocket.MessageText, typ)
		event, err := protocol.DecodeServer(raw, protocol.Limits{MaxFrameBytes: 8 << 10, MaxJSONDepth: 16, MaxAudioBytes: 1024})
		require.NoError(t, err)
		decoded := event.(protocol.ErrorEvent)
		seen[decoded.EventID] = true
	}
	for range writes {
		require.NoError(t, <-results)
	}
	require.Len(t, seen, writes)
}

func TestConnectionFirstCloseCodeAndBoundedReasonWinOnce(t *testing.T) {
	adapter, client := openConnectionAdapter(t, Options{MaxFrameBytes: 1024})
	longReason := strings.Repeat("first-", 40)
	firstResult := make(chan error, 1)
	go func() { firstResult <- adapter.Close(int(websocket.StatusUnsupportedData), longReason) }()
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	_, _, err := client.Read(ctx)
	require.Error(t, err)
	var closeErr websocket.CloseError
	require.ErrorAs(t, err, &closeErr)
	require.Equal(t, websocket.StatusUnsupportedData, closeErr.Code)
	require.Equal(t, boundedCloseReason(longReason), closeErr.Reason)
	require.LessOrEqual(t, len(closeErr.Reason), 123)
	require.NoError(t, <-firstResult)
	require.NoError(t, adapter.Close(int(websocket.StatusInternalError), "second"))
	require.Equal(t, websocket.StatusUnsupportedData, closeErr.Code)
}

func TestConnectionReadTimeoutAndWriteCancellationUnblock(t *testing.T) {
	t.Run("read timeout", func(t *testing.T) {
		adapter, _ := openConnectionAdapter(t, Options{MaxFrameBytes: 1024, ReadTimeout: time.Nanosecond})
		_, err := adapter.Read(context.Background())
		require.Error(t, err)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
	t.Run("write cancellation", func(t *testing.T) {
		adapter, _ := openConnectionAdapter(t, Options{MaxFrameBytes: 1024})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := adapter.Write(ctx, protocol.ErrorEvent{
			Type: "error", Error: protocol.ProtocolError{Type: "test", Message: "cancelled"},
		})
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestConnectionOwnsFrameBytesAtBothBoundaries(t *testing.T) {
	adapter, client := openConnectionAdapter(t, Options{MaxFrameBytes: 4096})
	clientRaw := []byte(`{"type":"input_audio_buffer.commit"}`)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	require.NoError(t, client.Write(ctx, websocket.MessageText, clientRaw))
	cancel()
	for i := range clientRaw {
		clientRaw[i] = 'x'
	}
	event, err := adapter.Read(t.Context())
	require.NoError(t, err)
	require.Equal(t, "input_audio_buffer.commit", event.EventType())

	providerSession := json.RawMessage(`{"id":"provider-session"}`)
	writeResult := make(chan error, 1)
	go func() {
		writeResult <- adapter.Write(t.Context(), protocol.SessionCreatedEvent{
			Type: "session.created", Session: providerSession,
		})
	}()
	ctx, cancel = context.WithTimeout(t.Context(), testTimeout)
	_, raw, err := client.Read(ctx)
	cancel()
	require.NoError(t, err)
	require.NoError(t, <-writeResult)
	for i := range providerSession {
		providerSession[i] = 'x'
	}
	var decoded protocol.SessionCreatedEvent
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.JSONEq(t, `{"id":"provider-session"}`, string(decoded.Session))
}

func TestAdmissionReleaseRunsOnceOnPeerNormalClose(t *testing.T) {
	released := make(chan struct{})
	var releases atomic.Int32
	admission := AdmissionFunc(func(context.Context, *http.Request) (session.Subject, func(), error) {
		return session.Subject{TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Minute)}, func() {
			if releases.Add(1) == 1 {
				close(released)
			}
		}, nil
	})
	server := httptest.NewServer(NewHandler(Options{MaxFrameBytes: 4096}, validManager(testkit.NewFakeConnector(4)), admission))
	defer server.Close()
	client := dialRealtime(t, server)
	require.Equal(t, "session.created", readServerType(t, client))
	require.NoError(t, client.Close(websocket.StatusNormalClosure, "done"))
	select {
	case <-released:
	case <-time.After(testTimeout):
		t.Fatal("release did not run after peer close")
	}
	require.Equal(t, int32(1), releases.Load())
}

func TestAdmissionReleaseRunsOnceOnContextCancellation(t *testing.T) {
	serverContext, cancelServer := context.WithCancel(context.Background())
	defer cancelServer()
	released := make(chan struct{})
	var releases atomic.Int32
	admission := AdmissionFunc(func(context.Context, *http.Request) (session.Subject, func(), error) {
		return session.Subject{TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Minute)}, func() {
			if releases.Add(1) == 1 {
				close(released)
			}
		}, nil
	})
	server := httptest.NewUnstartedServer(NewHandler(Options{MaxFrameBytes: 4096}, validManager(testkit.NewFakeConnector(4)), admission))
	server.Config.BaseContext = func(net.Listener) context.Context { return serverContext }
	server.Start()
	defer server.Close()
	client := dialRealtime(t, server)
	require.Equal(t, "session.created", readServerType(t, client))
	cancelServer()
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	_, _, err := client.Read(ctx)
	require.Error(t, err)
	require.NotEqual(t, websocket.StatusInternalError, websocket.CloseStatus(err))
	select {
	case <-released:
	case <-time.After(testTimeout):
		t.Fatal("release did not run after context cancellation")
	}
	require.Equal(t, int32(1), releases.Load())
}

func TestAdmissionReleaseRunsOnceOnManagerFailure(t *testing.T) {
	released := make(chan struct{})
	var releases atomic.Int32
	admission := AdmissionFunc(func(context.Context, *http.Request) (session.Subject, func(), error) {
		return session.Subject{TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Minute)}, func() {
			if releases.Add(1) == 1 {
				close(released)
			}
		}, nil
	})
	invalidManager := session.NewManager(session.Config{}, testkit.NewFakeConnector(4), fixedIDs{})
	server := httptest.NewServer(NewHandler(Options{MaxFrameBytes: 4096}, invalidManager, admission))
	defer server.Close()
	client := dialRealtime(t, server)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	_, _, err := client.Read(ctx)
	require.Error(t, err)
	require.Equal(t, websocket.StatusInternalError, websocket.CloseStatus(err))
	select {
	case <-released:
	case <-time.After(testTimeout):
		t.Fatal("release did not run after manager failure")
	}
	require.Equal(t, int32(1), releases.Load())
}
