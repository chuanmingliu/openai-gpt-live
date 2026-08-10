package ws

import (
	"context"
	"encoding/base64"
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

	admissionpkg "github.com/chuanmingliu/openai-gpt-live/internal/admission"
	"github.com/chuanmingliu/openai-gpt-live/internal/auth"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/protocol"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/chuanmingliu/openai-gpt-live/internal/session"
	"github.com/chuanmingliu/openai-gpt-live/internal/testkit"
	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testTimeout = 3 * time.Second

type fixedIDs struct{}

type e2eLeaseStore struct{ calls, releases atomic.Int32 }

type forgedOriginAdmission struct {
	*admissionpkg.Controller
	releases atomic.Int32
}

func (f *forgedOriginAdmission) Admit(context.Context, *http.Request) (session.Subject, func(), error) {
	return session.Subject{TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Minute)}, func() {
		f.releases.Add(1)
	}, nil
}

// ExactOriginValidated intentionally matches the removed public marker. It
// must not grant the package-sealed origin capability.
func (*forgedOriginAdmission) ExactOriginValidated() {}

func (*e2eLeaseStore) Ready(context.Context) error { return nil }
func (s *e2eLeaseStore) Acquire(context.Context, session.Subject, admissionpkg.Limits) (func(), error) {
	s.calls.Add(1)
	var once atomic.Bool
	return func() {
		if once.CompareAndSwap(false, true) {
			s.releases.Add(1)
		}
	}, nil
}

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

func validUpgradeRequest() *http.Request {
	request := httptest.NewRequest(http.MethodGet, "http://example.test/v1/realtime", nil)
	request.Header.Set("Connection", "Upgrade")
	request.Header.Set("Upgrade", "websocket")
	request.Header.Set("Sec-WebSocket-Version", "13")
	request.Header.Set("Sec-WebSocket-Key", base64.StdEncoding.EncodeToString(make([]byte, 16)))
	return request
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

func TestAdmissionTypedErrorsMapToContentFreeHTTPResponses(t *testing.T) {
	tests := []struct {
		err    error
		status int
		header string
	}{
		{admissionpkg.ErrMalformed, http.StatusBadRequest, ""},
		{admissionpkg.ErrUnauthorized, http.StatusUnauthorized, "WWW-Authenticate"},
		{admissionpkg.ErrForbidden, http.StatusForbidden, ""},
		{admissionpkg.ErrLimited, http.StatusTooManyRequests, "Retry-After"},
		{admissionpkg.ErrUnavailable, http.StatusServiceUnavailable, "Retry-After"},
	}
	for _, test := range tests {
		recorder := httptest.NewRecorder()
		handler := NewHandler(Options{}, validManager(testkit.NewFakeConnector(1)), AdmissionFunc(func(context.Context, *http.Request) (session.Subject, func(), error) {
			return session.Subject{}, nil, test.err
		}))
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/realtime", nil))
		require.Equal(t, test.status, recorder.Code)
		require.Equal(t, "no-store", recorder.Header().Get("Cache-Control"))
		require.Empty(t, recorder.Body.Bytes())
		if test.header != "" {
			require.NotEmpty(t, recorder.Header().Get(test.header))
		}
	}
}

func TestRealAdmissionOriginPolicyAndWebSocketUpgradeAgree(t *testing.T) {
	for _, test := range []struct {
		name     string
		origin   func(string) string
		allowed  func(string) []string
		empty    bool
		accepted bool
	}{
		{"cross origin", func(string) string { return "http://app.example" }, func(string) []string { return []string{"http://app.example"} }, false, true},
		{"same origin", func(g string) string { return g }, func(g string) []string { return []string{g} }, false, true},
		{"empty origin", func(string) string { return "" }, func(string) []string { return nil }, true, true},
		{"rejected origin", func(string) string { return "http://evil.example" }, func(string) []string { return []string{"http://app.example"} }, false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			gatewayOrigin := "http://" + listener.Addr().String()
			store := &e2eLeaseStore{}
			static := auth.NewStatic([]byte("development-token"), auth.Principal{TenantID: "tenant", UserID: "user", Scopes: map[string]struct{}{"realtime": {}}})
			controller, err := admissionpkg.New(static, admissionpkg.Config{AllowedOrigins: test.allowed(gatewayOrigin), AllowEmptyOrigin: test.empty, GlobalRate: 100, GlobalBurst: 10, IPRate: 100, IPBurst: 10, IPKeyTTL: time.Minute, MaxIPKeys: 10, RequiredScopes: []string{"realtime"}, Limits: admissionpkg.Limits{Global: 10, PerTenant: 10, PerUser: 10}}, store)
			require.NoError(t, err)
			server := httptest.NewUnstartedServer(NewHandler(Options{}, validManager(testkit.NewFakeConnector(2)), controller))
			server.Listener = listener
			server.Start()
			defer server.Close()
			headers := http.Header{"Authorization": []string{"Bearer development-token"}}
			if origin := test.origin(gatewayOrigin); origin != "" {
				headers.Set("Origin", origin)
			}
			ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
			defer cancel()
			conn, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/realtime", &websocket.DialOptions{HTTPHeader: headers})
			if test.accepted {
				require.NoError(t, err)
				require.NotNil(t, conn)
				require.Equal(t, "session.created", readServerType(t, conn))
				require.NoError(t, conn.Close(websocket.StatusNormalClosure, "done"))
				require.Eventually(t, func() bool { return store.releases.Load() == 1 }, testTimeout, 10*time.Millisecond)
				assert.Equal(t, int32(1), store.calls.Load())
			} else {
				require.Error(t, err)
				require.Nil(t, conn)
				require.NotNil(t, response)
				body, readErr := io.ReadAll(response.Body)
				require.NoError(t, readErr)
				response.Body.Close()
				require.Empty(t, body)
				require.Zero(t, store.calls.Load())
				require.Zero(t, store.releases.Load())
			}
		})
	}
}

func TestCustomAdmissionCannotForgeOriginCapability(t *testing.T) {
	// Embedding the real controller would promote any structural marker it
	// implemented. Exact concrete-type recognition must still reject this fake.
	forged := &forgedOriginAdmission{Controller: new(admissionpkg.Controller)}
	server := httptest.NewServer(NewHandler(Options{}, validManager(testkit.NewFakeConnector(2)), forged))
	defer server.Close()

	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	conn, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/realtime", &websocket.DialOptions{
		HTTPHeader: http.Header{"Origin": []string{"http://evil.example"}},
	})
	require.Error(t, err)
	require.Nil(t, conn)
	require.NotNil(t, response)
	body, readErr := io.ReadAll(response.Body)
	require.NoError(t, readErr)
	response.Body.Close()
	require.Equal(t, http.StatusForbidden, response.StatusCode)
	require.Equal(t, "no-store", response.Header.Get("Cache-Control"))
	require.Empty(t, body)
	require.Equal(t, int32(1), forged.releases.Load())
}

func TestOrdinaryCustomAdmissionRetainsSameOriginFallback(t *testing.T) {
	var releases atomic.Int32
	server := httptest.NewServer(NewHandler(Options{}, validManager(testkit.NewFakeConnector(2)), trustedAdmission(&releases)))
	defer server.Close()

	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	cross, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/realtime", &websocket.DialOptions{
		HTTPHeader: http.Header{"Origin": []string{"http://evil.example"}},
	})
	require.Error(t, err)
	require.Nil(t, cross)
	require.NotNil(t, response)
	body, readErr := io.ReadAll(response.Body)
	require.NoError(t, readErr)
	response.Body.Close()
	require.Equal(t, http.StatusForbidden, response.StatusCode)
	require.Equal(t, "no-store", response.Header.Get("Cache-Control"))
	require.Empty(t, body)

	sameOrigin := strings.TrimPrefix(server.URL, "http://")
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/realtime", &websocket.DialOptions{
		HTTPHeader: http.Header{"Origin": []string{"http://" + sameOrigin}},
	})
	require.NoError(t, err)
	require.NotNil(t, conn)
	require.Equal(t, "session.created", readServerType(t, conn))
	require.NoError(t, conn.Close(websocket.StatusNormalClosure, "done"))
	require.Eventually(t, func() bool { return releases.Load() == 2 }, testTimeout, 10*time.Millisecond)
}

func TestHandshakeFailureAfterAdmissionReleasesExactLeaseWithEmptyBody(t *testing.T) {
	store := &e2eLeaseStore{}
	static := auth.NewStatic([]byte("development-token"), auth.Principal{TenantID: "tenant", UserID: "user", Scopes: map[string]struct{}{"realtime": {}}})
	controller, err := admissionpkg.New(static, admissionpkg.Config{AllowedOrigins: []string{"http://app.example"}, GlobalRate: 10, GlobalBurst: 10, IPRate: 10, IPBurst: 10, IPKeyTTL: time.Minute, MaxIPKeys: 10, RequiredScopes: []string{"realtime"}, Limits: admissionpkg.Limits{Global: 10, PerTenant: 10, PerUser: 10}}, store)
	require.NoError(t, err)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "http://gateway.example/v1/realtime", nil)
	request.RemoteAddr = "192.0.2.1:1234"
	request.Header.Set("Origin", "http://app.example")
	request.Header.Set("Authorization", "Bearer development-token")
	NewHandler(Options{}, validManager(testkit.NewFakeConnector(1)), controller).ServeHTTP(recorder, request)
	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Empty(t, recorder.Body.Bytes())
	require.Equal(t, "no-store", recorder.Header().Get("Cache-Control"))
	require.Equal(t, int32(1), store.calls.Load())
	require.Equal(t, int32(1), store.releases.Load())
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
	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Empty(t, recorder.Body.Bytes())
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

func TestCanaryRuntimeHeadersRequireAuthenticatedEphemeralSubject(t *testing.T) {
	identity := RuntimeIdentity{
		ConfigHash:   "v1:" + strings.Repeat("a", 64),
		Region:       "us-east-1",
		GatewayImage: "ghcr.io/chuanmingliu/openai-gpt-live/gateway@sha256:" + strings.Repeat("b", 64),
	}
	for name, ephemeral := range map[string]bool{"exact ephemeral canary": true, "ordinary authenticated user": false} {
		t.Run(name, func(t *testing.T) {
			admission := AdmissionFunc(func(context.Context, *http.Request) (session.Subject, func(), error) {
				return session.Subject{TenantID: "trusted", UserID: "trusted-user", Ephemeral: ephemeral, ExpiresAt: time.Now().Add(time.Minute)}, func() {}, nil
			})
			server := httptest.NewServer(NewHandler(Options{MaxFrameBytes: 4096, CanaryRuntimeIdentity: identity}, validManager(testkit.NewFakeConnector(4)), admission))
			defer server.Close()
			ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
			defer cancel()
			conn, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/realtime", nil)
			if response != nil && response.Body != nil {
				defer response.Body.Close()
			}
			require.NoError(t, err)
			defer conn.CloseNow()
			if ephemeral {
				require.Equal(t, identity.ConfigHash, response.Header.Get("X-Live-Config-Hash"))
				require.Equal(t, identity.Region, response.Header.Get("X-Live-Region"))
				require.Equal(t, identity.GatewayImage, response.Header.Get("X-Live-Gateway-Image"))
			} else {
				require.Empty(t, response.Header.Get("X-Live-Config-Hash"))
				require.Empty(t, response.Header.Get("X-Live-Region"))
				require.Empty(t, response.Header.Get("X-Live-Gateway-Image"))
			}
		})
	}
}

func TestCanaryRuntimeHeadersFailClosedAndNeverLeakBeforeAdmission(t *testing.T) {
	for name, identity := range map[string]RuntimeIdentity{
		"missing":         {},
		"partial":         {ConfigHash: "v1:" + strings.Repeat("a", 64)},
		"malformed image": {ConfigHash: "v1:" + strings.Repeat("a", 64), Region: "us-east-1", GatewayImage: "gateway:mutable"},
	} {
		t.Run(name, func(t *testing.T) {
			admission := AdmissionFunc(func(context.Context, *http.Request) (session.Subject, func(), error) {
				return session.Subject{TenantID: "synthetic", UserID: "canary", Ephemeral: true, ExpiresAt: time.Now().Add(time.Minute)}, func() {}, nil
			})
			server := httptest.NewServer(NewHandler(Options{CanaryRuntimeIdentity: identity}, validManager(testkit.NewFakeConnector(2)), admission))
			defer server.Close()
			ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
			defer cancel()
			conn, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/realtime", nil)
			if conn != nil {
				_ = conn.CloseNow()
			}
			require.Error(t, err)
			require.NotNil(t, response)
			require.Empty(t, response.Header.Get("X-Live-Config-Hash"))
			require.Empty(t, response.Header.Get("X-Live-Region"))
			require.Empty(t, response.Header.Get("X-Live-Gateway-Image"))
		})
	}

	identity := RuntimeIdentity{ConfigHash: "v1:" + strings.Repeat("a", 64), Region: "us-east-1", GatewayImage: "ghcr.io/chuanmingliu/openai-gpt-live/gateway@sha256:" + strings.Repeat("b", 64)}
	denied := AdmissionFunc(func(context.Context, *http.Request) (session.Subject, func(), error) {
		return session.Subject{}, nil, admissionpkg.ErrUnauthorized
	})
	recorder := httptest.NewRecorder()
	request := validUpgradeRequest()
	NewHandler(Options{CanaryRuntimeIdentity: identity}, validManager(testkit.NewFakeConnector(2)), denied).ServeHTTP(recorder, request)
	require.Equal(t, http.StatusUnauthorized, recorder.Code)
	require.Empty(t, recorder.Header().Get("X-Live-Config-Hash"))
	require.Empty(t, recorder.Header().Get("X-Live-Region"))
	require.Empty(t, recorder.Header().Get("X-Live-Gateway-Image"))
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
	go func() { firstResult <- adapter.Close(t.Context(), int(websocket.StatusUnsupportedData), longReason) }()
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
	require.NoError(t, adapter.Close(t.Context(), int(websocket.StatusInternalError), "second"))
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
