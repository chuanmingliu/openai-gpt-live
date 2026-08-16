package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStaticAssetsCarryStrictHeaders(t *testing.T) {
	handler := mustTestHandler(t, nil)
	for _, path := range []string{"/", "/styles.css", "/app.js", "/audio-core.mjs", "/capture-worklet.js", "/playback-worklet.js", "/session-controller.mjs", "/setup-panel.mjs"} {
		request := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:3000"+path, nil)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		assert.Equal(t, http.StatusOK, response.Code, path)
		assert.Contains(t, response.Header().Get("Content-Security-Policy"), "default-src 'self'")
		assert.Equal(t, "nosniff", response.Header().Get("X-Content-Type-Options"))
		assert.Equal(t, "no-store", response.Header().Get("Cache-Control"))
		assert.Equal(t, "DENY", response.Header().Get("X-Frame-Options"))
		assert.Equal(t, "same-origin", response.Header().Get("Cross-Origin-Resource-Policy"))
		assert.Empty(t, response.Header().Get("Strict-Transport-Security"))
		assert.NotContains(t, response.Body.String(), "Authorization")
		assert.NotContains(t, response.Body.String(), "sentinel")
	}
}

func TestSetupStatusIsContentFreeAndReportsMode(t *testing.T) {
	handler := mustTestHandler(t, nil)
	request := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:3000/setup", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Equal(t, "application/json; charset=utf-8", response.Header().Get("Content-Type"))
	assert.NotContains(t, response.Body.String(), "sentinel")
	assert.NotContains(t, response.Body.String(), "Authorization")
	assert.NotContains(t, response.Body.String(), "LIVE_DEMO_BEARER=")
	assert.Contains(t, response.Body.String(), `"mode":"local"`)
	assert.Contains(t, response.Body.String(), `"gateway_host":"127.0.0.1:18080"`)
	assert.Contains(t, response.Body.String(), `"model":"gpt-live-simulator"`)
}

func TestHealthReadyIsContentFree(t *testing.T) {
	handler := mustTestHandler(t, nil)
	request := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:3000/health/ready", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Equal(t, "ready\n", response.Body.String())
}

func TestHealthLiveIsIndependentOfReady(t *testing.T) {
	rt := mustTestRuntime(t, nil)
	rt.sessions.BeginDrain()
	live := httptest.NewRecorder()
	rt.handler().ServeHTTP(live, httptest.NewRequest(http.MethodGet, "http://127.0.0.1:3000/health/live", nil))
	assert.Equal(t, http.StatusOK, live.Code)
	ready := httptest.NewRecorder()
	rt.handler().ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "http://127.0.0.1:3000/health/ready", nil))
	assert.Equal(t, http.StatusServiceUnavailable, ready.Code)
}

func TestFaviconIsContentFree(t *testing.T) {
	handler := mustTestHandler(t, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "http://127.0.0.1:3000/favicon.ico", nil))
	assert.Equal(t, http.StatusNoContent, response.Code)
	assert.Empty(t, response.Body.String())
}

func TestStaticAssetsDoNotServeTests(t *testing.T) {
	handler := mustTestHandler(t, nil)
	request := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:3000/audio-core.test.mjs", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	assert.Equal(t, http.StatusNotFound, response.Code)
}

func TestWebSocketRejectsWhenDrainingBeforeDial(t *testing.T) {
	var calls atomic.Int32
	rt := mustTestRuntime(t, func(context.Context, *url.URL, http.Header) (*websocket.Conn, error) {
		calls.Add(1)
		return nil, errors.New("must not dial")
	})
	rt.sessions.BeginDrain()
	server := httptest.NewServer(rt.handler())
	t.Cleanup(server.Close)
	header := http.Header{"Origin": []string{"http://127.0.0.1:3000"}}
	_, response, err := websocket.Dial(t.Context(), wsURL(server.URL)+"/ws", &websocket.DialOptions{HTTPHeader: header})
	require.Error(t, err)
	require.NotNil(t, response)
	assert.Equal(t, http.StatusServiceUnavailable, response.StatusCode)
	assert.Zero(t, calls.Load())
}

func TestProductionHeadersIncludeHSTS(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-mode", "production",
		"-behind-ingress",
		"-origin", "https://voice.example",
		"-gateway-url", "wss://gateway.example/v1/realtime",
		"-listen", "0.0.0.0:3000",
	})
	require.NoError(t, err)
	rt, err := newRuntime(cfg, []byte("sentinel"), func(context.Context, *url.URL, http.Header) (*websocket.Conn, error) {
		return nil, errors.New("unused dial")
	})
	require.NoError(t, err)
	response := httptest.NewRecorder()
	rt.handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "https://voice.example/", nil))
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Contains(t, response.Header().Get("Strict-Transport-Security"), "max-age=31536000")
	assert.Contains(t, response.Header().Get("Content-Security-Policy"), "wss://voice.example")
}

func TestWebSocketRejectsQueryBeforeDial(t *testing.T) {
	var calls atomic.Int32
	handler := mustTestHandler(t, func(context.Context, *url.URL, http.Header) (*websocket.Conn, error) {
		calls.Add(1)
		return nil, errors.New("must not dial")
	})
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	header := http.Header{"Origin": []string{"http://127.0.0.1:3000"}}
	_, response, err := websocket.Dial(t.Context(), wsURL(server.URL)+"/ws?token=1", &websocket.DialOptions{HTTPHeader: header})
	require.Error(t, err)
	require.NotNil(t, response)
	assert.Equal(t, http.StatusBadRequest, response.StatusCode)
	assert.Zero(t, calls.Load())
}

func TestWebSocketRejectsWhenAtCapacity(t *testing.T) {
	var calls atomic.Int32
	rt := mustTestRuntime(t, func(context.Context, *url.URL, http.Header) (*websocket.Conn, error) {
		calls.Add(1)
		return nil, errors.New("must not dial")
	})
	for i := 0; i < rt.cfg.MaxSessions; i++ {
		_, ok := rt.sessions.Acquire(func() {})
		require.True(t, ok)
	}
	server := httptest.NewServer(rt.handler())
	t.Cleanup(server.Close)
	header := http.Header{"Origin": []string{"http://127.0.0.1:3000"}}
	_, response, err := websocket.Dial(t.Context(), wsURL(server.URL)+"/ws", &websocket.DialOptions{HTTPHeader: header})
	require.Error(t, err)
	require.NotNil(t, response)
	assert.Equal(t, http.StatusServiceUnavailable, response.StatusCode)
	assert.Zero(t, calls.Load())
}

func TestWebSocketRejectsForeignOriginBeforeDial(t *testing.T) {
	var calls atomic.Int32
	handler := mustTestHandler(t, func(context.Context, *url.URL, http.Header) (*websocket.Conn, error) {
		calls.Add(1)
		return nil, errors.New("must not dial")
	})
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	header := http.Header{"Origin": []string{"http://evil.invalid"}}
	_, response, err := websocket.Dial(t.Context(), wsURL(server.URL)+"/ws", &websocket.DialOptions{HTTPHeader: header})
	require.Error(t, err)
	require.NotNil(t, response)
	assert.Equal(t, http.StatusForbidden, response.StatusCode)
	assert.Zero(t, calls.Load())
}

func mustTestHandler(t *testing.T, dial dialUpstream) http.Handler {
	t.Helper()
	return mustTestRuntime(t, dial).handler()
}

func mustTestRuntime(t *testing.T, dial dialUpstream) *runtime {
	t.Helper()
	cfg, err := parseConfig(nil)
	require.NoError(t, err)
	if dial == nil {
		dial = func(context.Context, *url.URL, http.Header) (*websocket.Conn, error) {
			return nil, errors.New("unused dial")
		}
	}
	rt, err := newRuntime(cfg, []byte("sentinel"), dial)
	require.NoError(t, err)
	return rt
}

func wsURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	parsed.Scheme = "ws"
	return parsed.String()
}

func writeText(t *testing.T, conn *websocket.Conn, payload []byte) {
	t.Helper()
	require.NoError(t, conn.Write(t.Context(), websocket.MessageText, payload))
}

func readText(t *testing.T, conn *websocket.Conn) []byte {
	t.Helper()
	kind, payload, err := conn.Read(t.Context())
	require.NoError(t, err)
	require.Equal(t, websocket.MessageText, kind)
	return payload
}

func drain(t *testing.T, body io.ReadCloser) {
	t.Helper()
	if body != nil {
		_, _ = io.Copy(io.Discard, body)
		_ = body.Close()
	}
}
