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
	for _, path := range []string{"/", "/styles.css", "/app.js", "/audio-core.mjs", "/capture-worklet.js", "/playback-worklet.js"} {
		request := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:3000"+path, nil)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		assert.Equal(t, http.StatusOK, response.Code, path)
		assert.Contains(t, response.Header().Get("Content-Security-Policy"), "default-src 'self'")
		assert.Equal(t, "nosniff", response.Header().Get("X-Content-Type-Options"))
		assert.Equal(t, "no-store", response.Header().Get("Cache-Control"))
	}
}

func TestHealthReadyIsContentFree(t *testing.T) {
	handler := mustTestHandler(t, nil)
	request := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:3000/health/ready", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Equal(t, "ready\n", response.Body.String())
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
	cfg, err := parseConfig(nil)
	require.NoError(t, err)
	if dial == nil {
		dial = func(context.Context, *url.URL, http.Header) (*websocket.Conn, error) {
			return nil, errors.New("unused dial")
		}
	}
	handler, err := newHandler(cfg, []byte("sentinel"), dial)
	require.NoError(t, err)
	return handler
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
