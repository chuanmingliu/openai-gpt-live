package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStaticAssetsCarryStrictHeadersWithoutBearer(t *testing.T) {
	handler := mustTestHandler(t, nil, []byte("sentinel-browser-secret"))
	request := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:3000/", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	assert.Equal(t, http.StatusOK, response.Code)
	assert.Contains(t, response.Header().Get("Content-Security-Policy"), "default-src 'self'")
	assert.Contains(t, response.Header().Get("Content-Security-Policy"), "connect-src 'self' ws://127.0.0.1:3000")
	assert.Equal(t, "nosniff", response.Header().Get("X-Content-Type-Options"))
	assert.Equal(t, "no-store", response.Header().Get("Cache-Control"))
	body := response.Body.String()
	controlsIndex := strings.Index(body, `class="controls"`)
	transcriptIndex := strings.Index(body, `id="transcript"`)
	require.GreaterOrEqual(t, controlsIndex, 0)
	require.GreaterOrEqual(t, transcriptIndex, 0)
	assert.Less(t, controlsIndex, transcriptIndex)
	assert.Contains(t, body, "This page connects to a loopback BFF")
	assert.Contains(t, body, "Audio is forwarded to configured providers")
	assert.Contains(t, body, "This demo does not persist audio or transcripts")
	transcriptTag := body[transcriptIndex : transcriptIndex+strings.Index(body[transcriptIndex:], ">")+1]
	assert.NotContains(t, transcriptTag, "aria-live")
	assert.Contains(t, body, `id="final-announcement" class="visually-hidden" aria-live="polite" aria-atomic="true"`)
	assert.Contains(t, body, `id="mute" class="button button-secondary" type="button" disabled aria-pressed="false"`)
	assert.Contains(t, body, `id="resume-audio"`)
	assert.NotContains(t, body, "sentinel-browser-secret")
	assert.NotContains(t, fmtHeaders(response.Header()), "sentinel-browser-secret")
}

func TestJavaScriptAssetsAreServedAsNoStoreJavaScript(t *testing.T) {
	handler := mustTestHandler(t, nil, []byte("sentinel"))
	for _, path := range []string{"/audio-core.mjs", "/browser-runtime.mjs", "/client-state.mjs", "/protocol-state.mjs", "/tool-state.mjs", "/app.js", "/capture-worklet.js", "/playback-worklet.js"} {
		path := path
		t.Run(path, func(t *testing.T) {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "http://127.0.0.1:3000"+path, nil))
			assert.Equal(t, http.StatusOK, response.Code)
			assert.Equal(t, "text/javascript; charset=utf-8", response.Header().Get("Content-Type"))
			assert.Equal(t, "no-store", response.Header().Get("Cache-Control"))
			assert.Contains(t, response.Header().Get("Content-Security-Policy"), "default-src 'self'")
		})
	}
}

func TestStaticPageKeepsVoiceControlsVisibleBesideAccessibleToolsDrawer(t *testing.T) {
	handler := mustTestHandler(t, nil, []byte("sentinel"))
	request := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:3000/", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	body := response.Body.String()
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Contains(t, body, `<details class="tool-drawer">`)
	assert.Contains(t, body, `id="tool-web-search"`)
	assert.Contains(t, body, `id="tool-safe-search"`)
	assert.Contains(t, body, `id="tool-region"`)
	assert.Contains(t, body, `id="tool-max-results"`)
	assert.Contains(t, body, `id="tool-timeout"`)
	assert.Contains(t, body, `id="tool-max-results" name="max_results" type="number" min="1" max="5"`)
	assert.Contains(t, body, `id="tool-timeout" name="timeout_ms" type="number" min="1000" max="8000"`)
	assert.Contains(t, body, `deepseek-v4-flash · thinking disabled`)
	assert.Contains(t, body, `id="tool-announcement" class="visually-hidden" aria-live="polite"`)
	assert.Less(t, strings.Index(body, `id="start"`), strings.Index(body, `<details class="tool-drawer">`))
}

func TestHealthAndUnknownRoutesAreExact(t *testing.T) {
	handler := mustTestHandler(t, nil, []byte("sentinel"))
	ready := httptest.NewRecorder()
	handler.ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "http://127.0.0.1:3000/health/ready", nil))
	assert.Equal(t, http.StatusOK, ready.Code)
	assert.Equal(t, "ready\n", ready.Body.String())

	missing := httptest.NewRecorder()
	handler.ServeHTTP(missing, httptest.NewRequest(http.MethodGet, "http://127.0.0.1:3000/not-found", nil))
	assert.Equal(t, http.StatusNotFound, missing.Code)
}

func TestWebSocketRejectsForeignOriginBeforeDial(t *testing.T) {
	var calls atomic.Int32
	handler := mustTestHandler(t, func(context.Context, *url.URL, http.Header) (*websocket.Conn, error) {
		calls.Add(1)
		return nil, errors.New("must not dial")
	}, []byte("sentinel"))
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	header := http.Header{"Origin": []string{"http://evil.invalid"}}
	_, response, err := websocket.Dial(t.Context(), wsURL(server.URL)+"/ws", &websocket.DialOptions{HTTPHeader: header})
	require.Error(t, err)
	require.NotNil(t, response)
	assert.Equal(t, http.StatusForbidden, response.StatusCode)
	assert.Zero(t, calls.Load())
}

func TestDefaultUpstreamDialRejectsRedirectWithoutContactingTargetOrForwardingBearer(t *testing.T) {
	var targetCalls atomic.Int32
	var targetAuthorization string
	var targetMu sync.Mutex
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		targetCalls.Add(1)
		targetMu.Lock()
		targetAuthorization = request.Header.Get("Authorization")
		targetMu.Unlock()
		writer.WriteHeader(http.StatusTeapot)
	}))
	t.Cleanup(target.Close)

	redirect := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, target.URL+"/escaped", http.StatusFound)
	}))
	t.Cleanup(redirect.Close)
	endpoint, err := url.Parse(wsURL(redirect.URL) + "/v1/realtime")
	require.NoError(t, err)

	conn, err := defaultDialUpstream(t.Context(), endpoint, http.Header{
		"Authorization": []string{"Bearer redirect-sentinel"},
	})
	require.Error(t, err)
	assert.Nil(t, conn)
	assert.Zero(t, targetCalls.Load())
	targetMu.Lock()
	defer targetMu.Unlock()
	assert.Empty(t, targetAuthorization)
}

func mustTestHandler(t *testing.T, dial dialUpstream, bearer []byte) http.Handler {
	t.Helper()
	cfg, err := parseConfig(nil)
	require.NoError(t, err)
	handler, err := newHandler(cfg, bearer, dial)
	require.NoError(t, err)
	return handler
}

func fmtHeaders(header http.Header) string {
	var builder strings.Builder
	for name, values := range header {
		builder.WriteString(name)
		builder.WriteString(strings.Join(values, ","))
	}
	return builder.String()
}

func wsURL(httpURL string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http")
}

func readBody(t *testing.T, response *http.Response) string {
	t.Helper()
	defer response.Body.Close()
	value, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	return string(value)
}
