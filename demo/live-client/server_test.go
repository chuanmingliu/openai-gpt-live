package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustTestHandler(t *testing.T, dependencies upstreamDependencies) http.Handler {
	t.Helper()
	return mustTestHandlerWithBearer(t, nil, dependencies)
}

func mustTestHandlerWithBearer(t *testing.T, bearer []byte, dependencies upstreamDependencies) http.Handler {
	t.Helper()
	cfg := defaultConfig()
	handler, err := newHandler(cfg, bearer, dependencies)
	require.NoError(t, err)
	return handler
}

func TestStaticAndHealthRoutesUseStrictHeaders(t *testing.T) {
	handler := mustTestHandler(t, upstreamDependencies{})
	for _, path := range []string{"/", "/styles.css", "/health/live"} {
		r := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:3000"+path, nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		assert.Equal(t, http.StatusOK, w.Code, path)
		assert.Equal(t, "no-store", w.Header().Get("Cache-Control"))
		assert.Equal(t, "nosniff", w.Header().Get("X-Content-Type-Options"))
		assert.Contains(t, w.Header().Get("Content-Security-Policy"), "default-src 'self'")
	}
}

func TestHealthRouteReturnsExactLiveBody(t *testing.T) {
	handler := mustTestHandler(t, upstreamDependencies{})
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "http://127.0.0.1:3000/health/live", nil))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "ok\n", w.Body.String())
}

func TestStaticRoutesServeOnlyExplicitPaths(t *testing.T) {
	handler := mustTestHandler(t, upstreamDependencies{})
	for _, path := range []string{"/web/index.html", "/missing", "/styles.css/extra"} {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "http://127.0.0.1:3000"+path, nil))
		assert.Equal(t, http.StatusNotFound, w.Code, path)
	}
}

func TestBrowserAssetsServeExpectedContentTypes(t *testing.T) {
	handler := mustTestHandler(t, upstreamDependencies{})
	assets := []struct {
		path        string
		contentType string
	}{
		{path: "/", contentType: "text/html"},
		{path: "/styles.css", contentType: "text/css"},
		{path: "/app.js", contentType: "text/javascript"},
		{path: "/audio-core.mjs", contentType: "text/javascript"},
		{path: "/protocol-state.mjs", contentType: "text/javascript"},
		{path: "/capture-worklet.js", contentType: "text/javascript"},
		{path: "/playback-worklet.js", contentType: "text/javascript"},
	}

	for _, asset := range assets {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "http://127.0.0.1:3000"+asset.path, nil))
		assert.Equal(t, http.StatusOK, w.Code, asset.path)
		assert.Contains(t, w.Header().Get("Content-Type"), asset.contentType, asset.path)
	}
}

func TestBrowserDocumentAndCSPPermitOnlySelfHostedCode(t *testing.T) {
	handler := mustTestHandler(t, upstreamDependencies{})
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "http://127.0.0.1:3000/", nil))
	require.Equal(t, http.StatusOK, w.Code)

	document := w.Body.String()
	scripts := regexp.MustCompile(`(?is)<script\b([^>]*)>(.*?)</script>`).FindAllStringSubmatch(document, -1)
	require.NotEmpty(t, scripts)
	for _, script := range scripts {
		assert.Regexp(t, regexp.MustCompile(`(?i)\bsrc\s*=`), script[1])
		assert.Empty(t, strings.TrimSpace(script[2]))
	}
	lowerDocument := strings.ToLower(document)
	assert.NotContains(t, lowerDocument, "style=")
	assert.NotContains(t, lowerDocument, "http://")
	assert.NotContains(t, lowerDocument, "https://")

	csp := w.Header().Get("Content-Security-Policy")
	assert.Contains(t, csp, "script-src 'self'")
	assert.Contains(t, csp, "worker-src 'self'")
	assert.NotContains(t, csp, "unsafe-inline")
	assert.NotContains(t, csp, "unsafe-eval")
}

func TestCapabilitiesUsesOptionalAuthorizationWithoutExposingIt(t *testing.T) {
	seen := make(chan http.Header, 1)
	deps := upstreamDependencies{Fetch: func(_ context.Context, _ *url.URL, header http.Header) ([]byte, error) {
		seen <- header.Clone()
		return []byte(`{"protocol":"openai-realtime-subset","version":"1.0.0"}`), nil
	}}
	handler := mustTestHandlerWithBearer(t, []byte("sentinel"), deps)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "http://127.0.0.1:3000/api/capabilities", nil))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
	assert.Equal(t, "Bearer sentinel", (<-seen).Get("Authorization"))
	assert.NotContains(t, w.Body.String(), "sentinel")
}

func TestCapabilitiesOmitsAuthorizationWhenBearerIsEmpty(t *testing.T) {
	seen := make(chan http.Header, 1)
	handler := mustTestHandler(t, upstreamDependencies{Fetch: func(_ context.Context, _ *url.URL, header http.Header) ([]byte, error) {
		seen <- header.Clone()
		return []byte(`{}`), nil
	}})
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "http://127.0.0.1:3000/api/capabilities", nil))
	require.Equal(t, http.StatusOK, w.Code)
	assert.Empty(t, (<-seen).Get("Authorization"))
}

func TestCapabilitiesReturnsContentFreeBadGatewayForUpstreamFailure(t *testing.T) {
	handler := mustTestHandler(t, upstreamDependencies{Fetch: func(context.Context, *url.URL, http.Header) ([]byte, error) {
		return nil, errors.New("upstream body includes sentinel")
	}})
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "http://127.0.0.1:3000/api/capabilities", nil))
	assert.Equal(t, http.StatusBadGateway, w.Code)
	assert.Empty(t, w.Body.String())
}

func TestWebSocketRejectsForeignOriginBeforeDial(t *testing.T) {
	var calls atomic.Int32
	deps := upstreamDependencies{Dial: func(context.Context, *url.URL, http.Header) (*websocket.Conn, error) {
		calls.Add(1)
		return nil, errors.New("must not dial")
	}}
	server := httptest.NewServer(mustTestHandler(t, deps))
	defer server.Close()
	header := http.Header{"Origin": []string{"http://evil.invalid"}}
	_, response, err := websocket.Dial(t.Context(), wsURL(server.URL)+"/ws", &websocket.DialOptions{HTTPHeader: header})
	require.Error(t, err)
	require.NotNil(t, response)
	assert.Equal(t, http.StatusForbidden, response.StatusCode)
	assert.Zero(t, calls.Load())
}

func TestWebSocketFailureNeverExposesUpstreamBodyOrBearer(t *testing.T) {
	const secret = "sentinel"
	demo := httptest.NewServer(mustTestHandlerWithBearer(t, []byte(secret), upstreamDependencies{Dial: func(context.Context, *url.URL, http.Header) (*websocket.Conn, error) {
		return nil, errors.New("upstream response body: " + secret)
	}}))
	defer demo.Close()
	conn := dialDemo(t, demo.URL, "http://127.0.0.1:3000")
	defer conn.CloseNow()
	_, _, err := conn.Read(t.Context())
	require.Error(t, err)
	assert.Equal(t, websocket.StatusTryAgainLater, websocket.CloseStatus(err))
	assert.NotContains(t, err.Error(), secret)
	assert.NotContains(t, strings.ToLower(err.Error()), "upstream response body")
}
