package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOperatorMainClientStopLeavesPersistentOperatorSurfaceForRestart(t *testing.T) {
	gateway := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Upgrade") != "websocket" {
			writer.WriteHeader(http.StatusOK)
			return
		}
		connection, err := websocket.Accept(writer, request, &websocket.AcceptOptions{InsecureSkipVerify: true})
		require.NoError(t, err)
		defer connection.CloseNow()
		<-request.Context().Done()
	}))
	t.Cleanup(gateway.Close)
	cfg := testOperatorMainConfig(t)
	cfg.GatewayURL = wsURL(gateway.URL) + "/v1/realtime"
	cfg.Operator.Readiness[componentGateway] = gateway.URL
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg, []byte("operator-main-bearer")) }()
	waitForOperatorMainReady(t, cfg.Origin)
	header := http.Header{}
	header.Set("Origin", cfg.Origin)
	browser, upgraded, err := websocket.Dial(t.Context(), "ws"+strings.TrimPrefix(cfg.Origin, "http")+"/ws", &websocket.DialOptions{HTTPHeader: header})
	require.NoError(t, err)
	require.NotNil(t, upgraded)
	assert.Equal(t, http.StatusSwitchingProtocols, upgraded.StatusCode)

	request, err := http.NewRequest(http.MethodPost, cfg.Origin+"/api/operator/action", strings.NewReader(`{"component":"client","action":"stop"}`))
	require.NoError(t, err)
	request.Header.Set("Origin", cfg.Origin)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, response.StatusCode)
	_ = response.Body.Close()

	waitForOperatorMainReady(t, cfg.Origin)
	response, err = http.Get(cfg.Origin + "/")
	require.NoError(t, err)
	assert.Equal(t, http.StatusServiceUnavailable, response.StatusCode)
	_ = response.Body.Close()
	readContext, stopRead := context.WithTimeout(t.Context(), time.Second)
	defer stopRead()
	_, _, err = browser.Read(readContext)
	require.Error(t, err, "Client Stop must close an already-admitted browser WebSocket")
	_, rejected, err := websocket.Dial(t.Context(), "ws"+strings.TrimPrefix(cfg.Origin, "http")+"/ws", &websocket.DialOptions{HTTPHeader: header})
	require.Error(t, err)
	require.NotNil(t, rejected)
	assert.Equal(t, http.StatusServiceUnavailable, rejected.StatusCode)
	_ = rejected.Body.Close()

	request, err = http.NewRequest(http.MethodPost, cfg.Origin+"/api/operator/action", strings.NewReader(`{"component":"client","action":"start"}`))
	require.NoError(t, err)
	request.Header.Set("Origin", cfg.Origin)
	request.Header.Set("Content-Type", "application/json")
	response, err = http.DefaultClient.Do(request)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, response.StatusCode)
	_ = response.Body.Close()
	response, err = http.Get(cfg.Origin + "/")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, response.StatusCode)
	_ = response.Body.Close()
	browser, upgraded, err = websocket.Dial(t.Context(), "ws"+strings.TrimPrefix(cfg.Origin, "http")+"/ws", &websocket.DialOptions{HTTPHeader: header})
	require.NoError(t, err)
	require.NotNil(t, upgraded)
	assert.Equal(t, http.StatusSwitchingProtocols, upgraded.StatusCode)
	_ = browser.Close(websocket.StatusNormalClosure, "done")
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("operator server did not shut down")
	}
}

func TestOperatorClientStopRejectsWebSocketUntilRestart(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, nil)
		require.NoError(t, err)
		defer connection.CloseNow()
		<-request.Context().Done()
	}))
	t.Cleanup(upstream.Close)
	config, err := parseConfig(nil)
	require.NoError(t, err)
	handler, err := newHandler(config, []byte("operator-client-bearer"), func(ctx context.Context, _ *url.URL, header http.Header) (*websocket.Conn, error) {
		assert.Equal(t, "Bearer operator-client-bearer", header.Get("Authorization"))
		connection, _, err := websocket.Dial(ctx, wsURL(upstream.URL), nil)
		return connection, err
	})
	require.NoError(t, err)
	demo := handler.(*demoHandler)
	runner := newFakeOperatorRunner()
	runner.setReady(componentClient, true)
	runner.setOwned(componentClient, true)
	runner.setReady(componentGateway, true)
	demo.operator = newOperatorSupervisor(runner, time.Second)
	server := httptest.NewServer(demo)
	t.Cleanup(server.Close)

	stop := makeOperatorActionRequest(demo, http.MethodPost, `{"component":"client","action":"stop"}`, config.Origin, "application/json")
	require.Equal(t, http.StatusOK, stop.Code)

	header := http.Header{}
	header.Set("Origin", config.Origin)
	_, rejected, err := websocket.Dial(t.Context(), wsURL(server.URL)+"/ws", &websocket.DialOptions{HTTPHeader: header})
	require.Error(t, err)
	require.NotNil(t, rejected)
	assert.Equal(t, http.StatusServiceUnavailable, rejected.StatusCode)
	_ = rejected.Body.Close()

	start := makeOperatorActionRequest(demo, http.MethodPost, `{"component":"client","action":"start"}`, config.Origin, "application/json")
	require.Equal(t, http.StatusOK, start.Code)
	connection, admitted, err := websocket.Dial(t.Context(), wsURL(server.URL)+"/ws", &websocket.DialOptions{HTTPHeader: header})
	require.NoError(t, err)
	require.NotNil(t, admitted)
	assert.Equal(t, http.StatusSwitchingProtocols, admitted.StatusCode)
	_ = connection.Close(websocket.StatusNormalClosure, "done")
}

func TestOperatorMainGracefulShutdownStopsOwnedSpeech(t *testing.T) {
	cfg := testOperatorMainConfig(t)
	speechReady := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(speechReady.Close)
	cfg.Operator.Readiness[componentSpeech] = speechReady.URL + "/v1/pool"
	t.Setenv("S2S_API_KEY", "operator-main-speech-secret")

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg, []byte("operator-main-bearer")) }()
	waitForOperatorMainReady(t, cfg.Origin)

	request, err := http.NewRequest(http.MethodPost, cfg.Origin+"/api/operator/action", strings.NewReader(`{"component":"speech","action":"start"}`))
	require.NoError(t, err)
	request.Header.Set("Origin", cfg.Origin)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, response.StatusCode)
	_ = response.Body.Close()

	pidPath := filepath.Join(cfg.Operator.StateDir, "speech.pid")
	var pid int
	require.Eventually(t, func() bool {
		content, err := os.ReadFile(pidPath)
		if err != nil {
			return false
		}
		fields := strings.Fields(string(content))
		if len(fields) != 2 {
			return false
		}
		pid, err = strconv.Atoi(fields[0])
		return err == nil && pid > 0
	}, time.Second, 10*time.Millisecond)

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("graceful shutdown did not complete")
	}
	require.Eventually(t, func() bool {
		process, err := os.FindProcess(pid)
		return err != nil || process.Signal(syscall.Signal(0)) != nil
	}, 2*time.Second, 10*time.Millisecond, "operator-owned speech process survived BFF shutdown")
}

func TestOperatorShutdownJoinsInFlightActionBeforeIndependentReverseCleanup(t *testing.T) {
	runner := newFakeOperatorRunner()
	for _, component := range []operatorComponent{componentSpeech, componentPostgres, componentGateway, componentWorker} {
		runner.setReady(component, true)
		runner.setOwned(component, true)
	}
	runner.setMigrated(true)
	runner.block = make(chan struct{})
	supervisor := newOperatorSupervisor(runner, time.Second)
	supervisor.migrated = true

	actionDone := make(chan componentStatus, 1)
	go func() { actionDone <- supervisor.Apply(context.Background(), componentWorker, actionStart) }()
	require.Eventually(t, func() bool { return runner.callsFor(componentWorker, actionStart) == 1 }, time.Second, time.Millisecond)

	shutdownDone := make(chan struct{})
	go func() {
		waitOperatorActions(supervisor)
		shutdownOwnedOperator(supervisor)
		close(shutdownDone)
	}()
	time.Sleep(20 * time.Millisecond)
	assert.Zero(t, runner.callsFor(componentGateway, actionStop), "cleanup must not race an in-flight action")

	close(runner.block)
	<-actionDone
	require.Eventually(t, func() bool {
		return runner.callsFor(componentWorker, actionStop) == 1 && runner.callsFor(componentGateway, actionStop) == 1 && runner.callsFor(componentPostgres, actionStop) == 1 && runner.callsFor(componentSpeech, actionStop) == 1
	}, time.Second, time.Millisecond)
	<-shutdownDone
}

func testOperatorMainConfig(t *testing.T) demoConfig {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())

	repository := t.TempDir()
	compose := filepath.Join(repository, "deploy", "scripts", "durable-compose.sh")
	require.NoError(t, os.MkdirAll(filepath.Dir(compose), 0o700))
	require.NoError(t, os.WriteFile(compose, []byte("#!/bin/sh\nexit 0\n"), 0o700))
	speech := filepath.Join(repository, "speech-launcher.sh")
	require.NoError(t, os.WriteFile(speech, []byte("#!/bin/sh\ntrap 'exit 0' TERM INT\nwhile :; do sleep 1; done\n"), 0o700))
	profile := filepath.Join(repository, "speech-profile.yaml")
	require.NoError(t, os.WriteFile(profile, []byte("transport: websocket\n"), 0o600))

	cfg, err := parseConfig([]string{
		"-listen", address,
		"-origin", "http://" + address,
		"-operator",
		"-operator-state-dir", filepath.Join(repository, "operator-state"),
		"-operator-repository-root", repository,
		"-operator-speech-command", speech,
		"-operator-speech-profile", profile,
		"-operator-client-ready-url", "http://" + address + "/health/ready",
	})
	require.NoError(t, err)
	return cfg
}

func waitForOperatorMainReady(t *testing.T, origin string) {
	t.Helper()
	require.Eventually(t, func() bool {
		response, err := http.Get(origin + "/health/ready")
		if err != nil {
			return false
		}
		_ = response.Body.Close()
		return response.StatusCode == http.StatusOK
	}, 2*time.Second, 10*time.Millisecond, fmt.Sprintf("operator server %s did not become ready", origin))
}

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

func TestSetupAssetsAreNoStoreAndRejectMutations(t *testing.T) {
	handler := mustTestHandler(t, nil, []byte("sentinel-browser-secret"))
	for _, path := range []string{"/setup", "/setup.css", "/setup.mjs"} {
		path := path
		t.Run(path, func(t *testing.T) {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "http://127.0.0.1:3000"+path, nil))
			assert.Equal(t, http.StatusMethodNotAllowed, response.Code)
			assert.Equal(t, "no-store", response.Header().Get("Cache-Control"))
			assert.Contains(t, response.Header().Get("Content-Security-Policy"), "connect-src 'self'")
			assert.Empty(t, response.Header().Get("Access-Control-Allow-Origin"))
			assert.NotContains(t, response.Body.String()+fmtHeaders(response.Header()), "sentinel-browser-secret")
			assert.NotContains(t, response.Body.String()+fmtHeaders(response.Header()), "postgres://")
			assert.NotContains(t, response.Body.String()+fmtHeaders(response.Header()), "S2S_API_KEY")
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
