package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOperatorAPIStatusReturnsOnlyFixedSnapshotSchema(t *testing.T) {
	handler, runner := newOperatorAPIHandler(t)
	runner.ready[componentSpeech] = true

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/operator/status", nil))

	require.Equal(t, http.StatusOK, response.Code)
	assert.Equal(t, "application/json; charset=utf-8", response.Header().Get("Content-Type"))
	assert.Equal(t, "no-store", response.Header().Get("Cache-Control"))
	assert.JSONEq(t, `{"components":[{"component":"speech","state":"ready","code":"ready","ready":true,"busy":false,"owned":false},{"component":"postgres","state":"stopped","code":"stopped","ready":false,"busy":false,"owned":false},{"component":"gateway","state":"stopped","code":"stopped","ready":false,"busy":false,"owned":false},{"component":"worker","state":"stopped","code":"stopped","ready":false,"busy":false,"owned":false},{"component":"client","state":"stopped","code":"stopped","ready":false,"busy":false,"owned":false}]}`, response.Body.String())
	assertContentFreeOperatorResponse(t, response, "sentinel-browser-secret")
}

func TestVoiceReadinessRejectsStoppedOperatorDependenciesAndAllowsReadyOrUnmanagedGateway(t *testing.T) {
	handler, runner := newOperatorAPIHandler(t)

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/voice-readiness", nil))
	assert.Equal(t, http.StatusServiceUnavailable, response.Code)
	assert.Empty(t, response.Body.String())

	for _, component := range []operatorComponent{componentSpeech, componentGateway, componentClient} {
		runner.ready[component] = true
	}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/voice-readiness", nil))
	assert.Equal(t, http.StatusNoContent, response.Code)
	assert.Empty(t, response.Body.String())

	unmanaged := mustTestHandler(t, nil, []byte("sentinel-browser-secret"))
	response = httptest.NewRecorder()
	unmanaged.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/voice-readiness", nil))
	assert.Equal(t, http.StatusNoContent, response.Code)
	assert.Empty(t, response.Body.String())
}

func TestOperatorAPIOwnedUnreadyComponentIsFailedAndCanBeStopped(t *testing.T) {
	handler, runner := newOperatorAPIHandler(t)
	runner.owned[componentSpeech] = true

	status := httptest.NewRecorder()
	handler.ServeHTTP(status, httptest.NewRequest(http.MethodGet, "/api/operator/status", nil))
	assert.Contains(t, status.Body.String(), `{"component":"speech","state":"failed","code":"process_exited","ready":false,"busy":false,"owned":true}`)

	response := makeOperatorActionRequest(handler, http.MethodPost, `{"component":"speech","action":"stop"}`, "http://127.0.0.1:3000", "application/json")
	assert.Equal(t, http.StatusOK, response.Code)
	assert.JSONEq(t, `{"component":"speech","state":"stopped","code":"stopped","ready":false,"busy":false,"owned":false}`, response.Body.String())
}

func TestOperatorAPIUnownedFailedStartCanRetryButCannotStop(t *testing.T) {
	handler, runner := newOperatorAPIHandler(t)
	runner.err = errors.New("local start failure")

	failed := makeOperatorActionRequest(handler, http.MethodPost, `{"component":"speech","action":"start"}`, "http://127.0.0.1:3000", "application/json")
	assert.Equal(t, http.StatusServiceUnavailable, failed.Code)
	assert.JSONEq(t, `{"component":"speech","state":"failed","code":"operation_failed","ready":false,"busy":false,"owned":false}`, failed.Body.String())

	stopped := makeOperatorActionRequest(handler, http.MethodPost, `{"component":"speech","action":"stop"}`, "http://127.0.0.1:3000", "application/json")
	assert.Equal(t, http.StatusConflict, stopped.Code)

	runner.err = nil
	retried := makeOperatorActionRequest(handler, http.MethodPost, `{"component":"speech","action":"start"}`, "http://127.0.0.1:3000", "application/json")
	assert.Equal(t, http.StatusOK, retried.Code)
	assert.JSONEq(t, `{"component":"speech","state":"ready","code":"ready","ready":true,"busy":false,"owned":true}`, retried.Body.String())
}

func TestOperatorAPIActionAcceptsOnlyExactValidJSON(t *testing.T) {
	handler, runner := newOperatorAPIHandler(t)

	response := makeOperatorActionRequest(handler, http.MethodPost, `{"component":"speech","action":"start"}`, "http://127.0.0.1:3000", "application/json; charset=utf-8")
	require.Equal(t, http.StatusOK, response.Code)
	assert.JSONEq(t, `{"component":"speech","state":"ready","code":"ready","ready":true,"busy":false,"owned":true}`, response.Body.String())
	assert.Equal(t, []fixedOperation{{Component: componentSpeech, Action: actionStart}}, runner.operations())

	for _, body := range []string{
		`{}`,
		`{"component":"speech"}`,
		`{"action":"start"}`,
		`{"component":"speech","action":"start","command":"not-allowed"}`,
		`{"component":"speech","component":"gateway","action":"start"}`,
		`{"component":"speech","action":"start"} {}`,
		`{"component":"speech","action":null}`,
		`[]`,
	} {
		response = makeOperatorActionRequest(handler, http.MethodPost, body, "http://127.0.0.1:3000", "application/json")
		assert.Equal(t, http.StatusBadRequest, response.Code, body)
		assert.Empty(t, response.Body.String(), body)
	}
}

func TestOperatorAPIActionRejectsForbiddenAndInvalidRequests(t *testing.T) {
	handler, _ := newOperatorAPIHandler(t)

	for _, test := range []struct {
		name        string
		method      string
		body        string
		origin      string
		contentType string
		status      int
	}{
		{name: "missing origin", method: http.MethodPost, body: `{"component":"speech","action":"start"}`, contentType: "application/json", status: http.StatusForbidden},
		{name: "foreign origin", method: http.MethodPost, body: `{"component":"speech","action":"start"}`, origin: "http://evil.invalid", contentType: "application/json", status: http.StatusForbidden},
		{name: "wrong method", method: http.MethodGet, status: http.StatusMethodNotAllowed},
		{name: "non json", method: http.MethodPost, body: `{"component":"speech","action":"start"}`, origin: "http://127.0.0.1:3000", contentType: "text/plain", status: http.StatusBadRequest},
		{name: "oversized", method: http.MethodPost, body: `{"component":"speech","action":"start","padding":"` + strings.Repeat("x", 4096) + `"}`, origin: "http://127.0.0.1:3000", contentType: "application/json", status: http.StatusBadRequest},
		{name: "unknown enum", method: http.MethodPost, body: `{"component":"shell","action":"start"}`, origin: "http://127.0.0.1:3000", contentType: "application/json", status: http.StatusUnprocessableEntity},
		{name: "invalid pair", method: http.MethodPost, body: `{"component":"speech","action":"migrate"}`, origin: "http://127.0.0.1:3000", contentType: "application/json", status: http.StatusUnprocessableEntity},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := makeOperatorActionRequest(handler, test.method, test.body, test.origin, test.contentType)
			assert.Equal(t, test.status, response.Code)
			assert.Empty(t, response.Body.String())
		})
	}
}

func TestOperatorAPIActionRejectsDuplicateOriginHeaders(t *testing.T) {
	handler, _ := newOperatorAPIHandler(t)
	request := httptest.NewRequest(http.MethodPost, "/api/operator/action", strings.NewReader(`{"component":"speech","action":"start"}`))
	request.Header.Add("Origin", "http://127.0.0.1:3000")
	request.Header.Add("Origin", "http://evil.invalid")
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	assert.Equal(t, http.StatusForbidden, response.Code)
	assert.Empty(t, response.Body.String())
}

func TestOperatorAPIReportsDependencyAndInFlightConflicts(t *testing.T) {
	handler, runner := newOperatorAPIHandler(t)

	response := makeOperatorActionRequest(handler, http.MethodPost, `{"component":"gateway","action":"start"}`, "http://127.0.0.1:3000", "application/json")
	require.Equal(t, http.StatusConflict, response.Code)
	assert.JSONEq(t, `{"component":"gateway","state":"failed","code":"dependency_not_ready","ready":false,"busy":false,"owned":false}`, response.Body.String())

	runner.block = make(chan struct{})
	completed := make(chan struct{})
	go func() {
		defer close(completed)
		makeOperatorActionRequest(handler, http.MethodPost, `{"component":"speech","action":"start"}`, "http://127.0.0.1:3000", "application/json")
	}()
	<-runner.started
	response = makeOperatorActionRequest(handler, http.MethodPost, `{"component":"speech","action":"start"}`, "http://127.0.0.1:3000", "application/json")
	assert.Equal(t, http.StatusConflict, response.Code)
	assert.JSONEq(t, `{"component":"speech","state":"starting","code":"operation_in_progress","ready":false,"busy":true,"owned":false}`, response.Body.String())
	close(runner.block)
	<-completed
}

func TestOperatorAPIRejectsDependencyInterleavingsDuringStops(t *testing.T) {
	t.Run("dependant start while required stop is in flight", func(t *testing.T) {
		handler, runner := newOperatorAPIHandler(t)
		for _, component := range []operatorComponent{componentSpeech, componentPostgres} {
			runner.ready[component], runner.owned[component] = true, true
		}
		handler.operator.migrated = true
		runner.blockStop = make(chan struct{})
		stopped := make(chan struct{})
		go func() {
			defer close(stopped)
			makeOperatorActionRequest(handler, http.MethodPost, `{"component":"speech","action":"stop"}`, "http://127.0.0.1:3000", "application/json")
		}()
		<-runner.stopStarted
		response := makeOperatorActionRequest(handler, http.MethodPost, `{"component":"gateway","action":"start"}`, "http://127.0.0.1:3000", "application/json")
		assert.Equal(t, http.StatusConflict, response.Code)
		assert.JSONEq(t, `{"component":"gateway","state":"failed","code":"dependency_not_ready","ready":false,"busy":false,"owned":false}`, response.Body.String())
		close(runner.blockStop)
		<-stopped
	})

	t.Run("parent stop while dependant stop is in flight", func(t *testing.T) {
		handler, runner := newOperatorAPIHandler(t)
		for _, component := range []operatorComponent{componentSpeech, componentPostgres, componentGateway} {
			runner.ready[component], runner.owned[component] = true, true
		}
		runner.blockStop = make(chan struct{})
		stopped := make(chan struct{})
		go func() {
			defer close(stopped)
			makeOperatorActionRequest(handler, http.MethodPost, `{"component":"gateway","action":"stop"}`, "http://127.0.0.1:3000", "application/json")
		}()
		<-runner.stopStarted
		for _, component := range []operatorComponent{componentSpeech, componentPostgres} {
			response := makeOperatorActionRequest(handler, http.MethodPost, `{"component":"`+string(component)+`","action":"stop"}`, "http://127.0.0.1:3000", "application/json")
			assert.Equal(t, http.StatusConflict, response.Code)
			assert.Contains(t, response.Body.String(), `"dependency_running"`)
		}
		close(runner.blockStop)
		<-stopped
	})
}

func TestOperatorAPIFailedActionsUseUnavailableStatus(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		code string
	}{
		{name: "missing configuration", err: errOperatorMissingConfiguration, code: "missing_configuration"},
		{name: "startup timeout", err: context.DeadlineExceeded, code: "startup_timeout"},
		{name: "operation failure", err: errors.New("private runner output"), code: "operation_failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler, runner := newOperatorAPIHandler(t)
			runner.err = test.err

			response := makeOperatorActionRequest(handler, http.MethodPost, `{"component":"speech","action":"start"}`, "http://127.0.0.1:3000", "application/json")

			assert.Equal(t, http.StatusServiceUnavailable, response.Code)
			assert.JSONEq(t, `{"component":"speech","state":"failed","code":"`+test.code+`","ready":false,"busy":false,"owned":false}`, response.Body.String())
		})
	}

	handler, _ := newOperatorAPIHandler(t)
	response := makeOperatorActionRequest(handler, http.MethodPost, `{"component":"speech","action":"stop"}`, "http://127.0.0.1:3000", "application/json")
	assert.Equal(t, http.StatusConflict, response.Code)
	assert.JSONEq(t, `{"component":"speech","state":"failed","code":"process_exited","ready":false,"busy":false,"owned":false}`, response.Body.String())
}

func TestOperatorAPIActionOutlivesDisconnectedRequest(t *testing.T) {
	handler, runner := newOperatorAPIHandler(t)
	serverContext, stopServer := context.WithCancel(context.Background())
	t.Cleanup(stopServer)
	handler.operatorContext = serverContext
	runner.block = make(chan struct{})

	requestContext, disconnect := context.WithCancel(context.Background())
	request := httptest.NewRequest(http.MethodPost, "/api/operator/action", strings.NewReader(`{"component":"speech","action":"start"}`)).WithContext(requestContext)
	request.Header.Set("Origin", "http://127.0.0.1:3000")
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	completed := make(chan struct{})
	go func() {
		defer close(completed)
		handler.ServeHTTP(response, request)
	}()
	<-runner.started
	disconnect()

	close(runner.block)
	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatal("operator request did not complete")
	}
	assert.Equal(t, http.StatusOK, response.Code)
	assert.True(t, runner.Ready(context.Background(), componentSpeech))
	assert.Equal(t, []fixedOperation{{Component: componentSpeech, Action: actionStart}}, runner.operations())
}

func TestOperatorAPIDisabledModeFailsClosed(t *testing.T) {
	disabled := mustTestHandler(t, nil, []byte("sentinel-browser-secret"))
	for _, path := range []string{"/api/operator/status"} {
		response := httptest.NewRecorder()
		disabled.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		assert.Equal(t, http.StatusServiceUnavailable, response.Code, path)
		assert.Empty(t, response.Body.String(), path)
	}
	response := makeOperatorActionRequest(disabled, http.MethodPost, `{"component":"speech","action":"start"}`, "http://127.0.0.1:3000", "application/json")
	assert.Equal(t, http.StatusServiceUnavailable, response.Code)
	assert.Empty(t, response.Body.String())

}

func newOperatorAPIHandler(t *testing.T) (*demoHandler, *operatorAPIRunner) {
	t.Helper()
	handler, err := newHandler(parseTestConfig(t), []byte("sentinel-browser-secret"), nil)
	require.NoError(t, err)
	demo := handler.(*demoHandler)
	runner := &operatorAPIRunner{ready: make(map[operatorComponent]bool), owned: make(map[operatorComponent]bool), started: make(chan struct{}), stopStarted: make(chan struct{})}
	demo.operator = newOperatorSupervisor(runner, 5*time.Second)
	return demo, runner
}

func parseTestConfig(t *testing.T) demoConfig {
	t.Helper()
	cfg, err := parseConfig(nil)
	require.NoError(t, err)
	return cfg
}

func makeOperatorActionRequest(handler http.Handler, method, body, origin, contentType string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, "/api/operator/action", strings.NewReader(body))
	if origin != "" {
		request.Header.Set("Origin", origin)
	}
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func assertContentFreeOperatorResponse(t *testing.T, response *httptest.ResponseRecorder, forbidden ...string) {
	t.Helper()
	all := response.Body.String() + fmtHeaders(response.Header())
	for _, value := range forbidden {
		assert.NotContains(t, all, value)
	}
}

type operatorAPIRunner struct {
	mu          sync.Mutex
	ready       map[operatorComponent]bool
	owned       map[operatorComponent]bool
	runs        []fixedOperation
	block       chan struct{}
	blockStop   chan struct{}
	started     chan struct{}
	stopStarted chan struct{}
	err         error
}

func (runner *operatorAPIRunner) Run(ctx context.Context, operation fixedOperation) error {
	runner.mu.Lock()
	runner.runs = append(runner.runs, operation)
	block := runner.block
	started := runner.started
	err := runner.err
	runner.mu.Unlock()
	if block != nil && operation.Action == actionStart {
		close(started)
		select {
		case <-block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if runner.blockStop != nil && operation.Action == actionStop {
		if runner.stopStarted != nil {
			close(runner.stopStarted)
		}
		select {
		case <-runner.blockStop:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err != nil {
		return err
	}
	runner.mu.Lock()
	runner.ready[operation.Component] = operation.Action != actionStop
	runner.owned[operation.Component] = operation.Action != actionStop
	runner.mu.Unlock()
	return nil
}

func (runner *operatorAPIRunner) Ready(_ context.Context, component operatorComponent) bool {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	return runner.ready[component]
}

func (runner *operatorAPIRunner) Owned(component operatorComponent) bool {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	return runner.owned[component]
}

func (runner *operatorAPIRunner) operations() []fixedOperation {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	return append([]fixedOperation(nil), runner.runs...)
}

var _ operatorRunner = (*operatorAPIRunner)(nil)
