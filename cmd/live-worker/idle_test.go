package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIdleWorkerHealthIsLiveButNotReady(t *testing.T) {
	handler := idleWorkerHandler()

	for _, test := range []struct {
		path string
		code int
		body string
	}{
		{path: "/health/live", code: http.StatusOK, body: "ok\n"},
		{path: "/health/ready", code: http.StatusServiceUnavailable, body: ""},
		{path: "/admin/v1/drain", code: http.StatusNotFound, body: "404 page not found\n"},
	} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, test.path, nil))
		assert.Equal(t, test.code, recorder.Code, test.path)
		assert.Equal(t, test.body, recorder.Body.String(), test.path)
		assert.Equal(t, "no-store", recorder.Header().Get("Cache-Control"), test.path)
	}
}

func TestIdleWorkerHealthRejectsMutationMethods(t *testing.T) {
	recorder := httptest.NewRecorder()
	idleWorkerHandler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/health/live", nil))
	assert.Equal(t, http.StatusMethodNotAllowed, recorder.Code)
	assert.Equal(t, "GET, HEAD", recorder.Header().Get("Allow"))
}

type blockingIdleListener struct {
	closed chan struct{}
	once   sync.Once
}

func newBlockingIdleListener() *blockingIdleListener {
	return &blockingIdleListener{closed: make(chan struct{})}
}

func (l *blockingIdleListener) Accept() (net.Conn, error) {
	<-l.closed
	return nil, net.ErrClosed
}

func (l *blockingIdleListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func (*blockingIdleListener) Addr() net.Addr { return idleTestAddr("idle") }

type idleTestAddr string

func (a idleTestAddr) Network() string { return "test" }
func (a idleTestAddr) String() string  { return string(a) }

func TestIdleRuntimeStaysAliveUntilCancellationAndCloses(t *testing.T) {
	listener := newBlockingIdleListener()
	runtime := newIdleRuntime(listener)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()

	select {
	case err := <-done:
		t.Fatalf("idle runtime stopped early: %v", err)
	default:
	}
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	runtime.Close()
	require.NoError(t, listener.Close())
}
