package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIdleWorkerHandlerHTTPContract(t *testing.T) {
	server := httptest.NewServer(idleWorkerHandler())
	t.Cleanup(server.Close)

	for _, test := range []struct {
		name       string
		method     string
		path       string
		wantStatus int
		wantBody   string
	}{
		{name: "live get", method: http.MethodGet, path: "/health/live", wantStatus: http.StatusOK, wantBody: "ok\n"},
		{name: "live head", method: http.MethodHead, path: "/health/live", wantStatus: http.StatusOK},
		{name: "ready get", method: http.MethodGet, path: "/health/ready", wantStatus: http.StatusServiceUnavailable},
		{name: "ready head", method: http.MethodHead, path: "/health/ready", wantStatus: http.StatusServiceUnavailable},
		{name: "live mutation", method: http.MethodPost, path: "/health/live", wantStatus: http.StatusMethodNotAllowed},
		{name: "ready mutation", method: http.MethodDelete, path: "/health/ready", wantStatus: http.StatusMethodNotAllowed},
		{name: "unknown path", method: http.MethodGet, path: "/other", wantStatus: http.StatusNotFound},
	} {
		t.Run(test.name, func(t *testing.T) {
			request, err := http.NewRequest(test.method, server.URL+test.path, nil)
			require.NoError(t, err)
			response, err := server.Client().Do(request)
			require.NoError(t, err)
			t.Cleanup(func() { _ = response.Body.Close() })

			body, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			assert.Equal(t, test.wantStatus, response.StatusCode)
			assert.Equal(t, test.wantBody, string(body))
		})
	}
}

func TestIdleRuntimeCloseClosesOwnedListenerAndIsIdempotent(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	runtime := newIdleRuntime(listener)

	done := make(chan error, 1)
	go func() { done <- runtime.Run(t.Context()) }()

	require.Eventually(t, func() bool {
		connection, err := net.DialTimeout("tcp", listener.Addr().String(), 100*time.Millisecond)
		if err != nil {
			return false
		}
		_ = connection.Close()
		return true
	}, time.Second, 10*time.Millisecond)

	runtime.Close()
	runtime.Close()
	require.NoError(t, <-done)

	connection, err := net.DialTimeout("tcp", listener.Addr().String(), 100*time.Millisecond)
	assert.Error(t, err)
	if connection != nil {
		_ = connection.Close()
	}
}

func TestBuildIdleRuntimeBindsLoopbackDiagnostics(t *testing.T) {
	built, err := buildIdleRuntime(context.Background())
	require.NoError(t, err)
	t.Cleanup(built.Close)

	runtime, ok := built.(*idleRuntime)
	require.True(t, ok)
	assert.Equal(t, idleWorkerListenAddress, runtime.listener.Addr().String())
}

func TestIdleRuntimeForcesPartialHeaderConnectionClosedAtShutdownBound(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	observed := &idleObservedAcceptListener{Listener: listener, accepted: make(chan struct{})}
	runtime := newIdleRuntime(observed)
	assert.Greater(t, runtime.server.ReadHeaderTimeout, time.Duration(0))
	assert.LessOrEqual(t, runtime.server.ReadHeaderTimeout, idleWorkerShutdownTimeout)
	runtime.shutdownTimeout = 20 * time.Millisecond
	runtime.server.ReadHeaderTimeout = time.Second

	done := make(chan error, 1)
	go func() { done <- runtime.Run(t.Context()) }()
	connection, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = connection.Close() })
	_, err = io.WriteString(connection, "GET /health/live HTTP/1.1\r\nHost:")
	require.NoError(t, err)
	select {
	case <-observed.accepted:
	case <-time.After(time.Second):
		t.Fatal("idle HTTP server did not accept the partial-header connection")
	}

	started := time.Now()
	runtime.Close()
	assert.Less(t, time.Since(started), 250*time.Millisecond)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("idle runtime did not join after forced server close")
	}
	require.NoError(t, connection.SetReadDeadline(time.Now().Add(100*time.Millisecond)))
	buffer := make([]byte, 1)
	_, err = connection.Read(buffer)
	require.Error(t, err, "accepted partial-header connection remained open after shutdown")
}

type idleObservedAcceptListener struct {
	net.Listener
	accepted chan struct{}
	once     sync.Once
}

func (l *idleObservedAcceptListener) Accept() (net.Conn, error) {
	connection, err := l.Listener.Accept()
	if err == nil {
		l.once.Do(func() { close(l.accepted) })
	}
	return connection, err
}
