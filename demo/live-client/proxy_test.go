package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProxyInjectsBearerAndRelaysTextWithoutBrowserExposure(t *testing.T) {
	upstream, authorized := newEchoUpstream(t, "Bearer sentinel")
	demo := newDemoTestServer(t, upstream, []byte("sentinel"), 64<<10)
	conn, response := dialDemo(t, demo.URL)
	t.Cleanup(func() { _ = conn.CloseNow() })

	payload := []byte(`{"type":"response.create"}`)
	require.NoError(t, conn.Write(t.Context(), websocket.MessageText, payload))
	kind, echoed, err := conn.Read(t.Context())
	require.NoError(t, err)
	assert.Equal(t, websocket.MessageText, kind)
	assert.JSONEq(t, string(payload), string(echoed))
	assert.True(t, <-authorized)
	assert.NotContains(t, fmtHeaders(response.Header), "sentinel")
}

func TestProxyRejectsBinaryFrames(t *testing.T) {
	upstream, _ := newEchoUpstream(t, "Bearer sentinel")
	demo := newDemoTestServer(t, upstream, []byte("sentinel"), 1024)
	conn, _ := dialDemo(t, demo.URL)
	t.Cleanup(func() { _ = conn.CloseNow() })

	require.NoError(t, conn.Write(t.Context(), websocket.MessageBinary, []byte{1}))
	_, _, err := conn.Read(t.Context())
	require.Error(t, err)
	assert.Equal(t, websocket.StatusUnsupportedData, websocket.CloseStatus(err))
}

func TestProxyRejectsOversizedTextFrames(t *testing.T) {
	upstream, _ := newEchoUpstream(t, "Bearer sentinel")
	demo := newDemoTestServer(t, upstream, []byte("sentinel"), 1024)
	conn, _ := dialDemo(t, demo.URL)
	t.Cleanup(func() { _ = conn.CloseNow() })

	require.NoError(t, conn.Write(t.Context(), websocket.MessageText, []byte(strings.Repeat("x", 1025))))
	_, _, err := conn.Read(t.Context())
	require.Error(t, err)
	assert.Equal(t, websocket.StatusMessageTooBig, websocket.CloseStatus(err))
}

func newEchoUpstream(t *testing.T, expectedAuthorization string) (string, <-chan bool) {
	t.Helper()
	authorized := make(chan bool, 4)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		authorized <- request.Header.Get("Authorization") == expectedAuthorization
		conn, err := websocket.Accept(writer, request, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		defer conn.CloseNow()
		kind, payload, err := conn.Read(request.Context())
		if err != nil {
			return
		}
		_ = conn.Write(request.Context(), kind, payload)
	}))
	t.Cleanup(server.Close)
	return wsURL(server.URL) + "/v1/realtime", authorized
}

func newDemoTestServer(t *testing.T, upstream string, bearer []byte, maxFrameBytes int64) *httptest.Server {
	t.Helper()
	cfg, err := parseConfig([]string{"-gateway-url", upstream})
	require.NoError(t, err)
	cfg.MaxFrameBytes = maxFrameBytes
	handler, err := newHandler(cfg, bearer, defaultDialUpstream)
	require.NoError(t, err)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

func dialDemo(t *testing.T, demoURL string) (*websocket.Conn, *http.Response) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	header := http.Header{"Origin": []string{"http://127.0.0.1:3000"}}
	conn, response, err := websocket.Dial(ctx, wsURL(demoURL)+"/ws", &websocket.DialOptions{HTTPHeader: header})
	require.NoError(t, err, "response body: %s", responseBody(response))
	return conn, response
}

func responseBody(response *http.Response) string {
	if response == nil || response.Body == nil {
		return ""
	}
	defer response.Body.Close()
	body := make([]byte, 256)
	count, _ := response.Body.Read(body)
	return string(body[:count])
}

var _ dialUpstream = func(context.Context, *url.URL, http.Header) (*websocket.Conn, error) { return nil, nil }
