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

func newEchoUpstream(t *testing.T, expectedAuthorization string) (*httptest.Server, <-chan bool) {
	t.Helper()
	authorized := make(chan bool, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorized <- r.Header.Get("Authorization") == expectedAuthorization
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			kind, payload, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			if err := conn.Write(r.Context(), kind, payload); err != nil {
				return
			}
		}
	}))
	return server, authorized
}

func newDemoTestServer(t *testing.T, gatewayURL string, bearer []byte) *httptest.Server {
	t.Helper()
	cfg := defaultConfig()
	cfg.GatewayURL = wsURL(gatewayURL) + "/v1/realtime"
	handler, err := newHandler(cfg, bearer, upstreamDependencies{})
	require.NoError(t, err)
	return httptest.NewServer(handler)
}

func dialDemo(t *testing.T, baseURL, origin string) *websocket.Conn {
	t.Helper()
	conn, response, err := websocket.Dial(t.Context(), wsURL(baseURL)+"/ws", &websocket.DialOptions{HTTPHeader: http.Header{"Origin": []string{origin}}})
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}
	require.NoError(t, err)
	return conn
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

func wsURL(baseURL string) string {
	return "ws" + strings.TrimPrefix(baseURL, "http")
}

func TestProxyInjectsBearerAndRelaysText(t *testing.T) {
	upstream, authorized := newEchoUpstream(t, "Bearer sentinel")
	defer upstream.Close()
	demo := newDemoTestServer(t, upstream.URL, []byte("sentinel"))
	defer demo.Close()
	conn := dialDemo(t, demo.URL, "http://127.0.0.1:3000")
	defer conn.CloseNow()
	writeText(t, conn, []byte(`{"type":"session.update","session":{"type":"realtime"}}`))
	assert.JSONEq(t, `{"type":"session.update","session":{"type":"realtime"}}`, string(readText(t, conn)))
	assert.True(t, <-authorized)
}

func TestProxyClosesUnsupportedAndOversizedFrames(t *testing.T) {
	for _, test := range []struct {
		name         string
		fromUpstream bool
		messageType  websocket.MessageType
		payload      []byte
		maxBytes     int64
		wantStatus   websocket.StatusCode
	}{
		{name: "browser binary", messageType: websocket.MessageBinary, payload: []byte{1}, maxBytes: 960, wantStatus: websocket.StatusUnsupportedData},
		{name: "upstream binary", fromUpstream: true, messageType: websocket.MessageBinary, payload: []byte{1}, maxBytes: 960, wantStatus: websocket.StatusUnsupportedData},
		{name: "browser text too large", messageType: websocket.MessageText, payload: make([]byte, 961), maxBytes: 960, wantStatus: websocket.StatusMessageTooBig},
		{name: "upstream text too large", fromUpstream: true, messageType: websocket.MessageText, payload: make([]byte, 961), maxBytes: 960, wantStatus: websocket.StatusMessageTooBig},
	} {
		t.Run(test.name, func(t *testing.T) {
			upstream := frameUpstream(t, test.fromUpstream, test.messageType, test.payload)
			defer upstream.Close()
			cfg := defaultConfig()
			cfg.GatewayURL = wsURL(upstream.URL) + "/v1/realtime"
			cfg.MaxFrameBytes = test.maxBytes
			handler, err := newHandler(cfg, nil, upstreamDependencies{})
			require.NoError(t, err)
			demo := httptest.NewServer(handler)
			defer demo.Close()
			conn := dialDemo(t, demo.URL, cfg.Origin)
			defer conn.CloseNow()
			if !test.fromUpstream {
				require.NoError(t, conn.Write(t.Context(), test.messageType, test.payload))
			}
			_, _, err = conn.Read(t.Context())
			require.Error(t, err)
			assert.Equal(t, test.wantStatus, websocket.CloseStatus(err))
		})
	}
}

func TestProxyNormalCloseDoesNotExposeUpstreamBodyOrBearer(t *testing.T) {
	const secret = "sentinel"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		require.NoError(t, err)
		defer conn.CloseNow()
		require.NoError(t, conn.Close(websocket.StatusNormalClosure, "upstream body "+secret))
	}))
	defer upstream.Close()
	demo := newDemoTestServer(t, upstream.URL, []byte(secret))
	defer demo.Close()
	conn := dialDemo(t, demo.URL, "http://127.0.0.1:3000")
	defer conn.CloseNow()
	_, _, err := conn.Read(t.Context())
	require.Error(t, err)
	assert.Equal(t, websocket.StatusNormalClosure, websocket.CloseStatus(err))
	assert.NotContains(t, err.Error(), secret)
	assert.NotContains(t, err.Error(), "upstream body")
}

func frameUpstream(t *testing.T, sendImmediately bool, kind websocket.MessageType, payload []byte) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		require.NoError(t, err)
		defer conn.CloseNow()
		if sendImmediately {
			require.NoError(t, conn.Write(r.Context(), kind, payload))
			<-r.Context().Done()
			return
		}
		_, _, _ = conn.Read(r.Context())
		<-time.After(10 * time.Millisecond)
	}))
}

func TestCopyTextReturnsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		require.NoError(t, err)
		defer conn.CloseNow()
		<-r.Context().Done()
	}))
	defer server.Close()
	upstream, response, err := websocket.Dial(t.Context(), wsURL(server.URL), nil)
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}
	require.NoError(t, err)
	defer upstream.CloseNow()
	assert.ErrorIs(t, copyText(ctx, upstream, upstream, 960), context.Canceled)
}

func TestDefaultDialUsesFreshHeaders(t *testing.T) {
	var first, second http.Header
	dial := func(_ context.Context, _ *url.URL, header http.Header) (*websocket.Conn, error) {
		if first == nil {
			first = header
			first.Set("X-Test", "mutated")
		} else {
			second = header
		}
		return nil, assert.AnError
	}
	handler := mustTestHandlerWithBearer(t, []byte("sentinel"), upstreamDependencies{Dial: dial})
	for range 2 {
		server := httptest.NewServer(handler)
		conn := dialDemo(t, server.URL, "http://127.0.0.1:3000")
		_, _, _ = conn.Read(t.Context())
		conn.CloseNow()
		server.Close()
	}
	require.NotNil(t, second)
	assert.Empty(t, second.Get("X-Test"))
}
