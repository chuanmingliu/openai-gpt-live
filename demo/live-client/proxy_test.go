package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProxyInjectsBearerAndRelaysTextWithoutBrowserExposure(t *testing.T) {
	upstream, authorized := newEchoUpstream(t, "Bearer sentinel")
	demo := newDemoTestServer(t, upstream, []byte("sentinel"))
	conn := dialDemo(t, demo.URL, "http://127.0.0.1:3000")
	writeText(t, conn, []byte(`{"type":"response.create"}`))
	assert.JSONEq(t, `{"type":"response.create"}`, string(readText(t, conn)))
	assert.True(t, <-authorized)
}

func TestProxyRejectsBinaryAndOversizedFrames(t *testing.T) {
	upstream, _ := newEchoUpstream(t, "Bearer sentinel")
	demo := newDemoTestServer(t, upstream, []byte("sentinel"))
	binary := dialDemo(t, demo.URL, "http://127.0.0.1:3000")
	require.NoError(t, binary.Write(t.Context(), websocket.MessageBinary, []byte{1, 2, 3}))
	_, _, err := binary.Read(t.Context())
	require.Error(t, err)
	var closeErr websocket.CloseError
	require.ErrorAs(t, err, &closeErr)
	assert.Equal(t, websocket.StatusUnsupportedData, closeErr.Code)

	oversized := dialDemo(t, demo.URL, "http://127.0.0.1:3000")
	require.NoError(t, oversized.Write(t.Context(), websocket.MessageText, make([]byte, (64<<10)+1)))
	_, _, err = oversized.Read(t.Context())
	require.Error(t, err)
}

func newEchoUpstream(t *testing.T, wantAuth string) (*httptest.Server, <-chan bool) {
	t.Helper()
	authorized := make(chan bool, 8)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		select {
		case authorized <- request.Header.Get("Authorization") == wantAuth:
		default:
		}
		conn, err := websocket.Accept(writer, request, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			kind, payload, err := conn.Read(request.Context())
			if err != nil {
				return
			}
			if err := conn.Write(request.Context(), kind, payload); err != nil {
				return
			}
		}
	}))
	t.Cleanup(server.Close)
	return server, authorized
}

func newDemoTestServer(t *testing.T, upstream *httptest.Server, bearer []byte) *httptest.Server {
	t.Helper()
	cfg, err := parseConfig(nil)
	require.NoError(t, err)
	handler, err := newHandler(cfg, bearer, func(ctx context.Context, endpoint *url.URL, header http.Header) (*websocket.Conn, error) {
		assert.Equal(t, "gpt-live-simulator", endpoint.Query().Get("model"))
		conn, _, err := websocket.Dial(ctx, wsURL(upstream.URL), &websocket.DialOptions{HTTPHeader: header})
		return conn, err
	})
	require.NoError(t, err)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

func dialDemo(t *testing.T, raw, origin string) *websocket.Conn {
	t.Helper()
	header := http.Header{"Origin": []string{origin}}
	conn, response, err := websocket.Dial(t.Context(), wsURL(raw)+"/ws", &websocket.DialOptions{HTTPHeader: header})
	require.NoError(t, err)
	drain(t, response.Body)
	t.Cleanup(func() { _ = conn.CloseNow() })
	return conn
}
