package main

import (
	"context"
	"errors"
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

func TestProxyRelaysUpstreamTextToBrowser(t *testing.T) {
	conn := dialDemoWithUpstreamAction(t, upstreamAction{
		kind: websocket.MessageText, payload: []byte(`{"type":"session.created"}`),
	})
	t.Cleanup(func() { _ = conn.CloseNow() })

	kind, payload, err := conn.Read(t.Context())
	require.NoError(t, err)
	assert.Equal(t, websocket.MessageText, kind)
	assert.JSONEq(t, `{"type":"session.created"}`, string(payload))
}

func TestProxyRejectsUpstreamBinaryFrames(t *testing.T) {
	conn := dialDemoWithUpstreamAction(t, upstreamAction{
		kind: websocket.MessageBinary, payload: []byte{1},
	})
	t.Cleanup(func() { _ = conn.CloseNow() })

	_, _, err := conn.Read(t.Context())
	requireBrowserClose(t, err, websocket.StatusUnsupportedData, "text frames required")
}

func TestProxyRejectsOversizedUpstreamTextFrames(t *testing.T) {
	conn := dialDemoWithUpstreamAction(t, upstreamAction{
		kind: websocket.MessageText, payload: []byte(strings.Repeat("x", 1025)),
	})
	t.Cleanup(func() { _ = conn.CloseNow() })

	_, _, err := conn.Read(t.Context())
	requireBrowserClose(t, err, websocket.StatusMessageTooBig, "frame too large")
}

func TestProxyRelaysNormalUpstreamClosure(t *testing.T) {
	conn := dialDemoWithUpstreamAction(t, upstreamAction{close: websocket.StatusNormalClosure})
	t.Cleanup(func() { _ = conn.CloseNow() })

	_, _, err := conn.Read(t.Context())
	requireBrowserClose(t, err, websocket.StatusNormalClosure, "peer closed")
}

func TestProxyBoundsAbnormalUpstreamFailure(t *testing.T) {
	conn := dialDemoWithUpstreamAction(t, upstreamAction{close: websocket.StatusAbnormalClosure})
	t.Cleanup(func() { _ = conn.CloseNow() })

	_, _, err := conn.Read(t.Context())
	requireBrowserClose(t, err, websocket.StatusInternalError, "relay unavailable")
}

func requireBrowserClose(t *testing.T, err error, code websocket.StatusCode, reason string) {
	t.Helper()
	require.Error(t, err)
	var closeError websocket.CloseError
	require.True(t, errors.As(err, &closeError))
	assert.Equal(t, code, closeError.Code)
	assert.Equal(t, reason, closeError.Reason)
}

type upstreamAction struct {
	kind    websocket.MessageType
	payload []byte
	close   websocket.StatusCode
}

func dialDemoWithUpstreamAction(t *testing.T, action upstreamAction) *websocket.Conn {
	t.Helper()
	upstream := newActionUpstream(t, action)
	demo := newDemoTestServer(t, upstream, []byte("sentinel"), 1024)
	conn, _ := dialDemo(t, demo.URL)
	return conn
}

func newActionUpstream(t *testing.T, action upstreamAction) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		conn, err := websocket.Accept(writer, request, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		defer conn.CloseNow()
		if action.close != 0 {
			if action.close == websocket.StatusAbnormalClosure {
				return
			}
			_ = conn.Close(action.close, "upstream failure")
			return
		}
		_ = conn.Write(request.Context(), action.kind, action.payload)
		_, _, _ = conn.Read(request.Context())
	}))
	t.Cleanup(server.Close)
	return wsURL(server.URL) + "/v1/realtime"
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

func TestUpstreamForwardsOnlyTheExactProfilePair(t *testing.T) {
	handler := mustTestHandler(t, nil, []byte("sentinel")).(*demoHandler)
	upstream, err := handler.upstreamWithProfile("profile=insurance&profile_version=3")
	require.NoError(t, err)
	query := upstream.Query()
	assert.Equal(t, []string{"insurance"}, query["profile"])
	assert.Equal(t, []string{"3"}, query["profile_version"])
	assert.Equal(t, []string{handler.cfg.Model}, query["model"])
	assert.Len(t, query, 3)
}

func TestUpstreamWithoutProfileKeepsTheFixedModelQueryOnly(t *testing.T) {
	handler := mustTestHandler(t, nil, []byte("sentinel")).(*demoHandler)
	upstream, err := handler.upstreamWithProfile("")
	require.NoError(t, err)
	assert.Equal(t, handler.upstream.RawQuery, upstream.RawQuery)
	assert.Equal(t, []string{handler.cfg.Model}, upstream.Query()["model"])
	assert.Len(t, upstream.Query(), 1)
}

func TestUpstreamRejectsHostileBrowserQueries(t *testing.T) {
	handler := mustTestHandler(t, nil, []byte("sentinel")).(*demoHandler)
	for name, rawQuery := range map[string]string{
		"unknown key":       "profile=insurance&profile_version=3&tenant=stolen",
		"model override":    "model=other-model",
		"reconnect claim":   "profile=insurance&profile_version=3&reconnect=token",
		"duplicate profile": "profile=insurance&profile=claims&profile_version=3",
		"missing version":   "profile=insurance",
		"missing id":        "profile_version=3",
		"zero version":      "profile=insurance&profile_version=0",
		"padded version":    "profile=insurance&profile_version=03",
		"negative version":  "profile=insurance&profile_version=-1",
		"reserved builtin":  "profile=builtin&profile_version=1",
		"traversal id":      "profile=../secrets&profile_version=1",
		"spaced id":         "profile=insurance%20sales&profile_version=1",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := handler.upstreamWithProfile(rawQuery)
			assert.Error(t, err)
		})
	}
}

func TestWebSocketForwardsSelectedProfileToGateway(t *testing.T) {
	queries := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		queries <- request.URL.RawQuery
		conn, err := websocket.Accept(writer, request, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		defer conn.CloseNow()
		<-request.Context().Done()
	}))
	t.Cleanup(server.Close)

	demo := newDemoTestServer(t, wsURL(server.URL)+"/v1/realtime", []byte("sentinel"), 64<<10)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	header := http.Header{"Origin": []string{"http://127.0.0.1:3000"}}
	conn, _, err := websocket.Dial(ctx, wsURL(demo.URL)+"/ws?profile=insurance&profile_version=3", &websocket.DialOptions{HTTPHeader: header})
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.CloseNow() })

	select {
	case rawQuery := <-queries:
		values, parseErr := url.ParseQuery(rawQuery)
		require.NoError(t, parseErr)
		assert.Equal(t, []string{"insurance"}, values["profile"])
		assert.Equal(t, []string{"3"}, values["profile_version"])
		assert.Equal(t, []string{"local"}, values["model"])
	case <-time.After(3 * time.Second):
		t.Fatal("gateway never received the upgraded request")
	}
}

func TestWebSocketRejectsUnknownBrowserQueryBeforeUpgrade(t *testing.T) {
	upstream, _ := newEchoUpstream(t, "Bearer sentinel")
	demo := newDemoTestServer(t, upstream, []byte("sentinel"), 64<<10)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	header := http.Header{"Origin": []string{"http://127.0.0.1:3000"}}
	conn, response, err := websocket.Dial(ctx, wsURL(demo.URL)+"/ws?tenant=stolen", &websocket.DialOptions{HTTPHeader: header})
	if conn != nil {
		_ = conn.CloseNow()
	}
	require.Error(t, err)
	require.NotNil(t, response)
	assert.Equal(t, http.StatusBadRequest, response.StatusCode)
}
