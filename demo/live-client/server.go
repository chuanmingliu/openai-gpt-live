package main

import (
	"context"
	"embed"
	"errors"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/tools/websearch"
	"github.com/coder/websocket"
)

//go:embed web/*
var webAssets embed.FS

type dialUpstream func(context.Context, *url.URL, http.Header) (*websocket.Conn, error)

type demoHandler struct {
	cfg             demoConfig
	bearer          []byte
	upstream        *url.URL
	websocketOrigin string
	dial            dialUpstream
	tools           *toolPolicyStore
}

func newHandler(cfg demoConfig, bearer []byte, dial dialUpstream) (http.Handler, error) {
	search, err := websearch.New(net.DefaultResolver)
	if err != nil {
		return nil, errors.New("demo tool search is unavailable")
	}
	return newHandlerWithToolSearch(cfg, bearer, dial, search)
}

func newHandlerWithToolSearch(cfg demoConfig, bearer []byte, dial dialUpstream, search toolSearcher) (http.Handler, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if len(bearer) == 0 || len(bearer) > maximumBearerBytes {
		return nil, errors.New("configured demo bearer is invalid")
	}
	upstream, err := cfg.upstreamURL()
	if err != nil {
		return nil, err
	}
	origin, err := url.Parse(cfg.Origin)
	if err != nil {
		return nil, errors.New("demo origin is invalid")
	}
	websocketScheme := "ws"
	if origin.Scheme == "https" {
		websocketScheme = "wss"
	}
	if dial == nil {
		dial = defaultDialUpstream
	}
	tools, err := newToolPolicyStore(search)
	if err != nil {
		return nil, err
	}
	return &demoHandler{
		cfg:             cfg,
		bearer:          append([]byte(nil), bearer...),
		upstream:        upstream,
		websocketOrigin: websocketScheme + "://" + origin.Host,
		dial:            dial,
		tools:           tools,
	}, nil
}

func (handler *demoHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	setBrowserHeaders(writer.Header(), handler.websocketOrigin)
	switch request.URL.Path {
	case "/api/tool-policy":
		handler.serveToolPolicy(writer, request)
	case "/api/tool-policy/test":
		handler.serveToolPolicyTest(writer, request)
	case "/":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handler.serveAsset(writer, "web/index.html", "text/html; charset=utf-8")
	case "/styles.css":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handler.serveAsset(writer, "web/styles.css", "text/css; charset=utf-8")
	case "/audio-core.mjs":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handler.serveAsset(writer, "web/audio-core.mjs", "text/javascript; charset=utf-8")
	case "/browser-runtime.mjs":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handler.serveAsset(writer, "web/browser-runtime.mjs", "text/javascript; charset=utf-8")
	case "/client-state.mjs":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handler.serveAsset(writer, "web/client-state.mjs", "text/javascript; charset=utf-8")
	case "/protocol-state.mjs":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handler.serveAsset(writer, "web/protocol-state.mjs", "text/javascript; charset=utf-8")
	case "/tool-state.mjs":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handler.serveAsset(writer, "web/tool-state.mjs", "text/javascript; charset=utf-8")
	case "/app.js":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handler.serveAsset(writer, "web/app.js", "text/javascript; charset=utf-8")
	case "/capture-worklet.js":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handler.serveAsset(writer, "web/capture-worklet.js", "text/javascript; charset=utf-8")
	case "/playback-worklet.js":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handler.serveAsset(writer, "web/playback-worklet.js", "text/javascript; charset=utf-8")
	case "/health/ready":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("ready\n"))
	case "/ws":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handler.serveWebSocket(writer, request)
	default:
		writer.WriteHeader(http.StatusNotFound)
	}
}

func (handler *demoHandler) serveAsset(writer http.ResponseWriter, name, contentType string) {
	content, err := webAssets.ReadFile(name)
	if err != nil {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	writer.Header().Set("Content-Type", contentType)
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(content)
}

func (handler *demoHandler) serveWebSocket(writer http.ResponseWriter, request *http.Request) {
	if request.Header.Get("Origin") != handler.cfg.Origin {
		writer.WriteHeader(http.StatusForbidden)
		return
	}
	browser, err := websocket.Accept(writer, request, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	defer browser.CloseNow()

	header := http.Header{}
	header.Set("Authorization", "Bearer "+string(handler.bearer))
	header.Set("Origin", handler.cfg.Origin)
	upstream, err := handler.dial(request.Context(), handler.upstream, header)
	if err != nil {
		_ = browser.Close(websocket.StatusInternalError, "upstream unavailable")
		return
	}
	defer upstream.CloseNow()
	_ = proxyWebSocket(request.Context(), browser, upstream, handler.cfg.MaxFrameBytes)
}

func setBrowserHeaders(header http.Header, websocketOrigin string) {
	header.Set("Cache-Control", "no-store")
	header.Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; worker-src 'self'; connect-src 'self' "+websocketOrigin+"; media-src 'none'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'")
	header.Set("Cross-Origin-Opener-Policy", "same-origin")
	header.Set("Permissions-Policy", "microphone=(self)")
	header.Set("Referrer-Policy", "no-referrer")
	header.Set("X-Content-Type-Options", "nosniff")
}

func defaultDialUpstream(ctx context.Context, endpoint *url.URL, header http.Header) (*websocket.Conn, error) {
	httpClient := &http.Client{
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	conn, response, err := websocket.Dial(ctx, endpoint.String(), &websocket.DialOptions{
		HTTPClient: httpClient,
		HTTPHeader: header,
	})
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	if err != nil {
		return nil, errors.New("upstream WebSocket unavailable")
	}
	return conn, nil
}

func run(ctx context.Context, cfg demoConfig, bearer []byte) error {
	handler, err := newHandler(cfg, bearer, defaultDialUpstream)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return errors.New("listen demo server")
	}
	server := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
	shutdownComplete := make(chan struct{})
	go func() {
		defer close(shutdownComplete)
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	serveErr := server.Serve(listener)
	if ctx.Err() != nil {
		<-shutdownComplete
	}
	if errors.Is(serveErr, http.ErrServerClosed) && ctx.Err() != nil {
		return nil
	}
	return serveErr
}
