package main

import (
	"context"
	"embed"
	"errors"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/coder/websocket"
)

//go:embed web/*
var webAssets embed.FS

type dialUpstream func(context.Context, *url.URL, http.Header) (*websocket.Conn, error)

func newHandler(cfg demoConfig, bearer []byte, dial dialUpstream) (http.Handler, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if len(bearer) == 0 {
		return nil, errors.New("demo bearer is required")
	}
	if dial == nil {
		dial = defaultDial
	}
	upstream, err := cfg.upstreamURL()
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	assets, err := fs.Sub(webAssets, "web")
	if err != nil {
		return nil, errors.New("demo assets are unavailable")
	}
	wsOrigin := cfg.websocketOrigin()
	mux.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		setBrowserHeaders(writer.Header(), wsOrigin)
		if request.URL.Path == "/health/ready" {
			if request.Method != http.MethodGet && request.Method != http.MethodHead {
				writer.Header().Set("Allow", "GET, HEAD")
				writer.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
			writer.WriteHeader(http.StatusOK)
			if request.Method != http.MethodHead {
				_, _ = writer.Write([]byte("ready\n"))
			}
			return
		}
		if request.URL.Path == "/ws" {
			serveBrowserSocket(writer, request, cfg, bearer, upstream, dial)
			return
		}
		if request.Method != http.MethodGet && request.Method != http.MethodHead {
			writer.Header().Set("Allow", "GET, HEAD")
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		path := request.URL.Path
		if path == "/" {
			path = "/index.html"
		}
		if !serveStatic(writer, request, assets, path) {
			http.NotFound(writer, request)
		}
	})
	return mux, nil
}

func serveStatic(writer http.ResponseWriter, request *http.Request, assets fs.FS, path string) bool {
	name := strings.TrimPrefix(path, "/")
	if name == "" || strings.Contains(name, "..") {
		return false
	}
	body, err := fs.ReadFile(assets, name)
	if err != nil {
		return false
	}
	writer.Header().Set("Content-Type", contentType(name))
	writer.WriteHeader(http.StatusOK)
	if request.Method != http.MethodHead {
		_, _ = writer.Write(body)
	}
	return true
}

func contentType(name string) string {
	switch {
	case strings.HasSuffix(name, ".html"):
		return "text/html; charset=utf-8"
	case strings.HasSuffix(name, ".css"):
		return "text/css; charset=utf-8"
	case strings.HasSuffix(name, ".js"), strings.HasSuffix(name, ".mjs"):
		return "text/javascript; charset=utf-8"
	default:
		return "application/octet-stream"
	}
}

func setBrowserHeaders(header http.Header, websocketOrigin string) {
	header.Set("Cache-Control", "no-store")
	header.Set("X-Content-Type-Options", "nosniff")
	header.Set("Referrer-Policy", "no-referrer")
	header.Set("Content-Security-Policy", "default-src 'self'; connect-src 'self' "+websocketOrigin+"; media-src 'none'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'")
}

func serveBrowserSocket(writer http.ResponseWriter, request *http.Request, cfg demoConfig, bearer []byte, upstream *url.URL, dial dialUpstream) {
	if request.Header.Get("Origin") != cfg.Origin {
		http.Error(writer, http.StatusText(http.StatusForbidden), http.StatusForbidden)
		return
	}
	browser, err := websocket.Accept(writer, request, &websocket.AcceptOptions{OriginPatterns: []string{cfg.Origin}})
	if err != nil {
		return
	}
	header := make(http.Header)
	header.Set("Authorization", "Bearer "+string(bearer))
	upstreamConn, err := dial(request.Context(), upstream, header)
	if err != nil {
		_ = browser.Close(websocket.StatusTryAgainLater, "upstream unavailable")
		return
	}
	_ = proxyWebSocket(request.Context(), browser, upstreamConn, cfg.MaxFrameBytes)
}

func defaultDial(ctx context.Context, endpoint *url.URL, header http.Header) (*websocket.Conn, error) {
	conn, _, err := websocket.Dial(ctx, endpoint.String(), &websocket.DialOptions{HTTPHeader: header})
	return conn, err
}

func run(ctx context.Context, cfg demoConfig, bearer []byte) error {
	handler, err := newHandler(cfg, bearer, defaultDial)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return errors.New("listen demo server")
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	err = server.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
