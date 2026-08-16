package main

import (
	"context"
	"crypto/tls"
	"embed"
	"encoding/json"
	"errors"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/coder/websocket"
)

//go:embed web/*
var webAssets embed.FS

type dialUpstream func(context.Context, *url.URL, http.Header) (*websocket.Conn, error)

type runtime struct {
	cfg      demoConfig
	bearer   []byte
	dial     dialUpstream
	upstream *url.URL
	assets   fs.FS
	wsOrigin string
	sessions *sessionGate
	log      *slog.Logger
}

func newHandler(cfg demoConfig, bearer []byte, dial dialUpstream) (http.Handler, error) {
	rt, err := newRuntime(cfg, bearer, dial)
	if err != nil {
		return nil, err
	}
	return rt.handler(), nil
}

func newRuntime(cfg demoConfig, bearer []byte, dial dialUpstream) (*runtime, error) {
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
	assets, err := fs.Sub(webAssets, "web")
	if err != nil {
		return nil, errors.New("demo assets are unavailable")
	}
	return &runtime{
		cfg:      cfg,
		bearer:   append([]byte(nil), bearer...),
		dial:     dial,
		upstream: upstream,
		assets:   assets,
		wsOrigin: cfg.websocketOrigin(),
		sessions: newSessionGate(cfg.MaxSessions),
		log:      slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})),
	}, nil
}

func (rt *runtime) close() {
	if rt == nil {
		return
	}
	clear(rt.bearer)
}

func (rt *runtime) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", rt.serve)
	return mux
}

func (rt *runtime) serve(writer http.ResponseWriter, request *http.Request) {
	rt.setBrowserHeaders(writer.Header())
	switch request.URL.Path {
	case "/favicon.ico":
		if request.Method != http.MethodGet && request.Method != http.MethodHead {
			writer.Header().Set("Allow", "GET, HEAD")
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	case "/health/live":
		rt.writePlain(writer, request, http.StatusOK, "ok\n")
	case "/health/ready":
		if !rt.sessions.Ready() {
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		rt.writePlain(writer, request, http.StatusOK, "ready\n")
	case "/setup":
		rt.serveSetup(writer, request)
	case "/ws":
		if request.Method != http.MethodGet {
			writer.Header().Set("Allow", http.MethodGet)
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		rt.serveBrowserSocket(writer, request)
	default:
		if request.Method != http.MethodGet && request.Method != http.MethodHead {
			writer.Header().Set("Allow", "GET, HEAD")
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		path := request.URL.Path
		if path == "/" {
			path = "/index.html"
		}
		if !rt.serveStatic(writer, request, path) {
			http.NotFound(writer, request)
		}
	}
}

type setupStatus struct {
	Mode             string `json:"mode"`
	Ready            bool   `json:"ready"`
	Origin           string `json:"origin"`
	Model            string `json:"model"`
	GatewayHost      string `json:"gateway_host"`
	GatewayReachable bool   `json:"gateway_reachable"`
	BearerEnv        string `json:"bearer_env"`
	ListenAddr       string `json:"listen"`
	SecureOrigin     bool   `json:"https_origin"`
}

func (rt *runtime) serveSetup(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet && request.Method != http.MethodHead {
		writer.Header().Set("Allow", "GET, HEAD")
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	status := setupStatus{
		Mode:         rt.cfg.Mode,
		Ready:        rt.sessions.Ready(),
		Origin:       rt.cfg.Origin,
		Model:        rt.cfg.Model,
		GatewayHost:  gatewayHost(rt.cfg.GatewayURL),
		BearerEnv:    rt.cfg.BearerEnv,
		ListenAddr:   rt.cfg.ListenAddr,
		SecureOrigin: strings.HasPrefix(rt.cfg.Origin, "https://"),
	}
	status.GatewayReachable = probeTCP(status.GatewayHost, 300*time.Millisecond)
	body, err := json.Marshal(status)
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(http.StatusOK)
	if request.Method != http.MethodHead {
		_, _ = writer.Write(append(body, '\n'))
	}
}

func gatewayHost(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" {
		return ""
	}
	return parsed.Host
}

func probeTCP(host string, timeout time.Duration) bool {
	if host == "" || timeout <= 0 {
		return false
	}
	conn, err := net.DialTimeout("tcp", host, timeout)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func (rt *runtime) writePlain(writer http.ResponseWriter, request *http.Request, status int, body string) {
	if request.Method != http.MethodGet && request.Method != http.MethodHead {
		writer.Header().Set("Allow", "GET, HEAD")
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
	writer.WriteHeader(status)
	if request.Method != http.MethodHead {
		_, _ = writer.Write([]byte(body))
	}
}

func (rt *runtime) serveStatic(writer http.ResponseWriter, request *http.Request, path string) bool {
	name := strings.TrimPrefix(path, "/")
	if name == "" || strings.Contains(name, "..") || strings.Contains(name, "\\") || strings.HasSuffix(name, ".test.mjs") {
		return false
	}
	body, err := fs.ReadFile(rt.assets, name)
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

func (rt *runtime) setBrowserHeaders(header http.Header) {
	header.Set("Cache-Control", "no-store")
	header.Set("X-Content-Type-Options", "nosniff")
	header.Set("Referrer-Policy", "no-referrer")
	header.Set("X-Frame-Options", "DENY")
	header.Set("Cross-Origin-Opener-Policy", "same-origin")
	header.Set("Cross-Origin-Resource-Policy", "same-origin")
	header.Set("Permissions-Policy", "camera=(), geolocation=(), microphone=(self)")
	header.Set("Content-Security-Policy", "default-src 'self'; connect-src 'self' "+rt.wsOrigin+"; media-src 'none'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'")
	if rt.cfg.Mode == modeProduction {
		header.Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
	}
}

func (rt *runtime) serveBrowserSocket(writer http.ResponseWriter, request *http.Request) {
	if request.URL.RawQuery != "" {
		rt.log.Info("ws_reject", "reason", "query")
		http.Error(writer, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	if request.Header.Get("Origin") != rt.cfg.Origin {
		rt.log.Info("ws_reject", "reason", "origin")
		http.Error(writer, http.StatusText(http.StatusForbidden), http.StatusForbidden)
		return
	}
	sessionCtx, cancel := context.WithCancel(request.Context())
	id, ok := rt.sessions.Acquire(cancel)
	if !ok {
		cancel()
		rt.log.Info("ws_reject", "reason", "capacity")
		http.Error(writer, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
		return
	}
	defer func() {
		cancel()
		rt.sessions.Release(id)
	}()
	browser, err := websocket.Accept(writer, request, &websocket.AcceptOptions{OriginPatterns: []string{rt.cfg.Origin}})
	if err != nil {
		return
	}
	dialCtx, cancelDial := context.WithTimeout(sessionCtx, rt.cfg.Handshake)
	header := make(http.Header)
	header.Set("Authorization", "Bearer "+string(rt.bearer))
	upstreamConn, err := rt.dial(dialCtx, rt.upstream, header)
	cancelDial()
	if err != nil {
		rt.log.Info("ws_reject", "reason", "upstream", "session", id)
		_ = browser.Close(websocket.StatusTryAgainLater, "upstream unavailable")
		return
	}
	limited, cancelLimit := context.WithTimeout(sessionCtx, rt.cfg.MaxSession)
	defer cancelLimit()
	rt.log.Info("ws_open", "session", id)
	_ = proxyWebSocket(limited, browser, upstreamConn, rt.cfg.MaxFrameBytes, rt.cfg.IdleTimeout)
	rt.log.Info("ws_close", "session", id)
}

func defaultDial(ctx context.Context, endpoint *url.URL, header http.Header) (*websocket.Conn, error) {
	conn, _, err := websocket.Dial(ctx, endpoint.String(), &websocket.DialOptions{HTTPHeader: header})
	return conn, err
}

func run(ctx context.Context, cfg demoConfig, bearer []byte) error {
	rt, err := newRuntime(cfg, bearer, defaultDial)
	if err != nil {
		return err
	}
	defer rt.close()
	listener, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return errors.New("listen demo server")
	}
	server := &http.Server{
		Handler:           rt.handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    8 << 10,
	}
	if cfg.TLSCertFile != "" {
		server.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS13}
	}
	serveErr := make(chan error, 1)
	go func() {
		if cfg.TLSCertFile != "" {
			serveErr <- server.ServeTLS(listener, cfg.TLSCertFile, cfg.TLSKeyFile)
		} else {
			serveErr <- server.Serve(listener)
		}
	}()
	var serveResult error
	select {
	case serveResult = <-serveErr:
	case <-ctx.Done():
	}
	rt.sessions.BeginDrain()
	shutdown, cancel := context.WithTimeout(context.Background(), cfg.DrainTimeout)
	defer cancel()
	_ = server.Shutdown(shutdown)
	_ = rt.sessions.Wait(shutdown)
	if serveResult != nil && !errors.Is(serveResult, http.ErrServerClosed) {
		return serveResult
	}
	return nil
}
