package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/coder/websocket"
)

const (
	capabilitiesTimeout = 5 * time.Second
	capabilitiesMaxBody = 64 << 10
)

//go:embed web/*
var webAssets embed.FS

type browserAsset struct {
	path        string
	contentType string
}

var browserAssets = map[string]browserAsset{
	"/":                    {path: "web/index.html", contentType: "text/html; charset=utf-8"},
	"/styles.css":          {path: "web/styles.css", contentType: "text/css; charset=utf-8"},
	"/app.js":              {path: "web/app.js", contentType: "text/javascript; charset=utf-8"},
	"/audio-core.mjs":      {path: "web/audio-core.mjs", contentType: "text/javascript; charset=utf-8"},
	"/protocol-state.mjs":  {path: "web/protocol-state.mjs", contentType: "text/javascript; charset=utf-8"},
	"/capture-worklet.js":  {path: "web/capture-worklet.js", contentType: "text/javascript; charset=utf-8"},
	"/playback-worklet.js": {path: "web/playback-worklet.js", contentType: "text/javascript; charset=utf-8"},
}

type dialUpstream func(context.Context, *url.URL, http.Header) (*websocket.Conn, error)

type fetchCapabilities func(context.Context, *url.URL, http.Header) ([]byte, error)

type upstreamDependencies struct {
	Dial  dialUpstream
	Fetch fetchCapabilities
}

func newHandler(cfg demoConfig, bearer []byte, dependencies upstreamDependencies) (http.Handler, error) {
	if err := cfg.validate(); err != nil {
		return nil, errors.New("invalid demo server configuration")
	}
	upstream, err := cfg.upstreamWebSocketURL()
	if err != nil {
		return nil, errors.New("invalid demo upstream URL")
	}
	capabilities, err := cfg.capabilitiesURL()
	if err != nil {
		return nil, errors.New("invalid demo capabilities URL")
	}
	if dependencies.Dial == nil {
		dependencies.Dial = dialWebSocket
	}
	if dependencies.Fetch == nil {
		dependencies.Fetch = fetchCapabilitiesHTTP
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, r *http.Request) {
		browserHeaders(w.Header())
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /api/capabilities", func(w http.ResponseWriter, r *http.Request) {
		browserHeaders(w.Header())
		ctx, cancel := context.WithTimeout(r.Context(), capabilitiesTimeout)
		defer cancel()
		body, fetchErr := dependencies.Fetch(ctx, capabilities, authorizationHeader(bearer))
		if fetchErr != nil || int64(len(body)) > capabilitiesMaxBody || !json.Valid(body) {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	})
	mux.HandleFunc("GET /ws", func(w http.ResponseWriter, r *http.Request) {
		browserHeaders(w.Header())
		if r.Header.Get("Origin") != cfg.Origin {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		browser, acceptErr := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if acceptErr != nil {
			return
		}
		browserClosed := false
		defer func() {
			if !browserClosed {
				_ = browser.CloseNow()
			}
		}()
		upstreamConn, dialErr := dependencies.Dial(r.Context(), upstream, authorizationHeader(bearer))
		if dialErr != nil {
			closeBrowser(browser, errors.New("upstream unavailable"))
			browserClosed = true
			return
		}
		defer upstreamConn.CloseNow()
		closeBrowser(browser, proxyWebSocket(r.Context(), browser, upstreamConn, cfg.MaxFrameBytes))
		browserClosed = true
	})
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		serveAsset(w, r)
	})
	return mux, nil
}

func browserHeaders(header http.Header) {
	header.Set("Cache-Control", "no-store")
	header.Set("X-Content-Type-Options", "nosniff")
	header.Set("Referrer-Policy", "no-referrer")
	header.Set("Content-Security-Policy", "default-src 'self'; connect-src 'self'; script-src 'self'; style-src 'self'; worker-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'")
}

func serveAsset(w http.ResponseWriter, r *http.Request) {
	browserHeaders(w.Header())
	asset, ok := browserAssets[r.URL.Path]
	if !ok {
		http.NotFound(w, r)
		return
	}
	serveEmbeddedAsset(w, asset.path, asset.contentType)
}

func serveEmbeddedAsset(w http.ResponseWriter, asset, contentType string) {
	content, err := webAssets.ReadFile(asset)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	browserHeaders(w.Header())
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(content)
}

func authorizationHeader(bearer []byte) http.Header {
	header := make(http.Header)
	if len(bearer) == 0 {
		return header
	}
	ownedBearer := append([]byte(nil), bearer...)
	value := make([]byte, 0, len("Bearer ")+len(ownedBearer))
	value = append(value, "Bearer "...)
	value = append(value, ownedBearer...)
	header.Set("Authorization", string(value))
	clear(ownedBearer)
	clear(value)
	return header
}

func dialWebSocket(ctx context.Context, endpoint *url.URL, header http.Header) (*websocket.Conn, error) {
	conn, _, err := websocket.Dial(ctx, endpoint.String(), &websocket.DialOptions{HTTPHeader: header})
	return conn, err
}

func fetchCapabilitiesHTTP(ctx context.Context, endpoint *url.URL, header http.Header) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, err
	}
	request.Header = header.Clone()
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, errors.New("capabilities upstream status")
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, capabilitiesMaxBody+1))
	if err != nil || int64(len(data)) > capabilitiesMaxBody {
		return nil, errors.New("capabilities upstream body")
	}
	return data, nil
}
