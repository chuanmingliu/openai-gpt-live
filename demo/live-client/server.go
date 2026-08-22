package main

import (
	"context"
	"embed"
	"errors"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
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
	operator        *operatorSupervisor
	operatorContext context.Context
	clientSessions  *clientSessionSet
	profiles        *profileAdminService
}

type clientSessionSet struct {
	mu          sync.Mutex
	accepting   bool
	connections map[*websocket.Conn]struct{}
}

func newClientSessionSet() *clientSessionSet {
	return &clientSessionSet{accepting: true, connections: make(map[*websocket.Conn]struct{})}
}

// add registers a browser connection only while the client surface is active.
// Together with stop this closes the small race between the HTTP admission
// check and WebSocket upgrade when an operator presses Client Stop.
func (sessions *clientSessionSet) add(connection *websocket.Conn) bool {
	sessions.mu.Lock()
	defer sessions.mu.Unlock()
	if !sessions.accepting {
		return false
	}
	sessions.connections[connection] = struct{}{}
	return true
}

func (sessions *clientSessionSet) remove(connection *websocket.Conn) {
	sessions.mu.Lock()
	delete(sessions.connections, connection)
	sessions.mu.Unlock()
}

func (sessions *clientSessionSet) stop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	sessions.mu.Lock()
	sessions.accepting = false
	connections := make([]*websocket.Conn, 0, len(sessions.connections))
	for connection := range sessions.connections {
		connections = append(connections, connection)
	}
	// A quiet peer may never return the graceful close frame. Remove every
	// connection before aborting it so neither a deadline nor peer behavior can
	// retain a client session in the operator lifecycle.
	sessions.connections = make(map[*websocket.Conn]struct{})
	sessions.mu.Unlock()
	for _, connection := range connections {
		connection.CloseNow()
	}
	return ctx.Err()
}

func (sessions *clientSessionSet) start(context.Context) error {
	sessions.mu.Lock()
	sessions.accepting = true
	sessions.mu.Unlock()
	return nil
}

func newHandler(cfg demoConfig, bearer []byte, dial dialUpstream) (http.Handler, error) {
	return newHandlerWithContext(context.Background(), cfg, bearer, dial)
}

func newHandlerWithContext(operatorContext context.Context, cfg demoConfig, bearer []byte, dial dialUpstream) (http.Handler, error) {
	search, err := websearch.New(net.DefaultResolver)
	if err != nil {
		return nil, errors.New("demo tool search is unavailable")
	}
	return newHandlerWithToolSearchContext(operatorContext, cfg, bearer, dial, search)
}

func newHandlerWithToolSearch(cfg demoConfig, bearer []byte, dial dialUpstream, search toolSearcher) (http.Handler, error) {
	return newHandlerWithToolSearchContext(context.Background(), cfg, bearer, dial, search)
}

func newHandlerWithToolSearchContext(operatorContext context.Context, cfg demoConfig, bearer []byte, dial dialUpstream, search toolSearcher) (http.Handler, error) {
	if operatorContext == nil {
		operatorContext = context.Background()
	}
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
	operator, err := newConfiguredOperatorSupervisor(cfg)
	if err != nil {
		return nil, errors.New("demo operator is unavailable")
	}
	profiles := cfg.Profiles.service
	if profiles == nil && cfg.Profiles.Enabled {
		profiles, err = openProfileAdminService(operatorContext, cfg.Profiles, osLookupEnv)
		if err != nil {
			return nil, errors.New("demo profile administration is unavailable")
		}
	}
	sessions := cfg.Operator.clientSessions
	if sessions == nil {
		sessions = newClientSessionSet()
	}
	return &demoHandler{
		cfg:             cfg,
		bearer:          append([]byte(nil), bearer...),
		upstream:        upstream,
		websocketOrigin: websocketScheme + "://" + origin.Host,
		dial:            dial,
		tools:           tools,
		operator:        operator,
		operatorContext: operatorContext,
		clientSessions:  sessions,
		profiles:        profiles,
	}, nil
}

// osLookupEnv is indirected so tests never depend on process environment.
var osLookupEnv = os.LookupEnv

func (handler *demoHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	setBrowserHeaders(writer.Header(), handler.websocketOrigin)
	if strings.HasPrefix(request.URL.Path, profileAPIPrefix) {
		handler.serveProfileAPI(writer, request)
		return
	}
	switch request.URL.Path {
	case "/api/voice-readiness":
		handler.serveVoiceReadiness(writer, request)
	case "/api/operator/status":
		handler.serveOperatorStatus(writer, request)
	case "/api/operator/action":
		handler.serveOperatorAction(writer, request)
	case "/api/tool-policy":
		if !handler.clientAvailable(request) {
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		handler.serveToolPolicy(writer, request)
	case "/api/tool-policy/test":
		if !handler.clientAvailable(request) {
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		handler.serveToolPolicyTest(writer, request)
	case "/":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if !handler.clientAvailable(request) {
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		handler.serveAsset(writer, "web/index.html", "text/html; charset=utf-8")
	case "/styles.css":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handler.serveClientAsset(writer, request, "web/styles.css", "text/css; charset=utf-8")
	case "/setup":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handler.serveAsset(writer, "web/setup.html", "text/html; charset=utf-8")
	case "/setup.css":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handler.serveAsset(writer, "web/setup.css", "text/css; charset=utf-8")
	case "/setup.mjs":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handler.serveAsset(writer, "web/setup.mjs", "text/javascript; charset=utf-8")
	case "/setup-profile.mjs":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handler.serveAsset(writer, "web/setup-profile.mjs", "text/javascript; charset=utf-8")
	case "/profile-api.mjs":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handler.serveAsset(writer, "web/profile-api.mjs", "text/javascript; charset=utf-8")
	case "/knowledge-editor.mjs":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handler.serveAsset(writer, "web/knowledge-editor.mjs", "text/javascript; charset=utf-8")
	case "/adapter-editor.mjs":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handler.serveAsset(writer, "web/adapter-editor.mjs", "text/javascript; charset=utf-8")
	case "/recipe-editor.mjs":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handler.serveAsset(writer, "web/recipe-editor.mjs", "text/javascript; charset=utf-8")
	case "/profile-draft-state.mjs":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handler.serveAsset(writer, "web/profile-draft-state.mjs", "text/javascript; charset=utf-8")
	case "/voice-profile-state.mjs":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handler.serveClientAsset(writer, request, "web/voice-profile-state.mjs", "text/javascript; charset=utf-8")
	case "/audio-core.mjs":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handler.serveClientAsset(writer, request, "web/audio-core.mjs", "text/javascript; charset=utf-8")
	case "/browser-runtime.mjs":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handler.serveClientAsset(writer, request, "web/browser-runtime.mjs", "text/javascript; charset=utf-8")
	case "/client-state.mjs":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handler.serveClientAsset(writer, request, "web/client-state.mjs", "text/javascript; charset=utf-8")
	case "/protocol-log.mjs":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handler.serveClientAsset(writer, request, "web/protocol-log.mjs", "text/javascript; charset=utf-8")
	case "/protocol-state.mjs":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handler.serveClientAsset(writer, request, "web/protocol-state.mjs", "text/javascript; charset=utf-8")
	case "/tool-state.mjs":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handler.serveClientAsset(writer, request, "web/tool-state.mjs", "text/javascript; charset=utf-8")
	case "/app.js":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handler.serveClientAsset(writer, request, "web/app.js", "text/javascript; charset=utf-8")
	case "/capture-worklet.js":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handler.serveClientAsset(writer, request, "web/capture-worklet.js", "text/javascript; charset=utf-8")
	case "/playback-worklet.js":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handler.serveClientAsset(writer, request, "web/playback-worklet.js", "text/javascript; charset=utf-8")
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
		if !handler.clientAvailable(request) {
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		handler.serveWebSocket(writer, request)
	default:
		writer.WriteHeader(http.StatusNotFound)
	}
}

func (handler *demoHandler) clientAvailable(request *http.Request) bool {
	return handler.operator == nil || handler.operator.runner.Ready(request.Context(), componentClient)
}

func (handler *demoHandler) serveClientAsset(writer http.ResponseWriter, request *http.Request, name, contentType string) {
	if !handler.clientAvailable(request) {
		writer.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	handler.serveAsset(writer, name, contentType)
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
	// The browser may select exactly one published profile. Every other query
	// key is rejected here so no browser value reaches the gateway untrusted.
	upstreamURL, err := handler.upstreamWithProfile(request.URL.RawQuery)
	if err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	browser, err := websocket.Accept(writer, request, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	if !handler.clientSessions.add(browser) {
		_ = browser.Close(websocket.StatusGoingAway, "client stopped")
		return
	}
	defer handler.clientSessions.remove(browser)
	defer browser.CloseNow()

	header := http.Header{}
	header.Set("Authorization", "Bearer "+string(handler.bearer))
	header.Set("Origin", handler.cfg.Origin)
	upstream, err := handler.dial(request.Context(), upstreamURL, header)
	if err != nil {
		_ = browser.Close(websocket.StatusInternalError, "upstream unavailable")
		return
	}
	defer upstream.CloseNow()
	_ = proxyWebSocket(request.Context(), browser, upstream, handler.cfg.MaxFrameBytes)
}

// upstreamWithProfile forwards only an exact profile/profile_version pair from
// the browser. Unknown, duplicate, or malformed keys fail closed, and the
// gateway always receives this BFF's fixed model value.
func (handler *demoHandler) upstreamWithProfile(rawQuery string) (*url.URL, error) {
	upstream := *handler.upstream
	if rawQuery == "" {
		return &upstream, nil
	}
	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		return nil, errors.New("demo websocket query is invalid")
	}
	id, hasID := values["profile"]
	version, hasVersion := values["profile_version"]
	if len(values) != 2 || !hasID || !hasVersion || len(id) != 1 || len(version) != 1 {
		return nil, errors.New("demo websocket query is invalid")
	}
	if !profileIdentifierPattern.MatchString(id[0]) || id[0] == "builtin" {
		return nil, errors.New("demo websocket profile is invalid")
	}
	parsed, err := strconv.ParseUint(version[0], 10, 64)
	if err != nil || parsed == 0 || strconv.FormatUint(parsed, 10) != version[0] {
		return nil, errors.New("demo websocket profile version is invalid")
	}
	query := url.Values{}
	query.Set("model", handler.cfg.Model)
	query.Set("profile", id[0])
	query.Set("profile_version", version[0])
	upstream.RawQuery = query.Encode()
	return &upstream, nil
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
	if ctx == nil {
		ctx = context.Background()
	}
	serverContext, stopServer := context.WithCancel(ctx)
	defer stopServer()
	if cfg.Operator.Enabled {
		if cfg.Operator.clientSessions == nil {
			cfg.Operator.clientSessions = newClientSessionSet()
		}
		cfg.Operator.ClientStart = cfg.Operator.clientSessions.start
		cfg.Operator.ClientStop = cfg.Operator.clientSessions.stop
	}
	handler, err := newHandlerWithContext(serverContext, cfg, bearer, defaultDialUpstream)
	if err != nil {
		return err
	}
	demo, ok := handler.(*demoHandler)
	if !ok {
		return errors.New("demo handler unavailable")
	}
	defer closeOperatorRunner(demo.operator)
	defer demo.profiles.close()
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
		<-serverContext.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	serveErr := server.Serve(listener)
	if serverContext.Err() != nil {
		<-shutdownComplete
		waitOperatorActions(demo.operator)
		shutdownOwnedOperator(demo.operator)
	}
	if errors.Is(serveErr, http.ErrServerClosed) && serverContext.Err() != nil {
		return nil
	}
	return serveErr
}

func waitOperatorActions(supervisor *operatorSupervisor) {
	if supervisor == nil {
		return
	}
	// Every action has its own bounded deadline.  Do not close the state root
	// while one is still unwinding; otherwise its ownership cleanup can race a
	// replacement runner.
	supervisor.WaitIdle(context.Background())
}

func shutdownOwnedOperator(supervisor *operatorSupervisor) {
	if supervisor == nil || supervisor.runner == nil {
		return
	}
	// http.Server.Shutdown does not close hijacked WebSockets. Client is the
	// first reverse dependency: its local runner callback aborts every browser
	// session before worker and gateway cleanup can touch their upstream.
	if supervisor.runner.Owned(componentClient) {
		step, cancel := context.WithTimeout(context.Background(), operatorActionTimeout)
		_ = supervisor.runner.Run(step, fixedOperation{Component: componentClient, Action: actionStop})
		cancel()
	}
	for _, component := range []operatorComponent{componentWorker, componentGateway, componentPostgres, componentSpeech} {
		if supervisor.runner.Owned(component) {
			step, cancel := context.WithTimeout(context.Background(), operatorActionTimeout)
			_ = supervisor.runner.Run(step, fixedOperation{Component: component, Action: actionStop})
			cancel()
		}
	}
}

func closeOperatorRunner(supervisor *operatorSupervisor) {
	if supervisor == nil {
		return
	}
	if runner, ok := supervisor.runner.(*localOperatorRunner); ok && runner.stateRoot != nil {
		_ = runner.stateRoot.Close()
	}
}
