// Package admin exposes a narrowly scoped loopback-only operational API.
package admin

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/auth"
)

const AdminScope = "live.admin"

type Drainer interface {
	Begin() bool
	IsDraining() bool
}
type Capacity struct{ Active, Limit, Available int64 }
type CapacityProvider interface{ Capacity() Capacity }
type SessionProvider interface{ ActiveSessions() int64 }

type Config struct {
	Authenticator auth.Authenticator
	Drainer       Drainer
	Capacity      CapacityProvider
	Sessions      SessionProvider
}

func NewHandler(cfg Config) (http.Handler, error) {
	if cfg.Authenticator == nil || cfg.Drainer == nil {
		return nil, errors.New("admin authentication and drainer are required")
	}
	h := &handler{cfg: cfg}
	return h, nil
}

type handler struct{ cfg Config }

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if !requestLoopback(r) {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	if status := h.authorizationStatus(r); status != 0 {
		w.WriteHeader(status)
		return
	}
	switch r.URL.Path {
	case "/admin/v1/capacity":
		h.capacity(w, r)
	case "/admin/v1/sessions":
		h.sessions(w, r)
	case "/admin/v1/drain":
		h.drain(w, r)
	default:
		http.NotFound(w, r)
	}
}
func (h *handler) authorizationStatus(r *http.Request) int {
	token, err := auth.Bearer(r.Header)
	if err != nil {
		return http.StatusUnauthorized
	}
	principal, err := h.cfg.Authenticator.Authenticate(r.Context(), token)
	if err != nil {
		return http.StatusUnauthorized
	}
	_, ok := principal.Scopes[AdminScope]
	if !ok {
		return http.StatusForbidden
	}
	return 0
}
func (h *handler) capacity(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		method(w, "GET, HEAD")
		return
	}
	c := Capacity{}
	if h.cfg.Capacity != nil {
		c = h.cfg.Capacity.Capacity()
	}
	writeJSON(w, r, http.StatusOK, c)
}
func (h *handler) sessions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		method(w, "GET, HEAD")
		return
	}
	var active int64
	if h.cfg.Sessions != nil {
		active = h.cfg.Sessions.ActiveSessions()
	}
	writeJSON(w, r, http.StatusOK, struct {
		Active int64 `json:"active"`
	}{active})
}
func (h *handler) drain(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		method(w, "POST")
		return
	}
	if r.Header.Get("Content-Type") != "application/json" || r.ContentLength > 2048 {
		w.WriteHeader(http.StatusUnsupportedMediaType)
		return
	}
	limited := io.LimitReader(r.Body, 2049)
	var body map[string]json.RawMessage
	decoder := json.NewDecoder(limited)
	if err := decoder.Decode(&body); err != nil || len(body) != 0 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	h.cfg.Drainer.Begin()
	writeJSON(w, r, http.StatusAccepted, struct {
		Status string `json:"status"`
	}{"draining"})
}
func method(w http.ResponseWriter, allow string) {
	w.Header().Set("Allow", allow)
	w.WriteHeader(http.StatusMethodNotAllowed)
}
func writeJSON(w http.ResponseWriter, r *http.Request, status int, value any) {
	body, err := json.Marshal(value)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", itoa(len(body)))
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		_, _ = w.Write(body)
	}
}
func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return string(b[i:])
}

func requestLoopback(r *http.Request) bool {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && ip.IsLoopback()
}

func ValidateListenAddress(ctx context.Context, address string, production bool) error {
	if address == "" {
		return errors.New("admin listen address is required")
	}
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return errors.New("admin listen address is invalid")
	}
	if !production {
		return nil
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		return errors.New("production admin listener must be loopback")
	}
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil || len(ips) == 0 {
		return errors.New("resolve admin listener")
	}
	for _, ip := range ips {
		if !ip.IP.IsLoopback() {
			return errors.New("production admin listener must be loopback")
		}
	}
	return nil
}

// DrainURL invokes a loopback admin endpoint with bounded I/O and no redirect.
func DrainURL(ctx context.Context, client *http.Client, rawURL, bearer string) error {
	resolver := func(ctx context.Context, host string) ([]net.IPAddr, error) {
		return net.DefaultResolver.LookupIPAddr(ctx, host)
	}
	dialer := (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 15 * time.Second}).DialContext
	return drainURLWithNetwork(ctx, client, rawURL, bearer, resolver, dialer)
}

type resolveIPFunc func(context.Context, string) ([]net.IPAddr, error)
type dialContextFunc func(context.Context, string, string) (net.Conn, error)

func drainURLWithNetwork(ctx context.Context, client *http.Client, rawURL, bearer string, resolve resolveIPFunc, dial dialContextFunc) error {
	if ctx == nil {
		return errors.New("drain context is required")
	}
	if resolve == nil || dial == nil {
		return errors.New("drain network is required")
	}
	u, err := url.Parse(rawURL)
	if err != nil || u.User != nil || u.Fragment != "" || u.RawQuery != "" || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.Path != "" && u.Path != "/admin/v1/drain" {
		return errors.New("drain URL is invalid")
	}
	port := u.Port()
	if port == "" {
		if u.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return errors.New("drain URL is invalid")
	}
	pinned, err := resolveLoopback(ctx, u.Hostname(), resolve)
	if err != nil {
		return err
	}
	u.Path = "/admin/v1/drain"
	u.RawQuery = ""
	var transport *http.Transport
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	} else {
		copy := *client
		client = &copy
		if client.Timeout <= 0 || client.Timeout > 30*time.Second {
			client.Timeout = 5 * time.Second
		}
		if original, ok := client.Transport.(*http.Transport); ok && original != nil {
			transport = original.Clone()
		}
	}
	if transport == nil {
		transport = http.DefaultTransport.(*http.Transport).Clone()
	}
	transport.Proxy = nil
	transport.DialTLS = nil
	transport.DialTLSContext = nil
	if u.Scheme == "https" {
		if transport.TLSClientConfig == nil {
			transport.TLSClientConfig = &tls.Config{}
		} else {
			transport.TLSClientConfig = transport.TLSClientConfig.Clone()
		}
		transport.TLSClientConfig.ServerName = u.Hostname()
	}
	transport.DialContext = func(dialCtx context.Context, network, _ string) (net.Conn, error) {
		conn, dialErr := dial(dialCtx, network, net.JoinHostPort(pinned.String(), port))
		if dialErr != nil {
			return nil, dialErr
		}
		peer, peerErr := remoteIP(conn.RemoteAddr())
		if peerErr != nil || !peer.IsLoopback() || !peer.Equal(pinned) {
			_ = conn.Close()
			return nil, errors.New("drain peer is not loopback")
		}
		return conn, nil
	}
	client.Transport = transport
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	defer transport.CloseIdleConnections()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), strings.NewReader("{}"))
	if err != nil {
		return errors.New("create drain request")
	}
	req.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	response, err := client.Do(req)
	if err != nil {
		return errors.New("drain request failed")
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode != http.StatusAccepted {
		return errors.New("drain request rejected")
	}
	return nil
}

func resolveLoopback(ctx context.Context, host string, resolve resolveIPFunc) (net.IP, error) {
	if parsed := net.ParseIP(host); parsed != nil {
		if !parsed.IsLoopback() {
			return nil, errors.New("drain URL must use loopback")
		}
		return parsed, nil
	}
	ips, err := resolve(ctx, host)
	if err != nil || len(ips) == 0 {
		return nil, errors.New("drain host is unavailable")
	}
	for _, ip := range ips {
		if !ip.IP.IsLoopback() {
			return nil, errors.New("drain URL must use loopback")
		}
	}
	return append(net.IP(nil), ips[0].IP...), nil
}

func remoteIP(address net.Addr) (net.IP, error) {
	if address == nil {
		return nil, errors.New("drain peer address is unavailable")
	}
	host, _, err := net.SplitHostPort(address.String())
	if err != nil {
		return nil, errors.New("drain peer address is invalid")
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	if ip == nil {
		return nil, errors.New("drain peer address is invalid")
	}
	return ip, nil
}

func Server(address string, handler http.Handler) *http.Server {
	return &http.Server{Addr: address, Handler: handler, ReadTimeout: 5 * time.Second, ReadHeaderTimeout: 2 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 15 * time.Second}
}
