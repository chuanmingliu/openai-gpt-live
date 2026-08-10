// Package admission validates a transport request and owns distributed
// session capacity before the WebSocket handshake begins.
package admission

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/chuanmingliu/openai-gpt-live/internal/auth"
	"github.com/chuanmingliu/openai-gpt-live/internal/session"
)

type ReleaseFunc = func()

type Class uint8

const (
	ClassMalformed Class = iota + 1
	ClassUnauthorized
	ClassForbidden
	ClassLimited
	ClassUnavailable
)

type Error struct{ Class Class }

func (e *Error) Error() string { return "session admission rejected" }

var (
	ErrMalformed     = &Error{Class: ClassMalformed}
	ErrUnauthorized  = &Error{Class: ClassUnauthorized}
	ErrForbidden     = &Error{Class: ClassForbidden}
	ErrLimited       = &Error{Class: ClassLimited}
	ErrUnavailable   = &Error{Class: ClassUnavailable}
	ErrInvalidConfig = errors.New("invalid admission configuration")
)

func ErrorClass(err error) Class {
	var target *Error
	if errors.As(err, &target) {
		return target.Class
	}
	return ClassUnauthorized
}

type Limits struct{ Global, PerTenant, PerUser int }

type LeaseStore interface {
	Acquire(context.Context, session.Subject, Limits) (ReleaseFunc, error)
	Ready(context.Context) error
}

type MonitoredLease struct {
	Release ReleaseFunc
	Lost    <-chan struct{}
}

type MonitoredLeaseStore interface {
	LeaseStore
	AcquireMonitored(context.Context, session.Subject, Limits) (MonitoredLease, error)
}

type Config struct {
	RequireTLS        bool
	BehindIngress     bool
	TrustedProxyCIDRs []string
	AllowedOrigins    []string
	AllowEmptyOrigin  bool
	GlobalRate        float64
	GlobalBurst       int
	IPRate            float64
	IPBurst           int
	IPKeyTTL          time.Duration
	MaxIPKeys         int
	RequiredScopes    []string
	EphemeralTenantID string
	EphemeralUserID   string
	Limits            Limits
	RequireStore      bool
	Now               func() time.Time
}

type Controller struct {
	authn   auth.Authenticator
	cfg     Config
	trusted []*net.IPNet
	origins map[string]struct{}
	limiter *limiter
	store   LeaseStore
	metrics func(string)
}

func New(authn auth.Authenticator, cfg Config, stores ...LeaseStore) (*Controller, error) {
	if authn == nil || len(stores) > 1 {
		return nil, ErrInvalidConfig
	}
	if cfg.GlobalRate == 0 {
		cfg.GlobalRate = 100
	}
	if cfg.GlobalBurst == 0 {
		cfg.GlobalBurst = 100
	}
	if cfg.IPRate == 0 {
		cfg.IPRate = 10
	}
	if cfg.IPBurst == 0 {
		cfg.IPBurst = 20
	}
	if cfg.IPKeyTTL == 0 {
		cfg.IPKeyTTL = 10 * time.Minute
	}
	if cfg.MaxIPKeys == 0 {
		cfg.MaxIPKeys = 10_000
	}
	if len(cfg.TrustedProxyCIDRs) > 128 || len(cfg.RequiredScopes) > 64 || cfg.Limits.Global < 0 || cfg.Limits.PerTenant < 0 || cfg.Limits.PerUser < 0 {
		return nil, ErrInvalidConfig
	}
	cfg.TrustedProxyCIDRs = append([]string(nil), cfg.TrustedProxyCIDRs...)
	cfg.AllowedOrigins = append([]string(nil), cfg.AllowedOrigins...)
	cfg.RequiredScopes = append([]string(nil), cfg.RequiredScopes...)
	origins, err := parseAllowedOrigins(cfg.AllowedOrigins)
	if err != nil {
		return nil, ErrInvalidConfig
	}
	if len(origins) == 0 && !cfg.AllowEmptyOrigin {
		return nil, ErrInvalidConfig
	}
	if (cfg.EphemeralTenantID == "") != (cfg.EphemeralUserID == "") || len(cfg.EphemeralTenantID) > 256 || len(cfg.EphemeralUserID) > 256 || strings.ContainsAny(cfg.EphemeralTenantID+cfg.EphemeralUserID, "\x00\r\n") {
		return nil, ErrInvalidConfig
	}
	trusted := make([]*net.IPNet, 0, len(cfg.TrustedProxyCIDRs))
	for _, raw := range cfg.TrustedProxyCIDRs {
		_, network, err := net.ParseCIDR(raw)
		if err != nil {
			return nil, ErrInvalidConfig
		}
		trusted = append(trusted, network)
	}
	if cfg.BehindIngress && len(trusted) == 0 {
		return nil, ErrInvalidConfig
	}
	limits, err := newLimiter(cfg.GlobalRate, cfg.GlobalBurst, cfg.IPRate, cfg.IPBurst, cfg.IPKeyTTL, cfg.MaxIPKeys, cfg.Now)
	if err != nil {
		return nil, err
	}
	c := &Controller{authn: authn, cfg: cfg, trusted: trusted, origins: origins, limiter: limits}
	if len(stores) == 1 {
		c.store = stores[0]
	}
	if cfg.RequireStore && c.store == nil {
		return nil, ErrInvalidConfig
	}
	return c, nil
}

func (c *Controller) Admit(ctx context.Context, request *http.Request) (session.Subject, ReleaseFunc, error) {
	subject, release, _, err := c.admit(ctx, request, false)
	return subject, release, err
}

func (c *Controller) AdmitMonitored(ctx context.Context, request *http.Request) (session.Subject, ReleaseFunc, <-chan struct{}, error) {
	return c.admit(ctx, request, true)
}

func (c *Controller) admit(ctx context.Context, request *http.Request, monitor bool) (session.Subject, ReleaseFunc, <-chan struct{}, error) {
	result := "rejected"
	defer func() {
		if c != nil && c.metrics != nil {
			c.metrics(result)
		}
	}()
	if c == nil || request == nil || ctx == nil {
		return session.Subject{}, nil, nil, ErrMalformed
	}
	clientIP, peerTrusted, err := c.transport(request)
	if err != nil {
		return session.Subject{}, nil, nil, err
	}
	_ = peerTrusted
	if err := checkOrigin(request.Header, c.origins, c.cfg.AllowEmptyOrigin); err != nil {
		return session.Subject{}, nil, nil, err
	}
	if !c.limiter.allow(clientIP.String(), c.cfg.GlobalRate, c.cfg.GlobalBurst) {
		return session.Subject{}, nil, nil, ErrLimited
	}
	token, err := auth.Bearer(request.Header)
	if err != nil {
		return session.Subject{}, nil, nil, ErrUnauthorized
	}
	principal, err := c.authn.Authenticate(ctx, token)
	if err != nil {
		if errors.Is(err, auth.ErrForbidden) {
			return session.Subject{}, nil, nil, ErrForbidden
		}
		return session.Subject{}, nil, nil, ErrUnauthorized
	}
	if err := c.validatePrincipal(principal); err != nil {
		return session.Subject{}, nil, nil, ErrUnauthorized
	}
	for _, required := range c.cfg.RequiredScopes {
		if _, ok := principal.Scopes[required]; !ok {
			return session.Subject{}, nil, nil, ErrForbidden
		}
	}
	subject := session.Subject{TenantID: principal.TenantID, UserID: principal.UserID, Scopes: cloneScopes(principal.Scopes), ExpiresAt: principal.ExpiresAt}
	subject.Ephemeral = c.cfg.EphemeralTenantID != "" && principal.TenantID == c.cfg.EphemeralTenantID && principal.UserID == c.cfg.EphemeralUserID
	if c.store == nil {
		result = "ok"
		return subject, func() {}, nil, nil
	}
	if err := c.store.Ready(ctx); err != nil {
		return session.Subject{}, nil, nil, ErrUnavailable
	}
	if monitor {
		if store, ok := c.store.(MonitoredLeaseStore); ok {
			lease, err := store.AcquireMonitored(ctx, subject, c.cfg.Limits)
			if err != nil {
				if errors.Is(err, ErrLimited) {
					return session.Subject{}, nil, nil, ErrLimited
				}
				return session.Subject{}, nil, nil, ErrUnavailable
			}
			if lease.Release == nil || lease.Lost == nil {
				return session.Subject{}, nil, nil, ErrUnavailable
			}
			result = "ok"
			return subject, lease.Release, lease.Lost, nil
		}
	}
	release, err := c.store.Acquire(ctx, subject, c.cfg.Limits)
	if err != nil {
		if errors.Is(err, ErrLimited) {
			return session.Subject{}, nil, nil, ErrLimited
		}
		return session.Subject{}, nil, nil, ErrUnavailable
	}
	if release == nil {
		return session.Subject{}, nil, nil, ErrUnavailable
	}
	result = "ok"
	return subject, release, nil, nil
}

func (c *Controller) SetMetricsObserver(observer func(string)) {
	if c != nil {
		c.metrics = observer
	}
}

// Authenticate exposes the already configured strict authentication boundary
// for the separately loopback-bound admin server.
func (c *Controller) Authenticate(ctx context.Context, token string) (auth.Principal, error) {
	if c == nil || c.authn == nil {
		return auth.Principal{}, auth.ErrUnauthorized
	}
	return c.authn.Authenticate(ctx, token)
}

// Check reports authoritative distributed admission readiness.
func (c *Controller) Check(ctx context.Context) error {
	if c == nil {
		return ErrUnavailable
	}
	if c.store == nil {
		if c.cfg.RequireStore {
			return ErrUnavailable
		}
		return nil
	}
	return c.store.Ready(ctx)
}

func (c *Controller) validatePrincipal(p auth.Principal) error {
	if !safeIdentifier(p.TenantID) || !safeIdentifier(p.UserID) {
		return ErrUnauthorized
	}
	if len(p.Scopes) > 64 {
		return ErrUnauthorized
	}
	for scope := range p.Scopes {
		if scope == "" || len(scope) > 256 || !utf8.ValidString(scope) {
			return ErrUnauthorized
		}
	}
	if p.ExpiresAt.IsZero() && !p.IsStaticDevelopment() {
		return ErrUnauthorized
	}
	now := time.Now
	if c.cfg.Now != nil {
		now = c.cfg.Now
	}
	if !p.ExpiresAt.IsZero() && !p.ExpiresAt.After(now()) {
		return ErrUnauthorized
	}
	return nil
}

func safeIdentifier(value string) bool {
	if strings.TrimSpace(value) == "" || len(value) > 256 || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

func cloneScopes(in map[string]struct{}) map[string]struct{} {
	if in == nil {
		return nil
	}
	out := make(map[string]struct{}, len(in))
	for k := range in {
		out[k] = struct{}{}
	}
	return out
}

func (c *Controller) transport(r *http.Request) (net.IP, bool, error) {
	host, port, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil || port == "" {
		return nil, false, ErrMalformed
	}
	peer := net.ParseIP(host)
	if peer == nil {
		return nil, false, ErrMalformed
	}
	trusted := c.isTrusted(peer)
	forwardedProto := r.Header.Values("X-Forwarded-Proto")
	secure := r.TLS != nil
	if len(forwardedProto) > 0 {
		if !trusted || len(forwardedProto) != 1 || len(forwardedProto[0]) > 32 || strings.Contains(forwardedProto[0], ",") || forwardedProto[0] != "https" {
			return nil, trusted, ErrMalformed
		}
		secure = true
	}
	if c.cfg.RequireTLS && !secure {
		return nil, trusted, ErrMalformed
	}
	forwardedFor := r.Header.Values("X-Forwarded-For")
	if len(forwardedFor) == 0 {
		return peer, trusted, nil
	}
	if !trusted || len(forwardedFor) != 1 {
		return nil, trusted, ErrMalformed
	}
	if len(forwardedFor[0]) > 4096 {
		return nil, trusted, ErrMalformed
	}
	parts := strings.Split(forwardedFor[0], ",")
	if len(parts) == 0 || len(parts) > maxProxyHops {
		return nil, trusted, ErrMalformed
	}
	chain := make([]net.IP, len(parts))
	for i, raw := range parts {
		raw = strings.TrimSpace(raw)
		if raw == "" || strings.ContainsAny(raw, "\t\r\n ") {
			return nil, trusted, ErrMalformed
		}
		chain[i] = net.ParseIP(raw)
		if chain[i] == nil {
			return nil, trusted, ErrMalformed
		}
	}
	current := peer
	for i := len(chain) - 1; i >= 0; i-- {
		if !c.isTrusted(current) {
			return current, trusted, nil
		}
		current = chain[i]
	}
	return current, trusted, nil
}

func (c *Controller) isTrusted(ip net.IP) bool {
	for _, network := range c.trusted {
		if network.Contains(ip) {
			return true
		}
	}
	return false
}
