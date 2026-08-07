package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/coreos/go-oidc/v3/oidc"
)

const maxClaimsBytes = 64 << 10

type OIDCConfig struct {
	Issuer             string
	Audience           string
	TenantClaim        string
	UserClaim          string
	ScopeClaim         string
	RequiredScopes     []string
	AllowedAlgorithms  []string
	MaxKeySetStaleness time.Duration
	HTTPTimeout        time.Duration
	HTTPClient         *http.Client
	Now                func() time.Time
}

type OIDC struct {
	verifier *oidc.IDTokenVerifier
	cfg      OIDCConfig
	tracker  *jwksTracker
	err      error
}

func NewOIDC(ctx context.Context, cfg OIDCConfig) (*OIDC, error) {
	if cfg.TenantClaim == "" {
		cfg.TenantClaim = "tenant_id"
	}
	if cfg.UserClaim == "" {
		cfg.UserClaim = "sub"
	}
	if cfg.ScopeClaim == "" {
		cfg.ScopeClaim = "scope"
	}
	if len(cfg.AllowedAlgorithms) == 0 {
		cfg.AllowedAlgorithms = []string{"RS256"}
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.MaxKeySetStaleness <= 0 {
		cfg.MaxKeySetStaleness = 15 * time.Minute
	}
	if cfg.HTTPTimeout == 0 {
		cfg.HTTPTimeout = 5 * time.Second
	}
	cfg.RequiredScopes = append([]string(nil), cfg.RequiredScopes...)
	cfg.AllowedAlgorithms = append([]string(nil), cfg.AllowedAlgorithms...)
	if err := validateOIDCConfig(cfg); err != nil {
		return nil, err
	}
	client := &http.Client{}
	if cfg.HTTPClient != nil {
		*client = *cfg.HTTPClient
	}
	base := client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	bounded := &deadlineTransport{base: base, timeout: cfg.HTTPTimeout}
	tracker := &jwksTracker{base: bounded, now: cfg.Now, timeout: cfg.HTTPTimeout}
	tracked := *client
	tracked.Transport = tracker
	tracked.Timeout = cfg.HTTPTimeout
	tracker.client = &tracked
	discoveryCtx, discoveryCancel := context.WithTimeout(ctx, cfg.HTTPTimeout)
	defer discoveryCancel()
	providerCtx := oidc.ClientContext(discoveryCtx, &tracked)
	provider, err := oidc.NewProvider(providerCtx, cfg.Issuer)
	if err != nil {
		return nil, fmt.Errorf("configure OIDC provider: %w", err)
	}
	var metadata struct {
		JWKSURL string `json:"jwks_uri"`
	}
	if err := provider.Claims(&metadata); err != nil || metadata.JWKSURL == "" {
		return nil, errors.New("OIDC provider has no JWKS endpoint")
	}
	tracker.setJWKS(metadata.JWKSURL)
	verifier := provider.Verifier(&oidc.Config{ClientID: cfg.Audience, SupportedSigningAlgs: append([]string(nil), cfg.AllowedAlgorithms...), Now: cfg.Now})
	return &OIDC{verifier: verifier, cfg: cfg, tracker: tracker}, nil
}

func validateOIDCConfig(cfg OIDCConfig) error {
	u, err := url.Parse(cfg.Issuer)
	if err != nil || u.Scheme == "" || u.Host == "" || cfg.Audience == "" || len(cfg.Issuer) > 2048 || len(cfg.Audience) > 512 {
		return errors.New("invalid OIDC issuer or audience")
	}
	for _, name := range []string{cfg.TenantClaim, cfg.UserClaim, cfg.ScopeClaim} {
		if name == "" || len(name) > 128 || !utf8.ValidString(name) {
			return errors.New("invalid OIDC claim name")
		}
	}
	if len(cfg.RequiredScopes) > 64 || len(cfg.AllowedAlgorithms) > 8 || cfg.MaxKeySetStaleness > 24*time.Hour || cfg.HTTPTimeout < 100*time.Millisecond || cfg.HTTPTimeout > 30*time.Second {
		return errors.New("OIDC configuration exceeds bounds")
	}
	return nil
}

func (o *OIDC) Authenticate(ctx context.Context, raw string) (Principal, error) {
	if o == nil || o.verifier == nil || o.err != nil || !validToken(raw) {
		return Principal{}, ErrUnauthorized
	}
	idToken, err := o.verifier.Verify(ctx, raw)
	if err != nil {
		return Principal{}, ErrUnauthorized
	}
	if len(idToken.Audience) != 1 || idToken.Audience[0] != o.cfg.Audience {
		return Principal{}, ErrUnauthorized
	}
	kid, ok := jwtKeyID(raw)
	if !ok || !o.tracker.allows(ctx, kid, o.cfg.MaxKeySetStaleness) {
		return Principal{}, ErrUnauthorized
	}
	var rawClaims json.RawMessage
	if err := idToken.Claims(&rawClaims); err != nil || len(rawClaims) > maxClaimsBytes {
		return Principal{}, ErrUnauthorized
	}
	var claims map[string]json.RawMessage
	decoder := json.NewDecoder(strings.NewReader(string(rawClaims)))
	if err := decoder.Decode(&claims); err != nil || len(claims) > 128 {
		return Principal{}, ErrUnauthorized
	}
	tenant, ok := claimString(claims[o.cfg.TenantClaim])
	if !ok {
		return Principal{}, ErrUnauthorized
	}
	user, ok := claimString(claims[o.cfg.UserClaim])
	if !ok {
		return Principal{}, ErrUnauthorized
	}
	if len(tenant) > 256 || len(user) > 256 {
		return Principal{}, ErrUnauthorized
	}
	scopes, ok := claimScopes(claims[o.cfg.ScopeClaim])
	if !ok {
		return Principal{}, ErrUnauthorized
	}
	for _, required := range o.cfg.RequiredScopes {
		if _, exists := scopes[required]; !exists {
			return Principal{}, ErrForbidden
		}
	}
	if idToken.Expiry.IsZero() || !idToken.Expiry.After(o.cfg.Now()) {
		return Principal{}, ErrUnauthorized
	}
	if rawNBF, exists := claims["nbf"]; exists {
		var nbf json.Number
		dec := json.NewDecoder(strings.NewReader(string(rawNBF)))
		dec.UseNumber()
		if dec.Decode(&nbf) != nil {
			return Principal{}, ErrUnauthorized
		}
		seconds, err := nbf.Int64()
		if err != nil || o.cfg.Now().Before(time.Unix(seconds, 0)) {
			return Principal{}, ErrUnauthorized
		}
	}
	return Principal{TenantID: tenant, UserID: user, Scopes: scopes, ExpiresAt: idToken.Expiry}, nil
}

func jwtKeyID(raw string) (string, bool) {
	parts := strings.Split(raw, ".")
	if len(parts) != 3 || len(parts[0]) > 4096 {
		return "", false
	}
	header, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil || len(header) > 3072 {
		return "", false
	}
	decoder := json.NewDecoder(strings.NewReader(string(header)))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return "", false
	}
	values := make(map[string]json.RawMessage)
	for decoder.More() {
		nameToken, err := decoder.Token()
		if err != nil {
			return "", false
		}
		name, ok := nameToken.(string)
		if !ok {
			return "", false
		}
		if _, duplicate := values[name]; duplicate {
			return "", false
		}
		var value json.RawMessage
		if decoder.Decode(&value) != nil {
			return "", false
		}
		values[name] = value
	}
	if _, err = decoder.Token(); err != nil {
		return "", false
	}
	kid, ok := claimString(values["kid"])
	if !ok || len(kid) > 256 {
		return "", false
	}
	return kid, true
}

func claimString(raw json.RawMessage) (string, bool) {
	var value string
	if len(raw) == 0 || json.Unmarshal(raw, &value) != nil || strings.TrimSpace(value) == "" || !utf8.ValidString(value) {
		return "", false
	}
	return value, true
}

func claimScopes(raw json.RawMessage) (map[string]struct{}, bool) {
	if len(raw) == 0 {
		return map[string]struct{}{}, true
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		parts := strings.Fields(text)
		if len(parts) > 64 {
			return nil, false
		}
		out := make(map[string]struct{}, len(parts))
		for _, value := range parts {
			if len(value) > 256 {
				return nil, false
			}
			out[value] = struct{}{}
		}
		return out, true
	}
	var list []string
	if json.Unmarshal(raw, &list) != nil || len(list) > 64 {
		return nil, false
	}
	out := make(map[string]struct{}, len(list))
	for _, value := range list {
		if value == "" || len(value) > 256 {
			return nil, false
		}
		out[value] = struct{}{}
	}
	return out, true
}

type jwksTracker struct {
	base    http.RoundTripper
	client  *http.Client
	now     func() time.Time
	mu      sync.RWMutex
	last    time.Time
	jwksURL string
	kids    map[string]struct{}
	timeout time.Duration
	flight  *refreshFlight
}

type refreshFlight struct {
	done chan struct{}
	ok   bool
}
type refreshProbeKey struct{}

func (t *jwksTracker) setJWKS(value string) { t.mu.Lock(); t.jwksURL = value; t.mu.Unlock() }
func (t *jwksTracker) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := t.base.RoundTrip(request)
	if err == nil && response.StatusCode >= 200 && response.StatusCode < 300 && request.Context().Value(refreshProbeKey{}) == nil {
		t.mu.Lock()
		t.last = t.now()
		t.kids = nil
		t.mu.Unlock()
		response.Body = &boundedReadCloser{ReadCloser: response.Body, remaining: maxClaimsBytes * 8}
	}
	return response, err
}
func (t *jwksTracker) stale(max time.Duration) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.last.IsZero() || t.now().Sub(t.last) > max
}
func (t *jwksTracker) allows(ctx context.Context, kid string, max time.Duration) bool {
	if t.stale(max) {
		flight := t.startRefresh(max)
		select {
		case <-ctx.Done():
			return false
		case <-flight.done:
			if !flight.ok {
				return false
			}
		}
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.kids == nil {
		return true
	}
	_, ok := t.kids[kid]
	return ok
}
func (t *jwksTracker) startRefresh(max time.Duration) *refreshFlight {
	t.mu.Lock()
	if !t.last.IsZero() && t.now().Sub(t.last) <= max {
		f := &refreshFlight{done: make(chan struct{}), ok: true}
		close(f.done)
		t.mu.Unlock()
		return f
	}
	if t.flight != nil {
		f := t.flight
		t.mu.Unlock()
		return f
	}
	f := &refreshFlight{done: make(chan struct{})}
	t.flight = f
	t.mu.Unlock()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
		ok := t.refresh(ctx)
		cancel()
		t.mu.Lock()
		f.ok = ok
		t.flight = nil
		close(f.done)
		t.mu.Unlock()
	}()
	return f
}
func (t *jwksTracker) refresh(ctx context.Context) bool {
	t.mu.RLock()
	endpoint := t.jwksURL
	t.mu.RUnlock()
	request, err := http.NewRequestWithContext(context.WithValue(ctx, refreshProbeKey{}, true), http.MethodGet, endpoint, nil)
	if err != nil {
		return false
	}
	response, err := t.client.Do(request)
	if err != nil {
		return false
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return false
	}
	response.Body = &boundedReadCloser{ReadCloser: response.Body, remaining: maxClaimsBytes * 8}
	var keys struct {
		Keys []struct {
			Kid string `json:"kid"`
		} `json:"keys"`
	}
	if json.NewDecoder(response.Body).Decode(&keys) != nil || len(keys.Keys) == 0 || len(keys.Keys) > 128 {
		return false
	}
	keyIDs := make(map[string]struct{}, len(keys.Keys))
	for _, key := range keys.Keys {
		if key.Kid == "" || len(key.Kid) > 256 {
			return false
		}
		keyIDs[key.Kid] = struct{}{}
	}
	t.mu.Lock()
	t.last = t.now()
	t.kids = keyIDs
	t.mu.Unlock()
	return true
}

type deadlineTransport struct {
	base    http.RoundTripper
	timeout time.Duration
}

func (t *deadlineTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(request.Context(), t.timeout)
	response, err := t.base.RoundTrip(request.Clone(ctx))
	if err != nil {
		cancel()
		return nil, err
	}
	response.Body = &cancelReadCloser{ReadCloser: response.Body, cancel: cancel}
	return response, nil
}

type cancelReadCloser struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (r *cancelReadCloser) Close() error { err := r.ReadCloser.Close(); r.cancel(); return err }

type boundedReadCloser struct {
	io.ReadCloser
	remaining int64
}

func (r *boundedReadCloser) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, errors.New("OIDC response exceeds limit")
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.ReadCloser.Read(p)
	r.remaining -= int64(n)
	return n, err
}
