package admission

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/auth"
	"github.com/chuanmingliu/openai-gpt-live/internal/session"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordingAuth struct {
	calls     atomic.Int32
	principal auth.Principal
	err       error
}

func (a *recordingAuth) Authenticate(context.Context, string) (auth.Principal, error) {
	a.calls.Add(1)
	return auth.ClonePrincipal(a.principal), a.err
}

type recordingStore struct {
	readyErr, error error
	releases, calls atomic.Int32
}

func (s *recordingStore) Ready(context.Context) error { return s.readyErr }
func (s *recordingStore) Acquire(context.Context, session.Subject, Limits) (ReleaseFunc, error) {
	s.calls.Add(1)
	if s.error != nil {
		return nil, s.error
	}
	return func() { s.releases.Add(1) }, nil
}

func controllerConfig() Config {
	return Config{RequireTLS: true, AllowedOrigins: []string{"https://app.example"}, GlobalRate: 100, GlobalBurst: 10, IPRate: 100, IPBurst: 2, IPKeyTTL: time.Minute, MaxIPKeys: 10, RequiredScopes: []string{"realtime"}, Limits: Limits{Global: 10, PerTenant: 10, PerUser: 10}}
}
func validRequest() *http.Request {
	r := httptest.NewRequest(http.MethodGet, "https://gateway.example/v1/realtime", nil)
	r.RemoteAddr = "192.0.2.1:1234"
	r.TLS = &tls.ConnectionState{}
	r.Header.Set("Origin", "https://app.example")
	r.Header.Set("Authorization", "Bearer token")
	return r
}

func TestControllerPipelineAndSubjectCloning(t *testing.T) {
	a := &recordingAuth{principal: auth.Principal{TenantID: "tenant", UserID: "user", Scopes: map[string]struct{}{"realtime": {}}, ExpiresAt: time.Now().Add(time.Minute)}}
	store := &recordingStore{}
	c, err := New(a, controllerConfig(), store)
	require.NoError(t, err)
	subject, release, err := c.Admit(t.Context(), validRequest())
	require.NoError(t, err)
	require.NotNil(t, release)
	delete(subject.Scopes, "realtime")
	assert.Equal(t, int32(1), store.calls.Load())
	release()
	assert.Equal(t, int32(1), store.releases.Load())
}

func TestInvalidFloodConsumesBucketsBeforeAuthentication(t *testing.T) {
	a := &recordingAuth{err: auth.ErrUnauthorized}
	cfg := controllerConfig()
	cfg.IPBurst = 1
	c, err := New(a, cfg)
	require.NoError(t, err)
	_, _, err = c.Admit(t.Context(), validRequest())
	assert.ErrorIs(t, err, ErrUnauthorized)
	_, _, err = c.Admit(t.Context(), validRequest())
	assert.ErrorIs(t, err, ErrLimited)
	assert.Equal(t, int32(1), a.calls.Load())
}

func TestControllerRejectsTransportAndOriginSpoofing(t *testing.T) {
	tests := map[string]func(*http.Request){
		"plaintext":                 func(r *http.Request) { r.TLS = nil },
		"untrusted forwarded proto": func(r *http.Request) { r.TLS = nil; r.Header.Set("X-Forwarded-Proto", "https") },
		"subdomain origin":          func(r *http.Request) { r.Header.Set("Origin", "https://app.example.attacker") },
		"origin path":               func(r *http.Request) { r.Header.Set("Origin", "https://app.example/path") },
		"origin port":               func(r *http.Request) { r.Header.Set("Origin", "https://app.example:444") },
		"untrusted forwarded ip":    func(r *http.Request) { r.Header.Set("X-Forwarded-For", "198.51.100.9") },
		"malformed remote":          func(r *http.Request) { r.RemoteAddr = "192.0.2.1" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			a := &recordingAuth{}
			c, err := New(a, controllerConfig())
			require.NoError(t, err)
			r := validRequest()
			mutate(r)
			_, _, err = c.Admit(t.Context(), r)
			assert.ErrorIs(t, err, ErrMalformed)
			assert.Zero(t, a.calls.Load())
		})
	}
}

func TestEmptyOriginRequiresSeparateExplicitNonBrowserPolicy(t *testing.T) {
	a := &recordingAuth{principal: auth.Principal{TenantID: "tenant", UserID: "user", Scopes: map[string]struct{}{"realtime": {}}, ExpiresAt: time.Now().Add(time.Minute)}}
	cfg := controllerConfig()
	controller, err := New(a, cfg)
	require.NoError(t, err)
	request := validRequest()
	request.Header.Del("Origin")
	_, _, err = controller.Admit(t.Context(), request)
	assert.ErrorIs(t, err, ErrMalformed)
	cfg.AllowedOrigins = nil
	cfg.AllowEmptyOrigin = true
	controller, err = New(a, cfg)
	require.NoError(t, err)
	subject, release, err := controller.Admit(t.Context(), request)
	require.NoError(t, err)
	assert.Equal(t, "tenant", subject.TenantID)
	release()
}

func TestControllerClonesSecurityConfigAndZeroExpiryRequiresStaticCapability(t *testing.T) {
	required := []string{"realtime"}
	proxies := []string{"10.0.0.0/8"}
	origins := []string{"https://app.example"}
	cfg := controllerConfig()
	cfg.RequiredScopes = required
	cfg.TrustedProxyCIDRs = proxies
	cfg.AllowedOrigins = origins
	a := &recordingAuth{principal: auth.Principal{TenantID: "tenant", UserID: "user", Scopes: map[string]struct{}{"realtime": {}}, ExpiresAt: time.Now().Add(time.Minute)}}
	controller, err := New(a, cfg)
	require.NoError(t, err)
	required[0] = "admin"
	proxies[0] = "192.0.2.0/24"
	origins[0] = "https://attacker.example"
	_, release, err := controller.Admit(t.Context(), validRequest())
	require.NoError(t, err)
	release()
	zero := &recordingAuth{principal: auth.Principal{TenantID: "tenant", UserID: "user", Scopes: map[string]struct{}{"realtime": {}}}}
	controller, err = New(zero, controllerConfig())
	require.NoError(t, err)
	_, _, err = controller.Admit(t.Context(), validRequest())
	assert.ErrorIs(t, err, ErrUnauthorized)
	static := auth.NewStatic([]byte("token"), auth.Principal{TenantID: "tenant", UserID: "user", Scopes: map[string]struct{}{"realtime": {}}})
	controller, err = New(static, controllerConfig())
	require.NoError(t, err)
	_, release, err = controller.Admit(t.Context(), validRequest())
	require.NoError(t, err)
	release()
}

func TestTrustedProxyUsesDeterministicRightBoundary(t *testing.T) {
	a := &recordingAuth{principal: auth.Principal{TenantID: "tenant", UserID: "user", Scopes: map[string]struct{}{"realtime": {}}, ExpiresAt: time.Now().Add(time.Minute)}}
	cfg := controllerConfig()
	cfg.TrustedProxyCIDRs = []string{"10.0.0.0/8"}
	cfg.BehindIngress = true
	c, err := New(a, cfg)
	require.NoError(t, err)
	r := validRequest()
	r.RemoteAddr = "10.0.0.2:443"
	r.TLS = nil
	r.Header.Set("X-Forwarded-Proto", "https")
	r.Header.Set("X-Forwarded-For", "198.51.100.9,10.0.0.1")
	_, release, err := c.Admit(t.Context(), r)
	require.NoError(t, err)
	release()
	r.Header["X-Forwarded-Proto"] = []string{"https", "http"}
	_, _, err = c.Admit(t.Context(), r)
	assert.ErrorIs(t, err, ErrMalformed)
}

func TestTrustedProxyRejectsDuplicateForwardingAndResolvesAllTrustedChain(t *testing.T) {
	a := &recordingAuth{principal: auth.Principal{TenantID: "tenant", UserID: "user", Scopes: map[string]struct{}{"realtime": {}}, ExpiresAt: time.Now().Add(time.Minute)}}
	cfg := controllerConfig()
	cfg.TrustedProxyCIDRs = []string{"10.0.0.0/8"}
	cfg.BehindIngress = true
	c, err := New(a, cfg)
	require.NoError(t, err)
	r := validRequest()
	r.RemoteAddr = "10.0.0.3:443"
	r.TLS = nil
	r.Header.Set("X-Forwarded-Proto", "https")
	r.Header.Set("X-Forwarded-For", "10.0.0.1, 10.0.0.2")
	ip, _, err := c.transport(r)
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.1", ip.String())
	r.Header["X-Forwarded-For"] = []string{"198.51.100.1", "198.51.100.2"}
	_, _, err = c.Admit(t.Context(), r)
	assert.ErrorIs(t, err, ErrMalformed)
}

func TestOriginDefaultPortCanonicalizationIsExact(t *testing.T) {
	a := &recordingAuth{principal: auth.Principal{TenantID: "tenant", UserID: "user", Scopes: map[string]struct{}{"realtime": {}}, ExpiresAt: time.Now().Add(time.Minute)}}
	cfg := controllerConfig()
	cfg.AllowedOrigins = []string{"https://app.example:443"}
	c, err := New(a, cfg)
	require.NoError(t, err)
	_, release, err := c.Admit(t.Context(), validRequest())
	require.NoError(t, err)
	release()
}

func TestControllerReadinessAndQuotaErrorsAreTyped(t *testing.T) {
	a := &recordingAuth{principal: auth.Principal{TenantID: "tenant", UserID: "user", Scopes: map[string]struct{}{"realtime": {}}, ExpiresAt: time.Now().Add(time.Minute)}}
	store := &recordingStore{readyErr: errors.New("database detail")}
	c, err := New(a, controllerConfig(), store)
	require.NoError(t, err)
	_, _, err = c.Admit(t.Context(), validRequest())
	assert.ErrorIs(t, err, ErrUnavailable)
	assert.Zero(t, store.calls.Load())
	store.readyErr = nil
	_, release, err := c.Admit(t.Context(), validRequest())
	require.NoError(t, err)
	release()
}
