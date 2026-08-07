package testkit

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
)

// JWKSServer is a process-local OIDC issuer intended only for tests.
type JWKSServer struct {
	Server      *httptest.Server
	mu          sync.RWMutex
	keys        map[string]*rsa.PrivateKey
	current     string
	unavailable bool
	delay       time.Duration
	keyRequests atomic.Int32
}

func NewJWKSServer(t testing.TB) *JWKSServer {
	t.Helper()
	s := &JWKSServer{keys: make(map[string]*rsa.PrivateKey)}
	s.Rotate(t, "key-1")
	s.Server = httptest.NewServer(http.HandlerFunc(s.serveHTTP))
	t.Cleanup(s.Server.Close)
	return s
}

func (s *JWKSServer) Issuer() string { return s.Server.URL }

func (s *JWKSServer) Rotate(t testing.TB, kid string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal("generate in-memory JWKS key")
	}
	s.mu.Lock()
	s.keys[kid] = key
	s.current = kid
	s.mu.Unlock()
}

func (s *JWKSServer) SetUnavailable(value bool)    { s.mu.Lock(); s.unavailable = value; s.mu.Unlock() }
func (s *JWKSServer) SetDelay(value time.Duration) { s.mu.Lock(); s.delay = value; s.mu.Unlock() }
func (s *JWKSServer) KeyRequests() int32           { return s.keyRequests.Load() }

func (s *JWKSServer) Token(t testing.TB, audience, tenant, user string, scopes []string, expiry time.Time, extra map[string]any) string {
	return s.TokenWithKID(t, audience, tenant, user, scopes, expiry, extra, s.currentKID())
}
func (s *JWKSServer) currentKID() string { s.mu.RLock(); defer s.mu.RUnlock(); return s.current }
func (s *JWKSServer) TokenWithKID(t testing.TB, audience, tenant, user string, scopes []string, expiry time.Time, extra map[string]any, headerKID string) string {
	t.Helper()
	s.mu.RLock()
	key := s.keys[s.current]
	s.mu.RUnlock()
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: jose.JSONWebKey{Key: key, KeyID: headerKID, Algorithm: string(jose.RS256), Use: "sig"}}, nil)
	if err != nil {
		t.Fatal("create in-memory JWT signer")
	}
	claims := map[string]any{"iss": s.Issuer(), "aud": audience, "sub": user, "tenant_id": tenant, "scope": scopes, "iat": time.Now().Unix(), "exp": expiry.Unix()}
	for name, value := range extra {
		claims[name] = value
	}
	raw, err := jwt.Signed(signer).Claims(claims).Serialize()
	if err != nil {
		t.Fatal("sign in-memory JWT")
	}
	return raw
}

func (s *JWKSServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	unavailable := s.unavailable
	delay := s.delay
	current := s.current
	key := s.keys[current]
	s.mu.RUnlock()
	if delay > 0 {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(delay):
		}
	}
	if unavailable {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	switch r.URL.Path {
	case "/.well-known/openid-configuration":
		_ = json.NewEncoder(w).Encode(map[string]any{"issuer": s.Issuer(), "jwks_uri": s.Issuer() + "/keys", "authorization_endpoint": s.Issuer() + "/authorize", "token_endpoint": s.Issuer() + "/token", "id_token_signing_alg_values_supported": []string{"RS256"}})
	case "/keys":
		s.keyRequests.Add(1)
		_ = json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: &key.PublicKey, KeyID: current, Algorithm: string(jose.RS256), Use: "sig"}}})
	default:
		http.NotFound(w, r)
	}
}
