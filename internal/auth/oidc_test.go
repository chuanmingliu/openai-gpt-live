package auth

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/testkit"
	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOIDCValidatesClaimsScopeExpiryNBFAndRotation(t *testing.T) {
	server := testkit.NewJWKSServer(t)
	verifier, err := NewOIDC(t.Context(), OIDCConfig{Issuer: server.Issuer(), Audience: "live", RequiredScopes: []string{"realtime"}})
	require.NoError(t, err)
	valid := server.Token(t, "live", "tenant", "user", []string{"realtime"}, time.Now().Add(time.Minute), nil)
	principal, err := verifier.Authenticate(t.Context(), valid)
	require.NoError(t, err)
	assert.Equal(t, "tenant", principal.TenantID)
	assert.Contains(t, principal.Scopes, "realtime")
	assert.False(t, principal.IsStaticDevelopment())
	for name, token := range map[string]string{
		"issuer":             server.Token(t, "live", "tenant", "user", []string{"realtime"}, time.Now().Add(time.Minute), map[string]any{"iss": "https://wrong.example"}),
		"audience":           server.Token(t, "other", "tenant", "user", []string{"realtime"}, time.Now().Add(time.Minute), nil),
		"expired":            server.Token(t, "live", "tenant", "user", []string{"realtime"}, time.Now().Add(-time.Minute), nil),
		"scope":              server.Token(t, "live", "tenant", "user", []string{"other"}, time.Now().Add(time.Minute), nil),
		"nbf":                server.Token(t, "live", "tenant", "user", []string{"realtime"}, time.Now().Add(time.Minute), map[string]any{"nbf": time.Now().Add(time.Minute).Unix()}),
		"claim type":         server.Token(t, "live", "tenant", "user", []string{"realtime"}, time.Now().Add(time.Minute), map[string]any{"tenant_id": []string{"tenant"}}),
		"missing claim":      server.Token(t, "live", "tenant", "user", []string{"realtime"}, time.Now().Add(time.Minute), map[string]any{"tenant_id": nil}),
		"user type":          server.Token(t, "live", "tenant", "user", []string{"realtime"}, time.Now().Add(time.Minute), map[string]any{"sub": []string{"user"}}),
		"scope type":         server.Token(t, "live", "tenant", "user", []string{"realtime"}, time.Now().Add(time.Minute), map[string]any{"scope": map[string]any{"realtime": true}}),
		"ambiguous audience": server.Token(t, "live", "tenant", "user", []string{"realtime"}, time.Now().Add(time.Minute), map[string]any{"aud": []string{"live", "other"}}),
		"missing kid":        server.TokenWithKID(t, "live", "tenant", "user", []string{"realtime"}, time.Now().Add(time.Minute), nil, ""),
	} {
		t.Run(name, func(t *testing.T) { _, err := verifier.Authenticate(t.Context(), token); assert.Error(t, err) })
	}
	server.Rotate(t, "key-2")
	rotated := server.Token(t, "live", "tenant", "user", []string{"realtime"}, time.Now().Add(time.Minute), nil)
	_, err = verifier.Authenticate(t.Context(), rotated)
	require.NoError(t, err)
	hsSigner, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.HS256, Key: []byte("test-only-hmac-key-with-thirty-two-bytes")}, nil)
	require.NoError(t, err)
	hsToken, err := jwt.Signed(hsSigner).Claims(map[string]any{"iss": server.Issuer(), "aud": "live", "sub": "user", "tenant_id": "tenant", "scope": "realtime", "exp": time.Now().Add(time.Minute).Unix()}).Serialize()
	require.NoError(t, err)
	_, err = verifier.Authenticate(t.Context(), hsToken)
	assert.ErrorIs(t, err, ErrUnauthorized)
}

func TestJWTKeyIDRejectsMissingDuplicateAndOversize(t *testing.T) {
	makeToken := func(header string) string {
		return base64.RawURLEncoding.EncodeToString([]byte(header)) + ".payload.signature"
	}
	for _, header := range []string{`{"alg":"RS256"}`, `{"alg":"RS256","kid":"one","kid":"two"}`, `{"alg":"RS256","kid":"` + strings.Repeat("k", 257) + `"}`} {
		_, ok := jwtKeyID(makeToken(header))
		assert.False(t, ok)
	}
}

func TestOIDCConfigurationIsClonedAndHTTPIsBounded(t *testing.T) {
	server := testkit.NewJWKSServer(t)
	required := []string{"realtime"}
	algorithms := []string{"RS256"}
	verifier, err := NewOIDC(t.Context(), OIDCConfig{Issuer: server.Issuer(), Audience: "live", RequiredScopes: required, AllowedAlgorithms: algorithms, HTTPTimeout: 100 * time.Millisecond})
	require.NoError(t, err)
	required[0] = "weakened"
	algorithms[0] = "HS256"
	token := server.Token(t, "live", "tenant", "user", []string{"realtime"}, time.Now().Add(time.Minute), nil)
	_, err = verifier.Authenticate(t.Context(), token)
	require.NoError(t, err)
	server.Rotate(t, "slow-key")
	slowToken := server.Token(t, "live", "tenant", "user", []string{"realtime"}, time.Now().Add(time.Minute), nil)
	server.SetDelay(300 * time.Millisecond)
	started := time.Now()
	_, err = verifier.Authenticate(t.Context(), slowToken)
	require.Error(t, err)
	assert.Less(t, time.Since(started), 250*time.Millisecond)
	started = time.Now()
	_, err = NewOIDC(t.Context(), OIDCConfig{Issuer: server.Issuer(), Audience: "live", HTTPTimeout: 100 * time.Millisecond})
	require.Error(t, err)
	assert.Less(t, time.Since(started), 250*time.Millisecond)
}

func TestOIDCStaleRefreshWaitersAreCancelableAndSingleflight(t *testing.T) {
	server := testkit.NewJWKSServer(t)
	now := time.Now()
	verifier, err := NewOIDC(t.Context(), OIDCConfig{Issuer: server.Issuer(), Audience: "live", RequiredScopes: []string{"realtime"}, MaxKeySetStaleness: time.Second, HTTPTimeout: time.Second, Now: func() time.Time { return now }})
	require.NoError(t, err)
	token := server.Token(t, "live", "tenant", "user", []string{"realtime"}, now.Add(time.Minute), nil)
	_, err = verifier.Authenticate(t.Context(), token)
	require.NoError(t, err)
	before := server.KeyRequests()
	now = now.Add(2 * time.Second)
	server.SetDelay(150 * time.Millisecond)
	const callers = 12
	results := make(chan error, callers)
	for i := 0; i < callers; i++ {
		go func(index int) {
			ctx := t.Context()
			if index == 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 20*time.Millisecond)
				defer cancel()
			}
			_, err := verifier.Authenticate(ctx, token)
			results <- err
		}(i)
	}
	var canceled, success int
	for range callers {
		err := <-results
		if err != nil {
			canceled++
		} else {
			success++
		}
	}
	assert.Equal(t, 1, canceled)
	assert.Equal(t, callers-1, success)
	assert.Equal(t, before+1, server.KeyRequests())
}

func TestOIDCCachedKnownKeyBridgesOnlyBoundedObservedOutage(t *testing.T) {
	server := testkit.NewJWKSServer(t)
	now := time.Now()
	verifier, err := NewOIDC(t.Context(), OIDCConfig{Issuer: server.Issuer(), Audience: "live", RequiredScopes: []string{"realtime"}, MaxKeySetStaleness: time.Minute, Now: func() time.Time { return now }})
	require.NoError(t, err)
	known := server.Token(t, "live", "tenant", "user", []string{"realtime"}, now.Add(10*time.Minute), nil)
	_, err = verifier.Authenticate(t.Context(), known)
	require.NoError(t, err)
	now = now.Add(2 * time.Minute)
	_, err = verifier.Authenticate(t.Context(), known)
	require.NoError(t, err, "a reachable issuer must refresh bounded cache freshness")
	server.Rotate(t, "unknown-during-outage")
	unknown := server.Token(t, "live", "tenant", "user", []string{"realtime"}, now.Add(10*time.Minute), nil)
	server.SetUnavailable(true)
	_, err = verifier.Authenticate(t.Context(), unknown)
	assert.ErrorIs(t, err, ErrUnauthorized)
	_, err = verifier.Authenticate(t.Context(), known)
	require.NoError(t, err)
	now = now.Add(2 * time.Minute)
	_, err = verifier.Authenticate(t.Context(), known)
	assert.ErrorIs(t, err, ErrUnauthorized)
}

func TestOIDCRefreshRejectsRemovedPreviouslyCachedKey(t *testing.T) {
	server := testkit.NewJWKSServer(t)
	now := time.Now()
	verifier, err := NewOIDC(t.Context(), OIDCConfig{Issuer: server.Issuer(), Audience: "live", RequiredScopes: []string{"realtime"}, MaxKeySetStaleness: time.Minute, Now: func() time.Time { return now }})
	require.NoError(t, err)
	old := server.Token(t, "live", "tenant", "user", []string{"realtime"}, now.Add(10*time.Minute), nil)
	_, err = verifier.Authenticate(t.Context(), old)
	require.NoError(t, err)
	server.Rotate(t, "replacement")
	now = now.Add(2 * time.Minute)
	_, err = verifier.Authenticate(t.Context(), old)
	assert.ErrorIs(t, err, ErrUnauthorized)
}
