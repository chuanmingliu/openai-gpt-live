package auth

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBearerIsStrictAndBounded(t *testing.T) {
	valid := http.Header{"Authorization": []string{"Bearer token"}}
	token, err := Bearer(valid)
	require.NoError(t, err)
	assert.Equal(t, "token", token)
	for name, header := range map[string]http.Header{
		"missing": {}, "duplicate": {"Authorization": []string{"Bearer a", "Bearer b"}}, "joined": {"Authorization": []string{"Bearer a, Bearer b"}}, "empty": {"Authorization": []string{"Bearer "}}, "wrong scheme": {"Authorization": []string{"bearer a"}}, "extra whitespace": {"Authorization": []string{"Bearer  a"}}, "control": {"Authorization": []string{"Bearer a\x00b"}}, "non utf8": {"Authorization": []string{"Bearer \xff"}}, "overlong": {"Authorization": []string{"Bearer " + strings.Repeat("a", MaxBearerBytes+1)}},
	} {
		t.Run(name, func(t *testing.T) { _, err := Bearer(header); assert.ErrorIs(t, err, ErrUnauthorized) })
	}
	for _, value := range []string{"\u0085", "\u00a0", "\u2028", "abc=def", "abc,"} {
		_, err := Bearer(http.Header{"Authorization": []string{"Bearer " + value}})
		assert.ErrorIs(t, err, ErrUnauthorized)
	}
	boundary := strings.Repeat("a", MaxBearerBytes)
	got, err := Bearer(http.Header{"Authorization": []string{"Bearer " + boundary}})
	require.NoError(t, err)
	assert.Len(t, got, MaxBearerBytes)
	_, err = Bearer(http.Header{"Authorization": []string{"Bearer " + boundary + "a"}})
	assert.ErrorIs(t, err, ErrUnauthorized)
}

func TestStaticAuthenticationClonesAndSupportsZeroExpiry(t *testing.T) {
	scopes := map[string]struct{}{"realtime": {}}
	token := []byte("development-token")
	static := NewStatic(token, Principal{TenantID: "tenant", UserID: "user", Scopes: scopes})
	token[0] = 'X'
	delete(scopes, "realtime")
	first, err := static.Authenticate(t.Context(), "development-token")
	require.NoError(t, err)
	assert.True(t, first.ExpiresAt.IsZero())
	assert.True(t, first.IsStaticDevelopment())
	require.Contains(t, first.Scopes, "realtime")
	delete(first.Scopes, "realtime")
	second, err := static.Authenticate(t.Context(), "development-token")
	require.NoError(t, err)
	assert.Contains(t, second.Scopes, "realtime")
	_, err = static.Authenticate(t.Context(), "development-toke")
	assert.ErrorIs(t, err, ErrUnauthorized)
	_, err = static.Authenticate(t.Context(), "development-token-x")
	assert.ErrorIs(t, err, ErrUnauthorized)
}

func TestStaticRejectsNonASCIIAndHonorsExactBoundary(t *testing.T) {
	principal := Principal{TenantID: "tenant", UserID: "user"}
	for _, token := range []string{"abc\u0085", "abc\u00a0", "abc\u2028"} {
		_, err := NewStatic([]byte(token), principal).Authenticate(t.Context(), token)
		assert.ErrorIs(t, err, ErrUnauthorized)
	}
	boundary := strings.Repeat("a", MaxBearerBytes)
	_, err := NewStatic([]byte(boundary), principal).Authenticate(t.Context(), boundary)
	require.NoError(t, err)
	_, err = NewStatic([]byte(boundary+"a"), principal).Authenticate(t.Context(), boundary+"a")
	assert.ErrorIs(t, err, ErrUnauthorized)
}
