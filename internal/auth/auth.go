// Package auth authenticates bearer credentials into trusted principals.
package auth

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"
)

const MaxBearerBytes = 16 << 10

var (
	ErrUnauthorized = errors.New("authentication failed")
	ErrForbidden    = errors.New("required scope is missing")
)

type Principal struct {
	TenantID          string
	UserID            string
	Scopes            map[string]struct{}
	ExpiresAt         time.Time
	staticDevelopment bool
}

type Authenticator interface {
	Authenticate(context.Context, string) (Principal, error)
}

// Bearer extracts exactly one strict Bearer credential. It intentionally does
// not include the rejected value in any returned error.
func Bearer(header http.Header) (string, error) {
	values := header.Values("Authorization")
	if len(values) != 1 {
		return "", ErrUnauthorized
	}
	value := values[0]
	if len(value) > MaxBearerBytes+7 || !utf8.ValidString(value) || strings.ContainsAny(value, "\r\n\x00") || strings.Contains(value, ",") {
		return "", ErrUnauthorized
	}
	if !strings.HasPrefix(value, "Bearer ") || strings.Count(value, " ") != 1 {
		return "", ErrUnauthorized
	}
	token := value[len("Bearer "):]
	if !validToken(token) {
		return "", ErrUnauthorized
	}
	return token, nil
}

func ClonePrincipal(principal Principal) Principal {
	copy := principal
	copy.Scopes = cloneScopes(principal.Scopes)
	return copy
}

// IsStaticDevelopment reports the unforgeable zero-expiry capability attached
// only by validated Static authentication. Callers cannot construct it.
func (p Principal) IsStaticDevelopment() bool { return p.staticDevelopment }

func validToken(token string) bool {
	if token == "" || len(token) > MaxBearerBytes {
		return false
	}
	equals := false
	for i := 0; i < len(token); i++ {
		b := token[i]
		if b == '=' {
			equals = true
			continue
		}
		if equals {
			return false
		}
		if b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || strings.ContainsRune("-._~+/", rune(b)) {
			continue
		}
		return false
	}
	return true
}

func cloneScopes(scopes map[string]struct{}) map[string]struct{} {
	if scopes == nil {
		return nil
	}
	copy := make(map[string]struct{}, len(scopes))
	for scope := range scopes {
		copy[scope] = struct{}{}
	}
	return copy
}
