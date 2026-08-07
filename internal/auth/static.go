package auth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"strings"
	"unicode/utf8"
)

type Static struct {
	digest    [sha256.Size]byte
	principal Principal
	err       error
}

func NewStatic(token []byte, principal Principal) *Static {
	s := &Static{principal: ClonePrincipal(principal)}
	copyToken := append([]byte(nil), token...)
	s.digest = sha256.Sum256(copyToken)
	for i := range copyToken {
		copyToken[i] = 0
	}
	if !validToken(string(token)) || !utf8.Valid(token) || strings.TrimSpace(principal.TenantID) == "" || strings.TrimSpace(principal.UserID) == "" {
		s.err = errors.New("invalid static authentication configuration")
	}
	if principal.ExpiresAt.IsZero() {
		s.principal.staticDevelopment = true
	}
	return s
}

func (s *Static) Authenticate(ctx context.Context, token string) (Principal, error) {
	if s == nil || s.err != nil || ctx == nil {
		return Principal{}, ErrUnauthorized
	}
	if err := ctx.Err(); err != nil {
		return Principal{}, err
	}
	candidate := sha256.Sum256([]byte(token))
	if subtle.ConstantTimeCompare(s.digest[:], candidate[:]) != 1 {
		return Principal{}, ErrUnauthorized
	}
	return ClonePrincipal(s.principal), nil
}
