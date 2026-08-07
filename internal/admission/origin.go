package admission

import (
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"
)

const (
	maxOrigins     = 128
	maxOriginBytes = 2048
	maxProxyHops   = 32
)

func canonicalOrigin(raw string) (string, error) {
	if raw == "" || len(raw) > maxOriginBytes || strings.Contains(raw, "*") {
		return "", errors.New("invalid origin")
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" || u.Opaque != "" {
		return "", errors.New("invalid origin")
	}
	host := strings.ToLower(u.Hostname())
	if host == "" {
		return "", errors.New("invalid origin")
	}
	port := u.Port()
	if port == "" {
		if u.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	return strings.ToLower(u.Scheme) + "://" + net.JoinHostPort(host, port), nil
}

func parseAllowedOrigins(values []string) (map[string]struct{}, error) {
	if len(values) > maxOrigins {
		return nil, errors.New("too many allowed origins")
	}
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		canonical, err := canonicalOrigin(value)
		if err != nil {
			return nil, err
		}
		out[canonical] = struct{}{}
	}
	return out, nil
}

func checkOrigin(header http.Header, allowed map[string]struct{}, allowEmpty bool) error {
	values := header.Values("Origin")
	if len(values) == 0 || len(values) == 1 && values[0] == "" {
		if allowEmpty {
			return nil
		}
		return ErrMalformed
	}
	if len(values) != 1 {
		return ErrMalformed
	}
	canonical, err := canonicalOrigin(values[0])
	if err != nil {
		return ErrMalformed
	}
	if _, ok := allowed[canonical]; !ok {
		return ErrMalformed
	}
	return nil
}
