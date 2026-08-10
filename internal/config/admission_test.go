package config

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdmissionDefaultsAreBounded(t *testing.T) {
	cfg, err := loadFixture(t, "mode: local\n", nil)
	require.NoError(t, err)
	assert.Equal(t, 1000, cfg.Admission.MaxSessions)
	assert.Equal(t, 100, cfg.Admission.MaxSessionsPerTenant)
	assert.Equal(t, 10, cfg.Admission.MaxSessionsPerUser)
	assert.Equal(t, 30*time.Second, cfg.Admission.LeaseDuration)
	assert.Equal(t, 10*time.Second, cfg.Admission.HeartbeatInterval)
	assert.Equal(t, 10_000, cfg.Admission.MaxIPKeys)
	assert.Equal(t, 5*time.Second, cfg.Auth.OIDCHTTPTimeout)
	_, err = loadFixture(t, "auth:\n  oidc_http_timeout: 50ms\n", nil)
	assert.ErrorContains(t, err, "cache outage")
}

func TestProductionTransportModeIsExplicitAndUnambiguous(t *testing.T) {
	base := `mode: production
%s
store:
  database_url_env: DATABASE_URL
  retention:
    hmac_key_env: RETENTION_HMAC_KEY
    hmac_key_id: retention-v1
auth:
  oidc_issuer: https://issuer.example
  oidc_audience: live
  oidc_required_scopes: [realtime]
  allowed_origins: [https://app.example]
`
	lookup := func(name string) (string, bool) {
		if name == "DATABASE_URL" {
			return "postgres://db.example/live?sslmode=require", true
		}
		return "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=", name == "RETENTION_HMAC_KEY" || name == "RECONNECT_HMAC_KEY"
	}
	tests := []struct {
		name, transport string
		ok              bool
	}{
		{"direct", "gateway:\n  tls:\n    cert_file: server.crt\n    key_file: server.key", true},
		{"ingress", "gateway:\n  behind_ingress: true\n  trusted_proxies: [10.0.0.0/8]", true},
		{"none", "gateway: {}", false},
		{"cert only", "gateway:\n  tls:\n    cert_file: server.crt", false},
		{"key only", "gateway:\n  tls:\n    key_file: server.key", false},
		{"ingress no proxies", "gateway:\n  behind_ingress: true", false},
		{"both modes", "gateway:\n  behind_ingress: true\n  trusted_proxies: [10.0.0.0/8]\n  tls:\n    cert_file: server.crt\n    key_file: server.key", false},
		{"direct with proxies", "gateway:\n  trusted_proxies: [10.0.0.0/8]\n  tls:\n    cert_file: server.crt\n    key_file: server.key", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			yaml := strings.Replace(fmt.Sprintf(base, test.transport), "gateway:\n", "gateway:\n  reconnect: {hmac_key_env: RECONNECT_HMAC_KEY, hmac_key_id: reconnect-v1}\n", 1)
			_, err := loadFixture(t, yaml, lookup)
			if test.ok {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestProductionRequiresOIDCOriginAndPostgres(t *testing.T) {
	_, err := loadFixture(t, "mode: production\n", nil)
	assert.ErrorContains(t, err, "direct mode")
	yaml := `mode: production
gateway:
  reconnect: {hmac_key_env: RECONNECT_HMAC_KEY, hmac_key_id: reconnect-v1}
  tls:
    cert_file: server.crt
    key_file: server.key
store:
  database_url_env: DATABASE_URL
  retention:
    hmac_key_env: RETENTION_HMAC_KEY
    hmac_key_id: retention-v1
auth:
  oidc_issuer: https://issuer.example
  oidc_audience: live
  oidc_required_scopes: [realtime]
  allowed_origins: [https://app.example]
`
	cfg, err := loadFixture(t, yaml, func(name string) (string, bool) {
		if name == "DATABASE_URL" {
			return "postgres://db.example/live?sslmode=require", true
		}
		return "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=", name == "RETENTION_HMAC_KEY" || name == "RECONNECT_HMAC_KEY"
	})
	require.NoError(t, err)
	assert.Equal(t, "tenant_id", cfg.Auth.OIDCTenantClaim)
}

func TestProductionRetentionHMACRequiresBothReferenceAndValidResolvedKey(t *testing.T) {
	base := `mode: production
gateway:
  reconnect: {hmac_key_env: RECONNECT_HMAC_KEY, hmac_key_id: reconnect-v1}
  tls:
    cert_file: server.crt
    key_file: server.key
store:
  database_url_env: DATABASE_URL
%s
auth:
  oidc_issuer: https://issuer.example
  oidc_audience: live
  oidc_required_scopes: [realtime]
  allowed_origins: [https://app.example]
`
	lookup := func(name string) (string, bool) {
		switch name {
		case "DATABASE_URL":
			return "postgres://db.example/live?sslmode=require", true
		case "RETENTION_HMAC_KEY":
			return "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=", true
		case "RECONNECT_HMAC_KEY":
			return "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=", true
		default:
			return "", false
		}
	}
	_, err := loadFixture(t, fmt.Sprintf(base, ""), lookup)
	assert.ErrorContains(t, err, "retention HMAC")
	_, err = loadFixture(t, fmt.Sprintf(base, "  retention:\n    hmac_key_env: RETENTION_HMAC_KEY"), lookup)
	assert.ErrorContains(t, err, "configured together")
	_, err = loadFixture(t, fmt.Sprintf(base, "  retention:\n    hmac_key_env: RETENTION_HMAC_KEY\n    hmac_key_id: retention-v1"), func(name string) (string, bool) {
		if name == "RETENTION_HMAC_KEY" {
			return "not-base64", true
		}
		return lookup(name)
	})
	assert.ErrorContains(t, err, "base64-encoded HMAC key")
	_, err = loadFixture(t, fmt.Sprintf(base, "  retention:\n    hmac_key_env: RETENTION_HMAC_KEY\n    hmac_key_id: retention-v1"), func(name string) (string, bool) {
		if name == "RETENTION_HMAC_KEY" {
			return base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{7}, 31)), true
		}
		return lookup(name)
	})
	assert.ErrorContains(t, err, "base64-encoded HMAC key")
	_, err = loadFixture(t, fmt.Sprintf(base, "  retention:\n    hmac_key_env: RETENTION_HMAC_KEY\n    hmac_key_id: retention-v1"), lookup)
	require.NoError(t, err)
}

func TestStaticAuthenticationIsDevelopmentOnlyAndRequiresIdentity(t *testing.T) {
	_, err := loadFixture(t, "auth:\n  static_bearer_env: STATIC_TOKEN\n", func(string) (string, bool) { return "value", true })
	assert.ErrorContains(t, err, "static authentication")
	_, err = loadFixture(t, "mode: production\ngateway:\n  tls:\n    cert_file: server.crt\n    key_file: server.key\nauth:\n  static_bearer_env: STATIC_TOKEN\n  static_tenant_id: tenant\n  static_user_id: user\n", func(string) (string, bool) { return "value", true })
	assert.ErrorContains(t, err, "static authentication")
}
