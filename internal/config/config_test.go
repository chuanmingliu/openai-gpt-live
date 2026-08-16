package config

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToolEncryptionKeyRequiresImmutableConfiguredID(t *testing.T) {
	key := base64.StdEncoding.EncodeToString(make([]byte, 32))
	lookup := func(name string) (string, bool) { return key, name == "TOOL_KEY" }
	_, err := loadFixture(t, "features:\n  tool_encryption_key_env: TOOL_KEY\n", lookup)
	assert.ErrorContains(t, err, "tool_encryption_key_id")
	cfg, err := loadFixture(t, "features:\n  tool_encryption_key_env: TOOL_KEY\n  tool_encryption_key_id: rotation-2026-v2\n", lookup)
	require.NoError(t, err)
	assert.Equal(t, "rotation-2026-v2", cfg.Features.ToolEncryptionKeyID)
	_, err = loadFixture(t, "features:\n  tool_encryption_key_env: TOOL_KEY\n  tool_encryption_key_id: invalid/key\n", lookup)
	assert.ErrorContains(t, err, "tool_encryption_key_id")
}

func TestEnabledWebSearchDefaultsEncryptionSecretReferenceAndRequiresKeyID(t *testing.T) {
	key := base64.StdEncoding.EncodeToString(make([]byte, 32))
	lookup := func(name string) (string, bool) { return key, name == "LIVE_TOOL_ENCRYPTION_KEY" }
	_, err := loadFixture(t, "tools:\n  web_search: {enabled: true}\n", lookup)
	assert.ErrorContains(t, err, "tool_encryption_key_id")

	cfg, err := loadFixture(t, "features:\n  tool_encryption_key_id: search-v1\ntools:\n  web_search: {enabled: true}\n", lookup)
	require.NoError(t, err)
	assert.False(t, cfg.Features.SideEffectingTools)
	assert.Equal(t, "LIVE_TOOL_ENCRYPTION_KEY", cfg.Features.ToolEncryptionKeyEnv)
	assert.Equal(t, "search-v1", cfg.Features.ToolEncryptionKeyID)

	direct := validConfig(t)
	direct.Features.ToolEncryptionKeyEnv = ""
	direct.Features.ToolEncryptionKeyID = ""
	require.ErrorContains(t, direct.Validate(), "tool_encryption_key_env")
}

func TestProductionReconnectAuthorityIsExplicitAndSecretIsBounded(t *testing.T) {
	const production = `
mode: production
gateway:
  behind_ingress: true
  trusted_proxies: [127.0.0.1/32]
  voice: {url: wss://voice.example/v1/realtime}
store:
  url: postgres://db.example/live?sslmode=require
  retention: {hmac_key_env: RETENTION_KEY, hmac_key_id: retention-v1}
auth:
  oidc_issuer: https://issuer.example
  oidc_audience: live
  oidc_required_scopes: [realtime]
  allowed_origins: [https://app.example]
admission: {instance_id: gateway-a}
`
	key := base64.StdEncoding.EncodeToString(make([]byte, 32))
	lookup := func(name string) (string, bool) { return key, name == "RETENTION_KEY" || name == "RECONNECT_KEY" }

	_, err := loadFixture(t, production, lookup)
	assert.ErrorContains(t, err, "reconnect HMAC")

	withReconnect := strings.Replace(production, "  voice:", "  reconnect: {hmac_key_env: RECONNECT_KEY, hmac_key_id: reconnect-v1, credential_ttl: 5m}\n  voice:", 1)
	cfg, err := loadFixture(t, withReconnect, lookup)
	require.NoError(t, err)
	assert.Equal(t, "reconnect-v1", cfg.Gateway.Reconnect.HMACKeyID)
	assert.Equal(t, 5*time.Minute, cfg.Gateway.Reconnect.CredentialTTL)

	short := base64.StdEncoding.EncodeToString(make([]byte, 31))
	_, err = loadFixture(t, withReconnect, func(name string) (string, bool) {
		if name == "RECONNECT_KEY" {
			return short, true
		}
		return key, name == "RETENTION_KEY"
	})
	assert.ErrorContains(t, err, "RECONNECT_KEY")
}

func TestShutdownDrainTimeoutDefaultsAndIsBounded(t *testing.T) {
	cfg, err := loadFixture(t, "mode: local\n", nil)
	require.NoError(t, err)
	assert.Equal(t, 10*time.Second, cfg.Shutdown.DrainTimeout)
	_, err = loadFixture(t, "shutdown:\n  drain_timeout: -1s\n", nil)
	assert.ErrorContains(t, err, "drain_timeout")
}

func TestRuntimeFeatureRefreshDefaultsAndIsBounded(t *testing.T) {
	cfg, err := loadFixture(t, "mode: local\n", nil)
	require.NoError(t, err)
	assert.Equal(t, time.Second, cfg.Features.RefreshInterval)
	_, err = loadFixture(t, "features:\n  runtime_refresh_interval: 1ms\n", nil)
	assert.ErrorContains(t, err, "runtime_refresh_interval")
	_, err = loadFixture(t, "features:\n  runtime_refresh_interval: 2m\n", nil)
	assert.ErrorContains(t, err, "runtime_refresh_interval")
}

func TestVerticalProfilesChangeConfigHashAndRequireDurability(t *testing.T) {
	cfg := validConfig(t)
	cfg.Features.VerticalProfiles = VerticalProfileConfig{Enabled: true, RenderWorkers: 4, RenderQueue: 64}
	require.ErrorContains(t, cfg.Validate(), "vertical profiles")

	cfg.Mode = "development_durable"
	require.NoError(t, cfg.Validate())

	disabled := cfg
	disabled.Features.VerticalProfiles = VerticalProfileConfig{}
	assert.NotEqual(t, mustHash(t, disabled), mustHash(t, cfg))
}

func TestVerticalProfileRendererBoundsDefaultOnlyWhenEnabled(t *testing.T) {
	cfg := validConfig(t)
	cfg.Mode = "development_durable"
	cfg.Features.VerticalProfiles.Enabled = true
	cfg.applyDefaults()
	require.Equal(t, VerticalProfileConfig{Enabled: true, RenderWorkers: 4, RenderQueue: 64}, cfg.Features.VerticalProfiles)
	require.NoError(t, cfg.Validate())

	for _, invalid := range []VerticalProfileConfig{
		{Enabled: true, RenderWorkers: 0, RenderQueue: 64},
		{Enabled: true, RenderWorkers: 65, RenderQueue: 64},
		{Enabled: true, RenderWorkers: 4, RenderQueue: 0},
		{Enabled: true, RenderWorkers: 4, RenderQueue: 1_025},
		{RenderWorkers: 4, RenderQueue: 64},
	} {
		candidate := cfg
		candidate.Features.VerticalProfiles = invalid
		assert.ErrorContains(t, candidate.Validate(), "vertical profiles")
	}
}

func TestDevelopmentDurableSeparatesSecurityAndDurabilitySemantics(t *testing.T) {
	cfg := validConfig(t)
	cfg.Mode = "development_durable"
	cfg.Auth.StaticBearerEnv = "LIVE_STATIC_BEARER_TOKEN"
	cfg.Store.DatabaseURLEnv = "DATABASE_URL"
	cfg.Features.Handoffs = true

	require.NoError(t, cfg.Validate())
	assert.False(t, cfg.IsProduction())
	assert.True(t, cfg.IsDurable())
	assert.True(t, cfg.UsesStaticAuth())
}

func TestDevelopmentDurableRequiresDurablePrerequisites(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{"static identity", func(cfg *Config) { cfg.Auth.StaticUserID = "" }},
		{"database reference", func(cfg *Config) { cfg.Store.DatabaseURLEnv = "" }},
		{"retention HMAC", func(cfg *Config) { cfg.Store.Retention.HMACKeyEnv = ""; cfg.Store.Retention.HMACKeyID = "" }},
		{"reconnect HMAC", func(cfg *Config) { cfg.Gateway.Reconnect.HMACKeyEnv = ""; cfg.Gateway.Reconnect.HMACKeyID = "" }},
		{"handoffs", func(cfg *Config) { cfg.Features.Handoffs = false }},
		{"enabled tool", func(cfg *Config) { cfg.Tools.WebSearch.Enabled = false }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := validConfig(t)
			cfg.Mode = "development_durable"
			test.mutate(&cfg)
			assert.Error(t, cfg.Validate())
		})
	}
}

func TestDirectConfigWithDisabledZeroValueToolCatalogUsesReviewedDefaults(t *testing.T) {
	cfg := validConfig(t)
	cfg.Tools = ToolCatalogConfig{}

	require.NoError(t, cfg.Validate())
	catalog := cfg.ToolCatalog().WebSearch
	assert.False(t, catalog.Enabled)
	assert.Equal(t, []string{"au-en", "ca-en", "cn-zh", "uk-en", "us-en", "wt-wt"}, catalog.Regions)
	assert.Equal(t, 5, catalog.MaximumResults)
	assert.Equal(t, 8_000, catalog.MaximumTimeoutMS)

	normalized := cfg
	normalized.Tools.WebSearch = WebSearchToolConfig{
		Regions:    []string{"wt-wt", "us-en", "uk-en", "au-en", "ca-en", "cn-zh"},
		MaxResults: 5,
		Timeout:    8 * time.Second,
	}
	assert.Equal(t, mustHash(t, normalized), mustHash(t, cfg))
}

// TestLoadAppliesRealtimeDefaults catches a loader regression that leaves realtime
// queues or PCM sample rate at zero when the operator omits optional limits.
func TestLoadAppliesRealtimeDefaults(t *testing.T) {
	cfg, err := loadFixture(t, "gateway:\n  voice:\n    url: ws://127.0.0.1:8000/v1/realtime\n", nil)
	require.NoError(t, err)
	assert.Equal(t, 25, cfg.Gateway.Session.IngressFrames)
	assert.Equal(t, 75, cfg.Gateway.Session.EgressFrames)
	assert.Equal(t, 256, cfg.Gateway.Session.ControlEvents)
	assert.Equal(t, 64, cfg.Gateway.Session.WriterControlEvents)
	assert.Equal(t, 24_000, cfg.Gateway.Audio.SampleRate)
}

// TestRejectsNonWebSocketVoiceURL catches a validator regression that admits a
// provider endpoint the WebSocket client cannot use.
func TestRejectsNonWebSocketVoiceURL(t *testing.T) {
	_, err := loadFixture(t, "gateway:\n  voice:\n    url: https://voice.example/v1/realtime\n", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "gateway.voice.url")
}

// TestRejectsNonCanonicalProviderEndpoints catches a trust-boundary regression
// that lets credentials or un-hashed endpoint semantics hide in URL fields.
func TestRejectsNonCanonicalProviderEndpoints(t *testing.T) {
	tests := []struct {
		name string
		yaml string
	}{
		{"voice userinfo", "gateway:\n  voice:\n    url: ws://user@voice.example/v1/realtime\n"},
		{"voice query", "gateway:\n  voice:\n    url: ws://voice.example/v1/realtime?tenant=other\n"},
		{"voice fragment", "gateway:\n  voice:\n    url: ws://voice.example/v1/realtime#hidden\n"},
		{"voice opaque", "gateway:\n  voice:\n    url: ws:provider-opaque\n"},
		{"reasoner userinfo", "worker:\n  reasoner:\n    base_url: https://user@reasoner.example\n"},
		{"reasoner query", "worker:\n  reasoner:\n    base_url: https://reasoner.example?region=other\n"},
		{"reasoner fragment", "worker:\n  reasoner:\n    base_url: https://reasoner.example#hidden\n"},
		{"oidc query", "auth:\n  oidc_issuer: https://issuer.example?tenant=other\n"},
		{"otlp fragment", "telemetry:\n  otlp_endpoint: https://otlp.example#hidden\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := loadFixture(t, test.yaml, nil)
			require.Error(t, err)
		})
	}
}

// TestRejectsNonFrameAlignedAudioLimits catches a validator regression that allows
// queue byte limits to split a 16-bit mono PCM frame.
func TestRejectsNonFrameAlignedAudioLimits(t *testing.T) {
	_, err := loadFixture(t, "gateway:\n  audio:\n    sample_rate: 24000\n    channels: 1\n    bytes_per_sample: 2\n  session:\n    ingress_audio_bytes: 501\n", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ingress_audio_bytes")
}

// TestMissingSecretEnvironmentVariable catches a loader regression that starts a
// configured provider without resolving its named credential.
func TestMissingSecretEnvironmentVariable(t *testing.T) {
	_, err := loadFixture(t, "worker:\n  reasoner:\n    api_key_env: DEEPSEEK_API_KEY\n", func(string) (string, bool) { return "", false })
	require.Error(t, err)
	assert.Contains(t, err.Error(), "DEEPSEEK_API_KEY")
}

func TestLoadUnresolvedPreservesStructuralValidationWithoutSecretLookup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte("gateway:\n  voice:\n    api_key_env: PROVIDER_KEY\n"), 0o600))
	cfg, err := LoadUnresolved(path)
	require.NoError(t, err)
	assert.Equal(t, "PROVIDER_KEY", cfg.Gateway.Voice.APIKeyEnv)

	require.NoError(t, os.WriteFile(path, []byte("gateway:\n  voice:\n    api_key_env: invalid-name\n"), 0o600))
	_, err = LoadUnresolved(path)
	assert.ErrorContains(t, err, "environment variable name")

	require.NoError(t, os.WriteFile(path, []byte("gateway:\n  session:\n    control_events: -1\n"), 0o600))
	_, err = LoadUnresolved(path)
	assert.ErrorContains(t, err, "queue sizes")
}

// TestRejectsDatabasePasswordInURLQuery catches a secret scanner regression that
// accepts a PostgreSQL URL whose password is supplied as a query parameter.
func TestRejectsDatabasePasswordInURLQuery(t *testing.T) {
	_, err := loadFixture(t, "store:\n  url: postgres://db.example/live?password=secret&sslmode=require\n", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "plaintext secret")
}

// TestRejectsDatabasePasswordInLibPQDSN catches a secret scanner regression that
// accepts a libpq-style PostgreSQL password assignment in checked-in YAML.
func TestRejectsDatabasePasswordInLibPQDSN(t *testing.T) {
	_, err := loadFixture(t, "store:\n  url: host=db.example dbname=live password=secret sslmode=require\n", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "plaintext secret")
}

// TestLoadDefaultsPCMFrameBytes catches a loader regression that omits the fixed
// 20 ms frame size required by the 24 kHz mono PCM16LE transport contract.
func TestLoadDefaultsPCMFrameBytes(t *testing.T) {
	cfg, err := loadFixture(t, "gateway:\n  voice:\n    url: ws://127.0.0.1:8000/v1/realtime\n", nil)
	require.NoError(t, err)
	assert.Equal(t, 960, cfg.Gateway.Audio.FrameBytes)
}

// TestRejectsPCMFrameBytesOutsideTwentyMilliseconds catches a validator
// regression that allows a frame size other than 20 ms of PCM16LE samples.
func TestRejectsPCMFrameBytesOutsideTwentyMilliseconds(t *testing.T) {
	_, err := loadFixture(t, "gateway:\n  audio:\n    sample_rate: 24000\n    channels: 1\n    bytes_per_sample: 2\n    frame_bytes: 480\n", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be 960")
}

func loadFixture(t *testing.T, yaml string, lookup func(string) (string, bool)) (Config, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(strings.TrimSpace(yaml)+"\n"), 0o600))
	return Load(path, lookup)
}

func validConfig(t *testing.T) Config {
	t.Helper()
	key := base64.StdEncoding.EncodeToString(make([]byte, 32))
	cfg, err := loadFixture(t, `
gateway:
  reconnect: {hmac_key_env: RECONNECT_HMAC_KEY, hmac_key_id: reconnect-v1}
store:
  database_url_env: DATABASE_URL
  retention: {hmac_key_env: RETENTION_HMAC_KEY, hmac_key_id: retention-v1}
auth:
  static_bearer_env: LIVE_STATIC_BEARER_TOKEN
  static_tenant_id: local-tenant
  static_user_id: local-user
  static_scopes: [realtime]
features:
  handoffs: true
  tool_encryption_key_id: test-tools-v1
tools:
  web_search: {enabled: true}
`, func(name string) (string, bool) {
		switch name {
		case "RECONNECT_HMAC_KEY", "RETENTION_HMAC_KEY", "LIVE_TOOL_ENCRYPTION_KEY":
			return key, true
		case "DATABASE_URL":
			return "postgres://db.example/live?sslmode=disable", true
		case "LIVE_STATIC_BEARER_TOKEN":
			return "local-token", true
		default:
			return "", false
		}
	})
	require.NoError(t, err)
	return cfg
}
