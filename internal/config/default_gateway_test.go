package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultLocalGatewayIsLoopbackCredentialFreeAndNonDurable(t *testing.T) {
	cfg, err := DefaultLocalGateway()
	require.NoError(t, err)

	assert.Equal(t, "local", cfg.Mode)
	assert.Equal(t, "127.0.0.1:8080", cfg.Gateway.ListenAddr)
	assert.Equal(t, "local", cfg.Gateway.AdvertisedModel)
	assert.Equal(t, "speech_to_speech", cfg.Gateway.Voice.Provider)
	assert.Equal(t, "ws://127.0.0.1:8765/v1/realtime", cfg.Gateway.Voice.URL)
	assert.Equal(t, "local", cfg.Gateway.Voice.Model)

	for name, reference := range map[string]string{
		"voice":       cfg.Gateway.Voice.APIKeyEnv,
		"reasoner":    cfg.Worker.Reasoner.APIKeyEnv,
		"database":    cfg.Store.DatabaseURLEnv,
		"static auth": cfg.Auth.StaticBearerEnv,
		"privacy":     cfg.Safety.PrivacyHMACKeyEnv,
		"retention":   cfg.Store.Retention.HMACKeyEnv,
		"reconnect":   cfg.Gateway.Reconnect.HMACKeyEnv,
		"tool":        cfg.Features.ToolEncryptionKeyEnv,
	} {
		assert.Empty(t, reference, name)
	}
	assert.False(t, cfg.Features.Handoffs)
	assert.False(t, cfg.Features.SideEffectingTools)

	require.NoError(t, cfg.ResolveSecrets(func(name string) (string, bool) {
		t.Fatalf("default config attempted secret lookup %q", name)
		return "", false
	}))
}
