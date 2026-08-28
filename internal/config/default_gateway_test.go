package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDefaultLocalGatewayIsLoopbackCredentialFreeAndNonDurable(t *testing.T) {
	cfg, err := DefaultLocalGateway()
	require.NoError(t, err)

	require.Equal(t, "local", cfg.Mode)
	require.Equal(t, "127.0.0.1:8080", cfg.Gateway.ListenAddr)
	require.Equal(t, "local", cfg.Gateway.AdvertisedModel)
	require.Equal(t, VoiceConfig{
		Role:     "realtime_voice",
		Provider: "speech_to_speech",
		URL:      "ws://127.0.0.1:8765/v1/realtime",
		Model:    "local",
	}, cfg.Gateway.Voice)

	require.False(t, cfg.IsDurable())
	require.False(t, cfg.Features.Handoffs)
	require.False(t, cfg.Features.SideEffectingTools)
	require.False(t, cfg.Tools.WebSearch.Enabled)
	require.Empty(t, cfg.Store.URL)
	require.Empty(t, cfg.Store.DatabaseURLEnv)
	require.Empty(t, cfg.Store.Retention.HMACKeyEnv)
	require.Empty(t, cfg.Store.Retention.HMACKeyID)
	require.Empty(t, cfg.Gateway.Reconnect.HMACKeyEnv)
	require.Empty(t, cfg.Gateway.Reconnect.HMACKeyID)
	require.Empty(t, cfg.Gateway.Voice.APIKeyEnv)
	require.Empty(t, cfg.Worker.Reasoner.APIKeyEnv)
	require.Empty(t, cfg.Auth.StaticBearerEnv)
	require.Empty(t, cfg.Safety.PrivacyHMACKeyEnv)
	require.Empty(t, cfg.Auth.OIDCIssuer)
	require.Empty(t, cfg.Features.ToolEncryptionKeyEnv)
	require.Empty(t, cfg.Features.ToolEncryptionKeyID)
	require.Empty(t, cfg.Telemetry.OTLPEndpoint)
	require.NoError(t, cfg.Validate())
}

func TestDefaultLocalGatewayReturnsOwnedSlices(t *testing.T) {
	cfg, err := DefaultLocalGateway()
	require.NoError(t, err)
	require.NotEmpty(t, cfg.Tools.WebSearch.Regions)

	cfg.Tools.WebSearch.Regions[0] = "mutated"
	cfg.Tools.WebSearch.Regions = append(cfg.Tools.WebSearch.Regions, "extra")

	later, err := DefaultLocalGateway()
	require.NoError(t, err)
	require.NotEqual(t, "mutated", later.Tools.WebSearch.Regions[0])
	require.NotContains(t, later.Tools.WebSearch.Regions, "extra")
}
