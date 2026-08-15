package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseConfigAcceptsCanonicalLoopbackDefaults(t *testing.T) {
	cfg, err := parseConfig(nil)
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:3000", cfg.ListenAddr)
	assert.Equal(t, "ws://127.0.0.1:18080/v1/realtime", cfg.GatewayURL)
	assert.Equal(t, "gpt-live-simulator", cfg.Model)
	assert.Equal(t, "http://127.0.0.1:3000", cfg.Origin)
	assert.Equal(t, "LIVE_DEMO_BEARER", cfg.BearerEnv)
	assert.EqualValues(t, 64<<10, cfg.MaxFrameBytes)
}

func TestParseConfigRejectsUnsafeNetworkAndURLInputs(t *testing.T) {
	for _, args := range [][]string{
		{"-listen", "0.0.0.0:3000"},
		{"-origin", "https://example.com"},
		{"-gateway-url", "ws://user:pass@127.0.0.1:18080/v1/realtime"},
		{"-gateway-url", "ws://127.0.0.1:18080/v1/realtime?token=x"},
		{"-model", "bad model\n"},
	} {
		_, err := parseConfig(args)
		require.Error(t, err, fmt.Sprint(args))
	}
}

func TestResolveBearerRequiresNonemptyBoundedValueWithoutEchoingIt(t *testing.T) {
	cfg, err := parseConfig(nil)
	require.NoError(t, err)
	_, err = resolveBearer(cfg, func(string) (string, bool) { return "", false })
	assert.EqualError(t, err, "configured demo bearer is unavailable")
	secret := "sentinel-demo-secret"
	value, err := resolveBearer(cfg, func(string) (string, bool) { return secret, true })
	require.NoError(t, err)
	assert.Equal(t, []byte(secret), value)
	assert.NotContains(t, fmt.Sprint(err), secret)
}
