package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseConfigUsesCredentialFreeLoopbackDefaults(t *testing.T) {
	cfg, err := parseConfig(nil)
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:3000", cfg.ListenAddr)
	assert.Equal(t, "ws://127.0.0.1:8080/v1/realtime", cfg.GatewayURL)
	assert.Equal(t, "local", cfg.Model)
	assert.Equal(t, "http://127.0.0.1:3000", cfg.Origin)
	assert.Empty(t, cfg.BearerEnv)
	assert.EqualValues(t, 1<<20, cfg.MaxFrameBytes)
}

func TestResolveBearerDoesNotLookupByDefault(t *testing.T) {
	cfg, err := parseConfig(nil)
	require.NoError(t, err)
	bearer, err := resolveBearer(cfg, func(name string) (string, bool) {
		t.Fatalf("unexpected credential lookup %q", name)
		return "", false
	})
	require.NoError(t, err)
	assert.Empty(t, bearer)
}

func TestResolveBearerRequiresExplicitReference(t *testing.T) {
	cfg, err := parseConfig([]string{"-bearer-env", "LIVE_DEMO_BEARER"})
	require.NoError(t, err)
	_, err = resolveBearer(cfg, func(name string) (string, bool) {
		assert.Equal(t, "LIVE_DEMO_BEARER", name)
		return "", false
	})
	assert.EqualError(t, err, "configured demo bearer is unavailable")
}

func TestParseConfigRejectsUnsafeNetworkInputs(t *testing.T) {
	for _, args := range [][]string{
		{"-listen", "0.0.0.0:3000"},
		{"-origin", "https://example.com"},
		{"-gateway-url", "ws://user:pass@127.0.0.1:8080/v1/realtime"},
		{"-gateway-url", "ws://127.0.0.1:8080/v1/realtime?token=x"},
		{"-gateway-url", "ws://127.0.0.1:8080/other"},
		{"-model", "bad model\n"},
		{"-max-frame-bytes", "0"},
	} {
		_, err := parseConfig(args)
		require.Error(t, err, "args=%q", args)
	}
}

func TestUpstreamURLsAreDerivedWithoutMutatingBase(t *testing.T) {
	cfg, err := parseConfig(nil)
	require.NoError(t, err)
	wsURL, err := cfg.upstreamWebSocketURL()
	require.NoError(t, err)
	assert.Equal(t, "ws://127.0.0.1:8080/v1/realtime?model=local", wsURL.String())
	capabilities, err := cfg.capabilitiesURL()
	require.NoError(t, err)
	assert.Equal(t, "http://127.0.0.1:8080/v1/realtime/capabilities", capabilities.String())
}

func TestResolveBearerBoundsAndDoesNotExposeSecret(t *testing.T) {
	cfg, err := parseConfig([]string{"-bearer-env", "LIVE_DEMO_BEARER"})
	require.NoError(t, err)
	_, err = resolveBearer(cfg, func(string) (string, bool) { return "", false })
	require.Error(t, err)
	_, err = resolveBearer(cfg, func(string) (string, bool) { return string(make([]byte, 8<<10+1)), true })
	require.EqualError(t, err, "configured demo bearer is unavailable")
	secret := "sentinel-demo-secret"
	value, err := resolveBearer(cfg, func(string) (string, bool) { return secret, true })
	require.NoError(t, err)
	assert.Equal(t, []byte(secret), value)
	assert.NotContains(t, fmt.Sprint(err), secret)
}
