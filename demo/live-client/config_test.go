package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseConfigAcceptsCanonicalLoopbackDefaults(t *testing.T) {
	cfg, err := parseConfig(nil)
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:3000", cfg.ListenAddr)
	assert.Equal(t, "ws://127.0.0.1:18080/v1/realtime", cfg.GatewayURL)
	assert.Equal(t, "gpt-live-real", cfg.Model)
	assert.Equal(t, "http://127.0.0.1:3000", cfg.Origin)
	assert.Equal(t, "LIVE_DEMO_BEARER", cfg.BearerEnv)
	assert.EqualValues(t, 64<<10, cfg.MaxFrameBytes)

	upstream, err := cfg.upstreamURL()
	require.NoError(t, err)
	assert.Equal(t, "ws://127.0.0.1:18080/v1/realtime?model=gpt-live-real", upstream.String())
}

func TestParseConfigRejectsUnsafeNetworkAndURLInputs(t *testing.T) {
	tests := [][]string{
		{"-listen", "0.0.0.0:3000"},
		{"-listen", "localhost:3000"},
		{"-origin", "https://example.com"},
		{"-origin", "http://127.0.0.1:3001"},
		{"-gateway-url", "ws://user:pass@127.0.0.1:18080/v1/realtime"},
		{"-gateway-url", "ws://127.0.0.1:18080/v1/realtime?token=x"},
		{"-gateway-url", "ws://127.0.0.1:18080/wrong"},
		{"-model", "bad model\n"},
		{"-bearer-env", "bad-name"},
	}
	for _, args := range tests {
		args := args
		t.Run(fmt.Sprint(args), func(t *testing.T) {
			_, err := parseConfig(args)
			require.Error(t, err)
		})
	}
}

func TestParseConfigRequiresLiteralLoopbackGateway(t *testing.T) {
	for _, endpoint := range []string{
		"ws://example.com/v1/realtime",
		"wss://example.com/v1/realtime",
		"ws://localhost:18080/v1/realtime",
		"ws://0.0.0.0:18080/v1/realtime",
		"ws://192.0.2.1:18080/v1/realtime",
	} {
		_, err := parseConfig([]string{"-gateway-url", endpoint})
		require.Error(t, err, endpoint)
	}
	for _, endpoint := range []string{
		"ws://127.0.0.1:18080/v1/realtime",
		"ws://[::1]:18080/v1/realtime",
	} {
		_, err := parseConfig([]string{"-gateway-url", endpoint})
		require.NoError(t, err, endpoint)
	}
}

func TestResolveBearerRequiresNonemptyBoundedValueWithoutEchoingIt(t *testing.T) {
	cfg, err := parseConfig(nil)
	require.NoError(t, err)

	_, err = resolveBearer(cfg, func(string) (string, bool) { return "", false })
	assert.EqualError(t, err, "configured demo bearer is unavailable")

	secret := "sentinel-demo-secret"
	value, err := resolveBearer(cfg, func(name string) (string, bool) {
		assert.Equal(t, "LIVE_DEMO_BEARER", name)
		return secret, true
	})
	require.NoError(t, err)
	assert.Equal(t, []byte(secret), value)
	assert.NotContains(t, fmt.Sprint(err), secret)

	_, err = resolveBearer(cfg, func(string) (string, bool) { return "contains whitespace", true })
	assert.EqualError(t, err, "configured demo bearer is invalid")
	_, err = resolveBearer(cfg, func(string) (string, bool) { return string(make([]byte, (16<<10)+1)), true })
	assert.EqualError(t, err, "configured demo bearer is invalid")
}

func TestParseConfigRequiresCompleteSafeOperatorStartupConfiguration(t *testing.T) {
	_, err := parseConfig([]string{"-operator"})
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "operator")
}

func TestParseConfigAcceptsSeparateFixedSpeechTrustRoot(t *testing.T) {
	repository, command, _ := newRunnerRepository(t)
	speechRoot := t.TempDir()
	profile := filepath.Join(speechRoot, "provider-profile.json")
	require.NoError(t, os.WriteFile(profile, []byte("{}\n"), 0o600))
	cfg, err := parseConfig([]string{
		"-operator",
		"-operator-state-dir", filepath.Join(t.TempDir(), "state"),
		"-operator-repository-root", repository,
		"-operator-speech-root", speechRoot,
		"-operator-speech-command", command,
		"-operator-speech-profile", profile,
		"-operator-speech-ready-url", "http://127.0.0.1:8766/v1/pool",
	})
	require.NoError(t, err)
	assert.Equal(t, speechRoot, cfg.Operator.SpeechRoot)
	assert.Equal(t, "http://127.0.0.1:8766/v1/pool", cfg.Operator.Readiness[componentSpeech])
}

func TestVerticalProfileConfigurationIsPrivateAndValidated(t *testing.T) {
	cfg, err := parseConfig(nil)
	require.NoError(t, err)
	assert.False(t, cfg.Profiles.Enabled)

	cfg, err = parseConfig([]string{
		"-vertical-profiles",
		"-profile-tenant", "local",
		"-profile-actor", "operator",
		"-profile-database-url-env", "LIVE_PROFILE_DATABASE_URL",
		"-profile-adapter-registry-version", "registry-v1",
	})
	require.NoError(t, err)
	assert.True(t, cfg.Profiles.Enabled)
	assert.Equal(t, "local", cfg.Profiles.TenantID)
	assert.Equal(t, "operator", cfg.Profiles.Actor)
	assert.Equal(t, "LIVE_PROFILE_DATABASE_URL", cfg.Profiles.DatabaseURLEnv)
	assert.Equal(t, "registry-v1", cfg.Profiles.AdapterRegistryVersion)

	for name, args := range map[string][]string{
		"missing identity": {"-vertical-profiles"},
		"missing actor":    {"-vertical-profiles", "-profile-tenant", "local"},
		"spaced tenant":    {"-vertical-profiles", "-profile-tenant", "local tenant", "-profile-actor", "operator"},
		"database value":   {"-vertical-profiles", "-profile-tenant", "local", "-profile-actor", "operator", "-profile-database-url-env", "postgres://user:pass@localhost/db"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parseConfig(args)
			assert.Error(t, err)
		})
	}
}

func TestProfileConfigurationNeverCarriesDatabaseValues(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-vertical-profiles", "-profile-tenant", "local", "-profile-actor", "operator",
		"-profile-adapter-registry-version", "registry-v1",
	})
	require.NoError(t, err)
	rendered := fmt.Sprintf("%+v", cfg)
	assert.NotContains(t, rendered, "postgres://")
	assert.NotContains(t, rendered, "password")
}
