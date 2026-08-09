package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const gatewayFatalLogHelperEnv = "LIVE_TEST_GATEWAY_FATAL_LOG_HELPER"

func TestGatewayFatalErrorIsStructuredRedactedAndExitsOne(t *testing.T) {
	const sentinel = "G4TES3C"
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	require.NoError(t, os.WriteFile(path, []byte("gateway:\n  audio:\n    sample_rate: "+sentinel+"\n"), 0o600))
	_, fixtureErr := config.LoadUnresolved(path)
	require.ErrorContains(t, fixtureErr, sentinel, "fixture must prove the underlying error contains the sensitive sentinel")

	if os.Getenv(gatewayFatalLogHelperEnv) == "1" {
		os.Args = []string{"live-gateway", "-config", path}
		main()
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestGatewayFatalErrorIsStructuredRedactedAndExitsOne$")
	command.Env = append(os.Environ(), gatewayFatalLogHelperEnv+"=1")
	output, err := command.CombinedOutput()
	require.Error(t, err, "fatal helper unexpectedly exited successfully: %s", output)
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	assert.Equal(t, 1, exitErr.ExitCode(), "fatal helper output: %s", output)
	assert.NotContains(t, string(output), sentinel)
	assert.Contains(t, string(output), "level=ERROR")
	assert.Contains(t, string(output), `msg="live gateway stopped"`)
	assert.Contains(t, string(output), "error=[REDACTED]")
}

func TestGatewayHandoffsRequireProductionDurabilityComposition(t *testing.T) {
	assert.NoError(t, validateComposition(config.Config{Mode: "production", Features: config.FeatureConfig{Handoffs: true}}))
	err := validateComposition(config.Config{Mode: "local", Features: config.FeatureConfig{Handoffs: true}})
	assert.ErrorContains(t, err, "production durability composition")
	assert.NoError(t, validateComposition(config.Config{}), "non-handoff local startup stays compatible")
}

func TestRunRejectsHandoffsBeforeEverySecretLookup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
features:
  handoffs: true
gateway:
  voice:
    api_key_env: PROVIDER_SECRET
`), 0o600))
	lookups := 0
	err := run(context.Background(), []string{"-config", path}, func(string) (string, bool) {
		lookups++
		return "secret", true
	})
	assert.ErrorContains(t, err, "production durability composition")
	assert.Zero(t, lookups)
}

func TestComposeAdmissionUsesConfiguredStaticDevelopmentIdentity(t *testing.T) {
	cfg := config.Config{Mode: "local", Auth: config.AuthConfig{StaticBearerEnv: "STATIC_TOKEN", StaticTenantID: "tenant", StaticUserID: "user", StaticScopes: []string{"realtime"}, AllowedOrigins: []string{"http://app.example"}}, Admission: config.AdmissionConfig{GlobalRate: 10, GlobalBurst: 10, IPRate: 10, IPBurst: 10, IPKeyTTL: time.Minute, MaxIPKeys: 10}}
	controller, closeAdmission, err := composeAdmission(t.Context(), cfg, func(name string) (string, bool) { return "development-token", name == "STATIC_TOKEN" })
	require.NoError(t, err)
	require.Nil(t, closeAdmission)
	request := httptest.NewRequest(http.MethodGet, "http://gateway.example/v1/realtime", nil)
	request.RemoteAddr = "192.0.2.1:1234"
	request.Header.Set("Origin", "http://app.example")
	request.Header.Set("Authorization", "Bearer development-token")
	subject, release, err := controller.Admit(t.Context(), request)
	require.NoError(t, err)
	defer release()
	assert.Equal(t, "tenant", subject.TenantID)
	assert.Equal(t, "user", subject.UserID)
	assert.True(t, subject.ExpiresAt.IsZero())
}

func TestFinishAdmissionReportsStoreCloseFailure(t *testing.T) {
	runFailure := errors.New("run failure")
	closeFailure := errors.New("close failure")
	err := finishAdmission(runFailure, func() error { return closeFailure })
	assert.ErrorIs(t, err, runFailure)
	assert.ErrorIs(t, err, closeFailure)
	assert.ErrorContains(t, err, "close admission storage")
	assert.NoError(t, finishAdmission(nil, func() error { return nil }))
}

func TestGatewayConfigCommandsDoNotLookupSecrets(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	require.NoError(t, os.WriteFile(path, []byte("mode: local\ngateway:\n  voice:\n    api_key_env: PROVIDER_SECRET\n"), 0600))
	old := commandOutput
	defer func() { commandOutput = old }()
	var output bytes.Buffer
	commandOutput = &output
	lookup := func(string) (string, bool) { t.Fatal("secret lookup"); return "", false }
	require.NoError(t, run(t.Context(), []string{"check-config", "--config", path}, lookup))
	require.NoError(t, run(t.Context(), []string{"config-hash", "--config", path}, lookup))
	assert.Regexp(t, `^v1:[0-9a-f]{64}\n$`, output.String())
}

func TestGatewayNormalStartupRejectsTrailingArgumentBeforeDownstreamConstruction(t *testing.T) {
	path := filepath.Join("..", "..", "configs", "gateway.example.yaml")
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
	lookup := func(string) (string, bool) {
		t.Fatal("environment lookup occurred after a positional argument")
		return "", false
	}
	err := run(t.Context(), []string{"-config", path, "unexpected"}, lookup)
	require.EqualError(t, err, "extra arguments are not accepted")
}

func TestGatewayExpectedHashMismatchIsContentFree(t *testing.T) {
	cfg, err := config.LoadUnresolved(filepath.Join("..", "..", "configs", "gateway.example.yaml"))
	require.NoError(t, err)
	err = verifyExpectedHash(cfg, func(name string) (string, bool) {
		assert.Equal(t, "LIVE_EXPECTED_CONFIG_HASH", name)
		return "v1:" + strings.Repeat("0", 64), true
	})
	require.EqualError(t, err, "config hash mismatch")
}
