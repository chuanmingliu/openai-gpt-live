package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/chuanmingliu/openai-gpt-live/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGatewayHandoffsFailClosedUntilAdmissionAndConfiguredSafetyComposition(t *testing.T) {
	err := validateComposition(config.Config{Features: config.FeatureConfig{Handoffs: true}})
	assert.ErrorContains(t, err, "handoff delivery is unavailable")
	assert.NotContains(t, err.Error(), "DATABASE_URL")

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
	assert.ErrorContains(t, err, "handoff delivery is unavailable")
	assert.Zero(t, lookups)
}
