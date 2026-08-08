//go:build integration

package main

import (
	"bytes"
	"errors"
	"net/url"
	"testing"

	"github.com/chuanmingliu/openai-gpt-live/internal/config"
	"github.com/chuanmingliu/openai-gpt-live/internal/store/postgres"
	"github.com/chuanmingliu/openai-gpt-live/internal/testkit"
	"github.com/chuanmingliu/openai-gpt-live/migrations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildRuntimeFailureAfterRepositoryZeroizesSourceAndOwnedRetentionKeys(t *testing.T) {
	pool := testkit.Postgres(t)
	require.NoError(t, migrations.Up(t.Context(), pool))
	databaseURL := pool.Config().ConnString()
	if searchPath := pool.Config().ConnConfig.RuntimeParams["search_path"]; searchPath != "" {
		parsed, err := url.Parse(databaseURL)
		require.NoError(t, err)
		query := parsed.Query()
		query.Set("search_path", searchPath)
		parsed.RawQuery = query.Encode()
		databaseURL = parsed.String()
	}
	cfg, err := config.LoadUnresolved(writeWorkerConfig(t))
	require.NoError(t, err)
	cfg.Store.URL = databaseURL
	cfg.Store.DatabaseURLEnv = ""
	source := bytes.Repeat([]byte{9}, 32)
	secrets := resolvedSecrets{
		DatabaseURL: databaseURL, ProviderAPIKey: "provider", PrivacyHMACKey: bytes.Repeat([]byte{7}, 32),
		ToolEncryptionKey: bytes.Repeat([]byte{5}, 32), RetentionHMACKey: source,
	}
	fault := errors.New("after repository")
	var captured *postgres.Repository
	previous := buildRuntimeRepositoryHook
	buildRuntimeRepositoryHook = func(repo *postgres.Repository) error {
		captured = repo
		require.True(t, repo.RetentionConfigured())
		return fault
	}
	defer func() { buildRuntimeRepositoryHook = previous }()
	runtime, err := buildRuntime(t.Context(), cfg, secrets)
	assert.Nil(t, runtime)
	assert.ErrorIs(t, err, fault)
	require.NotNil(t, captured)
	assert.False(t, captured.RetentionConfigured(), "error cleanup must close the repository-owned key before pool close")
	for _, value := range source {
		assert.Zero(t, value, "builder source key must be cleared on error")
	}
}
