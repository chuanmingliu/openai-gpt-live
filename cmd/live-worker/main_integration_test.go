//go:build integration

package main

import (
	"bytes"
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/config"
	"github.com/chuanmingliu/openai-gpt-live/internal/store/postgres"
	"github.com/chuanmingliu/openai-gpt-live/internal/testkit"
	"github.com/chuanmingliu/openai-gpt-live/migrations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildRuntimeComposesVerticalProfileRecipesAndKnowledgeCatalog(t *testing.T) {
	pool := testkit.Postgres(t)
	require.NoError(t, migrations.Up(t.Context(), pool))
	databaseURL := databaseURLForWorkerTest(t, pool.Config().ConnString(), pool.Config().ConnConfig.RuntimeParams["search_path"])
	cfg, err := config.LoadUnresolved(writeWorkerConfig(t))
	require.NoError(t, err)
	cfg.Store.URL = databaseURL
	cfg.Store.DatabaseURLEnv = ""
	cfg.Mode = "development_durable"
	cfg.Features.Handoffs = true
	cfg.Features.VerticalProfiles = config.VerticalProfileConfig{Enabled: true, RenderWorkers: 2, RenderQueue: 16}
	cfg.Auth.StaticBearerEnv = "TEST_STATIC_BEARER"
	cfg.Auth.StaticTenantID = "development"
	cfg.Auth.StaticUserID = "worker-test"
	cfg.Auth.StaticScopes = []string{"realtime", "live.admin"}
	cfg.Gateway.Reconnect.HMACKeyEnv = "TEST_RECONNECT_KEY"
	cfg.Gateway.Reconnect.HMACKeyID = "reconnect-v1"
	cfg.Tools.WebSearch.Enabled = true
	cfg.Tools.WebSearch.ForceStrict = true
	cfg.Tools.WebSearch.Regions = []string{"us-en"}
	cfg.Tools.WebSearch.MaxResults = 5
	cfg.Tools.WebSearch.Timeout = time.Second

	runtime, err := buildRuntime(t.Context(), cfg, resolvedSecrets{
		DatabaseURL: databaseURL, ProviderAPIKey: "provider", PrivacyHMACKey: bytes.Repeat([]byte{7}, 32),
		ToolEncryptionKey: bytes.Repeat([]byte{5}, 32), RetentionHMACKey: bytes.Repeat([]byte{9}, 32), DeploymentRegion: "test", AdminBearer: "static-token",
	})
	require.NoError(t, err)
	t.Cleanup(runtime.Close)
	production, ok := runtime.(*productionRuntime)
	require.True(t, ok)
	require.NotNil(t, production.runner.Recipes)
	registered, ok := production.runner.Tools.Lookup(configHashForTest(t, cfg), "knowledge_search")
	require.True(t, ok)
	assert.Equal(t, "knowledge.search", registered.AdapterID)
	assert.True(t, registered.ReadOnly)
}

func TestBuildRuntimeFailureAfterRepositoryZeroizesSourceAndOwnedRetentionKeys(t *testing.T) {
	pool := testkit.Postgres(t)
	require.NoError(t, migrations.Up(t.Context(), pool))
	databaseURL := databaseURLForWorkerTest(t, pool.Config().ConnString(), pool.Config().ConnConfig.RuntimeParams["search_path"])
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

func databaseURLForWorkerTest(t *testing.T, databaseURL, searchPath string) string {
	t.Helper()
	if searchPath == "" {
		return databaseURL
	}
	parsed, err := url.Parse(databaseURL)
	require.NoError(t, err)
	query := parsed.Query()
	query.Set("search_path", searchPath)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func configHashForTest(t *testing.T, cfg config.Config) string {
	t.Helper()
	hash, err := config.Hash(cfg)
	require.NoError(t, err)
	return hash
}
