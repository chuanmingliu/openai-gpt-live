//go:build integration

package main

import (
	"bytes"
	"encoding/base64"
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

func TestComposeGatewayFailureAfterRepositoryZeroizesOwnedRetentionKey(t *testing.T) {
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
	cfg := config.Config{Mode: "production"}
	cfg.Store.URL = databaseURL
	cfg.Store.Retention.ApprovalTTL = 5 * time.Minute
	cfg.Store.Retention.HMACKeyEnv = "RETENTION_KEY"
	cfg.Store.Retention.HMACKeyID = "retention-key-v1"
	cfg.Shutdown.CleanupTimeout = time.Second
	cfg.Features.RefreshInterval = time.Second
	fault := errors.New("after gateway repository")
	var captured *postgres.Repository
	previous := composeGatewayRepositoryHook
	composeGatewayRepositoryHook = func(repo *postgres.Repository) error {
		captured = repo
		require.True(t, repo.RetentionConfigured())
		return fault
	}
	defer func() { composeGatewayRepositoryHook = previous }()
	lookup := func(name string) (string, bool) {
		return base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{8}, 32)), name == "RETENTION_KEY"
	}
	runtime, err := composeGatewayFeatures(t.Context(), cfg, lookup)
	assert.Nil(t, runtime)
	assert.ErrorIs(t, err, fault)
	require.NotNil(t, captured)
	assert.False(t, captured.RetentionConfigured(), "gateway error cleanup must close key material before pool close")
}
