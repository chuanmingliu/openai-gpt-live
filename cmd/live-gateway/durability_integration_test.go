//go:build integration

package main

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/config"
	"github.com/chuanmingliu/openai-gpt-live/internal/gateway"
	"github.com/chuanmingliu/openai-gpt-live/internal/session"
	"github.com/chuanmingliu/openai-gpt-live/internal/store/postgres"
	"github.com/chuanmingliu/openai-gpt-live/internal/testkit"
	transportws "github.com/chuanmingliu/openai-gpt-live/internal/transport/ws"
	"github.com/chuanmingliu/openai-gpt-live/migrations"
	"github.com/stretchr/testify/require"
)

func TestProductionDurabilityCompositionStartsAndClosesExistingComponents(t *testing.T) {
	pool := testkit.Postgres(t)
	require.NoError(t, migrations.Up(t.Context(), pool))
	repository, err := postgres.NewRepository(pool, clock.Real{})
	require.NoError(t, err)
	t.Cleanup(repository.CloseSensitive)

	path := filepath.Join(t.TempDir(), "gateway.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
mode: production
gateway:
  listen_addr: 127.0.0.1:0
  behind_ingress: true
  trusted_proxies: [127.0.0.1/32]
  voice: {url: wss://voice.invalid/v1/realtime, model: voice-v1}
store:
  database_url_env: DATABASE_URL
  retention: {hmac_key_env: LIVE_RETENTION_HMAC_KEY, hmac_key_id: retention-v1}
auth:
  oidc_issuer: https://oidc.invalid
  oidc_audience: live
  oidc_required_scopes: [realtime]
  allowed_origins: [https://app.invalid]
admission: {instance_id: gateway-integration}
safety: {policy_version: static-v1, mode: enforce, privacy_hmac_key_env: LIVE_PRIVACY_HMAC_KEY}
worker:
  reasoner: {base_url: https://reasoner.invalid, model: reasoner-v1}
features: {handoffs: true, runtime_refresh_interval: 1s}
`), 0o600))
	cfg, err := config.LoadUnresolved(path)
	require.NoError(t, err)
	hash, err := config.Hash(cfg)
	require.NoError(t, err)
	options, err := composeGatewayDurability(cfg, &gatewayFeatureRuntime{pool: pool, repo: repository}, hash)
	require.NoError(t, err)
	options = append(options, gateway.WithAdmission(transportws.AdmissionFunc(func(context.Context, *http.Request) (session.Subject, func(), error) {
		return session.Subject{TenantID: "tenant", UserID: "user", Scopes: map[string]struct{}{"realtime": {}}}, func() {}, nil
	})))
	app, err := gateway.New(cfg, testkit.NewFakeConnector(4), options...)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- app.Run(ctx) }()
	select {
	case err := <-done:
		t.Fatalf("production durability components stopped before cancellation: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("production durability components did not close within the lifecycle bound")
	}
}
