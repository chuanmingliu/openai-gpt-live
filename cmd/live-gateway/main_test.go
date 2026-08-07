package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

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
