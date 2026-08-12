package main

import (
	"bytes"
	"context"
	"encoding/json"
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
	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/store/postgres"
	"github.com/chuanmingliu/openai-gpt-live/internal/toolpolicy"
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

func TestGatewayHandoffsRequireDurablePostgresComposition(t *testing.T) {
	assert.NoError(t, validateComposition(config.Config{Mode: "production", Features: config.FeatureConfig{Handoffs: true}}))
	err := validateComposition(config.Config{Mode: "local", Features: config.FeatureConfig{Handoffs: true}})
	assert.ErrorContains(t, err, "durable PostgreSQL composition")
	assert.NoError(t, validateComposition(config.Config{}), "non-handoff local startup stays compatible")
}

func TestDevelopmentDurableUsesStaticAuthAndPostgresDurability(t *testing.T) {
	cfg := config.Config{
		Mode:     "development_durable",
		Auth:     config.AuthConfig{StaticBearerEnv: "LIVE_STATIC_BEARER_TOKEN"},
		Features: config.FeatureConfig{Handoffs: true},
	}

	assert.NoError(t, validateComposition(cfg))
	assert.True(t, requiresRuntimeRepository(cfg))
	assert.True(t, allowsStaticAuthenticator(cfg))
	assert.False(t, requiresProductionTLS(cfg))
	assert.True(t, requiresReconnectReadiness(cfg))
}

func TestLocalStillRejectsHandoffs(t *testing.T) {
	cfg := config.Config{Mode: "local", Features: config.FeatureConfig{Handoffs: true}}
	require.Error(t, validateComposition(cfg))
}

func TestDevelopmentDurableGatewaySecretResolutionDoesNotRequireWorkerCredential(t *testing.T) {
	cfg := config.Config{
		Mode:    "development_durable",
		Gateway: config.GatewayConfig{Voice: config.VoiceConfig{APIKeyEnv: "S2S_API_KEY"}},
		Worker:  config.WorkerConfig{Reasoner: config.ReasonerConfig{APIKeyEnv: "DEEPSEEK_API_KEY"}},
		Store:   config.StoreConfig{DatabaseURLEnv: "DATABASE_URL"},
		Auth:    config.AuthConfig{StaticBearerEnv: "LIVE_STATIC_BEARER_TOKEN"},
	}
	values := map[string]string{
		"S2S_API_KEY":              "speech-secret",
		"DATABASE_URL":             "postgres://live@postgres/live?sslmode=disable",
		"LIVE_STATIC_BEARER_TOKEN": "local-bearer",
	}

	err := resolveGatewayConfigSecrets(cfg, func(name string) (string, bool) {
		if name == "DEEPSEEK_API_KEY" {
			t.Fatal("gateway looked up worker-only DeepSeek credential")
		}
		value, ok := values[name]
		return value, ok
	})
	require.NoError(t, err)
}

func TestDevelopmentDurableHandoffSnapshotUsesExactEnabledCatalog(t *testing.T) {
	cfg := config.Config{
		Worker: config.WorkerConfig{MaxOutputTokens: 4096, Reasoner: config.ReasonerConfig{Model: "deepseek-v4-flash"}},
		Safety: config.SafetyConfig{PolicyVersion: "static-v1"},
		Tools: config.ToolCatalogConfig{WebSearch: config.WebSearchToolConfig{
			Enabled: true, ForceStrict: true,
			Regions:    []string{"wt-wt", "us-en", "uk-en", "au-en", "ca-en", "cn-zh"},
			MaxResults: 5, Timeout: 8 * time.Second,
		}},
	}
	configHash := "v1:" + strings.Repeat("a", 64)

	snapshot, err := gatewayHandoffSnapshot(cfg, configHash)
	require.NoError(t, err)
	assert.Equal(t, []string{"web_search"}, snapshot.ToolAllowlist)
	assert.JSONEq(t, `{"web_search":{"enabled":true,"safe_search":"strict","region":"wt-wt","max_results":5,"timeout_ms":8000}}`, string(snapshot.ToolPolicy))
	assert.Equal(t, "deepseek-v4-flash", snapshot.Versions.Model)
	assert.Equal(t, configHash, snapshot.Versions.ToolRegistry)
	assert.Equal(t, configHash, snapshot.Versions.Config)
	_, err = handoff.ValidateCreateRequest(handoff.CreateRequest{
		Scope:     handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "conversation"},
		SessionID: "session", SourceTurn: identity.TurnKey{ID: "turn", Revision: 1},
		UserRequest: "search", Objective: "search", ContextRef: snapshot.ContextRef,
		ContextHash: snapshot.ContextHash, Deadline: snapshot.Deadline, TokenBudget: snapshot.TokenBudget,
		ToolAllowlist: snapshot.ToolAllowlist, ToolPolicy: snapshot.ToolPolicy,
		ApprovalPolicy: json.RawMessage(`{"mode":"none"}`), Versions: snapshot.Versions,
		PresentationHint: snapshot.PresentationHint, IdempotencyKey: "snapshot-validation",
	}, time.Now().UTC())
	require.NoError(t, err)

	cfg.Tools.WebSearch.Enabled = false
	disabled, err := gatewayHandoffSnapshot(cfg, configHash)
	require.NoError(t, err)
	assert.Empty(t, disabled.ToolAllowlist)
	assert.JSONEq(t, `{"web_search":{"enabled":false,"safe_search":"strict","region":"wt-wt","max_results":5,"timeout_ms":8000}}`, string(disabled.ToolPolicy))
	assert.Equal(t, toolpolicy.DevelopmentDurableCatalog().WebSearch.MaximumResults, cfg.ToolCatalog().WebSearch.MaximumResults)
}

func TestComposeGatewayDurabilityIncludesRecoveryWhenHandoffsAreDisabled(t *testing.T) {
	cfg := config.Config{Mode: "production"}
	cfg.Store.Retention.ConversationTTL = 24 * time.Hour
	cfg.Admission.LeaseDuration = time.Minute

	options, err := composeGatewayDurability(cfg, nil, "v1:"+strings.Repeat("a", 64), "gateway-replica-a", "us-east-1")
	assert.Nil(t, options)
	assert.ErrorContains(t, err, "conversation durability runtime")

	options, err = composeGatewayDurability(cfg, &gatewayFeatureRuntime{repo: &postgres.Repository{}}, "v1:"+strings.Repeat("a", 64), "gateway-replica-a", "us-east-1")
	require.NoError(t, err)
	require.Len(t, options, 1, "feature-off production still needs the common durable dependency option")
}

func TestGatewayDurabilityIdentityIsStableAndReplicaDistinct(t *testing.T) {
	first, err := gatewayDurabilityIdentity("gateway-replica-a")
	require.NoError(t, err)
	replay, err := gatewayDurabilityIdentity("gateway-replica-a")
	require.NoError(t, err)
	second, err := gatewayDurabilityIdentity("gateway-replica-b")
	require.NoError(t, err)
	assert.Equal(t, first, replay)
	assert.NotEqual(t, first, second)
	assert.Regexp(t, `^gateway-[0-9a-f]{32}$`, first)
	_, err = gatewayDurabilityIdentity("")
	assert.Error(t, err)
}

func TestGatewayDeliveryShardIsStableAcrossReplicaReplacementAndRegionIsolated(t *testing.T) {
	first, err := gatewayDeliveryShard("us-east-1")
	require.NoError(t, err)
	replacement, err := gatewayDeliveryShard("us-east-1")
	require.NoError(t, err)
	otherRegion, err := gatewayDeliveryShard("eu-west-1")
	require.NoError(t, err)

	firstOwner, err := gatewayDurabilityIdentity("live-gateway-old-pod")
	require.NoError(t, err)
	replacementOwner, err := gatewayDurabilityIdentity("live-gateway-replacement-pod")
	require.NoError(t, err)
	assert.Equal(t, first, replacement, "pod replacement must retain the delivery shard")
	assert.NotEqual(t, firstOwner, replacementOwner, "runtime owners must remain independently fenced")
	assert.NotEqual(t, first, otherRegion, "delivery shards in different regions must remain isolated")
	assert.Regexp(t, `^gateway-shard-[0-9a-f]{32}$`, first)
	_, err = gatewayDeliveryShard("")
	assert.Error(t, err)
}

func TestComposeGatewayReconnectFailsClosedWithoutProductionRuntime(t *testing.T) {
	option, err := composeGatewayReconnect(config.Config{Mode: "local"}, nil, nil)
	assert.NoError(t, err)
	assert.Nil(t, option)

	cfg := config.Config{Mode: "production"}
	cfg.Gateway.Reconnect.HMACKeyEnv = "RECONNECT_KEY"
	cfg.Gateway.Reconnect.HMACKeyID = "reconnect-v1"
	cfg.Gateway.Reconnect.CredentialTTL = 5 * time.Minute
	cfg.Admission.InstanceID = "gateway-a"
	option, err = composeGatewayReconnect(cfg, nil, func(string) (string, bool) { return "", false })
	assert.Nil(t, option)
	assert.ErrorContains(t, err, "runtime")
}

func TestGatewayMetricInstanceRequiresDedicatedProductionPodIdentity(t *testing.T) {
	lookup := func(name string) (string, bool) {
		assert.Equal(t, "LIVE_METRIC_INSTANCE", name)
		return "live-gateway-7d8b9c6f5d-x2k4m", true
	}
	instance, err := metricInstance("production", lookup)
	require.NoError(t, err)
	assert.Equal(t, "live-gateway-7d8b9c6f5d-x2k4m", instance)

	_, err = metricInstance("production", func(string) (string, bool) { return "", false })
	assert.EqualError(t, err, "production requires LIVE_METRIC_INSTANCE")

	instance, err = metricInstance("local", func(string) (string, bool) { return "", false })
	require.NoError(t, err)
	assert.Equal(t, "local", instance)
}

func TestGatewayMetricLabelsIgnoreAdmissionOwnershipIdentity(t *testing.T) {
	cfg := config.Config{
		Mode:      "production",
		Gateway:   config.GatewayConfig{Voice: config.VoiceConfig{Provider: "speech_to_speech"}},
		Admission: config.AdmissionConfig{InstanceID: "shared-admission-owner"},
	}
	values := map[string]string{
		"LIVE_METRIC_INSTANCE":   "live-gateway-7d8b9c6f5d-x2k4m",
		"LIVE_DEPLOYMENT_REGION": "us-east-1",
		"LIVE_ALLOWED_REGIONS":   "us-east-1",
	}
	labels, err := gatewayMetricLabels(cfg, "v1:"+strings.Repeat("1", 64), func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	})
	require.NoError(t, err)
	assert.Equal(t, "live-gateway-7d8b9c6f5d-x2k4m", labels.Instance)
	assert.NotEqual(t, cfg.Admission.InstanceID, labels.Instance)
}

func TestCanaryRuntimeIdentityComesFromImmutableProductionRuntime(t *testing.T) {
	configHash := "v1:" + strings.Repeat("a", 64)
	image := "ghcr.io/chuanmingliu/openai-gpt-live/gateway@sha256:" + strings.Repeat("b", 64)
	identity, err := canaryRuntimeIdentity("production", configHash, "us-east-1", func(name string) (string, bool) {
		assert.Equal(t, "LIVE_GATEWAY_IMAGE", name)
		return image, true
	})
	require.NoError(t, err)
	assert.Equal(t, configHash, identity.ConfigHash)
	assert.Equal(t, "us-east-1", identity.Region)
	assert.Equal(t, image, identity.GatewayImage)

	for name, value := range map[string]string{"missing": "", "mutable": "gateway:latest", "wrong target": "ghcr.io/chuanmingliu/openai-gpt-live/worker@sha256:" + strings.Repeat("b", 64)} {
		t.Run(name, func(t *testing.T) {
			_, err := canaryRuntimeIdentity("production", configHash, "us-east-1", func(string) (string, bool) { return value, value != "" })
			require.Error(t, err)
		})
	}
	identity, err = canaryRuntimeIdentity("local", configHash, "local", func(string) (string, bool) { return "", false })
	require.NoError(t, err)
	assert.Empty(t, identity)
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
	assert.ErrorContains(t, err, "durable PostgreSQL composition")
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
