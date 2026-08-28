package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/config"
	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/chuanmingliu/openai-gpt-live/internal/session"
	"github.com/chuanmingliu/openai-gpt-live/internal/store/postgres"
	"github.com/chuanmingliu/openai-gpt-live/internal/toolpolicy"
	"github.com/coder/websocket"
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

	lookup := func(name string) (string, bool) {
		if name == "DEEPSEEK_API_KEY" {
			t.Fatal("gateway looked up worker-only DeepSeek credential")
		}
		value, ok := values[name]
		return value, ok
	}
	_, err := resolveGatewayProviderCredential(cfg.Gateway.Voice.APIKeyEnv, lookup)
	require.NoError(t, err)
	_, err = resolveGatewayDatabaseURL(cfg, lookup)
	require.NoError(t, err)
	_, err = resolveGatewayStaticBearer(cfg.Auth.StaticBearerEnv, lookup)
	require.NoError(t, err)
}

func TestLoadGatewayConfigUsesCredentialFreeDefaultWhenConfigFlagAbsent(t *testing.T) {
	cfg, err := loadGatewayConfig(gatewayConfigSource{})
	require.NoError(t, err)
	assert.Equal(t, "local", cfg.Mode)
	assert.Empty(t, cfg.Gateway.Voice.APIKeyEnv)
}

func TestGatewayExplicitEmptyConfigNeverDowngradesToLocal(t *testing.T) {
	_, err := loadGatewayConfig(gatewayConfigSource{Explicit: true})
	require.EqualError(t, err, "explicit configuration path is required")
}

func TestLoadGatewayConfigRequiresProviderCredentialAndAuthentication(t *testing.T) {
	for _, test := range []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "provider credential",
			yaml: `mode: local
gateway:
  listen_addr: 127.0.0.1:8080
  voice:
    url: ws://127.0.0.1:8765/v1/realtime
auth:
  static_bearer_env: STATIC_TOKEN
  static_tenant_id: tenant
  static_user_id: user
  static_scopes: [realtime]
`,
			want: "explicit gateway configuration requires a provider credential reference",
		},
		{
			name: "loopback authentication",
			yaml: `mode: local
gateway:
  listen_addr: 127.0.0.1:8080
  voice:
    url: ws://127.0.0.1:8765/v1/realtime
    api_key_env: PROVIDER_TOKEN
`,
			want: "explicit gateway configuration requires authentication",
		},
		{
			name: "non-loopback authentication",
			yaml: `mode: local
gateway:
  listen_addr: :8080
  voice:
    url: ws://127.0.0.1:8765/v1/realtime
    api_key_env: PROVIDER_TOKEN
`,
			want: "explicit gateway configuration requires authentication",
		},
		{
			name: "complete OIDC authentication",
			yaml: `mode: local
gateway:
  listen_addr: :8080
  voice:
    url: ws://127.0.0.1:8765/v1/realtime
    api_key_env: PROVIDER_TOKEN
auth:
  oidc_issuer: http://127.0.0.1:9099
`,
			want: "explicit gateway OIDC authentication is incomplete",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "gateway.yaml")
			require.NoError(t, os.WriteFile(path, []byte(test.yaml), 0o600))

			_, err := loadGatewayConfig(gatewayConfigSource{Path: path, Explicit: true})
			require.EqualError(t, err, test.want)
		})
	}
}

func TestGatewayRuntimeRejectsExplicitCredentialFreeConfig(t *testing.T) {
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	listenAddress := probe.Addr().String()
	require.NoError(t, probe.Close())

	path := filepath.Join(t.TempDir(), "gateway.yaml")
	require.NoError(t, os.WriteFile(path, []byte(fmt.Sprintf(`mode: local
gateway:
  advertised_model: local
  listen_addr: %s
  voice:
    role: realtime_voice
    provider: speech_to_speech
    url: ws://127.0.0.1:8765/v1/realtime
    model: local
`, listenAddress)), 0o600))

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	err = run(ctx, []string{"-config", path}, func(string) (string, bool) { return "", false })
	require.EqualError(t, err, "load gateway configuration: explicit gateway configuration requires a provider credential reference")
}

func TestGatewayStartupDoesNotResolveInactiveWorkerSecrets(t *testing.T) {
	forbidden := map[string]bool{
		"DEEPSEEK_API_KEY": true,
		"PRIVACY_HMAC":     true,
		"TOOL_ENCRYPTION":  true,
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := run(ctx, nil, func(name string) (string, bool) {
		if forbidden[name] {
			t.Fatalf("gateway resolved inactive worker secret %q", name)
		}
		return "", false
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestGatewayConfiguredVoiceCredentialRemainsStrict(t *testing.T) {
	_, err := resolveGatewayProviderCredential("SPEECH_KEY", func(string) (string, bool) { return "", false })
	require.EqualError(t, err, "configured provider credential is unavailable")
}

func TestGatewayConfiguredStaticBearerRemainsStrict(t *testing.T) {
	cfg := config.Config{Mode: "local", Auth: config.AuthConfig{StaticBearerEnv: "STATIC_TOKEN"}}
	_, err := resolveGatewayRuntimeSecrets(cfg, func(string) (string, bool) { return "", false })
	require.EqualError(t, err, "configured static credential is unavailable")
}

func TestGatewayConfiguredDatabaseCredentialRemainsStrict(t *testing.T) {
	_, err := resolveGatewayDatabaseURL(config.Config{Store: config.StoreConfig{DatabaseURLEnv: "DATABASE_URL"}}, func(string) (string, bool) { return "", false })
	require.EqualError(t, err, "configured database credential is unavailable")
}

func TestGatewayProductionDatabaseURLRejectsEnvironmentTLSDisableBeforeDial(t *testing.T) {
	for _, test := range []struct {
		name  string
		value string
		want  string
	}{
		{name: "literal", value: "postgres://gateway@postgres/live?sslmode=disable", want: "production database URL must not disable SSL"},
		{name: "percent encoded", value: "postgres://gateway@postgres/live?sslmode=dis%61ble", want: "production database URL must not disable SSL"},
		{name: "duplicate effective first", value: "postgres://gateway@postgres/live?sslmode=disable&sslmode=require", want: "production database URL must not disable SSL"},
		{name: "case variant", value: "postgres://gateway@postgres/live?sslmode=DISABLE", want: "production database URL is invalid"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := resolveGatewayDatabaseURL(config.Config{Mode: "production", Store: config.StoreConfig{DatabaseURLEnv: "DATABASE_URL"}}, func(string) (string, bool) {
				return test.value, true
			})
			assert.EqualError(t, err, test.want)
			assert.NotContains(t, err.Error(), test.value)
		})
	}
}

func TestGatewayTextualCredentialsAreBounded(t *testing.T) {
	values := []struct {
		name  string
		value string
		ok    bool
	}{
		{name: "missing"},
		{name: "empty", ok: true},
		{name: "whitespace", value: " adversarial", ok: true},
		{name: "invalid UTF-8", value: string([]byte{0xff}), ok: true},
		{name: "control", value: "adversarial\n", ok: true},
		{name: "too large", value: strings.Repeat("z", gatewaySecretMaximumBytes+1), ok: true},
	}
	credentials := []struct {
		name      string
		wantError string
		resolve   func(func(string) (string, bool)) error
	}{
		{name: "speech API key", wantError: "configured provider credential is unavailable", resolve: func(lookup func(string) (string, bool)) error {
			_, err := resolveGatewayProviderCredential("SPEECH_KEY", lookup)
			return err
		}},
		{name: "static bearer", wantError: "configured static credential is unavailable", resolve: func(lookup func(string) (string, bool)) error {
			_, err := resolveGatewayStaticBearer("STATIC_BEARER", lookup)
			return err
		}},
		{name: "database URL", wantError: "configured database credential is unavailable", resolve: func(lookup func(string) (string, bool)) error {
			_, err := resolveGatewayDatabaseURL(config.Config{Store: config.StoreConfig{DatabaseURLEnv: "DATABASE_URL"}}, lookup)
			return err
		}},
	}
	for _, credential := range credentials {
		for _, value := range values {
			t.Run(credential.name+"/"+value.name, func(t *testing.T) {
				err := credential.resolve(func(string) (string, bool) { return value.value, value.ok })
				require.EqualError(t, err, credential.wantError)
				if value.value != "" {
					assert.NotContains(t, err.Error(), value.value)
				}
			})
		}
	}
}

func TestGatewayConfigFlagFailuresAreContentFree(t *testing.T) {
	const sentinel = "path-MUST-NOT-LEAK"
	tests := []struct {
		name string
		args []string
	}{
		{name: "empty", args: []string{"-config="}},
		{name: "invalid UTF-8", args: []string{"-config=" + string([]byte{0xff})}},
		{name: "control", args: []string{"-config=" + sentinel + "\n"}},
		{name: "oversized", args: []string{"-config=" + strings.Repeat("x", 4097)}},
		{name: "duplicate", args: []string{"-config=" + sentinel, "--config=" + sentinel + "-again"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := run(t.Context(), test.args, func(string) (string, bool) {
				t.Fatal("configuration flag failure performed environment lookup")
				return "", false
			})
			require.EqualError(t, err, "gateway configuration flags are invalid")
			for _, arg := range test.args {
				assert.NotContains(t, err.Error(), arg)
			}
		})
	}
}

func TestGatewayEncodedKeysAreBoundedBeforeDecode(t *testing.T) {
	tooLarge := strings.Repeat("A", gatewayHMACEncodedMaximumSize+1)
	_, err := resolveGatewayRetentionKey("RETENTION_KEY", func(string) (string, bool) { return tooLarge, true })
	require.EqualError(t, err, "retention HMAC key is unavailable")
	_, err = resolveGatewayReconnectKey("RECONNECT_KEY", func(string) (string, bool) { return tooLarge, true })
	require.EqualError(t, err, "reconnect HMAC key is unavailable")
}

func TestGatewayResolvesActiveSecretsBeforeOIDCActivity(t *testing.T) {
	var oidcRequests atomic.Int32
	oidc := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		oidcRequests.Add(1)
		http.Error(response, "unavailable", http.StatusServiceUnavailable)
	}))
	t.Cleanup(oidc.Close)

	listenAddress := reserveGatewayTestAddress(t)
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	require.NoError(t, os.WriteFile(path, []byte(fmt.Sprintf(`mode: local
gateway:
  advertised_model: local
  listen_addr: %s
  voice:
    role: realtime_voice
    provider: speech_to_speech
    url: ws://127.0.0.1:8765/v1/realtime
    model: local
    api_key_env: PROVIDER_TOKEN
auth:
  oidc_issuer: %s
  oidc_audience: live
  oidc_required_scopes: [realtime]
`, listenAddress, oidc.URL)), 0o600))

	err := run(t.Context(), []string{"-config", path}, func(name string) (string, bool) {
		if name == "PROVIDER_TOKEN" {
			return "", false
		}
		return "", false
	})
	assert.EqualError(t, err, "configured provider credential is unavailable")
	assert.Zero(t, oidcRequests.Load(), "invalid provider credential contacted OIDC")
	assertGatewayTestAddressRemainsFree(t, listenAddress)
}

func TestGatewayResolvesAllActiveSecretsBeforeDatabaseOrListenerActivity(t *testing.T) {
	validHMAC := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))
	for _, test := range []struct {
		name       string
		secretName string
		invalid    string
		present    bool
		want       string
	}{
		{name: "provider", secretName: "PROVIDER_TOKEN", present: false, want: "configured provider credential is unavailable"},
		{name: "static bearer", secretName: "STATIC_TOKEN", invalid: "bad\nvalue", present: true, want: "configured static credential is unavailable"},
		{name: "database", secretName: "DATABASE_URL", invalid: "bad\nvalue", present: true, want: "configured database credential is unavailable"},
		{name: "retention", secretName: "RETENTION_KEY", invalid: "not-base64", present: true, want: "retention HMAC key is invalid"},
		{name: "reconnect", secretName: "RECONNECT_KEY", invalid: "not-base64", present: true, want: "reconnect HMAC key is invalid"},
	} {
		t.Run(test.name, func(t *testing.T) {
			databaseListener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			t.Cleanup(func() { _ = databaseListener.Close() })
			var databaseConnections atomic.Int32
			go func() {
				for {
					connection, err := databaseListener.Accept()
					if err != nil {
						return
					}
					databaseConnections.Add(1)
					_ = connection.Close()
				}
			}()

			listenAddress := reserveGatewayTestAddress(t)
			path := writeDurableGatewayTestConfig(t, listenAddress)
			values := map[string]string{
				"PROVIDER_TOKEN": "provider-token",
				"STATIC_TOKEN":   "static-token",
				"DATABASE_URL": fmt.Sprintf(
					"postgres://live@%s/live?sslmode=disable&connect_timeout=1",
					databaseListener.Addr().String(),
				),
				"RETENTION_KEY": validHMAC,
				"RECONNECT_KEY": validHMAC,
			}
			if test.present {
				values[test.secretName] = test.invalid
			} else {
				delete(values, test.secretName)
			}

			err = run(t.Context(), []string{"-config", path}, func(name string) (string, bool) {
				value, ok := values[name]
				return value, ok
			})
			assert.EqualError(t, err, test.want)
			assert.Zero(t, databaseConnections.Load(), "invalid active secret contacted PostgreSQL")
			assertGatewayTestAddressRemainsFree(t, listenAddress)
		})
	}
}

func TestGatewaySuccessfulConsumerConstructionReleasesStagingSecrets(t *testing.T) {
	staticStaging := []byte("development-token")
	retentionStaging := bytes.Repeat([]byte{7}, 32)
	reconnectStaging := bytes.Repeat([]byte{9}, 32)
	resolved := gatewayRuntimeSecrets{
		providerCredential: "provider-token",
		staticBearer:       staticStaging,
		databaseURL:        "postgres://live@127.0.0.1:5432/live?sslmode=disable",
		retentionKey:       retentionStaging,
		reconnectKey:       reconnectStaging,
	}
	cfg := config.Config{
		Mode: "local",
		Gateway: config.GatewayConfig{
			Voice:     config.VoiceConfig{URL: "ws://127.0.0.1:1/v1/realtime", Model: "test"},
			WebSocket: config.WebSocketConfig{MaxFrameBytes: 1 << 20},
		},
		Auth: config.AuthConfig{
			StaticBearerEnv: "STATIC_TOKEN", StaticTenantID: "tenant", StaticUserID: "user",
			StaticScopes: []string{"realtime"}, AllowedOrigins: []string{"http://app.example"},
		},
		Admission: config.AdmissionConfig{GlobalRate: 10, GlobalBurst: 10, IPRate: 10, IPBurst: 10, IPKeyTTL: time.Minute, MaxIPKeys: 10},
		Store:     config.StoreConfig{Retention: config.RetentionConfig{ApprovalTTL: 5 * time.Minute, HMACKeyEnv: "RETENTION_KEY", HMACKeyID: "retention-v1"}},
	}

	controller, closeAdmission, err := composeAdmission(t.Context(), cfg, &resolved)
	require.NoError(t, err)
	require.Nil(t, closeAdmission)
	assert.Nil(t, resolved.staticBearer)
	assert.Equal(t, make([]byte, len(staticStaging)), staticStaging)
	request := httptest.NewRequest(http.MethodGet, "http://gateway.example/v1/realtime", nil)
	request.RemoteAddr = "192.0.2.1:1234"
	request.Header.Set("Origin", "http://app.example")
	request.Header.Set("Authorization", "Bearer development-token")
	_, release, err := controller.Admit(t.Context(), request)
	require.NoError(t, err, "static authenticator must retain its digest after staging is cleared")
	release()

	authorization := make(chan string, 1)
	provider := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		authorization <- request.Header.Get("Authorization")
		connection, acceptErr := websocket.Accept(writer, request, nil)
		if acceptErr != nil {
			return
		}
		defer connection.CloseNow()
		_, _, _ = connection.Read(context.Background())
	}))
	t.Cleanup(provider.Close)
	cfg.Gateway.Voice.URL = "ws" + strings.TrimPrefix(provider.URL, "http")
	connector, err := composeGatewayConnector(cfg, &resolved)
	require.NoError(t, err)
	assert.Empty(t, resolved.providerCredential)
	providerSession, err := connector.Connect(t.Context(), realtime.OpenRequest{Model: "test"})
	require.NoError(t, err)
	assert.Equal(t, "Bearer provider-token", <-authorization, "provider connector must retain its credential after staging is dropped")
	closeCtx, cancelClose := context.WithTimeout(context.Background(), time.Second)
	defer cancelClose()
	require.NoError(t, providerSession.Close(closeCtx))

	pool, err := newGatewayFeaturePool(t.Context(), &resolved)
	require.NoError(t, err)
	defer pool.Close()
	assert.Empty(t, resolved.databaseURL)
	assert.Contains(t, pool.Config().ConnString(), "postgres://live@127.0.0.1:5432/live", "pool must retain its parsed database configuration after staging is dropped")
	repository, err := newGatewayRepository(pool, cfg, &resolved)
	require.NoError(t, err)
	defer repository.CloseSensitive()
	assert.Nil(t, resolved.retentionKey)
	assert.Equal(t, make([]byte, len(retentionStaging)), retentionStaging)
	assert.True(t, repository.RetentionConfigured(), "repository must retain its owned retention key copy")

	credentials, err := newGatewayReconnectCredentials(&resolved)
	require.NoError(t, err)
	assert.Nil(t, resolved.reconnectKey)
	assert.Equal(t, make([]byte, len(reconnectStaging)), reconnectStaging)
	issued, err := credentials.Issue(session.ReconnectClaims{
		Scope:     handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "conversation"},
		SessionID: "session", Incarnation: 1, Model: "test", ExpiresAt: time.Now().Add(time.Minute),
		ProfileID: "profile", ProfileVersion: 1, ProfileSnapshotHash: strings.Repeat("a", 64),
		ProfileEngineVersion: "jinja-go-v1/minijinja-go/v2.24.0",
	})
	require.NoError(t, err, "reconnect authority must retain its owned key copy")
	assert.NotEmpty(t, issued.Credential)
}

func reserveGatewayTestAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	return address
}

func assertGatewayTestAddressRemainsFree(t *testing.T, address string) {
	t.Helper()
	listener, err := net.Listen("tcp", address)
	require.NoError(t, err, "gateway listener was created before secret validation completed")
	require.NoError(t, listener.Close())
}

func writeDurableGatewayTestConfig(t *testing.T, listenAddress string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	require.NoError(t, os.WriteFile(path, []byte(fmt.Sprintf(`mode: development_durable
gateway:
  advertised_model: durable-test
  listen_addr: %s
  voice:
    role: realtime_voice
    provider: speech_to_speech
    url: ws://127.0.0.1:8765/v1/realtime
    model: durable-test
    api_key_env: PROVIDER_TOKEN
  reconnect:
    hmac_key_env: RECONNECT_KEY
    hmac_key_id: reconnect-v1
store:
  database_url_env: DATABASE_URL
  retention:
    hmac_key_env: RETENTION_KEY
    hmac_key_id: retention-v1
auth:
  static_bearer_env: STATIC_TOKEN
  static_tenant_id: tenant
  static_user_id: user
  static_scopes: [realtime]
features:
  handoffs: true
  tool_encryption_key_id: tool-v1
tools:
  web_search:
    enabled: true
`, listenAddress)), 0o600))
	return path
}

func TestLocalDeploymentRegionDoesNotReadAllowedRegionPolicy(t *testing.T) {
	region, err := deploymentRegion("local", func(name string) (string, bool) {
		if name == "LIVE_ALLOWED_REGIONS" {
			t.Fatal("local deployment read allowed-region policy")
		}
		return "", false
	})
	require.NoError(t, err)
	assert.Equal(t, "local", region)
}

func TestLocalGatewayLivenessIsLoopbackHealthOnly(t *testing.T) {
	server, err := startDefaultGatewayLiveness(t.Context(), gatewayConfigSource{}, time.Second)
	require.NoError(t, err)
	defer server.close()

	client := &http.Client{Timeout: time.Second}
	for _, test := range []struct {
		method string
		path   string
		status int
		body   string
	}{
		{method: http.MethodGet, path: "/health/live", status: http.StatusOK, body: "ok\n"},
		{method: http.MethodHead, path: "/health/live", status: http.StatusOK},
		{method: http.MethodPost, path: "/health/live", status: http.StatusMethodNotAllowed},
		{method: http.MethodGet, path: "/health/ready", status: http.StatusNotFound},
	} {
		t.Run(test.method+test.path, func(t *testing.T) {
			request, err := http.NewRequest(test.method, "http://"+localGatewayLivenessAddress+test.path, nil)
			require.NoError(t, err)
			response, err := client.Do(request)
			require.NoError(t, err)
			defer response.Body.Close()
			assert.Equal(t, test.status, response.StatusCode)
			if test.body != "" {
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Equal(t, test.body, string(body))
			}
		})
	}
}

func TestLocalGatewayLivenessJoinsOnShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	server, err := startDefaultGatewayLiveness(ctx, gatewayConfigSource{}, time.Second)
	require.NoError(t, err)
	cancel()
	select {
	case <-server.done:
	case <-time.After(time.Second):
		t.Fatal("local liveness server did not join after shutdown")
	}
}

func TestLocalGatewayLivenessReportsUnexpectedServeFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	require.NoError(t, listener.Close())
	server := startGatewayLiveness(t.Context(), listener, time.Second)
	select {
	case err := <-server.errors:
		require.EqualError(t, err, "local gateway liveness stopped")
	case <-time.After(time.Second):
		t.Fatal("liveness server failure was not reported")
	}
	server.close()
}

func TestLocalGatewayLivenessUsesConfiguredShutdownBound(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	entered := make(chan struct{})
	release := make(chan struct{})
	server := startGatewayLivenessWithHandler(t.Context(), listener, 20*time.Millisecond, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(entered)
		<-release
	}))
	go func() {
		response, _ := http.Get("http://" + listener.Addr().String())
		if response != nil {
			_ = response.Body.Close()
		}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("liveness handler did not start")
	}
	started := time.Now()
	server.close()
	assert.Less(t, time.Since(started), 250*time.Millisecond)
	select {
	case <-server.done:
	default:
		t.Fatal("liveness server did not join within the configured shutdown bound")
	}
	close(release)
}

func TestExplicitConfigNeverStartsDefaultLiveness(t *testing.T) {
	server, err := startDefaultGatewayLiveness(t.Context(), gatewayConfigSource{Explicit: true, Path: "gateway.yaml"}, time.Second)
	require.NoError(t, err)
	assert.Nil(t, server)
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
		Profile:          snapshot.Profile,
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
	option, err := composeGatewayReconnect(config.Config{Mode: "local"}, nil, &gatewayRuntimeSecrets{}, nil)
	assert.NoError(t, err)
	assert.Nil(t, option)

	cfg := config.Config{Mode: "production"}
	cfg.Gateway.Reconnect.HMACKeyEnv = "RECONNECT_KEY"
	cfg.Gateway.Reconnect.HMACKeyID = "reconnect-v1"
	cfg.Gateway.Reconnect.CredentialTTL = 5 * time.Minute
	cfg.Admission.InstanceID = "gateway-a"
	option, err = composeGatewayReconnect(cfg, nil, &gatewayRuntimeSecrets{}, func(string) (string, bool) { return "", false })
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
auth:
  static_bearer_env: STATIC_TOKEN
  static_tenant_id: tenant
  static_user_id: user
  static_scopes: [realtime]
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
	controller, closeAdmission, err := composeAdmission(t.Context(), cfg, &gatewayRuntimeSecrets{staticBearer: []byte("development-token")})
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
	require.NoError(t, os.WriteFile(path, []byte("mode: local\ngateway:\n  voice:\n    api_key_env: PROVIDER_SECRET\nauth:\n  static_bearer_env: STATIC_TOKEN\n  static_tenant_id: tenant\n  static_user_id: user\n  static_scopes: [realtime]\n"), 0600))
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

func TestComposeGatewayVerticalProfilesRequiresDurableStorage(t *testing.T) {
	off := config.Config{Mode: "development_durable"}
	option, err := composeGatewayVerticalProfiles(off, nil)
	require.NoError(t, err)
	assert.Nil(t, option, "a deployment without the feature must not resolve profiles")

	on := off
	on.Features.VerticalProfiles = config.VerticalProfileConfig{Enabled: true, RenderWorkers: 2, RenderQueue: 8}
	option, err = composeGatewayVerticalProfiles(on, nil)
	assert.Nil(t, option)
	assert.ErrorContains(t, err, "durable profile storage")

	option, err = composeGatewayVerticalProfiles(on, &gatewayFeatureRuntime{})
	assert.Nil(t, option)
	assert.ErrorContains(t, err, "durable profile storage")

	option, err = composeGatewayVerticalProfiles(on, &gatewayFeatureRuntime{repo: &postgres.Repository{}})
	require.NoError(t, err)
	require.NotNil(t, option)
}
