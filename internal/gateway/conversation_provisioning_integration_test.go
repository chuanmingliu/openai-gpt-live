//go:build integration

package gateway

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/config"
	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/presentation"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/chuanmingliu/openai-gpt-live/internal/session"
	"github.com/chuanmingliu/openai-gpt-live/internal/store/postgres"
	"github.com/chuanmingliu/openai-gpt-live/internal/testkit"
	transportws "github.com/chuanmingliu/openai-gpt-live/internal/transport/ws"
	"github.com/chuanmingliu/openai-gpt-live/migrations"
	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

type provisioningHandoffIDs struct{}

type provisioningJitter struct{}

func (provisioningJitter) Duration(time.Duration) time.Duration { return 0 }

func (provisioningHandoffIDs) NewHandoffID() identity.HandoffID { return "handoff-provisioned" }
func (provisioningHandoffIDs) NewEventID() string               { return "event-provisioned" }

func TestProductionWebSocketProvisionsConversationBeforeFirstTranscriptAndHandoff(t *testing.T) {
	pool := testkit.Postgres(t)
	require.NoError(t, migrations.Up(t.Context(), pool))
	repository, err := postgres.NewRepository(pool, clock.Real{})
	require.NoError(t, err)
	t.Cleanup(repository.CloseSensitive)
	service, err := handoff.NewService(repository, clock.Real{}, provisioningHandoffIDs{})
	require.NoError(t, err)
	listener, err := postgres.NewListener(pool, clock.Real{}, provisioningJitter{}, postgres.ListenerConfig{ReconnectDelay: 10 * time.Millisecond})
	require.NoError(t, err)

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
  retention: {conversation_ttl: 24h, hmac_key_env: LIVE_RETENTION_HMAC_KEY, hmac_key_id: retention-v1}
auth:
  oidc_issuer: https://oidc.invalid
  oidc_audience: live
  oidc_required_scopes: [realtime]
  allowed_origins: [https://app.invalid]
admission: {instance_id: gateway-provisioning-integration}
safety: {policy_version: static-v1, mode: enforce, privacy_hmac_key_env: LIVE_PRIVACY_HMAC_KEY}
worker:
  reasoner: {base_url: https://reasoner.invalid, model: reasoner-v1}
features: {handoffs: true, runtime_refresh_interval: 1s}
`), 0o600))
	cfg, err := config.LoadUnresolved(path)
	require.NoError(t, err)
	connector := testkit.NewFakeConnector(32)
	app, err := New(cfg, connector,
		WithIDGenerator(deterministicIDs{}),
		WithAdmission(transportws.AdmissionFunc(func(context.Context, *http.Request) (session.Subject, func(), error) {
			return session.Subject{TenantID: "tenant-provision", UserID: "user-provision", Scopes: map[string]struct{}{"realtime": {}}, ExpiresAt: time.Now().Add(time.Hour)}, func() {}, nil
		})),
		WithDurableHandoffs(DurableDependencies{
			Service: service, ConversationWriter: repository, ConversationProvisioner: repository, ConversationTTL: 24 * time.Hour,
			RecoveryStore: repository, Journal: repository, DeliveryAffinity: "gateway-provisioning-affinity",
			JournalOwner: "gateway-provisioning-owner", JournalClaimLease: time.Second,
			HandoffQueueCapacity: 8, TranscriptQueueCapacity: 8,
			Snapshot: SessionHandoffSnapshot{
				Profile:    handoff.LegacyProfileSnapshot(),
				ContextRef: "context://integration", ContextHash: "integration-hash", Deadline: time.Now().Add(time.Hour),
				TokenBudget: 128, ToolAllowlist: []string{}, ApprovalPolicy: json.RawMessage(`{"mode":"none"}`),
				Versions:         handoff.Versions{Model: "reasoner-v1", Prompt: "static-v1", ToolRegistry: "integration", Config: "integration"},
				PresentationHint: "brief",
			},
		}),
		WithHandoffPresentation(HandoffPresentationDependencies{
			Reader: repository, Cursors: repository, Listener: listener, Clock: clock.Real{}, PollInterval: 10 * time.Millisecond,
			ConsumerID: "gateway-provisioning-consumer", WakeupCapacity: 8,
			Policy: presentation.NewPolicy(presentation.Config{}), Safety: gatewayAllowSafety{},
		}),
	)
	require.NoError(t, err)
	networkListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	app.listener = networkListener
	runCtx, cancelRun := context.WithCancel(t.Context())
	done := make(chan error, 1)
	var stopped bool
	go func() { done <- app.Run(runCtx) }()
	t.Cleanup(func() {
		cancelRun()
		if stopped {
			return
		}
		select {
		case runErr := <-done:
			require.NoError(t, runErr)
		case <-time.After(5 * time.Second):
			t.Fatal("production gateway did not drain")
		}
	})

	dialCtx, cancelDial := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelDial()
	conn, response, err := websocket.Dial(dialCtx, "ws://"+networkListener.Addr().String()+"/v1/realtime", nil)
	if response != nil && response.Body != nil {
		response.Body.Close()
	}
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.CloseNow() })
	voice, err := connector.NextSession(dialCtx)
	require.NoError(t, err)
	emit(t, voice, realtime.Event{Kind: realtime.EventSessionCreated, Payload: json.RawMessage(`{"id":"provider-session"}`)})
	client := &gatewayClient{t: t, conn: conn}
	client.expectType("session.created")

	scope := handoff.Scope{TenantID: "tenant-provision", UserID: "user-provision", ConversationID: "conversation-fixed"}
	require.Eventually(t, func() bool {
		var count int
		return pool.QueryRow(t.Context(), `SELECT count(*) FROM conversations WHERE tenant_id=$1 AND id=$2 AND user_id=$3`,
			scope.TenantID, scope.ConversationID, scope.UserID).Scan(&count) == nil && count == 1
	}, 2*time.Second, 5*time.Millisecond)

	emit(t, voice, realtime.Event{Kind: realtime.EventSpeechStarted, ProviderEventID: "speech-start", ItemID: "user-item"})
	emit(t, voice, realtime.Event{Kind: realtime.EventSpeechStopped, ProviderEventID: "speech-stop", ItemID: "user-item"})
	emit(t, voice, realtime.Event{Kind: realtime.EventTranscriptionCompleted, ProviderEventID: "transcript", ItemID: "user-item", Text: "investigate the outage"})
	client.expectType("input_audio_buffer.speech_started")
	client.expectType("input_audio_buffer.speech_stopped")
	client.expectType("conversation.item.input_audio_transcription.completed")
	require.Eventually(t, func() bool {
		var count int
		return pool.QueryRow(t.Context(), `SELECT count(*) FROM conversation_items WHERE tenant_id=$1 AND conversation_id=$2 AND content=$3`,
			scope.TenantID, scope.ConversationID, "investigate the outage").Scan(&count) == nil && count == 1
	}, 2*time.Second, 5*time.Millisecond)
	require.Eventually(t, func() bool {
		var count int
		return pool.QueryRow(t.Context(), `SELECT count(*) FROM coordinator_work_journal WHERE kind='transcript' AND tenant_id=$1 AND conversation_id=$2 AND delivered=true`,
			scope.TenantID, scope.ConversationID).Scan(&count) == nil && count == 1
	}, 2*time.Second, 5*time.Millisecond)

	client.write([]byte(`{"type":"response.create"}`))
	create := nextCommand(t, voice)
	require.Equal(t, realtime.CommandCreateResponse, create.Kind)
	emit(t, voice, realtime.Event{Kind: realtime.EventResponseCreated, Epoch: create.Epoch, ProviderEventID: "response-created", ResponseID: "response-provision", Status: "in_progress"})
	client.expectType("response.created")
	emit(t, voice, realtime.Event{
		Kind: realtime.EventFunctionCallDone, Epoch: create.Epoch, ProviderEventID: "function-done", ResponseID: "response-provision",
		ItemID: "tool-item", CallID: "tool-call", Name: handoff.ToolStartBackgroundTask,
		Arguments: json.RawMessage(`{"objective":"investigate the outage"}`),
	})
	client.expectType("response.function_call_arguments.done")
	var runErr error
	created := false
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case runErr = <-done:
			stopped = true
			break
		default:
		}
		var count int
		if pool.QueryRow(t.Context(), `SELECT count(*) FROM handoffs WHERE tenant_id=$1 AND conversation_id=$2 AND id='handoff-provisioned'`,
			scope.TenantID, scope.ConversationID).Scan(&count) == nil && count == 1 {
			created = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if stopped {
		t.Fatalf("production gateway stopped during first handoff: %v", runErr)
	}
	if !created {
		var journal string
		_ = pool.QueryRow(t.Context(), `SELECT COALESCE(json_agg(json_build_object('kind',kind,'delivered',delivered,'payload',payload)),'[]')::text FROM coordinator_work_journal`).Scan(&journal)
		t.Fatalf("first durable handoff was not created; coordinator journal=%s", journal)
	}
}
