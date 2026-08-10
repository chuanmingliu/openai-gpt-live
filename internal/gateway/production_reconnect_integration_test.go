//go:build integration

package gateway

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/protocol"
	"github.com/chuanmingliu/openai-gpt-live/internal/session"
	"github.com/chuanmingliu/openai-gpt-live/internal/store/postgres"
	"github.com/chuanmingliu/openai-gpt-live/internal/testkit"
	transportws "github.com/chuanmingliu/openai-gpt-live/internal/transport/ws"
	"github.com/chuanmingliu/openai-gpt-live/migrations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errIntegrationClientClosed = errors.New("integration session client closed")

type integrationSessionClient struct {
	reads  chan protocol.ClientEvent
	writes chan protocol.ServerEvent
	closed chan struct{}
	once   sync.Once
}

func newIntegrationSessionClient() *integrationSessionClient {
	return &integrationSessionClient{
		reads: make(chan protocol.ClientEvent), writes: make(chan protocol.ServerEvent, 16), closed: make(chan struct{}),
	}
}

func (c *integrationSessionClient) Read(ctx context.Context) (protocol.ClientEvent, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.closed:
		return nil, errIntegrationClientClosed
	case event := <-c.reads:
		return event, nil
	}
}

func (c *integrationSessionClient) Write(ctx context.Context, event protocol.ServerEvent) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closed:
		return errIntegrationClientClosed
	case c.writes <- event:
		return nil
	}
}

func (c *integrationSessionClient) Close(context.Context, int, string) error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

func nextIntegrationServerEvent(t *testing.T, client *integrationSessionClient) protocol.ServerEvent {
	t.Helper()
	select {
	case event := <-client.writes:
		return event
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for session server event")
		return nil
	}
}

func TestProductionFeatureOffReconnectsAcrossPostgresBackedReplicas(t *testing.T) {
	pool := testkit.Postgres(t)
	require.NoError(t, migrations.Up(t.Context(), pool))
	repository, err := postgres.NewRepository(pool, clock.Real{})
	require.NoError(t, err)
	t.Cleanup(repository.CloseSensitive)
	ownersA, err := postgres.NewReconnectOwners(pool)
	require.NoError(t, err)
	ownersB, err := postgres.NewReconnectOwners(pool)
	require.NoError(t, err)
	credentials, err := session.NewReconnectCredentials([]byte("0123456789abcdef0123456789abcdef"), clock.Real{})
	require.NoError(t, err)

	cfg := validProductionConfig()
	cfg.Features.Handoffs = false
	cfg.Store.Retention.ConversationTTL = 24 * time.Hour
	admission := transportws.AdmissionFunc(func(context.Context, *http.Request) (session.Subject, func(), error) {
		return session.Subject{TenantID: "tenant-production-reconnect", UserID: "user-production-reconnect"}, func() {}, nil
	})
	newReplica := func(ownerID, affinity string, connector *testkit.FakeConnector, owners session.ReconnectOwnerStore) *App {
		durable := DurableDependencies{
			ConversationWriter: repository, ConversationProvisioner: repository, ConversationTTL: 24 * time.Hour,
			RecoveryStore: repository, Journal: repository, DeliveryAffinity: affinity, JournalOwner: ownerID,
			JournalClaimLease: time.Second, TranscriptQueueCapacity: 8,
		}
		app, appErr := New(cfg, connector, WithIDGenerator(deterministicIDs{}), WithAdmission(admission),
			WithReconnectAuthority(credentials, owners, ownerID, time.Minute), WithDurableHandoffs(durable))
		require.NoError(t, appErr)
		require.False(t, app.managerConfig.HandoffToolsEnabled)
		require.Same(t, repository, app.managerConfig.RecoveryStore)
		return app
	}

	connectorA := testkit.NewFakeConnector(8)
	connectorB := testkit.NewFakeConnector(8)
	replicaA := newReplica("replica-a", "transcript-shard-region-a", connectorA, ownersA)
	replicaB := newReplica("replica-b", "transcript-shard-region-a", connectorB, ownersB)
	assert.Equal(t, replicaA.durable.DeliveryAffinity, replicaB.durable.DeliveryAffinity)
	assert.NotEqual(t, replicaA.durable.JournalOwner, replicaB.durable.JournalOwner)

	subject := session.Subject{
		TenantID: "tenant-production-reconnect", UserID: "user-production-reconnect", ExpiresAt: time.Now().Add(time.Hour),
	}
	clientA := newIntegrationSessionClient()
	runCtx, cancelRun := context.WithCancel(t.Context())
	runDone := make(chan error, 1)
	go func() {
		runDone <- replicaA.manager.Run(runCtx, session.OpenRequest{Subject: subject, RequestedModel: "model-default"}, clientA)
	}()
	providerCtxA, cancelProviderA := context.WithTimeout(t.Context(), 5*time.Second)
	_, err = connectorA.NextSession(providerCtxA)
	cancelProviderA()
	require.NoError(t, err)
	createdA, ok := nextIntegrationServerEvent(t, clientA).(protocol.SessionCreatedEvent)
	require.True(t, ok)
	require.NotNil(t, createdA.Reconnect)
	require.Equal(t, uint64(1), createdA.Reconnect.Incarnation)
	require.Eventually(t, func() bool {
		var state string
		return pool.QueryRow(t.Context(), `SELECT owner_state FROM session_reconnect_owners WHERE session_id=$1`, createdA.Reconnect.SessionID).Scan(&state) == nil && state == "active"
	}, 5*time.Second, 10*time.Millisecond, "replica A did not durably publish reconnect ownership")
	cancelRun()
	runErr := <-runDone
	require.Error(t, runErr)

	claims, err := credentials.Verify(createdA.Reconnect.Credential)
	require.NoError(t, err)
	var ownerState, credentialID, ownerID, incarnation string
	var turnBoundary bool
	var eventCursor int64
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT owner_state,turn_boundary,credential_id,owner_id,incarnation::text,event_cursor
		FROM session_reconnect_owners WHERE session_id=$1`, claims.SessionID).Scan(
		&ownerState, &turnBoundary, &credentialID, &ownerID, &incarnation, &eventCursor))
	require.Equal(t, "tombstoned", ownerState, "replica A cleanup error: %v", runErr)
	require.True(t, turnBoundary)
	require.Equal(t, claims.CredentialID, credentialID)
	require.Equal(t, "replica-a", ownerID)
	require.Equal(t, "1", incarnation)
	require.Equal(t, claims.Cursor, eventCursor)
	scope := handoff.Scope{
		TenantID: claims.Scope.TenantID, UserID: claims.Scope.UserID, ConversationID: claims.Scope.ConversationID,
	}
	clientB := newIntegrationSessionClient()
	reconnectCtx, cancelReconnect := context.WithCancel(t.Context())
	reconnectDone := make(chan error, 1)
	go func() {
		reconnectDone <- replicaB.manager.Reconnect(reconnectCtx, session.ReconnectRequest{
			Scope: scope, SessionID: claims.SessionID, LastIncarnation: claims.Incarnation,
			LastEventSeq: claims.Cursor, Subject: subject, Credential: claims,
		}, clientB)
	}()
	providerCtxB, cancelProviderB := context.WithTimeout(t.Context(), 5*time.Second)
	_, providerErr := connectorB.NextSession(providerCtxB)
	cancelProviderB()
	if providerErr != nil {
		select {
		case reconnectErr := <-reconnectDone:
			t.Fatalf("reconnect ended before replica B opened a provider session: %v", reconnectErr)
		default:
			t.Fatalf("replica B did not open a provider session: %v", providerErr)
		}
	}
	createdB, ok := nextIntegrationServerEvent(t, clientB).(protocol.SessionCreatedEvent)
	require.True(t, ok)
	require.NotNil(t, createdB.Reconnect)
	assert.Equal(t, uint64(2), createdB.Reconnect.Incarnation)
	assert.Equal(t, string(claims.Scope.ConversationID), createdB.Reconnect.ConversationID)
	cancelReconnect()
	require.Error(t, <-reconnectDone)
}
