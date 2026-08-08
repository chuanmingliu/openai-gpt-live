//go:build integration

package postgres

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/protocol"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/chuanmingliu/openai-gpt-live/internal/session"
	"github.com/chuanmingliu/openai-gpt-live/internal/testkit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type reconnectIntegrationIDs struct {
	session      identity.SessionID
	conversation identity.ConversationID
}

func (i reconnectIntegrationIDs) NewSessionID() identity.SessionID           { return i.session }
func (i reconnectIntegrationIDs) NewConversationID() identity.ConversationID { return i.conversation }

type reconnectIntegrationClient struct {
	writes chan protocol.ServerEvent
	done   chan struct{}
	once   sync.Once
}

func newReconnectIntegrationClient() *reconnectIntegrationClient {
	return &reconnectIntegrationClient{writes: make(chan protocol.ServerEvent, 8), done: make(chan struct{})}
}

func (c *reconnectIntegrationClient) Read(ctx context.Context) (protocol.ClientEvent, error) {
	select {
	case <-c.done:
		return nil, io.EOF
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *reconnectIntegrationClient) Write(ctx context.Context, event protocol.ServerEvent) error {
	select {
	case c.writes <- event:
		return nil
	case <-c.done:
		return io.ErrClosedPipe
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *reconnectIntegrationClient) Close(context.Context, int, string) error {
	c.once.Do(func() { close(c.done) })
	return nil
}

func TestManagerReconnectRecoversPendingApprovalFromPostgres(t *testing.T) {
	repo, scope, record := createPendingApproval(t, "manager-reconnect", 5*time.Minute)
	recovery, err := repo.LoadRecovery(t.Context(), scope, 8, 1024)
	require.NoError(t, err)
	require.Len(t, recovery.PendingApprovals, 1)
	pending := recovery.PendingApprovals[0]
	require.Equal(t, record.InvocationID, pending.Approval.InvocationID)

	connector := testkit.NewFakeConnector(4)
	ids := reconnectIntegrationIDs{session: "session-manager-reconnect", conversation: scope.ConversationID}
	manager := session.NewManager(session.Config{
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
		RecoveryStore:       repo,
		RecoveryItemLimit:   8,
		RecoveryByteLimit:   1024,
	}, connector, ids)

	oldCtx, oldCancel := context.WithCancel(context.Background())
	defer oldCancel()
	oldClient := newReconnectIntegrationClient()
	oldDone := make(chan error, 1)
	go func() {
		oldDone <- manager.Run(oldCtx, session.OpenRequest{Subject: session.Subject{
			TenantID: scope.TenantID, UserID: scope.UserID, ExpiresAt: time.Now().Add(time.Hour),
		}, ConversationID: scope.ConversationID}, oldClient)
	}()
	oldVoice, err := connector.NextSession(t.Context())
	require.NoError(t, err)
	require.Equal(t, "session.created", (<-oldClient.writes).EventType())

	newCtx, newCancel := context.WithCancel(context.Background())
	defer newCancel()
	newClient := newReconnectIntegrationClient()
	newDone := make(chan error, 1)
	go func() {
		for {
			err := manager.Reconnect(newCtx, session.ReconnectRequest{
				Scope: scope, SessionID: ids.session, LastIncarnation: 1, LastEventSeq: recovery.LastEventSeq,
			}, newClient)
			if !errors.Is(err, session.ErrStaleReconnect) {
				newDone <- err
				return
			}
			select {
			case <-newCtx.Done():
				newDone <- newCtx.Err()
				return
			case <-time.After(time.Millisecond):
			}
		}
	}()
	replacement := make(chan error, 1)
	go func() {
		_, nextErr := connector.NextSession(newCtx)
		replacement <- nextErr
	}()
	select {
	case err = <-replacement:
		require.NoError(t, err)
	case err = <-newDone:
		require.NoError(t, err, "reconnect failed before opening replacement provider")
	}
	require.Equal(t, "session.created", (<-newClient.writes).EventType())
	replayed, ok := (<-newClient.writes).(protocol.LiveToolApprovalRequiredEvent)
	require.True(t, ok)
	assert.Equal(t, pending.EventID, replayed.EventID)
	assert.Equal(t, pending.Sequence, replayed.SequenceID)
	assert.Equal(t, record.InvocationID, replayed.InvocationID)
	assert.Equal(t, string(pending.Approval.HandoffID), replayed.HandoffID)
	encoded, err := protocol.Encode(replayed)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), "arguments")
	assert.NotContains(t, string(encoded), "result")

	newCancel()
	select {
	case <-newDone:
	case <-time.After(time.Second):
		t.Fatal("reconnected runtime did not stop")
	}
	select {
	case <-oldDone:
	case <-time.After(time.Second):
		t.Fatal("old runtime did not quiesce")
	}
	require.Error(t, oldVoice.Emit(t.Context(), realtime.Event{Kind: realtime.EventResponseDone, Epoch: 1}), "old provider remains fenced after reconnect")
}
