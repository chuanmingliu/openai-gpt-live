package gateway

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/session"
	"github.com/stretchr/testify/require"
)

type blockingApprovalResolutionStore struct {
	started  chan struct{}
	release  chan struct{}
	resolved chan struct{}
	once     sync.Once
	mu       sync.Mutex
	calls    int
	scope    handoff.Scope
	value    handoff.ApprovalResolution
	deadline time.Time
}

func (s *blockingApprovalResolutionStore) ResolveApproval(ctx context.Context, scope handoff.Scope, value handoff.ApprovalResolution) error {
	s.mu.Lock()
	s.calls++
	s.scope, s.value = scope, value
	s.deadline, _ = ctx.Deadline()
	s.mu.Unlock()
	s.once.Do(func() { close(s.started) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.release:
		close(s.resolved)
		return nil
	}
}

func (*blockingApprovalResolutionStore) LoadApproval(context.Context, handoff.Scope, string) (handoff.ToolApproval, error) {
	return handoff.ToolApproval{}, handoff.ErrNotFound
}

func TestApprovalCoordinatorCompletesAcceptedResolutionOnceAfterLiveSessionDisconnect(t *testing.T) {
	store := &blockingApprovalResolutionStore{
		started: make(chan struct{}), release: make(chan struct{}), resolved: make(chan struct{}),
	}
	submit := &submitterFake{err: session.ErrSessionNotActive}
	coordinator := NewApprovalCoordinator(store, submit, 2, 200*time.Millisecond)
	require.NotNil(t, coordinator)
	resolvedAt := time.Now().UTC()
	work := ApprovalWork{
		SessionID: "approval-session", Incarnation: 7, ExternalSequence: 3,
		Scope: handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "conversation"},
		Resolution: handoff.ApprovalResolution{
			InvocationID: "invocation", Decision: handoff.ApprovalApproved,
			ActorTenantID: "tenant", ActorUserID: "user", Reason: "confirmed",
			IdempotencyKey: "approval-key", ResolvedAt: resolvedAt,
		},
	}
	require.NoError(t, coordinator.Enqueue(work))

	runCtx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- coordinator.Run(runCtx) }()
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("durable approval resolution did not start")
	}
	// The submitter now represents a disconnected/stale live incarnation. Its
	// completion rejection must not cancel the app-owned store operation.
	close(store.release)
	select {
	case <-store.resolved:
	case <-time.After(time.Second):
		t.Fatal("durable approval resolution did not survive disconnect")
	}
	require.Eventually(t, func() bool { return len(coordinator.slots) == 0 }, time.Second, time.Millisecond)

	store.mu.Lock()
	require.Equal(t, 1, store.calls)
	require.Equal(t, work.Scope, store.scope)
	require.Equal(t, work.Resolution, store.value)
	require.True(t, store.deadline.After(resolvedAt))
	require.LessOrEqual(t, store.deadline.Sub(time.Now()), 200*time.Millisecond)
	store.mu.Unlock()

	cancel()
	require.NoError(t, <-runDone)
}

func TestApprovalCoordinatorQueueIsBounded(t *testing.T) {
	coordinator := NewApprovalCoordinator(
		&blockingApprovalResolutionStore{}, &submitterFake{}, 1, time.Second,
	)
	require.NotNil(t, coordinator)
	work := ApprovalWork{
		SessionID: "session", Incarnation: 1, ExternalSequence: 1,
		Scope: handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "conversation"},
		Resolution: handoff.ApprovalResolution{
			InvocationID: "invocation", Decision: handoff.ApprovalDenied,
			ActorTenantID: "tenant", ActorUserID: "user", IdempotencyKey: "key", ResolvedAt: time.Now().UTC(),
		},
	}
	require.NoError(t, coordinator.Enqueue(work))
	require.ErrorIs(t, coordinator.Enqueue(work), ErrCoordinatorOverflow)
}

func TestApprovalDispatcherBindsOnlyTrustedScopeAndActor(t *testing.T) {
	coordinator := NewApprovalCoordinator(
		&blockingApprovalResolutionStore{}, &submitterFake{}, 1, time.Second,
	)
	dispatcher := &approvalDispatcher{coordinator: coordinator}
	trusted := session.TrustedSessionContext{
		IDs:     session.IDs{SessionID: "trusted-session", ConversationID: "trusted-conversation", Incarnation: identity.Incarnation(9)},
		Subject: session.Subject{TenantID: "trusted-tenant", UserID: "trusted-user"},
	}
	require.NoError(t, dispatcher.DispatchApproval(trusted, session.ApprovalResolutionRequestedEffect{
		ExternalSequence: 4, InvocationID: "invocation", Decision: handoff.ApprovalApproved,
		Reason: "confirmed", IdempotencyKey: "trusted-key", ResolvedAt: time.Now().UTC(),
	}))
	queued := <-coordinator.work
	require.Equal(t, identity.SessionID("trusted-session"), queued.SessionID)
	require.Equal(t, identity.Incarnation(9), queued.Incarnation)
	require.Equal(t, handoff.Scope{TenantID: "trusted-tenant", UserID: "trusted-user", ConversationID: "trusted-conversation"}, queued.Scope)
	require.Equal(t, "trusted-tenant", queued.Resolution.ActorTenantID)
	require.Equal(t, "trusted-user", queued.Resolution.ActorUserID)
	require.Equal(t, "trusted-key", queued.Resolution.IdempotencyKey)
}
