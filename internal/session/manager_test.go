package session

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/feature"
	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/presentation"
	"github.com/chuanmingliu/openai-gpt-live/internal/profile"
	"github.com/chuanmingliu/openai-gpt-live/internal/protocol"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime/s2sws"
	"github.com/chuanmingliu/openai-gpt-live/internal/testkit"
	"github.com/chuanmingliu/openai-gpt-live/internal/toolpolicy"
	"github.com/stretchr/testify/require"
)

func TestManagerPublishesOwnedSafeProfileSelection(t *testing.T) {
	connector := testkit.NewFakeConnector(2)
	manager := NewManager(Config{IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second}, connector,
		managerIDs{session: "profile-session", conversation: "profile-conversation"})
	client := newChannelClient(4)
	selected := profile.Builtin()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- manager.Run(ctx, OpenRequest{Subject: Subject{TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour)}, Profile: selected}, client)
	}()
	_, err := connector.NextSession(t.Context())
	require.NoError(t, err)
	select {
	case event := <-client.writes:
		require.IsType(t, protocol.SessionCreatedEvent{}, event)
	case runErr := <-done:
		t.Fatalf("manager failed before profile publication: %v", runErr)
	}
	selectedEvent := (<-client.writes).(protocol.LiveProfileSelectedEvent)
	require.Equal(t, "builtin", selectedEvent.Profile.ID)
	require.Equal(t, uint64(1), selectedEvent.Profile.Version)
	require.Equal(t, selected.Snapshot.SnapshotHash, selectedEvent.Profile.SnapshotHash)
	selected.Snapshot.Ref.ID = "mutated"
	manager.activeMu.RLock()
	retained := manager.logical["profile-session"].profile.Clone()
	manager.activeMu.RUnlock()
	require.Equal(t, profile.ID("builtin"), retained.Snapshot.Ref.ID)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestManagerWithoutExplicitProfileDoesNotPublishSelection(t *testing.T) {
	connector := testkit.NewFakeConnector(2)
	manager := NewManager(Config{IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second}, connector,
		managerIDs{session: "profile-disabled", conversation: "profile-disabled"})
	client := newChannelClient(4)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- manager.Run(ctx, OpenRequest{Subject: Subject{TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour)}}, client)
	}()
	_, err := connector.NextSession(t.Context())
	require.NoError(t, err)
	require.IsType(t, protocol.SessionCreatedEvent{}, <-client.writes)
	_ = waitForActiveRuntime(t, manager, "profile-disabled", 1)
	select {
	case event := <-client.writes:
		t.Fatalf("disabled profile mode published unexpected event %T", event)
	default:
	}
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestManagerReconnectAuthorityPublishesCustomProfileSelection(t *testing.T) {
	manual := clock.NewManual(time.Now().UTC())
	credentials, err := NewReconnectCredentials([]byte(strings.Repeat("z", 32)), manual)
	require.NoError(t, err)
	connector := testkit.NewFakeConnector(2)
	owners := NewMemoryReconnectOwners(manual)
	manager := NewManager(Config{IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
		ReconnectCredentials: credentials, ReconnectOwners: owners, ReconnectOwnerID: "profile-owner",
		ReconnectCredentialTTL: time.Minute, Clock: manual}, connector,
		managerIDs{session: "custom-profile-session", conversation: "custom-profile-conversation"})
	selected := profile.Builtin()
	selected.Snapshot.TenantID = "tenant"
	selected.Snapshot.Ref = profile.VersionRef{ID: "insurance", Version: 3}
	selected.Snapshot.SnapshotHash = ""
	selected.Snapshot.SnapshotHash, err = selected.Snapshot.Hash()
	require.NoError(t, err)
	client := newChannelClient(4)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- manager.Run(ctx, OpenRequest{Subject: Subject{TenantID: "tenant", UserID: "user", ExpiresAt: manual.Now().Add(time.Hour)},
			RequestedModel: "provider-model", Profile: selected}, client)
	}()
	_, err = connector.NextSession(t.Context())
	require.NoError(t, err)
	select {
	case event := <-client.writes:
		require.IsType(t, protocol.SessionCreatedEvent{}, event)
	case runErr := <-done:
		t.Fatalf("manager failed before custom profile publication: %v", runErr)
	}
	require.IsType(t, protocol.LiveProfileSelectedEvent{}, <-client.writes)
	_ = waitForActiveRuntime(t, manager, "custom-profile-session", 1)
	require.Eventually(t, func() bool {
		owner, ok := owners.Read("custom-profile-session")
		return ok && owner.State == ReconnectOwnerActive
	}, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestManagerReconnectRequiresExactRetainedProfileSnapshot(t *testing.T) {
	manager := NewManager(Config{}, testkit.NewFakeConnector(1), managerIDs{})
	selected := profile.Builtin()
	scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "conversation"}
	logical := &logicalSession{scope: scope, highWater: 1, turnBoundary: true, state: logicalTombstoned, profile: selected.Clone()}
	request := ReconnectRequest{Scope: scope, SessionID: "session", LastIncarnation: 1, Profile: selected.Clone()}
	require.NoError(t, manager.checkReconnectLocked(request, logical))

	different := selected.Clone()
	different.Snapshot.AdapterRegistryVersion = "builtin-v2"
	different.Snapshot.SnapshotHash = ""
	hash, err := different.Snapshot.Hash()
	require.NoError(t, err)
	different.Snapshot.SnapshotHash = hash
	request.Profile = different
	require.ErrorIs(t, manager.checkReconnectLocked(request, logical), ErrInvalidReconnectCredential)
}

func TestManagerRejectsProfileBundleWithoutImmutableSnapshot(t *testing.T) {
	_, _, err := normalizeBoundProfile(Subject{TenantID: "tenant"}, profile.Resolved{Bundle: profile.Builtin().Bundle})
	require.ErrorIs(t, err, profile.ErrInvalidProfile)
	_, _, err = normalizeBoundProfile(Subject{TenantID: "tenant"}, profile.Resolved{Definition: profile.Builtin().Definition})
	require.ErrorIs(t, err, profile.ErrInvalidProfile)
}

func TestManagerRejectsProfileSourceThatDoesNotMatchImmutableSnapshot(t *testing.T) {
	selected := profile.Builtin()
	selected.Bundle.Templates["system.j2"] = "mutated prompt outside snapshot authority"
	_, _, err := normalizeBoundProfile(Subject{TenantID: "tenant"}, selected)
	require.ErrorIs(t, err, profile.ErrInvalidProfile)
}

func withInitialManualInputTurn(cfg Config) Config {
	cfg.InitialSession.TurnDetection = json.RawMessage(`null`)
	return cfg
}

func withInitialProviderVADInputTurn(cfg Config) Config {
	cfg.InitialSession.TurnDetection = json.RawMessage(`{"type":"server_vad"}`)
	return cfg
}

func activateInitialManualInputTurn(t *testing.T, manager *Manager, sessionID identity.SessionID, incarnation identity.Incarnation, voice *testkit.FakeVoiceSession) {
	activateInitialInputTurn(t, manager, sessionID, incarnation, voice, `null`, realtime.InputTurnModeManual)
}

func activateInitialProviderVADInputTurn(t *testing.T, manager *Manager, sessionID identity.SessionID, incarnation identity.Incarnation, voice *testkit.FakeVoiceSession) {
	activateInitialInputTurn(t, manager, sessionID, incarnation, voice, `{"type":"server_vad"}`, realtime.InputTurnModeProviderVAD)
}

func activateInitialInputTurn(
	t *testing.T,
	manager *Manager,
	sessionID identity.SessionID,
	incarnation identity.Incarnation,
	voice *testkit.FakeVoiceSession,
	turnDetection string,
	mode realtime.InputTurnMode,
) {
	t.Helper()
	command, err := voice.NextCommand(t.Context())
	require.NoError(t, err)
	require.Equal(t, realtime.CommandSessionUpdate, command.Kind)
	require.JSONEq(t, turnDetection, string(command.Session.TurnDetection))
	require.NoError(t, voice.Emit(t.Context(), realtime.Event{
		Kind:                         realtime.EventSessionUpdated,
		ProviderEventID:              "initial-input-turn-mode-confirmed",
		InputTurnMode:                mode,
		InputTurnModeSpecified:       true,
		SessionUpdateAcknowledgement: true,
	}))
	require.Eventually(t, func() bool {
		manager.activeMu.RLock()
		runtime := manager.active[runtimeKey{sessionID: sessionID, incarnation: incarnation}]
		manager.activeMu.RUnlock()
		if runtime == nil || runtime.state == nil {
			return false
		}
		return runtime.state.currentInputTurnMode() == mode
	}, time.Second, time.Millisecond)
}

func TestManagerInitialSessionAckCannotConsumePendingBrowserMode(t *testing.T) {
	connector := testkit.NewFakeConnector(8)
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
		InitialSession: realtime.SessionConfig{
			Instructions: "manager bootstrap",
		},
	}, connector, managerIDs{session: "initial-ack-race", conversation: "initial-ack-race"})
	client := newChannelClient(16)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- manager.Run(ctx, OpenRequest{Subject: Subject{
			TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour),
		}}, client)
	}()

	voice, err := connector.NextSession(t.Context())
	require.NoError(t, err)
	initial, err := voice.NextCommand(t.Context())
	require.NoError(t, err)
	require.Equal(t, realtime.CommandSessionUpdate, initial.Kind)
	require.Zero(t, initial.Sequence)
	require.Equal(t, "session.created", (<-client.writes).EventType())
	runtime := waitForActiveRuntime(t, manager, "initial-ack-race", 1)

	require.NoError(t, manager.Submit(t.Context(), "initial-ack-race", 1, ClientInput{
		Event: inputTurnSessionUpdate(json.RawMessage(`null`)),
	}))
	browser, err := voice.NextCommand(t.Context())
	require.NoError(t, err)
	require.Equal(t, realtime.CommandSessionUpdate, browser.Kind)
	require.NotZero(t, browser.Sequence)

	require.NoError(t, voice.Emit(t.Context(), realtime.Event{
		Kind: realtime.EventSessionUpdated, ProviderEventID: "manager-initial-ack",
		InputTurnMode: realtime.InputTurnModeManual, InputTurnModeSpecified: true,
		SessionUpdateAcknowledgement: true,
		SessionUpdateSequence:        0,
	}))
	require.NoError(t, voice.Emit(t.Context(), realtime.Event{
		Kind: realtime.EventError, ProviderEventID: "initial-ack-barrier", Reason: "barrier",
	}))
	for {
		event := <-client.writes
		require.NotEqual(t, "session.updated", event.EventType(), "sequence-zero manager ack stole the browser pending update")
		if event.EventType() == "error" {
			break
		}
	}
	require.Equal(t, realtime.InputTurnModeUnspecified, runtime.state.currentInputTurnMode())

	require.NoError(t, voice.Emit(t.Context(), realtime.Event{
		Kind: realtime.EventSessionUpdated, ProviderEventID: "browser-mode-ack",
		InputTurnMode: realtime.InputTurnModeManual, InputTurnModeSpecified: true,
		SessionUpdateAcknowledgement: true,
		SessionUpdateSequence:        browser.Sequence,
	}))
	for {
		event := <-client.writes
		if event.EventType() == "session.updated" {
			break
		}
	}
	require.Equal(t, realtime.InputTurnModeManual, runtime.state.currentInputTurnMode(),
		"the input boundary must publish before session.updated is externally observable")
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestManagerAcknowledgesPolicyWithoutSendingProviderCommand(t *testing.T) {
	catalog := toolpolicy.DefaultCatalog()
	catalog.WebSearch.ForceStrict = true
	dispatcher := &durableDispatcherFake{
		trusted: make(chan TrustedSessionContext, 1), transcripts: make(chan TranscriptRequestedEffect, 1),
		handoffs: make(chan HandoffRequestedEffect, 1),
	}
	connector := testkit.NewFakeConnector(4)
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
		ToolCatalog: catalog, AsyncModel: "deepseek-v4-flash", DurableDispatcher: dispatcher,
	}, connector, managerIDs{session: "policy-session", conversation: "policy-conversation"})
	client := newChannelClient(8)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- manager.Run(ctx, OpenRequest{Subject: Subject{
			TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour),
		}}, client)
	}()
	voice, err := connector.NextSession(t.Context())
	require.NoError(t, err)
	require.IsType(t, protocol.SessionCreatedEvent{}, <-client.writes)

	requested := toolpolicy.DefaultPolicy()
	requested.WebSearch.Region = "us-en"
	requested.WebSearch.MaxResults = 2
	requested.WebSearch.TimeoutMS = 4_000
	client.reads <- protocol.LiveToolPolicyUpdateEvent{Type: "live.tool_policy.update", EventID: "policy-1", Tools: requested}
	ack := (<-client.writes).(protocol.LiveToolPolicyUpdatedEvent)
	require.Equal(t, "policy-1", ack.EventID)
	require.Equal(t, "deepseek-v4-flash", ack.Model)
	require.Equal(t, toolpolicy.SafeSearchStrict, ack.Tools.WebSearch.SafeSearch)
	require.Equal(t, "us-en", ack.Tools.WebSearch.Region)

	manager.activeMu.RLock()
	var state *runState
	for _, runtime := range manager.active {
		state = runtime.state
	}
	manager.activeMu.RUnlock()
	require.NotNil(t, state)
	require.NoError(t, state.dispatchEffects(t.Context(), []Effect{{Handoff: &HandoffRequestedEffect{
		ResponseID: "response", CallID: "call", ToolName: StartBackgroundTaskTool,
		Arguments: json.RawMessage(`{"objective":"x"}`),
	}}}, 0, 0))
	require.Equal(t, ack.Tools, (<-dispatcher.handoffs).ToolPolicy)

	providerCtx, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	_, err = voice.NextCommand(providerCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)
}

func TestManagerDerivesDefaultPolicyFromCatalogForHandoffs(t *testing.T) {
	dispatcher := &durableDispatcherFake{
		trusted: make(chan TrustedSessionContext, 1), transcripts: make(chan TranscriptRequestedEffect, 1),
		handoffs: make(chan HandoffRequestedEffect, 1),
	}
	catalog := toolpolicy.DefaultCatalog()
	catalog.WebSearch.MaximumResults = 3
	catalog.WebSearch.MaximumTimeoutMS = 5_000
	cfg, err := normalizeManagerConfig(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
		ToolCatalog: catalog, AsyncModel: "deepseek-v4-flash", DurableDispatcher: dispatcher,
	})
	require.NoError(t, err)
	state := newRunState(cfg, testIDs(), &recordingVoice{sent: make(chan realtime.Command, 1)}, newChannelClient(1), func() {})
	handoffEffect := HandoffRequestedEffect{
		ResponseID: "response", CallID: "call", ToolName: StartBackgroundTaskTool,
		Arguments: json.RawMessage(`{"objective":"x"}`),
	}
	require.NoError(t, state.dispatchEffects(t.Context(), []Effect{{Handoff: &handoffEffect}}, 0, 0))
	got := <-dispatcher.handoffs
	require.Equal(t, toolpolicy.Policy{WebSearch: toolpolicy.WebSearchPolicy{
		Enabled: true, SafeSearch: toolpolicy.SafeSearchModerate, Region: "wt-wt", MaxResults: 3, TimeoutMS: 5_000,
	}}, got.ToolPolicy)
}

func TestManagerOwnsToolCatalogRegionStorage(t *testing.T) {
	catalog := toolpolicy.DefaultCatalog()
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second, ToolCatalog: catalog,
	}, testkit.NewFakeConnector(1), managerIDs{})
	require.NoError(t, manager.validationErr)

	catalog.WebSearch.Regions[0] = "caller-mutated"
	require.NotEqual(t, "caller-mutated", manager.cfg.ToolCatalog.WebSearch.Regions[0])
}

type recoveryStoreFake struct {
	snapshot RecoverySnapshot
	err      error
	calls    atomic.Int32
}

type failNthRecoveryStore struct {
	mu       sync.Mutex
	calls    int
	failCall int
}

func (s *failNthRecoveryStore) LoadRecovery(_ context.Context, scope handoff.Scope, _, _ int) (RecoverySnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.calls == s.failCall {
		return RecoverySnapshot{}, errors.New("scripted recovery failure")
	}
	return RecoverySnapshot{ConversationID: scope.ConversationID}, nil
}

type reconnectOwnersProbe struct {
	*MemoryReconnectOwners
	aborts           atomic.Int32
	publishCalls     atomic.Int32
	failPublishCall  int32
	blockPublishCall int32
	publishStarted   chan struct{}
	publishRelease   chan struct{}
	heartbeatStarted chan struct{}
	heartbeatRelease chan struct{}
	rotateStarted    chan struct{}
	rotateRelease    chan struct{}
	heartbeatErr     atomic.Bool
	tombstoneErr     error
}

type blockingCleanupOwners struct {
	*MemoryReconnectOwners
	started chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (s *blockingCleanupOwners) CleanupExpired(ctx context.Context, limit int) (int, error) {
	if s.calls.Add(1) == 1 {
		close(s.started)
		<-s.release
		ctx = context.Background()
	}
	return s.MemoryReconnectOwners.CleanupExpired(ctx, limit)
}

func (s *reconnectOwnersProbe) AbortClaim(ctx context.Context, failed ReconnectOwnerKey, previous ReconnectOwner) error {
	s.aborts.Add(1)
	return s.MemoryReconnectOwners.AbortClaim(ctx, failed, previous)
}

func (s *reconnectOwnersProbe) Heartbeat(ctx context.Context, key ReconnectOwnerKey, epoch identity.ResponseEpoch, boundary bool, lease time.Duration) (ReconnectOwnerLease, error) {
	if s.heartbeatStarted != nil {
		select {
		case <-s.heartbeatStarted:
		default:
			close(s.heartbeatStarted)
		}
		<-s.heartbeatRelease
		ctx = context.Background()
	}
	if s.heartbeatErr.Load() {
		return ReconnectOwnerLease{}, ErrStaleReconnect
	}
	return s.MemoryReconnectOwners.Heartbeat(ctx, key, epoch, boundary, lease)
}

func (s *reconnectOwnersProbe) Publish(ctx context.Context, key ReconnectOwnerKey, epoch identity.ResponseEpoch, boundary bool, lease time.Duration) (ReconnectOwnerLease, error) {
	call := s.publishCalls.Add(1)
	if s.blockPublishCall != 0 && call == s.blockPublishCall {
		close(s.publishStarted)
		<-s.publishRelease
		ctx = context.Background()
	}
	if s.failPublishCall != 0 && call == s.failPublishCall {
		return ReconnectOwnerLease{}, ErrRecoveryUnavailable
	}
	return s.MemoryReconnectOwners.Publish(ctx, key, epoch, boundary, lease)
}

func (s *reconnectOwnersProbe) Rotate(ctx context.Context, key ReconnectOwnerKey, current, next ReconnectClaims, overlapUntil time.Time) error {
	if s.rotateStarted != nil {
		select {
		case <-s.rotateStarted:
		default:
			close(s.rotateStarted)
		}
		<-s.rotateRelease
		ctx = context.Background()
	}
	return s.MemoryReconnectOwners.Rotate(ctx, key, current, next, overlapUntil)
}

func TestManagerReconnectOwnerBlockedPublishAndHeartbeatFenceIOAtOperationDeadline(t *testing.T) {
	t.Run("publish", func(t *testing.T) {
		credentials, err := NewReconnectCredentials([]byte(strings.Repeat("o", 32)), clock.Real{})
		require.NoError(t, err)
		owners := &reconnectOwnersProbe{MemoryReconnectOwners: NewMemoryReconnectOwners(clock.Real{}), blockPublishCall: 1,
			publishStarted: make(chan struct{}), publishRelease: make(chan struct{})}
		defer func() {
			select {
			case <-owners.publishRelease:
			default:
				close(owners.publishRelease)
			}
		}()
		connector := newLifecycleConnector()
		manager := NewManager(Config{
			IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
			ReconnectCredentials: credentials, ReconnectOwners: owners, ReconnectOwnerID: "replica-blocked-publish",
			ReconnectCredentialTTL: 12 * time.Second, ReconnectCredentialOverlap: 2 * time.Second,
			ReconnectOwnerLeaseDuration: 3 * time.Second, ReconnectOwnerHeartbeatInterval: time.Second,
			ReconnectOwnerOperationTimeout: 25 * time.Millisecond, Clock: clock.Real{},
		}, connector, managerIDs{session: "blocked-publish-session", conversation: "blocked-publish-conversation"})
		require.NoError(t, manager.validationErr)
		client := newChannelClient(8)
		done := make(chan error, 1)
		go func() {
			done <- manager.Run(t.Context(), OpenRequest{Subject: Subject{TenantID: "tenant-blocked-publish", UserID: "user", ExpiresAt: time.Now().Add(time.Hour)}, RequestedModel: "provider-model"}, client)
		}()
		<-connector.connected
		voice := connector.voice
		created := (<-client.writes).(protocol.SessionCreatedEvent)
		require.NotNil(t, created.Reconnect)
		<-owners.publishStarted
		select {
		case <-client.done:
		case <-time.After(250 * time.Millisecond):
			t.Fatal("blocked owner Publish did not independently fence client I/O")
		}
		select {
		case <-voice.closed:
		case <-time.After(250 * time.Millisecond):
			t.Fatal("blocked owner Publish did not independently fence provider I/O")
		}
		close(owners.publishRelease)
		require.ErrorIs(t, <-done, ErrRecoveryUnavailable)
	})

	t.Run("heartbeat", func(t *testing.T) {
		credentials, err := NewReconnectCredentials([]byte(strings.Repeat("h", 32)), clock.Real{})
		require.NoError(t, err)
		owners := &reconnectOwnersProbe{MemoryReconnectOwners: NewMemoryReconnectOwners(clock.Real{}),
			heartbeatStarted: make(chan struct{}), heartbeatRelease: make(chan struct{})}
		defer func() {
			select {
			case <-owners.heartbeatRelease:
			default:
				close(owners.heartbeatRelease)
			}
		}()
		connector := newLifecycleConnector()
		manager := NewManager(Config{
			IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
			ReconnectCredentials: credentials, ReconnectOwners: owners, ReconnectOwnerID: "replica-blocked-heartbeat",
			ReconnectCredentialTTL: 12 * time.Second, ReconnectCredentialOverlap: 2 * time.Second,
			ReconnectOwnerLeaseDuration: 3 * time.Second, ReconnectOwnerHeartbeatInterval: time.Second,
			ReconnectOwnerOperationTimeout: 25 * time.Millisecond, Clock: clock.Real{},
		}, connector, managerIDs{session: "blocked-heartbeat-session", conversation: "blocked-heartbeat-conversation"})
		require.NoError(t, manager.validationErr)
		client := newChannelClient(8)
		done := make(chan error, 1)
		go func() {
			done <- manager.Run(t.Context(), OpenRequest{Subject: Subject{TenantID: "tenant-blocked-heartbeat", UserID: "user", ExpiresAt: time.Now().Add(time.Hour)}, RequestedModel: "provider-model"}, client)
		}()
		<-connector.connected
		voice := connector.voice
		<-client.writes
		<-owners.heartbeatStarted
		select {
		case <-client.done:
		case <-time.After(2 * time.Second):
			t.Fatal("blocked owner Heartbeat did not independently fence client I/O")
		}
		select {
		case <-voice.closed:
		case <-time.After(250 * time.Millisecond):
			t.Fatal("blocked owner Heartbeat did not independently fence provider I/O")
		}
		close(owners.heartbeatRelease)
		require.ErrorIs(t, <-done, ErrRecoveryUnavailable)
	})

	t.Run("credential rotation", func(t *testing.T) {
		manual := clock.NewManual(time.Now().UTC())
		credentials, err := NewReconnectCredentials([]byte(strings.Repeat("r", 32)), manual)
		require.NoError(t, err)
		owners := &reconnectOwnersProbe{MemoryReconnectOwners: NewMemoryReconnectOwners(manual),
			rotateStarted: make(chan struct{}), rotateRelease: make(chan struct{})}
		defer func() {
			select {
			case <-owners.rotateRelease:
			default:
				close(owners.rotateRelease)
			}
		}()
		connector := newLifecycleConnector()
		heartbeatReady := make(chan struct{})
		var heartbeatReadyOnce sync.Once
		manager := NewManager(Config{
			IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
			ReconnectCredentials: credentials, ReconnectOwners: owners, ReconnectOwnerID: "replica-blocked-rotation",
			ReconnectCredentialTTL: 12 * time.Second, ReconnectCredentialOverlap: 2 * time.Second,
			ReconnectOwnerLeaseDuration: 6 * time.Second, ReconnectOwnerHeartbeatInterval: time.Second,
			ReconnectOwnerOperationTimeout: 25 * time.Millisecond, Clock: manual,
			reconnectHeartbeatReady: func() { heartbeatReadyOnce.Do(func() { close(heartbeatReady) }) },
		}, connector, managerIDs{session: "blocked-rotation-session", conversation: "blocked-rotation-conversation"})
		require.NoError(t, manager.validationErr)
		client := newChannelClient(16)
		done := make(chan error, 1)
		go func() {
			done <- manager.Run(t.Context(), OpenRequest{Subject: Subject{TenantID: "tenant-blocked-rotation", UserID: "user", ExpiresAt: manual.Now().Add(time.Hour)}, RequestedModel: "provider-model"}, client)
		}()
		<-connector.connected
		voice := connector.voice
		<-client.writes
		<-heartbeatReady
		for range 8 {
			manual.Advance(time.Second)
			time.Sleep(time.Millisecond)
		}
		select {
		case <-owners.rotateStarted:
		case <-time.After(time.Second):
			t.Fatal("credential rotation did not reach the blocked owner store")
		}
		select {
		case <-client.done:
		case <-time.After(250 * time.Millisecond):
			t.Fatal("blocked owner Rotate did not independently fence client I/O")
		}
		select {
		case <-voice.closed:
		case <-time.After(250 * time.Millisecond):
			t.Fatal("blocked owner Rotate did not independently fence provider I/O")
		}
		close(owners.rotateRelease)
		require.ErrorIs(t, <-done, ErrRecoveryUnavailable)
	})
}

func TestManagerReconnectOwnerOperationSlotsBoundNonCooperativeStoreCalls(t *testing.T) {
	credentials, err := NewReconnectCredentials([]byte(strings.Repeat("s", 32)), clock.Real{})
	require.NoError(t, err)
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
		ReconnectCredentials: credentials, ReconnectOwners: NewMemoryReconnectOwners(clock.Real{}), ReconnectOwnerID: "replica-operation-slots",
		ReconnectCredentialTTL: 12 * time.Second, ReconnectCredentialOverlap: 2 * time.Second,
		ReconnectOwnerLeaseDuration: 3 * time.Second, ReconnectOwnerHeartbeatInterval: time.Second,
		ReconnectOwnerOperationTimeout: 10 * time.Millisecond, ReconnectOwnerMaxOperations: 1, Clock: clock.Real{},
	}, testkit.NewFakeConnector(1), managerIDs{})
	require.NoError(t, manager.validationErr)
	started := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		_, operationErr := manager.runReconnectOwnerLeaseOperation(t.Context(), time.Now().Add(time.Second),
			func(context.Context) (ReconnectOwnerLease, error) {
				close(started)
				<-release
				return ReconnectOwnerLease{}, nil
			})
		firstDone <- operationErr
	}()
	<-started
	require.ErrorIs(t, <-firstDone, ErrRecoveryUnavailable)

	var secondCalled atomic.Bool
	secondStarted := time.Now()
	_, err = manager.runReconnectOwnerLeaseOperation(t.Context(), time.Now().Add(time.Second),
		func(context.Context) (ReconnectOwnerLease, error) {
			secondCalled.Store(true)
			return ReconnectOwnerLease{}, nil
		})
	require.ErrorIs(t, err, ErrRecoveryUnavailable)
	require.False(t, secondCalled.Load(), "saturation must fail closed without spawning another unbounded store goroutine")
	require.Less(t, time.Since(secondStarted), 100*time.Millisecond)

	close(release)
	require.Eventually(t, func() bool {
		_, operationErr := manager.runReconnectOwnerLeaseOperation(t.Context(), time.Now().Add(time.Second),
			func(context.Context) (ReconnectOwnerLease, error) { return ReconnectOwnerLease{}, nil })
		return operationErr == nil
	}, time.Second, time.Millisecond, "a completed underlying call releases its bounded slot")
}

func TestManagerReconnectOwnerOperationSlotValidationCannotPanicConstruction(t *testing.T) {
	credentials, err := NewReconnectCredentials([]byte(strings.Repeat("v", 32)), clock.Real{})
	require.NoError(t, err)
	require.NotPanics(t, func() {
		manager := NewManager(Config{
			IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
			ReconnectCredentials: credentials, ReconnectOwners: NewMemoryReconnectOwners(clock.Real{}), ReconnectOwnerID: "replica-invalid-slots",
			ReconnectCredentialTTL: 12 * time.Second, ReconnectCredentialOverlap: 2 * time.Second,
			ReconnectOwnerLeaseDuration: 3 * time.Second, ReconnectOwnerHeartbeatInterval: time.Second,
			ReconnectOwnerMaxOperations: -1, Clock: clock.Real{},
		}, testkit.NewFakeConnector(1), managerIDs{})
		require.ErrorIs(t, manager.validationErr, ErrInvalidManagerConfig)
	})
}

func (s *reconnectOwnersProbe) Tombstone(ctx context.Context, sessionID identity.SessionID, incarnation identity.Incarnation, ownerID string, epoch identity.ResponseEpoch, boundary bool) error {
	err := s.MemoryReconnectOwners.Tombstone(ctx, sessionID, incarnation, ownerID, epoch, boundary)
	if err != nil {
		return err
	}
	return s.tombstoneErr
}

type conversationProvisionerFake struct {
	mu     sync.Mutex
	scopes []handoff.Scope
	ttls   []time.Duration
	err    error
}

func (p *conversationProvisionerFake) ProvisionConversation(_ context.Context, scope handoff.Scope, ttl time.Duration) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.scopes = append(p.scopes, scope)
	p.ttls = append(p.ttls, ttl)
	return p.err
}

func (p *conversationProvisionerFake) snapshot() ([]handoff.Scope, []time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]handoff.Scope(nil), p.scopes...), append([]time.Duration(nil), p.ttls...)
}

type blockingRecoveryStore struct {
	snapshot RecoverySnapshot
	started  chan struct{}
	release  chan struct{}
	once     sync.Once
}

type quiescenceRefreshStore struct {
	preflight  RecoverySnapshot
	refreshed  RecoverySnapshot
	refreshErr error
	committed  atomic.Bool
	quiesced   <-chan struct{}
	calls      atomic.Int32
}

func (s *quiescenceRefreshStore) LoadRecovery(context.Context, handoff.Scope, int, int) (RecoverySnapshot, error) {
	switch s.calls.Add(1) {
	case 1:
		return cloneRecoverySnapshot(s.preflight), nil
	case 2:
		select {
		case <-s.quiesced:
		default:
			return RecoverySnapshot{}, errors.New("recovery refresh ran before old runtime quiesced")
		}
		if !s.committed.Load() {
			return RecoverySnapshot{}, errors.New("recovery refresh ran before durable commit")
		}
		if s.refreshErr != nil {
			return RecoverySnapshot{}, s.refreshErr
		}
		return cloneRecoverySnapshot(s.refreshed), nil
	default:
		return RecoverySnapshot{}, errors.New("unexpected recovery read")
	}
}

type commitOnCloseClient struct {
	*channelClient
	commit func()
	once   sync.Once
}

func (c *commitOnCloseClient) Close(ctx context.Context, code int, reason string) error {
	c.once.Do(c.commit)
	return c.channelClient.Close(ctx, code, reason)
}

type notifyingReadClient struct {
	*channelClient
	readObserved chan struct{}
	once         sync.Once
}

func (c *notifyingReadClient) Read(ctx context.Context) (protocol.ClientEvent, error) {
	event, err := c.channelClient.Read(ctx)
	if err == nil {
		c.once.Do(func() { close(c.readObserved) })
	}
	return event, err
}

type replayRegistrarFake struct {
	after int64
	sub   handoff.EventSubscription
}

type replayAttemptCallbacks struct {
	terminal sync.Once
	commit   func()
	rollback func()
}

func (a *replayAttemptCallbacks) Commit() {
	if a != nil {
		a.terminal.Do(func() {
			if a.commit != nil {
				a.commit()
			}
		})
	}
}

func (a *replayAttemptCallbacks) Rollback() {
	if a != nil {
		a.terminal.Do(func() {
			if a.rollback != nil {
				a.rollback()
			}
		})
	}
}

func (*replayRegistrarFake) Register(handoff.EventSubscription) error { return nil }
func (*replayRegistrarFake) Unregister(handoff.EventSubscription)     {}
func (r *replayRegistrarFake) RegisterReplay(sub handoff.EventSubscription, after int64) error {
	r.sub, r.after = sub, after
	return nil
}
func (r *replayRegistrarFake) BeginReplay(sub handoff.EventSubscription, after int64) (handoff.ReplayAttempt, error) {
	return &replayAttemptCallbacks{commit: func() { r.sub, r.after = sub, after }}, nil
}

func (s *blockingRecoveryStore) LoadRecovery(ctx context.Context, _ handoff.Scope, _, _ int) (RecoverySnapshot, error) {
	s.once.Do(func() { close(s.started) })
	select {
	case <-ctx.Done():
		return RecoverySnapshot{}, ctx.Err()
	case <-s.release:
		return cloneRecoverySnapshot(s.snapshot), nil
	}
}

type zeroJitter struct{}

func (zeroJitter) Duration(time.Duration) time.Duration { return 0 }

type failingReconnectConnector struct {
	calls atomic.Int32
}

type blockingApprovalDispatcher struct {
	started   chan struct{}
	release   chan struct{}
	completed chan struct{}
	once      sync.Once
	trusted   TrustedSessionContext
	request   ApprovalResolutionRequestedEffect
}

func (d *blockingApprovalDispatcher) DispatchApproval(trusted TrustedSessionContext, request ApprovalResolutionRequestedEffect) error {
	d.trusted, d.request = trusted, request
	d.once.Do(func() { close(d.started) })
	go func() {
		<-d.release
		close(d.completed)
	}()
	return nil
}

type contextBlockingCloseClient struct {
	created   chan struct{}
	closeCall chan struct{}
	once      sync.Once
}

func (c *contextBlockingCloseClient) Read(ctx context.Context) (protocol.ClientEvent, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (c *contextBlockingCloseClient) Write(_ context.Context, event protocol.ServerEvent) error {
	if event.EventType() == "session.created" {
		c.once.Do(func() { close(c.created) })
	}
	return nil
}

func (c *contextBlockingCloseClient) Close(ctx context.Context, _ int, _ string) error {
	select {
	case <-c.closeCall:
	default:
		close(c.closeCall)
	}
	<-ctx.Done()
	return ctx.Err()
}

type blockingProviderCloseVoice struct {
	closeStarted chan struct{}
	closeRelease chan struct{}
	once         sync.Once
}

func (*blockingProviderCloseVoice) Send(context.Context, realtime.Command) error { return nil }
func (*blockingProviderCloseVoice) Receive(ctx context.Context) (realtime.Event, error) {
	<-ctx.Done()
	return realtime.Event{}, ctx.Err()
}
func (v *blockingProviderCloseVoice) Close(context.Context) error {
	v.once.Do(func() { close(v.closeStarted) })
	<-v.closeRelease
	return nil
}

type blockingProviderConnector struct {
	voice *blockingProviderCloseVoice
	calls atomic.Int32
}

func (c *blockingProviderConnector) Connect(context.Context, realtime.OpenRequest) (realtime.Session, error) {
	c.calls.Add(1)
	return c.voice, nil
}

func (c *failingReconnectConnector) Connect(context.Context, realtime.OpenRequest) (realtime.Session, error) {
	c.calls.Add(1)
	return nil, errors.New("raw provider detail")
}

func (s *recoveryStoreFake) LoadRecovery(context.Context, handoff.Scope, int, int) (RecoverySnapshot, error) {
	s.calls.Add(1)
	return cloneRecoverySnapshot(s.snapshot), s.err
}

func TestManagerReconnectCASRestoresBeforePublishingAndRejectsReplay(t *testing.T) {
	connector := testkit.NewFakeConnector(8)
	scope := handoff.Scope{TenantID: "tenant-reconnect", UserID: "user-reconnect", ConversationID: "conversation-reconnect"}
	store := &recoveryStoreFake{snapshot: RecoverySnapshot{
		ConversationID: scope.ConversationID,
		CommittedItems: []CommittedItem{{Sequence: 1, Role: "user", Content: "durable history"}},
		PendingApprovals: []RecoveryApproval{{
			EventID: "approval-required-event", Sequence: 7,
			Approval: handoff.ToolApproval{
				InvocationID: "invocation-reconnect", HandoffID: "handoff-reconnect", ApprovalState: "pending",
				ActionSummary: "Confirm external action", RiskClass: handoff.ApprovalRiskSideEffecting,
				ExpiresAt: time.Now().Add(5 * time.Minute),
			},
		}},
		LastEventSeq: 7,
	}}
	provisioner := &conversationProvisionerFake{}
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
		RecoveryStore: store, RecoveryItemLimit: 8, RecoveryByteLimit: 1024,
		ConversationProvisioner: provisioner, ConversationTTL: 24 * time.Hour,
	}, connector, managerIDs{session: "logical-session", conversation: scope.ConversationID})

	oldCtx, oldCancel := context.WithCancel(context.Background())
	defer oldCancel()
	oldClient := newChannelClient(8)
	oldDone := make(chan error, 1)
	go func() {
		oldDone <- manager.Run(oldCtx, OpenRequest{Subject: Subject{
			TenantID: scope.TenantID, UserID: scope.UserID, ExpiresAt: time.Now().Add(time.Hour),
		}, ConversationID: scope.ConversationID}, oldClient)
	}()
	oldVoice, err := connector.NextSession(t.Context())
	require.NoError(t, err)
	require.Equal(t, "session.created", (<-oldClient.writes).EventType())
	waitForActiveRuntime(t, manager, "logical-session", 1)

	newCtx, newCancel := context.WithCancel(context.Background())
	defer newCancel()
	newClient := newChannelClient(8)
	newDone := make(chan error, 1)
	go func() {
		newDone <- manager.Reconnect(newCtx, ReconnectRequest{
			Scope: scope, SessionID: "logical-session", LastIncarnation: 1, LastEventSeq: 7,
		}, newClient)
	}()
	newVoice, err := connector.NextSession(t.Context())
	require.NoError(t, err)
	restored, err := newVoice.NextCommand(t.Context())
	require.NoError(t, err)
	require.Equal(t, realtime.CommandCreateItem, restored.Kind)
	require.Contains(t, string(restored.Item), "durable history")
	require.Equal(t, "session.created", (<-newClient.writes).EventType())
	replayedApproval, ok := (<-newClient.writes).(protocol.LiveToolApprovalRequiredEvent)
	require.True(t, ok)
	require.Equal(t, "approval-required-event", replayedApproval.EventID)
	require.Equal(t, int64(7), replayedApproval.SequenceID)
	require.Equal(t, "invocation-reconnect", replayedApproval.InvocationID)
	require.Eventually(t, func() bool {
		manager.activeMu.RLock()
		defer manager.activeMu.RUnlock()
		return manager.active[runtimeKey{sessionID: "logical-session", incarnation: 2}] != nil
	}, time.Second, time.Millisecond)
	require.ErrorIs(t, manager.Submit(t.Context(), "logical-session", 1, FailedInput{Source: "late", Err: errors.New("late")}), ErrSessionNotActive)
	require.ErrorIs(t, manager.Reconnect(t.Context(), ReconnectRequest{
		Scope: scope, SessionID: "logical-session", LastIncarnation: 1, LastEventSeq: 7,
	}, newChannelClient(1)), ErrStaleReconnect)
	require.Error(t, oldVoice.Emit(t.Context(), realtime.Event{Kind: realtime.EventResponseDone, Epoch: 1}), "old provider is closed before replacement publication")
	scopes, _ := provisioner.snapshot()
	require.Equal(t, []handoff.Scope{scope}, scopes, "reconnect reuses the already-provisioned exact owner conversation")

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
}

func TestManagerReconnectRestoresContentFreeActiveHandoffStateRegardlessOfCursor(t *testing.T) {
	connector := testkit.NewFakeConnector(16)
	scope := handoff.Scope{TenantID: "tenant-active-recovery", UserID: "user", ConversationID: "conversation-active-recovery"}
	secret := json.RawMessage(`{"credential":"must-not-cross-recovery"}`)
	store := &recoveryStoreFake{snapshot: RecoverySnapshot{
		ConversationID: scope.ConversationID,
		ActiveHandoffs: []RecoveryActiveHandoff{
			{EventID: "recovery-event-running", Snapshot: handoff.Snapshot{ID: "handoff-running", Request: handoff.CreateRequest{Scope: scope, Objective: "private objective", PresentationHint: "private hint"}, State: handoff.StateRunning, Input: secret}},
			{EventID: "recovery-event-needs-input", Snapshot: handoff.Snapshot{ID: "handoff-needs-input", Request: handoff.CreateRequest{Scope: scope, Objective: "another private objective"}, State: handoff.StateNeedsInput, Input: secret}},
		},
		LastEventSeq: 91,
	}}
	manager := NewManager(Config{IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second, RecoveryStore: store}, connector,
		managerIDs{session: "active-recovery-session", conversation: scope.ConversationID})
	oldCtx, oldCancel := context.WithCancel(context.Background())
	defer oldCancel()
	oldDone := make(chan error, 1)
	oldClient := newChannelClient(8)
	go func() {
		oldDone <- manager.Run(oldCtx, OpenRequest{Subject: Subject{TenantID: scope.TenantID, UserID: scope.UserID, ExpiresAt: time.Now().Add(time.Hour)}, ConversationID: scope.ConversationID}, oldClient)
	}()
	_, err := connector.NextSession(t.Context())
	require.NoError(t, err)
	require.Equal(t, "session.created", (<-oldClient.writes).EventType())
	waitForActiveRuntime(t, manager, "active-recovery-session", 1)

	newCtx, newCancel := context.WithCancel(context.Background())
	defer newCancel()
	newClient := newChannelClient(8)
	reconnectDone := make(chan error, 1)
	go func() {
		reconnectDone <- manager.Reconnect(newCtx, ReconnectRequest{Scope: scope, SessionID: "active-recovery-session", LastIncarnation: 1, LastEventSeq: 91}, newClient)
	}()
	voice, err := connector.NextSession(t.Context())
	require.NoError(t, err)
	for _, expected := range []string{"handoff-running", "handoff-needs-input"} {
		command, commandErr := voice.NextCommand(t.Context())
		require.NoError(t, commandErr)
		require.Equal(t, realtime.CommandCreateItem, command.Kind)
		require.Contains(t, string(command.Item), expected)
		require.NotContains(t, string(command.Item), "private objective")
		require.NotContains(t, string(command.Item), "private hint")
		require.NotContains(t, string(command.Item), "must-not-cross-recovery")
	}
	require.Equal(t, "session.created", (<-newClient.writes).EventType())
	progress, ok := (<-newClient.writes).(protocol.LiveHandoffProgressEvent)
	require.True(t, ok)
	require.Equal(t, "recovery-event-running", progress.EventID)
	require.Equal(t, "handoff-running", progress.HandoffID)
	require.Equal(t, int64(91), progress.SequenceID)
	needsInput, ok := (<-newClient.writes).(protocol.LiveHandoffNeedsInputEvent)
	require.True(t, ok)
	require.Equal(t, "recovery-event-needs-input", needsInput.EventID)
	require.Equal(t, "handoff-needs-input", needsInput.HandoffID)
	require.Equal(t, int64(91), needsInput.SequenceID)
	progressWire, err := protocol.Encode(progress)
	require.NoError(t, err, "recovery progress must traverse the production encoder")
	needsInputWire, err := protocol.Encode(needsInput)
	require.NoError(t, err, "recovery needs-input must traverse the production encoder")
	encoded, err := json.Marshal([]json.RawMessage{progressWire, needsInputWire})
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "private objective")
	require.NotContains(t, string(encoded), "must-not-cross-recovery")
	newCancel()
	require.Error(t, <-reconnectDone)
	require.Error(t, <-oldDone)
}

func TestManagerReconnectAbortsExactClaimAfterPostClaimRecoveryFailureAndCredentialRetries(t *testing.T) {
	manual := clock.NewManual(time.Now().UTC())
	credentials, err := NewReconnectCredentials([]byte(strings.Repeat("a", 32)), manual)
	require.NoError(t, err)
	owners := &reconnectOwnersProbe{MemoryReconnectOwners: NewMemoryReconnectOwners(manual)}
	recovery := &failNthRecoveryStore{failCall: 2}
	connector := testkit.NewFakeConnector(8)
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second, RecoveryStore: recovery,
		ReconnectCredentials: credentials, ReconnectOwners: owners, ReconnectOwnerID: "replica-a",
		ReconnectCredentialTTL: time.Minute, Clock: manual,
	}, connector, managerIDs{session: "abort-claim-session", conversation: "abort-claim-conversation"})
	require.NoError(t, manager.validationErr)
	subject := Subject{TenantID: "tenant-abort", UserID: "user-abort", ExpiresAt: manual.Now().Add(time.Hour)}
	_, ownerProbe, probeErr := manager.issueReconnectCredential(subject,
		handoff.Scope{TenantID: subject.TenantID, UserID: subject.UserID, ConversationID: "abort-claim-conversation"},
		"abort-claim-session", 1, 0, "provider-model", ReconnectOwnerEstablishing)
	require.NoError(t, probeErr)
	require.True(t, validReconnectOwner(ownerProbe))
	baseCtx, baseCancel := context.WithCancel(t.Context())
	baseClient := newChannelClient(8)
	baseDone := make(chan error, 1)
	go func() {
		baseDone <- manager.Run(baseCtx, OpenRequest{Subject: subject, RequestedModel: "provider-model"}, baseClient)
	}()
	_, err = connector.NextSession(t.Context())
	require.NoError(t, err)
	var createdEvent protocol.ServerEvent
	select {
	case createdEvent = <-baseClient.writes:
	case runErr := <-baseDone:
		require.NoError(t, runErr)
	}
	created, ok := createdEvent.(protocol.SessionCreatedEvent)
	require.True(t, ok)
	require.NotNil(t, created.Reconnect)
	claims, err := credentials.Verify(created.Reconnect.Credential)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		owner, found := owners.Read(claims.SessionID)
		return found && owner.State == ReconnectOwnerActive
	}, time.Second, time.Millisecond, "the base owner must publish before this test exercises tombstone/claim rollback")
	baseCancel()
	require.Error(t, <-baseDone)
	require.Eventually(t, func() bool {
		owner, found := owners.Read(claims.SessionID)
		return found && owner.State == ReconnectOwnerTombstoned
	}, time.Second, time.Millisecond)

	request := ReconnectRequest{Scope: claims.Scope, SessionID: claims.SessionID, LastIncarnation: claims.Incarnation,
		LastEventSeq: claims.Cursor, Subject: subject, Credential: claims}
	err = manager.Reconnect(t.Context(), request, newChannelClient(4))
	require.ErrorIs(t, err, ErrRecoveryUnavailable)
	require.Equal(t, int32(1), owners.aborts.Load())
	restored, found := owners.Read(claims.SessionID)
	require.True(t, found)
	require.Equal(t, claims.CredentialID, restored.CredentialID)
	require.Equal(t, ReconnectOwnerTombstoned, restored.State)

	retryCtx, retryCancel := context.WithCancel(t.Context())
	retryClient := newChannelClient(8)
	retryDone := make(chan error, 1)
	go func() { retryDone <- manager.Reconnect(retryCtx, request, retryClient) }()
	_, err = connector.NextSession(t.Context())
	require.NoError(t, err)
	reconnected, ok := (<-retryClient.writes).(protocol.SessionCreatedEvent)
	require.True(t, ok)
	require.Equal(t, uint64(2), reconnected.Reconnect.Incarnation)
	retryCancel()
	require.Error(t, <-retryDone)
}

func TestManagerReconnectPublishFailureRollsBackLocalPublicationAndRetainsAdvertisedStage(t *testing.T) {
	manual := clock.NewManual(time.Now().UTC())
	credentials, err := NewReconnectCredentials([]byte(strings.Repeat("p", 32)), manual)
	require.NoError(t, err)
	owners := &reconnectOwnersProbe{MemoryReconnectOwners: NewMemoryReconnectOwners(manual), failPublishCall: 2}
	connector := testkit.NewFakeConnector(8)
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
		RecoveryStore:        &recoveryStoreFake{snapshot: RecoverySnapshot{ConversationID: "publish-fail-conversation"}},
		ReconnectCredentials: credentials, ReconnectOwners: owners, ReconnectOwnerID: "replica-publish",
		ReconnectCredentialTTL: time.Minute, Clock: manual,
	}, connector, managerIDs{session: "publish-fail-session", conversation: "publish-fail-conversation"})
	require.NoError(t, manager.validationErr)
	subject := Subject{TenantID: "tenant-publish", UserID: "user-publish", ExpiresAt: manual.Now().Add(time.Hour)}
	baseCtx, baseCancel := context.WithCancel(t.Context())
	baseClient := newChannelClient(8)
	baseDone := make(chan error, 1)
	go func() {
		baseDone <- manager.Run(baseCtx, OpenRequest{Subject: subject, RequestedModel: "provider-model"}, baseClient)
	}()
	_, err = connector.NextSession(t.Context())
	require.NoError(t, err)
	created := (<-baseClient.writes).(protocol.SessionCreatedEvent)
	claims, err := credentials.Verify(created.Reconnect.Credential)
	require.NoError(t, err)
	waitForActiveRuntime(t, manager, claims.SessionID, claims.Incarnation)
	baseCancel()
	require.Error(t, <-baseDone)

	reconnectClient := newChannelClient(8)
	err = manager.Reconnect(t.Context(), ReconnectRequest{Scope: claims.Scope, SessionID: claims.SessionID,
		LastIncarnation: claims.Incarnation, LastEventSeq: claims.Cursor, Subject: subject, Credential: claims}, reconnectClient)
	require.ErrorIs(t, err, ErrRecoveryUnavailable)
	advertised := (<-reconnectClient.writes).(protocol.SessionCreatedEvent)
	advertisedClaims, err := credentials.Verify(advertised.Reconnect.Credential)
	require.NoError(t, err)
	require.Equal(t, int32(0), owners.aborts.Load(), "an advertised stage must not be rolled back to an incompatible durable key")
	staged, ok := owners.Read(claims.SessionID)
	require.True(t, ok)
	require.Equal(t, advertisedClaims.CredentialID, staged.CredentialID)
	require.Equal(t, advertisedClaims.Incarnation, staged.Incarnation)
	require.Equal(t, claims.CredentialID, staged.PreviousCredentialID)
	require.Equal(t, claims.Incarnation, staged.PreviousIncarnation)
	require.Equal(t, ReconnectOwnerClaiming, staged.State)
	manager.activeMu.RLock()
	logical := manager.logical[claims.SessionID]
	if logical != nil {
		require.Equal(t, claims.Incarnation, logical.highWater)
		require.Nil(t, logical.runtime)
		require.False(t, logical.reconnecting)
	}
	manager.activeMu.RUnlock()

	manual.Advance(staged.OwnerLeaseExpiresAt.Sub(manual.Now()))
	retryCtx, retryCancel := context.WithCancel(t.Context())
	retryClient := newChannelClient(8)
	retryDone := make(chan error, 1)
	go func() {
		retryDone <- manager.Reconnect(retryCtx, ReconnectRequest{Scope: claims.Scope, SessionID: claims.SessionID,
			LastIncarnation: claims.Incarnation, LastEventSeq: claims.Cursor, Subject: subject, Credential: claims}, retryClient)
	}()
	_, err = connector.NextSession(t.Context())
	require.NoError(t, err)
	var retried protocol.SessionCreatedEvent
	select {
	case event := <-retryClient.writes:
		retried = event.(protocol.SessionCreatedEvent)
	case retryErr := <-retryDone:
		require.NoError(t, retryErr)
	}
	require.Equal(t, advertised.Reconnect.Credential, retried.Reconnect.Credential,
		"retrying N after an ambiguous publish must re-advertise the same durable N+1 credential")
	require.Eventually(t, func() bool {
		owner, found := owners.Read(claims.SessionID)
		return found && owner.State == ReconnectOwnerActive && owner.CredentialID == advertisedClaims.CredentialID
	}, time.Second, time.Millisecond)
	retryCancel()
	require.Error(t, <-retryDone)
}

func TestManagerRemoteReconnectWithoutLocalLogicalIsNilSafe(t *testing.T) {
	type reconnectOutcome struct {
		err      error
		panicked any
	}
	for _, test := range []struct {
		name       string
		suffix     string
		cancelGate bool
	}{
		{name: "normal", suffix: "normal"},
		{name: "cancelled at boundary gate", suffix: "cancelled", cancelGate: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			credentials, err := NewReconnectCredentials([]byte(strings.Repeat("n", 32)), clock.Real{})
			require.NoError(t, err)
			owners := NewMemoryReconnectOwners(clock.Real{})
			scope := handoff.Scope{TenantID: "tenant-remote-nil", UserID: "user-remote-nil", ConversationID: identity.ConversationID("conversation-remote-nil-" + test.suffix)}
			subject := Subject{TenantID: scope.TenantID, UserID: scope.UserID, ExpiresAt: time.Now().Add(time.Hour)}
			baseConnector := testkit.NewFakeConnector(2)
			baseManager := NewManager(Config{
				IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
				RecoveryStore:        &recoveryStoreFake{snapshot: RecoverySnapshot{ConversationID: scope.ConversationID}},
				ReconnectCredentials: credentials, ReconnectOwners: owners, ReconnectOwnerID: "remote-nil-origin",
				ReconnectCredentialTTL: time.Minute,
			}, baseConnector, managerIDs{session: identity.SessionID("remote-nil-session-" + test.suffix), conversation: scope.ConversationID})
			require.NoError(t, baseManager.validationErr)
			baseCtx, baseCancel := context.WithCancel(t.Context())
			baseClient := newChannelClient(8)
			baseDone := make(chan error, 1)
			go func() {
				baseDone <- baseManager.Run(baseCtx, OpenRequest{Subject: subject, RequestedModel: "provider-model", ConversationID: scope.ConversationID}, baseClient)
			}()
			_, err = baseConnector.NextSession(t.Context())
			require.NoError(t, err)
			created := (<-baseClient.writes).(protocol.SessionCreatedEvent)
			require.NotNil(t, created.Reconnect)
			claims, err := credentials.Verify(created.Reconnect.Credential)
			require.NoError(t, err)
			require.Eventually(t, func() bool {
				owner, found := owners.Read(claims.SessionID)
				return found && owner.State == ReconnectOwnerActive
			}, time.Second, time.Millisecond)
			baseCancel()
			require.Error(t, <-baseDone)
			require.Eventually(t, func() bool {
				owner, found := owners.Read(claims.SessionID)
				return found && owner.State == ReconnectOwnerTombstoned
			}, time.Second, time.Millisecond)

			boundaryEntered := make(chan struct{})
			boundaryRelease := make(chan struct{})
			var boundaryOnce sync.Once
			replicaConnector := testkit.NewFakeConnector(2)
			replicaManager := NewManager(Config{
				IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
				RecoveryStore:        &recoveryStoreFake{snapshot: RecoverySnapshot{ConversationID: scope.ConversationID}},
				ReconnectCredentials: credentials, ReconnectOwners: owners, ReconnectOwnerID: "remote-nil-replica",
				ReconnectCredentialTTL: time.Minute,
				reconnectBoundaryGate: func() <-chan struct{} {
					if !test.cancelGate {
						return nil
					}
					boundaryOnce.Do(func() { close(boundaryEntered) })
					return boundaryRelease
				},
			}, replicaConnector, managerIDs{})
			require.NoError(t, replicaManager.validationErr)
			replicaManager.activeMu.RLock()
			require.Nil(t, replicaManager.logical[claims.SessionID], "the reconnecting replica must begin without a local logical cache entry")
			replicaManager.activeMu.RUnlock()

			reconnectCtx, reconnectCancel := context.WithCancel(t.Context())
			defer reconnectCancel()
			outcome := make(chan reconnectOutcome, 1)
			go func() {
				result := reconnectOutcome{}
				defer func() {
					result.panicked = recover()
					outcome <- result
				}()
				result.err = replicaManager.Reconnect(reconnectCtx, ReconnectRequest{
					Scope: claims.Scope, SessionID: claims.SessionID, LastIncarnation: claims.Incarnation,
					LastEventSeq: claims.Cursor, Subject: subject, Credential: claims,
				}, newChannelClient(8))
			}()

			if test.cancelGate {
				select {
				case <-boundaryEntered:
				case result := <-outcome:
					require.Nil(t, result.panicked, "remote reconnect panicked before the cancellation gate")
					t.Fatalf("remote reconnect returned before the cancellation gate: %v", result.err)
				case <-time.After(time.Second):
					t.Fatal("remote reconnect did not reach the cancellation gate")
				}
				reconnectCancel()
				result := <-outcome
				require.Nil(t, result.panicked, "cancelling an authoritative remote reconnect must not dereference a nil logical cache entry")
				require.ErrorIs(t, result.err, context.Canceled)
				return
			}

			connected := make(chan error, 1)
			go func() {
				_, connectErr := replicaConnector.NextSession(t.Context())
				connected <- connectErr
			}()
			select {
			case result := <-outcome:
				require.Nil(t, result.panicked, "authoritative remote reconnect must not dereference a nil logical cache entry")
				t.Fatalf("remote reconnect returned before opening the replacement provider: %v", result.err)
			case connectErr := <-connected:
				require.NoError(t, connectErr)
			case <-time.After(time.Second):
				t.Fatal("remote reconnect neither opened the replacement provider nor returned")
			}
			reconnectCancel()
			result := <-outcome
			require.Nil(t, result.panicked, "authoritative remote reconnect must remain nil-safe through shutdown")
			require.Error(t, result.err)
		})
	}
}

func TestManagerReconnectHeartbeatLossFencesRuntimeAndTombstoneFailureIsReturned(t *testing.T) {
	manual := clock.NewManual(time.Now().UTC())
	credentials, err := NewReconnectCredentials([]byte(strings.Repeat("b", 32)), manual)
	require.NoError(t, err)
	owners := &reconnectOwnersProbe{MemoryReconnectOwners: NewMemoryReconnectOwners(manual)}
	connector := testkit.NewFakeConnector(4)
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
		ReconnectCredentials: credentials, ReconnectOwners: owners, ReconnectOwnerID: "replica-heartbeat",
		ReconnectCredentialTTL: 12 * time.Second, ReconnectCredentialOverlap: 2 * time.Second,
		ReconnectOwnerLeaseDuration: 6 * time.Second, ReconnectOwnerHeartbeatInterval: time.Second, Clock: manual,
	}, connector, managerIDs{session: "heartbeat-session", conversation: "heartbeat-conversation"})
	require.NoError(t, manager.validationErr)
	client := newChannelClient(8)
	done := make(chan error, 1)
	go func() {
		done <- manager.Run(t.Context(), OpenRequest{Subject: Subject{TenantID: "tenant-heartbeat", UserID: "user-heartbeat", ExpiresAt: manual.Now().Add(time.Hour)}, RequestedModel: "provider-model"}, client)
	}()
	_, err = connector.NextSession(t.Context())
	require.NoError(t, err)
	created := (<-client.writes).(protocol.SessionCreatedEvent)
	require.Eventually(t, func() bool {
		owner, ok := owners.Read(identity.SessionID(created.Reconnect.SessionID))
		return ok && owner.State == ReconnectOwnerActive
	}, time.Second, time.Millisecond)
	owners.heartbeatErr.Store(true)
	require.Eventually(t, func() bool {
		manual.Advance(time.Second)
		select {
		case err = <-done:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
	require.ErrorIs(t, err, ErrRecoveryUnavailable)

	owners = &reconnectOwnersProbe{MemoryReconnectOwners: NewMemoryReconnectOwners(manual), tombstoneErr: errors.New("tombstone unavailable")}
	manager = NewManager(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
		ReconnectCredentials: credentials, ReconnectOwners: owners, ReconnectOwnerID: "replica-tombstone",
		ReconnectCredentialTTL: time.Minute, Clock: manual,
	}, connector, managerIDs{session: "tombstone-session", conversation: "tombstone-conversation"})
	require.NoError(t, manager.validationErr)
	cancelCtx, cancel := context.WithCancel(t.Context())
	done = make(chan error, 1)
	tombstoneClient := newChannelClient(4)
	go func() {
		done <- manager.Run(cancelCtx, OpenRequest{Subject: Subject{TenantID: "tenant-tombstone", UserID: "user-tombstone", ExpiresAt: manual.Now().Add(time.Hour)}, RequestedModel: "provider-model"}, tombstoneClient)
	}()
	_, err = connector.NextSession(t.Context())
	require.NoError(t, err)
	created = (<-tombstoneClient.writes).(protocol.SessionCreatedEvent)
	require.Eventually(t, func() bool {
		owner, ok := owners.Read(identity.SessionID(created.Reconnect.SessionID))
		return ok && owner.State == ReconnectOwnerActive
	}, time.Second, time.Millisecond)
	cancel()
	err = <-done
	require.ErrorContains(t, err, "tombstone unavailable")
}

func TestManagerReconnectCredentialRefreshSurvivesTTLAndAdvancesRepeatedNonzeroCursor(t *testing.T) {
	manual := clock.NewManual(time.Now().UTC())
	credentials, err := NewReconnectCredentials([]byte(strings.Repeat("c", 32)), manual)
	require.NoError(t, err)
	owners := NewMemoryReconnectOwners(manual)
	connector := testkit.NewFakeConnector(4)
	heartbeatReady := make(chan struct{})
	var heartbeatReadyOnce sync.Once
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
		ReconnectCredentials: credentials, ReconnectOwners: owners, ReconnectOwnerID: "replica-refresh",
		ReconnectCredentialTTL: 12 * time.Second, ReconnectCredentialOverlap: 2 * time.Second,
		ReconnectOwnerLeaseDuration: 6 * time.Second, ReconnectOwnerHeartbeatInterval: time.Second, Clock: manual,
		reconnectHeartbeatReady: func() { heartbeatReadyOnce.Do(func() { close(heartbeatReady) }) },
	}, connector, managerIDs{session: "refresh-token-session", conversation: "refresh-token-conversation"})
	require.NoError(t, manager.validationErr)
	ctx, cancel := context.WithCancel(t.Context())
	client := newChannelClient(16)
	done := make(chan error, 1)
	go func() {
		done <- manager.Run(ctx, OpenRequest{Subject: Subject{TenantID: "tenant-refresh", UserID: "user-refresh", ExpiresAt: manual.Now().Add(time.Hour)}, RequestedModel: "provider-model"}, client)
	}()
	_, err = connector.NextSession(t.Context())
	require.NoError(t, err)
	created := (<-client.writes).(protocol.SessionCreatedEvent)
	runtime := waitForActiveRuntime(t, manager, "refresh-token-session", 1)
	require.Eventually(t, func() bool {
		owner, ok := owners.Read("refresh-token-session")
		return ok && owner.State == ReconnectOwnerActive
	}, time.Second, time.Millisecond)
	<-heartbeatReady
	waitForLeaseRefresh := func() {
		t.Helper()
		deadline := time.NewTimer(time.Second)
		defer deadline.Stop()
		poll := time.NewTicker(time.Millisecond)
		defer poll.Stop()
		for {
			owner, ok := owners.Read("refresh-token-session")
			if ok && owner.OwnerLeaseExpiresAt.After(manual.Now().Add(4*time.Second)) {
				return
			}
			select {
			case runtimeErr := <-done:
				t.Fatalf("reconnect owner maintainer stopped before lease refresh: %v", runtimeErr)
			case <-deadline.C:
				t.Fatal("reconnect owner lease did not refresh")
			case <-poll.C:
			}
		}
	}

	for range 8 {
		manual.Advance(time.Second)
		waitForLeaseRefresh()
	}
	periodic, ok := (<-client.writes).(protocol.SessionReconnectUpdatedEvent)
	require.True(t, ok)
	require.Equal(t, int64(0), periodic.Reconnect.Cursor)
	for range 5 {
		manual.Advance(time.Second)
		waitForLeaseRefresh()
	}
	_, err = credentials.Verify(created.Reconnect.Credential)
	require.ErrorIs(t, err, ErrExpiredReconnectCredential)
	_, err = credentials.Verify(periodic.Reconnect.Credential)
	require.NoError(t, err, "periodic refresh keeps the live session reconnectable beyond the original TTL")

	require.NoError(t, manager.rotateReconnectCredential(t.Context(), runtime, 7, false))
	firstCursor := (<-client.writes).(protocol.SessionReconnectUpdatedEvent)
	require.Equal(t, int64(7), firstCursor.Reconnect.Cursor)
	require.NoError(t, manager.rotateReconnectCredential(t.Context(), runtime, 19, false))
	secondCursor := (<-client.writes).(protocol.SessionReconnectUpdatedEvent)
	require.Equal(t, int64(19), secondCursor.Reconnect.Cursor)
	owner, ok := owners.Read("refresh-token-session")
	require.True(t, ok)
	require.Equal(t, int64(19), owner.Cursor)
	require.Equal(t, int64(7), owner.PreviousCursor)
	require.Equal(t, "lrt1", firstCursor.Reconnect.Credential[:4])
	require.ErrorIs(t, manager.rotateReconnectCredential(t.Context(), runtime, 18, false), ErrStaleReconnect)
	cancel()
	require.Error(t, <-done)
}

func TestManagerReconnectMaintenanceGloballyCleansExpiredRowsAndBecomesReady(t *testing.T) {
	manual := clock.NewManual(time.Now().UTC())
	owners := NewMemoryReconnectOwners(manual)
	insert := func(sessionID, ownerID string, active bool) {
		owner := testReconnectOwner(manual.Now(), ownerID, "credential-"+sessionID, 1, 0)
		owner.SessionID = identity.SessionID(sessionID)
		owner.Scope.ConversationID = identity.ConversationID("conversation-" + sessionID)
		owner.ExpiresAt = manual.Now().Add(2 * time.Second)
		_, err := owners.Create(t.Context(), owner, 2*time.Second)
		require.NoError(t, err)
		_, err = owners.Publish(t.Context(), owner.Key(), 1, true, 2*time.Second)
		require.NoError(t, err)
		if !active {
			require.NoError(t, owners.Tombstone(t.Context(), owner.SessionID, owner.Incarnation, owner.OwnerID, 1, true))
		}
	}
	insert("owned-expired", "replica-cleaner", false)
	insert("owned-active", "replica-cleaner", true)
	insert("foreign-expired", "replica-foreign", false)
	manual.Advance(3 * time.Second)
	credentials, err := NewReconnectCredentials([]byte(strings.Repeat("m", 32)), manual)
	require.NoError(t, err)
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
		ReconnectCredentials: credentials, ReconnectOwners: owners, ReconnectOwnerID: "replica-cleaner",
		ReconnectCredentialTTL: 12 * time.Second, ReconnectCredentialOverlap: 2 * time.Second,
		ReconnectOwnerLeaseDuration: 6 * time.Second, ReconnectOwnerHeartbeatInterval: time.Second,
		ReconnectCleanupInterval: time.Second, ReconnectCleanupBatch: 1, Clock: manual,
	}, testkit.NewFakeConnector(1), managerIDs{session: "unused", conversation: "unused"})
	require.NoError(t, manager.validationErr)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- manager.RunReconnectMaintenance(ctx) }()
	require.Eventually(t, func() bool { return manager.ReconnectReady(t.Context()) == nil }, time.Second, time.Millisecond)
	manual.Advance(time.Second)
	manual.Advance(time.Second)
	require.Eventually(t, func() bool {
		_, ownedExpired := owners.Read("owned-expired")
		_, ownedActive := owners.Read("owned-active")
		_, foreignExpired := owners.Read("foreign-expired")
		return !ownedExpired && !ownedActive && !foreignExpired
	}, time.Second, time.Millisecond)
	_, ownedExpired := owners.Read("owned-expired")
	_, ownedActive := owners.Read("owned-active")
	_, foreignExpired := owners.Read("foreign-expired")
	require.False(t, ownedExpired)
	require.False(t, ownedActive)
	require.False(t, foreignExpired)
	cancel()
	require.NoError(t, <-done)
}

func TestManagerReconnectCleanupIsOperationBoundedAndReadinessExpiresWithoutFreshSuccess(t *testing.T) {
	manual := clock.NewManual(time.Now().UTC())
	owners := &blockingCleanupOwners{MemoryReconnectOwners: NewMemoryReconnectOwners(manual),
		started: make(chan struct{}), release: make(chan struct{})}
	credentials, err := NewReconnectCredentials([]byte(strings.Repeat("z", 32)), manual)
	require.NoError(t, err)
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
		ReconnectCredentials: credentials, ReconnectOwners: owners, ReconnectOwnerID: "replica-cleanup-bound",
		ReconnectCredentialTTL: 12 * time.Second, ReconnectCredentialOverlap: 2 * time.Second,
		ReconnectOwnerLeaseDuration: 3 * time.Second, ReconnectOwnerHeartbeatInterval: time.Second,
		ReconnectOwnerOperationTimeout: 25 * time.Millisecond,
		ReconnectCleanupInterval:       time.Second, ReconnectCleanupBatch: 8, Clock: manual,
	}, testkit.NewFakeConnector(1), managerIDs{})
	require.NoError(t, manager.validationErr)

	started := time.Now()
	manager.sweepReconnectOwners(t.Context())
	require.Less(t, time.Since(started), 250*time.Millisecond, "a cleanup store that ignores context cannot block lifecycle readiness")
	<-owners.started
	require.Error(t, manager.ReconnectReady(t.Context()))
	close(owners.release)
	manager.sweepReconnectOwners(t.Context())
	require.NoError(t, manager.ReconnectReady(t.Context()))
	manual.Advance(3 * time.Second)
	require.ErrorContains(t, manager.ReconnectReady(t.Context()), "stale")
}

func TestManagerReconnectEpochUsesQuiescedLiveFenceHighWaterAndStaysMonotonic(t *testing.T) {
	connector := testkit.NewFakeConnector(16)
	scope := handoff.Scope{TenantID: "tenant-epoch", UserID: "user", ConversationID: "conversation-epoch"}
	manager := NewManager(Config{IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
		RecoveryStore: &recoveryStoreFake{snapshot: RecoverySnapshot{ConversationID: scope.ConversationID}}}, connector,
		managerIDs{session: "epoch-session", conversation: scope.ConversationID})
	oldCtx, oldCancel := context.WithCancel(context.Background())
	defer oldCancel()
	oldDone := make(chan error, 1)
	oldClient := newChannelClient(4)
	go func() {
		oldDone <- manager.Run(oldCtx, OpenRequest{Subject: Subject{TenantID: scope.TenantID, UserID: scope.UserID, ExpiresAt: time.Now().Add(time.Hour)}, ConversationID: scope.ConversationID}, oldClient)
	}()
	_, err := connector.NextSession(t.Context())
	require.NoError(t, err)
	<-oldClient.writes
	waitForActiveRuntime(t, manager, "epoch-session", 1)
	manager.activeMu.RLock()
	oldRuntime := manager.active[runtimeKey{sessionID: "epoch-session", incarnation: 1}]
	manager.activeMu.RUnlock()
	for oldRuntime.state.fence.Current() < 41 {
		oldRuntime.state.fence.Advance()
	}
	manager.activeMu.RLock()
	require.Zero(t, manager.logical["epoch-session"].responseEpoch, "cached epoch deliberately trails the live fence")
	manager.activeMu.RUnlock()

	firstCtx, firstCancel := context.WithCancel(context.Background())
	firstClient := newChannelClient(4)
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- manager.Reconnect(firstCtx, ReconnectRequest{Scope: scope, SessionID: "epoch-session", LastIncarnation: 1}, firstClient)
	}()
	_, err = connector.NextSession(t.Context())
	require.NoError(t, err)
	<-firstClient.writes
	waitForActiveRuntime(t, manager, "epoch-session", 2)
	manager.activeMu.RLock()
	firstRuntime := manager.active[runtimeKey{sessionID: "epoch-session", incarnation: 2}]
	require.Equal(t, identity.ResponseEpoch(42), firstRuntime.state.fence.Current())
	manager.activeMu.RUnlock()
	firstCancel()
	require.Error(t, <-firstDone)

	secondCtx, secondCancel := context.WithCancel(context.Background())
	defer secondCancel()
	secondClient := newChannelClient(4)
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- manager.Reconnect(secondCtx, ReconnectRequest{Scope: scope, SessionID: "epoch-session", LastIncarnation: 2}, secondClient)
	}()
	_, err = connector.NextSession(t.Context())
	require.NoError(t, err)
	<-secondClient.writes
	waitForActiveRuntime(t, manager, "epoch-session", 3)
	manager.activeMu.RLock()
	require.Equal(t, identity.ResponseEpoch(43), manager.active[runtimeKey{sessionID: "epoch-session", incarnation: 3}].state.fence.Current())
	manager.activeMu.RUnlock()
	secondCancel()
	require.Error(t, <-secondDone)
	require.Error(t, <-oldDone)
}

func TestManagerTransfersAcceptedApprovalBeyondSessionDisconnect(t *testing.T) {
	connector := testkit.NewFakeConnector(8)
	scope := handoff.Scope{TenantID: "tenant-approval-boundary", UserID: "user", ConversationID: "conversation-approval-boundary"}
	dispatcher := &blockingApprovalDispatcher{started: make(chan struct{}), release: make(chan struct{}), completed: make(chan struct{})}
	manager := NewManager(Config{IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
		RecoveryStore: &recoveryStoreFake{snapshot: RecoverySnapshot{ConversationID: scope.ConversationID}}, ApprovalDispatcher: dispatcher}, connector,
		managerIDs{session: "approval-boundary-session", conversation: scope.ConversationID})
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := newChannelClient(8)
	runDone := make(chan error, 1)
	go func() {
		runDone <- manager.Run(runCtx, OpenRequest{Subject: Subject{TenantID: scope.TenantID, UserID: scope.UserID, ExpiresAt: time.Now().Add(time.Hour)}, ConversationID: scope.ConversationID}, client)
	}()
	_, err := connector.NextSession(t.Context())
	require.NoError(t, err)
	<-client.writes
	waitForActiveRuntime(t, manager, "approval-boundary-session", 1)
	client.reads <- protocol.LiveToolApprovalResolveEvent{Type: "live.tool.approval.resolve", InvocationID: "invocation", Decision: "approved", Reason: "confirmed", IdempotencyKey: "approval-key"}
	select {
	case <-dispatcher.started:
	case <-time.After(time.Second):
		t.Fatal("approval dispatcher did not accept durable resolution")
	}
	require.Equal(t, scope.TenantID, dispatcher.trusted.Subject.TenantID)
	require.Equal(t, scope.UserID, dispatcher.trusted.Subject.UserID)
	require.Equal(t, scope.ConversationID, dispatcher.trusted.IDs.ConversationID)
	require.Equal(t, "invocation", dispatcher.request.InvocationID)
	require.Equal(t, handoff.ApprovalApproved, dispatcher.request.Decision)
	require.Equal(t, "approval-key", dispatcher.request.IdempotencyKey)
	require.False(t, dispatcher.request.ResolvedAt.IsZero())
	require.ErrorIs(t, manager.Submit(t.Context(), "approval-boundary-session", 2, ApprovalResolutionCompletedInput{
		ExternalSequence: dispatcher.request.ExternalSequence, InvocationID: "invocation", Accepted: true,
	}), ErrSessionNotActive)
	manager.activeMu.RLock()
	runtime := manager.active[runtimeKey{sessionID: "approval-boundary-session", incarnation: 1}]
	manager.activeMu.RUnlock()
	require.NotNil(t, runtime)
	require.Equal(t, runtime.state.externalAccepted.Load(), runtime.state.externalApplied.Load(),
		"app-owned queue admission must complete the session boundary before durable resolution")
	require.NoError(t, manager.Submit(t.Context(), "approval-boundary-session", 1, ApprovalResolutionCompletedInput{
		ExternalSequence: dispatcher.request.ExternalSequence, InvocationID: "invocation", Accepted: true,
	}))
	require.Eventually(t, func() bool {
		return runtime.state.externalApplied.Load() == runtime.state.externalAccepted.Load()
	}, time.Second, time.Millisecond)
	cancel()
	require.Error(t, <-runDone)
	close(dispatcher.release)
	select {
	case <-dispatcher.completed:
	case <-time.After(time.Second):
		t.Fatal("app-owned approval work did not survive session disconnect")
	}
}

func TestManagerRegistersReservedToolsBeforeFreshAndReconnectedProviderContext(t *testing.T) {
	connector := testkit.NewFakeConnector(16)
	scope := handoff.Scope{TenantID: "tenant-tools", UserID: "user-tools", ConversationID: "conversation-tools"}
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
		RecoveryStore: &recoveryStoreFake{snapshot: RecoverySnapshot{
			ConversationID: scope.ConversationID,
			CommittedItems: []CommittedItem{{Sequence: 1, Role: "user", Content: "safe committed context"}},
		}},
		InitialSession: ReservedHandoffSessionConfig(), HandoffToolsEnabled: true,
	}, connector, managerIDs{session: "tools-session", conversation: scope.ConversationID})

	oldCtx, oldCancel := context.WithCancel(t.Context())
	oldClient := newChannelClient(8)
	oldDone := make(chan error, 1)
	go func() {
		oldDone <- manager.Run(oldCtx, OpenRequest{Subject: Subject{
			TenantID: scope.TenantID, UserID: scope.UserID, ExpiresAt: time.Now().Add(time.Hour),
		}, ConversationID: scope.ConversationID}, oldClient)
	}()
	oldVoice, err := connector.NextSession(t.Context())
	require.NoError(t, err)
	freshRegistration, err := oldVoice.NextCommand(t.Context())
	require.NoError(t, err)
	require.Equal(t, realtime.CommandSessionUpdate, freshRegistration.Kind)
	require.Len(t, freshRegistration.Session.Tools, 3)
	require.Equal(t, "session.created", (<-oldClient.writes).EventType())
	waitForActiveRuntime(t, manager, "tools-session", 1)
	oldCancel()
	require.Error(t, <-oldDone)

	newCtx, newCancel := context.WithCancel(t.Context())
	newClient := newChannelClient(8)
	reconnectDone := make(chan error, 1)
	go func() {
		reconnectDone <- manager.Reconnect(newCtx, ReconnectRequest{
			Scope: scope, SessionID: "tools-session", LastIncarnation: 1,
		}, newClient)
	}()
	newVoice, err := connector.NextSession(t.Context())
	require.NoError(t, err)
	reconnectedRegistration, err := newVoice.NextCommand(t.Context())
	require.NoError(t, err)
	require.Equal(t, realtime.CommandSessionUpdate, reconnectedRegistration.Kind)
	require.Len(t, reconnectedRegistration.Session.Tools, 3)
	restored, err := newVoice.NextCommand(t.Context())
	require.NoError(t, err)
	require.Equal(t, realtime.CommandCreateItem, restored.Kind)
	newCancel()
	require.Error(t, <-reconnectDone)
}

func TestManagerReconnectFutureCursorDoesNotDisturbLiveRuntime(t *testing.T) {
	connector := testkit.NewFakeConnector(4)
	scope := handoff.Scope{TenantID: "tenant-future", UserID: "user", ConversationID: "conversation-future"}
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
		RecoveryStore: &recoveryStoreFake{snapshot: RecoverySnapshot{ConversationID: scope.ConversationID, LastEventSeq: 4}},
	}, connector, managerIDs{session: "future-session", conversation: scope.ConversationID})
	ctx, cancel := context.WithCancel(t.Context())
	client := newChannelClient(8)
	done := make(chan error, 1)
	go func() {
		done <- manager.Run(ctx, OpenRequest{Subject: Subject{
			TenantID: scope.TenantID, UserID: scope.UserID, ExpiresAt: time.Now().Add(time.Hour),
		}, ConversationID: scope.ConversationID}, client)
	}()
	voice, err := connector.NextSession(t.Context())
	require.NoError(t, err)
	require.Equal(t, "session.created", (<-client.writes).EventType())
	waitForActiveRuntime(t, manager, "future-session", 1)

	err = manager.Reconnect(t.Context(), ReconnectRequest{
		Scope: scope, SessionID: "future-session", LastIncarnation: 1, LastEventSeq: 5,
	}, newChannelClient(1))
	require.ErrorIs(t, err, ErrStaleReconnect)
	require.NoError(t, manager.Submit(t.Context(), "future-session", 1, ClientInput{Event: inputTurnSessionUpdate(json.RawMessage(`null`))}))
	command, err := voice.NextCommand(t.Context())
	require.NoError(t, err)
	require.Equal(t, realtime.CommandSessionUpdate, command.Kind)
	select {
	case <-client.done:
		t.Fatal("future cursor closed the valid live client")
	default:
	}
	secondCtx, secondCancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer secondCancel()
	_, err = connector.NextSession(secondCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	cancel()
	<-done
}

func TestManagerReconnectRejectsClientVisibleInitialEstablishment(t *testing.T) {
	registerReached := make(chan struct{})
	releaseRegister := make(chan struct{})
	var reachedOnce sync.Once
	connector := testkit.NewFakeConnector(4)
	scope := handoff.Scope{TenantID: "tenant-establish", UserID: "user", ConversationID: "conversation-establish"}
	store := &recoveryStoreFake{snapshot: RecoverySnapshot{ConversationID: scope.ConversationID}}
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second, RecoveryStore: store,
		initialRegisterGate: func() <-chan struct{} {
			reachedOnce.Do(func() { close(registerReached) })
			return releaseRegister
		},
	}, connector, managerIDs{session: "establish-session", conversation: scope.ConversationID})
	runCtx, runCancel := context.WithCancel(t.Context())
	defer runCancel()
	client := newChannelClient(4)
	runDone := make(chan error, 1)
	go func() {
		runDone <- manager.Run(runCtx, OpenRequest{Subject: Subject{
			TenantID: scope.TenantID, UserID: scope.UserID, ExpiresAt: time.Now().Add(time.Hour),
		}, ConversationID: scope.ConversationID}, client)
	}()
	_, err := connector.NextSession(t.Context())
	require.NoError(t, err)
	require.Equal(t, "session.created", (<-client.writes).EventType())
	<-registerReached

	err = manager.Reconnect(t.Context(), ReconnectRequest{
		Scope: scope, SessionID: "establish-session", LastIncarnation: 1,
	}, newChannelClient(1))
	require.ErrorIs(t, err, ErrStaleReconnect)
	require.Zero(t, store.calls.Load(), "establishing sessions do not begin recovery")
	manager.activeMu.RLock()
	logical := manager.logical["establish-session"]
	require.NotNil(t, logical)
	require.Equal(t, identity.Incarnation(1), logical.highWater)
	require.False(t, logical.reconnecting)
	require.Nil(t, logical.runtime)
	require.Equal(t, logicalEstablishing, logical.state)
	require.Empty(t, manager.active)
	manager.activeMu.RUnlock()
	shortCtx, shortCancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	_, err = connector.NextSession(shortCtx)
	shortCancel()
	require.ErrorIs(t, err, context.DeadlineExceeded, "reconnect cannot open a second provider while establishment is gated")

	close(releaseRegister)
	waitForActiveRuntime(t, manager, "establish-session", 1)
	manager.activeMu.RLock()
	require.Equal(t, identity.Incarnation(1), manager.logical["establish-session"].highWater)
	require.False(t, manager.logical["establish-session"].reconnecting)
	require.Equal(t, logicalActive, manager.logical["establish-session"].state)
	manager.activeMu.RUnlock()
	runCancel()
	<-runDone
}

func TestManagerInitialEstablishmentFailureDoesNotConsumeReconnectCapacity(t *testing.T) {
	registerReached := make(chan struct{})
	releaseRegister := make(chan struct{})
	connector := testkit.NewFakeConnector(4)
	scope := handoff.Scope{TenantID: "tenant-establish-fail", UserID: "user", ConversationID: "conversation-establish-fail"}
	store := &recoveryStoreFake{snapshot: RecoverySnapshot{ConversationID: scope.ConversationID}}
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second, RecoveryStore: store,
		initialRegisterGate: func() <-chan struct{} {
			select {
			case <-registerReached:
			default:
				close(registerReached)
			}
			return releaseRegister
		},
	}, connector, managerIDs{session: "establish-fail-session", conversation: scope.ConversationID})
	runCtx, runCancel := context.WithCancel(t.Context())
	client := newChannelClient(4)
	runDone := make(chan error, 1)
	go func() {
		runDone <- manager.Run(runCtx, OpenRequest{Subject: Subject{
			TenantID: scope.TenantID, UserID: scope.UserID, ExpiresAt: time.Now().Add(time.Hour),
		}, ConversationID: scope.ConversationID}, client)
	}()
	_, err := connector.NextSession(t.Context())
	require.NoError(t, err)
	require.Equal(t, "session.created", (<-client.writes).EventType())
	<-registerReached
	runCancel()
	close(releaseRegister)
	require.Error(t, <-runDone)

	err = manager.Reconnect(t.Context(), ReconnectRequest{
		Scope: scope, SessionID: "establish-fail-session", LastIncarnation: 1,
	}, newChannelClient(1))
	require.ErrorIs(t, err, ErrStaleReconnect)
	require.Zero(t, store.calls.Load(), "failed establishment is not recovery input")
	manager.activeMu.RLock()
	logical := manager.logical["establish-fail-session"]
	require.Nil(t, logical)
	require.Empty(t, manager.active)
	manager.activeMu.RUnlock()
	shortCtx, shortCancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer shortCancel()
	_, err = connector.NextSession(shortCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestManagerTombstonesAreTTLAndBudgetBoundedWithoutEvictingActiveSessions(t *testing.T) {
	manual := clock.NewManual(time.Unix(1_000, 0))
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
		ReconnectTombstoneTTL: time.Minute, MaxReconnectTombstones: 2, Clock: manual,
	}, testkit.NewFakeConnector(1), managerIDs{})
	var observed []int
	manager.SetMetricsHooks(MetricsHooks{ReconnectTombstones: func(count int) { observed = append(observed, count) }})
	activeScope := handoff.Scope{TenantID: "tenant", UserID: "active", ConversationID: "active"}
	manager.logical["active"] = &logicalSession{scope: activeScope, highWater: 1, state: logicalActive}
	for index := range 3 {
		sessionID := identity.SessionID(fmt.Sprintf("dead-%d", index))
		scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: identity.ConversationID(sessionID)}
		state := &runState{ids: IDs{SessionID: sessionID, ConversationID: scope.ConversationID, Incarnation: 1}, fence: NewEpochFence(identity.ResponseEpoch(index + 1))}
		runtime := &sessionRuntime{key: runtimeKey{sessionID: sessionID, incarnation: 1}, subject: Subject{TenantID: scope.TenantID, UserID: scope.UserID}, state: state}
		state.runtime = runtime
		manager.logical[sessionID] = &logicalSession{scope: scope, highWater: 1, runtime: runtime, turnBoundary: true, state: logicalActive}
		manager.active[runtime.key] = runtime
		manager.current[scope] = runtime
		manager.unregister(runtime.key, runtime)
		manual.Advance(time.Second)
	}
	manager.activeMu.RLock()
	require.NotNil(t, manager.logical["active"], "active sessions must never be tombstone eviction candidates")
	require.Nil(t, manager.logical["dead-0"], "oldest reconnect tombstone is evicted first")
	require.NotNil(t, manager.logical["dead-1"])
	require.NotNil(t, manager.logical["dead-2"])
	manager.activeMu.RUnlock()
	require.Contains(t, observed, 2)

	manual.Advance(time.Minute)
	manager.pruneLogicalSessions()
	manager.activeMu.RLock()
	require.NotNil(t, manager.logical["active"])
	require.Nil(t, manager.logical["dead-1"])
	require.Nil(t, manager.logical["dead-2"])
	manager.activeMu.RUnlock()
	require.Equal(t, 0, observed[len(observed)-1])
}

func TestManagerReconnectRefreshesRecoveryAfterOldRuntimeQuiesces(t *testing.T) {
	connector := testkit.NewFakeConnector(8)
	scope := handoff.Scope{TenantID: "tenant-refresh", UserID: "user", ConversationID: "conversation-refresh"}
	store := &quiescenceRefreshStore{
		preflight: RecoverySnapshot{
			ConversationID: scope.ConversationID,
			CommittedItems: []CommittedItem{{Sequence: 1, Role: "user", Content: "preflight text"}},
			LastEventSeq:   3,
		},
		refreshed: RecoverySnapshot{
			ConversationID: scope.ConversationID,
			CommittedItems: []CommittedItem{
				{Sequence: 1, Role: "user", Content: "preflight text"},
				{Sequence: 2, Role: "assistant", Content: "committed while quiescing"},
			},
			LastEventSeq: 4,
		},
	}
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
		RecoveryStore: store, RecoveryItemLimit: 8, RecoveryByteLimit: 1024,
	}, connector, managerIDs{session: "refresh-session", conversation: scope.ConversationID})
	oldCtx, oldCancel := context.WithCancel(t.Context())
	defer oldCancel()
	baseClient := newChannelClient(8)
	oldClient := &commitOnCloseClient{channelClient: baseClient, commit: func() { store.committed.Store(true) }}
	oldDone := make(chan error, 1)
	go func() {
		oldDone <- manager.Run(oldCtx, OpenRequest{Subject: Subject{
			TenantID: scope.TenantID, UserID: scope.UserID, ExpiresAt: time.Now().Add(time.Hour),
		}, ConversationID: scope.ConversationID}, oldClient)
	}()
	_, err := connector.NextSession(t.Context())
	require.NoError(t, err)
	require.Equal(t, "session.created", (<-baseClient.writes).EventType())
	oldRuntime := waitForActiveRuntime(t, manager, "refresh-session", 1)
	store.quiesced = oldRuntime.quiesced

	newCtx, newCancel := context.WithCancel(t.Context())
	defer newCancel()
	newClient := newChannelClient(8)
	reconnectDone := make(chan error, 1)
	go func() {
		reconnectDone <- manager.Reconnect(newCtx, ReconnectRequest{
			Scope: scope, SessionID: "refresh-session", LastIncarnation: 1, LastEventSeq: 3,
		}, newClient)
	}()
	newVoice, err := connector.NextSession(t.Context())
	require.NoError(t, err)
	require.Equal(t, int32(2), store.calls.Load())
	first, err := newVoice.NextCommand(t.Context())
	require.NoError(t, err)
	second, err := newVoice.NextCommand(t.Context())
	require.NoError(t, err)
	require.Contains(t, string(first.Item), "preflight text")
	require.Contains(t, string(second.Item), "committed while quiescing")
	shortCtx, shortCancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer shortCancel()
	_, err = newVoice.NextCommand(shortCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded, "post-quiescence item is restored exactly once")
	require.Equal(t, "session.created", (<-newClient.writes).EventType())
	newCancel()
	<-reconnectDone
	<-oldDone
}

func TestManagerReconnectRefreshFailureNeverPublishesStaleSnapshot(t *testing.T) {
	connector := testkit.NewFakeConnector(4)
	scope := handoff.Scope{TenantID: "tenant-refresh-fail", UserID: "user", ConversationID: "conversation-refresh-fail"}
	store := &quiescenceRefreshStore{
		preflight:  RecoverySnapshot{ConversationID: scope.ConversationID, LastEventSeq: 2},
		refreshErr: errors.New("refresh unavailable"),
	}
	store.committed.Store(true)
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second, RecoveryStore: store,
	}, connector, managerIDs{session: "refresh-fail-session", conversation: scope.ConversationID})
	oldCtx, oldCancel := context.WithCancel(t.Context())
	defer oldCancel()
	oldClient := newChannelClient(4)
	oldDone := make(chan error, 1)
	go func() {
		oldDone <- manager.Run(oldCtx, OpenRequest{Subject: Subject{
			TenantID: scope.TenantID, UserID: scope.UserID, ExpiresAt: time.Now().Add(time.Hour),
		}, ConversationID: scope.ConversationID}, oldClient)
	}()
	_, err := connector.NextSession(t.Context())
	require.NoError(t, err)
	require.Equal(t, "session.created", (<-oldClient.writes).EventType())
	oldRuntime := waitForActiveRuntime(t, manager, "refresh-fail-session", 1)
	store.quiesced = oldRuntime.quiesced

	newClient := newChannelClient(1)
	err = manager.Reconnect(t.Context(), ReconnectRequest{
		Scope: scope, SessionID: "refresh-fail-session", LastIncarnation: 1, LastEventSeq: 2,
	}, newClient)
	require.ErrorIs(t, err, ErrRecoveryUnavailable)
	require.Equal(t, int32(2), store.calls.Load())
	select {
	case <-newClient.done:
	default:
		t.Fatal("failed refresh did not close the unpublished client")
	}
	shortCtx, shortCancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer shortCancel()
	_, err = connector.NextSession(shortCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded, "stale preflight snapshot must not open a replacement provider")
	require.Eventually(t, func() bool {
		manager.activeMu.RLock()
		defer manager.activeMu.RUnlock()
		logical := manager.logical["refresh-fail-session"]
		return logical != nil && logical.highWater == 1 && !logical.reconnecting && logical.runtime == nil
	}, time.Second, time.Millisecond)
	<-oldDone
}

func TestManagerReconnectFencesClientIntakeAtomicallyWithFinalBoundary(t *testing.T) {
	boundarySampled := make(chan struct{})
	releaseBoundary := make(chan struct{})
	var sampleOnce sync.Once
	connector := testkit.NewFakeConnector(8)
	scope := handoff.Scope{TenantID: "tenant-late", UserID: "user", ConversationID: "conversation-late"}
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
		RecoveryStore: &recoveryStoreFake{snapshot: RecoverySnapshot{ConversationID: scope.ConversationID}},
		reconnectBoundaryGate: func() <-chan struct{} {
			sampleOnce.Do(func() { close(boundarySampled) })
			return releaseBoundary
		},
	}, connector, managerIDs{session: "late-session", conversation: scope.ConversationID})
	oldCtx, oldCancel := context.WithCancel(t.Context())
	defer oldCancel()
	baseClient := newChannelClient(8)
	oldClient := &notifyingReadClient{channelClient: baseClient, readObserved: make(chan struct{})}
	oldDone := make(chan error, 1)
	go func() {
		oldDone <- manager.Run(oldCtx, OpenRequest{Subject: Subject{
			TenantID: scope.TenantID, UserID: scope.UserID, ExpiresAt: time.Now().Add(time.Hour),
		}, ConversationID: scope.ConversationID}, oldClient)
	}()
	_, err := connector.NextSession(t.Context())
	require.NoError(t, err)
	require.Equal(t, "session.created", (<-baseClient.writes).EventType())
	oldRuntime := waitForActiveRuntime(t, manager, "late-session", 1)
	acceptedBefore := oldRuntime.state.controlAccepted.Load()
	externalBefore := oldRuntime.state.externalAccepted.Load()

	newCtx, newCancel := context.WithCancel(t.Context())
	defer newCancel()
	newClient := newChannelClient(8)
	reconnectDone := make(chan error, 1)
	go func() {
		reconnectDone <- manager.Reconnect(newCtx, ReconnectRequest{
			Scope: scope, SessionID: "late-session", LastIncarnation: 1,
		}, newClient)
	}()
	<-boundarySampled
	baseClient.reads <- protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}
	<-oldClient.readObserved
	close(releaseBoundary)
	_, err = connector.NextSession(t.Context())
	require.NoError(t, err)
	require.Equal(t, "session.created", (<-newClient.writes).EventType())
	require.Equal(t, acceptedBefore, oldRuntime.state.controlAccepted.Load(), "late client event was rejected before acceptance")
	require.Equal(t, externalBefore, oldRuntime.state.externalAccepted.Load(), "late client event did not cross the external intake fence")
	newCancel()
	<-reconnectDone
	<-oldDone
}

func TestManagerReconnectRejectsQueuedCommitUntilReactorAppliesIt(t *testing.T) {
	commitGate := make(chan struct{})
	connector := testkit.NewFakeConnector(8)
	scope := handoff.Scope{TenantID: "tenant-commit", UserID: "user", ConversationID: "conversation-commit"}
	manager := NewManager(withInitialManualInputTurn(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
		RecoveryStore: &recoveryStoreFake{snapshot: RecoverySnapshot{ConversationID: scope.ConversationID}},
		reactorApplyGate: func(input Input) <-chan struct{} {
			clientInput, ok := input.(ClientInput)
			if !ok {
				return nil
			}
			if _, ok := clientInput.Event.(protocol.InputAudioBufferCommitEvent); ok {
				return commitGate
			}
			return nil
		},
	}), connector, managerIDs{session: "commit-session", conversation: scope.ConversationID})
	ctx, cancel := context.WithCancel(t.Context())
	client := newChannelClient(8)
	done := make(chan error, 1)
	go func() {
		done <- manager.Run(ctx, OpenRequest{Subject: Subject{
			TenantID: scope.TenantID, UserID: scope.UserID, ExpiresAt: time.Now().Add(time.Hour),
		}, ConversationID: scope.ConversationID}, client)
	}()
	voice, err := connector.NextSession(t.Context())
	require.NoError(t, err)
	require.Equal(t, "session.created", (<-client.writes).EventType())
	activateInitialManualInputTurn(t, manager, "commit-session", 1, voice)
	frame := base64.StdEncoding.EncodeToString(make([]byte, 960))
	client.reads <- protocol.InputAudioBufferAppendEvent{Type: "input_audio_buffer.append", Audio: frame}
	appendCommand, err := voice.NextCommand(t.Context())
	require.NoError(t, err)
	require.Equal(t, realtime.CommandAppendAudio, appendCommand.Kind)
	client.reads <- protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}
	require.Eventually(t, func() bool {
		manager.activeMu.RLock()
		runtime := manager.active[runtimeKey{sessionID: "commit-session", incarnation: 1}]
		manager.activeMu.RUnlock()
		return runtime != nil && runtime.state.controlAccepted.Load() > runtime.state.controlApplied.Load()
	}, time.Second, time.Millisecond)

	err = manager.Reconnect(t.Context(), ReconnectRequest{
		Scope: scope, SessionID: "commit-session", LastIncarnation: 1,
	}, newChannelClient(1))
	require.ErrorIs(t, err, ErrReconnectNotAtBoundary)
	close(commitGate)
	commitCommand, err := voice.NextCommand(t.Context())
	require.NoError(t, err)
	require.Equal(t, realtime.CommandCommitAudio, commitCommand.Kind)
	cancel()
	<-done
}

func TestManagerReconnectRevalidatesBoundaryAfterRecoveryPreflight(t *testing.T) {
	commitGate := make(chan struct{})
	connector := testkit.NewFakeConnector(8)
	scope := handoff.Scope{TenantID: "tenant-cas", UserID: "user", ConversationID: "conversation-cas"}
	store := &blockingRecoveryStore{
		snapshot: RecoverySnapshot{ConversationID: scope.ConversationID},
		started:  make(chan struct{}), release: make(chan struct{}),
	}
	manager := NewManager(withInitialManualInputTurn(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second, RecoveryStore: store,
		reactorApplyGate: func(input Input) <-chan struct{} {
			if clientInput, ok := input.(ClientInput); ok {
				if _, ok := clientInput.Event.(protocol.InputAudioBufferCommitEvent); ok {
					return commitGate
				}
			}
			return nil
		},
	}), connector, managerIDs{session: "cas-session", conversation: scope.ConversationID})
	ctx, cancel := context.WithCancel(t.Context())
	client := newChannelClient(8)
	runDone := make(chan error, 1)
	go func() {
		runDone <- manager.Run(ctx, OpenRequest{Subject: Subject{
			TenantID: scope.TenantID, UserID: scope.UserID, ExpiresAt: time.Now().Add(time.Hour),
		}, ConversationID: scope.ConversationID}, client)
	}()
	voice, err := connector.NextSession(t.Context())
	require.NoError(t, err)
	select {
	case event := <-client.writes:
		require.Equal(t, "session.created", event.EventType())
	case <-time.After(time.Second):
		t.Fatal("initial session.created was not delivered")
	}
	activateInitialManualInputTurn(t, manager, "cas-session", 1, voice)
	waitForActiveRuntime(t, manager, "cas-session", 1)

	reconnectDone := make(chan error, 1)
	go func() {
		reconnectDone <- manager.Reconnect(t.Context(), ReconnectRequest{
			Scope: scope, SessionID: "cas-session", LastIncarnation: 1,
		}, newChannelClient(1))
	}()
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("reconnect did not enter recovery preflight")
	}
	frame := base64.StdEncoding.EncodeToString(make([]byte, 960))
	client.reads <- protocol.InputAudioBufferAppendEvent{Type: "input_audio_buffer.append", Audio: frame}
	commandCtx, commandCancel := context.WithTimeout(t.Context(), time.Second)
	_, err = voice.NextCommand(commandCtx)
	commandCancel()
	require.NoError(t, err)
	client.reads <- protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}
	require.Eventually(t, func() bool {
		manager.activeMu.RLock()
		runtime := manager.active[runtimeKey{sessionID: "cas-session", incarnation: 1}]
		manager.activeMu.RUnlock()
		return runtime != nil && runtime.state.controlAccepted.Load() > runtime.state.controlApplied.Load()
	}, time.Second, time.Millisecond)
	close(store.release)
	select {
	case reconnectErr := <-reconnectDone:
		require.ErrorIs(t, reconnectErr, ErrReconnectNotAtBoundary)
	case <-time.After(time.Second):
		t.Fatal("reconnect did not complete after recovery preflight was released")
	}
	select {
	case <-client.done:
		t.Fatal("failed CAS revalidation disturbed old runtime")
	default:
	}
	close(commitGate)
	commandCtx, commandCancel = context.WithTimeout(t.Context(), time.Second)
	command, err := voice.NextCommand(commandCtx)
	commandCancel()
	require.NoError(t, err)
	require.Equal(t, realtime.CommandCommitAudio, command.Kind)
	cancel()
	select {
	case <-runDone:
	case <-time.After(time.Second):
		t.Fatal("initial runtime did not stop after cancellation")
	}
}

func TestManagerReplacementRegistersClientReplayCursor(t *testing.T) {
	scope := handoff.Scope{TenantID: "tenant-replay", UserID: "user", ConversationID: "conversation-replay"}
	registrar := &replayRegistrarFake{}
	manager := &Manager{
		cfg:    Config{HandoffConsumerID: "client", HandoffSubscriptions: registrar},
		active: make(map[runtimeKey]*sessionRuntime), current: make(map[handoff.Scope]*sessionRuntime),
		logical: make(map[identity.SessionID]*logicalSession),
	}
	logical := &logicalSession{scope: scope, highWater: 1, reconnecting: true, state: logicalTombstoned}
	manager.logical["session-replay"] = logical
	replayAfter := int64(17)
	runtime := &sessionRuntime{
		key:         runtimeKey{sessionID: "session-replay", incarnation: 2},
		subject:     Subject{TenantID: scope.TenantID, UserID: scope.UserID},
		state:       &runState{ids: IDs{SessionID: "session-replay", ConversationID: scope.ConversationID, Incarnation: 2}, fence: NewEpochFence(3)},
		replayAfter: &replayAfter,
	}
	require.NoError(t, manager.registerReplacement(runtime, logical, 1))
	require.Equal(t, replayAfter, registrar.after)
	require.Equal(t, handoff.EventSubscription{ConsumerID: "client", Scope: scope}, registrar.sub)
}

func TestManagerReconnectRejectsWrongScopeAndIncarnationOverflow(t *testing.T) {
	manager := NewManager(Config{IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second},
		testkit.NewFakeConnector(1), managerIDs{})
	manager.activeMu.Lock()
	manager.logical = map[identity.SessionID]*logicalSession{"session": {
		scope:     handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "conversation"},
		highWater: identity.Incarnation(^uint64(0)),
		state:     logicalTombstoned,
	}}
	manager.activeMu.Unlock()
	request := ReconnectRequest{Scope: handoff.Scope{TenantID: "tenant", UserID: "wrong", ConversationID: "conversation"},
		SessionID: "session", LastIncarnation: identity.Incarnation(^uint64(0))}
	require.ErrorIs(t, manager.Reconnect(t.Context(), request, newChannelClient(1)), ErrStaleReconnect)
	request.Scope.UserID = "user"
	require.ErrorIs(t, manager.Reconnect(t.Context(), request, newChannelClient(1)), ErrStaleReconnect)
}

func TestManagerReconnectRetryIsBoundedCancellableAndOnlyAtTurnBoundary(t *testing.T) {
	connector := &failingReconnectConnector{}
	scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "conversation"}
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
		RecoveryStore:       &recoveryStoreFake{snapshot: RecoverySnapshot{ConversationID: scope.ConversationID}},
		ReconnectMaxRetries: 2, ReconnectRetryBase: time.Second, ReconnectRetryMax: time.Second,
		Clock: clock.NewManual(time.Unix(0, 0)), Jitter: zeroJitter{},
	}, connector, managerIDs{})
	manager.activeMu.Lock()
	manager.logical["session"] = &logicalSession{
		scope: scope, subject: Subject{TenantID: scope.TenantID, UserID: scope.UserID, ExpiresAt: time.Now().Add(time.Hour)},
		highWater: 1, turnBoundary: false, state: logicalTombstoned,
	}
	manager.activeMu.Unlock()
	request := ReconnectRequest{Scope: scope, SessionID: "session", LastIncarnation: 1}
	require.ErrorIs(t, manager.Reconnect(t.Context(), request, newChannelClient(1)), ErrReconnectNotAtBoundary)
	require.Zero(t, connector.calls.Load())

	manager.activeMu.Lock()
	manager.logical["session"].turnBoundary = true
	manager.activeMu.Unlock()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- manager.Reconnect(ctx, request, newChannelClient(1)) }()
	require.Eventually(t, func() bool { return connector.calls.Load() == 1 }, time.Second, time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
		require.NotContains(t, err.Error(), "raw provider detail")
	case <-time.After(time.Second):
		t.Fatal("reconnect retry did not stop on cancellation")
	}
	require.Equal(t, int32(1), connector.calls.Load())
}

func TestOfferReconnectLockOrderCompletes(t *testing.T) {
	scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "offer-reconnect-lock-order"}
	offerAtRevalidation := make(chan struct{})
	releaseOffer := make(chan struct{})
	var gateOnce sync.Once
	store := &blockingRecoveryStore{
		snapshot: RecoverySnapshot{ConversationID: scope.ConversationID},
		started:  make(chan struct{}), release: make(chan struct{}),
	}
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
		RecoveryStore: store,
		offerRevalidationGate: func() <-chan struct{} {
			gateOnce.Do(func() { close(offerAtRevalidation) })
			return releaseOffer
		},
	}, &failingReconnectConnector{}, managerIDs{})
	require.NoError(t, manager.validationErr)
	runtime, _, runtimeDone := currentRuntime(scope, "offer-reconnect-session")
	t.Cleanup(func() { close(runtimeDone) })
	require.NoError(t, manager.register(runtime))

	offerDone := make(chan error, 1)
	go func() {
		offerDone <- manager.OfferHandoffEvent(context.Background(), scope, handoff.Event{
			Seq: 1, EventID: "offer-reconnect-event", HandoffID: "handoff", TenantID: scope.TenantID,
			ConversationID: scope.ConversationID, Kind: "handoff.created", State: handoff.StatePending,
			Payload: json.RawMessage(`{}`),
		})
	}()
	select {
	case <-offerAtRevalidation:
	case <-time.After(time.Second):
		t.Fatal("offer did not reach ownership revalidation")
	}

	reconnectCtx, cancelReconnect := context.WithCancel(context.Background())
	reconnectDone := make(chan error, 1)
	go func() {
		reconnectDone <- manager.Reconnect(reconnectCtx, ReconnectRequest{
			Scope: scope, SessionID: runtime.key.sessionID, LastIncarnation: runtime.key.incarnation,
		}, newChannelClient(1))
	}()
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("reconnect did not enter its recovery preflight")
	}
	manager.activeMu.RLock()
	directoryReadHeld := true
	defer func() {
		if directoryReadHeld {
			manager.activeMu.RUnlock()
		}
	}()
	close(store.release)
	require.Eventually(t, func() bool {
		if manager.activeMu.TryRLock() {
			manager.activeMu.RUnlock()
			return false
		}
		return true
	}, time.Second, time.Millisecond, "reconnect did not reach the directory ownership boundary")

	cancelReconnect()
	close(releaseOffer)
	manager.activeMu.RUnlock()
	directoryReadHeld = false
	select {
	case err := <-offerDone:
		require.NoError(t, err)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("offer and reconnect deadlocked across runtime and directory ownership")
	}
	select {
	case err := <-reconnectDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("reconnect did not release directory ownership after cancellation")
	}
}

func TestShutdownUnregisterLockOrderCompletes(t *testing.T) {
	scope := handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "shutdown-unregister-lock-order"}
	offerAtRevalidation := make(chan struct{})
	releaseOffer := make(chan struct{})
	var gateOnce sync.Once
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
		offerRevalidationGate: func() <-chan struct{} {
			gateOnce.Do(func() { close(offerAtRevalidation) })
			return releaseOffer
		},
	}, testkit.NewFakeConnector(1), managerIDs{})
	require.NoError(t, manager.validationErr)
	runtime, _, runtimeDone := currentRuntime(scope, "shutdown-unregister-session")
	t.Cleanup(func() { close(runtimeDone) })
	require.NoError(t, manager.register(runtime))

	offerDone := make(chan error, 1)
	go func() {
		offerDone <- manager.OfferHandoffEvent(context.Background(), scope, handoff.Event{
			Seq: 1, EventID: "shutdown-unregister-event", HandoffID: "handoff", TenantID: scope.TenantID,
			ConversationID: scope.ConversationID, Kind: "handoff.created", State: handoff.StatePending,
			Payload: json.RawMessage(`{}`),
		})
	}()
	select {
	case <-offerAtRevalidation:
	case <-time.After(time.Second):
		t.Fatal("offer did not reach ownership revalidation")
	}

	shutdownStarted := make(chan struct{})
	shutdownDone := make(chan error, 1)
	go func() {
		close(shutdownStarted)
		runtime.stop()
		shutdownDone <- manager.unregister(runtime.key, runtime)
	}()
	<-shutdownStarted
	select {
	case err := <-shutdownDone:
		require.NoError(t, err)
	case <-time.After(250 * time.Millisecond):
		close(releaseOffer)
		t.Fatal("shutdown unregister deadlocked with offer ownership revalidation")
	}
	close(releaseOffer)
	select {
	case err := <-offerDone:
		require.ErrorIs(t, err, handoff.ErrNoLiveSession)
	case <-time.After(time.Second):
		t.Fatal("stale offer did not complete after shutdown unregister")
	}
	manager.activeMu.RLock()
	require.Empty(t, manager.active)
	require.Empty(t, manager.current)
	manager.activeMu.RUnlock()
}

func TestManagerReconnectCleanupIsDeadlineBoundedAndNeverPublishesEarly(t *testing.T) {
	t.Run("client close honors reconnect deadline", func(t *testing.T) {
		connector := testkit.NewFakeConnector(4)
		scope := handoff.Scope{TenantID: "tenant-close", UserID: "user", ConversationID: "conversation-close"}
		manager := NewManager(Config{
			IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
			RecoveryStore: &recoveryStoreFake{snapshot: RecoverySnapshot{ConversationID: scope.ConversationID}},
		}, connector, managerIDs{session: "close-session", conversation: scope.ConversationID})
		client := &contextBlockingCloseClient{created: make(chan struct{}), closeCall: make(chan struct{})}
		runDone := make(chan error, 1)
		go func() {
			runDone <- manager.Run(t.Context(), OpenRequest{Subject: Subject{
				TenantID: scope.TenantID, UserID: scope.UserID, ExpiresAt: time.Now().Add(time.Hour),
			}, ConversationID: scope.ConversationID}, client)
		}()
		_, err := connector.NextSession(t.Context())
		require.NoError(t, err)
		<-client.created
		require.Eventually(t, func() bool {
			manager.activeMu.RLock()
			defer manager.activeMu.RUnlock()
			return manager.active[runtimeKey{sessionID: "close-session", incarnation: 1}] != nil
		}, time.Second, time.Millisecond)
		reconnectCtx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
		defer cancel()
		started := time.Now()
		err = manager.Reconnect(reconnectCtx, ReconnectRequest{
			Scope: scope, SessionID: "close-session", LastIncarnation: 1,
		}, newChannelClient(1))
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Less(t, time.Since(started), 250*time.Millisecond)
		<-client.closeCall
		secondCtx, secondCancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer secondCancel()
		_, err = connector.NextSession(secondCtx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		select {
		case <-runDone:
		case <-time.After(2 * time.Second):
			t.Fatal("old runtime did not finish bounded client cleanup")
		}
	})

	t.Run("provider close cannot permit premature replacement", func(t *testing.T) {
		voice := &blockingProviderCloseVoice{closeStarted: make(chan struct{}), closeRelease: make(chan struct{})}
		connector := &blockingProviderConnector{voice: voice}
		scope := handoff.Scope{TenantID: "tenant-provider-close", UserID: "user", ConversationID: "conversation-provider-close"}
		manager := NewManager(Config{
			IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
			RecoveryStore: &recoveryStoreFake{snapshot: RecoverySnapshot{ConversationID: scope.ConversationID}},
		}, connector, managerIDs{session: "provider-close-session", conversation: scope.ConversationID})
		client := newChannelClient(4)
		runDone := make(chan error, 1)
		go func() {
			runDone <- manager.Run(t.Context(), OpenRequest{Subject: Subject{
				TenantID: scope.TenantID, UserID: scope.UserID, ExpiresAt: time.Now().Add(time.Hour),
			}, ConversationID: scope.ConversationID}, client)
		}()
		require.Equal(t, "session.created", (<-client.writes).EventType())
		require.Eventually(t, func() bool {
			manager.activeMu.RLock()
			defer manager.activeMu.RUnlock()
			return manager.active[runtimeKey{sessionID: "provider-close-session", incarnation: 1}] != nil
		}, time.Second, time.Millisecond)
		reconnectCtx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
		defer cancel()
		err := manager.Reconnect(reconnectCtx, ReconnectRequest{
			Scope: scope, SessionID: "provider-close-session", LastIncarnation: 1,
		}, newChannelClient(1))
		require.ErrorIs(t, err, context.DeadlineExceeded)
		<-voice.closeStarted
		require.Equal(t, int32(1), connector.calls.Load(), "replacement provider was never opened")
		manager.activeMu.RLock()
		_, published := manager.active[runtimeKey{sessionID: "provider-close-session", incarnation: 2}]
		manager.activeMu.RUnlock()
		require.False(t, published)
		retryCtx, retryCancel := context.WithTimeout(t.Context(), 15*time.Millisecond)
		retryErr := manager.Reconnect(retryCtx, ReconnectRequest{
			Scope: scope, SessionID: "provider-close-session", LastIncarnation: 1,
		}, newChannelClient(1))
		retryCancel()
		require.ErrorIs(t, retryErr, context.DeadlineExceeded)
		require.Equal(t, int32(1), connector.calls.Load(), "retry also waits for original provider cleanup")
		close(voice.closeRelease)
		select {
		case <-runDone:
		case <-time.After(time.Second):
			t.Fatal("old runtime did not finish after provider close released")
		}
	})
}

type managerIDs struct {
	session      identity.SessionID
	conversation identity.ConversationID
}

func waitForActiveRuntime(t *testing.T, manager *Manager, sessionID identity.SessionID, incarnation identity.Incarnation) *sessionRuntime {
	t.Helper()
	var runtime *sessionRuntime
	require.Eventually(t, func() bool {
		manager.activeMu.RLock()
		runtime = manager.active[runtimeKey{sessionID: sessionID, incarnation: incarnation}]
		manager.activeMu.RUnlock()
		return runtime != nil
	}, time.Second, time.Millisecond)
	return runtime
}

func (i managerIDs) NewSessionID() identity.SessionID           { return i.session }
func (i managerIDs) NewConversationID() identity.ConversationID { return i.conversation }

func TestManagerProvisionsAuthenticatedConversationBeforeProviderConnectAndFailsClosed(t *testing.T) {
	sentinel := errors.New("database unavailable")
	provisioner := &conversationProvisionerFake{err: sentinel}
	connector := testkit.NewFakeConnector(1)
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
		ConversationProvisioner: provisioner, ConversationTTL: 24 * time.Hour,
	}, connector, managerIDs{session: "session-provision", conversation: "conversation-provision"})

	err := manager.Run(t.Context(), OpenRequest{Subject: Subject{
		TenantID: "tenant-provision", UserID: "user-provision", ExpiresAt: time.Now().Add(time.Hour),
	}}, newChannelClient(1))
	require.ErrorIs(t, err, sentinel)
	scopes, ttls := provisioner.snapshot()
	require.Equal(t, []handoff.Scope{{
		TenantID: "tenant-provision", UserID: "user-provision", ConversationID: "conversation-provision",
	}}, scopes)
	require.Equal(t, []time.Duration{24 * time.Hour}, ttls)

	providerCtx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	_, connectErr := connector.NextSession(providerCtx)
	require.ErrorIs(t, connectErr, context.DeadlineExceeded, "provider connection must not begin after provisioning failure")
	manager.activeMu.RLock()
	defer manager.activeMu.RUnlock()
	require.Empty(t, manager.active)
	require.Nil(t, manager.logical["session-provision"])
}

func TestManagerEphemeralSyntheticSessionUsesProviderWithoutConversationProvisioning(t *testing.T) {
	provisioner := &conversationProvisionerFake{err: errors.New("must not be called")}
	connector := testkit.NewFakeConnector(1)
	manager := NewManager(Config{IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second, ConversationProvisioner: provisioner, ConversationTTL: 24 * time.Hour}, connector, managerIDs{session: "synthetic-session", conversation: "synthetic-conversation"})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- manager.Run(ctx, OpenRequest{Subject: Subject{TenantID: "synthetic-tenant", UserID: "synthetic-canary", Ephemeral: true, ExpiresAt: time.Now().Add(time.Hour)}}, newChannelClient(4))
	}()
	providerCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if _, err := connector.NextSession(providerCtx); err != nil {
		t.Fatalf("ephemeral session did not exercise provider path: %v", err)
	}
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)
	scopes, ttls := provisioner.snapshot()
	require.Empty(t, scopes)
	require.Empty(t, ttls)
}

type channelClient struct {
	reads  chan protocol.ClientEvent
	writes chan protocol.ServerEvent
	done   chan struct{}

	closeOnce sync.Once
}

type inputModeBoundaryClient struct {
	*channelClient
	current  func() realtime.InputTurnMode
	observed chan realtime.InputTurnMode
}

type gateAudioClient struct {
	*channelClient
	audioStarted chan struct{}
	audioRelease chan struct{}
	startedOnce  sync.Once

	mu            sync.Mutex
	recordedAudio []protocol.ResponseOutputAudioDeltaEvent
}

type lifecycleConnector struct {
	voice     *lifecycleVoice
	connected chan realtime.OpenRequest
}

func newLifecycleConnector() *lifecycleConnector {
	return &lifecycleConnector{
		voice: &lifecycleVoice{
			failure: make(chan error, 1),
			events:  make(chan realtime.Event, 1),
			closed:  make(chan struct{}),
			sending: make(chan struct{}),
		},
		connected: make(chan realtime.OpenRequest, 1),
	}
}

func (c *lifecycleConnector) Connect(ctx context.Context, request realtime.OpenRequest) (realtime.Session, error) {
	select {
	case c.connected <- request:
		return c.voice, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type lifecycleVoice struct {
	failure chan error
	events  chan realtime.Event
	closed  chan struct{}
	sending chan struct{}

	closeOnce      sync.Once
	sendOnce       sync.Once
	sendMu         sync.Mutex
	immediateSends int
	closes         atomic.Int32
	closeLive      atomic.Bool
}

func (v *lifecycleVoice) Send(ctx context.Context, _ realtime.Command) error {
	v.sendMu.Lock()
	if v.immediateSends > 0 {
		v.immediateSends--
		v.sendMu.Unlock()
		return nil
	}
	v.sendMu.Unlock()
	v.sendOnce.Do(func() { close(v.sending) })
	select {
	case <-v.closed:
		return io.ErrClosedPipe
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (v *lifecycleVoice) Receive(ctx context.Context) (realtime.Event, error) {
	select {
	case event := <-v.events:
		return event, nil
	case err := <-v.failure:
		return realtime.Event{}, err
	case <-ctx.Done():
		return realtime.Event{}, ctx.Err()
	}
}

func (v *lifecycleVoice) allowImmediateSends(count int) {
	v.sendMu.Lock()
	v.immediateSends = count
	v.sendMu.Unlock()
}

func (v *lifecycleVoice) Close(ctx context.Context) error {
	v.closes.Add(1)
	v.closeLive.Store(ctx.Err() == nil)
	v.closeOnce.Do(func() { close(v.closed) })
	return ctx.Err()
}

type lifecycleClient struct {
	created chan struct{}
	closed  chan struct{}

	createdOnce sync.Once
	closeOnce   sync.Once
	closes      atomic.Int32
}

type blockingWriteClient struct {
	started chan struct{}
	closed  chan struct{}

	startOnce sync.Once
	closeOnce sync.Once
}

type gatedResponseControlClient struct {
	started   chan struct{}
	writes    chan string
	closed    chan struct{}
	startOnce sync.Once
	closeOnce sync.Once
}

func (c *gatedResponseControlClient) Read(ctx context.Context) (protocol.ClientEvent, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (c *gatedResponseControlClient) Write(ctx context.Context, event protocol.ServerEvent) error {
	if event.EventType() == "response.output_audio_transcript.delta" {
		c.startOnce.Do(func() { close(c.started) })
		<-ctx.Done()
		return ctx.Err()
	}
	select {
	case c.writes <- event.EventType():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *gatedResponseControlClient) Close(context.Context, int, string) error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

type failureConnector struct {
	voice     *failureVoice
	connected chan struct{}
	once      sync.Once
}

func (c *failureConnector) Connect(ctx context.Context, _ realtime.OpenRequest) (realtime.Session, error) {
	c.once.Do(func() { close(c.connected) })
	return c.voice, ctx.Err()
}

type failureVoice struct {
	sendErr    chan error
	receiveErr chan error
	closeErr   error
	closes     atomic.Int32
	closeLive  atomic.Bool
}

func (v *failureVoice) Send(ctx context.Context, _ realtime.Command) error {
	select {
	case err := <-v.sendErr:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (v *failureVoice) Receive(ctx context.Context) (realtime.Event, error) {
	select {
	case err := <-v.receiveErr:
		return realtime.Event{}, err
	case <-ctx.Done():
		return realtime.Event{}, ctx.Err()
	}
}

func (v *failureVoice) Close(ctx context.Context) error {
	v.closes.Add(1)
	v.closeLive.Store(ctx.Err() == nil)
	if err := ctx.Err(); err != nil {
		return err
	}
	return v.closeErr
}

type failureClient struct {
	reads    chan protocol.ClientEvent
	readErr  chan error
	writeErr chan error
	closes   atomic.Int32
	closeErr error
}

type recordingVoice struct {
	sent chan realtime.Command
}

func (v *recordingVoice) Send(ctx context.Context, command realtime.Command) error {
	select {
	case v.sent <- command:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (*recordingVoice) Receive(ctx context.Context) (realtime.Event, error) {
	<-ctx.Done()
	return realtime.Event{}, ctx.Err()
}

func (*recordingVoice) Close(context.Context) error { return nil }

type gatedCancelVoice struct {
	sent          chan realtime.Command
	cancelStarted chan struct{}
	cancelRelease chan struct{}
}

func (v *gatedCancelVoice) Send(ctx context.Context, command realtime.Command) error {
	v.sent <- command
	if command.Kind == realtime.CommandCancelResponse {
		close(v.cancelStarted)
		select {
		case <-v.cancelRelease:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (*gatedCancelVoice) Receive(ctx context.Context) (realtime.Event, error) {
	<-ctx.Done()
	return realtime.Event{}, ctx.Err()
}

func (*gatedCancelVoice) Close(context.Context) error { return nil }

type gatedProviderVoice struct {
	started chan realtime.Command
	release chan struct{}
}

func (v *gatedProviderVoice) Send(ctx context.Context, command realtime.Command) error {
	v.started <- command
	select {
	case <-v.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (*gatedProviderVoice) Receive(ctx context.Context) (realtime.Event, error) {
	<-ctx.Done()
	return realtime.Event{}, ctx.Err()
}

func (*gatedProviderVoice) Close(context.Context) error { return nil }

type closeErrorConnector struct {
	voice     *closeErrorVoice
	connected chan struct{}
}

func (c *closeErrorConnector) Connect(context.Context, realtime.OpenRequest) (realtime.Session, error) {
	close(c.connected)
	return c.voice, nil
}

type closeErrorVoice struct{ closeErr error }

func (*closeErrorVoice) Send(context.Context, realtime.Command) error { return nil }
func (*closeErrorVoice) Receive(ctx context.Context) (realtime.Event, error) {
	<-ctx.Done()
	return realtime.Event{}, ctx.Err()
}
func (v *closeErrorVoice) Close(context.Context) error { return v.closeErr }

type closeErrorClient struct {
	closeErr error
	writes   chan string
	closed   chan struct{}
	once     sync.Once
}

func (c *closeErrorClient) Read(ctx context.Context) (protocol.ClientEvent, error) {
	select {
	case <-c.closed:
		return nil, io.EOF
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *closeErrorClient) Write(_ context.Context, event protocol.ServerEvent) error {
	c.writes <- event.EventType()
	return nil
}

func (c *closeErrorClient) Close(context.Context, int, string) error {
	c.writes <- "client.close"
	c.once.Do(func() { close(c.closed) })
	return c.closeErr
}

func newFailureClient() *failureClient {
	return &failureClient{
		reads: make(chan protocol.ClientEvent, 1), readErr: make(chan error, 1), writeErr: make(chan error, 1),
	}
}

func (c *failureClient) Read(ctx context.Context) (protocol.ClientEvent, error) {
	select {
	case event := <-c.reads:
		return event, nil
	case err := <-c.readErr:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *failureClient) Write(ctx context.Context, _ protocol.ServerEvent) error {
	select {
	case err := <-c.writeErr:
		return err
	default:
		return ctx.Err()
	}
}

func (c *failureClient) Close(context.Context, int, string) error {
	c.closes.Add(1)
	return c.closeErr
}

func newBlockingWriteClient() *blockingWriteClient {
	return &blockingWriteClient{started: make(chan struct{}), closed: make(chan struct{})}
}

func (c *blockingWriteClient) Read(ctx context.Context) (protocol.ClientEvent, error) {
	select {
	case <-c.closed:
		return nil, io.EOF
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *blockingWriteClient) Write(ctx context.Context, event protocol.ServerEvent) error {
	if event.EventType() == "session.created" {
		return nil
	}
	c.startOnce.Do(func() { close(c.started) })
	<-ctx.Done()
	return ctx.Err()
}

func (c *blockingWriteClient) Close(context.Context, int, string) error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

func newLifecycleClient() *lifecycleClient {
	return &lifecycleClient{created: make(chan struct{}), closed: make(chan struct{})}
}

func (c *lifecycleClient) Read(ctx context.Context) (protocol.ClientEvent, error) {
	select {
	case <-c.closed:
		return nil, io.EOF
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *lifecycleClient) Write(ctx context.Context, event protocol.ServerEvent) error {
	if event.EventType() == "session.created" {
		c.createdOnce.Do(func() { close(c.created) })
	}
	return ctx.Err()
}

func (c *lifecycleClient) Close(context.Context, int, string) error {
	c.closes.Add(1)
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

func newGateAudioClient(buffer int) *gateAudioClient {
	return &gateAudioClient{
		channelClient: newChannelClient(buffer),
		audioStarted:  make(chan struct{}),
		audioRelease:  make(chan struct{}),
	}
}

func (c *gateAudioClient) Write(ctx context.Context, event protocol.ServerEvent) error {
	audio, ok := event.(protocol.ResponseOutputAudioDeltaEvent)
	if !ok {
		return c.channelClient.Write(ctx, event)
	}
	c.startedOnce.Do(func() { close(c.audioStarted) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.audioRelease:
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	c.recordedAudio = append(c.recordedAudio, audio)
	c.mu.Unlock()
	return nil
}

func newChannelClient(buffer int) *channelClient {
	return &channelClient{
		reads:  make(chan protocol.ClientEvent, buffer),
		writes: make(chan protocol.ServerEvent, buffer),
		done:   make(chan struct{}),
	}
}

func (c *channelClient) Read(ctx context.Context) (protocol.ClientEvent, error) {
	select {
	case event := <-c.reads:
		return event, nil
	case <-c.done:
		return nil, io.EOF
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *channelClient) Write(ctx context.Context, event protocol.ServerEvent) error {
	select {
	case c.writes <- event:
		return nil
	case <-c.done:
		return io.ErrClosedPipe
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *inputModeBoundaryClient) Write(ctx context.Context, event protocol.ServerEvent) error {
	if event.EventType() == "session.updated" {
		select {
		case c.observed <- c.current():
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return c.channelClient.Write(ctx, event)
}

func (c *channelClient) Close(context.Context, int, string) error {
	c.closeOnce.Do(func() { close(c.done) })
	return nil
}

func TestManagerFramesAppendAndFlushesEvenTailBeforeCommit(t *testing.T) {
	connector := testkit.NewFakeConnector(16)
	manager := NewManager(withInitialManualInputTurn(Config{
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
	}), connector, managerIDs{session: "session-1", conversation: "conversation-1"})
	client := newChannelClient(8)
	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() {
		runResult <- manager.Run(ctx, OpenRequest{
			Subject: Subject{TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour)},
		}, client)
	}()

	voice, err := connector.NextSession(context.Background())
	require.NoError(t, err)
	activateInitialManualInputTurn(t, manager, "session-1", 1, voice)
	pcm := make([]byte, 962)
	for index := range pcm {
		pcm[index] = byte(index)
	}
	client.reads <- protocol.InputAudioBufferAppendEvent{
		Type:  "input_audio_buffer.append",
		Audio: base64.StdEncoding.EncodeToString(pcm[:481]),
	}
	client.reads <- protocol.InputAudioBufferAppendEvent{
		Type:  "input_audio_buffer.append",
		Audio: base64.StdEncoding.EncodeToString(pcm[481:]),
	}
	client.reads <- protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}

	first, err := voice.NextCommand(context.Background())
	require.NoError(t, err)
	require.Equal(t, realtime.CommandAppendAudio, first.Kind)
	require.Equal(t, uint64(1), first.Sequence)
	require.Equal(t, pcm[:960], first.Audio)

	tail, err := voice.NextCommand(context.Background())
	require.NoError(t, err)
	require.Equal(t, realtime.CommandAppendAudio, tail.Kind)
	require.Equal(t, uint64(2), tail.Sequence)
	require.Equal(t, pcm[960:], tail.Audio)

	commit, err := voice.NextCommand(context.Background())
	require.NoError(t, err)
	require.Equal(t, realtime.CommandCommitAudio, commit.Kind)
	require.Equal(t, uint64(1), commit.Sequence, "reactor control identity is independent of media sequence")
	require.Equal(t, uint64(2), commit.AfterSeq)

	cancel()
	require.ErrorIs(t, <-runResult, context.Canceled)
}

func TestManagerRejectsSecondCommitUntilAckAndOrdersAckBeforeTranscript(t *testing.T) {
	connector := testkit.NewFakeConnector(32)
	manager := NewManager(withInitialManualInputTurn(Config{
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
	}), connector, managerIDs{session: "session-commit-ack", conversation: "conversation-commit-ack"})
	client := newChannelClient(32)
	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() {
		runResult <- manager.Run(ctx, OpenRequest{Subject: Subject{
			TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour),
		}}, client)
	}()
	voice, err := connector.NextSession(t.Context())
	require.NoError(t, err)
	activateInitialManualInputTurn(t, manager, "session-commit-ack", 1, voice)
	commandCtx, stop := context.WithTimeout(t.Context(), time.Second)
	defer stop()

	appendAndExpect := func(fill byte) {
		client.reads <- protocol.InputAudioBufferAppendEvent{
			Type: "input_audio_buffer.append", Audio: base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{fill}, 960)),
		}
		command, commandErr := voice.NextCommand(commandCtx)
		require.NoError(t, commandErr)
		require.Equal(t, realtime.CommandAppendAudio, command.Kind)
	}
	commitAndExpect := func() realtime.Command {
		client.reads <- protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}
		command, commandErr := voice.NextCommand(commandCtx)
		require.NoError(t, commandErr)
		require.Equal(t, realtime.CommandCommitAudio, command.Kind)
		return command
	}
	releaseAndExpect := func(itemID string, commitSequence uint64) {
		command, commandErr := voice.NextCommand(commandCtx)
		require.NoError(t, commandErr)
		require.Equal(t, realtime.CommandAcknowledgeAudioCommit, command.Kind)
		require.Equal(t, itemID, command.InputItemID)
		require.Equal(t, commitSequence, command.InputAudioCommitSequence)
	}
	nextClientType := func(want string) protocol.ServerEvent {
		for {
			select {
			case event := <-client.writes:
				if event.EventType() == want {
					return event
				}
			case runErr := <-runResult:
				t.Fatalf("manager stopped before %s: %v", want, runErr)
			case <-commandCtx.Done():
				t.Fatalf("timed out waiting for %s", want)
			}
		}
	}

	appendAndExpect(0x11)
	firstCommit := commitAndExpect()
	appendAndExpect(0x22)
	client.reads <- protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}
	semantic := nextClientType("error").(protocol.ErrorEvent)
	require.Equal(t, "input_audio_buffer_commit_in_flight", semantic.Error.Code)
	require.Equal(t, "input audio commit acknowledgement is pending", semantic.Error.Message)
	require.NotContains(t, semantic.Error.Message, "0x22")

	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventInputAudioCommitted, ProviderEventID: "ack-a", ItemID: "input-a",
		InputAudioCommitSequence: firstCommit.Sequence,
	}))
	releaseAndExpect("input-a", firstCommit.Sequence)
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventTranscriptionDelta, ProviderEventID: "delta-a", ItemID: "input-a", Text: "partial",
	}))
	committed := nextClientType("input_audio_buffer.committed").(protocol.InputAudioBufferCommittedEvent)
	require.Equal(t, "input-a", committed.ItemID)
	delta := nextClientType("conversation.item.input_audio_transcription.delta").(protocol.ConversationItemInputAudioTranscriptionDeltaEvent)
	require.Equal(t, "input-a", delta.ItemID, "the sync ack boundary must open the exact item before its transcript")

	secondCommit := commitAndExpect()
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventInputAudioCommitted, ProviderEventID: "duplicate-ack-a", ItemID: "input-a",
		InputAudioCommitSequence: secondCommit.Sequence,
	}))
	appendAndExpect(0x33)
	client.reads <- protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}
	duplicateSemantic := nextClientType("error").(protocol.ErrorEvent)
	require.Equal(t, "input_audio_buffer_commit_in_flight", duplicateSemantic.Error.Code,
		"a historical ack must not release the later commit")
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventInputAudioCommitted, ProviderEventID: "ack-b", ItemID: "input-b",
		InputAudioCommitSequence: secondCommit.Sequence,
	}))
	releaseAndExpect("input-b", secondCommit.Sequence)
	nextClientType("input_audio_buffer.committed")
	thirdCommit := commitAndExpect()
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventInputAudioCommitted, ProviderEventID: "ack-c", ItemID: "input-c",
		InputAudioCommitSequence: thirdCommit.Sequence,
	}))
	releaseAndExpect("input-c", thirdCommit.Sequence)
	nextClientType("input_audio_buffer.committed")

	cancel()
	require.ErrorIs(t, <-runResult, context.Canceled)
}

func TestManagerSubmitReturnsCommitInFlightWithoutTerminatingSession(t *testing.T) {
	connector := testkit.NewFakeConnector(16)
	manager := NewManager(withInitialManualInputTurn(Config{
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
	}), connector, managerIDs{session: "session-submit-commit", conversation: "conversation-submit-commit"})
	client := newChannelClient(16)
	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() {
		runResult <- manager.Run(ctx, OpenRequest{Subject: Subject{
			TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour),
		}}, client)
	}()
	voice, err := connector.NextSession(t.Context())
	require.NoError(t, err)
	waitForActiveRuntime(t, manager, "session-submit-commit", 1)
	activateInitialManualInputTurn(t, manager, "session-submit-commit", 1, voice)

	commit := ClientInput{Event: protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}}
	require.NoError(t, manager.Submit(t.Context(), "session-submit-commit", 1, commit))
	first, err := voice.NextCommand(t.Context())
	require.NoError(t, err)
	require.Equal(t, realtime.CommandCommitAudio, first.Kind)

	err = manager.Submit(t.Context(), "session-submit-commit", 1, commit)
	require.ErrorIs(t, err, ErrAudioCommitInFlight)
	require.Equal(t, int64(1), manager.ActiveSessions(), "a semantic rejection must not terminate the runtime")

	require.NoError(t, voice.Emit(t.Context(), realtime.Event{
		Kind: realtime.EventInputAudioCommitted, ProviderEventID: "ack-submit-a", ItemID: "submit-a",
		InputAudioCommitSequence: first.Sequence,
	}))
	release, err := voice.NextCommand(t.Context())
	require.NoError(t, err)
	require.Equal(t, realtime.CommandAcknowledgeAudioCommit, release.Kind)
	require.Equal(t, "submit-a", release.InputItemID)
	require.Equal(t, first.Sequence, release.InputAudioCommitSequence)
	for {
		event := <-client.writes
		if event.EventType() == "input_audio_buffer.committed" {
			break
		}
	}
	require.NoError(t, manager.Submit(t.Context(), "session-submit-commit", 1, commit))
	second, err := voice.NextCommand(t.Context())
	require.NoError(t, err)
	require.Equal(t, realtime.CommandCommitAudio, second.Kind)

	cancel()
	require.ErrorIs(t, <-runResult, context.Canceled)
}

func TestClientCommitSemanticErrorDispatchOverflowRemainsFatal(t *testing.T) {
	cfg, err := normalizeManagerConfig(withInitialManualInputTurn(Config{
		ControlEvents: 2, WriterControlEvents: 1,
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
	}))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	state := newRunState(cfg, testIDs(), &recordingVoice{sent: make(chan realtime.Command, 1)}, newChannelClient(1), cancel)
	loopResult := make(chan error, 1)
	go func() { loopResult <- state.reactorLoop(ctx) }()
	require.NoError(t, state.enqueueReactorSync(t.Context(), reactorMessage{input: VoiceInput{Incarnation: state.ids.Incarnation, Event: realtime.Event{
		Kind: realtime.EventSessionUpdated, InputTurnMode: realtime.InputTurnModeManual, InputTurnModeSpecified: true,
		SessionUpdateAcknowledgement: true,
	}}}))

	first := reactorMessage{
		input: ClientInput{Event: protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}},
		done:  make(chan error, 1),
	}
	require.NoError(t, state.enqueueReactor(first))
	require.NoError(t, <-first.done)
	<-state.clientSlots

	second := reactorMessage{
		input:             ClientInput{Event: protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}},
		clientAudioCommit: true,
		done:              make(chan error, 1),
	}
	require.NoError(t, state.enqueueReactor(second))
	require.ErrorIs(t, <-second.done, ErrControlOverflow)
	select {
	case loopErr := <-loopResult:
		require.ErrorIs(t, loopErr, ErrControlOverflow)
	case <-time.After(20 * time.Millisecond):
		cancel()
		require.ErrorIs(t, <-loopResult, ErrControlOverflow)
	}
}

func TestManagerCommitAckTimeoutTearsDownRuntimeAndPreservesCause(t *testing.T) {
	connector := testkit.NewFakeConnector(8)
	manager := NewManager(withInitialManualInputTurn(Config{
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
	}), connector, managerIDs{session: "session-commit-timeout", conversation: "conversation-commit-timeout"})
	client := newChannelClient(8)
	runResult := make(chan error, 1)
	go func() {
		runResult <- manager.Run(t.Context(), OpenRequest{Subject: Subject{
			TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour),
		}}, client)
	}()
	voice, err := connector.NextSession(t.Context())
	require.NoError(t, err)
	waitForActiveRuntime(t, manager, "session-commit-timeout", 1)
	activateInitialManualInputTurn(t, manager, "session-commit-timeout", 1, voice)
	require.NoError(t, manager.Submit(t.Context(), "session-commit-timeout", 1,
		ClientInput{Event: protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}}))
	command, err := voice.NextCommand(t.Context())
	require.NoError(t, err)
	require.Equal(t, realtime.CommandCommitAudio, command.Kind)

	voice.Fail(s2sws.ErrAudioCommitAckTimeout)
	require.ErrorIs(t, <-runResult, s2sws.ErrAudioCommitAckTimeout)
	require.Zero(t, manager.ActiveSessions())
	require.ErrorIs(t, manager.Submit(t.Context(), "session-commit-timeout", 1, DurableInput{Sequence: 1}), ErrSessionNotActive)
}

func TestManagerRejectsExpiredSubjectBeforeConnect(t *testing.T) {
	connector := testkit.NewFakeConnector(1)
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
	}, connector, managerIDs{session: "session-1", conversation: "conversation-1"})
	err := manager.Run(context.Background(), OpenRequest{
		Subject: Subject{TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(-time.Second)},
	}, newChannelClient(1))
	require.Error(t, err)
	require.False(t, errors.Is(err, context.Canceled))
}

func TestManagerZeroExpiryInheritsParentAndFiniteExpiryCancelsExactly(t *testing.T) {
	t.Run("zero expiry", func(t *testing.T) {
		connector := testkit.NewFakeConnector(1)
		manager := NewManager(Config{IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second}, connector, managerIDs{session: "zero-session", conversation: "zero-conversation"})
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() {
			done <- manager.Run(ctx, OpenRequest{Subject: Subject{TenantID: "tenant", UserID: "user"}}, newChannelClient(1))
		}()
		_, err := connector.NextSession(t.Context())
		require.NoError(t, err)
		select {
		case err := <-done:
			t.Fatalf("zero-expiry session ended early: %v", err)
		default:
		}
		cancel()
		require.ErrorIs(t, <-done, context.Canceled)
	})
	t.Run("finite expiry", func(t *testing.T) {
		connector := testkit.NewFakeConnector(1)
		manager := NewManager(Config{IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second}, connector, managerIDs{session: "finite-session", conversation: "finite-conversation"})
		expires := time.Now().Add(80 * time.Millisecond)
		done := make(chan error, 1)
		go func() {
			done <- manager.Run(t.Context(), OpenRequest{Subject: Subject{TenantID: "tenant", UserID: "user", ExpiresAt: expires}}, newChannelClient(1))
		}()
		_, err := connector.NextSession(t.Context())
		require.NoError(t, err)
		started := time.Now()
		err = <-done
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.GreaterOrEqual(t, time.Since(started), time.Until(expires)-20*time.Millisecond)
	})
}

func TestDispatchFlushFencesActiveAndQueuedResponseControlsBeforeCancelOrTerminate(t *testing.T) {
	for _, action := range []string{"cancel", "terminate"} {
		t.Run(action, func(t *testing.T) {
			cfg, err := normalizeManagerConfig(Config{IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second})
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			client := &gatedResponseControlClient{started: make(chan struct{}), writes: make(chan string, 2), closed: make(chan struct{})}
			// Keep provider Send blocked until the assertion observes it. This
			// makes the required cancel-before-client-write composition order
			// deterministic instead of relying on scheduler timing.
			voice := &recordingVoice{sent: make(chan realtime.Command)}
			ids := IDs{SessionID: "session", ConversationID: "conversation", Incarnation: 1}
			state := newRunState(cfg, ids, voice, client, cancel)
			epoch := state.fence.Advance()
			writerDone := make(chan error, 1)
			go func() { writerDone <- state.clientWriter(ctx) }()
			voiceDone := make(chan error, 1)
			go func() { voiceDone <- state.voiceWriter(ctx) }()

			stale := protocol.ResponseOutputAudioTranscriptDeltaEvent{Type: "response.output_audio_transcript.delta", Delta: "private"}
			require.NoError(t, state.enqueueClientControl(clientControl{event: stale, epoch: epoch, incarnation: ids.Incarnation, writeCtx: ctx}))
			select {
			case <-client.started:
			case <-time.After(time.Second):
				t.Fatal("response control write did not start")
			}
			require.NoError(t, state.enqueueClientControl(clientControl{event: stale, epoch: epoch, incarnation: ids.Incarnation, writeCtx: ctx}))
			flushEpoch := state.fence.Advance()
			effects := []Effect{{FlushBefore: flushEpoch}}
			if action == "cancel" {
				command := realtime.Command{Kind: realtime.CommandCancelResponse, Epoch: flushEpoch, Sequence: 1}
				effects = append(effects, Effect{VoiceCommand: &command}, Effect{ClientEvent: protocol.ErrorEvent{Type: "error"}, RequireClientWrite: true})
			} else {
				effects = append(effects, Effect{Close: &CloseEffect{Code: 1008, Reason: "safety"}})
			}
			dispatchDone := make(chan error, 1)
			go func() { dispatchDone <- state.dispatchEffects(ctx, effects, 0, 0) }()

			if action == "cancel" {
				select {
				case command := <-voice.sent:
					require.Equal(t, realtime.CommandCancelResponse, command.Kind)
				case eventType := <-client.writes:
					t.Fatalf("client event %q escaped before provider cancellation", eventType)
				case <-time.After(time.Second):
					t.Fatal("provider cancellation was not sent")
				}
				require.Equal(t, "error", <-client.writes)
				require.NoError(t, <-dispatchDone)
			} else {
				require.NoError(t, <-dispatchDone)
				select {
				case <-client.closed:
				case <-time.After(time.Second):
					t.Fatal("safety close was not written")
				}
			}
			select {
			case eventType := <-client.writes:
				t.Fatalf("unexpected stale client event %q", eventType)
			default:
			}
			cancel()
			select {
			case <-writerDone:
			case <-time.After(time.Second):
				t.Fatal("client writer did not stop")
			}
			select {
			case <-voiceDone:
			case <-time.After(time.Second):
				t.Fatal("voice writer did not stop")
			}
		})
	}
}

func TestDispatchPolicySuppressionDoesNotWaitForAllowedSpeechSend(t *testing.T) {
	cfg, err := normalizeManagerConfig(Config{IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	providerErr := errors.New("provider rejected presentation")
	voice := &failureVoice{sendErr: make(chan error, 1), receiveErr: make(chan error)}
	voice.sendErr <- providerErr
	state := newRunState(cfg, IDs{SessionID: "session", ConversationID: "conversation", Incarnation: 1}, voice, newChannelClient(1), cancel)
	voiceDone := make(chan error, 1)
	go func() { voiceDone <- state.voiceWriter(ctx) }()
	command := realtime.Command{Kind: realtime.CommandCreateResponse, Sequence: 1}
	err = state.dispatchEffects(ctx, []Effect{{VoiceCommand: &command}, {PolicySuppressed: true}}, 0, 0)
	require.NoError(t, err)
	require.ErrorIs(t, <-voiceDone, providerErr)
}

type sessionShadowGates map[string]bool

func (g sessionShadowGates) Enabled(name string) bool { return g[name] }

func TestDispatchShadowRechecksVisualAndSpeechBoundariesSeparately(t *testing.T) {
	gates := sessionShadowGates{feature.ShadowMode: true}
	cfg, err := normalizeManagerConfig(Config{IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second, FeatureGates: gates})
	require.NoError(t, err)
	state := newRunState(cfg, testIDs(), &recordingVoice{sent: make(chan realtime.Command, 1)}, newChannelClient(1), func() {})
	visual := protocol.LiveHandoffCompletedEvent{Type: "live.handoff.completed", EventID: "event", HandoffID: "handoff", Status: "succeeded"}
	speech := realtime.Command{Kind: realtime.CommandCreateResponse, Sequence: 1, Epoch: 2}
	require.NoError(t, state.dispatchEffects(t.Context(), []Effect{
		{ClientEvent: visual, RequireClientWrite: true, HandoffPresentation: true},
		{VoiceCommand: &speech, HandoffPresentation: true},
	}, 0, 0))
	select {
	case got := <-state.clientCtl:
		t.Fatalf("shadow visual reached client boundary: %#v", got)
	default:
	}
	select {
	case got := <-state.voiceCtl:
		t.Fatalf("shadow speech reached provider boundary: %#v", got)
	default:
	}
}

func TestShadowSuppressedVoiceCompletionPreservesExactOutstandingCount(t *testing.T) {
	gates := sessionShadowGates{feature.ShadowMode: true}
	cfg, err := normalizeManagerConfig(Config{
		ControlEvents: 1, WriterControlEvents: 1,
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second, FeatureGates: gates,
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	state := newRunState(cfg, testIDs(), &recordingVoice{sent: make(chan realtime.Command, 1)}, newChannelClient(1), cancel)
	done := make(chan error, 1)
	go func() { done <- state.reactorLoop(ctx) }()
	command := realtime.Command{Kind: realtime.CommandCreateResponse, Sequence: 1, Epoch: 2}
	_, err = state.dispatchEffectsQueued(ctx, []Effect{{VoiceCommand: &command, HandoffPresentation: true}}, 0, 0, true)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return state.pendingVoice.Load() == 0 }, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func handoffAcceptanceEffectsForShadowTest(t *testing.T) (*Reactor, HandoffAcceptedInput, []Effect) {
	t.Helper()
	reactor, _ := testReactor(2)
	epoch := startStreaming(t, reactor, "response-manager-shadow")
	turn := identity.TurnKey{ID: "turn-manager-shadow", Revision: 1}
	applyOK(t, reactor, TurnCommittedInput{Sequence: 1, Turn: turn, Transcript: "request"})
	applyOK(t, reactor, currentVoice(reactor, backgroundToolEvent(epoch, "response-manager-shadow", "call-manager-shadow", json.RawMessage(`{"objective":"request"}`))))
	applyOK(t, reactor, currentVoice(reactor, realtime.Event{Kind: realtime.EventResponseDone, Epoch: epoch, ResponseID: "response-manager-shadow", Status: "completed"}))
	input := HandoffAcceptedInput{OriginEpoch: epoch, Turn: turn, ResponseID: "response-manager-shadow", CallID: "call-manager-shadow", Accepted: true, HandoffID: "handoff-manager-shadow"}
	effects := applyOK(t, reactor, input)
	markHandoffPresentationEffects(input, effects, 0)
	return reactor, input, effects
}

func TestShadowFromStartSuppressesEveryHandoffAcceptanceReinjectionAndReconcilesState(t *testing.T) {
	gates := feature.New(map[string]bool{feature.HandoffsEnabled: true, feature.ShadowMode: true})
	reactor, accepted, effects := handoffAcceptanceEffectsForShadowTest(t)
	voice := &recordingVoice{sent: make(chan realtime.Command, 4)}
	client := newChannelClient(4)
	cfg, err := normalizeManagerConfig(Config{IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second, FeatureGates: gates})
	require.NoError(t, err)
	state := newRunState(cfg, testIDs(), voice, client, func() {})
	err = state.dispatchEffects(t.Context(), effects, 0, 0)
	require.NoError(t, err)
	for len(state.completions) != 0 {
		message := <-state.completions
		completed, ok := message.input.(VoiceCommandCompletedInput)
		require.True(t, ok)
		applyOK(t, reactor, completed)
	}
	require.Equal(t, ResponseTerminal, reactor.Snapshot().Response)
	require.Empty(t, applyOK(t, reactor, accepted), "acceptance durable state must remain resolved")
	select {
	case command := <-state.voiceCtl:
		t.Fatalf("handoff acceptance reached provider queue in shadow: %#v", command)
	default:
	}
	select {
	case event := <-state.clientCtl:
		t.Fatalf("handoff acceptance reached client queue in shadow: %#v", event)
	default:
	}
	normal := applyOK(t, reactor, ClientInput{Event: protocol.ResponseCreateEvent{Type: "response.create"}})
	require.Len(t, normal, 1)
}

func TestQueuedHandoffAcceptanceSuppressionReconcilesBeforeProviderSend(t *testing.T) {
	gates := feature.New(map[string]bool{feature.HandoffsEnabled: true, feature.ShadowMode: false})
	reactor, _, effects := handoffAcceptanceEffectsForShadowTest(t)
	voice := &recordingVoice{sent: make(chan realtime.Command, 4)}
	cfg, err := normalizeManagerConfig(Config{IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second, FeatureGates: gates})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	state := newRunState(cfg, testIDs(), voice, newChannelClient(4), cancel)
	require.NoError(t, state.dispatchEffects(ctx, effects, 0, 0))
	require.True(t, gates.Apply(feature.Snapshot{Version: 1, Values: map[string]bool{feature.HandoffsEnabled: true, feature.ShadowMode: true}}))
	done := make(chan error, 1)
	go func() { done <- state.voiceWriter(ctx) }()
	for range 2 {
		message := <-state.completions
		completed, ok := message.input.(VoiceCommandCompletedInput)
		require.True(t, ok)
		require.True(t, completed.Suppressed)
		applyOK(t, reactor, completed)
	}
	require.Equal(t, ResponseTerminal, reactor.Snapshot().Response)
	select {
	case command := <-voice.sent:
		t.Fatalf("queued handoff acceptance reached provider after shadow enable: %#v", command)
	default:
	}
	normal := applyOK(t, reactor, ClientInput{Event: protocol.ResponseCreateEvent{Type: "response.create"}})
	require.Len(t, normal, 1)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestManagerConfigDefaultsAndBounds(t *testing.T) {
	connector := testkit.NewFakeConnector(1)
	ids := managerIDs{session: "session-1", conversation: "conversation-1"}
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
	}, connector, ids)
	require.NoError(t, manager.validationErr)
	require.Equal(t, 25, manager.cfg.IngressFrames)
	require.Equal(t, 75, manager.cfg.EgressFrames)
	require.Equal(t, 256, manager.cfg.ControlEvents)
	require.Equal(t, 64, manager.cfg.WriterControlEvents)

	aboveDefaults := NewManager(Config{
		IngressFrames:       26,
		EgressFrames:        76,
		ControlEvents:       257,
		WriterControlEvents: 65,
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
	}, connector, ids)
	require.NoError(t, aboveDefaults.validationErr)

	tooLarge := NewManager(Config{
		IngressFrames:       10_001,
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
	}, connector, ids)
	require.ErrorIs(t, tooLarge.validationErr, ErrInvalidManagerConfig)
}

func TestManagerReconnectRefreshCadenceIsBoundedByCredentialTTL(t *testing.T) {
	manual := clock.NewManual(time.Now().UTC())
	credentials, err := NewReconnectCredentials([]byte(strings.Repeat("r", 32)), manual)
	require.NoError(t, err)
	base := Config{
		IngressBlockTimeout:    time.Second,
		EgressBlockTimeout:     time.Second,
		ReconnectCredentials:   credentials,
		ReconnectOwners:        NewMemoryReconnectOwners(manual),
		ReconnectOwnerID:       "replica-refresh-cadence",
		ReconnectCredentialTTL: 10 * time.Second,
		Clock:                  manual,
	}

	normalized, err := normalizeManagerConfig(base)
	require.NoError(t, err)
	require.LessOrEqual(t, normalized.ReconnectOwnerHeartbeatInterval, normalized.ReconnectCredentialTTL/6,
		"the default maintenance cadence must leave a bounded refresh margin before the credential expires")

	base.ReconnectOwnerHeartbeatInterval = 2 * time.Second
	_, err = normalizeManagerConfig(base)
	require.ErrorIs(t, err, ErrInvalidManagerConfig,
		"an explicit cadence that can miss the refresh margin must fail closed")
}

func TestManagerOddCommitKeepsTailForTheNextAppend(t *testing.T) {
	connector := testkit.NewFakeConnector(8)
	manager := NewManager(withInitialManualInputTurn(Config{
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
	}), connector, managerIDs{session: "session-odd", conversation: "conversation-odd"})
	client := newChannelClient(8)
	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() {
		runResult <- manager.Run(ctx, OpenRequest{
			Subject: Subject{TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour)},
		}, client)
	}()
	voice, err := connector.NextSession(context.Background())
	require.NoError(t, err)
	activateInitialManualInputTurn(t, manager, "session-odd", 1, voice)

	client.reads <- protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}
	client.reads <- protocol.InputAudioBufferAppendEvent{
		Type: "input_audio_buffer.append", Audio: base64.StdEncoding.EncodeToString([]byte{0x12}),
	}
	client.reads <- protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}
	client.reads <- protocol.InputAudioBufferAppendEvent{
		Type: "input_audio_buffer.append", Audio: base64.StdEncoding.EncodeToString([]byte{0x34}),
	}
	client.reads <- protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}

	commandCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	appendCommand, err := voice.NextCommand(commandCtx)
	require.NoError(t, err)
	require.Equal(t, realtime.CommandAppendAudio, appendCommand.Kind)
	require.Equal(t, []byte{0x12, 0x34}, appendCommand.Audio)
	require.Equal(t, uint64(1), appendCommand.Sequence)
	commit, err := voice.NextCommand(commandCtx)
	require.NoError(t, err)
	require.Equal(t, realtime.CommandCommitAudio, commit.Kind)
	require.Equal(t, uint64(1), commit.AfterSeq)
	var commitErrors []protocol.ErrorEvent
	for len(commitErrors) < 2 {
		select {
		case event := <-client.writes:
			if invalid, ok := event.(protocol.ErrorEvent); ok {
				commitErrors = append(commitErrors, invalid)
			}
		case <-commandCtx.Done():
			t.Fatal("timed out waiting for invalid commit errors")
		}
	}
	require.Equal(t, "invalid_audio_commit", commitErrors[0].Error.Code)
	require.Equal(t, "invalid_audio_commit", commitErrors[1].Error.Code)
	require.NotEqual(t, commitErrors[0].EventID, commitErrors[1].EventID)

	cancel()
	require.ErrorIs(t, <-runResult, context.Canceled)
}

func TestManagerNormalizesProviderChunksAndOrdersAudioDoneAfterFrames(t *testing.T) {
	connector := testkit.NewFakeConnector(16)
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
	}, connector, managerIDs{session: "session-output", conversation: "conversation-output"})
	client := newChannelClient(16)
	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() {
		runResult <- manager.Run(ctx, OpenRequest{
			Subject: Subject{TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour)},
		}, client)
	}()
	voice, err := connector.NextSession(context.Background())
	require.NoError(t, err)

	client.reads <- protocol.ResponseCreateEvent{Type: "response.create"}
	commandCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	create, err := voice.NextCommand(commandCtx)
	require.NoError(t, err)
	require.Equal(t, realtime.CommandCreateResponse, create.Kind)
	require.NotZero(t, create.Epoch)

	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventResponseCreated, Epoch: create.Epoch,
		ProviderEventID: "provider-created", ResponseID: "response-1", Status: "in_progress",
	}))
	pcm := make([]byte, 1_442)
	for index := range pcm {
		pcm[index] = byte(index)
	}
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventAudioDelta, Epoch: create.Epoch,
		ResponseID: "response-1", ItemID: "item-1", Audio: pcm[:481],
	}))
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventAudioDelta, Epoch: create.Epoch,
		ResponseID: "response-1", ItemID: "item-1", Audio: pcm[481:962],
	}))
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventAudioDelta, Epoch: create.Epoch,
		ResponseID: "response-1", ItemID: "item-1", Audio: pcm[962:],
	}))
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventAudioDone, Epoch: create.Epoch,
		ProviderEventID: "provider-audio-done", ResponseID: "response-1", ItemID: "item-1",
	}))

	var types []string
	var gotPCM []byte
	var audioEvents []protocol.ResponseOutputAudioDeltaEvent
	for len(types) < 4 {
		select {
		case event := <-client.writes:
			if event.EventType() == "session.created" {
				continue
			}
			types = append(types, event.EventType())
			if audio, ok := event.(protocol.ResponseOutputAudioDeltaEvent); ok {
				decoded, decodeErr := base64.StdEncoding.DecodeString(audio.Delta)
				require.NoError(t, decodeErr)
				require.LessOrEqual(t, len(decoded), 960)
				require.NotEmpty(t, audio.EventID)
				require.NotEmpty(t, audio.ResponseID)
				require.NotEmpty(t, audio.ItemID)
				gotPCM = append(gotPCM, decoded...)
				audioEvents = append(audioEvents, audio)
			}
		case <-commandCtx.Done():
			t.Fatal("timed out waiting for normalized output")
		}
	}
	require.Equal(t, []string{
		"response.created",
		"response.output_audio.delta",
		"response.output_audio.delta",
		"response.output_audio.done",
	}, types[:4])
	require.Len(t, audioEvents, 2)
	require.NotEqual(t, audioEvents[0].EventID, audioEvents[1].EventID)
	require.True(t, bytes.Equal(pcm, gotPCM))

	cancel()
	require.ErrorIs(t, <-runResult, context.Canceled)
}

func TestManagerCancelsInFlightStaleAudioBeforeBargeWriteCanRecord(t *testing.T) {
	connector := testkit.NewFakeConnector(16)
	manager := NewManager(withInitialProviderVADInputTurn(Config{
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
	}), connector, managerIDs{session: "session-barge", conversation: "conversation-barge"})
	client := newGateAudioClient(16)
	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() {
		runResult <- manager.Run(ctx, OpenRequest{
			Subject: Subject{TenantID: "trusted-tenant", UserID: "trusted-user", ExpiresAt: time.Now().Add(time.Hour)},
		}, client)
	}()
	voice, err := connector.NextSession(context.Background())
	require.NoError(t, err)
	activateInitialProviderVADInputTurn(t, manager, "session-barge", 1, voice)
	commandCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()

	client.reads <- protocol.ResponseCreateEvent{Type: "response.create"}
	create, err := voice.NextCommand(commandCtx)
	require.NoError(t, err)
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventResponseCreated, Epoch: create.Epoch,
		ResponseID: "response-barge", ProviderEventID: "created-barge", Status: "in_progress",
	}))
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventAudioDelta, Epoch: create.Epoch,
		ResponseID: "response-barge", ItemID: "item-output", Audio: make([]byte, 960),
	}))

	select {
	case <-client.audioStarted:
	case <-commandCtx.Done():
		t.Fatal("audio write did not reach gate")
	}
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventAudioDelta, Epoch: create.Epoch,
		ResponseID: "response-barge", ItemID: "item-output", Audio: bytes.Repeat([]byte{0x7f}, 960),
	}))
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventSpeechStarted, ProviderEventID: "speech-started", ItemID: "item-input",
	}))

	for {
		select {
		case event := <-client.writes:
			if event.EventType() == "input_audio_buffer.speech_started" {
				close(client.audioRelease)
				client.mu.Lock()
				require.Empty(t, client.recordedAudio)
				client.mu.Unlock()
				cancel()
				require.ErrorIs(t, <-runResult, context.Canceled)
				return
			}
		case <-commandCtx.Done():
			t.Fatal("timed out waiting for barge-in notification")
		}
	}
}

// TestSpeechStartedAdvancesFenceWhileEarlierVisualWriteIsBlocked catches a
// reactor loop that waits for exact client I/O before applying barge-in.
func TestSpeechStartedAdvancesFenceWhileEarlierVisualWriteIsBlocked(t *testing.T) {
	connector := testkit.NewFakeConnector(16)
	manager := NewManager(withInitialProviderVADInputTurn(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
	}), connector, managerIDs{session: "session-async-visual", conversation: "conversation-async-visual"})
	require.NoError(t, manager.validationErr)
	client := newDeliveryClient()
	client.visualRelease = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- manager.Run(ctx, OpenRequest{Subject: Subject{
			TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour),
		}}, client)
	}()
	voice, err := connector.NextSession(t.Context())
	require.NoError(t, err)
	activateInitialProviderVADInputTurn(t, manager, "session-async-visual", 1, voice)
	require.Eventually(t, func() bool { return manager.ActiveSessions() == 1 }, time.Second, time.Millisecond)

	manager.activeMu.RLock()
	var runtime *sessionRuntime
	for _, active := range manager.active {
		runtime = active
	}
	manager.activeMu.RUnlock()
	require.NotNil(t, runtime)
	require.NoError(t, manager.Submit(t.Context(), runtime.key.sessionID, runtime.key.incarnation, ClientInput{
		Event: protocol.ResponseCreateEvent{Type: "response.create"},
	}))
	command, err := voice.NextCommand(t.Context())
	require.NoError(t, err)
	require.NoError(t, voice.Emit(t.Context(), realtime.Event{
		Kind: realtime.EventResponseCreated, Epoch: command.Epoch, ResponseID: "response-async-visual", Status: "in_progress",
	}))
	require.Eventually(t, func() bool { return runtime.state.fence.Current() == command.Epoch }, time.Second, time.Millisecond)

	offered := make(chan error, 1)
	go func() {
		offered <- manager.OfferHandoffEvent(context.Background(), runtime.handoffScope(), handoff.Event{
			Seq: 1, EventID: "visual-blocked", HandoffID: "handoff", TenantID: "tenant",
			ConversationID: runtime.state.ids.ConversationID, Kind: "tool.approval_required", State: handoff.StateNeedsInput,
			Payload: json.RawMessage(`{"invocation_id":"invoke","handoff_id":"handoff","action_summary":"approve","expires_at":"2099-01-01T00:00:00Z","risk_class":"low"}`),
		})
	}()
	select {
	case <-client.visualStarted:
	case err := <-offered:
		t.Fatalf("offer failed before visual write: %v", err)
	case <-time.After(time.Second):
		t.Fatal("visual write did not reach gate")
	}
	require.NoError(t, voice.Emit(t.Context(), realtime.Event{
		Kind: realtime.EventSpeechStarted, ProviderEventID: "speech-async-visual", ItemID: "new-input",
	}))
	require.Eventually(t, func() bool { return runtime.state.fence.Current() > command.Epoch }, 100*time.Millisecond, time.Millisecond)

	close(client.visualRelease)
	select {
	case <-offered:
	case <-time.After(time.Second):
		t.Fatal("offer did not finish after visual release")
	}
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestSpeechStartedAdvancesFenceWhileEarlierProviderSendIsBlocked(t *testing.T) {
	cfg, err := normalizeManagerConfig(withInitialProviderVADInputTurn(Config{ControlEvents: 2, WriterControlEvents: 2, IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second}))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	voice := &gatedProviderVoice{started: make(chan realtime.Command, 2), release: make(chan struct{})}
	state := newRunState(cfg, testIDs(), voice, newChannelClient(8), cancel)
	reactorDone := make(chan error, 1)
	writerDone := make(chan error, 1)
	go func() { reactorDone <- state.reactorLoop(ctx) }()
	go func() { writerDone <- state.voiceWriter(ctx) }()
	require.NoError(t, state.enqueueReactorSync(t.Context(), reactorMessage{input: ConnectedInput{ProviderSession: json.RawMessage(`{}`)}}))
	require.NoError(t, state.enqueueReactorSync(t.Context(), reactorMessage{input: VoiceInput{Incarnation: state.ids.Incarnation, Event: realtime.Event{
		Kind: realtime.EventSessionUpdated, InputTurnMode: realtime.InputTurnModeProviderVAD, InputTurnModeSpecified: true,
		SessionUpdateAcknowledgement: true,
	}}}))
	require.NoError(t, state.enqueueReactorSync(t.Context(), reactorMessage{input: ClientInput{Event: protocol.ResponseCreateEvent{Type: "response.create"}}}))
	var create realtime.Command
	select {
	case create = <-voice.started:
		require.Equal(t, realtime.CommandCreateResponse, create.Kind)
	case <-time.After(time.Second):
		t.Fatal("provider send did not reach gate")
	}
	require.NoError(t, state.enqueueReactorSync(t.Context(), reactorMessage{input: VoiceInput{Incarnation: state.ids.Incarnation, Event: realtime.Event{
		Kind: realtime.EventSpeechStarted, ProviderEventID: "speech-provider-blocked", ItemID: "new-input",
	}}}))
	require.Greater(t, state.fence.Current(), create.Epoch)
	close(voice.release)
	cancel()
	require.ErrorIs(t, <-reactorDone, context.Canceled)
	require.ErrorIs(t, <-writerDone, context.Canceled)
}

func TestSpeechStartedAdvancesFenceWhilePresentationSafetyIsBlocked(t *testing.T) {
	connector := testkit.NewFakeConnector(16)
	policy := &blockingContextSafety{started: make(chan struct{}), cancelled: make(chan struct{})}
	manager := NewManager(withInitialProviderVADInputTurn(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: 20 * time.Millisecond,
		HandoffConsumerID: "public-client", HandoffSubscriptions: &signalingSubscriptions{},
		PresentationPolicy: presentation.NewPolicy(presentation.Config{}), SafetyPolicy: policy,
	}), connector, managerIDs{session: "session-async-safety", conversation: "conversation-async-safety"})
	require.NoError(t, manager.validationErr)
	client := newDeliveryClient()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- manager.Run(ctx, OpenRequest{Subject: Subject{
			TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour),
		}}, client)
	}()
	voice, err := connector.NextSession(t.Context())
	require.NoError(t, err)
	activateInitialProviderVADInputTurn(t, manager, "session-async-safety", 1, voice)
	require.Eventually(t, func() bool { return manager.ActiveSessions() == 1 }, time.Second, time.Millisecond)

	manager.activeMu.RLock()
	var runtime *sessionRuntime
	for _, active := range manager.active {
		runtime = active
	}
	manager.activeMu.RUnlock()
	require.NotNil(t, runtime)
	require.NoError(t, manager.Submit(t.Context(), runtime.key.sessionID, runtime.key.incarnation, ClientInput{
		Event: protocol.ResponseCreateEvent{Type: "response.create"},
	}))
	command, err := voice.NextCommand(t.Context())
	require.NoError(t, err)
	require.NoError(t, voice.Emit(t.Context(), realtime.Event{
		Kind: realtime.EventResponseCreated, Epoch: command.Epoch, ResponseID: "response-async-safety", Status: "in_progress",
	}))
	require.Eventually(t, func() bool { return runtime.state.fence.Current() == command.Epoch }, time.Second, time.Millisecond)

	offered := make(chan error, 1)
	go func() {
		offered <- manager.OfferHandoffEvent(context.Background(), runtime.handoffScope(), handoff.Event{
			Seq: 1, EventID: "safety-blocked", HandoffID: "handoff", TenantID: "tenant",
			ConversationID: runtime.state.ids.ConversationID, Kind: "handoff.needs-input", State: handoff.StateNeedsInput,
			Payload: json.RawMessage(`{"Prompt":"choose","Schema":{}}`),
		})
	}()
	select {
	case <-policy.started:
	case <-time.After(time.Second):
		t.Fatal("presentation safety did not block")
	}
	require.NoError(t, voice.Emit(t.Context(), realtime.Event{
		Kind: realtime.EventSpeechStarted, ProviderEventID: "speech-async-safety", ItemID: "new-input",
	}))
	require.Eventually(t, func() bool { return runtime.state.fence.Current() > command.Epoch }, 100*time.Millisecond, time.Millisecond)

	require.ErrorIs(t, <-offered, ErrControlOverflow)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	select {
	case <-policy.cancelled:
	case <-time.After(time.Second):
		t.Fatal("presentation safety did not stop with session")
	}
}

func TestResponseCreateWaitsForBlockedBargeCancelCompletion(t *testing.T) {
	cfg, err := normalizeManagerConfig(withInitialProviderVADInputTurn(Config{ControlEvents: 1, IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second}))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	voice := &gatedCancelVoice{sent: make(chan realtime.Command, 8), cancelStarted: make(chan struct{}), cancelRelease: make(chan struct{})}
	state := newRunState(cfg, testIDs(), voice, newChannelClient(8), cancel)
	reactorDone := make(chan error, 1)
	writerDone := make(chan error, 1)
	go func() { reactorDone <- state.reactorLoop(ctx) }()
	go func() { writerDone <- state.voiceWriter(ctx) }()
	require.NoError(t, state.enqueueReactorSync(t.Context(), reactorMessage{input: ConnectedInput{ProviderSession: json.RawMessage(`{}`)}}))
	require.NoError(t, state.enqueueReactorSync(t.Context(), reactorMessage{input: VoiceInput{Incarnation: state.ids.Incarnation, Event: realtime.Event{
		Kind: realtime.EventSessionUpdated, InputTurnMode: realtime.InputTurnModeProviderVAD, InputTurnModeSpecified: true,
		SessionUpdateAcknowledgement: true,
	}}}))
	require.NoError(t, state.enqueueReactorSync(t.Context(), reactorMessage{input: ClientInput{Event: protocol.ResponseCreateEvent{Type: "response.create"}}}))
	first := <-voice.sent
	require.Equal(t, realtime.CommandCreateResponse, first.Kind)
	require.Eventually(t, func() bool { return state.pendingVoice.Load() == 0 }, time.Second, time.Millisecond)
	require.NoError(t, state.enqueueReactorSync(t.Context(), reactorMessage{input: VoiceInput{Incarnation: state.ids.Incarnation, Event: realtime.Event{
		Kind: realtime.EventResponseCreated, Epoch: first.Epoch, ResponseID: "response-before-barge", Status: "in_progress",
	}}}))
	require.NoError(t, state.enqueueReactorSync(t.Context(), reactorMessage{input: VoiceInput{Incarnation: state.ids.Incarnation, Event: realtime.Event{
		Kind: realtime.EventSpeechStarted, ItemID: "new-input",
	}}}))
	select {
	case <-voice.cancelStarted:
	case <-time.After(time.Second):
		t.Fatal("cancel send did not reach gate")
	}
	require.Equal(t, realtime.CommandCancelResponse, (<-voice.sent).Kind)
	createDone := make(chan error, 1)
	require.NoError(t, state.enqueueReactor(reactorMessage{input: ClientInput{Event: protocol.ResponseCreateEvent{Type: "response.create"}}, done: createDone}))
	select {
	case err := <-createDone:
		t.Fatalf("response.create completed before cancel: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(voice.cancelRelease)
	select {
	case err := <-createDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("response.create did not resume after cancel completion")
	}
	second := <-voice.sent
	require.Equal(t, realtime.CommandCreateResponse, second.Kind)
	cancel()
	require.ErrorIs(t, <-reactorDone, context.Canceled)
	require.ErrorIs(t, <-writerDone, context.Canceled)
}

func TestClientWriterPublishesTypedCompletionOnReservedLane(t *testing.T) {
	cfg, err := normalizeManagerConfig(Config{ControlEvents: 1, WriterControlEvents: 1, IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := newChannelClient(2)
	state := newRunState(cfg, testIDs(), &recordingVoice{sent: make(chan realtime.Command, 1)}, client, cancel)
	<-state.inboxSlots
	done := make(chan error, 1)
	go func() { done <- state.clientWriter(ctx) }()
	_, err = state.dispatchEffectsQueued(ctx, []Effect{{ClientEvent: protocol.SessionCreatedEvent{Type: "session.created"}}}, 0, 0, true)
	require.NoError(t, err)
	select {
	case <-client.writes:
	case <-time.After(time.Second):
		t.Fatal("client effect was not written")
	}
	select {
	case completion := <-state.completions:
		typed, ok := completion.input.(ClientEffectCompletedInput)
		require.True(t, ok)
		require.NotZero(t, typed.Sequence)
		require.NoError(t, typed.Err)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("client writer did not publish a typed reserved completion")
	}
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestClientWriterPublishesInputTurnModeBeforeSessionUpdated(t *testing.T) {
	cfg, err := normalizeManagerConfig(Config{
		ControlEvents: 1, WriterControlEvents: 1,
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &inputModeBoundaryClient{
		channelClient: newChannelClient(2),
		observed:      make(chan realtime.InputTurnMode, 1),
	}
	state := newRunState(cfg, testIDs(), &recordingVoice{sent: make(chan realtime.Command, 1)}, client, cancel)
	client.current = state.currentInputTurnMode
	done := make(chan error, 1)
	go func() { done <- state.clientWriter(ctx) }()

	_, err = state.dispatchEffectsQueued(ctx, []Effect{{
		ClientEvent: protocol.SessionUpdatedEvent{
			Type: "session.updated", Session: json.RawMessage(`{"type":"realtime"}`),
		},
		InputTurnModeBoundary: realtime.InputTurnModeManual,
	}}, 0, 0, false)
	require.NoError(t, err)
	select {
	case observed := <-client.observed:
		require.Equal(t, realtime.InputTurnModeManual, observed,
			"the confirmed input mode must publish before session.updated becomes client-observable")
	case <-time.After(time.Second):
		t.Fatal("session.updated was not written")
	}
	require.Equal(t, realtime.InputTurnModeManual, state.currentInputTurnMode())

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestDurableWriterPublishesTypedCompletionOnReservedLane(t *testing.T) {
	cfg, err := normalizeManagerConfig(Config{ControlEvents: 1, WriterControlEvents: 1, IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second})
	require.NoError(t, err)
	dispatcher := &durableDispatcherFake{
		trusted: make(chan TrustedSessionContext, 1), transcripts: make(chan TranscriptRequestedEffect, 1), handoffs: make(chan HandoffRequestedEffect, 1),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	state := newRunState(cfg, testIDs(), &recordingVoice{sent: make(chan realtime.Command, 1)}, newChannelClient(1), cancel)
	state.dispatcher = dispatcher
	<-state.inboxSlots
	done := make(chan error, 1)
	go func() { done <- state.durableWriter(ctx) }()
	transcript := TranscriptRequestedEffect{Turn: identity.TurnKey{ID: "turn", Revision: 1}, Sequence: 3, Role: "assistant", FinalText: "done"}
	_, err = state.dispatchEffectsQueued(ctx, []Effect{{Transcript: &transcript}}, 0, 0, true)
	require.NoError(t, err)
	select {
	case <-dispatcher.transcripts:
	case <-time.After(time.Second):
		t.Fatal("durable effect was not dispatched")
	}
	select {
	case completion := <-state.completions:
		typed, ok := completion.input.(DurableEffectCompletedInput)
		require.True(t, ok)
		require.NotZero(t, typed.Sequence)
		require.Equal(t, "transcript", typed.Kind)
		require.NoError(t, typed.Err)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("durable writer did not publish a typed reserved completion")
	}
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestClientWriterPublishesTypedFailureCompletion(t *testing.T) {
	cfg, err := normalizeManagerConfig(Config{ControlEvents: 1, WriterControlEvents: 1, IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second})
	require.NoError(t, err)
	writeErr := errors.New("client write failed")
	client := &failureClient{reads: make(chan protocol.ClientEvent), readErr: make(chan error), writeErr: make(chan error, 1)}
	client.writeErr <- writeErr
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	state := newRunState(cfg, testIDs(), &recordingVoice{sent: make(chan realtime.Command, 1)}, client, cancel)
	done := make(chan error, 1)
	go func() { done <- state.clientWriter(ctx) }()
	_, err = state.dispatchEffectsQueued(ctx, []Effect{{ClientEvent: protocol.SessionCreatedEvent{Type: "session.created"}}}, 0, 0, true)
	require.NoError(t, err)
	completion := <-state.completions
	typed, ok := completion.input.(ClientEffectCompletedInput)
	require.True(t, ok)
	require.ErrorIs(t, typed.Err, writeErr)
	require.NoError(t, <-done, "writer failure is reconciled through the typed reactor input")
}

func TestDurableWriterPublishesTypedFailureCompletion(t *testing.T) {
	cfg, err := normalizeManagerConfig(Config{ControlEvents: 1, WriterControlEvents: 1, IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second})
	require.NoError(t, err)
	dispatchErr := errors.New("durable dispatch failed")
	dispatcher := &durableDispatcherFake{
		trusted: make(chan TrustedSessionContext, 1), transcripts: make(chan TranscriptRequestedEffect, 1),
		handoffs: make(chan HandoffRequestedEffect, 1), err: dispatchErr,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	state := newRunState(cfg, testIDs(), &recordingVoice{sent: make(chan realtime.Command, 1)}, newChannelClient(1), cancel)
	state.dispatcher = dispatcher
	done := make(chan error, 1)
	go func() { done <- state.durableWriter(ctx) }()
	transcript := TranscriptRequestedEffect{Turn: identity.TurnKey{ID: "turn", Revision: 1}, Sequence: 3, Role: "assistant", FinalText: "done"}
	_, err = state.dispatchEffectsQueued(ctx, []Effect{{Transcript: &transcript}}, 0, 0, true)
	require.NoError(t, err)
	completion := <-state.completions
	typed, ok := completion.input.(DurableEffectCompletedInput)
	require.True(t, ok)
	require.ErrorIs(t, typed.Err, dispatchErr)
	require.NoError(t, <-done, "dispatcher failure is reconciled through the typed reactor input")
}

func TestManagerTransportFailureClosesOnceUnregistersAndPreservesCause(t *testing.T) {
	connector := newLifecycleConnector()
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
	}, connector, managerIDs{session: "session-lifecycle", conversation: "conversation-lifecycle"})
	client := newLifecycleClient()
	scopes := map[string]struct{}{"live": {}}
	permissions := json.RawMessage(`{"allow":true}`)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runResult := make(chan error, 1)
	go func() {
		runResult <- manager.Run(ctx, OpenRequest{Subject: Subject{
			TenantID: "trusted-tenant", UserID: "trusted-user", Scopes: scopes,
			Permissions: permissions, ExpiresAt: time.Now().Add(time.Hour),
		}}, client)
	}()

	open := <-connector.connected
	require.Equal(t, identity.SessionID("session-lifecycle"), open.SessionID)
	require.Equal(t, identity.Incarnation(1), open.Incarnation)
	select {
	case <-client.created:
	case <-time.After(time.Second):
		t.Fatal("session did not become active")
	}

	delete(scopes, "live")
	permissions[2] = 'X'
	var runtime *sessionRuntime
	require.Eventually(t, func() bool {
		manager.activeMu.RLock()
		runtime = manager.active[runtimeKey{sessionID: "session-lifecycle", incarnation: 1}]
		manager.activeMu.RUnlock()
		return runtime != nil
	}, time.Second, time.Millisecond)
	require.NotNil(t, runtime)
	require.Contains(t, runtime.subject.Scopes, "live")
	require.JSONEq(t, `{"allow":true}`, string(runtime.subject.Permissions))

	spoof := protocol.ResponseCreateEvent{
		Type: "response.create",
		Response: &protocol.ResponseConfig{
			Metadata: json.RawMessage(`{"tenant_id":"evil-tenant","user_id":"evil-user"}`),
		},
	}
	require.NoError(t, manager.Submit(context.Background(), "session-lifecycle", 1, ClientInput{Event: spoof}))
	select {
	case <-connector.voice.sending:
	case <-time.After(time.Second):
		t.Fatal("spoofing client metadata was not processed")
	}
	require.Equal(t, "trusted-tenant", runtime.subject.TenantID)
	require.Equal(t, "trusted-user", runtime.subject.UserID)
	require.NoError(t, manager.Submit(context.Background(), "session-lifecycle", 1, DurableInput{Sequence: 1, Kind: "noop"}))
	require.ErrorIs(t, manager.Submit(context.Background(), "session-lifecycle", 2, DurableInput{Sequence: 2}), ErrSessionNotActive)

	transportErr := errors.New("provider receive failed")
	connector.voice.failure <- transportErr
	err := <-runResult
	require.ErrorIs(t, err, transportErr)
	require.Equal(t, int32(1), connector.voice.closes.Load())
	require.True(t, connector.voice.closeLive.Load())
	require.Equal(t, int32(1), client.closes.Load())
	require.ErrorIs(t, manager.Submit(context.Background(), "session-lifecycle", 1, DurableInput{Sequence: 2}), ErrSessionNotActive)
	manager.activeMu.RLock()
	require.Empty(t, manager.active)
	manager.activeMu.RUnlock()
}

func TestManagerKeepsPCMFramersIsolatedAcrossConcurrentSessions(t *testing.T) {
	type running struct {
		cancel context.CancelFunc
		done   chan error
		client *channelClient
		voice  *testkit.FakeVoiceSession
	}
	start := func(sessionID string, sentinel byte) running {
		connector := testkit.NewFakeConnector(8)
		manager := NewManager(withInitialManualInputTurn(Config{
			IngressBlockTimeout: time.Second,
			EgressBlockTimeout:  time.Second,
		}), connector, managerIDs{session: identity.SessionID(sessionID), conversation: identity.ConversationID("conversation-" + sessionID)})
		client := newChannelClient(4)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- manager.Run(ctx, OpenRequest{Subject: Subject{
				TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour),
			}}, client)
		}()
		voice, err := connector.NextSession(context.Background())
		require.NoError(t, err)
		activateInitialManualInputTurn(t, manager, identity.SessionID(sessionID), 1, voice)
		client.reads <- protocol.InputAudioBufferAppendEvent{
			Type: "input_audio_buffer.append", Audio: base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{sentinel}, 960)),
		}
		return running{cancel: cancel, done: done, client: client, voice: voice}
	}

	first := start("session-a", 0x11)
	second := start("session-b", 0x22)
	commandCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	firstCommand, err := first.voice.NextCommand(commandCtx)
	require.NoError(t, err)
	secondCommand, err := second.voice.NextCommand(commandCtx)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{0x11}, 960), firstCommand.Audio)
	require.Equal(t, bytes.Repeat([]byte{0x22}, 960), secondCommand.Audio)

	first.cancel()
	second.cancel()
	require.ErrorIs(t, <-first.done, context.Canceled)
	require.ErrorIs(t, <-second.done, context.Canceled)
}

func TestManagerIngressAcceptsCapacityAndFailsOnFirstExcessFrame(t *testing.T) {
	connector := newLifecycleConnector()
	connector.voice.allowImmediateSends(1)
	manager := NewManager(withInitialManualInputTurn(Config{
		IngressFrames:       1,
		IngressBlockTimeout: 20 * time.Millisecond,
		EgressBlockTimeout:  time.Second,
	}), connector, managerIDs{session: "session-ingress-full", conversation: "conversation-ingress-full"})
	client := newChannelClient(8)
	runCtx, runCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer runCancel()
	runResult := make(chan error, 1)
	go func() {
		runResult <- manager.Run(runCtx, OpenRequest{Subject: Subject{
			TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour),
		}}, client)
	}()
	<-connector.connected
	connector.voice.events <- realtime.Event{
		Kind: realtime.EventSessionUpdated, InputTurnMode: realtime.InputTurnModeManual, InputTurnModeSpecified: true,
		SessionUpdateAcknowledgement: true,
	}
	require.Eventually(t, func() bool {
		manager.activeMu.RLock()
		runtime := manager.active[runtimeKey{sessionID: "session-ingress-full", incarnation: 1}]
		manager.activeMu.RUnlock()
		return runtime != nil && runtime.state.currentInputTurnMode() == realtime.InputTurnModeManual
	}, time.Second, time.Millisecond)
	frame := base64.StdEncoding.EncodeToString(make([]byte, 960))
	client.reads <- protocol.InputAudioBufferAppendEvent{Type: "input_audio_buffer.append", Audio: frame}
	select {
	case <-connector.voice.sending:
	case <-time.After(time.Second):
		t.Fatal("voice writer did not consume the first frame")
	}
	client.reads <- protocol.InputAudioBufferAppendEvent{Type: "input_audio_buffer.append", Audio: frame}
	require.ErrorIs(t, <-runResult, ErrIngressBackpressure)
}

func TestManagerEgressAcceptsCapacityAndFailsOnFirstExcessFrame(t *testing.T) {
	connector := testkit.NewFakeConnector(16)
	manager := NewManager(Config{
		EgressFrames:        1,
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  20 * time.Millisecond,
	}, connector, managerIDs{session: "session-egress-full", conversation: "conversation-egress-full"})
	client := newGateAudioClient(16)
	runCtx, runCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer runCancel()
	runResult := make(chan error, 1)
	go func() {
		runResult <- manager.Run(runCtx, OpenRequest{Subject: Subject{
			TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour),
		}}, client)
	}()
	voice, err := connector.NextSession(context.Background())
	require.NoError(t, err)
	commandCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	client.reads <- protocol.ResponseCreateEvent{Type: "response.create"}
	create, err := voice.NextCommand(commandCtx)
	require.NoError(t, err)
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventResponseCreated, Epoch: create.Epoch,
		ResponseID: "response-egress", ProviderEventID: "created-egress", Status: "in_progress",
	}))
	frame := realtime.Event{
		Kind: realtime.EventAudioDelta, Epoch: create.Epoch,
		ResponseID: "response-egress", ItemID: "item-egress", Audio: make([]byte, 960),
	}
	require.NoError(t, voice.Emit(commandCtx, frame))
	select {
	case <-client.audioStarted:
	case <-commandCtx.Done():
		t.Fatal("client writer did not consume the first frame")
	}
	require.NoError(t, voice.Emit(commandCtx, frame))
	require.ErrorIs(t, <-runResult, ErrEgressBackpressure)
}

func TestManagerVoiceControlAcceptsCapacityAndFailsOnFirstExcessControl(t *testing.T) {
	connector := newLifecycleConnector()
	manager := NewManager(Config{
		WriterControlEvents: 1,
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
	}, connector, managerIDs{session: "session-voice-control", conversation: "conversation-voice-control"})
	client := newChannelClient(8)
	runCtx, runCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer runCancel()
	runResult := make(chan error, 1)
	go func() {
		runResult <- manager.Run(runCtx, OpenRequest{Subject: Subject{
			TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour),
		}}, client)
	}()
	<-connector.connected
	createItem := func(itemID string) protocol.ConversationItemCreateEvent {
		return protocol.ConversationItemCreateEvent{
			Type: "conversation.item.create",
			Item: protocol.ConversationItem{
				ID: itemID, Type: "message", Role: "assistant",
				Content: json.RawMessage(`[{"type":"text","text":"queued"}]`),
			},
		}
	}
	client.reads <- createItem("queued-a")
	select {
	case <-connector.voice.sending:
	case <-time.After(time.Second):
		t.Fatal("voice writer did not consume the first control")
	}
	client.reads <- createItem("queued-b")
	require.ErrorIs(t, <-runResult, ErrControlOverflow)
}

func TestManagerClientControlAcceptsCapacityAndFailsOnFirstExcessControl(t *testing.T) {
	connector := testkit.NewFakeConnector(8)
	manager := NewManager(withInitialProviderVADInputTurn(Config{
		WriterControlEvents: 1,
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
	}), connector, managerIDs{session: "session-client-control", conversation: "conversation-client-control"})
	client := newBlockingWriteClient()
	runCtx, runCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer runCancel()
	runResult := make(chan error, 1)
	go func() {
		runResult <- manager.Run(runCtx, OpenRequest{Subject: Subject{
			TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour),
		}}, client)
	}()
	voice, err := connector.NextSession(context.Background())
	require.NoError(t, err)
	activateInitialProviderVADInputTurn(t, manager, "session-client-control", 1, voice)
	commandCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventSpeechStarted, ProviderEventID: "speech-start", ItemID: "input-item",
	}))
	select {
	case <-client.started:
	case err := <-runResult:
		t.Fatalf("manager stopped before first post-ready event: %v", err)
	case <-time.After(time.Second):
		t.Fatal("client writer did not consume first post-ready event")
	}
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventSpeechStopped, ProviderEventID: "speech-stop", ItemID: "input-item",
	}))
	require.ErrorIs(t, <-runResult, ErrControlOverflow)
}

func TestManagerQueuesImplicitCreatedBeforeBindReleasesAudio(t *testing.T) {
	connector := testkit.NewFakeConnector(16)
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
	}, connector, managerIDs{session: "session-implicit", conversation: "conversation-implicit"})
	client := newChannelClient(16)
	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() {
		runResult <- manager.Run(ctx, OpenRequest{Subject: Subject{
			TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour),
		}}, client)
	}()
	voice, err := connector.NextSession(context.Background())
	require.NoError(t, err)
	commandCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventResponseCreated, NeedsEpochBinding: true,
		ProviderEventID: "implicit-created", ResponseID: "implicit-response", Status: "in_progress",
	}))
	bind, err := voice.NextCommand(commandCtx)
	require.NoError(t, err)
	require.Equal(t, realtime.CommandBindResponseEpoch, bind.Kind)
	require.Equal(t, "implicit-response", bind.ResponseID)
	require.NotZero(t, bind.Epoch)
	require.Zero(t, bind.AfterSeq)
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventAudioDelta, Epoch: bind.Epoch,
		ResponseID: "implicit-response", ItemID: "implicit-item", Audio: make([]byte, 960),
	}))
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventAudioDone, Epoch: bind.Epoch,
		ResponseID: "implicit-response", ItemID: "implicit-item", ProviderEventID: "implicit-audio-done",
	}))
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventResponseDone, Epoch: bind.Epoch,
		ResponseID: "implicit-response", ProviderEventID: "implicit-response-done", Status: "completed",
	}))

	var ordered []string
	for len(ordered) < 4 {
		select {
		case event := <-client.writes:
			if event.EventType() == "session.created" {
				continue
			}
			ordered = append(ordered, event.EventType())
		case <-commandCtx.Done():
			t.Fatal("timed out waiting for implicit response output")
		}
	}
	require.Equal(t, []string{
		"response.created", "response.output_audio.delta", "response.output_audio.done", "response.done",
	}, ordered)

	cancel()
	require.ErrorIs(t, <-runResult, context.Canceled)
}

func TestManagerRejectsOddProviderTailWithoutMixingTheNextResponse(t *testing.T) {
	connector := testkit.NewFakeConnector(16)
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
	}, connector, managerIDs{session: "session-odd-output", conversation: "conversation-odd-output"})
	client := newChannelClient(16)
	runResult := make(chan error, 1)
	go func() {
		runResult <- manager.Run(context.Background(), OpenRequest{Subject: Subject{
			TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour),
		}}, client)
	}()
	voice, err := connector.NextSession(context.Background())
	require.NoError(t, err)
	commandCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	client.reads <- protocol.ResponseCreateEvent{Type: "response.create"}
	create, err := voice.NextCommand(commandCtx)
	require.NoError(t, err)
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventResponseCreated, Epoch: create.Epoch,
		ResponseID: "odd-response", ProviderEventID: "odd-created", Status: "in_progress",
	}))
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventAudioDelta, Epoch: create.Epoch,
		ResponseID: "odd-response", ItemID: "odd-item", Audio: []byte{0x01},
	}))
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventAudioDone, Epoch: create.Epoch,
		ResponseID: "odd-response", ItemID: "odd-item",
	}))
	err = <-runResult
	require.ErrorContains(t, err, "split sample")

	for {
		select {
		case event := <-client.writes:
			_, audio := event.(protocol.ResponseOutputAudioDeltaEvent)
			require.False(t, audio, "odd output tail must never be forwarded")
		default:
			return
		}
	}
}

func TestManagerRejectsFirstByteOverProviderAudioEventLimit(t *testing.T) {
	connector := testkit.NewFakeConnector(8)
	manager := NewManager(Config{
		EgressFrames: 2, IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
	}, connector, managerIDs{session: "session-audio-limit", conversation: "conversation-audio-limit"})
	client := newChannelClient(8)
	runCtx, runCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer runCancel()
	runResult := make(chan error, 1)
	go func() {
		runResult <- manager.Run(runCtx, OpenRequest{Subject: Subject{
			TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour),
		}}, client)
	}()
	voice, err := connector.NextSession(context.Background())
	require.NoError(t, err)
	commandCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	client.reads <- protocol.ResponseCreateEvent{Type: "response.create"}
	create, err := voice.NextCommand(commandCtx)
	require.NoError(t, err)
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventResponseCreated, Epoch: create.Epoch,
		ResponseID: "limited-response", ProviderEventID: "limited-created", Status: "in_progress",
	}))
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventAudioDelta, Epoch: create.Epoch,
		ResponseID: "limited-response", ItemID: "limited-item", Audio: make([]byte, 2*960),
	}))
	for frames := 0; frames < 2; {
		select {
		case event := <-client.writes:
			if _, ok := event.(protocol.ResponseOutputAudioDeltaEvent); ok {
				frames++
			}
		case <-commandCtx.Done():
			t.Fatal("exact provider audio event limit was not accepted")
		}
	}
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventAudioDelta, Epoch: create.Epoch,
		ResponseID: "limited-response", ItemID: "limited-item", Audio: make([]byte, 2*960+1),
	}))
	require.ErrorContains(t, <-runResult, "provider audio event exceeds 1920 bytes")
}

func TestManagerStopsProviderBatchAtMidBatchInvalidation(t *testing.T) {
	connector := testkit.NewFakeConnector(16)
	manager := NewManager(Config{
		EgressFrames: 3, IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
	}, connector, managerIDs{session: "session-mid-batch", conversation: "conversation-mid-batch"})
	dropped := make(chan int, 4)
	manager.SetMetricsHooks(MetricsHooks{AudioDropped: func(bytes int) { dropped <- bytes }})
	client := newGateAudioClient(16)
	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() {
		runResult <- manager.Run(ctx, OpenRequest{Subject: Subject{
			TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour),
		}}, client)
	}()
	voice, err := connector.NextSession(context.Background())
	require.NoError(t, err)
	commandCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	client.reads <- protocol.ResponseCreateEvent{Type: "response.create"}
	create, err := voice.NextCommand(commandCtx)
	require.NoError(t, err)
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventResponseCreated, Epoch: create.Epoch,
		ResponseID: "mid-response", ProviderEventID: "mid-created", Status: "in_progress",
	}))
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventAudioDelta, Epoch: create.Epoch,
		ResponseID: "mid-response", ItemID: "mid-item", Audio: make([]byte, 3*960),
	}))
	select {
	case <-client.audioStarted:
	case <-commandCtx.Done():
		t.Fatal("first provider frame did not enter client write")
	}
	client.reads <- protocol.ResponseCancelEvent{Type: "response.cancel", ResponseID: "mid-response"}
	cancelCommand, err := voice.NextCommand(commandCtx)
	require.NoError(t, err)
	require.Equal(t, realtime.CommandCancelResponse, cancelCommand.Kind)
	require.Equal(t, 960, <-dropped)
	require.Equal(t, 960, <-dropped)
	select {
	case bytes := <-dropped:
		t.Fatalf("normalizer continued after invalidation and dropped an extra %d-byte frame", bytes)
	default:
	}
	close(client.audioRelease)
	cancel()
	require.ErrorIs(t, <-runResult, context.Canceled)
}

func TestManagerDiscardsInvalidatedProviderTailBeforeNextResponse(t *testing.T) {
	connector := testkit.NewFakeConnector(32)
	manager := NewManager(withInitialProviderVADInputTurn(Config{
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
	}), connector, managerIDs{session: "session-tail-barge", conversation: "conversation-tail-barge"})
	client := newChannelClient(32)
	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() {
		runResult <- manager.Run(ctx, OpenRequest{Subject: Subject{
			TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour),
		}}, client)
	}()
	voice, err := connector.NextSession(context.Background())
	require.NoError(t, err)
	activateInitialProviderVADInputTurn(t, manager, "session-tail-barge", 1, voice)
	commandCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	client.reads <- protocol.ResponseCreateEvent{Type: "response.create"}
	create, err := voice.NextCommand(commandCtx)
	require.NoError(t, err)
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventResponseCreated, Epoch: create.Epoch,
		ResponseID: "old-response", ProviderEventID: "old-created", Status: "in_progress",
	}))
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventAudioDelta, Epoch: create.Epoch,
		ResponseID: "old-response", ItemID: "old-item", Audio: []byte{0xaa, 0xbb},
	}))
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventSpeechStarted, ProviderEventID: "barge", ItemID: "input-item",
	}))
	cancelCommand, err := voice.NextCommand(commandCtx)
	require.NoError(t, err)
	require.Equal(t, realtime.CommandCancelResponse, cancelCommand.Kind)
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventSpeechStopped, ProviderEventID: "barge-stopped", ItemID: "input-item",
	}))

	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventResponseCreated, NeedsEpochBinding: true,
		ProviderEventID: "new-created", ResponseID: "new-response", Status: "in_progress",
	}))
	bind, err := voice.NextCommand(commandCtx)
	require.NoError(t, err)
	require.Equal(t, realtime.CommandBindResponseEpoch, bind.Kind)
	newPCM := []byte{0x11, 0x22}
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventAudioDelta, Epoch: bind.Epoch,
		ResponseID: "new-response", ItemID: "new-item", Audio: newPCM,
	}))
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventAudioDone, Epoch: bind.Epoch,
		ResponseID: "new-response", ItemID: "new-item",
	}))

	for {
		select {
		case event := <-client.writes:
			audio, ok := event.(protocol.ResponseOutputAudioDeltaEvent)
			if !ok {
				continue
			}
			decoded, decodeErr := base64.StdEncoding.DecodeString(audio.Delta)
			require.NoError(t, decodeErr)
			require.Equal(t, newPCM, decoded)
			cancel()
			require.ErrorIs(t, <-runResult, context.Canceled)
			return
		case err := <-runResult:
			t.Fatalf("session ended before forwarding the new response: %v", err)
		case <-commandCtx.Done():
			t.Fatal("timed out waiting for new response audio")
		}
	}
}

func TestManagerSubjectExpiryCancelsAllLoopsAndClosesTransports(t *testing.T) {
	connector := newLifecycleConnector()
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
	}, connector, managerIDs{session: "session-expiry", conversation: "conversation-expiry"})
	client := newLifecycleClient()
	runResult := make(chan error, 1)
	go func() {
		runResult <- manager.Run(context.Background(), OpenRequest{Subject: Subject{
			TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(50 * time.Millisecond),
		}}, client)
	}()
	select {
	case <-connector.connected:
	case <-time.After(time.Second):
		t.Fatal("session did not connect before expiry")
	}
	require.ErrorIs(t, <-runResult, context.DeadlineExceeded)
	require.Equal(t, int32(1), connector.voice.closes.Load())
	require.True(t, connector.voice.closeLive.Load())
	require.Equal(t, int32(1), client.closes.Load())
}

func TestManagerSubmitHonorsBoundedInboxContext(t *testing.T) {
	manager := &Manager{active: make(map[runtimeKey]*sessionRuntime)}
	done := make(chan struct{})
	state := &runState{}
	runtime := &sessionRuntime{
		key:        runtimeKey{sessionID: "session-submit", incarnation: 1},
		inbox:      make(chan reactorMessage, 1),
		done:       done,
		state:      state,
		accepting:  true,
		inboxSlots: make(chan struct{}, 1),
	}
	manager.active[runtime.key] = runtime
	runtime.inbox <- reactorMessage{input: DurableInput{Sequence: 1}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, manager.Submit(ctx, "session-submit", 1, DurableInput{Sequence: 2}), context.Canceled)
	require.Len(t, runtime.inbox, 1, "a canceled submit must not overrun the bounded inbox")
}

func TestManagerSubmitRejectsAlreadyCanceledCallerWithReadyInbox(t *testing.T) {
	for attempt := 0; attempt < 1_000; attempt++ {
		manager := &Manager{active: make(map[runtimeKey]*sessionRuntime)}
		runtime := &sessionRuntime{
			key:   runtimeKey{sessionID: "session-submit-canceled", incarnation: 1},
			inbox: make(chan reactorMessage, 1), done: make(chan struct{}), state: &runState{},
			accepting: true, inboxSlots: make(chan struct{}, 1),
		}
		runtime.inboxSlots <- struct{}{}
		manager.active[runtime.key] = runtime
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.ErrorIs(t, manager.Submit(ctx, runtime.key.sessionID, 1, DurableInput{Sequence: 1}), context.Canceled)
		require.Empty(t, runtime.inbox)
	}
}

func TestManagerSubmitRejectsStoppedRuntimeWithReadyInbox(t *testing.T) {
	for attempt := 0; attempt < 1_000; attempt++ {
		manager := &Manager{active: make(map[runtimeKey]*sessionRuntime)}
		done := make(chan struct{})
		close(done)
		runtime := &sessionRuntime{
			key:   runtimeKey{sessionID: "session-submit-stopped", incarnation: 1},
			inbox: make(chan reactorMessage, 1), done: done, state: &runState{},
			accepting: true, inboxSlots: make(chan struct{}, 1),
		}
		runtime.inboxSlots <- struct{}{}
		manager.active[runtime.key] = runtime
		require.ErrorIs(t, manager.Submit(context.Background(), runtime.key.sessionID, 1, DurableInput{Sequence: 1}), ErrSessionNotActive)
		require.Empty(t, runtime.inbox)
	}
}

func TestManagerSubmitDoesNotAcknowledgeDeliveryToDeadInbox(t *testing.T) {
	manager := &Manager{active: make(map[runtimeKey]*sessionRuntime)}
	done := make(chan struct{})
	runtime := &sessionRuntime{
		key:   runtimeKey{sessionID: "session-submit-dead-inbox", incarnation: 1},
		inbox: make(chan reactorMessage, 1), done: done, state: &runState{},
		accepting: true, inboxSlots: make(chan struct{}, 1),
	}
	runtime.inboxSlots <- struct{}{}
	manager.active[runtime.key] = runtime
	result := make(chan error, 1)
	go func() {
		result <- manager.Submit(context.Background(), runtime.key.sessionID, 1, DurableInput{Sequence: 1})
	}()
	message := <-runtime.inbox
	require.NotNil(t, message.done, "Submit delivery requires reactor acknowledgement")
	close(done)
	require.ErrorIs(t, <-result, ErrSessionNotActive)
}

func TestVoiceWriterCancellationBypassesUnmetOrderedBarrier(t *testing.T) {
	voice := &recordingVoice{sent: make(chan realtime.Command, 2)}
	state := &runState{
		voice: voice, voiceCtl: make(chan voiceControl, 2), ingress: make(chan ingressFrame),
		inbox: make(chan reactorMessage, 2), inboxSlots: make(chan struct{}, 2),
	}
	state.inboxSlots <- struct{}{}
	state.inboxSlots <- struct{}{}
	state.voiceCtl <- voiceControl{command: realtime.Command{Kind: realtime.CommandCommitAudio, Sequence: 10, AfterSeq: 99}}
	state.voiceCtl <- voiceControl{command: realtime.Command{Kind: realtime.CommandCancelResponse, Sequence: 11}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- state.voiceWriter(ctx) }()
	select {
	case command := <-voice.sent:
		require.Equal(t, realtime.CommandCancelResponse, command.Kind)
		require.Equal(t, uint64(11), command.Sequence)
	case <-time.After(time.Second):
		t.Fatal("barrierless cancellation did not bypass ordered head")
	}
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestManagerRejectsAsyncEffectsInsteadOfDroppingThem(t *testing.T) {
	state := &runState{}
	err := state.dispatchEffects(context.Background(), []Effect{{Async: &AsyncEffect{
		Kind: "background", Sequence: 1,
	}}}, 0, 0)
	require.ErrorIs(t, err, ErrUnsupportedAsync)
}

type durableDispatcherFake struct {
	trusted     chan TrustedSessionContext
	transcripts chan TranscriptRequestedEffect
	handoffs    chan HandoffRequestedEffect
	err         error
}

func (f *durableDispatcherFake) DispatchTranscript(_ context.Context, trusted TrustedSessionContext, effect TranscriptRequestedEffect) error {
	f.trusted <- trusted
	f.transcripts <- effect
	return f.err
}

func (f *durableDispatcherFake) DispatchHandoff(_ context.Context, trusted TrustedSessionContext, effect HandoffRequestedEffect) error {
	f.trusted <- trusted
	f.handoffs <- effect
	return f.err
}

func TestManagerDispatchesTypedDurableEffectsWithTrustedRuntimeIdentity(t *testing.T) {
	dispatcher := &durableDispatcherFake{
		trusted: make(chan TrustedSessionContext, 2), transcripts: make(chan TranscriptRequestedEffect, 1),
		handoffs: make(chan HandoffRequestedEffect, 1),
	}
	state := &runState{
		ids:        IDs{SessionID: "session-trusted", ConversationID: "conversation-trusted", Incarnation: 9},
		subject:    Subject{TenantID: "tenant-trusted", UserID: "user-trusted"},
		dispatcher: dispatcher,
	}
	transcript := TranscriptRequestedEffect{Turn: identity.TurnKey{ID: "turn", Revision: 2}, Sequence: 3, Role: "user", FinalText: "final"}
	handoffEffect := HandoffRequestedEffect{ResponseID: "response", CallID: "call", ToolName: StartBackgroundTaskTool, Arguments: json.RawMessage(`{"objective":"x"}`)}

	require.NoError(t, state.dispatchEffects(t.Context(), []Effect{
		{Transcript: &transcript},
		{Handoff: &handoffEffect, Async: &AsyncEffect{Kind: AsyncKindStartBackgroundTask}},
	}, 0, 0))
	require.Equal(t, transcript, <-dispatcher.transcripts)
	require.Equal(t, handoffEffect, <-dispatcher.handoffs)
	for range 2 {
		trusted := <-dispatcher.trusted
		require.Equal(t, state.ids, trusted.IDs)
		require.Equal(t, "tenant-trusted", trusted.Subject.TenantID)
		require.Equal(t, "user-trusted", trusted.Subject.UserID)
	}
}

func TestManagerOrdinaryTranscriptDoesNotTerminateWithoutDurableDispatcher(t *testing.T) {
	state := &runState{ids: testIDs()}
	require.NoError(t, state.dispatchEffects(t.Context(), []Effect{{Transcript: &TranscriptRequestedEffect{
		Turn: identity.TurnKey{ID: "turn", Revision: 1}, Sequence: 1, Role: "user", FinalText: "text",
	}}}, 0, 0))
}

func TestManagerHandoffEffectFailsClosedWithoutDispatcher(t *testing.T) {
	state := &runState{ids: testIDs()}
	err := state.dispatchEffects(t.Context(), []Effect{{Handoff: &HandoffRequestedEffect{
		Turn: identity.TurnKey{ID: "turn", Revision: 1}, ResponseID: "response", CallID: "call",
		ToolName: StartBackgroundTaskTool, Arguments: json.RawMessage(`{"objective":"test"}`),
	}}}, 0, 0)
	require.ErrorIs(t, err, ErrUnsupportedAsync)
}

func TestManagerGracefulCloseDrainsOutputAndReturnsCleanupErrors(t *testing.T) {
	providerErr := errors.New("provider cleanup failed")
	clientErr := errors.New("client cleanup failed")
	connector := &closeErrorConnector{
		voice: &closeErrorVoice{closeErr: providerErr}, connected: make(chan struct{}),
	}
	client := &closeErrorClient{closeErr: clientErr, writes: make(chan string, 8), closed: make(chan struct{})}
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
	}, connector, managerIDs{session: "session-close-errors", conversation: "conversation-close-errors"})
	runResult := make(chan error, 1)
	go func() {
		runResult <- manager.Run(context.Background(), OpenRequest{Subject: Subject{
			TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour),
		}}, client)
	}()
	<-connector.connected
	require.Equal(t, "session.created", <-client.writes)
	require.Eventually(t, func() bool {
		manager.activeMu.RLock()
		defer manager.activeMu.RUnlock()
		return manager.active[runtimeKey{sessionID: "session-close-errors", incarnation: 1}] != nil
	}, time.Second, time.Millisecond, "runtime is published only after session.created Write returns")
	require.NoError(t, manager.Submit(context.Background(), "session-close-errors", 1, FailedInput{
		Source: "test", Err: errors.New("graceful terminal"),
	}))
	err := <-runResult
	require.ErrorIs(t, err, clientErr)
	require.ErrorIs(t, err, providerErr)
	require.Equal(t, "error", <-client.writes)
	require.Equal(t, "client.close", <-client.writes)
}

func TestManagerMetricsHooksObserveOverflowAndDroppedAudio(t *testing.T) {
	overflows := make(chan string, 5)
	dropped := make(chan int, 1)
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
	}, testkit.NewFakeConnector(1), managerIDs{session: "metrics", conversation: "metrics"})
	manager.SetMetricsHooks(MetricsHooks{
		QueueOverflow: func(queue string) { overflows <- queue },
		AudioDropped:  func(bytes int) { dropped <- bytes },
	})
	state := newRunState(manager.cfg, IDs{SessionID: "metrics", ConversationID: "metrics", Incarnation: 1},
		&recordingVoice{sent: make(chan realtime.Command, 1)}, newChannelClient(1), func() {})
	state.metrics = manager.metricsSnapshot()
	state.cfg.IngressBlockTimeout = time.Nanosecond
	state.cfg.EgressBlockTimeout = time.Nanosecond
	for range state.cfg.IngressFrames {
		<-state.ingressSlots
	}
	require.ErrorIs(t, state.enqueueIngress(context.Background(), ingressFrame{sequence: 1, audio: []byte{0, 0}}), ErrIngressBackpressure)
	for range state.cfg.EgressFrames {
		<-state.egressSlots
	}
	state.fence = NewEpochFence(1)
	_, err := state.enqueueEgress(context.Background(), realtime.Event{Epoch: 1, ResponseID: "r", ItemID: "i"}, []byte{0, 0})
	require.ErrorIs(t, err, ErrEgressBackpressure)
	for range state.cfg.ControlEvents {
		<-state.inboxSlots
	}
	require.ErrorIs(t, state.enqueueReactor(reactorMessage{input: DurableInput{Sequence: 1}}), ErrControlOverflow)
	for range state.cfg.WriterControlEvents {
		<-state.voiceSlots
	}
	err = state.dispatchEffects(context.Background(), []Effect{{VoiceCommand: &realtime.Command{
		Kind: realtime.CommandCommitAudio, Sequence: 1,
	}}}, 0, 0)
	require.ErrorIs(t, err, ErrControlOverflow)
	for range state.cfg.WriterControlEvents {
		<-state.clientSlots
	}
	require.ErrorIs(t, state.enqueueClientControl(clientControl{event: protocol.ErrorEvent{Type: "error"}}), ErrControlOverflow)
	gotQueues := map[string]int{}
	for range 5 {
		gotQueues[<-overflows]++
	}
	require.Equal(t, map[string]int{
		"ingress": 1, "egress": 1, "reactor": 1, "voice_control": 1, "client_control": 1,
	}, gotQueues)

	state.metrics.audioDropped(960)
	require.Equal(t, 960, <-dropped)
}

func TestManagerReactorReleasesAggregateCreditBeforeCancelIOCompletes(t *testing.T) {
	cfg := Config{
		IngressFrames: 1, EgressFrames: 1, ControlEvents: 1, WriterControlEvents: 4,
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
	}
	state := newRunState(cfg, IDs{SessionID: "reactor-bound", ConversationID: "reactor-bound", Incarnation: 1},
		&recordingVoice{sent: make(chan realtime.Command, 4)}, newChannelClient(4), func() {})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- state.reactorLoop(ctx) }()
	require.NoError(t, state.enqueueReactorSync(context.Background(), reactorMessage{
		input: ConnectedInput{ProviderSession: json.RawMessage(`{}`)},
	}))
	require.NoError(t, state.enqueueReactorSync(context.Background(), reactorMessage{
		input: ClientInput{Event: protocol.ResponseCreateEvent{Type: "response.create"}},
	}))
	createControl := <-state.voiceCtl
	require.Equal(t, realtime.CommandCreateResponse, createControl.command.Kind)
	state.releaseVoiceSlot()
	state.pendingVoice.Add(-1)
	cancelSeen := make(chan struct{})
	releaseApply := make(chan struct{})
	state.activeWrite.mu.Lock()
	state.activeWrite.cancel = func() { close(cancelSeen) }
	state.activeWrite.done = releaseApply
	state.activeWrite.mu.Unlock()
	applyDone := make(chan error, 1)
	require.NoError(t, state.enqueueReactor(reactorMessage{
		input: ClientInput{Event: protocol.ResponseCancelEvent{Type: "response.cancel"}}, done: applyDone,
	}))
	select {
	case <-cancelSeen:
	case <-time.After(time.Second):
		t.Fatal("reactor Apply did not reach flush gate")
	}
	require.NoError(t, <-applyDone)
	require.NoError(t, state.enqueueReactorSync(context.Background(), reactorMessage{input: DurableInput{Sequence: 1, Kind: "benign-before-ack"}}))
	cancelControl := <-state.voiceCtl
	require.Equal(t, realtime.CommandCancelResponse, cancelControl.command.Kind)
	require.Nil(t, cancelControl.sent)
	state.releaseVoiceSlot()
	require.NoError(t, state.enqueueReactorSync(context.Background(), reactorMessage{input: VoiceCommandCompletedInput{
		Sequence: cancelControl.command.Sequence, Kind: cancelControl.command.Kind,
	}}))
	close(releaseApply)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestManagerMetricsHooksAreNoOpByDefaultAndRaceSafeToReplace(t *testing.T) {
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
	}, testkit.NewFakeConnector(1), managerIDs{session: "metrics-race", conversation: "metrics-race"})
	manager.metricsSnapshot().queueOverflow("noop")
	start := make(chan struct{})
	var wait sync.WaitGroup
	wait.Add(2)
	go func() {
		defer wait.Done()
		<-start
		for index := 0; index < 1_000; index++ {
			manager.SetMetricsHooks(MetricsHooks{})
		}
	}()
	go func() {
		defer wait.Done()
		<-start
		for index := 0; index < 1_000; index++ {
			manager.metricsSnapshot().audioDropped(960)
		}
	}()
	close(start)
	wait.Wait()
}

func TestManagerTransportFailureInEveryDirectionStopsAllFiveLoops(t *testing.T) {
	tests := []struct {
		name    string
		trigger func(*failureVoice, *failureClient, error)
	}{
		{
			name: "client read",
			trigger: func(_ *failureVoice, client *failureClient, failure error) {
				client.readErr <- failure
			},
		},
		{
			name: "client write",
			trigger: func(_ *failureVoice, client *failureClient, failure error) {
				client.writeErr <- failure
			},
		},
		{
			name: "provider receive",
			trigger: func(voice *failureVoice, _ *failureClient, failure error) {
				voice.receiveErr <- failure
			},
		},
		{
			name: "provider send",
			trigger: func(voice *failureVoice, client *failureClient, failure error) {
				voice.sendErr <- failure
				client.reads <- protocol.ResponseCreateEvent{Type: "response.create"}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			providerCleanupErr := errors.New("provider cleanup after " + test.name)
			clientCleanupErr := errors.New("client cleanup after " + test.name)
			voice := &failureVoice{
				sendErr: make(chan error, 1), receiveErr: make(chan error, 1), closeErr: providerCleanupErr,
			}
			connector := &failureConnector{voice: voice, connected: make(chan struct{})}
			client := newFailureClient()
			client.closeErr = clientCleanupErr
			manager := NewManager(Config{
				IngressBlockTimeout: time.Second,
				EgressBlockTimeout:  time.Second,
			}, connector, managerIDs{
				session:      identity.SessionID("session-direction-" + test.name),
				conversation: identity.ConversationID("conversation-direction-" + test.name),
			})
			runResult := make(chan error, 1)
			go func() {
				runResult <- manager.Run(context.Background(), OpenRequest{Subject: Subject{
					TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour),
				}}, client)
			}()
			select {
			case <-connector.connected:
			case <-time.After(time.Second):
				t.Fatal("session did not connect")
			}
			failure := errors.New(test.name + " transport failed")
			test.trigger(voice, client, failure)
			err := <-runResult
			require.ErrorIs(t, err, failure)
			require.NotErrorIs(t, err, providerCleanupErr)
			require.NotErrorIs(t, err, clientCleanupErr)
			require.Equal(t, int32(1), voice.closes.Load())
			require.True(t, voice.closeLive.Load())
			require.Equal(t, int32(1), client.closes.Load())
			manager.activeMu.RLock()
			require.Empty(t, manager.active)
			manager.activeMu.RUnlock()
		})
	}
}
