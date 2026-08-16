package ws

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/protocol"
	"github.com/chuanmingliu/openai-gpt-live/internal/session"
	"github.com/chuanmingliu/openai-gpt-live/internal/testkit"
	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

type reconnectRecoveryStore struct {
	snapshot session.RecoverySnapshot
}

type ambiguousPublishOwners struct {
	*session.MemoryReconnectOwners
	failCall int32
	calls    atomic.Int32
}

func (s *ambiguousPublishOwners) Publish(ctx context.Context, key session.ReconnectOwnerKey, epoch identity.ResponseEpoch,
	boundary bool, lease time.Duration,
) (session.ReconnectOwnerLease, error) {
	call := s.calls.Add(1)
	confirmed, err := s.MemoryReconnectOwners.Publish(ctx, key, epoch, boundary, lease)
	if err == nil && call == s.failCall {
		return confirmed, session.ErrRecoveryUnavailable
	}
	return confirmed, err
}

func (s reconnectRecoveryStore) LoadRecovery(_ context.Context, scope handoff.Scope, _, _ int) (session.RecoverySnapshot, error) {
	snapshot := s.snapshot
	snapshot.ConversationID = scope.ConversationID
	snapshot.ActiveHandoffs = append([]session.RecoveryActiveHandoff(nil), s.snapshot.ActiveHandoffs...)
	for index := range snapshot.ActiveHandoffs {
		snapshot.ActiveHandoffs[index].Snapshot = handoff.CloneSnapshot(snapshot.ActiveHandoffs[index].Snapshot)
		snapshot.ActiveHandoffs[index].Snapshot.Request.Scope = scope
	}
	return snapshot, nil
}

func readSessionCreated(t *testing.T, conn *websocket.Conn) protocol.SessionCreatedEvent {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	typ, raw, err := conn.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, websocket.MessageText, typ)
	var created protocol.SessionCreatedEvent
	require.NoError(t, json.Unmarshal(raw, &created))
	require.Equal(t, "session.created", created.Type)
	return created
}

func readProfileSelected(t *testing.T, conn *websocket.Conn) protocol.LiveProfileSelectedEvent {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	typ, raw, err := conn.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, websocket.MessageText, typ)
	event, err := protocol.DecodeServer(raw, protocol.Limits{MaxFrameBytes: 8 << 10, MaxJSONDepth: 32, MaxAudioBytes: 1})
	require.NoError(t, err)
	selected, ok := event.(protocol.LiveProfileSelectedEvent)
	require.True(t, ok)
	return selected
}

func TestCustomProfileReconnectRetainsExactSnapshotAndPublishesEachIncarnation(t *testing.T) {
	manual := clock.NewManual(time.Now().UTC())
	credentials, err := session.NewReconnectCredentials([]byte(strings.Repeat("v", 32)), manual)
	require.NoError(t, err)
	owners := session.NewMemoryReconnectOwners(manual)
	connector := testkit.NewFakeConnector(4)
	managerFor := func(ownerID string) *session.Manager {
		return session.NewManager(session.Config{IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
			RecoveryStore: reconnectRecoveryStore{snapshot: session.RecoverySnapshot{}}, ReconnectCredentials: credentials,
			ReconnectOwners: owners, ReconnectOwnerID: ownerID, ReconnectCredentialTTL: time.Minute}, connector, fixedIDs{})
	}
	subject := session.Subject{TenantID: "tenant-profile", UserID: "user-profile", ExpiresAt: manual.Now().Add(time.Hour)}
	resolver := &recordingProfileResolver{resolved: resolvedProfile(t, subject.TenantID, "insurance", 3)}
	require.NoError(t, resolver.resolved.Validate())
	admit := AdmissionFunc(func(context.Context, *http.Request) (session.Subject, func(), error) { return subject, func() {}, nil })
	opts := Options{MaxFrameBytes: 8 << 10, ProviderModel: "provider-model", ReconnectCredentials: credentials,
		VerticalProfiles: true, ProfileResolver: resolver}
	serverA := httptest.NewServer(NewHandler(opts, managerFor("profile-a"), admit))
	defer serverA.Close()
	serverB := httptest.NewServer(NewHandler(opts, managerFor("profile-b"), admit))
	defer serverB.Close()
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	query := "?profile=insurance&profile_version=3"
	first, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(serverA.URL, "http")+"/v1/realtime"+query, nil)
	require.NoError(t, err)
	created := readSessionCreated(t, first)
	initialSelection := readProfileSelected(t, first)
	require.Equal(t, resolver.resolved.Snapshot.SnapshotHash, initialSelection.Profile.SnapshotHash)
	require.NoError(t, first.Close(websocket.StatusNormalClosure, "reconnect"))
	require.Eventually(t, func() bool {
		owner, ok := owners.Read(identity.SessionID(created.Reconnect.SessionID))
		return ok && owner.State == session.ReconnectOwnerTombstoned
	}, testTimeout, time.Millisecond)

	second, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(serverB.URL, "http")+"/v1/realtime?reconnect=1&profile=insurance&profile_version=3", nil)
	require.NoError(t, err)
	writeReconnectControl(t, second, created.Reconnect.Credential)
	reconnected := readSessionCreated(t, second)
	reconnectSelection := readProfileSelected(t, second)
	require.Equal(t, initialSelection.Profile, reconnectSelection.Profile)
	require.NotEqual(t, initialSelection.EventID, reconnectSelection.EventID, "different incarnations need distinct replay identities")
	verified, err := credentials.Verify(reconnected.Reconnect.Credential)
	require.NoError(t, err)
	require.Equal(t, reconnectSelection.Profile.ID, verified.ProfileID)
	require.Equal(t, reconnectSelection.Profile.Version, verified.ProfileVersion)
	require.Equal(t, reconnectSelection.Profile.SnapshotHash, verified.ProfileSnapshotHash)
	require.NoError(t, second.Close(websocket.StatusNormalClosure, "done"))
}

func writeReconnectControl(t *testing.T, conn *websocket.Conn, credential string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	raw, err := json.Marshal(map[string]string{"type": "session.reconnect", "credential": credential})
	require.NoError(t, err)
	require.NoError(t, conn.Write(ctx, websocket.MessageText, raw))
}

type requestURILog struct {
	mu   sync.Mutex
	seen []string
}

func (l *requestURILog) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		l.mu.Lock()
		l.seen = append(l.seen, r.RequestURI)
		l.mu.Unlock()
		next.ServeHTTP(w, r)
	})
}

func (l *requestURILog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.seen...)
}

func TestPublicReconnectCredentialTraversesRealWebSocketAndCrossReplicaOwnerCAS(t *testing.T) {
	manual := clock.NewManual(time.Now().UTC())
	credentials, err := session.NewReconnectCredentials([]byte(strings.Repeat("r", 32)), manual)
	require.NoError(t, err)
	owners := session.NewMemoryReconnectOwners(manual)
	recoveryScope := handoff.Scope{TenantID: "tenant-test", UserID: "user-test", ConversationID: "conversation-test"}
	recoveryEventID, err := session.RecoveryEventIDForActiveHandoff(recoveryScope, "handoff-restored", handoff.StateRunning)
	require.NoError(t, err)
	recoveryStore := reconnectRecoveryStore{snapshot: session.RecoverySnapshot{
		LastEventSeq: 12,
		ActiveHandoffs: []session.RecoveryActiveHandoff{{EventID: recoveryEventID, Snapshot: handoff.Snapshot{
			ID: "handoff-restored", State: handoff.StateRunning,
			Request: handoff.CreateRequest{Objective: "private objective must not cross", PresentationHint: "private hint"},
			Input:   json.RawMessage(`{"secret":"input must not cross"}`),
		}}},
	}}
	connector := testkit.NewFakeConnector(16)
	managerFor := func(ownerID string) *session.Manager {
		return session.NewManager(session.Config{
			IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
			RecoveryStore: recoveryStore, ReconnectCredentials: credentials, ReconnectOwners: owners,
			ReconnectOwnerID: ownerID, ReconnectCredentialTTL: time.Minute,
		}, connector, fixedIDs{})
	}
	subject := session.Subject{TenantID: "tenant-test", UserID: "user-test", ExpiresAt: manual.Now().Add(time.Hour)}
	admit := func(expected session.Subject) AdmissionFunc {
		return func(context.Context, *http.Request) (session.Subject, func(), error) { return expected, func() {}, nil }
	}
	opts := Options{MaxFrameBytes: 8 << 10, AdvertisedModel: "public-model", ProviderModel: "provider-model", ReconnectCredentials: credentials}
	requestLog := &requestURILog{}
	serverA := httptest.NewServer(requestLog.wrap(NewHandler(opts, managerFor("replica-a"), admit(subject))))
	defer serverA.Close()
	serverB := httptest.NewServer(requestLog.wrap(NewHandler(opts, managerFor("replica-b"), admit(subject))))
	defer serverB.Close()

	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	first, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(serverA.URL, "http")+"/v1/realtime?model=public-model", nil)
	require.NoError(t, err)
	created := readSessionCreated(t, first)
	require.NotNil(t, created.Reconnect)
	require.Equal(t, uint64(1), created.Reconnect.Incarnation)
	require.Equal(t, int64(0), created.Reconnect.Cursor)
	require.Equal(t, "provider-model", created.Reconnect.Model)
	require.NotEmpty(t, created.Reconnect.Credential)
	require.NoError(t, first.Close(websocket.StatusNormalClosure, "network handoff"))
	require.Eventually(t, func() bool {
		owner, ok := owners.Read("session-test")
		return ok && owner.State == session.ReconnectOwnerTombstoned
	}, testTimeout, time.Millisecond)

	reconnectURL := "ws" + strings.TrimPrefix(serverB.URL, "http") + "/v1/realtime?model=public-model&reconnect=1"
	second, _, err := websocket.Dial(ctx, reconnectURL, nil)
	require.NoError(t, err)
	writeReconnectControl(t, second, created.Reconnect.Credential)
	reconnected := readSessionCreated(t, second)
	require.NotNil(t, reconnected.Reconnect)
	require.Equal(t, uint64(2), reconnected.Reconnect.Incarnation)
	require.NotEqual(t, created.Reconnect.Credential, reconnected.Reconnect.Credential)
	frameType, recoveryRaw, err := second.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, websocket.MessageText, frameType)
	recovered, err := protocol.DecodeServer(recoveryRaw, protocol.Limits{MaxFrameBytes: 8 << 10, MaxJSONDepth: 32, MaxAudioBytes: 1})
	require.NoError(t, err)
	progress, ok := recovered.(protocol.LiveHandoffProgressEvent)
	require.True(t, ok)
	require.Equal(t, recoveryEventID, progress.EventID)
	require.LessOrEqual(t, len(progress.EventID), 256)
	require.Equal(t, "handoff-restored", progress.HandoffID)
	require.Equal(t, string(handoff.StateRunning), progress.Status)
	require.NotContains(t, string(recoveryRaw), "private objective")
	require.NotContains(t, string(recoveryRaw), "private hint")
	require.NotContains(t, string(recoveryRaw), "input must not cross")
	for range 2 {
		_, err = connector.NextSession(ctx)
		require.NoError(t, err)
	}

	duplicate, _, err := websocket.Dial(ctx, reconnectURL, nil)
	require.NoError(t, err, "duplicate reaches the authenticated WebSocket runtime boundary")
	writeReconnectControl(t, duplicate, created.Reconnect.Credential)
	_, _, err = duplicate.Read(ctx)
	require.Error(t, err)
	require.Equal(t, websocket.StatusInternalError, websocket.CloseStatus(err))
	_ = duplicate.CloseNow()

	wrongSubject := subject
	wrongSubject.UserID = "other-user"
	crossScope := httptest.NewServer(NewHandler(opts, managerFor("replica-c"), admit(wrongSubject)))
	defer crossScope.Close()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(crossScope.URL, "http")+"/v1/realtime?model=public-model&reconnect=1", nil)
	require.NoError(t, err)
	writeReconnectControl(t, conn, reconnected.Reconnect.Credential)
	_, _, err = conn.Read(ctx)
	require.Error(t, err)
	require.Equal(t, websocket.StatusPolicyViolation, websocket.CloseStatus(err))
	_ = conn.CloseNow()

	for _, invalid := range []string{"malformed", strings.Repeat("x", session.MaximumReconnectCredentialBytes+1)} {
		conn, _, err = websocket.Dial(ctx, "ws"+strings.TrimPrefix(serverA.URL, "http")+"/v1/realtime?model=public-model&reconnect=1", nil)
		require.NoError(t, err)
		writeReconnectControl(t, conn, invalid)
		_, _, err = conn.Read(ctx)
		require.Error(t, err)
		require.Equal(t, websocket.StatusPolicyViolation, websocket.CloseStatus(err))
		_ = conn.CloseNow()
	}
	for _, requestURI := range requestLog.snapshot() {
		require.NotContains(t, requestURI, created.Reconnect.Credential)
		require.NotContains(t, requestURI, "lrt1.")
	}
	providerCtx, providerCancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer providerCancel()
	_, err = connector.NextSession(providerCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded, "invalid, stale, and cross-scope reconnect controls never open a provider session")
	require.NoError(t, second.Close(websocket.StatusNormalClosure, "done"))
}

func TestPublicReconnectAmbiguousPublishKeepsPriorAndAdvertisedCredentialsRetryable(t *testing.T) {
	manual := clock.NewManual(time.Now().UTC())
	credentials, err := session.NewReconnectCredentials([]byte(strings.Repeat("a", 32)), manual)
	require.NoError(t, err)
	owners := &ambiguousPublishOwners{MemoryReconnectOwners: session.NewMemoryReconnectOwners(manual), failCall: 2}
	connector := testkit.NewFakeConnector(16)
	recovery := reconnectRecoveryStore{snapshot: session.RecoverySnapshot{}}
	managerFor := func(ownerID string) *session.Manager {
		return session.NewManager(session.Config{
			IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
			RecoveryStore: recovery, ReconnectCredentials: credentials, ReconnectOwners: owners,
			ReconnectOwnerID: ownerID, ReconnectCredentialTTL: time.Minute,
		}, connector, fixedIDs{})
	}
	subject := session.Subject{TenantID: "tenant-ambiguous", UserID: "user-ambiguous", ExpiresAt: manual.Now().Add(time.Hour)}
	admit := AdmissionFunc(func(context.Context, *http.Request) (session.Subject, func(), error) { return subject, func() {}, nil })
	opts := Options{MaxFrameBytes: 8 << 10, AdvertisedModel: "public-model", ProviderModel: "provider-model", ReconnectCredentials: credentials}
	serverA := httptest.NewServer(NewHandler(opts, managerFor("replica-a"), admit))
	defer serverA.Close()
	serverB := httptest.NewServer(NewHandler(opts, managerFor("replica-b"), admit))
	defer serverB.Close()
	serverC := httptest.NewServer(NewHandler(opts, managerFor("replica-c"), admit))
	defer serverC.Close()

	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	endpoint := func(server *httptest.Server, reconnect bool) string {
		uri := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/realtime?model=public-model"
		if reconnect {
			uri += "&reconnect=1"
		}
		return uri
	}
	first, _, err := websocket.Dial(ctx, endpoint(serverA, false), nil)
	require.NoError(t, err)
	created := readSessionCreated(t, first)
	require.NoError(t, first.Close(websocket.StatusNormalClosure, "handoff"))
	require.Eventually(t, func() bool {
		owner, ok := owners.Read(identity.SessionID(created.Reconnect.SessionID))
		return ok && owner.State == session.ReconnectOwnerTombstoned
	}, testTimeout, time.Millisecond)

	failed, _, err := websocket.Dial(ctx, endpoint(serverB, true), nil)
	require.NoError(t, err)
	writeReconnectControl(t, failed, created.Reconnect.Credential)
	advertised := readSessionCreated(t, failed)
	require.Equal(t, uint64(2), advertised.Reconnect.Incarnation)
	advertisedClaims, err := credentials.Verify(advertised.Reconnect.Credential)
	require.NoError(t, err)
	_, _, err = failed.Read(ctx)
	require.Error(t, err, "the committed-but-unconfirmed Publish closes the unpublished runtime")
	_ = failed.CloseNow()
	staged, ok := owners.Read(identity.SessionID(created.Reconnect.SessionID))
	require.True(t, ok)
	require.Equal(t, session.ReconnectOwnerActive, staged.State, "the fake models a committed Publish whose response was lost")
	manual.Advance(staged.OwnerLeaseExpiresAt.Sub(manual.Now()))

	priorRetry, _, err := websocket.Dial(ctx, endpoint(serverC, true), nil)
	require.NoError(t, err)
	writeReconnectControl(t, priorRetry, created.Reconnect.Credential)
	restaged := readSessionCreated(t, priorRetry)
	require.Equal(t, advertised.Reconnect.Credential, restaged.Reconnect.Credential,
		"the prior credential reclaims and re-advertises the exact possibly delivered successor")
	require.Eventually(t, func() bool {
		owner, found := owners.Read(identity.SessionID(created.Reconnect.SessionID))
		return found && owner.State == session.ReconnectOwnerActive && owner.CredentialID == advertisedClaims.CredentialID
	}, testTimeout, time.Millisecond)
	require.NoError(t, priorRetry.Close(websocket.StatusNormalClosure, "retry successor"))
	require.Eventually(t, func() bool {
		owner, found := owners.Read(identity.SessionID(created.Reconnect.SessionID))
		return found && owner.State == session.ReconnectOwnerTombstoned
	}, testTimeout, time.Millisecond)

	stagedRetry, _, err := websocket.Dial(ctx, endpoint(serverC, true), nil)
	require.NoError(t, err)
	writeReconnectControl(t, stagedRetry, advertised.Reconnect.Credential)
	advanced := readSessionCreated(t, stagedRetry)
	require.Equal(t, uint64(3), advanced.Reconnect.Incarnation,
		"the possibly delivered staged credential remains independently retryable")
	require.NoError(t, stagedRetry.Close(websocket.StatusNormalClosure, "done"))
}

func TestPublicInitialSessionAmbiguousPublishKeepsAdvertisedCredentialRetryable(t *testing.T) {
	manual := clock.NewManual(time.Now().UTC())
	credentials, err := session.NewReconnectCredentials([]byte(strings.Repeat("i", 32)), manual)
	require.NoError(t, err)
	owners := &ambiguousPublishOwners{MemoryReconnectOwners: session.NewMemoryReconnectOwners(manual), failCall: 1}
	connector := testkit.NewFakeConnector(8)
	managerFor := func(ownerID string) *session.Manager {
		return session.NewManager(session.Config{
			IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
			RecoveryStore: reconnectRecoveryStore{}, ReconnectCredentials: credentials, ReconnectOwners: owners,
			ReconnectOwnerID: ownerID, ReconnectCredentialTTL: time.Minute,
		}, connector, fixedIDs{})
	}
	subject := session.Subject{TenantID: "tenant-initial-ambiguous", UserID: "user", ExpiresAt: manual.Now().Add(time.Hour)}
	admit := AdmissionFunc(func(context.Context, *http.Request) (session.Subject, func(), error) { return subject, func() {}, nil })
	opts := Options{MaxFrameBytes: 8 << 10, AdvertisedModel: "public-model", ProviderModel: "provider-model", ReconnectCredentials: credentials}
	serverA := httptest.NewServer(NewHandler(opts, managerFor("replica-a"), admit))
	defer serverA.Close()
	serverB := httptest.NewServer(NewHandler(opts, managerFor("replica-b"), admit))
	defer serverB.Close()
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	first, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(serverA.URL, "http")+"/v1/realtime?model=public-model", nil)
	require.NoError(t, err)
	created := readSessionCreated(t, first)
	_, _, err = first.Read(ctx)
	require.Error(t, err)
	_ = first.CloseNow()
	staged, ok := owners.Read(identity.SessionID(created.Reconnect.SessionID))
	require.True(t, ok, "advertisement prevents deletion of an outcome-ambiguous initial owner")
	require.Equal(t, session.ReconnectOwnerActive, staged.State)
	manual.Advance(staged.OwnerLeaseExpiresAt.Sub(manual.Now()))

	retry, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(serverB.URL, "http")+"/v1/realtime?model=public-model&reconnect=1", nil)
	require.NoError(t, err)
	writeReconnectControl(t, retry, created.Reconnect.Credential)
	reconnected := readSessionCreated(t, retry)
	require.Equal(t, uint64(2), reconnected.Reconnect.Incarnation)
	require.NoError(t, retry.Close(websocket.StatusNormalClosure, "done"))
}
