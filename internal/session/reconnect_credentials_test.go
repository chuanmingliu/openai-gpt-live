package session

import (
	"strings"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/stretchr/testify/require"
)

func TestReconnectCredentialIsBoundedIntegrityProtectedScopedAndExpiring(t *testing.T) {
	manual := clock.NewManual(time.Unix(10_000, 0))
	authority, err := NewReconnectCredentials([]byte(strings.Repeat("k", 32)), manual)
	require.NoError(t, err)
	claims := ReconnectClaims{
		Scope:     handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "conversation"},
		SessionID: "session", Incarnation: 7, Cursor: 19, Model: "provider-model", ExpiresAt: manual.Now().Add(time.Minute),
	}
	issued, err := authority.Issue(claims)
	require.NoError(t, err)
	require.NotEmpty(t, issued.Claims.CredentialID)
	require.LessOrEqual(t, len(issued.Credential), MaximumReconnectCredentialBytes)
	verified, err := authority.Verify(issued.Credential)
	require.NoError(t, err)
	require.Equal(t, issued.Claims, verified)
	reissued, err := authority.Issue(issued.Claims)
	require.NoError(t, err)
	require.Equal(t, issued, reissued, "a trusted durable staged claim must reconstruct the exact advertised credential")

	for _, invalid := range []string{"", "not-a-token", issued.Credential + "x", strings.Repeat("x", MaximumReconnectCredentialBytes+1)} {
		_, verifyErr := authority.Verify(invalid)
		require.ErrorIs(t, verifyErr, ErrInvalidReconnectCredential)
	}
	manual.Advance(time.Minute)
	_, err = authority.Verify(issued.Credential)
	require.ErrorIs(t, err, ErrExpiredReconnectCredential)
}

func TestReconnectOwnerCASIsCrossOwnerSafeAndConsumesCredentialExactlyOnce(t *testing.T) {
	manual := clock.NewManual(time.Unix(20_000, 0))
	owners := NewMemoryReconnectOwners(manual)
	current := ReconnectOwner{
		Scope:     handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "conversation"},
		SessionID: "session", Incarnation: 3, Cursor: 8, Model: "provider-model", ResponseEpoch: 11,
		CredentialID: "credential-current", OwnerID: "replica-a", State: ReconnectOwnerEstablishing,
		TurnBoundary: true, ExpiresAt: manual.Now().Add(time.Minute), SubjectExpiresAt: manual.Now().Add(time.Hour),
	}
	_, err := owners.Create(t.Context(), current, 15*time.Second)
	require.NoError(t, err)
	_, err = owners.Publish(t.Context(), current.Key(), current.ResponseEpoch, true, 15*time.Second)
	require.NoError(t, err)
	current.State = ReconnectOwnerActive
	claims := current.Claims()
	next := current
	next.Incarnation++
	next.CredentialID = "credential-next"
	next.OwnerID = "replica-b"
	next.State = ReconnectOwnerClaiming
	_, _, err = owners.Claim(t.Context(), claims, next, 15*time.Second)
	require.ErrorIs(t, err, ErrStaleReconnect, "a foreign replica cannot steal a still-active owner")
	require.NoError(t, owners.Tombstone(t.Context(), current.SessionID, current.Incarnation, current.OwnerID, identity.ResponseEpoch(13), true))
	claimed, _, err := owners.Claim(t.Context(), claims, next, 15*time.Second)
	require.NoError(t, err)
	require.Equal(t, identity.ResponseEpoch(13), claimed.ResponseEpoch)
	_, _, err = owners.Claim(t.Context(), claims, next, 15*time.Second)
	require.ErrorIs(t, err, ErrStaleReconnect, "the old credential is consumed by the CAS")

	wrong := claims
	wrong.Scope.UserID = "other-user"
	wrongNext := next
	wrongNext.Incarnation++
	wrongNext.CredentialID = "credential-third"
	_, _, err = owners.Claim(t.Context(), wrong, wrongNext, 15*time.Second)
	require.ErrorIs(t, err, ErrStaleReconnect)
}

func TestReconnectOwnerAbortClaimRestoresExactPreviousOwnerAndCredential(t *testing.T) {
	manual := clock.NewManual(time.Unix(30_000, 0))
	owners := NewMemoryReconnectOwners(manual)
	previous := testReconnectOwner(manual.Now(), "replica-a", "credential-one", 1, 7)
	lease, err := owners.Create(t.Context(), previous, 20*time.Second)
	require.NoError(t, err)
	previous.OwnerLeaseExpiresAt = lease.ExpiresAt
	lease, err = owners.Publish(t.Context(), previous.Key(), 4, true, 20*time.Second)
	require.NoError(t, err)
	previous.OwnerLeaseExpiresAt = lease.ExpiresAt
	require.NoError(t, owners.Tombstone(t.Context(), previous.SessionID, previous.Incarnation, previous.OwnerID, 5, true))
	previous.State, previous.ResponseEpoch, previous.TurnBoundary = ReconnectOwnerTombstoned, 5, true

	next := previous
	next.Incarnation++
	next.OwnerID = "replica-b"
	next.CredentialID = "credential-two"
	next.State = ReconnectOwnerClaiming
	next.TurnBoundary = false
	claimed, _, err := owners.Claim(t.Context(), previous.Claims(), next, 20*time.Second)
	require.NoError(t, err)
	require.Equal(t, previous, claimed)
	require.NoError(t, owners.AbortClaim(t.Context(), next.Key(), claimed))
	restored, ok := owners.Read(previous.SessionID)
	require.True(t, ok)
	require.Equal(t, previous, restored)

	claimed, _, err = owners.Claim(t.Context(), previous.Claims(), next, 20*time.Second)
	require.NoError(t, err, "the client-known credential remains retryable after an exact abort")
	wrong := next.Key()
	wrong.OwnerID = "replica-c"
	require.ErrorIs(t, owners.AbortClaim(t.Context(), wrong, claimed), ErrStaleReconnect)
}

func TestReconnectOwnerLeaseFencesLiveOwnerAndAllowsForeignTakeoverAfterHardDeath(t *testing.T) {
	manual := clock.NewManual(time.Unix(40_000, 0))
	owners := NewMemoryReconnectOwners(manual)
	current := testReconnectOwner(manual.Now(), "replica-a", "credential-one", 1, 4)
	current.ExpiresAt = manual.Now().Add(time.Minute)
	lease, err := owners.Create(t.Context(), current, 12*time.Second)
	require.NoError(t, err)
	current.OwnerLeaseExpiresAt = lease.ExpiresAt
	lease, err = owners.Publish(t.Context(), current.Key(), 8, true, 12*time.Second)
	require.NoError(t, err)
	current.OwnerLeaseExpiresAt = lease.ExpiresAt
	current.State, current.ResponseEpoch, current.TurnBoundary = ReconnectOwnerActive, 8, true
	next := current
	next.Incarnation++
	next.OwnerID, next.CredentialID, next.State, next.TurnBoundary = "replica-b", "credential-two", ReconnectOwnerClaiming, false

	_, _, err = owners.Claim(t.Context(), current.Claims(), next, 12*time.Second)
	require.ErrorIs(t, err, ErrStaleReconnect)
	manual.Advance(12 * time.Second)
	claimed, _, err := owners.Claim(t.Context(), current.Claims(), next, 12*time.Second)
	require.NoError(t, err)
	require.Equal(t, current.OwnerID, claimed.OwnerID)
	_, err = owners.Heartbeat(t.Context(), current.Key(), 9, true, 12*time.Second)
	require.ErrorIs(t, err, ErrStaleReconnect, "a dead owner cannot revive or mutate the foreign claim")
}

func TestReconnectOwnerHardDeadClaimingKeepsPriorAndStagedIncarnationsRetryable(t *testing.T) {
	newStore := func(t *testing.T) (*MemoryReconnectOwners, *clock.Manual, ReconnectOwner) {
		t.Helper()
		manual := clock.NewManual(time.Unix(45_000, 0))
		owners := NewMemoryReconnectOwners(manual)
		prior := testReconnectOwner(manual.Now(), "replica-a", "credential-prior", 7, 13)
		prior.ResponseEpoch = 21
		lease, err := owners.Create(t.Context(), prior, 5*time.Second)
		require.NoError(t, err)
		prior.OwnerLeaseExpiresAt = lease.ExpiresAt
		lease, err = owners.Publish(t.Context(), prior.Key(), prior.ResponseEpoch, true, 5*time.Second)
		require.NoError(t, err)
		prior.OwnerLeaseExpiresAt = lease.ExpiresAt
		require.NoError(t, owners.Tombstone(t.Context(), prior.SessionID, prior.Incarnation, prior.OwnerID, prior.ResponseEpoch, true))
		prior.State = ReconnectOwnerTombstoned
		return owners, manual, prior
	}

	t.Run("prior credential restages the same successor after a post-claim crash", func(t *testing.T) {
		owners, manual, prior := newStore(t)
		staged := prior
		staged.Incarnation++
		staged.ResponseEpoch++
		staged.OwnerID, staged.CredentialID = "replica-b", "credential-staged"
		staged.State, staged.TurnBoundary = ReconnectOwnerClaiming, true
		staged.PreviousCredentialID, staged.PreviousIncarnation, staged.PreviousCursor = prior.CredentialID, prior.Incarnation, prior.Cursor
		staged.PreviousExpiresAt, staged.PreviousAcceptUntil = prior.ExpiresAt, manual.Now().Add(10*time.Second)
		_, _, err := owners.Claim(t.Context(), prior.Claims(), staged, 5*time.Second)
		require.NoError(t, err)

		manual.Advance(5 * time.Second)
		restaged := staged
		restaged.OwnerID, restaged.CredentialID = "replica-c", "credential-restaged"
		claimed, _, err := owners.Claim(t.Context(), prior.Claims(), restaged, 5*time.Second)
		require.NoError(t, err, "the exact prior incarnation and credential must survive a hard-dead staged claim")
		require.Equal(t, staged.ResponseEpoch, claimed.ResponseEpoch, "restaging the same incarnation must not regress its allocated epoch")
		stored, ok := owners.Read(prior.SessionID)
		require.True(t, ok)
		require.Equal(t, staged.CredentialID, stored.CredentialID, "retrying N must not invalidate a possibly advertised N+1 credential")
		require.Equal(t, staged.Incarnation, stored.Incarnation)
		require.Equal(t, prior.CredentialID, stored.PreviousCredentialID)
	})

	t.Run("advertised staged credential advances after an ambiguous publish", func(t *testing.T) {
		owners, manual, prior := newStore(t)
		staged := prior
		staged.Incarnation++
		staged.ResponseEpoch++
		staged.OwnerID, staged.CredentialID = "replica-b", "credential-staged"
		staged.State, staged.TurnBoundary = ReconnectOwnerClaiming, true
		_, _, err := owners.Claim(t.Context(), prior.Claims(), staged, 5*time.Second)
		require.NoError(t, err)

		manual.Advance(5 * time.Second)
		next := staged
		next.Incarnation++
		next.ResponseEpoch++
		next.OwnerID, next.CredentialID = "replica-c", "credential-next"
		claimed, _, err := owners.Claim(t.Context(), staged.Claims(), next, 5*time.Second)
		require.NoError(t, err, "a client that received the staged token must be able to retry after an ambiguous publish")
		require.Equal(t, staged.Incarnation, claimed.Incarnation)
		require.Equal(t, staged.ResponseEpoch, claimed.ResponseEpoch)
	})

	t.Run("prior credential reclaims an ambiguously published successor", func(t *testing.T) {
		owners, manual, prior := newStore(t)
		staged := prior
		staged.Incarnation++
		staged.ResponseEpoch++
		staged.OwnerID, staged.CredentialID = "replica-b", "credential-staged"
		staged.State, staged.TurnBoundary = ReconnectOwnerClaiming, true
		staged.PreviousCredentialID, staged.PreviousIncarnation, staged.PreviousCursor = prior.CredentialID, prior.Incarnation, prior.Cursor
		staged.PreviousExpiresAt, staged.PreviousAcceptUntil = prior.ExpiresAt, manual.Now().Add(10*time.Second)
		_, _, err := owners.Claim(t.Context(), prior.Claims(), staged, 5*time.Second)
		require.NoError(t, err)
		_, err = owners.Publish(t.Context(), staged.Key(), staged.ResponseEpoch, true, 5*time.Second)
		require.NoError(t, err)

		manual.Advance(5 * time.Second)
		restageRequest := staged
		restageRequest.OwnerID, restageRequest.CredentialID = "replica-c", "must-not-replace-staged"
		claimed, _, err := owners.Claim(t.Context(), prior.Claims(), restageRequest, 5*time.Second)
		require.NoError(t, err)
		require.Equal(t, ReconnectOwnerActive, claimed.State)
		stored, ok := owners.Read(prior.SessionID)
		require.True(t, ok)
		require.Equal(t, staged.Incarnation, stored.Incarnation)
		require.Equal(t, staged.CredentialID, stored.CredentialID)
		require.Equal(t, prior.Incarnation, stored.PreviousIncarnation)
		require.Equal(t, ReconnectOwnerClaiming, stored.State)
	})
}

func TestReconnectOwnerPublishIsIdempotentAfterAmbiguousSuccess(t *testing.T) {
	manual := clock.NewManual(time.Unix(47_000, 0))
	owners := NewMemoryReconnectOwners(manual)
	owner := testReconnectOwner(manual.Now(), "replica-a", "credential", 1, 0)
	owner.ResponseEpoch = 9
	_, err := owners.Create(t.Context(), owner, 10*time.Second)
	require.NoError(t, err)
	first, err := owners.Publish(t.Context(), owner.Key(), 9, true, 10*time.Second)
	require.NoError(t, err)
	second, err := owners.Publish(t.Context(), owner.Key(), 9, true, 10*time.Second)
	require.NoError(t, err, "an outcome-ambiguous committed publish must be confirmable with the exact key")
	require.False(t, second.ExpiresAt.Before(first.ExpiresAt))
	_, err = owners.Publish(t.Context(), owner.Key(), 8, false, 10*time.Second)
	require.ErrorIs(t, err, ErrStaleReconnect, "idempotent confirmation cannot regress epoch or boundary")
}

func TestMemoryReconnectOwnerCleanupGloballyRemovesHardDeadStatesAfterEveryCredentialExpires(t *testing.T) {
	manual := clock.NewManual(time.Unix(48_000, 0))
	owners := NewMemoryReconnectOwners(manual)
	insert := func(sessionID, ownerID string, state ReconnectOwnerState) {
		owner := testReconnectOwner(manual.Now(), ownerID, "credential-"+sessionID, 1, 0)
		owner.SessionID = identity.SessionID(sessionID)
		owner.Scope.ConversationID = identity.ConversationID("conversation-" + sessionID)
		owner.ExpiresAt = manual.Now().Add(2 * time.Second)
		_, err := owners.Create(t.Context(), owner, time.Second)
		require.NoError(t, err)
		if state != ReconnectOwnerEstablishing {
			_, err = owners.Publish(t.Context(), owner.Key(), 1, true, time.Second)
			require.NoError(t, err)
		}
		if state == ReconnectOwnerTombstoned {
			require.NoError(t, owners.Tombstone(t.Context(), owner.SessionID, owner.Incarnation, owner.OwnerID, 1, true))
		}
		if state == ReconnectOwnerClaiming {
			current, ok := owners.Read(owner.SessionID)
			require.True(t, ok)
			next := current
			next.Incarnation++
			next.OwnerID, next.CredentialID, next.State, next.TurnBoundary = current.OwnerID, "staged-"+sessionID, ReconnectOwnerClaiming, true
			_, _, err = owners.Claim(t.Context(), current.Claims(), next, time.Second)
			require.NoError(t, err)
		}
	}
	insert("foreign-active", "dead-foreign", ReconnectOwnerActive)
	insert("foreign-tombstoned", "dead-foreign", ReconnectOwnerTombstoned)
	insert("foreign-claiming", "dead-foreign", ReconnectOwnerClaiming)
	insert("local-establishing", "local", ReconnectOwnerEstablishing)
	live := testReconnectOwner(manual.Now(), "live-foreign", "credential-live", 1, 0)
	live.SessionID, live.Scope.ConversationID = "live-current", "conversation-live-current"
	live.ExpiresAt = manual.Now().Add(10 * time.Second)
	_, err := owners.Create(t.Context(), live, time.Second)
	require.NoError(t, err)
	_, err = owners.Publish(t.Context(), live.Key(), live.ResponseEpoch, true, time.Second)
	require.NoError(t, err)
	previousOverlap := testReconnectOwner(manual.Now(), "dead-overlap", "credential-current-overlap", 2, 0)
	previousOverlap.SessionID, previousOverlap.Scope.ConversationID = "live-previous-overlap", "conversation-live-previous-overlap"
	previousOverlap.ExpiresAt = manual.Now().Add(2 * time.Second)
	previousOverlap.OwnerLeaseExpiresAt = manual.Now().Add(time.Second)
	previousOverlap.State = ReconnectOwnerTombstoned
	previousOverlap.PreviousCredentialID, previousOverlap.PreviousIncarnation = "credential-previous-overlap", 1
	previousOverlap.PreviousExpiresAt, previousOverlap.PreviousAcceptUntil = manual.Now().Add(10*time.Second), manual.Now().Add(5*time.Second)
	owners.mu.Lock()
	owners.owners[previousOverlap.SessionID] = previousOverlap
	owners.mu.Unlock()
	manual.Advance(3 * time.Second)

	cleaned, err := owners.CleanupExpired(t.Context(), 10)
	require.NoError(t, err)
	require.Equal(t, 4, cleaned)
	for _, sessionID := range []identity.SessionID{"foreign-active", "foreign-tombstoned", "foreign-claiming", "local-establishing"} {
		_, exists := owners.Read(sessionID)
		require.False(t, exists, sessionID)
	}
	for _, sessionID := range []identity.SessionID{"live-current", "live-previous-overlap"} {
		_, exists := owners.Read(sessionID)
		require.True(t, exists, sessionID)
	}
}

func TestReconnectOwnerRotationKeepsOneBoundedOverlapAndMonotonicNonzeroCursor(t *testing.T) {
	manual := clock.NewManual(time.Unix(50_000, 0))
	owners := NewMemoryReconnectOwners(manual)
	current := testReconnectOwner(manual.Now(), "replica-a", "credential-one", 1, 4)
	current.ExpiresAt = manual.Now().Add(time.Minute)
	lease, err := owners.Create(t.Context(), current, 20*time.Second)
	require.NoError(t, err)
	current.OwnerLeaseExpiresAt = lease.ExpiresAt
	lease, err = owners.Publish(t.Context(), current.Key(), 3, true, 20*time.Second)
	require.NoError(t, err)
	current.OwnerLeaseExpiresAt = lease.ExpiresAt

	next := current.Claims()
	next.CredentialID, next.Cursor, next.ExpiresAt = "credential-two", 9, manual.Now().Add(70*time.Second)
	require.NoError(t, owners.Rotate(t.Context(), current.Key(), current.Claims(), next, manual.Now().Add(10*time.Second)))
	rotated, ok := owners.Read(current.SessionID)
	require.True(t, ok)
	require.Equal(t, int64(9), rotated.Cursor)
	require.Equal(t, current.CredentialID, rotated.PreviousCredentialID)
	require.Equal(t, current.Incarnation, rotated.PreviousIncarnation)
	require.Equal(t, current.Cursor, rotated.PreviousCursor)

	third := next
	third.CredentialID, third.Cursor, third.ExpiresAt = "credential-three", 15, manual.Now().Add(80*time.Second)
	require.NoError(t, owners.Rotate(t.Context(), rotated.Key(), next, third, manual.Now().Add(10*time.Second)))
	rotated, ok = owners.Read(current.SessionID)
	require.True(t, ok)
	require.Equal(t, "credential-two", rotated.PreviousCredentialID, "only the immediately acknowledged credential overlaps")
	require.Equal(t, next.Incarnation, rotated.PreviousIncarnation)
	require.Equal(t, int64(9), rotated.PreviousCursor)
	require.ErrorIs(t, owners.Rotate(t.Context(), rotated.Key(), third, ReconnectClaims{CredentialID: "backwards", Cursor: 8}, manual.Now().Add(time.Second)), ErrStaleReconnect)
}

func testReconnectOwner(now time.Time, ownerID, credentialID string, incarnation identity.Incarnation, cursor int64) ReconnectOwner {
	return ReconnectOwner{
		Scope:     handoff.Scope{TenantID: "tenant", UserID: "user", ConversationID: "conversation"},
		SessionID: "session", Incarnation: incarnation, Cursor: cursor, Model: "provider-model", ResponseEpoch: 1,
		CredentialID: credentialID, OwnerID: ownerID, State: ReconnectOwnerEstablishing,
		TurnBoundary: true, ExpiresAt: now.Add(time.Minute), SubjectExpiresAt: now.Add(time.Hour),
	}
}
