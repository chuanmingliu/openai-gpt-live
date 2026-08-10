//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/session"
	"github.com/chuanmingliu/openai-gpt-live/internal/store/postgres"
	"github.com/chuanmingliu/openai-gpt-live/internal/testkit"
	"github.com/chuanmingliu/openai-gpt-live/migrations"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func TestReconnectOwnersPostgresCrossReplicaCASAndDuplicateFence(t *testing.T) {
	pool := testkit.Postgres(t)
	require.NoError(t, migrations.Up(t.Context(), pool))
	owners, err := postgres.NewReconnectOwners(pool)
	require.NoError(t, err)
	now := time.Now().UTC()
	owner := session.ReconnectOwner{
		Scope:     handoff.Scope{TenantID: "tenant-owner", UserID: "user-owner", ConversationID: "conversation-owner"},
		SessionID: "session-owner", Incarnation: 1, Cursor: 7, Model: "provider-model", ResponseEpoch: 9,
		CredentialID: "credential-one", OwnerID: "replica-a", State: session.ReconnectOwnerEstablishing,
		ExpiresAt: now.Add(time.Minute), SubjectExpiresAt: now.Add(time.Hour),
	}
	_, err = owners.Create(t.Context(), owner, time.Second)
	require.NoError(t, err)
	_, err = owners.Publish(t.Context(), owner.Key(), owner.ResponseEpoch, true, time.Second)
	require.NoError(t, err)
	owner.State = session.ReconnectOwnerActive
	owner.TurnBoundary = true
	require.NoError(t, owners.Tombstone(t.Context(), owner.SessionID, owner.Incarnation, owner.OwnerID, identity.ResponseEpoch(12), true))
	next := owner
	next.Incarnation = 2
	next.CredentialID = "credential-two"
	next.OwnerID = "replica-b"
	next.State = session.ReconnectOwnerClaiming
	next.TurnBoundary = false
	next.PreviousAcceptUntil = now.Add(30 * time.Second)
	claimed, _, err := owners.Claim(t.Context(), owner.Claims(), next, time.Second)
	require.NoError(t, err)
	require.Equal(t, identity.ResponseEpoch(12), claimed.ResponseEpoch)
	_, _, err = owners.Claim(t.Context(), owner.Claims(), next, time.Second)
	require.ErrorIs(t, err, session.ErrStaleReconnect)
	_, err = owners.Publish(t.Context(), next.Key(), 13, true, time.Second)
	require.NoError(t, err)
}

func TestReconnectOwnersPostgresPostClaimCrashKeepsPriorAndAdvertisedStageRetryable(t *testing.T) {
	pool := testkit.Postgres(t)
	require.NoError(t, migrations.Up(t.Context(), pool))
	owners, err := postgres.NewReconnectOwners(pool)
	require.NoError(t, err)
	now := time.Now().UTC()
	prior := session.ReconnectOwner{
		Scope:     handoff.Scope{TenantID: "tenant-staged", UserID: "user-staged", ConversationID: "conversation-staged"},
		SessionID: "session-staged", Incarnation: 7, Cursor: 13, Model: "provider-model", ResponseEpoch: 21,
		CredentialID: "credential-prior", OwnerID: "replica-a", State: session.ReconnectOwnerEstablishing,
		TurnBoundary: true, ExpiresAt: now.Add(time.Minute), SubjectExpiresAt: now.Add(time.Hour),
	}
	_, err = owners.Create(t.Context(), prior, 150*time.Millisecond)
	require.NoError(t, err)
	_, err = owners.Publish(t.Context(), prior.Key(), prior.ResponseEpoch, true, 150*time.Millisecond)
	require.NoError(t, err)
	require.NoError(t, owners.Tombstone(t.Context(), prior.SessionID, prior.Incarnation, prior.OwnerID, prior.ResponseEpoch, true))

	staged := prior
	staged.Incarnation, staged.ResponseEpoch = 8, 22
	staged.OwnerID, staged.CredentialID, staged.State = "replica-b", "credential-staged", session.ReconnectOwnerClaiming
	staged.PreviousAcceptUntil = now.Add(30 * time.Second)
	_, _, err = owners.Claim(t.Context(), prior.Claims(), staged, 150*time.Millisecond)
	require.NoError(t, err)
	time.Sleep(175 * time.Millisecond)

	restageRequest := staged
	restageRequest.OwnerID, restageRequest.CredentialID = "replica-c", "must-not-replace-advertised-stage"
	claimedStage, _, err := owners.Claim(t.Context(), prior.Claims(), restageRequest, 150*time.Millisecond)
	require.NoError(t, err, "the prior token reclaims the same staged successor after a hard owner death")
	require.Equal(t, staged.Incarnation, claimedStage.Incarnation)
	require.Equal(t, staged.CredentialID, claimedStage.CredentialID)
	time.Sleep(175 * time.Millisecond)

	next := staged
	next.Incarnation, next.ResponseEpoch = 9, 23
	next.OwnerID, next.CredentialID = "replica-d", "credential-next"
	next.PreviousAcceptUntil = time.Now().UTC().Add(30 * time.Second)
	claimedAdvertised, _, err := owners.Claim(t.Context(), staged.Claims(), next, 150*time.Millisecond)
	require.NoError(t, err, "the advertised staged token advances after an ambiguous publish")
	require.Equal(t, staged.Incarnation, claimedAdvertised.Incarnation)
	require.Equal(t, staged.CredentialID, claimedAdvertised.CredentialID)
}

func TestReconnectOwnersPostgresAbortLeaseTakeoverRotationAndBoundedCleanup(t *testing.T) {
	pool := testkit.Postgres(t)
	require.NoError(t, migrations.Up(t.Context(), pool))
	owners, err := postgres.NewReconnectOwners(pool)
	require.NoError(t, err)
	now := time.Now().UTC()
	current := session.ReconnectOwner{
		Scope:     handoff.Scope{TenantID: "tenant-owner-residual", UserID: "user-owner", ConversationID: "conversation-owner"},
		SessionID: "session-owner-residual", Incarnation: 1, Cursor: 3, Model: "provider-model", ResponseEpoch: 4,
		CredentialID: "credential-current", OwnerID: "replica-a", State: session.ReconnectOwnerEstablishing,
		ExpiresAt: now.Add(time.Minute), SubjectExpiresAt: now.Add(time.Hour),
	}
	lease, err := owners.Create(t.Context(), current, 150*time.Millisecond)
	require.NoError(t, err)
	current.OwnerLeaseExpiresAt = lease.ExpiresAt
	lease, err = owners.Publish(t.Context(), current.Key(), 5, true, 150*time.Millisecond)
	require.NoError(t, err)
	current.OwnerLeaseExpiresAt, current.State, current.ResponseEpoch, current.TurnBoundary = lease.ExpiresAt, session.ReconnectOwnerActive, 5, true

	next := current
	next.Incarnation, next.OwnerID, next.CredentialID = 2, "replica-b", "credential-next"
	next.State, next.TurnBoundary, next.PreviousAcceptUntil = session.ReconnectOwnerClaiming, false, now.Add(30*time.Second)
	_, _, err = owners.Claim(t.Context(), current.Claims(), next, 150*time.Millisecond)
	require.ErrorIs(t, err, session.ErrStaleReconnect)
	time.Sleep(175 * time.Millisecond)
	previous, _, err := owners.Claim(t.Context(), current.Claims(), next, 150*time.Millisecond)
	require.NoError(t, err, "an expired live owner lease permits safe foreign takeover")
	require.NoError(t, owners.AbortClaim(t.Context(), next.Key(), previous))
	previous, _, err = owners.Claim(t.Context(), current.Claims(), next, 150*time.Millisecond)
	require.NoError(t, err, "exact abort restores the client-known token")
	_, err = owners.Publish(t.Context(), next.Key(), 6, true, 150*time.Millisecond)
	require.NoError(t, err)
	_, err = owners.Heartbeat(t.Context(), current.Key(), 7, true, time.Second)
	require.ErrorIs(t, err, session.ErrStaleReconnect, "the old owner is fenced after takeover")

	currentClaims := next.Claims()
	rotatedClaims := currentClaims
	rotatedClaims.CredentialID, rotatedClaims.Cursor, rotatedClaims.ExpiresAt = "credential-rotated", 11, now.Add(90*time.Second)
	require.NoError(t, owners.Rotate(t.Context(), next.Key(), currentClaims, rotatedClaims, time.Now().UTC().Add(20*time.Second)))
	thirdClaims := rotatedClaims
	thirdClaims.CredentialID, thirdClaims.Cursor, thirdClaims.ExpiresAt = "credential-third", 19, now.Add(100*time.Second)
	rotatedKey := next.Key()
	rotatedKey.CredentialID = rotatedClaims.CredentialID
	require.NoError(t, owners.Rotate(t.Context(), rotatedKey, rotatedClaims, thirdClaims, time.Now().UTC().Add(20*time.Second)))

	insertExpiredReconnectOwner(t, pool, "cleanup-owned", "replica-cleaner", "tombstoned")
	insertExpiredReconnectOwner(t, pool, "cleanup-active", "replica-foreign-a", "active")
	insertExpiredReconnectOwner(t, pool, "cleanup-foreign", "replica-foreign-b", "tombstoned")
	insertExpiredReconnectOwner(t, pool, "cleanup-claiming", "replica-foreign-c", "claiming")
	cleaned, err := owners.CleanupExpired(t.Context(), 10)
	require.NoError(t, err)
	require.Equal(t, 4, cleaned)
	for _, sessionID := range []string{"cleanup-owned", "cleanup-active", "cleanup-foreign", "cleanup-claiming"} {
		var present bool
		require.NoError(t, pool.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM session_reconnect_owners WHERE session_id=$1)`, sessionID).Scan(&present))
		require.False(t, present, "%s must be globally reclaimed after every authority window expires", sessionID)
	}
	var livePresent bool
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM session_reconnect_owners WHERE session_id=$1)`, next.SessionID).Scan(&livePresent))
	require.True(t, livePresent, "global cleanup must preserve a live current credential and owner lease")
}

func insertExpiredReconnectOwner(t *testing.T, pool interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}, sessionID, ownerID, state string) {
	t.Helper()
	_, err := pool.Exec(t.Context(), `INSERT INTO session_reconnect_owners(
		session_id,tenant_id,user_id,conversation_id,incarnation,event_cursor,provider_model,response_epoch,
		credential_id,owner_id,owner_state,turn_boundary,expires_at,owner_lease_expires_at,created_at,updated_at)
		VALUES($1,'tenant-cleanup','user-cleanup','conversation-cleanup',1,0,'provider-model',0,
			'credential-cleanup',$2,$3,TRUE,clock_timestamp()-interval '1 minute',clock_timestamp()-interval '1 minute',
			clock_timestamp()-interval '2 minutes',clock_timestamp()-interval '2 minutes')`, sessionID, ownerID, state)
	require.NoError(t, err)
}
