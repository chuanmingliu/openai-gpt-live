package postgres

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/session"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ReconnectOwners struct{ pool *pgxpool.Pool }

func NewReconnectOwners(pool *pgxpool.Pool) (*ReconnectOwners, error) {
	if pool == nil {
		return nil, session.ErrRecoveryUnavailable
	}
	return &ReconnectOwners{pool: pool}, nil
}

func (s *ReconnectOwners) Create(ctx context.Context, owner session.ReconnectOwner, leaseDuration time.Duration) (session.ReconnectOwnerLease, error) {
	if s == nil || s.pool == nil || owner.State != session.ReconnectOwnerEstablishing || leaseDuration <= 0 {
		return session.ReconnectOwnerLease{}, session.ErrStaleReconnect
	}
	var subjectExpiry any
	if !owner.SubjectExpiresAt.IsZero() {
		subjectExpiry = owner.SubjectExpiresAt
	}
	row := s.pool.QueryRow(ctx, `WITH db AS (SELECT clock_timestamp() AS now), inserted AS (
		INSERT INTO session_reconnect_owners(
		session_id,tenant_id,user_id,conversation_id,incarnation,event_cursor,provider_model,response_epoch,
		credential_id,owner_id,owner_state,turn_boundary,expires_at,subject_expires_at,owner_lease_expires_at,ephemeral,created_at,updated_at)
		SELECT $1,$2,$3,$4,$5::numeric,$6,$7,$8::numeric,$9,$10,$11,$12,$13,$14,
			db.now + $16::bigint * interval '1 millisecond',$15,db.now,db.now FROM db
		RETURNING owner_lease_expires_at)
		SELECT db.now,inserted.owner_lease_expires_at FROM db,inserted`,
		owner.SessionID, owner.Scope.TenantID, owner.Scope.UserID, owner.Scope.ConversationID,
		strconv.FormatUint(uint64(owner.Incarnation), 10), owner.Cursor, owner.Model,
		strconv.FormatUint(uint64(owner.ResponseEpoch), 10), owner.CredentialID, owner.OwnerID, owner.State,
		owner.TurnBoundary, owner.ExpiresAt, subjectExpiry, owner.Ephemeral, leaseDuration.Milliseconds())
	var lease session.ReconnectOwnerLease
	if err := row.Scan(&lease.DatabaseNow, &lease.ExpiresAt); err != nil {
		if ctx.Err() != nil {
			return session.ReconnectOwnerLease{}, ctx.Err()
		}
		return session.ReconnectOwnerLease{}, session.ErrStaleReconnect
	}
	lease.DatabaseNow, lease.ExpiresAt = lease.DatabaseNow.UTC(), lease.ExpiresAt.UTC()
	return lease, nil
}

func (s *ReconnectOwners) Claim(ctx context.Context, claims session.ReconnectClaims, next session.ReconnectOwner, leaseDuration time.Duration) (session.ReconnectOwner, session.ReconnectOwnerLease, error) {
	if s == nil || s.pool == nil || next.State != session.ReconnectOwnerClaiming || leaseDuration <= 0 ||
		uint64(claims.Incarnation) == ^uint64(0) || next.Incarnation != claims.Incarnation+1 ||
		next.Cursor != claims.Cursor || next.Model != claims.Model {
		return session.ReconnectOwner{}, session.ReconnectOwnerLease{}, session.ErrStaleReconnect
	}
	var subjectExpiry any
	if !next.SubjectExpiresAt.IsZero() {
		subjectExpiry = next.SubjectExpiresAt
	}
	var overlapUntil any
	if !next.PreviousAcceptUntil.IsZero() {
		overlapUntil = next.PreviousAcceptUntil
	}
	row := s.pool.QueryRow(ctx, `WITH db AS (SELECT clock_timestamp() AS now), previous AS (
		SELECT session_id,tenant_id,user_id,conversation_id,incarnation,event_cursor,provider_model,response_epoch,
			credential_id,owner_id,owner_state,turn_boundary,expires_at,subject_expires_at,
			previous_credential_id,previous_incarnation,previous_event_cursor,previous_expires_at,previous_accept_until,owner_lease_expires_at,ephemeral
		FROM session_reconnect_owners,db
		WHERE session_id=$1 AND tenant_id=$2 AND user_id=$3 AND conversation_id=$4
			AND provider_model=$7 AND turn_boundary
			AND ((incarnation=$5::numeric AND event_cursor=$6 AND credential_id=$8 AND expires_at=$9 AND expires_at>db.now) OR
				(previous_incarnation=$5::numeric AND previous_event_cursor=$6 AND previous_credential_id=$8 AND previous_expires_at=$9 AND
				 previous_expires_at>db.now AND previous_accept_until>db.now))
			AND (owner_state='tombstoned' OR owner_id=$10 OR owner_lease_expires_at<=db.now)
			AND (owner_state<>'claiming' OR owner_lease_expires_at<=db.now)
			AND (NOT (owner_state IN ('claiming','active') AND incarnation=$11::numeric AND previous_incarnation=$5::numeric AND
				previous_event_cursor=$6 AND previous_credential_id=$8) OR owner_lease_expires_at<=db.now)
			AND response_epoch<18446744073709551615
		FOR UPDATE
	), changed AS (
		UPDATE session_reconnect_owners AS owners SET
			incarnation=CASE WHEN previous.owner_state IN ('claiming','active') AND previous.incarnation=$11::numeric AND
				previous.previous_incarnation=$5::numeric AND previous.previous_event_cursor=$6 AND previous.previous_credential_id=$8
				THEN previous.incarnation ELSE $11::numeric END,
			event_cursor=CASE WHEN previous.owner_state IN ('claiming','active') AND previous.incarnation=$11::numeric AND
				previous.previous_incarnation=$5::numeric AND previous.previous_event_cursor=$6 AND previous.previous_credential_id=$8
				THEN previous.event_cursor ELSE $12 END,
			provider_model=$13,
			response_epoch=GREATEST($20::numeric,previous.response_epoch+1),
			credential_id=CASE WHEN previous.owner_state IN ('claiming','active') AND previous.incarnation=$11::numeric AND
				previous.previous_incarnation=$5::numeric AND previous.previous_event_cursor=$6 AND previous.previous_credential_id=$8
				THEN previous.credential_id ELSE $14 END,
			owner_id=$10,owner_state='claiming',turn_boundary=TRUE,
			expires_at=CASE WHEN previous.owner_state IN ('claiming','active') AND previous.incarnation=$11::numeric AND
				previous.previous_incarnation=$5::numeric AND previous.previous_event_cursor=$6 AND previous.previous_credential_id=$8
				THEN previous.expires_at ELSE $15 END,
			subject_expires_at=CASE WHEN previous.owner_state IN ('claiming','active') AND previous.incarnation=$11::numeric AND
				previous.previous_incarnation=$5::numeric AND previous.previous_event_cursor=$6 AND previous.previous_credential_id=$8
				THEN previous.subject_expires_at ELSE $16 END,
			previous_credential_id=CASE WHEN previous.owner_state IN ('claiming','active') AND previous.incarnation=$11::numeric AND
				previous.previous_incarnation=$5::numeric AND previous.previous_event_cursor=$6 AND previous.previous_credential_id=$8
				THEN previous.previous_credential_id ELSE $8 END,
			previous_incarnation=CASE WHEN previous.owner_state IN ('claiming','active') AND previous.incarnation=$11::numeric AND
				previous.previous_incarnation=$5::numeric AND previous.previous_event_cursor=$6 AND previous.previous_credential_id=$8
				THEN previous.previous_incarnation ELSE $5::numeric END,
			previous_event_cursor=CASE WHEN previous.owner_state IN ('claiming','active') AND previous.incarnation=$11::numeric AND
				previous.previous_incarnation=$5::numeric AND previous.previous_event_cursor=$6 AND previous.previous_credential_id=$8
				THEN previous.previous_event_cursor ELSE $6 END,
			previous_expires_at=CASE WHEN previous.owner_state IN ('claiming','active') AND previous.incarnation=$11::numeric AND
				previous.previous_incarnation=$5::numeric AND previous.previous_event_cursor=$6 AND previous.previous_credential_id=$8
				THEN previous.previous_expires_at ELSE $9 END,
			previous_accept_until=CASE WHEN previous.owner_state IN ('claiming','active') AND previous.incarnation=$11::numeric AND
				previous.previous_incarnation=$5::numeric AND previous.previous_event_cursor=$6 AND previous.previous_credential_id=$8
				THEN previous.previous_accept_until
				ELSE LEAST($9,COALESCE($18::timestamptz,db.now + $19::bigint * interval '1 millisecond')) END,
			owner_lease_expires_at=db.now + $19::bigint * interval '1 millisecond',
			ephemeral=CASE WHEN previous.owner_state IN ('claiming','active') AND previous.incarnation=$11::numeric AND
				previous.previous_incarnation=$5::numeric AND previous.previous_event_cursor=$6 AND previous.previous_credential_id=$8
				THEN previous.ephemeral ELSE $17 END,updated_at=db.now
		FROM previous,db WHERE owners.session_id=previous.session_id
		RETURNING previous.session_id,previous.tenant_id,previous.user_id,previous.conversation_id,
			previous.incarnation::text,previous.event_cursor,previous.provider_model,previous.response_epoch::text,
			previous.credential_id,previous.owner_id,previous.owner_state,previous.turn_boundary,previous.expires_at,
			previous.subject_expires_at,previous.previous_credential_id,previous.previous_incarnation::text,
			previous.previous_event_cursor,previous.previous_expires_at,previous.previous_accept_until,
			previous.owner_lease_expires_at,previous.ephemeral,db.now,owners.owner_lease_expires_at
	) SELECT * FROM changed`,
		claims.SessionID, claims.Scope.TenantID, claims.Scope.UserID, claims.Scope.ConversationID,
		strconv.FormatUint(uint64(claims.Incarnation), 10), claims.Cursor, claims.Model, claims.CredentialID, claims.ExpiresAt,
		next.OwnerID, strconv.FormatUint(uint64(next.Incarnation), 10), next.Cursor, next.Model, next.CredentialID,
		next.ExpiresAt, subjectExpiry, next.Ephemeral, overlapUntil, leaseDuration.Milliseconds(),
		strconv.FormatUint(uint64(next.ResponseEpoch), 10))
	owner, lease, err := scanClaimedReconnectOwner(row)
	if err != nil {
		if ctx.Err() != nil {
			return session.ReconnectOwner{}, session.ReconnectOwnerLease{}, ctx.Err()
		}
		if errors.Is(err, pgx.ErrNoRows) {
			return session.ReconnectOwner{}, session.ReconnectOwnerLease{}, session.ErrStaleReconnect
		}
		return session.ReconnectOwner{}, session.ReconnectOwnerLease{}, session.ErrRecoveryUnavailable
	}
	return owner, lease, nil
}

func (s *ReconnectOwners) AbortClaim(ctx context.Context, failed session.ReconnectOwnerKey, previous session.ReconnectOwner) error {
	if s == nil || s.pool == nil {
		return session.ErrStaleReconnect
	}
	var subjectExpiry, previousIncarnation, previousCursor, previousExpiresAt, previousAcceptUntil any
	if !previous.SubjectExpiresAt.IsZero() {
		subjectExpiry = previous.SubjectExpiresAt
	}
	if previous.PreviousCredentialID != "" {
		previousIncarnation = strconv.FormatUint(uint64(previous.PreviousIncarnation), 10)
		previousCursor, previousExpiresAt, previousAcceptUntil = previous.PreviousCursor, previous.PreviousExpiresAt, previous.PreviousAcceptUntil
	}
	result, err := s.pool.Exec(ctx, `UPDATE session_reconnect_owners SET
		tenant_id=$5,user_id=$6,conversation_id=$7,incarnation=$8::numeric,event_cursor=$9,provider_model=$10,
		response_epoch=$11::numeric,credential_id=$12,owner_id=$13,owner_state=$14,turn_boundary=$15,
		expires_at=$16,subject_expires_at=$17,previous_credential_id=$18,previous_incarnation=$19::numeric,previous_event_cursor=$20,
		previous_expires_at=$21,previous_accept_until=$22,owner_lease_expires_at=$23,ephemeral=$24,updated_at=clock_timestamp()
		WHERE session_id=$1 AND incarnation=$2::numeric AND owner_id=$3 AND credential_id=$4 AND owner_state='claiming'`,
		failed.SessionID, strconv.FormatUint(uint64(failed.Incarnation), 10), failed.OwnerID, failed.CredentialID,
		previous.Scope.TenantID, previous.Scope.UserID, previous.Scope.ConversationID,
		strconv.FormatUint(uint64(previous.Incarnation), 10), previous.Cursor, previous.Model,
		strconv.FormatUint(uint64(previous.ResponseEpoch), 10), previous.CredentialID, previous.OwnerID, previous.State,
		previous.TurnBoundary, previous.ExpiresAt, subjectExpiry, nullableString(previous.PreviousCredentialID), previousIncarnation,
		previousCursor, previousExpiresAt, previousAcceptUntil, previous.OwnerLeaseExpiresAt, previous.Ephemeral)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return session.ErrRecoveryUnavailable
	}
	if result.RowsAffected() != 1 {
		return session.ErrStaleReconnect
	}
	return nil
}

func (s *ReconnectOwners) Publish(ctx context.Context, key session.ReconnectOwnerKey, epoch identity.ResponseEpoch, boundary bool, leaseDuration time.Duration) (session.ReconnectOwnerLease, error) {
	return s.renew(ctx, key, epoch, boundary, leaseDuration, "establishing", "claiming", "active")
}

func (s *ReconnectOwners) Heartbeat(ctx context.Context, key session.ReconnectOwnerKey, epoch identity.ResponseEpoch, boundary bool, leaseDuration time.Duration) (session.ReconnectOwnerLease, error) {
	return s.renew(ctx, key, epoch, boundary, leaseDuration, "active")
}

func (s *ReconnectOwners) renew(ctx context.Context, key session.ReconnectOwnerKey, epoch identity.ResponseEpoch, boundary bool, leaseDuration time.Duration, states ...string) (session.ReconnectOwnerLease, error) {
	if s == nil || s.pool == nil || leaseDuration <= 0 || len(states) == 0 {
		return session.ReconnectOwnerLease{}, session.ErrStaleReconnect
	}
	row := s.pool.QueryRow(ctx, `WITH db AS (SELECT clock_timestamp() AS now), changed AS (
		UPDATE session_reconnect_owners SET owner_state='active',turn_boundary=$5,response_epoch=$6::numeric,
			owner_lease_expires_at=db.now + $7::bigint * interval '1 millisecond',updated_at=db.now
		FROM db WHERE session_id=$1 AND incarnation=$2::numeric AND owner_id=$3 AND credential_id=$4
			AND owner_state=ANY($8::text[]) AND owner_lease_expires_at>db.now
			AND (owner_state<>'active' OR response_epoch<$6::numeric OR
				(response_epoch=$6::numeric AND (NOT turn_boundary OR $5)))
		RETURNING db.now,owner_lease_expires_at)
		SELECT * FROM changed`, key.SessionID, strconv.FormatUint(uint64(key.Incarnation), 10), key.OwnerID, key.CredentialID,
		boundary, strconv.FormatUint(uint64(epoch), 10), leaseDuration.Milliseconds(), states)
	var lease session.ReconnectOwnerLease
	if err := row.Scan(&lease.DatabaseNow, &lease.ExpiresAt); err != nil {
		if ctx.Err() != nil {
			return session.ReconnectOwnerLease{}, ctx.Err()
		}
		if errors.Is(err, pgx.ErrNoRows) {
			return session.ReconnectOwnerLease{}, session.ErrStaleReconnect
		}
		return session.ReconnectOwnerLease{}, session.ErrRecoveryUnavailable
	}
	lease.DatabaseNow, lease.ExpiresAt = lease.DatabaseNow.UTC(), lease.ExpiresAt.UTC()
	return lease, nil
}

func (s *ReconnectOwners) Rotate(ctx context.Context, key session.ReconnectOwnerKey, current, next session.ReconnectClaims, overlapUntil time.Time) error {
	if s == nil || s.pool == nil {
		return session.ErrStaleReconnect
	}
	result, err := s.pool.Exec(ctx, `WITH db AS (SELECT clock_timestamp() AS now)
		UPDATE session_reconnect_owners SET previous_credential_id=credential_id,previous_incarnation=incarnation,previous_event_cursor=event_cursor,
			previous_expires_at=expires_at,previous_accept_until=$14,credential_id=$11,event_cursor=$10,expires_at=$12,updated_at=db.now
		FROM db WHERE session_id=$1 AND incarnation=$2::numeric AND owner_id=$3 AND credential_id=$4 AND owner_state='active'
			AND tenant_id=$5 AND user_id=$6 AND conversation_id=$7 AND event_cursor=$8 AND provider_model=$9 AND expires_at=$13
			AND owner_lease_expires_at>db.now AND $10 >= event_cursor AND $12 > db.now AND $14 > db.now AND $14 <= expires_at`,
		key.SessionID, strconv.FormatUint(uint64(key.Incarnation), 10), key.OwnerID, key.CredentialID,
		current.Scope.TenantID, current.Scope.UserID, current.Scope.ConversationID, current.Cursor, current.Model,
		next.Cursor, next.CredentialID, next.ExpiresAt, current.ExpiresAt, overlapUntil)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return session.ErrRecoveryUnavailable
	}
	if result.RowsAffected() != 1 {
		return session.ErrStaleReconnect
	}
	return nil
}

func (s *ReconnectOwners) Tombstone(ctx context.Context, sessionID identity.SessionID, incarnation identity.Incarnation, ownerID string, epoch identity.ResponseEpoch, boundary bool) error {
	if s == nil || s.pool == nil {
		return session.ErrStaleReconnect
	}
	result, err := s.pool.Exec(ctx, `UPDATE session_reconnect_owners SET owner_state='tombstoned',turn_boundary=$4,
		response_epoch=$5::numeric,updated_at=clock_timestamp()
		WHERE session_id=$1 AND incarnation=$2::numeric AND owner_id=$3 AND owner_state='active'`,
		sessionID, strconv.FormatUint(uint64(incarnation), 10), ownerID, boundary, strconv.FormatUint(uint64(epoch), 10))
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return session.ErrRecoveryUnavailable
	}
	if result.RowsAffected() != 1 {
		return session.ErrStaleReconnect
	}
	return nil
}

func (s *ReconnectOwners) Delete(ctx context.Context, sessionID identity.SessionID, incarnation identity.Incarnation, ownerID string) error {
	if s == nil || s.pool == nil {
		return session.ErrStaleReconnect
	}
	_, err := s.pool.Exec(ctx, `DELETE FROM session_reconnect_owners WHERE session_id=$1 AND incarnation=$2::numeric AND owner_id=$3`,
		sessionID, strconv.FormatUint(uint64(incarnation), 10), ownerID)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return session.ErrRecoveryUnavailable
	}
	return nil
}

func (s *ReconnectOwners) CleanupExpired(ctx context.Context, limit int) (int, error) {
	if s == nil || s.pool == nil || limit <= 0 || limit > 10_000 {
		return 0, session.ErrStaleReconnect
	}
	result, err := s.pool.Exec(ctx, `WITH db AS (SELECT clock_timestamp() AS now), candidates AS (
		SELECT owners.ctid FROM session_reconnect_owners owners,db
		WHERE owner_lease_expires_at<=db.now AND expires_at<=db.now
			AND (previous_credential_id IS NULL OR (previous_expires_at<=db.now AND previous_accept_until<=db.now))
		ORDER BY expires_at,session_id LIMIT $1 FOR UPDATE OF owners SKIP LOCKED
	) DELETE FROM session_reconnect_owners owners USING candidates WHERE owners.ctid=candidates.ctid`, limit)
	if err != nil {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		return 0, session.ErrRecoveryUnavailable
	}
	return int(result.RowsAffected()), nil
}

type reconnectOwnerRow interface{ Scan(...any) error }

func scanReconnectOwner(row reconnectOwnerRow) (session.ReconnectOwner, error) {
	var owner session.ReconnectOwner
	var tenantID, userID, conversationID, incarnation, responseEpoch string
	var subjectExpiry, previousExpiry, previousAcceptUntil *time.Time
	var previousCredentialID, previousIncarnation *string
	var previousCursor *int64
	if err := row.Scan(&owner.SessionID, &tenantID, &userID, &conversationID, &incarnation, &owner.Cursor, &owner.Model,
		&responseEpoch, &owner.CredentialID, &owner.OwnerID, &owner.State, &owner.TurnBoundary, &owner.ExpiresAt,
		&subjectExpiry, &previousCredentialID, &previousIncarnation, &previousCursor, &previousExpiry, &previousAcceptUntil,
		&owner.OwnerLeaseExpiresAt, &owner.Ephemeral); err != nil {
		return session.ReconnectOwner{}, err
	}
	parsedIncarnation, err := strconv.ParseUint(incarnation, 10, 64)
	if err != nil {
		return session.ReconnectOwner{}, session.ErrRecoveryUnavailable
	}
	parsedEpoch, err := strconv.ParseUint(responseEpoch, 10, 64)
	if err != nil {
		return session.ReconnectOwner{}, session.ErrRecoveryUnavailable
	}
	owner.Scope = handoff.Scope{TenantID: tenantID, UserID: userID, ConversationID: identity.ConversationID(conversationID)}
	owner.Incarnation = identity.Incarnation(parsedIncarnation)
	owner.ResponseEpoch = identity.ResponseEpoch(parsedEpoch)
	if subjectExpiry != nil {
		owner.SubjectExpiresAt = subjectExpiry.UTC()
	}
	if previousCredentialID != nil && previousIncarnation != nil && previousCursor != nil && previousExpiry != nil && previousAcceptUntil != nil {
		parsedPreviousIncarnation, parseErr := strconv.ParseUint(*previousIncarnation, 10, 64)
		if parseErr != nil {
			return session.ReconnectOwner{}, session.ErrRecoveryUnavailable
		}
		owner.PreviousIncarnation = identity.Incarnation(parsedPreviousIncarnation)
		owner.PreviousCredentialID, owner.PreviousCursor = *previousCredentialID, *previousCursor
		owner.PreviousExpiresAt, owner.PreviousAcceptUntil = previousExpiry.UTC(), previousAcceptUntil.UTC()
	}
	owner.ExpiresAt = owner.ExpiresAt.UTC()
	owner.OwnerLeaseExpiresAt = owner.OwnerLeaseExpiresAt.UTC()
	return owner, nil
}

func scanClaimedReconnectOwner(row reconnectOwnerRow) (session.ReconnectOwner, session.ReconnectOwnerLease, error) {
	var owner session.ReconnectOwner
	var tenantID, userID, conversationID, incarnation, responseEpoch string
	var subjectExpiry, previousExpiry, previousAcceptUntil *time.Time
	var previousCredentialID, previousIncarnation *string
	var previousCursor *int64
	var lease session.ReconnectOwnerLease
	if err := row.Scan(&owner.SessionID, &tenantID, &userID, &conversationID, &incarnation, &owner.Cursor, &owner.Model,
		&responseEpoch, &owner.CredentialID, &owner.OwnerID, &owner.State, &owner.TurnBoundary, &owner.ExpiresAt,
		&subjectExpiry, &previousCredentialID, &previousIncarnation, &previousCursor, &previousExpiry, &previousAcceptUntil,
		&owner.OwnerLeaseExpiresAt, &owner.Ephemeral, &lease.DatabaseNow, &lease.ExpiresAt); err != nil {
		return session.ReconnectOwner{}, session.ReconnectOwnerLease{}, err
	}
	parsedIncarnation, err := strconv.ParseUint(incarnation, 10, 64)
	if err != nil {
		return session.ReconnectOwner{}, session.ReconnectOwnerLease{}, session.ErrRecoveryUnavailable
	}
	parsedEpoch, err := strconv.ParseUint(responseEpoch, 10, 64)
	if err != nil {
		return session.ReconnectOwner{}, session.ReconnectOwnerLease{}, session.ErrRecoveryUnavailable
	}
	owner.Scope = handoff.Scope{TenantID: tenantID, UserID: userID, ConversationID: identity.ConversationID(conversationID)}
	owner.Incarnation, owner.ResponseEpoch = identity.Incarnation(parsedIncarnation), identity.ResponseEpoch(parsedEpoch)
	if subjectExpiry != nil {
		owner.SubjectExpiresAt = subjectExpiry.UTC()
	}
	if previousCredentialID != nil && previousIncarnation != nil && previousCursor != nil && previousExpiry != nil && previousAcceptUntil != nil {
		parsedPreviousIncarnation, parseErr := strconv.ParseUint(*previousIncarnation, 10, 64)
		if parseErr != nil {
			return session.ReconnectOwner{}, session.ReconnectOwnerLease{}, session.ErrRecoveryUnavailable
		}
		owner.PreviousIncarnation = identity.Incarnation(parsedPreviousIncarnation)
		owner.PreviousCredentialID, owner.PreviousCursor = *previousCredentialID, *previousCursor
		owner.PreviousExpiresAt, owner.PreviousAcceptUntil = previousExpiry.UTC(), previousAcceptUntil.UTC()
	}
	owner.ExpiresAt, owner.OwnerLeaseExpiresAt = owner.ExpiresAt.UTC(), owner.OwnerLeaseExpiresAt.UTC()
	lease.DatabaseNow, lease.ExpiresAt = lease.DatabaseNow.UTC(), lease.ExpiresAt.UTC()
	return owner, lease, nil
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

var _ session.ReconnectOwnerStore = (*ReconnectOwners)(nil)
