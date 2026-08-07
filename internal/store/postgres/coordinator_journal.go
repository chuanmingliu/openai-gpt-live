package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/jackc/pgx/v5"
)

const (
	CoordinatorKindTranscript = "transcript"
	CoordinatorKindHandoff    = "handoff"
	maxCoordinatorPayload     = 256 << 10
	maxCoordinatorClaim       = 1
	CoordinatorMinLease       = 10 * time.Millisecond
	CoordinatorMaxLease       = 24 * time.Hour
)

type CoordinatorJournalRecord struct {
	WorkKey          string
	Kind             string
	Scope            handoff.Scope
	SessionID        identity.SessionID
	DeliveryAffinity string
	Incarnation      identity.Incarnation
	LocalSequence    uint64
	Payload          json.RawMessage
	PayloadHash      string
	StorageSequence  uint64
	CreatedAt        time.Time
	ClaimGeneration  uint64
}

var ErrCoordinatorAffinityInUse = errors.New("coordinator delivery affinity is already owned")

const coordinatorAffinityLockNamespace = "openai-gpt-live/coordinator-affinity/v1\x00"

type coordinatorAffinityLock struct {
	connection *pgx.Conn
	once       sync.Once
	err        error
}

var _ io.Closer = (*coordinatorAffinityLock)(nil)

func (r *Repository) AcquireCoordinatorAffinity(ctx context.Context, affinity string) (io.Closer, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	if err := bounded("delivery affinity", affinity, maxIdentityBytes, true); err != nil {
		return nil, err
	}
	poolConnection, err := r.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire coordinator affinity connection: %w", err)
	}
	var acquired bool
	if err := poolConnection.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, coordinatorAffinityLockID(affinity)).Scan(&acquired); err != nil {
		poolConnection.Release()
		return nil, fmt.Errorf("acquire coordinator affinity lock: %w", err)
	}
	if !acquired {
		poolConnection.Release()
		return nil, ErrCoordinatorAffinityInUse
	}
	// Session locks must never return to the pool. Hijacking and closing the
	// dedicated connection releases the affinity lock with the backend itself.
	return &coordinatorAffinityLock{connection: poolConnection.Hijack()}, nil
}

func coordinatorAffinityLockID(affinity string) int64 {
	digest := sha256.Sum256([]byte(coordinatorAffinityLockNamespace + affinity))
	return int64(binary.BigEndian.Uint64(digest[:8]))
}

func (l *coordinatorAffinityLock) Close() error {
	if l == nil || l.connection == nil {
		return errors.New("coordinator affinity lock is nil")
	}
	l.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := l.connection.Close(ctx); err != nil {
			l.err = fmt.Errorf("release coordinator affinity lock: %w", err)
		}
	})
	return l.err
}

func (r *Repository) AdmitCoordinatorWork(ctx context.Context, record CoordinatorJournalRecord) (CoordinatorJournalRecord, error) {
	if err := validateContext(ctx); err != nil {
		return CoordinatorJournalRecord{}, err
	}
	normalized, err := validateCoordinatorRecord(record)
	if err != nil {
		return CoordinatorJournalRecord{}, err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return CoordinatorJournalRecord{}, err
	}
	defer rollback(tx)

	existing, found, err := loadCoordinatorRecord(ctx, tx, normalized.WorkKey, true)
	if err != nil {
		return CoordinatorJournalRecord{}, err
	}
	if found {
		if !sameCoordinatorAdmission(existing, normalized) {
			return CoordinatorJournalRecord{}, handoff.ErrIdempotencyConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return CoordinatorJournalRecord{}, err
		}
		return existing, nil
	}

	var owner string
	err = tx.QueryRow(ctx, `
		SELECT user_id FROM conversations
		WHERE tenant_id=$1 AND id=$2 AND status <> 'deleted'
		FOR UPDATE`, normalized.Scope.TenantID, normalized.Scope.ConversationID).Scan(&owner)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && owner != normalized.Scope.UserID) {
		return CoordinatorJournalRecord{}, handoff.ErrNotFound
	}
	if err != nil {
		return CoordinatorJournalRecord{}, err
	}
	if err := r.after("journal_after_conversation_lock"); err != nil {
		return CoordinatorJournalRecord{}, err
	}

	var storage any
	if normalized.Kind == CoordinatorKindTranscript {
		var allocated int64
		err = tx.QueryRow(ctx, `
			SELECT GREATEST(
				COALESCE((SELECT max(sequence) FROM conversation_items WHERE tenant_id=$1 AND conversation_id=$2), 0),
				COALESCE((SELECT max(storage_sequence) FROM coordinator_work_journal WHERE tenant_id=$1 AND conversation_id=$2), 0)
			) + 1`, normalized.Scope.TenantID, normalized.Scope.ConversationID).Scan(&allocated)
		if err != nil {
			return CoordinatorJournalRecord{}, err
		}
		normalized.StorageSequence = uint64(allocated)
		storage = allocated
	}
	now := r.clock.Now().UTC()
	_, err = tx.Exec(ctx, `
		INSERT INTO coordinator_work_journal (
			work_key, kind, tenant_id, user_id, conversation_id, session_id, delivery_affinity,
			incarnation, local_sequence, payload, payload_hash, storage_sequence, created_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
		normalized.WorkKey, normalized.Kind, normalized.Scope.TenantID, normalized.Scope.UserID,
		normalized.Scope.ConversationID, normalized.SessionID, normalized.DeliveryAffinity, int64(normalized.Incarnation),
		int64(normalized.LocalSequence), normalized.Payload, normalized.PayloadHash, storage, now)
	if err != nil {
		return CoordinatorJournalRecord{}, mapWriteError(err)
	}
	normalized.CreatedAt = now
	if err := r.after("journal_after_insert"); err != nil {
		return CoordinatorJournalRecord{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return CoordinatorJournalRecord{}, err
	}
	return normalized, nil
}

func (r *Repository) ClaimCoordinatorWork(
	ctx context.Context,
	kind, affinity, owner string,
	lease time.Duration,
	limit int,
) ([]CoordinatorJournalRecord, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	if kind != CoordinatorKindTranscript && kind != CoordinatorKindHandoff {
		return nil, fmt.Errorf("%w: invalid coordinator kind", handoff.ErrInvalidRequest)
	}
	if err := bounded("delivery affinity", affinity, maxIdentityBytes, true); err != nil {
		return nil, err
	}
	leaseMicros, leaseErr := coordinatorLeaseMicroseconds(lease)
	if ownerErr := bounded("claim owner", owner, maxIdentityBytes, true); ownerErr != nil || leaseErr != nil || limit <= 0 || limit > maxCoordinatorClaim {
		if ownerErr != nil {
			return nil, ownerErr
		}
		if leaseErr != nil {
			return nil, leaseErr
		}
		return nil, fmt.Errorf("%w: invalid coordinator claim", handoff.ErrInvalidRequest)
	}
	rows, err := r.pool.Query(ctx, `
		WITH candidates AS (
			SELECT work_key FROM coordinator_work_journal
			WHERE kind=$1 AND delivery_affinity=$2 AND delivered=FALSE
			  AND (claim_until IS NULL OR claim_until <= clock_timestamp())
			ORDER BY created_at, work_key
			FOR UPDATE SKIP LOCKED
			LIMIT $3
		)
		UPDATE coordinator_work_journal j
		SET claim_owner=$4,
			claim_until=clock_timestamp() + ($5::bigint * interval '1 microsecond'),
			claim_generation=claim_generation + 1
		FROM candidates c
		WHERE j.work_key=c.work_key
		RETURNING j.work_key,j.kind,j.tenant_id,j.user_id,j.conversation_id,j.session_id,j.delivery_affinity,
			j.incarnation,j.local_sequence,j.payload,j.payload_hash,j.storage_sequence,j.created_at,j.claim_generation`,
		kind, affinity, limit, owner, leaseMicros)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []CoordinatorJournalRecord
	for rows.Next() {
		record, err := scanCoordinatorRecord(rows)
		if err != nil {
			return nil, err
		}
		if _, err := validateCoordinatorRecord(record); err != nil {
			return nil, fmt.Errorf("corrupt coordinator journal record: %w", err)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].CreatedAt.Equal(records[j].CreatedAt) {
			return records[i].WorkKey < records[j].WorkKey
		}
		return records[i].CreatedAt.Before(records[j].CreatedAt)
	})
	return records, nil
}

func (r *Repository) RenewCoordinatorWork(ctx context.Context, workKey, owner string, generation uint64, lease time.Duration) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := bounded("work key", workKey, maxIdentityBytes, true); err != nil {
		return err
	}
	leaseMicros, leaseErr := coordinatorLeaseMicroseconds(lease)
	if err := bounded("claim owner", owner, maxIdentityBytes, true); err != nil || leaseErr != nil || generation == 0 || generation > math.MaxInt64 {
		if err != nil {
			return err
		}
		if leaseErr != nil {
			return leaseErr
		}
		return fmt.Errorf("%w: invalid coordinator renewal", handoff.ErrInvalidRequest)
	}
	command, err := r.pool.Exec(ctx, `
		UPDATE coordinator_work_journal
		SET claim_until=clock_timestamp() + ($4::bigint * interval '1 microsecond')
		WHERE work_key=$1 AND claim_owner=$2 AND claim_generation=$3 AND delivered=FALSE
		  AND claim_until > clock_timestamp()`, workKey, owner, int64(generation), leaseMicros)
	if err != nil {
		return err
	}
	if command.RowsAffected() != 1 {
		return handoff.ErrNotFound
	}
	return nil
}

func (r *Repository) AckCoordinatorWork(ctx context.Context, workKey, owner string, generation uint64) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := bounded("work key", workKey, maxIdentityBytes, true); err != nil {
		return err
	}
	if err := bounded("claim owner", owner, maxIdentityBytes, true); err != nil || generation == 0 || generation > math.MaxInt64 {
		if err != nil {
			return err
		}
		return fmt.Errorf("%w: invalid coordinator acknowledgement", handoff.ErrInvalidRequest)
	}
	command, err := r.pool.Exec(ctx, `
		UPDATE coordinator_work_journal
		SET delivered=TRUE, delivered_at=clock_timestamp(), claim_owner=NULL, claim_until=NULL
		WHERE work_key=$1 AND claim_owner=$2 AND claim_generation=$3 AND delivered=FALSE
		  AND claim_until > clock_timestamp()`, workKey, owner, int64(generation))
	if err != nil {
		return err
	}
	if command.RowsAffected() != 1 {
		return handoff.ErrNotFound
	}
	return nil
}

func validateCoordinatorRecord(record CoordinatorJournalRecord) (CoordinatorJournalRecord, error) {
	if err := validateScope(record.Scope); err != nil {
		return CoordinatorJournalRecord{}, err
	}
	if err := bounded("work key", record.WorkKey, maxIdentityBytes, true); err != nil {
		return CoordinatorJournalRecord{}, err
	}
	if err := bounded("session id", string(record.SessionID), maxIdentityBytes, true); err != nil {
		return CoordinatorJournalRecord{}, err
	}
	if err := bounded("delivery affinity", record.DeliveryAffinity, maxIdentityBytes, true); err != nil {
		return CoordinatorJournalRecord{}, err
	}
	if record.Kind != CoordinatorKindTranscript && record.Kind != CoordinatorKindHandoff {
		return CoordinatorJournalRecord{}, fmt.Errorf("%w: invalid coordinator kind", handoff.ErrInvalidRequest)
	}
	if record.Incarnation == 0 || uint64(record.Incarnation) > math.MaxInt64 || record.LocalSequence > math.MaxInt64 {
		return CoordinatorJournalRecord{}, fmt.Errorf("%w: invalid coordinator sequence", handoff.ErrInvalidRequest)
	}
	if len(record.Payload) < 2 || len(record.Payload) > maxCoordinatorPayload || !json.Valid(record.Payload) {
		return CoordinatorJournalRecord{}, fmt.Errorf("%w: invalid coordinator payload", handoff.ErrInvalidRequest)
	}
	var compacted bytes.Buffer
	if err := json.Compact(&compacted, record.Payload); err != nil {
		return CoordinatorJournalRecord{}, fmt.Errorf("%w: invalid coordinator payload", handoff.ErrInvalidRequest)
	}
	record.Payload = json.RawMessage(compacted.String())
	hash := sha256.Sum256(record.Payload)
	wantHash := hex.EncodeToString(hash[:])
	if record.PayloadHash != "" && record.PayloadHash != wantHash {
		return CoordinatorJournalRecord{}, handoff.ErrIdempotencyConflict
	}
	record.PayloadHash = wantHash
	if record.Kind == CoordinatorKindTranscript && record.LocalSequence == 0 {
		return CoordinatorJournalRecord{}, fmt.Errorf("%w: transcript local sequence is required", handoff.ErrInvalidRequest)
	}
	return record, nil
}

func loadCoordinatorRecord(ctx context.Context, tx pgx.Tx, key string, lock bool) (CoordinatorJournalRecord, bool, error) {
	query := `SELECT work_key,kind,tenant_id,user_id,conversation_id,session_id,
		delivery_affinity,incarnation,local_sequence,payload,payload_hash,storage_sequence,created_at,claim_generation
		FROM coordinator_work_journal WHERE work_key=$1`
	if lock {
		query += ` FOR UPDATE`
	}
	record, err := scanCoordinatorRecord(tx.QueryRow(ctx, query, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return CoordinatorJournalRecord{}, false, nil
	}
	return record, err == nil, err
}

type coordinatorScanner interface{ Scan(...any) error }

func scanCoordinatorRecord(row coordinatorScanner) (CoordinatorJournalRecord, error) {
	var record CoordinatorJournalRecord
	var incarnation, local, generation int64
	var storage *int64
	err := row.Scan(&record.WorkKey, &record.Kind, &record.Scope.TenantID, &record.Scope.UserID,
		&record.Scope.ConversationID, &record.SessionID, &record.DeliveryAffinity, &incarnation, &local, &record.Payload,
		&record.PayloadHash, &storage, &record.CreatedAt, &generation)
	if err != nil {
		return CoordinatorJournalRecord{}, err
	}
	if incarnation <= 0 || local < 0 || generation < 0 || (storage != nil && *storage <= 0) {
		return CoordinatorJournalRecord{}, errors.New("postgres: corrupt coordinator journal numeric value")
	}
	record.Incarnation = identity.Incarnation(incarnation)
	record.LocalSequence = uint64(local)
	record.ClaimGeneration = uint64(generation)
	if storage != nil {
		record.StorageSequence = uint64(*storage)
	}
	record.CreatedAt = record.CreatedAt.UTC()
	return record, nil
}

func sameCoordinatorAdmission(left, right CoordinatorJournalRecord) bool {
	return left.WorkKey == right.WorkKey && left.Kind == right.Kind && left.Scope == right.Scope &&
		left.SessionID == right.SessionID && left.DeliveryAffinity == right.DeliveryAffinity && left.Incarnation == right.Incarnation &&
		left.LocalSequence == right.LocalSequence && left.PayloadHash == right.PayloadHash
}

func coordinatorLeaseMicroseconds(lease time.Duration) (int64, error) {
	if lease < CoordinatorMinLease || lease > CoordinatorMaxLease {
		return 0, fmt.Errorf("%w: invalid coordinator lease", handoff.ErrInvalidRequest)
	}
	return lease.Microseconds(), nil
}
