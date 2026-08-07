package postgres

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/jackc/pgx/v5"
)

type ConversationItem struct {
	SessionID   identity.SessionID
	Turn        identity.TurnKey
	Sequence    uint64
	Role        string
	Content     string
	ContentHash string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

func (r *Repository) AppendCommittedItem(ctx context.Context, scope handoff.Scope, item ConversationItem) (ConversationItem, error) {
	if err := validateContext(ctx); err != nil {
		return ConversationItem{}, err
	}
	if err := validateScope(scope); err != nil {
		return ConversationItem{}, err
	}
	if err := validateConversationItem(item); err != nil {
		return ConversationItem{}, err
	}
	item.CreatedAt = item.CreatedAt.UTC()
	if item.UpdatedAt.IsZero() {
		item.UpdatedAt = item.CreatedAt
	} else {
		item.UpdatedAt = item.UpdatedAt.UTC()
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return ConversationItem{}, err
	}
	defer rollback(tx)
	var owner bool
	err = tx.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM conversations WHERE tenant_id=$1 AND id=$2 AND user_id=$3 AND status <> 'deleted'
	)`, scope.TenantID, scope.ConversationID, scope.UserID).Scan(&owner)
	if err != nil {
		return ConversationItem{}, err
	}
	if !owner {
		return ConversationItem{}, handoff.ErrNotFound
	}

	var inserted ConversationItem
	err = scanConversationItem(tx.QueryRow(ctx, `
		INSERT INTO conversation_items (
			tenant_id, conversation_id, session_id, turn_id, turn_revision, sequence,
			role, content, content_hash, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		ON CONFLICT DO NOTHING
		RETURNING session_id, turn_id, turn_revision, sequence, role, content, content_hash, created_at, updated_at`,
		scope.TenantID, scope.ConversationID, item.SessionID, item.Turn.ID, item.Turn.Revision, item.Sequence,
		item.Role, item.Content, item.ContentHash, item.CreatedAt, item.UpdatedAt), &inserted)
	if err == nil {
		if err := tx.Commit(ctx); err != nil {
			return ConversationItem{}, err
		}
		return inserted, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return ConversationItem{}, mapWriteError(err)
	}

	var existing ConversationItem
	err = scanConversationItem(tx.QueryRow(ctx, `
		SELECT session_id, turn_id, turn_revision, sequence, role, content, content_hash, created_at, updated_at
		FROM conversation_items
		WHERE tenant_id=$1 AND conversation_id=$2
		  AND ((session_id=$3 AND sequence=$4) OR (turn_id=$5 AND turn_revision=$6 AND role=$7))
		ORDER BY (session_id=$3 AND sequence=$4) DESC
		LIMIT 1 FOR UPDATE`, scope.TenantID, scope.ConversationID, item.SessionID, item.Sequence,
		item.Turn.ID, item.Turn.Revision, item.Role), &existing)
	if errors.Is(err, pgx.ErrNoRows) {
		return ConversationItem{}, fmt.Errorf("%w: committed item identity collided", handoff.ErrIdempotencyConflict)
	}
	if err != nil {
		return ConversationItem{}, err
	}
	if !sameConversationItem(existing, item) {
		return ConversationItem{}, fmt.Errorf("%w: committed item replay differs", handoff.ErrIdempotencyConflict)
	}
	if err := tx.Commit(ctx); err != nil {
		return ConversationItem{}, err
	}
	return existing, nil
}

func (r *Repository) IsCommittedTurn(ctx context.Context, scope handoff.Scope, turn identity.TurnKey) (bool, error) {
	if err := validateContext(ctx); err != nil {
		return false, err
	}
	if err := validateScope(scope); err != nil {
		return false, err
	}
	if err := bounded("turn id", turn.ID, maxIdentityBytes, true); err != nil || turn.Revision == 0 {
		if err != nil {
			return false, err
		}
		return false, fmt.Errorf("%w: turn revision must be positive", handoff.ErrInvalidRequest)
	}
	var found bool
	err := r.pool.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM conversation_items i
		JOIN conversations c ON c.tenant_id=i.tenant_id AND c.id=i.conversation_id
		WHERE i.tenant_id=$1 AND i.conversation_id=$2 AND c.user_id=$3
		  AND i.turn_id=$4 AND i.turn_revision=$5 AND c.status <> 'deleted'
	)`, scope.TenantID, scope.ConversationID, scope.UserID, turn.ID, turn.Revision).Scan(&found)
	return found, err
}

func validateConversationItem(item ConversationItem) error {
	if err := bounded("session id", string(item.SessionID), maxIdentityBytes, true); err != nil {
		return err
	}
	if err := bounded("turn id", item.Turn.ID, maxIdentityBytes, true); err != nil {
		return err
	}
	if item.Turn.Revision == 0 {
		return fmt.Errorf("%w: turn revision must be positive", handoff.ErrInvalidRequest)
	}
	if item.Sequence > math.MaxInt64 {
		return fmt.Errorf("%w: committed sequence is too large", handoff.ErrInvalidRequest)
	}
	switch item.Role {
	case "system", "user", "assistant", "tool":
	default:
		return fmt.Errorf("%w: invalid committed item role", handoff.ErrInvalidRequest)
	}
	if err := bounded("content", item.Content, 65536, true); err != nil {
		return err
	}
	if err := bounded("content hash", item.ContentHash, 512, true); err != nil {
		return err
	}
	if item.CreatedAt.IsZero() {
		return fmt.Errorf("%w: committed timestamp is required", handoff.ErrInvalidRequest)
	}
	if !item.UpdatedAt.IsZero() && item.UpdatedAt.Before(item.CreatedAt) {
		return fmt.Errorf("%w: invalid committed timestamps", handoff.ErrInvalidRequest)
	}
	return nil
}

func scanConversationItem(row pgx.Row, out *ConversationItem) error {
	var revision int64
	var sequence int64
	err := row.Scan(&out.SessionID, &out.Turn.ID, &revision, &sequence, &out.Role, &out.Content, &out.ContentHash, &out.CreatedAt, &out.UpdatedAt)
	if err != nil {
		return err
	}
	if revision <= 0 || uint64(revision) > uint64(^uint32(0)) || sequence < 0 {
		return errors.New("postgres: invalid committed item numeric value")
	}
	out.Turn.Revision = uint32(revision)
	out.Sequence = uint64(sequence)
	out.CreatedAt = out.CreatedAt.UTC()
	out.UpdatedAt = out.UpdatedAt.UTC()
	return nil
}

func sameConversationItem(left, right ConversationItem) bool {
	return left.SessionID == right.SessionID && left.Turn == right.Turn && left.Sequence == right.Sequence &&
		left.Role == right.Role && left.Content == right.Content && left.ContentHash == right.ContentHash
}
