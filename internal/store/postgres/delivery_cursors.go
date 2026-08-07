package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/jackc/pgx/v5"
)

func (r *Repository) LoadDeliveryCursor(ctx context.Context, consumerID string, scope handoff.Scope) (int64, error) {
	if err := validateContext(ctx); err != nil {
		return 0, err
	}
	if err := validateCursorArguments(consumerID, scope, 0); err != nil {
		return 0, err
	}
	var sequence int64
	err := r.pool.QueryRow(ctx, `
		SELECT d.last_sequence FROM delivery_cursors d
		JOIN conversations c ON c.tenant_id=d.tenant_id AND c.id=d.conversation_id
		WHERE d.consumer_id=$1 AND d.tenant_id=$2 AND d.conversation_id=$3 AND c.user_id=$4 AND c.status <> 'deleted'`,
		consumerID, scope.TenantID, scope.ConversationID, scope.UserID).Scan(&sequence)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return sequence, err
}

func (r *Repository) AdvanceDeliveryCursor(ctx context.Context, consumerID string, scope handoff.Scope, sequence int64) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := validateCursorArguments(consumerID, scope, sequence); err != nil {
		return err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	now := r.clock.Now().UTC()
	_, err = tx.Exec(ctx, `
		INSERT INTO delivery_cursors (consumer_id,tenant_id,conversation_id,last_sequence,created_at,updated_at)
		SELECT $1,$2,$3,$4,$5,$5 FROM conversations
		WHERE tenant_id=$2 AND id=$3 AND user_id=$6 AND status <> 'deleted'
		ON CONFLICT (consumer_id,tenant_id,conversation_id) DO NOTHING`,
		consumerID, scope.TenantID, scope.ConversationID, 0, now, scope.UserID)
	if err != nil {
		return mapWriteError(err)
	}
	var current int64
	err = tx.QueryRow(ctx, `SELECT d.last_sequence FROM delivery_cursors d
		JOIN conversations c ON c.tenant_id=d.tenant_id AND c.id=d.conversation_id
		WHERE d.consumer_id=$1 AND d.tenant_id=$2 AND d.conversation_id=$3
		  AND c.user_id=$4 AND c.status <> 'deleted' FOR UPDATE OF d`,
		consumerID, scope.TenantID, scope.ConversationID, scope.UserID).Scan(&current)
	if errors.Is(err, pgx.ErrNoRows) {
		return handoff.ErrNotFound
	}
	if err != nil {
		return err
	}
	if sequence <= current {
		return tx.Commit(ctx)
	}
	var next int64
	err = tx.QueryRow(ctx, `SELECT e.seq FROM handoff_events e
		JOIN handoffs h ON h.tenant_id=e.tenant_id AND h.conversation_id=e.conversation_id AND h.id=e.handoff_id
		WHERE e.tenant_id=$1 AND e.conversation_id=$2 AND h.user_id=$3 AND e.seq>$4
		ORDER BY e.seq LIMIT 1`, scope.TenantID, scope.ConversationID, scope.UserID, current).Scan(&next)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && next != sequence) {
		return fmt.Errorf("%w: cursor must acknowledge the next scoped event", handoff.ErrRevisionConflict)
	}
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE delivery_cursors SET last_sequence=$4,updated_at=GREATEST(updated_at,$5)
		WHERE consumer_id=$1 AND tenant_id=$2 AND conversation_id=$3`,
		consumerID, scope.TenantID, scope.ConversationID, sequence, now)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func validateCursorArguments(consumerID string, scope handoff.Scope, sequence int64) error {
	if err := bounded("consumer id", consumerID, maxIdentityBytes, true); err != nil {
		return err
	}
	if err := validateScope(scope); err != nil {
		return err
	}
	if sequence < 0 {
		return fmt.Errorf("%w: cursor sequence must not be negative", handoff.ErrInvalidRequest)
	}
	return nil
}
