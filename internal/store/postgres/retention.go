package postgres

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/jackc/pgx/v5"
)

const maximumDeletionBatch = 1000

func (r *Repository) MarkConversationDeleting(ctx context.Context, scope handoff.Scope, actor, reasonCode string) error {
	if err := validateScope(scope); err != nil {
		return err
	}
	return r.markConversationDeleting(ctx, scope.TenantID, string(scope.ConversationID), scope.UserID, actor, reasonCode, false)
}

// MarkConversationDeletingAsAdmin is intentionally separate from the stable
// owner-scoped store contract. Only the authenticated admin route composes it.
func (r *Repository) MarkConversationDeletingAsAdmin(ctx context.Context, tenantID, conversationID, actor, reasonCode string) error {
	return r.markConversationDeleting(ctx, tenantID, conversationID, "", actor, reasonCode, true)
}

func (r *Repository) markConversationDeleting(ctx context.Context, tenantID, conversationID, owner, actor, reasonCode string, admin bool) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	for name, value := range map[string]string{"tenant id": tenantID, "conversation id": conversationID, "deletion actor": actor, "deletion reason": reasonCode} {
		if err := bounded(name, value, maxIdentityBytes, true); err != nil {
			return err
		}
	}
	if err := handoff.ValidateExternalDeletionReason(reasonCode); err != nil {
		return err
	}
	if !admin {
		if err := bounded("owner user id", owner, maxIdentityBytes, true); err != nil {
			return err
		}
	}
	now := r.clock.Now().UTC()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if err := r.lockEventAppend(ctx, tx); err != nil {
		return err
	}
	query := `SELECT status,deletion_actor,deletion_reason_code FROM conversations WHERE tenant_id=$1 AND id=$2 AND user_id=$3 FOR UPDATE`
	args := []any{tenantID, conversationID, owner}
	if admin {
		query = `SELECT status,deletion_actor,deletion_reason_code FROM conversations WHERE tenant_id=$1 AND id=$2 FOR UPDATE`
		args = args[:2]
	}
	var status string
	var existingActor, existingReason *string
	if err := tx.QueryRow(ctx, query, args...).Scan(&status, &existingActor, &existingReason); errors.Is(err, pgx.ErrNoRows) {
		return handoff.ErrNotFound
	} else if err != nil {
		return err
	}
	if err := r.after("deletion_after_conversation_lock"); err != nil {
		return err
	}
	if status == "deleted" {
		if existingActor != nil && existingReason != nil && (*existingActor != actor || *existingReason != reasonCode) {
			return handoff.ErrIdempotencyConflict
		}
		return tx.Commit(ctx)
	}
	command, err := tx.Exec(ctx, `UPDATE conversations SET status='deleted',deletion_actor=$3,deletion_reason_code=$4,deletion_requested_at=$5,updated_at=$5
		WHERE tenant_id=$1 AND id=$2 AND status<>'deleted'`, tenantID, conversationID, actor, reasonCode, now)
	if err != nil {
		return mapWriteError(err)
	}
	if command.RowsAffected() != 1 {
		return handoff.ErrIdempotencyConflict
	}
	if err := r.cancelConversationHandoffs(ctx, tx, tenantID, conversationID, actor, reasonCode, now); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *Repository) cancelConversationHandoffs(ctx context.Context, tx pgx.Tx, tenantID, conversationID, actor, reasonCode string, now time.Time) error {
	rows, err := tx.Query(ctx, `SELECT id,state,attempt FROM handoffs WHERE tenant_id=$1 AND conversation_id=$2
		AND state IN ('pending','leased','running','needs_input') AND cancel_requested_at IS NULL ORDER BY created_at,id FOR UPDATE`, tenantID, conversationID)
	if err != nil {
		return err
	}
	type item struct {
		id, state string
		attempt   uint32
	}
	var items []item
	for rows.Next() {
		var value item
		var attempt int64
		if err := rows.Scan(&value.id, &value.state, &attempt); err != nil {
			rows.Close()
			return err
		}
		if attempt < 0 {
			rows.Close()
			return errors.New("postgres: deletion handoff attempt is invalid")
		}
		value.attempt = uint32(attempt)
		items = append(items, value)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, value := range items {
		if _, err := tx.Exec(ctx, `UPDATE handoffs SET cancel_requested_at=$4,cancel_actor=$5,cancel_reason=$6,updated_at=$4
			WHERE tenant_id=$1 AND conversation_id=$2 AND id=$3 AND cancel_requested_at IS NULL`, tenantID, conversationID, value.id, now, actor, reasonCode); err != nil {
			return mapWriteError(err)
		}
		payload, _ := json.Marshal(struct {
			ReasonCode string `json:"reason_code"`
		}{"conversation_deleting"})
		eventID := deletionEventID(tenantID, conversationID, value.id)
		if err := r.appendWorkerEvent(ctx, tx, eventID, tenantID, conversationID, identity.HandoffID(value.id), "handoff.cancel_requested", handoff.State(value.state), value.attempt, payload, now, "conversation_delete"); err != nil {
			return err
		}
	}
	return nil
}

func (r *Repository) DeleteReadyConversations(ctx context.Context, now time.Time, batch int) (int, error) {
	if err := validateContext(ctx); err != nil {
		return 0, err
	}
	if now.IsZero() || batch < 1 || batch > maximumDeletionBatch {
		return 0, fmt.Errorf("%w: retention sweep bounds are invalid", handoff.ErrInvalidRequest)
	}
	r.sensitiveMu.RLock()
	defer r.sensitiveMu.RUnlock()
	if len(r.retentionHMACKey) < 32 || r.retentionHMACKeyID == "" {
		return 0, errors.New("postgres retention HMAC is not configured")
	}
	now = now.UTC()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer rollback(tx)
	if err := r.lockEventAppend(ctx, tx); err != nil {
		return 0, err
	}
	type candidate struct {
		tenant, id    string
		actor, reason *string
	}
	// Establish TTL tombstones first. Once marked, active leases cannot make
	// the same oldest rows consume this claim budget forever.
	rows, err := tx.Query(ctx, `SELECT tenant_id,id,deletion_actor,deletion_reason_code
		FROM conversations WHERE status<>'deleted' AND retention_until<=$1
		ORDER BY retention_until,tenant_id,id FOR UPDATE SKIP LOCKED LIMIT $2`, now, batch)
	if err != nil {
		return 0, err
	}
	ttl := make([]candidate, 0, batch)
	for rows.Next() {
		var value candidate
		if err := rows.Scan(&value.tenant, &value.id, &value.actor, &value.reason); err != nil {
			rows.Close()
			return 0, err
		}
		ttl = append(ttl, value)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	for _, value := range ttl {
		if _, err := tx.Exec(ctx, `UPDATE conversations SET status='deleted',deletion_actor='retention_sweeper',deletion_reason_code=$4,deletion_requested_at=$3,updated_at=$3
			WHERE tenant_id=$1 AND id=$2 AND status<>'deleted'`, value.tenant, value.id, now, handoff.DeletionReasonTTLExpired); err != nil {
			return 0, mapWriteError(err)
		}
		if err := r.cancelConversationHandoffs(ctx, tx, value.tenant, value.id, "retention_sweeper", "ttl_expired", now); err != nil {
			return 0, err
		}
	}

	rows, err = tx.Query(ctx, `SELECT c.tenant_id,c.id,c.deletion_actor,c.deletion_reason_code
		FROM conversations c WHERE c.status='deleted'
		AND NOT EXISTS(SELECT 1 FROM handoffs h WHERE h.tenant_id=c.tenant_id AND h.conversation_id=c.id
		  AND h.lease_owner IS NOT NULL AND h.lease_expires_at>$1)
		ORDER BY COALESCE(c.deletion_requested_at,c.retention_until),c.tenant_id,c.id
		FOR UPDATE OF c SKIP LOCKED LIMIT $2`, now, batch)
	if err != nil {
		return 0, err
	}
	values := make([]candidate, 0, batch)
	for rows.Next() {
		var value candidate
		if err := rows.Scan(&value.tenant, &value.id, &value.actor, &value.reason); err != nil {
			rows.Close()
			return 0, err
		}
		values = append(values, value)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	deleted := 0
	for _, value := range values {
		actor, reason := "retention_sweeper", handoff.DeletionReasonLegacyDeleted
		if value.actor != nil && value.reason != nil && handoff.ValidDeletionAuditReason(*value.reason) {
			actor, reason = *value.actor, *value.reason
		} else if _, err := tx.Exec(ctx, `UPDATE conversations SET deletion_actor=$3,deletion_reason_code=$4,deletion_requested_at=$5,updated_at=GREATEST(updated_at,$5)
			WHERE tenant_id=$1 AND id=$2 AND deletion_actor IS NULL`, value.tenant, value.id, actor, reason, now); err != nil {
			return 0, err
		}
		if _, err := tx.Exec(ctx, `UPDATE handoffs SET lease_owner=NULL,lease_expires_at=NULL,lease_version=lease_version+1,updated_at=$3
			WHERE tenant_id=$1 AND conversation_id=$2 AND lease_owner IS NOT NULL AND lease_expires_at<=$3`, value.tenant, value.id, now); err != nil {
			return 0, err
		}
		if _, err := tx.Exec(ctx, `UPDATE tool_invocations SET status='cancelled',updated_at=$3 WHERE tenant_id=$1 AND conversation_id=$2 AND status IN ('pending','running')`, value.tenant, value.id, now); err != nil {
			return 0, err
		}
		command, err := tx.Exec(ctx, `DELETE FROM conversations WHERE tenant_id=$1 AND id=$2 AND status='deleted'`, value.tenant, value.id)
		if err != nil {
			return 0, err
		}
		if command.RowsAffected() != 1 {
			continue
		}
		conversationHash := r.deletionConversationHashLocked(value.tenant, value.id)
		auditSum := sha256.Sum256([]byte("gpt-live/deletion/audit/v1\x00" + value.tenant + "\x00" + conversationHash))
		auditID := "deletion:" + hex.EncodeToString(auditSum[:])
		if _, err := tx.Exec(ctx, `INSERT INTO deletion_audits(audit_id,tenant_id,conversation_hash,actor,reason_code,deleted_at)
			VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT (audit_id) DO NOTHING`, auditID, value.tenant, conversationHash, actor, reason, now); err != nil {
			return 0, mapWriteError(err)
		}
		deleted++
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return deleted, nil
}

func (r *Repository) deletionConversationHash(tenantID, conversationID string) string {
	r.sensitiveMu.RLock()
	defer r.sensitiveMu.RUnlock()
	return r.deletionConversationHashLocked(tenantID, conversationID)
}

func (r *Repository) deletionConversationHashLocked(tenantID, conversationID string) string {
	mac := hmac.New(sha256.New, r.retentionHMACKey)
	_, _ = mac.Write([]byte("gpt-live/deletion/conversation/v1\x00"))
	_, _ = mac.Write([]byte(r.retentionHMACKeyID))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write([]byte(tenantID))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write([]byte(conversationID))
	return hex.EncodeToString(mac.Sum(nil))
}

func deletionEventID(tenantID, conversationID, handoffID string) string {
	sum := sha256.Sum256([]byte("gpt-live/deletion/mark/v1\x00" + tenantID + "\x00" + conversationID + "\x00" + handoffID))
	return "deletion-mark:" + hex.EncodeToString(sum[:])
}

var _ handoff.RetentionStore = (*Repository)(nil)
