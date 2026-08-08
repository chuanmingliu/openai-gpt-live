package postgres

import (
	"context"
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

func (r *Repository) ResolveApproval(ctx context.Context, scope handoff.Scope, resolution handoff.ApprovalResolution) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := validateScope(scope); err != nil {
		return err
	}
	if err := handoff.ValidateApprovalResolution(resolution); err != nil {
		return err
	}
	if resolution.ActorTenantID != scope.TenantID || resolution.ActorUserID != scope.UserID {
		return handoff.ErrNotFound
	}
	resolution.ResolvedAt = resolution.ResolvedAt.UTC()
	reasonHash := sha256.Sum256([]byte(resolution.Reason))
	payload, _ := json.Marshal(struct {
		InvocationID      string                   `json:"invocation_id"`
		RequestedDecision handoff.ApprovalDecision `json:"requested_decision"`
		Outcome           string                   `json:"outcome"`
		ReasonHash        string                   `json:"reason_hash"`
	}{resolution.InvocationID, resolution.Decision, string(resolution.Decision), hex.EncodeToString(reasonHash[:])})
	eventID := approvalResolutionEventID(scope, resolution.IdempotencyKey)

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if err := r.lockEventAppend(ctx, tx); err != nil {
		return err
	}
	var conversationStatus string
	err = tx.QueryRow(ctx, `SELECT status FROM conversations WHERE tenant_id=$1 AND id=$2 AND user_id=$3 FOR UPDATE`, scope.TenantID, scope.ConversationID, scope.UserID).Scan(&conversationStatus)
	if errors.Is(err, pgx.ErrNoRows) || conversationStatus == "deleted" {
		return handoff.ErrNotFound
	}
	if err != nil {
		return err
	}
	matched, err := matchingApprovalEvent(ctx, tx, eventID, scope, resolution.InvocationID, payload)
	if err != nil {
		return err
	}
	if matched {
		return tx.Commit(ctx)
	}

	var handoffID string
	var approvalState, invocationStatus, handoffState string
	var expiry *time.Time
	var attempt int64
	err = tx.QueryRow(ctx, `SELECT i.handoff_id,i.approval_state,i.status,i.approval_expires_at,h.state,h.attempt
		FROM tool_invocations i JOIN handoffs h ON h.tenant_id=i.tenant_id AND h.conversation_id=i.conversation_id AND h.id=i.handoff_id
		WHERE i.invocation_id=$1 AND i.tenant_id=$2 AND i.conversation_id=$3 AND h.user_id=$4
		FOR UPDATE OF h,i`, resolution.InvocationID, scope.TenantID, scope.ConversationID, scope.UserID).
		Scan(&handoffID, &approvalState, &invocationStatus, &expiry, &handoffState, &attempt)
	if errors.Is(err, pgx.ErrNoRows) {
		return handoff.ErrNotFound
	}
	if err != nil {
		return err
	}
	if approvalState != "pending" || invocationStatus != "pending" || handoffState != string(handoff.StateNeedsInput) || expiry == nil {
		return handoff.ErrIdempotencyConflict
	}
	if attempt < 0 {
		return errors.New("postgres: approval attempt out of range")
	}
	if err := r.after("approval_after_invocation_lock"); err != nil {
		return err
	}

	decision := resolution.Decision
	if !expiry.After(resolution.ResolvedAt) {
		decision = "expired"
	}
	state := handoff.StatePending
	kind := "tool.approval_approved"
	if decision == handoff.ApprovalApproved {
		command, updateErr := tx.Exec(ctx, `UPDATE tool_invocations SET approval_state='approved',updated_at=$2
			WHERE invocation_id=$1 AND approval_state='pending' AND status='pending'`, resolution.InvocationID, resolution.ResolvedAt)
		if updateErr != nil || command.RowsAffected() != 1 {
			if updateErr != nil {
				return mapWriteError(updateErr)
			}
			return handoff.ErrIdempotencyConflict
		}
		command, updateErr = tx.Exec(ctx, `UPDATE handoffs SET state='pending',ready_at=$5,updated_at=$5
			WHERE tenant_id=$1 AND conversation_id=$2 AND id=$3 AND user_id=$4 AND state='needs_input' AND cancel_requested_at IS NULL`, scope.TenantID, scope.ConversationID, handoffID, scope.UserID, resolution.ResolvedAt)
		if updateErr != nil || command.RowsAffected() != 1 {
			if updateErr != nil {
				return mapWriteError(updateErr)
			}
			return handoff.ErrInvalidTransition
		}
	} else {
		state = handoff.StateFailed
		kind = "tool.approval_denied"
		code := "approval_denied"
		message := "tool approval denied"
		if decision == "expired" {
			kind, code, message = "tool.approval_expired", "approval_expired", "tool approval expired"
		}
		if err := failPendingApproval(ctx, tx, scope, resolution.InvocationID, handoffID, string(decision), code, message, eventID, resolution.ResolvedAt); err != nil {
			return err
		}
	}
	// Expiry is a server decision at the stored boundary, not a client choice.
	// Re-encode it so reconciliation matches the immutable committed outcome.
	if decision == "expired" {
		payload, _ = json.Marshal(struct {
			InvocationID      string                   `json:"invocation_id"`
			RequestedDecision handoff.ApprovalDecision `json:"requested_decision"`
			Outcome           string                   `json:"outcome"`
			ReasonHash        string                   `json:"reason_hash"`
		}{resolution.InvocationID, resolution.Decision, "expired", hex.EncodeToString(reasonHash[:])})
	}
	if err := r.appendWorkerEvent(ctx, tx, eventID, scope.TenantID, string(scope.ConversationID), identity.HandoffID(handoffID), kind, state, uint32(attempt), payload, resolution.ResolvedAt, "approval_resolution"); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		if r.reconcileApprovalCommit(ctx, eventID, scope, resolution.InvocationID, payload) {
			return nil
		}
		return err
	}
	if err := r.after("approval_commit"); err != nil {
		if r.reconcileApprovalCommit(ctx, eventID, scope, resolution.InvocationID, payload) {
			return nil
		}
		return err
	}
	return nil
}

func (r *Repository) reconcileApprovalCommit(ctx context.Context, eventID string, scope handoff.Scope, invocationID string, payload []byte) bool {
	reconcileCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	return r.approvalEventCommitted(reconcileCtx, eventID, scope, invocationID, payload)
}

func failPendingApproval(ctx context.Context, tx pgx.Tx, scope handoff.Scope, invocationID, handoffID, approvalState, code, message, mutationID string, now time.Time) error {
	command, err := tx.Exec(ctx, `UPDATE tool_invocations SET approval_state=$2,status='failed',updated_at=$3 WHERE invocation_id=$1 AND approval_state='pending' AND status='pending'`, invocationID, approvalState, now)
	if err != nil || command.RowsAffected() != 1 {
		if err != nil {
			return mapWriteError(err)
		}
		return handoff.ErrIdempotencyConflict
	}
	command, err = tx.Exec(ctx, `UPDATE handoffs SET state='failed',terminal_mutation_id=$5,failure_code=$6,failure_retryable=false,failure_message=$7,terminal_at=$8,updated_at=$8
		WHERE tenant_id=$1 AND conversation_id=$2 AND id=$3 AND user_id=$4 AND state='needs_input' AND cancel_requested_at IS NULL`, scope.TenantID, scope.ConversationID, handoffID, scope.UserID, mutationID, code, message, now)
	if err != nil || command.RowsAffected() != 1 {
		if err != nil {
			return mapWriteError(err)
		}
		return handoff.ErrInvalidTransition
	}
	return nil
}

func (r *Repository) LoadApproval(ctx context.Context, scope handoff.Scope, invocationID string) (handoff.ToolApproval, error) {
	if err := validateContext(ctx); err != nil {
		return handoff.ToolApproval{}, err
	}
	if err := validateScope(scope); err != nil {
		return handoff.ToolApproval{}, err
	}
	if err := bounded("invocation id", invocationID, maxIdentityBytes, true); err != nil {
		return handoff.ToolApproval{}, err
	}
	var value handoff.ToolApproval
	var toolName string
	err := r.pool.QueryRow(ctx, `SELECT i.invocation_id,i.handoff_id,i.approval_state,i.approval_expires_at,i.tool_name
		FROM tool_invocations i JOIN handoffs h ON h.tenant_id=i.tenant_id AND h.conversation_id=i.conversation_id AND h.id=i.handoff_id
		JOIN conversations c ON c.tenant_id=i.tenant_id AND c.id=i.conversation_id
		WHERE i.invocation_id=$1 AND i.tenant_id=$2 AND i.conversation_id=$3 AND h.user_id=$4 AND c.status<>'deleted'`, invocationID, scope.TenantID, scope.ConversationID, scope.UserID).
		Scan(&value.InvocationID, &value.HandoffID, &value.ApprovalState, &value.ExpiresAt, &toolName)
	if errors.Is(err, pgx.ErrNoRows) {
		return handoff.ToolApproval{}, handoff.ErrNotFound
	}
	if err != nil {
		return handoff.ToolApproval{}, err
	}
	value.ExpiresAt = value.ExpiresAt.UTC()
	value.ActionSummary = approvalActionSummary(toolName)
	value.RiskClass = handoff.ApprovalRiskSideEffecting
	return handoff.CloneToolApproval(value), nil
}

func matchingApprovalEvent(ctx context.Context, tx pgx.Tx, eventID string, scope handoff.Scope, invocationID string, payload []byte) (bool, error) {
	var tenant, conversation string
	var existing []byte
	err := tx.QueryRow(ctx, `SELECT tenant_id,conversation_id,payload::text FROM handoff_events WHERE event_id=$1`, eventID).Scan(&tenant, &conversation, &existing)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if tenant != scope.TenantID || conversation != string(scope.ConversationID) || !sameApprovalRequest(existing, payload, invocationID) {
		return false, handoff.ErrIdempotencyConflict
	}
	return true, nil
}

func sameApprovalRequest(existing, requested []byte, invocationID string) bool {
	type approvalEvent struct {
		InvocationID      string `json:"invocation_id"`
		RequestedDecision string `json:"requested_decision"`
		ReasonHash        string `json:"reason_hash"`
	}
	var left, right approvalEvent
	return json.Unmarshal(existing, &left) == nil && json.Unmarshal(requested, &right) == nil &&
		left == right && left.InvocationID == invocationID
}

func (r *Repository) approvalEventCommitted(ctx context.Context, eventID string, scope handoff.Scope, invocationID string, payload []byte) bool {
	var tenant, conversation string
	var existing []byte
	err := r.pool.QueryRow(ctx, `SELECT tenant_id,conversation_id,payload::text FROM handoff_events WHERE event_id=$1`, eventID).Scan(&tenant, &conversation, &existing)
	return err == nil && tenant == scope.TenantID && conversation == string(scope.ConversationID) && sameApprovalRequest(existing, payload, invocationID)
}

func approvalResolutionEventID(scope handoff.Scope, key string) string {
	sum := sha256.Sum256([]byte(scope.TenantID + "\x00" + string(scope.ConversationID) + "\x00" + scope.UserID + "\x00approval\x00" + key))
	return "approval:" + hex.EncodeToString(sum[:])
}

func (r *Repository) ExpireApprovals(ctx context.Context, now time.Time, batch int) (int, error) {
	if err := validateContext(ctx); err != nil {
		return 0, err
	}
	if now.IsZero() || batch < 1 || batch > 1000 {
		return 0, fmt.Errorf("%w: approval expiry bounds are invalid", handoff.ErrInvalidRequest)
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
	rows, err := tx.Query(ctx, `SELECT i.invocation_id,i.handoff_id,i.tenant_id,i.conversation_id,h.user_id,h.attempt
		FROM tool_invocations i JOIN handoffs h ON h.tenant_id=i.tenant_id AND h.conversation_id=i.conversation_id AND h.id=i.handoff_id
		JOIN conversations c ON c.tenant_id=i.tenant_id AND c.id=i.conversation_id
		WHERE i.approval_state='pending' AND i.status='pending' AND i.approval_expires_at<=$1
		  AND h.state='needs_input' AND h.cancel_requested_at IS NULL AND c.status<>'deleted'
		ORDER BY i.approval_expires_at,i.invocation_id FOR UPDATE OF i,h SKIP LOCKED LIMIT $2`, now, batch)
	if err != nil {
		return 0, err
	}
	type pending struct {
		invocation, handoff, tenant, conversation, user string
		attempt                                         uint32
	}
	values := make([]pending, 0, batch)
	for rows.Next() {
		var value pending
		var attempt int64
		if err := rows.Scan(&value.invocation, &value.handoff, &value.tenant, &value.conversation, &value.user, &attempt); err != nil {
			rows.Close()
			return 0, err
		}
		if attempt < 0 {
			rows.Close()
			return 0, errors.New("postgres: approval expiry attempt is invalid")
		}
		value.attempt = uint32(attempt)
		values = append(values, value)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	for _, value := range values {
		scope := handoff.Scope{TenantID: value.tenant, UserID: value.user, ConversationID: identity.ConversationID(value.conversation)}
		eventID := workerEventID(value.tenant, scope.ConversationID, identity.HandoffID(value.handoff), "approval_expired", value.invocation)
		if err := failPendingApproval(ctx, tx, scope, value.invocation, value.handoff, "expired", "approval_expired", "tool approval expired", eventID, now); err != nil {
			return 0, err
		}
		payload, _ := json.Marshal(struct {
			InvocationID string `json:"invocation_id"`
			Outcome      string `json:"outcome"`
		}{value.invocation, "expired"})
		if err := r.appendWorkerEvent(ctx, tx, eventID, value.tenant, value.conversation, identity.HandoffID(value.handoff), "tool.approval_expired", handoff.StateFailed, value.attempt, payload, now, "approval_expiry"); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(values), nil
}

var _ handoff.ApprovalStore = (*Repository)(nil)
