package postgres

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/session"
	"github.com/jackc/pgx/v5"
)

const maximumRecoveryHandoffs = 1000

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

// LoadRecovery reads one ownership-consistent, observational recovery view.
// The transaction is read-only and repeatable so items, active jobs, and the
// event high-water cannot come from different durable moments.
func (r *Repository) LoadRecovery(ctx context.Context, scope handoff.Scope, itemLimit, byteLimit int) (session.RecoverySnapshot, error) {
	if err := validateContext(ctx); err != nil {
		return session.RecoverySnapshot{}, err
	}
	if err := validateScope(scope); err != nil {
		return session.RecoverySnapshot{}, err
	}
	if itemLimit <= 0 || itemLimit > session.MaximumRecoveryItemLimit || byteLimit <= 0 || byteLimit > session.MaximumRecoveryByteLimit {
		return session.RecoverySnapshot{}, session.ErrInvalidRecoveryBounds
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return session.RecoverySnapshot{}, err
	}
	defer rollback(tx)

	var owned bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM conversations WHERE tenant_id=$1 AND id=$2 AND user_id=$3 AND status <> 'deleted'
	)`, scope.TenantID, scope.ConversationID, scope.UserID).Scan(&owned); err != nil {
		return session.RecoverySnapshot{}, err
	}
	if !owned {
		return session.RecoverySnapshot{}, handoff.ErrNotFound
	}

	rows, err := tx.Query(ctx, `
		WITH ordered AS MATERIALIZED (
			SELECT session_id,turn_id,turn_revision,sequence,role,content,created_at
			FROM conversation_items
			WHERE tenant_id=$1 AND conversation_id=$2 AND role IN ('user','assistant')
			ORDER BY sequence DESC,created_at DESC,session_id DESC,turn_id DESC,role DESC
			LIMIT $3
		), bounded AS (
			SELECT *,sum(octet_length(content)) OVER (
				ORDER BY sequence DESC,created_at DESC,session_id DESC,turn_id DESC,role DESC
				ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
			) AS cumulative_bytes
			FROM ordered
		)
		SELECT session_id,turn_id,turn_revision,sequence,role,content,created_at
		FROM bounded WHERE cumulative_bytes <= $4
		ORDER BY sequence DESC,created_at DESC,session_id DESC,turn_id DESC,role DESC`,
		scope.TenantID, scope.ConversationID, itemLimit, byteLimit)
	if err != nil {
		return session.RecoverySnapshot{}, err
	}
	newest := make([]session.CommittedItem, 0, itemLimit)
	retainedBytes := 0
	for rows.Next() {
		var item session.CommittedItem
		var revision, sequence int64
		if err := rows.Scan(&item.SessionID, &item.Turn.ID, &revision, &sequence, &item.Role, &item.Content, &item.CreatedAt); err != nil {
			rows.Close()
			return session.RecoverySnapshot{}, err
		}
		if revision <= 0 || revision > math.MaxUint32 || sequence < 0 {
			rows.Close()
			return session.RecoverySnapshot{}, session.ErrRecoveryUnavailable
		}
		if len(item.Content) > byteLimit-retainedBytes {
			break
		}
		item.Turn.Revision = uint32(revision)
		item.Sequence = uint64(sequence)
		item.CreatedAt = item.CreatedAt.UTC()
		retainedBytes += len(item.Content)
		newest = append(newest, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return session.RecoverySnapshot{}, err
	}
	rows.Close()
	items := make([]session.CommittedItem, len(newest))
	for index := range newest {
		items[len(newest)-1-index] = newest[index]
	}

	handoffRows, err := tx.Query(ctx, `SELECT id,tenant_id,conversation_id,user_id,state,objective,objective_revision,
		attempt,retry_count,presentation_hint,created_at,updated_at FROM handoffs
		WHERE tenant_id=$1 AND conversation_id=$2 AND user_id=$3
		  AND state IN ('pending','leased','running','needs_input')
		ORDER BY created_at,id LIMIT $4`, scope.TenantID, scope.ConversationID, scope.UserID, maximumRecoveryHandoffs+1)
	if err != nil {
		return session.RecoverySnapshot{}, err
	}
	active := make([]handoff.Snapshot, 0)
	for handoffRows.Next() {
		if len(active) == maximumRecoveryHandoffs {
			handoffRows.Close()
			return session.RecoverySnapshot{}, session.ErrRecoveryExhausted
		}
		snapshot, err := loadRecoveryHandoffRow(handoffRows)
		if err != nil {
			handoffRows.Close()
			return session.RecoverySnapshot{}, err
		}
		active = append(active, snapshot)
	}
	if err := handoffRows.Err(); err != nil {
		handoffRows.Close()
		return session.RecoverySnapshot{}, err
	}
	handoffRows.Close()

	approvalRows, err := tx.Query(ctx, `SELECT i.invocation_id,i.handoff_id,i.approval_state,i.approval_expires_at,i.tool_name,e.event_id,e.seq
		FROM tool_invocations i
		JOIN handoffs h ON h.tenant_id=i.tenant_id AND h.conversation_id=i.conversation_id AND h.id=i.handoff_id
		JOIN LATERAL (SELECT event_id,seq FROM handoff_events e WHERE e.tenant_id=i.tenant_id AND e.conversation_id=i.conversation_id
		  AND e.handoff_id=i.handoff_id AND e.kind='tool.approval_required' AND e.payload->>'invocation_id'=i.invocation_id
		  ORDER BY e.seq DESC LIMIT 1) e ON true
		WHERE i.tenant_id=$1 AND i.conversation_id=$2 AND h.user_id=$3 AND i.approval_state='pending' AND i.status='pending'
		ORDER BY e.seq,i.invocation_id LIMIT $4`, scope.TenantID, scope.ConversationID, scope.UserID, maximumRecoveryHandoffs+1)
	if err != nil {
		return session.RecoverySnapshot{}, err
	}
	pendingApprovals := make([]session.RecoveryApproval, 0)
	for approvalRows.Next() {
		if len(pendingApprovals) == maximumRecoveryHandoffs {
			approvalRows.Close()
			return session.RecoverySnapshot{}, session.ErrRecoveryExhausted
		}
		var recovery session.RecoveryApproval
		var toolName string
		if err := approvalRows.Scan(&recovery.Approval.InvocationID, &recovery.Approval.HandoffID, &recovery.Approval.ApprovalState,
			&recovery.Approval.ExpiresAt, &toolName, &recovery.EventID, &recovery.Sequence); err != nil {
			approvalRows.Close()
			return session.RecoverySnapshot{}, err
		}
		recovery.Approval.ExpiresAt = recovery.Approval.ExpiresAt.UTC()
		recovery.Approval.ActionSummary = approvalActionSummary(toolName)
		recovery.Approval.RiskClass = handoff.ApprovalRiskSideEffecting
		pendingApprovals = append(pendingApprovals, recovery)
	}
	if err := approvalRows.Err(); err != nil {
		approvalRows.Close()
		return session.RecoverySnapshot{}, err
	}
	approvalRows.Close()

	var lastEventSeq int64
	if err := tx.QueryRow(ctx, `SELECT COALESCE(max(seq),0) FROM handoff_events
		WHERE tenant_id=$1 AND conversation_id=$2`, scope.TenantID, scope.ConversationID).Scan(&lastEventSeq); err != nil {
		return session.RecoverySnapshot{}, err
	}
	if lastEventSeq < 0 {
		return session.RecoverySnapshot{}, session.ErrRecoveryUnavailable
	}
	if err := tx.Commit(ctx); err != nil {
		return session.RecoverySnapshot{}, err
	}
	return session.RecoverySnapshot{
		ConversationID: scope.ConversationID, CommittedItems: items,
		ActiveHandoffs: active, PendingApprovals: pendingApprovals, LastEventSeq: lastEventSeq,
	}, nil
}

func loadRecoveryHandoffRow(row pgx.Row) (handoff.Snapshot, error) {
	var snapshot handoff.Snapshot
	var tenant, conversation, user, state string
	var objectiveRevision, attempt, retryCount int64
	if err := row.Scan(&snapshot.ID, &tenant, &conversation, &user, &state, &snapshot.Request.Objective,
		&objectiveRevision, &attempt, &retryCount, &snapshot.Request.PresentationHint,
		&snapshot.CreatedAt, &snapshot.UpdatedAt); err != nil {
		return handoff.Snapshot{}, err
	}
	for _, value := range []int64{objectiveRevision, attempt, retryCount} {
		if value < 0 || value > math.MaxUint32 {
			return handoff.Snapshot{}, session.ErrRecoveryUnavailable
		}
	}
	snapshot.Request.Scope = handoff.Scope{
		TenantID: tenant, UserID: user, ConversationID: identity.ConversationID(conversation),
	}
	snapshot.State = handoff.State(state)
	snapshot.ObjectiveRevision = uint32(objectiveRevision)
	snapshot.Attempt, snapshot.RetryCount = uint32(attempt), uint32(retryCount)
	snapshot.CreatedAt, snapshot.UpdatedAt = snapshot.CreatedAt.UTC(), snapshot.UpdatedAt.UTC()
	return snapshot, nil
}

var _ session.RecoveryStore = (*Repository)(nil)

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
	var status string
	err = tx.QueryRow(ctx, `SELECT status FROM conversations
		WHERE tenant_id=$1 AND id=$2 AND user_id=$3 FOR UPDATE`,
		scope.TenantID, scope.ConversationID, scope.UserID).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) || status == "deleted" {
		return ConversationItem{}, handoff.ErrNotFound
	}
	if err != nil {
		return ConversationItem{}, err
	}
	if err := r.after("conversation_append_after_lock"); err != nil {
		return ConversationItem{}, err
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
