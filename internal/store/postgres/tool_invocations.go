package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/worker"
	"github.com/jackc/pgx/v5"
)

func (r *Repository) BeginToolInvocation(ctx context.Context, token worker.LeaseToken, record worker.ToolInvocationRecord) (worker.ToolInvocation, bool, error) {
	if err := validateContext(ctx); err != nil {
		return worker.ToolInvocation{}, false, err
	}
	if err := validateToken(token); err != nil {
		return worker.ToolInvocation{}, false, err
	}
	if err := validateToolRecord(token, record); err != nil {
		return worker.ToolInvocation{}, false, err
	}
	if r.toolGate == nil {
		return worker.ToolInvocation{}, false, worker.ErrToolNotRegistered
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return worker.ToolInvocation{}, false, err
	}
	defer rollback(tx)
	lease, expires, err := r.lockLease(ctx, tx, token)
	now := r.clock.Now().UTC()
	if err != nil || !expires.After(now) {
		if err == nil {
			err = worker.ErrLeaseLost
		}
		return worker.ToolInvocation{}, false, err
	}
	if lease.state != handoff.StateRunning || lease.cancelRequested {
		return worker.ToolInvocation{}, false, worker.ErrLeaseLost
	}
	var allowlistRaw, policyRaw []byte
	var registryVersion string
	var objectiveRevision int64
	err = tx.QueryRow(ctx, `SELECT tool_allowlist::text,approval_policy::text,tool_registry_version,objective_revision
		FROM handoffs WHERE tenant_id=$1 AND conversation_id=$2 AND id=$3`, lease.tenant, lease.conversation, token.HandoffID).
		Scan(&allowlistRaw, &policyRaw, &registryVersion, &objectiveRevision)
	if err != nil {
		return worker.ToolInvocation{}, false, err
	}
	if objectiveRevision < 0 || objectiveRevision > math.MaxUint32 {
		return worker.ToolInvocation{}, false, errors.New("postgres: objective revision out of range")
	}
	if uint32(objectiveRevision) != record.ObjectiveRevision {
		return worker.ToolInvocation{}, false, worker.ErrLeaseLost
	}
	var allowlist []string
	if err := json.Unmarshal(allowlistRaw, &allowlist); err != nil {
		return worker.ToolInvocation{}, false, err
	}
	allowed := false
	for _, name := range allowlist {
		if name == record.ToolName {
			allowed = true
			break
		}
	}
	if !allowed {
		return worker.ToolInvocation{}, false, worker.ErrToolNotAllowed
	}
	if !r.toolGate.Registered(ctx, registryVersion, record.ToolName) {
		return worker.ToolInvocation{}, false, worker.ErrToolNotRegistered
	}
	requiresApproval, err := approvalRequired(policyRaw)
	if err != nil {
		return worker.ToolInvocation{}, false, err
	}

	existing, approvalState, approvalExpires, existingAttempt, found, err := loadToolInvocation(ctx, tx, lease, token, record)
	if err != nil {
		return worker.ToolInvocation{}, false, err
	}
	if found {
		if !sameToolIdentity(existing.Record, record) {
			return worker.ToolInvocation{}, false, handoff.ErrIdempotencyConflict
		}
		if existing.Status == "succeeded" {
			if err := tx.Commit(ctx); err != nil {
				return worker.ToolInvocation{}, false, err
			}
			return worker.CloneToolInvocation(existing), false, nil
		}
		validateApproval := func() error {
			if !requiresApproval {
				if approvalState != "not_required" {
					return worker.ErrApprovalRequired
				}
				return nil
			}
			switch approvalState {
			case "approved":
				if approvalExpires == nil || !approvalExpires.After(now) {
					return worker.ErrApprovalExpired
				}
			case "denied":
				return worker.ErrApprovalDenied
			case "expired":
				return worker.ErrApprovalExpired
			default:
				return worker.ErrApprovalRequired
			}
			return nil
		}
		if err := validateApproval(); err != nil {
			return existing, false, err
		}
		switch existing.Status {
		case "pending":
			if !requiresApproval || approvalState != "approved" {
				return existing, false, worker.ErrApprovalRequired
			}
			command, updateErr := tx.Exec(ctx, `UPDATE tool_invocations i SET status='running',attempt=$1,updated_at=$2
				WHERE i.tenant_id=$3 AND i.conversation_id=$4 AND i.handoff_id=$5 AND i.invocation_id=$6 AND i.status='pending'
				  AND i.attempt=$10 AND i.objective_revision=$9 AND i.approval_state='approved' AND i.approval_expires_at>$2
				  AND i.result IS NULL AND i.mutation_id IS NULL
				  AND EXISTS (SELECT 1 FROM handoffs h WHERE h.tenant_id=$3 AND h.conversation_id=$4 AND h.id=$5
				    AND h.lease_owner=$7 AND h.lease_version=$8 AND h.attempt=$1 AND h.state='running'
				    AND h.lease_expires_at>$2 AND h.cancel_requested_at IS NULL AND h.objective_revision=$9)`, token.Attempt, now, token.TenantID, token.ConversationID, token.HandoffID, record.InvocationID, token.WorkerID, token.Version, record.ObjectiveRevision, existingAttempt)
			err = updateErr
			if err != nil {
				return worker.ToolInvocation{}, false, mapWriteError(err)
			}
			if command.RowsAffected() != 1 {
				return worker.ToolInvocation{}, false, worker.ErrLeaseLost
			}
			if err := r.after("tool_invocation_pending_rebind"); err != nil {
				return worker.ToolInvocation{}, false, err
			}
			existing.Status = "running"
		case "running":
			if existingAttempt != token.Attempt {
				command, updateErr := tx.Exec(ctx, `UPDATE tool_invocations i SET attempt=$1,updated_at=$2
					WHERE i.tenant_id=$3 AND i.conversation_id=$4 AND i.handoff_id=$5 AND i.invocation_id=$6
					  AND i.attempt=$10 AND i.objective_revision=$9 AND i.status='running' AND i.approval_state=$11
					  AND i.result IS NULL AND i.mutation_id IS NULL
					  AND ($11='not_required' OR i.approval_expires_at>$2)
					  AND EXISTS (SELECT 1 FROM handoffs h WHERE h.tenant_id=$3 AND h.conversation_id=$4 AND h.id=$5
					    AND h.lease_owner=$7 AND h.lease_version=$8 AND h.attempt=$1 AND h.state='running'
					    AND h.lease_expires_at>$2 AND h.cancel_requested_at IS NULL AND h.objective_revision=$9)`, token.Attempt, now, token.TenantID, token.ConversationID, token.HandoffID, record.InvocationID, token.WorkerID, token.Version, record.ObjectiveRevision, existingAttempt, approvalState)
				err = updateErr
				if err != nil {
					return worker.ToolInvocation{}, false, mapWriteError(err)
				}
				if command.RowsAffected() != 1 {
					return worker.ToolInvocation{}, false, worker.ErrLeaseLost
				}
				if err := r.after("tool_invocation_running_rebind"); err != nil {
					return worker.ToolInvocation{}, false, err
				}
			}
		default:
			return worker.ToolInvocation{}, false, worker.ErrLeaseLost
		}
		if err := tx.Commit(ctx); err != nil {
			return worker.ToolInvocation{}, false, err
		}
		return worker.CloneToolInvocation(existing), false, nil
	}
	approvalState = "not_required"
	status := "running"
	if requiresApproval {
		approvalState = "pending"
		status = "pending"
	}
	command, err := tx.Exec(ctx, `INSERT INTO tool_invocations(invocation_id,tenant_id,conversation_id,handoff_id,attempt,objective_revision,tool_ordinal,tool_name,approval_state,idempotency_key,canonical_arguments_hash,arguments_ciphertext,arguments_nonce,arguments_key_id,status,created_at,updated_at)
		SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$16 FROM handoffs h
		WHERE h.tenant_id=$2 AND h.conversation_id=$3 AND h.id=$4 AND h.lease_owner=$17 AND h.lease_version=$18 AND h.attempt=$5
		  AND h.state='running' AND h.lease_expires_at>$16 AND h.cancel_requested_at IS NULL AND h.objective_revision=$6`, record.InvocationID, lease.tenant, lease.conversation, token.HandoffID, token.Attempt, record.ObjectiveRevision, record.ToolOrdinal, record.ToolName, approvalState, record.IdempotencyKey, record.ArgumentsHash, record.Ciphertext, record.Nonce, record.KeyID, status, now, token.WorkerID, token.Version)
	if err != nil {
		return worker.ToolInvocation{}, false, mapWriteError(err)
	}
	if command.RowsAffected() != 1 {
		return worker.ToolInvocation{}, false, worker.ErrLeaseLost
	}
	if err := r.after("tool_invocation_insert"); err != nil {
		return worker.ToolInvocation{}, false, err
	}
	invocation := worker.ToolInvocation{Record: cloneToolRecord(record), Status: status}
	if err := tx.Commit(ctx); err != nil {
		return worker.ToolInvocation{}, false, err
	}
	if requiresApproval {
		return invocation, true, worker.ErrApprovalRequired
	}
	return invocation, true, nil
}

func (r *Repository) CompleteToolInvocation(ctx context.Context, token worker.LeaseToken, completion worker.ToolCompletionRecord) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := validateToken(token); err != nil {
		return err
	}
	if err := bounded("invocation id", completion.InvocationID, maxIdentityBytes, true); err != nil {
		return err
	}
	if err := bounded("mutation id", completion.MutationID, maxIdentityBytes, true); err != nil {
		return err
	}
	result, err := canonicalWorkerJSON(completion.Result)
	if err != nil {
		return err
	}
	completion.Result = result
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	lease, expires, err := r.lockLease(ctx, tx, token)
	if err != nil {
		return err
	}
	now := r.clock.Now().UTC()
	if !expires.After(now) {
		return worker.ErrLeaseLost
	}
	if lease.state != handoff.StateRunning || lease.cancelRequested {
		return worker.ErrLeaseLost
	}
	var currentObjective int64
	err = tx.QueryRow(ctx, `SELECT objective_revision FROM handoffs WHERE tenant_id=$1 AND conversation_id=$2 AND id=$3`, token.TenantID, token.ConversationID, token.HandoffID).Scan(&currentObjective)
	if err != nil || currentObjective < 0 || currentObjective > math.MaxUint32 {
		if err != nil {
			return err
		}
		return errors.New("postgres: objective revision out of range")
	}
	var status string
	var invocationAttempt, invocationObjective int64
	var existingMutation sql.NullString
	var existingResult []byte
	err = tx.QueryRow(ctx, `SELECT status,attempt,objective_revision,mutation_id,COALESCE(result::text,'')
		FROM tool_invocations WHERE tenant_id=$1 AND conversation_id=$2 AND handoff_id=$3 AND invocation_id=$4 FOR UPDATE`, token.TenantID, token.ConversationID, token.HandoffID, completion.InvocationID).
		Scan(&status, &invocationAttempt, &invocationObjective, &existingMutation, &existingResult)
	if errors.Is(err, pgx.ErrNoRows) {
		return worker.ErrLeaseLost
	}
	if err != nil {
		return err
	}
	if invocationAttempt != int64(token.Attempt) || invocationObjective != currentObjective {
		return worker.ErrLeaseLost
	}
	if status == "succeeded" {
		if !existingMutation.Valid || existingMutation.String != completion.MutationID || !bytes.Equal(existingResult, result) {
			return handoff.ErrIdempotencyConflict
		}
		return tx.Commit(ctx)
	}
	if status != "running" {
		return worker.ErrLeaseLost
	}
	command, err := tx.Exec(ctx, `UPDATE tool_invocations i SET status='succeeded',result=$1::json,mutation_id=$2,updated_at=$3
		WHERE i.tenant_id=$4 AND i.conversation_id=$5 AND i.handoff_id=$6 AND i.invocation_id=$7 AND i.attempt=$8 AND i.objective_revision=$9 AND i.status='running'
		  AND EXISTS (SELECT 1 FROM handoffs h WHERE h.tenant_id=$4 AND h.conversation_id=$5 AND h.id=$6
		    AND h.lease_owner=$10 AND h.lease_version=$11 AND h.attempt=$8 AND h.state='running' AND h.lease_expires_at>$3 AND h.cancel_requested_at IS NULL)`,
		result, completion.MutationID, now, token.TenantID, token.ConversationID, token.HandoffID, completion.InvocationID, token.Attempt, currentObjective, token.WorkerID, token.Version)
	if err != nil {
		return mapWriteError(err)
	}
	if command.RowsAffected() != 1 {
		return worker.ErrLeaseLost
	}
	if err := r.after("tool_invocation_complete"); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func validateToolRecord(token worker.LeaseToken, record worker.ToolInvocationRecord) error {
	for name, value := range map[string]string{"invocation id": record.InvocationID, "idempotency key": record.IdempotencyKey, "tool name": record.ToolName, "arguments hash": record.ArgumentsHash, "key id": record.KeyID} {
		if err := bounded(name, value, maxIdentityBytes, true); err != nil {
			return err
		}
	}
	if record.ToolOrdinal > math.MaxInt32 {
		return fmt.Errorf("%w: tool ordinal out of range", handoff.ErrInvalidRequest)
	}
	decoded, err := hex.DecodeString(record.ArgumentsHash)
	if err != nil || len(decoded) != 32 || hex.EncodeToString(decoded) != record.ArgumentsHash {
		return fmt.Errorf("%w: arguments hash must be SHA-256 hex", handoff.ErrInvalidRequest)
	}
	expected := worker.StableToolInvocationID(token.TenantID, token.ConversationID, token.HandoffID, record.ObjectiveRevision, record.ToolOrdinal, record.ToolName, record.ArgumentsHash)
	if record.InvocationID != expected || record.IdempotencyKey != expected {
		return handoff.ErrIdempotencyConflict
	}
	if len(record.Ciphertext) < 16 || len(record.Ciphertext) > 65552 || len(record.Nonce) != 12 {
		return fmt.Errorf("%w: invalid encrypted arguments", handoff.ErrInvalidRequest)
	}
	return nil
}

func approvalRequired(raw []byte) (bool, error) {
	var policy struct {
		Mode string `json:"mode"`
	}
	if err := json.Unmarshal(raw, &policy); err != nil {
		return false, err
	}
	switch policy.Mode {
	case "none", "not_required", "automatic", "allow":
		return false, nil
	default:
		return true, nil
	}
}

func loadToolInvocation(ctx context.Context, tx pgx.Tx, lease leaseRow, token worker.LeaseToken, record worker.ToolInvocationRecord) (worker.ToolInvocation, string, *time.Time, uint32, bool, error) {
	var out worker.ToolInvocation
	var objective, ordinal, attempt int64
	var approval string
	var approvalExpires *time.Time
	var result []byte
	err := tx.QueryRow(ctx, `SELECT invocation_id,objective_revision,tool_ordinal,tool_name,idempotency_key,canonical_arguments_hash,arguments_ciphertext,arguments_nonce,arguments_key_id,status,COALESCE(result::text,''),approval_state,approval_expires_at,attempt
		FROM tool_invocations WHERE tenant_id=$1 AND conversation_id=$2 AND handoff_id=$3 AND (invocation_id=$4 OR idempotency_key=$5 OR (objective_revision=$6 AND tool_ordinal=$7 AND tool_name=$8 AND canonical_arguments_hash=$9)) FOR UPDATE LIMIT 1`, lease.tenant, lease.conversation, token.HandoffID, record.InvocationID, record.IdempotencyKey, record.ObjectiveRevision, record.ToolOrdinal, record.ToolName, record.ArgumentsHash).
		Scan(&out.Record.InvocationID, &objective, &ordinal, &out.Record.ToolName, &out.Record.IdempotencyKey, &out.Record.ArgumentsHash, &out.Record.Ciphertext, &out.Record.Nonce, &out.Record.KeyID, &out.Status, &result, &approval, &approvalExpires, &attempt)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, "", nil, 0, false, nil
	}
	if err != nil {
		return out, "", nil, 0, false, err
	}
	if objective < 0 || objective > math.MaxUint32 || ordinal < 0 || ordinal > math.MaxUint32 || attempt < 0 || attempt > math.MaxUint32 {
		return out, "", nil, 0, false, errors.New("postgres: tool numeric value out of range")
	}
	out.Record.ObjectiveRevision = uint32(objective)
	out.Record.ToolOrdinal = uint32(ordinal)
	out.Result = append(json.RawMessage(nil), result...)
	return worker.CloneToolInvocation(out), approval, approvalExpires, uint32(attempt), true, nil
}

func sameToolIdentity(a, b worker.ToolInvocationRecord) bool {
	return a.InvocationID == b.InvocationID && a.ObjectiveRevision == b.ObjectiveRevision && a.ToolOrdinal == b.ToolOrdinal && a.ToolName == b.ToolName && a.IdempotencyKey == b.IdempotencyKey && a.ArgumentsHash == b.ArgumentsHash
}
func cloneToolRecord(in worker.ToolInvocationRecord) worker.ToolInvocationRecord {
	out := in
	out.Ciphertext = append([]byte(nil), in.Ciphertext...)
	out.Nonce = append([]byte(nil), in.Nonce...)
	return out
}
