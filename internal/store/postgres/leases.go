package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/worker"
	"github.com/jackc/pgx/v5"
)

var _ worker.Store = (*Repository)(nil)

type leaseRow struct {
	tenant, conversation string
	state                handoff.State
	attempt              uint32
	retryCount           uint32
	objectiveRevision    uint32
	cancelRequested      bool
}

func (r *Repository) LeaseNext(ctx context.Context, request worker.LeaseRequest) (worker.Lease, bool, error) {
	if err := validateContext(ctx); err != nil {
		return worker.Lease{}, false, err
	}
	if err := bounded("worker id", request.WorkerID, maxIdentityBytes, true); err != nil {
		return worker.Lease{}, false, err
	}
	if request.Now.IsZero() || request.LeaseDuration <= 0 {
		return worker.Lease{}, false, fmt.Errorf("%w: invalid lease time", handoff.ErrInvalidRequest)
	}
	now := request.Now.UTC()
	expires := now.Add(request.LeaseDuration)
	if !expires.After(now) {
		return worker.Lease{}, false, fmt.Errorf("%w: lease duration overflows", handoff.ErrInvalidRequest)
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return worker.Lease{}, false, err
	}
	defer rollback(tx)
	if err := r.lockEventAppend(ctx, tx); err != nil {
		return worker.Lease{}, false, err
	}
	var tenant, conversation, id, oldState string
	var oldAttempt, oldVersion int64
	err = tx.QueryRow(ctx, `SELECT h.tenant_id,h.conversation_id,h.id,h.state,h.attempt,h.lease_version
		FROM handoffs h
		WHERE h.cancel_requested_at IS NULL AND h.deadline>$1
		  AND (
		    (h.state IN ('pending','running') AND h.lease_owner IS NULL AND h.ready_at<=$1) OR
		    (h.state IN ('leased','running') AND h.lease_owner IS NOT NULL AND h.lease_expires_at<=$1) OR
		    (h.state='needs_input' AND h.input_revision>h.consumed_input_revision
		      AND (h.lease_owner IS NULL OR h.lease_expires_at<=$1))
		  )
		ORDER BY h.priority DESC,h.ready_at,h.created_at,h.tenant_id,h.conversation_id,h.id
		FOR UPDATE OF h SKIP LOCKED LIMIT 1`, now).Scan(&tenant, &conversation, &id, &oldState, &oldAttempt, &oldVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return worker.Lease{}, false, err
		}
		return worker.Lease{}, false, nil
	}
	if err != nil {
		return worker.Lease{}, false, err
	}
	if oldAttempt < 0 || oldAttempt >= math.MaxInt32 || oldVersion < 0 || oldVersion == math.MaxInt64 {
		return worker.Lease{}, false, errors.New("postgres: lease fencing value out of range")
	}
	attempt, version := uint32(oldAttempt+1), oldVersion+1
	state := handoff.StateRunning
	if oldState == string(handoff.StatePending) || oldState == string(handoff.StateLeased) {
		state = handoff.StateLeased
	}
	command, err := tx.Exec(ctx, `UPDATE handoffs SET state=$4,attempt=$5,lease_version=$6,lease_owner=$7,
		lease_expires_at=$8,consumed_input_revision=CASE WHEN state='needs_input' THEN input_revision ELSE consumed_input_revision END,
		updated_at=$1 WHERE tenant_id=$2 AND conversation_id=$3 AND id=$9`, now, tenant, conversation, state,
		attempt, version, request.WorkerID, expires, id)
	if err != nil {
		return worker.Lease{}, false, mapWriteError(err)
	}
	if command.RowsAffected() != 1 {
		return worker.Lease{}, false, worker.ErrLeaseLost
	}
	if err := r.after("lease_update"); err != nil {
		return worker.Lease{}, false, err
	}
	payload, _ := json.Marshal(struct {
		WorkerID string `json:"worker_id"`
		Version  int64  `json:"lease_version"`
	}{request.WorkerID, version})
	eventID := workerEventID(tenant, identity.ConversationID(conversation), identity.HandoffID(id), "lease", fmt.Sprintf("%d", version))
	if err := r.appendWorkerEvent(ctx, tx, eventID, tenant, conversation, identity.HandoffID(id), "handoff.leased", state, attempt, payload, now, "lease"); err != nil {
		return worker.Lease{}, false, err
	}
	job, err := loadHandoffRow(tx.QueryRow(ctx, `SELECT `+handoffColumns+` FROM handoffs
		WHERE tenant_id=$1 AND conversation_id=$2 AND id=$3`, tenant, conversation, id))
	if err != nil {
		return worker.Lease{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return worker.Lease{}, false, err
	}
	return worker.Lease{Token: worker.LeaseToken{TenantID: tenant, ConversationID: identity.ConversationID(conversation), HandoffID: identity.HandoffID(id), WorkerID: request.WorkerID, Version: version, Attempt: attempt}, Job: handoff.CloneSnapshot(job)}, true, nil
}

func (r *Repository) MarkRunning(ctx context.Context, token worker.LeaseToken, now time.Time) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := validateToken(token); err != nil {
		return err
	}
	if now.IsZero() {
		return fmt.Errorf("%w: timestamp required", handoff.ErrInvalidRequest)
	}
	now = now.UTC()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if err := r.lockEventAppend(ctx, tx); err != nil {
		return err
	}
	row, expires, err := r.lockLease(ctx, tx, token)
	if err != nil || !expires.After(now) {
		if err == nil {
			err = worker.ErrLeaseLost
		}
		return err
	}
	if row.state == handoff.StateRunning {
		return tx.Commit(ctx)
	}
	if row.state != handoff.StateLeased {
		return worker.ErrLeaseLost
	}
	command, err := tx.Exec(ctx, `UPDATE handoffs SET state='running',updated_at=$1
		WHERE tenant_id=$2 AND conversation_id=$3 AND id=$4 AND lease_owner=$5 AND lease_version=$6 AND attempt=$7
		  AND state='leased' AND lease_expires_at>$1 AND cancel_requested_at IS NULL`, now, token.TenantID, token.ConversationID, token.HandoffID, token.WorkerID, token.Version, token.Attempt)
	if err != nil {
		return mapWriteError(err)
	}
	if command.RowsAffected() != 1 {
		return worker.ErrLeaseLost
	}
	if err := r.after("running_update"); err != nil {
		return err
	}
	payload := json.RawMessage(`{}`)
	if err := r.appendWorkerEvent(ctx, tx, workerEventID(token.TenantID, token.ConversationID, token.HandoffID, "running", fmt.Sprintf("%d", token.Version)), row.tenant, row.conversation, token.HandoffID, "handoff.running", handoff.StateRunning, token.Attempt, payload, now, "running"); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *Repository) Heartbeat(ctx context.Context, token worker.LeaseToken, newExpiry time.Time) (worker.Control, error) {
	if err := validateContext(ctx); err != nil {
		return worker.Control{}, err
	}
	if err := validateToken(token); err != nil {
		return worker.Control{}, err
	}
	if newExpiry.IsZero() {
		return worker.Control{}, fmt.Errorf("%w: lease expiry required", handoff.ErrInvalidRequest)
	}
	now := r.clock.Now().UTC()
	newExpiry = newExpiry.UTC()
	if !newExpiry.After(now) {
		return worker.Control{}, fmt.Errorf("%w: lease expiry must be future", handoff.ErrInvalidRequest)
	}
	var cancel bool
	var objective string
	var objectiveRev, inputRev, consumed int64
	var input []byte
	err := r.pool.QueryRow(ctx, `UPDATE handoffs SET lease_expires_at=GREATEST(lease_expires_at,$1),updated_at=$2
		WHERE tenant_id=$3 AND conversation_id=$4 AND id=$5 AND lease_owner=$6 AND lease_version=$7 AND attempt=$8
		  AND state IN ('leased','running') AND lease_expires_at>$2
		RETURNING cancel_requested_at IS NOT NULL,objective,objective_revision,input_revision,consumed_input_revision,COALESCE(input::text,'')`,
		newExpiry, now, token.TenantID, token.ConversationID, token.HandoffID, token.WorkerID, token.Version, token.Attempt).Scan(&cancel, &objective, &objectiveRev, &inputRev, &consumed, &input)
	if errors.Is(err, pgx.ErrNoRows) {
		return worker.Control{}, worker.ErrLeaseLost
	}
	if err != nil {
		return worker.Control{}, err
	}
	if objectiveRev < 0 || objectiveRev > math.MaxUint32 || inputRev < 0 || inputRev > math.MaxUint32 || consumed < 0 || consumed > math.MaxUint32 {
		return worker.Control{}, errors.New("postgres: control revision out of range")
	}
	return worker.Control{CancelRequested: cancel, ObjectiveRevision: uint32(objectiveRev), Objective: objective, InputRevision: uint32(inputRev), ConsumedInput: uint32(consumed), Input: append(json.RawMessage(nil), input...)}, nil
}

func (r *Repository) ScheduleRetry(ctx context.Context, token worker.LeaseToken, record worker.RetryRecord) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := bounded("mutation id", record.MutationID, maxIdentityBytes, true); err != nil {
		return err
	}
	if err := bounded("retry code", record.Code, maxIdentityBytes, true); err != nil {
		return err
	}
	if record.RetryCount > math.MaxInt32 {
		return fmt.Errorf("%w: retry count out of range", handoff.ErrInvalidRequest)
	}
	now := r.clock.Now().UTC()
	record.ReadyAt = record.ReadyAt.UTC()
	if !record.ReadyAt.After(now) {
		return fmt.Errorf("%w: retry readiness must be future", handoff.ErrInvalidRequest)
	}
	payload, _ := json.Marshal(record)
	return r.mutateLease(ctx, token, record.ExpectedObjectiveRevision, "retry", record.MutationID, payload, func(ctx context.Context, tx pgx.Tx, row leaseRow) (handoff.State, error) {
		if row.retryCount == math.MaxUint32 || record.RetryCount != row.retryCount+1 {
			return "", fmt.Errorf("%w: retry count must increment exactly", handoff.ErrInvalidRequest)
		}
		command, err := tx.Exec(ctx, `UPDATE handoffs SET state='running',lease_owner=NULL,lease_expires_at=NULL,ready_at=$1,retry_count=$2,updated_at=$3
			WHERE tenant_id=$4 AND conversation_id=$5 AND id=$6 AND lease_owner=$7 AND lease_version=$8 AND attempt=$9
			  AND state='running' AND lease_expires_at>$3 AND cancel_requested_at IS NULL AND objective_revision=$10`, record.ReadyAt, record.RetryCount, now, token.TenantID, token.ConversationID, token.HandoffID, token.WorkerID, token.Version, token.Attempt, record.ExpectedObjectiveRevision)
		if err != nil {
			return "", mapWriteError(err)
		}
		if command.RowsAffected() != 1 {
			return "", worker.ErrLeaseLost
		}
		return handoff.StateRunning, nil
	})
}

func (r *Repository) AppendProgress(ctx context.Context, token worker.LeaseToken, record worker.ProgressRecord) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := bounded("mutation id", record.MutationID, maxIdentityBytes, true); err != nil {
		return err
	}
	if err := bounded("progress summary", record.Summary, 16*1024, true); err != nil {
		return err
	}
	if record.Sequence > math.MaxInt64 {
		return fmt.Errorf("%w: progress sequence out of range", handoff.ErrInvalidRequest)
	}
	payload, _ := json.Marshal(record)
	now := r.clock.Now().UTC()
	return r.mutateLease(ctx, token, record.ExpectedObjectiveRevision, "progress", record.MutationID, payload, func(ctx context.Context, tx pgx.Tx, row leaseRow) (handoff.State, error) {
		command, err := tx.Exec(ctx, `UPDATE handoffs SET updated_at=$1
			WHERE tenant_id=$2 AND conversation_id=$3 AND id=$4 AND lease_owner=$5 AND lease_version=$6 AND attempt=$7
			  AND state='running' AND lease_expires_at>$1 AND cancel_requested_at IS NULL AND objective_revision=$8`, now, token.TenantID, token.ConversationID, token.HandoffID, token.WorkerID, token.Version, token.Attempt, record.ExpectedObjectiveRevision)
		if err != nil {
			return "", mapWriteError(err)
		}
		if command.RowsAffected() != 1 {
			return "", worker.ErrLeaseLost
		}
		return row.state, nil
	})
}

func (r *Repository) MarkNeedsInput(ctx context.Context, token worker.LeaseToken, record worker.NeedsInputRecord) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := bounded("mutation id", record.MutationID, maxIdentityBytes, true); err != nil {
		return err
	}
	if err := bounded("input prompt", record.Prompt, 16*1024, true); err != nil {
		return err
	}
	schema, err := canonicalWorkerJSON(record.Schema)
	if err != nil {
		return err
	}
	record.Schema = schema
	payload, _ := json.Marshal(record)
	now := r.clock.Now().UTC()
	return r.mutateLease(ctx, token, record.ExpectedObjectiveRevision, "needs-input", record.MutationID, payload, func(ctx context.Context, tx pgx.Tx, row leaseRow) (handoff.State, error) {
		command, err := tx.Exec(ctx, `UPDATE handoffs SET state='needs_input',lease_owner=NULL,lease_expires_at=NULL,updated_at=$1
			WHERE tenant_id=$2 AND conversation_id=$3 AND id=$4 AND lease_owner=$5 AND lease_version=$6 AND attempt=$7
			  AND state='running' AND lease_expires_at>$1 AND cancel_requested_at IS NULL AND objective_revision=$8`, now, token.TenantID, token.ConversationID, token.HandoffID, token.WorkerID, token.Version, token.Attempt, record.ExpectedObjectiveRevision)
		if err != nil {
			return "", err
		}
		if command.RowsAffected() != 1 {
			return "", worker.ErrLeaseLost
		}
		return handoff.StateNeedsInput, nil
	})
}

func (r *Repository) Complete(ctx context.Context, token worker.LeaseToken, record worker.CompletionRecord) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	result, err := handoff.ValidateResult(record.Result)
	if err != nil {
		return err
	}
	record.Result = result
	payload, _ := json.Marshal(record)
	now := r.clock.Now().UTC()
	return r.mutateLease(ctx, token, record.ExpectedObjectiveRevision, "complete", result.MutationID, payload, func(ctx context.Context, tx pgx.Tx, row leaseRow) (handoff.State, error) {
		command, err := tx.Exec(ctx, `UPDATE handoffs SET state='succeeded',lease_owner=NULL,lease_expires_at=NULL,terminal_mutation_id=$1,result_summary=$2,structured_result=$3::json,safety_decision=$4::json,usage=$5::json,terminal_at=$6,updated_at=$6
			WHERE tenant_id=$7 AND conversation_id=$8 AND id=$9 AND lease_owner=$10 AND lease_version=$11 AND attempt=$12
			  AND state='running' AND lease_expires_at>$6 AND cancel_requested_at IS NULL AND objective_revision=$13`, result.MutationID, result.Summary, result.Structured, result.SafetyDecision, result.Usage, now, token.TenantID, token.ConversationID, token.HandoffID, token.WorkerID, token.Version, token.Attempt, record.ExpectedObjectiveRevision)
		if err != nil {
			return "", mapWriteError(err)
		}
		if command.RowsAffected() != 1 {
			return "", worker.ErrLeaseLost
		}
		return handoff.StateSucceeded, nil
	})
}

func (r *Repository) Fail(ctx context.Context, token worker.LeaseToken, record worker.FailureRecord) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	failure, err := handoff.ValidateFailure(record.Failure)
	if err != nil {
		return err
	}
	record.Failure = failure
	payload, _ := json.Marshal(record)
	now := r.clock.Now().UTC()
	return r.mutateLease(ctx, token, record.ExpectedObjectiveRevision, "fail", failure.MutationID, payload, func(ctx context.Context, tx pgx.Tx, row leaseRow) (handoff.State, error) {
		command, err := tx.Exec(ctx, `UPDATE handoffs SET state='failed',lease_owner=NULL,lease_expires_at=NULL,terminal_mutation_id=$1,failure_code=$2,failure_retryable=$3,failure_message=$4,terminal_at=$5,updated_at=$5
			WHERE tenant_id=$6 AND conversation_id=$7 AND id=$8 AND lease_owner=$9 AND lease_version=$10 AND attempt=$11
			  AND state='running' AND lease_expires_at>$5 AND cancel_requested_at IS NULL AND objective_revision=$12`, failure.MutationID, failure.Code, failure.Retryable, failure.Message, now, token.TenantID, token.ConversationID, token.HandoffID, token.WorkerID, token.Version, token.Attempt, record.ExpectedObjectiveRevision)
		if err != nil {
			return "", mapWriteError(err)
		}
		if command.RowsAffected() != 1 {
			return "", worker.ErrLeaseLost
		}
		return handoff.StateFailed, nil
	})
}

func (r *Repository) MarkCancelled(ctx context.Context, token worker.LeaseToken, record worker.CancelledRecord) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := bounded("mutation id", record.MutationID, maxIdentityBytes, true); err != nil {
		return err
	}
	if err := bounded("cancel reason", record.Reason, 2048, false); err != nil {
		return err
	}
	payload, _ := json.Marshal(record)
	now := r.clock.Now().UTC()
	return r.mutateLease(ctx, token, record.ExpectedObjectiveRevision, "cancel", record.MutationID, payload, func(ctx context.Context, tx pgx.Tx, row leaseRow) (handoff.State, error) {
		command, err := tx.Exec(ctx, `UPDATE handoffs SET state='cancelled',lease_owner=NULL,lease_expires_at=NULL,terminal_mutation_id=$1,terminal_at=$2,updated_at=$2
			WHERE tenant_id=$3 AND conversation_id=$4 AND id=$5 AND lease_owner=$6 AND lease_version=$7 AND attempt=$8
			  AND state IN ('leased','running') AND lease_expires_at>$2 AND objective_revision=$9`, record.MutationID, now, token.TenantID, token.ConversationID, token.HandoffID, token.WorkerID, token.Version, token.Attempt, record.ExpectedObjectiveRevision)
		if err != nil {
			return "", mapWriteError(err)
		}
		if command.RowsAffected() != 1 {
			return "", worker.ErrLeaseLost
		}
		return handoff.StateCancelled, nil
	})
}

func (r *Repository) mutateLease(ctx context.Context, token worker.LeaseToken, expectedObjectiveRevision uint32, action, mutationID string, payload []byte, update func(context.Context, pgx.Tx, leaseRow) (handoff.State, error)) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := validateToken(token); err != nil {
		return err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if err := r.lockEventAppend(ctx, tx); err != nil {
		return err
	}
	eventID := workerEventID(token.TenantID, token.ConversationID, token.HandoffID, action, mutationID)
	var existing []byte
	var existingTenant, existingConversation, existingHandoff string
	err = tx.QueryRow(ctx, `SELECT payload::text,tenant_id,conversation_id,handoff_id FROM handoff_events WHERE event_id=$1`, eventID).Scan(&existing, &existingTenant, &existingConversation, &existingHandoff)
	if err == nil {
		if existingTenant != token.TenantID || existingConversation != string(token.ConversationID) || existingHandoff != string(token.HandoffID) || !bytes.Equal(existing, payload) {
			return handoff.ErrIdempotencyConflict
		}
		return tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	row, expires, err := r.lockLease(ctx, tx, token)
	if err != nil {
		return err
	}
	if !expires.After(r.clock.Now().UTC()) {
		return worker.ErrLeaseLost
	}
	if row.objectiveRevision != expectedObjectiveRevision {
		return worker.ErrLeaseLost
	}
	if row.cancelRequested && action != "cancel" {
		return worker.ErrLeaseLost
	}
	if action != "cancel" && row.state != handoff.StateRunning {
		return handoff.ErrInvalidTransition
	}
	if action == "cancel" && row.state != handoff.StateLeased && row.state != handoff.StateRunning {
		return handoff.ErrInvalidTransition
	}
	state, err := update(ctx, tx, row)
	if err != nil {
		return err
	}
	if err := r.after("worker_" + action + "_update"); err != nil {
		return err
	}
	if err := r.appendWorkerEvent(ctx, tx, eventID, row.tenant, row.conversation, token.HandoffID, "handoff."+action, state, token.Attempt, payload, r.clock.Now().UTC(), "worker_"+action); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return r.after("worker_" + action + "_commit")
}

func (r *Repository) SweepExpired(ctx context.Context, now time.Time, limit int) (int, error) {
	if err := validateContext(ctx); err != nil {
		return 0, err
	}
	if now.IsZero() || limit <= 0 || limit > 1000 {
		return 0, fmt.Errorf("%w: invalid sweep bounds", handoff.ErrInvalidRequest)
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
	rows, err := tx.Query(ctx, `SELECT tenant_id,conversation_id,id,state,attempt,cancel_requested_at IS NOT NULL,deadline FROM handoffs WHERE state NOT IN ('succeeded','failed','cancelled','expired') AND (cancel_requested_at IS NOT NULL OR deadline<=$1 OR (lease_owner IS NOT NULL AND lease_expires_at<=$1)) ORDER BY COALESCE(lease_expires_at,deadline),tenant_id,conversation_id,id FOR UPDATE SKIP LOCKED LIMIT $2`, now, limit)
	if err != nil {
		return 0, err
	}
	type candidate struct {
		tenant, conversation, id, state string
		attempt                         int64
		cancel                          bool
		deadline                        time.Time
	}
	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.tenant, &c.conversation, &c.id, &c.state, &c.attempt, &c.cancel, &c.deadline); err != nil {
			rows.Close()
			return 0, err
		}
		candidates = append(candidates, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(candidates) > 0 {
		if err := r.after("sweep_locked"); err != nil {
			return 0, err
		}
	}
	for _, c := range candidates {
		if c.attempt < 0 || c.attempt > math.MaxUint32 {
			return 0, errors.New("postgres: sweep attempt out of range")
		}
		state := handoff.State(c.state)
		kind := "handoff.lease_expired"
		mutation := fmt.Sprintf("lease-%d", c.attempt)
		if c.cancel {
			state = handoff.StateCancelled
			kind = "handoff.cancelled"
			mutation = "cancel"
		} else if !c.deadline.After(now) {
			state = handoff.StateExpired
			kind = "handoff.expired"
			mutation = "deadline"
		} else if state == handoff.StateLeased {
			state = handoff.StatePending
		}
		terminal := state == handoff.StateCancelled || state == handoff.StateExpired
		if terminal {
			_, err = tx.Exec(ctx, `UPDATE handoffs SET state=$1,lease_owner=NULL,lease_expires_at=NULL,terminal_mutation_id=$2,terminal_at=$3,updated_at=$3 WHERE tenant_id=$4 AND conversation_id=$5 AND id=$6`, state, workerEventID(c.tenant, identity.ConversationID(c.conversation), identity.HandoffID(c.id), "sweep", mutation), now, c.tenant, c.conversation, c.id)
		} else {
			_, err = tx.Exec(ctx, `UPDATE handoffs SET state=$1,lease_owner=NULL,lease_expires_at=NULL,updated_at=$2 WHERE tenant_id=$3 AND conversation_id=$4 AND id=$5`, state, now, c.tenant, c.conversation, c.id)
		}
		if err != nil {
			return 0, mapWriteError(err)
		}
		if err := r.after("sweep_update"); err != nil {
			return 0, err
		}
		payload := json.RawMessage(`{}`)
		if err := r.appendWorkerEvent(ctx, tx, workerEventID(c.tenant, identity.ConversationID(c.conversation), identity.HandoffID(c.id), "sweep", mutation), c.tenant, c.conversation, identity.HandoffID(c.id), kind, state, uint32(c.attempt), payload, now, "sweep"); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(candidates), nil
}

func validateToken(token worker.LeaseToken) error {
	if err := bounded("tenant id", token.TenantID, maxIdentityBytes, true); err != nil {
		return err
	}
	if err := bounded("conversation id", string(token.ConversationID), maxIdentityBytes, true); err != nil {
		return err
	}
	if err := bounded("handoff id", string(token.HandoffID), maxIdentityBytes, true); err != nil {
		return err
	}
	if err := bounded("worker id", token.WorkerID, maxIdentityBytes, true); err != nil {
		return err
	}
	if token.Version <= 0 || token.Attempt == 0 || token.Attempt > math.MaxInt32 {
		return fmt.Errorf("%w: invalid lease token", handoff.ErrInvalidRequest)
	}
	return nil
}

func (r *Repository) lockLease(ctx context.Context, tx pgx.Tx, token worker.LeaseToken) (leaseRow, time.Time, error) {
	var row leaseRow
	var attempt, retryCount, objectiveRevision int64
	var expires time.Time
	err := tx.QueryRow(ctx, `SELECT tenant_id,conversation_id,state,attempt,retry_count,objective_revision,lease_expires_at,cancel_requested_at IS NOT NULL FROM handoffs
		WHERE tenant_id=$1 AND conversation_id=$2 AND id=$3 AND lease_owner=$4 AND lease_version=$5 AND attempt=$6 FOR UPDATE`, token.TenantID, token.ConversationID, token.HandoffID, token.WorkerID, token.Version, token.Attempt).Scan(&row.tenant, &row.conversation, &row.state, &attempt, &retryCount, &objectiveRevision, &expires, &row.cancelRequested)
	if errors.Is(err, pgx.ErrNoRows) {
		return row, time.Time{}, worker.ErrLeaseLost
	}
	if err != nil {
		return row, time.Time{}, err
	}
	if attempt < 0 || attempt > math.MaxUint32 || retryCount < 0 || retryCount > math.MaxUint32 || objectiveRevision < 0 || objectiveRevision > math.MaxUint32 {
		return row, time.Time{}, errors.New("postgres: lease attempt out of range")
	}
	row.attempt = uint32(attempt)
	row.retryCount = uint32(retryCount)
	row.objectiveRevision = uint32(objectiveRevision)
	return row, expires.UTC(), nil
}

func workerEventID(tenant string, conversation identity.ConversationID, id identity.HandoffID, action, mutation string) string {
	sum := sha256.Sum256([]byte(tenant + "\x00" + string(conversation) + "\x00" + string(id) + "\x00" + action + "\x00" + mutation))
	return "worker:" + hex.EncodeToString(sum[:])
}

func (r *Repository) appendWorkerEvent(ctx context.Context, tx pgx.Tx, eventID, tenant, conversation string, id identity.HandoffID, kind string, state handoff.State, attempt uint32, payload []byte, now time.Time, step string) error {
	var seq int64
	err := tx.QueryRow(ctx, `INSERT INTO handoff_events(event_id,tenant_id,conversation_id,handoff_id,kind,state,attempt,payload,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8::json,$9) RETURNING seq`, eventID, tenant, conversation, id, kind, state, attempt, payload, now).Scan(&seq)
	if err != nil {
		return mapWriteError(err)
	}
	if err := r.after(step + "_event"); err != nil {
		return err
	}
	wakeup, _ := json.Marshal(struct {
		TenantID       string `json:"tenant_id"`
		ConversationID string `json:"conversation_id"`
		HighestSeq     int64  `json:"highest_seq"`
	}{tenant, conversation, seq})
	_, err = tx.Exec(ctx, `INSERT INTO outbox(topic,aggregate_id,event_seq,payload,created_at) VALUES('handoff.event',$1,$2,$3::json,$4)`, id, seq, wakeup, now)
	if err != nil {
		return mapWriteError(err)
	}
	return r.after(step + "_wakeup")
}

func canonicalWorkerJSON(raw json.RawMessage) (json.RawMessage, error) {
	result, err := handoff.ValidateResult(handoff.Result{MutationID: "canonical", Summary: "canonical", Structured: raw, SafetyDecision: json.RawMessage(`{}`), Usage: json.RawMessage(`{}`)})
	if err != nil {
		return nil, err
	}
	return append(json.RawMessage(nil), result.Structured...), nil
}
