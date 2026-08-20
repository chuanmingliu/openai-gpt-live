package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/jackc/pgx/v5"
)

const handoffColumns = `
	id, tenant_id, conversation_id, user_id, session_id, source_turn_id, source_turn_revision,
	user_request, objective, context_ref, context_hash, deadline, token_budget, priority,
	tool_allowlist::text, tool_policy::text, approval_policy::text, model_version, prompt_version, tool_registry_version,
	config_version, profile_id, profile_version, profile_snapshot_hash, profile_engine_version,
	worker_recipe_id, worker_recipe_version, worker_recipe_hash, adapter_registry_version,
	profile_knowledge::text, profile_evidence::text, presentation_hint, idempotency_key, request_hash, state, objective_revision,
	input_revision, consumed_input_revision, input::text, attempt, retry_count, terminal_mutation_id,
	result_summary, structured_result::text, safety_decision::text, usage::text, failure_code, failure_retryable,
	failure_message, created_at, updated_at`

const handoffCreateLockNamespace = "openai-gpt-live/handoff-create/v1\x00"

func handoffCreateLockID(tenantID, idempotencyKey string) int64 {
	digest := sha256.Sum256([]byte(handoffCreateLockNamespace + tenantID + "\x00" + idempotencyKey))
	return int64(binary.BigEndian.Uint64(digest[:8]))
}

func (r *Repository) lockHandoffCreate(ctx context.Context, tx pgx.Tx, tenantID, idempotencyKey string) error {
	if err := r.after("before_create_idempotency_lock"); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, handoffCreateLockID(tenantID, idempotencyKey)); err != nil {
		return err
	}
	return r.after("create_idempotency_lock")
}

// admitNewHandoff serializes the irreversible create boundary with runtime
// feature mutations. Set locks this same singleton row FOR UPDATE. A create
// that takes this shared lock first is ordered before a later disable; a create
// that reaches it after disable commits must observe disabled and reject.
func (r *Repository) admitNewHandoff(ctx context.Context, tx pgx.Tx) error {
	if err := r.after("before_handoff_feature_admission"); err != nil {
		return err
	}
	if r.handoffAdmissionHook != nil {
		r.handoffAdmissionHook()
	}
	var authorityVersion int64
	if err := tx.QueryRow(ctx, `SELECT version FROM runtime_feature_authority WHERE singleton=true FOR SHARE`).Scan(&authorityVersion); err != nil || authorityVersion <= 0 {
		return errors.New("runtime feature authority is corrupt")
	}
	var enabled bool
	var flagVersion int64
	if err := tx.QueryRow(ctx, `SELECT enabled, version FROM runtime_feature_flags WHERE name='handoffs_enabled'`).Scan(&enabled, &flagVersion); err != nil || flagVersion <= 0 || flagVersion > authorityVersion {
		return errors.New("runtime feature authority is corrupt")
	}
	if !enabled {
		return handoff.ErrHandoffsDisabled
	}
	return r.after("handoff_feature_admitted")
}

func (r *Repository) GetHandoffByIdempotencyKey(ctx context.Context, scope handoff.Scope, key string) (handoff.Snapshot, error) {
	if err := validateContext(ctx); err != nil {
		return handoff.Snapshot{}, err
	}
	if err := validateScope(scope); err != nil {
		return handoff.Snapshot{}, err
	}
	if err := bounded("idempotency key", key, maxIdentityBytes, true); err != nil {
		return handoff.Snapshot{}, err
	}
	snapshot, err := loadHandoffRow(r.pool.QueryRow(ctx, `SELECT `+handoffColumns+`
		FROM handoffs
		WHERE tenant_id=$1 AND conversation_id=$2 AND user_id=$3 AND idempotency_key=$4`,
		scope.TenantID, scope.ConversationID, scope.UserID, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return handoff.Snapshot{}, handoff.ErrNotFound
	}
	if err != nil {
		return handoff.Snapshot{}, err
	}
	return snapshot, nil
}

func (r *Repository) CreateHandoff(ctx context.Context, record handoff.CreateRecord) (handoff.Snapshot, bool, error) {
	if err := validateContext(ctx); err != nil {
		return handoff.Snapshot{}, false, err
	}
	static, err := handoff.NormalizeCreateRequest(record.Request)
	if err != nil {
		return handoff.Snapshot{}, false, err
	}
	if err := bounded("request hash", record.RequestHash, 512, true); err != nil {
		return handoff.Snapshot{}, false, err
	}
	if record.CreatedAt.IsZero() {
		return handoff.Snapshot{}, false, fmt.Errorf("%w: created timestamp is required", handoff.ErrInvalidRequest)
	}
	computedHash, err := handoff.CanonicalRequestHash(static)
	if err != nil {
		return handoff.Snapshot{}, false, err
	}
	if computedHash != record.RequestHash {
		return handoff.Snapshot{}, false, handoff.ErrIdempotencyConflict
	}
	record.Request = static
	record.CreatedAt = record.CreatedAt.UTC()

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return handoff.Snapshot{}, false, err
	}
	defer rollback(tx)
	// Every creator takes the per-(tenant,idempotency-key) lock before any
	// replay/deadline decision. New creates then take the global event-append
	// lock. No path acquires these locks in the opposite order.
	if err := r.lockHandoffCreate(ctx, tx, record.Request.Scope.TenantID, record.Request.IdempotencyKey); err != nil {
		return handoff.Snapshot{}, false, err
	}
	existing, loadErr := loadHandoffRow(tx.QueryRow(ctx, `SELECT `+handoffColumns+`
		FROM handoffs WHERE tenant_id=$1 AND idempotency_key=$2 FOR UPDATE`,
		record.Request.Scope.TenantID, record.Request.IdempotencyKey))
	if loadErr == nil {
		if existing.RequestHash != record.RequestHash || existing.Request.Scope != record.Request.Scope {
			return handoff.Snapshot{}, false, handoff.ErrIdempotencyConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return handoff.Snapshot{}, false, err
		}
		return existing, false, nil
	}
	if !errors.Is(loadErr, pgx.ErrNoRows) {
		return handoff.Snapshot{}, false, loadErr
	}
	if err := r.admitNewHandoff(ctx, tx); err != nil {
		return handoff.Snapshot{}, false, err
	}

	normalized, err := handoff.ValidateCreateRequest(record.Request, record.CreatedAt)
	if err != nil {
		return handoff.Snapshot{}, false, err
	}
	if err := bounded("handoff id", string(record.HandoffID), maxIdentityBytes, true); err != nil {
		return handoff.Snapshot{}, false, err
	}
	if err := bounded("event id", record.EventID, maxIdentityBytes, true); err != nil {
		return handoff.Snapshot{}, false, err
	}
	record.Request = normalized
	allowlist, err := json.Marshal(record.Request.ToolAllowlist)
	if err != nil {
		return handoff.Snapshot{}, false, err
	}
	knowledge := []byte(`[]`)
	if len(record.Request.Profile.Knowledge) != 0 {
		knowledge, err = json.Marshal(record.Request.Profile.Knowledge)
		if err != nil {
			return handoff.Snapshot{}, false, err
		}
	}
	if err := r.lockEventAppend(ctx, tx); err != nil {
		return handoff.Snapshot{}, false, err
	}
	var insertedID string
	err = tx.QueryRow(ctx, `
		INSERT INTO handoffs (
			tenant_id, conversation_id, id, user_id, session_id, source_turn_id, source_turn_revision,
			user_request, objective, context_ref, context_hash, deadline, token_budget, priority,
			tool_allowlist, tool_policy, approval_policy, model_version, prompt_version, tool_registry_version, config_version,
			profile_id, profile_version, profile_snapshot_hash, profile_engine_version,
			worker_recipe_id, worker_recipe_version, worker_recipe_hash, adapter_registry_version, profile_knowledge, profile_evidence,
			presentation_hint, idempotency_key, request_hash, ready_at, created_at, updated_at
		)
		SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::json,$16::jsonb,$17::json,$18,$19,$20,$21,
		       $22,$23,$24,$25,$26,$27,$28,$29,$30::json,$31::json,$32,$33,$34,$35,$35,$35
		FROM conversations c WHERE c.tenant_id=$1 AND c.id=$2 AND c.user_id=$4 AND c.status <> 'deleted'
		  AND EXISTS (SELECT 1 FROM conversation_items i
		    WHERE i.tenant_id=c.tenant_id AND i.conversation_id=c.id AND i.session_id=$5
		      AND i.turn_id=$6 AND i.turn_revision=$7)
		ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
		RETURNING id`,
		record.Request.Scope.TenantID, record.Request.Scope.ConversationID, record.HandoffID, record.Request.Scope.UserID,
		record.Request.SessionID, record.Request.SourceTurn.ID, record.Request.SourceTurn.Revision,
		record.Request.UserRequest, record.Request.Objective, record.Request.ContextRef, record.Request.ContextHash,
		record.Request.Deadline, record.Request.TokenBudget, record.Request.Priority, allowlist, record.Request.ToolPolicy, record.Request.ApprovalPolicy,
		record.Request.Versions.Model, record.Request.Versions.Prompt, record.Request.Versions.ToolRegistry,
		record.Request.Versions.Config, record.Request.Profile.ProfileID, record.Request.Profile.ProfileVersion,
		record.Request.Profile.SnapshotHash, record.Request.Profile.EngineVersion, record.Request.Profile.RecipeID,
		record.Request.Profile.RecipeVersion, record.Request.Profile.RecipeHash, record.Request.Profile.RegistryVersion,
		knowledge, record.Request.Profile.Evidence, record.Request.PresentationHint, record.Request.IdempotencyKey,
		record.RequestHash, record.CreatedAt).Scan(&insertedID)
	if errors.Is(err, pgx.ErrNoRows) {
		snapshot, loadErr := loadHandoffRow(tx.QueryRow(ctx, `SELECT `+handoffColumns+`
			FROM handoffs WHERE tenant_id=$1 AND idempotency_key=$2 FOR UPDATE`,
			record.Request.Scope.TenantID, record.Request.IdempotencyKey))
		if errors.Is(loadErr, pgx.ErrNoRows) {
			var owner bool
			ownerErr := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM conversations
				WHERE tenant_id=$1 AND id=$2 AND user_id=$3 AND status <> 'deleted')`,
				record.Request.Scope.TenantID, record.Request.Scope.ConversationID, record.Request.Scope.UserID).Scan(&owner)
			if ownerErr != nil {
				return handoff.Snapshot{}, false, ownerErr
			}
			if owner {
				return handoff.Snapshot{}, false, handoff.ErrUncommittedTurn
			}
			return handoff.Snapshot{}, false, handoff.ErrNotFound
		}
		if loadErr != nil {
			return handoff.Snapshot{}, false, loadErr
		}
		if snapshot.RequestHash != record.RequestHash {
			return handoff.Snapshot{}, false, handoff.ErrIdempotencyConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return handoff.Snapshot{}, false, err
		}
		return snapshot, false, nil
	}
	if err != nil {
		return handoff.Snapshot{}, false, mapWriteError(err)
	}
	if err := r.after("insert_handoff"); err != nil {
		return handoff.Snapshot{}, false, err
	}

	createdPayload := json.RawMessage(`{}`)
	var eventSeq int64
	err = tx.QueryRow(ctx, `INSERT INTO handoff_events
		(event_id,tenant_id,conversation_id,handoff_id,kind,state,attempt,payload,created_at)
		VALUES ($1,$2,$3,$4,'handoff.created','pending',0,$5::json,$6) RETURNING seq`,
		record.EventID, record.Request.Scope.TenantID, record.Request.Scope.ConversationID,
		record.HandoffID, createdPayload, record.CreatedAt).Scan(&eventSeq)
	if err != nil {
		return handoff.Snapshot{}, false, mapWriteError(err)
	}
	if err := r.after("insert_event"); err != nil {
		return handoff.Snapshot{}, false, err
	}
	dispatch, _ := json.Marshal(struct {
		TenantID       string `json:"tenant_id"`
		ConversationID string `json:"conversation_id"`
		HandoffID      string `json:"handoff_id"`
		EventSeq       int64  `json:"event_seq"`
	}{record.Request.Scope.TenantID, string(record.Request.Scope.ConversationID), string(record.HandoffID), eventSeq})
	_, err = tx.Exec(ctx, `INSERT INTO outbox (topic,aggregate_id,event_seq,payload,created_at)
		VALUES ('handoff.dispatch',$1,$2,$3::json,$4)`, record.HandoffID, eventSeq, dispatch, record.CreatedAt)
	if err != nil {
		return handoff.Snapshot{}, false, mapWriteError(err)
	}
	if err := r.after("insert_dispatch"); err != nil {
		return handoff.Snapshot{}, false, err
	}
	wakeup, _ := json.Marshal(struct {
		TenantID       string `json:"tenant_id"`
		ConversationID string `json:"conversation_id"`
		HighestSeq     int64  `json:"highest_seq"`
	}{record.Request.Scope.TenantID, string(record.Request.Scope.ConversationID), eventSeq})
	_, err = tx.Exec(ctx, `INSERT INTO outbox (topic,aggregate_id,event_seq,payload,created_at)
		VALUES ('handoff.event',$1,$2,$3::json,$4)`, record.HandoffID, eventSeq, wakeup, record.CreatedAt)
	if err != nil {
		return handoff.Snapshot{}, false, mapWriteError(err)
	}
	if err := r.after("insert_wakeup"); err != nil {
		return handoff.Snapshot{}, false, err
	}

	snapshot, err := loadHandoffRow(tx.QueryRow(ctx, `SELECT `+handoffColumns+`
		FROM handoffs WHERE tenant_id=$1 AND conversation_id=$2 AND id=$3 AND user_id=$4`,
		record.Request.Scope.TenantID, record.Request.Scope.ConversationID, insertedID, record.Request.Scope.UserID))
	if err != nil {
		return handoff.Snapshot{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return handoff.Snapshot{}, false, err
	}
	if err := r.after("commit"); err != nil {
		return handoff.Snapshot{}, false, err
	}
	return snapshot, true, nil
}

func (r *Repository) GetHandoff(ctx context.Context, scope handoff.Scope, id identity.HandoffID) (handoff.Snapshot, error) {
	if err := validateContext(ctx); err != nil {
		return handoff.Snapshot{}, err
	}
	if err := validateScope(scope); err != nil {
		return handoff.Snapshot{}, err
	}
	if err := bounded("handoff id", string(id), maxIdentityBytes, true); err != nil {
		return handoff.Snapshot{}, err
	}
	snapshot, err := loadHandoffRow(r.pool.QueryRow(ctx, `SELECT `+handoffColumns+`
		FROM handoffs WHERE tenant_id=$1 AND conversation_id=$2 AND id=$3 AND user_id=$4`,
		scope.TenantID, scope.ConversationID, id, scope.UserID))
	if errors.Is(err, pgx.ErrNoRows) {
		return handoff.Snapshot{}, handoff.ErrNotFound
	}
	return snapshot, err
}

func (r *Repository) ListActive(ctx context.Context, scope handoff.Scope) ([]handoff.Snapshot, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	if err := validateScope(scope); err != nil {
		return nil, err
	}
	rows, err := r.pool.Query(ctx, `SELECT `+handoffColumns+` FROM handoffs
		WHERE tenant_id=$1 AND conversation_id=$2 AND user_id=$3
		  AND state IN ('pending','leased','running','needs_input') ORDER BY created_at,id`,
		scope.TenantID, scope.ConversationID, scope.UserID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]handoff.Snapshot, 0)
	for rows.Next() {
		snapshot, err := loadHandoffRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, snapshot)
	}
	return out, rows.Err()
}

func (r *Repository) SteerHandoff(ctx context.Context, scope handoff.Scope, id identity.HandoffID, request handoff.SteerRequest) (handoff.Snapshot, error) {
	if err := bounded("objective", request.Objective, 8192, true); err != nil {
		return handoff.Snapshot{}, err
	}
	if request.ExpectedRevision == 0 {
		return handoff.Snapshot{}, fmt.Errorf("%w: expected revision must be positive", handoff.ErrInvalidRequest)
	}
	if err := validateMutationMetadata(request.Actor, request.IdempotencyKey); err != nil {
		return handoff.Snapshot{}, err
	}
	payload, err := json.Marshal(struct {
		Actor            string `json:"actor"`
		ExpectedRevision uint32 `json:"expected_revision"`
		Objective        string `json:"objective"`
	}{request.Actor, request.ExpectedRevision, request.Objective})
	if err != nil {
		return handoff.Snapshot{}, err
	}
	return r.mutateHandoff(ctx, scope, id, "steer", request.IdempotencyKey, "handoff.steered", payload,
		func(ctx context.Context, tx pgx.Tx, current mutationCurrent) error {
			if current.CancelRequested {
				return handoff.ErrSteeringBlocked
			}
			if err := r.guardSteeringToolInvocations(ctx, tx, scope, id, current.ToolRegistryVersion); err != nil {
				return err
			}
			if current.ObjectiveRevision+1 != request.ExpectedRevision {
				return handoff.ErrRevisionConflict
			}
			_, err := tx.Exec(ctx, `UPDATE handoffs SET objective=$5,objective_revision=$6,updated_at=$7
				WHERE tenant_id=$1 AND conversation_id=$2 AND id=$3 AND user_id=$4`,
				scope.TenantID, scope.ConversationID, id, scope.UserID, request.Objective, request.ExpectedRevision, r.clock.Now().UTC())
			return err
		})
}

func (r *Repository) ProvideHandoffInput(ctx context.Context, scope handoff.Scope, id identity.HandoffID, request handoff.InputRequest) (handoff.Snapshot, error) {
	if request.ExpectedRevision == 0 {
		return handoff.Snapshot{}, fmt.Errorf("%w: expected revision must be positive", handoff.ErrInvalidRequest)
	}
	if err := validateMutationMetadata(request.Actor, request.IdempotencyKey); err != nil {
		return handoff.Snapshot{}, err
	}
	if len(request.Input) == 0 || len(request.Input) > 65536 || !json.Valid(request.Input) {
		return handoff.Snapshot{}, fmt.Errorf("%w: invalid input JSON", handoff.ErrInvalidRequest)
	}
	request.Input = append(json.RawMessage(nil), request.Input...)
	payload, err := json.Marshal(struct {
		Actor            string          `json:"actor"`
		ExpectedRevision uint32          `json:"expected_revision"`
		Input            json.RawMessage `json:"input"`
	}{request.Actor, request.ExpectedRevision, request.Input})
	if err != nil {
		return handoff.Snapshot{}, err
	}
	return r.mutateHandoff(ctx, scope, id, "input", request.IdempotencyKey, "handoff.input_provided", payload,
		func(ctx context.Context, tx pgx.Tx, current mutationCurrent) error {
			if current.State != handoff.StateNeedsInput {
				return handoff.ErrInvalidTransition
			}
			if current.InputRevision+1 != request.ExpectedRevision {
				return handoff.ErrRevisionConflict
			}
			_, err := tx.Exec(ctx, `UPDATE handoffs SET input=$5::json,input_revision=$6,updated_at=$7
				WHERE tenant_id=$1 AND conversation_id=$2 AND id=$3 AND user_id=$4`,
				scope.TenantID, scope.ConversationID, id, scope.UserID, request.Input, request.ExpectedRevision, r.clock.Now().UTC())
			return err
		})
}

func (r *Repository) RequestHandoffCancel(ctx context.Context, scope handoff.Scope, id identity.HandoffID, request handoff.CancelRequest) (handoff.Snapshot, error) {
	if err := validateMutationMetadata(request.Actor, request.IdempotencyKey); err != nil {
		return handoff.Snapshot{}, err
	}
	if err := bounded("cancel reason", request.Reason, 2048, false); err != nil {
		return handoff.Snapshot{}, err
	}
	payload, err := json.Marshal(struct {
		Actor  string `json:"actor"`
		Reason string `json:"reason"`
	}{request.Actor, request.Reason})
	if err != nil {
		return handoff.Snapshot{}, err
	}
	return r.mutateHandoff(ctx, scope, id, "cancel", request.IdempotencyKey, "handoff.cancel_requested", payload,
		func(ctx context.Context, tx pgx.Tx, current mutationCurrent) error {
			if current.CancelRequested {
				return handoff.ErrInvalidTransition
			}
			now := r.clock.Now().UTC()
			_, err := tx.Exec(ctx, `UPDATE handoffs SET cancel_requested_at=$5,cancel_actor=$6,cancel_reason=NULLIF($7,''),updated_at=$5
				WHERE tenant_id=$1 AND conversation_id=$2 AND id=$3 AND user_id=$4`,
				scope.TenantID, scope.ConversationID, id, scope.UserID, now, request.Actor, request.Reason)
			return err
		})
}

type mutationCurrent struct {
	State               handoff.State
	ObjectiveRevision   uint32
	InputRevision       uint32
	Attempt             uint32
	CancelRequested     bool
	ToolRegistryVersion string
}

func (r *Repository) guardSteeringToolInvocations(ctx context.Context, tx pgx.Tx, scope handoff.Scope, id identity.HandoffID, registryVersion string) error {
	rows, err := tx.Query(ctx, `SELECT tool_name,status FROM tool_invocations
		WHERE tenant_id=$1 AND conversation_id=$2 AND handoff_id=$3`, scope.TenantID, scope.ConversationID, id)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var name, status string
		if err := rows.Scan(&name, &status); err != nil {
			return err
		}
		if nilInterface(r.toolGate) {
			return handoff.ErrSteeringBlocked
		}
		metadata, registered := r.toolGate.Metadata(ctx, registryVersion, name)
		if !registered {
			return handoff.ErrSteeringBlocked
		}
		if metadata.SideEffecting && status != "pending" {
			return handoff.ErrSteeringBlocked
		}
	}
	return rows.Err()
}

type mutationUpdate func(context.Context, pgx.Tx, mutationCurrent) error

func (r *Repository) mutateHandoff(ctx context.Context, scope handoff.Scope, id identity.HandoffID, action, key, kind string, payload []byte, update mutationUpdate) (handoff.Snapshot, error) {
	if err := validateContext(ctx); err != nil {
		return handoff.Snapshot{}, err
	}
	if err := validateScope(scope); err != nil {
		return handoff.Snapshot{}, err
	}
	if err := bounded("handoff id", string(id), maxIdentityBytes, true); err != nil {
		return handoff.Snapshot{}, err
	}
	eventID := mutationEventID(scope, id, action, key)
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return handoff.Snapshot{}, err
	}
	defer rollback(tx)
	if err := r.lockEventAppend(ctx, tx); err != nil {
		return handoff.Snapshot{}, err
	}
	var conversationStatus string
	err = tx.QueryRow(ctx, `SELECT status FROM conversations
		WHERE tenant_id=$1 AND id=$2 AND user_id=$3 FOR UPDATE`,
		scope.TenantID, scope.ConversationID, scope.UserID).Scan(&conversationStatus)
	if errors.Is(err, pgx.ErrNoRows) || conversationStatus == "deleted" {
		return handoff.Snapshot{}, handoff.ErrNotFound
	}
	if err != nil {
		return handoff.Snapshot{}, err
	}

	var existingPayload []byte
	var existingTenant, existingConversation, existingHandoff string
	err = tx.QueryRow(ctx, `SELECT payload::text,tenant_id,conversation_id,handoff_id FROM handoff_events WHERE event_id=$1`, eventID).
		Scan(&existingPayload, &existingTenant, &existingConversation, &existingHandoff)
	if err == nil {
		if existingTenant != scope.TenantID || existingConversation != string(scope.ConversationID) || existingHandoff != string(id) || !bytes.Equal(existingPayload, payload) {
			return handoff.Snapshot{}, handoff.ErrIdempotencyConflict
		}
		snapshot, err := loadHandoffRow(tx.QueryRow(ctx, `SELECT `+handoffColumns+` FROM handoffs
			WHERE tenant_id=$1 AND conversation_id=$2 AND id=$3 AND user_id=$4 FOR UPDATE`, scope.TenantID, scope.ConversationID, id, scope.UserID))
		if errors.Is(err, pgx.ErrNoRows) {
			return handoff.Snapshot{}, handoff.ErrNotFound
		}
		if err != nil {
			return handoff.Snapshot{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return handoff.Snapshot{}, err
		}
		return snapshot, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return handoff.Snapshot{}, err
	}

	var state, toolRegistryVersion string
	var objectiveRevision, inputRevision, attempt int64
	var cancelRequested bool
	err = tx.QueryRow(ctx, `SELECT state,objective_revision,input_revision,attempt,cancel_requested_at IS NOT NULL,tool_registry_version
		FROM handoffs WHERE tenant_id=$1 AND conversation_id=$2 AND id=$3 AND user_id=$4 FOR UPDATE`,
		scope.TenantID, scope.ConversationID, id, scope.UserID).Scan(&state, &objectiveRevision, &inputRevision, &attempt, &cancelRequested, &toolRegistryVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return handoff.Snapshot{}, handoff.ErrNotFound
	}
	if err != nil {
		return handoff.Snapshot{}, err
	}
	if objectiveRevision < 0 || objectiveRevision > math.MaxUint32 ||
		inputRevision < 0 || inputRevision > math.MaxUint32 || attempt < 0 || attempt > math.MaxUint32 {
		return handoff.Snapshot{}, errors.New("postgres: mutation numeric value out of range")
	}
	// A concurrent same-key transaction may have committed while this one was
	// waiting for the handoff row lock. Recheck under that lock before applying
	// revision/state guards so every retry observes the committed mutation.
	err = tx.QueryRow(ctx, `SELECT payload::text,tenant_id,conversation_id,handoff_id FROM handoff_events WHERE event_id=$1`, eventID).
		Scan(&existingPayload, &existingTenant, &existingConversation, &existingHandoff)
	if err == nil {
		if existingTenant != scope.TenantID || existingConversation != string(scope.ConversationID) || existingHandoff != string(id) || !bytes.Equal(existingPayload, payload) {
			return handoff.Snapshot{}, handoff.ErrIdempotencyConflict
		}
		snapshot, loadErr := loadHandoffRow(tx.QueryRow(ctx, `SELECT `+handoffColumns+` FROM handoffs
			WHERE tenant_id=$1 AND conversation_id=$2 AND id=$3 AND user_id=$4`, scope.TenantID, scope.ConversationID, id, scope.UserID))
		if loadErr != nil {
			return handoff.Snapshot{}, loadErr
		}
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return handoff.Snapshot{}, commitErr
		}
		return snapshot, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return handoff.Snapshot{}, err
	}
	current := mutationCurrent{State: handoff.State(state), ObjectiveRevision: uint32(objectiveRevision), InputRevision: uint32(inputRevision), Attempt: uint32(attempt), CancelRequested: cancelRequested, ToolRegistryVersion: toolRegistryVersion}
	if terminalHandoffState(current.State) {
		return handoff.Snapshot{}, errors.Join(handoff.ErrInvalidTransition, handoff.ErrTerminalState)
	}
	if err := update(ctx, tx, current); err != nil {
		return handoff.Snapshot{}, err
	}
	if err := r.after("mutation_update"); err != nil {
		return handoff.Snapshot{}, err
	}
	now := r.clock.Now().UTC()
	var seq int64
	err = tx.QueryRow(ctx, `INSERT INTO handoff_events
		(event_id,tenant_id,conversation_id,handoff_id,kind,state,attempt,payload,created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8::json,$9) RETURNING seq`, eventID, scope.TenantID,
		scope.ConversationID, id, kind, current.State, current.Attempt, payload, now).Scan(&seq)
	if err != nil {
		return handoff.Snapshot{}, mapWriteError(err)
	}
	if err := r.after("mutation_event"); err != nil {
		return handoff.Snapshot{}, err
	}
	wakeup, _ := json.Marshal(struct {
		TenantID       string `json:"tenant_id"`
		ConversationID string `json:"conversation_id"`
		HighestSeq     int64  `json:"highest_seq"`
	}{scope.TenantID, string(scope.ConversationID), seq})
	_, err = tx.Exec(ctx, `INSERT INTO outbox (topic,aggregate_id,event_seq,payload,created_at)
		VALUES ('handoff.event',$1,$2,$3::json,$4)`, id, seq, wakeup, now)
	if err != nil {
		return handoff.Snapshot{}, mapWriteError(err)
	}
	if err := r.after("mutation_wakeup"); err != nil {
		return handoff.Snapshot{}, err
	}
	snapshot, err := loadHandoffRow(tx.QueryRow(ctx, `SELECT `+handoffColumns+` FROM handoffs
		WHERE tenant_id=$1 AND conversation_id=$2 AND id=$3 AND user_id=$4`, scope.TenantID, scope.ConversationID, id, scope.UserID))
	if err != nil {
		return handoff.Snapshot{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return handoff.Snapshot{}, err
	}
	return snapshot, nil
}

func mutationEventID(scope handoff.Scope, id identity.HandoffID, action, key string) string {
	digest := sha256.Sum256([]byte(strings.Join([]string{scope.TenantID, string(scope.ConversationID), string(id), action, key}, "\x00")))
	return fmt.Sprintf("mutation:%x", digest[:])
}

func validateMutationMetadata(actor, key string) error {
	if err := bounded("actor", actor, maxIdentityBytes, true); err != nil {
		return err
	}
	return bounded("idempotency key", key, maxIdentityBytes, true)
}

func terminalHandoffState(state handoff.State) bool {
	switch state {
	case handoff.StateSucceeded, handoff.StateFailed, handoff.StateCancelled, handoff.StateExpired:
		return true
	default:
		return false
	}
}

func loadHandoffRow(row pgx.Row) (handoff.Snapshot, error) {
	var out handoff.Snapshot
	var tenant, conversation, user, session, sourceTurn string
	var sourceRevision, objectiveRevision, inputRevision, consumedInput, attempt, retryCount int64
	var profileVersion, recipeVersion int64
	var state string
	var allowlistRaw, toolPolicyRaw, approvalRaw, knowledgeRaw, evidenceRaw, inputRaw []byte
	var mutationID, resultSummary, structured, safetyDecision, usage, failureCode, failureMessage sql.NullString
	var failureRetryable sql.NullBool
	err := row.Scan(&out.ID, &tenant, &conversation, &user, &session, &sourceTurn, &sourceRevision,
		&out.Request.UserRequest, &out.Request.Objective, &out.Request.ContextRef, &out.Request.ContextHash,
		&out.Request.Deadline, &out.Request.TokenBudget, &out.Request.Priority, &allowlistRaw, &toolPolicyRaw, &approvalRaw,
		&out.Request.Versions.Model, &out.Request.Versions.Prompt, &out.Request.Versions.ToolRegistry,
		&out.Request.Versions.Config, &out.Request.Profile.ProfileID, &profileVersion, &out.Request.Profile.SnapshotHash,
		&out.Request.Profile.EngineVersion, &out.Request.Profile.RecipeID, &recipeVersion, &out.Request.Profile.RecipeHash,
		&out.Request.Profile.RegistryVersion, &knowledgeRaw, &evidenceRaw, &out.Request.PresentationHint,
		&out.Request.IdempotencyKey, &out.RequestHash,
		&state, &objectiveRevision, &inputRevision, &consumedInput, &inputRaw, &attempt, &retryCount, &mutationID,
		&resultSummary, &structured, &safetyDecision, &usage, &failureCode, &failureRetryable, &failureMessage, &out.CreatedAt, &out.UpdatedAt)
	if err != nil {
		return handoff.Snapshot{}, err
	}
	for _, value := range []int64{sourceRevision, objectiveRevision, inputRevision, consumedInput, attempt, retryCount} {
		if value < 0 || value > math.MaxUint32 {
			return handoff.Snapshot{}, errors.New("postgres: handoff numeric value out of range")
		}
	}
	if profileVersion <= 0 || recipeVersion <= 0 {
		return handoff.Snapshot{}, errors.New("postgres: handoff profile version out of range")
	}
	out.Request.Scope = handoff.Scope{TenantID: tenant, UserID: user, ConversationID: identity.ConversationID(conversation)}
	out.Request.SessionID = identity.SessionID(session)
	out.Request.SourceTurn = identity.TurnKey{ID: sourceTurn, Revision: uint32(sourceRevision)}
	if err := json.Unmarshal(allowlistRaw, &out.Request.ToolAllowlist); err != nil {
		return handoff.Snapshot{}, err
	}
	out.Request.ToolPolicy = append(json.RawMessage(nil), toolPolicyRaw...)
	out.Request.ApprovalPolicy = append(json.RawMessage(nil), approvalRaw...)
	out.Request.Profile.ProfileVersion = uint64(profileVersion)
	out.Request.Profile.RecipeVersion = uint64(recipeVersion)
	if err := json.Unmarshal(knowledgeRaw, &out.Request.Profile.Knowledge); err != nil {
		return handoff.Snapshot{}, err
	}
	out.Request.Profile.Evidence = append(json.RawMessage(nil), evidenceRaw...)
	out.State = handoff.State(state)
	out.ObjectiveRevision, out.InputRevision, out.ConsumedInput = uint32(objectiveRevision), uint32(inputRevision), uint32(consumedInput)
	out.Input = append(json.RawMessage(nil), inputRaw...)
	out.Attempt, out.RetryCount = uint32(attempt), uint32(retryCount)
	if state == string(handoff.StateSucceeded) {
		out.Result = &handoff.Result{MutationID: mutationID.String, Summary: resultSummary.String,
			Structured:     append(json.RawMessage(nil), structured.String...),
			SafetyDecision: append(json.RawMessage(nil), safetyDecision.String...),
			Usage:          append(json.RawMessage(nil), usage.String...)}
	}
	if state == string(handoff.StateFailed) {
		out.Failure = &handoff.Failure{MutationID: mutationID.String, Code: failureCode.String,
			Retryable: failureRetryable.Bool, Message: failureMessage.String}
	}
	out.Request.Deadline = out.Request.Deadline.UTC()
	normalized, err := handoff.NormalizeCreateRequest(out.Request)
	if err != nil {
		return handoff.Snapshot{}, err
	}
	out.Request = normalized
	out.CreatedAt, out.UpdatedAt = out.CreatedAt.UTC(), out.UpdatedAt.UTC()
	return out, nil
}
