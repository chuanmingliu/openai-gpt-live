//go:build integration

package postgres_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/testkit"
	"github.com/chuanmingliu/openai-gpt-live/migrations"
	migratedatabase "github.com/golang-migrate/migrate/v4/database"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var requiredColumns = map[string]map[string]string{
	"conversations": {
		"tenant_id": "text", "id": "text", "user_id": "text", "status": "text",
		"retention_until": "timestamp with time zone", "created_at": "timestamp with time zone", "updated_at": "timestamp with time zone",
	},
	"conversation_items": {
		"tenant_id": "text", "conversation_id": "text", "session_id": "text", "turn_id": "text",
		"turn_revision": "bigint", "sequence": "bigint", "role": "text", "content": "text",
		"content_hash": "text", "created_at": "timestamp with time zone", "updated_at": "timestamp with time zone",
	},
	"coordinator_work_journal": {
		"work_key": "text", "kind": "text", "tenant_id": "text", "user_id": "text", "conversation_id": "text",
		"session_id": "text", "delivery_affinity": "text", "incarnation": "bigint", "local_sequence": "bigint", "payload": "json",
		"payload_hash": "text", "storage_sequence": "bigint", "delivered": "boolean", "claim_owner": "text",
		"claim_until": "timestamp with time zone", "claim_generation": "bigint", "created_at": "timestamp with time zone", "delivered_at": "timestamp with time zone",
	},
	"handoffs": {
		"tenant_id": "text", "conversation_id": "text", "id": "text", "user_id": "text", "session_id": "text",
		"source_turn_id": "text", "source_turn_revision": "bigint", "user_request": "text", "objective": "text",
		"context_ref": "text", "context_hash": "text", "deadline": "timestamp with time zone", "token_budget": "integer",
		"priority": "smallint", "tool_allowlist": "json", "approval_policy": "json", "model_version": "text",
		"prompt_version": "text", "tool_registry_version": "text", "config_version": "text", "presentation_hint": "text",
		"idempotency_key": "text", "request_hash": "text", "state": "text", "objective_revision": "bigint",
		"input_revision": "bigint", "consumed_input_revision": "bigint", "input": "json", "ready_at": "timestamp with time zone",
		"attempt": "integer", "retry_count": "integer", "lease_owner": "text", "lease_expires_at": "timestamp with time zone",
		"lease_version": "bigint", "cancel_requested_at": "timestamp with time zone", "cancel_actor": "text", "cancel_reason": "text",
		"terminal_mutation_id": "text", "result_summary": "text", "structured_result": "json", "failure_code": "text",
		"failure_retryable": "boolean", "failure_message": "text", "safety_decision": "json", "usage": "json",
		"terminal_at": "timestamp with time zone", "created_at": "timestamp with time zone", "updated_at": "timestamp with time zone",
	},
	"handoff_events": {
		"seq": "bigint", "event_id": "text", "tenant_id": "text", "conversation_id": "text", "handoff_id": "text",
		"kind": "text", "state": "text", "attempt": "integer", "payload": "json", "created_at": "timestamp with time zone",
	},
	"outbox": {
		"id": "bigint", "topic": "text", "aggregate_id": "text", "event_seq": "bigint", "payload": "json",
		"created_at": "timestamp with time zone", "published_at": "timestamp with time zone", "attempts": "integer", "last_error": "text",
	},
	"tool_invocations": {
		"invocation_id": "text", "tenant_id": "text", "conversation_id": "text", "handoff_id": "text", "attempt": "integer",
		"objective_revision": "bigint", "tool_ordinal": "integer", "tool_name": "text", "approval_state": "text",
		"approval_expires_at": "timestamp with time zone", "idempotency_key": "text", "canonical_arguments_hash": "text",
		"arguments_ciphertext": "bytea", "arguments_nonce": "bytea", "arguments_key_id": "text", "result_metadata": "json",
		"result": "json", "mutation_id": "text", "status": "text", "created_at": "timestamp with time zone", "updated_at": "timestamp with time zone",
	},
	"delivery_cursors": {
		"consumer_id": "text", "tenant_id": "text", "conversation_id": "text", "last_sequence": "bigint",
		"created_at": "timestamp with time zone", "updated_at": "timestamp with time zone",
	},
	"session_admission_leases": {
		"instance_id": "text", "session_id": "text", "tenant_id": "text", "user_id": "text",
		"expires_at": "timestamp with time zone", "heartbeat_at": "timestamp with time zone", "created_at": "timestamp with time zone",
	},
	"deletion_audits": {
		"audit_id": "text", "tenant_id": "text", "conversation_hash": "text", "actor": "text",
		"reason_code": "text", "deleted_at": "timestamp with time zone",
	},
}

var requiredDefaults = map[string]string{
	"conversations.status":                      `'active'::text`,
	"conversations.created_at":                  `CURRENT_TIMESTAMP`,
	"conversations.updated_at":                  `CURRENT_TIMESTAMP`,
	"conversation_items.created_at":             `CURRENT_TIMESTAMP`,
	"conversation_items.updated_at":             `CURRENT_TIMESTAMP`,
	"coordinator_work_journal.delivered":        `false`,
	"coordinator_work_journal.claim_generation": `0`,
	"coordinator_work_journal.created_at":       `CURRENT_TIMESTAMP`,
	"handoffs.priority":                         `0`,
	"handoffs.tool_allowlist":                   `'[]'::json`,
	"handoffs.presentation_hint":                `''::text`,
	"handoffs.state":                            `'pending'::text`,
	"handoffs.objective_revision":               `0`,
	"handoffs.input_revision":                   `0`,
	"handoffs.consumed_input_revision":          `0`,
	"handoffs.ready_at":                         `CURRENT_TIMESTAMP`,
	"handoffs.attempt":                          `0`,
	"handoffs.retry_count":                      `0`,
	"handoffs.lease_version":                    `0`,
	"handoffs.created_at":                       `CURRENT_TIMESTAMP`,
	"handoffs.updated_at":                       `CURRENT_TIMESTAMP`,
	"handoff_events.seq":                        `nextval('handoff_events_seq_seq'::regclass)`,
	"handoff_events.attempt":                    `0`,
	"handoff_events.created_at":                 `CURRENT_TIMESTAMP`,
	"outbox.id":                                 `nextval('outbox_id_seq'::regclass)`,
	"outbox.created_at":                         `CURRENT_TIMESTAMP`,
	"outbox.attempts":                           `0`,
	"tool_invocations.status":                   `'pending'::text`,
	"tool_invocations.created_at":               `CURRENT_TIMESTAMP`,
	"tool_invocations.updated_at":               `CURRENT_TIMESTAMP`,
	"delivery_cursors.last_sequence":            `0`,
	"delivery_cursors.created_at":               `CURRENT_TIMESTAMP`,
	"delivery_cursors.updated_at":               `CURRENT_TIMESTAMP`,
	"session_admission_leases.created_at":       `CURRENT_TIMESTAMP`,
	"deletion_audits.deleted_at":                `CURRENT_TIMESTAMP`,
}

var requiredNamedConstraintNames = strings.Fields(`
	conversations_tenant_id_size_check conversations_id_size_check conversations_user_id_size_check
	conversations_status_check conversations_retention_check conversations_timestamps_check
	conversation_items_sequence_uniq conversation_items_turn_role_uniq
	conversation_items_tenant_id_size_check conversation_items_conversation_id_size_check
	conversation_items_session_id_size_check conversation_items_turn_id_size_check
	conversation_items_turn_revision_check conversation_items_sequence_check conversation_items_role_check
	conversation_items_content_size_check conversation_items_content_hash_size_check conversation_items_timestamps_check
	coordinator_work_journal_kind_check coordinator_work_journal_work_key_size coordinator_work_journal_identity_size
	coordinator_work_journal_incarnation_check coordinator_work_journal_local_sequence_check
	coordinator_work_journal_payload_size coordinator_work_journal_payload_hash_size
	coordinator_work_journal_payload_depth_check
	coordinator_work_journal_storage_sequence_check coordinator_work_journal_delivery_check coordinator_work_journal_claim_check
	coordinator_work_journal_claim_owner_size coordinator_work_journal_claim_generation_check
	handoffs_tenant_idempotency_uniq handoffs_tenant_id_size_check handoffs_conversation_id_size_check
	handoffs_id_size_check handoffs_user_id_size_check handoffs_session_id_size_check
	handoffs_source_turn_id_size_check handoffs_source_turn_revision_check handoffs_user_request_size_check
	handoffs_objective_size_check handoffs_context_ref_size_check handoffs_context_hash_size_check
	handoffs_deadline_check handoffs_token_budget_check handoffs_priority_check handoffs_tool_allowlist_check
	handoffs_approval_policy_check handoffs_model_version_size_check handoffs_prompt_version_size_check
	handoffs_tool_registry_version_size_check handoffs_config_version_size_check handoffs_presentation_hint_size_check
	handoffs_idempotency_key_size_check handoffs_request_hash_size_check handoffs_state_check handoffs_revision_check
	handoffs_input_check handoffs_ready_at_check handoffs_attempt_check handoffs_lease_tuple_check
	handoffs_lease_owner_size_check handoffs_cancel_tuple_check handoffs_cancel_actor_size_check
	handoffs_cancel_reason_size_check handoffs_terminal_mutation_id_size_check handoffs_result_summary_size_check
	handoffs_structured_result_check handoffs_failure_code_size_check handoffs_failure_message_size_check
	handoffs_safety_decision_check handoffs_usage_check handoffs_terminal_fields_check handoffs_timestamps_check
	handoff_events_event_id_uniq handoff_events_event_id_size_check handoff_events_kind_size_check
	handoff_events_state_check handoff_events_attempt_check handoff_events_payload_check
	outbox_topic_event_seq_uniq outbox_topic_size_check outbox_aggregate_id_size_check outbox_event_seq_check
	outbox_payload_check outbox_payload_keys_check outbox_attempts_check outbox_last_error_size_check outbox_timestamps_check
	tool_invocations_stable_uniq tool_invocations_idempotency_uniq tool_invocations_invocation_id_size_check
	tool_invocations_attempt_check tool_invocations_objective_revision_check tool_invocations_tool_ordinal_check
	tool_invocations_tool_name_size_check tool_invocations_approval_state_check tool_invocations_idempotency_key_size_check
	tool_invocations_arguments_hash_size_check tool_invocations_ciphertext_check tool_invocations_nonce_check
	tool_invocations_key_id_size_check tool_invocations_result_metadata_check tool_invocations_result_check
	tool_invocations_mutation_id_size_check tool_invocations_status_check tool_invocations_timestamps_check
	delivery_cursors_consumer_id_size_check delivery_cursors_sequence_check delivery_cursors_timestamps_check
	session_admission_leases_instance_id_size_check session_admission_leases_session_id_size_check
	session_admission_leases_tenant_id_size_check session_admission_leases_user_id_size_check
	session_admission_leases_timestamps_check deletion_audits_audit_id_size_check deletion_audits_tenant_id_size_check
	deletion_audits_conversation_hash_size_check deletion_audits_actor_size_check deletion_audits_reason_code_size_check
`)

// Removing the embedded migration, leaving it dirty, or reporting a stale
// version must make this lifecycle contract fail.
func TestSchemaMigrationLifecycle(t *testing.T) {
	pool := testkit.Postgres(t)

	assert.Error(t, migrations.Check(t.Context(), pool))
	require.NoError(t, migrations.Up(t.Context(), pool))
	require.NoError(t, migrations.Check(t.Context(), pool))
	require.NoError(t, migrations.Up(t.Context(), pool))

	version, dirty, err := migrations.Version(t.Context(), pool)
	require.NoError(t, err)
	assert.Equal(t, uint(3), version)
	assert.False(t, dirty)
	var journalPresent bool
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT to_regclass('coordinator_work_journal') IS NOT NULL`).Scan(&journalPresent))
	assert.True(t, journalPresent)

	require.NoError(t, migrations.Down(t.Context(), pool))
	assert.Error(t, migrations.Check(t.Context(), pool))
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT to_regclass('coordinator_work_journal') IS NOT NULL`).Scan(&journalPresent))
	assert.False(t, journalPresent)
	for _, signature := range []string{
		"canonical_tool_name_bytes(text)",
		"canonical_json_depth_valid(json)",
		"tool_allowlist_valid(json)",
		"outbox_payload_keys_valid(json)",
	} {
		var removed bool
		require.NoError(t, pool.QueryRow(t.Context(), `SELECT to_regprocedure($1) IS NULL`, signature).Scan(&removed))
		assert.True(t, removed, "%s must be removed by Down", signature)
	}
	require.NoError(t, migrations.Down(t.Context(), pool))
	require.NoError(t, migrations.Up(t.Context(), pool))
	require.NoError(t, migrations.Steps(t.Context(), pool, -1))
	version, dirty, err = migrations.Version(t.Context(), pool)
	require.NoError(t, err)
	assert.Equal(t, uint(2), version)
	assert.False(t, dirty)
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT to_regclass('coordinator_work_journal') IS NOT NULL`).Scan(&journalPresent))
	assert.True(t, journalPresent)
	require.NoError(t, migrations.Steps(t.Context(), pool, -1))
	version, dirty, err = migrations.Version(t.Context(), pool)
	require.NoError(t, err)
	assert.Equal(t, uint(1), version)
	assert.False(t, dirty)
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT to_regclass('coordinator_work_journal') IS NOT NULL`).Scan(&journalPresent))
	assert.False(t, journalPresent)
	require.NoError(t, migrations.Steps(t.Context(), pool, 2))
	require.NoError(t, migrations.Steps(t.Context(), pool, 0))
	require.NoError(t, migrations.Check(t.Context(), pool))
}

func TestSchemaTask12ToolInvocationScopeConstraint(t *testing.T) {
	pool := migratedPostgres(t)
	assertToolInvocationStableConstraintColumns(t, pool, []string{
		"tenant_id", "conversation_id", "handoff_id", "objective_revision",
		"tool_ordinal", "tool_name", "canonical_arguments_hash",
	})

	insertConversation(t, pool, "tenant-scope", "conversation-a")
	insertConversation(t, pool, "tenant-scope", "conversation-b")
	require.NoError(t, insertHandoff(t, pool, "tenant-scope", "conversation-a", "same-handoff", "handoff-idem-a"))
	require.NoError(t, insertHandoff(t, pool, "tenant-scope", "conversation-b", "same-handoff", "handoff-idem-b"))
	require.NoError(t, insertToolInvocation(t, pool, "invocation-a", "tenant-scope", "conversation-a", "same-handoff", "tool-idem-a"))
	require.NoError(t, insertToolInvocation(t, pool, "invocation-b", "tenant-scope", "conversation-b", "same-handoff", "tool-idem-b"))

	err := insertToolInvocation(t, pool, "invocation-duplicate", "tenant-scope", "conversation-a", "same-handoff", "tool-idem-duplicate")
	assertConstraint(t, err, "tool_invocations_stable_uniq")
}

func TestSchemaTask12ToolInvocationScopeSafeDownAndUp(t *testing.T) {
	pool := migratedPostgres(t)
	insertConversation(t, pool, "tenant-safe", "conversation-safe")
	require.NoError(t, insertHandoff(t, pool, "tenant-safe", "conversation-safe", "handoff-safe", "handoff-idem-safe"))
	require.NoError(t, insertToolInvocation(t, pool, "invocation-safe", "tenant-safe", "conversation-safe", "handoff-safe", "tool-idem-safe"))

	require.NoError(t, migrations.Steps(t.Context(), pool, -1))
	version, dirty, err := migrations.Version(t.Context(), pool)
	require.NoError(t, err)
	assert.Equal(t, uint(2), version)
	assert.False(t, dirty)
	assertToolInvocationStableConstraintColumns(t, pool, []string{
		"tenant_id", "handoff_id", "objective_revision", "tool_ordinal", "tool_name", "canonical_arguments_hash",
	})

	require.NoError(t, migrations.Up(t.Context(), pool))
	require.NoError(t, migrations.Check(t.Context(), pool))
	assertToolInvocationStableConstraintColumns(t, pool, []string{
		"tenant_id", "conversation_id", "handoff_id", "objective_revision",
		"tool_ordinal", "tool_name", "canonical_arguments_hash",
	})
	var count int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM tool_invocations WHERE invocation_id='invocation-safe'`).Scan(&count))
	assert.Equal(t, 1, count)
}

func TestSchemaTask12ToolInvocationScopeConflictingDownRefusalPreservesV3(t *testing.T) {
	pool := migratedPostgres(t)
	insertConversation(t, pool, "tenant-conflict", "conversation-a")
	insertConversation(t, pool, "tenant-conflict", "conversation-b")
	require.NoError(t, insertHandoff(t, pool, "tenant-conflict", "conversation-a", "same-handoff", "handoff-idem-a"))
	require.NoError(t, insertHandoff(t, pool, "tenant-conflict", "conversation-b", "same-handoff", "handoff-idem-b"))
	require.NoError(t, insertToolInvocation(t, pool, "invocation-a", "tenant-conflict", "conversation-a", "same-handoff", "tool-idem-a"))
	require.NoError(t, insertToolInvocation(t, pool, "invocation-b", "tenant-conflict", "conversation-b", "same-handoff", "tool-idem-b"))

	err := migrations.Steps(t.Context(), pool, -1)
	require.ErrorIs(t, err, migrations.ErrToolInvocationScopeConflict)
	version, dirty, versionErr := migrations.Version(t.Context(), pool)
	require.NoError(t, versionErr)
	assert.Equal(t, uint(3), version)
	assert.False(t, dirty)
	require.NoError(t, migrations.Check(t.Context(), pool))
	assertToolInvocationStableConstraintColumns(t, pool, []string{
		"tenant_id", "conversation_id", "handoff_id", "objective_revision",
		"tool_ordinal", "tool_name", "canonical_arguments_hash",
	})
	var count int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM tool_invocations WHERE tenant_id='tenant-conflict'`).Scan(&count))
	assert.Equal(t, 2, count)
}

// Replacing caller cancellation with a post-operation ctx.Err check leaves a
// migrator wedged behind the advisory lock and must fail this test.
func TestSchemaMigrationCancellationWhileWaitingForAdvisoryLock(t *testing.T) {
	pool := testkit.Postgres(t)
	var databaseName, schemaName string
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT current_database(), current_schema()`).Scan(&databaseName, &schemaName))
	lockID, err := migratedatabase.GenerateAdvisoryLockId(databaseName, schemaName, "schema_migrations")
	require.NoError(t, err)
	locker, err := pool.Acquire(t.Context())
	require.NoError(t, err)
	defer locker.Release()
	require.NoError(t, locker.QueryRow(t.Context(), `SELECT pg_advisory_lock($1)`, lockID).Scan(new(any)))
	locked := true
	defer func() {
		if locked {
			_, _ = locker.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, lockID)
		}
	}()

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- migrations.Up(ctx, pool) }()
	waitForBlockedMigration(t, pool, "advisory")
	cancel()
	assertPromptCancellation(t, done, func() {
		_, _ = locker.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, lockID)
		locked = false
	})
	_, err = locker.Exec(t.Context(), `SELECT pg_advisory_unlock($1)`, lockID)
	require.NoError(t, err)
	locked = false
	require.NoError(t, migrations.Up(t.Context(), pool))
	require.NoError(t, migrations.Check(t.Context(), pool))
}

// The server lock timeout must fire before migrate's client timeout so the
// client never abandons a still-running advisory-lock goroutine.
func TestSchemaMigrationLockTimeoutLeavesNoOrphan(t *testing.T) {
	pool := testkit.Postgres(t)
	var databaseName, schemaName string
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT current_database(), current_schema()`).Scan(&databaseName, &schemaName))
	lockID, err := migratedatabase.GenerateAdvisoryLockId(databaseName, schemaName, "schema_migrations")
	require.NoError(t, err)
	locker, err := pool.Acquire(t.Context())
	require.NoError(t, err)
	defer locker.Release()
	_, err = locker.Exec(t.Context(), `SELECT pg_advisory_lock($1)`, lockID)
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- migrations.Up(t.Context(), pool) }()
	waitForBlockedMigration(t, pool, "advisory")
	deadline := time.NewTimer(7 * time.Second)
	defer deadline.Stop()
	select {
	case err := <-done:
		require.Error(t, err)
		assert.NotErrorIs(t, err, context.Canceled)
	case <-deadline.C:
		t.Fatal("server lock timeout did not beat the migrate client timeout")
	}
	_, err = locker.Exec(t.Context(), `SELECT pg_advisory_unlock($1)`, lockID)
	require.NoError(t, err)
	require.NoError(t, migrations.Up(t.Context(), pool))
	require.NoError(t, migrations.Check(t.Context(), pool))
}

// Cancelling a migration blocked inside its SQL must cancel that dedicated
// backend, roll back the statement, and preserve a clean schema version.
func TestSchemaMigrationCancellationWhileExecutingSQL(t *testing.T) {
	pool := migratedPostgres(t)
	require.NoError(t, migrations.Steps(t.Context(), pool, -1))
	locker, err := pool.Acquire(t.Context())
	require.NoError(t, err)
	defer locker.Release()
	tx, err := locker.Begin(t.Context())
	require.NoError(t, err)
	require.NoError(t, func() error {
		_, err := tx.Exec(t.Context(), `LOCK TABLE conversations IN ACCESS SHARE MODE`)
		return err
	}())
	locked := true
	defer func() {
		if locked {
			_ = tx.Rollback(context.Background())
		}
	}()

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- migrations.Down(ctx, pool) }()
	waitForBlockedMigration(t, pool, "relation")
	cancel()
	assertPromptCancellation(t, done, func() {
		_ = tx.Rollback(context.Background())
		locked = false
	})
	require.NoError(t, tx.Rollback(t.Context()))
	locked = false
	version, dirty, err := migrations.Version(t.Context(), pool)
	require.NoError(t, err)
	assert.Equal(t, uint(1), version)
	assert.True(t, dirty, "a cancelled down migration must never report a falsely clean schema")
	assert.ErrorContains(t, migrations.Check(t.Context(), pool), "dirty")
	var coordinatorTablePresent bool
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT to_regclass('coordinator_work_journal') IS NOT NULL`).Scan(&coordinatorTablePresent))
	assert.True(t, coordinatorTablePresent, "the cancelled v2 down transaction must roll back its table drop")
	_, err = pool.Exec(t.Context(), `UPDATE schema_migrations SET version=2, dirty=false`)
	require.NoError(t, err)
	require.NoError(t, migrations.Down(t.Context(), pool))
	version, dirty, err = migrations.Version(t.Context(), pool)
	require.NoError(t, err)
	assert.Equal(t, uint(0), version)
	assert.False(t, dirty)
	require.NoError(t, migrations.Up(t.Context(), pool))
	require.NoError(t, migrations.Check(t.Context(), pool))
}

// Dropping a required column, changing its PostgreSQL type, or adding a
// forbidden sensitive representation must fail this catalog contract.
func TestSchemaCatalog(t *testing.T) {
	pool := migratedPostgres(t)
	rows, err := pool.Query(t.Context(), `
		SELECT table_name, column_name, data_type, is_nullable, column_default
		FROM information_schema.columns
		WHERE table_schema = current_schema()
		ORDER BY table_name, ordinal_position`)
	require.NoError(t, err)
	defer rows.Close()

	got := make(map[string]map[string]string)
	gotDefaults := make(map[string]string)
	for rows.Next() {
		var table, column, dataType, nullable string
		var defaultValue *string
		require.NoError(t, rows.Scan(&table, &column, &dataType, &nullable, &defaultValue))
		if table == "schema_migrations" {
			continue
		}
		if got[table] == nil {
			got[table] = make(map[string]string)
		}
		got[table][column] = dataType
		if defaultValue != nil {
			gotDefaults[table+"."+column] = *defaultValue
		}
		wantNullable := "NO"
		if allowedNullableColumn(table, column) {
			wantNullable = "YES"
		}
		assert.Equal(t, wantNullable, nullable, "%s.%s nullability", table, column)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, requiredColumns, got)
	assert.Equal(t, requiredDefaults, gotDefaults, "required defaults and only required defaults")

	for table, columns := range got {
		for column := range columns {
			lower := strings.ToLower(column)
			for _, forbidden := range []string{"raw_audio", "partial_transcript", "token_delta", "plaintext_argument"} {
				assert.NotContains(t, lower, forbidden, "%s.%s", table, column)
			}
			if table == "deletion_audits" {
				assert.NotContains(t, lower, "conversation_id")
				assert.NotContains(t, lower, "transcript")
				assert.NotContains(t, lower, "result")
				assert.NotContains(t, lower, "argument")
			}
		}
	}
}

// Removing or renaming a required uniqueness/check invariant, or permitting
// one of its invalid single-field mutations, must fail this test.
func TestSchemaNamedConstraints(t *testing.T) {
	pool := migratedPostgres(t)
	rows, err := pool.Query(t.Context(), `
		SELECT conname
		FROM pg_constraint
		WHERE connamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema())
			AND contype IN ('c', 'u')
		ORDER BY conname`)
	require.NoError(t, err)
	var gotNames []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		gotNames = append(gotNames, name)
	}
	require.NoError(t, rows.Err())
	rows.Close()
	wantNames := slices.Clone(requiredNamedConstraintNames)
	slices.Sort(wantNames)
	assert.Equal(t, wantNames, gotNames, "every CHECK and UNIQUE constraint name")

	insertConversation(t, pool, "tenant-a", "conversation-a")
	require.NoError(t, insertHandoff(t, pool, "tenant-a", "conversation-a", "handoff-a", "idem-a"))

	mutations := []struct {
		name       string
		constraint string
		query      string
	}{
		{"invalid state", "handoffs_state_check", `UPDATE handoffs SET state = 'unknown' WHERE id = 'handoff-a'`},
		{"zero token budget", "handoffs_token_budget_check", `UPDATE handoffs SET token_budget = 0 WHERE id = 'handoff-a'`},
		{"negative attempt", "handoffs_attempt_check", `UPDATE handoffs SET attempt = -1 WHERE id = 'handoff-a'`},
		{"negative retry", "handoffs_attempt_check", `UPDATE handoffs SET retry_count = -1 WHERE id = 'handoff-a'`},
		{"partial lease", "handoffs_lease_tuple_check", `UPDATE handoffs SET lease_owner = 'worker-a' WHERE id = 'handoff-a'`},
		{"negative lease version", "handoffs_lease_tuple_check", `UPDATE handoffs SET lease_version = -1 WHERE id = 'handoff-a'`},
		{"consumed input ahead", "handoffs_revision_check", `UPDATE handoffs SET consumed_input_revision = 1 WHERE id = 'handoff-a'`},
		{"input revision without input", "handoffs_input_check", `UPDATE handoffs SET input_revision = 1 WHERE id = 'handoff-a'`},
		{"nonterminal terminal timestamp", "handoffs_terminal_fields_check", `UPDATE handoffs SET terminal_at = CURRENT_TIMESTAMP WHERE id = 'handoff-a'`},
		{"wrong allowlist shape", "handoffs_tool_allowlist_check", `UPDATE handoffs SET tool_allowlist = '{}' WHERE id = 'handoff-a'`},
		{"policy over limit", "handoffs_approval_policy_check", `UPDATE handoffs SET approval_policy=to_json(repeat('x',65535)) WHERE id='handoff-a'`},
		{"priority too large", "handoffs_priority_check", `UPDATE handoffs SET priority = 1001 WHERE id = 'handoff-a'`},
		{"deadline before create", "handoffs_deadline_check", `UPDATE handoffs SET deadline = created_at WHERE id = 'handoff-a'`},
		{"blank objective", "handoffs_objective_size_check", `UPDATE handoffs SET objective = '' WHERE id = 'handoff-a'`},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			_, err := pool.Exec(t.Context(), mutation.query)
			assertConstraint(t, err, mutation.constraint)
		})
	}

	assertConstraint(t, insertHandoff(t, pool, "tenant-a", "conversation-a", "handoff-b", "idem-a"), "handoffs_tenant_idempotency_uniq")

	_, err = pool.Exec(t.Context(), `INSERT INTO conversation_items
		(tenant_id, conversation_id, session_id, turn_id, turn_revision, sequence, role, content, content_hash)
		VALUES ('tenant-a','conversation-a','session-a','turn-a',1,1,'user','hello','hash-a')`)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `INSERT INTO conversation_items
		(tenant_id, conversation_id, session_id, turn_id, turn_revision, sequence, role, content, content_hash)
		VALUES ('tenant-a','conversation-a','session-b','turn-b',1,1,'assistant','hello','hash-b')`)
	assertConstraint(t, err, "conversation_items_sequence_uniq")
	_, err = pool.Exec(t.Context(), `INSERT INTO conversation_items
		(tenant_id, conversation_id, session_id, turn_id, turn_revision, sequence, role, content, content_hash)
		VALUES ('tenant-a','conversation-a','session-b','turn-a',1,2,'user','hello','hash-b')`)
	assertConstraint(t, err, "conversation_items_turn_role_uniq")
}

// Removing the bounded role/status/revision/cursor/timestamp checks from any
// durable table must make a concrete invalid write succeed and fail this test.
func TestSchemaChecksAcrossAllTables(t *testing.T) {
	pool := migratedPostgres(t)
	insertConversation(t, pool, "tenant-a", "conversation-a")
	require.NoError(t, insertHandoff(t, pool, "tenant-a", "conversation-a", "handoff-a", "idem-a"))
	sequence := insertEvent(t, pool, "event-a", "tenant-a", "conversation-a", "handoff-a")
	_, err := pool.Exec(t.Context(), `INSERT INTO conversation_items
		(tenant_id,conversation_id,session_id,turn_id,turn_revision,sequence,role,content,content_hash)
		VALUES ('tenant-a','conversation-a','session','turn',1,1,'user','hello','hash')`)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `INSERT INTO outbox (topic,aggregate_id,event_seq,payload)
		VALUES ('handoff.event','handoff-a',$1,'{"tenant_id":"tenant-a","conversation_id":"conversation-a","highest_seq":1}')`, sequence)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `INSERT INTO delivery_cursors (consumer_id,tenant_id,conversation_id)
		VALUES ('consumer','tenant-a','conversation-a')`)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `INSERT INTO session_admission_leases
		(instance_id,session_id,tenant_id,user_id,expires_at,heartbeat_at)
		VALUES ('instance','session','tenant-a','user',CURRENT_TIMESTAMP + interval '1 minute',CURRENT_TIMESTAMP)`)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `INSERT INTO deletion_audits
		(audit_id,tenant_id,conversation_hash,actor,reason_code)
		VALUES ('audit','tenant-a','keyed-hash','operator','retention')`)
	require.NoError(t, err)

	mutations := []struct{ name, constraint, query string }{
		{"conversation status", "conversations_status_check", `UPDATE conversations SET status='unknown'`},
		{"conversation retention", "conversations_retention_check", `UPDATE conversations SET retention_until=created_at-interval '1 second'`},
		{"conversation timestamps", "conversations_timestamps_check", `UPDATE conversations SET updated_at=created_at-interval '1 second'`},
		{"item revision", "conversation_items_turn_revision_check", `UPDATE conversation_items SET turn_revision=-1`},
		{"item sequence", "conversation_items_sequence_check", `UPDATE conversation_items SET sequence=-1`},
		{"item role", "conversation_items_role_check", `UPDATE conversation_items SET role='unknown'`},
		{"empty finalized content", "conversation_items_content_size_check", `UPDATE conversation_items SET content=''`},
		{"event state", "handoff_events_state_check", `UPDATE handoff_events SET state='unknown'`},
		{"event attempt", "handoff_events_attempt_check", `UPDATE handoff_events SET attempt=-1`},
		{"event payload type", "handoff_events_payload_check", `UPDATE handoff_events SET payload='[]'`},
		{"outbox attempts", "outbox_attempts_check", `UPDATE outbox SET attempts=-1`},
		{"outbox payload type", "outbox_payload_check", `UPDATE outbox SET payload='[]'`},
		{"outbox last error bound", "outbox_last_error_size_check", `UPDATE outbox SET last_error=repeat('x',2049)`},
		{"negative cursor", "delivery_cursors_sequence_check", `UPDATE delivery_cursors SET last_sequence=-1`},
		{"lease time order", "session_admission_leases_timestamps_check", `UPDATE session_admission_leases SET expires_at=heartbeat_at-interval '1 second'`},
		{"empty deletion hash", "deletion_audits_conversation_hash_size_check", `UPDATE deletion_audits SET conversation_hash=''`},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			_, err := pool.Exec(t.Context(), mutation.query)
			assertConstraint(t, err, mutation.constraint)
		})
	}
}

// The mutation registry is intentionally exhaustive: its keys are compared
// with PostgreSQL's complete CHECK catalog before every mutation is executed.
// Adding a CHECK without a concrete rejection test therefore fails here.
func TestSchemaEveryNamedCheckRejectsInvalidMutation(t *testing.T) {
	pool := migratedPostgres(t)
	insertConversation(t, pool, "tenant-c", "conversation-c")
	insertConversation(t, pool, "tenant-free", "conversation-free")
	require.NoError(t, insertHandoff(t, pool, "tenant-c", "conversation-c", "handoff-c", "idem-c"))
	require.NoError(t, insertHandoff(t, pool, "tenant-c", "conversation-c", "handoff-success", "idem-success"))
	require.NoError(t, insertHandoff(t, pool, "tenant-c", "conversation-c", "handoff-failed", "idem-failed"))
	_, err := pool.Exec(t.Context(), `UPDATE handoffs SET state='succeeded',terminal_mutation_id='mutation',
		result_summary='done',structured_result='{}',usage='{}',terminal_at=CURRENT_TIMESTAMP WHERE id='handoff-success'`)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `UPDATE handoffs SET state='failed',terminal_mutation_id='mutation',
		failure_code='failed',failure_retryable=false,failure_message='reason',terminal_at=CURRENT_TIMESTAMP WHERE id='handoff-failed'`)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `INSERT INTO conversation_items
		(tenant_id,conversation_id,session_id,turn_id,turn_revision,sequence,role,content,content_hash)
		VALUES ('tenant-c','conversation-c','session-c','turn-c',1,1,'user','content','hash')`)
	require.NoError(t, err)
	sequence := insertEvent(t, pool, "event-c", "tenant-c", "conversation-c", "handoff-c")
	_, err = pool.Exec(t.Context(), `INSERT INTO outbox (topic,aggregate_id,event_seq,payload)
		VALUES ('topic-c','handoff-c',$1,'{}')`, sequence)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `INSERT INTO tool_invocations
		(invocation_id,tenant_id,conversation_id,handoff_id,attempt,objective_revision,tool_ordinal,tool_name,
		 approval_state,idempotency_key,canonical_arguments_hash,arguments_ciphertext,arguments_nonce,arguments_key_id)
		VALUES ('invocation-c','tenant-c','conversation-c','handoff-c',0,0,0,'read','approved','tool-idem','hash',
		 decode(repeat('00',16),'hex'),decode(repeat('00',12),'hex'),'key')`)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `INSERT INTO delivery_cursors (consumer_id,tenant_id,conversation_id)
		VALUES ('consumer-c','tenant-c','conversation-c')`)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `INSERT INTO session_admission_leases
		(instance_id,session_id,tenant_id,user_id,expires_at,heartbeat_at)
		VALUES ('instance-c','session-c','tenant-c','user-c',CURRENT_TIMESTAMP+interval '1 minute',CURRENT_TIMESTAMP)`)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `INSERT INTO deletion_audits
		(audit_id,tenant_id,conversation_hash,actor,reason_code)
		VALUES ('audit-c','tenant-c','hash-c','actor-c','reason-c')`)
	require.NoError(t, err)
	coordinatorPayload := []byte(`{"kind":"handoff"}`)
	coordinatorHash := sha256.Sum256(coordinatorPayload)
	_, err = pool.Exec(t.Context(), `INSERT INTO coordinator_work_journal (
		work_key,kind,tenant_id,user_id,conversation_id,session_id,delivery_affinity,
		incarnation,local_sequence,payload,payload_hash,storage_sequence
	) VALUES ('check-work','handoff','tenant-c','user','conversation-c','session-c','app-c',
		1,1,$1::json,$2,NULL)`, coordinatorPayload, hex.EncodeToString(coordinatorHash[:]))
	require.NoError(t, err)

	mutations := map[string]string{
		"conversations_tenant_id_size_check":              `UPDATE conversations SET tenant_id='' WHERE tenant_id='tenant-free'`,
		"conversations_id_size_check":                     `UPDATE conversations SET id='' WHERE tenant_id='tenant-free'`,
		"conversations_user_id_size_check":                `UPDATE conversations SET user_id='' WHERE tenant_id='tenant-free'`,
		"conversations_status_check":                      `UPDATE conversations SET status='unknown' WHERE tenant_id='tenant-free'`,
		"conversations_retention_check":                   `UPDATE conversations SET retention_until=created_at-interval '1 second' WHERE tenant_id='tenant-free'`,
		"conversations_timestamps_check":                  `UPDATE conversations SET updated_at=created_at-interval '1 second' WHERE tenant_id='tenant-free'`,
		"conversation_items_tenant_id_size_check":         `UPDATE conversation_items SET tenant_id='' WHERE session_id='session-c'`,
		"conversation_items_conversation_id_size_check":   `UPDATE conversation_items SET conversation_id='' WHERE session_id='session-c'`,
		"conversation_items_session_id_size_check":        `UPDATE conversation_items SET session_id='' WHERE session_id='session-c'`,
		"conversation_items_turn_id_size_check":           `UPDATE conversation_items SET turn_id='' WHERE session_id='session-c'`,
		"conversation_items_turn_revision_check":          `UPDATE conversation_items SET turn_revision=-1 WHERE session_id='session-c'`,
		"conversation_items_sequence_check":               `UPDATE conversation_items SET sequence=-1 WHERE session_id='session-c'`,
		"conversation_items_role_check":                   `UPDATE conversation_items SET role='unknown' WHERE session_id='session-c'`,
		"conversation_items_content_size_check":           `UPDATE conversation_items SET content='' WHERE session_id='session-c'`,
		"conversation_items_content_hash_size_check":      `UPDATE conversation_items SET content_hash='' WHERE session_id='session-c'`,
		"conversation_items_timestamps_check":             `UPDATE conversation_items SET updated_at=created_at-interval '1 second' WHERE session_id='session-c'`,
		"coordinator_work_journal_kind_check":             `UPDATE coordinator_work_journal SET kind='unknown' WHERE work_key='check-work'`,
		"coordinator_work_journal_work_key_size":          `UPDATE coordinator_work_journal SET work_key='' WHERE work_key='check-work'`,
		"coordinator_work_journal_identity_size":          `UPDATE coordinator_work_journal SET delivery_affinity='' WHERE work_key='check-work'`,
		"coordinator_work_journal_incarnation_check":      `UPDATE coordinator_work_journal SET incarnation=0 WHERE work_key='check-work'`,
		"coordinator_work_journal_local_sequence_check":   `UPDATE coordinator_work_journal SET local_sequence=-1 WHERE work_key='check-work'`,
		"coordinator_work_journal_payload_size":           `UPDATE coordinator_work_journal SET payload=to_json(repeat('x',262145)) WHERE work_key='check-work'`,
		"coordinator_work_journal_payload_depth_check":    `UPDATE coordinator_work_journal SET payload=(repeat('[',65)||repeat(']',65))::json WHERE work_key='check-work'`,
		"coordinator_work_journal_payload_hash_size":      `UPDATE coordinator_work_journal SET payload_hash='' WHERE work_key='check-work'`,
		"coordinator_work_journal_storage_sequence_check": `UPDATE coordinator_work_journal SET storage_sequence=1 WHERE work_key='check-work'`,
		"coordinator_work_journal_delivery_check":         `UPDATE coordinator_work_journal SET delivered=true WHERE work_key='check-work'`,
		"coordinator_work_journal_claim_check":            `UPDATE coordinator_work_journal SET claim_owner='worker' WHERE work_key='check-work'`,
		"coordinator_work_journal_claim_owner_size":       `UPDATE coordinator_work_journal SET claim_owner='',claim_until=CURRENT_TIMESTAMP WHERE work_key='check-work'`,
		"coordinator_work_journal_claim_generation_check": `UPDATE coordinator_work_journal SET claim_generation=-1 WHERE work_key='check-work'`,
		"handoffs_tenant_id_size_check":                   `UPDATE handoffs SET tenant_id='' WHERE id='handoff-c'`,
		"handoffs_conversation_id_size_check":             `UPDATE handoffs SET conversation_id='' WHERE id='handoff-c'`,
		"handoffs_id_size_check":                          `UPDATE handoffs SET id='' WHERE id='handoff-c'`,
		"handoffs_user_id_size_check":                     `UPDATE handoffs SET user_id='' WHERE id='handoff-c'`,
		"handoffs_session_id_size_check":                  `UPDATE handoffs SET session_id='' WHERE id='handoff-c'`,
		"handoffs_source_turn_id_size_check":              `UPDATE handoffs SET source_turn_id='' WHERE id='handoff-c'`,
		"handoffs_source_turn_revision_check":             `UPDATE handoffs SET source_turn_revision=0 WHERE id='handoff-c'`,
		"handoffs_user_request_size_check":                `UPDATE handoffs SET user_request='' WHERE id='handoff-c'`,
		"handoffs_objective_size_check":                   `UPDATE handoffs SET objective='' WHERE id='handoff-c'`,
		"handoffs_context_ref_size_check":                 `UPDATE handoffs SET context_ref='' WHERE id='handoff-c'`,
		"handoffs_context_hash_size_check":                `UPDATE handoffs SET context_hash='' WHERE id='handoff-c'`,
		"handoffs_deadline_check":                         `UPDATE handoffs SET deadline=created_at WHERE id='handoff-c'`,
		"handoffs_token_budget_check":                     `UPDATE handoffs SET token_budget=0 WHERE id='handoff-c'`,
		"handoffs_priority_check":                         `UPDATE handoffs SET priority=1001 WHERE id='handoff-c'`,
		"handoffs_tool_allowlist_check":                   `UPDATE handoffs SET tool_allowlist='[1]' WHERE id='handoff-c'`,
		"handoffs_approval_policy_check":                  `UPDATE handoffs SET approval_policy=to_json(repeat('x',65535)) WHERE id='handoff-c'`,
		"handoffs_model_version_size_check":               `UPDATE handoffs SET model_version='' WHERE id='handoff-c'`,
		"handoffs_prompt_version_size_check":              `UPDATE handoffs SET prompt_version='' WHERE id='handoff-c'`,
		"handoffs_tool_registry_version_size_check":       `UPDATE handoffs SET tool_registry_version='' WHERE id='handoff-c'`,
		"handoffs_config_version_size_check":              `UPDATE handoffs SET config_version='' WHERE id='handoff-c'`,
		"handoffs_presentation_hint_size_check":           `UPDATE handoffs SET presentation_hint=repeat('x',2049) WHERE id='handoff-c'`,
		"handoffs_idempotency_key_size_check":             `UPDATE handoffs SET idempotency_key='' WHERE id='handoff-c'`,
		"handoffs_request_hash_size_check":                `UPDATE handoffs SET request_hash='' WHERE id='handoff-c'`,
		"handoffs_state_check":                            `UPDATE handoffs SET state='unknown' WHERE id='handoff-c'`,
		"handoffs_revision_check":                         `UPDATE handoffs SET objective_revision=-1 WHERE id='handoff-c'`,
		"handoffs_input_check":                            `UPDATE handoffs SET input_revision=1,input=NULL WHERE id='handoff-c'`,
		"handoffs_ready_at_check":                         `UPDATE handoffs SET ready_at=created_at-interval '1 second' WHERE id='handoff-c'`,
		"handoffs_attempt_check":                          `UPDATE handoffs SET attempt=-1 WHERE id='handoff-c'`,
		"handoffs_lease_tuple_check":                      `UPDATE handoffs SET lease_owner='worker' WHERE id='handoff-c'`,
		"handoffs_lease_owner_size_check":                 `UPDATE handoffs SET lease_owner='',lease_expires_at=CURRENT_TIMESTAMP WHERE id='handoff-c'`,
		"handoffs_cancel_tuple_check":                     `UPDATE handoffs SET cancel_actor='actor' WHERE id='handoff-c'`,
		"handoffs_cancel_actor_size_check":                `UPDATE handoffs SET cancel_requested_at=CURRENT_TIMESTAMP,cancel_actor='' WHERE id='handoff-c'`,
		"handoffs_cancel_reason_size_check":               `UPDATE handoffs SET cancel_requested_at=CURRENT_TIMESTAMP,cancel_actor='actor',cancel_reason=repeat('x',2049) WHERE id='handoff-c'`,
		"handoffs_terminal_mutation_id_size_check":        `UPDATE handoffs SET terminal_mutation_id='' WHERE id='handoff-success'`,
		"handoffs_result_summary_size_check":              `UPDATE handoffs SET result_summary='' WHERE id='handoff-success'`,
		"handoffs_structured_result_check":                `UPDATE handoffs SET structured_result=to_json(repeat('x',65535)) WHERE id='handoff-success'`,
		"handoffs_failure_code_size_check":                `UPDATE handoffs SET failure_code='' WHERE id='handoff-failed'`,
		"handoffs_failure_message_size_check":             `UPDATE handoffs SET failure_message='' WHERE id='handoff-failed'`,
		"handoffs_safety_decision_check":                  `UPDATE handoffs SET safety_decision='[]' WHERE id='handoff-c'`,
		"handoffs_usage_check":                            `UPDATE handoffs SET usage=to_json(repeat('x',65535)) WHERE id='handoff-success'`,
		"handoffs_terminal_fields_check":                  `UPDATE handoffs SET terminal_at=CURRENT_TIMESTAMP WHERE id='handoff-c'`,
		"handoffs_timestamps_check":                       `UPDATE handoffs SET updated_at=created_at-interval '1 second' WHERE id='handoff-c'`,
		"handoff_events_event_id_size_check":              `UPDATE handoff_events SET event_id='' WHERE event_id='event-c'`,
		"handoff_events_kind_size_check":                  `UPDATE handoff_events SET kind='' WHERE event_id='event-c'`,
		"handoff_events_state_check":                      `UPDATE handoff_events SET state='unknown' WHERE event_id='event-c'`,
		"handoff_events_attempt_check":                    `UPDATE handoff_events SET attempt=-1 WHERE event_id='event-c'`,
		"handoff_events_payload_check":                    `UPDATE handoff_events SET payload='[]' WHERE event_id='event-c'`,
		"outbox_topic_size_check":                         `UPDATE outbox SET topic='' WHERE event_seq=` + fmt.Sprint(sequence),
		"outbox_aggregate_id_size_check":                  `UPDATE outbox SET aggregate_id='' WHERE event_seq=` + fmt.Sprint(sequence),
		"outbox_event_seq_check":                          `UPDATE outbox SET event_seq=-1 WHERE event_seq=` + fmt.Sprint(sequence),
		"outbox_payload_check":                            `UPDATE outbox SET payload=json_build_object('tenant_id',repeat('x',8192)) WHERE event_seq=` + fmt.Sprint(sequence),
		"outbox_payload_keys_check":                       `UPDATE outbox SET payload='{"secret":"x"}' WHERE event_seq=` + fmt.Sprint(sequence),
		"outbox_attempts_check":                           `UPDATE outbox SET attempts=-1 WHERE event_seq=` + fmt.Sprint(sequence),
		"outbox_last_error_size_check":                    `UPDATE outbox SET last_error=repeat('x',2049) WHERE event_seq=` + fmt.Sprint(sequence),
		"outbox_timestamps_check":                         `UPDATE outbox SET published_at=created_at-interval '1 second' WHERE event_seq=` + fmt.Sprint(sequence),
		"tool_invocations_invocation_id_size_check":       `UPDATE tool_invocations SET invocation_id='' WHERE invocation_id='invocation-c'`,
		"tool_invocations_attempt_check":                  `UPDATE tool_invocations SET attempt=-1 WHERE invocation_id='invocation-c'`,
		"tool_invocations_objective_revision_check":       `UPDATE tool_invocations SET objective_revision=-1 WHERE invocation_id='invocation-c'`,
		"tool_invocations_tool_ordinal_check":             `UPDATE tool_invocations SET tool_ordinal=-1 WHERE invocation_id='invocation-c'`,
		"tool_invocations_tool_name_size_check":           `UPDATE tool_invocations SET tool_name='' WHERE invocation_id='invocation-c'`,
		"tool_invocations_approval_state_check":           `UPDATE tool_invocations SET approval_state='unknown' WHERE invocation_id='invocation-c'`,
		"tool_invocations_idempotency_key_size_check":     `UPDATE tool_invocations SET idempotency_key='' WHERE invocation_id='invocation-c'`,
		"tool_invocations_arguments_hash_size_check":      `UPDATE tool_invocations SET canonical_arguments_hash='' WHERE invocation_id='invocation-c'`,
		"tool_invocations_ciphertext_check":               `UPDATE tool_invocations SET arguments_ciphertext=decode(repeat('00',15),'hex') WHERE invocation_id='invocation-c'`,
		"tool_invocations_nonce_check":                    `UPDATE tool_invocations SET arguments_nonce=decode(repeat('00',11),'hex') WHERE invocation_id='invocation-c'`,
		"tool_invocations_key_id_size_check":              `UPDATE tool_invocations SET arguments_key_id='' WHERE invocation_id='invocation-c'`,
		"tool_invocations_result_metadata_check":          `UPDATE tool_invocations SET result_metadata='[]' WHERE invocation_id='invocation-c'`,
		"tool_invocations_result_check":                   `UPDATE tool_invocations SET result=to_json(repeat('x',65535)) WHERE invocation_id='invocation-c'`,
		"tool_invocations_mutation_id_size_check":         `UPDATE tool_invocations SET mutation_id='' WHERE invocation_id='invocation-c'`,
		"tool_invocations_status_check":                   `UPDATE tool_invocations SET status='unknown' WHERE invocation_id='invocation-c'`,
		"tool_invocations_timestamps_check":               `UPDATE tool_invocations SET updated_at=created_at-interval '1 second' WHERE invocation_id='invocation-c'`,
		"delivery_cursors_consumer_id_size_check":         `UPDATE delivery_cursors SET consumer_id='' WHERE consumer_id='consumer-c'`,
		"delivery_cursors_sequence_check":                 `UPDATE delivery_cursors SET last_sequence=-1 WHERE consumer_id='consumer-c'`,
		"delivery_cursors_timestamps_check":               `UPDATE delivery_cursors SET updated_at=created_at-interval '1 second' WHERE consumer_id='consumer-c'`,
		"session_admission_leases_instance_id_size_check": `UPDATE session_admission_leases SET instance_id='' WHERE instance_id='instance-c'`,
		"session_admission_leases_session_id_size_check":  `UPDATE session_admission_leases SET session_id='' WHERE instance_id='instance-c'`,
		"session_admission_leases_tenant_id_size_check":   `UPDATE session_admission_leases SET tenant_id='' WHERE instance_id='instance-c'`,
		"session_admission_leases_user_id_size_check":     `UPDATE session_admission_leases SET user_id='' WHERE instance_id='instance-c'`,
		"session_admission_leases_timestamps_check":       `UPDATE session_admission_leases SET heartbeat_at=created_at-interval '1 second' WHERE instance_id='instance-c'`,
		"deletion_audits_audit_id_size_check":             `UPDATE deletion_audits SET audit_id='' WHERE audit_id='audit-c'`,
		"deletion_audits_tenant_id_size_check":            `UPDATE deletion_audits SET tenant_id='' WHERE audit_id='audit-c'`,
		"deletion_audits_conversation_hash_size_check":    `UPDATE deletion_audits SET conversation_hash='' WHERE audit_id='audit-c'`,
		"deletion_audits_actor_size_check":                `UPDATE deletion_audits SET actor='' WHERE audit_id='audit-c'`,
		"deletion_audits_reason_code_size_check":          `UPDATE deletion_audits SET reason_code='' WHERE audit_id='audit-c'`,
	}

	rows, err := pool.Query(t.Context(), `
		SELECT conname FROM pg_constraint
		WHERE connamespace=(SELECT oid FROM pg_namespace WHERE nspname=current_schema()) AND contype='c'
		ORDER BY conname`)
	require.NoError(t, err)
	var catalogChecks []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		catalogChecks = append(catalogChecks, name)
	}
	require.NoError(t, rows.Err())
	rows.Close()
	mutationNames := make([]string, 0, len(mutations))
	for name := range mutations {
		mutationNames = append(mutationNames, name)
	}
	slices.Sort(mutationNames)
	require.Equal(t, catalogChecks, mutationNames, "every catalog CHECK needs exactly one invalid mutation")
	for _, name := range mutationNames {
		t.Run(name, func(t *testing.T) {
			_, err := pool.Exec(t.Context(), mutations[name])
			assertConstraint(t, err, name)
		})
	}
}

// Weakening any composite tenant key allows a child row to cross a tenant
// boundary; globally unique IDs must still permit equal local IDs per tenant.
func TestSchemaTenantBoundariesAndGlobalEventSequence(t *testing.T) {
	pool := migratedPostgres(t)
	insertConversation(t, pool, "tenant-a", "shared-conversation")
	insertConversation(t, pool, "tenant-b", "shared-conversation")
	insertConversation(t, pool, "tenant-b", "tenant-b-only")
	require.NoError(t, insertHandoff(t, pool, "tenant-a", "shared-conversation", "shared-handoff", "idem-a"))
	require.NoError(t, insertHandoff(t, pool, "tenant-b", "shared-conversation", "shared-handoff", "idem-b"))

	_, err := pool.Exec(t.Context(), `INSERT INTO conversation_items
		(tenant_id,conversation_id,session_id,turn_id,turn_revision,sequence,role,content,content_hash)
		VALUES ('tenant-a','tenant-b-only','s','t',1,1,'user','x','h')`)
	assertConstraint(t, err, "conversation_items_conversation_fkey")
	assertConstraint(t, insertHandoff(t, pool, "tenant-a", "tenant-b-only", "cross", "cross"), "handoffs_conversation_fkey")

	first := insertEvent(t, pool, "event-a", "tenant-a", "shared-conversation", "shared-handoff")
	second := insertEvent(t, pool, "event-b", "tenant-b", "shared-conversation", "shared-handoff")
	assert.Greater(t, second, first)
	_, err = pool.Exec(t.Context(), `INSERT INTO handoff_events
		(event_id,tenant_id,conversation_id,handoff_id,kind,state,payload)
		VALUES ('cross-event','tenant-a','shared-conversation','missing-handoff','created','pending','{}')`)
	assertConstraint(t, err, "handoff_events_handoff_fkey")
	_, err = pool.Exec(t.Context(), `INSERT INTO handoff_events
		(event_id,tenant_id,conversation_id,handoff_id,kind,state,payload)
		VALUES ('event-a','tenant-b','shared-conversation','shared-handoff','created','pending','{}')`)
	assertConstraint(t, err, "handoff_events_event_id_uniq")

	_, err = pool.Exec(t.Context(), `INSERT INTO delivery_cursors (consumer_id,tenant_id,conversation_id)
		VALUES ('consumer','tenant-a','tenant-b-only')`)
	assertConstraint(t, err, "delivery_cursors_conversation_fkey")
	_, err = pool.Exec(t.Context(), `INSERT INTO tool_invocations
		(invocation_id,tenant_id,conversation_id,handoff_id,attempt,objective_revision,tool_ordinal,tool_name,
		 approval_state,idempotency_key,canonical_arguments_hash,arguments_ciphertext,arguments_nonce,arguments_key_id)
		VALUES ('invocation-cross','tenant-a','shared-conversation','missing-handoff',0,0,0,'read','approved','tool-idem','hash',decode(repeat('01',16),'hex'),decode('000000000000000000000000','hex'),'key')`)
	assertConstraint(t, err, "tool_invocations_handoff_fkey")

	_, err = pool.Exec(t.Context(), `INSERT INTO outbox (topic,aggregate_id,event_seq,payload)
		VALUES ('handoff.event','shared-handoff',$1,'{"tenant_id":"tenant-a","conversation_id":"shared-conversation","highest_seq":1}')`, first)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `INSERT INTO outbox (topic,aggregate_id,event_seq,payload)
		VALUES ('handoff.event','shared-handoff',$1,'{}')`, first)
	assertConstraint(t, err, "outbox_topic_event_seq_uniq")
}

// JSON caps are byte boundaries, not character counts, and lease fencing must
// reject partial tuples while allowing a cleared tuple with a retained version.
func TestSchemaJSONValueBoundariesAndLeaseVersion(t *testing.T) {
	pool := migratedPostgres(t)
	insertConversation(t, pool, "tenant-a", "conversation-a")
	require.NoError(t, insertHandoff(t, pool, "tenant-a", "conversation-a", "handoff-a", "idem-a"))
	require.NoError(t, insertHandoff(t, pool, "tenant-a", "conversation-a", "handoff-success-scalar", "idem-success-scalar"))
	require.NoError(t, insertHandoff(t, pool, "tenant-a", "conversation-a", "handoff-success-object", "idem-success-object"))

	scalarAtLimit := `"` + strings.Repeat("x", 65534) + `"`
	scalarOverLimit := `"` + strings.Repeat("x", 65535) + `"`
	objectAtLimit := `{"v":"` + strings.Repeat("x", 65528) + `"}`
	objectOverLimit := `{"v":"` + strings.Repeat("x", 65529) + `"}`
	require.Len(t, scalarAtLimit, 65536)
	require.Len(t, objectAtLimit, 65536)
	highKeyCount := highKeyJSON(3400)
	require.LessOrEqual(t, len(highKeyCount), 65536)
	for _, value := range []string{`null`, `true`, `42`, `[]`, `[1,"x"]`} {
		_, err := pool.Exec(t.Context(), `UPDATE handoffs SET approval_policy=$1 WHERE id='handoff-a'`, value)
		require.NoError(t, err, "approval policy must accept Task 8 JSON value %s", value)
	}
	validAllowlist := toolNamesJSON(64)
	_, err := pool.Exec(t.Context(), `UPDATE handoffs SET tool_allowlist=$1 WHERE id='handoff-a'`, validAllowlist)
	require.NoError(t, err)
	for name, value := range map[string]string{
		"too many names":            toolNamesJSON(65),
		"blank name":                `[" "]`,
		"tab-only name":             `["\t"]`,
		"unicode blank":             `["\u00a0"]`,
		"oversize name":             `[` + `"` + strings.Repeat("n", 257) + `"` + `]`,
		"oversize NUL":              `["` + strings.Repeat(`\u0000`, 257) + `"]`,
		"non-string":                `[1]`,
		"duplicate":                 `["same","same"]`,
		"duplicate NUL":             `["\u0000","\u0000"]`,
		"equivalent ASCII escape":   `["a","\u0061"]`,
		"equivalent slash escape":   `["/","\/"]`,
		"equivalent Unicode escape": `["é","\u00e9"]`,
		"equivalent escape case":    `["\u00e9","\u00E9"]`,
		"equivalent surrogate pair": `["😀","\ud83d\ude00"]`,
		"equivalent control escape": `["\n","\u000a"]`,
	} {
		t.Run("tool allowlist "+name, func(t *testing.T) {
			_, err := pool.Exec(t.Context(), `UPDATE handoffs SET tool_allowlist=$1 WHERE id='handoff-a'`, value)
			assertConstraint(t, err, "handoffs_tool_allowlist_check")
		})
	}
	_, err = pool.Exec(t.Context(), `UPDATE handoffs SET tool_allowlist=$1 WHERE id='handoff-a'`, `["`+strings.Repeat("n", 256)+`"]`)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `UPDATE handoffs SET tool_allowlist=$1 WHERE id='handoff-a'`, `["`+strings.Repeat(`\u0000`, 256)+`"]`)
	require.NoError(t, err)
	maxTask8Allowlist := nulPrefixedToolNamesJSON(64, 254)
	require.Len(t, maxTask8Allowlist, 97857)
	_, err = pool.Exec(t.Context(), `UPDATE handoffs SET tool_allowlist=$1 WHERE id='handoff-a'`, maxTask8Allowlist)
	require.NoError(t, err)
	var storedMaxAllowlist []byte
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT tool_allowlist FROM handoffs WHERE id='handoff-a'`).Scan(&storedMaxAllowlist))
	assert.Equal(t, maxTask8Allowlist, string(storedMaxAllowlist))

	_, err = pool.Exec(t.Context(), `UPDATE handoffs SET approval_policy=$1 WHERE id='handoff-a'`, scalarAtLimit)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `UPDATE handoffs SET approval_policy=$1 WHERE id='handoff-a'`, highKeyCount)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `UPDATE handoffs SET approval_policy=$1 WHERE id='handoff-a'`, scalarOverLimit)
	assertConstraint(t, err, "handoffs_approval_policy_check")
	_, err = pool.Exec(t.Context(), `UPDATE handoffs SET approval_policy=$1 WHERE id='handoff-a'`, `"`+strings.Repeat(`\u003c`, 11000)+`"`)
	assertConstraint(t, err, "handoffs_approval_policy_check")

	_, err = pool.Exec(t.Context(), `UPDATE handoffs SET input_revision=1,input=$1 WHERE id='handoff-a'`, scalarAtLimit)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `UPDATE handoffs SET input=$1 WHERE id='handoff-a'`, scalarOverLimit)
	assertConstraint(t, err, "handoffs_input_check")
	_, err = pool.Exec(t.Context(), `UPDATE handoffs SET safety_decision=$1 WHERE id='handoff-a'`, objectAtLimit)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `UPDATE handoffs SET safety_decision=$1 WHERE id='handoff-a'`, objectOverLimit)
	assertConstraint(t, err, "handoffs_safety_decision_check")

	_, err = pool.Exec(t.Context(), `UPDATE handoffs SET
		state='succeeded',terminal_mutation_id='mutation-scalar',result_summary='done',
		structured_result=$1,usage=$1,terminal_at=CURRENT_TIMESTAMP
		WHERE id='handoff-success-scalar'`, scalarAtLimit)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `UPDATE handoffs SET
		state='succeeded',terminal_mutation_id='mutation-object',result_summary='done',
		structured_result=$1,usage=$1,terminal_at=CURRENT_TIMESTAMP
		WHERE id='handoff-success-object'`, highKeyCount)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `UPDATE handoffs SET structured_result=$1 WHERE id='handoff-success-scalar'`, scalarOverLimit)
	assertConstraint(t, err, "handoffs_structured_result_check")
	_, err = pool.Exec(t.Context(), `UPDATE handoffs SET usage=$1 WHERE id='handoff-success-scalar'`, scalarOverLimit)
	assertConstraint(t, err, "handoffs_usage_check")

	_, err = pool.Exec(t.Context(), `UPDATE handoffs SET lease_owner='worker',lease_expires_at=CURRENT_TIMESTAMP + interval '1 minute',lease_version=7 WHERE id='handoff-a'`)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `UPDATE handoffs SET lease_owner=NULL,lease_expires_at=NULL WHERE id='handoff-a'`)
	require.NoError(t, err)
	var leaseVersion int64
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT lease_version FROM handoffs WHERE id='handoff-a'`).Scan(&leaseVersion))
	assert.Equal(t, int64(7), leaseVersion)

	_, err = pool.Exec(t.Context(), `INSERT INTO handoff_events
		(event_id,tenant_id,conversation_id,handoff_id,kind,state,payload)
		VALUES ('event-limit','tenant-a','conversation-a','handoff-a','progress','running',$1)`, highKeyCount)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `INSERT INTO handoff_events
		(event_id,tenant_id,conversation_id,handoff_id,kind,state,payload)
		VALUES ('event-exact-limit','tenant-a','conversation-a','handoff-a','progress','running',$1)`, objectAtLimit)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `INSERT INTO handoff_events
		(event_id,tenant_id,conversation_id,handoff_id,kind,state,payload)
		VALUES ('event-over','tenant-a','conversation-a','handoff-a','progress','running',$1)`, `{"v":`+scalarOverLimit+`}`)
	assertConstraint(t, err, "handoff_events_payload_check")

	_, err = pool.Exec(t.Context(), `INSERT INTO tool_invocations
		(invocation_id,tenant_id,conversation_id,handoff_id,attempt,objective_revision,tool_ordinal,tool_name,
		 approval_state,idempotency_key,canonical_arguments_hash,arguments_ciphertext,arguments_nonce,arguments_key_id)
		VALUES ('invocation-json','tenant-a','conversation-a','handoff-a',0,0,0,'read','approved','tool-json','hash-json',
		 decode(repeat('00',16),'hex'),decode(repeat('00',12),'hex'),'key-json')`)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `UPDATE tool_invocations SET result=$1,result_metadata=$2 WHERE invocation_id='invocation-json'`, scalarAtLimit, objectAtLimit)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `UPDATE tool_invocations SET result=$1 WHERE invocation_id='invocation-json'`, scalarOverLimit)
	assertConstraint(t, err, "tool_invocations_result_check")
	_, err = pool.Exec(t.Context(), `UPDATE tool_invocations SET result_metadata=$1 WHERE invocation_id='invocation-json'`, objectOverLimit)
	assertConstraint(t, err, "tool_invocations_result_metadata_check")
}

// Task 8 accepts canonical JSON lexemes that binary-normalizing storage cannot
// represent. Every durable JSON path must preserve those bytes as PostgreSQL
// JSON, including escaped NUL strings and numbers beyond numeric's range.
func TestSchemaTask8JSONDomainAcrossEveryPath(t *testing.T) {
	pool := migratedPostgres(t)
	insertConversation(t, pool, "tenant-json-domain", "conversation-json-domain")
	require.NoError(t, insertHandoff(t, pool, "tenant-json-domain", "conversation-json-domain", "handoff-json-domain", "idem-json-domain"))

	highPrecisionNumber := `0.` + strings.Repeat("1", 20000)
	document := `{"nul":"\u0000","number":` + highPrecisionNumber + `}`
	allowlist := `["\u0000"]`
	_, err := pool.Exec(t.Context(), `UPDATE handoffs SET
		tool_allowlist=$1,approval_policy=$2,input_revision=1,input=$2,
		state='succeeded',terminal_mutation_id='mutation-json-domain',result_summary='done',
		structured_result=$2,safety_decision=$2,usage=$2,terminal_at=CURRENT_TIMESTAMP
		WHERE id='handoff-json-domain'`, allowlist, document)
	require.NoError(t, err)
	var storedAllowlist, storedApproval, storedInput, storedStructured, storedSafety, storedUsage []byte
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT
		tool_allowlist,approval_policy,input,structured_result,safety_decision,usage
		FROM handoffs WHERE id='handoff-json-domain'`).Scan(
		&storedAllowlist, &storedApproval, &storedInput, &storedStructured, &storedSafety, &storedUsage))
	assert.Equal(t, allowlist, string(storedAllowlist))
	for name, stored := range map[string][]byte{
		"approval_policy":   storedApproval,
		"input":             storedInput,
		"structured_result": storedStructured,
		"safety_decision":   storedSafety,
		"usage":             storedUsage,
	} {
		assert.Equal(t, document, string(stored), name)
	}

	var eventSequence int64
	err = pool.QueryRow(t.Context(), `INSERT INTO handoff_events
		(event_id,tenant_id,conversation_id,handoff_id,kind,state,payload)
		VALUES ('event-json-domain','tenant-json-domain','conversation-json-domain','handoff-json-domain','completed','succeeded',$1)
		RETURNING seq`, document).Scan(&eventSequence)
	require.NoError(t, err)
	var storedEvent []byte
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT payload FROM handoff_events WHERE seq=$1`, eventSequence).Scan(&storedEvent))
	assert.Equal(t, document, string(storedEvent))

	outboxDocument := `{"tenant_id":"\u0000","highest_seq":9223372036854775807}`
	_, err = pool.Exec(t.Context(), `INSERT INTO outbox (topic,aggregate_id,event_seq,payload)
		VALUES ('json-domain','handoff-json-domain',$1,$2)`, eventSequence, outboxDocument)
	require.NoError(t, err)
	var storedOutbox []byte
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT payload FROM outbox WHERE topic='json-domain'`).Scan(&storedOutbox))
	assert.Equal(t, outboxDocument, string(storedOutbox))

	_, err = pool.Exec(t.Context(), `INSERT INTO tool_invocations
		(invocation_id,tenant_id,conversation_id,handoff_id,attempt,objective_revision,tool_ordinal,tool_name,
		 approval_state,idempotency_key,canonical_arguments_hash,arguments_ciphertext,arguments_nonce,arguments_key_id,
		 result_metadata,result)
		VALUES ('invocation-json-domain','tenant-json-domain','conversation-json-domain','handoff-json-domain',0,0,0,
		 'read','approved','tool-json-domain','hash-json-domain',decode(repeat('00',16),'hex'),
		 decode(repeat('00',12),'hex'),'key-json-domain',$1,$1)`, document)
	require.NoError(t, err)
	var storedMetadata, storedResult []byte
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT result_metadata,result FROM tool_invocations
		WHERE invocation_id='invocation-json-domain'`).Scan(&storedMetadata, &storedResult))
	assert.Equal(t, document, string(storedMetadata))
	assert.Equal(t, document, string(storedResult))
}

func TestSchemaTask8JSONDepthAcrossEveryPath(t *testing.T) {
	pool := migratedPostgres(t)
	insertConversation(t, pool, "tenant-json-depth", "conversation-json-depth")
	require.NoError(t, insertHandoff(t, pool, "tenant-json-depth", "conversation-json-depth", "handoff-json-depth", "idem-json-depth"))
	require.NoError(t, insertHandoff(t, pool, "tenant-json-depth", "conversation-json-depth", "handoff-json-depth-result", "idem-json-depth-result"))

	validDocument := strings.Repeat("[", 63) + "0" + strings.Repeat("]", 63)
	invalidDocument := strings.Repeat("[", 64) + "0" + strings.Repeat("]", 64)
	validObject := `{"v":` + strings.Repeat("[", 62) + "0" + strings.Repeat("]", 62) + `}`
	invalidObject := `{"v":` + strings.Repeat("[", 63) + "0" + strings.Repeat("]", 63) + `}`
	validOutbox := `{"tenant_id":"tenant-json-depth","highest_seq":9223372036854775807}`
	invalidOutbox := `{"tenant_id":[0]}`

	_, err := pool.Exec(t.Context(), `UPDATE handoffs SET
		tool_allowlist='["read"]',approval_policy=$1,input_revision=1,input=$1,safety_decision=$2
		WHERE id='handoff-json-depth'`, validDocument, validObject)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `UPDATE handoffs SET
		state='succeeded',terminal_mutation_id='mutation-json-depth',result_summary='done',
		structured_result=$1,usage=$1,terminal_at=CURRENT_TIMESTAMP
		WHERE id='handoff-json-depth-result'`, validDocument)
	require.NoError(t, err)
	var eventSequence int64
	require.NoError(t, pool.QueryRow(t.Context(), `INSERT INTO handoff_events
		(event_id,tenant_id,conversation_id,handoff_id,kind,state,payload)
		VALUES ('event-json-depth','tenant-json-depth','conversation-json-depth','handoff-json-depth','progress','running',$1)
		RETURNING seq`, validObject).Scan(&eventSequence))
	_, err = pool.Exec(t.Context(), `INSERT INTO outbox (topic,aggregate_id,event_seq,payload)
		VALUES ('json-depth','handoff-json-depth',$1,$2)`, eventSequence, validOutbox)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `INSERT INTO tool_invocations
		(invocation_id,tenant_id,conversation_id,handoff_id,attempt,objective_revision,tool_ordinal,tool_name,
		 approval_state,idempotency_key,canonical_arguments_hash,arguments_ciphertext,arguments_nonce,arguments_key_id,
		 result_metadata,result)
		VALUES ('invocation-json-depth','tenant-json-depth','conversation-json-depth','handoff-json-depth',0,0,0,
		 'read','approved','tool-json-depth','hash-json-depth',decode(repeat('00',16),'hex'),
		 decode(repeat('00',12),'hex'),'key-json-depth',$1,$2)`, validObject, validDocument)
	require.NoError(t, err)

	mutations := []struct {
		name       string
		constraint string
		query      string
		argument   string
	}{
		{"tool allowlist shape", "handoffs_tool_allowlist_check", `UPDATE handoffs SET tool_allowlist=$1 WHERE id='handoff-json-depth'`, `[["read"]]`},
		{"approval policy", "handoffs_approval_policy_check", `UPDATE handoffs SET approval_policy=$1 WHERE id='handoff-json-depth'`, invalidDocument},
		{"input", "handoffs_input_check", `UPDATE handoffs SET input=$1 WHERE id='handoff-json-depth'`, invalidDocument},
		{"structured result", "handoffs_structured_result_check", `UPDATE handoffs SET structured_result=$1 WHERE id='handoff-json-depth-result'`, invalidDocument},
		{"safety decision", "handoffs_safety_decision_check", `UPDATE handoffs SET safety_decision=$1 WHERE id='handoff-json-depth'`, invalidObject},
		{"usage", "handoffs_usage_check", `UPDATE handoffs SET usage=$1 WHERE id='handoff-json-depth-result'`, invalidDocument},
		{"event payload", "handoff_events_payload_check", `UPDATE handoff_events SET payload=$1 WHERE event_id='event-json-depth'`, invalidObject},
		{"outbox payload shape", "outbox_payload_keys_check", `UPDATE outbox SET payload=$1 WHERE topic='json-depth'`, invalidOutbox},
		{"tool metadata", "tool_invocations_result_metadata_check", `UPDATE tool_invocations SET result_metadata=$1 WHERE invocation_id='invocation-json-depth'`, invalidObject},
		{"tool result", "tool_invocations_result_check", `UPDATE tool_invocations SET result=$1 WHERE invocation_id='invocation-json-depth'`, invalidDocument},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			_, err := pool.Exec(t.Context(), mutation.query, mutation.argument)
			assertConstraint(t, err, mutation.constraint)
		})
	}
}

// Adding transcript/result bodies to wakeup payloads, storing unusable AES-GCM
// material, or replaying a stable invocation under a later attempt must fail.
func TestSchemaOutboxPrivacyAndToolReplay(t *testing.T) {
	pool := migratedPostgres(t)
	insertConversation(t, pool, "tenant-a", "conversation-a")
	require.NoError(t, insertHandoff(t, pool, "tenant-a", "conversation-a", "handoff-a", "idem-a"))
	sequence := insertEvent(t, pool, "event-a", "tenant-a", "conversation-a", "handoff-a")
	validRouting := `{"tenant_id":"` + strings.Repeat("x", 256) + `","conversation_id":"conversation-a","handoff_id":"handoff-a","event_seq":9223372036854775807,"highest_seq":9223372036854775807}`
	_, err := pool.Exec(t.Context(), `INSERT INTO outbox (topic,aggregate_id,event_seq,payload)
		VALUES ('boundary','handoff-a',$1,$2)`, sequence, validRouting)
	require.NoError(t, err)
	for _, key := range []string{"tenant_id", "conversation_id", "handoff_id", "event_seq", "highest_seq"} {
		for _, nested := range []string{`{"transcript":"secret body"}`, `["secret body"]`} {
			t.Run("nested "+key+" "+nested[:1], func(t *testing.T) {
				payload := fmt.Sprintf(`{"%s":%s}`, key, nested)
				_, err := pool.Exec(t.Context(), `UPDATE outbox SET payload=$1 WHERE topic='boundary'`, payload)
				assertConstraint(t, err, "outbox_payload_keys_check")
			})
		}
	}
	wrongRouting := map[string]string{
		"tenant null":          `{"tenant_id":null}`,
		"conversation boolean": `{"conversation_id":true}`,
		"handoff number":       `{"handoff_id":1}`,
		"identity empty":       `{"tenant_id":""}`,
		"identity blank":       `{"tenant_id":"\u00a0"}`,
		"identity oversized":   `{"tenant_id":"` + strings.Repeat("x", 257) + `"}`,
		"event string":         `{"event_seq":"1"}`,
		"event negative":       `{"event_seq":-1}`,
		"event fraction":       `{"event_seq":1.0}`,
		"event exponent":       `{"event_seq":1e0}`,
		"highest boolean":      `{"highest_seq":false}`,
		"highest null":         `{"highest_seq":null}`,
		"highest overflow":     `{"highest_seq":9223372036854775808}`,
	}
	for name, payload := range wrongRouting {
		t.Run(name, func(t *testing.T) {
			_, err := pool.Exec(t.Context(), `UPDATE outbox SET payload=$1 WHERE topic='boundary'`, payload)
			assertConstraint(t, err, "outbox_payload_keys_check")
		})
	}

	_, err = pool.Exec(t.Context(), `INSERT INTO outbox (topic,aggregate_id,event_seq,payload)
		VALUES ('handoff.event','handoff-a',$1,'{"tenant_id":"tenant-a","conversation_id":"conversation-a","highest_seq":1,"transcript":"secret"}')`, sequence)
	assertConstraint(t, err, "outbox_payload_keys_check")

	_, err = pool.Exec(t.Context(), `INSERT INTO tool_invocations
		(invocation_id,tenant_id,conversation_id,handoff_id,attempt,objective_revision,tool_ordinal,tool_name,
		 approval_state,idempotency_key,canonical_arguments_hash,arguments_ciphertext,arguments_nonce,arguments_key_id)
		VALUES ('invocation-a','tenant-a','conversation-a','handoff-a',1,2,3,'read','approved','tool-idem-a','arguments-hash',
		 decode(repeat('00',16),'hex'),decode('000000000000000000000000','hex'),'key-a')`)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `INSERT INTO tool_invocations
		(invocation_id,tenant_id,conversation_id,handoff_id,attempt,objective_revision,tool_ordinal,tool_name,
		 approval_state,idempotency_key,canonical_arguments_hash,arguments_ciphertext,arguments_nonce,arguments_key_id)
		VALUES ('invocation-b','tenant-a','conversation-a','handoff-a',2,2,3,'read','approved','tool-idem-b','arguments-hash',
		 decode(repeat('11',16),'hex'),decode('111111111111111111111111','hex'),'key-b')`)
	assertConstraint(t, err, "tool_invocations_stable_uniq")
	_, err = pool.Exec(t.Context(), `INSERT INTO tool_invocations
		(invocation_id,tenant_id,conversation_id,handoff_id,attempt,objective_revision,tool_ordinal,tool_name,
		 approval_state,idempotency_key,canonical_arguments_hash,arguments_ciphertext,arguments_nonce,arguments_key_id)
		VALUES ('invocation-c','tenant-a','conversation-a','handoff-a',2,3,3,'read','approved','tool-idem-a','different-hash',
		 decode(repeat('22',16),'hex'),decode('222222222222222222222222','hex'),'key-c')`)
	assertConstraint(t, err, "tool_invocations_idempotency_uniq")
	_, err = pool.Exec(t.Context(), `UPDATE tool_invocations SET arguments_nonce=decode('00','hex') WHERE invocation_id='invocation-a'`)
	assertConstraint(t, err, "tool_invocations_nonce_check")
	_, err = pool.Exec(t.Context(), `UPDATE tool_invocations SET arguments_ciphertext=decode(repeat('00',15),'hex') WHERE invocation_id='invocation-a'`)
	assertConstraint(t, err, "tool_invocations_ciphertext_check")
	_, err = pool.Exec(t.Context(), `UPDATE tool_invocations SET arguments_ciphertext=decode(repeat('00',16),'hex') WHERE invocation_id='invocation-a'`)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `UPDATE tool_invocations SET arguments_ciphertext=decode(repeat('00',65552),'hex') WHERE invocation_id='invocation-a'`)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `UPDATE tool_invocations SET arguments_ciphertext=decode(repeat('00',65553),'hex') WHERE invocation_id='invocation-a'`)
	assertConstraint(t, err, "tool_invocations_ciphertext_check")
	_, err = pool.Exec(t.Context(), `UPDATE tool_invocations SET approval_state='bypassed' WHERE invocation_id='invocation-a'`)
	assertConstraint(t, err, "tool_invocations_approval_state_check")
	_, err = pool.Exec(t.Context(), `UPDATE tool_invocations SET status='unknown' WHERE invocation_id='invocation-a'`)
	assertConstraint(t, err, "tool_invocations_status_check")
}

// Terminal states require their state-specific fields, and a terminal time
// must never mask an invalid updated_at ordering.
func TestSchemaTerminalFieldCoherence(t *testing.T) {
	pool := migratedPostgres(t)
	insertConversation(t, pool, "tenant-a", "conversation-a")
	require.NoError(t, insertHandoff(t, pool, "tenant-a", "conversation-a", "handoff-success", "idem-success"))
	require.NoError(t, insertHandoff(t, pool, "tenant-a", "conversation-a", "handoff-failed", "idem-failed"))

	_, err := pool.Exec(t.Context(), `UPDATE handoffs SET
		state='succeeded',terminal_mutation_id='mutation-success',result_summary='done',
		structured_result='{}',usage='{}',terminal_at=CURRENT_TIMESTAMP
		WHERE id='handoff-success'`)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `UPDATE handoffs SET terminal_at=NULL WHERE id='handoff-success'`)
	assertConstraint(t, err, "handoffs_terminal_fields_check")
	_, err = pool.Exec(t.Context(), `UPDATE handoffs SET updated_at=created_at-interval '1 second' WHERE id='handoff-success'`)
	assertConstraint(t, err, "handoffs_timestamps_check")

	_, err = pool.Exec(t.Context(), `UPDATE handoffs SET
		state='failed',terminal_mutation_id='mutation-failed',failure_code='provider',
		failure_retryable=true,failure_message='unavailable',terminal_at=CURRENT_TIMESTAMP
		WHERE id='handoff-failed'`)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `UPDATE handoffs SET failure_message=NULL WHERE id='handoff-failed'`)
	assertConstraint(t, err, "handoffs_terminal_fields_check")
}

// Changing an index key order or predicate must fail catalog inspection, and
// the 10k-row workload must use each intended partial index without a seqscan.
func TestSchemaRequiredIndexesAndPlans(t *testing.T) {
	pool := migratedPostgres(t)
	type expectedIndex struct {
		keys      []string
		options   []int16
		predicate string
	}
	expected := map[string]expectedIndex{
		"handoffs_runnable_idx": {
			keys:      []string{"priority", "ready_at", "created_at"},
			options:   []int16{3, 0, 0},
			predicate: "((state = ANY (ARRAY['pending'::text, 'running'::text])) AND (lease_owner IS NULL))",
		},
		"handoffs_expired_lease_idx": {
			keys: []string{"lease_expires_at"}, options: []int16{0}, predicate: "(lease_owner IS NOT NULL)",
		},
		"handoffs_supplied_input_idx": {
			keys: []string{"updated_at"}, options: []int16{0}, predicate: "((state = 'needs_input'::text) AND (input_revision > consumed_input_revision))",
		},
		"handoff_events_cursor_idx": {
			keys: []string{"tenant_id", "conversation_id", "seq"}, options: []int16{0, 0, 0},
		},
		"outbox_unpublished_idx": {
			keys: []string{"created_at"}, options: []int16{0}, predicate: "(published_at IS NULL)",
		},
		"outbox_event_seq_idx": {
			keys: []string{"event_seq"}, options: []int16{0},
		},
		"session_admission_leases_active_tenant_user_idx": {
			keys: []string{"tenant_id", "user_id", "expires_at"}, options: []int16{0, 0, 0},
		},
	}
	for name, want := range expected {
		var keyCount, attributeCount int
		var keys []string
		var options []int16
		var predicate string
		require.NoError(t, pool.QueryRow(t.Context(), `
			SELECT indnkeyatts, indnatts,
				ARRAY(SELECT pg_get_indexdef(indexrelid, position, false)
					FROM generate_series(1, indnatts) AS positions(position) ORDER BY position),
				indoption::int2[],
				COALESCE(pg_get_expr(indpred, indrelid), '')
			FROM pg_index WHERE indexrelid = to_regclass($1)`, name).
			Scan(&keyCount, &attributeCount, &keys, &options, &predicate), name)
		assert.Equal(t, len(want.keys), keyCount, "%s key count", name)
		assert.Equal(t, keyCount, attributeCount, "%s must not gain unreviewed INCLUDE columns", name)
		assert.Equal(t, want.keys, keys, "%s ordered key expressions", name)
		assert.Equal(t, want.options, options, "%s ordering/null-position options", name)
		assert.Equal(t, want.predicate, predicate, "%s normalized predicate", name)
	}

	insertConversation(t, pool, "tenant-plan", "conversation-plan")
	_, err := pool.Exec(t.Context(), `INSERT INTO handoffs (
		tenant_id,conversation_id,id,user_id,session_id,source_turn_id,source_turn_revision,
		user_request,objective,context_ref,context_hash,deadline,token_budget,priority,
		tool_allowlist,approval_policy,model_version,prompt_version,tool_registry_version,config_version,
		idempotency_key,request_hash,state,input_revision,consumed_input_revision,input,ready_at,
		lease_owner,lease_expires_at,created_at,updated_at)
	SELECT 'tenant-plan','conversation-plan','handoff-'||n,'user','session','turn',1,
		'request','objective','context','hash',CURRENT_TIMESTAMP + interval '1 day',1024,(n % 21 - 10)::smallint,
		'[]','{}','model','prompt','tools','config','idem-'||n,'hash-'||n,
		CASE n % 4 WHEN 0 THEN 'pending' WHEN 1 THEN 'running' WHEN 2 THEN 'leased' ELSE 'needs_input' END,
		CASE WHEN n % 4 = 3 THEN 2 ELSE 0 END,
		CASE WHEN n % 4 = 3 THEN 1 ELSE 0 END,
		CASE WHEN n % 4 = 3 THEN '{"answer":1}'::json ELSE NULL END,
		CURRENT_TIMESTAMP + ((n % 30) || ' seconds')::interval,
		CASE WHEN n % 4 = 2 THEN 'worker-'||n ELSE NULL END,
		CASE WHEN n % 4 = 2 THEN CURRENT_TIMESTAMP - interval '1 minute' ELSE NULL END,
		CURRENT_TIMESTAMP - ((n % 30) || ' seconds')::interval,
		CURRENT_TIMESTAMP - ((n % 30) || ' seconds')::interval
	FROM generate_series(1,10000) AS n`)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `ANALYZE handoffs`)
	require.NoError(t, err)

	plans := []struct{ name, query string }{
		{"handoffs_runnable_idx", `SELECT id FROM handoffs WHERE state IN ('pending','running') AND lease_owner IS NULL AND ready_at <= CURRENT_TIMESTAMP ORDER BY priority DESC,ready_at,created_at LIMIT 1`},
		{"handoffs_expired_lease_idx", `SELECT id FROM handoffs WHERE lease_owner IS NOT NULL AND lease_expires_at <= CURRENT_TIMESTAMP ORDER BY lease_expires_at LIMIT 1`},
		{"handoffs_supplied_input_idx", `SELECT id FROM handoffs WHERE state='needs_input' AND input_revision > consumed_input_revision ORDER BY updated_at LIMIT 1`},
		{"outbox_event_seq_idx", `SELECT id FROM outbox WHERE event_seq=1`},
	}
	for _, plan := range plans {
		t.Run(plan.name, func(t *testing.T) {
			tx, err := pool.Begin(t.Context())
			require.NoError(t, err)
			defer tx.Rollback(t.Context())
			_, err = tx.Exec(t.Context(), `SET LOCAL enable_seqscan=off`)
			require.NoError(t, err)
			var document []byte
			require.NoError(t, tx.QueryRow(t.Context(), `EXPLAIN (FORMAT JSON) `+plan.query).Scan(&document))
			text := string(document)
			assert.Contains(t, text, plan.name)
			assert.NotContains(t, text, `"Node Type": "Seq Scan"`)
		})
	}
}

func migratedPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := testkit.Postgres(t)
	require.NoError(t, migrations.Up(t.Context(), pool))
	return pool
}

func insertConversation(t *testing.T, pool *pgxpool.Pool, tenant, conversation string) {
	t.Helper()
	_, err := pool.Exec(t.Context(), `INSERT INTO conversations (tenant_id,id,user_id,retention_until)
		VALUES ($1,$2,'user',CURRENT_TIMESTAMP + interval '30 days')`, tenant, conversation)
	require.NoError(t, err)
}

func insertHandoff(t *testing.T, pool *pgxpool.Pool, tenant, conversation, id, idempotencyKey string) error {
	t.Helper()
	_, err := pool.Exec(t.Context(), `INSERT INTO handoffs (
		tenant_id,conversation_id,id,user_id,session_id,source_turn_id,source_turn_revision,
		user_request,objective,context_ref,context_hash,deadline,token_budget,tool_allowlist,
		approval_policy,model_version,prompt_version,tool_registry_version,config_version,idempotency_key,request_hash)
	VALUES ($1,$2,$3,'user','session','turn',1,'request','objective','context','hash',
		CURRENT_TIMESTAMP + interval '1 day',1024,'["read"]','{"mode":"auto"}','model','prompt','tools','config',$4,'request-hash')`,
		tenant, conversation, id, idempotencyKey)
	return err
}

func insertToolInvocation(
	t *testing.T,
	pool *pgxpool.Pool,
	invocationID, tenant, conversation, handoffID, idempotencyKey string,
) error {
	t.Helper()
	_, err := pool.Exec(t.Context(), `INSERT INTO tool_invocations (
		invocation_id,tenant_id,conversation_id,handoff_id,attempt,objective_revision,tool_ordinal,tool_name,
		approval_state,idempotency_key,canonical_arguments_hash,arguments_ciphertext,arguments_nonce,arguments_key_id)
	VALUES ($1,$2,$3,$4,1,2,3,'read','approved',$5,'arguments-hash',
		decode(repeat('00',16),'hex'),decode(repeat('00',12),'hex'),'key')`,
		invocationID, tenant, conversation, handoffID, idempotencyKey)
	return err
}

func assertToolInvocationStableConstraintColumns(t *testing.T, pool *pgxpool.Pool, want []string) {
	t.Helper()
	rows, err := pool.Query(t.Context(), `
		SELECT attribute.attname
		FROM pg_catalog.pg_constraint constraint_definition
		CROSS JOIN LATERAL unnest(constraint_definition.conkey) WITH ORDINALITY AS key_column(attnum, ordinal)
		JOIN pg_catalog.pg_attribute attribute
			ON attribute.attrelid=constraint_definition.conrelid AND attribute.attnum=key_column.attnum
		WHERE constraint_definition.connamespace=(SELECT oid FROM pg_catalog.pg_namespace WHERE nspname=current_schema())
			AND constraint_definition.conrelid='tool_invocations'::regclass
			AND constraint_definition.conname='tool_invocations_stable_uniq'
			AND constraint_definition.contype='u'
		ORDER BY key_column.ordinal`)
	require.NoError(t, err)
	defer rows.Close()
	var got []string
	for rows.Next() {
		var column string
		require.NoError(t, rows.Scan(&column))
		got = append(got, column)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, want, got, "tool invocation stable uniqueness scope and ordering")
}

func insertEvent(t *testing.T, pool *pgxpool.Pool, eventID, tenant, conversation, handoff string) int64 {
	t.Helper()
	var sequence int64
	err := pool.QueryRow(t.Context(), `INSERT INTO handoff_events
		(event_id,tenant_id,conversation_id,handoff_id,kind,state,payload)
		VALUES ($1,$2,$3,$4,'created','pending','{}') RETURNING seq`, eventID, tenant, conversation, handoff).Scan(&sequence)
	require.NoError(t, err)
	return sequence
}

func assertConstraint(t *testing.T, err error, name string) {
	t.Helper()
	require.Error(t, err)
	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr), "expected PostgreSQL constraint error, got %v", err)
	assert.Equal(t, name, pgErr.ConstraintName)
}

func waitForBlockedMigration(t *testing.T, pool *pgxpool.Pool, lockType string) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	for {
		var waiting int
		require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM pg_locks WHERE locktype=$1 AND NOT granted`, lockType).Scan(&waiting))
		if waiting > 0 {
			return
		}
		select {
		case <-poll.C:
		case <-deadline.C:
			t.Fatalf("migration never waited on %s lock", lockType)
		}
	}
}

func assertPromptCancellation(t *testing.T, done <-chan error, unblock func()) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-deadline.C:
		unblock()
		select {
		case err := <-done:
			t.Logf("late migration cancellation result: %v", err)
		case <-time.After(5 * time.Second):
		}
		t.Fatal("migration did not return promptly after caller cancellation")
	}
}

func allowedNullableColumn(table, column string) bool {
	key := table + "." + column
	allowed := map[string]bool{
		"handoffs.input": true, "handoffs.lease_owner": true, "handoffs.lease_expires_at": true,
		"handoffs.cancel_requested_at": true, "handoffs.cancel_actor": true, "handoffs.cancel_reason": true,
		"handoffs.terminal_mutation_id": true, "handoffs.result_summary": true, "handoffs.structured_result": true,
		"handoffs.failure_code": true, "handoffs.failure_retryable": true, "handoffs.failure_message": true,
		"handoffs.safety_decision": true, "handoffs.usage": true, "handoffs.terminal_at": true,
		"outbox.published_at": true, "outbox.last_error": true,
		"tool_invocations.approval_expires_at": true, "tool_invocations.result_metadata": true,
		"tool_invocations.result": true, "tool_invocations.mutation_id": true,
		"coordinator_work_journal.storage_sequence": true, "coordinator_work_journal.claim_owner": true,
		"coordinator_work_journal.claim_until": true, "coordinator_work_journal.delivered_at": true,
	}
	return allowed[key]
}

func highKeyJSON(fields int) string {
	var document strings.Builder
	document.WriteByte('{')
	for index := 0; index < fields; index++ {
		if index > 0 {
			document.WriteByte(',')
		}
		_, _ = fmt.Fprintf(&document, `"field-%04d":%d`, index, index)
	}
	document.WriteByte('}')
	return document.String()
}

func toolNamesJSON(count int) string {
	var document strings.Builder
	document.WriteByte('[')
	for index := 0; index < count; index++ {
		if index > 0 {
			document.WriteByte(',')
		}
		_, _ = fmt.Fprintf(&document, `"tool-%02d"`, index)
	}
	document.WriteByte(']')
	return document.String()
}

func nulPrefixedToolNamesJSON(count, nulBytes int) string {
	var document strings.Builder
	document.WriteByte('[')
	for index := 0; index < count; index++ {
		if index > 0 {
			document.WriteByte(',')
		}
		_, _ = fmt.Fprintf(&document, `"%02d%s"`, index, strings.Repeat(`\u0000`, nulBytes))
	}
	document.WriteByte(']')
	return document.String()
}
