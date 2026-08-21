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
	"profile_blobs": {
		"tenant_id": "text", "content_hash": "text", "media_type": "text", "content": "bytea",
		"content_bytes": "integer", "created_at": "timestamp with time zone",
	},
	"profile_drafts": {
		"tenant_id": "text", "profile_id": "text", "generation": "bigint", "manifest": "json",
		"updated_at": "timestamp with time zone", "updated_by": "text",
	},
	"profile_versions": {
		"tenant_id": "text", "profile_id": "text", "version": "bigint", "draft_generation": "bigint",
		"engine_version": "text", "snapshot_hash": "text", "manifest": "json", "snapshot": "json",
		"active": "boolean", "revoked_at": "timestamp with time zone", "revoked_by": "text",
		"published_at": "timestamp with time zone", "published_by": "text",
		"activated_at": "timestamp with time zone", "activated_by": "text",
	},
	"knowledge_versions": {
		"tenant_id": "text", "pack_id": "text", "version": "bigint", "content_hash": "text",
		"title": "text", "media_type": "text", "excerpt_count": "integer", "content_bytes": "bigint",
		"published_at": "timestamp with time zone", "published_by": "text",
		"tombstoned_at": "timestamp with time zone", "tombstoned_by": "text",
	},
	"knowledge_excerpts": {
		"tenant_id": "text", "pack_id": "text", "version": "bigint", "ordinal": "integer",
		"body": "text", "body_bytes": "integer", "excerpt_hash": "text", "tokens": "ARRAY",
		"source_title": "text", "section": "text", "page": "integer",
	},
	"conversations": {
		"tenant_id": "text", "id": "text", "user_id": "text", "status": "text",
		"retention_until": "timestamp with time zone", "deletion_actor": "text", "deletion_reason_code": "text",
		"deletion_requested_at": "timestamp with time zone", "created_at": "timestamp with time zone", "updated_at": "timestamp with time zone",
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
		"claim_until": "timestamp with time zone", "claim_generation": "bigint", "claim_affinity_generation": "bigint",
		"created_at": "timestamp with time zone", "delivered_at": "timestamp with time zone",
	},
	"coordinator_delivery_shards": {
		"delivery_affinity": "text", "lease_owner": "text", "lease_until": "timestamp with time zone",
		"lease_generation": "bigint", "created_at": "timestamp with time zone", "updated_at": "timestamp with time zone",
	},
	"handoffs": {
		"tenant_id": "text", "conversation_id": "text", "id": "text", "user_id": "text", "session_id": "text",
		"source_turn_id": "text", "source_turn_revision": "bigint", "user_request": "text", "objective": "text",
		"context_ref": "text", "context_hash": "text", "deadline": "timestamp with time zone", "token_budget": "integer",
		"priority": "smallint", "tool_allowlist": "json", "approval_policy": "json", "model_version": "text",
		"tool_policy":    "jsonb",
		"prompt_version": "text", "tool_registry_version": "text", "config_version": "text",
		"profile_id": "text", "profile_version": "bigint", "profile_snapshot_hash": "text", "profile_engine_version": "text",
		"worker_recipe_id": "text", "worker_recipe_version": "bigint", "worker_recipe_hash": "text",
		"adapter_registry_version": "text", "profile_knowledge": "json", "profile_evidence": "json", "presentation_hint": "text",
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
	"session_reconnect_owners": {
		"session_id": "text", "tenant_id": "text", "user_id": "text", "conversation_id": "text",
		"incarnation": "numeric", "event_cursor": "bigint", "provider_model": "text", "response_epoch": "numeric",
		"credential_id": "text", "owner_id": "text", "owner_state": "text", "turn_boundary": "boolean",
		"expires_at": "timestamp with time zone", "subject_expires_at": "timestamp with time zone",
		"previous_credential_id": "text", "previous_incarnation": "numeric", "previous_event_cursor": "bigint", "previous_expires_at": "timestamp with time zone",
		"previous_accept_until": "timestamp with time zone", "owner_lease_expires_at": "timestamp with time zone", "ephemeral": "boolean",
		"created_at": "timestamp with time zone", "updated_at": "timestamp with time zone",
	},
	"deletion_audits": {
		"audit_id": "text", "tenant_id": "text", "conversation_hash": "text", "actor": "text",
		"reason_code": "text", "deleted_at": "timestamp with time zone",
	},
	"runtime_feature_authority": {
		"singleton": "boolean", "version": "bigint", "updated_at": "timestamp with time zone",
	},
	"runtime_feature_flags": {
		"name": "text", "enabled": "boolean", "version": "bigint", "updated_at": "timestamp with time zone",
	},
	"runtime_feature_audit": {
		"version": "bigint", "name": "text", "enabled": "boolean", "actor": "text", "reason": "text", "changed_at": "timestamp with time zone",
	},
}

var requiredDefaults = map[string]string{
	"profile_versions.active":                   `false`,
	"conversations.status":                      `'active'::text`,
	"conversations.created_at":                  `CURRENT_TIMESTAMP`,
	"conversations.updated_at":                  `CURRENT_TIMESTAMP`,
	"conversation_items.created_at":             `CURRENT_TIMESTAMP`,
	"conversation_items.updated_at":             `CURRENT_TIMESTAMP`,
	"coordinator_work_journal.delivered":        `false`,
	"coordinator_work_journal.claim_generation": `0`,
	"coordinator_work_journal.created_at":       `CURRENT_TIMESTAMP`,
	"coordinator_delivery_shards.created_at":    `CURRENT_TIMESTAMP`,
	"coordinator_delivery_shards.updated_at":    `CURRENT_TIMESTAMP`,
	"handoffs.priority":                         `0`,
	"handoffs.tool_allowlist":                   `'[]'::json`,
	"handoffs.tool_policy":                      `'{"web_search": {"region": "wt-wt", "enabled": false, "timeout_ms": 8000, "max_results": 5, "safe_search": "moderate"}}'::jsonb`,
	"handoffs.profile_id":                       `'builtin'::text`,
	"handoffs.profile_version":                  `1`,
	"handoffs.profile_snapshot_hash":            `'0000000000000000000000000000000000000000000000000000000000000000'::text`,
	"handoffs.profile_engine_version":           `'jinja-go-v1/legacy'::text`,
	"handoffs.worker_recipe_id":                 `'builtin'::text`,
	"handoffs.worker_recipe_version":            `1`,
	"handoffs.worker_recipe_hash":               `'0000000000000000000000000000000000000000000000000000000000000000'::text`,
	"handoffs.adapter_registry_version":         `'builtin-v1'::text`,
	"handoffs.profile_knowledge":                `'[]'::json`,
	"handoffs.profile_evidence":                 `'{}'::json`,
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
	"session_reconnect_owners.turn_boundary":    `false`,
	"session_reconnect_owners.ephemeral":        `false`,
	"session_reconnect_owners.created_at":       `CURRENT_TIMESTAMP`,
	"session_reconnect_owners.updated_at":       `CURRENT_TIMESTAMP`,
	"deletion_audits.deleted_at":                `CURRENT_TIMESTAMP`,
	"runtime_feature_authority.singleton":       `true`,
	"runtime_feature_authority.updated_at":      `CURRENT_TIMESTAMP`,
	"runtime_feature_flags.updated_at":          `CURRENT_TIMESTAMP`,
	"runtime_feature_audit.changed_at":          `CURRENT_TIMESTAMP`,
}

var requiredNamedConstraintNames = strings.Fields(`
	profile_blobs_identity_check profile_blobs_media_type_check profile_blobs_content_check profile_blobs_created_at_check
	profile_drafts_identity_check profile_drafts_generation_check profile_drafts_manifest_check
	profile_drafts_updated_by_check profile_drafts_updated_at_check
	profile_versions_draft_generation_uniq profile_versions_identity_check profile_versions_numbers_check
	profile_versions_engine_hash_check profile_versions_documents_check profile_versions_publication_check
	profile_versions_activation_check profile_versions_revocation_check profile_versions_chronology_check
	profile_versions_snapshot_coherence_check profile_versions_state_check
	conversations_tenant_id_size_check conversations_id_size_check conversations_user_id_size_check
	conversations_status_check conversations_retention_check conversations_timestamps_check
	conversations_deletion_actor_size_check conversations_deletion_reason_size_check conversations_deletion_tuple_check
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
	coordinator_work_journal_claim_affinity_generation_check
	coordinator_delivery_shards_affinity_size_check coordinator_delivery_shards_owner_size_check
	coordinator_delivery_shards_generation_check coordinator_delivery_shards_timestamps_check
	handoffs_tenant_idempotency_uniq handoffs_tenant_id_size_check handoffs_conversation_id_size_check
	handoffs_id_size_check handoffs_user_id_size_check handoffs_session_id_size_check
	handoffs_source_turn_id_size_check handoffs_source_turn_revision_check handoffs_user_request_size_check
	handoffs_objective_size_check handoffs_context_ref_size_check handoffs_context_hash_size_check
	handoffs_deadline_check handoffs_token_budget_check handoffs_priority_check handoffs_tool_allowlist_check
	handoffs_approval_policy_check handoffs_model_version_size_check handoffs_prompt_version_size_check
	handoffs_tool_registry_version_size_check handoffs_config_version_size_check handoffs_presentation_hint_size_check
	handoffs_profile_snapshot_check
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
	session_reconnect_owners_identifiers_check session_reconnect_owners_counters_check
	session_reconnect_owners_state_check session_reconnect_owners_times_check session_reconnect_owners_previous_check
	deletion_audits_conversation_hash_size_check deletion_audits_actor_size_check deletion_audits_reason_code_size_check
	runtime_feature_authority_singleton_check runtime_feature_authority_version_check
	runtime_feature_flags_name_check runtime_feature_flags_version_check
	runtime_feature_audit_name_check runtime_feature_audit_actor_size_check
	runtime_feature_audit_reason_size_check runtime_feature_audit_version_check
	knowledge_versions_content_uniq knowledge_versions_identity_check knowledge_versions_version_check
	knowledge_versions_hash_check knowledge_versions_title_check knowledge_versions_aggregate_check
	knowledge_versions_publication_check knowledge_versions_tombstone_check
	knowledge_excerpts_ordinal_check knowledge_excerpts_body_check knowledge_excerpts_hash_check
	knowledge_excerpts_tokens_check knowledge_excerpts_citation_check
`)

// stepDownTo walks the schema down to an exact version. Tests use it instead of
// a literal step count so adding a migration cannot silently retarget them.
func stepDownTo(t *testing.T, pool *pgxpool.Pool, target uint) {
	t.Helper()
	current, dirty, err := migrations.Version(t.Context(), pool)
	require.NoError(t, err)
	require.False(t, dirty)
	require.GreaterOrEqual(t, current, target, "schema is already below the requested version")
	if current == target {
		return
	}
	require.NoError(t, migrations.Steps(t.Context(), pool, -int(current-target)))
	reached, dirty, err := migrations.Version(t.Context(), pool)
	require.NoError(t, err)
	require.False(t, dirty)
	require.Equal(t, target, reached)
}

// stepUpTo walks the schema up to an exact version.
func stepUpTo(t *testing.T, pool *pgxpool.Pool, target uint) {
	t.Helper()
	current, dirty, err := migrations.Version(t.Context(), pool)
	require.NoError(t, err)
	require.False(t, dirty)
	require.LessOrEqual(t, current, target, "schema is already above the requested version")
	if current == target {
		return
	}
	require.NoError(t, migrations.Steps(t.Context(), pool, int(target-current)))
	reached, dirty, err := migrations.Version(t.Context(), pool)
	require.NoError(t, err)
	require.False(t, dirty)
	require.Equal(t, target, reached)
}

func latestSchemaVersion(t *testing.T, pool *pgxpool.Pool) uint {
	t.Helper()
	require.NoError(t, migrations.Up(t.Context(), pool))
	version, dirty, err := migrations.Version(t.Context(), pool)
	require.NoError(t, err)
	require.False(t, dirty)
	return version
}

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
	assert.Equal(t, uint(11), version)
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
	stepDownTo(t, pool, 8)
	version, dirty, err = migrations.Version(t.Context(), pool)
	require.NoError(t, err)
	assert.Equal(t, uint(8), version)
	assert.False(t, dirty)
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT to_regclass('coordinator_work_journal') IS NOT NULL`).Scan(&journalPresent))
	assert.True(t, journalPresent)
	require.NoError(t, migrations.Steps(t.Context(), pool, -1))
	version, dirty, err = migrations.Version(t.Context(), pool)
	require.NoError(t, err)
	assert.Equal(t, uint(7), version)
	assert.False(t, dirty)
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT to_regclass('coordinator_work_journal') IS NOT NULL`).Scan(&journalPresent))
	assert.True(t, journalPresent)
	require.NoError(t, migrations.Steps(t.Context(), pool, -1))
	version, dirty, err = migrations.Version(t.Context(), pool)
	require.NoError(t, err)
	assert.Equal(t, uint(6), version)
	assert.False(t, dirty)
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT to_regclass('coordinator_work_journal') IS NOT NULL`).Scan(&journalPresent))
	assert.True(t, journalPresent)
	require.NoError(t, migrations.Steps(t.Context(), pool, -1))
	version, dirty, err = migrations.Version(t.Context(), pool)
	require.NoError(t, err)
	assert.Equal(t, uint(5), version)
	assert.False(t, dirty)
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT to_regclass('coordinator_work_journal') IS NOT NULL`).Scan(&journalPresent))
	assert.True(t, journalPresent)
	require.NoError(t, migrations.Steps(t.Context(), pool, -1))
	version, dirty, err = migrations.Version(t.Context(), pool)
	require.NoError(t, err)
	assert.Equal(t, uint(4), version)
	assert.False(t, dirty)
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT to_regclass('coordinator_work_journal') IS NOT NULL`).Scan(&journalPresent))
	assert.True(t, journalPresent)
	require.NoError(t, migrations.Steps(t.Context(), pool, -1))
	version, dirty, err = migrations.Version(t.Context(), pool)
	require.NoError(t, err)
	assert.Equal(t, uint(3), version)
	assert.False(t, dirty)
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT to_regclass('coordinator_work_journal') IS NOT NULL`).Scan(&journalPresent))
	assert.True(t, journalPresent)
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
	require.NoError(t, migrations.Up(t.Context(), pool))
	require.NoError(t, migrations.Steps(t.Context(), pool, 0))
	require.NoError(t, migrations.Check(t.Context(), pool))
}

func TestSchemaProfileV9UpDownUpCatalogAndConstraints(t *testing.T) {
	pool := testkit.Postgres(t)
	require.NoError(t, migrations.Up(t.Context(), pool))

	for _, table := range []string{"profile_blobs", "profile_drafts", "profile_versions"} {
		var present bool
		require.NoError(t, pool.QueryRow(t.Context(), `SELECT to_regclass($1) IS NOT NULL`, table).Scan(&present))
		assert.True(t, present, table)
	}
	_, err := pool.Exec(t.Context(), `INSERT INTO profile_blobs
		(tenant_id,content_hash,media_type,content,content_bytes,created_at)
		VALUES ('tenant-v9',$1,'text/plain; charset=utf-8',''::bytea,0,$2)`, strings.Repeat("a", 64), profileTestCatalogTime())
	require.NoError(t, err, "zero-byte deterministic templates are valid")
	_, err = pool.Exec(t.Context(), `INSERT INTO profile_blobs
		(tenant_id,content_hash,media_type,content,content_bytes,created_at)
		VALUES ('tenant-v9',$1,'text/plain; charset=utf-8',''::bytea,524289,$2)`, strings.Repeat("b", 64), profileTestCatalogTime())
	assertConstraint(t, err, "profile_blobs_content_check")

	_, err = pool.Exec(t.Context(), `INSERT INTO profile_drafts
		(tenant_id,profile_id,generation,manifest,updated_at,updated_by)
		VALUES ('tenant-v9','profile-v9',2,'{}',$1,'editor')`, profileTestCatalogTime())
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `INSERT INTO profile_versions
		(tenant_id,profile_id,version,draft_generation,engine_version,snapshot_hash,manifest,snapshot,
		 active,published_at,published_by,activated_at,activated_by)
		VALUES ('tenant-v9','profile-v9',1,1,'jinja-go-v1/test-engine-v1',$1,'{}',
			json_build_object('tenant_id','tenant-v9','ref',json_build_object('id','profile-v9','version',1),
				'engine_version','jinja-go-v1/test-engine-v1','snapshot_hash',$1::text),true,$2,'publisher',$2,'publisher')`,
		strings.Repeat("c", 64), profileTestCatalogTime())
	require.NoError(t, err)
	var foreignKeyName, deleteAction string
	var sourceColumns, targetColumns []string
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT constraint_definition.conname,
		constraint_definition.confdeltype::text,
		ARRAY(SELECT source_attribute.attname FROM unnest(constraint_definition.conkey) WITH ORDINALITY key(attnum,ordinal)
			JOIN pg_attribute source_attribute ON source_attribute.attrelid=constraint_definition.conrelid AND source_attribute.attnum=key.attnum
			ORDER BY key.ordinal),
		ARRAY(SELECT target_attribute.attname FROM unnest(constraint_definition.confkey) WITH ORDINALITY key(attnum,ordinal)
			JOIN pg_attribute target_attribute ON target_attribute.attrelid=constraint_definition.confrelid AND target_attribute.attnum=key.attnum
			ORDER BY key.ordinal)
		FROM pg_constraint constraint_definition
		WHERE constraint_definition.conrelid='profile_versions'::regclass AND constraint_definition.contype='f'`).
		Scan(&foreignKeyName, &deleteAction, &sourceColumns, &targetColumns))
	assert.Equal(t, "profile_versions_draft_fkey", foreignKeyName)
	assert.Equal(t, "r", deleteAction, "draft deletion must be RESTRICT")
	assert.Equal(t, []string{"tenant_id", "profile_id"}, sourceColumns)
	assert.Equal(t, []string{"tenant_id", "profile_id"}, targetColumns)

	_, err = pool.Exec(t.Context(), `INSERT INTO profile_versions
		(tenant_id,profile_id,version,draft_generation,engine_version,snapshot_hash,manifest,snapshot,published_at,published_by)
		VALUES ('tenant-v9','orphan-v9',1,1,'jinja-go-v1/test-engine-v1',$1,'{}',
			json_build_object('tenant_id','tenant-v9','ref',json_build_object('id','orphan-v9','version',1),
				'engine_version','jinja-go-v1/test-engine-v1','snapshot_hash',$1::text),$2,'publisher')`, strings.Repeat("f", 64), profileTestCatalogTime())
	assertConstraint(t, err, "profile_versions_draft_fkey")
	_, err = pool.Exec(t.Context(), `INSERT INTO profile_versions
		(tenant_id,profile_id,version,draft_generation,engine_version,snapshot_hash,manifest,snapshot,published_at,published_by)
		VALUES ('tenant-other','profile-v9',1,1,'jinja-go-v1/test-engine-v1',$1,'{}',
			json_build_object('tenant_id','tenant-other','ref',json_build_object('id','profile-v9','version',1),
				'engine_version','jinja-go-v1/test-engine-v1','snapshot_hash',$1::text),$2,'publisher')`, strings.Repeat("f", 64), profileTestCatalogTime())
	assertConstraint(t, err, "profile_versions_draft_fkey")
	_, err = pool.Exec(t.Context(), `DELETE FROM profile_drafts WHERE tenant_id='tenant-v9' AND profile_id='profile-v9'`)
	assertConstraint(t, err, "profile_versions_draft_fkey")

	coherenceMutations := []struct {
		name  string
		query string
	}{
		{name: "tenant", query: `UPDATE profile_versions SET snapshot=json_build_object(
			'tenant_id','tenant-other','ref',json_build_object('id','profile-v9','version',1),
			'engine_version','jinja-go-v1/test-engine-v1','snapshot_hash',snapshot_hash)
			WHERE tenant_id='tenant-v9' AND profile_id='profile-v9' AND version=1`},
		{name: "profile id", query: `UPDATE profile_versions SET snapshot=json_build_object(
			'tenant_id','tenant-v9','ref',json_build_object('id','profile-other','version',1),
			'engine_version','jinja-go-v1/test-engine-v1','snapshot_hash',snapshot_hash)
			WHERE tenant_id='tenant-v9' AND profile_id='profile-v9' AND version=1`},
		{name: "version", query: `UPDATE profile_versions SET snapshot=json_build_object(
			'tenant_id','tenant-v9','ref',json_build_object('id','profile-v9','version',2),
			'engine_version','jinja-go-v1/test-engine-v1','snapshot_hash',snapshot_hash)
			WHERE tenant_id='tenant-v9' AND profile_id='profile-v9' AND version=1`},
		{name: "engine", query: `UPDATE profile_versions SET snapshot=json_build_object(
			'tenant_id','tenant-v9','ref',json_build_object('id','profile-v9','version',1),
			'engine_version','jinja-go-v2','snapshot_hash',snapshot_hash)
			WHERE tenant_id='tenant-v9' AND profile_id='profile-v9' AND version=1`},
		{name: "snapshot hash", query: `UPDATE profile_versions SET snapshot=json_build_object(
			'tenant_id','tenant-v9','ref',json_build_object('id','profile-v9','version',1),
			'engine_version','jinja-go-v1/test-engine-v1','snapshot_hash',repeat('f',64))
			WHERE tenant_id='tenant-v9' AND profile_id='profile-v9' AND version=1`},
	}
	for _, mutation := range coherenceMutations {
		t.Run("snapshot coherence "+mutation.name, func(t *testing.T) {
			_, mutationErr := pool.Exec(t.Context(), mutation.query)
			assertConstraint(t, mutationErr, "profile_versions_snapshot_coherence_check")
		})
	}
	_, err = pool.Exec(t.Context(), `UPDATE profile_versions SET active=false,
		activated_at=published_at+interval '10 minutes',activated_by='operator',
		revoked_at=published_at+interval '5 minutes',revoked_by='operator'
		WHERE tenant_id='tenant-v9' AND profile_id='profile-v9' AND version=1`)
	assertConstraint(t, err, "profile_versions_chronology_check")

	_, err = pool.Exec(t.Context(), `INSERT INTO profile_versions
		(tenant_id,profile_id,version,draft_generation,engine_version,snapshot_hash,manifest,snapshot,
		 active,published_at,published_by,activated_at,activated_by)
		VALUES ('tenant-v9','profile-v9',2,2,'jinja-go-v1/test-engine-v1',$1,'{}',
			json_build_object('tenant_id','tenant-v9','ref',json_build_object('id','profile-v9','version',2),
				'engine_version','jinja-go-v1/test-engine-v1','snapshot_hash',$1::text),true,$2,'publisher',$2,'publisher')`,
		strings.Repeat("d", 64), profileTestCatalogTime())
	assertConstraint(t, err, "profile_one_active_version")
	_, err = pool.Exec(t.Context(), `INSERT INTO profile_versions
		(tenant_id,profile_id,version,draft_generation,engine_version,snapshot_hash,manifest,snapshot,
		 active,published_at,published_by)
		VALUES ('tenant-v9','profile-v9',3,1,'jinja-go-v1/test-engine-v1',$1,'{}',
			json_build_object('tenant_id','tenant-v9','ref',json_build_object('id','profile-v9','version',3),
				'engine_version','jinja-go-v1/test-engine-v1','snapshot_hash',$1::text),false,$2,'publisher')`,
		strings.Repeat("e", 64), profileTestCatalogTime())
	assertConstraint(t, err, "profile_versions_draft_generation_uniq")
	_, err = pool.Exec(t.Context(), `UPDATE profile_versions
		SET revoked_at=$1,revoked_by='operator',active=true WHERE tenant_id='tenant-v9' AND profile_id='profile-v9' AND version=1`,
		profileTestCatalogTime().Add(time.Minute))
	assertConstraint(t, err, "profile_versions_state_check")

	stepDownTo(t, pool, 8)
	version, dirty, err := migrations.Version(t.Context(), pool)
	require.NoError(t, err)
	assert.Equal(t, uint(8), version)
	assert.False(t, dirty)
	for _, table := range []string{"profile_blobs", "profile_drafts", "profile_versions"} {
		var present bool
		require.NoError(t, pool.QueryRow(t.Context(), `SELECT to_regclass($1) IS NOT NULL`, table).Scan(&present))
		assert.False(t, present, table)
	}
	var priorPresent bool
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT to_regclass('handoffs') IS NOT NULL`).Scan(&priorPresent))
	assert.True(t, priorPresent)
	require.NoError(t, migrations.Up(t.Context(), pool))
	require.NoError(t, migrations.Check(t.Context(), pool))
}

func TestSchemaProfileEngineIdentityMatchesGoBoundary(t *testing.T) {
	pool := testkit.Postgres(t)
	require.NoError(t, migrations.Up(t.Context(), pool))

	tests := []struct {
		name     string
		identity string
		valid    bool
	}{
		{"exact 255 bytes", "jinja-go-v1/" + strings.Repeat("a", 243), true},
		{"missing prefix", "implementation-v1", false},
		{"missing suffix", "jinja-go-v1/", false},
		{"empty segment", "jinja-go-v1/implementation//v1", false},
		{"dot segment", "jinja-go-v1/./v1", false},
		{"dotdot segment", "jinja-go-v1/../v1", false},
		{"invalid segment character", "jinja-go-v1/implementation v1", false},
		{"limit plus one", "jinja-go-v1/" + strings.Repeat("a", 244), false},
	}
	for index, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			profileID := fmt.Sprintf("engine-boundary-%d", index)
			_, err := pool.Exec(t.Context(), `INSERT INTO profile_drafts
				(tenant_id,profile_id,generation,manifest,updated_at,updated_by)
				VALUES ('tenant-engine',$1,1,'{}',$2,'editor')`, profileID, profileTestCatalogTime())
			require.NoError(t, err)
			_, err = pool.Exec(t.Context(), `INSERT INTO profile_versions
				(tenant_id,profile_id,version,draft_generation,engine_version,snapshot_hash,manifest,snapshot,published_at,published_by)
				VALUES ('tenant-engine',$1,1,1,$2,$3,'{}',json_build_object(
					'tenant_id','tenant-engine','ref',json_build_object('id',$1::text,'version',1),
					'engine_version',$2::text,'snapshot_hash',$3::text),$4,'publisher')`,
				profileID, tc.identity, strings.Repeat("a", 64), profileTestCatalogTime())
			if tc.valid {
				require.NoError(t, err)
				return
			}
			assertConstraint(t, err, "profile_versions_engine_hash_check")
		})
	}
}

func TestSchemaHandoffToolPolicyV8UpDownUpIsExactAndRefusesDataLoss(t *testing.T) {
	pool := migratedPostgres(t)
	insertConversation(t, pool, "tenant-v8", "conversation-v8")
	require.NoError(t, insertHandoff(t, pool, "tenant-v8", "conversation-v8", "handoff-v8", "idem-v8"))

	const defaultPolicy = `{"web_search":{"enabled":false,"safe_search":"moderate","region":"wt-wt","max_results":5,"timeout_ms":8000}}`
	var exact bool
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT tool_policy=$1::jsonb FROM handoffs WHERE id='handoff-v8'`, defaultPolicy).Scan(&exact))
	assert.True(t, exact)

	stepDownTo(t, pool, 7)
	var columnPresent bool
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema=current_schema() AND table_name='handoffs' AND column_name='tool_policy'
	)`).Scan(&columnPresent))
	assert.False(t, columnPresent)
	require.NoError(t, migrations.Steps(t.Context(), pool, 1))
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT tool_policy=$1::jsonb FROM handoffs WHERE id='handoff-v8'`, defaultPolicy).Scan(&exact))
	assert.True(t, exact)

	const enabledPolicy = `{"web_search":{"enabled":true,"safe_search":"strict","region":"cn-zh","max_results":2,"timeout_ms":3000}}`
	_, err := pool.Exec(t.Context(), `UPDATE handoffs SET tool_policy=$1::jsonb WHERE id='handoff-v8'`, enabledPolicy)
	require.NoError(t, err)
	err = migrations.Steps(t.Context(), pool, -1)
	require.ErrorIs(t, err, migrations.ErrHandoffToolPolicyDataLoss)
	version, dirty, versionErr := migrations.Version(t.Context(), pool)
	require.NoError(t, versionErr)
	assert.Equal(t, uint(8), version)
	assert.False(t, dirty)
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT tool_policy=$1::jsonb FROM handoffs WHERE id='handoff-v8'`, enabledPolicy).Scan(&exact))
	assert.True(t, exact)
}

func TestSchemaHandoffProfileSnapshotV11UpDownUpIsExactAndRefusesDataLoss(t *testing.T) {
	pool := migratedPostgres(t)
	insertConversation(t, pool, "tenant-v11", "conversation-v11")
	require.NoError(t, insertHandoff(t, pool, "tenant-v11", "conversation-v11", "handoff-v11", "idem-v11"))

	stepDownTo(t, pool, 10)
	var columnPresent bool
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema=current_schema() AND table_name='handoffs' AND column_name='profile_snapshot_hash'
	)`).Scan(&columnPresent))
	assert.False(t, columnPresent)
	require.NoError(t, migrations.Steps(t.Context(), pool, 1))

	_, err := pool.Exec(t.Context(), `UPDATE handoffs SET profile_id='insurance' WHERE id='handoff-v11'`)
	require.NoError(t, err)
	err = migrations.Steps(t.Context(), pool, -1)
	require.ErrorIs(t, err, migrations.ErrHandoffProfileSnapshotDataLoss)
	version, dirty, versionErr := migrations.Version(t.Context(), pool)
	require.NoError(t, versionErr)
	assert.Equal(t, uint(11), version)
	assert.False(t, dirty)
	var profileID string
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT profile_id FROM handoffs WHERE id='handoff-v11'`).Scan(&profileID))
	assert.Equal(t, "insurance", profileID)
}

func TestSchemaTask20RuntimeFlagsV4UpDownUpPreservesPriorDurableRowsAndTriggers(t *testing.T) {
	pool := migratedPostgres(t)
	insertConversation(t, pool, "tenant-v4-preserve", "conversation-v4-preserve")
	_, err := pool.Exec(t.Context(), `
		UPDATE runtime_feature_authority SET version=2 WHERE singleton=true;
		UPDATE runtime_feature_flags SET enabled=false, version=2 WHERE name='handoffs_enabled';
		INSERT INTO runtime_feature_audit(version,name,enabled,actor,reason)
		VALUES(2,'handoffs_enabled',false,'operator','migration-cycle')`)
	require.NoError(t, err)

	assertV4Objects := func(expected bool) {
		t.Helper()
		for _, object := range []string{"runtime_feature_authority", "runtime_feature_flags", "runtime_feature_audit"} {
			var present bool
			require.NoError(t, pool.QueryRow(t.Context(), `SELECT to_regclass($1) IS NOT NULL`, object).Scan(&present))
			assert.Equal(t, expected, present, object)
		}
		if !expected {
			return
		}
		var triggers int
		require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM pg_trigger
			WHERE tgrelid='runtime_feature_audit'::regclass AND NOT tgisinternal
			  AND tgname IN ('runtime_feature_audit_append_only','runtime_feature_audit_reject_truncate')`).Scan(&triggers))
		assert.Equal(t, 2, triggers)
	}
	assertV4Objects(true)
	stepDownTo(t, pool, 3)
	assertV4Objects(false)
	var preserved int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM conversations WHERE tenant_id='tenant-v4-preserve' AND id='conversation-v4-preserve'`).Scan(&preserved))
	assert.Equal(t, 1, preserved)
	require.NoError(t, migrations.Up(t.Context(), pool))
	assertV4Objects(true)
	var authorityVersion, flags int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT version FROM runtime_feature_authority WHERE singleton=true`).Scan(&authorityVersion))
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM runtime_feature_flags`).Scan(&flags))
	assert.Equal(t, 1, authorityVersion)
	assert.Equal(t, 2, flags)
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM conversations WHERE tenant_id='tenant-v4-preserve' AND id='conversation-v4-preserve'`).Scan(&preserved))
	assert.Equal(t, 1, preserved)
}

func TestSchemaTask21DeletionProvenanceV5DownUpPreservesPriorDurableRows(t *testing.T) {
	pool := migratedPostgres(t)
	insertConversation(t, pool, "tenant-v5-preserve", "conversation-v5-preserve")
	_, err := pool.Exec(t.Context(), `UPDATE conversations
		SET status='deleted',deletion_actor='operator',deletion_reason_code='user_request',
			deletion_requested_at=CURRENT_TIMESTAMP
		WHERE tenant_id='tenant-v5-preserve' AND id='conversation-v5-preserve'`)
	require.NoError(t, err)

	stepDownTo(t, pool, 4)
	version, dirty, err := migrations.Version(t.Context(), pool)
	require.NoError(t, err)
	assert.Equal(t, uint(4), version)
	assert.False(t, dirty)
	var preserved int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM conversations
		WHERE tenant_id='tenant-v5-preserve' AND id='conversation-v5-preserve' AND status='deleted'`).Scan(&preserved))
	assert.Equal(t, 1, preserved)
	var provenanceColumnPresent bool
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema=current_schema() AND table_name='conversations' AND column_name='deletion_actor'
	)`).Scan(&provenanceColumnPresent))
	assert.False(t, provenanceColumnPresent)

	require.NoError(t, migrations.Up(t.Context(), pool))
	require.NoError(t, migrations.Check(t.Context(), pool))
	var actor, reason *string
	var requestedAt *time.Time
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT deletion_actor,deletion_reason_code,deletion_requested_at
		FROM conversations WHERE tenant_id='tenant-v5-preserve' AND id='conversation-v5-preserve'`).Scan(&actor, &reason, &requestedAt))
	assert.Nil(t, actor)
	assert.Nil(t, reason)
	assert.Nil(t, requestedAt)
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

	stepDownTo(t, pool, 2)
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

	stepDownTo(t, pool, 3)
	err := migrations.Steps(t.Context(), pool, -1)
	require.ErrorIs(t, err, migrations.ErrToolInvocationScopeConflict)
	version, dirty, versionErr := migrations.Version(t.Context(), pool)
	require.NoError(t, versionErr)
	assert.Equal(t, uint(3), version)
	assert.False(t, dirty)
	assert.Error(t, migrations.Check(t.Context(), pool))
	assertToolInvocationStableConstraintColumns(t, pool, []string{
		"tenant_id", "conversation_id", "handoff_id", "objective_revision",
		"tool_ordinal", "tool_name", "canonical_arguments_hash",
	})
	var count int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM tool_invocations WHERE tenant_id='tenant-conflict'`).Scan(&count))
	assert.Equal(t, 2, count)
	require.NoError(t, migrations.Up(t.Context(), pool))
	require.NoError(t, migrations.Check(t.Context(), pool))
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
	stepDownTo(t, pool, 2)
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
		{"invalid profile snapshot", "handoffs_profile_snapshot_check", `UPDATE handoffs SET profile_id = '' WHERE id = 'handoff-a'`},
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
	_, err = pool.Exec(t.Context(), `INSERT INTO coordinator_delivery_shards
		(delivery_affinity,lease_owner,lease_until,lease_generation)
		VALUES ('app-c','worker-c',CURRENT_TIMESTAMP+interval '1 minute',1)`)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `INSERT INTO session_reconnect_owners (
		session_id,tenant_id,user_id,conversation_id,incarnation,event_cursor,provider_model,response_epoch,
		credential_id,owner_id,owner_state,expires_at,owner_lease_expires_at)
		VALUES ('reconnect-c','tenant-c','user-c','conversation-c',1,0,'model-c',0,
		'credential-c','owner-c','active',CURRENT_TIMESTAMP+interval '5 minutes',CURRENT_TIMESTAMP+interval '1 minute')`)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `INSERT INTO knowledge_versions
		(tenant_id,pack_id,version,content_hash,title,media_type,excerpt_count,content_bytes,published_at,published_by)
		VALUES ('tenant-c','pack-c',1,$1,'policy.md','text/markdown',1,4,CURRENT_TIMESTAMP,'operator')`,
		strings.Repeat("a", 64))
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `INSERT INTO knowledge_excerpts
		(tenant_id,pack_id,version,ordinal,body,body_bytes,excerpt_hash,tokens,source_title,section,page)
		VALUES ('tenant-c','pack-c',1,0,'body',4,$1,ARRAY['body'],'policy.md','',0)`,
		strings.Repeat("b", 64))
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `INSERT INTO profile_blobs
		(tenant_id,content_hash,media_type,content,content_bytes,created_at)
		VALUES ('tenant-c',$1,'text/plain; charset=utf-8','x'::bytea,1,CURRENT_TIMESTAMP)`, strings.Repeat("a", 64))
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `INSERT INTO profile_drafts
		(tenant_id,profile_id,generation,manifest,updated_at,updated_by)
		VALUES ('tenant-c','profile-c',1,'{}',CURRENT_TIMESTAMP,'editor-c')`)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `INSERT INTO profile_versions
		(tenant_id,profile_id,version,draft_generation,engine_version,snapshot_hash,manifest,snapshot,
		 active,published_at,published_by)
		VALUES ('tenant-c','profile-c',1,1,'jinja-go-v1/test-engine-v1',$1,'{}',
			json_build_object('tenant_id','tenant-c','ref',json_build_object('id','profile-c','version',1),
				'engine_version','jinja-go-v1/test-engine-v1','snapshot_hash',$1::text),false,CURRENT_TIMESTAMP,'publisher-c')`, strings.Repeat("b", 64))
	require.NoError(t, err)

	mutations := map[string]string{
		"profile_blobs_identity_check":                             `UPDATE profile_blobs SET tenant_id='' WHERE tenant_id='tenant-c'`,
		"profile_blobs_media_type_check":                           `UPDATE profile_blobs SET media_type='' WHERE tenant_id='tenant-c'`,
		"profile_blobs_content_check":                              `UPDATE profile_blobs SET content_bytes=2 WHERE tenant_id='tenant-c'`,
		"profile_blobs_created_at_check":                           `UPDATE profile_blobs SET created_at='infinity' WHERE tenant_id='tenant-c'`,
		"profile_drafts_identity_check":                            `UPDATE profile_drafts SET profile_id='' WHERE profile_id='profile-c'`,
		"profile_drafts_generation_check":                          `UPDATE profile_drafts SET generation=0 WHERE profile_id='profile-c'`,
		"profile_drafts_manifest_check":                            `UPDATE profile_drafts SET manifest='[]' WHERE profile_id='profile-c'`,
		"profile_drafts_updated_by_check":                          `UPDATE profile_drafts SET updated_by='' WHERE profile_id='profile-c'`,
		"profile_drafts_updated_at_check":                          `UPDATE profile_drafts SET updated_at='infinity' WHERE profile_id='profile-c'`,
		"profile_versions_identity_check":                          `UPDATE profile_versions SET profile_id='' WHERE profile_id='profile-c'`,
		"profile_versions_numbers_check":                           `UPDATE profile_versions SET version=0 WHERE profile_id='profile-c'`,
		"profile_versions_engine_hash_check":                       `UPDATE profile_versions SET engine_version='' WHERE profile_id='profile-c'`,
		"profile_versions_documents_check":                         `UPDATE profile_versions SET snapshot='[]' WHERE profile_id='profile-c'`,
		"profile_versions_publication_check":                       `UPDATE profile_versions SET published_by='' WHERE profile_id='profile-c'`,
		"profile_versions_activation_check":                        `UPDATE profile_versions SET activated_by='operator' WHERE profile_id='profile-c'`,
		"profile_versions_revocation_check":                        `UPDATE profile_versions SET revoked_by='operator' WHERE profile_id='profile-c'`,
		"profile_versions_chronology_check":                        `UPDATE profile_versions SET activated_at=published_at+interval '2 minutes',activated_by='operator',revoked_at=published_at+interval '1 minute',revoked_by='operator' WHERE profile_id='profile-c'`,
		"profile_versions_snapshot_coherence_check":                `UPDATE profile_versions SET snapshot=json_build_object('tenant_id','tenant-other','ref',json_build_object('id','profile-c','version',1),'engine_version','jinja-go-v1/test-engine-v1','snapshot_hash',snapshot_hash) WHERE profile_id='profile-c'`,
		"knowledge_versions_identity_check":                        `UPDATE knowledge_versions SET pack_id='pack c' WHERE pack_id='pack-c'`,
		"knowledge_versions_version_check":                         `UPDATE knowledge_versions SET version=0 WHERE pack_id='pack-c'`,
		"knowledge_versions_hash_check":                            `UPDATE knowledge_versions SET content_hash='short' WHERE pack_id='pack-c'`,
		"knowledge_versions_title_check":                           `UPDATE knowledge_versions SET title='' WHERE pack_id='pack-c'`,
		"knowledge_versions_aggregate_check":                       `UPDATE knowledge_versions SET excerpt_count=2049 WHERE pack_id='pack-c'`,
		"knowledge_versions_publication_check":                     `UPDATE knowledge_versions SET published_by='' WHERE pack_id='pack-c'`,
		"knowledge_versions_tombstone_check":                       `UPDATE knowledge_versions SET tombstoned_at=CURRENT_TIMESTAMP WHERE pack_id='pack-c'`,
		"knowledge_excerpts_ordinal_check":                         `UPDATE knowledge_excerpts SET ordinal=2048 WHERE pack_id='pack-c'`,
		"knowledge_excerpts_body_check":                            `UPDATE knowledge_excerpts SET body_bytes=5 WHERE pack_id='pack-c'`,
		"knowledge_excerpts_hash_check":                            `UPDATE knowledge_excerpts SET excerpt_hash='short' WHERE pack_id='pack-c'`,
		"knowledge_excerpts_tokens_check":                          `UPDATE knowledge_excerpts SET tokens=array_fill('x'::text, ARRAY[1025]) WHERE pack_id='pack-c'`,
		"knowledge_excerpts_citation_check":                        `UPDATE knowledge_excerpts SET source_title='' WHERE pack_id='pack-c'`,
		"profile_versions_state_check":                             `UPDATE profile_versions SET active=true WHERE profile_id='profile-c'`,
		"session_reconnect_owners_identifiers_check":               `UPDATE session_reconnect_owners SET owner_id='' WHERE session_id='reconnect-c'`,
		"session_reconnect_owners_counters_check":                  `UPDATE session_reconnect_owners SET incarnation=0 WHERE session_id='reconnect-c'`,
		"session_reconnect_owners_state_check":                     `UPDATE session_reconnect_owners SET owner_state='unknown' WHERE session_id='reconnect-c'`,
		"session_reconnect_owners_times_check":                     `UPDATE session_reconnect_owners SET expires_at=created_at WHERE session_id='reconnect-c'`,
		"session_reconnect_owners_previous_check":                  `UPDATE session_reconnect_owners SET previous_credential_id='previous' WHERE session_id='reconnect-c'`,
		"conversations_tenant_id_size_check":                       `UPDATE conversations SET tenant_id='' WHERE tenant_id='tenant-free'`,
		"conversations_id_size_check":                              `UPDATE conversations SET id='' WHERE tenant_id='tenant-free'`,
		"conversations_user_id_size_check":                         `UPDATE conversations SET user_id='' WHERE tenant_id='tenant-free'`,
		"conversations_status_check":                               `UPDATE conversations SET status='unknown' WHERE tenant_id='tenant-free'`,
		"conversations_retention_check":                            `UPDATE conversations SET retention_until=created_at-interval '1 second' WHERE tenant_id='tenant-free'`,
		"conversations_timestamps_check":                           `UPDATE conversations SET updated_at=created_at-interval '1 second' WHERE tenant_id='tenant-free'`,
		"conversations_deletion_actor_size_check":                  `UPDATE conversations SET deletion_actor='',deletion_reason_code='reason',deletion_requested_at=CURRENT_TIMESTAMP WHERE tenant_id='tenant-free'`,
		"conversations_deletion_reason_size_check":                 `UPDATE conversations SET deletion_actor='actor',deletion_reason_code='',deletion_requested_at=CURRENT_TIMESTAMP WHERE tenant_id='tenant-free'`,
		"conversations_deletion_tuple_check":                       `UPDATE conversations SET deletion_actor='actor' WHERE tenant_id='tenant-free'`,
		"conversation_items_tenant_id_size_check":                  `UPDATE conversation_items SET tenant_id='' WHERE session_id='session-c'`,
		"conversation_items_conversation_id_size_check":            `UPDATE conversation_items SET conversation_id='' WHERE session_id='session-c'`,
		"conversation_items_session_id_size_check":                 `UPDATE conversation_items SET session_id='' WHERE session_id='session-c'`,
		"conversation_items_turn_id_size_check":                    `UPDATE conversation_items SET turn_id='' WHERE session_id='session-c'`,
		"conversation_items_turn_revision_check":                   `UPDATE conversation_items SET turn_revision=-1 WHERE session_id='session-c'`,
		"conversation_items_sequence_check":                        `UPDATE conversation_items SET sequence=-1 WHERE session_id='session-c'`,
		"conversation_items_role_check":                            `UPDATE conversation_items SET role='unknown' WHERE session_id='session-c'`,
		"conversation_items_content_size_check":                    `UPDATE conversation_items SET content='' WHERE session_id='session-c'`,
		"conversation_items_content_hash_size_check":               `UPDATE conversation_items SET content_hash='' WHERE session_id='session-c'`,
		"conversation_items_timestamps_check":                      `UPDATE conversation_items SET updated_at=created_at-interval '1 second' WHERE session_id='session-c'`,
		"coordinator_work_journal_kind_check":                      `UPDATE coordinator_work_journal SET kind='unknown' WHERE work_key='check-work'`,
		"coordinator_work_journal_work_key_size":                   `UPDATE coordinator_work_journal SET work_key='' WHERE work_key='check-work'`,
		"coordinator_work_journal_identity_size":                   `UPDATE coordinator_work_journal SET delivery_affinity='' WHERE work_key='check-work'`,
		"coordinator_work_journal_incarnation_check":               `UPDATE coordinator_work_journal SET incarnation=0 WHERE work_key='check-work'`,
		"coordinator_work_journal_local_sequence_check":            `UPDATE coordinator_work_journal SET local_sequence=-1 WHERE work_key='check-work'`,
		"coordinator_work_journal_payload_size":                    `UPDATE coordinator_work_journal SET payload=to_json(repeat('x',262145)) WHERE work_key='check-work'`,
		"coordinator_work_journal_payload_depth_check":             `UPDATE coordinator_work_journal SET payload=(repeat('[',65)||repeat(']',65))::json WHERE work_key='check-work'`,
		"coordinator_work_journal_payload_hash_size":               `UPDATE coordinator_work_journal SET payload_hash='' WHERE work_key='check-work'`,
		"coordinator_work_journal_storage_sequence_check":          `UPDATE coordinator_work_journal SET storage_sequence=1 WHERE work_key='check-work'`,
		"coordinator_work_journal_delivery_check":                  `UPDATE coordinator_work_journal SET delivered=true WHERE work_key='check-work'`,
		"coordinator_work_journal_claim_check":                     `UPDATE coordinator_work_journal SET claim_owner='worker' WHERE work_key='check-work'`,
		"coordinator_work_journal_claim_owner_size":                `UPDATE coordinator_work_journal SET claim_owner='',claim_until=CURRENT_TIMESTAMP+interval '1 minute',claim_affinity_generation=1 WHERE work_key='check-work'`,
		"coordinator_work_journal_claim_generation_check":          `UPDATE coordinator_work_journal SET claim_generation=-1 WHERE work_key='check-work'`,
		"coordinator_work_journal_claim_affinity_generation_check": `UPDATE coordinator_work_journal SET claim_owner='worker-c',claim_until=CURRENT_TIMESTAMP+interval '1 minute',claim_affinity_generation=0 WHERE work_key='check-work'`,
		"coordinator_delivery_shards_affinity_size_check":          `UPDATE coordinator_delivery_shards SET delivery_affinity='' WHERE delivery_affinity='app-c'`,
		"coordinator_delivery_shards_owner_size_check":             `UPDATE coordinator_delivery_shards SET lease_owner='' WHERE delivery_affinity='app-c'`,
		"coordinator_delivery_shards_generation_check":             `UPDATE coordinator_delivery_shards SET lease_generation=0 WHERE delivery_affinity='app-c'`,
		"coordinator_delivery_shards_timestamps_check":             `UPDATE coordinator_delivery_shards SET updated_at=lease_until WHERE delivery_affinity='app-c'`,
		"handoffs_tenant_id_size_check":                            `UPDATE handoffs SET tenant_id='' WHERE id='handoff-c'`,
		"handoffs_conversation_id_size_check":                      `UPDATE handoffs SET conversation_id='' WHERE id='handoff-c'`,
		"handoffs_id_size_check":                                   `UPDATE handoffs SET id='' WHERE id='handoff-c'`,
		"handoffs_user_id_size_check":                              `UPDATE handoffs SET user_id='' WHERE id='handoff-c'`,
		"handoffs_session_id_size_check":                           `UPDATE handoffs SET session_id='' WHERE id='handoff-c'`,
		"handoffs_source_turn_id_size_check":                       `UPDATE handoffs SET source_turn_id='' WHERE id='handoff-c'`,
		"handoffs_source_turn_revision_check":                      `UPDATE handoffs SET source_turn_revision=0 WHERE id='handoff-c'`,
		"handoffs_user_request_size_check":                         `UPDATE handoffs SET user_request='' WHERE id='handoff-c'`,
		"handoffs_objective_size_check":                            `UPDATE handoffs SET objective='' WHERE id='handoff-c'`,
		"handoffs_context_ref_size_check":                          `UPDATE handoffs SET context_ref='' WHERE id='handoff-c'`,
		"handoffs_context_hash_size_check":                         `UPDATE handoffs SET context_hash='' WHERE id='handoff-c'`,
		"handoffs_deadline_check":                                  `UPDATE handoffs SET deadline=created_at WHERE id='handoff-c'`,
		"handoffs_token_budget_check":                              `UPDATE handoffs SET token_budget=0 WHERE id='handoff-c'`,
		"handoffs_priority_check":                                  `UPDATE handoffs SET priority=1001 WHERE id='handoff-c'`,
		"handoffs_tool_allowlist_check":                            `UPDATE handoffs SET tool_allowlist='[1]' WHERE id='handoff-c'`,
		"handoffs_approval_policy_check":                           `UPDATE handoffs SET approval_policy=to_json(repeat('x',65535)) WHERE id='handoff-c'`,
		"handoffs_model_version_size_check":                        `UPDATE handoffs SET model_version='' WHERE id='handoff-c'`,
		"handoffs_prompt_version_size_check":                       `UPDATE handoffs SET prompt_version='' WHERE id='handoff-c'`,
		"handoffs_tool_registry_version_size_check":                `UPDATE handoffs SET tool_registry_version='' WHERE id='handoff-c'`,
		"handoffs_config_version_size_check":                       `UPDATE handoffs SET config_version='' WHERE id='handoff-c'`,
		"handoffs_profile_snapshot_check":                          `UPDATE handoffs SET profile_snapshot_hash='invalid' WHERE id='handoff-c'`,
		"handoffs_presentation_hint_size_check":                    `UPDATE handoffs SET presentation_hint=repeat('x',2049) WHERE id='handoff-c'`,
		"handoffs_idempotency_key_size_check":                      `UPDATE handoffs SET idempotency_key='' WHERE id='handoff-c'`,
		"handoffs_request_hash_size_check":                         `UPDATE handoffs SET request_hash='' WHERE id='handoff-c'`,
		"handoffs_state_check":                                     `UPDATE handoffs SET state='unknown' WHERE id='handoff-c'`,
		"handoffs_revision_check":                                  `UPDATE handoffs SET objective_revision=-1 WHERE id='handoff-c'`,
		"handoffs_input_check":                                     `UPDATE handoffs SET input_revision=1,input=NULL WHERE id='handoff-c'`,
		"handoffs_ready_at_check":                                  `UPDATE handoffs SET ready_at=created_at-interval '1 second' WHERE id='handoff-c'`,
		"handoffs_attempt_check":                                   `UPDATE handoffs SET attempt=-1 WHERE id='handoff-c'`,
		"handoffs_lease_tuple_check":                               `UPDATE handoffs SET lease_owner='worker' WHERE id='handoff-c'`,
		"handoffs_lease_owner_size_check":                          `UPDATE handoffs SET lease_owner='',lease_expires_at=CURRENT_TIMESTAMP WHERE id='handoff-c'`,
		"handoffs_cancel_tuple_check":                              `UPDATE handoffs SET cancel_actor='actor' WHERE id='handoff-c'`,
		"handoffs_cancel_actor_size_check":                         `UPDATE handoffs SET cancel_requested_at=CURRENT_TIMESTAMP,cancel_actor='' WHERE id='handoff-c'`,
		"handoffs_cancel_reason_size_check":                        `UPDATE handoffs SET cancel_requested_at=CURRENT_TIMESTAMP,cancel_actor='actor',cancel_reason=repeat('x',2049) WHERE id='handoff-c'`,
		"handoffs_terminal_mutation_id_size_check":                 `UPDATE handoffs SET terminal_mutation_id='' WHERE id='handoff-success'`,
		"handoffs_result_summary_size_check":                       `UPDATE handoffs SET result_summary='' WHERE id='handoff-success'`,
		"handoffs_structured_result_check":                         `UPDATE handoffs SET structured_result=to_json(repeat('x',65535)) WHERE id='handoff-success'`,
		"handoffs_failure_code_size_check":                         `UPDATE handoffs SET failure_code='' WHERE id='handoff-failed'`,
		"handoffs_failure_message_size_check":                      `UPDATE handoffs SET failure_message='' WHERE id='handoff-failed'`,
		"handoffs_safety_decision_check":                           `UPDATE handoffs SET safety_decision='[]' WHERE id='handoff-c'`,
		"handoffs_usage_check":                                     `UPDATE handoffs SET usage=to_json(repeat('x',65535)) WHERE id='handoff-success'`,
		"handoffs_terminal_fields_check":                           `UPDATE handoffs SET terminal_at=CURRENT_TIMESTAMP WHERE id='handoff-c'`,
		"handoffs_timestamps_check":                                `UPDATE handoffs SET updated_at=created_at-interval '1 second' WHERE id='handoff-c'`,
		"handoff_events_event_id_size_check":                       `UPDATE handoff_events SET event_id='' WHERE event_id='event-c'`,
		"handoff_events_kind_size_check":                           `UPDATE handoff_events SET kind='' WHERE event_id='event-c'`,
		"handoff_events_state_check":                               `UPDATE handoff_events SET state='unknown' WHERE event_id='event-c'`,
		"handoff_events_attempt_check":                             `UPDATE handoff_events SET attempt=-1 WHERE event_id='event-c'`,
		"handoff_events_payload_check":                             `UPDATE handoff_events SET payload='[]' WHERE event_id='event-c'`,
		"outbox_topic_size_check":                                  `UPDATE outbox SET topic='' WHERE event_seq=` + fmt.Sprint(sequence),
		"outbox_aggregate_id_size_check":                           `UPDATE outbox SET aggregate_id='' WHERE event_seq=` + fmt.Sprint(sequence),
		"outbox_event_seq_check":                                   `UPDATE outbox SET event_seq=-1 WHERE event_seq=` + fmt.Sprint(sequence),
		"outbox_payload_check":                                     `UPDATE outbox SET payload=json_build_object('tenant_id',repeat('x',8192)) WHERE event_seq=` + fmt.Sprint(sequence),
		"outbox_payload_keys_check":                                `UPDATE outbox SET payload='{"secret":"x"}' WHERE event_seq=` + fmt.Sprint(sequence),
		"outbox_attempts_check":                                    `UPDATE outbox SET attempts=-1 WHERE event_seq=` + fmt.Sprint(sequence),
		"outbox_last_error_size_check":                             `UPDATE outbox SET last_error=repeat('x',2049) WHERE event_seq=` + fmt.Sprint(sequence),
		"outbox_timestamps_check":                                  `UPDATE outbox SET published_at=created_at-interval '1 second' WHERE event_seq=` + fmt.Sprint(sequence),
		"tool_invocations_invocation_id_size_check":                `UPDATE tool_invocations SET invocation_id='' WHERE invocation_id='invocation-c'`,
		"tool_invocations_attempt_check":                           `UPDATE tool_invocations SET attempt=-1 WHERE invocation_id='invocation-c'`,
		"tool_invocations_objective_revision_check":                `UPDATE tool_invocations SET objective_revision=-1 WHERE invocation_id='invocation-c'`,
		"tool_invocations_tool_ordinal_check":                      `UPDATE tool_invocations SET tool_ordinal=-1 WHERE invocation_id='invocation-c'`,
		"tool_invocations_tool_name_size_check":                    `UPDATE tool_invocations SET tool_name='' WHERE invocation_id='invocation-c'`,
		"tool_invocations_approval_state_check":                    `UPDATE tool_invocations SET approval_state='unknown' WHERE invocation_id='invocation-c'`,
		"tool_invocations_idempotency_key_size_check":              `UPDATE tool_invocations SET idempotency_key='' WHERE invocation_id='invocation-c'`,
		"tool_invocations_arguments_hash_size_check":               `UPDATE tool_invocations SET canonical_arguments_hash='' WHERE invocation_id='invocation-c'`,
		"tool_invocations_ciphertext_check":                        `UPDATE tool_invocations SET arguments_ciphertext=decode(repeat('00',15),'hex') WHERE invocation_id='invocation-c'`,
		"tool_invocations_nonce_check":                             `UPDATE tool_invocations SET arguments_nonce=decode(repeat('00',11),'hex') WHERE invocation_id='invocation-c'`,
		"tool_invocations_key_id_size_check":                       `UPDATE tool_invocations SET arguments_key_id='' WHERE invocation_id='invocation-c'`,
		"tool_invocations_result_metadata_check":                   `UPDATE tool_invocations SET result_metadata='[]' WHERE invocation_id='invocation-c'`,
		"tool_invocations_result_check":                            `UPDATE tool_invocations SET result=to_json(repeat('x',65535)) WHERE invocation_id='invocation-c'`,
		"tool_invocations_mutation_id_size_check":                  `UPDATE tool_invocations SET mutation_id='' WHERE invocation_id='invocation-c'`,
		"tool_invocations_status_check":                            `UPDATE tool_invocations SET status='unknown' WHERE invocation_id='invocation-c'`,
		"tool_invocations_timestamps_check":                        `UPDATE tool_invocations SET updated_at=created_at-interval '1 second' WHERE invocation_id='invocation-c'`,
		"delivery_cursors_consumer_id_size_check":                  `UPDATE delivery_cursors SET consumer_id='' WHERE consumer_id='consumer-c'`,
		"delivery_cursors_sequence_check":                          `UPDATE delivery_cursors SET last_sequence=-1 WHERE consumer_id='consumer-c'`,
		"delivery_cursors_timestamps_check":                        `UPDATE delivery_cursors SET updated_at=created_at-interval '1 second' WHERE consumer_id='consumer-c'`,
		"session_admission_leases_instance_id_size_check":          `UPDATE session_admission_leases SET instance_id='' WHERE instance_id='instance-c'`,
		"session_admission_leases_session_id_size_check":           `UPDATE session_admission_leases SET session_id='' WHERE instance_id='instance-c'`,
		"session_admission_leases_tenant_id_size_check":            `UPDATE session_admission_leases SET tenant_id='' WHERE instance_id='instance-c'`,
		"session_admission_leases_user_id_size_check":              `UPDATE session_admission_leases SET user_id='' WHERE instance_id='instance-c'`,
		"session_admission_leases_timestamps_check":                `UPDATE session_admission_leases SET heartbeat_at=created_at-interval '1 second' WHERE instance_id='instance-c'`,
		"deletion_audits_audit_id_size_check":                      `UPDATE deletion_audits SET audit_id='' WHERE audit_id='audit-c'`,
		"deletion_audits_tenant_id_size_check":                     `UPDATE deletion_audits SET tenant_id='' WHERE audit_id='audit-c'`,
		"deletion_audits_conversation_hash_size_check":             `UPDATE deletion_audits SET conversation_hash='' WHERE audit_id='audit-c'`,
		"deletion_audits_actor_size_check":                         `UPDATE deletion_audits SET actor='' WHERE audit_id='audit-c'`,
		"deletion_audits_reason_code_size_check":                   `UPDATE deletion_audits SET reason_code='' WHERE audit_id='audit-c'`,
		"runtime_feature_authority_singleton_check":                `INSERT INTO runtime_feature_authority(singleton,version) VALUES(false,1)`,
		"runtime_feature_authority_version_check":                  `UPDATE runtime_feature_authority SET version=0 WHERE singleton=true`,
		"runtime_feature_flags_name_check":                         `INSERT INTO runtime_feature_flags(name,enabled,version) VALUES('unknown',false,1)`,
		"runtime_feature_flags_version_check":                      `UPDATE runtime_feature_flags SET version=0 WHERE name='shadow_mode'`,
		"runtime_feature_audit_name_check":                         `INSERT INTO runtime_feature_audit(version,name,enabled,actor,reason) VALUES(2,'unknown',false,'actor','reason')`,
		"runtime_feature_audit_actor_size_check":                   `INSERT INTO runtime_feature_audit(version,name,enabled,actor,reason) VALUES(2,'handoffs_enabled',false,'','reason')`,
		"runtime_feature_audit_reason_size_check":                  `INSERT INTO runtime_feature_audit(version,name,enabled,actor,reason) VALUES(2,'handoffs_enabled',false,'actor','')`,
		"runtime_feature_audit_version_check":                      `INSERT INTO runtime_feature_audit(version,name,enabled,actor,reason) VALUES(1,'handoffs_enabled',false,'actor','reason')`,
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
		unique    bool
	}
	expected := map[string]expectedIndex{
		"profile_one_active_version": {
			keys: []string{"tenant_id", "profile_id"}, options: []int16{0, 0}, predicate: "active", unique: true,
		},
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
		"session_reconnect_owners_expiry_idx": {
			keys: []string{"expires_at"}, options: []int16{0},
		},
		"session_reconnect_owners_cleanup_idx": {
			keys: []string{"owner_lease_expires_at", "expires_at"}, options: []int16{0, 0},
		},
	}
	for name, want := range expected {
		var keyCount, attributeCount int
		var keys []string
		var options []int16
		var predicate string
		var unique bool
		require.NoError(t, pool.QueryRow(t.Context(), `
			SELECT indnkeyatts, indnatts, indisunique,
				ARRAY(SELECT pg_get_indexdef(indexrelid, position, false)
					FROM generate_series(1, indnatts) AS positions(position) ORDER BY position),
				indoption::int2[],
				COALESCE(pg_get_expr(indpred, indrelid), '')
			FROM pg_index WHERE indexrelid = to_regclass($1)`, name).
			Scan(&keyCount, &attributeCount, &unique, &keys, &options, &predicate), name)
		assert.Equal(t, len(want.keys), keyCount, "%s key count", name)
		assert.Equal(t, keyCount, attributeCount, "%s must not gain unreviewed INCLUDE columns", name)
		assert.Equal(t, want.keys, keys, "%s ordered key expressions", name)
		assert.Equal(t, want.options, options, "%s ordering/null-position options", name)
		assert.Equal(t, want.predicate, predicate, "%s normalized predicate", name)
		assert.Equal(t, want.unique, unique, "%s uniqueness", name)
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
		"profile_versions.revoked_at": true, "profile_versions.revoked_by": true,
		"profile_versions.activated_at": true, "profile_versions.activated_by": true,
		"conversations.deletion_actor": true, "conversations.deletion_reason_code": true,
		"conversations.deletion_requested_at":         true,
		"session_reconnect_owners.subject_expires_at": true, "session_reconnect_owners.previous_credential_id": true,
		"session_reconnect_owners.previous_incarnation":  true,
		"session_reconnect_owners.previous_event_cursor": true, "session_reconnect_owners.previous_expires_at": true,
		"session_reconnect_owners.previous_accept_until": true,
		"handoffs.input": true, "handoffs.lease_owner": true, "handoffs.lease_expires_at": true,
		"handoffs.cancel_requested_at": true, "handoffs.cancel_actor": true, "handoffs.cancel_reason": true,
		"handoffs.terminal_mutation_id": true, "handoffs.result_summary": true, "handoffs.structured_result": true,
		"handoffs.failure_code": true, "handoffs.failure_retryable": true, "handoffs.failure_message": true,
		"handoffs.safety_decision": true, "handoffs.usage": true, "handoffs.terminal_at": true,
		"outbox.published_at": true, "outbox.last_error": true,
		"tool_invocations.approval_expires_at": true, "tool_invocations.result_metadata": true,
		"tool_invocations.result": true, "tool_invocations.mutation_id": true,
		"coordinator_work_journal.storage_sequence": true, "coordinator_work_journal.claim_owner": true,
		"coordinator_work_journal.claim_until": true, "coordinator_work_journal.claim_affinity_generation": true,
		"coordinator_work_journal.delivered_at": true,
		"knowledge_versions.tombstoned_at":      true, "knowledge_versions.tombstoned_by": true,
	}
	return allowed[key]
}

func profileTestCatalogTime() time.Time {
	return time.Date(2026, 8, 15, 10, 0, 0, 0, time.UTC)
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

func TestSchemaKnowledgeV10UpDownUpCatalogAndConstraints(t *testing.T) {
	pool := testkit.Postgres(t)
	require.NoError(t, migrations.Up(t.Context(), pool))

	for _, table := range []string{"knowledge_versions", "knowledge_excerpts"} {
		var present bool
		require.NoError(t, pool.QueryRow(t.Context(), `SELECT to_regclass($1) IS NOT NULL`, table).Scan(&present))
		assert.True(t, present, table)
	}

	insertVersion := func(hash string, count int, bytes int) error {
		_, err := pool.Exec(t.Context(), `INSERT INTO knowledge_versions
			(tenant_id,pack_id,version,content_hash,title,media_type,excerpt_count,content_bytes,published_at,published_by)
			VALUES ('tenant-a','policy',1,$1,'policy.md','text/markdown',$2,$3,now(),'operator')`, hash, count, bytes)
		return err
	}
	require.NoError(t, insertVersion(strings.Repeat("a", 64), 1, 10))

	// A tenant or pack identity carrying whitespace never reaches a key.
	_, err := pool.Exec(t.Context(), `INSERT INTO knowledge_versions
		(tenant_id,pack_id,version,content_hash,title,media_type,excerpt_count,content_bytes,published_at,published_by)
		VALUES ('tenant a','policy',9,$1,'policy.md','text/markdown',1,10,now(),'operator')`, strings.Repeat("b", 64))
	assertConstraint(t, err, "knowledge_versions_identity_check")

	// Aggregate bounds are enforced in SQL, not only in Go.
	_, err = pool.Exec(t.Context(), `INSERT INTO knowledge_versions
		(tenant_id,pack_id,version,content_hash,title,media_type,excerpt_count,content_bytes,published_at,published_by)
		VALUES ('tenant-a','policy',2,$1,'policy.md','text/markdown',2049,10,now(),'operator')`, strings.Repeat("c", 64))
	assertConstraint(t, err, "knowledge_versions_aggregate_check")

	// A tombstone must carry both a time and an actor.
	_, err = pool.Exec(t.Context(), `UPDATE knowledge_versions SET tombstoned_at=now() WHERE tenant_id='tenant-a'`)
	assertConstraint(t, err, "knowledge_versions_tombstone_check")

	// Excerpts cannot outlive the version they cite.
	_, err = pool.Exec(t.Context(), `INSERT INTO knowledge_excerpts
		(tenant_id,pack_id,version,ordinal,body,body_bytes,excerpt_hash,tokens,source_title,section,page)
		VALUES ('tenant-a','absent',1,0,'body',4,$1,ARRAY['body'],'policy.md','',0)`, strings.Repeat("d", 64))
	assertConstraint(t, err, "knowledge_excerpts_version_fkey")

	_, err = pool.Exec(t.Context(), `INSERT INTO knowledge_excerpts
		(tenant_id,pack_id,version,ordinal,body,body_bytes,excerpt_hash,tokens,source_title,section,page)
		VALUES ('tenant-a','policy',1,0,'body',5,$1,ARRAY['body'],'policy.md','',0)`, strings.Repeat("d", 64))
	assertConstraint(t, err, "knowledge_excerpts_body_check")

	// The schema now has v11 handoff snapshot persistence above the v10
	// knowledge catalog. Roll back both steps so this test still exercises the
	// exact v10 down migration rather than merely landing on v10.
	require.NoError(t, migrations.Steps(t.Context(), pool, -1))
	require.NoError(t, migrations.Steps(t.Context(), pool, -1))
	version, dirty, err := migrations.Version(t.Context(), pool)
	require.NoError(t, err)
	assert.Equal(t, uint(9), version)
	assert.False(t, dirty)
	var present bool
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT to_regclass('knowledge_versions') IS NOT NULL`).Scan(&present))
	assert.False(t, present)

	require.NoError(t, migrations.Up(t.Context(), pool))
	require.NoError(t, migrations.Check(t.Context(), pool))
}
