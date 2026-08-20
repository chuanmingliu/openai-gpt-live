//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/admission"
	"github.com/chuanmingliu/openai-gpt-live/internal/auth"
	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/feature"
	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/reasoner"
	"github.com/chuanmingliu/openai-gpt-live/internal/safety"
	"github.com/chuanmingliu/openai-gpt-live/internal/session"
	"github.com/chuanmingliu/openai-gpt-live/internal/store/postgres"
	"github.com/chuanmingliu/openai-gpt-live/internal/testkit"
	"github.com/chuanmingliu/openai-gpt-live/internal/toolpolicy"
	"github.com/chuanmingliu/openai-gpt-live/internal/worker"
	"github.com/chuanmingliu/openai-gpt-live/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type sequenceHandoffIDs struct{ suffix string }

func (i sequenceHandoffIDs) NewHandoffID() identity.HandoffID {
	return identity.HandoffID("handoff-" + i.suffix)
}
func (i sequenceHandoffIDs) NewEventID() string { return "event-" + i.suffix }

type recoveryHarness struct {
	pool *pgxpool.Pool
	repo *postgres.Repository
	now  *clock.Manual
}

func newRecoveryHarness(t *testing.T, options ...postgres.RepositoryOption) recoveryHarness {
	t.Helper()
	pool := testkit.Postgres(t)
	require.NoError(t, migrations.Up(t.Context(), pool))
	now := clock.NewManual(time.Now().UTC().Truncate(time.Microsecond))
	repo, err := postgres.NewRepository(pool, now, options...)
	require.NoError(t, err)
	return recoveryHarness{pool: pool, repo: repo, now: now}
}

func (h recoveryHarness) create(t *testing.T, suffix string) (handoff.Service, handoff.Scope, handoff.Snapshot) {
	t.Helper()
	scope := handoff.Scope{TenantID: "tenant-" + suffix, UserID: "user-" + suffix, ConversationID: identity.ConversationID("conversation-" + suffix)}
	_, err := h.pool.Exec(t.Context(), `INSERT INTO conversations(tenant_id,id,user_id,retention_until,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$5)`,
		scope.TenantID, scope.ConversationID, scope.UserID, h.now.Now().Add(24*time.Hour), h.now.Now())
	require.NoError(t, err)
	text := "bounded-" + suffix
	digest := sha256.Sum256([]byte(text))
	_, err = h.repo.AppendCommittedItem(t.Context(), scope, postgres.ConversationItem{
		SessionID: identity.SessionID("session-" + suffix), Turn: identity.TurnKey{ID: "turn-" + suffix, Revision: 1}, Sequence: 1,
		Role: "user", Content: text, ContentHash: hex.EncodeToString(digest[:]), CreatedAt: h.now.Now(),
	})
	require.NoError(t, err)
	service, err := handoff.NewService(h.repo, h.now, sequenceHandoffIDs{suffix: suffix})
	require.NoError(t, err)
	policy := toolpolicy.DefaultPolicy()
	policy.WebSearch.Enabled = false
	policyRaw, err := toolpolicy.MarshalCanonical(policy)
	require.NoError(t, err)
	snapshot, err := service.Create(t.Context(), handoff.CreateRequest{
		Scope: scope, SessionID: identity.SessionID("session-" + suffix), SourceTurn: identity.TurnKey{ID: "turn-" + suffix, Revision: 1},
		UserRequest: text, Objective: "objective-" + suffix, ContextRef: "context://" + suffix, ContextHash: "sha256:context-" + suffix,
		Deadline: h.now.Now().Add(time.Hour), TokenBudget: 4096, Priority: 12,
		ToolAllowlist: []string{"fetch", "search"}, ToolPolicy: policyRaw, ApprovalPolicy: json.RawMessage(`{"mode":"ask"}`),
		Versions:         handoff.Versions{Model: "model-v1", Prompt: "prompt-v2", ToolRegistry: "tools-v3", Config: "config-v4"},
		Profile:          handoff.LegacyProfileSnapshot(),
		PresentationHint: "compact", IdempotencyKey: "key-" + suffix,
	})
	require.NoError(t, err)
	return service, scope, snapshot
}

func TestPostgresServicePersistsSteeringAndCancelRequest(t *testing.T) {
	h := newRecoveryHarness(t)
	service, scope, snapshot := h.create(t, "steer")
	steered, err := service.Steer(t.Context(), scope, snapshot.ID, handoff.SteerRequest{
		Objective: "revised-bounded-objective", ExpectedRevision: snapshot.ObjectiveRevision + 1,
		Actor: scope.UserID, IdempotencyKey: "steer-key",
	})
	require.NoError(t, err)
	assert.Equal(t, snapshot.ObjectiveRevision+1, steered.ObjectiveRevision)
	cancelled, err := service.Cancel(t.Context(), scope, snapshot.ID, handoff.CancelRequest{
		Actor: scope.UserID, Reason: "bounded cancellation", IdempotencyKey: "cancel-key",
	})
	require.NoError(t, err)
	assert.Equal(t, handoff.StatePending, cancelled.State, "the durable request remains active for a worker to terminalize")
	reloaded, err := service.Get(t.Context(), scope, snapshot.ID)
	require.NoError(t, err)
	assert.Equal(t, handoff.StatePending, reloaded.State)
	events, err := h.repo.ReadEvents(t.Context(), scope, 0, 16)
	require.NoError(t, err)
	require.Len(t, events, 3)
	assert.Equal(t, "handoff.cancel_requested", events[2].Kind)
}

func TestPostgresServiceCreateIsIdempotentAfterAmbiguousCallerOutcome(t *testing.T) {
	h := newRecoveryHarness(t)
	_, scope, first := h.create(t, "post-persist")
	replay, err := handoff.NewService(h.repo, h.now, sequenceHandoffIDs{suffix: "different-id"})
	require.NoError(t, err)
	replayed, err := replay.Create(t.Context(), first.Request)
	require.NoError(t, err)
	assert.Equal(t, first.ID, replayed.ID)
	events, err := h.repo.ReadEvents(t.Context(), scope, 0, 16)
	require.NoError(t, err)
	assert.Len(t, events, 1)
}

func TestPostgresLeaseExpiryFencesOldOwner(t *testing.T) {
	h := newRecoveryHarness(t)
	_, _, snapshot := h.create(t, "lease-recovery")
	first, ok, err := h.repo.LeaseNext(t.Context(), worker.LeaseRequest{WorkerID: "worker-old", LeaseDuration: time.Second, Now: h.now.Now()})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, snapshot.ID, first.Job.ID)
	h.now.Advance(time.Second)
	second, ok, err := h.repo.LeaseNext(t.Context(), worker.LeaseRequest{WorkerID: "worker-new", LeaseDuration: time.Second, Now: h.now.Now()})
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, first.Token.HandoffID, second.Token.HandoffID)
	assert.Greater(t, second.Token.Version, first.Token.Version)
	assert.ErrorIs(t, h.repo.MarkRunning(t.Context(), first.Token, h.now.Now()), worker.ErrLeaseLost)
}

func TestPostgresDuplicateCompletionCreatesOneTerminalEvent(t *testing.T) {
	h := newRecoveryHarness(t)
	_, scope, _ := h.create(t, "duplicate-complete")
	lease, ok, err := h.repo.LeaseNext(t.Context(), worker.LeaseRequest{WorkerID: "worker-duplicate", LeaseDuration: time.Minute, Now: h.now.Now()})
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, h.repo.MarkRunning(t.Context(), lease.Token, h.now.Now()))
	record := worker.CompletionRecord{ExpectedObjectiveRevision: lease.Job.ObjectiveRevision, Result: handoff.Result{
		MutationID: "completion-once", Summary: "bounded summary", Structured: json.RawMessage(`{"ok":true}`),
		SafetyDecision: json.RawMessage(`{"action":"allow","reason_code":"test"}`), Usage: json.RawMessage(`{"total_tokens":2}`),
	}}
	require.NoError(t, h.repo.Complete(t.Context(), lease.Token, record))
	require.NoError(t, h.repo.Complete(t.Context(), lease.Token, record))
	events, err := h.repo.ReadEvents(t.Context(), scope, 0, 16)
	require.NoError(t, err)
	terminal := 0
	for _, event := range events {
		if event.State == handoff.StateSucceeded {
			terminal++
		}
	}
	assert.Equal(t, 1, terminal)
}

func TestPostgresDeliveryCursorReadsDurableSuffixInOrder(t *testing.T) {
	h := newRecoveryHarness(t)
	_, scope, _ := h.create(t, "lost-notification")
	events, err := h.repo.ReadEvents(t.Context(), scope, 0, 16)
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, int64(0), mustCursor(t, h.repo, scope))
	require.NoError(t, h.repo.AdvanceDeliveryCursor(t.Context(), "consumer-replay", scope, events[0].Seq))
	assert.Equal(t, events[0].Seq, mustCursor(t, h.repo, scope))
	remaining, err := h.repo.ReadEvents(t.Context(), scope, mustCursor(t, h.repo, scope), 16)
	require.NoError(t, err)
	assert.Empty(t, remaining)
}

func mustCursor(t *testing.T, repo *postgres.Repository, scope handoff.Scope) int64 {
	t.Helper()
	value, err := repo.LoadDeliveryCursor(t.Context(), "consumer-replay", scope)
	require.NoError(t, err)
	return value
}

func TestPostgresEventReadsAreScopeIsolated(t *testing.T) {
	h := newRecoveryHarness(t)
	_, firstScope, _ := h.create(t, "isolation-a")
	_, secondScope, _ := h.create(t, "isolation-b")
	first, err := h.repo.ReadEvents(t.Context(), firstScope, 0, 16)
	require.NoError(t, err)
	second, err := h.repo.ReadEvents(t.Context(), secondScope, 0, 16)
	require.NoError(t, err)
	require.Len(t, first, 1)
	require.Len(t, second, 1)
	assert.NotEqual(t, first[0].TenantID, second[0].TenantID)
	assert.NotEqual(t, first[0].HandoffID, second[0].HandoffID)
}

func TestApprovalResolutionValidationRejectsIncompleteRecords(t *testing.T) {
	base := handoff.ApprovalResolution{InvocationID: "invocation", Decision: handoff.ApprovalDenied,
		ActorTenantID: "tenant", ActorUserID: "user", Reason: "bounded", IdempotencyKey: "decision", ResolvedAt: time.Now().UTC()}
	require.NoError(t, handoff.ValidateApprovalResolution(base))
	for _, mutate := range []func(*handoff.ApprovalResolution){
		func(value *handoff.ApprovalResolution) { value.Decision = "" },
		func(value *handoff.ApprovalResolution) { value.ResolvedAt = time.Time{} },
		func(value *handoff.ApprovalResolution) { value.IdempotencyKey = "" },
	} {
		invalid := base
		mutate(&invalid)
		assert.ErrorIs(t, handoff.ValidateApprovalResolution(invalid), handoff.ErrInvalidRequest)
	}
}

func TestPostgresDeletionTombstoneRejectsRecovery(t *testing.T) {
	h := newRecoveryHarness(t, postgres.WithRetentionHMAC("test-key", []byte(strings.Repeat("k", 32))))
	_, scope, _ := h.create(t, "retention")
	require.NoError(t, h.repo.MarkConversationDeleting(t.Context(), scope, scope.UserID, handoff.DeletionReasonUserRequested))
	_, err := h.repo.LoadRecovery(t.Context(), scope, session.DefaultRecoveryItemLimit, session.DefaultRecoveryByteLimit)
	assert.ErrorIs(t, err, handoff.ErrNotFound)
}

func TestRetentionUserAndTTLDeletionProtectLeaseCascadeChildrenAndFenceClients(t *testing.T) {
	tool := worker.RegisteredTool{
		Definition: reasoner.ToolDefinition{Name: "fetch", Parameters: json.RawMessage(`{"type":"object"}`)},
		ReadOnly:   true, ParallelSafe: true, StableIdempotency: true,
		Executor: toolExecutor(func(context.Context, worker.ToolExecutionRequest) (json.RawMessage, error) {
			return json.RawMessage(`{"ok":true}`), nil
		}),
	}
	registry, err := worker.NewToolRegistry(map[string][]worker.RegisteredTool{"tools-v3": {tool}})
	require.NoError(t, err)
	h := newRecoveryHarness(t, postgres.WithToolGate(registry), postgres.WithRetentionHMAC("task22-retention", []byte(strings.Repeat("r", 32))))
	_, userScope, userSnapshot := h.create(t, "retention-user-boundary")
	_, err = h.pool.Exec(t.Context(), `UPDATE handoffs SET approval_policy='{"mode":"none"}'::json WHERE tenant_id=$1 AND conversation_id=$2`, userScope.TenantID, userScope.ConversationID)
	require.NoError(t, err)

	lease, ok, err := h.repo.LeaseNext(t.Context(), worker.LeaseRequest{WorkerID: "retention-boundary-worker", LeaseDuration: time.Minute, Now: h.now.Now()})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, userSnapshot.ID, lease.Token.HandoffID)
	require.NoError(t, h.repo.MarkRunning(t.Context(), lease.Token, h.now.Now()))
	arguments := json.RawMessage(`{"private":"delete-me"}`)
	digest := sha256.Sum256(arguments)
	sealer, err := worker.NewAESArgumentSealer("task22-tool-key", bytes.Repeat([]byte{4}, 32), bytes.NewReader(bytes.Repeat([]byte{7}, 12)))
	require.NoError(t, err)
	sealed, err := sealer.Seal(arguments)
	require.NoError(t, err)
	invocationID := worker.StableToolInvocationID(lease.Token.TenantID, lease.Token.ConversationID, lease.Token.HandoffID, lease.Job.ObjectiveRevision, 1, "fetch", hex.EncodeToString(digest[:]))
	_, created, err := h.repo.BeginToolInvocation(t.Context(), lease.Token, worker.ToolInvocationRecord{
		InvocationID: invocationID, ObjectiveRevision: lease.Job.ObjectiveRevision, ToolOrdinal: 1, ToolName: "fetch", IdempotencyKey: invocationID,
		ArgumentsHash: hex.EncodeToString(digest[:]), Ciphertext: sealed.Ciphertext, Nonce: sealed.Nonce, KeyID: sealed.KeyID,
	})
	require.NoError(t, err)
	require.True(t, created)
	_, ttlScope, _ := h.create(t, "retention-ttl-boundary")
	_, err = h.pool.Exec(t.Context(), `UPDATE conversations SET retention_until=$3 WHERE tenant_id=$1 AND id=$2`, ttlScope.TenantID, ttlScope.ConversationID, h.now.Now())
	require.NoError(t, err)
	events, err := h.repo.ReadEvents(t.Context(), userScope, 0, 32)
	require.NoError(t, err)
	require.NotEmpty(t, events)
	require.NoError(t, h.repo.AdvanceDeliveryCursor(t.Context(), "retention-boundary-consumer", userScope, events[0].Seq))
	_, err = h.pool.Exec(t.Context(), `INSERT INTO coordinator_work_journal(work_key,kind,tenant_id,user_id,conversation_id,session_id,
		delivery_affinity,incarnation,local_sequence,payload,payload_hash,created_at)
		VALUES('retention-boundary-journal','handoff',$1,$2,$3,'retention-boundary-session','retention-boundary-affinity',1,1,
		'{"private":"delete-me"}'::json,repeat('b',64),$4)`, userScope.TenantID, userScope.UserID, userScope.ConversationID, h.now.Now())
	require.NoError(t, err)
	outboxRows, err := h.pool.Query(t.Context(), `SELECT o.id FROM outbox o JOIN handoff_events e ON e.seq=o.event_seq
		WHERE e.tenant_id=$1 AND e.conversation_id=$2`, userScope.TenantID, userScope.ConversationID)
	require.NoError(t, err)
	var outboxIDs []int64
	for outboxRows.Next() {
		var id int64
		require.NoError(t, outboxRows.Scan(&id))
		outboxIDs = append(outboxIDs, id)
	}
	require.NoError(t, outboxRows.Err())
	outboxRows.Close()
	require.NotEmpty(t, outboxIDs)

	require.NoError(t, h.repo.MarkConversationDeleting(t.Context(), userScope, userScope.UserID, handoff.DeletionReasonUserRequested))
	_, err = h.repo.LoadRecovery(t.Context(), userScope, session.DefaultRecoveryItemLimit, session.DefaultRecoveryByteLimit)
	assert.ErrorIs(t, err, handoff.ErrNotFound, "the deletion tombstone must immediately fence reconnecting clients")
	deleted, err := h.repo.DeleteReadyConversations(t.Context(), h.now.Now().Add(30*time.Second), 10)
	require.NoError(t, err)
	assert.Equal(t, 1, deleted, "the TTL-expired conversation is ready while the user-deleted conversation has an active lease")
	var userRemaining, ttlRemaining int
	require.NoError(t, h.pool.QueryRow(t.Context(), `SELECT count(*) FROM conversations WHERE tenant_id=$1 AND id=$2`, userScope.TenantID, userScope.ConversationID).Scan(&userRemaining))
	require.NoError(t, h.pool.QueryRow(t.Context(), `SELECT count(*) FROM conversations WHERE tenant_id=$1 AND id=$2`, ttlScope.TenantID, ttlScope.ConversationID).Scan(&ttlRemaining))
	assert.Equal(t, 1, userRemaining)
	assert.Zero(t, ttlRemaining)

	deleted, err = h.repo.DeleteReadyConversations(t.Context(), h.now.Now().Add(2*time.Minute), 10)
	require.NoError(t, err)
	assert.Equal(t, 1, deleted)
	for _, table := range []string{"conversations", "conversation_items", "handoffs", "handoff_events", "tool_invocations", "delivery_cursors", "coordinator_work_journal"} {
		var remaining int
		query := `SELECT count(*) FROM ` + table + ` WHERE tenant_id=$1 AND `
		if table == "conversations" {
			query += `id=$2`
		} else {
			query += `conversation_id=$2`
		}
		require.NoError(t, h.pool.QueryRow(t.Context(), query, userScope.TenantID, userScope.ConversationID).Scan(&remaining), table)
		assert.Zero(t, remaining, table)
	}
	var outboxRemaining int
	require.NoError(t, h.pool.QueryRow(t.Context(), `SELECT count(*) FROM outbox WHERE id=ANY($1)`, outboxIDs).Scan(&outboxRemaining))
	assert.Zero(t, outboxRemaining)
	_, err = h.repo.LoadRecovery(t.Context(), userScope, session.DefaultRecoveryItemLimit, session.DefaultRecoveryByteLimit)
	assert.ErrorIs(t, err, handoff.ErrNotFound, "hard deletion must preserve the client fence")
	assert.ErrorIs(t, h.repo.MarkRunning(t.Context(), lease.Token, h.now.Now().Add(2*time.Minute)), worker.ErrLeaseLost)
}

func TestOIDCExpiryRejectsBeforeSessionAdmission(t *testing.T) {
	server := testkit.NewJWKSServer(t)
	verifier, err := auth.NewOIDC(t.Context(), auth.OIDCConfig{Issuer: server.Issuer(), Audience: "live", RequiredScopes: []string{"realtime"}})
	require.NoError(t, err)
	token := server.Token(t, "live", "tenant", "user", []string{"realtime"}, time.Now().Add(-time.Minute), nil)
	_, err = verifier.Authenticate(t.Context(), token)
	assert.ErrorIs(t, err, auth.ErrUnauthorized)
}

func TestDistributedAdmissionLimitsSerializeAcrossInstances(t *testing.T) {
	h := newRecoveryHarness(t)
	first, err := admission.NewPostgres(h.pool, admission.PostgresConfig{InstanceID: "gateway-a", LeaseDuration: time.Second, HeartbeatInterval: 100 * time.Millisecond})
	require.NoError(t, err)
	second, err := admission.NewPostgres(h.pool, admission.PostgresConfig{InstanceID: "gateway-b", LeaseDuration: time.Second, HeartbeatInterval: 100 * time.Millisecond})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, first.Close()); assert.NoError(t, second.Close()) })
	subject := session.Subject{TenantID: "tenant-admission", UserID: "user-admission"}
	release, err := first.Acquire(t.Context(), subject, admission.Limits{Global: 1, PerTenant: 1, PerUser: 1})
	require.NoError(t, err)
	defer release()
	_, err = second.Acquire(t.Context(), subject, admission.Limits{Global: 1, PerTenant: 1, PerUser: 1})
	assert.ErrorIs(t, err, admission.ErrLimited)
}

func TestFeatureGateAndSafetyHelpersFailClosed(t *testing.T) {
	gates := feature.New(map[string]bool{feature.HandoffsEnabled: true, feature.ShadowMode: true})
	assert.True(t, gates.Enabled(feature.ShadowMode))
	decision := safety.EvaluateOrSuppress(context.Background(), safety.DenyAll{}, safety.WorkerResultCandidate{
		SessionID: "session", ConversationID: "conversation", HandoffID: "handoff", Channel: safety.ChannelWorker,
		Summary: "bounded", Structured: json.RawMessage(`{"bounded":true}`),
	})
	assert.Equal(t, safety.ActionSuppress, decision.Action)
	assert.NotEmpty(t, decision.ReasonCode)
}

func TestPostgresCursorRejectsSkippedSequenceAndPreservesSuffix(t *testing.T) {
	h := newRecoveryHarness(t)
	service, scope, snapshot := h.create(t, "cursor-crash")
	_, err := service.Steer(t.Context(), scope, snapshot.ID, handoff.SteerRequest{Objective: "revision", ExpectedRevision: snapshot.ObjectiveRevision + 1, Actor: scope.UserID, IdempotencyKey: "cursor-steer"})
	require.NoError(t, err)
	events, err := h.repo.ReadEvents(t.Context(), scope, 0, 16)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(events), 2)
	err = h.repo.AdvanceDeliveryCursor(t.Context(), "cursor-crash", scope, events[1].Seq)
	assert.ErrorIs(t, err, handoff.ErrRevisionConflict)
	require.NoError(t, h.repo.AdvanceDeliveryCursor(t.Context(), "cursor-crash", scope, events[0].Seq))
	replayed, err := h.repo.ReadEvents(t.Context(), scope, events[0].Seq, 16)
	require.NoError(t, err)
	assert.Equal(t, events[1].EventID, replayed[0].EventID)
}
