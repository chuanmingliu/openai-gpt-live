package handoff

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/toolpolicy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLegacyProfilePreservesPreProfileIdempotencyHash(t *testing.T) {
	request := validCreateRequest(time.Date(2026, time.August, 20, 12, 0, 0, 0, time.UTC))
	request.Profile = LegacyProfileSnapshot()
	normalized, err := NormalizeCreateRequest(request)
	require.NoError(t, err)
	legacyDocument := struct {
		Scope            Scope           `json:"scope"`
		SessionID        string          `json:"session_id"`
		SourceTurnID     string          `json:"source_turn_id"`
		SourceRevision   uint32          `json:"source_turn_revision"`
		UserRequest      string          `json:"user_request"`
		Objective        string          `json:"objective"`
		ContextRef       string          `json:"context_ref"`
		ContextHash      string          `json:"context_hash"`
		Deadline         string          `json:"deadline"`
		TokenBudget      int             `json:"token_budget"`
		Priority         int16           `json:"priority"`
		ToolAllowlist    []string        `json:"tool_allowlist"`
		ToolPolicy       json.RawMessage `json:"tool_policy"`
		ApprovalPolicy   json.RawMessage `json:"approval_policy"`
		Versions         Versions        `json:"versions"`
		PresentationHint string          `json:"presentation_hint"`
	}{
		Scope: normalized.Scope, SessionID: string(normalized.SessionID), SourceTurnID: normalized.SourceTurn.ID,
		SourceRevision: normalized.SourceTurn.Revision, UserRequest: normalized.UserRequest, Objective: normalized.Objective,
		ContextRef: normalized.ContextRef, ContextHash: normalized.ContextHash, Deadline: normalized.Deadline.Format(time.RFC3339Nano),
		TokenBudget: normalized.TokenBudget, Priority: normalized.Priority, ToolAllowlist: normalized.ToolAllowlist,
		ToolPolicy: normalized.ToolPolicy, ApprovalPolicy: normalized.ApprovalPolicy, Versions: normalized.Versions,
		PresentationHint: normalized.PresentationHint,
	}
	encoded, err := json.Marshal(legacyDocument)
	require.NoError(t, err)
	digest := sha256.Sum256(encoded)
	want := hex.EncodeToString(digest[:])
	got, err := CanonicalRequestHash(request)
	require.NoError(t, err)
	require.Equal(t, want, got)

	request.Profile = validProfileSnapshot()
	custom, err := CanonicalRequestHash(request)
	require.NoError(t, err)
	require.NotEqual(t, want, custom)
}

func TestNormalizeCreateRequestCanonicalizesToolPolicy(t *testing.T) {
	request := validCreateRequest(time.Now().UTC())
	request.ToolAllowlist = append(request.ToolAllowlist, toolpolicy.WebSearchTool)
	request.ToolPolicy = json.RawMessage(`{"web_search":{"timeout_ms":8000,"max_results":5,"region":"wt-wt","safe_search":"moderate","enabled":true}}`)

	normalized, err := NormalizeCreateRequest(request)
	require.NoError(t, err)
	assert.Equal(t, json.RawMessage(`{"web_search":{"enabled":true,"safe_search":"moderate","region":"wt-wt","max_results":5,"timeout_ms":8000}}`), normalized.ToolPolicy)
}

func TestNormalizeCreateRequestRejectsPolicyNotInAllowlist(t *testing.T) {
	request := validCreateRequest(time.Now().UTC())
	request.ToolAllowlist = nil
	request.ToolPolicy = mustPolicyJSON(t, enabledPolicy())

	_, err := NormalizeCreateRequest(request)
	assert.ErrorIs(t, err, ErrInvalidRequest)
}

func TestNormalizeCreateRequestRejectsAllowlistedToolDisabledByPolicy(t *testing.T) {
	request := validCreateRequest(time.Now().UTC())
	request.ToolAllowlist = append(request.ToolAllowlist, toolpolicy.WebSearchTool)
	policy := enabledPolicy()
	policy.WebSearch.Enabled = false
	request.ToolPolicy = mustPolicyJSON(t, policy)

	_, err := NormalizeCreateRequest(request)
	assert.ErrorIs(t, err, ErrInvalidRequest)
}

func TestCanonicalRequestHashCoversToolPolicy(t *testing.T) {
	request := validCreateRequest(time.Now().UTC())
	disabledHash, err := CanonicalRequestHash(request)
	require.NoError(t, err)

	stricterPolicy := enabledPolicy()
	stricterPolicy.WebSearch.Enabled = false
	stricterPolicy.WebSearch.SafeSearch = toolpolicy.SafeSearchStrict
	request.ToolPolicy = mustPolicyJSON(t, stricterPolicy)
	stricterHash, err := CanonicalRequestHash(request)
	require.NoError(t, err)
	assert.NotEqual(t, disabledHash, stricterHash)
}

func TestCloneSnapshotDeepCopiesToolPolicy(t *testing.T) {
	original := Snapshot{Request: validCreateRequest(time.Now().UTC())}
	cloned := CloneSnapshot(original)
	cloned.Request.ToolPolicy[0] = '['
	assert.Equal(t, byte('{'), original.Request.ToolPolicy[0])
}

func TestProfileSnapshotIsHashedAndDeepCopiedWithCreateRequest(t *testing.T) {
	request := validCreateRequest(time.Now().UTC())
	first, err := CanonicalRequestHash(request)
	require.NoError(t, err)
	request.Profile.Knowledge[0].Hash = strings.Repeat("e", 64)
	second, err := CanonicalRequestHash(request)
	require.NoError(t, err)
	require.NotEqual(t, first, second)

	original := Snapshot{Request: request}
	cloned := CloneSnapshot(original)
	cloned.Request.Profile.Knowledge[0].ID = "mutated"
	cloned.Request.Profile.Evidence[0] = '['
	require.Equal(t, "policy", original.Request.Profile.Knowledge[0].ID)
	require.Equal(t, byte('{'), original.Request.Profile.Evidence[0])
}

func TestProfileSnapshotBoundsKnowledgeAndEvidence(t *testing.T) {
	now := time.Now().UTC()
	atLimit := validCreateRequest(now)
	atLimit.Profile.Knowledge = make([]KnowledgeVersion, 32)
	for index := range atLimit.Profile.Knowledge {
		atLimit.Profile.Knowledge[index] = KnowledgeVersion{
			ID: "pack-" + string(rune('a'+index)), Version: 1, Hash: strings.Repeat("a", 64),
		}
	}
	atLimit.Profile.Evidence = json.RawMessage(`{"value":"` + strings.Repeat("x", 256*1024-len(`{"value":""}`)) + `"}`)
	_, err := NormalizeCreateRequest(atLimit)
	require.NoError(t, err)

	overKnowledge := validCreateRequest(now)
	overKnowledge.Profile.Knowledge = append(overKnowledge.Profile.Knowledge, make([]KnowledgeVersion, 32)...)
	_, err = NormalizeCreateRequest(overKnowledge)
	require.ErrorIs(t, err, ErrInvalidRequest)

	overEvidence := validCreateRequest(now)
	overEvidence.Profile.Evidence = json.RawMessage(`"` + strings.Repeat("x", 256*1024) + `"`)
	_, err = NormalizeCreateRequest(overEvidence)
	require.ErrorIs(t, err, ErrInvalidRequest)
}

func TestValidateTransition_AllOrderedPairs(t *testing.T) {
	states := []State{StatePending, StateLeased, StateRunning, StateNeedsInput, StateSucceeded, StateFailed, StateCancelled, StateExpired}
	allowed := map[[2]State]bool{
		{StatePending, StateLeased}:       true,
		{StatePending, StateCancelled}:    true,
		{StatePending, StateExpired}:      true,
		{StateLeased, StateRunning}:       true,
		{StateLeased, StateCancelled}:     true,
		{StateLeased, StateExpired}:       true,
		{StateRunning, StateNeedsInput}:   true,
		{StateRunning, StateSucceeded}:    true,
		{StateRunning, StateFailed}:       true,
		{StateRunning, StateCancelled}:    true,
		{StateRunning, StateExpired}:      true,
		{StateNeedsInput, StateRunning}:   true,
		{StateNeedsInput, StateCancelled}: true,
		{StateNeedsInput, StateExpired}:   true,
	}

	for _, from := range states {
		for _, to := range states {
			from, to := from, to
			t.Run(string(from)+"_to_"+string(to), func(t *testing.T) {
				err := ValidateTransition(from, to)
				if allowed[[2]State{from, to}] {
					require.NoError(t, err)
					return
				}
				assert.ErrorIs(t, err, ErrInvalidTransition)
				if from == StateSucceeded || from == StateFailed || from == StateCancelled || from == StateExpired {
					assert.ErrorIs(t, err, ErrTerminalState)
				}
			})
		}
	}
}

func TestValidateTransition_RejectsUnknownStates(t *testing.T) {
	assert.ErrorIs(t, ValidateTransition("unknown", StatePending), ErrInvalidTransition)
	assert.ErrorIs(t, ValidateTransition(StatePending, "unknown"), ErrInvalidTransition)
}

func TestValidateCreateRequest_RequiredFieldsAndBounds(t *testing.T) {
	now := time.Date(2026, 8, 7, 12, 0, 0, 0, time.FixedZone("CST", 8*60*60))
	valid := validCreateRequest(now)

	tests := []struct {
		name string
		edit func(*CreateRequest)
	}{
		{"tenant", func(r *CreateRequest) { r.Scope.TenantID = "" }},
		{"user", func(r *CreateRequest) { r.Scope.UserID = "" }},
		{"conversation", func(r *CreateRequest) { r.Scope.ConversationID = "" }},
		{"session", func(r *CreateRequest) { r.SessionID = "" }},
		{"turn id", func(r *CreateRequest) { r.SourceTurn.ID = "" }},
		{"turn revision", func(r *CreateRequest) { r.SourceTurn.Revision = 0 }},
		{"user request", func(r *CreateRequest) { r.UserRequest = " \n" }},
		{"objective", func(r *CreateRequest) { r.Objective = "\t" }},
		{"context ref", func(r *CreateRequest) { r.ContextRef = "" }},
		{"context hash", func(r *CreateRequest) { r.ContextHash = "" }},
		{"deadline zero", func(r *CreateRequest) { r.Deadline = time.Time{} }},
		{"deadline past", func(r *CreateRequest) { r.Deadline = now }},
		{"budget", func(r *CreateRequest) { r.TokenBudget = 0 }},
		{"idempotency", func(r *CreateRequest) { r.IdempotencyKey = "" }},
		{"model version", func(r *CreateRequest) { r.Versions.Model = "" }},
		{"prompt version", func(r *CreateRequest) { r.Versions.Prompt = "" }},
		{"tool version", func(r *CreateRequest) { r.Versions.ToolRegistry = "" }},
		{"config version", func(r *CreateRequest) { r.Versions.Config = "" }},
		{"user request oversize", func(r *CreateRequest) { r.UserRequest = strings.Repeat("u", 16*1024+1) }},
		{"objective oversize", func(r *CreateRequest) { r.Objective = strings.Repeat("o", 8*1024+1) }},
		{"context ref oversize", func(r *CreateRequest) { r.ContextRef = strings.Repeat("c", 2*1024+1) }},
		{"policy oversize", func(r *CreateRequest) { r.ApprovalPolicy = json.RawMessage(`"` + strings.Repeat("p", 64*1024-1) + `"`) }},
		{"allowlist oversize", func(r *CreateRequest) { r.ToolAllowlist = makeToolNames(65) }},
		{"allowlist blank", func(r *CreateRequest) { r.ToolAllowlist = []string{"ok", " "} }},
		{"allowlist duplicate", func(r *CreateRequest) { r.ToolAllowlist = []string{"same", "same"} }},
		{"invalid policy", func(r *CreateRequest) { r.ApprovalPolicy = json.RawMessage(`{"x":`) }},
		{"trailing policy", func(r *CreateRequest) { r.ApprovalPolicy = json.RawMessage(`{} {}`) }},
		{"deep policy", func(r *CreateRequest) {
			r.ApprovalPolicy = json.RawMessage(strings.Repeat("[", 65) + strings.Repeat("]", 65))
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := valid
			req.ToolAllowlist = append([]string(nil), valid.ToolAllowlist...)
			req.ApprovalPolicy = append(json.RawMessage(nil), valid.ApprovalPolicy...)
			tc.edit(&req)
			_, err := ValidateCreateRequest(req, now)
			assert.ErrorIs(t, err, ErrInvalidRequest)
		})
	}

	boundary := valid
	boundary.UserRequest = strings.Repeat("u", 16*1024)
	boundary.Objective = strings.Repeat("o", 8*1024)
	boundary.ContextRef = strings.Repeat("c", 2*1024)
	boundary.ApprovalPolicy = json.RawMessage(`"` + strings.Repeat("p", 64*1024-2) + `"`)
	boundary.ToolAllowlist = makeToolNames(64)
	_, err := ValidateCreateRequest(boundary, now)
	require.NoError(t, err)
}

func TestValidateCreateRequest_NormalizesPrivateCopies(t *testing.T) {
	now := time.Date(2026, 8, 7, 4, 0, 0, 0, time.UTC)
	req := validCreateRequest(now)
	req.ToolAllowlist = []string{"zeta", "alpha"}
	req.ApprovalPolicy = json.RawMessage(`{ "b": 2, "a": 1 }`)

	got, err := ValidateCreateRequest(req, now)
	require.NoError(t, err)
	assert.Equal(t, []string{"alpha", "zeta"}, got.ToolAllowlist)
	req.ToolAllowlist[0] = "mutated"
	req.ApprovalPolicy[2] = 'x'
	assert.Equal(t, []string{"alpha", "zeta"}, got.ToolAllowlist)
	assert.JSONEq(t, `{"a":1,"b":2}`, string(got.ApprovalPolicy))

	empty := validCreateRequest(now)
	empty.ToolAllowlist = []string{}
	got, err = ValidateCreateRequest(empty, now)
	require.NoError(t, err)
	require.NotNil(t, got.ToolAllowlist, "an explicit empty allowlist must remain a JSON array at the repository boundary")
	assert.Empty(t, got.ToolAllowlist)
}

func TestValidateCreateRequest_AllSecondaryByteBoundaries(t *testing.T) {
	now := time.Date(2026, 8, 7, 4, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		at   func(*CreateRequest)
		over func(*CreateRequest)
	}{
		{"tenant id", func(r *CreateRequest) { r.Scope.TenantID = strings.Repeat("t", 256) }, func(r *CreateRequest) { r.Scope.TenantID = strings.Repeat("t", 257) }},
		{"user id", func(r *CreateRequest) { r.Scope.UserID = strings.Repeat("u", 256) }, func(r *CreateRequest) { r.Scope.UserID = strings.Repeat("u", 257) }},
		{"conversation id", func(r *CreateRequest) { r.Scope.ConversationID = identity.ConversationID(strings.Repeat("c", 256)) }, func(r *CreateRequest) { r.Scope.ConversationID = identity.ConversationID(strings.Repeat("c", 257)) }},
		{"session id", func(r *CreateRequest) { r.SessionID = identity.SessionID(strings.Repeat("s", 256)) }, func(r *CreateRequest) { r.SessionID = identity.SessionID(strings.Repeat("s", 257)) }},
		{"source turn id", func(r *CreateRequest) { r.SourceTurn.ID = strings.Repeat("r", 256) }, func(r *CreateRequest) { r.SourceTurn.ID = strings.Repeat("r", 257) }},
		{"context hash", func(r *CreateRequest) { r.ContextHash = strings.Repeat("h", 512) }, func(r *CreateRequest) { r.ContextHash = strings.Repeat("h", 513) }},
		{"model version", func(r *CreateRequest) { r.Versions.Model = strings.Repeat("m", 256) }, func(r *CreateRequest) { r.Versions.Model = strings.Repeat("m", 257) }},
		{"prompt version", func(r *CreateRequest) { r.Versions.Prompt = strings.Repeat("p", 256) }, func(r *CreateRequest) { r.Versions.Prompt = strings.Repeat("p", 257) }},
		{"tool registry version", func(r *CreateRequest) { r.Versions.ToolRegistry = strings.Repeat("t", 256) }, func(r *CreateRequest) { r.Versions.ToolRegistry = strings.Repeat("t", 257) }},
		{"config version", func(r *CreateRequest) { r.Versions.Config = strings.Repeat("c", 256) }, func(r *CreateRequest) { r.Versions.Config = strings.Repeat("c", 257) }},
		{"presentation hint", func(r *CreateRequest) { r.PresentationHint = strings.Repeat("p", 2*1024) }, func(r *CreateRequest) { r.PresentationHint = strings.Repeat("p", 2*1024+1) }},
		{"idempotency key", func(r *CreateRequest) { r.IdempotencyKey = strings.Repeat("i", 256) }, func(r *CreateRequest) { r.IdempotencyKey = strings.Repeat("i", 257) }},
		{"tool name", func(r *CreateRequest) { r.ToolAllowlist = []string{strings.Repeat("n", 256)} }, func(r *CreateRequest) { r.ToolAllowlist = []string{strings.Repeat("n", 257)} }},
	}
	for _, tc := range tests {
		t.Run(tc.name+" at limit", func(t *testing.T) {
			candidate := validCreateRequest(now)
			tc.at(&candidate)
			_, err := ValidateCreateRequest(candidate, now)
			require.NoError(t, err)
		})
		t.Run(tc.name+" limit plus one", func(t *testing.T) {
			candidate := validCreateRequest(now)
			tc.over(&candidate)
			_, err := ValidateCreateRequest(candidate, now)
			assert.ErrorIs(t, err, ErrInvalidRequest)
		})
	}

	for _, priority := range []int16{-1000, 1000} {
		candidate := validCreateRequest(now)
		candidate.Priority = priority
		_, err := ValidateCreateRequest(candidate, now)
		require.NoError(t, err)
	}
	for _, priority := range []int16{-1001, 1001} {
		candidate := validCreateRequest(now)
		candidate.Priority = priority
		_, err := ValidateCreateRequest(candidate, now)
		assert.ErrorIs(t, err, ErrInvalidRequest)
	}
}

func TestValidateCreateRequest_RejectsAdversarialPolicyJSON(t *testing.T) {
	now := time.Date(2026, 8, 7, 4, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		raw  json.RawMessage
	}{
		{"invalid utf8", json.RawMessage{'"', 0xff, '"'}},
		{"duplicate key", json.RawMessage(`{"mode":"ask","mode":"allow"}`)},
		{"extreme number", json.RawMessage(`{"threshold":1e999}`)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			candidate := validCreateRequest(now)
			candidate.ApprovalPolicy = tc.raw
			_, err := ValidateCreateRequest(candidate, now)
			assert.ErrorIs(t, err, ErrInvalidRequest)
		})
	}
}

func TestCanonicalRequestHash_IsSemanticAndCoversRequestExceptIdempotency(t *testing.T) {
	now := time.Date(2026, 8, 7, 4, 0, 0, 123, time.UTC)
	base := validCreateRequest(now)
	base.ToolAllowlist = []string{"search", "read"}
	base.ApprovalPolicy = json.RawMessage(`{"b":[2,1],"a":1.0}`)

	equivalent := base
	equivalent.Deadline = base.Deadline.In(time.FixedZone("offset", 9*60*60))
	equivalent.ToolAllowlist = []string{"read", "search"}
	equivalent.ApprovalPolicy = json.RawMessage("{ \"a\" : 1.00, \"b\" : [2.0,1] }")
	equivalent.IdempotencyKey = "different-key"

	h1, err := CanonicalRequestHash(base)
	require.NoError(t, err)
	h2, err := CanonicalRequestHash(equivalent)
	require.NoError(t, err)
	assert.Equal(t, h1, h2)
	assert.Regexp(t, `^[0-9a-f]{64}$`, h1)

	changes := []func(*CreateRequest){
		func(r *CreateRequest) { r.Scope.TenantID += "x" },
		func(r *CreateRequest) { r.Scope.UserID += "x" },
		func(r *CreateRequest) { r.Scope.ConversationID += "x" },
		func(r *CreateRequest) { r.SessionID += "x" },
		func(r *CreateRequest) { r.SourceTurn.ID += "x" },
		func(r *CreateRequest) { r.SourceTurn.Revision++ },
		func(r *CreateRequest) { r.UserRequest += "x" },
		func(r *CreateRequest) { r.Objective += "x" },
		func(r *CreateRequest) { r.ContextRef += "x" },
		func(r *CreateRequest) { r.ContextHash += "x" },
		func(r *CreateRequest) { r.Deadline = r.Deadline.Add(time.Second) },
		func(r *CreateRequest) { r.TokenBudget++ },
		func(r *CreateRequest) { r.Priority++ },
		func(r *CreateRequest) { r.ToolAllowlist = append(r.ToolAllowlist, "write") },
		func(r *CreateRequest) { r.ApprovalPolicy = json.RawMessage(`{"a":2,"b":[2,1]}`) },
		func(r *CreateRequest) { r.Versions.Model += "x" },
		func(r *CreateRequest) { r.Versions.Prompt += "x" },
		func(r *CreateRequest) { r.Versions.ToolRegistry += "x" },
		func(r *CreateRequest) { r.Versions.Config += "x" },
		func(r *CreateRequest) { r.PresentationHint += "x" },
	}
	for i, change := range changes {
		candidate := cloneCreateRequest(base)
		change(&candidate)
		got, err := CanonicalRequestHash(candidate)
		require.NoError(t, err, "change %d", i)
		assert.NotEqual(t, h1, got, "semantic change %d must affect hash", i)
	}
}

func TestCanonicalRequestHash_NormalizesFractionalAndExponentNumbers(t *testing.T) {
	now := time.Date(2026, 8, 7, 4, 0, 0, 0, time.UTC)
	decimal := validCreateRequest(now)
	decimal.ApprovalPolicy = json.RawMessage(`{"small":0.0100,"large":1200.0}`)
	exponent := cloneCreateRequest(decimal)
	exponent.ApprovalPolicy = json.RawMessage(`{"large":1.2e3,"small":1e-2}`)

	decimalHash, err := CanonicalRequestHash(decimal)
	require.NoError(t, err)
	exponentHash, err := CanonicalRequestHash(exponent)
	require.NoError(t, err)
	assert.Equal(t, decimalHash, exponentHash)
}

func TestCloneSnapshot_DeepCopiesMutableValues(t *testing.T) {
	now := time.Date(2026, 8, 7, 4, 0, 0, 0, time.UTC)
	original := Snapshot{
		ID: "handoff-1", Request: validCreateRequest(now), RequestHash: "hash", State: StateSucceeded,
		Result:  &Result{MutationID: "m", Summary: "done", Structured: json.RawMessage(`{"nested":[1]}`), SafetyDecision: json.RawMessage(`{"action":"allow"}`), Usage: json.RawMessage(`{"tokens":2}`)},
		Failure: &Failure{MutationID: "f", Code: "code", Message: "message"},
	}
	cloned := CloneSnapshot(original)
	cloned.Request.ToolAllowlist[0] = "changed"
	cloned.Request.ApprovalPolicy[0] = '['
	cloned.Result.Structured[0] = '['
	cloned.Result.SafetyDecision[0] = '['
	cloned.Result.Usage[0] = '['
	cloned.Failure.Message = "changed"
	assert.Equal(t, "read", original.Request.ToolAllowlist[0])
	assert.Equal(t, byte('{'), original.Request.ApprovalPolicy[0])
	assert.Equal(t, byte('{'), original.Result.Structured[0])
	assert.Equal(t, byte('{'), original.Result.SafetyDecision[0])
	assert.Equal(t, byte('{'), original.Result.Usage[0])
	assert.Equal(t, "message", original.Failure.Message)
}

func TestCloneEvent_DeepCopiesPayload(t *testing.T) {
	original := Event{EventID: "event-1", HandoffID: "handoff-1", Payload: json.RawMessage(`{"nested":[1]}`)}
	cloned := CloneEvent(original)
	cloned.Payload[0] = '['
	assert.Equal(t, byte('{'), original.Payload[0])
}

func TestValidateResult_CanonicalizesPrivateCopies(t *testing.T) {
	input := Result{
		MutationID:     "mutation-1",
		Summary:        "completed",
		Structured:     json.RawMessage(`{ "b": [2.0, 1], "a": 0.0100 }`),
		SafetyDecision: json.RawMessage(`{ "reason_code": "bounded", "action": "allow" }`),
		Usage:          json.RawMessage(`{"output":2.0,"input":1}`),
	}

	got, err := ValidateResult(input)
	require.NoError(t, err)
	assert.Equal(t, json.RawMessage(`{"a":0.01,"b":[2,1]}`), got.Structured)
	assert.Equal(t, json.RawMessage(`{"action":"allow","reason_code":"bounded"}`), got.SafetyDecision)
	assert.Equal(t, json.RawMessage(`{"input":1,"output":2}`), got.Usage)
	input.Structured[2] = 'x'
	input.SafetyDecision[2] = 'x'
	input.Usage[2] = 'x'
	assert.Equal(t, byte('{'), got.Structured[0])
	assert.Equal(t, byte('{'), got.SafetyDecision[0])
	assert.Equal(t, byte('{'), got.Usage[0])
}

func TestValidateResult_RequiresBoundedMetadataAndBothJSONDocuments(t *testing.T) {
	valid := validResult()
	tests := []struct {
		name string
		edit func(*Result)
	}{
		{"blank mutation id", func(r *Result) { r.MutationID = " " }},
		{"mutation id over limit", func(r *Result) { r.MutationID = strings.Repeat("m", 257) }},
		{"blank summary", func(r *Result) { r.Summary = "\n" }},
		{"summary over limit", func(r *Result) { r.Summary = strings.Repeat("s", 16*1024+1) }},
		{"missing structured", func(r *Result) { r.Structured = nil }},
		{"missing safety decision", func(r *Result) { r.SafetyDecision = nil }},
		{"missing usage", func(r *Result) { r.Usage = nil }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			candidate := cloneResultForTest(valid)
			tc.edit(&candidate)
			_, err := ValidateResult(candidate)
			assert.ErrorIs(t, err, ErrInvalidRequest)
		})
	}

	atLimit := valid
	atLimit.MutationID = strings.Repeat("m", 256)
	atLimit.Summary = strings.Repeat("s", 16*1024)
	_, err := ValidateResult(atLimit)
	require.NoError(t, err)
}

func TestValidateResult_EnforcesStrictJSONForBothDocuments(t *testing.T) {
	deep := json.RawMessage(strings.Repeat("[", 65) + strings.Repeat("]", 65))
	invalidUTF8 := json.RawMessage{'"', 0xff, '"'}
	tests := []struct {
		name string
		raw  json.RawMessage
	}{
		{"invalid utf8", invalidUTF8},
		{"malformed", json.RawMessage(`{"x":`)},
		{"trailing", json.RawMessage(`{} {}`)},
		{"duplicate", json.RawMessage(`{"x":1,"x":2}`)},
		{"deep", deep},
		{"extreme number", json.RawMessage(`{"x":1e999}`)},
	}
	for _, field := range []string{"structured", "safety decision", "usage"} {
		for _, tc := range tests {
			t.Run(field+"/"+tc.name, func(t *testing.T) {
				candidate := validResult()
				if field == "structured" {
					candidate.Structured = tc.raw
				} else if field == "safety decision" {
					candidate.SafetyDecision = tc.raw
				} else {
					candidate.Usage = tc.raw
				}
				_, err := ValidateResult(candidate)
				assert.ErrorIs(t, err, ErrInvalidRequest)
			})
		}
	}
}

func TestValidateResult_EnforcesRawAndCanonicalJSONSizeForBothDocuments(t *testing.T) {
	atLimit := json.RawMessage(`"` + strings.Repeat("x", 64*1024-2) + `"`)
	overRawLimit := json.RawMessage(`"` + strings.Repeat("x", 64*1024-1) + `"`)
	overCanonicalLimit := json.RawMessage(`"` + strings.Repeat("<", 12*1024) + `"`)

	for _, field := range []string{"structured", "safety decision", "usage"} {
		t.Run(field+" at limit", func(t *testing.T) {
			candidate := validResult()
			fieldAtLimit := atLimit
			if field == "safety decision" {
				fieldAtLimit = json.RawMessage(`{"x":"` + strings.Repeat("x", 64*1024-8) + `"}`)
			}
			if field == "structured" {
				candidate.Structured = fieldAtLimit
			} else if field == "safety decision" {
				candidate.SafetyDecision = fieldAtLimit
			} else {
				candidate.Usage = atLimit
			}
			_, err := ValidateResult(candidate)
			require.NoError(t, err)
		})
		for name, raw := range map[string]json.RawMessage{"raw limit plus one": overRawLimit, "canonical output over limit": overCanonicalLimit} {
			t.Run(field+"/"+name, func(t *testing.T) {
				candidate := validResult()
				fieldRaw := raw
				if field == "safety decision" {
					if name == "raw limit plus one" {
						fieldRaw = json.RawMessage(`{"x":"` + strings.Repeat("x", 64*1024-7) + `"}`)
					} else {
						fieldRaw = json.RawMessage(`{"x":"` + strings.Repeat("<", 12*1024) + `"}`)
					}
				}
				if field == "structured" {
					candidate.Structured = fieldRaw
				} else if field == "safety decision" {
					candidate.SafetyDecision = fieldRaw
				} else {
					candidate.Usage = raw
				}
				_, err := ValidateResult(candidate)
				assert.ErrorIs(t, err, ErrInvalidRequest)
			})
		}
	}
}

func TestValidateResult_SafetyDecisionMustBeAnObject(t *testing.T) {
	for _, raw := range []json.RawMessage{json.RawMessage(`[]`), json.RawMessage(`"allow"`), json.RawMessage(`null`)} {
		candidate := validResult()
		candidate.SafetyDecision = raw
		_, err := ValidateResult(candidate)
		assert.ErrorIs(t, err, ErrInvalidRequest)
	}
}

func TestValidateFailure_RequiresBoundedTerminalMetadata(t *testing.T) {
	valid := Failure{MutationID: "mutation-1", Code: "worker_failed", Message: "reason"}
	tests := []struct {
		name string
		edit func(*Failure)
	}{
		{"blank mutation id", func(f *Failure) { f.MutationID = " " }},
		{"mutation id over limit", func(f *Failure) { f.MutationID = strings.Repeat("m", 257) }},
		{"blank code", func(f *Failure) { f.Code = "" }},
		{"code over limit", func(f *Failure) { f.Code = strings.Repeat("c", 257) }},
		{"blank message", func(f *Failure) { f.Message = "\t" }},
		{"message over limit", func(f *Failure) { f.Message = strings.Repeat("m", 16*1024+1) }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			candidate := valid
			tc.edit(&candidate)
			_, err := ValidateFailure(candidate)
			assert.ErrorIs(t, err, ErrInvalidRequest)
		})
	}

	atLimit := Failure{MutationID: strings.Repeat("m", 256), Code: strings.Repeat("c", 256), Message: strings.Repeat("x", 16*1024)}
	got, err := ValidateFailure(atLimit)
	require.NoError(t, err)
	assert.Equal(t, atLimit, got)
}

func TestStableSentinelsSupportErrorsIs(t *testing.T) {
	for _, target := range []error{ErrInvalidRequest, ErrNotFound, ErrUncommittedTurn, ErrIdempotencyConflict, ErrInvalidTransition, ErrTerminalState, ErrRevisionConflict, ErrSteeringBlocked} {
		assert.True(t, errors.Is(errors.Join(errors.New("context"), target), target))
	}
}

func validCreateRequest(now time.Time) CreateRequest {
	disabledPolicy := enabledPolicy()
	disabledPolicy.WebSearch.Enabled = false
	return CreateRequest{
		Scope:     Scope{TenantID: "tenant-1", UserID: "user-1", ConversationID: "conversation-1"},
		SessionID: "session-1", SourceTurn: identity.TurnKey{ID: "turn-1", Revision: 3},
		UserRequest: "research this", Objective: "produce a report", ContextRef: "context://one", ContextHash: "sha256:context",
		Deadline: now.Add(time.Hour), TokenBudget: 2048, Priority: 5, ToolAllowlist: []string{"read", "search"},
		ToolPolicy:       mustPolicyJSONWithoutTest(disabledPolicy),
		ApprovalPolicy:   json.RawMessage(`{"mode":"ask"}`),
		Versions:         Versions{Model: "model-1", Prompt: "prompt-1", ToolRegistry: "tools-1", Config: "config-1"},
		Profile:          validProfileSnapshot(),
		PresentationHint: "brief", IdempotencyKey: "idem-1",
	}
}

func validProfileSnapshot() ProfileSnapshot {
	return ProfileSnapshot{
		ProfileID: "insurance", ProfileVersion: 3, SnapshotHash: strings.Repeat("a", 64),
		EngineVersion: "jinja-go-v1/test", RecipeID: "renewal", RecipeVersion: 2,
		RecipeHash: strings.Repeat("b", 64), RegistryVersion: "registry-v1",
		Knowledge: []KnowledgeVersion{{ID: "policy", Version: 7, Hash: strings.Repeat("c", 64)}},
		Evidence:  json.RawMessage(`{"source":"turn"}`),
	}
}

func cloneCreateRequest(in CreateRequest) CreateRequest {
	in.ToolAllowlist = append([]string(nil), in.ToolAllowlist...)
	in.ToolPolicy = append(json.RawMessage(nil), in.ToolPolicy...)
	in.ApprovalPolicy = append(json.RawMessage(nil), in.ApprovalPolicy...)
	in.Profile = cloneProfileSnapshot(in.Profile)
	return in
}

func enabledPolicy() toolpolicy.Policy {
	return toolpolicy.DefaultPolicy()
}

func mustPolicyJSON(t *testing.T, policy toolpolicy.Policy) json.RawMessage {
	t.Helper()
	raw, err := toolpolicy.MarshalCanonical(policy)
	require.NoError(t, err)
	return raw
}

func mustPolicyJSONWithoutTest(policy toolpolicy.Policy) json.RawMessage {
	raw, err := toolpolicy.MarshalCanonical(policy)
	if err != nil {
		panic(err)
	}
	return raw
}

func makeToolNames(n int) []string {
	result := make([]string, n)
	for i := range result {
		result[i] = "tool-" + string(rune('A'+i))
	}
	return result
}

func validResult() Result {
	return Result{MutationID: "mutation-1", Summary: "completed", Structured: json.RawMessage(`{"ok":true}`), SafetyDecision: json.RawMessage(`{"action":"allow"}`), Usage: json.RawMessage(`{"tokens":2}`)}
}

func cloneResultForTest(in Result) Result {
	in.Structured = append(json.RawMessage(nil), in.Structured...)
	in.SafetyDecision = append(json.RawMessage(nil), in.SafetyDecision...)
	in.Usage = append(json.RawMessage(nil), in.Usage...)
	return in
}
