package handoff

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseReservedTool_Valid(t *testing.T) {
	tests := []struct {
		name string
		args string
		want any
	}{
		{ToolStartBackgroundTask, `{"objective":"research","presentation_hint":"brief"}`, StartBackgroundTaskArgs{Objective: "research", PresentationHint: "brief"}},
		{ToolSteerBackgroundTask, `{"handoff_id":"handoff-1","objective":"focus on risks"}`, SteerBackgroundTaskArgs{HandoffID: identity.HandoffID("handoff-1"), Objective: "focus on risks"}},
		{ToolCancelBackgroundTask, `{"handoff_id":"handoff-1","reason":"no longer needed"}`, CancelBackgroundTaskArgs{HandoffID: identity.HandoffID("handoff-1"), Reason: "no longer needed"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseReservedTool(tc.name, json.RawMessage(tc.args))
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestParseReservedTool_RejectsMalformedAndBoundedInputs(t *testing.T) {
	deep := strings.Repeat("[", 65) + strings.Repeat("]", 65)
	tests := []struct {
		name string
		tool string
		args []byte
	}{
		{"unknown tool", "other", []byte(`{}`)},
		{"empty", ToolStartBackgroundTask, nil},
		{"not object", ToolStartBackgroundTask, []byte(`[]`)},
		{"trailing", ToolStartBackgroundTask, []byte(`{"objective":"x"}{}`)},
		{"oversize", ToolStartBackgroundTask, []byte(`{"objective":"` + strings.Repeat("x", 64*1024) + `"}`)},
		{"deep", ToolStartBackgroundTask, []byte(`{"objective":"x","presentation_hint":` + deep + `}`)},
		{"duplicate", ToolStartBackgroundTask, []byte(`{"objective":"one","objective":"two"}`)},
		{"unknown field", ToolStartBackgroundTask, []byte(`{"objective":"x","other":1}`)},
		{"invalid utf8", ToolStartBackgroundTask, []byte{'{', '"', 'o', 'b', 'j', 'e', 'c', 't', 'i', 'v', 'e', '"', ':', '"', 0xff, '"', '}'}},
		{"blank start objective", ToolStartBackgroundTask, []byte(`{"objective":" "}`)},
		{"start objective over limit", ToolStartBackgroundTask, []byte(`{"objective":"` + strings.Repeat("x", 8*1024+1) + `"}`)},
		{"blank steer id", ToolSteerBackgroundTask, []byte(`{"handoff_id":"","objective":"x"}`)},
		{"blank steer objective", ToolSteerBackgroundTask, []byte(`{"handoff_id":"h","objective":" "}`)},
		{"blank cancel id", ToolCancelBackgroundTask, []byte(`{"handoff_id":""}`)},
		{"cancel reason over limit", ToolCancelBackgroundTask, []byte(`{"handoff_id":"h","reason":"` + strings.Repeat("r", 2*1024+1) + `"}`)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseReservedTool(tc.tool, tc.args)
			assert.ErrorIs(t, err, ErrInvalidRequest)
		})
	}

	atLimit := []byte(`{"objective":"` + strings.Repeat("x", 8*1024) + `"}`)
	_, err := ParseReservedTool(ToolStartBackgroundTask, atLimit)
	require.NoError(t, err)
}

func TestParseReservedTool_StartRejectsEveryAuthoritativeField(t *testing.T) {
	fields := []string{
		"handoff_id", "event_id", "tenant_id", "user_id", "conversation_id", "session_id", "source_turn_id",
		"source_turn_revision", "deadline", "token_budget", "priority", "tool_allowlist", "approval_policy",
		"versions", "context_ref", "context_hash", "idempotency_key",
	}
	for _, field := range fields {
		t.Run(field, func(t *testing.T) {
			_, err := ParseReservedTool(ToolStartBackgroundTask, json.RawMessage(`{"objective":"ok","`+field+`":"model-value"}`))
			assert.ErrorIs(t, err, ErrInvalidRequest)
		})
	}
}

func TestParseReservedTool_SteerAndCancelRejectServerDerivedFields(t *testing.T) {
	for _, tool := range []string{ToolSteerBackgroundTask, ToolCancelBackgroundTask} {
		for _, field := range []string{"expected_revision", "actor", "idempotency_key", "event_id"} {
			t.Run(tool+"/"+field, func(t *testing.T) {
				body := `{"handoff_id":"h","` + field + `":"model-value"}`
				if tool == ToolSteerBackgroundTask {
					body = `{"handoff_id":"h","objective":"x","` + field + `":"model-value"}`
				}
				_, err := ParseReservedTool(tool, json.RawMessage(body))
				assert.ErrorIs(t, err, ErrInvalidRequest)
			})
		}
	}
}

func TestParseReservedTool_RejectsCaseAliasesAndFoldedDuplicates(t *testing.T) {
	tests := []struct {
		name string
		tool string
		body string
	}{
		{"start objective alias", ToolStartBackgroundTask, `{"OBJECTIVE":"x"}`},
		{"start presentation alias", ToolStartBackgroundTask, `{"objective":"x","PRESENTATION_HINT":"brief"}`},
		{"start folded duplicate", ToolStartBackgroundTask, `{"objective":"one","OBJECTIVE":"two"}`},
		{"steer id alias", ToolSteerBackgroundTask, `{"HANDOFF_ID":"h","objective":"x"}`},
		{"steer objective alias", ToolSteerBackgroundTask, `{"handoff_id":"h","OBJECTIVE":"x"}`},
		{"steer folded duplicate", ToolSteerBackgroundTask, `{"handoff_id":"h","HANDOFF_ID":"other","objective":"x"}`},
		{"cancel id alias", ToolCancelBackgroundTask, `{"HANDOFF_ID":"h"}`},
		{"cancel reason alias", ToolCancelBackgroundTask, `{"handoff_id":"h","REASON":"x"}`},
		{"cancel folded duplicate", ToolCancelBackgroundTask, `{"handoff_id":"h","reason":"one","REASON":"two"}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseReservedTool(tc.tool, json.RawMessage(tc.body))
			assert.ErrorIs(t, err, ErrInvalidRequest)
		})
	}
}

func TestParseReservedTool_RejectsNullWrongTypesAndMissingRequiredFields(t *testing.T) {
	tests := []struct {
		name string
		tool string
		body string
	}{
		{"start objective null", ToolStartBackgroundTask, `{"objective":null}`},
		{"start presentation null", ToolStartBackgroundTask, `{"objective":"x","presentation_hint":null}`},
		{"start objective wrong type", ToolStartBackgroundTask, `{"objective":1}`},
		{"start presentation wrong type", ToolStartBackgroundTask, `{"objective":"x","presentation_hint":false}`},
		{"start objective missing", ToolStartBackgroundTask, `{}`},
		{"start presentation blank", ToolStartBackgroundTask, `{"objective":"x","presentation_hint":" "}`},
		{"steer id null", ToolSteerBackgroundTask, `{"handoff_id":null,"objective":"x"}`},
		{"steer objective null", ToolSteerBackgroundTask, `{"handoff_id":"h","objective":null}`},
		{"steer id wrong type", ToolSteerBackgroundTask, `{"handoff_id":1,"objective":"x"}`},
		{"steer objective wrong type", ToolSteerBackgroundTask, `{"handoff_id":"h","objective":[]}`},
		{"steer id missing", ToolSteerBackgroundTask, `{"objective":"x"}`},
		{"steer objective missing", ToolSteerBackgroundTask, `{"handoff_id":"h"}`},
		{"cancel id null", ToolCancelBackgroundTask, `{"handoff_id":null}`},
		{"cancel reason null", ToolCancelBackgroundTask, `{"handoff_id":"h","reason":null}`},
		{"cancel id wrong type", ToolCancelBackgroundTask, `{"handoff_id":true}`},
		{"cancel reason wrong type", ToolCancelBackgroundTask, `{"handoff_id":"h","reason":{}}`},
		{"cancel id missing", ToolCancelBackgroundTask, `{}`},
		{"cancel reason blank", ToolCancelBackgroundTask, `{"handoff_id":"h","reason":"\t"}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseReservedTool(tc.tool, json.RawMessage(tc.body))
			assert.ErrorIs(t, err, ErrInvalidRequest)
		})
	}
}

func TestParseReservedTool_EnforcesEveryFieldByteBoundary(t *testing.T) {
	tests := []struct {
		name      string
		tool      string
		atLimit   string
		overLimit string
	}{
		{
			name: "start presentation hint", tool: ToolStartBackgroundTask,
			atLimit:   `{"objective":"x","presentation_hint":"` + strings.Repeat("p", 2*1024) + `"}`,
			overLimit: `{"objective":"x","presentation_hint":"` + strings.Repeat("p", 2*1024+1) + `"}`,
		},
		{
			name: "steer handoff id", tool: ToolSteerBackgroundTask,
			atLimit:   `{"handoff_id":"` + strings.Repeat("h", 256) + `","objective":"x"}`,
			overLimit: `{"handoff_id":"` + strings.Repeat("h", 257) + `","objective":"x"}`,
		},
		{
			name: "steer objective", tool: ToolSteerBackgroundTask,
			atLimit:   `{"handoff_id":"h","objective":"` + strings.Repeat("o", 8*1024) + `"}`,
			overLimit: `{"handoff_id":"h","objective":"` + strings.Repeat("o", 8*1024+1) + `"}`,
		},
		{
			name: "cancel handoff id", tool: ToolCancelBackgroundTask,
			atLimit:   `{"handoff_id":"` + strings.Repeat("h", 256) + `"}`,
			overLimit: `{"handoff_id":"` + strings.Repeat("h", 257) + `"}`,
		},
		{
			name: "cancel reason", tool: ToolCancelBackgroundTask,
			atLimit:   `{"handoff_id":"h","reason":"` + strings.Repeat("r", 2*1024) + `"}`,
			overLimit: `{"handoff_id":"h","reason":"` + strings.Repeat("r", 2*1024+1) + `"}`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name+" at limit", func(t *testing.T) {
			_, err := ParseReservedTool(tc.tool, json.RawMessage(tc.atLimit))
			require.NoError(t, err)
		})
		t.Run(tc.name+" limit plus one", func(t *testing.T) {
			_, err := ParseReservedTool(tc.tool, json.RawMessage(tc.overLimit))
			assert.ErrorIs(t, err, ErrInvalidRequest)
		})
	}
}
