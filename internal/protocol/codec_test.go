package protocol

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/testkit"
	"github.com/chuanmingliu/openai-gpt-live/internal/toolpolicy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testLimits = Limits{MaxFrameBytes: 16_384, MaxJSONDepth: 12, MaxAudioBytes: 1_920}

func TestDecodeClientCanonicalizesToolPolicyUpdate(t *testing.T) {
	event, err := DecodeClient([]byte(`{"type":"live.tool_policy.update","event_id":"policy-1","tools":{"web_search":{"enabled":true,"safe_search":"moderate","region":"wt-wt","max_results":5,"timeout_ms":8000}}}`), testLimits)
	require.NoError(t, err)
	assert.Equal(t, LiveToolPolicyUpdateEvent{
		Type: "live.tool_policy.update", EventID: "policy-1", Tools: toolpolicy.DefaultPolicy(),
	}, event)
}

func TestDecodeClientRejectsNonCanonicalToolPolicyUpdates(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{"executable field", `{"type":"live.tool_policy.update","event_id":"policy-1","tools":{"web_search":{"enabled":true,"safe_search":"moderate","region":"wt-wt","max_results":5,"timeout_ms":8000,"command":"npx"}}}`},
		{"unknown tool", `{"type":"live.tool_policy.update","event_id":"policy-1","tools":{"web_search":{"enabled":true,"safe_search":"moderate","region":"wt-wt","max_results":5,"timeout_ms":8000},"shell":{}}}`},
		{"duplicate nested key", `{"type":"live.tool_policy.update","event_id":"policy-1","tools":{"web_search":{"enabled":true,"enabled":false,"safe_search":"moderate","region":"wt-wt","max_results":5,"timeout_ms":8000}}}`},
		{"unsafe region", `{"type":"live.tool_policy.update","event_id":"policy-1","tools":{"web_search":{"enabled":true,"safe_search":"moderate","region":"ru-ru","max_results":5,"timeout_ms":8000}}}`},
		{"excess results", `{"type":"live.tool_policy.update","event_id":"policy-1","tools":{"web_search":{"enabled":true,"safe_search":"moderate","region":"wt-wt","max_results":11,"timeout_ms":8000}}}`},
		{"excess timeout", `{"type":"live.tool_policy.update","event_id":"policy-1","tools":{"web_search":{"enabled":true,"safe_search":"moderate","region":"wt-wt","max_results":5,"timeout_ms":10001}}}`},
		{"unknown top-level field", `{"type":"live.tool_policy.update","event_id":"policy-1","tools":{"web_search":{"enabled":true,"safe_search":"moderate","region":"wt-wt","max_results":5,"timeout_ms":8000}},"model":"browser-model"}`},
		{"empty event id", `{"type":"live.tool_policy.update","event_id":"","tools":{"web_search":{"enabled":true,"safe_search":"moderate","region":"wt-wt","max_results":5,"timeout_ms":8000}}}`},
		{"oversized event id", `{"type":"live.tool_policy.update","event_id":"` + strings.Repeat("x", 129) + `","tools":{"web_search":{"enabled":true,"safe_search":"moderate","region":"wt-wt","max_results":5,"timeout_ms":8000}}}`},
		{"control character event id", "{\"type\":\"live.tool_policy.update\",\"event_id\":\"policy\\u000a1\",\"tools\":{\"web_search\":{\"enabled\":true,\"safe_search\":\"moderate\",\"region\":\"wt-wt\",\"max_results\":5,\"timeout_ms\":8000}}}"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeClient([]byte(tt.raw), testLimits)
			require.Error(t, err)
		})
	}
}

func TestLiveToolPolicyUpdatedRoundTripsCanonicalAcknowledgement(t *testing.T) {
	event := LiveToolPolicyUpdatedEvent{
		Type: "live.tool_policy.updated", EventID: "policy-1", Tools: toolpolicy.DefaultPolicy(), Model: "deepseek-v4-flash",
	}
	raw, err := Encode(event)
	require.NoError(t, err)
	decoded, err := DecodeServer(raw, testLimits)
	require.NoError(t, err)
	assert.Equal(t, event, decoded)

	for _, invalid := range []string{
		`{"type":"live.tool_policy.updated","event_id":"policy-1","tools":{"web_search":{"enabled":true,"safe_search":"moderate","region":"wt-wt","max_results":5,"timeout_ms":8000}},"model":"deepseek-v4-flash","command":"npx"}`,
		`{"type":"live.tool_policy.updated","event_id":"policy-1","tools":{"web_search":{"enabled":true,"safe_search":"moderate","region":"xx-xx","max_results":5,"timeout_ms":8000}},"model":"deepseek-v4-flash"}`,
		`{"type":"live.tool_policy.updated","event_id":"policy-1","tools":{"web_search":{"enabled":true,"safe_search":"moderate","region":"wt-wt","max_results":5,"timeout_ms":8000}},"model":""}`,
	} {
		_, err := DecodeServer([]byte(invalid), testLimits)
		require.Error(t, err)
	}
}

func TestDecodeClientDispatchesEverySupportedType(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want any
	}{
		{"session update wrong branch", `{"type":"session.update","event_id":"evt-1","session":{"type":"realtime","audio":{"input":{"format":{"type":"audio/pcm","rate":24000}}}}}`, SessionUpdateEvent{}},
		{"audio append wrong branch", `{"type":"input_audio_buffer.append","event_id":"evt-2","audio":"AAE="}`, InputAudioBufferAppendEvent{}},
		{"audio commit wrong branch", `{"type":"input_audio_buffer.commit","event_id":"evt-3"}`, InputAudioBufferCommitEvent{}},
		{"conversation item wrong branch", `{"type":"conversation.item.create","event_id":"evt-4","previous_item_id":"item-0","item":{"id":"item-1","type":"function_call_output","call_id":"call-1","output":"ok"}}`, ConversationItemCreateEvent{}},
		{"response create wrong branch", `{"type":"response.create","event_id":"evt-5","response":{"instructions":"brief","tool_choice":"auto"}}`, ResponseCreateEvent{}},
		{"response cancel wrong branch", `{"type":"response.cancel","event_id":"evt-6","response_id":"resp-1"}`, ResponseCancelEvent{}},
		{"approval resolve", `{"type":"live.tool.approval.resolve","invocation_id":"inv-1","decision":"approved","reason":"confirmed","idempotency_key":"decision-1"}`, LiveToolApprovalResolveEvent{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeClient([]byte(tt.raw), testLimits)
			require.NoError(t, err)
			assert.Equal(t, reflect.TypeOf(tt.want), reflect.TypeOf(got))
		})
	}
}

func TestApprovalProtocolIsExactAndContentFree(t *testing.T) {
	valid := `{"type":"live.tool.approval.resolve","invocation_id":"inv-1","decision":"denied","reason":"not expected","idempotency_key":"decision-1"}`
	event, err := DecodeClient([]byte(valid), testLimits)
	require.NoError(t, err)
	assert.Equal(t, "inv-1", event.(LiveToolApprovalResolveEvent).InvocationID)

	for _, raw := range []string{
		`{"type":"live.tool.approval.resolve","invocation_id":"inv-1","decision":"allow","reason":"x","idempotency_key":"k"}`,
		`{"type":"live.tool.approval.resolve","invocation_id":"inv-1","decision":"approved","reason":"x","idempotency_key":"k","tenant_id":"spoof"}`,
		`{"type":"live.tool.approval.resolve","invocation_id":"inv-1","decision":"approved","reason":"x","idempotency_key":"k","arguments":{"secret":true}}`,
		`{"type":"live.tool.approval.resolve","invocation_id":"inv-1","invocation_id":"inv-2","decision":"approved","reason":"x","idempotency_key":"k"}`,
	} {
		_, err := DecodeClient([]byte(raw), testLimits)
		assert.Error(t, err)
	}

	required := LiveToolApprovalRequiredEvent{Type: "live.tool.approval.required", EventID: "evt-1", SequenceID: 7, InvocationID: "inv-1", HandoffID: "handoff-1", ActionSummary: "Run side-effecting action notify", ExpiresAt: time.Now().UTC().Add(time.Minute), RiskClass: "side_effecting"}
	raw, err := Encode(required)
	require.NoError(t, err)
	_, err = DecodeServer(raw, testLimits)
	require.NoError(t, err)
	assert.NotContains(t, string(raw), "arguments")
	assert.NotContains(t, string(raw), "tenant")
}

func TestDecodeClientRejectsInvalidUTF8BeforeApprovalJSONNormalization(t *testing.T) {
	for _, raw := range [][]byte{
		append([]byte(`{"type":"live.tool.approval.resolve","invocation_id":"inv-`), append([]byte{0xff}, []byte(`","decision":"approved","reason":"x","idempotency_key":"key"}`)...)...),
		append([]byte(`{"type":"live.tool.approval.resolve","invocation_id":"inv","decision":"approved","reason":"x","idempotency_key":"key-`), append([]byte{0xfe}, []byte(`"}`)...)...),
		append([]byte(`{"type":"live.tool.approval.resolve"}`), 0xff),
	} {
		_, err := DecodeClient(raw, testLimits)
		require.ErrorContains(t, err, "valid UTF-8")
	}
}

func TestDecodeClientPreservesTypedCoreFields(t *testing.T) {
	raw := []byte(`{"type":"conversation.item.create","event_id":"evt-core","previous_item_id":"prior","item":{"id":"item-7","type":"function_call","role":"assistant","call_id":"call-7","name":"lookup","arguments":"{\"q\":\"rain\"}","content":[{"type":"input_text","text":"weather"}]}}`)

	event, err := DecodeClient(raw, testLimits)

	require.NoError(t, err)
	got, ok := event.(ConversationItemCreateEvent)
	require.True(t, ok)
	assert.Equal(t, "evt-core", got.EventID)
	assert.Equal(t, "prior", got.PreviousItemID)
	assert.Equal(t, "item-7", got.Item.ID)
	assert.Equal(t, "function_call", got.Item.Type)
	assert.Equal(t, "assistant", got.Item.Role)
	assert.Equal(t, "call-7", got.Item.CallID)
	assert.Equal(t, "lookup", got.Item.Name)
	assert.Equal(t, `{"q":"rain"}`, got.Item.Arguments)
	assert.JSONEq(t, `[{"type":"input_text","text":"weather"}]`, string(got.Item.Content))
}

func TestDecodeClientRejectsMalformedAndUnboundedMessages(t *testing.T) {
	tests := []struct {
		name   string
		raw    string
		limits Limits
	}{
		{"missing required type accepted", `{}`, testLimits},
		{"unknown type accepted", `{"type":"response.future"}`, testLimits},
		{"truncated json accepted", `{"type":"response.create"`, testLimits},
		{"duplicate sequence id accepted", `{"type":"response.create","sequence_id":4,"sequence_id":5}`, testLimits},
		{"empty session accepted", `{"type":"session.update"}`, testLimits},
		{"missing audio accepted", `{"type":"input_audio_buffer.append"}`, testLimits},
		{"invalid base64 accepted", `{"type":"input_audio_buffer.append","audio":"%%%"}`, testLimits},
		{"empty conversation item accepted", `{"type":"conversation.item.create","item":{}}`, testLimits},
		{"oversized frame accepted", `{"type":"response.create"}`, Limits{MaxFrameBytes: 10, MaxJSONDepth: 12, MaxAudioBytes: 100}},
		{"excessive json depth accepted", `{"type":"response.create","response":{"metadata":{"a":{"b":{"c":1}}}}}`, Limits{MaxFrameBytes: 1_000, MaxJSONDepth: 3, MaxAudioBytes: 100}},
		{"zero frame bound accepted", `{"type":"response.cancel"}`, Limits{MaxFrameBytes: 0, MaxJSONDepth: 3, MaxAudioBytes: 100}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeClient([]byte(tt.raw), tt.limits)
			require.Error(t, err)
		})
	}
}

func TestDecodeClientRejectsNonPCM24kSessionFormats(t *testing.T) {
	tests := []string{
		`{"type":"session.update","session":{"type":"transcription"}}`,
		`{"type":"session.update","session":{"type":"realtime","audio":{"input":{"format":{"type":"audio/pcmu"}}}}}`,
		`{"type":"session.update","session":{"type":"realtime","audio":{"input":{"format":{"type":"audio/pcm","rate":16000}}}}}`,
		`{"type":"session.update","session":{"type":"realtime","audio":{"output":{"format":{"type":"audio/pcm","rate":48000}}}}}`,
	}
	for i, raw := range tests {
		t.Run(fmt.Sprintf("format %d bypassed contract", i), func(t *testing.T) {
			_, err := DecodeClient([]byte(raw), testLimits)
			require.Error(t, err)
		})
	}
}

func TestDecodeClientRejectsInvalidPayloadForEverySupportedType(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{"session update missing session", `{"type":"session.update"}`},
		{"audio append missing audio", `{"type":"input_audio_buffer.append"}`},
		{"audio commit event id is not a string", `{"type":"input_audio_buffer.commit","event_id":7}`},
		{"conversation item kind is unsupported", `{"type":"conversation.item.create","item":{"type":"provider_future_item"}}`},
		{"response create response is not an object", `{"type":"response.create","response":"invalid"}`},
		{"response cancel response id is not a string", `{"type":"response.cancel","response_id":7}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeClient([]byte(tt.raw), testLimits)
			require.Error(t, err)
		})
	}
}

func TestDecodeClientRejectsMessageWithoutContentEntries(t *testing.T) {
	for _, raw := range []string{
		`{"type":"conversation.item.create","item":{"type":"message","role":"user","content":null}}`,
		`{"type":"conversation.item.create","item":{"type":"message","role":"user","content":[]}}`,
	} {
		_, err := DecodeClient([]byte(raw), testLimits)
		require.Error(t, err)
	}
}

func TestDecodeClientDistinguishesEmptyFromMissingFunctionCallOutput(t *testing.T) {
	t.Run("present empty output remains valid", func(t *testing.T) {
		event, err := DecodeClient([]byte(`{"type":"conversation.item.create","item":{"type":"function_call_output","call_id":"call-1","output":""}}`), testLimits)
		require.NoError(t, err)
		got, ok := event.(ConversationItemCreateEvent)
		require.True(t, ok)
		assert.Equal(t, "", got.Item.Output)
	})

	t.Run("missing output is rejected", func(t *testing.T) {
		_, err := DecodeClient([]byte(`{"type":"conversation.item.create","item":{"type":"function_call_output","call_id":"call-1"}}`), testLimits)
		require.Error(t, err)
	})
}

func TestDecodePCM24kRejectsInvalidBase64BeforeAllocation(t *testing.T) {
	_, err := DecodePCM24k(InputAudioBufferAppendEvent{Type: "input_audio_buffer.append", Audio: "AAAA"}, 2)
	require.Error(t, err)

	_, err = DecodePCM24k(InputAudioBufferAppendEvent{Type: "input_audio_buffer.append", Audio: "A!AA"}, 100)
	require.Error(t, err)
}

func TestDecodePCM24kAllowsSampleSplitAcrossAppends(t *testing.T) {
	first, err := DecodePCM24k(InputAudioBufferAppendEvent{Type: "input_audio_buffer.append", Audio: "Bw=="}, 2)
	require.NoError(t, err)
	second, err := DecodePCM24k(InputAudioBufferAppendEvent{Type: "input_audio_buffer.append", Audio: "JAA="}, 2)
	require.NoError(t, err)
	assert.Equal(t, []byte{7}, first)
	assert.Equal(t, []byte{36, 0}, second)
}

func TestPCMFramerCarriesTailsAcrossAppends(t *testing.T) {
	pcm := testkit.RampPCM(1_920)
	framer := NewPCMFramer()
	first, err := framer.Push(pcm[:480])
	require.NoError(t, err)
	assert.Empty(t, first)
	frames, err := framer.Push(pcm[480:1_440])
	require.NoError(t, err)
	require.Len(t, frames, 1)
	assert.Equal(t, pcm[:960], frames[0])
	tail, err := framer.Flush()
	require.NoError(t, err)
	assert.Equal(t, pcm[960:1_440], tail)
}

func TestPCMFramerRejectsOddCommitWithoutDroppingTail(t *testing.T) {
	framer := NewPCMFramer()
	_, err := framer.Push([]byte{1, 2, 3})
	require.NoError(t, err)
	_, err = framer.Flush()
	require.Error(t, err)

	_, err = framer.Push([]byte{4})
	require.NoError(t, err)
	tail, err := framer.Flush()
	require.NoError(t, err)
	assert.Equal(t, []byte{1, 2, 3, 4}, tail)
}

func TestPCMFramerReturnsEven480ByteFinalTail(t *testing.T) {
	want := testkit.RampPCM(480)
	framer := NewPCMFramer()
	frames, err := framer.Push(want)
	require.NoError(t, err)
	assert.Empty(t, frames)

	got, err := framer.Flush()
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestPCMFramerRejectsEmptyCommit(t *testing.T) {
	_, err := NewPCMFramer().Flush()
	require.Error(t, err)
}

func TestPCMFramerAcceptsCommitAfterCompleteFrames(t *testing.T) {
	framer := NewPCMFramer()
	frames, err := framer.Push(testkit.RampPCM(960))
	require.NoError(t, err)
	require.Len(t, frames, 1)

	tail, err := framer.Flush()
	require.NoError(t, err)
	assert.Empty(t, tail)

	_, err = framer.Flush()
	require.Error(t, err)
}

func TestPCMFramerResetDropsPriorPartialFrame(t *testing.T) {
	framer := NewPCMFramer()
	_, err := framer.Push([]byte{1, 2, 3, 4})
	require.NoError(t, err)
	framer.Reset()
	_, err = framer.Push([]byte{9, 10})
	require.NoError(t, err)

	got, err := framer.Flush()
	require.NoError(t, err)
	assert.Equal(t, []byte{9, 10}, got)
}

func TestPCMFramerReturnsIndependentFrames(t *testing.T) {
	input := testkit.RampPCM(960)
	framer := NewPCMFramer()
	frames, err := framer.Push(input)
	require.NoError(t, err)
	require.Len(t, frames, 1)
	input[0] = 255
	assert.Equal(t, byte(7), frames[0][0])
}

func TestServerEventsRoundTripEverySupportedType(t *testing.T) {
	session := json.RawMessage(`{"id":"sess-1","provider_extra":{"kept":true}}`)
	previousItemID := "assistant-or-tool-item"
	response := ResponsePayload{
		ID:     "resp-1",
		Status: "completed",
		Raw:    json.RawMessage(`{"id":"resp-1","status":"completed","provider_extra":7}`),
	}
	details := json.RawMessage(`{"step":"search","percent":50}`)
	events := []ServerEvent{
		SessionCreatedEvent{Type: "session.created", EventID: "e1", Session: session},
		SessionUpdatedEvent{Type: "session.updated", EventID: "e2", Session: session},
		ErrorEvent{Type: "error", EventID: "e3", Error: ProtocolError{Type: "invalid_request_error", Code: "bad", Message: "bad request", Param: "audio", EventID: "source"}},
		InputAudioBufferCommittedEvent{Type: "input_audio_buffer.committed", EventID: "e3-committed", PreviousItemID: &previousItemID, ItemID: "item-committed"},
		InputAudioBufferCommittedEvent{Type: "input_audio_buffer.committed", EventID: "e3-committed-null", PreviousItemID: nil, ItemID: "item-first"},
		InputAudioBufferSpeechStartedEvent{Type: "input_audio_buffer.speech_started", EventID: "e4", AudioStartMS: 120, ItemID: "item-1"},
		InputAudioBufferSpeechStoppedEvent{Type: "input_audio_buffer.speech_stopped", EventID: "e5", AudioEndMS: 420, ItemID: "item-1"},
		ConversationItemInputAudioTranscriptionDeltaEvent{Type: "conversation.item.input_audio_transcription.delta", EventID: "e6", ItemID: "item-1", ContentIndex: 0, Delta: "hel"},
		ConversationItemInputAudioTranscriptionCompletedEvent{Type: "conversation.item.input_audio_transcription.completed", EventID: "e7", ItemID: "item-1", ContentIndex: 0, Transcript: "hello"},
		ResponseCreatedEvent{Type: "response.created", EventID: "e8", Response: response},
		ResponseOutputAudioDeltaEvent{Type: "response.output_audio.delta", EventID: "e9", ResponseID: "resp-1", ItemID: "item-2", OutputIndex: 0, ContentIndex: 0, Delta: base64.StdEncoding.EncodeToString([]byte{1, 2})},
		ResponseOutputAudioDoneEvent{Type: "response.output_audio.done", EventID: "e10", ResponseID: "resp-1", ItemID: "item-2", OutputIndex: 0, ContentIndex: 0},
		ResponseOutputAudioTranscriptDeltaEvent{Type: "response.output_audio_transcript.delta", EventID: "e11", ResponseID: "resp-1", ItemID: "item-2", OutputIndex: 0, ContentIndex: 0, Delta: "hi"},
		ResponseOutputAudioTranscriptDoneEvent{Type: "response.output_audio_transcript.done", EventID: "e12", ResponseID: "resp-1", ItemID: "item-2", OutputIndex: 0, ContentIndex: 0, Transcript: "hi there"},
		ResponseFunctionCallArgumentsDoneEvent{Type: "response.function_call_arguments.done", EventID: "e13", ResponseID: "resp-1", ItemID: "item-3", OutputIndex: 1, CallID: "call-1", Name: "lookup", Arguments: `{"q":"x"}`},
		ResponseDoneEvent{Type: "response.done", EventID: "e14", Response: response},
		LiveHandoffCreatedEvent{Type: "live.handoff.created", EventID: "e15", SequenceID: 1, HandoffID: "h-1", Status: "queued", Details: details},
		LiveHandoffProgressEvent{Type: "live.handoff.progress", EventID: "e16", SequenceID: 2, HandoffID: "h-1", Status: "running", Details: details},
		LiveHandoffNeedsInputEvent{Type: "live.handoff.needs_input", EventID: "e17", SequenceID: 3, HandoffID: "h-1", Status: "needs_input", Details: details},
		LiveHandoffCompletedEvent{Type: "live.handoff.completed", EventID: "e18", SequenceID: 4, HandoffID: "h-1", Status: "completed", Details: details},
		LiveHandoffFailedEvent{Type: "live.handoff.failed", EventID: "e19", SequenceID: 5, HandoffID: "h-1", Status: "failed", Details: details},
		LiveHandoffCancelledEvent{Type: "live.handoff.cancelled", EventID: "e20", SequenceID: 6, HandoffID: "h-1", Status: "cancelled", Details: details},
		LiveSessionDegradedEvent{Type: "live.session.degraded", EventID: "e21", SequenceID: 7, Status: "degraded", Details: details},
		LiveSessionResumedEvent{Type: "live.session.resumed", EventID: "e22", SequenceID: 8, Status: "resumed", Details: details},
	}
	for _, event := range events {
		t.Run(event.EventType()+" wrong dispatch or field loss", func(t *testing.T) {
			raw, err := Encode(event)
			require.NoError(t, err)
			decoded, err := DecodeServer(raw, testLimits)
			require.NoError(t, err)
			assert.Equal(t, event, decoded)
		})
	}
}

func TestDecodeServerExposesResponseCoreAndPreservesProviderPayload(t *testing.T) {
	raw := []byte(`{"type":"response.done","event_id":"evt-response","response":{"id":"resp-typed","status":"failed","provider_status_details":{"reason":"content_filter"}}}`)

	event, err := DecodeServer(raw, testLimits)

	require.NoError(t, err)
	got, ok := event.(ResponseDoneEvent)
	require.True(t, ok)
	assert.Equal(t, "resp-typed", got.Response.ID)
	assert.Equal(t, "failed", got.Response.Status)
	assert.JSONEq(t, `{"id":"resp-typed","status":"failed","provider_status_details":{"reason":"content_filter"}}`, string(got.Response.Raw))

	reencoded, err := Encode(got)
	require.NoError(t, err)
	assert.JSONEq(t, string(raw), string(reencoded))
}

func TestDecodeServerRejectsUnknownAndOversizedEvents(t *testing.T) {
	_, err := DecodeServer([]byte(`{"type":"server.future"}`), testLimits)
	require.Error(t, err)
	_, err = DecodeServer([]byte(`{"type":"response.done","response":{}}`), Limits{MaxFrameBytes: 8, MaxJSONDepth: 8, MaxAudioBytes: 10})
	require.Error(t, err)
}

func TestSessionReconnectUpdatedRoundTripsBoundedMetadata(t *testing.T) {
	event := SessionReconnectUpdatedEvent{Type: "session.reconnect.updated", EventID: "manager-credential-2", Reconnect: ReconnectMetadata{
		Credential: "lrt1.payload.signature", SessionID: "session", ConversationID: "conversation", Incarnation: 2,
		Cursor: 19, Model: "provider-model", ExpiresAt: time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC),
	}}
	raw, err := Encode(event)
	require.NoError(t, err)
	decoded, err := DecodeServer(raw, testLimits)
	require.NoError(t, err)
	assert.Equal(t, event, decoded)

	event.Reconnect.Cursor = -1
	raw, err = json.Marshal(event)
	require.NoError(t, err)
	_, err = DecodeServer(raw, testLimits)
	assert.Error(t, err)
}

func TestDecodeServerRejectsOversizedAudioDelta(t *testing.T) {
	raw := `{"type":"response.output_audio.delta","event_id":"evt","response_id":"resp-1","item_id":"item-1","output_index":0,"content_index":0,"delta":"` + strings.Repeat("A", 8) + `"}`
	_, err := DecodeServer([]byte(raw), Limits{MaxFrameBytes: 1_000, MaxJSONDepth: 8, MaxAudioBytes: 3})
	require.Error(t, err)
}

func TestDecodeServerRejectsMissingOrEmptyEventIDForEverySupportedType(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{"session created", `{"type":"session.created","event_id":"evt","session":{}}`},
		{"session updated", `{"type":"session.updated","event_id":"evt","session":{}}`},
		{"error", `{"type":"error","event_id":"evt","error":{"type":"invalid_request_error","message":"bad"}}`},
		{"speech started", `{"type":"input_audio_buffer.speech_started","event_id":"evt","item_id":"item-1","audio_start_ms":0}`},
		{"speech stopped", `{"type":"input_audio_buffer.speech_stopped","event_id":"evt","item_id":"item-1","audio_end_ms":0}`},
		{"transcription delta", `{"type":"conversation.item.input_audio_transcription.delta","event_id":"evt","item_id":"item-1","content_index":0,"delta":""}`},
		{"transcription completed", `{"type":"conversation.item.input_audio_transcription.completed","event_id":"evt","item_id":"item-1","content_index":0,"transcript":""}`},
		{"response created", `{"type":"response.created","event_id":"evt","response":{"id":"resp-1","status":"in_progress"}}`},
		{"audio delta", `{"type":"response.output_audio.delta","event_id":"evt","response_id":"resp-1","item_id":"item-1","output_index":0,"content_index":0,"delta":"AA=="}`},
		{"audio done", `{"type":"response.output_audio.done","event_id":"evt","response_id":"resp-1","item_id":"item-1","output_index":0,"content_index":0}`},
		{"audio transcript delta", `{"type":"response.output_audio_transcript.delta","event_id":"evt","response_id":"resp-1","item_id":"item-1","output_index":0,"content_index":0,"delta":""}`},
		{"audio transcript done", `{"type":"response.output_audio_transcript.done","event_id":"evt","response_id":"resp-1","item_id":"item-1","output_index":0,"content_index":0,"transcript":""}`},
		{"function arguments done", `{"type":"response.function_call_arguments.done","event_id":"evt","response_id":"resp-1","item_id":"item-1","output_index":0,"call_id":"call-1","name":"lookup","arguments":"{}"}`},
		{"response done", `{"type":"response.done","event_id":"evt","response":{"id":"resp-1","status":"completed"}}`},
		{"handoff created", `{"type":"live.handoff.created","event_id":"evt","sequence_id":0,"handoff_id":"h-1","status":"queued"}`},
		{"handoff progress", `{"type":"live.handoff.progress","event_id":"evt","sequence_id":0,"handoff_id":"h-1","status":"running"}`},
		{"handoff needs input", `{"type":"live.handoff.needs_input","event_id":"evt","sequence_id":0,"handoff_id":"h-1","status":"needs_input"}`},
		{"handoff completed", `{"type":"live.handoff.completed","event_id":"evt","sequence_id":0,"handoff_id":"h-1","status":"completed"}`},
		{"handoff failed", `{"type":"live.handoff.failed","event_id":"evt","sequence_id":0,"handoff_id":"h-1","status":"failed"}`},
		{"handoff cancelled", `{"type":"live.handoff.cancelled","event_id":"evt","sequence_id":0,"handoff_id":"h-1","status":"cancelled"}`},
		{"session degraded", `{"type":"live.session.degraded","event_id":"evt","sequence_id":0,"status":"degraded"}`},
		{"session resumed", `{"type":"live.session.resumed","event_id":"evt","sequence_id":0,"status":"resumed"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name+" missing event id", func(t *testing.T) {
			raw := strings.Replace(tt.raw, `"event_id":"evt",`, "", 1)
			_, err := DecodeServer([]byte(raw), testLimits)
			require.Error(t, err)
		})
		t.Run(tt.name+" empty event id", func(t *testing.T) {
			raw := strings.Replace(tt.raw, `"event_id":"evt"`, `"event_id":""`, 1)
			_, err := DecodeServer([]byte(raw), testLimits)
			require.Error(t, err)
		})
	}
}

func TestDecodeServerRejectsMissingLifecycleFieldsForEverySupportedType(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{"session created missing session", `{"type":"session.created","event_id":"evt"}`},
		{"session updated session is null", `{"type":"session.updated","event_id":"evt","session":null}`},
		{"error missing message", `{"type":"error","event_id":"evt","error":{"type":"invalid_request_error"}}`},
		{"speech started missing zero audio start", `{"type":"input_audio_buffer.speech_started","event_id":"evt","item_id":"item-1"}`},
		{"speech stopped missing zero audio end", `{"type":"input_audio_buffer.speech_stopped","event_id":"evt","item_id":"item-1"}`},
		{"transcription delta missing zero content index", `{"type":"conversation.item.input_audio_transcription.delta","event_id":"evt","item_id":"item-1","delta":""}`},
		{"transcription completed missing transcript", `{"type":"conversation.item.input_audio_transcription.completed","event_id":"evt","item_id":"item-1","content_index":0}`},
		{"response created missing response status", `{"type":"response.created","event_id":"evt","response":{"id":"resp-1"}}`},
		{"audio delta missing zero output index", `{"type":"response.output_audio.delta","event_id":"evt","response_id":"resp-1","item_id":"item-1","content_index":0,"delta":"AA=="}`},
		{"audio done missing zero content index", `{"type":"response.output_audio.done","event_id":"evt","response_id":"resp-1","item_id":"item-1","output_index":0}`},
		{"audio transcript delta missing delta", `{"type":"response.output_audio_transcript.delta","event_id":"evt","response_id":"resp-1","item_id":"item-1","output_index":0,"content_index":0}`},
		{"audio transcript done missing transcript", `{"type":"response.output_audio_transcript.done","event_id":"evt","response_id":"resp-1","item_id":"item-1","output_index":0,"content_index":0}`},
		{"function arguments done missing call id", `{"type":"response.function_call_arguments.done","event_id":"evt","response_id":"resp-1","item_id":"item-1","output_index":0,"name":"lookup","arguments":"{}"}`},
		{"response done missing response", `{"type":"response.done","event_id":"evt"}`},
		{"handoff created missing zero sequence", `{"type":"live.handoff.created","event_id":"evt","handoff_id":"h-1","status":"queued"}`},
		{"handoff progress missing handoff id", `{"type":"live.handoff.progress","event_id":"evt","sequence_id":0,"status":"running"}`},
		{"handoff needs input missing status", `{"type":"live.handoff.needs_input","event_id":"evt","sequence_id":0,"handoff_id":"h-1"}`},
		{"handoff completed missing zero sequence", `{"type":"live.handoff.completed","event_id":"evt","handoff_id":"h-1","status":"completed"}`},
		{"handoff failed missing handoff id", `{"type":"live.handoff.failed","event_id":"evt","sequence_id":0,"status":"failed"}`},
		{"handoff cancelled missing status", `{"type":"live.handoff.cancelled","event_id":"evt","sequence_id":0,"handoff_id":"h-1"}`},
		{"session degraded missing zero sequence", `{"type":"live.session.degraded","event_id":"evt","status":"degraded"}`},
		{"session resumed missing status", `{"type":"live.session.resumed","event_id":"evt","sequence_id":0}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeServer([]byte(tt.raw), testLimits)
			require.Error(t, err)
		})
	}
}

func TestEncodeRejectsNilAndUnsupportedValues(t *testing.T) {
	_, err := Encode(nil)
	require.Error(t, err)
	_, err = Encode(struct{ Value string }{Value: "not-an-event"})
	require.Error(t, err)
}
