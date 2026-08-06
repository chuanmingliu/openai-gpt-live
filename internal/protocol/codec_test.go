package protocol

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/chuanmingliu/openai-gpt-live/internal/testkit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testLimits = Limits{MaxFrameBytes: 16_384, MaxJSONDepth: 12, MaxAudioBytes: 1_920}

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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeClient([]byte(tt.raw), testLimits)
			require.NoError(t, err)
			assert.Equal(t, reflect.TypeOf(tt.want), reflect.TypeOf(got))
		})
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
	response := json.RawMessage(`{"id":"resp-1","status":"completed","provider_extra":7}`)
	details := json.RawMessage(`{"step":"search","percent":50}`)
	events := []ServerEvent{
		SessionCreatedEvent{Type: "session.created", EventID: "e1", Session: session},
		SessionUpdatedEvent{Type: "session.updated", EventID: "e2", Session: session},
		ErrorEvent{Type: "error", EventID: "e3", Error: ProtocolError{Type: "invalid_request_error", Code: "bad", Message: "bad request", Param: "audio", EventID: "source"}},
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

func TestDecodeServerRejectsUnknownAndOversizedEvents(t *testing.T) {
	_, err := DecodeServer([]byte(`{"type":"server.future"}`), testLimits)
	require.Error(t, err)
	_, err = DecodeServer([]byte(`{"type":"response.done","response":{}}`), Limits{MaxFrameBytes: 8, MaxJSONDepth: 8, MaxAudioBytes: 10})
	require.Error(t, err)
}

func TestDecodeServerRejectsOversizedAudioDelta(t *testing.T) {
	raw := `{"type":"response.output_audio.delta","delta":"` + strings.Repeat("A", 8) + `"}`
	_, err := DecodeServer([]byte(raw), Limits{MaxFrameBytes: 1_000, MaxJSONDepth: 8, MaxAudioBytes: 3})
	require.Error(t, err)
}

func TestEncodeRejectsNilAndUnsupportedValues(t *testing.T) {
	_, err := Encode(nil)
	require.Error(t, err)
	_, err = Encode(struct{ Value string }{Value: "not-an-event"})
	require.Error(t, err)
}
