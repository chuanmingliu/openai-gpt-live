package protocol

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCapabilitiesPublishesPinnedFirstReleaseContract(t *testing.T) {
	limits := Limits{MaxFrameBytes: 1_048_576, MaxJSONDepth: 24, MaxAudioBytes: 24_000}

	got := Capabilities(limits)

	assert.Equal(t, "openai-realtime-subset", got.Protocol)
	assert.Equal(t, "1.0.0", got.Version)
	assert.Equal(t, []string{
		"session.update",
		"input_audio_buffer.append",
		"input_audio_buffer.commit",
		"conversation.item.create",
		"response.create",
		"response.cancel",
		"live.tool.approval.resolve",
		"live.tool_policy.update",
	}, got.ClientEvents)
	assert.Equal(t, []string{
		"session.created",
		"session.reconnect.updated",
		"session.updated",
		"error",
		"input_audio_buffer.speech_started",
		"input_audio_buffer.speech_stopped",
		"conversation.item.input_audio_transcription.delta",
		"conversation.item.input_audio_transcription.completed",
		"response.created",
		"response.output_audio.delta",
		"response.output_audio.done",
		"response.output_audio_transcript.delta",
		"response.output_audio_transcript.done",
		"response.function_call_arguments.done",
		"response.done",
	}, got.ServerEvents)
	assert.Equal(t, []string{
		"live.handoff.created",
		"live.handoff.progress",
		"live.handoff.needs_input",
		"live.handoff.completed",
		"live.handoff.failed",
		"live.handoff.cancelled",
		"live.session.degraded",
		"live.session.resumed",
		"live.tool.approval.required",
		"live.tool_policy.updated",
	}, got.Extensions)
	assert.Equal(t, PCMContract{
		Encoding:        "pcm_s16le",
		Transport:       "base64",
		SampleRate:      24_000,
		Channels:        1,
		BytesPerSample:  2,
		FrameDurationMS: 20,
		FrameBytes:      960,
	}, got.Audio)
	assert.Equal(t, limits, got.Limits)
}

func TestCapabilitiesReturnsIndependentEventSlices(t *testing.T) {
	first := Capabilities(Limits{MaxFrameBytes: 100, MaxJSONDepth: 4, MaxAudioBytes: 20})
	first.ClientEvents[0] = "mutated"
	first.ServerEvents[0] = "mutated"
	first.Extensions[0] = "mutated"

	second := Capabilities(Limits{MaxFrameBytes: 100, MaxJSONDepth: 4, MaxAudioBytes: 20})

	require.Equal(t, "session.update", second.ClientEvents[0])
	require.Equal(t, "session.created", second.ServerEvents[0])
	require.Equal(t, "live.handoff.created", second.Extensions[0])
}
