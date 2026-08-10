package protocol

type CapabilityDocument struct {
	Protocol     string      `json:"protocol"`
	Version      string      `json:"version"`
	ClientEvents []string    `json:"client_events"`
	ServerEvents []string    `json:"server_events"`
	Extensions   []string    `json:"extensions"`
	Audio        PCMContract `json:"audio"`
	Limits       Limits      `json:"limits"`
}

type PCMContract struct {
	Encoding        string `json:"encoding"`
	Transport       string `json:"transport"`
	SampleRate      int    `json:"sample_rate"`
	Channels        int    `json:"channels"`
	BytesPerSample  int    `json:"bytes_per_sample"`
	FrameDurationMS int    `json:"frame_duration_ms"`
	FrameBytes      int    `json:"frame_bytes"`
}

var clientEventTypes = [...]string{
	"session.update",
	"input_audio_buffer.append",
	"input_audio_buffer.commit",
	"conversation.item.create",
	"response.create",
	"response.cancel",
	"live.tool.approval.resolve",
}

var serverEventTypes = [...]string{
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
}

var handoffExtensionEventTypes = [...]string{
	"live.handoff.created",
	"live.handoff.progress",
	"live.handoff.needs_input",
	"live.handoff.completed",
	"live.handoff.failed",
	"live.handoff.cancelled",
}

var extensionEventTypes = [...]string{
	"live.session.degraded",
	"live.session.resumed",
	"live.tool.approval.required",
}

type CapabilityFeatures struct {
	Handoffs bool
}

func Capabilities(limits Limits) CapabilityDocument {
	return CapabilitiesForFeatures(limits, CapabilityFeatures{Handoffs: true})
}

func CapabilitiesForFeatures(limits Limits, features CapabilityFeatures) CapabilityDocument {
	extensions := make([]string, 0, len(handoffExtensionEventTypes)+len(extensionEventTypes))
	if features.Handoffs {
		extensions = append(extensions, handoffExtensionEventTypes[:]...)
	}
	extensions = append(extensions, extensionEventTypes[:]...)
	return CapabilityDocument{
		Protocol:     "openai-realtime-subset",
		Version:      "1.0.0",
		ClientEvents: append([]string(nil), clientEventTypes[:]...),
		ServerEvents: append([]string(nil), serverEventTypes[:]...),
		Extensions:   extensions,
		Audio: PCMContract{
			Encoding:        "pcm_s16le",
			Transport:       "base64",
			SampleRate:      24_000,
			Channels:        1,
			BytesPerSample:  2,
			FrameDurationMS: 20,
			FrameBytes:      pcmFrameBytes,
		},
		Limits: limits,
	}
}
