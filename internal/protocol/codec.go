package protocol

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
)

const pcmFrameBytes = 960

type Limits struct {
	MaxFrameBytes int `json:"max_frame_bytes"`
	MaxJSONDepth  int `json:"max_json_depth"`
	MaxAudioBytes int `json:"max_audio_bytes"`
}

func DecodeClient(raw []byte, limits Limits) (ClientEvent, error) {
	eventType, err := inspectEnvelope(raw, limits)
	if err != nil {
		return nil, err
	}

	var target any
	switch eventType {
	case "session.update":
		target = &SessionUpdateEvent{}
	case "input_audio_buffer.append":
		target = &InputAudioBufferAppendEvent{}
	case "input_audio_buffer.commit":
		target = &InputAudioBufferCommitEvent{}
	case "conversation.item.create":
		target = &ConversationItemCreateEvent{}
	case "response.create":
		target = &ResponseCreateEvent{}
	case "response.cancel":
		target = &ResponseCancelEvent{}
	default:
		return nil, fmt.Errorf("unsupported client event type %q", eventType)
	}

	if err := json.Unmarshal(raw, target); err != nil {
		return nil, fmt.Errorf("decode client event: %w", err)
	}
	event := reflect.ValueOf(target).Elem().Interface().(ClientEvent)
	if err := validateClient(event, limits); err != nil {
		return nil, err
	}
	return event, nil
}

func DecodeServer(raw []byte, limits Limits) (ServerEvent, error) {
	eventType, err := inspectEnvelope(raw, limits)
	if err != nil {
		return nil, err
	}

	var target any
	switch eventType {
	case "session.created":
		target = &SessionCreatedEvent{}
	case "session.updated":
		target = &SessionUpdatedEvent{}
	case "error":
		target = &ErrorEvent{}
	case "input_audio_buffer.speech_started":
		target = &InputAudioBufferSpeechStartedEvent{}
	case "input_audio_buffer.speech_stopped":
		target = &InputAudioBufferSpeechStoppedEvent{}
	case "conversation.item.input_audio_transcription.delta":
		target = &ConversationItemInputAudioTranscriptionDeltaEvent{}
	case "conversation.item.input_audio_transcription.completed":
		target = &ConversationItemInputAudioTranscriptionCompletedEvent{}
	case "response.created":
		target = &ResponseCreatedEvent{}
	case "response.output_audio.delta":
		target = &ResponseOutputAudioDeltaEvent{}
	case "response.output_audio.done":
		target = &ResponseOutputAudioDoneEvent{}
	case "response.output_audio_transcript.delta":
		target = &ResponseOutputAudioTranscriptDeltaEvent{}
	case "response.output_audio_transcript.done":
		target = &ResponseOutputAudioTranscriptDoneEvent{}
	case "response.function_call_arguments.done":
		target = &ResponseFunctionCallArgumentsDoneEvent{}
	case "response.done":
		target = &ResponseDoneEvent{}
	case "live.handoff.created":
		target = &LiveHandoffCreatedEvent{}
	case "live.handoff.progress":
		target = &LiveHandoffProgressEvent{}
	case "live.handoff.needs_input":
		target = &LiveHandoffNeedsInputEvent{}
	case "live.handoff.completed":
		target = &LiveHandoffCompletedEvent{}
	case "live.handoff.failed":
		target = &LiveHandoffFailedEvent{}
	case "live.handoff.cancelled":
		target = &LiveHandoffCancelledEvent{}
	case "live.session.degraded":
		target = &LiveSessionDegradedEvent{}
	case "live.session.resumed":
		target = &LiveSessionResumedEvent{}
	default:
		return nil, fmt.Errorf("unsupported server event type %q", eventType)
	}

	if err := json.Unmarshal(raw, target); err != nil {
		return nil, fmt.Errorf("decode server event: %w", err)
	}
	event := reflect.ValueOf(target).Elem().Interface().(ServerEvent)
	if audio, ok := event.(ResponseOutputAudioDeltaEvent); ok {
		if _, err := decodeBase64(audio.Delta, limits.MaxAudioBytes); err != nil {
			return nil, fmt.Errorf("decode output audio: %w", err)
		}
	}
	return event, nil
}

func Encode(event any) ([]byte, error) {
	if event == nil {
		return nil, errors.New("event is required")
	}
	value := reflect.ValueOf(event)
	if value.Kind() == reflect.Pointer && value.IsNil() {
		return nil, errors.New("event is required")
	}
	var eventType string
	switch typed := event.(type) {
	case ClientEvent:
		eventType = typed.EventType()
	case ServerEvent:
		eventType = typed.EventType()
	default:
		return nil, fmt.Errorf("unsupported event %T", event)
	}
	if eventType == "" {
		return nil, errors.New("event type is required")
	}
	raw, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("encode event: %w", err)
	}
	return raw, nil
}

func DecodePCM24k(event InputAudioBufferAppendEvent, maxBytes int) ([]byte, error) {
	if event.Audio == "" {
		return nil, errors.New("audio is required")
	}
	pcm, err := decodeBase64(event.Audio, maxBytes)
	if err != nil {
		return nil, fmt.Errorf("decode PCM24k: %w", err)
	}
	return pcm, nil
}

type PCMFramer struct {
	tail     []byte
	hasAudio bool
}

func NewPCMFramer() *PCMFramer { return &PCMFramer{} }

func (f *PCMFramer) Push(pcm []byte) ([][]byte, error) {
	if f == nil {
		return nil, errors.New("PCM framer is nil")
	}
	if len(pcm) == 0 {
		return nil, nil
	}
	f.hasAudio = true
	f.tail = append(f.tail, pcm...)
	frameCount := len(f.tail) / pcmFrameBytes
	frames := make([][]byte, 0, frameCount)
	for i := 0; i < frameCount; i++ {
		start := i * pcmFrameBytes
		frame := append([]byte(nil), f.tail[start:start+pcmFrameBytes]...)
		frames = append(frames, frame)
	}
	consumed := frameCount * pcmFrameBytes
	if consumed > 0 {
		f.tail = append([]byte(nil), f.tail[consumed:]...)
	}
	return frames, nil
}

func (f *PCMFramer) Flush() ([]byte, error) {
	if f == nil {
		return nil, errors.New("PCM framer is nil")
	}
	if !f.hasAudio {
		return nil, errors.New("PCM commit is empty")
	}
	if len(f.tail)%2 != 0 {
		return nil, errors.New("PCM commit ends with a split sample")
	}
	tail := append([]byte(nil), f.tail...)
	f.tail = nil
	f.hasAudio = false
	return tail, nil
}

func (f *PCMFramer) Reset() {
	if f != nil {
		f.tail = nil
		f.hasAudio = false
	}
}

func validateClient(event ClientEvent, limits Limits) error {
	switch typed := event.(type) {
	case SessionUpdateEvent:
		if typed.Session.Type != "realtime" {
			return fmt.Errorf("session type must be realtime, got %q", typed.Session.Type)
		}
		if err := validateSessionAudio(typed.Session.Audio); err != nil {
			return err
		}
	case InputAudioBufferAppendEvent:
		if _, err := DecodePCM24k(typed, limits.MaxAudioBytes); err != nil {
			return err
		}
	case ConversationItemCreateEvent:
		if typed.Item.Type == "" {
			return errors.New("conversation item type is required")
		}
		if err := validateConversationItem(typed.Item); err != nil {
			return err
		}
	}
	return nil
}

func validateSessionAudio(audio *SessionAudio) error {
	if audio == nil {
		return nil
	}
	formats := []*AudioFormat{}
	if audio.Input != nil && audio.Input.Format != nil {
		formats = append(formats, audio.Input.Format)
	}
	if audio.Output != nil && audio.Output.Format != nil {
		formats = append(formats, audio.Output.Format)
	}
	for _, format := range formats {
		if format.Type != "audio/pcm" || format.Rate != 24_000 {
			return fmt.Errorf("audio format must be 24 kHz audio/pcm, got type %q rate %d", format.Type, format.Rate)
		}
	}
	return nil
}

func validateConversationItem(item ConversationItem) error {
	switch item.Type {
	case "message":
		if item.Role == "" || len(item.Content) == 0 {
			return errors.New("message item requires role and content")
		}
	case "function_call":
		if item.CallID == "" || item.Name == "" || item.Arguments == "" {
			return errors.New("function call item requires call_id, name, and arguments")
		}
	case "function_call_output":
		if item.CallID == "" || item.Output == "" {
			return errors.New("function call output requires call_id and output")
		}
	}
	return nil
}

func inspectEnvelope(raw []byte, limits Limits) (string, error) {
	if limits.MaxFrameBytes <= 0 || limits.MaxJSONDepth <= 0 || limits.MaxAudioBytes <= 0 {
		return "", errors.New("all protocol limits must be positive")
	}
	if len(raw) == 0 {
		return "", errors.New("event is empty")
	}
	if len(raw) > limits.MaxFrameBytes {
		return "", fmt.Errorf("event size %d exceeds limit %d", len(raw), limits.MaxFrameBytes)
	}
	if err := scanJSONDepth(raw, limits.MaxJSONDepth); err != nil {
		return "", err
	}
	if err := rejectDuplicateFields(raw); err != nil {
		return "", err
	}
	var envelope struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return "", fmt.Errorf("decode event envelope: %w", err)
	}
	if envelope.Type == "" {
		return "", errors.New("event type is required")
	}
	return envelope.Type, nil
}

func scanJSONDepth(raw []byte, maxDepth int) error {
	depth := 0
	inString := false
	escaped := false
	for _, b := range raw {
		if inString {
			if escaped {
				escaped = false
				continue
			}
			if b == '\\' {
				escaped = true
			} else if b == '"' {
				inString = false
			}
			continue
		}
		switch b {
		case '"':
			inString = true
		case '{', '[':
			depth++
			if depth > maxDepth {
				return fmt.Errorf("JSON depth exceeds limit %d", maxDepth)
			}
		case '}', ']':
			depth--
		}
	}
	return nil
}

func rejectDuplicateFields(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := parseJSONValue(decoder); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	if token, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return fmt.Errorf("invalid JSON: unexpected trailing token %v", token)
		}
		return fmt.Errorf("invalid JSON: %w", err)
	}
	return nil
}

func parseJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("object key is not a string")
			}
			if _, duplicate := seen[key]; duplicate {
				return fmt.Errorf("duplicate field %q", key)
			}
			seen[key] = struct{}{}
			if err := parseJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil {
			return err
		}
		if end != json.Delim('}') {
			return errors.New("object is not terminated")
		}
	case '[':
		for decoder.More() {
			if err := parseJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil {
			return err
		}
		if end != json.Delim(']') {
			return errors.New("array is not terminated")
		}
	default:
		return fmt.Errorf("unexpected delimiter %q", delim)
	}
	return nil
}

func decodeBase64(encoded string, maxBytes int) ([]byte, error) {
	if maxBytes <= 0 {
		return nil, errors.New("audio limit must be positive")
	}
	if len(encoded) == 0 || len(encoded)%4 != 0 {
		return nil, errors.New("audio must be non-empty padded base64")
	}
	padding := 0
	if encoded[len(encoded)-1] == '=' {
		padding++
	}
	if len(encoded) > 1 && encoded[len(encoded)-2] == '=' {
		padding++
	}
	decodedLen := len(encoded)/4*3 - padding
	if decodedLen > maxBytes {
		return nil, fmt.Errorf("decoded audio size %d exceeds limit %d", decodedLen, maxBytes)
	}
	decoded := make([]byte, decodedLen)
	n, err := base64.StdEncoding.Strict().Decode(decoded, []byte(encoded))
	if err != nil {
		return nil, fmt.Errorf("invalid base64 audio: %w", err)
	}
	if n != decodedLen {
		return nil, errors.New("base64 decoded length mismatch")
	}
	return decoded, nil
}
