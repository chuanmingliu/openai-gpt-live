package protocol

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"unicode/utf8"

	"github.com/chuanmingliu/openai-gpt-live/internal/toolpolicy"
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
	case "live.tool.approval.resolve":
		target = &LiveToolApprovalResolveEvent{}
	case "live.tool_policy.update":
		target = &LiveToolPolicyUpdateEvent{}
	default:
		return nil, fmt.Errorf("unsupported client event type %q", eventType)
	}

	if err := json.Unmarshal(raw, target); err != nil {
		return nil, fmt.Errorf("decode client event: %w", err)
	}
	event := reflect.ValueOf(target).Elem().Interface().(ClientEvent)
	if err := validateClient(event, raw, limits); err != nil {
		return nil, err
	}
	if typed, ok := event.(LiveToolPolicyUpdateEvent); ok {
		fields, _ := rawObject(raw)
		typed.Tools, _ = decodeCanonicalToolPolicy(fields, toolpolicy.DefaultCatalog())
		event = typed
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
	case "session.reconnect.updated":
		target = &SessionReconnectUpdatedEvent{}
	case "session.updated":
		target = &SessionUpdatedEvent{}
	case "error":
		target = &ErrorEvent{}
	case "input_audio_buffer.committed":
		target = &InputAudioBufferCommittedEvent{}
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
	case "live.tool.approval.required":
		target = &LiveToolApprovalRequiredEvent{}
	case "live.tool_policy.updated":
		target = &LiveToolPolicyUpdatedEvent{}
	default:
		return nil, fmt.Errorf("unsupported server event type %q", eventType)
	}

	if err := json.Unmarshal(raw, target); err != nil {
		return nil, fmt.Errorf("decode server event: %w", err)
	}
	event := reflect.ValueOf(target).Elem().Interface().(ServerEvent)
	if err := validateServer(event, raw, limits); err != nil {
		return nil, err
	}
	if typed, ok := event.(LiveToolPolicyUpdatedEvent); ok {
		fields, _ := rawObject(raw)
		typed.Tools, _ = decodeCanonicalToolPolicy(fields, toolpolicy.DefaultCatalog())
		event = typed
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

func validateClient(event ClientEvent, raw []byte, limits Limits) error {
	fields, err := rawObject(raw)
	if err != nil {
		return err
	}
	switch typed := event.(type) {
	case SessionUpdateEvent:
		if _, err := requiredObject(fields, "session"); err != nil {
			return err
		}
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
		itemFields, err := requiredObject(fields, "item")
		if err != nil {
			return err
		}
		if typed.Item.Type == "" {
			return errors.New("conversation item type is required")
		}
		if err := validateConversationItem(typed.Item, itemFields); err != nil {
			return err
		}
	case InputAudioBufferCommitEvent, ResponseCreateEvent, ResponseCancelEvent:
		return nil
	case LiveToolApprovalResolveEvent:
		if err := exactFields(fields, "type", "invocation_id", "decision", "reason", "idempotency_key"); err != nil {
			return err
		}
		if err := requireBoundedString(fields, "invocation_id", 256, true); err != nil {
			return err
		}
		if typed.Decision != "approved" && typed.Decision != "denied" {
			return errors.New("approval decision is invalid")
		}
		if err := requireBoundedString(fields, "reason", 2048, false); err != nil {
			return err
		}
		return requireBoundedString(fields, "idempotency_key", 256, true)
	case LiveToolPolicyUpdateEvent:
		if err := exactFields(fields, "type", "event_id", "tools"); err != nil {
			return err
		}
		if err := requirePrintableASCIIString(fields, "event_id", 128); err != nil {
			return err
		}
		_, err := decodeCanonicalToolPolicy(fields, toolpolicy.DefaultCatalog())
		return err
	default:
		return fmt.Errorf("unsupported client event %T", event)
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

func validateConversationItem(item ConversationItem, fields map[string]json.RawMessage) error {
	switch item.Type {
	case "message":
		if item.Role == "" {
			return errors.New("message item requires role and content")
		}
		var content []json.RawMessage
		if err := json.Unmarshal(item.Content, &content); err != nil || len(content) == 0 {
			return errors.New("message item requires a non-empty content array")
		}
	case "function_call":
		if item.CallID == "" || item.Name == "" || item.Arguments == "" {
			return errors.New("function call item requires call_id, name, and arguments")
		}
	case "function_call_output":
		if item.CallID == "" {
			return errors.New("function call output requires call_id")
		}
		return requireString(fields, "output", false)
	default:
		return fmt.Errorf("unsupported conversation item type %q", item.Type)
	}
	return nil
}

func validateServer(event ServerEvent, raw []byte, limits Limits) error {
	fields, err := rawObject(raw)
	if err != nil {
		return err
	}
	if err := requireString(fields, "event_id", true); err != nil {
		return err
	}
	requireResponse := func() error {
		response, err := requiredObject(fields, "response")
		if err != nil {
			return err
		}
		if err := requireString(response, "id", true); err != nil {
			return err
		}
		return requireString(response, "status", true)
	}
	requireOutputCoordinates := func() error {
		if err := requireString(fields, "response_id", true); err != nil {
			return err
		}
		if err := requireString(fields, "item_id", true); err != nil {
			return err
		}
		if err := requireNonNegativeInt(fields, "output_index"); err != nil {
			return err
		}
		return requireNonNegativeInt(fields, "content_index")
	}
	requireHandoff := func() error {
		if err := requireNonNegativeInt(fields, "sequence_id"); err != nil {
			return err
		}
		if err := requireString(fields, "handoff_id", true); err != nil {
			return err
		}
		return requireString(fields, "status", true)
	}
	requireReconnect := func(metadata ReconnectMetadata) error {
		reconnect, reconnectErr := requiredObject(fields, "reconnect")
		if reconnectErr != nil {
			return reconnectErr
		}
		if err := exactFields(reconnect, "credential", "session_id", "conversation_id", "incarnation", "cursor", "model", "expires_at"); err != nil {
			return err
		}
		for _, field := range []string{"session_id", "conversation_id", "model"} {
			if err := requireBoundedString(reconnect, field, 256, true); err != nil {
				return err
			}
		}
		if err := requireBoundedString(reconnect, "credential", 2048, true); err != nil {
			return err
		}
		if err := requireNonNegativeInt(reconnect, "incarnation"); err != nil || metadata.Incarnation == 0 {
			return errors.New("reconnect incarnation is invalid")
		}
		if err := requireNonNegativeInt(reconnect, "cursor"); err != nil {
			return err
		}
		if metadata.ExpiresAt.IsZero() || metadata.ExpiresAt.Year() < 1970 || metadata.ExpiresAt.Year() > 9999 {
			return errors.New("reconnect expiry is invalid")
		}
		return nil
	}

	switch typed := event.(type) {
	case SessionCreatedEvent:
		_, err = requiredObject(fields, "session")
		if err != nil || typed.Reconnect == nil {
			return err
		}
		return requireReconnect(*typed.Reconnect)
	case SessionReconnectUpdatedEvent:
		if err := exactFields(fields, "type", "event_id", "reconnect"); err != nil {
			return err
		}
		return requireReconnect(typed.Reconnect)
	case SessionUpdatedEvent:
		_, err = requiredObject(fields, "session")
		return err
	case ErrorEvent:
		errorFields, err := requiredObject(fields, "error")
		if err != nil {
			return err
		}
		if err := requireString(errorFields, "type", true); err != nil {
			return err
		}
		return requireString(errorFields, "message", true)
	case InputAudioBufferCommittedEvent:
		if err := requireString(fields, "item_id", true); err != nil {
			return err
		}
		previous, ok := fields["previous_item_id"]
		if !ok {
			return errors.New("previous_item_id is required")
		}
		if bytes.Equal(bytes.TrimSpace(previous), []byte("null")) {
			return nil
		}
		return requireString(fields, "previous_item_id", true)
	case InputAudioBufferSpeechStartedEvent:
		if err := requireString(fields, "item_id", true); err != nil {
			return err
		}
		return requireNonNegativeInt(fields, "audio_start_ms")
	case InputAudioBufferSpeechStoppedEvent:
		if err := requireString(fields, "item_id", true); err != nil {
			return err
		}
		return requireNonNegativeInt(fields, "audio_end_ms")
	case ConversationItemInputAudioTranscriptionDeltaEvent:
		if err := requireString(fields, "item_id", true); err != nil {
			return err
		}
		if err := requireNonNegativeInt(fields, "content_index"); err != nil {
			return err
		}
		return requireString(fields, "delta", false)
	case ConversationItemInputAudioTranscriptionCompletedEvent:
		if err := requireString(fields, "item_id", true); err != nil {
			return err
		}
		if err := requireNonNegativeInt(fields, "content_index"); err != nil {
			return err
		}
		return requireString(fields, "transcript", false)
	case ResponseCreatedEvent, ResponseDoneEvent:
		return requireResponse()
	case ResponseOutputAudioDeltaEvent:
		if err := requireOutputCoordinates(); err != nil {
			return err
		}
		if _, err := decodeBase64(typed.Delta, limits.MaxAudioBytes); err != nil {
			return fmt.Errorf("decode output audio: %w", err)
		}
		return nil
	case ResponseOutputAudioDoneEvent:
		return requireOutputCoordinates()
	case ResponseOutputAudioTranscriptDeltaEvent:
		if err := requireOutputCoordinates(); err != nil {
			return err
		}
		return requireString(fields, "delta", false)
	case ResponseOutputAudioTranscriptDoneEvent:
		if err := requireOutputCoordinates(); err != nil {
			return err
		}
		return requireString(fields, "transcript", false)
	case ResponseFunctionCallArgumentsDoneEvent:
		if err := requireString(fields, "response_id", true); err != nil {
			return err
		}
		if err := requireString(fields, "item_id", true); err != nil {
			return err
		}
		if err := requireNonNegativeInt(fields, "output_index"); err != nil {
			return err
		}
		for _, name := range []string{"call_id", "name", "arguments"} {
			if err := requireString(fields, name, true); err != nil {
				return err
			}
		}
		return nil
	case LiveHandoffCreatedEvent, LiveHandoffProgressEvent, LiveHandoffNeedsInputEvent,
		LiveHandoffCompletedEvent, LiveHandoffFailedEvent, LiveHandoffCancelledEvent:
		return requireHandoff()
	case LiveSessionDegradedEvent, LiveSessionResumedEvent:
		if err := requireNonNegativeInt(fields, "sequence_id"); err != nil {
			return err
		}
		return requireString(fields, "status", true)
	case LiveToolApprovalRequiredEvent:
		if err := exactFields(fields, "type", "event_id", "sequence_id", "invocation_id", "handoff_id", "action_summary", "expires_at", "risk_class"); err != nil {
			return err
		}
		if err := requireNonNegativeInt(fields, "sequence_id"); err != nil || typed.SequenceID <= 0 {
			return errors.New("approval sequence is invalid")
		}
		for _, field := range []string{"event_id", "invocation_id", "handoff_id"} {
			if err := requireBoundedString(fields, field, 256, true); err != nil {
				return err
			}
		}
		if err := requireBoundedString(fields, "action_summary", 512, true); err != nil {
			return err
		}
		if typed.ExpiresAt.IsZero() || typed.ExpiresAt.Year() < 1970 || typed.ExpiresAt.Year() > 9999 {
			return errors.New("approval expiry is invalid")
		}
		if typed.RiskClass != "side_effecting" {
			return errors.New("approval risk class is invalid")
		}
		return nil
	case LiveToolPolicyUpdatedEvent:
		if err := exactFields(fields, "type", "event_id", "tools", "model"); err != nil {
			return err
		}
		if err := requirePrintableASCIIString(fields, "event_id", 128); err != nil {
			return err
		}
		if err := requireBoundedString(fields, "model", 256, true); err != nil {
			return err
		}
		_, err := decodeCanonicalToolPolicy(fields, toolpolicy.DefaultCatalog())
		return err
	default:
		return fmt.Errorf("unsupported server event %T", event)
	}
}

func rawObject(raw []byte) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, fmt.Errorf("decode object: %w", err)
	}
	if fields == nil {
		return nil, errors.New("object is required")
	}
	return fields, nil
}

func requiredObject(fields map[string]json.RawMessage, name string) (map[string]json.RawMessage, error) {
	raw, ok := fields[name]
	if !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, fmt.Errorf("%s object is required", name)
	}
	object, err := rawObject(raw)
	if err != nil {
		return nil, fmt.Errorf("%s must be an object: %w", name, err)
	}
	return object, nil
}

func requireString(fields map[string]json.RawMessage, name string, nonEmpty bool) error {
	raw, ok := fields[name]
	if !ok {
		return fmt.Errorf("%s is required", name)
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return fmt.Errorf("%s must be a string: %w", name, err)
	}
	if nonEmpty && value == "" {
		return fmt.Errorf("%s must not be empty", name)
	}
	return nil
}

func requireBoundedString(fields map[string]json.RawMessage, name string, limit int, nonEmpty bool) error {
	if err := requireString(fields, name, nonEmpty); err != nil {
		return err
	}
	var value string
	_ = json.Unmarshal(fields[name], &value)
	if len(value) > limit {
		return fmt.Errorf("%s exceeds its byte limit", name)
	}
	return nil
}

func requirePrintableASCIIString(fields map[string]json.RawMessage, name string, limit int) error {
	if err := requireBoundedString(fields, name, limit, true); err != nil {
		return err
	}
	var value string
	_ = json.Unmarshal(fields[name], &value)
	for _, character := range value {
		if character < 0x20 || character > 0x7e {
			return fmt.Errorf("%s must contain only printable ASCII", name)
		}
	}
	return nil
}

func decodeCanonicalToolPolicy(fields map[string]json.RawMessage, catalog toolpolicy.Catalog) (toolpolicy.Policy, error) {
	raw, ok := fields["tools"]
	if !ok {
		return toolpolicy.Policy{}, errors.New("tools is required")
	}
	policy, err := toolpolicy.Decode(raw)
	if err != nil {
		return toolpolicy.Policy{}, err
	}
	return toolpolicy.Canonicalize(policy, catalog)
}

func exactFields(fields map[string]json.RawMessage, names ...string) error {
	if len(fields) != len(names) {
		return errors.New("event fields are invalid")
	}
	for _, name := range names {
		if _, ok := fields[name]; !ok {
			return errors.New("event fields are invalid")
		}
	}
	return nil
}

func requireNonNegativeInt(fields map[string]json.RawMessage, name string) error {
	raw, ok := fields[name]
	if !ok {
		return fmt.Errorf("%s is required", name)
	}
	var value int64
	if err := json.Unmarshal(raw, &value); err != nil {
		return fmt.Errorf("%s must be an integer: %w", name, err)
	}
	if value < 0 {
		return fmt.Errorf("%s must not be negative", name)
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
	if !utf8.Valid(raw) {
		return "", errors.New("event is not valid UTF-8")
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
