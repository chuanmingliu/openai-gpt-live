package s2sws

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
)

type wireAudioFormat struct {
	Type string `json:"type"`
	Rate int    `json:"rate"`
}

type wireAudioInput struct {
	Format        wireAudioFormat `json:"format"`
	TurnDetection json.RawMessage `json:"turn_detection,omitempty"`
}

type wireAudioOutput struct {
	Format wireAudioFormat `json:"format"`
	Voice  string          `json:"voice,omitempty"`
}

type wireSessionAudio struct {
	Input  wireAudioInput  `json:"input"`
	Output wireAudioOutput `json:"output"`
}

type wireTool struct {
	Type        string          `json:"type"`
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

type wireSession struct {
	Type         string           `json:"type"`
	Instructions string           `json:"instructions,omitempty"`
	Audio        wireSessionAudio `json:"audio"`
	Tools        []wireTool       `json:"tools,omitempty"`
	ToolChoice   json.RawMessage  `json:"tool_choice,omitempty"`
}

func marshalCommand(command realtime.Command) ([]byte, error) {
	switch command.Kind {
	case realtime.CommandSessionUpdate:
		if err := validateAudioFormat(command.Session.InputFormat); err != nil {
			return nil, fmt.Errorf("input format: %w", err)
		}
		if err := validateAudioFormat(command.Session.OutputFormat); err != nil {
			return nil, fmt.Errorf("output format: %w", err)
		}
		tools := make([]wireTool, len(command.Session.Tools))
		for index, tool := range command.Session.Tools {
			if err := validateRawJSON(tool.Parameters, true); err != nil {
				return nil, fmt.Errorf("tool %d parameters: %w", index, err)
			}
			tools[index] = wireTool{Type: tool.Type, Name: tool.Name, Description: tool.Description, Parameters: tool.Parameters}
		}
		if err := validateRawJSON(command.Session.ToolChoice, true); err != nil {
			return nil, fmt.Errorf("tool choice: %w", err)
		}
		if err := validateRawJSON(command.Session.TurnDetection, true); err != nil {
			return nil, fmt.Errorf("turn detection: %w", err)
		}
		format := wireAudioFormat{Type: "audio/pcm", Rate: 24_000}
		return json.Marshal(struct {
			Type    string      `json:"type"`
			Session wireSession `json:"session"`
		}{
			Type: "session.update",
			Session: wireSession{
				Type:         "realtime",
				Instructions: command.Session.Instructions,
				Audio: wireSessionAudio{
					Input:  wireAudioInput{Format: format, TurnDetection: command.Session.TurnDetection},
					Output: wireAudioOutput{Format: format, Voice: command.Session.Voice},
				},
				Tools:      tools,
				ToolChoice: command.Session.ToolChoice,
			},
		})
	case realtime.CommandAppendAudio:
		if len(command.Audio) == 0 {
			return nil, errors.New("audio append is empty")
		}
		return json.Marshal(struct {
			Type  string `json:"type"`
			Audio string `json:"audio"`
		}{Type: "input_audio_buffer.append", Audio: base64.StdEncoding.EncodeToString(command.Audio)})
	case realtime.CommandCommitAudio:
		return []byte(`{"type":"input_audio_buffer.commit"}`), nil
	case realtime.CommandCreateItem:
		if err := validateJSONObject(command.Item); err != nil {
			return nil, fmt.Errorf("conversation item: %w", err)
		}
		return json.Marshal(struct {
			Type string          `json:"type"`
			Item json.RawMessage `json:"item"`
		}{Type: "conversation.item.create", Item: command.Item})
	case realtime.CommandCreateResponse:
		if command.Epoch == 0 {
			return nil, errors.New("response epoch must be positive")
		}
		if err := validateRawJSON(command.Response.ToolChoice, true); err != nil {
			return nil, fmt.Errorf("response tool choice: %w", err)
		}
		if err := validateRawJSON(command.Response.Input, true); err != nil {
			return nil, fmt.Errorf("response input: %w", err)
		}
		metadata := make(map[string]json.RawMessage, len(command.Response.Metadata)+1)
		for key, value := range command.Response.Metadata {
			if key == "live_epoch" {
				continue
			}
			var decoded string
			if err := json.Unmarshal(value, &decoded); err != nil {
				return nil, fmt.Errorf("response metadata %q must be a JSON string", key)
			}
			metadata[key] = append(json.RawMessage(nil), value...)
		}
		epoch, _ := json.Marshal(strconv.FormatUint(uint64(command.Epoch), 10))
		metadata["live_epoch"] = epoch
		return json.Marshal(struct {
			Type     string `json:"type"`
			EventID  string `json:"event_id"`
			Response struct {
				Instructions string                     `json:"instructions,omitempty"`
				ToolChoice   json.RawMessage            `json:"tool_choice,omitempty"`
				Metadata     map[string]json.RawMessage `json:"metadata"`
				Input        json.RawMessage            `json:"input,omitempty"`
			} `json:"response"`
		}{
			Type: "response.create", EventID: responseCreateEventID(command.Epoch),
			Response: struct {
				Instructions string                     `json:"instructions,omitempty"`
				ToolChoice   json.RawMessage            `json:"tool_choice,omitempty"`
				Metadata     map[string]json.RawMessage `json:"metadata"`
				Input        json.RawMessage            `json:"input,omitempty"`
			}{
				Instructions: command.Response.Instructions,
				ToolChoice:   command.Response.ToolChoice,
				Metadata:     metadata,
				Input:        command.Response.Input,
			},
		})
	case realtime.CommandCancelResponse:
		return json.Marshal(struct {
			Type       string `json:"type"`
			EventID    string `json:"event_id,omitempty"`
			ResponseID string `json:"response_id,omitempty"`
		}{Type: "response.cancel", EventID: responseCancelEventID(command.Epoch), ResponseID: command.ResponseID})
	case realtime.CommandBindResponseEpoch:
		return nil, errors.New("bind response epoch is internal")
	default:
		return nil, fmt.Errorf("unsupported realtime command kind %d", command.Kind)
	}
}

const responseCreateEventPrefix = "live-response-create-"

func responseCreateEventID(epoch identity.ResponseEpoch) string {
	return responseCreateEventPrefix + strconv.FormatUint(uint64(epoch), 10)
}

const responseCancelEventPrefix = "live-response-cancel-"

func responseCancelEventID(epoch identity.ResponseEpoch) string {
	if epoch == 0 {
		return ""
	}
	return responseCancelEventPrefix + strconv.FormatUint(uint64(epoch), 10)
}

func responseCreateEpoch(eventID string) identity.ResponseEpoch {
	if !strings.HasPrefix(eventID, responseCreateEventPrefix) {
		return 0
	}
	raw := strings.TrimPrefix(eventID, responseCreateEventPrefix)
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || value == 0 || strconv.FormatUint(value, 10) != raw {
		return 0
	}
	return identity.ResponseEpoch(value)
}

func responseCommandEpoch(eventID string) identity.ResponseEpoch {
	if epoch := responseCreateEpoch(eventID); epoch != 0 {
		return epoch
	}
	if !strings.HasPrefix(eventID, responseCancelEventPrefix) {
		return 0
	}
	raw := strings.TrimPrefix(eventID, responseCancelEventPrefix)
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || value == 0 || strconv.FormatUint(value, 10) != raw {
		return 0
	}
	return identity.ResponseEpoch(value)
}

func validateAudioFormat(format realtime.AudioFormat) error {
	if format == (realtime.AudioFormat{}) {
		return nil
	}
	if format.Encoding != "pcm16le" || format.SampleRate != 24_000 || format.Channels != 1 {
		return errors.New("must be mono pcm16le at 24 kHz")
	}
	return nil
}

func validateRawJSON(raw json.RawMessage, optional bool) error {
	if len(raw) == 0 {
		if optional {
			return nil
		}
		return errors.New("JSON value is required")
	}
	if !json.Valid(raw) {
		return errors.New("invalid JSON")
	}
	return nil
}

func validateJSONObject(raw json.RawMessage) error {
	if err := validateRawJSON(raw, false); err != nil {
		return err
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return errors.New("JSON object is required")
	}
	return nil
}

func cloneCommand(command realtime.Command) realtime.Command {
	command.Audio = append([]byte(nil), command.Audio...)
	command.Item = append(json.RawMessage(nil), command.Item...)
	command.Session.ToolChoice = append(json.RawMessage(nil), command.Session.ToolChoice...)
	command.Session.TurnDetection = append(json.RawMessage(nil), command.Session.TurnDetection...)
	command.Session.Tools = append([]realtime.Tool(nil), command.Session.Tools...)
	for index := range command.Session.Tools {
		command.Session.Tools[index].Parameters = append(json.RawMessage(nil), command.Session.Tools[index].Parameters...)
	}
	command.Response.ToolChoice = append(json.RawMessage(nil), command.Response.ToolChoice...)
	command.Response.Input = append(json.RawMessage(nil), command.Response.Input...)
	if command.Response.Metadata != nil {
		metadata := make(map[string]json.RawMessage, len(command.Response.Metadata))
		for key, value := range command.Response.Metadata {
			metadata[key] = append(json.RawMessage(nil), value...)
		}
		command.Response.Metadata = metadata
	}
	return command
}

func cloneEvent(event realtime.Event) realtime.Event {
	event.Audio = append([]byte(nil), event.Audio...)
	event.Arguments = append(json.RawMessage(nil), event.Arguments...)
	event.Payload = append(json.RawMessage(nil), event.Payload...)
	return event
}

type responseCreatedDetails struct {
	responseID string
	hasEpoch   bool
	epoch      identity.ResponseEpoch
}

type mappedServerEvent struct {
	event            realtime.Event
	created          *responseCreatedDetails
	responseSpecific bool
	ignore           bool
	frameBytes       int
}

type eventEnvelope struct {
	Type    string `json:"type"`
	EventID string `json:"event_id"`
}

func mapServerEvent(raw []byte, maxFrameBytes int64) (mappedServerEvent, error) {
	if int64(len(raw)) > maxFrameBytes {
		return mappedServerEvent{}, fmt.Errorf("provider frame is %d bytes, limit is %d", len(raw), maxFrameBytes)
	}
	var envelope eventEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return mappedServerEvent{}, fmt.Errorf("decode provider event: %w", err)
	}
	if envelope.Type == "" {
		return mappedServerEvent{}, errors.New("provider event type is required")
	}
	mapped := mappedServerEvent{
		event:      realtime.Event{ProviderEventID: envelope.EventID, Payload: append(json.RawMessage(nil), raw...)},
		frameBytes: len(raw),
	}

	switch envelope.Type {
	case "session.created":
		mapped.event.Kind = realtime.EventSessionCreated
	case "session.updated":
		mapped.event.Kind = realtime.EventSessionUpdated
	case "input_audio_buffer.committed":
		var event struct {
			ItemID         string  `json:"item_id"`
			PreviousItemID *string `json:"previous_item_id"`
		}
		if err := json.Unmarshal(raw, &event); err != nil {
			return mappedServerEvent{}, err
		}
		mapped.event.Kind = realtime.EventInputAudioCommitted
		mapped.event.ItemID = event.ItemID
		if event.PreviousItemID != nil {
			previous := strings.Clone(*event.PreviousItemID)
			mapped.event.PreviousItemID = &previous
		}
	case "conversation.item.created":
		mapped.ignore = true
	case "input_audio_buffer.speech_started":
		var event struct {
			ItemID string `json:"item_id"`
		}
		if err := json.Unmarshal(raw, &event); err != nil {
			return mappedServerEvent{}, err
		}
		mapped.event.Kind, mapped.event.ItemID = realtime.EventSpeechStarted, event.ItemID
	case "input_audio_buffer.speech_stopped":
		var event struct {
			ItemID string `json:"item_id"`
		}
		if err := json.Unmarshal(raw, &event); err != nil {
			return mappedServerEvent{}, err
		}
		mapped.event.Kind, mapped.event.ItemID = realtime.EventSpeechStopped, event.ItemID
	case "conversation.item.input_audio_transcription.delta":
		var event struct {
			ItemID string `json:"item_id"`
			Delta  string `json:"delta"`
		}
		if err := json.Unmarshal(raw, &event); err != nil {
			return mappedServerEvent{}, err
		}
		mapped.event.Kind, mapped.event.ItemID, mapped.event.Text = realtime.EventTranscriptionDelta, event.ItemID, event.Delta
	case "conversation.item.input_audio_transcription.completed":
		var event struct {
			ItemID     string `json:"item_id"`
			Transcript string `json:"transcript"`
		}
		if err := json.Unmarshal(raw, &event); err != nil {
			return mappedServerEvent{}, err
		}
		mapped.event.Kind, mapped.event.ItemID, mapped.event.Text = realtime.EventTranscriptionCompleted, event.ItemID, event.Transcript
	case "response.created":
		var event struct {
			Response struct {
				ID       string                     `json:"id"`
				Status   string                     `json:"status"`
				Metadata map[string]json.RawMessage `json:"metadata"`
			} `json:"response"`
		}
		if err := json.Unmarshal(raw, &event); err != nil {
			return mappedServerEvent{}, fmt.Errorf("decode response.created: %w", err)
		}
		created := &responseCreatedDetails{responseID: event.Response.ID}
		if encoded, exists := event.Response.Metadata["live_epoch"]; exists {
			var value string
			if err := json.Unmarshal(encoded, &value); err != nil {
				return mappedServerEvent{}, errors.New("response.created live_epoch must be a decimal JSON string")
			}
			parsed, err := strconv.ParseUint(value, 10, 64)
			if err != nil || parsed == 0 || strconv.FormatUint(parsed, 10) != value {
				return mappedServerEvent{}, fmt.Errorf("invalid live_epoch %q", value)
			}
			created.hasEpoch, created.epoch = true, identity.ResponseEpoch(parsed)
		}
		mapped.event.Kind = realtime.EventResponseCreated
		mapped.event.ResponseID, mapped.event.Status, mapped.created = event.Response.ID, event.Response.Status, created
	case "response.output_audio.delta":
		var event struct {
			ResponseID string `json:"response_id"`
			ItemID     string `json:"item_id"`
			Delta      string `json:"delta"`
		}
		if err := json.Unmarshal(raw, &event); err != nil {
			return mappedServerEvent{}, fmt.Errorf("decode audio delta: %w", err)
		}
		if event.Delta == "" {
			return mappedServerEvent{}, errors.New("provider audio delta is empty")
		}
		if int64(base64.StdEncoding.DecodedLen(len(event.Delta))) > maxFrameBytes {
			return mappedServerEvent{}, errors.New("provider audio delta exceeds PCM limit")
		}
		audio, err := base64.StdEncoding.Strict().DecodeString(event.Delta)
		if err != nil || base64.StdEncoding.EncodeToString(audio) != event.Delta {
			return mappedServerEvent{}, errors.New("provider audio delta is malformed base64")
		}
		mapped.event.Kind = realtime.EventAudioDelta
		mapped.event.ResponseID, mapped.event.ItemID, mapped.event.Audio = event.ResponseID, event.ItemID, audio
		mapped.responseSpecific = true
	case "response.output_audio.done":
		var event struct {
			ResponseID string `json:"response_id"`
			ItemID     string `json:"item_id"`
		}
		if err := json.Unmarshal(raw, &event); err != nil {
			return mappedServerEvent{}, err
		}
		mapped.event.Kind, mapped.event.ResponseID, mapped.event.ItemID = realtime.EventAudioDone, event.ResponseID, event.ItemID
		mapped.responseSpecific = true
	case "response.output_audio_transcript.delta":
		var event struct {
			ResponseID string `json:"response_id"`
			ItemID     string `json:"item_id"`
			Delta      string `json:"delta"`
		}
		if err := json.Unmarshal(raw, &event); err != nil {
			return mappedServerEvent{}, err
		}
		mapped.event.Kind, mapped.event.ResponseID, mapped.event.ItemID, mapped.event.Text = realtime.EventTranscriptDelta, event.ResponseID, event.ItemID, event.Delta
		mapped.responseSpecific = true
	case "response.output_audio_transcript.done":
		var event struct {
			ResponseID string `json:"response_id"`
			ItemID     string `json:"item_id"`
			Transcript string `json:"transcript"`
		}
		if err := json.Unmarshal(raw, &event); err != nil {
			return mappedServerEvent{}, err
		}
		mapped.event.Kind, mapped.event.ResponseID, mapped.event.ItemID, mapped.event.Text = realtime.EventTranscriptDone, event.ResponseID, event.ItemID, event.Transcript
		mapped.responseSpecific = true
	case "response.function_call_arguments.done":
		var event struct {
			ResponseID string `json:"response_id"`
			ItemID     string `json:"item_id"`
			CallID     string `json:"call_id"`
			Name       string `json:"name"`
			Arguments  string `json:"arguments"`
		}
		if err := json.Unmarshal(raw, &event); err != nil {
			return mappedServerEvent{}, fmt.Errorf("decode function arguments: %w", err)
		}
		arguments := []byte(event.Arguments)
		if len(arguments) == 0 || !json.Valid(arguments) {
			return mappedServerEvent{}, errors.New("provider function arguments contain invalid JSON")
		}
		mapped.event.Kind = realtime.EventFunctionCallDone
		mapped.event.ResponseID, mapped.event.ItemID, mapped.event.CallID, mapped.event.Name = event.ResponseID, event.ItemID, event.CallID, event.Name
		mapped.event.Arguments = append(json.RawMessage(nil), arguments...)
		mapped.responseSpecific = true
	case "response.done":
		var event struct {
			Response struct {
				ID            string `json:"id"`
				Status        string `json:"status"`
				StatusDetails struct {
					Reason string `json:"reason"`
				} `json:"status_details"`
			} `json:"response"`
		}
		if err := json.Unmarshal(raw, &event); err != nil {
			return mappedServerEvent{}, fmt.Errorf("decode response.done: %w", err)
		}
		mapped.event.Kind = realtime.EventResponseDone
		mapped.event.ResponseID, mapped.event.Status, mapped.event.Reason = event.Response.ID, event.Response.Status, event.Response.StatusDetails.Reason
		mapped.responseSpecific = true
	case "error":
		var event struct {
			Error struct {
				Message string `json:"message"`
				EventID string `json:"event_id"`
			} `json:"error"`
		}
		if err := json.Unmarshal(raw, &event); err != nil {
			return mappedServerEvent{}, fmt.Errorf("decode provider error: %w", err)
		}
		mapped.event.Kind, mapped.event.Reason = realtime.EventError, event.Error.Message
		mapped.event.Epoch = responseCommandEpoch(event.Error.EventID)
	default:
		return mappedServerEvent{}, fmt.Errorf("unsupported provider event type %q", envelope.Type)
	}
	return mapped, nil
}
