package session

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
)

// ReservedHandoffSessionConfig returns an owned provider session update with
// exactly the server-controlled durable handoff tool definitions.
func ReservedHandoffSessionConfig() realtime.SessionConfig {
	schemas := handoff.ReservedToolSchemas()
	tools := make([]realtime.Tool, len(schemas))
	for index, schema := range schemas {
		tools[index] = realtime.Tool{
			Type: schema.Type, Name: schema.Name, Description: schema.Description,
			Parameters: append(json.RawMessage(nil), schema.Parameters...),
		}
	}
	return realtime.SessionConfig{Tools: tools, ToolChoice: json.RawMessage(`"auto"`)}
}

func appendReservedHandoffTools(client []realtime.Tool) []realtime.Tool {
	reserved := ReservedHandoffSessionConfig().Tools
	out := make([]realtime.Tool, 0, len(client)+len(reserved))
	out = append(out, client...)
	out = append(out, reserved...)
	return out
}

func cloneRealtimeSessionConfig(config realtime.SessionConfig) realtime.SessionConfig {
	config.ToolChoice = append(json.RawMessage(nil), config.ToolChoice...)
	config.TurnDetection = append(json.RawMessage(nil), config.TurnDetection...)
	config.Tools = append([]realtime.Tool(nil), config.Tools...)
	for index := range config.Tools {
		config.Tools[index].Parameters = append(json.RawMessage(nil), config.Tools[index].Parameters...)
	}
	return config
}

func isReservedToolName(name string) bool {
	switch name {
	case handoff.ToolStartBackgroundTask, handoff.ToolSteerBackgroundTask, handoff.ToolCancelBackgroundTask:
		return true
	default:
		return false
	}
}

func validateClientToolChoice(raw json.RawMessage, label string) error {
	if len(raw) == 0 {
		return nil
	}
	if !json.Valid(raw) {
		return fmt.Errorf("%s is invalid JSON", label)
	}
	var scalar string
	if json.Unmarshal(raw, &scalar) == nil {
		switch scalar {
		case "auto", "none", "required":
			return nil
		default:
			return fmt.Errorf("%s string is unsupported", label)
		}
	}
	var object struct {
		Type     string `json:"type"`
		Name     string `json:"name"`
		Function *struct {
			Name string `json:"name"`
		} `json:"function"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&object); err != nil {
		return fmt.Errorf("%s must be a supported string or function reference: %w", label, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		// The codec already limits one JSON value; retain an explicit exact-value
		// check here without accepting trailing data in direct package callers.
		return fmt.Errorf("%s must contain exactly one JSON value", label)
	}
	name := object.Name
	if object.Function != nil {
		if name != "" && object.Function.Name != "" && name != object.Function.Name {
			return fmt.Errorf("%s contains conflicting function names", label)
		}
		name = object.Function.Name
	}
	if object.Type != "function" || strings.TrimSpace(name) == "" {
		return fmt.Errorf("%s must name an ordinary function", label)
	}
	if isReservedToolName(name) {
		return fmt.Errorf("%s cannot force a reserved server tool", label)
	}
	return nil
}
