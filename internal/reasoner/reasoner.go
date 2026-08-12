package reasoner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	RoleSystem    = "system"
	RoleUser      = "user"
	RoleAssistant = "assistant"
	RoleTool      = "tool"

	maxModelBytes       = 256
	maxMessages         = 256
	maxTools            = 128
	maxContentBytes     = 1 << 20
	maxDescriptionBytes = 16 << 10
	maxSchemaBytes      = 256 << 10
	maxCallIDBytes      = 512
	maxFunctionBytes    = 128
	maxSafetyIDBytes    = 512
)

var functionNamePattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
var safetyIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

type Streamer interface {
	Stream(ctx context.Context, request Request, onChunk func(Chunk) error) (Result, error)
}

type Request struct {
	Model           string
	Messages        []Message
	Tools           []ToolDefinition
	MaxOutputTokens int
	DisableThinking bool
	SafetyUserID    string
}

type Message struct {
	Role       string
	Content    string
	ToolCalls  []ToolCall
	ToolCallID string
}

type ToolDefinition struct {
	Name        string
	Description string
	Parameters  json.RawMessage
}

type Chunk struct {
	Text      string
	ToolCalls []ToolCallDelta
}

type Result struct {
	Text              string
	ToolCalls         []ToolCall
	FinishReason      string
	ProviderRequestID string
	ProviderBlocked   bool
	Usage             Usage
}

type ToolCallDelta struct {
	Index     int
	ID        string
	Name      string
	Arguments string
}

type ToolCall struct {
	ID        string
	Name      string
	Arguments json.RawMessage
}

type Usage struct {
	PromptTokens          int
	CompletionTokens      int
	TotalTokens           int
	PromptCacheHitTokens  int
	PromptCacheMissTokens int
}

type ProviderError struct {
	Code       string
	Message    string
	HTTPStatus int
	Retryable  bool
	RetryAfter time.Duration
}

func (e *ProviderError) Error() string {
	if e == nil {
		return "reasoner provider error"
	}
	if e.HTTPStatus != 0 {
		return fmt.Sprintf("reasoner provider error (status %d)", e.HTTPStatus)
	}
	return "reasoner provider error"
}

func (request Request) Clone() Request {
	request.Messages = cloneMessages(request.Messages)
	request.Tools = cloneTools(request.Tools)
	return request
}

func (message Message) Clone() Message {
	message.ToolCalls = cloneToolCalls(message.ToolCalls)
	return message
}

func (definition ToolDefinition) Clone() ToolDefinition {
	definition.Parameters = cloneRaw(definition.Parameters)
	return definition
}

func (chunk Chunk) Clone() Chunk {
	chunk.ToolCalls = append([]ToolCallDelta(nil), chunk.ToolCalls...)
	return chunk
}

func (result Result) Clone() Result {
	result.ToolCalls = cloneToolCalls(result.ToolCalls)
	return result
}

func (call ToolCall) Clone() ToolCall {
	call.Arguments = cloneRaw(call.Arguments)
	return call
}

func (request Request) Validate() error {
	if err := boundedText("model", request.Model, maxModelBytes, true); err != nil {
		return err
	}
	if request.MaxOutputTokens <= 0 || request.MaxOutputTokens > 1_000_000 {
		return fmt.Errorf("max output tokens is out of bounds")
	}
	if len(request.Messages) == 0 || len(request.Messages) > maxMessages {
		return fmt.Errorf("message count is out of bounds")
	}
	if len(request.Tools) > maxTools {
		return fmt.Errorf("tool count is out of bounds")
	}
	if request.SafetyUserID != "" && (len(request.SafetyUserID) > maxSafetyIDBytes || !safetyIDPattern.MatchString(request.SafetyUserID)) {
		return fmt.Errorf("safety user ID is invalid")
	}
	declaredCalls := make(map[string]struct{})
	answeredCalls := make(map[string]struct{})
	for index := range request.Messages {
		message := request.Messages[index]
		if err := validateMessage(message); err != nil {
			return fmt.Errorf("message %d: %w", index, err)
		}
		if message.Role == RoleAssistant {
			for _, call := range message.ToolCalls {
				if _, exists := declaredCalls[call.ID]; exists {
					return fmt.Errorf("message %d: duplicate tool call ID", index)
				}
				declaredCalls[call.ID] = struct{}{}
			}
		}
		if message.Role == RoleTool {
			if _, exists := declaredCalls[message.ToolCallID]; !exists {
				return fmt.Errorf("message %d: tool call ID has no preceding assistant call", index)
			}
			if _, exists := answeredCalls[message.ToolCallID]; exists {
				return fmt.Errorf("message %d: duplicate tool result", index)
			}
			answeredCalls[message.ToolCallID] = struct{}{}
		}
	}
	seenTools := make(map[string]struct{}, len(request.Tools))
	for index := range request.Tools {
		tool := request.Tools[index]
		if err := ValidateFunctionName(tool.Name, false); err != nil {
			return fmt.Errorf("tool %d: %w", index, err)
		}
		if _, exists := seenTools[tool.Name]; exists {
			return fmt.Errorf("tool %d: duplicate function name", index)
		}
		seenTools[tool.Name] = struct{}{}
		if err := boundedText("description", tool.Description, maxDescriptionBytes, false); err != nil {
			return fmt.Errorf("tool %d: %w", index, err)
		}
		if err := validateObjectSchema(tool.Parameters); err != nil {
			return fmt.Errorf("tool %d: %w", index, err)
		}
	}
	return nil
}

func validateMessage(message Message) error {
	if !utf8.ValidString(message.Content) || len(message.Content) > maxContentBytes {
		return fmt.Errorf("content is invalid")
	}
	switch message.Role {
	case RoleSystem, RoleUser:
		if message.Content == "" || len(message.ToolCalls) != 0 || message.ToolCallID != "" {
			return fmt.Errorf("role fields are invalid")
		}
	case RoleAssistant:
		if message.Content == "" && len(message.ToolCalls) == 0 {
			return fmt.Errorf("assistant message is empty")
		}
		if message.ToolCallID != "" {
			return fmt.Errorf("assistant tool call ID is forbidden")
		}
		seen := make(map[string]struct{}, len(message.ToolCalls))
		for _, call := range message.ToolCalls {
			if err := validateToolCall(call); err != nil {
				return err
			}
			if _, exists := seen[call.ID]; exists {
				return fmt.Errorf("duplicate assistant tool call ID")
			}
			seen[call.ID] = struct{}{}
		}
	case RoleTool:
		if message.Content == "" || len(message.ToolCalls) != 0 {
			return fmt.Errorf("role fields are invalid")
		}
		if err := boundedText("tool call ID", message.ToolCallID, maxCallIDBytes, true); err != nil {
			return err
		}
	default:
		return fmt.Errorf("role is invalid")
	}
	return nil
}

func validateToolCall(call ToolCall) error {
	if err := boundedText("tool call ID", call.ID, maxCallIDBytes, true); err != nil {
		return err
	}
	if err := ValidateFunctionName(call.Name, false); err != nil {
		return err
	}
	if len(call.Arguments) == 0 || len(call.Arguments) > maxContentBytes || !utf8.Valid(call.Arguments) || !validJSONObject(call.Arguments) {
		return fmt.Errorf("tool call arguments are invalid")
	}
	return nil
}

func validJSONObject(raw []byte) bool {
	var object map[string]json.RawMessage
	return json.Unmarshal(raw, &object) == nil && object != nil
}

func ValidateFunctionName(name string, allowEmpty bool) error {
	if allowEmpty && name == "" {
		return nil
	}
	if err := boundedText("function name", name, maxFunctionBytes, true); err != nil {
		return err
	}
	if !functionNamePattern.MatchString(name) {
		return fmt.Errorf("function name is invalid")
	}
	return nil
}

func validateObjectSchema(raw json.RawMessage) error {
	if len(raw) == 0 || len(raw) > maxSchemaBytes || !utf8.Valid(raw) {
		return fmt.Errorf("parameters are invalid")
	}
	var schema map[string]json.RawMessage
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	if err := decoder.Decode(&schema); err != nil || schema == nil {
		return fmt.Errorf("parameters must be a JSON object")
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return fmt.Errorf("parameters contain trailing JSON")
	}
	typeRaw, exists := schema["type"]
	if !exists {
		return fmt.Errorf("parameters must declare object type")
	}
	var schemaType string
	if json.Unmarshal(typeRaw, &schemaType) != nil || schemaType != "object" {
		return fmt.Errorf("parameters must declare object type")
	}
	return nil
}

func boundedText(label, value string, limit int, required bool) error {
	if (required && value == "") || len(value) > limit || !utf8.ValidString(value) {
		return fmt.Errorf("%s is invalid", label)
	}
	return nil
}

func cloneMessages(messages []Message) []Message {
	if messages == nil {
		return nil
	}
	out := make([]Message, len(messages))
	for index := range messages {
		out[index] = messages[index].Clone()
	}
	return out
}

func cloneTools(tools []ToolDefinition) []ToolDefinition {
	if tools == nil {
		return nil
	}
	out := make([]ToolDefinition, len(tools))
	for index := range tools {
		out[index] = tools[index].Clone()
	}
	return out
}

func cloneToolCalls(calls []ToolCall) []ToolCall {
	if calls == nil {
		return nil
	}
	out := make([]ToolCall, len(calls))
	for index := range calls {
		out[index] = calls[index].Clone()
	}
	return out
}

func cloneRaw(raw json.RawMessage) json.RawMessage {
	return append(json.RawMessage(nil), raw...)
}
