package deepseek

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/chuanmingliu/openai-gpt-live/internal/reasoner"
)

type Client struct {
	config   Config
	endpoint *url.URL
	http     *http.Client
}

func New(config Config, httpClient *http.Client) (*Client, error) {
	if httpClient == nil {
		return nil, fmt.Errorf("deepseek: HTTP client is required")
	}
	validated, endpoint, err := config.validated()
	if err != nil {
		return nil, err
	}
	return &Client{config: validated, endpoint: endpoint, http: httpClient}, nil
}

type providerRequest struct {
	Model         string             `json:"model"`
	Messages      []providerMessage  `json:"messages"`
	Tools         []providerTool     `json:"tools,omitempty"`
	MaxTokens     int                `json:"max_tokens"`
	Stream        bool               `json:"stream"`
	StreamOptions providerStreamOpts `json:"stream_options"`
	UserID        string             `json:"user_id,omitempty"`
}

type providerStreamOpts struct {
	IncludeUsage bool `json:"include_usage"`
}

type providerMessage struct {
	Role       string             `json:"role"`
	Content    string             `json:"content,omitempty"`
	ToolCalls  []providerToolCall `json:"tool_calls,omitempty"`
	ToolCallID string             `json:"tool_call_id,omitempty"`
}

type providerToolCall struct {
	ID       string           `json:"id"`
	Type     string           `json:"type"`
	Function providerFunction `json:"function"`
}

type providerFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type providerTool struct {
	Type     string                 `json:"type"`
	Function providerToolDefinition `json:"function"`
}

type providerToolDefinition struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters"`
}

func (client *Client) Stream(ctx context.Context, request reasoner.Request, onChunk func(reasoner.Chunk) error) (reasoner.Result, error) {
	if client == nil {
		return reasoner.Result{}, fmt.Errorf("deepseek: nil client")
	}
	if ctx == nil {
		return reasoner.Result{}, fmt.Errorf("deepseek: context is required")
	}
	if err := ctx.Err(); err != nil {
		return reasoner.Result{}, err
	}
	if onChunk == nil {
		return reasoner.Result{}, fmt.Errorf("deepseek: callback is required")
	}
	owned := request.Clone()
	if err := owned.Validate(); err != nil {
		return reasoner.Result{}, fmt.Errorf("deepseek: invalid request: %w", err)
	}
	body, err := json.Marshal(mapRequest(owned))
	if err != nil {
		return reasoner.Result{}, fmt.Errorf("deepseek: encode request: %w", err)
	}
	if len(body) > client.config.MaxRequestBytes {
		return reasoner.Result{}, fmt.Errorf("deepseek: serialized request exceeds limit")
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, client.endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return reasoner.Result{}, fmt.Errorf("deepseek: create request: %w", err)
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Accept", "text/event-stream")
	if client.config.APIKey != "" {
		httpRequest.Header.Set("Authorization", "Bearer "+client.config.APIKey)
	}
	response, err := client.http.Do(httpRequest)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return reasoner.Result{}, contextErr
		}
		return reasoner.Result{}, providerError("transport_error", "", 0, true, 0)
	}
	defer response.Body.Close()
	if err := ctx.Err(); err != nil {
		return reasoner.Result{}, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		retryable := retryableHTTPStatus(response.StatusCode)
		retryAfter := parseRetryAfter(response.Header.Get("Retry-After"), response.Header.Get("Date"), time.Now)
		limited := io.LimitReader(response.Body, maxErrorBodyBytes+1)
		errorBody, readErr := io.ReadAll(limited)
		if readErr != nil {
			if contextErr := ctx.Err(); contextErr != nil {
				return reasoner.Result{}, contextErr
			}
			return reasoner.Result{}, providerError("error_body_read", "", response.StatusCode, retryable, retryAfter)
		}
		over := len(errorBody) > maxErrorBodyBytes
		if over {
			errorBody = errorBody[:maxErrorBodyBytes]
		}
		return reasoner.Result{}, responseError(response, errorBody, over, retryable, retryAfter)
	}
	return client.readStream(ctx, response.Body, onChunk)
}

func mapRequest(request reasoner.Request) providerRequest {
	out := providerRequest{Model: request.Model, MaxTokens: request.MaxOutputTokens, Stream: true, StreamOptions: providerStreamOpts{IncludeUsage: true}, UserID: request.SafetyUserID}
	out.Messages = make([]providerMessage, len(request.Messages))
	for index, message := range request.Messages {
		mapped := providerMessage{Role: message.Role, Content: message.Content, ToolCallID: message.ToolCallID}
		if len(message.ToolCalls) > 0 {
			mapped.ToolCalls = make([]providerToolCall, len(message.ToolCalls))
			for callIndex, call := range message.ToolCalls {
				mapped.ToolCalls[callIndex] = providerToolCall{ID: call.ID, Type: "function", Function: providerFunction{Name: call.Name, Arguments: string(call.Arguments)}}
			}
		}
		out.Messages[index] = mapped
	}
	if len(request.Tools) > 0 {
		out.Tools = make([]providerTool, len(request.Tools))
		for index, tool := range request.Tools {
			out.Tools[index] = providerTool{Type: "function", Function: providerToolDefinition{Name: tool.Name, Description: tool.Description, Parameters: append(json.RawMessage(nil), tool.Parameters...)}}
		}
	}
	return out
}

type streamChunk struct {
	ID      string         `json:"id"`
	Choices []streamChoice `json:"choices"`
	Usage   *streamUsage   `json:"usage"`
	Error   *streamError   `json:"error"`
}

type streamChoice struct {
	Index        *int         `json:"index"`
	Delta        *streamDelta `json:"delta"`
	FinishReason *string      `json:"finish_reason"`
}

type streamDelta struct {
	Content   *string           `json:"content"`
	ToolCalls []streamToolDelta `json:"tool_calls"`
}

type streamToolDelta struct {
	Index    *int           `json:"index"`
	ID       string         `json:"id"`
	Type     string         `json:"type"`
	Function streamFunction `json:"function"`
}

type streamFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type streamUsage struct {
	PromptTokens          *int `json:"prompt_tokens"`
	CompletionTokens      *int `json:"completion_tokens"`
	TotalTokens           *int `json:"total_tokens"`
	PromptCacheHitTokens  int  `json:"prompt_cache_hit_tokens"`
	PromptCacheMissTokens int  `json:"prompt_cache_miss_tokens"`
}

type streamError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type toolAccumulator struct {
	id        strings.Builder
	name      strings.Builder
	arguments strings.Builder
	sealed    bool
}

type toolDeltaPlan struct {
	index          int
	id             string
	name           string
	arguments      string
	appendIdentity bool
}

func (client *Client) readStream(ctx context.Context, body io.Reader, onChunk func(reasoner.Chunk) error) (reasoner.Result, error) {
	reader := newSSEReader(bufio.NewReaderSize(body, 32<<10), maxSSEEventBytes)
	result := reasoner.Result{}
	var answer strings.Builder
	tools := make(map[int]*toolAccumulator)
	toolArgumentBytes := 0
	var terminalToolCalls []reasoner.ToolCall
	terminal := false
	usageSeen := false
	for {
		if err := ctx.Err(); err != nil {
			return reasoner.Result{}, err
		}
		event, err := reader.Next()
		if err != nil {
			if contextErr := ctx.Err(); contextErr != nil {
				return reasoner.Result{}, contextErr
			}
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return reasoner.Result{}, providerError("truncated_stream", "", 0, true, 0)
			}
			return reasoner.Result{}, providerError("invalid_stream", "", 0, false, 0)
		}
		if bytes.Equal(event, []byte("[DONE]")) {
			if err := ctx.Err(); err != nil {
				return reasoner.Result{}, err
			}
			if !terminal {
				return reasoner.Result{}, providerError("missing_finish", "", 0, true, 0)
			}
			break
		}
		var chunk streamChunk
		if len(event) == 0 || !utf8.Valid(event) || json.Unmarshal(event, &chunk) != nil {
			return reasoner.Result{}, providerError("malformed_chunk", "", 0, false, 0)
		}
		if chunk.Error != nil {
			return reasoner.Result{}, providerError(chunk.Error.Code, chunk.Error.Message, 0, chunk.Error.Code == "insufficient_system_resource", 0)
		}
		if chunk.ID == "" {
			return reasoner.Result{}, providerError("invalid_chunk", "", 0, false, 0)
		}
		if result.ProviderRequestID == "" {
			result.ProviderRequestID = chunk.ID
		} else if result.ProviderRequestID != chunk.ID {
			return reasoner.Result{}, providerError("conflicting_request_id", "", 0, false, 0)
		}
		if len(chunk.Choices) == 0 {
			if chunk.Usage == nil || !terminal || usageSeen {
				return reasoner.Result{}, providerError("invalid_usage_chunk", "", 0, false, 0)
			}
			if !validUsage(*chunk.Usage) {
				return reasoner.Result{}, providerError("invalid_usage", "", 0, false, 0)
			}
			usageSeen = true
			result.Usage = mapUsage(*chunk.Usage)
			continue
		}
		if terminal || len(chunk.Choices) != 1 || chunk.Choices[0].Index == nil || *chunk.Choices[0].Index != 0 || chunk.Choices[0].Delta == nil || chunk.Usage != nil {
			return reasoner.Result{}, providerError("invalid_choice", "", 0, false, 0)
		}
		choice := chunk.Choices[0]
		if choice.FinishReason != nil && !validFinishReason(*choice.FinishReason) {
			return reasoner.Result{}, providerError("invalid_finish", "", 0, false, 0)
		}
		if choice.FinishReason != nil && *choice.FinishReason == "insufficient_system_resource" {
			terminal = true
			result.FinishReason = *choice.FinishReason
			continue
		}
		callbackChunk := reasoner.Chunk{}
		if choice.Delta.Content != nil {
			callbackChunk.Text = *choice.Delta.Content
			if exceedsLimit(answer.Len()+toolArgumentBytes, len(callbackChunk.Text), client.config.MaxOutputBytes) {
				return reasoner.Result{}, providerError("output_limit", "", 0, false, 0)
			}
		}
		plans, deltas, addedArgumentBytes, err := client.planToolDeltas(
			choice.Delta.ToolCalls,
			tools,
			answer.Len()+toolArgumentBytes+len(callbackChunk.Text),
		)
		if err != nil {
			return reasoner.Result{}, err
		}
		callbackChunk.ToolCalls = deltas
		if choice.FinishReason != nil {
			assembled, err := assembleProjectedTools(tools, plans, client.config.MaxToolCalls)
			if err != nil {
				return reasoner.Result{}, err
			}
			hasToolCalls := len(assembled) != 0
			if (*choice.FinishReason == "tool_calls") != hasToolCalls {
				return reasoner.Result{}, providerError("inconsistent_finish", "", 0, false, 0)
			}
			terminalToolCalls = assembled
		}
		answer.WriteString(callbackChunk.Text)
		applyToolPlans(tools, plans)
		toolArgumentBytes += addedArgumentBytes
		if choice.FinishReason != nil {
			terminal = true
			result.FinishReason = *choice.FinishReason
		}
		if callbackChunk.Text != "" || len(callbackChunk.ToolCalls) != 0 {
			if err := ctx.Err(); err != nil {
				return reasoner.Result{}, err
			}
			if err := onChunk(callbackChunk.Clone()); err != nil {
				return reasoner.Result{}, err
			}
			if err := ctx.Err(); err != nil {
				return reasoner.Result{}, err
			}
		}
	}
	if result.FinishReason == "insufficient_system_resource" {
		return reasoner.Result{}, providerError("insufficient_system_resource", "", 0, true, 0)
	}
	if err := ctx.Err(); err != nil {
		return reasoner.Result{}, err
	}
	result.Text = answer.String()
	result.ToolCalls = terminalToolCalls
	return result, nil
}

func (client *Client) planToolDeltas(
	deltas []streamToolDelta,
	tools map[int]*toolAccumulator,
	projectedOutputBytes int,
) ([]toolDeltaPlan, []reasoner.ToolCallDelta, int, error) {
	plans := make([]toolDeltaPlan, 0, len(deltas))
	callbackDeltas := make([]reasoner.ToolCallDelta, 0, len(deltas))
	seenIndexes := make(map[int]struct{}, len(deltas))
	newIndexes := make(map[int]struct{}, len(deltas))
	addedArgumentBytes := 0
	for _, delta := range deltas {
		if delta.Index == nil || *delta.Index < 0 || *delta.Index >= client.config.MaxToolCalls {
			return nil, nil, 0, providerError("tool_index", "", 0, false, 0)
		}
		index := *delta.Index
		if _, exists := seenIndexes[index]; exists {
			return nil, nil, 0, providerError("conflicting_tool_delta", "", 0, false, 0)
		}
		seenIndexes[index] = struct{}{}
		if delta.Type != "" && delta.Type != "function" {
			return nil, nil, 0, providerError("tool_type", "", 0, false, 0)
		}
		if err := reasoner.ValidateFunctionName(delta.Function.Name, true); err != nil {
			return nil, nil, 0, providerError("invalid_tool_name", "", 0, false, 0)
		}
		accumulator := tools[index]
		appendIdentity := accumulator == nil || !accumulator.sealed
		if accumulator == nil {
			accumulator = &toolAccumulator{}
			newIndexes[index] = struct{}{}
		}
		if accumulator.sealed {
			if (delta.ID != "" && delta.ID != accumulator.id.String()) || (delta.Function.Name != "" && delta.Function.Name != accumulator.name.String()) {
				return nil, nil, 0, providerError("conflicting_tool_identity", "", 0, false, 0)
			}
		} else if exceedsLimit(accumulator.id.Len(), len(delta.ID), 512) || exceedsLimit(accumulator.name.Len(), len(delta.Function.Name), 128) {
			return nil, nil, 0, providerError("tool_identity_limit", "", 0, false, 0)
		}
		if exceedsLimit(accumulator.arguments.Len(), len(delta.Function.Arguments), client.config.MaxToolArgumentBytes) ||
			exceedsLimit(projectedOutputBytes+addedArgumentBytes, len(delta.Function.Arguments), client.config.MaxOutputBytes) {
			return nil, nil, 0, providerError("tool_output_limit", "", 0, false, 0)
		}
		addedArgumentBytes += len(delta.Function.Arguments)
		plans = append(plans, toolDeltaPlan{
			index:          index,
			id:             delta.ID,
			name:           delta.Function.Name,
			arguments:      delta.Function.Arguments,
			appendIdentity: appendIdentity,
		})
		callbackDeltas = append(callbackDeltas, reasoner.ToolCallDelta{
			Index: index, ID: delta.ID, Name: delta.Function.Name, Arguments: delta.Function.Arguments,
		})
	}
	projectedCount := len(tools) + len(newIndexes)
	if projectedCount > client.config.MaxToolCalls {
		return nil, nil, 0, providerError("tool_count", "", 0, false, 0)
	}
	for index := range projectedCount {
		if tools[index] == nil {
			if _, exists := newIndexes[index]; !exists {
				return nil, nil, 0, providerError("sparse_tool_indexes", "", 0, false, 0)
			}
		}
	}
	return plans, callbackDeltas, addedArgumentBytes, nil
}

func applyToolPlans(tools map[int]*toolAccumulator, plans []toolDeltaPlan) {
	for _, plan := range plans {
		accumulator := tools[plan.index]
		if accumulator == nil {
			accumulator = &toolAccumulator{}
			tools[plan.index] = accumulator
		}
		if plan.appendIdentity {
			accumulator.id.WriteString(plan.id)
			accumulator.name.WriteString(plan.name)
		}
		accumulator.arguments.WriteString(plan.arguments)
		if plan.arguments != "" {
			accumulator.sealed = true
		}
	}
}

func assembleProjectedTools(tools map[int]*toolAccumulator, plans []toolDeltaPlan, maximum int) ([]reasoner.ToolCall, error) {
	planByIndex := make(map[int]toolDeltaPlan, len(plans))
	newCount := 0
	for _, plan := range plans {
		planByIndex[plan.index] = plan
		if tools[plan.index] == nil {
			newCount++
		}
	}
	count := len(tools) + newCount
	if count == 0 {
		return nil, nil
	}
	if count > maximum {
		return nil, providerError("tool_count", "", 0, false, 0)
	}
	result := make([]reasoner.ToolCall, 0, count)
	seenIDs := make(map[string]struct{}, count)
	for index := range count {
		accumulator := tools[index]
		plan, hasPlan := planByIndex[index]
		if accumulator == nil && !hasPlan {
			return nil, providerError("sparse_tool_indexes", "", 0, false, 0)
		}
		var id, name, arguments string
		if accumulator != nil {
			id = accumulator.id.String()
			name = accumulator.name.String()
			arguments = accumulator.arguments.String()
		}
		if hasPlan {
			if plan.appendIdentity {
				id += plan.id
				name += plan.name
			}
			arguments += plan.arguments
		}
		if id == "" || len(id) > 512 || !utf8.ValidString(id) || reasoner.ValidateFunctionName(name, false) != nil || !validToolArguments(arguments) {
			return nil, providerError("invalid_tool_call", "", 0, false, 0)
		}
		if _, exists := seenIDs[id]; exists {
			return nil, providerError("duplicate_tool_id", "", 0, false, 0)
		}
		seenIDs[id] = struct{}{}
		result = append(result, reasoner.ToolCall{ID: id, Name: name, Arguments: append(json.RawMessage(nil), arguments...)})
	}
	return result, nil
}

func exceedsLimit(current, addition, limit int) bool {
	return current < 0 || addition < 0 || current > limit || addition > limit-current
}

func validToolArguments(arguments string) bool {
	var object map[string]json.RawMessage
	return json.Unmarshal([]byte(arguments), &object) == nil && object != nil
}

func validUsage(usage streamUsage) bool {
	const maxUsageTokens = 1_000_000_000
	return usage.PromptTokens != nil && usage.CompletionTokens != nil && usage.TotalTokens != nil &&
		*usage.PromptTokens >= 0 && *usage.PromptTokens <= maxUsageTokens &&
		*usage.CompletionTokens >= 0 && *usage.CompletionTokens <= maxUsageTokens &&
		*usage.TotalTokens >= 0 && *usage.TotalTokens <= maxUsageTokens &&
		*usage.TotalTokens == *usage.PromptTokens+*usage.CompletionTokens &&
		usage.PromptCacheHitTokens >= 0 && usage.PromptCacheHitTokens <= maxUsageTokens &&
		usage.PromptCacheMissTokens >= 0 && usage.PromptCacheMissTokens <= maxUsageTokens
}

func validFinishReason(reason string) bool {
	switch reason {
	case "stop", "length", "content_filter", "tool_calls", "insufficient_system_resource":
		return true
	default:
		return false
	}
}

func mapUsage(usage streamUsage) reasoner.Usage {
	return reasoner.Usage{PromptTokens: *usage.PromptTokens, CompletionTokens: *usage.CompletionTokens, TotalTokens: *usage.TotalTokens, PromptCacheHitTokens: usage.PromptCacheHitTokens, PromptCacheMissTokens: usage.PromptCacheMissTokens}
}
