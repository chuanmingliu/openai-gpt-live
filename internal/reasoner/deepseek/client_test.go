package deepseek_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/reasoner"
	"github.com/chuanmingliu/openai-gpt-live/internal/reasoner/deepseek"
	"github.com/chuanmingliu/openai-gpt-live/internal/safety"
	"github.com/chuanmingliu/openai-gpt-live/internal/testkit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamUsesV4FlashAndOfficialThinkingDisabled(t *testing.T) {
	raw := "data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"
	server := testkit.NewDeepSeekServer(t, testkit.DeepSeekResponse{Chunks: [][]byte{[]byte(raw)}})
	client := newClient(t, server.URL, server.Client())
	request := reasoner.Request{
		Model:           "deepseek-v4-flash",
		Messages:        []reasoner.Message{{Role: reasoner.RoleUser, Content: "hello"}},
		MaxOutputTokens: 64,
		DisableThinking: true,
	}

	_, err := client.Stream(t.Context(), request, func(reasoner.Chunk) error { return nil })
	require.NoError(t, err)
	captured := <-server.Requests
	assert.JSONEq(t, `{"model":"deepseek-v4-flash","messages":[{"role":"user","content":"hello"}],"max_tokens":64,"stream":true,"stream_options":{"include_usage":true},"thinking":{"type":"disabled"}}`, string(captured.Body))
}

func TestStreamParsesSplitSSEAndOmitsReasoning(t *testing.T) {
	raw := ": hi\r\n\r\ndata: {\"id\":\"req-1\",\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":\"private\",\"content\":\"hel\"}}]}\r\n\r\n" +
		"data: {\"id\":\"req-1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"lo\"},\"finish_reason\":\"stop\"}]}\r\n\r\n" +
		"data: {\"id\":\"req-1\",\"choices\":[],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":1,\"total_tokens\":3}}\r\n\r\n" +
		"data: [DONE]\r\n\r\n"
	server := testkit.NewDeepSeekServer(t, testkit.DeepSeekResponse{Headers: http.Header{"Content-Type": {"text/event-stream"}}, Chunks: testkit.SplitBytes(raw, 1)})
	client := newClient(t, server.URL, server.Client())
	var chunks []reasoner.Chunk
	result, err := client.Stream(t.Context(), validRequest(), func(chunk reasoner.Chunk) error {
		chunks = append(chunks, chunk.Clone())
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, "hello", result.Text)
	assert.Equal(t, "stop", result.FinishReason)
	assert.Equal(t, "req-1", result.ProviderRequestID)
	assert.Equal(t, reasoner.Usage{PromptTokens: 2, CompletionTokens: 1, TotalTokens: 3}, result.Usage)
	assert.Equal(t, []reasoner.Chunk{{Text: "hel"}, {Text: "lo"}}, chunks)
	assert.NotContains(t, result.Text, "private")
}

func TestStreamReturnsOnlyTrustedProviderBlockMetadata(t *testing.T) {
	raw := "data: {\"id\":\"blocked-request\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"unsafe-provider-sentinel\"},\"finish_reason\":\"content_filter\"}]}\n\ndata: [DONE]\n\n"
	server := testkit.NewDeepSeekServer(t, testkit.DeepSeekResponse{Chunks: [][]byte{[]byte(raw)}})
	client := newClient(t, server.URL, server.Client())
	var chunks []reasoner.Chunk
	result, err := client.Stream(t.Context(), validRequest(), func(chunk reasoner.Chunk) error { chunks = append(chunks, chunk); return nil })
	require.NoError(t, err)
	assert.True(t, result.ProviderBlocked)
	assert.Equal(t, "content_filter", result.FinishReason)
	assert.Empty(t, result.Text)
	assert.Empty(t, result.ToolCalls)
	assert.Empty(t, chunks)
}

func TestStreamBuffersAllDeltasUntilValidatedTerminalAndDiscardsOnLateBlock(t *testing.T) {
	raw := "data: {\"id\":\"blocked-request\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"unsafe-text\"}}]}\n\n" +
		"data: {\"id\":\"blocked-request\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-1\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{\\\"secret\\\":\"}}]}}]}\n\n" +
		"data: {\"id\":\"blocked-request\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"true}\"}}]}}]}\n\n" +
		"data: {\"id\":\"blocked-request\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"content_filter\"}]}\n\n" +
		"data: [DONE]\n\n"
	server := testkit.NewDeepSeekServer(t, testkit.DeepSeekResponse{Chunks: testkit.SplitBytes(raw, 11)})
	client, err := deepseek.New(deepseek.Config{BaseURL: server.URL, MaxOutputBytes: len("unsafe-text") + len(`{"secret":true}`), MaxToolCalls: 4, MaxToolArgumentBytes: len(`{"secret":true}`)}, server.Client())
	require.NoError(t, err)
	var callbacks []reasoner.Chunk
	result, err := client.Stream(t.Context(), validRequest(), func(chunk reasoner.Chunk) error { callbacks = append(callbacks, chunk.Clone()); return nil })
	require.NoError(t, err)
	assert.True(t, result.ProviderBlocked)
	assert.Empty(t, result.Text)
	assert.Empty(t, result.ToolCalls)
	assert.Empty(t, callbacks)
}

func TestStreamLateBlockOutranksQuarantineByteAndCallbackOverflow(t *testing.T) {
	t.Run("byte limit plus one", func(t *testing.T) {
		raw := "data: {\"id\":\"blocked\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"123456789\"}}]}\n\n" +
			"data: {\"id\":\"blocked\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"content_filter\"}]}\n\ndata: [DONE]\n\n"
		result, callbacks, err := runQuarantineStream(t, raw, 8)
		require.NoError(t, err)
		assert.True(t, result.ProviderBlocked)
		assert.Empty(t, result.Text)
		assert.Empty(t, result.ToolCalls)
		assert.Zero(t, callbacks)
	})

	t.Run("callback limit plus one", func(t *testing.T) {
		const callbackLimit = 16 << 10
		const delta = "data: {\"id\":\"blocked\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0}]}}]}\n\n"
		var raw strings.Builder
		raw.Grow((callbackLimit + 1) * len(delta))
		for range callbackLimit + 1 {
			raw.WriteString(delta)
		}
		raw.WriteString("data: {\"id\":\"blocked\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"content_filter\"}]}\n\ndata: [DONE]\n\n")
		result, callbacks, err := runQuarantineStream(t, raw.String(), 1<<20)
		require.NoError(t, err)
		assert.True(t, result.ProviderBlocked)
		assert.Empty(t, result.Text)
		assert.Empty(t, result.ToolCalls)
		assert.Zero(t, callbacks)
	})
}

func TestStreamQuarantineOverflowDrainsToSafeTerminalOrDeterministicFailure(t *testing.T) {
	t.Run("safe terminal", func(t *testing.T) {
		raw := "data: {\"id\":\"safe\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"123456789\"}}]}\n\n" +
			"data: {\"id\":\"safe\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"
		_, callbacks, err := runQuarantineStream(t, raw, 8)
		var providerErr *reasoner.ProviderError
		require.ErrorAs(t, err, &providerErr)
		assert.Equal(t, "output_limit", providerErr.Code)
		assert.Zero(t, callbacks)
	})

	t.Run("malformed after overflow", func(t *testing.T) {
		raw := "data: {\"id\":\"safe\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"123456789\"}}]}\n\ndata: {\n\n"
		_, callbacks, err := runQuarantineStream(t, raw, 8)
		var providerErr *reasoner.ProviderError
		require.ErrorAs(t, err, &providerErr)
		assert.Equal(t, "malformed_chunk", providerErr.Code)
		assert.Zero(t, callbacks)
	})

	t.Run("provider error after overflow", func(t *testing.T) {
		raw := "data: {\"id\":\"safe\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"123456789\"}}]}\n\n" +
			"data: {\"id\":\"safe\",\"error\":{\"code\":\"overloaded\",\"message\":\"bounded\"}}\n\n"
		_, callbacks, err := runQuarantineStream(t, raw, 8)
		var providerErr *reasoner.ProviderError
		require.ErrorAs(t, err, &providerErr)
		assert.Equal(t, "overloaded", providerErr.Code)
		assert.Zero(t, callbacks)
	})

	t.Run("cancellation after overflow", func(t *testing.T) {
		raw := "data: {\"id\":\"safe\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"123456789\"}}]}\n\n"
		server := testkit.NewDeepSeekServer(t, testkit.DeepSeekResponse{Chunks: [][]byte{[]byte(raw)}, Stall: true})
		client, err := deepseek.New(deepseek.Config{BaseURL: server.URL, MaxOutputBytes: 8, MaxToolCalls: 4, MaxToolArgumentBytes: 64}, server.Client())
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() {
			_, streamErr := client.Stream(ctx, validRequest(), func(reasoner.Chunk) error { return nil })
			done <- streamErr
		}()
		<-server.Started
		select {
		case err := <-done:
			t.Fatalf("stream returned before terminal status: %v", err)
		case <-time.After(50 * time.Millisecond):
		}
		cancel()
		assert.ErrorIs(t, <-done, context.Canceled)
	})
}

func TestStreamReplaysBufferedCallbacksInOriginalOrderOnlyAfterValidDone(t *testing.T) {
	raw := "data: {\"id\":\"request\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"first\"}}]}\n\n" +
		"data: {\"id\":\"request\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"second\"},\"finish_reason\":\"stop\"}]}\n\n" +
		"data: [DONE]\n\n"
	server := testkit.NewDeepSeekServer(t, testkit.DeepSeekResponse{Chunks: [][]byte{[]byte(raw)}})
	client := newClient(t, server.URL, server.Client())
	var callbacks []reasoner.Chunk
	result, err := client.Stream(t.Context(), validRequest(), func(chunk reasoner.Chunk) error { callbacks = append(callbacks, chunk.Clone()); return nil })
	require.NoError(t, err)
	assert.Equal(t, "firstsecond", result.Text)
	assert.Equal(t, []reasoner.Chunk{{Text: "first"}, {Text: "second"}}, callbacks)
}

func TestStreamSerializesOnlyProviderContractAndSafetyID(t *testing.T) {
	server := testkit.NewDeepSeekServer(t, testkit.DeepSeekResponse{Chunks: [][]byte{[]byte("data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")}})
	client := newClientWithKey(t, server.URL, server.Client(), "secret-key")
	key := []byte("0123456789abcdef0123456789abcdef")
	safetyID, err := safety.ProviderSafetyID(key, "raw-tenant", "raw-user")
	require.NoError(t, err)
	request := validRequest()
	request.SafetyUserID = safetyID
	request.Messages = append(request.Messages,
		reasoner.Message{Role: reasoner.RoleAssistant, ToolCalls: []reasoner.ToolCall{{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"q":"x"}`)}}},
		reasoner.Message{Role: reasoner.RoleTool, ToolCallID: "call-1", Content: `{"value":1}`},
	)
	_, err = client.Stream(t.Context(), request, func(reasoner.Chunk) error { return nil })
	require.NoError(t, err)
	captured := <-server.Requests
	assert.Equal(t, "Bearer secret-key", captured.Header.Get("Authorization"))
	assert.Equal(t, "application/json", captured.Header.Get("Content-Type"))
	assert.Equal(t, "text/event-stream", captured.Header.Get("Accept"))
	assert.NotContains(t, string(captured.Body), "raw-tenant")
	assert.NotContains(t, string(captured.Body), "raw-user")
	assert.NotContains(t, string(captured.Body), "secret-key")
	assert.JSONEq(t, `{"model":"deepseek-chat","messages":[{"role":"user","content":"hello"},{"role":"assistant","tool_calls":[{"id":"call-1","type":"function","function":{"name":"lookup","arguments":"{\"q\":\"x\"}"}}]},{"role":"tool","content":"{\"value\":1}","tool_call_id":"call-1"}],"tools":[{"type":"function","function":{"name":"lookup","description":"find","parameters":{"type":"object","properties":{"q":{"type":"string"}}}}}],"max_tokens":64,"stream":true,"stream_options":{"include_usage":true},"user_id":"`+safetyID+`"}`, string(captured.Body))
}

func TestStreamAssemblesToolCallsInIndexOrderAndCopiesArguments(t *testing.T) {
	raw := "data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":1,\"id\":\"b\",\"function\":{\"name\":\"second\",\"arguments\":\"{\\\"b\\\":\"}},{\"index\":0,\"id\":\"a\",\"function\":{\"name\":\"first\",\"arguments\":\"{\\\"a\\\":\"}}]}}]}\n\n" +
		"data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"1}\"}},{\"index\":1,\"function\":{\"arguments\":\"2}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\n" +
		"data: [DONE]\n\n"
	server := testkit.NewDeepSeekServer(t, testkit.DeepSeekResponse{Chunks: testkit.SplitBytes(raw, 7)})
	client := newClient(t, server.URL, server.Client())
	var callback []reasoner.ToolCallDelta
	result, err := client.Stream(t.Context(), validRequest(), func(chunk reasoner.Chunk) error {
		callback = append(callback, chunk.ToolCalls...)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, result.ToolCalls, 2)
	assert.Equal(t, "a", result.ToolCalls[0].ID)
	assert.JSONEq(t, `{"a":1}`, string(result.ToolCalls[0].Arguments))
	assert.Equal(t, "b", result.ToolCalls[1].ID)
	assert.JSONEq(t, `{"b":2}`, string(result.ToolCalls[1].Arguments))
	callback[0].Arguments = "changed"
	assert.JSONEq(t, `{"b":2}`, string(result.ToolCalls[1].Arguments))
}

func TestStreamAssemblesFragmentedToolIdentity(t *testing.T) {
	raw := "data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_\",\"function\":{\"name\":\"look\"}}]}}]}\n\n" +
		"data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"1\",\"function\":{\"name\":\"up\"}}]}}]}\n\n" +
		"data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\n" +
		"data: [DONE]\n\n"
	server := testkit.NewDeepSeekServer(t, testkit.DeepSeekResponse{Chunks: [][]byte{[]byte(raw)}})
	client := newClient(t, server.URL, server.Client())
	result, err := client.Stream(t.Context(), validRequest(), func(reasoner.Chunk) error { return nil })
	require.NoError(t, err)
	require.Len(t, result.ToolCalls, 1)
	assert.Equal(t, "call_1", result.ToolCalls[0].ID)
	assert.Equal(t, "lookup", result.ToolCalls[0].Name)
	assert.JSONEq(t, `{}`, string(result.ToolCalls[0].Arguments))

	copyResult := result.Clone()
	copyResult.ToolCalls[0].Arguments[0] = '['
	assert.JSONEq(t, `{}`, string(result.ToolCalls[0].Arguments))
}

func TestStreamRejectsConflictingIdentityAfterArgumentsStart(t *testing.T) {
	raw := "data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-a\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{\"}}]}}]}\n\n" +
		"data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-b\",\"function\":{\"name\":\"other\",\"arguments\":\"}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\n" +
		"data: [DONE]\n\n"
	server := testkit.NewDeepSeekServer(t, testkit.DeepSeekResponse{Chunks: [][]byte{[]byte(raw)}})
	client := newClient(t, server.URL, server.Client())
	_, err := client.Stream(t.Context(), validRequest(), func(reasoner.Chunk) error { return nil })
	assert.Error(t, err)
}

func TestStreamRejectsInvalidToolAssembly(t *testing.T) {
	tests := map[string]string{
		"sparse":       `{"id":"r","choices":[{"index":0,"delta":{"tool_calls":[{"index":1,"id":"x","function":{"name":"f","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}`,
		"duplicate id": `{"id":"r","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"x","function":{"name":"f","arguments":"{}"}},{"index":1,"id":"x","function":{"name":"g","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}`,
		"bad json":     `{"id":"r","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"x","function":{"name":"f","arguments":"{"}}]},"finish_reason":"tool_calls"}]}`,
		"missing id":   `{"id":"r","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"name":"f","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}`,
	}
	for name, event := range tests {
		t.Run(name, func(t *testing.T) {
			server := testkit.NewDeepSeekServer(t, testkit.DeepSeekResponse{Chunks: [][]byte{[]byte("data: " + event + "\n\ndata: [DONE]\n\n")}})
			client := newClient(t, server.URL, server.Client())
			_, err := client.Stream(t.Context(), validRequest(), func(reasoner.Chunk) error { return nil })
			assert.Error(t, err)
		})
	}
}

func TestStreamEnforcesAggregateAnswerAndToolOutput(t *testing.T) {
	raw := "data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"x\",\"function\":{\"name\":\"f\",\"arguments\":\"{}\"}}]}}]}\n\n" +
		"data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"12\"},\"finish_reason\":\"tool_calls\"}]}\n\n" +
		"data: [DONE]\n\n"
	server := testkit.NewDeepSeekServer(t, testkit.DeepSeekResponse{Chunks: [][]byte{[]byte(raw)}})
	client, err := deepseek.New(deepseek.Config{BaseURL: server.URL, MaxRequestBytes: 1 << 20, MaxOutputBytes: 3, MaxToolCalls: 2, MaxToolArgumentBytes: 16}, server.Client())
	require.NoError(t, err)
	_, err = client.Stream(t.Context(), validRequest(), func(reasoner.Chunk) error { return nil })
	assert.Error(t, err)
}

func TestStreamClassifiesHTTPAndRetryAfter(t *testing.T) {
	now := time.Date(2026, 8, 7, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		status     int
		retryAfter string
		date       string
		retryable  bool
		wantAfter  time.Duration
	}{
		{408, "", "", true, 0},
		{429, "7", "", true, 7 * time.Second},
		{429, now.Add(9 * time.Second).Format(http.TimeFormat), now.Format(http.TimeFormat), true, 9 * time.Second},
		{500, "", "", true, 0},
		{400, "", "", false, 0},
	}
	for _, test := range tests {
		t.Run(http.StatusText(test.status), func(t *testing.T) {
			headers := http.Header{"Retry-After": {test.retryAfter}, "Date": {test.date}}
			server := testkit.NewDeepSeekServer(t, testkit.DeepSeekResponse{Status: test.status, Headers: headers, Chunks: [][]byte{[]byte(`{"error":{"code":"bad","message":"bounded"}}`)}})
			client := newClient(t, server.URL, server.Client())
			_, err := client.Stream(t.Context(), validRequest(), func(reasoner.Chunk) error { return nil })
			var providerErr *reasoner.ProviderError
			require.ErrorAs(t, err, &providerErr)
			assert.Equal(t, test.retryable, providerErr.Retryable)
			assert.Equal(t, test.wantAfter, providerErr.RetryAfter)
			assert.Equal(t, test.status, providerErr.HTTPStatus)
			assert.NotContains(t, err.Error(), "hello")
		})
	}
}

func TestStreamRejectsMalformedRetryAfterAndBoundsErrorBody(t *testing.T) {
	for _, value := range []string{"-1", "+1", "999999999999999999999999", "not-a-date"} {
		t.Run(value, func(t *testing.T) {
			server := testkit.NewDeepSeekServer(t, testkit.DeepSeekResponse{Status: 429, Headers: http.Header{"Retry-After": {value}}, Chunks: [][]byte{[]byte(strings.Repeat("secret-prompt-", 8<<10))}})
			client := newClient(t, server.URL, server.Client())
			_, err := client.Stream(t.Context(), validRequest(), func(reasoner.Chunk) error { return nil })
			var providerErr *reasoner.ProviderError
			require.ErrorAs(t, err, &providerErr)
			assert.Zero(t, providerErr.RetryAfter)
			assert.LessOrEqual(t, len(providerErr.Message), 512)
			assert.NotContains(t, err.Error(), "secret-prompt")
		})
	}
}

func TestStreamPreservesStatusRetryPolicyWhenErrorBodyReadFails(t *testing.T) {
	for _, status := range []int{http.StatusRequestTimeout, http.StatusTooManyRequests} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			closed := make(chan struct{})
			body := &trackingBody{Reader: &failAfterReader{data: []byte(`{"error":{"message":"diagnostic-secret"}`), err: errors.New("truncated diagnostic")}, closed: closed}
			httpClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: status,
					Header:     http.Header{"Retry-After": {"7"}},
					Body:       body,
				}, nil
			})}
			client := newClient(t, "http://provider.example", httpClient)
			_, err := client.Stream(t.Context(), validRequest(), func(reasoner.Chunk) error { return nil })
			var providerErr *reasoner.ProviderError
			require.ErrorAs(t, err, &providerErr)
			assert.True(t, providerErr.Retryable)
			assert.Equal(t, 7*time.Second, providerErr.RetryAfter)
			assert.Equal(t, status, providerErr.HTTPStatus)
			assert.NotContains(t, err.Error(), "diagnostic-secret")
			assert.LessOrEqual(t, len(err.Error()), 128)
			assertClosed(t, closed)
		})
	}
}

func TestStreamFailureModes(t *testing.T) {
	tests := map[string]struct {
		stream    string
		retryable bool
	}{
		"malformed":            {"data: {\n\ndata: [DONE]\n\n", false},
		"premature eof":        {"data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"x\"}}]}\n\n", true},
		"bad choice":           {"data: {\"id\":\"r\",\"choices\":[{\"index\":1,\"delta\":{}}]}\n\ndata: [DONE]\n\n", false},
		"missing choice index": {"data: {\"id\":\"r\",\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n", false},
		"missing delta":        {"data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n", false},
		"bad finish":           {"data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"invented\"}]}\n\ndata: [DONE]\n\n", false},
		"missing tool index":   {"data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"id\":\"x\",\"function\":{\"name\":\"f\",\"arguments\":\"{}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n", false},
		"scalar arguments":     {"data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"x\",\"function\":{\"name\":\"f\",\"arguments\":\"1\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n", false},
		"conflict id":          {"data: {\"id\":\"r1\",\"choices\":[{\"index\":0,\"delta\":{}}]}\n\ndata: {\"id\":\"r2\",\"choices\":[]}\n\ndata: [DONE]\n\n", false},
		"duplicate finish": {"data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n" +
			"data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n", false},
		"finish then eof": {"data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n", true},
		"usage before terminal": {"data: {\"id\":\"r\",\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2}}\n\n" +
			"data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n", false},
		"incomplete usage": {"data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n" +
			"data: {\"id\":\"r\",\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1}}\n\ndata: [DONE]\n\n", false},
		"after terminal": {"data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n" +
			"data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"late\"}}]}\n\ndata: [DONE]\n\n", false},
		"resource": {"data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"insufficient_system_resource\"}]}\n\ndata: [DONE]\n\n", true},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			server := testkit.NewDeepSeekServer(t, testkit.DeepSeekResponse{Chunks: [][]byte{[]byte(test.stream)}})
			client := newClient(t, server.URL, server.Client())
			_, err := client.Stream(t.Context(), validRequest(), func(reasoner.Chunk) error { return nil })
			var providerErr *reasoner.ProviderError
			require.ErrorAs(t, err, &providerErr)
			assert.Equal(t, test.retryable, providerErr.Retryable)
		})
	}
}

func TestProviderErrorStringDoesNotExposeProviderControlledText(t *testing.T) {
	err := &reasoner.ProviderError{Code: "raw-tenant/raw-user/tool-arguments", Message: "prompt and reasoning_content", HTTPStatus: 400}
	assert.NotContains(t, err.Error(), "raw-tenant")
	assert.NotContains(t, err.Error(), "prompt")
}

func TestRequestValidationEnforcesStableToolCallIdentity(t *testing.T) {
	request := validRequest()
	request.Messages = append(request.Messages,
		reasoner.Message{Role: reasoner.RoleAssistant, ToolCalls: []reasoner.ToolCall{{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{}`)}}},
		reasoner.Message{Role: reasoner.RoleTool, ToolCallID: "call-1", Content: "ok"},
		reasoner.Message{Role: reasoner.RoleAssistant, ToolCalls: []reasoner.ToolCall{{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{}`)}}},
	)
	assert.Error(t, request.Validate())

	request = validRequest()
	request.Messages = append(request.Messages, reasoner.Message{Role: reasoner.RoleTool, ToolCallID: "unknown", Content: "ok"})
	assert.Error(t, request.Validate())
}

func TestStreamCancellationCallbackAndTransportFailures(t *testing.T) {
	t.Run("stalled cancellation", func(t *testing.T) {
		server := testkit.NewDeepSeekServer(t, testkit.DeepSeekResponse{Stall: true})
		client := newClient(t, server.URL, server.Client())
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() {
			_, err := client.Stream(ctx, validRequest(), func(reasoner.Chunk) error { return nil })
			done <- err
		}()
		<-server.Started
		cancel()
		select {
		case err := <-done:
			assert.ErrorIs(t, err, context.Canceled)
		case <-time.After(2 * time.Second):
			t.Fatal("Stream did not honor cancellation")
		}
	})
	t.Run("callback", func(t *testing.T) {
		server := testkit.NewDeepSeekServer(t, testkit.DeepSeekResponse{Chunks: [][]byte{[]byte("data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"x\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")}})
		client := newClient(t, server.URL, server.Client())
		callbackErr := errors.New("stop callback")
		_, err := client.Stream(t.Context(), validRequest(), func(reasoner.Chunk) error { return callbackErr })
		assert.ErrorIs(t, err, callbackErr)
		var providerErr *reasoner.ProviderError
		assert.False(t, errors.As(err, &providerErr))
	})
	t.Run("transport", func(t *testing.T) {
		transportErr := errors.New("offline")
		client := newClient(t, "http://example.invalid", &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, transportErr })})
		_, err := client.Stream(t.Context(), validRequest(), func(reasoner.Chunk) error { return nil })
		var providerErr *reasoner.ProviderError
		require.ErrorAs(t, err, &providerErr)
		assert.True(t, providerErr.Retryable)
		assert.NotContains(t, err.Error(), "offline")
	})
}

func TestStreamCancellationAfterTransportStopsBufferedCallbacks(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	closed := make(chan struct{})
	body := &trackingBody{Reader: strings.NewReader("data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"x\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"), closed: closed}
	httpClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		cancel()
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: body}, nil
	})}
	client := newClient(t, "http://provider.example", httpClient)
	callbacks := 0
	_, err := client.Stream(ctx, validRequest(), func(reasoner.Chunk) error {
		callbacks++
		return nil
	})
	assert.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, callbacks)
	assertClosed(t, closed)
}

func TestStreamCancellationFromCallbackStopsLaterBufferedCallbacks(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	closed := make(chan struct{})
	raw := "data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"first\"}}]}\n\n" +
		"data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"second\"},\"finish_reason\":\"stop\"}]}\n\n" +
		"data: [DONE]\n\n"
	body := &trackingBody{Reader: strings.NewReader(raw), closed: closed}
	httpClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: body}, nil
	})}
	client := newClient(t, "http://provider.example", httpClient)
	callbacks := 0
	_, err := client.Stream(ctx, validRequest(), func(reasoner.Chunk) error {
		callbacks++
		cancel()
		return nil
	})
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, callbacks)
	assertClosed(t, closed)
}

func TestStreamRejectsInvalidFunctionNameFragmentsBeforeCallback(t *testing.T) {
	streams := map[string]string{
		"whitespace":  `bad name`,
		"control":     `bad\u0000name`,
		"punctuation": `bad.name`,
	}
	for name, encodedName := range streams {
		t.Run(name, func(t *testing.T) {
			raw := "data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-1\",\"function\":{\"name\":\"" + encodedName + "\",\"arguments\":\"{}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n"
			server := testkit.NewDeepSeekServer(t, testkit.DeepSeekResponse{Chunks: [][]byte{[]byte(raw)}})
			client := newClient(t, server.URL, server.Client())
			callbacks := 0
			_, err := client.Stream(t.Context(), validRequest(), func(reasoner.Chunk) error {
				callbacks++
				return nil
			})
			assert.Error(t, err)
			assert.Zero(t, callbacks)
		})
	}
}

func TestStreamEnforcesFinishReasonToolCallConsistencyBeforeCallback(t *testing.T) {
	tests := map[string]struct {
		stream        string
		wantCallbacks int
	}{
		"tool_calls without calls": {
			stream: "data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n",
		},
		"calls with stop": {
			stream: "data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-1\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{}\"}}]},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n",
		},
		"invalid finish with content": {
			stream: "data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"must-not-deliver\"},\"finish_reason\":\"invented\"}]}\n\ndata: [DONE]\n\n",
		},
		"earlier calls with stop": {
			stream: "data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-1\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{}\"}}]}}]}\n\n" +
				"data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			server := testkit.NewDeepSeekServer(t, testkit.DeepSeekResponse{Chunks: [][]byte{[]byte(test.stream)}})
			client := newClient(t, server.URL, server.Client())
			callbacks := 0
			_, err := client.Stream(t.Context(), validRequest(), func(reasoner.Chunk) error {
				callbacks++
				return nil
			})
			assert.Error(t, err)
			assert.Equal(t, test.wantCallbacks, callbacks)
		})
	}
}

func TestStreamAcceptsEarlierCompletedCallsWithEmptyToolTerminal(t *testing.T) {
	raw := "data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-1\",\"function\":{\"name\":\"look\"}}]}}]}\n\n" +
		"data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"name\":\"up\",\"arguments\":\"{}\"}}]}}]}\n\n" +
		"data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n"
	server := testkit.NewDeepSeekServer(t, testkit.DeepSeekResponse{Chunks: [][]byte{[]byte(raw)}})
	client := newClient(t, server.URL, server.Client())
	result, err := client.Stream(t.Context(), validRequest(), func(reasoner.Chunk) error { return nil })
	require.NoError(t, err)
	require.Len(t, result.ToolCalls, 1)
	assert.Equal(t, "lookup", result.ToolCalls[0].Name)
}

func TestStreamResourceFinishTakesPrecedenceOverPartialOutput(t *testing.T) {
	resourceTerminal := "data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"insufficient_system_resource\"}]}\n\ndata: [DONE]\n\n"
	tests := map[string]struct {
		stream        string
		wantCallbacks int
	}{
		"no fragments": {stream: resourceTerminal},
		"prior text": {
			stream: "data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"partial-text\"}}]}\n\n" + resourceTerminal,
		},
		"prior tool fragment": {
			stream: "data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-1\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{\"}}]}}]}\n\n" + resourceTerminal,
		},
		"same chunk tool fragment": {
			stream: "data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"must-not-deliver\",\"tool_calls\":[{\"index\":0,\"id\":\"call-1\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{\"}}]},\"finish_reason\":\"insufficient_system_resource\"}]}\n\ndata: [DONE]\n\n",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			closed := make(chan struct{})
			body := &trackingBody{Reader: strings.NewReader(test.stream), closed: closed}
			httpClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: body}, nil
			})}
			client := newClient(t, "http://provider.example", httpClient)
			callbacks := 0
			_, err := client.Stream(t.Context(), validRequest(), func(reasoner.Chunk) error {
				callbacks++
				return nil
			})
			var providerErr *reasoner.ProviderError
			require.ErrorAs(t, err, &providerErr)
			assert.Equal(t, "insufficient_system_resource", providerErr.Code)
			assert.True(t, providerErr.Retryable)
			assert.Equal(t, test.wantCallbacks, callbacks)
			assert.NotContains(t, err.Error(), "partial")
			assertClosed(t, closed)
		})
	}
}

func TestStreamToolFragmentAllocationScalesLinearly(t *testing.T) {
	warm := toolFragmentStream(100)
	_, err := runBufferedToolStream(t, warm, 32<<10)
	require.NoError(t, err)

	small := allocatedBytesForToolStream(t, toolFragmentStream(1_000), 32<<10)
	large := allocatedBytesForToolStream(t, toolFragmentStream(16_000), 32<<10)
	t.Logf("allocated bytes: 1,000 fragments=%d; 16,000 fragments=%d; ratio=%.2f", small, large, float64(large)/float64(small))
	assert.Less(t, large, small*20, "16x fragments must remain within a generous linear allocation envelope: small=%d large=%d", small, large)
}

func TestStreamToolArgumentsHonorExactAggregateBoundaries(t *testing.T) {
	const limit = 64
	for _, test := range []struct {
		name          string
		argumentBytes int
		wantError     bool
		wantCallbacks int
	}{
		{name: "exact", argumentBytes: limit, wantCallbacks: 1},
		{name: "first byte over", argumentBytes: limit + 1, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			arguments := "{" + strings.Repeat(" ", test.argumentBytes-2) + "}"
			raw := "data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-1\",\"function\":{\"name\":\"lookup\",\"arguments\":\"" + arguments + "\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n"
			httpClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(raw))}, nil
			})}
			client, err := deepseek.New(deepseek.Config{BaseURL: "http://provider.example", MaxRequestBytes: 1 << 20, MaxOutputBytes: limit, MaxToolCalls: 2, MaxToolArgumentBytes: limit}, httpClient)
			require.NoError(t, err)
			callbacks := 0
			result, err := client.Stream(t.Context(), validRequest(), func(reasoner.Chunk) error {
				callbacks++
				return nil
			})
			if test.wantError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Len(t, result.ToolCalls, 1)
				assert.Len(t, result.ToolCalls[0].Arguments, limit)
			}
			assert.Equal(t, test.wantCallbacks, callbacks)
		})
	}
}

func TestStreamClosesBodyAfterCallbackFailure(t *testing.T) {
	closed := make(chan struct{})
	body := &trackingBody{Reader: strings.NewReader("data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"x\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"), closed: closed}
	httpClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: body}, nil
	})}
	client := newClient(t, "http://provider.example", httpClient)
	callbackErr := errors.New("callback stopped")
	_, err := client.Stream(t.Context(), validRequest(), func(reasoner.Chunk) error { return callbackErr })
	assert.ErrorIs(t, err, callbackErr)
	select {
	case <-closed:
	default:
		t.Fatal("response body was not closed")
	}
}

func TestRequestValidationRejectsRoleSchemaAndSafetyErrorsBeforeHTTP(t *testing.T) {
	server := testkit.NewDeepSeekServer(t, testkit.DeepSeekResponse{})
	client := newClient(t, server.URL, server.Client())
	tests := map[string]func(*reasoner.Request){
		"invalid UTF-8": func(request *reasoner.Request) { request.Messages[0].Content = string([]byte{0xff}) },
		"system tool calls": func(request *reasoner.Request) {
			request.Messages[0] = reasoner.Message{Role: reasoner.RoleSystem, Content: "x", ToolCalls: []reasoner.ToolCall{{ID: "x", Name: "f", Arguments: json.RawMessage(`{}`)}}}
		},
		"tool without ID": func(request *reasoner.Request) {
			request.Messages[0] = reasoner.Message{Role: reasoner.RoleTool, Content: "x"}
		},
		"assistant empty": func(request *reasoner.Request) { request.Messages[0] = reasoner.Message{Role: reasoner.RoleAssistant} },
		"bad schema type": func(request *reasoner.Request) { request.Tools[0].Parameters = json.RawMessage(`{"type":"array"}`) },
		"schema trailing": func(request *reasoner.Request) {
			request.Tools[0].Parameters = json.RawMessage(`{"type":"object"} false`)
		},
		"schema garbage": func(request *reasoner.Request) {
			request.Tools[0].Parameters = json.RawMessage(`{"type":"object"} garbage`)
		},
		"bad safety ID": func(request *reasoner.Request) { request.SafetyUserID = "not safe" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			request := validRequest()
			mutate(&request)
			assert.Error(t, request.Validate())
			_, err := client.Stream(t.Context(), request, func(reasoner.Chunk) error { return nil })
			assert.Error(t, err)
		})
	}
	select {
	case <-server.Requests:
		t.Fatal("invalid request reached provider")
	default:
	}
}

func TestStreamValidatesBeforeRequestAndBoundsOutput(t *testing.T) {
	server := testkit.NewDeepSeekServer(t, testkit.DeepSeekResponse{Chunks: [][]byte{[]byte("data: [DONE]\n\n")}})
	client, err := deepseek.New(deepseek.Config{BaseURL: server.URL, MaxRequestBytes: 128, MaxOutputBytes: 3, MaxToolCalls: 2, MaxToolArgumentBytes: 16}, server.Client())
	require.NoError(t, err)
	invalid := validRequest()
	invalid.Messages[0].Role = "invalid"
	_, err = client.Stream(t.Context(), invalid, func(reasoner.Chunk) error { return nil })
	assert.Error(t, err)
	select {
	case <-server.Requests:
		t.Fatal("invalid request reached provider")
	default:
	}

	server2 := testkit.NewDeepSeekServer(t, testkit.DeepSeekResponse{Chunks: [][]byte{[]byte("data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"four\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")}})
	client, err = deepseek.New(deepseek.Config{BaseURL: server2.URL, MaxRequestBytes: 1 << 20, MaxOutputBytes: 3, MaxToolCalls: 2, MaxToolArgumentBytes: 16}, server2.Client())
	require.NoError(t, err)
	_, err = client.Stream(t.Context(), validRequest(), func(reasoner.Chunk) error { return nil })
	assert.Error(t, err)
}

func TestClientConstructorNilCallbackCancelledAndConcurrentReuse(t *testing.T) {
	_, err := deepseek.New(deepseek.Config{}, http.DefaultClient)
	assert.Error(t, err)
	_, err = deepseek.New(deepseek.Config{BaseURL: "https://api.deepseek.com"}, nil)
	assert.Error(t, err)

	server := testkit.NewDeepSeekServer(t, testkit.DeepSeekResponse{Chunks: [][]byte{[]byte("data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")}})
	client := newClient(t, server.URL, server.Client())
	_, err = client.Stream(t.Context(), validRequest(), nil)
	assert.Error(t, err)
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = client.Stream(cancelled, validRequest(), func(reasoner.Chunk) error { return nil })
	assert.ErrorIs(t, err, context.Canceled)

	var failures atomic.Int32
	var wait sync.WaitGroup
	for range 8 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if _, err := client.Stream(t.Context(), validRequest(), func(reasoner.Chunk) error { return nil }); err != nil {
				failures.Add(1)
			}
		}()
	}
	wait.Wait()
	assert.Zero(t, failures.Load())
}

func TestClientConstructorRejectsUnsafeConfiguration(t *testing.T) {
	configs := []deepseek.Config{
		{BaseURL: "ftp://provider.example"},
		{BaseURL: "https://user:pass@provider.example"},
		{BaseURL: "https://provider.example?token=secret"},
		{BaseURL: "https://provider.example#fragment"},
		{BaseURL: "https://provider.example", APIKey: "key\r\ninjected: yes"},
		{BaseURL: "https://provider.example", APIKey: "key\x00value"},
		{BaseURL: "https://provider.example", APIKey: "key\x7fvalue"},
		{BaseURL: "https://provider.example", APIKey: "key value"},
		{BaseURL: "https://provider.example", APIKey: string([]byte{0xff})},
		{BaseURL: "https://provider.example", MaxOutputBytes: -1},
	}
	for _, config := range configs {
		_, err := deepseek.New(config, http.DefaultClient)
		assert.Error(t, err)
	}
}

func validRequest() reasoner.Request {
	return reasoner.Request{
		Model:           "deepseek-chat",
		Messages:        []reasoner.Message{{Role: reasoner.RoleUser, Content: "hello"}},
		Tools:           []reasoner.ToolDefinition{{Name: "lookup", Description: "find", Parameters: json.RawMessage(`{"type":"object","properties":{"q":{"type":"string"}}}`)}},
		MaxOutputTokens: 64,
		SafetyUserID:    "safe_user-ID",
	}
}

func newClient(t *testing.T, baseURL string, httpClient *http.Client) *deepseek.Client {
	t.Helper()
	return newClientWithKey(t, baseURL, httpClient, "")
}

func newClientWithKey(t *testing.T, baseURL string, httpClient *http.Client, key string) *deepseek.Client {
	t.Helper()
	client, err := deepseek.New(deepseek.Config{BaseURL: baseURL, APIKey: key, MaxRequestBytes: 1 << 20, MaxOutputBytes: 1 << 20, MaxToolCalls: 16, MaxToolArgumentBytes: 64 << 10}, httpClient)
	require.NoError(t, err)
	return client
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

type trackingBody struct {
	io.Reader
	closed chan struct{}
	once   sync.Once
}

type failAfterReader struct {
	data []byte
	err  error
}

func (reader *failAfterReader) Read(buffer []byte) (int, error) {
	if len(reader.data) == 0 {
		return 0, reader.err
	}
	count := copy(buffer, reader.data)
	reader.data = reader.data[count:]
	return count, nil
}

func assertClosed(t *testing.T, closed <-chan struct{}) {
	t.Helper()
	select {
	case <-closed:
	default:
		t.Fatal("response body was not closed")
	}
}

func toolFragmentStream(fragments int) string {
	if fragments < 2 {
		panic("test requires at least two argument fragments")
	}
	const prefix = "data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-1\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{\"}}]}}]}\n\n"
	const middle = "data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\" \"}}]}}]}\n\n"
	const suffix = "data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"}\"}}]}}]}\n\n" +
		"data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n"
	var stream strings.Builder
	stream.Grow(len(prefix) + len(suffix) + (fragments-2)*len(middle))
	stream.WriteString(prefix)
	for range fragments - 2 {
		stream.WriteString(middle)
	}
	stream.WriteString(suffix)
	return stream.String()
}

func allocatedBytesForToolStream(t *testing.T, stream string, limit int) uint64 {
	t.Helper()
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := runBufferedToolStream(t, stream, limit)
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	require.NoError(t, err)
	return after.TotalAlloc - before.TotalAlloc
}

func runBufferedToolStream(t *testing.T, stream string, limit int) (reasoner.Result, error) {
	t.Helper()
	httpClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(stream))}, nil
	})}
	client, err := deepseek.New(deepseek.Config{BaseURL: "http://provider.example", MaxRequestBytes: 1 << 20, MaxOutputBytes: limit, MaxToolCalls: 2, MaxToolArgumentBytes: limit}, httpClient)
	require.NoError(t, err)
	return client.Stream(t.Context(), validRequest(), func(reasoner.Chunk) error { return nil })
}

func runQuarantineStream(t *testing.T, stream string, limit int) (reasoner.Result, int, error) {
	t.Helper()
	httpClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(stream))}, nil
	})}
	client, err := deepseek.New(deepseek.Config{BaseURL: "http://provider.example", MaxRequestBytes: 1 << 20, MaxOutputBytes: limit, MaxToolCalls: 4, MaxToolArgumentBytes: 64}, httpClient)
	require.NoError(t, err)
	callbacks := 0
	result, streamErr := client.Stream(t.Context(), validRequest(), func(reasoner.Chunk) error {
		callbacks++
		return nil
	})
	return result, callbacks, streamErr
}

func (body *trackingBody) Close() error {
	body.once.Do(func() { close(body.closed) })
	return nil
}

var _ = io.EOF
var _ = strings.Builder{}
