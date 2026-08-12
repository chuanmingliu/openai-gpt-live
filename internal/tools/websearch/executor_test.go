package websearch

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/chuanmingliu/openai-gpt-live/internal/config"
	"github.com/chuanmingliu/openai-gpt-live/internal/toolpolicy"
	"github.com/chuanmingliu/openai-gpt-live/internal/worker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func successfulExecutor(t *testing.T) *Executor {
	t.Helper()
	return executorWithTransport(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return response(http.StatusOK, "text/html", "", resultHTML("Go &amp; Docs", "  Go\n documentation  ", "https://go.dev/doc/")), nil
	}))
}

func TestExecuteAcceptsOnlyCanonicalClosedWorldArguments(t *testing.T) {
	executor := successfulExecutor(t)
	output, err := executor.Execute(t.Context(), worker.ToolExecutionRequest{Arguments: json.RawMessage(" { \n \t \"query\" : \"golang\" } "), Policy: toolpolicy.DefaultPolicy()})
	require.NoError(t, err)
	assert.JSONEq(t, `{"query":"golang","results":[{"title":"Go & Docs","snippet":"Go documentation","url":"https://go.dev/doc/"}]}`, string(output))

	invalid := []json.RawMessage{
		nil,
		json.RawMessage(`null`),
		json.RawMessage(`[]`),
		json.RawMessage(`{}`),
		json.RawMessage(`{"query":""}`),
		json.RawMessage(`{"query":"golang","extra":true}`),
		json.RawMessage(`{"query":"first","query":"second"}`),
		json.RawMessage(`{"query":"golang"} {}`),
		json.RawMessage(`{"query":7}`),
		json.RawMessage(`{"query":"` + strings.Repeat("q", 513) + `"}`),
	}
	for _, arguments := range invalid {
		_, err := executor.Execute(t.Context(), worker.ToolExecutionRequest{Arguments: arguments})
		require.Error(t, err, string(arguments))
	}
}

func TestExecuteCanonicalizesTrimmedQueryBeforeHTTPAndPrivateOutput(t *testing.T) {
	var posted string
	executor := executorWithTransport(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		require.NoError(t, request.ParseForm())
		posted = request.Form.Get("q")
		return response(http.StatusOK, "text/html", "", resultHTML("Go", "Docs", "https://go.dev/")), nil
	}))
	output, err := executor.Execute(t.Context(), worker.ToolExecutionRequest{
		Arguments: json.RawMessage(`{"query":"  golang  "}`),
		Policy:    toolpolicy.DefaultPolicy(),
	})
	require.NoError(t, err)
	assert.Equal(t, "golang", posted)
	assert.JSONEq(t, `{"query":"golang","results":[{"title":"Go","snippet":"Docs","url":"https://go.dev/"}]}`, string(output))
}

func TestExecuteRejectsWhitespaceAndControlBearingQueriesBeforeHTTP(t *testing.T) {
	for _, query := range []string{"   ", "  ", "line\nfeed", "tab\tinside", "nul\x00byte"} {
		called := false
		executor := executorWithTransport(roundTripFunc(func(*http.Request) (*http.Response, error) {
			called = true
			return nil, errors.New("must not execute")
		}))
		arguments, err := json.Marshal(map[string]string{"query": query})
		require.NoError(t, err)
		_, err = executor.Execute(t.Context(), worker.ToolExecutionRequest{Arguments: arguments, Policy: toolpolicy.DefaultPolicy()})
		require.Error(t, err)
		assert.False(t, called)
		assert.NotContains(t, err.Error(), query)
	}
}

func TestExecuteUsesServerPolicyNotArgumentPolicyFields(t *testing.T) {
	executor := successfulExecutor(t)
	policy := toolpolicy.DefaultPolicy()
	policy.WebSearch.MaxResults = 2
	output, err := executor.Execute(t.Context(), worker.ToolExecutionRequest{Arguments: json.RawMessage(`{"query":"golang"}`), Policy: policy})
	require.NoError(t, err)
	assert.JSONEq(t, `{"query":"golang","results":[{"title":"Go & Docs","snippet":"Go documentation","url":"https://go.dev/doc/"}]}`, string(output))

	_, err = executor.Execute(t.Context(), worker.ToolExecutionRequest{Arguments: json.RawMessage(`{"query":"golang","timeout_ms":10000}`), Policy: policy})
	require.Error(t, err)
	_, err = executor.Execute(t.Context(), worker.ToolExecutionRequest{Arguments: json.RawMessage(`{"query":"golang","url":"https://attacker.example/"}`), Policy: policy})
	require.Error(t, err)
}

func TestProjectRetainsOnlyBoundedPublicSources(t *testing.T) {
	executor := successfulExecutor(t)
	private := json.RawMessage(`{"query":"private query","results":[{"title":"Go Docs","snippet":"private snippet","url":"https://go.dev/doc/"},{"title":"Spec","snippet":"private result","url":"https://go.dev/ref/spec"}]}`)
	projected, err := executor.Project(private)
	require.NoError(t, err)
	assert.JSONEq(t, `{"sources":[{"title":"Go Docs","url":"https://go.dev/doc/"},{"title":"Spec","url":"https://go.dev/ref/spec"}]}`, string(projected))
	assert.NotContains(t, string(projected), "private")
}

func TestProjectRejectsMalformedPrivateOutputAndUnsafeSources(t *testing.T) {
	executor := successfulExecutor(t)
	invalid := []json.RawMessage{
		nil,
		json.RawMessage(`{"query":"q","results":[]} trailing`),
		json.RawMessage(`{"query":"q","results":[],"unknown":true}`),
		json.RawMessage(`{"query":"q","results":[{"title":"x","snippet":"s","url":"http://example.com/"}]}`),
		json.RawMessage(`{"query":"q","results":[{"title":"","snippet":"s","url":"https://example.com/"}]}`),
		json.RawMessage(`{"query":"q","results":[{"title":"x","snippet":"` + strings.Repeat("s", 1025) + `","url":"https://example.com/"}]}`),
	}
	for _, raw := range invalid {
		_, err := executor.Project(raw)
		require.Error(t, err, string(raw))
	}

	eleven := output{Query: "q"}
	for range 11 {
		eleven.Results = append(eleven.Results, Result{Title: "x", Snippet: "s", URL: "https://example.com/"})
	}
	raw, err := json.Marshal(eleven)
	require.NoError(t, err)
	_, err = executor.Project(raw)
	require.Error(t, err)
}

func TestRegisteredToolHasExactSchemaAndReadOnlyMetadata(t *testing.T) {
	executor := successfulExecutor(t)
	registered := RegisteredTool(executor)
	assert.Equal(t, toolpolicy.WebSearchTool, registered.Definition.Name)
	assert.JSONEq(t, `{"type":"object","additionalProperties":false,"required":["query"],"properties":{"query":{"type":"string","minLength":1,"maxLength":512}}}`, string(registered.Definition.Parameters))
	assert.False(t, registered.SideEffecting)
	assert.True(t, registered.ReadOnly)
	assert.True(t, registered.ParallelSafe)
	assert.True(t, registered.StableIdempotency)
	assert.Equal(t, worker.ProgressPhaseSearching, registered.StartedPhase)
	assert.Equal(t, worker.ProgressPhaseReadingSources, registered.CompletedPhase)
	assert.Same(t, executor, registered.Executor)
	assert.Same(t, executor, registered.Projector)

	_, err := worker.NewToolRegistry(map[string][]worker.RegisteredTool{"tools-v1": {registered}})
	require.NoError(t, err)
}

func TestRegisteredReadOnlyWebSearchExecutesWithSideEffectingToolsDisabled(t *testing.T) {
	cfg := config.Config{}
	cfg.Tools.WebSearch.Enabled = true
	cfg.Features.SideEffectingTools = false
	registered := RegisteredTool(successfulExecutor(t))
	require.False(t, cfg.Features.SideEffectingTools)
	require.True(t, cfg.Tools.WebSearch.Enabled)
	require.False(t, registered.SideEffecting)

	output, err := registered.Executor.Execute(t.Context(), worker.ToolExecutionRequest{
		Arguments: json.RawMessage(`{"query":"golang"}`),
		Policy:    toolpolicy.DefaultPolicy(),
	})
	require.NoError(t, err)
	assert.JSONEq(t, `{"query":"golang","results":[{"title":"Go & Docs","snippet":"Go documentation","url":"https://go.dev/doc/"}]}`, string(output))
}

func TestExecuteAndProjectErrorsDoNotContainPrivateContent(t *testing.T) {
	executor := executorWithTransport(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("private dependency result")
	}))
	_, err := executor.Execute(t.Context(), worker.ToolExecutionRequest{Arguments: json.RawMessage(`{"query":"private query"}`), Policy: toolpolicy.DefaultPolicy()})
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "private query")
	assert.NotContains(t, err.Error(), "private dependency result")

	_, err = executor.Project(json.RawMessage(`{"query":"private query","results":[{"title":"private title","snippet":"private snippet","url":"http://private.example/"}]}`))
	require.Error(t, err)
	for _, content := range []string{"private query", "private title", "private snippet", "private.example"} {
		assert.NotContains(t, err.Error(), content)
	}
}
