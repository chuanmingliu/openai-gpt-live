package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/chuanmingliu/openai-gpt-live/internal/toolpolicy"
	"github.com/chuanmingliu/openai-gpt-live/internal/tools/websearch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToolPolicyAPIStoresOnlyCanonicalSafeFields(t *testing.T) {
	handler, search := newDemoHandlerWithTools(t)
	put := httptest.NewRequest(http.MethodPut, "/api/tool-policy", strings.NewReader(`{"web_search":{"enabled":true,"safe_search":"moderate","region":"wt-wt","max_results":5,"timeout_ms":8000}}`))
	put.Header.Set("Origin", "http://127.0.0.1:3000")
	put.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, put)
	require.Equal(t, http.StatusNoContent, response.Code)

	get := httptest.NewRequest(http.MethodGet, "/api/tool-policy", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, get)
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Equal(t, "no-store", response.Header().Get("Cache-Control"))
	assert.JSONEq(t, `{"web_search":{"enabled":true,"safe_search":"strict","region":"wt-wt","max_results":5,"timeout_ms":8000}}`, response.Body.String())
	assert.Zero(t, search.calls)
}

func TestToolPolicyAPIUsesExactGatewayCatalogBounds(t *testing.T) {
	for _, body := range []string{
		`{"web_search":{"enabled":true,"safe_search":"strict","region":"wt-wt","max_results":6,"timeout_ms":8000}}`,
		`{"web_search":{"enabled":true,"safe_search":"strict","region":"wt-wt","max_results":5,"timeout_ms":8001}}`,
	} {
		assertRejectedToolPolicy(t, body, "http://127.0.0.1:3000")
	}
}

func TestToolPolicyAPIRejectsExecutableAndCrossOriginInput(t *testing.T) {
	for _, body := range []string{`{"command":"npx"}`, `{"mcpServers":{}}`, `{"web_search":{"enabled":true,"safe_search":"moderate","region":"wt-wt","max_results":5,"timeout_ms":8000},"command":"npx"}`} {
		assertRejectedToolPolicy(t, body, "http://127.0.0.1:3000")
	}
	assertRejectedToolPolicy(t, `{"web_search":{"enabled":true,"safe_search":"moderate","region":"wt-wt","max_results":5,"timeout_ms":8000}}`, "http://evil.invalid")
}

func TestToolPolicyAPIRejectsOversizedTrailingAndWrongMediaTypeInput(t *testing.T) {
	for _, body := range []string{
		`{"web_search":{"enabled":true,"safe_search":"moderate","region":"wt-wt","max_results":5,"timeout_ms":8000}} {}`,
		`{"web_search":{"enabled":true,"safe_search":"moderate","region":"wt-wt","max_results":5,"timeout_ms":8000,"destination":"https://evil.invalid"}}`,
		`{"web_search":{"enabled":true,"safe_search":"moderate","region":"wt-wt","max_results":5,"timeout_ms":8000},"padding":"` + strings.Repeat("x", 4096) + `"}`,
	} {
		assertRejectedToolPolicy(t, body, "http://127.0.0.1:3000")
	}
	handler, _ := newDemoHandlerWithTools(t)
	request := httptest.NewRequest(http.MethodPut, "/api/tool-policy", strings.NewReader(`{}`))
	request.Header.Set("Origin", "http://127.0.0.1:3000")
	request.Header.Set("Content-Type", "text/plain")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	assert.Equal(t, http.StatusUnsupportedMediaType, response.Code)
}

func TestToolPolicyDiagnosticUsesFixedQueryAndReturnsOnlyCount(t *testing.T) {
	handler, search := newDemoHandlerWithTools(t)
	request := httptest.NewRequest(http.MethodPost, "/api/tool-policy/test", strings.NewReader(`{"query":"private browser query"}`))
	request.Header.Set("Origin", "http://127.0.0.1:3000")
	request.Header.Set("Content-Type", "application/json; charset=utf-8")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	assert.Equal(t, http.StatusOK, response.Code)
	assert.JSONEq(t, `{"ok":true,"result_count":3}`, response.Body.String())
	assert.Equal(t, "DuckDuckGo privacy search", search.request.Query)
	assert.Equal(t, toolpolicy.WebSearchPolicy{Enabled: true, SafeSearch: toolpolicy.SafeSearchStrict, Region: "wt-wt", MaxResults: 5, TimeoutMS: 8_000}, search.request.Policy)
	assert.NotContains(t, response.Body.String(), "private")
}

func TestToolPolicyDiagnosticBoundsFailuresAndHonorsCancellation(t *testing.T) {
	handler, search := newDemoHandlerWithTools(t)
	search.err = errors.New("private provider result")
	request := httptest.NewRequest(http.MethodPost, "/api/tool-policy/test", nil)
	request.Header.Set("Origin", "http://127.0.0.1:3000")
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	assert.Equal(t, http.StatusServiceUnavailable, response.Code)
	assert.JSONEq(t, `{"ok":false}`, response.Body.String())
	assert.NotContains(t, response.Body.String(), "private")

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	request = httptest.NewRequest(http.MethodPost, "/api/tool-policy/test", nil).WithContext(ctx)
	request.Header.Set("Origin", "http://127.0.0.1:3000")
	request.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	assert.Equal(t, http.StatusServiceUnavailable, response.Code)
	assert.True(t, search.canceled)
}

func assertRejectedToolPolicy(t *testing.T, body, origin string) {
	t.Helper()
	handler, _ := newDemoHandlerWithTools(t)
	request := httptest.NewRequest(http.MethodPut, "/api/tool-policy", strings.NewReader(body))
	request.Header.Set("Origin", origin)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	assert.Equal(t, http.StatusBadRequest, response.Code)
	assert.NotContains(t, response.Body.String(), "npx")
	assert.NotContains(t, response.Body.String(), "evil")
}

type recordingToolSearch struct {
	request  websearch.Request
	results  []websearch.Result
	err      error
	calls    int
	canceled bool
}

func (search *recordingToolSearch) Search(ctx context.Context, request websearch.Request) ([]websearch.Result, error) {
	search.calls++
	search.request = request
	if ctx.Err() != nil {
		search.canceled = true
		return nil, ctx.Err()
	}
	if search.err != nil {
		return nil, search.err
	}
	if search.results == nil {
		return []websearch.Result{{Title: "one"}, {Title: "two"}, {Title: "three"}}, nil
	}
	return search.results, nil
}

func newDemoHandlerWithTools(t *testing.T) (http.Handler, *recordingToolSearch) {
	t.Helper()
	search := &recordingToolSearch{}
	cfg, err := parseConfig(nil)
	require.NoError(t, err)
	handler, err := newHandlerWithToolSearch(cfg, []byte("sentinel"), nil, search)
	require.NoError(t, err)
	return handler, search
}
