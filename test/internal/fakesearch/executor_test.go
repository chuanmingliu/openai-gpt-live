package fakesearch

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/chuanmingliu/openai-gpt-live/internal/toolpolicy"
	"github.com/chuanmingliu/openai-gpt-live/internal/worker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecutorReturnsPrivateSearchSchemaAndPublicProjection(t *testing.T) {
	executor := New()
	request := worker.ToolExecutionRequest{
		InvocationID: "invocation-1", IdempotencyKey: "invocation-1",
		Arguments: json.RawMessage(`{"query":"Find the official Go website"}`),
		Policy:    toolpolicy.DefaultPolicy(),
	}

	private, err := executor.Execute(t.Context(), request)
	require.NoError(t, err)
	assert.JSONEq(t, `{"query":"Find the official Go website","results":[{"title":"The Go Programming Language","snippet":"Build simple, secure, scalable systems.","url":"https://go.dev/"}]}`, string(private))
	public, err := executor.Project(private)
	require.NoError(t, err)
	assert.JSONEq(t, `{"sources":[{"title":"The Go Programming Language","url":"https://go.dev/"}]}`, string(public))
	assert.Equal(t, 1, executor.Calls())
}

func TestExecutorCancellationStopsControlledSearch(t *testing.T) {
	executor := New(Controlled())
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() {
		_, err := executor.Execute(ctx, worker.ToolExecutionRequest{
			InvocationID: "invocation-2", IdempotencyKey: "invocation-2",
			Arguments: json.RawMessage(`{"query":"bounded fixture"}`), Policy: toolpolicy.DefaultPolicy(),
		})
		result <- err
	}()
	<-executor.Started()
	cancel()
	<-executor.ContextDone()
	assert.ErrorIs(t, <-result, context.Canceled)
	assert.Equal(t, 1, executor.Cancellations())
}

func TestExecutorCanReleaseLateResultForStaleWorkerFence(t *testing.T) {
	executor := New(Controlled(), IgnoreCancellation())
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan json.RawMessage, 1)
	go func() {
		raw, _ := executor.Execute(ctx, worker.ToolExecutionRequest{
			InvocationID: "invocation-3", IdempotencyKey: "invocation-3",
			Arguments: json.RawMessage(`{"query":"bounded fixture"}`), Policy: toolpolicy.DefaultPolicy(),
		})
		result <- raw
	}()
	<-executor.Started()
	cancel()
	<-executor.ContextDone()
	executor.Release()
	assert.JSONEq(t, `{"query":"bounded fixture","results":[{"title":"The Go Programming Language","snippet":"Build simple, secure, scalable systems.","url":"https://go.dev/"}]}`, string(<-result))
	assert.Zero(t, executor.Cancellations())
}

func TestRegisteredToolIsImmutableReadOnlyWebSearch(t *testing.T) {
	registered := RegisteredTool(New())
	assert.Equal(t, toolpolicy.WebSearchTool, registered.Definition.Name)
	assert.True(t, registered.ReadOnly)
	assert.True(t, registered.ParallelSafe)
	assert.False(t, registered.SideEffecting)
	assert.Equal(t, worker.ProgressPhaseSearching, registered.StartedPhase)
	assert.Equal(t, worker.ProgressPhaseReadingSources, registered.CompletedPhase)
}

func TestExecutorReturnsConfiguredContentFreeFailure(t *testing.T) {
	want := context.DeadlineExceeded
	executor := New(WithError(want))
	_, err := executor.Execute(t.Context(), worker.ToolExecutionRequest{
		InvocationID: "invocation-4", IdempotencyKey: "invocation-4",
		Arguments: json.RawMessage(`{"query":"private marker"}`), Policy: toolpolicy.DefaultPolicy(),
	})
	assert.ErrorIs(t, err, want)
	assert.NotContains(t, err.Error(), "private marker")
}
