package worker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/reasoner"
)

func TestToolRegistryEnforcesAdapterExecutionBounds(t *testing.T) {
	block := make(chan struct{})
	registry, err := NewToolRegistry(map[string][]RegisteredTool{"registry-v1": {{
		AdapterID: "knowledge_search", AdapterVersion: "v1", RegistryVersion: "registry-v1",
		DestinationID: "knowledge_store", TransportClass: "native", Approval: "none",
		Definition: reasoner.ToolDefinition{Name: "knowledge_search", Parameters: json.RawMessage(`{"type":"object"}`)},
		ReadOnly:   true, ParallelSafe: true, Timeout: 100 * time.Millisecond,
		MaxConcurrency: 1, MaxArgumentBytes: 8, MaxResultBytes: 7,
		Executor: toolExecutorFuncForRegistry(func(ctx context.Context, _ ToolExecutionRequest) (json.RawMessage, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-block:
				return json.RawMessage(`{"ok":1}`), nil
			}
		}),
	}}})
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	tool, _ := registry.Lookup("registry-v1", "knowledge_search")
	if _, err := tool.Executor.Execute(context.Background(), ToolExecutionRequest{Arguments: json.RawMessage(`{"large":1}`)}); !errors.Is(err, ErrToolArgumentLimit) {
		t.Fatalf("argument bound: got %v", err)
	}
	if _, err := tool.Executor.Execute(context.Background(), ToolExecutionRequest{Arguments: json.RawMessage(`{}`)}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout: got %v", err)
	}
	close(block)
	if _, err := tool.Executor.Execute(context.Background(), ToolExecutionRequest{Arguments: json.RawMessage(`{}`)}); !errors.Is(err, ErrToolResultLimit) {
		t.Fatalf("result bound: got %v", err)
	}
}

type toolExecutorFuncForRegistry func(context.Context, ToolExecutionRequest) (json.RawMessage, error)

func (f toolExecutorFuncForRegistry) Execute(ctx context.Context, request ToolExecutionRequest) (json.RawMessage, error) {
	return f(ctx, request)
}
