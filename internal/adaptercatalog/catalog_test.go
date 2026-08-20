package adaptercatalog

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/profile"
	"github.com/chuanmingliu/openai-gpt-live/internal/worker"
)

func TestCompileRegistryRequiresExactNativeIdentityAndClonesDefinitions(t *testing.T) {
	manifest := testManifest()
	catalog, err := New("registry-v1", []Manifest{manifest}, []string{"crm_token"})
	if err != nil {
		t.Fatalf("new catalog: %v", err)
	}
	executor := executorFunc(func(context.Context, worker.ToolExecutionRequest) (json.RawMessage, error) {
		return json.RawMessage(`{"ok":true}`), nil
	})
	registry, err := CompileRegistry(catalog, map[string]Binding{
		manifest.AdapterID: {Enabled: true, Timeout: time.Second, MaxConcurrency: 2, MaxResultBytes: 8 << 10},
	}, []Installed{{
		AdapterID: manifest.AdapterID, AdapterVersion: manifest.AdapterVersion,
		DestinationID: manifest.DestinationID, TransportClass: manifest.TransportClass, Executor: executor,
	}})
	if err != nil {
		t.Fatalf("compile registry: %v", err)
	}
	tool, ok := registry.Lookup("registry-v1", manifest.Tool.Name)
	if !ok {
		t.Fatal("compiled tool missing")
	}
	manifest.Tool.Parameters[0] = '['
	if string(tool.Definition.Parameters) != `{"type":"object","additionalProperties":false}` {
		t.Fatalf("definition was not cloned: %s", tool.Definition.Parameters)
	}
	if tool.AdapterID != "knowledge_search" || tool.AdapterVersion != "v1" || tool.Timeout != time.Second || tool.MaxConcurrency != 2 || tool.MaxResultBytes != 8<<10 {
		t.Fatalf("compiled authority mismatch: %#v", tool)
	}
}

func TestCatalogValidatesProfileBindingWithoutLoosening(t *testing.T) {
	manifest := testManifest()
	catalog, err := New("registry-v1", []Manifest{manifest}, []string{"crm_token"})
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.ValidateBindings(t.Context(), "tenant-a", []profile.AdapterBinding{{AdapterID: manifest.AdapterID, Version: manifest.AdapterVersion, ToolIDs: []string{manifest.Tool.Name}, MaxResultBytes: 8 << 10}}); err != nil {
		t.Fatalf("tight binding: %v", err)
	}
	if err := catalog.ValidateBindings(t.Context(), "tenant-a", []profile.AdapterBinding{{AdapterID: manifest.AdapterID, Version: manifest.AdapterVersion, ToolIDs: []string{manifest.Tool.Name}, MaxResultBytes: manifest.MaxResultBytes + 1}}); !errors.Is(err, ErrBindingExpansion) {
		t.Fatalf("widen binding: got %v", err)
	}
}

func TestCompileRegistryRejectsDestinationMismatch(t *testing.T) {
	manifest := testManifest()
	catalog, err := New("registry-v1", []Manifest{manifest}, []string{"crm_token"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = CompileRegistry(catalog, map[string]Binding{manifest.AdapterID: {Enabled: true}}, []Installed{{
		AdapterID: manifest.AdapterID, AdapterVersion: manifest.AdapterVersion,
		DestinationID: "other", TransportClass: manifest.TransportClass,
		Executor: executorFunc(func(context.Context, worker.ToolExecutionRequest) (json.RawMessage, error) { return nil, nil }),
	}})
	if !errors.Is(err, ErrInstalledMismatch) {
		t.Fatalf("got %v, want %v", err, ErrInstalledMismatch)
	}
}

type executorFunc func(context.Context, worker.ToolExecutionRequest) (json.RawMessage, error)

func (f executorFunc) Execute(ctx context.Context, request worker.ToolExecutionRequest) (json.RawMessage, error) {
	return f(ctx, request)
}
