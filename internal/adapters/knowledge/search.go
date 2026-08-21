// Package knowledgeadapter exposes deterministic lexical search as one native,
// closed-world worker adapter. The model controls only query text and result
// count; tenant and immutable versions always come from the handoff snapshot.
package knowledgeadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"time"
	"unicode/utf8"

	"github.com/chuanmingliu/openai-gpt-live/internal/adaptercatalog"
	"github.com/chuanmingliu/openai-gpt-live/internal/knowledge"
	"github.com/chuanmingliu/openai-gpt-live/internal/reasoner"
	"github.com/chuanmingliu/openai-gpt-live/internal/worker"
)

const (
	AdapterID      = "knowledge.search"
	AdapterVersion = "v1"
	ToolName       = "knowledge_search"
	DestinationID  = "postgres_knowledge"
	maxArguments   = 4 << 10
	maxResults     = 96 << 10
)

var argumentSchema = json.RawMessage(`{"type":"object","properties":{"query":{"type":"string","maxLength":512},"max_results":{"type":"integer","minimum":1,"maximum":10}},"required":["query"],"additionalProperties":false}`)

type Searcher interface {
	Search(context.Context, knowledge.SearchRequest) ([]knowledge.Result, error)
}

type Executor struct{ searcher Searcher }

func New(searcher Searcher) (*Executor, error) {
	if nilSearcher(searcher) {
		return nil, errors.New("knowledge search dependency is unavailable")
	}
	return &Executor{searcher: searcher}, nil
}

func nilSearcher(searcher Searcher) bool {
	if searcher == nil {
		return true
	}
	value := reflect.ValueOf(searcher)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func (executor *Executor) Execute(ctx context.Context, request worker.ToolExecutionRequest) (json.RawMessage, error) {
	if executor == nil || executor.searcher == nil || ctx == nil || ctx.Err() != nil || request.TenantID == "" || len(request.Knowledge) == 0 || len(request.Arguments) == 0 || len(request.Arguments) > maxArguments || !utf8.Valid(request.Arguments) {
		return nil, knowledge.ErrInvalidKnowledge
	}
	arguments, err := decodeArguments(request.Arguments)
	if err != nil {
		return nil, err
	}
	refs := make([]knowledge.VersionRef, len(request.Knowledge))
	seen := make(map[string]struct{}, len(request.Knowledge))
	for index, pinned := range request.Knowledge {
		ref := knowledge.VersionRef{PackID: pinned.ID, Version: pinned.Version, Hash: pinned.Hash}
		if err := ref.Validate(); err != nil {
			return nil, err
		}
		key := fmt.Sprintf("%s\x00%d\x00%s", ref.PackID, ref.Version, ref.Hash)
		if _, duplicate := seen[key]; duplicate {
			return nil, knowledge.ErrInvalidKnowledge
		}
		seen[key] = struct{}{}
		refs[index] = ref
	}
	results, err := executor.searcher.Search(ctx, knowledge.SearchRequest{TenantID: request.TenantID, Refs: refs, Query: arguments.Query, MaxResults: arguments.MaxResults})
	if err != nil {
		return nil, fmt.Errorf("knowledge search failed: %w", err)
	}
	encoded, err := json.Marshal(struct {
		Results []knowledge.Result `json:"results"`
	}{Results: results})
	if err != nil || len(encoded) > maxResults {
		return nil, knowledge.ErrContentLimit
	}
	return append(json.RawMessage(nil), encoded...), nil
}

type arguments struct {
	Query      string
	MaxResults int
}

func decodeArguments(raw json.RawMessage) (arguments, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return arguments{}, knowledge.ErrInvalidKnowledge
	}
	var out arguments
	seen := make(map[string]struct{}, 2)
	for decoder.More() {
		keyToken, err := decoder.Token()
		key, ok := keyToken.(string)
		if err != nil || !ok {
			return arguments{}, knowledge.ErrInvalidKnowledge
		}
		if _, duplicate := seen[key]; duplicate {
			return arguments{}, knowledge.ErrInvalidKnowledge
		}
		seen[key] = struct{}{}
		switch key {
		case "query":
			if err := decoder.Decode(&out.Query); err != nil {
				return arguments{}, knowledge.ErrInvalidKnowledge
			}
		case "max_results":
			if err := decoder.Decode(&out.MaxResults); err != nil {
				return arguments{}, knowledge.ErrInvalidKnowledge
			}
		default:
			return arguments{}, knowledge.ErrInvalidKnowledge
		}
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') || decoder.Decode(&struct{}{}) != io.EOF || out.Query == "" || out.MaxResults < 0 || out.MaxResults > knowledge.MaxSearchResults {
		return arguments{}, knowledge.ErrInvalidKnowledge
	}
	return out, nil
}

func Manifest(registryVersion string) adaptercatalog.Manifest {
	return adaptercatalog.Manifest{
		AdapterID: AdapterID, AdapterVersion: AdapterVersion, RegistryVersion: registryVersion,
		Tool:     reasoner.ToolDefinition{Name: ToolName, Description: "Search exact knowledge versions pinned by this handoff and return cited excerpts.", Parameters: append(json.RawMessage(nil), argumentSchema...)},
		ReadOnly: true, ParallelSafe: true, StableIdempotency: true,
		Approval: adaptercatalog.ApprovalNone, Timeout: 5 * time.Second,
		MaxConcurrency: 8, MaxArgumentBytes: maxArguments, MaxResultBytes: maxResults,
		DestinationID: DestinationID, TransportClass: adaptercatalog.TransportNative,
		HealthClass: adaptercatalog.HealthRequired,
	}
}

func Installed(executor *Executor) adaptercatalog.Installed {
	return adaptercatalog.Installed{AdapterID: AdapterID, AdapterVersion: AdapterVersion, DestinationID: DestinationID, TransportClass: adaptercatalog.TransportNative, Executor: executor}
}
