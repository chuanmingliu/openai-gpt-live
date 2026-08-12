// Package fakesearch provides a deterministic, network-free web_search
// boundary for vertical tests. It records only lifecycle counters.
package fakesearch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/chuanmingliu/openai-gpt-live/internal/reasoner"
	"github.com/chuanmingliu/openai-gpt-live/internal/toolpolicy"
	"github.com/chuanmingliu/openai-gpt-live/internal/worker"
)

var argumentSchema = json.RawMessage(`{"type":"object","additionalProperties":false,"required":["query"],"properties":{"query":{"type":"string","minLength":1,"maxLength":512}}}`)

type option func(*Executor)

// Controlled holds execution until Release is called.
func Controlled() option {
	return func(executor *Executor) { executor.release = make(chan struct{}) }
}

// IgnoreCancellation makes a controlled executor return a deliberately late
// result. It exists only to prove that the worker's lease fence rejects stale
// output from a misbehaving dependency.
func IgnoreCancellation() option {
	return func(executor *Executor) { executor.ignoreCancellation = true }
}

// WithError returns one fixed content-free dependency failure after the
// controlled boundary is released.
func WithError(err error) option {
	return func(executor *Executor) { executor.resultErr = err }
}

type Executor struct {
	release            chan struct{}
	started            chan struct{}
	contextDone        chan struct{}
	ignoreCancellation bool
	resultErr          error
	startOnce          sync.Once
	contextDoneOnce    sync.Once
	releaseOnce        sync.Once
	calls              atomic.Int64
	cancellations      atomic.Int64
}

func New(options ...option) *Executor {
	executor := &Executor{started: make(chan struct{}), contextDone: make(chan struct{})}
	for _, apply := range options {
		apply(executor)
	}
	return executor
}

func (e *Executor) Execute(ctx context.Context, request worker.ToolExecutionRequest) (json.RawMessage, error) {
	if e == nil || ctx == nil || ctx.Err() != nil || !request.Policy.WebSearch.Enabled || request.InvocationID == "" || request.IdempotencyKey != request.InvocationID {
		return nil, errors.New("fake search request is invalid")
	}
	query, err := decodeQuery(request.Arguments)
	if err != nil {
		return nil, err
	}
	e.calls.Add(1)
	e.startOnce.Do(func() { close(e.started) })
	if e.release != nil {
		if e.ignoreCancellation {
			select {
			case <-e.release:
			case <-ctx.Done():
				e.contextDoneOnce.Do(func() { close(e.contextDone) })
				<-e.release
			}
		} else {
			select {
			case <-e.release:
			case <-ctx.Done():
				e.contextDoneOnce.Do(func() { close(e.contextDone) })
				e.cancellations.Add(1)
				return nil, ctx.Err()
			}
		}
	}
	if e.resultErr != nil {
		return nil, e.resultErr
	}
	result := struct {
		Query   string `json:"query"`
		Results []struct {
			Title   string `json:"title"`
			Snippet string `json:"snippet"`
			URL     string `json:"url"`
		} `json:"results"`
	}{Query: query}
	result.Results = append(result.Results, struct {
		Title   string `json:"title"`
		Snippet string `json:"snippet"`
		URL     string `json:"url"`
	}{Title: "The Go Programming Language", Snippet: "Build simple, secure, scalable systems.", URL: "https://go.dev/"})
	raw, err := json.Marshal(result)
	if err != nil {
		return nil, errors.New("fake search result is unavailable")
	}
	return raw, nil
}

func (e *Executor) Project(raw json.RawMessage) (json.RawMessage, error) {
	var private struct {
		Query   string `json:"query"`
		Results []struct {
			Title   string `json:"title"`
			Snippet string `json:"snippet"`
			URL     string `json:"url"`
		} `json:"results"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&private); err != nil || private.Query == "" || len(private.Results) != 1 {
		return nil, errors.New("fake search result is invalid")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("fake search result is invalid")
	}
	result := private.Results[0]
	if result.Title == "" || result.Snippet == "" || result.URL != "https://go.dev/" {
		return nil, errors.New("fake search result is invalid")
	}
	return json.Marshal(struct {
		Sources []struct {
			Title string `json:"title"`
			URL   string `json:"url"`
		} `json:"sources"`
	}{Sources: []struct {
		Title string `json:"title"`
		URL   string `json:"url"`
	}{{Title: result.Title, URL: result.URL}}})
}

func (e *Executor) Started() <-chan struct{} { return e.started }

func (e *Executor) ContextDone() <-chan struct{} { return e.contextDone }

func (e *Executor) Release() {
	if e != nil && e.release != nil {
		e.releaseOnce.Do(func() { close(e.release) })
	}
}

func (e *Executor) Calls() int { return int(e.calls.Load()) }

func (e *Executor) Cancellations() int { return int(e.cancellations.Load()) }

func RegisteredTool(executor *Executor) worker.RegisteredTool {
	return worker.RegisteredTool{
		Definition: reasoner.ToolDefinition{
			Name: toolpolicy.WebSearchTool, Description: "Search the public web for current information.",
			Parameters: append(json.RawMessage(nil), argumentSchema...),
		},
		ReadOnly: true, ParallelSafe: true, StableIdempotency: true,
		StartedPhase: worker.ProgressPhaseSearching, CompletedPhase: worker.ProgressPhaseReadingSources,
		Executor: executor, Projector: executor,
	}
}

func decodeQuery(raw json.RawMessage) (string, error) {
	var arguments struct {
		Query string `json:"query"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if len(raw) == 0 || len(raw) > 4<<10 || decoder.Decode(&arguments) != nil || arguments.Query == "" || len(arguments.Query) > 512 {
		return "", errors.New("fake search arguments are invalid")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return "", errors.New("fake search arguments are invalid")
	}
	return arguments.Query, nil
}

var _ worker.ToolExecutor = (*Executor)(nil)
var _ worker.ToolResultProjector = (*Executor)(nil)
