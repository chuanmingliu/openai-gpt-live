package websearch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"unicode/utf8"

	"github.com/chuanmingliu/openai-gpt-live/internal/reasoner"
	"github.com/chuanmingliu/openai-gpt-live/internal/toolpolicy"
	"github.com/chuanmingliu/openai-gpt-live/internal/worker"
)

const (
	maximumArgumentBytes = 4 << 10
	maximumOutputBytes   = 128 << 10
)

var argumentSchema = json.RawMessage(`{"type":"object","additionalProperties":false,"required":["query"],"properties":{"query":{"type":"string","minLength":1,"maxLength":512}}}`)

type output struct {
	Query   string   `json:"query"`
	Results []Result `json:"results"`
}

type projection struct {
	Sources []source `json:"sources"`
}

type source struct {
	Title string `json:"title"`
	URL   string `json:"url"`
}

// Execute implements worker.ToolExecutor using only the durable server-owned
// policy attached by the worker. Arguments cannot alter execution policy.
func (e *Executor) Execute(ctx context.Context, request worker.ToolExecutionRequest) (json.RawMessage, error) {
	query, err := decodeArguments(request.Arguments)
	if err != nil {
		return nil, err
	}
	results, err := e.Search(ctx, Request{Query: query, Policy: request.Policy.WebSearch})
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(output{Query: query, Results: results})
	if err != nil || len(raw) > maximumOutputBytes {
		return nil, errors.New("web search output exceeds bounds")
	}
	return json.RawMessage(raw), nil
}

// Project reparses private executor output and retains only bounded sources.
func (e *Executor) Project(raw json.RawMessage) (json.RawMessage, error) {
	if e == nil {
		return nil, errors.New("web search projector is unavailable")
	}
	private, err := decodeOutput(raw)
	if err != nil {
		return nil, err
	}
	public := projection{Sources: make([]source, 0, len(private.Results))}
	for _, result := range private.Results {
		public.Sources = append(public.Sources, source{Title: result.Title, URL: result.URL})
	}
	projected, err := json.Marshal(public)
	if err != nil {
		return nil, errors.New("web search projection failed")
	}
	return json.RawMessage(projected), nil
}

// RegisteredTool returns immutable read-only execution metadata.
func RegisteredTool(executor *Executor) worker.RegisteredTool {
	return worker.RegisteredTool{
		Definition: reasoner.ToolDefinition{
			Name:        toolpolicy.WebSearchTool,
			Description: "Search the public web for current information.",
			Parameters:  append(json.RawMessage(nil), argumentSchema...),
		},
		SideEffecting:     false,
		ReadOnly:          true,
		ParallelSafe:      true,
		StableIdempotency: true,
		StartedPhase:      worker.ProgressPhaseSearching,
		CompletedPhase:    worker.ProgressPhaseReadingSources,
		Executor:          executor,
		Projector:         executor,
	}
}

func decodeArguments(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || len(raw) > maximumArgumentBytes || !utf8.Valid(raw) {
		return "", errors.New("web search arguments are invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return "", errors.New("web search arguments are invalid")
	}
	query := ""
	seen := false
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil || key != "query" || seen {
			return "", errors.New("web search arguments are invalid")
		}
		if err := decoder.Decode(&query); err != nil {
			return "", errors.New("web search arguments are invalid")
		}
		seen = true
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') || !seen {
		return "", errors.New("web search arguments are invalid")
	}
	if _, err = decoder.Token(); !errors.Is(err, io.EOF) {
		return "", errors.New("web search arguments are invalid")
	}
	query, err = normalizeQuery(query)
	if err != nil {
		return "", errors.New("web search arguments are invalid")
	}
	return query, nil
}

func decodeOutput(raw json.RawMessage) (output, error) {
	if len(raw) == 0 || len(raw) > maximumOutputBytes || !utf8.Valid(raw) {
		return output{}, errors.New("web search private output is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return output{}, errors.New("web search private output is invalid")
	}
	var decoded output
	seenQuery, seenResults := false, false
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return output{}, errors.New("web search private output is invalid")
		}
		switch key {
		case "query":
			if seenQuery || decoder.Decode(&decoded.Query) != nil {
				return output{}, errors.New("web search private output is invalid")
			}
			seenQuery = true
		case "results":
			if seenResults {
				return output{}, errors.New("web search private output is invalid")
			}
			results, err := decodeResults(decoder)
			if err != nil {
				return output{}, err
			}
			decoded.Results = results
			seenResults = true
		default:
			return output{}, errors.New("web search private output is invalid")
		}
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') || !seenQuery || !seenResults {
		return output{}, errors.New("web search private output is invalid")
	}
	if _, err = decoder.Token(); !errors.Is(err, io.EOF) {
		return output{}, errors.New("web search private output is invalid")
	}
	query, err := normalizeQuery(decoded.Query)
	if err != nil || query != decoded.Query {
		return output{}, errors.New("web search private output is invalid")
	}
	return decoded, nil
}

func decodeResults(decoder *json.Decoder) ([]Result, error) {
	token, err := decoder.Token()
	if err != nil || token != json.Delim('[') {
		return nil, errors.New("web search private output is invalid")
	}
	results := make([]Result, 0)
	for decoder.More() {
		if len(results) == maximumResults {
			return nil, errors.New("web search private output is invalid")
		}
		result, err := decodeResult(decoder)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim(']') {
		return nil, errors.New("web search private output is invalid")
	}
	return results, nil
}

func decodeResult(decoder *json.Decoder) (Result, error) {
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return Result{}, errors.New("web search private output is invalid")
	}
	var result Result
	seen := map[string]bool{}
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return Result{}, errors.New("web search private output is invalid")
		}
		name, ok := key.(string)
		if !ok || seen[name] {
			return Result{}, errors.New("web search private output is invalid")
		}
		seen[name] = true
		switch name {
		case "title":
			err = decoder.Decode(&result.Title)
		case "snippet":
			err = decoder.Decode(&result.Snippet)
		case "url":
			err = decoder.Decode(&result.URL)
		default:
			return Result{}, errors.New("web search private output is invalid")
		}
		if err != nil {
			return Result{}, errors.New("web search private output is invalid")
		}
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') || !seen["title"] || !seen["snippet"] || !seen["url"] {
		return Result{}, errors.New("web search private output is invalid")
	}
	validatedURL, ok := resultURL(result.URL)
	if !ok || validatedURL != result.URL || !validField(result.Title, 1, maximumTitleRunes) || !validField(result.Snippet, 1, maximumSnippetRunes) {
		return Result{}, errors.New("web search private output is invalid")
	}
	return result, nil
}
