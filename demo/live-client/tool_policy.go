package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strconv"
	"sync"

	"github.com/chuanmingliu/openai-gpt-live/internal/toolpolicy"
	"github.com/chuanmingliu/openai-gpt-live/internal/tools/websearch"
)

const (
	maximumToolPolicyBytes = 4 << 10
	diagnosticQuery        = "DuckDuckGo privacy search"
)

type toolSearcher interface {
	Search(context.Context, websearch.Request) ([]websearch.Result, error)
}

type toolPolicyStore struct {
	mu       sync.RWMutex
	policy   toolpolicy.Policy
	catalog  toolpolicy.Catalog
	searcher toolSearcher
}

func newToolPolicyStore(searcher toolSearcher) (*toolPolicyStore, error) {
	if searcher == nil {
		return nil, errors.New("demo tool search is unavailable")
	}
	catalog := toolpolicy.DevelopmentDurableCatalog()
	policy, err := toolpolicy.DefaultPolicyForCatalog(catalog)
	if err != nil {
		return nil, errors.New("demo tool policy is unavailable")
	}
	return &toolPolicyStore{policy: policy, catalog: catalog, searcher: searcher}, nil
}

func (handler *demoHandler) serveToolPolicy(writer http.ResponseWriter, request *http.Request) {
	switch request.Method {
	case http.MethodGet:
		policy := handler.tools.snapshot()
		writer.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(writer).Encode(policy)
	case http.MethodPut:
		if status := handler.toolPolicyMutationStatus(request); status != 0 {
			writeToolPolicyError(writer, status)
			return
		}
		body, err := readToolPolicyBody(request)
		if err != nil {
			writeToolPolicyError(writer, http.StatusBadRequest)
			return
		}
		policy, err := toolpolicy.Decode(body)
		if err != nil {
			writeToolPolicyError(writer, http.StatusBadRequest)
			return
		}
		if err := handler.tools.replace(policy); err != nil {
			writeToolPolicyError(writer, http.StatusBadRequest)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	default:
		writer.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (handler *demoHandler) serveToolPolicyTest(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if status := handler.toolPolicyMutationStatus(request); status != 0 {
		writeToolPolicyError(writer, status)
		return
	}
	results, err := handler.tools.search(request.Context())
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	if err != nil {
		writer.WriteHeader(http.StatusServiceUnavailable)
		_, _ = writer.Write([]byte(`{"ok":false}`))
		return
	}
	_, _ = writer.Write([]byte(`{"ok":true,"result_count":` + strconv.Itoa(len(results)) + `}`))
}

func (handler *demoHandler) toolPolicyMutationStatus(request *http.Request) int {
	if request.Header.Get("Origin") != handler.cfg.Origin {
		return http.StatusBadRequest
	}
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return http.StatusUnsupportedMediaType
	}
	return 0
}

func readToolPolicyBody(request *http.Request) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(request.Body, maximumToolPolicyBytes+1))
	if err != nil || len(body) > maximumToolPolicyBytes {
		return nil, errors.New("tool policy request is invalid")
	}
	return body, nil
}

func (store *toolPolicyStore) snapshot() toolpolicy.Policy {
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.policy
}

func (store *toolPolicyStore) replace(policy toolpolicy.Policy) error {
	canonical, err := toolpolicy.Canonicalize(policy, store.catalog)
	if err != nil {
		return err
	}
	store.mu.Lock()
	store.policy = canonical
	store.mu.Unlock()
	return nil
}

func (store *toolPolicyStore) search(ctx context.Context) ([]websearch.Result, error) {
	policy := store.snapshot()
	return store.searcher.Search(ctx, websearch.Request{Query: diagnosticQuery, Policy: policy.WebSearch})
}

func writeToolPolicyError(writer http.ResponseWriter, status int) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(status)
	_, _ = writer.Write([]byte(`{"error":"tool policy request is invalid"}`))
}
