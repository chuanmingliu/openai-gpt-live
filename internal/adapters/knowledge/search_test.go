package knowledgeadapter

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/knowledge"
	"github.com/chuanmingliu/openai-gpt-live/internal/worker"
)

func TestExecutorSearchesOnlyPinnedKnowledgeAndReturnsCitations(t *testing.T) {
	version := knowledge.Version{TenantID: "tenant-a", Ref: knowledge.VersionRef{PackID: "policy", Version: 2}, Title: "Policy", MediaType: "text/plain", Excerpts: []knowledge.Excerpt{{Ordinal: 0, Text: "Renewal coverage includes a grace period.", Citation: knowledge.Citation{SourceTitle: "policy.md", Section: "Renewal"}}}, PublishedAt: time.Now().UTC(), PublishedBy: "operator"}
	version.Excerpts[0].Citation.ExcerptHash = knowledge.ExcerptHash(version.Excerpts[0].Text)
	version.Ref.Hash = knowledge.ContentHash(version)
	store := &knowledgeStoreFake{versions: map[knowledge.VersionRef]knowledge.Version{version.Ref: version}}
	executor, err := New(knowledge.NewSearcher(store))
	if err != nil {
		t.Fatal(err)
	}
	request := worker.ToolExecutionRequest{TenantID: "tenant-a", Knowledge: []handoff.KnowledgeVersion{{ID: "policy", Version: 2, Hash: version.Ref.Hash}}, Arguments: json.RawMessage(`{"query":"renewal coverage","max_results":3}`)}
	result, err := executor.Execute(t.Context(), request)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if store.lastRef != version.Ref {
		t.Fatalf("search widened or changed pinned ref: %#v", store.lastRef)
	}
	var decoded struct {
		Results []knowledge.Result `json:"results"`
	}
	if err := json.Unmarshal(result, &decoded); err != nil || len(decoded.Results) != 1 || decoded.Results[0].Citation.SourceTitle != "policy.md" || decoded.Results[0].Ref.Hash != version.Ref.Hash {
		t.Fatalf("unexpected cited result: %s %v", result, err)
	}
}

func TestExecutorFailsClosedWhenPinnedKnowledgeIsUnavailable(t *testing.T) {
	executor, err := New(knowledge.NewSearcher(&knowledgeStoreFake{}))
	if err != nil {
		t.Fatal(err)
	}
	_, err = executor.Execute(t.Context(), worker.ToolExecutionRequest{TenantID: "tenant-a", Knowledge: []handoff.KnowledgeVersion{{ID: "missing", Version: 1, Hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}, Arguments: json.RawMessage(`{"query":"coverage"}`)})
	if !errors.Is(err, knowledge.ErrKnowledgeNotFound) {
		t.Fatalf("got %v, want knowledge not found", err)
	}
}

func TestExecutorRejectsTypedNilAndModelSuppliedAuthority(t *testing.T) {
	var typedNil *knowledge.Searcher
	if _, err := New(typedNil); err == nil {
		t.Fatal("typed-nil search dependency was accepted")
	}

	executor, err := New(knowledge.NewSearcher(&knowledgeStoreFake{}))
	if err != nil {
		t.Fatal(err)
	}
	_, err = executor.Execute(t.Context(), worker.ToolExecutionRequest{
		TenantID:  "tenant-a",
		Knowledge: []handoff.KnowledgeVersion{{ID: "policy", Version: 1, Hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}},
		Arguments: json.RawMessage(`{"query":"coverage","tenant":"other","pack":"secret"}`),
	})
	if !errors.Is(err, knowledge.ErrInvalidKnowledge) {
		t.Fatalf("got %v, want invalid knowledge", err)
	}
}

type knowledgeStoreFake struct {
	versions map[knowledge.VersionRef]knowledge.Version
	lastRef  knowledge.VersionRef
}

func (*knowledgeStoreFake) CreateDraft(context.Context, knowledge.Draft) (knowledge.Draft, error) {
	return knowledge.Draft{}, errors.New("unexpected mutation")
}
func (*knowledgeStoreFake) Publish(context.Context, knowledge.PublishRecord) (knowledge.Version, error) {
	return knowledge.Version{}, errors.New("unexpected mutation")
}
func (store *knowledgeStoreFake) Resolve(_ context.Context, tenant string, ref knowledge.VersionRef) (knowledge.Version, bool, error) {
	store.lastRef = ref
	version, ok := store.versions[ref]
	if tenant != "tenant-a" {
		return knowledge.Version{}, false, nil
	}
	return version.Clone(), ok, nil
}
func (*knowledgeStoreFake) List(context.Context, string, string, int) ([]knowledge.Version, error) {
	return nil, nil
}
func (*knowledgeStoreFake) Tombstone(context.Context, string, knowledge.VersionRef, string, time.Time) error {
	return errors.New("unexpected mutation")
}
