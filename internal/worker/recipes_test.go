package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/profile"
	"github.com/chuanmingliu/openai-gpt-live/internal/prompttmpl"
	"github.com/chuanmingliu/openai-gpt-live/internal/reasoner"
)

func TestRunnerUsesRenderedRecipeAndNeverPromotesItToSystemAuthority(t *testing.T) {
	resolved, snapshot := recipeFixture(t, "Renew {{ topic }}")
	registry := recipeRegistry(t)
	recipes, err := NewRecipeResolver(profileResolverFunc(func(context.Context, string, profile.VersionRef, profile.ResolvePurpose) (profile.Resolved, error) {
		return resolved.Clone(), nil
	}), prompttmpl.NewMiniJinja(), registry)
	if err != nil {
		t.Fatal(err)
	}
	lease := workerLease()
	lease.Job.Request.Profile = snapshot
	lease.Job.Request.ToolAllowlist = []string{"knowledge_search"}
	lease.Job.Request.Versions.ToolRegistry = "registry-v1"
	ctx, cancel := context.WithCancel(t.Context())
	store := &memoryStore{leases: []Lease{lease}, onComplete: cancel}
	stream := streamerFunc(func(_ context.Context, request reasoner.Request, onChunk func(reasoner.Chunk) error) (reasoner.Result, error) {
		if request.Messages[0].Role != reasoner.RoleSystem || request.Messages[0].Content != workerSystemTemplate || strings.Contains(request.Messages[0].Content, "Renew policy") {
			t.Fatalf("rendered data crossed system boundary: %#v", request.Messages)
		}
		if !strings.Contains(request.Messages[1].Content, "Renew policy") {
			t.Fatalf("rendered objective missing from untrusted user message: %s", request.Messages[1].Content)
		}
		_ = onChunk(reasoner.Chunk{Text: "done"})
		return reasoner.Result{Text: "done", FinishReason: "stop"}, nil
	})
	runner := testRunner(store, stream, allowPolicy(), clock.Real{}, fixedJitter(0), registry)
	runner.Recipes = recipes
	runner.ArgumentSealer, err = NewAESArgumentSealer("test-key", bytes.Repeat([]byte{5}, 32), bytes.NewReader(bytes.Repeat([]byte{1}, 12)))
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(store.completed) != 1 || len(store.failed) != 0 {
		t.Fatalf("unexpected terminal state: completed=%d failed=%#v", len(store.completed), store.failed)
	}
}

func TestRecipeFailureIsPersistedAsTaskFailureBeforeReasonerCall(t *testing.T) {
	resolved, snapshot := recipeFixture(t, "Renew {{ topic }}")
	registry := recipeRegistry(t)
	recipes, err := NewRecipeResolver(profileResolverFunc(func(context.Context, string, profile.VersionRef, profile.ResolvePurpose) (profile.Resolved, error) {
		return resolved.Clone(), nil
	}), prompttmpl.NewMiniJinja(), registry)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.RecipeHash = strings.Repeat("f", 64)
	lease := workerLease()
	lease.Job.Request.Profile = snapshot
	lease.Job.Request.ToolAllowlist = []string{"knowledge_search"}
	lease.Job.Request.Versions.ToolRegistry = "registry-v1"
	ctx, cancel := context.WithCancel(t.Context())
	store := &memoryStore{leases: []Lease{lease}, onComplete: cancel}
	streamCalls := 0
	runner := testRunner(store, streamerFunc(func(context.Context, reasoner.Request, func(reasoner.Chunk) error) (reasoner.Result, error) {
		streamCalls++
		return reasoner.Result{}, nil
	}), allowPolicy(), clock.Real{}, fixedJitter(0), registry)
	runner.Recipes = recipes
	runner.ArgumentSealer, err = NewAESArgumentSealer("test-key", bytes.Repeat([]byte{5}, 32), bytes.NewReader(bytes.Repeat([]byte{1}, 12)))
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	if streamCalls != 0 || len(store.failed) != 1 || store.failed[0].Code != "profile_incompatible" {
		t.Fatalf("failure isolation mismatch: calls=%d failures=%#v", streamCalls, store.failed)
	}
}

func TestWorkerRendersExactPinnedRecipeAndOwnsOutput(t *testing.T) {
	resolved, snapshot := recipeFixture(t, "Renew {{ topic }}")
	registry := recipeRegistry(t)
	resolver, err := NewRecipeResolver(profileResolverFunc(func(_ context.Context, _ string, ref profile.VersionRef, purpose profile.ResolvePurpose) (profile.Resolved, error) {
		if ref != (profile.VersionRef{ID: "insurance", Version: 1}) || purpose != profile.ResolveAcceptedWork {
			t.Fatalf("unexpected resolve authority: %#v %q", ref, purpose)
		}
		return resolved.Clone(), nil
	}), prompttmpl.NewMiniJinja(), registry)
	if err != nil {
		t.Fatal(err)
	}
	got, err := resolver.ResolveAndRender(t.Context(), snapshot, RecipeEvidence{TenantID: "tenant-a", Values: json.RawMessage(`{"topic":"policy"}`)})
	if err != nil {
		t.Fatalf("resolve recipe: %v", err)
	}
	if got.Objective != "Renew policy" || got.System != workerSystemTemplate || got.RegistryVersion != "registry-v1" || len(got.ToolAllowlist) != 1 || got.ToolAllowlist[0] != "knowledge_search" {
		t.Fatalf("unexpected rendered recipe: %#v", got)
	}
	got.ToolAllowlist[0] = "mutated"
	again, err := resolver.ResolveAndRender(t.Context(), snapshot, RecipeEvidence{TenantID: "tenant-a", Values: json.RawMessage(`{"topic":"policy"}`)})
	if err != nil || again.ToolAllowlist[0] != "knowledge_search" {
		t.Fatalf("resolver retained caller mutation: %#v %v", again, err)
	}
}

func TestRecipeResolverRejectsHashMismatchAndAdapterExpansion(t *testing.T) {
	resolved, snapshot := recipeFixture(t, "Renew {{ topic }}")
	resolver, err := NewRecipeResolver(profileResolverFunc(func(context.Context, string, profile.VersionRef, profile.ResolvePurpose) (profile.Resolved, error) {
		return resolved.Clone(), nil
	}), prompttmpl.NewMiniJinja(), recipeRegistry(t))
	if err != nil {
		t.Fatal(err)
	}
	snapshot.RecipeHash = strings.Repeat("f", 64)
	if _, err := resolver.ResolveAndRender(t.Context(), snapshot, RecipeEvidence{TenantID: "tenant-a", Values: json.RawMessage(`{"topic":"policy"}`)}); !errors.Is(err, ErrProfileIncompatible) {
		t.Fatalf("hash mismatch: got %v", err)
	}
	snapshot.RecipeHash = mustRecipeHash(t, resolved.Snapshot.Recipes[0])
	resolved.Snapshot.Bindings[0].ToolIDs = append(resolved.Snapshot.Bindings[0].ToolIDs, "unreviewed")
	if _, err := resolver.ResolveAndRender(t.Context(), snapshot, RecipeEvidence{TenantID: "tenant-a", Values: json.RawMessage(`{"topic":"policy"}`)}); !errors.Is(err, ErrProfileIncompatible) {
		t.Fatalf("adapter expansion: got %v", err)
	}
}

func TestRecipeResolverClassifiesInvalidEvidence(t *testing.T) {
	resolved, snapshot := recipeFixture(t, "Renew {{ topic }}")
	newResolver := func(source profile.Resolved) RecipeResolver {
		resolver, err := NewRecipeResolver(profileResolverFunc(func(context.Context, string, profile.VersionRef, profile.ResolvePurpose) (profile.Resolved, error) {
			return source.Clone(), nil
		}), prompttmpl.NewMiniJinja(), recipeRegistry(t))
		if err != nil {
			t.Fatal(err)
		}
		return resolver
	}
	invalid := json.RawMessage(`{"other":"x"}`)
	snapshot.Evidence = invalid
	if _, err := newResolver(resolved).ResolveAndRender(t.Context(), snapshot, RecipeEvidence{TenantID: "tenant-a", Values: invalid}); !errors.Is(err, ErrRecipeContextInvalid) {
		t.Fatalf("context class: got %v", err)
	}
}

func TestRecipeResolverRejectsDuplicateEvidenceKeys(t *testing.T) {
	resolved, snapshot := recipeFixture(t, "Renew {{ topic }}")
	duplicate := json.RawMessage(`{"topic":"policy","topic":"override"}`)
	snapshot.Evidence = duplicate
	resolver, err := NewRecipeResolver(profileResolverFunc(func(context.Context, string, profile.VersionRef, profile.ResolvePurpose) (profile.Resolved, error) {
		return resolved.Clone(), nil
	}), prompttmpl.NewMiniJinja(), recipeRegistry(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.ResolveAndRender(t.Context(), snapshot, RecipeEvidence{TenantID: "tenant-a", Values: duplicate}); !errors.Is(err, ErrRecipeContextInvalid) {
		t.Fatalf("duplicate evidence: got %v", err)
	}
}

type profileResolverFunc func(context.Context, string, profile.VersionRef, profile.ResolvePurpose) (profile.Resolved, error)

func (function profileResolverFunc) Resolve(ctx context.Context, tenant string, ref profile.VersionRef, purpose profile.ResolvePurpose) (profile.Resolved, error) {
	return function(ctx, tenant, ref, purpose)
}

func recipeFixture(t *testing.T, objectiveSource string) (profile.Resolved, handoff.ProfileSnapshot) {
	t.Helper()
	systemSource := "Fixed profile system context."
	systemRef := refForRecipeSource(systemSource)
	objectiveRef := refForRecipeSource(objectiveSource)
	templateName := "workers/policy_followup.j2"
	schema := prompttmpl.ContextSchema{Fields: map[string]prompttmpl.Field{
		"topic": {Type: prompttmpl.TypeString, Required: true, MaxBytes: 64, Templates: []string{templateName}, Source: "handoff"},
	}}
	recipe := profile.WorkerRecipe{ID: "policy_followup", Version: 1, Objective: objectiveRef, BindingIDs: []string{"knowledge_search"}, RequiredEvidence: []string{"topic"}, MaxObjectiveBytes: 4096, MaxEvidenceBytes: 4096, MaxToolResultBytes: 8192, MaxFinalResultBytes: 8192, ExecutionTimeout: time.Second, ToolTimeout: time.Second, OverallTimeout: 2 * time.Second, ResultSchema: json.RawMessage(`{"type":"object"}`)}
	binding := profile.AdapterBinding{AdapterID: "knowledge_search", Version: "v1", ToolIDs: []string{"knowledge_search"}, MaxResultBytes: 8192}
	definition := profile.PromptBundle{Templates: map[string]profile.TemplateRef{"system.j2": systemRef, templateName: objectiveRef}, Schema: schema}
	bundleHash, err := definition.Hash()
	if err != nil {
		t.Fatal(err)
	}
	profileSnapshot := profile.Snapshot{TenantID: "tenant-a", Ref: profile.VersionRef{ID: "insurance", Version: 1}, EngineVersion: prompttmpl.NewMiniJinja().Version(), BundleHash: bundleHash, System: systemRef, Recipes: []profile.WorkerRecipe{recipe}, Bindings: []profile.AdapterBinding{binding}, AdapterRegistryVersion: "registry-v1"}
	hash, err := profileSnapshot.Hash()
	if err != nil {
		t.Fatal(err)
	}
	profileSnapshot.SnapshotHash = hash
	resolved := profile.Resolved{Snapshot: profileSnapshot, Definition: definition, Bundle: prompttmpl.Bundle{Templates: map[string]string{"system.j2": systemSource, templateName: objectiveSource}, Schema: schema}}
	if err := resolved.Validate(); err != nil {
		t.Fatalf("invalid recipe fixture: %v", err)
	}
	return resolved, handoff.ProfileSnapshot{ProfileID: "insurance", ProfileVersion: 1, SnapshotHash: hash, EngineVersion: profileSnapshot.EngineVersion, RecipeID: recipe.ID, RecipeVersion: recipe.Version, RecipeHash: mustRecipeHash(t, recipe), RegistryVersion: "registry-v1", Evidence: json.RawMessage(`{"topic":"policy"}`)}
}

func refForRecipeSource(source string) profile.TemplateRef {
	digest := sha256.Sum256([]byte(source))
	return profile.TemplateRef{Hash: hex.EncodeToString(digest[:]), MediaType: "text/plain; charset=utf-8", Bytes: len(source)}
}

func mustRecipeHash(t *testing.T, recipe profile.WorkerRecipe) string {
	t.Helper()
	hash, err := recipe.Hash()
	if err != nil {
		t.Fatal(err)
	}
	return hash
}

func recipeRegistry(t *testing.T) ToolRegistry {
	t.Helper()
	registry, err := NewToolRegistry(map[string][]RegisteredTool{"registry-v1": {{
		AdapterID: "knowledge_search", AdapterVersion: "v1", RegistryVersion: "registry-v1",
		DestinationID: "knowledge", TransportClass: "native", Approval: "none",
		Definition: reasoner.ToolDefinition{Name: "knowledge_search", Parameters: json.RawMessage(`{"type":"object"}`)},
		ReadOnly:   true, ParallelSafe: true, StableIdempotency: true, Timeout: time.Second,
		MaxConcurrency: 2, MaxArgumentBytes: 4096, MaxResultBytes: 8192,
		Executor: toolExecutorFuncForRegistry(func(context.Context, ToolExecutionRequest) (json.RawMessage, error) {
			return json.RawMessage(`{}`), nil
		}),
	}}})
	if err != nil {
		t.Fatal(err)
	}
	return registry
}
