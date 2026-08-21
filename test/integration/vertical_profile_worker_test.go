//go:build integration

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/adaptercatalog"
	knowledgeadapter "github.com/chuanmingliu/openai-gpt-live/internal/adapters/knowledge"
	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/knowledge"
	"github.com/chuanmingliu/openai-gpt-live/internal/profile"
	"github.com/chuanmingliu/openai-gpt-live/internal/prompttmpl"
	"github.com/chuanmingliu/openai-gpt-live/internal/worker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInsuranceRecipeUsesPinnedKnowledgeAndFailureDoesNotEndVoice(t *testing.T) {
	version2 := verticalKnowledgeVersion(t, 2, "The renewal grace period is thirty days.")
	version3 := verticalKnowledgeVersion(t, 3, "The renewal grace period is forty days.")
	store := &verticalKnowledgeStore{versions: map[knowledge.VersionRef]knowledge.Version{version2.Ref: version2, version3.Ref: version3}}
	executor, err := knowledgeadapter.New(knowledge.NewSearcher(store))
	require.NoError(t, err)
	manifest := knowledgeadapter.Manifest("registry-v1")
	catalog, err := adaptercatalog.New("registry-v1", []adaptercatalog.Manifest{manifest}, nil)
	require.NoError(t, err)
	registry, err := adaptercatalog.CompileRegistry(catalog, map[string]adaptercatalog.Binding{
		knowledgeadapter.AdapterID: {Enabled: true},
	}, []adaptercatalog.Installed{knowledgeadapter.Installed(executor)})
	require.NoError(t, err)

	resolved, snapshot := verticalRecipeFixture(t, version2.Ref)
	profileResolver := &verticalProfileResolver{resolved: resolved}
	recipes, err := worker.NewRecipeResolver(profileResolver, prompttmpl.NewMiniJinja(), registry)
	require.NoError(t, err)
	rendered, err := recipes.ResolveAndRender(t.Context(), snapshot, worker.RecipeEvidence{TenantID: "tenant-a", Values: snapshot.Evidence})
	require.NoError(t, err)
	assert.Contains(t, rendered.Objective, "policy")
	assert.Equal(t, []string{knowledgeadapter.ToolName}, rendered.ToolAllowlist)
	assert.Equal(t, profile.ResolveAcceptedWork, profileResolver.lastPurpose)

	tool, ok := registry.Lookup("registry-v1", knowledgeadapter.ToolName)
	require.True(t, ok)
	result, err := tool.Executor.Execute(t.Context(), worker.ToolExecutionRequest{
		TenantID: "tenant-a", Knowledge: snapshot.Knowledge,
		Arguments: json.RawMessage(`{"query":"renewal grace period","max_results":3}`),
	})
	require.NoError(t, err)
	var cited struct {
		Results []knowledge.Result `json:"results"`
	}
	require.NoError(t, json.Unmarshal(result, &cited))
	require.Len(t, cited.Results, 1)
	assert.Equal(t, version2.Ref, cited.Results[0].Ref, "newer published material must not widen accepted work")
	assert.Equal(t, version2.Excerpts[0].Citation.ExcerptHash, cited.Results[0].Citation.ExcerptHash)

	voiceSessionAlive := true
	delete(store.versions, version2.Ref)
	_, err = tool.Executor.Execute(t.Context(), worker.ToolExecutionRequest{
		TenantID: "tenant-a", Knowledge: snapshot.Knowledge,
		Arguments: json.RawMessage(`{"query":"renewal"}`),
	})
	assert.ErrorIs(t, err, knowledge.ErrKnowledgeNotFound)
	assert.True(t, voiceSessionAlive, "worker material failure must remain task-local")
}

type verticalProfileResolver struct {
	resolved    profile.Resolved
	lastPurpose profile.ResolvePurpose
}

func (resolver *verticalProfileResolver) Resolve(ctx context.Context, tenant string, ref profile.VersionRef, purpose profile.ResolvePurpose) (profile.Resolved, error) {
	resolver.lastPurpose = purpose
	if ctx.Err() != nil {
		return profile.Resolved{}, ctx.Err()
	}
	if tenant != "tenant-a" || ref != resolver.resolved.Snapshot.Ref || purpose != profile.ResolveAcceptedWork {
		return profile.Resolved{}, profile.ErrProfileIncompatible
	}
	return resolver.resolved.Clone(), nil
}

func verticalRecipeFixture(t *testing.T, knowledgeRef knowledge.VersionRef) (profile.Resolved, handoff.ProfileSnapshot) {
	t.Helper()
	const systemSource = "Fixed insurance system context."
	const objectiveSource = "Follow up on {{ topic }} using only cited policy material."
	const templateName = "workers/policy_followup.j2"
	systemRef := verticalTemplateRef(systemSource)
	objectiveRef := verticalTemplateRef(objectiveSource)
	schema := prompttmpl.ContextSchema{Fields: map[string]prompttmpl.Field{
		"topic": {Type: prompttmpl.TypeString, Required: true, MaxBytes: 64, Templates: []string{templateName}, Source: "handoff"},
	}}
	recipe := profile.WorkerRecipe{ID: "policy_followup", Version: 1, Objective: objectiveRef, BindingIDs: []string{knowledgeadapter.AdapterID}, RequiredEvidence: []string{"topic"}, MaxObjectiveBytes: 4096, MaxEvidenceBytes: 4096, MaxToolResultBytes: 8192, MaxFinalResultBytes: 8192, ExecutionTimeout: time.Second, ToolTimeout: time.Second, OverallTimeout: 2 * time.Second, ResultSchema: json.RawMessage(`{"type":"object"}`)}
	binding := profile.AdapterBinding{AdapterID: knowledgeadapter.AdapterID, Version: knowledgeadapter.AdapterVersion, ToolIDs: []string{knowledgeadapter.ToolName}, MaxResultBytes: 8192}
	definition := profile.PromptBundle{Templates: map[string]profile.TemplateRef{"system.j2": systemRef, templateName: objectiveRef}, Schema: schema}
	bundleHash, err := definition.Hash()
	require.NoError(t, err)
	profileSnapshot := profile.Snapshot{TenantID: "tenant-a", Ref: profile.VersionRef{ID: "insurance", Version: 1}, EngineVersion: prompttmpl.NewMiniJinja().Version(), BundleHash: bundleHash, System: systemRef, Recipes: []profile.WorkerRecipe{recipe}, Bindings: []profile.AdapterBinding{binding}, Knowledge: []profile.KnowledgeRef{{ID: knowledgeRef.PackID, Version: knowledgeRef.Version, Hash: knowledgeRef.Hash}}, AdapterRegistryVersion: "registry-v1"}
	snapshotHash, err := profileSnapshot.Hash()
	require.NoError(t, err)
	profileSnapshot.SnapshotHash = snapshotHash
	resolved := profile.Resolved{Snapshot: profileSnapshot, Definition: definition, Bundle: prompttmpl.Bundle{Templates: map[string]string{"system.j2": systemSource, templateName: objectiveSource}, Schema: schema}}
	require.NoError(t, resolved.Validate())
	recipeHash, err := recipe.Hash()
	require.NoError(t, err)
	return resolved, handoff.ProfileSnapshot{ProfileID: "insurance", ProfileVersion: 1, SnapshotHash: snapshotHash, EngineVersion: profileSnapshot.EngineVersion, RecipeID: recipe.ID, RecipeVersion: recipe.Version, RecipeHash: recipeHash, RegistryVersion: "registry-v1", Knowledge: []handoff.KnowledgeVersion{{ID: knowledgeRef.PackID, Version: knowledgeRef.Version, Hash: knowledgeRef.Hash}}, Evidence: json.RawMessage(`{"topic":"policy"}`)}
}

func verticalTemplateRef(source string) profile.TemplateRef {
	digest := sha256.Sum256([]byte(source))
	return profile.TemplateRef{Hash: hex.EncodeToString(digest[:]), MediaType: "text/plain; charset=utf-8", Bytes: len(source)}
}

func verticalKnowledgeVersion(t *testing.T, version uint64, text string) knowledge.Version {
	t.Helper()
	excerpt := knowledge.Excerpt{Ordinal: 0, Text: text, Citation: knowledge.Citation{SourceTitle: "insurance-policy.md", Section: "Renewal"}}
	excerpt.Citation.ExcerptHash = knowledge.ExcerptHash(text)
	value := knowledge.Version{TenantID: "tenant-a", Ref: knowledge.VersionRef{PackID: "policy", Version: version}, Title: "Insurance policy", MediaType: "text/plain", Excerpts: []knowledge.Excerpt{excerpt}, PublishedAt: time.Unix(1, 0).UTC(), PublishedBy: "operator"}
	value.Ref.Hash = knowledge.ContentHash(value)
	require.NoError(t, value.Validate())
	return value
}

type verticalKnowledgeStore struct {
	versions map[knowledge.VersionRef]knowledge.Version
}

func (*verticalKnowledgeStore) CreateDraft(context.Context, knowledge.Draft) (knowledge.Draft, error) {
	return knowledge.Draft{}, errors.New("unexpected mutation")
}
func (*verticalKnowledgeStore) Publish(context.Context, knowledge.PublishRecord) (knowledge.Version, error) {
	return knowledge.Version{}, errors.New("unexpected mutation")
}
func (store *verticalKnowledgeStore) Resolve(_ context.Context, tenant string, ref knowledge.VersionRef) (knowledge.Version, bool, error) {
	value, ok := store.versions[ref]
	if tenant != "tenant-a" {
		return knowledge.Version{}, false, nil
	}
	return value.Clone(), ok, nil
}
func (*verticalKnowledgeStore) List(context.Context, string, string, int) ([]knowledge.Version, error) {
	return nil, nil
}
func (*verticalKnowledgeStore) Tombstone(context.Context, string, knowledge.VersionRef, string, time.Time) error {
	return errors.New("unexpected mutation")
}
