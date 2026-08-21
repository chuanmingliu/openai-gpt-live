//go:build integration

package integration_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/adaptercatalog"
	knowledgeadapter "github.com/chuanmingliu/openai-gpt-live/internal/adapters/knowledge"
	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/knowledge"
	"github.com/chuanmingliu/openai-gpt-live/internal/profile"
	"github.com/chuanmingliu/openai-gpt-live/internal/prompttmpl"
	"github.com/chuanmingliu/openai-gpt-live/internal/store/postgres"
	"github.com/chuanmingliu/openai-gpt-live/internal/testkit"
	"github.com/chuanmingliu/openai-gpt-live/migrations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInsuranceProfileSetupFlow is the durable release boundary behind the
// browser workspace: reviewed source and knowledge become immutable v1/v2
// snapshots, reconnect stays pinned, and revocation closes future admission.
func TestInsuranceProfileSetupFlow(t *testing.T) {
	ctx := t.Context()
	pool := testkit.Postgres(t)
	require.NoError(t, migrations.Up(ctx, pool))
	now := clock.NewManual(time.Date(2026, 8, 15, 8, 0, 0, 0, time.UTC))
	repository, err := postgres.NewRepository(pool, now)
	require.NoError(t, err)

	material := publishInsuranceKnowledge(t, repository.KnowledgeStore(), now.Now())
	catalog, err := adaptercatalog.New("insurance-registry-v1", []adaptercatalog.Manifest{
		knowledgeadapter.Manifest("insurance-registry-v1"),
	}, nil)
	require.NoError(t, err)
	engine := prompttmpl.NewMiniJinja()
	publisher := profile.NewPublisher(repository, engine, catalog, knowledge.NewReferenceValidator(repository.KnowledgeStore()), profile.WithPublisherClock(now))
	resolver := profile.NewResolver(repository, engine)

	draft := saveInsuranceDraft(t, repository, now.Now(), 0, "Welcome to insurance sales.", material.Ref)
	preview, err := publisher.Preview(ctx, profile.PublishCommand{TenantID: "development", ProfileID: "insurance-sales", ExpectedGeneration: draft.Generation, Actor: "operator"})
	require.NoError(t, err)
	assert.Equal(t, "Welcome to insurance sales.", preview.Outputs["opening.json"])

	v1, err := publisher.Publish(ctx, profile.PublishCommand{TenantID: "development", ProfileID: "insurance-sales", ExpectedGeneration: draft.Generation, Actor: "operator", Activate: true})
	require.NoError(t, err)
	openedV1, err := resolver.Resolve(ctx, "development", v1.Ref, profile.ResolveNewSession)
	require.NoError(t, err)
	assert.Equal(t, v1.Snapshot.SnapshotHash, openedV1.Snapshot.SnapshotHash)

	now.Advance(time.Second)
	draft = saveInsuranceDraft(t, repository, now.Now(), draft.Generation, "Welcome to the renewed insurance experience.", material.Ref)
	v2, err := publisher.Publish(ctx, profile.PublishCommand{TenantID: "development", ProfileID: "insurance-sales", ExpectedGeneration: draft.Generation, Actor: "operator", Activate: true})
	require.NoError(t, err)
	require.NotEqual(t, v1.Snapshot.SnapshotHash, v2.Snapshot.SnapshotHash)

	reconnectedV1, err := resolver.Resolve(ctx, "development", v1.Ref, profile.ResolveReconnect)
	require.NoError(t, err)
	assert.Equal(t, v1.Snapshot.SnapshotHash, reconnectedV1.Snapshot.SnapshotHash)
	openedV2, err := resolver.Resolve(ctx, "development", v2.Ref, profile.ResolveNewSession)
	require.NoError(t, err)
	assert.Equal(t, v2.Snapshot.SnapshotHash, openedV2.Snapshot.SnapshotHash)
	_, err = resolver.Resolve(ctx, "development", v1.Ref, profile.ResolveNewSession)
	assert.ErrorIs(t, err, profile.ErrProfileIncompatible)

	now.Advance(time.Second)
	require.NoError(t, publisher.Revoke(ctx, "development", v1.Ref, "operator"))
	_, err = resolver.Resolve(ctx, "development", v1.Ref, profile.ResolveReconnect)
	assert.ErrorIs(t, err, profile.ErrProfileRevoked)
}

func publishInsuranceKnowledge(t *testing.T, store knowledge.Store, at time.Time) knowledge.Version {
	t.Helper()
	document := knowledge.Document{TenantID: "development", PackID: "policy", Filename: "policy.md", Content: []byte("# Renewals\nThe renewal grace period is thirty days."), Actor: "operator", At: at}
	preview, err := knowledge.NewIngestor().Preview(t.Context(), document)
	require.NoError(t, err)
	draft, err := store.CreateDraft(t.Context(), knowledge.Draft{TenantID: document.TenantID, PackID: document.PackID, Title: preview.Title, MediaType: preview.MediaType, Excerpts: preview.Excerpts, Hash: preview.Hash, CreatedAt: at, CreatedBy: document.Actor})
	require.NoError(t, err)
	version, err := store.Publish(t.Context(), knowledge.PublishRecord{TenantID: draft.TenantID, PackID: draft.PackID, Title: draft.Title, MediaType: draft.MediaType, Excerpts: draft.Excerpts, Hash: draft.Hash, PublishedAt: at, PublishedBy: document.Actor})
	require.NoError(t, err)
	return version
}

func saveInsuranceDraft(t *testing.T, store profile.Store, at time.Time, expected int64, opening string, material knowledge.VersionRef) profile.Draft {
	t.Helper()
	system := setupBlob(t, "text/plain; charset=utf-8", "You are a licensed insurance sales assistant. Use only reviewed policy material.")
	openingBlob := setupBlob(t, "text/plain; charset=utf-8", opening)
	objective := setupBlob(t, "text/plain; charset=utf-8", "Research the customer's policy question using cited material.")
	fixtureBytes, err := json.Marshal(map[string]any{"template": "opening_message.j2", "context": map[string]any{}, "expected": opening})
	require.NoError(t, err)
	fixture := setupBlob(t, "application/json", string(fixtureBytes))
	for _, blob := range []profile.Blob{system, openingBlob, objective, fixture} {
		require.NoError(t, store.PutBlob(t.Context(), "development", blob))
	}
	recipe := profile.WorkerRecipe{
		ID: "policy_followup", Version: 1, Objective: setupRef(objective), BindingIDs: []string{knowledgeadapter.AdapterID},
		MaxObjectiveBytes: 4096, MaxEvidenceBytes: 4096, MaxToolResultBytes: 8192, MaxFinalResultBytes: 8192,
		ExecutionTimeout: 5 * time.Second, ToolTimeout: 2 * time.Second, OverallTimeout: 10 * time.Second,
		ResultSchema: json.RawMessage(`{"type":"object"}`),
	}
	manifest := profile.Manifest{Description: "Insurance sales", Bundle: profile.PromptBundle{
		Templates: map[string]profile.TemplateRef{"system.j2": setupRef(system), "opening_message.j2": setupRef(openingBlob), "workers/policy_followup.j2": setupRef(objective)},
		Schema:    prompttmpl.ContextSchema{Fields: map[string]prompttmpl.Field{}}, Fixtures: map[string]profile.TemplateRef{"opening.json": setupRef(fixture)},
	}, Recipes: []profile.WorkerRecipe{recipe}, Bindings: []profile.AdapterBinding{{
		AdapterID: knowledgeadapter.AdapterID, Version: knowledgeadapter.AdapterVersion, ToolIDs: []string{knowledgeadapter.ToolName}, MaxResultBytes: 8192,
	}}, Knowledge: []profile.KnowledgeRef{{ID: material.PackID, Version: material.Version, Hash: material.Hash}}}
	draft, err := store.SaveDraft(t.Context(), profile.Draft{TenantID: "development", ID: "insurance-sales", Generation: expected, Manifest: manifest, UpdatedAt: at, UpdatedBy: "operator"}, expected)
	require.NoError(t, err)
	return draft
}

func setupBlob(t *testing.T, mediaType, content string) profile.Blob {
	t.Helper()
	blob, err := (profile.Blob{MediaType: mediaType, Content: []byte(content)}).Canonical()
	require.NoError(t, err)
	return blob
}

func setupRef(blob profile.Blob) profile.TemplateRef {
	return profile.TemplateRef{Hash: blob.Hash, MediaType: blob.MediaType, Bytes: blob.ContentBytes}
}
