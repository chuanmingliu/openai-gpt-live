package profile

import (
	"strings"
	"testing"

	"github.com/chuanmingliu/openai-gpt-live/internal/prompttmpl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManifestHashIgnoresMapInsertionOrder(t *testing.T) {
	a := validManifest(map[string]TemplateRef{"system.j2": {Hash: strings.Repeat("a", 64), MediaType: "text/plain", Bytes: 4}})
	b := validManifest(map[string]TemplateRef{"system.j2": {Hash: strings.Repeat("a", 64), MediaType: "text/plain", Bytes: 4}})
	assert.Equal(t, mustHash(t, a), mustHash(t, b))
}

func TestManifestRejectsInvalidReferencesAndDuplicateBindings(t *testing.T) {
	manifest := validManifest(map[string]TemplateRef{"system.j2": {Hash: strings.Repeat("a", 64), MediaType: "text/plain", Bytes: 4}})
	manifest.Bundle.Templates["../secret.j2"] = TemplateRef{Hash: strings.Repeat("b", 64), MediaType: "text/plain", Bytes: 1}
	require.ErrorIs(t, manifest.Validate(), ErrInvalidProfile)

	manifest = validManifest(map[string]TemplateRef{"system.j2": {Hash: strings.Repeat("a", 64), MediaType: "text/plain", Bytes: 4}})
	manifest.Bindings = []AdapterBinding{{AdapterID: "lookup", Version: "v1"}, {AdapterID: "lookup", Version: "v2"}}
	require.ErrorIs(t, manifest.Validate(), ErrInvalidProfile)

	manifest = validManifest(map[string]TemplateRef{"system.j2": {Hash: strings.Repeat("A", 64), MediaType: "text/plain", Bytes: 4}})
	require.ErrorIs(t, manifest.Validate(), ErrInvalidProfile)

	manifest = validManifest(map[string]TemplateRef{"system.j2": {Hash: strings.Repeat("a", 64), MediaType: "text/plain", Bytes: 4}})
	manifest.Bundle.Schema.Fields = map[string]prompttmpl.Field{"customer": {Type: prompttmpl.TypeString, Templates: []string{"unknown.j2"}}}
	require.ErrorIs(t, manifest.Validate(), ErrInvalidProfile)
}

func TestValidateTemplateNameRejectsFilesystemPaths(t *testing.T) {
	require.NoError(t, ValidateTemplateName("fragments/rules.j2"))
	require.ErrorIs(t, ValidateTemplateName("../secret.j2"), ErrInvalidProfile)
}

func TestSnapshotAcceptsPinnedWorkerObjectiveOutsideSystemTemplate(t *testing.T) {
	manifest := validManifest(map[string]TemplateRef{"system.j2": {Hash: strings.Repeat("a", 64), MediaType: "text/plain", Bytes: 4}})
	bundleHash := mustHash(t, manifest)
	snapshot := Snapshot{
		TenantID:               "tenant-a",
		Ref:                    VersionRef{ID: "insurance", Version: 1},
		EngineVersion:          "jinja-go-v1/test",
		BundleHash:             bundleHash,
		System:                 manifest.Bundle.Templates["system.j2"],
		Recipes:                []WorkerRecipe{{ID: "research", Version: 1, Objective: TemplateRef{Hash: strings.Repeat("b", 64), MediaType: "text/plain", Bytes: 4}, BindingIDs: []string{"lookup"}}},
		Bindings:               manifest.Bindings,
		AdapterRegistryVersion: "registry-v1",
	}
	hash, err := snapshot.Hash()
	require.NoError(t, err)
	snapshot.SnapshotHash = hash
	require.NoError(t, snapshot.Validate())
}

func TestManifestHashHasExactCanonicalValue(t *testing.T) {
	manifest := validManifest(map[string]TemplateRef{"system.j2": {Hash: strings.Repeat("a", 64), MediaType: "text/plain", Bytes: 4}})
	got := mustHash(t, manifest)
	const want = "0f8df7f1260d4bc6324f28aafb6239a2303952bdd4a52bea7bc9cb6ad79235ec"
	require.Equal(t, want, got)
}

func TestBlobCanonicalizesHashAndOwnsContent(t *testing.T) {
	input := Blob{MediaType: "text/plain", Content: []byte("hello")}
	got, err := input.Canonical()
	require.NoError(t, err)
	input.Content[0] = 'x'
	require.Equal(t, []byte("hello"), got.Content)
	require.Equal(t, 5, got.ContentBytes)
	const want = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
	require.Equal(t, want, got.Hash)
}

func TestSnapshotHashHasExactCanonicalValue(t *testing.T) {
	manifest := validManifest(map[string]TemplateRef{"system.j2": {Hash: strings.Repeat("a", 64), MediaType: "text/plain", Bytes: 4}})
	bundleHash := mustHash(t, manifest)
	snapshot := Snapshot{
		TenantID:               "tenant-a",
		Ref:                    VersionRef{ID: "insurance", Version: 1},
		EngineVersion:          "jinja-go-v1/test",
		BundleHash:             bundleHash,
		System:                 manifest.Bundle.Templates["system.j2"],
		AdapterRegistryVersion: "registry-v1",
	}
	got, err := snapshot.Hash()
	require.NoError(t, err)
	const want = "f9d2302516ed8dd4eae7237e3051c4571576a4a63e0863d6255dd03e1d9d59ea"
	require.Equal(t, want, got)
}

func TestManifestCloneIsOwned(t *testing.T) {
	manifest := validManifest(map[string]TemplateRef{"system.j2": {Hash: strings.Repeat("a", 64), MediaType: "text/plain", Bytes: 4}})
	manifest.Bundle.Schema.Fields = map[string]prompttmpl.Field{}
	manifest.Bundle.Schema.Fields["customer"] = prompttmpl.Field{Type: prompttmpl.TypeString, Templates: []string{"system.j2"}}
	clone := manifest.Clone()
	manifest.Bundle.Templates["system.j2"] = TemplateRef{Hash: strings.Repeat("b", 64), MediaType: "text/plain", Bytes: 4}
	field := manifest.Bundle.Schema.Fields["customer"]
	field.Templates[0] = "turn_instruction.j2"
	manifest.Bundle.Schema.Fields["customer"] = field

	require.Equal(t, strings.Repeat("a", 64), clone.Bundle.Templates["system.j2"].Hash)
	require.Equal(t, "system.j2", clone.Bundle.Schema.Fields["customer"].Templates[0])
}

func TestIdentityBoundsAndSnapshotValidation(t *testing.T) {
	require.ErrorIs(t, ID(strings.Repeat("x", 257)).Validate(), ErrInvalidProfile)
	require.ErrorIs(t, (VersionRef{ID: "insurance", Version: 0}).Validate(), ErrInvalidProfile)
	overlongRecipe := validManifest(map[string]TemplateRef{"system.j2": {Hash: strings.Repeat("a", 64), MediaType: "text/plain", Bytes: 4}})
	overlongRecipe.Recipes = []WorkerRecipe{{ID: strings.Repeat("x", 257), Version: 1, Objective: overlongRecipe.Bundle.Templates["system.j2"]}}
	require.ErrorIs(t, overlongRecipe.Validate(), ErrInvalidProfile)

	manifest := validManifest(map[string]TemplateRef{"system.j2": {Hash: strings.Repeat("a", 64), MediaType: "text/plain", Bytes: 4}})
	bundleHash := mustHash(t, manifest)
	snapshot := Snapshot{
		TenantID:               "tenant-a",
		Ref:                    VersionRef{ID: "insurance", Version: 1},
		EngineVersion:          "jinja-go-v1/test",
		BundleHash:             bundleHash,
		System:                 manifest.Bundle.Templates["system.j2"],
		AdapterRegistryVersion: "registry-v1",
	}
	hash, err := snapshot.Hash()
	require.NoError(t, err)
	snapshot.SnapshotHash = hash
	require.NoError(t, snapshot.Validate())
	snapshot.System.Hash = strings.Repeat("b", 64)
	require.ErrorIs(t, snapshot.Validate(), ErrInvalidProfile)
}

func TestSnapshotRejectsPartialOptionalTemplateReference(t *testing.T) {
	manifest := validManifest(map[string]TemplateRef{"system.j2": {Hash: strings.Repeat("a", 64), MediaType: "text/plain", Bytes: 4}})
	bundleHash := mustHash(t, manifest)
	snapshot := Snapshot{
		TenantID:               "tenant-a",
		Ref:                    VersionRef{ID: "insurance", Version: 1},
		EngineVersion:          "jinja-go-v1/test",
		BundleHash:             bundleHash,
		System:                 manifest.Bundle.Templates["system.j2"],
		Opening:                TemplateRef{Bytes: -1},
		AdapterRegistryVersion: "registry-v1",
	}
	_, err := snapshot.Hash()
	require.ErrorIs(t, err, ErrInvalidProfile)
}

func validManifest(templates map[string]TemplateRef) Manifest {
	return Manifest{
		Bundle:   PromptBundle{Templates: templates},
		Bindings: []AdapterBinding{{AdapterID: "lookup", Version: "v1", ToolIDs: []string{"search"}}},
	}
}

func mustHash(t *testing.T, manifest Manifest) string {
	t.Helper()
	hash, err := manifest.Hash()
	require.NoError(t, err)
	return hash
}
