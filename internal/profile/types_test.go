package profile

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/chuanmingliu/openai-gpt-live/internal/prompttmpl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManifestHashIgnoresMapInsertionOrder(t *testing.T) {
	a, b := orderedManifest(false), orderedManifest(true)
	assert.Equal(t, mustHash(t, a), mustHash(t, b))
}

func TestManifestHashPreservesResultSchemaNumberText(t *testing.T) {
	a, b := orderedManifest(false), orderedManifest(false)
	a.Recipes[0].ResultSchema = json.RawMessage(`{"number":1e2}`)
	b.Recipes[0].ResultSchema = json.RawMessage(`{"number":100}`)
	assert.NotEqual(t, mustHash(t, a), mustHash(t, b))
}

func TestManifestRejectsInvalidUTF8ResultSchemaBeforeHashing(t *testing.T) {
	for _, raw := range invalidUTF8Schemas() {
		manifest := validManifest(map[string]TemplateRef{"system.j2": {Hash: strings.Repeat("a", 64), MediaType: "text/plain", Bytes: 4}})
		manifest.Recipes = []WorkerRecipe{{ID: "research", Version: 1, Objective: manifest.Bundle.Templates["system.j2"], ResultSchema: raw}}
		hash, err := manifest.Hash()
		require.ErrorIs(t, err, ErrInvalidProfile)
		require.Empty(t, hash)
	}
}

func TestSnapshotRejectsInvalidUTF8ResultSchemaBeforeHashing(t *testing.T) {
	manifest := validManifest(map[string]TemplateRef{"system.j2": {Hash: strings.Repeat("a", 64), MediaType: "text/plain", Bytes: 4}})
	manifest.Recipes = []WorkerRecipe{{ID: "research", Version: 1, Objective: manifest.Bundle.Templates["system.j2"], ResultSchema: json.RawMessage(`{"x":"valid"}`)}}
	for _, raw := range invalidUTF8Schemas() {
		snapshot := snapshotFor(t, manifest)
		snapshot.Recipes[0].ResultSchema = raw
		hash, err := snapshot.Hash()
		require.ErrorIs(t, err, ErrInvalidProfile)
		require.Empty(t, hash)
	}
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
	bundleHash := mustBundleHash(t, manifest.Bundle)
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
	bundleHash := mustBundleHash(t, manifest.Bundle)
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
	const want = "a7df07d8b2ca3c58e8e0da247b72340dd29e91387bbfe7655aae623b0196f71a"
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
	bundleHash := mustBundleHash(t, manifest.Bundle)
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
	bundleHash := mustBundleHash(t, manifest.Bundle)
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

func mustBundleHash(t *testing.T, bundle PromptBundle) string {
	t.Helper()
	hash, err := bundle.Hash()
	require.NoError(t, err)
	return hash
}

func orderedManifest(reverse bool) Manifest {
	first, second := "system.j2", "fragments/rules.j2"
	if reverse {
		first, second = second, first
	}
	templates := make(map[string]TemplateRef, 2)
	fixtures := make(map[string]TemplateRef, 2)
	fields := make(map[string]prompttmpl.Field, 2)
	for _, name := range []string{first, second} {
		if name == "system.j2" {
			templates[name] = TemplateRef{Hash: strings.Repeat("a", 64), MediaType: "text/plain", Bytes: 4}
			fixtures["alpha.json"] = TemplateRef{Hash: strings.Repeat("c", 64), MediaType: "application/json", Bytes: 2}
			fields["customer"] = prompttmpl.Field{Type: prompttmpl.TypeString, Templates: []string{"system.j2"}}
		} else {
			templates[name] = TemplateRef{Hash: strings.Repeat("b", 64), MediaType: "text/plain", Bytes: 5}
			fixtures["beta.json"] = TemplateRef{Hash: strings.Repeat("d", 64), MediaType: "application/json", Bytes: 2}
			fields["region"] = prompttmpl.Field{Type: prompttmpl.TypeString, Templates: []string{"fragments/rules.j2"}}
		}
	}
	resultSchema := json.RawMessage(`{"a":1e2,"b":{"x":true,"y":null}}`)
	if reverse {
		resultSchema = json.RawMessage(`{"b":{"y":null,"x":true},"a":1e2}`)
	}
	return Manifest{
		Bundle:   PromptBundle{Templates: templates, Fixtures: fixtures, Schema: prompttmpl.ContextSchema{Fields: fields}},
		Bindings: []AdapterBinding{{AdapterID: "lookup", Version: "v1", ToolIDs: []string{"search"}}},
		Recipes:  []WorkerRecipe{{ID: "research", Version: 1, Objective: templates["fragments/rules.j2"], BindingIDs: []string{"lookup"}, ResultSchema: resultSchema}},
	}
}

func TestManifestRejectsUnboundedUnsafeAndDuplicateComponents(t *testing.T) {
	base := validManifest(map[string]TemplateRef{"system.j2": {Hash: strings.Repeat("a", 64), MediaType: "text/plain", Bytes: 4}})
	tests := []Manifest{
		func() Manifest {
			m := base.Clone()
			m.Bundle.Templates["system.j2"] = TemplateRef{Hash: strings.Repeat("a", 64), MediaType: "text/plain", Bytes: prompttmpl.MaxTemplateBytes + 1}
			return m
		}(),
		func() Manifest {
			m := base.Clone()
			m.Bundle.Templates[strings.Repeat("a", 254)+".j2"] = m.Bundle.Templates["system.j2"]
			return m
		}(),
		func() Manifest {
			m := base.Clone()
			m.Bundle.Fixtures = make(map[string]TemplateRef, 65)
			for i := range 65 {
				m.Bundle.Fixtures[fmt.Sprintf("fixture-%d.json", i)] = TemplateRef{Hash: strings.Repeat("b", 64), MediaType: "application/json", Bytes: 1}
			}
			return m
		}(),
		func() Manifest {
			m := base.Clone()
			m.Bindings[0].ToolIDs = []string{strings.Repeat("x", 257)}
			return m
		}(),
		func() Manifest {
			m := base.Clone()
			m.Recipes = []WorkerRecipe{{ID: "recipe", Version: 1, Objective: m.Bundle.Templates["system.j2"], ResultSchema: json.RawMessage("\"" + strings.Repeat("x", 65537) + "\"")}}
			return m
		}(),
		func() Manifest {
			m := base.Clone()
			m.Knowledge = []KnowledgeRef{{ID: "pack", Version: 1, Hash: strings.Repeat("a", 64)}, {ID: "pack", Version: 2, Hash: strings.Repeat("b", 64)}}
			return m
		}(),
		func() Manifest { m := base.Clone(); m.Description = "bad\ntext"; return m }(),
	}
	for _, manifest := range tests {
		require.ErrorIs(t, manifest.Validate(), ErrInvalidProfile)
	}
}

func TestManifestRequiresCompleteObjectiveReference(t *testing.T) {
	manifest := validManifest(map[string]TemplateRef{"system.j2": {Hash: strings.Repeat("a", 64), MediaType: "text/plain", Bytes: 4}})
	manifest.Recipes = []WorkerRecipe{{ID: "research", Version: 1, Objective: TemplateRef{Hash: strings.Repeat("a", 64), MediaType: "text/plain", Bytes: 5}}}
	require.ErrorIs(t, manifest.Validate(), ErrInvalidProfile)
}

func TestManifestSnapshotConsistencyPinsBundleAndTemplateRefs(t *testing.T) {
	manifest := validManifest(map[string]TemplateRef{
		"system.j2":          {Hash: strings.Repeat("a", 64), MediaType: "text/plain", Bytes: 4},
		"opening_message.j2": {Hash: strings.Repeat("b", 64), MediaType: "text/plain", Bytes: 5},
	})
	snapshot := snapshotFor(t, manifest)
	require.NoError(t, manifest.ValidateSnapshot(snapshot))
	snapshot.BundleHash = strings.Repeat("c", 64)
	require.ErrorIs(t, manifest.ValidateSnapshot(snapshot), ErrInvalidProfile)
	snapshot = snapshotFor(t, manifest)
	snapshot.Opening.Bytes++
	require.ErrorIs(t, manifest.ValidateSnapshot(snapshot), ErrInvalidProfile)
}

func TestSnapshotRejectsUnsafeTenantAndVersions(t *testing.T) {
	manifest := validManifest(map[string]TemplateRef{"system.j2": {Hash: strings.Repeat("a", 64), MediaType: "text/plain", Bytes: 4}})
	snapshot := snapshotFor(t, manifest)
	snapshot.TenantID = "tenant\n"
	_, err := snapshot.Hash()
	require.ErrorIs(t, err, ErrInvalidProfile)
	snapshot = snapshotFor(t, manifest)
	snapshot.EngineVersion = "\xff"
	_, err = snapshot.Hash()
	require.ErrorIs(t, err, ErrInvalidProfile)
	require.ErrorIs(t, (Draft{TenantID: "tenant", ID: "insurance", UpdatedBy: "operator\n", Manifest: manifest}).Validate(), ErrInvalidProfile)
	require.ErrorIs(t, (PublishRecord{TenantID: "tenant", ProfileID: "insurance", Actor: "operator\n", EngineVersion: "jinja-go-v1/test", Manifest: manifest, Snapshot: snapshotFor(t, manifest)}).Validate(), ErrInvalidProfile)
}

func snapshotFor(t *testing.T, manifest Manifest) Snapshot {
	t.Helper()
	snapshot := Snapshot{TenantID: "tenant-a", Ref: VersionRef{ID: "insurance", Version: 1}, EngineVersion: "jinja-go-v1/test", BundleHash: mustBundleHash(t, manifest.Bundle), System: manifest.Bundle.Templates["system.j2"], Opening: manifest.Bundle.Templates["opening_message.j2"], Turn: manifest.Bundle.Templates["turn_instruction.j2"], Recipes: cloneRecipes(manifest.Recipes), Bindings: cloneBindings(manifest.Bindings), Knowledge: append([]KnowledgeRef(nil), manifest.Knowledge...), AdapterRegistryVersion: "registry-v1"}
	hash, err := snapshot.Hash()
	require.NoError(t, err)
	snapshot.SnapshotHash = hash
	return snapshot
}

func invalidUTF8Schemas() []json.RawMessage {
	return []json.RawMessage{
		[]byte{'{', '"', 'x', '"', ':', '"', 0xff, '"', '}'},
		[]byte{'{', '"', 'x', '"', ':', '"', 0xfe, '"', '}'},
	}
}
