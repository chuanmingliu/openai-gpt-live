// Package profile defines immutable, tenant-scoped vertical profile records.
package profile

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/chuanmingliu/openai-gpt-live/internal/prompttmpl"
)

const (
	maxIdentifierBytes  = 256
	maxDescriptionBytes = 64 * 1024
	maxRawJSONBytes     = 64 * 1024
	maxTemplates        = 64
	maxFixtures         = 64
	maxSchemaFields     = 64
	maxRecipes          = 32
	maxBindings         = 64
	maxKnowledgeRefs    = 32
	maxNestedItems      = 64
	maxProfileBlobBytes = prompttmpl.MaxBundleBytes
	maxTemplateRefBytes = prompttmpl.MaxTemplateBytes
	maxRecipeBytes      = prompttmpl.MaxBundleBytes
	maxRecipeDuration   = 24 * time.Hour
	profileHashVersion  = "vertical-profile-v1"
	snapshotHashVersion = "vertical-profile-snapshot-v1"
)

var (
	ErrInvalidProfile      = errors.New("profile_invalid")
	ErrStaleDraft          = errors.New("stale_draft")
	ErrPublicationConflict = errors.New("publication_conflict")
	ErrProfileIncompatible = errors.New("profile_incompatible")
	ErrProfileRevoked      = errors.New("profile_revoked")
)

// ID is a stable profile identity, scoped by the calling tenant.
type ID string

func (id ID) Validate() error {
	if !validIdentifier(string(id)) {
		return ErrInvalidProfile
	}
	return nil
}

// VersionRef identifies one immutable published profile version.
type VersionRef struct {
	ID      ID     `json:"id"`
	Version uint64 `json:"version"`
}

func (ref VersionRef) Validate() error {
	if err := ref.ID.Validate(); err != nil || ref.Version == 0 {
		return ErrInvalidProfile
	}
	return nil
}

// Blob is immutable tenant-scoped content. Content is always returned as an
// owned byte slice, and ContentBytes/Hash describe exactly those bytes.
type Blob struct {
	MediaType    string `json:"media_type"`
	Content      []byte `json:"content"`
	ContentBytes int    `json:"content_bytes"`
	Hash         string `json:"hash"`
}

func (blob Blob) Clone() Blob {
	blob.Content = append([]byte(nil), blob.Content...)
	return blob
}

func (blob Blob) Canonical() (Blob, error) {
	if !validShortText(blob.MediaType) || len(blob.Content) > maxProfileBlobBytes || blob.ContentBytes < 0 || blob.ContentBytes != 0 && blob.ContentBytes != len(blob.Content) {
		return Blob{}, ErrInvalidProfile
	}
	owned := blob.Clone()
	owned.ContentBytes = len(owned.Content)
	sum := sha256.Sum256(owned.Content)
	owned.Hash = hex.EncodeToString(sum[:])
	return owned, nil
}

// TemplateRef identifies immutable blob content referenced by a logical name.
type TemplateRef struct {
	Hash      string `json:"hash"`
	MediaType string `json:"media_type"`
	Bytes     int    `json:"bytes"`
}

func (ref TemplateRef) Validate() error {
	if !validHash(ref.Hash) || !validShortText(ref.MediaType) || ref.Bytes < 0 || ref.Bytes > maxTemplateRefBytes {
		return ErrInvalidProfile
	}
	return nil
}

// PromptBundle contains only logical, blob-backed template references and a
// typed context schema. Source is loaded from the referenced blobs by a
// resolver; no filesystem path is represented here.
type PromptBundle struct {
	Templates map[string]TemplateRef   `json:"templates"`
	Schema    prompttmpl.ContextSchema `json:"schema"`
	Fixtures  map[string]TemplateRef   `json:"fixtures,omitempty"`
}

func (bundle PromptBundle) Clone() PromptBundle {
	clone := PromptBundle{
		Templates: make(map[string]TemplateRef, len(bundle.Templates)),
		Schema:    cloneSchema(bundle.Schema),
		Fixtures:  make(map[string]TemplateRef, len(bundle.Fixtures)),
	}
	for name, ref := range bundle.Templates {
		clone.Templates[name] = ref
	}
	for name, ref := range bundle.Fixtures {
		clone.Fixtures[name] = ref
	}
	return clone
}

func (bundle PromptBundle) Validate() error {
	if len(bundle.Templates) == 0 || len(bundle.Templates) > maxTemplates || len(bundle.Fixtures) > maxFixtures || len(bundle.Schema.Fields) > maxSchemaFields || !validRefMap(bundle.Templates) || !validRefMap(bundle.Fixtures) {
		return ErrInvalidProfile
	}
	for name, ref := range bundle.Templates {
		if ValidateTemplateName(name) != nil || ref.Validate() != nil {
			return ErrInvalidProfile
		}
	}
	if _, ok := bundle.Templates["system.j2"]; !ok {
		return ErrInvalidProfile
	}
	for name, ref := range bundle.Fixtures {
		if !validFixtureName(name) || ref.Validate() != nil {
			return ErrInvalidProfile
		}
	}
	for name, field := range bundle.Schema.Fields {
		if !validShortText(name) || !validField(field) {
			return ErrInvalidProfile
		}
		for _, template := range field.Templates {
			if _, ok := bundle.Templates[template]; !ok {
				return ErrInvalidProfile
			}
		}
	}
	return nil
}

func (bundle PromptBundle) Hash() (string, error) {
	if err := bundle.Validate(); err != nil {
		return "", err
	}
	return hashCanonical(profileHashVersion+"/bundle", canonicalBundle(bundle)), nil
}

// WorkerRecipe describes a bounded asynchronous objective and its reviewed
// binding allowance. Objective must identify a template present in the bundle.
type WorkerRecipe struct {
	ID                  string          `json:"id"`
	Version             uint64          `json:"version"`
	Objective           TemplateRef     `json:"objective"`
	BindingIDs          []string        `json:"binding_ids,omitempty"`
	RequiredEvidence    []string        `json:"required_evidence,omitempty"`
	OptionalEvidence    []string        `json:"optional_evidence,omitempty"`
	MaxObjectiveBytes   int             `json:"max_objective_bytes"`
	MaxEvidenceBytes    int             `json:"max_evidence_bytes"`
	MaxToolResultBytes  int             `json:"max_tool_result_bytes"`
	MaxFinalResultBytes int             `json:"max_final_result_bytes"`
	ExecutionTimeout    time.Duration   `json:"execution_timeout"`
	ToolTimeout         time.Duration   `json:"tool_timeout"`
	OverallTimeout      time.Duration   `json:"overall_timeout"`
	RequireApproval     bool            `json:"require_approval"`
	ResultSchema        json.RawMessage `json:"result_schema,omitempty"`
}

func (recipe WorkerRecipe) Clone() WorkerRecipe {
	recipe.BindingIDs = append([]string(nil), recipe.BindingIDs...)
	recipe.RequiredEvidence = append([]string(nil), recipe.RequiredEvidence...)
	recipe.OptionalEvidence = append([]string(nil), recipe.OptionalEvidence...)
	recipe.ResultSchema = append(json.RawMessage(nil), recipe.ResultSchema...)
	return recipe
}

// AdapterBinding narrows one server-owned adapter catalog entry.
type AdapterBinding struct {
	AdapterID       string   `json:"adapter_id"`
	Version         string   `json:"version"`
	ToolIDs         []string `json:"tool_ids,omitempty"`
	MaxResultBytes  int      `json:"max_result_bytes"`
	RequireApproval bool     `json:"require_approval"`
}

func (binding AdapterBinding) Clone() AdapterBinding {
	binding.ToolIDs = append([]string(nil), binding.ToolIDs...)
	return binding
}

// KnowledgeRef pins one immutable knowledge pack version.
type KnowledgeRef struct {
	ID      string `json:"id"`
	Version uint64 `json:"version"`
	Hash    string `json:"hash"`
}

// Manifest is the canonical, immutable profile definition.
type Manifest struct {
	Description string           `json:"description,omitempty"`
	Bundle      PromptBundle     `json:"bundle"`
	Recipes     []WorkerRecipe   `json:"recipes,omitempty"`
	Bindings    []AdapterBinding `json:"bindings,omitempty"`
	Knowledge   []KnowledgeRef   `json:"knowledge,omitempty"`
}

func (manifest Manifest) Clone() Manifest {
	clone := Manifest{
		Description: manifest.Description,
		Bundle:      manifest.Bundle.Clone(),
		Recipes:     make([]WorkerRecipe, len(manifest.Recipes)),
		Bindings:    make([]AdapterBinding, len(manifest.Bindings)),
		Knowledge:   append([]KnowledgeRef(nil), manifest.Knowledge...),
	}
	for index, recipe := range manifest.Recipes {
		clone.Recipes[index] = recipe.Clone()
	}
	for index, binding := range manifest.Bindings {
		clone.Bindings[index] = binding.Clone()
	}
	return clone
}

func (manifest Manifest) Validate() error {
	if !validProfileText(manifest.Description) || manifest.Bundle.Validate() != nil {
		return ErrInvalidProfile
	}
	return validateComponents(manifest.Recipes, manifest.Bindings, manifest.Knowledge, manifest.Bundle.Templates)
}

func (manifest Manifest) Hash() (string, error) {
	if err := manifest.Validate(); err != nil {
		return "", err
	}
	return hashCanonical(profileHashVersion, canonicalManifest(manifest)), nil
}

// ValidateSnapshot verifies that a snapshot pins this exact manifest's prompt
// bundle, optional prompt refs, and component records.
func (manifest Manifest) ValidateSnapshot(snapshot Snapshot) error {
	if manifest.Validate() != nil || snapshot.Validate() != nil {
		return ErrInvalidProfile
	}
	bundleHash, err := manifest.Bundle.Hash()
	if err != nil || snapshot.BundleHash != bundleHash || snapshot.System != manifest.Bundle.Templates["system.j2"] || snapshot.Opening != manifest.Bundle.Templates["opening_message.j2"] || snapshot.Turn != manifest.Bundle.Templates["turn_instruction.j2"] {
		return ErrInvalidProfile
	}
	canonicalizedManifest := canonicalManifest(manifest)
	canonicalizedSnapshot := canonicalSnapshot(snapshot)
	if !reflect.DeepEqual(canonicalizedManifest.Recipes, canonicalizedSnapshot.Recipes) || !reflect.DeepEqual(canonicalizedManifest.Bindings, canonicalizedSnapshot.Bindings) || !reflect.DeepEqual(canonicalizedManifest.Knowledge, canonicalizedSnapshot.Knowledge) {
		return ErrInvalidProfile
	}
	return nil
}

// Draft is the only mutable persistence record for a profile. Stores return an
// owned copy after every mutation.
type Draft struct {
	TenantID   string    `json:"tenant_id"`
	ID         ID        `json:"id"`
	Generation int64     `json:"generation"`
	Manifest   Manifest  `json:"manifest"`
	UpdatedAt  time.Time `json:"updated_at"`
	UpdatedBy  string    `json:"updated_by"`
}

func (draft Draft) Clone() Draft {
	draft.Manifest = draft.Manifest.Clone()
	return draft
}

func (draft Draft) Validate() error {
	if !validIdentifier(draft.TenantID) || draft.Generation < 0 || draft.ID.Validate() != nil || !validIdentifier(draft.UpdatedBy) || draft.Manifest.Validate() != nil {
		return ErrInvalidProfile
	}
	return nil
}

// Snapshot pins every component a session or worker is allowed to use.
type Snapshot struct {
	TenantID               string           `json:"tenant_id"`
	Ref                    VersionRef       `json:"ref"`
	EngineVersion          string           `json:"engine_version"`
	BundleHash             string           `json:"bundle_hash"`
	System                 TemplateRef      `json:"system"`
	Opening                TemplateRef      `json:"opening,omitempty"`
	Turn                   TemplateRef      `json:"turn,omitempty"`
	Recipes                []WorkerRecipe   `json:"recipes,omitempty"`
	Bindings               []AdapterBinding `json:"bindings,omitempty"`
	Knowledge              []KnowledgeRef   `json:"knowledge,omitempty"`
	AdapterRegistryVersion string           `json:"adapter_registry_version"`
	SnapshotHash           string           `json:"snapshot_hash"`
}

func (snapshot Snapshot) Clone() Snapshot {
	snapshot.Recipes = cloneRecipes(snapshot.Recipes)
	snapshot.Bindings = cloneBindings(snapshot.Bindings)
	snapshot.Knowledge = append([]KnowledgeRef(nil), snapshot.Knowledge...)
	return snapshot
}

func (snapshot Snapshot) Hash() (string, error) {
	if err := snapshot.validate(false); err != nil {
		return "", err
	}
	return hashCanonical(snapshotHashVersion, canonicalSnapshot(snapshot)), nil
}

func (snapshot Snapshot) Validate() error {
	if err := snapshot.validate(true); err != nil {
		return err
	}
	hash, err := snapshot.Hash()
	if err != nil || snapshot.SnapshotHash != hash {
		return ErrInvalidProfile
	}
	return nil
}

func (snapshot Snapshot) validate(requireHash bool) error {
	if !validIdentifier(snapshot.TenantID) || snapshot.Ref.Validate() != nil || !validShortText(snapshot.EngineVersion) || !validShortText(snapshot.AdapterRegistryVersion) || !validHash(snapshot.BundleHash) || snapshot.System.Validate() != nil || !validOptionalTemplateRef(snapshot.Opening) || !validOptionalTemplateRef(snapshot.Turn) || (requireHash && !validHash(snapshot.SnapshotHash)) {
		return ErrInvalidProfile
	}
	return validateComponents(snapshot.Recipes, snapshot.Bindings, snapshot.Knowledge, nil)
}

func validateComponents(recipes []WorkerRecipe, bindings []AdapterBinding, knowledgeRefs []KnowledgeRef, templates map[string]TemplateRef) error {
	if len(recipes) > maxRecipes || len(bindings) > maxBindings || len(knowledgeRefs) > maxKnowledgeRefs {
		return ErrInvalidProfile
	}
	bindingIDs := make(map[string]struct{}, len(bindings))
	for _, binding := range bindings {
		if !validBinding(binding) {
			return ErrInvalidProfile
		}
		if _, exists := bindingIDs[binding.AdapterID]; exists {
			return ErrInvalidProfile
		}
		bindingIDs[binding.AdapterID] = struct{}{}
	}
	recipeIDs := make(map[string]struct{}, len(recipes))
	for _, recipe := range recipes {
		if !validRecipe(recipe) || templates != nil && !hasTemplateRef(templates, recipe.Objective) {
			return ErrInvalidProfile
		}
		if _, exists := recipeIDs[recipe.ID]; exists {
			return ErrInvalidProfile
		}
		recipeIDs[recipe.ID] = struct{}{}
		for _, bindingID := range recipe.BindingIDs {
			if _, exists := bindingIDs[bindingID]; !exists {
				return ErrInvalidProfile
			}
		}
	}
	knowledgeIDs := make(map[string]struct{}, len(knowledgeRefs))
	for _, knowledge := range knowledgeRefs {
		if !validKnowledgeRef(knowledge) {
			return ErrInvalidProfile
		}
		if _, exists := knowledgeIDs[knowledge.ID]; exists {
			return ErrInvalidProfile
		}
		knowledgeIDs[knowledge.ID] = struct{}{}
	}
	return nil
}

// Published is an immutable profile version returned by the store.
type Published struct {
	TenantID        string     `json:"tenant_id"`
	Ref             VersionRef `json:"ref"`
	DraftGeneration int64      `json:"draft_generation"`
	EngineVersion   string     `json:"engine_version"`
	Snapshot        Snapshot   `json:"snapshot"`
	Manifest        Manifest   `json:"manifest"`
	Active          bool       `json:"active"`
	RevokedAt       *time.Time `json:"revoked_at,omitempty"`
	PublishedAt     time.Time  `json:"published_at"`
	PublishedBy     string     `json:"published_by"`
}

func (published Published) Clone() Published {
	published.Snapshot = published.Snapshot.Clone()
	published.Manifest = published.Manifest.Clone()
	if published.RevokedAt != nil {
		value := *published.RevokedAt
		published.RevokedAt = &value
	}
	return published
}

// Validate verifies that persisted published metadata and its snapshot remain
// mutually consistent and bounded.
func (published Published) Validate() error {
	if !validIdentifier(published.TenantID) || published.Ref.Validate() != nil || published.DraftGeneration < 0 || !validShortText(published.EngineVersion) || !validIdentifier(published.PublishedBy) || published.Snapshot.TenantID != published.TenantID || published.Snapshot.Ref != published.Ref || published.Snapshot.EngineVersion != published.EngineVersion || published.Manifest.ValidateSnapshot(published.Snapshot) != nil {
		return ErrInvalidProfile
	}
	return nil
}

// Resolved is the owned, exact prompt source bundle paired with its immutable
// snapshot. No caller needs to reopen or choose another blob.
type Resolved struct {
	Snapshot Snapshot          `json:"snapshot"`
	Bundle   prompttmpl.Bundle `json:"bundle"`
}

func (resolved Resolved) Clone() Resolved {
	resolved.Snapshot = resolved.Snapshot.Clone()
	resolved.Bundle = clonePromptBundle(resolved.Bundle)
	return resolved
}

func validHash(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for _, b := range []byte(value) {
		if !(b >= '0' && b <= '9' || b >= 'a' && b <= 'f') {
			return false
		}
	}
	return true
}

func validIdentifier(value string) bool {
	return validText(value, maxIdentifierBytes, false)
}

func validShortText(value string) bool { return validText(value, maxIdentifierBytes, false) }

func validProfileText(value string) bool { return validText(value, maxDescriptionBytes, true) }

func validText(value string, maximum int, allowEmpty bool) bool {
	if (!allowEmpty && value == "") || len(value) > maximum || !utf8.ValidString(value) {
		return false
	}
	for _, runeValue := range value {
		if unicode.IsControl(runeValue) {
			return false
		}
	}
	return true
}

func validOptionalTemplateRef(ref TemplateRef) bool {
	if ref.Hash == "" && ref.MediaType == "" && ref.Bytes == 0 {
		return true
	}
	return ref.Validate() == nil
}

func validLogicalTemplateName(name string) bool {
	if len(name) < len("a.j2") || len(name) > maxIdentifierBytes || !strings.HasSuffix(name, ".j2") || !isLowerAlphaNum(name[0]) {
		return false
	}
	for _, b := range []byte(name) {
		if !(b >= 'a' && b <= 'z' || b >= '0' && b <= '9' || b == '_' || b == '.' || b == '/' || b == '-') {
			return false
		}
	}
	for _, segment := range strings.Split(name, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return false
		}
	}
	return true
}

// ValidateTemplateName accepts the bounded logical paths used inside a prompt
// bundle. It intentionally excludes filesystem paths and traversal segments.
func ValidateTemplateName(name string) error {
	if !validLogicalTemplateName(name) {
		return ErrInvalidProfile
	}
	return nil
}

func isLowerAlphaNum(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= '0' && value <= '9'
}

func validFixtureName(name string) bool {
	if len(name) < len("a.json") || len(name) > maxIdentifierBytes || !strings.HasSuffix(name, ".json") || !isLowerAlphaNum(name[0]) {
		return false
	}
	for _, byteValue := range []byte(name) {
		if !(byteValue >= 'a' && byteValue <= 'z' || byteValue >= '0' && byteValue <= '9' || byteValue == '_' || byteValue == '.' || byteValue == '/' || byteValue == '-') {
			return false
		}
	}
	for _, segment := range strings.Split(name, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return false
		}
	}
	return true
}

func validField(field prompttmpl.Field) bool {
	if field.Type != prompttmpl.TypeString && field.Type != prompttmpl.TypeBool && field.Type != prompttmpl.TypeNumber && field.Type != prompttmpl.TypeObject && field.Type != prompttmpl.TypeArray || field.MaxBytes < 0 || field.MaxBytes > prompttmpl.MaxContextBytes || field.MaxItems < 0 || field.MaxItems > prompttmpl.MaxCollectionItems || !validText(field.Sensitivity, maxIdentifierBytes, true) || !validText(field.Source, maxIdentifierBytes, true) || len(field.Templates) > maxTemplates {
		return false
	}
	for _, template := range field.Templates {
		if !validLogicalTemplateName(template) {
			return false
		}
	}
	return uniqueStrings(field.Templates)
}

func uniqueStrings(values []string) bool {
	if len(values) > maxNestedItems {
		return false
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if !validShortText(value) {
			return false
		}
		if _, exists := seen[value]; exists {
			return false
		}
		seen[value] = struct{}{}
	}
	return true
}

func validRefMap(refs map[string]TemplateRef) bool {
	total := 0
	for _, ref := range refs {
		if ref.Validate() != nil || ref.Bytes > maxProfileBlobBytes-total {
			return false
		}
		total += ref.Bytes
	}
	return true
}

func hasTemplateRef(templates map[string]TemplateRef, want TemplateRef) bool {
	for _, ref := range templates {
		if ref == want {
			return true
		}
	}
	return false
}

func validBinding(binding AdapterBinding) bool {
	return validIdentifier(binding.AdapterID) && validIdentifier(binding.Version) && binding.MaxResultBytes >= 0 && binding.MaxResultBytes <= maxRecipeBytes && uniqueStrings(binding.ToolIDs)
}

func validRecipe(recipe WorkerRecipe) bool {
	return validIdentifier(recipe.ID) && recipe.Version != 0 && recipe.Objective.Validate() == nil && uniqueStrings(recipe.BindingIDs) && uniqueStrings(recipe.RequiredEvidence) && uniqueStrings(recipe.OptionalEvidence) && boundedRecipeBytes(recipe.MaxObjectiveBytes) && boundedRecipeBytes(recipe.MaxEvidenceBytes) && boundedRecipeBytes(recipe.MaxToolResultBytes) && boundedRecipeBytes(recipe.MaxFinalResultBytes) && recipe.ExecutionTimeout >= 0 && recipe.ExecutionTimeout <= maxRecipeDuration && recipe.ToolTimeout >= 0 && recipe.ToolTimeout <= maxRecipeDuration && recipe.OverallTimeout >= 0 && recipe.OverallTimeout <= maxRecipeDuration && validJSON(recipe.ResultSchema)
}

func boundedRecipeBytes(value int) bool { return value >= 0 && value <= maxRecipeBytes }

func validKnowledgeRef(reference KnowledgeRef) bool {
	return validIdentifier(reference.ID) && reference.Version != 0 && validHash(reference.Hash)
}

func validJSON(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return true
	}
	if len(raw) > maxRawJSONBytes || !json.Valid(raw) {
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return false
	}
	return decoder.Decode(&struct{}{}) == io.EOF
}

func canonicalJSON(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	return json.RawMessage(encoded)
}

func hashCanonical(version string, value any) string {
	encoded, _ := json.Marshal(struct {
		Version string `json:"version"`
		Value   any    `json:"value"`
	}{Version: version, Value: value})
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func canonicalBundle(bundle PromptBundle) PromptBundle {
	clone := bundle.Clone()
	for name, field := range clone.Schema.Fields {
		sort.Strings(field.Templates)
		clone.Schema.Fields[name] = field
	}
	return clone
}

func canonicalManifest(manifest Manifest) Manifest {
	clone := manifest.Clone()
	clone.Bundle = canonicalBundle(clone.Bundle)
	sort.Slice(clone.Recipes, func(i, j int) bool { return clone.Recipes[i].ID < clone.Recipes[j].ID })
	sort.Slice(clone.Bindings, func(i, j int) bool { return clone.Bindings[i].AdapterID < clone.Bindings[j].AdapterID })
	sort.Slice(clone.Knowledge, func(i, j int) bool {
		return clone.Knowledge[i].ID < clone.Knowledge[j].ID || clone.Knowledge[i].ID == clone.Knowledge[j].ID && clone.Knowledge[i].Version < clone.Knowledge[j].Version
	})
	for index := range clone.Recipes {
		sort.Strings(clone.Recipes[index].BindingIDs)
		sort.Strings(clone.Recipes[index].RequiredEvidence)
		sort.Strings(clone.Recipes[index].OptionalEvidence)
		clone.Recipes[index].ResultSchema = canonicalJSON(clone.Recipes[index].ResultSchema)
	}
	for index := range clone.Bindings {
		sort.Strings(clone.Bindings[index].ToolIDs)
	}
	return clone
}

func canonicalSnapshot(snapshot Snapshot) Snapshot {
	clone := snapshot.Clone()
	clone.SnapshotHash = ""
	canonical := canonicalManifest(Manifest{Recipes: clone.Recipes, Bindings: clone.Bindings, Knowledge: clone.Knowledge})
	clone.Recipes, clone.Bindings, clone.Knowledge = canonical.Recipes, canonical.Bindings, canonical.Knowledge
	return clone
}

func cloneSchema(schema prompttmpl.ContextSchema) prompttmpl.ContextSchema {
	clone := prompttmpl.ContextSchema{Fields: make(map[string]prompttmpl.Field, len(schema.Fields))}
	for name, field := range schema.Fields {
		field.Templates = append([]string(nil), field.Templates...)
		clone.Fields[name] = field
	}
	return clone
}

func cloneRecipes(recipes []WorkerRecipe) []WorkerRecipe {
	clone := make([]WorkerRecipe, len(recipes))
	for index, recipe := range recipes {
		clone[index] = recipe.Clone()
	}
	return clone
}

func cloneBindings(bindings []AdapterBinding) []AdapterBinding {
	clone := make([]AdapterBinding, len(bindings))
	for index, binding := range bindings {
		clone[index] = binding.Clone()
	}
	return clone
}

func clonePromptBundle(bundle prompttmpl.Bundle) prompttmpl.Bundle {
	clone := prompttmpl.Bundle{Templates: make(map[string]string, len(bundle.Templates)), Schema: cloneSchema(bundle.Schema)}
	for name, source := range bundle.Templates {
		clone.Templates[name] = source
	}
	return clone
}
