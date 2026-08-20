package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"sort"
	"time"
	"unicode/utf8"

	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/profile"
	"github.com/chuanmingliu/openai-gpt-live/internal/prompttmpl"
)

var (
	ErrProfileIncompatible   = errors.New("profile_incompatible")
	ErrRecipeTemplateInvalid = errors.New("template_invalid")
	ErrRecipeContextInvalid  = errors.New("context_invalid")
	ErrRecipeRenderLimit     = errors.New("render_limit_exceeded")
)

type RecipeEvidence struct {
	TenantID string
	Values   json.RawMessage
}

type RecipeLimits struct {
	MaxObjectiveBytes   int
	MaxEvidenceBytes    int
	MaxToolResultBytes  int
	MaxFinalResultBytes int
	ExecutionTimeout    time.Duration
	ToolTimeout         time.Duration
	OverallTimeout      time.Duration
}

type RenderedRecipe struct {
	Objective        string
	System           string
	Evidence         json.RawMessage
	ToolAllowlist    []string
	RequiresApproval bool
	RegistryVersion  string
	RecipeHash       string
	SnapshotHash     string
	Limits           RecipeLimits
}

func (rendered RenderedRecipe) clone() RenderedRecipe {
	rendered.Evidence = append(json.RawMessage(nil), rendered.Evidence...)
	rendered.ToolAllowlist = append([]string(nil), rendered.ToolAllowlist...)
	return rendered
}

type RecipeResolver interface {
	ResolveAndRender(context.Context, handoff.ProfileSnapshot, RecipeEvidence) (RenderedRecipe, error)
}

type exactRecipeResolver struct {
	profiles profile.Resolver
	engine   prompttmpl.Engine
	tools    ToolRegistry
}

func NewRecipeResolver(profiles profile.Resolver, engine prompttmpl.Engine, tools ToolRegistry) (RecipeResolver, error) {
	if nilValue(profiles) || nilValue(engine) || nilValue(tools) || prompttmpl.ValidateEngineIdentity(engine.Version()) != nil {
		return nil, ErrProfileIncompatible
	}
	return &exactRecipeResolver{profiles: profiles, engine: engine, tools: tools}, nil
}

func (resolver *exactRecipeResolver) ResolveAndRender(ctx context.Context, snapshot handoff.ProfileSnapshot, evidence RecipeEvidence) (RenderedRecipe, error) {
	if resolver == nil || ctx == nil || ctx.Err() != nil {
		if ctx != nil && ctx.Err() != nil {
			return RenderedRecipe{}, ctx.Err()
		}
		return RenderedRecipe{}, ErrProfileIncompatible
	}
	if !validRecipeBoundary(snapshot, evidence) {
		return RenderedRecipe{}, ErrProfileIncompatible
	}
	canonicalEvidence, values, err := decodeRecipeEvidence(evidence.Values)
	if err != nil {
		return RenderedRecipe{}, ErrRecipeContextInvalid
	}
	canonicalSnapshotEvidence, _, err := decodeRecipeEvidence(snapshot.Evidence)
	if err != nil || !bytes.Equal(canonicalEvidence, canonicalSnapshotEvidence) {
		return RenderedRecipe{}, ErrProfileIncompatible
	}
	resolved, err := resolver.profiles.Resolve(ctx, evidence.TenantID, profile.VersionRef{ID: profile.ID(snapshot.ProfileID), Version: snapshot.ProfileVersion}, profile.ResolveAcceptedWork)
	if ctx.Err() != nil {
		return RenderedRecipe{}, ctx.Err()
	}
	if err != nil {
		return RenderedRecipe{}, normalizeRecipeDependencyError(err)
	}
	resolved = resolved.Clone()
	if resolved.Validate() != nil || resolved.Snapshot.SnapshotHash != snapshot.SnapshotHash || resolved.Snapshot.EngineVersion != snapshot.EngineVersion || resolved.Snapshot.AdapterRegistryVersion != snapshot.RegistryVersion || resolver.engine.Version() != snapshot.EngineVersion || !sameKnowledge(snapshot.Knowledge, resolved.Snapshot.Knowledge) {
		return RenderedRecipe{}, ErrProfileIncompatible
	}
	recipe, ok := exactRecipe(resolved.Snapshot.Recipes, snapshot.RecipeID, snapshot.RecipeVersion)
	if !ok {
		return RenderedRecipe{}, ErrProfileIncompatible
	}
	recipeHash, err := recipe.Hash()
	if err != nil || recipeHash != snapshot.RecipeHash {
		return RenderedRecipe{}, ErrProfileIncompatible
	}
	if recipe.MaxEvidenceBytes <= 0 || len(canonicalEvidence) > recipe.MaxEvidenceBytes || !exactEvidenceKeys(recipe, values) {
		return RenderedRecipe{}, ErrRecipeContextInvalid
	}
	templateName := "workers/" + recipe.ID + ".j2"
	validated, err := prompttmpl.ValidateContext(resolved.Bundle.Schema, prompttmpl.Context(values), templateName)
	if err != nil {
		return RenderedRecipe{}, ErrRecipeContextInvalid
	}
	allowlist, requiresApproval, bindingResultLimit, err := resolveRecipeTools(resolver.tools, resolved.Snapshot, recipe)
	if err != nil {
		return RenderedRecipe{}, err
	}
	limits := prompttmpl.DefaultLimits()
	limits.ContextBytes = minPositive(recipe.MaxEvidenceBytes, prompttmpl.MaxContextBytes)
	limits.OutputBytes = minPositive(recipe.MaxObjectiveBytes, prompttmpl.MaxOutputBytes)
	compiled, err := resolver.engine.Compile(ctx, resolved.Bundle, limits)
	if ctx.Err() != nil {
		return RenderedRecipe{}, ctx.Err()
	}
	if err != nil {
		return RenderedRecipe{}, normalizeRecipeTemplateError(err)
	}
	if nilValue(compiled) || compiled.EngineVersion() != snapshot.EngineVersion || resolver.engine.Version() != snapshot.EngineVersion {
		return RenderedRecipe{}, ErrProfileIncompatible
	}
	objective, err := compiled.Render(ctx, templateName, validated)
	if ctx.Err() != nil {
		return RenderedRecipe{}, ctx.Err()
	}
	if err != nil {
		return RenderedRecipe{}, normalizeRecipeTemplateError(err)
	}
	if objective == "" || len(objective) > recipe.MaxObjectiveBytes || !utf8.ValidString(objective) {
		return RenderedRecipe{}, ErrRecipeRenderLimit
	}
	return (RenderedRecipe{
		Objective: objective, System: workerSystemTemplate, Evidence: canonicalEvidence,
		ToolAllowlist: allowlist, RequiresApproval: requiresApproval, RegistryVersion: snapshot.RegistryVersion,
		RecipeHash: recipeHash, SnapshotHash: snapshot.SnapshotHash,
		Limits: RecipeLimits{MaxObjectiveBytes: recipe.MaxObjectiveBytes, MaxEvidenceBytes: recipe.MaxEvidenceBytes,
			MaxToolResultBytes: minNonzero(recipe.MaxToolResultBytes, bindingResultLimit), MaxFinalResultBytes: recipe.MaxFinalResultBytes,
			ExecutionTimeout: recipe.ExecutionTimeout, ToolTimeout: recipe.ToolTimeout, OverallTimeout: recipe.OverallTimeout},
	}).clone(), nil
}

func validRecipeBoundary(snapshot handoff.ProfileSnapshot, evidence RecipeEvidence) bool {
	return snapshot.ProfileID != "" && snapshot.ProfileVersion != 0 && snapshot.SnapshotHash != "" && snapshot.EngineVersion != "" && snapshot.RecipeID != "" && snapshot.RecipeVersion != 0 && snapshot.RecipeHash != "" && snapshot.RegistryVersion != "" && evidence.TenantID != "" && len(evidence.Values) != 0
}

func exactRecipe(recipes []profile.WorkerRecipe, id string, version uint64) (profile.WorkerRecipe, bool) {
	for _, recipe := range recipes {
		if recipe.ID == id && recipe.Version == version {
			return recipe.Clone(), true
		}
	}
	return profile.WorkerRecipe{}, false
}

func sameKnowledge(actual []handoff.KnowledgeVersion, expected []profile.KnowledgeRef) bool {
	if len(actual) != len(expected) {
		return false
	}
	byID := make(map[string]handoff.KnowledgeVersion, len(actual))
	for _, value := range actual {
		byID[value.ID] = value
	}
	for _, value := range expected {
		got, ok := byID[value.ID]
		if !ok || got.Version != value.Version || got.Hash != value.Hash {
			return false
		}
	}
	return true
}

func exactEvidenceKeys(recipe profile.WorkerRecipe, values map[string]any) bool {
	allowed := make(map[string]bool, len(recipe.RequiredEvidence)+len(recipe.OptionalEvidence))
	for _, name := range recipe.RequiredEvidence {
		if allowed[name] {
			return false
		}
		allowed[name] = true
		if _, ok := values[name]; !ok {
			return false
		}
	}
	for _, name := range recipe.OptionalEvidence {
		if allowed[name] {
			return false
		}
		allowed[name] = true
	}
	for name := range values {
		if !allowed[name] {
			return false
		}
	}
	return true
}

func resolveRecipeTools(registry ToolRegistry, snapshot profile.Snapshot, recipe profile.WorkerRecipe) ([]string, bool, int, error) {
	if snapshot.AdapterRegistryVersion == "" || len(recipe.BindingIDs) == 0 {
		return nil, false, recipe.MaxToolResultBytes, nil
	}
	bindings := make(map[string]profile.AdapterBinding, len(snapshot.Bindings))
	for _, binding := range snapshot.Bindings {
		bindings[binding.AdapterID] = binding.Clone()
	}
	seen := make(map[string]struct{})
	allowlist := make([]string, 0)
	requiresApproval := false
	resultLimit := recipe.MaxToolResultBytes
	for _, bindingID := range recipe.BindingIDs {
		binding, ok := bindings[bindingID]
		if !ok {
			return nil, false, 0, ErrProfileIncompatible
		}
		if binding.RequireApproval {
			requiresApproval = true
		}
		resultLimit = minNonzero(resultLimit, binding.MaxResultBytes)
		for _, name := range binding.ToolIDs {
			tool, ok := registry.Lookup(snapshot.AdapterRegistryVersion, name)
			if !ok || tool.AdapterID != binding.AdapterID || tool.AdapterVersion != binding.Version || tool.RegistryVersion != snapshot.AdapterRegistryVersion || binding.MaxResultBytes > 0 && binding.MaxResultBytes > tool.MaxResultBytes || tool.Approval == "required" && !binding.RequireApproval {
				return nil, false, 0, ErrProfileIncompatible
			}
			if _, duplicate := seen[name]; duplicate {
				return nil, false, 0, ErrProfileIncompatible
			}
			seen[name] = struct{}{}
			allowlist = append(allowlist, name)
		}
	}
	sort.Strings(allowlist)
	return allowlist, requiresApproval, resultLimit, nil
}

func decodeRecipeEvidence(raw json.RawMessage) (json.RawMessage, map[string]any, error) {
	if len(raw) < 2 || len(raw) > prompttmpl.MaxContextBytes || !utf8.Valid(raw) || !json.Valid(raw) {
		return nil, nil, ErrRecipeContextInvalid
	}
	duplicateDecoder := json.NewDecoder(bytes.NewReader(raw))
	duplicateDecoder.UseNumber()
	if err := scanRecipeJSON(duplicateDecoder); err != nil || duplicateDecoder.Decode(&struct{}{}) != io.EOF {
		return nil, nil, ErrRecipeContextInvalid
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var values map[string]any
	if err := decoder.Decode(&values); err != nil || values == nil || decoder.Decode(&struct{}{}) != io.EOF {
		return nil, nil, ErrRecipeContextInvalid
	}
	canonical, err := json.Marshal(values)
	if err != nil {
		return nil, nil, ErrRecipeContextInvalid
	}
	return json.RawMessage(canonical), values, nil
}

func scanRecipeJSON(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return ErrRecipeContextInvalid
			}
			if _, duplicate := seen[key]; duplicate {
				return ErrRecipeContextInvalid
			}
			seen[key] = struct{}{}
			if err := scanRecipeJSON(decoder); err != nil {
				return err
			}
		}
	case '[':
		for decoder.More() {
			if err := scanRecipeJSON(decoder); err != nil {
				return err
			}
		}
	default:
		return ErrRecipeContextInvalid
	}
	if _, err := decoder.Token(); err != nil {
		return err
	}
	return nil
}

func normalizeRecipeDependencyError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return ErrProfileIncompatible
}

func normalizeRecipeTemplateError(err error) error {
	switch {
	case errors.Is(err, prompttmpl.ErrContextInvalid):
		return ErrRecipeContextInvalid
	case errors.Is(err, prompttmpl.ErrRenderLimit), errors.Is(err, prompttmpl.ErrLimitsInvalid):
		return ErrRecipeRenderLimit
	case errors.Is(err, prompttmpl.ErrTemplateInvalid):
		return ErrRecipeTemplateInvalid
	default:
		return ErrRecipeTemplateInvalid
	}
}

func minPositive(value, maximum int) int {
	if value <= 0 || value > maximum {
		return maximum
	}
	return value
}
