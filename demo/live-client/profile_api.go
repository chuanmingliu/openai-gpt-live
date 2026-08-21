package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/chuanmingliu/openai-gpt-live/internal/knowledge"
	"github.com/chuanmingliu/openai-gpt-live/internal/profile"
)

const (
	profileAPIPrefix      = "/api/setup/v1/"
	profileRequestTimeout = 10 * time.Second
	// maximumRecipeTimeoutMS mirrors the domain's 24-hour recipe duration cap.
	maximumRecipeTimeoutMS = 24 * 60 * 60 * 1000
	// maximumRecipeResultBytes mirrors the domain's bundle byte cap.
	maximumRecipeResultBytes = 1 << 20
)

// serveProfileAPI is the single strict boundary for /api/setup/v1/*.
func (handler *demoHandler) serveProfileAPI(writer http.ResponseWriter, request *http.Request) {
	if handler.profiles == nil {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	if request.URL.RawPath != "" && request.URL.RawPath != request.URL.Path {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	if request.Method != http.MethodGet {
		origins := request.Header.Values("Origin")
		if len(origins) != 1 || origins[0] != handler.cfg.Origin {
			writer.WriteHeader(http.StatusForbidden)
			return
		}
	}
	rest, ok := strings.CutPrefix(request.URL.Path, profileAPIPrefix)
	if !ok || rest == "" {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	segments := strings.Split(rest, "/")
	for _, segment := range segments {
		if segment == "" || segment == "." || segment == ".." {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
	}
	ctx, cancel := context.WithTimeout(request.Context(), profileRequestTimeout)
	defer cancel()
	request = request.WithContext(ctx)

	if segments[0] == "knowledge" {
		handler.serveKnowledge(writer, request, segments)
		return
	}
	if len(segments) == 1 && segments[0] == "catalog" {
		handler.serveAdapterCatalog(writer, request)
		return
	}
	if segments[0] != "profiles" {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	if len(segments) == 1 {
		handler.serveProfileCollection(writer, request)
		return
	}
	id := segments[1]
	if !validProfilePathID(id) {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	if request.URL.RawQuery != "" {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	switch {
	case len(segments) == 3 && segments[2] == "draft":
		handler.serveProfileDraft(writer, request, profile.ID(id))
	case len(segments) >= 4 && segments[2] == "templates":
		handler.serveProfileTemplate(writer, request, profile.ID(id), strings.Join(segments[3:], "/"))
	case len(segments) == 4 && segments[2] == "fixtures":
		handler.serveProfileFixture(writer, request, profile.ID(id), segments[3])
	case len(segments) == 4 && segments[2] == "knowledge":
		handler.serveProfileKnowledgePin(writer, request, profile.ID(id), segments[3])
	case len(segments) == 4 && segments[2] == "recipes":
		handler.serveProfileRecipe(writer, request, profile.ID(id), segments[3])
	case len(segments) == 4 && segments[2] == "bindings":
		handler.serveProfileBinding(writer, request, profile.ID(id), segments[3])
	case len(segments) == 3 && (segments[2] == "validate" || segments[2] == "preview" || segments[2] == "publish"):
		handler.serveProfilePublication(writer, request, profile.ID(id), segments[2])
	case len(segments) == 3 && (segments[2] == "activate" || segments[2] == "revoke"):
		handler.serveProfileLifecycle(writer, request, profile.ID(id), segments[2])
	default:
		writer.WriteHeader(http.StatusNotFound)
	}
}

type safeAdapterCatalog struct {
	RegistryVersion string                `json:"registry_version"`
	Adapters        []safeAdapterManifest `json:"adapters"`
}

type safeAdapterManifest struct {
	ID               string          `json:"id"`
	Version          string          `json:"version"`
	ToolName         string          `json:"tool_name"`
	Description      string          `json:"description"`
	ToolSchema       json.RawMessage `json:"tool_schema"`
	ReadOnly         bool            `json:"read_only"`
	SideEffecting    bool            `json:"side_effecting"`
	ParallelSafe     bool            `json:"parallel_safe"`
	ApprovalRequired bool            `json:"approval_required"`
	HealthClass      string          `json:"health_class"`
	MaxResultBytes   int             `json:"max_result_bytes"`
}

func (handler *demoHandler) serveAdapterCatalog(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if request.URL.RawQuery != "" {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	service := handler.profiles
	if service == nil || service.catalog == nil {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	manifests := service.catalog.Manifests()
	adapters := make([]safeAdapterManifest, 0, len(manifests))
	for _, manifest := range manifests {
		adapters = append(adapters, safeAdapterManifest{
			ID: manifest.AdapterID, Version: manifest.AdapterVersion, ToolName: manifest.Tool.Name,
			Description: manifest.Tool.Description, ToolSchema: append(json.RawMessage(nil), manifest.Tool.Parameters...), ReadOnly: manifest.ReadOnly, SideEffecting: manifest.SideEffecting,
			ParallelSafe: manifest.ParallelSafe, ApprovalRequired: manifest.Approval == "required",
			HealthClass: manifest.HealthClass, MaxResultBytes: manifest.MaxResultBytes,
		})
	}
	writeProfileJSON(writer, http.StatusOK, safeAdapterCatalog{RegistryVersion: service.catalog.RegistryVersion(), Adapters: adapters})
}

var profileBindingFields = map[string]byte{"max_result_bytes": 'n', "require_approval": 'b'}

func (handler *demoHandler) serveProfileBinding(writer http.ResponseWriter, request *http.Request, id profile.ID, adapterID string) {
	service := handler.profiles
	manifest, reviewed := service.catalog.Manifest(adapterID)
	if !reviewed {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	if request.Method != http.MethodPut && request.Method != http.MethodDelete {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	generation, present, err := parseIfMatchGeneration(request)
	if !present {
		writer.WriteHeader(http.StatusPreconditionRequired)
		return
	}
	if err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	if request.Method == http.MethodDelete {
		handler.mutateProfileDraft(writer, request, id, generation, func(profileManifest *profile.Manifest) error {
			removeProfileBinding(profileManifest, adapterID)
			return nil
		})
		return
	}
	if !requireProfileJSON(writer, request) {
		return
	}
	values, err := decodeProfileObject(writer, request, profileBindingFields)
	if err != nil {
		writeProfileBodyError(writer, err)
		return
	}
	maxBytes, err := strconv.Atoi(values["max_result_bytes"])
	requireApproval := values["require_approval"] == "true"
	if err != nil || maxBytes < 1 || maxBytes > manifest.MaxResultBytes || manifest.Approval == "required" && !requireApproval {
		writeProfileErrorCode(writer, http.StatusUnprocessableEntity, "profile_incompatible")
		return
	}
	binding := profile.AdapterBinding{AdapterID: manifest.AdapterID, Version: manifest.AdapterVersion, ToolIDs: []string{manifest.Tool.Name}, MaxResultBytes: maxBytes, RequireApproval: requireApproval}
	handler.mutateProfileDraft(writer, request, id, generation, func(profileManifest *profile.Manifest) error {
		removeProfileBinding(profileManifest, adapterID)
		profileManifest.Bindings = append(profileManifest.Bindings, binding)
		for index := range profileManifest.Recipes {
			profileManifest.Recipes[index].BindingIDs = append(profileManifest.Recipes[index].BindingIDs, adapterID)
		}
		return nil
	})
}

func removeProfileBinding(manifest *profile.Manifest, adapterID string) {
	bindings := make([]profile.AdapterBinding, 0, len(manifest.Bindings))
	for _, binding := range manifest.Bindings {
		if binding.AdapterID != adapterID {
			bindings = append(bindings, binding)
		}
	}
	manifest.Bindings = bindings
	for index := range manifest.Recipes {
		ids := make([]string, 0, len(manifest.Recipes[index].BindingIDs))
		for _, bindingID := range manifest.Recipes[index].BindingIDs {
			if bindingID != adapterID {
				ids = append(ids, bindingID)
			}
		}
		manifest.Recipes[index].BindingIDs = ids
	}
}

func requireProfileJSON(writer http.ResponseWriter, request *http.Request) bool {
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writer.WriteHeader(http.StatusUnsupportedMediaType)
		return false
	}
	return true
}

func (handler *demoHandler) serveProfileCollection(writer http.ResponseWriter, request *http.Request) {
	service := handler.profiles
	switch request.Method {
	case http.MethodGet:
		query, err := url.ParseQuery(request.URL.RawQuery)
		if err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		limit := defaultProfileListLimit
		cursor := ""
		for key, values := range query {
			if len(values) != 1 {
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			switch key {
			case "state":
				if values[0] != "published" {
					writer.WriteHeader(http.StatusBadRequest)
					return
				}
			case "limit":
				parsed, parseErr := parseProfileGeneration(values[0])
				if parseErr != nil || parsed > maximumProfileListLimit {
					writer.WriteHeader(http.StatusBadRequest)
					return
				}
				limit = int(parsed)
			case "cursor":
				if len(values[0]) == 0 || len(values[0]) > 512 {
					writer.WriteHeader(http.StatusBadRequest)
					return
				}
				cursor = values[0]
			default:
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
		}
		summaries, nextCursor, err := service.store.ListPublished(request.Context(), service.tenantID, limit, cursor)
		if err != nil {
			writeProfileDomainError(writer, err)
			return
		}
		response := publishedProfileList{Profiles: make([]publishedProfileSummary, 0, len(summaries)), Cursor: nextCursor}
		for _, summary := range summaries {
			response.Profiles = append(response.Profiles, publishedSummary(summary))
		}
		writeProfileJSON(writer, http.StatusOK, response)
	case http.MethodPost:
		if request.URL.RawQuery != "" {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		if !requireProfileJSON(writer, request) {
			return
		}
		values, err := decodeProfileObject(writer, request, map[string]byte{"id": 's', "description": 's'})
		if err != nil {
			writeProfileBodyError(writer, err)
			return
		}
		id, ok := values["id"]
		if !ok || !validProfilePathID(id) || !validProfileDescription(values["description"]) {
			writeProfileErrorCode(writer, http.StatusUnprocessableEntity, "profile_invalid")
			return
		}
		draft, err := service.seedDraft(request.Context(), profile.ID(id), values["description"])
		if err != nil {
			writeProfileDomainError(writer, err)
			return
		}
		writer.Header().Set("ETag", draftETag(draft.Generation))
		writeProfileJSON(writer, http.StatusCreated, draftResponse(draft))
	default:
		writer.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (handler *demoHandler) serveProfileDraft(writer http.ResponseWriter, request *http.Request, id profile.ID) {
	service := handler.profiles
	switch request.Method {
	case http.MethodGet:
		draft, ok, err := service.store.LoadDraft(request.Context(), service.tenantID, id)
		if err != nil {
			writeProfileDomainError(writer, err)
			return
		}
		if !ok {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		writer.Header().Set("ETag", draftETag(draft.Generation))
		writeProfileJSON(writer, http.StatusOK, draftResponse(draft))
	case http.MethodPut:
		if !requireProfileJSON(writer, request) {
			return
		}
		generation, present, err := parseIfMatchGeneration(request)
		if !present {
			writer.WriteHeader(http.StatusPreconditionRequired)
			return
		}
		if err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		values, err := decodeProfileObject(writer, request, map[string]byte{"description": 's'})
		if err != nil {
			writeProfileBodyError(writer, err)
			return
		}
		description, ok := values["description"]
		if !ok || !validProfileDescription(description) {
			writeProfileErrorCode(writer, http.StatusUnprocessableEntity, "profile_invalid")
			return
		}
		draft, ok, err := service.store.LoadDraft(request.Context(), service.tenantID, id)
		if err != nil {
			writeProfileDomainError(writer, err)
			return
		}
		if !ok {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		if draft.Generation != generation {
			writeProfileErrorCode(writer, http.StatusConflict, "stale_draft")
			return
		}
		draft.Manifest = draft.Manifest.Clone()
		draft.Manifest.Description = description
		draft.UpdatedAt = service.now()
		draft.UpdatedBy = service.actor
		saved, err := service.store.SaveDraft(request.Context(), draft, generation)
		if err != nil {
			writeProfileDomainError(writer, err)
			return
		}
		writer.Header().Set("ETag", draftETag(saved.Generation))
		writeProfileJSON(writer, http.StatusOK, draftResponse(saved))
	default:
		writer.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (handler *demoHandler) serveProfileTemplate(writer http.ResponseWriter, request *http.Request, id profile.ID, path string) {
	service := handler.profiles
	if profile.ValidateTemplateName(path) != nil {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	switch request.Method {
	case http.MethodGet:
		handler.serveProfileBlob(writer, request, id, path, false)
	case http.MethodPut:
		mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
		if err != nil || mediaType != "text/plain" {
			writer.WriteHeader(http.StatusUnsupportedMediaType)
			return
		}
		generation, present, err := parseIfMatchGeneration(request)
		if !present {
			writer.WriteHeader(http.StatusPreconditionRequired)
			return
		}
		if err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		source, ok := readProfileBody(writer, request, maximumProfileTemplateBytes)
		if !ok {
			return
		}
		if !utf8.Valid(source) {
			writeProfileErrorCode(writer, http.StatusUnprocessableEntity, "profile_invalid")
			return
		}
		draft, err := service.store.SaveTemplate(request.Context(), service.tenantID, id, path, source, generation, service.actor)
		if err != nil {
			writeProfileDomainError(writer, err)
			return
		}
		writer.Header().Set("ETag", draftETag(draft.Generation))
		writeProfileJSON(writer, http.StatusOK, draftResponse(draft))
	case http.MethodDelete:
		generation, present, err := parseIfMatchGeneration(request)
		if !present {
			writer.WriteHeader(http.StatusPreconditionRequired)
			return
		}
		if err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		draft, err := service.store.DeleteTemplate(request.Context(), service.tenantID, id, path, generation, service.actor)
		if err != nil {
			writeProfileDomainError(writer, err)
			return
		}
		writer.Header().Set("ETag", draftETag(draft.Generation))
		writer.WriteHeader(http.StatusNoContent)
	default:
		writer.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (handler *demoHandler) serveProfileFixture(writer http.ResponseWriter, request *http.Request, id profile.ID, name string) {
	service := handler.profiles
	if !validProfileFixtureName(name) {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	switch request.Method {
	case http.MethodGet:
		handler.serveProfileBlob(writer, request, id, name, true)
	case http.MethodPut:
		if !requireProfileJSON(writer, request) {
			return
		}
		generation, present, err := parseIfMatchGeneration(request)
		if !present {
			writer.WriteHeader(http.StatusPreconditionRequired)
			return
		}
		if err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		content, ok := readProfileBody(writer, request, maximumProfileTemplateBytes)
		if !ok {
			return
		}
		handler.mutateProfileFixture(writer, request, id, generation, func(manifest *profile.Manifest) error {
			blob, err := (profile.Blob{MediaType: "application/json", Content: content}).Canonical()
			if err != nil {
				return profile.ErrInvalidProfile
			}
			if err := service.store.PutBlob(request.Context(), service.tenantID, blob); err != nil {
				return err
			}
			if manifest.Bundle.Fixtures == nil {
				manifest.Bundle.Fixtures = make(map[string]profile.TemplateRef)
			}
			manifest.Bundle.Fixtures[name] = profile.TemplateRef{Hash: blob.Hash, MediaType: blob.MediaType, Bytes: blob.ContentBytes}
			return nil
		})
	case http.MethodDelete:
		generation, present, err := parseIfMatchGeneration(request)
		if !present {
			writer.WriteHeader(http.StatusPreconditionRequired)
			return
		}
		if err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		handler.mutateProfileFixture(writer, request, id, generation, func(manifest *profile.Manifest) error {
			if _, ok := manifest.Bundle.Fixtures[name]; !ok {
				return profile.ErrInvalidProfile
			}
			delete(manifest.Bundle.Fixtures, name)
			return nil
		})
	default:
		writer.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// recipeBodyFields are the only worker-recipe values a browser may supply.
// Identity, version, objective reference, and binding IDs stay server-owned.
var recipeBodyFields = map[string]byte{
	"require_approval":       'b',
	"execution_timeout_ms":   'n',
	"tool_timeout_ms":        'n',
	"overall_timeout_ms":     'n',
	"max_objective_bytes":    'n',
	"max_evidence_bytes":     'n',
	"max_tool_result_bytes":  'n',
	"max_final_result_bytes": 'n',
}

func (handler *demoHandler) serveProfileRecipe(writer http.ResponseWriter, request *http.Request, id profile.ID, recipeID string) {
	if !validProfileRecipeID(recipeID) {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	switch request.Method {
	case http.MethodPut:
		if !requireProfileJSON(writer, request) {
			return
		}
		generation, present, err := parseIfMatchGeneration(request)
		if !present {
			writer.WriteHeader(http.StatusPreconditionRequired)
			return
		}
		if err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		values, err := decodeProfileObject(writer, request, recipeBodyFields)
		if err != nil {
			writeProfileBodyError(writer, err)
			return
		}
		recipe, err := recipeFromValues(recipeID, values)
		if err != nil {
			writeProfileErrorCode(writer, http.StatusUnprocessableEntity, "profile_invalid")
			return
		}
		handler.mutateProfileDraft(writer, request, id, generation, func(manifest *profile.Manifest) error {
			objective, ok := manifest.Bundle.Templates[workerObjectiveName(recipeID)]
			if !ok {
				return errObjectiveTemplateMissing
			}
			// The domain requires the recipe objective to equal the current
			// template reference exactly, so the server always relinks it.
			recipe.Objective = objective
			recipe.Version = 1
			recipe.BindingIDs = make([]string, 0, len(manifest.Bindings))
			for _, binding := range manifest.Bindings {
				recipe.BindingIDs = append(recipe.BindingIDs, binding.AdapterID)
			}
			replaced := false
			for index, existing := range manifest.Recipes {
				if existing.ID != recipeID {
					continue
				}
				recipe.Version = existing.Version + 1
				manifest.Recipes[index] = recipe
				replaced = true
				break
			}
			if !replaced {
				manifest.Recipes = append(manifest.Recipes, recipe)
			}
			return nil
		})
	case http.MethodDelete:
		generation, present, err := parseIfMatchGeneration(request)
		if !present {
			writer.WriteHeader(http.StatusPreconditionRequired)
			return
		}
		if err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		handler.mutateProfileDraft(writer, request, id, generation, func(manifest *profile.Manifest) error {
			remaining := make([]profile.WorkerRecipe, 0, len(manifest.Recipes))
			for _, existing := range manifest.Recipes {
				if existing.ID != recipeID {
					remaining = append(remaining, existing)
				}
			}
			if len(remaining) == len(manifest.Recipes) {
				return profile.ErrInvalidProfile
			}
			manifest.Recipes = remaining
			return nil
		}, http.StatusNoContent)
	default:
		writer.WriteHeader(http.StatusMethodNotAllowed)
	}
}

var errObjectiveTemplateMissing = errors.New("objective_template_missing")

// recipeFromValues builds a bounded recipe from the decoded safe fields. Every
// numeric bound is validated here so an out-of-range value never reaches the
// domain as a generic decode failure.
func recipeFromValues(recipeID string, values map[string]string) (profile.WorkerRecipe, error) {
	recipe := profile.WorkerRecipe{ID: recipeID}
	recipe.RequireApproval = values["require_approval"] == "true"
	durations := map[string]*time.Duration{
		"execution_timeout_ms": &recipe.ExecutionTimeout,
		"tool_timeout_ms":      &recipe.ToolTimeout,
		"overall_timeout_ms":   &recipe.OverallTimeout,
	}
	for key, target := range durations {
		raw, ok := values[key]
		if !ok {
			continue
		}
		milliseconds, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || milliseconds < 0 || milliseconds > maximumRecipeTimeoutMS {
			return profile.WorkerRecipe{}, errProfileBadRequest
		}
		*target = time.Duration(milliseconds) * time.Millisecond
	}
	byteFields := map[string]*int{
		"max_objective_bytes":    &recipe.MaxObjectiveBytes,
		"max_evidence_bytes":     &recipe.MaxEvidenceBytes,
		"max_tool_result_bytes":  &recipe.MaxToolResultBytes,
		"max_final_result_bytes": &recipe.MaxFinalResultBytes,
	}
	for key, target := range byteFields {
		raw, ok := values[key]
		if !ok {
			continue
		}
		count, err := strconv.Atoi(raw)
		if err != nil || count < 0 || count > maximumRecipeResultBytes {
			return profile.WorkerRecipe{}, errProfileBadRequest
		}
		*target = count
	}
	return recipe, nil
}

// validProfileRecipeID keeps recipe identities inside the logical template
// namespace so workers/<id>.j2 is always a valid bundle name.
func validProfileRecipeID(recipeID string) bool {
	if len(recipeID) == 0 || len(recipeID) > 64 {
		return false
	}
	for _, value := range []byte(recipeID) {
		if !(value >= 'a' && value <= 'z' || value >= '0' && value <= '9' || value == '_' || value == '-') {
			return false
		}
	}
	return profile.ValidateTemplateName(workerObjectiveName(recipeID)) == nil
}

// mutateProfileDraft applies one bounded manifest change under the exact draft
// generation and persists it as the next generation.
var knowledgePinFields = map[string]byte{"version": 'n'}

// serveProfileKnowledgePin attaches one published knowledge version to the
// draft, or removes it. The browser names a pack and a version number only: the
// content hash is resolved from the store, so a profile can never pin material
// whose bytes the operator asserted rather than published.
func (handler *demoHandler) serveProfileKnowledgePin(writer http.ResponseWriter, request *http.Request, id profile.ID, packID string) {
	service := handler.profiles
	if service.knowledge == nil || !validKnowledgePackID(packID) {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	if request.Method != http.MethodPut && request.Method != http.MethodDelete {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	generation, present, err := parseIfMatchGeneration(request)
	if err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	if !present {
		writer.WriteHeader(http.StatusPreconditionRequired)
		return
	}

	if request.Method == http.MethodDelete {
		handler.mutateProfileDraft(writer, request, id, generation, func(manifest *profile.Manifest) error {
			kept := make([]profile.KnowledgeRef, 0, len(manifest.Knowledge))
			for _, reference := range manifest.Knowledge {
				if reference.ID != packID {
					kept = append(kept, reference)
				}
			}
			manifest.Knowledge = kept
			return nil
		}, http.StatusNoContent)
		return
	}

	if !requireProfileJSON(writer, request) {
		return
	}
	values, err := decodeProfileObject(writer, request, knowledgePinFields)
	if err != nil {
		writeProfileBodyError(writer, err)
		return
	}
	raw, ok := values["version"]
	if !ok {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	version, err := parseProfileVersion(raw)
	if err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}

	// Resolution happens before the draft is touched, so a bad pin cannot
	// advance the generation.
	versions, err := service.knowledge.List(request.Context(), service.tenantID, packID, maximumKnowledgeListLimit)
	if err != nil {
		writeKnowledgeError(writer, err)
		return
	}
	var resolved knowledge.Version
	found := false
	for _, candidate := range versions {
		if candidate.Ref.Version == version {
			resolved = candidate
			found = true
			break
		}
	}
	if !found {
		writeProfileErrorCode(writer, http.StatusUnprocessableEntity, "knowledge_not_found")
		return
	}
	if !resolved.Bindable() {
		writeProfileErrorCode(writer, http.StatusConflict, "knowledge_tombstoned")
		return
	}

	handler.mutateProfileDraft(writer, request, id, generation, func(manifest *profile.Manifest) error {
		pinned := make([]profile.KnowledgeRef, 0, len(manifest.Knowledge)+1)
		for _, reference := range manifest.Knowledge {
			if reference.ID != packID {
				pinned = append(pinned, reference)
			}
		}
		// One version per pack: pinning again replaces rather than accumulates,
		// so a manifest cannot hold two versions of the same material.
		manifest.Knowledge = append(pinned, profile.KnowledgeRef{
			ID: packID, Version: resolved.Ref.Version, Hash: resolved.Ref.Hash,
		})
		return nil
	})
}

func (handler *demoHandler) mutateProfileDraft(writer http.ResponseWriter, request *http.Request, id profile.ID, generation int64, mutate func(*profile.Manifest) error, successStatus ...int) {
	service := handler.profiles
	draft, ok, err := service.store.LoadDraft(request.Context(), service.tenantID, id)
	if err != nil {
		writeProfileDomainError(writer, err)
		return
	}
	if !ok {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	if draft.Generation != generation {
		writeProfileErrorCode(writer, http.StatusConflict, "stale_draft")
		return
	}
	draft.Manifest = draft.Manifest.Clone()
	if err := mutate(&draft.Manifest); err != nil {
		if errors.Is(err, errObjectiveTemplateMissing) {
			writeProfileErrorCode(writer, http.StatusUnprocessableEntity, "objective_template_missing")
			return
		}
		writeProfileDomainError(writer, err)
		return
	}
	draft.UpdatedAt = service.now()
	draft.UpdatedBy = service.actor
	saved, err := service.store.SaveDraft(request.Context(), draft, generation)
	if err != nil {
		writeProfileDomainError(writer, err)
		return
	}
	writer.Header().Set("ETag", draftETag(saved.Generation))
	if len(successStatus) == 1 && successStatus[0] == http.StatusNoContent {
		writer.WriteHeader(http.StatusNoContent)
		return
	}
	writeProfileJSON(writer, http.StatusOK, draftResponse(saved))
}

func (handler *demoHandler) mutateProfileFixture(writer http.ResponseWriter, request *http.Request, id profile.ID, generation int64, mutate func(*profile.Manifest) error) {
	service := handler.profiles
	draft, ok, err := service.store.LoadDraft(request.Context(), service.tenantID, id)
	if err != nil {
		writeProfileDomainError(writer, err)
		return
	}
	if !ok {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	if draft.Generation != generation {
		writeProfileErrorCode(writer, http.StatusConflict, "stale_draft")
		return
	}
	draft.Manifest = draft.Manifest.Clone()
	if err := mutate(&draft.Manifest); err != nil {
		writeProfileDomainError(writer, err)
		return
	}
	draft.UpdatedAt = service.now()
	draft.UpdatedBy = service.actor
	saved, err := service.store.SaveDraft(request.Context(), draft, generation)
	if err != nil {
		writeProfileDomainError(writer, err)
		return
	}
	writer.Header().Set("ETag", draftETag(saved.Generation))
	writeProfileJSON(writer, http.StatusOK, draftResponse(saved))
}

func (handler *demoHandler) serveProfileBlob(writer http.ResponseWriter, request *http.Request, id profile.ID, path string, fixture bool) {
	service := handler.profiles
	draft, ok, err := service.store.LoadDraft(request.Context(), service.tenantID, id)
	if err != nil {
		writeProfileDomainError(writer, err)
		return
	}
	if !ok {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	refs := draft.Manifest.Bundle.Templates
	if fixture {
		refs = draft.Manifest.Bundle.Fixtures
	}
	ref, ok := refs[path]
	if !ok {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	blob, ok, err := service.store.LoadBlob(request.Context(), service.tenantID, ref.Hash)
	if err != nil {
		writeProfileDomainError(writer, err)
		return
	}
	if !ok {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	writer.Header().Set("Content-Type", blob.MediaType)
	writer.Header().Set("ETag", draftETag(draft.Generation))
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(blob.Content)
}

func (handler *demoHandler) serveProfilePublication(writer http.ResponseWriter, request *http.Request, id profile.ID, action string) {
	service := handler.profiles
	if request.Method != http.MethodPost {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if !requireProfileJSON(writer, request) {
		return
	}
	allowed := map[string]byte{"expected_generation": 'n'}
	if action == "publish" {
		allowed["activate"] = 'b'
	}
	values, err := decodeProfileObject(writer, request, allowed)
	if err != nil {
		writeProfileBodyError(writer, err)
		return
	}
	rawGeneration, ok := values["expected_generation"]
	if !ok {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	generation, err := parseProfileGeneration(rawGeneration)
	if err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	command := profile.PublishCommand{
		TenantID:           service.tenantID,
		ProfileID:          id,
		ExpectedGeneration: generation,
		Actor:              service.actor,
		Activate:           values["activate"] == "true",
	}
	switch action {
	case "validate":
		if err := service.publisher.Validate(request.Context(), command); err != nil {
			writeProfileDomainError(writer, err)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	case "preview":
		preview, err := service.publisher.Preview(request.Context(), command)
		if err != nil {
			writeProfileDomainError(writer, err)
			return
		}
		outputs := preview.Outputs
		if outputs == nil {
			outputs = map[string]string{}
		}
		writeProfileJSON(writer, http.StatusOK, profilePreviewResponse{
			BundleHash: preview.BundleHash, SnapshotHash: preview.SnapshotHash, Outputs: outputs,
		})
	case "publish":
		published, err := service.publisher.Publish(request.Context(), command)
		if err != nil {
			writeProfileDomainError(writer, err)
			return
		}
		writeProfileJSON(writer, http.StatusCreated, profilePublishResponse{
			ID:           string(published.Ref.ID),
			Version:      published.Ref.Version,
			SnapshotHash: published.Snapshot.SnapshotHash,
			Active:       published.Active,
		})
	}
}

func (handler *demoHandler) serveProfileLifecycle(writer http.ResponseWriter, request *http.Request, id profile.ID, action string) {
	service := handler.profiles
	if request.Method != http.MethodPost {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if !requireProfileJSON(writer, request) {
		return
	}
	values, err := decodeProfileObject(writer, request, map[string]byte{"version": 'n'})
	if err != nil {
		writeProfileBodyError(writer, err)
		return
	}
	rawVersion, ok := values["version"]
	if !ok {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	version, err := parseProfileVersion(rawVersion)
	if err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	ref := profile.VersionRef{ID: id, Version: version}
	if action == "activate" {
		err = service.publisher.Activate(request.Context(), service.tenantID, ref, service.actor)
	} else {
		err = service.publisher.Revoke(request.Context(), service.tenantID, ref, service.actor)
	}
	if err != nil {
		writeProfileDomainError(writer, err)
		return
	}
	writer.WriteHeader(http.StatusNoContent)
}

// readProfileBody reads at most maximum bytes and reports oversize as 413.
func readProfileBody(writer http.ResponseWriter, request *http.Request, maximum int64) ([]byte, bool) {
	content, err := io.ReadAll(http.MaxBytesReader(writer, request.Body, maximum))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writer.WriteHeader(http.StatusRequestEntityTooLarge)
		} else {
			writer.WriteHeader(http.StatusBadRequest)
		}
		return nil, false
	}
	return content, true
}

func writeProfileBodyError(writer http.ResponseWriter, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writer.WriteHeader(http.StatusRequestEntityTooLarge)
		return
	}
	writer.WriteHeader(http.StatusBadRequest)
}

func validProfileDescription(value string) bool {
	return len(value) <= 4096 && utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}

// validProfileFixtureName mirrors the profile package's fixture rules for one
// path segment: lowercase alphanumeric start, .json suffix, bounded charset.
func validProfileFixtureName(name string) bool {
	if len(name) < len("a.json") || len(name) > 256 || !strings.HasSuffix(name, ".json") {
		return false
	}
	first := name[0]
	if !(first >= 'a' && first <= 'z' || first >= '0' && first <= '9') {
		return false
	}
	for _, value := range []byte(name) {
		if !(value >= 'a' && value <= 'z' || value >= '0' && value <= '9' || value == '_' || value == '.' || value == '-') {
			return false
		}
	}
	return true
}
