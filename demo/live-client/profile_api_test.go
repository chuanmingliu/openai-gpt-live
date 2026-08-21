package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/profile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeProfileStore is an in-memory profile.Store with the same generation,
// idempotency, and lifecycle semantics the API depends on.
type fakeProfileStore struct {
	mu                   sync.Mutex
	calls                int
	blobs                map[string]profile.Blob
	drafts               map[string]profile.Draft
	published            map[string]profile.Published
	publishedGenerations map[string]bool
}

func newFakeProfileStore() *fakeProfileStore {
	return &fakeProfileStore{
		blobs:                make(map[string]profile.Blob),
		drafts:               make(map[string]profile.Draft),
		published:            make(map[string]profile.Published),
		publishedGenerations: make(map[string]bool),
	}
}

func (store *fakeProfileStore) Calls() int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.calls
}

func draftKey(tenantID string, id profile.ID) string { return tenantID + "\x00" + string(id) }

func publishedKey(tenantID string, id profile.ID, version uint64) string {
	return fmt.Sprintf("%s\x00%s\x00%d", tenantID, id, version)
}

func (store *fakeProfileStore) PutBlob(_ context.Context, tenantID string, blob profile.Blob) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.calls++
	canonical, err := blob.Canonical()
	if err != nil {
		return profile.ErrInvalidProfile
	}
	store.blobs[tenantID+"\x00"+canonical.Hash] = canonical
	return nil
}

func (store *fakeProfileStore) LoadBlob(_ context.Context, tenantID, hash string) (profile.Blob, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.calls++
	blob, ok := store.blobs[tenantID+"\x00"+hash]
	if !ok {
		return profile.Blob{}, false, nil
	}
	return blob.Clone(), true, nil
}

func (store *fakeProfileStore) LoadDraft(_ context.Context, tenantID string, id profile.ID) (profile.Draft, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.calls++
	draft, ok := store.drafts[draftKey(tenantID, id)]
	if !ok {
		return profile.Draft{}, false, nil
	}
	return draft.Clone(), true, nil
}

func (store *fakeProfileStore) SaveDraft(_ context.Context, draft profile.Draft, expectedGeneration int64) (profile.Draft, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.calls++
	owned := draft.Clone()
	if owned.Validate() != nil || expectedGeneration < 0 {
		return profile.Draft{}, profile.ErrInvalidProfile
	}
	key := draftKey(owned.TenantID, owned.ID)
	current, exists := store.drafts[key]
	if expectedGeneration == 0 {
		if exists {
			return profile.Draft{}, profile.ErrStaleDraft
		}
		owned.Generation = 1
	} else {
		if !exists || current.Generation != expectedGeneration {
			return profile.Draft{}, profile.ErrStaleDraft
		}
		owned.Generation = expectedGeneration + 1
	}
	store.drafts[key] = owned.Clone()
	return owned.Clone(), nil
}

func (store *fakeProfileStore) SaveTemplate(_ context.Context, tenantID string, id profile.ID, logicalName string, source []byte, expectedGeneration int64, actor string) (profile.Draft, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.calls++
	if profile.ValidateTemplateName(logicalName) != nil || expectedGeneration <= 0 {
		return profile.Draft{}, profile.ErrInvalidProfile
	}
	blob, err := (profile.Blob{MediaType: "text/plain; charset=utf-8", Content: append([]byte(nil), source...)}).Canonical()
	if err != nil {
		return profile.Draft{}, profile.ErrInvalidProfile
	}
	key := draftKey(tenantID, id)
	current, exists := store.drafts[key]
	if !exists || current.Generation != expectedGeneration {
		return profile.Draft{}, profile.ErrStaleDraft
	}
	store.blobs[tenantID+"\x00"+blob.Hash] = blob
	next := current.Clone()
	next.Manifest.Bundle.Templates[logicalName] = profile.TemplateRef{Hash: blob.Hash, MediaType: blob.MediaType, Bytes: blob.ContentBytes}
	if next.Manifest.Validate() != nil {
		return profile.Draft{}, profile.ErrInvalidProfile
	}
	next.Generation = expectedGeneration + 1
	next.UpdatedBy = actor
	next.UpdatedAt = time.Now().UTC().Truncate(time.Microsecond)
	store.drafts[key] = next.Clone()
	return next.Clone(), nil
}

func (store *fakeProfileStore) DeleteTemplate(_ context.Context, tenantID string, id profile.ID, logicalName string, expectedGeneration int64, actor string) (profile.Draft, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.calls++
	key := draftKey(tenantID, id)
	current, exists := store.drafts[key]
	if !exists || current.Generation != expectedGeneration {
		return profile.Draft{}, profile.ErrStaleDraft
	}
	next := current.Clone()
	if _, ok := next.Manifest.Bundle.Templates[logicalName]; !ok {
		return profile.Draft{}, profile.ErrInvalidProfile
	}
	delete(next.Manifest.Bundle.Templates, logicalName)
	if next.Manifest.Validate() != nil {
		return profile.Draft{}, profile.ErrInvalidProfile
	}
	next.Generation = expectedGeneration + 1
	next.UpdatedBy = actor
	next.UpdatedAt = time.Now().UTC().Truncate(time.Microsecond)
	store.drafts[key] = next.Clone()
	return next.Clone(), nil
}

func (store *fakeProfileStore) NextVersion(_ context.Context, tenantID string, id profile.ID) (uint64, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.calls++
	return store.nextVersionLocked(tenantID, id), nil
}

func (store *fakeProfileStore) nextVersionLocked(tenantID string, id profile.ID) uint64 {
	next := uint64(1)
	for _, published := range store.published {
		if published.TenantID == tenantID && published.Ref.ID == id && published.Ref.Version >= next {
			next = published.Ref.Version + 1
		}
	}
	return next
}

func (store *fakeProfileStore) Publish(_ context.Context, record profile.PublishRecord) (profile.Published, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.calls++
	if record.Validate() != nil {
		return profile.Published{}, profile.ErrInvalidProfile
	}
	key := draftKey(record.TenantID, record.ProfileID)
	current, exists := store.drafts[key]
	if !exists || current.Generation != record.ExpectedGeneration {
		return profile.Published{}, profile.ErrStaleDraft
	}
	generationKey := fmt.Sprintf("%s\x00%d", key, record.ExpectedGeneration)
	if store.publishedGenerations[generationKey] {
		return profile.Published{}, profile.ErrPublicationConflict
	}
	if record.Snapshot.Ref.Version != store.nextVersionLocked(record.TenantID, record.ProfileID) {
		return profile.Published{}, profile.ErrPublicationConflict
	}
	published := profile.Published{
		TenantID:        record.TenantID,
		Ref:             record.Snapshot.Ref,
		DraftGeneration: record.DraftGeneration,
		EngineVersion:   record.EngineVersion,
		Snapshot:        record.Snapshot.Clone(),
		Manifest:        record.Manifest.Clone(),
		Active:          record.Activate,
		PublishedAt:     record.PublishedAt,
		PublishedBy:     record.Actor,
	}
	if record.Activate {
		for existingKey, existing := range store.published {
			if existing.TenantID == record.TenantID && existing.Ref.ID == record.ProfileID {
				existing.Active = false
				store.published[existingKey] = existing
			}
		}
	}
	store.published[publishedKey(record.TenantID, record.ProfileID, published.Ref.Version)] = published.Clone()
	store.publishedGenerations[generationKey] = true
	return published.Clone(), nil
}

func (store *fakeProfileStore) Resolve(_ context.Context, tenantID string, id profile.ID, version uint64) (profile.Published, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.calls++
	published, ok := store.published[publishedKey(tenantID, id, version)]
	if !ok {
		return profile.Published{}, false, nil
	}
	return published.Clone(), true, nil
}

func (store *fakeProfileStore) ListPublished(_ context.Context, tenantID string, limit int, cursor string) ([]profile.PublishedSummary, string, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.calls++
	if limit <= 0 || cursor != "" {
		return nil, "", profile.ErrInvalidProfile
	}
	summaries := make([]profile.PublishedSummary, 0, len(store.published))
	for _, published := range store.published {
		if published.TenantID != tenantID {
			continue
		}
		summary := profile.PublishedSummary{
			Ref: published.Ref, Active: published.Active, PublishedAt: published.PublishedAt,
			EngineVersion: published.EngineVersion, SnapshotHash: published.Snapshot.SnapshotHash,
		}
		if published.RevokedAt != nil {
			revokedAt := *published.RevokedAt
			summary.RevokedAt = &revokedAt
		}
		summaries = append(summaries, summary)
	}
	sort.Slice(summaries, func(left, right int) bool {
		if summaries[left].Ref.ID != summaries[right].Ref.ID {
			return summaries[left].Ref.ID < summaries[right].Ref.ID
		}
		return summaries[left].Ref.Version < summaries[right].Ref.Version
	})
	if len(summaries) > limit {
		summaries = summaries[:limit]
	}
	return summaries, "", nil
}

func (store *fakeProfileStore) Activate(_ context.Context, tenantID string, id profile.ID, version uint64, _ string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.calls++
	key := publishedKey(tenantID, id, version)
	target, ok := store.published[key]
	if !ok {
		return profile.ErrInvalidProfile
	}
	if target.RevokedAt != nil {
		return profile.ErrProfileRevoked
	}
	for existingKey, existing := range store.published {
		if existing.TenantID == tenantID && existing.Ref.ID == id {
			existing.Active = existingKey == key
			store.published[existingKey] = existing
		}
	}
	return nil
}

func (store *fakeProfileStore) Revoke(_ context.Context, tenantID string, id profile.ID, version uint64, _ string, revokedAt time.Time) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.calls++
	key := publishedKey(tenantID, id, version)
	target, ok := store.published[key]
	if !ok {
		return profile.ErrInvalidProfile
	}
	target.Active = false
	value := revokedAt
	target.RevokedAt = &value
	store.published[key] = target
	return nil
}

var _ profile.Store = (*fakeProfileStore)(nil)

func newProfileTestHandler(t *testing.T) (http.Handler, *fakeProfileStore, demoConfig) {
	t.Helper()
	cfg := parseTestConfig(t)
	cfg.Profiles.Enabled = true
	cfg.Profiles.TenantID = "local"
	cfg.Profiles.Actor = "operator"
	cfg.Profiles.AdapterRegistryVersion = "registry-v1"
	store := newFakeProfileStore()
	service, err := newProfileAdminService(cfg.Profiles, store, nil, nil)
	require.NoError(t, err)
	cfg.Profiles.service = service
	handler, err := newHandlerWithToolSearch(cfg, []byte("profile-test-bearer"), nil, &recordingToolSearch{})
	require.NoError(t, err)
	return handler, store, cfg
}

func TestAdapterCatalogAPIProjectsOnlySafeReviewedFields(t *testing.T) {
	handler, _, cfg := newProfileTestHandler(t)
	response := serveProfile(handler, profileRequest(cfg, http.MethodGet, "/api/setup/v1/catalog", "", "", nil))
	require.Equal(t, http.StatusOK, response.Code)
	body := response.Body.String()
	for _, forbidden := range []string{"destination", "transport", "secret", "command", "executable", "url"} {
		assert.NotContains(t, body, forbidden)
	}
	var decoded struct {
		RegistryVersion string `json:"registry_version"`
		Adapters        []struct {
			ID               string          `json:"id"`
			Version          string          `json:"version"`
			ToolName         string          `json:"tool_name"`
			ReadOnly         bool            `json:"read_only"`
			ApprovalRequired bool            `json:"approval_required"`
			MaxResultBytes   int             `json:"max_result_bytes"`
			ToolSchema       json.RawMessage `json:"tool_schema"`
		} `json:"adapters"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &decoded))
	assert.Equal(t, "registry-v1", decoded.RegistryVersion)
	require.Len(t, decoded.Adapters, 2)
	assert.Equal(t, "knowledge.search", decoded.Adapters[0].ID)
	assert.Equal(t, "knowledge_search", decoded.Adapters[0].ToolName)
	assert.True(t, decoded.Adapters[0].ReadOnly)
	assert.JSONEq(t, `{"type":"object","properties":{"query":{"type":"string","maxLength":512},"max_results":{"type":"integer","minimum":1,"maximum":10}},"required":["query"],"additionalProperties":false}`, string(decoded.Adapters[0].ToolSchema))
	assert.Equal(t, "web_search", decoded.Adapters[1].ID)
	assert.Equal(t, 256<<10, decoded.Adapters[1].MaxResultBytes, "setup projection must match the deployed worker catalog")
}

func TestAdapterBindingAPIEnablesReviewedAuthorityAndUpdatesRecipes(t *testing.T) {
	handler, _, cfg := newProfileTestHandler(t)
	created := serveProfile(handler, profileRequest(cfg, http.MethodPost, "/api/setup/v1/profiles", "application/json", `{"id":"insurance","description":"Insurance"}`, nil))
	require.Equal(t, http.StatusCreated, created.Code)
	system := serveProfile(handler, profileRequest(cfg, http.MethodPut, "/api/setup/v1/profiles/insurance/templates/workers/policy_followup.j2", "text/plain; charset=utf-8", "Research {{ topic }}", map[string]string{"If-Match": `"1"`}))
	require.Equal(t, http.StatusOK, system.Code)
	recipe := serveProfile(handler, profileRequest(cfg, http.MethodPut, "/api/setup/v1/profiles/insurance/recipes/policy_followup", "application/json", `{"require_approval":false}`, map[string]string{"If-Match": `"2"`}))
	require.Equal(t, http.StatusOK, recipe.Code)

	bound := serveProfile(handler, profileRequest(cfg, http.MethodPut, "/api/setup/v1/profiles/insurance/bindings/knowledge.search", "application/json", `{"max_result_bytes":8192,"require_approval":false}`, map[string]string{"If-Match": `"3"`}))
	require.Equal(t, http.StatusOK, bound.Code, bound.Body.String())
	var draft profileDraftResponse
	require.NoError(t, json.Unmarshal(bound.Body.Bytes(), &draft))
	require.Len(t, draft.Bindings, 1)
	assert.Equal(t, "knowledge.search", draft.Bindings[0].ID)
	require.Len(t, draft.Recipes, 1)
	assert.Equal(t, []string{"knowledge.search"}, draft.Recipes[0].BindingIDs)

	expanded := serveProfile(handler, profileRequest(cfg, http.MethodPut, "/api/setup/v1/profiles/insurance/bindings/knowledge.search", "application/json", `{"max_result_bytes":999999,"require_approval":false}`, map[string]string{"If-Match": `"4"`}))
	assert.Equal(t, http.StatusUnprocessableEntity, expanded.Code)

	removed := serveProfile(handler, profileRequest(cfg, http.MethodDelete, "/api/setup/v1/profiles/insurance/bindings/knowledge.search", "", "", map[string]string{"If-Match": `"4"`}))
	require.Equal(t, http.StatusOK, removed.Code)
	require.NoError(t, json.Unmarshal(removed.Body.Bytes(), &draft))
	assert.Empty(t, draft.Bindings)
	assert.Empty(t, draft.Recipes[0].BindingIDs)
}

func profileRequest(cfg demoConfig, method, path, contentType, body string, headers map[string]string) *http.Request {
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	if method != http.MethodGet {
		request.Header.Set("Origin", cfg.Origin)
	}
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	return request
}

func serveProfile(handler http.Handler, request *http.Request) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func TestProfileAdminConfigRequiresIdentity(t *testing.T) {
	_, err := parseConfig([]string{"-vertical-profiles"})
	require.ErrorContains(t, err, "demo profile identity is required")

	_, err = parseConfig([]string{"-vertical-profiles", "-profile-tenant", "local", "-profile-actor", "operator", "-profile-database-url-env", "lowercase"})
	require.ErrorContains(t, err, "demo profile database environment name is invalid")

	cfg, err := parseConfig([]string{"-vertical-profiles", "-profile-tenant", "local", "-profile-actor", "operator", "-profile-adapter-registry-version", "registry-v1"})
	require.NoError(t, err)
	assert.Equal(t, "LIVE_PROFILE_DATABASE_URL", cfg.Profiles.DatabaseURLEnv)
}

func TestSetupAPIDisabledReturnsNotFound(t *testing.T) {
	cfg := parseTestConfig(t)
	handler, err := newHandlerWithToolSearch(cfg, []byte("profile-test-bearer"), nil, &recordingToolSearch{})
	require.NoError(t, err)
	response := serveProfile(handler, profileRequest(cfg, http.MethodGet, "/api/setup/v1/profiles", "", "", nil))
	assert.Equal(t, http.StatusNotFound, response.Code)
}

func TestSetupAPIRejectsHostileRequestsBeforeStore(t *testing.T) {
	handler, store, cfg := newProfileTestHandler(t)
	cases := []struct {
		name    string
		request *http.Request
		status  int
	}{
		{"wrong origin", func() *http.Request {
			request := profileRequest(cfg, http.MethodPost, "/api/setup/v1/profiles", "application/json", `{"id":"insurance"}`, nil)
			request.Header.Set("Origin", "https://evil.invalid")
			return request
		}(), http.StatusForbidden},
		{"missing origin", func() *http.Request {
			request := profileRequest(cfg, http.MethodPost, "/api/setup/v1/profiles", "application/json", `{"id":"insurance"}`, nil)
			request.Header.Del("Origin")
			return request
		}(), http.StatusForbidden},
		{"method", profileRequest(cfg, http.MethodDelete, "/api/setup/v1/profiles", "application/json", "", nil), http.StatusMethodNotAllowed},
		{"media", profileRequest(cfg, http.MethodPost, "/api/setup/v1/profiles", "text/plain", `{"id":"insurance"}`, nil), http.StatusUnsupportedMediaType},
		{"unknown field", profileRequest(cfg, http.MethodPost, "/api/setup/v1/profiles", "application/json", `{"id":"insurance","tenant":"stolen"}`, nil), http.StatusBadRequest},
		{"duplicate key", profileRequest(cfg, http.MethodPost, "/api/setup/v1/profiles", "application/json", `{"id":"a","id":"b"}`, nil), http.StatusBadRequest},
		{"trailing content", profileRequest(cfg, http.MethodPost, "/api/setup/v1/profiles", "application/json", `{"id":"insurance"}{}`, nil), http.StatusBadRequest},
		{"oversize", profileRequest(cfg, http.MethodPost, "/api/setup/v1/profiles", "application/json", `{"id":"`+strings.Repeat("x", maximumProfileJSONBytes)+`"}`, nil), http.StatusRequestEntityTooLarge},
		{"dot segment", profileRequest(cfg, http.MethodGet, "/api/setup/v1/profiles/insurance/templates/system.j2/../other.j2", "", "", nil), http.StatusNotFound},
		{"reserved builtin", profileRequest(cfg, http.MethodPost, "/api/setup/v1/profiles", "application/json", `{"id":"builtin"}`, nil), http.StatusUnprocessableEntity},
		{"unknown resource", profileRequest(cfg, http.MethodGet, "/api/setup/v1/other", "", "", nil), http.StatusNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.status, serveProfile(handler, tc.request).Code)
		})
	}
	assert.Zero(t, store.Calls())
}

func TestProfileDraftLifecycle(t *testing.T) {
	handler, _, cfg := newProfileTestHandler(t)

	created := serveProfile(handler, profileRequest(cfg, http.MethodPost, "/api/setup/v1/profiles", "application/json", `{"id":"insurance","description":"Insurance renewal sales"}`, nil))
	require.Equal(t, http.StatusCreated, created.Code)
	assert.Equal(t, `"1"`, created.Header().Get("ETag"))
	var draft profileDraftResponse
	require.NoError(t, json.Unmarshal(created.Body.Bytes(), &draft))
	assert.Equal(t, "insurance", draft.ID)
	assert.EqualValues(t, 1, draft.Generation)
	assert.Equal(t, "Insurance renewal sales", draft.Description)
	require.Len(t, draft.Templates, 1)
	assert.Equal(t, "system.j2", draft.Templates[0].Path)
	require.Len(t, draft.Fixtures, 1)
	assert.Equal(t, "default.json", draft.Fixtures[0].Path)

	duplicate := serveProfile(handler, profileRequest(cfg, http.MethodPost, "/api/setup/v1/profiles", "application/json", `{"id":"insurance"}`, nil))
	assert.Equal(t, http.StatusConflict, duplicate.Code)
	assert.JSONEq(t, `{"error":{"code":"stale_draft"}}`, duplicate.Body.String())

	loaded := serveProfile(handler, profileRequest(cfg, http.MethodGet, "/api/setup/v1/profiles/insurance/draft", "", "", nil))
	require.Equal(t, http.StatusOK, loaded.Code)
	assert.Equal(t, `"1"`, loaded.Header().Get("ETag"))

	missingIfMatch := serveProfile(handler, profileRequest(cfg, http.MethodPut, "/api/setup/v1/profiles/insurance/draft", "application/json", `{"description":"updated"}`, nil))
	assert.Equal(t, http.StatusPreconditionRequired, missingIfMatch.Code)

	stale := serveProfile(handler, profileRequest(cfg, http.MethodPut, "/api/setup/v1/profiles/insurance/draft", "application/json", `{"description":"stale"}`, map[string]string{"If-Match": `"9"`}))
	assert.Equal(t, http.StatusConflict, stale.Code)
	assert.JSONEq(t, `{"error":{"code":"stale_draft"}}`, stale.Body.String())

	saved := serveProfile(handler, profileRequest(cfg, http.MethodPut, "/api/setup/v1/profiles/insurance/draft", "application/json", `{"description":"updated"}`, map[string]string{"If-Match": `"1"`}))
	require.Equal(t, http.StatusOK, saved.Code)
	assert.Equal(t, `"2"`, saved.Header().Get("ETag"))
	require.NoError(t, json.Unmarshal(saved.Body.Bytes(), &draft))
	assert.Equal(t, "updated", draft.Description)

	reread := serveProfile(handler, profileRequest(cfg, http.MethodGet, "/api/setup/v1/profiles/insurance/draft", "", "", nil))
	require.Equal(t, http.StatusOK, reread.Code)
	require.NoError(t, json.Unmarshal(reread.Body.Bytes(), &draft))
	assert.Equal(t, "updated", draft.Description)

	absent := serveProfile(handler, profileRequest(cfg, http.MethodGet, "/api/setup/v1/profiles/unknown/draft", "", "", nil))
	assert.Equal(t, http.StatusNotFound, absent.Code)
}

func TestProfileTemplateSourceLifecycle(t *testing.T) {
	handler, _, cfg := newProfileTestHandler(t)
	created := serveProfile(handler, profileRequest(cfg, http.MethodPost, "/api/setup/v1/profiles", "application/json", `{"id":"insurance"}`, nil))
	require.Equal(t, http.StatusCreated, created.Code)

	source := serveProfile(handler, profileRequest(cfg, http.MethodGet, "/api/setup/v1/profiles/insurance/templates/system.j2", "", "", nil))
	require.Equal(t, http.StatusOK, source.Code)
	assert.Equal(t, defaultSystemTemplate, source.Body.String())
	assert.Contains(t, source.Header().Get("Content-Type"), "text/plain")

	missingIfMatch := serveProfile(handler, profileRequest(cfg, http.MethodPut, "/api/setup/v1/profiles/insurance/templates/opening_message.j2", "text/plain; charset=utf-8", "Welcome to the insurance desk.", nil))
	assert.Equal(t, http.StatusPreconditionRequired, missingIfMatch.Code)

	putTemplate := serveProfile(handler, profileRequest(cfg, http.MethodPut, "/api/setup/v1/profiles/insurance/templates/opening_message.j2", "text/plain; charset=utf-8", "Welcome to the insurance desk.", map[string]string{"If-Match": `"1"`}))
	require.Equal(t, http.StatusOK, putTemplate.Code)
	assert.Equal(t, `"2"`, putTemplate.Header().Get("ETag"))

	opening := serveProfile(handler, profileRequest(cfg, http.MethodGet, "/api/setup/v1/profiles/insurance/templates/opening_message.j2", "", "", nil))
	require.Equal(t, http.StatusOK, opening.Code)
	assert.Equal(t, "Welcome to the insurance desk.", opening.Body.String())

	invalidName := serveProfile(handler, profileRequest(cfg, http.MethodPut, "/api/setup/v1/profiles/insurance/templates/System.j2", "text/plain; charset=utf-8", "x", map[string]string{"If-Match": `"2"`}))
	assert.Equal(t, http.StatusNotFound, invalidName.Code)

	oversize := serveProfile(handler, profileRequest(cfg, http.MethodPut, "/api/setup/v1/profiles/insurance/templates/big.j2", "text/plain; charset=utf-8", strings.Repeat("x", maximumProfileTemplateBytes+1), map[string]string{"If-Match": `"2"`}))
	assert.Equal(t, http.StatusRequestEntityTooLarge, oversize.Code)

	deleted := serveProfile(handler, profileRequest(cfg, http.MethodDelete, "/api/setup/v1/profiles/insurance/templates/opening_message.j2", "", "", map[string]string{"If-Match": `"2"`}))
	assert.Equal(t, http.StatusNoContent, deleted.Code)

	gone := serveProfile(handler, profileRequest(cfg, http.MethodGet, "/api/setup/v1/profiles/insurance/templates/opening_message.j2", "", "", nil))
	assert.Equal(t, http.StatusNotFound, gone.Code)

	protectedSystem := serveProfile(handler, profileRequest(cfg, http.MethodDelete, "/api/setup/v1/profiles/insurance/templates/system.j2", "", "", map[string]string{"If-Match": `"3"`}))
	assert.Equal(t, http.StatusUnprocessableEntity, protectedSystem.Code)
}

func TestProfilePublishActivateRevokeFlow(t *testing.T) {
	handler, _, cfg := newProfileTestHandler(t)
	created := serveProfile(handler, profileRequest(cfg, http.MethodPost, "/api/setup/v1/profiles", "application/json", `{"id":"insurance"}`, nil))
	require.Equal(t, http.StatusCreated, created.Code)

	validated := serveProfile(handler, profileRequest(cfg, http.MethodPost, "/api/setup/v1/profiles/insurance/validate", "application/json", `{"expected_generation":1}`, nil))
	assert.Equal(t, http.StatusNoContent, validated.Code)

	staleValidate := serveProfile(handler, profileRequest(cfg, http.MethodPost, "/api/setup/v1/profiles/insurance/validate", "application/json", `{"expected_generation":7}`, nil))
	assert.Equal(t, http.StatusConflict, staleValidate.Code)

	preview := serveProfile(handler, profileRequest(cfg, http.MethodPost, "/api/setup/v1/profiles/insurance/preview", "application/json", `{"expected_generation":1}`, nil))
	require.Equal(t, http.StatusOK, preview.Code)
	var previewBody profilePreviewResponse
	require.NoError(t, json.Unmarshal(preview.Body.Bytes(), &previewBody))
	assert.NotEmpty(t, previewBody.SnapshotHash)
	assert.Equal(t, defaultSystemTemplate, previewBody.Outputs["default.json"])

	published := serveProfile(handler, profileRequest(cfg, http.MethodPost, "/api/setup/v1/profiles/insurance/publish", "application/json", `{"expected_generation":1,"activate":true}`, nil))
	require.Equal(t, http.StatusCreated, published.Code)
	var publishBody profilePublishResponse
	require.NoError(t, json.Unmarshal(published.Body.Bytes(), &publishBody))
	assert.Equal(t, "insurance", publishBody.ID)
	assert.EqualValues(t, 1, publishBody.Version)
	assert.True(t, publishBody.Active)
	assert.Equal(t, previewBody.SnapshotHash, publishBody.SnapshotHash)

	republish := serveProfile(handler, profileRequest(cfg, http.MethodPost, "/api/setup/v1/profiles/insurance/publish", "application/json", `{"expected_generation":1}`, nil))
	assert.Equal(t, http.StatusConflict, republish.Code)

	list := serveProfile(handler, profileRequest(cfg, http.MethodGet, "/api/setup/v1/profiles?state=published", "", "", nil))
	require.Equal(t, http.StatusOK, list.Code)
	var listBody publishedProfileList
	require.NoError(t, json.Unmarshal(list.Body.Bytes(), &listBody))
	require.Len(t, listBody.Profiles, 1)
	assert.Equal(t, "insurance", listBody.Profiles[0].ID)
	assert.True(t, listBody.Profiles[0].Active)
	assert.False(t, listBody.Profiles[0].Revoked)

	revoked := serveProfile(handler, profileRequest(cfg, http.MethodPost, "/api/setup/v1/profiles/insurance/revoke", "application/json", `{"version":1}`, nil))
	assert.Equal(t, http.StatusNoContent, revoked.Code)

	activateRevoked := serveProfile(handler, profileRequest(cfg, http.MethodPost, "/api/setup/v1/profiles/insurance/activate", "application/json", `{"version":1}`, nil))
	assert.Equal(t, http.StatusConflict, activateRevoked.Code)

	listAfter := serveProfile(handler, profileRequest(cfg, http.MethodGet, "/api/setup/v1/profiles?state=published", "", "", nil))
	require.Equal(t, http.StatusOK, listAfter.Code)
	require.NoError(t, json.Unmarshal(listAfter.Body.Bytes(), &listBody))
	require.Len(t, listBody.Profiles, 1)
	assert.True(t, listBody.Profiles[0].Revoked)
	assert.False(t, listBody.Profiles[0].Active)
}

func TestProfileFixtureLifecycle(t *testing.T) {
	handler, _, cfg := newProfileTestHandler(t)
	created := serveProfile(handler, profileRequest(cfg, http.MethodPost, "/api/setup/v1/profiles", "application/json", `{"id":"insurance"}`, nil))
	require.Equal(t, http.StatusCreated, created.Code)

	fixture := `{"template":"system.j2","context":{},"expected":"` + defaultSystemTemplate + `"}`
	putFixture := serveProfile(handler, profileRequest(cfg, http.MethodPut, "/api/setup/v1/profiles/insurance/fixtures/extra.json", "application/json", fixture, map[string]string{"If-Match": `"1"`}))
	require.Equal(t, http.StatusOK, putFixture.Code)
	assert.Equal(t, `"2"`, putFixture.Header().Get("ETag"))

	readBack := serveProfile(handler, profileRequest(cfg, http.MethodGet, "/api/setup/v1/profiles/insurance/fixtures/extra.json", "", "", nil))
	require.Equal(t, http.StatusOK, readBack.Code)
	assert.JSONEq(t, fixture, readBack.Body.String())

	preview := serveProfile(handler, profileRequest(cfg, http.MethodPost, "/api/setup/v1/profiles/insurance/preview", "application/json", `{"expected_generation":2}`, nil))
	require.Equal(t, http.StatusOK, preview.Code)
	var previewBody profilePreviewResponse
	require.NoError(t, json.Unmarshal(preview.Body.Bytes(), &previewBody))
	assert.Len(t, previewBody.Outputs, 2)

	deleted := serveProfile(handler, profileRequest(cfg, http.MethodDelete, "/api/setup/v1/profiles/insurance/fixtures/extra.json", "", "", map[string]string{"If-Match": `"2"`}))
	assert.Equal(t, http.StatusOK, deleted.Code)

	invalidName := serveProfile(handler, profileRequest(cfg, http.MethodPut, "/api/setup/v1/profiles/insurance/fixtures/Bad.JSON", "application/json", fixture, map[string]string{"If-Match": `"3"`}))
	assert.Equal(t, http.StatusNotFound, invalidName.Code)
}

func TestWorkerRecipeLifecycleLinksObjectiveTemplate(t *testing.T) {
	handler, _, cfg := newProfileTestHandler(t)
	created := serveProfile(handler, profileRequest(cfg, http.MethodPost, "/api/setup/v1/profiles", "application/json", `{"id":"insurance"}`, nil))
	require.Equal(t, http.StatusCreated, created.Code)

	// A recipe cannot exist before its objective template.
	missing := serveProfile(handler, profileRequest(cfg, http.MethodPut, "/api/setup/v1/profiles/insurance/recipes/lead_research", "application/json", `{"require_approval":false}`, map[string]string{"If-Match": `"1"`}))
	assert.Equal(t, http.StatusUnprocessableEntity, missing.Code)
	assert.JSONEq(t, `{"error":{"code":"objective_template_missing"}}`, missing.Body.String())

	objective := serveProfile(handler, profileRequest(cfg, http.MethodPut, "/api/setup/v1/profiles/insurance/templates/workers/lead_research.j2", "text/plain; charset=utf-8", "Research the prospect's stated needs and cite every source.", map[string]string{"If-Match": `"1"`}))
	require.Equal(t, http.StatusOK, objective.Code)

	saved := serveProfile(handler, profileRequest(cfg, http.MethodPut, "/api/setup/v1/profiles/insurance/recipes/lead_research", "application/json",
		`{"require_approval":true,"execution_timeout_ms":60000,"tool_timeout_ms":15000,"overall_timeout_ms":120000,"max_final_result_bytes":16384}`,
		map[string]string{"If-Match": `"2"`}))
	require.Equal(t, http.StatusOK, saved.Code)
	assert.Equal(t, `"3"`, saved.Header().Get("ETag"))
	var draft profileDraftResponse
	require.NoError(t, json.Unmarshal(saved.Body.Bytes(), &draft))
	require.Len(t, draft.Recipes, 1)
	assert.Equal(t, "lead_research", draft.Recipes[0].ID)
	assert.Equal(t, "workers/lead_research.j2", draft.Recipes[0].Objective)
	assert.True(t, draft.Recipes[0].RequireApproval)
	assert.EqualValues(t, 60000, draft.Recipes[0].ExecutionTimeoutMS)
	assert.EqualValues(t, 1, draft.Recipes[0].Version)

	// The publication pipeline accepts the linked recipe.
	published := serveProfile(handler, profileRequest(cfg, http.MethodPost, "/api/setup/v1/profiles/insurance/publish", "application/json", `{"expected_generation":3,"activate":true}`, nil))
	require.Equal(t, http.StatusCreated, published.Code)

	// Re-saving advances the recipe version, not the identity.
	resaved := serveProfile(handler, profileRequest(cfg, http.MethodPut, "/api/setup/v1/profiles/insurance/recipes/lead_research", "application/json", `{"require_approval":false}`, map[string]string{"If-Match": `"3"`}))
	require.Equal(t, http.StatusOK, resaved.Code)
	require.NoError(t, json.Unmarshal(resaved.Body.Bytes(), &draft))
	require.Len(t, draft.Recipes, 1)
	assert.EqualValues(t, 2, draft.Recipes[0].Version)
	assert.False(t, draft.Recipes[0].RequireApproval)

	deleted := serveProfile(handler, profileRequest(cfg, http.MethodDelete, "/api/setup/v1/profiles/insurance/recipes/lead_research", "", "", map[string]string{"If-Match": `"4"`}))
	assert.Equal(t, http.StatusNoContent, deleted.Code)
	reread := serveProfile(handler, profileRequest(cfg, http.MethodGet, "/api/setup/v1/profiles/insurance/draft", "", "", nil))
	require.NoError(t, json.Unmarshal(reread.Body.Bytes(), &draft))
	assert.Empty(t, draft.Recipes)
}

func TestWorkerRecipeRejectsHostileFieldsAndBounds(t *testing.T) {
	handler, store, cfg := newProfileTestHandler(t)
	require.Equal(t, http.StatusCreated, serveProfile(handler, profileRequest(cfg, http.MethodPost, "/api/setup/v1/profiles", "application/json", `{"id":"insurance"}`, nil)).Code)
	require.Equal(t, http.StatusOK, serveProfile(handler, profileRequest(cfg, http.MethodPut, "/api/setup/v1/profiles/insurance/templates/workers/research.j2", "text/plain; charset=utf-8", "Research.", map[string]string{"If-Match": `"1"`})).Code)
	before := store.Calls()

	for name, body := range map[string]string{
		"server owned objective": `{"objective":"workers/other.j2"}`,
		"server owned version":   `{"version":9}`,
		"binding injection":      `{"binding_ids":["web_search"]}`,
		"tenant injection":       `{"tenant_id":"stolen"}`,
		"string timeout":         `{"execution_timeout_ms":"60000"}`,
	} {
		t.Run(name, func(t *testing.T) {
			response := serveProfile(handler, profileRequest(cfg, http.MethodPut, "/api/setup/v1/profiles/insurance/recipes/research", "application/json", body, map[string]string{"If-Match": `"2"`}))
			assert.Equal(t, http.StatusBadRequest, response.Code)
		})
	}
	assert.Equal(t, before, store.Calls(), "hostile recipe bodies must be rejected before any store call")

	for name, body := range map[string]string{
		"negative timeout": `{"execution_timeout_ms":-1}`,
		"timeout too long": `{"overall_timeout_ms":90000000}`,
		"negative bytes":   `{"max_final_result_bytes":-1}`,
		"bytes too large":  `{"max_evidence_bytes":999999999}`,
	} {
		t.Run(name, func(t *testing.T) {
			response := serveProfile(handler, profileRequest(cfg, http.MethodPut, "/api/setup/v1/profiles/insurance/recipes/research", "application/json", body, map[string]string{"If-Match": `"2"`}))
			assert.Equal(t, http.StatusUnprocessableEntity, response.Code)
		})
	}

	badID := serveProfile(handler, profileRequest(cfg, http.MethodPut, "/api/setup/v1/profiles/insurance/recipes/Bad%20ID", "application/json", `{}`, map[string]string{"If-Match": `"2"`}))
	assert.Equal(t, http.StatusNotFound, badID.Code)

	noIfMatch := serveProfile(handler, profileRequest(cfg, http.MethodPut, "/api/setup/v1/profiles/insurance/recipes/research", "application/json", `{}`, nil))
	assert.Equal(t, http.StatusPreconditionRequired, noIfMatch.Code)
}

// A deployment with no knowledge store must still fail closed on a manifest
// that pins knowledge, rather than publishing material it could never resolve.
func TestPublicationWithoutAKnowledgeStoreRejectsPinnedKnowledge(t *testing.T) {
	cfg := parseTestConfig(t)
	cfg.Profiles.Enabled = true
	cfg.Profiles.TenantID = "local"
	cfg.Profiles.Actor = "operator"
	cfg.Profiles.AdapterRegistryVersion = "registry-v1"
	store := newFakeProfileStore()
	service, err := newProfileAdminService(cfg.Profiles, store, nil, nil)
	require.NoError(t, err)

	draft, err := service.seedDraft(t.Context(), profile.ID("insurance"), "Insurance")
	require.NoError(t, err)

	pinned := draft.Manifest.Clone()
	pinned.Knowledge = []profile.KnowledgeRef{{ID: "policy", Version: 1, Hash: strings.Repeat("a", 64)}}
	pinned.Description = "pins knowledge"
	updated := draft.Clone()
	updated.Manifest = pinned
	updated.UpdatedAt = service.now()
	saved, err := store.SaveDraft(t.Context(), updated, draft.Generation)
	require.NoError(t, err)

	_, err = service.publisher.Publish(t.Context(), profile.PublishCommand{
		TenantID: "local", ProfileID: profile.ID("insurance"),
		ExpectedGeneration: saved.Generation, Actor: "operator",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, profile.ErrProfileIncompatible,
		"without a validator the publisher accepts only an empty knowledge list")
}
