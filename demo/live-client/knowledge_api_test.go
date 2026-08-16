package main

import (
	"bytes"
	"context"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/knowledge"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func knowledgeUpload(t *testing.T, filename string, body []byte) (string, string) {
	t.Helper()
	var buffer bytes.Buffer
	writer := multipart.NewWriter(&buffer)
	part, err := writer.CreateFormFile("document", filename)
	require.NoError(t, err)
	_, err = part.Write(body)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return writer.FormDataContentType(), buffer.String()
}

func knowledgeRequest(cfg demoConfig, method, path, contentType, body string) *http.Request {
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	if method != http.MethodGet {
		request.Header.Set("Origin", cfg.Origin)
	}
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	return request
}

func TestKnowledgeAPIIsAbsentWithoutAKnowledgeStore(t *testing.T) {
	handler, _, cfg := newProfileTestHandler(t)
	contentType, body := knowledgeUpload(t, "policy.md", []byte("# Coverage\n\nwaiting period"))

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, knowledgeRequest(cfg, http.MethodPost,
		"/api/setup/v1/knowledge/policy", contentType, body))
	assert.Equal(t, http.StatusNotFound, response.Code,
		"a demo without knowledge storage must not expose the endpoints at all")
}

func TestKnowledgeUploadRejectsHostileRequestsBeforeAnyStoreCall(t *testing.T) {
	handler, store, cfg := newKnowledgeTestHandler(t)
	pdf, pdfBody := knowledgeUpload(t, "policy.md", []byte("%PDF-1.7\n%\xE2\xE3\xCF\xD3"))
	textType, textBody := knowledgeUpload(t, "policy.md", []byte("# Coverage\n\nwaiting period"))

	for name, test := range map[string]struct {
		method      string
		path        string
		contentType string
		body        string
		want        int
	}{
		"binary content in a text file": {http.MethodPost, "/api/setup/v1/knowledge/policy", pdf, pdfBody, http.StatusUnprocessableEntity},
		"unsupported extension": func() struct {
			method      string
			path        string
			contentType string
			body        string
			want        int
		} {
			contentType, body := knowledgeUpload(t, "policy.pdf", []byte("plain text"))
			return struct {
				method      string
				path        string
				contentType string
				body        string
				want        int
			}{http.MethodPost, "/api/setup/v1/knowledge/policy", contentType, body, http.StatusUnprocessableEntity}
		}(),
		"traversal pack id":  {http.MethodPost, "/api/setup/v1/knowledge/..", textType, textBody, http.StatusNotFound},
		"reserved pack id":   {http.MethodPost, "/api/setup/v1/knowledge/builtin", textType, textBody, http.StatusNotFound},
		"json not multipart": {http.MethodPost, "/api/setup/v1/knowledge/policy", "application/json", `{"a":1}`, http.StatusUnsupportedMediaType},
		"wrong method":       {http.MethodPut, "/api/setup/v1/knowledge/policy", textType, textBody, http.StatusMethodNotAllowed},
	} {
		t.Run(name, func(t *testing.T) {
			before := store.publishes
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, knowledgeRequest(cfg, test.method, test.path, test.contentType, test.body))
			assert.Equal(t, test.want, response.Code)
			assert.Equal(t, before, store.publishes, "a rejected upload must never reach the store")
		})
	}

	// A cross-origin write is refused before anything is parsed.
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/setup/v1/knowledge/policy", strings.NewReader(textBody))
	request.Header.Set("Origin", "http://evil.example")
	request.Header.Set("Content-Type", textType)
	handler.ServeHTTP(response, request)
	assert.Equal(t, http.StatusForbidden, response.Code)
}

func TestKnowledgeUploadNeverEchoesTheFilename(t *testing.T) {
	handler, _, cfg := newKnowledgeTestHandler(t)
	contentType, body := knowledgeUpload(t, "secret-customer-name.pdf", []byte("plain text"))

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, knowledgeRequest(cfg, http.MethodPost,
		"/api/setup/v1/knowledge/policy", contentType, body))
	require.Equal(t, http.StatusUnprocessableEntity, response.Code)
	assert.NotContains(t, response.Body.String(), "secret-customer-name",
		"an uploaded filename is customer data and must not round-trip into an error")
}

func TestKnowledgePreviewThenPublishUsesTheExactPreviewedHash(t *testing.T) {
	handler, store, cfg := newKnowledgeTestHandler(t)
	contentType, body := knowledgeUpload(t, "policy.md", []byte("# Coverage\n\nwaiting period is thirty days"))

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, knowledgeRequest(cfg, http.MethodPost,
		"/api/setup/v1/knowledge/policy", contentType, body))
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())

	var preview struct {
		PackID    string `json:"pack_id"`
		Hash      string `json:"hash"`
		MediaType string `json:"media_type"`
		Excerpts  []struct {
			Ordinal int    `json:"ordinal"`
			Text    string `json:"text"`
			Section string `json:"section"`
		} `json:"excerpts"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &preview))
	assert.Equal(t, "policy", preview.PackID)
	assert.Len(t, preview.Hash, 64)
	assert.Equal(t, "text/markdown", preview.MediaType)
	require.NotEmpty(t, preview.Excerpts)
	assert.Equal(t, 0, preview.Excerpts[0].Ordinal)
	assert.Zero(t, store.publishes, "previewing is not publishing")

	// Publishing names the previewed hash and the same bytes.
	publishType, publishBody := knowledgeUploadWithHash(t, "policy.md",
		[]byte("# Coverage\n\nwaiting period is thirty days"), preview.Hash)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, knowledgeRequest(cfg, http.MethodPost,
		"/api/setup/v1/knowledge/policy/publish", publishType, publishBody))
	require.Equal(t, http.StatusCreated, response.Code, response.Body.String())
	assert.Equal(t, 1, store.publishes)

	var published struct {
		PackID  string `json:"pack_id"`
		Version uint64 `json:"version"`
		Hash    string `json:"hash"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &published))
	assert.Equal(t, uint64(1), published.Version)
	assert.Equal(t, preview.Hash, published.Hash)

	// A hash the operator never previewed is refused.
	staleType, staleBody := knowledgeUploadWithHash(t, "policy.md",
		[]byte("# Coverage\n\nwaiting period is sixty days"), preview.Hash)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, knowledgeRequest(cfg, http.MethodPost,
		"/api/setup/v1/knowledge/policy/publish", staleType, staleBody))
	assert.Equal(t, http.StatusUnprocessableEntity, response.Code)
	assert.Equal(t, 1, store.publishes)
}

func TestKnowledgeListAndTombstoneExposeOnlySafeFields(t *testing.T) {
	handler, store, cfg := newKnowledgeTestHandler(t)
	published := store.seed(t, "policy", "waiting period is thirty days")

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, knowledgeRequest(cfg, http.MethodGet, "/api/setup/v1/knowledge/policy", "", ""))
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())

	var list struct {
		Versions []map[string]any `json:"versions"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &list))
	require.Len(t, list.Versions, 1)
	keys := make([]string, 0, len(list.Versions[0]))
	for key := range list.Versions[0] {
		keys = append(keys, key)
	}
	assert.ElementsMatch(t, []string{"version", "hash", "title", "excerpt_count", "published_at", "tombstoned"}, keys,
		"listing exposes metadata only, never excerpt text")

	response = httptest.NewRecorder()
	handler.ServeHTTP(response, knowledgeRequest(cfg, http.MethodPost,
		"/api/setup/v1/knowledge/policy/1/tombstone", "", ""))
	require.Equal(t, http.StatusNoContent, response.Code)
	assert.Equal(t, 1, store.tombstones)

	response = httptest.NewRecorder()
	handler.ServeHTTP(response, knowledgeRequest(cfg, http.MethodPost,
		"/api/setup/v1/knowledge/policy/0/tombstone", "", ""))
	assert.Equal(t, http.StatusNotFound, response.Code)
	assert.Equal(t, "policy", published.PackID)
}

func TestKnowledgeUploadIsBoundedBeforeReadingTheWholeBody(t *testing.T) {
	handler, store, cfg := newKnowledgeTestHandler(t)
	contentType, body := knowledgeUpload(t, "policy.txt", bytes.Repeat([]byte("a"), knowledge.MaxUploadBytes+1024))

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, knowledgeRequest(cfg, http.MethodPost,
		"/api/setup/v1/knowledge/policy", contentType, body))
	assert.Equal(t, http.StatusRequestEntityTooLarge, response.Code)
	assert.Zero(t, store.publishes)
}

func knowledgeUploadWithHash(t *testing.T, filename string, body []byte, hash string) (string, string) {
	t.Helper()
	var buffer bytes.Buffer
	writer := multipart.NewWriter(&buffer)
	require.NoError(t, writer.WriteField("hash", hash))
	part, err := writer.CreateFormFile("document", filename)
	require.NoError(t, err)
	_, err = part.Write(body)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return writer.FormDataContentType(), buffer.String()
}

// fakeKnowledgeStore records writes so a test can prove a rejected request
// never reached storage.
type fakeKnowledgeStore struct {
	versions   map[string][]knowledge.Version
	publishes  int
	tombstones int
}

func newFakeKnowledgeStore() *fakeKnowledgeStore {
	return &fakeKnowledgeStore{versions: map[string][]knowledge.Version{}}
}

func (store *fakeKnowledgeStore) seed(t *testing.T, packID, body string) knowledge.VersionRef {
	t.Helper()
	excerpt := knowledge.Excerpt{
		Ordinal: 0, Text: body, Tokens: knowledge.Tokenize(body),
		Citation: knowledge.Citation{SourceTitle: packID + ".md", ExcerptHash: knowledge.ExcerptHash(body)},
	}
	version, err := store.Publish(t.Context(), knowledge.PublishRecord{
		TenantID: "local", PackID: packID, Title: packID + ".md", MediaType: "text/markdown",
		Excerpts: []knowledge.Excerpt{excerpt}, PublishedAt: knowledgeTestTime(), PublishedBy: "operator",
	})
	require.NoError(t, err)
	store.publishes = 0
	return version.Ref
}

func knowledgeTestTime() time.Time { return time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC) }

func (store *fakeKnowledgeStore) CreateDraft(_ context.Context, draft knowledge.Draft) (knowledge.Draft, error) {
	return draft, nil
}

func (store *fakeKnowledgeStore) Publish(_ context.Context, record knowledge.PublishRecord) (knowledge.Version, error) {
	store.publishes++
	next := uint64(len(store.versions[record.PackID]) + 1)
	version := knowledge.Version{
		TenantID: record.TenantID, Ref: knowledge.VersionRef{PackID: record.PackID, Version: next},
		Title: record.Title, MediaType: record.MediaType, Excerpts: record.Excerpts,
		PublishedAt: record.PublishedAt, PublishedBy: record.PublishedBy,
	}
	version.Ref.Hash = knowledge.ContentHash(version)
	if err := version.Validate(); err != nil {
		return knowledge.Version{}, err
	}
	store.versions[record.PackID] = append(store.versions[record.PackID], version)
	return version, nil
}

func (store *fakeKnowledgeStore) Resolve(_ context.Context, tenantID string, ref knowledge.VersionRef) (knowledge.Version, bool, error) {
	for _, version := range store.versions[ref.PackID] {
		if version.TenantID == tenantID && version.Ref == ref {
			return version, true, nil
		}
	}
	return knowledge.Version{}, false, nil
}

func (store *fakeKnowledgeStore) List(_ context.Context, tenantID, packID string, _ int) ([]knowledge.Version, error) {
	versions := make([]knowledge.Version, 0, len(store.versions[packID]))
	for index := len(store.versions[packID]) - 1; index >= 0; index-- {
		if store.versions[packID][index].TenantID == tenantID {
			versions = append(versions, store.versions[packID][index])
		}
	}
	return versions, nil
}

func (store *fakeKnowledgeStore) Tombstone(_ context.Context, tenantID string, ref knowledge.VersionRef, actor string, at time.Time) error {
	for index, version := range store.versions[ref.PackID] {
		if version.TenantID == tenantID && version.Ref.Version == ref.Version && version.TombstonedAt == nil {
			moment := at
			store.versions[ref.PackID][index].TombstonedAt = &moment
			store.versions[ref.PackID][index].TombstonedBy = actor
			store.tombstones++
			return nil
		}
	}
	return knowledge.ErrKnowledgeNotFound
}

func newKnowledgeTestHandler(t *testing.T) (http.Handler, *fakeKnowledgeStore, demoConfig) {
	t.Helper()
	cfg := parseTestConfig(t)
	cfg.Profiles.Enabled = true
	cfg.Profiles.TenantID = "local"
	cfg.Profiles.Actor = "operator"
	knowledgeStore := newFakeKnowledgeStore()
	service, err := newProfileAdminService(cfg.Profiles, newFakeProfileStore(), knowledgeStore, nil)
	require.NoError(t, err)
	cfg.Profiles.service = service
	handler, err := newHandlerWithToolSearch(cfg, []byte("knowledge-test-bearer"), nil, &recordingToolSearch{})
	require.NoError(t, err)
	return handler, knowledgeStore, cfg
}
