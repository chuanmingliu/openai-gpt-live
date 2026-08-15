package profile

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/prompttmpl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testTenant  = "tenant-a"
	testProfile = ID("insurance")
	testActor   = "operator"
)

var testPublicationTime = time.Date(2026, time.August, 15, 9, 30, 0, 123456000, time.UTC)

type publisherStoreFake struct {
	draft         Draft
	draftOK       bool
	blobs         map[string]Blob
	nextVersion   uint64
	loadDraftHook func()
	loadBlobHook  func()
	nextHook      func()
	publishHook   func()
	resolved      Published
	resolveOK     bool
	resolveHook   func()
	activateHook  func()
	revokeHook    func()

	loadDraftErr   error
	loadBlobErr    error
	nextVersionErr error
	publishErr     error
	resolveErr     error
	activateErr    error
	revokeErr      error

	loadDraftCalls int
	loadBlobCalls  int
	nextCalls      int
	publishCalls   int
	resolveCalls   int
	activateCalls  int
	revokeCalls    int
	published      PublishRecord
	activated      struct {
		tenant, actor string
		ref           VersionRef
	}
	revoked struct {
		tenant, actor string
		ref           VersionRef
		at            time.Time
	}
}

func (s *publisherStoreFake) PutBlob(context.Context, string, Blob) error { return nil }

func (s *publisherStoreFake) LoadBlob(_ context.Context, tenantID, hash string) (Blob, bool, error) {
	s.loadBlobCalls++
	if s.loadBlobHook != nil {
		s.loadBlobHook()
	}
	if s.loadBlobErr != nil {
		return Blob{}, false, s.loadBlobErr
	}
	if tenantID != testTenant && tenantID != "published-tenant" {
		return Blob{}, false, nil
	}
	blob, ok := s.blobs[hash]
	return blob, ok, nil // deliberately do not clone: the caller owns that boundary
}

func (s *publisherStoreFake) LoadDraft(_ context.Context, tenantID string, id ID) (Draft, bool, error) {
	s.loadDraftCalls++
	if s.loadDraftHook != nil {
		s.loadDraftHook()
	}
	if s.loadDraftErr != nil {
		return Draft{}, false, s.loadDraftErr
	}
	if !s.draftOK || tenantID != s.draft.TenantID || id != s.draft.ID {
		return Draft{}, false, nil
	}
	return s.draft, true, nil // deliberately retain aliases to exercise publisher ownership
}

func (s *publisherStoreFake) SaveDraft(context.Context, Draft, int64) (Draft, error) {
	return Draft{}, errors.New("unexpected mutation")
}

func (s *publisherStoreFake) SaveTemplate(context.Context, string, ID, string, []byte, int64, string) (Draft, error) {
	return Draft{}, errors.New("unexpected mutation")
}

func (s *publisherStoreFake) DeleteTemplate(context.Context, string, ID, string, int64, string) (Draft, error) {
	return Draft{}, errors.New("unexpected mutation")
}

func (s *publisherStoreFake) NextVersion(context.Context, string, ID) (uint64, error) {
	s.nextCalls++
	if s.nextHook != nil {
		s.nextHook()
	}
	if s.nextVersionErr != nil {
		return 0, s.nextVersionErr
	}
	return s.nextVersion, nil
}

func (s *publisherStoreFake) Publish(_ context.Context, record PublishRecord) (Published, error) {
	s.publishCalls++
	s.published = record
	if s.publishHook != nil {
		s.publishHook()
	}
	if s.publishErr != nil {
		return Published{}, s.publishErr
	}
	return Published{
		TenantID: record.TenantID, Ref: record.Snapshot.Ref, DraftGeneration: record.DraftGeneration,
		EngineVersion: record.EngineVersion, Snapshot: record.Snapshot, Manifest: record.Manifest,
		Active: record.Activate, PublishedAt: record.PublishedAt, PublishedBy: record.Actor,
	}, nil
}

func (s *publisherStoreFake) Resolve(_ context.Context, tenantID string, id ID, version uint64) (Published, bool, error) {
	s.resolveCalls++
	if s.resolveHook != nil {
		s.resolveHook()
	}
	if s.resolveErr != nil {
		return Published{}, false, s.resolveErr
	}
	if !s.resolveOK {
		return Published{}, false, nil
	}
	return s.resolved, true, nil // deliberately do not clone
}

func (s *publisherStoreFake) ListPublished(context.Context, string, int, string) ([]PublishedSummary, string, error) {
	return nil, "", nil
}

func (s *publisherStoreFake) Activate(_ context.Context, tenant string, id ID, version uint64, actor string) error {
	s.activateCalls++
	s.activated.tenant, s.activated.ref, s.activated.actor = tenant, VersionRef{ID: id, Version: version}, actor
	if s.activateHook != nil {
		s.activateHook()
	}
	return s.activateErr
}

func (s *publisherStoreFake) Revoke(_ context.Context, tenant string, id ID, version uint64, actor string, at time.Time) error {
	s.revokeCalls++
	s.revoked.tenant, s.revoked.ref, s.revoked.actor, s.revoked.at = tenant, VersionRef{ID: id, Version: version}, actor, at
	if s.revokeHook != nil {
		s.revokeHook()
	}
	return s.revokeErr
}

func (s *publisherStoreFake) mutationCalls() int {
	return s.publishCalls + s.activateCalls + s.revokeCalls
}

type publisherEngine struct {
	delegate          prompttmpl.Engine
	mu                sync.Mutex
	compileCalls      int
	versionCalls      int
	versions          []string
	compileErr        error
	compiledVersion   string
	renderErr         error
	renderReplacement *string
	afterCompile      func()
	afterRender       func()
	versionHook       func(int)
	seen              prompttmpl.Bundle
}

func (e *publisherEngine) Version() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.versionCalls++
	if e.versionHook != nil {
		e.versionHook(e.versionCalls)
	}
	if len(e.versions) > 0 {
		index := e.versionCalls - 1
		if index >= len(e.versions) {
			index = len(e.versions) - 1
		}
		return e.versions[index]
	}
	return e.delegate.Version()
}

func (e *publisherEngine) Compile(ctx context.Context, bundle prompttmpl.Bundle, limits prompttmpl.Limits) (prompttmpl.CompiledBundle, error) {
	e.mu.Lock()
	e.compileCalls++
	e.seen = cloneTestRenderBundle(bundle)
	compileErr := e.compileErr
	e.mu.Unlock()
	if compileErr != nil {
		return nil, compileErr
	}
	compiled, err := e.delegate.Compile(ctx, bundle, limits)
	if err != nil {
		return nil, err
	}
	if e.afterCompile != nil {
		e.afterCompile()
	}
	return publisherCompiled{CompiledBundle: compiled, version: e.compiledVersion, renderErr: e.renderErr, replacement: e.renderReplacement, afterRender: e.afterRender}, nil
}

func (e *publisherEngine) calls() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.compileCalls
}

type publisherCompiled struct {
	prompttmpl.CompiledBundle
	version     string
	renderErr   error
	replacement *string
	afterRender func()
}

type permissivePublisherEngine struct{}

func (permissivePublisherEngine) Version() string { return "jinja-go-v1/permissive/v1" }

func (permissivePublisherEngine) Compile(context.Context, prompttmpl.Bundle, prompttmpl.Limits) (prompttmpl.CompiledBundle, error) {
	return permissivePublisherCompiled{}, nil
}

type permissivePublisherCompiled struct{}

func (permissivePublisherCompiled) Render(context.Context, string, prompttmpl.Context) (string, error) {
	return "Hello 李明", nil
}
func (permissivePublisherCompiled) Hash() string          { return "permissive-hash" }
func (permissivePublisherCompiled) EngineVersion() string { return "jinja-go-v1/permissive/v1" }
func (permissivePublisherCompiled) ResidentBytes() int    { return 1 }

func (c publisherCompiled) EngineVersion() string {
	if c.version != "" {
		return c.version
	}
	return c.CompiledBundle.EngineVersion()
}

func (c publisherCompiled) Render(ctx context.Context, name string, values prompttmpl.Context) (string, error) {
	var output string
	var err error
	if c.renderErr != nil {
		err = c.renderErr
	} else if c.replacement != nil {
		output = *c.replacement
	} else {
		output, err = c.CompiledBundle.Render(ctx, name, values)
	}
	if c.afterRender != nil {
		c.afterRender()
	}
	return output, err
}

type publisherCatalog struct {
	version       string
	versions      []string
	versionCalls  int
	validateCalls int
	validateHook  func()
	versionHook   func(int)
	err           error
	mutate        bool
	seen          []AdapterBinding
}

func (c *publisherCatalog) Version() string {
	c.versionCalls++
	if c.versionHook != nil {
		c.versionHook(c.versionCalls)
	}
	if len(c.versions) == 0 {
		return c.version
	}
	index := c.versionCalls - 1
	if index >= len(c.versions) {
		index = len(c.versions) - 1
	}
	return c.versions[index]
}

func (c *publisherCatalog) ValidateBindings(_ context.Context, _ string, bindings []AdapterBinding) error {
	c.validateCalls++
	if c.validateHook != nil {
		c.validateHook()
	}
	c.seen = cloneBindings(bindings)
	if c.mutate && len(bindings) > 0 {
		bindings[0].AdapterID = "mutated"
		if len(bindings[0].ToolIDs) > 0 {
			bindings[0].ToolIDs[0] = "mutated"
		}
	}
	return c.err
}

type publisherCapableCatalog struct {
	*publisherCatalog
	allowed bool
	err     error
	calls   int
	hook    func()
}

func (c *publisherCapableCatalog) SupportsPerResponseInstructions(context.Context, string) (bool, error) {
	c.calls++
	if c.hook != nil {
		c.hook()
	}
	return c.allowed, c.err
}

type publisherKnowledge struct {
	calls  int
	err    error
	mutate bool
	seen   []KnowledgeRef
	hook   func()
}

func (v *publisherKnowledge) ValidateReferences(_ context.Context, _ string, refs []KnowledgeRef) error {
	v.calls++
	if v.hook != nil {
		v.hook()
	}
	v.seen = append([]KnowledgeRef(nil), refs...)
	if v.mutate && len(refs) > 0 {
		refs[0].ID = "mutated"
	}
	return v.err
}

type publisherClock struct {
	now  time.Time
	hook func()
}

func (c publisherClock) Now() time.Time {
	if c.hook != nil {
		c.hook()
	}
	return c.now
}

type publisherHarness struct {
	store     *publisherStoreFake
	engine    *publisherEngine
	catalog   *publisherCapableCatalog
	knowledge *publisherKnowledge
	publisher *Publisher
}

func newPublisherHarness(t *testing.T) *publisherHarness {
	t.Helper()
	manifest, blobs := publisherManifest(t)
	store := &publisherStoreFake{
		draft:   Draft{TenantID: testTenant, ID: testProfile, Generation: 3, Manifest: manifest, UpdatedAt: testPublicationTime.Add(-time.Hour), UpdatedBy: testActor},
		draftOK: true, blobs: blobs, nextVersion: 7,
	}
	engine := &publisherEngine{delegate: prompttmpl.NewMiniJinja()}
	catalog := &publisherCapableCatalog{publisherCatalog: &publisherCatalog{version: "catalog-v1"}, allowed: true}
	knowledge := &publisherKnowledge{}
	publisher := NewPublisher(store, engine, catalog, knowledge, WithPublisherClock(publisherClock{now: testPublicationTime}))
	return &publisherHarness{store: store, engine: engine, catalog: catalog, knowledge: knowledge, publisher: publisher}
}

func publisherManifest(t *testing.T) (Manifest, map[string]Blob) {
	t.Helper()
	template := canonicalPublisherBlob(t, "text/plain; charset=utf-8", []byte("Hello {{ customer }}"))
	fixtureJSON, err := json.Marshal(struct {
		Template string                 `json:"template"`
		Context  map[string]interface{} `json:"context"`
		Expected string                 `json:"expected"`
	}{Template: "system.j2", Context: map[string]interface{}{"customer": "李明"}, Expected: "Hello 李明"})
	require.NoError(t, err)
	fixture := canonicalPublisherBlob(t, "application/json", fixtureJSON)
	manifest := Manifest{Bundle: PromptBundle{
		Templates: map[string]TemplateRef{"system.j2": refForBlob(template)},
		Schema: prompttmpl.ContextSchema{Fields: map[string]prompttmpl.Field{
			"customer": {Type: prompttmpl.TypeString, Required: true, MaxBytes: 64, Templates: []string{"system.j2"}, Source: "synthetic"},
		}},
		Fixtures: map[string]TemplateRef{"basic.json": refForBlob(fixture)},
	}}
	require.NoError(t, manifest.Validate())
	return manifest, map[string]Blob{template.Hash: template, fixture.Hash: fixture}
}

func canonicalPublisherBlob(t *testing.T, mediaType string, content []byte) Blob {
	t.Helper()
	blob, err := (Blob{MediaType: mediaType, Content: append([]byte(nil), content...)}).Canonical()
	require.NoError(t, err)
	return blob
}

func refForBlob(blob Blob) TemplateRef {
	return TemplateRef{Hash: blob.Hash, MediaType: blob.MediaType, Bytes: blob.ContentBytes}
}

func (h *publisherHarness) setTemplate(t *testing.T, name, source, mediaType string) {
	t.Helper()
	blob := canonicalPublisherBlob(t, mediaType, []byte(source))
	h.store.draft.Manifest.Bundle.Templates[name] = refForBlob(blob)
	h.store.blobs[blob.Hash] = blob
}

func (h *publisherHarness) setFixture(t *testing.T, name, raw, mediaType string) {
	t.Helper()
	old := h.store.draft.Manifest.Bundle.Fixtures[name]
	delete(h.store.blobs, old.Hash)
	blob := canonicalPublisherBlob(t, mediaType, []byte(raw))
	h.store.draft.Manifest.Bundle.Fixtures[name] = refForBlob(blob)
	h.store.blobs[blob.Hash] = blob
}

func publishCommand() PublishCommand {
	return PublishCommand{TenantID: testTenant, ProfileID: testProfile, ExpectedGeneration: 3, Actor: testActor, Activate: true}
}

func TestPublishCompilesEveryTemplateAndValidatesEveryFixtureBeforeStoreMutation(t *testing.T) {
	t.Run("unreferenced template still compiles", func(t *testing.T) {
		h := newPublisherHarness(t)
		h.setTemplate(t, "fragments/unsafe.j2", "{% if true %}unterminated", "text/plain; charset=utf-8")
		_, err := h.publisher.Publish(t.Context(), publishCommand())
		require.ErrorIs(t, err, prompttmpl.ErrTemplateInvalid)
		require.Zero(t, h.store.mutationCalls())
		require.Equal(t, 1, h.engine.calls())
	})

	t.Run("every fixture expected output is checked", func(t *testing.T) {
		h := newPublisherHarness(t)
		h.setFixture(t, "second.json", `{"template":"system.j2","context":{"customer":"李明"},"expected":"wrong"}`, "application/json")
		_, err := h.publisher.Publish(t.Context(), publishCommand())
		require.ErrorIs(t, err, prompttmpl.ErrTemplateInvalid)
		require.Zero(t, h.store.mutationCalls())
		require.Equal(t, 1, h.engine.calls(), "one pipeline call must compile exactly once")
	})
}

func TestPublishValidatesSourcePolicyIndependentlyOfSuppliedEngine(t *testing.T) {
	h := newPublisherHarness(t)
	h.setTemplate(t, "system.j2", "{{ forbidden_call() }}", "text/plain; charset=utf-8")
	publisher := NewPublisher(h.store, permissivePublisherEngine{}, h.catalog, h.knowledge, WithPublisherClock(publisherClock{now: testPublicationTime}))
	_, err := publisher.Publish(t.Context(), publishCommand())
	require.ErrorIs(t, err, prompttmpl.ErrTemplateInvalid)
	require.Zero(t, h.store.mutationCalls())
}

func TestValidateAndPreviewPreserveTrustedSourcePolicyLocationWithoutSource(t *testing.T) {
	for _, operation := range []struct {
		name string
		run  func(*Publisher) error
	}{
		{"validate", func(publisher *Publisher) error { return publisher.Validate(t.Context(), publishCommand()) }},
		{"preview", func(publisher *Publisher) error {
			_, err := publisher.Preview(t.Context(), publishCommand())
			return err
		}},
	} {
		t.Run(operation.name, func(t *testing.T) {
			h := newPublisherHarness(t)
			h.setTemplate(t, "system.j2", "safe first line\n{{ private_source_token() }}", "text/plain; charset=utf-8")
			err := operation.run(h.publisher)
			require.ErrorIs(t, err, prompttmpl.ErrTemplateInvalid)
			require.Equal(t, "template_invalid: system.j2:2:1", err.Error())
			require.NotContains(t, err.Error(), "private_source_token")
			require.Zero(t, h.store.mutationCalls())
		})
	}
}

func TestPublishPreservesOnlyReviewedCompileErrorTemplateMetadata(t *testing.T) {
	t.Run("known template", func(t *testing.T) {
		h := newPublisherHarness(t)
		h.engine.compileErr = &prompttmpl.CompileError{Template: "system.j2", Err: errors.New("private compiler source")}
		_, err := h.publisher.Publish(t.Context(), publishCommand())
		require.ErrorIs(t, err, prompttmpl.ErrTemplateInvalid)
		require.Equal(t, "compile system.j2: template_invalid", err.Error())
		require.NotContains(t, err.Error(), "private compiler source")
	})

	t.Run("untrusted template metadata", func(t *testing.T) {
		h := newPublisherHarness(t)
		h.engine.compileErr = &prompttmpl.CompileError{Template: "credential-private.j2", Err: errors.New("private compiler source")}
		_, err := h.publisher.Publish(t.Context(), publishCommand())
		require.ErrorIs(t, err, prompttmpl.ErrTemplateInvalid)
		require.Equal(t, prompttmpl.ErrTemplateInvalid.Error(), err.Error())
		require.NotContains(t, err.Error(), "credential-private")
		require.NotContains(t, err.Error(), "private compiler source")
	})
}

func TestPreviewAndValidateSharePipelineWithoutMutationAndOwnOutputs(t *testing.T) {
	h := newPublisherHarness(t)
	require.NoError(t, h.publisher.Validate(t.Context(), publishCommand()))
	preview, err := h.publisher.Preview(t.Context(), publishCommand())
	require.NoError(t, err)
	require.Equal(t, map[string]string{"basic.json": "Hello 李明"}, preview.Outputs)
	require.Len(t, preview.BundleHash, 64)
	require.Len(t, preview.SnapshotHash, 64)
	require.Zero(t, h.store.mutationCalls())
	require.Equal(t, 2, h.engine.calls())

	preview.Outputs["basic.json"] = "mutated"
	again, err := h.publisher.Preview(t.Context(), publishCommand())
	require.NoError(t, err)
	require.Equal(t, "Hello 李明", again.Outputs["basic.json"])
}

func TestPublishSendsExactPreparedManifestSnapshotAndAdvisoryVersion(t *testing.T) {
	h := newPublisherHarness(t)
	published, err := h.publisher.Publish(t.Context(), publishCommand())
	require.NoError(t, err)
	require.Equal(t, 1, h.store.publishCalls)
	require.Equal(t, uint64(7), h.store.published.Snapshot.Ref.Version)
	require.Equal(t, int64(3), h.store.published.ExpectedGeneration)
	require.Equal(t, int64(3), h.store.published.DraftGeneration)
	require.Equal(t, testPublicationTime, h.store.published.PublishedAt)
	require.Equal(t, "catalog-v1", h.store.published.Snapshot.AdapterRegistryVersion)
	require.Equal(t, h.engine.delegate.Version(), h.store.published.EngineVersion)
	require.NoError(t, h.store.published.Validate())
	require.Equal(t, h.store.published.Snapshot, published.Snapshot)
	require.Equal(t, h.store.published.Manifest, published.Manifest)
	require.True(t, published.Active)

	preparedSystem := h.store.published.Manifest.Bundle.Templates["system.j2"]
	h.store.draft.Manifest.Bundle.Templates["system.j2"] = TemplateRef{}
	for _, blob := range h.store.blobs {
		if len(blob.Content) > 0 {
			blob.Content[0] = 'X'
		}
	}
	require.Equal(t, preparedSystem, h.store.published.Manifest.Bundle.Templates["system.j2"], "publication input must own draft/blob state")
}

func TestPublishRejectsAdvisoryVersionOutsideStoreDomainBeforeMutation(t *testing.T) {
	h := newPublisherHarness(t)
	h.store.nextVersion = uint64(math.MaxInt64) + 1
	_, err := h.publisher.Publish(t.Context(), publishCommand())
	require.ErrorIs(t, err, ErrPublicationConflict)
	require.Zero(t, h.store.mutationCalls())
}

func TestPublishRejectsOutOfRangeAuditClockBeforeMutation(t *testing.T) {
	h := newPublisherHarness(t)
	publisher := NewPublisher(h.store, h.engine, h.catalog, h.knowledge, WithPublisherClock(publisherClock{now: time.Date(10000, time.January, 1, 0, 0, 0, 0, time.UTC)}))
	_, err := publisher.Publish(t.Context(), publishCommand())
	require.ErrorIs(t, err, ErrInvalidProfile)
	require.Zero(t, h.store.publishCalls)
}

func TestPublishRejectsMissingOrCorruptBlobTuples(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*publisherHarness)
	}{
		{"missing", func(h *publisherHarness) {
			delete(h.store.blobs, h.store.draft.Manifest.Bundle.Templates["system.j2"].Hash)
		}},
		{"content hash", func(h *publisherHarness) {
			ref := h.store.draft.Manifest.Bundle.Templates["system.j2"]
			blob := h.store.blobs[ref.Hash]
			blob.Content = []byte("different")
			h.store.blobs[ref.Hash] = blob
		}},
		{"byte count", func(h *publisherHarness) {
			ref := h.store.draft.Manifest.Bundle.Templates["system.j2"]
			blob := h.store.blobs[ref.Hash]
			blob.ContentBytes++
			h.store.blobs[ref.Hash] = blob
		}},
		{"tuple media", func(h *publisherHarness) {
			ref := h.store.draft.Manifest.Bundle.Templates["system.j2"]
			blob := h.store.blobs[ref.Hash]
			blob.MediaType = "application/json"
			h.store.blobs[ref.Hash] = blob
		}},
		{"unreviewed template media", func(h *publisherHarness) { h.setTemplate(t, "system.j2", "safe", "application/octet-stream") }},
		{"invalid utf8 template", func(h *publisherHarness) {
			blob := canonicalPublisherBlob(t, "text/plain; charset=utf-8", []byte{0xff})
			h.store.draft.Manifest.Bundle.Templates["system.j2"] = refForBlob(blob)
			h.store.blobs[blob.Hash] = blob
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newPublisherHarness(t)
			tc.mutate(h)
			_, err := h.publisher.Publish(t.Context(), publishCommand())
			require.Error(t, err)
			require.Zero(t, h.store.mutationCalls())
		})
	}
}

func TestPublishRejectsStrictFixtureGrammarContextAndExpectedOutput(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want error
	}{
		{"unknown", `{"template":"system.j2","context":{"customer":"Li"},"expected":"Hello Li","extra":true}`, prompttmpl.ErrTemplateInvalid},
		{"duplicate", `{"template":"system.j2","template":"system.j2","context":{"customer":"Li"},"expected":"Hello Li"}`, prompttmpl.ErrTemplateInvalid},
		{"trailing", `{"template":"system.j2","context":{"customer":"Li"},"expected":"Hello Li"}{}`, prompttmpl.ErrTemplateInvalid},
		{"context object", `{"template":"system.j2","context":[],"expected":"Hello Li"}`, prompttmpl.ErrContextInvalid},
		{"context type", `{"template":"system.j2","context":{"customer":7},"expected":"Hello 7"}`, prompttmpl.ErrContextInvalid},
		{"expected type", `{"template":"system.j2","context":{"customer":"Li"},"expected":7}`, prompttmpl.ErrTemplateInvalid},
		{"expected mismatch", `{"template":"system.j2","context":{"customer":"Li"},"expected":"private-expected-value"}`, prompttmpl.ErrTemplateInvalid},
		{"malformed", `{"template":`, prompttmpl.ErrTemplateInvalid},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newPublisherHarness(t)
			h.setFixture(t, "basic.json", tc.raw, "application/json")
			_, err := h.publisher.Publish(t.Context(), publishCommand())
			require.ErrorIs(t, err, tc.want)
			require.Zero(t, h.store.mutationCalls())
			require.NotContains(t, err.Error(), "private-expected-value")
		})
	}
}

func TestDecodeSyntheticFixtureAdmitsExactAggregateItemLimit(t *testing.T) {
	fixture := func(arrayItems int) []byte {
		t.Helper()
		items := make([]any, arrayItems)
		for index := range items {
			items[index] = nil
		}
		raw, err := json.Marshal(map[string]any{
			"template": "system.j2",
			"context":  map[string]any{"items": items},
			"expected": "",
		})
		require.NoError(t, err)
		return raw
	}

	// Root fields (3), the context member (1), and 1,020 array values are
	// exactly the shared 1,024-item aggregate limit.
	_, err := decodeSyntheticFixture(fixture(prompttmpl.MaxCollectionItems - 4))
	require.NoError(t, err)
	_, err = decodeSyntheticFixture(fixture(prompttmpl.MaxCollectionItems - 3))
	require.ErrorIs(t, err, prompttmpl.ErrTemplateInvalid)
}

func TestPublishValidatesFixtureTemplateACLAndBounds(t *testing.T) {
	t.Run("template ACL", func(t *testing.T) {
		h := newPublisherHarness(t)
		h.setTemplate(t, "opening_message.j2", "Hello {{ customer }}", "text/plain; charset=utf-8")
		h.setFixture(t, "basic.json", `{"template":"opening_message.j2","context":{"customer":"Li"},"expected":"Hello Li"}`, "application/json")
		_, err := h.publisher.Publish(t.Context(), publishCommand())
		require.ErrorIs(t, err, prompttmpl.ErrContextInvalid)
		require.Zero(t, h.store.mutationCalls())
	})

	t.Run("fixture blob bound", func(t *testing.T) {
		h := newPublisherHarness(t)
		raw, err := json.Marshal(map[string]any{"template": "system.j2", "context": map[string]any{"customer": "Li"}, "expected": strings.Repeat("x", prompttmpl.MaxOutputBytes+1)})
		require.NoError(t, err)
		h.setFixture(t, "basic.json", string(raw), "application/json")
		_, err = h.publisher.Publish(t.Context(), publishCommand())
		require.ErrorIs(t, err, ErrInvalidProfile)
		require.Zero(t, h.store.mutationCalls())
	})
}

func TestPublishMapsRenderLimitWithoutContentLeakage(t *testing.T) {
	h := newPublisherHarness(t)
	h.engine.renderErr = &prompttmpl.RenderError{Template: "system.j2", Err: prompttmpl.ErrRenderLimit}
	_, err := h.publisher.Publish(t.Context(), publishCommand())
	require.ErrorIs(t, err, prompttmpl.ErrRenderLimit)
	require.Zero(t, h.store.mutationCalls())
	require.NotContains(t, err.Error(), "李明")
}

func TestPublishFailsClosedForCatalogVersionBindingsAndPerTurnCapability(t *testing.T) {
	t.Run("missing catalog", func(t *testing.T) {
		h := newPublisherHarness(t)
		publisher := NewPublisher(h.store, h.engine, nil, h.knowledge, WithPublisherClock(publisherClock{now: testPublicationTime}))
		_, err := publisher.Publish(t.Context(), publishCommand())
		require.ErrorIs(t, err, ErrProfileIncompatible)
		require.Zero(t, h.store.loadDraftCalls)
	})

	t.Run("empty catalog version", func(t *testing.T) {
		h := newPublisherHarness(t)
		h.catalog.version = ""
		_, err := h.publisher.Publish(t.Context(), publishCommand())
		require.ErrorIs(t, err, ErrProfileIncompatible)
		require.Zero(t, h.store.mutationCalls())
	})

	t.Run("changed catalog version", func(t *testing.T) {
		h := newPublisherHarness(t)
		h.catalog.versions = []string{"catalog-v1", "catalog-v2"}
		_, err := h.publisher.Publish(t.Context(), publishCommand())
		require.ErrorIs(t, err, ErrProfileIncompatible)
		require.Zero(t, h.store.mutationCalls())
	})

	t.Run("binding rejection is content free", func(t *testing.T) {
		h := newPublisherHarness(t)
		h.store.draft.Manifest.Bindings = []AdapterBinding{{AdapterID: "lookup", Version: "v1", ToolIDs: []string{"search"}}}
		h.catalog.publisherCatalog.err = errors.New("https://secret.invalid credential-raw-adapter-output")
		_, err := h.publisher.Publish(t.Context(), publishCommand())
		require.ErrorIs(t, err, ErrProfileIncompatible)
		require.NotContains(t, err.Error(), "secret.invalid")
		require.Zero(t, h.store.mutationCalls())
	})

	t.Run("catalog cannot mutate publication", func(t *testing.T) {
		h := newPublisherHarness(t)
		h.store.draft.Manifest.Bindings = []AdapterBinding{{AdapterID: "lookup", Version: "v1", ToolIDs: []string{"search"}}}
		h.catalog.mutate = true
		_, err := h.publisher.Publish(t.Context(), publishCommand())
		require.NoError(t, err)
		require.Equal(t, "lookup", h.store.published.Manifest.Bindings[0].AdapterID)
		require.Equal(t, "search", h.store.published.Manifest.Bindings[0].ToolIDs[0])
	})

	t.Run("per-turn catalog capability", func(t *testing.T) {
		for _, tc := range []struct {
			name    string
			catalog AdapterCatalog
			wantErr bool
		}{
			{"missing", &publisherCatalog{version: "catalog-v1"}, true},
			{"denied", &publisherCapableCatalog{publisherCatalog: &publisherCatalog{version: "catalog-v1"}, allowed: false}, true},
			{"allowed", &publisherCapableCatalog{publisherCatalog: &publisherCatalog{version: "catalog-v1"}, allowed: true}, false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				h := newPublisherHarness(t)
				h.setTemplate(t, "turn_instruction.j2", "Answer {{ customer }}", "text/plain; charset=utf-8")
				publisher := NewPublisher(h.store, h.engine, tc.catalog, h.knowledge, WithPublisherClock(publisherClock{now: testPublicationTime}))
				_, err := publisher.Publish(t.Context(), publishCommand())
				if tc.wantErr {
					require.ErrorIs(t, err, ErrProfileIncompatible)
					require.Zero(t, h.store.mutationCalls())
				} else {
					require.NoError(t, err)
				}
			})
		}
	})
}

func TestPublishRequiresStableEngineVersionAcrossCompilation(t *testing.T) {
	h := newPublisherHarness(t)
	current := h.engine.delegate.Version()
	h.engine.versions = []string{current, current + "/changed"}
	_, err := h.publisher.Publish(t.Context(), publishCommand())
	require.ErrorIs(t, err, ErrProfileIncompatible)
	require.Zero(t, h.store.mutationCalls())

	h = newPublisherHarness(t)
	h.engine.compiledVersion = "different-engine"
	_, err = h.publisher.Publish(t.Context(), publishCommand())
	require.ErrorIs(t, err, ErrProfileIncompatible)
	require.Zero(t, h.store.mutationCalls())
}

func TestPublishRejectsNoncompliantEngineIdentityBeforeStoreAccess(t *testing.T) {
	h := newPublisherHarness(t)
	h.engine.versions = []string{"minijinja-go/v2.24.0"}
	_, err := h.publisher.Publish(t.Context(), publishCommand())
	require.ErrorIs(t, err, ErrProfileIncompatible)
	require.Zero(t, h.store.loadDraftCalls)
}

func TestPublishKnowledgeValidationDefaultsFailClosedAndOwnsReferences(t *testing.T) {
	knowledge := KnowledgeRef{ID: "policy", Version: 1, Hash: strings.Repeat("a", 64)}

	t.Run("nil accepts empty", func(t *testing.T) {
		h := newPublisherHarness(t)
		publisher := NewPublisher(h.store, h.engine, h.catalog, nil, WithPublisherClock(publisherClock{now: testPublicationTime}))
		_, err := publisher.Publish(t.Context(), publishCommand())
		require.NoError(t, err)
	})

	t.Run("nil rejects nonempty", func(t *testing.T) {
		h := newPublisherHarness(t)
		h.store.draft.Manifest.Knowledge = []KnowledgeRef{knowledge}
		publisher := NewPublisher(h.store, h.engine, h.catalog, nil, WithPublisherClock(publisherClock{now: testPublicationTime}))
		_, err := publisher.Publish(t.Context(), publishCommand())
		require.ErrorIs(t, err, ErrProfileIncompatible)
		require.Zero(t, h.store.mutationCalls())
	})

	t.Run("explicit validator error is content free", func(t *testing.T) {
		h := newPublisherHarness(t)
		h.store.draft.Manifest.Knowledge = []KnowledgeRef{knowledge}
		h.knowledge.err = errors.New("customer-policy-private-material")
		_, err := h.publisher.Publish(t.Context(), publishCommand())
		require.ErrorIs(t, err, ErrProfileIncompatible)
		require.NotContains(t, err.Error(), "customer-policy")
	})

	t.Run("validator receives an owned list", func(t *testing.T) {
		h := newPublisherHarness(t)
		h.store.draft.Manifest.Knowledge = []KnowledgeRef{knowledge}
		h.knowledge.mutate = true
		_, err := h.publisher.Publish(t.Context(), publishCommand())
		require.NoError(t, err)
		require.Equal(t, "policy", h.store.published.Manifest.Knowledge[0].ID)
	})
}

func TestPublishFailureOrConflictNeverChangesActiveVersion(t *testing.T) {
	for _, storeErr := range []error{ErrStaleDraft, ErrPublicationConflict, ErrStoreUnavailable} {
		h := newPublisherHarness(t)
		h.store.publishErr = storeErr
		_, err := h.publisher.Publish(t.Context(), publishCommand())
		require.ErrorIs(t, err, storeErr)
		require.Equal(t, 1, h.store.publishCalls)
		require.Zero(t, h.store.activateCalls)
	}
}

func TestPublisherActivateAndRevokeDelegateExactIdentityActorAndClock(t *testing.T) {
	h := newPublisherHarness(t)
	ref := VersionRef{ID: testProfile, Version: 9}
	require.NoError(t, h.publisher.Activate(t.Context(), testTenant, ref, testActor))
	require.Equal(t, testTenant, h.store.activated.tenant)
	require.Equal(t, ref, h.store.activated.ref)
	require.Equal(t, testActor, h.store.activated.actor)

	require.NoError(t, h.publisher.Revoke(t.Context(), testTenant, ref, testActor))
	require.Equal(t, testTenant, h.store.revoked.tenant)
	require.Equal(t, ref, h.store.revoked.ref)
	require.Equal(t, testActor, h.store.revoked.actor)
	require.Equal(t, testPublicationTime, h.store.revoked.at)

	h.store.activateErr = ErrProfileRevoked
	require.ErrorIs(t, h.publisher.Activate(t.Context(), testTenant, ref, testActor), ErrProfileRevoked)
	h.store.revokeErr = errors.New("raw database and prompt source")
	err := h.publisher.Revoke(t.Context(), testTenant, ref, testActor)
	require.ErrorIs(t, err, ErrStoreUnavailable)
	require.NotContains(t, err.Error(), "raw database")
}

func TestPublisherActivateAndRevokeRejectVersionAboveStoreDomainBeforeStoreAccess(t *testing.T) {
	h := newPublisherHarness(t)
	ref := VersionRef{ID: testProfile, Version: uint64(math.MaxInt64) + 1}
	require.ErrorIs(t, h.publisher.Activate(t.Context(), testTenant, ref, testActor), ErrInvalidProfile)
	require.ErrorIs(t, h.publisher.Revoke(t.Context(), testTenant, ref, testActor), ErrInvalidProfile)
	require.Zero(t, h.store.activateCalls)
	require.Zero(t, h.store.revokeCalls)
}

func TestPublisherRejectsInvalidNilTypedNilCancelledAndReservedInputsBeforeStoreWork(t *testing.T) {
	h := newPublisherHarness(t)
	commands := []PublishCommand{
		{},
		{TenantID: testTenant, ProfileID: "builtin", ExpectedGeneration: 3, Actor: testActor},
		{TenantID: testTenant, ProfileID: testProfile, ExpectedGeneration: 0, Actor: testActor},
		{TenantID: testTenant, ProfileID: testProfile, ExpectedGeneration: 3, Actor: "bad\nactor"},
	}
	for _, command := range commands {
		_, err := h.publisher.Publish(t.Context(), command)
		require.ErrorIs(t, err, ErrInvalidProfile)
	}
	require.Zero(t, h.store.loadDraftCalls)

	_, err := h.publisher.Publish(nil, publishCommand())
	require.ErrorIs(t, err, ErrInvalidProfile)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = h.publisher.Publish(ctx, publishCommand())
	require.ErrorIs(t, err, context.Canceled)

	var typedNilStore *publisherStoreFake
	typedNilPublisher := NewPublisher(typedNilStore, h.engine, h.catalog, h.knowledge)
	_, err = typedNilPublisher.Publish(t.Context(), publishCommand())
	require.ErrorIs(t, err, ErrProfileIncompatible)

	var typedNilEngine *publisherEngine
	typedNilPublisher = NewPublisher(h.store, typedNilEngine, h.catalog, h.knowledge)
	_, err = typedNilPublisher.Publish(t.Context(), publishCommand())
	require.ErrorIs(t, err, ErrProfileIncompatible)
}

func TestPublisherMapsDependencyAndStoreErrorsWithoutSensitiveContent(t *testing.T) {
	secret := "credential=https://private.invalid reasoning=raw-model-output"
	for _, setup := range []func(*publisherHarness){
		func(h *publisherHarness) { h.store.loadDraftErr = errors.New(secret) },
		func(h *publisherHarness) { h.store.loadBlobErr = errors.New(secret) },
		func(h *publisherHarness) { h.store.nextVersionErr = errors.New(secret) },
		func(h *publisherHarness) { h.engine.compileErr = errors.New(secret) },
	} {
		h := newPublisherHarness(t)
		setup(h)
		_, err := h.publisher.Publish(t.Context(), publishCommand())
		require.Error(t, err)
		require.NotContains(t, err.Error(), "private.invalid")
		require.NotContains(t, err.Error(), "raw-model-output")
		require.Zero(t, h.store.mutationCalls())
	}
}

func TestPublisherPreservesDependencyCancellationClasses(t *testing.T) {
	t.Run("catalog", func(t *testing.T) {
		h := newPublisherHarness(t)
		h.catalog.publisherCatalog.err = context.Canceled
		_, err := h.publisher.Publish(t.Context(), publishCommand())
		require.ErrorIs(t, err, context.Canceled)
	})
	t.Run("knowledge", func(t *testing.T) {
		h := newPublisherHarness(t)
		h.knowledge.err = context.DeadlineExceeded
		_, err := h.publisher.Publish(t.Context(), publishCommand())
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
	t.Run("engine", func(t *testing.T) {
		h := newPublisherHarness(t)
		h.engine.compileErr = context.Canceled
		_, err := h.publisher.Publish(t.Context(), publishCommand())
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestPublishStopsWhenDependencyCancelsContextButReturnsSuccess(t *testing.T) {
	h := newPublisherHarness(t)
	ctx, cancel := context.WithCancel(context.Background())
	h.store.nextHook = cancel
	_, err := h.publisher.Publish(ctx, publishCommand())
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, h.store.publishCalls)
}

func TestPublisherPrefersCancellationAfterMissingOrDeniedDependencyResults(t *testing.T) {
	t.Run("missing draft", func(t *testing.T) {
		h := newPublisherHarness(t)
		ctx, cancel := context.WithCancel(context.Background())
		h.store.draftOK = false
		h.store.loadDraftHook = cancel
		_, err := h.publisher.Publish(ctx, publishCommand())
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, h.engine.calls())
	})

	t.Run("missing blob", func(t *testing.T) {
		h := newPublisherHarness(t)
		ctx, cancel := context.WithCancel(context.Background())
		delete(h.store.blobs, h.store.draft.Manifest.Bundle.Templates["system.j2"].Hash)
		h.store.loadBlobHook = cancel
		_, err := h.publisher.Publish(ctx, publishCommand())
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, h.engine.calls())
	})

	t.Run("denied per-response capability", func(t *testing.T) {
		h := newPublisherHarness(t)
		ctx, cancel := context.WithCancel(context.Background())
		h.setTemplate(t, "turn_instruction.j2", "Answer {{ customer }}", "text/plain; charset=utf-8")
		h.catalog.allowed = false
		h.catalog.hook = cancel
		_, err := h.publisher.Publish(ctx, publishCommand())
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, h.knowledge.calls)
	})
}

func TestPublisherFencesSuccessfulDependencyReturnsBeforeClassification(t *testing.T) {
	t.Run("engine identity", func(t *testing.T) {
		h := newPublisherHarness(t)
		ctx, cancel := context.WithCancel(context.Background())
		h.engine.versions = []string{"implementation-only"}
		h.engine.versionHook = func(call int) {
			if call == 1 {
				cancel()
			}
		}
		_, err := h.publisher.Publish(ctx, publishCommand())
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, h.store.loadDraftCalls)
	})

	t.Run("catalog identity", func(t *testing.T) {
		h := newPublisherHarness(t)
		ctx, cancel := context.WithCancel(context.Background())
		h.catalog.publisherCatalog.version = ""
		h.catalog.publisherCatalog.versionHook = func(call int) {
			if call == 1 {
				cancel()
			}
		}
		_, err := h.publisher.Publish(ctx, publishCommand())
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, h.store.loadDraftCalls)
	})

	t.Run("compile", func(t *testing.T) {
		h := newPublisherHarness(t)
		ctx, cancel := context.WithCancel(context.Background())
		h.engine.afterCompile = cancel
		_, err := h.publisher.Publish(ctx, publishCommand())
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, h.catalog.validateCalls)
	})

	t.Run("rendered payload", func(t *testing.T) {
		h := newPublisherHarness(t)
		ctx, cancel := context.WithCancel(context.Background())
		replacement := "wrong private output"
		h.engine.renderReplacement = &replacement
		h.engine.afterRender = cancel
		_, err := h.publisher.Publish(ctx, publishCommand())
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, h.catalog.validateCalls)
	})

	t.Run("catalog validation", func(t *testing.T) {
		h := newPublisherHarness(t)
		ctx, cancel := context.WithCancel(context.Background())
		h.catalog.publisherCatalog.validateHook = cancel
		_, err := h.publisher.Publish(ctx, publishCommand())
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, h.knowledge.calls)
	})

	t.Run("post-validation catalog identity", func(t *testing.T) {
		h := newPublisherHarness(t)
		ctx, cancel := context.WithCancel(context.Background())
		h.catalog.publisherCatalog.versionHook = func(call int) {
			if call == 2 {
				cancel()
			}
		}
		_, err := h.publisher.Publish(ctx, publishCommand())
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, h.knowledge.calls)
	})

	t.Run("knowledge validation", func(t *testing.T) {
		h := newPublisherHarness(t)
		ctx, cancel := context.WithCancel(context.Background())
		h.knowledge.hook = cancel
		_, err := h.publisher.Publish(ctx, publishCommand())
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, h.store.nextCalls)
	})
}

func TestPublisherFencesCancellationAtFixtureLoopBoundary(t *testing.T) {
	h := newPublisherHarness(t)
	h.setFixture(t, "second.json", `{"template":"system.j2","context":{"customer":"李明"},"expected":"Hello 李明"}`, "application/json")
	ctx, cancel := context.WithCancel(context.Background())
	renderCalls := 0
	h.engine.afterRender = func() {
		renderCalls++
		if renderCalls == 1 {
			cancel()
		}
	}
	_, err := h.publisher.Publish(ctx, publishCommand())
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, renderCalls)
	require.Zero(t, h.catalog.validateCalls)
}

func TestPublisherFencesClockAndStoreMutationReturns(t *testing.T) {
	t.Run("publish clock", func(t *testing.T) {
		h := newPublisherHarness(t)
		ctx, cancel := context.WithCancel(context.Background())
		publisher := NewPublisher(h.store, h.engine, h.catalog, h.knowledge, WithPublisherClock(publisherClock{now: time.Time{}, hook: cancel}))
		_, err := publisher.Publish(ctx, publishCommand())
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, h.store.publishCalls)
	})

	t.Run("publish store", func(t *testing.T) {
		h := newPublisherHarness(t)
		ctx, cancel := context.WithCancel(context.Background())
		h.store.publishHook = cancel
		_, err := h.publisher.Publish(ctx, publishCommand())
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 1, h.store.publishCalls)
	})

	t.Run("activate store", func(t *testing.T) {
		h := newPublisherHarness(t)
		ctx, cancel := context.WithCancel(context.Background())
		h.store.activateHook = cancel
		err := h.publisher.Activate(ctx, testTenant, VersionRef{ID: testProfile, Version: 1}, testActor)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("revoke clock", func(t *testing.T) {
		h := newPublisherHarness(t)
		ctx, cancel := context.WithCancel(context.Background())
		publisher := NewPublisher(h.store, h.engine, h.catalog, h.knowledge, WithPublisherClock(publisherClock{now: time.Time{}, hook: cancel}))
		err := publisher.Revoke(ctx, testTenant, VersionRef{ID: testProfile, Version: 1}, testActor)
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, h.store.revokeCalls)
	})

	t.Run("revoke store", func(t *testing.T) {
		h := newPublisherHarness(t)
		ctx, cancel := context.WithCancel(context.Background())
		h.store.revokeHook = cancel
		err := h.publisher.Revoke(ctx, testTenant, VersionRef{ID: testProfile, Version: 1}, testActor)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 1, h.store.revokeCalls)
	})
}

func cloneTestRenderBundle(bundle prompttmpl.Bundle) prompttmpl.Bundle {
	clone := prompttmpl.Bundle{Templates: make(map[string]string, len(bundle.Templates)), Schema: prompttmpl.ContextSchema{Fields: make(map[string]prompttmpl.Field, len(bundle.Schema.Fields))}}
	for name, source := range bundle.Templates {
		clone.Templates[name] = source
	}
	for name, field := range bundle.Schema.Fields {
		field.Templates = append([]string(nil), field.Templates...)
		clone.Schema.Fields[name] = field
	}
	return clone
}

func TestPublishRecordHashesRemainCanonicalAfterMapInsertionOrderChanges(t *testing.T) {
	first := newPublisherHarness(t)
	second := newPublisherHarness(t)
	second.store.draft.Manifest.Bundle.Templates = map[string]TemplateRef{}
	for _, name := range []string{"fragments/rules.j2", "system.j2"} {
		if name == "fragments/rules.j2" {
			second.setTemplate(t, name, "Static rules.", "text/plain; charset=utf-8")
			first.setTemplate(t, name, "Static rules.", "text/plain; charset=utf-8")
			continue
		}
		ref := first.store.draft.Manifest.Bundle.Templates[name]
		second.store.draft.Manifest.Bundle.Templates[name] = ref
	}
	_, err := first.publisher.Publish(t.Context(), publishCommand())
	require.NoError(t, err)
	_, err = second.publisher.Publish(t.Context(), publishCommand())
	require.NoError(t, err)
	assert.Equal(t, first.store.published.Snapshot.BundleHash, second.store.published.Snapshot.BundleHash)
	assert.Equal(t, first.store.published.Snapshot.SnapshotHash, second.store.published.Snapshot.SnapshotHash)
}
