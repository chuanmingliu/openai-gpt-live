package knowledge_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/knowledge"
	"github.com/chuanmingliu/openai-gpt-live/internal/profile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeKnowledgeStore resolves only what it was given, and counts reads so a
// test can prove bounds are enforced before any store call.
type fakeKnowledgeStore struct {
	versions map[string]knowledge.Version
	reads    int
	err      error
}

func newFakeStore() *fakeKnowledgeStore {
	return &fakeKnowledgeStore{versions: map[string]knowledge.Version{}}
}

func storeKey(tenantID string, ref knowledge.VersionRef) string {
	return tenantID + "\x00" + ref.PackID + "\x00" + ref.Hash
}

func (store *fakeKnowledgeStore) put(version knowledge.Version) knowledge.VersionRef {
	store.versions[storeKey(version.TenantID, version.Ref)] = version
	return version.Ref
}

func (store *fakeKnowledgeStore) Resolve(_ context.Context, tenantID string, ref knowledge.VersionRef) (knowledge.Version, bool, error) {
	store.reads++
	if store.err != nil {
		return knowledge.Version{}, false, store.err
	}
	version, ok := store.versions[storeKey(tenantID, ref)]
	if !ok || version.Ref.Version != ref.Version {
		return knowledge.Version{}, false, nil
	}
	return version, true, nil
}

func (store *fakeKnowledgeStore) CreateDraft(context.Context, knowledge.Draft) (knowledge.Draft, error) {
	return knowledge.Draft{}, errors.New("unused")
}

func (store *fakeKnowledgeStore) Publish(context.Context, knowledge.PublishRecord) (knowledge.Version, error) {
	return knowledge.Version{}, errors.New("unused")
}

func (store *fakeKnowledgeStore) List(context.Context, string, string, int) ([]knowledge.Version, error) {
	return nil, errors.New("unused")
}

func (store *fakeKnowledgeStore) Tombstone(context.Context, string, knowledge.VersionRef, string, time.Time) error {
	return errors.New("unused")
}

func storedVersion(tenantID, packID string, number uint64, body string) knowledge.Version {
	version := knowledge.Version{
		TenantID:  tenantID,
		Ref:       knowledge.VersionRef{PackID: packID, Version: number},
		Title:     "policy.md",
		MediaType: "text/markdown",
		Excerpts:  []knowledge.Excerpt{excerpt(0, body)},
	}
	version.Ref.Hash = knowledge.ContentHash(version)
	return version
}

func profileRef(ref knowledge.VersionRef) profile.KnowledgeRef {
	return profile.KnowledgeRef{ID: ref.PackID, Version: ref.Version, Hash: ref.Hash}
}

func TestPublicationRequiresEveryKnowledgeRefToResolveExactly(t *testing.T) {
	store := newFakeStore()
	published := store.put(storedVersion("tenant-a", "policy", 1, "waiting period is thirty days"))
	validator := knowledge.NewReferenceValidator(store)

	require.NoError(t, validator.ValidateReferences(t.Context(), "tenant-a", []profile.KnowledgeRef{profileRef(published)}))
	require.NoError(t, validator.ValidateReferences(t.Context(), "tenant-a", nil),
		"a profile that pins nothing needs no material")

	for name, ref := range map[string]profile.KnowledgeRef{
		"wrong hash":    {ID: "policy", Version: 1, Hash: strings.Repeat("a", 64)},
		"wrong version": {ID: "policy", Version: 2, Hash: published.Hash},
		"unknown pack":  {ID: "absent", Version: 1, Hash: published.Hash},
		"empty pack":    {ID: "", Version: 1, Hash: published.Hash},
		"zero version":  {ID: "policy", Version: 0, Hash: published.Hash},
		"short hash":    {ID: "policy", Version: 1, Hash: "abc"},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Error(t, validator.ValidateReferences(t.Context(), "tenant-a", []profile.KnowledgeRef{ref}))
		})
	}

	assert.Error(t, validator.ValidateReferences(t.Context(), "tenant-b", []profile.KnowledgeRef{profileRef(published)}),
		"another tenant's material is not pinnable")
}

func TestTombstonedVersionsAreNotPinnableByANewPublication(t *testing.T) {
	store := newFakeStore()
	version := storedVersion("tenant-a", "policy", 1, "waiting period is thirty days")
	moment := time.Date(2026, 8, 16, 13, 0, 0, 0, time.UTC)
	version.TombstonedAt = &moment
	version.TombstonedBy = "operator"
	ref := store.put(version)

	err := knowledge.NewReferenceValidator(store).ValidateReferences(t.Context(), "tenant-a", []profile.KnowledgeRef{profileRef(ref)})
	assert.ErrorIs(t, err, knowledge.ErrKnowledgeTombstoned)
}

func TestDuplicateAndOversizeReferenceSetsAreRejectedBeforeAnyStoreCall(t *testing.T) {
	store := newFakeStore()
	published := store.put(storedVersion("tenant-a", "policy", 1, "body"))
	validator := knowledge.NewReferenceValidator(store)

	store.reads = 0
	tooMany := make([]profile.KnowledgeRef, 33)
	for index := range tooMany {
		tooMany[index] = profileRef(published)
	}
	assert.ErrorIs(t, validator.ValidateReferences(t.Context(), "tenant-a", tooMany), knowledge.ErrContentLimit)
	assert.Zero(t, store.reads, "a hostile manifest must not become unbounded reads")

	store.reads = 0
	assert.ErrorIs(t, validator.ValidateReferences(t.Context(), "tenant a", []profile.KnowledgeRef{profileRef(published)}),
		knowledge.ErrInvalidKnowledge)
	assert.Zero(t, store.reads, "identity is checked before any read")

	store.reads = 0
	duplicated := []profile.KnowledgeRef{profileRef(published), profileRef(published)}
	assert.ErrorIs(t, validator.ValidateReferences(t.Context(), "tenant-a", duplicated), knowledge.ErrInvalidKnowledge)
}

func TestStoreOutageFailsPublicationClosed(t *testing.T) {
	store := newFakeStore()
	published := store.put(storedVersion("tenant-a", "policy", 1, "body"))
	store.err = knowledge.ErrStoreUnavailable

	err := knowledge.NewReferenceValidator(store).ValidateReferences(t.Context(), "tenant-a", []profile.KnowledgeRef{profileRef(published)})
	assert.ErrorIs(t, err, knowledge.ErrStoreUnavailable,
		"an unreachable store must never be read as an empty store")

	assert.Error(t, knowledge.NewReferenceValidator(nil).ValidateReferences(
		t.Context(), "tenant-a", []profile.KnowledgeRef{profileRef(published)}))
}

func TestCancellationIsReportedBeforeResolution(t *testing.T) {
	store := newFakeStore()
	published := store.put(storedVersion("tenant-a", "policy", 1, "body"))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := knowledge.NewReferenceValidator(store).ValidateReferences(ctx, "tenant-a", []profile.KnowledgeRef{profileRef(published)})
	assert.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, store.reads)
}
