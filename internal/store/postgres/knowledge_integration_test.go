//go:build integration

package postgres

import (
	"strings"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/knowledge"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func knowledgeExcerpt(ordinal int, text string) knowledge.Excerpt {
	return knowledge.Excerpt{
		Ordinal: ordinal,
		Text:    text,
		Tokens:  []string{"coverage", "waiting"},
		Citation: knowledge.Citation{
			SourceTitle: "policy.md",
			Section:     "Coverage",
			ExcerptHash: knowledge.ExcerptHash(text),
		},
	}
}

func knowledgeRecord(tenantID, packID string, excerpts ...knowledge.Excerpt) knowledge.PublishRecord {
	return knowledge.PublishRecord{
		TenantID:    tenantID,
		PackID:      packID,
		Title:       "policy.md",
		MediaType:   "text/markdown",
		Excerpts:    excerpts,
		PublishedAt: time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC),
		PublishedBy: "operator",
	}
}

func TestKnowledgeVersionsAllocateSequentiallyAndResolveByExactHash(t *testing.T) {
	repo, _, _ := repositoryHarness(t)
	store := repo.KnowledgeStore()

	first, err := store.Publish(t.Context(), knowledgeRecord("tenant-a", "policy", knowledgeExcerpt(0, "waiting period is thirty days")))
	require.NoError(t, err)
	assert.Equal(t, uint64(1), first.Ref.Version)

	second, err := store.Publish(t.Context(), knowledgeRecord("tenant-a", "policy",
		knowledgeExcerpt(0, "waiting period is thirty days"), knowledgeExcerpt(1, "exclusions apply to pre-existing conditions")))
	require.NoError(t, err)
	assert.Equal(t, uint64(2), second.Ref.Version)
	assert.NotEqual(t, first.Ref.Hash, second.Ref.Hash)

	resolved, ok, err := store.Resolve(t.Context(), "tenant-a", second.Ref)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, second.Ref.Hash, resolved.Ref.Hash)
	require.Len(t, resolved.Excerpts, 2)
	assert.Equal(t, "waiting period is thirty days", resolved.Excerpts[0].Text)
	assert.Equal(t, 0, resolved.Excerpts[0].Ordinal)
	assert.Equal(t, 1, resolved.Excerpts[1].Ordinal)

	// A hash that does not match the stored content is not a lookup miss with
	// a different body; it simply does not resolve.
	wrongHash := second.Ref
	wrongHash.Hash = strings.Repeat("a", 64)
	_, ok, err = store.Resolve(t.Context(), "tenant-a", wrongHash)
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestKnowledgeResolutionIsTenantScoped(t *testing.T) {
	repo, _, _ := repositoryHarness(t)
	store := repo.KnowledgeStore()

	published, err := store.Publish(t.Context(), knowledgeRecord("tenant-a", "policy", knowledgeExcerpt(0, "tenant a body")))
	require.NoError(t, err)

	_, ok, err := store.Resolve(t.Context(), "tenant-b", published.Ref)
	require.NoError(t, err, "a cross-tenant read is a miss, never an error class that confirms existence")
	assert.False(t, ok)

	// The same pack name in another tenant is a separate version sequence.
	other, err := store.Publish(t.Context(), knowledgeRecord("tenant-b", "policy", knowledgeExcerpt(0, "tenant b body")))
	require.NoError(t, err)
	assert.Equal(t, uint64(1), other.Ref.Version)
}

func TestKnowledgeTombstoneBlocksBindingButKeepsPinnedReadsWorking(t *testing.T) {
	repo, _, _ := repositoryHarness(t)
	store := repo.KnowledgeStore()

	published, err := store.Publish(t.Context(), knowledgeRecord("tenant-a", "policy", knowledgeExcerpt(0, "waiting period is thirty days")))
	require.NoError(t, err)

	at := time.Date(2026, 8, 16, 13, 0, 0, 0, time.UTC)
	require.NoError(t, store.Tombstone(t.Context(), "tenant-a", published.Ref, "operator", at))

	resolved, ok, err := store.Resolve(t.Context(), "tenant-a", published.Ref)
	require.NoError(t, err)
	require.True(t, ok, "work already pinned to this version keeps reading it")
	assert.False(t, resolved.Bindable(), "a tombstoned version may not be pinned by a new publication")
	assert.True(t, resolved.Readable())
	require.NotNil(t, resolved.TombstonedAt)
	assert.Equal(t, "operator", resolved.TombstonedBy)
	require.Len(t, resolved.Excerpts, 1, "tombstoning must not delete excerpts before retention")

	// Tombstoning is not idempotent-by-overwrite: a second attempt reports the
	// version is no longer eligible rather than rewriting provenance.
	assert.ErrorIs(t, store.Tombstone(t.Context(), "tenant-a", published.Ref, "another", at), knowledge.ErrKnowledgeNotFound)
	assert.ErrorIs(t, store.Tombstone(t.Context(), "tenant-b", published.Ref, "operator", at), knowledge.ErrKnowledgeNotFound)
}

func TestKnowledgePublicationRollsBackWhenExcerptsViolateBounds(t *testing.T) {
	repo, _, _ := repositoryHarness(t)
	store := repo.KnowledgeStore()

	oversize := knowledgeExcerpt(0, strings.Repeat("x", knowledge.MaxExcerptBytes+1))
	oversize.Citation.ExcerptHash = knowledge.ExcerptHash(oversize.Text)
	_, err := store.Publish(t.Context(), knowledgeRecord("tenant-a", "policy", oversize))
	assert.ErrorIs(t, err, knowledge.ErrContentLimit)

	gapped := knowledgeRecord("tenant-a", "policy", knowledgeExcerpt(0, "first"), knowledgeExcerpt(4, "second"))
	_, err = store.Publish(t.Context(), gapped)
	assert.ErrorIs(t, err, knowledge.ErrInvalidKnowledge)

	_, err = store.Publish(t.Context(), knowledgeRecord("tenant-a", "policy"))
	assert.ErrorIs(t, err, knowledge.ErrContentLimit)

	// No partial row survived any refusal, so the next real publication is
	// still version 1.
	published, err := store.Publish(t.Context(), knowledgeRecord("tenant-a", "policy", knowledgeExcerpt(0, "first")))
	require.NoError(t, err)
	assert.Equal(t, uint64(1), published.Ref.Version)

	versions, err := store.List(t.Context(), "tenant-a", "policy", 10)
	require.NoError(t, err)
	require.Len(t, versions, 1)
}

func TestKnowledgePublishRejectsAPreviewedHashFromAnotherVersion(t *testing.T) {
	repo, _, _ := repositoryHarness(t)
	store := repo.KnowledgeStore()

	first, err := store.Publish(t.Context(), knowledgeRecord("tenant-a", "policy", knowledgeExcerpt(0, "first")))
	require.NoError(t, err)

	stale := knowledgeRecord("tenant-a", "policy", knowledgeExcerpt(0, "second"))
	stale.Hash = first.Ref.Hash
	_, err = store.Publish(t.Context(), stale)
	assert.ErrorIs(t, err, knowledge.ErrInvalidKnowledge,
		"content reviewed at one version must not publish under another version's identity")
}

func TestKnowledgeSearchReadsOnlyPinnedVersionsAndFailsOnAMissingPin(t *testing.T) {
	repo, _, _ := repositoryHarness(t)
	store := repo.KnowledgeStore()

	pinned, err := store.Publish(t.Context(), knowledgeRecord("tenant-a", "policy",
		knowledgeExcerpt(0, "waiting period is thirty days")))
	require.NoError(t, err)
	newer, err := store.Publish(t.Context(), knowledgeRecord("tenant-a", "policy",
		knowledgeExcerpt(0, "waiting period is sixty days")))
	require.NoError(t, err)

	searcher := knowledge.NewSearcher(store)
	results, err := searcher.Search(t.Context(), knowledge.SearchRequest{
		TenantID: "tenant-a", Refs: []knowledge.VersionRef{pinned.Ref}, Query: "waiting period", MaxResults: 5,
	})
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "waiting period is thirty days", results[0].Text,
		"publishing a newer version must not change what a pinned reader sees")
	assert.Equal(t, pinned.Ref, results[0].Ref, "every result names the exact version behind it")
	assert.NotEqual(t, newer.Ref.Hash, results[0].Ref.Hash)

	absent := pinned.Ref
	absent.Version = 99
	_, err = searcher.Search(t.Context(), knowledge.SearchRequest{
		TenantID: "tenant-a", Refs: []knowledge.VersionRef{absent}, Query: "waiting period",
	})
	assert.ErrorIs(t, err, knowledge.ErrKnowledgeNotFound,
		"answering from less material than the pin promised is worse than failing")

	// Tokens must survive the database round-trip, or ranking silently
	// degrades to phrase-only matching.
	resolved, ok, err := store.Resolve(t.Context(), "tenant-a", pinned.Ref)
	require.NoError(t, err)
	require.True(t, ok)
	assert.NotEmpty(t, resolved.Excerpts[0].Tokens)
	assert.Equal(t, knowledge.Tokenize(resolved.Excerpts[0].Text), resolved.Excerpts[0].Tokens)
}

func TestKnowledgeListIsNewestFirstAndBounded(t *testing.T) {
	repo, _, _ := repositoryHarness(t)
	store := repo.KnowledgeStore()

	for index := range 3 {
		_, err := store.Publish(t.Context(), knowledgeRecord("tenant-a", "policy",
			knowledgeExcerpt(0, strings.Repeat("body ", index+1))))
		require.NoError(t, err)
	}

	versions, err := store.List(t.Context(), "tenant-a", "policy", 2)
	require.NoError(t, err)
	require.Len(t, versions, 2)
	assert.Equal(t, uint64(3), versions[0].Ref.Version)
	assert.Equal(t, uint64(2), versions[1].Ref.Version)
	assert.Empty(t, versions[0].Excerpts, "listing is metadata only, never a content read")

	none, err := store.List(t.Context(), "tenant-b", "policy", 10)
	require.NoError(t, err)
	assert.Empty(t, none)
}
