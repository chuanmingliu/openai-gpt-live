package knowledge_test

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/knowledge"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func hashOf(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func excerpt(ordinal int, text string) knowledge.Excerpt {
	return knowledge.Excerpt{
		Ordinal: ordinal,
		Text:    text,
		Tokens:  []string{"coverage"},
		Citation: knowledge.Citation{
			SourceTitle: "policy.md",
			Section:     "Coverage",
			ExcerptHash: hashOf(text),
		},
	}
}

func TestVersionRefValidatesIdentityVersionAndHashExactly(t *testing.T) {
	valid := knowledge.VersionRef{PackID: "policy", Version: 1, Hash: hashOf("body")}
	require.NoError(t, valid.Validate())

	for name, ref := range map[string]knowledge.VersionRef{
		"empty pack":      {PackID: "", Version: 1, Hash: hashOf("body")},
		"control byte":    {PackID: "pol\x00icy", Version: 1, Hash: hashOf("body")},
		"space":           {PackID: "policy pack", Version: 1, Hash: hashOf("body")},
		"zero version":    {PackID: "policy", Version: 0, Hash: hashOf("body")},
		"short hash":      {PackID: "policy", Version: 1, Hash: "abc"},
		"uppercase hash":  {PackID: "policy", Version: 1, Hash: strings.ToUpper(hashOf("body"))},
		"non-hex hash":    {PackID: "policy", Version: 1, Hash: strings.Repeat("z", 64)},
		"oversize pack":   {PackID: strings.Repeat("p", 257), Version: 1, Hash: hashOf("body")},
		"invalid unicode": {PackID: "poli\xffcy", Version: 1, Hash: hashOf("body")},
	} {
		t.Run(name, func(t *testing.T) {
			assert.ErrorIs(t, ref.Validate(), knowledge.ErrInvalidKnowledge)
		})
	}
}

func TestExcerptBoundsAreExactAndCitationHashMustMatchText(t *testing.T) {
	valid := excerpt(0, "waiting period is thirty days")
	require.NoError(t, valid.Validate())

	oversize := excerpt(0, strings.Repeat("x", knowledge.MaxExcerptBytes+1))
	oversize.Citation.ExcerptHash = hashOf(oversize.Text)
	assert.ErrorIs(t, oversize.Validate(), knowledge.ErrContentLimit)

	mismatched := excerpt(0, "waiting period is thirty days")
	mismatched.Citation.ExcerptHash = hashOf("different body")
	assert.ErrorIs(t, mismatched.Validate(), knowledge.ErrInvalidKnowledge)

	negative := excerpt(-1, "body")
	negative.Citation.ExcerptHash = hashOf("body")
	assert.ErrorIs(t, negative.Validate(), knowledge.ErrInvalidKnowledge)

	empty := excerpt(0, "")
	empty.Citation.ExcerptHash = hashOf("")
	assert.ErrorIs(t, empty.Validate(), knowledge.ErrInvalidKnowledge)
}

func TestVersionRequiresDenseOrdinalsAndBoundedAggregate(t *testing.T) {
	excerpts := []knowledge.Excerpt{excerpt(0, "first"), excerpt(1, "second")}
	version := knowledge.Version{
		TenantID:  "tenant-a",
		Ref:       knowledge.VersionRef{PackID: "policy", Version: 1},
		Excerpts:  excerpts,
		Title:     "policy.md",
		MediaType: "text/markdown",
	}
	version.Ref.Hash = knowledge.ContentHash(version)
	require.NoError(t, version.Validate())

	gapped := version.Clone()
	gapped.Excerpts[1].Ordinal = 7
	gapped.Ref.Hash = knowledge.ContentHash(gapped)
	assert.ErrorIs(t, gapped.Validate(), knowledge.ErrInvalidKnowledge)

	duplicated := version.Clone()
	duplicated.Excerpts[1].Ordinal = 0
	duplicated.Ref.Hash = knowledge.ContentHash(duplicated)
	assert.ErrorIs(t, duplicated.Validate(), knowledge.ErrInvalidKnowledge)

	tooMany := version.Clone()
	tooMany.Excerpts = make([]knowledge.Excerpt, knowledge.MaxExcerpts+1)
	for index := range tooMany.Excerpts {
		tooMany.Excerpts[index] = excerpt(index, "body")
	}
	assert.ErrorIs(t, tooMany.Validate(), knowledge.ErrContentLimit)

	empty := version.Clone()
	empty.Excerpts = nil
	assert.ErrorIs(t, empty.Validate(), knowledge.ErrInvalidKnowledge)
}

func TestContentHashCoversExcerptTextCitationsAndOrderOnly(t *testing.T) {
	base := knowledge.Version{
		TenantID:  "tenant-a",
		Ref:       knowledge.VersionRef{PackID: "policy", Version: 1},
		Excerpts:  []knowledge.Excerpt{excerpt(0, "first"), excerpt(1, "second")},
		Title:     "policy.md",
		MediaType: "text/markdown",
	}
	original := knowledge.ContentHash(base)
	assert.Len(t, original, 64)

	reordered := base.Clone()
	reordered.Excerpts[0], reordered.Excerpts[1] = reordered.Excerpts[1], reordered.Excerpts[0]
	assert.NotEqual(t, original, knowledge.ContentHash(reordered), "excerpt order is part of identity")

	edited := base.Clone()
	edited.Excerpts[0].Text = "first edited"
	assert.NotEqual(t, original, knowledge.ContentHash(edited))

	recited := base.Clone()
	recited.Excerpts[0].Citation.Section = "Exclusions"
	assert.NotEqual(t, original, knowledge.ContentHash(recited))

	// Publication bookkeeping is not content and must not perturb identity.
	rebadged := base.Clone()
	rebadged.PublishedBy = "another-operator"
	assert.Equal(t, original, knowledge.ContentHash(rebadged))
}

func TestCloneIsDeepSoStoredVersionsCannotBeMutatedThroughAliases(t *testing.T) {
	base := knowledge.Version{
		TenantID: "tenant-a",
		Ref:      knowledge.VersionRef{PackID: "policy", Version: 1},
		Excerpts: []knowledge.Excerpt{excerpt(0, "first")},
		Title:    "policy.md",
	}
	clone := base.Clone()
	clone.Excerpts[0].Text = "mutated"
	clone.Excerpts[0].Tokens[0] = "mutated"
	clone.Excerpts[0].Citation.Section = "mutated"

	assert.Equal(t, "first", base.Excerpts[0].Text)
	assert.Equal(t, "coverage", base.Excerpts[0].Tokens[0])
	assert.Equal(t, "Coverage", base.Excerpts[0].Citation.Section)
}

func TestTombstonedVersionIsUnbindableButStillReadable(t *testing.T) {
	version := knowledge.Version{
		TenantID: "tenant-a",
		Ref:      knowledge.VersionRef{PackID: "policy", Version: 1},
		Excerpts: []knowledge.Excerpt{excerpt(0, "first")},
		Title:    "policy.md",
	}
	require.True(t, version.Bindable())

	version.TombstonedBy = "operator"
	version.TombstonedAt = knownTime()
	assert.False(t, version.Bindable(), "a tombstoned version may not be pinned by a new publication")
	assert.True(t, version.Readable(), "work already pinned to this version keeps reading it")
}

func knownTime() *time.Time {
	moment := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	return &moment
}
