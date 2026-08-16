package knowledge_test

import (
	"strings"
	"testing"

	"github.com/chuanmingliu/openai-gpt-live/internal/knowledge"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func versionWith(packID string, number uint64, bodies ...string) knowledge.Version {
	excerpts := make([]knowledge.Excerpt, 0, len(bodies))
	for ordinal, body := range bodies {
		excerpts = append(excerpts, knowledge.Excerpt{
			Ordinal: ordinal,
			Text:    body,
			Tokens:  knowledge.Tokenize(body),
			Citation: knowledge.Citation{
				SourceTitle: packID + ".md",
				Section:     "Coverage",
				ExcerptHash: knowledge.ExcerptHash(body),
			},
		})
	}
	version := knowledge.Version{
		TenantID: "tenant-a",
		Ref:      knowledge.VersionRef{PackID: packID, Version: number},
		Title:    packID + ".md",
		Excerpts: excerpts,
	}
	version.Ref.Hash = knowledge.ContentHash(version)
	return version
}

func TestRankingPrefersPhraseThenTokenOverlap(t *testing.T) {
	version := versionWith("policy", 1,
		"the waiting period is thirty days from the effective date",
		"waiting rooms are available at every clinic",
		"unrelated text about parking validation",
	)
	results, err := knowledge.Rank("waiting period", 3, []knowledge.Version{version})
	require.NoError(t, err)
	require.NotEmpty(t, results)

	assert.Equal(t, 0, results[0].Ordinal, "the exact phrase must outrank an incidental token match")
	assert.Contains(t, results[0].Text, "waiting period")
	for index := 1; index < len(results); index++ {
		assert.LessOrEqual(t, results[index].Score, results[index-1].Score, "results are score-ordered")
	}
	for _, result := range results {
		assert.NotContains(t, result.Text, "parking validation", "a zero-overlap excerpt is not a result")
	}
}

func TestRankingIsStableAndTiesBreakByPackVersionOrdinal(t *testing.T) {
	// Identical text in three places: only the tie-break can order these.
	body := "waiting period is thirty days"
	versions := []knowledge.Version{
		versionWith("zeta", 1, body),
		versionWith("alpha", 2, body),
		versionWith("alpha", 1, body, body),
	}
	first, err := knowledge.Rank("waiting period", 10, versions)
	require.NoError(t, err)
	require.Len(t, first, 4)

	assert.Equal(t, "alpha", first[0].Ref.PackID)
	assert.Equal(t, uint64(1), first[0].Ref.Version)
	assert.Equal(t, 0, first[0].Ordinal)
	assert.Equal(t, "alpha", first[1].Ref.PackID)
	assert.Equal(t, uint64(1), first[1].Ref.Version)
	assert.Equal(t, 1, first[1].Ordinal)
	assert.Equal(t, uint64(2), first[2].Ref.Version)
	assert.Equal(t, "zeta", first[3].Ref.PackID)

	// Input order must not influence output order.
	shuffled := []knowledge.Version{versions[2], versions[0], versions[1]}
	second, err := knowledge.Rank("waiting period", 10, shuffled)
	require.NoError(t, err)
	assert.Equal(t, first, second)
}

func TestCJKRankingPrefersBigramMatchesOverIncidentalRunes(t *testing.T) {
	version := versionWith("policy", 1,
		"保险等待期为三十天",
		"等候室位于一楼",
		"停车场规定与本保单无关",
	)
	results, err := knowledge.Rank("保险 等待期", 3, []knowledge.Version{version})
	require.NoError(t, err)
	require.NotEmpty(t, results)
	assert.Equal(t, 0, results[0].Ordinal, "保险 and 等待 bigrams must outrank a single shared rune")
	assert.Contains(t, results[0].Text, "等待期")
}

func TestQueryBoundsAndResultCountAreExact(t *testing.T) {
	version := versionWith("policy", 1, "waiting period is thirty days")

	for name, query := range map[string]string{
		"empty":        "",
		"whitespace":   "   ",
		"oversize":     strings.Repeat("a", 513),
		"control byte": "waiting\x00period",
		"no tokens":    "!!! ??? ...",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := knowledge.Rank(query, 5, []knowledge.Version{version})
			assert.ErrorIs(t, err, knowledge.ErrInvalidKnowledge)
		})
	}

	for _, count := range []int{0, -1, 11} {
		_, err := knowledge.Rank("waiting", count, []knowledge.Version{version})
		assert.ErrorIs(t, err, knowledge.ErrInvalidKnowledge, "max results %d", count)
	}

	many := versionWith("policy", 1,
		"waiting period one", "waiting period two", "waiting period three", "waiting period four")
	results, err := knowledge.Rank("waiting period", 2, []knowledge.Version{many})
	require.NoError(t, err)
	assert.Len(t, results, 2, "the caller's ceiling is exact")
}

func TestResultsCarryTheirExactPinnedProvenance(t *testing.T) {
	version := versionWith("policy", 3, "waiting period is thirty days")
	results, err := knowledge.Rank("waiting period", 5, []knowledge.Version{version})
	require.NoError(t, err)
	require.Len(t, results, 1)

	assert.Equal(t, "policy", results[0].Ref.PackID)
	assert.Equal(t, uint64(3), results[0].Ref.Version)
	assert.Equal(t, version.Ref.Hash, results[0].Ref.Hash)
	assert.Equal(t, "policy.md", results[0].Citation.SourceTitle)
	assert.Equal(t, knowledge.ExcerptHash(results[0].Text), results[0].Citation.ExcerptHash)
	assert.Contains(t, []knowledge.Confidence{knowledge.ConfidenceHigh, knowledge.ConfidenceMedium, knowledge.ConfidenceLow},
		results[0].Confidence)
}

func TestAggregateResultBytesAreBounded(t *testing.T) {
	big := strings.Repeat("waiting period ", 270) // ~4 KiB per excerpt
	bodies := make([]string, 0, 32)
	for index := 0; index < 32; index++ {
		bodies = append(bodies, big)
	}
	version := versionWith("policy", 1, bodies...)

	results, err := knowledge.Rank("waiting period", 10, []knowledge.Version{version})
	require.NoError(t, err)
	total := 0
	for _, result := range results {
		total += len(result.Text)
	}
	assert.LessOrEqual(t, total, knowledge.MaxSearchResultBytes)
}

func TestRankingReadsOnlyTheVersionsItWasGiven(t *testing.T) {
	pinned := versionWith("policy", 1, "waiting period is thirty days")
	newer := versionWith("policy", 2, "waiting period is sixty days")

	results, err := knowledge.Rank("waiting period", 5, []knowledge.Version{pinned})
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Contains(t, results[0].Text, "thirty")
	assert.NotEqual(t, newer.Ref.Hash, results[0].Ref.Hash,
		"publishing a newer version cannot change what a pinned reader sees")

	_, err = knowledge.Rank("waiting period", 5, nil)
	assert.ErrorIs(t, err, knowledge.ErrInvalidKnowledge, "a search with nothing pinned is not an empty result")
}
