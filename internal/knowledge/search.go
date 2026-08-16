package knowledge

import (
	"context"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	// MaxQueryBytes bounds one search query.
	MaxQueryBytes = 512
	// MaxSearchResults bounds how many excerpts one search may return.
	MaxSearchResults = 10
	// MaxSearchResultBytes bounds the aggregate excerpt text of one response.
	MaxSearchResultBytes = 64 << 10

	// Scoring weights. Integers only: floating point would make ordering
	// depend on rounding, and a citation that reorders between runs is not a
	// citation. Phrase dominates so an exact hit cannot lose to many weak
	// token overlaps.
	phraseWeight   = 1000
	tokenWeight    = 10
	frequencyCap   = 8
	confidenceHigh = phraseWeight
	confidenceMed  = tokenWeight * 3
)

// Confidence is a coarse, stable class. It is deliberately not a percentage:
// a lexical score has no calibrated probability behind it, and presenting one
// would invite more trust than the ranking earns.
type Confidence string

const (
	ConfidenceHigh   Confidence = "high"
	ConfidenceMedium Confidence = "medium"
	ConfidenceLow    Confidence = "low"
)

// Result is one ranked excerpt together with the exact pinned version it came
// from, so every answer can be traced to reviewed bytes.
type Result struct {
	Ref        VersionRef `json:"ref"`
	Ordinal    int        `json:"ordinal"`
	Text       string     `json:"text"`
	Citation   Citation   `json:"citation"`
	Score      int        `json:"score"`
	Confidence Confidence `json:"confidence"`
}

// Rank scores every excerpt of the supplied versions against the query and
// returns the best matches, highest first.
//
// It is a pure function of its inputs: the caller decides which versions are in
// scope, so a search can never widen beyond what a handoff pinned. Ordering is
// total - score, then pack, version, ordinal - so identical inputs always
// produce an identical list regardless of input order or platform.
func Rank(query string, maxResults int, versions []Version) ([]Result, error) {
	if maxResults < 1 || maxResults > MaxSearchResults {
		return nil, ErrInvalidKnowledge
	}
	if len(versions) == 0 {
		return nil, ErrInvalidKnowledge
	}
	normalizedQuery, queryTokens, err := prepareQuery(query)
	if err != nil {
		return nil, err
	}

	results := make([]Result, 0, maxResults*4)
	for _, version := range versions {
		if version.Ref.PackID == "" {
			return nil, ErrInvalidKnowledge
		}
		for _, excerpt := range version.Excerpts {
			score := scoreExcerpt(normalizedQuery, queryTokens, excerpt)
			if score == 0 {
				continue
			}
			results = append(results, Result{
				Ref:        version.Ref,
				Ordinal:    excerpt.Ordinal,
				Text:       excerpt.Text,
				Citation:   excerpt.Citation,
				Score:      score,
				Confidence: confidenceFor(score),
			})
		}
	}

	sort.SliceStable(results, func(left, right int) bool {
		if results[left].Score != results[right].Score {
			return results[left].Score > results[right].Score
		}
		if results[left].Ref.PackID != results[right].Ref.PackID {
			return results[left].Ref.PackID < results[right].Ref.PackID
		}
		if results[left].Ref.Version != results[right].Ref.Version {
			return results[left].Ref.Version < results[right].Ref.Version
		}
		return results[left].Ordinal < results[right].Ordinal
	})

	if len(results) > maxResults {
		results = results[:maxResults]
	}
	// Trim by aggregate bytes last, so the byte ceiling never changes which
	// excerpt ranks first - only how many of them are returned.
	total := 0
	for index, result := range results {
		total += len(result.Text)
		if total > MaxSearchResultBytes {
			return results[:index], nil
		}
	}
	return results, nil
}

// Searcher ranks over exactly the versions a caller has pinned, resolving each
// one from the store first.
type Searcher struct{ store Store }

func NewSearcher(store Store) *Searcher { return &Searcher{store: store} }

// Search resolves every pinned reference and ranks across them. A reference
// that no longer resolves is a hard failure: answering from less material than
// the pin promised would silently narrow the evidence behind a citation.
func (searcher *Searcher) Search(ctx context.Context, request SearchRequest) ([]Result, error) {
	if searcher == nil || searcher.store == nil {
		return nil, ErrStoreUnavailable
	}
	if err := contextErr(ctx); err != nil {
		return nil, err
	}
	if !validIdentifier(request.TenantID) || len(request.Refs) == 0 {
		return nil, ErrInvalidKnowledge
	}
	versions := make([]Version, 0, len(request.Refs))
	for _, ref := range request.Refs {
		if err := ref.Validate(); err != nil {
			return nil, err
		}
		version, ok, err := searcher.store.Resolve(ctx, request.TenantID, ref)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, ErrKnowledgeNotFound
		}
		versions = append(versions, version)
	}
	maxResults := request.MaxResults
	if maxResults == 0 {
		maxResults = 5
	}
	return Rank(request.Query, maxResults, versions)
}

func prepareQuery(query string) (string, []string, error) {
	if query == "" || len(query) > MaxQueryBytes || !utf8.ValidString(query) {
		return "", nil, ErrInvalidKnowledge
	}
	for _, character := range query {
		if unicode.IsControl(character) {
			return "", nil, ErrInvalidKnowledge
		}
	}
	normalized := strings.ToLower(strings.Join(strings.Fields(query), " "))
	if normalized == "" {
		return "", nil, ErrInvalidKnowledge
	}
	tokens := Tokenize(normalized)
	if len(tokens) == 0 {
		return "", nil, ErrInvalidKnowledge
	}
	return normalized, tokens, nil
}

// scoreExcerpt combines an exact phrase hit with bounded token overlap. Term
// frequency is capped so one excerpt cannot win by repeating a term.
func scoreExcerpt(normalizedQuery string, queryTokens []string, excerpt Excerpt) int {
	lowered := strings.ToLower(excerpt.Text)
	score := 0
	if strings.Contains(lowered, normalizedQuery) {
		score += phraseWeight
	}

	present := make(map[string]struct{}, len(excerpt.Tokens))
	for _, token := range excerpt.Tokens {
		present[token] = struct{}{}
	}
	for _, token := range queryTokens {
		if _, ok := present[token]; !ok {
			continue
		}
		occurrences := strings.Count(lowered, token)
		if occurrences > frequencyCap {
			occurrences = frequencyCap
		}
		if occurrences == 0 {
			occurrences = 1
		}
		score += tokenWeight * occurrences
	}
	return score
}

func confidenceFor(score int) Confidence {
	switch {
	case score >= confidenceHigh:
		return ConfidenceHigh
	case score >= confidenceMed:
		return ConfidenceMedium
	default:
		return ConfidenceLow
	}
}
