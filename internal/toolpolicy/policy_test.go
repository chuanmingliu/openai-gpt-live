package toolpolicy

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCanonicalizeRejectsPolicyExpansion(t *testing.T) {
	catalog := Catalog{WebSearch: WebSearchCatalog{
		Enabled: true, ForceStrict: true, Regions: []string{"wt-wt", "us-en"},
		MaximumResults: 5, MaximumTimeoutMS: 8_000,
	}}
	_, err := Canonicalize(Policy{WebSearch: WebSearchPolicy{
		Enabled: true, SafeSearch: SafeSearchModerate, Region: "cn-zh",
		MaxResults: 10, TimeoutMS: 10_000,
	}}, catalog)
	require.Error(t, err)
}

func TestCanonicalizeReturnsExactStableJSON(t *testing.T) {
	policy, err := Canonicalize(DefaultPolicy(), DefaultCatalog())
	require.NoError(t, err)
	raw, err := MarshalCanonical(policy)
	require.NoError(t, err)
	assert.JSONEq(t, `{"web_search":{"enabled":true,"safe_search":"moderate","region":"wt-wt","max_results":5,"timeout_ms":8000}}`, string(raw))
}

func TestDevelopmentDurableCatalogProducesExactCanonicalDefault(t *testing.T) {
	catalog := DevelopmentDurableCatalog()
	policy, err := DefaultPolicyForCatalog(catalog)
	require.NoError(t, err)
	assert.Equal(t, Policy{WebSearch: WebSearchPolicy{
		Enabled: true, SafeSearch: SafeSearchStrict, Region: "wt-wt",
		MaxResults: 5, TimeoutMS: 8_000,
	}}, policy)
	assert.Equal(t, []string{"au-en", "ca-en", "cn-zh", "uk-en", "us-en", "wt-wt"}, catalog.WebSearch.Regions)

	disabled := catalog
	disabled.WebSearch.Enabled = false
	policy, err = DefaultPolicyForCatalog(disabled)
	require.NoError(t, err)
	assert.False(t, policy.WebSearch.Enabled)
}

func TestCanonicalizeRejectsInvalidWebSearchPolicies(t *testing.T) {
	base := DefaultPolicy()
	tests := []struct {
		name       string
		catalog    Catalog
		catalogSet bool
		mutate     func(*Policy)
	}{
		{
			name:   "unknown safe search",
			mutate: func(policy *Policy) { policy.WebSearch.SafeSearch = "off" },
		},
		{
			name: "off safe search when disabled",
			mutate: func(policy *Policy) {
				policy.WebSearch.Enabled = false
				policy.WebSearch.SafeSearch = "off"
			},
		},
		{
			name:   "zero results",
			mutate: func(policy *Policy) { policy.WebSearch.MaxResults = 0 },
		},
		{
			name:   "eleven results",
			mutate: func(policy *Policy) { policy.WebSearch.MaxResults = 11 },
		},
		{
			name:   "timeout below minimum",
			mutate: func(policy *Policy) { policy.WebSearch.TimeoutMS = 999 },
		},
		{
			name:   "timeout above maximum",
			mutate: func(policy *Policy) { policy.WebSearch.TimeoutMS = 10_001 },
		},
		{
			name:       "catalog disabled request",
			catalog:    Catalog{WebSearch: WebSearchCatalog{Enabled: false, Regions: []string{"wt-wt"}, MaximumResults: 10, MaximumTimeoutMS: 10_000}},
			catalogSet: true,
			mutate:     func(policy *Policy) { policy.WebSearch.Enabled = true },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy := base
			test.mutate(&policy)
			catalog := test.catalog
			if !test.catalogSet {
				catalog = DefaultCatalog()
			}
			_, err := Canonicalize(policy, catalog)
			require.Error(t, err)
		})
	}
}

func TestCanonicalizeAcceptsUnsortedDuplicateCatalogRegions(t *testing.T) {
	policy := DefaultPolicy()
	policy.WebSearch.Region = "us-en"
	catalog := DefaultCatalog()
	catalog.WebSearch.Regions = []string{"us-en", "wt-wt", "us-en"}

	actual, err := Canonicalize(policy, catalog)
	require.NoError(t, err)
	assert.Equal(t, "us-en", actual.WebSearch.Region)
}

func TestDecodeRejectsUnknownFieldsAndTrailingJSON(t *testing.T) {
	tests := []string{
		`{"web_search":{"enabled":true,"safe_search":"moderate","region":"wt-wt","max_results":5,"timeout_ms":8000,"command":"npx"}}`,
		`{"web_search":{"enabled":true,"safe_search":"moderate","region":"wt-wt","max_results":5,"timeout_ms":8000}} {}`,
	}
	for _, input := range tests {
		_, err := Decode([]byte(input))
		require.Error(t, err)
	}
}
