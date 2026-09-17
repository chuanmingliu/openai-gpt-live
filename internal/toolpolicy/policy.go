// Package toolpolicy defines the closed-world, server-owned tool policy.
package toolpolicy

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
)

const (
	WebSearchTool = "web_search"

	minimumResults   = 1
	maximumResults   = 10
	minimumTimeoutMS = 1_000
	maximumTimeoutMS = 10_000
)

var defaultRegions = []string{"wt-wt", "us-en", "uk-en", "au-en", "ca-en", "cn-zh"}

type SafeSearch string

const (
	SafeSearchStrict   SafeSearch = "strict"
	SafeSearchModerate SafeSearch = "moderate"
)

type WebSearchPolicy struct {
	Enabled    bool       `json:"enabled"`
	SafeSearch SafeSearch `json:"safe_search"`
	Region     string     `json:"region"`
	MaxResults int        `json:"max_results"`
	TimeoutMS  int        `json:"timeout_ms"`
}

type Policy struct {
	WebSearch WebSearchPolicy `json:"web_search"`
}

type WebSearchCatalog struct {
	Enabled          bool
	ForceStrict      bool
	Regions          []string
	MaximumResults   int
	MaximumTimeoutMS int
}

type Catalog struct {
	WebSearch WebSearchCatalog
}

// DefaultPolicy is the reviewed browser preset. It intentionally does not
// contain destinations, models, commands, or other executor configuration.
func DefaultPolicy() Policy {
	return Policy{WebSearch: WebSearchPolicy{
		Enabled: true, SafeSearch: SafeSearchModerate, Region: "wt-wt",
		MaxResults: 5, TimeoutMS: 8_000,
	}}
}

// DefaultCatalog is the reviewed upper bound for the browser preset.
func DefaultCatalog() Catalog {
	return Catalog{WebSearch: WebSearchCatalog{
		Enabled:          true,
		Regions:          sortedUnique(defaultRegions),
		MaximumResults:   maximumResults,
		MaximumTimeoutMS: maximumTimeoutMS,
	}}
}

// DevelopmentDurableCatalog is the exact reviewed catalog shared by the
// durable gateway composition and its loopback browser/BFF client.
func DevelopmentDurableCatalog() Catalog {
	return Catalog{WebSearch: WebSearchCatalog{
		Enabled: true, ForceStrict: true, Regions: sortedUnique(defaultRegions),
		MaximumResults: 5, MaximumTimeoutMS: 8_000,
	}}
}

// DefaultPolicyForCatalog derives the canonical fail-closed session default
// from one server-owned catalog.
func DefaultPolicyForCatalog(catalog Catalog) (Policy, error) {
	canonical, err := canonicalCatalog(catalog)
	if err != nil {
		return Policy{}, err
	}
	policy := DefaultPolicy()
	web := canonical.WebSearch
	policy.WebSearch.Enabled = web.Enabled
	if web.MaximumResults < policy.WebSearch.MaxResults {
		policy.WebSearch.MaxResults = web.MaximumResults
	}
	if web.MaximumTimeoutMS < policy.WebSearch.TimeoutMS {
		policy.WebSearch.TimeoutMS = web.MaximumTimeoutMS
	}
	if !contains(web.Regions, policy.WebSearch.Region) {
		policy.WebSearch.Region = web.Regions[0]
	}
	return Canonicalize(policy, canonical)
}

// Canonicalize validates a browser policy against the fixed catalog and
// returns its normalized form. A policy can tighten a catalog, never loosen it.
func Canonicalize(policy Policy, catalog Catalog) (Policy, error) {
	canonicalCatalog, err := canonicalCatalog(catalog)
	if err != nil {
		return Policy{}, err
	}
	web := policy.WebSearch
	if !validSafeSearch(web.SafeSearch) {
		return Policy{}, errors.New("web search safe_search is invalid")
	}
	if !contains(canonicalCatalog.WebSearch.Regions, web.Region) {
		return Policy{}, errors.New("web search region is not allowed")
	}
	if web.MaxResults < minimumResults || web.MaxResults > maximumResults || web.MaxResults > canonicalCatalog.WebSearch.MaximumResults {
		return Policy{}, errors.New("web search max_results is not allowed")
	}
	if web.TimeoutMS < minimumTimeoutMS || web.TimeoutMS > maximumTimeoutMS || web.TimeoutMS > canonicalCatalog.WebSearch.MaximumTimeoutMS {
		return Policy{}, errors.New("web search timeout_ms is not allowed")
	}
	if web.Enabled && !canonicalCatalog.WebSearch.Enabled {
		return Policy{}, errors.New("web search is disabled by the catalog")
	}
	if canonicalCatalog.WebSearch.ForceStrict {
		web.SafeSearch = SafeSearchStrict
	}
	return Policy{WebSearch: web}, nil
}

// Decode parses one closed-world policy JSON value. Unknown fields and trailing
// values are rejected before callers bind the policy to a durable handoff.
func Decode(data []byte) (Policy, error) {
	var policy Policy
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&policy); err != nil {
		return Policy{}, fmt.Errorf("decode tool policy: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return Policy{}, errors.New("decode tool policy: trailing JSON value")
		}
		return Policy{}, fmt.Errorf("decode tool policy: trailing JSON: %w", err)
	}
	return policy, nil
}

// MarshalCanonical emits one normalized policy object with a stable field order.
func MarshalCanonical(policy Policy) (json.RawMessage, error) {
	canonical, err := Canonicalize(policy, DefaultCatalog())
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(canonical)
	if err != nil {
		return nil, fmt.Errorf("marshal canonical tool policy: %w", err)
	}
	return json.RawMessage(raw), nil
}

func canonicalCatalog(catalog Catalog) (Catalog, error) {
	web := catalog.WebSearch
	web.Regions = sortedUnique(web.Regions)
	if len(web.Regions) == 0 {
		return Catalog{}, errors.New("web search catalog must include a region")
	}
	for _, region := range web.Regions {
		if !allowedRegion(region) {
			return Catalog{}, errors.New("web search catalog contains an unreviewed region")
		}
	}
	if web.MaximumResults < minimumResults || web.MaximumResults > maximumResults {
		return Catalog{}, errors.New("web search catalog maximum results is invalid")
	}
	if web.MaximumTimeoutMS < minimumTimeoutMS || web.MaximumTimeoutMS > maximumTimeoutMS {
		return Catalog{}, errors.New("web search catalog maximum timeout is invalid")
	}
	return Catalog{WebSearch: web}, nil
}

func validSafeSearch(value SafeSearch) bool {
	return value == SafeSearchStrict || value == SafeSearchModerate
}

func contains(values []string, value string) bool {
	i := sort.SearchStrings(values, value)
	return i < len(values) && values[i] == value
}

func allowedRegion(value string) bool {
	for _, region := range defaultRegions {
		if region == value {
			return true
		}
	}
	return false
}

func sortedUnique(values []string) []string {
	out := append([]string(nil), values...)
	sort.Strings(out)
	return slicesCompact(out)
}

func slicesCompact(values []string) []string {
	if len(values) < 2 {
		return values
	}
	n := 1
	for _, value := range values[1:] {
		if value != values[n-1] {
			values[n] = value
			n++
		}
	}
	return values[:n]
}
