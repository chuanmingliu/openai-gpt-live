package prompttmpl

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type corpusCase struct {
	Name      string            `json:"name"`
	Accepted  bool              `json:"accepted"`
	Templates map[string]string `json:"templates"`
}

func loadCorpus(t *testing.T) []corpusCase {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "jinja_go_v1.json"))
	require.NoError(t, err)
	var cases []corpusCase
	require.NoError(t, json.Unmarshal(data, &cases))
	return cases
}

func TestJinjaGoV1Corpus(t *testing.T) {
	for _, tc := range loadCorpus(t) {
		t.Run(tc.Name, func(t *testing.T) {
			err := ValidateBundleSources(tc.Templates)
			if tc.Accepted {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, ErrTemplateInvalid)
			}
		})
	}
}

func TestValidateSourceIgnoresDelimiterInsideQuotedString(t *testing.T) {
	err := ValidateSource("system.j2", `{{ "a }} b" }}`, []string{"system.j2"})
	require.NoError(t, err)
}

func TestValidateSourceAllowsGroupingAndIndexingWithoutCalls(t *testing.T) {
	err := ValidateSource("system.j2", `{% if not (customer.blocked) %}{{ products[0].label }}{% endif %}`, []string{"system.j2"})
	require.NoError(t, err)
}

func TestValidateSourceReturnsContentFreeLocation(t *testing.T) {
	secret := "credential=do-not-disclose"
	err := ValidateSource("system.j2", "{{ "+secret+".append(1) }}", []string{"system.j2"})
	require.ErrorIs(t, err, ErrTemplateInvalid)
	require.Contains(t, err.Error(), "template_invalid: system.j2:1:")
	require.NotContains(t, err.Error(), secret)
}

func TestValidateLogicalNameScansWithoutAllocation(t *testing.T) {
	name := longValidLogicalName(64)
	var err error
	allocations := testing.AllocsPerRun(100, func() {
		err = validateLogicalName(name)
	})
	require.NoError(t, err)
	require.Zero(t, allocations)
}

func TestValidateSourceRejectsCallInMacroSignature(t *testing.T) {
	err := ValidateSource("system.j2", `{% macro greeting(name=helper()) %}hi{% endmacro %}`, []string{"system.j2"})
	require.ErrorIs(t, err, ErrTemplateInvalid)
}

func TestValidateBundleSourcesRejectsNinthInclude(t *testing.T) {
	templates := map[string]string{}
	for i := 0; i < 10; i++ {
		name := "part" + string(rune('a'+i)) + ".j2"
		if i == 9 {
			templates[name] = "done"
			continue
		}
		next := "part" + string(rune('a'+i+1)) + ".j2"
		templates[name] = `{% include "` + next + `" %}`
	}
	require.ErrorIs(t, ValidateBundleSources(templates), ErrTemplateInvalid)
}

func TestValidateBundleSourcesChecksSharedSubgraphAtEveryRootDepth(t *testing.T) {
	templates := map[string]string{
		"a.j2": `{% include "b.j2" %}`,
		"b.j2": `{% include "c.j2" %}`,
		"c.j2": "done",
		"d.j2": `{% include "a.j2" %}`,
		"e.j2": `{% include "d.j2" %}`,
		"f.j2": `{% include "e.j2" %}`,
		"g.j2": `{% include "f.j2" %}`,
		"h.j2": `{% include "g.j2" %}`,
		"i.j2": `{% include "h.j2" %}`,
		"j.j2": `{% include "i.j2" %}`,
	}
	require.ErrorIs(t, ValidateBundleSources(templates), ErrTemplateInvalid)
}

func FuzzValidateSourceNeverPanics(f *testing.F) {
	for _, seed := range []string{"", "{{ customer.name }}", "{% include \"part.j2\" %}", "{{ \"}}\" }}", strings.Repeat("{", 64)} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, source string) {
		_ = ValidateSource("system.j2", source, []string{"system.j2", "part.j2"})
	})
}
