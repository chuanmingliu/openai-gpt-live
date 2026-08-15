package prompttmpl

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func compile(t *testing.T, bundle Bundle) CompiledBundle {
	t.Helper()
	compiled, err := NewMiniJinja().Compile(t.Context(), bundle, DefaultLimits())
	require.NoError(t, err)
	return compiled
}

func render(t *testing.T, source string, values Context) (string, error) {
	t.Helper()
	compiled := compile(t, Bundle{Templates: map[string]string{"system.j2": source}})
	return compiled.Render(t.Context(), "system.j2", values)
}

func renderWithFuel(t *testing.T, source string, values Context, fuel uint64) (string, error) {
	t.Helper()
	limits := DefaultLimits()
	limits.Fuel = fuel
	compiled, err := NewMiniJinja().Compile(t.Context(), Bundle{Templates: map[string]string{"system.j2": source}}, limits)
	require.NoError(t, err)
	return compiled.Render(t.Context(), "system.j2", values)
}

func hugeValues() []any {
	values := make([]any, DefaultCollectionItems)
	for i := range values {
		values[i] = i
	}
	return values
}

func TestMiniJinjaStrictStaticBundle(t *testing.T) {
	compiled := compile(t, Bundle{Templates: map[string]string{
		"system.j2":          `{% include "fragments/rules.j2" %} {{ customer.name }}`,
		"fragments/rules.j2": `Use approved facts only.`,
	}})
	got, err := compiled.Render(t.Context(), "system.j2", Context{"customer": map[string]any{"name": "李明"}})
	require.NoError(t, err)
	assert.Equal(t, "Use approved facts only. 李明", got)
}

func TestMiniJinjaRejectsUndefinedAndOutOfFuel(t *testing.T) {
	_, err := render(t, `{{ missing }}`, Context{})
	require.Error(t, err)
	_, err = renderWithFuel(t, `{% for x in values %}{{ x }}{% endfor %}`, Context{"values": hugeValues()}, 8)
	require.ErrorIs(t, err, ErrRenderLimit)
}

func TestMiniJinjaCancellationReturnsRenderLimit(t *testing.T) {
	compiled := compile(t, Bundle{Templates: map[string]string{"system.j2": `{% for x in values %}{{ x }}{% endfor %}`}})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := compiled.Render(ctx, "system.j2", Context{"values": hugeValues()})
	require.ErrorIs(t, err, ErrRenderLimit)
}

func TestMiniJinjaRejectsRecursion(t *testing.T) {
	compiled := compile(t, Bundle{Templates: map[string]string{
		"system.j2": `{% macro recurse() %}{{ recurse() }}{% endmacro %}{{ recurse() }}`,
	}})
	_, err := compiled.Render(t.Context(), "system.j2", Context{})
	require.ErrorIs(t, err, ErrRenderLimit)
}

func TestMiniJinjaCapsOutput(t *testing.T) {
	limits := DefaultLimits()
	limits.OutputBytes = 4
	compiled, err := NewMiniJinja().Compile(t.Context(), Bundle{Templates: map[string]string{"system.j2": `{{ value }}`}}, limits)
	require.NoError(t, err)
	_, err = compiled.Render(t.Context(), "system.j2", Context{"value": "12345"})
	require.ErrorIs(t, err, ErrRenderLimit)
}

func TestMiniJinjaAllowsMacroOnlyCalls(t *testing.T) {
	got, err := render(t, `{% macro greeting(name) %}Hi {{ name }}{% endmacro %}{{ greeting("Ada") }}`, Context{})
	require.NoError(t, err)
	assert.Equal(t, "Hi Ada", got)
}

func TestMiniJinjaRejectsDynamicInclude(t *testing.T) {
	_, err := NewMiniJinja().Compile(t.Context(), Bundle{Templates: map[string]string{
		"system.j2": `{% include target %}`,
		"part.j2":   "safe",
	}}, DefaultLimits())
	require.ErrorIs(t, err, ErrTemplateInvalid)
}

func TestMiniJinjaHonorsConfiguredIncludeDepth(t *testing.T) {
	limits := DefaultLimits()
	limits.IncludeDepth = 1
	_, err := NewMiniJinja().Compile(t.Context(), Bundle{Templates: map[string]string{
		"system.j2": `{% include "part.j2" %}`,
		"part.j2":   `{% include "leaf.j2" %}`,
		"leaf.j2":   "safe",
	}}, limits)
	require.ErrorIs(t, err, ErrTemplateInvalid)
}

func TestMiniJinjaRejectsMethodAndReflection(t *testing.T) {
	_, err := NewMiniJinja().Compile(t.Context(), Bundle{Templates: map[string]string{"system.j2": `{{ customer.name.upper() }}`}}, DefaultLimits())
	require.ErrorIs(t, err, ErrTemplateInvalid)
	compiled := compile(t, Bundle{Templates: map[string]string{"system.j2": `{{ customer.__class__ }}`}})
	_, err = compiled.Render(t.Context(), "system.j2", Context{"customer": map[string]any{"name": "Ada"}})
	require.Error(t, err)
}

func TestMiniJinjaRenderIsConcurrent(t *testing.T) {
	compiled := compile(t, Bundle{Templates: map[string]string{"system.j2": `{{ customer.name | upper }}`}})
	const workers = 32
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := compiled.Render(context.Background(), "system.j2", Context{"customer": map[string]any{"name": "ada"}})
			if err != nil {
				errs <- err
				return
			}
			if got != "ADA" {
				errs <- errors.New("unexpected render output")
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

func TestMiniJinjaErrorsNeverExposeSourceOrRenderedBytes(t *testing.T) {
	const secret = "credential=do-not-disclose"
	_, err := NewMiniJinja().Compile(t.Context(), Bundle{Templates: map[string]string{"system.j2": "{{ " + secret + ".append(1) }}"}}, DefaultLimits())
	require.Error(t, err)
	assert.NotContains(t, err.Error(), secret)
	compiled := compile(t, Bundle{Templates: map[string]string{"system.j2": `{{ value }}`}})
	_, err = compiled.Render(t.Context(), "system.j2", Context{"value": secret})
	require.NoError(t, err)
	assert.NotContains(t, strings.ToLower((&RenderError{Template: "system.j2", Err: err}).Error()), secret)
}

func TestMiniJinjaRenderTimeout(t *testing.T) {
	limits := DefaultLimits()
	limits.RenderTimeout = time.Nanosecond
	compiled, err := NewMiniJinja().Compile(t.Context(), Bundle{Templates: map[string]string{"system.j2": `{% for x in values %}{{ x }}{% endfor %}`}}, limits)
	require.NoError(t, err)
	_, err = compiled.Render(t.Context(), "system.j2", Context{"values": hugeValues()})
	require.ErrorIs(t, err, ErrRenderLimit)
}
