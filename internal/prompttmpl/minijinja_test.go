package prompttmpl

import (
	"context"
	"encoding/json"
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
		values[i] = json.Number("1")
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

type blockingMarshaler struct {
	entered chan struct{}
	release <-chan struct{}
}

func (m blockingMarshaler) MarshalJSON() ([]byte, error) {
	close(m.entered)
	<-m.release
	return []byte(`"unexpected"`), nil
}

func TestMiniJinjaRejectsCustomMarshalersWithoutCallingThem(t *testing.T) {
	compiled := compile(t, Bundle{Templates: map[string]string{"system.j2": `safe`}})
	entered := make(chan struct{})
	release := make(chan struct{})
	type result struct {
		got string
		err error
	}
	done := make(chan result, 1)
	go func() {
		got, err := compiled.Render(t.Context(), "system.j2", Context{"value": blockingMarshaler{entered: entered, release: release}})
		done <- result{got: got, err: err}
	}()
	select {
	case <-entered:
		close(release)
		<-done
		t.Fatal("custom Marshaler was invoked")
	case result := <-done:
		require.ErrorIs(t, result.err, ErrRenderLimit)
		assert.Empty(t, result.got)
	case <-time.After(time.Second):
		t.Fatal("render did not finish")
	}
}

func TestMiniJinjaCancelledContextSkipsBlockingMarshaler(t *testing.T) {
	compiled := compile(t, Bundle{Templates: map[string]string{"system.j2": `safe`}})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	entered := make(chan struct{})
	release := make(chan struct{})
	type result struct {
		got string
		err error
	}
	done := make(chan result, 1)
	go func() {
		got, err := compiled.Render(ctx, "system.j2", Context{"value": blockingMarshaler{entered: entered, release: release}})
		done <- result{got: got, err: err}
	}()
	select {
	case <-entered:
		close(release)
		<-done
		t.Fatal("cancelled render invoked custom Marshaler")
	case result := <-done:
		require.ErrorIs(t, result.err, ErrRenderLimit)
		assert.Empty(t, result.got)
	case <-time.After(time.Second):
		t.Fatal("cancelled render did not finish")
	}
}

func TestMiniJinjaCountsNestedCollectionItemsGlobally(t *testing.T) {
	compiled := compile(t, Bundle{Templates: map[string]string{"system.j2": `safe`}})
	within := make([]any, DefaultCollectionItems-1)
	for i := range within {
		within[i] = json.Number("1")
	}
	got, err := compiled.Render(t.Context(), "system.j2", Context{"values": within})
	require.NoError(t, err)
	assert.Equal(t, "safe", got)
	tooMany := append(within, json.Number("1"))
	_, err = compiled.Render(t.Context(), "system.j2", Context{"values": tooMany})
	require.ErrorIs(t, err, ErrRenderLimit)
}

func TestMiniJinjaRejectsContextDeeperThanRecursionLimit(t *testing.T) {
	compiled := compile(t, Bundle{Templates: map[string]string{"system.j2": `safe`}})
	var value any = nil
	for i := 0; i < DefaultRecursionDepth; i++ {
		value = map[string]any{"next": value}
	}
	_, err := compiled.Render(t.Context(), "system.j2", Context{"root": value})
	require.ErrorIs(t, err, ErrRenderLimit)
}

func TestMiniJinjaCapsContextBytesDuringOwnedTraversal(t *testing.T) {
	limits := DefaultLimits()
	limits.ContextBytes = 8
	compiled, err := NewMiniJinja().Compile(t.Context(), Bundle{Templates: map[string]string{"system.j2": `safe`}}, limits)
	require.NoError(t, err)
	_, err = compiled.Render(t.Context(), "system.j2", Context{"value": "too long"})
	require.ErrorIs(t, err, ErrRenderLimit)
}

func TestMiniJinjaNormalizesJSONNumbersForArithmeticAndComparison(t *testing.T) {
	compiled := compile(t, Bundle{Templates: map[string]string{
		"system.j2": `{% if whole > 1 and decimal > 1 %}{{ whole + 1 }} {{ decimal * 2 }}{% endif %}`,
	}})
	got, err := compiled.Render(t.Context(), "system.j2", Context{
		"whole":   json.Number("2"),
		"decimal": json.Number("1.25"),
	})
	require.NoError(t, err)
	assert.Equal(t, "3 2.5", got)
}

func TestMiniJinjaRejectsInvalidOrOverflowJSONNumbers(t *testing.T) {
	compiled := compile(t, Bundle{Templates: map[string]string{"system.j2": `safe`}})
	for _, number := range []json.Number{"01", "9223372036854775808", "1e999"} {
		t.Run(string(number), func(t *testing.T) {
			_, err := compiled.Render(t.Context(), "system.j2", Context{"number": number})
			require.ErrorIs(t, err, ErrRenderLimit)
		})
	}
}

func TestMiniJinjaRejectsNonJSONNumericTypes(t *testing.T) {
	compiled := compile(t, Bundle{Templates: map[string]string{"system.j2": `safe`}})
	for _, value := range []any{int(1), int64(1), float64(1)} {
		_, err := compiled.Render(t.Context(), "system.j2", Context{"number": value})
		require.ErrorIs(t, err, ErrRenderLimit)
	}
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
	const sourceProbe = "source-probe-do-not-disclose"
	compiled := compile(t, Bundle{Templates: map[string]string{"system.j2": `{{ value }}` + sourceProbe + `{{ missing }}`}})
	got, err := compiled.Render(t.Context(), "system.j2", Context{"value": secret})
	require.Error(t, err)
	assert.Empty(t, got)
	assert.NotContains(t, strings.ToLower(err.Error()), secret)
	assert.NotContains(t, err.Error(), sourceProbe)
	var renderErr *RenderError
	require.ErrorAs(t, err, &renderErr)
	assert.Equal(t, "system.j2", renderErr.Template)
}

func TestMiniJinjaRenderTimeout(t *testing.T) {
	limits := DefaultLimits()
	limits.RenderTimeout = time.Nanosecond
	compiled, err := NewMiniJinja().Compile(t.Context(), Bundle{Templates: map[string]string{"system.j2": `{% for x in values %}{{ x }}{% endfor %}`}}, limits)
	require.NoError(t, err)
	_, err = compiled.Render(t.Context(), "system.j2", Context{"values": hugeValues()})
	require.ErrorIs(t, err, ErrRenderLimit)
}
