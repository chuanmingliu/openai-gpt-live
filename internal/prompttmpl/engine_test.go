package prompttmpl

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLimitsValidateAppliesExactDefaults(t *testing.T) {
	got, err := (Limits{}).Validate()
	require.NoError(t, err)
	require.Equal(t, Limits{
		TemplateBytes:   64 * 1024,
		BundleBytes:     512 * 1024,
		ContextBytes:    256 * 1024,
		OutputBytes:     64 * 1024,
		CollectionItems: 1024,
		IncludeDepth:    8,
		RecursionDepth:  32,
		Fuel:            1_000_000,
		RenderTimeout:   250 * time.Millisecond,
	}, got)
}

func TestLimitsValidateRejectsInvalidBounds(t *testing.T) {
	for _, limits := range []Limits{
		{TemplateBytes: -1},
		{BundleBytes: 512*1024 + 1},
		{ContextBytes: 256*1024 + 1},
		{OutputBytes: 64*1024 + 1},
		{CollectionItems: 1025},
		{IncludeDepth: 9},
		{RecursionDepth: 33},
		{Fuel: 1_000_001},
		{RenderTimeout: 251 * time.Millisecond},
	} {
		_, err := limits.Validate()
		require.ErrorIs(t, err, ErrLimitsInvalid, "%+v", limits)
	}
}

func TestCappedWriterRejectsLimitPlusOneWithoutPartialSuccess(t *testing.T) {
	var dst bytes.Buffer
	w := NewCappedWriter(&dst, 4)
	n, err := w.Write([]byte("12345"))
	require.Zero(t, n)
	require.ErrorIs(t, err, ErrRenderLimit)
	require.Empty(t, dst.String())
}

func TestCompileAndRenderErrorsPreserveTheirCause(t *testing.T) {
	compileErr := &CompileError{Template: "system.j2", Err: ErrTemplateInvalid}
	renderErr := &RenderError{Template: "system.j2", Err: ErrRenderLimit}
	require.ErrorIs(t, compileErr, ErrTemplateInvalid)
	require.ErrorIs(t, renderErr, ErrRenderLimit)
	require.NotContains(t, compileErr.Error(), "source")
	require.NotContains(t, renderErr.Error(), "output")
	require.True(t, errors.Is(compileErr, ErrTemplateInvalid))
}
