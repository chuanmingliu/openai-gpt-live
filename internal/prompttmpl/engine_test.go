package prompttmpl

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type cancelAfterChecksContext struct {
	checks, cancelAfter int
}

func (ctx *cancelAfterChecksContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (ctx *cancelAfterChecksContext) Done() <-chan struct{}       { return nil }
func (ctx *cancelAfterChecksContext) Value(any) any               { return nil }
func (ctx *cancelAfterChecksContext) Err() error {
	ctx.checks++
	if ctx.checks >= ctx.cancelAfter {
		return context.Canceled
	}
	return nil
}

func longValidLogicalName(segments int) string {
	return strings.Repeat("a/", segments) + "a.j2"
}

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

func TestCompileAndRenderErrorsKeepUnknownCauseContentPrivate(t *testing.T) {
	secret := errors.New("prompt-or-output-secret-93c682")
	compileErr := &CompileError{Template: "system.j2", Err: secret}
	renderErr := &RenderError{Template: "system.j2", Err: secret}

	require.NotContains(t, compileErr.Error(), secret.Error())
	require.NotContains(t, renderErr.Error(), secret.Error())
	require.Equal(t, "compile system.j2: compile_failed", compileErr.Error())
	require.Equal(t, "render system.j2: render_failed", renderErr.Error())
	require.False(t, errors.Is(compileErr, secret))
	require.False(t, errors.Is(renderErr, secret))
}

func TestValidateEngineIdentityRequiresCompositeJinjaGoVersion(t *testing.T) {
	exactLimit := EngineIdentityPrefix + strings.Repeat("a", MaxEngineIdentityBytes-len(EngineIdentityPrefix))
	for _, identity := range []string{
		"jinja-go-v1/test",
		"jinja-go-v1/minijinja-go/v2.24.0",
		exactLimit,
	} {
		require.NoError(t, ValidateEngineIdentity(identity), identity)
	}

	for _, identity := range []string{
		"",
		"minijinja-go/v2.24.0",
		"jinja-go-v1/",
		"jinja-go-v1//v2.24.0",
		"jinja-go-v1/../v2.24.0",
		"jinja-go-v1/implementation with spaces",
		"jinja-go-v1/implementation\nsecret",
		exactLimit + "a",
	} {
		require.Error(t, ValidateEngineIdentity(identity), identity)
	}
}

func TestBundleEnvelopeChargesOversizedLogicalNameWithoutGrammarAllocation(t *testing.T) {
	limits := DefaultLimits()
	bundle := Bundle{Templates: map[string]string{longValidLogicalName(limits.BundleBytes / 2): "safe"}}
	var err error
	allocations := testing.AllocsPerRun(10, func() {
		err = validateBundleEnvelope(context.Background(), bundle, limits)
	})
	require.ErrorIs(t, err, ErrTemplateInvalid)
	require.Zero(t, allocations, "oversized logical names must be rejected before segment processing")
}
