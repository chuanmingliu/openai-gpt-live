package prompttmpl

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"sort"

	minijinja "github.com/mitsuhiko/minijinja/minijinja-go/v2"
	"github.com/mitsuhiko/minijinja/minijinja-go/v2/filters"
	"github.com/mitsuhiko/minijinja/minijinja-go/v2/tests"
)

const miniJinjaVersion = "minijinja-go/v2.24.0"

type miniJinjaEngine struct{}

// NewMiniJinja returns the native, deliberately minimal MiniJinja engine.
func NewMiniJinja() Engine { return miniJinjaEngine{} }

func (miniJinjaEngine) Compile(ctx context.Context, bundle Bundle, requested Limits) (CompiledBundle, error) {
	limits, err := requested.Validate()
	if err != nil {
		return nil, &CompileError{Err: ErrLimitsInvalid}
	}
	if err := ctxErr(ctx); err != nil {
		return nil, &CompileError{Err: ErrTemplateInvalid}
	}
	if err := validateBundleSize(bundle, limits); err != nil {
		return nil, &CompileError{Err: err}
	}
	if err := ValidateBundleSources(bundle.Templates); err != nil {
		return nil, &CompileError{Err: ErrTemplateInvalid}
	}
	if err := validateIncludeDepth(bundle.Templates, limits.IncludeDepth); err != nil {
		return nil, &CompileError{Err: ErrTemplateInvalid}
	}

	owned := cloneBundle(bundle)
	names := sortedTemplateNames(owned.Templates)
	env := minijinja.EmptyEnvironment()
	env.SetUndefinedBehavior(minijinja.UndefinedStrict)
	env.SetRecursionLimit(limits.RecursionDepth)
	fuel := limits.Fuel
	env.SetFuel(&fuel)
	env.SetLoader(func(name string) (string, error) {
		source, ok := owned.Templates[name]
		if !ok {
			return "", minijinja.NewError(minijinja.ErrTemplateNotFound, "template unavailable")
		}
		return source, nil
	})
	env.AddFilter("lower", filters.FilterLower)
	env.AddFilter("upper", filters.FilterUpper)
	env.AddFilter("trim", filters.FilterTrim)
	env.AddFilter("length", filters.FilterLength)
	env.AddFilter("join", filters.FilterJoin)
	env.AddFilter("default", filters.FilterDefault)
	env.AddTest("defined", tests.TestDefined)

	templates := make(map[string]*minijinja.Template, len(names))
	for _, name := range names {
		template, err := env.GetTemplate(name)
		if err != nil {
			return nil, &CompileError{Template: name, Err: ErrTemplateInvalid}
		}
		templates[name] = template
	}
	return &miniJinjaBundle{
		templates: templates,
		limits:    limits,
		hash:      bundleFingerprint(owned, limits),
	}, nil
}

type miniJinjaBundle struct {
	templates map[string]*minijinja.Template
	limits    Limits
	hash      string
}

func (b *miniJinjaBundle) Render(ctx context.Context, name string, values Context) (string, error) {
	template, ok := b.templates[name]
	if !ok {
		return "", &RenderError{Err: nil}
	}
	data, err := boundedJSONContext(values, b.limits.ContextBytes)
	if err != nil {
		return "", &RenderError{Template: name, Err: ErrRenderLimit}
	}
	if err := validateCollectionSize(data, b.limits.CollectionItems); err != nil {
		return "", &RenderError{Template: name, Err: ErrRenderLimit}
	}
	if err := ctxErr(ctx); err != nil {
		return "", &RenderError{Template: name, Err: ErrRenderLimit}
	}
	renderCtx, cancel := context.WithTimeout(nonNilContext(ctx), b.limits.RenderTimeout)
	defer cancel()
	var output bytes.Buffer
	writer := &contextCappedWriter{ctx: renderCtx, capped: NewCappedWriter(&output, b.limits.OutputBytes)}
	if err := template.RenderToWriteCtx(renderCtx, data, writer); err != nil {
		return "", &RenderError{Template: name, Err: normalizedRenderCause(renderCtx, err)}
	}
	if err := ctxErr(renderCtx); err != nil {
		return "", &RenderError{Template: name, Err: ErrRenderLimit}
	}
	return output.String(), nil
}

func (b *miniJinjaBundle) Hash() string          { return b.hash }
func (b *miniJinjaBundle) EngineVersion() string { return miniJinjaVersion }

type contextCappedWriter struct {
	ctx    context.Context
	capped *CappedWriter
}

func (w *contextCappedWriter) Write(p []byte) (int, error) {
	if err := ctxErr(w.ctx); err != nil {
		return 0, ErrRenderLimit
	}
	return w.capped.Write(p)
}

func normalizedRenderCause(ctx context.Context, err error) error {
	if errors.Is(err, ErrRenderLimit) || ctxErr(ctx) != nil {
		return ErrRenderLimit
	}
	var miniErr *minijinja.Error
	if errors.As(err, &miniErr) && (miniErr.Kind == minijinja.ErrOutOfFuel || (miniErr.Kind == minijinja.ErrInvalidOperation && miniErr.Message == "recursion limit exceeded")) {
		return ErrRenderLimit
	}
	return nil
}

func boundedJSONContext(values Context, max int) (any, error) {
	encoded, err := json.Marshal(values)
	if err != nil || len(encoded) > max {
		return nil, ErrRenderLimit
	}
	var normalized any
	if err := json.Unmarshal(encoded, &normalized); err != nil {
		return nil, ErrRenderLimit
	}
	return normalized, nil
}

func validateCollectionSize(value any, max int) error {
	switch value := value.(type) {
	case []any:
		if len(value) > max {
			return ErrRenderLimit
		}
		for _, item := range value {
			if err := validateCollectionSize(item, max); err != nil {
				return err
			}
		}
	case map[string]any:
		if len(value) > max {
			return ErrRenderLimit
		}
		for _, item := range value {
			if err := validateCollectionSize(item, max); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateBundleSize(bundle Bundle, limits Limits) error {
	total := 0
	for name, source := range bundle.Templates {
		if err := validateLogicalName(name); err != nil {
			return ErrTemplateInvalid
		}
		if len(source) > limits.TemplateBytes {
			return ErrTemplateInvalid
		}
		total += len(source)
		if total > limits.BundleBytes {
			return ErrTemplateInvalid
		}
	}
	return nil
}

func validateIncludeDepth(templates map[string]string, maxDepth int) error {
	names := sortedTemplateNames(templates)
	graph := newIncludeGraph(len(names))
	for _, name := range names {
		meta, err := inspectSource(name, templates[name], names)
		if err != nil {
			return err
		}
		graph.Add(name, meta.StaticDependencies)
	}
	return graph.ValidateAcyclic(maxDepth)
}

func nonNilContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func ctxErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

func bundleFingerprint(bundle Bundle, limits Limits) string {
	hash := sha256.New()
	writeBundleFingerprint(hash, bundle, limits)
	return stringHash(hash.Sum(nil))
}

func writeBundleFingerprint(hash hash.Hash, bundle Bundle, limits Limits) {
	for _, name := range sortedTemplateNames(bundle.Templates) {
		fmt.Fprintf(hash, "template:%d:%s:%d:%s\n", len(name), name, len(bundle.Templates[name]), bundle.Templates[name])
	}
	for _, name := range sortedSchemaNames(bundle.Schema.Fields) {
		field := bundle.Schema.Fields[name]
		fmt.Fprintf(hash, "field:%d:%s:%s:%t:%d:%d:%s:%s", len(name), name, field.Type, field.Required, field.MaxBytes, field.MaxItems, field.Sensitivity, field.Source)
		for _, template := range field.Templates {
			fmt.Fprintf(hash, ":%d:%s", len(template), template)
		}
		fmt.Fprintln(hash)
	}
	fmt.Fprintf(hash, "limits:%d:%d:%d:%d:%d:%d:%d:%d:%d", limits.TemplateBytes, limits.BundleBytes, limits.ContextBytes, limits.OutputBytes, limits.CollectionItems, limits.IncludeDepth, limits.RecursionDepth, limits.Fuel, limits.RenderTimeout)
}

func stringHash(sum []byte) string { return fmt.Sprintf("%x", sum) }

func cloneBundle(bundle Bundle) Bundle {
	cloned := Bundle{Templates: make(map[string]string, len(bundle.Templates)), Schema: ContextSchema{Fields: make(map[string]Field, len(bundle.Schema.Fields))}}
	for name, source := range bundle.Templates {
		cloned.Templates[name] = source
	}
	for name, field := range bundle.Schema.Fields {
		field.Templates = append([]string(nil), field.Templates...)
		cloned.Schema.Fields[name] = field
	}
	return cloned
}

func sortedSchemaNames(fields map[string]Field) []string {
	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
