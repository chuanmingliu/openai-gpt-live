package prompttmpl

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash"
	"sort"
	"unicode/utf8"

	minijinja "github.com/mitsuhiko/minijinja/minijinja-go/v2"
	"github.com/mitsuhiko/minijinja/minijinja-go/v2/filters"
	"github.com/mitsuhiko/minijinja/minijinja-go/v2/tests"
)

const miniJinjaVersion = "minijinja-go/v2.24.0"

type miniJinjaEngine struct{}

// NewMiniJinja returns the native, deliberately minimal MiniJinja engine.
func NewMiniJinja() Engine { return miniJinjaEngine{} }

func (miniJinjaEngine) Version() string { return miniJinjaVersion }

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
		templates:     templates,
		limits:        limits,
		hash:          bundleFingerprint(owned, limits),
		residentBytes: estimateMiniJinjaResidentBytes(owned),
	}, nil
}

type miniJinjaBundle struct {
	templates     map[string]*minijinja.Template
	limits        Limits
	hash          string
	residentBytes int
}

func (b *miniJinjaBundle) Render(ctx context.Context, name string, values Context) (string, error) {
	template, ok := b.templates[name]
	if !ok {
		return "", &RenderError{Err: nil}
	}
	renderCtx, cancel := context.WithTimeout(nonNilContext(ctx), b.limits.RenderTimeout)
	defer cancel()
	data, err := cloneBoundedContext(renderCtx, values, b.limits)
	if err != nil {
		return "", &RenderError{Template: name, Err: ErrRenderLimit}
	}
	if err := ctxErr(renderCtx); err != nil {
		return "", &RenderError{Template: name, Err: ErrRenderLimit}
	}
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
func (b *miniJinjaBundle) ResidentBytes() int    { return b.residentBytes }

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

type contextBudget struct {
	ctx                context.Context
	bytes, items       int
	maxBytes, maxItems int
}

func cloneBoundedContext(ctx context.Context, values Context, limits Limits) (map[string]any, error) {
	budget := &contextBudget{ctx: ctx, maxBytes: limits.ContextBytes, maxItems: limits.CollectionItems}
	return cloneContextMap(budget, map[string]any(values), 1, limits.RecursionDepth)
}

func cloneContextMap(budget *contextBudget, input map[string]any, depth, maxDepth int) (map[string]any, error) {
	if depth > maxDepth {
		return nil, ErrRenderLimit
	}
	if len(input) > budget.maxItems-budget.items {
		return nil, ErrRenderLimit
	}
	if err := budget.add(1); err != nil { // {
		return nil, err
	}
	cloned := make(map[string]any, len(input))
	for key, value := range input {
		if err := budget.item(); err != nil {
			return nil, err
		}
		if err := budget.jsonString(key); err != nil {
			return nil, err
		}
		if err := budget.add(1); err != nil { // :
			return nil, err
		}
		copied, err := cloneContextValue(budget, value, depth, maxDepth)
		if err != nil {
			return nil, err
		}
		cloned[key] = copied
		if err := budget.add(1); err != nil { // comma or }
			return nil, err
		}
	}
	if len(input) == 0 {
		return cloned, budget.add(1) // }
	}
	return cloned, nil
}

func cloneContextValue(budget *contextBudget, value any, depth, maxDepth int) (any, error) {
	if err := budget.check(); err != nil {
		return nil, err
	}
	switch value := value.(type) {
	case nil:
		return nil, budget.add(4)
	case string:
		return value, budget.jsonString(value)
	case bool:
		if value {
			return value, budget.add(4)
		}
		return value, budget.add(5)
	case json.Number:
		if err := budget.add(len(value)); err != nil {
			return nil, err
		}
		if value == "" || (value[0] != '-' && (value[0] < '0' || value[0] > '9')) || !json.Valid([]byte(value)) {
			return nil, ErrRenderLimit
		}
		return value, nil
	case map[string]any:
		return cloneContextMap(budget, value, depth+1, maxDepth)
	case []any:
		return cloneContextSlice(budget, value, depth+1, maxDepth)
	default:
		return nil, ErrRenderLimit
	}
}

func cloneContextSlice(budget *contextBudget, input []any, depth, maxDepth int) ([]any, error) {
	if depth > maxDepth {
		return nil, ErrRenderLimit
	}
	if len(input) > budget.maxItems-budget.items {
		return nil, ErrRenderLimit
	}
	if err := budget.add(1); err != nil { // [
		return nil, err
	}
	cloned := make([]any, len(input))
	for index, value := range input {
		if err := budget.item(); err != nil {
			return nil, err
		}
		copied, err := cloneContextValue(budget, value, depth, maxDepth)
		if err != nil {
			return nil, err
		}
		cloned[index] = copied
		if err := budget.add(1); err != nil { // comma or ]
			return nil, err
		}
	}
	if len(input) == 0 {
		return cloned, budget.add(1) // ]
	}
	return cloned, nil
}

func (b *contextBudget) item() error {
	if err := b.check(); err != nil {
		return err
	}
	if b.items >= b.maxItems {
		return ErrRenderLimit
	}
	b.items++
	return nil
}

func (b *contextBudget) add(n int) error {
	if err := b.check(); err != nil {
		return err
	}
	if n < 0 || n > b.maxBytes-b.bytes {
		return ErrRenderLimit
	}
	b.bytes += n
	return nil
}

func (b *contextBudget) jsonString(value string) error {
	if !utf8.ValidString(value) {
		return ErrRenderLimit
	}
	if err := b.add(1); err != nil { // opening quote
		return err
	}
	for index, runeValue := range value {
		if index%1024 == 0 {
			if err := b.check(); err != nil {
				return err
			}
		}
		switch {
		case runeValue == '\\' || runeValue == '"', runeValue == '\b' || runeValue == '\f' || runeValue == '\n' || runeValue == '\r' || runeValue == '\t':
			if err := b.add(2); err != nil {
				return err
			}
		case runeValue < 0x20 || runeValue == 0x2028 || runeValue == 0x2029:
			if err := b.add(6); err != nil {
				return err
			}
		default:
			if err := b.add(utf8.RuneLen(runeValue)); err != nil {
				return err
			}
		}
	}
	return b.add(1) // closing quote
}

func (b *contextBudget) check() error {
	if err := ctxErr(b.ctx); err != nil {
		return ErrRenderLimit
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
	return canonicalFingerprint("", bundle, limits)
}

func cacheFingerprint(engineVersion string, bundle Bundle, limits Limits) string {
	return canonicalFingerprint(engineVersion, bundle, limits)
}

func canonicalFingerprint(engineVersion string, bundle Bundle, limits Limits) string {
	digest := sha256.New()
	encodeCanonicalBundle(digest, engineVersion, bundle, limits)
	return string(digest.Sum(nil))
}

func canonicalBundleBytes(engineVersion string, bundle Bundle, limits Limits) int {
	return encodeCanonicalBundle(nil, engineVersion, bundle, limits)
}

func encodeCanonicalBundle(destination hash.Hash, engineVersion string, bundle Bundle, limits Limits) int {
	encoder := canonicalEncoder{destination: destination}
	encoder.string("prompttmpl-cache-v2")
	encoder.string(engineVersion)
	templateNames := sortedTemplateNames(bundle.Templates)
	encoder.uint64(uint64(len(templateNames)))
	for _, name := range templateNames {
		encoder.string(name)
		encoder.string(bundle.Templates[name])
	}
	fieldNames := sortedSchemaNames(bundle.Schema.Fields)
	encoder.uint64(uint64(len(fieldNames)))
	for _, name := range fieldNames {
		field := bundle.Schema.Fields[name]
		encoder.string(name)
		encoder.string(string(field.Type))
		encoder.bool(field.Required)
		encoder.int(field.MaxBytes)
		encoder.int(field.MaxItems)
		encoder.string(field.Sensitivity)
		encoder.string(field.Source)
		templates := append([]string(nil), field.Templates...)
		sort.Strings(templates)
		encoder.uint64(uint64(len(templates)))
		for _, template := range templates {
			encoder.string(template)
		}
	}
	encoder.int(limits.TemplateBytes)
	encoder.int(limits.BundleBytes)
	encoder.int(limits.ContextBytes)
	encoder.int(limits.OutputBytes)
	encoder.int(limits.CollectionItems)
	encoder.int(limits.IncludeDepth)
	encoder.int(limits.RecursionDepth)
	encoder.uint64(limits.Fuel)
	encoder.int64(int64(limits.RenderTimeout))
	return encoder.bytes
}

type canonicalEncoder struct {
	destination hash.Hash
	bytes       int
}

func (e *canonicalEncoder) string(value string) {
	e.uint64(uint64(len(value)))
	e.bytes += len(value)
	if e.destination != nil {
		_, _ = e.destination.Write([]byte(value))
	}
}

func (e *canonicalEncoder) bool(value bool) {
	if value {
		e.uint64(1)
		return
	}
	e.uint64(0)
}

func (e *canonicalEncoder) int(value int) { e.int64(int64(value)) }

func (e *canonicalEncoder) int64(value int64) { e.uint64(uint64(value)) }

func (e *canonicalEncoder) uint64(value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	e.bytes += len(encoded)
	if e.destination != nil {
		_, _ = e.destination.Write(encoded[:])
	}
}

func estimateMiniJinjaResidentBytes(bundle Bundle) int {
	bytes := 1024
	for name, source := range bundle.Templates {
		bytes += 256 + len(name) + 4*len(source)
	}
	return bytes
}

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
