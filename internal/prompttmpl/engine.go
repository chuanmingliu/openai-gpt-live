// Package prompttmpl defines the bounded, engine-neutral prompt template API.
package prompttmpl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

const (
	DefaultTemplateBytes   = 64 * 1024
	DefaultBundleBytes     = 512 * 1024
	DefaultContextBytes    = 256 * 1024
	DefaultOutputBytes     = 64 * 1024
	DefaultCollectionItems = 1024
	DefaultIncludeDepth    = 8
	DefaultRecursionDepth  = 32
	DefaultFuel            = 1_000_000
	DefaultRenderTimeout   = 250 * time.Millisecond

	MaxTemplateBytes   = DefaultTemplateBytes
	MaxBundleBytes     = DefaultBundleBytes
	MaxContextBytes    = DefaultContextBytes
	MaxOutputBytes     = DefaultOutputBytes
	MaxCollectionItems = DefaultCollectionItems
	MaxIncludeDepth    = DefaultIncludeDepth
	MaxRecursionDepth  = DefaultRecursionDepth
	MaxFuel            = DefaultFuel
	MaxRenderTimeout   = DefaultRenderTimeout

	EngineIdentityPrefix   = "jinja-go-v1/"
	MaxEngineIdentityBytes = 255
)

var (
	ErrTemplateInvalid = errors.New("template_invalid")
	ErrLimitsInvalid   = errors.New("template_limits_invalid")
	ErrRenderLimit     = errors.New("render_limit")
	ErrEngineIdentity  = errors.New("engine_identity_invalid")
)

// ValidateEngineIdentity requires the published Jinja-Go language/policy
// contract and one bounded implementation/version suffix.
func ValidateEngineIdentity(identity string) error {
	if len(identity) <= len(EngineIdentityPrefix) || len(identity) > MaxEngineIdentityBytes || !strings.HasPrefix(identity, EngineIdentityPrefix) {
		return ErrEngineIdentity
	}
	implementation := strings.TrimPrefix(identity, EngineIdentityPrefix)
	segments := strings.Split(implementation, "/")
	for _, segment := range segments {
		if segment == "" || segment == "." || segment == ".." {
			return ErrEngineIdentity
		}
		for index := 0; index < len(segment); index++ {
			value := segment[index]
			if !(value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9' || value == '-' || value == '_' || value == '.') {
				return ErrEngineIdentity
			}
		}
	}
	return nil
}

// Limits bounds every resource supplied to or consumed by an Engine.
type Limits struct {
	TemplateBytes, BundleBytes, ContextBytes, OutputBytes int
	CollectionItems, IncludeDepth, RecursionDepth         int
	Fuel                                                  uint64
	RenderTimeout                                         time.Duration
}

// DefaultLimits returns the largest allowed deterministic rendering budget.
func DefaultLimits() Limits {
	return Limits{
		TemplateBytes:   DefaultTemplateBytes,
		BundleBytes:     DefaultBundleBytes,
		ContextBytes:    DefaultContextBytes,
		OutputBytes:     DefaultOutputBytes,
		CollectionItems: DefaultCollectionItems,
		IncludeDepth:    DefaultIncludeDepth,
		RecursionDepth:  DefaultRecursionDepth,
		Fuel:            DefaultFuel,
		RenderTimeout:   DefaultRenderTimeout,
	}
}

// Validate applies defaults to omitted values and rejects invalid or oversized
// values. The returned value is safe to pass to an Engine.
func (l Limits) Validate() (Limits, error) {
	defaults := DefaultLimits()
	if l.TemplateBytes == 0 {
		l.TemplateBytes = defaults.TemplateBytes
	}
	if l.BundleBytes == 0 {
		l.BundleBytes = defaults.BundleBytes
	}
	if l.ContextBytes == 0 {
		l.ContextBytes = defaults.ContextBytes
	}
	if l.OutputBytes == 0 {
		l.OutputBytes = defaults.OutputBytes
	}
	if l.CollectionItems == 0 {
		l.CollectionItems = defaults.CollectionItems
	}
	if l.IncludeDepth == 0 {
		l.IncludeDepth = defaults.IncludeDepth
	}
	if l.RecursionDepth == 0 {
		l.RecursionDepth = defaults.RecursionDepth
	}
	if l.Fuel == 0 {
		l.Fuel = defaults.Fuel
	}
	if l.RenderTimeout == 0 {
		l.RenderTimeout = defaults.RenderTimeout
	}

	if l.TemplateBytes < 0 || l.TemplateBytes > MaxTemplateBytes ||
		l.BundleBytes < 0 || l.BundleBytes > MaxBundleBytes ||
		l.ContextBytes < 0 || l.ContextBytes > MaxContextBytes ||
		l.OutputBytes < 0 || l.OutputBytes > MaxOutputBytes ||
		l.CollectionItems < 0 || l.CollectionItems > MaxCollectionItems ||
		l.IncludeDepth < 0 || l.IncludeDepth > MaxIncludeDepth ||
		l.RecursionDepth < 0 || l.RecursionDepth > MaxRecursionDepth ||
		l.Fuel > MaxFuel || l.RenderTimeout < 0 || l.RenderTimeout > MaxRenderTimeout {
		return Limits{}, ErrLimitsInvalid
	}
	return l, nil
}

// FieldType describes the permitted JSON-like context value category.
type FieldType string

const (
	TypeString FieldType = "string"
	TypeBool   FieldType = "bool"
	TypeNumber FieldType = "number"
	TypeObject FieldType = "object"
	TypeArray  FieldType = "array"
)

// Field declares one value that a template may receive.
type Field struct {
	Type        FieldType
	Required    bool
	MaxBytes    int
	MaxItems    int
	Templates   []string
	Sensitivity string
	Source      string
}

// ContextSchema constrains values visible to a Bundle's templates.
type ContextSchema struct {
	Fields map[string]Field
}

// Bundle is a logical-name keyed collection of template source.
type Bundle struct {
	Templates map[string]string
	Schema    ContextSchema
}

// validateBundleEnvelope bounds the complete engine-neutral bundle before any
// clone, sort, or fingerprint allocation. Its byte accounting uses the cache's
// length-prefixed canonical primitives while excluding cache format, engine
// identity, and render limits, which are not part of Bundle.
func validateBundleEnvelope(ctx context.Context, bundle Bundle, limits Limits) error {
	ctx = nonNilContext(ctx)
	if ctx.Err() != nil || len(bundle.Templates) > limits.CollectionItems || len(bundle.Schema.Fields) > limits.CollectionItems {
		return ErrTemplateInvalid
	}
	budget := bundleEnvelopeBudget{ctx: ctx, maxBytes: limits.BundleBytes, maxItems: limits.CollectionItems}
	if budget.add(8) != nil { // template count
		return ErrTemplateInvalid
	}
	for name, source := range bundle.Templates {
		if budget.item() != nil || budget.string(name) != nil || validateLogicalNameContext(ctx, name) != nil || len(source) > limits.TemplateBytes || budget.string(source) != nil {
			return ErrTemplateInvalid
		}
	}
	if budget.add(8) != nil { // schema field count
		return ErrTemplateInvalid
	}
	for name, field := range bundle.Schema.Fields {
		if budget.item() != nil || budget.string(name) != nil || budget.string(string(field.Type)) != nil ||
			budget.add(8) != nil || budget.add(8) != nil || budget.add(8) != nil ||
			budget.string(field.Sensitivity) != nil || budget.string(field.Source) != nil || budget.add(8) != nil {
			return ErrTemplateInvalid
		}
		if len(field.Templates) > limits.CollectionItems-budget.items {
			return ErrTemplateInvalid
		}
		for _, template := range field.Templates {
			if budget.item() != nil || budget.string(template) != nil {
				return ErrTemplateInvalid
			}
		}
	}
	return nil
}

type bundleEnvelopeBudget struct {
	ctx                context.Context
	bytes, items       int
	maxBytes, maxItems int
}

func (budget *bundleEnvelopeBudget) add(amount int) error {
	if budget.ctx.Err() != nil || amount < 0 || amount > budget.maxBytes-budget.bytes {
		return ErrTemplateInvalid
	}
	budget.bytes += amount
	return nil
}

func (budget *bundleEnvelopeBudget) item() error {
	if budget.ctx.Err() != nil || budget.items >= budget.maxItems {
		return ErrTemplateInvalid
	}
	budget.items++
	return nil
}

func (budget *bundleEnvelopeBudget) string(value string) error {
	if budget.add(8) != nil || len(value) > budget.maxBytes-budget.bytes {
		return ErrTemplateInvalid
	}
	return budget.add(len(value))
}

// Context is the JSON-like data available to a rendered template.
type Context map[string]any

// CompiledBundle is the implementation-independent result of compilation.
type CompiledBundle interface {
	Render(context.Context, string, Context) (string, error)
	Hash() string
	EngineVersion() string
	ResidentBytes() int
}

// Engine compiles a source Bundle with explicit resource limits.
type Engine interface {
	Version() string
	Compile(context.Context, Bundle, Limits) (CompiledBundle, error)
}

// CompileError identifies a compilation failure without exposing template text.
type CompileError struct {
	Template string
	Err      error
}

func (e *CompileError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Template == "" {
		return "compile: " + compileErrorClass(e.Err)
	}
	return fmt.Sprintf("compile %s: %s", e.Template, compileErrorClass(e.Err))
}

func (e *CompileError) Unwrap() error {
	if e == nil {
		return nil
	}
	return approvedCompileCause(e.Err)
}

// RenderError identifies a render failure without exposing context or output.
type RenderError struct {
	Template string
	Err      error
}

func (e *RenderError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Template == "" {
		return "render: " + renderErrorClass(e.Err)
	}
	return fmt.Sprintf("render %s: %s", e.Template, renderErrorClass(e.Err))
}

func (e *RenderError) Unwrap() error {
	if e == nil {
		return nil
	}
	return approvedRenderCause(e.Err)
}

func compileErrorClass(err error) string {
	if cause := approvedCompileCause(err); cause != nil {
		return cause.Error()
	}
	return "compile_failed"
}

func renderErrorClass(err error) string {
	if cause := approvedRenderCause(err); cause != nil {
		return cause.Error()
	}
	return "render_failed"
}

func approvedCompileCause(err error) error {
	switch {
	case errors.Is(err, ErrTemplateInvalid):
		return ErrTemplateInvalid
	case errors.Is(err, ErrLimitsInvalid):
		return ErrLimitsInvalid
	case errors.Is(err, ErrEngineIdentity):
		return ErrEngineIdentity
	default:
		return nil
	}
}

func approvedRenderCause(err error) error {
	switch {
	case errors.Is(err, ErrRenderLimit):
		return ErrRenderLimit
	case errors.Is(err, ErrLimitsInvalid):
		return ErrLimitsInvalid
	default:
		return nil
	}
}

// CappedWriter writes whole chunks until its byte limit is reached. A chunk
// that would exceed the limit is rejected before it reaches the destination.
type CappedWriter struct {
	dst   io.Writer
	max   int
	wrote int
}

// NewCappedWriter returns a writer that never writes more than max bytes.
func NewCappedWriter(dst io.Writer, max int) *CappedWriter {
	return &CappedWriter{dst: dst, max: max}
}

func (w *CappedWriter) Write(p []byte) (int, error) {
	if w == nil || w.dst == nil || w.max < 0 || len(p) > w.max-w.wrote {
		return 0, ErrRenderLimit
	}
	n, err := w.dst.Write(p)
	w.wrote += n
	if err != nil {
		return n, err
	}
	if n != len(p) {
		return n, io.ErrShortWrite
	}
	return n, nil
}
