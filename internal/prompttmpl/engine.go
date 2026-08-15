// Package prompttmpl defines the bounded, engine-neutral prompt template API.
package prompttmpl

import (
	"context"
	"errors"
	"fmt"
	"io"
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
)

var (
	ErrTemplateInvalid = errors.New("template_invalid")
	ErrLimitsInvalid   = errors.New("template_limits_invalid")
	ErrRenderLimit     = errors.New("render_limit")
)

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

// Context is the JSON-like data available to a rendered template.
type Context map[string]any

// CompiledBundle is the implementation-independent result of compilation.
type CompiledBundle interface {
	Render(context.Context, string, Context) (string, error)
	Hash() string
	EngineVersion() string
}

// Engine compiles a source Bundle with explicit resource limits.
type Engine interface {
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
