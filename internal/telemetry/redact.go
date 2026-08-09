// Package telemetry contains bounded, content-safe logging adapters.
package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

const RedactedValue = "[REDACTED]"

const (
	maximumDepth       = 12
	maximumCollection  = 128
	maximumStringBytes = 4096
	maximumLogValues   = 8
)

var defaultDeniedKeys = []string{
	"authorization", "bearer", "token", "access_token", "refresh_token", "cookie", "set_cookie",
	"api_key", "apikey", "password", "passwd", "secret", "client_secret", "database_url", "dsn",
	"audio", "raw_audio", "pcm", "raw_pcm", "base64", "transcript", "tool_arguments", "arguments",
	"payload", "raw_payload", "privacy_hmac_key", "privacy_key", "safety_user_id", "tenant_id", "user_id",
	"error", "err",
}

type redactingHandler struct {
	next      slog.Handler
	denied    map[string]struct{}
	redactAll bool
}

func NewRedactingHandler(next slog.Handler, deniedKeys []string) slog.Handler {
	if next == nil {
		next = slog.NewTextHandler(discardWriter{}, nil)
	}
	denied := make(map[string]struct{}, len(defaultDeniedKeys)+len(deniedKeys))
	for _, key := range append(append([]string(nil), defaultDeniedKeys...), deniedKeys...) {
		if canonical := canonicalKey(key); canonical != "" {
			denied[canonical] = struct{}{}
		}
	}
	return &redactingHandler{next: next, denied: denied}
}

// InstallDefaultRedactingLogger installs the process-wide content-safe logger.
func InstallDefaultRedactingLogger(deniedKeys []string) {
	// Do not wrap slog.Default's initial defaultHandler. SetDefault bridges the
	// standard log package back into the installed slog logger; retaining that
	// handler as the sink would form log -> slog -> log recursion.
	sink := slog.NewTextHandler(os.Stderr, nil)
	slog.SetDefault(slog.New(NewRedactingHandler(sink, deniedKeys)))
}

func (h *redactingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *redactingHandler) Handle(ctx context.Context, record slog.Record) error {
	copy := slog.NewRecord(record.Time, record.Level, record.Message, record.PC)
	record.Attrs(func(attr slog.Attr) bool {
		copy.AddAttrs(h.sanitizeAttr(attr, 0, make(map[visit]struct{})))
		return true
	})
	return h.next.Handle(ctx, copy)
}

func (h *redactingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	owned := make([]slog.Attr, 0, len(attrs))
	for _, attr := range attrs {
		owned = append(owned, h.sanitizeAttr(attr, 0, make(map[visit]struct{})))
	}
	return &redactingHandler{next: h.next.WithAttrs(owned), denied: cloneSet(h.denied), redactAll: h.redactAll}
}

func (h *redactingHandler) WithGroup(name string) slog.Handler {
	return &redactingHandler{next: h.next.WithGroup(name), denied: cloneSet(h.denied), redactAll: h.redactAll || h.isDenied(name)}
}

func (h *redactingHandler) sanitizeAttr(attr slog.Attr, depth int, seen map[visit]struct{}) slog.Attr {
	if h.redactAll || h.isDenied(attr.Key) {
		return slog.String(attr.Key, RedactedValue)
	}
	return slog.Attr{Key: attr.Key, Value: h.sanitizeValue(attr.Value, depth, seen)}
}

func (h *redactingHandler) sanitizeValue(value slog.Value, depth int, seen map[visit]struct{}) slog.Value {
	if depth >= maximumDepth {
		return slog.StringValue("[TRUNCATED]")
	}
	for attempts := 0; value.Kind() == slog.KindLogValuer && attempts < maximumLogValues; attempts++ {
		resolved, ok := resolveLogValue(value)
		if !ok {
			return slog.StringValue(RedactedValue)
		}
		value = resolved
	}
	if value.Kind() == slog.KindLogValuer {
		return slog.StringValue(RedactedValue)
	}
	switch value.Kind() {
	case slog.KindGroup:
		attrs := value.Group()
		if len(attrs) > maximumCollection {
			attrs = attrs[:maximumCollection]
		}
		owned := make([]slog.Attr, 0, len(attrs))
		for _, attr := range attrs {
			owned = append(owned, h.sanitizeAttr(attr, depth+1, seen))
		}
		return slog.GroupValue(owned...)
	case slog.KindString:
		return slog.StringValue(boundString(value.String()))
	case slog.KindAny:
		return slog.AnyValue(h.sanitizeAny(reflect.ValueOf(value.Any()), depth+1, seen))
	default:
		return value
	}
}

type visit struct {
	typ reflect.Type
	ptr uintptr
}

func (h *redactingHandler) sanitizeAny(value reflect.Value, depth int, seen map[visit]struct{}) any {
	if !value.IsValid() {
		return nil
	}
	if depth >= maximumDepth {
		return "[TRUNCATED]"
	}
	if value.Kind() == reflect.Interface {
		if value.IsNil() {
			return nil
		}
		return h.sanitizeAny(value.Elem(), depth+1, seen)
	}
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil
		}
		key := visit{value.Type(), value.Pointer()}
		if _, exists := seen[key]; exists {
			return "[CYCLE]"
		}
		seen[key] = struct{}{}
		defer delete(seen, key)
		return h.sanitizeAny(value.Elem(), depth+1, seen)
	}
	if value.CanInterface() {
		if valuer, ok := value.Interface().(slog.LogValuer); ok {
			resolved, ok := callLogValuer(valuer)
			if !ok {
				return RedactedValue
			}
			return h.sanitizeValue(resolved, depth+1, seen).Any()
		}
		if _, ok := value.Interface().(error); ok {
			return RedactedValue
		}
	}
	switch value.Kind() {
	case reflect.String:
		return boundString(value.String())
	case reflect.Bool:
		return value.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return value.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return value.Uint()
	case reflect.Float32, reflect.Float64:
		return value.Float()
	case reflect.Slice, reflect.Array:
		if value.Type().Elem().Kind() == reflect.Uint8 {
			return RedactedValue
		}
		length := value.Len()
		if length > maximumCollection {
			length = maximumCollection
		}
		out := make([]any, 0, length)
		for index := 0; index < length; index++ {
			out = append(out, h.sanitizeAny(value.Index(index), depth+1, seen))
		}
		return out
	case reflect.Map:
		if value.IsNil() {
			return nil
		}
		if value.Pointer() != 0 {
			key := visit{value.Type(), value.Pointer()}
			if _, exists := seen[key]; exists {
				return "[CYCLE]"
			}
			seen[key] = struct{}{}
			defer delete(seen, key)
		}
		keys := value.MapKeys()
		sort.Slice(keys, func(i, j int) bool { return safeMapKey(keys[i]) < safeMapKey(keys[j]) })
		if len(keys) > maximumCollection {
			keys = keys[:maximumCollection]
		}
		out := make(map[string]any, len(keys))
		for _, key := range keys {
			name := safeMapKey(key)
			if h.isDenied(name) {
				out[name] = RedactedValue
			} else {
				out[name] = h.sanitizeAny(value.MapIndex(key), depth+1, seen)
			}
		}
		return out
	case reflect.Struct:
		out := make(map[string]any)
		typ := value.Type()
		for index := 0; index < value.NumField() && len(out) < maximumCollection; index++ {
			field := typ.Field(index)
			if field.PkgPath != "" {
				continue
			}
			name := field.Name
			if tag := strings.Split(field.Tag.Get("json"), ",")[0]; tag != "" && tag != "-" {
				name = tag
			}
			if name == "-" {
				continue
			}
			if h.isDenied(name) {
				out[name] = RedactedValue
			} else {
				out[name] = h.sanitizeAny(value.Field(index), depth+1, seen)
			}
		}
		return out
	default:
		return RedactedValue
	}
}

func (h *redactingHandler) isDenied(key string) bool {
	_, denied := h.denied[canonicalKey(key)]
	return denied
}

func resolveLogValue(value slog.Value) (resolved slog.Value, ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	return value.LogValuer().LogValue(), true
}

func callLogValuer(value slog.LogValuer) (resolved slog.Value, ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	return value.LogValue(), true
}

func canonicalKey(value string) string {
	var builder strings.Builder
	for _, char := range strings.ToLower(value) {
		if unicode.IsLetter(char) || unicode.IsDigit(char) {
			builder.WriteRune(char)
		}
	}
	return builder.String()
}

func boundString(value string) string {
	if !utf8.ValidString(value) {
		return RedactedValue
	}
	if len(value) <= maximumStringBytes {
		return value
	}
	value = value[:maximumStringBytes]
	for !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value + "[TRUNCATED]"
}

func safeMapKey(value reflect.Value) string {
	if value.IsValid() && value.Kind() == reflect.String {
		return boundString(value.String())
	}
	if value.IsValid() {
		switch value.Kind() {
		case reflect.Bool:
			return fmt.Sprintf("%t", value.Bool())
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return fmt.Sprintf("%d", value.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			return fmt.Sprintf("%d", value.Uint())
		}
	}
	return "invalid_key"
}

func cloneSet(input map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(input))
	for key := range input {
		out[key] = struct{}{}
	}
	return out
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

var _ slog.Handler = (*redactingHandler)(nil)
