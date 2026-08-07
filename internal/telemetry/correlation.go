package telemetry

import (
	"context"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const maxCorrelationBytes = 128

type Correlation struct {
	SessionID        string
	HandoffID        string
	EventID          string
	ProviderSafetyID string
}
type correlationKey struct{}

func WithCorrelation(ctx context.Context, correlation Correlation) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, correlationKey{}, sanitizeCorrelation(correlation))
}
func CorrelationFromContext(ctx context.Context) (Correlation, bool) {
	if ctx == nil {
		return Correlation{}, false
	}
	c, ok := ctx.Value(correlationKey{}).(Correlation)
	return sanitizeCorrelation(c), ok
}
func StartCorrelated(ctx context.Context, tracer trace.Tracer, name string) (context.Context, trace.Span) {
	c, _ := CorrelationFromContext(ctx)
	attrs := make([]attribute.KeyValue, 0, 4)
	if c.SessionID != "" {
		attrs = append(attrs, attribute.String("session.id", c.SessionID))
	}
	if c.HandoffID != "" {
		attrs = append(attrs, attribute.String("handoff.id", c.HandoffID))
	}
	if c.EventID != "" {
		attrs = append(attrs, attribute.String("event.id", c.EventID))
	}
	if c.ProviderSafetyID != "" {
		attrs = append(attrs, attribute.String("provider.safety_id", c.ProviderSafetyID))
	}
	return tracer.Start(ctx, bounded(name), trace.WithAttributes(attrs...))
}
func sanitizeCorrelation(c Correlation) Correlation {
	c.SessionID = safeCorrelation(c.SessionID)
	c.HandoffID = safeCorrelation(c.HandoffID)
	c.EventID = safeCorrelation(c.EventID)
	c.ProviderSafetyID = safeCorrelation(c.ProviderSafetyID)
	return c
}
func safeCorrelation(v string) string {
	if len(v) > maxCorrelationBytes {
		v = v[:maxCorrelationBytes]
	}
	if strings.ContainsAny(v, "\r\n\x00") {
		return ""
	}
	return strings.Clone(v)
}
