package telemetry

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestRuntimeRequiresSecureProductionExporter(t *testing.T) {
	if _, err := NewRuntime(context.Background(), RuntimeConfig{Production: true, Endpoint: "http://127.0.0.1:4318"}); err == nil {
		t.Fatal("accepted insecure exporter")
	}
}

type recoveringExporter struct {
	failing   atomic.Bool
	exports   atomic.Int64
	shutdowns atomic.Int64
}

func (e *recoveringExporter) ExportSpans(context.Context, []sdktrace.ReadOnlySpan) error {
	e.exports.Add(1)
	if e.failing.Load() {
		return errors.New("collector unavailable")
	}
	return nil
}
func (e *recoveringExporter) Shutdown(context.Context) error {
	e.shutdowns.Add(1)
	return nil
}

func waitForTelemetryState(t *testing.T, runtime *Runtime, healthy bool) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer deadline.Stop()
	defer ticker.Stop()
	for {
		got := runtime.Ready() == nil
		if got == healthy {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("telemetry healthy=%v, want %v", got, healthy)
		case <-ticker.C:
		}
	}
}

func TestRuntimeExporterHealthFailsRecoversAndRegressesWhileIdle(t *testing.T) {
	exporter := &recoveringExporter{}
	exporter.failing.Store(true)
	runtime, err := NewRuntime(t.Context(), RuntimeConfig{Exporter: exporter, Timeout: 50 * time.Millisecond, HealthInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	waitForTelemetryState(t, runtime, false)
	exporter.failing.Store(false)
	waitForTelemetryState(t, runtime, true)
	exporter.failing.Store(true)
	waitForTelemetryState(t, runtime, false)
	if err := runtime.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if exporter.exports.Load() < 3 || exporter.shutdowns.Load() != 1 {
		t.Fatalf("exports=%d shutdowns=%d", exporter.exports.Load(), exporter.shutdowns.Load())
	}
}
func TestCorrelationIsBoundedAndNotBaggage(t *testing.T) {
	ctx := WithCorrelation(context.Background(), Correlation{SessionID: strings.Repeat("a", 256)})
	c, ok := CorrelationFromContext(ctx)
	if !ok || len(c.SessionID) != 128 {
		t.Fatalf("correlation=%d %v", len(c.SessionID), ok)
	}
}

type countingExporter struct{ shutdowns atomic.Int64 }

func (*countingExporter) ExportSpans(context.Context, []sdktrace.ReadOnlySpan) error { return nil }
func (e *countingExporter) Shutdown(context.Context) error                           { e.shutdowns.Add(1); return nil }
func TestRuntimeShutdownIsIdempotent(t *testing.T) {
	exporter := &countingExporter{}
	runtime, err := NewRuntime(t.Context(), RuntimeConfig{Exporter: exporter})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if exporter.shutdowns.Load() != 1 {
		t.Fatalf("shutdowns=%d", exporter.shutdowns.Load())
	}
}

type recordingRuntimeExporter struct {
	mu        sync.Mutex
	names     []string
	shutdowns int
}

func (e *recordingRuntimeExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, span := range spans {
		e.names = append(e.names, span.Name())
	}
	return nil
}
func (e *recordingRuntimeExporter) Shutdown(context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.shutdowns++
	return nil
}

func TestRuntimeCloseFlushesApplicationSpanAndShutsExporterOnce(t *testing.T) {
	exporter := &recordingRuntimeExporter{}
	runtime, err := NewRuntime(t.Context(), RuntimeConfig{Exporter: exporter, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	_, span := runtime.TracerProvider().Tracer("test").Start(t.Context(), "application.boundary")
	span.End()
	if err := runtime.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	exporter.mu.Lock()
	defer exporter.mu.Unlock()
	seen := 0
	for _, name := range exporter.names {
		if name == "application.boundary" {
			seen++
		}
	}
	if seen != 1 || exporter.shutdowns != 1 {
		t.Fatalf("application exports=%d shutdowns=%d", seen, exporter.shutdowns)
	}
}
