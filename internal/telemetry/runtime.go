package telemetry

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const defaultExportTimeout = 5 * time.Second

type RuntimeConfig struct {
	Endpoint                                                       string
	Protocol                                                       string
	Timeout                                                        time.Duration
	Production                                                     bool
	ServiceName, Component, Provider, Region, Instance, ConfigHash string
	Exporter                                                       sdktrace.SpanExporter
	HealthInterval                                                 time.Duration
}

type Runtime struct {
	provider     *sdktrace.TracerProvider
	healthy      atomic.Bool
	timeout      time.Duration
	closeOnce    sync.Once
	closeErr     error
	healthCancel context.CancelFunc
	healthDone   chan struct{}
}

func NewRuntime(ctx context.Context, cfg RuntimeConfig) (*Runtime, error) {
	if ctx == nil {
		return nil, errors.New("telemetry context is required")
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = defaultExportTimeout
	}
	if cfg.Timeout < 10*time.Millisecond || cfg.Timeout > time.Minute {
		return nil, errors.New("telemetry timeout is invalid")
	}
	if cfg.Protocol == "" {
		cfg.Protocol = "http/protobuf"
	}
	if cfg.Protocol != "http/protobuf" {
		return nil, errors.New("telemetry protocol is unsupported")
	}
	if cfg.HealthInterval == 0 {
		cfg.HealthInterval = 15 * time.Second
	}
	if cfg.HealthInterval < 10*time.Millisecond || cfg.HealthInterval > time.Minute {
		return nil, errors.New("telemetry health interval is invalid")
	}
	if cfg.Production && cfg.Endpoint == "" && cfg.Exporter == nil {
		return nil, errors.New("production telemetry exporter is required")
	}
	for _, value := range []string{cfg.ServiceName, cfg.Component, cfg.Provider, cfg.Region, cfg.Instance, cfg.ConfigHash} {
		if len(value) > 256 || strings.ContainsAny(value, "\r\n\x00") {
			return nil, errors.New("telemetry resource attribute is invalid")
		}
	}
	exporter := cfg.Exporter
	if exporter == nil && cfg.Endpoint != "" {
		u, err := url.Parse(cfg.Endpoint)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.Fragment != "" {
			return nil, errors.New("telemetry endpoint is invalid")
		}
		if cfg.Production && u.Scheme != "https" {
			return nil, errors.New("production telemetry endpoint must use https")
		}
		exporter, err = otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(u.String()), otlptracehttp.WithTimeout(cfg.Timeout))
		if err != nil {
			return nil, errors.New("configure telemetry exporter")
		}
	}
	res, err := resource.New(ctx, resource.WithAttributes(
		attribute.String("service.name", bounded(cfg.ServiceName)), attribute.String("component", bounded(cfg.Component)),
		attribute.String("provider", bounded(cfg.Provider)), attribute.String("region", bounded(cfg.Region)),
		attribute.String("instance", bounded(cfg.Instance)), attribute.String("config_hash", bounded(cfg.ConfigHash)),
	))
	if err != nil {
		if exporter != nil {
			_ = exporter.Shutdown(ctx)
		}
		return nil, errors.New("configure telemetry resource")
	}
	r := &Runtime{timeout: cfg.Timeout}
	r.healthy.Store(exporter == nil)
	options := []sdktrace.TracerProviderOption{sdktrace.WithResource(res)}
	if exporter != nil {
		options = append(options, sdktrace.WithBatcher(&healthExporter{next: exporter, healthy: &r.healthy}, sdktrace.WithExportTimeout(cfg.Timeout)))
	}
	r.provider = sdktrace.NewTracerProvider(options...)
	if exporter != nil {
		probeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.Timeout)
		r.probe(probeCtx)
		cancel()
		healthCtx, healthCancel := context.WithCancel(context.Background())
		r.healthCancel = healthCancel
		r.healthDone = make(chan struct{})
		go r.healthLoop(healthCtx, cfg.HealthInterval)
	}
	return r, nil
}

func (r *Runtime) probe(ctx context.Context) {
	_, span := r.provider.Tracer("live.telemetry.health").Start(ctx, "telemetry.exporter.probe")
	span.End()
	if err := r.provider.ForceFlush(ctx); err != nil {
		r.healthy.Store(false)
	}
}
func (r *Runtime) healthLoop(ctx context.Context, interval time.Duration) {
	defer close(r.healthDone)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			probeCtx, cancel := context.WithTimeout(ctx, r.timeout)
			r.probe(probeCtx)
			cancel()
		}
	}
}

func (r *Runtime) TracerProvider() trace.TracerProvider {
	if r == nil || r.provider == nil {
		return trace.NewNoopTracerProvider()
	}
	return r.provider
}
func (r *Runtime) Ready() error {
	if r == nil || !r.healthy.Load() {
		return errors.New("telemetry exporter is unhealthy")
	}
	return nil
}
func (r *Runtime) Close(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("telemetry shutdown context is required")
	}
	r.closeOnce.Do(func() {
		if r.healthCancel != nil {
			r.healthCancel()
			select {
			case <-r.healthDone:
			case <-ctx.Done():
				r.closeErr = ctx.Err()
			}
		}
		boundedCtx, cancel := context.WithTimeout(ctx, r.timeout)
		defer cancel()
		if r.provider != nil {
			r.closeErr = errors.Join(r.closeErr, r.provider.Shutdown(boundedCtx))
		}
		r.healthy.Store(false)
	})
	return r.closeErr
}

type healthExporter struct {
	next    sdktrace.SpanExporter
	healthy *atomic.Bool
}

func (e *healthExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	err := e.next.ExportSpans(ctx, spans)
	e.healthy.Store(err == nil)
	return err
}
func (e *healthExporter) Shutdown(ctx context.Context) error { return e.next.Shutdown(ctx) }
func bounded(value string) string {
	if len(value) > 256 {
		return value[:256]
	}
	return value
}
