package telemetry

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

var MillisecondBucketsSeconds = []float64{.001, .003, .005, .010, .015, .030, .050, .100}
var MillisecondBuckets = []float64{1, 3, 5, 10, 15, 30, 50, 100}
var DurationBucketsSeconds = []float64{.001, .003, .005, .010, .015, .030, .050, .100, .250, .500, 1, 3, 10, 30, 120}

type MetricLabels struct {
	Component, Provider, Region, Instance, ConfigHash string
}

type MetricsConfig struct {
	Labels                                                                                     MetricLabels
	AllowedComponents, AllowedProviders, AllowedRegions, AllowedInstances, AllowedConfigHashes []string
	AllowedQueues, AllowedResults                                                              []string
}

func RegisterRuntimeCollectors(reg prometheus.Registerer, labels MetricLabels) error {
	if reg == nil {
		return errors.New("prometheus registry is required")
	}
	values := map[string]string{"component": labels.Component, "provider": labels.Provider, "region": labels.Region, "instance": labels.Instance, "config_hash": labels.ConfigHash}
	for name, value := range values {
		if value == "" || len(value) > 128 || strings.ContainsAny(value, "\r\n\x00") {
			return fmt.Errorf("runtime metric label %s is invalid", name)
		}
	}
	wrapped := prometheus.WrapRegistererWith(values, reg)
	if err := wrapped.Register(collectors.NewGoCollector()); err != nil {
		return fmt.Errorf("register Go runtime collector: %w", err)
	}
	if err := wrapped.Register(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})); err != nil {
		return fmt.Errorf("register process collector: %w", err)
	}
	return nil
}

type allowlist map[string]struct{}

func ResolveDeploymentRegion(region, commaSeparatedAllowlist string) (string, error) {
	if region == "" || len(region) > 63 || strings.ContainsAny(region, "\r\n\x00,") {
		return "", errors.New("deployment region is invalid")
	}
	for _, allowed := range strings.Split(commaSeparatedAllowlist, ",") {
		if allowed == region {
			return region, nil
		}
	}
	return "", fmt.Errorf("deployment region %q is not allowlisted", region)
}

func newAllowlist(values []string) (allowlist, error) {
	if len(values) > 128 {
		return nil, errors.New("metric label allowlist is too large")
	}
	a := make(allowlist, len(values)+1)
	a["other"] = struct{}{}
	for _, value := range values {
		if value == "" || len(value) > 128 || strings.ContainsAny(value, "\r\n\x00") {
			return nil, errors.New("metric label allowlist is invalid")
		}
		a[value] = struct{}{}
	}
	return a, nil
}
func (a allowlist) value(value string) string {
	if _, ok := a[value]; ok {
		return value
	}
	return "other"
}

type Metrics struct {
	SessionsActive                                                                  prometheus.Gauge
	SessionAdmissions                                                               *prometheus.CounterVec
	AudioFrameTransit                                                               prometheus.Histogram
	QueueBufferedMilliseconds, QueueDepth, QueueBytes                               prometheus.GaugeVec
	QueueOldestItemAge, QueueBlocked                                                *prometheus.HistogramVec
	QueueOverflows                                                                  *prometheus.CounterVec
	BargeinSuppression, HandoffACK, WorkerQueue, ReasonerFirstToken, WorkerDuration *prometheus.HistogramVec
	WorkerRetries                                                                   *prometheus.CounterVec
	PresentationDelay, OutboxLag                                                    *prometheus.HistogramVec
	ExpiredLeases, DuplicateDeliveries                                              *prometheus.CounterVec
	ReconnectRestore                                                                *prometheus.HistogramVec
	StaleOutputRejected                                                             *prometheus.CounterVec
	queues, results                                                                 allowlist
	active                                                                          atomic.Int64
}

func NewMetrics(reg prometheus.Registerer, cfg MetricsConfig) (*Metrics, error) {
	if reg == nil {
		return nil, errors.New("prometheus registry is required")
	}
	components, err := newAllowlist(cfg.AllowedComponents)
	if err != nil {
		return nil, err
	}
	providers, err := newAllowlist(cfg.AllowedProviders)
	if err != nil {
		return nil, err
	}
	regions, err := newAllowlist(cfg.AllowedRegions)
	if err != nil {
		return nil, err
	}
	instances, err := newAllowlist(cfg.AllowedInstances)
	if err != nil {
		return nil, err
	}
	hashes, err := newAllowlist(cfg.AllowedConfigHashes)
	if err != nil {
		return nil, err
	}
	queues, err := newAllowlist(cfg.AllowedQueues)
	if err != nil {
		return nil, err
	}
	results, err := newAllowlist(cfg.AllowedResults)
	if err != nil {
		return nil, err
	}
	labels := prometheus.Labels{
		"component": components.value(cfg.Labels.Component), "provider": providers.value(cfg.Labels.Provider),
		"region": regions.value(cfg.Labels.Region), "instance": instances.value(cfg.Labels.Instance), "config_hash": hashes.value(cfg.Labels.ConfigHash),
	}
	m := &Metrics{queues: queues, results: results}
	newGauge := func(name, help string) (prometheus.Gauge, error) {
		return registerGauge(reg, prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help, ConstLabels: labels}))
	}
	newCounterVec := func(name, help string, variable ...string) (*prometheus.CounterVec, error) {
		return registerCounterVec(reg, prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help, ConstLabels: labels}, variable))
	}
	newGaugeVec := func(name, help string, variable ...string) (*prometheus.GaugeVec, error) {
		return registerGaugeVec(reg, prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help, ConstLabels: labels}, variable))
	}
	newHistogram := func(name, help string, buckets []float64) (prometheus.Histogram, error) {
		return registerHistogram(reg, prometheus.NewHistogram(prometheus.HistogramOpts{Name: name, Help: help, ConstLabels: labels, Buckets: buckets}))
	}
	newHistogramVec := func(name, help string, buckets []float64, variable ...string) (*prometheus.HistogramVec, error) {
		return registerHistogramVec(reg, prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: name, Help: help, ConstLabels: labels, Buckets: buckets}, variable))
	}
	if m.SessionsActive, err = newGauge("live_sessions_active", "Currently active live sessions."); err != nil {
		return nil, err
	}
	if m.SessionAdmissions, err = newCounterVec("live_session_admissions_total", "Live session admission decisions.", "result"); err != nil {
		return nil, err
	}
	if m.AudioFrameTransit, err = newHistogram("live_audio_frame_transit_seconds", "Audio frame transit latency.", MillisecondBucketsSeconds); err != nil {
		return nil, err
	}
	if p, e := newGaugeVec("live_queue_buffered_milliseconds", "Buffered media duration in milliseconds.", "queue"); e != nil {
		return nil, e
	} else {
		m.QueueBufferedMilliseconds = *p
	}
	if p, e := newGaugeVec("live_queue_depth", "Current queue item depth.", "queue"); e != nil {
		return nil, e
	} else {
		m.QueueDepth = *p
	}
	if p, e := newGaugeVec("live_queue_bytes", "Current queue bytes.", "queue"); e != nil {
		return nil, e
	} else {
		m.QueueBytes = *p
	}
	if m.QueueOldestItemAge, err = newHistogramVec("live_queue_oldest_item_age_seconds", "Age of the oldest queue item.", DurationBucketsSeconds, "queue"); err != nil {
		return nil, err
	}
	if m.QueueBlocked, err = newHistogramVec("live_queue_blocked_seconds", "Time blocked on a bounded queue.", DurationBucketsSeconds, "queue"); err != nil {
		return nil, err
	}
	if m.QueueOverflows, err = newCounterVec("live_queue_overflows_total", "Bounded queue overflow decisions.", "queue", "result"); err != nil {
		return nil, err
	}
	if m.BargeinSuppression, err = newHistogramVec("live_bargein_suppression_seconds", "Suppressed output duration after barge-in.", DurationBucketsSeconds, "result"); err != nil {
		return nil, err
	}
	if m.HandoffACK, err = newHistogramVec("live_handoff_ack_seconds", "Durable handoff acknowledgement latency.", DurationBucketsSeconds, "result"); err != nil {
		return nil, err
	}
	if m.WorkerQueue, err = newHistogramVec("live_worker_queue_seconds", "Worker queue latency.", DurationBucketsSeconds, "result"); err != nil {
		return nil, err
	}
	if m.ReasonerFirstToken, err = newHistogramVec("live_reasoner_first_token_seconds", "Reasoner first-token latency.", DurationBucketsSeconds, "result"); err != nil {
		return nil, err
	}
	if m.WorkerDuration, err = newHistogramVec("live_worker_duration_seconds", "Worker job duration.", DurationBucketsSeconds, "result"); err != nil {
		return nil, err
	}
	if m.WorkerRetries, err = newCounterVec("live_worker_retries_total", "Worker retry decisions.", "result"); err != nil {
		return nil, err
	}
	if m.PresentationDelay, err = newHistogramVec("live_presentation_delay_seconds", "Result presentation delay.", DurationBucketsSeconds, "result"); err != nil {
		return nil, err
	}
	if m.OutboxLag, err = newHistogramVec("live_outbox_lag_seconds", "Durable outbox publication lag.", DurationBucketsSeconds, "queue"); err != nil {
		return nil, err
	}
	if m.ExpiredLeases, err = newCounterVec("live_expired_leases_total", "Expired leases observed.", "queue"); err != nil {
		return nil, err
	}
	if m.DuplicateDeliveries, err = newCounterVec("live_duplicate_deliveries_total", "Duplicate deliveries suppressed.", "result"); err != nil {
		return nil, err
	}
	if m.ReconnectRestore, err = newHistogramVec("live_reconnect_restore_seconds", "Reconnect restoration latency.", DurationBucketsSeconds, "result"); err != nil {
		return nil, err
	}
	if m.StaleOutputRejected, err = newCounterVec("live_stale_output_rejected_total", "Stale output rejection decisions.", "result"); err != nil {
		return nil, err
	}
	for q := range queues {
		m.QueueBufferedMilliseconds.WithLabelValues(q)
		m.QueueDepth.WithLabelValues(q)
		m.QueueBytes.WithLabelValues(q)
		m.QueueOldestItemAge.WithLabelValues(q)
		m.QueueBlocked.WithLabelValues(q)
		m.OutboxLag.WithLabelValues(q)
		m.ExpiredLeases.WithLabelValues(q)
		for result := range results {
			m.QueueOverflows.WithLabelValues(q, result)
		}
	}
	for result := range results {
		m.SessionAdmissions.WithLabelValues(result)
		m.BargeinSuppression.WithLabelValues(result)
		m.HandoffACK.WithLabelValues(result)
		m.WorkerQueue.WithLabelValues(result)
		m.ReasonerFirstToken.WithLabelValues(result)
		m.WorkerDuration.WithLabelValues(result)
		m.WorkerRetries.WithLabelValues(result)
		m.PresentationDelay.WithLabelValues(result)
		m.DuplicateDeliveries.WithLabelValues(result)
		m.ReconnectRestore.WithLabelValues(result)
		m.StaleOutputRejected.WithLabelValues(result)
	}
	return m, nil
}

func register(reg prometheus.Registerer, c prometheus.Collector) (prometheus.Collector, error) {
	if err := reg.Register(c); err != nil {
		var exists prometheus.AlreadyRegisteredError
		if errors.As(err, &exists) {
			return exists.ExistingCollector, nil
		}
		return nil, fmt.Errorf("register metric: %w", err)
	}
	return c, nil
}
func registerGauge(r prometheus.Registerer, c prometheus.Gauge) (prometheus.Gauge, error) {
	x, e := register(r, c)
	if e != nil {
		return nil, e
	}
	v, ok := x.(prometheus.Gauge)
	if !ok {
		return nil, errors.New("incompatible duplicate gauge")
	}
	return v, nil
}
func registerHistogram(r prometheus.Registerer, c prometheus.Histogram) (prometheus.Histogram, error) {
	x, e := register(r, c)
	if e != nil {
		return nil, e
	}
	v, ok := x.(prometheus.Histogram)
	if !ok {
		return nil, errors.New("incompatible duplicate histogram")
	}
	return v, nil
}
func registerCounterVec(r prometheus.Registerer, c *prometheus.CounterVec) (*prometheus.CounterVec, error) {
	x, e := register(r, c)
	if e != nil {
		return nil, e
	}
	v, ok := x.(*prometheus.CounterVec)
	if !ok {
		return nil, errors.New("incompatible duplicate counter")
	}
	return v, nil
}
func registerGaugeVec(r prometheus.Registerer, c *prometheus.GaugeVec) (*prometheus.GaugeVec, error) {
	x, e := register(r, c)
	if e != nil {
		return nil, e
	}
	v, ok := x.(*prometheus.GaugeVec)
	if !ok {
		return nil, errors.New("incompatible duplicate gauge vector")
	}
	return v, nil
}
func registerHistogramVec(r prometheus.Registerer, c *prometheus.HistogramVec) (*prometheus.HistogramVec, error) {
	x, e := register(r, c)
	if e != nil {
		return nil, e
	}
	v, ok := x.(*prometheus.HistogramVec)
	if !ok {
		return nil, errors.New("incompatible duplicate histogram vector")
	}
	return v, nil
}

type AudioTransitObserver struct{ observer prometheus.Observer }

func (m *Metrics) BindAudioTransit() AudioTransitObserver {
	if m == nil {
		return AudioTransitObserver{}
	}
	return AudioTransitObserver{observer: m.AudioFrameTransit}
}
func (o AudioTransitObserver) Observe(seconds float64) {
	if o.observer != nil && validObservation(seconds) {
		o.observer.Observe(seconds)
	}
}
func (o AudioTransitObserver) ObserveDuration(d time.Duration) {
	if d >= 0 {
		o.Observe(float64(d) / float64(time.Second))
	}
}

type QueueObserver struct {
	buffered, depth, bytes prometheus.Gauge
	oldest, blocked        prometheus.Observer
}

func (m *Metrics) BindQueue(queue string) QueueObserver {
	if m == nil {
		return QueueObserver{}
	}
	q := m.queues.value(queue)
	return QueueObserver{buffered: m.QueueBufferedMilliseconds.WithLabelValues(q), depth: m.QueueDepth.WithLabelValues(q), bytes: m.QueueBytes.WithLabelValues(q), oldest: m.QueueOldestItemAge.WithLabelValues(q), blocked: m.QueueBlocked.WithLabelValues(q)}
}
func (q QueueObserver) Set(depth, bytes int, buffered time.Duration) {
	if depth >= 0 && q.depth != nil {
		q.depth.Set(float64(depth))
	}
	if bytes >= 0 && q.bytes != nil {
		q.bytes.Set(float64(bytes))
	}
	if buffered >= 0 && q.buffered != nil {
		q.buffered.Set(float64(buffered) / float64(time.Millisecond))
	}
}
func (q QueueObserver) ObserveOldest(d time.Duration) {
	if d >= 0 && q.oldest != nil {
		q.oldest.Observe(d.Seconds())
	}
}
func (q QueueObserver) ObserveBlocked(d time.Duration) {
	if d >= 0 && q.blocked != nil {
		q.blocked.Observe(d.Seconds())
	}
}

func validObservation(value float64) bool {
	return value >= 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}
func (m *Metrics) SessionStarted() {
	if m == nil {
		return
	}
	m.active.Add(1)
	m.SessionsActive.Inc()
}
func (m *Metrics) SessionEnded() {
	if m == nil {
		return
	}
	for {
		old := m.active.Load()
		if old == 0 {
			return
		}
		if m.active.CompareAndSwap(old, old-1) {
			m.SessionsActive.Dec()
			return
		}
	}
}
func (m *Metrics) ObserveAdmission(result string) {
	if m != nil {
		m.SessionAdmissions.WithLabelValues(m.results.value(result)).Inc()
	}
}
func (m *Metrics) SetQueue(queue string, depth int, bytes int, bufferedMilliseconds float64) {
	if m == nil {
		return
	}
	q := m.queues.value(queue)
	if depth >= 0 {
		m.QueueDepth.WithLabelValues(q).Set(float64(depth))
	}
	if bytes >= 0 {
		m.QueueBytes.WithLabelValues(q).Set(float64(bytes))
	}
	if validObservation(bufferedMilliseconds) {
		m.QueueBufferedMilliseconds.WithLabelValues(q).Set(bufferedMilliseconds)
	}
}
func (m *Metrics) ObserveQueueOverflow(queue, result string) {
	if m != nil {
		m.QueueOverflows.WithLabelValues(m.queues.value(queue), m.results.value(result)).Inc()
	}
}
func (m *Metrics) ObserveSeconds(vector *prometheus.HistogramVec, result string, seconds float64) {
	if m != nil && vector != nil && validObservation(seconds) {
		vector.WithLabelValues(m.results.value(result)).Observe(seconds)
	}
}
