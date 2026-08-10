package telemetry

import (
	"math"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestRegisterRuntimeCollectorsEmitsBoundedDeploymentLabels(t *testing.T) {
	registry := prometheus.NewRegistry()
	labels := MetricLabels{Component: "gateway", Provider: "speech", Region: "us-east-1", Instance: "gateway-1", ConfigHash: "v1:" + strings.Repeat("a", 64)}
	if err := RegisterRuntimeCollectors(registry, labels); err != nil {
		t.Fatal(err)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"go_goroutines": false, "go_memstats_heap_alloc_bytes": false, "process_cpu_seconds_total": false}
	for _, family := range families {
		if _, ok := want[family.GetName()]; !ok {
			continue
		}
		want[family.GetName()] = true
		for _, metric := range family.Metric {
			seen := map[string]string{}
			for _, label := range metric.Label {
				seen[label.GetName()] = label.GetValue()
			}
			for key, value := range map[string]string{"component": labels.Component, "provider": labels.Provider, "region": labels.Region, "instance": labels.Instance, "config_hash": labels.ConfigHash} {
				if seen[key] != value {
					t.Fatalf("%s label %s=%q, want %q", family.GetName(), key, seen[key], value)
				}
			}
		}
	}
	for name, found := range want {
		if !found {
			t.Errorf("runtime metric %s was not emitted", name)
		}
	}
}

func TestResolveDeploymentRegionRequiresAllowlist(t *testing.T) {
	if got, err := ResolveDeploymentRegion("us-east-1", "us-east-1,eu-west-1"); err != nil || got != "us-east-1" {
		t.Fatalf("ResolveDeploymentRegion() = %q, %v", got, err)
	}
	if _, err := ResolveDeploymentRegion("attacker-region", "us-east-1,eu-west-1"); err == nil {
		t.Fatal("non-allowlisted region was accepted")
	}
}

func testMetrics(t testing.TB) (*Metrics, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	m, err := NewMetrics(reg, testMetricsConfig())
	if err != nil {
		t.Fatal(err)
	}
	return m, reg
}
func testMetricsConfig() MetricsConfig {
	return MetricsConfig{Labels: MetricLabels{Component: "gateway", Provider: "voice", Region: "local", Instance: "one", ConfigHash: "v1:abc"}, AllowedComponents: []string{"gateway"}, AllowedProviders: []string{"voice"}, AllowedRegions: []string{"local"}, AllowedInstances: []string{"one"}, AllowedConfigHashes: []string{"v1:abc"}, AllowedQueues: []string{"ingress"}, AllowedResults: []string{"ok", "rejected"}}
}
func TestMetricsExactFamiliesAndDuplicateConstruction(t *testing.T) {
	m, reg := testMetrics(t)
	m.SessionStarted()
	m.ObserveAdmission("raw-unbounded")
	m.BindAudioTransit().Observe(.003)
	m.SetQueue("ingress", 1, 960, 20)
	m.ObserveQueueOverflow("raw", "raw")
	if _, err := NewMetrics(reg, testMetricsConfig()); err != nil {
		t.Fatal(err)
	}
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{}
	for _, name := range []string{"live_sessions_active", "live_session_admissions_total", "live_audio_frame_transit_seconds", "live_queue_buffered_milliseconds", "live_queue_depth", "live_queue_bytes", "live_queue_oldest_item_age_seconds", "live_queue_blocked_seconds", "live_queue_overflows_total", "live_bargein_suppression_seconds", "live_handoff_ack_seconds", "live_worker_queue_seconds", "live_reasoner_first_token_seconds", "live_worker_duration_seconds", "live_worker_retries_total", "live_presentation_delay_seconds", "live_outbox_lag_seconds", "live_expired_leases_total", "live_duplicate_deliveries_total", "live_reconnect_restore_seconds", "live_stale_output_rejected_total"} {
		want[name] = false
	}
	allowedLabels := map[string]bool{"component": true, "provider": true, "region": true, "instance": true, "config_hash": true, "queue": true, "result": true}
	for _, f := range families {
		if _, ok := want[f.GetName()]; ok {
			want[f.GetName()] = true
			for _, metric := range f.Metric {
				for _, label := range metric.Label {
					if !allowedLabels[label.GetName()] {
						t.Errorf("disallowed label %s on %s", label.GetName(), f.GetName())
					}
				}
			}
		}
	}
	for n, ok := range want {
		if !ok {
			t.Errorf("missing %s", n)
		}
	}
	for _, family := range families {
		if family.GetName() == "live_audio_frame_transit_seconds" {
			buckets := family.Metric[0].Histogram.Bucket
			if len(buckets) < len(MillisecondBucketsSeconds) {
				t.Fatalf("buckets=%d", len(buckets))
			}
			for i, want := range MillisecondBucketsSeconds {
				if buckets[i].GetUpperBound() != want {
					t.Fatalf("bucket %d=%v want %v", i, buckets[i].GetUpperBound(), want)
				}
			}
		}
	}
}
func TestMetricsUnderflowAndInvalidObservation(t *testing.T) {
	m, _ := testMetrics(t)
	m.SessionEnded()
	m.BindAudioTransit().Observe(-1)
	m.BindAudioTransit().Observe(math.NaN())
}
func BenchmarkObserveAudioTransit(b *testing.B) {
	m, _ := testMetrics(b)
	o := m.BindAudioTransit()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		o.Observe(.003)
	}
}
