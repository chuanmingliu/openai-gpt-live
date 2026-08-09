package loadgen

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

func TestMetricDeltaRequiresObservedAdequateSamples(t *testing.T) {
	baseline, err := parseMetricSnapshot(strings.NewReader(`# TYPE live_queue_overflows_total counter
live_audio_frame_transit_seconds_bucket{le="0.01"} 0
live_audio_frame_transit_seconds_bucket{le="0.02"} 0
live_audio_frame_transit_seconds_bucket{le="+Inf"} 0
live_bargein_suppression_seconds_bucket{result="suppressed",le="0.05"} 0
live_bargein_suppression_seconds_bucket{result="suppressed",le="0.1"} 0
live_bargein_suppression_seconds_bucket{result="suppressed",le="+Inf"} 0
`), time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	final, err := parseMetricSnapshot(strings.NewReader(`# TYPE live_queue_overflows_total counter
live_audio_frame_transit_seconds_bucket{le="0.01"} 10
live_audio_frame_transit_seconds_bucket{le="0.02"} 20
live_audio_frame_transit_seconds_bucket{le="+Inf"} 20
live_bargein_suppression_seconds_bucket{result="suppressed",le="0.05"} 19
live_bargein_suppression_seconds_bucket{result="suppressed",le="0.1"} 20
live_bargein_suppression_seconds_bucket{result="suppressed",le="+Inf"} 20
live_queue_overflows_total{queue="egress",result="closed"} 0
`), time.Unix(2, 0))
	if err != nil {
		t.Fatal(err)
	}

	report := Report{}
	applyScrapedTelemetry(&report, baseline, final, nil)
	if report.Telemetry.Status != "adequate" || !report.Telemetry.QueueOverflowObserved {
		t.Fatalf("telemetry=%+v", report.Telemetry)
	}
	if report.GatewayTransit.Count != 20 || report.GatewayTransit.P95.Duration() != 20*time.Millisecond {
		t.Fatalf("transit=%+v", report.GatewayTransit)
	}
	if report.GatewayBargein.Count != 20 || report.GatewayBargein.P95.Duration() != 50*time.Millisecond {
		t.Fatalf("gateway barge diagnostic=%+v", report.GatewayBargein)
	}
}

func TestMetricDeltaReportsUnavailableAndStaleExplicitly(t *testing.T) {
	for _, test := range []struct {
		name     string
		baseline *metricSnapshot
		final    *metricSnapshot
		err      error
		want     string
	}{
		{name: "scrape failure", err: errors.New("unavailable"), want: "unavailable"},
		{name: "unchanged", baseline: emptyMetricSnapshot(1), final: emptyMetricSnapshot(2), want: "stale"},
		{name: "counter reset", baseline: metricSnapshotWithTransit(20), final: metricSnapshotWithTransit(10), want: "stale"},
	} {
		t.Run(test.name, func(t *testing.T) {
			report := Report{}
			applyScrapedTelemetry(&report, test.baseline, test.final, test.err)
			if report.Telemetry.Status != test.want {
				t.Fatalf("status=%q, want %q", report.Telemetry.Status, test.want)
			}
		})
	}
}

func TestScrapedConstantBargeMetricCannotOverwriteMeasuredFenceLatency(t *testing.T) {
	baseline := emptyMetricSnapshot(1)
	final := emptyMetricSnapshot(2)
	baseline.transit.buckets[math.Inf(1)] = 0
	final.transit.buckets[0.01] = 20
	final.transit.buckets[math.Inf(1)] = 20
	final.bargein.buckets[0.02] = 20
	final.bargein.buckets[math.Inf(1)] = 20
	report := Report{Bargein: LatencySummary{Count: 20, P95: Duration(150 * time.Millisecond)}}

	applyScrapedTelemetry(&report, baseline, final, nil)

	if report.Bargein.Count != 20 || report.Bargein.P95.Duration() != 150*time.Millisecond {
		t.Fatalf("scraped constant replaced measured fence latency: %+v", report.Bargein)
	}
}

func emptyMetricSnapshot(second int64) *metricSnapshot {
	return &metricSnapshot{
		at: time.Unix(second, 0), transit: metricHistogram{buckets: map[float64]uint64{}},
		bargein: metricHistogram{buckets: map[float64]uint64{}}, queueObservable: true,
	}
}

func metricSnapshotWithTransit(count uint64) *metricSnapshot {
	snapshot := emptyMetricSnapshot(int64(count))
	snapshot.transit.buckets[0.01] = count
	snapshot.transit.buckets[math.Inf(1)] = count
	return snapshot
}
