//go:build soak

package load_test

import (
	"math"
	"os"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/loadgen"
	"github.com/chuanmingliu/openai-gpt-live/test/internal/loadgateway"
)

func TestGatewaySoak(t *testing.T) {
	raw, ok := os.LookupEnv("LIVE_SOAK_DURATION")
	if !ok {
		t.Skip("LIVE_SOAK_DURATION is required before any connection")
	}
	duration, err := time.ParseDuration(raw)
	if err != nil || duration < 10*time.Second || duration > 2*time.Hour {
		t.Fatal("LIVE_SOAK_DURATION must be from 10s through 2h")
	}
	productionGate := duration == 2*time.Hour
	thresholds := loadgen.Thresholds{}
	if !productionGate {
		// A short run validates sampling/regression mechanics but cannot turn a
		// seconds-long startup slope into two-hour release evidence.
		thresholds.MaximumSlopePerHour = math.MaxFloat64
	}
	interval := duration / 8
	if interval < time.Second {
		interval = time.Second
	}
	server := loadgateway.Start()
	defer server.Close()
	evidenceClass := loadgen.EvidenceSmoke
	if productionGate {
		evidenceClass = loadgen.EvidenceSoak2H
	}
	report, err := loadgen.Run(t.Context(), loadgen.Config{
		GatewayURL: server.URL(), Scenario: loadgen.ScenarioSoak, EvidenceClass: evidenceClass,
		Sessions: 250, ConnectionConcurrency: 50, Duration: duration,
		FrameInterval: time.Second, SoakSampleInterval: interval,
		DialTimeout: 5 * time.Second, ResponseTimeout: 2 * time.Second, TeardownTimeout: 5 * time.Second,
		TelemetrySnapshot: server.Snapshot, Thresholds: thresholds,
	})
	if err != nil {
		t.Fatalf("soak failed with bounded report: %+v: %v", report, err)
	}
	if report.HeapSamples < 5 || (productionGate && report.HeapSlopePerHour > 4<<20) {
		t.Fatalf("heap regression gate failed: samples=%d slope=%f", report.HeapSamples, report.HeapSlopePerHour)
	}
	if productionGate {
		t.Logf("two-hour soak gate PASS: samples=%d slope=%f", report.HeapSamples, report.HeapSlopePerHour)
	} else {
		t.Logf("short soak harness PASS (not two-hour evidence): duration=%s samples=%d slope=%f", duration, report.HeapSamples, report.HeapSlopePerHour)
	}
}
