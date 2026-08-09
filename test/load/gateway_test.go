//go:build load

package load_test

import (
	"os"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/loadgen"
	"github.com/chuanmingliu/openai-gpt-live/test/internal/loadgateway"
)

func TestGateway250Sessions(t *testing.T) {
	duration := 10 * time.Minute
	if raw, ok := os.LookupEnv("LIVE_CAPACITY_DURATION"); ok {
		parsed, err := time.ParseDuration(raw)
		if err != nil || parsed < 2*time.Second || parsed > 10*time.Minute {
			t.Fatal("LIVE_CAPACITY_DURATION must be from 2s through 10m")
		}
		duration = parsed
	}
	server := loadgateway.Start()
	defer server.Close()
	evidenceClass := loadgen.EvidenceSmoke
	if duration == 10*time.Minute {
		evidenceClass = loadgen.EvidenceCapacity10
	}
	enforceSLO := false
	var pinnedProfile *loadgen.HostProfile
	switch raw := os.Getenv("LIVE_ENFORCE_PINNED_SLO"); raw {
	case "":
	case "1":
		if evidenceClass != loadgen.EvidenceCapacity10 {
			t.Fatal("LIVE_ENFORCE_PINNED_SLO requires the exact ten-minute capacity evidence class")
		}
		profile, err := loadgen.LoadPinnedProfile("pinned-profile.json")
		if err != nil {
			t.Fatal("checked-in pinned load profile is unavailable")
		}
		pinnedProfile, enforceSLO = &profile, true
	default:
		t.Fatal("LIVE_ENFORCE_PINNED_SLO must be unset or 1")
	}
	report, err := loadgen.Run(t.Context(), loadgen.Config{
		GatewayURL: server.URL(), Scenario: loadgen.ScenarioCapacity, EvidenceClass: evidenceClass,
		Sessions: 250, ConnectionConcurrency: 50,
		Duration: duration, FrameInterval: 250 * time.Millisecond,
		DialTimeout: 5 * time.Second, ResponseTimeout: 2 * time.Second, TeardownTimeout: 5 * time.Second,
		TelemetrySnapshot: server.Snapshot, PinnedProfile: pinnedProfile, EnforceSLO: enforceSLO,
	})
	if err != nil {
		t.Fatalf("capacity failed with bounded report: %+v: %v", report, err)
	}
	if report.SessionsHeld != 250 || server.MaximumProviderSessions() < 250 {
		t.Fatalf("sessions held=%d provider maximum=%d", report.SessionsHeld, server.MaximumProviderSessions())
	}
	if report.FencesRequested != 250 || report.FencesAcknowledged != 250 || report.FenceHealthChecks != 250 {
		t.Fatalf("fence evidence requested=%d acknowledged=%d healthy=%d", report.FencesRequested, report.FencesAcknowledged, report.FenceHealthChecks)
	}
	if report.Bargein.Count != 250 {
		t.Fatalf("measured acknowledged-fence samples=%d, want 250", report.Bargein.Count)
	}
	if server.StaleFramesInjected() != 250 || report.StaleEpoch != 0 {
		t.Fatalf("provider stale injection=%d gateway leaks=%d", server.StaleFramesInjected(), report.StaleEpoch)
	}
	if report.CrossSession != 0 || report.StaleEpoch != 0 || report.QueueOverflow != 0 || report.Missing != 0 {
		t.Fatalf("correctness counters are nonzero: %+v", report)
	}
	if report.GoroutineDelta > 10 || report.HeapDeltaBytes > 16<<20 {
		t.Fatalf("teardown bounds exceeded: goroutines=%d heap=%d", report.GoroutineDelta, report.HeapDeltaBytes)
	}
	if report.SLOEnforced != enforceSLO {
		t.Fatalf("SLO enforced=%v, want %v", report.SLOEnforced, enforceSLO)
	}
	t.Logf("held=%d frames=%d p95=%s p99=%s internal_p95=%s fence_p95=%s gateway_barge_diagnostic_p95=%s goroutine_delta=%d heap_delta=%d pinned=%v slo_enforced=%v evidence_class=%s config_hash=%s",
		report.SessionsHeld, report.FramesReceived, report.Audio.P95.Duration(), report.Audio.P99.Duration(), report.GatewayTransit.P95.Duration(), report.Bargein.P95.Duration(), report.GatewayBargein.P95.Duration(), report.GoroutineDelta, report.HeapDeltaBytes, report.PinnedProfileMatch, report.SLOEnforced, report.EvidenceClass, report.ConfigHash)
	if duration < 10*time.Minute {
		t.Logf("short capacity harness PASS (not ten-minute evidence): duration=%s", duration)
	}
}

func TestPinnedSLORejectsSlowAcknowledgedFenceEvenWhenGatewayMetricIsConstantTwentyMilliseconds(t *testing.T) {
	server := loadgateway.StartWithCancelDelay(150 * time.Millisecond)
	defer server.Close()
	profile := loadgen.DetectHostProfile()
	telemetry := func() loadgen.GatewayTelemetry {
		snapshot := server.Snapshot()
		snapshot.Bargein = loadgen.LatencySummary{
			Count: 20, P50: loadgen.Duration(20 * time.Millisecond), P95: loadgen.Duration(20 * time.Millisecond),
			P99: loadgen.Duration(20 * time.Millisecond), Max: loadgen.Duration(20 * time.Millisecond),
		}
		return snapshot
	}
	report, err := loadgen.Run(t.Context(), loadgen.Config{
		GatewayURL: server.URL(), Scenario: loadgen.ScenarioCapacity, EvidenceClass: loadgen.EvidenceSmoke,
		Sessions: 20, ConnectionConcurrency: 20, Duration: 200 * time.Millisecond, FrameInterval: 50 * time.Millisecond,
		DialTimeout: time.Second, ResponseTimeout: time.Second, TeardownTimeout: time.Second,
		TelemetrySnapshot: telemetry, PinnedProfile: &profile, EnforceSLO: true,
		Thresholds: loadgen.Thresholds{MinimumSessions: 20},
	})
	if err == nil || !hasLoadFailure(report.Failures, loadgen.FailureSLOBargeP95) {
		t.Fatalf("slow acknowledged fence passed: barge=%+v gateway_diagnostic=%+v failures=%+v err=%v", report.Bargein, report.GatewayBargein, report.Failures, err)
	}
	if report.GatewayBargein.P95.Duration() != 20*time.Millisecond {
		t.Fatalf("expected constant gateway diagnostic, got %+v", report.GatewayBargein)
	}
}

func hasLoadFailure(failures []loadgen.Failure, wanted loadgen.FailureCode) bool {
	for _, failure := range failures {
		if failure.Code == wanted {
			return true
		}
	}
	return false
}
