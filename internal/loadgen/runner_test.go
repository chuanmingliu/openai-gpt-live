package loadgen

import (
	"bytes"
	"context"
	"encoding/json"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestFrameBatchVisitsRunnableSessionsInDeterministicOrder(t *testing.T) {
	clients := []*loadClient{{id: 3}, nil, {id: 1}, {id: 2}}
	clients[2].failed.Store(true)
	var visited []uint64
	forEachRunnableClient(t.Context(), clients, func(client *loadClient) { visited = append(visited, client.id) })
	want := []uint64{3, 2}
	if len(visited) != len(want) || visited[0] != want[0] || visited[1] != want[1] {
		t.Fatalf("scheduled clients=%v, want stable input order %v", visited, want)
	}
}

func TestWatermarkRoundTripChecksumAndDeterminism(t *testing.T) {
	mark := Watermark{Session: 19, Epoch: 3, Sequence: 7, SentOffsetNanos: 42}
	a := EncodeWatermark(mark)
	b := EncodeWatermark(mark)
	if len(a) != PCMFrameBytes || !bytes.Equal(a, b) {
		t.Fatalf("watermark is not one deterministic PCM frame: %d", len(a))
	}
	got, err := DecodeWatermark(a)
	if err != nil || got != mark {
		t.Fatalf("decode = %#v, %v", got, err)
	}
	a[41] ^= 0xff
	if _, err := DecodeWatermark(a); err == nil {
		t.Fatal("corrupt checksum accepted")
	}
}

func TestHistogramPercentilesAreBoundedAndStable(t *testing.T) {
	h := NewHistogram(10*time.Second, 3)
	for i := 1; i <= 100; i++ {
		h.Record(time.Duration(i) * time.Millisecond)
	}
	if got := h.Percentile(50); got < 49*time.Millisecond || got > 51*time.Millisecond {
		t.Fatalf("p50 = %v", got)
	}
	if got := h.Percentile(95); got < 94*time.Millisecond || got > 97*time.Millisecond {
		t.Fatalf("p95 = %v", got)
	}
	if h.BucketCount() > 4096 {
		t.Fatalf("histogram has %d buckets", h.BucketCount())
	}
}

func TestHistogramUsesNearestRankForSmallSamples(t *testing.T) {
	h := NewHistogram(time.Second, 3)
	h.Record(time.Millisecond)
	h.Record(100 * time.Millisecond)
	if got := h.Percentile(95); got < 100*time.Millisecond {
		t.Fatalf("two-sample p95 selected the lower sample: %v", got)
	}
	h.Record(200 * time.Millisecond)
	if got := h.Percentile(50); got < 100*time.Millisecond || got >= 200*time.Millisecond {
		t.Fatalf("three-sample p50=%v, want the second sample bucket", got)
	}
}

func TestSentinelClassifiesIsolationOrderingAndEpochFailures(t *testing.T) {
	s := NewSentinel()
	s.Sent(Watermark{Session: 1, Epoch: 1, Sequence: 1, SentOffsetNanos: 1})
	s.Sent(Watermark{Session: 1, Epoch: 1, Sequence: 2, SentOffsetNanos: 2})
	s.Fence(1, 2)

	if got := s.Receive(1, Watermark{Session: 2, Epoch: 2, Sequence: 1}); got != ClassificationCrossSession {
		t.Fatalf("cross-session classification = %q", got)
	}
	if got := s.Receive(1, Watermark{Session: 1, Epoch: 1, Sequence: 1}); got != ClassificationStaleEpoch {
		t.Fatalf("stale classification = %q", got)
	}
	s.Sent(Watermark{Session: 1, Epoch: 2, Sequence: 4, SentOffsetNanos: 4})
	s.Sent(Watermark{Session: 1, Epoch: 2, Sequence: 3, SentOffsetNanos: 3})
	if got := s.Receive(1, Watermark{Session: 1, Epoch: 2, Sequence: 4}); got != ClassificationOK {
		t.Fatalf("first classification = %q", got)
	}
	if got := s.Receive(1, Watermark{Session: 1, Epoch: 2, Sequence: 3}); got != ClassificationReordered {
		t.Fatalf("reordered classification = %q", got)
	}
	if got := s.Receive(1, Watermark{Session: 1, Epoch: 2, Sequence: 4}); got != ClassificationDuplicate {
		t.Fatalf("duplicate classification = %q", got)
	}
	if got := s.Missing(); got != 2 {
		t.Fatalf("missing = %d, want 2", got)
	}
}

func TestSentinelHistoryRemainsBoundedDuringSoak(t *testing.T) {
	s := NewSentinel()
	for sequence := uint64(1); sequence <= 20_000; sequence++ {
		mark := Watermark{Session: 1, Epoch: 1, Sequence: sequence}
		s.Sent(mark)
		if got := s.Receive(1, mark); got != ClassificationOK {
			t.Fatalf("sequence %d classification=%q", sequence, got)
		}
	}
	if len(s.expected) != 0 || len(s.streams) != 1 {
		t.Fatalf("sentinel state grew: expected=%d streams=%d", len(s.expected), len(s.streams))
	}
	if got := s.Receive(1, Watermark{Session: 1, Epoch: 1, Sequence: 1}); got != ClassificationDuplicate {
		t.Fatalf("duplicate older than 4096 frames classified as %q", got)
	}
}

func TestSentinelSequenceStateStaysConstantWhenEarliestFrameNeverArrives(t *testing.T) {
	s := NewSentinel()
	for sequence := uint64(2); sequence <= 20_000; sequence++ {
		mark := Watermark{Session: 1, Epoch: 1, Sequence: sequence}
		s.Sent(mark)
		if got := s.Receive(1, mark); got != ClassificationOK {
			t.Fatalf("sequence %d classification=%q", sequence, got)
		}
	}
	state := s.streams[sentinelStreamKey{session: 1, epoch: 1}]
	if state == nil || state.maximum != 20_000 {
		t.Fatalf("constant sequence high-water state=%+v", state)
	}
}

func TestSentinelBoundsOutstandingAndExpiresToMissing(t *testing.T) {
	s := NewBoundedSentinel(3, 2)
	base := time.Now()
	if !s.SentAt(Watermark{Session: 1, Epoch: 1, Sequence: 1}, base, base.Add(time.Second)) ||
		!s.SentAt(Watermark{Session: 1, Epoch: 1, Sequence: 2}, base, base.Add(time.Second)) {
		t.Fatal("sentinel rejected in-budget work")
	}
	if s.SentAt(Watermark{Session: 1, Epoch: 1, Sequence: 3}, base, base.Add(time.Second)) {
		t.Fatal("per-session outstanding budget was exceeded")
	}
	if !s.SentAt(Watermark{Session: 2, Epoch: 1, Sequence: 1}, base, base.Add(time.Second)) ||
		s.SentAt(Watermark{Session: 2, Epoch: 1, Sequence: 2}, base, base.Add(time.Second)) {
		t.Fatal("global outstanding budget was not enforced")
	}
	if got := s.Expire(base.Add(time.Second)); got != 3 || s.Pending() != 0 || s.Missing() != 3 {
		t.Fatalf("expired=%d pending=%d missing=%d", got, s.Pending(), s.Missing())
	}
}

func TestSentinelFenceReclaimsOldEpochSequenceState(t *testing.T) {
	s := NewBoundedSentinel(4, 4)
	for epoch := uint64(1); epoch <= 100; epoch++ {
		mark := Watermark{Session: 1, Epoch: epoch, Sequence: 1}
		s.Sent(mark)
		if got := s.Receive(1, mark); got != ClassificationOK {
			t.Fatalf("epoch %d classification=%q", epoch, got)
		}
		s.Fence(1, epoch+1)
	}
	if len(s.streams) > 1 {
		t.Fatalf("old epoch sequence state retained: streams=%d", len(s.streams))
	}
}

func TestEvaluateSeparatesPortableAndPinnedSLOGates(t *testing.T) {
	report := Report{
		SessionsRequested: 250, SessionsHeld: 250,
		Audio:          LatencySummary{Count: 20, P95: Duration(20 * time.Millisecond), P99: Duration(40 * time.Millisecond)},
		GatewayTransit: LatencySummary{Count: 20, P95: Duration(time.Millisecond), P99: Duration(2 * time.Millisecond)},
		Bargein:        LatencySummary{Count: 20, P95: Duration(50 * time.Millisecond)},
		Telemetry:      TelemetryEvidence{Status: "adequate", TransitSamples: 20, BargeinSamples: 20, QueueOverflowObserved: true},
	}
	if failures := Evaluate(report, Thresholds{}, false); len(failures) != 0 {
		t.Fatalf("portable evaluation enforced host latency: %#v", failures)
	}
	failures := Evaluate(report, Thresholds{}, true)
	if !containsFailure(failures, FailureSLOAudioP95) || !containsFailure(failures, FailureSLOAudioP99) {
		t.Fatalf("pinned SLO failures = %#v", failures)
	}
	report.CrossSession = 1
	if !containsFailure(Evaluate(report, Thresholds{}, false), FailureCrossSession) {
		t.Fatal("portable isolation failure was not enforced")
	}
}

func TestSLOEnforcementRequiresObservableGatewayMetrics(t *testing.T) {
	profile := DetectHostProfile()
	cfg := Config{
		GatewayURL: "ws://127.0.0.1:1/v1/realtime", Scenario: ScenarioCapacity, EvidenceClass: EvidenceSmoke,
		Sessions: 1, Duration: time.Second, FrameInterval: 100 * time.Millisecond,
		EnforceSLO: true, PinnedProfile: &profile,
	}
	if _, err := normalizeConfig(cfg); err == nil {
		t.Fatal("SLO enforcement accepted an unobservable gateway metrics source")
	}
	cfg.MetricsURL = "http://127.0.0.1:9090/metrics"
	if _, err := normalizeConfig(cfg); err != nil {
		t.Fatalf("explicit private metrics source rejected: %v", err)
	}
}

func TestReportJSONIsBoundedAndPrivacySafe(t *testing.T) {
	report := Report{
		SchemaVersion: ReportSchemaVersion,
		ConfigHash:    "v1:test",
		Scenario:      ScenarioCapacity,
		Failures:      []Failure{{Code: FailureMissing, Count: 2}},
		Host:          HostProfile{OS: runtime.GOOS, GoVersion: runtime.Version()},
	}
	raw, err := MarshalReport(report)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) > MaxReportBytes {
		t.Fatalf("report too large: %d", len(raw))
	}
	for _, forbidden := range []string{"gateway_url", "bearer", "audio", "transcript", "prompt", "credential", "api_key", "reasoning", "result_text"} {
		if strings.Contains(strings.ToLower(string(raw)), forbidden) {
			t.Fatalf("report contains forbidden field %q: %s", forbidden, raw)
		}
	}
	var decoded Report
	if err := json.Unmarshal(raw, &decoded); err != nil || decoded.SchemaVersion != report.SchemaVersion {
		t.Fatalf("round trip = %#v, %v", decoded, err)
	}
}

func TestHostProfileMatchingIsExact(t *testing.T) {
	a := HostProfile{CPUModel: "cpu", CPUCores: 8, MemoryBytes: 16 << 30, OS: "linux", Arch: "amd64", GoVersion: "go1.26.0"}
	b := a
	if !a.ExactMatch(b) {
		t.Fatal("identical profiles did not match")
	}
	b.CPUCores++
	if a.ExactMatch(b) {
		t.Fatal("different profiles matched")
	}
}

func TestHeapSlopeUsesMedianPairwiseRegression(t *testing.T) {
	start := time.Unix(0, 0)
	samples := []HeapSample{
		{At: start, Bytes: 10 << 20},
		{At: start.Add(time.Hour), Bytes: 12 << 20},
		{At: start.Add(2 * time.Hour), Bytes: 14 << 20},
		{At: start.Add(3 * time.Hour), Bytes: 100 << 20}, // outlier
		{At: start.Add(4 * time.Hour), Bytes: 18 << 20},
	}
	slope, err := HeapSlopePerHour(samples)
	if err != nil {
		t.Fatal(err)
	}
	if slope < 1.9*(1<<20) || slope > 2.1*(1<<20) {
		t.Fatalf("robust slope = %f", slope)
	}
	if _, err := HeapSlopePerHour(samples[:4]); err == nil {
		t.Fatal("too few samples accepted")
	}
}

func TestRunCancellationBeforeDialReturnsBoundedReportAndJoins(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	report, err := Run(ctx, Config{GatewayURL: "ws://127.0.0.1:1/v1/realtime", Sessions: 250, ConnectionConcurrency: 25, Duration: time.Second, FrameInterval: 100 * time.Millisecond})
	if err == nil || report.SchemaVersion != ReportSchemaVersion || report.SessionsEstablished != 0 {
		t.Fatalf("report=%+v err=%v", report, err)
	}
	if report.GoroutineDelta > 10 {
		t.Fatalf("canceled run goroutine delta=%d", report.GoroutineDelta)
	}
	if _, err := MarshalReport(report); err != nil {
		t.Fatal(err)
	}
}

func TestGatewayURLRejectsCredentialAndPublicTargets(t *testing.T) {
	for _, raw := range []string{"", "https://127.0.0.1/v1/realtime", "ws://user:pass@127.0.0.1/v1/realtime", "wss://public.example.com/v1/realtime", "ws://127.0.0.1/v1/realtime?token=x"} {
		if err := validateGatewayURL(raw); err == nil {
			t.Fatalf("unsafe URL accepted: %q", raw)
		}
	}
	for _, raw := range []string{"ws://127.0.0.1:8080/v1/realtime", "wss://localhost/v1/realtime"} {
		if err := validateGatewayURL(raw); err != nil {
			t.Fatalf("safe test URL rejected: %q: %v", raw, err)
		}
	}
	if err := validateEndpointURL("ws://gateway:8080/v1/realtime", map[string]struct{}{"ws": {}}, nil); err == nil {
		t.Fatal("arbitrary single-label Docker host accepted without allow-host")
	}
	if err := validateEndpointURL("ws://gateway:8080/v1/realtime", map[string]struct{}{"ws": {}}, []string{"gateway"}); err != nil {
		t.Fatalf("explicitly allowed Docker host rejected: %v", err)
	}
}

func TestConfigHashCoversOperationalConfigButExcludesBearers(t *testing.T) {
	a := Config{GatewayURL: "ws://127.0.0.1:1/v1/realtime", BearerToken: "first", Scenario: ScenarioCapacity, Sessions: 250, ConnectionConcurrency: 25, Duration: time.Minute, FrameInterval: time.Second}
	b := a
	b.BearerToken, b.MetricsBearerToken = "second", "also-secret"
	if configHash(a) != configHash(b) {
		t.Fatal("bearer affected report config hash")
	}
	for name, mutate := range map[string]func(*Config){
		"gateway":   func(cfg *Config) { cfg.GatewayURL = "ws://127.0.0.1:2/v1/realtime" },
		"deadline":  func(cfg *Config) { cfg.ResponseTimeout = 3 * time.Second },
		"allowlist": func(cfg *Config) { cfg.AllowHosts = []string{"gateway"} },
		"metrics":   func(cfg *Config) { cfg.MetricsURL = "http://127.0.0.1:9090/metrics" },
	} {
		changed := a
		mutate(&changed)
		if configHash(a) == configHash(changed) {
			t.Fatalf("%s did not affect config hash", name)
		}
	}
}

func TestEvidenceClassesRequireExactReleaseDurations(t *testing.T) {
	base := Config{GatewayURL: "ws://127.0.0.1:1/v1/realtime", Scenario: ScenarioSoak, EvidenceClass: EvidenceSoak2H, Sessions: 1, Duration: 2 * time.Hour, FrameInterval: time.Second, SoakSampleInterval: 10 * time.Minute}
	if _, err := normalizeConfig(base); err != nil {
		t.Fatalf("exact soak evidence rejected: %v", err)
	}
	base.Duration = time.Minute
	if _, err := normalizeConfig(base); err == nil {
		t.Fatal("short run was accepted as two-hour evidence")
	}
	base.EvidenceClass = EvidenceSmoke
	base.SoakSampleInterval = 10 * time.Second
	if _, err := normalizeConfig(base); err != nil {
		t.Fatalf("explicit smoke run rejected: %v", err)
	}
}

func containsFailure(failures []Failure, code FailureCode) bool {
	for _, failure := range failures {
		if failure.Code == code {
			return true
		}
	}
	return false
}
