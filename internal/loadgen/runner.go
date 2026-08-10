// Package loadgen implements a deterministic, content-free realtime load
// client. It retains only counters and bounded histograms, never PCM frames or
// provider/client text.
package loadgen

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/mem"
)

const (
	PCMFrameBytes       = 960
	ReportSchemaVersion = "live-load-report/v1"
	MaxReportBytes      = 64 << 10
	watermarkHeader     = 48
	watermarkHash       = 32
	watermarkBody       = PCMFrameBytes - watermarkHash
	maximumSessions     = 10_000
	maximumDuration     = 24 * time.Hour
	maximumOutstanding  = 1_000_000
)

var watermarkMagic = [8]byte{'L', 'I', 'V', 'E', 'L', 'O', 'A', 'D'}

type Scenario string

const (
	ScenarioCapacity Scenario = "capacity"
	ScenarioSoak     Scenario = "soak"
)

type EvidenceClass string

const (
	EvidenceSmoke      EvidenceClass = "smoke"
	EvidenceCapacity10 EvidenceClass = "capacity_10m"
	EvidenceSoak2H     EvidenceClass = "soak_2h"
)

type Watermark struct {
	Session         uint64
	Epoch           uint64
	Sequence        uint64
	SentOffsetNanos int64
}

func EncodeWatermark(mark Watermark) []byte {
	frame := make([]byte, PCMFrameBytes)
	copy(frame[:8], watermarkMagic[:])
	binary.BigEndian.PutUint16(frame[8:10], 1)
	binary.BigEndian.PutUint64(frame[16:24], mark.Session)
	binary.BigEndian.PutUint64(frame[24:32], mark.Epoch)
	binary.BigEndian.PutUint64(frame[32:40], mark.Sequence)
	binary.BigEndian.PutUint64(frame[40:48], uint64(mark.SentOffsetNanos))
	seed := mark.Session ^ (mark.Epoch << 17) ^ (mark.Sequence << 33) ^ uint64(mark.SentOffsetNanos)
	for offset := watermarkHeader; offset < watermarkBody; offset += 8 {
		seed ^= seed << 13
		seed ^= seed >> 7
		seed ^= seed << 17
		binary.BigEndian.PutUint64(frame[offset:offset+8], seed)
	}
	digest := sha256.Sum256(frame[:watermarkBody])
	copy(frame[watermarkBody:], digest[:])
	return frame
}

func DecodeWatermark(frame []byte) (Watermark, error) {
	if len(frame) != PCMFrameBytes || !stringEqualBytes(frame[:8], watermarkMagic[:]) || binary.BigEndian.Uint16(frame[8:10]) != 1 {
		return Watermark{}, errors.New("invalid watermark envelope")
	}
	digest := sha256.Sum256(frame[:watermarkBody])
	if !stringEqualBytes(frame[watermarkBody:], digest[:]) {
		return Watermark{}, errors.New("invalid watermark checksum")
	}
	return Watermark{
		Session: binary.BigEndian.Uint64(frame[16:24]), Epoch: binary.BigEndian.Uint64(frame[24:32]),
		Sequence: binary.BigEndian.Uint64(frame[32:40]), SentOffsetNanos: int64(binary.BigEndian.Uint64(frame[40:48])),
	}, nil
}

func stringEqualBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	var different byte
	for i := range a {
		different |= a[i] ^ b[i]
	}
	return different == 0
}

type Classification string

const (
	ClassificationOK           Classification = "ok"
	ClassificationCrossSession Classification = "cross_session"
	ClassificationStaleEpoch   Classification = "stale_epoch"
	ClassificationDuplicate    Classification = "duplicate"
	ClassificationReordered    Classification = "reordered"
	ClassificationUnexpected   Classification = "unexpected"
)

type sentinelKey struct{ session, epoch, sequence uint64 }

type sentinelOutstanding struct {
	sentAt   time.Time
	deadline time.Time
}

type sentinelStreamKey struct{ session, epoch uint64 }

type sentinelSequenceState struct {
	maximum uint64
}

type Sentinel struct {
	mu            sync.Mutex
	expected      map[sentinelKey]sentinelOutstanding
	streams       map[sentinelStreamKey]*sentinelSequenceState
	fences        map[uint64]uint64
	perSession    map[uint64]int
	missing       int64
	maxGlobal     int
	maxPerSession int
}

func NewSentinel() *Sentinel {
	return NewBoundedSentinel(4096, 4096)
}

func NewBoundedSentinel(maxGlobal, maxPerSession int) *Sentinel {
	if maxGlobal <= 0 || maxPerSession <= 0 {
		panic("sentinel bounds must be positive")
	}
	return &Sentinel{
		expected: make(map[sentinelKey]sentinelOutstanding), streams: make(map[sentinelStreamKey]*sentinelSequenceState),
		fences: make(map[uint64]uint64), perSession: make(map[uint64]int), maxGlobal: maxGlobal, maxPerSession: maxPerSession,
	}
}

func (s *Sentinel) Sent(mark Watermark) {
	now := time.Now()
	s.SentAt(mark, now, now.Add(time.Hour))
}

func (s *Sentinel) SentAt(mark Watermark, sentAt, deadline time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := sentinelKey{mark.Session, mark.Epoch, mark.Sequence}
	if mark.Session == 0 || mark.Epoch == 0 || mark.Sequence == 0 || !deadline.After(sentAt) {
		return false
	}
	if _, exists := s.expected[key]; exists || len(s.expected) >= s.maxGlobal || s.perSession[mark.Session] >= s.maxPerSession {
		return false
	}
	s.expected[key] = sentinelOutstanding{sentAt: sentAt, deadline: deadline}
	s.perSession[mark.Session]++
	return true
}

func (s *Sentinel) Fence(session, epoch uint64) {
	s.mu.Lock()
	if epoch > s.fences[session] {
		s.fences[session] = epoch
		for key := range s.streams {
			if key.session == session && key.epoch < epoch {
				delete(s.streams, key)
			}
		}
	}
	s.mu.Unlock()
}

func (s *Sentinel) Receive(observedSession uint64, mark Watermark) Classification {
	classification, _, _ := s.ReceiveAt(observedSession, mark)
	return classification
}

func (s *Sentinel) ReceiveAt(observedSession uint64, mark Watermark) (Classification, time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if observedSession != mark.Session {
		return ClassificationCrossSession, time.Time{}, false
	}
	if mark.Epoch < s.fences[observedSession] {
		return ClassificationStaleEpoch, time.Time{}, false
	}
	key := sentinelKey{mark.Session, mark.Epoch, mark.Sequence}
	streamKey := sentinelStreamKey{session: mark.Session, epoch: mark.Epoch}
	state := s.streams[streamKey]
	outstanding, ok := s.expected[key]
	if !ok {
		if state != nil && mark.Sequence <= state.maximum {
			return ClassificationDuplicate, time.Time{}, false
		}
		return ClassificationUnexpected, time.Time{}, false
	}
	delete(s.expected, key)
	s.perSession[mark.Session]--
	if state == nil {
		state = &sentinelSequenceState{}
		s.streams[streamKey] = state
	}
	reordered := mark.Sequence < state.maximum
	if mark.Sequence > state.maximum {
		state.maximum = mark.Sequence
	}
	if reordered {
		return ClassificationReordered, outstanding.sentAt, true
	}
	return ClassificationOK, outstanding.sentAt, true
}

func (s *Sentinel) Drop(mark Watermark) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := sentinelKey{mark.Session, mark.Epoch, mark.Sequence}
	if _, exists := s.expected[key]; exists {
		delete(s.expected, key)
		s.perSession[mark.Session]--
	}
}

func (s *Sentinel) Expire(now time.Time) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var expired int64
	for key, outstanding := range s.expected {
		if !now.Before(outstanding.deadline) {
			delete(s.expected, key)
			s.perSession[key.session]--
			expired++
		}
	}
	s.missing += expired
	return expired
}

func (s *Sentinel) Missing() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.missing + int64(len(s.expected))
}

func (s *Sentinel) Pending() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(len(s.expected))
}

type FailureCode string

const (
	FailureConfiguration FailureCode = "configuration"
	FailureRuntime       FailureCode = "runtime"
	FailureCapacity      FailureCode = "capacity"
	FailureCrossSession  FailureCode = "cross_session"
	FailureStaleEpoch    FailureCode = "stale_epoch"
	FailureDuplicate     FailureCode = "duplicate"
	FailureReordered     FailureCode = "reordered"
	FailureMissing       FailureCode = "missing"
	FailureUnexpected    FailureCode = "unexpected"
	FailureQueueOverflow FailureCode = "queue_overflow"
	FailureGoroutineLeak FailureCode = "goroutine_leak"
	FailureHeapLeak      FailureCode = "heap_leak"
	FailureHeapSlope     FailureCode = "heap_slope"
	FailureSLOAudioP95   FailureCode = "slo_transit_p95"
	FailureSLOAudioP99   FailureCode = "slo_transit_p99"
	FailureSLOBargeP95   FailureCode = "slo_bargein_p95"
	FailureTelemetry     FailureCode = "telemetry_unavailable_or_inadequate"
	FailureFence         FailureCode = "fence_probe"
)

type Failure struct {
	Code  FailureCode `json:"code"`
	Count int64       `json:"count"`
}

type LatencySummary struct {
	Count uint64   `json:"count"`
	P50   Duration `json:"p50"`
	P95   Duration `json:"p95"`
	P99   Duration `json:"p99"`
	Max   Duration `json:"max"`
}

func summarize(histogram *Histogram) LatencySummary {
	return LatencySummary{Count: histogram.Count(), P50: Duration(histogram.Percentile(50)), P95: Duration(histogram.Percentile(95)), P99: Duration(histogram.Percentile(99)), Max: Duration(histogram.Max())}
}

type HostProfile struct {
	CPUModel    string `json:"cpu_model"`
	CPUCores    int    `json:"cpu_cores"`
	MemoryBytes uint64 `json:"memory_bytes"`
	OS          string `json:"os"`
	Arch        string `json:"arch"`
	GoVersion   string `json:"go_version"`
}

func (p HostProfile) ExactMatch(other HostProfile) bool { return p == other }

type Thresholds struct {
	MinimumSessions       int
	MaximumGoroutineDelta int
	MaximumHeapDelta      int64
	MaximumSlopePerHour   float64
	TransitP95            time.Duration
	TransitP99            time.Duration
	BargeinP95            time.Duration
}

func (t Thresholds) normalized() Thresholds {
	if t.MinimumSessions == 0 {
		t.MinimumSessions = 250
	}
	if t.MaximumGoroutineDelta == 0 {
		t.MaximumGoroutineDelta = 10
	}
	if t.MaximumHeapDelta == 0 {
		t.MaximumHeapDelta = 16 << 20
	}
	if t.MaximumSlopePerHour == 0 {
		t.MaximumSlopePerHour = 4 << 20
	}
	if t.TransitP95 == 0 {
		t.TransitP95 = 15 * time.Millisecond
	}
	if t.TransitP99 == 0 {
		t.TransitP99 = 30 * time.Millisecond
	}
	if t.BargeinP95 == 0 {
		t.BargeinP95 = 100 * time.Millisecond
	}
	return t
}

type GatewayTelemetry struct {
	Transit       LatencySummary
	Bargein       LatencySummary
	QueueOverflow int64
}

type TelemetryEvidence struct {
	Status                string    `json:"status"`
	CollectedAt           time.Time `json:"collected_at,omitempty"`
	TransitSamples        uint64    `json:"transit_samples"`
	BargeinSamples        uint64    `json:"bargein_samples"`
	QueueOverflowObserved bool      `json:"queue_overflow_observed"`
}

type Config struct {
	GatewayURL            string
	MetricsURL            string
	BearerToken           string
	MetricsBearerToken    string
	AllowHosts            []string
	Scenario              Scenario
	EvidenceClass         EvidenceClass
	Sessions              int
	ConnectionConcurrency int
	Duration              time.Duration
	FrameInterval         time.Duration
	DialTimeout           time.Duration
	ResponseTimeout       time.Duration
	TeardownTimeout       time.Duration
	EnforceSLO            bool
	PinnedProfile         *HostProfile
	Thresholds            Thresholds
	SoakSampleInterval    time.Duration
	TelemetrySnapshot     func() GatewayTelemetry
}

type Report struct {
	SchemaVersion       string            `json:"schema_version"`
	ConfigHash          string            `json:"config_hash"`
	Scenario            Scenario          `json:"scenario"`
	EvidenceClass       EvidenceClass     `json:"evidence_class"`
	StartedAt           time.Time         `json:"started_at"`
	FinishedAt          time.Time         `json:"finished_at"`
	Duration            Duration          `json:"duration"`
	SessionsRequested   int               `json:"sessions_requested"`
	SessionsEstablished int               `json:"sessions_established"`
	SessionsHeld        int               `json:"sessions_held"`
	FramesSent          int64             `json:"frames_sent"`
	FramesReceived      int64             `json:"frames_received"`
	CrossSession        int64             `json:"cross_session"`
	StaleEpoch          int64             `json:"stale_epoch"`
	Duplicate           int64             `json:"duplicate"`
	Reordered           int64             `json:"reordered"`
	Missing             int64             `json:"missing"`
	Unexpected          int64             `json:"unexpected"`
	RuntimeFailures     int64             `json:"runtime_failures"`
	FencesRequested     int               `json:"fences_requested"`
	FencesAcknowledged  int               `json:"fences_acknowledged"`
	FenceHealthChecks   int               `json:"fence_health_checks"`
	QueueOverflow       int64             `json:"queue_overflow"`
	Audio               LatencySummary    `json:"transit_latency"`
	GatewayTransit      LatencySummary    `json:"gateway_transit_latency"`
	Bargein             LatencySummary    `json:"bargein_suppression"`
	GatewayBargein      LatencySummary    `json:"gateway_bargein_diagnostic"`
	Telemetry           TelemetryEvidence `json:"gateway_metrics"`
	GoroutineBaseline   int               `json:"goroutine_baseline"`
	GoroutineAfter      int               `json:"goroutine_after"`
	GoroutineDelta      int               `json:"goroutine_delta"`
	HeapBaselineBytes   uint64            `json:"heap_baseline_bytes"`
	HeapAfterBytes      uint64            `json:"heap_after_bytes"`
	HeapDeltaBytes      int64             `json:"heap_delta_bytes"`
	HeapSlopePerHour    float64           `json:"heap_slope_bytes_per_hour,omitempty"`
	HeapSamples         int               `json:"heap_samples,omitempty"`
	Host                HostProfile       `json:"host"`
	PinnedProfileMatch  bool              `json:"pinned_profile_match"`
	SLOEnforced         bool              `json:"slo_enforced"`
	Failures            []Failure         `json:"failures"`
}

func Evaluate(report Report, thresholds Thresholds, enforceSLO bool) []Failure {
	t := thresholds.normalized()
	counts := map[FailureCode]int64{}
	if report.SessionsRequested >= t.MinimumSessions && report.SessionsHeld < t.MinimumSessions {
		counts[FailureCapacity] = int64(t.MinimumSessions - report.SessionsHeld)
	}
	if report.FencesAcknowledged < report.FencesRequested {
		counts[FailureFence] += int64(report.FencesRequested - report.FencesAcknowledged)
	}
	if report.FenceHealthChecks < report.FencesRequested {
		counts[FailureFence] += int64(report.FencesRequested - report.FenceHealthChecks)
	}
	for code, count := range map[FailureCode]int64{FailureRuntime: report.RuntimeFailures, FailureCrossSession: report.CrossSession, FailureStaleEpoch: report.StaleEpoch, FailureDuplicate: report.Duplicate, FailureReordered: report.Reordered, FailureMissing: report.Missing, FailureUnexpected: report.Unexpected, FailureQueueOverflow: report.QueueOverflow} {
		if count > 0 {
			counts[code] = count
		}
	}
	if report.GoroutineDelta > t.MaximumGoroutineDelta {
		counts[FailureGoroutineLeak] = int64(report.GoroutineDelta - t.MaximumGoroutineDelta)
	}
	if report.HeapDeltaBytes > t.MaximumHeapDelta {
		counts[FailureHeapLeak] = report.HeapDeltaBytes - t.MaximumHeapDelta
	}
	if report.HeapSamples >= 5 && report.HeapSlopePerHour > t.MaximumSlopePerHour {
		counts[FailureHeapSlope] = int64(report.HeapSlopePerHour - t.MaximumSlopePerHour)
	}
	if enforceSLO {
		if report.Telemetry.Status != "adequate" || report.Audio.Count < 20 || report.GatewayTransit.Count < 20 || report.Bargein.Count < 20 || !report.Telemetry.QueueOverflowObserved {
			counts[FailureTelemetry] = 1
		}
		if report.Audio.P95.Duration() >= t.TransitP95 {
			counts[FailureSLOAudioP95] = 1
		}
		if report.Audio.P99.Duration() >= t.TransitP99 {
			counts[FailureSLOAudioP99] = 1
		}
		if report.Bargein.Count > 0 && report.Bargein.P95.Duration() >= t.BargeinP95 {
			counts[FailureSLOBargeP95] = 1
		}
	}
	codes := make([]string, 0, len(counts))
	for code := range counts {
		codes = append(codes, string(code))
	}
	sort.Strings(codes)
	failures := make([]Failure, 0, len(codes))
	for _, code := range codes {
		failures = append(failures, Failure{Code: FailureCode(code), Count: counts[FailureCode(code)]})
	}
	return failures
}

func MarshalReport(report Report) ([]byte, error) {
	if len(report.Failures) > 64 {
		report.Failures = append([]Failure(nil), report.Failures[:64]...)
	}
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return nil, err
	}
	if len(raw) > MaxReportBytes {
		return nil, errors.New("load report exceeds bounded size")
	}
	return append(raw, '\n'), nil
}

func DetectHostProfile() HostProfile {
	profile := HostProfile{CPUCores: runtime.NumCPU(), OS: runtime.GOOS, Arch: runtime.GOARCH, GoVersion: runtime.Version()}
	if information, err := cpu.Info(); err == nil && len(information) != 0 {
		profile.CPUModel = strings.TrimSpace(information[0].ModelName)
	}
	if memory, err := mem.VirtualMemory(); err == nil {
		profile.MemoryBytes = memory.Total
	}
	if raw, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		for _, line := range strings.Split(string(raw), "\n") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 && (strings.TrimSpace(parts[0]) == "model name" || strings.TrimSpace(parts[0]) == "Hardware") {
				profile.CPUModel = strings.TrimSpace(parts[1])
				break
			}
		}
	}
	if raw, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, line := range strings.Split(string(raw), "\n") {
			fields := strings.Fields(line)
			if len(fields) >= 2 && fields[0] == "MemTotal:" {
				if kb, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
					profile.MemoryBytes = kb * 1024
				}
				break
			}
		}
	}
	if profile.CPUModel == "" {
		profile.CPUModel = runtime.GOARCH
	}
	return profile
}

func configHash(cfg Config) string {
	allowHosts := append([]string(nil), cfg.AllowHosts...)
	for index := range allowHosts {
		allowHosts[index] = strings.ToLower(strings.TrimSpace(allowHosts[index]))
	}
	sort.Strings(allowHosts)
	value := struct {
		Scenario, Evidence              string
		Gateway, Metrics, TelemetryMode string
		AllowHosts                      []string
		Sessions, Concurrency           int
		Duration, Interval              string
		Dial, Response, Teardown        string
		EnforceSLO                      bool
		Thresholds                      Thresholds
		SoakSampleInterval              string
		PinnedProfile                   *HostProfile
	}{
		Scenario: string(cfg.Scenario), Evidence: string(cfg.EvidenceClass), Gateway: normalizedEndpoint(cfg.GatewayURL),
		Metrics: normalizedEndpoint(cfg.MetricsURL), TelemetryMode: telemetryMode(cfg), AllowHosts: allowHosts,
		Sessions: cfg.Sessions, Concurrency: cfg.ConnectionConcurrency, Duration: cfg.Duration.String(), Interval: cfg.FrameInterval.String(),
		Dial: cfg.DialTimeout.String(), Response: cfg.ResponseTimeout.String(), Teardown: cfg.TeardownTimeout.String(),
		EnforceSLO: cfg.EnforceSLO, Thresholds: cfg.Thresholds.normalized(), SoakSampleInterval: cfg.SoakSampleInterval.String(),
		PinnedProfile: cfg.PinnedProfile,
	}
	raw, _ := json.Marshal(value)
	digest := sha256.Sum256(raw)
	return "v1:" + hex.EncodeToString(digest[:])
}

func telemetryMode(cfg Config) string {
	if cfg.MetricsURL != "" {
		return "scrape"
	}
	if cfg.TelemetrySnapshot != nil {
		return "callback"
	}
	return "unavailable"
}

func normalizedEndpoint(raw string) string {
	if raw == "" {
		return ""
	}
	endpoint, err := url.Parse(raw)
	if err != nil {
		return "invalid"
	}
	endpoint.Scheme = strings.ToLower(endpoint.Scheme)
	host := strings.ToLower(endpoint.Hostname())
	if port := endpoint.Port(); port != "" {
		host = net.JoinHostPort(host, port)
	}
	endpoint.Host = host
	return endpoint.String()
}

func normalizeConfig(cfg Config) (Config, error) {
	if cfg.Scenario == "" {
		cfg.Scenario = ScenarioCapacity
	}
	if cfg.EvidenceClass == "" {
		cfg.EvidenceClass = EvidenceSmoke
	}
	if cfg.Sessions <= 0 || cfg.Sessions > maximumSessions {
		return cfg, errors.New("sessions must be between 1 and 10000")
	}
	if cfg.ConnectionConcurrency == 0 {
		cfg.ConnectionConcurrency = min(cfg.Sessions, 64)
	}
	if cfg.ConnectionConcurrency <= 0 || cfg.ConnectionConcurrency > cfg.Sessions {
		return cfg, errors.New("connection concurrency is invalid")
	}
	if cfg.Duration <= 0 || cfg.Duration > maximumDuration {
		return cfg, errors.New("duration must be between zero and 24h")
	}
	if cfg.FrameInterval == 0 {
		cfg.FrameInterval = time.Second
	}
	if cfg.FrameInterval < 10*time.Millisecond || cfg.FrameInterval > cfg.Duration {
		return cfg, errors.New("frame interval is outside run bounds")
	}
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	if cfg.ResponseTimeout == 0 {
		cfg.ResponseTimeout = 2 * time.Second
	}
	if cfg.TeardownTimeout == 0 {
		cfg.TeardownTimeout = 5 * time.Second
	}
	for _, timeout := range []time.Duration{cfg.DialTimeout, cfg.ResponseTimeout, cfg.TeardownTimeout} {
		if timeout <= 0 || timeout > time.Minute {
			return cfg, errors.New("deadline is outside safe bounds")
		}
	}
	perSessionOutstanding := int((cfg.ResponseTimeout + cfg.FrameInterval - 1) / cfg.FrameInterval)
	if perSessionOutstanding < 1 || perSessionOutstanding > maximumOutstanding/cfg.Sessions {
		return cfg, errors.New("response deadline and frame interval exceed the outstanding-work budget")
	}
	if cfg.Scenario != ScenarioCapacity && cfg.Scenario != ScenarioSoak {
		return cfg, errors.New("scenario must be capacity or soak")
	}
	switch cfg.EvidenceClass {
	case EvidenceSmoke:
	case EvidenceCapacity10:
		if cfg.Scenario != ScenarioCapacity || cfg.Duration != 10*time.Minute {
			return cfg, errors.New("capacity evidence requires an exact ten-minute capacity run")
		}
	case EvidenceSoak2H:
		if cfg.Scenario != ScenarioSoak || cfg.Duration != 2*time.Hour {
			return cfg, errors.New("soak evidence requires an exact two-hour soak run")
		}
	default:
		return cfg, errors.New("evidence class is invalid")
	}
	if cfg.Scenario == ScenarioSoak {
		if cfg.SoakSampleInterval == 0 {
			cfg.SoakSampleInterval = 10 * time.Minute
		}
		if cfg.SoakSampleInterval < time.Second || cfg.SoakSampleInterval > cfg.Duration/2 {
			return cfg, errors.New("soak sample interval is outside run bounds")
		}
	}
	if err := validateEndpointURL(cfg.GatewayURL, map[string]struct{}{"ws": {}, "wss": {}}, cfg.AllowHosts); err != nil {
		return cfg, err
	}
	if cfg.MetricsURL != "" {
		if err := validateEndpointURL(cfg.MetricsURL, map[string]struct{}{"http": {}, "https": {}}, cfg.AllowHosts); err != nil {
			return cfg, fmt.Errorf("metrics URL: %w", err)
		}
	}
	if cfg.EnforceSLO && (cfg.PinnedProfile == nil || !DetectHostProfile().ExactMatch(*cfg.PinnedProfile)) {
		return cfg, errors.New("SLO enforcement requires an exact pinned profile match")
	}
	if cfg.EnforceSLO && cfg.MetricsURL == "" && cfg.TelemetrySnapshot == nil {
		return cfg, errors.New("SLO enforcement requires observable gateway metrics")
	}
	return cfg, nil
}

func validateGatewayURL(raw string) error {
	return validateEndpointURL(raw, map[string]struct{}{"ws": {}, "wss": {}}, nil)
}

func validateEndpointURL(raw string, schemes map[string]struct{}, allowHosts []string) error {
	endpoint, err := url.Parse(raw)
	if err != nil || endpoint.Host == "" {
		return errors.New("endpoint must be an absolute URL")
	}
	if _, ok := schemes[strings.ToLower(endpoint.Scheme)]; !ok {
		return errors.New("endpoint scheme is not allowed")
	}
	if endpoint.User != nil || endpoint.Fragment != "" || endpoint.RawQuery != "" {
		return errors.New("gateway URL credentials, query, and fragment are forbidden")
	}
	host := endpoint.Hostname()
	if host == "" {
		return errors.New("endpoint host is required")
	}
	host = strings.ToLower(host)
	if host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return nil
	}
	for _, allowed := range allowHosts {
		if host == strings.ToLower(strings.TrimSpace(allowed)) && host != "" {
			return nil
		}
	}
	ip := net.ParseIP(host)
	if ip != nil && (ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast()) {
		return nil
	}
	if ip != nil {
		return errors.New("endpoint must target a local or private test host")
	}
	if !strings.Contains(host, ".") {
		return errors.New("single-label hosts require an explicit allow-host entry")
	}
	resolveContext, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	addresses, err := net.DefaultResolver.LookupIPAddr(resolveContext, host)
	if err != nil || len(addresses) == 0 {
		return errors.New("endpoint host did not resolve to a private address")
	}
	for _, address := range addresses {
		if !(address.IP.IsLoopback() || address.IP.IsPrivate() || address.IP.IsLinkLocalUnicast()) {
			return errors.New("endpoint host resolved outside local or private networks")
		}
	}
	return nil
}

type runCounters struct {
	mu                                             sync.Mutex
	framesSent, framesReceived                     int64
	cross, stale, duplicate, reordered, unexpected int64
	runtimeFailures                                int64
	fencesAcknowledged, fenceHealthChecks          int
	histogram, fenceHistogram                      *Histogram
}

func Run(ctx context.Context, input Config) (Report, error) {
	started := time.Now().UTC()
	report := Report{SchemaVersion: ReportSchemaVersion, StartedAt: started, Scenario: input.Scenario, SessionsRequested: input.Sessions, Host: DetectHostProfile()}
	cfg, err := normalizeConfig(input)
	if err != nil {
		report.Failures = []Failure{{Code: FailureConfiguration, Count: 1}}
		report.FinishedAt = time.Now().UTC()
		report.Duration = Duration(report.FinishedAt.Sub(started))
		return report, err
	}
	report.Scenario, report.EvidenceClass, report.ConfigHash = cfg.Scenario, cfg.EvidenceClass, configHash(cfg)
	if cfg.PinnedProfile != nil {
		report.PinnedProfileMatch = report.Host.ExactMatch(*cfg.PinnedProfile)
	}
	telemetryBaseline, telemetryBaselineErr := captureTelemetry(ctx, cfg)
	report.SLOEnforced = cfg.EnforceSLO && report.PinnedProfileMatch
	runtime.GC()
	debug.FreeOSMemory()
	report.GoroutineBaseline = runtime.NumGoroutine()
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	report.HeapBaselineBytes = memory.HeapAlloc

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	clients := make([]*loadClient, cfg.Sessions)
	jobs := make(chan int)
	var establish sync.WaitGroup
	var establishMu sync.Mutex
	var establishErr error
	for worker := 0; worker < cfg.ConnectionConcurrency; worker++ {
		establish.Add(1)
		go func() {
			defer establish.Done()
			for index := range jobs {
				client, dialErr := dialLoadClient(runCtx, cfg, uint64(index+1))
				if dialErr != nil {
					establishMu.Lock()
					establishErr = errors.Join(establishErr, dialErr)
					establishMu.Unlock()
					continue
				}
				clients[index] = client
			}
		}()
	}
queueing:
	for index := range clients {
		select {
		case jobs <- index:
		case <-runCtx.Done():
			break queueing
		}
	}
	close(jobs)
	establish.Wait()
	for _, client := range clients {
		if client != nil {
			report.SessionsEstablished++
		}
	}
	report.SessionsHeld = report.SessionsEstablished
	if report.SessionsEstablished != cfg.Sessions {
		closeClients(clients, cfg.TeardownTimeout)
		report.Failures = append(report.Failures, Failure{Code: FailureCapacity, Count: int64(cfg.Sessions - report.SessionsEstablished)})
		finishReport(&report, cfg, nil, nil, telemetryBaseline, telemetryBaselineErr)
		return report, errors.New("not all load sessions established")
	}

	perSessionOutstanding := int((cfg.ResponseTimeout + cfg.FrameInterval - 1) / cfg.FrameInterval)
	sentinel := NewBoundedSentinel(perSessionOutstanding*cfg.Sessions, perSessionOutstanding)
	counters := &runCounters{histogram: NewHistogram(time.Minute, 3), fenceHistogram: NewHistogram(time.Minute, 3)}
	runOrigin := time.Now()
	var readerWG sync.WaitGroup
	for _, client := range clients {
		readerWG.Add(1)
		go func(client *loadClient) { defer readerWG.Done(); client.read(runCtx, sentinel, counters) }(client)
	}
	report.FencesRequested = report.SessionsEstablished
	if probeErr := runFenceProbes(runCtx, cfg, runOrigin, clients, sentinel, counters); probeErr != nil {
		cancelRun()
		closeClients(clients, cfg.TeardownTimeout)
		readerWG.Wait()
		finishReport(&report, cfg, sentinel, counters, telemetryBaseline, telemetryBaselineErr)
		return report, errors.New("load fence probe failed")
	}
	var heapSamples []HeapSample
	if cfg.Scenario == ScenarioSoak {
		runtime.GC()
		runtime.ReadMemStats(&memory)
		heapSamples = append(heapSamples, HeapSample{At: time.Now(), Bytes: memory.HeapAlloc})
	}
	timer := time.NewTimer(cfg.Duration)
	frameTicker := time.NewTicker(cfg.FrameInterval)
	defer frameTicker.Stop()
	var sampler *time.Ticker
	if cfg.Scenario == ScenarioSoak {
		sampler = time.NewTicker(cfg.SoakSampleInterval)
		defer sampler.Stop()
	}
	sequence := uint64(2)
	sendFrameBatch(runCtx, clients, cfg, runOrigin, sequence, sentinel, counters)
	waiting := true
	for waiting {
		select {
		case <-ctx.Done():
			waiting = false
		case <-timer.C:
			waiting = false
		case <-frameTicker.C:
			sentinel.Expire(time.Now())
			sequence++
			sendFrameBatch(runCtx, clients, cfg, runOrigin, sequence, sentinel, counters)
		case at := <-tickerChannel(sampler):
			runtime.GC()
			runtime.ReadMemStats(&memory)
			heapSamples = append(heapSamples, HeapSample{At: at, Bytes: memory.HeapAlloc})
		}
	}
	timer.Stop()
	if ctx.Err() == nil {
		grace := time.NewTimer(cfg.ResponseTimeout)
		poll := time.NewTicker(time.Millisecond)
		for sentinel.Pending() != 0 {
			select {
			case <-ctx.Done():
				grace.Stop()
				poll.Stop()
				goto graceComplete
			case <-grace.C:
				poll.Stop()
				goto graceComplete
			case now := <-poll.C:
				sentinel.Expire(now)
			}
		}
		if !grace.Stop() {
			select {
			case <-grace.C:
			default:
			}
		}
		poll.Stop()
	}
graceComplete:
	sentinel.Expire(time.Now())
	cancelRun()
	closeClients(clients, cfg.TeardownTimeout)
	readerWG.Wait()
	runtime.ReadMemStats(&memory)
	if cfg.Scenario == ScenarioSoak {
		runtime.GC()
		runtime.ReadMemStats(&memory)
		heapSamples = append(heapSamples, HeapSample{At: time.Now(), Bytes: memory.HeapAlloc})
	}
	finishReport(&report, cfg, sentinel, counters, telemetryBaseline, telemetryBaselineErr)
	if cfg.Scenario == ScenarioSoak && len(heapSamples) >= 5 {
		report.HeapSamples = len(heapSamples)
		report.HeapSlopePerHour, _ = HeapSlopePerHour(heapSamples)
		report.Failures = Evaluate(report, cfg.Thresholds, report.SLOEnforced)
	}
	if establishErr != nil {
		return report, errors.New("load session establishment failed")
	}
	if len(report.Failures) > 0 {
		return report, errors.New("load correctness or SLO gate failed")
	}
	return report, nil
}

func tickerChannel(ticker *time.Ticker) <-chan time.Time {
	if ticker == nil {
		return nil
	}
	return ticker.C
}

func finishReport(report *Report, cfg Config, sentinel *Sentinel, counters *runCounters, telemetryBaseline *metricSnapshot, telemetryBaselineErr error) {
	if counters != nil {
		counters.mu.Lock()
		report.FramesSent, report.FramesReceived = counters.framesSent, counters.framesReceived
		report.CrossSession, report.StaleEpoch, report.Duplicate, report.Reordered, report.Unexpected = counters.cross, counters.stale, counters.duplicate, counters.reordered, counters.unexpected
		report.RuntimeFailures = counters.runtimeFailures
		report.FencesAcknowledged = counters.fencesAcknowledged
		report.FenceHealthChecks = counters.fenceHealthChecks
		report.Audio = summarize(counters.histogram)
		report.Bargein = summarize(counters.fenceHistogram)
		counters.mu.Unlock()
	}
	if sentinel != nil {
		report.Missing = sentinel.Missing()
	}
	if cfg.MetricsURL != "" {
		final, err := captureTelemetry(context.Background(), cfg)
		applyScrapedTelemetry(report, telemetryBaseline, final, errors.Join(telemetryBaselineErr, err))
	} else if cfg.TelemetrySnapshot != nil {
		telemetry := cfg.TelemetrySnapshot()
		report.GatewayTransit, report.GatewayBargein, report.QueueOverflow = telemetry.Transit, telemetry.Bargein, telemetry.QueueOverflow
		report.Telemetry = TelemetryEvidence{
			Status: "available", CollectedAt: time.Now().UTC(), TransitSamples: telemetry.Transit.Count,
			BargeinSamples: telemetry.Bargein.Count, QueueOverflowObserved: true,
		}
		if telemetry.Transit.Count >= 20 {
			report.Telemetry.Status = "adequate"
		}
	} else {
		report.Telemetry.Status = "unavailable"
	}
	runtime.GC()
	debug.FreeOSMemory()
	report.GoroutineAfter = runtime.NumGoroutine()
	report.GoroutineDelta = report.GoroutineAfter - report.GoroutineBaseline
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	report.HeapAfterBytes = memory.HeapAlloc
	report.HeapDeltaBytes = int64(report.HeapAfterBytes) - int64(report.HeapBaselineBytes)
	report.FinishedAt = time.Now().UTC()
	report.Duration = Duration(report.FinishedAt.Sub(report.StartedAt))
	report.Failures = Evaluate(*report, cfg.Thresholds, report.SLOEnforced)
}

func runFenceProbes(ctx context.Context, cfg Config, origin time.Time, clients []*loadClient, sentinel *Sentinel, counters *runCounters) error {
	jobs := make(chan *loadClient)
	var workers sync.WaitGroup
	var failuresMu sync.Mutex
	var failures error
	for range cfg.ConnectionConcurrency {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for client := range jobs {
				if err := client.fenceProbe(ctx, cfg, origin, sentinel, counters); err != nil {
					client.fail(counters)
					failuresMu.Lock()
					failures = errors.Join(failures, err)
					failuresMu.Unlock()
				}
			}
		}()
	}
	for _, client := range clients {
		select {
		case jobs <- client:
		case <-ctx.Done():
			close(jobs)
			workers.Wait()
			return ctx.Err()
		}
	}
	close(jobs)
	workers.Wait()
	return failures
}

func (c *loadClient) fenceProbe(parent context.Context, cfg Config, origin time.Time, sentinel *Sentinel, counters *runCounters) error {
	ctx, cancel := context.WithTimeout(parent, cfg.ResponseTimeout)
	defer cancel()
	if err := c.send(ctx, cfg, origin, 1, 1, true, sentinel, counters); err != nil {
		return err
	}
	created, err := waitResponseSignal(ctx, c.created, "")
	if err != nil || created.id == "" {
		return errors.New("fence response was not created")
	}
	if err := waitObservedMark(ctx, c.observed, Watermark{Session: c.id, Epoch: 1, Sequence: 1}); err != nil {
		return err
	}
	cancelStarted := time.Now()
	if err := writeJSON(ctx, c.conn, struct {
		Type       string `json:"type"`
		ResponseID string `json:"response_id"`
	}{Type: "response.cancel", ResponseID: created.id}); err != nil {
		return err
	}
	done, err := waitResponseSignal(ctx, c.done, created.id)
	if err != nil || done.status != "cancelled" {
		return errors.New("fence cancellation was not acknowledged")
	}
	acknowledgementLatency := time.Since(cancelStarted)
	sentinel.Fence(c.id, 2)
	counters.mu.Lock()
	counters.fencesAcknowledged++
	counters.fenceHistogram.Record(acknowledgementLatency)
	counters.mu.Unlock()

	if err := c.send(ctx, cfg, origin, 2, 1, false, sentinel, counters); err != nil {
		return err
	}
	healthCreated, err := waitResponseSignal(ctx, c.created, "")
	if err != nil || healthCreated.id == "" {
		return errors.New("post-fence response was not created")
	}
	if err := waitObservedMark(ctx, c.observed, Watermark{Session: c.id, Epoch: 2, Sequence: 1}); err != nil {
		return err
	}
	healthDone, err := waitResponseSignal(ctx, c.done, healthCreated.id)
	if err != nil || healthDone.status != "completed" {
		return errors.New("post-fence response did not complete")
	}
	counters.mu.Lock()
	counters.fenceHealthChecks++
	counters.mu.Unlock()
	return nil
}

func waitResponseSignal(ctx context.Context, signals <-chan responseSignal, responseID string) (responseSignal, error) {
	for {
		select {
		case signal := <-signals:
			if responseID == "" || signal.id == responseID {
				return signal, nil
			}
		case <-ctx.Done():
			return responseSignal{}, ctx.Err()
		}
	}
}

func waitObservedMark(ctx context.Context, observed <-chan Watermark, wanted Watermark) error {
	for {
		select {
		case mark := <-observed:
			if mark.Session == wanted.Session && mark.Epoch == wanted.Epoch && mark.Sequence == wanted.Sequence {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

type loadClient struct {
	id       uint64
	conn     *websocket.Conn
	failed   atomic.Bool
	created  chan responseSignal
	done     chan responseSignal
	observed chan Watermark
}

type responseSignal struct {
	id, status string
}

func dialLoadClient(ctx context.Context, cfg Config, id uint64) (*loadClient, error) {
	dialCtx, cancel := context.WithTimeout(ctx, cfg.DialTimeout)
	defer cancel()
	header := make(http.Header)
	if cfg.BearerToken != "" {
		header.Set("Authorization", "Bearer "+cfg.BearerToken)
	}
	conn, response, err := websocket.Dial(dialCtx, cfg.GatewayURL, &websocket.DialOptions{HTTPHeader: header})
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	if err != nil {
		return nil, err
	}
	conn.SetReadLimit(64 << 10)
	_, raw, err := conn.Read(dialCtx)
	if err != nil {
		_ = conn.CloseNow()
		return nil, err
	}
	var envelope struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(raw, &envelope) != nil || envelope.Type != "session.created" {
		_ = conn.CloseNow()
		return nil, errors.New("gateway did not create load session")
	}
	return &loadClient{
		id: id, conn: conn, created: make(chan responseSignal, 4),
		done: make(chan responseSignal, 4), observed: make(chan Watermark, 4),
	}, nil
}

func sendFrameBatch(ctx context.Context, clients []*loadClient, cfg Config, origin time.Time, sequence uint64, sentinel *Sentinel, counters *runCounters) {
	forEachRunnableClient(ctx, clients, func(client *loadClient) {
		if err := client.send(ctx, cfg, origin, 2, sequence, false, sentinel, counters); err != nil {
			client.fail(counters)
		}
	})
}

func forEachRunnableClient(ctx context.Context, clients []*loadClient, visit func(*loadClient)) {
	for _, client := range clients {
		if client == nil || client.failed.Load() || ctx.Err() != nil {
			continue
		}
		visit(client)
	}
}

func (c *loadClient) send(ctx context.Context, cfg Config, origin time.Time, epoch, sequence uint64, fenceProbe bool, sentinel *Sentinel, counters *runCounters) error {
	now := time.Now()
	mark := Watermark{Session: c.id, Epoch: epoch, Sequence: sequence, SentOffsetNanos: now.Sub(origin).Nanoseconds()}
	if !sentinel.SentAt(mark, now, now.Add(cfg.ResponseTimeout)) {
		return errors.New("outstanding response budget exhausted")
	}
	appendEvent := struct{ Type, Audio string }{"input_audio_buffer.append", base64.StdEncoding.EncodeToString(EncodeWatermark(mark))}
	createEvent := struct {
		Type     string         `json:"type"`
		Response map[string]any `json:"response,omitempty"`
	}{Type: "response.create"}
	if fenceProbe {
		createEvent.Response = map[string]any{"metadata": map[string]string{"live_load_fence_probe": "1"}}
	}
	if err := writeJSON(ctx, c.conn, appendEvent); err != nil {
		sentinel.Drop(mark)
		return err
	}
	if err := writeJSON(ctx, c.conn, createEvent); err != nil {
		sentinel.Drop(mark)
		return err
	}
	counters.mu.Lock()
	counters.framesSent++
	counters.mu.Unlock()
	return nil
}

func writeJSON(ctx context.Context, conn *websocket.Conn, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, raw)
}

func (c *loadClient) read(ctx context.Context, sentinel *Sentinel, counters *runCounters) {
	for {
		_, raw, err := c.conn.Read(ctx)
		if err != nil {
			if ctx.Err() == nil {
				c.fail(counters)
			}
			return
		}
		var envelope struct {
			Type     string `json:"type"`
			Delta    string `json:"delta"`
			Response struct {
				ID, Status string
			} `json:"response"`
		}
		if json.Unmarshal(raw, &envelope) != nil {
			counters.record(ClassificationUnexpected, 0)
			continue
		}
		switch envelope.Type {
		case "response.created":
			nonblockingSignal(c.created, responseSignal{id: envelope.Response.ID, status: envelope.Response.Status})
			continue
		case "response.done":
			nonblockingSignal(c.done, responseSignal{id: envelope.Response.ID, status: envelope.Response.Status})
			continue
		case "response.output_audio.delta":
		default:
			continue
		}
		frame, err := base64.StdEncoding.DecodeString(envelope.Delta)
		if err != nil {
			counters.record(ClassificationUnexpected, 0)
			continue
		}
		mark, err := DecodeWatermark(frame)
		if err != nil {
			counters.record(ClassificationUnexpected, 0)
			continue
		}
		classification, sentAt, matched := sentinel.ReceiveAt(c.id, mark)
		var latency time.Duration
		if matched {
			latency = time.Since(sentAt)
		}
		counters.record(classification, latency)
		select {
		case c.observed <- mark:
		default:
		}
	}
}

func nonblockingSignal(target chan<- responseSignal, signal responseSignal) {
	select {
	case target <- signal:
	default:
	}
}

func (c *loadClient) fail(counters *runCounters) {
	if c.failed.CompareAndSwap(false, true) {
		counters.mu.Lock()
		counters.runtimeFailures++
		counters.mu.Unlock()
	}
}

func (c *runCounters) record(classification Classification, latency time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.framesReceived++
	switch classification {
	case ClassificationOK:
		c.histogram.Record(latency)
	case ClassificationCrossSession:
		c.cross++
	case ClassificationStaleEpoch:
		c.stale++
	case ClassificationDuplicate:
		c.duplicate++
	case ClassificationReordered:
		c.reordered++
	default:
		c.unexpected++
	}
}

func closeClients(clients []*loadClient, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var wg sync.WaitGroup
	for _, client := range clients {
		if client == nil {
			continue
		}
		wg.Add(1)
		go func(client *loadClient) {
			defer wg.Done()
			_ = client.conn.Close(websocket.StatusNormalClosure, "done")
		}(client)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		for _, client := range clients {
			if client != nil {
				_ = client.conn.CloseNow()
			}
		}
	}
}

func LoadPinnedProfile(path string) (HostProfile, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return HostProfile{}, err
	}
	if len(raw) > 16<<10 {
		return HostProfile{}, errors.New("pinned profile is too large")
	}
	var profile HostProfile
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&profile); err != nil {
		return HostProfile{}, fmt.Errorf("decode pinned profile: %w", err)
	}
	if profile.CPUModel == "" || profile.CPUCores <= 0 || profile.MemoryBytes == 0 || profile.OS == "" || profile.Arch == "" || profile.GoVersion == "" {
		return HostProfile{}, errors.New("pinned profile is incomplete")
	}
	return profile, nil
}

func VerifyPinnedHost(path string) error {
	profile, err := LoadPinnedProfile(path)
	if err != nil {
		return errors.New("pinned runner preflight failed")
	}
	if !DetectHostProfile().ExactMatch(profile) {
		return errors.New("pinned runner preflight failed")
	}
	return nil
}
