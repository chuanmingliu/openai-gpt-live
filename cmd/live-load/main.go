// Command live-load runs the content-free deterministic capacity and soak
// clients. It prints only the bounded JSON report on stdout and fixed status
// messages on stderr.
package main

import (
	"context"
	"flag"
	"io"
	"os"
	"regexp"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/loadgen"
)

const (
	exitOK          = 0
	exitRuntime     = 1
	exitCorrectness = 2
	exitSLO         = 3
	exitConfig      = 64
)

var envNamePattern = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,127}$`)

func main() {
	code := execute(context.Background(), os.Args[1:], os.LookupEnv, os.Stdout, os.Stderr)
	os.Exit(code)
}

func execute(ctx context.Context, args []string, lookup func(string) (string, bool), stdout, stderr io.Writer) int {
	if len(args) == 0 || (args[0] != string(loadgen.ScenarioCapacity) && args[0] != string(loadgen.ScenarioSoak)) {
		_, _ = io.WriteString(stderr, "load configuration failed\n")
		return exitConfig
	}
	scenario := loadgen.Scenario(args[0])
	flags := flag.NewFlagSet("live-load", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	gatewayURL := flags.String("gateway-url", "", "explicit local/private gateway WebSocket URL")
	metricsURL := flags.String("metrics-url", "", "explicit local/private gateway metrics URL")
	sessions := flags.Int("sessions", 250, "simultaneous held sessions")
	concurrency := flags.Int("connection-concurrency", 64, "fixed dial concurrency")
	duration := flags.Duration("duration", 0, "bounded run duration")
	interval := flags.Duration("frame-interval", time.Second, "deterministic frame interval")
	dialTimeout := flags.Duration("dial-timeout", 5*time.Second, "per-dial deadline")
	responseTimeout := flags.Duration("response-timeout", 2*time.Second, "final response drain deadline")
	teardownTimeout := flags.Duration("teardown-timeout", 5*time.Second, "socket teardown deadline")
	bearerEnv := flags.String("bearer-token-env", "", "environment name containing the test bearer")
	metricsBearerEnv := flags.String("metrics-bearer-token-env", "", "environment name containing the metrics bearer")
	defaultEvidence := string(loadgen.EvidenceCapacity10)
	if scenario == loadgen.ScenarioSoak {
		defaultEvidence = string(loadgen.EvidenceSoak2H)
	}
	evidenceClass := flags.String("evidence-class", defaultEvidence, "smoke, capacity_10m, or soak_2h")
	var allowHosts []string
	flags.Func("allow-host", "explicit test-only host allowlist entry (repeatable)", func(value string) error {
		if value == "" {
			return flag.ErrHelp
		}
		allowHosts = append(allowHosts, value)
		return nil
	})
	pinnedProfilePath := flags.String("pinned-profile", "", "checked-in exact runner profile")
	enforceSLO := flags.Bool("enforce-slo", false, "enforce latency only on an exact pinned profile")
	soakSampleInterval := flags.Duration("soak-sample-interval", 10*time.Minute, "heap sample interval")
	if err := flags.Parse(args[1:]); err != nil || flags.NArg() != 0 || *gatewayURL == "" {
		_, _ = io.WriteString(stderr, "load configuration failed\n")
		return exitConfig
	}
	if *duration == 0 {
		if scenario == loadgen.ScenarioCapacity {
			*duration = 10 * time.Minute
		} else {
			*duration = 2 * time.Hour
		}
	}
	cfg := loadgen.Config{
		GatewayURL: *gatewayURL, MetricsURL: *metricsURL, AllowHosts: allowHosts, Scenario: scenario, EvidenceClass: loadgen.EvidenceClass(*evidenceClass), Sessions: *sessions, ConnectionConcurrency: *concurrency,
		Duration: *duration, FrameInterval: *interval, DialTimeout: *dialTimeout, ResponseTimeout: *responseTimeout,
		TeardownTimeout: *teardownTimeout, EnforceSLO: *enforceSLO, SoakSampleInterval: *soakSampleInterval,
	}
	if *bearerEnv != "" {
		if !envNamePattern.MatchString(*bearerEnv) {
			_, _ = io.WriteString(stderr, "load configuration failed\n")
			return exitConfig
		}
		value, ok := lookup(*bearerEnv)
		if !ok || value == "" {
			_, _ = io.WriteString(stderr, "load configuration failed\n")
			return exitConfig
		}
		cfg.BearerToken = value
	}
	if *metricsBearerEnv != "" {
		if !envNamePattern.MatchString(*metricsBearerEnv) {
			_, _ = io.WriteString(stderr, "load configuration failed\n")
			return exitConfig
		}
		value, ok := lookup(*metricsBearerEnv)
		if !ok || value == "" {
			_, _ = io.WriteString(stderr, "load configuration failed\n")
			return exitConfig
		}
		cfg.MetricsBearerToken = value
	}
	if *pinnedProfilePath != "" {
		profile, err := loadgen.LoadPinnedProfile(*pinnedProfilePath)
		if err != nil {
			_, _ = io.WriteString(stderr, "load configuration failed\n")
			return exitConfig
		}
		cfg.PinnedProfile = &profile
	}
	report, runErr := loadgen.Run(ctx, cfg)
	raw, marshalErr := loadgen.MarshalReport(report)
	if marshalErr != nil {
		_, _ = io.WriteString(stderr, "load report failed\n")
		return exitRuntime
	}
	if _, err := stdout.Write(raw); err != nil {
		_, _ = io.WriteString(stderr, "load report failed\n")
		return exitRuntime
	}
	if runErr == nil {
		return exitOK
	}
	if hasFailure(report, loadgen.FailureConfiguration) {
		_, _ = io.WriteString(stderr, "load configuration failed\n")
		return exitConfig
	}
	if hasSLOFailure(report) {
		_, _ = io.WriteString(stderr, "load SLO gate failed\n")
		return exitSLO
	}
	if len(report.Failures) != 0 {
		_, _ = io.WriteString(stderr, "load correctness gate failed\n")
		return exitCorrectness
	}
	_, _ = io.WriteString(stderr, "load run failed\n")
	return exitRuntime
}

func hasFailure(report loadgen.Report, code loadgen.FailureCode) bool {
	for _, failure := range report.Failures {
		if failure.Code == code {
			return true
		}
	}
	return false
}

func hasSLOFailure(report loadgen.Report) bool {
	return hasFailure(report, loadgen.FailureSLOAudioP95) || hasFailure(report, loadgen.FailureSLOAudioP99) ||
		hasFailure(report, loadgen.FailureSLOBargeP95) || hasFailure(report, loadgen.FailureTelemetry)
}
