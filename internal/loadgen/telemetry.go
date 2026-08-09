package loadgen

import (
	"bufio"
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

const maximumMetricsBytes = 1 << 20

type metricHistogram struct {
	buckets map[float64]uint64
}

type metricSnapshot struct {
	at              time.Time
	transit         metricHistogram
	bargein         metricHistogram
	queueOverflow   float64
	queueObservable bool
}

func captureTelemetry(ctx context.Context, cfg Config) (*metricSnapshot, error) {
	if cfg.MetricsURL == "" {
		return nil, nil
	}
	requestContext, cancel := context.WithTimeout(ctx, cfg.ResponseTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestContext, http.MethodGet, cfg.MetricsURL, nil)
	if err != nil {
		return nil, errors.New("construct metrics request")
	}
	request.Header.Set("Accept", "text/plain")
	if cfg.MetricsBearerToken != "" {
		request.Header.Set("Authorization", "Bearer "+cfg.MetricsBearerToken)
	}
	client := &http.Client{
		Timeout: cfg.ResponseTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("metrics redirects are forbidden")
		},
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, errors.New("scrape gateway metrics")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, errors.New("gateway metrics returned a non-success status")
	}
	return parseMetricSnapshot(io.LimitReader(response.Body, maximumMetricsBytes+1), time.Now().UTC())
}

func parseMetricSnapshot(reader io.Reader, at time.Time) (*metricSnapshot, error) {
	snapshot := &metricSnapshot{
		at: at, transit: metricHistogram{buckets: make(map[float64]uint64)},
		bargein: metricHistogram{buckets: make(map[float64]uint64)},
	}
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64<<10), maximumMetricsBytes)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "# TYPE live_queue_overflows_total ") {
			snapshot.queueObservable = true
			continue
		}
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, labels, value, ok := parseMetricLine(line)
		if !ok {
			continue
		}
		switch name {
		case "live_audio_frame_transit_seconds_bucket":
			if upper, ok := metricUpperBound(labels); ok {
				snapshot.transit.buckets[upper] += uint64(value)
			}
		case "live_bargein_suppression_seconds_bucket":
			if upper, ok := metricUpperBound(labels); ok {
				snapshot.bargein.buckets[upper] += uint64(value)
			}
		case "live_queue_overflows_total":
			snapshot.queueObservable = true
			snapshot.queueOverflow += value
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, errors.New("parse gateway metrics")
	}
	return snapshot, nil
}

func parseMetricLine(line string) (string, string, float64, bool) {
	separator := strings.LastIndexByte(line, ' ')
	if separator <= 0 {
		return "", "", 0, false
	}
	value, err := strconv.ParseFloat(strings.TrimSpace(line[separator+1:]), 64)
	if err != nil || value < 0 {
		return "", "", 0, false
	}
	identity := line[:separator]
	if brace := strings.IndexByte(identity, '{'); brace >= 0 {
		return identity[:brace], identity[brace+1 : len(identity)-1], value, true
	}
	return identity, "", value, true
}

func metricUpperBound(labels string) (float64, bool) {
	for _, field := range strings.Split(labels, ",") {
		parts := strings.SplitN(strings.TrimSpace(field), "=", 2)
		if len(parts) != 2 || parts[0] != "le" {
			continue
		}
		raw, err := strconv.Unquote(parts[1])
		if err != nil {
			return 0, false
		}
		if raw == "+Inf" {
			return math.Inf(1), true
		}
		upper, err := strconv.ParseFloat(raw, 64)
		return upper, err == nil && upper >= 0
	}
	return 0, false
}

func applyScrapedTelemetry(report *Report, baseline, final *metricSnapshot, scrapeErr error) {
	if scrapeErr != nil || baseline == nil || final == nil {
		report.Telemetry.Status = "unavailable"
		return
	}
	report.Telemetry.CollectedAt = final.at
	if histogramRegressed(baseline.transit, final.transit) || histogramRegressed(baseline.bargein, final.bargein) || final.queueOverflow < baseline.queueOverflow {
		report.Telemetry.Status = "stale"
		return
	}
	report.GatewayTransit = summarizeMetricDelta(baseline.transit, final.transit)
	report.GatewayBargein = summarizeMetricDelta(baseline.bargein, final.bargein)
	report.QueueOverflow = int64(final.queueOverflow - baseline.queueOverflow)
	report.Telemetry.TransitSamples = report.GatewayTransit.Count
	report.Telemetry.BargeinSamples = report.GatewayBargein.Count
	report.Telemetry.QueueOverflowObserved = baseline.queueObservable && final.queueObservable
	report.Telemetry.Status = "available"
	if report.Telemetry.TransitSamples == 0 && report.Telemetry.BargeinSamples == 0 {
		report.Telemetry.Status = "stale"
	} else if report.Telemetry.TransitSamples >= 20 && report.Telemetry.QueueOverflowObserved {
		report.Telemetry.Status = "adequate"
	}
}

func histogramRegressed(before, after metricHistogram) bool {
	for upper, count := range before.buckets {
		if after.buckets[upper] < count {
			return true
		}
	}
	return false
}

func summarizeMetricDelta(before, after metricHistogram) LatencySummary {
	bounds := make([]float64, 0, len(after.buckets))
	for upper := range after.buckets {
		bounds = append(bounds, upper)
	}
	sort.Float64s(bounds)
	count := deltaBucket(before, after, math.Inf(1))
	if count == 0 {
		return LatencySummary{}
	}
	percentile := func(percent uint64) Duration {
		rank := (count*percent + 99) / 100
		for _, upper := range bounds {
			if deltaBucket(before, after, upper) >= rank {
				if math.IsInf(upper, 1) {
					return Duration(time.Minute)
				}
				return Duration(time.Duration(upper * float64(time.Second)))
			}
		}
		return Duration(time.Minute)
	}
	maximum := Duration(0)
	var previous uint64
	for _, upper := range bounds {
		cumulative := deltaBucket(before, after, upper)
		if !math.IsInf(upper, 1) && cumulative > previous {
			maximum = Duration(time.Duration(upper * float64(time.Second)))
		}
		previous = cumulative
	}
	return LatencySummary{Count: count, P50: percentile(50), P95: percentile(95), P99: percentile(99), Max: maximum}
}

func deltaBucket(before, after metricHistogram, upper float64) uint64 {
	current := after.buckets[upper]
	previous := before.buckets[upper]
	if current < previous {
		return 0
	}
	return current - previous
}
