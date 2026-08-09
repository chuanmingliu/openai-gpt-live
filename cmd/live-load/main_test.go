package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/chuanmingliu/openai-gpt-live/internal/loadgen"
)

func TestExecuteRejectsMissingExplicitURLContentFree(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := execute(context.Background(), []string{"capacity"}, func(string) (string, bool) { return "", false }, &stdout, &stderr)
	if code != exitConfig || stdout.Len() != 0 || stderr.String() != "load configuration failed\n" {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestExecuteNeverPrintsBearerOnConfigurationFailure(t *testing.T) {
	const secret = "task24-private-value"
	var stdout, stderr bytes.Buffer
	code := execute(context.Background(), []string{"capacity", "-gateway-url", "wss://public.example.com/v1/realtime", "-duration", "1s", "-bearer-token-env", "LOAD_TOKEN"}, func(name string) (string, bool) {
		return secret, name == "LOAD_TOKEN"
	}, &stdout, &stderr)
	if code != exitConfig || strings.Contains(stdout.String()+stderr.String(), secret) {
		t.Fatalf("code=%d output contains credential", code)
	}
}

func TestSoakEvidenceRejectsNonTwoHourDuration(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := execute(context.Background(), []string{"soak", "-gateway-url", "ws://127.0.0.1:1/v1/realtime", "-duration", "1m"}, func(string) (string, bool) { return "", false }, &stdout, &stderr)
	if code != exitConfig || stderr.String() != "load configuration failed\n" {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
}

func TestShortSoakRequiresExplicitSmokeEvidenceClass(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var stdout, stderr bytes.Buffer
	code := execute(ctx, []string{"soak", "-gateway-url", "ws://127.0.0.1:1/v1/realtime", "-duration", "1m", "-soak-sample-interval", "10s", "-evidence-class", "smoke"}, func(string) (string, bool) { return "", false }, &stdout, &stderr)
	if code == exitConfig {
		t.Fatalf("explicit smoke configuration rejected: stderr=%q", stderr.String())
	}
}

func TestTelemetryEvidenceFailureUsesSLOExitClass(t *testing.T) {
	report := loadgen.Report{Failures: []loadgen.Failure{{Code: loadgen.FailureTelemetry, Count: 1}}}
	if !hasSLOFailure(report) {
		t.Fatal("inadequate metrics evidence was not classified as an SLO failure")
	}
}
