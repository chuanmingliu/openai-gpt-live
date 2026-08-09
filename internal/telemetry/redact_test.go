package telemetry_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/telemetry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const defaultLoggerHelperEnv = "LIVE_TEST_DEFAULT_REDACTING_LOGGER_HELPER"

type secretValue struct{ Value string }

type recursiveValue struct{ Next *recursiveValue }

type hostileLogValuer struct{ sentinel string }

func (v hostileLogValuer) LogValue() slog.Value {
	return slog.GroupValue(slog.String("token", v.sentinel))
}

type recursiveLogValuer struct{ calls *int }

func (v recursiveLogValuer) LogValue() slog.Value {
	*v.calls++
	return slog.AnyValue(v)
}

func TestRedactingHandlerRecursesForTextAndJSON(t *testing.T) {
	for _, jsonOutput := range []bool{false, true} {
		t.Run(map[bool]string{false: "text", true: "json"}[jsonOutput], func(t *testing.T) {
			var output bytes.Buffer
			var next slog.Handler = slog.NewTextHandler(&output, nil)
			if jsonOutput {
				next = slog.NewJSONHandler(&output, nil)
			}
			handler := telemetry.NewRedactingHandler(next, []string{"custom_secret"})
			logger := slog.New(handler.WithGroup("outer").WithAttrs([]slog.Attr{slog.Any("credentials", map[string]any{
				"authorization": "Bearer credential-sentinel", "nested": []any{map[string]any{"database_url": "postgres://db-sentinel"}},
			})}))
			cycle := &recursiveValue{}
			cycle.Next = cycle
			logger.InfoContext(context.Background(), "safe message",
				slog.String("transcript", "transcript-sentinel"),
				slog.String("custom_secret", "custom-sentinel"),
				slog.Any("valuer", hostileLogValuer{sentinel: "valuer-sentinel"}),
				slog.Any("cycle", cycle),
				slog.Any("safe", secretValue{Value: "ordinary"}),
				slog.String("raw_pcm", strings.Repeat("QUJD", 100)),
				slog.Any("tool_arguments", json.RawMessage(`{"private":"tool-sentinel"}`)),
			)
			got := output.String()
			for _, sentinel := range []string{"credential-sentinel", "db-sentinel", "transcript-sentinel", "custom-sentinel", "valuer-sentinel", "tool-sentinel"} {
				assert.NotContains(t, got, sentinel)
			}
			assert.Contains(t, got, "safe message")
			assert.Contains(t, got, telemetry.RedactedValue)
			if jsonOutput {
				var parsed map[string]any
				require.NoError(t, json.Unmarshal(output.Bytes(), &parsed))
			}
		})
	}
}

func TestRedactingHandlerClonesDeniedKeys(t *testing.T) {
	var output bytes.Buffer
	keys := []string{"temporary"}
	handler := telemetry.NewRedactingHandler(slog.NewTextHandler(&output, nil), keys)
	keys[0] = "changed"
	slog.New(handler).Info("event", "temporary", "sentinel")
	assert.NotContains(t, output.String(), "sentinel")
	output.Reset()
	slog.New(handler.WithGroup("token")).Info("event", "otherwise_safe", "group-sentinel")
	assert.NotContains(t, output.String(), "group-sentinel")
}

func TestRedactingHandlerBoundsRecursiveLogValuerResolution(t *testing.T) {
	var output bytes.Buffer
	calls := 0
	logger := slog.New(telemetry.NewRedactingHandler(slog.NewJSONHandler(&output, nil), nil))
	logger.Info("event", "recursive", recursiveLogValuer{calls: &calls})
	assert.LessOrEqual(t, calls, 8)
	assert.Contains(t, output.String(), telemetry.RedactedValue)
}

func TestInstallDefaultRedactingLoggerDoesNotRecurseThroughStandardLog(t *testing.T) {
	if os.Getenv(defaultLoggerHelperEnv) == "1" {
		telemetry.InstallDefaultRedactingLogger(nil)
		slog.Info("safe event", "transcript", "transcript-sentinel")
		log.Print("startup returned error")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestInstallDefaultRedactingLoggerDoesNotRecurseThroughStandardLog$")
	command.Env = append(os.Environ(), defaultLoggerHelperEnv+"=1")
	output, err := command.CombinedOutput()
	require.NoError(t, err, "default logger helper did not return before timeout: %s", output)
	assert.NotContains(t, string(output), "transcript-sentinel")
	assert.Contains(t, string(output), telemetry.RedactedValue)
	assert.Contains(t, string(output), "startup returned error")
}
