package telemetry_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/chuanmingliu/openai-gpt-live/internal/telemetry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
