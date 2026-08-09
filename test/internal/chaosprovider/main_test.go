package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/reasoner"
	"github.com/chuanmingliu/openai-gpt-live/internal/reasoner/deepseek"
	"github.com/coder/websocket"
)

func TestVoiceControlsDriveEpochCorrelatedHandoff(t *testing.T) {
	server := httptest.NewServer(voiceHandler())
	t.Cleanup(server.Close)
	postControl(t, server.URL+"/control/handoff?enabled=true")
	postControl(t, server.URL+"/control/burst?frames=2")

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	conn, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/realtime", nil)
	if response != nil && response.Body != nil {
		response.Body.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	readProviderType(t, conn, "session.created")
	request := map[string]any{
		"type":     "response.create",
		"response": map[string]any{"metadata": map[string]any{"live_epoch": "7"}},
	}
	writeProviderEvent(t, conn, request)
	created := readProviderType(t, conn, "response.created")
	var decoded struct {
		Response struct {
			ID       string            `json:"id"`
			Metadata map[string]string `json:"metadata"`
		} `json:"response"`
	}
	if json.Unmarshal(created, &decoded) != nil || decoded.Response.Metadata["live_epoch"] != "7" {
		t.Fatal("response.created did not echo the request epoch")
	}
	if got := countProviderType(t, conn, "response.output_audio.delta", 2); got != 2 {
		t.Fatalf("audio deltas=%d, want 2", got)
	}
	function := readProviderType(t, conn, "response.function_call_arguments.done")
	var tool struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	}
	if json.Unmarshal(function, &tool) != nil || tool.Name != "start_background_task" || !json.Valid([]byte(tool.Arguments)) {
		t.Fatal("handoff control did not emit the bounded start_background_task call")
	}
	readProviderType(t, conn, "response.done")

	writeProviderEvent(t, conn, request)
	var secondResponseID string
	for {
		_, raw, readErr := conn.Read(ctx)
		if readErr != nil {
			t.Fatal(readErr)
		}
		var envelope struct{ Type string }
		if json.Unmarshal(raw, &envelope) != nil {
			continue
		}
		if envelope.Type == "response.function_call_arguments.done" {
			t.Fatal("synthetic provider recursively emitted a handoff for the gateway's acknowledgement response")
		}
		if envelope.Type == "response.created" {
			var second struct {
				Response struct {
					ID string `json:"id"`
				} `json:"response"`
			}
			if json.Unmarshal(raw, &second) != nil || second.Response.ID == "" || second.Response.ID == decoded.Response.ID {
				t.Fatal("synthetic provider reused a response identity across acknowledgement epochs")
			}
			secondResponseID = second.Response.ID
		}
		if envelope.Type == "response.done" {
			if secondResponseID == "" {
				t.Fatal("second response completed without a unique created identity")
			}
			break
		}
	}
}

func TestDeepSeekControlsExposeContentFreeAttemptStats(t *testing.T) {
	server := httptest.NewServer(deepSeekHandler())
	t.Cleanup(server.Close)
	postControl(t, server.URL+"/control/reset")
	postControl(t, server.URL+"/control/limit?enabled=true")
	requestCompletion(t, server.URL, http.StatusTooManyRequests)
	time.Sleep(10 * time.Millisecond)
	requestCompletion(t, server.URL, http.StatusTooManyRequests)

	response, err := http.Get(server.URL + "/control/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var stats struct {
		Requests          int   `json:"requests"`
		MinimumIntervalMS int64 `json:"minimum_interval_ms"`
	}
	if response.StatusCode != http.StatusOK || json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&stats) != nil {
		t.Fatal("stats control is unavailable")
	}
	if stats.Requests != 2 || stats.MinimumIntervalMS <= 0 {
		t.Fatalf("stats=%+v, want two bounded timing-only attempts", stats)
	}

	postControl(t, server.URL+"/control/stall?enabled=true")
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/chat/completions", strings.NewReader(`{}`))
	if _, err := http.DefaultClient.Do(request); err == nil {
		t.Fatal("stalled request completed before its caller cancelled")
	}
	postControl(t, server.URL+"/control/stall?enabled=false")
}

func TestDeepSeekSuccessTraversesTheProductionStreamParser(t *testing.T) {
	server := httptest.NewServer(deepSeekHandler())
	t.Cleanup(server.Close)
	client, err := deepseek.New(deepseek.Config{BaseURL: server.URL + "/v1"}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.Stream(t.Context(), reasoner.Request{
		Model: "fake-deepseek", Messages: []reasoner.Message{{Role: reasoner.RoleUser, Content: "synthetic"}},
		MaxOutputTokens: 16, SafetyUserID: "chaos-user",
	}, func(reasoner.Chunk) error { return nil })
	if err != nil {
		t.Fatal("fake provider success did not satisfy the production parser")
	}
	if result.ProviderRequestID == "" || result.FinishReason != "stop" || result.Text != "bounded synthetic result" {
		t.Fatalf("unexpected content-free terminal metadata: request_id=%t finish=%q", result.ProviderRequestID != "", result.FinishReason)
	}
}

func postControl(t *testing.T, endpoint string) {
	t.Helper()
	request, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint, strings.NewReader(`{}`))
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("control status=%d", response.StatusCode)
	}
}

func requestCompletion(t *testing.T, serverURL string, wanted int) {
	t.Helper()
	request, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, serverURL+"/v1/chat/completions", strings.NewReader(`{}`))
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != wanted {
		t.Fatalf("completion status=%d, want %d", response.StatusCode, wanted)
	}
}

func writeProviderEvent(t *testing.T, conn *websocket.Conn, value any) {
	t.Helper()
	raw, _ := json.Marshal(value)
	if err := conn.Write(t.Context(), websocket.MessageText, raw); err != nil {
		t.Fatal(err)
	}
}

func readProviderType(t *testing.T, conn *websocket.Conn, wanted string) []byte {
	t.Helper()
	for {
		_, raw, err := conn.Read(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		var envelope struct{ Type string }
		if json.Unmarshal(raw, &envelope) == nil && envelope.Type == wanted {
			return raw
		}
	}
}

func countProviderType(t *testing.T, conn *websocket.Conn, wanted string, count int) int {
	t.Helper()
	for found := 0; ; {
		_, raw, err := conn.Read(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		var envelope struct{ Type string }
		if json.Unmarshal(raw, &envelope) == nil && envelope.Type == wanted {
			found++
			if found == count {
				return found
			}
		}
	}
}
