//go:build chaos

package chaos_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

const (
	chaosTimeout = 15 * time.Second
	chaosOrigin  = "http://127.0.0.1:3000"
)

type chaosConfig struct {
	controlURL, postgresProxy, voiceProxy, deepseekProxy string
	voiceControl, deepseekControl                        string
	gatewayURL, gatewayHealth, workerHealth, bearer      string
	oidcControl                                          string
	composeProject, composeBase, composeOverride         string
}

func requireChaos(t *testing.T) chaosConfig {
	t.Helper()
	names := []string{
		"CHAOS_TOXIPROXY_URL", "CHAOS_POSTGRES_PROXY", "CHAOS_VOICE_PROXY", "CHAOS_DEEPSEEK_PROXY",
		"CHAOS_VOICE_CONTROL_URL", "CHAOS_DEEPSEEK_CONTROL_URL", "CHAOS_GATEWAY_URL",
		"CHAOS_GATEWAY_HEALTH_URL", "CHAOS_WORKER_HEALTH_URL", "CHAOS_BEARER_TOKEN",
		"CHAOS_COMPOSE_PROJECT", "CHAOS_COMPOSE_BASE", "CHAOS_COMPOSE_OVERRIDE",
		"CHAOS_DATABASE_URL", "CHAOS_OIDC_CONTROL_URL",
	}
	values := make([]string, len(names))
	for index, name := range names {
		values[index] = os.Getenv(name)
		if values[index] == "" {
			t.Skip("complete explicit application-chaos environment is required before any connection")
		}
	}
	cfg := chaosConfig{
		controlURL: strings.TrimRight(values[0], "/"), postgresProxy: values[1], voiceProxy: values[2], deepseekProxy: values[3],
		voiceControl: strings.TrimRight(values[4], "/"), deepseekControl: strings.TrimRight(values[5], "/"), gatewayURL: values[6],
		gatewayHealth: strings.TrimRight(values[7], "/"), workerHealth: strings.TrimRight(values[8], "/"), bearer: values[9],
		composeProject: values[10], composeBase: values[11], composeOverride: values[12], oidcControl: strings.TrimRight(values[14], "/"),
	}
	cfg.bearer = fetchOIDCToken(t, cfg.oidcControl+"/token")
	for _, path := range []string{cfg.composeBase, cfg.composeOverride} {
		absolute, err := filepath.Abs(path)
		if err != nil {
			t.Fatal("chaos Compose path is invalid")
		}
		if _, err := os.Stat(absolute); err != nil {
			t.Fatal("chaos Compose file is unavailable")
		}
	}
	if status := getStatus(t, cfg.controlURL+"/proxies", time.Second); status != http.StatusOK {
		t.Fatal("configured chaos control endpoint is unavailable")
	}
	requireEventuallyStatus(t, cfg.gatewayHealth+"/health/ready", http.StatusOK)
	requireEventuallyStatus(t, cfg.workerHealth+"/health/ready", http.StatusOK)
	return cfg
}

func fetchOIDCToken(t *testing.T, endpoint string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal("disposable OIDC token control is unavailable")
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 4097))
	token := strings.TrimSpace(string(raw))
	if err != nil || response.StatusCode != http.StatusOK || len(raw) > 4096 || strings.Count(token, ".") != 2 {
		t.Fatal("disposable OIDC token control returned an invalid bounded token")
	}
	return token
}

type toxic struct {
	proxy, name string
}

func addToxic(t *testing.T, cfg chaosConfig, proxy, name, kind, stream string, attributes map[string]any) toxic {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"name": name, "type": kind, "stream": stream, "toxicity": 1, "attributes": attributes})
	status := requestStatus(t, http.MethodPost, cfg.controlURL+"/proxies/"+url.PathEscape(proxy)+"/toxics", body, chaosTimeout)
	if status != http.StatusOK {
		t.Fatal("fault injection failed after application preflight")
	}
	created := toxic{proxy: proxy, name: name}
	t.Cleanup(func() { removeToxic(t, cfg, created) })
	return created
}

func removeToxic(t *testing.T, cfg chaosConfig, value toxic) {
	t.Helper()
	status := requestStatus(t, http.MethodDelete, cfg.controlURL+"/proxies/"+url.PathEscape(value.proxy)+"/toxics/"+url.PathEscape(value.name), nil, chaosTimeout)
	if status != http.StatusNoContent && status != http.StatusNotFound {
		t.Errorf("toxic cleanup status=%d", status)
	}
}

func TestPostgreSQLDisconnect(t *testing.T) {
	cfg := requireChaos(t)
	healthy := connectGateway(t, cfg)
	healthy.CloseNow()
	fault := addToxic(t, cfg, cfg.postgresProxy, "task24-postgres-reset", "reset_peer", "downstream", map[string]any{"timeout": 0})

	if gatewaySessionStarts(t, cfg, 2*time.Second) {
		t.Fatal("gateway admitted a new persisted session while its PostgreSQL path was disconnected")
	}
	removeToxic(t, cfg, fault)
	restartWorker(t, cfg)
	requireEventuallyStatus(t, cfg.gatewayHealth+"/health/ready", http.StatusOK)
	requireEventuallyStatus(t, cfg.workerHealth+"/health/ready", http.StatusOK)
	recovered := connectGateway(t, cfg)
	recovered.CloseNow()
}

func TestDeepSeek429HonorsBoundedRetryAfter(t *testing.T) {
	cfg := requireChaos(t)
	setControl(t, cfg.deepseekControl+"/control/reset")
	setControl(t, cfg.deepseekControl+"/control/limit?enabled=true")
	t.Cleanup(func() { setControl(t, cfg.deepseekControl+"/control/limit?enabled=false") })

	client, _ := beginGatewayHandoff(t, cfg)
	defer client.CloseNow()
	stats := waitProviderRequests(t, cfg.deepseekControl, 2)
	if stats.MinimumIntervalMS < 900 || stats.MinimumIntervalMS > 5_000 {
		t.Fatalf("worker Retry-After interval=%dms, want bounded 1s..5s", stats.MinimumIntervalMS)
	}
	setControl(t, cfg.deepseekControl+"/control/limit?enabled=false")
	readGatewayType(t, client, "live.handoff.completed", chaosTimeout)
}

func TestSlowClientBackpressure(t *testing.T) {
	cfg := requireChaos(t)
	setControl(t, cfg.voiceControl+"/control/burst?frames=4096&bytes=65536")
	t.Cleanup(func() { setControl(t, cfg.voiceControl+"/control/burst?frames=1&bytes=0") })
	client := connectGateway(t, cfg)
	defer client.CloseNow()
	writeGateway(t, client, map[string]any{"type": "input_audio_buffer.append", "audio": strings.Repeat("A", 1280)})
	writeGateway(t, client, map[string]any{"type": "response.create"})

	deadline := time.Now().Add(chaosTimeout)
	for time.Now().Before(deadline) {
		if metricValue(t, cfg.gatewayHealth+"/metrics", "live_queue_overflows_total") > 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("real gateway writer path did not report bounded slow-client backpressure")
}

func TestStalledVoiceProvider(t *testing.T) {
	cfg := requireChaos(t)
	setControl(t, cfg.voiceControl+"/control/stall?enabled=true")
	t.Cleanup(func() { setControl(t, cfg.voiceControl+"/control/stall?enabled=false") })
	client := connectGateway(t, cfg)
	writeGateway(t, client, map[string]any{"type": "response.create"})
	readCtx, cancelRead := context.WithTimeout(t.Context(), 500*time.Millisecond)
	_, _, err := client.Read(readCtx)
	cancelRead()
	client.CloseNow()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("gateway voice path did not expose the provider stall deadline: %v", err)
	}
	setControl(t, cfg.voiceControl+"/control/stall?enabled=false")
	recovered := connectGateway(t, cfg)
	defer recovered.CloseNow()
	writeGateway(t, recovered, map[string]any{"type": "response.create"})
	readGatewayType(t, recovered, "response.done", chaosTimeout)
}

func TestKilledWorkerLeaseRecovery(t *testing.T) {
	cfg := requireChaos(t)
	setControl(t, cfg.deepseekControl+"/control/reset")
	setControl(t, cfg.deepseekControl+"/control/stall?enabled=true")
	t.Cleanup(func() { setControl(t, cfg.deepseekControl+"/control/stall?enabled=false") })
	client, _ := beginGatewayHandoff(t, cfg)
	defer client.CloseNow()
	waitProviderRequests(t, cfg.deepseekControl, 1)
	compose(t, cfg, "kill", "worker")
	t.Cleanup(func() { restartWorker(t, cfg) })
	time.Sleep(3 * time.Second)
	setControl(t, cfg.deepseekControl+"/control/stall?enabled=false")
	restartWorker(t, cfg)
	requireEventuallyStatus(t, cfg.workerHealth+"/health/ready", http.StatusOK)
	readGatewayType(t, client, "live.handoff.completed", chaosTimeout)
	if stats := waitProviderRequests(t, cfg.deepseekControl, 2); stats.Requests < 2 {
		t.Fatal("replacement worker did not reacquire the expired durable lease")
	}
}

func TestLostNotificationPollRepair(t *testing.T) {
	cfg := requireChaos(t)
	compose(t, cfg, "stop", "worker")
	t.Cleanup(func() { restartWorker(t, cfg) })
	client, _ := beginGatewayHandoff(t, cfg)
	defer client.CloseNow()
	restartWorker(t, cfg)
	requireEventuallyStatus(t, cfg.workerHealth+"/health/ready", http.StatusOK)
	readGatewayType(t, client, "live.handoff.completed", chaosTimeout)
}

type providerStats struct {
	Requests          int   `json:"requests"`
	MinimumIntervalMS int64 `json:"minimum_interval_ms"`
}

func waitProviderRequests(t *testing.T, control string, minimum int) providerStats {
	t.Helper()
	deadline := time.Now().Add(chaosTimeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		request, _ := http.NewRequestWithContext(ctx, http.MethodGet, control+"/control/stats", nil)
		response, err := http.DefaultClient.Do(request)
		if err == nil && response.StatusCode == http.StatusOK {
			var stats providerStats
			decodeErr := json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&stats)
			response.Body.Close()
			cancel()
			if decodeErr == nil && stats.Requests >= minimum {
				return stats
			}
		} else {
			if response != nil {
				response.Body.Close()
			}
			cancel()
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("worker did not traverse the configured DeepSeek provider path")
	return providerStats{}
}

func beginGatewayHandoff(t *testing.T, cfg chaosConfig) (*websocket.Conn, string) {
	t.Helper()
	setControl(t, cfg.voiceControl+"/control/handoff?enabled=true")
	t.Cleanup(func() { setControl(t, cfg.voiceControl+"/control/handoff?enabled=false") })
	client := connectGateway(t, cfg)
	writeGateway(t, client, map[string]any{"type": "input_audio_buffer.append", "audio": strings.Repeat("A", 1280)})
	writeGateway(t, client, map[string]any{"type": "input_audio_buffer.commit"})
	readGatewayType(t, client, "conversation.item.input_audio_transcription.completed", chaosTimeout)
	writeGateway(t, client, map[string]any{"type": "response.create"})
	raw := readGatewayType(t, client, "live.handoff.created", chaosTimeout)
	var event struct {
		HandoffID string `json:"handoff_id"`
	}
	if json.Unmarshal(raw, &event) != nil || event.HandoffID == "" {
		client.CloseNow()
		t.Fatal("gateway did not persist a durable handoff")
	}
	return client, event.HandoffID
}

func connectGateway(t *testing.T, cfg chaosConfig) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), chaosTimeout)
	defer cancel()
	header := make(http.Header)
	header.Set("Authorization", "Bearer "+cfg.bearer)
	header.Set("Origin", chaosOrigin)
	header.Set("X-Forwarded-Proto", "https")
	client, response, err := websocket.Dial(ctx, cfg.gatewayURL, &websocket.DialOptions{HTTPHeader: header})
	if response != nil && response.Body != nil {
		response.Body.Close()
	}
	if err != nil {
		t.Fatal("gateway application WebSocket was unavailable")
	}
	readGatewayType(t, client, "session.created", chaosTimeout)
	return client
}

func gatewaySessionStarts(t *testing.T, cfg chaosConfig, timeout time.Duration) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	header := make(http.Header)
	header.Set("Authorization", "Bearer "+cfg.bearer)
	header.Set("Origin", chaosOrigin)
	header.Set("X-Forwarded-Proto", "https")
	client, response, err := websocket.Dial(ctx, cfg.gatewayURL, &websocket.DialOptions{HTTPHeader: header})
	if response != nil && response.Body != nil {
		response.Body.Close()
	}
	if err != nil {
		return false
	}
	defer client.CloseNow()
	_, raw, err := client.Read(ctx)
	if err != nil {
		return false
	}
	var envelope struct{ Type string }
	return json.Unmarshal(raw, &envelope) == nil && envelope.Type == "session.created"
}

func writeGateway(t *testing.T, client *websocket.Conn, event any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), chaosTimeout)
	defer cancel()
	raw, err := json.Marshal(event)
	if err != nil || client.Write(ctx, websocket.MessageText, raw) != nil {
		t.Fatal("gateway application event write failed")
	}
}

func readGatewayType(t *testing.T, client *websocket.Conn, wanted string, timeout time.Duration) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	for {
		_, raw, err := client.Read(ctx)
		if err != nil {
			t.Fatalf("gateway application event %s was not observed: %v", wanted, err)
		}
		var envelope struct{ Type string }
		if json.Unmarshal(raw, &envelope) == nil && envelope.Type == wanted {
			return raw
		}
	}
}

func compose(t *testing.T, cfg chaosConfig, arguments ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), chaosTimeout)
	defer cancel()
	base := []string{"compose", "-p", cfg.composeProject, "-f", cfg.composeBase, "-f", cfg.composeOverride}
	command := exec.CommandContext(ctx, "docker", append(base, arguments...)...)
	command.Stdout, command.Stderr = io.Discard, io.Discard
	if err := command.Run(); err != nil {
		t.Fatal("disposable chaos worker lifecycle command failed")
	}
}

func restartWorker(t *testing.T, cfg chaosConfig) {
	t.Helper()
	compose(t, cfg, "up", "-d", "--no-deps", "--no-recreate", "worker")
	// The admin proxy joins the worker's network namespace. Restart it after
	// the worker so health checks cannot remain attached to a stale namespace.
	compose(t, cfg, "restart", "worker-admin-proxy")
}

func setControl(t *testing.T, endpoint string) {
	t.Helper()
	if status := requestStatus(t, http.MethodPost, endpoint, []byte("{}"), chaosTimeout); status != http.StatusNoContent {
		t.Fatal("provider fault control failed")
	}
}

func requireEventuallyStatus(t *testing.T, endpoint string, wanted int) {
	t.Helper()
	deadline := time.Now().Add(chaosTimeout)
	for time.Now().Before(deadline) {
		if getStatus(t, endpoint, time.Second) == wanted {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("application readiness did not recover within its deadline")
}

func getStatus(t *testing.T, endpoint string, timeout time.Duration) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	return response.StatusCode
}

func requestStatus(t *testing.T, method, endpoint string, body []byte, timeout time.Duration) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal("chaos control request failed")
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	return response.StatusCode
}

func metricValue(t *testing.T, endpoint, metric string) float64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0
	}
	defer response.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	var total float64
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(line, metric) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) >= 2 {
			value, _ := strconv.ParseFloat(fields[len(fields)-1], 64)
			total += value
		}
	}
	return total
}
