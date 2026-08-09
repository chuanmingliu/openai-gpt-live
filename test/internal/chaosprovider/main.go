// Command chaosprovider is a deterministic content-free voice/DeepSeek peer
// for the disposable Compose chaos topology.
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

func main() {
	flags := flag.NewFlagSet("chaosprovider", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	listen := flags.String("listen", ":8000", "listen address")
	controlListen := flags.String("control-listen", ":8081", "plain loopback control address")
	caPath := flags.String("ca-path", "/tmp/chaos-ca.pem", "generated disposable CA path")
	voiceUpstream := flags.String("voice-upstream", "http://toxiproxy:18001", "voice reverse-proxy upstream")
	deepseekUpstream := flags.String("deepseek-upstream", "http://toxiproxy:18002", "DeepSeek reverse-proxy upstream")
	postgresListen := flags.String("postgres-listen", ":15433", "PostgreSQL TLS listen address")
	postgresUpstream := flags.String("postgres-upstream", "toxiproxy:15432", "PostgreSQL plain upstream")
	upstream := flags.String("upstream", "http://127.0.0.1:9090", "plain reverse-proxy upstream")
	issuer := flags.String("issuer", "https://chaos-edge:8443/oidc", "disposable OIDC issuer")
	mode := flags.String("mode", "voice", "voice, deepseek, edge, or toxiproxy-init")
	toxiproxyURL := flags.String("toxiproxy-url", "http://toxiproxy:8474", "disposable Toxiproxy control URL")
	if flags.Parse(os.Args[1:]) != nil || flags.NArg() != 0 || (*mode != "voice" && *mode != "deepseek" && *mode != "edge" && *mode != "proxy" && *mode != "toxiproxy-init") {
		os.Exit(64)
	}
	if *mode == "toxiproxy-init" {
		if initToxiproxy(*toxiproxyURL) != nil {
			os.Exit(1)
		}
		return
	}
	if *mode == "edge" {
		if runEdge(*listen, *controlListen, *postgresListen, *caPath, *voiceUpstream, *deepseekUpstream, *postgresUpstream, *issuer) != nil {
			os.Exit(1)
		}
		return
	}
	if *mode == "proxy" {
		if runPlainProxy(*listen, *upstream) != nil {
			os.Exit(1)
		}
		return
	}
	server := &http.Server{Addr: *listen, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	if *mode == "voice" {
		server.Handler = voiceHandler()
	} else {
		server.Handler = deepSeekHandler()
	}
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		os.Exit(1)
	}
}

func initToxiproxy(controlURL string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 2 * time.Second}
	for {
		request, _ := http.NewRequestWithContext(ctx, http.MethodGet, controlURL+"/proxies", nil)
		response, err := client.Do(request)
		if err == nil {
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	for _, proxy := range []struct{ name, listen, upstream string }{
		{"postgres", "0.0.0.0:15432", "postgres:5432"},
		{"voice", "0.0.0.0:18001", "speech-to-speech:8000"},
		{"deepseek", "0.0.0.0:18002", "fake-deepseek:8001"},
	} {
		raw, _ := json.Marshal(map[string]string{"name": proxy.name, "listen": proxy.listen, "upstream": proxy.upstream})
		request, _ := http.NewRequestWithContext(ctx, http.MethodPost, controlURL+"/proxies", bytes.NewReader(raw))
		request.Header.Set("Content-Type", "application/json")
		response, err := client.Do(request)
		if err != nil {
			return err
		}
		response.Body.Close()
		if response.StatusCode != http.StatusCreated && response.StatusCode != http.StatusOK {
			return errors.New("Toxiproxy initialization failed")
		}
	}
	return nil
}

func voiceHandler() http.Handler {
	var stalled atomic.Bool
	var handoff atomic.Bool
	var burst atomic.Int64
	var burstBytes atomic.Int64
	burst.Store(1)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /control/stall", func(w http.ResponseWriter, r *http.Request) {
		stalled.Store(r.URL.Query().Get("enabled") == "true")
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /control/handoff", func(w http.ResponseWriter, r *http.Request) {
		handoff.Store(r.URL.Query().Get("enabled") == "true")
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /control/burst", func(w http.ResponseWriter, r *http.Request) {
		frames, err := strconv.ParseInt(r.URL.Query().Get("frames"), 10, 64)
		bytes, bytesErr := strconv.ParseInt(r.URL.Query().Get("bytes"), 10, 64)
		if r.URL.Query().Get("bytes") == "" {
			bytes, bytesErr = 0, nil
		}
		if err != nil || frames < 1 || frames > 4096 || bytesErr != nil || bytes < 0 || bytes > 64<<10 {
			http.Error(w, "bounded burst required", http.StatusBadRequest)
			return
		}
		burst.Store(frames)
		burstBytes.Store(bytes)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("GET /v1/realtime", func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		defer conn.CloseNow()
		_ = writeEvent(r.Context(), conn, map[string]any{"type": "session.created", "session": map[string]any{"id": "fake"}})
		var pending string
		handoffEmitted := false
		responseSequence := 0
		for {
			_, raw, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			var event struct {
				Type     string `json:"type"`
				Audio    string `json:"audio"`
				Response struct {
					Metadata map[string]json.RawMessage `json:"metadata"`
				} `json:"response"`
			}
			if json.Unmarshal(raw, &event) != nil {
				continue
			}
			switch event.Type {
			case "input_audio_buffer.append":
				pending = event.Audio
			case "input_audio_buffer.commit":
				if handoff.Load() {
					item := "fake-user-turn"
					if writeEvent(r.Context(), conn, map[string]any{"type": "input_audio_buffer.speech_started", "item_id": item}) != nil ||
						writeEvent(r.Context(), conn, map[string]any{"type": "input_audio_buffer.speech_stopped", "item_id": item}) != nil ||
						writeEvent(r.Context(), conn, map[string]any{"type": "conversation.item.input_audio_transcription.completed", "item_id": item, "transcript": "bounded synthetic request"}) != nil {
						return
					}
				}
			case "response.create":
				if stalled.Load() {
					continue
				}
				epoch := "1"
				if encoded := event.Response.Metadata["live_epoch"]; len(encoded) > 0 {
					if json.Unmarshal(encoded, &epoch) != nil {
						continue
					}
				}
				if pending == "" {
					pending = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
				}
				delta := pending
				if bytes := burstBytes.Load(); bytes > 0 {
					delta = base64.StdEncoding.EncodeToString(make([]byte, int(bytes)))
				}
				responseSequence++
				id := "fake-response-" + strconv.Itoa(responseSequence)
				item := "fake-item"
				if writeEvent(r.Context(), conn, map[string]any{"type": "response.created", "response": map[string]any{"id": id, "status": "in_progress", "metadata": map[string]string{"live_epoch": epoch}}}) != nil {
					return
				}
				for range burst.Load() {
					if writeEvent(r.Context(), conn, map[string]any{"type": "response.output_audio.delta", "response_id": id, "item_id": item, "output_index": 0, "content_index": 0, "delta": delta}) != nil {
						return
					}
				}
				if writeEvent(r.Context(), conn, map[string]any{"type": "response.output_audio.done", "response_id": id, "item_id": item, "output_index": 0, "content_index": 0}) != nil {
					return
				}
				if handoff.Load() && !handoffEmitted {
					if writeEvent(r.Context(), conn, map[string]any{
						"type": "response.function_call_arguments.done", "response_id": id, "item_id": "fake-tool-item",
						"call_id": "fake-call", "name": "start_background_task", "arguments": `{"objective":"bounded synthetic objective"}`,
					}) != nil {
						return
					}
					handoffEmitted = true
				}
				if writeEvent(r.Context(), conn, map[string]any{"type": "response.done", "response": map[string]any{"id": id, "status": "completed"}}) != nil {
					return
				}
			}
		}
	})
	return mux
}

func deepSeekHandler() http.Handler {
	var limited atomic.Bool
	var stalled atomic.Bool
	var requests atomic.Int64
	var lastRequest atomic.Int64
	var minimumInterval atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("POST /control/limit", func(w http.ResponseWriter, r *http.Request) {
		limited.Store(r.URL.Query().Get("enabled") == "true")
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /control/stall", func(w http.ResponseWriter, r *http.Request) {
		stalled.Store(r.URL.Query().Get("enabled") == "true")
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /control/reset", func(w http.ResponseWriter, _ *http.Request) {
		requests.Store(0)
		lastRequest.Store(0)
		minimumInterval.Store(0)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /control/stats", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"requests": requests.Load(), "minimum_interval_ms": minimumInterval.Load() / int64(time.Millisecond),
		})
	})
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("POST /v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		now := time.Now().UnixNano()
		requests.Add(1)
		if previous := lastRequest.Swap(now); previous > 0 {
			interval := now - previous
			for current := minimumInterval.Load(); current == 0 || interval < current; current = minimumInterval.Load() {
				if minimumInterval.CompareAndSwap(current, interval) {
					break
				}
			}
		}
		if stalled.Load() {
			for stalled.Load() {
				select {
				case <-r.Context().Done():
					return
				case <-time.After(10 * time.Millisecond):
				}
			}
		}
		if limited.Load() {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"id\":\"fake-request\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"bounded synthetic result\"},\"finish_reason\":\"stop\"}]}\n\n"+
			"data: {\"id\":\"fake-request\",\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":0,\"total_tokens\":1}}\n\n"+
			"data: [DONE]\n\n")
	})
	return mux
}

func writeEvent(ctx context.Context, conn *websocket.Conn, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	writeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return conn.Write(writeCtx, websocket.MessageText, raw)
}
