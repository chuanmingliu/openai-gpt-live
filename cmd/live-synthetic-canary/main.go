package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/protocol"
	"github.com/coder/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type createdEvent struct {
	Type    string `json:"type"`
	EventID string `json:"event_id"`
	Session struct {
		ID string `json:"id"`
	} `json:"session"`
}

const postTerminalObservation = 500 * time.Millisecond

type runtimeExpectation struct {
	ConfigHash   string
	Region       string
	GatewayImage string
}

type probeObservation struct {
	Runtime                   runtimeExpectation
	ResponseIDCommitments     []string
	PostTerminalObservationMS int
}

type responseObservation struct {
	own          string
	foreign      string
	responseID   string
	ownPresented bool
	terminal     bool
}

func validateIsolatedSessions(left, right []byte) error {
	var a, b createdEvent
	if err := json.Unmarshal(left, &a); err != nil {
		return errors.New("first session response is not bounded JSON")
	}
	if err := json.Unmarshal(right, &b); err != nil {
		return errors.New("second session response is not bounded JSON")
	}
	if a.Type != "session.created" || b.Type != "session.created" || a.EventID == "" || b.EventID == "" {
		return errors.New("canary did not receive two complete session.created events")
	}
	if a.EventID == b.EventID || strings.Contains(string(left), b.EventID) || strings.Contains(string(right), a.EventID) {
		return errors.New("cross-session sentinel identity leakage detected")
	}
	if a.Session.ID != "" && b.Session.ID != "" && a.Session.ID == b.Session.ID {
		return errors.New("cross-session provider identity reuse detected")
	}
	return nil
}

func main() {
	once := flag.Bool("once", false, "run one bounded deployed observation")
	evidenceOut := flag.String("evidence-out", "", "exclusive content-free observation output")
	flag.Parse()
	if flag.NArg() != 0 || (*once && *evidenceOut == "") || (!*once && *evidenceOut != "") {
		panic("valid --once/--evidence-out combination and no positional arguments are required")
	}
	url := os.Getenv("LIVE_CANARY_GATEWAY_URL")
	origin := os.Getenv("LIVE_CANARY_ORIGIN")
	tokenPath := os.Getenv("LIVE_CANARY_BEARER_TOKEN_FILE")
	region := os.Getenv("LIVE_DEPLOYMENT_REGION")
	configHash := os.Getenv("LIVE_EXPECTED_CONFIG_HASH")
	gatewayImage := os.Getenv("LIVE_EXPECTED_GATEWAY_IMAGE")
	if url == "" || origin == "" || tokenPath == "" || region == "" || configHash == "" || gatewayImage == "" || os.Getenv("LIVE_CANARY_RETENTION") != "none" {
		panic("bounded canary URL, origin, token file, region, config hash, gateway image, and retention=none are required")
	}
	expectedRuntime := runtimeExpectation{ConfigHash: configHash, Region: region, GatewayImage: gatewayImage}
	if *once {
		if !strings.HasPrefix(url, "wss://") {
			panic("release observation requires the reviewed production WSS route")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		observation, err := probe(ctx, url, origin, tokenPath, expectedRuntime)
		cancel()
		if err != nil {
			panic(err)
		}
		document, err := marshalCanaryEvidence(observation, time.Now().UTC())
		if err != nil {
			panic("encode canary evidence")
		}
		file, err := os.OpenFile(*evidenceOut, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			panic("exclusively create canary evidence")
		}
		if _, err := file.Write(document); err != nil {
			file.Close()
			panic("write canary evidence")
		}
		if err := file.Sync(); err != nil {
			file.Close()
			panic("sync canary evidence")
		}
		if err := file.Close(); err != nil {
			panic("close canary evidence")
		}
		return
	}
	registry := prometheus.NewRegistry()
	labels := prometheus.Labels{"component": "synthetic-canary", "provider": "gateway", "region": region, "instance": "two-session", "config_hash": configHash}
	leaks := prometheus.NewCounter(prometheus.CounterOpts{Name: "live_synthetic_cross_session_leaks_total", Help: "Detected cross-session sentinel leaks.", ConstLabels: labels})
	lastSuccess := prometheus.NewGauge(prometheus.GaugeOpts{Name: "live_synthetic_canary_last_success_timestamp_seconds", Help: "Unix timestamp of the last isolated two-session success.", ConstLabels: labels})
	registry.MustRegister(leaks, lastSuccess)
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if time.Since(time.Unix(lastSuccessObserved.Load(), 0)) > 2*time.Minute {
			http.Error(w, "stale", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	go func() { _ = http.ListenAndServe(":9090", mux) }()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		_, err := probe(ctx, url, origin, tokenPath, expectedRuntime)
		cancel()
		if err != nil {
			if strings.Contains(err.Error(), "leak") {
				leaks.Inc()
			}
		} else {
			now := float64(time.Now().Unix())
			lastSuccess.Set(now)
			lastSuccessObserved.Store(int64(now))
		}
		<-ticker.C
	}
}

func marshalCanaryEvidence(observation probeObservation, observedAt time.Time) ([]byte, error) {
	document, err := json.Marshal(struct {
		SchemaVersion             string    `json:"schema_version"`
		ArtifactType              string    `json:"artifact_type"`
		Status                    string    `json:"status"`
		Observation               string    `json:"observation"`
		Retention                 string    `json:"retention"`
		Route                     string    `json:"route"`
		Sentinels                 int       `json:"sentinels"`
		Region                    string    `json:"region"`
		ConfigHash                string    `json:"config_hash"`
		GatewayImage              string    `json:"gateway_image"`
		ResponseIDCommitments     []string  `json:"response_id_commitments"`
		PostTerminalObservationMS int       `json:"post_terminal_observation_ms"`
		ObservedAt                time.Time `json:"observed_at"`
	}{"release-artifact-v2", "canary_snapshot", "passed", "isolated-two-session", "none", "wss", 2, observation.Runtime.Region, observation.Runtime.ConfigHash, observation.Runtime.GatewayImage, observation.ResponseIDCommitments, observation.PostTerminalObservationMS, observedAt})
	if err != nil {
		return nil, err
	}
	return append(document, '\n'), nil
}

var lastSuccessObserved atomic.Int64

func probe(ctx context.Context, url, origin, tokenPath string, expectedRuntime runtimeExpectation) (probeObservation, error) {
	token, err := os.ReadFile(tokenPath)
	if err != nil || len(token) == 0 || len(token) > 8192 || bytes.ContainsAny(token, "\x00\r\n") {
		return probeObservation{}, errors.New("read bounded renewable synthetic identity token")
	}
	credential := string(token)
	clear(token)
	header := http.Header{"Authorization": []string{"Bearer " + credential}, "Origin": []string{origin}}
	a, responseA, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: header})
	var headersA http.Header
	if responseA != nil {
		headersA = responseA.Header.Clone()
	}
	if responseA != nil && responseA.Body != nil {
		_ = responseA.Body.Close()
	}
	if err != nil {
		return probeObservation{}, fmt.Errorf("first synthetic session: %w", err)
	}
	defer a.CloseNow()
	b, responseB, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: header})
	var headersB http.Header
	if responseB != nil {
		headersB = responseB.Header.Clone()
	}
	if responseB != nil && responseB.Body != nil {
		_ = responseB.Body.Close()
	}
	if err != nil {
		return probeObservation{}, fmt.Errorf("second synthetic session: %w", err)
	}
	defer b.CloseNow()
	runtimeObservation, err := validateRuntimeHeaders(headersA, headersB, expectedRuntime)
	if err != nil {
		return probeObservation{}, err
	}
	_, left, err := a.Read(ctx)
	if err != nil {
		return probeObservation{}, fmt.Errorf("read first synthetic session: %w", err)
	}
	_, right, err := b.Read(ctx)
	if err != nil {
		return probeObservation{}, fmt.Errorf("read second synthetic session: %w", err)
	}
	if err := validateIsolatedSessions(left, right); err != nil {
		return probeObservation{}, err
	}
	leftSentinel, rightSentinel, err := newSentinels()
	if err != nil {
		return probeObservation{}, err
	}
	if err := sendSentinel(ctx, a, leftSentinel, "left"); err != nil {
		return probeObservation{}, fmt.Errorf("send first non-audio sentinel: %w", err)
	}
	if err := sendSentinel(ctx, b, rightSentinel, "right"); err != nil {
		return probeObservation{}, fmt.Errorf("send second non-audio sentinel: %w", err)
	}
	type result struct {
		responseID string
		err        error
	}
	results := make(chan result, 2)
	go func() {
		responseID, observeErr := observeResponse(ctx, a, leftSentinel, rightSentinel)
		results <- result{responseID, observeErr}
	}()
	go func() {
		responseID, observeErr := observeResponse(ctx, b, rightSentinel, leftSentinel)
		results <- result{responseID, observeErr}
	}()
	responseIDs := make([]string, 0, 2)
	for range 2 {
		result := <-results
		if result.err != nil {
			return probeObservation{}, result.err
		}
		responseIDs = append(responseIDs, result.responseID)
	}
	if responseIDs[0] == responseIDs[1] {
		return probeObservation{}, errors.New("cross-session response identity reuse detected")
	}
	commitments := make([]string, 0, len(responseIDs))
	for _, responseID := range responseIDs {
		digest := sha256.Sum256([]byte(responseID))
		commitments = append(commitments, hex.EncodeToString(digest[:]))
	}
	sort.Strings(commitments)
	return probeObservation{Runtime: runtimeObservation, ResponseIDCommitments: commitments, PostTerminalObservationMS: int(postTerminalObservation / time.Millisecond)}, nil
}

func newSentinels() (string, string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", "", errors.New("generate bounded canary sentinels")
	}
	encoded := hex.EncodeToString(raw)
	return "gpt-live-synthetic-left-" + encoded, "gpt-live-synthetic-right-" + encoded, nil
}

func sentinelMessages(sentinel, side string) ([]byte, []byte, error) {
	if len(sentinel) == 0 || len(sentinel) > 128 || (side != "left" && side != "right") {
		return nil, nil, errors.New("bounded sentinel and side are required")
	}
	suffix := sentinel
	if len(suffix) > 32 {
		suffix = suffix[len(suffix)-32:]
	}
	item, err := json.Marshal(map[string]any{"type": "conversation.item.create", "event_id": "canary-item-event-" + side + "-" + suffix, "item": map[string]any{"id": "canary-item-" + side + "-" + suffix, "type": "message", "role": "user", "content": []map[string]string{{"type": "input_text", "text": sentinel}}}})
	if err != nil {
		return nil, nil, err
	}
	response, err := json.Marshal(map[string]any{"type": "response.create", "event_id": "canary-response-event-" + side + "-" + suffix, "response": map[string]any{"instructions": "Return exactly this synthetic token and nothing else: " + sentinel}})
	if err != nil {
		return nil, nil, err
	}
	return item, response, nil
}

func sendSentinel(ctx context.Context, connection *websocket.Conn, sentinel, side string) error {
	item, response, err := sentinelMessages(sentinel, side)
	if err != nil {
		return err
	}
	if err := connection.Write(ctx, websocket.MessageText, item); err != nil {
		return err
	}
	return connection.Write(ctx, websocket.MessageText, response)
}

func newResponseObservation(own, foreign string) *responseObservation {
	return &responseObservation{own: own, foreign: foreign}
}

func (observation *responseObservation) consume(frame []byte) error {
	if observation == nil || len(frame) == 0 || len(frame) > 64<<10 {
		return errors.New("synthetic response frame is empty or exceeds 64 KiB")
	}
	if bytes.Contains(frame, []byte(observation.foreign)) {
		return errors.New("cross-session sentinel leak detected")
	}
	event, err := protocol.DecodeServer(frame, protocol.Limits{MaxFrameBytes: 64 << 10, MaxJSONDepth: 32, MaxAudioBytes: 64 << 10})
	if err != nil {
		return fmt.Errorf("decode typed synthetic response event: %w", err)
	}
	switch typed := event.(type) {
	case protocol.ResponseCreatedEvent:
		if typed.EventID == "" || len(typed.Response.ID) == 0 || len(typed.Response.ID) > 128 || typed.Response.Status != "in_progress" || (observation.responseID != "" && observation.responseID != typed.Response.ID) {
			return errors.New("synthetic response.created correlation is invalid")
		}
		observation.responseID = typed.Response.ID
	case protocol.ResponseOutputAudioTranscriptDeltaEvent:
		if typed.EventID == "" || observation.responseID == "" || typed.ResponseID != observation.responseID {
			return errors.New("synthetic transcript delta correlation is invalid")
		}
		if strings.Contains(typed.Delta, observation.own) {
			observation.ownPresented = true
		}
	case protocol.ResponseOutputAudioTranscriptDoneEvent:
		if typed.EventID == "" || observation.responseID == "" || typed.ResponseID != observation.responseID {
			return errors.New("synthetic transcript terminal correlation is invalid")
		}
		if strings.Contains(typed.Transcript, observation.own) {
			observation.ownPresented = true
		}
	case protocol.ResponseDoneEvent:
		if typed.EventID == "" || observation.responseID == "" || typed.Response.ID != observation.responseID || typed.Response.Status != "completed" || observation.terminal {
			return errors.New("synthetic response.done correlation is invalid")
		}
		if !observation.ownPresented {
			return errors.New("synthetic terminal response did not present its own sentinel")
		}
		observation.terminal = true
	}
	return nil
}

func (observation *responseObservation) complete() bool {
	return observation != nil && observation.responseID != "" && observation.ownPresented && observation.terminal
}

func observeResponse(ctx context.Context, connection *websocket.Conn, own, foreign string) (string, error) {
	observation := newResponseObservation(own, foreign)
	for frameCount := 0; frameCount < 64 && !observation.complete(); frameCount++ {
		_, frame, err := connection.Read(ctx)
		if err != nil {
			return "", fmt.Errorf("observe synthetic response: %w", err)
		}
		if err := observation.consume(frame); err != nil {
			return "", err
		}
	}
	if !observation.complete() {
		return "", errors.New("synthetic response did not reach a matching terminal event within 64 frames")
	}
	postCtx, cancel := context.WithTimeout(ctx, postTerminalObservation)
	defer cancel()
	for frameCount := 0; frameCount < 8; frameCount++ {
		_, frame, err := connection.Read(postCtx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(postCtx.Err(), context.DeadlineExceeded) {
				return observation.responseID, nil
			}
			return "", fmt.Errorf("observe post-terminal synthetic response window: %w", err)
		}
		if err := observation.consume(frame); err != nil {
			return "", err
		}
	}
	return "", errors.New("post-terminal synthetic response frame limit exceeded")
}

func validateRuntimeHeaders(left, right http.Header, expected runtimeExpectation) (runtimeExpectation, error) {
	read := func(header http.Header, name string) (string, error) {
		values := header.Values(name)
		if len(values) != 1 || values[0] == "" || len(values[0]) > 256 || strings.ContainsAny(values[0], "\x00\r\n") {
			return "", fmt.Errorf("synthetic session runtime header %s is missing or noncanonical", name)
		}
		return values[0], nil
	}
	observed := runtimeExpectation{}
	for name, target := range map[string]*string{"X-Live-Config-Hash": &observed.ConfigHash, "X-Live-Region": &observed.Region, "X-Live-Gateway-Image": &observed.GatewayImage} {
		leftValue, err := read(left, name)
		if err != nil {
			return runtimeExpectation{}, err
		}
		rightValue, err := read(right, name)
		if err != nil {
			return runtimeExpectation{}, err
		}
		if leftValue != rightValue {
			return runtimeExpectation{}, fmt.Errorf("synthetic sessions observed different %s values", name)
		}
		*target = leftValue
	}
	if observed != expected {
		return runtimeExpectation{}, errors.New("authenticated gateway runtime identity differs from signed release inputs")
	}
	return observed, nil
}

func framesContain(frames [][]byte, sentinel string) bool {
	for _, frame := range frames {
		if bytes.Contains(frame, []byte(sentinel)) {
			return true
		}
	}
	return false
}

func validateSentinelFrames(leftFrames, rightFrames [][]byte, left, right string) error {
	if left == "" || right == "" || left == right || len(left) > 128 || len(right) > 128 {
		return errors.New("canary sentinels are invalid")
	}
	if framesContain(leftFrames, right) || framesContain(rightFrames, left) {
		return errors.New("cross-session sentinel leakage detected")
	}
	if !framesContain(leftFrames, left) || !framesContain(rightFrames, right) {
		return errors.New("both synthetic sessions must present their own sentinel")
	}
	return nil
}
