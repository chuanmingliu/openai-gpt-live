//go:build contract

package contract_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime/s2sws"
	"github.com/chuanmingliu/openai-gpt-live/internal/testkit"
	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	contractOperationTimeout = 20 * time.Second
	contractFlowTimeout      = 90 * time.Second
	contractFrameLimit       = 1 << 20
)

type contractSocket struct{ conn *websocket.Conn }

type admissionResult struct {
	conn      *websocket.Conn
	handshake bool
	err       error
}

func collectAdmissions(count int, results <-chan admissionResult) ([]*websocket.Conn, error) {
	connections := make([]*websocket.Conn, 0, count)
	failed := false
	for range count {
		result := <-results
		if result.err != nil {
			failed = true
			continue
		}
		connections = append(connections, result.conn)
	}
	if !failed {
		return connections, nil
	}
	for _, conn := range connections {
		_ = conn.CloseNow()
	}
	return nil, errors.New("simulator did not admit the declared sessions simultaneously")
}

func endpoint(t *testing.T) string {
	t.Helper()
	value := os.Getenv("S2S_REALTIME_URL")
	if value == "" {
		t.Skip("S2S_REALTIME_URL is not configured; simulator contract suite is opt-in")
	}
	return value
}

func dialContract(t *testing.T) contractSocket {
	t.Helper()
	conn := awaitReadySocket(t, endpoint(t))
	conn.SetReadLimit(contractFrameLimit)
	t.Cleanup(func() { _ = conn.CloseNow() })
	return contractSocket{conn: conn}
}

func awaitReadySocket(t *testing.T, url string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), contractOperationTimeout)
	defer cancel()
	conn, endpointSelected, err := waitReadySocket(ctx, url)
	if err != nil {
		if maySkipDial(endpointSelected, err) {
			t.Skip("configured simulator endpoint is unreachable")
		}
		t.Fatal("configured simulator failed after selection or WebSocket handshake")
	}
	return conn
}

func waitReadySocket(ctx context.Context, url string) (*websocket.Conn, bool, error) {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	everHandshook := false
	for {
		conn, handshake, event, err := dialInitial(ctx, url)
		everHandshook = everHandshook || handshake
		if err != nil {
			return nil, everHandshook, err
		}
		if event.Type == "session.created" {
			return conn, true, nil
		}
		if event.Type != "error" || event.errorClass() != "session_limit_reached" {
			_ = conn.CloseNow()
			return nil, true, errors.New("configured simulator emitted an invalid readiness event")
		}
		_ = conn.CloseNow()
		select {
		case <-ctx.Done():
			return nil, true, ctx.Err()
		case <-ticker.C:
		}
	}
}

func dialInitial(ctx context.Context, url string) (*websocket.Conn, bool, contractEnvelope, error) {
	conn, response, err := websocket.Dial(ctx, url, nil)
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	if err != nil {
		return nil, false, contractEnvelope{}, err
	}
	conn.SetReadLimit(contractFrameLimit)
	kind, raw, err := conn.Read(ctx)
	if err != nil {
		_ = conn.CloseNow()
		return nil, true, contractEnvelope{}, err
	}
	if kind != websocket.MessageText || len(raw) > contractFrameLimit {
		_ = conn.CloseNow()
		return nil, true, contractEnvelope{}, errors.New("simulator initial frame class is invalid")
	}
	var event contractEnvelope
	if json.Unmarshal(raw, &event) != nil || event.Type == "" {
		_ = conn.CloseNow()
		return nil, true, contractEnvelope{}, errors.New("simulator initial event class is invalid")
	}
	return conn, true, event, nil
}

func dialReady(ctx context.Context, url string) (*websocket.Conn, bool, error) {
	conn, handshake, event, err := dialInitial(ctx, url)
	if err != nil {
		return nil, handshake, err
	}
	if event.Type != "session.created" {
		_ = conn.CloseNow()
		return nil, true, errors.New("simulator did not admit a ready session")
	}
	return conn, true, nil
}

func maySkipDial(handshake bool, err error) bool {
	if handshake || err == nil {
		return false
	}
	var network net.Error
	return errors.As(err, &network) || errors.Is(err, context.DeadlineExceeded)
}

func (s contractSocket) write(t *testing.T, event any) {
	t.Helper()
	raw, err := json.Marshal(event)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), contractOperationTimeout)
	defer cancel()
	if err := s.conn.Write(ctx, websocket.MessageText, raw); err != nil {
		t.Fatal("simulator event write failed")
	}
}

type contractEnvelope struct {
	Type  string `json:"type"`
	Error struct {
		Code string `json:"code"`
		Type string `json:"type"`
	} `json:"error"`
}

func (s contractSocket) readUntil(t *testing.T, timeout time.Duration, wanted ...string) (contractEnvelope, json.RawMessage) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	accepted := make(map[string]struct{}, len(wanted))
	for _, name := range wanted {
		accepted[name] = struct{}{}
	}
	for {
		kind, raw, err := s.conn.Read(ctx)
		if err != nil {
			t.Fatalf("simulator receive failed before expected event class (expected_count=%d)", len(accepted))
		}
		if kind != websocket.MessageText || len(raw) > contractFrameLimit {
			t.Fatal("simulator emitted an invalid frame class")
		}
		var envelope contractEnvelope
		if json.Unmarshal(raw, &envelope) != nil || envelope.Type == "" {
			t.Fatal("simulator emitted an invalid event envelope")
		}
		if envelope.Type == "error" {
			if _, wantedError := accepted[envelope.Type]; !wantedError {
				t.Fatalf("simulator returned protocol error (code=%q)", envelope.errorClass())
			}
		}
		if _, ok := accepted[envelope.Type]; ok {
			return envelope, append(json.RawMessage(nil), raw...)
		}
	}
}

func (e contractEnvelope) errorClass() string {
	if e.Error.Code != "" {
		return e.Error.Code
	}
	return e.Error.Type
}

func receiveKind(t *testing.T, ctx context.Context, voice realtime.Session, wanted realtime.EventKind) realtime.Event {
	t.Helper()
	for {
		event, err := voice.Receive(ctx)
		require.NoError(t, err)
		if event.Kind == wanted {
			return event
		}
	}
}

func TestDialReadyPostHandshakeCloseIsNeverSkippable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err == nil {
			_ = conn.CloseNow()
		}
	}))
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(t.Context(), contractOperationTimeout)
	defer cancel()
	_, handshake, err := dialReady(ctx, "ws"+server.URL[len("http"):])
	require.Error(t, err)
	assert.True(t, handshake)
	assert.False(t, maySkipDial(handshake, err))
}

func capacityThenUnavailableServer(t *testing.T, payload map[string]any) string {
	t.Helper()
	var server *httptest.Server
	server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		ctx, cancel := context.WithTimeout(r.Context(), contractOperationTimeout)
		defer cancel()
		raw, err := json.Marshal(payload)
		if err != nil || conn.Write(ctx, websocket.MessageText, raw) != nil {
			return
		}
		_ = server.Listener.Close()
	}))
	server.Start()
	t.Cleanup(server.Close)
	return "ws" + server.URL[len("http"):]
}

func TestReadinessRetriesRememberSuccessfulHandshakeAcrossServiceLoss(t *testing.T) {
	t.Run("raw socket", func(t *testing.T) {
		url := capacityThenUnavailableServer(t, map[string]any{
			"type": "error", "error": map[string]any{"type": "session_limit_reached"},
		})
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		conn, endpointSelected, err := waitReadySocket(ctx, url)
		if conn != nil {
			_ = conn.CloseNow()
		}
		require.Error(t, err)
		assert.True(t, maySkipDial(false, err), "fixture must end with a pre-upgrade network failure")
		assert.True(t, endpointSelected, "the first successful upgrade must survive a later pre-upgrade failure")
		assert.False(t, maySkipDial(endpointSelected, err))
	})

	t.Run("real connector", func(t *testing.T) {
		url := capacityThenUnavailableServer(t, map[string]any{
			"type": "error", "error": map[string]any{"type": "session_limit_reached", "message": "all session slots are in use"},
		})
		connector, err := s2sws.New(s2sws.Config{URL: url, Model: "local", ConnectTimeout: time.Second, MaxFrameBytes: contractFrameLimit})
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		voice, endpointSelected, err := connectReadyVoice(ctx, connector)
		if voice != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second)
			_ = voice.Close(closeCtx)
			closeCancel()
		}
		require.Error(t, err)
		assert.True(t, maySkipDial(false, err), "fixture must end with a pre-upgrade network failure")
		assert.True(t, endpointSelected, "the first successful connector upgrade must survive a later pre-upgrade failure")
		assert.False(t, maySkipDial(endpointSelected, err))
	})
}

func TestReadinessUnavailableBeforeAnyHandshakeRemainsSkippable(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	url := "ws" + server.URL[len("http"):]
	server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	conn, endpointSelected, err := waitReadySocket(ctx, url)
	if conn != nil {
		_ = conn.CloseNow()
	}
	require.Error(t, err)
	assert.False(t, endpointSelected)
	assert.True(t, maySkipDial(endpointSelected, err))
}

func TestS2SConnectorForwardsPCMBytesExactly(t *testing.T) {
	expected := testkit.RampPCM(3_840)
	received := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		ctx, cancel := context.WithTimeout(r.Context(), contractOperationTimeout)
		defer cancel()
		created, _ := json.Marshal(map[string]any{"type": "session.created", "event_id": "provider-created", "session": map[string]any{"id": "provider-session"}})
		if conn.Write(ctx, websocket.MessageText, created) != nil {
			return
		}
		var observed []byte
		for {
			kind, raw, readErr := conn.Read(ctx)
			if readErr != nil || kind != websocket.MessageText {
				return
			}
			var frame struct {
				Type  string `json:"type"`
				Audio string `json:"audio"`
			}
			if json.Unmarshal(raw, &frame) != nil {
				return
			}
			switch frame.Type {
			case "input_audio_buffer.append":
				decoded, decodeErr := base64.StdEncoding.DecodeString(frame.Audio)
				if decodeErr != nil {
					return
				}
				observed = append(observed, decoded...)
			case "input_audio_buffer.commit":
				received <- observed
				return
			}
		}
	}))
	t.Cleanup(server.Close)
	connector, err := s2sws.New(s2sws.Config{URL: "ws" + server.URL[len("http"):], Model: "local", ConnectTimeout: contractOperationTimeout, MaxFrameBytes: contractFrameLimit})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), contractOperationTimeout)
	defer cancel()
	voice, err := connector.Connect(ctx, realtime.OpenRequest{SessionID: "pcm-contract", Incarnation: 1, Model: "local"})
	require.NoError(t, err)
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), contractOperationTimeout)
		defer closeCancel()
		_ = voice.Close(closeCtx)
	})
	receiveKind(t, ctx, voice, realtime.EventSessionCreated)
	require.NoError(t, voice.Send(ctx, realtime.Command{Kind: realtime.CommandAppendAudio, Audio: expected}))
	require.NoError(t, voice.Send(ctx, realtime.Command{Kind: realtime.CommandCommitAudio}))
	select {
	case actual := <-received:
		assert.Equal(t, expected, actual)
	case <-ctx.Done():
		t.Fatal("observable provider did not receive committed PCM")
	}
}

func TestContractPartialAdmissionFailureDrainsEverySuccessfulSession(t *testing.T) {
	var accepted atomic.Int32
	var active atomic.Int32
	released := make(chan struct{})
	var releaseOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		active.Add(1)
		defer func() {
			if active.Add(-1) == 0 && accepted.Load() == 3 {
				releaseOnce.Do(func() { close(released) })
			}
			_ = conn.CloseNow()
		}()
		ordinal := accepted.Add(1)
		ctx, cancel := context.WithTimeout(context.Background(), contractOperationTimeout)
		defer cancel()
		if ordinal == 1 {
			payload, _ := json.Marshal(map[string]any{"type": "error", "error": map[string]any{"type": "session_limit_reached"}})
			_ = conn.Write(ctx, websocket.MessageText, payload)
			return
		}
		payload, _ := json.Marshal(map[string]any{"type": "session.created", "session": map[string]any{"id": ordinal}})
		if conn.Write(ctx, websocket.MessageText, payload) != nil {
			return
		}
		_, _, _ = conn.Read(ctx)
	}))
	t.Cleanup(func() {
		server.CloseClientConnections()
		server.Close()
	})
	ctx, cancel := context.WithTimeout(t.Context(), contractOperationTimeout)
	defer cancel()
	results := make(chan admissionResult, 3)
	for range 3 {
		go func() {
			conn, handshake, err := dialReady(ctx, "ws"+server.URL[len("http"):])
			results <- admissionResult{conn: conn, handshake: handshake, err: err}
		}()
	}
	connections, err := collectAdmissions(3, results)
	require.Error(t, err)
	assert.Empty(t, connections)
	select {
	case <-released:
	case <-ctx.Done():
		t.Fatal("partial admission cleanup retained a successful session")
	}
}

func TestSimulatorSessionUpdateAndPCM24AppendCommitAcceptance(t *testing.T) {
	socket := dialContract(t)
	socket.write(t, map[string]any{"type": "session.update", "session": map[string]any{"type": "realtime"}})
	socket.readUntil(t, contractOperationTimeout, "session.updated")
	socket.write(t, map[string]any{"type": "input_audio_buffer.append", "audio": base64.StdEncoding.EncodeToString(testkit.RampPCM(3_840))})
	socket.write(t, map[string]any{"type": "input_audio_buffer.commit"})
	// The simulator contract has no byte-identity acknowledgement. This final
	// update proves only documented append/commit acceptance; exact connector
	// forwarding is asserted separately against an observable provider fake.
	socket.write(t, map[string]any{"type": "session.update", "session": map[string]any{"type": "realtime"}})
	socket.readUntil(t, contractOperationTimeout, "session.updated")
}

func TestSimulatorFunctionCallOneActiveResponseAndCancellation(t *testing.T) {
	socket := dialContract(t)
	socket.write(t, map[string]any{
		"type": "session.update",
		"session": map[string]any{
			"type":  "realtime",
			"tools": []any{map[string]any{"type": "function", "name": "contract_probe", "description": "bounded synthetic contract probe", "parameters": map[string]any{"type": "object", "additionalProperties": false}}},
		},
	})
	socket.readUntil(t, contractOperationTimeout, "session.updated")
	socket.write(t, map[string]any{"type": "conversation.item.create", "item": map[string]any{"type": "message", "role": "user", "content": []any{map[string]any{"type": "input_text", "text": "Run the bounded contract probe."}}}})
	socket.readUntil(t, contractOperationTimeout, "conversation.item.created")
	socket.write(t, map[string]any{"type": "response.create", "response": map[string]any{"instructions": "Call contract_probe exactly once.", "tool_choice": "required"}})
	socket.write(t, map[string]any{"type": "response.create"})
	envelope, _ := socket.readUntil(t, contractFlowTimeout, "error")
	assert.Equal(t, "conversation_already_has_active_response", envelope.errorClass())
	socket.write(t, map[string]any{"type": "response.cancel"})
	_, raw := socket.readUntil(t, contractFlowTimeout, "response.done")
	var done struct {
		Response struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		} `json:"response"`
	}
	require.NoError(t, json.Unmarshal(raw, &done))
	assert.NotEmpty(t, done.Response.ID)
	assert.Equal(t, "cancelled", done.Response.Status)

	// A fresh response after cancellation must be admitted, must surface the
	// configured function call, and must accept its bounded output item.
	socket.write(t, map[string]any{"type": "response.create", "response": map[string]any{"instructions": "Call contract_probe exactly once.", "tool_choice": "required"}})
	_, functionRaw := socket.readUntil(t, contractFlowTimeout, "response.function_call_arguments.done")
	var function struct {
		CallID string `json:"call_id"`
		Name   string `json:"name"`
	}
	require.NoError(t, json.Unmarshal(functionRaw, &function))
	assert.NotEmpty(t, function.CallID)
	assert.Equal(t, "contract_probe", function.Name)
	socket.write(t, map[string]any{"type": "conversation.item.create", "item": map[string]any{
		"type": "function_call_output", "call_id": function.CallID, "output": "{\"ok\":true}",
	}})
	socket.readUntil(t, contractOperationTimeout, "conversation.item.created")
}

func TestSimulatorDeclaredConcurrency(t *testing.T) {
	count := 1
	if raw := os.Getenv("S2S_CONTRACT_CONCURRENCY"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 || parsed > 64 {
			t.Fatal("S2S_CONTRACT_CONCURRENCY must be an integer from 1 through 64")
		}
		count = parsed
	}
	results := make(chan admissionResult, count)
	url := endpoint(t)
	ctx, cancel := context.WithTimeout(t.Context(), contractOperationTimeout)
	defer cancel()
	connections := []*websocket.Conn{awaitReadySocket(t, url)}
	t.Cleanup(func() {
		for _, conn := range connections {
			_ = conn.CloseNow()
		}
	})
	for range count - 1 {
		go func() {
			conn, handshake, err := dialReady(ctx, url)
			results <- admissionResult{conn: conn, handshake: handshake, err: err}
		}()
	}
	admitted, err := collectAdmissions(count-1, results)
	if err != nil {
		for _, conn := range connections {
			_ = conn.CloseNow()
		}
		t.Fatal("simulator did not admit the declared sessions simultaneously")
	}
	connections = append(connections, admitted...)
	// All declared sessions remain open here. The next admission must expose
	// the simulator's declared exact pool bound rather than waiting for a slot.
	extra, handshake, event, err := dialInitial(ctx, url)
	require.NoError(t, err)
	require.True(t, handshake)
	require.NotNil(t, extra)
	assert.Equal(t, "error", event.Type)
	assert.Equal(t, "session_limit_reached", event.errorClass())
	_ = extra.CloseNow()

	for _, conn := range connections {
		require.NoError(t, conn.CloseNow())
	}
	connections = nil
}

func TestSimulatorVADImplicitResponseBindsEpochBeforeAudioAndCompletesOnce(t *testing.T) {
	connector, err := s2sws.New(s2sws.Config{URL: endpoint(t), Model: "local", ConnectTimeout: contractOperationTimeout, MaxFrameBytes: contractFrameLimit})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), contractFlowTimeout)
	defer cancel()
	voice, handshake, err := connectReadyVoice(ctx, connector)
	if err != nil {
		if maySkipDial(handshake, err) {
			t.Skip("configured simulator endpoint is unreachable")
		}
		t.Fatal("configured simulator failed after WebSocket handshake")
	}
	t.Cleanup(func() {
		if voice == nil {
			return
		}
		closeCtx, closeCancel := context.WithTimeout(context.Background(), contractOperationTimeout)
		defer closeCancel()
		_ = voice.Close(closeCtx)
	})
	require.NoError(t, voice.Send(ctx, realtime.Command{Kind: realtime.CommandSessionUpdate, Session: realtime.SessionConfig{
		InputFormat:   realtime.AudioFormat{Encoding: "pcm16le", SampleRate: 24_000, Channels: 1},
		OutputFormat:  realtime.AudioFormat{Encoding: "pcm16le", SampleRate: 24_000, Channels: 1},
		TurnDetection: json.RawMessage(`{"type":"server_vad","threshold":0.2,"silence_duration_ms":300}`),
	}}))
	for {
		event, receiveErr := voice.Receive(ctx)
		require.NoError(t, receiveErr)
		if event.Kind == realtime.EventSessionUpdated {
			break
		}
	}
	// Bootstrap a bounded spoken fixture from the simulator's own TTS. This is
	// synthesized, ephemeral PCM rather than recorded audio, and guarantees the
	// VAD/STT leg receives speech shaped for the configured local pipeline.
	item := json.RawMessage(`{"type":"message","role":"user","content":[{"type":"input_text","text":"Speak a short synthetic test phrase."}]}`)
	require.NoError(t, voice.Send(ctx, realtime.Command{Kind: realtime.CommandCreateItem, Item: item}))
	const fixtureEpoch identity.ResponseEpoch = 7
	require.NoError(t, voice.Send(ctx, realtime.Command{Kind: realtime.CommandCreateResponse, Epoch: fixtureEpoch,
		Response: realtime.ResponseConfig{Instructions: "Speak one short synthetic phrase."}}))
	fixturePCM := make([]byte, 0, 96_000)
	for {
		event, receiveErr := voice.Receive(ctx)
		require.NoError(t, receiveErr)
		switch event.Kind {
		case realtime.EventAudioDelta:
			if event.Epoch == fixtureEpoch {
				if len(event.Audio) > 2<<20-len(fixturePCM) {
					t.Fatal("simulator synthesized PCM exceeded contract bound")
				}
				fixturePCM = append(fixturePCM, event.Audio...)
			}
		case realtime.EventResponseDone:
			if event.Epoch == fixtureEpoch {
				require.NotEmpty(t, fixturePCM)
				goto fixtureReady
			}
		}
	}

fixtureReady:
	closeCtx, closeCancel := context.WithTimeout(context.Background(), contractOperationTimeout)
	require.NoError(t, voice.Close(closeCtx))
	closeCancel()
	voice, handshake, err = connectReadyVoice(ctx, connector)
	require.True(t, handshake)
	require.NoError(t, err)
	require.NoError(t, voice.Send(ctx, realtime.Command{Kind: realtime.CommandSessionUpdate, Session: realtime.SessionConfig{
		InputFormat:   realtime.AudioFormat{Encoding: "pcm16le", SampleRate: 24_000, Channels: 1},
		OutputFormat:  realtime.AudioFormat{Encoding: "pcm16le", SampleRate: 24_000, Channels: 1},
		TurnDetection: json.RawMessage(`{"type":"server_vad","threshold":0.2,"silence_duration_ms":300}`),
	}}))
	for {
		event, receiveErr := voice.Receive(ctx)
		require.NoError(t, receiveErr)
		if event.Kind == realtime.EventSessionUpdated {
			break
		}
	}
	for len(fixturePCM) != 0 {
		width := min(len(fixturePCM), 3_840)
		require.NoError(t, voice.Send(ctx, realtime.Command{Kind: realtime.CommandAppendAudio, Audio: fixturePCM[:width]}))
		fixturePCM = fixturePCM[width:]
	}
	for range 8 {
		require.NoError(t, voice.Send(ctx, realtime.Command{Kind: realtime.CommandAppendAudio, Audio: make([]byte, 3_840)}))
	}
	var responseID string
	var epoch identity.ResponseEpoch = 41
	doneCount := 0
	audioSeen := false
	transcriptionFinalized := false
	for doneCount == 0 {
		event, receiveErr := voice.Receive(ctx)
		if errors.Is(receiveErr, s2sws.ErrUnknownResponseEpoch) {
			t.Fatal("simulator emitted response-specific output before bindable response.created metadata")
		}
		require.NoError(t, receiveErr)
		switch event.Kind {
		case realtime.EventTranscriptionCompleted:
			transcriptionFinalized = true
		case realtime.EventResponseCreated:
			require.True(t, event.NeedsEpochBinding)
			require.True(t, transcriptionFinalized, "implicit response must follow finalized transcription")
			responseID = event.ResponseID
			require.NotEmpty(t, responseID)
			require.NoError(t, voice.Send(ctx, realtime.Command{Kind: realtime.CommandBindResponseEpoch, Epoch: epoch, ResponseID: responseID}))
		case realtime.EventAudioDelta:
			assert.Equal(t, epoch, event.Epoch)
			assert.Equal(t, responseID, event.ResponseID)
			audioSeen = true
		case realtime.EventResponseDone:
			if event.ResponseID == responseID {
				assert.Equal(t, epoch, event.Epoch)
				doneCount++
			}
		}
	}
	assert.True(t, audioSeen)
	probeCtx, probeCancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer probeCancel()
	for {
		event, receiveErr := voice.Receive(probeCtx)
		if errors.Is(receiveErr, context.DeadlineExceeded) {
			break
		}
		require.NoError(t, receiveErr)
		if event.Kind == realtime.EventResponseDone && event.ResponseID == responseID {
			doneCount++
		}
	}
	assert.Equal(t, 1, doneCount)
}

func connectReadyVoice(ctx context.Context, connector *s2sws.Connector) (realtime.Session, bool, error) {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	everHandshook := false
	for {
		voice, err := connector.Connect(ctx, realtime.OpenRequest{SessionID: "contract-vad", Incarnation: 1, Model: "local"})
		if err != nil {
			return nil, everHandshook, err
		}
		everHandshook = true
		event, receiveErr := voice.Receive(ctx)
		if receiveErr == nil && event.Kind == realtime.EventSessionCreated {
			return voice, everHandshook, nil
		}
		closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second)
		_ = voice.Close(closeCtx)
		closeCancel()
		if receiveErr != nil {
			return nil, everHandshook, receiveErr
		}
		if event.Kind != realtime.EventError || !strings.Contains(event.Reason, "session slots are in use") {
			return nil, everHandshook, errors.New("simulator initial connector event class is invalid")
		}
		select {
		case <-ctx.Done():
			return nil, everHandshook, ctx.Err()
		case <-ticker.C:
		}
	}
}
