//go:build load

package load_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/loadgen"
	"github.com/chuanmingliu/openai-gpt-live/internal/protocol"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/chuanmingliu/openai-gpt-live/internal/session"
	"github.com/chuanmingliu/openai-gpt-live/internal/testkit"
	transportws "github.com/chuanmingliu/openai-gpt-live/internal/transport/ws"
	"github.com/coder/websocket"
)

type bargeIDs struct{}

func (bargeIDs) NewSessionID() identity.SessionID           { return "barge-session" }
func (bargeIDs) NewConversationID() identity.ConversationID { return "barge-conversation" }

func TestBargeInAcknowledgedFenceSuppressesEveryStaleFrame(t *testing.T) {
	connector := testkit.NewFakeConnector(16)
	manager := session.NewManager(session.Config{
		IngressFrames: 4, EgressFrames: 4, ControlEvents: 16, WriterControlEvents: 8,
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
	}, connector, bargeIDs{})
	handler := transportws.NewHandler(transportws.Options{MaxFrameBytes: 64 << 10, MaxAudioBytes: 8 << 10, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second}, manager,
		transportws.AdmissionFunc(func(context.Context, *http.Request) (session.Subject, func(), error) {
			return session.Subject{TenantID: "barge", UserID: "barge", ExpiresAt: time.Now().Add(time.Hour)}, func() {}, nil
		}))
	server := httptest.NewServer(handler)
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	client, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/realtime", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseNow()
	voice, err := connector.NextSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	events := make(chan bargeClientEvent, 64)
	readErrors := make(chan error, 1)
	go readBargeClient(ctx, client, events, readErrors)
	requireClientType(t, ctx, events, readErrors, "session.created")

	histogram := loadgen.NewHistogram(time.Second, 3)
	const samples = 20
	for sample := 1; sample <= samples; sample++ {
		responseID := "barge-response-" + strconv.Itoa(sample)
		itemID := "barge-item-" + strconv.Itoa(sample)
		writeClient(t, ctx, client, protocol.ResponseCreateEvent{Type: "response.create"})
		create, err := voice.NextCommand(ctx)
		if err != nil || create.Kind != realtime.CommandCreateResponse {
			t.Fatalf("sample %d create command=%+v err=%v", sample, create, err)
		}
		if err := voice.Emit(ctx, realtime.Event{Kind: realtime.EventResponseCreated, Epoch: create.Epoch, ProviderEventID: "created", ResponseID: responseID, Status: "in_progress"}); err != nil {
			t.Fatal(err)
		}
		requireClientType(t, ctx, events, readErrors, "response.created")
		frame := loadgen.EncodeWatermark(loadgen.Watermark{Session: 1, Epoch: uint64(create.Epoch), Sequence: uint64(sample), SentOffsetNanos: int64(sample)})
		if err := voice.Emit(ctx, realtime.Event{Kind: realtime.EventAudioDelta, Epoch: create.Epoch, ProviderEventID: "initial", ResponseID: responseID, ItemID: itemID, Audio: frame}); err != nil {
			t.Fatal(err)
		}
		requireClientType(t, ctx, events, readErrors, "response.output_audio.delta")

		started := time.Now()
		if err := voice.Emit(ctx, realtime.Event{Kind: realtime.EventSpeechStarted, ProviderEventID: "speech", ItemID: "new-turn"}); err != nil {
			t.Fatal(err)
		}
		cancelCommand, err := voice.NextCommand(ctx)
		if err != nil || cancelCommand.Kind != realtime.CommandCancelResponse {
			t.Fatalf("sample %d cancel command=%+v err=%v", sample, cancelCommand, err)
		}
		histogram.Record(time.Since(started))
		requireClientTypes(t, ctx, events, readErrors, "input_audio_buffer.speech_started", "response.done")
		for sequence := uint64(1); sequence <= 8; sequence++ {
			stale := loadgen.EncodeWatermark(loadgen.Watermark{Session: 1, Epoch: uint64(create.Epoch), Sequence: sequence, SentOffsetNanos: int64(sequence)})
			if err := voice.Emit(ctx, realtime.Event{Kind: realtime.EventAudioDelta, Epoch: create.Epoch, ProviderEventID: "stale", ResponseID: responseID, ItemID: itemID, Audio: stale}); err != nil {
				t.Fatal(err)
			}
		}
		requireSilenceDeadline(t, events, readErrors, 10*time.Millisecond)
		if err := voice.Emit(ctx, realtime.Event{Kind: realtime.EventSpeechStopped, ProviderEventID: "speech-stopped", ItemID: "new-turn"}); err != nil {
			t.Fatal(err)
		}
		requireClientType(t, ctx, events, readErrors, "input_audio_buffer.speech_stopped")

		proveBargeConnectionHealthy(t, ctx, client, voice, events, readErrors, sample)
	}
	if histogram.Count() != samples || histogram.Percentile(95) >= 100*time.Millisecond {
		t.Fatalf("barge-in samples=%d p95=%s", histogram.Count(), histogram.Percentile(95))
	}
	t.Logf("barge-in samples=%d p95=%s stale_frames=%d leaked=0 healthy_after_each=true", histogram.Count(), histogram.Percentile(95), samples*8)
}

func requireClientTypes(t *testing.T, ctx context.Context, events <-chan bargeClientEvent, readErrors <-chan error, wanted ...string) {
	t.Helper()
	remaining := make(map[string]struct{}, len(wanted))
	for _, typeName := range wanted {
		remaining[typeName] = struct{}{}
	}
	for len(remaining) != 0 {
		select {
		case event := <-events:
			if _, ok := remaining[event.typeName]; !ok {
				t.Fatalf("unexpected event %q while draining cancellation controls", event.typeName)
			}
			delete(remaining, event.typeName)
		case err := <-readErrors:
			t.Fatalf("connection failed while draining cancellation controls: %v", err)
		case <-ctx.Done():
			t.Fatalf("draining cancellation controls: %v", ctx.Err())
		}
	}
}

type bargeClientEvent struct {
	typeName string
	delta    string
}

func readBargeClient(ctx context.Context, client *websocket.Conn, events chan<- bargeClientEvent, readErrors chan<- error) {
	for {
		_, raw, err := client.Read(ctx)
		if err != nil {
			readErrors <- err
			return
		}
		var envelope struct {
			Type  string `json:"type"`
			Delta string `json:"delta"`
		}
		if err := json.Unmarshal(raw, &envelope); err != nil {
			readErrors <- err
			return
		}
		select {
		case events <- bargeClientEvent{typeName: envelope.Type, delta: envelope.Delta}:
		case <-ctx.Done():
			return
		}
	}
}

func requireClientType(t *testing.T, ctx context.Context, events <-chan bargeClientEvent, readErrors <-chan error, want string) {
	t.Helper()
	for {
		select {
		case event := <-events:
			if event.typeName != want {
				continue
			}
			if event.delta != "" {
				if _, err := base64.StdEncoding.DecodeString(event.delta); err != nil {
					t.Fatal("invalid encoded frame")
				}
			}
			return
		case err := <-readErrors:
			t.Fatalf("connection failed while waiting for %s: %v", want, err)
		case <-ctx.Done():
			t.Fatalf("waiting for %s: %v", want, ctx.Err())
		}
	}
}

func requireSilenceDeadline(t *testing.T, events <-chan bargeClientEvent, readErrors <-chan error, duration time.Duration) {
	t.Helper()
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case event := <-events:
		t.Fatalf("event %q crossed the acknowledged fence", event.typeName)
	case err := <-readErrors:
		t.Fatalf("closed connection masqueraded as silence: %v", err)
	case <-timer.C:
	}
}

func proveBargeConnectionHealthy(t *testing.T, ctx context.Context, client *websocket.Conn, voice *testkit.FakeVoiceSession, events <-chan bargeClientEvent, readErrors <-chan error, sample int) {
	t.Helper()
	writeClient(t, ctx, client, protocol.ResponseCreateEvent{Type: "response.create"})
	create, err := voice.NextCommand(ctx)
	if err != nil || create.Kind != realtime.CommandCreateResponse {
		t.Fatalf("sample %d health create command=%+v err=%v", sample, create, err)
	}
	responseID := "health-response-" + strconv.Itoa(sample)
	itemID := "health-item-" + strconv.Itoa(sample)
	if err := voice.Emit(ctx, realtime.Event{Kind: realtime.EventResponseCreated, Epoch: create.Epoch, ProviderEventID: "health-created", ResponseID: responseID, Status: "in_progress"}); err != nil {
		t.Fatal(err)
	}
	requireClientType(t, ctx, events, readErrors, "response.created")
	frame := loadgen.EncodeWatermark(loadgen.Watermark{Session: 1, Epoch: uint64(create.Epoch), Sequence: uint64(sample), SentOffsetNanos: int64(sample)})
	if err := voice.Emit(ctx, realtime.Event{Kind: realtime.EventAudioDelta, Epoch: create.Epoch, ProviderEventID: "health-audio", ResponseID: responseID, ItemID: itemID, Audio: frame}); err != nil {
		t.Fatal(err)
	}
	requireClientType(t, ctx, events, readErrors, "response.output_audio.delta")
	if err := voice.Emit(ctx, realtime.Event{Kind: realtime.EventResponseDone, Epoch: create.Epoch, ProviderEventID: "health-done", ResponseID: responseID, Status: "completed"}); err != nil {
		t.Fatal(err)
	}
	requireClientType(t, ctx, events, readErrors, "response.done")
}

func writeClient(t *testing.T, ctx context.Context, conn *websocket.Conn, event any) {
	t.Helper()
	raw, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Write(ctx, websocket.MessageText, raw); err != nil {
		t.Fatal(err)
	}
}
