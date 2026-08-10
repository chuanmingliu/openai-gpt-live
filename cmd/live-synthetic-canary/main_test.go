package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/chuanmingliu/openai-gpt-live/internal/session"
	"github.com/chuanmingliu/openai-gpt-live/internal/testkit"
	transportws "github.com/chuanmingliu/openai-gpt-live/internal/transport/ws"
)

type canaryTestIDs struct{ next atomic.Int64 }

func (ids *canaryTestIDs) NewSessionID() identity.SessionID {
	return identity.SessionID(fmt.Sprintf("canary-session-%d", ids.next.Add(1)))
}

func (ids *canaryTestIDs) NewConversationID() identity.ConversationID {
	return identity.ConversationID(fmt.Sprintf("canary-conversation-%d", ids.next.Add(1)))
}

func TestValidateIsolatedSessionsRejectsCrossSessionIdentity(t *testing.T) {
	if err := validateIsolatedSessions([]byte(`{"type":"session.created","event_id":"e1","session":{}}`), []byte(`{"type":"session.created","event_id":"e2","session":{}}`)); err != nil {
		t.Fatal(err)
	}
	if err := validateIsolatedSessions([]byte(`{"type":"session.created","event_id":"e1","session":{}}`), []byte(`{"type":"session.created","event_id":"e1","session":{}}`)); err == nil {
		t.Fatal("shared cross-session identity was accepted")
	}
}

func TestValidateSentinelFramesRequiresOwnAndRejectsCrossPresentation(t *testing.T) {
	left, right := "synthetic-left-123", "synthetic-right-456"
	if err := validateSentinelFrames([][]byte{[]byte(`{"type":"response.output_audio_transcript.delta","delta":"synthetic-left-123"}`)}, [][]byte{[]byte(`{"type":"response.output_audio_transcript.delta","delta":"synthetic-right-456"}`)}, left, right); err != nil {
		t.Fatal(err)
	}
	if err := validateSentinelFrames([][]byte{[]byte(`{"delta":"synthetic-right-456"}`)}, [][]byte{[]byte(`{"delta":"synthetic-right-456"}`)}, left, right); err == nil {
		t.Fatal("cross-session sentinel presentation was accepted")
	}
	if err := validateSentinelFrames([][]byte{[]byte(`{"type":"response.done"}`)}, [][]byte{[]byte(`{"delta":"synthetic-right-456"}`)}, left, right); err == nil {
		t.Fatal("missing own sentinel presentation was accepted")
	}
}

func TestSentinelMessagesCarryRequiredItemAndEventIDs(t *testing.T) {
	item, response, err := sentinelMessages("gpt-live-synthetic-left-0123456789abcdef", "left")
	if err != nil {
		t.Fatal(err)
	}
	for name, document := range map[string][]byte{"item": item, "response": response} {
		var event map[string]any
		if err := json.Unmarshal(document, &event); err != nil {
			t.Fatal(err)
		}
		if value, _ := event["event_id"].(string); value == "" {
			t.Fatalf("%s event has no event_id", name)
		}
	}
	var itemEvent struct {
		Item struct {
			ID string `json:"id"`
		} `json:"item"`
	}
	if err := json.Unmarshal(item, &itemEvent); err != nil {
		t.Fatal(err)
	}
	if itemEvent.Item.ID == "" {
		t.Fatal("conversation.item.create has no item.id")
	}
}

func TestResponseObservationRequiresTypedMatchingTerminalAndDetectsPostTerminalLeak(t *testing.T) {
	left := "gpt-live-synthetic-left-0123456789abcdef"
	right := "gpt-live-synthetic-right-0123456789abcdef"
	observation := newResponseObservation(left, right)
	for _, frame := range [][]byte{
		[]byte(`{"type":"response.created","event_id":"created","response":{"id":"response-left","status":"in_progress"}}`),
		[]byte(`{"type":"response.output_audio_transcript.delta","event_id":"delta","response_id":"response-left","item_id":"item-left","output_index":0,"content_index":0,"delta":"` + left + `"}`),
		[]byte(`{"type":"response.done","event_id":"done","response":{"id":"response-left","status":"completed"}}`),
	} {
		if err := observation.consume(frame); err != nil {
			t.Fatal(err)
		}
	}
	if !observation.complete() || observation.responseID != "response-left" {
		t.Fatal("matching terminal response was not aggregated")
	}
	if err := observation.consume([]byte(`{"type":"response.output_audio_transcript.delta","event_id":"late","response_id":"other","item_id":"other","output_index":0,"content_index":0,"delta":"` + right + `"}`)); err == nil || !strings.Contains(err.Error(), "leak") {
		t.Fatalf("post-terminal foreign sentinel error = %v", err)
	}

	mismatch := newResponseObservation(left, right)
	if err := mismatch.consume([]byte(`{"type":"response.created","event_id":"created","response":{"id":"response-left","status":"in_progress"}}`)); err != nil {
		t.Fatal(err)
	}
	if err := mismatch.consume([]byte(`{"type":"response.done","event_id":"done","response":{"id":"response-other","status":"completed"}}`)); err == nil {
		t.Fatal("mismatched response.done correlation was accepted")
	}

	oversized := newResponseObservation(left, right)
	oversizedID := strings.Repeat("x", 129)
	if err := oversized.consume([]byte(`{"type":"response.created","event_id":"created","response":{"id":"` + oversizedID + `","status":"in_progress"}}`)); err == nil {
		t.Fatal("provider response ID above the bounded live-correlation limit was accepted")
	}
}

func TestRuntimeHeadersMustMatchBothAuthenticatedSessions(t *testing.T) {
	want := runtimeExpectation{ConfigHash: "v1:" + strings.Repeat("a", 64), Region: "us-east-1", GatewayImage: "ghcr.io/chuanmingliu/openai-gpt-live/gateway@sha256:" + strings.Repeat("b", 64)}
	headers := http.Header{"X-Live-Config-Hash": []string{want.ConfigHash}, "X-Live-Region": []string{want.Region}, "X-Live-Gateway-Image": []string{want.GatewayImage}}
	if _, err := validateRuntimeHeaders(headers, headers.Clone(), want); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(http.Header){
		"missing":   func(header http.Header) { header.Del("X-Live-Region") },
		"mismatch":  func(header http.Header) { header.Set("X-Live-Config-Hash", "v1:"+strings.Repeat("c", 64)) },
		"duplicate": func(header http.Header) { header.Add("X-Live-Gateway-Image", want.GatewayImage) },
	} {
		t.Run(name, func(t *testing.T) {
			other := headers.Clone()
			mutate(other)
			if _, err := validateRuntimeHeaders(headers, other, want); err == nil {
				t.Fatal("invalid runtime response metadata was accepted")
			}
		})
	}
}

func TestProbeExercisesRealGatewayProviderAndBothTerminalResponses(t *testing.T) {
	connector := testkit.NewFakeConnector(8)
	manager := session.NewManager(session.Config{
		IngressFrames: 8, EgressFrames: 8, ControlEvents: 8, WriterControlEvents: 8,
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
	}, connector, &canaryTestIDs{})
	want := runtimeExpectation{ConfigHash: "v1:" + strings.Repeat("a", 64), Region: "us-east-1", GatewayImage: "ghcr.io/chuanmingliu/openai-gpt-live/gateway@sha256:" + strings.Repeat("b", 64)}
	admission := transportws.AdmissionFunc(func(_ context.Context, request *http.Request) (session.Subject, func(), error) {
		if request.Header.Get("Authorization") != "Bearer renewable-canary-token" {
			return session.Subject{}, nil, fmt.Errorf("unauthorized")
		}
		return session.Subject{TenantID: "synthetic", UserID: "canary", Ephemeral: true, ExpiresAt: time.Now().Add(time.Minute)}, func() {}, nil
	})
	handler := transportws.NewHandler(transportws.Options{MaxFrameBytes: 64 << 10, CanaryRuntimeIdentity: transportws.RuntimeIdentity{ConfigHash: want.ConfigHash, Region: want.Region, GatewayImage: want.GatewayImage}}, manager, admission)
	server := httptest.NewServer(handler)
	defer server.Close()

	providerResponseIDs := []string{
		"github_pat_AAAAAAAAAAAAAAAAAAAA",
		"sk_live_BBBBBBBBBBBBBBBBBBBB",
	}
	providerErr := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		for index := 0; index < 2; index++ {
			voice, err := connector.NextSession(ctx)
			if err != nil {
				providerErr <- err
				return
			}
			item, err := voice.NextCommand(ctx)
			if err != nil || item.Kind != realtime.CommandCreateItem {
				providerErr <- fmt.Errorf("read synthetic item command: %v kind=%v", err, item.Kind)
				return
			}
			var decoded struct {
				Content []struct {
					Text string `json:"text"`
				} `json:"content"`
			}
			if err := json.Unmarshal(item.Item, &decoded); err != nil || len(decoded.Content) != 1 {
				providerErr <- fmt.Errorf("decode synthetic item: %v", err)
				return
			}
			create, err := voice.NextCommand(ctx)
			if err != nil || create.Kind != realtime.CommandCreateResponse {
				providerErr <- fmt.Errorf("read synthetic response command: %v kind=%v", err, create.Kind)
				return
			}
			responseID := providerResponseIDs[index]
			for _, event := range []realtime.Event{
				{Kind: realtime.EventResponseCreated, Epoch: create.Epoch, ProviderEventID: "created-" + responseID, ResponseID: responseID, Status: "in_progress"},
				{Kind: realtime.EventTranscriptDelta, Epoch: create.Epoch, ProviderEventID: "delta-" + responseID, ResponseID: responseID, ItemID: fmt.Sprintf("item-%d", index+1), Text: decoded.Content[0].Text},
				{Kind: realtime.EventResponseDone, Epoch: create.Epoch, ProviderEventID: "done-" + responseID, ResponseID: responseID, Status: "completed"},
			} {
				if err := voice.Emit(ctx, event); err != nil {
					providerErr <- err
					return
				}
			}
		}
		providerErr <- nil
	}()

	tokenPath := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenPath, []byte("renewable-canary-token"), 0o400); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	observation, err := probe(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/realtime", server.URL, tokenPath, want)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-providerErr; err != nil {
		t.Fatal(err)
	}
	wantCommitments := []string{
		"19cfe453f4352e12750875044e9fd4a1201da7a326a802da451d7914cb8fe94a",
		"2ae39b4c87e5f71cb3791f64395ad35d293c37b92c04c220800993a939222c72",
	}
	if observation.Runtime != want || !slices.Equal(observation.ResponseIDCommitments, wantCommitments) || observation.PostTerminalObservationMS != 500 {
		t.Fatalf("unexpected probe observation: %+v", observation)
	}
}

func TestCanaryEvidenceContainsOnlyDistinctSHA256ResponseCommitments(t *testing.T) {
	observation := probeObservation{
		Runtime: runtimeExpectation{
			ConfigHash:   "v1:" + strings.Repeat("a", 64),
			Region:       "us-east-1",
			GatewayImage: "ghcr.io/chuanmingliu/openai-gpt-live/gateway@sha256:" + strings.Repeat("b", 64),
		},
		ResponseIDCommitments: []string{
			"19cfe453f4352e12750875044e9fd4a1201da7a326a802da451d7914cb8fe94a",
			"2ae39b4c87e5f71cb3791f64395ad35d293c37b92c04c220800993a939222c72",
		},
		PostTerminalObservationMS: 500,
	}
	document, err := marshalCanaryEvidence(observation, time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"github_pat_AAAAAAAAAAAAAAAAAAAA", "sk_live_BBBBBBBBBBBBBBBBBBBB", "response_ids"} {
		if strings.Contains(string(document), raw) {
			t.Fatalf("canary evidence persisted forbidden raw identifier material %q: %s", raw, document)
		}
	}
	var artifact struct {
		ResponseIDCommitments []string `json:"response_id_commitments"`
	}
	if err := json.Unmarshal(document, &artifact); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(artifact.ResponseIDCommitments, observation.ResponseIDCommitments) {
		t.Fatalf("response_id_commitments = %v, want %v", artifact.ResponseIDCommitments, observation.ResponseIDCommitments)
	}
}
