package session

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/protocol"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime/s2sws"
	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Removing exact rejection retirement must leave A retained and stamp B's
// acknowledgement with A's sequence. Exercise the real WebSocket adapter.
func TestSessionUpdateRejectionRetiresOnlyRejectedSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	reactor, _ := testReactor(0)
	var commands []realtime.Command
	for _, instructions := range []string{"snapshot A", "snapshot B"} {
		effects := applyOK(t, reactor, ClientInput{Event: protocol.SessionUpdateEvent{
			Type: "session.update", Session: protocol.SessionUpdate{Type: "realtime", Instructions: instructions},
		}})
		require.Len(t, effects, 1)
		commands = append(commands, *effects[0].VoiceCommand)
	}
	a, b := commands[0].Sequence, commands[1].Sequence
	bSnapshot := string(reactor.pendingSessionUpdates[b].session)
	bBytes := reactor.pendingSessionUpdates[b].retainedBytes
	wireIDs := make(chan string, 2)
	providerErrors := make(chan error, 1)
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			providerErrors <- err
			return
		}
		defer conn.CloseNow()
		var updateIDs []string
		for range commands {
			_, raw, err := conn.Read(ctx)
			if err != nil {
				providerErrors <- err
				return
			}
			var frame struct {
				EventID string `json:"event_id"`
			}
			if err := json.Unmarshal(raw, &frame); err != nil {
				providerErrors <- err
				return
			}
			wireIDs <- frame.EventID
			updateIDs = append(updateIDs, frame.EventID)
		}
		for _, raw := range []string{
			fmt.Sprintf(`{"type":"error","event_id":"provider-rejection","error":{"event_id":%q,"message":"private provider detail"}}`, updateIDs[0]),
			`{"type":"session.updated","event_id":"provider-ack-B"}`,
		} {
			if err := conn.Write(ctx, websocket.MessageText, []byte(raw)); err != nil {
				providerErrors <- err
				return
			}
		}
		providerErrors <- nil
		<-ctx.Done()
	}))
	defer provider.Close()
	connector, err := s2sws.New(s2sws.Config{URL: "ws" + strings.TrimPrefix(provider.URL, "http"), ConnectTimeout: time.Second, MaxFrameBytes: 1 << 20})
	require.NoError(t, err)
	voice, err := connector.Connect(ctx, realtime.OpenRequest{Model: "model"})
	require.NoError(t, err)
	defer voice.Close(context.Background())
	defer cancel()
	for _, command := range commands {
		require.NoError(t, voice.Send(ctx, command))
	}
	for _, command := range commands {
		select {
		case eventID := <-wireIDs:
			assert.Equal(t, fmt.Sprintf("live-session-update-%d", command.Sequence), eventID)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	rejected, err := voice.Receive(ctx)
	require.NoError(t, err)
	assert.Equal(t, realtime.EventError, rejected.Kind)
	assert.True(t, rejected.SessionUpdateRejected)
	assert.Equal(t, a, rejected.SessionUpdateSequence)
	assert.False(t, rejected.SessionUpdateAcknowledgement)
	assert.Equal(t, "Realtime provider request failed.", rejected.Reason)
	assert.Empty(t, rejected.Payload)
	effects := applyOK(t, reactor, currentVoice(reactor, rejected))
	require.Len(t, effects, 1)
	public, err := protocol.Encode(effects[0].ClientEvent)
	require.NoError(t, err)
	assert.IsType(t, protocol.ErrorEvent{}, effects[0].ClientEvent)
	assert.NotContains(t, string(public), "private provider detail")
	assert.NotContains(t, string(public), "live-session-update-")
	assert.NotContains(t, reactor.pendingSessionUpdates, a)
	assert.Equal(t, bBytes, reactor.pendingSessionUpdateBytes)
	ack, err := voice.Receive(ctx)
	require.NoError(t, err)
	assert.True(t, ack.SessionUpdateAcknowledgement)
	assert.Equal(t, b, ack.SessionUpdateSequence)
	effects = applyOK(t, reactor, currentVoice(reactor, ack))
	require.Len(t, effects, 1)
	updated, ok := effects[0].ClientEvent.(protocol.SessionUpdatedEvent)
	require.True(t, ok)
	assert.JSONEq(t, bSnapshot, string(updated.Session))
	assert.Empty(t, reactor.pendingSessionUpdates)
	assert.Zero(t, reactor.pendingSessionUpdateBytes)
	require.NoError(t, <-providerErrors)
}
