package session

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/profile"
	"github.com/chuanmingliu/openai-gpt-live/internal/prompttmpl"
	"github.com/chuanmingliu/openai-gpt-live/internal/protocol"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/chuanmingliu/openai-gpt-live/internal/testkit"
	"github.com/stretchr/testify/require"
)

func profileWithOpening(t *testing.T, opening string) profile.Resolved {
	t.Helper()
	resolved := profile.Builtin()
	resolved.Snapshot.TenantID = "tenant"
	resolved.Snapshot.Ref = profile.VersionRef{ID: "insurance", Version: 1}

	blob, err := (profile.Blob{MediaType: "text/plain; charset=utf-8", Content: []byte(opening)}).Canonical()
	require.NoError(t, err)
	ref := profile.TemplateRef{Hash: blob.Hash, MediaType: blob.MediaType, Bytes: blob.ContentBytes}
	resolved.Definition.Templates["opening_message.j2"] = ref
	resolved.Bundle.Templates["opening_message.j2"] = opening
	resolved.Snapshot.Opening = ref
	resolved.Snapshot.BundleHash, err = resolved.Definition.Hash()
	require.NoError(t, err)
	resolved.Snapshot.SnapshotHash = ""
	resolved.Snapshot.SnapshotHash, err = resolved.Snapshot.Hash()
	require.NoError(t, err)
	require.NoError(t, resolved.Validate())
	return resolved
}

func TestProfileSystemInstructionsJoinReservedToolsBeforeProviderAck(t *testing.T) {
	connector := testkit.NewFakeConnector(2)
	selected := profile.Builtin()
	cfg := Config{
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
		InitialSession:      ReservedHandoffSessionConfig(),
		HandoffToolsEnabled: true,
		ProfileEngine:       prompttmpl.NewMiniJinja(),
	}
	cfg.InitialSession.TurnDetection = json.RawMessage(`null`)
	manager := NewManager(cfg, connector, managerIDs{session: "profile-system", conversation: "profile-system"})
	client := newChannelClient(8)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- manager.Run(ctx, OpenRequest{Subject: Subject{
			TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour),
		}, Profile: selected}, client)
	}()

	voice, err := connector.NextSession(t.Context())
	require.NoError(t, err)
	initial, err := voice.NextCommand(t.Context())
	require.NoError(t, err)
	require.Equal(t, realtime.CommandSessionUpdate, initial.Kind)
	require.Equal(t, selected.Bundle.Templates["system.j2"], initial.Session.Instructions)
	require.NotEmpty(t, initial.Session.Tools)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestProfileOpeningStartsOnlyAfterMatchingProviderSessionUpdated(t *testing.T) {
	connector := testkit.NewFakeConnector(2)
	selected := profileWithOpening(t, "Welcome to insurance sales.")
	cfg := Config{
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
		InitialSession:      realtime.SessionConfig{TurnDetection: json.RawMessage(`null`)},
		ProfileEngine:       prompttmpl.NewMiniJinja(),
	}
	manager := NewManager(cfg, connector, managerIDs{session: "profile-opening", conversation: "profile-opening"})
	client := newChannelClient(8)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- manager.Run(ctx, OpenRequest{Subject: Subject{
			TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour),
		}, Profile: selected}, client)
	}()

	voice, err := connector.NextSession(t.Context())
	require.NoError(t, err)
	initial, err := voice.NextCommand(t.Context())
	require.NoError(t, err)
	require.Equal(t, realtime.CommandSessionUpdate, initial.Kind)
	require.Equal(t, selected.Bundle.Templates["system.j2"], initial.Session.Instructions)
	require.IsType(t, protocol.SessionCreatedEvent{}, <-client.writes)
	require.IsType(t, protocol.LiveProfileSelectedEvent{}, <-client.writes)

	shortCtx, shortCancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	_, err = voice.NextCommand(shortCtx)
	shortCancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)

	require.NoError(t, voice.Emit(t.Context(), realtime.Event{
		Kind:                         realtime.EventSessionUpdated,
		ProviderEventID:              "profile-bootstrap-ack",
		InputTurnMode:                realtime.InputTurnModeManual,
		InputTurnModeSpecified:       true,
		SessionUpdateAcknowledgement: true,
		SessionUpdateSequence:        0,
	}))
	opening, err := voice.NextCommand(t.Context())
	require.NoError(t, err)
	require.Equal(t, realtime.CommandCreateResponse, opening.Kind)
	require.Equal(t, "Welcome to insurance sales.", opening.Response.Instructions)
	require.JSONEq(t, `"none"`, string(opening.Response.ToolChoice))

	// A repeated provider acknowledgement must not speak the opening twice.
	require.NoError(t, voice.Emit(t.Context(), realtime.Event{
		Kind:                         realtime.EventSessionUpdated,
		ProviderEventID:              "profile-bootstrap-ack-duplicate",
		InputTurnMode:                realtime.InputTurnModeManual,
		InputTurnModeSpecified:       true,
		SessionUpdateAcknowledgement: true,
		SessionUpdateSequence:        0,
	}))
	shortCtx, shortCancel = context.WithTimeout(t.Context(), 20*time.Millisecond)
	_, err = voice.NextCommand(shortCtx)
	shortCancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestProfileOwnedInstructionsRejectBrowserOverride(t *testing.T) {
	reactor := NewReactor(IDs{SessionID: "profile-owned", ConversationID: "profile-owned", Incarnation: 1}, NewEpochFence(0))
	reactor.setProfileOwned(true)
	_, err := reactor.Apply(ClientInput{Event: protocol.SessionUpdateEvent{Session: protocol.SessionUpdate{
		Type:         "realtime",
		Instructions: "replace the server-owned profile",
	}}})
	require.ErrorIs(t, err, ErrUnsupportedField)
}
