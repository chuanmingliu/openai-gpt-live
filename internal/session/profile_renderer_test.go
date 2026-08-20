package session

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/profile"
	"github.com/chuanmingliu/openai-gpt-live/internal/prompttmpl"
	"github.com/chuanmingliu/openai-gpt-live/internal/protocol"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/chuanmingliu/openai-gpt-live/internal/testkit"
	"github.com/stretchr/testify/require"
)

type gatedProfileEngine struct {
	engine  prompttmpl.Engine
	entered chan struct{}
	gate    chan struct{}
	once    sync.Once
}

func (engine *gatedProfileEngine) Version() string { return engine.engine.Version() }

func (engine *gatedProfileEngine) Compile(ctx context.Context, bundle prompttmpl.Bundle, limits prompttmpl.Limits) (prompttmpl.CompiledBundle, error) {
	engine.once.Do(func() { close(engine.entered) })
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-engine.gate:
		return engine.engine.Compile(ctx, bundle, limits)
	}
}

func profileWithTurn(t *testing.T, source string) profile.Resolved {
	t.Helper()
	resolved := profileWithOpening(t, "")
	delete(resolved.Definition.Templates, "opening_message.j2")
	delete(resolved.Bundle.Templates, "opening_message.j2")
	resolved.Snapshot.Opening = profile.TemplateRef{}

	blob, err := (profile.Blob{MediaType: "text/plain; charset=utf-8", Content: []byte(source)}).Canonical()
	require.NoError(t, err)
	ref := profile.TemplateRef{Hash: blob.Hash, MediaType: blob.MediaType, Bytes: blob.ContentBytes}
	resolved.Definition.Templates["turn_instruction.j2"] = ref
	resolved.Bundle.Templates["turn_instruction.j2"] = source
	field := prompttmpl.Field{
		Type: prompttmpl.TypeString, Required: true, MaxBytes: DurableTranscriptBytes,
		Templates: []string{"turn_instruction.j2"}, Source: "finalized_transcript",
	}
	resolved.Definition.Schema.Fields = map[string]prompttmpl.Field{"transcript": field}
	resolved.Bundle.Schema.Fields = map[string]prompttmpl.Field{"transcript": field}
	resolved.Snapshot.Turn = ref
	resolved.Snapshot.BundleHash, err = resolved.Definition.Hash()
	require.NoError(t, err)
	resolved.Snapshot.SnapshotHash = ""
	resolved.Snapshot.SnapshotHash, err = resolved.Snapshot.Hash()
	require.NoError(t, err)
	require.NoError(t, resolved.Validate())
	return resolved
}

func profileWithWorkerRecipe(t *testing.T) profile.Resolved {
	t.Helper()
	resolved := profileWithTurn(t, "Answer: {{ transcript }}")
	worker, err := (profile.Blob{MediaType: "text/plain; charset=utf-8", Content: []byte("Research {{ transcript }}")}).Canonical()
	require.NoError(t, err)
	ref := profile.TemplateRef{Hash: worker.Hash, MediaType: worker.MediaType, Bytes: worker.ContentBytes}
	resolved.Definition.Templates["workers/renewal.j2"] = ref
	resolved.Bundle.Templates["workers/renewal.j2"] = "Research {{ transcript }}"
	resolved.Snapshot.Recipes = []profile.WorkerRecipe{{ID: "renewal", Version: 2, Objective: ref}}
	resolved.Snapshot.Knowledge = []profile.KnowledgeRef{{ID: "policy", Version: 7, Hash: strings.Repeat("c", 64)}}
	resolved.Snapshot.BundleHash, err = resolved.Definition.Hash()
	require.NoError(t, err)
	resolved.Snapshot.SnapshotHash = ""
	resolved.Snapshot.SnapshotHash, err = resolved.Snapshot.Hash()
	require.NoError(t, err)
	require.NoError(t, resolved.Validate())
	return resolved
}

func TestProfileRuntimePinsExactWorkerRecipeAndKnowledge(t *testing.T) {
	selected := profileWithWorkerRecipe(t)
	runtime, err := prepareProfileRuntime(t.Context(), selected, prompttmpl.NewMiniJinja())
	require.NoError(t, err)
	got, ok := runtime.HandoffProfileSnapshot("renewal question")
	require.True(t, ok)
	require.Equal(t, "insurance", got.ProfileID)
	require.Equal(t, uint64(1), got.ProfileVersion)
	require.Equal(t, selected.Snapshot.SnapshotHash, got.SnapshotHash)
	require.Equal(t, "renewal", got.RecipeID)
	require.Equal(t, uint64(2), got.RecipeVersion)
	require.Equal(t, selected.Snapshot.AdapterRegistryVersion, got.RegistryVersion)
	require.Equal(t, []handoff.KnowledgeVersion{{ID: "policy", Version: 7, Hash: strings.Repeat("c", 64)}}, got.Knowledge)
	require.JSONEq(t, `{"transcript":"renewal question"}`, string(got.Evidence))
}

func TestProfileWithoutWorkerRecipeRejectsStartHandoffWithoutLegacyFallback(t *testing.T) {
	selected := profileWithTurn(t, "Answer: {{ transcript }}")
	runtime, err := prepareProfileRuntime(t.Context(), selected, prompttmpl.NewMiniJinja())
	require.NoError(t, err)
	reactor, _ := testReactor(1)
	reactor.configureProfileBootstrap(runtime, false)
	reactor.profileBootstrapComplete = true

	epoch := reactor.fence.Current()
	reactor.state.Response = ResponseStreaming
	reactor.activeResponseID = "response-profile-handoff"
	reactor.activeResponseEpoch = epoch
	openManualInputTranscription(t, reactor, "turn-profile-handoff")
	effects := applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventTranscriptionCompleted, ItemID: "turn-profile-handoff", Text: "needs research",
	}))
	turn := effects[0].Transcript.Turn
	applyOK(t, reactor, currentVoice(reactor, backgroundToolEvent(
		epoch, "response-profile-handoff", "call-profile-handoff", json.RawMessage(`{"objective":"research"}`),
	)))
	effects = applyOK(t, reactor, TurnCommittedInput{Sequence: 1, Turn: turn, Transcript: "needs research"})
	require.Len(t, effects, 1)
	require.NotNil(t, effects[0].Handoff)
	require.Equal(t, "profile_recipe_unavailable", effects[0].Handoff.RejectReason)
	require.Empty(t, effects[0].Handoff.Profile.ProfileID)
}

func TestProfileStartHandoffCarriesExactImmutableRecipeAuthority(t *testing.T) {
	selected := profileWithWorkerRecipe(t)
	runtime, err := prepareProfileRuntime(t.Context(), selected, prompttmpl.NewMiniJinja())
	require.NoError(t, err)
	reactor, _ := testReactor(1)
	reactor.configureProfileBootstrap(runtime, false)
	reactor.profileBootstrapComplete = true
	epoch := reactor.fence.Current()
	reactor.state.Response = ResponseStreaming
	reactor.activeResponseID = "response-profile-authority"
	reactor.activeResponseEpoch = epoch
	openManualInputTranscription(t, reactor, "turn-profile-authority")
	effects := applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventTranscriptionCompleted, ItemID: "turn-profile-authority", Text: "renewal question",
	}))
	turn := effects[0].Transcript.Turn
	applyOK(t, reactor, currentVoice(reactor, backgroundToolEvent(
		epoch, "response-profile-authority", "call-profile-authority", json.RawMessage(`{"objective":"research"}`),
	)))
	effects = applyOK(t, reactor, TurnCommittedInput{Sequence: 1, Turn: turn, Transcript: "renewal question"})
	require.Len(t, effects, 1)
	require.NotNil(t, effects[0].Handoff)
	want, ok := runtime.HandoffProfileSnapshot("renewal question")
	require.True(t, ok)
	require.Equal(t, want, effects[0].Handoff.Profile)
	require.Empty(t, effects[0].Handoff.RejectReason)
}

func finishProviderVADTurn(t *testing.T, reactor *Reactor, itemID, transcript string) []Effect {
	t.Helper()
	applyOK(t, reactor, currentVoice(reactor, realtime.Event{Kind: realtime.EventSpeechStarted, ItemID: itemID}))
	applyOK(t, reactor, currentVoice(reactor, realtime.Event{Kind: realtime.EventInputAudioCommitted, ItemID: itemID}))
	applyOK(t, reactor, currentVoice(reactor, realtime.Event{Kind: realtime.EventSpeechStopped, ItemID: itemID}))
	return applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventTranscriptionCompleted, ItemID: itemID, Text: transcript,
	}))
}

func TestPromptRenderCompletionCreatesResponseOnlyForExactCurrentTurn(t *testing.T) {
	selected := profileWithTurn(t, "Answer the current request: {{ transcript }}")
	runtime, err := prepareProfileRuntime(t.Context(), selected, prompttmpl.NewMiniJinja())
	require.NoError(t, err)
	reactor, fence := providerVADTestReactor(4)
	reactor.configureProfileBootstrap(runtime, false)
	reactor.profileBootstrapComplete = true

	effects := finishProviderVADTurn(t, reactor, "turn-a", "quote details")
	require.Len(t, effects, 1)
	require.NotNil(t, effects[0].PromptRender)
	request := *effects[0].PromptRender
	require.Equal(t, identity.TurnKey{ID: "turn-a", Revision: 1}, request.Turn)
	require.Equal(t, "quote details", request.Transcript)
	require.Equal(t, fence.Current(), request.Epoch)
	require.Equal(t, selected.Snapshot.SnapshotHash, request.SnapshotHash)

	completed := PromptRenderCompletedInput{
		Incarnation: request.Incarnation, SnapshotHash: request.SnapshotHash,
		Epoch: request.Epoch, InputRevision: request.InputRevision, Turn: request.Turn,
		Instructions: "rendered turn instruction",
	}
	created := applyOK(t, reactor, completed)
	require.Len(t, created, 1)
	require.NotNil(t, created[0].VoiceCommand)
	require.Equal(t, realtime.CommandCreateResponse, created[0].VoiceCommand.Kind)
	require.Equal(t, "rendered turn instruction", created[0].VoiceCommand.Response.Instructions)
	require.Empty(t, applyOK(t, reactor, completed), "duplicate completion created a second response")
}

func TestLateTurnRenderCannotCreateResponseAfterBarge(t *testing.T) {
	selected := profileWithTurn(t, "Answer the current request: {{ transcript }}")
	runtime, err := prepareProfileRuntime(t.Context(), selected, prompttmpl.NewMiniJinja())
	require.NoError(t, err)
	reactor, fence := providerVADTestReactor(7)
	reactor.configureProfileBootstrap(runtime, false)
	reactor.profileBootstrapComplete = true

	effects := finishProviderVADTurn(t, reactor, "turn-old", "old request")
	require.NotNil(t, effects[0].PromptRender)
	request := *effects[0].PromptRender
	oldEpoch := request.Epoch
	applyOK(t, reactor, currentVoice(reactor, realtime.Event{Kind: realtime.EventSpeechStarted, ItemID: "turn-new"}))
	require.Greater(t, fence.Current(), oldEpoch)

	completed := PromptRenderCompletedInput{
		Incarnation: request.Incarnation, SnapshotHash: request.SnapshotHash,
		Epoch: request.Epoch, InputRevision: request.InputRevision, Turn: request.Turn,
		Instructions: "must never be spoken",
	}
	require.Empty(t, applyOK(t, reactor, completed))
}

func TestProfileTurnTemplateSuppressesProviderAutomaticResponse(t *testing.T) {
	selected := profileWithTurn(t, "Answer: {{ transcript }}")
	runtime, err := prepareProfileRuntime(t.Context(), selected, prompttmpl.NewMiniJinja())
	require.NoError(t, err)
	reactor, _ := providerVADTestReactor(3)
	reactor.configureProfileBootstrap(runtime, false)
	reactor.profileBootstrapComplete = true

	effects := applyOK(t, reactor, currentVoice(reactor, realtime.Event{
		Kind: realtime.EventResponseCreated, ResponseID: "automatic-response", Status: "in_progress", NeedsEpochBinding: true,
	}))
	require.Len(t, effects, 3)
	require.Equal(t, realtime.CommandBindResponseEpoch, effects[0].VoiceCommand.Kind)
	require.NotZero(t, effects[1].FlushBefore)
	require.Equal(t, realtime.CommandCancelResponse, effects[2].VoiceCommand.Kind)
	for _, effect := range effects {
		require.Nil(t, effect.ClientEvent, "automatic provider response crossed the profile render gate")
	}
}

func TestPromptRenderWrongSnapshotAndFailureStayContentFree(t *testing.T) {
	selected := profileWithTurn(t, "Answer the current request: {{ transcript }}")
	runtime, err := prepareProfileRuntime(t.Context(), selected, prompttmpl.NewMiniJinja())
	require.NoError(t, err)
	reactor, _ := providerVADTestReactor(2)
	reactor.configureProfileBootstrap(runtime, false)
	reactor.profileBootstrapComplete = true
	request := *finishProviderVADTurn(t, reactor, "turn-a", "private customer request")[0].PromptRender

	wrong := PromptRenderCompletedInput{
		Incarnation: request.Incarnation, SnapshotHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Epoch: request.Epoch, InputRevision: request.InputRevision, Turn: request.Turn,
		Instructions: "must not run",
	}
	require.Empty(t, applyOK(t, reactor, wrong))

	failed := PromptRenderCompletedInput{
		Incarnation: request.Incarnation, SnapshotHash: request.SnapshotHash,
		Epoch: request.Epoch, InputRevision: request.InputRevision, Turn: request.Turn,
		ErrClass: "render_limit_exceeded",
	}
	effects := applyOK(t, reactor, failed)
	require.Len(t, effects, 1)
	require.Equal(t, "live.session.degraded", effects[0].ClientEvent.EventType())
	encoded, err := protocol.Encode(effects[0].ClientEvent)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "private customer request")
	require.NotContains(t, string(encoded), "must not run")
	var payload map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(encoded, &payload))
	require.JSONEq(t, `{"code":"render_limit_exceeded"}`, string(payload["details"]))
}

func TestProfileRenderDispatcherClonesAndRendersFinalizedTranscript(t *testing.T) {
	selected := profileWithTurn(t, "Answer: {{ transcript }}")
	runtime, err := prepareProfileRuntime(t.Context(), selected, prompttmpl.NewMiniJinja())
	require.NoError(t, err)
	dispatcher, err := NewProfileRenderDispatcher(prompttmpl.NewMiniJinja(), 1, 2, prompttmpl.DefaultLimits())
	require.NoError(t, err)
	t.Cleanup(func() { closeProfileRenderer(t, dispatcher) })

	request := PromptRenderRequestedEffect{
		Incarnation: 1, SnapshotHash: selected.Snapshot.SnapshotHash, Epoch: 3,
		InputRevision: 1, Turn: identity.TurnKey{ID: "turn-a", Revision: 1}, Transcript: "quote details",
	}
	completed := make(chan PromptRenderCompletedInput, 1)
	require.NoError(t, dispatcher.Dispatch(t.Context(), runtime, request, func(_ context.Context, input PromptRenderCompletedInput) error {
		completed <- input
		return nil
	}))
	request.Transcript = "mutated after admission"
	result := <-completed
	require.Empty(t, result.ErrClass)
	require.Equal(t, "Answer: quote details", result.Instructions)
	require.Equal(t, "turn-a", result.Turn.ID)
}

func TestProfileRenderDispatcherQueueIsBoundedAndNonblocking(t *testing.T) {
	selected := profileWithTurn(t, "Answer: {{ transcript }}")
	runtime, err := prepareProfileRuntime(t.Context(), selected, prompttmpl.NewMiniJinja())
	require.NoError(t, err)
	engine := &gatedProfileEngine{engine: prompttmpl.NewMiniJinja(), entered: make(chan struct{}), gate: make(chan struct{})}
	dispatcher, err := NewProfileRenderDispatcher(engine, 1, 1, prompttmpl.DefaultLimits())
	require.NoError(t, err)
	t.Cleanup(func() {
		close(engine.gate)
		closeProfileRenderer(t, dispatcher)
	})
	completion := func(context.Context, PromptRenderCompletedInput) error { return nil }
	request := PromptRenderRequestedEffect{Incarnation: 1, SnapshotHash: selected.Snapshot.SnapshotHash, Epoch: 1,
		InputRevision: 1, Turn: identity.TurnKey{ID: "one", Revision: 1}, Transcript: "one"}
	require.NoError(t, dispatcher.Dispatch(t.Context(), runtime, request, completion))
	<-engine.entered
	request.Turn = identity.TurnKey{ID: "two", Revision: 2}
	request.InputRevision = 2
	require.NoError(t, dispatcher.Dispatch(t.Context(), runtime, request, completion))
	request.Turn = identity.TurnKey{ID: "three", Revision: 3}
	request.InputRevision = 3
	started := time.Now()
	err = dispatcher.Dispatch(t.Context(), runtime, request, completion)
	require.ErrorIs(t, err, ErrProfileRenderQueueFull)
	require.Less(t, time.Since(started), 100*time.Millisecond)
}

func TestProfileRenderDispatcherClassifiesCancellationWithoutPromptContent(t *testing.T) {
	selected := profileWithTurn(t, "Answer: {{ transcript }}")
	runtime, err := prepareProfileRuntime(t.Context(), selected, prompttmpl.NewMiniJinja())
	require.NoError(t, err)
	engine := &gatedProfileEngine{engine: prompttmpl.NewMiniJinja(), entered: make(chan struct{}), gate: make(chan struct{})}
	dispatcher, err := NewProfileRenderDispatcher(engine, 1, 1, prompttmpl.DefaultLimits())
	require.NoError(t, err)
	t.Cleanup(func() { closeProfileRenderer(t, dispatcher) })
	ctx, cancel := context.WithCancel(t.Context())
	completed := make(chan PromptRenderCompletedInput, 1)
	request := PromptRenderRequestedEffect{Incarnation: 1, SnapshotHash: selected.Snapshot.SnapshotHash, Epoch: 1,
		InputRevision: 1, Turn: identity.TurnKey{ID: "turn", Revision: 1}, Transcript: "private customer request"}
	require.NoError(t, dispatcher.Dispatch(ctx, runtime, request, func(_ context.Context, input PromptRenderCompletedInput) error {
		completed <- input
		return nil
	}))
	<-engine.entered
	cancel()
	result := <-completed
	require.Equal(t, "render_cancelled", result.ErrClass)
	require.Empty(t, result.Instructions)
}

func TestProfileTurnTemplateRejectsClientResponseCreation(t *testing.T) {
	selected := profileWithTurn(t, "Answer: {{ transcript }}")
	runtime, err := prepareProfileRuntime(t.Context(), selected, prompttmpl.NewMiniJinja())
	require.NoError(t, err)
	reactor, _ := providerVADTestReactor(1)
	reactor.configureProfileBootstrap(runtime, false)
	reactor.profileBootstrapComplete = true

	effects, err := reactor.Apply(ClientInput{Event: protocol.ResponseCreateEvent{Type: "response.create"}})
	require.ErrorIs(t, err, ErrUnsupportedField)
	require.Empty(t, effects)
}

func TestProfileTurnTemplateKeepsAutomaticProviderResponseDisabled(t *testing.T) {
	selected := profileWithTurn(t, "Answer: {{ transcript }}")
	runtime, err := prepareProfileRuntime(t.Context(), selected, prompttmpl.NewMiniJinja())
	require.NoError(t, err)
	reactor, _ := providerVADTestReactor(1)
	reactor.configureProfileBootstrap(runtime, false)
	reactor.profileBootstrapComplete = true

	effects := applyOK(t, reactor, ClientInput{Event: inputTurnSessionUpdate(json.RawMessage(
		`{"type":"server_vad","create_response":true}`,
	))})
	require.Len(t, effects, 1)
	require.NotNil(t, effects[0].VoiceCommand)
	require.JSONEq(t, `{"type":"server_vad","create_response":false}`, string(effects[0].VoiceCommand.Session.TurnDetection))
}

func closeProfileRenderer(t *testing.T, dispatcher *ProfileRenderDispatcher) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, dispatcher.Close(ctx))
}

func TestBlockedTurnRenderCannotDelaySpeechStartFence(t *testing.T) {
	selected := profileWithTurn(t, "Answer: {{ transcript }}")
	engine := &gatedProfileEngine{engine: prompttmpl.NewMiniJinja(), entered: make(chan struct{}), gate: make(chan struct{})}
	dispatcher, err := NewProfileRenderDispatcher(engine, 1, 2, prompttmpl.DefaultLimits())
	require.NoError(t, err)
	connector := testkit.NewFakeConnector(32)
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
		InitialSession: realtime.SessionConfig{TurnDetection: json.RawMessage(`{"type":"server_vad"}`)},
		ProfileEngine:  prompttmpl.NewMiniJinja(), ProfileRenderer: dispatcher,
	}, connector, managerIDs{session: "profile-render", conversation: "profile-render"})
	client := newChannelClient(32)
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
	require.JSONEq(t, `{"type":"server_vad","create_response":false}`, string(initial.Session.TurnDetection))
	require.Equal(t, "session.created", (<-client.writes).EventType())
	require.Equal(t, "live.profile.selected", (<-client.writes).EventType())
	require.NoError(t, voice.Emit(t.Context(), realtime.Event{
		Kind: realtime.EventSessionUpdated, InputTurnMode: realtime.InputTurnModeProviderVAD,
		InputTurnModeSpecified: true, SessionUpdateAcknowledgement: true,
	}))

	for _, event := range []realtime.Event{
		{Kind: realtime.EventSpeechStarted, ItemID: "turn-a"},
		{Kind: realtime.EventInputAudioCommitted, ItemID: "turn-a"},
		{Kind: realtime.EventSpeechStopped, ItemID: "turn-a"},
		{Kind: realtime.EventTranscriptionCompleted, ItemID: "turn-a", Text: "private request"},
	} {
		require.NoError(t, voice.Emit(t.Context(), event))
	}
	select {
	case <-engine.entered:
	case <-time.After(time.Second):
		t.Fatal("turn render was not dispatched")
	}
	require.NoError(t, voice.Emit(t.Context(), realtime.Event{Kind: realtime.EventSpeechStarted, ItemID: "turn-b"}))
	deadline := time.After(time.Second)
	for {
		select {
		case event := <-client.writes:
			if started, ok := event.(protocol.InputAudioBufferSpeechStartedEvent); ok && started.ItemID == "turn-b" {
				goto fenced
			}
		case <-deadline:
			t.Fatal("speech-start fence waited for prompt rendering")
		}
	}

fenced:
	close(engine.gate)
	short, shortCancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	_, err = voice.NextCommand(short)
	shortCancel()
	require.ErrorIs(t, err, context.DeadlineExceeded, "late render created a response after barge-in")
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	closeProfileRenderer(t, dispatcher)
}
