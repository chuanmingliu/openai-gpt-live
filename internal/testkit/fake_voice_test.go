package testkit_test

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"testing"

	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/chuanmingliu/openai-gpt-live/internal/testkit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewFakeConnectorRejectsNonPositiveBuffer(t *testing.T) {
	for _, buffer := range []int{0, -1} {
		t.Run(strconv.Itoa(buffer), func(t *testing.T) {
			assert.Panics(t, func() {
				testkit.NewFakeConnector(buffer)
			})
		})
	}
}

func TestFakeConnectorHonorsConnectCancellation(t *testing.T) {
	fake := testkit.NewFakeConnector(1)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := fake.Connect(ctx, realtime.OpenRequest{SessionID: identity.SessionID("cancelled")})
	require.ErrorIs(t, err, context.Canceled)

	nextCtx, cancelNext := context.WithCancel(t.Context())
	cancelNext()
	_, err = fake.NextSession(nextCtx)
	require.ErrorIs(t, err, context.Canceled, "a cancelled connect must not publish a session")
}

func TestFakeConnectorHonorsCancellationWhileBackpressured(t *testing.T) {
	fake := testkit.NewFakeConnector(1)
	_, err := fake.Connect(t.Context(), realtime.OpenRequest{SessionID: identity.SessionID("first")})
	require.NoError(t, err)

	ctx, cancel, observed := newObservedCancelContext(t.Context())
	result := make(chan error, 1)
	go func() {
		_, connectErr := fake.Connect(ctx, realtime.OpenRequest{SessionID: identity.SessionID("second")})
		result <- connectErr
	}()
	waitUntilBlocked(t, observed, result)
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)

	_, err = fake.NextSession(t.Context())
	require.NoError(t, err)
	nextCtx, cancelNext := context.WithCancel(t.Context())
	cancelNext()
	_, err = fake.NextSession(nextCtx)
	require.ErrorIs(t, err, context.Canceled, "the cancelled connect must not leak into the session queue")
}

func TestFakeVoicePreservesCommandOrder(t *testing.T) {
	client, server := connectSession(t, 4)
	require.NoError(t, client.Send(t.Context(), realtime.Command{
		Kind:     realtime.CommandAppendAudio,
		Sequence: 1,
	}))
	require.NoError(t, client.Send(t.Context(), realtime.Command{
		Kind:     realtime.CommandCommitAudio,
		AfterSeq: 1,
	}))

	appendCommand, err := server.NextCommand(t.Context())
	require.NoError(t, err)
	commitCommand, err := server.NextCommand(t.Context())
	require.NoError(t, err)
	assert.Equal(t, realtime.CommandAppendAudio, appendCommand.Kind)
	assert.Equal(t, uint64(1), appendCommand.Sequence)
	assert.Equal(t, realtime.CommandCommitAudio, commitCommand.Kind)
	assert.Equal(t, uint64(1), commitCommand.AfterSeq)
}

func TestFakeVoicePreservesEventOrder(t *testing.T) {
	client, server := connectSession(t, 4)
	require.NoError(t, server.Emit(t.Context(), realtime.Event{
		Kind: realtime.EventTranscriptionDelta,
		Text: "first",
	}))
	require.NoError(t, server.Emit(t.Context(), realtime.Event{
		Kind: realtime.EventTranscriptionCompleted,
		Text: "second",
	}))

	first, err := client.Receive(t.Context())
	require.NoError(t, err)
	second, err := client.Receive(t.Context())
	require.NoError(t, err)
	assert.Equal(t, realtime.EventTranscriptionDelta, first.Kind)
	assert.Equal(t, "first", first.Text)
	assert.Equal(t, realtime.EventTranscriptionCompleted, second.Kind)
	assert.Equal(t, "second", second.Text)
}

func TestFakeVoiceInjectsTerminalFailure(t *testing.T) {
	client, server := connectSession(t, 1)
	ctx, _, observed := newObservedCancelContext(t.Context())
	receiveResult := make(chan error, 1)
	go func() {
		_, err := client.Receive(ctx)
		receiveResult <- err
	}()
	waitUntilBlocked(t, observed, receiveResult)

	injected := errors.New("provider read failed")
	server.Fail(injected)
	require.ErrorIs(t, <-receiveResult, injected)

	require.ErrorIs(t, client.Send(t.Context(), realtime.Command{}), injected)
	_, err := client.Receive(t.Context())
	require.ErrorIs(t, err, injected)
	require.ErrorIs(t, server.Emit(t.Context(), realtime.Event{}), injected)
	_, err = server.NextCommand(t.Context())
	require.ErrorIs(t, err, injected)

	server.Fail(errors.New("later failure must not replace the first"))
	_, err = client.Receive(t.Context())
	require.ErrorIs(t, err, injected)
	require.NoError(t, client.Close(t.Context()))
}

func TestFakeVoiceCloseIsConcurrentSafeAndIdempotent(t *testing.T) {
	client, server := connectSession(t, 2)
	const closers = 32
	start := make(chan struct{})
	results := make(chan error, closers)
	for range closers {
		go func() {
			<-start
			results <- client.Close(t.Context())
		}()
	}
	close(start)
	for range closers {
		require.NoError(t, <-results)
	}
	require.NoError(t, client.Close(t.Context()))

	require.ErrorIs(t, client.Send(t.Context(), realtime.Command{}), testkit.ErrFakeVoiceClosed)
	_, err := client.Receive(t.Context())
	require.ErrorIs(t, err, testkit.ErrFakeVoiceClosed)
	require.ErrorIs(t, server.Emit(t.Context(), realtime.Event{}), testkit.ErrFakeVoiceClosed)
	_, err = server.NextCommand(t.Context())
	require.ErrorIs(t, err, testkit.ErrFakeVoiceClosed)
}

func TestFakeVoiceCloseUnblocksSessionOperations(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*testing.T, realtime.Session, *testkit.FakeVoiceSession)
		run     func(context.Context, realtime.Session, *testkit.FakeVoiceSession) error
	}{
		{
			name: "send",
			prepare: func(t *testing.T, client realtime.Session, _ *testkit.FakeVoiceSession) {
				require.NoError(t, client.Send(t.Context(), realtime.Command{Sequence: 1}))
			},
			run: func(ctx context.Context, client realtime.Session, _ *testkit.FakeVoiceSession) error {
				return client.Send(ctx, realtime.Command{Sequence: 2})
			},
		},
		{
			name: "receive",
			run: func(ctx context.Context, client realtime.Session, _ *testkit.FakeVoiceSession) error {
				_, err := client.Receive(ctx)
				return err
			},
		},
		{
			name: "emit",
			prepare: func(t *testing.T, _ realtime.Session, server *testkit.FakeVoiceSession) {
				require.NoError(t, server.Emit(t.Context(), realtime.Event{Text: "first"}))
			},
			run: func(ctx context.Context, _ realtime.Session, server *testkit.FakeVoiceSession) error {
				return server.Emit(ctx, realtime.Event{Text: "second"})
			},
		},
		{
			name: "next command",
			run: func(ctx context.Context, _ realtime.Session, server *testkit.FakeVoiceSession) error {
				_, err := server.NextCommand(ctx)
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, server := connectSession(t, 1)
			if tt.prepare != nil {
				tt.prepare(t, client, server)
			}
			ctx, _, observed := newObservedCancelContext(t.Context())
			result := make(chan error, 1)
			go func() {
				result <- tt.run(ctx, client, server)
			}()
			waitUntilBlocked(t, observed, result)

			require.NoError(t, client.Close(t.Context()))
			require.ErrorIs(t, <-result, testkit.ErrFakeVoiceClosed)
		})
	}
}

func TestFakeVoiceOperationsHonorContextCancellation(t *testing.T) {
	t.Run("send under backpressure", func(t *testing.T) {
		client, server := connectSession(t, 1)
		require.NoError(t, client.Send(t.Context(), realtime.Command{Sequence: 1}))

		ctx, cancel, observed := newObservedCancelContext(t.Context())
		result := make(chan error, 1)
		go func() {
			result <- client.Send(ctx, realtime.Command{Sequence: 2})
		}()
		waitUntilBlocked(t, observed, result)
		cancel()
		require.ErrorIs(t, <-result, context.Canceled)

		command, err := server.NextCommand(t.Context())
		require.NoError(t, err)
		assert.Equal(t, uint64(1), command.Sequence)
		cancelled, cancelNext := context.WithCancel(t.Context())
		cancelNext()
		_, err = server.NextCommand(cancelled)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("emit under backpressure", func(t *testing.T) {
		client, server := connectSession(t, 1)
		require.NoError(t, server.Emit(t.Context(), realtime.Event{Text: "first"}))

		ctx, cancel, observed := newObservedCancelContext(t.Context())
		result := make(chan error, 1)
		go func() {
			result <- server.Emit(ctx, realtime.Event{Text: "second"})
		}()
		waitUntilBlocked(t, observed, result)
		cancel()
		require.ErrorIs(t, <-result, context.Canceled)

		event, err := client.Receive(t.Context())
		require.NoError(t, err)
		assert.Equal(t, "first", event.Text)
		cancelled, cancelNext := context.WithCancel(t.Context())
		cancelNext()
		_, err = client.Receive(cancelled)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("receive", func(t *testing.T) {
		client, _ := connectSession(t, 1)
		ctx, cancel, observed := newObservedCancelContext(t.Context())
		result := make(chan error, 1)
		go func() {
			_, err := client.Receive(ctx)
			result <- err
		}()
		waitUntilBlocked(t, observed, result)
		cancel()
		require.ErrorIs(t, <-result, context.Canceled)
	})

	t.Run("next command", func(t *testing.T) {
		_, server := connectSession(t, 1)
		ctx, cancel, observed := newObservedCancelContext(t.Context())
		result := make(chan error, 1)
		go func() {
			_, err := server.NextCommand(ctx)
			result <- err
		}()
		waitUntilBlocked(t, observed, result)
		cancel()
		require.ErrorIs(t, <-result, context.Canceled)
	})

	t.Run("next session", func(t *testing.T) {
		fake := testkit.NewFakeConnector(1)
		ctx, cancel, observed := newObservedCancelContext(t.Context())
		result := make(chan error, 1)
		go func() {
			_, err := fake.NextSession(ctx)
			result <- err
		}()
		waitUntilBlocked(t, observed, result)
		cancel()
		require.ErrorIs(t, <-result, context.Canceled)
	})
}

func TestFakeVoiceCopiesCommandsAcrossBoundaries(t *testing.T) {
	t.Run("sender mutation cannot change queued command", func(t *testing.T) {
		client, server := connectSession(t, 2)
		command := commandFixture()
		want := commandFixture()
		require.NoError(t, client.Send(t.Context(), command))

		mutateCommand(&command)
		got, err := server.NextCommand(t.Context())
		require.NoError(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("receiver mutation cannot change sender command or another queued command", func(t *testing.T) {
		client, server := connectSession(t, 2)
		command := commandFixture()
		want := commandFixture()
		require.NoError(t, client.Send(t.Context(), command))
		require.NoError(t, client.Send(t.Context(), command))

		first, err := server.NextCommand(t.Context())
		require.NoError(t, err)
		mutateCommand(&first)
		assert.Equal(t, want, command)
		second, err := server.NextCommand(t.Context())
		require.NoError(t, err)
		assert.Equal(t, want, second)
	})
}

func TestFakeVoiceCopiesEventsAcrossBoundaries(t *testing.T) {
	t.Run("emitter mutation cannot change queued event", func(t *testing.T) {
		client, server := connectSession(t, 2)
		event := eventFixture()
		want := eventFixture()
		require.NoError(t, server.Emit(t.Context(), event))

		mutateEvent(&event)
		got, err := client.Receive(t.Context())
		require.NoError(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("receiver mutation cannot change emitter event or another queued event", func(t *testing.T) {
		client, server := connectSession(t, 2)
		event := eventFixture()
		want := eventFixture()
		require.NoError(t, server.Emit(t.Context(), event))
		require.NoError(t, server.Emit(t.Context(), event))

		first, err := client.Receive(t.Context())
		require.NoError(t, err)
		mutateEvent(&first)
		assert.Equal(t, want, event)
		second, err := client.Receive(t.Context())
		require.NoError(t, err)
		assert.Equal(t, want, second)
	})
}

func connectSession(t *testing.T, buffer int) (realtime.Session, *testkit.FakeVoiceSession) {
	t.Helper()
	fake := testkit.NewFakeConnector(buffer)
	client, err := fake.Connect(t.Context(), realtime.OpenRequest{
		SessionID:   identity.SessionID("s1"),
		Incarnation: identity.Incarnation(2),
		Model:       "voice-model",
	})
	require.NoError(t, err)
	server, err := fake.NextSession(t.Context())
	require.NoError(t, err)
	return client, server
}

type observedDoneContext struct {
	context.Context
	once     sync.Once
	observed chan struct{}
}

func (c *observedDoneContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.observed) })
	return c.Context.Done()
}

func newObservedCancelContext(parent context.Context) (context.Context, context.CancelFunc, <-chan struct{}) {
	ctx, cancel := context.WithCancel(parent)
	observed := make(chan struct{})
	return &observedDoneContext{Context: ctx, observed: observed}, cancel, observed
}

func waitUntilBlocked(t *testing.T, observed <-chan struct{}, result <-chan error) {
	t.Helper()
	<-observed
	select {
	case err := <-result:
		t.Fatalf("operation completed instead of blocking: %v", err)
	default:
	}
}

func commandFixture() realtime.Command {
	return realtime.Command{
		Kind:     realtime.CommandCreateResponse,
		Sequence: 9,
		AfterSeq: 8,
		Epoch:    identity.ResponseEpoch(7),
		Session: realtime.SessionConfig{
			Instructions: "be concise",
			InputFormat: realtime.AudioFormat{
				Encoding:   "pcm16le",
				SampleRate: 24_000,
				Channels:   1,
			},
			OutputFormat: realtime.AudioFormat{
				Encoding:   "pcm16le",
				SampleRate: 24_000,
				Channels:   1,
			},
			Voice: "alloy",
			Tools: []realtime.Tool{{
				Type:        "function",
				Name:        "lookup",
				Description: "look something up",
				Parameters:  []byte(`{"type":"object"}`),
			}},
			ToolChoice:    []byte(`{"type":"auto"}`),
			TurnDetection: []byte(`{"type":"server_vad"}`),
		},
		Audio: []byte{1, 2, 3},
		Item:  []byte(`{"type":"message"}`),
		Response: realtime.ResponseConfig{
			Instructions: "answer now",
			ToolChoice:   []byte(`{"type":"required"}`),
			Metadata: map[string]json.RawMessage{
				"source": []byte(`"test"`),
			},
			Input: []byte(`[{"type":"text"}]`),
		},
		ResponseID: "response-1",
	}
}

func mutateCommand(command *realtime.Command) {
	command.Audio[0] = 99
	command.Item[0] = 'x'
	command.Session.Tools[0].Name = "changed"
	command.Session.Tools[0].Parameters[0] = 'x'
	command.Session.ToolChoice[0] = 'x'
	command.Session.TurnDetection[0] = 'x'
	command.Response.ToolChoice[0] = 'x'
	command.Response.Metadata["source"][0] = 'x'
	command.Response.Metadata["added"] = []byte(`true`)
	command.Response.Input[0] = 'x'
}

func eventFixture() realtime.Event {
	return realtime.Event{
		Kind:              realtime.EventFunctionCallDone,
		Epoch:             identity.ResponseEpoch(11),
		ProviderEventID:   "event-1",
		ResponseID:        "response-1",
		ItemID:            "item-1",
		CallID:            "call-1",
		Name:              "lookup",
		Audio:             []byte{4, 5, 6},
		Text:              "done",
		Arguments:         []byte(`{"query":"weather"}`),
		Status:            "completed",
		Reason:            "stop",
		Payload:           []byte(`{"provider":"value"}`),
		NeedsEpochBinding: true,
	}
}

func mutateEvent(event *realtime.Event) {
	event.Audio[0] = 99
	event.Arguments[0] = 'x'
	event.Payload[0] = 'x'
}
