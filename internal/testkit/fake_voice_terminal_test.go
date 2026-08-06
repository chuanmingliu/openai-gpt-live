package testkit

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFakeVoiceTerminalWinsReadyIO(t *testing.T) {
	injected := errors.New("injected terminal failure")
	terminals := []struct {
		name      string
		want      error
		terminate func(*testing.T, realtime.Session, *FakeVoiceSession)
	}{
		{
			name: "close",
			want: ErrFakeVoiceClosed,
			terminate: func(t *testing.T, client realtime.Session, _ *FakeVoiceSession) {
				require.NoError(t, client.Close(t.Context()))
			},
		},
		{
			name: "fail",
			want: injected,
			terminate: func(_ *testing.T, _ realtime.Session, server *FakeVoiceSession) {
				server.Fail(injected)
			},
		},
	}
	operations := []struct {
		name    string
		prepare func(*testing.T, realtime.Session, *FakeVoiceSession)
		run     func(*testing.T, context.Context, realtime.Session, *FakeVoiceSession) error
		checkIO func(*testing.T, *FakeVoiceSession)
	}{
		{
			name: "send",
			run: func(_ *testing.T, ctx context.Context, client realtime.Session, _ *FakeVoiceSession) error {
				return client.Send(ctx, realtime.Command{Sequence: 41})
			},
			checkIO: func(t *testing.T, server *FakeVoiceSession) {
				assert.Len(t, server.commands, 1, "the ready send branch must have been selected")
			},
		},
		{
			name: "receive",
			prepare: func(t *testing.T, _ realtime.Session, server *FakeVoiceSession) {
				require.NoError(t, server.Emit(t.Context(), realtime.Event{Text: "queued"}))
			},
			run: func(t *testing.T, ctx context.Context, client realtime.Session, _ *FakeVoiceSession) error {
				event, err := client.Receive(ctx)
				assert.Equal(t, realtime.Event{}, event, "an event consumed during the terminal race must not escape")
				return err
			},
			checkIO: func(t *testing.T, server *FakeVoiceSession) {
				assert.Empty(t, server.events, "the ready receive branch must have been selected")
			},
		},
		{
			name: "emit",
			run: func(_ *testing.T, ctx context.Context, _ realtime.Session, server *FakeVoiceSession) error {
				return server.Emit(ctx, realtime.Event{Text: "racing"})
			},
			checkIO: func(t *testing.T, server *FakeVoiceSession) {
				assert.Len(t, server.events, 1, "the ready emit branch must have been selected")
			},
		},
		{
			name: "next command",
			prepare: func(t *testing.T, client realtime.Session, _ *FakeVoiceSession) {
				require.NoError(t, client.Send(t.Context(), realtime.Command{Sequence: 43}))
			},
			run: func(t *testing.T, ctx context.Context, _ realtime.Session, server *FakeVoiceSession) error {
				command, err := server.NextCommand(ctx)
				assert.Equal(t, realtime.Command{}, command, "a command consumed during the terminal race must not escape")
				return err
			},
			checkIO: func(t *testing.T, server *FakeVoiceSession) {
				assert.Empty(t, server.commands, "the ready command branch must have been selected")
			},
		},
	}

	for _, terminal := range terminals {
		for _, operation := range operations {
			t.Run(terminal.name+"/"+operation.name, func(t *testing.T) {
				client, server := newTerminalRaceSession(t)
				if operation.prepare != nil {
					operation.prepare(t, client, server)
				}

				ctx := &terminalDuringSelectContext{
					Context: t.Context(),
					transition: func() {
						// The select has already captured the original done channel.
						// Move the terminal notification to a new channel so only the
						// ready I/O branch can commit after termination completes.
						server.done = make(chan struct{})
						terminal.terminate(t, client, server)
					},
				}

				err := operation.run(t, ctx, client, server)
				require.ErrorIs(t, err, terminal.want)
				operation.checkIO(t, server)
				assertSessionTerminal(t, client, server, terminal.want)
			})
		}
	}
}

type terminalDuringSelectContext struct {
	context.Context
	once       sync.Once
	transition func()
}

func (c *terminalDuringSelectContext) Done() <-chan struct{} {
	c.once.Do(c.transition)
	return nil
}

func newTerminalRaceSession(t *testing.T) (realtime.Session, *FakeVoiceSession) {
	t.Helper()
	connector := NewFakeConnector(1)
	client, err := connector.Connect(t.Context(), realtime.OpenRequest{})
	require.NoError(t, err)
	server, err := connector.NextSession(t.Context())
	require.NoError(t, err)
	return client, server
}

func assertSessionTerminal(
	t *testing.T,
	client realtime.Session,
	server *FakeVoiceSession,
	want error,
) {
	t.Helper()
	require.ErrorIs(t, client.Send(t.Context(), realtime.Command{}), want)
	_, err := client.Receive(t.Context())
	require.ErrorIs(t, err, want)
	require.ErrorIs(t, server.Emit(t.Context(), realtime.Event{}), want)
	_, err = server.NextCommand(t.Context())
	require.ErrorIs(t, err, want)
}
