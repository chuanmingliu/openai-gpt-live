package testkit

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
)

var ErrFakeVoiceClosed = errors.New("testkit: fake voice session closed")

type FakeConnector struct {
	buffer   int
	sessions chan *FakeVoiceSession
}

func NewFakeConnector(buffer int) *FakeConnector {
	if buffer <= 0 {
		panic("testkit: fake voice buffer must be positive")
	}
	return &FakeConnector{
		buffer:   buffer,
		sessions: make(chan *FakeVoiceSession, buffer),
	}
}

func (f *FakeConnector) Connect(ctx context.Context, _ realtime.OpenRequest) (realtime.Session, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	session := newFakeVoiceSession(f.buffer)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case f.sessions <- session:
		return session, nil
	}
}

func (f *FakeConnector) NextSession(ctx context.Context) (*FakeVoiceSession, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case session := <-f.sessions:
		return session, nil
	}
}

type FakeVoiceSession struct {
	commands chan realtime.Command
	events   chan realtime.Event
	done     chan struct{}

	closeOnce sync.Once
	errMu     sync.RWMutex
	err       error
}

func newFakeVoiceSession(buffer int) *FakeVoiceSession {
	return &FakeVoiceSession{
		commands: make(chan realtime.Command, buffer),
		events:   make(chan realtime.Event, buffer),
		done:     make(chan struct{}),
	}
}

func (s *FakeVoiceSession) Send(ctx context.Context, command realtime.Command) error {
	if err := s.terminalError(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	command = cloneCommand(command)
	select {
	case <-s.done:
		return s.terminalError()
	case <-ctx.Done():
		return ctx.Err()
	case s.commands <- command:
		return s.terminalError()
	}
}

func (s *FakeVoiceSession) Receive(ctx context.Context) (realtime.Event, error) {
	if err := s.terminalError(); err != nil {
		return realtime.Event{}, err
	}
	if err := ctx.Err(); err != nil {
		return realtime.Event{}, err
	}
	select {
	case <-s.done:
		return realtime.Event{}, s.terminalError()
	case <-ctx.Done():
		return realtime.Event{}, ctx.Err()
	case event := <-s.events:
		if err := s.terminalError(); err != nil {
			return realtime.Event{}, err
		}
		return cloneEvent(event), nil
	}
}

func (s *FakeVoiceSession) Close(context.Context) error {
	s.terminate(ErrFakeVoiceClosed)
	return nil
}

func (s *FakeVoiceSession) Emit(ctx context.Context, event realtime.Event) error {
	if err := s.terminalError(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	event = cloneEvent(event)
	select {
	case <-s.done:
		return s.terminalError()
	case <-ctx.Done():
		return ctx.Err()
	case s.events <- event:
		return s.terminalError()
	}
}

func (s *FakeVoiceSession) NextCommand(ctx context.Context) (realtime.Command, error) {
	if err := s.terminalError(); err != nil {
		return realtime.Command{}, err
	}
	if err := ctx.Err(); err != nil {
		return realtime.Command{}, err
	}
	select {
	case <-s.done:
		return realtime.Command{}, s.terminalError()
	case <-ctx.Done():
		return realtime.Command{}, ctx.Err()
	case command := <-s.commands:
		if err := s.terminalError(); err != nil {
			return realtime.Command{}, err
		}
		return cloneCommand(command), nil
	}
}

func (s *FakeVoiceSession) Fail(err error) {
	if err == nil {
		panic("testkit: fake voice failure must be non-nil")
	}
	s.terminate(err)
}

func (s *FakeVoiceSession) terminate(err error) {
	s.closeOnce.Do(func() {
		s.errMu.Lock()
		s.err = err
		close(s.done)
		s.errMu.Unlock()
	})
}

func (s *FakeVoiceSession) terminalError() error {
	s.errMu.RLock()
	defer s.errMu.RUnlock()
	return s.err
}

func cloneCommand(command realtime.Command) realtime.Command {
	command.Session = cloneSessionConfig(command.Session)
	command.Audio = cloneBytes(command.Audio)
	command.Item = cloneRawMessage(command.Item)
	command.Response = cloneResponseConfig(command.Response)
	return command
}

func cloneSessionConfig(config realtime.SessionConfig) realtime.SessionConfig {
	if config.Tools != nil {
		tools := make([]realtime.Tool, len(config.Tools))
		copy(tools, config.Tools)
		config.Tools = tools
	}
	for i := range config.Tools {
		config.Tools[i].Parameters = cloneRawMessage(config.Tools[i].Parameters)
	}
	config.ToolChoice = cloneRawMessage(config.ToolChoice)
	config.TurnDetection = cloneRawMessage(config.TurnDetection)
	return config
}

func cloneResponseConfig(config realtime.ResponseConfig) realtime.ResponseConfig {
	config.ToolChoice = cloneRawMessage(config.ToolChoice)
	if config.Metadata != nil {
		metadata := make(map[string]json.RawMessage, len(config.Metadata))
		for key, value := range config.Metadata {
			metadata[key] = cloneRawMessage(value)
		}
		config.Metadata = metadata
	}
	config.Input = cloneRawMessage(config.Input)
	return config
}

func cloneEvent(event realtime.Event) realtime.Event {
	event.Audio = cloneBytes(event.Audio)
	event.Arguments = cloneRawMessage(event.Arguments)
	event.Payload = cloneRawMessage(event.Payload)
	return event
}

func cloneRawMessage(message json.RawMessage) json.RawMessage {
	if message == nil {
		return nil
	}
	cloned := make(json.RawMessage, len(message))
	copy(cloned, message)
	return cloned
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	cloned := make([]byte, len(value))
	copy(cloned, value)
	return cloned
}

var _ realtime.Connector = (*FakeConnector)(nil)
var _ realtime.Session = (*FakeVoiceSession)(nil)
