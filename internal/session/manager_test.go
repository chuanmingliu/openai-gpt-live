package session

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/protocol"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/chuanmingliu/openai-gpt-live/internal/testkit"
	"github.com/stretchr/testify/require"
)

type managerIDs struct {
	session      identity.SessionID
	conversation identity.ConversationID
}

func (i managerIDs) NewSessionID() identity.SessionID           { return i.session }
func (i managerIDs) NewConversationID() identity.ConversationID { return i.conversation }

type channelClient struct {
	reads  chan protocol.ClientEvent
	writes chan protocol.ServerEvent
	done   chan struct{}

	closeOnce sync.Once
}

type gateAudioClient struct {
	*channelClient
	audioStarted chan struct{}
	audioRelease chan struct{}
	startedOnce  sync.Once

	mu            sync.Mutex
	recordedAudio []protocol.ResponseOutputAudioDeltaEvent
}

type lifecycleConnector struct {
	voice     *lifecycleVoice
	connected chan realtime.OpenRequest
}

func newLifecycleConnector() *lifecycleConnector {
	return &lifecycleConnector{
		voice: &lifecycleVoice{
			failure: make(chan error, 1),
			closed:  make(chan struct{}),
			sending: make(chan struct{}),
		},
		connected: make(chan realtime.OpenRequest, 1),
	}
}

func (c *lifecycleConnector) Connect(ctx context.Context, request realtime.OpenRequest) (realtime.Session, error) {
	select {
	case c.connected <- request:
		return c.voice, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type lifecycleVoice struct {
	failure chan error
	closed  chan struct{}
	sending chan struct{}

	closeOnce sync.Once
	sendOnce  sync.Once
	closes    atomic.Int32
	closeLive atomic.Bool
}

func (v *lifecycleVoice) Send(ctx context.Context, _ realtime.Command) error {
	v.sendOnce.Do(func() { close(v.sending) })
	select {
	case <-v.closed:
		return io.ErrClosedPipe
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (v *lifecycleVoice) Receive(ctx context.Context) (realtime.Event, error) {
	select {
	case err := <-v.failure:
		return realtime.Event{}, err
	case <-ctx.Done():
		return realtime.Event{}, ctx.Err()
	}
}

func (v *lifecycleVoice) Close(ctx context.Context) error {
	v.closes.Add(1)
	v.closeLive.Store(ctx.Err() == nil)
	v.closeOnce.Do(func() { close(v.closed) })
	return ctx.Err()
}

type lifecycleClient struct {
	created chan struct{}
	closed  chan struct{}

	createdOnce sync.Once
	closeOnce   sync.Once
	closes      atomic.Int32
}

type blockingWriteClient struct {
	started chan struct{}
	closed  chan struct{}

	startOnce sync.Once
	closeOnce sync.Once
}

type failureConnector struct {
	voice     *failureVoice
	connected chan struct{}
	once      sync.Once
}

func (c *failureConnector) Connect(ctx context.Context, _ realtime.OpenRequest) (realtime.Session, error) {
	c.once.Do(func() { close(c.connected) })
	return c.voice, ctx.Err()
}

type failureVoice struct {
	sendErr    chan error
	receiveErr chan error
	closeErr   error
	closes     atomic.Int32
	closeLive  atomic.Bool
}

func (v *failureVoice) Send(ctx context.Context, _ realtime.Command) error {
	select {
	case err := <-v.sendErr:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (v *failureVoice) Receive(ctx context.Context) (realtime.Event, error) {
	select {
	case err := <-v.receiveErr:
		return realtime.Event{}, err
	case <-ctx.Done():
		return realtime.Event{}, ctx.Err()
	}
}

func (v *failureVoice) Close(ctx context.Context) error {
	v.closes.Add(1)
	v.closeLive.Store(ctx.Err() == nil)
	if err := ctx.Err(); err != nil {
		return err
	}
	return v.closeErr
}

type failureClient struct {
	reads    chan protocol.ClientEvent
	readErr  chan error
	writeErr chan error
	closes   atomic.Int32
	closeErr error
}

type recordingVoice struct {
	sent chan realtime.Command
}

func (v *recordingVoice) Send(ctx context.Context, command realtime.Command) error {
	select {
	case v.sent <- command:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (*recordingVoice) Receive(ctx context.Context) (realtime.Event, error) {
	<-ctx.Done()
	return realtime.Event{}, ctx.Err()
}

func (*recordingVoice) Close(context.Context) error { return nil }

type closeErrorConnector struct {
	voice     *closeErrorVoice
	connected chan struct{}
}

func (c *closeErrorConnector) Connect(context.Context, realtime.OpenRequest) (realtime.Session, error) {
	close(c.connected)
	return c.voice, nil
}

type closeErrorVoice struct{ closeErr error }

func (*closeErrorVoice) Send(context.Context, realtime.Command) error { return nil }
func (*closeErrorVoice) Receive(ctx context.Context) (realtime.Event, error) {
	<-ctx.Done()
	return realtime.Event{}, ctx.Err()
}
func (v *closeErrorVoice) Close(context.Context) error { return v.closeErr }

type closeErrorClient struct {
	closeErr error
	writes   chan string
	closed   chan struct{}
	once     sync.Once
}

func (c *closeErrorClient) Read(ctx context.Context) (protocol.ClientEvent, error) {
	select {
	case <-c.closed:
		return nil, io.EOF
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *closeErrorClient) Write(_ context.Context, event protocol.ServerEvent) error {
	c.writes <- event.EventType()
	return nil
}

func (c *closeErrorClient) Close(int, string) error {
	c.writes <- "client.close"
	c.once.Do(func() { close(c.closed) })
	return c.closeErr
}

func newFailureClient() *failureClient {
	return &failureClient{
		reads: make(chan protocol.ClientEvent, 1), readErr: make(chan error, 1), writeErr: make(chan error, 1),
	}
}

func (c *failureClient) Read(ctx context.Context) (protocol.ClientEvent, error) {
	select {
	case event := <-c.reads:
		return event, nil
	case err := <-c.readErr:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *failureClient) Write(ctx context.Context, _ protocol.ServerEvent) error {
	select {
	case err := <-c.writeErr:
		return err
	default:
		return ctx.Err()
	}
}

func (c *failureClient) Close(int, string) error {
	c.closes.Add(1)
	return c.closeErr
}

func newBlockingWriteClient() *blockingWriteClient {
	return &blockingWriteClient{started: make(chan struct{}), closed: make(chan struct{})}
}

func (c *blockingWriteClient) Read(ctx context.Context) (protocol.ClientEvent, error) {
	select {
	case <-c.closed:
		return nil, io.EOF
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *blockingWriteClient) Write(ctx context.Context, event protocol.ServerEvent) error {
	if event.EventType() == "session.created" {
		return nil
	}
	c.startOnce.Do(func() { close(c.started) })
	<-ctx.Done()
	return ctx.Err()
}

func (c *blockingWriteClient) Close(int, string) error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

func newLifecycleClient() *lifecycleClient {
	return &lifecycleClient{created: make(chan struct{}), closed: make(chan struct{})}
}

func (c *lifecycleClient) Read(ctx context.Context) (protocol.ClientEvent, error) {
	select {
	case <-c.closed:
		return nil, io.EOF
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *lifecycleClient) Write(ctx context.Context, event protocol.ServerEvent) error {
	if event.EventType() == "session.created" {
		c.createdOnce.Do(func() { close(c.created) })
	}
	return ctx.Err()
}

func (c *lifecycleClient) Close(int, string) error {
	c.closes.Add(1)
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

func newGateAudioClient(buffer int) *gateAudioClient {
	return &gateAudioClient{
		channelClient: newChannelClient(buffer),
		audioStarted:  make(chan struct{}),
		audioRelease:  make(chan struct{}),
	}
}

func (c *gateAudioClient) Write(ctx context.Context, event protocol.ServerEvent) error {
	audio, ok := event.(protocol.ResponseOutputAudioDeltaEvent)
	if !ok {
		return c.channelClient.Write(ctx, event)
	}
	c.startedOnce.Do(func() { close(c.audioStarted) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.audioRelease:
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	c.recordedAudio = append(c.recordedAudio, audio)
	c.mu.Unlock()
	return nil
}

func newChannelClient(buffer int) *channelClient {
	return &channelClient{
		reads:  make(chan protocol.ClientEvent, buffer),
		writes: make(chan protocol.ServerEvent, buffer),
		done:   make(chan struct{}),
	}
}

func (c *channelClient) Read(ctx context.Context) (protocol.ClientEvent, error) {
	select {
	case event := <-c.reads:
		return event, nil
	case <-c.done:
		return nil, io.EOF
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *channelClient) Write(ctx context.Context, event protocol.ServerEvent) error {
	select {
	case c.writes <- event:
		return nil
	case <-c.done:
		return io.ErrClosedPipe
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *channelClient) Close(int, string) error {
	c.closeOnce.Do(func() { close(c.done) })
	return nil
}

func TestManagerFramesAppendAndFlushesEvenTailBeforeCommit(t *testing.T) {
	connector := testkit.NewFakeConnector(16)
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
	}, connector, managerIDs{session: "session-1", conversation: "conversation-1"})
	client := newChannelClient(8)
	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() {
		runResult <- manager.Run(ctx, OpenRequest{
			Subject: Subject{TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour)},
		}, client)
	}()

	voice, err := connector.NextSession(context.Background())
	require.NoError(t, err)
	pcm := make([]byte, 962)
	for index := range pcm {
		pcm[index] = byte(index)
	}
	client.reads <- protocol.InputAudioBufferAppendEvent{
		Type:  "input_audio_buffer.append",
		Audio: base64.StdEncoding.EncodeToString(pcm[:481]),
	}
	client.reads <- protocol.InputAudioBufferAppendEvent{
		Type:  "input_audio_buffer.append",
		Audio: base64.StdEncoding.EncodeToString(pcm[481:]),
	}
	client.reads <- protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}

	first, err := voice.NextCommand(context.Background())
	require.NoError(t, err)
	require.Equal(t, realtime.CommandAppendAudio, first.Kind)
	require.Equal(t, uint64(1), first.Sequence)
	require.Equal(t, pcm[:960], first.Audio)

	tail, err := voice.NextCommand(context.Background())
	require.NoError(t, err)
	require.Equal(t, realtime.CommandAppendAudio, tail.Kind)
	require.Equal(t, uint64(2), tail.Sequence)
	require.Equal(t, pcm[960:], tail.Audio)

	commit, err := voice.NextCommand(context.Background())
	require.NoError(t, err)
	require.Equal(t, realtime.CommandCommitAudio, commit.Kind)
	require.Equal(t, uint64(1), commit.Sequence, "reactor control identity is independent of media sequence")
	require.Equal(t, uint64(2), commit.AfterSeq)

	cancel()
	require.ErrorIs(t, <-runResult, context.Canceled)
}

func TestManagerRejectsExpiredSubjectBeforeConnect(t *testing.T) {
	connector := testkit.NewFakeConnector(1)
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
	}, connector, managerIDs{session: "session-1", conversation: "conversation-1"})
	err := manager.Run(context.Background(), OpenRequest{
		Subject: Subject{TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(-time.Second)},
	}, newChannelClient(1))
	require.Error(t, err)
	require.False(t, errors.Is(err, context.Canceled))
}

func TestManagerConfigDefaultsAndBounds(t *testing.T) {
	connector := testkit.NewFakeConnector(1)
	ids := managerIDs{session: "session-1", conversation: "conversation-1"}
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
	}, connector, ids)
	require.NoError(t, manager.validationErr)
	require.Equal(t, 25, manager.cfg.IngressFrames)
	require.Equal(t, 75, manager.cfg.EgressFrames)
	require.Equal(t, 256, manager.cfg.ControlEvents)
	require.Equal(t, 64, manager.cfg.WriterControlEvents)

	aboveDefaults := NewManager(Config{
		IngressFrames:       26,
		EgressFrames:        76,
		ControlEvents:       257,
		WriterControlEvents: 65,
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
	}, connector, ids)
	require.NoError(t, aboveDefaults.validationErr)

	tooLarge := NewManager(Config{
		IngressFrames:       10_001,
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
	}, connector, ids)
	require.ErrorIs(t, tooLarge.validationErr, ErrInvalidManagerConfig)
}

func TestManagerOddCommitKeepsTailForTheNextAppend(t *testing.T) {
	connector := testkit.NewFakeConnector(8)
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
	}, connector, managerIDs{session: "session-odd", conversation: "conversation-odd"})
	client := newChannelClient(8)
	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() {
		runResult <- manager.Run(ctx, OpenRequest{
			Subject: Subject{TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour)},
		}, client)
	}()
	voice, err := connector.NextSession(context.Background())
	require.NoError(t, err)

	client.reads <- protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}
	client.reads <- protocol.InputAudioBufferAppendEvent{
		Type: "input_audio_buffer.append", Audio: base64.StdEncoding.EncodeToString([]byte{0x12}),
	}
	client.reads <- protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}
	client.reads <- protocol.InputAudioBufferAppendEvent{
		Type: "input_audio_buffer.append", Audio: base64.StdEncoding.EncodeToString([]byte{0x34}),
	}
	client.reads <- protocol.InputAudioBufferCommitEvent{Type: "input_audio_buffer.commit"}

	commandCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	appendCommand, err := voice.NextCommand(commandCtx)
	require.NoError(t, err)
	require.Equal(t, realtime.CommandAppendAudio, appendCommand.Kind)
	require.Equal(t, []byte{0x12, 0x34}, appendCommand.Audio)
	require.Equal(t, uint64(1), appendCommand.Sequence)
	commit, err := voice.NextCommand(commandCtx)
	require.NoError(t, err)
	require.Equal(t, realtime.CommandCommitAudio, commit.Kind)
	require.Equal(t, uint64(1), commit.AfterSeq)
	var commitErrors []protocol.ErrorEvent
	for len(commitErrors) < 2 {
		select {
		case event := <-client.writes:
			if invalid, ok := event.(protocol.ErrorEvent); ok {
				commitErrors = append(commitErrors, invalid)
			}
		case <-commandCtx.Done():
			t.Fatal("timed out waiting for invalid commit errors")
		}
	}
	require.Equal(t, "invalid_audio_commit", commitErrors[0].Error.Code)
	require.Equal(t, "invalid_audio_commit", commitErrors[1].Error.Code)
	require.NotEqual(t, commitErrors[0].EventID, commitErrors[1].EventID)

	cancel()
	require.ErrorIs(t, <-runResult, context.Canceled)
}

func TestManagerNormalizesProviderChunksAndOrdersAudioDoneAfterFrames(t *testing.T) {
	connector := testkit.NewFakeConnector(16)
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
	}, connector, managerIDs{session: "session-output", conversation: "conversation-output"})
	client := newChannelClient(16)
	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() {
		runResult <- manager.Run(ctx, OpenRequest{
			Subject: Subject{TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour)},
		}, client)
	}()
	voice, err := connector.NextSession(context.Background())
	require.NoError(t, err)

	client.reads <- protocol.ResponseCreateEvent{Type: "response.create"}
	commandCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	create, err := voice.NextCommand(commandCtx)
	require.NoError(t, err)
	require.Equal(t, realtime.CommandCreateResponse, create.Kind)
	require.NotZero(t, create.Epoch)

	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventResponseCreated, Epoch: create.Epoch,
		ProviderEventID: "provider-created", ResponseID: "response-1", Status: "in_progress",
	}))
	pcm := make([]byte, 1_442)
	for index := range pcm {
		pcm[index] = byte(index)
	}
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventAudioDelta, Epoch: create.Epoch,
		ResponseID: "response-1", ItemID: "item-1", Audio: pcm[:481],
	}))
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventAudioDelta, Epoch: create.Epoch,
		ResponseID: "response-1", ItemID: "item-1", Audio: pcm[481:962],
	}))
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventAudioDelta, Epoch: create.Epoch,
		ResponseID: "response-1", ItemID: "item-1", Audio: pcm[962:],
	}))
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventAudioDone, Epoch: create.Epoch,
		ProviderEventID: "provider-audio-done", ResponseID: "response-1", ItemID: "item-1",
	}))

	var types []string
	var gotPCM []byte
	var audioEvents []protocol.ResponseOutputAudioDeltaEvent
	for len(types) < 4 {
		select {
		case event := <-client.writes:
			if event.EventType() == "session.created" {
				continue
			}
			types = append(types, event.EventType())
			if audio, ok := event.(protocol.ResponseOutputAudioDeltaEvent); ok {
				decoded, decodeErr := base64.StdEncoding.DecodeString(audio.Delta)
				require.NoError(t, decodeErr)
				require.LessOrEqual(t, len(decoded), 960)
				require.NotEmpty(t, audio.EventID)
				require.NotEmpty(t, audio.ResponseID)
				require.NotEmpty(t, audio.ItemID)
				gotPCM = append(gotPCM, decoded...)
				audioEvents = append(audioEvents, audio)
			}
		case <-commandCtx.Done():
			t.Fatal("timed out waiting for normalized output")
		}
	}
	require.Equal(t, []string{
		"response.created",
		"response.output_audio.delta",
		"response.output_audio.delta",
		"response.output_audio.done",
	}, types[:4])
	require.Len(t, audioEvents, 2)
	require.NotEqual(t, audioEvents[0].EventID, audioEvents[1].EventID)
	require.True(t, bytes.Equal(pcm, gotPCM))

	cancel()
	require.ErrorIs(t, <-runResult, context.Canceled)
}

func TestManagerCancelsInFlightStaleAudioBeforeBargeWriteCanRecord(t *testing.T) {
	connector := testkit.NewFakeConnector(16)
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
	}, connector, managerIDs{session: "session-barge", conversation: "conversation-barge"})
	client := newGateAudioClient(16)
	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() {
		runResult <- manager.Run(ctx, OpenRequest{
			Subject: Subject{TenantID: "trusted-tenant", UserID: "trusted-user", ExpiresAt: time.Now().Add(time.Hour)},
		}, client)
	}()
	voice, err := connector.NextSession(context.Background())
	require.NoError(t, err)
	commandCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()

	client.reads <- protocol.ResponseCreateEvent{Type: "response.create"}
	create, err := voice.NextCommand(commandCtx)
	require.NoError(t, err)
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventResponseCreated, Epoch: create.Epoch,
		ResponseID: "response-barge", ProviderEventID: "created-barge", Status: "in_progress",
	}))
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventAudioDelta, Epoch: create.Epoch,
		ResponseID: "response-barge", ItemID: "item-output", Audio: make([]byte, 960),
	}))

	select {
	case <-client.audioStarted:
	case <-commandCtx.Done():
		t.Fatal("audio write did not reach gate")
	}
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventAudioDelta, Epoch: create.Epoch,
		ResponseID: "response-barge", ItemID: "item-output", Audio: bytes.Repeat([]byte{0x7f}, 960),
	}))
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventSpeechStarted, ProviderEventID: "speech-started", ItemID: "item-input",
	}))

	for {
		select {
		case event := <-client.writes:
			if event.EventType() == "input_audio_buffer.speech_started" {
				close(client.audioRelease)
				client.mu.Lock()
				require.Empty(t, client.recordedAudio)
				client.mu.Unlock()
				cancel()
				require.ErrorIs(t, <-runResult, context.Canceled)
				return
			}
		case <-commandCtx.Done():
			t.Fatal("timed out waiting for barge-in notification")
		}
	}
}

func TestManagerTransportFailureClosesOnceUnregistersAndPreservesCause(t *testing.T) {
	connector := newLifecycleConnector()
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
	}, connector, managerIDs{session: "session-lifecycle", conversation: "conversation-lifecycle"})
	client := newLifecycleClient()
	scopes := map[string]struct{}{"live": {}}
	permissions := json.RawMessage(`{"allow":true}`)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runResult := make(chan error, 1)
	go func() {
		runResult <- manager.Run(ctx, OpenRequest{Subject: Subject{
			TenantID: "trusted-tenant", UserID: "trusted-user", Scopes: scopes,
			Permissions: permissions, ExpiresAt: time.Now().Add(time.Hour),
		}}, client)
	}()

	open := <-connector.connected
	require.Equal(t, identity.SessionID("session-lifecycle"), open.SessionID)
	require.Equal(t, identity.Incarnation(1), open.Incarnation)
	select {
	case <-client.created:
	case <-time.After(time.Second):
		t.Fatal("session did not become active")
	}

	delete(scopes, "live")
	permissions[2] = 'X'
	var runtime *sessionRuntime
	require.Eventually(t, func() bool {
		manager.activeMu.RLock()
		runtime = manager.active[runtimeKey{sessionID: "session-lifecycle", incarnation: 1}]
		manager.activeMu.RUnlock()
		return runtime != nil
	}, time.Second, time.Millisecond)
	require.NotNil(t, runtime)
	require.Contains(t, runtime.subject.Scopes, "live")
	require.JSONEq(t, `{"allow":true}`, string(runtime.subject.Permissions))

	spoof := protocol.ResponseCreateEvent{
		Type: "response.create",
		Response: &protocol.ResponseConfig{
			Metadata: json.RawMessage(`{"tenant_id":"evil-tenant","user_id":"evil-user"}`),
		},
	}
	require.NoError(t, manager.Submit(context.Background(), "session-lifecycle", 1, ClientInput{Event: spoof}))
	select {
	case <-connector.voice.sending:
	case <-time.After(time.Second):
		t.Fatal("spoofing client metadata was not processed")
	}
	require.Equal(t, "trusted-tenant", runtime.subject.TenantID)
	require.Equal(t, "trusted-user", runtime.subject.UserID)
	require.NoError(t, manager.Submit(context.Background(), "session-lifecycle", 1, DurableInput{Sequence: 1, Kind: "noop"}))
	require.ErrorIs(t, manager.Submit(context.Background(), "session-lifecycle", 2, DurableInput{Sequence: 2}), ErrSessionNotActive)

	transportErr := errors.New("provider receive failed")
	connector.voice.failure <- transportErr
	err := <-runResult
	require.ErrorIs(t, err, transportErr)
	require.Equal(t, int32(1), connector.voice.closes.Load())
	require.True(t, connector.voice.closeLive.Load())
	require.Equal(t, int32(1), client.closes.Load())
	require.ErrorIs(t, manager.Submit(context.Background(), "session-lifecycle", 1, DurableInput{Sequence: 2}), ErrSessionNotActive)
	manager.activeMu.RLock()
	require.Empty(t, manager.active)
	manager.activeMu.RUnlock()
}

func TestManagerKeepsPCMFramersIsolatedAcrossConcurrentSessions(t *testing.T) {
	type running struct {
		cancel context.CancelFunc
		done   chan error
		client *channelClient
		voice  *testkit.FakeVoiceSession
	}
	start := func(sessionID string, sentinel byte) running {
		connector := testkit.NewFakeConnector(8)
		manager := NewManager(Config{
			IngressBlockTimeout: time.Second,
			EgressBlockTimeout:  time.Second,
		}, connector, managerIDs{session: identity.SessionID(sessionID), conversation: identity.ConversationID("conversation-" + sessionID)})
		client := newChannelClient(4)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- manager.Run(ctx, OpenRequest{Subject: Subject{
				TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour),
			}}, client)
		}()
		voice, err := connector.NextSession(context.Background())
		require.NoError(t, err)
		client.reads <- protocol.InputAudioBufferAppendEvent{
			Type: "input_audio_buffer.append", Audio: base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{sentinel}, 960)),
		}
		return running{cancel: cancel, done: done, client: client, voice: voice}
	}

	first := start("session-a", 0x11)
	second := start("session-b", 0x22)
	commandCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	firstCommand, err := first.voice.NextCommand(commandCtx)
	require.NoError(t, err)
	secondCommand, err := second.voice.NextCommand(commandCtx)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{0x11}, 960), firstCommand.Audio)
	require.Equal(t, bytes.Repeat([]byte{0x22}, 960), secondCommand.Audio)

	first.cancel()
	second.cancel()
	require.ErrorIs(t, <-first.done, context.Canceled)
	require.ErrorIs(t, <-second.done, context.Canceled)
}

func TestManagerIngressAcceptsCapacityAndFailsOnFirstExcessFrame(t *testing.T) {
	connector := newLifecycleConnector()
	manager := NewManager(Config{
		IngressFrames:       1,
		IngressBlockTimeout: 20 * time.Millisecond,
		EgressBlockTimeout:  time.Second,
	}, connector, managerIDs{session: "session-ingress-full", conversation: "conversation-ingress-full"})
	client := newChannelClient(8)
	runCtx, runCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer runCancel()
	runResult := make(chan error, 1)
	go func() {
		runResult <- manager.Run(runCtx, OpenRequest{Subject: Subject{
			TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour),
		}}, client)
	}()
	<-connector.connected
	frame := base64.StdEncoding.EncodeToString(make([]byte, 960))
	client.reads <- protocol.InputAudioBufferAppendEvent{Type: "input_audio_buffer.append", Audio: frame}
	select {
	case <-connector.voice.sending:
	case <-time.After(time.Second):
		t.Fatal("voice writer did not consume the first frame")
	}
	client.reads <- protocol.InputAudioBufferAppendEvent{Type: "input_audio_buffer.append", Audio: frame}
	require.ErrorIs(t, <-runResult, ErrIngressBackpressure)
}

func TestManagerEgressAcceptsCapacityAndFailsOnFirstExcessFrame(t *testing.T) {
	connector := testkit.NewFakeConnector(16)
	manager := NewManager(Config{
		EgressFrames:        1,
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  20 * time.Millisecond,
	}, connector, managerIDs{session: "session-egress-full", conversation: "conversation-egress-full"})
	client := newGateAudioClient(16)
	runCtx, runCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer runCancel()
	runResult := make(chan error, 1)
	go func() {
		runResult <- manager.Run(runCtx, OpenRequest{Subject: Subject{
			TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour),
		}}, client)
	}()
	voice, err := connector.NextSession(context.Background())
	require.NoError(t, err)
	commandCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	client.reads <- protocol.ResponseCreateEvent{Type: "response.create"}
	create, err := voice.NextCommand(commandCtx)
	require.NoError(t, err)
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventResponseCreated, Epoch: create.Epoch,
		ResponseID: "response-egress", ProviderEventID: "created-egress", Status: "in_progress",
	}))
	frame := realtime.Event{
		Kind: realtime.EventAudioDelta, Epoch: create.Epoch,
		ResponseID: "response-egress", ItemID: "item-egress", Audio: make([]byte, 960),
	}
	require.NoError(t, voice.Emit(commandCtx, frame))
	select {
	case <-client.audioStarted:
	case <-commandCtx.Done():
		t.Fatal("client writer did not consume the first frame")
	}
	require.NoError(t, voice.Emit(commandCtx, frame))
	require.ErrorIs(t, <-runResult, ErrEgressBackpressure)
}

func TestManagerVoiceControlAcceptsCapacityAndFailsOnFirstExcessControl(t *testing.T) {
	connector := newLifecycleConnector()
	manager := NewManager(Config{
		WriterControlEvents: 1,
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
	}, connector, managerIDs{session: "session-voice-control", conversation: "conversation-voice-control"})
	client := newChannelClient(8)
	runCtx, runCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer runCancel()
	runResult := make(chan error, 1)
	go func() {
		runResult <- manager.Run(runCtx, OpenRequest{Subject: Subject{
			TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour),
		}}, client)
	}()
	<-connector.connected
	update := protocol.SessionUpdateEvent{Type: "session.update", Session: protocol.SessionUpdate{Type: "realtime"}}
	client.reads <- update
	select {
	case <-connector.voice.sending:
	case <-time.After(time.Second):
		t.Fatal("voice writer did not consume the first control")
	}
	client.reads <- update
	require.ErrorIs(t, <-runResult, ErrControlOverflow)
}

func TestManagerClientControlAcceptsCapacityAndFailsOnFirstExcessControl(t *testing.T) {
	connector := testkit.NewFakeConnector(8)
	manager := NewManager(Config{
		WriterControlEvents: 1,
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
	}, connector, managerIDs{session: "session-client-control", conversation: "conversation-client-control"})
	client := newBlockingWriteClient()
	runCtx, runCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer runCancel()
	runResult := make(chan error, 1)
	go func() {
		runResult <- manager.Run(runCtx, OpenRequest{Subject: Subject{
			TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour),
		}}, client)
	}()
	voice, err := connector.NextSession(context.Background())
	require.NoError(t, err)
	commandCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventSpeechStarted, ProviderEventID: "speech-start", ItemID: "input-item",
	}))
	select {
	case <-client.started:
	case <-time.After(time.Second):
		t.Fatal("client writer did not consume first post-ready event")
	}
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventSpeechStopped, ProviderEventID: "speech-stop", ItemID: "input-item",
	}))
	require.ErrorIs(t, <-runResult, ErrControlOverflow)
}

func TestManagerQueuesImplicitCreatedBeforeBindReleasesAudio(t *testing.T) {
	connector := testkit.NewFakeConnector(16)
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
	}, connector, managerIDs{session: "session-implicit", conversation: "conversation-implicit"})
	client := newChannelClient(16)
	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() {
		runResult <- manager.Run(ctx, OpenRequest{Subject: Subject{
			TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour),
		}}, client)
	}()
	voice, err := connector.NextSession(context.Background())
	require.NoError(t, err)
	commandCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventResponseCreated, NeedsEpochBinding: true,
		ProviderEventID: "implicit-created", ResponseID: "implicit-response", Status: "in_progress",
	}))
	bind, err := voice.NextCommand(commandCtx)
	require.NoError(t, err)
	require.Equal(t, realtime.CommandBindResponseEpoch, bind.Kind)
	require.Equal(t, "implicit-response", bind.ResponseID)
	require.NotZero(t, bind.Epoch)
	require.Zero(t, bind.AfterSeq)
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventAudioDelta, Epoch: bind.Epoch,
		ResponseID: "implicit-response", ItemID: "implicit-item", Audio: make([]byte, 960),
	}))
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventAudioDone, Epoch: bind.Epoch,
		ResponseID: "implicit-response", ItemID: "implicit-item", ProviderEventID: "implicit-audio-done",
	}))
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventResponseDone, Epoch: bind.Epoch,
		ResponseID: "implicit-response", ProviderEventID: "implicit-response-done", Status: "completed",
	}))

	var ordered []string
	for len(ordered) < 4 {
		select {
		case event := <-client.writes:
			if event.EventType() == "session.created" {
				continue
			}
			ordered = append(ordered, event.EventType())
		case <-commandCtx.Done():
			t.Fatal("timed out waiting for implicit response output")
		}
	}
	require.Equal(t, []string{
		"response.created", "response.output_audio.delta", "response.output_audio.done", "response.done",
	}, ordered)

	cancel()
	require.ErrorIs(t, <-runResult, context.Canceled)
}

func TestManagerRejectsOddProviderTailWithoutMixingTheNextResponse(t *testing.T) {
	connector := testkit.NewFakeConnector(16)
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
	}, connector, managerIDs{session: "session-odd-output", conversation: "conversation-odd-output"})
	client := newChannelClient(16)
	runResult := make(chan error, 1)
	go func() {
		runResult <- manager.Run(context.Background(), OpenRequest{Subject: Subject{
			TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour),
		}}, client)
	}()
	voice, err := connector.NextSession(context.Background())
	require.NoError(t, err)
	commandCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	client.reads <- protocol.ResponseCreateEvent{Type: "response.create"}
	create, err := voice.NextCommand(commandCtx)
	require.NoError(t, err)
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventResponseCreated, Epoch: create.Epoch,
		ResponseID: "odd-response", ProviderEventID: "odd-created", Status: "in_progress",
	}))
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventAudioDelta, Epoch: create.Epoch,
		ResponseID: "odd-response", ItemID: "odd-item", Audio: []byte{0x01},
	}))
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventAudioDone, Epoch: create.Epoch,
		ResponseID: "odd-response", ItemID: "odd-item",
	}))
	err = <-runResult
	require.ErrorContains(t, err, "split sample")

	for {
		select {
		case event := <-client.writes:
			_, audio := event.(protocol.ResponseOutputAudioDeltaEvent)
			require.False(t, audio, "odd output tail must never be forwarded")
		default:
			return
		}
	}
}

func TestManagerRejectsFirstByteOverProviderAudioEventLimit(t *testing.T) {
	connector := testkit.NewFakeConnector(8)
	manager := NewManager(Config{
		EgressFrames: 2, IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
	}, connector, managerIDs{session: "session-audio-limit", conversation: "conversation-audio-limit"})
	client := newChannelClient(8)
	runCtx, runCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer runCancel()
	runResult := make(chan error, 1)
	go func() {
		runResult <- manager.Run(runCtx, OpenRequest{Subject: Subject{
			TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour),
		}}, client)
	}()
	voice, err := connector.NextSession(context.Background())
	require.NoError(t, err)
	commandCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	client.reads <- protocol.ResponseCreateEvent{Type: "response.create"}
	create, err := voice.NextCommand(commandCtx)
	require.NoError(t, err)
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventResponseCreated, Epoch: create.Epoch,
		ResponseID: "limited-response", ProviderEventID: "limited-created", Status: "in_progress",
	}))
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventAudioDelta, Epoch: create.Epoch,
		ResponseID: "limited-response", ItemID: "limited-item", Audio: make([]byte, 2*960),
	}))
	for frames := 0; frames < 2; {
		select {
		case event := <-client.writes:
			if _, ok := event.(protocol.ResponseOutputAudioDeltaEvent); ok {
				frames++
			}
		case <-commandCtx.Done():
			t.Fatal("exact provider audio event limit was not accepted")
		}
	}
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventAudioDelta, Epoch: create.Epoch,
		ResponseID: "limited-response", ItemID: "limited-item", Audio: make([]byte, 2*960+1),
	}))
	require.ErrorContains(t, <-runResult, "provider audio event exceeds 1920 bytes")
}

func TestManagerStopsProviderBatchAtMidBatchInvalidation(t *testing.T) {
	connector := testkit.NewFakeConnector(16)
	manager := NewManager(Config{
		EgressFrames: 3, IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
	}, connector, managerIDs{session: "session-mid-batch", conversation: "conversation-mid-batch"})
	dropped := make(chan int, 4)
	manager.SetMetricsHooks(MetricsHooks{AudioDropped: func(bytes int) { dropped <- bytes }})
	client := newGateAudioClient(16)
	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() {
		runResult <- manager.Run(ctx, OpenRequest{Subject: Subject{
			TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour),
		}}, client)
	}()
	voice, err := connector.NextSession(context.Background())
	require.NoError(t, err)
	commandCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	client.reads <- protocol.ResponseCreateEvent{Type: "response.create"}
	create, err := voice.NextCommand(commandCtx)
	require.NoError(t, err)
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventResponseCreated, Epoch: create.Epoch,
		ResponseID: "mid-response", ProviderEventID: "mid-created", Status: "in_progress",
	}))
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventAudioDelta, Epoch: create.Epoch,
		ResponseID: "mid-response", ItemID: "mid-item", Audio: make([]byte, 3*960),
	}))
	select {
	case <-client.audioStarted:
	case <-commandCtx.Done():
		t.Fatal("first provider frame did not enter client write")
	}
	client.reads <- protocol.ResponseCancelEvent{Type: "response.cancel", ResponseID: "mid-response"}
	cancelCommand, err := voice.NextCommand(commandCtx)
	require.NoError(t, err)
	require.Equal(t, realtime.CommandCancelResponse, cancelCommand.Kind)
	require.Equal(t, 960, <-dropped)
	require.Equal(t, 960, <-dropped)
	select {
	case bytes := <-dropped:
		t.Fatalf("normalizer continued after invalidation and dropped an extra %d-byte frame", bytes)
	default:
	}
	close(client.audioRelease)
	cancel()
	require.ErrorIs(t, <-runResult, context.Canceled)
}

func TestManagerDiscardsInvalidatedProviderTailBeforeNextResponse(t *testing.T) {
	connector := testkit.NewFakeConnector(32)
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
	}, connector, managerIDs{session: "session-tail-barge", conversation: "conversation-tail-barge"})
	client := newChannelClient(32)
	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() {
		runResult <- manager.Run(ctx, OpenRequest{Subject: Subject{
			TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour),
		}}, client)
	}()
	voice, err := connector.NextSession(context.Background())
	require.NoError(t, err)
	commandCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	client.reads <- protocol.ResponseCreateEvent{Type: "response.create"}
	create, err := voice.NextCommand(commandCtx)
	require.NoError(t, err)
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventResponseCreated, Epoch: create.Epoch,
		ResponseID: "old-response", ProviderEventID: "old-created", Status: "in_progress",
	}))
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventAudioDelta, Epoch: create.Epoch,
		ResponseID: "old-response", ItemID: "old-item", Audio: []byte{0xaa, 0xbb},
	}))
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventSpeechStarted, ProviderEventID: "barge", ItemID: "input-item",
	}))
	cancelCommand, err := voice.NextCommand(commandCtx)
	require.NoError(t, err)
	require.Equal(t, realtime.CommandCancelResponse, cancelCommand.Kind)

	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventResponseCreated, NeedsEpochBinding: true,
		ProviderEventID: "new-created", ResponseID: "new-response", Status: "in_progress",
	}))
	bind, err := voice.NextCommand(commandCtx)
	require.NoError(t, err)
	require.Equal(t, realtime.CommandBindResponseEpoch, bind.Kind)
	newPCM := []byte{0x11, 0x22}
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventAudioDelta, Epoch: bind.Epoch,
		ResponseID: "new-response", ItemID: "new-item", Audio: newPCM,
	}))
	require.NoError(t, voice.Emit(commandCtx, realtime.Event{
		Kind: realtime.EventAudioDone, Epoch: bind.Epoch,
		ResponseID: "new-response", ItemID: "new-item",
	}))

	for {
		select {
		case event := <-client.writes:
			audio, ok := event.(protocol.ResponseOutputAudioDeltaEvent)
			if !ok {
				continue
			}
			decoded, decodeErr := base64.StdEncoding.DecodeString(audio.Delta)
			require.NoError(t, decodeErr)
			require.Equal(t, newPCM, decoded)
			cancel()
			require.ErrorIs(t, <-runResult, context.Canceled)
			return
		case err := <-runResult:
			t.Fatalf("session ended before forwarding the new response: %v", err)
		case <-commandCtx.Done():
			t.Fatal("timed out waiting for new response audio")
		}
	}
}

func TestManagerSubjectExpiryCancelsAllLoopsAndClosesTransports(t *testing.T) {
	connector := newLifecycleConnector()
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second,
		EgressBlockTimeout:  time.Second,
	}, connector, managerIDs{session: "session-expiry", conversation: "conversation-expiry"})
	client := newLifecycleClient()
	runResult := make(chan error, 1)
	go func() {
		runResult <- manager.Run(context.Background(), OpenRequest{Subject: Subject{
			TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(50 * time.Millisecond),
		}}, client)
	}()
	select {
	case <-connector.connected:
	case <-time.After(time.Second):
		t.Fatal("session did not connect before expiry")
	}
	require.ErrorIs(t, <-runResult, context.DeadlineExceeded)
	require.Equal(t, int32(1), connector.voice.closes.Load())
	require.True(t, connector.voice.closeLive.Load())
	require.Equal(t, int32(1), client.closes.Load())
}

func TestManagerSubmitHonorsBoundedInboxContext(t *testing.T) {
	manager := &Manager{active: make(map[runtimeKey]*sessionRuntime)}
	done := make(chan struct{})
	state := &runState{}
	runtime := &sessionRuntime{
		key:        runtimeKey{sessionID: "session-submit", incarnation: 1},
		inbox:      make(chan reactorMessage, 1),
		done:       done,
		state:      state,
		accepting:  true,
		inboxSlots: make(chan struct{}, 1),
	}
	manager.active[runtime.key] = runtime
	runtime.inbox <- reactorMessage{input: DurableInput{Sequence: 1}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, manager.Submit(ctx, "session-submit", 1, DurableInput{Sequence: 2}), context.Canceled)
	require.Len(t, runtime.inbox, 1, "a canceled submit must not overrun the bounded inbox")
}

func TestManagerSubmitRejectsAlreadyCanceledCallerWithReadyInbox(t *testing.T) {
	for attempt := 0; attempt < 1_000; attempt++ {
		manager := &Manager{active: make(map[runtimeKey]*sessionRuntime)}
		runtime := &sessionRuntime{
			key:   runtimeKey{sessionID: "session-submit-canceled", incarnation: 1},
			inbox: make(chan reactorMessage, 1), done: make(chan struct{}), state: &runState{},
			accepting: true, inboxSlots: make(chan struct{}, 1),
		}
		runtime.inboxSlots <- struct{}{}
		manager.active[runtime.key] = runtime
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.ErrorIs(t, manager.Submit(ctx, runtime.key.sessionID, 1, DurableInput{Sequence: 1}), context.Canceled)
		require.Empty(t, runtime.inbox)
	}
}

func TestManagerSubmitRejectsStoppedRuntimeWithReadyInbox(t *testing.T) {
	for attempt := 0; attempt < 1_000; attempt++ {
		manager := &Manager{active: make(map[runtimeKey]*sessionRuntime)}
		done := make(chan struct{})
		close(done)
		runtime := &sessionRuntime{
			key:   runtimeKey{sessionID: "session-submit-stopped", incarnation: 1},
			inbox: make(chan reactorMessage, 1), done: done, state: &runState{},
			accepting: true, inboxSlots: make(chan struct{}, 1),
		}
		runtime.inboxSlots <- struct{}{}
		manager.active[runtime.key] = runtime
		require.ErrorIs(t, manager.Submit(context.Background(), runtime.key.sessionID, 1, DurableInput{Sequence: 1}), ErrSessionNotActive)
		require.Empty(t, runtime.inbox)
	}
}

func TestManagerSubmitDoesNotAcknowledgeDeliveryToDeadInbox(t *testing.T) {
	manager := &Manager{active: make(map[runtimeKey]*sessionRuntime)}
	done := make(chan struct{})
	runtime := &sessionRuntime{
		key:   runtimeKey{sessionID: "session-submit-dead-inbox", incarnation: 1},
		inbox: make(chan reactorMessage, 1), done: done, state: &runState{},
		accepting: true, inboxSlots: make(chan struct{}, 1),
	}
	runtime.inboxSlots <- struct{}{}
	manager.active[runtime.key] = runtime
	result := make(chan error, 1)
	go func() {
		result <- manager.Submit(context.Background(), runtime.key.sessionID, 1, DurableInput{Sequence: 1})
	}()
	message := <-runtime.inbox
	require.NotNil(t, message.done, "Submit delivery requires reactor acknowledgement")
	close(done)
	require.ErrorIs(t, <-result, ErrSessionNotActive)
}

func TestVoiceWriterCancellationBypassesUnmetOrderedBarrier(t *testing.T) {
	voice := &recordingVoice{sent: make(chan realtime.Command, 2)}
	state := &runState{
		voice: voice, voiceCtl: make(chan voiceControl, 2), ingress: make(chan ingressFrame),
		inbox: make(chan reactorMessage, 2), inboxSlots: make(chan struct{}, 2),
	}
	state.inboxSlots <- struct{}{}
	state.inboxSlots <- struct{}{}
	state.voiceCtl <- voiceControl{command: realtime.Command{Kind: realtime.CommandCommitAudio, Sequence: 10, AfterSeq: 99}}
	state.voiceCtl <- voiceControl{command: realtime.Command{Kind: realtime.CommandCancelResponse, Sequence: 11}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- state.voiceWriter(ctx) }()
	select {
	case command := <-voice.sent:
		require.Equal(t, realtime.CommandCancelResponse, command.Kind)
		require.Equal(t, uint64(11), command.Sequence)
	case <-time.After(time.Second):
		t.Fatal("barrierless cancellation did not bypass ordered head")
	}
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestManagerRejectsAsyncEffectsInsteadOfDroppingThem(t *testing.T) {
	state := &runState{}
	err := state.dispatchEffects(context.Background(), []Effect{{Async: &AsyncEffect{
		Kind: "background", Sequence: 1,
	}}}, 0, 0)
	require.ErrorIs(t, err, ErrUnsupportedAsync)
}

type durableDispatcherFake struct {
	trusted     chan TrustedSessionContext
	transcripts chan TranscriptRequestedEffect
	handoffs    chan HandoffRequestedEffect
}

func (f *durableDispatcherFake) DispatchTranscript(_ context.Context, trusted TrustedSessionContext, effect TranscriptRequestedEffect) error {
	f.trusted <- trusted
	f.transcripts <- effect
	return nil
}

func (f *durableDispatcherFake) DispatchHandoff(_ context.Context, trusted TrustedSessionContext, effect HandoffRequestedEffect) error {
	f.trusted <- trusted
	f.handoffs <- effect
	return nil
}

func TestManagerDispatchesTypedDurableEffectsWithTrustedRuntimeIdentity(t *testing.T) {
	dispatcher := &durableDispatcherFake{
		trusted: make(chan TrustedSessionContext, 2), transcripts: make(chan TranscriptRequestedEffect, 1),
		handoffs: make(chan HandoffRequestedEffect, 1),
	}
	state := &runState{
		ids:        IDs{SessionID: "session-trusted", ConversationID: "conversation-trusted", Incarnation: 9},
		subject:    Subject{TenantID: "tenant-trusted", UserID: "user-trusted"},
		dispatcher: dispatcher,
	}
	transcript := TranscriptRequestedEffect{Turn: identity.TurnKey{ID: "turn", Revision: 2}, Sequence: 3, Role: "user", FinalText: "final"}
	handoffEffect := HandoffRequestedEffect{ResponseID: "response", CallID: "call", ToolName: StartBackgroundTaskTool, Arguments: json.RawMessage(`{"objective":"x"}`)}

	require.NoError(t, state.dispatchEffects(t.Context(), []Effect{
		{Transcript: &transcript},
		{Handoff: &handoffEffect, Async: &AsyncEffect{Kind: AsyncKindStartBackgroundTask}},
	}, 0, 0))
	require.Equal(t, transcript, <-dispatcher.transcripts)
	require.Equal(t, handoffEffect, <-dispatcher.handoffs)
	for range 2 {
		trusted := <-dispatcher.trusted
		require.Equal(t, state.ids, trusted.IDs)
		require.Equal(t, "tenant-trusted", trusted.Subject.TenantID)
		require.Equal(t, "user-trusted", trusted.Subject.UserID)
	}
}

func TestManagerTypedDurableEffectsFailClosedWithoutDispatcher(t *testing.T) {
	state := &runState{ids: testIDs()}
	err := state.dispatchEffects(t.Context(), []Effect{{Transcript: &TranscriptRequestedEffect{
		Turn: identity.TurnKey{ID: "turn", Revision: 1}, Sequence: 1, Role: "user", FinalText: "text",
	}}}, 0, 0)
	require.ErrorIs(t, err, ErrUnsupportedAsync)
}

func TestManagerGracefulCloseDrainsOutputAndReturnsCleanupErrors(t *testing.T) {
	providerErr := errors.New("provider cleanup failed")
	clientErr := errors.New("client cleanup failed")
	connector := &closeErrorConnector{
		voice: &closeErrorVoice{closeErr: providerErr}, connected: make(chan struct{}),
	}
	client := &closeErrorClient{closeErr: clientErr, writes: make(chan string, 8), closed: make(chan struct{})}
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
	}, connector, managerIDs{session: "session-close-errors", conversation: "conversation-close-errors"})
	runResult := make(chan error, 1)
	go func() {
		runResult <- manager.Run(context.Background(), OpenRequest{Subject: Subject{
			TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour),
		}}, client)
	}()
	<-connector.connected
	require.Equal(t, "session.created", <-client.writes)
	require.Eventually(t, func() bool {
		manager.activeMu.RLock()
		defer manager.activeMu.RUnlock()
		return manager.active[runtimeKey{sessionID: "session-close-errors", incarnation: 1}] != nil
	}, time.Second, time.Millisecond, "runtime is published only after session.created Write returns")
	require.NoError(t, manager.Submit(context.Background(), "session-close-errors", 1, FailedInput{
		Source: "test", Err: errors.New("graceful terminal"),
	}))
	err := <-runResult
	require.ErrorIs(t, err, clientErr)
	require.ErrorIs(t, err, providerErr)
	require.Equal(t, "error", <-client.writes)
	require.Equal(t, "client.close", <-client.writes)
}

func TestManagerMetricsHooksObserveOverflowAndDroppedAudio(t *testing.T) {
	overflows := make(chan string, 5)
	dropped := make(chan int, 1)
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
	}, testkit.NewFakeConnector(1), managerIDs{session: "metrics", conversation: "metrics"})
	manager.SetMetricsHooks(MetricsHooks{
		QueueOverflow: func(queue string) { overflows <- queue },
		AudioDropped:  func(bytes int) { dropped <- bytes },
	})
	state := newRunState(manager.cfg, IDs{SessionID: "metrics", ConversationID: "metrics", Incarnation: 1},
		&recordingVoice{sent: make(chan realtime.Command, 1)}, newChannelClient(1), func() {})
	state.metrics = manager.metricsSnapshot()
	state.cfg.IngressBlockTimeout = time.Nanosecond
	state.cfg.EgressBlockTimeout = time.Nanosecond
	for range state.cfg.IngressFrames {
		<-state.ingressSlots
	}
	require.ErrorIs(t, state.enqueueIngress(context.Background(), ingressFrame{sequence: 1, audio: []byte{0, 0}}), ErrIngressBackpressure)
	for range state.cfg.EgressFrames {
		<-state.egressSlots
	}
	state.fence = NewEpochFence(1)
	_, err := state.enqueueEgress(context.Background(), realtime.Event{Epoch: 1, ResponseID: "r", ItemID: "i"}, []byte{0, 0})
	require.ErrorIs(t, err, ErrEgressBackpressure)
	for range state.cfg.ControlEvents {
		<-state.inboxSlots
	}
	require.ErrorIs(t, state.enqueueReactor(reactorMessage{input: DurableInput{Sequence: 1}}), ErrControlOverflow)
	for range state.cfg.WriterControlEvents {
		<-state.voiceSlots
	}
	err = state.dispatchEffects(context.Background(), []Effect{{VoiceCommand: &realtime.Command{
		Kind: realtime.CommandCommitAudio, Sequence: 1,
	}}}, 0, 0)
	require.ErrorIs(t, err, ErrControlOverflow)
	for range state.cfg.WriterControlEvents {
		<-state.clientSlots
	}
	require.ErrorIs(t, state.enqueueClientControl(clientControl{event: protocol.ErrorEvent{Type: "error"}}), ErrControlOverflow)
	gotQueues := map[string]int{}
	for range 5 {
		gotQueues[<-overflows]++
	}
	require.Equal(t, map[string]int{
		"ingress": 1, "egress": 1, "reactor": 1, "voice_control": 1, "client_control": 1,
	}, gotQueues)

	state.metrics.audioDropped(960)
	require.Equal(t, 960, <-dropped)
}

func TestManagerReactorAggregateBoundRejectsFirstExcessWhileApplyIsActive(t *testing.T) {
	cfg := Config{
		IngressFrames: 1, EgressFrames: 1, ControlEvents: 1, WriterControlEvents: 4,
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
	}
	state := newRunState(cfg, IDs{SessionID: "reactor-bound", ConversationID: "reactor-bound", Incarnation: 1},
		&recordingVoice{sent: make(chan realtime.Command, 4)}, newChannelClient(4), func() {})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- state.reactorLoop(ctx) }()
	require.NoError(t, state.enqueueReactorSync(context.Background(), reactorMessage{
		input: ConnectedInput{ProviderSession: json.RawMessage(`{}`)},
	}))
	require.NoError(t, state.enqueueReactorSync(context.Background(), reactorMessage{
		input: ClientInput{Event: protocol.ResponseCreateEvent{Type: "response.create"}},
	}))
	cancelSeen := make(chan struct{})
	releaseApply := make(chan struct{})
	state.activeWrite.mu.Lock()
	state.activeWrite.cancel = func() { close(cancelSeen) }
	state.activeWrite.done = releaseApply
	state.activeWrite.mu.Unlock()
	applyDone := make(chan error, 1)
	require.NoError(t, state.enqueueReactor(reactorMessage{
		input: ClientInput{Event: protocol.ResponseCancelEvent{Type: "response.cancel"}}, done: applyDone,
	}))
	select {
	case <-cancelSeen:
	case <-time.After(time.Second):
		t.Fatal("reactor Apply did not reach flush gate")
	}
	require.ErrorIs(t, state.enqueueReactor(reactorMessage{input: DurableInput{Sequence: 1}}), ErrControlOverflow)
	close(releaseApply)
	require.NoError(t, <-applyDone)
	require.NoError(t, state.enqueueReactorSync(context.Background(), reactorMessage{
		input: DurableInput{Sequence: 1, Kind: "benign-after-ack"},
	}), "an observed completion ack must make its aggregate credit immediately reusable")
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestManagerMetricsHooksAreNoOpByDefaultAndRaceSafeToReplace(t *testing.T) {
	manager := NewManager(Config{
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
	}, testkit.NewFakeConnector(1), managerIDs{session: "metrics-race", conversation: "metrics-race"})
	manager.metricsSnapshot().queueOverflow("noop")
	start := make(chan struct{})
	var wait sync.WaitGroup
	wait.Add(2)
	go func() {
		defer wait.Done()
		<-start
		for index := 0; index < 1_000; index++ {
			manager.SetMetricsHooks(MetricsHooks{})
		}
	}()
	go func() {
		defer wait.Done()
		<-start
		for index := 0; index < 1_000; index++ {
			manager.metricsSnapshot().audioDropped(960)
		}
	}()
	close(start)
	wait.Wait()
}

func TestManagerTransportFailureInEveryDirectionStopsAllFiveLoops(t *testing.T) {
	tests := []struct {
		name    string
		trigger func(*failureVoice, *failureClient, error)
	}{
		{
			name: "client read",
			trigger: func(_ *failureVoice, client *failureClient, failure error) {
				client.readErr <- failure
			},
		},
		{
			name: "client write",
			trigger: func(_ *failureVoice, client *failureClient, failure error) {
				client.writeErr <- failure
			},
		},
		{
			name: "provider receive",
			trigger: func(voice *failureVoice, _ *failureClient, failure error) {
				voice.receiveErr <- failure
			},
		},
		{
			name: "provider send",
			trigger: func(voice *failureVoice, client *failureClient, failure error) {
				voice.sendErr <- failure
				client.reads <- protocol.ResponseCreateEvent{Type: "response.create"}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			providerCleanupErr := errors.New("provider cleanup after " + test.name)
			clientCleanupErr := errors.New("client cleanup after " + test.name)
			voice := &failureVoice{
				sendErr: make(chan error, 1), receiveErr: make(chan error, 1), closeErr: providerCleanupErr,
			}
			connector := &failureConnector{voice: voice, connected: make(chan struct{})}
			client := newFailureClient()
			client.closeErr = clientCleanupErr
			manager := NewManager(Config{
				IngressBlockTimeout: time.Second,
				EgressBlockTimeout:  time.Second,
			}, connector, managerIDs{
				session:      identity.SessionID("session-direction-" + test.name),
				conversation: identity.ConversationID("conversation-direction-" + test.name),
			})
			runResult := make(chan error, 1)
			go func() {
				runResult <- manager.Run(context.Background(), OpenRequest{Subject: Subject{
					TenantID: "tenant", UserID: "user", ExpiresAt: time.Now().Add(time.Hour),
				}}, client)
			}()
			select {
			case <-connector.connected:
			case <-time.After(time.Second):
				t.Fatal("session did not connect")
			}
			failure := errors.New(test.name + " transport failed")
			test.trigger(voice, client, failure)
			err := <-runResult
			require.ErrorIs(t, err, failure)
			require.NotErrorIs(t, err, providerCleanupErr)
			require.NotErrorIs(t, err, clientCleanupErr)
			require.Equal(t, int32(1), voice.closes.Load())
			require.True(t, voice.closeLive.Load())
			require.Equal(t, int32(1), client.closes.Load())
			manager.activeMu.RLock()
			require.Empty(t, manager.active)
			manager.activeMu.RUnlock()
		})
	}
}
