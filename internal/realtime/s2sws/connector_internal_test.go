package s2sws

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

func TestBindImplicitZeroesAndNilsDrainedOwnedBuffers(t *testing.T) {
	raw := []byte("private-raw-payload")
	decoded := []byte("decoded-owned-bytes")
	session := &voiceSession{
		ctx: context.Background(), events: make(chan realtime.Event, 1), done: make(chan struct{}),
		responses: map[string]*responseCorrelation{
			"response": {
				unbound: true,
				buffered: []bufferedResponseEvent{{
					event:    realtime.Event{Kind: realtime.EventAudioDelta, ResponseID: "response", Payload: raw, Audio: decoded},
					rawBytes: len(raw), decodedBytes: len(decoded), ownedBytes: len(raw) + len(decoded),
				}},
				bufferedBytes: len(raw),
			},
		},
		implicitUnbound: 1, implicitBufferedEvents: 1, implicitBufferedBytes: len(raw) + len(decoded),
	}
	require.NoError(t, session.bindImplicit(correlationRequest{ctx: t.Context(), responseID: "response", epoch: identity.ResponseEpoch(7)}))
	correlation := session.responses["response"]
	require.Nil(t, correlation.buffered)
	require.Zero(t, correlation.bufferedBytes)
	require.Zero(t, session.implicitUnbound)
	require.Zero(t, session.implicitBufferedEvents)
	require.Zero(t, session.implicitBufferedBytes)
	for _, value := range raw {
		require.Zero(t, value)
	}
	for _, value := range decoded {
		require.Zero(t, value)
	}
}

// TestMappingLoopServicesCorrelationWhileEventDeliveryIsBackpressured catches
// a direct blocking send to events from the sole correlation owner.
func TestMappingLoopServicesCorrelationWhileEventDeliveryIsBackpressured(t *testing.T) {
	const count = 4
	ctx, cancel := context.WithCancel(context.Background())
	session := &voiceSession{
		ctx: ctx, cancel: cancel,
		reads: make(chan socketRead, count), correlations: make(chan correlationRequest),
		events: make(chan realtime.Event, 1), closing: make(chan struct{}), done: make(chan struct{}),
		responses: map[string]*responseCorrelation{"response": {unbound: true}}, pending: make(map[identity.ResponseEpoch]struct{}),
		implicitUnbound: 1,
		maxFrameBytes:   1 << 20, closeNow: func() error { return nil },
	}
	go session.mappingLoop()
	t.Cleanup(cancel)

	for index := 1; index <= count; index++ {
		raw := []byte(fmt.Sprintf(`{"type":"input_audio_buffer.speech_started","event_id":"event-%d","item_id":"item-%d","audio_start_ms":%d}`, index, index, index))
		select {
		case session.reads <- socketRead{messageType: websocket.MessageText, raw: raw}:
		case <-time.After(time.Second):
			t.Fatalf("mapped event %d was not accepted", index)
		}
		if index == 1 {
			require.Eventually(t, func() bool { return len(session.events) == 1 }, time.Second, time.Millisecond)
		}
	}
	// Give the current implementation time to enter its second direct event
	// send. The bounded implementation instead owns the accepted tail locally.
	time.Sleep(20 * time.Millisecond)

	correlated := make(chan error, 2)
	go func() {
		correlated <- session.correlate(context.Background(), correlationRequest{kind: correlationBind, epoch: 7, responseID: "response"})
		correlated <- session.correlate(context.Background(), correlationRequest{kind: correlationCancel, epoch: 8, responseID: "response"})
	}()
	for _, kind := range []string{"bind", "cancel"} {
		select {
		case err := <-correlated:
			require.NoError(t, err)
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("%s correlation did not progress while public event delivery was backpressured", kind)
		}
	}
	require.Equal(t, identity.ResponseEpoch(7), session.responses["response"].epoch)
	require.Equal(t, identity.ResponseEpoch(8), session.responses["response"].cancelEpoch)

	for index := 1; index <= count; index++ {
		select {
		case event := <-session.events:
			require.Equal(t, fmt.Sprintf("event-%d", index), event.ProviderEventID)
		case <-time.After(time.Second):
			t.Fatalf("mapped event %d was silently dropped", index)
		}
	}
}

func TestConcurrentTerminationDrainsAcceptedMappedFIFO(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	session := &voiceSession{
		ctx: ctx, cancel: cancel,
		reads: make(chan socketRead), correlations: make(chan correlationRequest),
		events: make(chan realtime.Event, 1), closing: make(chan struct{}), done: make(chan struct{}),
		responses: make(map[string]*responseCorrelation), pending: make(map[identity.ResponseEpoch]struct{}),
		maxFrameBytes: 1 << 20, closeNow: func() error { return nil },
	}
	go session.mappingLoop()
	for index := 1; index <= 3; index++ {
		raw := []byte(fmt.Sprintf(`{"type":"input_audio_buffer.speech_started","event_id":"tail-%d","item_id":"item-%d","audio_start_ms":%d}`, index, index, index))
		session.reads <- socketRead{messageType: websocket.MessageText, raw: raw}
		require.NoError(t, session.correlate(t.Context(), correlationRequest{kind: correlationRegister, epoch: identity.ResponseEpoch(index)}))
	}
	session.terminate(errors.New("concurrent writer failure"))

	for index := 1; index <= 3; index++ {
		event, err := session.Receive(t.Context())
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("tail-%d", index), event.ProviderEventID)
	}
	_, err := session.Receive(t.Context())
	require.EqualError(t, err, "concurrent writer failure")
}

// TestCloseAfterWriterTerminalHandoffCompletes catches Close enqueueing work
// after the outbound writer has handed terminal ownership to the mapping loop
// and exited. The accepted tail must remain receivable before every concurrent
// Close completes, and closing must reject any later Send.
func TestCloseAfterWriterTerminalHandoffCompletes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	writeErr := errors.New("outbound write failed")
	session := &voiceSession{
		ctx: ctx, cancel: cancel,
		writeMessage:   func(context.Context, websocket.MessageType, []byte) error { return writeErr },
		closeHandshake: func(websocket.StatusCode, string) error { return nil }, closeNow: func() error { return nil },
		maxFrameBytes: 1 << 20,
		writes:        make(chan writeRequest, 1), closeWrites: make(chan struct{}, 1),
		reads: make(chan socketRead), correlations: make(chan correlationRequest), events: make(chan realtime.Event, 1),
		closing: make(chan struct{}), closeDone: make(chan struct{}), done: make(chan struct{}),
		responses: make(map[string]*responseCorrelation), pending: make(map[identity.ResponseEpoch]struct{}),
	}
	go session.mappingLoop()
	writerExited := make(chan struct{})
	go func() {
		session.writeLoop()
		close(writerExited)
	}()

	for index := 1; index <= 2; index++ {
		raw := []byte(fmt.Sprintf(`{"type":"input_audio_buffer.speech_started","event_id":"close-tail-%d","item_id":"item-%d","audio_start_ms":%d}`, index, index, index))
		session.reads <- socketRead{messageType: websocket.MessageText, raw: raw}
		require.NoError(t, session.correlate(t.Context(), correlationRequest{kind: correlationRegister, epoch: identity.ResponseEpoch(index)}))
	}
	require.ErrorIs(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCommitAudio}), writeErr)
	select {
	case <-writerExited:
	case <-time.After(time.Second):
		t.Fatal("writer did not finish terminal handoff")
	}

	closeCtx, closeCancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer closeCancel()
	const closers = 8
	closeResults := make(chan error, closers)
	for range closers {
		go func() { closeResults <- session.Close(closeCtx) }()
	}
	require.Eventually(t, session.isClosing, time.Second, time.Millisecond)
	require.ErrorIs(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCommitAudio}), ErrSessionClosed)

	for index := 1; index <= 2; index++ {
		event, err := session.Receive(t.Context())
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("close-tail-%d", index), event.ProviderEventID)
	}
	_, err := session.Receive(t.Context())
	require.ErrorIs(t, err, writeErr)
	for range closers {
		require.NoError(t, <-closeResults)
	}
	require.NoError(t, session.Close(t.Context()))
}

// TestReceiveBlockedBeforeWriterTerminalHandoffPreservesAcceptedTail catches a
// receiver retaining the normal-close signal after terminal ownership moves to
// the mapping loop. The correlation result deliberately holds the mapping
// owner after it accepts the tail so Close cannot race with tail delivery.
func TestReceiveBlockedBeforeWriterTerminalHandoffPreservesAcceptedTail(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	writeErr := errors.New("outbound write failed")
	session := &voiceSession{
		ctx: ctx, cancel: cancel,
		writeMessage:   func(context.Context, websocket.MessageType, []byte) error { return writeErr },
		closeHandshake: func(websocket.StatusCode, string) error { return nil }, closeNow: func() error { return nil },
		maxFrameBytes: 1 << 20,
		writes:        make(chan writeRequest, 1), closeWrites: make(chan struct{}, 1),
		reads: make(chan socketRead), correlations: make(chan correlationRequest), events: make(chan realtime.Event, 1),
		closing: make(chan struct{}), closeDone: make(chan struct{}), done: make(chan struct{}),
		responses: make(map[string]*responseCorrelation), pending: make(map[identity.ResponseEpoch]struct{}),
	}
	go session.mappingLoop()
	writerExited := make(chan struct{})
	go func() {
		session.writeLoop()
		close(writerExited)
	}()

	session.events <- realtime.Event{Kind: realtime.EventSessionCreated, ProviderEventID: "public-prefix"}
	session.reads <- socketRead{
		messageType: websocket.MessageText,
		raw:         []byte(`{"type":"input_audio_buffer.speech_started","event_id":"accepted-tail","item_id":"item","audio_start_ms":1}`),
	}
	blockerResult := make(chan error)
	session.correlations <- correlationRequest{
		kind: correlationRegister, ctx: t.Context(), epoch: 1, result: blockerResult,
	}
	require.Equal(t, "public-prefix", (<-session.events).ProviderEventID)

	receiveContext := newCloseWaitContext(t.Context())
	type receiveOutcome struct {
		event realtime.Event
		err   error
	}
	receiveResult := make(chan receiveOutcome, 1)
	go func() {
		event, err := session.Receive(receiveContext)
		receiveResult <- receiveOutcome{event: event, err: err}
	}()
	<-receiveContext.observed

	require.ErrorIs(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCommitAudio}), writeErr)
	select {
	case <-writerExited:
	case <-time.After(time.Second):
		t.Fatal("writer did not finish terminal handoff")
	}
	closeResult := make(chan error, 1)
	go func() { closeResult <- session.Close(t.Context()) }()
	require.Eventually(t, session.isClosing, time.Second, time.Millisecond)

	var received receiveOutcome
	select {
	case received = <-receiveResult:
	case <-time.After(50 * time.Millisecond):
	}
	require.NoError(t, <-blockerResult)
	if received.event.Kind == 0 && received.err == nil {
		select {
		case received = <-receiveResult:
		case <-time.After(time.Second):
			t.Fatal("blocked Receive did not resume with the accepted tail")
		}
	}
	require.NoError(t, received.err)
	require.Equal(t, "accepted-tail", received.event.ProviderEventID)

	_, err := session.Receive(t.Context())
	require.ErrorIs(t, err, writeErr)
	require.NoError(t, <-closeResult)
}

func newAggregateEventBoundSession(t *testing.T, accepted int) *voiceSession {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	session := &voiceSession{
		ctx: ctx, cancel: cancel,
		reads: make(chan socketRead), correlations: make(chan correlationRequest),
		events: make(chan realtime.Event, receiveQueueCapacity), closing: make(chan struct{}), done: make(chan struct{}),
		responses: make(map[string]*responseCorrelation), pending: make(map[identity.ResponseEpoch]struct{}),
		maxFrameBytes: 1 << 20, closeNow: func() error { return nil },
	}
	for index := range accepted {
		session.events <- realtime.Event{Kind: realtime.EventSessionUpdated, ProviderEventID: fmt.Sprintf("accepted-%03d", index)}
	}
	go session.mappingLoop()
	t.Cleanup(func() {
		session.publishTerminal(errors.New("aggregate event bound test cleanup"))
	})
	return session
}

func implicitTerminalTranscriptRead(responseID string) socketRead {
	return socketRead{
		messageType: websocket.MessageText,
		raw: []byte(fmt.Sprintf(
			`{"type":"response.output_audio_transcript.done","event_id":"implicit-terminal","response_id":%q,"item_id":"assistant-item","transcript":"done"}`,
			responseID,
		)),
	}
}

// TestImplicitTerminalTranscriptRespectsAggregateAcceptedEventBound catches
// admission that counts the provider frame but not its synthetic
// response.created plus original terminal-transcript expansion.
func TestImplicitTerminalTranscriptRespectsAggregateAcceptedEventBound(t *testing.T) {
	t.Run("exact boundary accepts both events FIFO", func(t *testing.T) {
		const existing = receiveQueueCapacity - 2
		session := newAggregateEventBoundSession(t, existing)
		session.reads <- implicitTerminalTranscriptRead("exact-boundary")

		require.NoError(t, session.correlate(t.Context(), correlationRequest{
			kind: correlationBind, epoch: 17, responseID: "exact-boundary",
		}))
		require.Eventually(t, func() bool {
			return len(session.events) == receiveQueueCapacity
		}, time.Second, time.Millisecond)

		for index := range existing {
			event := <-session.events
			require.Equal(t, fmt.Sprintf("accepted-%03d", index), event.ProviderEventID)
		}
		created := <-session.events
		require.Equal(t, realtime.EventResponseCreated, created.Kind)
		require.Equal(t, "exact-boundary", created.ResponseID)
		terminal := <-session.events
		require.Equal(t, realtime.EventTranscriptDone, terminal.Kind)
		require.Equal(t, "implicit-terminal", terminal.ProviderEventID)
		require.Equal(t, identity.ResponseEpoch(17), terminal.Epoch)
	})

	t.Run("one over boundary fails closed without partial expansion", func(t *testing.T) {
		const existing = receiveQueueCapacity - 1
		session := newAggregateEventBoundSession(t, existing)
		session.reads <- implicitTerminalTranscriptRead("over-boundary")

		select {
		case <-session.done:
		case <-time.After(100 * time.Millisecond):
			t.Fatal("aggregate overflow did not fail the session closed")
		}
		require.ErrorIs(t, session.sessionError(), ErrAcceptedEventLimit)
		require.Len(t, session.events, existing, "overflow must not expose only the synthetic half of the frame")
	})
}
