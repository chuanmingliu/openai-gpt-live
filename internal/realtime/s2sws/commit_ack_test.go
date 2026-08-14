package s2sws

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

func newCommitAckTestSession(
	t *testing.T,
	timeout time.Duration,
	writeMessage func(context.Context, websocket.MessageType, []byte) error,
) *voiceSession {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	session := &voiceSession{
		ctx:              ctx,
		cancel:           cancel,
		writeMessage:     writeMessage,
		closeHandshake:   func(websocket.StatusCode, string) error { return nil },
		closeNow:         func() error { return nil },
		maxFrameBytes:    1 << 20,
		commitAckTimeout: timeout,
		writes:           make(chan writeRequest, writeQueueCapacity),
		closeWrites:      make(chan struct{}, 1),
		reads:            make(chan socketRead),
		correlations:     make(chan correlationRequest),
		events:           make(chan realtime.Event, 8),
		closing:          make(chan struct{}),
		closeDone:        make(chan struct{}),
		done:             make(chan struct{}),
		responses:        make(map[string]*responseCorrelation),
		pending:          make(map[identity.ResponseEpoch]struct{}),
		cancelledPending: make(map[identity.ResponseEpoch]identity.ResponseEpoch),
	}
	mappingDone := make(chan struct{})
	go func() {
		session.mappingLoop()
		close(mappingDone)
	}()
	writerDone := make(chan struct{})
	go func() {
		session.writeLoop()
		close(writerDone)
	}()
	t.Cleanup(func() {
		_ = session.Close(context.Background())
		for name, done := range map[string]<-chan struct{}{
			"mapping": mappingDone,
			"writer":  writerDone,
		} {
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Errorf("%s loop leaked after session close", name)
			}
		}
	})
	return session
}

func sendCommitAckTestRead(t *testing.T, session *voiceSession, raw string) {
	t.Helper()
	select {
	case session.reads <- socketRead{
		messageType: websocket.MessageText, raw: []byte(raw), sequence: session.readSequence.Add(1),
	}:
	case <-time.After(time.Second):
		t.Fatal("mapping loop did not accept provider event")
	}
}

func authorizeCommitAck(t *testing.T, session *voiceSession, itemID string, sequence uint64) {
	t.Helper()
	require.NoError(t, session.Send(t.Context(), realtime.Command{
		Kind: realtime.CommandAcknowledgeAudioCommit, InputItemID: itemID, InputAudioCommitSequence: sequence,
	}))
}

// TestCommitAckTimeoutConfig catches a zero timeout reaching a live session
// without a deadline and a negative timeout being accepted as meaningful.
func TestCommitAckTimeoutConfig(t *testing.T) {
	valid := Config{
		URL:            "ws://127.0.0.1:8080/v1/realtime",
		ConnectTimeout: time.Second,
		MaxFrameBytes:  1 << 20,
	}

	t.Run("zero selects a safe default", func(t *testing.T) {
		connector, err := New(valid)
		require.NoError(t, err)
		require.Positive(t, connector.cfg.CommitAckTimeout)
	})

	t.Run("negative is rejected", func(t *testing.T) {
		cfg := valid
		cfg.CommitAckTimeout = -time.Nanosecond
		_, err := New(cfg)
		require.Error(t, err)
	})
}

func TestCloneCommandOwnsInputItemID(t *testing.T) {
	backing := []byte("owned-input")
	command := realtime.Command{
		Kind: realtime.CommandCommitAudio, InputItemID: unsafe.String(&backing[0], len(backing)),
	}
	cloned := cloneCommand(command)
	backing[0] = 'X'
	require.Equal(t, "owned-input", cloned.InputItemID)
}

func TestCorrelateWaitsForDefinitiveResultAfterEnqueue(t *testing.T) {
	session := &voiceSession{
		correlations: make(chan correlationRequest),
		closing:      make(chan struct{}),
		done:         make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	admitted := make(chan struct{})
	release := make(chan struct{})
	go func() {
		request := <-session.correlations
		close(admitted)
		<-release
		request.result <- nil
	}()
	result := make(chan error, 1)
	go func() {
		result <- session.correlate(ctx, correlationRequest{kind: correlationRegisterAudioCommit})
	}()

	<-admitted
	cancel()
	select {
	case err := <-result:
		close(release)
		require.NoError(t, err, "an enqueued correlation must return the mapping owner's definitive result")
	case <-time.After(20 * time.Millisecond):
		close(release)
		require.NoError(t, <-result)
	}
}

// TestCommitAudioRegistersBeforeWireWrite catches registration after the
// provider frame, which permits an immediate second commit to escape.
func TestCommitAudioRegistersBeforeWireWrite(t *testing.T) {
	var session *voiceSession
	writeObserved := make(chan error, 1)
	session = newCommitAckTestSession(t, time.Second, func(context.Context, websocket.MessageType, []byte) error {
		writeObserved <- session.correlate(context.Background(), correlationRequest{kind: correlationRegisterAudioCommit})
		return nil
	})

	require.NoError(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCommitAudio, Sequence: 1}))
	require.ErrorIs(t, <-writeObserved, ErrPendingAudioCommit)
}

func TestCommitSequenceMustBePositiveAndNeverReused(t *testing.T) {
	session := newCommitAckTestSession(t, time.Second, func(context.Context, websocket.MessageType, []byte) error { return nil })

	require.Error(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCommitAudio}))
	require.NoError(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCommitAudio, Sequence: 10}))
	sendCommitAckTestRead(t, session, `{"type":"input_audio_buffer.committed","event_id":"ack","item_id":"input","previous_item_id":null}`)
	event, err := session.Receive(t.Context())
	require.NoError(t, err)
	require.NoError(t, session.Send(t.Context(), realtime.Command{
		Kind: realtime.CommandAcknowledgeAudioCommit, InputItemID: event.ItemID, InputAudioCommitSequence: 10,
	}))
	require.Error(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCommitAudio, Sequence: 10}))
}

// TestPendingCommitRejectsWithoutWriteAndAckReopens catches a second commit
// being written or poisoning the session, and catches previous_item_id being
// misused as commit correlation instead of whole-conversation metadata.
func TestPendingCommitRejectsWithoutWriteAndAckReopens(t *testing.T) {
	var writes atomic.Int32
	session := newCommitAckTestSession(t, time.Second, func(context.Context, websocket.MessageType, []byte) error {
		writes.Add(1)
		return nil
	})

	require.NoError(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCommitAudio, Sequence: 1}))
	require.Equal(t, int32(1), writes.Load())

	err := session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCommitAudio, Sequence: 2})
	require.ErrorIs(t, err, ErrPendingAudioCommit)
	require.True(t, errors.Is(err, ErrPendingAudioCommit))
	require.Equal(t, ErrPendingAudioCommit.Error(), err.Error())
	require.Equal(t, int32(1), writes.Load(), "rejected commit must not reach the wire")

	sendCommitAckTestRead(t, session, `{"type":"input_audio_buffer.committed","event_id":"ack-a","item_id":"input-a","previous_item_id":"assistant-or-tool-tail"}`)
	event, receiveErr := session.Receive(t.Context())
	require.NoError(t, receiveErr)
	require.Equal(t, realtime.EventInputAudioCommitted, event.Kind)
	require.Equal(t, "input-a", event.ItemID)
	require.True(t, event.InputAudioCommitCandidate)
	require.ErrorIs(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCommitAudio, Sequence: 2}), ErrPendingAudioCommit,
		"raw arrival cannot release the slot before semantic correlation")
	authorizeCommitAck(t, session, "input-a", 1)

	require.NoError(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCommitAudio, Sequence: 2}))
	require.Equal(t, int32(2), writes.Load())
	sendCommitAckTestRead(t, session, `{"type":"input_audio_buffer.committed","event_id":"ack-b","item_id":"input-b","previous_item_id":null}`)
	event, receiveErr = session.Receive(t.Context())
	require.NoError(t, receiveErr)
	require.Equal(t, realtime.EventInputAudioCommitted, event.Kind)
	require.Equal(t, "input-b", event.ItemID)
	authorizeCommitAck(t, session, "input-b", 2)
}

// TestPreRegistrationAckIsIneligibleForLaterCommit catches cross-loop
// reordering where an older automatic ack is delivered to the reactor only
// after a newer client commit has been applied. The transport's registration
// boundary makes the older event ineligible and refuses its release control.
func TestPreRegistrationAckIsIneligibleForLaterCommit(t *testing.T) {
	session := newCommitAckTestSession(t, time.Second, func(context.Context, websocket.MessageType, []byte) error { return nil })

	sendCommitAckTestRead(t, session, `{"type":"input_audio_buffer.committed","event_id":"old-auto","item_id":"old-input","previous_item_id":null}`)
	old, err := session.Receive(t.Context())
	require.NoError(t, err)
	require.False(t, old.InputAudioCommitCandidate)

	require.NoError(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCommitAudio, Sequence: 1}))
	err = session.Send(t.Context(), realtime.Command{
		Kind: realtime.CommandAcknowledgeAudioCommit, InputItemID: "old-input", InputAudioCommitSequence: 1,
	})
	require.ErrorContains(t, err, "was not observed")
	require.ErrorIs(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCommitAudio, Sequence: 2}), ErrPendingAudioCommit)

	sendCommitAckTestRead(t, session, `{"type":"input_audio_buffer.committed","event_id":"current","item_id":"current-input","previous_item_id":null}`)
	current, err := session.Receive(t.Context())
	require.NoError(t, err)
	require.True(t, current.InputAudioCommitCandidate)
	authorizeCommitAck(t, session, "current-input", 1)
}

func TestReadBufferedBeforeRegistrationRemainsIneligible(t *testing.T) {
	for iteration := range 32 {
		ctx, cancel := context.WithCancel(context.Background())
		session := &voiceSession{
			ctx: ctx, cancel: cancel, maxFrameBytes: 1 << 20, commitAckTimeout: time.Second,
			reads: make(chan socketRead, 1), correlations: make(chan correlationRequest, 1),
			events: make(chan realtime.Event, 1), closing: make(chan struct{}), done: make(chan struct{}),
			responses: make(map[string]*responseCorrelation), pending: make(map[identity.ResponseEpoch]struct{}),
			cancelledPending: make(map[identity.ResponseEpoch]identity.ResponseEpoch), closeNow: func() error { return nil },
		}
		session.readSequence.Store(1)
		session.reads <- socketRead{
			messageType: websocket.MessageText,
			raw:         []byte(`{"type":"input_audio_buffer.committed","event_id":"buffered","item_id":"buffered","previous_item_id":null}`),
			sequence:    1,
		}
		registered := make(chan error, 1)
		session.correlations <- correlationRequest{
			kind: correlationRegisterAudioCommit, ctx: context.Background(), inputAudioCommitSequence: 77, result: registered,
		}
		go session.mappingLoop()
		require.NoError(t, <-registered, "iteration %d", iteration)
		event, err := session.Receive(t.Context())
		require.NoError(t, err, "iteration %d", iteration)
		require.False(t, event.InputAudioCommitCandidate, "iteration %d", iteration)
		require.Zero(t, event.InputAudioCommitSequence, "iteration %d", iteration)
		session.publishTerminal(errors.New("watermark test cleanup"))
		<-session.done
		cancel()
	}
}

func TestBufferedCandidateFromPriorSlotCannotAuthorizeNextSlot(t *testing.T) {
	session := newCommitAckTestSession(t, time.Second, func(context.Context, websocket.MessageType, []byte) error { return nil })

	require.NoError(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCommitAudio, Sequence: 11}))
	sendCommitAckTestRead(t, session, `{"type":"input_audio_buffer.committed","event_id":"slot-one-a","item_id":"slot-one-a","previous_item_id":null}`)
	sendCommitAckTestRead(t, session, `{"type":"input_audio_buffer.committed","event_id":"slot-one-b","item_id":"slot-one-b","previous_item_id":null}`)
	first, err := session.Receive(t.Context())
	require.NoError(t, err)
	buffered, err := session.Receive(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint64(11), first.InputAudioCommitSequence)
	require.Equal(t, uint64(11), buffered.InputAudioCommitSequence)
	require.NoError(t, session.Send(t.Context(), realtime.Command{
		Kind: realtime.CommandAcknowledgeAudioCommit, InputItemID: first.ItemID, InputAudioCommitSequence: 11,
	}))

	require.NoError(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCommitAudio, Sequence: 22}))
	err = session.Send(t.Context(), realtime.Command{
		Kind: realtime.CommandAcknowledgeAudioCommit, InputItemID: buffered.ItemID, InputAudioCommitSequence: 11,
	})
	require.ErrorContains(t, err, "does not match pending generation")
	require.ErrorIs(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCommitAudio, Sequence: 23}), ErrPendingAudioCommit)

	sendCommitAckTestRead(t, session, `{"type":"input_audio_buffer.committed","event_id":"slot-two","item_id":"slot-two","previous_item_id":null}`)
	current, err := session.Receive(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint64(22), current.InputAudioCommitSequence)
	require.NoError(t, session.Send(t.Context(), realtime.Command{
		Kind: realtime.CommandAcknowledgeAudioCommit, InputItemID: current.ItemID, InputAudioCommitSequence: 22,
	}))
}

// TestCommitAckStopsDeadline catches a stale timer terminating a healthy
// session after the acknowledged slot has already reopened.
func TestCommitAckStopsDeadline(t *testing.T) {
	const timeout = 20 * time.Millisecond
	session := newCommitAckTestSession(t, timeout, func(context.Context, websocket.MessageType, []byte) error { return nil })

	require.NoError(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCommitAudio, Sequence: 1}))
	sendCommitAckTestRead(t, session, `{"type":"input_audio_buffer.committed","event_id":"ack","item_id":"input","previous_item_id":"tool-tail"}`)
	event, err := session.Receive(t.Context())
	require.NoError(t, err)
	require.Equal(t, realtime.EventInputAudioCommitted, event.Kind)
	authorizeCommitAck(t, session, "input", 1)

	select {
	case <-session.done:
		t.Fatalf("acknowledged commit retained a stale deadline: %v", session.sessionError())
	case <-time.After(3 * timeout):
	}

	require.NoError(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCommitAudio, Sequence: 2}))
	sendCommitAckTestRead(t, session, `{"type":"input_audio_buffer.committed","event_id":"ack-next","item_id":"input-next","previous_item_id":null}`)
	_, err = session.Receive(t.Context())
	require.NoError(t, err)
	authorizeCommitAck(t, session, "input-next", 2)
}

// TestCommitAckTimeoutDrainsAcceptedFIFO catches immediate termination that
// loses events already accepted ahead of the missing-ack deadline.
func TestCommitAckTimeoutDrainsAcceptedFIFO(t *testing.T) {
	const timeout = 30 * time.Millisecond
	session := newCommitAckTestSession(t, timeout, func(context.Context, websocket.MessageType, []byte) error { return nil })
	session.events <- realtime.Event{Kind: realtime.EventSessionUpdated, ProviderEventID: "accepted-prefix"}

	require.NoError(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCommitAudio, Sequence: 1}))
	sendCommitAckTestRead(t, session, `{"type":"input_audio_buffer.speech_started","event_id":"accepted-one","item_id":"input-one","audio_start_ms":1}`)
	sendCommitAckTestRead(t, session, `{"type":"input_audio_buffer.speech_started","event_id":"accepted-two","item_id":"input-two","audio_start_ms":2}`)
	// A synchronous request to the sole mapping owner proves both unbuffered
	// reads were fully mapped, rather than merely handed to the loop.
	require.NoError(t, session.correlate(t.Context(), correlationRequest{kind: correlationRegister, epoch: 1}))

	select {
	case <-session.done:
	case <-time.After(time.Second):
		t.Fatal("missing commit acknowledgment did not terminate the session")
	}

	for _, want := range []string{"accepted-prefix", "accepted-one", "accepted-two"} {
		event, err := session.Receive(t.Context())
		require.NoError(t, err)
		require.Equal(t, want, event.ProviderEventID)
	}
	_, err := session.Receive(t.Context())
	require.ErrorIs(t, err, ErrAudioCommitAckTimeout)
}

// TestHistoricalAckCannotReleaseNewCommit catches a duplicate acknowledgment
// being mistaken for the sole later outstanding commit.
func TestHistoricalAckCannotReleaseNewCommit(t *testing.T) {
	t.Run("duplicate leaves later commit to time out", func(t *testing.T) {
		const timeout = 30 * time.Millisecond
		var writes atomic.Int32
		session := newCommitAckTestSession(t, timeout, func(context.Context, websocket.MessageType, []byte) error {
			writes.Add(1)
			return nil
		})

		// This auto-VAD acknowledgment predates the manual slot and must remain
		// a tombstone even though no local commit was pending when it arrived.
		sendCommitAckTestRead(t, session, `{"type":"input_audio_buffer.committed","event_id":"ack-a","item_id":"input-a","previous_item_id":"assistant-tail"}`)
		_, err := session.Receive(t.Context())
		require.NoError(t, err)

		require.NoError(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCommitAudio, Sequence: 1}))
		sendCommitAckTestRead(t, session, `{"type":"input_audio_buffer.committed","event_id":"ack-a-duplicate","item_id":"input-a","previous_item_id":"different-tool-tail"}`)
		duplicate, err := session.Receive(t.Context())
		require.NoError(t, err)
		require.Equal(t, "input-a", duplicate.ItemID)

		require.ErrorIs(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCommitAudio, Sequence: 2}), ErrPendingAudioCommit)
		require.Equal(t, int32(1), writes.Load())
		_, err = session.Receive(t.Context())
		require.ErrorIs(t, err, ErrAudioCommitAckTimeout)
	})

	t.Run("new exact item clears later commit", func(t *testing.T) {
		const timeout = 20 * time.Millisecond
		session := newCommitAckTestSession(t, timeout, func(context.Context, websocket.MessageType, []byte) error { return nil })

		sendCommitAckTestRead(t, session, `{"type":"input_audio_buffer.committed","event_id":"ack-a","item_id":"input-a","previous_item_id":null}`)
		_, err := session.Receive(t.Context())
		require.NoError(t, err)
		require.NoError(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCommitAudio, Sequence: 1}))

		sendCommitAckTestRead(t, session, `{"type":"input_audio_buffer.committed","event_id":"ack-a-duplicate","item_id":"input-a","previous_item_id":"assistant-tail"}`)
		_, err = session.Receive(t.Context())
		require.NoError(t, err)
		sendCommitAckTestRead(t, session, `{"type":"input_audio_buffer.committed","event_id":"ack-c","item_id":"input-c","previous_item_id":"tool-tail"}`)
		committed, err := session.Receive(t.Context())
		require.NoError(t, err)
		require.Equal(t, "input-c", committed.ItemID)
		authorizeCommitAck(t, session, "input-c", 1)

		select {
		case <-session.done:
			t.Fatalf("new acknowledgment did not clear the later commit: %v", session.sessionError())
		case <-time.After(3 * timeout):
		}
		require.NoError(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCommitAudio, Sequence: 2}))
		sendCommitAckTestRead(t, session, `{"type":"input_audio_buffer.committed","event_id":"ack-d","item_id":"input-d","previous_item_id":null}`)
		_, err = session.Receive(t.Context())
		require.NoError(t, err)
		authorizeCommitAck(t, session, "input-d", 2)
	})
}

// TestDifferentAutoCommitCannotReleaseKnownManualCommit catches an unrelated
// automatic VAD commit cancelling the deadline for the sole manual slot. The
// event is still delivered, but only the exact manual item reopens admission.
func TestDifferentAutoCommitCannotReleaseKnownManualCommit(t *testing.T) {
	const timeout = 100 * time.Millisecond
	session := newCommitAckTestSession(t, timeout, func(context.Context, websocket.MessageType, []byte) error { return nil })

	require.NoError(t, session.Send(t.Context(), realtime.Command{
		Kind: realtime.CommandCommitAudio, Sequence: 1, InputItemID: "manual-input",
	}))
	sendCommitAckTestRead(t, session, `{"type":"input_audio_buffer.committed","event_id":"auto-ack","item_id":"auto-input","previous_item_id":null}`)
	auto, err := session.Receive(t.Context())
	require.NoError(t, err)
	require.Equal(t, "auto-input", auto.ItemID)
	require.False(t, auto.InputAudioCommitCandidate)
	require.ErrorIs(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCommitAudio, Sequence: 2}), ErrPendingAudioCommit)

	sendCommitAckTestRead(t, session, `{"type":"input_audio_buffer.committed","event_id":"manual-ack","item_id":"manual-input","previous_item_id":"tool-tail"}`)
	manual, err := session.Receive(t.Context())
	require.NoError(t, err)
	require.Equal(t, "manual-input", manual.ItemID)
	require.True(t, manual.InputAudioCommitCandidate)
	authorizeCommitAck(t, session, "manual-input", 1)
	require.NoError(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCommitAudio, Sequence: 2}))
	sendCommitAckTestRead(t, session, `{"type":"input_audio_buffer.committed","event_id":"next-ack","item_id":"next-input","previous_item_id":null}`)
	_, err = session.Receive(t.Context())
	require.NoError(t, err)
	authorizeCommitAck(t, session, "next-input", 2)
}

// TestKnownManualItemMayReuseHistoricalAckID catches tombstones overriding an
// explicit fresh speech generation. Provider item IDs may be reused after a
// new speech_started edge, so an exact live binding is stronger than history.
func TestKnownManualItemMayReuseHistoricalAckID(t *testing.T) {
	const timeout = 100 * time.Millisecond
	session := newCommitAckTestSession(t, timeout, func(context.Context, websocket.MessageType, []byte) error { return nil })

	sendCommitAckTestRead(t, session, `{"type":"input_audio_buffer.committed","event_id":"old-ack","item_id":"reused-input","previous_item_id":null}`)
	_, err := session.Receive(t.Context())
	require.NoError(t, err)

	require.NoError(t, session.Send(t.Context(), realtime.Command{
		Kind: realtime.CommandCommitAudio, Sequence: 1, InputItemID: "reused-input",
	}))
	sendCommitAckTestRead(t, session, `{"type":"input_audio_buffer.committed","event_id":"fresh-ack","item_id":"reused-input","previous_item_id":"assistant-tail"}`)
	_, err = session.Receive(t.Context())
	require.NoError(t, err)
	authorizeCommitAck(t, session, "reused-input", 1)
	require.NoError(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCommitAudio, Sequence: 2}))
	sendCommitAckTestRead(t, session, `{"type":"input_audio_buffer.committed","event_id":"next-ack","item_id":"next-input","previous_item_id":null}`)
	_, err = session.Receive(t.Context())
	require.NoError(t, err)
	authorizeCommitAck(t, session, "next-input", 2)
}

// TestSemanticAckReleasesUnknownManualCommit catches the same deadline leak
// when the provider supplies the manual item's exact ID only after the commit
// was sent. Raw speech/commit arrival cannot replace reactor authorization.
func TestSemanticAckReleasesUnknownManualCommit(t *testing.T) {
	const timeout = 100 * time.Millisecond
	session := newCommitAckTestSession(t, timeout, func(context.Context, websocket.MessageType, []byte) error { return nil })

	require.NoError(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCommitAudio, Sequence: 1}))
	sendCommitAckTestRead(t, session, `{"type":"input_audio_buffer.speech_started","event_id":"speech-manual","item_id":"manual-input","audio_start_ms":1}`)
	_, err := session.Receive(t.Context())
	require.NoError(t, err)
	sendCommitAckTestRead(t, session, `{"type":"input_audio_buffer.speech_started","event_id":"speech-auto","item_id":"auto-input","audio_start_ms":2}`)
	_, err = session.Receive(t.Context())
	require.NoError(t, err)
	sendCommitAckTestRead(t, session, `{"type":"input_audio_buffer.committed","event_id":"auto-ack","item_id":"auto-input","previous_item_id":null}`)
	_, err = session.Receive(t.Context())
	require.NoError(t, err)
	require.ErrorIs(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCommitAudio, Sequence: 2}), ErrPendingAudioCommit)

	sendCommitAckTestRead(t, session, `{"type":"input_audio_buffer.committed","event_id":"manual-ack","item_id":"manual-input","previous_item_id":null}`)
	_, err = session.Receive(t.Context())
	require.NoError(t, err)
	authorizeCommitAck(t, session, "manual-input", 1)
	require.NoError(t, session.Send(t.Context(), realtime.Command{Kind: realtime.CommandCommitAudio, Sequence: 2}))
	sendCommitAckTestRead(t, session, `{"type":"input_audio_buffer.committed","event_id":"next-ack","item_id":"next-input","previous_item_id":null}`)
	_, err = session.Receive(t.Context())
	require.NoError(t, err)
	authorizeCommitAck(t, session, "next-input", 2)
}

// TestAudioCommitAckHistoryExactBoundary catches eviction, off-by-one
// admission, and partial mutation when a new tombstone exceeds either bound.
func TestAudioCommitAckHistoryExactBoundary(t *testing.T) {
	var history audioCommitAckHistory
	exactID := func(index int) string {
		return fmt.Sprintf("%04d-", index) + strings.Repeat("x", audioCommitItemIDBytes-5)
	}

	for index := range audioCommitAckHistoryItems {
		duplicate, err := history.remember(exactID(index))
		require.NoError(t, err)
		require.False(t, duplicate)
	}
	duplicate, err := history.remember(exactID(0))
	require.NoError(t, err, "an existing tombstone remains admissible at the exact boundary")
	require.True(t, duplicate)

	over := exactID(audioCommitAckHistoryItems)
	for range 2 {
		duplicate, err = history.remember(over)
		require.ErrorIs(t, err, errAudioCommitAckHistoryLimit)
		require.False(t, duplicate, "rejected tombstone must never become retained state")
	}
}

// TestAudioCommitAckHistoryRejectsUnsafeItemIDs catches retained keys that can
// evade exact matching or exceed the bounded-memory contract.
func TestAudioCommitAckHistoryRejectsUnsafeItemIDs(t *testing.T) {
	tests := []struct {
		name string
		id   string
	}{
		{name: "empty", id: ""},
		{name: "over byte limit", id: strings.Repeat("x", audioCommitItemIDBytes+1)},
		{name: "invalid UTF-8", id: string([]byte{0xff})},
		{name: "ASCII space", id: "input item"},
		{name: "Unicode space", id: "input\u00a0item"},
		{name: "control", id: "input\x00item"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var history audioCommitAckHistory
			duplicate, err := history.remember(test.id)
			require.ErrorIs(t, err, errInvalidAudioCommitAckItemID)
			require.False(t, duplicate)

			duplicate, err = history.remember("safe-item")
			require.NoError(t, err, "invalid input must not corrupt later admission")
			require.False(t, duplicate)
		})
	}
}
