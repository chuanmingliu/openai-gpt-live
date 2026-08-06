package s2sws

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type closeWaitContext struct {
	context.Context
	observed chan struct{}
	once     sync.Once
}

type secondSendWaitContext struct {
	context.Context
	calls       atomic.Int32
	waiting     chan struct{}
	releaseWait chan struct{}
	waitOnce    sync.Once
}

func newSecondSendWaitContext(parent context.Context) *secondSendWaitContext {
	return &secondSendWaitContext{
		Context:     parent,
		waiting:     make(chan struct{}),
		releaseWait: make(chan struct{}),
	}
}

func (c *secondSendWaitContext) Done() <-chan struct{} {
	if c.calls.Add(1) == 2 {
		c.waitOnce.Do(func() { close(c.waiting) })
		<-c.releaseWait
	}
	return c.Context.Done()
}

func newCloseWaitContext(parent context.Context) *closeWaitContext {
	return &closeWaitContext{Context: parent, observed: make(chan struct{})}
}

func (c *closeWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.observed) })
	return c.Context.Done()
}

func newUnitCloseSession(
	t *testing.T,
	closeHandshake func(websocket.StatusCode, string) error,
	closeNow func() error,
) (*voiceSession, <-chan struct{}) {
	t.Helper()
	sessionContext, cancelSession := context.WithCancel(context.Background())
	session := &voiceSession{
		ctx:            sessionContext,
		cancel:         cancelSession,
		writes:         make(chan writeRequest, writeQueueCapacity),
		closeWrites:    make(chan struct{}, 1),
		closing:        make(chan struct{}),
		closeDone:      make(chan struct{}),
		done:           make(chan struct{}),
		closeHandshake: closeHandshake,
		closeNow:       closeNow,
	}
	writerDone := make(chan struct{})
	go func() {
		session.writeLoop()
		close(writerDone)
	}()
	return session, writerDone
}

func newUnitWriteSession(
	t *testing.T,
	writeMessage func(context.Context, websocket.MessageType, []byte) error,
	closeHandshake func(websocket.StatusCode, string) error,
) (*voiceSession, <-chan struct{}) {
	t.Helper()
	sessionContext, cancelSession := context.WithCancel(context.Background())
	session := &voiceSession{
		ctx:            sessionContext,
		cancel:         cancelSession,
		writeMessage:   writeMessage,
		closeHandshake: closeHandshake,
		closeNow:       func() error { return nil },
		maxFrameBytes:  1 << 20,
		writes:         make(chan writeRequest, writeQueueCapacity),
		closeWrites:    make(chan struct{}, 1),
		closing:        make(chan struct{}),
		closeDone:      make(chan struct{}),
		done:           make(chan struct{}),
	}
	writerDone := make(chan struct{})
	go func() {
		session.writeLoop()
		close(writerDone)
	}()
	return session, writerDone
}

func TestSendPreservesWriteErrorWithoutLocalClose(t *testing.T) {
	rawWriteError := errors.New("raw write failure")
	session, writerDone := newUnitWriteSession(t,
		func(context.Context, websocket.MessageType, []byte) error { return rawWriteError },
		func(websocket.StatusCode, string) error { return nil },
	)

	err := session.Send(context.Background(), realtime.Command{Kind: realtime.CommandCommitAudio})
	require.ErrorIs(t, err, rawWriteError)
	<-writerDone
}

func TestSendNormalizesCanceledWriteAfterLocalCloseWithoutNetwork(t *testing.T) {
	const (
		iterations = 128
		senders    = 8
	)
	rawWriteError := errors.New("raw canceled write")
	closeHandshakeError := errors.New("close handshake result")

	for iteration := range iterations {
		writeStarted := make(chan struct{})
		var writeStartedOnce sync.Once
		closeHandshakeStarted := make(chan struct{})
		closeHandshakeRelease := make(chan struct{})
		session, writerDone := newUnitWriteSession(t,
			func(ctx context.Context, _ websocket.MessageType, _ []byte) error {
				writeStartedOnce.Do(func() { close(writeStarted) })
				<-ctx.Done()
				return rawWriteError
			},
			func(websocket.StatusCode, string) error {
				close(closeHandshakeStarted)
				<-closeHandshakeRelease
				return closeHandshakeError
			},
		)

		activeContext := newSecondSendWaitContext(context.Background())
		activeSendResult := make(chan error, 1)
		go func() {
			activeSendResult <- session.Send(activeContext, realtime.Command{Kind: realtime.CommandCommitAudio})
		}()
		<-activeContext.waiting
		<-writeStarted

		concurrentSendResults := make(chan error, senders-1)
		for range senders - 1 {
			go func() {
				concurrentSendResults <- session.Send(context.Background(), realtime.Command{Kind: realtime.CommandCommitAudio})
			}()
		}

		closeResult := make(chan error, 1)
		go func() { closeResult <- session.Close(context.Background()) }()
		<-closeHandshakeStarted
		close(activeContext.releaseWait)
		activeError := <-activeSendResult
		for range senders - 1 {
			require.ErrorIs(t, <-concurrentSendResults, ErrSessionClosed)
		}
		close(closeHandshakeRelease)
		require.ErrorIs(t, <-closeResult, closeHandshakeError)
		<-writerDone
		if !errors.Is(activeError, ErrSessionClosed) {
			t.Fatalf("iteration %d: active Send error = %v, want %v", iteration, activeError, ErrSessionClosed)
		}
	}
}

func TestPendingExplicitBudgetWithoutSocket(t *testing.T) {
	session := &voiceSession{pending: make(map[identity.ResponseEpoch]struct{})}
	for value := 1; value <= explicitPendingLimit; value++ {
		require.NoError(t, session.registerExplicit(identity.ResponseEpoch(value)))
	}
	assert.Len(t, session.pending, explicitPendingLimit)

	overLimit := identity.ResponseEpoch(explicitPendingLimit + 1)
	err := session.registerExplicit(overLimit)
	require.ErrorIs(t, err, ErrPendingResponseLimit)
	assert.Len(t, session.pending, explicitPendingLimit)
	assert.NotContains(t, session.pending, overLimit)

	matched, err := session.resolvePendingExplicit(identity.ResponseEpoch(1), true)
	require.NoError(t, err)
	require.True(t, matched)
	assert.Len(t, session.pending, explicitPendingLimit-1)
	require.NoError(t, session.registerExplicit(overLimit))
	assert.Len(t, session.pending, explicitPendingLimit)
}

func TestPendingExplicitResolutionValidationWithoutSocket(t *testing.T) {
	session := &voiceSession{pending: map[identity.ResponseEpoch]struct{}{7: {}}}

	matched, err := session.resolvePendingExplicit(0, false)
	require.ErrorContains(t, err, "missing live_epoch")
	assert.False(t, matched)
	assert.Contains(t, session.pending, identity.ResponseEpoch(7))

	matched, err = session.resolvePendingExplicit(8, true)
	require.ErrorContains(t, err, "unmatched live_epoch 8")
	assert.False(t, matched)
	assert.Contains(t, session.pending, identity.ResponseEpoch(7))

	matched, err = session.resolvePendingExplicit(7, true)
	require.NoError(t, err)
	require.True(t, matched)
	assert.Empty(t, session.pending)

	matched, err = session.resolvePendingExplicit(0, false)
	require.NoError(t, err)
	assert.False(t, matched)

	matched, err = session.resolvePendingExplicit(9, true)
	require.ErrorContains(t, err, "unregistered live_epoch 9")
	assert.False(t, matched)
}

func TestCloseWaitersShareSocketAttemptWithoutNetwork(t *testing.T) {
	closeStarted := make(chan struct{})
	closeRelease := make(chan struct{})
	var closeReleaseOnce sync.Once
	closeCode := make(chan websocket.StatusCode, 1)
	closeResult := errors.New("close handshake failed")
	var closeCalls atomic.Int32
	var closeNowCalls atomic.Int32
	session, writerDone := newUnitCloseSession(t,
		func(code websocket.StatusCode, _ string) error {
			closeCalls.Add(1)
			closeCode <- code
			close(closeStarted)
			<-closeRelease
			return closeResult
		},
		func() error {
			closeNowCalls.Add(1)
			return nil
		},
	)
	t.Cleanup(func() {
		closeReleaseOnce.Do(func() { close(closeRelease) })
		_ = session.Close(context.Background())
		<-writerDone
	})

	firstParent, cancelFirst := context.WithCancel(context.Background())
	firstContext := newCloseWaitContext(firstParent)
	firstResult := make(chan error, 1)
	go func() { firstResult <- session.Close(firstContext) }()
	<-closeStarted
	<-firstContext.observed
	select {
	case err := <-firstResult:
		t.Fatalf("first Close returned before its context or the handshake completed: %v", err)
	default:
	}
	cancelFirst()
	require.ErrorIs(t, <-firstResult, context.Canceled)

	const laterCallers = 16
	laterResults := make(chan error, laterCallers)
	laterContexts := make([]*closeWaitContext, 0, laterCallers)
	for range laterCallers {
		closeContext := newCloseWaitContext(context.Background())
		laterContexts = append(laterContexts, closeContext)
		go func() { laterResults <- session.Close(closeContext) }()
	}
	for _, closeContext := range laterContexts {
		<-closeContext.observed
	}
	select {
	case err := <-laterResults:
		t.Fatalf("later Close returned before the shared handshake completed: %v", err)
	default:
	}

	closeReleaseOnce.Do(func() { close(closeRelease) })
	for range laterCallers {
		require.ErrorIs(t, <-laterResults, closeResult)
	}
	<-writerDone
	require.ErrorIs(t, session.Close(context.Background()), closeResult)
	assert.Equal(t, int32(1), closeCalls.Load())
	assert.Equal(t, int32(1), closeNowCalls.Load())
	assert.Equal(t, websocket.StatusNormalClosure, <-closeCode)
	select {
	case <-session.done:
	default:
		t.Fatal("session loops were not terminated before close completion")
	}
}

func TestCloseNormalizesClosedSocketWithoutNetwork(t *testing.T) {
	var closeCalls atomic.Int32
	session, writerDone := newUnitCloseSession(t,
		func(code websocket.StatusCode, reason string) error {
			closeCalls.Add(1)
			assert.Equal(t, websocket.StatusNormalClosure, code)
			assert.Empty(t, reason)
			return net.ErrClosed
		},
		func() error { return nil },
	)

	require.NoError(t, session.Close(context.Background()))
	<-writerDone
	require.NoError(t, session.Close(context.Background()))
	assert.Equal(t, int32(1), closeCalls.Load())
}
