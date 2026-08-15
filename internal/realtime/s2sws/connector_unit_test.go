package s2sws

import (
	"context"
	"encoding/json"
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

	err := session.Send(context.Background(), realtime.Command{
		Kind: realtime.CommandCreateItem, Item: json.RawMessage(`{"type":"message"}`),
	})
	require.ErrorIs(t, err, rawWriteError)
	<-writerDone
}

func TestWaitForSendResultNormalizesReadyResultDuringLocalClose(t *testing.T) {
	rawWriteError := errors.New("raw canceled write")
	closing := make(chan struct{})
	close(closing)
	session := &voiceSession{closing: closing, done: make(chan struct{})}
	result := make(chan error, 1)

	for range 1024 {
		select {
		case result <- rawWriteError:
		default:
		}
		require.ErrorIs(t, session.waitForSendResult(context.Background(), result), ErrSessionClosed)
	}
}

func TestWaitForSendResultPreservesReadyResultWithoutLocalClose(t *testing.T) {
	rawWriteError := errors.New("raw write failure")

	for _, test := range []struct {
		name       string
		doneClosed bool
	}{
		{name: "direct result"},
		{name: "terminal result", doneClosed: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			closing := make(chan struct{})
			done := make(chan struct{})
			if test.doneClosed {
				close(done)
			}
			session := &voiceSession{closing: closing, done: done}
			result := make(chan error, 1)

			for range 1024 {
				select {
				case result <- rawWriteError:
				default:
				}
				require.ErrorIs(t, session.waitForSendResult(context.Background(), result), rawWriteError)
			}
		})
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

func TestCorrelatedProviderErrorReleasesPendingExplicitBudget(t *testing.T) {
	session := &voiceSession{
		ctx: context.Background(), events: make(chan realtime.Event, 1),
		pending: map[identity.ResponseEpoch]struct{}{7: {}},
	}
	require.NoError(t, session.handleMapped(mappedServerEvent{event: realtime.Event{Kind: realtime.EventError, Epoch: 7}}))
	assert.Empty(t, session.pending)
	assert.Equal(t, identity.ResponseEpoch(7), (<-session.events).Epoch)
}

func TestCorrelatedRejectionsBeyondPendingLimitRemainRecoverable(t *testing.T) {
	session := &voiceSession{ctx: context.Background(), events: make(chan realtime.Event, explicitPendingLimit+8), pending: make(map[identity.ResponseEpoch]struct{})}
	for value := 1; value <= explicitPendingLimit+5; value++ {
		epoch := identity.ResponseEpoch(value)
		require.NoError(t, session.registerExplicit(epoch))
		require.NoError(t, session.handleMapped(mappedServerEvent{event: realtime.Event{Kind: realtime.EventError, Epoch: epoch}}))
		assert.Empty(t, session.pending)
		delivered := <-session.events
		assert.Equal(t, realtime.EventError, delivered.Kind)
		assert.Equal(t, epoch, delivered.Epoch)
	}
}

func TestEpochZeroErrorPreservesPendingCreateAndValidCreationBinds(t *testing.T) {
	session := &voiceSession{
		ctx: context.Background(), events: make(chan realtime.Event, 3),
		pending: map[identity.ResponseEpoch]struct{}{7: {}}, responses: make(map[string]*responseCorrelation),
	}
	require.NoError(t, session.handleMapped(mappedServerEvent{event: realtime.Event{Kind: realtime.EventError}}))
	assert.Contains(t, session.pending, identity.ResponseEpoch(7), "uncorrelated error must preserve the valid create")
	assert.Equal(t, realtime.EventError, (<-session.events).Kind)

	require.NoError(t, session.handleMapped(mappedServerEvent{
		event:   realtime.Event{Kind: realtime.EventResponseCreated, ResponseID: "healthy", Status: "in_progress"},
		created: &responseCreatedDetails{responseID: "healthy", hasEpoch: true, epoch: 7},
	}))
	assert.Empty(t, session.pending)
	created := <-session.events
	assert.Equal(t, realtime.EventResponseCreated, created.Kind)
	assert.Equal(t, identity.ResponseEpoch(7), created.Epoch)
	assert.False(t, created.NeedsEpochBinding)

	session.pending[101] = struct{}{}
	session.pending[102] = struct{}{}
	require.NoError(t, session.handleMapped(mappedServerEvent{event: realtime.Event{Kind: realtime.EventError}}))
	assert.Len(t, session.pending, 2, "ambiguous error must preserve every pending create")
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
