package feature

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGatesFailClosedAndApplyOnlyNewerCompleteSnapshots(t *testing.T) {
	gates := New(nil)
	assert.False(t, gates.Enabled(HandoffsEnabled))
	assert.False(t, gates.Enabled(ShadowMode))

	input := map[string]bool{HandoffsEnabled: true, ShadowMode: false}
	assert.True(t, gates.Apply(Snapshot{Version: 2, Values: input}))
	input[HandoffsEnabled] = false
	assert.True(t, gates.Enabled(HandoffsEnabled), "Apply must clone input")
	assert.False(t, gates.Apply(Snapshot{Version: 2, Values: map[string]bool{HandoffsEnabled: false, ShadowMode: true}}))
	assert.False(t, gates.Apply(Snapshot{Version: 3, Values: map[string]bool{HandoffsEnabled: false}}))
	assert.False(t, gates.Apply(Snapshot{Version: 4, Values: map[string]bool{HandoffsEnabled: false, ShadowMode: true, "unknown": true}}))
	assert.True(t, gates.Enabled(HandoffsEnabled), "partial high-version authority must not publish mixed-version state")
	assert.False(t, gates.Enabled(ShadowMode))
}

type blockingNotificationStore struct {
	*scriptedStore
	started chan struct{}
	stopped chan struct{}
}

func (s *blockingNotificationStore) RunNotifications(ctx context.Context) error {
	close(s.started)
	<-ctx.Done()
	close(s.stopped)
	return ctx.Err()
}

func TestRunCancellationCancelsAndJoinsNotificationRunner(t *testing.T) {
	manual := clock.NewManual(time.Unix(200, 0))
	store := &blockingNotificationStore{
		scriptedStore: &scriptedStore{loads: []Snapshot{{Version: 1, Values: map[string]bool{HandoffsEnabled: true, ShadowMode: false}}}},
		started:       make(chan struct{}),
		stopped:       make(chan struct{}),
	}
	gates := NewWithClock(nil, manual)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- gates.Run(ctx, store, time.Second, time.Second) }()
	<-store.started
	cancel()
	assert.ErrorIs(t, <-done, context.Canceled)
	select {
	case <-store.stopped:
	default:
		t.Fatal("notification runner remained alive after Gates.Run returned")
	}
}

func TestUnavailableStoreCannotReenableDisabledGate(t *testing.T) {
	gates := New(map[string]bool{HandoffsEnabled: true, ShadowMode: false})
	require.True(t, gates.Apply(Snapshot{Version: 2, Values: map[string]bool{HandoffsEnabled: false, ShadowMode: false}}))
	gates.RecordRefreshFailure()
	assert.False(t, gates.Enabled(HandoffsEnabled))
	assert.False(t, gates.Healthy())
}

func TestGatesConcurrentReadersAndWriters(t *testing.T) {
	gates := New(nil)
	var wg sync.WaitGroup
	for reader := 0; reader < 32; reader++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 2_000; i++ {
				_ = gates.Enabled(HandoffsEnabled)
				_ = gates.Snapshot()
			}
		}()
	}
	for version := uint64(1); version <= 2_000; version++ {
		gates.Apply(Snapshot{Version: version, Values: map[string]bool{HandoffsEnabled: version%2 == 0, ShadowMode: version%3 == 0}})
	}
	wg.Wait()
	assert.Equal(t, uint64(2_000), gates.Snapshot().Version)
}

type scriptedStore struct {
	mu      sync.Mutex
	loads   []Snapshot
	err     error
	wakeups chan struct{}
}

func (s *scriptedStore) Load(context.Context) (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return Snapshot{}, s.err
	}
	if len(s.loads) == 0 {
		return Snapshot{}, errors.New("empty")
	}
	snapshot := s.loads[0]
	if len(s.loads) > 1 {
		s.loads = s.loads[1:]
	}
	return snapshot, nil
}
func (s *scriptedStore) Set(context.Context, string, bool, string, string) (Snapshot, error) {
	return Snapshot{}, errors.New("unused")
}
func (s *scriptedStore) Wakeups() <-chan struct{} { return s.wakeups }

func TestRunLoadsOnStartupWakeupAndPollWithoutSleeping(t *testing.T) {
	manual := clock.NewManual(time.Unix(100, 0))
	store := &scriptedStore{wakeups: make(chan struct{}, 1), loads: []Snapshot{
		{Version: 1, Values: map[string]bool{HandoffsEnabled: true, ShadowMode: false}},
		{Version: 2, Values: map[string]bool{HandoffsEnabled: false, ShadowMode: false}},
		{Version: 3, Values: map[string]bool{HandoffsEnabled: false, ShadowMode: true}},
	}}
	gates := NewWithClock(nil, manual)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- gates.Run(ctx, store, time.Second) }()
	require.Eventually(t, func() bool { return gates.Snapshot().Version == 1 }, time.Second, time.Millisecond)
	store.wakeups <- struct{}{}
	require.Eventually(t, func() bool { return gates.Snapshot().Version == 2 }, time.Second, time.Millisecond)
	manual.Advance(time.Second)
	require.Eventually(t, func() bool { return gates.Snapshot().Version == 3 }, time.Second, time.Millisecond)
	cancel()
	assert.ErrorIs(t, <-done, context.Canceled)
}
