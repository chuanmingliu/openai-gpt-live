package retention

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/lifecycle"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type sweepStore struct {
	mu                        sync.Mutex
	leaseCalls, approvalCalls int
	deleteCalls               int
	now                       time.Time
	batch                     int
	errs                      []error
	started, release          chan struct{}
	cancelled                 chan struct{}
	blockStage                string
}

func (s *sweepStore) SweepExpired(ctx context.Context, now time.Time, batch int) (int, error) {
	s.mu.Lock()
	s.leaseCalls++
	s.now, s.batch = now, batch
	var err error
	if len(s.errs) > 0 {
		err, s.errs = s.errs[0], s.errs[1:]
	}
	s.mu.Unlock()
	if waitErr := s.wait(ctx, "lease"); waitErr != nil {
		return 0, waitErr
	}
	return 0, err
}

func (s *sweepStore) wait(ctx context.Context, stage string) error {
	s.mu.Lock()
	active := s.release != nil && (s.blockStage == "" && stage == "lease" || s.blockStage == stage)
	started, release, cancelled := s.started, s.release, s.cancelled
	s.mu.Unlock()
	if !active {
		return nil
	}
	if started != nil {
		select {
		case <-started:
		default:
			close(started)
		}
	}
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			if cancelled != nil {
				select {
				case <-cancelled:
				default:
					close(cancelled)
				}
			}
			return ctx.Err()
		}
	}
	return nil
}

func (s *sweepStore) ExpireApprovals(ctx context.Context, _ time.Time, _ int) (int, error) {
	s.mu.Lock()
	s.approvalCalls++
	s.mu.Unlock()
	return 0, s.wait(ctx, "approval")
}

func (s *sweepStore) DeleteReadyConversations(ctx context.Context, _ time.Time, _ int) (int, error) {
	s.mu.Lock()
	s.deleteCalls++
	s.mu.Unlock()
	return 0, s.wait(ctx, "delete")
}

func sweeperConfig(store *sweepStore, manual *clock.Manual, drainer *lifecycle.Drainer) Config {
	return Config{Store: store, Leases: store, Approvals: store, Clock: manual, Drainer: drainer,
		Interval: time.Second, BatchSize: 17, RetryBase: 10 * time.Millisecond, RetryMax: time.Second,
		MaxRetries: 2, FailureThreshold: 3, OperationTimeout: 50 * time.Millisecond}
}

func TestSweeperUsesInjectedClockAndBoundedBatch(t *testing.T) {
	now := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)
	manual := clock.NewManual(now)
	store := &sweepStore{}
	sweeper, err := NewSweeper(sweeperConfig(store, manual, lifecycle.NewDrainer()))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- sweeper.Run(ctx) }()
	require.Eventually(t, func() bool { store.mu.Lock(); defer store.mu.Unlock(); return store.deleteCalls == 1 }, time.Second, time.Millisecond)
	store.mu.Lock()
	assert.Equal(t, now, store.now)
	assert.Equal(t, 17, store.batch)
	assert.Equal(t, 1, store.leaseCalls)
	assert.Equal(t, 1, store.approvalCalls)
	store.mu.Unlock()
	cancel()
	assert.NoError(t, <-done)
}

func TestSweeperConfigurationAndNonTransientFailure(t *testing.T) {
	_, err := NewSweeper(Config{})
	assert.Error(t, err)
	manual := clock.NewManual(time.Now())
	fault := errors.New("invalid sweep")
	store := &sweepStore{errs: []error{fault}}
	config := sweeperConfig(store, manual, lifecycle.NewDrainer())
	config.Transient = func(error) bool { return false }
	sweeper, err := NewSweeper(config)
	require.NoError(t, err)
	assert.ErrorIs(t, sweeper.Run(t.Context()), fault)
}

func TestSweeperRetriesTransientFailureAndRecoversHealth(t *testing.T) {
	manual := clock.NewManual(time.Now().UTC())
	fault := errors.New("transient")
	store := &sweepStore{errs: []error{fault}}
	config := sweeperConfig(store, manual, lifecycle.NewDrainer())
	config.Transient = func(err error) bool { return errors.Is(err, fault) }
	config.FailureThreshold = 1
	sweeper, err := NewSweeper(config)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- sweeper.Run(ctx) }()
	require.Eventually(t, func() bool { store.mu.Lock(); defer store.mu.Unlock(); return store.leaseCalls == 1 }, time.Second, time.Millisecond)
	require.Eventually(t, func() bool { return sweeper.Check(t.Context()) != nil }, time.Second, time.Millisecond)
	manual.Advance(10 * time.Millisecond)
	require.Eventually(t, func() bool { store.mu.Lock(); defer store.mu.Unlock(); return store.deleteCalls == 1 }, time.Second, time.Millisecond)
	assert.NoError(t, sweeper.Check(t.Context()))
	cancel()
	assert.NoError(t, <-done)
}

func TestSweeperDrainCancelsStuckAttemptReleasesTokenAndRejectsNewClaims(t *testing.T) {
	manual := clock.NewManual(time.Now().UTC())
	drainer := lifecycle.NewDrainer()
	store := &sweepStore{started: make(chan struct{}), release: make(chan struct{}), cancelled: make(chan struct{})}
	sweeper, err := NewSweeper(sweeperConfig(store, manual, drainer))
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- sweeper.Run(t.Context()) }()
	<-store.started
	drainer.Begin()
	select {
	case <-store.cancelled:
	case <-time.After(time.Second):
		t.Fatal("drain did not cancel stuck store attempt")
	}
	select {
	case <-drainer.Done():
	case <-time.After(time.Second):
		t.Fatal("drain did not release admitted sweeper token")
	}
	require.NoError(t, <-done)
	store.mu.Lock()
	assert.Equal(t, 1, store.leaseCalls)
	store.mu.Unlock()
}

func TestSweeperEachStoreAttemptHasInjectedClockDeadlineAndRecoversReadiness(t *testing.T) {
	for _, stage := range []string{"lease", "approval", "delete"} {
		t.Run(stage, func(t *testing.T) {
			manual := clock.NewManual(time.Now().UTC())
			store := &sweepStore{blockStage: stage, started: make(chan struct{}), release: make(chan struct{}), cancelled: make(chan struct{})}
			config := sweeperConfig(store, manual, lifecycle.NewDrainer())
			config.FailureThreshold = 1
			config.MaxRetries = 1
			config.OperationTimeout = 20 * time.Millisecond
			sweeper, err := NewSweeper(config)
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() { done <- sweeper.Run(ctx) }()
			<-store.started
			manual.Advance(config.OperationTimeout)
			select {
			case <-store.cancelled:
			case <-time.After(time.Second):
				t.Fatal("operation deadline did not cancel blocked store")
			}
			require.Eventually(t, func() bool { return sweeper.Check(t.Context()) != nil }, time.Second, time.Millisecond)
			store.mu.Lock()
			store.release = nil
			store.mu.Unlock()
			manual.Advance(config.RetryBase)
			require.Eventually(t, func() bool { return sweeper.Check(t.Context()) == nil }, time.Second, time.Millisecond)
			select {
			case err := <-done:
				t.Fatalf("deadline failure stopped liveness: %v", err)
			default:
			}
			cancel()
			require.NoError(t, <-done)
		})
	}
}

func TestSweeperSustainedTransientFailureMarksUnhealthyWithoutStoppingProcess(t *testing.T) {
	manual := clock.NewManual(time.Now().UTC())
	fault := errors.New("transient")
	store := &sweepStore{errs: []error{fault, fault, fault}}
	config := sweeperConfig(store, manual, lifecycle.NewDrainer())
	config.Transient = func(error) bool { return true }
	config.MaxRetries = 1
	config.FailureThreshold = 2
	sweeper, err := NewSweeper(config)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- sweeper.Run(ctx) }()
	require.Eventually(t, func() bool { store.mu.Lock(); defer store.mu.Unlock(); return store.leaseCalls == 1 }, time.Second, time.Millisecond)
	manual.Advance(10 * time.Millisecond)
	require.Eventually(t, func() bool { store.mu.Lock(); defer store.mu.Unlock(); return store.leaseCalls == 2 }, time.Second, time.Millisecond)
	assert.Error(t, sweeper.Check(t.Context()))
	select {
	case err := <-done:
		t.Fatalf("transient failure stopped sweeper: %v", err)
	default:
	}
	cancel()
	assert.NoError(t, <-done)
}
