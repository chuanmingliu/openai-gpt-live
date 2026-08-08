package retention

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/lifecycle"
)

type Store interface {
	DeleteReadyConversations(context.Context, time.Time, int) (int, error)
}
type ApprovalStore interface {
	ExpireApprovals(context.Context, time.Time, int) (int, error)
}
type LeaseStore interface {
	SweepExpired(context.Context, time.Time, int) (int, error)
}

type Config struct {
	Store            Store
	Clock            clock.Clock
	Interval         time.Duration
	BatchSize        int
	Approvals        ApprovalStore
	Leases           LeaseStore
	Drainer          *lifecycle.Drainer
	Transient        func(error) bool
	RetryBase        time.Duration
	RetryMax         time.Duration
	MaxRetries       int
	FailureThreshold int
	OperationTimeout time.Duration
}

type Sweeper struct {
	config Config
	mu     sync.RWMutex
	fails  int
	last   error
}

func NewSweeper(config Config) (*Sweeper, error) {
	if nilValue(config.Clock) || nilValue(config.Leases) || nilValue(config.Approvals) || config.Drainer == nil ||
		config.Interval < time.Millisecond || config.Interval > 24*time.Hour || config.BatchSize < 1 || config.BatchSize > 1000 {
		return nil, errors.New("retention sweeper configuration is invalid")
	}
	if config.RetryBase == 0 {
		config.RetryBase = 100 * time.Millisecond
	}
	if config.RetryMax == 0 {
		config.RetryMax = 5 * time.Second
	}
	if config.MaxRetries == 0 {
		config.MaxRetries = 3
	}
	if config.FailureThreshold == 0 {
		config.FailureThreshold = 3
	}
	if config.OperationTimeout == 0 {
		config.OperationTimeout = 30 * time.Second
	}
	if config.RetryBase < time.Millisecond || config.RetryMax < config.RetryBase || config.RetryMax > time.Minute ||
		config.MaxRetries < 0 || config.MaxRetries > 20 || config.FailureThreshold < 1 || config.FailureThreshold > 100 ||
		config.OperationTimeout < time.Millisecond || config.OperationTimeout > 10*time.Minute {
		return nil, errors.New("retention sweeper retry configuration is invalid")
	}
	return &Sweeper{config: config}, nil
}

func (s *Sweeper) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("retention sweeper context is required")
	}
	if err := ctx.Err(); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
	if s == nil {
		return errors.New("retention sweeper is required")
	}
	ticker := s.config.Clock.NewTicker(s.config.Interval)
	defer ticker.Stop()
	for {
		err := s.sweepWithRetry(ctx)
		if errors.Is(err, context.Canceled) && (ctx.Err() != nil || s.config.Drainer.IsDraining()) {
			return nil
		}
		if err != nil && !s.transient(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-s.config.Drainer.Begun():
			return nil
		case <-ticker.C():
		}
	}
}

func (s *Sweeper) sweepWithRetry(ctx context.Context) error {
	var last error
	for attempt := 0; attempt <= s.config.MaxRetries; attempt++ {
		token, ok := s.config.Drainer.Acquire()
		if !ok {
			return context.Canceled
		}
		now := s.config.Clock.Now().UTC()
		last = s.attempt(ctx, func(callCtx context.Context) error {
			_, err := s.config.Leases.SweepExpired(callCtx, now, s.config.BatchSize)
			return err
		})
		if last == nil {
			last = s.attempt(ctx, func(callCtx context.Context) error {
				_, err := s.config.Approvals.ExpireApprovals(callCtx, now, s.config.BatchSize)
				return err
			})
		}
		if last == nil && !nilValue(s.config.Store) {
			last = s.attempt(ctx, func(callCtx context.Context) error {
				_, err := s.config.Store.DeleteReadyConversations(callCtx, now, s.config.BatchSize)
				return err
			})
		}
		token.Release()
		if last == nil {
			s.recordSuccess()
			return nil
		}
		if errors.Is(last, context.Canceled) && ctx.Err() != nil {
			return last
		}
		s.recordFailure(last)
		if !s.transient(last) || attempt == s.config.MaxRetries {
			return last
		}
		delay := s.config.RetryBase
		for i := 0; i < attempt && delay < s.config.RetryMax; i++ {
			if delay > s.config.RetryMax/2 {
				delay = s.config.RetryMax
				break
			}
			delay *= 2
		}
		wait := s.config.Clock.NewTicker(delay)
		select {
		case <-ctx.Done():
			wait.Stop()
			return ctx.Err()
		case <-s.config.Drainer.Begun():
			wait.Stop()
			return context.Canceled
		case <-wait.C():
			wait.Stop()
		}
	}
	return last
}

func (s *Sweeper) transient(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || s.config.Transient != nil && s.config.Transient(err)
}

func (s *Sweeper) attempt(parent context.Context, operation func(context.Context) error) error {
	callCtx, cancel := context.WithCancelCause(parent)
	timer := s.config.Clock.NewTicker(s.config.OperationTimeout)
	done := make(chan struct{})
	go func() {
		select {
		case <-done:
		case <-callCtx.Done():
		case <-s.config.Drainer.Begun():
			cancel(context.Canceled)
		case <-timer.C():
			cancel(context.DeadlineExceeded)
		}
	}()
	err := operation(callCtx)
	close(done)
	timer.Stop()
	cause := context.Cause(callCtx)
	cancel(context.Canceled)
	if cause != nil && (err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		return cause
	}
	return err
}

func (s *Sweeper) recordFailure(err error) {
	s.mu.Lock()
	s.fails++
	s.last = err
	s.mu.Unlock()
}

func (s *Sweeper) recordSuccess() {
	s.mu.Lock()
	s.fails = 0
	s.last = nil
	s.mu.Unlock()
}

func (s *Sweeper) Check(context.Context) error {
	if s == nil {
		return errors.New("retention sweeper is unavailable")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.fails >= s.config.FailureThreshold {
		return errors.New("retention sweeper is unhealthy")
	}
	return nil
}

func nilValue(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
