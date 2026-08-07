// Package lifecycle provides coordinated, race-safe graceful draining.
package lifecycle

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

var ErrDrainTimeout = errors.New("drain deadline exceeded")

// Drainer linearizes admission of tracked work with the transition to drain.
// Its zero value is not ready for use; construct it with NewDrainer.
type Drainer struct {
	mu       sync.Mutex
	draining atomic.Bool
	active   int64
	done     chan struct{}
	begun    chan struct{}
}

func NewDrainer() *Drainer { return &Drainer{done: make(chan struct{}), begun: make(chan struct{})} }

// Token represents one admitted unit of work. Release is idempotent.
type Token struct {
	d    *Drainer
	once sync.Once
}

// Acquire admits work only while the service is accepting new work.
func (d *Drainer) Acquire() (*Token, bool) {
	if d == nil {
		return nil, false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.draining.Load() {
		return nil, false
	}
	d.active++
	return &Token{d: d}, true
}

func (t *Token) Release() {
	if t == nil || t.d == nil {
		return
	}
	t.once.Do(func() { t.d.release() })
}

func (d *Drainer) release() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.active == 0 {
		return
	}
	d.active--
	if d.draining.Load() && d.active == 0 {
		select {
		case <-d.done:
		default:
			close(d.done)
		}
	}
}

// Begin atomically rejects future work before returning. It returns true only
// for the caller that performed the transition.
func (d *Drainer) Begin() bool {
	if d == nil {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	first := !d.draining.Load()
	d.draining.Store(true)
	if first {
		close(d.begun)
	}
	if d.active == 0 {
		select {
		case <-d.done:
		default:
			close(d.done)
		}
	}
	return first
}

// Begun is closed exactly once when draining starts.
func (d *Drainer) Begun() <-chan struct{} {
	if d == nil {
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	return d.begun
}

// Done is closed after Begin once every admitted token has been released.
func (d *Drainer) Done() <-chan struct{} {
	if d == nil {
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	return d.done
}

func (d *Drainer) Accepting() bool  { return d != nil && !d.draining.Load() }
func (d *Drainer) IsDraining() bool { return d == nil || d.draining.Load() }

func (d *Drainer) Active() int64 {
	if d == nil {
		return 0
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.active
}

// Wait waits for all work admitted before Begin. It begins draining if needed.
func (d *Drainer) Wait(ctx context.Context) error {
	if d == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("drain context is required")
	}
	d.Begin()
	select {
	case <-d.done:
		return nil
	case <-ctx.Done():
		return errors.Join(ErrDrainTimeout, ctx.Err())
	}
}

// Drain waits until the deadline, cancels remaining I/O on timeout, then waits
// through the smaller cleanup bound. All callers share the same drain state.
func (d *Drainer) Drain(ctx context.Context, cancelRemaining context.CancelFunc, cleanup context.Context) error {
	err := d.Wait(ctx)
	if err == nil {
		return nil
	}
	if cancelRemaining != nil {
		cancelRemaining()
	}
	if cleanup == nil {
		return err
	}
	select {
	case <-d.done:
	case <-cleanup.Done():
	}
	return err
}
