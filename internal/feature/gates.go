// Package feature implements PostgreSQL-authoritative runtime feature gates.
package feature

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
)

const (
	HandoffsEnabled = "handoffs_enabled"
	ShadowMode      = "shadow_mode"
)

type Snapshot struct {
	Version uint64
	Values  map[string]bool
}

// Valid reports whether the snapshot is a complete authoritative view.
func (s Snapshot) Valid() bool {
	return s.Version > 0 && validAuthorityValues(s.Values)
}

// Equal reports exact authority equality, including version and both values.
func (s Snapshot) Equal(other Snapshot) bool {
	return s.Version == other.Version &&
		s.Values[HandoffsEnabled] == other.Values[HandoffsEnabled] &&
		s.Values[ShadowMode] == other.Values[ShadowMode] &&
		validAuthorityValues(s.Values) && validAuthorityValues(other.Values)
}

type Store interface {
	Load(context.Context) (Snapshot, error)
	Set(context.Context, string, bool, string, string) (Snapshot, error)
}

type wakeupStore interface{ Wakeups() <-chan struct{} }
type notificationRunner interface{ RunNotifications(context.Context) error }

const defaultNotificationShutdownTimeout = 5 * time.Second

type gateState struct {
	version uint64
	values  map[string]bool
}

// Gates is an immutable-snapshot runtime view. Readers perform one atomic load.
type Gates struct {
	state    atomic.Pointer[gateState]
	healthy  atomic.Bool
	failures atomic.Uint64
	clock    clock.Clock
}

func New(initial map[string]bool) *Gates { return NewWithClock(initial, clock.Real{}) }

func NewWithClock(initial map[string]bool, source clock.Clock) *Gates {
	if source == nil {
		source = clock.Real{}
	}
	g := &Gates{clock: source}
	values := map[string]bool{HandoffsEnabled: false, ShadowMode: false}
	if validAuthorityValues(initial) {
		values = cloneValues(initial)
	}
	g.state.Store(&gateState{values: values})
	return g
}

func (g *Gates) Enabled(name string) bool {
	if g == nil || !known(name) {
		return false
	}
	state := g.state.Load()
	return state != nil && state.values[name]
}

func (g *Gates) Snapshot() Snapshot {
	if g == nil {
		return Snapshot{Values: map[string]bool{HandoffsEnabled: false, ShadowMode: false}}
	}
	state := g.state.Load()
	if state == nil {
		return Snapshot{Values: map[string]bool{HandoffsEnabled: false, ShadowMode: false}}
	}
	return Snapshot{Version: state.version, Values: cloneValues(state.values)}
}

// Apply publishes only a complete, valid and strictly newer authority view.
func (g *Gates) Apply(snapshot Snapshot) bool {
	if g == nil || !snapshot.Valid() {
		return false
	}
	for {
		current := g.state.Load()
		if current != nil && snapshot.Version <= current.version {
			return false
		}
		next := &gateState{version: snapshot.Version, values: cloneValues(snapshot.Values)}
		if g.state.CompareAndSwap(current, next) {
			g.healthy.Store(true)
			return true
		}
	}
}

func (g *Gates) RecordRefreshFailure() {
	if g == nil {
		return
	}
	g.failures.Add(1)
	g.healthy.Store(false)
}

func (g *Gates) Healthy() bool { return g != nil && g.healthy.Load() }
func (g *Gates) RefreshFailures() uint64 {
	if g == nil {
		return 0
	}
	return g.failures.Load()
}

// Run reloads the authoritative snapshot on startup, every poll, and every
// notification hint. Store errors retain the last known safe state.
func (g *Gates) Run(ctx context.Context, store Store, interval time.Duration, shutdownTimeout ...time.Duration) error {
	if g == nil || ctx == nil || store == nil {
		return errors.New("feature gate runtime dependencies are required")
	}
	if interval < time.Millisecond || interval > time.Minute {
		return errors.New("feature gate refresh interval is out of bounds")
	}
	joinTimeout := defaultNotificationShutdownTimeout
	if len(shutdownTimeout) > 1 || (len(shutdownTimeout) == 1 && (shutdownTimeout[0] <= 0 || shutdownTimeout[0] > time.Minute)) {
		return errors.New("feature gate shutdown timeout is out of bounds")
	}
	if len(shutdownTimeout) == 1 {
		joinTimeout = shutdownTimeout[0]
	}
	refresh := func() {
		snapshot, err := store.Load(ctx)
		current := g.Snapshot().Version
		if err != nil || snapshot.Version == 0 || !validAuthorityValues(snapshot.Values) || snapshot.Version < current {
			g.RecordRefreshFailure()
			return
		}
		if snapshot.Version > current && !g.Apply(snapshot) {
			g.RecordRefreshFailure()
			return
		}
		g.healthy.Store(true)
	}
	refresh()
	listenerCtx, cancelListener := context.WithCancel(ctx)
	defer cancelListener()
	var listenerDone <-chan error
	if listener, ok := store.(notificationRunner); ok {
		done := make(chan error, 1)
		listenerDone = done
		go func() { done <- listener.RunNotifications(listenerCtx) }()
	}
	joinListener := func() error {
		if listenerDone == nil {
			return nil
		}
		cancelListener()
		joinCtx, cancel := context.WithTimeout(context.Background(), joinTimeout)
		defer cancel()
		select {
		case <-listenerDone:
			return nil
		case <-joinCtx.Done():
			return errors.New("feature notification listener shutdown timed out")
		}
	}
	ticker := g.clock.NewTicker(interval)
	defer ticker.Stop()
	var wakeups <-chan struct{}
	if notifying, ok := store.(wakeupStore); ok {
		wakeups = notifying.Wakeups()
	}
	for {
		select {
		case <-ctx.Done():
			if err := joinListener(); err != nil {
				return err
			}
			return ctx.Err()
		case err := <-listenerDone:
			if ctx.Err() != nil {
				return ctx.Err()
			}
			g.RecordRefreshFailure()
			return err
		case <-ticker.C():
			refresh()
		case _, ok := <-wakeups:
			if !ok {
				wakeups = nil
				continue
			}
			refresh()
		}
	}
}

func known(name string) bool { return name == HandoffsEnabled || name == ShadowMode }

func validAuthorityValues(values map[string]bool) bool {
	if len(values) != 2 {
		return false
	}
	_, handoffs := values[HandoffsEnabled]
	_, shadow := values[ShadowMode]
	return handoffs && shadow
}

func cloneValues(values map[string]bool) map[string]bool {
	return map[string]bool{HandoffsEnabled: values[HandoffsEnabled], ShadowMode: values[ShadowMode]}
}
