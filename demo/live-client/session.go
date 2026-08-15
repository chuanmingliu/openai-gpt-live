package main

import (
	"context"
	"sync"
	"sync/atomic"
)

type sessionGate struct {
	mu        sync.Mutex
	active    int
	limit     int
	next      uint64
	cancels   map[uint64]context.CancelFunc
	done      chan struct{}
	draining  atomic.Bool
	accepting atomic.Bool
}

func newSessionGate(limit int) *sessionGate {
	gate := &sessionGate{
		limit:   limit,
		cancels: make(map[uint64]context.CancelFunc),
		done:    make(chan struct{}),
	}
	gate.accepting.Store(true)
	return gate
}

func (g *sessionGate) Acquire(cancel context.CancelFunc) (uint64, bool) {
	if g == nil || !g.accepting.Load() || g.draining.Load() {
		return 0, false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.accepting.Load() || g.draining.Load() || g.active >= g.limit {
		return 0, false
	}
	g.active++
	g.next++
	g.cancels[g.next] = cancel
	return g.next, true
}

func (g *sessionGate) Release(id uint64) {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.cancels, id)
	if g.active > 0 {
		g.active--
	}
	if g.draining.Load() && g.active == 0 {
		select {
		case <-g.done:
		default:
			close(g.done)
		}
	}
}

func (g *sessionGate) BeginDrain() {
	if g == nil {
		return
	}
	g.mu.Lock()
	if g.draining.Swap(true) {
		g.mu.Unlock()
		return
	}
	g.accepting.Store(false)
	cancels := make([]context.CancelFunc, 0, len(g.cancels))
	for _, cancel := range g.cancels {
		if cancel != nil {
			cancels = append(cancels, cancel)
		}
	}
	if g.active == 0 {
		close(g.done)
	}
	g.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}

func (g *sessionGate) Wait(ctx context.Context) error {
	if g == nil {
		return nil
	}
	select {
	case <-g.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *sessionGate) Ready() bool {
	return g != nil && g.accepting.Load() && !g.draining.Load()
}

func (g *sessionGate) Active() int {
	if g == nil {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.active
}
