package clock

import (
	"sync"
	"time"
)

// Manual advances only when Advance is called. Ticks are delivered before
// Advance returns, so tests need not coordinate through wall-clock sleeps.
type Manual struct {
	mu      sync.Mutex
	now     time.Time
	tickers map[*manualTicker]struct{}
}

func NewManual(start time.Time) *Manual {
	return &Manual{now: start, tickers: make(map[*manualTicker]struct{})}
}

func (c *Manual) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *Manual) NewTicker(period time.Duration) Ticker {
	if period <= 0 {
		panic("clock: non-positive ticker period")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &manualTicker{clock: c, period: period, next: c.now.Add(period), ch: make(chan time.Time, 1)}
	c.tickers[t] = struct{}{}
	return t
}

func (c *Manual) Advance(elapsed time.Duration) {
	if elapsed < 0 {
		panic("clock: cannot advance backwards")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(elapsed)
	for t := range c.tickers {
		for !t.stopped && !t.next.After(c.now) {
			select {
			case t.ch <- t.next:
			default:
			}
			t.next = t.next.Add(t.period)
		}
	}
}

type manualTicker struct {
	clock   *Manual
	period  time.Duration
	next    time.Time
	ch      chan time.Time
	stopped bool
}

func (t *manualTicker) C() <-chan time.Time { return t.ch }

func (t *manualTicker) Stop() {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	if t.stopped {
		return
	}
	t.stopped = true
	delete(t.clock.tickers, t)
}
