// Package clock provides real and deterministic clocks for components whose
// retries, leases, and retention behavior must be testable without sleeps.
package clock

import "time"

type Clock interface {
	Now() time.Time
	NewTicker(time.Duration) Ticker
}

type Ticker interface {
	C() <-chan time.Time
	Stop()
}

type JitterSource interface {
	Duration(max time.Duration) time.Duration
}

// Real is the wall-clock implementation used by production composition roots.
type Real struct{}

func (Real) Now() time.Time { return time.Now() }

func (Real) NewTicker(period time.Duration) Ticker {
	return realTicker{ticker: time.NewTicker(period)}
}

type realTicker struct{ ticker *time.Ticker }

func (t realTicker) C() <-chan time.Time { return t.ticker.C }
func (t realTicker) Stop()               { t.ticker.Stop() }
