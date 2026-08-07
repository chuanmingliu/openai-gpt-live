// Package health implements bounded liveness and readiness reporting.
package health

import (
	"context"
	"errors"
	"regexp"
	"sync"
	"time"
)

const (
	defaultCheckTimeout     = 500 * time.Millisecond
	defaultAggregateTimeout = time.Second
	defaultCacheTTL         = time.Second
	maxChecks               = 128
)

var checkName = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,63}$`)

type Checker interface{ Check(context.Context) error }
type CheckFunc func(context.Context) error

func (f CheckFunc) Check(ctx context.Context) error { return f(ctx) }

type Drainer interface{ Accepting() bool }

type Options struct {
	CheckTimeout     time.Duration
	AggregateTimeout time.Duration
	CacheTTL         time.Duration
	Now              func() time.Time
}

type Registry struct {
	mu      sync.RWMutex
	checks  map[string]*entry
	started bool
	drainer Drainer
	opts    Options
}

type entry struct {
	required bool
	checker  Checker
	gate     chan struct{}
	mu       sync.RWMutex
	result   result
}

type result struct {
	healthy bool
	checked time.Time
}

func NewRegistry(options ...Options) *Registry {
	opts := Options{CheckTimeout: defaultCheckTimeout, AggregateTimeout: defaultAggregateTimeout, CacheTTL: defaultCacheTTL, Now: time.Now}
	if len(options) > 0 {
		candidate := options[0]
		if candidate.CheckTimeout > 0 {
			opts.CheckTimeout = candidate.CheckTimeout
		}
		if candidate.AggregateTimeout > 0 {
			opts.AggregateTimeout = candidate.AggregateTimeout
		}
		if candidate.CacheTTL >= 0 {
			opts.CacheTTL = candidate.CacheTTL
		}
		if candidate.Now != nil {
			opts.Now = candidate.Now
		}
	}
	return &Registry{checks: make(map[string]*entry), opts: opts}
}

func (r *Registry) Register(name string, required bool, checker Checker) error {
	if r == nil || !checkName.MatchString(name) || checker == nil {
		return errors.New("health check registration is invalid")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return errors.New("health registry is already started")
	}
	if len(r.checks) >= maxChecks {
		return errors.New("health check registry is full")
	}
	if _, exists := r.checks[name]; exists {
		return errors.New("health check is already registered")
	}
	r.checks[name] = &entry{required: required, checker: checker, gate: make(chan struct{}, 1)}
	return nil
}

func (r *Registry) RegisterRequired(name string, checker Checker) error {
	return r.Register(name, true, checker)
}

func (r *Registry) RegisterDrainer(d Drainer) error {
	if r == nil || d == nil {
		return errors.New("health drainer is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return errors.New("health registry is already started")
	}
	if r.drainer != nil {
		return errors.New("health drainer is already registered")
	}
	r.drainer = d
	return nil
}

func (r *Registry) Start() {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.started = true
	r.mu.Unlock()
}

func (r *Registry) Ready(ctx context.Context) bool {
	if r == nil || ctx == nil {
		return false
	}
	r.Start()
	r.mu.RLock()
	d := r.drainer
	entries := make([]*entry, 0, len(r.checks))
	for _, e := range r.checks {
		if e.required {
			entries = append(entries, e)
		}
	}
	opts := r.opts
	r.mu.RUnlock()
	if d != nil && !d.Accepting() {
		return false
	}
	aggregate, cancel := context.WithTimeout(ctx, opts.AggregateTimeout)
	defer cancel()
	out := make(chan bool, len(entries))
	for _, e := range entries {
		go func(e *entry) { out <- e.evaluate(aggregate, opts) }(e)
	}
	for range entries {
		select {
		case healthy := <-out:
			if !healthy {
				return false
			}
		case <-aggregate.Done():
			return false
		}
	}
	return d == nil || d.Accepting()
}

func (e *entry) evaluate(parent context.Context, opts Options) bool {
	now := opts.Now()
	e.mu.RLock()
	cached := e.result
	e.mu.RUnlock()
	if !cached.checked.IsZero() && now.Sub(cached.checked) <= opts.CacheTTL {
		return cached.healthy
	}
	select {
	case e.gate <- struct{}{}:
		// At most one goroutine can be inside a checker. A permanently stuck
		// checker therefore consumes one bounded goroutine, not one per request.
	case <-parent.Done():
		return false
	default:
		return false
	}
	done := make(chan bool, 1)
	checkCtx, cancel := context.WithTimeout(parent, opts.CheckTimeout)
	go func() {
		healthy := false
		func() {
			defer func() { _ = recover() }()
			healthy = e.checker.Check(checkCtx) == nil
		}()
		done <- healthy
		<-e.gate
	}()
	defer cancel()
	select {
	case healthy := <-done:
		e.mu.Lock()
		e.result = result{healthy: healthy, checked: opts.Now()}
		e.mu.Unlock()
		return healthy
	case <-checkCtx.Done():
		e.mu.Lock()
		e.result = result{healthy: false, checked: opts.Now()}
		e.mu.Unlock()
		return false
	}
}
