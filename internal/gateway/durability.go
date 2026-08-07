package gateway

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/store/postgres"
)

var ErrTransientDurability = errors.New("transient durability failure")

type durabilityLane uint32

const (
	transcriptLane durabilityLane = 1 << iota
	handoffLane
)

type durabilityHealth struct{ unavailable atomic.Uint32 }

func (h *durabilityHealth) markUnavailable(lane durabilityLane) {
	if h == nil {
		return
	}
	for {
		old := h.unavailable.Load()
		if h.unavailable.CompareAndSwap(old, old|uint32(lane)) {
			return
		}
	}
}

func durabilityTicker(source clock.Clock, period time.Duration) (<-chan time.Time, func()) {
	if !isNil(source) {
		ticker := source.NewTicker(period)
		return ticker.C(), ticker.Stop
	}
	ticker := time.NewTicker(period)
	return ticker.C, ticker.Stop
}

func (h *durabilityHealth) markHealthy(lane durabilityLane) {
	if h == nil {
		return
	}
	for {
		old := h.unavailable.Load()
		if h.unavailable.CompareAndSwap(old, old&^uint32(lane)) {
			return
		}
	}
}

func (h *durabilityHealth) anyUnavailable() bool {
	return h != nil && h.unavailable.Load() != 0
}

func transientDurabilityError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	return errors.Is(err, ErrTransientDurability) || postgres.IsTransientDurabilityError(err)
}
