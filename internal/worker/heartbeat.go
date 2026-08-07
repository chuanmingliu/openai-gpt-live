package worker

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	errDurableCancel = errors.New("worker durable cancellation requested")
	errSteered       = errors.New("worker objective or input revised")
	errDeadline      = errors.New("worker handoff deadline reached")
	errHeartbeat     = errors.New("worker heartbeat failed")
)

type heartbeatEvent struct {
	cause   error
	control Control
}

type generationCanceller struct {
	mu     sync.Mutex
	cancel context.CancelCauseFunc
}

func (c *generationCanceller) install(cancel context.CancelCauseFunc) {
	c.mu.Lock()
	c.cancel = cancel
	c.mu.Unlock()
}

func (c *generationCanceller) clear(cancel context.CancelCauseFunc) {
	c.mu.Lock()
	// CancelCauseFunc values are not comparable. Clearing is safe because one
	// generation is executed synchronously and installs its successor only
	// after the prior Stream call returns.
	c.cancel = nil
	c.mu.Unlock()
}

func (c *generationCanceller) stop(cause error) {
	c.mu.Lock()
	cancel := c.cancel
	c.mu.Unlock()
	if cancel != nil {
		cancel(cause)
	}
}

func (r *Runner) heartbeatLoop(ctx context.Context, lease Lease, initial Control, canceller *generationCanceller, events chan heartbeatEvent) {
	period := r.LeaseDuration / 3
	ticker := r.Clock.NewTicker(period)
	defer ticker.Stop()
	deadlineDelay := lease.Job.Request.Deadline.Sub(r.Clock.Now())
	if deadlineDelay <= 0 {
		r.publishHeartbeatEvent(ctx, canceller, events, heartbeatEvent{cause: errDeadline})
		return
	}
	deadlineTicker := r.Clock.NewTicker(deadlineDelay)
	defer deadlineTicker.Stop()
	last := initial
	for {
		select {
		case <-ctx.Done():
			return
		case <-deadlineTicker.C():
			now := r.Clock.Now().UTC()
			expires, ok := stableExpiry(now, r.LeaseDuration)
			if !ok {
				r.publishHeartbeatEvent(ctx, canceller, events, heartbeatEvent{cause: errHeartbeat})
				return
			}
			control, err := r.Store.Heartbeat(ctx, lease.Token, expires)
			if errors.Is(err, ErrLeaseLost) {
				r.publishHeartbeatEvent(ctx, canceller, events, heartbeatEvent{cause: ErrLeaseLost})
				return
			}
			if err != nil {
				r.publishHeartbeatEvent(ctx, canceller, events, heartbeatEvent{cause: errHeartbeat})
				return
			}
			if control.CancelRequested {
				r.publishHeartbeatEvent(ctx, canceller, events, heartbeatEvent{cause: errDurableCancel, control: control})
				return
			}
			r.publishHeartbeatEvent(ctx, canceller, events, heartbeatEvent{cause: errDeadline})
			return
		case <-ticker.C():
			now := r.Clock.Now().UTC()
			expires := now.Add(r.LeaseDuration)
			if !expires.After(now) {
				r.publishHeartbeatEvent(ctx, canceller, events, heartbeatEvent{cause: errHeartbeat})
				return
			}
			control, err := r.Store.Heartbeat(ctx, lease.Token, expires)
			if err != nil {
				cause := errHeartbeat
				if errors.Is(err, ErrLeaseLost) {
					cause = ErrLeaseLost
				}
				r.publishHeartbeatEvent(ctx, canceller, events, heartbeatEvent{cause: cause})
				return
			}
			if control.CancelRequested {
				r.publishHeartbeatEvent(ctx, canceller, events, heartbeatEvent{cause: errDurableCancel, control: control})
				return
			}
			if !lease.Job.Request.Deadline.After(now) {
				r.publishHeartbeatEvent(ctx, canceller, events, heartbeatEvent{cause: errDeadline})
				return
			}
			if control.ObjectiveRevision != last.ObjectiveRevision || control.InputRevision != last.InputRevision || control.ConsumedInput != last.ConsumedInput {
				last = cloneControl(control)
				r.publishHeartbeatEvent(ctx, canceller, events, heartbeatEvent{cause: errSteered, control: last})
			}
		}
	}
}

func (r *Runner) publishHeartbeatEvent(ctx context.Context, canceller *generationCanceller, events chan heartbeatEvent, event heartbeatEvent) {
	select {
	case events <- event:
	default:
		select {
		case <-events:
		default:
		}
		select {
		case events <- event:
		default:
		}
	}
	canceller.stop(event.cause)
}

func cloneControl(control Control) Control {
	control.Input = append([]byte(nil), control.Input...)
	return control
}

func consumeHeartbeatEvent(events <-chan heartbeatEvent, cause error) heartbeatEvent {
	chosen := heartbeatEvent{cause: cause}
	priority := heartbeatPriority(cause)
	for {
		select {
		case event := <-events:
			if next := heartbeatPriority(event.cause); next >= priority {
				chosen, priority = event, next
			}
		default:
			return chosen
		}
	}
}

func heartbeatPriority(cause error) int {
	switch {
	case errors.Is(cause, ErrLeaseLost):
		return 5
	case errors.Is(cause, errDurableCancel):
		return 4
	case errors.Is(cause, errDeadline):
		return 3
	case errors.Is(cause, errHeartbeat):
		return 2
	case errors.Is(cause, errSteered):
		return 1
	default:
		return 0
	}
}

func stableExpiry(now time.Time, duration time.Duration) (time.Time, bool) {
	expires := now.Add(duration)
	return expires, expires.After(now)
}
