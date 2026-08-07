package worker

import (
	"errors"
	"math"
	"reflect"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
)

// RetryPolicy bounds provider retries. retryCount is the number of provider
// retries already scheduled for the durable handoff.
type RetryPolicy struct {
	Base       time.Duration
	Maximum    time.Duration
	MaxRetries uint32
}

func (p RetryPolicy) Validate() error {
	if p.Base <= 0 || p.Maximum < p.Base || p.MaxRetries == 0 {
		return errors.New("worker retry policy is invalid")
	}
	return nil
}

// Next returns a strictly pre-deadline readiness time. A false result means
// the durable retry budget or deadline does not permit another attempt.
func (p RetryPolicy) Next(now, deadline time.Time, retryCount uint32, retryAfter time.Duration, jitter clock.JitterSource) (time.Time, bool, error) {
	if err := p.Validate(); err != nil {
		return time.Time{}, false, err
	}
	if nilValue(jitter) {
		return time.Time{}, false, errors.New("worker retry jitter source is required")
	}
	if now.IsZero() || deadline.IsZero() || !deadline.After(now) {
		return time.Time{}, false, nil
	}
	if retryCount >= p.MaxRetries {
		return time.Time{}, false, nil
	}
	bound := exponentialDuration(p.Base, retryCount)
	if bound > p.Maximum {
		bound = p.Maximum
	}
	delay := jitter.Duration(bound)
	if delay < 0 {
		delay = 0
	} else if delay > bound {
		delay = bound
	}
	if retryAfter > delay {
		delay = retryAfter
	}
	ready := now.Add(delay)
	if ready.Before(now) || !ready.Before(deadline) {
		return time.Time{}, false, nil
	}
	return ready.UTC(), true, nil
}

func exponentialDuration(base time.Duration, exponent uint32) time.Duration {
	if exponent >= 63 || base > time.Duration(math.MaxInt64)>>exponent {
		return time.Duration(math.MaxInt64)
	}
	return base << exponent
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
