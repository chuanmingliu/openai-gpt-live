package admission

import (
	"sync"
	"time"
)

type tokenBucket struct {
	tokens   float64
	updated  time.Time
	lastSeen time.Time
}

type limiter struct {
	mu      sync.Mutex
	rate    float64
	burst   float64
	keyTTL  time.Duration
	maxKeys int
	now     func() time.Time
	global  tokenBucket
	keys    map[string]*tokenBucket
}

func newLimiter(rate float64, burst int, ipRate float64, ipBurst int, ttl time.Duration, maxKeys int, now func() time.Time) (*limiter, error) {
	if now == nil {
		now = time.Now
	}
	if !finitePositive(rate) || !finitePositive(ipRate) || burst <= 0 || ipBurst <= 0 || burst > 1_000_000 || ipBurst > 1_000_000 || ttl < time.Second || ttl > 24*time.Hour || maxKeys <= 0 || maxKeys > 1_000_000 {
		return nil, ErrInvalidConfig
	}
	t := now()
	return &limiter{rate: ipRate, burst: float64(ipBurst), keyTTL: ttl, maxKeys: maxKeys, now: now, global: tokenBucket{tokens: float64(burst), updated: t, lastSeen: t}, keys: make(map[string]*tokenBucket)}, nil
}

func finitePositive(value float64) bool { return value > 0 && value < 1e12 && value == value }

// allow consumes the global bucket and then the per-IP bucket. Global
// consumption is retained if the IP bucket denies, which is intentional.
func (l *limiter) allow(key string, globalRate float64, globalBurst int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	if !take(&l.global, globalRate, float64(globalBurst), now) {
		return false
	}
	bucket := l.keys[key]
	if bucket != nil && now.Sub(bucket.lastSeen) >= l.keyTTL {
		delete(l.keys, key)
		bucket = nil
	}
	if bucket == nil {
		if len(l.keys) >= l.maxKeys {
			for candidate, inactive := range l.keys {
				if now.Sub(inactive.lastSeen) >= l.keyTTL {
					delete(l.keys, candidate)
				}
			}
			if len(l.keys) >= l.maxKeys {
				return false
			}
		}
		bucket = &tokenBucket{tokens: l.burst, updated: now, lastSeen: now}
		l.keys[key] = bucket
	}
	return take(bucket, l.rate, l.burst, now)
}

func take(bucket *tokenBucket, rate, burst float64, now time.Time) bool {
	if now.Before(bucket.updated) {
		now = bucket.updated
	}
	elapsed := now.Sub(bucket.updated).Seconds()
	bucket.tokens += elapsed * rate
	if bucket.tokens > burst {
		bucket.tokens = burst
	}
	bucket.updated, bucket.lastSeen = now, now
	if bucket.tokens < 1 {
		return false
	}
	bucket.tokens--
	return true
}
