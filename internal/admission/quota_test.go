package admission

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLimiterCapsKeysFailsClosedAndEvictsOnlyInactiveKeys(t *testing.T) {
	now := time.Now()
	limiter, err := newLimiter(100, 100, 100, 2, time.Second, 1, func() time.Time { return now })
	require.NoError(t, err)
	assert.True(t, limiter.allow("192.0.2.1", 100, 100))
	assert.False(t, limiter.allow("192.0.2.2", 100, 100), "active key cap must fail closed")
	now = now.Add(2 * time.Second)
	assert.True(t, limiter.allow("192.0.2.2", 100, 100), "inactive keys must be evicted")
}

func TestLimiterKeyCapRemainsBoundedUnderConcurrency(t *testing.T) {
	limiter, err := newLimiter(1e6, 1000, 1e6, 10, time.Minute, 8, time.Now)
	require.NoError(t, err)
	var group sync.WaitGroup
	for i := 0; i < 128; i++ {
		group.Add(1)
		go func(i int) { defer group.Done(); _ = limiter.allow(fmt.Sprintf("192.0.2.%d", i), 1e6, 1000) }(i)
	}
	group.Wait()
	limiter.mu.Lock()
	keys := len(limiter.keys)
	limiter.mu.Unlock()
	assert.LessOrEqual(t, keys, 8)
}
