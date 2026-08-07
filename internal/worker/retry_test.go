package worker

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fixedJitter time.Duration

func (j fixedJitter) Duration(time.Duration) time.Duration { return time.Duration(j) }

func TestRetryPolicyFullJitterClampsHostileSources(t *testing.T) {
	now := time.Date(2026, 8, 7, 0, 0, 0, 0, time.UTC)
	policy := RetryPolicy{Base: time.Second, Maximum: 8 * time.Second, MaxRetries: 100}
	for _, tc := range []struct {
		name       string
		retryCount uint32
		jitter     time.Duration
		want       time.Duration
	}{
		{"negative", 0, -time.Second, 0},
		{"inside", 2, 3 * time.Second, 3 * time.Second},
		{"oversized", 2, 10 * time.Second, 4 * time.Second},
		{"exponent overflow saturates", 99, time.Duration(math.MaxInt64), 8 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ready, ok, err := policy.Next(now, now.Add(time.Hour), tc.retryCount, 0, fixedJitter(tc.jitter))
			require.NoError(t, err)
			require.True(t, ok)
			assert.Equal(t, now.Add(tc.want), ready)
		})
	}
}

func TestRetryPolicyLongerRetryAfterWinsAndOverflowIsRejected(t *testing.T) {
	now := time.Unix(1, 0).UTC()
	policy := RetryPolicy{Base: time.Second, Maximum: 8 * time.Second, MaxRetries: 4}
	ready, ok, err := policy.Next(now, now.Add(time.Minute), 0, 9*time.Second, fixedJitter(time.Second))
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, now.Add(9*time.Second), ready)

	_, ok, err = policy.Next(time.Unix(0, math.MaxInt64-1), time.Unix(0, math.MaxInt64), 0, time.Second, fixedJitter(0))
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestRetryPolicyRefusesLimitAndExactDeadline(t *testing.T) {
	now := time.Date(2026, 8, 7, 0, 0, 0, 0, time.UTC)
	policy := RetryPolicy{Base: time.Second, Maximum: 8 * time.Second, MaxRetries: 2}
	_, ok, err := policy.Next(now, now.Add(time.Hour), 2, 0, fixedJitter(0))
	require.NoError(t, err)
	assert.False(t, ok)

	_, ok, err = policy.Next(now, now.Add(time.Second), 0, time.Second, fixedJitter(0))
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestRetryPolicyValidation(t *testing.T) {
	valid := RetryPolicy{Base: time.Second, Maximum: time.Minute, MaxRetries: 3}
	require.NoError(t, valid.Validate())
	for _, policy := range []RetryPolicy{
		{},
		{Base: -1, Maximum: time.Second, MaxRetries: 1},
		{Base: time.Second, Maximum: time.Second - 1, MaxRetries: 1},
		{Base: time.Second, Maximum: time.Second, MaxRetries: 0},
	} {
		assert.Error(t, policy.Validate())
	}
}
