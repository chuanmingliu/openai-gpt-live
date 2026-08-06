package clock

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestManualClockTicker catches a manual-clock regression that makes ticker
// delivery depend on wall-clock sleeps instead of the test-controlled advance.
func TestManualClockTicker(t *testing.T) {
	start := time.Date(2026, time.August, 7, 12, 0, 0, 0, time.UTC)
	clock := NewManual(start)
	ticker := clock.NewTicker(5 * time.Second)
	t.Cleanup(ticker.Stop)

	clock.Advance(4 * time.Second)
	select {
	case <-ticker.C():
		t.Fatal("ticker fired before its hand-derived five second period")
	default:
	}

	clock.Advance(time.Second)
	select {
	case got := <-ticker.C():
		require.Equal(t, start.Add(5*time.Second), got)
	default:
		t.Fatal("ticker did not fire synchronously when the manual clock reached its period")
	}
	assert.Equal(t, start.Add(5*time.Second), clock.Now())
}
