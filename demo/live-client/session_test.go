package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSessionGateRejectsAfterCapacityAndDrain(t *testing.T) {
	gate := newSessionGate(1)
	id, ok := gate.Acquire(func() {})
	assert.True(t, ok)
	_, again := gate.Acquire(func() {})
	assert.False(t, again)
	gate.Release(id)
	id, ok = gate.Acquire(func() {})
	assert.True(t, ok)
	gate.BeginDrain()
	assert.False(t, gate.Ready())
	gate.Release(id)
	_, ok = gate.Acquire(func() {})
	assert.False(t, ok)
}

func TestSessionGateCancelsAndUnblocksWaitOnDrain(t *testing.T) {
	var cancelled bool
	gate := newSessionGate(1)
	id, ok := gate.Acquire(func() { cancelled = true })
	require.True(t, ok)
	gate.BeginDrain()
	assert.True(t, cancelled)
	gate.Release(id)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, gate.Wait(ctx))
	_, ok = gate.Acquire(func() {})
	assert.False(t, ok)
}
