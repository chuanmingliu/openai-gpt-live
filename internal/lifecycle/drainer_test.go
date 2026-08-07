package lifecycle

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestDrainerLinearizesAcquireAndBegin(t *testing.T) {
	d := NewDrainer()
	token, ok := d.Acquire()
	if !ok || !d.Begin() || d.Accepting() {
		t.Fatal("unexpected transition")
	}
	if _, ok := d.Acquire(); ok {
		t.Fatal("late work admitted")
	}
	done := make(chan error, 1)
	go func() { done <- d.Wait(context.Background()) }()
	select {
	case <-done:
		t.Fatal("returned with active work")
	default:
	}
	token.Release()
	token.Release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestDrainerConcurrentBeginAndTimeout(t *testing.T) {
	d := NewDrainer()
	token, _ := d.Acquire()
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() { defer wg.Done(); d.Begin() }()
	}
	wg.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := d.Wait(ctx); !errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("got %v", err)
	}
	token.Release()
}

func TestDrainerZeroWork(t *testing.T) {
	d := NewDrainer()
	if err := d.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestDrainTimeoutCancelsRemainingIO(t *testing.T) {
	d := NewDrainer()
	token, _ := d.Acquire()
	cancelled := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	cleanup, cleanupCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cleanupCancel()
	err := d.Drain(ctx, func() { close(cancelled); token.Release() }, cleanup)
	if !errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("got %v", err)
	}
	select {
	case <-cancelled:
	default:
		t.Fatal("remaining I/O was not cancelled")
	}
}
