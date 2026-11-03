package queue_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/omkute/gofetch/internal/queue"
)

func TestSemaphore_AcquireAndRelease(t *testing.T) {
	sem := queue.NewSemaphore(2)

	if sem.Capacity() != 2 {
		t.Fatalf("expected capacity 2, got %d", sem.Capacity())
	}
	if sem.Available() != 2 {
		t.Fatalf("expected available 2, got %d", sem.Available())
	}

	ctx := context.Background()

	// Acquire 1
	if err := sem.Acquire(ctx); err != nil {
		t.Fatalf("failed acquiring ticket 1: %v", err)
	}
	if sem.Available() != 1 {
		t.Fatalf("expected available 1, got %d", sem.Available())
	}

	// Acquire 2
	if err := sem.Acquire(ctx); err != nil {
		t.Fatalf("failed acquiring ticket 2: %v", err)
	}
	if sem.Available() != 0 {
		t.Fatalf("expected available 0, got %d", sem.Available())
	}

	// 3rd acquire with timeout must fail
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()

	err := sem.Acquire(timeoutCtx)
	if err == nil {
		t.Fatal("expected acquire to block and time out, got nil error")
	}

	// Release 1 ticket
	sem.Release()
	if sem.Available() != 1 {
		t.Fatalf("expected available 1 after release, got %d", sem.Available())
	}

	// Now acquire should succeed
	quickCtx, quickCancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer quickCancel()
	if err := sem.Acquire(quickCtx); err != nil {
		t.Fatalf("acquire failed after release: %v", err)
	}
}

func TestSemaphore_TryAcquire(t *testing.T) {
	sem := queue.NewSemaphore(1)

	if !sem.TryAcquire() {
		t.Fatal("expected first TryAcquire to succeed")
	}

	if sem.TryAcquire() {
		t.Fatal("expected second TryAcquire to fail when exhausted")
	}

	sem.Release()

	if !sem.TryAcquire() {
		t.Fatal("expected TryAcquire to succeed after release")
	}
}

func TestSemaphore_ConcurrentBoundedCapacity(t *testing.T) {
	const capacity = 3
	const numGoroutines = 20

	sem := queue.NewSemaphore(capacity)
	var activeCount int32
	var maxObserved int32
	var wg sync.WaitGroup

	ctx := context.Background()

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := sem.Acquire(ctx); err != nil {
				t.Errorf("acquire error: %v", err)
				return
			}

			current := atomic.AddInt32(&activeCount, 1)
			for {
				prevMax := atomic.LoadInt32(&maxObserved)
				if current <= prevMax || atomic.CompareAndSwapInt32(&maxObserved, prevMax, current) {
					break
				}
			}

			// Hold slot briefly
			time.Sleep(5 * time.Millisecond)

			atomic.AddInt32(&activeCount, -1)
			sem.Release()
		}()
	}

	wg.Wait()

	if maxObserved > capacity {
		t.Fatalf("semaphore invariant violated: max observed active %d exceeded capacity %d", maxObserved, capacity)
	}
}
