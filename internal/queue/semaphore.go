package queue

import (
	"context"
)

// Semaphore provides a concurrency limiter bounding active concurrent operations.
type Semaphore struct {
	capacity int
	ch       chan struct{}
}

// NewSemaphore initializes a counting semaphore with the specified capacity.
func NewSemaphore(capacity int) *Semaphore {
	if capacity <= 0 {
		capacity = 1
	}

	return &Semaphore{
		capacity: capacity,
		ch:       make(chan struct{}, capacity),
	}
}

// Acquire blocks until a slot is available or ctx is cancelled.
func (s *Semaphore) Acquire(ctx context.Context) error {
	select {
	case s.ch <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release yields an acquired slot back to the semaphore pool.
func (s *Semaphore) Release() {
	select {
	case <-s.ch:
	default:
	}
}

// TryAcquire attempts to acquire a slot without blocking. Returns true if acquired.
func (s *Semaphore) TryAcquire() bool {
	select {
	case s.ch <- struct{}{}:
		return true
	default:
		return false
	}
}

// Capacity returns the maximum concurrent slot capacity.
func (s *Semaphore) Capacity() int {
	return s.capacity
}

// Available returns the count of currently unallocated slots.
func (s *Semaphore) Available() int {
	return s.capacity - len(s.ch)
}
