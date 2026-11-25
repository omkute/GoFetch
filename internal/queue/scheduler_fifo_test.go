package queue_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/omkute/gofetch/internal/queue"
)

// TestScheduler_FIFOSubmissionOrder verifies that the FIFO scheduler ignores
// priority, runs exactly one task at a time, and completes tasks in the order
// they were submitted.
func TestScheduler_FIFOSubmissionOrder(t *testing.T) {
	s := queue.NewFIFOScheduler(1)
	if !s.IsFIFO() {
		t.Fatal("expected FIFO scheduler")
	}
	if got := s.MaxConcurrent(); got != 1 {
		t.Fatalf("MaxConcurrent() = %d, want 1", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.Start(ctx)
	defer s.Stop()

	const taskCount = 5
	var mu sync.Mutex
	active := 0
	maxActive := 0
	completed := []string{}
	var wg sync.WaitGroup
	wg.Add(taskCount)

	for i := 0; i < taskCount; i++ {
		id := fmt.Sprintf("task-%d", i)
		priority := queue.PriorityLow
		if i == taskCount-1 {
			priority = queue.PriorityHigh // must NOT jump the queue in FIFO mode
		}
		if err := s.Submit(&queue.Task{
			ID:       id,
			Priority: priority,
			Fn: func(context.Context) error {
				mu.Lock()
				active++
				if active > maxActive {
					maxActive = active
				}
				mu.Unlock()

				time.Sleep(5 * time.Millisecond)

				mu.Lock()
				completed = append(completed, id)
				active--
				mu.Unlock()
				wg.Done()
				return nil
			},
		}); err != nil {
			t.Fatalf("Submit(%s) failed: %v", id, err)
		}
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for FIFO tasks")
	}

	mu.Lock()
	defer mu.Unlock()
	if maxActive != 1 {
		t.Errorf("max concurrent active tasks = %d, want 1", maxActive)
	}
	for i := 0; i < taskCount; i++ {
		want := fmt.Sprintf("task-%d", i)
		if completed[i] != want {
			t.Fatalf("completion[%d] = %s, want %s (full order: %v)", i, completed[i], want, completed)
		}
	}
}
