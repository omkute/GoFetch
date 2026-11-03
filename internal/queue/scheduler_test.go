package queue_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/omkute/gofetch/internal/queue"
)

func TestScheduler_DispatchesHighPriorityFirst(t *testing.T) {
	// Scheduler with maxConcurrent=1 to test strict serialized slot assignment
	scheduler := queue.NewScheduler(1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	scheduler.Start(ctx)
	defer scheduler.Stop()

	gate1 := make(chan struct{})
	executedOrder := make([]string, 0, 4)
	var mu sync.Mutex

	recordExecution := func(id string) {
		mu.Lock()
		executedOrder = append(executedOrder, id)
		mu.Unlock()
	}

	// 1. Submit Task 1 (Low priority) which blocks on gate1
	task1Started := make(chan struct{})
	task1 := &queue.Task{
		ID:       "task-1-low",
		Priority: queue.PriorityLow,
		Fn: func(ctx context.Context) error {
			close(task1Started)
			<-gate1
			recordExecution("task-1-low")
			return nil
		},
	}
	_ = scheduler.Submit(task1)

	// Wait until task 1 occupies the sole execution slot
	select {
	case <-task1Started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for task 1 to start")
	}

	// 2. While Task 1 is running, submit Normal, Low, and High priority tasks in that order
	task2 := &queue.Task{
		ID:       "task-2-norm",
		Priority: queue.PriorityNormal,
		Fn: func(ctx context.Context) error {
			recordExecution("task-2-norm")
			return nil
		},
	}
	task3 := &queue.Task{
		ID:       "task-3-low",
		Priority: queue.PriorityLow,
		Fn: func(ctx context.Context) error {
			recordExecution("task-3-low")
			return nil
		},
	}
	task4 := &queue.Task{
		ID:       "task-4-high",
		Priority: queue.PriorityHigh,
		Fn: func(ctx context.Context) error {
			recordExecution("task-4-high")
			return nil
		},
	}

	_ = scheduler.Submit(task2)
	time.Sleep(5 * time.Millisecond)
	_ = scheduler.Submit(task3)
	time.Sleep(5 * time.Millisecond)
	_ = scheduler.Submit(task4) // Submitted LAST, but has highest priority!

	// 3. Release gate 1 to let task 1 complete and trigger the scheduler for remaining tasks
	close(gate1)

	// Wait for all 4 tasks to finish
	deadline := time.After(3 * time.Second)
	for {
		mu.Lock()
		count := len(executedOrder)
		mu.Unlock()
		if count == 4 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for all tasks to complete, only executed: %v", executedOrder)
		case <-time.After(10 * time.Millisecond):
		}
	}

	// Expected execution sequence:
	// task-1-low was already active
	// next MUST be task-4-high (PriorityHigh > Normal > Low)
	// next MUST be task-2-norm (PriorityNormal > Low)
	// next MUST be task-3-low
	expected := []string{"task-1-low", "task-4-high", "task-2-norm", "task-3-low"}
	for i, exp := range expected {
		if executedOrder[i] != exp {
			t.Fatalf("position %d: expected %s, got %s (full order: %v)",
				i, exp, executedOrder[i], executedOrder)
		}
	}
}

func TestScheduler_CancelEnqueuedTask(t *testing.T) {
	scheduler := queue.NewScheduler(1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	scheduler.Start(ctx)
	defer scheduler.Stop()

	gate := make(chan struct{})
	task1 := &queue.Task{
		ID:       "blocking-task",
		Priority: queue.PriorityHigh,
		Fn: func(ctx context.Context) error {
			<-gate
			return nil
		},
	}
	_ = scheduler.Submit(task1)

	task2Ran := false
	task2 := &queue.Task{
		ID:       "to-cancel",
		Priority: queue.PriorityNormal,
		Fn: func(ctx context.Context) error {
			task2Ran = true
			return nil
		},
	}
	_ = scheduler.Submit(task2)

	// Cancel task2 while it's still in the queue waiting for slot
	cancelled := scheduler.Cancel("to-cancel")
	if !cancelled {
		t.Fatal("expected Cancel(to-cancel) to return true")
	}

	close(gate) // let task1 finish
	time.Sleep(50 * time.Millisecond)

	if task2Ran {
		t.Fatal("cancelled task2 should not have run")
	}
}
