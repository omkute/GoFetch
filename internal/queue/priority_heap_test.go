package queue_test

import (
	"sync"
	"testing"
	"time"

	"github.com/omkute/gofetch/internal/queue"
)

func TestPriorityHeap_Ordering(t *testing.T) {
	t0 := time.Now()
	t1 := t0.Add(1 * time.Millisecond)
	t2 := t0.Add(2 * time.Millisecond)
	t3 := t0.Add(3 * time.Millisecond)
	t4 := t0.Add(4 * time.Millisecond)

	taskLow := &queue.Task{ID: "low-1", Priority: queue.PriorityLow, CreatedAt: t0}
	taskNormal1 := &queue.Task{ID: "norm-1", Priority: queue.PriorityNormal, CreatedAt: t1}
	taskHigh1 := &queue.Task{ID: "high-1", Priority: queue.PriorityHigh, CreatedAt: t2}
	taskHigh2 := &queue.Task{ID: "high-2", Priority: queue.PriorityHigh, CreatedAt: t3}
	taskNormal2 := &queue.Task{ID: "norm-2", Priority: queue.PriorityNormal, CreatedAt: t4}

	pq := queue.NewPriorityQueue()
	// Insert in non-priority mixed order
	pq.Push(taskLow)
	pq.Push(taskHigh2)
	pq.Push(taskNormal1)
	pq.Push(taskHigh1)
	pq.Push(taskNormal2)

	if pq.Len() != 5 {
		t.Fatalf("expected queue len 5, got %d", pq.Len())
	}

	// Expected order: High1 (earlier than High2) -> High2 -> Norm1 (earlier than Norm2) -> Norm2 -> Low
	expectedIDs := []string{"high-1", "high-2", "norm-1", "norm-2", "low-1"}
	for i, expectedID := range expectedIDs {
		popped := pq.Pop()
		if popped == nil {
			t.Fatalf("step %d: expected task %s, got nil", i, expectedID)
		}
		if popped.ID != expectedID {
			t.Fatalf("step %d: expected task %s, got %s (priority %v)", i, expectedID, popped.ID, popped.Priority)
		}
	}

	if pq.Pop() != nil {
		t.Fatal("expected nil pop on empty queue")
	}
}

func TestPriorityHeap_RemoveByID(t *testing.T) {
	pq := queue.NewPriorityQueue()
	t0 := time.Now()

	t1 := &queue.Task{ID: "task-1", Priority: queue.PriorityNormal, CreatedAt: t0}
	t2 := &queue.Task{ID: "task-2", Priority: queue.PriorityHigh, CreatedAt: t0.Add(time.Millisecond)}
	t3 := &queue.Task{ID: "task-3", Priority: queue.PriorityLow, CreatedAt: t0.Add(2 * time.Millisecond)}

	pq.Push(t1)
	pq.Push(t2)
	pq.Push(t3)

	// Remove high priority task-2 before it's popped
	removed := pq.Remove("task-2")
	if !removed {
		t.Fatal("expected Remove(task-2) to return true")
	}

	if pq.Len() != 2 {
		t.Fatalf("expected 2 tasks remaining, got %d", pq.Len())
	}

	first := pq.Pop()
	if first == nil || first.ID != "task-1" {
		t.Fatalf("expected task-1, got %v", first)
	}

	second := pq.Pop()
	if second == nil || second.ID != "task-3" {
		t.Fatalf("expected task-3, got %v", second)
	}
}

func TestPriorityHeap_ConcurrentAccess(t *testing.T) {
	pq := queue.NewPriorityQueue()
	const numGoroutines = 10
	const opsPerGoroutine = 50

	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		gIdx := g
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				task := &queue.Task{
					ID:        "concurrent",
					Priority:  queue.Priority((gIdx + i) % 3),
					CreatedAt: time.Now(),
				}
				pq.Push(task)
				_ = pq.Peek()
				_ = pq.Len()
				_ = pq.Pop()
			}
		}()
	}

	wg.Wait()
}
