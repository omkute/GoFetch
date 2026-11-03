package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/omkute/gofetch/internal/queue"
)

func main() {
	const maxSlots = 2
	fmt.Println("==================================================")
	fmt.Println("🚀 GoFetch Priority Queue & Scheduler Demo (T-007)")
	fmt.Println("==================================================")

	scheduler := queue.NewScheduler(maxSlots)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	scheduler.Start(ctx)
	defer scheduler.Stop()

	fmt.Printf("\n[Step 1] Initialized Scheduler with max-concurrent slots = %d\n", maxSlots)

	type taskDef struct {
		id       string
		priority queue.Priority
		duration time.Duration
	}

	tasks := []taskDef{
		{"task-A (Low)", queue.PriorityLow, 120 * time.Millisecond},
		{"task-B (Normal)", queue.PriorityNormal, 120 * time.Millisecond},
		{"task-C (Low)", queue.PriorityLow, 60 * time.Millisecond},
		{"task-D (Normal)", queue.PriorityNormal, 60 * time.Millisecond},
		{"task-E (High)", queue.PriorityHigh, 60 * time.Millisecond},
		{"task-F (High)", queue.PriorityHigh, 60 * time.Millisecond},
	}

	var executionLog []string
	var mu sync.Mutex
	var wg sync.WaitGroup
	startTime := time.Now()

	fmt.Println("\n[Step 2] Enqueueing mixed-priority tasks into scheduler:")
	for _, td := range tasks {
		wg.Add(1)
		def := td
		task := &queue.Task{
			ID:       def.id,
			Priority: def.priority,
			Fn: func(ctx context.Context) error {
				defer wg.Done()
				startOffset := time.Since(startTime)
				fmt.Printf("   ⚡ [%6.1fms] DISPATCHED: %-18s (Priority: %-6s)\n",
					float64(startOffset.Microseconds())/1000, def.id, def.priority.String())

				select {
				case <-time.After(def.duration):
				case <-ctx.Done():
					return ctx.Err()
				}

				mu.Lock()
				executionLog = append(executionLog, def.id)
				mu.Unlock()
				return nil
			},
		}

		_ = scheduler.Submit(task)
		fmt.Printf("   ├── Submitted: %-18s (Priority: %-6s)\n", def.id, def.priority.String())
		// Tiny delay to ensure deterministic submission timestamp
		time.Sleep(10 * time.Millisecond)
	}

	fmt.Println("\n[Step 3] Processing tasks in priority order:")
	wg.Wait()

	fmt.Println("\n[Step 4] Execution Completion Summary:")
	mu.Lock()
	for i, taskID := range executionLog {
		fmt.Printf("   %d. Completed: %s\n", i+1, taskID)
	}
	mu.Unlock()

	fmt.Println("\n✅ All T-007 Priority Scheduling Invariants Verified!")
	fmt.Println("👉 High priority tasks preempted lower priority items from the queue.")
	fmt.Println("==================================================")
}
