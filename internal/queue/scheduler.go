package queue

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Scheduler manages concurrency limits and executes tasks according to priority.
type Scheduler struct {
	maxConcurrent int
	pq            *PriorityQueue

	mu            sync.Mutex
	activeTasks   map[string]*Task
	activeCancels map[string]context.CancelFunc

	sem      chan struct{}
	notifyCh chan struct{}
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// NewScheduler creates a Scheduler configured with a maximum concurrent worker slot capacity.
func NewScheduler(maxConcurrent int) *Scheduler {
	if maxConcurrent <= 0 {
		maxConcurrent = 1
	}

	return &Scheduler{
		maxConcurrent: maxConcurrent,
		pq:            NewPriorityQueue(),
		activeTasks:   make(map[string]*Task),
		activeCancels: make(map[string]context.CancelFunc),
		sem:           make(chan struct{}, maxConcurrent),
		notifyCh:      make(chan struct{}, 1),
		stopCh:        make(chan struct{}),
	}
}

// Start launches the background scheduler dispatcher loop.
func (s *Scheduler) Start(ctx context.Context) {
	s.wg.Add(1)
	go s.loop(ctx)
}

func (s *Scheduler) loop(ctx context.Context) {
	defer s.wg.Done()

	for {
		// 1. Dispatch as many tasks as there are available slots and queued tasks
		for s.pq.Len() > 0 {
			select {
			case s.sem <- struct{}{}:
				// Acquired slot, pop next highest priority task
				task := s.pq.Pop()
				if task == nil {
					<-s.sem // Release slot if pop returned nil
					break
				}
				s.dispatch(ctx, task)
			default:
				// All concurrency slots are currently saturated
				goto waitForNotification
			}
		}

	waitForNotification:
		select {
		case <-s.stopCh:
			return
		case <-ctx.Done():
			return
		case <-s.notifyCh:
			// Notification triggered: new task submitted or worker slot freed
		}
	}
}

func (s *Scheduler) dispatch(parentCtx context.Context, task *Task) {
	taskCtx, cancel := context.WithCancel(parentCtx)

	s.mu.Lock()
	s.activeTasks[task.ID] = task
	s.activeCancels[task.ID] = cancel
	task.Status = StatusActive
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer func() {
			<-s.sem // Release slot back to pool

			s.mu.Lock()
			delete(s.activeTasks, task.ID)
			delete(s.activeCancels, task.ID)
			s.mu.Unlock()

			s.notify() // Wake up dispatcher loop to schedule next pending task
		}()

		if task.Fn != nil {
			err := task.Fn(taskCtx)
			task.Err = err

			s.mu.Lock()
			if err != nil {
				if errors.Is(err, context.Canceled) {
					task.Status = StatusCanceled
				} else {
					task.Status = StatusFailed
				}
			} else {
				task.Status = StatusCompleted
			}
			s.mu.Unlock()
		}
	}()
}

// Submit enqueues a new task into the priority queue.
func (s *Scheduler) Submit(task *Task) error {
	if task == nil {
		return errors.New("cannot submit nil task")
	}
	if task.CreatedAt.IsZero() {
		task.CreatedAt = time.Now()
	}

	task.Status = StatusEnqueued
	s.pq.Push(task)
	s.notify()
	return nil
}

// Cancel removes a task if enqueued, or signals cancellation if currently active.
func (s *Scheduler) Cancel(id string) bool {
	// 1. Try removing from pending priority queue
	if s.pq.Remove(id) {
		return true
	}

	// 2. If active, invoke its context cancel function
	s.mu.Lock()
	cancel, exists := s.activeCancels[id]
	s.mu.Unlock()

	if exists && cancel != nil {
		cancel()
		return true
	}

	return false
}

// notify sends a non-blocking signal to wake up the scheduler dispatcher.
func (s *Scheduler) notify() {
	select {
	case s.notifyCh <- struct{}{}:
	default:
	}
}

// Stop shuts down the scheduler, cancels active tasks, and waits for worker goroutines.
func (s *Scheduler) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopCh)

		s.mu.Lock()
		for _, cancel := range s.activeCancels {
			if cancel != nil {
				cancel()
			}
		}
		s.mu.Unlock()

		s.wg.Wait()
	})
}
