package queue

import (
	"container/heap"
	"sync"
)

// taskHeap implements container/heap.Interface for *Task.
type taskHeap []*Task

func (h taskHeap) Len() int { return len(h) }

func (h taskHeap) Less(i, j int) bool {
	// 1. Highest priority comes first (PriorityHigh > Normal > Low)
	if h[i].Priority != h[j].Priority {
		return h[i].Priority > h[j].Priority
	}
	// 2. Deterministic FIFO tie-breaking: earlier CreatedAt pops first
	return h[i].CreatedAt.Before(h[j].CreatedAt)
}

func (h taskHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *taskHeap) Push(x any) {
	n := len(*h)
	item := x.(*Task)
	item.index = n
	*h = append(*h, item)
}

func (h *taskHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	item.index = -1
	*h = old[0 : n-1]
	return item
}

// PriorityQueue provides a thread-safe priority queue wrapping container/heap.
type PriorityQueue struct {
	mu    sync.Mutex
	h     taskHeap
	items map[string]*Task
}

// NewPriorityQueue creates an empty PriorityQueue.
func NewPriorityQueue() *PriorityQueue {
	pq := &PriorityQueue{
		h:     make(taskHeap, 0),
		items: make(map[string]*Task),
	}
	heap.Init(&pq.h)
	return pq
}

// Push adds a task into the priority queue, organizing it by priority and timestamp.
func (pq *PriorityQueue) Push(task *Task) {
	if task == nil {
		return
	}
	pq.mu.Lock()
	defer pq.mu.Unlock()

	pq.items[task.ID] = task
	heap.Push(&pq.h, task)
}

// Pop removes and returns the highest-priority task from the queue, or nil if empty.
func (pq *PriorityQueue) Pop() *Task {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if len(pq.h) == 0 {
		return nil
	}

	task := heap.Pop(&pq.h).(*Task)
	delete(pq.items, task.ID)
	return task
}

// Peek returns the highest-priority task without removing it.
func (pq *PriorityQueue) Peek() *Task {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if len(pq.h) == 0 {
		return nil
	}
	return pq.h[0]
}

// Remove deletes a task by ID from the queue. Returns true if task was found and removed.
func (pq *PriorityQueue) Remove(id string) bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	task, exists := pq.items[id]
	if !exists || task.index < 0 || task.index >= len(pq.h) {
		return false
	}

	heap.Remove(&pq.h, task.index)
	delete(pq.items, id)
	return true
}

// Len returns the current count of enqueued tasks.
func (pq *PriorityQueue) Len() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return len(pq.h)
}

// Get looks up an enqueued task by ID.
func (pq *PriorityQueue) Get(id string) *Task {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return pq.items[id]
}
