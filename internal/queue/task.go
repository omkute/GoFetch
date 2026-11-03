package queue

import (
	"context"
	"strings"
	"time"
)

// Priority represents the scheduling importance of a task.
type Priority int

const (
	// PriorityLow represents background/batch downloads.
	PriorityLow Priority = 0
	// PriorityNormal represents standard user downloads.
	PriorityNormal Priority = 1
	// PriorityHigh represents prioritized expedited downloads.
	PriorityHigh Priority = 2
)

// String returns the human-readable priority name.
func (p Priority) String() string {
	switch p {
	case PriorityHigh:
		return "high"
	case PriorityNormal:
		return "normal"
	case PriorityLow:
		return "low"
	default:
		return "normal"
	}
}

// ParsePriority parses a string into a Priority enum.
func ParsePriority(s string) Priority {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "high":
		return PriorityHigh
	case "low":
		return PriorityLow
	default:
		return PriorityNormal
	}
}

// TaskStatus represents the lifecycle state of a scheduled task.
type TaskStatus string

const (
	// StatusEnqueued task is queued in the priority heap waiting for an execution slot.
	StatusEnqueued TaskStatus = "enqueued"
	// StatusActive task is currently running in a worker slot.
	StatusActive TaskStatus = "active"
	// StatusPaused task has been temporarily suspended.
	StatusPaused TaskStatus = "paused"
	// StatusCompleted task has successfully downloaded and committed.
	StatusCompleted TaskStatus = "completed"
	// StatusFailed task encountered a non-recoverable error.
	StatusFailed TaskStatus = "failed"
	// StatusCanceled task was aborted by the user before completion.
	StatusCanceled TaskStatus = "canceled"
)

// Task encapsulates an actionable download unit within the scheduler.
type Task struct {
	ID        string                         `json:"id"`
	URL       string                         `json:"url"`
	Priority  Priority                       `json:"priority"`
	Status    TaskStatus                     `json:"status"`
	CreatedAt time.Time                      `json:"created_at"`
	Fn        func(ctx context.Context) error `json:"-"`
	Err       error                          `json:"error,omitempty"`

	index int // internal index maintained by container/heap
}
