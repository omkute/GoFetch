package state

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"time"
)

// EventType categorizes download and chunk transfer lifecycle events.
type EventType string

const (
	// EventDownloadStarted is emitted when a download job initiates.
	EventDownloadStarted EventType = "download.started"
	// EventChunkCompleted is emitted when a chunk finishes writing to disk.
	EventChunkCompleted EventType = "chunk.completed"
	// EventDownloadFinished is emitted when a download fully completes and is committed.
	EventDownloadFinished EventType = "download.finished"
	// EventDownloadFailed is emitted when an unrecoverable failure occurs.
	EventDownloadFailed EventType = "download.failed"
)

// Event represents an individual lifecycle occurrence.
type Event struct {
	Type      EventType
	JobID     string
	Payload   any
	Timestamp time.Time
}

// DownloadStartedPayload provides metadata when a download begins.
type DownloadStartedPayload struct {
	URL       string
	Filename  string
	TotalSize int64
	Chunks    int
}

// ChunkCompletedPayload reports metrics for a completed chunk slice.
type ChunkCompletedPayload struct {
	ChunkIndex int
	BytesRead  int64
	Duration   time.Duration
}

// DownloadFinishedPayload reports completion metrics.
type DownloadFinishedPayload struct {
	FinalPath    string
	TotalBytes   int64
	TotalElapsed time.Duration
	Checksum     string
}

// DownloadFailedPayload details a failed download job.
type DownloadFailedPayload struct {
	Error string
}

type subscription struct {
	id        string
	types     map[EventType]struct{}
	ch        chan Event
	allEvents bool
}

// EventBus is an asynchronous, non-blocking on the hot path pub/sub event dispatcher.
type EventBus struct {
	mu          sync.RWMutex
	subscribers map[string]*subscription
	defaultCap  int
	dropped     atomic.Int64
	closed      atomic.Bool
}

// NewEventBus creates a new EventBus with the specified default buffer capacity.
func NewEventBus(defaultCapacity int) *EventBus {
	if defaultCapacity <= 0 {
		defaultCapacity = 64
	}
	return &EventBus{
		subscribers: make(map[string]*subscription),
		defaultCap:  defaultCapacity,
	}
}

func generateSubID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Subscribe registers a new subscriber receiving events of the specified types.
// If no types are provided, all events are delivered.
func (b *EventBus) Subscribe(bufferCap int, types ...EventType) (string, <-chan Event) {
	if bufferCap <= 0 {
		bufferCap = b.defaultCap
	}

	subID := generateSubID()
	typeMap := make(map[EventType]struct{}, len(types))
	for _, t := range types {
		typeMap[t] = struct{}{}
	}

	sub := &subscription{
		id:        subID,
		types:     typeMap,
		ch:        make(chan Event, bufferCap),
		allEvents: len(types) == 0,
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	b.subscribers[subID] = sub

	return subID, sub.ch
}

// Unsubscribe removes a subscription and closes its channel.
func (b *EventBus) Unsubscribe(subID string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if sub, exists := b.subscribers[subID]; exists {
		delete(b.subscribers, subID)
		close(sub.ch)
	}
}

// Publish dispatches an event to all matching subscribers.
// It is guaranteed non-blocking: if a subscriber's buffer is full, the event
// is dropped for that subscriber and DroppedCount is incremented, ensuring
// zero latency or blocking on the hot path chunk transfer loop.
func (b *EventBus) Publish(evt Event) bool {
	if b.closed.Load() {
		return false
	}

	if evt.Timestamp.IsZero() {
		evt.Timestamp = time.Now()
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	if len(b.subscribers) == 0 {
		return true
	}

	allDelivered := true
	for _, sub := range b.subscribers {
		if !sub.allEvents {
			if _, match := sub.types[evt.Type]; !match {
				continue
			}
		}

		select {
		case sub.ch <- evt:
		default:
			b.dropped.Add(1)
			allDelivered = false
		}
	}

	return allDelivered
}

// DroppedCount returns the cumulative number of events dropped due to full subscriber buffers.
func (b *EventBus) DroppedCount() int64 {
	return b.dropped.Load()
}

// Close gracefully closes the event bus and all active subscriber channels.
func (b *EventBus) Close() {
	if b.closed.Swap(true) {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	for id, sub := range b.subscribers {
		delete(b.subscribers, id)
		close(sub.ch)
	}
}
