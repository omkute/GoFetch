package engine

import (
	"sync"
	"time"
)

// defaultCheckpointInterval bounds how often chunk progress is handed off to
// the checkpoint consumer. It is intentionally throttled so streaming workers
// never perform storage I/O on every 32 KB write.
const defaultCheckpointInterval = 500 * time.Millisecond

// checkpointTracker collects in-flight download offsets without doing any
// database work in worker goroutines. Workers publish offsets through a
// mutex-protected snapshot; a throttled, non-blocking handoff sends copies to a
// single background goroutine which is the only place CheckpointStore writes
// happen.
type checkpointTracker struct {
	store    CheckpointStore
	template Checkpoint
	bounds   []Chunk

	mu       sync.Mutex
	offsets  []int64
	complete []bool
	last     time.Time
	interval time.Duration

	ch        chan []Chunk
	done      chan struct{}
	closeOnce sync.Once
}

func newCheckpointTracker(store CheckpointStore, cp *Checkpoint, chunks []Chunk, interval time.Duration) *checkpointTracker {
	if interval <= 0 {
		interval = defaultCheckpointInterval
	}
	t := &checkpointTracker{
		store:    store,
		template: *cp,
		bounds:   append([]Chunk(nil), chunks...),
		offsets:  make([]int64, len(chunks)),
		complete: make([]bool, len(chunks)),
		last:     time.Now(),
		interval: interval,
		ch:       make(chan []Chunk, 8),
		done:     make(chan struct{}),
	}
	for i := range chunks {
		t.offsets[i] = chunks[i].Current
		t.complete[i] = chunks[i].Completed
	}
	go t.run()
	return t
}

// record publishes the latest offset for a chunk. It is safe to call from
// concurrent worker goroutines: each worker owns its chunk index.
func (t *checkpointTracker) record(index int, current int64, completed bool) {
	if index < 0 || index >= len(t.offsets) {
		return
	}
	t.mu.Lock()
	t.offsets[index] = current
	t.complete[index] = completed
	t.mu.Unlock()
}

// maybePersist performs a throttled, non-blocking handoff of a progress
// snapshot. A full buffer drops the update; the final save on completion or
// interruption always captures the exact offsets.
func (t *checkpointTracker) maybePersist(now time.Time) {
	t.mu.Lock()
	if now.Sub(t.last) < t.interval {
		t.mu.Unlock()
		return
	}
	t.last = now
	snap := t.snapshotLocked()
	t.mu.Unlock()

	select {
	case t.ch <- snap:
	default:
	}
}

func (t *checkpointTracker) snapshotLocked() []Chunk {
	snap := make([]Chunk, len(t.bounds))
	for i, b := range t.bounds {
		snap[i] = b
		snap[i].Current = t.offsets[i]
		snap[i].Completed = t.complete[i]
	}
	return snap
}

// stop drains the consumer and waits for it to finish. All database writes
// are complete when it returns, so a final authoritative save can follow.
func (t *checkpointTracker) stop() {
	t.closeOnce.Do(func() { close(t.ch) })
	<-t.done
}

func (t *checkpointTracker) run() {
	defer close(t.done)
	for snap := range t.ch {
		cp := t.template
		cp.Chunks = snap
		cp.UpdatedAt = time.Now()
		_ = t.store.SaveCheckpoint(&cp)
	}
}
