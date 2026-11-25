package api

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/omkute/gofetch/internal/state"
)

// TestProgressPersist_NonBlockingHandoffAndAsyncSave verifies that progress
// updates are handed off without blocking or touching the DB from a worker,
// and that only the dedicated background consumer performs the save.
func TestProgressPersist_NonBlockingHandoffAndAsyncSave(t *testing.T) {
	tempDir := t.TempDir()
	store, err := state.NewTaskStore(filepath.Join(tempDir, "tasks.db"))
	if err != nil {
		t.Fatalf("failed to create task store: %v", err)
	}
	defer func() { _ = store.Close() }()

	s := NewServer("")
	s.SetTaskStore(store)
	s.persistCh = make(chan state.PersistedTask, 1) // tiny buffer, no consumer yet

	job := &jobRecord{
		ID:              "progress-job",
		Status:          "downloading",
		DownloadedBytes: 4321,
		destDir:         tempDir,
	}

	// Saturated buffer with no consumer: handoffs must drop, never block.
	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			s.queuePersistSnapshot(job)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("queuePersistSnapshot blocked while the buffer was full")
	}
	if got := len(s.persistCh); got != 1 {
		t.Fatalf("expected exactly 1 buffered snapshot, got %d", got)
	}

	// The worker-side handoff must not perform any DB write.
	persisted, err := store.GetTask(job.ID)
	if err != nil {
		t.Fatalf("failed to query task store: %v", err)
	}
	if persisted != nil {
		t.Fatal("progress handoff must not write to the DB synchronously")
	}

	// Start the dedicated consumer: it, not the worker, saves the snapshot.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.runPersistConsumer(ctx)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		persisted, err = store.GetTask(job.ID)
		if err == nil && persisted != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if persisted == nil {
		t.Fatal("background consumer did not persist the progress snapshot")
	}
	if persisted.DownloadedBytes != 4321 {
		t.Errorf("persisted DownloadedBytes = %d, want 4321", persisted.DownloadedBytes)
	}
}
