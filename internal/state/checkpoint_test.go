package state_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/omkute/gofetch/internal/engine"
	"github.com/omkute/gofetch/internal/state"
)

func TestBboltStore_SaveAndRetrieve(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test_checkpoints.db")

	store, err := state.NewBboltStore(dbPath)
	if err != nil {
		t.Fatalf("failed to create BboltStore: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	cp := &state.Checkpoint{
		ID:        "job-abc-123",
		URL:       "https://example.com/archive.zip",
		FinalPath: "/downloads/archive.zip",
		TotalSize: 1048576,
		ETag:      `"etag-999"`,
		Chunks: []engine.Chunk{
			{Index: 0, Start: 0, End: 262143, Current: 262144, Completed: true},
			{Index: 1, Start: 262144, End: 524287, Current: 300000, Completed: false},
			{Index: 2, Start: 524288, End: 786431, Current: 524288, Completed: false},
			{Index: 3, Start: 786432, End: 1048575, Current: 786432, Completed: false},
		},
		UpdatedAt: time.Now().UTC().Truncate(time.Millisecond),
	}

	if err := store.SaveCheckpoint(cp); err != nil {
		t.Fatalf("failed to save checkpoint: %v", err)
	}

	retrieved, err := store.GetCheckpoint("job-abc-123")
	if err != nil {
		t.Fatalf("failed to retrieve checkpoint: %v", err)
	}
	if retrieved == nil {
		t.Fatal("expected checkpoint, got nil")
	}

	if retrieved.ID != cp.ID {
		t.Errorf("expected ID %q, got %q", cp.ID, retrieved.ID)
	}
	if retrieved.TotalSize != cp.TotalSize {
		t.Errorf("expected TotalSize %d, got %d", cp.TotalSize, retrieved.TotalSize)
	}
	if len(retrieved.Chunks) != 4 {
		t.Fatalf("expected 4 chunks, got %d", len(retrieved.Chunks))
	}
	if !retrieved.Chunks[0].Completed {
		t.Errorf("expected chunk 0 to be completed")
	}
	if retrieved.Chunks[1].Current != 300000 {
		t.Errorf("expected chunk 1 Current=300000, got %d", retrieved.Chunks[1].Current)
	}
}

func TestBboltStore_DeleteCheckpoint(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test_delete.db")

	store, err := state.NewBboltStore(dbPath)
	if err != nil {
		t.Fatalf("failed to open store: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	cp := &state.Checkpoint{
		ID:        "job-delete-me",
		URL:       "https://example.com/file.bin",
		FinalPath: "/downloads/file.bin",
		TotalSize: 500,
	}

	if err := store.SaveCheckpoint(cp); err != nil {
		t.Fatalf("save error: %v", err)
	}

	if err := store.DeleteCheckpoint("job-delete-me"); err != nil {
		t.Fatalf("delete error: %v", err)
	}

	retrieved, err := store.GetCheckpoint("job-delete-me")
	if err != nil {
		t.Fatalf("unexpected error after delete: %v", err)
	}
	if retrieved != nil {
		t.Fatalf("expected nil checkpoint after delete, got %+v", retrieved)
	}
}
