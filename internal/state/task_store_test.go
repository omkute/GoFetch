package state

import (
	"path/filepath"
	"testing"
	"time"
)

func TestTaskStore_Lifecycle(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test_tasks.db")

	store, err := NewTaskStore(dbPath)
	if err != nil {
		t.Fatalf("failed to create task store: %v", err)
	}
	defer func() { _ = store.Close() }()

	task := &PersistedTask{
		ID:         "task-1234",
		URL:        "https://example.com/file.zip",
		FileName:   "file.zip",
		TargetPath: "/downloads/file.zip",
		Status:     "queued",
		Priority:   "high",
		TotalBytes: 1024 * 1024,
		CreatedAt:  time.Now().UTC(),
	}

	// 1. Save
	if err := store.SaveTask(task); err != nil {
		t.Fatalf("SaveTask failed: %v", err)
	}

	// 2. Get
	fetched, err := store.GetTask("task-1234")
	if err != nil {
		t.Fatalf("GetTask failed: %v", err)
	}
	if fetched == nil {
		t.Fatal("expected task, got nil")
	}
	if fetched.FileName != "file.zip" || fetched.Priority != "high" {
		t.Errorf("task data mismatch: got %+v", fetched)
	}

	// 3. Update
	task.Status = "downloading"
	task.DownloadedBytes = 512 * 1024
	if err := store.SaveTask(task); err != nil {
		t.Fatalf("SaveTask update failed: %v", err)
	}

	// 4. List
	all, err := store.ListTasks()
	if err != nil {
		t.Fatalf("ListTasks failed: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("expected 1 task, got %d", len(all))
	}
	if all[0].Status != "downloading" {
		t.Errorf("expected status 'downloading', got %q", all[0].Status)
	}

	// 5. Close and reopen to verify durability
	if err := store.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	reopened, err := NewTaskStore(dbPath)
	if err != nil {
		t.Fatalf("reopening store failed: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	persisted, err := reopened.GetTask("task-1234")
	if err != nil {
		t.Fatalf("GetTask after reopen failed: %v", err)
	}
	if persisted == nil || persisted.DownloadedBytes != 512*1024 {
		t.Errorf("persisted task mismatch after reopen: %+v", persisted)
	}

	// 6. Delete
	if err := reopened.DeleteTask("task-1234"); err != nil {
		t.Fatalf("DeleteTask failed: %v", err)
	}
	deleted, err := reopened.GetTask("task-1234")
	if err != nil {
		t.Fatalf("GetTask after delete failed: %v", err)
	}
	if deleted != nil {
		t.Errorf("expected nil after delete, got %+v", deleted)
	}
}
