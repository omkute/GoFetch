package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/omkute/gofetch/internal/state"
)

func TestDaemonTaskPersistenceAndRecovery(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "daemon_tasks.db")

	taskStore, err := state.NewTaskStore(dbPath)
	if err != nil {
		t.Fatalf("failed to create task store: %v", err)
	}

	server := NewServer("127.0.0.1:0")
	server.SetTaskStore(taskStore)

	// Enqueue a download
	reqBody := CreateDownloadRequest{
		URL:            "https://example.com/test-recovery.iso",
		DestinationDir: tempDir,
		Priority:       "high",
		RateLimit:      "5MB",
	}
	bodyBytes, _ := json.Marshal(reqBody)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/downloads", bytes.NewReader(bodyBytes))
	rec := httptest.NewRecorder()

	server.Router().ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d", rec.Code)
	}

	var dlResp DownloadResponse
	if err := json.NewDecoder(rec.Body).Decode(&dlResp); err != nil {
		t.Fatalf("failed decoding response: %v", err)
	}

	// Verify task in taskStore
	persisted, err := taskStore.GetTask(dlResp.ID)
	if err != nil {
		t.Fatalf("failed getting task from store: %v", err)
	}
	if persisted == nil {
		t.Fatal("expected task to be in task store, got nil")
	}
	if persisted.Priority != "high" || persisted.RateLimit != "5MB" {
		t.Errorf("task fields mismatch in store: %+v", persisted)
	}

	// Simulate restart: close old task store, create new server with new task store on same DB
	_ = taskStore.Close()

	reopenedStore, err := state.NewTaskStore(dbPath)
	if err != nil {
		t.Fatalf("reopening task store failed: %v", err)
	}
	defer func() { _ = reopenedStore.Close() }()

	server2 := NewServer("127.0.0.1:0")
	server2.SetTaskStore(reopenedStore)
	server2.loadPersistedTasks()

	server2.jobsMu.RLock()
	recoveredJob, exists := server2.jobs[dlResp.ID]
	server2.jobsMu.RUnlock()

	if !exists {
		t.Fatalf("expected task %s to be recovered in server2", dlResp.ID)
	}
	if recoveredJob.Status != "paused" {
		t.Errorf("expected recovered task to be paused, got %s", recoveredJob.Status)
	}
	if recoveredJob.Error != "Interrupted by daemon restart" {
		t.Errorf("expected restart error message, got %q", recoveredJob.Error)
	}
}
