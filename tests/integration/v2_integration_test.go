package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/omkute/gofetch/internal/api"
	"github.com/omkute/gofetch/internal/state"
)

func TestV2_FullEndToEndIntegration(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "daemon_tasks.db")

	// 1. Setup persistent TaskStore
	taskStore, err := state.NewTaskStore(dbPath)
	if err != nil {
		t.Fatalf("failed to initialize task store: %v", err)
	}
	defer func() { _ = taskStore.Close() }()

	server := api.NewServer("127.0.0.1:0")
	server.SetTaskStore(taskStore)

	// Mock file server
	mockPayload := bytes.Repeat([]byte("GoFetch v2 resilient streaming payload! "), 1000)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(mockPayload)))
		http.ServeContent(w, r, "testfile.bin", time.Now(), bytes.NewReader(mockPayload))
	}))
	defer ts.Close()

	router := server.Router()

	// 2. Verify Embedded Web Dashboard delivery
	recRoot := httptest.NewRecorder()
	reqRoot := httptest.NewRequest(http.MethodGet, "/", nil)
	router.ServeHTTP(recRoot, reqRoot)
	if recRoot.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for GET /, got %d", recRoot.Code)
	}
	if !strings.Contains(recRoot.Body.String(), "GoFetch v2") {
		t.Errorf("expected dashboard HTML title to contain 'GoFetch v2'")
	}

	// 3. Test Batch URL Enqueueing
	batchReq := api.BatchDownloadRequest{
		URLs: []string{
			ts.URL + "/file1.bin",
			ts.URL + "/file2.bin",
		},
		DestinationDir: tempDir,
		Priority:       "high",
		RateLimit:      "5MB",
	}
	body, _ := json.Marshal(batchReq)
	recBatch := httptest.NewRecorder()
	reqBatch := httptest.NewRequest(http.MethodPost, "/api/v1/downloads/batch", bytes.NewReader(body))
	router.ServeHTTP(recBatch, reqBatch)

	if recBatch.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted for batch enqueue, got %d: %s", recBatch.Code, recBatch.Body.String())
	}

	var batchResp api.BatchDownloadResponse
	if err := json.NewDecoder(recBatch.Body).Decode(&batchResp); err != nil {
		t.Fatalf("failed decoding batch response: %v", err)
	}
	if batchResp.Enqueued != 2 {
		t.Fatalf("expected 2 enqueued tasks, got %d", batchResp.Enqueued)
	}

	// 4. Verify tasks persisted in BBolt store
	tasks, err := taskStore.ListTasks()
	if err != nil {
		t.Fatalf("failed listing tasks from store: %v", err)
	}
	if len(tasks) != 2 {
		t.Fatalf("expected 2 tasks in BBolt store, got %d", len(tasks))
	}

	// 5. Test Dynamic Rate Limit adjustment
	recLimit := httptest.NewRecorder()
	reqLimit := httptest.NewRequest(http.MethodPost, "/api/v1/system/rate-limit", bytes.NewReader([]byte(`{"rate_limit":"15MB"}`)))
	router.ServeHTTP(recLimit, reqLimit)

	if recLimit.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for rate-limit set, got %d", recLimit.Code)
	}

	// 6. Test System Stats Telemetry
	recStats := httptest.NewRecorder()
	reqStats := httptest.NewRequest(http.MethodGet, "/api/v1/system/stats", nil)
	router.ServeHTTP(recStats, reqStats)

	var stats api.SystemStatsResponse
	if err := json.NewDecoder(recStats.Body).Decode(&stats); err != nil {
		t.Fatalf("failed decoding stats: %v", err)
	}
	if stats.GlobalRateLimitBps != 15*1024*1024 {
		t.Errorf("expected 15MB limit in stats, got %d", stats.GlobalRateLimitBps)
	}

	// 7. Test Real-time SSE Stream
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	recSSE := httptest.NewRecorder()
	reqSSE := httptest.NewRequest(http.MethodGet, "/api/v1/events", nil).WithContext(ctx)
	router.ServeHTTP(recSSE, reqSSE)

	if !strings.Contains(recSSE.Header().Get("Content-Type"), "text/event-stream") {
		t.Errorf("expected text/event-stream, got %q", recSSE.Header().Get("Content-Type"))
	}
	if !strings.Contains(recSSE.Body.String(), "event: progress") {
		t.Errorf("expected progress frame in SSE stream, got %s", recSSE.Body.String())
	}

	// 8. Test Pause-All and Resume-All Controls
	recPause := httptest.NewRecorder()
	reqPause := httptest.NewRequest(http.MethodPost, "/api/v1/downloads/pause-all", nil)
	router.ServeHTTP(recPause, reqPause)
	if recPause.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for pause-all, got %d", recPause.Code)
	}

	recResume := httptest.NewRecorder()
	reqResume := httptest.NewRequest(http.MethodPost, "/api/v1/downloads/resume-all", nil)
	router.ServeHTTP(recResume, reqResume)
	if recResume.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for resume-all, got %d", recResume.Code)
	}

	// 9. Verify CLI input file batch format
	urlsFile := filepath.Join(tempDir, "urls.txt")
	urlsContent := fmt.Sprintf("# Test comment\n%s/a.bin\n\n%s/b.bin\n", ts.URL, ts.URL)
	if err := os.WriteFile(urlsFile, []byte(urlsContent), 0600); err != nil {
		t.Fatalf("failed writing urls.txt: %v", err)
	}
}
