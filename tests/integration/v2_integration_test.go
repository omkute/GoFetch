package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
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

	// 1. Setup persistent TaskStore (closed by the daemon's cleanup).
	taskStore, err := state.NewTaskStore(dbPath)
	if err != nil {
		t.Fatalf("failed to initialize task store: %v", err)
	}
	t.Cleanup(func() { _ = taskStore.Close() })

	// The scheduler is started by the helper, so the whole E2E flow runs
	// against a real loopback listener rather than an in-memory router.
	daemon := startTestDaemon(t, func(s *api.Server) { s.SetTaskStore(taskStore) })

	// Mock range-capable origin for every path used below.
	mockPayload := bytes.Repeat([]byte("GoFetch v2 resilient streaming payload! "), 1000)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(mockPayload)))
		http.ServeContent(w, r, "testfile.bin", time.Time{}, bytes.NewReader(mockPayload))
	}))
	t.Cleanup(origin.Close)

	// 2. Verify Embedded Web Dashboard delivery over HTTP.
	rootResp := daemon.request(t, http.MethodGet, "/", nil, "")
	rootBody, err := io.ReadAll(rootResp.Body)
	_ = rootResp.Body.Close()
	if err != nil {
		t.Fatalf("read dashboard response: %v", err)
	}
	if rootResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for GET /, got %d", rootResp.StatusCode)
	}
	if !strings.Contains(string(rootBody), "GoFetch v2") {
		t.Errorf("expected dashboard HTML title to contain 'GoFetch v2'")
	}

	// 3. Test Batch URL Enqueueing over HTTP.
	var batchResp api.BatchDownloadResponse
	if code := daemon.postJSON(t, "/api/v1/downloads/batch", api.BatchDownloadRequest{
		URLs: []string{
			origin.URL + "/file1.bin",
			origin.URL + "/file2.bin",
		},
		DestinationDir: tempDir,
		Priority:       "high",
		RateLimit:      "5MB",
		NoOrganize:     true,
	}, &batchResp); code != http.StatusAccepted {
		t.Fatalf("batch enqueue: expected 202 Accepted, got %d", code)
	}
	if batchResp.Enqueued != 2 {
		t.Fatalf("expected 2 enqueued tasks, got %d", batchResp.Enqueued)
	}

	// 4. Verify tasks persisted in BBolt store.
	tasks, err := taskStore.ListTasks()
	if err != nil {
		t.Fatalf("failed listing tasks from store: %v", err)
	}
	if len(tasks) != 2 {
		t.Fatalf("expected 2 tasks in BBolt store, got %d", len(tasks))
	}

	// 5. Test Dynamic Rate Limit adjustment.
	if code := daemon.postJSON(t, "/api/v1/system/rate-limit", map[string]string{"rate_limit": "15MB"}, nil); code != http.StatusOK {
		t.Fatalf("rate-limit set: expected 200 OK, got %d", code)
	}

	// 6. Test System Stats Telemetry.
	var stats api.SystemStatsResponse
	if code := daemon.getJSON(t, "/api/v1/system/stats", &stats); code != http.StatusOK {
		t.Fatalf("system stats: expected 200 OK, got %d", code)
	}
	if stats.GlobalRateLimitBps != 15*1024*1024 {
		t.Errorf("expected 15MB limit in stats, got %d", stats.GlobalRateLimitBps)
	}

	// 7. Test real-time SSE Stream against the listener with a bounded context.
	sseCtx, cancelSSE := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancelSSE()
	sseReq, err := http.NewRequestWithContext(sseCtx, http.MethodGet, daemon.baseURL+"/api/v1/events", nil)
	if err != nil {
		t.Fatalf("build SSE request: %v", err)
	}
	sseResp, err := daemon.client.Do(sseReq)
	if err != nil {
		t.Fatalf("SSE request: %v", err)
	}
	sseBody, _ := io.ReadAll(sseResp.Body) // ends when the bounded context expires
	_ = sseResp.Body.Close()

	if !strings.Contains(sseResp.Header.Get("Content-Type"), "text/event-stream") {
		t.Errorf("expected text/event-stream, got %q", sseResp.Header.Get("Content-Type"))
	}
	if !strings.Contains(string(sseBody), "event: progress") {
		t.Errorf("expected progress frame in SSE stream, got %s", sseBody)
	}

	// 8. Test Pause-All and Resume-All Controls.
	if code := daemon.postJSON(t, "/api/v1/downloads/pause-all", nil, nil); code != http.StatusOK {
		t.Fatalf("pause-all: expected 200 OK, got %d", code)
	}
	if code := daemon.postJSON(t, "/api/v1/downloads/resume-all", nil, nil); code != http.StatusOK {
		t.Fatalf("resume-all: expected 200 OK, got %d", code)
	}

	// 9. Verify CLI --input-file (urls.txt) batch format is actually ingested.
	urlsFile := filepath.Join(tempDir, "urls.txt")
	urlsContent := fmt.Sprintf("# Test comment\n%s/a.bin\n\n%s/b.bin\n", origin.URL, origin.URL)
	if err := os.WriteFile(urlsFile, []byte(urlsContent), 0600); err != nil {
		t.Fatalf("failed writing urls.txt: %v", err)
	}
	urls := parseURLListFile(t, urlsFile)
	if len(urls) != 2 {
		t.Fatalf("expected 2 URLs parsed from urls.txt, got %d: %v", len(urls), urls)
	}

	var before api.ListDownloadsResponse
	if code := daemon.getJSON(t, "/api/v1/downloads", &before); code != http.StatusOK {
		t.Fatalf("list before urls.txt batch: expected 200 OK, got %d", code)
	}
	var fromFile api.BatchDownloadResponse
	if code := daemon.postJSON(t, "/api/v1/downloads/batch", api.BatchDownloadRequest{
		URLs:           urls,
		DestinationDir: tempDir,
		Priority:       "normal",
		NoOrganize:     true,
	}, &fromFile); code != http.StatusAccepted {
		t.Fatalf("urls.txt batch enqueue: expected 202 Accepted, got %d", code)
	}
	if fromFile.Enqueued != len(urls) {
		t.Fatalf("urls.txt batch enqueued %d tasks, want %d", fromFile.Enqueued, len(urls))
	}

	expectedTotal := before.Total + len(urls)
	var latest api.ListDownloadsResponse
	waitForCondition(t, "urls.txt batch tasks in list endpoint", integrationPollLimit, func() bool {
		var current api.ListDownloadsResponse
		if code := daemon.getJSON(t, "/api/v1/downloads", &current); code != http.StatusOK {
			t.Fatalf("list during urls.txt batch poll: expected 200 OK, got %d", code)
		}
		latest = current
		if current.Total != expectedTotal {
			return false
		}
		listed := make(map[string]bool, len(current.Items))
		for _, item := range current.Items {
			listed[item.ID] = true
		}
		for _, item := range fromFile.Items {
			if !listed[item.ID] {
				return false
			}
		}
		return true
	})
	if latest.Total != expectedTotal {
		t.Fatalf("list reports %d tasks after urls.txt batch, want %d", latest.Total, expectedTotal)
	}
}
