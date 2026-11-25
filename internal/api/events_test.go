package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSystemStatsEndpoint(t *testing.T) {
	server := NewServer("127.0.0.1:0")

	req := httptest.NewRequest(http.MethodGet, "/api/v1/system/stats", nil)
	rec := httptest.NewRecorder()

	server.Router().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	var stats SystemStatsResponse
	if err := json.NewDecoder(rec.Body).Decode(&stats); err != nil {
		t.Fatalf("failed decoding stats JSON: %v", err)
	}

	if stats.ActiveDownloads != 0 || stats.QueuedDownloads != 0 {
		t.Errorf("unexpected initial stats: %+v", stats)
	}
}

func TestSetGlobalRateLimitEndpoint(t *testing.T) {
	server := NewServer("127.0.0.1:0")

	body := []byte(`{"rate_limit":"10MB"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/system/rate-limit", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	server.Router().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
	}

	var stats SystemStatsResponse
	if err := json.NewDecoder(rec.Body).Decode(&stats); err != nil {
		t.Fatalf("failed decoding stats response: %v", err)
	}

	expectedLimit := int64(10 * 1024 * 1024)
	if stats.GlobalRateLimitBps != expectedLimit {
		t.Errorf("expected limit %d, got %d", expectedLimit, stats.GlobalRateLimitBps)
	}
}

func TestBatchCreateAndPauseResumeAll(t *testing.T) {
	server := NewServer("127.0.0.1:0")

	batchReq := BatchDownloadRequest{
		URLs:           []string{"https://example.com/file1.iso", "https://example.com/file2.iso"},
		DestinationDir: t.TempDir(),
		Priority:       "high",
		RateLimit:      "2MB",
	}
	body, _ := json.Marshal(batchReq)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/downloads/batch", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	server.Router().ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d: %s", rec.Code, rec.Body.String())
	}

	var batchResp BatchDownloadResponse
	if err := json.NewDecoder(rec.Body).Decode(&batchResp); err != nil {
		t.Fatalf("failed decoding batch response: %v", err)
	}

	if batchResp.Enqueued != 2 || len(batchResp.Items) != 2 {
		t.Fatalf("expected 2 enqueued items, got %d", batchResp.Enqueued)
	}

	// Test Pause All
	pauseReq := httptest.NewRequest(http.MethodPost, "/api/v1/downloads/pause-all", nil)
	pauseRec := httptest.NewRecorder()
	server.Router().ServeHTTP(pauseRec, pauseReq)
	if pauseRec.Code != http.StatusOK {
		t.Fatalf("pause-all failed: %d", pauseRec.Code)
	}

	server.jobsMu.RLock()
	for _, job := range server.jobs {
		if job.Status != "paused" {
			t.Errorf("expected job %s to be paused, got %s", job.ID, job.Status)
		}
	}
	server.jobsMu.RUnlock()

	// Test Resume All
	resumeReq := httptest.NewRequest(http.MethodPost, "/api/v1/downloads/resume-all", nil)
	resumeRec := httptest.NewRecorder()
	server.Router().ServeHTTP(resumeRec, resumeReq)
	if resumeRec.Code != http.StatusOK {
		t.Fatalf("resume-all failed: %d", resumeRec.Code)
	}

	server.jobsMu.RLock()
	for _, job := range server.jobs {
		if job.Status != "queued" && job.Status != "downloading" {
			t.Errorf("expected job %s to be queued/downloading, got %s", job.ID, job.Status)
		}
	}
	server.jobsMu.RUnlock()
}

func TestSSEProgressIntervalIsSub100ms(t *testing.T) {
	if SSEProgressInterval >= 100*time.Millisecond {
		t.Fatalf("SSEProgressInterval = %v, want sub-100ms", SSEProgressInterval)
	}
}

func TestSSEProgressFramesWithinSubSecondWindow(t *testing.T) {
	server := NewServer("127.0.0.1:0")
	httpServer := httptest.NewServer(server.Router())
	defer httpServer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, httpServer.URL+"/api/v1/events", nil)
	if err != nil {
		t.Fatalf("creating SSE request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("connecting to SSE endpoint: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if got := resp.Header.Get("Content-Type"); !strings.Contains(got, "text/event-stream") {
		t.Fatalf("expected text/event-stream content type, got %q", got)
	}

	// The initial snapshot is sent immediately, then one frame per
	// SSEProgressInterval. Four frames should therefore arrive in ~150ms;
	// the generous 700ms budget avoids flaking under CI load while still
	// failing the old 500ms cadence (which yields at most two frames).
	const wantFrames = 4
	const window = 700 * time.Millisecond

	start := time.Now()
	frames := 0
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "event: progress") {
			frames++
			if frames >= wantFrames {
				break
			}
		}
	}
	elapsed := time.Since(start)

	if err := scanner.Err(); err != nil && ctx.Err() == nil {
		t.Fatalf("reading SSE stream: %v", err)
	}
	if frames < wantFrames {
		t.Fatalf("observed only %d progress frames, want %d", frames, wantFrames)
	}
	if elapsed >= window {
		t.Fatalf("%d progress frames took %v, want < %v (interval %v)", frames, elapsed, window, SSEProgressInterval)
	}
}

func TestSSEEndpointStreaming(t *testing.T) {
	server := NewServer("127.0.0.1:0")

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/events", nil).WithContext(ctx)
	rec := httptest.NewRecorder()

	server.Router().ServeHTTP(rec, req)

	contentType := rec.Header().Get("Content-Type")
	if !strings.Contains(contentType, "text/event-stream") {
		t.Errorf("expected text/event-stream content type, got %q", contentType)
	}

	output := rec.Body.String()
	if !strings.Contains(output, "event: progress") {
		t.Errorf("expected initial progress event frame, got %q", output)
	}
}
