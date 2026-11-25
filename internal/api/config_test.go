package api

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSetDaemonConfig_DefaultsAppliedOnEnqueue(t *testing.T) {
	server := NewServer("")
	destDir := t.TempDir()
	autoClassify := false
	cfg := DaemonConfig{
		DefaultMaxChunks:    7,
		DefaultDestDir:      destDir,
		DefaultAutoClassify: &autoClassify,
		EnableWebGUI:        true,
	}
	if err := server.SetDaemonConfig(cfg); err != nil {
		t.Fatalf("SetDaemonConfig failed: %v", err)
	}

	resp := server.enqueueDownload(CreateDownloadRequest{URL: "https://example.com/file.pdf"})

	server.jobsMu.RLock()
	record := server.jobs[resp.ID]
	if record == nil {
		server.jobsMu.RUnlock()
		t.Fatal("enqueued job not found")
	}
	if record.maxChunks != 7 {
		t.Errorf("maxChunks = %d, want config default 7", record.maxChunks)
	}
	if record.destDir != destDir {
		t.Errorf("destDir = %q, want config default %q", record.destDir, destDir)
	}
	if record.autoClassify {
		t.Error("autoClassify = true, want config default false")
	}
	wantPath := filepath.Join(destDir, "file.pdf")
	if record.TargetPath != wantPath {
		t.Errorf("TargetPath = %q, want %q", record.TargetPath, wantPath)
	}
	server.jobsMu.RUnlock()

	// An explicit request field must win over every config default.
	overrideDir := t.TempDir()
	explicitClassify := true
	override := server.enqueueDownload(CreateDownloadRequest{
		URL:            "https://example.com/other.pdf",
		DestinationDir: overrideDir,
		MaxChunks:      2,
		AutoClassify:   &explicitClassify,
	})
	server.jobsMu.RLock()
	record = server.jobs[override.ID]
	if record == nil {
		server.jobsMu.RUnlock()
		t.Fatal("overridden job not found")
	}
	if record.maxChunks != 2 || record.destDir != overrideDir || !record.autoClassify {
		t.Errorf("request fields did not override config: %+v", record)
	}
	server.jobsMu.RUnlock()
}

func TestSetDaemonConfig_RejectsTraversalCategoryPaths(t *testing.T) {
	unsafe := []struct {
		folder string
		reason string
	}{
		{folder: "/etc", reason: "absolute"},
		{folder: "../escape", reason: "parent-directory"},
		{folder: "Media/../../escape", reason: "parent-directory"},
		{folder: `..\escape`, reason: "parent-directory"},
		{folder: `C:\evil`, reason: "absolute"},
		{folder: "Videos\x00evil", reason: "null byte"},
	}

	for _, tc := range unsafe {
		t.Run(tc.folder, func(t *testing.T) {
			server := NewServer("")
			err := server.SetDaemonConfig(DaemonConfig{
				CategoryPaths: map[string]string{"Videos": tc.folder},
				EnableWebGUI:  true,
			})
			if err == nil {
				t.Fatalf("expected error for unsafe category path %q", tc.folder)
			}
			if !strings.Contains(err.Error(), "Videos") || !strings.Contains(err.Error(), tc.reason) {
				t.Fatalf("error %q should name the category and explain the rejection", err)
			}
		})
	}
}

func TestDaemonConfig_CustomCategoryPathHonored(t *testing.T) {
	payload := []byte("%PDF-1.4 custom category path payload")
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "report.pdf", time.Now(), bytes.NewReader(payload))
	}))
	defer origin.Close()

	destDir := t.TempDir()
	autoClassify := true
	server := NewServer("127.0.0.1:0")
	server.SetClient(origin.Client())
	if err := server.SetDaemonConfig(DaemonConfig{
		DefaultMaxChunks:    2,
		DefaultDestDir:      destDir,
		DefaultAutoClassify: &autoClassify,
		CategoryPaths:       map[string]string{"Documents": "Papers"},
		EnableWebGUI:        true,
	}); err != nil {
		t.Fatalf("SetDaemonConfig failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server.Scheduler().Start(ctx)
	defer server.Scheduler().Stop()

	resp := server.enqueueDownload(CreateDownloadRequest{URL: origin.URL + "/report.pdf"})

	var status, errMsg string
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		server.jobsMu.RLock()
		if record := server.jobs[resp.ID]; record != nil {
			status, errMsg = record.Status, record.Error
		}
		server.jobsMu.RUnlock()
		if status == "completed" || status == "failed" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if status != "completed" {
		t.Fatalf("download did not complete (status %q, error %q)", status, errMsg)
	}

	customPath := filepath.Join(destDir, "Papers", "report.pdf")
	if _, err := os.Stat(customPath); err != nil {
		t.Fatalf("file not found at configured category path %q: %v", customPath, err)
	}
	data, err := os.ReadFile(customPath)
	if err != nil || !bytes.Equal(data, payload) {
		t.Fatalf("file at %q has unexpected content (err: %v)", customPath, err)
	}
	if _, err := os.Stat(filepath.Join(destDir, "Documents")); !os.IsNotExist(err) {
		t.Fatal("default Documents folder should not have been created")
	}

	server.jobsMu.RLock()
	record := server.jobs[resp.ID]
	targetPath := record.TargetPath
	server.jobsMu.RUnlock()
	if targetPath != customPath {
		t.Fatalf("record TargetPath = %q, want %q", targetPath, customPath)
	}
}

func TestSetDaemonConfig_WebGUIAndStreamingDialTimeout(t *testing.T) {
	server := NewServer("")
	if err := server.SetDaemonConfig(DaemonConfig{
		ConnectionTimeoutSec: 7,
		EnableWebGUI:         false,
	}); err != nil {
		t.Fatalf("SetDaemonConfig failed: %v", err)
	}

	// The configured timeout is a TCP dial deadline, not an overall request
	// deadline: body reads must remain unbounded for large downloads.
	client := server.getClient()
	if client.Timeout != 0 {
		t.Errorf("streaming client Timeout = %v, want 0 (no overall deadline)", client.Timeout)
	}
	transport, ok := client.Transport.(*streamingTransport)
	if !ok {
		t.Fatalf("streaming transport type = %T, want *streamingTransport", client.Transport)
	}
	if transport.dialTimeout != 7*time.Second {
		t.Errorf("dial timeout = %v, want 7s", transport.dialTimeout)
	}
	if transport.ResponseHeaderTimeout != 30*time.Second {
		t.Errorf("ResponseHeaderTimeout = %v, want preserved 30s", transport.ResponseHeaderTimeout)
	}

	router := server.Router()
	for _, tc := range []struct {
		path string
		want int
	}{
		{path: "/", want: http.StatusNotFound},
		{path: "/style.css", want: http.StatusNotFound},
		{path: "/api/v1/health", want: http.StatusOK},
	} {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.path, nil))
		if rec.Code != tc.want {
			t.Errorf("GET %s = %d, want %d (%s)", tc.path, rec.Code, tc.want, fmt.Sprintf("%.80s", rec.Body.String()))
		}
	}
}
