package integration_test

import (
	"bytes"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/omkute/gofetch/internal/api"
	"github.com/omkute/gofetch/internal/engine"
)

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestIntegration_DaemonRESTAPILifecycle(t *testing.T) {
	tempDir := t.TempDir()
	const fileName = "lifecycle.bin"
	payload := bytes.Repeat([]byte("GoFetch loopback lifecycle payload! "), 16*1024)

	// A real range-capable origin and a real daemon listener: every request
	// below goes over loopback HTTP, and the scheduler actually runs the job.
	// The origin holds back part of each range so the in-flight job can be
	// observed and paused deterministically.
	origin := newGatedRangeOrigin(t, fileName, payload)
	daemon := startTestDaemon(t, nil)

	// 1. Submit download via POST /api/v1/downloads.
	var created api.DownloadResponse
	if code := daemon.postJSON(t, "/api/v1/downloads", api.CreateDownloadRequest{
		URL:            origin.URL + "/" + fileName,
		DestinationDir: tempDir,
		Priority:       "high",
		MaxChunks:      4,
		NoOrganize:     true,
	}, &created); code != http.StatusAccepted {
		t.Fatalf("POST /api/v1/downloads: expected 202 Accepted, got %d", code)
	}
	if created.ID == "" {
		t.Fatal("expected a non-empty task ID from enqueue")
	}
	taskID := created.ID

	// 2. List endpoint reports the enqueued task.
	var list api.ListDownloadsResponse
	if code := daemon.getJSON(t, "/api/v1/downloads", &list); code != http.StatusOK {
		t.Fatalf("GET /api/v1/downloads: expected 200 OK, got %d", code)
	}
	if list.Total != 1 || len(list.Items) != 1 || list.Items[0].ID != taskID {
		t.Fatalf("expected exactly 1 listed task with ID %s, got %+v", taskID, list)
	}

	// 3. Detail endpoint reports real progress and per-chunk state while the
	// origin is still serving byte ranges. Wait for a real chunk request first
	// so this cannot miss the brief in-flight window.
	select {
	case <-origin.chunkRequested:
	case <-time.After(integrationPollLimit):
		t.Fatal("origin never received a chunk request")
	}
	detail := daemon.waitForDetail(t, taskID, func(d api.DownloadDetailResponse) bool {
		return d.Status == "downloading" && d.DownloadedBytes > 0 && len(d.Chunks) > 0
	})
	if detail.TotalBytes != int64(len(payload)) {
		t.Fatalf("detail total bytes = %d, want %d", detail.TotalBytes, len(payload))
	}

	// 4. Pause halts the running transfer.
	if code := daemon.postJSON(t, "/api/v1/downloads/"+taskID+"/pause", nil, nil); code != http.StatusOK {
		t.Fatalf("POST pause: expected 200 OK, got %d", code)
	}
	paused := daemon.waitForDetail(t, taskID, func(d api.DownloadDetailResponse) bool {
		return d.Status == "paused"
	})
	if paused.DownloadedBytes <= 0 {
		t.Errorf("paused task should retain progress, got %d bytes", paused.DownloadedBytes)
	}
	// Release any blocked origin handlers now; the paused download must not
	// continue simply because bytes became available.
	origin.unblock()

	// 5. Resume returns the task to downloading and it completes with real
	// bytes on disk.
	if code := daemon.postJSON(t, "/api/v1/downloads/"+taskID+"/resume", nil, nil); code != http.StatusOK {
		t.Fatalf("POST resume: expected 200 OK, got %d", code)
	}
	daemon.waitForDetail(t, taskID, func(d api.DownloadDetailResponse) bool {
		return (d.Status == "downloading" || d.Status == "completed") && d.DownloadedBytes > 0
	})

	completed := daemon.waitForDetail(t, taskID, func(d api.DownloadDetailResponse) bool {
		return d.Status == "completed" && d.DownloadedBytes == int64(len(payload))
	})
	if len(completed.Chunks) == 0 {
		t.Fatal("completed detail must include per-chunk information")
	}
	for i, chunk := range completed.Chunks {
		if chunk.ChunkIndex != i || chunk.StartByte < 0 || chunk.EndByte < chunk.StartByte ||
			chunk.EndByte >= int64(len(payload)) || !chunk.Completed {
			t.Errorf("unexpected chunk %d: %+v", i, chunk)
		}
	}

	finalPath := filepath.Join(tempDir, fileName)
	stored, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatalf("expected committed file %q on disk: %v", finalPath, err)
	}
	if !bytes.Equal(stored, payload) {
		t.Fatalf("committed file has %d bytes, want the exact %d-byte payload", len(stored), len(payload))
	}
	if _, statErr := os.Stat(engine.StagingPathFor(finalPath, taskID)); !os.IsNotExist(statErr) {
		t.Errorf("staging file should be removed after commit: %v", statErr)
	}

	// 6. Cancel/delete removes the task from the daemon and from disk.
	if code := daemon.jsonRequest(t, http.MethodDelete, "/api/v1/downloads/"+taskID+"?delete_files=true", nil, nil); code != http.StatusOK {
		t.Fatalf("DELETE download: expected 200 OK, got %d", code)
	}
	var afterDelete api.ListDownloadsResponse
	if code := daemon.getJSON(t, "/api/v1/downloads", &afterDelete); code != http.StatusOK {
		t.Fatalf("GET /api/v1/downloads after delete: expected 200 OK, got %d", code)
	}
	if afterDelete.Total != 0 || len(afterDelete.Items) != 0 {
		t.Fatalf("deleted task still listed: %+v", afterDelete)
	}
	if code := daemon.getJSON(t, "/api/v1/downloads/"+taskID, nil); code != http.StatusNotFound {
		t.Fatalf("GET deleted task: expected 404 Not Found, got %d", code)
	}
	if _, statErr := os.Stat(finalPath); !os.IsNotExist(statErr) {
		t.Errorf("delete_files=true should remove %q: %v", finalPath, statErr)
	}
}

func TestIntegration_DaemonChecksumVerification(t *testing.T) {
	tempDir := t.TempDir()
	const fileName = "data.bin"
	content := []byte("Daemon checksum validation content stream!")

	origin := newRangeOrigin(t, fileName, content)
	daemon := startTestDaemon(t, nil)

	// Enqueue with an intentionally incorrect checksum.
	var created api.DownloadResponse
	if code := daemon.postJSON(t, "/api/v1/downloads", api.CreateDownloadRequest{
		URL:            origin.URL + "/" + fileName,
		DestinationDir: tempDir,
		Checksum:       "wronghash000000000000000000000000000000000000000000000000000000",
		ChecksumAlgo:   "sha256",
		NoOrganize:     true,
	}, &created); code != http.StatusAccepted {
		t.Fatalf("POST /api/v1/downloads: expected 202 Accepted, got %d", code)
	}

	detail := daemon.waitForDetail(t, created.ID, func(d api.DownloadDetailResponse) bool {
		return d.Status == "failed"
	})
	if !strings.Contains(detail.Error, "checksum mismatch") {
		t.Errorf("expected checksum mismatch error, got: %s", detail.Error)
	}

	// A checksum mismatch must never commit the corrupt bytes to the final
	// destination: no os.Rename should have produced this path.
	finalPath := filepath.Join(tempDir, fileName)
	if _, statErr := os.Stat(finalPath); !os.IsNotExist(statErr) {
		t.Fatalf("checksum mismatch must never create final file %q, stat error: %v", finalPath, statErr)
	}
	partPath := engine.StagingPathFor(finalPath, created.ID)
	if _, statErr := os.Stat(partPath); statErr != nil {
		t.Fatalf("checksum mismatch must preserve staged part file %q: %v", partPath, statErr)
	}
	staged, err := os.ReadFile(partPath)
	if err != nil {
		t.Fatalf("read staged part file %q: %v", partPath, err)
	}
	if !bytes.Equal(staged, content) {
		t.Fatalf("staged file holds %d bytes, want the %d-byte source", len(staged), len(content))
	}
}
