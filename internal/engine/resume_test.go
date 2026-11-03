package engine_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/omkute/gofetch/internal/engine"
	"github.com/omkute/gofetch/internal/state"
	"github.com/omkute/gofetch/internal/storage"
)

func TestResume_MidDownloadInterruption(t *testing.T) {
	tempDir := t.TempDir()
	const totalSize = int64(1024 * 1024) // 1 MB
	const numWorkers = 4

	sourcePayload := make([]byte, totalSize)
	if _, err := rand.Read(sourcePayload); err != nil {
		t.Fatalf("failed to generate random payload: %v", err)
	}

	var requestRanges []string
	var mu sync.Mutex

	mockClient := newMockClient(func(req *http.Request) (*http.Response, error) {
		header := make(http.Header)
		header.Set("Accept-Ranges", "bytes")
		header.Set("Content-Length", strconv.FormatInt(totalSize, 10))
		header.Set("Content-Type", "application/octet-stream")
		header.Set("Content-Disposition", `attachment; filename="resumable_test.bin"`)
		header.Set("ETag", `"etag-resumable-1"`)

		if req.Method == http.MethodHead {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     header,
				Body:       io.NopCloser(bytes.NewReader(nil)),
				Request:    req,
			}, nil
		}

		rangeHeader := req.Header.Get("Range")
		mu.Lock()
		requestRanges = append(requestRanges, rangeHeader)
		mu.Unlock()

		parts := strings.Split(strings.TrimPrefix(rangeHeader, "bytes="), "-")
		start, _ := strconv.ParseInt(parts[0], 10, 64)
		end, _ := strconv.ParseInt(parts[1], 10, 64)

		slice := sourcePayload[start : end+1]
		rangeRespHeader := make(http.Header)
		rangeRespHeader.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, totalSize))
		rangeRespHeader.Set("Content-Length", strconv.Itoa(len(slice)))

		return &http.Response{
			StatusCode: http.StatusPartialContent,
			Header:     rangeRespHeader,
			Body:       io.NopCloser(bytes.NewReader(slice)),
			Request:    req,
		}, nil
	})

	// Setup bbolt checkpoint store
	dbPath := filepath.Join(tempDir, "checkpoints.db")
	store, err := state.NewBboltStore(dbPath)
	if err != nil {
		t.Fatalf("failed to init bbolt: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	finalPath := filepath.Join(tempDir, "resumable_test.bin")
	partPath := finalPath + storage.PartFileExtension

	// Pre-create the part file as if chunk 0 and chunk 1 were already written, and chunk 2 partially written
	// Chunks for 1MB with 4 workers:
	// Chunk 0: [0, 262143] (262144 bytes) - Completed
	// Chunk 1: [262144, 524287] (262144 bytes) - Completed
	// Chunk 2: [524288, 786431] (262144 bytes) - Partially downloaded (up to 600000)
	// Chunk 3: [786432, 1048575] (262144 bytes) - Not started
	fw, err := storage.NewFileWriter(finalPath, totalSize)
	if err != nil {
		t.Fatalf("failed to create existing part file: %v", err)
	}

	// Write completed chunk 0 and 1
	_, _ = fw.WriteAt(sourcePayload[0:524288], 0)
	// Write partial chunk 2 (from 524288 to 599999)
	_, _ = fw.WriteAt(sourcePayload[524288:600000], 524288)
	_ = fw.Close() // leave .part on disk

	// Save existing checkpoint into bbolt store
	jobID := "job-resumable-1"
	existingCheckpoint := &state.Checkpoint{
		ID:        jobID,
		URL:       "http://mock.local/resumable_test.bin",
		FinalPath: finalPath,
		TotalSize: totalSize,
		ETag:      `"etag-resumable-1"`,
		Chunks: []engine.Chunk{
			{Index: 0, Start: 0, End: 262143, Current: 262144, Completed: true},
			{Index: 1, Start: 262144, End: 524287, Current: 524288, Completed: true},
			{Index: 2, Start: 524288, End: 786431, Current: 600000, Completed: false},
			{Index: 3, Start: 786432, End: 1048575, Current: 786432, Completed: false},
		},
		UpdatedAt: time.Now(),
	}

	if err := store.SaveCheckpoint(existingCheckpoint); err != nil {
		t.Fatalf("failed to seed checkpoint: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	probeInfo, err := engine.Probe(ctx, mockClient, "http://mock.local/resumable_test.bin")
	if err != nil {
		t.Fatalf("probe failed: %v", err)
	}

	coordinator := engine.NewCoordinatorWithStore(mockClient, store)
	resumedPath, err := coordinator.DownloadResumable(ctx, jobID, probeInfo, tempDir, numWorkers)
	if err != nil {
		t.Fatalf("DownloadResumable failed: %v", err)
	}

	if resumedPath != finalPath {
		t.Fatalf("expected resumed path %q, got %q", finalPath, resumedPath)
	}

	// Invariant: Chunk 0 and 1 were already completed, so NO requests should have been made for them!
	// Chunk 2 should resume at bytes=600000-786431!
	// Chunk 3 should request bytes=786432-1048575!
	mu.Lock()
	ranges := append([]string(nil), requestRanges...)
	mu.Unlock()

	for _, r := range ranges {
		if strings.Contains(r, "bytes=0-") || strings.Contains(r, "bytes=262144-") {
			t.Errorf("refetched already completed chunk! Range header: %s", r)
		}
	}

	// Verify chunk 2 resumed at offset 600000
	foundChunk2Resume := false
	for _, r := range ranges {
		if r == "bytes=600000-786431" {
			foundChunk2Resume = true
			break
		}
	}
	if !foundChunk2Resume {
		t.Errorf("did not find expected resumed Range bytes=600000-786431, got: %v", ranges)
	}

	// Verify final file exists and content matches bit-for-bit
	finalContent, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatalf("failed reading final file: %v", err)
	}
	if !bytes.Equal(finalContent, sourcePayload) {
		t.Fatal("assembled file content does not match expected source payload")
	}

	// Verify part file was removed
	if _, err := os.Stat(partPath); !os.IsNotExist(err) {
		t.Fatalf("part file %q still exists after completion", partPath)
	}

	// Verify checkpoint was deleted upon completion
	cpAfter, err := store.GetCheckpoint(jobID)
	if err != nil {
		t.Fatalf("error checking checkpoint: %v", err)
	}
	if cpAfter != nil {
		t.Fatalf("checkpoint should be deleted after successful download completion")
	}
}
